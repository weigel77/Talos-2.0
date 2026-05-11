"""Local-only Talos autonomous simulated trade orchestration."""

from __future__ import annotations

import json
import logging
from collections import Counter
from dataclasses import replace
from datetime import date, datetime, time, timedelta
from itertools import combinations
from pathlib import Path
from threading import RLock
from typing import Any, Dict, List
from zoneinfo import ZoneInfo

from config import AppConfig, get_app_config

from .apollo_service import ApolloService
from .market_calendar_service import MarketCalendarService
from .market_data import MarketDataService
from .open_trade_manager import OpenTradeManager
from .options_chain_service import OptionsChainService
from .performance_dashboard_service import build_performance_record, calculate_expectancy, summarize_outcomes
from .repositories.trade_repository import TradeRepository
from .runtime.scheduler import RuntimeJobHandle, RuntimeScheduler, ThreadingTimerScheduler
from .trade_store import JOURNAL_NAME_DEFAULT, current_timestamp, normalize_system_name, parse_date_value


LOGGER = logging.getLogger(__name__)


class NoopTalosService:
    def initialize(self) -> None:
        return None

    def start_background_monitoring(self) -> None:
        return None

    def shutdown(self) -> None:
        return None

    def get_dashboard_payload(self) -> Dict[str, Any]:
        return {
            "enabled": False,
            "account_balance": 135000.0,
            "account_balance_display": "$135,000.00",
            "settled_balance": 135000.0,
            "settled_balance_display": "$135,000.00",
            "unrealized_pnl": 0.0,
            "unrealized_pnl_display": "$0.00",
            "open_trade_count": 0,
            "open_records": [],
            "activity_log": [],
            "skip_log": [],
            "slot_statuses": [],
            "header_market_snapshots": {},
            "last_cycle_at": "",
            "last_cycle_at_display": "Not yet evaluated",
            "last_cycle_reason": "",
            "last_cycle_status": "disabled",
        }

    def run_cycle(self, *, trigger_reason: str = "manual") -> Dict[str, Any]:
        return self.get_dashboard_payload()

    def reset_state(self, *, new_starting_balance: float) -> Dict[str, Any]:
        return {"deleted_trade_count": 0}


class TalosService:
    STARTING_BALANCE = 135000.0
    BACKGROUND_INTERVAL_SECONDS = 60
    POST_CLOSE_FINAL_SCAN_MINUTES = 1
    MARKET_OPEN = time(8, 30)
    MARKET_CLOSE = time(15, 0)
    APOLLO_ENTRY_WINDOW_HOURS = 3
    NO_CLOSE_AFTER_OPEN_MINUTES = 15
    PROFIT_CAPTURE_ROTATION_THRESHOLD = 75.0
    TALOS_CAPTURE_LIQUIDATION_THRESHOLD = 80.0
    ENTRY_SCORE_MINIMUM = 70.0
    MAX_ACTIVITY_ITEMS = 60
    MAX_SKIP_ITEMS = 60
    MAX_TOTAL_OPEN_TRADES = 4
    MAX_OPEN_TRADES_PER_SYSTEM = {"Apollo": 3}
    APOLLO_MAX_PORTFOLIO_POSITIONS = 3
    APOLLO_PREMIUM_TARGET_RATIO = 0.01
    APOLLO_PROJECTED_BLACK_SWAN_LOSS_CAP_RATIO = 0.10
    APOLLO_PORTFOLIO_CANDIDATE_LIMIT = 8
    APOLLO_ALLOWED_PROFILE_COMBINATIONS = (
        ("Fortress",),
        ("Standard",),
        ("Aggressive",),
        ("Standard", "Fortress"),
        ("Standard", "Aggressive"),
        ("Standard", "Fortress", "Aggressive"),
    )
    SLOT_ORDER = ("Apollo",)
    SCORE_BANDS = (
        (90.0, "Elite", "elite"),
        (80.0, "Strong", "strong"),
        (70.0, "Watch", "watch"),
        (60.0, "Weak", "weak"),
        (0.0, "Reject", "reject"),
    )
    DEFAULT_COMPONENT_WEIGHTS = {
        "safety": 32.0,
        "history": 22.0,
        "credit": 14.0,
        "profit": 12.0,
        "fit": 10.0,
        "standard_preference": 10.0,
        "loss_penalty": 20.0,
    }
    COMPONENT_WEIGHT_BOUNDS = {
        "safety": (24.0, 40.0),
        "history": (14.0, 30.0),
        "credit": (10.0, 22.0),
        "profit": (8.0, 18.0),
        "fit": (6.0, 16.0),
        "standard_preference": (4.0, 15.0),
        "loss_penalty": (10.0, 30.0),
    }
    LEARNING_SEGMENTS = ("Apollo_HIGH_VIX", "Apollo_LOW_VIX")
    LEARNING_SOURCE_WEIGHTS = {"real": 1.0, "simulated": 0.7, "talos": 1.5}
    LEARNING_MIN_WEIGHTED_TRADES = 3.0
    MAX_WEIGHT_CHANGE_PER_CYCLE = 2.0
    MAX_DECISION_ITEMS = 120
    def __init__(
        self,
        *,
        trade_store: TradeRepository,
        market_data_service: MarketDataService,
        options_chain_service: OptionsChainService,
        open_trade_manager: OpenTradeManager,
        config: AppConfig | None = None,
        scheduler: RuntimeScheduler | None = None,
        state_path: str | Path | None = None,
        now_provider=None,
    ) -> None:
        self.config = config or get_app_config()
        self.trade_store = trade_store
        self.market_data_service = market_data_service
        self.options_chain_service = options_chain_service
        self.open_trade_manager = open_trade_manager
        self.scheduler = scheduler or ThreadingTimerScheduler()
        self.display_timezone = ZoneInfo(self.config.app_timezone)
        self.market_calendar_service = MarketCalendarService(self.config)
        self.now_provider = now_provider or (lambda: datetime.now(self.display_timezone))
        self.state_path = Path(state_path) if state_path is not None else Path(self.trade_store.database_path).with_name("talos_state.json")
        self.archive_directory = self.state_path.with_name("talos_recovery")
        self._lock = RLock()
        self._monitor_running = False
        self._monitor_timer: RuntimeJobHandle | None = None

    def initialize(self) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.archive_directory.mkdir(parents=True, exist_ok=True)
        with self._lock:
            if not self.state_path.exists():
                self._save_state(self._default_state())

    def start_background_monitoring(self) -> None:
        with self._lock:
            if self._monitor_running:
                return
            self._monitor_running = True
            self._schedule_background_monitor()

    def shutdown(self) -> None:
        with self._lock:
            self._monitor_running = False
            if self._monitor_timer is not None:
                self._monitor_timer.cancel()
                self._monitor_timer = None

    def get_dashboard_payload(self) -> Dict[str, Any]:
        with self._lock:
            state = self._load_state()
            changed = False
            scan_engine_running = bool(self._monitor_running)
            if bool(state.get("scan_engine_running")) != scan_engine_running:
                state["scan_engine_running"] = scan_engine_running
                changed = True
            next_scan_at = str(state.get("next_scan_at") or "")
            self._refresh_runtime_schedule_state(state, reference_time=self._now())
            if str(state.get("next_scan_at") or "") != next_scan_at:
                changed = True
            management_payload = self.open_trade_manager.evaluate_open_trades(
                send_alerts=False,
                caller_source="job:talos-dashboard-refresh",
            )
            open_records = self._filter_talos_records(management_payload)
            if self._reconcile_open_trade_governance(state, open_records, source_label="dashboard refresh"):
                management_payload = self.open_trade_manager.evaluate_open_trades(
                    send_alerts=False,
                    caller_source="job:talos-dashboard-refresh",
                )
                open_records = self._filter_talos_records(management_payload)
                changed = True
            if self._sync_trade_metadata(state, open_records):
                changed = True
            if self._refresh_learning_state(state):
                changed = True
            if self._refresh_apollo_portfolio_state(state, open_records):
                changed = True
            if changed:
                self._save_state(state)
            return self._build_dashboard_payload(state, management_payload=management_payload, open_records=open_records)

    def run_cycle(self, *, trigger_reason: str = "manual") -> Dict[str, Any]:
        with self._lock:
            state = self._load_state()
            now = self._now()
            self._refresh_runtime_schedule_state(state, reference_time=now)
            if state.get("paused"):
                state["last_scan_at"] = now.isoformat()
                state["total_scan_count"] = int(state.get("total_scan_count") or 0) + 1
                state["last_cycle_at"] = now.isoformat()
                state["last_cycle_reason"] = trigger_reason
                state["last_cycle_status"] = "paused"
                self._refresh_runtime_schedule_state(state, reference_time=now)
                self._append_activity(
                    state,
                    {
                        "timestamp": now.isoformat(),
                        "category": "status",
                        "title": "Talos cycle skipped",
                        "detail": "Talos is paused, so no autonomous actions were taken.",
                    },
                )
                self._save_state(state)
                return self._build_dashboard_payload(state)

            management_payload = self.open_trade_manager.evaluate_open_trades(
                send_alerts=False,
                caller_source=f"job:talos-cycle:{trigger_reason}",
            )
            open_records = self._filter_talos_records(management_payload)
            if self._reconcile_open_trade_governance(state, open_records, source_label=f"{trigger_reason} cycle"):
                management_payload = self.open_trade_manager.evaluate_open_trades(
                    send_alerts=False,
                    caller_source=f"job:talos-cycle:{trigger_reason}",
                )
                open_records = self._filter_talos_records(management_payload)
            self._sync_trade_metadata(state, open_records)
            self._refresh_learning_state(state)
            cycle_mode = self._resolve_cycle_mode(now)

            state["last_scan_at"] = now.isoformat()
            state["total_scan_count"] = int(state.get("total_scan_count") or 0) + 1

            if cycle_mode == "closed":
                state["last_cycle_at"] = now.isoformat()
                state["last_cycle_reason"] = trigger_reason
                state["last_cycle_status"] = "outside-market-hours"
                self._refresh_runtime_schedule_state(state, reference_time=now)
                self._append_skip(
                    state,
                    {
                        "timestamp": now.isoformat(),
                        "system": "Talos",
                        "reason": "Market window is closed.",
                    },
                )
                self._save_state(state)
                return self._build_dashboard_payload(state, management_payload=management_payload, open_records=open_records)

            management_changed = self._manage_open_trades(state, open_records)
            if management_changed:
                management_payload = self.open_trade_manager.evaluate_open_trades(
                    send_alerts=False,
                    caller_source=f"job:talos-cycle:{trigger_reason}",
                )
                open_records = self._filter_talos_records(management_payload)
                self._sync_trade_metadata(state, open_records)

            if cycle_mode == "post-close-final":
                state["last_cycle_at"] = now.isoformat()
                state["last_cycle_reason"] = trigger_reason
                state["last_cycle_status"] = "post-close-final-pass"
                self._refresh_runtime_schedule_state(state, reference_time=now)
                self._append_activity(
                    state,
                    {
                        "timestamp": now.isoformat(),
                        "category": "status",
                        "title": "Talos final post-close pass",
                        "detail": "Talos completed the final post-close management pass. No new Talos entries are allowed after the regular market close.",
                    },
                )
                self._save_state(state)
                return self._build_dashboard_payload(state, management_payload=management_payload, open_records=open_records)

            balance = self._compute_balance_components(open_records)
            candidates = self._build_candidates(
                balance["settled_balance"],
                open_records,
                learning_state=dict(state.get("learning_state") or {}),
            )
            apollo_plan = candidates.get("Apollo")
            apollo_action_taken = self._evaluate_apollo_portfolio(state, apollo_plan, open_records)
            if apollo_action_taken:
                management_payload = self.open_trade_manager.evaluate_open_trades(
                    send_alerts=False,
                    caller_source=f"job:talos-cycle:{trigger_reason}",
                )
                open_records = self._filter_talos_records(management_payload)
                self._sync_trade_metadata(state, open_records)
                balance = self._compute_balance_components(open_records)
                apollo_plan = self._build_apollo_portfolio_plan(
                    balance["settled_balance"],
                    open_records,
                    learning_state=dict(state.get("learning_state") or {}),
                )
            self._refresh_apollo_portfolio_state(state, open_records, existing_plan=apollo_plan)
            self._refresh_runtime_schedule_state(state, reference_time=now)
            if management_changed:
                self._append_post_management_follow_through_activity(
                    state,
                    cycle_time=now,
                    apollo_action_taken=apollo_action_taken,
                )

            state["last_cycle_at"] = now.isoformat()
            state["last_cycle_reason"] = trigger_reason
            state["last_cycle_status"] = "completed"
            self._refresh_runtime_schedule_state(state, reference_time=now)
            self._save_state(state)
            return self._build_dashboard_payload(state, management_payload=management_payload, open_records=open_records)

    def reset_state(self, *, new_starting_balance: float) -> Dict[str, Any]:
        with self._lock:
            talos_trades = list(self.trade_store.list_trades("talos"))
            archive_path = self._archive_runtime_snapshot(self._load_state(), talos_trades)
            for trade in talos_trades:
                trade_id = int(trade.get("id") or 0)
                if trade_id > 0:
                    self.trade_store.delete_trade(trade_id)
            self._save_state(self._default_state(starting_balance=new_starting_balance, last_archive_path=str(archive_path)))
            return {
                "deleted_trade_count": len(talos_trades),
                "archive_path": str(archive_path),
                "new_starting_balance": float(new_starting_balance),
            }

    def _schedule_background_monitor(self) -> None:
        if not self._monitor_running:
            return
        next_run_at = self._resolve_next_background_run_at(self._now())
        if next_run_at is None:
            return
        delay_seconds = max((next_run_at - self._now()).total_seconds(), 1.0)
        state = self._load_state()
        state["scan_engine_running"] = True
        state["next_scan_at"] = next_run_at.isoformat()
        self._save_state(state)
        self._monitor_timer = self.scheduler.schedule(
            delay_seconds,
            self._background_monitor_tick,
            daemon=True,
        )

    def _background_monitor_tick(self) -> None:
        try:
            self.run_cycle(trigger_reason="background")
        except Exception as exc:  # pragma: no cover - defensive scheduler guard
            LOGGER.warning("Talos background cycle failed: %s", exc)
        finally:
            with self._lock:
                if self._monitor_running:
                    self._schedule_background_monitor()

    def _build_dashboard_payload(
        self,
        state: Dict[str, Any],
        *,
        management_payload: Dict[str, Any] | None = None,
        open_records: List[Dict[str, Any]] | None = None,
    ) -> Dict[str, Any]:
        management_payload = management_payload or self.open_trade_manager.evaluate_open_trades(
            send_alerts=False,
            caller_source="job:talos-build-dashboard",
        )
        open_records = open_records if open_records is not None else self._filter_talos_records(management_payload)
        balance = self._compute_balance_components(open_records)
        metadata_map = state.get("trade_metadata") or {}
        learning_state = dict(state.get("learning_state") or {})
        decorated_open_records = [
            self._decorate_open_record(record, metadata_map.get(str(record.get("trade_id") or ""), {}), learning_state=learning_state)
            for record in open_records
        ]
        skip_log = list(state.get("skip_log") or [])
        runtime_status = self._build_runtime_status_payload(state)
        return {
            "enabled": True,
            "account_balance": balance["settled_balance"],
            "account_balance_display": self._format_currency(balance["settled_balance"]),
            "settled_balance": balance["settled_balance"],
            "settled_balance_display": self._format_currency(balance["settled_balance"]),
            "current_account_value": balance["current_account_value"],
            "current_account_value_display": self._format_currency(balance["current_account_value"]),
            "realized_pnl": balance["realized_pnl"],
            "realized_pnl_display": self._format_currency(balance["realized_pnl"]),
            "unrealized_pnl": balance["unrealized_pnl"],
            "unrealized_pnl_display": self._format_currency(balance["unrealized_pnl"]),
            "open_trade_count": len(open_records),
            "open_records": decorated_open_records,
            "apollo_portfolio": self._build_apollo_portfolio_dashboard_payload(state, open_records, balance),
            "formula_chart": self._build_formula_chart_payload(learning_state),
            "current_commitment": self._build_current_commitment_payload(open_records, balance),
            "settled_equity_curve": self._build_settled_equity_curve_payload(state),
            "performance_metrics": self._build_performance_metrics_payload(),
            "activity_log": self._build_activity_log_payload(list(state.get("activity_log") or [])),
            "decision_log": list(state.get("decision_log") or []),
            "skip_log": skip_log,
            "skip_log_summary": self._build_skip_log_summary(skip_log),
            "slot_statuses": self._build_slot_statuses(open_records, metadata_map),
            "decision_model": self._build_decision_model_payload(decorated_open_records, learning_state),
            "learning_state": self._build_learning_state_payload(learning_state),
            "header_market_snapshots": dict(management_payload.get("header_market_snapshots") or {}),
            "last_cycle_at": str(state.get("last_cycle_at") or ""),
            "last_cycle_at_display": self._format_datetime(state.get("last_cycle_at")),
            "last_cycle_reason": str(state.get("last_cycle_reason") or ""),
            "last_cycle_status": str(state.get("last_cycle_status") or "idle"),
            "last_scan_at_display": self._format_datetime(state.get("last_scan_at")),
            "next_scan_at_display": self._format_datetime(state.get("next_scan_at")),
            "scan_engine_running": bool(state.get("scan_engine_running")),
            "runtime_status": runtime_status,
            "runtime_status_display": runtime_status["label"],
            "runtime_status_key": runtime_status["key"],
            "total_scan_count": int(state.get("total_scan_count") or 0),
            "starting_balance": float(state.get("starting_balance") or self.STARTING_BALANCE),
            "starting_balance_display": self._format_currency(float(state.get("starting_balance") or self.STARTING_BALANCE)),
            "recovery_archive_dir": str(self.archive_directory),
            "last_archive_path": str(state.get("last_archive_path") or ""),
            "paused": bool(state.get("paused")),
        }

    def _decorate_open_record(
        self,
        record: Dict[str, Any],
        metadata: Dict[str, Any],
        *,
        learning_state: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        decorated = dict(record)
        trade = self.trade_store.get_trade(int(record.get("trade_id") or 0)) if int(record.get("trade_id") or 0) > 0 else None
        talos_metadata = dict(metadata or {})
        score_breakdown = self._score_open_position_record(decorated, trade, talos_metadata, learning_state=learning_state)
        candidate_score = self._coerce_float(score_breakdown.get("candidate_score"))
        score_band = self._score_band_for_value(candidate_score)
        decorated["talos_metadata"] = talos_metadata
        decorated["talos_candidate_score"] = candidate_score
        decorated["talos_candidate_score_display"] = f"{candidate_score:.1f}" if candidate_score is not None else "—"
        decorated["talos_score_band_label"] = score_band["label"]
        decorated["talos_score_band_key"] = score_band["key"]
        decorated["talos_entry_ready"] = bool(candidate_score is not None and candidate_score >= self.ENTRY_SCORE_MINIMUM)
        decorated["talos_entry_readiness_label"] = "Enterable" if decorated["talos_entry_ready"] else "Do Not Enter"
        decorated["talos_score_breakdown_items"] = self._build_score_breakdown_items(score_breakdown)
        decorated["talos_pricing_basis_label"] = talos_metadata.get("pricing_basis_label") or "Conservative executable fills"
        decorated["talos_pricing_math_display"] = talos_metadata.get("pricing_math_display") or "Entry uses short bid - long ask. Exit uses short ask - long bid."
        decorated["talos_entry_gate_label"] = talos_metadata.get("entry_gate_label") or "Talos accepted a qualified system candidate."
        if not decorated.get("percent_credit_captured_display"):
            captured = self._coerce_float(decorated.get("percent_credit_captured"))
            decorated["percent_credit_captured_display"] = self._format_percent_value(captured) if captured is not None else "—"
        decorated["talos_score_explanation_lines"] = self._build_score_explanation_lines(score_breakdown, talos_metadata)
        return decorated

    def _score_open_position_record(
        self,
        record: Dict[str, Any],
        trade: Dict[str, Any] | None,
        talos_metadata: Dict[str, Any],
        *,
        learning_state: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        system_name = normalize_system_name((trade or {}).get("system_name") or record.get("system_name"))
        candidate_profile = str((trade or {}).get("candidate_profile") or record.get("profile_label") or "Standard")
        vix_value = self._coerce_float((trade or {}).get("vix_at_entry") or record.get("vix_at_entry"))
        current_expected_move = self._coerce_float(record.get("current_live_expected_move"))
        distance_points = max(float(record.get("distance_to_short") or 0.0), 0.0)
        em_multiple = self._coerce_float(record.get("current_em_multiple"))
        if em_multiple is None:
            em_multiple = self._coerce_float(record.get("actual_em_multiple")) or self._coerce_float((trade or {}).get("actual_em_multiple"))
        short_delta = self._coerce_float(record.get("short_delta"))
        if short_delta is None:
            short_delta = self._coerce_float((trade or {}).get("short_delta"))
        conservative_open_credit = self._coerce_float((trade or {}).get("actual_entry_credit"))
        if conservative_open_credit is None:
            conservative_open_credit = self._coerce_float(record.get("net_credit_per_contract"))
        spread_width = self._coerce_float((trade or {}).get("spread_width"))
        if spread_width is None:
            spread_width = self._coerce_float((record.get("max_loss") or 0.0) / 100.0)
        contracts = int((trade or {}).get("contracts") or record.get("contracts") or 0)
        total_premium = self._coerce_float((trade or {}).get("total_premium"))
        if total_premium is None and conservative_open_credit is not None and contracts > 0:
            total_premium = conservative_open_credit * contracts * 100.0
        fit_component = self._fit_score(
            system_name=system_name,
            structure_context=record.get("current_structure_grade") or (trade or {}).get("structure_grade"),
            macro_context=record.get("thesis_status") or (trade or {}).get("macro_grade"),
        ) * 10.0
        history = self._build_history_profile(system_name, candidate_profile, vix_value=vix_value, learning_state=learning_state)
        learning_profile = self._resolve_learning_profile(learning_state, system_name=system_name, vix_value=vix_value)
        weights = self._normalize_component_weights((learning_profile or {}).get("weights"))
        base_safety_component = self._score_safety_component(
            system_name=system_name,
            em_multiple=em_multiple,
            distance_points=distance_points,
            expected_move=current_expected_move,
            short_delta=short_delta,
        )
        base_credit_component = self._score_credit_efficiency_component(conservative_open_credit, spread_width)
        base_profit_component = self._score_profit_opportunity_component(total_premium)
        base_standard_preference_component = self._score_standard_preference_component(system_name=system_name, candidate_profile=candidate_profile)
        safety_component = self._scale_component(base_safety_component, default_weight=32.0, adaptive_weight=weights["safety"])
        history_component = self._scale_component(history["history_component"], default_weight=22.0, adaptive_weight=weights["history"])
        credit_component = self._scale_component(base_credit_component, default_weight=14.0, adaptive_weight=weights["credit"])
        profit_component = self._scale_component(base_profit_component, default_weight=12.0, adaptive_weight=weights["profit"])
        fit_component = self._scale_component(fit_component, default_weight=10.0, adaptive_weight=weights["fit"])
        standard_preference_component = self._scale_component(base_standard_preference_component, default_weight=10.0, adaptive_weight=weights["standard_preference"])
        loss_penalty_component = self._scale_component(history["loss_penalty_component"], default_weight=20.0, adaptive_weight=weights["loss_penalty"])
        candidate_score = max(
            0.0,
            min(
                100.0,
                round(
                    safety_component
                    + history_component
                    + credit_component
                    + profit_component
                    + fit_component
                    + standard_preference_component
                    - loss_penalty_component,
                    2,
                ),
            ),
        )
        return {
            "candidate_score": candidate_score,
            "safety_component": round(safety_component, 2),
            "history_component": round(history_component, 2),
            "credit_component": round(credit_component, 2),
            "profit_component": round(profit_component, 2),
            "fit_component": round(fit_component, 2),
            "loss_penalty_component": round(loss_penalty_component, 2),
            "history_scope": history["scope"],
            "history_sample_count": history["sample_count"],
            "segment_name": history["segment_name"],
            "adaptive_enabled": history["adaptive_enabled"],
            "adaptive_weights": weights,
            "win_rate_component": round(history["win_rate_component"], 2),
            "expectancy_component": round(history["expectancy_component"], 2),
            "sample_confidence_component": round(history["sample_confidence_component"], 2),
            "distance_component": round(self._score_distance_component(system_name, distance_points, current_expected_move, em_multiple), 2),
            "em_multiple_component": round(self._score_em_multiple_component(em_multiple), 2),
            "short_delta_component": round(self._score_short_delta_component(short_delta), 2),
            "score_breakdown_display": (
                f"Score {candidate_score:.1f} = Safety {safety_component:.1f} + History {history_component:.1f} + "
                f"Credit {credit_component:.1f} + Profit {profit_component:.1f} + Fit {fit_component:.1f} + Standard Preference {standard_preference_component:.1f} - "
                f"Loss Penalty {loss_penalty_component:.1f}."
            ),
        }

    def _build_score_explanation_lines(self, score_breakdown: Dict[str, Any], talos_metadata: Dict[str, Any]) -> List[str]:
        lines: List[str] = []
        if talos_metadata.get("selection_basis"):
            lines.append(str(talos_metadata.get("selection_basis")))
        if talos_metadata.get("decision_note"):
            lines.append(str(talos_metadata.get("decision_note")))
        if talos_metadata.get("pricing_math_display"):
            lines.append(f"Pricing: {talos_metadata.get('pricing_math_display')}")
        if score_breakdown:
            segment_name = str(score_breakdown.get("segment_name") or "")
            if segment_name:
                mode_label = "adaptive" if score_breakdown.get("adaptive_enabled") else "fallback"
                lines.append(f"Learning segment: {segment_name} ({mode_label}).")
            lines.append(
                "Components: "
                f"Safety {float(score_breakdown.get('safety_component') or 0.0):.1f}, "
                f"History {float(score_breakdown.get('history_component') or 0.0):.1f}, "
                f"Credit {float(score_breakdown.get('credit_component') or 0.0):.1f}, "
                f"Profit {float(score_breakdown.get('profit_component') or 0.0):.1f}, "
                f"Fit {float(score_breakdown.get('fit_component') or 0.0):.1f}, "
                f"Standard Preference {float(score_breakdown.get('standard_preference_component') or 0.0):.1f}, "
                f"Loss Penalty {float(score_breakdown.get('loss_penalty_component') or 0.0):.1f}."
            )
        return lines

    def _build_score_breakdown_items(self, score_breakdown: Dict[str, Any]) -> List[Dict[str, str]]:
        component_specs = (
            ("Safety", "safety_component"),
            ("History", "history_component"),
            ("Credit", "credit_component"),
            ("Profit", "profit_component"),
            ("Fit", "fit_component"),
            ("Standard Preference", "standard_preference_component"),
            ("Loss Penalty", "loss_penalty_component"),
        )
        items: List[Dict[str, str]] = []
        for label, key in component_specs:
            value = self._coerce_float(score_breakdown.get(key))
            if value is None:
                continue
            prefix = "-" if key == "loss_penalty_component" else "+"
            items.append({"label": label, "value": f"{prefix}{value:.1f}"})
        return items

    def _build_skip_log_summary(self, skip_log: List[Dict[str, Any]]) -> List[Dict[str, str]]:
        summary: List[Dict[str, str]] = []
        for item in skip_log[:5]:
            repeat_count = max(int(item.get("repeat_count") or 1), 1)
            label = str(item.get("reason") or "Talos skip")
            if repeat_count > 1:
                label = f"{label} x{repeat_count}"
            summary.append(
                {
                    "system": str(item.get("system") or "Talos"),
                    "label": label,
                    "timestamp": self._format_datetime(item.get("timestamp") or item.get("latest_timestamp") or ""),
                }
            )
        return summary

    def _build_decision_model_payload(self, open_records: List[Dict[str, Any]], learning_state: Dict[str, Any]) -> Dict[str, Any]:
        example_record = next((item for item in open_records if item.get("talos_score_breakdown_items")), None)
        learning_payload = self._build_learning_state_payload(learning_state)
        current_weights = learning_payload["current_weights"]
        active_segment = learning_payload["active_segment"]
        return {
            "formula": (
                f"Score = safety {current_weights['safety']:.0f} + history {current_weights['history']:.0f} + "
                f"credit {current_weights['credit']:.0f} + profit {current_weights['profit']:.0f} + "
                f"fit {current_weights['fit']:.0f} + standard preference {current_weights['standard_preference']:.0f} - "
                f"loss penalty {current_weights['loss_penalty']:.0f}."
            ),
            "minimum_entry_score": self.ENTRY_SCORE_MINIMUM,
            "score_policy": "Talos scores every candidate on a 1-100 scale and only enters trades that score 70 or higher.",
            "active_segment": active_segment,
            "bands": [
                {"minimum": minimum, "label": label, "key": key}
                for minimum, label, key in self.SCORE_BANDS
            ],
            "weights": [
                f"Safety {current_weights['safety']:.0f}: expected-move spacing plus raw distance to the short strike.",
                f"History {current_weights['history']:.0f}: segmented win rate, expectancy, and sample confidence.",
                f"Credit {current_weights['credit']:.0f}: credit earned per width of risk and credit-efficiency signal.",
                f"Profit {current_weights['profit']:.0f}: absolute premium opportunity against Talos's Apollo daily premium target.",
                f"Fit {current_weights['fit']:.0f}: current structure and timing alignment when fit remains meaningful.",
                f"Standard Preference {current_weights['standard_preference']:.0f}: deliberate bias toward Standard-led Apollo portfolios when quality is comparable.",
                f"Loss penalty {current_weights['loss_penalty']:.0f}: segmented drawdown severity plus black-swan frequency.",
            ],
            "pricing_rule": "Simulated entry fill = short bid - long ask. Simulated exit fill = short ask - long bid.",
            "rotation_rule": "Talos only rotates after more than 75% of credit is captured, a replacement candidate is available, and the replacement score is equal or better.",
            "capture_close_rule": f"Talos automatically liquidates any open Talos position once credit captured reaches {self.TALOS_CAPTURE_LIQUIDATION_THRESHOLD:.1f}% or greater.",
            "apollo_portfolio_rule": "Talos actively builds up to three Apollo positions from the current live state while keeping projected Black Swan loss under 10% of settled balance and using the daily premium target as a scoring objective rather than a gate.",
            "example_record": example_record,
        }

    def _build_runtime_status_payload(self, state: Dict[str, Any]) -> Dict[str, str]:
        if bool(state.get("paused")):
            return {"label": "Paused", "key": "paused"}
        if bool(state.get("scan_engine_running")):
            return {"label": "Live Monitor Running", "key": "running"}
        last_cycle_status = str(state.get("last_cycle_status") or "").strip().lower()
        if last_cycle_status == "post-close-final-pass":
            return {"label": "Final Post-Close Pass Complete", "key": "final-pass"}
        if last_cycle_status == "outside-market-hours":
            return {"label": "Waiting For Market Window", "key": "waiting"}
        if last_cycle_status == "completed":
            return {"label": "Ready For Next Scan", "key": "ready"}
        return {"label": "Idle", "key": "idle"}

    def _refresh_runtime_schedule_state(self, state: Dict[str, Any], *, reference_time: datetime) -> None:
        state["scan_engine_running"] = bool(self._monitor_running)
        next_run_at = self._resolve_next_background_run_at(reference_time)
        state["next_scan_at"] = next_run_at.isoformat() if next_run_at is not None else ""

    def _latest_skip_reason_for_system(self, state: Dict[str, Any], system_name: str, *, timestamp: str) -> str | None:
        for item in (state.get("skip_log") or []):
            if not isinstance(item, dict):
                continue
            if str(item.get("system") or "").strip() != system_name:
                continue
            item_timestamp = str(item.get("latest_timestamp") or item.get("timestamp") or "").strip()
            if item_timestamp != timestamp:
                continue
            reason = str(item.get("reason") or "").strip()
            return reason or None
        return None

    def _append_post_management_follow_through_activity(
        self,
        state: Dict[str, Any],
        *,
        cycle_time: datetime,
        apollo_action_taken: bool,
    ) -> None:
        next_scan_display = self._format_datetime(state.get("next_scan_at"))
        detail_parts: List[str] = [
            f"Talos resumed scanning after the close and scheduled the next scan for {next_scan_display}."
        ]
        opened_systems: List[str] = []
        if apollo_action_taken:
            opened_systems.append("Apollo")
        if opened_systems:
            detail_parts.append(f"New {' and '.join(opened_systems)} entry activity was permitted in the same session.")
        else:
            timestamp = cycle_time.isoformat()
            reasons: List[str] = []
            apollo_reason = self._latest_skip_reason_for_system(state, "Apollo", timestamp=timestamp)
            if apollo_reason:
                reasons.append(f"Apollo: {apollo_reason}")
            if reasons:
                detail_parts.append("; ".join(reasons))
            else:
                detail_parts.append("No new Talos entry qualified during this follow-through pass.")
        self._append_activity(
            state,
            {
                "timestamp": cycle_time.isoformat(),
                "category": "runtime",
                "title": "Talos runtime re-armed",
                "detail": " ".join(detail_parts),
            },
        )

    def _build_formula_chart_payload(self, learning_state: Dict[str, Any]) -> Dict[str, Any]:
        learning_payload = self._build_learning_state_payload(learning_state)
        current_weights = learning_payload["current_weights"]
        segments = [
            {"label": "Safety", "weight": current_weights["safety"], "label_display": f"Safety {current_weights['safety']:.0f}", "color": "#ef4444", "shade_color": "#b91c1c"},
            {"label": "History", "weight": current_weights["history"], "label_display": f"History {current_weights['history']:.0f}", "color": "#f97316", "shade_color": "#c2410c"},
            {"label": "Credit", "weight": current_weights["credit"], "label_display": f"Credit {current_weights['credit']:.0f}", "color": "#eab308", "shade_color": "#a16207"},
            {"label": "Profit", "weight": current_weights["profit"], "label_display": f"Profit {current_weights['profit']:.0f}", "color": "#22c55e", "shade_color": "#15803d"},
            {"label": "Fit", "weight": current_weights["fit"], "label_display": f"Fit {current_weights['fit']:.0f}", "color": "#3b82f6", "shade_color": "#1d4ed8"},
            {"label": "Standard", "weight": current_weights["standard_preference"], "label_display": f"Standard {current_weights['standard_preference']:.0f}", "color": "#8b5cf6", "shade_color": "#6d28d9"},
        ]
        positive_total = max(sum(float(item["weight"]) for item in segments), 1.0)
        cursor = 0.0
        gradients: List[str] = []
        for item in segments:
            sweep = (item["weight"] / positive_total) * 360.0
            gradients.append(f"{item['color']} {cursor:.2f}deg {cursor + sweep:.2f}deg")
            cursor += sweep
        return {
            "chart_style": f"conic-gradient({', '.join(gradients)})",
            "segments": [
                {
                    "label": item["label"],
                    "label_display": item["label_display"],
                    "weight": item["weight"],
                    "weight_display": f"{item['weight']:.0f}%",
                    "color": item["color"],
                    "shade_color": item["shade_color"],
                }
                for item in segments
            ],
            "penalty_label": "Loss Penalty",
            "penalty_weight_display": f"{current_weights['loss_penalty']:.0f} penalty",
            "penalty_note": f"Loss penalty is applied separately for the active segment {learning_payload['active_segment']} rather than folded into the positive-weight pie.",
        }

    def _build_current_commitment_payload(self, open_records: List[Dict[str, Any]], balance: Dict[str, float]) -> Dict[str, Any]:
        black_swan_roi = self._average_black_swan_roi()
        commitment_value = 0.0
        for item in open_records:
            commitment_value += self._projected_black_swan_loss_for_record(item, black_swan_roi)
        gauge_max = max(float(balance.get("current_account_value") or 0.0), 1.0)
        ratio = max(0.0, min(commitment_value / gauge_max, 1.0))
        return {
            "value": round(commitment_value, 2),
            "value_display": self._format_currency(commitment_value),
            "gauge_max": round(gauge_max, 2),
            "gauge_max_display": self._format_currency(gauge_max),
            "ratio": round(ratio, 4),
            "basis": "projected_black_swan_loss",
            "black_swan_roi_used": black_swan_roi,
            "black_swan_roi_used_display": self._format_percent_value(black_swan_roi),
        }

    def _build_performance_metrics_payload(self) -> Dict[str, Any]:
        talos_records = [build_performance_record(trade) for trade in self.trade_store.list_trades("talos")]
        outcomes = summarize_outcomes(talos_records)
        total_trades = len(talos_records)
        open_trades = len(outcomes.open_records)
        closed_trades = len(outcomes.closed_records)
        win_count = len(outcomes.wins)
        loss_count = len(outcomes.losses)
        black_swan_count = len(outcomes.black_swans)
        total_loss_outcomes = len(outcomes.loss_events)
        scored_outcomes = len(outcomes.scored_outcomes)
        win_rate_ratio = (win_count / scored_outcomes) if scored_outcomes else 0.0
        loss_rate_ratio = (total_loss_outcomes / scored_outcomes) if scored_outcomes else 0.0
        average_win = self._average(record.get("gross_pnl") for record in outcomes.wins)
        average_loss = abs(self._average(record.get("gross_pnl") for record in outcomes.losses))
        average_black_swan_loss = abs(self._average(record.get("gross_pnl") for record in outcomes.black_swans))
        expectancy_average_loss = abs(self._average(record.get("gross_pnl") for record in outcomes.loss_events))
        expectancy = calculate_expectancy(
            win_rate=win_rate_ratio,
            average_win=average_win,
            loss_rate=loss_rate_ratio,
            average_loss=expectancy_average_loss,
        )
        expectancy_scale = max(50.0, abs(expectancy) * 1.75, average_win, average_loss, average_black_swan_loss)
        net_pnl = sum(float(record.get("gross_pnl") or 0.0) for record in talos_records)
        closed_outcomes = win_count + loss_count + black_swan_count
        return {
            "totals": {
                "total_trades": total_trades,
                "open_trades": open_trades,
                "closed_trades": closed_trades,
            },
            "win_rate": {
                "value": round(win_rate_ratio * 100.0, 2),
                "ratio": round(win_rate_ratio, 4),
                "wins": win_count,
                "losses": total_loss_outcomes,
            },
            "expectancy": {
                "value": round(expectancy, 2),
                "scale": round(expectancy_scale, 2),
            },
            "net_pnl": {
                "value": round(net_pnl, 2),
            },
            "outcome_mix": {
                "win_percent": round((win_count / closed_outcomes) * 100.0, 2) if closed_outcomes else 0.0,
                "loss_percent": round((loss_count / closed_outcomes) * 100.0, 2) if closed_outcomes else 0.0,
                "black_swan_percent": round((black_swan_count / closed_outcomes) * 100.0, 2) if closed_outcomes else 0.0,
                "win_count": win_count,
                "loss_count": loss_count,
                "black_swan_count": black_swan_count,
                "closed_outcomes": closed_outcomes,
                "source": "Talos closed outcomes only",
            },
            "average_win_loss": {
                "average_win": round(average_win, 2),
                "average_loss": round(average_loss, 2),
                "average_black_swan_loss": round(average_black_swan_loss, 2),
                "ratio": round((average_win / average_loss), 2) if average_loss else None,
                "win_count": win_count,
                "loss_count": loss_count,
                "black_swan_count": black_swan_count,
            },
        }

    def _build_learning_state_payload(self, learning_state: Dict[str, Any]) -> Dict[str, Any]:
        profiles = dict((learning_state or {}).get("profiles") or {})
        active_segment = str((learning_state or {}).get("active_segment") or "Apollo_LOW_VIX")
        active_profile = dict(profiles.get(active_segment) or {})
        current_weights = self._normalize_component_weights(active_profile.get("weights"))
        return {
            "active_segment": active_segment,
            "current_weights": current_weights,
            "dataset_size": int(active_profile.get("trade_count") or 0),
            "weighted_dataset_size": round(float(active_profile.get("weighted_trade_count") or 0.0), 2),
            "win_rate": round(float(active_profile.get("weighted_win_rate") or 0.0) * 100.0, 2),
            "black_swan_rate": round(float(active_profile.get("weighted_black_swan_rate") or 0.0) * 100.0, 2),
            "adaptive_label": "Adaptive" if active_profile.get("adaptive_enabled") else "Fallback",
            "adaptive_enabled": bool(active_profile.get("adaptive_enabled")),
            "profiles": profiles,
        }

    def _build_settled_equity_curve_payload(self, state: Dict[str, Any]) -> Dict[str, Any]:
        closed_trades = []
        for trade in self.trade_store.list_trades("talos"):
            status = str(trade.get("derived_status_raw") or trade.get("status") or "").strip().lower()
            if status in {"open", "reduced"}:
                continue
            closed_trades.append(trade)
        closed_trades.sort(key=lambda trade: str(trade.get("exit_datetime") or trade.get("entry_datetime") or ""))
        starting_balance = float(state.get("starting_balance") or self.STARTING_BALANCE)
        values = [starting_balance]
        labels = ["Start"]
        cumulative = starting_balance
        for trade in closed_trades:
            cumulative += float(trade.get("gross_pnl") or 0.0)
            values.append(round(cumulative, 2))
            labels.append(str(trade.get("trade_number") or len(labels)))
        width = 640
        height = 220
        padding = 24
        if len(values) == 1:
            polyline_points = f"{padding:.2f},{height / 2.0:.2f} {width - padding:.2f},{height / 2.0:.2f}"
            return {
                "polyline_points": polyline_points,
                "area_points": f"{padding:.2f},{height - padding:.2f} {polyline_points} {width - padding:.2f},{height - padding:.2f}",
                "markers": [{"label": labels[0], "x": padding, "y": height / 2.0, "value_display": self._format_currency(values[0])}],
                "start_value_display": self._format_currency(values[0]),
                "end_value_display": self._format_currency(values[-1]),
                "closed_trade_count": len(closed_trades),
            }
        minimum = min(values)
        maximum = max(values)
        span = max(maximum - minimum, 1.0)
        x_step = (width - (padding * 2)) / max(len(values) - 1, 1)
        polyline_pairs: List[str] = []
        markers: List[Dict[str, Any]] = []
        for index, value in enumerate(values):
            x_value = padding + (x_step * index)
            y_value = height - padding - (((value - minimum) / span) * (height - (padding * 2)))
            polyline_pairs.append(f"{x_value:.2f},{y_value:.2f}")
            markers.append({"label": labels[index], "x": x_value, "y": y_value, "value_display": self._format_currency(value)})
        return {
            "polyline_points": " ".join(polyline_pairs),
            "area_points": f"{padding:.2f},{height - padding:.2f} {' '.join(polyline_pairs)} {width - padding:.2f},{height - padding:.2f}",
            "markers": markers,
            "start_value_display": self._format_currency(values[0]),
            "end_value_display": self._format_currency(values[-1]),
            "closed_trade_count": len(closed_trades),
        }

    def _score_band_for_value(self, value: float | None) -> Dict[str, Any]:
        numeric_value = float(value or 0.0)
        for minimum, label, key in self.SCORE_BANDS:
            if numeric_value >= minimum:
                return {"minimum": minimum, "label": label, "key": key}
        minimum, label, key = self.SCORE_BANDS[-1]
        return {"minimum": minimum, "label": label, "key": key}

    def _build_slot_statuses(self, open_records: List[Dict[str, Any]], metadata_map: Dict[str, Any]) -> List[Dict[str, Any]]:
        statuses: List[Dict[str, Any]] = []
        records_by_system: Dict[str, List[Dict[str, Any]]] = {system_name: [] for system_name in self.SLOT_ORDER}
        for item in open_records:
            records_by_system.setdefault(normalize_system_name(item.get("system_name")), []).append(item)
        for system_name in self.SLOT_ORDER:
            system_records = records_by_system.get(system_name) or []
            record = system_records[0] if system_records else None
            metadata = metadata_map.get(str((record or {}).get("trade_id") or ""), {}) if record else {}
            statuses.append(
                {
                    "system_name": system_name,
                    "occupied": record is not None,
                    "active_trade_count": len(system_records),
                    "trade_number": (record or {}).get("trade_number"),
                    "status": (record or {}).get("status") or "Idle",
                    "candidate_score": metadata.get("candidate_score"),
                    "score_breakdown_display": metadata.get("score_breakdown_display") or "",
                    "pricing_basis_label": metadata.get("pricing_basis_label") or "",
                    "decision_note": metadata.get("decision_note") or "No active Talos slot.",
                }
            )
        return statuses

    def _reconcile_open_trade_governance(self, state: Dict[str, Any], open_records: List[Dict[str, Any]], *, source_label: str) -> bool:
        records_by_system: Dict[str, List[Dict[str, Any]]] = {}
        for record in open_records:
            records_by_system.setdefault(normalize_system_name(record.get("system_name")), []).append(record)
        for system_name, records in records_by_system.items():
            allowed = int(self.MAX_OPEN_TRADES_PER_SYSTEM.get(system_name, 1))
            if len(records) <= allowed:
                continue
            self._append_skip(
                state,
                {
                    "timestamp": self._now().isoformat(),
                    "system": system_name,
                    "reason": (
                        f"Talos governance detected {len(records)} open {system_name} positions during {source_label}, "
                        "but did not auto-close any trade because gate or rotation rules were not satisfied."
                    ),
                },
            )
        if len(open_records) > self.MAX_TOTAL_OPEN_TRADES:
            self._append_skip(
                state,
                {
                    "timestamp": self._now().isoformat(),
                    "system": "Talos",
                    "reason": (
                        f"Talos governance detected {len(open_records)} open positions during {source_label}, "
                        "but did not auto-close any trade because gate or rotation rules were not satisfied."
                    ),
                },
            )
        return False

    def _resolve_conservative_exit_value(self, record: Dict[str, Any]) -> float:
        current_spread_mark = self._coerce_float(record.get("current_spread_mark"))
        if current_spread_mark is not None:
            return round(max(current_spread_mark, 0.0), 4)
        return 0.0

    def _record_sort_key(self, record: Dict[str, Any]) -> tuple[float, int, int]:
        entry_timestamp = self._timestamp_sort_value(record.get("entry_datetime") or record.get("opened_at") or record.get("last_updated"))
        trade_number = int(record.get("trade_number") or 0)
        trade_id = int(record.get("trade_id") or record.get("id") or 0)
        return (entry_timestamp, trade_number, trade_id)

    def _timestamp_sort_value(self, value: Any) -> float:
        if not value:
            return 0.0
        try:
            stamp = datetime.fromisoformat(str(value))
        except (TypeError, ValueError):
            return 0.0
        if stamp.tzinfo is None:
            stamp = stamp.replace(tzinfo=self.display_timezone)
        return stamp.timestamp()

    def _manage_open_trades(self, state: Dict[str, Any], open_records: List[Dict[str, Any]]) -> bool:
        now = self._now()
        action_taken = False
        for record in open_records:
            trade_id = int(record.get("trade_id") or 0)
            if trade_id <= 0:
                continue
            current_spread_mark = record.get("current_spread_mark")
            captured = self._coerce_float(record.get("percent_credit_captured")) or 0.0
            expiration_date = parse_date_value(record.get("expiration") or record.get("expiration_date"))
            if captured >= self.TALOS_CAPTURE_LIQUIDATION_THRESHOLD:
                if self._is_close_restricted_window(now):
                    self._append_skip(
                        state,
                        {
                            "timestamp": now.isoformat(),
                            "system": str(record.get("system_name") or "Talos"),
                            "reason": (
                                f"Talos blocked the {self.TALOS_CAPTURE_LIQUIDATION_THRESHOLD:.0f}% capture liquidation for Trade #{record.get('trade_number')} because no Talos close is allowed "
                                f"during the first {self.NO_CLOSE_AFTER_OPEN_MINUTES} minutes after the market opens."
                            ),
                        },
                    )
                    continue
                contracts_to_close = int(record.get("contracts") or record.get("remaining_contracts") or 0)
                if current_spread_mark in {None, ""} or contracts_to_close <= 0:
                    self._append_skip(
                        state,
                        {
                            "timestamp": now.isoformat(),
                            "system": str(record.get("system_name") or "Talos"),
                            "reason": (
                                f"Talos blocked the {self.TALOS_CAPTURE_LIQUIDATION_THRESHOLD:.0f}% capture liquidation for Trade #{record.get('trade_number')} because conservative exit pricing or remaining quantity was unavailable."
                            ),
                        },
                    )
                    continue
                actual_exit_value = self._resolve_conservative_exit_value(record)
                total_premium = self._coerce_float(record.get("total_premium")) or 0.0
                remaining_premium = self._coerce_float(record.get("current_total_close_cost")) or 0.0
                self.trade_store.reduce_trade(
                    trade_id,
                    {
                        "contracts_closed": contracts_to_close,
                        "actual_exit_value": actual_exit_value,
                        "event_datetime": current_timestamp(),
                        "close_method": "Close",
                        "close_reason": f"Talos {self.TALOS_CAPTURE_LIQUIDATION_THRESHOLD:.0f}% credit-capture liquidation",
                        "notes_exit": (
                            f"Talos automatically liquidated this trade after credit capture reached {captured:.1f}% of original premium. "
                            f"Original premium {self._format_currency(total_premium)}; remaining premium {self._format_currency(remaining_premium)}; "
                            f"conservative simulated exit debit {actual_exit_value:.2f} per contract."
                        ),
                    },
                )
                self._append_activity(
                    state,
                    {
                        "timestamp": now.isoformat(),
                        "category": "capture-close",
                        "title": f"Talos auto-liquidated {record.get('system_name')}",
                        "detail": (
                            f"Talos closed Trade #{record.get('trade_number')} after credit capture reached {captured:.1f}% of original premium "
                            f"({self._format_currency(total_premium)} original, {self._format_currency(remaining_premium)} remaining)."
                        ),
                    },
                )
                metadata = dict((state.get("trade_metadata") or {}).get(str(trade_id), {}))
                self._record_talos_decision(
                    state,
                    system_name=str(record.get("system_name") or "Talos"),
                    decision="exit",
                    reason_text=(
                        f"Talos liquidated Trade #{record.get('trade_number')} because credit captured reached {captured:.1f}% of original premium."
                    ),
                    score_breakdown=dict(metadata.get("score_breakdown") or {}),
                    weights=metadata.get("adaptive_weights"),
                )
                action_taken = True
                continue
            due_gate = self._first_due_exit_gate(record)
            if due_gate is not None:
                if self._is_close_restricted_window(now):
                    self._append_skip(
                        state,
                        {
                            "timestamp": now.isoformat(),
                            "system": str(record.get("system_name") or "Talos"),
                            "reason": (
                                f"Talos blocked close for Trade #{record.get('trade_number')} because no Talos close is allowed "
                                f"during the first {self.NO_CLOSE_AFTER_OPEN_MINUTES} minutes after the market opens."
                            ),
                        },
                    )
                    continue
                contracts_to_close = min(
                    int(due_gate.get("quantity_to_close") or 0),
                    int(record.get("contracts") or record.get("remaining_contracts") or 0),
                )
                if current_spread_mark in {None, ""} or contracts_to_close <= 0:
                    self._append_skip(
                        state,
                        {
                            "timestamp": now.isoformat(),
                            "system": str(record.get("system_name") or "Talos"),
                            "reason": (
                                f"Talos blocked close for Trade #{record.get('trade_number')} because gate {due_gate.get('label') or 'pending gate'} "
                                "was due but conservative exit pricing or gate quantity was unavailable."
                            ),
                        },
                    )
                    continue
                close_method = "Close" if contracts_to_close >= int(record.get("contracts") or 0) else "Reduce"
                actual_exit_value = self._resolve_conservative_exit_value(record)
                self.trade_store.reduce_trade(
                    trade_id,
                    {
                        "contracts_closed": contracts_to_close,
                        "actual_exit_value": actual_exit_value,
                        "event_datetime": current_timestamp(),
                        "close_method": close_method,
                        "close_reason": f"Talos gate-based close: {due_gate.get('label') or 'exit gate'}",
                        "notes_exit": (
                            f"Talos {close_method.lower()} executed the due exit gate {due_gate.get('label') or 'exit gate'} at "
                            f"{record.get('evaluated_at_display') or self._format_datetime(now)} for exactly {contracts_to_close} contract(s). "
                            f"Conservative simulated exit debit {actual_exit_value:.2f} per contract."
                        ),
                    },
                )
                self._append_activity(
                    state,
                    {
                        "timestamp": now.isoformat(),
                        "category": "gate-close",
                        "title": f"Talos gate close {record.get('system_name')}",
                        "detail": (
                            f"Trade #{record.get('trade_number')} closed {contracts_to_close} contract(s) from gate "
                            f"{due_gate.get('label') or 'exit gate'}."
                        ),
                    },
                )
                metadata = dict((state.get("trade_metadata") or {}).get(str(trade_id), {}))
                self._record_talos_decision(
                    state,
                    system_name=str(record.get("system_name") or "Talos"),
                    decision="exit",
                    reason_text=f"Gate {due_gate.get('label') or 'exit gate'} triggered for Trade #{record.get('trade_number')}.",
                    score_breakdown=dict(metadata.get("score_breakdown") or {}),
                    weights=metadata.get("adaptive_weights"),
                )
                action_taken = True
                continue
            expiration_passed = expiration_date is not None and expiration_date < now.date()
            expiration_reached_close = expiration_date is not None and expiration_date == now.date() and now.time() >= self.MARKET_CLOSE
            if expiration_passed or expiration_reached_close:
                self.trade_store.expire_trade(
                    trade_id,
                    {
                        "event_datetime": current_timestamp(),
                        "actual_exit_value": 0.0,
                        "close_reason": "Talos expiration sweep",
                        "notes_exit": "Talos expired the remaining contracts because expiration has passed or the regular market session has closed on expiration day.",
                    },
                )
                self._append_activity(
                    state,
                    {
                        "timestamp": now.isoformat(),
                        "category": "expiration",
                        "title": "Talos expiration",
                        "detail": f"Trade #{record.get('trade_number')} {record.get('system_name')} expired because its expiration date has passed or the market has closed on expiration day.",
                    },
                )
                metadata = dict((state.get("trade_metadata") or {}).get(str(trade_id), {}))
                self._record_talos_decision(
                    state,
                    system_name=str(record.get("system_name") or "Talos"),
                    decision="expire",
                    reason_text=f"Trade #{record.get('trade_number')} expired because its expiration date has passed or the market has closed on expiration day.",
                    score_breakdown=dict(metadata.get("score_breakdown") or {}),
                    weights=metadata.get("adaptive_weights"),
                )
                action_taken = True
        return action_taken

    def _evaluate_apollo_portfolio(self, state: Dict[str, Any], plan: Dict[str, Any] | None, open_records: List[Dict[str, Any]]) -> bool:
        now = self._now()
        governance = self._build_slot_governance(open_records)
        apollo_open_records = [item for item in open_records if normalize_system_name(item.get("system_name")) == "Apollo"]
        allowed_per_system = int(self.MAX_OPEN_TRADES_PER_SYSTEM.get("Apollo", self.APOLLO_MAX_PORTFOLIO_POSITIONS))
        if isinstance(plan, dict):
            self._append_activity(
                state,
                {
                    "timestamp": now.isoformat(),
                    "category": "apollo-portfolio",
                    "title": "Apollo portfolio review",
                    "detail": self._build_apollo_portfolio_log_line(plan),
                },
            )
        if governance["system_counts"].get("Apollo", 0) > allowed_per_system:
            self._append_skip(
                state,
                {
                    "timestamp": now.isoformat(),
                    "system": "Apollo",
                    "reason": f"Talos governance blocked new Apollo entries because {governance['system_counts'].get('Apollo', 0)} Apollo Talos trades are already open.",
                },
            )
            return False
        if plan is None or not list(plan.get("selected_candidates") or []):
            reason = str((plan or {}).get("selection_note") or "No qualified Apollo portfolio was available.")
            self._append_skip(
                state,
                {
                    "timestamp": now.isoformat(),
                    "system": "Apollo",
                    "reason": reason,
                },
            )
            self._record_talos_decision(
                state,
                system_name="Apollo",
                decision="skip",
                reason_text=reason,
            )
            return False
        selected_candidates = [item for item in (plan.get("selected_candidates") or []) if isinstance(item, dict)]
        next_candidate = self._resolve_next_apollo_execution_candidate(selected_candidates, apollo_open_records)
        if next_candidate is None:
            self._append_skip(
                state,
                {
                    "timestamp": now.isoformat(),
                    "system": "Apollo",
                    "reason": self._build_apollo_no_add_reason(plan, apollo_open_records),
                },
            )
            return False
        if not self._is_apollo_entry_window(now):
            entry_block_reason = (
                "Talos blocked Apollo portfolio entry because Apollo can only open during the final 3 hours of the market day."
            )
            self._append_skip(
                state,
                {
                    "timestamp": now.isoformat(),
                    "system": "Apollo",
                    "reason": entry_block_reason,
                },
            )
            self._record_talos_decision(
                state,
                system_name="Apollo",
                decision="skip",
                reason_text=entry_block_reason,
            )
            return False
        action_taken = False
        for candidate in [next_candidate]:
            live_governance = self._build_slot_governance_from_trade_store()
            if not self._can_open_trade_for_system("Apollo", live_governance):
                self._append_skip(
                    state,
                    {
                        "timestamp": now.isoformat(),
                        "system": "Apollo",
                        "reason": "Talos blocked the Apollo portfolio entry because governance changed before the order was written.",
                    },
                )
                break
            if float(candidate.get("candidate_score") or 0.0) < self.ENTRY_SCORE_MINIMUM:
                reason = (
                    f"Talos rejected the Apollo candidate because its score was {float(candidate.get('candidate_score') or 0.0):.1f}. "
                    f"Talos only enters candidates scoring {self.ENTRY_SCORE_MINIMUM:.0f} or higher."
                )
                self._append_skip(
                    state,
                    {
                        "timestamp": now.isoformat(),
                        "system": "Apollo",
                        "reason": reason,
                    },
                )
                self._record_talos_decision(
                    state,
                    system_name="Apollo",
                    decision="skip",
                    reason_text=reason,
                    score_breakdown=dict(candidate.get("score_breakdown") or {}),
                )
                continue
            management_payload = self.open_trade_manager.evaluate_open_trades(
                send_alerts=False,
                caller_source="job:talos-reset-state",
            ) or {}
            live_open_records = self._filter_talos_records(management_payload) or open_records
            live_apollo_open_records = [item for item in live_open_records if normalize_system_name(item.get("system_name")) == "Apollo"]
            black_swan_cap = round(float(plan.get("projected_black_swan_loss_max") or 0.0), 2)
            black_swan_roi = plan.get("black_swan_roi_used") if isinstance(plan, dict) else None
            current_live_loss = round(
                sum(self._projected_black_swan_loss_for_record(item, black_swan_roi) for item in live_apollo_open_records),
                2,
            )
            candidate_loss = round(
                float(candidate.get("projected_black_swan_loss") or self._projected_black_swan_loss_for_payload(candidate.get("trade_payload") or {}, black_swan_roi)),
                2,
            )
            resulting_live_loss = round(current_live_loss + candidate_loss, 2)
            if black_swan_cap > 0 and current_live_loss >= black_swan_cap:
                reason = self._build_apollo_live_cap_stop_reason(current_live_loss, black_swan_cap)
                self._append_skip(
                    state,
                    {
                        "timestamp": now.isoformat(),
                        "system": "Apollo",
                        "reason": reason,
                    },
                )
                self._record_talos_decision(
                    state,
                    system_name="Apollo",
                    decision="skip",
                    reason_text=reason,
                    score_breakdown=dict(candidate.get("score_breakdown") or {}),
                )
                break
            if black_swan_cap > 0 and resulting_live_loss > black_swan_cap:
                reason = self._build_apollo_live_cap_rejection_reason(
                    current_live_loss=current_live_loss,
                    candidate_loss=candidate_loss,
                    resulting_live_loss=resulting_live_loss,
                    black_swan_cap=black_swan_cap,
                )
                self._append_skip(
                    state,
                    {
                        "timestamp": now.isoformat(),
                        "system": "Apollo",
                        "reason": reason,
                    },
                )
                self._record_talos_decision(
                    state,
                    system_name="Apollo",
                    decision="skip",
                    reason_text=reason,
                    score_breakdown=dict(candidate.get("score_breakdown") or {}),
                )
                continue
            was_empty_before_entry = not live_apollo_open_records
            bootstrap_trade = was_empty_before_entry
            live_count_before_entry = len(live_apollo_open_records)
            action_taken = self._create_trade_from_candidate(state, "Apollo", candidate) or action_taken
            if action_taken:
                remaining_live_capacity = round(max(black_swan_cap - resulting_live_loss, 0.0), 2)
                more_capacity_remains = (
                    (black_swan_cap <= 0 or remaining_live_capacity > 0.0)
                    and (live_count_before_entry + 1) < allowed_per_system
                )
                self._append_activity(
                    state,
                    {
                        "timestamp": now.isoformat(),
                        "category": "apollo-portfolio",
                        "title": "Apollo bootstrap entry" if bootstrap_trade else "Apollo portfolio expanded",
                        "detail": self._build_apollo_addition_log_line(
                            plan,
                            candidate,
                            current_live_loss=current_live_loss,
                            candidate_loss=candidate_loss,
                            resulting_live_loss=resulting_live_loss,
                            black_swan_cap=black_swan_cap,
                            was_empty_before_entry=was_empty_before_entry,
                            bootstrap_trade=bootstrap_trade,
                            remaining_live_capacity=remaining_live_capacity,
                            more_capacity_remains=more_capacity_remains,
                        ),
                    },
                )
                break
        if not action_taken and len(apollo_open_records) >= allowed_per_system:
            self._append_skip(
                state,
                {
                    "timestamp": now.isoformat(),
                    "system": "Apollo",
                    "reason": "Talos identified a preferred Apollo portfolio mix but all Apollo slots are already occupied.",
                },
            )
        return action_taken

    def _resolve_next_apollo_execution_candidate(
        self,
        selected_candidates: List[Dict[str, Any]],
        apollo_open_records: List[Dict[str, Any]],
    ) -> Dict[str, Any] | None:
        current_signatures = {self._apollo_trade_signature(record) for record in apollo_open_records}
        missing_candidates = [
            item
            for item in selected_candidates
            if self._apollo_candidate_signature(item) not in current_signatures
        ]
        if not missing_candidates:
            return None
        return max(
            missing_candidates,
            key=lambda item: (
                round(float(item.get("candidate_score") or 0.0), 4),
                round(float((item.get("trade_payload") or {}).get("total_premium") or 0.0), 4),
                self._apollo_candidate_signature(item),
            ),
        )

    def _evaluate_slot(self, state: Dict[str, Any], system_name: str, candidate: Dict[str, Any] | None, open_records: List[Dict[str, Any]]) -> bool:
        now = self._now()
        governance = self._build_slot_governance(open_records)
        system_records = [item for item in open_records if normalize_system_name(item.get("system_name")) == system_name]
        active_record = system_records[0] if system_records else None
        allowed_per_system = int(self.MAX_OPEN_TRADES_PER_SYSTEM.get(system_name, 1))
        if governance["system_counts"].get(system_name, 0) > allowed_per_system:
            self._append_skip(
                state,
                {
                    "timestamp": now.isoformat(),
                    "system": system_name,
                    "reason": f"Talos governance blocked new {system_name} entries because {governance['system_counts'].get(system_name, 0)} {system_name} Talos trades are already open.",
                },
            )
            return False
        if governance["total_open_count"] > self.MAX_TOTAL_OPEN_TRADES:
            self._append_skip(
                state,
                {
                    "timestamp": now.isoformat(),
                    "system": system_name,
                    "reason": f"Talos governance blocked new entries because {governance['total_open_count']} Talos trades are already open.",
                },
            )
            return False
        if candidate is None:
            if active_record is None and governance["total_open_count"] >= self.MAX_TOTAL_OPEN_TRADES:
                self._append_skip(
                    state,
                    {
                        "timestamp": now.isoformat(),
                        "system": system_name,
                        "reason": "Talos candidate evaluation was skipped because both Talos governance slots are already occupied.",
                    },
                )
                return False
            if active_record is None and governance["system_counts"].get(system_name, 0) >= allowed_per_system:
                self._append_skip(
                    state,
                    {
                        "timestamp": now.isoformat(),
                        "system": system_name,
                        "reason": f"Talos candidate evaluation was skipped because the {system_name} slot is already full.",
                    },
                )
                return False
            self._append_skip(
                state,
                {
                    "timestamp": now.isoformat(),
                    "system": system_name,
                    "reason": f"No qualified {system_name} Talos candidate was available.",
                },
            )
            self._record_talos_decision(
                state,
                system_name=system_name,
                decision="skip",
                reason_text=f"No qualified {system_name} Talos candidate was available.",
            )
            return False

        candidate_score = float(candidate.get("candidate_score") or 0.0)
        if candidate_score < self.ENTRY_SCORE_MINIMUM:
            reason = (
                f"Talos rejected the {system_name} candidate because its score was {candidate_score:.1f}. "
                f"Talos only enters candidates scoring {self.ENTRY_SCORE_MINIMUM:.0f} or higher."
            )
            self._append_skip(
                state,
                {
                    "timestamp": now.isoformat(),
                    "system": system_name,
                    "reason": reason,
                },
            )
            self._record_talos_decision(
                state,
                system_name=system_name,
                decision="skip",
                reason_text=reason,
                score_breakdown=dict(candidate.get("score_breakdown") or {}),
            )
            return False

        if active_record is None:
            if system_name == "Apollo" and not self._is_apollo_entry_window(now):
                self._append_skip(
                    state,
                    {
                        "timestamp": now.isoformat(),
                        "system": system_name,
                        "reason": "Talos blocked Apollo entry because Apollo can only open during the final 3 hours of the market day.",
                    },
                )
                self._record_talos_decision(
                    state,
                    system_name=system_name,
                    decision="skip",
                    reason_text="Talos blocked Apollo entry because Apollo can only open during the final 3 hours of the market day.",
                    score_breakdown=dict(candidate.get("score_breakdown") or {}),
                )
                return False
            return self._create_trade_from_candidate(state, system_name, candidate)

        metadata = (state.get("trade_metadata") or {}).get(str(active_record.get("trade_id") or ""), {})
        previous_score = float(metadata.get("candidate_score") or 0.0)
        current_trade_score = float(active_record.get("talos_candidate_score") or 0.0)
        baseline_score = max(previous_score, current_trade_score)
        captured = float(active_record.get("percent_credit_captured") or 0.0)
        current_spread_mark = active_record.get("current_spread_mark")
        if self._is_close_restricted_window(now):
            self._append_skip(
                state,
                {
                    "timestamp": now.isoformat(),
                    "system": system_name,
                    "reason": (
                        f"Talos blocked close for Trade #{active_record.get('trade_number')} because no Talos close is allowed during the first "
                        f"{self.NO_CLOSE_AFTER_OPEN_MINUTES} minutes after the market opens."
                    ),
                },
            )
            return False
        if system_name == "Apollo" and not self._is_apollo_entry_window(now):
            self._append_skip(
                state,
                {
                    "timestamp": now.isoformat(),
                    "system": system_name,
                    "reason": "Talos blocked Apollo rotation because Apollo can only open during the final 3 hours of the market day.",
                },
            )
            return False
        if captured > self.PROFIT_CAPTURE_ROTATION_THRESHOLD and candidate_score >= baseline_score and current_spread_mark not in {None, ""}:
            contracts_to_close = int(active_record.get("contracts") or 0)
            if contracts_to_close > 0:
                trade_id = int(active_record.get("trade_id") or 0)
                actual_exit_value = self._resolve_conservative_exit_value(active_record)
                self.trade_store.reduce_trade(
                    trade_id,
                    {
                        "contracts_closed": contracts_to_close,
                        "actual_exit_value": actual_exit_value,
                        "event_datetime": current_timestamp(),
                        "close_method": "Close",
                        "close_reason": "Talos rotation-based close",
                        "notes_exit": (
                            f"Talos rotated out of this trade after {captured:.1f}% credit capture when an equal-or-better {system_name} candidate appeared. "
                            f"Conservative simulated exit debit {actual_exit_value:.2f} per contract."
                        ),
                    },
                )
                self._append_activity(
                    state,
                    {
                        "timestamp": now.isoformat(),
                        "category": "rotation",
                        "title": f"Talos rotated {system_name}",
                        "detail": f"Trade #{active_record.get('trade_number')} was closed after {captured:.1f}% credit capture for an equal-or-better {system_name} setup.",
                    },
                )
                self._record_talos_decision(
                    state,
                    system_name=system_name,
                    decision="manage",
                    reason_text=(
                        f"Talos rotated after {captured:.1f}% credit capture because a replacement {system_name} candidate scored {candidate_score:.1f}."
                    ),
                    score_breakdown=dict(candidate.get("score_breakdown") or {}),
                )
                return self._create_trade_from_candidate(state, system_name, candidate)

        reason = (
            f"Talos blocked close for Trade #{active_record.get('trade_number')} because rotation requires more than "
            f"{self.PROFIT_CAPTURE_ROTATION_THRESHOLD:.0f}% credit capture and a replacement score at least as strong as the current slot. "
            f"Current capture {captured:.1f}%; replacement {candidate_score:.1f}; current slot {baseline_score:.1f}."
        )
        self._append_skip(
            state,
            {
                "timestamp": now.isoformat(),
                "system": system_name,
                "reason": reason,
            },
        )
        self._record_talos_decision(
            state,
            system_name=system_name,
            decision="skip",
            reason_text=reason,
            score_breakdown=dict(candidate.get("score_breakdown") or {}),
        )
        return False

    def _first_due_exit_gate(self, record: Dict[str, Any]) -> Dict[str, Any] | None:
        for gate in record.get("exit_plan_gates") or []:
            if gate.get("due_now"):
                return gate
        return None

    def _is_close_restricted_window(self, now: datetime) -> bool:
        local_now = now.astimezone(self.display_timezone)
        market_open_at = self._combine_market_time(local_now.date(), self.MARKET_OPEN)
        restricted_until = market_open_at + timedelta(minutes=self.NO_CLOSE_AFTER_OPEN_MINUTES)
        return market_open_at <= local_now < restricted_until

    def _is_apollo_entry_window(self, now: datetime) -> bool:
        local_now = now.astimezone(self.display_timezone)
        session_close = self._combine_market_time(local_now.date(), self.MARKET_CLOSE)
        session_open = self._combine_market_time(local_now.date(), self.MARKET_OPEN)
        entry_window_open = session_close - timedelta(hours=self.APOLLO_ENTRY_WINDOW_HOURS)
        return session_open <= local_now <= session_close and local_now >= entry_window_open

    def _create_trade_from_candidate(self, state: Dict[str, Any], system_name: str, candidate: Dict[str, Any]) -> bool:
        now = self._now()
        payload = dict(candidate.get("trade_payload") or {})
        if not payload:
            return False
        live_governance = self._build_slot_governance_from_trade_store()
        if not self._can_open_trade_for_system(system_name, live_governance):
            self._append_skip(
                state,
                {
                    "timestamp": now.isoformat(),
                    "system": system_name,
                    "reason": "Talos blocked the autonomous entry because slot governance changed before the order was written.",
                },
            )
            return False
        trade_id = self.trade_store.create_trade(payload)
        trade = self.trade_store.get_trade(trade_id) or {"id": trade_id}
        trade_number = trade.get("trade_number") or trade_id
        score_breakdown = dict(candidate.get("score_breakdown") or {})
        state.setdefault("trade_metadata", {})[str(trade_id)] = {
            "slot_key": system_name.lower(),
            "candidate_score": float(candidate.get("candidate_score") or 0.0),
            "decision_note": str(candidate.get("decision_note") or ""),
            "selection_basis": str(candidate.get("selection_basis") or ""),
            "score_breakdown_display": str(candidate.get("score_breakdown_display") or ""),
            "score_breakdown": score_breakdown,
            "adaptive_weights": dict(score_breakdown.get("adaptive_weights") or {}),
            "pricing_basis_label": str(candidate.get("pricing_basis_label") or ""),
            "pricing_math_display": str(candidate.get("pricing_math_display") or ""),
            "entry_gate_label": str(candidate.get("entry_gate_label") or ""),
            "opened_at": now.isoformat(),
        }
        self._append_activity(
            state,
            {
                "timestamp": now.isoformat(),
                "category": "entry",
                "title": f"Talos opened {system_name}",
                "detail": f"Trade #{trade_number} was opened automatically. {candidate.get('decision_note') or ''}".strip(),
            },
        )
        self._record_talos_decision(
            state,
            system_name=system_name,
            decision="enter",
            reason_text=str(candidate.get("decision_note") or f"Trade #{trade_number} was opened automatically."),
            score_breakdown=score_breakdown,
            weights=score_breakdown.get("adaptive_weights"),
        )
        return True

    def _build_candidates(
        self,
        account_balance: float,
        open_records: List[Dict[str, Any]],
        *,
        learning_state: Dict[str, Any] | None = None,
    ) -> Dict[str, Dict[str, Any] | None]:
        governance = self._build_slot_governance(open_records)
        return {
            "Apollo": self._build_apollo_portfolio_plan(account_balance, open_records, learning_state=learning_state) if self._should_evaluate_candidate("Apollo", governance) else None,
        }

    def _build_apollo_portfolio_plan(
        self,
        account_balance: float,
        open_records: List[Dict[str, Any]],
        *,
        learning_state: Dict[str, Any] | None = None,
    ) -> Dict[str, Any] | None:
        if "_build_apollo_candidate" in self.__dict__:
            legacy_candidate = self._build_apollo_candidate(account_balance)
            if not isinstance(legacy_candidate, dict):
                return self._select_apollo_portfolio([], account_balance=account_balance, open_records=open_records)
            selected_total_premium = float((legacy_candidate.get("trade_payload") or {}).get("total_premium") or 0.0)
            black_swan_roi = self._average_black_swan_roi()
            return {
                "selected_candidates": [legacy_candidate],
                "premium_target": round(max(float(account_balance or 0.0), 1.0) * self.APOLLO_PREMIUM_TARGET_RATIO, 2),
                "selected_total_premium": selected_total_premium,
                "current_total_premium": round(sum(float(item.get("total_premium") or 0.0) for item in open_records if normalize_system_name(item.get("system_name")) == "Apollo"), 2),
                "projected_black_swan_loss_used": self._projected_black_swan_loss_for_payload(legacy_candidate.get("trade_payload") or {}, black_swan_roi),
                "projected_black_swan_loss_current": round(sum(self._projected_black_swan_loss_for_record(item, black_swan_roi) for item in open_records if normalize_system_name(item.get("system_name")) == "Apollo"), 2),
                "projected_black_swan_loss_max": round(max(float(account_balance or 0.0), 1.0) * self.APOLLO_PROJECTED_BLACK_SWAN_LOSS_CAP_RATIO, 2),
                "black_swan_roi_used": black_swan_roi,
                "selected_profile_mix": self._format_profile_mix([str((legacy_candidate.get("trade_payload") or {}).get("candidate_profile") or "Standard")]),
                "current_profile_mix": self._format_profile_mix([str(item.get("profile_label") or item.get("candidate_profile") or "Standard") for item in open_records if normalize_system_name(item.get("system_name")) == "Apollo"]),
                "selection_note": str(legacy_candidate.get("decision_note") or "Legacy Apollo test candidate."),
            }
        candidates = self._build_apollo_candidate_models(account_balance, learning_state=learning_state)
        return self._select_apollo_portfolio(candidates, account_balance=account_balance, open_records=open_records)

    def _build_apollo_candidate(self, account_balance: float) -> Dict[str, Any] | None:
        plan = self._build_apollo_portfolio_plan(account_balance, [], learning_state=dict((self._load_state().get("learning_state") or {})))
        if not plan:
            return None
        selected_candidates = [item for item in (plan.get("selected_candidates") or []) if isinstance(item, dict)]
        return selected_candidates[0] if selected_candidates else None

    def _build_apollo_candidate_models(self, account_balance: float, *, learning_state: Dict[str, Any] | None = None) -> List[Dict[str, Any]]:
        service = ApolloService(
            market_data_service=self.market_data_service,
            options_chain_service=self.options_chain_service,
            config=replace(self.config, apollo_account_value=max(account_balance, 1.0)),
        )
        precheck = service.run_precheck()
        trade_candidates = precheck.get("trade_candidates") or {}
        candidates = [item for item in (trade_candidates.get("candidates") or []) if isinstance(item, dict) and item.get("available")]
        if not candidates:
            return []
        candidate_models: List[Dict[str, Any]] = []
        for item in candidates:
            entry_pricing = self._resolve_apollo_entry_pricing(item)
            contracts = int(item.get("adjusted_contract_size") or item.get("recommended_contract_size") or 1)
            actual_entry_credit = entry_pricing["actual_entry_credit"]
            premium_per_contract = round(actual_entry_credit * 100.0, 2)
            total_premium = round(premium_per_contract * contracts, 2)
            score_summary = self._score_candidate(
                system_name="Apollo",
                candidate_profile=item.get("mode_label") or item.get("pass_type_label") or "Standard",
                vix_value=self._coerce_float((precheck.get("vix") or {}).get("value")),
                em_multiple=self._coerce_float(item.get("actual_em_multiple") or item.get("em_multiple")),
                distance_points=self._coerce_float(item.get("actual_distance_to_short") or item.get("distance_points")),
                short_delta=self._coerce_float(item.get("short_delta")),
                expected_move=self._coerce_float(item.get("expected_move_used") or item.get("expected_move")),
                credit=self._coerce_float(item.get("credit") or item.get("premium_per_contract")),
                spread_width=self._coerce_float(item.get("width")),
                max_loss=self._coerce_float(item.get("max_theoretical_risk") or item.get("max_loss")),
                total_premium=total_premium,
                raw_candidate_score=self._coerce_float(item.get("score")),
                structure_context=(precheck.get("structure") or {}).get("final_grade") or (precheck.get("structure") or {}).get("grade"),
                macro_context=(precheck.get("macro") or {}).get("grade"),
                learning_state=learning_state,
            )
            payload = {
                "trade_mode": "talos",
                "system_name": "Apollo",
                "journal_name": JOURNAL_NAME_DEFAULT,
                "system_version": self._version_number(),
                "candidate_profile": item.get("mode_label") or item.get("pass_type_label") or "Standard",
                "status": "open",
                "trade_date": current_timestamp().split("T", 1)[0],
                "entry_datetime": current_timestamp(),
                "expiration_date": self._format_market_day((precheck.get("market_calendar") or {}).get("next_market_day")),
                "underlying_symbol": "SPX",
                "spx_at_entry": (precheck.get("spx") or {}).get("value") or "",
                "vix_at_entry": (precheck.get("vix") or {}).get("value") or "",
                "structure_grade": ((precheck.get("structure") or {}).get("final_grade") or (precheck.get("structure") or {}).get("grade") or ""),
                "macro_grade": (precheck.get("macro") or {}).get("grade") or "",
                "expected_move": item.get("expected_move") or "",
                "expected_move_used": item.get("expected_move_used") or item.get("expected_move") or "",
                "expected_move_source": item.get("expected_move_source") or "same_day_atm_straddle",
                "option_type": "Put Credit Spread",
                "short_strike": item.get("short_strike") or "",
                "long_strike": item.get("long_strike") or "",
                "spread_width": item.get("width") or "",
                "contracts": contracts,
                "candidate_credit_estimate": item.get("credit") or "",
                "actual_entry_credit": actual_entry_credit,
                "net_credit_per_contract": actual_entry_credit,
                "distance_to_short": item.get("distance_points") or "",
                "em_multiple_floor": item.get("applied_em_multiple_floor") or item.get("target_em_multiple") or "",
                "percent_floor": item.get("percent_floor") or "",
                "boundary_rule_used": item.get("boundary_rule_used") or "",
                "actual_distance_to_short": item.get("actual_distance_to_short") or item.get("distance_points") or "",
                "actual_em_multiple": item.get("actual_em_multiple") or item.get("em_multiple") or "",
                "fallback_used": "yes" if item.get("fallback_used") else "no",
                "fallback_rule_name": item.get("fallback_rule_name") or "",
                "short_delta": item.get("short_delta") or "",
                "pass_type": item.get("pass_type") or "",
                "premium_per_contract": premium_per_contract,
                "total_premium": total_premium,
                "max_theoretical_risk": item.get("max_theoretical_risk") or item.get("max_loss") or "",
                "risk_efficiency": item.get("risk_efficiency") or "",
                "target_em": item.get("target_em") or item.get("target_em_multiple") or "",
                "notes_entry": "",
                "prefill_source": "talos-apollo",
                "automation_status": "autonomous-entry-and-management",
                "close_method": "",
                "close_reason": "",
                "notes_exit": "",
            }
            payload["notes_entry"] = self._build_decision_note(
                system_name="Apollo",
                descriptor=item.get("mode_label") or item.get("mode_descriptor") or "candidate",
                account_balance=account_balance,
                candidate_score=float(score_summary.get("candidate_score") or 0.0),
                details=(
                    f"Distance {item.get('distance_points') or 'n/a'} pts; EM {item.get('em_multiple') or 'n/a'}x. "
                    f"Entry fill {entry_pricing['pricing_math_display']}. "
                    f"{score_summary.get('score_breakdown_display') or ''}"
                ).strip(),
            )
            candidate_models.append(
                {
                    "candidate_score": float(score_summary.get("candidate_score") or 0.0),
                    "decision_note": str(payload.get("notes_entry") or ""),
                    "selection_basis": str(score_summary.get("selection_basis") or ""),
                    "score_breakdown": score_summary,
                    "score_breakdown_display": str(score_summary.get("score_breakdown_display") or ""),
                    "pricing_basis_label": entry_pricing["pricing_basis_label"],
                    "pricing_math_display": entry_pricing["pricing_math_display"],
                    "entry_gate_label": "Talos accepts Apollo when a qualified Apollo portfolio clears governance and Black Swan loss constraints while pursuing the premium target as a scoring objective.",
                    "trade_payload": payload,
                    "projected_black_swan_loss": 0.0,
                    "profile_label": str(payload.get("candidate_profile") or "Standard"),
                    "signature": self._apollo_payload_signature(payload),
                }
            )
        return sorted(
            candidate_models,
            key=lambda entry: (
                float(entry.get("candidate_score") or 0.0),
                float((entry.get("score_breakdown") or {}).get("standard_preference_component") or 0.0),
                float((entry.get("trade_payload") or {}).get("total_premium") or 0.0),
            ),
            reverse=True,
        )[: self.APOLLO_PORTFOLIO_CANDIDATE_LIMIT]

    def _select_apollo_portfolio(
        self,
        candidates: List[Dict[str, Any]],
        *,
        account_balance: float,
        open_records: List[Dict[str, Any]],
    ) -> Dict[str, Any] | None:
        qualified_candidates = [item for item in candidates if float(item.get("candidate_score") or 0.0) >= self.ENTRY_SCORE_MINIMUM]
        premium_target = round(max(float(account_balance or 0.0), 1.0) * self.APOLLO_PREMIUM_TARGET_RATIO, 2)
        black_swan_roi = self._average_black_swan_roi()
        max_projected_loss = round(max(float(account_balance or 0.0), 1.0) * self.APOLLO_PROJECTED_BLACK_SWAN_LOSS_CAP_RATIO, 2)
        current_open_apollo = [item for item in open_records if normalize_system_name(item.get("system_name")) == "Apollo"]
        current_premium = round(sum(float(item.get("total_premium") or 0.0) for item in current_open_apollo), 2)
        current_projected_loss = round(sum(self._projected_black_swan_loss_for_record(item, black_swan_roi) for item in current_open_apollo), 2)
        current_profiles = [str(item.get("profile_label") or item.get("candidate_profile") or "Standard") for item in current_open_apollo]
        current_profile_mix = self._format_profile_mix(current_profiles)
        profile_history = self._build_apollo_profile_history_summary()
        combination_history = self._build_apollo_combination_history_summary(profile_history)
        if max_projected_loss > 0 and current_projected_loss >= max_projected_loss:
            reason = self._build_apollo_live_cap_stop_reason(current_projected_loss, max_projected_loss)
            return {
                "selected_candidates": [],
                "premium_target": premium_target,
                "selected_total_premium": 0.0,
                "current_total_premium": current_premium,
                "projected_black_swan_loss_used": 0.0,
                "projected_black_swan_loss_current": current_projected_loss,
                "projected_black_swan_loss_max": max_projected_loss,
                "black_swan_roi_used": black_swan_roi,
                "selected_profile_mix": "None",
                "current_profile_mix": current_profile_mix,
                "combination_shape": "none",
                "selected_portfolio_score": 0.0,
                "portfolio_rationale": reason,
                "considered_candidates_summary": "None",
                "combinations_evaluated_summary": "None",
                "quantity_scaling_applied": False,
                "scaled_candidate_count": 0,
                "default_contract_quantity_total": 0,
                "selected_contract_quantity_total": 0,
                "default_projected_black_swan_loss_total": 0.0,
                "selected_position_count": len(current_open_apollo),
                "next_addition_summary": "None",
                "scaling_summary": "No Black Swan capacity remained.",
                "selection_note": reason,
            }
        if not qualified_candidates:
            return {
                "selected_candidates": [],
                "premium_target": premium_target,
                "selected_total_premium": 0.0,
                "current_total_premium": current_premium,
                "projected_black_swan_loss_used": 0.0,
                "projected_black_swan_loss_current": current_projected_loss,
                "projected_black_swan_loss_max": max_projected_loss,
                "black_swan_roi_used": black_swan_roi,
                "selected_profile_mix": "None",
                "current_profile_mix": current_profile_mix,
                "combination_shape": "none",
                "selected_portfolio_score": 0.0,
                "portfolio_rationale": "No Apollo candidates cleared the Talos entry floor.",
                "considered_candidates_summary": "None",
                "combinations_evaluated_summary": "None",
                "quantity_scaling_applied": False,
                "scaled_candidate_count": 0,
                "default_contract_quantity_total": 0,
                "selected_contract_quantity_total": 0,
                "default_projected_black_swan_loss_total": 0.0,
                "selected_position_count": len(current_open_apollo),
                "next_addition_summary": "None",
                "scaling_summary": "No Apollo candidates qualified.",
                "selection_note": "No Apollo candidates cleared the Talos entry floor.",
            }
        for item in qualified_candidates:
            payload = dict(item.get("trade_payload") or {})
            default_contract_quantity = max(int(payload.get("contracts") or 1), 1)
            default_total_premium = round(float(self._coerce_float(payload.get("total_premium")) or 0.0), 2)
            default_projected_loss = self._projected_black_swan_loss_for_payload(payload, black_swan_roi)
            item["default_contract_quantity"] = default_contract_quantity
            item["selected_contract_quantity"] = default_contract_quantity
            item["default_total_premium"] = default_total_premium
            item["selected_total_premium"] = default_total_premium
            item["default_max_theoretical_risk"] = round(float(self._coerce_float(payload.get("max_theoretical_risk")) or 0.0), 2)
            item["projected_black_swan_loss"] = default_projected_loss
            item["default_projected_black_swan_loss"] = default_projected_loss
            item["scaled_projected_black_swan_loss"] = default_projected_loss
            item["quantity_scaling_applied"] = False
        considered_candidates_summary = "; ".join(
            f"{item.get('profile_label') or 'Standard'} {float(item.get('candidate_score') or 0.0):.1f}"
            for item in sorted(qualified_candidates, key=lambda entry: float(entry.get("candidate_score") or 0.0), reverse=True)
        ) or "None"
        evaluated_summaries: List[str] = []
        ranked_options: List[tuple[tuple[float, float, float, float], Dict[str, Any]]] = []
        for allowed_profiles in self.APOLLO_ALLOWED_PROFILE_COMBINATIONS:
            combo = self._resolve_apollo_profile_combination(qualified_candidates, allowed_profiles)
            if not combo:
                continue
            combo_summary = self._score_apollo_portfolio_combination(
                combo,
                premium_target=premium_target,
                current_open_apollo=current_open_apollo,
                current_projected_loss=current_projected_loss,
                max_projected_loss=max_projected_loss,
                black_swan_roi=black_swan_roi,
                profile_history=profile_history,
                combination_history=combination_history,
            )
            if combo_summary is None:
                continue
            evaluated_summaries.append(str(combo_summary.get("evaluation_summary") or combo_summary.get("combination_label") or "Apollo combo"))
            if combo_summary.get("qualified") and list(combo_summary.get("selected_candidates") or []):
                combo_option = dict(combo_summary)
                combo_option["option_kind"] = "combination"
                combo_option["selected_total_premium"] = round(float(combo_option.get("selected_total_premium") or combo_option.get("premium_total") or 0.0), 2)
                combo_option["selected_live_black_swan_loss"] = round(float(combo_option.get("projected_black_swan_loss_used") or 0.0), 2)
                combo_option["next_addition_summary"] = str(
                    combo_option.get("next_addition_summary")
                    or f"{combo_option.get('combination_shape') or 'portfolio'} {combo_option.get('selected_profile_mix') or 'None'}"
                )
                ranked_options.append(
                    (
                        (
                            round(float(combo_option.get("portfolio_score") or 0.0), 4),
                            round(float(combo_option.get("average_candidate_score") or 0.0), 4),
                            round(float(combo_option.get("selected_total_premium") or 0.0), 4),
                            round(float(combo_option.get("history_mix_quality") or 0.0), 4),
                        ),
                        combo_option,
                    )
                )
        incremental_summaries: List[str] = []
        for candidate in qualified_candidates:
            addition_summary = self._score_apollo_incremental_addition(
                candidate,
                premium_target=premium_target,
                current_open_apollo=current_open_apollo,
                current_profiles=current_profiles,
                current_premium=current_premium,
                current_projected_loss=current_projected_loss,
                max_projected_loss=max_projected_loss,
                black_swan_roi=black_swan_roi,
                profile_history=profile_history,
                combination_history=combination_history,
            )
            incremental_summaries.append(str(addition_summary.get("evaluation_summary") or addition_summary.get("candidate_label") or "Apollo candidate"))
            if not addition_summary.get("qualified"):
                continue
            addition_option = dict(addition_summary)
            addition_option["option_kind"] = "incremental-addition"
            addition_option["selected_candidates"] = [dict(addition_option.get("selected_candidate") or {})]
            addition_option["selected_total_premium"] = round(float(addition_option.get("resulting_total_premium") or 0.0), 2)
            addition_option["selected_live_black_swan_loss"] = round(float(addition_option.get("resulting_live_black_swan_loss") or 0.0), 2)
            ranked_options.append(
                (
                    (
                        round(float(addition_option.get("portfolio_score") or 0.0), 4),
                        round(float(addition_option.get("candidate_score") or 0.0), 4),
                        round(float(addition_option.get("selected_total_premium") or 0.0), 4),
                        round(float(addition_option.get("history_mix_quality") or 0.0), 4),
                    ),
                    addition_option,
                )
            )
        if not ranked_options:
            no_fit_reason = "; ".join(incremental_summaries + evaluated_summaries) or "Apollo candidates were available, but no legal Apollo option remained under the hard constraints."
            return {
                "selected_candidates": [],
                "premium_target": premium_target,
                "selected_total_premium": 0.0,
                "current_total_premium": current_premium,
                "projected_black_swan_loss_used": 0.0,
                "projected_black_swan_loss_current": current_projected_loss,
                "projected_black_swan_loss_max": max_projected_loss,
                "black_swan_roi_used": black_swan_roi,
                "selected_profile_mix": "None",
                "current_profile_mix": current_profile_mix,
                "combination_shape": "none",
                "selected_portfolio_score": 0.0,
                "portfolio_rationale": "Talos reviewed Apollo candidates for the highest-scoring legal option, but no legal Apollo option remained within the hard constraints.",
                "considered_candidates_summary": considered_candidates_summary,
                "combinations_evaluated_summary": ", ".join(evaluated_summaries) or "None",
                "quantity_scaling_applied": False,
                "scaled_candidate_count": 0,
                "default_contract_quantity_total": 0,
                "selected_contract_quantity_total": 0,
                "default_projected_black_swan_loss_total": 0.0,
                "selected_position_count": len(current_open_apollo),
                "next_addition_summary": "None",
                "scaling_summary": no_fit_reason,
                "selection_note": no_fit_reason,
            }
        _, selected_summary = max(ranked_options, key=lambda item: item[0])
        selected_candidates = [item for item in (selected_summary.get("selected_candidates") or []) if isinstance(item, dict)]
        selected_total_premium = round(float(selected_summary.get("selected_total_premium") or 0.0), 2)
        selected_live_black_swan_loss = round(float(selected_summary.get("selected_live_black_swan_loss") or 0.0), 2)
        selected_option_summary = str(selected_summary.get("next_addition_summary") or selected_summary.get("selected_profile_mix") or "None")
        return {
            "selected_candidates": selected_candidates,
            "premium_target": premium_target,
            "selected_total_premium": selected_total_premium,
            "current_total_premium": current_premium,
            "projected_black_swan_loss_used": selected_live_black_swan_loss,
            "projected_black_swan_loss_current": current_projected_loss,
            "projected_black_swan_loss_max": max_projected_loss,
            "black_swan_roi_used": black_swan_roi,
            "selected_profile_mix": selected_summary["selected_profile_mix"],
            "current_profile_mix": current_profile_mix,
            "combination_shape": str(selected_summary.get("combination_shape") or selected_summary.get("option_kind") or "incremental-addition"),
            "selected_portfolio_score": selected_summary["portfolio_score"],
            "portfolio_rationale": selected_summary["portfolio_rationale"],
            "considered_candidates_summary": considered_candidates_summary,
            "combinations_evaluated_summary": ", ".join(evaluated_summaries) or "None",
            "quantity_scaling_applied": bool(selected_summary.get("quantity_scaling_applied")),
            "scaled_candidate_count": int(selected_summary.get("scaled_candidate_count") or 0),
            "default_contract_quantity_total": int(selected_summary.get("default_contract_quantity_total") or 0),
            "selected_contract_quantity_total": int(selected_summary.get("selected_contract_quantity_total") or 0),
            "default_projected_black_swan_loss_total": round(float(selected_summary.get("default_projected_black_swan_loss_total") or 0.0), 2),
            "selected_position_count": len(current_open_apollo) + len(selected_candidates),
            "next_addition_summary": selected_option_summary,
            "scaling_summary": str(selected_summary.get("scaling_summary") or "Apollo quantities were unchanged."),
            "additional_position_available": True,
            "additional_position_count": len(selected_candidates),
            "selection_note": (
                f"Apollo selected the highest-scoring legal option at score {selected_summary['portfolio_score']:.1f}: "
                f"{selected_option_summary}; portfolio premium {self._format_currency(selected_total_premium)} vs {self._format_currency(premium_target)} target; "
                f"target proximity improved Credit and Profit scoring, but the premium target remained a scoring objective rather than a gate; "
                f"live Black Swan {self._format_currency(selected_live_black_swan_loss)} of {self._format_currency(max_projected_loss)}."
            ),
        }

    def _score_apollo_incremental_addition(
        self,
        candidate: Dict[str, Any],
        *,
        premium_target: float,
        current_open_apollo: List[Dict[str, Any]],
        current_profiles: List[str],
        current_premium: float,
        current_projected_loss: float,
        max_projected_loss: float,
        black_swan_roi: float | None,
        profile_history: Dict[str, Dict[str, float]],
        combination_history: Dict[str, Dict[str, float]],
    ) -> Dict[str, Any]:
        candidate_profile = str(candidate.get("profile_label") or (candidate.get("trade_payload") or {}).get("candidate_profile") or "Standard")
        candidate_label = f"{candidate_profile} {float(candidate.get('candidate_score') or 0.0):.1f}"
        overlap_penalty = self._apollo_combination_overlap_penalty([candidate], current_open_apollo)
        if overlap_penalty >= 1.0:
            return {
                "qualified": False,
                "candidate_label": candidate_label,
                "evaluation_summary": f"{candidate_profile} blocked as materially redundant against the current Apollo book",
            }
        adjusted_combo, scaling_context = self._scale_apollo_portfolio_combination_to_cap(
            [candidate],
            available_projected_loss=round(max(max_projected_loss - current_projected_loss, 0.0), 2),
            black_swan_roi=black_swan_roi,
        )
        if not adjusted_combo:
            return {
                "qualified": False,
                "candidate_label": candidate_label,
                "evaluation_summary": str(scaling_context.get("evaluation_summary") or f"{candidate_profile} blocked by Black Swan cap"),
            }
        selected_candidate = adjusted_combo[0]
        candidate_premium = round(float((selected_candidate.get("trade_payload") or {}).get("total_premium") or 0.0), 2)
        candidate_loss = round(float(selected_candidate.get("projected_black_swan_loss") or 0.0), 2)
        resulting_total_premium = round(current_premium + candidate_premium, 2)
        resulting_live_black_swan_loss = round(current_projected_loss + candidate_loss, 2)
        resulting_profiles = list(current_profiles) + [candidate_profile]
        premium_coverage = min((resulting_total_premium / premium_target), 1.0) if premium_target > 0 else 1.0
        current_gap = max(premium_target - current_premium, 0.0)
        progress_gain = candidate_premium / max(current_gap, candidate_premium, 1.0)
        loss_efficiency = min((candidate_premium / max(candidate_loss, 1.0)) / 0.25, 1.0)
        profile_mix_quality = self._apollo_profile_mix_quality(resulting_profiles, profile_history, combination_history)
        diversification_benefit = self._apollo_diversification_benefit(resulting_profiles, profile_history)
        concentration_penalty = self._apollo_concentration_penalty(resulting_profiles)
        standard_core_bonus = self._apollo_standard_core_bonus(resulting_profiles, loss_efficiency, profile_mix_quality)
        portfolio_score = round(
            (min(float(candidate.get("candidate_score") or 0.0) / 100.0, 1.0) * 30.0)
            + (premium_coverage * 18.0)
            + (min(progress_gain, 1.0) * 16.0)
            + (loss_efficiency * 12.0)
            + (profile_mix_quality * 12.0)
            + (diversification_benefit * 6.0)
            + standard_core_bonus
            - (overlap_penalty * 10.0)
            - (concentration_penalty * 8.0)
            - (2.0 if scaling_context.get("quantity_scaling_applied") else 0.0),
            2,
        )
        payload = dict(selected_candidate.get("trade_payload") or {})
        next_addition_summary = (
            f"{candidate_profile} {payload.get('short_strike') or ''}/{payload.get('long_strike') or ''} "
            f"qty {int(selected_candidate.get('default_contract_quantity') or 1)}->{int(selected_candidate.get('selected_contract_quantity') or 1)}"
        ).strip()
        scaling_summary = str(scaling_context.get("scaling_summary") or "Apollo quantities were unchanged.")
        return {
            "qualified": True,
            "candidate_label": candidate_label,
            "selected_candidate": selected_candidate,
            "candidate_score": float(candidate.get("candidate_score") or 0.0),
            "portfolio_score": portfolio_score,
            "selected_profile_mix": self._format_profile_mix(resulting_profiles),
            "resulting_total_premium": resulting_total_premium,
            "resulting_live_black_swan_loss": resulting_live_black_swan_loss,
            "projected_black_swan_loss_used": candidate_loss,
            "history_mix_quality": round(profile_mix_quality, 4),
            "quantity_scaling_applied": bool(scaling_context.get("quantity_scaling_applied")),
            "scaled_candidate_count": int(scaling_context.get("scaled_candidate_count") or 0),
            "default_contract_quantity_total": int(scaling_context.get("default_contract_quantity_total") or 0),
            "selected_contract_quantity_total": int(scaling_context.get("selected_contract_quantity_total") or 0),
            "default_projected_black_swan_loss_total": round(float(scaling_context.get("default_projected_black_swan_loss_total") or 0.0), 2),
            "next_addition_summary": next_addition_summary,
            "scaling_summary": scaling_summary,
            "evaluation_summary": (
                f"{candidate_profile} next-add {payload.get('short_strike') or ''}/{payload.get('long_strike') or ''}; "
                f"qty {int(selected_candidate.get('default_contract_quantity') or 1)}->{int(selected_candidate.get('selected_contract_quantity') or 1)}; "
                f"portfolio score {portfolio_score:.1f}"
            ),
            "portfolio_rationale": (
                f"incremental addition {next_addition_summary} improved the live Apollo book to {self._format_currency(resulting_total_premium)} premium "
                f"against target {self._format_currency(premium_target)} while keeping live Black Swan loss at {self._format_currency(resulting_live_black_swan_loss)}; "
                f"overlap penalty {overlap_penalty:.2f}; mix quality {profile_mix_quality:.2f}; {scaling_summary}"
            ),
        }

    def _resolve_apollo_profile_combination(self, candidates: List[Dict[str, Any]], allowed_profiles: tuple[str, ...]) -> List[Dict[str, Any]]:
        selected: List[Dict[str, Any]] = []
        used_signatures: set[str] = set()
        for profile in allowed_profiles:
            matching_candidates = [
                item
                for item in candidates
                if str(item.get("profile_label") or (item.get("trade_payload") or {}).get("candidate_profile") or "Standard") == profile
            ]
            if not matching_candidates:
                return []
            best_candidate = max(
                matching_candidates,
                key=lambda entry: (
                    float(entry.get("candidate_score") or 0.0),
                    float((entry.get("trade_payload") or {}).get("total_premium") or 0.0),
                ),
            )
            signature = self._apollo_candidate_signature(best_candidate)
            if signature in used_signatures:
                return []
            used_signatures.add(signature)
            selected.append(best_candidate)
        return selected

    def _score_apollo_portfolio_combination(
        self,
        combo: List[Dict[str, Any]],
        *,
        premium_target: float,
        current_open_apollo: List[Dict[str, Any]],
        current_projected_loss: float,
        max_projected_loss: float,
        black_swan_roi: float | None,
        profile_history: Dict[str, Dict[str, float]],
        combination_history: Dict[str, Dict[str, float]],
    ) -> Dict[str, Any] | None:
        if not combo:
            return None
        profiles = [str(item.get("profile_label") or (item.get("trade_payload") or {}).get("candidate_profile") or "Standard") for item in combo]
        selected_profile_mix = self._format_profile_mix(profiles)
        signatures = {self._apollo_candidate_signature(item) for item in combo}
        if len(signatures) != len(combo):
            return {
                "qualified": False,
                "combination_label": selected_profile_mix,
                "evaluation_summary": f"{selected_profile_mix} blocked duplicate candidate signature",
            }
        redundancy_penalty = self._apollo_combination_overlap_penalty(combo, current_open_apollo)
        if redundancy_penalty >= 1.0:
            return {
                "qualified": False,
                "combination_label": selected_profile_mix,
                "evaluation_summary": f"{selected_profile_mix} blocked overlap penalty {redundancy_penalty:.2f}",
            }
        adjusted_combo, scaling_context = self._scale_apollo_portfolio_combination_to_cap(
            combo,
            available_projected_loss=round(max(max_projected_loss - current_projected_loss, 0.0), 2),
            black_swan_roi=black_swan_roi,
        )
        if not adjusted_combo:
            return {
                "qualified": False,
                "combination_label": selected_profile_mix,
                "evaluation_summary": str(scaling_context.get("evaluation_summary") or f"{selected_profile_mix} blocked Black Swan cap"),
            }
        projected_loss = round(float(scaling_context.get("projected_black_swan_loss_total") or 0.0), 2)
        premium_total = round(sum(float((item.get("trade_payload") or {}).get("total_premium") or 0.0) for item in adjusted_combo), 2)
        average_candidate_score = sum(float(item.get("candidate_score") or 0.0) for item in combo) / len(combo)
        combination_shape = "single-profile" if len(set(profiles)) == 1 and len(adjusted_combo) == 1 else ("single-profile" if len(set(profiles)) == 1 else "mixed-profile")
        premium_coverage = min((premium_total / premium_target), 1.0) if premium_target > 0 else 1.0
        premium_per_projected_loss = premium_total / max(projected_loss, 1.0)
        loss_efficiency = min(premium_per_projected_loss / 0.25, 1.0)
        profile_mix_quality = self._apollo_profile_mix_quality(profiles, profile_history, combination_history)
        diversification_benefit = self._apollo_diversification_benefit(profiles, profile_history)
        concentration_penalty = self._apollo_concentration_penalty(profiles)
        standard_core_bonus = self._apollo_standard_core_bonus(profiles, loss_efficiency, profile_mix_quality)
        complexity_penalty = max(len(combo) - 1, 0) * 4.0
        portfolio_score = round(
            (premium_coverage * 25.0)
            + (min(average_candidate_score / 100.0, 1.0) * 24.0)
            + (min(premium_total / max(premium_target, 1.0), 1.2) * 12.0)
            + (loss_efficiency * 14.0)
            + (profile_mix_quality * 12.0)
            + (diversification_benefit * 6.0)
            + standard_core_bonus
            - (redundancy_penalty * 10.0)
            - (concentration_penalty * 8.0)
            - complexity_penalty
            - (2.0 if scaling_context.get("quantity_scaling_applied") else 0.0),
            2,
        )
        scaling_summary = str(scaling_context.get("scaling_summary") or "Apollo quantities were unchanged.")
        return {
            "qualified": True,
            "combination_label": selected_profile_mix,
            "combination_shape": combination_shape,
            "selected_candidates": adjusted_combo,
            "projected_black_swan_loss_used": projected_loss,
            "selected_profile_mix": selected_profile_mix,
            "portfolio_score": portfolio_score,
            "average_candidate_score": round(average_candidate_score, 2),
            "premium_total": premium_total,
            "premium_per_projected_black_swan_loss": round(premium_per_projected_loss, 4),
            "history_mix_quality": round(profile_mix_quality, 4),
            "diversification_benefit": round(diversification_benefit, 4),
            "redundancy_penalty": round(redundancy_penalty, 4),
            "concentration_penalty": round(concentration_penalty, 4),
            "complexity_penalty": round(complexity_penalty, 4),
            "standard_core_bonus": round(standard_core_bonus, 4),
            "quantity_scaling_applied": bool(scaling_context.get("quantity_scaling_applied")),
            "scaled_candidate_count": int(scaling_context.get("scaled_candidate_count") or 0),
            "selected_total_premium": premium_total,
            "default_contract_quantity_total": int(scaling_context.get("default_contract_quantity_total") or 0),
            "selected_contract_quantity_total": int(scaling_context.get("selected_contract_quantity_total") or 0),
            "default_projected_black_swan_loss_total": round(float(scaling_context.get("default_projected_black_swan_loss_total") or 0.0), 2),
            "scaling_summary": scaling_summary,
            "evaluation_summary": str(scaling_context.get("evaluation_summary") or f"{selected_profile_mix} {portfolio_score:.1f}"),
            "portfolio_rationale": (
                f"{combination_shape} {selected_profile_mix} beat alternatives on portfolio utility {portfolio_score:.1f}; "
                f"premium {self._format_currency(premium_total)} vs target {self._format_currency(premium_target)}; "
                f"selected Black Swan loss {self._format_currency(projected_loss)}; mix quality {profile_mix_quality:.2f}; "
                f"overlap penalty {redundancy_penalty:.2f}; complexity penalty {complexity_penalty:.1f}; {scaling_summary}"
            ),
        }

    def _scale_apollo_portfolio_combination_to_cap(
        self,
        combo: List[Dict[str, Any]],
        *,
        available_projected_loss: float,
        black_swan_roi: float | None,
    ) -> tuple[List[Dict[str, Any]] | None, Dict[str, Any]]:
        adjusted_combo = [
            self._apollo_candidate_with_contract_quantity(
                item,
                int(item.get("default_contract_quantity") or (item.get("trade_payload") or {}).get("contracts") or 1),
                black_swan_roi,
            )
            for item in combo
        ]
        default_contract_total = sum(int(item.get("default_contract_quantity") or 1) for item in adjusted_combo)
        default_projected_loss_total = round(sum(float(item.get("default_projected_black_swan_loss") or 0.0) for item in adjusted_combo), 2)
        selected_profile_mix = self._format_profile_mix(
            [str(item.get("profile_label") or (item.get("trade_payload") or {}).get("candidate_profile") or "Standard") for item in adjusted_combo]
        )
        projected_loss_total = round(sum(float(item.get("projected_black_swan_loss") or 0.0) for item in adjusted_combo), 2)
        if available_projected_loss <= 0:
            return None, {
                "quantity_scaling_applied": False,
                "scaled_candidate_count": 0,
                "default_contract_quantity_total": default_contract_total,
                "selected_contract_quantity_total": default_contract_total,
                "default_projected_black_swan_loss_total": default_projected_loss_total,
                "projected_black_swan_loss_total": projected_loss_total,
                "evaluation_summary": f"{selected_profile_mix} blocked because no Black Swan capacity remained",
                "scaling_summary": "no Black Swan capacity remained",
            }
        while projected_loss_total > available_projected_loss:
            scalable_indexes = [
                index
                for index, item in enumerate(adjusted_combo)
                if int(item.get("selected_contract_quantity") or item.get("default_contract_quantity") or 1) > 1
            ]
            if not scalable_indexes:
                break
            target_index = max(
                scalable_indexes,
                key=lambda index: (
                    float(adjusted_combo[index].get("projected_black_swan_loss") or 0.0),
                    float(adjusted_combo[index].get("candidate_score") or 0.0),
                    int(adjusted_combo[index].get("selected_contract_quantity") or adjusted_combo[index].get("default_contract_quantity") or 1),
                ),
            )
            current_quantity = int(adjusted_combo[target_index].get("selected_contract_quantity") or adjusted_combo[target_index].get("default_contract_quantity") or 1)
            adjusted_combo[target_index] = self._apollo_candidate_with_contract_quantity(combo[target_index], current_quantity - 1, black_swan_roi)
            projected_loss_total = round(sum(float(item.get("projected_black_swan_loss") or 0.0) for item in adjusted_combo), 2)
        selected_contract_total = sum(int(item.get("selected_contract_quantity") or item.get("default_contract_quantity") or 1) for item in adjusted_combo)
        scaling_applied = any(bool(item.get("quantity_scaling_applied")) for item in adjusted_combo)
        scaled_candidate_count = sum(1 for item in adjusted_combo if bool(item.get("quantity_scaling_applied")))
        if projected_loss_total > available_projected_loss:
            return None, {
                "quantity_scaling_applied": scaling_applied,
                "scaled_candidate_count": scaled_candidate_count,
                "default_contract_quantity_total": default_contract_total,
                "selected_contract_quantity_total": selected_contract_total,
                "default_projected_black_swan_loss_total": default_projected_loss_total,
                "projected_black_swan_loss_total": projected_loss_total,
                "evaluation_summary": (
                    f"{selected_profile_mix} blocked after scaling qty {default_contract_total}->{selected_contract_total}; "
                    f"Black Swan {self._format_currency(default_projected_loss_total)}->{self._format_currency(projected_loss_total)} still above remaining cap {self._format_currency(available_projected_loss)}"
                ),
                "scaling_summary": (
                    f"quantity scaling could not qualify the portfolio; qty {default_contract_total}->{selected_contract_total}; "
                    f"Black Swan {self._format_currency(default_projected_loss_total)}->{self._format_currency(projected_loss_total)} still above remaining cap {self._format_currency(available_projected_loss)}"
                ),
            }
        if scaling_applied:
            evaluation_summary = (
                f"{selected_profile_mix} qty {default_contract_total}->{selected_contract_total}; "
                f"Black Swan {self._format_currency(default_projected_loss_total)}->{self._format_currency(projected_loss_total)}"
            )
            scaling_summary = (
                f"quantity scaling qualified the portfolio; qty {default_contract_total}->{selected_contract_total}; "
                f"Black Swan {self._format_currency(default_projected_loss_total)}->{self._format_currency(projected_loss_total)}"
            )
        else:
            evaluation_summary = f"{selected_profile_mix} {projected_loss_total:.1f}"
            scaling_summary = "Apollo quantities were unchanged."
        return adjusted_combo, {
            "quantity_scaling_applied": scaling_applied,
            "scaled_candidate_count": scaled_candidate_count,
            "default_contract_quantity_total": default_contract_total,
            "selected_contract_quantity_total": selected_contract_total,
            "default_projected_black_swan_loss_total": default_projected_loss_total,
            "projected_black_swan_loss_total": projected_loss_total,
            "evaluation_summary": evaluation_summary,
            "scaling_summary": scaling_summary,
        }

    def _apollo_candidate_with_contract_quantity(
        self,
        candidate: Dict[str, Any],
        contracts: int,
        black_swan_roi: float | None,
    ) -> Dict[str, Any]:
        payload = dict(candidate.get("trade_payload") or {})
        default_contract_quantity = max(int(candidate.get("default_contract_quantity") or payload.get("contracts") or 1), 1)
        target_contract_quantity = max(int(contracts or 1), 1)
        default_total_premium = round(float(self._coerce_float(candidate.get("default_total_premium")) or self._coerce_float(payload.get("total_premium")) or 0.0), 2)
        premium_per_contract = round(default_total_premium / max(default_contract_quantity, 1), 2)
        default_max_risk = float(self._coerce_float(candidate.get("default_max_theoretical_risk")) or self._coerce_float(payload.get("max_theoretical_risk")) or 0.0)
        max_risk_per_contract = default_max_risk / max(default_contract_quantity, 1)
        payload["contracts"] = target_contract_quantity
        payload["premium_per_contract"] = premium_per_contract
        payload["total_premium"] = round(premium_per_contract * target_contract_quantity, 2)
        payload["max_theoretical_risk"] = round(max_risk_per_contract * target_contract_quantity, 2)
        updated_candidate = dict(candidate)
        updated_candidate["trade_payload"] = payload
        updated_candidate["signature"] = self._apollo_payload_signature(payload)
        updated_candidate["default_contract_quantity"] = default_contract_quantity
        updated_candidate["selected_contract_quantity"] = target_contract_quantity
        updated_candidate["default_total_premium"] = default_total_premium
        updated_candidate["selected_total_premium"] = payload["total_premium"]
        default_projected_black_swan_loss = float(
            self._coerce_float(candidate.get("default_projected_black_swan_loss"))
            or self._projected_black_swan_loss_for_payload(
                {
                    **payload,
                    "contracts": default_contract_quantity,
                    "total_premium": default_total_premium,
                    "max_theoretical_risk": round(max_risk_per_contract * default_contract_quantity, 2),
                },
                black_swan_roi,
            )
        )
        projected_black_swan_loss = self._projected_black_swan_loss_for_payload(payload, black_swan_roi)
        updated_candidate["default_projected_black_swan_loss"] = round(default_projected_black_swan_loss, 2)
        updated_candidate["projected_black_swan_loss"] = projected_black_swan_loss
        updated_candidate["scaled_projected_black_swan_loss"] = projected_black_swan_loss
        updated_candidate["quantity_scaling_applied"] = target_contract_quantity != default_contract_quantity
        if updated_candidate["quantity_scaling_applied"]:
            payload["notes_entry"] = self._append_apollo_scaling_note(
                str(payload.get("notes_entry") or ""),
                default_contract_quantity=default_contract_quantity,
                selected_contract_quantity=target_contract_quantity,
                default_projected_black_swan_loss=default_projected_black_swan_loss,
                selected_projected_black_swan_loss=projected_black_swan_loss,
            )
        return updated_candidate

    def _append_apollo_scaling_note(
        self,
        note: str,
        *,
        default_contract_quantity: int,
        selected_contract_quantity: int,
        default_projected_black_swan_loss: float,
        selected_projected_black_swan_loss: float,
    ) -> str:
        scaling_note = (
            f"Talos scaled Apollo quantity {default_contract_quantity} -> {selected_contract_quantity} to fit the hard Black Swan cap "
            f"({self._format_currency(default_projected_black_swan_loss)} -> {self._format_currency(selected_projected_black_swan_loss)} projected Black Swan loss)."
        )
        stripped_note = str(note or "").strip()
        if scaling_note in stripped_note:
            return stripped_note
        if not stripped_note:
            return scaling_note
        return f"{stripped_note} {scaling_note}".strip()

    def _build_apollo_profile_history_summary(self) -> Dict[str, Dict[str, float]]:
        records = [record for record in self._build_learning_records() if record.get("system") == "Apollo"]
        summary: Dict[str, Dict[str, float]] = {}
        for profile in {str(record.get("profile") or "Standard") for record in records}:
            summary[profile] = self._summarize_apollo_learning_records(
                [record for record in records if str(record.get("profile") or "Standard") == profile]
            )
        return summary

    def _build_apollo_combination_history_summary(self, profile_history: Dict[str, Dict[str, float]] | None = None) -> Dict[str, Dict[str, float]]:
        profile_history = profile_history or self._build_apollo_profile_history_summary()
        combination_summary: Dict[str, Dict[str, float]] = {}
        for profiles in self.APOLLO_ALLOWED_PROFILE_COMBINATIONS:
            metrics = [profile_history.get(profile) or {} for profile in profiles]
            trade_count = sum(float(metric.get("weighted_trade_count") or 0.0) for metric in metrics)
            average_win_rate = sum(float(metric.get("win_rate") or 0.0) for metric in metrics) / len(metrics)
            average_expectancy = sum(float(metric.get("expectancy") or 0.0) for metric in metrics) / len(metrics)
            average_black_swan_rate = sum(float(metric.get("black_swan_rate") or 0.0) for metric in metrics) / len(metrics)
            average_net_pnl = sum(float(metric.get("net_pnl") or 0.0) for metric in metrics) / len(metrics)
            mix_quality = max(
                0.0,
                min(
                    1.0,
                    0.34
                    + max(min(average_win_rate, 1.0), 0.0) * 0.28
                    + max(min((average_expectancy + 50.0) / 200.0, 1.0), 0.0) * 0.22
                    + max(min((average_net_pnl + 500.0) / 2500.0, 1.0), 0.0) * 0.10
                    + max(min(1.0 - average_black_swan_rate, 1.0), 0.0) * 0.06,
                ),
            )
            combination_summary[self._apollo_combination_key(list(profiles))] = {
                "mix_quality": round(mix_quality, 4),
                "trade_count": round(trade_count, 2),
            }
        return combination_summary

    def _summarize_apollo_learning_records(self, records: List[Dict[str, Any]]) -> Dict[str, float]:
        weighted_total = sum(float(record.get("source_weight") or 0.0) for record in records)
        weighted_wins = sum(float(record.get("source_weight") or 0.0) for record in records if float(record.get("realized_pnl") or 0.0) > 0.0)
        weighted_black_swans = sum(float(record.get("source_weight") or 0.0) for record in records if str(record.get("outcome") or "") == "black_swan")
        weighted_pnl = sum(float(record.get("realized_pnl") or 0.0) * float(record.get("source_weight") or 0.0) for record in records)
        weighted_avg_win = self._weighted_average([record for record in records if float(record.get("realized_pnl") or 0.0) > 0.0], value_key="realized_pnl")
        weighted_avg_loss = self._weighted_average([record for record in records if float(record.get("realized_pnl") or 0.0) <= 0.0], value_key="realized_pnl")
        weighted_credit_efficiency = self._weighted_average([record for record in records if record.get("credit_efficiency") is not None], value_key="credit_efficiency")
        expectancy = (weighted_pnl / weighted_total) if weighted_total else 0.0
        win_rate = (weighted_wins / weighted_total) if weighted_total else 0.0
        black_swan_rate = (weighted_black_swans / weighted_total) if weighted_total else 0.0
        outcome_mix_quality = max(0.0, min((win_rate * 0.7) + ((1.0 - black_swan_rate) * 0.3), 1.0))
        return {
            "weighted_trade_count": round(weighted_total, 2),
            "win_rate": round(win_rate, 4),
            "expectancy": round(expectancy, 2),
            "average_win": round(weighted_avg_win, 2),
            "average_loss": round(weighted_avg_loss, 2),
            "black_swan_rate": round(black_swan_rate, 4),
            "net_pnl": round(weighted_pnl, 2),
            "credit_efficiency": round(weighted_credit_efficiency, 4),
            "outcome_mix_quality": round(outcome_mix_quality, 4),
        }

    def _apollo_profile_mix_quality(
        self,
        profiles: List[str],
        profile_history: Dict[str, Dict[str, float]],
        combination_history: Dict[str, Dict[str, float]],
    ) -> float:
        combination_quality = float((combination_history.get(self._apollo_combination_key(profiles)) or {}).get("mix_quality") or 0.5)
        if not profiles:
            return combination_quality
        profile_quality = sum(
            max(
                0.0,
                min(
                    1.0,
                    0.32
                    + float((profile_history.get(profile) or {}).get("outcome_mix_quality") or 0.0) * 0.28
                    + max(min((float((profile_history.get(profile) or {}).get("expectancy") or 0.0) + 50.0) / 200.0, 1.0), 0.0) * 0.20
                    + max(min(float((profile_history.get(profile) or {}).get("credit_efficiency") or 0.0), 1.0), 0.0) * 0.10
                    + max(min(1.0 - float((profile_history.get(profile) or {}).get("black_swan_rate") or 0.0), 1.0), 0.0) * 0.10,
                ),
            )
            for profile in profiles
        ) / len(profiles)
        return round((combination_quality * 0.55) + (profile_quality * 0.45), 4)

    def _apollo_diversification_benefit(self, profiles: List[str], profile_history: Dict[str, Dict[str, float]]) -> float:
        if len(set(profiles)) <= 1:
            return 0.0
        win_rates = [float((profile_history.get(profile) or {}).get("win_rate") or 0.0) for profile in profiles]
        black_swan_rates = [float((profile_history.get(profile) or {}).get("black_swan_rate") or 0.0) for profile in profiles]
        dispersion = min(max(win_rates) - min(win_rates), 0.3) if win_rates else 0.0
        tail_offset = min(max(black_swan_rates) - min(black_swan_rates), 0.2) if black_swan_rates else 0.0
        return round(min(1.0, 0.25 + (dispersion * 1.5) + (tail_offset * 1.5)), 4)

    def _apollo_combination_overlap_penalty(self, combo: List[Dict[str, Any]], current_open_apollo: List[Dict[str, Any]]) -> float:
        penalties: List[float] = []
        combo_overlap_keys = [self._apollo_candidate_overlap_signature(item) for item in combo]
        if len(set(combo_overlap_keys)) != len(combo_overlap_keys):
            return 1.0
        current_signatures = {self._apollo_trade_signature(item) for item in current_open_apollo}
        for candidate in combo:
            signature = self._apollo_candidate_signature(candidate)
            if signature in current_signatures:
                return 1.0
            candidate_payload = dict(candidate.get("trade_payload") or {})
            short_strike = self._coerce_float((candidate.get("trade_payload") or {}).get("short_strike")) or 0.0
            long_strike = self._coerce_float(candidate_payload.get("long_strike")) or 0.0
            candidate_profile = str(candidate_payload.get("candidate_profile") or candidate.get("profile_label") or "Standard")
            for record in current_open_apollo:
                persisted = self.trade_store.get_trade(int(record.get("trade_id") or 0)) or {}
                existing_short = self._coerce_float(persisted.get("short_strike")) or 0.0
                existing_long = self._coerce_float(persisted.get("long_strike")) or 0.0
                existing_profile = str(persisted.get("candidate_profile") or record.get("profile_label") or record.get("candidate_profile") or "Standard")
                if (
                    existing_short
                    and short_strike
                    and existing_long
                    and long_strike
                    and existing_profile == candidate_profile
                    and abs(existing_short - short_strike) <= 5.0
                    and abs(existing_long - long_strike) <= 5.0
                ):
                    return 1.0
                if existing_short and short_strike and abs(existing_short - short_strike) <= 5.0:
                    penalties.append(0.22 if existing_profile != candidate_profile else 0.38)
        for first, second in combinations(combo, 2):
            first_payload = dict(first.get("trade_payload") or {})
            second_payload = dict(second.get("trade_payload") or {})
            first_short = self._coerce_float((first.get("trade_payload") or {}).get("short_strike")) or 0.0
            second_short = self._coerce_float((second.get("trade_payload") or {}).get("short_strike")) or 0.0
            first_long = self._coerce_float(first_payload.get("long_strike")) or 0.0
            second_long = self._coerce_float(second_payload.get("long_strike")) or 0.0
            first_profile = str(first_payload.get("candidate_profile") or first.get("profile_label") or "Standard")
            second_profile = str(second_payload.get("candidate_profile") or second.get("profile_label") or "Standard")
            if (
                first_short
                and second_short
                and first_long
                and second_long
                and first_profile == second_profile
                and abs(first_short - second_short) <= 5.0
                and abs(first_long - second_long) <= 5.0
            ):
                return 1.0
            if first_short and second_short and abs(first_short - second_short) <= 5.0:
                penalties.append(0.22 if first_profile != second_profile else 0.34)
        return round(min(sum(penalties), 1.0), 4)

    def _build_activity_log_payload(self, activity_log: List[Dict[str, Any]]) -> List[Dict[str, str]]:
        payload: List[Dict[str, str]] = []
        for item in activity_log:
            if not isinstance(item, dict):
                continue
            payload.append(
                {
                    "timestamp": self._format_activity_log_timestamp(item.get("timestamp") or item.get("latest_timestamp") or ""),
                    "category": str(item.get("category") or "Talos"),
                    "title": str(item.get("title") or "Talos activity"),
                    "detail": str(item.get("detail") or ""),
                }
            )
        return payload

    def _apollo_concentration_penalty(self, profiles: List[str]) -> float:
        counts = Counter(profiles)
        if not counts:
            return 0.0
        largest_weight = max(counts.values()) / len(profiles)
        penalty = max(0.0, largest_weight - 0.55)
        if len(set(profiles)) == 1 and next(iter(counts.keys()), "") != "Standard":
            penalty += 0.18
        return round(min(penalty, 1.0), 4)

    def _apollo_standard_core_bonus(self, profiles: List[str], loss_efficiency: float, profile_mix_quality: float) -> float:
        if "Standard" not in profiles:
            return 0.0
        bonus = 4.0
        if len(set(profiles)) == 1 and profiles[0] == "Standard":
            bonus += 1.5
        if loss_efficiency >= 0.15 and profile_mix_quality >= 0.55:
            bonus += 1.5
        return round(min(bonus, 8.0), 4)

    def _apollo_combination_key(self, profiles: List[str]) -> str:
        return "+".join(sorted(str(profile or "Standard") for profile in profiles))

    def _apollo_candidate_overlap_signature(self, candidate: Dict[str, Any]) -> str:
        payload = dict(candidate.get("trade_payload") or {})
        return "|".join(
            [
                str(payload.get("candidate_profile") or "Standard"),
                str(payload.get("short_strike") or ""),
                str(payload.get("long_strike") or ""),
            ]
        )

    def _resolve_apollo_entry_pricing(self, candidate: Dict[str, Any]) -> Dict[str, Any]:
        executable_credit = self._coerce_float(candidate.get("executable_credit"))
        if executable_credit is None:
            executable_credit = max(self._coerce_float(candidate.get("credit")) or 0.0, 0.0)
        executable_credit = round(max(executable_credit, 0.0), 4)
        short_bid = self._coerce_float((candidate.get("short_put") or {}).get("bid"))
        long_ask = self._coerce_float((candidate.get("long_put") or {}).get("ask"))
        if short_bid is not None and long_ask is not None:
            pricing_math_display = f"short bid {short_bid:.2f} - long ask {long_ask:.2f} = {executable_credit:.2f}"
        else:
            pricing_math_display = f"executable credit {executable_credit:.2f}"
        return {
            "actual_entry_credit": executable_credit,
            "pricing_basis_label": "Conservative executable entry credit",
            "pricing_math_display": pricing_math_display,
        }

    def _build_decision_note(
        self,
        *,
        system_name: str,
        descriptor: str,
        account_balance: float,
        candidate_score: float,
        details: str,
    ) -> str:
        return (
            f"Talos auto-entered this {system_name} setup from the {descriptor} slot using account balance {self._format_currency(account_balance)}. "
            f"Score {candidate_score:.1f}. {details} Autonomous management remains enabled."
        )

    def _compute_account_balance(self, open_records: List[Dict[str, Any]]) -> float:
        return self._compute_balance_components(open_records)["settled_balance"]

    def _compute_balance_components(self, open_records: List[Dict[str, Any]]) -> Dict[str, float]:
        realized_pnl = 0.0
        for trade in self.trade_store.list_trades("talos"):
            status = str(trade.get("derived_status_raw") or trade.get("status") or "").strip().lower()
            if status not in {"open", "reduced"}:
                realized_pnl += float(trade.get("gross_pnl") or 0.0)
        unrealized_pnl = sum(float(item.get("current_pl") or 0.0) for item in open_records)
        starting_balance = float(self._load_state().get("starting_balance") or self.STARTING_BALANCE)
        settled_balance = starting_balance + realized_pnl
        current_account_value = settled_balance + unrealized_pnl
        return {
            "realized_pnl": realized_pnl,
            "unrealized_pnl": unrealized_pnl,
            "settled_balance": settled_balance,
            "equity_balance": settled_balance,
            "current_account_value": current_account_value,
        }

    def _filter_talos_records(self, management_payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        return [
            item
            for item in (management_payload.get("records") or [])
            if str(item.get("trade_mode") or "").strip().lower() == "talos"
        ]

    def _sync_trade_metadata(self, state: Dict[str, Any], open_records: List[Dict[str, Any]]) -> bool:
        active_ids = {str(int(item.get("trade_id") or 0)) for item in open_records if int(item.get("trade_id") or 0) > 0}
        metadata = dict(state.get("trade_metadata") or {})
        changed = False
        for key in list(metadata.keys()):
            if key not in active_ids:
                metadata.pop(key, None)
                changed = True
        state["trade_metadata"] = metadata
        return changed

    def _default_state(self, *, starting_balance: float | None = None, last_archive_path: str = "") -> Dict[str, Any]:
        return {
            "paused": False,
            "starting_balance": float(starting_balance if starting_balance is not None else self.STARTING_BALANCE),
            "last_cycle_at": "",
            "last_cycle_reason": "",
            "last_cycle_status": "idle",
            "last_scan_at": "",
            "next_scan_at": "",
            "scan_engine_running": False,
            "total_scan_count": 0,
            "last_archive_path": last_archive_path,
            "activity_log": [],
            "decision_log": [],
            "skip_log": [],
            "apollo_portfolio_state": {},
            "learning_state": {"active_segment": "Apollo_LOW_VIX", "profiles": {}, "records": [], "last_refreshed_at": ""},
            "trade_metadata": {},
        }

    def _load_state(self) -> Dict[str, Any]:
        if not self.state_path.exists():
            return self._default_state()
        try:
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return self._default_state()
        if not isinstance(payload, dict):
            return self._default_state()
        state = self._default_state()
        state.update(payload)
        return state

    def _save_state(self, state: Dict[str, Any]) -> None:
        self.state_path.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")

    def _append_activity(self, state: Dict[str, Any], item: Dict[str, Any]) -> None:
        activity_log = [entry for entry in (state.get("activity_log") or []) if isinstance(entry, dict)]
        activity_log.insert(0, item)
        state["activity_log"] = activity_log[: self.MAX_ACTIVITY_ITEMS]

    def _append_skip(self, state: Dict[str, Any], item: Dict[str, Any]) -> None:
        skip_log = [entry for entry in (state.get("skip_log") or []) if isinstance(entry, dict)]
        if skip_log:
            latest = skip_log[0]
            latest_reason = str(latest.get("reason") or "").strip()
            latest_system = str(latest.get("system") or "").strip()
            item_reason = str(item.get("reason") or "").strip()
            item_system = str(item.get("system") or "").strip()
            if latest_reason == item_reason and latest_system == item_system:
                latest["repeat_count"] = int(latest.get("repeat_count") or 1) + 1
                latest["latest_timestamp"] = item.get("timestamp") or latest.get("latest_timestamp") or latest.get("timestamp")
                latest.setdefault("first_timestamp", latest.get("timestamp") or item.get("timestamp") or "")
                latest["timestamp"] = item.get("timestamp") or latest.get("timestamp") or ""
                state["skip_log"] = skip_log[: self.MAX_SKIP_ITEMS]
                return
        item = dict(item)
        item.setdefault("repeat_count", 1)
        item.setdefault("first_timestamp", item.get("timestamp") or "")
        item.setdefault("latest_timestamp", item.get("timestamp") or "")
        skip_log.insert(0, item)
        state["skip_log"] = skip_log[: self.MAX_SKIP_ITEMS]

    def _append_decision_log(self, state: Dict[str, Any], item: Dict[str, Any]) -> None:
        decision_log = [entry for entry in (state.get("decision_log") or []) if isinstance(entry, dict)]
        decision_log.insert(0, item)
        state["decision_log"] = decision_log[: self.MAX_DECISION_ITEMS]

    def _record_talos_decision(
        self,
        state: Dict[str, Any],
        *,
        system_name: str,
        decision: str,
        reason_text: str,
        score_breakdown: Dict[str, Any] | None = None,
        weights: Dict[str, Any] | None = None,
    ) -> None:
        segment_name = str((score_breakdown or {}).get("segment_name") or "")
        if segment_name:
            learning_state = dict(state.get("learning_state") or {})
            learning_state["active_segment"] = segment_name
            state["learning_state"] = learning_state
        self._append_decision_log(
            state,
            {
                "timestamp": self._now().isoformat(),
                "system": system_name,
                "segment": segment_name,
                "decision": decision,
                "reason": reason_text,
                "weights": self._normalize_component_weights(weights or (score_breakdown or {}).get("adaptive_weights")),
                "score_breakdown": dict(score_breakdown or {}),
            },
        )

    def _is_market_window(self, now: datetime) -> bool:
        local_now = now.astimezone(self.display_timezone)
        if not self.market_calendar_service.is_tradable_market_day(local_now.date()):
            return False
        current_time = local_now.time()
        return self.MARKET_OPEN <= current_time <= self.MARKET_CLOSE

    def _resolve_cycle_mode(self, now: datetime) -> str:
        local_now = now.astimezone(self.display_timezone)
        if not self.market_calendar_service.is_tradable_market_day(local_now.date()):
            return "closed"
        current_time = local_now.time()
        final_scan_cutoff = self._final_post_close_time(local_now.date())
        if self.MARKET_OPEN <= current_time <= self.MARKET_CLOSE:
            return "market"
        if self.MARKET_CLOSE < current_time <= final_scan_cutoff:
            return "post-close-final"
        return "closed"

    def _resolve_next_background_run_at(self, now: datetime) -> datetime | None:
        local_now = now.astimezone(self.display_timezone)
        session_date = self.market_calendar_service.get_next_or_same_tradable_market_day(local_now.date())
        market_open_at = self._combine_market_time(session_date, self.MARKET_OPEN)
        market_close_at = self._combine_market_time(session_date, self.MARKET_CLOSE)
        final_scan_at = market_close_at + timedelta(minutes=self.POST_CLOSE_FINAL_SCAN_MINUTES)
        if session_date != local_now.date() or not self.market_calendar_service.is_tradable_market_day(local_now.date()):
            return market_open_at
        if local_now < market_open_at:
            return market_open_at
        if local_now < market_close_at:
            return local_now + timedelta(seconds=self.BACKGROUND_INTERVAL_SECONDS)
        if local_now < final_scan_at:
            return final_scan_at
        next_session = self.market_calendar_service.get_next_or_same_tradable_market_day(local_now.date() + timedelta(days=1))
        return self._combine_market_time(next_session, self.MARKET_OPEN)

    def _final_post_close_time(self, session_date: date) -> time:
        return (datetime.combine(session_date, self.MARKET_CLOSE, tzinfo=self.display_timezone) + timedelta(minutes=self.POST_CLOSE_FINAL_SCAN_MINUTES)).time()

    def _combine_market_time(self, session_date: date, session_time: time) -> datetime:
        return datetime.combine(session_date, session_time, tzinfo=self.display_timezone)

    def _build_slot_governance(self, open_records: List[Dict[str, Any]]) -> Dict[str, Any]:
        system_counts = Counter(normalize_system_name(item.get("system_name")) for item in open_records)
        return {
            "total_open_count": len(open_records),
            "system_counts": dict(system_counts),
        }

    def _build_slot_governance_from_trade_store(self) -> Dict[str, Any]:
        live_trades = []
        for trade in self.trade_store.list_trades("talos"):
            status = str(trade.get("derived_status_raw") or trade.get("status") or "").strip().lower()
            if status in {"open", "reduced"}:
                live_trades.append(trade)
        return self._build_slot_governance(live_trades)

    def _should_evaluate_candidate(self, system_name: str, governance: Dict[str, Any]) -> bool:
        system_count = int((governance.get("system_counts") or {}).get(system_name, 0))
        if system_count > int(self.MAX_OPEN_TRADES_PER_SYSTEM.get(system_name, 1)):
            return False
        if system_count > 0:
            return True
        return int(governance.get("total_open_count") or 0) < self.MAX_TOTAL_OPEN_TRADES

    def _can_open_trade_for_system(self, system_name: str, governance: Dict[str, Any]) -> bool:
        system_count = int((governance.get("system_counts") or {}).get(system_name, 0))
        total_open_count = int(governance.get("total_open_count") or 0)
        if total_open_count >= self.MAX_TOTAL_OPEN_TRADES:
            return False
        return system_count < int(self.MAX_OPEN_TRADES_PER_SYSTEM.get(system_name, 1))

    def _refresh_apollo_portfolio_state(
        self,
        state: Dict[str, Any],
        open_records: List[Dict[str, Any]],
        *,
        existing_plan: Dict[str, Any] | None = None,
    ) -> bool:
        balance = self._compute_balance_components(open_records)
        try:
            plan = existing_plan or self._build_apollo_portfolio_plan(
                balance["settled_balance"],
                open_records,
                learning_state=dict(state.get("learning_state") or {}),
            )
        except Exception:
            plan = None
        payload = self._serialize_apollo_portfolio_state(plan, open_records, balance)
        if payload != dict(state.get("apollo_portfolio_state") or {}):
            state["apollo_portfolio_state"] = payload
            return True
        return False

    def _build_apollo_portfolio_dashboard_payload(
        self,
        state: Dict[str, Any],
        open_records: List[Dict[str, Any]],
        balance: Dict[str, float],
    ) -> Dict[str, Any]:
        payload = dict(state.get("apollo_portfolio_state") or {})
        if payload:
            return payload
        return self._serialize_apollo_portfolio_state(None, open_records, balance)

    def _serialize_apollo_portfolio_state(
        self,
        plan: Dict[str, Any] | None,
        open_records: List[Dict[str, Any]],
        balance: Dict[str, float],
    ) -> Dict[str, Any]:
        current_open_apollo = [item for item in open_records if normalize_system_name(item.get("system_name")) == "Apollo"]
        black_swan_roi = plan.get("black_swan_roi_used") if isinstance(plan, dict) else self._average_black_swan_roi()
        current_total_premium = round(sum(float(item.get("total_premium") or 0.0) for item in current_open_apollo), 2)
        current_projected_loss = round(sum(self._projected_black_swan_loss_for_record(item, black_swan_roi) for item in current_open_apollo), 2)
        premium_target = round(max(float(balance.get("settled_balance") or 0.0), 1.0) * self.APOLLO_PREMIUM_TARGET_RATIO, 2)
        projected_loss_max = round(max(float(balance.get("settled_balance") or 0.0), 1.0) * self.APOLLO_PROJECTED_BLACK_SWAN_LOSS_CAP_RATIO, 2)
        selected_total_premium = float((plan or {}).get("selected_total_premium") or 0.0)
        projected_loss_used = float((plan or {}).get("projected_black_swan_loss_used") or 0.0)
        return {
            "premium_target": premium_target,
            "premium_target_display": self._format_currency(premium_target),
            "selected_premium": round(selected_total_premium, 2),
            "selected_premium_display": self._format_currency(selected_total_premium),
            "current_premium": round(current_total_premium, 2),
            "current_premium_display": self._format_currency(current_total_premium),
            "projected_black_swan_loss_used": round(projected_loss_used, 2),
            "projected_black_swan_loss_used_display": self._format_currency(projected_loss_used),
            "projected_black_swan_loss_current": round(current_projected_loss, 2),
            "projected_black_swan_loss_current_display": self._format_currency(current_projected_loss),
            "projected_black_swan_loss_max": projected_loss_max,
            "projected_black_swan_loss_max_display": self._format_currency(projected_loss_max),
            "black_swan_roi_used": black_swan_roi,
            "black_swan_roi_used_display": self._format_percent_value(black_swan_roi),
            "selected_profile_mix": str((plan or {}).get("selected_profile_mix") or "None"),
            "current_profile_mix": self._format_profile_mix([str(item.get("profile_label") or item.get("candidate_profile") or "Standard") for item in current_open_apollo]),
            "combination_shape": str((plan or {}).get("combination_shape") or "none"),
            "selected_portfolio_score": round(float((plan or {}).get("selected_portfolio_score") or 0.0), 2),
            "portfolio_rationale": str((plan or {}).get("portfolio_rationale") or "Talos is waiting for the next Apollo portfolio evaluation."),
            "considered_candidates_summary": str((plan or {}).get("considered_candidates_summary") or "None"),
            "combinations_evaluated_summary": str((plan or {}).get("combinations_evaluated_summary") or "None"),
            "quantity_scaling_applied": bool((plan or {}).get("quantity_scaling_applied")),
            "scaled_candidate_count": int((plan or {}).get("scaled_candidate_count") or 0),
            "default_contract_quantity_total": int((plan or {}).get("default_contract_quantity_total") or 0),
            "selected_contract_quantity_total": int((plan or {}).get("selected_contract_quantity_total") or 0),
            "default_projected_black_swan_loss_total": round(float((plan or {}).get("default_projected_black_swan_loss_total") or 0.0), 2),
            "default_projected_black_swan_loss_total_display": self._format_currency(float((plan or {}).get("default_projected_black_swan_loss_total") or 0.0)),
            "next_addition_summary": str((plan or {}).get("next_addition_summary") or "None"),
            "scaling_summary": str((plan or {}).get("scaling_summary") or "Apollo quantities were unchanged."),
            "selected_position_count": int((plan or {}).get("selected_position_count") or len((plan or {}).get("selected_candidates") or [])),
            "current_position_count": len(current_open_apollo),
            "additional_position_available": bool((plan or {}).get("additional_position_available")),
            "additional_position_count": int((plan or {}).get("additional_position_count") or 0),
            "selection_note": str((plan or {}).get("selection_note") or "Talos is waiting for the next Apollo portfolio evaluation."),
        }

    def _build_apollo_portfolio_log_line(self, plan: Dict[str, Any]) -> str:
        return (
            f"Candidates {plan.get('considered_candidates_summary') or 'None'}; "
            f"combos {plan.get('combinations_evaluated_summary') or 'None'}; "
            f"selected option {plan.get('next_addition_summary') or 'None'}; "
            f"selected {plan.get('combination_shape') or 'none'} {plan.get('selected_profile_mix') or 'None'} at {float(plan.get('selected_portfolio_score') or 0.0):.1f}; "
            f"qty {int(plan.get('default_contract_quantity_total') or 0)}->{int(plan.get('selected_contract_quantity_total') or 0)}; scaled {'yes' if plan.get('quantity_scaling_applied') else 'no'}; "
            f"Black Swan before {self._format_currency(float(plan.get('default_projected_black_swan_loss_total') or 0.0))} after {self._format_currency(float(plan.get('projected_black_swan_loss_used') or 0.0))}; "
            f"premium {self._format_currency(float(plan.get('selected_total_premium') or 0.0))} vs target {self._format_currency(float(plan.get('premium_target') or 0.0))}; "
            f"selected Black Swan loss {self._format_currency(float(plan.get('projected_black_swan_loss_used') or 0.0))}; current live {self._format_currency(float(plan.get('projected_black_swan_loss_current') or 0.0))} of {self._format_currency(float(plan.get('projected_black_swan_loss_max') or 0.0))}; "
            f"target scored as objective, not gate; why {plan.get('portfolio_rationale') or 'No rationale recorded.'}"
        )

    def _build_apollo_addition_log_line(
        self,
        plan: Dict[str, Any],
        candidate: Dict[str, Any],
        *,
        current_live_loss: float,
        candidate_loss: float,
        resulting_live_loss: float,
        black_swan_cap: float,
        was_empty_before_entry: bool,
        bootstrap_trade: bool,
        remaining_live_capacity: float,
        more_capacity_remains: bool,
    ) -> str:
        payload = dict(candidate.get("trade_payload") or {})
        default_contract_quantity = int(candidate.get("default_contract_quantity") or payload.get("contracts") or 1)
        selected_contract_quantity = int(candidate.get("selected_contract_quantity") or payload.get("contracts") or 1)
        default_candidate_loss = float(candidate.get("default_projected_black_swan_loss") or candidate_loss)
        return (
            f"Selected option {plan.get('next_addition_summary') or plan.get('selected_profile_mix') or 'None'} won because {plan.get('portfolio_rationale') or 'it carried the highest legal Apollo score'}; "
            f"portfolio empty before entry {'yes' if was_empty_before_entry else 'no'}; bootstrap trade {'yes' if bootstrap_trade else 'no'}; "
            f"Opened {payload.get('candidate_profile') or 'Apollo'} {payload.get('short_strike') or ''}/{payload.get('long_strike') or ''}; "
            f"qty {default_contract_quantity}->{selected_contract_quantity}; scaled {'yes' if selected_contract_quantity != default_contract_quantity else 'no'}; "
            f"candidate Black Swan {self._format_currency(default_candidate_loss)}->{self._format_currency(candidate_loss)}; "
            f"live {self._format_currency(current_live_loss)} + candidate {self._format_currency(candidate_loss)} = {self._format_currency(resulting_live_loss)} vs cap {self._format_currency(black_swan_cap)}; "
            f"remaining live Black Swan capacity {self._format_currency(remaining_live_capacity)}; more capacity remains {'yes' if more_capacity_remains else 'no'}; "
            f"selected {plan.get('combination_shape') or 'portfolio'} {plan.get('selected_profile_mix') or 'None'} at {float(plan.get('selected_portfolio_score') or 0.0):.1f}; "
            f"premium {self._format_currency(float(plan.get('selected_total_premium') or 0.0))} vs target {self._format_currency(float(plan.get('premium_target') or 0.0))}; "
            f"selected Black Swan loss {self._format_currency(float(plan.get('projected_black_swan_loss_used') or 0.0))}."
        )

    def _build_apollo_no_add_reason(self, plan: Dict[str, Any], apollo_open_records: List[Dict[str, Any]]) -> str:
        current_count = len(apollo_open_records)
        selected_count = int(plan.get("selected_position_count") or len([item for item in (plan.get("selected_candidates") or []) if isinstance(item, dict)]))
        if current_count >= self.APOLLO_MAX_PORTFOLIO_POSITIONS:
            return "Talos did not add another Apollo position because the Apollo portfolio is already at the 3-position cap."
        if selected_count <= current_count:
            return (
                f"Talos did not add another Apollo position because the current Apollo book already matches the highest-scoring legal option under the hard constraints: "
                f"selected {self._format_currency(float(plan.get('selected_total_premium') or 0.0))} vs target {self._format_currency(float(plan.get('premium_target') or 0.0))}; "
                f"selected Black Swan loss {self._format_currency(float(plan.get('projected_black_swan_loss_used') or 0.0))}; "
                f"current live {self._format_currency(float(plan.get('projected_black_swan_loss_current') or 0.0))} of {self._format_currency(float(plan.get('projected_black_swan_loss_max') or 0.0))}; "
                f"selected mix {plan.get('selected_profile_mix') or 'None'} / current mix {plan.get('current_profile_mix') or 'None'}."
            )
        return (
            f"Talos did not add another Apollo position because no legal Apollo option remained under the hard constraints: "
            f"selected {self._format_currency(float(plan.get('selected_total_premium') or 0.0))} vs target {self._format_currency(float(plan.get('premium_target') or 0.0))}; "
            f"selected Black Swan loss {self._format_currency(float(plan.get('projected_black_swan_loss_used') or 0.0))}; "
            f"current live {self._format_currency(float(plan.get('projected_black_swan_loss_current') or 0.0))} of {self._format_currency(float(plan.get('projected_black_swan_loss_max') or 0.0))}; "
            f"selected mix {plan.get('selected_profile_mix') or 'None'} / current mix {plan.get('current_profile_mix') or 'None'}."
        )

    def _build_apollo_live_cap_stop_reason(self, current_live_loss: float, black_swan_cap: float) -> str:
        return (
            "Talos blocked Apollo expansion because live open Apollo Black Swan loss is already at or above the hard cap: "
            f"current {self._format_currency(current_live_loss)} vs cap {self._format_currency(black_swan_cap)}."
        )

    def _build_apollo_live_cap_rejection_reason(
        self,
        *,
        current_live_loss: float,
        candidate_loss: float,
        resulting_live_loss: float,
        black_swan_cap: float,
    ) -> str:
        return (
            "Talos rejected the Apollo addition because it would exceed the hard live Black Swan cap: "
            f"current {self._format_currency(current_live_loss)} + candidate {self._format_currency(candidate_loss)} = "
            f"{self._format_currency(resulting_live_loss)} vs cap {self._format_currency(black_swan_cap)}."
        )

    def _apollo_payload_signature(self, payload: Dict[str, Any]) -> str:
        return "|".join(
            [
                str(payload.get("candidate_profile") or "Standard"),
                str(payload.get("short_strike") or ""),
                str(payload.get("long_strike") or ""),
                str(payload.get("expiration_date") or payload.get("expiration") or ""),
            ]
        )

    def _apollo_candidate_signature(self, candidate: Dict[str, Any]) -> str:
        payload = dict(candidate.get("trade_payload") or {})
        return str(candidate.get("signature") or self._apollo_payload_signature(payload))

    def _apollo_trade_signature(self, record: Dict[str, Any]) -> str:
        trade_id = int(record.get("trade_id") or 0)
        trade = self.trade_store.get_trade(trade_id) if trade_id > 0 else None
        return self._apollo_payload_signature(
            {
                "candidate_profile": (trade or {}).get("candidate_profile") or record.get("profile_label") or record.get("candidate_profile") or "Standard",
                "short_strike": (trade or {}).get("short_strike") or record.get("short_strike") or record.get("short_strike_display") or "",
                "long_strike": (trade or {}).get("long_strike") or record.get("long_strike") or record.get("long_strike_display") or "",
                "expiration_date": (trade or {}).get("expiration_date") or record.get("expiration") or record.get("expiration_date") or "",
            }
        )

    def _average_black_swan_roi(self) -> float | None:
        roi_values: List[float] = []
        for trade_mode in ("real", "simulated", "talos"):
            for trade in self.trade_store.list_trades(trade_mode):
                status = str(trade.get("derived_status_raw") or trade.get("status") or "").strip().lower()
                if status in {"open", "reduced"}:
                    continue
                if self._trade_outcome_label(trade) != "black_swan":
                    continue
                risk_basis = self._candidate_risk_basis(trade)
                if risk_basis <= 0:
                    continue
                roi_values.append(float(trade.get("gross_pnl") or 0.0) / risk_basis)
        if not roi_values:
            return None
        return sum(roi_values) / len(roi_values)

    def _projected_black_swan_loss_for_payload(self, payload: Dict[str, Any], black_swan_roi: float | None) -> float:
        risk_basis = self._candidate_risk_basis(payload)
        if risk_basis <= 0:
            return 0.0
        if black_swan_roi is None:
            return round(risk_basis, 2)
        return round(min(risk_basis, abs(float(black_swan_roi)) * risk_basis), 2)

    def _projected_black_swan_loss_for_record(self, record: Dict[str, Any], black_swan_roi: float | None) -> float:
        trade_id = int(record.get("trade_id") or 0)
        persisted_trade = self.trade_store.get_trade(trade_id) if trade_id > 0 else None
        return self._projected_black_swan_loss_for_payload(
            {
                "contracts": (persisted_trade or {}).get("contracts") or record.get("contracts") or 1,
                "spread_width": (persisted_trade or {}).get("spread_width") or record.get("spread_width"),
                "max_theoretical_risk": (persisted_trade or {}).get("max_theoretical_risk") or record.get("remaining_risk") or record.get("max_loss") or record.get("max_theoretical_risk"),
                "actual_entry_credit": (persisted_trade or {}).get("actual_entry_credit") or record.get("actual_entry_credit") or record.get("net_credit_per_contract"),
            },
            black_swan_roi,
        )

    def _candidate_risk_basis(self, payload: Dict[str, Any]) -> float:
        max_risk = self._coerce_float(payload.get("max_theoretical_risk") or payload.get("max_loss") or payload.get("remaining_risk"))
        if max_risk not in {None, 0.0}:
            return max(float(max_risk), 0.0)
        width = self._coerce_float(payload.get("spread_width")) or 0.0
        contracts = int(payload.get("contracts") or 1)
        credit = self._coerce_float(payload.get("actual_entry_credit") or payload.get("net_credit_per_contract") or payload.get("candidate_credit_estimate")) or 0.0
        return max((width - credit) * 100.0 * contracts, 0.0)

    @staticmethod
    def _format_profile_mix(profiles: List[str]) -> str:
        if not profiles:
            return "None"
        counts = Counter(str(item or "Standard") for item in profiles)
        ordered = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
        return ", ".join(f"{label} x{count}" for label, count in ordered)

    @staticmethod
    def _format_percent_value(value: float | None) -> str:
        if value is None:
            return "Fallback max-loss model"
        return f"{float(value) * 100.0:.2f}%"

    def _archive_runtime_snapshot(self, state: Dict[str, Any], talos_trades: List[Dict[str, Any]]) -> Path:
        self.archive_directory.mkdir(parents=True, exist_ok=True)
        stamp = self._now().strftime("%Y%m%d_%H%M%S")
        archive_path = self.archive_directory / f"talos_reset_{stamp}.json"
        archive_payload = {
            "archived_at": self._now().isoformat(),
            "state": state,
            "talos_trades": talos_trades,
        }
        archive_path.write_text(json.dumps(archive_payload, indent=2, sort_keys=True), encoding="utf-8")
        return archive_path

    def _score_candidate(
        self,
        *,
        system_name: str,
        candidate_profile: str,
        vix_value: float | None,
        em_multiple: float | None,
        distance_points: float | None,
        short_delta: float | None,
        expected_move: float | None,
        credit: float | None,
        spread_width: float | None,
        max_loss: float | None,
        total_premium: float | None,
        raw_candidate_score: float | None,
        structure_context: Any,
        macro_context: Any,
        learning_state: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        history = self._build_history_profile(system_name, candidate_profile, vix_value=vix_value, learning_state=learning_state)
        learning_profile = self._resolve_learning_profile(learning_state, system_name=system_name, vix_value=vix_value)
        weights = self._normalize_component_weights((learning_profile or {}).get("weights"))
        base_safety_component = self._score_safety_component(
            system_name=system_name,
            em_multiple=em_multiple,
            distance_points=distance_points,
            expected_move=expected_move,
            short_delta=short_delta,
        )
        base_credit_component = self._score_credit_efficiency_component(credit, spread_width)
        base_profit_component = self._score_profit_opportunity_component(total_premium)
        base_fit_component = self._fit_score(system_name=system_name, structure_context=structure_context, macro_context=macro_context) * 10.0
        base_standard_preference_component = self._score_standard_preference_component(system_name=system_name, candidate_profile=candidate_profile)
        safety_component = self._scale_component(base_safety_component, default_weight=32.0, adaptive_weight=weights["safety"])
        history_component = self._scale_component(history["history_component"], default_weight=22.0, adaptive_weight=weights["history"])
        credit_component = self._scale_component(base_credit_component, default_weight=14.0, adaptive_weight=weights["credit"])
        profit_component = self._scale_component(base_profit_component, default_weight=12.0, adaptive_weight=weights["profit"])
        fit_component = self._scale_component(base_fit_component, default_weight=10.0, adaptive_weight=weights["fit"])
        standard_preference_component = self._scale_component(base_standard_preference_component, default_weight=10.0, adaptive_weight=weights["standard_preference"])
        loss_penalty_component = self._scale_component(history["loss_penalty_component"], default_weight=20.0, adaptive_weight=weights["loss_penalty"])
        candidate_score = max(
            0.0,
            min(
                100.0,
                round(
                    safety_component
                    + history_component
                    + credit_component
                    + profit_component
                    + fit_component
                    + standard_preference_component
                    - loss_penalty_component,
                    2,
                ),
            ),
        )
        score_breakdown_display = (
            f"Score {candidate_score:.1f} = Safety {safety_component:.1f} + History {history_component:.1f} + "
            f"Credit {credit_component:.1f} + Profit {profit_component:.1f} + Fit {fit_component:.1f} + "
            f"Standard Preference {standard_preference_component:.1f} - "
            f"Loss Penalty {loss_penalty_component:.1f}."
        )
        return {
            "candidate_score": candidate_score,
            "selection_basis": (
                f"Talos used segmented adaptive scoring for {system_name} {candidate_profile} in {history['segment_name']}: "
                f"Safety(0-{weights['safety']:.0f}) + History(0-{weights['history']:.0f}) + CreditEfficiency(0-{weights['credit']:.0f}) + "
                f"ProfitOpportunity(0-{weights['profit']:.0f}) + Fit(0-{weights['fit']:.0f}) + "
                f"StandardPreference(0-{weights['standard_preference']:.0f}) - LossPenalty(0-{weights['loss_penalty']:.0f})."
            ),
            "score_breakdown_display": score_breakdown_display,
            "history_scope": history["scope"],
            "history_sample_count": history["sample_count"],
            "segment_name": history["segment_name"],
            "adaptive_enabled": history["adaptive_enabled"],
            "adaptive_weights": weights,
            "safety_component": round(safety_component, 2),
            "history_component": round(history_component, 2),
            "credit_component": round(credit_component, 2),
            "profit_component": round(profit_component, 2),
            "fit_component": round(fit_component, 2),
            "standard_preference_component": round(standard_preference_component, 2),
            "loss_penalty_component": round(loss_penalty_component, 2),
            "win_rate_component": round(history["win_rate_component"], 2),
            "expectancy_component": round(history["expectancy_component"], 2),
            "sample_confidence_component": round(history["sample_confidence_component"], 2),
            "em_multiple_component": round(self._score_em_multiple_component(em_multiple), 2),
            "distance_component": round(self._score_distance_component(system_name, distance_points, expected_move, em_multiple), 2),
            "short_delta_component": round(self._score_short_delta_component(short_delta), 2),
            "raw_score": float(raw_candidate_score or 0.0),
        }

    def _build_history_profile(
        self,
        system_name: str,
        candidate_profile: str,
        *,
        vix_value: float | None,
        learning_state: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        learning_profile = self._resolve_learning_profile(learning_state, system_name=system_name, vix_value=vix_value)
        if int(learning_profile.get("trade_count") or 0) <= 0:
            return {
                "scope": "neutral",
                "segment_name": self._segment_name(system_name=system_name, vix_value=vix_value),
                "adaptive_enabled": False,
                "sample_count": 0,
                "win_rate_component": 6.0,
                "expectancy_component": 4.0,
                "sample_confidence_component": 0.0,
                "history_component": 10.0,
                "average_loss_severity": 0.15,
                "black_swan_rate": 0.0,
                "loss_penalty_component": 3.0,
            }
        sample_count = int(learning_profile.get("trade_count") or 0)
        weighted_trade_count = float(learning_profile.get("weighted_trade_count") or 0.0)
        win_rate = float(learning_profile.get("weighted_win_rate") or 0.0)
        expectancy = float(learning_profile.get("weighted_expectancy") or 0.0)
        black_swan_rate = float(learning_profile.get("weighted_black_swan_rate") or 0.0)
        average_loss_severity = float(learning_profile.get("weighted_drawdown_severity") or 0.0)
        win_rate_component = 12.0 * max(0.0, min((win_rate - 0.35) / 0.45, 1.0))
        expectancy_component = 8.0 * max(0.0, min((expectancy + 25.0) / 150.0, 1.0))
        sample_confidence_component = self._sample_confidence_component(max(int(round(weighted_trade_count)), sample_count))
        history_component = win_rate_component + expectancy_component + sample_confidence_component
        loss_penalty_component = min(20.0, (12.0 * average_loss_severity) + (8.0 * black_swan_rate))
        return {
            "scope": "segment",
            "segment_name": str(learning_profile.get("segment") or self._segment_name(system_name=system_name, vix_value=vix_value)),
            "adaptive_enabled": bool(learning_profile.get("adaptive_enabled")),
            "sample_count": sample_count,
            "win_rate_component": win_rate_component,
            "expectancy_component": expectancy_component,
            "sample_confidence_component": sample_confidence_component,
            "history_component": history_component,
            "average_loss_severity": average_loss_severity,
            "black_swan_rate": black_swan_rate,
            "loss_penalty_component": loss_penalty_component,
        }

    def _refresh_learning_state(self, state: Dict[str, Any]) -> bool:
        learning_state = dict(state.get("learning_state") or {})
        prior_profiles = dict(learning_state.get("profiles") or {})
        learning_records = self._build_learning_records()
        profiles: Dict[str, Dict[str, Any]] = {}
        for segment in self.LEARNING_SEGMENTS:
            segment_records = [record for record in learning_records if record.get("segment") == segment]
            metrics = self._summarize_learning_segment(segment, segment_records)
            prior_weights = self._normalize_component_weights((prior_profiles.get(segment) or {}).get("weights"))
            if metrics["adaptive_enabled"]:
                target_weights = self._build_target_component_weights(metrics)
                metrics["weights"] = self._step_component_weights(prior_weights, target_weights)
            else:
                metrics["weights"] = self._default_component_weights()
            profiles[segment] = metrics
        active_segment = str((learning_state or {}).get("active_segment") or "Apollo_LOW_VIX")
        state["learning_state"] = {
            "active_segment": active_segment if active_segment in profiles else "Apollo_LOW_VIX",
            "profiles": profiles,
            "records": learning_records,
            "last_refreshed_at": self._now().isoformat(),
        }
        return True

    def _build_learning_records(self) -> List[Dict[str, Any]]:
        records: List[Dict[str, Any]] = []
        for trade_mode in ("real", "simulated", "talos"):
            for trade in self.trade_store.list_trades(trade_mode):
                status = str(trade.get("derived_status_raw") or trade.get("status") or "").strip().lower()
                if status in {"open", "reduced"}:
                    continue
                system_name = normalize_system_name(trade.get("system_name"))
                if system_name != "Apollo":
                    continue
                vix_value = self._coerce_float(trade.get("vix_at_entry"))
                records.append(
                    {
                        "source_type": trade_mode,
                        "source_weight": float(self.LEARNING_SOURCE_WEIGHTS.get(trade_mode, 1.0)),
                        "system": system_name,
                        "profile": str(trade.get("candidate_profile") or trade.get("pass_type") or "Standard"),
                        "segment": self._segment_name(system_name=system_name, vix_value=vix_value),
                        "vix_regime": self._vix_regime(vix_value),
                        "em_multiple": self._coerce_float(trade.get("actual_em_multiple") or trade.get("target_em")),
                        "distance_to_short": self._coerce_float(trade.get("actual_distance_to_short") or trade.get("distance_to_short")),
                        "outcome": self._trade_outcome_label(trade),
                        "realized_pnl": float(trade.get("gross_pnl") or 0.0),
                        "max_drawdown": self._coerce_float(trade.get("max_drawdown") or trade.get("max_drawdown_dollars") or trade.get("max_loss")) or 0.0,
                        "drawdown_severity": self._drawdown_severity(trade),
                        "credit_efficiency": self._credit_efficiency(trade),
                        "fit_context_available": bool(str(trade.get("structure_grade") or "").strip() or str(trade.get("macro_grade") or "").strip()),
                        "total_premium": self._coerce_float(trade.get("total_premium")),
                    }
                )
        return records

    def _summarize_learning_segment(self, segment: str, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        weighted_total = sum(float(record.get("source_weight") or 0.0) for record in records)
        weighted_wins = sum(float(record.get("source_weight") or 0.0) for record in records if float(record.get("realized_pnl") or 0.0) > 0.0)
        weighted_black_swans = sum(float(record.get("source_weight") or 0.0) for record in records if str(record.get("outcome") or "") == "black_swan")
        weighted_pnl = sum(float(record.get("realized_pnl") or 0.0) * float(record.get("source_weight") or 0.0) for record in records)
        weighted_drawdown = sum(float(record.get("drawdown_severity") or 0.0) * float(record.get("source_weight") or 0.0) for record in records)
        credit_samples = [record for record in records if record.get("credit_efficiency") is not None]
        credit_weight_total = sum(float(record.get("source_weight") or 0.0) for record in credit_samples)
        weighted_credit_efficiency = (
            sum(float(record.get("credit_efficiency") or 0.0) * float(record.get("source_weight") or 0.0) for record in credit_samples) / credit_weight_total
            if credit_weight_total
            else 0.0
        )
        win_rate = (weighted_wins / weighted_total) if weighted_total else 0.0
        expectancy = (weighted_pnl / weighted_total) if weighted_total else 0.0
        black_swan_rate = (weighted_black_swans / weighted_total) if weighted_total else 0.0
        drawdown_severity = (weighted_drawdown / weighted_total) if weighted_total else 0.0
        premium_values = [float(record.get("total_premium") or 0.0) for record in records if record.get("total_premium") not in {None, ""}]
        premium_threshold = (sum(premium_values) / len(premium_values)) if premium_values else 0.0
        high_premium_records = [record for record in records if float(record.get("total_premium") or 0.0) >= premium_threshold and premium_threshold > 0.0]
        high_premium_weight = sum(float(record.get("source_weight") or 0.0) for record in high_premium_records)
        high_premium_expectancy = (
            sum(float(record.get("realized_pnl") or 0.0) * float(record.get("source_weight") or 0.0) for record in high_premium_records) / high_premium_weight
            if high_premium_weight
            else expectancy
        )
        credit_win_records = [record for record in credit_samples if float(record.get("realized_pnl") or 0.0) > 0.0]
        credit_loss_records = [record for record in credit_samples if float(record.get("realized_pnl") or 0.0) <= 0.0]
        avg_credit_win = self._weighted_average(credit_win_records, value_key="credit_efficiency")
        avg_credit_loss = self._weighted_average(credit_loss_records, value_key="credit_efficiency")
        fit_context_ratio = (sum(1 for record in records if record.get("fit_context_available")) / len(records)) if records else 0.0
        expectancy_signal = max(0.0, min((expectancy + 50.0) / 200.0, 1.0))
        consistency_score = max(0.0, min((win_rate * 0.45) + (expectancy_signal * 0.35) + ((1.0 - drawdown_severity) * 0.20), 1.0))
        adaptive_enabled = weighted_total >= self.LEARNING_MIN_WEIGHTED_TRADES
        system_name, _, vix_regime = segment.partition("_")
        return {
            "segment": segment,
            "system": system_name,
            "vix_regime": vix_regime,
            "trade_count": len(records),
            "weighted_trade_count": round(weighted_total, 2),
            "weighted_win_rate": win_rate,
            "weighted_expectancy": expectancy,
            "weighted_black_swan_rate": black_swan_rate,
            "weighted_drawdown_severity": drawdown_severity,
            "weighted_credit_efficiency": weighted_credit_efficiency,
            "high_premium_expectancy": high_premium_expectancy,
            "credit_success_edge": avg_credit_win - avg_credit_loss,
            "consistency_score": consistency_score,
            "fit_meaningful": fit_context_ratio >= 0.6 and len(records) >= 4,
            "adaptive_enabled": adaptive_enabled,
        }

    def _build_target_component_weights(self, metrics: Dict[str, Any]) -> Dict[str, float]:
        weights = self._default_component_weights()
        black_swan_rate = float(metrics.get("weighted_black_swan_rate") or 0.0)
        consistency_score = float(metrics.get("consistency_score") or 0.0)
        high_premium_expectancy = float(metrics.get("high_premium_expectancy") or 0.0)
        expectancy = float(metrics.get("weighted_expectancy") or 0.0)
        credit_success_edge = float(metrics.get("credit_success_edge") or 0.0)
        if black_swan_rate >= 0.15:
            weights["safety"] += 4.0
            weights["loss_penalty"] += 4.0
        elif black_swan_rate >= 0.08:
            weights["safety"] += 2.0
            weights["loss_penalty"] += 2.0
        if consistency_score >= 0.68:
            weights["history"] += 4.0
        elif consistency_score >= 0.58:
            weights["history"] += 2.0
        elif consistency_score < 0.45:
            weights["history"] -= 2.0
        if high_premium_expectancy < (expectancy - 25.0):
            weights["profit"] -= 4.0
        elif high_premium_expectancy < expectancy:
            weights["profit"] -= 2.0
        if credit_success_edge >= 0.02:
            weights["credit"] += 4.0
        elif credit_success_edge >= 0.01:
            weights["credit"] += 2.0
        if metrics.get("fit_meaningful"):
            if consistency_score >= 0.65:
                weights["fit"] += 2.0
            elif black_swan_rate >= 0.15:
                weights["fit"] -= 2.0
        return self._normalize_component_weights(weights)

    def _resolve_learning_profile(
        self,
        learning_state: Dict[str, Any] | None,
        *,
        system_name: str,
        vix_value: float | None,
    ) -> Dict[str, Any]:
        if learning_state is None:
            learning_state = dict((self._load_state().get("learning_state") or {}))
        segment = self._segment_name(system_name=system_name, vix_value=vix_value)
        profiles = dict((learning_state or {}).get("profiles") or {})
        profile = dict(profiles.get(segment) or {})
        if not profile:
            profile = {
                "segment": segment,
                "trade_count": 0,
                "weighted_trade_count": 0.0,
                "weighted_win_rate": 0.0,
                "weighted_expectancy": 0.0,
                "weighted_black_swan_rate": 0.0,
                "weighted_drawdown_severity": 0.15,
                "adaptive_enabled": False,
                "weights": self._default_component_weights(),
            }
        return profile

    def _segment_name(self, *, system_name: str, vix_value: float | None) -> str:
        return f"{normalize_system_name(system_name)}_{self._vix_regime(vix_value)}"

    @staticmethod
    def _vix_regime(vix_value: float | None) -> str:
        return "HIGH_VIX" if float(vix_value or 0.0) >= 19.0 else "LOW_VIX"

    def _default_component_weights(self) -> Dict[str, float]:
        return dict(self.DEFAULT_COMPONENT_WEIGHTS)

    def _normalize_component_weights(self, weights: Dict[str, Any] | None) -> Dict[str, float]:
        normalized: Dict[str, float] = {}
        for key, default_value in self.DEFAULT_COMPONENT_WEIGHTS.items():
            minimum, maximum = self.COMPONENT_WEIGHT_BOUNDS[key]
            value = self._coerce_float((weights or {}).get(key))
            normalized[key] = max(minimum, min(maximum, float(value if value is not None else default_value)))
        return normalized

    def _step_component_weights(self, current: Dict[str, Any] | None, target: Dict[str, Any]) -> Dict[str, float]:
        current_weights = self._normalize_component_weights(current)
        target_weights = self._normalize_component_weights(target)
        stepped: Dict[str, float] = {}
        for key, current_value in current_weights.items():
            delta = target_weights[key] - current_value
            bounded_delta = max(-self.MAX_WEIGHT_CHANGE_PER_CYCLE, min(self.MAX_WEIGHT_CHANGE_PER_CYCLE, delta))
            stepped[key] = round(current_value + bounded_delta, 2)
        return self._normalize_component_weights(stepped)

    @staticmethod
    def _scale_component(raw_value: float, *, default_weight: float, adaptive_weight: float) -> float:
        if default_weight <= 0:
            return max(float(raw_value or 0.0), 0.0)
        return round(max(float(raw_value or 0.0), 0.0) * (float(adaptive_weight) / float(default_weight)), 2)

    def _trade_outcome_label(self, trade: Dict[str, Any]) -> str:
        outcome = str(trade.get("win_loss_result") or "").strip().lower()
        if outcome == "black swan":
            return "black_swan"
        if float(trade.get("gross_pnl") or 0.0) > 0.0:
            return "win"
        if float(trade.get("gross_pnl") or 0.0) < 0.0:
            return "loss"
        return "flat"

    def _drawdown_severity(self, trade: Dict[str, Any]) -> float:
        max_drawdown = self._coerce_float(trade.get("max_drawdown") or trade.get("max_drawdown_dollars"))
        max_loss = self._coerce_float(trade.get("max_theoretical_risk") or trade.get("max_loss") or trade.get("max_theoretical_loss"))
        if max_drawdown not in {None, 0.0} and max_loss not in {None, 0.0}:
            return min(1.0, abs(float(max_drawdown)) / float(max_loss))
        return self._loss_severity(trade)

    def _credit_efficiency(self, trade: Dict[str, Any]) -> float | None:
        credit = self._coerce_float(trade.get("actual_entry_credit") or trade.get("net_credit_per_contract") or trade.get("candidate_credit_estimate"))
        spread_width = self._coerce_float(trade.get("spread_width"))
        if credit in {None, 0.0} or spread_width in {None, 0.0}:
            return None
        return max(float(credit) / float(spread_width), 0.0)

    def _weighted_average(self, records: List[Dict[str, Any]], *, value_key: str) -> float:
        weighted_total = sum(float(record.get("source_weight") or 0.0) for record in records)
        if weighted_total <= 0:
            return 0.0
        return sum(float(record.get(value_key) or 0.0) * float(record.get("source_weight") or 0.0) for record in records) / weighted_total

    def _score_safety_component(
        self,
        *,
        system_name: str,
        em_multiple: float | None,
        distance_points: float | None,
        expected_move: float | None,
        short_delta: float | None,
    ) -> float:
        return (
            self._score_em_multiple_component(em_multiple)
            + self._score_distance_component(system_name, distance_points, expected_move, em_multiple)
            + self._score_short_delta_component(short_delta)
        )

    @staticmethod
    def _score_em_multiple_component(em_multiple: float | None) -> float:
        value = float(em_multiple or 0.0)
        if value >= 2.0:
            return 18.0
        if value >= 1.8:
            return 16.0
        if value >= 1.6:
            return 13.0
        if value >= 1.4:
            return 9.0
        if value >= 1.2:
            return 5.0
        return 0.0

    def _score_distance_component(
        self,
        system_name: str,
        distance_points: float | None,
        expected_move: float | None,
        em_multiple: float | None,
    ) -> float:
        distance_value = max(float(distance_points or 0.0), 0.0)
        target_distance = self._resolve_target_distance(system_name, distance_value, expected_move, em_multiple)
        if target_distance <= 0:
            return 0.0
        return min(14.0, max(0.0, (distance_value / target_distance) * 14.0))

    def _resolve_target_distance(self, system_name: str, distance_points: float, expected_move: float | None, em_multiple: float | None) -> float:
        normalized_system = normalize_system_name(system_name)
        target_em_multiple = 1.8 if normalized_system == "Apollo" else 1.5
        expected_move_value = self._coerce_float(expected_move)
        em_multiple_value = self._coerce_float(em_multiple)
        if expected_move_value is None and em_multiple_value not in {None, 0.0} and distance_points > 0:
            expected_move_value = distance_points / float(em_multiple_value)
        if expected_move_value is None:
            return 55.0 if normalized_system == "Apollo" else 32.0
        return max(expected_move_value * target_em_multiple, 1.0)

    @staticmethod
    def _score_short_delta_component(short_delta: float | None) -> float:
        value = abs(float(short_delta or 0.0))
        if value <= 0.08:
            return 8.0
        if value <= 0.12:
            return 6.0
        if value <= 0.16:
            return 4.0
        if value <= 0.20:
            return 2.0
        return 0.0

    @staticmethod
    def _score_credit_efficiency_component(credit: float | None, spread_width: float | None) -> float:
        if credit in {None, 0.0} or spread_width in {None, 0.0}:
            return 0.0
        ratio = max(float(credit) / float(spread_width), 0.0)
        return min(15.0, (ratio / 0.20) * 15.0)

    @staticmethod
    def _score_profit_opportunity_component(total_premium: float | None) -> float:
        if total_premium in {None, 0.0}:
            return 0.0
        normalized = max(float(total_premium), 0.0)
        return min(12.0, ((normalized / 600.0) ** 0.5) * 12.0)

    @staticmethod
    def _score_standard_preference_component(*, system_name: str, candidate_profile: str) -> float:
        if normalize_system_name(system_name) != "Apollo":
            return 0.0
        profile_value = str(candidate_profile or "").strip().lower()
        if "standard" in profile_value:
            return 10.0
        if "balanced" in profile_value or "hybrid" in profile_value:
            return 7.0
        if "fortress" in profile_value:
            return 3.0
        return 1.0

    @staticmethod
    def _sample_confidence_component(sample_count: int) -> float:
        if sample_count >= 20:
            return 5.0
        if sample_count >= 12:
            return 4.0
        if sample_count >= 8:
            return 3.0
        if sample_count >= 4:
            return 2.0
        if sample_count >= 2:
            return 1.0
        return 0.0

    def _filter_history_records(self, *, system_name: str, candidate_profile: str | None) -> List[Dict[str, Any]]:
        records: List[Dict[str, Any]] = []
        for trade_mode in ("real", "simulated"):
            for trade in self.trade_store.list_trades(trade_mode):
                status = str(trade.get("derived_status_raw") or trade.get("status") or "").strip().lower()
                if status in {"open", "reduced"}:
                    continue
                if normalize_system_name(trade.get("system_name")) != system_name:
                    continue
                if candidate_profile is not None:
                    profile_value = str(trade.get("candidate_profile") or trade.get("pass_type") or "").strip().lower()
                    if profile_value != str(candidate_profile).strip().lower():
                        continue
                records.append(trade)
        return records

    def _loss_severity(self, trade: Dict[str, Any]) -> float:
        gross_pnl = abs(float(trade.get("gross_pnl") or 0.0))
        max_loss = self._coerce_float(trade.get("max_theoretical_risk") or trade.get("max_loss") or trade.get("max_theoretical_loss"))
        if max_loss in {None, 0.0}:
            return self._clamp_ratio(gross_pnl, ceiling=1000.0)
        return min(1.0, gross_pnl / max_loss)

    def _fit_score(self, *, system_name: str, structure_context: Any, macro_context: Any) -> float:
        structure_score = self._qualitative_score(structure_context, good_terms=("good", "allowed", "healthy", "balanced"), poor_terms=("poor", "stand aside", "blocked", "risk-off"), neutral=0.65)
        macro_score = self._qualitative_score(macro_context, good_terms=("none", "minor", "neutral"), poor_terms=("major", "high", "blocked", "risk"), neutral=0.6)
        return min(1.0, (0.7 * structure_score) + (0.3 * macro_score))

    def _qualitative_score(self, value: Any, *, good_terms: tuple[str, ...], poor_terms: tuple[str, ...], neutral: float) -> float:
        text = str(value or "").strip().lower()
        if not text:
            return neutral
        if any(term in text for term in poor_terms):
            return 0.25
        if any(term in text for term in good_terms):
            return 0.9
        return neutral

    @staticmethod
    def _clamp_ratio(value: float | None, *, ceiling: float) -> float:
        if value is None or ceiling <= 0:
            return 0.0
        return max(0.0, min(float(value) / ceiling, 1.0))

    @staticmethod
    def _coerce_float(value: Any) -> float | None:
        if value in (None, ""):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _format_market_day(self, value: Any) -> str:
        if value is None:
            return ""
        if hasattr(value, "isoformat"):
            return value.isoformat()
        return str(value)

    @staticmethod
    def _average(values) -> float:
        items = [float(value) for value in values if value not in {None, ""}]
        if not items:
            return 0.0
        return sum(items) / len(items)

    def _version_number(self) -> str:
        return str(self.config.app_version_label or "Version 7.2.11").replace("Version", "").strip() or "7.2.11"

    def _format_currency(self, value: float) -> str:
        return f"${value:,.2f}"

    def _format_datetime(self, value: Any) -> str:
        if not value:
            return "Not yet evaluated"
        if isinstance(value, datetime):
            stamp = value
        else:
            stamp = datetime.fromisoformat(str(value))
        if stamp.tzinfo is None:
            stamp = stamp.replace(tzinfo=self.display_timezone)
        return stamp.astimezone(self.display_timezone).strftime("%Y-%m-%d %I:%M:%S %p %Z")

    def _format_activity_log_timestamp(self, value: Any) -> str:
        if not value:
            return "Not yet evaluated"
        if isinstance(value, datetime):
            stamp = value
        else:
            stamp = datetime.fromisoformat(str(value))
        if stamp.tzinfo is None:
            stamp = stamp.replace(tzinfo=self.display_timezone)
        return f"{stamp.astimezone(self.display_timezone).strftime('%m-%d-%y %H:%M')} CST"

    def _now(self) -> datetime:
        return self.now_provider()
