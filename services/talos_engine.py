"""Talos 3.10 master operating mode orchestration."""

from __future__ import annotations

import copy
import json
import logging
import re
from datetime import date, datetime, time, timedelta
from pathlib import Path
from threading import RLock
from time import perf_counter
from typing import Any, Dict
from zoneinfo import ZoneInfo

from config import AppConfig, HOSTED_APP_VERSION, get_app_config
from services.apollo_service import ApolloService
from services.open_trade_manager import OpenTradeManager
from services.providers.base_provider import ProviderAuthRequiredError, ProviderError, ProviderReauthenticationRequiredError
from services.repositories.apollo_snapshot_repository import ApolloSnapshotRepository
from services.repositories.strategy_repository import build_default_strategy_snapshots
from services.runtime.scheduler import RuntimeJobHandle, RuntimeScheduler, ThreadingTimerScheduler
from services.runtime_metrics_service import get_runtime_metrics_service
from services.schwab_trading_auth_service import SchwabTradingAuthService
from services.talos_execution_service import TalosExecutionService
from services.repositories.trade_repository import TradeRepository
from services.trade_store import JOURNAL_NAME_DEFAULT, normalize_trade_mode, resolve_trade_system_name


LOGGER = logging.getLogger(__name__)


class TalosEngine:
    """Own Talos 3.10 mode control, shared-journal sizing, and Talos lifecycle management."""

    MODE_INACTIVE = "INACTIVE"
    MODE_SIMULATED = "SIMULATED"
    MODE_ACTIVE = "ACTIVE"
    MASTER_MODES = (MODE_INACTIVE, MODE_SIMULATED, MODE_ACTIVE)
    SIMULATED_OPEN_WINDOW_MINUTES = 10
    SIMULATED_CLOSE_WINDOW_MINUTES = 15
    MAX_BLACK_SWAN_ALLOCATION_RATIO = 0.05
    DEFAULT_MANUAL_ACCOUNT_VALUE = 135000.0
    MAX_ACTIVITY_ITEMS = 60
    MAX_MONITOR_RECORDS = 12
    BACKGROUND_MONITOR_INTERVAL_SECONDS = 120
    REGULAR_MARKET_OPEN = time(8, 30)
    REGULAR_MARKET_CLOSE = time(15, 0)
    TIMING_BEFORE_OPEN_WINDOW = "BEFORE_OPEN_WINDOW"
    TIMING_OPEN_WINDOW_ACTIVE = "OPEN_WINDOW_ACTIVE"
    TIMING_MARKET_CLOSED = "MARKET_CLOSED"
    TIMING_EXIT_ONLY_WINDOW = "EXIT_ONLY_WINDOW"

    def __init__(
        self,
        *,
        trade_store: TradeRepository,
        apollo_service: ApolloService,
        open_trade_manager: OpenTradeManager,
        execution_auth_service: SchwabTradingAuthService | None = None,
        order_service: TalosExecutionService | None = None,
        apollo_snapshot_repository: ApolloSnapshotRepository | None = None,
        config: AppConfig | None = None,
        scheduler: RuntimeScheduler | None = None,
        state_path: str | Path,
    ) -> None:
        self.trade_store = trade_store
        self.apollo_service = apollo_service
        self.open_trade_manager = open_trade_manager
        self.apollo_snapshot_repository = apollo_snapshot_repository
        self.config = config or get_app_config()
        self.execution_auth_service = execution_auth_service or SchwabTradingAuthService(config=self.config)
        self.order_service = order_service or TalosExecutionService(execution_auth_service=self.execution_auth_service, config=self.config)
        self.state_path = Path(state_path)
        self.scheduler = scheduler or ThreadingTimerScheduler()
        self.display_timezone = ZoneInfo(self.config.app_timezone)
        self._lock = RLock()
        self._monitor_lock = RLock()
        self._monitor_timer: RuntimeJobHandle | None = None
        self._monitor_running = False
        self._candidate_payload_cache: Dict[str, Any] | None = None
        self._candidate_payload_cache_expires_at: datetime | None = None

    def initialize(self) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock:
            state = self._load_state()
            self._reconcile_expired_talos_trades(state, now=self._now(), caller_source="startup")
            self._save_state(state)

    def start_background_monitoring(self) -> None:
        with self._monitor_lock:
            if self._monitor_running:
                return
            self._monitor_running = True
            self._schedule_background_monitor()

    def shutdown(self) -> None:
        with self._monitor_lock:
            self._monitor_running = False
            if self._monitor_timer is not None:
                self._monitor_timer.cancel()
                self._monitor_timer = None

    def _schedule_background_monitor(self) -> None:
        if not self._monitor_running:
            return
        self._monitor_timer = self.scheduler.schedule(
            self.BACKGROUND_MONITOR_INTERVAL_SECONDS,
            self._background_monitor_tick,
            daemon=True,
        )

    def _background_monitor_tick(self) -> None:
        try:
            self.run_background_monitor_cycle()
        except Exception as exc:  # pragma: no cover - defensive scheduler guard
            LOGGER.warning("Talos background monitor failed: %s", exc)
        finally:
            with self._monitor_lock:
                if self._monitor_running:
                    self._schedule_background_monitor()

    def run_background_monitor_cycle(self) -> Dict[str, Any]:
        started = perf_counter()
        with self._lock:
            state = self._load_state()
            now = self._now()
            self._sync_open_trade_manager_gate_test_hook(state)
            self._reconcile_expired_talos_trades(state, now=now, caller_source="background-monitor")
            if not self._is_regular_session(now):
                self._record_monitor_snapshot(
                    state,
                    now=now,
                    records=[],
                    market_status="Outside regular market hours",
                    current_spx_display="—",
                    current_vix_display="—",
                    live_expected_move_display="—",
                )
                self._save_state(state)
                result = self._build_monitor_loop_payload(state)
                get_runtime_metrics_service().record(
                    "Talos monitor loop",
                    (perf_counter() - started) * 1000.0,
                    detail="outside-regular-session",
                )
                return result

            management_payload = self.open_trade_manager.evaluate_open_trades(
                send_alerts=False,
                caller_source="job:talos-monitor-loop",
            )
            talos_records = [
                dict(item)
                for item in (management_payload.get("records") or [])
                if isinstance(item, dict) and str(item.get("system_name") or "") == "Talos"
            ]
            if self._management_payload_has_market_data_issue(management_payload):
                self._record_monitor_snapshot(
                    state,
                    now=now,
                    records=talos_records,
                    market_status="Market data unavailable - reconnect required",
                    current_spx_display="Unavailable",
                    current_vix_display="Unavailable",
                    live_expected_move_display="Unavailable",
                )
                self._save_state(state)
                result = self._build_monitor_loop_payload(state)
                get_runtime_metrics_service().record(
                    "Talos monitor loop",
                    (perf_counter() - started) * 1000.0,
                    detail="market-data-unavailable",
                )
                return result
            owned_trades = self._load_talos_trades()
            delta_payload = self._build_monitor_delta_payload(now=now, owned_trades=owned_trades)
            if self._can_skip_monitor_recalculation(state, delta_payload=delta_payload, owned_trades=owned_trades):
                self._record_monitor_snapshot(
                    state,
                    now=now,
                    records=[],
                    market_status="No open Talos positions",
                    current_spx_display=delta_payload["current_spx_display"],
                    current_vix_display=delta_payload["current_vix_display"],
                    live_expected_move_display=delta_payload["live_expected_move_display"],
                )
                state["monitor_loop"]["delta_signature"] = delta_payload["signature"]
                state["monitor_loop"]["timing_state"] = delta_payload["timing_state"]
                state["monitor_loop"]["last_delta_reason"] = "cache-reuse"
                self._save_state(state)
                result = self._build_monitor_loop_payload(state)
                get_runtime_metrics_service().record(
                    "Talos monitor loop",
                    (perf_counter() - started) * 1000.0,
                    cache_hit=True,
                    detail="delta-skip",
                )
                return result
            trades_by_id = {
                int(trade.get("id") or 0): trade
                for trade in owned_trades
                if int(trade.get("id") or 0) > 0
            }
            for record in talos_records:
                self._update_monitored_trade_state(record, trades_by_id, now)
            self._track_monitor_actions(state, talos_records, trades_by_id, now)
            self._record_monitor_snapshot(
                state,
                now=now,
                records=talos_records,
                market_status=("Monitoring live Talos positions" if talos_records else "No open Talos positions"),
                current_spx_display=self._extract_market_snapshot_display(
                    (management_payload.get("header_market_snapshots") or {}).get("^GSPC"),
                    fallback=next((item.get("current_underlying_price_display") for item in talos_records if item.get("current_underlying_price_display")), "—"),
                ),
                current_vix_display=self._extract_market_snapshot_display(
                    (management_payload.get("header_market_snapshots") or {}).get("^VIX"),
                    fallback=next((item.get("current_vix_display") for item in talos_records if item.get("current_vix_display")), "—"),
                ),
                live_expected_move_display=str(management_payload.get("live_expected_move_display") or "—"),
            )
            state["monitor_loop"]["delta_signature"] = delta_payload["signature"]
            state["monitor_loop"]["timing_state"] = delta_payload["timing_state"]
            state["monitor_loop"]["last_delta_reason"] = ("full-recompute" if talos_records else "snapshot-refresh")
            self._save_state(state)
            result = self._build_monitor_loop_payload(state)
            get_runtime_metrics_service().record(
                "Talos monitor loop",
                (perf_counter() - started) * 1000.0,
                detail="completed",
            )
            return result

    def get_dashboard_payload(self, *, force_refresh: bool = False, caller_source: str = "talos-dashboard") -> Dict[str, Any]:
        started = perf_counter()
        with self._lock:
            state = self._load_state()
            self._sync_open_trade_manager_gate_test_hook(state)
            reconcile_report = self._reconcile_expired_talos_trades(state, now=self._now(), caller_source=caller_source)
            state_changed = bool(reconcile_report.get("changed")) or self._run_automatic_checks(state, caller_source=caller_source)
            account_payload = self._build_account_payload(state, force_refresh=force_refresh)
            candidate_payload = self._build_candidate_payload(
                force_refresh=force_refresh,
                caller_source=caller_source,
                allow_snapshot=False,
            )
            owned_trades = self._load_talos_trades()
            sizing_payload = self._build_sizing_payload(account_payload, candidate_payload, owned_trades)
            management_evaluator = getattr(self.open_trade_manager, "evaluate_open_trades", None)
            management_payload = (
                management_evaluator(
                    send_alerts=False,
                    caller_source=f"job:{caller_source}:talos-page",
                )
                if callable(management_evaluator)
                else {"records": []}
            )
            talos_management_records = [
                dict(item)
                for item in (management_payload.get("records") or [])
                if isinstance(item, dict) and str(item.get("system_name") or "") == "Talos"
            ]
            market_data_unavailable = self._management_payload_has_market_data_issue(management_payload) or self._candidate_payload_has_market_data_issue(candidate_payload)
            if market_data_unavailable:
                candidate_payload = self._build_market_data_unavailable_candidate_payload(candidate_payload)
                sizing_payload = self._build_market_data_unavailable_sizing_payload(sizing_payload)
            decision_payload = self._build_decision_payload(
                state=state,
                account_payload=account_payload,
                candidate_payload=candidate_payload,
                sizing_payload=sizing_payload,
                owned_trades=owned_trades,
            )
            if market_data_unavailable:
                decision_payload.update(
                    {
                        "phase_label": "Unavailable",
                        "decision_label": "Cannot Evaluate",
                        "reason": "Decision Engine unavailable due to market-data auth. Reconnect Schwab market data.",
                    }
                )
            if state_changed:
                self._save_state(state)
            result = {
                "master_mode": self._build_master_mode_payload(state),
                "timing_status": self._build_timing_status_payload(),
                "status_summary": self._build_status_summary(state, decision_payload, owned_trades),
                "engine_health": self._build_engine_health_payload(
                    state,
                    market_data_connected=not market_data_unavailable,
                    execution_connected=bool(account_payload.get("execution_connected")),
                    journal_available=True,
                ),
                "account": account_payload,
                "candidate": candidate_payload,
                "decision_engine": decision_payload,
                "parameters": self._build_parameter_rows(state),
                "gate_test": self._build_gate_test_payload(state),
                "gate_status": self._build_gate_status_payload(market_data_unavailable=market_data_unavailable),
                "real_execution": self._build_real_execution_payload(state),
                "trade_summary": self._build_trade_summary(owned_trades),
                "recent_trades": self._build_recent_trades(owned_trades, talos_management_records),
                "monitor_loop": self._build_monitor_loop_payload(state),
                "activity_log": list(state.get("activity_log") or []),
                "performance_report": get_runtime_metrics_service().build_report(top_n=5),
                "evaluated_at_display": self._format_timestamp(self._now()),
            }
            get_runtime_metrics_service().record(
                "Talos dashboard payload",
                (perf_counter() - started) * 1000.0,
                detail=str(caller_source or "talos-dashboard"),
            )
            return result

    @staticmethod
    def _management_payload_has_market_data_issue(payload: Dict[str, Any]) -> bool:
        return not bool(payload.get("market_data_available", True))

    def _candidate_payload_has_market_data_issue(self, payload: Dict[str, Any]) -> bool:
        return self._is_market_data_auth_issue(payload.get("error") or payload.get("summary") or payload.get("block_reason"))

    def _build_market_data_unavailable_candidate_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        updated = dict(payload)
        updated.update(
            {
                "ready": False,
                "summary": "Apollo Fortress candidate unavailable until Schwab market-data reconnect completes.",
                "error": "Schwab market-data reconnect required.",
                "block_reason": "market-data auth unavailable",
                "short_strike": "—",
                "long_strike": "—",
                "expiration_date": "—",
                "premium_per_contract": "—",
                "spread_width": "—",
                "distance_to_short": "—",
                "em_multiple": "—",
                "projected_black_swan_loss_per_contract": "—",
                "projected_black_swan_loss_per_contract_value": None,
                "max_theoretical_loss_per_contract": "—",
                "max_theoretical_loss_per_contract_value": None,
                "rationale": [],
                "diagnostics": ["Decision Engine unavailable due to market-data auth. Reconnect Schwab market data."],
            }
        )
        return updated

    @staticmethod
    def _build_market_data_unavailable_sizing_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
        updated = dict(payload)
        updated.update(
            {
                "max_contracts_allowed": 0,
                "max_contracts_display": "0",
                "contracts_selected": 0,
                "contracts_selected_display": "0",
                "projected_total_black_swan_loss": 0.0,
                "projected_total_black_swan_loss_display": "$0.00",
                "projected_total_theoretical_loss": 0.0,
                "projected_total_theoretical_loss_display": "$0.00",
                "projected_exposure": 0.0,
                "projected_exposure_display": "$0.00",
                "projected_exposure_after_trade": updated.get("open_exposure") or 0.0,
                "projected_exposure_after_trade_display": updated.get("open_exposure_display") or "$0.00",
                "remaining_capacity_after_trade": updated.get("remaining_capacity"),
                "remaining_capacity_after_trade_display": updated.get("remaining_capacity_display") or "$0.00",
                "block_reason": "market-data auth unavailable",
                "sizing_note": "Talos sizing is unavailable because Schwab market data needs to reconnect.",
            }
        )
        return updated

    @staticmethod
    def _build_gate_status_payload(*, market_data_unavailable: bool) -> Dict[str, Any]:
        if market_data_unavailable:
            return {
                "label": "Unavailable / stale",
                "tone": "warning",
                "message": "Talos Fortress exit gates are unavailable until Schwab market-data reconnect completes.",
            }
        return {
            "label": "Live",
            "tone": "info",
            "message": "Talos Fortress exit gates are using live Schwab market data.",
        }

    @staticmethod
    def _is_market_data_auth_issue(value: Any) -> bool:
        text = str(value or "").strip().lower()
        if not text:
            return False
        return any(
            token in text
            for token in (
                "providerreauthenticationrequirederror",
                "providerauthrequirederror",
                "unable to authenticate with schwab right now",
                "please log in again",
                "market-data reconnect required",
                "market data reconnect required",
                "status_code=400",
            )
        )

    def reconcile_expired_talos_trades(self, *, caller_source: str = "manual") -> Dict[str, Any]:
        with self._lock:
            state = self._load_state()
            report = self._reconcile_expired_talos_trades(state, now=self._now(), caller_source=caller_source)
            if report.get("changed"):
                self._save_state(state)
            return report

    def update_settings(self, values: Dict[str, Any]) -> Dict[str, Any]:
        with self._lock:
            state = self._load_state()
            overrides = dict(state.get("parameter_overrides") or {})
            current_mode = self._normalize_master_mode(state.get("master_mode"))
            requested_mode = self._normalize_master_mode(values.get("master_mode") or current_mode)

            state["master_mode"] = requested_mode
            manual_account_value = self._parse_numeric(values.get("manual_account_value"))
            if "manual_account_value" in values:
                state["manual_account_value"] = round(float(manual_account_value), 2) if manual_account_value is not None else self.DEFAULT_MANUAL_ACCOUNT_VALUE
            if "gate_test_scenario" in values:
                state["gate_test_scenario"] = self._normalize_gate_test_scenario(values.get("gate_test_scenario"))
            for field_name in (
                "fortress_base_width",
                "fortress_base_contracts",
                "fortress_neighborhood_range",
                "fortress_max_loss_cap_dollars",
            ):
                parsed_value = self._parse_numeric(values.get(field_name))
                if parsed_value is not None:
                    overrides[field_name] = parsed_value
            state["parameter_overrides"] = overrides
            if requested_mode != current_mode:
                self._append_activity(
                    state,
                    event_type="mode-change",
                    result="ok",
                    reason=f"Master mode changed from {current_mode} to {requested_mode}.",
                    details=f"Talos {HOSTED_APP_VERSION} uses the Master Operating Mode switch as the primary control surface.",
                    mode=requested_mode,
                )
            self._save_state(state)
            return {"ok": True, "level": "info", "message": f"Saved Talos {HOSTED_APP_VERSION} settings. Active mode: {requested_mode}."}

    def get_gate_test_scenario(self) -> str:
        with self._lock:
            return self._resolve_gate_test_scenario(self._load_state())

    def _sync_open_trade_manager_gate_test_hook(self, state: Dict[str, Any]) -> None:
        setter = getattr(self.open_trade_manager, "set_talos_gate_test_hook", None)
        if callable(setter):
            setter(self._resolve_gate_test_scenario(state))

    def queue_manual_execution_test(self, values: Dict[str, Any]) -> Dict[str, Any]:
        del values
        with self._lock:
            state = self._load_state()
            self._append_activity(
                state,
                event_type="legacy-control",
                result="blocked",
                reason=f"Legacy manual draft routing is disabled in Talos {HOSTED_APP_VERSION}.",
                details="Use the Master Operating Mode panel and the simulated open/close checks instead.",
            )
            self._save_state(state)
        return {
            "ok": False,
            "level": "warning",
            "message": f"Legacy manual Talos draft routing is disabled in Talos {HOSTED_APP_VERSION}.",
        }

    def run_simulated_open_check(self, *, trigger_reason: str = "manual") -> Dict[str, Any]:
        with self._lock:
            state = self._load_state()
            result = self._execute_simulated_open_check(state, trigger_reason=trigger_reason, enforce_time_window=False)
            self._save_state(state)
            return result

    def run_simulated_close_check(self, *, trigger_reason: str = "manual") -> Dict[str, Any]:
        with self._lock:
            state = self._load_state()
            result = self._execute_simulated_close_check(state, trigger_reason=trigger_reason, enforce_time_window=False)
            self._save_state(state)
            return result

    def run_real_open_check(self, *, trigger_reason: str = "manual", confirmation_text: str = "") -> Dict[str, Any]:
        with self._lock:
            state = self._load_state()
            result = self._execute_real_open_check(state, trigger_reason=trigger_reason, confirmation_text=confirmation_text)
            self._save_state(state)
            return result

    def run_real_close_check(self, *, trigger_reason: str = "manual", confirmation_text: str = "", manual_override: bool = False) -> Dict[str, Any]:
        with self._lock:
            state = self._load_state()
            result = self._execute_real_close_check(
                state,
                trigger_reason=trigger_reason,
                confirmation_text=confirmation_text,
                manual_override=manual_override,
            )
            self._save_state(state)
            return result

    def _run_automatic_checks(self, state: Dict[str, Any], *, caller_source: str) -> bool:
        changed = False
        mode = self._normalize_master_mode(state.get("master_mode"))
        if mode != self.MODE_SIMULATED:
            return changed
        now = self._now()
        if not self._is_regular_session(now):
            return changed
        today_key = now.date().isoformat()
        minutes_left = self._minutes_to_regular_close(now)
        if minutes_left <= self.SIMULATED_CLOSE_WINDOW_MINUTES and str(state.get("last_auto_close_check_on") or "") != today_key:
            self._execute_simulated_close_check(state, trigger_reason=f"auto:{caller_source}", enforce_time_window=True)
            state["last_auto_close_check_on"] = today_key
            changed = True
        if str(state.get("last_auto_open_check_on") or "") != today_key:
            if not self._allow_autonomous_open_attempt(
                state,
                event_type="simulated-open-check",
                trigger_reason=f"auto:{caller_source}",
                now=now,
            ):
                return True
            self._execute_simulated_open_check(state, trigger_reason=f"auto:{caller_source}", enforce_time_window=True)
            state["last_auto_open_check_on"] = today_key
            changed = True
        return changed

    def _execute_simulated_open_check(
        self,
        state: Dict[str, Any],
        *,
        trigger_reason: str,
        enforce_time_window: bool,
    ) -> Dict[str, Any]:
        mode = self._normalize_master_mode(state.get("master_mode"))
        manual_forced = self._is_manual_forced_trigger(trigger_reason)
        self._append_activity(
            state,
            event_type="simulated-open-check",
            result="started",
            reason=("Manual forced simulated open" if manual_forced else f"Simulated open check started ({trigger_reason})."),
            details=(
                "Timing window bypassed by operator. Talos is evaluating Apollo Fortress with real Schwab market and account data."
                if manual_forced
                else "Talos is evaluating Apollo Fortress with real Schwab market and account data."
            ),
            mode=mode,
        )
        if mode != self.MODE_SIMULATED:
            return self._blocked_result(
                state,
                event_type="simulated-open-check",
                reason="INACTIVE mode" if mode == self.MODE_INACTIVE else f"ACTIVE execution routing is not enabled in Talos {HOSTED_APP_VERSION}.",
                details="Talos only creates simulated trades when Master Operating Mode is SIMULATED.",
            )
        if enforce_time_window:
            timing_result = self._enforce_autonomous_open_timing(
                state,
                event_type="simulated-open-check",
                trigger_reason=trigger_reason,
                now=self._now(),
            )
            if timing_result is not None:
                return timing_result

        account_payload = self._build_account_payload(state, force_refresh=True)
        if not account_payload.get("account_value"):
            return self._blocked_result(
                state,
                event_type="simulated-open-check",
                reason=account_payload.get("block_reason") or "account value missing",
                details="Talos cannot size a Fortress trade until an execution account value is available.",
            )

        candidate_payload = self._build_candidate_payload(
            force_refresh=True,
            caller_source="talos-simulated-open-check",
            allow_snapshot=False,
        )
        if not candidate_payload.get("ready"):
            return self._blocked_result(
                state,
                event_type="simulated-open-check",
                reason=candidate_payload.get("block_reason") or "no Fortress candidate",
                details=str(candidate_payload.get("summary") or "Apollo did not produce a Fortress candidate ready for Talos."),
            )

        owned_trades = self._load_talos_trades()
        carryforward_simulated_trades = self._open_non_expiring_simulated_talos_trades(owned_trades)
        if carryforward_simulated_trades:
            return self._blocked_result(
                state,
                event_type="simulated-open-check",
                reason="simulated trade already open",
                details="Talos already has a non-expiring simulated Fortress trade open and will not duplicate the cycle.",
            )

        sizing_payload = self._build_sizing_payload(account_payload, candidate_payload, owned_trades)
        contracts_selected = int(sizing_payload.get("contracts_selected") or 0)
        if contracts_selected <= 0:
            return self._blocked_result(
                state,
                event_type="simulated-open-check",
                reason=sizing_payload.get("block_reason") or "risk exceeds 5%",
                details=str(sizing_payload.get("sizing_note") or "Talos could not fit the Fortress candidate within the 5% Black Swan cap."),
            )

        trade_payload = self._build_simulated_trade_payload(
            candidate_payload=candidate_payload,
            sizing_payload=sizing_payload,
            account_payload=account_payload,
        )
        existing_duplicate = self.trade_store.find_duplicate_trade(trade_payload)
        if existing_duplicate is not None and str(existing_duplicate.get("status") or "").strip().lower() == "open":
            return self._blocked_result(
                state,
                event_type="simulated-open-check",
                reason="simulated trade already open",
                details="A matching Talos simulated Fortress trade already exists in the journal.",
            )

        trade_id = self.trade_store.create_trade(trade_payload)
        self._append_activity(
            state,
            event_type="simulated-open-created",
            result="ok",
            reason="valid candidate ready",
            details=(
                f"Created Talos simulated Fortress trade #{trade_id} at {trade_payload.get('short_strike')}/{trade_payload.get('long_strike')} "
                f"for {contracts_selected} contract{'s' if contracts_selected != 1 else ''}."
            ),
            mode=mode,
        )
        return {
            "ok": True,
            "level": "success",
            "message": (
                f"Created Talos simulated Fortress trade for {contracts_selected} contract{'s' if contracts_selected != 1 else ''}."
            ),
        }

    def _execute_simulated_close_check(
        self,
        state: Dict[str, Any],
        *,
        trigger_reason: str,
        enforce_time_window: bool,
    ) -> Dict[str, Any]:
        mode = self._normalize_master_mode(state.get("master_mode"))
        self._append_activity(
            state,
            event_type="simulated-close-check",
            result="started",
            reason=f"Simulated close check started ({trigger_reason}).",
            details="Talos is reviewing shared-journal simulated positions for controlled close actions.",
            mode=mode,
        )
        if mode != self.MODE_SIMULATED:
            return self._blocked_result(
                state,
                event_type="simulated-close-check",
                reason="INACTIVE mode" if mode == self.MODE_INACTIVE else f"ACTIVE execution routing is not enabled in Talos {HOSTED_APP_VERSION}.",
                details="Talos only closes simulated Shared Supabase Journal trades when Master Operating Mode is SIMULATED.",
            )
        if enforce_time_window and not self._is_close_window(self._now()):
            return self._blocked_result(
                state,
                event_type="simulated-close-check",
                reason="outside trading window",
                details="The simulated close window begins when 15 minutes remain in the regular trading day.",
            )

        owned_trades = self._load_talos_trades()
        open_simulated_trades = self._open_simulated_talos_trades(owned_trades)
        if not open_simulated_trades:
            return self._blocked_result(
                state,
                event_type="simulated-close-check",
                reason="no Talos simulated trades open",
                details="There were no Talos simulated trades available to close in the Shared Supabase Journal.",
            )

        management_payload = self.open_trade_manager.evaluate_open_trades(
            send_alerts=False,
            caller_source="job:talos-simulated-close-check",
        )
        records_by_id = {
            int(item.get("trade_id") or 0): item
            for item in (management_payload.get("records") or [])
            if isinstance(item, dict)
        }
        closed_count = 0
        blocked_count = 0
        held_count = 0
        for trade in open_simulated_trades:
            trade_id = int(trade.get("id") or 0)
            record = records_by_id.get(trade_id) or {}
            contracts_to_close = max(int(record.get("contracts_to_close") or 0), 0)
            if not self._record_requires_exit_action(record) or contracts_to_close <= 0:
                held_count += 1
                self._append_activity(
                    state,
                    event_type="simulated-close-held",
                    result="ok",
                    reason="no exit gate triggered",
                    details=(
                        f"Held Talos simulated Fortress Trade #{trade.get('trade_number') or trade_id} open because no active exit gate was triggered."
                    ),
                    mode=mode,
                )
                continue
            close_value = self._coerce_float(record.get("current_close_price"))
            if close_value is None:
                close_value = self._coerce_float(record.get("current_spread_mark"))
            if close_value is None:
                blocked_count += 1
                self._append_activity(
                    state,
                    event_type="simulated-close-blocked",
                    result="blocked",
                    reason="real market close value unavailable",
                    details=f"Talos could not close Trade #{trade.get('trade_number') or trade_id} because no current close price was available.",
                    mode=mode,
                )
                continue

            close_events = [
                *list(trade.get("close_events") or []),
                {
                    "contracts_closed": contracts_to_close,
                    "actual_exit_value": close_value,
                    "event_datetime": self._now().isoformat(),
                    "close_method": ("Reduce" if contracts_to_close < int(trade.get("remaining_contracts") or trade.get("contracts") or 0) else "Close"),
                    "close_reason": str(record.get("next_active_gate_label") or "Talos Fortress gate"),
                    "notes_exit": (
                        "Talos simulated close | source=TalosEngine | execution_status=Simulated/NoBroker | "
                        f"action={record.get('action_type') or 'Manual'} | gate={record.get('active_gate_key') or 'unknown'}"
                    ),
                },
            ]
            updated_trade = self.open_trade_manager.update_talos_trade_gate_state(
                {**trade, "close_events": close_events},
                str(record.get("active_gate_key") or "").strip().lower(),
            )
            self.trade_store.update_trade(trade_id, updated_trade)
            closed_count += 1
            self._append_activity(
                state,
                event_type="simulated-close-completed",
                result="ok",
                reason=str(record.get("status") or "exit gate triggered"),
                details=(
                    f"Applied simulated {str(record.get('action_recommendation') or 'close').lower()} for Trade #{trade.get('trade_number') or trade_id} "
                    f"at {close_value:.2f} across {contracts_to_close} contract{'s' if contracts_to_close != 1 else ''}."
                ),
                mode=mode,
            )

        if closed_count == 0:
            return {
                "ok": False,
                "level": "warning",
                "message": (
                    "Talos did not close any simulated Fortress trades because no active exit gate was triggered."
                    if held_count
                    else "Talos did not close any simulated Fortress trades. Real market close values were unavailable."
                ),
            }
        message = f"Closed {closed_count} Talos simulated Fortress trade{'s' if closed_count != 1 else ''}."
        if blocked_count:
            message = f"{message} {blocked_count} trade{'s were' if blocked_count != 1 else ' was'} blocked by missing close pricing."
        if held_count:
            message = f"{message} {held_count} trade{'s remain' if held_count != 1 else ' remains'} open because no exit gate is active."
        return {"ok": True, "level": "success", "message": message}

    def _execute_real_open_check(self, state: Dict[str, Any], *, trigger_reason: str, confirmation_text: str) -> Dict[str, Any]:
        manual_forced = self._is_manual_forced_trigger(trigger_reason)
        self._append_activity(
            state,
            event_type="real-open-check",
            result="started",
            reason=("Manual forced real preview" if manual_forced else f"Real open check started ({trigger_reason})."),
            details=(
                "Timing window bypassed by operator. Talos is preparing a real execution preview under ACTIVE-mode protections."
                if manual_forced
                else "Talos is preparing a real execution preview under ACTIVE-mode protections."
            ),
            mode=self._normalize_master_mode(state.get("master_mode")),
        )
        preview, reasons, trade_payload = self._prepare_real_open_preview(state, trigger_reason=trigger_reason)
        real_state = state.setdefault("real_execution", {})
        real_state["open_preview"] = dict(preview)
        real_state["last_open_reason"] = "; ".join(reasons)
        if reasons:
            return self._blocked_result(
                state,
                event_type="real-open-check",
                reason=reasons[0],
                details="; ".join(reasons),
            )
        if not self._real_preview_ready(real_state, preview=preview, signature_key="open_preview_signature"):
            real_state["open_preview_signature"] = str(preview.get("signature") or "")
            return self._blocked_result(
                state,
                event_type="real-open-check",
                reason="dry-run preview displayed first",
                details="Dry-run order preview displayed. Review it, then resubmit with the exact confirmation text.",
            )
        if str(confirmation_text or "").strip() != self.order_service.CONFIRMATION_TEXT:
            return self._blocked_result(
                state,
                event_type="real-open-check",
                reason="confirmation text mismatch",
                details=f"Type {self.order_service.CONFIRMATION_TEXT} exactly to submit a real Talos order.",
            )
        execution_result = self.order_service.submit_open_order(preview)
        if not execution_result.get("ok"):
            return self._blocked_result(
                state,
                event_type="real-open-check",
                reason="broker submission failed",
                details=str(execution_result.get("message") or "Talos real order failed."),
            )
        trade_payload["notes_entry"] = (
            f"{trade_payload.get('notes_entry') or ''} | broker_order_id={execution_result.get('order_id') or ''} "
            f"| broker_method={'complex' if execution_result.get('preferred_method_used') else 'fallback'}"
        ).strip()
        trade_id = self.trade_store.create_trade(trade_payload)
        real_state["open_preview_signature"] = ""
        self._append_activity(
            state,
            event_type="real-open-check",
            result="ok",
            reason="real Talos order submitted",
            details=f"Submitted Talos real Fortress order and recorded trade #{trade_id}.",
            mode=self.MODE_ACTIVE,
        )
        return {"ok": True, "level": "success", "message": f"Submitted Talos real Fortress order and recorded trade #{trade_id}."}

    def _execute_real_close_check(self, state: Dict[str, Any], *, trigger_reason: str, confirmation_text: str, manual_override: bool) -> Dict[str, Any]:
        preview, reasons, trade, record = self._prepare_real_close_preview(
            state,
            trigger_reason=trigger_reason,
            manual_override=manual_override,
        )
        real_state = state.setdefault("real_execution", {})
        real_state["close_preview"] = dict(preview)
        real_state["last_close_reason"] = "; ".join(reasons)
        if reasons:
            return self._blocked_result(
                state,
                event_type="real-close-check",
                reason=reasons[0],
                details="; ".join(reasons),
            )
        if not self._real_preview_ready(real_state, preview=preview, signature_key="close_preview_signature"):
            real_state["close_preview_signature"] = str(preview.get("signature") or "")
            return self._blocked_result(
                state,
                event_type="real-close-check",
                reason="dry-run preview displayed first",
                details="Dry-run close preview displayed. Review it, then resubmit with the exact confirmation text.",
            )
        if str(confirmation_text or "").strip() != self.order_service.CONFIRMATION_TEXT:
            return self._blocked_result(
                state,
                event_type="real-close-check",
                reason="confirmation text mismatch",
                details=f"Type {self.order_service.CONFIRMATION_TEXT} exactly to submit a real Talos close order.",
            )
        execution_result = self.order_service.submit_close_order(preview)
        if not execution_result.get("ok"):
            return self._blocked_result(
                state,
                event_type="real-close-check",
                reason="broker submission failed",
                details=str(execution_result.get("message") or "Talos real close order failed."),
            )
        close_events = [
            *list(trade.get("close_events") or []),
            {
                "contracts_closed": int(record.get("contracts_to_close") or preview.get("contracts") or 0),
                "actual_exit_value": self._coerce_float(preview.get("limit_price")) or 0.0,
                "event_datetime": self._now().isoformat(),
                "close_method": ("Reduce" if int(record.get("contracts_to_close") or 0) < int(trade.get("remaining_contracts") or trade.get("contracts") or 0) else "Close"),
                "close_reason": str(record.get("next_active_gate_label") or record.get("status") or "Talos real close"),
                "notes_exit": (
                    f"Talos real close | broker_order_id={execution_result.get('order_id') or ''} "
                    f"| broker_method={'complex' if execution_result.get('preferred_method_used') else 'fallback'}"
                ),
            },
        ]
        updated_trade = self.open_trade_manager.update_talos_trade_gate_state(
            {**trade, "close_events": close_events},
            str(record.get("active_gate_key") or "manual-review"),
        )
        self.trade_store.update_trade(int(trade.get("id") or 0), updated_trade)
        real_state["close_preview_signature"] = ""
        self._append_activity(
            state,
            event_type="real-close-check",
            result="ok",
            reason="real Talos close submitted",
            details=f"Submitted Talos real close order for trade #{trade.get('trade_number') or trade.get('id') or 'unknown'}.",
            mode=self.MODE_ACTIVE,
        )
        return {"ok": True, "level": "success", "message": f"Submitted Talos real close order for trade #{trade.get('trade_number') or trade.get('id') or 'unknown'}."}

    def _prepare_real_open_preview(self, state: Dict[str, Any], *, trigger_reason: str) -> tuple[Dict[str, Any], list[str], Dict[str, Any]]:
        reasons: list[str] = []
        mode = self._normalize_master_mode(state.get("master_mode"))
        if mode != self.MODE_ACTIVE:
            reasons.append("Master Mode must be ACTIVE.")
        if self.config.talos_real_kill_switch:
            reasons.append("Talos real execution kill switch is enabled.")
        account_payload = self._build_account_payload(state, force_refresh=True)
        if not bool(account_payload.get("execution_connected")):
            reasons.append("Execution Schwab must be Connected.")
        candidate_payload = self._build_candidate_payload(
            force_refresh=True,
            caller_source=f"talos-real-open-check:{trigger_reason}",
            allow_snapshot=False,
        )
        if self._candidate_payload_has_market_data_issue(candidate_payload):
            reasons.append("Market Data Schwab must be Connected.")
        account_context = self.order_service.resolve_account_context()
        if not candidate_payload.get("ready"):
            reasons.append(str(candidate_payload.get("summary") or candidate_payload.get("block_reason") or "No valid Fortress candidate is available."))
        owned_trades = self._load_talos_trades()
        sizing_payload = self._build_sizing_payload(account_payload, candidate_payload, owned_trades)
        preview_sizing_payload = sizing_payload
        if mode != self.MODE_ACTIVE:
            preview_sizing_payload = self._build_sizing_payload(
                account_payload,
                candidate_payload,
                [
                    trade
                    for trade in owned_trades
                    if str(trade.get("trade_mode") or "").strip().lower() == "real"
                ],
            )
        if int(sizing_payload.get("contracts_selected") or 0) <= 0:
            reasons.append("Selected contracts must be greater than zero.")
        trade_payload = self._build_real_trade_payload(
            candidate_payload=candidate_payload,
            sizing_payload=sizing_payload,
            account_payload=account_payload,
            account_context=account_context,
        )
        existing_duplicate = self.trade_store.find_duplicate_trade(trade_payload)
        if existing_duplicate is not None and str(existing_duplicate.get("status") or "").strip().lower() in {"open", "reduced"}:
            reasons.append("A Talos real trade with the same expiration, strikes, and date already exists.")
        preview = self.order_service.build_open_order_preview(
            candidate_payload=candidate_payload,
            sizing_payload=preview_sizing_payload,
            account_context=account_context,
        )
        for item in preview.get("validation_errors") or []:
            if item not in reasons:
                reasons.append(str(item))
        if mode != self.MODE_ACTIVE:
            preview["execution_blocked_message"] = f"Execution Blocked ({mode.title()} Mode)"
            preview["estimated_contracts"] = int(preview_sizing_payload.get("contracts_selected") or 0)
        return preview, reasons, trade_payload

    def _prepare_real_close_preview(self, state: Dict[str, Any], *, trigger_reason: str, manual_override: bool) -> tuple[Dict[str, Any], list[str], Dict[str, Any], Dict[str, Any]]:
        reasons: list[str] = []
        trade = self._select_real_close_trade()
        if not trade:
            reasons.append("No Talos real trade is available for close management.")
            return {}, reasons, {}, {}
        mode = self._normalize_master_mode(state.get("master_mode"))
        if mode != self.MODE_ACTIVE:
            reasons.append("Master Mode must be ACTIVE.")
        if self.config.talos_real_kill_switch:
            reasons.append("Talos real execution kill switch is enabled.")
        account_payload = self._build_account_payload(state, force_refresh=True)
        if not bool(account_payload.get("execution_connected")):
            reasons.append("Execution Schwab must be Connected.")
        management_payload = self.open_trade_manager.evaluate_open_trades(
            send_alerts=False,
            caller_source=f"job:talos-real-close-check:{trigger_reason}",
        )
        records_by_id = {
            int(item.get("trade_id") or 0): dict(item)
            for item in (management_payload.get("records") or [])
            if isinstance(item, dict)
        }
        record = records_by_id.get(int(trade.get("id") or 0), {})
        if not management_payload.get("market_data_available", True):
            reasons.append("Market Data Schwab must be Connected.")
        if not manual_override and (not self._record_requires_exit_action(record) or int(record.get("contracts_to_close") or 0) <= 0):
            reasons.append("No Talos Fortress exit gate is active for this real trade.")
        account_context = self.order_service.resolve_account_context()
        preview = self.order_service.build_close_order_preview(
            trade=trade,
            record={**record, "contracts_to_close": int(record.get("contracts_to_close") or trade.get("remaining_contracts") or trade.get("contracts") or 0)},
            account_context=account_context,
            manual_override=manual_override,
        )
        for item in preview.get("validation_errors") or []:
            if item not in reasons:
                reasons.append(str(item))
        return preview, reasons, trade, record

    def _select_real_close_trade(self) -> Dict[str, Any]:
        real_trades = [
            trade
            for trade in self._load_talos_trades()
            if str(trade.get("trade_mode") or "").strip().lower() == "real"
            and str(trade.get("derived_status_raw") or trade.get("status") or "").strip().lower() in {"open", "reduced"}
        ]
        return dict(real_trades[0]) if real_trades else {}

    @staticmethod
    def _real_preview_ready(real_state: Dict[str, Any], *, preview: Dict[str, Any], signature_key: str) -> bool:
        preview_signature = str(preview.get("signature") or "")
        stored_signature = str(real_state.get(signature_key) or "")
        return bool(preview_signature and stored_signature and preview_signature == stored_signature)

    def _build_master_mode_payload(self, state: Dict[str, Any]) -> Dict[str, Any]:
        current_mode = self._normalize_master_mode(state.get("master_mode"))
        return {
            "current": current_mode,
            "label": self._mode_label(current_mode),
            "tone": self._mode_tone(current_mode),
            "description": {
                self.MODE_INACTIVE: "Diagnostics only. Talos does not open, close, simulate, or route trades.",
                self.MODE_SIMULATED: "Full Talos logic with live Schwab market data and simulated Talos Fortress trades only.",
                self.MODE_ACTIVE: f"Execution standby. Talos {HOSTED_APP_VERSION} can preview and manually test hard-gated real Fortress orders.",
            }.get(current_mode, "Talos mode unavailable."),
            "options": [
                {"value": self.MODE_INACTIVE, "label": "INACTIVE", "tone": "inactive", "disabled": False},
                {"value": self.MODE_SIMULATED, "label": "SIMULATED", "tone": "simulated", "disabled": False},
                {"value": self.MODE_ACTIVE, "label": "ACTIVE", "tone": "active", "disabled": False},
            ],
        }

    def _build_status_summary(self, state: Dict[str, Any], decision_payload: Dict[str, Any], owned_trades: list[Dict[str, Any]]) -> Dict[str, Any]:
        return {
            "mode_label": self._mode_label(self._normalize_master_mode(state.get("master_mode"))),
            "phase": decision_payload.get("phase_label") or "Monitoring",
            "open_talos_trades": len(self._load_open_talos_trades(owned_trades)),
            "managed_talos_trades": len(owned_trades),
        }

    def _build_timing_status_payload(self) -> Dict[str, Any]:
        now = self._now()
        timing_state = self._resolve_timing_state(now)
        minutes_to_close = self._minutes_to_regular_close(now) if self._is_regular_session(now) else None
        return {
            "state": timing_state,
            "label": {
                self.TIMING_BEFORE_OPEN_WINDOW: "Awaiting open window",
                self.TIMING_OPEN_WINDOW_ACTIVE: "Open window active",
                self.TIMING_EXIT_ONLY_WINDOW: "Exit-only management",
                self.TIMING_MARKET_CLOSED: "Market closed",
            }.get(timing_state, "Timing unavailable"),
            "tone": {
                self.TIMING_BEFORE_OPEN_WINDOW: "info",
                self.TIMING_OPEN_WINDOW_ACTIVE: "success",
                self.TIMING_EXIT_ONLY_WINDOW: "warning",
                self.TIMING_MARKET_CLOSED: "muted",
            }.get(timing_state, "muted"),
            "minutes_to_close": minutes_to_close,
            "details": {
                self.TIMING_BEFORE_OPEN_WINDOW: "Autonomous opens remain blocked until the final 10 minutes of regular trading.",
                self.TIMING_OPEN_WINDOW_ACTIVE: "Autonomous Talos open routing is allowed during the approved window.",
                self.TIMING_EXIT_ONLY_WINDOW: "Talos may manage exits, but autonomous opens remain blocked until the open window begins.",
                self.TIMING_MARKET_CLOSED: "The market is outside regular session hours. Talos will not open new positions.",
            }.get(timing_state, "Timing status unavailable."),
        }

    def _build_engine_health_payload(
        self,
        state: Dict[str, Any],
        *,
        market_data_connected: bool,
        execution_connected: bool,
        journal_available: bool,
    ) -> list[Dict[str, str]]:
        monitor_state = dict(state.get("monitor_loop") or {})
        last_run_at_raw = str(monitor_state.get("last_run_at") or "").strip()
        last_run_at: datetime | None = None
        if last_run_at_raw:
            try:
                last_run_at = datetime.fromisoformat(last_run_at_raw)
            except ValueError:
                last_run_at = None
        monitor_fresh = bool(last_run_at and (self._now() - last_run_at) <= timedelta(minutes=6))
        scheduler_running = bool(self._monitor_running and self._monitor_timer is not None)
        return [
            {"label": "Market Data", "status": "ready" if market_data_connected else "down", "display": "Ready" if market_data_connected else "Auth"},
            {"label": "Execution", "status": "ready" if execution_connected else "down", "display": "Ready" if execution_connected else "Auth"},
            {"label": "Monitor", "status": "ready" if monitor_fresh else "warn", "display": "Live" if monitor_fresh else "Stale"},
            {"label": "Scheduler", "status": "ready" if scheduler_running else "down", "display": "On" if scheduler_running else "Off"},
            {"label": "Journal", "status": "ready" if journal_available else "down", "display": "Ready" if journal_available else "Error"},
        ]

    def _build_real_execution_payload(self, state: Dict[str, Any]) -> Dict[str, Any]:
        real_state = dict(state.get("real_execution") or {})
        status_snapshot = getattr(self.order_service, "get_status_snapshot", lambda: {})()
        return {
            "confirmation_text_required": self.order_service.CONFIRMATION_TEXT,
            "open_preview": dict(real_state.get("open_preview") or {}),
            "close_preview": dict(real_state.get("close_preview") or {}),
            "last_open_reason": str(real_state.get("last_open_reason") or ""),
            "last_close_reason": str(real_state.get("last_close_reason") or ""),
            "kill_switch_enabled": bool(self.config.talos_real_kill_switch),
            "allow_market_orders": bool(self.config.talos_allow_market_orders),
            "execution_enabled": bool(self.config.talos_execution_enabled),
            "execution_account_configured": bool(self.config.talos_execution_account),
            "execution_account_name": str(self.config.talos_execution_account_name or "Talos execution"),
            "status_snapshot": dict(status_snapshot or {}),
            "account_hash_fallback": str(self.config.schwab_trading_account_hash or ""),
            "account_number_fallback": str(self.config.schwab_trading_account_number or ""),
        }

    def _build_account_payload(self, state: Dict[str, Any], *, force_refresh: bool) -> Dict[str, Any]:
        del force_refresh
        owned_trades = self._load_talos_trades()
        exposure_snapshot = self._build_exposure_snapshot(owned_trades)
        open_exposure = exposure_snapshot["counted_open_exposure"]
        manual_account_value = self._resolve_manual_account_value(state)
        execution_status = self.execution_auth_service.get_connection_status()
        execution_account_configured = bool(self.config.talos_execution_account)
        execution_configuration_message = "" if execution_account_configured else "Execution account not configured"
        LOGGER.info(
            "Talos execution status resolved | connected=%s | label=%s | meta=%s | token_expiration=%s",
            bool(execution_status.get("connected")),
            execution_status.get("status_label"),
            execution_status.get("status_meta"),
            execution_status.get("token_expiration_display"),
        )
        if execution_status.get("connected") or execution_status.get("usable_token_chain"):
            LOGGER.info("Talos execution account fetch started")
            try:
                account_summary = self.execution_auth_service.get_account_summary()
            except (ProviderAuthRequiredError, ProviderReauthenticationRequiredError, ProviderError, Exception) as exc:
                refreshed_execution_status = self.execution_auth_service.get_connection_status()
                refreshed_connected = bool(
                    refreshed_execution_status.get("connected") or refreshed_execution_status.get("usable_token_chain")
                )
                LOGGER.warning("Talos execution account fetch failed: %s", exc)
                manual_status_label = "Connected" if refreshed_connected else str(refreshed_execution_status.get("status_label") or "Reconnect required")
                manual_status_meta = (
                    "Execution connected; account refresh temporarily unavailable. Manual fallback active."
                    if refreshed_connected
                    else str(refreshed_execution_status.get("status_meta") or "Reconnect Schwab trading")
                )
                payload = self._build_manual_account_payload(
                    manual_account_value=manual_account_value,
                    exposure_snapshot=exposure_snapshot,
                    status_label=manual_status_label,
                    status_meta=(f"{manual_status_meta} {str(exc)}".strip() if refreshed_connected else manual_status_meta),
                    execution_connected=refreshed_connected,
                    token_expiration_display=str(refreshed_execution_status.get("token_expiration_display") or "—"),
                    execution_account_type="Schwab trading",
                )
                payload["execution_account_configured"] = execution_account_configured
                payload["execution_configuration_message"] = execution_configuration_message
                payload["execution_account_name"] = str(self.config.talos_execution_account_name or "Talos execution")
                self._track_account_status(
                    state,
                    status_key=("connected-manual-fallback" if refreshed_connected else "reconnect-required"),
                    payload=payload,
                )
                return payload

            LOGGER.info("Talos execution account fetch succeeded")
            account_value = self._coerce_float(account_summary.get("liquidation_value"))
            buying_power = self._coerce_float(account_summary.get("buying_power"))
            max_black_swan_allocation = round(max(account_value or 0.0, 0.0) * self.MAX_BLACK_SWAN_ALLOCATION_RATIO, 2) if account_value else None
            remaining_capacity = round(max((max_black_swan_allocation or 0.0) - open_exposure, 0.0), 2) if max_black_swan_allocation is not None else None
            payload = {
                "available": account_value is not None,
                "auth_status": "Connected",
                "auth_tone": "success",
                "execution_status": "Connected",
                "execution_status_meta": str(execution_status.get("status_meta") or "Schwab trading"),
                "execution_connected": True,
                "execution_account_display": str(account_summary.get("account_number_masked") or "—"),
                "execution_account_type": str(account_summary.get("account_type") or "Schwab"),
                "token_expiration_display": str(execution_status.get("token_expiration_display") or "—"),
                "account_source": "Live Schwab trading",
                "manual_override_enabled": False,
                "manual_account_value": manual_account_value,
                "manual_account_value_input": self._format_numeric_input(manual_account_value),
                "account_value": account_value,
                "account_value_display": self._format_currency(account_value),
                "buying_power": buying_power,
                "buying_power_display": self._format_currency(buying_power),
                "max_black_swan_allocation": max_black_swan_allocation,
                "max_black_swan_allocation_display": self._format_currency(max_black_swan_allocation),
                "open_exposure": open_exposure,
                "open_exposure_display": self._format_currency(open_exposure),
                "ignored_same_day_exposure": exposure_snapshot["ignored_same_day_exposure"],
                "ignored_same_day_exposure_display": self._format_currency(exposure_snapshot["ignored_same_day_exposure"]),
                "total_open_exposure": exposure_snapshot["total_open_exposure"],
                "total_open_exposure_display": self._format_currency(exposure_snapshot["total_open_exposure"]),
                "remaining_capacity": remaining_capacity,
                "remaining_capacity_display": self._format_currency(remaining_capacity),
                "timestamp_display": str(account_summary.get("as_of_display") or "—"),
                "block_reason": "account value missing" if account_value is None else "",
                "execution_account_configured": execution_account_configured,
                "execution_configuration_message": execution_configuration_message,
                "execution_account_name": str(self.config.talos_execution_account_name or "Talos execution"),
            }
            self._track_account_status(state, status_key="ok" if account_value is not None else "missing-value", payload=payload)
            return payload

        payload = self._build_manual_account_payload(
            manual_account_value=manual_account_value,
            exposure_snapshot=exposure_snapshot,
            status_label=str(execution_status.get("status_label") or "Execution auth disconnected"),
            status_meta=str(execution_status.get("status_meta") or "Manual fallback active"),
        )
        payload["execution_account_configured"] = execution_account_configured
        payload["execution_configuration_message"] = execution_configuration_message
        payload["execution_account_name"] = str(self.config.talos_execution_account_name or "Talos execution")
        self._track_account_status(state, status_key="manual-fallback", payload=payload)
        return payload

    def _reconcile_expired_talos_trades(
        self,
        state: Dict[str, Any],
        *,
        now: datetime,
        caller_source: str,
    ) -> Dict[str, Any]:
        reconciled_trade_numbers: list[int] = []
        skipped_trade_numbers: list[int] = []
        for trade in self._load_talos_trades():
            if resolve_trade_system_name(trade) != "Talos":
                continue
            current_status = str(trade.get("derived_status_raw") or trade.get("status") or "").strip().lower()
            if current_status not in {"open", "reduced"}:
                continue
            expiration_date = self._resolve_trade_expiration_date(trade)
            if expiration_date is None:
                continue
            if expiration_date > now.date():
                continue
            if expiration_date == now.date() and now.time() <= self.REGULAR_MARKET_CLOSE:
                skipped_trade_numbers.append(int(trade.get("trade_number") or 0))
                continue
            trade_id = int(trade.get("id") or 0)
            if trade_id <= 0:
                continue
            try:
                self.trade_store.expire_trade(
                    trade_id,
                    {
                        "event_datetime": now.isoformat(),
                        "actual_exit_value": 0.0,
                        "close_method": "Expire",
                        "close_reason": "Talos automatic expiration reconciliation",
                        "notes_exit": f"Talos auto-expired after market close | source={caller_source}",
                    },
                )
                reconciled_trade_numbers.append(int(trade.get("trade_number") or trade_id))
            except ValueError:
                continue

        if reconciled_trade_numbers:
            self._append_activity(
                state,
                event_type="expired-trade-reconciliation",
                result="ok",
                reason="Talos expiration reconciliation applied",
                details=(
                    f"Expired Talos trade{'s' if len(reconciled_trade_numbers) != 1 else ''}: "
                    + ", ".join(str(item) for item in reconciled_trade_numbers)
                ),
            )
        return {
            "changed": bool(reconciled_trade_numbers),
            "reconciled_trade_numbers": reconciled_trade_numbers,
            "skipped_same_day_trade_numbers": [item for item in skipped_trade_numbers if item > 0],
        }

    def _track_account_status(self, state: Dict[str, Any], *, status_key: str, payload: Dict[str, Any]) -> None:
        previous_key = str(state.get("last_account_status_key") or "")
        if previous_key == status_key:
            return
        state["last_account_status_key"] = status_key
        self._append_activity(
            state,
            event_type="account-data",
            result="ok" if status_key == "ok" else "blocked",
            reason=str(payload.get("auth_status") or status_key),
            details=(
                f"Account value {payload.get('account_value_display')} | Remaining Talos risk capacity {payload.get('remaining_capacity_display')}"
                if status_key == "ok"
                else str(payload.get("block_reason") or payload.get("auth_status") or status_key)
            ),
        )

    def _build_manual_account_payload(
        self,
        *,
        manual_account_value: float,
        exposure_snapshot: Dict[str, float],
        status_label: str,
        status_meta: str,
        execution_connected: bool = False,
        token_expiration_display: str = "—",
        execution_account_type: str = "Manual override",
    ) -> Dict[str, Any]:
        open_exposure = exposure_snapshot["counted_open_exposure"]
        max_black_swan_allocation = round(max(manual_account_value, 0.0) * self.MAX_BLACK_SWAN_ALLOCATION_RATIO, 2)
        remaining_capacity = round(max(max_black_swan_allocation - open_exposure, 0.0), 2)
        return {
            "available": True,
            "auth_status": status_label,
            "auth_tone": "success" if execution_connected else "warning",
            "execution_status": status_label,
            "execution_status_meta": status_meta,
            "execution_connected": execution_connected,
            "execution_account_display": "—",
            "execution_account_type": execution_account_type,
            "token_expiration_display": token_expiration_display,
            "account_source": "Manual override",
            "manual_override_enabled": True,
            "manual_account_value": manual_account_value,
            "manual_account_value_input": self._format_numeric_input(manual_account_value),
            "account_value": manual_account_value,
            "account_value_display": self._format_currency(manual_account_value),
            "buying_power": None,
            "buying_power_display": "—",
            "max_black_swan_allocation": max_black_swan_allocation,
            "max_black_swan_allocation_display": self._format_currency(max_black_swan_allocation),
            "open_exposure": open_exposure,
            "open_exposure_display": self._format_currency(open_exposure),
            "ignored_same_day_exposure": exposure_snapshot["ignored_same_day_exposure"],
            "ignored_same_day_exposure_display": self._format_currency(exposure_snapshot["ignored_same_day_exposure"]),
            "total_open_exposure": exposure_snapshot["total_open_exposure"],
            "total_open_exposure_display": self._format_currency(exposure_snapshot["total_open_exposure"]),
            "remaining_capacity": remaining_capacity,
            "remaining_capacity_display": self._format_currency(remaining_capacity),
            "timestamp_display": ("Manual override" if not execution_connected else "Manual override while execution is connected"),
            "block_reason": "",
            "execution_account_configured": bool(self.config.talos_execution_account),
            "execution_configuration_message": ("" if self.config.talos_execution_account else "Execution account not configured"),
            "execution_account_name": str(self.config.talos_execution_account_name or "Talos execution"),
        }

    def _build_candidate_payload(self, *, force_refresh: bool, caller_source: str, allow_snapshot: bool) -> Dict[str, Any]:
        del allow_snapshot
        now = self._now()
        if not force_refresh and self._candidate_payload_cache is not None and self._candidate_payload_cache_expires_at is not None and now <= self._candidate_payload_cache_expires_at:
            cached_payload = copy.deepcopy(self._candidate_payload_cache)
            cached_payload.setdefault("cache_diagnostics", {})
            cached_payload["cache_diagnostics"].update(
                {
                    "cache_state": "hit",
                    "cache_expires_at": self._format_timestamp(self._candidate_payload_cache_expires_at),
                }
            )
            return cached_payload
        try:
            precheck = self.apollo_service.run_precheck(force_refresh=force_refresh, caller_source=caller_source)
        except Exception as exc:
            return {
                "ready": False,
                "summary": "Talos could not evaluate Apollo Fortress right now.",
                "error": str(exc),
                "block_reason": str(exc),
                "raw_candidate": None,
                "raw_precheck": None,
                "short_strike": "—",
                "long_strike": "—",
                "expiration_date": "—",
                "premium_per_contract": "—",
                "spread_width": "—",
                "distance_to_short": "—",
                "em_multiple": "—",
                "projected_black_swan_loss_per_contract": "—",
                "rationale": [],
                "diagnostics": [str(exc)],
                "cache_diagnostics": {"cache_state": "miss"},
            }
        candidate = self._select_fortress_candidate(precheck)
        if candidate is None:
            real_reason = str(
                (precheck.get("option_chain") or {}).get("message")
                or (precheck.get("trade_candidates") or {}).get("message")
                or "Apollo did not return a Fortress candidate."
            )
            return {
                "ready": False,
                "summary": real_reason,
                "error": real_reason,
                "block_reason": "no Fortress candidate",
                "raw_candidate": None,
                "raw_precheck": precheck,
                "short_strike": "—",
                "long_strike": "—",
                "expiration_date": str((precheck.get("option_chain") or {}).get("expiration_date") or "—"),
                "premium_per_contract": "—",
                "spread_width": "—",
                "distance_to_short": "—",
                "em_multiple": "—",
                "projected_black_swan_loss_per_contract": "—",
                "rationale": [],
                "diagnostics": [real_reason],
                "cache_diagnostics": {"cache_state": "miss"},
            }
        option_chain = precheck.get("option_chain") or {}
        ready = bool(candidate.get("available", True))
        projected_loss_per_contract = self._resolve_black_swan_loss_per_contract(candidate)
        max_theoretical_loss_per_contract = self._resolve_max_theoretical_loss_per_contract(candidate)
        result = {
            "ready": ready,
            "summary": f"{self._format_number(candidate.get('short_strike'))} / {self._format_number(candidate.get('long_strike'))} put spread",
            "error": None,
            "block_reason": "valid candidate ready" if ready else str(candidate.get("no_trade_message") or "candidate unavailable"),
            "raw_candidate": candidate,
            "raw_precheck": precheck,
            "short_strike": self._format_number(candidate.get("short_strike")),
            "long_strike": self._format_number(candidate.get("long_strike")),
            "expiration_date": str(option_chain.get("expiration_date") or precheck.get("market_calendar", {}).get("next_market_day") or "—"),
            "premium_per_contract": self._format_currency(candidate.get("premium_per_contract") or candidate.get("credit")),
            "spread_width": self._format_number(candidate.get("width")),
            "distance_to_short": self._format_number(candidate.get("actual_distance_to_short") or candidate.get("distance_points")),
            "em_multiple": self._format_number(candidate.get("actual_em_multiple") or candidate.get("em_multiple"), decimals=2),
            "projected_black_swan_loss_per_contract": self._format_currency(projected_loss_per_contract),
            "projected_black_swan_loss_per_contract_value": projected_loss_per_contract,
            "max_theoretical_loss_per_contract": self._format_currency(max_theoretical_loss_per_contract),
            "max_theoretical_loss_per_contract_value": max_theoretical_loss_per_contract,
            "rationale": [str(item) for item in (candidate.get("rationale") or [])[:4]],
            "diagnostics": [str(item) for item in (candidate.get("diagnostics") or [])[:4]],
            "cache_diagnostics": {"cache_state": "miss"},
        }
        self._candidate_payload_cache = copy.deepcopy(result)
        self._candidate_payload_cache_expires_at = now + timedelta(seconds=max(int(self.config.talos_candidate_cache_ttl_seconds or 20), 1))
        result["cache_diagnostics"].update(
            {
                "cache_expires_at": self._format_timestamp(self._candidate_payload_cache_expires_at),
                "cache_ttl_seconds": int(self.config.talos_candidate_cache_ttl_seconds or 20),
            }
        )
        return result

    def _build_sizing_payload(
        self,
        account_payload: Dict[str, Any],
        candidate_payload: Dict[str, Any],
        owned_trades: list[Dict[str, Any]],
    ) -> Dict[str, Any]:
        account_value = self._coerce_float(account_payload.get("account_value"))
        risk_budget = round(max(account_value or 0.0, 0.0) * self.MAX_BLACK_SWAN_ALLOCATION_RATIO, 2) if account_value else None
        exposure_snapshot = self._build_exposure_snapshot(owned_trades)
        open_exposure = exposure_snapshot["counted_open_exposure"]
        ignored_same_day_exposure = exposure_snapshot["ignored_same_day_exposure"]
        remaining_capacity = round(max((risk_budget or 0.0) - open_exposure, 0.0), 2) if risk_budget is not None else None
        per_contract_loss = self._coerce_float(candidate_payload.get("projected_black_swan_loss_per_contract_value"))
        max_theoretical_loss_per_contract = self._coerce_float(candidate_payload.get("max_theoretical_loss_per_contract_value"))
        max_contracts = 0
        projected_total_loss = 0.0
        projected_total_theoretical_loss = 0.0
        projected_exposure_after_trade = open_exposure
        remaining_capacity_after_trade = remaining_capacity
        block_reason = ""
        sizing_note = ""
        if account_value is None:
            block_reason = "account value missing"
            sizing_note = "Unable to size trade until Schwab account value is available."
        elif not candidate_payload.get("ready"):
            block_reason = candidate_payload.get("block_reason") or "no Fortress candidate"
            sizing_note = "No Talos sizing decision was made because Apollo Fortress is not ready."
        elif per_contract_loss in {None, 0.0}:
            block_reason = "projected Black Swan loss unavailable"
            sizing_note = "Talos could not determine per-contract Black Swan loss for the current Fortress candidate."
        else:
            max_contracts = int((remaining_capacity or 0.0) // per_contract_loss)
            projected_total_loss = round(max_contracts * per_contract_loss, 2)
            projected_exposure_after_trade = round(open_exposure + projected_total_loss, 2)
            if max_theoretical_loss_per_contract is not None:
                projected_total_theoretical_loss = round(max_contracts * max_theoretical_loss_per_contract, 2)
            if remaining_capacity is not None:
                remaining_capacity_after_trade = round(max(remaining_capacity - projected_total_loss, 0.0), 2)
            if max_contracts <= 0:
                block_reason = "risk exceeds 5%"
                sizing_note = "The Fortress candidate does not fit within the remaining 5% Black Swan risk capacity."
            else:
                sizing_note = (
                    "Talos selected the maximum contracts that stay within the 5% Black Swan cap. "
                    "Same-day expiring Talos exposure is ignored for next-cycle sizing. "
                    "Contract sizing uses Black Swan exposure, not theoretical max spread loss."
                )
        return {
            "account_value": account_value,
            "account_value_display": self._format_currency(account_value),
            "risk_budget": risk_budget,
            "risk_budget_display": self._format_currency(risk_budget),
            "open_exposure": open_exposure,
            "open_exposure_display": self._format_currency(open_exposure),
            "ignored_same_day_exposure": ignored_same_day_exposure,
            "ignored_same_day_exposure_display": self._format_currency(ignored_same_day_exposure),
            "remaining_capacity": remaining_capacity,
            "remaining_capacity_display": self._format_currency(remaining_capacity),
            "per_contract_loss": per_contract_loss,
            "per_contract_loss_display": self._format_currency(per_contract_loss),
            "max_contracts_allowed": max_contracts,
            "max_contracts_display": str(max_contracts),
            "contracts_selected": max_contracts,
            "contracts_selected_display": str(max_contracts),
            "projected_total_black_swan_loss": projected_total_loss,
            "projected_total_black_swan_loss_display": self._format_currency(projected_total_loss),
            "max_theoretical_loss_per_contract": max_theoretical_loss_per_contract,
            "max_theoretical_loss_per_contract_display": self._format_currency(max_theoretical_loss_per_contract),
            "projected_total_theoretical_loss": projected_total_theoretical_loss,
            "projected_total_theoretical_loss_display": self._format_currency(projected_total_theoretical_loss),
            "projected_exposure": projected_total_loss,
            "projected_exposure_display": self._format_currency(projected_exposure_after_trade),
            "projected_exposure_after_trade": projected_exposure_after_trade,
            "projected_exposure_after_trade_display": self._format_currency(projected_exposure_after_trade),
            "remaining_capacity_after_trade": remaining_capacity_after_trade,
            "remaining_capacity_after_trade_display": self._format_currency(remaining_capacity_after_trade),
            "block_reason": str(block_reason),
            "sizing_note": sizing_note,
        }

    def _build_decision_payload(
        self,
        *,
        state: Dict[str, Any],
        account_payload: Dict[str, Any],
        candidate_payload: Dict[str, Any],
        sizing_payload: Dict[str, Any],
        owned_trades: list[Dict[str, Any]],
    ) -> Dict[str, Any]:
        mode = self._normalize_master_mode(state.get("master_mode"))
        now = self._now()
        open_simulated_trades = self._open_simulated_talos_trades(owned_trades)
        carryforward_simulated_trades = self._open_non_expiring_simulated_talos_trades(owned_trades)
        same_day_expiring_simulated_trades = [trade for trade in open_simulated_trades if self._is_same_day_expiring_trade(trade, now=now)]
        in_open_window = self._is_open_window(now)
        in_close_window = self._is_close_window(now)
        phase_label = "Monitoring"
        decision_label = "Would Not Trade"
        reason = "outside trading window"
        if mode == self.MODE_INACTIVE:
            phase_label = "Dormant"
            decision_label = "Would Not Trade"
            reason = "INACTIVE mode"
        elif mode == self.MODE_ACTIVE:
            phase_label = "Execution standby"
            decision_label = "Would Not Trade"
            reason = "ACTIVE mode is reserved; real order routing is still disabled."
        elif carryforward_simulated_trades and in_close_window:
            phase_label = "Simulated close pending"
            decision_label = "Would Not Trade"
            reason = "non-expiring simulated trade already open"
        elif carryforward_simulated_trades:
            phase_label = "Simulated position open"
            decision_label = "Would Not Trade"
            reason = "non-expiring simulated trade already open"
        elif not candidate_payload.get("ready"):
            phase_label = "Monitoring"
            decision_label = "Would Not Trade"
            reason = candidate_payload.get("block_reason") or "no Fortress candidate"
        elif not account_payload.get("account_value"):
            phase_label = "Monitoring"
            decision_label = "Cannot Evaluate"
            reason = account_payload.get("block_reason") or "account value missing"
        elif in_open_window and int(sizing_payload.get("contracts_selected") or 0) > 0:
            phase_label = "Ready to simulate open"
            decision_label = "Would Trade"
            reason = (
                "valid candidate ready"
                if not same_day_expiring_simulated_trades
                else "valid next-cycle candidate ready; same-day expiring exposure ignored"
            )
        elif in_open_window:
            phase_label = "Monitoring"
            decision_label = "Would Not Trade"
            reason = sizing_payload.get("block_reason") or "risk exceeds 5%"
        elif same_day_expiring_simulated_trades:
            phase_label = "Holding same-day expiry"
            decision_label = "Would Not Trade"
            reason = "same-day simulated trade remains open until an exit gate triggers or expiration completes"
        elif open_simulated_trades:
            phase_label = "Waiting for close window"
            decision_label = "Would Not Trade"
            reason = "simulated trade already open"
        return {
            "current_mode": self._mode_label(mode),
            "phase_label": phase_label,
            "decision_label": decision_label,
            "reason": reason,
            "candidate": {
                "short_strike": candidate_payload.get("short_strike"),
                "long_strike": candidate_payload.get("long_strike"),
                "expiration_date": candidate_payload.get("expiration_date"),
                "premium_per_contract": candidate_payload.get("premium_per_contract"),
                "spread_width": candidate_payload.get("spread_width"),
                "em_multiple": candidate_payload.get("em_multiple"),
                "distance_to_short": candidate_payload.get("distance_to_short"),
                "projected_black_swan_loss_per_contract": candidate_payload.get("projected_black_swan_loss_per_contract"),
                "max_theoretical_loss_per_contract": candidate_payload.get("max_theoretical_loss_per_contract"),
            },
            "sizing": sizing_payload,
        }

    def _build_trade_summary(self, owned_trades: list[Dict[str, Any]]) -> Dict[str, Any]:
        return {
            "total": len(owned_trades),
            "open": sum(1 for item in owned_trades if str(item.get("derived_status_raw") or item.get("status") or "").strip().lower() in {"open", "reduced"}),
            "closed": sum(1 for item in owned_trades if str(item.get("derived_status_raw") or item.get("status") or "").strip().lower() in {"closed", "expired", "cancelled"}),
            "real": sum(1 for item in owned_trades if self._resolve_visual_trade_mode(item) == "real"),
            "simulated": sum(1 for item in owned_trades if str(item.get("trade_mode") or "").strip().lower() == "simulated"),
        }

    def _build_recent_trades(self, owned_trades: list[Dict[str, Any]], management_records: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
        trades = sorted(owned_trades, key=self._trade_sort_key, reverse=True)
        records_by_trade_number = {
            int(item.get("trade_number") or 0): dict(item)
            for item in management_records
            if int(item.get("trade_number") or 0) > 0
        }
        recent = []
        for trade in trades:
            visual_mode_key = self._resolve_visual_trade_mode(trade)
            management_record = records_by_trade_number.get(int(trade.get("trade_number") or 0), {})
            recent.append(
                {
                    "trade_number": trade.get("trade_number") or trade.get("id") or "—",
                    "trade_mode": "Simulated" if visual_mode_key == "simulated" else "Real",
                    "trade_mode_key": visual_mode_key,
                    "system_label": f"TALOS {'SIMULATED' if visual_mode_key == 'simulated' else 'REAL'}",
                    "status": str(trade.get("derived_status_label") or trade.get("status") or "—").title(),
                    "candidate_profile": str(trade.get("candidate_profile") or "Fortress"),
                    "strike_pair": self._format_strike_pair(trade),
                    "contracts": self._format_number(trade.get("contracts"), decimals=0),
                    "entry_credit": self._format_currency(trade.get("actual_entry_credit") or trade.get("candidate_credit_estimate")),
                    "trade_date": str(trade.get("trade_date") or trade.get("entry_datetime") or "—"),
                    "ownership_reason": "Shared Supabase Journal",
                    "short_strike": management_record.get("short_strike_display") or self._format_number(trade.get("short_strike")),
                    "long_strike": management_record.get("long_strike_display") or self._format_number(trade.get("long_strike")),
                    "current_spx": management_record.get("current_spx_gate_display") or "—",
                    "distance_to_short": management_record.get("distance_to_short_display") or "—",
                    "management_status": str(management_record.get("status") or "Healthy"),
                    "action_recommendation": str(management_record.get("action_recommendation") or "Watch"),
                    "gate_1_level": management_record.get("gate_1_level_display") or "—",
                    "gate_2_level": management_record.get("gate_2_level_display") or "—",
                    "gate_3_condition": management_record.get("gate_3_condition") or "—",
                    "gate_4_condition": management_record.get("gate_4_condition") or "—",
                    "next_active_gate": management_record.get("next_active_gate_label") or "—",
                    "next_gate_contracts": management_record.get("next_gate_contracts_display") or "0",
                    "triggered_gates": management_record.get("triggered_gates_display") or "None",
                    "gate_progress_display": self._build_gate_progress_display(
                        management_record.get("next_active_gate_label"),
                        management_record.get("triggered_gates_display"),
                    ),
                }
            )
        return recent

    @staticmethod
    def _build_gate_progress_display(next_active_gate: Any, triggered_gates: Any) -> str:
        triggered = str(triggered_gates or "").strip()
        if not triggered or triggered.lower() == "none":
            completed = 0
        else:
            completed = len([item for item in triggered.split(",") if item.strip()])
        next_label = str(next_active_gate or "").strip()
        if next_label == "All gates fired":
            return "Gate 4/4"
        if next_label.startswith("Gate "):
            return f"{next_label}/4"
        return f"Gate {min(completed + 1, 4)}/4"

    def _build_gate_test_payload(self, state: Dict[str, Any]) -> Dict[str, Any]:
        current = self._resolve_gate_test_scenario(state)
        return {
            "current": current,
            "label": {
                "live": "Live market",
                "within15": "Within 15 points",
                "within5": "Within 5 points",
                "below2": "2 full 5-minute candles below short strike",
                "below30": "More than 30 minutes below short strike",
            }.get(current, "Live market"),
            "options": [
                {"value": "live", "label": "Live market"},
                {"value": "within15", "label": "Within 15 points"},
                {"value": "within5", "label": "Within 5 points"},
                {"value": "below2", "label": "2 full 5-minute candles below short strike"},
                {"value": "below30", "label": "More than 30 minutes below short strike"},
            ],
        }

    def _build_parameter_rows(self, state: Dict[str, Any]) -> Dict[str, Any]:
        parameters = self._get_effective_parameters(state)
        return {
            "values": parameters,
            "rows": [
                {
                    "key": "fortress_base_width",
                    "label": "Base Width",
                    "value": self._format_number(parameters.get("fortress_base_width")),
                    "description": "Retained Fortress width default from Apollo policy snapshots.",
                },
                {
                    "key": "fortress_base_contracts",
                    "label": "Base Contracts",
                    "value": self._format_number(parameters.get("fortress_base_contracts"), decimals=0),
                    "description": "Reference contract count from Apollo Fortress defaults.",
                },
                {
                    "key": "fortress_neighborhood_range",
                    "label": "Neighborhood Range",
                    "value": self._format_number(parameters.get("fortress_neighborhood_range")),
                    "description": "Strike search range inherited from Apollo Fortress logic.",
                },
                {
                    "key": "fortress_max_loss_cap_dollars",
                    "label": "Max Loss Cap",
                    "value": self._format_currency(parameters.get("fortress_max_loss_cap_dollars")),
                    "description": f"Apollo reference cap. Talos {HOSTED_APP_VERSION} applies the separate 5% account Black Swan rule for execution sizing.",
                },
            ],
        }

    def _build_simulated_trade_payload(
        self,
        *,
        candidate_payload: Dict[str, Any],
        sizing_payload: Dict[str, Any],
        account_payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        candidate = dict(candidate_payload.get("raw_candidate") or {})
        precheck = dict(candidate_payload.get("raw_precheck") or {})
        option_chain = precheck.get("option_chain") or {}
        structure = precheck.get("structure") or {}
        macro = precheck.get("macro") or {}
        spx = precheck.get("spx") or {}
        vix = precheck.get("vix") or {}
        now = self._now().replace(second=0, microsecond=0)
        credit = self._coerce_float(candidate.get("credit") or candidate.get("premium_per_contract")) or 0.0
        contracts = int(sizing_payload.get("contracts_selected") or 0)
        metadata_note = (
            "Talos simulated ownership | system=Talos | profile=Fortress | mode=Simulated | source=TalosEngine | "
            "automation=true | strategy=Apollo Fortress | execution_status=Simulated/NoBroker | "
            f"talos_mode_at_entry={self.MODE_SIMULATED} | talos_version={HOSTED_APP_VERSION}"
        )
        return {
            "trade_mode": "simulated",
            "system_name": "Talos",
            "journal_name": JOURNAL_NAME_DEFAULT,
            "system_version": HOSTED_APP_VERSION,
            "candidate_profile": "Fortress",
            "status": "open",
            "trade_date": now.date().isoformat(),
            "entry_datetime": now.isoformat(),
            "expiration_date": option_chain.get("expiration_date") or precheck.get("market_calendar", {}).get("next_market_day") or "",
            "underlying_symbol": option_chain.get("symbol_requested") or "SPX",
            "spx_at_entry": spx.get("value") or "",
            "vix_at_entry": vix.get("value") or "",
            "structure_grade": structure.get("final_grade") or structure.get("grade") or "",
            "macro_grade": macro.get("grade") or "",
            "expected_move": candidate.get("expected_move") or precheck.get("trade_candidates", {}).get("expected_move") or "",
            "expected_move_used": candidate.get("expected_move_used") or candidate.get("expected_move") or precheck.get("trade_candidates", {}).get("expected_move") or "",
            "expected_move_source": candidate.get("expected_move_source") or "same_day_atm_straddle",
            "option_type": "Put Credit Spread",
            "short_strike": candidate.get("short_strike") or "",
            "long_strike": candidate.get("long_strike") or "",
            "spread_width": candidate.get("width") or "",
            "contracts": contracts,
            "candidate_credit_estimate": credit,
            "actual_entry_credit": credit,
            "distance_to_short": candidate.get("actual_distance_to_short") or candidate.get("distance_points") or "",
            "em_multiple_floor": candidate.get("applied_em_multiple_floor") or candidate.get("target_em_multiple") or "",
            "percent_floor": candidate.get("percent_floor") or "",
            "boundary_rule_used": candidate.get("boundary_rule_used") or "",
            "actual_distance_to_short": candidate.get("actual_distance_to_short") or candidate.get("distance_points") or "",
            "actual_em_multiple": candidate.get("actual_em_multiple") or candidate.get("em_multiple") or "",
            "pass_type": candidate.get("pass_type") or "Fortress",
            "premium_per_contract": round(credit * 100.0, 2),
            "total_premium": round(credit * 100.0 * contracts, 2),
            "max_theoretical_risk": round((self._resolve_max_theoretical_loss_per_contract(candidate) or 0.0) * contracts, 2),
            "risk_efficiency": candidate.get("risk_efficiency") or "",
            "target_em": candidate.get("target_em") or candidate.get("target_em_multiple") or "",
            "fallback_used": "yes" if candidate.get("fallback_used") else "no",
            "fallback_rule_name": candidate.get("fallback_rule_name") or "",
            "short_delta": candidate.get("short_delta") or "",
            "notes_entry": f"{metadata_note} | account_value={self._format_currency(account_payload.get('account_value'))}",
            "prefill_source": "TalosEngine",
            "automation_status": "TalosEngine|automation=true|mode=SIMULATED|execution=NoBroker|strategy=Apollo Fortress",
        }

    def _build_real_trade_payload(
        self,
        *,
        candidate_payload: Dict[str, Any],
        sizing_payload: Dict[str, Any],
        account_payload: Dict[str, Any],
        account_context: Dict[str, Any],
    ) -> Dict[str, Any]:
        payload = self._build_simulated_trade_payload(
            candidate_payload=candidate_payload,
            sizing_payload=sizing_payload,
            account_payload=account_payload,
        )
        candidate = dict(candidate_payload.get("raw_candidate") or {})
        short_symbol = str((candidate.get("short_put") or {}).get("symbol") or candidate.get("short_symbol") or "").strip()
        long_symbol = str((candidate.get("long_put") or {}).get("symbol") or candidate.get("long_symbol") or "").strip()
        payload.update(
            {
                "trade_mode": "real",
                "notes_entry": (
                    "Talos real ownership | system=Talos | profile=Fortress | mode=Real | source=TalosEngine | "
                    f"account_hash_source={account_context.get('source') or 'Unavailable'} | short_option_symbol={short_symbol} | long_option_symbol={long_symbol}"
                ),
                "automation_status": "TalosEngine|automation=false|mode=ACTIVE|execution=Schwab|strategy=Apollo Fortress",
            }
        )
        return payload

    def _select_fortress_candidate(self, precheck: Dict[str, Any]) -> Dict[str, Any] | None:
        for item in (precheck.get("trade_candidates") or {}).get("candidates") or []:
            if isinstance(item, dict) and str(item.get("mode_key") or "").strip().lower() == "fortress":
                return item
        return None

    def _load_talos_trades(self) -> list[Dict[str, Any]]:
        records: dict[Any, Dict[str, Any]] = {}
        for trade_mode in ("real", "simulated", "talos"):
            for trade in self.trade_store.list_trades(trade_mode):
                if resolve_trade_system_name(trade) != "Talos":
                    continue
                key = trade.get("id") or (trade.get("trade_number"), trade.get("trade_mode"), trade.get("trade_date"))
                records[key] = dict(trade)
        return list(records.values())

    def _open_simulated_talos_trades(self, trades: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
        return [
            trade
            for trade in trades
            if str(trade.get("trade_mode") or "").strip().lower() == "simulated"
            and str(trade.get("derived_status_raw") or trade.get("status") or "").strip().lower() in {"open", "reduced"}
        ]

    def _open_non_expiring_simulated_talos_trades(self, trades: list[Dict[str, Any]], *, now: datetime | None = None) -> list[Dict[str, Any]]:
        evaluation_now = now or self._now()
        return [trade for trade in self._open_simulated_talos_trades(trades) if not self._is_same_day_expiring_trade(trade, now=evaluation_now)]

    def _open_black_swan_exposure(self, trades: list[Dict[str, Any]]) -> float:
        return self._build_exposure_snapshot(trades)["counted_open_exposure"]

    def _build_exposure_snapshot(self, trades: list[Dict[str, Any]], *, now: datetime | None = None) -> Dict[str, float]:
        evaluation_now = now or self._now()
        counted_open_exposure = 0.0
        ignored_same_day_exposure = 0.0
        for trade in self._load_open_talos_trades(trades):
            trade_exposure = self._resolve_trade_black_swan_loss_total(trade)
            if self._is_same_day_expiring_trade(trade, now=evaluation_now):
                ignored_same_day_exposure += trade_exposure
            else:
                counted_open_exposure += trade_exposure
        counted_open_exposure = round(counted_open_exposure, 2)
        ignored_same_day_exposure = round(ignored_same_day_exposure, 2)
        return {
            "counted_open_exposure": counted_open_exposure,
            "ignored_same_day_exposure": ignored_same_day_exposure,
            "total_open_exposure": round(counted_open_exposure + ignored_same_day_exposure, 2),
        }

    def _load_open_talos_trades(self, trades: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
        return [
            trade
            for trade in trades
            if str(trade.get("derived_status_raw") or trade.get("status") or "").strip().lower() in {"open", "reduced"}
        ]

    def _resolve_trade_expiration_date(self, trade: Dict[str, Any]) -> date | None:
        expiration_value = str(trade.get("expiration_date") or "").strip()
        if not expiration_value:
            return None
        try:
            return date.fromisoformat(expiration_value[:10])
        except ValueError:
            return None

    def _is_same_day_expiring_trade(self, trade: Dict[str, Any], *, now: datetime | None = None) -> bool:
        evaluation_now = now or self._now()
        expiration_date = self._resolve_trade_expiration_date(trade)
        return expiration_date is not None and expiration_date == evaluation_now.date()

    def _record_requires_exit_action(self, record: Dict[str, Any]) -> bool:
        action_type = str(record.get("action_type") or "").strip().lower()
        return action_type in {"reduce", "partial exit", "full exit", "close"}

    def _resolve_visual_trade_mode(self, trade: Dict[str, Any]) -> str:
        return "simulated" if str(trade.get("trade_mode") or "").strip().lower() == "simulated" else "real"

    def _resolve_trade_black_swan_loss_total(self, trade: Dict[str, Any]) -> float:
        direct_total = (
            self._coerce_float(trade.get("projected_black_swan_loss"))
            or self._coerce_float(trade.get("black_swan_loss"))
            or self._coerce_float(trade.get("realistic_max_loss"))
        )
        if direct_total is not None:
            return round(max(direct_total, 0.0), 2)

        notes_entry = str(trade.get("notes_entry") or "")
        match = re.search(r"->\s*\$?([\d,]+(?:\.\d+)?)\s+projected\s+black\s+swan\s+loss", notes_entry, re.IGNORECASE)
        if match:
            parsed_total = self._coerce_float(match.group(1).replace(",", ""))
            if parsed_total is not None:
                original_contracts = max(int(trade.get("contracts") or 0), 0)
                remaining_contracts = max(
                    int(trade.get("remaining_contracts") or trade.get("contracts_remaining") or original_contracts or 0),
                    0,
                )
                if original_contracts > 0 and remaining_contracts > 0 and remaining_contracts != original_contracts:
                    parsed_total = parsed_total * (remaining_contracts / original_contracts)
                return round(max(parsed_total, 0.0), 2)

        fallback_total = (
            self._coerce_float(trade.get("total_max_loss"))
            or self._coerce_float(trade.get("max_theoretical_risk"))
            or self._coerce_float(trade.get("max_loss"))
            or 0.0
        )
        return round(max(fallback_total, 0.0), 2)

    def _blocked_result(
        self,
        state: Dict[str, Any],
        *,
        event_type: str,
        reason: str,
        details: str,
    ) -> Dict[str, Any]:
        self._append_activity(
            state,
            event_type=event_type,
            result="blocked",
            reason=reason,
            details=details,
        )
        return {"ok": False, "level": "warning", "message": details if reason == details else f"{details} ({reason})"}

    def _allow_autonomous_open_attempt(
        self,
        state: Dict[str, Any],
        *,
        event_type: str,
        trigger_reason: str,
        now: datetime,
    ) -> bool:
        timing_state = self._resolve_timing_state(now)
        if timing_state == self.TIMING_OPEN_WINDOW_ACTIVE:
            state["last_auto_open_timing_block"] = ""
            return True
        block_signature = f"{now.date().isoformat()}|{timing_state}|{trigger_reason}"
        if str(state.get("last_auto_open_timing_block") or "") != block_signature:
            timing_payload = self._build_timing_status_payload()
            self._append_activity(
                state,
                event_type=event_type,
                result="blocked",
                reason="autonomous open blocked outside approved timing window",
                details=(
                    f"Autonomous Talos open attempt refused. Timing status: {timing_payload.get('label') or 'Unavailable'}. "
                    "No order routing or trade creation was allowed."
                ),
                mode=self.MODE_SIMULATED,
            )
            state["last_auto_open_timing_block"] = block_signature
        return False

    def _enforce_autonomous_open_timing(
        self,
        state: Dict[str, Any],
        *,
        event_type: str,
        trigger_reason: str,
        now: datetime,
    ) -> Dict[str, Any] | None:
        if self._allow_autonomous_open_attempt(
            state,
            event_type=event_type,
            trigger_reason=trigger_reason,
            now=now,
        ):
            return None
        timing_payload = self._build_timing_status_payload()
        return {
            "ok": False,
            "level": "warning",
            "message": f"Autonomous open blocked. {timing_payload.get('label') or 'Timing window unavailable'}.",
        }

    def _append_activity(
        self,
        state: Dict[str, Any],
        *,
        event_type: str,
        result: str,
        reason: str,
        details: str,
        mode: str | None = None,
    ) -> None:
        entry = {
            "timestamp": self._format_timestamp(self._now()),
            "mode": self._mode_label(mode or self._normalize_master_mode(state.get("master_mode"))),
            "event_type": event_type,
            "result": result,
            "reason": reason,
            "details": details,
        }
        activity_log = [item for item in (state.get("activity_log") or []) if isinstance(item, dict)]
        activity_log.insert(0, entry)
        state["activity_log"] = activity_log[: self.MAX_ACTIVITY_ITEMS]

    @staticmethod
    def _is_manual_forced_trigger(trigger_reason: str) -> bool:
        normalized = str(trigger_reason or "").strip().lower()
        return normalized.startswith("manual") or normalized.startswith("force")

    def _build_monitor_delta_payload(self, *, now: datetime, owned_trades: list[Dict[str, Any]]) -> Dict[str, Any]:
        try:
            precheck = self.apollo_service.run_precheck(force_refresh=False, caller_source="talos-monitor-delta")
        except Exception:
            precheck = {}
        header_snapshots = dict(precheck.get("header_market_snapshots") or {})
        current_spx_display = self._extract_market_snapshot_display(header_snapshots.get("^GSPC"), fallback="—")
        current_vix_display = self._extract_market_snapshot_display(header_snapshots.get("^VIX"), fallback="—")
        live_expected_move_display = str((precheck.get("trade_candidates") or {}).get("expected_move") or "—")
        open_trades = self._load_open_talos_trades(owned_trades)
        timing_state = self._resolve_timing_state(now)
        signature = json.dumps(
            {
                "timing_state": timing_state,
                "open_trade_count": len(open_trades),
                "current_spx_display": current_spx_display,
                "current_vix_display": current_vix_display,
                "live_expected_move_display": live_expected_move_display,
                "open_exposure": self._build_exposure_snapshot(owned_trades, now=now).get("counted_open_exposure"),
            },
            sort_keys=True,
        )
        return {
            "signature": signature,
            "timing_state": timing_state,
            "current_spx_display": current_spx_display,
            "current_vix_display": current_vix_display,
            "live_expected_move_display": live_expected_move_display,
        }

    def _can_skip_monitor_recalculation(
        self,
        state: Dict[str, Any],
        *,
        delta_payload: Dict[str, Any],
        owned_trades: list[Dict[str, Any]],
    ) -> bool:
        if self._load_open_talos_trades(owned_trades):
            return False
        monitor_state = dict(state.get("monitor_loop") or {})
        prior_signature = str(monitor_state.get("delta_signature") or "")
        if not prior_signature:
            return False
        return prior_signature == str(delta_payload.get("signature") or "")

    def _update_monitored_trade_state(self, record: Dict[str, Any], trades_by_id: Dict[int, Dict[str, Any]], now: datetime) -> None:
        trade_id = int(record.get("trade_id") or 0)
        trade = trades_by_id.get(trade_id)
        if trade is None:
            return
        updated_trade = dict(trade)
        updated_trade["last_status"] = str(record.get("status") or updated_trade.get("last_status") or "")
        updated_trade["last_action_sent"] = str(record.get("action_type") or updated_trade.get("last_action_sent") or "")
        updated_trade["last_alert_timestamp"] = now.isoformat()
        self.trade_store.update_trade(trade_id, updated_trade)

    def _track_monitor_actions(
        self,
        state: Dict[str, Any],
        records: list[Dict[str, Any]],
        trades_by_id: Dict[int, Dict[str, Any]],
        now: datetime,
    ) -> None:
        monitor_state = dict(state.get("monitor_loop") or {})
        logged_signatures = dict(monitor_state.get("logged_action_signatures") or {})
        applied_signatures = dict(monitor_state.get("applied_action_signatures") or {})
        mode = self._normalize_master_mode(state.get("master_mode"))

        for record in records:
            action_type = str(record.get("action_type") or "").strip().lower()
            contracts_to_close = max(int(record.get("contracts_to_close") or 0), 0)
            if action_type not in {"reduce", "partial exit", "full exit", "close"} or contracts_to_close <= 0:
                continue

            trade_id = str(int(record.get("trade_id") or 0))
            signature = self._monitor_action_signature(record)
            visual_mode = "simulated" if str(record.get("trade_mode") or "").strip().lower() == "simulated" else "real"
            if visual_mode == "simulated" and mode == self.MODE_SIMULATED:
                if applied_signatures.get(trade_id) == signature:
                    continue
                if self._apply_simulated_monitor_action(record, trades_by_id, now):
                    applied_signatures[trade_id] = signature
                    logged_signatures[trade_id] = signature
                    self._append_activity(
                        state,
                        event_type="monitor-simulated-exit",
                        result="ok",
                        reason=str(record.get("status") or "Exit action"),
                        details=(
                            f"Applied simulated {str(record.get('action_recommendation') or 'close').lower()} for Trade #{record.get('trade_number') or trade_id}. "
                            f"Contracts closed: {contracts_to_close}."
                        ),
                        mode=mode,
                    )
                elif logged_signatures.get(trade_id) != signature:
                    logged_signatures[trade_id] = signature
                    self._append_activity(
                        state,
                        event_type="monitor-simulated-exit",
                        result="blocked",
                        reason="live close price unavailable",
                        details=f"Talos identified a simulated exit for Trade #{record.get('trade_number') or trade_id}, but no close price was available.",
                        mode=mode,
                    )
                continue

            if logged_signatures.get(trade_id) == signature:
                continue
            logged_signatures[trade_id] = signature
            self._append_activity(
                state,
                event_type="monitor-close-intent",
                result="queued",
                reason=str(record.get("status") or "Exit action"),
                details=(
                    f"Real execution prepared but blocked for Trade #{record.get('trade_number') or trade_id}. "
                    f"Planned action: {str(record.get('action_recommendation') or 'close').lower()} | gate={record.get('active_gate_key') or 'manual-review'}."
                ),
                mode=mode,
            )

        active_trade_ids = {str(int(item.get("trade_id") or 0)) for item in records if int(item.get("trade_id") or 0) > 0}
        monitor_state["logged_action_signatures"] = {key: value for key, value in logged_signatures.items() if key in active_trade_ids}
        monitor_state["applied_action_signatures"] = {key: value for key, value in applied_signatures.items() if key in active_trade_ids}
        state["monitor_loop"] = monitor_state

    def _apply_simulated_monitor_action(
        self,
        record: Dict[str, Any],
        trades_by_id: Dict[int, Dict[str, Any]],
        now: datetime,
    ) -> bool:
        trade_id = int(record.get("trade_id") or 0)
        trade = trades_by_id.get(trade_id)
        if trade is None:
            return False
        close_value = self._coerce_float(record.get("current_close_price"))
        if close_value is None:
            close_value = self._coerce_float(record.get("current_spread_mark"))
        if close_value is None:
            return False

        remaining_contracts = max(int(trade.get("remaining_contracts") or trade.get("contracts_remaining") or trade.get("contracts") or 0), 0)
        contracts_to_close = min(max(int(record.get("contracts_to_close") or 0), 0), remaining_contracts)
        if contracts_to_close <= 0:
            return False

        close_method = "Reduce" if contracts_to_close < remaining_contracts else "Close"
        gate_key = str(record.get("active_gate_key") or "").strip().lower()
        close_events = [
            *list(trade.get("close_events") or []),
            {
                "contracts_closed": contracts_to_close,
                "actual_exit_value": close_value,
                "event_datetime": now.isoformat(),
                "close_method": close_method,
                "close_reason": str(record.get("next_active_gate_label") or "Talos Fortress gate"),
                "notes_exit": (
                    f"Talos monitor simulated {close_method.lower()} | source=TalosMonitor | "
                    f"status={record.get('status') or 'Exit'} | execution_status=Simulated/NoBroker | gate={gate_key or 'unknown'}"
                ),
            },
        ]
        updated_trade = self.open_trade_manager.update_talos_trade_gate_state({**trade, "close_events": close_events}, gate_key) if gate_key else {**trade, "close_events": close_events}
        self.trade_store.update_trade(trade_id, updated_trade)
        trades_by_id[trade_id] = updated_trade
        return True

    def _monitor_action_signature(self, record: Dict[str, Any]) -> str:
        return "|".join(
            [
                str(record.get("status") or ""),
                str(record.get("action_type") or ""),
                str(record.get("contracts_to_close") or 0),
                str(record.get("contracts") or 0),
                str(record.get("current_close_price") or record.get("current_spread_mark") or ""),
            ]
        )

    def _record_monitor_snapshot(
        self,
        state: Dict[str, Any],
        *,
        now: datetime,
        records: list[Dict[str, Any]],
        market_status: str,
        current_spx_display: str,
        current_vix_display: str,
        live_expected_move_display: str,
    ) -> None:
        monitor_state = dict(state.get("monitor_loop") or {})
        monitor_state.update(
            {
                "last_run_at": now.isoformat(),
                "last_run_display": self._format_timestamp(now),
                "market_status": market_status,
                "evaluated_trade_count": len(records),
                "current_spx_display": current_spx_display or "—",
                "current_vix_display": current_vix_display or "—",
                "live_expected_move_display": live_expected_move_display or "—",
                "records": [
                    {
                        "trade_id": int(item.get("trade_id") or 0),
                        "trade_number": item.get("trade_number") or "—",
                        "trade_mode": "Simulated" if str(item.get("trade_mode") or "").strip().lower() == "simulated" else "Real",
                        "trade_mode_key": "simulated" if str(item.get("trade_mode") or "").strip().lower() == "simulated" else "real",
                        "system_label": (
                            "TALOS SIMULATED"
                            if str(item.get("trade_mode") or "").strip().lower() == "simulated"
                            else "TALOS REAL"
                        ),
                        "status": str(item.get("status") or "—"),
                        "action_recommendation": str(item.get("action_recommendation") or "Monitor"),
                        "next_trigger": str(item.get("next_trigger") or "—"),
                        "reason": str(item.get("reason") or "—"),
                        "current_em_multiple_display": str(item.get("current_em_multiple_display") or "—"),
                        "current_live_expected_move_display": str(item.get("current_live_expected_move_display") or "—"),
                        "evaluated_at_display": str(item.get("evaluated_at_display") or self._format_timestamp(now)),
                    }
                    for item in records[: self.MAX_MONITOR_RECORDS]
                ],
            }
        )
        state["monitor_loop"] = monitor_state

    def _build_monitor_loop_payload(self, state: Dict[str, Any]) -> Dict[str, Any]:
        monitor_state = dict(state.get("monitor_loop") or {})
        return {
            "last_run_at": str(monitor_state.get("last_run_at") or ""),
            "last_run_display": str(monitor_state.get("last_run_display") or "Waiting for first cycle"),
            "market_status": str(monitor_state.get("market_status") or "Waiting for first cycle"),
            "evaluated_trade_count": int(monitor_state.get("evaluated_trade_count") or 0),
            "current_spx_display": str(monitor_state.get("current_spx_display") or "—"),
            "current_vix_display": str(monitor_state.get("current_vix_display") or "—"),
            "live_expected_move_display": str(monitor_state.get("live_expected_move_display") or "—"),
            "timing_state": str(monitor_state.get("timing_state") or self.TIMING_MARKET_CLOSED),
            "last_delta_reason": str(monitor_state.get("last_delta_reason") or ""),
            "records": [dict(item) for item in (monitor_state.get("records") or []) if isinstance(item, dict)],
        }

    def _build_default_state(self) -> Dict[str, Any]:
        return {
            "master_mode": self.MODE_SIMULATED,
            "manual_account_value": self.DEFAULT_MANUAL_ACCOUNT_VALUE,
            "gate_test_scenario": "live",
            "parameter_overrides": {},
            "activity_log": [],
            "monitor_loop": {
                "last_run_at": "",
                "last_run_display": "Waiting for first cycle",
                "market_status": "Waiting for first cycle",
                "evaluated_trade_count": 0,
                "current_spx_display": "—",
                "current_vix_display": "—",
                "live_expected_move_display": "—",
                "timing_state": self.TIMING_MARKET_CLOSED,
                "delta_signature": "",
                "last_delta_reason": "",
                "records": [],
                "logged_action_signatures": {},
                "applied_action_signatures": {},
            },
            "last_account_status_key": "",
            "last_auto_open_check_on": "",
            "last_auto_close_check_on": "",
            "last_auto_open_timing_block": "",
            "real_execution": {
                "open_preview": {},
                "close_preview": {},
                "open_preview_signature": "",
                "close_preview_signature": "",
                "last_open_reason": "",
                "last_close_reason": "",
            },
        }

    def _default_state(self) -> Dict[str, Any]:
        return self._build_default_state()

    def _load_state(self) -> Dict[str, Any]:
        baseline = self._build_default_state()
        if not self.state_path.exists():
            return baseline
        try:
            data = json.loads(self.state_path.read_text(encoding="utf-8"))
        except Exception:
            return baseline
        if not isinstance(data, dict):
            return baseline
        return self._merge_dicts(baseline, data)

    def _save_state(self, state: Dict[str, Any]) -> None:
        self.state_path.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")

    def _merge_dicts(self, baseline: Dict[str, Any], overrides: Dict[str, Any]) -> Dict[str, Any]:
        merged = copy.deepcopy(baseline)
        for key, value in overrides.items():
            if isinstance(value, dict) and isinstance(merged.get(key), dict):
                merged[key] = self._merge_dicts(dict(merged[key]), value)
            else:
                merged[key] = value
        merged["gate_test_scenario"] = self._normalize_gate_test_scenario(merged.get("gate_test_scenario"))
        return merged

    def _resolve_gate_test_scenario(self, state: Dict[str, Any]) -> str:
        return self._normalize_gate_test_scenario(state.get("gate_test_scenario"))

    @staticmethod
    def _normalize_gate_test_scenario(value: Any) -> str:
        normalized = str(value or "live").strip().lower() or "live"
        if normalized not in {"live", "within15", "within5", "below2", "below30"}:
            return "live"
        return normalized

    def _get_effective_parameters(self, state: Dict[str, Any]) -> Dict[str, Any]:
        defaults = copy.deepcopy(build_default_strategy_snapshots()["apollo"].parameters)
        for key, value in dict(state.get("parameter_overrides") or {}).items():
            defaults[key] = value
        return defaults

    def _normalize_master_mode(self, value: Any) -> str:
        candidate = str(value or "").strip().upper()
        if candidate in self.MASTER_MODES:
            return candidate
        return self.MODE_SIMULATED

    def _mode_label(self, mode: str) -> str:
        return self._normalize_master_mode(mode)

    def _mode_tone(self, mode: str) -> str:
        normalized = self._normalize_master_mode(mode)
        return {
            self.MODE_INACTIVE: "inactive",
            self.MODE_SIMULATED: "simulated",
            self.MODE_ACTIVE: "active",
        }.get(normalized, "inactive")

    def _resolve_manual_account_value(self, state: Dict[str, Any]) -> float:
        parsed = self._coerce_float(state.get("manual_account_value"))
        if parsed is None or parsed <= 0:
            return self.DEFAULT_MANUAL_ACCOUNT_VALUE
        return round(parsed, 2)

    def _format_numeric_input(self, value: float | None) -> str:
        if value is None:
            return ""
        return f"{value:.2f}"

    def _now(self) -> datetime:
        return datetime.now(self.display_timezone)

    def _minutes_to_regular_close(self, now: datetime) -> int:
        close_at = datetime.combine(now.date(), self.REGULAR_MARKET_CLOSE, tzinfo=self.display_timezone)
        return max(int((close_at - now).total_seconds() // 60), 0)

    def _is_regular_session(self, now: datetime) -> bool:
        return now.weekday() < 5 and self.REGULAR_MARKET_OPEN <= now.time() <= self.REGULAR_MARKET_CLOSE

    def _resolve_timing_state(self, now: datetime) -> str:
        if not self._is_regular_session(now):
            return self.TIMING_MARKET_CLOSED
        minutes_to_close = self._minutes_to_regular_close(now)
        if minutes_to_close <= self.SIMULATED_OPEN_WINDOW_MINUTES:
            return self.TIMING_OPEN_WINDOW_ACTIVE
        if minutes_to_close <= self.SIMULATED_CLOSE_WINDOW_MINUTES:
            return self.TIMING_EXIT_ONLY_WINDOW
        return self.TIMING_BEFORE_OPEN_WINDOW

    def _is_open_window(self, now: datetime) -> bool:
        return self._minutes_to_regular_close(now) <= self.SIMULATED_OPEN_WINDOW_MINUTES

    def _is_close_window(self, now: datetime) -> bool:
        return self._minutes_to_regular_close(now) <= self.SIMULATED_CLOSE_WINDOW_MINUTES

    def _resolve_black_swan_loss_per_contract(self, candidate: Dict[str, Any]) -> float | None:
        total_black_swan_loss = self._coerce_float(
            candidate.get("projected_black_swan_loss")
            or candidate.get("black_swan_loss")
            or candidate.get("realistic_max_loss")
        )
        recommended_contracts = self._coerce_float(candidate.get("recommended_contract_size") or candidate.get("contracts"))
        if total_black_swan_loss is not None and recommended_contracts not in {None, 0.0}:
            return round(max(total_black_swan_loss / recommended_contracts, 0.0), 2)

        direct = self._coerce_float(candidate.get("projected_black_swan_loss_per_contract") or candidate.get("black_swan_loss_per_contract"))
        if direct is not None:
            return round(direct, 2)

        return self._resolve_max_theoretical_loss_per_contract(candidate)

    def _resolve_max_theoretical_loss_per_contract(self, candidate: Dict[str, Any]) -> float | None:
        direct = self._coerce_float(candidate.get("max_theoretical_risk_per_contract"))
        if direct is not None:
            return round(direct, 2)
        spread_width = self._coerce_float(candidate.get("width"))
        credit = self._coerce_float(candidate.get("credit") or candidate.get("premium_per_contract"))
        if spread_width is None or credit is None:
            return None
        return round(max((spread_width - credit) * 100.0, 0.0), 2)

    def _trade_sort_key(self, trade: Dict[str, Any]) -> str:
        return str(trade.get("entry_datetime") or trade.get("trade_date") or trade.get("updated_at") or "")

    def _format_strike_pair(self, trade: Dict[str, Any]) -> str:
        short_strike = self._format_number(trade.get("short_strike"))
        long_strike = self._format_number(trade.get("long_strike"))
        return f"{short_strike} / {long_strike}" if short_strike != "—" or long_strike != "—" else "—"

    def _to_bool(self, value: Any, *, default: bool = False) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {"1", "true", "yes", "on"}

    def _parse_numeric(self, value: Any) -> float | int | None:
        text = str(value or "").strip()
        if not text:
            return None
        try:
            parsed = float(text)
        except ValueError:
            return None
        return int(parsed) if parsed.is_integer() else parsed

    def _coerce_float(self, value: Any) -> float | None:
        if value in {None, ""}:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _format_currency(self, value: Any) -> str:
        if value in (None, ""):
            return "—"
        try:
            return f"${float(value):,.2f}"
        except (TypeError, ValueError):
            return str(value)

    def _format_number(self, value: Any, *, decimals: int = 1) -> str:
        if value in (None, ""):
            return "—"
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return str(value)
        if decimals == 0 or numeric.is_integer():
            return str(int(round(numeric)))
        return f"{numeric:.{decimals}f}"

    def _format_timestamp(self, value: datetime) -> str:
        return value.strftime("%Y-%m-%d %I:%M %p %Z").replace(" 0", " ")

    def _extract_market_snapshot_display(self, snapshot: Any, *, fallback: str) -> str:
        if isinstance(snapshot, dict):
            for key in ("price_display", "last_display", "close_display", "value_display", "current_price_display"):
                value = str(snapshot.get(key) or "").strip()
                if value:
                    return value
            for key in ("price", "last", "close", "value", "current_price"):
                numeric_value = self._coerce_float(snapshot.get(key))
                if numeric_value is not None:
                    return self._format_number(numeric_value)
        return fallback or "—"