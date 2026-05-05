"""Open-trade management, scheduled notifications, and deduped alerts."""

from __future__ import annotations

import logging
import math
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, time, timedelta
from pathlib import Path
from threading import RLock
from typing import Any, Callable, Dict, Iterable, List, Optional
from zoneinfo import ZoneInfo

from config import AppConfig, get_app_config

from .apollo_service import ApolloService
from .market_calendar_service import MarketCalendarService
from .market_data import MarketDataError, MarketDataService
from .options_chain_service import OptionsChainService
from .repositories.management_state_repository import (
    OpenTradeManagementStateRepository,
    SQLiteOpenTradeManagementStateRepository,
    build_management_state_payload,
)
from .repositories.global_notification_settings_repository import GlobalNotificationSettingsRepository
from .repositories.trade_notification_repository import TradeNotificationRepository
from .repositories.trade_repository import TradeRepository
from .runtime.notifications import NotificationDelivery
from .runtime.scheduler import RuntimeJobHandle, RuntimeScheduler, ThreadingTimerScheduler
from .trade_notifications import (
    GLOBAL_NOTIFICATION_DAILY_END,
    GLOBAL_NOTIFICATION_DAILY_START,
    GLOBAL_NOTIFICATION_OPEN_TRADES,
    default_global_notification_settings,
    evaluate_trade_notifications,
    normalize_trade_notifications,
)
from .trade_store import (
    current_timestamp,
    is_retained_trade_system,
    parse_date_value,
    parse_datetime_value,
    resolve_trade_candidate_profile,
    resolve_trade_credit_model,
    resolve_trade_system_name,
    summarize_trade_close_events,
    to_float,
)


LOGGER = logging.getLogger(__name__)


class OpenTradeManager:
    """Classify open trades and send low-noise management alerts."""

    STATUS_SEVERITY = {
        "Closed": 0,
        "Healthy": 0,
        "Watch": 1,
        "Exit Approaching": 2,
        "Exit Partial": 3,
        "Exit Now": 4,
    }
    ALERT_COOLDOWN_MINUTES = {
        "Watch": 45,
        "Exit Approaching": 20,
        "Exit Partial": 20,
        "Exit Now": 5,
    }
    MARKET_OPEN_HOUR = 8
    MARKET_OPEN_MINUTE = 30
    MARKET_CLOSE_HOUR = 15
    MARKET_CLOSE_MINUTE = 0
    MORNING_SNAPSHOT_DELAY_MINUTES = 2
    EOD_SUMMARY_DELAY_MINUTES = 10
    BACKGROUND_INTERVAL_SECONDS = 60
    APOLLO_HEALTHY_EM_THRESHOLD = 2.0
    APOLLO_WATCH_EM_THRESHOLD = 1.5
    TALOS_HEALTHY_EM_THRESHOLD = 3.0
    TALOS_WATCH_EM_THRESHOLD = 1.5

    def __init__(
        self,
        *,
        trade_store: TradeRepository,
        market_data_service: MarketDataService,
        apollo_service: ApolloService,
        options_chain_service: OptionsChainService,
        notification_delivery: NotificationDelivery | None = None,
        pushover_service: NotificationDelivery | None = None,
        market_calendar_service: MarketCalendarService | None = None,
        config: AppConfig | None = None,
        now_provider: Callable[[], datetime] | None = None,
        state_repository: OpenTradeManagementStateRepository | None = None,
        trade_notification_repository: TradeNotificationRepository | None = None,
        global_notification_settings_repository: GlobalNotificationSettingsRepository | None = None,
        scheduler: RuntimeScheduler | None = None,
    ) -> None:
        self.config = config or get_app_config()
        self.trade_store = trade_store
        self.market_data_service = market_data_service
        self.apollo_service = apollo_service
        self.options_chain_service = options_chain_service
        notification_delivery = notification_delivery or pushover_service
        if notification_delivery is None:
            raise ValueError("OpenTradeManager requires a notification delivery adapter.")
        self.notification_delivery = notification_delivery
        self.pushover_service = notification_delivery
        self.market_calendar_service = market_calendar_service or MarketCalendarService(self.config)
        self.database_path = Path(self.trade_store.database_path)
        self.state_repository = state_repository or SQLiteOpenTradeManagementStateRepository(self.database_path)
        self.trade_notification_repository = trade_notification_repository
        self.global_notification_settings_repository = global_notification_settings_repository
        self.scheduler = scheduler or ThreadingTimerScheduler()
        self.display_timezone = ZoneInfo(self.config.app_timezone)
        self.now_provider = now_provider or (lambda: datetime.now(self.display_timezone))
        self._monitor_lock = RLock()
        self._monitor_timer: RuntimeJobHandle | None = None
        self._monitor_running = False

    def initialize(self) -> None:
        self.state_repository.initialize()
        if self.trade_notification_repository is not None:
            self.trade_notification_repository.initialize()
        if self.global_notification_settings_repository is not None:
            self.global_notification_settings_repository.initialize()

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

    def set_notifications_enabled(self, enabled: bool) -> None:
        self.state_repository.set_notifications_enabled(enabled)
        settings = self.load_global_notification_settings()
        target_keys = {GLOBAL_NOTIFICATION_OPEN_TRADES}
        if self.global_notification_settings_repository is None:
            target_keys = {
                GLOBAL_NOTIFICATION_OPEN_TRADES,
                GLOBAL_NOTIFICATION_DAILY_START,
                GLOBAL_NOTIFICATION_DAILY_END,
            }
        updated = [
            {**item, "enabled": bool(enabled)} if item.get("key") in target_keys else dict(item)
            for item in settings
        ]
        self.save_global_notification_settings(updated)

    def notifications_enabled(self) -> bool:
        settings = {item["key"]: item for item in self.load_global_notification_settings() if item.get("key")}
        return bool((settings.get(GLOBAL_NOTIFICATION_OPEN_TRADES) or {}).get("enabled", True))

    def load_global_notification_settings(self) -> List[Dict[str, Any]]:
        if self.global_notification_settings_repository is None:
            fallback_enabled = bool(self.state_repository.load_runtime_settings().get("notifications_enabled", True))
            return [{**item, "enabled": fallback_enabled} for item in default_global_notification_settings()]
        return self.global_notification_settings_repository.load_settings()

    def save_global_notification_settings(self, settings: List[Dict[str, Any]]) -> None:
        if self.global_notification_settings_repository is None:
            return
        self.global_notification_settings_repository.save_settings(settings)

    def _schedule_background_monitor(self) -> None:
        if not self._monitor_running:
            return
        self._monitor_timer = self.scheduler.schedule(
            self.BACKGROUND_INTERVAL_SECONDS,
            self._background_monitor_tick,
            daemon=True,
        )

    def _background_monitor_tick(self) -> None:
        try:
            self.run_background_monitor_cycle()
        except Exception as exc:  # pragma: no cover - defensive scheduler guard
            LOGGER.warning("Open trade background monitor failed: %s", exc)
        finally:
            with self._monitor_lock:
                if self._monitor_running:
                    self._schedule_background_monitor()

    def run_background_monitor_cycle(self) -> Dict[str, Any]:
        now = self._now()
        if not self._is_monitor_window(now):
            return {"ran": False, "reason": "outside-monitor-window", "evaluated_at": now.isoformat()}
        payload = self.evaluate_open_trades(send_alerts=True)
        self.state_repository.mark_runtime_setting("last_background_run_at", now.isoformat())
        return {"ran": True, **payload}

    def evaluate_open_trades(self, *, send_alerts: bool = False) -> Dict[str, Any]:
        now = self._now()
        open_trades = self._load_open_trades()
        global_notification_settings = self.load_global_notification_settings()
        global_notification_map = {
            item["key"]: item for item in global_notification_settings if item.get("key")
        }
        alert_eligible_trades = [trade for trade in open_trades if str(trade.get("trade_mode") or "").strip().lower() == "real"] if send_alerts else []
        notifications_by_trade_id: Dict[int, List[Dict[str, Any]]] = {}
        states_by_trade: Dict[int, Dict[str, Any]] = {}
        runtime_settings: Dict[str, Any] = {}
        if send_alerts:
            notifications_by_trade_id = self._load_trade_notifications(open_trades)
            states_by_trade = self._load_management_states()
            runtime_settings = self._load_runtime_settings()
        shared_context = self._build_shared_context(open_trades, now)
        alerts_sent = 0
        alert_failures: List[str] = []
        records: List[Dict[str, Any]] = []

        daily_outcomes: list[Dict[str, Any]] = []
        for trade in open_trades:
            previous_state = states_by_trade.get(int(trade.get("id") or 0), {})
            record = self._evaluate_trade(trade, shared_context, now)
            if send_alerts:
                notifications = list(notifications_by_trade_id.get(int(trade.get("id") or 0), normalize_trade_notifications([])))
                triggered_notifications = evaluate_trade_notifications({**trade, "notifications": notifications}, record)
                triggered_by_type = {item["type"]: item for item in triggered_notifications}
                record["notifications"] = [
                    {
                        **notification,
                        "triggered": notification["type"] in triggered_by_type,
                        "trigger_reason": (triggered_by_type.get(notification["type"]) or {}).get("reason", ""),
                    }
                    for notification in notifications
                ]
                record["active_notifications"] = [item for item in record["notifications"] if item.get("enabled")]
                record["triggered_notifications"] = triggered_notifications
                record["triggered_notification_count"] = len(triggered_notifications)
                alert_outcome = {"sent": False, "error": "", "priority": None, "alert_type": None, "sent_at": None}
                record["trade_notification_state"] = {
                    "last_status": self._coerce_text(trade.get("last_status"), fallback="—"),
                    "last_action_sent": self._coerce_text(trade.get("last_action_sent"), fallback="—"),
                    "last_alert_timestamp": self._format_datetime(parse_datetime_value(trade.get("last_alert_timestamp"))),
                }
                self._upsert_management_state(record, previous_state, alert_outcome, now)
                persisted_state = build_management_state_payload(record, previous_state, alert_outcome, now)
                record["alert_state"] = self._build_alert_state_payload(persisted_state, trade)
            records.append(record)

        if send_alerts:
            real_records = [
                item for item in records if str(item.get("trade_mode") or "").strip().lower() == "real"
            ]
            daily_outcomes = self._process_daily_notifications(real_records, now, runtime_settings, global_notification_map)
            for outcome in daily_outcomes:
                if outcome.get("sent"):
                    alerts_sent += 1
                elif outcome.get("error"):
                    alert_failures.append(str(outcome["error"]))
            if bool((global_notification_map.get(GLOBAL_NOTIFICATION_OPEN_TRADES) or {}).get("enabled", True)):
                for trade in alert_eligible_trades:
                    record = next((item for item in records if int(item.get("trade_id") or 0) == int(trade.get("id") or 0)), None)
                    if record is None:
                        continue
                    previous_state = states_by_trade.get(int(trade.get("id") or 0), {})
                    alert_outcome = self._process_trade_notifications(trade, record, previous_state, now)
                    if alert_outcome.get("sent"):
                        alerts_sent += int(alert_outcome.get("sent_count") or 1)
                    elif alert_outcome.get("error"):
                        alert_failures.append(f"Trade #{record['trade_number']}: {alert_outcome['error']}")
                    self._upsert_management_state(record, previous_state, alert_outcome, now)
                    persisted_state = build_management_state_payload(record, previous_state, alert_outcome, now)
                    updated_trade = dict(trade)
                    updated_trade["last_status"] = str(record.get("status") or "")
                    updated_trade["last_action_sent"] = str(record.get("action_type") or "")
                    updated_trade["last_alert_timestamp"] = alert_outcome.get("sent_at") or trade.get("last_alert_timestamp")
                    record["alert_state"] = self._build_alert_state_payload(persisted_state, updated_trade)
        records.sort(key=lambda item: (-int(item.get("status_severity") or 0), str(item.get("system_name") or ""), -int(item.get("trade_number") or 0)))
        status_counts = self._build_status_counts(records)
        return {
            "evaluated_at": now.isoformat(),
            "evaluated_at_display": self._format_datetime(now),
            "open_trade_count": len(records),
            "alerts_sent": alerts_sent,
            "alert_failures": alert_failures,
            "notifications_enabled": bool((global_notification_map.get(GLOBAL_NOTIFICATION_OPEN_TRADES) or {}).get("enabled", True)),
            "global_notification_settings": global_notification_settings,
            "last_morning_snapshot_date": runtime_settings.get("last_morning_snapshot_date") or "",
            "last_eod_summary_date": runtime_settings.get("last_eod_summary_date") or "",
            "live_expected_move_display": next((record.get("current_live_expected_move_display") for record in records if record.get("current_live_expected_move_display")), "—"),
            "header_market_snapshots": {
                "^GSPC": shared_context.get("spx_snapshot") or {},
                "^VIX": shared_context.get("vix_snapshot") or {},
            },
            "status_counts": status_counts,
            "records": records,
        }

    def evaluate_trade_record(self, trade_id: int) -> Dict[str, Any] | None:
        trade = self.trade_store.get_trade(trade_id)
        if not trade:
            return None
        now = self._now()
        shared_context = self._build_shared_context([trade], now)
        return self._evaluate_trade(trade, shared_context, now)

    def send_manual_status_update(self, *, trade_mode: str) -> Dict[str, Any]:
        normalized_trade_mode = str(trade_mode or "").strip().lower()
        if normalized_trade_mode not in {"real", "simulated"}:
            raise ValueError(f"Unsupported trade mode for manual status update: {trade_mode}")
        if normalized_trade_mode != "real":
            return {
                "sent": False,
                "error": "",
                "record_count": 0,
                "notifications_enabled": bool(self.notifications_enabled()),
            }

        payload = self.evaluate_open_trades(send_alerts=False)
        selected_records = [
            item for item in payload["records"] if str(item.get("trade_mode") or "").strip().lower() == normalized_trade_mode
        ]
        now = self._now()

        if not selected_records:
            return {
                "sent": False,
                "error": "",
                "trade_mode": normalized_trade_mode,
                "record_count": 0,
                "priority": 0,
                "alert_type": f"manual-{normalized_trade_mode}-status-update",
                "sent_at": None,
            }

        snapshot_payload = self._build_open_positions_snapshot_payload(
            selected_records,
            alert_type=f"manual-{normalized_trade_mode}-status-update",
            reason_code=f"manual-{normalized_trade_mode}-status-update",
        )
        outcome = self._send_management_alert(
            None,
            {"trade_id": 0, "trade_mode": normalized_trade_mode, "system_name": "Delphi"},
            snapshot_payload,
            now,
        )
        if outcome.get("sent"):
            self._stamp_trade_alert_timestamps(
                [int(item.get("trade_id") or 0) for item in selected_records],
                outcome.get("sent_at"),
            )

        return {
            "sent": bool(outcome.get("sent")),
            "error": str(outcome.get("error") or ""),
            "trade_mode": normalized_trade_mode,
            "record_count": len(selected_records),
            "priority": int(outcome.get("priority") or 0),
            "alert_type": str(outcome.get("alert_type") or f"manual-{normalized_trade_mode}-status-update"),
            "sent_at": outcome.get("sent_at"),
        }

    def _build_shared_context(self, open_trades: List[Dict[str, Any]], now: datetime) -> Dict[str, Any]:
        normalized_systems = {resolve_trade_system_name(item).strip().lower() for item in open_trades}
        has_apollo_trades = "apollo" in normalized_systems
        with ThreadPoolExecutor(max_workers=2) as executor:
            spx_future = executor.submit(self._safe_snapshot, "^GSPC", query_type="open_trade_management_spx")
            vix_future = executor.submit(self._safe_snapshot, "^VIX", query_type="open_trade_management_vix")
            spx_snapshot = spx_future.result()
            vix_snapshot = vix_future.result()
        expirations = {
            parsed_date
            for parsed_date in (self._coerce_trade_expiration(item) for item in open_trades)
            if parsed_date is not None
        }
        option_chains: Dict[str, Any] = {}
        sorted_expirations = sorted(expirations)
        if sorted_expirations:
            with ThreadPoolExecutor(max_workers=min(4, len(sorted_expirations))) as executor:
                future_map = {
                    executor.submit(self.options_chain_service.get_spx_option_chain_summary, expiration_date): expiration_date
                    for expiration_date in sorted_expirations
                }
                for future, expiration_date in future_map.items():
                    option_chains[expiration_date.isoformat()] = future.result()
        current_spx = self._coerce_float((spx_snapshot or {}).get("Latest Value"))
        prepared_option_chains = self._prepare_option_chain_contexts(
            option_chains=option_chains,
            spot=current_spx,
            evaluation_date=now.date(),
        )

        apollo_context: Dict[str, Any] = {}
        context_futures: Dict[Any, str] = {}
        context_worker_count = int(has_apollo_trades)
        if context_worker_count > 0:
            with ThreadPoolExecutor(max_workers=context_worker_count) as executor:
                if has_apollo_trades:
                    context_futures[executor.submit(self._build_apollo_management_context)] = "apollo"
                for future, context_name in context_futures.items():
                    if context_name == "apollo":
                        apollo_context = future.result()
        return {
            "now": now,
            "spx_snapshot": spx_snapshot,
            "vix_snapshot": vix_snapshot,
            "option_chains": option_chains,
            "prepared_option_chains": prepared_option_chains,
            "apollo": apollo_context,
            "current_spx": current_spx,
            "current_vix": self._coerce_float((vix_snapshot or {}).get("Latest Value")),
        }

    def _build_apollo_management_context(self) -> Dict[str, Any]:
        apollo_context: Dict[str, Any] = {}
        try:
            management_context_builder = getattr(self.apollo_service, "build_management_context", None)
            if callable(management_context_builder):
                apollo_context = management_context_builder()
            else:
                apollo_precheck = self.apollo_service.run_precheck()
                apollo_context = {
                    "current_structure_grade": self._coerce_text(((apollo_precheck.get("structure") or {}).get("final_grade") or (apollo_precheck.get("structure") or {}).get("grade")), fallback="Not available"),
                }
        except Exception as exc:  # pragma: no cover - defensive provider handling
            LOGGER.warning("Apollo management context unavailable: %s", exc)
            apollo_context = {}
        return apollo_context

    def _prepare_option_chain_contexts(
        self,
        *,
        option_chains: Dict[str, Any],
        spot: float | None,
        evaluation_date: date,
    ) -> Dict[str, Dict[str, Any]]:
        prepared: Dict[str, Dict[str, Any]] = {}
        for expiration_key, chain_summary in option_chains.items():
            if not isinstance(chain_summary, dict):
                continue
            expiration_date = parse_date_value(expiration_key)
            prepared[expiration_key] = self._prepare_option_chain_context(
                chain_summary=chain_summary,
                spot=spot,
                expiration_date=expiration_date,
                evaluation_date=evaluation_date,
            )
        return prepared

    def _prepare_option_chain_context(
        self,
        *,
        chain_summary: Dict[str, Any],
        spot: float | None,
        expiration_date: date | None,
        evaluation_date: date,
    ) -> Dict[str, Any]:
        normalized_puts = self._normalize_live_contracts(chain_summary.get("puts") or [])
        normalized_calls = self._normalize_live_contracts(chain_summary.get("calls") or [])
        prepared = {
            "normalized_puts": normalized_puts,
            "normalized_calls": normalized_calls,
            "puts_by_strike": {round(float(item["strike"]), 2): item for item in normalized_puts},
            "calls_by_strike": {round(float(item["strike"]), 2): item for item in normalized_calls},
            "expected_move_by_system": {},
        }
        if spot is not None:
            prepared["expected_move_by_system"] = {
                "apollo": self._build_expected_move_details_from_normalized(
                    normalized_puts=normalized_puts,
                    normalized_calls=normalized_calls,
                    spot=spot,
                    system_name="Apollo",
                    expiration_date=expiration_date,
                    evaluation_date=evaluation_date,
                ),
                "talos": self._build_expected_move_details_from_normalized(
                    normalized_puts=normalized_puts,
                    normalized_calls=normalized_calls,
                    spot=spot,
                    system_name="Talos",
                    expiration_date=expiration_date,
                    evaluation_date=evaluation_date,
                ),
            }
        return prepared

    def _evaluate_trade(self, trade: Dict[str, Any], shared_context: Dict[str, Any], now: datetime) -> Dict[str, Any]:
        system_name = resolve_trade_system_name(trade)
        trade_mode = str(trade.get("trade_mode") or "").strip().title() or "Unknown"
        option_type = self._coerce_text(trade.get("option_type"), fallback="Put Credit Spread")
        current_underlying = self._coerce_float(shared_context.get("current_spx"), fallback=self._coerce_float(trade.get("spx_at_entry"), fallback=0.0))
        current_vix = self._coerce_float(shared_context.get("current_vix"))
        metrics = self._build_live_metrics(trade, current_underlying=current_underlying, current_vix=current_vix, shared_context=shared_context, now=now)

        if system_name == "Talos":
            classification = self._classify_talos_trade(trade, metrics)
            profile_label = resolve_trade_candidate_profile(trade)
        else:
            classification = self._classify_apollo_trade(trade, metrics, shared_context.get("apollo") or {}, now)
            profile_label = resolve_trade_candidate_profile(trade)

        record = {
            "trade_id": int(trade.get("id") or 0),
            "trade_number": int(trade.get("trade_number") or 0),
            "system_name": system_name,
            "trade_mode": trade_mode,
            "profile_label": profile_label,
            "pass_type": self._coerce_text(trade.get("pass_type"), fallback="—"),
            "short_strike": self._coerce_float(trade.get("short_strike")),
            "short_strike_display": self._format_number(self._coerce_float(trade.get("short_strike"))),
            "long_strike": self._coerce_float(trade.get("long_strike")),
            "long_strike_display": self._format_number(self._coerce_float(trade.get("long_strike"))),
            "entry_timestamp": self._format_datetime(parse_datetime_value(trade.get("entry_datetime")) or now),
            "expiration": self._format_date(self._coerce_trade_expiration(trade)),
            "expiration_display": self._format_expiration_label(self._coerce_trade_expiration(trade)),
            "underlying_symbol": self._coerce_text(trade.get("underlying_symbol"), fallback="SPX"),
            "option_type": option_type,
            "net_credit_per_contract": metrics["net_credit_per_contract"],
            "net_credit_per_contract_display": self._format_number(metrics["net_credit_per_contract"], decimals=4),
            "contracts": metrics["remaining_contracts"],
            "contracts_display": str(metrics["remaining_contracts"]),
            "original_contracts": metrics["original_contracts"],
            "original_contracts_display": str(metrics["original_contracts"]),
            "total_premium": metrics["total_premium"],
            "total_premium_display": self._format_currency(metrics["total_premium"]),
            "max_loss": metrics["max_loss"],
            "max_loss_display": self._format_currency(metrics["max_loss"]),
            "current_underlying_price": metrics["current_underlying_price"],
            "current_underlying_price_display": self._format_number(metrics["current_underlying_price"]),
            "distance_to_short": metrics["distance_to_short"],
            "distance_to_short_display": self._format_distance_to_strike(metrics["distance_to_short"]),
            "distance_to_long": metrics["distance_to_long"],
            "distance_to_long_display": self._format_distance_to_strike(metrics["distance_to_long"]),
            "entry_expected_move": metrics["entry_expected_move"],
            "entry_expected_move_display": self._format_number(metrics["entry_expected_move"]),
            "entry_em_multiple": metrics["entry_em_multiple"],
            "entry_em_multiple_display": self._format_number(metrics["entry_em_multiple"]),
            "current_live_expected_move": metrics["current_live_expected_move"],
            "current_live_expected_move_display": self._format_number(metrics["current_live_expected_move"]),
            "current_live_expected_move_source_label": metrics["current_live_expected_move_source_label"],
            "current_live_expected_move_formula_label": metrics["current_live_expected_move_formula_label"],
            "current_live_expected_move_contracts_label": metrics["current_live_expected_move_contracts_label"],
            "current_em_multiple": classification["status_em_multiple"],
            "current_em_multiple_display": self._format_number(classification["status_em_multiple"]),
            "status_em_multiple": classification["status_em_multiple"],
            "status_em_multiple_display": self._format_number(classification["status_em_multiple"]),
            "current_vix": metrics["current_vix"],
            "current_vix_display": self._format_number(metrics["current_vix"]),
            "current_structure_grade": classification["current_structure_grade"],
            "time_remaining_to_expiration": metrics["time_remaining_to_expiration"],
            "time_remaining_to_expiration_display": metrics["time_remaining_display"],
            "current_spread_mark": metrics["current_spread_mark"],
            "current_spread_mark_display": self._format_number(metrics["current_spread_mark"], decimals=4),
            "current_total_close_cost": metrics["current_total_close_cost"],
            "current_total_close_cost_display": self._format_currency(metrics["current_total_close_cost"]),
            "current_close_price": metrics["current_close_price"],
            "current_close_price_display": self._format_number(metrics["current_close_price"], decimals=4),
            "unrealized_pnl": metrics["unrealized_pnl"],
            "unrealized_pnl_display": self._format_currency(metrics["unrealized_pnl"]),
            "realized_close_cost": metrics["realized_close_cost"],
            "realized_close_cost_display": self._format_currency(metrics["realized_close_cost"]),
            "closed_contracts": metrics["closed_contracts"],
            "closed_contracts_display": str(metrics["closed_contracts"]),
            "percent_credit_captured": metrics["percent_credit_captured"],
            "percent_credit_captured_display": self._format_signed_percent(metrics["percent_credit_captured"]),
            "current_pl": metrics["current_pl"],
            "current_pl_display": self._format_currency(metrics["current_pl"]),
            "status": classification["status"],
            "status_severity": int(self.STATUS_SEVERITY.get(classification["status"], 0)),
            "status_key": self._slugify(classification["status"]),
            "thesis_status": classification["thesis_status"],
            "thesis_status_key": self._slugify(classification["thesis_status"]),
            "reason": classification["reason"],
            "reason_code": classification["status_reason_code"],
            "status_reason_code": classification["status_reason_code"],
            "thesis_reason_code": classification["thesis_reason_code"],
            "next_trigger": classification["next_trigger"],
            "exit_plan_summary": classification.get("exit_plan_summary") or classification["next_trigger"],
            "exit_plan_gates": classification.get("exit_plan_gates") or [],
            "trigger_source": classification["trigger_source"],
            "structure_trigger_fired": classification["structure_trigger_fired"],
            "vwap_trigger_fired": classification["vwap_trigger_fired"],
            "short_proximity_trigger_fired": classification["short_proximity_trigger_fired"],
            "long_proximity_trigger_fired": classification["long_proximity_trigger_fired"],
            "final_stop_trigger_fired": classification["final_stop_trigger_fired"],
            "invalid_strike_order": metrics["invalid_strike_order"],
            "distance_warning": metrics["distance_warning"],
            "action_recommendation": classification["action_recommendation"],
            "action_type": classification["action_type"],
            "contracts_to_close": classification["contracts_to_close"],
            "contracts_to_close_display": str(classification["contracts_to_close"]),
            "pl_after_close": classification["pl_after_close"],
            "pl_after_close_display": self._format_currency(classification["pl_after_close"]),
            "remaining_risk": classification["remaining_risk"],
            "remaining_risk_display": self._format_currency(classification["remaining_risk"]),
            "critical_alert": classification["critical_alert"],
            "send_close_to_journal_enabled": bool(metrics["current_spread_mark"] is not None and metrics["remaining_contracts"] > 0),
            "evaluated_at": now.isoformat(),
            "evaluated_at_display": self._format_datetime(now),
        }
        return record

    def _build_live_metrics(self, trade: Dict[str, Any], *, current_underlying: float, current_vix: float | None, shared_context: Dict[str, Any], now: datetime) -> Dict[str, Any]:
        short_strike = self._coerce_float(trade.get("short_strike"), fallback=0.0)
        long_strike = self._coerce_float(trade.get("long_strike"), fallback=0.0)
        entry_underlying = self._coerce_float(trade.get("spx_at_entry") or trade.get("spx_entry"), fallback=0.0)
        close_summary = summarize_trade_close_events(trade, trade.get("close_events") or [])
        original_contracts = int(close_summary.get("original_contracts") or trade.get("contracts") or 0)
        remaining_contracts = int(close_summary.get("remaining_contracts") or trade.get("remaining_contracts") or original_contracts or 0)
        closed_contracts = max(original_contracts - remaining_contracts, 0)
        credit_model = resolve_trade_credit_model(trade)
        net_credit_per_contract = self._coerce_float(credit_model.get("net_credit_per_contract"), fallback=0.0)
        total_premium = self._coerce_float(credit_model.get("total_premium"))
        option_type = self._coerce_text(trade.get("option_type"), fallback="Put Credit Spread")
        is_call = "call" in option_type.lower()
        distance_snapshot = self._build_distance_snapshot(
            current_price=current_underlying,
            short_strike=short_strike,
            long_strike=long_strike,
            option_type=option_type,
        )
        original_buffer = self._resolve_original_buffer(trade, entry_underlying=entry_underlying, short_strike=short_strike, is_call=is_call)
        distance_to_short = distance_snapshot["distance_to_short"]
        distance_to_long = distance_snapshot["distance_to_long"]
        current_spread_mark = self._resolve_current_spread_mark(trade, shared_context)
        current_total_close_cost = None
        unrealized_pnl = None
        realized_close_cost = round(self._coerce_float(close_summary.get("total_close_cost"), fallback=0.0) or 0.0, 2)
        if current_spread_mark is not None and remaining_contracts > 0:
            current_total_close_cost = round(current_spread_mark * 100.0 * remaining_contracts, 2)
        if total_premium is not None:
            unrealized_pnl = round(total_premium - realized_close_cost, 2)
        current_close_price = current_spread_mark if current_spread_mark is not None else net_credit_per_contract
        percent_credit_captured = None
        current_pl = None
        if net_credit_per_contract not in {None, 0}:
            percent_credit_captured = round(((net_credit_per_contract - current_close_price) / net_credit_per_contract) * 100.0, 2)
        if current_close_price is not None and remaining_contracts > 0:
            current_pl = round((net_credit_per_contract - current_close_price) * remaining_contracts * 100.0, 2)

        expiration_date = self._coerce_trade_expiration(trade)
        expiration_key = expiration_date.isoformat() if expiration_date is not None else ""
        chain_summary = (shared_context.get("option_chains") or {}).get(expiration_key) if expiration_key else None
        prepared_chain = (shared_context.get("prepared_option_chains") or {}).get(expiration_key) if expiration_key else None
        current_live_expected_move_details = self._estimate_live_expected_move_from_chain(
            chain_summary=chain_summary,
            prepared_chain=prepared_chain,
            spot=current_underlying,
            system_name=resolve_trade_system_name(trade),
            expiration_date=expiration_date,
            evaluation_date=now.date(),
        )
        entry_expected_move = self._coerce_float(trade.get("expected_move_used") or trade.get("expected_move"))
        current_live_expected_move = self._coerce_float(current_live_expected_move_details.get("expected_move"))
        if current_live_expected_move in {None, 0.0}:
            current_live_expected_move = entry_expected_move
            current_live_expected_move_details = {
                "expected_move": current_live_expected_move,
                "source_label": "Entry expected move fallback",
                "formula_label": "Fallback to the stored trade entry expected move because the live chain EM was unavailable",
                "contracts_label": "—",
            }
        entry_distance_to_short = self._coerce_float(trade.get("actual_distance_to_short"))
        if entry_distance_to_short is None:
            entry_distance_to_short = original_buffer
        entry_em_multiple = self._safe_divide(entry_distance_to_short, entry_expected_move)
        time_remaining = self._build_time_remaining(expiration_date, now)
        return {
            "entry_underlying": entry_underlying,
            "net_credit_per_contract": net_credit_per_contract,
            "total_premium": total_premium,
            "max_loss": self._coerce_float(credit_model.get("max_theoretical_risk")),
            "current_underlying_price": current_underlying,
            "distance_to_short": distance_to_short,
            "distance_to_long": distance_to_long,
            "entry_expected_move": entry_expected_move,
            "entry_em_multiple": entry_em_multiple,
            "current_live_expected_move": current_live_expected_move,
            "current_live_expected_move_source_label": self._coerce_text(current_live_expected_move_details.get("source_label"), fallback="Unavailable"),
            "current_live_expected_move_formula_label": self._coerce_text(current_live_expected_move_details.get("formula_label"), fallback="Unavailable"),
            "current_live_expected_move_contracts_label": self._coerce_text(current_live_expected_move_details.get("contracts_label"), fallback="—"),
            "current_vix": current_vix,
            "current_spread_mark": current_spread_mark,
            "current_total_close_cost": current_total_close_cost,
            "current_close_price": current_close_price,
            "unrealized_pnl": unrealized_pnl,
            "percent_credit_captured": percent_credit_captured,
            "current_pl": current_pl,
            "remaining_contracts": remaining_contracts,
            "original_contracts": original_contracts,
            "closed_contracts": closed_contracts,
            "realized_close_cost": realized_close_cost,
            "time_remaining_to_expiration": time_remaining["seconds"],
            "time_remaining_display": time_remaining["display"],
            "entry_structure_grade": self._coerce_text(trade.get("structure_grade"), fallback="Not available"),
            "entry_macro_grade": self._coerce_text(trade.get("macro_grade"), fallback="Not available"),
            "entry_vix": self._coerce_float(trade.get("vix_at_entry") or trade.get("vix_entry")),
            "expected_move_used": entry_expected_move,
            "actual_em_multiple": self._coerce_float(trade.get("actual_em_multiple")),
            "short_delta": self._coerce_float(trade.get("short_delta")),
            "fallback_used": self._coerce_text(trade.get("fallback_used"), fallback="no"),
            "fallback_rule_name": self._coerce_text(trade.get("fallback_rule_name"), fallback="—"),
            "notes_entry": self._coerce_text(trade.get("notes_entry"), fallback="—"),
            "original_buffer": original_buffer,
            "invalid_strike_order": distance_snapshot["invalid_strike_order"],
            "distance_warning": distance_snapshot["distance_warning"],
        }

    def _classify_apollo_trade(self, trade: Dict[str, Any], metrics: Dict[str, Any], apollo_context: Dict[str, Any], now: datetime) -> Dict[str, Any]:
        distance_to_short = float(metrics.get("distance_to_short") or 0.0)
        distance_to_long = float(metrics.get("distance_to_long") or 0.0)
        short_breach = self._is_strike_breached(distance_to_short)
        long_breach = self._is_long_stop_breached(distance_to_short=distance_to_short, distance_to_long=distance_to_long)
        current_structure = self._coerce_text(apollo_context.get("current_structure_grade"), fallback="Not used for status")
        current_live_expected_move = self._coerce_float(metrics.get("current_live_expected_move"))
        em_multiple = self._safe_divide(distance_to_short, current_live_expected_move)

        if long_breach or short_breach or em_multiple < 1.0:
            status = "Exit Now"
            status_reason_code = "apollo-em-exit-now"
            reason = "Apollo spread has breached its short or long stop and should be fully exited."
        elif em_multiple < 1.2:
            status = "Exit Partial"
            status_reason_code = "apollo-em-exit-partial"
            reason = f"Apollo spread is down to {em_multiple:.2f}x current live expected-move clearance and needs a partial exit."
        elif em_multiple >= self.APOLLO_HEALTHY_EM_THRESHOLD:
            status = "Healthy"
            status_reason_code = "apollo-em-healthy"
            reason = f"Apollo spread retains {em_multiple:.2f}x current live expected-move clearance to the short strike."
        elif em_multiple >= self.APOLLO_WATCH_EM_THRESHOLD:
            status = "Watch"
            status_reason_code = "apollo-em-watch"
            reason = f"Apollo spread has compressed to {em_multiple:.2f}x current live expected-move clearance and should be watched."
        else:
            status = "Exit Approaching"
            status_reason_code = "apollo-em-exit-approaching"
            if current_live_expected_move is None or current_live_expected_move <= 0:
                reason = "Apollo has exhausted its current live expected-move cushion and is approaching exit territory."
            else:
                reason = f"Apollo spread is down to {em_multiple:.2f}x current live expected-move clearance and is approaching exit territory."

        action_plan = self._build_action_plan(status, int(metrics.get("remaining_contracts") or 0), metrics)
        next_trigger, next_trigger_action = self._build_price_ladder_next_trigger(trade, metrics)
        exit_plan_summary = self._build_exit_plan_summary(trade, metrics)
        return {
            "status": status,
            "thesis_status": "neutral",
            "reason": reason,
            "status_reason_code": status_reason_code,
            "thesis_reason_code": "apollo-thesis-neutral",
            "next_trigger": next_trigger,
            "exit_plan_summary": exit_plan_summary,
            "exit_plan_gates": self._build_exit_plan_gates(trade, metrics),
            "trigger_source": next_trigger_action,
            "current_structure_grade": current_structure,
            "status_em_multiple": em_multiple,
            "structure_trigger_fired": False,
            "vwap_trigger_fired": False,
            "short_proximity_trigger_fired": bool(status != "Healthy" or short_breach),
            "long_proximity_trigger_fired": long_breach,
            "final_stop_trigger_fired": bool(short_breach or long_breach),
            **action_plan,
        }

    def _classify_talos_trade(self, trade: Dict[str, Any], metrics: Dict[str, Any]) -> Dict[str, Any]:
        distance_to_short = float(metrics.get("distance_to_short") or 0.0)
        distance_to_long = float(metrics.get("distance_to_long") or 0.0)
        current_structure = self._coerce_text(resolve_trade_system_name(trade), fallback="Talos")
        current_live_expected_move = self._coerce_float(metrics.get("current_live_expected_move"))
        em_multiple = self._safe_divide(distance_to_short, current_live_expected_move)
        short_watch_zone = em_multiple < self.TALOS_HEALTHY_EM_THRESHOLD
        short_hard_stop = self._is_strike_breached(distance_to_short)
        long_breach = self._is_long_stop_breached(distance_to_short=distance_to_short, distance_to_long=distance_to_long)
        if long_breach or short_hard_stop or em_multiple < 1.0:
            status = "Exit Now"
            status_reason_code = "talos-em-exit-now"
            reason = "Talos spread has breached its short or long stop and should be fully exited."
        elif em_multiple < 1.2:
            status = "Exit Partial"
            status_reason_code = "talos-em-exit-partial"
            reason = f"Talos spread is down to {em_multiple:.2f}x current same-day expected-move clearance and needs a partial exit."
        elif em_multiple >= self.TALOS_HEALTHY_EM_THRESHOLD:
            status = "Healthy"
            status_reason_code = "talos-em-healthy"
            reason = f"Talos spread retains {em_multiple:.2f}x current same-day expected-move clearance to the short strike."
        elif em_multiple >= self.TALOS_WATCH_EM_THRESHOLD:
            status = "Watch"
            status_reason_code = "talos-em-watch"
            reason = f"Talos spread has compressed to {em_multiple:.2f}x current same-day expected-move clearance and should be watched."
        else:
            status = "Exit Approaching"
            status_reason_code = "talos-em-exit-approaching"
            reason = f"Talos spread is down to {em_multiple:.2f}x current same-day expected-move clearance and is approaching exit territory."

        action_plan = self._build_action_plan(status, int(metrics.get("remaining_contracts") or 0), metrics)
        next_trigger, next_trigger_action = self._build_price_ladder_next_trigger(trade, metrics)
        exit_plan_summary = self._build_exit_plan_summary(trade, metrics)
        return {
            "status": status,
            "thesis_status": "managed",
            "reason": reason,
            "status_reason_code": status_reason_code,
            "thesis_reason_code": "talos-governed",
            "next_trigger": next_trigger,
            "exit_plan_summary": exit_plan_summary,
            "exit_plan_gates": self._build_exit_plan_gates(trade, metrics),
            "trigger_source": next_trigger_action,
            "current_structure_grade": current_structure,
            "status_em_multiple": em_multiple,
            "structure_trigger_fired": False,
            "vwap_trigger_fired": False,
            "short_proximity_trigger_fired": short_watch_zone,
            "long_proximity_trigger_fired": long_breach,
            "final_stop_trigger_fired": bool(short_hard_stop or long_breach),
            **action_plan,
        }

    def _process_daily_notifications(
        self,
        records: List[Dict[str, Any]],
        now: datetime,
        runtime_settings: Dict[str, Any],
        global_notification_map: Dict[str, Dict[str, Any]],
    ) -> list[Dict[str, Any]]:
        outcomes: list[Dict[str, Any]] = []
        if bool((global_notification_map.get(GLOBAL_NOTIFICATION_DAILY_START) or {}).get("enabled", True)):
            morning_outcome = self._send_morning_snapshot_if_due(records, now, runtime_settings)
            if morning_outcome is not None:
                outcomes.append(morning_outcome)
        if bool((global_notification_map.get(GLOBAL_NOTIFICATION_DAILY_END) or {}).get("enabled", True)):
            eod_outcome = self._send_eod_summary_if_due(records, now, runtime_settings)
            if eod_outcome is not None:
                outcomes.append(eod_outcome)
        return outcomes

    def _process_trade_notifications(self, trade: Dict[str, Any], record: Dict[str, Any], previous_state: Dict[str, Any], now: datetime) -> Dict[str, Any]:
        sent_count = 0
        last_priority = None
        last_alert_type = None
        sent_at = None

        status_change = self._build_status_change_alert(trade, record)
        if status_change is not None:
            outcome = self._send_management_alert(trade, record, status_change, now)
            if not outcome.get("sent") and outcome.get("error"):
                return outcome
            if outcome.get("sent"):
                sent_count += 1
                last_priority = outcome.get("priority")
                last_alert_type = outcome.get("alert_type")
                sent_at = outcome.get("sent_at")

        action_alert = self._build_action_alert(trade, record)
        if action_alert is not None:
            outcome = self._send_management_alert(trade, record, action_alert, now)
            if not outcome.get("sent") and outcome.get("error"):
                return outcome
            if outcome.get("sent"):
                sent_count += 1
                last_priority = outcome.get("priority")
                last_alert_type = outcome.get("alert_type")
                sent_at = outcome.get("sent_at")

        self._update_trade_notification_state(
            trade_id=int(trade.get("id") or 0),
            last_status=str(record.get("status") or ""),
            last_action_sent=str(record.get("action_type") or ""),
            last_alert_timestamp=sent_at,
        )
        return {
            "sent": sent_count > 0,
            "sent_count": sent_count,
            "error": "",
            "priority": last_priority,
            "alert_type": last_alert_type,
            "sent_at": sent_at,
        }

    def _upsert_management_state(self, record: Dict[str, Any], previous_state: Dict[str, Any], alert_outcome: Dict[str, Any], now: datetime) -> None:
        self.state_repository.upsert_management_state(record, previous_state, alert_outcome, now)

    def _load_open_trades(self) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for trade_mode in ("real", "simulated", "talos"):
            rows.extend(
                trade
                for trade in self.trade_store.list_trades(trade_mode)
                if str(trade.get("derived_status_raw") or trade.get("status") or "").strip().lower() in {"open", "reduced"}
                and is_retained_trade_system(trade)
            )
        return rows

    def _load_trade_notifications(self, trades: List[Dict[str, Any]]) -> Dict[int, List[Dict[str, Any]]]:
        if self.trade_notification_repository is None:
            return {int(trade.get("id") or 0): normalize_trade_notifications([]) for trade in trades if int(trade.get("id") or 0) > 0}
        trade_ids = [int(trade.get("id") or 0) for trade in trades if int(trade.get("id") or 0) > 0]
        return self.trade_notification_repository.load_notifications_for_trade_ids(trade_ids)

    def _load_runtime_settings(self) -> Dict[str, Any]:
        return self.state_repository.load_runtime_settings()

    def _load_trade(self, trade_id: int) -> Dict[str, Any]:
        return self.state_repository.load_trade(trade_id)

    def _load_management_states(self) -> Dict[int, Dict[str, Any]]:
        return self.state_repository.load_management_states()

    def _build_alert_state_payload(self, state: Dict[str, Any], trade: Dict[str, Any]) -> Dict[str, Any]:
        last_alert_sent_at = parse_datetime_value(state.get("last_alert_sent_at")) if state else None
        last_alert_priority = state.get("last_alert_priority") if state else None
        return {
            "last_alert_sent_at": self._format_datetime(last_alert_sent_at),
            "last_alert_type": self._coerce_text((state or {}).get("last_alert_type"), fallback="—"),
            "last_alert_priority": "High" if last_alert_priority == 1 else ("Normal" if last_alert_priority == 0 else "—"),
            "alert_count": int((state or {}).get("alert_count") or 0),
            "last_status": self._coerce_text((trade or {}).get("last_status"), fallback="—"),
            "last_action_sent": self._coerce_text((trade or {}).get("last_action_sent"), fallback="—"),
            "last_trade_alert_timestamp": self._format_datetime(parse_datetime_value((trade or {}).get("last_alert_timestamp"))),
        }

    def _build_status_counts(self, records: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        counts: Dict[str, int] = {label: 0 for label in ("Healthy", "Watch", "Exit Approaching", "Exit Partial", "Exit Now")}
        for record in records:
            label = str(record.get("status") or "Healthy")
            if label in counts:
                counts[label] += 1
        return [{"label": label, "count": counts[label], "key": self._slugify(label)} for label in counts]

    def _build_price_ladder_next_trigger(self, trade: Dict[str, Any], metrics: Dict[str, Any]) -> tuple[str, str]:
        original_contracts = max(int(metrics.get("original_contracts") or 0), 0)
        remaining_contracts = max(int(metrics.get("remaining_contracts") or 0), 0)
        current_spx = self._coerce_float(metrics.get("current_underlying_price"))
        short_strike = self._coerce_float(trade.get("short_strike"), fallback=0.0) or 0.0
        long_strike = self._coerce_float(trade.get("long_strike"), fallback=0.0) or 0.0
        option_type = self._coerce_text(trade.get("option_type"), fallback="Put Credit Spread")

        if original_contracts <= 0 or remaining_contracts <= 0 or current_spx is None or short_strike <= 0 or long_strike <= 0:
            return ("No open-contract trigger ladder is available.", "price-ladder-unavailable")

        is_call = "call" in option_type.lower()
        cumulative_targets = self._build_cumulative_exit_targets(original_contracts)
        triggered_steps = self._build_trigger_steps(short_strike=short_strike, long_strike=long_strike, is_call=is_call, cumulative_targets=cumulative_targets)
        actual_closed_contracts = max(original_contracts - remaining_contracts, 0)

        last_triggered_step: Dict[str, Any] | None = None
        for step in triggered_steps:
            if self._is_trigger_crossed(current_spx, float(step["trigger_level"]), is_call=is_call):
                last_triggered_step = step
            else:
                break

        if last_triggered_step is not None:
            catch_up_contracts = max(int(last_triggered_step["required_closed_contracts"]) - actual_closed_contracts, 0)
            if catch_up_contracts > 0:
                return (
                    f"Close Now: {catch_up_contracts} contracts. SPX has already crossed {last_triggered_step['label']} ({last_triggered_step['comparison']} {self._format_number(last_triggered_step['trigger_level'])}).",
                    "price-ladder-catch-up",
                )

        for step in triggered_steps:
            if int(step["incremental_contracts"]) <= 0:
                continue
            if self._is_trigger_crossed(current_spx, float(step["trigger_level"]), is_call=is_call):
                continue
            return (
                f"{step['label']}: close {step['incremental_contracts']} contracts at SPX {step['comparison']} {self._format_number(step['trigger_level'])}.",
                "price-ladder-next",
            )

        return ("No future price trigger remains. Close the balance on the next manual risk signal.", "price-ladder-complete")

    def _build_exit_plan_summary(self, trade: Dict[str, Any], metrics: Dict[str, Any]) -> str:
        original_contracts = max(int(metrics.get("original_contracts") or 0), 0)
        remaining_contracts = max(int(metrics.get("remaining_contracts") or 0), 0)
        short_strike = self._coerce_float(trade.get("short_strike"), fallback=0.0) or 0.0
        long_strike = self._coerce_float(trade.get("long_strike"), fallback=0.0) or 0.0
        option_type = self._coerce_text(trade.get("option_type"), fallback="Put Credit Spread")
        if original_contracts <= 0 or remaining_contracts <= 0 or short_strike <= 0 or long_strike <= 0:
            return "No exit plan is available."

        is_call = "call" in option_type.lower()
        cumulative_targets = self._build_cumulative_exit_targets(original_contracts)
        triggered_steps = self._build_trigger_steps(
            short_strike=short_strike,
            long_strike=long_strike,
            is_call=is_call,
            cumulative_targets=cumulative_targets,
        )
        actual_closed_contracts = max(original_contracts - remaining_contracts, 0)
        future_steps = [step for step in triggered_steps if int(step["required_closed_contracts"]) > actual_closed_contracts and int(step["incremental_contracts"]) > 0]
        if not future_steps:
            return "No future trigger remains. Close the balance on the next manual risk signal."

        next_step = future_steps[0]
        detail = f"Next: {next_step['label']} close {next_step['incremental_contracts']} at SPX {next_step['comparison']} {self._format_number(next_step['trigger_level'])}."
        later_steps = future_steps[1:]
        if later_steps:
            detail = f"{detail} Later: {'; '.join(f"{step['label']} close {step['incremental_contracts']}" for step in later_steps)}."
        return detail

    def _build_exit_plan_gates(self, trade: Dict[str, Any], metrics: Dict[str, Any]) -> list[Dict[str, Any]]:
        original_contracts = max(int(metrics.get("original_contracts") or 0), 0)
        short_strike = self._coerce_float(trade.get("short_strike"), fallback=0.0) or 0.0
        long_strike = self._coerce_float(trade.get("long_strike"), fallback=0.0) or 0.0
        option_type = self._coerce_text(trade.get("option_type"), fallback="Put Credit Spread")
        current_spx = self._coerce_float(metrics.get("current_underlying_price"))
        remaining_contracts = max(int(metrics.get("remaining_contracts") or 0), 0)
        if original_contracts <= 0 or short_strike <= 0 or long_strike <= 0:
            return []

        is_call = "call" in option_type.lower()
        cumulative_targets = self._build_cumulative_exit_targets(original_contracts)
        triggered_steps = self._build_trigger_steps(
            short_strike=short_strike,
            long_strike=long_strike,
            is_call=is_call,
            cumulative_targets=cumulative_targets,
        )
        actual_closed_contracts = max(original_contracts - remaining_contracts, 0)
        gates: list[Dict[str, Any]] = []
        for step in triggered_steps:
            incremental_contracts = int(step["incremental_contracts"])
            executed = incremental_contracts > 0 and int(step["required_closed_contracts"]) <= actual_closed_contracts
            due_now = False
            if not executed and current_spx is not None:
                due_now = self._is_trigger_crossed(current_spx, float(step["trigger_level"]), is_call=is_call)
            gates.append(
                {
                    "label": step["label"],
                    "trigger_value": float(step["trigger_level"]),
                    "trigger_value_display": self._format_number(step["trigger_level"]),
                    "comparison": step["comparison"],
                    "quantity_to_close": incremental_contracts,
                    "quantity_to_close_display": str(incremental_contracts),
                    "executed": executed,
                    "due_now": due_now,
                    "state_key": "executed" if executed else ("due" if due_now else "pending"),
                    "state_label": "Executed" if executed else ("Due Now" if due_now else "Pending"),
                }
            )
        return gates

    @staticmethod
    def _build_cumulative_exit_targets(original_contracts: int) -> list[int]:
        if original_contracts <= 0:
            return [0, 0, 0, 0]
        ratios = (0.25, 0.50, 0.75, 1.0)
        targets: list[int] = []
        for ratio in ratios:
            targets.append(min(original_contracts, max(targets[-1] if targets else 0, math.ceil(original_contracts * ratio))))
        return targets

    def _build_trigger_steps(self, *, short_strike: float, long_strike: float, is_call: bool, cumulative_targets: list[int]) -> list[Dict[str, Any]]:
        comparison = ">=" if is_call else "<="
        levels = [
            short_strike - 30.0 if is_call else short_strike + 30.0,
            short_strike - 15.0 if is_call else short_strike + 15.0,
            short_strike,
            long_strike,
        ]
        labels = [
            "30-point short-strike proximity",
            "15-point short-strike proximity",
            "short-strike breach",
            "long-strike touch",
        ]
        steps: list[Dict[str, Any]] = []
        prior_target = 0
        for index, required_closed_contracts in enumerate(cumulative_targets):
            incremental_contracts = max(required_closed_contracts - prior_target, 0)
            prior_target = required_closed_contracts
            steps.append(
                {
                    "label": labels[index],
                    "trigger_level": levels[index],
                    "required_closed_contracts": required_closed_contracts,
                    "incremental_contracts": incremental_contracts,
                    "comparison": comparison,
                }
            )
        return steps

    @staticmethod
    def _is_trigger_crossed(current_spx: float, trigger_level: float, *, is_call: bool) -> bool:
        return current_spx >= trigger_level if is_call else current_spx <= trigger_level

    def _resolve_current_spread_mark(self, trade: Dict[str, Any], shared_context: Dict[str, Any]) -> float | None:
        expiration = self._coerce_trade_expiration(trade)
        if expiration is None:
            return None
        chain_summary = (shared_context.get("option_chains") or {}).get(expiration.isoformat()) or {}
        if not chain_summary.get("success"):
            return None
        option_type = self._coerce_text(trade.get("option_type"), fallback="Put Credit Spread")
        prepared_chain = (shared_context.get("prepared_option_chains") or {}).get(expiration.isoformat()) or {}
        strike_lookup = prepared_chain.get("calls_by_strike") if "call" in option_type.lower() else prepared_chain.get("puts_by_strike")
        if not isinstance(strike_lookup, dict):
            contracts = chain_summary.get("calls") if "call" in option_type.lower() else chain_summary.get("puts")
            if not isinstance(contracts, list):
                return None
            strike_lookup = {round(float(item.get("strike") or 0.0), 2): item for item in contracts if self._coerce_float(item.get("strike")) is not None}
        short_leg = strike_lookup.get(round(float(trade.get("short_strike") or 0.0), 2))
        long_leg = strike_lookup.get(round(float(trade.get("long_strike") or 0.0), 2))
        if not short_leg or not long_leg:
            return None
        short_mark = self._option_executable_price(short_leg, side="buy")
        long_mark = self._option_executable_price(long_leg, side="sell")
        if short_mark is None or long_mark is None:
            return None
        return round(max(short_mark - long_mark, 0.0), 2)

    def _option_executable_price(self, contract: Dict[str, Any], *, side: str) -> float | None:
        if side == "buy":
            ask = to_float(contract.get("ask"))
            if ask is not None:
                return ask
        if side == "sell":
            bid = to_float(contract.get("bid"))
            if bid is not None:
                return bid
        return self._option_mark(contract)

    def _estimate_live_expected_move_from_chain(
        self,
        *,
        chain_summary: Dict[str, Any] | None,
        prepared_chain: Dict[str, Any] | None = None,
        spot: float,
        system_name: str,
        expiration_date: date | None,
        evaluation_date: date,
    ) -> Dict[str, Any]:
        if not chain_summary or not chain_summary.get("success"):
            return {
                "expected_move": None,
                "source_label": "Unavailable",
                "formula_label": "Live option-chain expected move unavailable",
                "contracts_label": "—",
            }

        normalized_system_name = resolve_trade_system_name({"system_name": system_name})
        if normalized_system_name == "Talos":
            system_key = "talos"
        else:
            system_key = "apollo"
        prepared_expected_moves = (prepared_chain or {}).get("expected_move_by_system") or {}
        cached_details = prepared_expected_moves.get(system_key)
        if isinstance(cached_details, dict) and cached_details:
            return dict(cached_details)

        normalized_puts = (prepared_chain or {}).get("normalized_puts") or self._normalize_live_contracts(chain_summary.get("puts") or [])
        normalized_calls = (prepared_chain or {}).get("normalized_calls") or self._normalize_live_contracts(chain_summary.get("calls") or [])
        return self._build_expected_move_details_from_normalized(
            normalized_puts=normalized_puts,
            normalized_calls=normalized_calls,
            spot=spot,
            system_name=system_name,
            expiration_date=expiration_date,
            evaluation_date=evaluation_date,
        )

    def _build_expected_move_details_from_normalized(
        self,
        *,
        normalized_puts: List[Dict[str, Any]],
        normalized_calls: List[Dict[str, Any]],
        spot: float,
        system_name: str,
        expiration_date: date | None,
        evaluation_date: date,
    ) -> Dict[str, Any]:
        selected_put = self._find_live_contract_at_or_below_strike(normalized_puts, spot) or self._find_nearest_live_contract(normalized_puts, spot)
        selected_call = self._find_live_contract_at_or_above_strike(normalized_calls, spot) or self._find_nearest_live_contract(normalized_calls, spot)
        put_anchor = self._option_mark(selected_put or {}) if selected_put is not None else 0.0
        call_anchor = self._option_mark(selected_call or {}) if selected_call is not None else 0.0

        expected_move = 0.0
        if put_anchor and call_anchor:
            expected_move = put_anchor + call_anchor
            contracts_label = f"{int(selected_put['strike'])}P {put_anchor:.2f} + {int(selected_call['strike'])}C {call_anchor:.2f}"
            formula_label = "ATM put premium + ATM call premium using midpoint when bid/ask are both available, else mark/bid/ask/last"
        elif put_anchor:
            expected_move = put_anchor * 2.0
            contracts_label = f"2 × {int(selected_put['strike'])}P {put_anchor:.2f}"
            formula_label = "2 × ATM put premium using midpoint when bid/ask are both available, else mark/bid/ask/last"
        elif call_anchor:
            expected_move = call_anchor * 2.0
            contracts_label = f"2 × {int(selected_call['strike'])}C {call_anchor:.2f}"
            formula_label = "2 × ATM call premium using midpoint when bid/ask are both available, else mark/bid/ask/last"
        else:
            contracts_label = "—"
            formula_label = "Live option-chain expected move unavailable"

        source_label = "Current trade-horizon ATM straddle"
        if expiration_date is not None and expiration_date == evaluation_date:
            source_label = "Same-day ATM straddle"
        if system_name == "Talos":
            source_label = "Current same-day ATM straddle"

        return {
            "expected_move": round(expected_move, 2) if expected_move > 0 else None,
            "source_label": source_label if expected_move > 0 else "Unavailable",
            "formula_label": formula_label,
            "contracts_label": contracts_label,
        }

    def _normalize_live_contracts(self, contracts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        normalized: List[Dict[str, Any]] = []
        for item in contracts:
            strike = self._coerce_float(item.get("strike"))
            if strike is None:
                continue
            normalized.append({**item, "strike": strike})
        return sorted(normalized, key=lambda item: item["strike"])

    @staticmethod
    def _find_live_contract_at_or_below_strike(contracts: List[Dict[str, Any]], strike: float) -> Dict[str, Any] | None:
        eligible = [item for item in contracts if float(item.get("strike") or 0.0) <= strike]
        return eligible[-1] if eligible else None

    @staticmethod
    def _find_live_contract_at_or_above_strike(contracts: List[Dict[str, Any]], strike: float) -> Dict[str, Any] | None:
        eligible = [item for item in contracts if float(item.get("strike") or 0.0) >= strike]
        return eligible[0] if eligible else None

    @staticmethod
    def _find_nearest_live_contract(contracts: List[Dict[str, Any]], strike: float) -> Dict[str, Any] | None:
        if not contracts:
            return None
        return min(contracts, key=lambda item: abs(float(item.get("strike") or 0.0) - strike))

    @staticmethod
    def _option_mark(contract: Dict[str, Any]) -> float | None:
        mark = to_float(contract.get("mark"))
        if mark is not None:
            return mark
        bid = to_float(contract.get("bid"))
        ask = to_float(contract.get("ask"))
        if bid is not None and ask is not None:
            return round((bid + ask) / 2.0, 2)
        return to_float(contract.get("last"))

    @staticmethod
    def _resolve_original_buffer(trade: Dict[str, Any], *, entry_underlying: float, short_strike: float, is_call: bool) -> float | None:
        stored = to_float(trade.get("actual_distance_to_short") or trade.get("distance_to_short"))
        if stored not in {None, 0}:
            return abs(float(stored))
        if entry_underlying in {None, 0} or short_strike in {None, 0}:
            return None
        return abs((short_strike - entry_underlying) if is_call else (entry_underlying - short_strike))

    @staticmethod
    def _compute_distance(current_price: float, *, strike: float, is_call: bool) -> float:
        if is_call:
            return round(strike - current_price, 2)
        return round(current_price - strike, 2)

    def _build_distance_snapshot(self, *, current_price: float, short_strike: float, long_strike: float, option_type: str) -> Dict[str, Any]:
        is_call = "call" in option_type.lower()
        distance_to_short = self._compute_distance(current_price, strike=short_strike, is_call=is_call)
        distance_to_long = self._compute_distance(current_price, strike=long_strike, is_call=is_call)
        invalid_strike_order = False
        distance_warning = ""
        if short_strike and long_strike:
            if is_call:
                invalid_strike_order = long_strike <= short_strike
                if invalid_strike_order:
                    distance_warning = "Stored call-spread strikes are inverted; alerts ignore long-stop breaches unless the short strike is also breached."
            else:
                invalid_strike_order = long_strike >= short_strike
                if invalid_strike_order:
                    distance_warning = "Stored put-spread strikes are inverted; alerts ignore long-stop breaches unless the short strike is also breached."
        return {
            "distance_to_short": distance_to_short,
            "distance_to_long": distance_to_long,
            "invalid_strike_order": invalid_strike_order,
            "distance_warning": distance_warning,
        }

    @staticmethod
    def _is_strike_breached(distance_to_strike: float) -> bool:
        return distance_to_strike <= 0

    def _is_long_stop_breached(self, *, distance_to_short: float, distance_to_long: float) -> bool:
        return self._is_strike_breached(distance_to_long) and self._is_strike_breached(distance_to_short)

    @staticmethod
    def _build_time_remaining(expiration_date: date | None, now: datetime) -> Dict[str, Any]:
        if expiration_date is None:
            return {"seconds": None, "display": "—"}
        expiration_dt = datetime.combine(expiration_date, time(15, 0), tzinfo=now.tzinfo)
        delta = expiration_dt - now
        seconds = int(delta.total_seconds())
        if seconds <= 0:
            return {"seconds": 0, "display": "Expired"}
        days, remainder = divmod(seconds, 86400)
        hours, remainder = divmod(remainder, 3600)
        minutes = remainder // 60
        return {"seconds": seconds, "display": f"{days}d {hours}h {minutes}m"}

    def _build_action_plan(self, status: str, contracts: int, metrics: Dict[str, Any]) -> Dict[str, Any]:
        contracts = max(int(contracts or 0), 0)
        current_close_price = self._coerce_float(metrics.get("current_close_price"), fallback=0.0) or 0.0
        entry_credit = self._coerce_float(metrics.get("net_credit_per_contract"), fallback=0.0) or 0.0
        max_loss = self._coerce_float(metrics.get("max_loss"), fallback=0.0) or 0.0
        action_type = ""
        contracts_to_close = 0
        if status == "Exit Approaching" and contracts > 0:
            action_type = "Reduce"
            contracts_to_close = 1 if contracts == 1 else max(1, contracts // 2)
        elif status == "Exit Partial" and contracts > 0:
            action_type = "Partial Exit"
            contracts_to_close = max(1, (contracts + 1) // 2)
        elif status == "Exit Now" and contracts > 0:
            action_type = "Full Exit"
            contracts_to_close = contracts

        remaining_contracts = max(contracts - contracts_to_close, 0)
        closed_pl = round((entry_credit - current_close_price) * contracts_to_close * 100.0, 2)
        remaining_unrealized_pl = round((entry_credit - current_close_price) * remaining_contracts * 100.0, 2)
        pl_after_close = round(closed_pl + remaining_unrealized_pl, 2)
        remaining_risk = round(max(max_loss - closed_pl, 0.0), 2)
        action_recommendation = action_type or "Watch"
        critical_alert = bool((self._coerce_float(metrics.get("distance_to_short"), fallback=999.0) or 999.0) < 25.0 or (self._coerce_float(metrics.get("current_live_expected_move")) and self._safe_divide(metrics.get("distance_to_short"), metrics.get("current_live_expected_move")) < 1.2))
        return {
            "action_recommendation": action_recommendation,
            "action_type": action_type,
            "contracts_to_close": contracts_to_close,
            "pl_after_close": pl_after_close,
            "remaining_risk": remaining_risk,
            "critical_alert": critical_alert,
        }

    def _build_status_change_alert(self, trade: Dict[str, Any], record: Dict[str, Any]) -> Dict[str, Any] | None:
        previous_status = str(trade.get("last_status") or "").strip()
        current_status = str(record.get("status") or "").strip()
        if not previous_status or previous_status == current_status:
            return None
        title = "⚠️ STATUS CHANGE"
        message = "\n".join(
            [
                f"{record['system_name']} | {record['profile_label']}",
                f"{previous_status} → {current_status}",
                "",
                f"Dist: {self._format_points(record.get('distance_to_short'))} | EM: {record['current_live_expected_move_display']}",
                f"Credit: {record['percent_credit_captured_display']}",
                f"Next Trigger: {record['next_trigger']}",
            ]
        )
        return {
            "title": title,
            "message": message,
            "priority": 1 if record.get("critical_alert") else 0,
            "alert_type": "status-change",
            "reason_code": record.get("reason_code"),
            "critical": bool(record.get("critical_alert")),
        }

    def _build_action_alert(self, trade: Dict[str, Any], record: Dict[str, Any]) -> Dict[str, Any] | None:
        action_type = str(record.get("action_type") or "").strip()
        if not action_type:
            return None
        previous_action = str(trade.get("last_action_sent") or "").strip()
        if previous_action == action_type:
            return None
        title = "🚨 ACTION REQUIRED"
        message = "\n".join(
            [
                f"{record['system_name']} | {record['profile_label']}",
                f"Status: {record['status']}",
                "",
                f"Dist: {self._format_points(record.get('distance_to_short'))} | EM: {record['current_live_expected_move_display']}",
                "",
                f"Action: Close {record['contracts_to_close_display']} contracts",
                f"P/L Now: {record['current_pl_display']}",
                f"After Close: {record['pl_after_close_display']}",
                "",
                f"Remaining Risk: {record['remaining_risk_display']}",
                "",
                f"Reason: {record['reason']}",
                f"Next Trigger: {record['next_trigger']}",
            ]
        )
        return {
            "title": title,
            "message": message,
            "priority": 1 if record.get("critical_alert") else 0,
            "alert_type": "action-required",
            "reason_code": record.get("reason_code"),
            "critical": bool(record.get("critical_alert")),
        }

    def _alert_profile_label(self, record: Dict[str, Any]) -> str:
        system_name = str(record.get("system_name") or "").strip().lower()
        if system_name == "talos":
            pass_type = self._coerce_text(record.get("pass_type"), fallback="")
            if pass_type:
                return pass_type
        return self._coerce_text(record.get("profile_label"), fallback="Unknown")

    def _send_morning_snapshot_if_due(
        self, records: List[Dict[str, Any]], now: datetime, runtime_settings: Dict[str, Any]
    ) -> Dict[str, Any] | None:
        if not self._is_morning_snapshot_due(now, runtime_settings):
            return None
        if not records:
            self._mark_runtime_setting("last_morning_snapshot_date", now.date().isoformat())
            return {"sent": False, "error": "", "priority": 0, "alert_type": "morning-snapshot", "sent_at": None}
        payload = self._build_open_positions_snapshot_payload(
            records,
            alert_type="morning-snapshot",
            reason_code="morning-snapshot",
        )
        outcome = self._send_management_alert(None, {"trade_id": 0, "trade_mode": "real", "system_name": "Delphi"}, payload, now)
        if outcome.get("sent"):
            self._mark_runtime_setting("last_morning_snapshot_date", now.date().isoformat())
            self._stamp_trade_alert_timestamps([int(item.get("trade_id") or 0) for item in records], outcome.get("sent_at"))
        return outcome

    def _build_open_positions_snapshot_payload(
        self,
        records: List[Dict[str, Any]],
        *,
        alert_type: str,
        reason_code: str,
    ) -> Dict[str, Any]:
        body_blocks = []
        critical = False
        for record in records:
            critical = critical or bool(record.get("critical_alert"))
            body_blocks.append(
                "\n".join(
                    [
                        f"{record['system_name']} | {self._alert_profile_label(record)} | {record['status']}",
                        f"Dist: {self._format_points(record.get('distance_to_short'))} | EM: {record['current_live_expected_move_display']}",
                        f"{record['percent_credit_captured_display']} credit",
                    ]
                )
            )
        return {
            "title": "DELPHI — OPEN POSITIONS",
            "message": "\n\n".join(body_blocks),
            "priority": 1 if critical else 0,
            "alert_type": alert_type,
            "reason_code": reason_code,
            "critical": critical,
        }

    def _send_eod_summary_if_due(
        self, open_records: List[Dict[str, Any]], now: datetime, runtime_settings: Dict[str, Any]
    ) -> Dict[str, Any] | None:
        if not self._is_eod_summary_due(now, runtime_settings):
            return None
        closed_today = self._load_closed_real_trades_for_date(now.date())
        if not closed_today and not open_records:
            self._mark_runtime_setting("last_eod_summary_date", now.date().isoformat())
            return {"sent": False, "error": "", "priority": 0, "alert_type": "eod-summary", "sent_at": None}
        lines = ["Closed Today:"]
        for trade in closed_today:
            lines.append(f"{trade['system_name']} #{trade['trade_number']} → {self._format_currency(trade.get('gross_pnl'))} ({trade.get('win_loss_result') or 'Closed'})")
        if not closed_today:
            lines.append("None")
        lines.append("")
        lines.append("Open (Next Day Risk):")
        critical = False
        for record in open_records:
            critical = critical or bool(record.get("critical_alert"))
            lines.append(f"{record['system_name']} | {self._alert_profile_label(record)}")
            lines.append(f"Dist: {self._format_points(record.get('distance_to_short'))} | EM: {record['current_live_expected_move_display']}")
        if not open_records:
            lines.append("None")
        payload = {
            "title": "DELPHI — EOD SUMMARY",
            "message": "\n".join(lines),
            "priority": 1 if critical else 0,
            "alert_type": "eod-summary",
            "reason_code": "eod-summary",
            "critical": critical,
        }
        if self.state_repository.has_matching_alert(
            alert_type="eod-summary",
            sent_on=now.date().isoformat(),
            title=str(payload["title"]),
            body=str(payload["message"]),
        ):
            self._mark_runtime_setting("last_eod_summary_date", now.date().isoformat())
            return {"sent": False, "error": "", "priority": payload["priority"], "alert_type": "eod-summary", "sent_at": None}
        outcome = self._send_management_alert(None, {"trade_id": 0, "trade_mode": "real", "system_name": "Delphi"}, payload, now)
        if outcome.get("sent"):
            self._mark_runtime_setting("last_eod_summary_date", now.date().isoformat())
            self._stamp_trade_alert_timestamps([int(item.get("trade_id") or 0) for item in open_records], outcome.get("sent_at"))
        return outcome

    def _send_management_alert(
        self, trade: Dict[str, Any] | None, record: Dict[str, Any], payload: Dict[str, Any], now: datetime
    ) -> Dict[str, Any]:
        trade_mode = str(record.get("trade_mode") or (trade or {}).get("trade_mode") or "real").strip().lower()
        if trade_mode != "real":
            return {
                "sent": False,
                "error": "",
                "priority": int(payload.get("priority") or 0),
                "alert_type": payload.get("alert_type"),
                "sent_at": None,
            }
        title = str(payload.get("title") or "").strip()
        message = str(payload.get("message") or "").strip()
        priority = int(payload.get("priority") or 0)
        if payload.get("critical"):
            title = f"🔴 CRITICAL {title}"
        result = self.notification_delivery.send_notification(title=title, message=message, priority=priority)
        if not result.get("ok"):
            return {
                "sent": False,
                "error": str(result.get("error") or "Unable to send Pushover alert."),
                "priority": priority,
                "alert_type": payload.get("alert_type"),
                "sent_at": None,
            }
        sent_at = now.isoformat()
        self.state_repository.record_alert(
            trade_id=int((trade or {}).get("id") or record.get("trade_id") or 0),
            system_name=str(record.get("system_name") or (trade or {}).get("system_name") or "Delphi"),
            trade_mode=str(record.get("trade_mode") or (trade or {}).get("trade_mode") or "real").lower(),
            alert_type=str(payload.get("alert_type") or "management-alert"),
            alert_priority=priority,
            alert_priority_label="High" if priority == 1 else "Normal",
            reason_code=payload.get("reason_code"),
            title=title,
            body=message,
            sent_at=sent_at,
        )
        return {
            "sent": True,
            "error": "",
            "priority": priority,
            "alert_type": payload.get("alert_type"),
            "sent_at": sent_at,
        }

    def _update_trade_notification_state(
        self, *, trade_id: int, last_status: str, last_action_sent: str, last_alert_timestamp: str | None
    ) -> None:
        self.state_repository.update_trade_notification_state(
            trade_id=trade_id,
            last_status=last_status,
            last_action_sent=last_action_sent,
            last_alert_timestamp=last_alert_timestamp,
        )

    def _stamp_trade_alert_timestamps(self, trade_ids: List[int], sent_at: str | None) -> None:
        self.state_repository.stamp_trade_alert_timestamps(trade_ids, sent_at)

    def _mark_runtime_setting(self, column_name: str, value: str) -> None:
        self.state_repository.mark_runtime_setting(column_name, value)

    def _load_closed_real_trades_for_date(self, target_date: date) -> List[Dict[str, Any]]:
        trades = self.trade_store.list_trades("real")
        results = []
        for trade in trades:
            close_events = trade.get("close_events") or []
            if str(trade.get("derived_status_raw") or "").lower() not in {"closed", "expired"}:
                continue
            if not close_events:
                continue
            last_event = close_events[-1]
            event_date = parse_datetime_value(last_event.get("event_datetime") or last_event.get("created_at"))
            if event_date is None or event_date.date() != target_date:
                continue
            results.append(trade)
        return results

    def _is_morning_snapshot_due(self, now: datetime, runtime_settings: Dict[str, Any]) -> bool:
        if not self._is_trading_day(now.date()):
            return False
        if str(runtime_settings.get("last_morning_snapshot_date") or "") == now.date().isoformat():
            return False
        snapshot_time = datetime.combine(now.date(), time(self.MARKET_OPEN_HOUR, self.MARKET_OPEN_MINUTE), tzinfo=self.display_timezone) + timedelta(minutes=self.MORNING_SNAPSHOT_DELAY_MINUTES)
        return now >= snapshot_time

    def _is_eod_summary_due(self, now: datetime, runtime_settings: Dict[str, Any]) -> bool:
        if not self._is_trading_day(now.date()):
            return False
        if str(runtime_settings.get("last_eod_summary_date") or "") == now.date().isoformat():
            return False
        eod_time = datetime.combine(now.date(), time(self.MARKET_CLOSE_HOUR, self.MARKET_CLOSE_MINUTE), tzinfo=self.display_timezone) + timedelta(minutes=self.EOD_SUMMARY_DELAY_MINUTES)
        return now >= eod_time

    def _is_monitor_window(self, now: datetime) -> bool:
        if not self._is_trading_day(now.date()):
            return False
        market_open = datetime.combine(now.date(), time(self.MARKET_OPEN_HOUR, self.MARKET_OPEN_MINUTE), tzinfo=self.display_timezone)
        eod_cutoff = datetime.combine(now.date(), time(self.MARKET_CLOSE_HOUR, self.MARKET_CLOSE_MINUTE), tzinfo=self.display_timezone) + timedelta(minutes=self.EOD_SUMMARY_DELAY_MINUTES + 1)
        return market_open <= now <= eod_cutoff

    def _is_trading_day(self, target_date: date) -> bool:
        return self.market_calendar_service.is_tradable_market_day(target_date)

    @staticmethod
    def _safe_divide(numerator: float | None, denominator: float | None) -> float:
        if numerator is None or denominator in {None, 0}:
            return 0.0
        return round(float(numerator) / float(denominator), 2)

    @staticmethod
    def _derive_session_posture(*, net_change_percent: float, close_vs_vwap_percent: float, recent_change_percent: float) -> str:
        if net_change_percent >= 0.18 and close_vs_vwap_percent > 0.05 and recent_change_percent >= -0.02:
            return "Trend Up Above VWAP"
        if net_change_percent <= -0.18 and close_vs_vwap_percent < -0.05 and recent_change_percent <= 0.02:
            return "Trend Down Below VWAP"
        if abs(close_vs_vwap_percent) <= 0.05:
            return "Rotating Around VWAP"
        return "Mixed / Transition"

    def _safe_snapshot(self, ticker: str, *, query_type: str) -> Dict[str, Any]:
        try:
            return self.market_data_service.get_latest_snapshot(ticker, query_type=query_type)
        except MarketDataError:
            return {"status_unavailable": True}

    def _coerce_trade_expiration(self, trade: Dict[str, Any]) -> date | None:
        return parse_date_value(trade.get("expiration_date"))

    def _now(self) -> datetime:
        value = self.now_provider()
        if value.tzinfo is None:
            return value.replace(tzinfo=self.display_timezone)
        return value.astimezone(self.display_timezone)

    @staticmethod
    def _coerce_text(value: Any, *, fallback: str) -> str:
        text = str(value or "").strip()
        return text or fallback

    @staticmethod
    def _coerce_float(value: Any, fallback: float | None = None) -> float | None:
        parsed = to_float(value)
        return fallback if parsed is None else parsed

    @staticmethod
    def _percent_change(current_value: float, reference_value: float) -> float:
        if reference_value in {None, 0}:
            return 0.0
        return ((current_value - reference_value) / reference_value) * 100.0

    @staticmethod
    def _slugify(value: str) -> str:
        return str(value or "").strip().lower().replace(" ", "-")

    def _format_date(self, value: date | None) -> str:
        return value.isoformat() if value is not None else "—"

    def _format_expiration_label(self, value: date | None) -> str:
        if value is None:
            return "—"
        return value.strftime("%a %Y-%m-%d")

    def _format_datetime(self, value: datetime | None) -> str:
        if value is None:
            return "—"
        return value.astimezone(self.display_timezone).strftime("%Y-%m-%d %I:%M %p %Z").replace(" 0", " ")

    @staticmethod
    def _format_number(value: float | None, decimals: int = 2) -> str:
        if value is None:
            return "—"
        return f"{float(value):,.{decimals}f}"

    def _format_points(self, value: float | None) -> str:
        if value is None:
            return "—"
        return f"{float(value):,.1f} pts"

    def _format_distance_to_strike(self, value: float | None) -> str:
        if value is None:
            return "—"
        if float(value) >= 0:
            return f"{abs(float(value)):,.1f} pts away"
        return f"Breached by {abs(float(value)):,.1f} pts"

    @staticmethod
    def _format_percent(value: float | None) -> str:
        if value is None:
            return "—"
        return f"{float(value):,.1f}%"

    @staticmethod
    def _format_signed_percent(value: float | None) -> str:
        if value is None:
            return "—"
        return f"{float(value):+,.1f}%"

    @staticmethod
    def _format_currency(value: float | None) -> str:
        if value is None:
            return "—"
        sign = "-" if value < 0 else ""
        return f"{sign}${abs(float(value)):,.2f}"