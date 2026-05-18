from __future__ import annotations

import copy
import hashlib
import json
from datetime import UTC, datetime, timedelta
from threading import RLock
from typing import Any, Dict

from config import AppConfig, get_app_config
from services.schwab_trading_auth_service import SchwabTradingAuthService
from services.talos_execution_order_service import TalosExecutionOrderService


class TalosExecutionService:
    EXECUTION_STATE_PREVIEW = "PREVIEW"
    EXECUTION_STATE_PENDING = "PENDING"
    EXECUTION_STATE_SUBMITTED = "SUBMITTED"
    EXECUTION_STATE_PARTIALLY_FILLED = "PARTIALLY_FILLED"
    EXECUTION_STATE_FILLED = "FILLED"
    EXECUTION_STATE_FAILED = "FAILED"
    EXECUTION_STATE_ROLLBACK_REQUIRED = "ROLLBACK_REQUIRED"
    EXECUTION_STATE_BLOCKED = "BLOCKED"

    def __init__(
        self,
        *,
        execution_auth_service: SchwabTradingAuthService,
        gateway: TalosExecutionOrderService | None = None,
        config: AppConfig | None = None,
    ) -> None:
        self.config = config or get_app_config()
        self.execution_auth_service = execution_auth_service
        self.gateway = gateway or TalosExecutionOrderService(execution_auth_service=execution_auth_service, config=self.config)
        self.CONFIRMATION_TEXT = self.gateway.CONFIRMATION_TEXT
        self._lock = RLock()
        self._active_routing: dict[str, str] = {}
        self._recent_routing: dict[str, datetime] = {}
        self._last_status: dict[str, Dict[str, Any]] = {
            "open": self._build_status_snapshot(action="open", state=self.EXECUTION_STATE_PREVIEW, message="Awaiting preview."),
            "close": self._build_status_snapshot(action="close", state=self.EXECUTION_STATE_PREVIEW, message="Awaiting preview."),
        }
        self._critical_message = ""

    def resolve_account_context(self) -> Dict[str, Any]:
        context = dict(self.gateway.resolve_account_context())
        configured_account = str(self.config.talos_execution_account or "").strip()
        account_name = str(self.config.talos_execution_account_name or "").strip()
        if configured_account:
            context["account_hash"] = configured_account
            context["account_number"] = str(context.get("account_number") or configured_account)
            context["source"] = "Talos execution config"
        context["account_name"] = account_name or "Talos execution"
        context["execution_enabled"] = bool(self.config.talos_execution_enabled)
        context["configured"] = bool(configured_account)
        return context

    def build_open_order_preview(
        self,
        *,
        candidate_payload: Dict[str, Any],
        sizing_payload: Dict[str, Any],
        account_context: Dict[str, Any],
    ) -> Dict[str, Any]:
        preview = dict(
            self.gateway.build_open_order_preview(
                candidate_payload=candidate_payload,
                sizing_payload=sizing_payload,
                account_context=account_context,
            )
        )
        preview.update(
            self._enrich_preview(
                action="open",
                preview=preview,
                sizing_payload=sizing_payload,
                account_context=account_context,
            )
        )
        return preview

    def build_close_order_preview(
        self,
        *,
        trade: Dict[str, Any],
        record: Dict[str, Any],
        account_context: Dict[str, Any],
        manual_override: bool,
    ) -> Dict[str, Any]:
        preview = dict(
            self.gateway.build_close_order_preview(
                trade=trade,
                record=record,
                account_context=account_context,
                manual_override=manual_override,
            )
        )
        preview.update(
            self._enrich_preview(
                action="close",
                preview=preview,
                sizing_payload={},
                account_context=account_context,
            )
        )
        return preview

    def submit_open_order(self, preview: Dict[str, Any]) -> Dict[str, Any]:
        candidate_hash = str(preview.get("candidate_hash") or self._build_candidate_hash("open", preview))
        block_message = self._begin_submission(action="open", candidate_hash=candidate_hash)
        if block_message:
            return self._blocked_submission(action="open", preview=preview, message=block_message)
        try:
            self._set_status("open", self.EXECUTION_STATE_PENDING, "Execution lock acquired. Preparing long-first routing.", candidate_hash)
            validation_errors = list(preview.get("validation_errors") or [])
            if validation_errors:
                return self._blocked_submission(action="open", preview=preview, message=str(validation_errors[0]))

            long_fill = self.gateway._submit_single_leg_order(
                preview=preview,
                symbol=str(preview.get("long_option_symbol") or ""),
                instruction="BUY_TO_OPEN",
                price=self.gateway._marketable_limit_price(preview.get("long_ask"), buy_side=True),
                action_label="Talos long-leg open",
            )
            self._set_status(
                "open",
                self.EXECUTION_STATE_PARTIALLY_FILLED,
                "Long leg submitted and accepted. Routing short leg.",
                candidate_hash,
                leg_results={"long_leg": dict(long_fill)},
            )

            short_error: Exception | None = None
            short_fill: Dict[str, Any] | None = None
            for _ in range(2):
                try:
                    short_fill = self.gateway._submit_single_leg_order(
                        preview=preview,
                        symbol=str(preview.get("short_option_symbol") or ""),
                        instruction="SELL_TO_OPEN",
                        price=self.gateway._marketable_limit_price(preview.get("short_bid"), buy_side=False),
                        action_label="Talos short-leg open",
                    )
                    short_error = None
                    break
                except Exception as exc:  # pragma: no cover - defensive retry wrapper
                    short_error = exc
            if short_error is not None or short_fill is None:
                critical_message = (
                    "CRITICAL execution state: long leg was accepted but short leg failed. "
                    "Further Talos execution attempts are halted until manual review."
                )
                self._critical_message = critical_message
                self._set_status(
                    "open",
                    self.EXECUTION_STATE_ROLLBACK_REQUIRED,
                    critical_message,
                    candidate_hash,
                    leg_results={"long_leg": dict(long_fill)},
                )
                return {
                    "ok": False,
                    "message": f"{critical_message} {short_error}",
                    "execution_state": self.EXECUTION_STATE_ROLLBACK_REQUIRED,
                    "preferred_method_used": False,
                    "emergency_state": True,
                    "status_badges": self._status_badges(self.EXECUTION_STATE_ROLLBACK_REQUIRED, ready=False, blocked_message=critical_message),
                }

            self._remember_submission(candidate_hash)
            self._critical_message = ""
            self._set_status(
                "open",
                self.EXECUTION_STATE_FILLED,
                "Long-first vertical routing completed. Spread ready for journaling.",
                candidate_hash,
                leg_results={"long_leg": dict(long_fill), "short_leg": dict(short_fill)},
            )
            return {
                "ok": True,
                "message": "Talos real vertical spread routed long-first and completed successfully.",
                "execution_state": self.EXECUTION_STATE_FILLED,
                "order_id": str(short_fill.get("order_id") or long_fill.get("order_id") or ""),
                "preferred_method_used": False,
                "emergency_state": False,
                "status_badges": self._status_badges(self.EXECUTION_STATE_FILLED, ready=True, blocked_message=""),
            }
        finally:
            self._end_submission(candidate_hash)

    def submit_close_order(self, preview: Dict[str, Any]) -> Dict[str, Any]:
        candidate_hash = str(preview.get("candidate_hash") or self._build_candidate_hash("close", preview))
        block_message = self._begin_submission(action="close", candidate_hash=candidate_hash)
        if block_message:
            return self._blocked_submission(action="close", preview=preview, message=block_message)
        try:
            self._set_status("close", self.EXECUTION_STATE_PENDING, "Preparing close routing.", candidate_hash)
            validation_errors = list(preview.get("validation_errors") or [])
            if validation_errors:
                return self._blocked_submission(action="close", preview=preview, message=str(validation_errors[0]))
            execution_result = self.gateway.submit_close_order(preview)
            if not execution_result.get("ok"):
                self._set_status("close", self.EXECUTION_STATE_FAILED, str(execution_result.get("message") or "Talos close order failed."), candidate_hash)
                return {
                    **execution_result,
                    "execution_state": self.EXECUTION_STATE_FAILED,
                    "status_badges": self._status_badges(self.EXECUTION_STATE_FAILED, ready=False, blocked_message=str(execution_result.get("message") or "")),
                }
            self._remember_submission(candidate_hash)
            self._set_status("close", self.EXECUTION_STATE_FILLED, "Talos close order submitted and accepted.", candidate_hash)
            return {
                **execution_result,
                "execution_state": self.EXECUTION_STATE_FILLED,
                "status_badges": self._status_badges(self.EXECUTION_STATE_FILLED, ready=True, blocked_message=""),
            }
        finally:
            self._end_submission(candidate_hash)

    def get_status_snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "open": copy.deepcopy(self._last_status.get("open") or {}),
                "close": copy.deepcopy(self._last_status.get("close") or {}),
                "critical_message": str(self._critical_message or ""),
            }

    def _enrich_preview(
        self,
        *,
        action: str,
        preview: Dict[str, Any],
        sizing_payload: Dict[str, Any],
        account_context: Dict[str, Any],
    ) -> Dict[str, Any]:
        candidate_hash = self._build_candidate_hash(action, preview)
        duplicate_message = self._duplicate_guard_message(candidate_hash)
        validation_errors: list[str] = []
        if not bool(account_context.get("execution_enabled")):
            validation_errors.append("Execution routing disabled by TALOS_EXECUTION_ENABLED.")
        if not bool(account_context.get("configured")):
            validation_errors.append("Execution account not configured.")
        if not str(preview.get("account_hash") or "").strip():
            validation_errors.append("Execution account identifier unavailable.")
        if int(preview.get("contracts") or 0) <= 0:
            validation_errors.append("Selected contracts must be greater than zero.")
        if duplicate_message:
            validation_errors.append(duplicate_message)
        execution_state = self.EXECUTION_STATE_BLOCKED if validation_errors else self.EXECUTION_STATE_PREVIEW
        return {
            "candidate_hash": candidate_hash,
            "execution_state": execution_state,
            "estimated_exposure_display": str(
                sizing_payload.get("projected_total_black_swan_loss_display")
                or sizing_payload.get("projected_exposure_after_trade_display")
                or "—"
            ),
            "routing_readiness": "Blocked" if validation_errors else "Ready",
            "routing_readiness_detail": validation_errors[0] if validation_errors else "Routing guards satisfied.",
            "validation_errors": validation_errors,
            "account_name": str(account_context.get("account_name") or "Talos execution"),
            "account_configured": bool(account_context.get("configured")),
            "execution_enabled": bool(account_context.get("execution_enabled")),
            "status_badges": self._status_badges(execution_state, ready=not validation_errors, blocked_message=duplicate_message),
        }

    def _blocked_submission(self, *, action: str, preview: Dict[str, Any], message: str) -> Dict[str, Any]:
        candidate_hash = str(preview.get("candidate_hash") or self._build_candidate_hash(action, preview))
        self._set_status(action, self.EXECUTION_STATE_BLOCKED, message, candidate_hash)
        return {
            "ok": False,
            "message": message,
            "execution_state": self.EXECUTION_STATE_BLOCKED,
            "preferred_method_used": False,
            "emergency_state": False,
            "status_badges": self._status_badges(self.EXECUTION_STATE_BLOCKED, ready=False, blocked_message=message),
        }

    def _begin_submission(self, *, action: str, candidate_hash: str) -> str:
        with self._lock:
            duplicate_message = self._duplicate_guard_message(candidate_hash)
            if duplicate_message:
                return duplicate_message
            self._active_routing[candidate_hash] = action
            return ""

    def _end_submission(self, candidate_hash: str) -> None:
        with self._lock:
            self._active_routing.pop(candidate_hash, None)

    def _remember_submission(self, candidate_hash: str) -> None:
        with self._lock:
            self._recent_routing[candidate_hash] = datetime.now(UTC)

    def _duplicate_guard_message(self, candidate_hash: str) -> str:
        now = datetime.now(UTC)
        with self._lock:
            for key, submitted_at in list(self._recent_routing.items()):
                if (now - submitted_at) > timedelta(seconds=max(int(self.config.talos_execution_cooldown_seconds or 45), 1)):
                    self._recent_routing.pop(key, None)
            if candidate_hash in self._active_routing:
                return "Duplicate order blocked: active routing is already in progress for this candidate."
            submitted_at = self._recent_routing.get(candidate_hash)
            if submitted_at is not None:
                return "Duplicate order blocked: cooldown protection is still active for this candidate."
        return ""

    def _set_status(
        self,
        action: str,
        state: str,
        message: str,
        candidate_hash: str,
        *,
        leg_results: Dict[str, Any] | None = None,
    ) -> None:
        with self._lock:
            self._last_status[action] = self._build_status_snapshot(
                action=action,
                state=state,
                message=message,
                candidate_hash=candidate_hash,
                leg_results=leg_results or {},
            )

    def _build_status_snapshot(
        self,
        *,
        action: str,
        state: str,
        message: str,
        candidate_hash: str = "",
        leg_results: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        return {
            "action": action,
            "state": state,
            "message": message,
            "candidate_hash": candidate_hash,
            "updated_at": datetime.now(UTC).isoformat(),
            "leg_results": dict(leg_results or {}),
        }

    def _status_badges(self, state: str, *, ready: bool, blocked_message: str) -> list[Dict[str, str]]:
        badges = [{"label": state.replace("_", " "), "tone": self._state_tone(state)}]
        badges.append({"label": "Ready" if ready else "Blocked", "tone": "success" if ready else "danger"})
        if blocked_message:
            badges.append({"label": "Duplicate Guard", "tone": "warning"})
        return badges

    @staticmethod
    def _state_tone(state: str) -> str:
        if state in {TalosExecutionService.EXECUTION_STATE_FILLED, TalosExecutionService.EXECUTION_STATE_PREVIEW}:
            return "success"
        if state in {TalosExecutionService.EXECUTION_STATE_PENDING, TalosExecutionService.EXECUTION_STATE_SUBMITTED, TalosExecutionService.EXECUTION_STATE_PARTIALLY_FILLED}:
            return "info"
        if state == TalosExecutionService.EXECUTION_STATE_ROLLBACK_REQUIRED:
            return "danger"
        if state == TalosExecutionService.EXECUTION_STATE_BLOCKED:
            return "warning"
        return "muted"

    @staticmethod
    def _build_candidate_hash(action: str, preview: Dict[str, Any]) -> str:
        payload = {
            "action": action,
            "expiration_date": preview.get("expiration_date"),
            "short_strike": preview.get("short_strike"),
            "long_strike": preview.get("long_strike"),
            "contracts": int(preview.get("contracts") or 0),
            "limit_price": str(preview.get("limit_price_display") or preview.get("limit_price") or ""),
        }
        return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()[:16]