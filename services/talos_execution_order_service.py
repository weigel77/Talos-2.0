from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, Optional

import requests

from config import AppConfig, get_app_config
from services.providers.base_provider import ProviderError
from services.schwab_trading_auth_service import SchwabTradingAuthService


class TalosExecutionOrderService:
    CONFIRMATION_TEXT = "ENABLE TALOS REAL ORDER"

    def __init__(self, *, execution_auth_service: SchwabTradingAuthService, config: AppConfig | None = None) -> None:
        self.execution_auth_service = execution_auth_service
        self.config = config or get_app_config()

    def resolve_account_context(self) -> Dict[str, str]:
        try:
            identity = self.execution_auth_service.get_account_identity()
            account_hash = str(identity.get("account_hash") or "").strip()
            account_number = str(identity.get("account_number") or identity.get("account_number_masked") or "").strip()
            if account_hash:
                return {
                    "account_hash": account_hash,
                    "account_number": account_number,
                    "source": "Schwab execution API",
                }
        except Exception:
            pass
        return {
            "account_hash": str(self.config.schwab_trading_account_hash or "").strip(),
            "account_number": str(self.config.schwab_trading_account_number or "").strip(),
            "source": (
                "Environment fallback"
                if (self.config.schwab_trading_account_hash or self.config.schwab_trading_account_number)
                else "Unavailable"
            ),
        }

    def build_open_order_preview(
        self,
        *,
        candidate_payload: Dict[str, Any],
        sizing_payload: Dict[str, Any],
        account_context: Dict[str, str],
    ) -> Dict[str, Any]:
        candidate = dict(candidate_payload.get("raw_candidate") or {})
        short_put = dict(candidate.get("short_put") or {})
        long_put = dict(candidate.get("long_put") or {})
        contracts = max(int(sizing_payload.get("contracts_selected") or 0), 0)
        credit = self._coerce_float(candidate.get("credit") or candidate.get("premium_per_contract")) or 0.0
        preview = {
            "action": "open",
            "profile": "Fortress",
            "mode": "Real",
            "preferred_method": "Complex vertical spread",
            "fallback_method": "Buy the long put first, confirm fill, then sell the short put if Schwab rejects the complex spread.",
            "order_type": "NET_CREDIT_LIMIT",
            "contracts": contracts,
            "expiration_date": str(candidate_payload.get("expiration_date") or ""),
            "short_strike": candidate_payload.get("short_strike"),
            "long_strike": candidate_payload.get("long_strike"),
            "limit_price": round(credit, 2),
            "limit_price_display": f"{credit:.2f}",
            "allow_market_orders": bool(self.config.talos_allow_market_orders),
            "account_hash": str(account_context.get("account_hash") or ""),
            "account_number": str(account_context.get("account_number") or ""),
            "account_source": str(account_context.get("source") or "Unavailable"),
            "confirmation_text_required": self.CONFIRMATION_TEXT,
            "short_option_symbol": str(short_put.get("symbol") or candidate.get("short_symbol") or "").strip(),
            "long_option_symbol": str(long_put.get("symbol") or candidate.get("long_symbol") or "").strip(),
            "short_bid": self._coerce_float(short_put.get("bid")),
            "long_ask": self._coerce_float(long_put.get("ask")),
        }
        preview["signature"] = self._build_signature(preview)
        preview["summary"] = (
            f"Sell {contracts}x {preview.get('short_strike')}P and buy {contracts}x {preview.get('long_strike')}P "
            f"for a {preview.get('limit_price_display')} net credit vertical."
        )
        return preview

    def build_close_order_preview(
        self,
        *,
        trade: Dict[str, Any],
        record: Dict[str, Any],
        account_context: Dict[str, str],
        manual_override: bool,
    ) -> Dict[str, Any]:
        notes_entry = str(trade.get("notes_entry") or "")
        contracts = max(int(record.get("contracts_to_close") or trade.get("remaining_contracts") or trade.get("contracts") or 0), 0)
        limit_price = self._coerce_float(record.get("current_close_price"))
        if limit_price is None:
            limit_price = self._coerce_float(record.get("current_spread_mark")) or 0.0
        preview = {
            "action": "close",
            "profile": "Fortress",
            "mode": "Real",
            "preferred_method": "Complex vertical spread close",
            "fallback_method": "Use protected leg routing only if Schwab rejects the complex close order.",
            "order_type": "NET_DEBIT_LIMIT",
            "contracts": contracts,
            "expiration_date": str(trade.get("expiration_date") or ""),
            "short_strike": trade.get("short_strike"),
            "long_strike": trade.get("long_strike"),
            "limit_price": round(limit_price, 2),
            "limit_price_display": f"{limit_price:.2f}",
            "account_hash": str(account_context.get("account_hash") or ""),
            "account_number": str(account_context.get("account_number") or ""),
            "account_source": str(account_context.get("source") or "Unavailable"),
            "confirmation_text_required": self.CONFIRMATION_TEXT,
            "manual_override": bool(manual_override),
            "active_gate": str(record.get("active_gate_key") or "manual-review"),
            "trigger_reason": str(record.get("reason") or record.get("status") or "Manual close review"),
            "short_option_symbol": self._extract_metadata(notes_entry, "short_option_symbol"),
            "long_option_symbol": self._extract_metadata(notes_entry, "long_option_symbol"),
        }
        preview["signature"] = self._build_signature(preview)
        preview["summary"] = (
            f"Buy back {contracts}x {preview.get('short_strike')}P and sell {contracts}x {preview.get('long_strike')}P "
            f"for a {preview.get('limit_price_display')} net debit close."
        )
        return preview

    def submit_open_order(self, preview: Dict[str, Any]) -> Dict[str, Any]:
        payload = self._build_complex_vertical_payload(preview=preview, opening=True)
        try:
            return self._submit_order(preview=preview, payload=payload, action_label="open")
        except ProviderError as exc:
            if not self._complex_order_unsupported(str(exc)):
                raise
            return self._submit_open_fallback(preview)

    def submit_close_order(self, preview: Dict[str, Any]) -> Dict[str, Any]:
        payload = self._build_complex_vertical_payload(preview=preview, opening=False)
        try:
            return self._submit_order(preview=preview, payload=payload, action_label="close")
        except ProviderError as exc:
            if not self._complex_order_unsupported(str(exc)):
                raise
            return self._submit_close_fallback(preview)

    def _submit_order(self, *, preview: Dict[str, Any], payload: Dict[str, Any], action_label: str) -> Dict[str, Any]:
        account_hash = str(preview.get("account_hash") or "").strip()
        if not account_hash:
            raise ProviderError("Talos real execution is missing a Schwab account hash.")
        response = self._authorized_request(
            method="POST",
            url=f"{self.config.schwab_trading_base_url}/accounts/{account_hash}/orders",
            json_payload=payload,
        )
        if response.status_code >= 400:
            raise ProviderError(f"Talos {action_label} order rejected by Schwab ({response.status_code}): {response.text}")
        location = str(response.headers.get("Location") or response.headers.get("location") or "").strip()
        return {
            "ok": True,
            "order_id": location.split("/")[-1] if location else "",
            "broker_status": response.status_code,
            "preferred_method_used": True,
            "emergency_state": False,
            "message": f"Talos real {action_label} order submitted to Schwab.",
        }

    def _submit_open_fallback(self, preview: Dict[str, Any]) -> Dict[str, Any]:
        long_fill = self._submit_single_leg_order(
            preview=preview,
            symbol=str(preview.get("long_option_symbol") or ""),
            instruction="BUY_TO_OPEN",
            price=self._marketable_limit_price(preview.get("long_ask"), buy_side=True),
            action_label="fallback long buy",
        )
        try:
            short_fill = self._submit_single_leg_order(
                preview=preview,
                symbol=str(preview.get("short_option_symbol") or ""),
                instruction="SELL_TO_OPEN",
                price=self._marketable_limit_price(preview.get("short_bid"), buy_side=False),
                action_label="fallback short sell",
            )
            return {
                "ok": True,
                "order_id": str(short_fill.get("order_id") or long_fill.get("order_id") or ""),
                "broker_status": short_fill.get("broker_status"),
                "preferred_method_used": False,
                "emergency_state": False,
                "message": "Talos real open order submitted through protected fallback leg routing.",
            }
        except Exception as exc:
            return {
                "ok": False,
                "order_id": str(long_fill.get("order_id") or ""),
                "broker_status": long_fill.get("broker_status"),
                "preferred_method_used": False,
                "emergency_state": True,
                "message": f"Protected long-only emergency state: long leg submitted but short leg failed. Manual review required. {exc}",
            }

    def _submit_close_fallback(self, preview: Dict[str, Any]) -> Dict[str, Any]:
        long_fill = self._submit_single_leg_order(
            preview=preview,
            symbol=str(preview.get("long_option_symbol") or ""),
            instruction="SELL_TO_CLOSE",
            price=self._marketable_limit_price(preview.get("limit_price"), buy_side=False),
            action_label="fallback long sale",
        )
        try:
            short_fill = self._submit_single_leg_order(
                preview=preview,
                symbol=str(preview.get("short_option_symbol") or ""),
                instruction="BUY_TO_CLOSE",
                price=self._marketable_limit_price(preview.get("limit_price"), buy_side=True),
                action_label="fallback short buyback",
            )
            return {
                "ok": True,
                "order_id": str(short_fill.get("order_id") or long_fill.get("order_id") or ""),
                "broker_status": short_fill.get("broker_status"),
                "preferred_method_used": False,
                "emergency_state": False,
                "message": "Talos real close order submitted through protected fallback leg routing.",
            }
        except Exception as exc:
            return {
                "ok": False,
                "order_id": str(long_fill.get("order_id") or ""),
                "broker_status": long_fill.get("broker_status"),
                "preferred_method_used": False,
                "emergency_state": True,
                "message": f"Protected long-only emergency state during close handling. Manual review required. {exc}",
            }

    def _submit_single_leg_order(self, *, preview: Dict[str, Any], symbol: str, instruction: str, price: float, action_label: str) -> Dict[str, Any]:
        account_hash = str(preview.get("account_hash") or "").strip()
        if not account_hash:
            raise ProviderError("Talos real execution is missing a Schwab account hash.")
        if not symbol:
            raise ProviderError(f"Talos {action_label} requires a valid option symbol.")
        order_type = "MARKET" if self.config.talos_allow_market_orders else "LIMIT"
        payload: Dict[str, Any] = {
            "orderType": order_type,
            "session": "NORMAL",
            "duration": "DAY",
            "orderStrategyType": "SINGLE",
            "orderLegCollection": [
                {
                    "instruction": instruction,
                    "quantity": int(preview.get("contracts") or 0),
                    "instrument": {"symbol": symbol, "assetType": "OPTION"},
                }
            ],
        }
        if order_type == "LIMIT":
            payload["price"] = f"{price:.2f}"
        response = self._authorized_request(
            method="POST",
            url=f"{self.config.schwab_trading_base_url}/accounts/{account_hash}/orders",
            json_payload=payload,
        )
        if response.status_code >= 400:
            raise ProviderError(f"Talos {action_label} failed ({response.status_code}): {response.text}")
        location = str(response.headers.get("Location") or response.headers.get("location") or "").strip()
        return {"ok": True, "order_id": location.split("/")[-1] if location else "", "broker_status": response.status_code}

    def _build_complex_vertical_payload(self, *, preview: Dict[str, Any], opening: bool) -> Dict[str, Any]:
        short_symbol = str(preview.get("short_option_symbol") or "").strip()
        long_symbol = str(preview.get("long_option_symbol") or "").strip()
        if not short_symbol or not long_symbol:
            raise ProviderError("Talos real execution requires option symbols for both vertical legs.")
        return {
            "complexOrderStrategyType": "VERTICAL",
            "orderType": "NET_CREDIT" if opening else "NET_DEBIT",
            "session": "NORMAL",
            "duration": "DAY",
            "price": f"{float(preview.get('limit_price') or 0.0):.2f}",
            "orderStrategyType": "SINGLE",
            "orderLegCollection": [
                {
                    "instruction": ("SELL_TO_OPEN" if opening else "BUY_TO_CLOSE"),
                    "quantity": int(preview.get("contracts") or 0),
                    "instrument": {"symbol": short_symbol, "assetType": "OPTION"},
                },
                {
                    "instruction": ("BUY_TO_OPEN" if opening else "SELL_TO_CLOSE"),
                    "quantity": int(preview.get("contracts") or 0),
                    "instrument": {"symbol": long_symbol, "assetType": "OPTION"},
                },
            ],
        }

    def _authorized_request(self, *, method: str, url: str, json_payload: Dict[str, Any]) -> requests.Response:
        access_token = self.execution_auth_service.get_valid_access_token()
        response = requests.request(
            method,
            url,
            json=json_payload,
            headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json", "Content-Type": "application/json"},
            timeout=30,
        )
        if response.status_code != 401:
            return response
        refreshed_token = self.execution_auth_service.recover_from_unauthorized_response()
        return requests.request(
            method,
            url,
            json=json_payload,
            headers={"Authorization": f"Bearer {refreshed_token}", "Accept": "application/json", "Content-Type": "application/json"},
            timeout=30,
        )

    @staticmethod
    def _complex_order_unsupported(message: str) -> bool:
        normalized = str(message or "").strip().lower()
        return any(token in normalized for token in ("unsupported", "not supported", "complex", "vertical"))

    def _marketable_limit_price(self, reference_price: Any, *, buy_side: bool) -> float:
        value = self._coerce_float(reference_price) or 0.0
        if self.config.talos_allow_market_orders:
            return round(value, 2)
        offset = 0.1
        adjusted = value + offset if buy_side else max(value - offset, 0.01)
        return round(adjusted, 2)

    @staticmethod
    def _extract_metadata(text: str, key: str) -> str:
        marker = f"{key}="
        for part in str(text or "").split("|"):
            token = str(part).strip()
            if token.startswith(marker):
                return token[len(marker):].strip()
        return ""

    @staticmethod
    def _build_signature(payload: Dict[str, Any]) -> str:
        return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()[:16]

    @staticmethod
    def _coerce_float(value: Any) -> Optional[float]:
        try:
            if value in {None, ""}:
                return None
            return float(value)
        except (TypeError, ValueError):
            return None