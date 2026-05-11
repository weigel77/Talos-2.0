"""Apollo option-chain retrieval and summarization services."""

from __future__ import annotations

import inspect
import logging
from datetime import date, datetime, timedelta
from typing import Any, Dict, List
from zoneinfo import ZoneInfo

from config import AppConfig, get_app_config

from .provider_factory import ProviderFactory
from .runtime.auth_composition import AuthComposer, LocalAuthComposer
from .runtime.provider_composition import LocalProviderComposer, ProviderComposer
from .repositories.token_repository import JsonFileTokenRepository
from .schwab_auth_service import SchwabAuthService
from .market_calendar_service import MarketCalendarService
from .providers.base_provider import ProviderError
from .providers.schwab_provider import SchwabProvider


LOGGER = logging.getLogger(__name__)


class OptionsChainService:
    """Retrieve and summarize SPX option-chain data for Apollo."""

    PREVIEW_WIDTHS = (5.0, 10.0, 15.0, 20.0, 25.0, 30.0)
    MIN_PREVIEW_NET_CREDIT = 1.0
    SUMMARY_CACHE_TTL_SECONDS = 30

    def __init__(
        self,
        config: AppConfig | None = None,
        provider: Any | None = None,
        provider_composer: ProviderComposer | None = None,
        auth_composer: AuthComposer | None = None,
    ) -> None:
        self.config = config or get_app_config()
        self.provider = provider
        self.auth_composer = auth_composer or LocalAuthComposer(self.config)
        self.provider_composer = provider_composer or LocalProviderComposer(self.config, self.auth_composer)
        self.market_calendar_service = MarketCalendarService(self.config)
        self.display_timezone = ZoneInfo(self.config.app_timezone)
        self._summary_cache: Dict[tuple[str, str], Dict[str, Any]] = {}
        self._normalization_log_cache: Dict[tuple[str, str, str, str], datetime] = {}

    def resolve_requested_expiration_date(self, expiration_date: date) -> tuple[date, str | None]:
        return self._normalize_requested_expiration_date(expiration_date)

    def get_spx_option_chain_summary(
        self,
        expiration_date: date,
        *,
        caller_context: str | None = None,
        log_normalization: bool = True,
    ) -> Dict[str, Any]:
        """Return a compact normalized next-market-day SPX option-chain summary."""
        provider = None
        requested_expiration = expiration_date
        resolved_expiration, normalization_reason = self._normalize_requested_expiration_date(expiration_date)
        resolved_caller_context = str(caller_context or self._describe_option_chain_request_caller()).strip() or "unknown"
        cache_key = ("spx", resolved_expiration.isoformat())
        cached_summary = self._get_cached_summary(cache_key)
        if cached_summary is not None:
            return cached_summary
        try:
            if normalization_reason and log_normalization:
                self._log_normalization_once(
                    caller_context=resolved_caller_context,
                    requested_expiration=requested_expiration,
                    resolved_expiration=resolved_expiration,
                    normalization_reason=normalization_reason,
                )

            LOGGER.warning(
                "Apollo option-chain refresh | caller=%s | requested_expiration=%s | resolved_expiration=%s | cache_hit=%s",
                resolved_caller_context,
                requested_expiration.isoformat(),
                resolved_expiration.isoformat(),
                False,
            )

            provider = self._resolve_provider()
            chain = provider.get_option_chain("^GSPC", target_date=resolved_expiration)
            request_diagnostics = chain.get("request_diagnostics") or getattr(provider, "last_option_chain_diagnostics", {})
            calls = list(chain.get("calls") or [])
            puts = list(chain.get("puts") or [])
            all_options = calls + puts
            strikes = [self._coerce_float(option.get("strike")) for option in all_options]
            strikes = [value for value in strikes if value is not None]
            preview_rows = self._build_preview_rows(
                calls=calls,
                puts=puts,
                underlying_price=self._coerce_float(chain.get("underlying_price")),
            )

            if not calls and not puts:
                summary = {
                    "success": False,
                    "source_name": getattr(provider, "provider_name", "Unknown Provider"),
                    "failure_category": "empty-response",
                    "failure_label": "Empty response",
                    "failure_status_class": "not-available",
                    "symbol_requested": chain.get("requested_symbol", self.config.schwab_spx_option_chain_symbol),
                    "expiration_target": chain.get("expiration_target") or requested_expiration,
                    "expiration_date": chain.get("expiration_date") or resolved_expiration,
                    "expiration_count": chain.get("expiration_count", 0),
                    "underlying_price": chain.get("underlying_price"),
                    "puts_count": 0,
                    "calls_count": 0,
                    "rows_displayed": 0,
                    "strike_range": "—",
                    "puts": [],
                    "calls": [],
                    "preview_rows": [],
                    "request_diagnostics": request_diagnostics,
                    "message": "Schwab returned a response, but no usable SPX option contracts were available for the requested expiration.",
                }
                self._store_cached_summary(cache_key, summary)
                return summary

            strike_range = (
                f"{min(strikes):,.0f} to {max(strikes):,.0f}"
                if strikes
                else "—"
            )
            summary = {
                "success": True,
                "source_name": getattr(provider, "provider_name", "Unknown Provider"),
                "failure_category": "",
                "failure_label": "",
                "failure_status_class": "good",
                "symbol_requested": chain.get("requested_symbol", self.config.schwab_spx_option_chain_symbol),
                "expiration_target": chain.get("expiration_target") or requested_expiration,
                "expiration_date": chain.get("expiration_date") or resolved_expiration,
                "expiration_count": chain.get("expiration_count", 0),
                "underlying_price": chain.get("underlying_price"),
                "puts_count": len(puts),
                "calls_count": len(calls),
                "rows_displayed": len(preview_rows),
                "strike_range": strike_range,
                "puts": puts,
                "calls": calls,
                "preview_rows": preview_rows,
                "request_diagnostics": request_diagnostics,
                "message": "SPX option chain retrieved successfully.",
            }
            self._store_cached_summary(cache_key, summary)
            return summary
        except Exception as exc:
            request_diagnostics = getattr(provider, "last_option_chain_diagnostics", {}) if provider is not None else {}
            failure_category, failure_label, failure_status_class = self._resolve_failure_metadata(
                request_diagnostics=request_diagnostics,
                message=str(exc),
            )
            summary = {
                "success": False,
                "source_name": "Schwab",
                "failure_category": failure_category,
                "failure_label": failure_label,
                "failure_status_class": failure_status_class,
                "symbol_requested": self.config.schwab_spx_option_chain_symbol,
                "expiration_target": requested_expiration,
                "expiration_date": resolved_expiration,
                "expiration_count": 0,
                "underlying_price": None,
                "puts_count": 0,
                "calls_count": 0,
                "rows_displayed": 0,
                "strike_range": "—",
                "puts": [],
                "calls": [],
                "preview_rows": [],
                "request_diagnostics": request_diagnostics,
                "message": str(exc),
            }
            self._store_cached_summary(cache_key, summary)
            return summary

    def _get_cached_summary(self, cache_key: tuple[str, str]) -> Dict[str, Any] | None:
        cached_entry = self._summary_cache.get(cache_key)
        if not cached_entry:
            return None
        cached_at = cached_entry.get("cached_at")
        if not isinstance(cached_at, datetime):
            self._summary_cache.pop(cache_key, None)
            return None
        if (self._now() - cached_at) > timedelta(seconds=self.SUMMARY_CACHE_TTL_SECONDS):
            self._summary_cache.pop(cache_key, None)
            return None
        return dict(cached_entry.get("summary") or {})

    def _store_cached_summary(self, cache_key: tuple[str, str], summary: Dict[str, Any]) -> None:
        self._summary_cache[cache_key] = {
            "cached_at": self._now(),
            "summary": dict(summary),
        }

    def _log_normalization_once(
        self,
        *,
        caller_context: str,
        requested_expiration: date,
        resolved_expiration: date,
        normalization_reason: str,
    ) -> None:
        log_key = (
            caller_context,
            requested_expiration.isoformat(),
            resolved_expiration.isoformat(),
            normalization_reason,
        )
        now = self._now()
        prior_logged_at = self._normalization_log_cache.get(log_key)
        if prior_logged_at is not None and (now - prior_logged_at) <= timedelta(seconds=self.SUMMARY_CACHE_TTL_SECONDS):
            return
        self._normalization_log_cache[log_key] = now
        LOGGER.warning(
            "Apollo option-chain expiration normalized | caller=%s | requested_expiration=%s | resolved_expiration=%s | reason=%s",
            caller_context,
            requested_expiration.isoformat(),
            resolved_expiration.isoformat(),
            normalization_reason,
        )

    def _normalize_requested_expiration_date(self, expiration_date: date) -> tuple[date, str | None]:
        local_now = self._now()
        resolved_expiration = expiration_date
        reasons: list[str] = []
        session_date = local_now.date()

        if resolved_expiration < session_date:
            resolved_expiration = session_date
            reasons.append(f"requested expiration was before the current session date {session_date.isoformat()}")

        if not self.market_calendar_service.is_tradable_market_day(resolved_expiration):
            closure_reason = self.market_calendar_service.get_holiday_name(resolved_expiration) or "weekend"
            rolled_expiration = self.market_calendar_service.get_next_or_same_tradable_market_day(resolved_expiration)
            reasons.append(
                f"requested expiration {resolved_expiration.isoformat()} was {closure_reason}; rolled forward to {rolled_expiration.isoformat()}"
            )
            resolved_expiration = rolled_expiration

        return resolved_expiration, "; ".join(reasons) if reasons else None

    def _describe_option_chain_request_caller(self) -> str:
        for frame in inspect.stack()[2:]:
            normalized_path = frame.filename.replace("\\", "/")
            if normalized_path.endswith("/services/options_chain_service.py"):
                continue
            if "/site-packages/" in normalized_path:
                continue
            if "Delphi-5.0-Web-Dev/" in normalized_path:
                normalized_path = normalized_path.split("Delphi-5.0-Web-Dev/", 1)[1]
            return f"{normalized_path}:{frame.lineno}:{frame.function}"
        return "unknown"

    def _now(self) -> datetime:
        return datetime.now(self.display_timezone)

    @staticmethod
    def _resolve_failure_metadata(
        *,
        request_diagnostics: Dict[str, Any],
        message: str,
    ) -> tuple[str, str, str]:
        """Return a normalized Apollo-friendly failure category."""
        failure_category = str(request_diagnostics.get("failure_category") or "").strip().lower()
        failure_label = str(request_diagnostics.get("failure_label") or "").strip()

        if not failure_category:
            normalized_message = message.lower()
            if any(code in normalized_message for code in ("503", "502", "504", "429")):
                failure_category = "upstream-unavailable"
            elif any(code in normalized_message for code in ("400", "404", "422")):
                failure_category = "malformed-request"
            else:
                failure_category = "unknown-error"

        if not failure_label:
            failure_label = {
                "upstream-unavailable": "Upstream unavailable",
                "malformed-request": "Malformed request",
                "exchange-closed": "Exchange closed",
                "empty-response": "Empty response",
            }.get(failure_category, "Unavailable")

        failure_status_class = "poor" if failure_category == "malformed-request" else "not-available"
        if failure_category == "exchange-closed":
            failure_status_class = "neutral"
        return failure_category, failure_label, failure_status_class

    def _resolve_provider(self) -> Any:
        if self.provider is not None:
            return self.provider

        if self.config.apollo_option_chain_source != "schwab":
            raise ProviderError(
                f"Unsupported Apollo option-chain source '{self.config.apollo_option_chain_source}'. Use 'schwab'."
            )

        provider = ProviderFactory.create_live_provider(self.config, provider_composer=self.provider_composer)
        if isinstance(provider, SchwabProvider):
            return provider

        auth_service = self.auth_composer.create_schwab_auth_service()
        return SchwabProvider(config=self.config, display_timezone=self.config.app_timezone, auth_service=auth_service)

    def _build_preview_rows(
        self,
        calls: List[Dict[str, Any]],
        puts: List[Dict[str, Any]],
        underlying_price: float | None,
    ) -> List[Dict[str, Any]]:
        if not calls and not puts:
            return []

        rows = self._build_credit_spread_preview_rows(side="PUT", contracts=puts, underlying_price=underlying_price)
        rows.extend(self._build_credit_spread_preview_rows(side="CALL", contracts=calls, underlying_price=underlying_price))
        return rows

    def _build_credit_spread_preview_rows(
        self,
        side: str,
        contracts: List[Dict[str, Any]],
        underlying_price: float | None,
    ) -> List[Dict[str, Any]]:
        if not contracts:
            return []

        normalized = sorted(
            [
                {
                    "strike": self._coerce_float(item.get("strike")),
                    "bid": self._coerce_float(item.get("bid")) or 0.0,
                    "ask": self._coerce_float(item.get("ask")) or 0.0,
                    "last": self._coerce_float(item.get("last")) or 0.0,
                    "delta": self._coerce_float(item.get("delta")),
                }
                for item in contracts
                if self._coerce_float(item.get("strike")) is not None
            ],
            key=lambda item: item["strike"],
        )
        if not normalized:
            return []

        strike_lookup = {round(item["strike"], 2): item for item in normalized}
        preview_rows: List[Dict[str, Any]] = []

        for short_leg in normalized:
            if underlying_price is not None:
                if side == "PUT" and short_leg["strike"] >= underlying_price:
                    continue
                if side == "CALL" and short_leg["strike"] <= underlying_price:
                    continue

            for width in self.PREVIEW_WIDTHS:
                long_strike = short_leg["strike"] - width if side == "PUT" else short_leg["strike"] + width
                long_leg = strike_lookup.get(round(long_strike, 2))
                if long_leg is None:
                    continue

                conservative_credit = short_leg["bid"] - long_leg["ask"]
                mid_credit = self._premium_anchor(short_leg) - self._premium_anchor(long_leg)
                net_credit = max(conservative_credit, mid_credit)
                if net_credit <= self.MIN_PREVIEW_NET_CREDIT:
                    continue

                distance_points = None if underlying_price is None else abs(underlying_price - short_leg["strike"])
                preview_rows.append(
                    {
                        "side": side,
                        "short_strike": self._format_numeric(short_leg["strike"]),
                        "long_strike": self._format_numeric(long_leg["strike"]),
                        "width": self._format_numeric(width),
                        "net_credit": self._format_numeric(net_credit),
                        "short_delta": self._format_numeric(abs(short_leg["delta"])) if short_leg["delta"] is not None else "—",
                        "distance": self._format_numeric(distance_points) if distance_points is not None else "—",
                    }
                )

        preview_rows.sort(
            key=lambda item: (
                0 if item["side"] == "PUT" else 1,
                float(str(item["distance"]).replace(",", "")) if item["distance"] not in {"—", None, ""} else 0.0,
                float(str(item["short_strike"]).replace(",", "")),
                float(str(item["width"]).replace(",", "")),
            )
        )
        return preview_rows

    @staticmethod
    def _coerce_float(value: Any) -> float | None:
        if value in (None, ""):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _format_numeric(self, value: Any) -> str:
        numeric = self._coerce_float(value)
        if numeric is None:
            return "—"
        return f"{numeric:,.2f}"

    @staticmethod
    def _premium_anchor(contract: Dict[str, Any]) -> float:
        bid = float(contract.get("bid") or 0.0)
        ask = float(contract.get("ask") or 0.0)
        last = float(contract.get("last") or 0.0)
        if bid > 0 and ask > 0:
            return (bid + ask) / 2.0
        return max(last, bid, ask)
