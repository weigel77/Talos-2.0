"""Market data orchestration layer for pluggable providers."""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from time import perf_counter
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

import pandas as pd
from flask import g, has_request_context

from config import AppConfig, get_app_config

from .cache_service import CacheEntry, CacheService
from .calculations import add_daily_change_columns, calculate_percent_change, calculate_point_change
from .market_calendar_service import MarketCalendarService
from .provider_factory import ProviderFactory
from .runtime_metrics_service import get_runtime_metrics_service
from .runtime.provider_composition import LocalProviderComposer, ProviderComposer
from .providers.base_provider import (
    ProviderAuthRequiredError,
    ProviderConfigurationError,
    ProviderError,
    ProviderRateLimitError,
    ProviderReauthenticationRequiredError,
)

CHICAGO_TZ = ZoneInfo(get_app_config().app_timezone)
LOOKBACK_DAYS = 30
LATEST_CACHE_SECONDS = 30
HISTORICAL_CACHE_SECONDS = 600
REGULAR_SESSION_START = (8, 30)
REQUEST_MARKET_DATA_CACHE_KEY = "_delphi_request_market_data_cache"
REQUEST_INTRADAY_CACHE_KEY = "_delphi_request_intraday_cache"
LOGGER = logging.getLogger(__name__)


class MarketDataError(Exception):
    """Raised when market data could not be retrieved or interpreted."""


class MarketDataAuthenticationError(MarketDataError):
    """Raised when the active provider requires an interactive login."""


class MarketDataReauthenticationRequired(MarketDataError):
    """Raised when the active provider session has expired and the user must log in again."""


class MarketDataService:
    """Fetch and normalize market data for the supported lookup types."""

    def __init__(
        self,
        display_timezone: Optional[str] = None,
        cache_service: Optional[CacheService] = None,
        provider: Any = None,
        historical_providers: Optional[Dict[str, Any]] = None,
        provider_composer: ProviderComposer | None = None,
        config: Optional[AppConfig] = None,
    ) -> None:
        self.config = config or get_app_config()
        self.provider_composer = provider_composer or LocalProviderComposer(self.config)
        timezone_name = display_timezone or self.config.app_timezone
        self.display_timezone = ZoneInfo(timezone_name)
        self.cache_service = cache_service or CacheService()
        self.market_calendar_service = MarketCalendarService(self.config)
        self.live_provider = provider or ProviderFactory.create_live_provider(self.config, provider_composer=self.provider_composer)
        self.latest_cache_seconds = max(int(self.config.market_snapshot_cache_ttl_seconds or LATEST_CACHE_SECONDS), 1)
        if historical_providers is not None:
            self.historical_providers = historical_providers
        elif provider is not None:
            self.historical_providers = {
                "^VIX": provider,
                "^GSPC": provider,
            }
        else:
            self.historical_providers = ProviderFactory.create_historical_providers(self.config, provider_composer=self.provider_composer)
        self.provider = self.live_provider

    def get_latest_snapshot(self, ticker: str, query_type: str = "latest") -> Dict[str, Any]:
        """Return the freshest available market snapshot for the ticker."""
        return self._get_latest_snapshot_with_request_cache(ticker=ticker, query_type=query_type, fresh=False)

    def get_fresh_latest_snapshot(self, ticker: str, query_type: str = "latest") -> Dict[str, Any]:
        """Force a fresh market snapshot by bypassing the short-lived cache key."""
        return self._get_latest_snapshot_with_request_cache(ticker=ticker, query_type=query_type, fresh=True)

    def _get_latest_snapshot_with_request_cache(
        self,
        *,
        ticker: str,
        query_type: str,
        fresh: bool,
    ) -> Dict[str, Any]:
        """Reuse the same latest snapshot across a single request when the ticker matches."""
        started = perf_counter()
        metrics = get_runtime_metrics_service()
        provider = self.live_provider
        provider_key = provider.get_metadata().get("provider_key", "unknown")
        request_cache = self._get_request_market_data_cache()
        request_cache_key = f"{provider_key}:{ticker}:latest"
        if not fresh and request_cache is not None and request_cache_key in request_cache:
            metrics.record(
                "Schwab snapshot calls",
                (perf_counter() - started) * 1000.0,
                cache_hit=True,
                detail=f"{ticker}|request-cache|{query_type}",
            )
            return self.cache_service.clone_payload(request_cache[request_cache_key])

        effective_query_type = query_type if not fresh else f"{query_type}:fresh:{self._current_time().isoformat()}"
        cache_key = self.cache_service.build_cache_key(
            provider=provider_key,
            ticker=ticker,
            query_type=effective_query_type,
        )
        cached_entry = self.cache_service.get_cached_result(cache_key)
        cache_hit = bool(cached_entry and self.cache_service.is_cache_valid(cached_entry, self.latest_cache_seconds, self._current_time()))
        payload = self._execute_cached_query(
            cache_key=cache_key,
            ttl_seconds=self.latest_cache_seconds,
            fetcher=lambda: self._build_latest_snapshot(ticker),
            provider=provider,
        )
        if not fresh and request_cache is not None:
            request_cache[request_cache_key] = self.cache_service.clone_payload(payload)
        metrics.record(
            "Schwab snapshot calls",
            (perf_counter() - started) * 1000.0,
            cache_hit=cache_hit,
            detail=f"{ticker}|{'fresh' if fresh else 'shared-cache'}|{query_type}",
        )
        return payload

    def get_history_with_changes(self, ticker: str, start_date: date, end_date: date, query_type: str = "history") -> pd.DataFrame:
        """Return daily history for a date range, including change columns."""
        provider = self._get_historical_provider(ticker)
        cache_key = self.cache_service.build_cache_key(
            provider=provider.get_metadata().get("provider_key", "unknown"),
            ticker=ticker,
            query_type=query_type,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
        )
        return self._execute_cached_query(
            cache_key=cache_key,
            ttl_seconds=HISTORICAL_CACHE_SECONDS,
            fetcher=lambda: self._build_history_with_changes(ticker, start_date, end_date),
            provider=provider,
        )

    def get_single_day_change(self, ticker: str, target_date: date, query_type: str = "single_change") -> Dict[str, Any]:
        """Return the requested date plus prior-day comparison for change calculations."""
        provider = self._get_historical_provider(ticker)
        cache_key = self.cache_service.build_cache_key(
            provider=provider.get_metadata().get("provider_key", "unknown"),
            ticker=ticker,
            query_type=query_type,
            target_date=target_date.isoformat(),
        )
        return self._execute_cached_query(
            cache_key=cache_key,
            ttl_seconds=HISTORICAL_CACHE_SECONDS,
            fetcher=lambda: self._build_single_day_change(ticker, target_date),
            provider=provider,
        )

    def get_single_day_bar(self, ticker: str, target_date: date, query_type: str = "single_close") -> Dict[str, Any]:
        """Return a single requested trading day bar."""
        provider = self._get_historical_provider(ticker)
        cache_key = self.cache_service.build_cache_key(
            provider=provider.get_metadata().get("provider_key", "unknown"),
            ticker=ticker,
            query_type=query_type,
            target_date=target_date.isoformat(),
        )
        return self._execute_cached_query(
            cache_key=cache_key,
            ttl_seconds=HISTORICAL_CACHE_SECONDS,
            fetcher=lambda: self._build_single_day_bar(ticker, target_date),
            provider=provider,
        )

    def get_same_day_intraday_candles(
        self,
        ticker: str,
        interval_minutes: int = 5,
        query_type: str = "intraday",
    ) -> pd.DataFrame:
        """Return same-day intraday candles from the active live provider."""
        current_time = self._current_time()
        session_request = self.resolve_intraday_session_request(current_time=current_time)
        return self._get_intraday_frame_with_request_cache(
            ticker=ticker,
            requested_session_date=session_request["requested_session_date"],
            resolved_session_date=session_request["resolved_session_date"],
            interval_minutes=interval_minutes,
            query_type=query_type,
            fresh=False,
        )

    def get_fresh_same_day_intraday_candles(
        self,
        ticker: str,
        interval_minutes: int = 5,
        query_type: str = "intraday",
    ) -> pd.DataFrame:
        """Force a fresh same-day intraday request by bypassing the short-lived cache key."""
        current_time = self._current_time()
        session_request = self.resolve_intraday_session_request(current_time=current_time)
        return self._get_intraday_frame_with_request_cache(
            ticker=ticker,
            requested_session_date=session_request["requested_session_date"],
            resolved_session_date=session_request["resolved_session_date"],
            interval_minutes=interval_minutes,
            query_type=query_type,
            fresh=True,
        )

    def get_intraday_candles_for_date(
        self,
        ticker: str,
        target_date: date,
        interval_minutes: int = 5,
        query_type: str = "intraday_date",
    ) -> pd.DataFrame:
        """Return intraday candles for the requested session date from the active live provider."""
        return self._get_intraday_frame_with_request_cache(
            ticker=ticker,
            requested_session_date=target_date,
            resolved_session_date=target_date,
            interval_minutes=interval_minutes,
            query_type=query_type,
            fresh=False,
        )

    def get_fresh_intraday_candles_for_date(
        self,
        ticker: str,
        target_date: date,
        interval_minutes: int = 5,
        query_type: str = "intraday_date",
    ) -> pd.DataFrame:
        """Force a fresh dated intraday request by bypassing the short-lived cache key."""
        return self._get_intraday_frame_with_request_cache(
            ticker=ticker,
            requested_session_date=target_date,
            resolved_session_date=target_date,
            interval_minutes=interval_minutes,
            query_type=query_type,
            fresh=True,
        )

    def get_account_summary(self, *, force_refresh: bool = False) -> Dict[str, Any]:
        """Return the authenticated broker account summary from the active live provider."""
        provider = self.live_provider
        provider_key = provider.get_metadata().get("provider_key", "unknown")
        query_type = "account_summary" if not force_refresh else f"account_summary:fresh:{self._current_time().isoformat()}"
        cache_key = self.cache_service.build_cache_key(
            provider=provider_key,
            ticker="ACCOUNT",
            query_type=query_type,
        )
        return self._execute_cached_query(
            cache_key=cache_key,
            ttl_seconds=LATEST_CACHE_SECONDS,
            fetcher=lambda: provider.get_account_summary(),
            provider=provider,
        )

    def resolve_intraday_session_request(
        self,
        requested_session_date: date | None = None,
        *,
        current_time: datetime | None = None,
    ) -> Dict[str, Any]:
        """Resolve the latest valid intraday session date for the current market state."""
        local_now = current_time or self._current_time()
        local_now = local_now.astimezone(self.display_timezone) if local_now.tzinfo else local_now.replace(tzinfo=self.display_timezone)
        requested_date = requested_session_date or local_now.date()
        latest_available_session = self._get_latest_available_intraday_session_date(local_now)
        resolved_date = requested_date
        reasons: list[str] = []

        if requested_date > latest_available_session:
            resolved_date = latest_available_session
            reasons.append(f"requested session exceeded latest available tradable session {latest_available_session.isoformat()}")

        if not self.market_calendar_service.is_tradable_market_day(resolved_date):
            closure_name = self.market_calendar_service.get_holiday_name(resolved_date) or ("weekend" if resolved_date.weekday() >= 5 else "exchange-closed")
            prior_tradable = self._get_previous_tradable_market_day(resolved_date)
            reasons.append(f"requested session {resolved_date.isoformat()} was {closure_name}; using {prior_tradable.isoformat()}")
            resolved_date = prior_tradable

        return {
            "requested_session_date": requested_date,
            "resolved_session_date": resolved_date,
            "local_now": local_now,
            "normalization_reason": "; ".join(reasons) if reasons else "",
        }

    def _get_intraday_frame_with_request_cache(
        self,
        *,
        ticker: str,
        requested_session_date: date,
        resolved_session_date: date,
        interval_minutes: int,
        query_type: str,
        fresh: bool,
    ) -> pd.DataFrame:
        provider = self.live_provider
        provider_key = provider.get_metadata().get("provider_key", "unknown")
        request_cache = self._get_request_intraday_cache()

        request_cache_key = ":".join(
            [
                provider_key,
                ticker,
                str(interval_minutes),
                resolved_session_date.isoformat(),
                "fresh" if fresh else "cached",
            ]
        )

        if request_cache is not None and request_cache_key in request_cache:
            return self.cache_service.clone_payload(request_cache[request_cache_key])

        effective_query_type = query_type if not fresh else f"{query_type}:fresh:{self._current_time().isoformat()}"
        cache_key = self.cache_service.build_cache_key(
            provider=provider_key,
            ticker=ticker,
            query_type=effective_query_type,
            interval=str(interval_minutes),
            session_date=resolved_session_date.isoformat(),
        )
        payload = self._execute_cached_query(
            cache_key=cache_key,
            ttl_seconds=LATEST_CACHE_SECONDS,
            fetcher=lambda: provider.get_intraday_candles_for_date(
                ticker,
                target_date=resolved_session_date,
                interval_minutes=interval_minutes,
            ),
            provider=provider,
        )

        if request_cache is not None:
            request_cache[request_cache_key] = self.cache_service.clone_payload(payload)

        return payload

    def _get_latest_available_intraday_session_date(self, local_now: datetime) -> date:
        candidate = local_now.date()
        market_open_hour, market_open_minute = REGULAR_SESSION_START
        if not self.market_calendar_service.is_tradable_market_day(candidate):
            return self._get_previous_tradable_market_day(candidate)
        if (local_now.hour, local_now.minute) < (market_open_hour, market_open_minute):
            return self._get_previous_tradable_market_day(candidate)
        return candidate

    def _get_previous_tradable_market_day(self, anchor_date: date) -> date:
        candidate = anchor_date - timedelta(days=1)
        while not self.market_calendar_service.is_tradable_market_day(candidate):
            candidate -= timedelta(days=1)
        return candidate

    def _get_request_intraday_cache(self) -> dict[str, Any] | None:
        if not has_request_context():
            return None
        cache = getattr(g, REQUEST_INTRADAY_CACHE_KEY, None)
        if cache is None:
            cache = {}
            setattr(g, REQUEST_INTRADAY_CACHE_KEY, cache)
        return cache

    def _get_request_market_data_cache(self) -> dict[str, Any] | None:
        if not has_request_context():
            return None
        cache = getattr(g, REQUEST_MARKET_DATA_CACHE_KEY, None)
        if cache is None:
            cache = {}
            setattr(g, REQUEST_MARKET_DATA_CACHE_KEY, cache)
        return cache

    @staticmethod
    def get_result_metadata(result: Any) -> Dict[str, Any]:
        """Extract result metadata from a cached or live payload."""
        if isinstance(result, pd.DataFrame):
            return result.attrs.get("result_meta", {})
        if isinstance(result, dict):
            return result.get("_meta", {})
        return {}

    def get_provider_metadata(self) -> Dict[str, Any]:
        """Return active provider metadata for the UI layer."""
        live_metadata = self.live_provider.get_metadata()
        vix_history = self._get_historical_provider("^VIX").get_metadata()
        spx_history = self._get_historical_provider("^GSPC").get_metadata()
        return {
            "provider_name": live_metadata.get("provider_name", "Unknown Provider"),
            "requires_auth": live_metadata.get("requires_auth", False),
            "authenticated": live_metadata.get("authenticated", True),
            "live_provider_name": live_metadata.get("provider_name", "Unknown Provider"),
            "live_provider_key": live_metadata.get("provider_key", "unknown"),
            "vix_historical_provider_name": vix_history.get("provider_name", "Unknown Provider"),
            "vix_historical_provider_key": vix_history.get("provider_key", "unknown"),
            "spx_historical_provider_name": spx_history.get("provider_name", "Unknown Provider"),
            "spx_historical_provider_key": spx_history.get("provider_key", "unknown"),
        }

    def _build_latest_snapshot(self, ticker: str) -> Dict[str, Any]:
        """Perform the live latest-snapshot lookup through the active provider."""
        return self.live_provider.get_latest_snapshot(ticker)

    def _build_history_with_changes(self, ticker: str, start_date: date, end_date: date) -> pd.DataFrame:
        """Perform the live historical range lookup through the active provider."""
        requested_history = self._fetch_exact_history(ticker=ticker, start_date=start_date, end_date=end_date)
        support_history = self._fetch_prior_history_support(ticker=ticker, target_date=start_date)
        history_frame = self._combine_support_and_requested_history(support_history, requested_history)
        history_frame = add_daily_change_columns(history_frame)
        filtered = history_frame.loc[(history_frame["Date"] >= start_date) & (history_frame["Date"] <= end_date)].copy()

        if filtered.empty:
            raise MarketDataError(f"No trading data was returned for {ticker} between {start_date.isoformat()} and {end_date.isoformat()}.")

        return self._round_numeric_columns(filtered)

    def _build_single_day_change(self, ticker: str, target_date: date) -> Dict[str, Any]:
        """Perform the live single-day change lookup through the active provider."""
        requested_history = self._fetch_exact_history(ticker=ticker, start_date=target_date, end_date=target_date)
        support_history = self._fetch_prior_history_support(ticker=ticker, target_date=target_date)
        history_frame = self._combine_support_and_requested_history(support_history, requested_history)
        history_frame = add_daily_change_columns(history_frame)
        matching_rows = history_frame.loc[history_frame["Date"] == target_date]

        if matching_rows.empty:
            raise MarketDataError(
                f"No trading data exists for {ticker} on {target_date.isoformat()}. The market may have been closed."
            )

        current_row = matching_rows.iloc[0]
        position = matching_rows.index[0]
        frame_position = history_frame.index.get_loc(position)
        if frame_position == 0:
            raise MarketDataError(f"Unable to calculate the prior trading day for {ticker} on {target_date.isoformat()}.")

        prior_row = history_frame.iloc[frame_position - 1]

        return {
            "current": self._row_to_dict(current_row),
            "prior": self._row_to_dict(prior_row),
        }

    def _build_single_day_bar(self, ticker: str, target_date: date) -> Dict[str, Any]:
        """Perform the live single-day bar lookup through the active provider."""
        return self._get_historical_provider(ticker).get_single_date(ticker, target_date)

    def _execute_cached_query(self, cache_key: str, ttl_seconds: int, fetcher: Any, provider: Any) -> Any:
        """Return live data when available, otherwise fall back to cache when appropriate."""
        current_time = self._current_time()
        cached_entry = self.cache_service.get_cached_result(cache_key)
        provider_metadata = provider.get_metadata()

        if cached_entry and self.cache_service.is_cache_valid(cached_entry, ttl_seconds, current_time):
            LOGGER.info("Serving cache hit for %s", cache_key)
            return self._attach_result_metadata(
                payload=self.cache_service.clone_payload(cached_entry.payload),
                provider_metadata=provider_metadata,
                source_type="cache",
                retrieved_at=cached_entry.cached_at,
                cache_age_seconds=self.cache_service.cache_age_seconds(cached_entry, current_time),
                warning=None,
            )

        try:
            payload = fetcher()
            retrieved_at = self._current_time()
            self.cache_service.set_cached_result(cache_key, payload, retrieved_at)
            LOGGER.info("Serving live %s data for %s", provider_metadata.get("provider_name", "provider"), cache_key)
            return self._attach_result_metadata(
                payload=self.cache_service.clone_payload(payload),
                provider_metadata=provider_metadata,
                source_type="live",
                retrieved_at=retrieved_at,
                cache_age_seconds=None,
                warning=None,
            )
        except Exception as exc:
            provider_error = self._normalize_provider_error(exc)

        if cached_entry and provider_error.is_transient:
            fallback_time = self._current_time()
            LOGGER.warning("Using cached fallback for %s after provider failure: %s", cache_key, provider_error)
            return self._attach_result_metadata(
                payload=self.cache_service.clone_payload(cached_entry.payload),
                provider_metadata=provider_metadata,
                source_type="fallback",
                retrieved_at=cached_entry.cached_at,
                cache_age_seconds=self.cache_service.cache_age_seconds(cached_entry, fallback_time),
                warning=str(provider_error),
            )

        raise self._normalize_market_data_error(provider_error)

    def _fetch_exact_history(self, ticker: str, start_date: date, end_date: date) -> pd.DataFrame:
        """Fetch the exact user-requested history range from the active provider."""
        history_frame = self._get_historical_provider(ticker).get_historical_range(ticker, start_date, end_date)
        return self._round_numeric_columns(history_frame)

    def _fetch_prior_history_support(self, ticker: str, target_date: date) -> pd.DataFrame:
        """Fetch a separate support range used only to calculate the first prior close.

        This internal support fetch never changes the user-facing requested date range.
        """
        support_end = target_date - timedelta(days=1)
        if support_end < date.min + timedelta(days=LOOKBACK_DAYS):
            support_start = date.min
        else:
            support_start = target_date - timedelta(days=LOOKBACK_DAYS)

        if support_end < support_start:
            return pd.DataFrame(columns=["Date", "Open", "High", "Low", "Close"])

        try:
            support_history = self._get_historical_provider(ticker).get_historical_range(ticker, support_start, support_end)
            return self._round_numeric_columns(support_history)
        except ProviderError as exc:
            LOGGER.info(
                "Internal prior-close support fetch failed for %s between %s and %s: %s",
                ticker,
                support_start.isoformat(),
                support_end.isoformat(),
                exc,
            )
            return pd.DataFrame(columns=["Date", "Open", "High", "Low", "Close"])

    def _get_historical_provider(self, ticker: str) -> Any:
        """Return the configured historical provider for the requested ticker."""
        if ticker == "^VIX":
            return self.historical_providers.get("^VIX", self.live_provider)
        if ticker == "^GSPC":
            return self.historical_providers.get("^GSPC", self.live_provider)
        return self.live_provider

    @staticmethod
    def _combine_support_and_requested_history(support_history: pd.DataFrame, requested_history: pd.DataFrame) -> pd.DataFrame:
        """Combine prior support rows with the exact requested history without duplicates."""
        if support_history.empty:
            return requested_history.copy()
        combined = pd.concat([support_history, requested_history], ignore_index=True)
        combined = combined.drop_duplicates(subset=["Date"], keep="last")
        return combined.sort_values("Date").reset_index(drop=True)

    def _round_numeric_columns(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        frame = dataframe.copy()
        numeric_columns = frame.select_dtypes(include=["number"]).columns
        for column in numeric_columns:
            frame[column] = frame[column].round(2)
        return frame

    def _attach_result_metadata(
        self,
        payload: Any,
        provider_metadata: Dict[str, Any],
        source_type: str,
        retrieved_at: datetime,
        cache_age_seconds: Optional[int],
        warning: Optional[str],
    ) -> Any:
        """Attach cache and retrieval metadata without affecting exportable data."""
        metadata = {
            "provider_name": provider_metadata.get("provider_name", "Unknown Provider"),
            "provider_key": provider_metadata.get("provider_key", "unknown"),
            "source": source_type,
            "source_type": source_type,
            "retrieved_at": retrieved_at.astimezone(self.display_timezone),
            "cache_age_seconds": cache_age_seconds,
            "warning": warning,
            "used_stale_cache": source_type == "fallback",
        }

        if isinstance(payload, pd.DataFrame):
            payload.attrs["result_meta"] = metadata
            return payload

        if isinstance(payload, dict):
            payload["_meta"] = metadata
            return payload

        return payload

    @staticmethod
    def _normalize_provider_error(error: Exception) -> ProviderError:
        """Normalize provider and unexpected errors to a provider error."""
        if isinstance(error, ProviderError):
            return error
        if isinstance(error, MarketDataError):
            return ProviderError(str(error))
        return ProviderError(f"Unable to retrieve market data from the active provider right now: {error}")

    @staticmethod
    def _normalize_market_data_error(error: ProviderError) -> MarketDataError:
        """Convert provider errors into friendly market-data errors for the Flask app."""
        if isinstance(error, ProviderReauthenticationRequiredError):
            return MarketDataReauthenticationRequired(str(error))
        if isinstance(error, ProviderAuthRequiredError):
            return MarketDataAuthenticationError(str(error))
        if isinstance(error, ProviderRateLimitError):
            return MarketDataError(str(error))
        if isinstance(error, ProviderConfigurationError):
            return MarketDataError(str(error))
        return MarketDataError(str(error))

    @staticmethod
    def _current_time() -> datetime:
        """Return the current Chicago-local timestamp."""
        return datetime.now(CHICAGO_TZ)

    @staticmethod
    def _row_to_dict(row: pd.Series) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        for key, value in row.items():
            if isinstance(value, float):
                result[key] = round(value, 2)
            else:
                result[key] = value
        return result

    @staticmethod
    def _to_chicago_timestamp(value: Any) -> datetime:
        timestamp = pd.Timestamp(value)
        if timestamp.tzinfo is None:
            timestamp = timestamp.tz_localize("UTC")
        return timestamp.tz_convert(CHICAGO_TZ).to_pydatetime()

    @staticmethod
    def _get_prior_close(dataframe: pd.DataFrame, market_date: date) -> Optional[float]:
        previous_rows = dataframe.loc[dataframe["Date"] < market_date]
        if previous_rows.empty:
            return None
        return float(previous_rows.iloc[-1]["Close"])
