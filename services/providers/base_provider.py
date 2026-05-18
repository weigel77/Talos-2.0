"""Base provider contracts for pluggable market-data backends."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date
from typing import Any, Dict
from zoneinfo import ZoneInfo

import pandas as pd


class ProviderError(Exception):
    """Base error for provider-related failures."""

    def __init__(self, message: str, *, is_transient: bool = False) -> None:
        super().__init__(message)
        self.is_transient = is_transient


class ProviderRateLimitError(ProviderError):
    """Raised when a provider indicates a temporary rate limit."""

    def __init__(self, message: str = "Yahoo Finance temporarily rate-limited the request. Please wait a moment and try again.") -> None:
        super().__init__(message, is_transient=True)


class ProviderConfigurationError(ProviderError):
    """Raised when a provider is selected but not configured correctly."""


class ProviderNotImplementedError(ProviderConfigurationError):
    """Raised when a provider scaffold exists but is not yet implemented."""


class ProviderAuthRequiredError(ProviderError):
    """Raised when the provider requires an interactive login."""


class ProviderReauthenticationRequiredError(ProviderError):
    """Raised when an expired provider session requires a new login."""


class BaseMarketDataProvider(ABC):
    """Abstract contract for market-data providers.

    Expected return shapes:
    - `get_latest_snapshot(symbol)` returns a dict containing at least:
      `Ticker`, `Market Date`, `Latest Value`, `Open`, `High`, `Low`, `Close`,
      `Prior Close`, `Daily Point Change`, `Daily Percent Change`, and `As Of`.
    - `get_historical_range(symbol, start_date, end_date)` returns a pandas DataFrame
      sorted ascending with columns: `Date`, `Open`, `High`, `Low`, `Close`.
    - `get_single_date(symbol, target_date)` returns a dict for a single market bar with
      keys: `Date`, `Open`, `High`, `Low`, `Close`.
    """

    provider_key = "base"
    provider_name = "Base Provider"

    def __init__(self, display_timezone: str = "America/Chicago") -> None:
        self.display_timezone = ZoneInfo(display_timezone)

    def get_metadata(self) -> Dict[str, Any]:
        """Return provider identity metadata."""
        return {
            "provider_key": self.provider_key,
            "provider_name": self.provider_name,
            "requires_auth": False,
            "authenticated": True,
        }

    @abstractmethod
    def get_latest_snapshot(self, symbol: str) -> Dict[str, Any]:
        """Return the latest snapshot for the requested symbol."""

    @abstractmethod
    def get_historical_range(self, symbol: str, start_date: date, end_date: date) -> pd.DataFrame:
        """Return historical daily bars for the inclusive date range."""

    @abstractmethod
    def get_single_date(self, symbol: str, target_date: date) -> Dict[str, Any]:
        """Return a single daily bar for the requested trading date."""

    def get_same_day_intraday_candles(self, symbol: str, interval_minutes: int = 5) -> pd.DataFrame:
        """Return same-day intraday candles when the provider supports them."""
        raise ProviderNotImplementedError(
            f"{self.provider_name} does not provide same-day {interval_minutes}-minute candles for this workflow yet."
        )

    def get_intraday_candles_for_date(self, symbol: str, target_date: date, interval_minutes: int = 5) -> pd.DataFrame:
        """Return intraday candles for a specific trading date when supported."""
        raise ProviderNotImplementedError(
            f"{self.provider_name} does not provide {interval_minutes}-minute candles for {target_date.isoformat()} yet."
        )

    def get_option_chain(self, symbol: str, target_date: date | None = None) -> Any:
        """Placeholder future option-chain contract for providers that support it."""
        raise ProviderNotImplementedError("Option-chain support has not been implemented for this provider yet.")

    def get_account_summary(self) -> Dict[str, Any]:
        """Return broker account summary details when the provider supports account access."""
        raise ProviderNotImplementedError(f"{self.provider_name} does not provide account summary data for this workflow yet.")


class UnavailableProvider(BaseMarketDataProvider):
    """Provider that raises a friendly error when configuration is unsupported."""

    provider_name = "Unavailable Provider"

    def __init__(self, provider_key: str, message: str, display_timezone: str = "America/Chicago") -> None:
        super().__init__(display_timezone=display_timezone)
        self.provider_key = provider_key
        self.message = message

    def get_metadata(self) -> Dict[str, Any]:
        metadata = super().get_metadata()
        metadata["message"] = self.message
        return metadata

    def get_latest_snapshot(self, symbol: str) -> Dict[str, Any]:
        raise ProviderConfigurationError(self.message)

    def get_historical_range(self, symbol: str, start_date: date, end_date: date) -> pd.DataFrame:
        raise ProviderConfigurationError(self.message)

    def get_single_date(self, symbol: str, target_date: date) -> Dict[str, Any]:
        raise ProviderConfigurationError(self.message)