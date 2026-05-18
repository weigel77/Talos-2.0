"""Provider composition boundary for local and future hosted Delphi runtimes."""

from __future__ import annotations

from typing import Dict, Protocol, runtime_checkable

from config import AppConfig

from services.providers.base_provider import BaseMarketDataProvider, UnavailableProvider
from services.providers.cboe_provider import CboeVixHistoricalProvider
from services.providers.schwab_provider import SchwabProvider
from services.providers.spx_history_provider import SpxHistoricalUnavailableProvider
from .auth_composition import AuthComposer, LocalAuthComposer


@runtime_checkable
class ProviderComposer(Protocol):
    """Abstract provider composition contract for runtime-specific provider assembly."""

    def create_live_provider(self) -> BaseMarketDataProvider:
        ...

    def create_historical_providers(self) -> Dict[str, BaseMarketDataProvider]:
        ...


class LocalProviderComposer:
    """Local provider composition that preserves the current provider wiring rules."""

    def __init__(self, config: AppConfig, auth_composer: AuthComposer | None = None) -> None:
        self.config = config
        self.auth_composer = auth_composer or LocalAuthComposer(config)

    def create_live_provider(self) -> BaseMarketDataProvider:
        provider_key = self.resolve_live_provider_key()

        if provider_key == "schwab":
            return self._create_schwab_provider()

        return UnavailableProvider(
            provider_key=provider_key,
            message=(
                f"Unsupported live market data provider '{provider_key}'. Talos 3.4 requires Schwab-only live data."
            ),
            display_timezone=self.config.app_timezone,
        )

    def create_historical_providers(self) -> Dict[str, BaseMarketDataProvider]:
        return {
            "^VIX": self._create_historical_provider("vix"),
            "^GSPC": self._create_historical_provider("spx"),
        }

    def resolve_live_provider_key(self) -> str:
        return (self.config.market_data_live_provider or self.config.market_data_provider or "yahoo").strip().lower()

    def resolve_historical_provider_key(self, capability: str) -> str:
        if capability == "vix":
            configured = (self.config.vix_historical_provider or "").strip().lower()
            if configured:
                return configured
            return "schwab"

        if capability == "spx":
            configured = (self.config.spx_historical_provider or "").strip().lower()
            if configured:
                return configured
            return "schwab"

        return "schwab"

    def _create_schwab_provider(self) -> SchwabProvider:
        auth_service = self.auth_composer.create_schwab_auth_service()
        return SchwabProvider(config=self.config, display_timezone=self.config.app_timezone, auth_service=auth_service)

    def _create_historical_provider(self, capability: str) -> BaseMarketDataProvider:
        provider_key = self.resolve_historical_provider_key(capability)

        if provider_key == "schwab":
            return self._create_schwab_provider()
        if provider_key in {"none", "unconfigured", "spx_stub"} and capability == "spx":
            return SpxHistoricalUnavailableProvider(display_timezone=self.config.app_timezone)

        return UnavailableProvider(
            provider_key=f"{capability}-historical-{provider_key}",
            message=(
                f"Unsupported historical provider '{provider_key}' for {capability.upper()}. Talos 3.4 requires Schwab-only market data."
            ),
            display_timezone=self.config.app_timezone,
        )