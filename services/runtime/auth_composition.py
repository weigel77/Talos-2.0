"""Auth composition boundary for local and future hosted Delphi runtimes."""

from __future__ import annotations

from pathlib import Path
from typing import Protocol, runtime_checkable

from config import AppConfig

from services.repositories.hosted_runtime_state_repository import SupabaseTokenRepository
from services.repositories.token_repository import JsonFileTokenRepository, TokenRepository
from services.schwab_auth_service import SchwabAuthService
from services.runtime.supabase_integration import SupabaseRuntimeContext, SupabaseTableGateway


@runtime_checkable
class AuthComposer(Protocol):
    """Abstract auth composition contract for runtime-specific assembly."""

    def create_token_repository(self) -> TokenRepository:
        ...

    def create_schwab_auth_service(self) -> SchwabAuthService:
        ...


class LocalAuthComposer:
    """Local auth composition that preserves the current token and Schwab auth wiring."""

    def __init__(self, config: AppConfig, *, token_path: str | Path | None = None) -> None:
        self.config = config
        self.token_path = token_path

    def create_token_repository(self) -> TokenRepository:
        return JsonFileTokenRepository(self.token_path or self.config.schwab_shared_market_token_path)

    def create_schwab_auth_service(self) -> SchwabAuthService:
        return SchwabAuthService(config=self.config, token_store=self.create_token_repository())


class HostedSupabaseAuthComposer(LocalAuthComposer):
    """Hosted auth composition that keeps Schwab OAuth tokens in Supabase."""

    def __init__(
        self,
        config: AppConfig,
        *,
        context: SupabaseRuntimeContext,
        gateway: SupabaseTableGateway,
    ) -> None:
        super().__init__(config)
        self.context = context
        self.gateway = gateway

    def create_token_repository(self) -> TokenRepository:
        return SupabaseTokenRepository(context=self.context, gateway=self.gateway)