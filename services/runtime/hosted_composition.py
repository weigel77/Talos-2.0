"""Hosted runtime composer skeleton for Delphi 5.0."""

from __future__ import annotations

from config import AppConfig
from services.market_data import MarketDataService
from services.repositories.hosted_runtime_state_repository import SupabaseApolloSnapshotRepository, SupabaseImportPreviewRepository
from services.repositories.management_state_repository import SupabaseOpenTradeManagementStateRepository
from services.repositories.trade_repository import SupabaseTradeRepository, TradeRepository

from .hosted_auth import HostedAuthConfig, SupabaseHostedIdentityResolver, SupabasePrivateAccessGate, SupabaseSessionInvalidator
from .auth_composition import AuthComposer, HostedSupabaseAuthComposer
from .host_infrastructure import HostInfrastructure
from .private_access import PrivateAccessGate, RequestIdentityResolver, SessionInvalidator
from .provider_composition import ProviderComposer
from .service_composition import LocalRuntimeServiceComposer, RuntimeServiceBundle, RuntimeServiceComposer


class HostedRuntimeServiceComposer(LocalRuntimeServiceComposer):
    """Hosted runtime composer that uses Supabase-only persistence."""

    def __init__(
        self,
        config: AppConfig,
        *,
        host_infrastructure: HostInfrastructure,
        auth_composer: AuthComposer,
        provider_composer: ProviderComposer,
        trade_prefill_key: str,
        trade_close_prefill_key: str,
        trade_form_fields: tuple[str, ...] | list[str],
        trade_mode_resolver,
    ) -> None:
        super().__init__(
            config,
            host_infrastructure=host_infrastructure,
            auth_composer=auth_composer or self._create_hosted_auth_composer(host_infrastructure),
            provider_composer=provider_composer,
            trade_prefill_key=trade_prefill_key,
            trade_close_prefill_key=trade_close_prefill_key,
            trade_form_fields=trade_form_fields,
            trade_mode_resolver=trade_mode_resolver,
        )

    @staticmethod
    def _require_supabase(host_infrastructure: HostInfrastructure) -> tuple:
        context = host_infrastructure.supabase_context
        integration = host_infrastructure.supabase_integration
        if context is None or integration is None or not context.configured:
            raise RuntimeError("Hosted Delphi 6.0 requires a configured Supabase project. Local persistence is not allowed in hosted mode.")
        return context, integration

    def _create_hosted_auth_composer(self, host_infrastructure: HostInfrastructure) -> HostedSupabaseAuthComposer:
        context, integration = self._require_supabase(host_infrastructure)
        return HostedSupabaseAuthComposer(
            self.config,
            context=context,
            gateway=integration.create_table_gateway(),
        )

    def _create_trade_repository(self, market_data_service: MarketDataService) -> tuple[TradeRepository, TradeRepository]:
        context, integration = self._require_supabase(self.host_infrastructure)
        trade_repository = SupabaseTradeRepository(
            context=context,
            gateway=integration.create_table_gateway(),
            database_path="supabase://journal_trades",
            market_data_service=market_data_service,
        )
        return trade_repository, trade_repository

    def _create_apollo_snapshot_repository(self, *, storage):
        context, integration = self._require_supabase(self.host_infrastructure)
        return SupabaseApolloSnapshotRepository(
            context=context,
            gateway=integration.create_table_gateway(),
        )

    def _create_import_preview_repository(self, *, storage):
        context, integration = self._require_supabase(self.host_infrastructure)
        return SupabaseImportPreviewRepository(
            context=context,
            gateway=integration.create_table_gateway(),
        )

    def _create_management_state_repository(self, trade_store: TradeRepository) -> SupabaseOpenTradeManagementStateRepository | None:
        _, integration = self._require_supabase(self.host_infrastructure)
        return SupabaseOpenTradeManagementStateRepository(integration.create_table_gateway())

    def _create_auth_components(self, app) -> tuple[RequestIdentityResolver, PrivateAccessGate, SessionInvalidator]:
        context = self.host_infrastructure.supabase_context
        auth_config = HostedAuthConfig.resolve(app, self.config, context)
        return (
            SupabaseHostedIdentityResolver(context=context, auth_config=auth_config),
            SupabasePrivateAccessGate(auth_config),
            SupabaseSessionInvalidator(auth_config),
        )


def select_runtime_service_composer(
    app,
    config: AppConfig,
    *,
    host_infrastructure: HostInfrastructure,
    auth_composer: AuthComposer,
    provider_composer: ProviderComposer,
    trade_prefill_key: str,
    trade_close_prefill_key: str,
    trade_form_fields: tuple[str, ...] | list[str],
    trade_mode_resolver,
) -> RuntimeServiceComposer:
    """Select the runtime service composer for the active runtime target."""

    if host_infrastructure.settings.runtime_target == "hosted":
        return HostedRuntimeServiceComposer(
            config,
            host_infrastructure=host_infrastructure,
            auth_composer=auth_composer,
            provider_composer=provider_composer,
            trade_prefill_key=trade_prefill_key,
            trade_close_prefill_key=trade_close_prefill_key,
            trade_form_fields=trade_form_fields,
            trade_mode_resolver=trade_mode_resolver,
        )

    return LocalRuntimeServiceComposer(
        config,
        host_infrastructure=host_infrastructure,
        auth_composer=auth_composer,
        provider_composer=provider_composer,
        trade_prefill_key=trade_prefill_key,
        trade_close_prefill_key=trade_close_prefill_key,
        trade_form_fields=trade_form_fields,
        trade_mode_resolver=trade_mode_resolver,
    )


HostedRuntimeServiceComposerSkeleton = HostedRuntimeServiceComposer