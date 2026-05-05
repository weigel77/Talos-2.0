"""Runtime service composition boundary for local and future hosted Delphi runtimes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Protocol, runtime_checkable

from flask import Flask

from config import AppConfig
from services.apollo_service import ApolloService
from services.market_data import MarketDataService
from services.open_trade_manager import OpenTradeManager
from services.options_chain_service import OptionsChainService
from services.performance_dashboard_service import PerformanceDashboardService
from services.performance_engine import PerformanceEngine
from services.pushover_service import PushoverService
from services.repositories.apollo_snapshot_repository import ApolloSnapshotRepository, JsonFileApolloSnapshotRepository
from services.repositories.import_preview_repository import FileSystemImportPreviewRepository, ImportPreviewRepository
from services.repositories.management_state_repository import OpenTradeManagementStateRepository
from services.repositories.trade_repository import MirroredTradeRepository, SQLiteTradeRepository, SupabaseTradeRepository, TradeRepository
from services.runtime.private_access import LocalPrivateAccessGate, LocalRequestIdentityResolver, NoopSessionInvalidator, PrivateAccessGate, RequestIdentityResolver, SessionInvalidator
from services.runtime.notifications import PushoverNotificationDelivery
from services.runtime.scheduler import ThreadingTimerScheduler
from services.runtime.workflow_state import FlaskSessionWorkflowState
from services.trade_store import TradeStore

from .auth_composition import AuthComposer, LocalAuthComposer
from .host_infrastructure import HostInfrastructure
from .lifecycle import RuntimeComponent
from .provider_composition import LocalProviderComposer, ProviderComposer


@dataclass(frozen=True)
class RuntimeServiceBundle:
    host_infrastructure: HostInfrastructure
    market_data_service: MarketDataService
    apollo_service: ApolloService
    apollo_snapshot_repository: ApolloSnapshotRepository
    runtime_scheduler: ThreadingTimerScheduler
    trade_store_backend: Any
    trade_store: TradeRepository
    import_preview_repository: ImportPreviewRepository
    workflow_state: FlaskSessionWorkflowState
    request_identity_resolver: RequestIdentityResolver
    private_access_gate: PrivateAccessGate
    session_invalidator: SessionInvalidator
    pushover_service: PushoverService
    notification_delivery: PushoverNotificationDelivery
    performance_service: PerformanceDashboardService
    performance_engine: PerformanceEngine
    open_trade_manager: OpenTradeManager
    runtime_components: list[RuntimeComponent]


@runtime_checkable
class RuntimeServiceComposer(Protocol):
    """Abstract runtime service composition contract for app-factory assembly."""

    def compose(self, app: Flask) -> RuntimeServiceBundle:
        ...


class LocalRuntimeServiceComposer:
    """Local service composer that preserves the current app-factory wiring."""

    def __init__(
        self,
        config: AppConfig,
        *,
        host_infrastructure: HostInfrastructure,
        auth_composer: AuthComposer | None = None,
        provider_composer: ProviderComposer | None = None,
        trade_prefill_key: str,
        trade_close_prefill_key: str,
        trade_form_fields: tuple[str, ...] | list[str],
        trade_mode_resolver: Callable[[str], str],
    ) -> None:
        self.config = config
        self.host_infrastructure = host_infrastructure
        self.auth_composer = auth_composer or LocalAuthComposer(config)
        self.provider_composer = provider_composer or LocalProviderComposer(config, self.auth_composer)
        self.trade_prefill_key = trade_prefill_key
        self.trade_close_prefill_key = trade_close_prefill_key
        self.trade_form_fields = tuple(trade_form_fields)
        self.trade_mode_resolver = trade_mode_resolver

    def compose(self, app: Flask) -> RuntimeServiceBundle:
        runtime_scheduler = ThreadingTimerScheduler()
        storage = self.host_infrastructure.storage
        market_data_service = MarketDataService(config=self.config, provider_composer=self.provider_composer)
        apollo_snapshot_repository = self._create_apollo_snapshot_repository(storage=storage)
        trade_store_backend, trade_store = self._create_trade_repository(market_data_service)
        import_preview_repository = self._create_import_preview_repository(storage=storage)
        request_identity_resolver, private_access_gate, session_invalidator = self._create_auth_components(app)
        workflow_state = FlaskSessionWorkflowState(
            trade_prefill_key=self.trade_prefill_key,
            trade_close_prefill_key=self.trade_close_prefill_key,
            trade_form_fields=self.trade_form_fields,
            trade_mode_resolver=self.trade_mode_resolver,
        )
        pushover_service = PushoverService(config=self.config)
        notification_delivery = PushoverNotificationDelivery(pushover_service)
        apollo_options_chain_service = OptionsChainService(
            self.config,
            provider=market_data_service.live_provider,
            provider_composer=self.provider_composer,
            auth_composer=self.auth_composer,
        )
        apollo_service = ApolloService(
            market_data_service=market_data_service,
            options_chain_service=apollo_options_chain_service,
            config=self.config,
        )
        performance_service = PerformanceDashboardService(trade_store)
        performance_engine = PerformanceEngine(trade_store)
        trade_store.initialize()
        management_state_repository = self._create_management_state_repository(trade_store)
        open_trade_manager = OpenTradeManager(
            trade_store=trade_store,
            market_data_service=market_data_service,
            apollo_service=apollo_service,
            options_chain_service=apollo_service.options_chain_service,
            notification_delivery=notification_delivery,
            config=self.config,
            state_repository=management_state_repository,
            scheduler=runtime_scheduler,
        )
        open_trade_manager.initialize()
        runtime_components = [
            RuntimeComponent(
                name="open_trade_manager",
                startup=open_trade_manager.start_background_monitoring,
                shutdown=open_trade_manager.shutdown,
            ),
        ]
        return RuntimeServiceBundle(
            host_infrastructure=self.host_infrastructure,
            market_data_service=market_data_service,
            apollo_service=apollo_service,
            apollo_snapshot_repository=apollo_snapshot_repository,
            runtime_scheduler=runtime_scheduler,
            trade_store_backend=trade_store_backend,
            trade_store=trade_store,
            import_preview_repository=import_preview_repository,
            workflow_state=workflow_state,
            request_identity_resolver=request_identity_resolver,
            private_access_gate=private_access_gate,
            session_invalidator=session_invalidator,
            pushover_service=pushover_service,
            notification_delivery=notification_delivery,
            performance_service=performance_service,
            performance_engine=performance_engine,
            open_trade_manager=open_trade_manager,
            runtime_components=runtime_components,
        )

    def _create_trade_repository(self, market_data_service: MarketDataService) -> tuple[Any, TradeRepository]:
        storage = self.host_infrastructure.storage
        trade_store_backend = TradeStore(str(storage.trade_database_path), market_data_service=market_data_service)
        local_repository = SQLiteTradeRepository(trade_store_backend)
        supabase_context = self.host_infrastructure.supabase_context
        supabase_integration = self.host_infrastructure.supabase_integration
        if supabase_context is not None and supabase_integration is not None and supabase_context.configured:
            remote_repository = SupabaseTradeRepository(
                context=supabase_context,
                gateway=supabase_integration.create_table_gateway(),
                database_path="supabase://journal_trades",
                market_data_service=market_data_service,
            )
            return trade_store_backend, MirroredTradeRepository(
                local_repository=local_repository,
                remote_repository=remote_repository,
            )
        return trade_store_backend, local_repository

    def _create_apollo_snapshot_repository(self, *, storage) -> ApolloSnapshotRepository:
        return JsonFileApolloSnapshotRepository(storage.instance_path / "apollo_last_run.json")

    def _create_import_preview_repository(self, *, storage) -> ImportPreviewRepository:
        return FileSystemImportPreviewRepository(storage.import_preview_root)

    def _create_management_state_repository(self, trade_store: TradeRepository) -> OpenTradeManagementStateRepository | None:
        return None

    def _create_auth_components(self, app: Flask) -> tuple[RequestIdentityResolver, PrivateAccessGate, SessionInvalidator]:
        return (LocalRequestIdentityResolver(), LocalPrivateAccessGate(), NoopSessionInvalidator())