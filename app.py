"""Flask entry point for the local SPX and VIX market lookup tool."""

from __future__ import annotations

import logging
import json
from pathlib import Path
from dataclasses import asdict
from datetime import date, datetime
from logging.handlers import RotatingFileHandler
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

import pandas as pd
from flask import Flask, abort, current_app, g, has_app_context, has_request_context, jsonify, redirect, render_template, request, url_for

from config import (
    AppConfig,
    HOSTED_APP_VERSION,
    get_app_config,
)
from services import (
    ApolloService,
    MarketDataAuthenticationError,
    MarketDataError,
    MarketDataReauthenticationRequired,
    MarketDataService,
    OpenTradeManager,
    PerformanceDashboardService,
)
from services.performance_dashboard_service import PERFORMANCE_FILTER_GROUPS, normalize_filter_value
from services.repositories.apollo_snapshot_repository import ApolloSnapshotRepository
from services.repositories.import_preview_repository import FileSystemImportPreviewRepository, ImportPreviewRepository
from services.repositories.global_notification_settings_repository import (
    GlobalNotificationSettingsRepository,
    SQLiteGlobalNotificationSettingsRepository,
)
from services.repositories.trade_notification_repository import (
    SQLiteTradeNotificationRepository,
    TradeNotificationRepository,
)
from services.repositories.trade_repository import TradeRepository
from services.runtime.auth_composition import LocalAuthComposer
from services.runtime.host_infrastructure import select_host_infrastructure_assembler
from services.runtime.private_access import RequestIdentity, RequestIdentityResolver, anonymous_request_identity
from services.runtime.launch import LaunchBehavior, WebBrowserLaunchBehavior
from services.runtime.lifecycle import LocalRuntimeLifecycleCoordinator, RuntimeLifecycleCoordinator
from services.runtime.provider_composition import LocalProviderComposer
from services.runtime.profile import RuntimeProfile, select_runtime_profile
from services.runtime.scheduler import ThreadingTimerScheduler
from services.runtime.service_composition import LocalRuntimeServiceComposer
from services.runtime.workflow_state import FlaskSessionWorkflowState, WorkflowStateStore
from services.trade_importer import parse_trade_import
from services.trade_store import (
    JOURNAL_NAME_DEFAULT,
    TradeStore,
    blank_trade_form,
    classify_closed_trade_outcome,
    build_trade_duplicate_signature,
    current_timestamp,
    form_trade_record,
    normalize_candidate_profile,
    normalize_expected_move_source,
    resolve_trade_credit_model,
    resolve_trade_candidate_profile,
    resolve_trade_distance,
    resolve_trade_expected_move,
    resolve_trade_system_name,
    is_retained_trade_system,
    summarize_trade_close_events,
    normalize_trade_mode,
    normalize_system_name,
    to_int,
)
APP_CONFIG = get_app_config()
CHICAGO_TZ = ZoneInfo(APP_CONFIG.app_timezone)
APP_HOST = APP_CONFIG.app_host
APP_PORT = APP_CONFIG.app_port
LOCAL_DEV_HOSTS = {"127.0.0.1", "localhost"}
LOCALHOST_DEV_CERT_BASENAME = "localhost+2"

TRADE_MODE_LABELS = {"real": "Journal", "simulated": "Simulated Trades", "talos": "Journal"}
TRADE_MODE_DESCRIPTIONS = {
    "real": "Apollo and Talos trade history for the retained Talos 2 journal.",
    "simulated": "Persistent Apollo paper-trade log for simulated execution review.",
    "talos": "Apollo and Talos trade history for the retained Talos 2 journal.",
}
PUBLIC_TRADE_MODES = ("real", "simulated")
LOCAL_SCHWAB_REDIRECT_URI = "https://127.0.0.1:5015/callback"
TRADE_STATUS_OPTIONS = ["open", "closed", "expired", "cancelled"]
TRADE_PROFILE_OPTIONS = ["Legacy", "Aggressive", "Fortress", "Standard", "Prime", "Subprime"]
TRADE_OPTION_TYPE_OPTIONS = ["Put Credit Spread", "Call Credit Spread"]
TRADE_SYSTEM_OPTIONS = ["Apollo", "Talos", "Fortress"]
TRADE_JOURNAL_OPTIONS = [JOURNAL_NAME_DEFAULT]
APOLLO_PREFILL_SESSION_KEY = "apollo_trade_prefill"
MANAGEMENT_CLOSE_PREFILL_SESSION_KEY = "management_close_prefill"
TRADE_IMPORT_PREVIEW_DIRNAME = "trade_import_previews"
TRADE_FORM_FIELDS = [
    "trade_number",
    "trade_mode",
    "system_name",
    "journal_name",
    "system_version",
    "candidate_profile",
    "status",
    "trade_date",
    "entry_datetime",
    "expiration_date",
    "underlying_symbol",
    "spx_at_entry",
    "vix_at_entry",
    "structure_grade",
    "macro_grade",
    "expected_move",
    "expected_move_used",
    "expected_move_source",
    "option_type",
    "short_strike",
    "long_strike",
    "spread_width",
    "contracts",
    "candidate_credit_estimate",
    "actual_entry_credit",
    "distance_to_short",
    "em_multiple_floor",
    "percent_floor",
    "boundary_rule_used",
    "actual_distance_to_short",
    "actual_em_multiple",
    "pass_type",
    "premium_per_contract",
    "total_premium",
    "max_theoretical_risk",
    "risk_efficiency",
    "target_em",
    "prefill_source",
    "automation_status",
    "fallback_used",
    "fallback_rule_name",
    "short_delta",
    "notes_entry",
    "prefill_source",
    "exit_datetime",
    "spx_at_exit",
    "actual_exit_value",
    "close_method",
    "close_reason",
    "notes_exit",
]
TRADE_FILTER_GROUPS = {
    "system": ["Apollo", "Talos"],
    "profile": ["Legacy", "Aggressive", "Fortress", "Standard", "Prime", "Subprime"],
    "result": ["Win", "Loss", "Black Swan"],
}


def build_oauth_session_keys(namespace: str) -> Dict[str, str]:
    """Return the environment-specific session keys used by the Schwab OAuth flow."""
    prefix = str(namespace or "delphi").strip().lower() or "delphi"
    return {
        "oauth_state": f"{prefix}_oauth_state",
        "pkce_verifier": f"{prefix}_pkce_verifier",
        "login_in_progress": f"{prefix}_login_in_progress",
        "connected": f"{prefix}_connected",
        "authorized": f"{prefix}_authorized",
        "callback_pending": f"{prefix}_callback_pending",
    }


def mask_oauth_state(value: Any) -> str:
    """Return a short non-secret representation of the OAuth state token."""
    text = str(value or "").strip()
    if not text:
        return "missing"
    if len(text) <= 10:
        return text
    return f"{text[:6]}...{text[-4:]}"


def resolve_runtime_app_config(app: Flask, base_config: AppConfig) -> AppConfig:
    """Merge Flask app overrides into the cached environment config for runtime composition."""
    config_payload = asdict(base_config)
    app_config_map = {
        "RUNTIME_TARGET": "runtime_target",
        "SUPABASE_URL": "supabase_url",
        "SUPABASE_PUBLISHABLE_KEY": "supabase_publishable_key",
        "SUPABASE_SECRET_KEY": "supabase_secret_key",
        "APP_HOST": "app_host",
        "APP_PORT": "app_port",
        "APP_DISPLAY_NAME": "app_display_name",
        "APP_PAGE_KICKER": "app_page_kicker",
        "APP_VERSION_LABEL": "app_version_label",
        "SESSION_COOKIE_NAME": "session_cookie_name",
        "OAUTH_SESSION_NAMESPACE": "oauth_session_namespace",
        "KAIROS_REPLAY_STORAGE_DIR": "kairos_replay_storage_dir",
        "APP_LOG_PATH": "app_log_path",
        "MARKET_DATA_PROVIDER": "market_data_provider",
        "MARKET_DATA_LIVE_PROVIDER": "market_data_live_provider",
        "VIX_HISTORICAL_PROVIDER": "vix_historical_provider",
        "SPX_HISTORICAL_PROVIDER": "spx_historical_provider",
        "APP_TIMEZONE": "app_timezone",
        "APOLLO_ENABLED": "apollo_enabled",
        "APOLLO_STRUCTURE_SOURCE": "apollo_structure_source",
        "APOLLO_STRUCTURE_FALLBACK_SOURCE": "apollo_structure_fallback_source",
        "APOLLO_OPTION_CHAIN_SOURCE": "apollo_option_chain_source",
        "MACRO_PROVIDER": "macro_provider",
        "APOLLO_ACCOUNT_VALUE": "apollo_account_value",
        "APOLLO_ROUTINE_LOSS_MODIFIER": "apollo_routine_loss_modifier",
        "FLASK_SECRET_KEY": "flask_secret_key",
        "SCHWAB_CLIENT_ID": "schwab_client_id",
        "SCHWAB_CLIENT_SECRET": "schwab_client_secret",
        "SCHWAB_REDIRECT_URI": "schwab_redirect_uri",
        "SCHWAB_AUTH_URL": "schwab_auth_url",
        "SCHWAB_TOKEN_URL": "schwab_token_url",
        "SCHWAB_BASE_URL": "schwab_base_url",
        "SCHWAB_TOKEN_PATH": "schwab_token_path",
        "SCHWAB_ES_PRIMARY_SYMBOL": "schwab_es_primary_symbol",
        "SCHWAB_ES_FALLBACK_SYMBOL": "schwab_es_fallback_symbol",
        "SCHWAB_SPX_OPTION_CHAIN_SYMBOL": "schwab_spx_option_chain_symbol",
        "SCHWAB_HISTORY_PERIOD_TYPE": "schwab_history_period_type",
        "SCHWAB_HISTORY_PERIOD": "schwab_history_period",
        "SCHWAB_HISTORY_FREQUENCY_TYPE": "schwab_history_frequency_type",
        "SCHWAB_HISTORY_FREQUENCY": "schwab_history_frequency",
        "SCHWAB_HISTORY_NEED_EXTENDED_HOURS": "schwab_history_need_extended_hours",
        "PUSHOVER_USER_KEY": "pushover_user_key",
        "PUSHOVER_API_TOKEN": "pushover_api_token",
    }
    for app_key, config_key in app_config_map.items():
        if app_key in app.config and app.config.get(app_key) is not None:
            config_payload[config_key] = app.config.get(app_key)

    configured_redirect_uri = str(config_payload.get("schwab_redirect_uri") or "").strip()
    if configured_redirect_uri:
        config_payload["schwab_redirect_uri"] = configured_redirect_uri
    else:
        config_payload["schwab_redirect_uri"] = LOCAL_SCHWAB_REDIRECT_URI

    return AppConfig(**config_payload)


def get_runtime_app_config(app: Optional[Flask] = None) -> AppConfig:
    container = _resolve_flask_container(app)
    runtime_config = container.extensions.get("runtime_app_config")
    if runtime_config is None:
        runtime_config = resolve_runtime_app_config(container, APP_CONFIG)
        container.extensions["runtime_app_config"] = runtime_config
    return runtime_config


def build_runtime_app_identity(app: Optional[Flask] = None) -> Dict[str, str]:
    runtime_config = get_runtime_app_config(app)
    container = _resolve_flask_container(app)
    return {
        "display_name": str(container.config.get("APP_DISPLAY_NAME") or runtime_config.app_display_name),
        "page_kicker": str(container.config.get("APP_PAGE_KICKER") or runtime_config.app_page_kicker),
        "version_label": str(container.config.get("APP_VERSION_LABEL") or runtime_config.app_version_label),
        "session_cookie_name": str(container.config.get("SESSION_COOKIE_NAME") or runtime_config.session_cookie_name),
    }


def create_app(test_config: Optional[Dict[str, Any]] = None) -> Flask:
    """Application factory."""
    app = Flask(__name__, instance_relative_config=True)
    app.config["RUNTIME_TARGET"] = "local"
    if test_config:
        normalized_test_config = dict(test_config)
        normalized_test_config.setdefault("RUNTIME_TARGET", "local")
        app.config.update(normalized_test_config)
    runtime_app_config = resolve_runtime_app_config(app, APP_CONFIG)
    host_infrastructure_assembler = select_host_infrastructure_assembler(app, runtime_app_config)
    host_infrastructure = host_infrastructure_assembler.assemble(app)
    configure_logging(app, host_infrastructure)
    runtime_app_config = resolve_runtime_app_config(app, APP_CONFIG)
    runtime_profile = select_runtime_profile(app, runtime_app_config)
    launch_behavior = WebBrowserLaunchBehavior()
    lifecycle_coordinator = LocalRuntimeLifecycleCoordinator(
        runtime_profile,
        launch_behavior=launch_behavior,
        scheduler=ThreadingTimerScheduler(),
        shutdown_registrar=__import__("atexit").register,
    )
    auth_composer = LocalAuthComposer(runtime_app_config, token_path=host_infrastructure.storage.schwab_token_path)
    provider_composer = LocalProviderComposer(runtime_app_config, auth_composer)
    service_composer = LocalRuntimeServiceComposer(
        runtime_app_config,
        host_infrastructure=host_infrastructure,
        auth_composer=auth_composer,
        provider_composer=provider_composer,
        trade_prefill_key=APOLLO_PREFILL_SESSION_KEY,
        trade_close_prefill_key=MANAGEMENT_CLOSE_PREFILL_SESSION_KEY,
        trade_form_fields=TRADE_FORM_FIELDS,
        trade_mode_resolver=resolve_trade_mode,
    )
    service_bundle = service_composer.compose(app)
    market_data_service = service_bundle.market_data_service
    apollo_service = service_bundle.apollo_service
    apollo_snapshot_repository = service_bundle.apollo_snapshot_repository
    runtime_scheduler = service_bundle.runtime_scheduler
    trade_store_backend = service_bundle.trade_store_backend
    trade_store = service_bundle.trade_store
    import_preview_repository = service_bundle.import_preview_repository
    workflow_state = service_bundle.workflow_state
    request_identity_resolver = service_bundle.request_identity_resolver
    pushover_service = service_bundle.pushover_service
    notification_delivery = service_bundle.notification_delivery
    performance_service = service_bundle.performance_service
    performance_engine = service_bundle.performance_engine
    open_trade_manager = service_bundle.open_trade_manager
    trade_notification_repository = build_trade_notification_repository(app, trade_store)
    trade_notification_repository.initialize()
    global_notification_settings_repository = build_global_notification_settings_repository(app, trade_store)
    global_notification_settings_repository.initialize()
    open_trade_manager.trade_notification_repository = trade_notification_repository
    open_trade_manager.global_notification_settings_repository = global_notification_settings_repository
    app.extensions["auth_composer"] = auth_composer
    app.extensions["host_infrastructure_assembler"] = host_infrastructure_assembler
    app.extensions["host_infrastructure"] = host_infrastructure
    app.extensions["runtime_app_config"] = runtime_app_config
    app.extensions["supabase_integration"] = host_infrastructure.supabase_integration
    app.extensions["supabase_context"] = host_infrastructure.supabase_context
    app.extensions["provider_composer"] = provider_composer
    app.extensions["service_composer"] = service_composer
    app.extensions["service_bundle"] = service_bundle
    app.extensions["trade_store"] = trade_store
    app.extensions["trade_store_backend"] = trade_store_backend
    app.extensions["market_data_service"] = market_data_service
    app.extensions["apollo_service"] = apollo_service
    app.extensions["apollo_snapshot_repository"] = apollo_snapshot_repository
    app.extensions["global_notification_settings_repository"] = global_notification_settings_repository
    app.extensions["import_preview_repository"] = import_preview_repository
    app.extensions["workflow_state"] = workflow_state
    app.extensions["request_identity_resolver"] = request_identity_resolver
    app.extensions["runtime_scheduler"] = runtime_scheduler
    app.extensions["notification_delivery"] = notification_delivery
    app.extensions["runtime_profile"] = runtime_profile
    app.extensions["launch_behavior"] = launch_behavior
    app.extensions["runtime_lifecycle"] = lifecycle_coordinator
    app.extensions["performance_service"] = performance_service
    app.extensions["performance_engine"] = performance_engine
    app.extensions["open_trade_manager"] = open_trade_manager
    app.extensions["strategy_snapshots"] = getattr(
        service_bundle,
        "strategy_snapshots",
        {"apollo": apollo_snapshot_repository},
    )
    app.extensions["trade_notification_repository"] = trade_notification_repository
    app.extensions["pushover_service"] = pushover_service
    app.extensions["oauth_session_keys"] = build_oauth_session_keys(app.config["OAUTH_SESSION_NAMESPACE"])
    runtime_components = service_bundle.runtime_components
    for component in runtime_components:
        lifecycle_coordinator.register_component(component)
    app.extensions["runtime_components"] = runtime_components

    lifecycle_coordinator.start_runtime()

    @app.before_request
    def resolve_request_identity_for_runtime() -> None:
        g.request_identity = get_request_identity_resolver(app).resolve_request_identity(request)

    @app.before_request
    def disable_removed_kairos_surface() -> None:
        path = request.path.rstrip("/") or "/"
        if path == "/kairos" or path.startswith("/kairos/"):
            abort(404)

    @app.context_processor
    def inject_universal_header_status() -> Dict[str, Any]:
        return {
            "menu_status": build_startup_menu_payload(
                market_data_service,
                snapshot_overrides=getattr(g, "startup_menu_snapshot_overrides", None),
            ),
            "delphi_routes": build_delphi_route_map(),
            "app_identity": build_runtime_app_identity(app),
            "request_identity": get_request_identity(),
        }

    @app.route("/", methods=["GET", "POST"])
    def index() -> str:
        return open_trade_management_page()

    def render_apollo_page(
        *,
        form_data: Dict[str, str],
        result: Optional[Dict[str, Any]],
        apollo_result: Optional[Dict[str, Any]],
        error_message: Optional[str],
        info_message: Optional[Dict[str, str]],
        diagnostic_message: Optional[str],
        active_page: str,
        page_browser_title: str,
        page_heading: str,
        page_copy: str,
        **template_context: Any,
    ) -> str:
        return render_template(
            "index.html",
            form_data=form_data,
            result=result,
            apollo_result=apollo_result,
            error_message=error_message,
            info_message=info_message,
            diagnostic_message=diagnostic_message,
            provider_meta=market_data_service.get_provider_metadata(),
            active_page=active_page,
            page_browser_title=page_browser_title,
            page_kicker=runtime_app_config.app_page_kicker,
            page_heading=page_heading,
            page_copy=page_copy,
            **template_context,
        )

    @app.post("/api/text-status")
    def text_status_api() -> Any:
        display_name = str(app.config.get("APP_DISPLAY_NAME") or APP_CONFIG.app_display_name or "Talos").strip() or "Talos"
        title_display_name = display_name[:-4] if display_name.endswith(" Dev") else display_name
        title = f"{title_display_name} Test Alert"
        if not open_trade_manager.notifications_enabled():
            return jsonify({"ok": False, "error": "Notifications are currently OFF.", "title": title}), 409
        status_timestamp = datetime.now(CHICAGO_TZ)
        spx_snapshot = get_status_snapshot(market_data_service, "^GSPC", query_type="pushover_status_spx")
        vix_snapshot = get_status_snapshot(market_data_service, "^VIX", query_type="pushover_status_vix")
        message = build_pushover_test_message(
            spx_snapshot=spx_snapshot,
            vix_snapshot=vix_snapshot,
            generated_at=status_timestamp,
            source_label=display_name,
        )
        result = pushover_service.send_notification(title=title, message=message, priority=0)
        status_code = int(result.get("status_code") or 200)
        if result.get("ok"):
            app.logger.info("Manual %s Pushover test notification sent.", display_name)
        else:
            app.logger.warning(
                "Manual %s Pushover test notification failed: %s",
                display_name,
                result.get("error") or "Unknown error",
            )
        response_payload = {
            key: value for key, value in result.items() if key not in {"status_code"}
        }
        response_payload["title"] = title
        response_payload["message_body"] = message
        return jsonify(response_payload), status_code

    @app.route("/apollo", methods=["GET", "POST"])
    def run_apollo():
        form_data = get_form_data(request.form if request.method == "POST" else None)
        error_message = None
        apollo_result = None
        diagnostic_message = None
        info_message = pop_status_message()
        trigger_source = get_apollo_trigger_source()

        if trigger_source:
            try:
                apollo_result = execute_apollo_precheck(apollo_service, trigger_source=trigger_source)
                if apollo_result is not None:
                    save_apollo_snapshot(apollo_result, app=app)
                    app.logger.info("Completed Apollo pre-check via %s", trigger_source)
            except MarketDataReauthenticationRequired as exc:
                set_status_message(str(exc), level="warning")
                app.logger.warning("Provider session expired during Apollo run: %s", exc)
                return redirect(url_for("login"))
            except MarketDataAuthenticationError as exc:
                error_message = str(exc)
                app.logger.warning("Provider login required during Apollo run: %s", exc)
            except MarketDataError as exc:
                error_message = str(exc)
                app.logger.warning("Apollo market data error: %s", exc)
            except Exception as exc:  # pragma: no cover - defensive logging
                error_message = "An unexpected error occurred while running Apollo. Check the log for details."
                app.logger.exception("Unexpected Apollo error: %s", exc)

        response = render_apollo_page(
            form_data=form_data,
            result=None,
            apollo_result=apollo_result,
            error_message=error_message,
            info_message=info_message,
            diagnostic_message=diagnostic_message,
            active_page="apollo",
            page_browser_title="Apollo | Talos",
            page_heading="Apollo",
            page_copy="Talos Apollo workflow for live structure, macro review, and next-market-day candidate selection.",
        )
        return response

    @app.route("/debug/run-apollo", methods=["GET", "POST"])
    def debug_run_apollo():
        if not is_local_dev_request():
            return ("Not Found", 404)

        try:
            apollo_result = execute_apollo_precheck(apollo_service, trigger_source="autorun URL")
            save_apollo_snapshot(apollo_result, app=app)
            app.logger.info("Completed Apollo debug pre-check")
        except MarketDataReauthenticationRequired as exc:
            app.logger.warning("Provider session expired during Apollo debug run: %s", exc)
            return jsonify({"ok": False, "error": str(exc), "requires_login": True}), 401
        except MarketDataAuthenticationError as exc:
            app.logger.warning("Provider login required during Apollo debug run: %s", exc)
            return jsonify({"ok": False, "error": str(exc), "requires_login": True}), 401
        except MarketDataError as exc:
            app.logger.warning("Apollo market data error during debug run: %s", exc)
            return jsonify({"ok": False, "error": str(exc)}), 400
        except Exception as exc:  # pragma: no cover - defensive logging
            app.logger.exception("Unexpected Apollo debug error: %s", exc)
            return jsonify({"ok": False, "error": "Unexpected Apollo debug error."}), 500

        if request.args.get("format", "json").strip().lower() == "html":
            return render_apollo_page(
                form_data=get_form_data(None),
                result=None,
                apollo_result=apollo_result,
                error_message=None,
                info_message=None,
                diagnostic_message=None,
                active_page="apollo",
                page_browser_title="Apollo | Talos",
                page_heading="Apollo",
                page_copy="Talos Apollo workflow for live structure, macro review, and next-market-day candidate selection.",
            )

        return jsonify(
            {
                "ok": True,
                "trigger_source": apollo_result.get("apollo_trigger_source", "autorun URL"),
                "title": apollo_result.get("title"),
                "status": apollo_result.get("status"),
                "structure_grade": apollo_result.get("structure_grade"),
                "macro_grade": apollo_result.get("macro_grade"),
                "trade_candidates_count": apollo_result.get("trade_candidates_count"),
                "trigger_note": apollo_result.get("apollo_trigger_note"),
            }
        )

    @app.get("/debug/option-chain")
    def debug_option_chain():
        provider = market_data_service.live_provider
        if not hasattr(provider, "debug_option_chain_request"):
            return "Option-chain debug is unavailable for the active provider.", 400

        try:
            payload = provider.debug_option_chain_request("^GSPC", target_date=date(2026, 4, 6), minimal_only=True)
            app.logger.info("Schwab option-chain debug payload: %s", json.dumps(payload, default=str))
            return f"<pre>{json.dumps(payload, default=str, indent=2)}</pre>"
        except MarketDataReauthenticationRequired as exc:
            app.logger.warning("Provider session expired during option-chain debug: %s", exc)
            return f"<pre>{exc}</pre>", 401
        except MarketDataAuthenticationError as exc:
            app.logger.warning("Provider login required during option-chain debug: %s", exc)
            return f"<pre>{exc}</pre>", 401
        except Exception as exc:  # pragma: no cover - defensive logging
            app.logger.exception("Unexpected option-chain debug error: %s", exc)
            return f"<pre>{exc}</pre>", 500

    @app.get("/login")
    def login():
        provider = market_data_service.provider
        if not hasattr(provider, "auth_service"):
            set_status_message("The active market data provider does not use OAuth login.", level="info")
            return redirect(url_for("index"))

        auth_service = provider.auth_service
        oauth_session_keys = app.extensions["oauth_session_keys"]
        workflow_state = get_workflow_state(app)
        state = auth_service.build_state_token()
        workflow_state.pop("schwab_oauth_state", None)
        workflow_state.put(oauth_session_keys["oauth_state"], state)
        workflow_state.put(oauth_session_keys["pkce_verifier"], "")
        workflow_state.put(oauth_session_keys["login_in_progress"], True)
        workflow_state.put(oauth_session_keys["callback_pending"], True)
        workflow_state.put(oauth_session_keys["connected"], False)
        workflow_state.put(oauth_session_keys["authorized"], False)
        app.logger.info(
            "OAuth state created | env=%s | port=%s | redirect_uri=%s | token_target_path=%s | session_cookie=%s | oauth_namespace=%s | oauth_state=%s",
            runtime_app_config.app_display_name,
            runtime_app_config.app_port,
            runtime_app_config.schwab_redirect_uri,
            getattr(auth_service.token_store, "file_path", runtime_app_config.schwab_token_path),
            runtime_app_config.session_cookie_name,
            runtime_app_config.oauth_session_namespace,
            mask_oauth_state(state),
        )

        try:
            authorize_url = auth_service.build_authorization_url(state=state)
            app.logger.info("Schwab authorize URL | %s", authorize_url)
            return redirect(authorize_url)
        except Exception as exc:
            workflow_state.put(oauth_session_keys["login_in_progress"], False)
            workflow_state.put(oauth_session_keys["callback_pending"], False)
            set_status_message(str(exc), level="error")
            app.logger.warning("Unable to build Schwab authorization URL: %s", exc)
            return redirect(url_for("index"))

    @app.get("/callback")
    def callback():
        provider = market_data_service.provider
        if not hasattr(provider, "auth_service"):
            set_status_message("The active market data provider does not support OAuth callbacks.", level="error")
            return redirect(url_for("index"))

        oauth_session_keys = app.extensions["oauth_session_keys"]
        workflow_state = get_workflow_state(app)
        received_state = request.args.get("state")
        authorization_code = request.args.get("code")
        app.logger.info(
            "OAuth callback received | env=%s | port=%s | redirect_uri=%s | token_target_path=%s | session_cookie=%s | oauth_namespace=%s | has_code=%s | oauth_state=%s",
            runtime_app_config.app_display_name,
            runtime_app_config.app_port,
            runtime_app_config.schwab_redirect_uri,
            getattr(provider.auth_service.token_store, "file_path", runtime_app_config.schwab_token_path),
            runtime_app_config.session_cookie_name,
            runtime_app_config.oauth_session_namespace,
            bool(authorization_code),
            mask_oauth_state(received_state),
        )

        error = request.args.get("error")
        if error:
            workflow_state.put(oauth_session_keys["login_in_progress"], False)
            workflow_state.put(oauth_session_keys["callback_pending"], False)
            set_status_message(f"Schwab authorization failed: {error}", level="error")
            return redirect(url_for("index"))

        expected_state = workflow_state.pop(oauth_session_keys["oauth_state"], None)
        workflow_state.pop("schwab_oauth_state", None)
        if not expected_state or received_state != expected_state:
            workflow_state.put(oauth_session_keys["login_in_progress"], False)
            workflow_state.put(oauth_session_keys["callback_pending"], False)
            workflow_state.put(oauth_session_keys["connected"], False)
            workflow_state.put(oauth_session_keys["authorized"], False)
            workflow_state.pop(oauth_session_keys["pkce_verifier"], None)
            app.logger.warning(
                "OAuth state validation failed | env=%s | port=%s | redirect_uri=%s | token_target_path=%s | oauth_namespace=%s | expected_state=%s | received_state=%s",
                runtime_app_config.app_display_name,
                runtime_app_config.app_port,
                runtime_app_config.schwab_redirect_uri,
                getattr(provider.auth_service.token_store, "file_path", runtime_app_config.schwab_token_path),
                runtime_app_config.oauth_session_namespace,
                mask_oauth_state(expected_state),
                mask_oauth_state(received_state),
            )
            set_status_message("Schwab authorization state did not match. Please try again.", level="error")
            return redirect(url_for("index"))
        app.logger.info(
            "OAuth state validation passed | env=%s | port=%s | redirect_uri=%s | token_target_path=%s | oauth_namespace=%s | oauth_state=%s",
            runtime_app_config.app_display_name,
            runtime_app_config.app_port,
            runtime_app_config.schwab_redirect_uri,
            getattr(provider.auth_service.token_store, "file_path", runtime_app_config.schwab_token_path),
            runtime_app_config.oauth_session_namespace,
            mask_oauth_state(received_state),
        )

        if not authorization_code:
            workflow_state.put(oauth_session_keys["login_in_progress"], False)
            workflow_state.put(oauth_session_keys["callback_pending"], False)
            set_status_message("Schwab did not return an authorization code.", level="error")
            return redirect(url_for("index"))

        try:
            provider.auth_service.exchange_code_for_tokens(authorization_code)
            workflow_state.put(oauth_session_keys["login_in_progress"], False)
            workflow_state.put(oauth_session_keys["callback_pending"], False)
            workflow_state.put(oauth_session_keys["connected"], True)
            workflow_state.put(oauth_session_keys["authorized"], True)
            workflow_state.pop(oauth_session_keys["pkce_verifier"], None)
            set_status_message("Connected to Schwab successfully.", level="info")
        except Exception as exc:
            workflow_state.put(oauth_session_keys["login_in_progress"], False)
            workflow_state.put(oauth_session_keys["callback_pending"], False)
            workflow_state.put(oauth_session_keys["connected"], False)
            workflow_state.put(oauth_session_keys["authorized"], False)
            set_status_message(str(exc), level="error")
            app.logger.warning("Schwab token exchange failed: %s", exc)

        return redirect(url_for("index"))

    @app.get("/journal")
    def journal_dashboard() -> Any:
        return trade_dashboard("real")

    @app.get("/trades/<trade_mode>")
    def trade_dashboard(trade_mode: str):
        normalized_mode = resolve_trade_mode(trade_mode)
        prefill_requested = str(request.args.get("prefill", "")).strip().lower() in {"1", "true", "yes", "on"}
        form_values = blank_trade_form(normalized_mode)
        form_values["trade_number"] = str(trade_store.next_trade_number())
        form_title = "Add Manual Trade"
        prefill_active = False
        prefill_notice = ""

        if prefill_requested:
            draft_values = get_trade_prefill(normalized_mode)
            if draft_values:
                form_values = merge_trade_form_values(form_values, draft_values)
                form_values["trade_number"] = form_values.get("trade_number") or str(trade_store.next_trade_number())
                prefill_meta = get_trade_prefill_metadata(form_values)
                form_title = prefill_meta["title"]
                prefill_notice = prefill_meta["notice"]
                prefill_active = True

        return render_template(
            "trades.html",
            **build_trade_page_context(
                store=trade_store,
                trade_mode=normalized_mode,
                form_values=form_values,
                form_action=url_for("trade_create", trade_mode=normalized_mode),
                form_title=form_title,
                editing_trade_id=None,
                error_message=None,
                info_message=pop_status_message(),
                prefill_active=prefill_active,
                prefill_notice=prefill_notice,
            ),
        )

    @app.route("/trades/<trade_mode>/new", methods=["GET", "POST"])
    def trade_create(trade_mode: str):
        normalized_mode = resolve_trade_mode(trade_mode)
        if request.method == "GET":
            return redirect(url_for("trade_dashboard", trade_mode=normalized_mode, _anchor="trade-entry-form"))

        if request.method == "POST":
            submitted_values = coerce_trade_form_input(request.form)
            try:
                if is_apollo_prefill_submission(submitted_values):
                    duplicate = trade_store.find_recent_duplicate(submitted_values, window_seconds=15)
                    if duplicate:
                        clear_trade_prefill(normalized_mode)
                        set_status_message("Apollo draft already saved recently. Review the existing trade instead of submitting twice.", level="warning")
                        return redirect(url_for("trade_dashboard", trade_mode=normalized_mode))

                trade_id = trade_store.create_trade(submitted_values)
                created_trade = trade_store.get_trade(trade_id) or {"trade_mode": normalized_mode}
                redirect_mode = str(created_trade.get("trade_mode", normalized_mode))
                clear_trade_prefill(redirect_mode)
                save_message, save_level = build_trade_save_status(created_trade, action="saved")
                set_status_message(save_message, level=save_level)
                return redirect(url_for("trade_dashboard", trade_mode=redirect_mode))
            except ValueError as exc:
                return render_template(
                    "trades.html",
                    **build_trade_page_context(
                        store=trade_store,
                        trade_mode=normalized_mode,
                        form_values=submitted_values,
                        form_action=url_for("trade_create", trade_mode=normalized_mode),
                        form_title=(get_trade_prefill_metadata(submitted_values)["title"] if str(submitted_values.get("prefill_source") or "").strip() else "Add Manual Trade"),
                        editing_trade_id=None,
                        error_message=str(exc),
                        info_message=pop_status_message(),
                        prefill_active=bool(str(submitted_values.get("prefill_source") or "").strip()),
                        prefill_notice=get_trade_prefill_metadata(submitted_values)["notice"] if str(submitted_values.get("prefill_source") or "").strip() else "",
                    ),
                )

    @app.route("/trades/<trade_mode>/<int:trade_id>/edit", methods=["GET", "POST"])
    def trade_edit(trade_mode: str, trade_id: int):
        normalized_mode = resolve_trade_mode(trade_mode)
        trade = trade_store.get_trade(trade_id)
        if not trade:
            set_status_message("Trade not found.", level="error")
            return redirect(url_for("trade_dashboard", trade_mode=normalized_mode))

        if request.method == "POST":
            submitted_values = coerce_trade_form_input(request.form)
            submitted_close_events = coerce_trade_close_event_input(request.form)
            if submitted_close_events is not None:
                submitted_values["close_events"] = submitted_close_events
            try:
                trade_store.update_trade(trade_id, submitted_values)
                updated_trade = trade_store.get_trade(trade_id) or trade
                redirect_mode = str(updated_trade.get("trade_mode", normalized_mode))
                clear_trade_close_prefill(trade_id)
                save_message, save_level = build_trade_save_status(updated_trade, action="updated")
                set_status_message(save_message, level=save_level)
                return redirect(url_for("trade_dashboard", trade_mode=redirect_mode))
            except ValueError as exc:
                return render_template(
                    "trades.html",
                    **build_trade_page_context(
                        store=trade_store,
                        trade_mode=normalized_mode,
                        form_values=submitted_values,
                        form_action=url_for("trade_edit", trade_mode=normalized_mode, trade_id=trade_id),
                        form_title=f"Edit Trade #{trade.get('trade_number') or trade_id}",
                        editing_trade_id=trade_id,
                        editing_trade=build_edit_trade_preview(trade, submitted_values, submitted_close_events),
                        error_message=str(exc),
                        info_message=pop_status_message(),
                        prefill_active=False,
                    ),
                )

        edit_mode = str(trade.get("trade_mode") or normalized_mode)
        close_prefill = get_trade_close_prefill(trade_id)
        editing_trade = apply_trade_close_prefill(trade, close_prefill)
        return render_template(
            "trades.html",
            **build_trade_page_context(
                store=trade_store,
                trade_mode=edit_mode,
                form_values=form_trade_record(trade),
                form_action=url_for("trade_edit", trade_mode=edit_mode, trade_id=trade_id),
                form_title=f"Edit Trade #{trade.get('trade_number') or trade_id}",
                editing_trade_id=trade_id,
                editing_trade=editing_trade,
                error_message=None,
                info_message=pop_status_message(),
                prefill_active=False,
            ),
        )

    @app.post("/trades/<trade_mode>/<int:trade_id>/delete")
    def trade_delete(trade_mode: str, trade_id: int):
        normalized_mode = resolve_trade_mode(trade_mode)
        trade = trade_store.get_trade(trade_id)
        if trade:
            trade_store.delete_trade(trade_id)
            set_status_message(f"Deleted trade #{trade.get('trade_number') or trade_id}.", level="info")
            return redirect(url_for("trade_dashboard", trade_mode=str(trade.get("trade_mode") or normalized_mode)))

        set_status_message("Trade not found.", level="error")
        return redirect(url_for("trade_dashboard", trade_mode=normalized_mode))

    @app.post("/trades/<trade_mode>/<int:trade_id>/reduce")
    def trade_reduce(trade_mode: str, trade_id: int):
        normalized_mode = resolve_trade_mode(trade_mode)
        trade = trade_store.get_trade(trade_id)
        if not trade:
            set_status_message("Trade not found.", level="error")
            return redirect(url_for("trade_dashboard", trade_mode=normalized_mode, _anchor="trade-log-table"))

        try:
            trade_store.reduce_trade(
                trade_id,
                {
                    "contracts_closed": request.form.get("contracts_closed"),
                    "actual_exit_value": request.form.get("actual_exit_value"),
                    "event_datetime": request.form.get("event_datetime"),
                    "spx_at_exit": request.form.get("spx_at_exit"),
                    "close_method": request.form.get("close_method") or "Reduce",
                    "close_reason": request.form.get("close_reason") or "Partial reduction",
                    "notes_exit": request.form.get("notes_exit") or "",
                },
            )
            updated_trade = trade_store.get_trade(trade_id) or trade
            set_status_message(
                f"Reduced trade #{updated_trade.get('trade_number') or trade_id}.",
                level="info",
            )
            return redirect(url_for("trade_dashboard", trade_mode=str(updated_trade.get("trade_mode") or normalized_mode), _anchor="trade-log-table"))
        except ValueError as exc:
            set_status_message(str(exc), level="error")
            return redirect(url_for("trade_dashboard", trade_mode=str(trade.get("trade_mode") or normalized_mode), _anchor="trade-log-table"))

    @app.post("/trades/<trade_mode>/<int:trade_id>/expire")
    def trade_expire(trade_mode: str, trade_id: int):
        normalized_mode = resolve_trade_mode(trade_mode)
        trade = trade_store.get_trade(trade_id)
        if not trade:
            set_status_message("Trade not found.", level="error")
            return redirect(url_for("trade_dashboard", trade_mode=normalized_mode, _anchor="trade-log-table"))

        try:
            trade_store.expire_trade(
                trade_id,
                {
                    "event_datetime": request.form.get("event_datetime"),
                    "actual_exit_value": request.form.get("actual_exit_value") or 0,
                    "spx_at_exit": request.form.get("spx_at_exit"),
                    "close_method": request.form.get("close_method") or "Expire",
                    "close_reason": request.form.get("close_reason") or "Expired Worthless",
                    "notes_exit": request.form.get("notes_exit") or "",
                },
            )
            updated_trade = trade_store.get_trade(trade_id) or trade
            set_status_message(
                f"Expired trade #{updated_trade.get('trade_number') or trade_id}.",
                level="info",
            )
            return redirect(url_for("trade_dashboard", trade_mode=str(updated_trade.get("trade_mode") or normalized_mode), _anchor="trade-log-table"))
        except ValueError as exc:
            set_status_message(str(exc), level="error")
            return redirect(url_for("trade_dashboard", trade_mode=str(trade.get("trade_mode") or normalized_mode), _anchor="trade-log-table"))

    @app.post("/trades/<trade_mode>/import/preview")
    def trade_import_preview(trade_mode: str):
        normalized_mode = resolve_trade_mode(trade_mode)
        form_values = blank_trade_form(normalized_mode)
        upload = request.files.get("import_file")
        import_journal_name = str(request.form.get("import_journal_name") or JOURNAL_NAME_DEFAULT).strip() or JOURNAL_NAME_DEFAULT

        try:
            preview_source = parse_trade_import(upload, trade_mode=normalized_mode, journal_name=import_journal_name)
            import_preview = build_trade_import_preview(store=trade_store, preview_source=preview_source)
            import_token = store_trade_import_preview(
                app,
                trade_mode=normalized_mode,
                preview_payload={
                    "importable_rows": import_preview["importable_rows"],
                    "file_name": import_preview["file_name"],
                    "journal_name": import_preview["journal_name"],
                    "duplicate_count": import_preview["duplicate_count"],
                },
            )
            import_preview["token"] = import_token
            return render_template(
                "trades.html",
                **build_trade_page_context(
                    store=trade_store,
                    trade_mode=normalized_mode,
                    form_values=form_values,
                    form_action=url_for("trade_create", trade_mode=normalized_mode),
                    form_title="Add Manual Trade",
                    editing_trade_id=None,
                    error_message=None,
                    info_message=pop_status_message(),
                    prefill_active=False,
                    import_preview=import_preview,
                    import_journal_name=import_journal_name,
                ),
            )
        except ValueError as exc:
            return render_template(
                "trades.html",
                **build_trade_page_context(
                    store=trade_store,
                    trade_mode=normalized_mode,
                    form_values=form_values,
                    form_action=url_for("trade_create", trade_mode=normalized_mode),
                    form_title="Add Manual Trade",
                    editing_trade_id=None,
                    error_message=str(exc),
                    info_message=pop_status_message(),
                    prefill_active=False,
                    import_preview=None,
                    import_journal_name=import_journal_name,
                ),
            )

    @app.post("/trades/<trade_mode>/import/confirm")
    def trade_import_confirm(trade_mode: str):
        normalized_mode = resolve_trade_mode(trade_mode)
        import_token = str(request.form.get("import_token") or "").strip()
        stored_preview = load_trade_import_preview(app, import_token)
        if not stored_preview or stored_preview.get("trade_mode") != normalized_mode:
            set_status_message("Trade import preview expired. Upload the file again before importing.", level="warning")
            return redirect(url_for("trade_dashboard", trade_mode=normalized_mode))

        imported_count = 0
        skipped_duplicates = int(stored_preview.get("duplicate_count") or 0)
        seen_signatures: set[tuple[Any, ...]] = set()
        for payload in stored_preview.get("importable_rows", []):
            signature = build_trade_duplicate_signature(payload, already_normalized=True)
            if signature in seen_signatures or trade_store.find_duplicate_trade(payload):
                skipped_duplicates += 1
                continue
            trade_store.create_trade(payload)
            seen_signatures.add(signature)
            imported_count += 1

        delete_trade_import_preview(app, import_token)
        if imported_count:
            summary = f"Imported {imported_count} trade{'s' if imported_count != 1 else ''}."
            if skipped_duplicates:
                summary += f" Skipped {skipped_duplicates} duplicate{'s' if skipped_duplicates != 1 else ''}."
            set_status_message(summary, level="info")
        else:
            set_status_message("No new trades were imported. All preview rows were duplicates or unavailable.", level="warning")
        return redirect(url_for("trade_dashboard", trade_mode=normalized_mode))

    @app.post("/trades/<trade_mode>/import/cancel")
    def trade_import_cancel(trade_mode: str):
        normalized_mode = resolve_trade_mode(trade_mode)
        delete_trade_import_preview(app, str(request.form.get("import_token") or "").strip())
        set_status_message("Trade import preview cleared.", level="info")
        return redirect(url_for("trade_dashboard", trade_mode=normalized_mode))

    @app.post("/apollo/prefill-candidate")
    def apollo_prefill_candidate():
        target_mode = resolve_trade_mode(request.form.get("target_mode") or request.form.get("trade_mode") or "simulated")
        draft_values = coerce_apollo_trade_input(request.form, trade_mode=target_mode)
        store_trade_prefill(target_mode, draft_values)
        set_status_message(
            f"Apollo {draft_values.get('candidate_profile') or 'Candidate'} sent to {TRADE_MODE_LABELS[target_mode]}. Review the draft, then click Save Trade to commit it.",
            level="info",
        )
        return redirect(url_for("trade_dashboard", trade_mode=target_mode, prefill=1, _anchor="trade-entry-form"))

    @app.get("/performance")
    def performance_dashboard() -> str:
        dashboard_payload = performance_service.build_dashboard()
        return render_template(
            "performance.html",
            dashboard_payload=dashboard_payload,
            filter_groups=PERFORMANCE_FILTER_GROUPS,
            info_message=pop_status_message(),
        )

    @app.get("/management/open-trades")
    def open_trade_management_page() -> str:
        management_payload = open_trade_manager.evaluate_open_trades(send_alerts=False)
        g.startup_menu_snapshot_overrides = dict(management_payload.get("header_market_snapshots") or {})
        response = render_template(
            "open_trade_management.html",
            management_payload=management_payload,
            info_message=pop_status_message(),
            management_actions_enabled=True,
            management_action_urls={
                "real_status_update": url_for("open_trade_management_status_update", trade_mode="real"),
                "simulated_status_update": url_for("open_trade_management_status_update", trade_mode="simulated"),
                "prefill_close": "open_trade_management_prefill_close",
            },
        )
        return response

    @app.post("/management/open-trades/status-update/<trade_mode>")
    def open_trade_management_status_update(trade_mode: str) -> Any:
        normalized_trade_mode = str(trade_mode or "").strip().lower()
        if normalized_trade_mode not in {"real", "simulated"}:
            abort(404)
        result = open_trade_manager.send_manual_status_update(trade_mode=normalized_trade_mode)
        trade_mode_label = "real" if normalized_trade_mode == "real" else "simulated"
        if result["sent"]:
            suffix = " Automatic notifications remain OFF." if not open_trade_manager.notifications_enabled() else ""
            set_status_message(
                f"Sent Pushover status update for {result['record_count']} {trade_mode_label} open trade(s).{suffix}",
                level="info",
            )
        elif result["record_count"] == 0:
            set_status_message(
                f"No open {trade_mode_label} trades are available for a status update.",
                level="warning",
            )
        else:
            set_status_message(
                f"Pushover {trade_mode_label} status update failed: {result['error'] or 'Unable to send notification.'}",
                level="warning",
            )
        return redirect(url_for("open_trade_management_page"))

    @app.post("/management/open-trades/<int:trade_id>/prefill-close")
    def open_trade_management_prefill_close(trade_id: int) -> Any:
        trade = trade_store.get_trade(trade_id)
        if not trade:
            set_status_message("Trade not found.", level="error")
            return redirect(url_for("open_trade_management_page"))

        record = open_trade_manager.evaluate_trade_record(trade_id)
        if record is None:
            set_status_message("Open trade record was not available for management prefill.", level="warning")
            return redirect(url_for("open_trade_management_page"))
        if record.get("current_spread_mark") in {None, ""}:
            set_status_message("Current close cost could not be derived from the live option chain, so no journal prefill was created.", level="warning")
            return redirect(url_for("open_trade_management_page"))
        if int(record.get("contracts") or 0) <= 0:
            set_status_message("No remaining contracts were available to prefill.", level="warning")
            return redirect(url_for("open_trade_management_page"))

        store_trade_close_prefill(trade_id, build_manage_trade_close_prefill(record))
        set_status_message(
            f"Prefilled a close event for trade #{trade.get('trade_number') or trade_id}. Review the journal entry and click Save Trade to commit it.",
            level="info",
        )
        return redirect(url_for("trade_edit", trade_mode=str(trade.get("trade_mode") or "real"), trade_id=trade_id, _anchor="position-management"))

    @app.get("/performance/data")
    def performance_dashboard_data():
        filters = parse_performance_request_filters(request.args)
        return jsonify(performance_service.build_dashboard(filters=filters))

    return app


def configure_logging(app: Flask, host_infrastructure: Any | None = None) -> None:
    """Configure console and rotating-file logging."""
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    infrastructure = host_infrastructure or app.extensions.get("host_infrastructure")
    if infrastructure is not None:
        log_path = infrastructure.storage.app_log_path
    else:
        log_path = Path(APP_CONFIG.app_log_path).expanduser() if APP_CONFIG.app_log_path else Path(app.root_path) / "market_lookup.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    file_handler = RotatingFileHandler(log_path, maxBytes=1_000_000, backupCount=3, encoding="utf-8")
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    for handler in app.logger.handlers:
        handler.close()
    app.logger.handlers.clear()
    app.logger.addHandler(file_handler)
    app.logger.addHandler(stream_handler)
    app.logger.setLevel(logging.INFO)
    app.logger.propagate = False


def get_form_data(source: Optional[Any]) -> Dict[str, str]:
    """Return template-safe form state."""
    source = source or {}
    return {
        "query_type": source.get("query_type", "latest_spx"),
        "single_date": source.get("single_date", ""),
        "start_date": source.get("start_date", ""),
        "end_date": source.get("end_date", ""),
    }


def build_startup_menu_payload(
    market_data_service: MarketDataService,
    snapshot_overrides: Dict[str, Dict[str, Any]] | None = None,
) -> Dict[str, Any]:
    """Return fresh startup-menu status cards for Delphi's command hub."""
    provider_meta = market_data_service.get_provider_metadata()
    requires_auth = bool(provider_meta.get("requires_auth"))
    authenticated = bool(provider_meta.get("authenticated", True))
    notes: list[str] = []
    auth_status = {"requires_login": False, "requires_refresh": False}
    snapshot_overrides = snapshot_overrides or {}

    def _snapshot_card(label: str, ticker: str, key: str) -> Dict[str, str]:
        try:
            snapshot = snapshot_overrides.get(ticker) or market_data_service.get_latest_snapshot(ticker, query_type=f"startup_{key}")
            change_summary = build_market_change_summary(snapshot)
            return {
                "label": label,
                "value": format_value(snapshot.get("Latest Value")),
                "value_trend": change_summary["trend"],
                "change": change_summary["display"],
                "meta": format_value(snapshot.get("As Of")),
            }
        except MarketDataReauthenticationRequired:
            auth_status["requires_refresh"] = True
            notes.append(f"{label} unavailable until the Schwab session is refreshed.")
            return {
                "label": label,
                "value": "--",
                "value_trend": "neutral",
                "change": "Change unavailable",
                "meta": "Session refresh required",
            }
        except MarketDataAuthenticationError:
            auth_status["requires_login"] = True
            notes.append(f"{label} unavailable until Schwab login is completed.")
            return {
                "label": label,
                "value": "--",
                "value_trend": "neutral",
                "change": "Change unavailable",
                "meta": "Login required",
            }
        except MarketDataError as exc:
            notes.append(f"{label} unavailable: {exc}")
            return {
                "label": label,
                "value": "--",
                "value_trend": "neutral",
                "change": "Change unavailable",
                "meta": str(exc),
            }
        except Exception as exc:  # pragma: no cover - defensive rendering fallback
            notes.append(f"{label} unavailable: {exc}")
            return {
                "label": label,
                "value": "--",
                "value_trend": "neutral",
                "change": "Change unavailable",
                "meta": "Unavailable",
            }

    connection_meta = provider_meta.get("live_provider_name", "Unknown Provider")
    cards = [
        _snapshot_card("SPX", "^GSPC", "spx"),
        _snapshot_card("VIX", "^VIX", "vix"),
    ]

    connection_label = "Connected"
    if auth_status["requires_refresh"]:
        connection_label = "Session refresh required"
    elif auth_status["requires_login"] or (requires_auth and not authenticated):
        connection_label = "Login required"
    elif not requires_auth:
        connection_label = "Ready"

    cards.append(
        {
            "label": "Schwab connection",
            "value": connection_label,
            "value_trend": "connection",
            "change": "",
            "meta": connection_meta,
        }
    )

    return {
        "brand_name": "Talos",
        "connection_label": connection_label,
        "connection_meta": connection_meta,
        "connection_requires_login": auth_status["requires_login"] or (requires_auth and not authenticated),
        "cards": cards,
        "notes": notes,
    }


def build_market_change_summary(snapshot: Dict[str, Any]) -> Dict[str, str]:
    point_change = _coerce_float(snapshot.get("Daily Point Change"))
    percent_change = _coerce_float(snapshot.get("Daily Percent Change"))

    if point_change is None and percent_change is None:
        return {"trend": "neutral", "display": "Change unavailable"}

    direction_source = point_change if point_change not in (None, 0) else percent_change
    if direction_source is None or direction_source == 0:
        trend = "neutral"
    else:
        trend = "positive" if direction_source > 0 else "negative"

    segments: list[str] = []
    if point_change is not None:
        segments.append(f"{point_change:+,.2f} pts")
    if percent_change is not None:
        segments.append(f"{percent_change:+,.2f}%")

    if not segments:
        segments.append("Change unavailable")

    return {"trend": trend, "display": " | ".join(segments)}


def get_status_snapshot(market_data_service: MarketDataService, ticker: str, *, query_type: str) -> Dict[str, Any]:
    """Return a snapshot payload suitable for notification formatting without raising provider errors."""
    try:
        return market_data_service.get_latest_snapshot(ticker, query_type=query_type)
    except MarketDataReauthenticationRequired:
        return {"status_unavailable": True, "status_note": "Session refresh required"}
    except MarketDataAuthenticationError:
        return {"status_unavailable": True, "status_note": "Login required"}
    except MarketDataError as exc:
        return {"status_unavailable": True, "status_note": str(exc)}
    except Exception:
        return {"status_unavailable": True, "status_note": "Unavailable"}


def build_pushover_test_message(
    *,
    spx_snapshot: Optional[Dict[str, Any]],
    vix_snapshot: Optional[Dict[str, Any]],
    generated_at: datetime,
    source_label: str,
) -> str:
    """Build the header-button Pushover test message."""
    return "\n".join(
        [
            "SPX Update",
            f"SPX: {format_notification_snapshot(spx_snapshot)}",
            f"VIX: {format_notification_snapshot(vix_snapshot)}",
            f"Time: {generated_at.strftime('%Y-%m-%d %I:%M %p %Z').lstrip('0')}",
            f"Source: {source_label} Pushover test",
        ]
    )


def format_notification_snapshot(snapshot: Optional[Dict[str, Any]]) -> str:
    if not snapshot or snapshot.get("status_unavailable"):
        return "Unavailable"
    latest_value = snapshot.get("Latest Value")
    as_of = str(snapshot.get("As Of") or "").strip()
    if latest_value in {None, ""}:
        return "Unavailable"
    formatted_value = format_value(latest_value)
    return f"{formatted_value} ({as_of})" if as_of else formatted_value


def parse_performance_request_filters(source: Any) -> Dict[str, list[str]]:
    filters: Dict[str, list[str]] = {}
    for key in PERFORMANCE_FILTER_GROUPS:
        group_is_active = bool(source.get(f"{key}__active")) if hasattr(source, "get") else False
        values = source.getlist(key) if hasattr(source, "getlist") else source.get(key, [])
        if isinstance(values, str):
            values = [item for item in values.split(",") if item]
        if values or group_is_active:
            filters[key] = list(values or [])
    for key in ("expiration_start", "expiration_end"):
        value = str(source.get(key) or "").strip() if hasattr(source, "get") else ""
        if value:
            filters[key] = [value]
    return filters


def normalize_requested_performance_filters(filters: Optional[Dict[str, list[str]]] = None) -> Dict[str, list[str]]:
    normalized: Dict[str, list[str]] = {}
    for group, options in PERFORMANCE_FILTER_GROUPS.items():
        if not filters or group not in filters:
            continue
        allowed = {normalize_filter_value(group, option) for option in options}
        values = [normalize_filter_value(group, value) for value in (filters.get(group) or [])]
        normalized[group] = [value for value in values if value in allowed]
    for key in ("expiration_start", "expiration_end"):
        if not filters or key not in filters:
            continue
        value = str((filters.get(key) or [""])[0] or "").strip()
        if value:
            normalized[key] = [value]
    return normalized


def get_workflow_state(app: Optional[Flask] = None) -> WorkflowStateStore:
    container = app or current_app
    workflow_state = container.extensions.get("workflow_state")
    if workflow_state is None:
        workflow_state = FlaskSessionWorkflowState(
            trade_prefill_key=APOLLO_PREFILL_SESSION_KEY,
            trade_close_prefill_key=MANAGEMENT_CLOSE_PREFILL_SESSION_KEY,
            trade_form_fields=TRADE_FORM_FIELDS,
            trade_mode_resolver=resolve_trade_mode,
        )
        container.extensions["workflow_state"] = workflow_state
    return workflow_state


def build_delphi_route_map() -> Dict[str, str]:
    return {
        "home": url_for("index"),
        "apollo": url_for("run_apollo", autorun=1),
        "management": url_for("open_trade_management_page"),
        "performance": url_for("performance_dashboard"),
        "journal": url_for("trade_dashboard", trade_mode="real"),
        "journal_real": url_for("trade_dashboard", trade_mode="real"),
        "journal_simulated": url_for("trade_dashboard", trade_mode="simulated"),
        "open_trades": url_for("open_trade_management_page"),
        "notifications": url_for("open_trade_management_page"),
        "performance_data": url_for("performance_dashboard_data"),
        "text_status": url_for("text_status_api"),
    }


def get_trade_store(app: Optional[Flask] = None) -> TradeRepository:
    container = _resolve_flask_container(app)
    store = container.extensions.get("trade_store")
    if store is None:
        store = container.extensions["service_bundle"].trade_store
        container.extensions["trade_store"] = store
    return store


def get_apollo_snapshot_repository(app: Optional[Flask] = None) -> ApolloSnapshotRepository:
    container = _resolve_flask_container(app)
    repository = container.extensions.get("apollo_snapshot_repository")
    if repository is None:
        repository = container.extensions["service_bundle"].apollo_snapshot_repository
        container.extensions["apollo_snapshot_repository"] = repository
    return repository


def get_apollo_service(app: Optional[Flask] = None) -> ApolloService:
    container = _resolve_flask_container(app)
    service = container.extensions.get("apollo_service")
    if service is None:
        service = container.extensions["service_bundle"].apollo_service
        container.extensions["apollo_service"] = service
    return service


def get_market_data_service(app: Optional[Flask] = None) -> MarketDataService:
    container = _resolve_flask_container(app)
    service = container.extensions.get("market_data_service")
    if service is None:
        service = container.extensions["service_bundle"].market_data_service
        container.extensions["market_data_service"] = service
    return service


def get_performance_service(app: Optional[Flask] = None) -> PerformanceDashboardService:
    container = _resolve_flask_container(app)
    service = container.extensions.get("performance_service")
    if service is None:
        service = PerformanceDashboardService(get_trade_store(container))
        container.extensions["performance_service"] = service
    return service


def build_trade_notification_repository(app: Flask, trade_store: TradeRepository) -> TradeNotificationRepository:
    return SQLiteTradeNotificationRepository(trade_store.database_path)


def build_global_notification_settings_repository(app: Flask, trade_store: TradeRepository) -> GlobalNotificationSettingsRepository:
    return SQLiteGlobalNotificationSettingsRepository(trade_store.database_path)


def get_global_notification_settings_repository(app: Optional[Flask] = None) -> GlobalNotificationSettingsRepository:
    container = _resolve_flask_container(app)
    repository = container.extensions.get("global_notification_settings_repository")
    if repository is None:
        repository = build_global_notification_settings_repository(container, get_trade_store(container))
        repository.initialize()
        container.extensions["global_notification_settings_repository"] = repository
    manager = container.extensions.get("open_trade_manager")
    if manager is not None and getattr(manager, "global_notification_settings_repository", None) is not repository:
        manager.global_notification_settings_repository = repository
    return repository


def get_trade_notification_repository(app: Optional[Flask] = None) -> TradeNotificationRepository:
    container = _resolve_flask_container(app)
    repository = container.extensions.get("trade_notification_repository")
    if repository is None:
        repository = build_trade_notification_repository(container, get_trade_store(container))
        repository.initialize()
        container.extensions["trade_notification_repository"] = repository
    manager = container.extensions.get("open_trade_manager")
    if manager is not None and getattr(manager, "trade_notification_repository", None) is not repository:
        manager.trade_notification_repository = repository
    return repository


def get_open_trade_manager(app: Optional[Flask] = None) -> OpenTradeManager:
    container = _resolve_flask_container(app)
    manager = container.extensions.get("open_trade_manager")
    trade_store = get_trade_store(container)
    if manager is None:
        manager = container.extensions["service_bundle"].open_trade_manager
        container.extensions["open_trade_manager"] = manager
    if getattr(manager, "trade_store", None) is not trade_store:
        manager.trade_store = trade_store
    repository = get_trade_notification_repository(container)
    if getattr(manager, "trade_notification_repository", None) is not repository:
        manager.trade_notification_repository = repository
    global_settings_repository = get_global_notification_settings_repository(container)
    if getattr(manager, "global_notification_settings_repository", None) is not global_settings_repository:
        manager.global_notification_settings_repository = global_settings_repository
    return manager


def get_request_identity_resolver(app: Optional[Flask] = None) -> RequestIdentityResolver:
    container = _resolve_flask_container(app)
    resolver = container.extensions.get("request_identity_resolver")
    if resolver is None:
        resolver = container.extensions["service_bundle"].request_identity_resolver
        container.extensions["request_identity_resolver"] = resolver
    return resolver


def get_request_identity(app: Optional[Flask] = None) -> RequestIdentity:
    if has_request_context() and hasattr(g, "request_identity"):
        return g.request_identity
    if app is None and not has_request_context() and not has_app_context():
        return anonymous_request_identity(auth_source="unbound")
    if not has_request_context():
        return anonymous_request_identity(auth_source="out-of-request")
    return get_request_identity_resolver(app).resolve_request_identity(request)


def save_apollo_snapshot(apollo_result: Optional[Dict[str, Any]], app: Optional[Flask] = None) -> None:
    if not apollo_result:
        return
    container = _resolve_flask_container(app)
    try:
        get_apollo_snapshot_repository(container).save_snapshot(apollo_result)
    except OSError as exc:
        container.logger.warning("Unable to persist Apollo snapshot: %s", exc)


def set_status_message(message: str, level: str = "info") -> None:
    """Store a one-time UI message in the session."""
    get_workflow_state().set_status_message(message, level=level)


def pop_status_message() -> Optional[Dict[str, str]]:
    """Retrieve and clear a one-time UI message from the session."""
    return get_workflow_state().pop_status_message()


def store_trade_prefill(trade_mode: str, values: Dict[str, Any]) -> None:
    get_workflow_state().store_trade_prefill(trade_mode, values)


def get_trade_prefill(trade_mode: str) -> Optional[Dict[str, Any]]:
    return get_workflow_state().get_trade_prefill(trade_mode)


def clear_trade_prefill(trade_mode: Optional[str] = None) -> None:
    get_workflow_state().clear_trade_prefill(trade_mode)


def store_trade_close_prefill(trade_id: int, values: Dict[str, Any]) -> None:
    get_workflow_state().store_trade_close_prefill(trade_id, values)


def get_trade_close_prefill(trade_id: int) -> Optional[Dict[str, Any]]:
    return get_workflow_state().get_trade_close_prefill(trade_id)


def clear_trade_close_prefill(trade_id: Optional[int] = None) -> None:
    get_workflow_state().clear_trade_close_prefill(trade_id)


def get_trade_import_preview_repository(app: Flask) -> ImportPreviewRepository:
    repository = app.extensions.get("import_preview_repository")
    if repository is None:
        host_infrastructure = app.extensions.get("host_infrastructure")
        import_preview_root = host_infrastructure.storage.import_preview_root if host_infrastructure is not None else Path(app.instance_path)
        repository = FileSystemImportPreviewRepository(import_preview_root)
        app.extensions["import_preview_repository"] = repository
    return repository


def store_trade_import_preview(app: Flask, trade_mode: str, preview_payload: Dict[str, Any]) -> str:
    return get_trade_import_preview_repository(app).store_preview(resolve_trade_mode(trade_mode), preview_payload)


def load_trade_import_preview(app: Flask, token: str) -> Optional[Dict[str, Any]]:
    return get_trade_import_preview_repository(app).load_preview(token)


def delete_trade_import_preview(app: Flask, token: str) -> None:
    get_trade_import_preview_repository(app).delete_preview(token)


def coerce_trade_form_input(source: Any) -> Dict[str, Any]:
    values = {key: source.get(key, "") for key in TRADE_FORM_FIELDS}
    values["trade_mode"] = source.get("trade_mode") or source.get("trade_mode_filter") or "real"
    values["journal_name"] = source.get("journal_name") or JOURNAL_NAME_DEFAULT
    values["net_credit_per_contract"] = ""
    values["premium_per_contract"] = ""
    values["total_premium"] = ""
    values["max_theoretical_risk"] = ""
    return values


def coerce_trade_close_event_input(source: Any) -> Optional[list[Dict[str, Any]]]:
    if not hasattr(source, "getlist"):
        return None
    if not source.get("close_events_present"):
        return None
    row_ids = source.getlist("close_event_id")
    contracts_closed = source.getlist("close_event_contracts_closed")
    actual_exit_values = source.getlist("close_event_actual_exit_value")
    close_methods = source.getlist("close_event_method")
    event_datetimes = source.getlist("close_event_event_datetime")
    notes_exit = source.getlist("close_event_notes_exit")
    row_count = max(len(row_ids), len(contracts_closed), len(actual_exit_values), len(close_methods), len(event_datetimes), len(notes_exit), 0)
    rows: list[Dict[str, Any]] = []
    for index in range(row_count):
        row = {
            "id": row_ids[index] if index < len(row_ids) else "",
            "contracts_closed": contracts_closed[index] if index < len(contracts_closed) else "",
            "actual_exit_value": actual_exit_values[index] if index < len(actual_exit_values) else "",
            "close_method": close_methods[index] if index < len(close_methods) else "",
            "event_datetime": event_datetimes[index] if index < len(event_datetimes) else "",
            "notes_exit": notes_exit[index] if index < len(notes_exit) else "",
        }
        if row["id"] in {None, ""} and all(row[key] in {None, ""} for key in ("contracts_closed", "actual_exit_value", "close_method")):
            continue
        rows.append(row)
    return rows


def merge_trade_form_values(base_values: Dict[str, Any], override_values: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(base_values)
    for key in TRADE_FORM_FIELDS:
        if key in override_values and override_values.get(key) not in {None, ""}:
            merged[key] = override_values.get(key)
    merged["trade_mode"] = override_values.get("trade_mode") or merged.get("trade_mode")
    merged["journal_name"] = override_values.get("journal_name") or merged.get("journal_name") or JOURNAL_NAME_DEFAULT
    return merged


def is_apollo_prefill_submission(values: Dict[str, Any]) -> bool:
    return str(values.get("prefill_source") or "").strip().lower() == "apollo"


def get_trade_prefill_metadata(values: Dict[str, Any]) -> Dict[str, str]:
    prefill_source = str(values.get("prefill_source") or "").strip().lower()
    if prefill_source in {"kairos-live", "kairos-candidate"}:
        return {
            "title": "Review Imported Draft",
            "notice": "Imported trade data is loaded into this draft. Review any field, then click Save Trade to write it to the journal.",
        }
    if prefill_source == "apollo":
        return {
            "title": "Review Apollo Draft",
            "notice": "Apollo candidate data is loaded into this draft. Review any field, then click Save Trade to write it to the journal.",
        }
    return {
        "title": "Review Prefilled Draft",
        "notice": "Prefilled trade data is loaded into this draft. Review any field, then click Save Trade to write it to the journal.",
    }


def coerce_apollo_trade_input(source: Any, trade_mode: Optional[str] = None) -> Dict[str, Any]:
    timestamp = current_timestamp()
    resolved_trade_mode = normalize_trade_mode(trade_mode or source.get("trade_mode") or "simulated")
    candidate_profile = str(source.get("candidate_profile") or "Apollo").strip()
    structure_grade = str(source.get("structure_grade") or "").strip()
    macro_grade = str(source.get("macro_grade") or "").strip()

    return {
        "trade_number": source.get("trade_number") or "",
        "trade_mode": resolved_trade_mode,
        "system_name": "Apollo",
        "journal_name": JOURNAL_NAME_DEFAULT,
        "system_version": source.get("system_version") or HOSTED_APP_VERSION,
        "candidate_profile": candidate_profile,
        "status": "open",
        "trade_date": timestamp.split("T", 1)[0],
        "entry_datetime": timestamp,
        "expiration_date": source.get("expiration_date") or "",
        "underlying_symbol": source.get("underlying_symbol") or "SPX",
        "spx_at_entry": source.get("spx_at_entry") or "",
        "vix_at_entry": source.get("vix_at_entry") or "",
        "structure_grade": structure_grade,
        "macro_grade": macro_grade,
        "expected_move": source.get("expected_move") or "",
        "expected_move_used": source.get("expected_move_used") or source.get("expected_move") or "",
        "expected_move_source": source.get("expected_move_source") or "",
        "option_type": source.get("option_type") or "Put Credit Spread",
        "short_strike": source.get("short_strike") or "",
        "long_strike": source.get("long_strike") or "",
        "spread_width": source.get("spread_width") or "",
        "contracts": source.get("contracts") or "",
        "candidate_credit_estimate": source.get("candidate_credit_estimate") or "",
        "actual_entry_credit": source.get("actual_entry_credit") or source.get("candidate_credit_estimate") or "",
        "distance_to_short": source.get("distance_to_short") or "",
        "em_multiple_floor": source.get("em_multiple_floor") or "",
        "percent_floor": source.get("percent_floor") or "",
        "boundary_rule_used": source.get("boundary_rule_used") or "",
        "actual_distance_to_short": source.get("actual_distance_to_short") or source.get("distance_to_short") or "",
        "actual_em_multiple": source.get("actual_em_multiple") or "",
        "fallback_used": source.get("fallback_used") or "no",
        "fallback_rule_name": source.get("fallback_rule_name") or "",
        "short_delta": source.get("short_delta") or "",
        "pass_type": source.get("pass_type") or "",
        "premium_per_contract": source.get("premium_per_contract") or "",
        "total_premium": source.get("total_premium") or "",
        "max_theoretical_risk": source.get("max_theoretical_risk") or "",
        "risk_efficiency": source.get("risk_efficiency") or "",
        "credit_efficiency_pct": source.get("credit_efficiency_pct") or "",
        "target_em": source.get("target_em") or "",
        "notes_entry": "Prefilled from Apollo candidate card.",
        "prefill_source": "apollo",
        "exit_datetime": "",
        "spx_at_exit": "",
        "actual_exit_value": "",
        "close_method": "",
        "close_reason": "",
        "notes_exit": "",
    }


def build_trade_page_context(
    store: TradeStore,
    trade_mode: str,
    form_values: Dict[str, Any],
    form_action: str,
    form_title: str,
    editing_trade_id: Optional[int],
    error_message: Optional[str],
    info_message: Optional[Dict[str, str]],
    editing_trade: Optional[Dict[str, Any]] = None,
    prefill_active: bool = False,
    prefill_notice: str = "",
    hosted_prefill_enabled: bool = False,
    import_preview: Optional[Dict[str, Any]] = None,
    import_journal_name: str = JOURNAL_NAME_DEFAULT,
    available_trade_modes: Optional[list[str] | tuple[str, ...]] = None,
) -> Dict[str, Any]:
    normalized_mode = resolve_trade_mode(trade_mode)
    if normalized_mode == "talos":
        normalized_mode = "real"
    loaded_trades = load_retained_trade_rows(store, normalized_mode)
    trades = [build_trade_row_payload(item) for item in loaded_trades]
    summary = summarize_loaded_trade_rows(loaded_trades)
    trade_record = editing_trade if editing_trade is not None else (store.get_trade(editing_trade_id) if editing_trade_id else None)
    prepared_form_values = prepare_trade_form_values(form_values)
    journal_name_options = sorted(
        {
            *(str(option or "").strip() for option in TRADE_JOURNAL_OPTIONS),
            str(import_journal_name or "").strip(),
            str(prepared_form_values.get("journal_name") or "").strip(),
        }
        - {""}
    )
    return {
        "trade_mode": normalized_mode,
        "trade_mode_label": TRADE_MODE_LABELS.get(normalized_mode, normalized_mode.title()),
        "form_values": prepared_form_values,
        "form_action": form_action,
        "form_title": form_title,
        "editing_trade_id": editing_trade_id,
        "editing_trade": build_trade_detail_payload(trade_record) if trade_record else None,
        "trades": trades,
        "summary_metrics": build_trade_summary_metrics(summary),
        "trade_modes": [
            {
                "key": key,
                "label": label,
                "description": TRADE_MODE_DESCRIPTIONS.get(key, ""),
            }
            for key, label in TRADE_MODE_LABELS.items()
            if key in (available_trade_modes or PUBLIC_TRADE_MODES)
        ],
        "filter_groups": TRADE_FILTER_GROUPS,
        "system_name_options": list(TRADE_SYSTEM_OPTIONS),
        "journal_name_options": journal_name_options,
        "candidate_profiles": list(TRADE_PROFILE_OPTIONS),
        "option_type_options": list(TRADE_OPTION_TYPE_OPTIONS),
        "expected_move_field_meta": build_expected_move_field_context(prepared_form_values),
        "total_max_loss_field": build_total_max_loss_field_context(prepared_form_values),
        "distance_field_meta": build_distance_field_context(prepared_form_values),
        "error_message": error_message,
        "info_message": info_message,
        "prefill_active": prefill_active,
        "prefill_notice": prefill_notice,
        "hosted_prefill_enabled": hosted_prefill_enabled,
        "import_preview": import_preview,
        "import_journal_name": import_journal_name,
    }


def summarize_loaded_trade_rows(rows: list[Dict[str, Any]]) -> Dict[str, Any]:
    total_pnl = sum(float(row.get("gross_pnl") or 0.0) for row in rows)
    pnl_values = [float(row.get("gross_pnl") or 0.0) for row in rows if row.get("gross_pnl") is not None]
    return {
        "total_trades": len(rows),
        "open_trades": sum(1 for row in rows if row.get("derived_status_raw") in {"open", "reduced"}),
        "closed_trades": sum(1 for row in rows if row.get("derived_status_raw") not in {"open", "reduced"}),
        "total_pnl": total_pnl,
        "average_pnl": (sum(pnl_values) / len(pnl_values)) if pnl_values else 0.0,
        "win_count": sum(1 for row in rows if row.get("win_loss_result") == "Win"),
        "loss_count": sum(1 for row in rows if row.get("win_loss_result") in {"Loss", "Black Swan"}),
    }


def load_retained_trade_rows(store: TradeRepository, trade_mode: str) -> list[Dict[str, Any]]:
    normalized_mode = resolve_trade_mode(trade_mode)
    loaded_trade_modes = ("real", "talos") if normalized_mode in {"real", "talos"} else ("simulated",)
    loaded_trades = [trade for trade_mode_key in loaded_trade_modes for trade in store.list_trades(trade_mode_key)]
    return [trade for trade in loaded_trades if is_retained_trade_system(trade)]


def build_trade_summary_metrics(summary: Dict[str, Any]) -> list[Dict[str, str]]:
    return [
        {"label": "Total trades", "value": format_value(summary["total_trades"])} ,
        {"label": "Open trades", "value": format_value(summary["open_trades"])} ,
        {"label": "Closed trades", "value": format_value(summary["closed_trades"])} ,
        {"label": "Total P/L", "value": format_currency(summary["total_pnl"])} ,
        {"label": "Average P/L", "value": format_currency(summary["average_pnl"])} ,
        {"label": "Wins", "value": format_value(summary["win_count"])} ,
        {"label": "Losses", "value": format_value(summary["loss_count"])} ,
    ]


def resolve_hosted_trade_mode_filter(trade_mode: str) -> str:
    normalized_trade_mode = str(trade_mode or "").strip().lower()
    if normalized_trade_mode == "all":
        return "all"
    return resolve_trade_mode(normalized_trade_mode)


def build_open_trade_status_counts(records: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
    counts: Dict[str, Dict[str, Any]] = {}
    for item in records:
        status = str(item.get("status") or "Unknown").strip() or "Unknown"
        entry = counts.setdefault(
            status,
            {
                "status": status,
                "status_severity": int(item.get("status_severity") or 0),
                "count": 0,
            },
        )
        entry["count"] += 1
        entry["status_severity"] = max(int(entry.get("status_severity") or 0), int(item.get("status_severity") or 0))
    return sorted(counts.values(), key=lambda item: (-int(item.get("status_severity") or 0), str(item.get("status") or "")))


def prepare_trade_form_values(values: Dict[str, Any]) -> Dict[str, str]:
    prepared = dict(values)
    for field in ("entry_datetime", "exit_datetime"):
        prepared[field] = format_datetime_local_input(prepared.get(field))
    for field in TRADE_FORM_FIELDS:
        prepared.setdefault(field, "")
    prepared["system_name"] = normalize_system_name(prepared.get("system_name"))
    prepared["candidate_profile"] = normalize_candidate_profile(prepared.get("candidate_profile"))
    expected_move_metadata = resolve_trade_expected_move(prepared)
    prepared["expected_move"] = (
        format_value(expected_move_metadata.get("value")) if expected_move_metadata.get("value") is not None else ""
    )
    prepared["expected_move_source"] = expected_move_metadata.get("source") or ""
    distance_metadata = resolve_trade_distance(prepared)
    prepared["distance_to_short"] = (
        format_value(distance_metadata.get("value")) if distance_metadata.get("value") is not None else ""
    )
    return prepared


def build_expected_move_field_context(values: Dict[str, Any]) -> Dict[str, Any]:
    expected_move_metadata = resolve_trade_expected_move(values)
    source = normalize_expected_move_source(expected_move_metadata.get("source"))
    if source == "recovered_candidate":
        note = "Recovered from stored entry inputs using the saved daily move anchor formula."
    elif source == "recovered_snapshot":
        note = "Recovered from a matching stored import snapshot."
    elif source == "estimated_calibrated":
        note = "Estimated from SPX and VIX entry values, then calibration-adjusted before learning and safety metrics use it."
    elif source == "original":
        note = "Stored journal expected move retained as the authoritative value."
    else:
        note = "Expected move unresolved. Add the original value or preserve SPX and VIX entry data for recovery/backfill."
    return {
        "value": expected_move_metadata.get("value"),
        "source": source,
        "estimated": bool(expected_move_metadata.get("estimated")),
        "note": note,
    }


def build_trade_save_status(trade: Dict[str, Any], *, action: str) -> tuple[str, str]:
    expected_move_metadata = resolve_trade_expected_move(trade)
    if expected_move_metadata.get("value") is None:
        return (
            f"Trade {action}, but expected move is still unresolved. Safety-ratio analytics will exclude it until the field is recovered or entered.",
            "warning",
        )
    return (f"Trade {action} successfully.", "info")


def build_total_max_loss_field_context(values: Dict[str, Any]) -> Dict[str, Any]:
    credit_model = resolve_trade_credit_model(values)
    total_max_loss = credit_model.get("max_theoretical_risk")
    return {
        "value": total_max_loss,
        "value_display": format_currency(total_max_loss) if total_max_loss is not None else "ΓÇö",
        "note": "System-calculated from spread width minus net credit, multiplied by contracts and 100. This field is saved automatically and cannot be edited.",
    }


def build_distance_field_context(values: Dict[str, Any]) -> Dict[str, Any]:
    distance_metadata = resolve_trade_distance(values)
    source = distance_metadata.get("source")
    if source == "derived":
        note = "System-derived from SPX at entry and short strike."
    elif source == "estimated_fallback":
        note = "Apollo fallback estimate from the SPX close on the trading day before expiration."
    elif source == "original":
        note = "Stored journal distance retained as the authoritative value."
        if distance_metadata.get("discrepancy_is_material"):
            note = "Stored journal distance retained. Current entry inputs differ materially from that journal value."
    else:
        note = "Distance unresolved. Add SPX at entry and short strike, or run Apollo history backfill for older trades."
    return {
        "value": distance_metadata.get("value"),
        "source": source,
        "estimated": bool(distance_metadata.get("estimated")),
        "has_material_discrepancy": bool(distance_metadata.get("discrepancy_is_material")),
        "note": note,
    }


def build_trade_row_payload(trade: Dict[str, Any]) -> Dict[str, Any]:
    result_label = derive_trade_result(trade)
    result_filter = normalize_result_filter_value(result_label)
    derived_status_raw = str(trade.get("derived_status_raw") or trade.get("status") or "open").strip().lower()
    original_contracts = trade.get("original_contracts") if trade.get("original_contracts") is not None else trade.get("contracts")
    closed_contracts = trade.get("contracts_closed") if trade.get("contracts_closed") is not None else trade.get("closed_contracts")
    remaining_contracts = trade.get("contracts_remaining") if trade.get("contracts_remaining") is not None else trade.get("remaining_contracts")
    realized_pnl = trade.get("realized_pnl") if trade.get("realized_pnl") is not None else trade.get("gross_pnl")
    projected_open_pnl = None
    if derived_status_raw in {"open", "reduced"} and realized_pnl is None:
        credit_model = resolve_trade_credit_model(trade)
        remaining_open_contracts = remaining_contracts if remaining_contracts is not None else original_contracts
        net_credit_per_contract = credit_model.get("net_credit_per_contract")
        if net_credit_per_contract is not None and remaining_open_contracts not in {None, ""}:
            projected_open_pnl = float(net_credit_per_contract) * float(remaining_open_contracts) * 100.0
    pnl_display_value = projected_open_pnl if projected_open_pnl is not None else realized_pnl
    distance_metadata = resolve_trade_distance(trade)
    total_max_loss = trade.get("total_max_loss")
    if total_max_loss is None:
        total_max_loss = trade.get("max_theoretical_risk")
    if total_max_loss is None:
        total_max_loss = trade.get("max_loss") if trade.get("max_loss") is not None else trade.get("max_risk")
    resolved_system_name = resolve_trade_system_name(trade)
    return {
        "id": trade.get("id"),
        "trade_number": format_value(trade.get("trade_number")),
        "trade_number_raw": trade.get("trade_number") or 0,
        "status": str(trade.get("derived_status_label") or trade.get("status") or "ΓÇö").title(),
        "status_raw": derived_status_raw,
        "candidate_profile": resolve_trade_candidate_profile(trade),
        "system_name": resolved_system_name,
        "system_version": trade.get("system_version") or "ΓÇö",
        "trade_date": trade.get("trade_date") or "ΓÇö",
        "trade_date_raw": trade.get("trade_date") or "",
        "expiration_date": trade.get("expiration_date") or "ΓÇö",
        "expiration_date_raw": trade.get("expiration_date") or "",
        "underlying_symbol": trade.get("underlying_symbol") or "ΓÇö",
        "underlying_symbol_raw": str(trade.get("underlying_symbol") or "").upper(),
        "strike_pair": build_strike_pair_label(trade),
        "strike_pair_raw": build_strike_pair_sort_value(trade),
        "contracts": format_value(original_contracts),
        "contracts_raw": original_contracts if original_contracts is not None else "",
        "closed_contracts": format_value(closed_contracts),
        "closed_contracts_raw": closed_contracts if closed_contracts is not None else 0,
        "remaining_contracts": remaining_contracts if remaining_contracts is not None else trade.get("contracts"),
        "remaining_contracts_raw": remaining_contracts if remaining_contracts is not None else trade.get("contracts") or 0,
        "actual_entry_credit": format_value(trade.get("actual_entry_credit")),
        "actual_entry_credit_raw": trade.get("actual_entry_credit") if trade.get("actual_entry_credit") is not None else "",
        "actual_exit_value": build_trade_exit_display(trade),
        "actual_exit_value_raw": trade.get("weighted_exit_value") if trade.get("weighted_exit_value") is not None else (trade.get("actual_exit_value") if trade.get("actual_exit_value") is not None else ""),
        "total_max_loss": format_currency(total_max_loss),
        "total_max_loss_raw": total_max_loss if total_max_loss is not None else "",
        "distance_to_short": format_value(distance_metadata.get("value")),
        "distance_to_short_raw": distance_metadata.get("value") if distance_metadata.get("value") is not None else "",
        "distance_source": distance_metadata.get("source") or trade.get("distance_source"),
        "gross_pnl": format_currency(pnl_display_value),
        "gross_pnl_raw": pnl_display_value if pnl_display_value is not None else "",
        "max_risk": format_currency(trade.get("max_risk")),
        "roi_on_risk": format_ratio_percent(trade.get("roi_on_risk")),
        "roi_on_risk_raw": trade.get("roi_on_risk") if trade.get("roi_on_risk") is not None else "",
        "win_loss_result": result_label,
        "result_filter": result_filter,
        "result_sort": result_filter or str(result_label or "").lower(),
        "result_class": build_result_cell_class(result_filter),
        "notes_entry": trade.get("notes_entry") or "",
        "notes_exit": trade.get("notes_exit") or "",
        "trade_mode": trade.get("trade_mode") or "real",
        "journal_name": trade.get("journal_name") or JOURNAL_NAME_DEFAULT,
        "journal_name_raw": str(trade.get("journal_name") or JOURNAL_NAME_DEFAULT).lower(),
        "close_reason": trade.get("close_reason") or "",
    }


def build_trade_import_preview(store: TradeRepository, preview_source: Dict[str, Any]) -> Dict[str, Any]:
    preview_rows = []
    importable_rows = []
    seen_signatures: set[tuple[Any, ...]] = set()

    for row in preview_source.get("rows", []):
        status = row.get("status") or "invalid"
        messages = list(row.get("messages") or [])
        payload = row.get("payload")

        if status == "ready" and isinstance(payload, dict):
            signature = build_trade_duplicate_signature(payload, already_normalized=True)
            if signature in seen_signatures:
                status = "duplicate"
                messages.append("Duplicate row inside this import file.")
            elif store.find_duplicate_trade(payload):
                status = "duplicate"
                messages.append("Matches an existing trade already saved in this journal.")
            else:
                seen_signatures.add(signature)
                importable_rows.append(payload)

        preview_rows.append(
            {
                "row_number": row.get("row_number"),
                "status": status,
                "status_label": {
                    "ready": "Ready",
                    "duplicate": "Duplicate",
                    "invalid": "Needs Review",
                }.get(status, "Needs Review"),
                "status_class": {
                    "ready": "good",
                    "duplicate": "warning",
                    "invalid": "error",
                }.get(status, "error"),
                "candidate_profile": format_value((payload or {}).get("candidate_profile")),
                "trade_date": format_value((payload or {}).get("trade_date")),
                "expiration_date": format_value((payload or {}).get("expiration_date")),
                "symbol": format_value((payload or {}).get("underlying_symbol")),
                "strike_pair": build_strike_pair_label(payload or {}),
                "contracts": format_value((payload or {}).get("contracts")),
                "actual_entry_credit": format_value((payload or {}).get("actual_entry_credit")),
                "actual_exit_value": format_value((payload or {}).get("actual_exit_value")),
                "journal_name": format_value((payload or {}).get("journal_name")),
                "messages": messages,
                "mapped_fields": row.get("mapped_fields") or [],
            }
        )

    return {
        "file_name": preview_source.get("file_name") or "Import file",
        "journal_name": (importable_rows[0].get("journal_name") if importable_rows else None) or JOURNAL_NAME_DEFAULT,
        "recognized_columns": preview_source.get("recognized_columns") or [],
        "rows": preview_rows,
        "row_count": len(preview_rows),
        "ready_count": sum(1 for row in preview_rows if row["status"] == "ready"),
        "duplicate_count": sum(1 for row in preview_rows if row["status"] == "duplicate"),
        "invalid_count": sum(1 for row in preview_rows if row["status"] == "invalid"),
        "importable_rows": importable_rows,
    }


def build_strike_pair_label(trade: Dict[str, Any]) -> str:
    short_strike = format_value(trade.get("short_strike"))
    long_strike = format_value(trade.get("long_strike"))
    if short_strike == "ΓÇö" and long_strike == "ΓÇö":
        return "ΓÇö"
    return f"{short_strike} / {long_strike}"


def build_strike_pair_sort_value(trade: Dict[str, Any]) -> str:
    short_strike = _coerce_float(trade.get("short_strike"))
    long_strike = _coerce_float(trade.get("long_strike"))
    if short_strike is None and long_strike is None:
        return ""
    short_label = f"{short_strike:012.2f}" if short_strike is not None else "000000000.00"
    long_label = f"{long_strike:012.2f}" if long_strike is not None else "000000000.00"
    return f"{short_label}:{long_label}"


def derive_trade_result(trade: Dict[str, Any]) -> str:
    current_status = str(trade.get("derived_status_raw") or trade.get("status") or "").strip().lower()
    closed_contracts = to_int(trade.get("contracts_closed") if trade.get("contracts_closed") is not None else trade.get("closed_contracts")) or 0
    remaining_contracts = to_int(trade.get("contracts_remaining") if trade.get("contracts_remaining") is not None else trade.get("remaining_contracts"))
    if closed_contracts > 0 and remaining_contracts not in {None, 0}:
        return "Reduced"
    if current_status == "open":
        return "Open"
    classified_result = classify_closed_trade_outcome(
        gross_pnl=trade.get("gross_pnl") if trade.get("gross_pnl") is not None else trade.get("pnl"),
        max_theoretical_risk=trade.get("total_max_loss") if trade.get("total_max_loss") is not None else trade.get("max_theoretical_risk"),
        explicit_result=trade.get("win_loss_result") or trade.get("result"),
        close_reason=trade.get("close_reason"),
    )
    if classified_result == "Flat":
        return "Scratched"
    if classified_result:
        return classified_result
    return "ΓÇö"


def build_trade_exit_display(trade: Dict[str, Any]) -> str:
    closed_contracts = to_int(trade.get("contracts_closed") if trade.get("contracts_closed") is not None else trade.get("closed_contracts")) or 0
    remaining_contracts = to_int(trade.get("contracts_remaining") if trade.get("contracts_remaining") is not None else trade.get("remaining_contracts"))
    status = str(trade.get("derived_status_raw") or trade.get("status") or "").strip().lower()
    weighted_exit = trade.get("weighted_exit_value") if trade.get("weighted_exit_value") is not None else trade.get("actual_exit_value")
    if closed_contracts > 0 and remaining_contracts not in {None, 0}:
        if weighted_exit is None:
            return "Partial"
        return f"Partial @ {format_value(weighted_exit)}"
    if status == "open":
        return "ΓÇö"
    return format_value(weighted_exit)


def build_trade_detail_payload(trade: Dict[str, Any]) -> Dict[str, Any]:
    if not trade:
        return {}
    close_events = trade.get("close_events") or []
    if trade.get("original_contracts") is None or trade.get("contracts_closed") is None or trade.get("contracts_remaining") is None:
        summary = summarize_trade_close_events(trade, close_events)
    else:
        summary = trade
    distance_metadata = resolve_trade_distance(summary)
    total_max_loss = trade.get("total_max_loss")
    if total_max_loss is None:
        total_max_loss = trade.get("max_theoretical_risk")
    if total_max_loss is None:
        total_max_loss = trade.get("max_loss") if trade.get("max_loss") is not None else trade.get("max_risk")
    return {
        "trade_number": trade.get("trade_number"),
        "status": str(summary.get("derived_status_label") or summary.get("status") or "ΓÇö").title(),
        "result": derive_trade_result(summary),
        "original_contracts": format_value(summary.get("original_contracts") if summary.get("original_contracts") is not None else summary.get("contracts")),
        "original_contracts_raw": summary.get("original_contracts") if summary.get("original_contracts") is not None else summary.get("contracts") or 0,
        "closed_contracts": format_value(summary.get("contracts_closed") if summary.get("contracts_closed") is not None else summary.get("closed_contracts")),
        "closed_contracts_raw": summary.get("contracts_closed") if summary.get("contracts_closed") is not None else summary.get("closed_contracts") or 0,
        "remaining_contracts": format_value(summary.get("contracts_remaining") if summary.get("contracts_remaining") is not None else summary.get("remaining_contracts")),
        "remaining_contracts_raw": summary.get("contracts_remaining") if summary.get("contracts_remaining") is not None else summary.get("remaining_contracts") or 0,
        "realized_pnl": format_currency(summary.get("realized_pnl") if summary.get("realized_pnl") is not None else summary.get("gross_pnl")),
        "realized_pnl_raw": summary.get("realized_pnl") if summary.get("realized_pnl") is not None else summary.get("gross_pnl") or 0,
        "total_max_loss": format_currency(total_max_loss),
        "total_max_loss_raw": total_max_loss if total_max_loss is not None else 0,
        "entry_credit_raw": trade.get("actual_entry_credit") if trade.get("actual_entry_credit") is not None else 0,
        "exit_display": build_trade_exit_display(summary),
        "distance_to_short": format_value(distance_metadata.get("value")),
        "distance_source": distance_metadata.get("source"),
        "distance_is_estimated": bool(distance_metadata.get("estimated")),
        "close_events": [
            {
                "id": format_value(event.get("id")),
                "contracts_closed": format_value(event.get("contracts_closed")),
                "contracts_closed_value": format_value(event.get("contracts_closed")),
                "actual_exit_value": format_value(event.get("actual_exit_value")),
                "actual_exit_value_value": format_value(event.get("actual_exit_value")),
                "event_datetime": format_datetime_local_input(event.get("event_datetime")) or format_value(event.get("event_datetime")),
                "event_datetime_value": format_datetime_local_input(event.get("event_datetime")) or "",
                "close_method": format_value(event.get("close_method") or event.get("event_type") or "Reduce"),
                "close_method_value": format_value(event.get("close_method") or event.get("event_type") or "Reduce"),
                "notes_exit": format_value(event.get("notes_exit")),
                "notes_exit_value": format_value(event.get("notes_exit")) if event.get("notes_exit") not in {None, "ΓÇö"} else "",
            }
            for event in close_events
        ],
    }


def build_edit_trade_preview(existing_trade: Dict[str, Any], form_values: Dict[str, Any], close_events: list[Dict[str, Any]]) -> Dict[str, Any]:
    preview = dict(existing_trade or {})
    for key in TRADE_FORM_FIELDS:
        if key in form_values:
            preview[key] = form_values.get(key)
    preview["total_max_loss"] = resolve_trade_credit_model(preview).get("max_theoretical_risk")
    preview["close_events"] = close_events
    preview.update(summarize_trade_close_events(preview, close_events))
    return preview


def build_manage_trade_close_prefill(record: Dict[str, Any]) -> Dict[str, Any]:
    close_timestamp = current_timestamp()
    return {
        "id": "",
        "contracts_closed": str(int(record.get("contracts") or 0)),
        "actual_exit_value": f"{float(record.get('current_spread_mark') or 0.0):.2f}",
        "close_method": "Manage Trade Prefill",
        "event_datetime": close_timestamp,
        "notes_exit": (
            f"Prefilled from Manage Trades at {record.get('evaluated_at_display') or close_timestamp} "
            f"using current total close cost {record.get('current_total_close_cost_display') or 'ΓÇö'}."
        ),
    }


def apply_trade_close_prefill(trade: Dict[str, Any], close_prefill: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not close_prefill:
        return trade
    preview = dict(trade or {})
    existing_events = [dict(item) for item in (preview.get("close_events") or [])]
    prefill_timestamp = str(close_prefill.get("event_datetime") or "").strip()
    prefill_method = str(close_prefill.get("close_method") or "").strip().lower()
    if any(
        str(item.get("id") or "").strip() == ""
        and str(item.get("event_datetime") or "").strip() == prefill_timestamp
        and str(item.get("close_method") or "").strip().lower() == prefill_method
        for item in existing_events
    ):
        return preview
    existing_events.append(dict(close_prefill))
    preview["close_events"] = existing_events
    preview.update(summarize_trade_close_events(preview, existing_events))
    return preview


def normalize_result_filter_value(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text == "black swan":
        return "black-swan"
    if text == "win":
        return "win"
    if text == "loss":
        return "loss"
    return ""


def build_result_cell_class(result_filter: str) -> str:
    return {
        "win": "trade-result-win",
        "loss": "trade-result-loss",
        "black-swan": "trade-result-black-swan",
    }.get(result_filter, "")


def resolve_trade_mode(trade_mode: str) -> str:
    try:
        return normalize_trade_mode(trade_mode)
    except ValueError as exc:
        raise abort(404) from exc


def format_datetime_local_input(value: Any) -> str:
    if value in {None, ""}:
        return ""
    text = str(value)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        timestamp = datetime.fromisoformat(text)
    except ValueError:
        return text
    if timestamp.tzinfo is not None:
        timestamp = timestamp.astimezone(CHICAGO_TZ)
    return timestamp.strftime("%Y-%m-%dT%H:%M")


def execute_apollo_precheck(apollo_service: ApolloService, trigger_source: str, *, force_refresh: bool = False) -> Dict[str, Any]:
    """Run Apollo once and convert the result into a template-ready payload."""
    payload = build_apollo_result_payload(
        apollo_service.run_precheck(force_refresh=force_refresh),
        trigger_source=trigger_source,
    )
    payload["execution_source_label"] = str(trigger_source or "unknown").title()
    payload["live_data_provider"] = apollo_service.market_data_service.get_provider_metadata().get("live_provider_name", "Unknown Provider")
    payload["live_data_mode"] = "Fresh live data" if force_refresh else "Live data"
    return payload


def get_apollo_trigger_source() -> Optional[str]:
    """Return the Apollo trigger source for the current request."""
    if request.method == "POST":
        return "button"
    if is_local_dev_request() and str(request.args.get("autorun", "")).strip().lower() in {"1", "true", "yes", "on"}:
        return "autorun URL"
    return None


def is_local_dev_request() -> bool:
    """Return whether the current request is coming from a local development host."""
    host = (request.host.split(":", 1)[0] if request.host else "").lower()
    return bool(app.testing or host in LOCAL_DEV_HOSTS)


def build_apollo_candidate_prefill_fields(
    item: Dict[str, Any],
    *,
    spx: Dict[str, Any],
    vix: Dict[str, Any],
    macro: Dict[str, Any],
    structure: Dict[str, Any],
    option_chain: Dict[str, Any],
    trade_candidates: Dict[str, Any],
) -> Dict[str, Any]:
    candidate_profile = {
        "fortress": "Fortress",
        "standard": "Standard",
        "aggressive": "Aggressive",
    }.get(str(item.get("mode_key") or "").strip().lower(), str(item.get("mode_label") or item.get("mode_descriptor") or "Apollo").split("(", 1)[0].strip() or "Apollo")

    contracts = item.get("adjusted_contract_size")
    if contracts in (None, ""):
        contracts = item.get("recommended_contract_size")
    if contracts in (None, ""):
        contracts = item.get("original_contract_size")

    return {
        "system_name": "Apollo",
        "journal_name": JOURNAL_NAME_DEFAULT,
        "system_version": HOSTED_APP_VERSION,
        "candidate_profile": candidate_profile,
        "expiration_date": option_chain.get("expiration_date") or trade_candidates.get("expiration_date") or "",
        "underlying_symbol": option_chain.get("symbol_requested") or "SPX",
        "spx_at_entry": spx.get("value") or trade_candidates.get("underlying_price") or "",
        "vix_at_entry": vix.get("value") or "",
        "structure_grade": structure.get("grade") or "",
        "macro_grade": macro.get("grade") or "",
        "expected_move": item.get("expected_move") or trade_candidates.get("expected_move") or "",
        "expected_move_used": item.get("expected_move_used") or item.get("expected_move") or trade_candidates.get("expected_move") or "",
        "expected_move_source": item.get("expected_move_source") or "same_day_atm_straddle",
        "option_type": "Put Credit Spread",
        "short_strike": item.get("short_strike") or "",
        "long_strike": item.get("long_strike") or "",
        "spread_width": item.get("width") or "",
        "contracts": contracts or "",
        "candidate_credit_estimate": item.get("credit") or "",
        "actual_entry_credit": item.get("credit") or "",
        "distance_to_short": item.get("distance_points") or "",
        "em_multiple_floor": item.get("applied_em_multiple_floor") or item.get("target_em_multiple") or "",
        "percent_floor": item.get("percent_floor") or "",
        "boundary_rule_used": item.get("boundary_rule_used") or "",
        "actual_distance_to_short": item.get("actual_distance_to_short") or item.get("distance_points") or "",
        "actual_em_multiple": item.get("actual_em_multiple") or item.get("em_multiple") or "",
        "pass_type": item.get("pass_type") or "",
        "premium_per_contract": item.get("premium_per_contract") or "",
        "total_premium": item.get("total_premium") or item.get("premium_received_dollars") or "",
        "max_theoretical_risk": item.get("max_theoretical_risk") or item.get("max_loss") or "",
        "risk_efficiency": item.get("risk_efficiency") or "",
        "credit_efficiency_pct": item.get("credit_efficiency_pct") or "",
        "target_em": item.get("target_em") or item.get("target_em_multiple") or "",
        "fallback_used": "yes" if item.get("fallback_used") else "no",
        "fallback_rule_name": item.get("fallback_rule_name") or "",
        "short_delta": item.get("short_delta") or "",
        "notes_entry": "Prefilled from Apollo candidate card.",
        "prefill_source": "apollo",
    }


def build_apollo_result_payload(apollo_data: Dict[str, Any], trigger_source: Optional[str] = None) -> Dict[str, Any]:
    """Convert Apollo service output into a template-ready payload."""
    spx = apollo_data.get("spx") or {}
    vix = apollo_data.get("vix") or {}
    macro = apollo_data.get("macro") or {}
    structure = apollo_data.get("structure") or {}
    market_calendar = apollo_data.get("market_calendar") or {}
    option_chain = apollo_data.get("option_chain") or {}
    trade_candidates = apollo_data.get("trade_candidates") or {}
    option_chain_diagnostics = option_chain.get("request_diagnostics") or {}
    metrics = structure.get("metrics") or {}
    option_chain_failure_category = str(
        option_chain.get("failure_category") or option_chain_diagnostics.get("failure_category") or ""
    ).strip().lower()
    option_chain_failure_label = str(
        option_chain.get("failure_label") or option_chain_diagnostics.get("failure_label") or ""
    ).strip()
    option_chain_status = "Ready" if option_chain.get("success", False) else (option_chain_failure_label or "Unavailable")
    option_chain_status_class = (
        "good"
        if option_chain.get("success", False)
        else (option_chain.get("failure_status_class") or ("poor" if option_chain_failure_category == "malformed-request" else "not-available"))
    )
    outcome_profile = get_real_trade_outcome_profile()
    routine_loss_percentage = float(outcome_profile.get("routine_loss_percentage") or 0.0)
    black_swan_loss_percentage = float(outcome_profile.get("black_swan_loss_percentage") or 0.0)
    routine_loss_count = int(outcome_profile.get("loss_count") or 0)
    black_swan_loss_count = int(outcome_profile.get("black_swan_count") or 0)

    trade_candidate_items = []
    for index, item in enumerate(trade_candidates.get("candidates", [])):
        if not isinstance(item, dict):
            continue
        probability_labels = build_candidate_probability_labels(item.get("short_delta"), item.get("long_delta"))
        prefill_fields = build_apollo_candidate_prefill_fields(
            item,
            spx=spx,
            vix=vix,
            macro=macro,
            structure=structure,
            option_chain=option_chain,
            trade_candidates=trade_candidates,
        )
        trade_candidate_items.append(
            {
                "rank": item.get("rank", index + 1),
                "mode_key": str(item.get("mode_key", "mode")),
                "mode_label": str(item.get("mode_label", f"Mode {index + 1}")),
                "mode_descriptor": str(item.get("mode_descriptor", "")),
                "available": bool(item.get("available", True)),
                "no_trade_message": str(item.get("no_trade_message", "")),
                "strategy_label": str(item.get("strategy_label", "SPX put credit spread")),
                "position_label": "Put Spread",
                "score": int(item.get("score", 0) or 0),
                "short_strike": format_value(item.get("short_strike")),
                "long_strike": format_value(item.get("long_strike")),
                "width": format_value(item.get("width")),
                "credit": format_value(item.get("credit")),
                "net_credit": format_currency(item.get("credit"), decimals=2),
                "premium_per_contract": format_currency(item.get("premium_per_contract")),
                "premium_received_dollars": format_currency(item.get("premium_received_dollars")),
                "total_premium": format_currency(item.get("total_premium") if item.get("total_premium") is not None else item.get("premium_received_dollars")),
                "max_theoretical_risk": format_currency(item.get("max_theoretical_risk") if item.get("max_theoretical_risk") is not None else item.get("max_loss")),
                "risk_efficiency": format_ratio(item.get("risk_efficiency"), decimals=4),
                "credit_efficiency": append_percent(item.get("credit_efficiency_pct")),
                "routine_loss": format_currency(project_historical_loss(item.get("max_loss"), routine_loss_percentage)),
                "black_swan_loss": format_currency(project_historical_loss(item.get("max_loss"), black_swan_loss_percentage)),
                "routine_loss_percentage": format_ratio_percent(routine_loss_percentage),
                "black_swan_loss_percentage": format_ratio_percent(black_swan_loss_percentage),
                "routine_loss_count": routine_loss_count,
                "black_swan_loss_count": black_swan_loss_count,
                "loss_model_source": "Historical real-trade ROI averages only (ordinary losses exclude Black Swans)",
                "max_loss": format_currency(item.get("max_loss")),
                "max_loss_per_contract": format_value(item.get("max_loss_per_contract")),
                "risk_cap_dollars": format_currency(item.get("risk_cap_dollars")),
                "risk_cap_status": str(item.get("risk_cap_status", "ΓÇö")),
                "risk_cap_adjusted": "Yes" if item.get("risk_cap_adjusted") else "No",
                "original_contract_size": format_value(item.get("original_contract_size")),
                "adjusted_contract_size": format_value(item.get("adjusted_contract_size")),
                "account_risk_percent": append_percent(item.get("account_risk_percent")),
                "exit_plan_applied": "Yes" if item.get("exit_plan_applied") else "No",
                "break_even": format_value(item.get("break_even")),
                "short_delta": format_value(item.get("short_delta")),
                "long_delta": format_value(item.get("long_delta")),
                "distance_points": format_value(item.get("distance_points")),
                "distance_to_short": format_value(item.get("distance_points")),
                "distance_percent": append_percent(item.get("distance_percent")),
                "expected_move": format_value(item.get("expected_move")),
                "expected_move_used": format_value(item.get("expected_move_used") or item.get("expected_move")),
                "expected_move_source": str(item.get("expected_move_source") or "same_day_atm_straddle"),
                "expected_move_1_5x_threshold": format_value(item.get("expected_move_1_5x_threshold")),
                "expected_move_2x_threshold": format_value(item.get("expected_move_2x_threshold")),
                "em_multiple": format_em_multiple(item.get("em_multiple")),
                "target_em": format_em_multiple(item.get("target_em") if item.get("target_em") is not None else item.get("target_em_multiple")),
                "applied_em_floor": format_em_multiple(item.get("applied_em_multiple_floor") or item.get("target_em_multiple")),
                "percent_floor": append_percent(item.get("percent_floor")),
                "percent_floor_points": format_value(item.get("percent_floor_points")),
                "em_floor_points": format_value(item.get("em_floor_points")),
                "hybrid_threshold": format_value(item.get("hybrid_distance_threshold")),
                "boundary_binding_source": str(item.get("boundary_binding_source") or "ΓÇö"),
                "pass_type": str(item.get("pass_type") or ""),
                "pass_type_label": str(item.get("pass_type_label") or item.get("pass_type") or "Strict Pass"),
                "fallback_used": "Yes" if item.get("fallback_used") else "No",
                "fallback_rule_name": str(item.get("fallback_rule_name") or "ΓÇö"),
                "economic_filter_status": (
                    "Passed" if item.get("available") else str(item.get("no_trade_message") or "ΓÇö")
                ),
                "boundary_rule_used": str(item.get("boundary_rule_used") or "ΓÇö"),
                "actual_distance_to_short": format_value(item.get("actual_distance_to_short") or item.get("distance_points")),
                "actual_em_multiple": format_em_multiple(item.get("actual_em_multiple") or item.get("em_multiple")),
                "expected_move_comparison": str(item.get("expected_move_comparison", "ΓÇö")),
                "required_distance_rule_used": str(item.get("required_distance_rule_used", "ΓÇö")),
                "active_em_rule": str(item.get("active_em_rule", "ΓÇö")),
                "active_rule_set": str(item.get("active_rule_set", "ΓÇö")),
                "recommended_contract_size": format_value(item.get("recommended_contract_size")),
                "recommended_contract_size_reason": str(item.get("recommended_contract_size_reason", "ΓÇö")),
                "pricing_basis": str(item.get("pricing_basis") or "ΓÇö"),
                "selection_variant": str(item.get("selection_variant") or "ΓÇö"),
                "short_open_interest": format_value(item.get("short_open_interest")),
                "short_volume": format_value(item.get("short_volume")),
                "premium_probability": probability_labels["premium"],
                "routine_probability": probability_labels["routine"],
                "tail_probability": probability_labels["tail"],
                "max_probability": probability_labels["max"],
                "prefill_fields": prefill_fields,
                "rationale": item.get("rationale", []),
                "exit_plan": item.get("exit_plan", []),
                "diagnostics": item.get("diagnostics", []),
            }
        )
    trade_candidates_credit_map = build_trade_candidates_credit_map(option_chain=option_chain, trade_candidates=trade_candidates)

    raw_macro_events = macro.get("macro_events") or []
    macro_events = [
        {
            "title": str(item.get("title", "ΓÇö")),
            "time": str(item.get("time", "Time unavailable")),
            "impact": "Major" if str(item.get("impact", "")).title() == "Major" else "Minor",
            "reason": str(item.get("reason", "")),
        }
        for item in raw_macro_events
        if isinstance(item, dict)
    ]
    macro_major_events = [item for item in macro_events if item["impact"] == "Major"]
    macro_minor_events = [item for item in macro_events if item["impact"] != "Major"]
    macro_grade = str(macro.get("grade", "None") or "None").title()
    macro_status_class = {
        "None": "good",
        "Minor": "neutral",
        "Major": "poor",
    }.get(macro_grade, "not-available")
    valid_trade_candidate_count = int(
        trade_candidates.get("valid_mode_count")
        or sum(1 for item in trade_candidate_items if item.get("available"))
    )
    trade_candidates_outcome_category = "ready" if valid_trade_candidate_count else ""
    trade_candidates_outcome_label = "Ready" if valid_trade_candidate_count else ""
    if option_chain.get("success", False) and not valid_trade_candidate_count:
        trade_candidates_outcome_category = "no-candidates"
        trade_candidates_outcome_label = "No candidates"

    return {
        "title": apollo_data.get("title", "Apollo Gate 1 -- SPX Structure"),
        "provider_name": apollo_data.get("provider_name", "Unknown Provider"),
        "status": str(apollo_data.get("apollo_status", "blocked")).title(),
        "status_class": str(apollo_data.get("apollo_status", "blocked")).lower(),
        "local_datetime": format_apollo_datetime(apollo_data.get("local_datetime")),
        "run_timestamp": format_apollo_datetime(apollo_data.get("local_datetime")),
        "spx_value": format_value(spx.get("value")) if spx else "ΓÇö",
        "spx_as_of": spx.get("as_of", "ΓÇö") if spx else "ΓÇö",
        "vix_value": format_value(vix.get("value")) if vix else "ΓÇö",
        "vix_as_of": vix.get("as_of", "ΓÇö") if vix else "ΓÇö",
        "macro_title": "Apollo Gate 2 -- Macro Event",
        "macro_source": macro.get("source_name", "MarketWatch (unavailable)"),
        "macro_grade": macro_grade,
        "macro_grade_class": macro_status_class,
        "macro_target_day": format_long_date(macro.get("target_date")),
        "macro_checked_at": format_apollo_datetime(macro.get("checked_at")),
        "macro_checked_dates": ", ".join(str(item) for item in (macro.get("checked_dates") or [])) or "ΓÇö",
        "macro_available": "Yes" if macro.get("available", True) else "No",
        "macro_event_count": len(macro_events),
        "macro_major_detected": "Yes" if macro.get("has_major_macro") else "No",
        "macro_explanation": macro.get("explanation", "ΓÇö"),
        "macro_diagnostic": macro.get("diagnostic") or {},
        "macro_source_attempts": [
            {
                "source": str(item.get("source", "Source")),
                "status": str(item.get("status", "unknown")),
                "detail": str(item.get("detail", "ΓÇö")),
                "response_status": str(item.get("response_status", "ΓÇö")),
                "final_url": str(item.get("final_url", "ΓÇö")),
                "parser_strategy": str(item.get("parser_strategy", "ΓÇö")),
                "event_count": item.get("event_count", 0),
                "failure_reason": str(item.get("failure_reason", "")),
                "body_snippet": str(item.get("body_snippet", "")),
            }
            for item in (macro.get("source_attempts") or [])
            if isinstance(item, dict)
        ],
        "macro_events": macro_events,
        "macro_major_events": macro_major_events,
        "macro_minor_events": macro_minor_events,
        "next_market_day": format_value(market_calendar.get("next_market_day")),
        "next_market_day_note": market_calendar.get("note", "ΓÇö"),
        "holiday_filter_applied": market_calendar.get("holiday_filter_applied_label", "No"),
        "skipped_holiday_name": market_calendar.get("skipped_holiday_name") or "ΓÇö",
        "candidate_date_considered": format_value(market_calendar.get("candidate_date_considered")),
        "structure_available": structure.get("available", False),
        "structure_preferred_source": structure.get("preferred_source", "Not available"),
        "structure_attempted_sources": structure.get("attempted_sources", []),
        "structure_fallback_reason": structure.get("fallback_reason", ""),
        "structure_source_used": structure.get("source_used", "Not available"),
        "structure_grade": structure.get("final_grade", structure.get("grade", "Not available")),
        "structure_grade_class": str(structure.get("final_grade", structure.get("grade", "not-available"))).lower().replace(" ", "-"),
        "structure_base_grade": structure.get("base_grade", structure.get("grade", "Not available")),
        "structure_final_grade": structure.get("final_grade", structure.get("grade", "Not available")),
        "structure_rsi_modifier": structure.get("rsi_modifier_label", "None"),
        "structure_rsi_value": format_value(structure.get("rsi_value")),
        "structure_rsi_note": structure.get("rsi_note") or "Daily RSI unavailable; base structure kept.",
        "structure_trend_classification": structure.get("trend_classification", "Not available"),
        "structure_damage_classification": structure.get("damage_classification", "Not available"),
        "structure_summary": structure.get("summary") or "ΓÇö",
        "structure_message": structure.get("message") or "",
        "structure_session_note": structure.get("session_note") or "",
        "structure_rules": structure.get("rules") or [],
        "structure_chart": structure.get("chart") or {"available": False, "points": []},
        "structure_session_high": format_value(metrics.get("session_high")),
        "structure_session_low": format_value(metrics.get("session_low")),
        "structure_current_price": format_value(metrics.get("current_price")),
        "structure_range_position": append_percent((metrics.get("range_position") or 0) * 100) if metrics.get("range_position") is not None else "ΓÇö",
        "structure_ema8": format_value(metrics.get("ema8")),
        "structure_ema21": format_value(metrics.get("ema21")),
        "structure_recent_price_action": metrics.get("recent_price_action", "ΓÇö"),
        "structure_session_window": (
            f"{metrics.get('session_start', 'ΓÇö')} to {metrics.get('session_end', 'ΓÇö')}"
            if metrics.get("session_start") or metrics.get("session_end")
            else "ΓÇö"
        ),
        "option_chain_success": option_chain.get("success", False),
        "option_chain_status": option_chain_status,
        "option_chain_status_class": option_chain_status_class,
        "option_chain_source": option_chain.get("source_name", "Schwab"),
        "option_chain_failure_category": option_chain_failure_category,
        "option_chain_failure_label": option_chain_failure_label or "ΓÇö",
        "option_chain_heading_date": format_long_date(option_chain.get("expiration_date")),
        "option_chain_symbol_requested": option_chain.get("symbol_requested", "ΓÇö"),
        "option_chain_expiration_target": format_value(option_chain.get("expiration_target")),
        "option_chain_expiration": format_value(option_chain.get("expiration_date")),
        "option_chain_expiration_count": option_chain.get("expiration_count", 0),
        "option_chain_puts_count": option_chain.get("puts_count", 0),
        "option_chain_calls_count": option_chain.get("calls_count", 0),
        "option_chain_rows_displayed": option_chain.get("rows_displayed", 0),
        "option_chain_display_puts_count": option_chain.get("display_puts_count", 0),
        "option_chain_display_calls_count": option_chain.get("display_calls_count", 0),
        "option_chain_min_premium_target": format_value(option_chain.get("min_premium_target")),
        "option_chain_rows_setting": option_chain.get("rows_setting", "Adaptive"),
        "option_chain_grouping": option_chain.get("grouping", "Puts ascending ΓåÆ Calls ascending"),
        "option_chain_strike_range": option_chain.get("strike_range", "ΓÇö"),
        "option_chain_message": option_chain.get("message", "ΓÇö"),
        "option_chain_preview_rows": option_chain.get("preview_rows", []),
        "option_chain_final_symbol": option_chain_diagnostics.get("final_symbol", option_chain.get("symbol_requested", "ΓÇö")),
        "option_chain_final_expiration_sent": option_chain_diagnostics.get("final_expiration", format_value(option_chain.get("expiration_target"))),
        "option_chain_request_attempt_used": option_chain_diagnostics.get("attempt_used", "ΓÇö"),
        "option_chain_raw_params_sent": option_chain_diagnostics.get("raw_params_sent", {}),
        "option_chain_error_detail": option_chain_diagnostics.get("error_detail") or option_chain.get("message", "ΓÇö"),
        "option_chain_attempt_results": [
            {
                "label": item.get("label", "Attempt"),
                "status": item.get("status", "unknown"),
                "status_code": item.get("status_code", "ΓÇö"),
                "params": item.get("params", {}),
                "error_detail": item.get("error_detail") or "ΓÇö",
                "failure_category": item.get("failure_category") or "",
                "failure_label": item.get("failure_label") or "",
            }
            for item in option_chain_diagnostics.get("attempts", [])
            if isinstance(item, dict)
        ],
        "trade_candidates_title": "Apollo Gate 3 -- Engine",
        "trade_candidates_status": trade_candidates.get("status", "Stand Aside"),
        "trade_candidates_status_class": trade_candidates.get("status_class", "not-available"),
        "trade_candidates_message": trade_candidates.get("message", "No trade candidates were produced."),
        "trade_candidates_count": trade_candidates.get("candidate_count", 0),
        "trade_candidates_valid_count": valid_trade_candidate_count,
        "trade_candidates_outcome_category": trade_candidates_outcome_category,
        "trade_candidates_outcome_label": trade_candidates_outcome_label,
        "trade_candidates_count_label": trade_candidates.get("count_label") or format_candidate_count_label(trade_candidates.get("candidate_count", 0)),
        "trade_candidates_underlying_price": format_value(trade_candidates.get("underlying_price")),
        "trade_candidates_expected_move": format_value(trade_candidates.get("expected_move")),
        "trade_candidates_expected_move_range": trade_candidates.get("expected_move_range", "ΓÇö"),
        "trade_candidates_diagnostics": {
            **(trade_candidates.get("diagnostics", {}) or {}),
            "baseline_distance_points_display": format_value((trade_candidates.get("diagnostics", {}) or {}).get("baseline_distance_points")),
            "baseline_max_short_strike_display": format_value((trade_candidates.get("diagnostics", {}) or {}).get("baseline_max_short_strike")),
            "expected_move_display": format_value((trade_candidates.get("diagnostics", {}) or {}).get("expected_move")),
            "expected_move_1_5x_threshold_display": format_value((trade_candidates.get("diagnostics", {}) or {}).get("expected_move_1_5x_threshold")),
            "expected_move_2x_threshold_display": format_value((trade_candidates.get("diagnostics", {}) or {}).get("expected_move_2x_threshold")),
            "active_barrier_points_display": format_value((trade_candidates.get("diagnostics", {}) or {}).get("active_barrier_points")),
            "account_value_display": format_currency((trade_candidates.get("diagnostics", {}) or {}).get("account_value")),
            "base_structure_grade": (trade_candidates.get("diagnostics", {}) or {}).get("base_structure_grade", "Not available"),
            "rsi_modifier_applied": (trade_candidates.get("diagnostics", {}) or {}).get("rsi_modifier_applied", "No"),
            "rsi_modifier_label": (trade_candidates.get("diagnostics", {}) or {}).get("rsi_modifier_label", "None"),
            "evaluated_spread_details": [
                {
                    "short_strike": format_value(item.get("short_strike")),
                    "long_strike": format_value(item.get("long_strike")),
                    "distance_points": format_value(item.get("distance_points")),
                    "distance_percent": append_percent(item.get("distance_percent")),
                    "baseline_em_pass": "Pass" if item.get("baseline_em_pass") else "Fail",
                    "baseline_max_short_strike": format_value(item.get("baseline_max_short_strike")),
                    "expected_move": format_value(item.get("expected_move")),
                    "expected_move_1_5x_threshold": format_value(item.get("expected_move_1_5x_threshold")),
                    "two_x_expected_move_threshold": format_value(item.get("two_x_expected_move_threshold")),
                    "required_distance_mode": item.get("required_distance_mode", "ΓÇö"),
                    "active_rule_set": item.get("active_rule_set", "ΓÇö"),
                    "macro_modifier_status": item.get("macro_modifier_status", "No"),
                    "structure_modifier_status": item.get("structure_modifier_status", "No"),
                    "net_credit": format_currency(item.get("net_credit"), decimals=2),
                    "premium_received_dollars": format_currency(item.get("premium_received_dollars")),
                    "max_loss_dollars": format_currency(item.get("max_loss_dollars")),
                    "risk_cap_dollars": format_currency(item.get("risk_cap_dollars")),
                    "risk_cap_status": item.get("risk_cap_status", "ΓÇö"),
                    "original_contract_size": format_value(item.get("original_contract_size")),
                    "account_risk_percent": append_percent(item.get("account_risk_percent")),
                    "short_delta": format_value(item.get("short_delta")),
                    "contract_size_chosen": format_value(item.get("contract_size_chosen")),
                    "qualifies_for_full_size": "Yes" if item.get("qualifies_for_full_size") else "No",
                    "reject_reason": item.get("reject_reason", "ΓÇö"),
                }
                for item in (trade_candidates.get("diagnostics", {}) or {}).get("evaluated_spread_details", [])
                if isinstance(item, dict)
            ],
        },
        "trade_candidates_short_barrier_put": format_short_barrier(
            option_chain=option_chain,
            trade_candidates=trade_candidates,
            side="put",
        ),
        "trade_candidates_short_barrier_call": format_short_barrier(
            option_chain=option_chain,
            trade_candidates=trade_candidates,
            side="call",
        ),
        "trade_candidates_credit_map": trade_candidates_credit_map,
        "trade_candidates_items": trade_candidate_items,
        "apollo_trigger_source": trigger_source or "",
        "apollo_trigger_note": (f"Apollo was triggered by {trigger_source}." if trigger_source else ""),
        "reasons": apollo_data.get("reasons", []),
    }


def format_apollo_datetime(value: Any) -> str:
    """Format Apollo's local datetime field for display."""
    if isinstance(value, datetime):
        localized = value.astimezone(CHICAGO_TZ) if value.tzinfo else value.replace(tzinfo=CHICAGO_TZ)
        return localized.strftime("%a %Y-%m-%d %I:%M %p %Z").replace(" 0", " ")
    return str(value) if value is not None else "ΓÇö"


def format_candidate_count_label(value: Any) -> str:
    count = 0
    try:
        count = int(value or 0)
    except (TypeError, ValueError):
        count = 0
    return f"{count} Candidate" if count == 1 else f"{count} Candidates"


def format_currency(value: Any, decimals: int = 0) -> str:
    amount = _coerce_float(value)
    if amount is None:
        return "ΓÇö"
    sign = "-" if amount < 0 else ""
    return f"{sign}${abs(amount):,.{decimals}f}"


def format_em_multiple(value: Any) -> str:
    multiple = _coerce_float(value)
    if multiple is None:
        return "ΓÇö"
    return f"{multiple:,.2f}x EM"


def format_ratio(value: Any, decimals: int = 4) -> str:
    ratio = _coerce_float(value)
    if ratio is None:
        return "ΓÇö"
    return f"{ratio:,.{decimals}f}"


def format_ratio_percent(value: Any) -> str:
    ratio = _coerce_float(value)
    if ratio is None:
        return "ΓÇö"
    return f"{ratio * 100:,.1f}%"


def get_real_trade_outcome_profile() -> Dict[str, Any]:
    trade_store = current_app.extensions.get("trade_store") if current_app else None
    if trade_store is None:
        return {
            "routine_loss_percentage": 0.0,
            "black_swan_loss_percentage": 0.0,
        }
    try:
        return trade_store.build_real_trade_outcome_profile()
    except Exception as exc:  # pragma: no cover - defensive fallback for hosted reachability issues
        if current_app:
            current_app.logger.warning("Falling back to neutral Apollo historical loss profile: %s", exc)
        return {
            "routine_loss_percentage": 0.0,
            "black_swan_loss_percentage": 0.0,
            "loss_count": 0,
            "black_swan_count": 0,
        }


def project_historical_loss(max_loss: Any, percentage: Any) -> float | None:
    max_loss_value = _coerce_float(max_loss)
    percentage_value = _coerce_float(percentage)
    if max_loss_value is None or percentage_value is None:
        return None
    return max_loss_value * abs(percentage_value)


def build_candidate_probability_labels(short_delta: Any, long_delta: Any) -> Dict[str, str]:
    short_delta_value = clamp_probability_fraction(short_delta)
    long_delta_value = clamp_probability_fraction(long_delta)

    premium_probability = None if short_delta_value is None else (1.0 - short_delta_value)
    routine_probability = None if short_delta_value is None or long_delta_value is None else (short_delta_value - long_delta_value)
    tail_probability = long_delta_value

    return {
        "premium": format_probability(premium_probability),
        "routine": format_probability(routine_probability),
        "tail": format_probability(tail_probability),
        "max": "<1%",
    }


def clamp_probability_fraction(value: Any) -> float | None:
    amount = _coerce_float(value)
    if amount is None:
        return None
    return max(0.0, min(1.0, amount))


def format_probability(value: Any) -> str:
    probability = clamp_probability_fraction(value)
    if probability is None:
        return "ΓÇö"
    return f"{probability * 100:.0f}%"


def build_trade_candidates_credit_map(option_chain: Dict[str, Any], trade_candidates: Dict[str, Any]) -> Dict[str, Any]:
    candidates = [item for item in (trade_candidates.get("candidates") or []) if isinstance(item, dict) and item.get("available")]
    spot = _coerce_float(trade_candidates.get("underlying_price") or option_chain.get("underlying_price"))
    expected_move = _coerce_float(trade_candidates.get("expected_move"))
    put_barrier = compute_short_barrier_value(option_chain=option_chain, trade_candidates=trade_candidates, side="put")
    call_barrier = compute_short_barrier_value(option_chain=option_chain, trade_candidates=trade_candidates, side="call")
    lower_em = (spot - expected_move) if spot is not None and expected_move is not None else None
    upper_em = (spot + expected_move) if spot is not None and expected_move is not None else None

    if spot is None:
        return {"available": False, "guides": [], "markers": [], "regions": []}

    plot_left_x = 130.0
    plot_right_x = 920.0
    baseline_y = 164.0
    floor_y = 146.0
    peak_y = 46.0
    spot_line_top_y = 28.0

    candidate_ranges = []
    has_put_side_positions = False
    has_call_side_positions = False
    relevant_values = [value for value in [spot, put_barrier, lower_em, upper_em, call_barrier] if value is not None]

    for item in candidates:
        short_strike = _coerce_float(item.get("short_strike"))
        long_strike = _coerce_float(item.get("long_strike"))
        if short_strike is None or long_strike is None:
            continue
        low_strike = min(short_strike, long_strike)
        high_strike = max(short_strike, long_strike)
        candidate_ranges.append(
            {
                "mode_key": str(item.get("mode_key", "mode")),
                "mode_label": str(item.get("mode_label", "Mode")),
                "short_strike": short_strike,
                "long_strike": long_strike,
                "low_strike": low_strike,
                "high_strike": high_strike,
                "premium": _coerce_float(item.get("premium_received_dollars")) or 0.0,
            }
        )
        relevant_values.extend([short_strike, long_strike])
        if high_strike <= spot:
            has_put_side_positions = True
        if low_strike >= spot:
            has_call_side_positions = True

    if not relevant_values:
        return {"available": False, "guides": [], "markers": [], "regions": []}

    left_reach = max((spot - value) for value in relevant_values if value <= spot) if any(value <= spot for value in relevant_values) else 0.0
    right_reach = max((value - spot) for value in relevant_values if value >= spot) if any(value >= spot for value in relevant_values) else 0.0

    data_span = max(max(relevant_values) - min(relevant_values), 1.0)
    base_padding = max(data_span * 0.08, 8.0)
    left_span = max(left_reach + base_padding, 1.0)
    right_span = max(right_reach + base_padding, 1.0)
    if has_put_side_positions and not has_call_side_positions:
        right_span = max(right_span, left_span * 0.58)
    elif has_call_side_positions and not has_put_side_positions:
        left_span = max(left_span, right_span * 0.58)
    else:
        shared_span = max(left_span, right_span)
        left_span = shared_span
        right_span = shared_span

    left_bound = spot - left_span
    right_bound = spot + right_span
    if right_bound <= left_bound:
        left_bound = spot - 1.0
        right_bound = spot + 1.0

    def to_x(value: float | None) -> float:
        if value is None:
            return plot_left_x + ((plot_right_x - plot_left_x) / 2)
        proportion = (value - left_bound) / (right_bound - left_bound)
        proportion = max(0.0, min(1.0, proportion))
        return plot_left_x + ((plot_right_x - plot_left_x) * proportion)

    def build_profile_paths(spot_x: float) -> tuple[str, str]:
        left_span_px = max(spot_x - plot_left_x, 1.0)
        right_span_px = max(plot_right_x - spot_x, 1.0)
        line_path = (
            f"M{plot_left_x:.1f} {floor_y:.1f} "
            f"C{(plot_left_x + left_span_px * 0.42):.1f} {floor_y:.1f}, {(plot_left_x + left_span_px * 0.78):.1f} {(peak_y + 58.0):.1f}, {spot_x:.1f} {peak_y:.1f} "
            f"C{(spot_x + right_span_px * 0.22):.1f} {(peak_y + 58.0):.1f}, {(spot_x + right_span_px * 0.58):.1f} {floor_y:.1f}, {plot_right_x:.1f} {floor_y:.1f}"
        )
        area_path = f"{line_path} L{plot_right_x:.1f} {baseline_y:.1f} L{plot_left_x:.1f} {baseline_y:.1f} Z"
        return line_path, area_path

    max_premium = max((_coerce_float(item.get("premium_received_dollars")) or 0.0) for item in candidates) if candidates else 0.0
    max_premium = max(max_premium, 1.0)

    guides = []
    for ratio in (1.0, 0.5, 0.2):
        y = 154.0 - (ratio * 92.0)
        guides.append({"y": round(y, 1), "label": format_currency(max_premium * ratio)})
    guides.append({"y": 164.0, "label": "$0"})

    markers = []
    short_labels = {"standard": "S", "aggressive": "A", "fortress": "F"}
    spread_regions = []
    spot_x = round(to_x(spot), 1)
    profile_line_path, profile_area_path = build_profile_paths(spot_x)

    for item in candidate_ranges:
        mode_key = item["mode_key"]
        premium = item["premium"]
        short_strike = item["short_strike"]
        long_strike = item["long_strike"]
        low_x = round(to_x(item["low_strike"]), 1)
        high_x = round(to_x(item["high_strike"]), 1)
        y = 154.0 - ((premium / max_premium) * 92.0)
        markers.append(
            {
                "mode_key": mode_key,
                "mode_label": item["mode_label"],
                "marker_label": short_labels.get(mode_key, mode_key[:1].upper()),
                "short_strike_label": format_value(short_strike),
                "long_strike_label": format_value(long_strike),
                "premium_label": format_currency(premium),
                "x": round(to_x(short_strike), 1),
                "y": round(max(56.0, min(154.0, y)), 1),
            }
        )
        spread_regions.append(
            {
                "mode_key": mode_key,
                "mode_label": item["mode_label"],
                "short_strike_label": format_value(short_strike),
                "long_strike_label": format_value(long_strike),
                "x": low_x,
                "width": round(max(high_x - low_x, 3.0), 1),
                "short_x": round(to_x(short_strike), 1),
                "long_x": round(to_x(long_strike), 1),
            }
        )

    show_upside_reference = not (has_put_side_positions and not has_call_side_positions)
    barrier_markers = [
        {
            "key": "put-barrier",
            "label": "Put Barrier",
            "value_label": format_value(put_barrier),
            "x": round(to_x(put_barrier), 1),
            "line_top_y": 132.0,
            "line_bottom_y": 176.0,
            "show": put_barrier is not None,
            "css_class": "apollo-risk-reference-danger",
        },
        {
            "key": "em-lower",
            "label": "EM Lower",
            "value_label": format_value(lower_em),
            "x": round(to_x(lower_em), 1),
            "line_top_y": 124.0,
            "line_bottom_y": 176.0,
            "show": lower_em is not None,
            "css_class": "apollo-risk-reference-em",
        },
        {
            "key": "em-upper",
            "label": "EM Upper",
            "value_label": format_value(upper_em),
            "x": round(to_x(upper_em), 1),
            "line_top_y": 124.0,
            "line_bottom_y": 176.0,
            "show": show_upside_reference and upper_em is not None,
            "css_class": "apollo-risk-reference-em",
        },
        {
            "key": "call-barrier",
            "label": "Call Barrier",
            "value_label": format_value(call_barrier),
            "x": round(to_x(call_barrier), 1),
            "line_top_y": 132.0,
            "line_bottom_y": 176.0,
            "show": show_upside_reference and call_barrier is not None,
            "css_class": "apollo-risk-reference-safe",
        },
    ]

    return {
        "available": True,
        "guides": guides,
        "baseline_y": baseline_y,
        "plot_left_x": plot_left_x,
        "plot_right_x": plot_right_x,
        "peak_y": peak_y,
        "spot_line_top_y": spot_line_top_y,
        "profile_line_path": profile_line_path,
        "profile_area_path": profile_area_path,
        "spot_x": spot_x,
        "put_barrier_x": round(to_x(put_barrier), 1),
        "em_lower_x": round(to_x(lower_em), 1),
        "em_upper_x": round(to_x(upper_em), 1),
        "call_barrier_x": round(to_x(call_barrier), 1),
        "spot_label": format_value(spot),
        "put_barrier_label": format_value(put_barrier),
        "em_lower_label": format_value(lower_em),
        "em_upper_label": format_value(upper_em),
        "call_barrier_label": format_value(call_barrier),
        "positioning_note": (
            "Put-side only positioning shifts SPX right to reveal more downside spread spacing."
            if has_put_side_positions and not has_call_side_positions
            else "Balanced positioning keeps SPX centered when both put and call sides matter."
        ),
        "spot_shifted_right": has_put_side_positions and not has_call_side_positions,
        "show_upside_reference": show_upside_reference,
        "barrier_markers": [item for item in barrier_markers if item["show"]],
        "markers": markers,
        "regions": spread_regions,
    }


def compute_short_barrier_value(option_chain: Dict[str, Any], trade_candidates: Dict[str, Any], side: str) -> float | None:
    diagnostics = trade_candidates.get("diagnostics") or {}
    spot = _coerce_float(trade_candidates.get("underlying_price") or option_chain.get("underlying_price"))
    if spot is None:
        return None

    active_distance_points = _coerce_float(diagnostics.get("active_barrier_points"))
    if active_distance_points is None:
        return None

    contracts = option_chain.get("puts" if side == "put" else "calls") or []
    strikes = sorted({_coerce_float(item.get("strike")) for item in contracts if _coerce_float(item.get("strike")) is not None})
    if side == "put":
        target = spot - active_distance_points
        return max((strike for strike in strikes if strike <= target), default=target)

    target = spot + active_distance_points
    return min((strike for strike in strikes if strike >= target), default=target)


def format_short_barrier(option_chain: Dict[str, Any], trade_candidates: Dict[str, Any], side: str) -> str:
    barrier = compute_short_barrier_value(option_chain=option_chain, trade_candidates=trade_candidates, side=side)
    if barrier is None:
        return "ΓÇö"
    if side == "put":
        formatted = format_value(barrier)
        return f"<= {formatted}"
    formatted = format_value(barrier)
    return f">= {formatted}"


def _coerce_float(value: Any) -> float | None:
    if value in (None, "", "ΓÇö"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def append_percent(value: Any) -> str:
    """Format a percentage summary value."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "ΓÇö"
    return f"{float(value):,.2f}%"


def format_value(value: Any) -> str:
    """Format a value for display in the HTML table or summary cards."""
    if value is None:
        return "ΓÇö"
    if isinstance(value, float) and pd.isna(value):
        return "ΓÇö"
    if isinstance(value, pd.Timestamp):
        timestamp = value.to_pydatetime()
        if timestamp.tzinfo is not None:
            timestamp = timestamp.astimezone(CHICAGO_TZ)
        return timestamp.strftime("%Y-%m-%d %I:%M:%S %p %Z")
    if isinstance(value, datetime):
        timestamp = value.astimezone(CHICAGO_TZ) if value.tzinfo else value
        return timestamp.strftime("%Y-%m-%d %I:%M:%S %p %Z") if value.tzinfo else timestamp.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, float):
        return f"{value:,.2f}"
    return str(value)


def open_browser() -> None:
    """Open the local app in the default browser."""
    root_app = _resolve_flask_container(None)
    get_launch_behavior(root_app).launch(get_runtime_profile(root_app))


def get_launch_url() -> str:
    """Return the local URL that should open in the browser."""
    return get_runtime_profile().launch_url


def should_use_https() -> bool:
    """Return whether the local app should start with an HTTPS dev server."""
    return get_runtime_profile().use_https


def get_runtime_profile(app: Optional[Flask] = None) -> RuntimeProfile:
    container = _resolve_flask_container(app)
    runtime_profile = container.extensions.get("runtime_profile")
    if runtime_profile is None:
        runtime_profile = select_runtime_profile(container, get_runtime_app_config(container))
        container.extensions["runtime_profile"] = runtime_profile
    return runtime_profile


def ensure_runtime_ssl_context(runtime_profile: RuntimeProfile, *, app: Optional[Flask] = None) -> tuple[str, str] | None:
    """Return a usable SSL context for the runtime, generating local dev certs when needed."""
    ssl_context = runtime_profile.ssl_context
    if ssl_context is None:
        return None

    cert_path = Path(ssl_context[0])
    key_path = Path(ssl_context[1])
    if cert_path.exists() and key_path.exists():
        return (str(cert_path), str(key_path))

    if runtime_profile.host not in LOCAL_DEV_HOSTS:
        raise FileNotFoundError(f"SSL certificate files are missing for host {runtime_profile.host}: {cert_path}, {key_path}")

    from werkzeug.serving import make_ssl_devcert

    container = _resolve_flask_container(app)
    cert_base_path = cert_path.with_suffix("")
    if cert_path.name == f"{LOCALHOST_DEV_CERT_BASENAME}.pem" and key_path.name == f"{LOCALHOST_DEV_CERT_BASENAME}-key.pem":
        cert_base_path = cert_path.with_name(LOCALHOST_DEV_CERT_BASENAME)

    cert_path.parent.mkdir(parents=True, exist_ok=True)
    make_ssl_devcert(str(cert_base_path), host=runtime_profile.host)

    if not cert_path.exists() or not key_path.exists():
        raise FileNotFoundError(f"Unable to generate local SSL certificate files: {cert_path}, {key_path}")

    container.logger.info("Generated local HTTPS development certificate | cert=%s | key=%s", cert_path, key_path)
    return (str(cert_path), str(key_path))


def build_runtime_startup_messages(root_app: Flask, runtime_profile: RuntimeProfile) -> list[str]:
    messages: list[str] = []
    if runtime_profile.use_https:
        messages.append(f"Running HTTPS on {runtime_profile.launch_url}")
    else:
        messages.append(f"Running HTTP on {runtime_profile.launch_url}")

    try:
        provider = get_market_data_service(root_app).provider
        auth_service = getattr(provider, "auth_service", None)
        if auth_service is not None:
            authorize_url = auth_service.build_authorization_url(state="startup-preview")
            messages.append(f"Schwab authorize URL: {authorize_url}")
    except Exception as exc:
        root_app.logger.warning("Unable to build Schwab authorize URL at startup: %s", exc)
    return messages


def get_launch_behavior(app: Optional[Flask] = None) -> LaunchBehavior:
    container = _resolve_flask_container(app)
    launch_behavior = container.extensions.get("launch_behavior")
    if launch_behavior is None:
        launch_behavior = WebBrowserLaunchBehavior()
        container.extensions["launch_behavior"] = launch_behavior
    return launch_behavior


def get_runtime_lifecycle(app: Optional[Flask] = None) -> RuntimeLifecycleCoordinator:
    container = _resolve_flask_container(app)
    lifecycle = container.extensions.get("runtime_lifecycle")
    if lifecycle is None:
        runtime_profile = get_runtime_profile(container)
        lifecycle = LocalRuntimeLifecycleCoordinator(
            runtime_profile,
            launch_behavior=get_launch_behavior(container),
            scheduler=ThreadingTimerScheduler(),
            shutdown_registrar=__import__("atexit").register,
        )
        container.extensions["runtime_lifecycle"] = lifecycle
    return lifecycle


def _resolve_flask_container(app: Optional[Flask]) -> Flask:
    if app is not None:
        return app
    if has_app_context():
        return current_app
    return globals()["app"]


app = create_app()


if __name__ == "__main__":
    runtime_profile = get_runtime_profile(app)
    resolved_ssl_context = ensure_runtime_ssl_context(runtime_profile, app=app)
    for startup_message in build_runtime_startup_messages(app, runtime_profile):
        print(startup_message)
    get_runtime_lifecycle(app).schedule_launch()
    app.run(
        host="0.0.0.0",
        port=runtime_profile.port,
        debug=False,
        use_reloader=False,
        ssl_context=resolved_ssl_context,
    )
