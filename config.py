"""Application configuration loaded from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional dependency during bootstrap
    load_dotenv = None


HOSTED_RUNTIME_HOST = "127.0.0.1"
HOSTED_RUNTIME_PORT = 5015
HOSTED_RUNTIME_BASE_URL = f"https://{HOSTED_RUNTIME_HOST}:{HOSTED_RUNTIME_PORT}"
HOSTED_PRODUCTION_PUBLIC_BASE_URL = "https://talos.eigeltrade.com"
HOSTED_PRODUCTION_CALLBACK_URL = f"{HOSTED_PRODUCTION_PUBLIC_BASE_URL}/callback"
HOSTED_PRODUCTION_TRADING_CALLBACK_URL = "https://talos.eigeltrade.com/auth/schwab-trading/callback"
LOCAL_SCHWAB_TRADING_REDIRECT_URI = f"{HOSTED_RUNTIME_BASE_URL}/auth/schwab-trading/callback"
HOSTED_APP_VERSION = "3.10"
HOSTED_APP_DISPLAY_NAME = f"Talos {HOSTED_APP_VERSION}"
HOSTED_APP_PAGE_KICKER = f"TALOS {HOSTED_APP_VERSION}"
HOSTED_APP_VERSION_LABEL = f"TALOS {HOSTED_APP_VERSION}"
HOSTED_SESSION_COOKIE_NAME = "delphi5_hosted_session"
HOSTED_OAUTH_SESSION_NAMESPACE = "delphi5hosted"
DEFAULT_SCHWAB_MARKET_TOKEN_PATH = str(Path("instance") / "schwab_market_data_token.json")
DEFAULT_SCHWAB_TRADING_TOKEN_PATH = str(Path("instance") / "schwab_trading_token.json")


def is_render_production_environment(
    *,
    hosted_public_base_url: str = "",
    deployment_env: str = "",
) -> bool:
    normalized_base_url = str(hosted_public_base_url or "").strip().lower()
    normalized_env = str(deployment_env or os.getenv("DELPHI_DEPLOYMENT_ENV") or "").strip().lower()
    return (
        str(os.getenv("RENDER") or "").strip().lower() == "true"
        or bool(str(os.getenv("RENDER_SERVICE_ID") or "").strip())
        or normalized_base_url.startswith(HOSTED_PRODUCTION_PUBLIC_BASE_URL)
        or normalized_env == "production"
    )


def _default_schwab_token_path() -> str:
    configured_path = os.getenv("SCHWAB_TOKEN_PATH", "").strip()
    if configured_path:
        return configured_path
    return DEFAULT_SCHWAB_MARKET_TOKEN_PATH


def _default_schwab_shared_market_token_path() -> str:
    configured_path = os.getenv("SCHWAB_SHARED_MARKET_TOKEN_PATH", "").strip()
    if configured_path:
        return configured_path

    execution_path = os.getenv("SCHWAB_TOKEN_PATH", "").strip()
    if execution_path:
        return execution_path

    return DEFAULT_SCHWAB_MARKET_TOKEN_PATH


@dataclass(frozen=True)
class AppConfig:
    """Runtime configuration for the Flask app and market-data providers."""

    runtime_target: str = "local"
    hosted_public_base_url: str = ""
    supabase_url: str = ""
    supabase_publishable_key: str = ""
    supabase_secret_key: str = ""
    hosted_private_allowed_emails: str = ""
    hosted_access_token_cookie_name: str = "delphi_hosted_access_token"
    hosted_refresh_token_cookie_name: str = "delphi_hosted_refresh_token"
    app_host: str = "127.0.0.1"
    app_port: int = 5015
    app_display_name: str = HOSTED_APP_DISPLAY_NAME
    app_page_kicker: str = HOSTED_APP_PAGE_KICKER
    app_version_label: str = HOSTED_APP_VERSION_LABEL
    session_cookie_name: str = "delphi5_hosted_session"
    oauth_session_namespace: str = "delphi5hosted"
    kairos_replay_storage_dir: str = ""
    app_log_path: str = ""
    market_data_provider: str = "schwab"
    market_data_live_provider: str = "schwab"
    vix_historical_provider: str = "schwab"
    spx_historical_provider: str = "schwab"
    app_timezone: str = "America/Chicago"
    apollo_enabled: bool = True
    apollo_structure_source: str = "spx"
    apollo_structure_fallback_source: str = ""
    apollo_option_chain_source: str = "schwab"
    macro_provider: str = "marketwatch"
    apollo_account_value: float = 135000.0
    apollo_routine_loss_modifier: float = 0.50
    flask_secret_key: str = "horme-dev-secret-key"
    schwab_client_id: str = ""
    schwab_client_secret: str = ""
    schwab_redirect_uri: str = ""
    schwab_auth_url: str = "https://api.schwabapi.com/v1/oauth/authorize"
    schwab_token_url: str = "https://api.schwabapi.com/v1/oauth/token"
    schwab_base_url: str = "https://api.schwabapi.com/marketdata/v1"
    schwab_token_path: str = _default_schwab_token_path()
    schwab_shared_market_token_path: str = _default_schwab_shared_market_token_path()
    schwab_es_primary_symbol: str = "/ES"
    schwab_es_fallback_symbol: str = "ES"
    schwab_spx_option_chain_symbol: str = "$SPX"
    schwab_history_period_type: str = "year"
    schwab_history_period: str = "20"
    schwab_history_frequency_type: str = "daily"
    schwab_history_frequency: str = "1"
    schwab_history_need_extended_hours: bool = False
    schwab_trading_client_id: str = ""
    schwab_trading_client_secret: str = ""
    schwab_trading_redirect_uri: str = LOCAL_SCHWAB_TRADING_REDIRECT_URI
    schwab_trading_auth_url: str = "https://api.schwabapi.com/v1/oauth/authorize"
    schwab_trading_token_url: str = "https://api.schwabapi.com/v1/oauth/token"
    schwab_trading_base_url: str = "https://api.schwabapi.com/trader/v1"
    schwab_trading_token_path: str = DEFAULT_SCHWAB_TRADING_TOKEN_PATH
    schwab_trading_oauth_session_namespace: str = "schwabtrading"
    schwab_trading_account_hash: str = ""
    schwab_trading_account_number: str = ""
    talos_execution_enabled: bool = False
    talos_execution_account: str = ""
    talos_execution_account_name: str = ""
    talos_execution_cooldown_seconds: int = 45
    talos_allow_market_orders: bool = False
    talos_real_kill_switch: bool = False
    talos_candidate_cache_ttl_seconds: int = 20
    apollo_option_chain_cache_ttl_seconds: int = 30
    market_snapshot_cache_ttl_seconds: int = 30
    pushover_user_key: str = ""
    pushover_api_token: str = ""

    def __post_init__(self) -> None:
        if not self.schwab_trading_client_id and self.schwab_client_id:
            object.__setattr__(self, "schwab_trading_client_id", self.schwab_client_id)
        if not self.schwab_trading_client_secret and self.schwab_client_secret:
            object.__setattr__(self, "schwab_trading_client_secret", self.schwab_client_secret)

    @classmethod
    def from_env(cls) -> "AppConfig":
        """Create configuration from environment variables."""
        return cls(
            runtime_target=os.getenv("DELPHI_RUNTIME_TARGET", "local").strip().lower() or "local",
            hosted_public_base_url=os.getenv("HOSTED_PUBLIC_BASE_URL", "").strip(),
            supabase_url=os.getenv("SUPABASE_URL", "").strip(),
            supabase_publishable_key=os.getenv("SUPABASE_PUBLISHABLE_KEY", "").strip(),
            supabase_secret_key=os.getenv("SUPABASE_SECRET_KEY", "").strip(),
            hosted_private_allowed_emails=os.getenv("DELPHI_HOSTED_ALLOWED_EMAILS", "").strip(),
            hosted_access_token_cookie_name=os.getenv("DELPHI_HOSTED_ACCESS_TOKEN_COOKIE_NAME", "delphi_hosted_access_token").strip()
            or "delphi_hosted_access_token",
            hosted_refresh_token_cookie_name=os.getenv("DELPHI_HOSTED_REFRESH_TOKEN_COOKIE_NAME", "delphi_hosted_refresh_token").strip()
            or "delphi_hosted_refresh_token",
            app_host=os.getenv("APP_HOST", "127.0.0.1").strip() or "127.0.0.1",
            app_port=int(os.getenv("APP_PORT", "5015").strip() or "5015"),
            app_display_name=os.getenv("APP_DISPLAY_NAME", HOSTED_APP_DISPLAY_NAME).strip() or HOSTED_APP_DISPLAY_NAME,
            app_page_kicker=os.getenv("APP_PAGE_KICKER", HOSTED_APP_PAGE_KICKER).strip() or HOSTED_APP_PAGE_KICKER,
            app_version_label=os.getenv("APP_VERSION_LABEL", HOSTED_APP_VERSION_LABEL).strip() or HOSTED_APP_VERSION_LABEL,
            session_cookie_name=os.getenv("SESSION_COOKIE_NAME", "delphi5_hosted_session").strip() or "delphi5_hosted_session",
            oauth_session_namespace=os.getenv("OAUTH_SESSION_NAMESPACE", "delphi5hosted").strip().lower() or "delphi5hosted",
            kairos_replay_storage_dir=os.getenv("KAIROS_REPLAY_STORAGE_DIR", "").strip(),
            app_log_path=os.getenv("APP_LOG_PATH", "").strip(),
            market_data_provider=os.getenv("MARKET_DATA_PROVIDER", "schwab").strip().lower() or "schwab",
            market_data_live_provider=os.getenv("MARKET_DATA_LIVE_PROVIDER", "schwab").strip().lower() or "schwab",
            vix_historical_provider=os.getenv("VIX_HISTORICAL_PROVIDER", "schwab").strip().lower() or "schwab",
            spx_historical_provider=os.getenv("SPX_HISTORICAL_PROVIDER", "schwab").strip().lower() or "schwab",
            app_timezone=os.getenv("APP_TIMEZONE", "America/Chicago").strip() or "America/Chicago",
            apollo_enabled=(os.getenv("APOLLO_ENABLED", "true").strip().lower() == "true"),
            apollo_structure_source=os.getenv("APOLLO_STRUCTURE_SOURCE", "spx").strip().lower() or "spx",
            apollo_structure_fallback_source=os.getenv("APOLLO_STRUCTURE_FALLBACK_SOURCE", "").strip().lower(),
            apollo_option_chain_source=os.getenv("APOLLO_OPTION_CHAIN_SOURCE", "schwab").strip().lower() or "schwab",
            macro_provider=os.getenv("MACRO_PROVIDER", "marketwatch").strip().lower() or "marketwatch",
            apollo_account_value=float(os.getenv("APOLLO_ACCOUNT_VALUE", "135000").strip() or "135000"),
            apollo_routine_loss_modifier=float(os.getenv("APOLLO_ROUTINE_LOSS_MODIFIER", "0.50").strip() or "0.50"),
            flask_secret_key=os.getenv("FLASK_SECRET_KEY", "horme-dev-secret-key").strip() or "horme-dev-secret-key",
            schwab_client_id=os.getenv("SCHWAB_CLIENT_ID", "").strip(),
            schwab_client_secret=os.getenv("SCHWAB_CLIENT_SECRET", "").strip(),
            schwab_redirect_uri=os.getenv("SCHWAB_REDIRECT_URI", "").strip(),
            schwab_auth_url=os.getenv("SCHWAB_AUTH_URL", "https://api.schwabapi.com/v1/oauth/authorize").strip()
            or "https://api.schwabapi.com/v1/oauth/authorize",
            schwab_token_url=os.getenv("SCHWAB_TOKEN_URL", "https://api.schwabapi.com/v1/oauth/token").strip()
            or "https://api.schwabapi.com/v1/oauth/token",
            schwab_base_url=os.getenv("SCHWAB_BASE_URL", "https://api.schwabapi.com/marketdata/v1").strip()
            or "https://api.schwabapi.com/marketdata/v1",
            schwab_token_path=_default_schwab_token_path(),
            schwab_shared_market_token_path=_default_schwab_shared_market_token_path(),
            schwab_es_primary_symbol=os.getenv("SCHWAB_ES_PRIMARY_SYMBOL", "/ES").strip() or "/ES",
            schwab_es_fallback_symbol=os.getenv("SCHWAB_ES_FALLBACK_SYMBOL", "ES").strip() or "ES",
            schwab_spx_option_chain_symbol=os.getenv("SCHWAB_SPX_OPTION_CHAIN_SYMBOL", "$SPX").strip() or "$SPX",
            schwab_history_period_type=os.getenv("SCHWAB_HISTORY_PERIOD_TYPE", "year").strip() or "year",
            schwab_history_period=os.getenv("SCHWAB_HISTORY_PERIOD", "20").strip() or "20",
            schwab_history_frequency_type=os.getenv("SCHWAB_HISTORY_FREQUENCY_TYPE", "daily").strip() or "daily",
            schwab_history_frequency=os.getenv("SCHWAB_HISTORY_FREQUENCY", "1").strip() or "1",
            schwab_history_need_extended_hours=(os.getenv("SCHWAB_HISTORY_NEED_EXTENDED_HOURS", "false").strip().lower() == "true"),
            schwab_trading_client_id=os.getenv("SCHWAB_TRADING_CLIENT_ID", "").strip(),
            schwab_trading_client_secret=os.getenv("SCHWAB_TRADING_CLIENT_SECRET", "").strip(),
            schwab_trading_redirect_uri=os.getenv("SCHWAB_TRADING_REDIRECT_URI", LOCAL_SCHWAB_TRADING_REDIRECT_URI).strip()
            or LOCAL_SCHWAB_TRADING_REDIRECT_URI,
            schwab_trading_auth_url=os.getenv("SCHWAB_TRADING_AUTH_URL", "https://api.schwabapi.com/v1/oauth/authorize").strip()
            or "https://api.schwabapi.com/v1/oauth/authorize",
            schwab_trading_token_url=os.getenv("SCHWAB_TRADING_TOKEN_URL", "https://api.schwabapi.com/v1/oauth/token").strip()
            or "https://api.schwabapi.com/v1/oauth/token",
            schwab_trading_base_url=os.getenv("SCHWAB_TRADING_BASE_URL", "https://api.schwabapi.com/trader/v1").strip()
            or "https://api.schwabapi.com/trader/v1",
            schwab_trading_token_path=os.getenv("SCHWAB_TRADING_TOKEN_PATH", DEFAULT_SCHWAB_TRADING_TOKEN_PATH).strip()
            or DEFAULT_SCHWAB_TRADING_TOKEN_PATH,
            schwab_trading_oauth_session_namespace=os.getenv("SCHWAB_TRADING_OAUTH_SESSION_NAMESPACE", "schwabtrading").strip().lower()
            or "schwabtrading",
            schwab_trading_account_hash=os.getenv("SCHWAB_TRADING_ACCOUNT_HASH", "").strip(),
            schwab_trading_account_number=os.getenv("SCHWAB_TRADING_ACCOUNT_NUMBER", "").strip(),
            talos_execution_enabled=(os.getenv("TALOS_EXECUTION_ENABLED", "false").strip().lower() == "true"),
            talos_execution_account=os.getenv("TALOS_EXECUTION_ACCOUNT", "").strip(),
            talos_execution_account_name=os.getenv("TALOS_EXECUTION_ACCOUNT_NAME", "").strip(),
            talos_execution_cooldown_seconds=int(os.getenv("TALOS_EXECUTION_COOLDOWN_SECONDS", "45").strip() or "45"),
            talos_allow_market_orders=(os.getenv("TALOS_ALLOW_MARKET_ORDERS", "false").strip().lower() == "true"),
            talos_real_kill_switch=(os.getenv("TALOS_REAL_KILL_SWITCH", "false").strip().lower() == "true"),
            talos_candidate_cache_ttl_seconds=int(os.getenv("TALOS_CANDIDATE_CACHE_TTL_SECONDS", "20").strip() or "20"),
            apollo_option_chain_cache_ttl_seconds=int(os.getenv("APOLLO_OPTION_CHAIN_CACHE_TTL_SECONDS", "30").strip() or "30"),
            market_snapshot_cache_ttl_seconds=int(os.getenv("MARKET_SNAPSHOT_CACHE_TTL_SECONDS", "30").strip() or "30"),
            pushover_user_key=os.getenv("PUSHOVER_USER_KEY", "").strip(),
            pushover_api_token=os.getenv("PUSHOVER_API_TOKEN", "").strip(),
        )


@lru_cache(maxsize=1)
def get_app_config() -> AppConfig:
    """Return the cached application configuration."""
    if load_dotenv is not None:
        load_dotenv()
    return AppConfig.from_env()