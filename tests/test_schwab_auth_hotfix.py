from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from app import (
    EXECUTION_OAUTH_CALLBACK_ROUTE,
    LEGACY_EXECUTION_OAUTH_CALLBACK_ROUTE,
    MARKET_OAUTH_CALLBACK_ROUTE,
    build_runtime_startup_messages,
    build_startup_menu_payload,
    create_app,
    get_runtime_profile,
    migrate_legacy_schwab_market_token,
    redact_authorize_url,
    resolve_runtime_app_config,
    resolve_schwab_connection_status,
    validate_persisted_schwab_token,
)
from config import AppConfig, DEFAULT_SCHWAB_MARKET_TOKEN_PATH, HOSTED_PRODUCTION_TRADING_CALLBACK_URL
from services.providers.base_provider import ProviderAuthRequiredError, ProviderReauthenticationRequiredError
from services.providers.schwab_provider import SchwabProvider
from services.repositories.token_repository import JsonFileTokenRepository
from services.runtime.auth_composition import LocalAuthComposer
from services.schwab_auth_service import SchwabAuthService


class StubResponse:
    def __init__(self, status_code: int, payload: dict | None = None) -> None:
        self.status_code = status_code
        self._payload = payload or {}

    def json(self) -> dict:
        return dict(self._payload)


class StubMarketDataService:
    def __init__(self, auth_service: SchwabAuthService) -> None:
        self.live_provider = type("Provider", (), {"auth_service": auth_service})()

    @staticmethod
    def get_provider_metadata() -> dict:
        return {
            "live_provider_key": "schwab",
            "requires_auth": True,
        }


class StubAuthService:
    def __init__(self, token_store: JsonFileTokenRepository) -> None:
        self.token_store = token_store


class StubProviderAuthService:
    def __init__(self) -> None:
        self.mark_reauthentication_required_called = False

    @staticmethod
    def get_valid_access_token() -> str:
        return "access-token"

    @staticmethod
    def recover_from_unauthorized_response() -> str:
        return "refreshed-access-token"

    def mark_reauthentication_required(self, message: str) -> None:
        self.mark_reauthentication_required_called = True


def build_config(**overrides) -> AppConfig:
    return AppConfig(
        schwab_client_id="client-id",
        schwab_client_secret="client-secret",
        schwab_redirect_uri="https://127.0.0.1:5015/callback",
        market_data_provider="schwab",
        market_data_live_provider="schwab",
        vix_historical_provider="schwab",
        spx_historical_provider="schwab",
        **overrides,
    )


def utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


class SchwabAuthHotfixTests(unittest.TestCase):
    def test_redact_authorize_url_masks_sensitive_query_values(self) -> None:
        redacted = redact_authorize_url(
            "https://example.com/oauth?client_id=abc123&client_secret=secret123&redirect_uri=https%3A%2F%2Ftalos.eigeltrade.com%2Fcallback"
        )

        self.assertIn("client_id=REDACTED", redacted)
        self.assertIn("client_secret=REDACTED", redacted)
        self.assertIn("redirect_uri=https%3A%2F%2Ftalos.eigeltrade.com%2Fcallback", redacted)

    def test_production_trading_callback_uses_auth_schwab_callback(self) -> None:
        self.assertEqual(HOSTED_PRODUCTION_TRADING_CALLBACK_URL, "https://talos.eigeltrade.com/auth/schwab/callback")

    def test_market_redirect_alias_env_is_accepted(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "SCHWAB_REDIRECT_URI": "",
                "SCHWAB_MARKET_REDIRECT_URI": "https://talos.eigeltrade.com/callback",
            },
            clear=False,
        ):
            resolved_config = AppConfig.from_env()

        self.assertEqual(resolved_config.schwab_redirect_uri, "https://talos.eigeltrade.com/callback")

    def test_app_exposes_market_and_execution_callback_routes(self) -> None:
        app = create_app({"TESTING": True, "RUNTIME_TARGET": "hosted", "HOSTED_PUBLIC_BASE_URL": "https://127.0.0.1:5015"})

        routes = {rule.rule for rule in app.url_map.iter_rules()}

        self.assertIn(MARKET_OAUTH_CALLBACK_ROUTE, routes)
        self.assertIn(EXECUTION_OAUTH_CALLBACK_ROUTE, routes)
        self.assertIn(LEGACY_EXECUTION_OAUTH_CALLBACK_ROUTE, routes)

    def test_startup_messages_log_registered_callback_routes(self) -> None:
        app = create_app({"TESTING": True, "RUNTIME_TARGET": "hosted", "HOSTED_PUBLIC_BASE_URL": "https://127.0.0.1:5015"})
        profile = get_runtime_profile(app)

        messages = build_runtime_startup_messages(app, profile)

        self.assertIn(f"Registered market callback route: {MARKET_OAUTH_CALLBACK_ROUTE}", messages)
        self.assertIn(f"Registered execution callback route: {EXECUTION_OAUTH_CALLBACK_ROUTE}", messages)

    def test_refreshing_market_auth_still_requires_header_reconnect_button(self) -> None:
        with TemporaryDirectory() as temp_dir:
            token_path = Path(temp_dir) / "schwab-token.json"
            repository = JsonFileTokenRepository(token_path)
            repository.save(
                {
                    "access_token": "expired-access",
                    "refresh_token": "refresh-token",
                    "expires_at": (utcnow() - timedelta(minutes=5)).isoformat(),
                    "refresh_expires_at": (utcnow() + timedelta(days=5)).isoformat(),
                    "auth_state": "connected",
                    "last_auth_error": "Schwab refresh token was rejected. Please log in again.",
                }
            )
            auth_service = SchwabAuthService(config=build_config(), token_store=repository)
            market_service = StubMarketDataService(auth_service)

            menu_status = build_startup_menu_payload(market_service)

            self.assertTrue(menu_status["connection_requires_login"])

    def test_default_market_token_path_is_one_authoritative_instance_file(self) -> None:
        config = build_config()

        self.assertEqual(config.schwab_token_path, DEFAULT_SCHWAB_MARKET_TOKEN_PATH)
        self.assertEqual(config.schwab_shared_market_token_path, DEFAULT_SCHWAB_MARKET_TOKEN_PATH)

    def test_local_auth_composer_uses_shared_market_token_path(self) -> None:
        config = build_config(
            schwab_token_path="execution-token.json",
            schwab_shared_market_token_path="instance/shared-market-token.json",
        )

        repository = LocalAuthComposer(config).create_token_repository()

        self.assertEqual(repository.file_path, Path("instance/shared-market-token.json"))

    def test_legacy_root_token_is_migrated_to_authoritative_instance_path(self) -> None:
        with TemporaryDirectory() as temp_dir:
            workspace_root = Path(temp_dir)
            legacy_token_path = workspace_root / "schwab_token.json"
            target_token_path = workspace_root / "instance" / "schwab_market_data_token.json"
            legacy_token_path.write_text('{"access_token": "legacy"}', encoding="utf-8")

            migrated_path = migrate_legacy_schwab_market_token(target_token_path, workspace_root=workspace_root)

            self.assertEqual(migrated_path, target_token_path)
            self.assertFalse(legacy_token_path.exists())
            self.assertTrue(target_token_path.exists())
            self.assertIn("legacy", target_token_path.read_text(encoding="utf-8"))

    def test_refresh_failure_preserves_token_payload(self) -> None:
        with TemporaryDirectory() as temp_dir:
            token_path = Path(temp_dir) / "schwab-token.json"
            repository = JsonFileTokenRepository(token_path)
            repository.save(
                {
                    "access_token": "expired-access",
                    "refresh_token": "refresh-token",
                    "expires_at": (utcnow() - timedelta(minutes=5)).isoformat(),
                    "refresh_expires_at": (utcnow() + timedelta(days=6)).isoformat(),
                    "auth_state": "connected",
                    "last_auth_error": "",
                }
            )
            service = SchwabAuthService(config=build_config(), token_store=repository)

            with patch("services.schwab_auth_service.requests.post", return_value=StubResponse(400)):
                with self.assertRaises(ProviderReauthenticationRequiredError):
                    service.get_valid_access_token()

            persisted = repository.load()
            self.assertIsNotNone(persisted)
            self.assertEqual(persisted["refresh_token"], "refresh-token")
            self.assertEqual(persisted["auth_state"], "refresh_expired")
            self.assertTrue(token_path.exists())

    def test_validate_persisted_schwab_token_requires_access_token(self) -> None:
        with TemporaryDirectory() as temp_dir:
            token_path = Path(temp_dir) / "schwab-token.json"
            repository = JsonFileTokenRepository(token_path)
            repository.save({"refresh_token": "refresh-only"})

            with self.assertRaises(RuntimeError):
                validate_persisted_schwab_token(StubAuthService(repository))

    def test_validate_persisted_schwab_token_accepts_existing_file(self) -> None:
        with TemporaryDirectory() as temp_dir:
            token_path = Path(temp_dir) / "schwab-token.json"
            repository = JsonFileTokenRepository(token_path)
            repository.save({"access_token": "usable-token", "refresh_token": "refresh-token"})

            payload = validate_persisted_schwab_token(StubAuthService(repository))

            self.assertEqual(payload["access_token"], "usable-token")

    def test_connection_status_shows_refreshing_when_refresh_token_is_still_valid(self) -> None:
        with TemporaryDirectory() as temp_dir:
            repository = JsonFileTokenRepository(Path(temp_dir) / "schwab-token.json")
            repository.save(
                {
                    "access_token": "expired-access",
                    "refresh_token": "refresh-token",
                    "expires_at": (utcnow() - timedelta(minutes=2)).isoformat(),
                    "refresh_expires_at": (utcnow() + timedelta(days=5)).isoformat(),
                    "auth_state": "connected",
                    "last_auth_error": "",
                }
            )
            service = SchwabAuthService(config=build_config(), token_store=repository)

            status = resolve_schwab_connection_status(StubMarketDataService(service))

            self.assertEqual(status["status_label"], "Refreshing Schwab token")
            self.assertFalse(status["requires_login"])
            self.assertTrue(status["requires_refresh"])

    def test_stale_refresh_expired_state_does_not_block_valid_access_token(self) -> None:
        with TemporaryDirectory() as temp_dir:
            repository = JsonFileTokenRepository(Path(temp_dir) / "schwab-token.json")
            repository.save(
                {
                    "access_token": "still-valid-access",
                    "refresh_token": "refresh-token",
                    "expires_at": (utcnow() + timedelta(minutes=20)).isoformat(),
                    "refresh_expires_at": (utcnow() + timedelta(days=5)).isoformat(),
                    "auth_state": "refresh_expired",
                    "last_auth_error": "Schwab authentication expired. Please log in again.",
                }
            )
            service = SchwabAuthService(config=build_config(), token_store=repository)

            token = service.get_valid_access_token()
            persisted = repository.load()

            self.assertEqual(token, "still-valid-access")
            self.assertEqual(persisted["auth_state"], "connected")
            self.assertEqual(persisted["last_auth_error"], "")

    def test_account_endpoint_401_does_not_mark_shared_token_reauthentication_required(self) -> None:
        auth_service = StubProviderAuthService()
        provider = SchwabProvider(config=build_config(), auth_service=auth_service)

        with patch(
            "services.providers.schwab_provider.requests.get",
            side_effect=[StubResponse(401), StubResponse(401)],
        ):
            with self.assertRaises(ProviderAuthRequiredError):
                provider.get_account_summary()

        self.assertFalse(auth_service.mark_reauthentication_required_called)

    def test_option_chain_attempts_are_built_from_real_param_dicts(self) -> None:
        provider = SchwabProvider(config=build_config(), auth_service=StubProviderAuthService())

        params = provider.build_schwab_option_chain_params(
            symbol="^GSPC",
            expiration_date=datetime(2026, 5, 13).date(),
            include_underlying_quote=True,
            strike_count=12,
        )
        attempts = provider._build_option_chain_attempts("$SPX", datetime(2026, 5, 13).date())

        self.assertIsInstance(params, dict)
        self.assertEqual(params["symbol"], "$SPX")
        self.assertEqual(params["contractType"], "PUT")
        self.assertEqual(params["fromDate"], "2026-05-13")
        self.assertEqual(params["toDate"], "2026-05-13")
        self.assertEqual(params["includeUnderlyingQuote"], "true")
        self.assertEqual(params["strikeCount"], "12")
        self.assertTrue(all(isinstance(item[1], dict) for item in attempts))


if __name__ == "__main__":
    unittest.main()