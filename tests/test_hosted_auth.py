import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

from flask import Flask

from app import APP_CONFIG, create_app, get_runtime_app_config, resolve_runtime_app_config
from config import HOSTED_PRODUCTION_CALLBACK_URL
from services.runtime.hosted_auth import HostedAuthConfig, SupabaseHostedIdentityResolver, SupabasePrivateAccessGate, SupabaseSessionInvalidator
from services.runtime.private_access import AuthenticationRequiredError, PrivateAccessDeniedError, RequestIdentity
from services.repositories.hosted_runtime_state_repository import SupabaseTokenRepository
from services.schwab_auth_service import SchwabAuthService


class _AuthResponse:
    def __init__(self, payload, status_code=200):
        self.payload = payload
        self.status = status_code

    def read(self):
        return json.dumps(self.payload).encode("utf-8")

    def getcode(self):
        return self.status

    def close(self):
        return None


class HostedAuthTest(unittest.TestCase):
    def test_hosted_production_authorize_url_uses_eigeltrade_callback_only(self):
        app = Flask(__name__)
        app.config.update(
            {
                "RUNTIME_TARGET": "hosted",
                "HOSTED_PUBLIC_BASE_URL": "https://eigeltrade.com",
                "SCHWAB_REDIRECT_URI": "https://127.0.0.1:5015/callback",
                "SCHWAB_CLIENT_ID": "client-id",
                "SCHWAB_CLIENT_SECRET": "client-secret",
            }
        )

        with patch.dict("os.environ", {"DELPHI_DEPLOYMENT_ENV": "production"}, clear=False):
            runtime_config = resolve_runtime_app_config(app, APP_CONFIG)
        authorization_url = SchwabAuthService(runtime_config).build_authorization_url("state-123")
        parsed = urlparse(authorization_url)
        params = parse_qs(parsed.query)
        redirect_uri = params["redirect_uri"][0]

        self.assertEqual(redirect_uri, HOSTED_PRODUCTION_CALLBACK_URL)
        self.assertNotIn("localhost", redirect_uri.lower())
        self.assertNotIn("127.0.0.1", redirect_uri)
        self.assertNotIn(":5000", redirect_uri)
        self.assertNotIn(":5001", redirect_uri)
        self.assertNotIn(":5015", redirect_uri)

    def test_hosted_production_rejects_non_eigeltrade_public_base_url(self):
        app = Flask(__name__)
        app.config.update(
            {
                "RUNTIME_TARGET": "hosted",
                "HOSTED_PUBLIC_BASE_URL": "https://127.0.0.1:5015",
            }
        )

        with patch.dict("os.environ", {"DELPHI_DEPLOYMENT_ENV": "production"}, clear=False):
            with self.assertRaisesRegex(RuntimeError, "HOSTED_PUBLIC_BASE_URL=https://eigeltrade.com"):
                resolve_runtime_app_config(app, APP_CONFIG)

    def test_supabase_identity_resolver_reads_authenticated_user_from_bearer_token(self):
        app = create_app(
            {
                "TESTING": True,
                "RUNTIME_TARGET": "hosted",
                "SUPABASE_URL": "https://project.supabase.co",
                "SUPABASE_PUBLISHABLE_KEY": "publishable-key",
                "SUPABASE_SECRET_KEY": "secret-key",
                "DELPHI_HOSTED_ALLOWED_EMAILS": "bill@example.com",
                "TRADE_DATABASE": str(Path(tempfile.gettempdir()) / "hosted-auth-resolver.db"),
            }
        )
        auth_config = HostedAuthConfig.resolve(app, get_runtime_app_config(app), app.extensions["supabase_context"])

        def requester(request, timeout=0):
            self.assertEqual(request.headers["Authorization"], "Bearer token-123")
            return _AuthResponse({"id": "user-1", "email": "bill@example.com", "user_metadata": {"full_name": "Bill"}})

        resolver = SupabaseHostedIdentityResolver(
            context=app.extensions["supabase_context"],
            auth_config=auth_config,
            requester=requester,
        )

        with app.test_request_context("/hosted/private-access-check", headers={"Authorization": "Bearer token-123"}):
            identity = resolver.resolve_request_identity(__import__("flask").request)

        self.assertTrue(identity.authenticated)
        self.assertEqual(identity.email, "bill@example.com")
        self.assertEqual(identity.display_name, "Bill")

    def test_private_access_gate_enforces_bill_only_allowlist(self):
        gate = SupabasePrivateAccessGate(
            HostedAuthConfig(
                allowed_emails=("bill@example.com",),
                access_token_cookie_name="delphi_hosted_access_token",
                refresh_token_cookie_name="delphi_hosted_refresh_token",
                project_cookie_name="sb-project-auth-token",
            )
        )

        allowed = gate.require_private_access(
            RequestIdentity(user_id="user-1", email="bill@example.com", display_name="Bill", authenticated=True, auth_source="supabase-hosted")
        )
        self.assertTrue(allowed.private_access_granted)

        with self.assertRaises(AuthenticationRequiredError):
            gate.require_private_access(RequestIdentity(authenticated=False, auth_source="supabase-hosted"))

        with self.assertRaises(PrivateAccessDeniedError):
            gate.require_private_access(
                RequestIdentity(user_id="user-2", email="other@example.com", display_name="Other", authenticated=True, auth_source="supabase-hosted")
            )

    def test_local_runtime_keeps_hosted_private_route_inactive(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = create_app({"TESTING": True, "RUNTIME_TARGET": "local", "TRADE_DATABASE": str(Path(temp_dir) / "local-auth.db")})
            response = app.test_client().get("/hosted/private-access-check")
            self.assertEqual(response.status_code, 404)

    def test_hosted_private_access_route_requires_authenticated_bill_identity(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = create_app(
                {
                    "TESTING": True,
                    "RUNTIME_TARGET": "hosted",
                    "SUPABASE_URL": "https://project.supabase.co",
                    "SUPABASE_PUBLISHABLE_KEY": "publishable-key",
                    "SUPABASE_SECRET_KEY": "secret-key",
                    "DELPHI_HOSTED_ALLOWED_EMAILS": "bill@example.com",
                    "TRADE_DATABASE": str(Path(temp_dir) / "hosted-private-access.db"),
                }
            )

            app.extensions["request_identity_resolver"] = type(
                "Resolver",
                (),
                {
                    "resolve_request_identity": lambda self, request: RequestIdentity(
                        user_id="user-1",
                        email="bill@example.com",
                        display_name="Bill",
                        authenticated=True,
                        auth_source="supabase-hosted",
                    )
                },
            )()

            response = app.test_client().get("/hosted/private-access-check")

            self.assertEqual(response.status_code, 200)
            payload = response.get_json()
            self.assertTrue(payload["ok"])
            self.assertEqual(payload["email"], "bill@example.com")

    def test_hosted_logout_invalidates_hosted_auth_cookies(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = create_app(
                {
                    "TESTING": True,
                    "RUNTIME_TARGET": "hosted",
                    "SUPABASE_URL": "https://project.supabase.co",
                    "SUPABASE_PUBLISHABLE_KEY": "publishable-key",
                    "SUPABASE_SECRET_KEY": "secret-key",
                    "DELPHI_HOSTED_ALLOWED_EMAILS": "bill@example.com",
                    "TRADE_DATABASE": str(Path(temp_dir) / "hosted-logout.db"),
                }
            )
            app.extensions["session_invalidator"] = SupabaseSessionInvalidator(
                HostedAuthConfig.resolve(app, get_runtime_app_config(app), app.extensions["supabase_context"])
            )

            response = app.test_client().post("/hosted/logout")

            self.assertEqual(response.status_code, 200)
            set_cookie_headers = response.headers.getlist("Set-Cookie")
            self.assertTrue(any("delphi_hosted_access_token=" in header for header in set_cookie_headers))

    def test_hosted_login_uses_runtime_hosted_redirect_uri_and_supabase_token_store(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = create_app(
                {
                    "TESTING": True,
                    "RUNTIME_TARGET": "hosted",
                    "HOSTED_PUBLIC_BASE_URL": "https://delphi-hosted.example.com",
                    "SCHWAB_REDIRECT_URI": "https://127.0.0.1:5015/callback",
                    "SUPABASE_URL": "https://project.supabase.co",
                    "SUPABASE_PUBLISHABLE_KEY": "publishable-key",
                    "SUPABASE_SECRET_KEY": "secret-key",
                    "SCHWAB_CLIENT_ID": "client-id",
                    "SCHWAB_CLIENT_SECRET": "client-secret",
                    "SCHWAB_AUTH_URL": "https://api.schwabapi.com/v1/oauth/authorize",
                    "SCHWAB_TOKEN_URL": "https://api.schwabapi.com/v1/oauth/token",
                    "DELPHI_HOSTED_ALLOWED_EMAILS": "bill@example.com",
                    "TRADE_DATABASE": str(Path(temp_dir) / "hosted-login.db"),
                }
            )

            runtime_config = get_runtime_app_config(app)
            provider = app.extensions["market_data_service"].provider

            self.assertEqual(runtime_config.schwab_redirect_uri, "https://eigeltrade.com/callback")
            self.assertEqual(runtime_config.schwab_token_path, "supabase://hosted_runtime_state/schwab_oauth_token/default")
            self.assertIsInstance(provider.auth_service.token_store, SupabaseTokenRepository)
            self.assertEqual(provider.auth_service.config.schwab_redirect_uri, runtime_config.schwab_redirect_uri)

            response = app.test_client().get("/login")

            self.assertEqual(response.status_code, 302)
            location = response.headers["Location"]
            parsed = urlparse(location)
            params = parse_qs(parsed.query)
            self.assertEqual(parsed.scheme, "https")
            self.assertEqual(parsed.netloc, "api.schwabapi.com")
            self.assertEqual(params["redirect_uri"], ["https://eigeltrade.com/callback"])
            self.assertNotIn("127.0.0.1", location)
            self.assertNotIn("localhost", location)
            self.assertNotIn(":5001", location)