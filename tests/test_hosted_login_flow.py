import json
import tempfile
import unittest
from pathlib import Path

from app import create_app
from services.runtime.hosted_auth import HostedBrowserSession, SupabaseEmailPasswordAuthenticator
from services.runtime.private_access import AuthenticationRequiredError, RequestIdentity


class _FakeHostedSessionAuthenticator:
    def __init__(self, sessions_by_credentials):
        self.sessions_by_credentials = sessions_by_credentials
        self.calls = []

    def sign_in(self, *, email: str, password: str) -> HostedBrowserSession:
        self.calls.append((email, password))
        session = self.sessions_by_credentials.get((email, password))
        if session is None:
            raise AuthenticationRequiredError("Invalid hosted email/password credentials.")
        return session

    def establish_response_session(self, response, session: HostedBrowserSession) -> None:
        response.set_cookie("delphi_hosted_access_token", session.access_token, path="/")
        response.set_cookie("delphi_hosted_refresh_token", session.refresh_token, path="/")


class _CookieIdentityResolver:
    def __init__(self, identities_by_token):
        self.identities_by_token = identities_by_token

    def resolve_request_identity(self, request):
        token = str(request.cookies.get("delphi_hosted_access_token") or "").strip()
        return self.identities_by_token.get(token, RequestIdentity(authenticated=False, auth_source="supabase-hosted"))


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


class HostedLoginFlowTest(unittest.TestCase):
    RAW_JS_FRAGMENTS = (
        b"const toggle",
        b"addEventListener(",
        b"HTMLFormElement.prototype.submit",
        b"querySelector(",
        b"window.matchMedia(",
    )

    def _create_hosted_app(self, temp_dir: str):
        return create_app(
            {
                "TESTING": True,
                "RUNTIME_TARGET": "hosted",
                "SUPABASE_URL": "https://project.supabase.co",
                "SUPABASE_PUBLISHABLE_KEY": "publishable-key",
                "SUPABASE_SECRET_KEY": "secret-key",
                "DELPHI_HOSTED_ALLOWED_EMAILS": "bill@example.com",
                "TRADE_DATABASE": str(Path(temp_dir) / "hosted-login.db"),
            }
        )

    def _create_hosted_debug_app(self, temp_dir: str):
        return create_app(
            {
                "TESTING": True,
                "RUNTIME_TARGET": "hosted",
                "SUPABASE_URL": "https://project.supabase.co",
                "SUPABASE_PUBLISHABLE_KEY": "publishable-key",
                "SUPABASE_SECRET_KEY": "secret-key",
                "DELPHI_HOSTED_ALLOWED_EMAILS": "bill@example.com,copilot-hosted-debug@example.com",
                "TRADE_DATABASE": str(Path(temp_dir) / "hosted-login-debug.db"),
            }
        )

    def test_supabase_email_password_authenticator_uses_token_endpoint_and_returns_session(self):
        app = self._create_hosted_app(tempfile.gettempdir())
        context = app.extensions["supabase_context"]
        auth_config = app.extensions["hosted_auth_config"]

        def requester(request, timeout=0):
            self.assertIn("/auth/v1/token?grant_type=password", request.full_url)
            header_lookup = {key.lower(): value for key, value in request.header_items()}
            self.assertEqual(header_lookup["apikey"], "publishable-key")
            payload = json.loads(request.data.decode("utf-8"))
            self.assertEqual(payload["email"], "bill@example.com")
            self.assertEqual(payload["password"], "secret123")
            return _AuthResponse(
                {
                    "access_token": "token-123",
                    "refresh_token": "refresh-123",
                    "expires_in": 3600,
                    "token_type": "bearer",
                    "user": {
                        "id": "user-1",
                        "email": "bill@example.com",
                        "user_metadata": {"full_name": "Bill"},
                    },
                }
            )

        authenticator = SupabaseEmailPasswordAuthenticator(context=context, auth_config=auth_config, requester=requester)
        session = authenticator.sign_in(email="bill@example.com", password="secret123")

        self.assertEqual(session.user_id, "user-1")
        self.assertEqual(session.email, "bill@example.com")
        self.assertEqual(session.display_name, "Bill")
        self.assertEqual(session.access_token, "token-123")

    def test_hosted_login_route_establishes_session_for_allowed_bill_identity(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = self._create_hosted_app(temp_dir)
            app.extensions["hosted_session_authenticator"] = _FakeHostedSessionAuthenticator(
                {
                    ("bill@example.com", "secret123"): HostedBrowserSession(
                        user_id="user-1",
                        email="bill@example.com",
                        display_name="Bill",
                        access_token="bill-token",
                        refresh_token="bill-refresh",
                    )
                }
            )
            app.extensions["request_identity_resolver"] = _CookieIdentityResolver(
                {
                    "bill-token": RequestIdentity(
                        user_id="user-1",
                        email="bill@example.com",
                        display_name="Bill",
                        authenticated=True,
                        auth_source="supabase-hosted",
                    )
                }
            )

            client = app.test_client()
            response = client.post("/hosted/login/desktop", data={"email": "bill@example.com", "password": "secret123", "next": "/hosted/apollo"}, follow_redirects=False)

            self.assertEqual(response.status_code, 302)
            self.assertEqual(response.headers["Location"], "/hosted/apollo")
            self.assertTrue(any("delphi_hosted_access_token=bill-token" in header for header in response.headers.getlist("Set-Cookie")))

            shell_response = client.get("/hosted/apollo")
            self.assertEqual(shell_response.status_code, 200)
            self.assertIn(b"Apollo Engine", shell_response.data)

    def test_hosted_mobile_login_route_preserves_mobile_branch_after_authentication(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = self._create_hosted_app(temp_dir)
            app.extensions["hosted_session_authenticator"] = _FakeHostedSessionAuthenticator(
                {
                    ("bill@example.com", "secret123"): HostedBrowserSession(
                        user_id="user-1",
                        email="bill@example.com",
                        display_name="Bill",
                        access_token="bill-token",
                        refresh_token="bill-refresh",
                    )
                }
            )
            app.extensions["request_identity_resolver"] = _CookieIdentityResolver(
                {
                    "bill-token": RequestIdentity(
                        user_id="user-1",
                        email="bill@example.com",
                        display_name="Bill",
                        authenticated=True,
                        auth_source="supabase-hosted",
                    )
                }
            )

            client = app.test_client()
            response = client.post("/hosted/login/mobile", data={"email": "bill@example.com", "password": "secret123", "next": "/hosted/apollo"}, follow_redirects=False)

            self.assertEqual(response.status_code, 302)
            self.assertEqual(response.headers["Location"], "/hosted/mobile/runs")
            self.assertTrue(any("delphi_hosted_access_token=bill-token" in header for header in response.headers.getlist("Set-Cookie")))

    def test_hosted_launch_page_exposes_desktop_and_mobile_login_targets_for_unauthenticated_users(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = self._create_hosted_app(temp_dir)

            response = app.test_client().get("/hosted/launch?next=/hosted/apollo&view=mobile")

            self.assertEqual(response.status_code, 200)
            self.assertIn(b"Selecting Your Command Portal", response.data)
            self.assertIn(b"Delphi 8.0.7 is detecting the right login portal for this device before sign-in.", response.data)
            self.assertIn(b'data-desktop-target="/hosted/login/desktop?next=/hosted/apollo"', response.data)
            self.assertIn(b'data-mobile-target="/hosted/login/mobile?next=/hosted/apollo"', response.data)
            self.assertIn(b'data-explicit-view="mobile"', response.data)

    def test_hosted_launch_page_uses_current_hosted_identity_for_authenticated_users(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = self._create_hosted_app(temp_dir)
            app.extensions["request_identity_resolver"] = _CookieIdentityResolver(
                {
                    "bill-token": RequestIdentity(
                        user_id="user-1",
                        email="bill@example.com",
                        display_name="Bill",
                        authenticated=True,
                        auth_source="supabase-hosted",
                    )
                }
            )

            client = app.test_client()
            client.set_cookie("delphi_hosted_access_token", "bill-token")
            response = client.get("/hosted/launch")

            self.assertEqual(response.status_code, 200)
            self.assertIn(b"Delphi 8.0.7 is preserving your active branch and routing you into the right command surface.", response.data)

    def test_hosted_desktop_login_page_renders_delphi_6_3_6_portal(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = self._create_hosted_app(temp_dir)

            response = app.test_client().get("/hosted/login/desktop")

            self.assertEqual(response.status_code, 200)
            self.assertIn(b"DELPHI", response.data)
            self.assertIn(b"Desktop hosted login", response.data)
            self.assertIn(b"type=\"email\"", response.data)
            self.assertIn(b"type=\"password\"", response.data)
            self.assertIn(b"Login", response.data)
            self.assertIn(b"Delphi 8.0.7", response.data)
            self.assertIn(b'action="/hosted/login/desktop"', response.data)
            self.assertNotIn(b"delphi-pyramid.png", response.data)
            for fragment in self.RAW_JS_FRAGMENTS:
                self.assertNotIn(fragment, response.data)

    def test_hosted_mobile_login_page_renders_delphi_6_3_6_portal(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = self._create_hosted_app(temp_dir)

            response = app.test_client().get("/hosted/login/mobile")

            self.assertEqual(response.status_code, 200)
            self.assertIn(b"DELPHI", response.data)
            self.assertIn(b"Mobile hosted login", response.data)
            self.assertIn(b"type=\"email\"", response.data)
            self.assertIn(b"type=\"password\"", response.data)
            self.assertIn(b"Login", response.data)
            self.assertIn(b"Delphi 8.0.7", response.data)
            self.assertIn(b'action="/hosted/login/mobile"', response.data)
            self.assertNotIn(b"delphi-pyramid.png", response.data)
            for fragment in self.RAW_JS_FRAGMENTS:
                self.assertNotIn(fragment, response.data)

    def test_hosted_login_pages_do_not_render_raw_javascript_fragments(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = self._create_hosted_app(temp_dir)
            client = app.test_client()

            for route in ("/hosted/login/desktop", "/hosted/login/mobile"):
                response = client.get(route)

                self.assertEqual(response.status_code, 200)
                for fragment in self.RAW_JS_FRAGMENTS:
                    self.assertNotIn(fragment, response.data)

    def test_hosted_login_route_denies_non_allowlisted_identity(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = self._create_hosted_app(temp_dir)
            app.extensions["hosted_session_authenticator"] = _FakeHostedSessionAuthenticator(
                {
                    ("other@example.com", "secret123"): HostedBrowserSession(
                        user_id="user-2",
                        email="other@example.com",
                        display_name="Other",
                        access_token="other-token",
                        refresh_token="other-refresh",
                    )
                }
            )

            response = app.test_client().post("/hosted/login/desktop", data={"email": "other@example.com", "password": "secret123"}, follow_redirects=False)

            self.assertEqual(response.status_code, 403)
            self.assertIn(b"private_access_denied", response.data)
            self.assertFalse(any("other-token" in header for header in response.headers.getlist("Set-Cookie")))

    def test_unauthenticated_hosted_shell_redirects_to_login(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = self._create_hosted_app(temp_dir)
            app.extensions["request_identity_resolver"] = _CookieIdentityResolver({})

            response = app.test_client().get("/hosted/manage-trades?trade_mode=all", follow_redirects=False)

            self.assertEqual(response.status_code, 302)
            self.assertIn("/hosted/launch?next=/hosted/manage-trades?trade_mode%3Dall", response.headers["Location"])

    def test_hosted_browser_sign_out_redirects_to_login_and_clears_cookies(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = self._create_hosted_app(temp_dir)

            response = app.test_client().post("/hosted/sign-out", follow_redirects=False)

            self.assertEqual(response.status_code, 302)
            self.assertEqual(response.headers["Location"], "/hosted/launch")
            self.assertTrue(any("delphi_hosted_access_token=" in header for header in response.headers.getlist("Set-Cookie")))

    def test_hosted_browser_sign_out_disables_local_debug_bypass_until_login(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = self._create_hosted_debug_app(temp_dir)

            client = app.test_client()

            allowed_response = client.get("/hosted/apollo", headers={"Host": "127.0.0.1:5015"}, follow_redirects=False)
            self.assertEqual(allowed_response.status_code, 200)

            sign_out_response = client.post("/hosted/sign-out", headers={"Host": "127.0.0.1:5015"}, follow_redirects=False)
            self.assertEqual(sign_out_response.status_code, 302)
            self.assertEqual(sign_out_response.headers["Location"], "/hosted/launch")

            redirected_response = client.get("/hosted/apollo", headers={"Host": "127.0.0.1:5015"}, follow_redirects=False)
            self.assertEqual(redirected_response.status_code, 302)
            self.assertIn("/hosted/launch?next=/hosted/apollo", redirected_response.headers["Location"])

    def test_hosted_login_clears_forced_reauth_and_restores_hosted_access(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = self._create_hosted_debug_app(temp_dir)
            app.extensions["hosted_session_authenticator"] = _FakeHostedSessionAuthenticator(
                {
                    ("bill@example.com", "secret123"): HostedBrowserSession(
                        user_id="user-1",
                        email="bill@example.com",
                        display_name="Bill",
                        access_token="bill-token",
                        refresh_token="bill-refresh",
                    )
                }
            )
            app.extensions["request_identity_resolver"] = _CookieIdentityResolver(
                {
                    "bill-token": RequestIdentity(
                        user_id="user-1",
                        email="bill@example.com",
                        display_name="Bill",
                        authenticated=True,
                        auth_source="supabase-hosted",
                    )
                }
            )

            client = app.test_client()
            client.post("/hosted/sign-out", headers={"Host": "127.0.0.1:5015"}, follow_redirects=False)

            login_response = client.post(
                "/hosted/login/desktop",
                data={"email": "bill@example.com", "password": "secret123", "next": "/hosted/apollo"},
                headers={"Host": "127.0.0.1:5015"},
                follow_redirects=False,
            )

            self.assertEqual(login_response.status_code, 302)
            self.assertEqual(login_response.headers["Location"], "/hosted/apollo")

            shell_response = client.get("/hosted/apollo", headers={"Host": "127.0.0.1:5015"}, follow_redirects=False)
            self.assertEqual(shell_response.status_code, 200)
            self.assertIn(b"Apollo Engine", shell_response.data)
