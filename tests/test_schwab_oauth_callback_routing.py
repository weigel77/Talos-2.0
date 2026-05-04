import unittest

from app import create_app, get_runtime_app_config
from services.schwab_auth_service import SchwabAuthService


class SchwabOauthCallbackRoutingTest(unittest.TestCase):
    def _build_authorize_url(self, **overrides: object) -> tuple[object, str]:
        config = {
            "TESTING": True,
            "MARKET_DATA_PROVIDER": "schwab",
            "SCHWAB_CLIENT_ID": "client-id",
            "SCHWAB_CLIENT_SECRET": "client-secret",
            "SCHWAB_AUTH_URL": "https://auth.example.test/oauth/authorize",
            "SUPABASE_URL": "https://project.supabase.co",
            "SUPABASE_PUBLISHABLE_KEY": "publishable-key",
            "SUPABASE_SECRET_KEY": "secret-key",
        }
        config.update(overrides)
        app = create_app(config)
        runtime_config = get_runtime_app_config(app)
        authorize_url = SchwabAuthService(runtime_config).build_authorization_url(state="state-token")
        return runtime_config, authorize_url

    def test_local_runtime_uses_loopback_callback_by_default(self):
        runtime_config, authorize_url = self._build_authorize_url(
            HOSTED_PUBLIC_BASE_URL="",
            SCHWAB_REDIRECT_URI="",
        )

        self.assertEqual(runtime_config.runtime_target, "local")
        self.assertEqual(runtime_config.schwab_redirect_uri, "https://127.0.0.1:5015/callback")
        self.assertIn("redirect_uri=https%3A%2F%2F127.0.0.1%3A5015%2Fcallback", authorize_url)

    def test_hosted_runtime_prefers_public_base_callback_in_non_production(self):
        runtime_config, authorize_url = self._build_authorize_url(
            RUNTIME_TARGET="hosted",
            HOSTED_PUBLIC_BASE_URL="https://wrong.example.test",
            SCHWAB_REDIRECT_URI="https://127.0.0.1:5001/callback",
        )

        self.assertEqual(runtime_config.runtime_target, "hosted")
        self.assertEqual(runtime_config.schwab_redirect_uri, "https://wrong.example.test/callback")
        self.assertIn("redirect_uri=https%3A%2F%2Fwrong.example.test%2Fcallback", authorize_url)

    def test_hosted_runtime_derives_callback_from_public_base_url_when_redirect_missing(self):
        runtime_config, authorize_url = self._build_authorize_url(
            RUNTIME_TARGET="hosted",
            HOSTED_PUBLIC_BASE_URL="https://eigeltrade.com",
            SCHWAB_REDIRECT_URI="",
        )

        self.assertEqual(runtime_config.schwab_redirect_uri, "https://eigeltrade.com/callback")
        self.assertIn("redirect_uri=https%3A%2F%2Feigeltrade.com%2Fcallback", authorize_url)

    def test_hosted_runtime_authorize_url_never_falls_back_to_loopback_when_public_base_url_exists(self):
        runtime_config, authorize_url = self._build_authorize_url(
            RUNTIME_TARGET="hosted",
            HOSTED_PUBLIC_BASE_URL="https://eigeltrade.com",
            SCHWAB_REDIRECT_URI="",
        )

        self.assertEqual(runtime_config.schwab_redirect_uri, "https://eigeltrade.com/callback")
        self.assertIn("https%3A%2F%2Feigeltrade.com%2Fcallback", authorize_url)
        self.assertNotIn("127.0.0.1", authorize_url)
        self.assertNotIn("localhost", authorize_url)
        self.assertNotIn(":5001", authorize_url)


if __name__ == "__main__":
    unittest.main()