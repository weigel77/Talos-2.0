import tempfile
import unittest
from pathlib import Path

from app import create_app


class RuntimeInfrastructureTest(unittest.TestCase):
    def test_local_host_infrastructure_preserves_local_filesystem_defaults(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "local-infra.db"
            replay_path = Path(temp_dir) / "kairos-replays"
            app = create_app(
                {
                    "TESTING": True,
                    "RUNTIME_TARGET": "local",
                    "TRADE_DATABASE": str(database_path),
                    "KAIROS_REPLAY_STORAGE_DIR": str(replay_path),
                }
            )

            infrastructure = app.extensions["host_infrastructure"]

            self.assertEqual(infrastructure.host_kind, "local")
            self.assertEqual(infrastructure.settings.runtime_target, "local")
            self.assertEqual(infrastructure.storage.trade_database_path, database_path)
            self.assertEqual(infrastructure.storage.kairos_replay_storage_dir, replay_path)
            self.assertEqual(infrastructure.storage.import_preview_root, Path(app.instance_path))
            self.assertEqual(app.config["TRADE_DATABASE"], str(database_path))
            self.assertEqual(app.config["KAIROS_REPLAY_STORAGE_DIR"], str(replay_path))
            self.assertEqual(app.config["APP_DISPLAY_NAME"], "Delphi 8.0.7 Local")
            self.assertEqual(app.config["APP_VERSION_LABEL"], "Version 8.0.7")
            self.assertIn("strategy_snapshots", app.extensions)
            self.assertIn("apollo", app.extensions["strategy_snapshots"])
            self.assertIn("kairos", app.extensions["strategy_snapshots"])

    def test_hosted_runtime_skeleton_reuses_local_storage_boundary_without_changing_routes(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "hosted-infra.db"
            app = create_app(
                {
                    "TESTING": True,
                    "RUNTIME_TARGET": "hosted",
                    "HOSTED_PUBLIC_BASE_URL": "https://hosted.example.test",
                    "SUPABASE_URL": "https://project.supabase.co",
                    "SUPABASE_PUBLISHABLE_KEY": "publishable-key",
                    "SUPABASE_SECRET_KEY": "secret-key",
                    "TRADE_DATABASE": str(database_path),
                }
            )

            infrastructure = app.extensions["host_infrastructure"]
            profile = app.extensions["runtime_profile"]

            self.assertEqual(infrastructure.host_kind, "hosted")
            self.assertEqual(infrastructure.settings.runtime_target, "hosted")
            self.assertEqual(app.config["APP_DISPLAY_NAME"], "Delphi 8.0.7")
            self.assertEqual(app.config["APP_VERSION_LABEL"], "Version 8.0.7")
            self.assertEqual(app.config["SESSION_COOKIE_NAME"], "delphi5_hosted_session")
            self.assertEqual(app.config["OAUTH_SESSION_NAMESPACE"], "delphi5hosted")
            self.assertIsNotNone(infrastructure.supabase_context)
            self.assertEqual(
                infrastructure.supabase_context.configured,
                bool(app.extensions["supabase_integration"] and app.extensions["supabase_integration"].config.is_configured),
            )
            self.assertEqual(profile.profile_name, "hosted-testing")
            self.assertEqual(profile.launch_url, "https://hosted.example.test")
            self.assertFalse(profile.auto_open_browser)
            self.assertEqual(app.extensions["service_bundle"].host_infrastructure, infrastructure)
            self.assertIs(app.extensions["supabase_context"], infrastructure.supabase_context)

            client = app.test_client()
            response = client.get("/")

            self.assertEqual(response.status_code, 302)
            self.assertEqual(response.headers["Location"], "/hosted/launch")

    def test_hosted_runtime_profile_can_target_local_http_validation_url(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "hosted-http-validation.db"
            app = create_app(
                {
                    "TESTING": True,
                    "RUNTIME_TARGET": "hosted",
                    "APP_HOST": "127.0.0.1",
                    "APP_PORT": 5015,
                    "HOSTED_PUBLIC_BASE_URL": "http://127.0.0.1:5015",
                    "SUPABASE_URL": "https://project.supabase.co",
                    "SUPABASE_PUBLISHABLE_KEY": "publishable-key",
                    "SUPABASE_SECRET_KEY": "secret-key",
                    "TRADE_DATABASE": str(database_path),
                }
            )

            profile = app.extensions["runtime_profile"]

            self.assertEqual(profile.launch_url, "http://127.0.0.1:5015")
            self.assertEqual(profile.host, "127.0.0.1")
            self.assertEqual(profile.port, 5015)
            self.assertFalse(profile.use_https)

    def test_hosted_runtime_profile_can_target_local_https_validation_url(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "hosted-https-validation.db"
            app = create_app(
                {
                    "TESTING": True,
                    "RUNTIME_TARGET": "hosted",
                    "APP_HOST": "127.0.0.1",
                    "APP_PORT": 5015,
                    "HOSTED_PUBLIC_BASE_URL": "https://127.0.0.1:5015",
                    "SUPABASE_URL": "https://project.supabase.co",
                    "SUPABASE_PUBLISHABLE_KEY": "publishable-key",
                    "SUPABASE_SECRET_KEY": "secret-key",
                    "TRADE_DATABASE": str(database_path),
                }
            )

            profile = app.extensions["runtime_profile"]

            self.assertEqual(profile.launch_url, "https://127.0.0.1:5015")
            self.assertEqual(profile.host, "127.0.0.1")
            self.assertEqual(profile.port, 5015)
            self.assertTrue(profile.use_https)
            self.assertEqual(profile.ssl_context, ("localhost+2.pem", "localhost+2-key.pem"))

    def test_hosted_runtime_overrides_stale_branding_overrides(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "hosted-branding.db"
            app = create_app(
                {
                    "TESTING": True,
                    "RUNTIME_TARGET": "hosted",
                    "APP_DISPLAY_NAME": "Delphi 5.1",
                    "APP_PAGE_KICKER": "Delphi 5.1",
                    "APP_VERSION_LABEL": "Version 5.1",
                    "SUPABASE_URL": "https://project.supabase.co",
                    "SUPABASE_PUBLISHABLE_KEY": "publishable-key",
                    "SUPABASE_SECRET_KEY": "secret-key",
                    "HOSTED_PUBLIC_BASE_URL": "https://hosted.example.test",
                    "TRADE_DATABASE": str(database_path),
                }
            )

            self.assertEqual(app.config["APP_DISPLAY_NAME"], "Delphi 8.0.7")
            self.assertEqual(app.config["APP_PAGE_KICKER"], "Delphi 8.0.7")
            self.assertEqual(app.config["APP_VERSION_LABEL"], "Version 8.0.7")
