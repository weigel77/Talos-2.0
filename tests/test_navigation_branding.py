import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app import create_app


class DelphiNavigationBrandingTest(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database_path = Path(self.temp_dir.name) / "horme_trades.db"
        self.app = create_app(
            {
                "TESTING": True,
                "TRADE_DATABASE": str(self.database_path),
            }
        )
        self.client = self.app.test_client()
        self.service = self.app.extensions["market_data_service"]
        self._apply_header_service_stub()

    def tearDown(self):
        self.temp_dir.cleanup()

    def _apply_header_service_stub(self, spx_change=12.34, vix_change=-1.23, spx_percent=0.20, vix_percent=-6.15):
        def fake_snapshot(ticker, query_type="latest"):
            if ticker == "^GSPC":
                return {
                    "Latest Value": 6123.45,
                    "Daily Point Change": spx_change,
                    "Daily Percent Change": spx_percent,
                    "As Of": "2026-04-04 11:00:00 AM CDT",
                }
            return {
                "Latest Value": 18.76,
                "Daily Point Change": vix_change,
                "Daily Percent Change": vix_percent,
                "As Of": "2026-04-04 11:00:00 AM CDT",
            }

        self.service.get_latest_snapshot = fake_snapshot
        self.service.get_provider_metadata = lambda: {
            "requires_auth": True,
            "authenticated": True,
            "live_provider_name": "Schwab",
            "provider_name": "Schwab",
            "live_provider_key": "schwab",
            "spx_historical_provider_name": "Schwab",
            "vix_historical_provider_name": "Schwab",
        }
        pushover_service = self.app.extensions["pushover_service"]
        pushover_service.user_key = "user-key"
        pushover_service.api_token = "api-token"

    def test_startup_page_shows_delphi_branding_navigation_and_background_art(self):
        calls = []

        def fake_snapshot(ticker, query_type="latest"):
            calls.append((ticker, query_type))
            value = 6123.45 if ticker == "^GSPC" else 18.76
            point_change = 12.34 if ticker == "^GSPC" else -1.23
            percent_change = 0.20 if ticker == "^GSPC" else -6.15
            return {
                "Latest Value": value,
                "Daily Point Change": point_change,
                "Daily Percent Change": percent_change,
                "As Of": "2026-04-04 11:00:00 AM CDT",
            }

        self.service.get_latest_snapshot = fake_snapshot
        self.service.get_provider_metadata = lambda: {
            "requires_auth": True,
            "authenticated": True,
            "live_provider_name": "Schwab",
            "provider_name": "Schwab",
            "live_provider_key": "schwab",
            "spx_historical_provider_name": "Schwab",
            "vix_historical_provider_name": "Schwab",
        }

        redirect_response = self.client.get("/", follow_redirects=False)
        response = self.client.get("/", follow_redirects=True)

        self.assertEqual(redirect_response.status_code, 302)
        self.assertEqual(redirect_response.headers["Location"], "/management/open-trades")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Open Trade Management", response.data)
        self.assertIn(b">Home<", response.data)
        self.assertIn(b"Research", response.data)
        self.assertIn(b"Run Apollo", response.data)
        self.assertIn(b"Run Kairos", response.data)
        self.assertIn(b"Performance", response.data)
        self.assertIn(b"Journal", response.data)
        self.assertIn(b"Schwab connection", response.data)
        self.assertIn(b"delphi-shared-header", response.data)
        self.assertIn(b"delphi-status-card-value", response.data)
        self.assertIn(b"6,123.45", response.data)
        self.assertIn(b"18.76", response.data)
        self.assertIn(b"delphi-status-card-value-positive", response.data)
        self.assertIn(b"delphi-status-card-value-negative", response.data)
        self.assertIn(b"+12.34 pts", response.data)
        self.assertIn(b"-1.23 pts", response.data)
        self.assertNotIn(b"Simulated Trades", response.data)
        self.assertNotIn(b"Live provider", response.data)
        self.assertNotIn(b"SPX history", response.data)
        self.assertNotIn(b"VIX history", response.data)
        self.assertLess(response.data.find(b">Home<"), response.data.find(b">Research<"))
        self.assertEqual(calls, [("^GSPC", "open_trade_management_spx"), ("^VIX", "open_trade_management_vix")])

        stylesheet = (Path(self.app.root_path) / "static" / "styles.css").read_text(encoding="utf-8")
        self.assertIn("background: rgba(20, 40, 80, 0.85);", stylesheet)
        self.assertIn("color: #63d297;", stylesheet)
        self.assertIn("color: #ff8b8b;", stylesheet)
        self.assertIn("color: #9fb3c8;", stylesheet)

    def test_startup_page_refreshes_spx_and_vix_on_each_access(self):
        calls = []

        def fake_snapshot(ticker, query_type="latest"):
            calls.append((ticker, query_type))
            return {
                "Latest Value": 1.0,
                "Daily Point Change": 0.0,
                "Daily Percent Change": 0.0,
                "As Of": "Now",
            }

        self.service.get_latest_snapshot = fake_snapshot
        self.service.get_provider_metadata = lambda: {
            "requires_auth": False,
            "authenticated": True,
            "live_provider_name": "Local",
            "provider_name": "Local",
            "live_provider_key": "local",
            "spx_historical_provider_name": "Local",
            "vix_historical_provider_name": "Local",
        }

        self.client.get("/", follow_redirects=True)
        self.client.get("/", follow_redirects=True)

        self.assertEqual(
            calls,
            [
                ("^GSPC", "open_trade_management_spx"),
                ("^VIX", "open_trade_management_vix"),
                ("^GSPC", "open_trade_management_spx"),
                ("^VIX", "open_trade_management_vix"),
            ],
        )

    def test_shared_header_status_cards_render_on_all_major_pages(self):
        with patch("app.execute_apollo_precheck") as execute_apollo_precheck:
            execute_apollo_precheck.return_value = {
                "title": "Apollo Gate 1 -- SPX Structure",
                "structure_grade": "Allowed",
                "structure_grade_class": "allowed",
                "structure_base_grade": "Allowed",
                "structure_final_grade": "Allowed",
                "structure_rsi_modifier": "None",
                "structure_rsi_note": "Daily RSI unavailable; base structure kept.",
                "structure_source_used": "Schwab",
                "structure_preferred_source": "Schwab",
                "structure_session_note": "",
                "structure_trend_classification": "Balanced",
                "structure_damage_classification": "Contained",
                "structure_session_low": "5,000.00",
                "structure_session_high": "5,100.00",
                "structure_range_position": "50.00%",
                "structure_ema8": "5,048.00",
                "structure_ema21": "5,042.00",
                "structure_recent_price_action": "Stable",
                "structure_session_window": "09:30 to 16:00",
                "structure_available": False,
                "structure_chart": {"available": False, "points": []},
                "structure_message": "Test structure",
                "structure_rules": [],
                "structure_attempted_sources": [],
                "structure_fallback_reason": "",
                "macro_grade": "None",
                "macro_grade_class": "good",
                "macro_major_events": [],
                "macro_minor_events": [],
                "macro_diagnostic": {},
                "macro_source_attempts": [],
                "trade_candidates_items": [],
                "trade_candidates_diagnostics": {},
                "reasons": [],
                "local_datetime": "Fri 2026-04-03 10:00 AM CDT",
            }

            routes = [
                "/",
                "/research",
                "/apollo?autorun=1",
                "/management/open-trades",
                "/performance",
                "/trades/real",
                "/trades/simulated",
                "/kairos/live",
            ]

            for route in routes:
                with self.subTest(route=route):
                    response = self.client.get(route, follow_redirects=(route == "/"))
                    self.assertEqual(response.status_code, 200)
                    self.assertIn(b"SPX", response.data)
                    self.assertIn(b"VIX", response.data)
                    self.assertIn(b"Schwab connection", response.data)
                    self.assertIn(b"Text Status", response.data)
                    if route != "/":
                        self.assertNotIn(b"Main Menu", response.data)

    def test_manual_text_status_endpoint_sends_current_market_status(self):
        pushover_calls = []
        pushover_service = self.app.extensions["pushover_service"]

        def fake_sender(api_url, payload):
            pushover_calls.append({"api_url": api_url, "payload": payload})
            return {"status": 1, "request": "push-456"}

        pushover_service._request_sender = fake_sender

        response = self.client.post("/api/text-status")
        payload = response.get_json()

        self.assertEqual(response.status_code, 200)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["message"], "Pushover test alert sent.")
        self.assertEqual(payload["title"], "Delphi 8.0.7 Local Test Alert")
        self.assertEqual(len(pushover_calls), 1)
        self.assertEqual(pushover_calls[0]["api_url"], "https://api.pushover.net/1/messages.json")
        self.assertEqual(pushover_calls[0]["payload"]["title"], "Delphi 8.0.7 Local Test Alert")
        self.assertIn("SPX Update", pushover_calls[0]["payload"]["message"])
        self.assertIn("SPX: 6,123.45", pushover_calls[0]["payload"]["message"])
        self.assertIn("VIX: 18.76", pushover_calls[0]["payload"]["message"])
        self.assertIn("Source: Delphi 8.0.7 Local Pushover test", pushover_calls[0]["payload"]["message"])

    def test_manual_text_status_endpoint_surfaces_missing_pushover_credentials(self):
        pushover_service = self.app.extensions["pushover_service"]
        pushover_service.user_key = ""
        pushover_service.api_token = ""

        response = self.client.post("/api/text-status")
        payload = response.get_json()

        self.assertEqual(response.status_code, 503)
        self.assertFalse(payload["ok"])
        self.assertIn("Pushover is not configured", payload["error"])

    def test_shared_header_uses_neutral_style_when_daily_change_is_unavailable(self):
        self._apply_header_service_stub(spx_change=None, vix_change=0.0, spx_percent=None, vix_percent=0.0)

        response = self.client.get("/research")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Change unavailable", response.data)
        self.assertIn(b"delphi-status-card-value-neutral", response.data)

    def test_research_route_renders_lookup_workspace(self):
        response = self.client.get("/research")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"delphi-shared-header", response.data)
        self.assertIn(b"Research", response.data)
        self.assertIn(b"Research Workspace", response.data)
        self.assertIn(b"Query type", response.data)
        self.assertIn(b"Run Lookup", response.data)
        self.assertIn(b"Journal", response.data)
        self.assertIn(b"SPX", response.data)
        self.assertIn(b"VIX", response.data)
        self.assertIn(b"Schwab connection", response.data)
        self.assertIn(b'class="panel research-workspace-panel"', response.data)
        self.assertNotIn(b"delphi-header-form", response.data)
        self.assertNotIn(b"Main Menu", response.data)
        self.assertIn(b'data-delphi-nav="research"', response.data)
        self.assertIn(b'data-delphi-nav="research" aria-current="page"', response.data)
        self.assertNotIn(b"Horme", response.data)

    @patch("app.execute_apollo_precheck")
    def test_apollo_route_uses_shared_header_active_nav_and_workflow_body(self, execute_apollo_precheck):
        execute_apollo_precheck.return_value = {
            "title": "Apollo Gate 1 -- SPX Structure",
            "structure_grade": "Allowed",
            "structure_grade_class": "allowed",
            "structure_base_grade": "Allowed",
            "structure_final_grade": "Allowed",
            "structure_rsi_modifier": "None",
            "structure_rsi_note": "Daily RSI unavailable; base structure kept.",
            "structure_source_used": "Schwab",
            "structure_preferred_source": "Schwab",
            "structure_session_note": "",
            "structure_trend_classification": "Balanced",
            "structure_damage_classification": "Contained",
            "structure_session_low": "5,000.00",
            "structure_session_high": "5,100.00",
            "structure_range_position": "50.00%",
            "structure_ema8": "5,048.00",
            "structure_ema21": "5,042.00",
            "structure_recent_price_action": "Stable",
            "structure_session_window": "09:30 to 16:00",
            "structure_available": False,
            "structure_chart": {"available": False, "points": []},
            "structure_message": "Test structure",
            "structure_rules": [],
            "structure_attempted_sources": [],
            "structure_fallback_reason": "",
            "spx_value": "5,050.00",
            "spx_as_of": "Now",
            "vix_value": "20.00",
            "vix_as_of": "Now",
            "local_datetime": "Fri 2026-04-03 10:00 AM CDT",
            "macro_title": "Apollo Gate 2 -- Macro Event",
            "macro_source": "MarketWatch",
            "macro_grade": "None",
            "macro_grade_class": "good",
            "macro_target_day": "Mon Apr 06, 2026",
            "macro_checked_at": "Fri 2026-04-03 10:00 AM CDT",
            "macro_major_events": [],
            "macro_minor_events": [],
            "macro_diagnostic": {},
            "macro_source_attempts": [],
            "option_chain_rows_setting": "Adaptive",
            "option_chain_min_premium_target": "1.00",
            "option_chain_grouping": "Puts ascending → Calls ascending",
            "trade_candidates_title": "Apollo Gate 3 -- Trade Candidates",
            "trade_candidates_status": "Stand Aside",
            "trade_candidates_status_class": "not-available",
            "trade_candidates_count": 0,
            "trade_candidates_count_label": "0 Candidates",
            "trade_candidates_message": "No trade candidates were produced.",
            "trade_candidates_expected_move": "—",
            "trade_candidates_short_barrier_put": "—",
            "trade_candidates_short_barrier_call": "—",
            "trade_candidates_items": [],
            "trade_candidates_diagnostics": {},
            "apollo_trigger_source": "autorun URL",
            "apollo_trigger_note": "Apollo was triggered by autorun URL.",
            "reasons": [],
        }

        response = self.client.get("/apollo?autorun=1")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"delphi-shared-header", response.data)
        self.assertIn(b"Apollo", response.data)
        self.assertIn(b"Apollo: Greek God of Prophecy and Part-Time Options Trader", response.data)
        self.assertIn(b"apollo-identity-emblem", response.data)
        self.assertIn(b"Last Apollo run", response.data)
        self.assertIn(b"Fri 2026-04-03 10:00 AM CDT", response.data)
        self.assertIn(b"Base Structure", response.data)
        self.assertIn(b'href="/apollo?autorun=1"', response.data)
        self.assertIn(b'data-refresh-market-status="true"', response.data)
        self.assertNotIn(b"Run Apollo</button>", response.data)
        self.assertNotIn(b">Open Research<", response.data)
        self.assertNotIn(b"Default view prioritizes", response.data)
        self.assertNotIn(b"Option-chain controls", response.data)
        self.assertNotIn(b"Local time", response.data)
        self.assertNotIn(b"Target next market day", response.data)
        self.assertNotIn(b"Checked at", response.data)
        self.assertIn(b'data-delphi-nav="apollo" aria-current="page"', response.data)

    def test_kairos_route_renders_watchtower_workspace(self):
        response = self.client.get("/kairos/live")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"delphi-shared-header", response.data)
        self.assertIn(b"Kairos", response.data)
        self.assertIn(b"SPX", response.data)
        self.assertIn(b"VIX", response.data)
        self.assertIn(b"The God of the Right Moment", response.data)
        self.assertIn(b"Kairos Credit Map", response.data)
        self.assertIn(b"Kairos Candidates", response.data)
        self.assertNotIn(b"Live Workspace Control", response.data)
        self.assertNotIn(b"Live Watchtower Status", response.data)
        self.assertNotIn(b"Kairos Scan Summary", response.data)
        self.assertIn(b"Structure", response.data)
        self.assertIn(b"Countdown", response.data)
        self.assertIn(b"Total Scans Today", response.data)
        self.assertIn(b"Scan Interval", response.data)
        self.assertIn(b"Last Scan", response.data)
        self.assertNotIn(b"Current session status", response.data)
        self.assertNotIn(b"kairos-live-box-panel", response.data)
        self.assertNotIn(b"kairos-summary-text", response.data)
        self.assertNotIn(b"kairos-classification-note", response.data)
        self.assertIn(b"Intraday Scan Log", response.data)
        self.assertNotIn(b"Main Menu", response.data)
        self.assertIn(b'data-delphi-nav="kairos" aria-current="page"', response.data)

    @patch("app.execute_apollo_precheck")
    def test_shared_header_uses_png_icons_for_local_pages(self, execute_apollo_precheck):
        execute_apollo_precheck.return_value = {
            "title": "Apollo Gate 1 -- SPX Structure",
            "structure_grade": "Allowed",
            "structure_grade_class": "allowed",
            "structure_base_grade": "Allowed",
            "structure_final_grade": "Allowed",
            "structure_rsi_modifier": "None",
            "structure_rsi_note": "Daily RSI unavailable; base structure kept.",
            "structure_source_used": "Schwab",
            "structure_preferred_source": "Schwab",
            "structure_session_note": "",
            "structure_trend_classification": "Balanced",
            "structure_damage_classification": "Contained",
            "structure_session_low": "5,000.00",
            "structure_session_high": "5,100.00",
            "structure_range_position": "50.00%",
            "structure_ema8": "5,048.00",
            "structure_ema21": "5,042.00",
            "structure_recent_price_action": "Stable",
            "structure_session_window": "09:30 to 16:00",
            "structure_available": False,
            "structure_chart": {"available": False, "points": []},
            "structure_message": "Test structure",
            "structure_rules": [],
            "structure_attempted_sources": [],
            "structure_fallback_reason": "",
            "macro_grade": "None",
            "macro_grade_class": "good",
            "macro_major_events": [],
            "macro_minor_events": [],
            "macro_diagnostic": {},
            "macro_source_attempts": [],
            "trade_candidates_items": [],
            "trade_candidates_diagnostics": {},
            "reasons": [],
            "local_datetime": "Fri 2026-04-03 10:00 AM CDT",
        }

        page_responses = {
            b'/static/icons/delphi-icon.png': [
                self.client.get("/management/open-trades"),
                self.client.get("/research"),
            ],
            b'/static/icons/apollo-icon.png': [self.client.get("/apollo?autorun=1")],
            b'/static/icons/kairos-icon.png': [self.client.get("/kairos/live")],
            b'/static/icons/journal-icon.png': [self.client.get("/trades/real")],
            b'/static/icons/performance-icon.png': [self.client.get("/performance")],
        }

        for expected_icon, responses in page_responses.items():
            for response in responses:
                self.assertIn(expected_icon, response.data)
                self.assertIn(b'delphi-header-emblem-image', response.data)
                self.assertNotIn(b'.jpg', response.data)

    def test_open_trade_management_route_renders_shared_header_and_management_board(self):
        response = self.client.get("/management/open-trades")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Open Trade Management", response.data)
        self.assertIn(b"Send Real Status Update", response.data)
        self.assertIn(b"Send Simulated Status Update", response.data)
        self.assertIn(b"Notifications: ON", response.data)
        self.assertIn(b"manual status updates stay limited to real and simulated positions", response.data)
        self.assertIn(b"No open real or simulated trades are currently available for management.", response.data)
        self.assertIn(b'data-delphi-nav="management" aria-current="page"', response.data)

    def test_performance_page_uses_shared_header_and_active_nav_state(self):
        response = self.client.get("/performance")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"delphi-shared-header", response.data)
        self.assertIn(b"SPX", response.data)
        self.assertIn(b"VIX", response.data)
        self.assertIn(b"Schwab connection", response.data)
        self.assertNotIn(b"Main Menu", response.data)
        self.assertIn(b'data-delphi-nav="performance" aria-current="page"', response.data)

    def test_journal_page_still_exposes_real_and_simulated_modes(self):
        response = self.client.get("/trades/real")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"delphi-shared-header", response.data)
        self.assertIn(b"Real Trades", response.data)
        self.assertIn(b"Simulated Trades", response.data)
        self.assertIn(b"SPX", response.data)
        self.assertIn(b"VIX", response.data)
        self.assertIn(b"Schwab connection", response.data)
        self.assertIn(b"Research", response.data)
        self.assertNotIn(b"Main Menu", response.data)
        self.assertIn(b'data-delphi-nav="journal" aria-current="page"', response.data)

    def test_simulated_journal_page_keeps_shared_header_and_mode_state(self):
        response = self.client.get("/trades/simulated")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"delphi-shared-header", response.data)
        self.assertIn(b"Simulated Trades", response.data)
        self.assertIn(b"SPX", response.data)
        self.assertIn(b"VIX", response.data)
        self.assertNotIn(b"Main Menu", response.data)
        self.assertIn(b'data-delphi-nav="journal" aria-current="page"', response.data)
        self.assertIn(b'aria-current="page">Simulated Trades<', response.data)


if __name__ == "__main__":
    unittest.main()

