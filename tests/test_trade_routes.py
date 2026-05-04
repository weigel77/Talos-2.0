import io
import re
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from app import coerce_apollo_trade_input, create_app


class TradeRoutesTest(unittest.TestCase):
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
        service = self.app.extensions["market_data_service"]
        service.get_latest_snapshot = lambda ticker, query_type="latest": {
            "Latest Value": 6123.45 if ticker == "^GSPC" else 18.76,
            "Daily Point Change": 10.0 if ticker == "^GSPC" else -1.0,
            "Daily Percent Change": 0.16 if ticker == "^GSPC" else -5.06,
            "As Of": "2026-04-04 11:00:00 AM CDT",
        }
        service.get_provider_metadata = lambda: {
            "requires_auth": True,
            "authenticated": True,
            "live_provider_name": "Schwab",
            "provider_name": "Schwab",
            "live_provider_key": "schwab",
            "spx_historical_provider_name": "Schwab",
            "vix_historical_provider_name": "Schwab",
        }

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_database_auto_creates_and_header_buttons_render(self):
        redirect_response = self.client.get("/", follow_redirects=False)
        response = self.client.get("/", follow_redirects=True)

        self.assertEqual(redirect_response.status_code, 302)
        self.assertEqual(redirect_response.headers["Location"], "/management/open-trades")
        self.assertEqual(response.status_code, 200)
        self.assertTrue(self.database_path.exists())
        self.assertIn(b"Open Trade Management", response.data)
        self.assertIn(b"delphi-shared-header", response.data)
        self.assertIn(b"Research", response.data)
        self.assertIn(b"Run Apollo", response.data)
        self.assertIn(b"Run Kairos", response.data)
        self.assertIn(b"Performance", response.data)
        self.assertIn(b"Journal", response.data)
        self.assertNotIn(b"Simulated Trades", response.data)
        self.assertNotIn(b"Live provider", response.data)
        self.assertNotIn(b"SPX history", response.data)
        self.assertNotIn(b"VIX history", response.data)

    def test_existing_rows_are_backfilled_with_trade_numbers_and_legacy_profile(self):
        with closing(sqlite3.connect(self.database_path)) as connection:
            connection.execute("DROP TABLE IF EXISTS trades")
            connection.execute(
                """
                CREATE TABLE trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    trade_mode TEXT NOT NULL,
                    system_name TEXT NOT NULL,
                    journal_name TEXT,
                    system_version TEXT,
                    candidate_profile TEXT,
                    status TEXT NOT NULL,
                    trade_date TEXT,
                    entry_datetime TEXT,
                    expiration_date TEXT,
                    underlying_symbol TEXT,
                    spx_at_entry REAL,
                    vix_at_entry REAL,
                    structure_grade TEXT,
                    macro_grade TEXT,
                    expected_move REAL,
                    option_type TEXT,
                    short_strike REAL,
                    long_strike REAL,
                    spread_width REAL,
                    contracts INTEGER,
                    candidate_credit_estimate REAL,
                    actual_entry_credit REAL,
                    distance_to_short REAL,
                    short_delta REAL,
                    notes_entry TEXT,
                    exit_datetime TEXT,
                    spx_at_exit REAL,
                    actual_exit_value REAL,
                    close_method TEXT,
                    close_reason TEXT,
                    notes_exit TEXT,
                    gross_pnl REAL,
                    max_risk REAL,
                    roi_on_risk REAL,
                    hours_held REAL,
                    win_loss_result TEXT
                )
                """
            )
            connection.execute(
                """
                INSERT INTO trades (
                    created_at, updated_at, trade_mode, system_name, journal_name, system_version,
                    candidate_profile, status, trade_date, expiration_date, underlying_symbol,
                    short_strike, long_strike, spread_width, contracts, actual_entry_credit, close_reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "2026-04-01T09:30:00",
                    "2026-04-01T09:30:00",
                    "real",
                    "apollo 2.0",
                    "Apollo Main",
                    "2.0",
                    None,
                    "open",
                    "2026-04-01",
                    "2026-04-04",
                    "SPX",
                    6400,
                    6395,
                    5,
                    1,
                    1.05,
                    "",
                ),
            )
            connection.commit()

        migrated_app = create_app({"TESTING": True, "TRADE_DATABASE": str(self.database_path)})
        migrated_client = migrated_app.test_client()
        response = migrated_client.get("/trades/real")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Legacy", response.data)
        self.assertIn(b">1<", response.data)

        with closing(sqlite3.connect(self.database_path)) as connection:
            connection.row_factory = sqlite3.Row
            row = connection.execute("SELECT * FROM trades").fetchone()

        self.assertEqual(row["trade_number"], 1)
        self.assertEqual(row["candidate_profile"], "Legacy")
        self.assertEqual(row["system_name"], "Apollo")
        self.assertAlmostEqual(row["total_max_loss"], 395.0)

    def test_trade_new_get_redirects_to_embedded_form_anchor(self):
        response = self.client.get("/trades/real/new", follow_redirects=False)

        self.assertEqual(response.status_code, 302)
        self.assertTrue(response.headers["Location"].endswith("/trades/real#trade-entry-form"))

    def test_trade_create_edit_delete_and_filtering(self):
        create_response = self.client.post(
            "/trades/real/new",
            data={
                "trade_mode": "real",
                "system_name": "Apollo",
                "journal_name": "Apollo Main",
                "system_version": "2.0",
                "candidate_profile": "Standard",
                "status": "closed",
                "trade_date": "2026-04-03",
                "expiration_date": "2026-04-06",
                "underlying_symbol": "SPX",
                "spx_at_entry": "6500",
                "vix_at_entry": "20",
                "structure_grade": "Good",
                "macro_grade": "None",
                "short_strike": "6450",
                "long_strike": "6445",
                "spread_width": "5",
                "contracts": "2",
                "actual_entry_credit": "1.2",
                "actual_exit_value": "0.4",
                "close_method": "Buy to Close",
                "notes_entry": "Initial entry",
                "notes_exit": "Closed for a gain",
            },
            follow_redirects=True,
        )

        self.assertEqual(create_response.status_code, 200)
        self.assertIn(b"Trade saved successfully.", create_response.data)
        self.assertIn(b"SPX", create_response.data)
        self.assertIn(b"Total max loss", create_response.data)

        with closing(sqlite3.connect(self.database_path)) as connection:
            connection.row_factory = sqlite3.Row
            row = connection.execute("SELECT * FROM trades WHERE trade_mode = 'real'").fetchone()

        self.assertIsNotNone(row)
        trade_id = row["id"]
        self.assertEqual(row["trade_number"], 1)
        self.assertEqual(row["journal_name"], "Apollo Main")
        self.assertAlmostEqual(row["gross_pnl"], 160.0)
        self.assertAlmostEqual(row["max_risk"], 760.0)
        self.assertAlmostEqual(row["total_max_loss"], 760.0)
        self.assertEqual(row["status"], "closed")
        self.assertEqual(row["win_loss_result"], "Win")

        edit_response = self.client.post(
            f"/trades/real/{trade_id}/edit",
            data={
                "trade_mode": "real",
                "system_name": "Apollo",
                "journal_name": "Apollo Main",
                "system_version": "2.0",
                "candidate_profile": "Aggressive",
                "status": "closed",
                "trade_date": "2026-04-03",
                "expiration_date": "2026-04-06",
                "underlying_symbol": "SPX",
                "spx_at_entry": "6500",
                "vix_at_entry": "20",
                "structure_grade": "Good",
                "macro_grade": "None",
                "short_strike": "6455",
                "long_strike": "6450",
                "spread_width": "5",
                "contracts": "2",
                "actual_entry_credit": "1.2",
                "actual_exit_value": "1.4",
                "close_method": "Buy to Close",
                "notes_entry": "Adjusted strikes",
                "notes_exit": "Closed for a loss",
            },
            follow_redirects=True,
        )

        self.assertEqual(edit_response.status_code, 200)
        self.assertIn(b"Trade updated successfully.", edit_response.data)

        with closing(sqlite3.connect(self.database_path)) as connection:
            connection.row_factory = sqlite3.Row
            updated_row = connection.execute("SELECT * FROM trades WHERE id = ?", (trade_id,)).fetchone()

        self.assertAlmostEqual(updated_row["gross_pnl"], -40.0)
        self.assertEqual(updated_row["status"], "closed")
        self.assertEqual(updated_row["win_loss_result"], "Loss")
        self.assertEqual(updated_row["candidate_profile"], "Aggressive")
        self.assertAlmostEqual(updated_row["total_max_loss"], 760.0)

    def test_trade_edit_derives_closed_status_from_full_close_and_hides_status_input(self):
        create_payload = {
            "trade_mode": "real",
            "system_name": "Apollo",
            "journal_name": "Apollo Main",
            "candidate_profile": "Standard",
            "status": "open",
            "trade_date": "2026-04-03",
            "entry_datetime": "2026-04-03T09:35",
            "expiration_date": "2026-04-06",
            "underlying_symbol": "SPX",
            "short_strike": "6450",
            "long_strike": "6445",
            "spread_width": "5",
            "contracts": "2",
            "actual_entry_credit": "1.20",
            "distance_to_short": "50",
        }
        self.client.post("/trades/real/new", data=create_payload, follow_redirects=True)

        with closing(sqlite3.connect(self.database_path)) as connection:
            connection.row_factory = sqlite3.Row
            trade_id = connection.execute("SELECT id FROM trades WHERE trade_number = 1").fetchone()["id"]

        response = self.client.post(
            f"/trades/real/{trade_id}/edit",
            data={
                **create_payload,
                "status": "open",
                "close_events_present": "1",
                "prefill_source": "",
                "system_version": "",
                "close_reason": "Target",
                "notes_entry": "",
                "notes_exit": "",
                "close_event_id": [""],
                "close_event_contracts_closed": ["2"],
                "close_event_actual_exit_value": ["0.40"],
                "close_event_method": ["Close"],
            },
            follow_redirects=True,
        )

        self.assertEqual(response.status_code, 200)

        with closing(sqlite3.connect(self.database_path)) as connection:
            connection.row_factory = sqlite3.Row
            row = connection.execute("SELECT * FROM trades WHERE id = ?", (trade_id,)).fetchone()

        self.assertEqual(row["status"], "closed")
        self.assertEqual(row["win_loss_result"], "Win")

        detail_response = self.client.get(f"/trades/real/{trade_id}/edit")

        self.assertEqual(detail_response.status_code, 200)
        self.assertIn(b'id="status_display"', detail_response.data)
        self.assertIn(b'value="Closed"', detail_response.data)
        self.assertNotIn(b'name="status"', detail_response.data)

        simulated_response = self.client.post(
            "/trades/simulated/new",
            data={
                "trade_mode": "simulated",
                "system_name": "Apollo",
                "journal_name": "Apollo Main",
                "status": "open",
                "trade_date": "2026-04-03",
                "expiration_date": "2026-04-07",
                "underlying_symbol": "SIMX",
                "short_strike": "6400",
                "long_strike": "6395",
                "spread_width": "5",
                "contracts": "1",
                "actual_entry_credit": "1.00",
                "actual_exit_value": "",
                "notes_entry": "Paper trade",
            },
            follow_redirects=True,
        )

        self.assertEqual(simulated_response.status_code, 200)
        self.assertIn(b"SIMX", simulated_response.data)
        self.assertIn(b"Trade #", simulated_response.data)
        self.assertIn(b"Kairos", simulated_response.data)
        self.assertIn(b"Legacy", simulated_response.data)

        real_view = self.client.get("/trades/real")
        simulated_view = self.client.get("/trades/simulated")

        self.assertIn(b"SPX", real_view.data)
        self.assertNotIn(b"SIMX", real_view.data)
        self.assertIn(b"SIMX", simulated_view.data)
        self.assertNotIn(b"<th>ID</th>", real_view.data)

        delete_response = self.client.post(f"/trades/real/{trade_id}/delete", follow_redirects=True)

        self.assertEqual(delete_response.status_code, 200)
        self.assertIn(b"Deleted trade", delete_response.data)

        with closing(sqlite3.connect(self.database_path)) as connection:
            remaining_real = connection.execute("SELECT COUNT(*) FROM trades WHERE trade_mode = 'real'").fetchone()[0]

        self.assertEqual(remaining_real, 0)

    def test_trade_create_warns_when_expected_move_remains_unresolved(self):
        response = self.client.post(
            "/trades/real/new",
            data={
                "trade_mode": "real",
                "system_name": "Apollo",
                "journal_name": "Apollo Main",
                "candidate_profile": "Standard",
                "status": "closed",
                "trade_date": "2026-04-03",
                "expiration_date": "2026-04-06",
                "underlying_symbol": "SPX",
                "option_type": "Put Credit Spread",
                "short_strike": "6450",
                "long_strike": "6445",
                "spread_width": "5",
                "contracts": "1",
                "actual_entry_credit": "1.2",
                "actual_exit_value": "0.4",
            },
            follow_redirects=True,
        )

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"expected move is still unresolved", response.data)

    def test_distance_field_stays_visible_read_only_and_system_derived_in_edit_form(self):
        create_response = self.client.post(
            "/trades/real/new",
            data={
                "trade_mode": "real",
                "system_name": "Apollo",
                "journal_name": "Apollo Main",
                "status": "open",
                "trade_date": "2026-04-03",
                "expiration_date": "2026-04-06",
                "underlying_symbol": "SPX",
                "spx_at_entry": "6500",
                "option_type": "Put Credit Spread",
                "short_strike": "6450",
                "long_strike": "6445",
                "spread_width": "5",
                "contracts": "1",
                "actual_entry_credit": "1.20",
                "distance_to_short": "",
            },
            follow_redirects=True,
        )

        self.assertEqual(create_response.status_code, 200)
        self.assertIn(b"Distance to short", create_response.data)

        with closing(sqlite3.connect(self.database_path)) as connection:
            connection.row_factory = sqlite3.Row
            row = connection.execute("SELECT * FROM trades WHERE trade_mode = 'real'").fetchone()

        self.assertIsNotNone(row)
        self.assertAlmostEqual(row["distance_to_short"], 50.0)

        edit_response = self.client.get(f"/trades/real/{row['id']}/edit")

        self.assertEqual(edit_response.status_code, 200)
        self.assertIn(b"Distance to short", edit_response.data)
        self.assertIn(b'id="distance_to_short"', edit_response.data)
        self.assertIn(b"readonly", edit_response.data)
        self.assertIn(b'data-distance-source="derived"', edit_response.data)
        self.assertIn(b"System-derived from SPX at entry and short strike.", edit_response.data)

    def test_distance_is_recalculated_on_create_and_edit_when_entry_inputs_exist(self):
        create_payload = {
            "trade_mode": "real",
            "system_name": "Apollo",
            "journal_name": "Apollo Main",
            "status": "open",
            "trade_date": "2026-04-03",
            "expiration_date": "2026-04-06",
            "underlying_symbol": "SPX",
            "spx_at_entry": "6500",
            "option_type": "Put Credit Spread",
            "short_strike": "6450",
            "long_strike": "6445",
            "spread_width": "5",
            "contracts": "1",
            "actual_entry_credit": "1.20",
            "distance_to_short": "",
        }
        self.client.post("/trades/real/new", data=create_payload, follow_redirects=True)

        with closing(sqlite3.connect(self.database_path)) as connection:
            connection.row_factory = sqlite3.Row
            row = connection.execute("SELECT * FROM trades WHERE trade_mode = 'real'").fetchone()

        self.assertIsNotNone(row)
        trade_id = row["id"]
        self.assertAlmostEqual(row["distance_to_short"], 50.0)

        edit_response = self.client.post(
            f"/trades/real/{trade_id}/edit",
            data={
                **create_payload,
                "option_type": "Call Credit Spread",
                "short_strike": "6540",
                "long_strike": "6545",
                "distance_to_short": "123",
            },
            follow_redirects=True,
        )

        self.assertEqual(edit_response.status_code, 200)

        with closing(sqlite3.connect(self.database_path)) as connection:
            connection.row_factory = sqlite3.Row
            updated_row = connection.execute("SELECT * FROM trades WHERE id = ?", (trade_id,)).fetchone()

        self.assertAlmostEqual(updated_row["distance_to_short"], 40.0)

    def test_distance_preserves_stored_fallback_when_entry_inputs_are_missing(self):
        self.client.post(
            "/trades/real/new",
            data={
                "trade_mode": "real",
                "system_name": "Apollo",
                "journal_name": "Apollo Main",
                "status": "open",
                "trade_date": "2026-04-03",
                "expiration_date": "2026-04-06",
                "underlying_symbol": "SPX",
                "short_strike": "6450",
                "long_strike": "6445",
                "spread_width": "5",
                "contracts": "1",
                "actual_entry_credit": "1.20",
                "distance_to_short": "88",
            },
            follow_redirects=True,
        )

        with closing(sqlite3.connect(self.database_path)) as connection:
            connection.row_factory = sqlite3.Row
            row = connection.execute("SELECT * FROM trades WHERE trade_mode = 'real'").fetchone()

        self.assertIsNotNone(row)
        self.assertAlmostEqual(row["distance_to_short"], 88.0)

        edit_response = self.client.get(f"/trades/real/{row['id']}/edit")

        self.assertEqual(edit_response.status_code, 200)
        self.assertIn(b'data-distance-source="original"', edit_response.data)
        self.assertIn(b"Stored journal distance retained as the authoritative value.", edit_response.data)

    def test_journal_actions_are_summary_only_and_edit_trade_renders_position_management(self):
        self.client.post(
            "/trades/real/new",
            data={
                "trade_mode": "real",
                "system_name": "Apollo",
                "journal_name": "Apollo Main",
                "candidate_profile": "Standard",
                "status": "open",
                "trade_date": "2026-04-03",
                "entry_datetime": "2026-04-03T09:35",
                "expiration_date": "2026-04-06",
                "underlying_symbol": "SPX",
                "spx_at_entry": "6607.49",
                "short_strike": "6505",
                "long_strike": "6480",
                "spread_width": "25",
                "contracts": "7",
                "actual_entry_credit": "1.60",
            },
            follow_redirects=True,
        )

        dashboard_response = self.client.get("/trades/real")

        with closing(sqlite3.connect(self.database_path)) as connection:
            connection.row_factory = sqlite3.Row
            trade_id = connection.execute("SELECT id FROM trades WHERE trade_number = 1").fetchone()["id"]

        self.assertEqual(dashboard_response.status_code, 200)
        self.assertNotIn(f"/trades/real/{trade_id}/reduce".encode(), dashboard_response.data)
        self.assertNotIn(f"/trades/real/{trade_id}/expire".encode(), dashboard_response.data)
        self.assertIn(b">Edit<", dashboard_response.data)
        self.assertIn(b">Delete<", dashboard_response.data)

        edit_response = self.client.get(f"/trades/real/{trade_id}/edit")

        self.assertEqual(edit_response.status_code, 200)
        self.assertIn(b"Position Management", edit_response.data)
        self.assertIn(b"Add Close Event", edit_response.data)
        self.assertIn(b"Expire Remaining", edit_response.data)
        self.assertIn(b"Close Remaining", edit_response.data)
        self.assertIn(b'name="close_event_contracts_closed"', edit_response.data)
        self.assertIn(b'name="close_event_event_datetime"', edit_response.data)
        self.assertIn(b'name="close_event_notes_exit"', edit_response.data)

    def test_edit_trade_save_persists_close_events_and_updates_journal_summary(self):
        create_payload = {
            "trade_mode": "real",
            "system_name": "Apollo",
            "journal_name": "Apollo Main",
            "candidate_profile": "Standard",
            "status": "open",
            "trade_date": "2026-04-03",
            "entry_datetime": "2026-04-03T09:35",
            "expiration_date": "2026-04-06",
            "underlying_symbol": "SPX",
            "spx_at_entry": "6607.49",
            "short_strike": "6505",
            "long_strike": "6480",
            "spread_width": "25",
            "contracts": "7",
            "actual_entry_credit": "1.60",
            "distance_to_short": "102.49",
        }
        self.client.post("/trades/real/new", data=create_payload, follow_redirects=True)

        with closing(sqlite3.connect(self.database_path)) as connection:
            connection.row_factory = sqlite3.Row
            trade_id = connection.execute("SELECT id FROM trades WHERE trade_number = 1").fetchone()["id"]

        response = self.client.post(
            f"/trades/real/{trade_id}/edit",
            data={
                **create_payload,
                "trade_number": "1",
                "notes_entry": "",
                "notes_exit": "",
                "close_reason": "",
                "close_events_present": "1",
                "prefill_source": "",
                "system_version": "",
                "close_event_id": [""],
                "close_event_contracts_closed": ["2"],
                "close_event_actual_exit_value": ["4.70"],
                "close_event_method": ["Reduce"],
            },
            follow_redirects=True,
        )

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"expected move is still unresolved", response.data)
        self.assertIn(b">Reduced<", response.data)
        self.assertIn(b"Partial @ 4.7", response.data)
        self.assertIn(b">7<", response.data)
        self.assertIn(b">2<", response.data)
        self.assertIn(b">5<", response.data)

        with closing(sqlite3.connect(self.database_path)) as connection:
            connection.row_factory = sqlite3.Row
            row = connection.execute("SELECT * FROM trades WHERE id = ?", (trade_id,)).fetchone()
            events = connection.execute("SELECT * FROM trade_close_events WHERE trade_id = ? ORDER BY id ASC", (trade_id,)).fetchall()

        self.assertEqual(row["status"], "open")
        self.assertAlmostEqual(row["gross_pnl"], 180.0)
        self.assertAlmostEqual(row["actual_exit_value"], 4.7)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["event_type"], "reduce")
        self.assertEqual(events[0]["contracts_closed"], 2)

        detail_response = self.client.get(f"/trades/real/{trade_id}/edit")

        self.assertEqual(detail_response.status_code, 200)
        self.assertIn(b"Original Contracts", detail_response.data)
        self.assertIn(b"Closed Contracts", detail_response.data)
        self.assertIn(b"Remaining Contracts", detail_response.data)
        self.assertIn(b"Realized P/L", detail_response.data)
        self.assertIn(b'id="status_display"', detail_response.data)
        self.assertIn(b'value="Reduced"', detail_response.data)
        self.assertNotIn(b'name="status"', detail_response.data)
        self.assertIn(b'value="2"', detail_response.data)
        self.assertIn(b'value="4.70"', detail_response.data)

    def test_edit_trade_can_reduce_again_and_expire_remaining_after_partial_reduction(self):
        create_payload = {
            "trade_mode": "real",
            "system_name": "Apollo",
            "journal_name": "Apollo Main",
            "candidate_profile": "Standard",
            "status": "open",
            "trade_date": "2026-04-03",
            "entry_datetime": "2026-04-03T09:35",
            "expiration_date": "2026-04-06",
            "underlying_symbol": "SPX",
            "short_strike": "6505",
            "long_strike": "6480",
            "spread_width": "25",
            "contracts": "7",
            "actual_entry_credit": "1.60",
            "distance_to_short": "102.49",
        }
        self.client.post("/trades/real/new", data=create_payload, follow_redirects=True)

        with closing(sqlite3.connect(self.database_path)) as connection:
            connection.row_factory = sqlite3.Row
            trade_id = connection.execute("SELECT id FROM trades WHERE trade_number = 1").fetchone()["id"]

        first_reduction = self.client.post(
            f"/trades/real/{trade_id}/edit",
            data={
                **create_payload,
                "trade_number": "1",
                "notes_entry": "",
                "notes_exit": "",
                "close_reason": "",
                "close_events_present": "1",
                "prefill_source": "",
                "system_version": "",
                "close_event_id": [""],
                "close_event_contracts_closed": ["2"],
                "close_event_actual_exit_value": ["4.70"],
                "close_event_method": ["Reduce"],
            },
            follow_redirects=True,
        )

        self.assertEqual(first_reduction.status_code, 200)

        with closing(sqlite3.connect(self.database_path)) as connection:
            connection.row_factory = sqlite3.Row
            persisted_event = connection.execute(
                "SELECT * FROM trade_close_events WHERE trade_id = ? ORDER BY id ASC",
                (trade_id,),
            ).fetchone()

        expire_response = self.client.post(
            f"/trades/real/{trade_id}/edit",
            data={
                **create_payload,
                "trade_number": "1",
                "notes_entry": "",
                "notes_exit": "",
                "close_reason": "",
                "close_events_present": "1",
                "prefill_source": "",
                "system_version": "",
                "close_event_id": [str(persisted_event["id"]), ""],
                "close_event_contracts_closed": ["2", "5"],
                "close_event_actual_exit_value": ["4.70", "0.00"],
                "close_event_method": ["Reduce", "Expire"],
            },
            follow_redirects=True,
        )

        self.assertEqual(expire_response.status_code, 200)
        self.assertIn(b">Expired<", expire_response.data)

        with closing(sqlite3.connect(self.database_path)) as connection:
            connection.row_factory = sqlite3.Row
            row = connection.execute("SELECT * FROM trades WHERE id = ?", (trade_id,)).fetchone()
            events = connection.execute(
                "SELECT * FROM trade_close_events WHERE trade_id = ? ORDER BY id ASC",
                (trade_id,),
            ).fetchall()

        self.assertEqual(row["status"], "expired")
        self.assertEqual(len(events), 2)
        self.assertEqual(events[0]["event_type"], "reduce")
        self.assertEqual(events[1]["event_type"], "expire")
        self.assertEqual(events[1]["contracts_closed"], 5)

    def test_edit_trade_can_reduce_again_and_fully_close_remaining_after_partial_reduction(self):
        create_payload = {
            "trade_mode": "real",
            "system_name": "Apollo",
            "journal_name": "Apollo Main",
            "candidate_profile": "Standard",
            "status": "open",
            "trade_date": "2026-04-03",
            "entry_datetime": "2026-04-03T09:35",
            "expiration_date": "2026-04-06",
            "underlying_symbol": "SPX",
            "short_strike": "6450",
            "long_strike": "6445",
            "spread_width": "5",
            "contracts": "3",
            "actual_entry_credit": "1.20",
            "distance_to_short": "50",
        }
        self.client.post("/trades/real/new", data=create_payload, follow_redirects=True)

        with closing(sqlite3.connect(self.database_path)) as connection:
            connection.row_factory = sqlite3.Row
            trade_id = connection.execute("SELECT id FROM trades WHERE trade_number = 1").fetchone()["id"]

        self.client.post(
            f"/trades/real/{trade_id}/edit",
            data={
                **create_payload,
                "trade_number": "1",
                "notes_entry": "",
                "notes_exit": "",
                "close_reason": "",
                "close_events_present": "1",
                "prefill_source": "",
                "system_version": "",
                "close_event_id": [""],
                "close_event_contracts_closed": ["1"],
                "close_event_actual_exit_value": ["0.40"],
                "close_event_method": ["Reduce"],
            },
            follow_redirects=True,
        )

        with closing(sqlite3.connect(self.database_path)) as connection:
            connection.row_factory = sqlite3.Row
            persisted_event = connection.execute(
                "SELECT * FROM trade_close_events WHERE trade_id = ? ORDER BY id ASC",
                (trade_id,),
            ).fetchone()

        close_response = self.client.post(
            f"/trades/real/{trade_id}/edit",
            data={
                **create_payload,
                "trade_number": "1",
                "notes_entry": "",
                "notes_exit": "",
                "close_reason": "",
                "close_events_present": "1",
                "prefill_source": "",
                "system_version": "",
                "close_event_id": [str(persisted_event["id"]), ""],
                "close_event_contracts_closed": ["1", "2"],
                "close_event_actual_exit_value": ["0.40", "0.80"],
                "close_event_method": ["Reduce", "Close"],
            },
            follow_redirects=True,
        )

        self.assertEqual(close_response.status_code, 200)

        with closing(sqlite3.connect(self.database_path)) as connection:
            connection.row_factory = sqlite3.Row
            row = connection.execute("SELECT * FROM trades WHERE id = ?", (trade_id,)).fetchone()
            events = connection.execute(
                "SELECT * FROM trade_close_events WHERE trade_id = ? ORDER BY id ASC",
                (trade_id,),
            ).fetchall()

        self.assertEqual(row["status"], "closed")
        self.assertEqual(len(events), 2)
        self.assertEqual(events[1]["event_type"], "close")
        self.assertEqual(events[1]["contracts_closed"], 2)

    def test_manage_trades_notification_settings_persist_per_trade(self):
        create_payload = {
            "trade_mode": "real",
            "system_name": "Apollo",
            "journal_name": "Apollo Main",
            "candidate_profile": "Standard",
            "status": "open",
            "trade_date": "2026-04-10",
            "entry_datetime": "2026-04-10T09:35",
            "expiration_date": "2026-04-10",
            "underlying_symbol": "SPX",
            "spx_at_entry": "6800",
            "vix_at_entry": "18",
            "structure_grade": "Good",
            "macro_grade": "None",
            "expected_move": "40",
            "expected_move_used": "40",
            "option_type": "Put Credit Spread",
            "short_strike": "6750",
            "long_strike": "6740",
            "spread_width": "10",
            "contracts": "2",
            "actual_entry_credit": "1.80",
            "distance_to_short": "50",
        }
        self.client.post("/trades/real/new", data=create_payload, follow_redirects=True)

        with closing(sqlite3.connect(self.database_path)) as connection:
            connection.row_factory = sqlite3.Row
            trade_id = connection.execute("SELECT id FROM trades WHERE trade_number = 1").fetchone()["id"]

        response = self.client.post(
            f"/management/open-trades/{trade_id}/notifications",
            data={
                "notification_enabled_SHORT_STRIKE_PROXIMITY": "1",
                "notification_threshold_SHORT_STRIKE_PROXIMITY": "7.5",
                "notification_description_SHORT_STRIKE_PROXIMITY": "Watch the short strike",
                "notification_enabled_TIME_WINDOW": "1",
                "notification_threshold_TIME_WINDOW": "1.25",
                "notification_description_TIME_WINDOW": "Final hour check",
            },
            follow_redirects=False,
        )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers["Location"], "/management/open-trades")

        repository = self.app.extensions["trade_notification_repository"]
        notifications = repository.load_trade_notifications(trade_id)
        short_rule = next(item for item in notifications if item["type"] == "SHORT_STRIKE_PROXIMITY")
        time_rule = next(item for item in notifications if item["type"] == "TIME_WINDOW")

        self.assertTrue(short_rule["enabled"])
        self.assertAlmostEqual(short_rule["threshold"], 7.5)
        self.assertEqual(short_rule["description"], "Watch the short strike")
        self.assertTrue(time_rule["enabled"])
        self.assertAlmostEqual(time_rule["threshold"], 1.25)
        self.assertEqual(time_rule["description"], "Final hour check")

    def test_manage_trades_send_close_to_journal_prefills_edit_trade_close_event(self):
        create_payload = {
            "trade_mode": "simulated",
            "system_name": "Apollo",
            "journal_name": "Apollo Main",
            "candidate_profile": "Standard",
            "status": "open",
            "trade_date": "2026-04-10",
            "entry_datetime": "2026-04-10T09:35",
            "expiration_date": "2026-04-10",
            "underlying_symbol": "SPX",
            "spx_at_entry": "6800",
            "vix_at_entry": "18",
            "structure_grade": "Good",
            "macro_grade": "None",
            "expected_move": "40",
            "expected_move_used": "40",
            "option_type": "Put Credit Spread",
            "short_strike": "6750",
            "long_strike": "6740",
            "spread_width": "10",
            "contracts": "2",
            "actual_entry_credit": "1.80",
            "distance_to_short": "50",
        }
        self.client.post("/trades/simulated/new", data=create_payload, follow_redirects=True)

        with closing(sqlite3.connect(self.database_path)) as connection:
            connection.row_factory = sqlite3.Row
            trade_id = connection.execute("SELECT id FROM trades WHERE trade_number = 1").fetchone()["id"]

        manager = self.app.extensions["open_trade_manager"]
        manager.evaluate_trade_record = lambda target_trade_id: {
            "trade_id": target_trade_id,
            "trade_number": 1,
            "trade_mode": "Simulated",
            "contracts": 2,
            "current_spread_mark": 4.9,
            "current_total_close_cost_display": "$980.00",
            "evaluated_at_display": "2026-04-10 12:00 PM CDT",
            "alert_state": {
                "last_alert_type": None,
                "last_alert_priority": None,
                "last_alert_sent_at": None,
                "alert_count": 0,
            },
        }

        response = self.client.post(f"/management/open-trades/{trade_id}/prefill-close", follow_redirects=False)

        self.assertEqual(response.status_code, 302)
        self.assertIn(f"/trades/simulated/{trade_id}/edit#position-management", response.headers["Location"])

        management_response = self.client.get("/management/open-trades")
        self.assertIn(b"Send to Close", management_response.data)
        self.assertIn(b"management-table-wrap", management_response.data)
        self.assertNotIn(b"trade-log-wrap", management_response.data)
        self.assertNotIn(b">Thesis<", management_response.data)
        self.assertNotIn(b">Distance to Long<", management_response.data)
        self.assertNotIn(b">Entry EM Multiple<", management_response.data)
        self.assertNotIn(b">Mode<", management_response.data)

        edit_response = self.client.get(response.headers["Location"], follow_redirects=True)

        self.assertEqual(edit_response.status_code, 200)
        self.assertIn(b'value="2"', edit_response.data)
        self.assertIn(b'value="4.90"', edit_response.data)
        self.assertIn(b'Manage Trade Prefill', edit_response.data)
        self.assertIn(b'Prefilled from Manage Trades', edit_response.data)

    def test_management_status_update_route_posts_manual_real_pushover_message(self):
        manager = self.app.extensions["open_trade_manager"]
        calls = []

        def fake_send_manual_status_update(*, trade_mode):
            calls.append(trade_mode)
            return {
                "sent": True,
                "error": "",
                "trade_mode": trade_mode,
                "record_count": 2,
                "priority": 0,
                "alert_type": f"manual-{trade_mode}-status-update",
                "sent_at": "2026-04-10T12:00:00-05:00",
            }

        manager.send_manual_status_update = fake_send_manual_status_update
        manager.notifications_enabled = lambda: False

        response = self.client.post("/management/open-trades/status-update/real", follow_redirects=True)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(calls, ["real"])
        self.assertIn(b"Sent Pushover status update for 2 real open trade(s).", response.data)
        self.assertIn(b"Automatic notifications remain OFF.", response.data)

    def test_edit_trade_can_remove_close_events_and_reject_overclose_validation(self):
        create_payload = {
            "trade_mode": "real",
            "system_name": "Apollo",
            "journal_name": "Apollo Main",
            "candidate_profile": "Standard",
            "status": "open",
            "trade_date": "2026-04-03",
            "entry_datetime": "2026-04-03T09:35",
            "expiration_date": "2026-04-06",
            "underlying_symbol": "SPX",
            "short_strike": "6450",
            "long_strike": "6445",
            "spread_width": "5",
            "contracts": "3",
            "actual_entry_credit": "1.20",
            "distance_to_short": "50",
        }
        self.client.post("/trades/real/new", data=create_payload, follow_redirects=True)

        with closing(sqlite3.connect(self.database_path)) as connection:
            connection.row_factory = sqlite3.Row
            trade_id = connection.execute("SELECT id FROM trades WHERE trade_number = 1").fetchone()["id"]

        self.client.post(
            f"/trades/real/{trade_id}/edit",
            data={
                **create_payload,
                "trade_number": "1",
                "notes_entry": "",
                "notes_exit": "",
                "close_reason": "",
                "close_events_present": "1",
                "prefill_source": "",
                "system_version": "",
                "close_event_id": ["", ""],
                "close_event_contracts_closed": ["1", "1"],
                "close_event_actual_exit_value": ["0.40", "0.60"],
                "close_event_method": ["Reduce", "Reduce"],
            },
            follow_redirects=True,
        )

        with closing(sqlite3.connect(self.database_path)) as connection:
            connection.row_factory = sqlite3.Row
            persisted_events = connection.execute("SELECT * FROM trade_close_events WHERE trade_id = ? ORDER BY id ASC", (trade_id,)).fetchall()

        self.assertEqual(len(persisted_events), 2)

        removal_response = self.client.post(
            f"/trades/real/{trade_id}/edit",
            data={
                **create_payload,
                "trade_number": "1",
                "notes_entry": "",
                "notes_exit": "",
                "close_reason": "",
                "close_events_present": "1",
                "prefill_source": "",
                "system_version": "",
                "close_event_id": [str(persisted_events[1]["id"])],
                "close_event_contracts_closed": ["1"],
                "close_event_actual_exit_value": ["0.60"],
                "close_event_method": ["Reduce"],
            },
            follow_redirects=True,
        )

        self.assertEqual(removal_response.status_code, 200)
        self.assertIn(b"expected move is still unresolved", removal_response.data)
        self.assertIn(b">Reduced<", removal_response.data)
        self.assertIn(b">1<", removal_response.data)
        self.assertIn(b">2<", removal_response.data)

        with closing(sqlite3.connect(self.database_path)) as connection:
            connection.row_factory = sqlite3.Row
            row = connection.execute("SELECT * FROM trades WHERE id = ?", (trade_id,)).fetchone()
            events_after_removal = connection.execute("SELECT * FROM trade_close_events WHERE trade_id = ? ORDER BY id ASC", (trade_id,)).fetchall()

        self.assertEqual(len(events_after_removal), 1)
        self.assertAlmostEqual(row["gross_pnl"], 300.0)

        invalid_response = self.client.post(
            f"/trades/real/{trade_id}/edit",
            data={
                **create_payload,
                "trade_number": "1",
                "notes_entry": "",
                "notes_exit": "",
                "close_reason": "",
                "close_events_present": "1",
                "prefill_source": "",
                "system_version": "",
                "close_event_id": [str(events_after_removal[0]["id"]), ""],
                "close_event_contracts_closed": ["1", "3"],
                "close_event_actual_exit_value": ["0.60", "0.00"],
                "close_event_method": ["Reduce", "Expire"],
            },
            follow_redirects=True,
        )

        self.assertEqual(invalid_response.status_code, 200)
        self.assertIn(b"The total closed quantity cannot exceed the original contracts.", invalid_response.data)

        with closing(sqlite3.connect(self.database_path)) as connection:
            remaining_events = connection.execute("SELECT COUNT(*) FROM trade_close_events WHERE trade_id = ?", (trade_id,)).fetchone()[0]

        self.assertEqual(remaining_events, 1)

    def test_apollo_prefill_transfer_does_not_save_until_trade_is_submitted(self):
        candidate_payload = {
            "system_version": "6.2",
            "candidate_profile": "Aggressive",
            "expiration_date": "2026-04-06",
            "underlying_symbol": "SPX",
            "spx_at_entry": "6500",
            "vix_at_entry": "20",
            "structure_grade": "Good",
            "macro_grade": "None",
            "expected_move": "45",
            "short_strike": "6450",
            "long_strike": "6445",
            "spread_width": "5",
            "contracts": "2",
            "candidate_credit_estimate": "1.2",
            "distance_to_short": "50",
            "pass_type": "aggressive_strict",
            "premium_per_contract": "120",
            "total_premium": "240",
            "max_theoretical_risk": "760",
            "risk_efficiency": "0.3158",
            "credit_efficiency_pct": "31.58",
            "target_em": "1.5",
            "short_delta": "0.12",
        }

        transfer_response = self.client.post(
            "/apollo/prefill-candidate",
            data={"target_mode": "simulated", **candidate_payload},
            follow_redirects=False,
        )

        self.assertEqual(transfer_response.status_code, 302)
        self.assertIn("/trades/simulated?prefill=1#trade-entry-form", transfer_response.headers["Location"])

        with closing(sqlite3.connect(self.database_path)) as connection:
            total_rows = connection.execute("SELECT COUNT(*) FROM trades").fetchone()[0]

        self.assertEqual(total_rows, 0)

        journal_response = self.client.get("/trades/simulated?prefill=1")
        direct_response = self.client.get("/trades/simulated")

        self.assertEqual(journal_response.status_code, 200)
        self.assertIn(b"Apollo candidate data is loaded into this draft.", journal_response.data)
        self.assertIn(b'value="6450"', journal_response.data)
        self.assertIn(b'value="6445"', journal_response.data)
        self.assertIn(b'value="Aggressive" selected', journal_response.data)
        self.assertNotIn(b'value="6450"', direct_response.data)

        save_payload = coerce_apollo_trade_input(candidate_payload, trade_mode="simulated")
        save_response = self.client.post("/trades/simulated/new", data=save_payload, follow_redirects=True)

        self.assertEqual(save_response.status_code, 200)
        self.assertIn(b"Trade saved successfully.", save_response.data)

        with closing(sqlite3.connect(self.database_path)) as connection:
            connection.row_factory = sqlite3.Row
            rows = connection.execute("SELECT * FROM trades ORDER BY id ASC").fetchall()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["system_name"], "Apollo")
        self.assertEqual(rows[0]["journal_name"], "Apollo Main")
        self.assertEqual(rows[0]["system_version"], "6.2")
        self.assertEqual(rows[0]["trade_mode"], "simulated")
        self.assertEqual(rows[0]["status"], "open")
        self.assertEqual(rows[0]["candidate_profile"], "Aggressive")
        self.assertAlmostEqual(rows[0]["actual_entry_credit"], 1.2)
        self.assertEqual(rows[0]["pass_type"], "aggressive_strict")
        self.assertAlmostEqual(rows[0]["premium_per_contract"], 120.0)
        self.assertAlmostEqual(rows[0]["total_premium"], 240.0)
        self.assertAlmostEqual(rows[0]["max_theoretical_risk"], 760.0)
        self.assertAlmostEqual(rows[0]["risk_efficiency"], 0.3158)
        self.assertAlmostEqual(rows[0]["target_em"], 1.5)
        self.assertEqual(rows[0]["trade_number"], 1)

        refresh_response = self.client.get("/trades/simulated")

        self.assertEqual(refresh_response.status_code, 200)

        with closing(sqlite3.connect(self.database_path)) as connection:
            rows_after_refresh = connection.execute("SELECT COUNT(*) FROM trades").fetchone()[0]

        self.assertEqual(rows_after_refresh, 1)

    def test_duplicate_apollo_prefill_save_is_blocked(self):
        candidate_payload = {
            "candidate_profile": "Standard",
            "expiration_date": "2026-04-06",
            "underlying_symbol": "SPX",
            "spx_at_entry": "6500",
            "vix_at_entry": "20",
            "structure_grade": "Good",
            "macro_grade": "None",
            "expected_move": "45",
            "short_strike": "6450",
            "long_strike": "6445",
            "spread_width": "5",
            "contracts": "2",
            "candidate_credit_estimate": "1.2",
            "distance_to_short": "50",
            "short_delta": "0.12",
        }
        save_payload = coerce_apollo_trade_input(candidate_payload, trade_mode="simulated")

        first_response = self.client.post("/trades/simulated/new", data=save_payload, follow_redirects=False)
        second_response = self.client.post("/trades/simulated/new", data=save_payload, follow_redirects=True)

        self.assertEqual(first_response.status_code, 302)
        self.assertEqual(second_response.status_code, 200)
        self.assertIn(b"already saved recently", second_response.data)

        with closing(sqlite3.connect(self.database_path)) as connection:
            total_rows = connection.execute("SELECT COUNT(*) FROM trades").fetchone()[0]

        self.assertEqual(total_rows, 1)

    def test_trade_import_preview_and_confirm_from_csv_skips_duplicates(self):
        self.client.post(
            "/trades/real/new",
            data={
                "trade_mode": "real",
                "system_name": "Apollo",
                "journal_name": "Apollo Main",
                "system_version": "2.0",
                "candidate_profile": "Standard",
                "status": "open",
                "trade_date": "2026-04-03",
                "expiration_date": "2026-04-06",
                "underlying_symbol": "SPX",
                "short_strike": "6450",
                "long_strike": "6445",
                "spread_width": "5",
                "contracts": "2",
                "actual_entry_credit": "1.20",
            },
            follow_redirects=True,
        )

        csv_payload = b"Date,Expiration,Profile,Symbol,Short Strike,Long Strike,Qty,Net Credit\n2026-04-03,2026-04-06,Standard,SPX,6450,6445,2,1.20\n2026-04-04,2026-04-07,Aggressive,SPX,6460,6455,3,1.55\n"
        preview_response = self.client.post(
            "/trades/real/import/preview",
            data={
                "import_journal_name": "Apollo Main",
                "import_file": (io.BytesIO(csv_payload), "history.csv"),
            },
            content_type="multipart/form-data",
        )

        self.assertEqual(preview_response.status_code, 200)
        self.assertIn(b"Import Preview", preview_response.data)
        self.assertIn(b"Duplicate", preview_response.data)
        self.assertIn(b"Ready", preview_response.data)

        token_match = re.search(rb'name="import_token" value="([a-f0-9]+)"', preview_response.data)
        self.assertIsNotNone(token_match)

        confirm_response = self.client.post(
            "/trades/real/import/confirm",
            data={"import_token": token_match.group(1).decode("utf-8")},
            follow_redirects=True,
        )

        self.assertEqual(confirm_response.status_code, 200)
        self.assertIn(b"Imported 1 trade.", confirm_response.data)
        self.assertIn(b"Skipped 1 duplicate.", confirm_response.data)

        with closing(sqlite3.connect(self.database_path)) as connection:
            connection.row_factory = sqlite3.Row
            rows = connection.execute("SELECT * FROM trades WHERE trade_mode = 'real' ORDER BY id ASC").fetchall()

        self.assertEqual(len(rows), 2)
        imported_row = rows[-1]
        self.assertEqual(imported_row["candidate_profile"], "Aggressive")
        self.assertEqual(imported_row["status"], "open")
        self.assertEqual(imported_row["system_name"], "Apollo")
        self.assertAlmostEqual(imported_row["actual_entry_credit"], 1.55)
        self.assertEqual(imported_row["trade_number"], 2)

    def test_trade_import_preview_and_confirm_from_xlsx_supports_partial_rows(self):
        frame = pd.DataFrame(
            [
                {
                    "Entry Time": "2026-04-08T09:35",
                    "Expiration": "2026-04-11",
                    "Profile": "Fortress",
                    "Ticker": "SPX",
                    "Short": 6435,
                    "Long": 6430,
                    "Qty": 1,
                    "Credit": 1.10,
                    "Close Value": 0.35,
                    "Notes": "Imported historical trade",
                }
            ]
        )
        excel_buffer = io.BytesIO()
        frame.to_excel(excel_buffer, index=False)
        excel_buffer.seek(0)

        preview_response = self.client.post(
            "/trades/simulated/import/preview",
            data={
                "import_journal_name": "Apollo Main",
                "import_file": (excel_buffer, "history.xlsx"),
            },
            content_type="multipart/form-data",
        )

        self.assertEqual(preview_response.status_code, 200)
        self.assertIn(b"Import Preview", preview_response.data)
        self.assertIn(b"Ready", preview_response.data)

        token_match = re.search(rb'name="import_token" value="([a-f0-9]+)"', preview_response.data)
        self.assertIsNotNone(token_match)

        confirm_response = self.client.post(
            "/trades/simulated/import/confirm",
            data={"import_token": token_match.group(1).decode("utf-8")},
            follow_redirects=True,
        )

        self.assertEqual(confirm_response.status_code, 200)
        self.assertIn(b"Imported 1 trade.", confirm_response.data)

        with closing(sqlite3.connect(self.database_path)) as connection:
            connection.row_factory = sqlite3.Row
            row = connection.execute("SELECT * FROM trades WHERE trade_mode = 'simulated'").fetchone()

        self.assertIsNotNone(row)
        self.assertEqual(row["candidate_profile"], "Fortress")
        self.assertEqual(row["status"], "closed")
        self.assertEqual(row["trade_date"], "2026-04-08")
        self.assertEqual(row["entry_datetime"], "2026-04-08T09:35")
        self.assertAlmostEqual(row["actual_entry_credit"], 1.1)
        self.assertAlmostEqual(row["actual_exit_value"], 0.35)
        self.assertAlmostEqual(row["gross_pnl"], 75.0)

    def test_trade_log_filters_sorting_and_result_shading_render(self):
        self.client.post(
            "/trades/real/new",
            data={
                "trade_number": "12",
                "trade_mode": "real",
                "system_name": "Kairos",
                "journal_name": "Apollo Main",
                "candidate_profile": "",
                "status": "closed",
                "trade_date": "2026-04-03",
                "expiration_date": "2026-04-06",
                "underlying_symbol": "SPX",
                "short_strike": "6450",
                "long_strike": "6445",
                "spread_width": "5",
                "contracts": "1",
                "actual_entry_credit": "1.20",
                "actual_exit_value": "4.80",
                "close_reason": "Black Swan stopout",
            },
            follow_redirects=True,
        )

        response = self.client.get("/trades/real")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"<legend>System</legend>", response.data)
        self.assertIn(b"<legend>Profile</legend>", response.data)
        self.assertIn(b"<legend>Result</legend>", response.data)
        self.assertIn(b'data-sort-key="trade-number"', response.data)
        self.assertIn(b'data-filter-group="system" value="apollo" checked', response.data)
        self.assertIn(b'data-filter-group="result" value="black-swan" checked', response.data)
        self.assertIn(b"trade-log-wrap", response.data)
        self.assertIn(b"trade-result-black-swan", response.data)
        self.assertIn(b">Black Swan<", response.data)
        self.assertIn(b">Legacy<", response.data)
        self.assertIn(b">Kairos<", response.data)
        self.assertNotIn(b"trade-inline-meta", response.data)

    @patch("app.execute_apollo_precheck")
    def test_apollo_page_renders_six_transfer_buttons(self, execute_apollo_precheck):
        execute_apollo_precheck.return_value = self._apollo_render_payload()

        response = self.client.post("/apollo")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Version 8.0.7", response.data)
        self.assertIn(b"Apollo: Greek God of Prophecy and Part-Time Options Trader", response.data)
        self.assertIn(b"Base Structure", response.data)
        self.assertIn(b"RSI Modifier", response.data)
        self.assertIn(b"Final Structure", response.data)
        self.assertIn(b"Apollo Gate 3 -- Engine", response.data)
        self.assertGreaterEqual(response.data.count(b"Premium / Contract"), 3)
        self.assertGreaterEqual(response.data.count(b"Send to Real Trades"), 3)
        self.assertGreaterEqual(response.data.count(b"Send to Simulated Trades"), 3)
        self.assertIn(b"$420", response.data)
        self.assertIn(b"$135", response.data)
        self.assertIn(b"$95", response.data)
        self.assertNotIn(b"Default view prioritizes", response.data)
        self.assertNotIn(b"Option-chain controls", response.data)
        self.assertNotIn(b"Local time", response.data)
        self.assertNotIn(b"Target next market day", response.data)
        self.assertNotIn(b"Checked at", response.data)
        self.assertIn(b"/apollo/prefill-candidate", response.data)

    @staticmethod
    def _apollo_render_payload():
        candidate_prefill = {
            "system_name": "Apollo",
            "journal_name": "Apollo Main",
            "system_version": "2.0",
            "candidate_profile": "Standard",
            "expiration_date": "2026-04-06",
            "underlying_symbol": "SPX",
            "spx_at_entry": "6500",
            "vix_at_entry": "20",
            "structure_grade": "Good",
            "macro_grade": "None",
            "expected_move": "45",
            "option_type": "Put Credit Spread",
            "short_strike": "6450",
            "long_strike": "6445",
            "spread_width": "5",
            "contracts": "2",
            "candidate_credit_estimate": "1.2",
            "actual_entry_credit": "1.2",
            "distance_to_short": "50",
            "em_multiple_floor": "1.8",
            "percent_floor": "1.2",
            "boundary_rule_used": "max(1.2% SPX, 1.80x expected move)",
            "actual_distance_to_short": "50",
            "actual_em_multiple": "1.11",
            "pass_type": "standard_strict",
            "premium_per_contract": "135",
            "total_premium": "1080",
            "max_theoretical_risk": "1200",
            "risk_efficiency": "0.1125",
            "target_em": "1.8",
            "fallback_used": "no",
            "fallback_rule_name": "",
            "short_delta": "0.12",
            "notes_entry": "Prefilled from Apollo candidate card.",
            "prefill_source": "apollo",
        }
        return {
            "title": "Apollo Gate 1 -- SPX Structure",
            "provider_name": "Test Provider",
            "status": "Allowed",
            "status_class": "allowed",
            "local_datetime": "Fri 2026-04-03 10:00 AM CDT",
            "run_timestamp": "Fri 2026-04-03 10:00 AM CDT",
            "spx_value": "6,500.00",
            "spx_as_of": "Now",
            "vix_value": "20.00",
            "vix_as_of": "Now",
            "macro_title": "Apollo Gate 2 -- Macro Event",
            "macro_source": "MarketWatch",
            "macro_grade": "None",
            "macro_grade_class": "good",
            "macro_target_day": "Mon Apr 06, 2026",
            "macro_checked_at": "Fri 2026-04-03 10:00 AM CDT",
            "macro_checked_dates": "2026-04-06",
            "macro_available": "Yes",
            "macro_event_count": 0,
            "macro_major_detected": "No",
            "macro_explanation": "—",
            "macro_diagnostic": {},
            "macro_source_attempts": [],
            "macro_events": [],
            "macro_major_events": [],
            "macro_minor_events": [],
            "next_market_day": "2026-04-06",
            "next_market_day_note": "—",
            "holiday_filter_applied": "No",
            "skipped_holiday_name": "—",
            "candidate_date_considered": "2026-04-06",
            "structure_available": False,
            "structure_preferred_source": "Schwab",
            "structure_attempted_sources": [],
            "structure_fallback_reason": "",
            "structure_source_used": "Schwab",
            "structure_grade": "Allowed",
            "structure_grade_class": "allowed",
            "structure_base_grade": "Neutral",
            "structure_final_grade": "Allowed",
            "structure_rsi_modifier": "None",
            "structure_rsi_note": "Daily RSI 44.10 does not trigger a structure upgrade.",
            "structure_trend_classification": "Balanced",
            "structure_damage_classification": "Contained",
            "structure_summary": "—",
            "structure_message": "Test structure",
            "structure_session_note": "",
            "structure_rules": [],
            "structure_chart": {"available": False, "points": []},
            "structure_session_high": "6,510.00",
            "structure_session_low": "6,490.00",
            "structure_current_price": "6,500.00",
            "structure_range_position": "50.00%",
            "structure_ema8": "6,499.00",
            "structure_ema21": "6,495.00",
            "structure_recent_price_action": "Stable",
            "structure_session_window": "09:30 to 16:00",
            "option_chain_success": True,
            "option_chain_status": "Ready",
            "option_chain_status_class": "good",
            "option_chain_source": "Schwab",
            "option_chain_heading_date": "Mon Apr 06, 2026",
            "option_chain_symbol_requested": "SPX",
            "option_chain_expiration_target": "2026-04-06",
            "option_chain_expiration": "2026-04-06",
            "option_chain_expiration_count": 1,
            "option_chain_puts_count": 12,
            "option_chain_calls_count": 12,
            "option_chain_rows_displayed": 24,
            "option_chain_display_puts_count": 12,
            "option_chain_display_calls_count": 12,
            "option_chain_min_premium_target": "1.00",
            "option_chain_rows_setting": "Adaptive",
            "option_chain_grouping": "Puts ascending → Calls ascending",
            "option_chain_strike_range": "—",
            "option_chain_message": "—",
            "option_chain_preview_rows": [],
            "option_chain_final_symbol": "SPX",
            "option_chain_final_expiration_sent": "2026-04-06",
            "option_chain_request_attempt_used": "1",
            "option_chain_raw_params_sent": {},
            "option_chain_error_detail": "—",
            "option_chain_attempt_results": [],
            "trade_candidates_title": "Apollo Gate 3 -- Engine",
            "trade_candidates_status": "Ready",
            "trade_candidates_status_class": "good",
            "trade_candidates_message": "Three candidates ready for review.",
            "trade_candidates_count": 3,
            "trade_candidates_count_label": "3 Modes",
            "trade_candidates_underlying_price": "6,500.00",
            "trade_candidates_expected_move": "45.00",
            "trade_candidates_expected_move_range": "6,455.00 to 6,545.00",
            "trade_candidates_diagnostics": {"selected_spreads": 3, "evaluated_spread_details": []},
            "trade_candidates_short_barrier_put": "<= 6450",
            "trade_candidates_short_barrier_call": ">= 6550",
            "trade_candidates_credit_map": {"available": False},
            "trade_candidates_items": [
                {
                    "mode_key": "fortress",
                    "mode_label": "Fortress",
                    "mode_descriptor": "Maximum safety",
                    "available": True,
                    "no_trade_message": "",
                    "position_label": "Put Spread",
                    "short_strike": "6440",
                    "long_strike": "6435",
                    "net_credit": "$4.20",
                    "premium_per_contract": "$420",
                    "total_premium": "$2,100",
                    "recommended_contract_size": "5",
                    "distance_points": "60",
                    "em_multiple": "1.33x EM",
                    "premium_received_dollars": "$2,100",
                    "premium_probability": "88%",
                    "routine_loss": "$300",
                    "routine_probability": "8%",
                    "black_swan_loss": "$600",
                    "tail_probability": "4%",
                    "max_theoretical_risk": "$1,000",
                    "risk_efficiency": "0.4200",
                    "max_loss": "$1,000",
                    "max_probability": "<1%",
                    "max_loss_per_contract": "100",
                    "pass_type_label": "Fortress",
                    "target_em": "2.00x EM",
                    "fallback_used": "No",
                    "actual_em_multiple": "1.33x EM",
                    "risk_cap_status": "Within risk cap",
                    "rationale": ["Fortress rationale"],
                    "exit_plan": ["Fortress exit"],
                    "diagnostics": ["Fortress diagnostic"],
                    "prefill_fields": {
                        **candidate_prefill,
                        "candidate_profile": "Fortress",
                        "short_strike": "6440",
                        "long_strike": "6435",
                        "actual_entry_credit": "4.2",
                        "candidate_credit_estimate": "4.2",
                        "distance_to_short": "60",
                        "em_multiple_floor": "2.0",
                        "percent_floor": "1.4",
                        "boundary_rule_used": "max(1.4% SPX, 2.00x expected move)",
                        "actual_distance_to_short": "60",
                        "actual_em_multiple": "1.33",
                        "pass_type": "fortress",
                        "premium_per_contract": "420",
                        "total_premium": "2100",
                        "max_theoretical_risk": "1000",
                        "risk_efficiency": "0.42",
                        "target_em": "2.0",
                    },
                },
                {
                    "mode_key": "standard",
                    "mode_label": "Standard",
                    "mode_descriptor": "Balanced core",
                    "available": True,
                    "no_trade_message": "",
                    "position_label": "Put Spread",
                    "short_strike": "6450",
                    "long_strike": "6445",
                    "net_credit": "$1.35",
                    "premium_per_contract": "$135",
                    "total_premium": "$1,080",
                    "recommended_contract_size": "8",
                    "distance_points": "50",
                    "em_multiple": "1.11x EM",
                    "premium_received_dollars": "$1,080",
                    "premium_probability": "85%",
                    "routine_loss": "$400",
                    "routine_probability": "10%",
                    "black_swan_loss": "$700",
                    "tail_probability": "5%",
                    "max_theoretical_risk": "$1,200",
                    "risk_efficiency": "0.1125",
                    "max_loss": "$1,200",
                    "max_probability": "<1%",
                    "max_loss_per_contract": "120",
                    "pass_type_label": "Standard",
                    "target_em": "1.80x EM",
                    "fallback_used": "No",
                    "actual_em_multiple": "1.11x EM",
                    "risk_cap_status": "Within risk cap",
                    "rationale": ["Standard rationale"],
                    "exit_plan": ["Standard exit"],
                    "diagnostics": ["Standard diagnostic"],
                    "prefill_fields": candidate_prefill,
                },
                {
                    "mode_key": "aggressive",
                    "mode_label": "Aggressive",
                    "mode_descriptor": "Higher premium",
                    "available": True,
                    "no_trade_message": "",
                    "position_label": "Put Spread",
                    "short_strike": "6460",
                    "long_strike": "6455",
                    "net_credit": "$0.95",
                    "premium_per_contract": "$95",
                    "total_premium": "$950",
                    "recommended_contract_size": "10",
                    "distance_points": "40",
                    "em_multiple": "0.89x EM",
                    "premium_received_dollars": "$950",
                    "premium_probability": "82%",
                    "routine_loss": "$550",
                    "routine_probability": "12%",
                    "black_swan_loss": "$850",
                    "tail_probability": "6%",
                    "max_theoretical_risk": "$1,500",
                    "risk_efficiency": "0.0633",
                    "max_loss": "$1,500",
                    "max_probability": "<1%",
                    "max_loss_per_contract": "150",
                    "pass_type_label": "Aggressive",
                    "target_em": "1.50x EM",
                    "fallback_used": "No",
                    "actual_em_multiple": "0.89x EM",
                    "risk_cap_status": "Within risk cap",
                    "rationale": ["Aggressive rationale"],
                    "exit_plan": ["Aggressive exit"],
                    "diagnostics": ["Aggressive diagnostic"],
                    "prefill_fields": {
                        **candidate_prefill,
                        "candidate_profile": "Aggressive",
                        "short_strike": "6460",
                        "long_strike": "6455",
                        "actual_entry_credit": "0.95",
                        "candidate_credit_estimate": "0.95",
                        "distance_to_short": "40",
                        "em_multiple_floor": "1.5",
                        "percent_floor": "1.0",
                        "boundary_rule_used": "max(1.0% SPX, 1.50x expected move)",
                        "actual_distance_to_short": "40",
                        "actual_em_multiple": "0.89",
                        "pass_type": "aggressive_strict",
                        "premium_per_contract": "95",
                        "total_premium": "950",
                        "max_theoretical_risk": "1500",
                        "risk_efficiency": "0.0633",
                        "target_em": "1.5",
                    },
                },
            ],
            "apollo_trigger_source": "button",
            "apollo_trigger_note": "Apollo was triggered by button.",
            "reasons": [],
        }


if __name__ == "__main__":
    unittest.main()

