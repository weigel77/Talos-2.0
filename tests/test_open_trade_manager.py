import sqlite3
import tempfile
import unittest
from contextlib import closing
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from services.open_trade_manager import OpenTradeManager
from services.trade_store import TradeStore


class StubMarketDataService:
    def __init__(self):
        self.current_spx = 6768.0
        self.current_vix = 21.5

    def get_latest_snapshot(self, ticker, query_type="latest"):
        if ticker == "^GSPC":
            return {
                "Latest Value": self.current_spx,
                "As Of": "2026-04-10 12:00:00 PM CDT",
            }
        return {
            "Latest Value": self.current_vix,
            "As Of": "2026-04-10 12:00:00 PM CDT",
        }

    def get_same_day_intraday_candles(self, ticker, interval_minutes=1, query_type="intraday"):
        session_anchor = datetime(2026, 4, 10, 8, 30, tzinfo=ZoneInfo("America/Chicago"))
        rows = []
        base_price = 6800.0
        for minute_offset in range(120):
            timestamp = session_anchor + pd.Timedelta(minutes=minute_offset)
            close_price = base_price - (minute_offset * 0.28)
            rows.append(
                {
                    "Datetime": timestamp,
                    "Open": close_price + 0.4,
                    "High": close_price + 0.7,
                    "Low": close_price - 0.8,
                    "Close": close_price,
                    "Volume": 1000 + minute_offset,
                }
            )
        return pd.DataFrame(rows)


class StubApolloService:
    def __init__(self):
        self.current_structure = "Neutral"
        self.current_macro = "Minor"

    def run_precheck(self):
        return {
            "structure": {"final_grade": self.current_structure, "grade": self.current_structure},
            "macro": {"grade": self.current_macro},
        }

    def build_management_context(self):
        return {
            "current_structure_grade": self.current_structure,
            "current_macro_grade": self.current_macro,
            "precheck": {},
        }


class StubOptionsChainService:
    def __init__(self):
        self.current_mark_by_expiration = {
            "2026-04-10": {
                "puts": [
                    {"strike": 6765.0, "bid": 5.8, "ask": 6.2, "mark": 6.0},
                    {"strike": 6725.0, "mark": 0.55},
                    {"strike": 6735.0, "mark": 1.15},
                    {"strike": 6750.0, "bid": 4.8, "ask": 5.0, "mark": 4.9},
                    {"strike": 6740.0, "bid": 1.1, "ask": 1.3, "mark": 1.2},
                    {"strike": 6760.0, "mark": 6.2},
                ],
                "calls": [
                    {"strike": 6770.0, "bid": 3.8, "ask": 4.2, "mark": 4.0},
                ],
            }
        }

    def get_spx_option_chain_summary(self, expiration_date):
        payload = self.current_mark_by_expiration.get(expiration_date.isoformat(), {"puts": [], "calls": []})
        return {
            "success": True,
            "expiration_date": expiration_date,
            "underlying_price": 6768.0,
            "puts": payload.get("puts", []),
            "calls": payload.get("calls", []),
        }


class StubPushoverService:
    def __init__(self):
        self.sent = []

    def send_notification(self, **kwargs):
        self.sent.append(kwargs)
        return {"ok": True, "request_id": f"req-{len(self.sent)}"}


class StubKairosService:
    def __init__(self):
        self.structure_status = "Weakening"
        self.momentum_status = "Weakening"

    def build_management_context(self):
        return {
            "current_structure_status": self.structure_status,
            "current_momentum_status": self.momentum_status,
        }

    def get_dashboard_payload(self):
        return {
            "latest_scan": {
                "structure_status": self.structure_status,
                "momentum_status": self.momentum_status,
            }
        }


class OpenTradeManagerTest(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database_path = Path(self.temp_dir.name) / "horme_trades.db"
        self.trade_store = TradeStore(self.database_path)
        self.trade_store.initialize()
        self.market_data_service = StubMarketDataService()
        self.apollo_service = StubApolloService()
        self.options_chain_service = StubOptionsChainService()
        self.pushover_service = StubPushoverService()
        self.kairos_service = StubKairosService()
        self.now = datetime(2026, 4, 10, 12, 0, tzinfo=ZoneInfo("America/Chicago"))
        self.manager = OpenTradeManager(
            trade_store=self.trade_store,
            market_data_service=self.market_data_service,
            apollo_service=self.apollo_service,
            options_chain_service=self.options_chain_service,
            pushover_service=self.pushover_service,
            kairos_service=self.kairos_service,
            now_provider=lambda: self.now,
        )
        self.manager.initialize()
        self._seed_trades()

    def tearDown(self):
        self.temp_dir.cleanup()

    def _seed_trades(self):
        self.apollo_trade_id = self.trade_store.create_trade(
            {
                "trade_mode": "real",
                "system_name": "Apollo",
                "candidate_profile": "Standard",
                "system_version": "4.3",
                "status": "open",
                "trade_date": "2026-04-10",
                "entry_datetime": "2026-04-10T09:35",
                "expiration_date": "2026-04-10",
                "underlying_symbol": "SPX",
                "spx_at_entry": 6800.0,
                "vix_at_entry": 18.0,
                "structure_grade": "Good",
                "macro_grade": "None",
                "expected_move": 40.0,
                "expected_move_used": 40.0,
                "option_type": "Put Credit Spread",
                "short_strike": 6750.0,
                "long_strike": 6740.0,
                "spread_width": 10.0,
                "contracts": 2,
                "actual_entry_credit": 1.8,
                "distance_to_short": 50.0,
                "actual_distance_to_short": 50.0,
                "actual_em_multiple": 1.25,
                "fallback_used": "no",
                "fallback_rule_name": "",
                "notes_entry": "Apollo rationale",
            }
        )
        self.kairos_trade_id = self.trade_store.create_trade(
            {
                "trade_mode": "real",
                "system_name": "Kairos",
                "candidate_profile": "Prime",
                "pass_type": "Strict Pass",
                "system_version": "4.3",
                "status": "open",
                "trade_date": "2026-04-10",
                "entry_datetime": "2026-04-10T10:15",
                "expiration_date": "2026-04-10",
                "underlying_symbol": "SPX",
                "spx_at_entry": 6792.0,
                "vix_at_entry": 18.0,
                "structure_grade": "Bullish Confirmation",
                "macro_grade": "Improving",
                "expected_move": 35.0,
                "expected_move_used": 35.0,
                "option_type": "Put Credit Spread",
                "short_strike": 6735.0,
                "long_strike": 6725.0,
                "spread_width": 10.0,
                "contracts": 1,
                "actual_entry_credit": 2.0,
                "distance_to_short": 57.0,
                "actual_distance_to_short": 57.0,
                "actual_em_multiple": 1.1,
                "fallback_used": "no",
                "fallback_rule_name": "",
                "notes_entry": "Kairos rationale",
            }
        )
        self.simulated_apollo_trade_id = self.trade_store.create_trade(
            {
                "trade_mode": "simulated",
                "system_name": "Apollo",
                "candidate_profile": "Fortress",
                "system_version": "4.3",
                "status": "open",
                "trade_date": "2026-04-10",
                "entry_datetime": "2026-04-10T11:00",
                "expiration_date": "2026-04-10",
                "underlying_symbol": "SPX",
                "spx_at_entry": 6810.0,
                "vix_at_entry": 18.5,
                "structure_grade": "Good",
                "macro_grade": "Minor",
                "expected_move": 32.0,
                "expected_move_used": 32.0,
                "option_type": "Put Credit Spread",
                "short_strike": 6740.0,
                "long_strike": 6730.0,
                "spread_width": 10.0,
                "contracts": 1,
                "actual_entry_credit": 1.4,
                "distance_to_short": 70.0,
                "actual_distance_to_short": 70.0,
                "actual_em_multiple": 2.19,
                "fallback_used": "no",
                "fallback_rule_name": "",
                "notes_entry": "Simulated review trade",
            }
        )
        self.talos_apollo_trade_id = self.trade_store.create_trade(
            {
                "trade_mode": "talos",
                "system_name": "Apollo",
                "candidate_profile": "Standard",
                "system_version": "7.0",
                "status": "open",
                "trade_date": "2026-04-10",
                "entry_datetime": "2026-04-10T11:15",
                "expiration_date": "2026-04-10",
                "underlying_symbol": "SPX",
                "spx_at_entry": 6812.0,
                "vix_at_entry": 18.4,
                "structure_grade": "Good",
                "macro_grade": "None",
                "expected_move": 32.0,
                "expected_move_used": 32.0,
                "option_type": "Put Credit Spread",
                "short_strike": 6745.0,
                "long_strike": 6735.0,
                "spread_width": 10.0,
                "contracts": 1,
                "actual_entry_credit": 1.3,
                "distance_to_short": 67.0,
                "actual_distance_to_short": 67.0,
                "actual_em_multiple": 2.09,
                "fallback_used": "no",
                "fallback_rule_name": "",
                "notes_entry": "Talos autonomous trade",
            }
        )

    def test_simulated_trades_still_appear_in_management_list(self):
        payload = self.manager.evaluate_open_trades(send_alerts=False)

        self.assertEqual(payload["open_trade_count"], 3)
        trade_modes = {item["trade_number"]: item["trade_mode"] for item in payload["records"]}
        self.assertIn("Simulated", trade_modes.values())
        self.assertNotIn("Talos", trade_modes.values())

    def test_plain_board_uses_minimal_kairos_management_context(self):
        def fail_dashboard_call():
            raise AssertionError("Plain Manage Trades board should not build the full Kairos dashboard payload.")

        self.kairos_service.get_dashboard_payload = fail_dashboard_call

        payload = self.manager.evaluate_open_trades(send_alerts=False)
        kairos_trade = next(item for item in payload["records"] if item["system_name"] == "Kairos")

        self.assertEqual(kairos_trade["current_structure_grade"], "Weakening")

    def test_simulated_trades_do_not_participate_in_alerts(self):
        self._set_runtime_field("last_morning_snapshot_date", "2026-04-10")
        self.manager.evaluate_open_trades(send_alerts=True)
        self.market_data_service.current_spx = 6754.0

        self.manager.evaluate_open_trades(send_alerts=True)

        joined_messages = "\n\n".join(item["message"] for item in self.pushover_service.sent)
        self.assertNotIn("Simulated", joined_messages)
        self.assertNotIn("Talos", joined_messages)

    def _set_runtime_field(self, column_name, value):
        with closing(sqlite3.connect(self.database_path)) as connection:
            connection.execute(
                f"UPDATE open_trade_management_runtime_settings SET {column_name} = ? WHERE singleton_id = 1",
                (value,),
            )
            connection.commit()

    def _create_closed_real_trade(self):
        trade_id = self.trade_store.create_trade(
            {
                "trade_mode": "real",
                "system_name": "Apollo",
                "candidate_profile": "Fortress",
                "system_version": "4.3",
                "status": "open",
                "trade_date": "2026-04-10",
                "entry_datetime": "2026-04-10T09:40",
                "expiration_date": "2026-04-10",
                "underlying_symbol": "SPX",
                "spx_at_entry": 6805.0,
                "vix_at_entry": 18.0,
                "structure_grade": "Good",
                "macro_grade": "None",
                "expected_move": 30.0,
                "expected_move_used": 30.0,
                "option_type": "Put Credit Spread",
                "short_strike": 6745.0,
                "long_strike": 6735.0,
                "spread_width": 10.0,
                "contracts": 1,
                "actual_entry_credit": 1.0,
                "distance_to_short": 60.0,
                "actual_distance_to_short": 60.0,
            }
        )
        self.trade_store.expire_trade(
            trade_id,
            {
                "event_datetime": "2026-04-10T15:05",
                "actual_exit_value": 0.0,
                "close_reason": "Expired Worthless",
            },
        )

    def test_morning_snapshot_sends_once_per_day(self):
        self.now = datetime(2026, 4, 10, 8, 32, tzinfo=ZoneInfo("America/Chicago"))

        first_payload = self.manager.evaluate_open_trades(send_alerts=True)
        second_payload = self.manager.evaluate_open_trades(send_alerts=True)

        self.assertEqual(first_payload["alerts_sent"], 1)
        self.assertEqual(second_payload["alerts_sent"], 0)
        self.assertEqual(len(self.pushover_service.sent), 1)
        self.assertIn("DELPHI — OPEN POSITIONS", self.pushover_service.sent[0]["title"])
        self.assertIn("Apollo | Standard | Watch", self.pushover_service.sent[0]["message"])
        self.assertIn("Kairos | Strict Pass | Healthy", self.pushover_service.sent[0]["message"])
        self.assertNotIn("Simulated", self.pushover_service.sent[0]["message"])
        self.assertNotIn("Talos", self.pushover_service.sent[0]["message"])

    def test_manual_real_status_update_matches_open_positions_snapshot_format(self):
        result = self.manager.send_manual_status_update(trade_mode="real")

        self.assertTrue(result["sent"])
        self.assertEqual(result["record_count"], 2)
        self.assertEqual(len(self.pushover_service.sent), 1)
        self.assertEqual(self.pushover_service.sent[0]["title"], "🔴 CRITICAL DELPHI — OPEN POSITIONS")
        self.assertIn("Apollo | Standard | Watch", self.pushover_service.sent[0]["message"])
        self.assertIn("Kairos | Strict Pass | Healthy", self.pushover_service.sent[0]["message"])
        self.assertNotIn("Fortress", self.pushover_service.sent[0]["message"])

    def test_manual_simulated_status_update_is_suppressed(self):
        result = self.manager.send_manual_status_update(trade_mode="simulated")

        self.assertFalse(result["sent"])
        self.assertEqual(result["record_count"], 0)
        self.assertEqual(len(self.pushover_service.sent), 0)

    def test_manual_status_update_can_send_when_notifications_are_off(self):
        self.manager.set_notifications_enabled(False)

        result = self.manager.send_manual_status_update(trade_mode="real")

        self.assertTrue(result["sent"])
        self.assertEqual(len(self.pushover_service.sent), 1)

    def test_no_duplicate_alerts_without_state_change(self):
        self._set_runtime_field("last_morning_snapshot_date", "2026-04-10")

        baseline_payload = self.manager.evaluate_open_trades(send_alerts=True)
        self.market_data_service.current_spx = 6754.0
        first_change_payload = self.manager.evaluate_open_trades(send_alerts=True)
        second_change_payload = self.manager.evaluate_open_trades(send_alerts=True)

        self.assertEqual(baseline_payload["alerts_sent"], 0)
        self.assertGreater(first_change_payload["alerts_sent"], 0)
        self.assertEqual(second_change_payload["alerts_sent"], 0)
        self.assertEqual(len(self.pushover_service.sent), first_change_payload["alerts_sent"])

    def test_action_alerts_trigger_with_decision_ready_fields(self):
        self._set_runtime_field("last_morning_snapshot_date", "2026-04-10")
        self.manager.evaluate_open_trades(send_alerts=True)
        self.market_data_service.current_spx = 6754.0

        payload = self.manager.evaluate_open_trades(send_alerts=True)
        action_alerts = [item for item in self.pushover_service.sent if "ACTION REQUIRED" in item["title"]]

        self.assertGreater(payload["alerts_sent"], 0)
        self.assertTrue(action_alerts)
        self.assertIn("Action: Close", action_alerts[-1]["message"])
        self.assertIn("P/L Now:", action_alerts[-1]["message"])
        self.assertIn("After Close:", action_alerts[-1]["message"])
        self.assertIn("Remaining Risk:", action_alerts[-1]["message"])

    def test_end_of_day_summary_sends_once_per_day(self):
        self._create_closed_real_trade()
        self._set_runtime_field("last_morning_snapshot_date", "2026-04-10")
        self.now = datetime(2026, 4, 10, 15, 10, tzinfo=ZoneInfo("America/Chicago"))

        first_payload = self.manager.evaluate_open_trades(send_alerts=True)
        second_payload = self.manager.evaluate_open_trades(send_alerts=True)
        eod_alerts = [item for item in self.pushover_service.sent if "EOD SUMMARY" in item["title"]]

        self.assertEqual(first_payload["alerts_sent"], 1)
        self.assertEqual(second_payload["alerts_sent"], 0)
        self.assertEqual(len(eod_alerts), 1)
        self.assertIn("Closed Today:", eod_alerts[0]["message"])
        self.assertIn("Open (Next Day Risk):", eod_alerts[0]["message"])

    def test_end_of_day_summary_is_suppressed_if_matching_alert_was_already_logged(self):
        self._create_closed_real_trade()
        self._set_runtime_field("last_morning_snapshot_date", "2026-04-10")
        self.now = datetime(2026, 4, 10, 15, 10, tzinfo=ZoneInfo("America/Chicago"))
        first_payload = self.manager.evaluate_open_trades(send_alerts=False)
        closed_trade = max(
            (
                trade
                for trade in self.trade_store.list_trades("real")
                if str(trade.get("status") or "").strip().lower() in {"closed", "expired"}
            ),
            key=lambda trade: int(trade.get("id") or 0),
        )
        expected_lines = [
            "Closed Today:",
            f"{closed_trade['system_name']} #{closed_trade['trade_number']} → ${int(closed_trade['gross_pnl'])} ({closed_trade.get('win_loss_result') or 'Closed'})",
            "",
            "Open (Next Day Risk):",
        ]
        for record in first_payload["records"]:
            if str(record.get("trade_mode") or "").strip().lower() != "real":
                continue
            expected_lines.append(f"{record['system_name']} | {record['profile_label']}")
            expected_lines.append(f"Dist: {record['distance_to_short_display']} | EM: {record['current_live_expected_move_display']}")
        expected_message = "\n".join(expected_lines)
        with closing(sqlite3.connect(self.database_path)) as connection:
            connection.execute(
                """
                INSERT INTO open_trade_management_alert_log (
                    trade_id, system_name, trade_mode, alert_type, alert_priority, alert_priority_label,
                    reason_code, title, body, sent_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (0, "Delphi", "real", "eod-summary", 0, "Normal", "eod-summary", "DELPHI — EOD SUMMARY", expected_message, self.now.isoformat()),
            )
            connection.commit()

        second_payload = self.manager.evaluate_open_trades(send_alerts=True)

        self.assertEqual(first_payload["open_trade_count"], 3)
        self.assertEqual(second_payload["alerts_sent"], 0)
        self.assertEqual(len(self.pushover_service.sent), 0)

    def test_alerts_are_suppressed_when_notifications_are_off(self):
        self.now = datetime(2026, 4, 10, 8, 32, tzinfo=ZoneInfo("America/Chicago"))
        self.manager.set_notifications_enabled(False)

        payload = self.manager.evaluate_open_trades(send_alerts=True)

        self.assertFalse(payload["notifications_enabled"])
        self.assertEqual(payload["alerts_sent"], 0)
        self.assertEqual(len(self.pushover_service.sent), 0)

    def test_critical_alerts_escalate_to_high_priority(self):
        self._set_runtime_field("last_morning_snapshot_date", "2026-04-10")
        self.manager.evaluate_open_trades(send_alerts=True)
        self.market_data_service.current_spx = 6748.0

        self.manager.evaluate_open_trades(send_alerts=True)
        action_alerts = [item for item in self.pushover_service.sent if "ACTION REQUIRED" in item["title"]]

        self.assertTrue(action_alerts)
        self.assertEqual(action_alerts[-1]["priority"], 1)
        self.assertTrue(action_alerts[-1]["title"].startswith("🔴 CRITICAL"))

    def test_pl_calculations_match_current_journal_math(self):
        self.market_data_service.current_spx = 6748.0

        payload = self.manager.evaluate_open_trades(send_alerts=False)
        trade = next(
            item
            for item in payload["records"]
            if item["system_name"] == "Apollo" and item["trade_mode"] == "Real"
        )

        expected_current_pl = round((1.8 - 3.9) * 2 * 100, 2)
        expected_pl_after_close = round((1.8 - 3.9) * 2 * 100, 2)
        expected_remaining_risk = round(1640.0 - expected_current_pl, 2)

        self.assertAlmostEqual(trade["current_pl"], expected_current_pl)
        self.assertAlmostEqual(trade["pl_after_close"], expected_pl_after_close)
        self.assertAlmostEqual(trade["remaining_risk"], expected_remaining_risk)

    def test_remaining_pl_uses_close_history_and_next_trigger_uses_price_ladder(self):
        self.trade_store.reduce_trade(
            self.apollo_trade_id,
            {
                "contracts_closed": 1,
                "actual_exit_value": 0.55,
                "event_datetime": "2026-04-10T10:45",
                "close_reason": "Trimmed first ladder step",
            },
        )
        self.market_data_service.current_spx = 6764.0

        payload = self.manager.evaluate_open_trades(send_alerts=False)
        trade = next(item for item in payload["records"] if int(item["trade_id"]) == int(self.apollo_trade_id))

        self.assertEqual(trade["contracts"], 1)
        self.assertEqual(trade["closed_contracts"], 1)
        self.assertAlmostEqual(trade["realized_close_cost"], 55.0)
        self.assertAlmostEqual(trade["current_total_close_cost"], 390.0)
        self.assertAlmostEqual(trade["unrealized_pnl"], 305.0)
        self.assertIn("close 1 contracts", trade["next_trigger"])
        self.assertIn("short-strike breach", trade["next_trigger"])

    def test_management_payload_excludes_performance_breakdown(self):
        payload = self.manager.evaluate_open_trades(send_alerts=False)

        self.assertNotIn("performance", payload)


if __name__ == "__main__":
    unittest.main()
