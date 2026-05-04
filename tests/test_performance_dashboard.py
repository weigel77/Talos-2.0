import sqlite3
import tempfile
import unittest
from contextlib import closing
from datetime import date, timedelta
from pathlib import Path

from app import create_app
from services.performance_dashboard_service import (
    build_dashboard_payload,
    build_metric_payload,
    build_performance_record,
    calculate_expectancy,
    classify_vix_bucket,
    classify_trade_result,
    compute_risk_efficiency,
    derive_entry_credit,
    summarize_outcomes,
)


class PerformanceDashboardTest(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database_path = Path(self.temp_dir.name) / "horme_trades.db"
        self.app = create_app({"TESTING": True, "TRADE_DATABASE": str(self.database_path)})
        self.client = self.app.test_client()
        self._seed_dashboard_trades()

    def tearDown(self):
        self.temp_dir.cleanup()

    def _create_trade(self, trade_mode: str, **overrides):
        payload = {
            "trade_number": overrides.pop("trade_number", ""),
            "trade_mode": trade_mode,
            "system_name": overrides.pop("system_name", "Apollo"),
            "journal_name": overrides.pop("journal_name", "Apollo Main"),
            "system_version": overrides.pop("system_version", "2.0"),
            "candidate_profile": overrides.pop("candidate_profile", "Legacy"),
            "status": overrides.pop("status", "closed"),
            "trade_date": overrides.pop("trade_date", "2026-04-01"),
            "entry_datetime": overrides.pop("entry_datetime", ""),
            "expiration_date": overrides.pop("expiration_date", "2026-04-04"),
            "underlying_symbol": overrides.pop("underlying_symbol", "SPX"),
            "spx_at_entry": overrides.pop("spx_at_entry", ""),
            "vix_at_entry": overrides.pop("vix_at_entry", ""),
            "structure_grade": overrides.pop("structure_grade", "Good"),
            "macro_grade": overrides.pop("macro_grade", "None"),
            "expected_move": overrides.pop("expected_move", ""),
            "option_type": overrides.pop("option_type", "Put Credit Spread"),
            "short_strike": overrides.pop("short_strike", "6450"),
            "long_strike": overrides.pop("long_strike", "6445"),
            "spread_width": overrides.pop("spread_width", "5"),
            "contracts": overrides.pop("contracts", "1"),
            "candidate_credit_estimate": overrides.pop("candidate_credit_estimate", ""),
            "actual_entry_credit": overrides.pop("actual_entry_credit", "1.5"),
            "distance_to_short": overrides.pop("distance_to_short", ""),
            "short_delta": overrides.pop("short_delta", ""),
            "notes_entry": overrides.pop("notes_entry", ""),
            "prefill_source": overrides.pop("prefill_source", ""),
            "exit_datetime": overrides.pop("exit_datetime", ""),
            "spx_at_exit": overrides.pop("spx_at_exit", ""),
            "actual_exit_value": overrides.pop("actual_exit_value", "0"),
            "close_method": overrides.pop("close_method", ""),
            "close_reason": overrides.pop("close_reason", ""),
            "notes_exit": overrides.pop("notes_exit", ""),
        }
        payload.update(overrides)
        response = self.client.post(f"/trades/{trade_mode}/new", data=payload, follow_redirects=True)
        self.assertEqual(response.status_code, 200)

    def _seed_dashboard_trades(self):
        self._create_trade(
            "real",
            system_name="Apollo",
            candidate_profile="Standard",
            trade_date="2026-04-01",
            expiration_date="2026-04-01",
            spx_at_entry="6500",
            vix_at_entry="17.5",
            macro_grade="None",
            structure_grade="Good",
            expected_move="25",
            actual_entry_credit="2.0",
            actual_exit_value="0.5",
        )
        self._create_trade(
            "real",
            system_name="Kairos",
            candidate_profile="",
            trade_date="2026-04-02",
            expiration_date="2026-04-02",
            spx_at_entry="6500",
            vix_at_entry="22.5",
            macro_grade="Major",
            structure_grade="Poor",
            expected_move="20",
            spread_width="10",
            actual_entry_credit="1.0",
            actual_exit_value="5.0",
            close_reason="Black Swan defense failure",
        )
        self._create_trade(
            "simulated",
            system_name="Apollo",
            candidate_profile="Fortress",
            trade_date="2026-04-03",
            expiration_date="2026-04-03",
            spx_at_entry="6500",
            vix_at_entry="20",
            macro_grade="Minor",
            structure_grade="Neutral",
            expected_move="30",
            actual_entry_credit="1.0",
            actual_exit_value="1.5",
        )
        self._create_trade(
            "simulated",
            system_name="Apollo",
            candidate_profile="Aggressive",
            status="open",
            trade_date="2026-04-04",
            expiration_date="2026-04-04",
            macro_grade="None",
            structure_grade="Good",
            actual_entry_credit="1.25",
            actual_exit_value="",
            close_reason="",
        )
        self._create_trade(
            "simulated",
            system_name="Kairos",
            candidate_profile="Prime",
            trade_date="2026-04-04",
            expiration_date="2026-04-04",
            spx_at_entry="6500",
            vix_at_entry="19",
            macro_grade="None",
            structure_grade="Good",
            expected_move="28",
            actual_entry_credit="1.6",
            actual_exit_value="0.4",
        )

    def test_timeframe_filters_use_expiration_date_instead_of_trade_date(self):
        today = date.today()
        older_than_year = today - timedelta(days=500)
        within_year = today - timedelta(days=30)

        payload = build_dashboard_payload(
            [
                build_performance_record(
                    {
                        "id": 101,
                        "trade_number": 101,
                        "trade_mode": "real",
                        "system_name": "Apollo",
                        "candidate_profile": "Standard",
                        "status": "closed",
                        "trade_date": today.isoformat(),
                        "expiration_date": older_than_year.isoformat(),
                        "gross_pnl": 150.0,
                        "actual_entry_credit": 1.5,
                        "spread_width": 5.0,
                        "contracts": 1,
                    }
                ),
                build_performance_record(
                    {
                        "id": 102,
                        "trade_number": 102,
                        "trade_mode": "real",
                        "system_name": "Apollo",
                        "candidate_profile": "Standard",
                        "status": "closed",
                        "trade_date": older_than_year.isoformat(),
                        "expiration_date": within_year.isoformat(),
                        "gross_pnl": 90.0,
                        "actual_entry_credit": 1.25,
                        "spread_width": 5.0,
                        "contracts": 1,
                    }
                ),
            ],
            filters={
                "system": ["apollo", "kairos", "aegis"],
                "profile": ["legacy", "aggressive", "fortress", "standard", "prime", "subprime"],
                "result": ["win", "loss", "black-swan", "scratched"],
                "trade_mode": ["real"],
                "macro_grade": ["none", "minor", "major"],
                "structure_grade": ["good", "neutral", "poor"],
                "timeframe": ["1-yr"],
            },
        )

        self.assertEqual(payload["records_total"], 2)
        self.assertEqual(payload["records_filtered"], 1)
        self.assertEqual(len(payload["charts"]["equity_curve"]["points"]), 1)
        self.assertEqual(payload["charts"]["equity_curve"]["points"][0]["label"], within_year.isoformat())

    def test_performance_page_route_loads_and_renders_chart_containers(self):
        response = self.client.get("/performance")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Performance", response.data)
        self.assertIn(b"id=\"win-rate-gauge\"", response.data)
        self.assertIn(b"id=\"equity-curve-chart\"", response.data)
        self.assertIn(b"id=\"outcome-composition-chart\"", response.data)
        self.assertIn(b"data-performance-url=\"/performance/data\"", response.data)
        self.assertIn(b"System", response.data)
        self.assertIn(b"Trade Mode", response.data)
        self.assertIn(b"Simulated", response.data)
        self.assertNotIn(b"Talos", response.data)
        self.assertIn(b"performance-line-popup", response.data)
        self.assertIn(b"Performance Learning Metrics", response.data)
        self.assertIn(b"id=\"credit-efficiency-system-panel\"", response.data)
        self.assertIn(b"id=\"credit-efficiency-profile-panel\"", response.data)
        self.assertIn(b"id=\"credit-efficiency-vix-panel\"", response.data)
        self.assertIn(b"EM Performance by Profile", response.data)
        self.assertIn(b"id=\"profile-em-performance-panel\"", response.data)
        self.assertIn(b"Distance Efficiency Score", response.data)
        self.assertIn(b"id=\"distance-efficiency-panel\"", response.data)
        self.assertIn(b"Time of Entry Edge", response.data)
        self.assertIn(b"id=\"time-of-entry-edge-panel\"", response.data)
        self.assertIn(b"Exit Efficiency", response.data)
        self.assertIn(b"id=\"exit-efficiency-panel\"", response.data)
        self.assertIn(b"VIX Regime Performance", response.data)
        self.assertIn(b"id=\"vix-regime-performance-panel\"", response.data)
        self.assertIn(b"Expected Move Deviation / Breach Analysis", response.data)
        self.assertIn(b"id=\"expected-move-breach-panel\"", response.data)
        self.assertIn(b"Safety Ratio Curve Summary", response.data)
        self.assertIn(b"id=\"safety-ratio-expectancy-chart\"", response.data)
        self.assertIn(b"id=\"safety-ratio-bucket-summary\"", response.data)
        self.assertIn(b"id=\"safety-ratio-summary-details\"", response.data)
        self.assertIn(b"Learning Audit", response.data)
        self.assertIn(b"id=\"learning-audit-details\"", response.data)
        self.assertIn(b"id=\"learning-audit-panel\"", response.data)
        self.assertNotIn(b"Live vs Simulated", response.data)
        self.assertNotIn(b"Adaptive Safety Guidance", response.data)
        self.assertNotIn(b"EM Policy Guidance", response.data)
        self.assertNotIn(b"Best EM Range by VIX", response.data)
        self.assertNotIn(b"Fallback Impact", response.data)
        self.assertNotIn(b"Learning Status", response.data)
        self.assertNotIn(b"Result Classification Audit", response.data)
        self.assertNotIn(b"id=\"trade-mode-learning-grid\"", response.data)
        self.assertNotIn(b"id=\"adaptive-safety-guidance\"", response.data)
        self.assertNotIn(b"id=\"em-policy-guidance\"", response.data)
        self.assertNotIn(b"id=\"em-policy-vix-guidance\"", response.data)
        self.assertNotIn(b"id=\"em-policy-fallback-impact\"", response.data)
        self.assertNotIn(b"id=\"em-policy-learning-status\"", response.data)
        self.assertNotIn(b"id=\"result-classification-audit\"", response.data)
        self.assertNotIn(b"id=\"safety-ratio-summary-details\" open", response.data)
        self.assertNotIn(b"id=\"learning-audit-details\" open", response.data)
        self.assertIn(b"Premium collected divided by maximum theoretical risk at entry.", response.data)
        self.assertNotIn(b"performance-line-tooltip", response.data)
        self.assertNotIn(b"mouseenter", response.data)
        self.assertNotIn(b"mouseleave", response.data)
        self.assertIn(b'data-filter-group="trade_mode" value="real" checked', response.data)
        self.assertIn(b'data-filter-group="trade_mode" value="simulated"', response.data)
        self.assertIn(b'value="custom"', response.data)
        self.assertIn(b'id="performance-expiration-start"', response.data)
        self.assertIn(b'id="performance-expiration-end"', response.data)
        self.assertIn(b'id="performance-custom-apply"', response.data)

    def test_custom_timeframe_filters_by_expiration_date_range(self):
        payload = build_dashboard_payload(
            [
                build_performance_record(
                    {
                        "id": 201,
                        "trade_number": 201,
                        "trade_mode": "real",
                        "system_name": "Apollo",
                        "candidate_profile": "Standard",
                        "status": "closed",
                        "trade_date": "2026-04-20",
                        "expiration_date": "2026-04-21",
                        "gross_pnl": 120.0,
                        "actual_entry_credit": 1.5,
                        "spread_width": 5.0,
                        "contracts": 1,
                    }
                ),
                build_performance_record(
                    {
                        "id": 202,
                        "trade_number": 202,
                        "trade_mode": "real",
                        "system_name": "Apollo",
                        "candidate_profile": "Standard",
                        "status": "closed",
                        "trade_date": "2026-04-27",
                        "expiration_date": "2026-04-28",
                        "gross_pnl": 80.0,
                        "actual_entry_credit": 1.25,
                        "spread_width": 5.0,
                        "contracts": 1,
                    }
                ),
            ],
            filters={
                "system": ["apollo", "kairos", "aegis"],
                "profile": ["legacy", "aggressive", "fortress", "standard", "prime", "subprime"],
                "result": ["win", "loss", "black-swan", "scratched"],
                "trade_mode": ["real"],
                "macro_grade": ["none", "minor", "major"],
                "structure_grade": ["good", "neutral", "poor"],
                "timeframe": ["custom"],
                "expiration_start": ["2026-04-22"],
                "expiration_end": ["2026-04-29"],
            },
        )

        self.assertEqual(payload["records_total"], 2)
        self.assertEqual(payload["records_filtered"], 1)
        self.assertEqual(payload["charts"]["equity_curve"]["points"][0]["label"], "2026-04-28")

    def test_performance_data_route_returns_expected_metrics_and_filters(self):
        response = self.client.get("/performance/data")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["records_total"], 5)
        self.assertEqual(payload["records_filtered"], 2)
        self.assertEqual(payload["filters"]["trade_mode"], ["real"])
        self.assertEqual(payload["metrics"]["totals"]["open_trades"], 0)
        self.assertEqual(payload["metrics"]["totals"]["closed_trades"], 2)
        self.assertEqual(payload["metrics"]["totals"]["wins"], 1)
        self.assertEqual(payload["metrics"]["totals"]["losses"], 0)
        self.assertEqual(payload["metrics"]["totals"]["loss_outcomes"], 1)
        self.assertEqual(payload["metrics"]["totals"]["black_swan_count"], 1)
        self.assertEqual(payload["metrics"]["win_rate"]["wins"], 1)
        self.assertEqual(payload["metrics"]["win_rate"]["losses"], 1)
        self.assertEqual(payload["metrics"]["win_rate"]["closed_outcomes"], 2)
        self.assertAlmostEqual(payload["metrics"]["win_rate"]["value"], 50.0, places=2)
        self.assertAlmostEqual(payload["metrics"]["net_pnl"]["value"], -250.0)
        self.assertAlmostEqual(payload["metrics"]["outcome_mix"]["win_roi"], 50.0, places=2)
        self.assertAlmostEqual(payload["metrics"]["outcome_mix"]["loss_roi"], 0.0, places=2)
        self.assertAlmostEqual(payload["metrics"]["outcome_mix"]["black_swan_roi"], 44.44, places=2)
        self.assertAlmostEqual(payload["metrics"]["average_win_loss"]["average_win"], 150.0)
        self.assertAlmostEqual(payload["metrics"]["average_win_loss"]["average_loss"], 0.0)
        self.assertAlmostEqual(payload["metrics"]["average_win_loss"]["average_black_swan_loss"], 400.0)
        self.assertAlmostEqual(payload["metrics"]["expectancy"]["value"], -125.0)
        self.assertEqual(payload["metrics"]["totals"]["scratched_count"], 0)
        self.assertEqual(payload["charts"]["outcome_composition"]["total"], 2)
        self.assertEqual(len(payload["charts"]["equity_curve"]["points"]), 2)
        self.assertEqual(payload["charts"]["equity_curve"]["points"][1]["result"], "Black Swan")
        self.assertAlmostEqual(payload["charts"]["equity_curve"]["points"][1]["value"], -400.0)
        self.assertAlmostEqual(payload["charts"]["profile_avg_pnl"]["items"][3]["value"], 150.0)
        self.assertEqual(payload["charts"]["profile_avg_pnl"]["items"][3]["trade_count"], 1)
        self.assertAlmostEqual(payload["charts"]["premium_em_by_profile"]["items"][0]["avg_premium_per_contract"], 100.0)
        self.assertAlmostEqual(payload["charts"]["premium_em_by_profile"]["items"][0]["avg_em_multiple"], 2.5)
        self.assertAlmostEqual(payload["charts"]["premium_em_by_profile"]["items"][0]["premium_per_em"], 5.0)
        self.assertAlmostEqual(payload["metrics"]["credit_efficiency"]["value"], 38.89, places=2)
        self.assertEqual(payload["metrics"]["credit_efficiency"]["qualified_trade_count"], 2)
        self.assertIn("learning", payload)
        self.assertAlmostEqual(payload["learning"]["overview"]["avg_safety_ratio"], 2.25)
        self.assertAlmostEqual(payload["learning"]["overview"]["avg_premium_per_em"], 6.5)
        self.assertAlmostEqual(payload["learning"]["overview"]["avg_credit_efficiency_pct"], 38.89, places=2)
        self.assertEqual(payload["learning"]["overview"]["qualified_credit_efficiency_trades"], 2)
        self.assertAlmostEqual(payload["charts"]["credit_efficiency_by_system"]["items"][0]["avg_credit_efficiency_pct"], 66.67, places=2)
        self.assertEqual(payload["charts"]["credit_efficiency_by_system"]["items"][0]["trade_count"], 1)
        self.assertAlmostEqual(payload["charts"]["credit_efficiency_by_system"]["items"][1]["avg_credit_efficiency_pct"], 11.11, places=2)
        self.assertEqual(payload["charts"]["credit_efficiency_by_system"]["items"][1]["trade_count"], 1)
        self.assertIn("safety_optimization", payload["learning"])
        self.assertIn("adaptive_safety_guidance", payload["learning"])
        self.assertIn("em_policy", payload["learning"])
        self.assertIn("apollo", payload["learning"]["em_policy"])
        self.assertIn("kairos", payload["learning"]["em_policy"])
        self.assertIn("profile_em_performance", payload["learning"])
        self.assertIn("distance_efficiency", payload["learning"])
        self.assertIn("time_of_entry_edge", payload["learning"])
        self.assertIn("exit_efficiency", payload["learning"])
        self.assertIn("vix_regime_performance", payload["learning"])
        self.assertIn("expected_move_breach_analysis", payload["learning"])
        self.assertGreaterEqual(len(payload["learning"]["data_limitations"]), 1)
        self.assertIn("safety_ratio_expectancy_curve", payload["charts"])

    def test_performance_data_route_respects_explicitly_empty_filter_group(self):
        response = self.client.get("/performance/data?profile__active=1")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["filters"]["profile"], [])
        self.assertEqual(payload["records_filtered"], 0)

    def test_profile_em_performance_uses_profile_specific_intervals(self):
        payload = build_dashboard_payload(
            [
                build_performance_record(
                    {
                        "id": 61,
                        "trade_number": 61,
                        "trade_mode": "real",
                        "system_name": "Apollo",
                        "candidate_profile": "Standard",
                        "status": "closed",
                        "trade_date": "2026-04-08",
                        "gross_pnl": 120.0,
                        "actual_em_multiple": 1.9,
                        "vix_at_entry": 20.0,
                        "actual_entry_credit": 1.4,
                        "spread_width": 5.0,
                        "contracts": 1,
                    }
                ),
                build_performance_record(
                    {
                        "id": 62,
                        "trade_number": 62,
                        "trade_mode": "real",
                        "system_name": "Apollo",
                        "candidate_profile": "Standard",
                        "status": "closed",
                        "trade_date": "2026-04-09",
                        "gross_pnl": 100.0,
                        "actual_em_multiple": 1.92,
                        "vix_at_entry": 21.0,
                        "actual_entry_credit": 1.5,
                        "spread_width": 5.0,
                        "contracts": 1,
                    }
                ),
                build_performance_record(
                    {
                        "id": 63,
                        "trade_number": 63,
                        "trade_mode": "real",
                        "system_name": "Apollo",
                        "candidate_profile": "Fortress",
                        "status": "closed",
                        "trade_date": "2026-04-10",
                        "gross_pnl": 140.0,
                        "actual_em_multiple": 2.25,
                        "vix_at_entry": 22.0,
                        "actual_entry_credit": 1.3,
                        "spread_width": 5.0,
                        "contracts": 1,
                    }
                ),
                build_performance_record(
                    {
                        "id": 64,
                        "trade_number": 64,
                        "trade_mode": "real",
                        "system_name": "Apollo",
                        "candidate_profile": "Fortress",
                        "status": "closed",
                        "trade_date": "2026-04-11",
                        "gross_pnl": 150.0,
                        "actual_em_multiple": 2.28,
                        "vix_at_entry": 23.0,
                        "actual_entry_credit": 1.35,
                        "spread_width": 5.0,
                        "contracts": 1,
                    }
                ),
            ],
            filters={
                "trade_mode": ["real"],
                "system": ["apollo", "kairos", "aegis"],
                "profile": ["legacy", "aggressive", "fortress", "standard", "prime", "subprime"],
                "result": ["win", "loss", "black-swan", "scratched"],
                "macro_grade": ["none", "minor", "major"],
                "structure_grade": ["good", "neutral", "poor"],
            },
        )

        profile_em = {item["profile"]: item for item in payload["learning"]["profile_em_performance"]["profiles"]}

        self.assertIn("Standard", profile_em)
        self.assertIn("Fortress", profile_em)
        self.assertEqual(profile_em["Standard"]["qualified_trade_count"], 2)
        self.assertEqual(profile_em["Fortress"]["qualified_trade_count"], 2)
        self.assertNotEqual(profile_em["Standard"]["interval_range"], profile_em["Fortress"]["interval_range"])
        self.assertEqual(profile_em["Standard"]["buckets"][0]["trade_count"], 2)
        self.assertEqual(profile_em["Fortress"]["buckets"][0]["trade_count"], 2)

    def test_performance_filters_change_metric_outputs_correctly(self):
        response = self.client.get("/performance/data?system=apollo&result=win&result=loss&result=black-swan&trade_mode=real&trade_mode=simulated&profile=legacy&profile=aggressive&profile=fortress&profile=standard&macro_grade=none&macro_grade=minor&macro_grade=major&structure_grade=good&structure_grade=neutral&structure_grade=poor")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["records_filtered"], 3)
        self.assertEqual(payload["metrics"]["totals"]["black_swan_count"], 0)
        self.assertAlmostEqual(payload["metrics"]["net_pnl"]["value"], 100.0)
        self.assertNotIn("system_comparison", payload["charts"])
        self.assertAlmostEqual(payload["metrics"]["outcome_mix"]["win_roi"], 50.0, places=2)
        self.assertAlmostEqual(payload["metrics"]["outcome_mix"]["loss_roi"], 0.0, places=2)
        self.assertAlmostEqual(payload["metrics"]["outcome_mix"]["black_swan_roi"], 0.0)

    def test_dashboard_payload_handles_zero_result_categories(self):
        with self.app.app_context():
            store = self.app.extensions["trade_store"]
            records = [build_performance_record(item) for item in store.list_trades("real") + store.list_trades("simulated")]
        payload = build_dashboard_payload(records, filters={"system": ["kairos"], "profile": ["fortress"]})

        self.assertEqual(payload["records_filtered"], 0)
        self.assertEqual(payload["metrics"]["totals"]["total_trades"], 0)
        self.assertTrue(all(item["value"] == 0 for item in payload["charts"]["profile_avg_pnl"]["items"]))
        self.assertTrue(all(item["value"] == 0 for item in payload["charts"]["outcome_composition"]["items"]))

    def test_expectancy_and_black_swan_classification_helpers(self):
        expectancy = calculate_expectancy(win_rate=0.4, average_win=200.0, loss_rate=0.6, average_loss=90.0)
        self.assertAlmostEqual(expectancy, 26.0)

        record = classify_trade_result({
            "close_reason": "BLACK SWAN gap event",
            "win_loss_result": "Loss",
            "gross_pnl": -400,
            "max_theoretical_risk": 900,
        })
        self.assertEqual(record, "Black Swan")

        regular_loss = classify_trade_result({"close_reason": "Stop", "win_loss_result": "Loss", "gross_pnl": -25, "max_theoretical_risk": 900})
        self.assertEqual(regular_loss, "Loss")

        explicit_black_swan = classify_trade_result({"close_reason": "", "win_loss_result": "Black Swan", "gross_pnl": -400, "max_theoretical_risk": 900})
        self.assertEqual(explicit_black_swan, "Black Swan")

        scratched_trade = classify_trade_result({"close_reason": "", "win_loss_result": "", "gross_pnl": 0, "status": "closed"})
        self.assertEqual(scratched_trade, "Scratched")

        reduced_trade = classify_trade_result({"derived_status_raw": "reduced", "close_reason": "", "win_loss_result": "Loss", "gross_pnl": -620})
        self.assertEqual(reduced_trade, "")

        self.assertEqual(classify_vix_bucket(17.5), "<18")
        self.assertEqual(classify_vix_bucket(18.0), "18-22")
        self.assertEqual(classify_vix_bucket(22.5), "22-26")
        self.assertEqual(classify_vix_bucket(26.0), "26+")

        self.assertAlmostEqual(compute_risk_efficiency(total_premium=180.0, max_loss=1640.0), 0.109756, places=6)

    def test_risk_efficiency_prefers_actual_entry_credit_and_falls_back_to_candidate_estimate(self):
        actual_credit, actual_source = derive_entry_credit(
            {
                "actual_entry_credit": 1.8,
                "candidate_credit_estimate": 2.1,
            }
        )
        fallback_credit, fallback_source = derive_entry_credit(
            {
                "actual_entry_credit": "",
                "candidate_credit_estimate": 1.55,
            }
        )
        record = build_performance_record(
            {
                "id": 303,
                "trade_number": 22,
                "trade_mode": "real",
                "system_name": "Apollo",
                "candidate_profile": "Standard",
                "status": "closed",
                "trade_date": "2026-04-08",
                "gross_pnl": 110.0,
                "spread_width": 5.0,
                "contracts": 1,
                "candidate_credit_estimate": 1.55,
            }
        )

        self.assertEqual(actual_source, "per_contract_credit")
        self.assertAlmostEqual(actual_credit, 1.8)
        self.assertEqual(fallback_source, "per_contract_credit")
        self.assertAlmostEqual(fallback_credit, 1.55)
        self.assertEqual(record["entry_credit_source"], "per_contract_credit")
        self.assertAlmostEqual(record["risk_efficiency"], 0.449275, places=6)

    def test_performance_record_uses_shared_distance_resolution_with_stored_first_precedence(self):
        original_record = build_performance_record(
            {
                "id": 401,
                "trade_number": 31,
                "trade_mode": "real",
                "system_name": "Apollo",
                "candidate_profile": "Standard",
                "status": "closed",
                "trade_date": "2026-04-08",
                "gross_pnl": 100.0,
                "option_type": "Put Credit Spread",
                "spx_at_entry": 6500.0,
                "short_strike": 6450.0,
                "distance_to_short": 999.0,
                "expected_move": 25.0,
                "actual_entry_credit": 1.5,
                "spread_width": 5.0,
                "contracts": 1,
            }
        )
        derived_record = build_performance_record(
            {
                "id": 402,
                "trade_number": 32,
                "trade_mode": "real",
                "system_name": "Apollo",
                "candidate_profile": "Standard",
                "status": "closed",
                "trade_date": "2026-04-08",
                "gross_pnl": 100.0,
                "option_type": "Put Credit Spread",
                "spx_at_entry": 6500.0,
                "short_strike": 6450.0,
                "expected_move": 25.0,
                "actual_entry_credit": 1.5,
                "spread_width": 5.0,
                "contracts": 1,
            }
        )

        self.assertAlmostEqual(original_record["distance_to_short"], 999.0)
        self.assertEqual(original_record["distance_source"], "original")
        self.assertAlmostEqual(original_record["safety_ratio"], 39.96)
        self.assertAlmostEqual(original_record["credit_per_point"], 0.001502, places=6)
        self.assertAlmostEqual(derived_record["distance_to_short"], 50.0)
        self.assertEqual(derived_record["distance_source"], "derived")
        self.assertAlmostEqual(derived_record["safety_ratio"], 2.0)

    def test_performance_record_recovers_expected_move_from_kairos_entry_inputs(self):
        record = build_performance_record(
            {
                "id": 450,
                "trade_number": 33,
                "trade_mode": "real",
                "system_name": "Kairos",
                "candidate_profile": "Standard",
                "status": "closed",
                "trade_date": "2026-04-08",
                "gross_pnl": 100.0,
                "option_type": "Put Credit Spread",
                "spx_at_entry": 6400.0,
                "vix_at_entry": 20.0,
                "short_strike": 6320.0,
                "actual_entry_credit": 1.5,
                "spread_width": 5.0,
                "contracts": 1,
            }
        )

        self.assertAlmostEqual(record["expected_move"], 80.0)
        self.assertEqual(record["expected_move_source"], "recovered_candidate")
        self.assertAlmostEqual(record["distance_to_short"], 80.0)
        self.assertAlmostEqual(record["safety_ratio"], 1.0)

    def test_safety_optimization_reports_explicit_exclusion_reasons(self):
        payload = build_dashboard_payload(
            [
                build_performance_record(
                    {
                        "id": 901,
                        "trade_mode": "real",
                        "system_name": "Apollo",
                        "candidate_profile": "Standard",
                        "status": "closed",
                        "trade_date": "2026-04-08",
                        "gross_pnl": 100.0,
                        "distance_to_short": 45.0,
                        "distance_source": "original",
                        "expected_move": 30.0,
                        "actual_entry_credit": 1.5,
                        "spread_width": 5.0,
                        "contracts": 1,
                    }
                ),
                build_performance_record(
                    {
                        "id": 902,
                        "trade_mode": "real",
                        "system_name": "Apollo",
                        "candidate_profile": "Standard",
                        "status": "closed",
                        "trade_date": "2026-04-08",
                        "gross_pnl": -50.0,
                        "distance_source": "unresolved",
                        "expected_move": 25.0,
                        "actual_entry_credit": 1.5,
                        "spread_width": 5.0,
                        "contracts": 1,
                    }
                ),
                build_performance_record(
                    {
                        "id": 903,
                        "trade_mode": "real",
                        "system_name": "Apollo",
                        "candidate_profile": "Standard",
                        "status": "closed",
                        "trade_date": "2026-04-08",
                        "gross_pnl": -25.0,
                        "distance_to_short": 30.0,
                        "distance_source": "derived",
                        "actual_entry_credit": 1.5,
                        "spread_width": 5.0,
                        "contracts": 1,
                    }
                ),
            ],
            filters={
                "trade_mode": ["real"],
                "system": ["apollo", "kairos", "aegis"],
                "profile": ["legacy", "aggressive", "fortress", "standard", "prime", "subprime"],
                "result": ["win", "loss", "black-swan", "scratched"],
                "macro_grade": ["none", "minor", "major"],
                "structure_grade": ["good", "neutral", "poor"],
            },
        )

        safety = payload["learning"]["safety_optimization"]
        self.assertEqual(safety["eligible_trade_count"], 1)
        self.assertEqual(safety["excluded_trade_count"], 2)
        self.assertEqual(safety["exclusion_summary"]["distance_unresolved_count"], 1)
        self.assertEqual(safety["exclusion_summary"]["expected_move_missing_count"], 1)
        self.assertEqual(safety["exclusion_summary"]["distance_conflict_count"], 0)
        self.assertEqual(safety["exclusion_summary"]["expected_move_excluded_count"], 0)
        self.assertEqual(safety["expected_move_source_counts"]["original"], 2)
        self.assertEqual(safety["expected_move_source_counts"]["unresolved"], 1)

    def test_outcome_summary_is_shared_for_win_rate_expectancy_and_outcome_mix(self):
        with self.app.app_context():
            store = self.app.extensions["trade_store"]
            records = [build_performance_record(item) for item in store.list_trades("real") + store.list_trades("simulated")]

        outcomes = summarize_outcomes(records)
        metrics = build_metric_payload(records, outcomes=outcomes)
        payload = build_dashboard_payload(
            records,
            filters={
                "trade_mode": ["real", "simulated"],
                "result": ["win", "loss", "black-swan"],
                "system": ["apollo", "kairos"],
                "profile": ["legacy", "aggressive", "fortress", "standard", "prime"],
                "macro_grade": ["none", "minor", "major"],
                "structure_grade": ["good", "neutral", "poor"],
            },
        )

        self.assertEqual(len(outcomes.open_records), 1)
        self.assertEqual(len(outcomes.closed_records), 4)
        self.assertEqual(len(outcomes.wins), 2)
        self.assertEqual(len(outcomes.losses), 1)
        self.assertEqual(len(outcomes.black_swans), 1)
        self.assertEqual(len(outcomes.loss_events), 2)
        self.assertEqual(len(outcomes.closed_outcomes), 4)
        self.assertEqual(metrics["win_rate"]["wins"], 2)
        self.assertEqual(metrics["win_rate"]["losses"], 2)
        self.assertEqual(metrics["win_rate"]["closed_outcomes"], 4)
        self.assertAlmostEqual(metrics["expectancy"]["value"], -45.0)

        outcome_mix = {item["label"]: item["value"] for item in payload["charts"]["outcome_composition"]["items"]}
        self.assertEqual(outcome_mix["Win"], metrics["win_rate"]["wins"])
        self.assertEqual(outcome_mix["Loss"], len(outcomes.losses))
        self.assertEqual(outcome_mix["Black Swan"], len(outcomes.black_swans))
        self.assertEqual(payload["metrics"]["totals"]["loss_outcomes"], metrics["win_rate"]["losses"])
        self.assertAlmostEqual(payload["metrics"]["outcome_mix"]["win_roi"], 50.0, places=2)
        self.assertAlmostEqual(payload["metrics"]["outcome_mix"]["loss_roi"], 0.0, places=2)
        self.assertAlmostEqual(payload["metrics"]["outcome_mix"]["black_swan_roi"], 44.44, places=2)

    def test_chart_payloads_support_mixed_apollo_kairos_legacy_data(self):
        response = self.client.get(
            "/performance/data?trade_mode=real&trade_mode=simulated&result=win&result=loss&result=black-swan&system=apollo&system=kairos&profile=legacy&profile=aggressive&profile=fortress&profile=standard&macro_grade=none&macro_grade=minor&macro_grade=major&structure_grade=good&structure_grade=neutral&structure_grade=poor"
        )
        payload = response.get_json()

        profile_items = {item["label"]: item["value"] for item in payload["charts"]["profile_avg_pnl"]["items"]}
        macro_items = {item["label"]: item["value"] for item in payload["charts"]["macro_avg_pnl"]["items"]}
        structure_items = {item["label"]: item["value"] for item in payload["charts"]["structure_avg_pnl"]["items"]}

        self.assertEqual(profile_items["Legacy"], -400.0)
        self.assertEqual(profile_items["Standard"], 150.0)
        self.assertEqual(profile_items["Fortress"], -50.0)
        self.assertEqual(macro_items["Major"], -400.0)
        self.assertEqual(macro_items["Minor"], -50.0)
        self.assertEqual(macro_items["None"], 150.0)
        self.assertEqual(structure_items["Good"], 150.0)
        self.assertEqual(structure_items["Neutral"], -50.0)
        self.assertEqual(structure_items["Poor"], -400.0)

    def test_learning_metrics_are_added_to_existing_dashboard_payload(self):
        record = build_performance_record(
            {
                "id": 101,
                "trade_number": 12,
                "trade_mode": "real",
                "system_name": "Kairos",
                "candidate_profile": "Prime",
                "status": "closed",
                "trade_date": "2026-04-08",
                "gross_pnl": 250.0,
                "macro_grade": "Minor",
                "structure_grade": "Good",
                "close_reason": "",
                "distance_to_short": 45.0,
                "expected_move": 15.0,
                "actual_entry_credit": 1.8,
                "spread_width": 10.0,
                "contracts": 2,
                "vix_at_entry": 21.0,
            }
        )

        payload = build_dashboard_payload(
            [record],
            filters={
                "trade_mode": ["real"],
                "system": ["kairos"],
                "profile": ["prime"],
                "result": ["win", "loss", "black-swan", "scratched"],
                "macro_grade": ["minor"],
                "structure_grade": ["good"],
            },
        )

        self.assertAlmostEqual(record["safety_ratio"], 3.0)
        self.assertAlmostEqual(record["credit_per_point"], 0.04)
        self.assertAlmostEqual(record["risk_efficiency"], 0.219512, places=6)
        self.assertEqual(record["vix_bucket"], "18-22")
        self.assertIn("safety_ratio_by_system", payload["charts"])
        self.assertIn("premium_em_by_profile", payload["charts"])
        self.assertIn("premium_em_by_vix_bucket", payload["charts"])
        self.assertEqual(payload["learning"]["trade_mode_split"]["items"][0]["label"], "Real")
        self.assertGreater(len(payload["learning"]["result_audit"]["rules"]), 0)
        self.assertEqual(payload["learning"]["overview"]["risk_efficiency_definition"], "Premium collected divided by maximum theoretical risk at entry.")
        self.assertEqual(payload["learning"]["overview"]["risk_efficiency_fallback_count"], 0)
        self.assertAlmostEqual(payload["learning"]["overview"]["avg_premium_per_em"], 12.0)
        self.assertIn("em_policy", payload["learning"])

    def test_safety_ratio_optimization_builds_guidance_buckets_and_recommendation_object(self):
        records = [
            build_performance_record(
                {
                    "id": 1,
                    "trade_number": 1,
                    "trade_mode": "real",
                    "system_name": "Apollo",
                    "candidate_profile": "Standard",
                    "status": "closed",
                    "trade_date": "2026-04-01",
                    "gross_pnl": 200.0,
                    "distance_to_short": 30.0,
                    "expected_move": 20.0,
                    "actual_entry_credit": 1.5,
                    "spread_width": 5.0,
                    "contracts": 1,
                    "vix_at_entry": 19.0,
                }
            ),
            build_performance_record(
                {
                    "id": 2,
                    "trade_number": 2,
                    "trade_mode": "real",
                    "system_name": "Apollo",
                    "candidate_profile": "Standard",
                    "status": "closed",
                    "trade_date": "2026-04-02",
                    "gross_pnl": 150.0,
                    "distance_to_short": 30.0,
                    "expected_move": 20.0,
                    "actual_entry_credit": 1.4,
                    "spread_width": 5.0,
                    "contracts": 1,
                    "vix_at_entry": 20.0,
                }
            ),
            build_performance_record(
                {
                    "id": 3,
                    "trade_number": 3,
                    "trade_mode": "real",
                    "system_name": "Apollo",
                    "candidate_profile": "Aggressive",
                    "status": "closed",
                    "trade_date": "2026-04-03",
                    "gross_pnl": -100.0,
                    "distance_to_short": 34.0,
                    "expected_move": 20.0,
                    "actual_entry_credit": 1.6,
                    "spread_width": 5.0,
                    "contracts": 1,
                    "vix_at_entry": 20.0,
                }
            ),
            build_performance_record(
                {
                    "id": 4,
                    "trade_number": 4,
                    "trade_mode": "real",
                    "system_name": "Kairos",
                    "candidate_profile": "Prime",
                    "status": "closed",
                    "trade_date": "2026-04-04",
                    "gross_pnl": 120.0,
                    "distance_to_short": 38.0,
                    "expected_move": 20.0,
                    "actual_entry_credit": 1.7,
                    "spread_width": 5.0,
                    "contracts": 1,
                    "vix_at_entry": 23.0,
                }
            ),
            build_performance_record(
                {
                    "id": 5,
                    "trade_number": 5,
                    "trade_mode": "real",
                    "system_name": "Kairos",
                    "candidate_profile": "Prime",
                    "status": "closed",
                    "trade_date": "2026-04-05",
                    "gross_pnl": -300.0,
                    "distance_to_short": 41.0,
                    "expected_move": 20.0,
                    "actual_entry_credit": 1.2,
                    "spread_width": 5.0,
                    "contracts": 1,
                    "vix_at_entry": 27.0,
                    "close_reason": "Black swan breakdown",
                }
            ),
            build_performance_record(
                {
                    "id": 6,
                    "trade_number": 6,
                    "trade_mode": "real",
                    "system_name": "Apollo",
                    "candidate_profile": "Standard",
                    "status": "closed",
                    "trade_date": "2026-04-06",
                    "gross_pnl": 0.0,
                    "distance_to_short": 30.0,
                    "expected_move": 20.0,
                    "actual_entry_credit": 1.1,
                    "spread_width": 5.0,
                    "contracts": 1,
                    "vix_at_entry": 19.0,
                }
            ),
            build_performance_record(
                {
                    "id": 7,
                    "trade_number": 7,
                    "trade_mode": "real",
                    "system_name": "Apollo",
                    "candidate_profile": "Standard",
                    "status": "closed",
                    "trade_date": "2026-04-07",
                    "gross_pnl": 50.0,
                    "distance_to_short": 30.0,
                    "expected_move": None,
                    "actual_entry_credit": 1.1,
                    "spread_width": 5.0,
                    "contracts": 1,
                    "vix_at_entry": 21.0,
                }
            ),
        ]

        payload = build_dashboard_payload(
            records,
            filters={
                "trade_mode": ["real"],
                "system": ["apollo", "kairos"],
                "profile": ["legacy", "aggressive", "fortress", "standard", "prime", "subprime"],
                "result": ["win", "loss", "black-swan", "scratched"],
                "macro_grade": ["none", "minor", "major"],
                "structure_grade": ["good", "neutral", "poor"],
            },
        )

        safety = payload["learning"]["safety_optimization"]
        bucket_lookup = {item["key"]: item for item in safety["bucket_results"]}
        recommendation = payload["learning"]["adaptive_safety_guidance"]["recommendation"]
        vix_lookup = {item["vix_bucket"]: item for item in payload["learning"]["adaptive_safety_guidance"]["by_vix_bucket"]}

        self.assertEqual(safety["closed_trade_count"], 7)
        self.assertEqual(safety["eligible_trade_count"], 6)
        self.assertEqual(safety["excluded_trade_count"], 1)
        self.assertEqual(bucket_lookup["1.4-1.6"]["trade_count"], 3)
        self.assertEqual(bucket_lookup["1.4-1.6"]["win_count"], 2)
        self.assertEqual(bucket_lookup["1.4-1.6"]["scratched_count"], 1)
        self.assertAlmostEqual(bucket_lookup["1.4-1.6"]["win_rate"], 100.0)
        self.assertAlmostEqual(bucket_lookup["1.4-1.6"]["expectancy"], 175.0)
        self.assertEqual(bucket_lookup["1.4-1.6"]["confidence"], "Low Confidence")
        self.assertAlmostEqual(bucket_lookup["1.6-1.8"]["expectancy"], -100.0)
        self.assertEqual(payload["learning"]["adaptive_safety_guidance"]["overall"]["key"], "1.4-1.6")
        self.assertEqual(recommendation["overall_optimal_safety_range"], "1.4-1.6")
        self.assertEqual(recommendation["overall_confidence"], "low")
        self.assertEqual(recommendation["overall_supporting_trade_count"], 3)
        self.assertEqual(vix_lookup["<18"]["recommended_range"], None)
        self.assertEqual(vix_lookup["<18"]["confidence"], "No Confidence")
        self.assertEqual(vix_lookup["18-22"]["recommended_range"], "1.4-1.6")
        self.assertEqual(vix_lookup["18-22"]["best_by_win_rate"]["key"], "1.4-1.6")
        self.assertEqual(vix_lookup["22-26"]["recommended_range"], "1.8-2.0")
        self.assertEqual(vix_lookup["26+"]["recommended_range"], "2.0-2.2")
        self.assertEqual(recommendation["vix_guidance"]["18-22"]["recommended_range"], "1.4-1.6")
        self.assertEqual(recommendation["vix_guidance"]["18-22"]["confidence"], "low")
        self.assertIn("safety_ratio_expectancy_curve", payload["charts"])

    def test_em_policy_learning_payload_reports_recommendations_and_exclusions(self):
        payload = build_dashboard_payload(
            [
                build_performance_record(
                    {
                        "id": 41,
                        "trade_number": 41,
                        "trade_mode": "real",
                        "system_name": "Apollo",
                        "candidate_profile": "Standard",
                        "status": "closed",
                        "trade_date": "2026-04-08",
                        "gross_pnl": 175.0,
                        "expected_move_used": 25.0,
                        "expected_move_source": "same_day_atm_straddle",
                        "em_multiple_floor": 1.8,
                        "percent_floor": 1.2,
                        "boundary_rule_used": "max(1.2% SPX, 1.8x expected move)",
                        "actual_distance_to_short": 43.75,
                        "actual_em_multiple": 1.75,
                        "fallback_used": "no",
                        "fallback_rule_name": "",
                        "structure_grade": "Good",
                        "macro_grade": "None",
                        "vix_at_entry": 20.0,
                        "actual_entry_credit": 1.6,
                        "spread_width": 5.0,
                        "contracts": 1,
                    }
                ),
                build_performance_record(
                    {
                        "id": 42,
                        "trade_number": 42,
                        "trade_mode": "real",
                        "system_name": "Kairos",
                        "candidate_profile": "Prime",
                        "status": "closed",
                        "trade_date": "2026-04-08",
                        "gross_pnl": -220.0,
                        "expected_move_used": 26.5,
                        "expected_move_source": "same_day_atm_straddle",
                        "em_multiple_floor": 2.0,
                        "percent_floor": 1.0,
                        "boundary_rule_used": "max(1.0% SPX, 2.0x expected move)",
                        "actual_distance_to_short": 54.85,
                        "actual_em_multiple": 2.07,
                        "fallback_used": "no",
                        "fallback_rule_name": "",
                        "structure_grade": "Poor",
                        "macro_grade": "Major",
                        "vix_at_entry": 24.0,
                        "actual_entry_credit": 1.2,
                        "spread_width": 10.0,
                        "contracts": 1,
                        "close_reason": "Black Swan defense failure",
                    }
                ),
                build_performance_record(
                    {
                        "id": 43,
                        "trade_number": 43,
                        "trade_mode": "real",
                        "system_name": "Apollo",
                        "candidate_profile": "Aggressive",
                        "status": "open",
                        "trade_date": "2026-04-09",
                        "gross_pnl": None,
                    }
                ),
            ],
            filters={
                "trade_mode": ["real"],
                "system": ["apollo", "kairos", "aegis"],
                "profile": ["legacy", "aggressive", "fortress", "standard", "prime", "subprime"],
                "result": ["win", "loss", "black-swan", "scratched"],
                "macro_grade": ["none", "minor", "major"],
                "structure_grade": ["good", "neutral", "poor"],
            },
        )

        em_policy = payload["learning"]["em_policy"]
        self.assertEqual(em_policy["qualified_trade_count"], 2)
        self.assertEqual(em_policy["excluded_trade_count"], 1)
        self.assertEqual(em_policy["excluded_reasons"]["open trade"], 1)
        self.assertEqual(em_policy["apollo"]["overall_recommended_em_range"], "1.6-1.8")
        self.assertEqual(em_policy["apollo"]["overall_confidence"], "low")
        self.assertEqual(em_policy["kairos"]["overall_recommended_em_range"], "2.0-2.2")
        self.assertEqual(em_policy["kairos"]["overall_confidence"], "low")


if __name__ == "__main__":
    unittest.main()
