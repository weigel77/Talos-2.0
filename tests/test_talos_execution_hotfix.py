from __future__ import annotations

from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch
from zoneinfo import ZoneInfo

from app import create_app, get_schwab_trading_auth_service
from config import AppConfig
from services.talos_execution_service import TalosExecutionService
from services.talos_execution_order_service import TalosExecutionOrderService
from services.talos_engine import TalosEngine
from services.schwab_trading_auth_service import SchwabTradingAuthService


def build_config(**overrides) -> AppConfig:
    return AppConfig(
        schwab_client_id="market-client",
        schwab_client_secret="market-secret",
        schwab_redirect_uri="https://127.0.0.1:5015/callback",
        schwab_trading_client_id="trade-client",
        schwab_trading_client_secret="trade-secret",
        schwab_trading_redirect_uri="https://127.0.0.1:5015/auth/schwab-trading/callback",
        market_data_provider="schwab",
        market_data_live_provider="schwab",
        vix_historical_provider="schwab",
        spx_historical_provider="schwab",
        **overrides,
    )


class StubTradeStore:
    def __init__(self) -> None:
        self.created_trade = None
        self.trades_by_mode = {"real": [], "simulated": [], "talos": []}
        self.expired_trade_calls = []

    def list_trades(self, trade_mode: str):
        return list(self.trades_by_mode.get(trade_mode, []))

    def create_trade(self, payload):
        self.created_trade = dict(payload)
        return 1

    @staticmethod
    def find_duplicate_trade(payload):
        del payload
        return None

    @staticmethod
    def update_trade(trade_id, payload):
        del trade_id, payload

    def expire_trade(self, trade_id, payload):
        self.expired_trade_calls.append({"trade_id": trade_id, **dict(payload)})


class StubOpenTradeManager:
    @staticmethod
    def evaluate_open_trades(*, send_alerts: bool, caller_source: str):
        del send_alerts, caller_source
        return {"records": []}


class MarketDataUnavailableOpenTradeManager:
    @staticmethod
    def evaluate_open_trades(*, send_alerts: bool, caller_source: str):
        del send_alerts, caller_source
        return {
            "records": [],
            "market_data_available": False,
            "market_data_error": "Unable to authenticate with Schwab right now (400). Please log in again.",
        }


class MarketDataUnavailableMonitorOpenTradeManager:
    @staticmethod
    def evaluate_open_trades(*, send_alerts: bool, caller_source: str):
        del send_alerts, caller_source
        return {
            "market_data_available": False,
            "market_data_error": "Unable to authenticate with Schwab right now (400). Please log in again.",
            "records": [
                {
                    "trade_id": 9,
                    "trade_number": 9009,
                    "system_name": "Talos",
                    "trade_mode": "simulated",
                    "status": "Exit Partial",
                    "action_type": "Reduce",
                    "contracts_to_close": 1,
                    "action_recommendation": "Close 1 contract",
                    "reason": "Talos Fortress exit gates are unavailable until Schwab market-data reconnect completes.",
                    "next_trigger": "Talos Fortress exit gates are unavailable until Schwab market-data reconnect completes.",
                }
            ],
        }


class FailOnUpdateTradeStore(StubTradeStore):
    @staticmethod
    def update_trade(trade_id, payload):
        raise AssertionError(f"update_trade should not be called when market data is unavailable: {trade_id} {payload}")


class StubApolloService:
    @staticmethod
    def run_precheck(*, force_refresh: bool, caller_source: str):
        del force_refresh, caller_source
        return {
            "trade_candidates": {
                "candidates": [
                    {
                        "mode_key": "fortress",
                        "short_strike": 7320,
                        "long_strike": 7305,
                        "width": 15,
                        "credit": 0.6,
                        "recommended_contract_size": 10,
                        "realistic_max_loss": 7307.5,
                        "max_theoretical_risk_per_contract": 1440.0,
                        "actual_distance_to_short": 80.96,
                        "actual_em_multiple": 2.1,
                        "rationale": ["Fortress candidate available."],
                        "diagnostics": ["Credit map aligned."],
                    }
                ]
            },
            "option_chain": {
                "expiration_date": "2026-05-13",
                "symbol_requested": "SPX",
            },
            "spx": {"value": 7400.96},
            "vix": {"value": 17.99},
        }


class DisconnectedExecutionAuth:
    @staticmethod
    def get_connection_status():
        return {
            "connected": False,
            "status_label": "Execution auth disconnected",
            "status_meta": "Manual fallback active",
            "token_expiration_display": "—",
        }

    @staticmethod
    def get_account_summary():
        raise AssertionError("manual fallback path should not request a live account summary")


class ConnectedExecutionAuth:
    @staticmethod
    def get_connection_status():
        return {
            "connected": True,
            "status_label": "Connected",
            "status_meta": "Schwab trading",
            "token_expiration_display": "2026-05-12 17:30",
        }

    @staticmethod
    def get_account_summary():
        return {
            "account_number_masked": "***1234",
            "account_hash": "hash-1234",
            "account_type": "Margin",
            "liquidation_value": 250000.0,
            "buying_power": 500000.0,
            "as_of_display": "2026-05-12 04:55 PM CDT",
        }

    @staticmethod
    def get_account_identity():
        return {"account_hash": "hash-1234", "account_number": "***1234", "account_number_masked": "***1234"}


class ConnectedExecutionAuthAccountUnavailable:
    @staticmethod
    def get_connection_status():
        return {
            "connected": True,
            "status_label": "Connected",
            "status_meta": "Schwab trading",
            "token_expiration_display": "2026-05-12 17:30",
        }

    @staticmethod
    def get_account_summary():
        raise RuntimeError("balances unavailable")


class TalosExecutionHotfixTests(unittest.TestCase):
    def test_candidate_cache_reuses_apollo_precheck_between_dashboard_reads(self) -> None:
        with TemporaryDirectory() as temp_dir:
            class CountingApolloService(StubApolloService):
                def __init__(self) -> None:
                    self.calls = 0

                def run_precheck(self, *, force_refresh: bool, caller_source: str):
                    del force_refresh, caller_source
                    self.calls += 1
                    return StubApolloService.run_precheck(force_refresh=False, caller_source="counting")

            apollo_service = CountingApolloService()
            engine = TalosEngine(
                trade_store=StubTradeStore(),
                apollo_service=apollo_service,
                open_trade_manager=StubOpenTradeManager(),
                execution_auth_service=DisconnectedExecutionAuth(),
                config=build_config(talos_candidate_cache_ttl_seconds=60),
                state_path=Path(temp_dir) / "talos_state.json",
            )
            engine.initialize()

            engine.get_dashboard_payload(force_refresh=False)
            engine.get_dashboard_payload(force_refresh=False)

            self.assertEqual(apollo_service.calls, 1)

    def test_execution_service_marks_open_flow_filled_after_long_first_sequence(self) -> None:
        class StubGateway:
            CONFIRMATION_TEXT = TalosExecutionOrderService.CONFIRMATION_TEXT

            @staticmethod
            def resolve_account_context():
                return {"account_hash": "acct-hash", "account_number": "acct-hash", "source": "Stub"}

            @staticmethod
            def build_open_order_preview(*, candidate_payload, sizing_payload, account_context):
                del candidate_payload, sizing_payload, account_context
                return {
                    "contracts": 2,
                    "expiration_date": "2026-05-19",
                    "short_strike": 7245,
                    "long_strike": 7230,
                    "limit_price_display": "0.45",
                    "limit_price": 0.45,
                    "account_hash": "acct-hash",
                    "short_option_symbol": "OPT-SHORT",
                    "long_option_symbol": "OPT-LONG",
                    "short_bid": 0.52,
                    "long_ask": 0.08,
                }

            @staticmethod
            def _marketable_limit_price(reference_price, *, buy_side: bool):
                del buy_side
                return float(reference_price or 0.0)

            @staticmethod
            def _submit_single_leg_order(*, preview, symbol, instruction, price, action_label):
                del preview, price, action_label
                return {"ok": True, "order_id": f"{instruction}-{symbol}", "broker_status": 201}

            @staticmethod
            def submit_close_order(preview):
                del preview
                return {"ok": True, "order_id": "close-1", "broker_status": 201}

        service = TalosExecutionService(
            execution_auth_service=ConnectedExecutionAuth(),
            gateway=StubGateway(),
            config=build_config(talos_execution_enabled=True, talos_execution_account="acct-hash", talos_execution_account_name="Prod Account"),
        )
        preview = service.build_open_order_preview(
            candidate_payload={"raw_candidate": {}, "expiration_date": "2026-05-19", "short_strike": 7245, "long_strike": 7230},
            sizing_payload={"contracts_selected": 2, "projected_total_black_swan_loss_display": "$900"},
            account_context=service.resolve_account_context(),
        )

        result = service.submit_open_order(preview)

        self.assertTrue(result["ok"])
        self.assertEqual(result["execution_state"], TalosExecutionService.EXECUTION_STATE_FILLED)
        self.assertEqual(service.get_status_snapshot()["open"]["state"], TalosExecutionService.EXECUTION_STATE_FILLED)

    def test_execution_service_blocks_duplicate_submission_during_cooldown(self) -> None:
        class StubGateway:
            CONFIRMATION_TEXT = TalosExecutionOrderService.CONFIRMATION_TEXT

            @staticmethod
            def resolve_account_context():
                return {"account_hash": "acct-hash", "account_number": "acct-hash", "source": "Stub"}

            @staticmethod
            def build_open_order_preview(*, candidate_payload, sizing_payload, account_context):
                del candidate_payload, sizing_payload, account_context
                return {
                    "contracts": 1,
                    "expiration_date": "2026-05-19",
                    "short_strike": 7245,
                    "long_strike": 7230,
                    "limit_price_display": "0.45",
                    "limit_price": 0.45,
                    "account_hash": "acct-hash",
                    "short_option_symbol": "OPT-SHORT",
                    "long_option_symbol": "OPT-LONG",
                    "short_bid": 0.52,
                    "long_ask": 0.08,
                }

            @staticmethod
            def _marketable_limit_price(reference_price, *, buy_side: bool):
                del buy_side
                return float(reference_price or 0.0)

            @staticmethod
            def _submit_single_leg_order(*, preview, symbol, instruction, price, action_label):
                del preview, symbol, instruction, price, action_label
                return {"ok": True, "order_id": "leg-1", "broker_status": 201}

            @staticmethod
            def submit_close_order(preview):
                del preview
                return {"ok": True, "order_id": "close-1", "broker_status": 201}

        service = TalosExecutionService(
            execution_auth_service=ConnectedExecutionAuth(),
            gateway=StubGateway(),
            config=build_config(talos_execution_enabled=True, talos_execution_account="acct-hash", talos_execution_cooldown_seconds=3600),
        )
        preview = service.build_open_order_preview(
            candidate_payload={"raw_candidate": {}, "expiration_date": "2026-05-19", "short_strike": 7245, "long_strike": 7230},
            sizing_payload={"contracts_selected": 1, "projected_total_black_swan_loss_display": "$450"},
            account_context=service.resolve_account_context(),
        )

        first_result = service.submit_open_order(preview)
        second_result = service.submit_open_order(preview)

        self.assertTrue(first_result["ok"])
        self.assertFalse(second_result["ok"])
        self.assertEqual(second_result["execution_state"], TalosExecutionService.EXECUTION_STATE_BLOCKED)
        self.assertIn("Duplicate order blocked", second_result["message"])

    def test_real_preview_requires_execution_account_configuration(self) -> None:
        with TemporaryDirectory() as temp_dir:
            engine = TalosEngine(
                trade_store=StubTradeStore(),
                apollo_service=StubApolloService(),
                open_trade_manager=StubOpenTradeManager(),
                execution_auth_service=ConnectedExecutionAuth(),
                config=build_config(talos_execution_enabled=True, talos_execution_account=""),
                state_path=Path(temp_dir) / "talos_state.json",
            )
            engine.initialize()
            engine.update_settings({"master_mode": "ACTIVE"})

            result = engine.run_real_open_check(trigger_reason="manual-test", confirmation_text="")
            payload = engine.get_dashboard_payload(force_refresh=False)

            self.assertFalse(result["ok"])
            self.assertIn("Execution account not configured", payload["real_execution"]["last_open_reason"])
            self.assertEqual(payload["real_execution"]["open_preview"]["execution_state"], TalosExecutionService.EXECUTION_STATE_BLOCKED)

    def test_autonomous_open_is_blocked_outside_timing_window(self) -> None:
        with TemporaryDirectory() as temp_dir:
            trade_store = StubTradeStore()
            engine = TalosEngine(
                trade_store=trade_store,
                apollo_service=StubApolloService(),
                open_trade_manager=StubOpenTradeManager(),
                execution_auth_service=ConnectedExecutionAuth(),
                config=build_config(),
                state_path=Path(temp_dir) / "talos_state.json",
            )
            engine.initialize()
            engine.update_settings({"master_mode": "SIMULATED"})
            state = engine._load_state()
            noon = datetime(2026, 5, 18, 12, 0, tzinfo=ZoneInfo("America/Chicago"))

            with patch.object(engine, "_now", return_value=noon):
                changed = engine._run_automatic_checks(state, caller_source="test-timing")

            self.assertTrue(changed)
            self.assertIsNone(trade_store.created_trade)
            self.assertEqual(
                state["activity_log"][0]["reason"],
                "autonomous open blocked outside approved timing window",
            )

    def test_manual_forced_simulated_open_bypasses_timing_window_and_logs(self) -> None:
        with TemporaryDirectory() as temp_dir:
            trade_store = StubTradeStore()
            engine = TalosEngine(
                trade_store=trade_store,
                apollo_service=StubApolloService(),
                open_trade_manager=StubOpenTradeManager(),
                execution_auth_service=ConnectedExecutionAuth(),
                config=build_config(),
                state_path=Path(temp_dir) / "talos_state.json",
            )
            engine.initialize()
            engine.update_settings({"master_mode": "SIMULATED"})
            noon = datetime(2026, 5, 18, 12, 0, tzinfo=ZoneInfo("America/Chicago"))

            with patch.object(engine, "_now", return_value=noon):
                result = engine.run_simulated_open_check(trigger_reason="manual-forced")

            state = engine._load_state()
            self.assertTrue(result["ok"])
            self.assertIsNotNone(trade_store.created_trade)
            matching_entries = [item for item in state["activity_log"] if item.get("reason") == "Manual forced simulated open"]
            self.assertTrue(matching_entries)
            self.assertIn("Timing window bypassed by operator", matching_entries[0]["details"])

    def test_real_open_preview_retains_estimated_contracts_when_mode_is_not_active(self) -> None:
        with TemporaryDirectory() as temp_dir:
            trade_store = StubTradeStore()
            trade_store.trades_by_mode["simulated"] = [
                {
                    "id": 600,
                    "trade_number": 600,
                    "trade_mode": "simulated",
                    "system_name": "Talos",
                    "status": "open",
                    "contracts": 6,
                    "remaining_contracts": 6,
                    "expiration_date": "2026-05-13",
                    "projected_black_swan_loss": 8640.0,
                    "notes_entry": "Talos simulated ownership",
                }
            ]

            engine = TalosEngine(
                trade_store=trade_store,
                apollo_service=StubApolloService(),
                open_trade_manager=StubOpenTradeManager(),
                execution_auth_service=ConnectedExecutionAuth(),
                config=build_config(),
                state_path=Path(temp_dir) / "talos_state.json",
            )
            engine.initialize()
            engine.update_settings({"master_mode": "SIMULATED"})

            result = engine.run_real_open_check(trigger_reason="manual-test", confirmation_text="")
            payload = engine.get_dashboard_payload(force_refresh=False)

            self.assertFalse(result["ok"])
            self.assertEqual(payload["real_execution"]["open_preview"]["contracts"], 17)
            self.assertEqual(
                payload["real_execution"]["open_preview"]["execution_blocked_message"],
                "Execution Blocked (Simulated Mode)",
            )

    def test_real_open_check_shows_preview_before_confirmation(self) -> None:
        with TemporaryDirectory() as temp_dir:
            class StubOrderService:
                CONFIRMATION_TEXT = TalosExecutionOrderService.CONFIRMATION_TEXT

                @staticmethod
                def resolve_account_context():
                    return {"account_hash": "hash-1234", "account_number": "***1234", "source": "Schwab execution API"}

                @staticmethod
                def build_open_order_preview(*, candidate_payload, sizing_payload, account_context):
                    del candidate_payload, sizing_payload, account_context
                    return {"signature": "preview-open-1", "summary": "Preview ready", "contracts": 17}

                @staticmethod
                def submit_open_order(preview):
                    raise AssertionError(f"submit_open_order should not run before confirmation: {preview}")

            engine = TalosEngine(
                trade_store=StubTradeStore(),
                apollo_service=StubApolloService(),
                open_trade_manager=StubOpenTradeManager(),
                execution_auth_service=ConnectedExecutionAuth(),
                order_service=StubOrderService(),
                config=build_config(),
                state_path=Path(temp_dir) / "talos_state.json",
            )
            engine.initialize()
            engine.update_settings({"master_mode": "ACTIVE"})

            result = engine.run_real_open_check(trigger_reason="manual-test", confirmation_text="")
            payload = engine.get_dashboard_payload(force_refresh=False)

            self.assertFalse(result["ok"])
            self.assertIn("preview", result["message"].lower())
            self.assertEqual(payload["real_execution"]["open_preview"]["signature"], "preview-open-1")

    def test_real_open_check_requires_duplicate_protection(self) -> None:
        with TemporaryDirectory() as temp_dir:
            class DuplicateTradeStore(StubTradeStore):
                @staticmethod
                def find_duplicate_trade(payload):
                    return {"id": 77, "status": "open", **dict(payload)}

            class StubOrderService:
                CONFIRMATION_TEXT = TalosExecutionOrderService.CONFIRMATION_TEXT

                @staticmethod
                def resolve_account_context():
                    return {"account_hash": "hash-1234", "account_number": "***1234", "source": "Schwab execution API"}

                @staticmethod
                def build_open_order_preview(*, candidate_payload, sizing_payload, account_context):
                    del candidate_payload, sizing_payload, account_context
                    return {"signature": "preview-open-2", "summary": "Preview ready", "contracts": 17}

                @staticmethod
                def submit_open_order(preview):
                    raise AssertionError(f"submit_open_order should not run for duplicate trade: {preview}")

            engine = TalosEngine(
                trade_store=DuplicateTradeStore(),
                apollo_service=StubApolloService(),
                open_trade_manager=StubOpenTradeManager(),
                execution_auth_service=ConnectedExecutionAuth(),
                order_service=StubOrderService(),
                config=build_config(),
                state_path=Path(temp_dir) / "talos_state.json",
            )
            engine.initialize()
            engine.update_settings({"master_mode": "ACTIVE"})

            result = engine.run_real_open_check(
                trigger_reason="manual-test",
                confirmation_text=TalosExecutionOrderService.CONFIRMATION_TEXT,
            )

            self.assertFalse(result["ok"])
            self.assertIn("same expiration", result["message"].lower())

    def test_real_close_check_blocks_without_active_gate_or_override(self) -> None:
        with TemporaryDirectory() as temp_dir:
            trade_store = StubTradeStore()
            trade_store.trades_by_mode["real"] = [
                {
                    "id": 510,
                    "trade_number": 510,
                    "trade_mode": "real",
                    "system_name": "Talos",
                    "status": "open",
                    "contracts": 2,
                    "remaining_contracts": 2,
                    "expiration_date": "2026-05-14",
                    "short_strike": 7320,
                    "long_strike": 7305,
                    "notes_entry": "Talos real ownership | short_option_symbol=OPT-SHORT | long_option_symbol=OPT-LONG",
                }
            ]

            class QuietCloseManager:
                @staticmethod
                def evaluate_open_trades(*, send_alerts: bool, caller_source: str):
                    del send_alerts, caller_source
                    return {"market_data_available": True, "records": [{"trade_id": 510, "status": "Healthy", "contracts_to_close": 0}]}

                @staticmethod
                def update_talos_trade_gate_state(trade, gate_key):
                    del gate_key
                    return trade

            class StubOrderService:
                CONFIRMATION_TEXT = TalosExecutionOrderService.CONFIRMATION_TEXT

                @staticmethod
                def resolve_account_context():
                    return {"account_hash": "hash-1234", "account_number": "***1234", "source": "Schwab execution API"}

                @staticmethod
                def build_close_order_preview(*, trade, record, account_context, manual_override):
                    del trade, record, account_context, manual_override
                    return {"signature": "preview-close-1", "summary": "Close preview ready", "contracts": 2}

                @staticmethod
                def submit_close_order(preview):
                    raise AssertionError(f"submit_close_order should not run without an active gate: {preview}")

            engine = TalosEngine(
                trade_store=trade_store,
                apollo_service=StubApolloService(),
                open_trade_manager=QuietCloseManager(),
                execution_auth_service=ConnectedExecutionAuth(),
                order_service=StubOrderService(),
                config=build_config(),
                state_path=Path(temp_dir) / "talos_state.json",
            )
            engine.initialize()
            engine.update_settings({"master_mode": "ACTIVE"})

            result = engine.run_real_close_check(trigger_reason="manual-test", confirmation_text="", manual_override=False)

            self.assertFalse(result["ok"])
            self.assertIn("exit gate", result["message"].lower())

    def test_manual_account_fallback_sizes_fortress_without_execution_auth(self) -> None:
        with TemporaryDirectory() as temp_dir:
            engine = TalosEngine(
                trade_store=StubTradeStore(),
                apollo_service=StubApolloService(),
                open_trade_manager=StubOpenTradeManager(),
                execution_auth_service=DisconnectedExecutionAuth(),
                config=build_config(),
                state_path=Path(temp_dir) / "talos_state.json",
            )
            engine.initialize()

            payload = engine.get_dashboard_payload(force_refresh=False)

            self.assertEqual(payload["account"]["account_source"], "Manual override")
            self.assertEqual(payload["account"]["account_value"], 135000.0)
            self.assertEqual(payload["decision_engine"]["sizing"]["max_contracts_allowed"], 9)
            self.assertEqual(payload["decision_engine"]["sizing"]["projected_total_theoretical_loss"], 12960.0)
            self.assertNotEqual(payload["decision_engine"]["reason"], "Schwab auth missing")

    def test_active_mode_is_selectable_but_not_live_routing_enabled(self) -> None:
        with TemporaryDirectory() as temp_dir:
            engine = TalosEngine(
                trade_store=StubTradeStore(),
                apollo_service=StubApolloService(),
                open_trade_manager=StubOpenTradeManager(),
                execution_auth_service=DisconnectedExecutionAuth(),
                config=build_config(),
                state_path=Path(temp_dir) / "talos_state.json",
            )
            engine.initialize()

            result = engine.update_settings({"master_mode": "ACTIVE"})
            payload = engine.get_dashboard_payload(force_refresh=False)

            self.assertTrue(result["ok"])
            self.assertEqual(payload["master_mode"]["current"], "ACTIVE")
            self.assertTrue(all(not option["disabled"] for option in payload["master_mode"]["options"]))
            self.assertIn("reserved", payload["decision_engine"]["reason"])

    def test_connected_execution_auth_uses_live_account_summary(self) -> None:
        with TemporaryDirectory() as temp_dir:
            engine = TalosEngine(
                trade_store=StubTradeStore(),
                apollo_service=StubApolloService(),
                open_trade_manager=StubOpenTradeManager(),
                execution_auth_service=ConnectedExecutionAuth(),
                config=build_config(),
                state_path=Path(temp_dir) / "talos_state.json",
            )
            engine.initialize()

            payload = engine.get_dashboard_payload(force_refresh=False)

            self.assertEqual(payload["account"]["account_source"], "Live Schwab trading")
            self.assertEqual(payload["account"]["execution_account_display"], "***1234")
            self.assertEqual(payload["decision_engine"]["sizing"]["max_contracts_allowed"], 17)

    def test_connected_execution_auth_falls_back_to_manual_value_without_marking_disconnected(self) -> None:
        with TemporaryDirectory() as temp_dir:
            engine = TalosEngine(
                trade_store=StubTradeStore(),
                apollo_service=StubApolloService(),
                open_trade_manager=StubOpenTradeManager(),
                execution_auth_service=ConnectedExecutionAuthAccountUnavailable(),
                config=build_config(),
                state_path=Path(temp_dir) / "talos_state.json",
            )
            engine.initialize()

            payload = engine.get_dashboard_payload(force_refresh=False)

            self.assertTrue(payload["account"]["execution_connected"])
            self.assertEqual(payload["account"]["execution_status"], "Connected")
            self.assertEqual(payload["account"]["account_source"], "Manual override")
            self.assertEqual(payload["account"]["account_value"], 135000.0)
            self.assertIn("Execution connected; account refresh temporarily unavailable.", payload["account"]["execution_status_meta"])

    def test_dashboard_payload_degrades_when_market_data_auth_is_unavailable(self) -> None:
        with TemporaryDirectory() as temp_dir:
            engine = TalosEngine(
                trade_store=StubTradeStore(),
                apollo_service=StubApolloService(),
                open_trade_manager=MarketDataUnavailableOpenTradeManager(),
                execution_auth_service=DisconnectedExecutionAuth(),
                config=build_config(),
                state_path=Path(temp_dir) / "talos_state.json",
            )
            engine.initialize()

            payload = engine.get_dashboard_payload(force_refresh=False)

            self.assertEqual(payload["decision_engine"]["phase_label"], "Unavailable")
            self.assertEqual(payload["decision_engine"]["decision_label"], "Cannot Evaluate")
            self.assertIn("market-data auth", payload["decision_engine"]["reason"])
            self.assertEqual(payload["candidate"]["block_reason"], "market-data auth unavailable")
            self.assertEqual(payload["decision_engine"]["sizing"]["contracts_selected"], 0)
            self.assertIn("market data needs to reconnect", payload["decision_engine"]["sizing"]["sizing_note"])

    def test_monitor_loop_skips_exit_actions_when_market_data_is_unavailable(self) -> None:
        with TemporaryDirectory() as temp_dir:
            engine = TalosEngine(
                trade_store=FailOnUpdateTradeStore(),
                apollo_service=StubApolloService(),
                open_trade_manager=MarketDataUnavailableMonitorOpenTradeManager(),
                execution_auth_service=DisconnectedExecutionAuth(),
                config=build_config(),
                state_path=Path(temp_dir) / "talos_state.json",
            )
            engine.initialize()

            payload = engine.run_background_monitor_cycle()

            self.assertIn("reconnect required", payload["market_status"].lower())
            self.assertEqual(payload["current_spx_display"], "Unavailable")
            self.assertEqual(payload["evaluated_trade_count"], 1)

    def test_same_day_expiring_exposure_is_ignored_for_next_cycle_sizing(self) -> None:
        with TemporaryDirectory() as temp_dir:
            trade_store = StubTradeStore()
            trade_store.trades_by_mode["simulated"] = [
                {
                    "id": 300,
                    "trade_number": 300,
                    "trade_mode": "simulated",
                    "system_name": "Talos",
                    "status": "open",
                    "contracts": 4,
                    "remaining_contracts": 4,
                    "expiration_date": "2026-05-13",
                    "projected_black_swan_loss": 2923.0,
                    "notes_entry": "Talos simulated ownership",
                },
                {
                    "id": 301,
                    "trade_number": 301,
                    "trade_mode": "simulated",
                    "system_name": "Talos",
                    "status": "open",
                    "contracts": 2,
                    "remaining_contracts": 2,
                    "expiration_date": "2026-05-14",
                    "projected_black_swan_loss": 1461.5,
                    "notes_entry": "Talos simulated ownership",
                },
            ]
            engine = TalosEngine(
                trade_store=trade_store,
                apollo_service=StubApolloService(),
                open_trade_manager=StubOpenTradeManager(),
                execution_auth_service=DisconnectedExecutionAuth(),
                config=build_config(),
                state_path=Path(temp_dir) / "talos_state.json",
            )
            engine.initialize()

            with patch.object(engine, "_now", return_value=datetime(2026, 5, 13, 14, 55, tzinfo=engine.display_timezone)):
                payload = engine.get_dashboard_payload(force_refresh=False)

            self.assertEqual(payload["account"]["open_exposure"], 1461.5)
            self.assertEqual(payload["account"]["ignored_same_day_exposure"], 2923.0)
            self.assertEqual(payload["decision_engine"]["sizing"]["open_exposure"], 1461.5)
            self.assertEqual(payload["decision_engine"]["sizing"]["ignored_same_day_exposure"], 2923.0)

    def test_simulated_close_check_only_closes_when_exit_gate_is_active(self) -> None:
        with TemporaryDirectory() as temp_dir:
            trade_store = StubTradeStore()
            trade_store.trades_by_mode["simulated"] = [
                {
                    "id": 410,
                    "trade_number": 410,
                    "trade_mode": "simulated",
                    "system_name": "Talos",
                    "status": "open",
                    "contracts": 2,
                    "remaining_contracts": 2,
                    "expiration_date": "2026-05-13",
                    "notes_entry": "Talos simulated ownership",
                }
            ]

            class CloseCheckManager:
                @staticmethod
                def evaluate_open_trades(*, send_alerts: bool, caller_source: str):
                    del send_alerts, caller_source
                    return {
                        "records": [
                            {
                                "trade_id": 410,
                                "trade_number": 410,
                                "trade_mode": "simulated",
                                "status": "Healthy",
                                "action_type": "",
                                "action_recommendation": "Watch",
                                "contracts_to_close": 0,
                            }
                        ]
                    }

            engine = TalosEngine(
                trade_store=trade_store,
                apollo_service=StubApolloService(),
                open_trade_manager=CloseCheckManager(),
                execution_auth_service=DisconnectedExecutionAuth(),
                config=build_config(),
                state_path=Path(temp_dir) / "talos_state.json",
            )
            engine.initialize()

            result = engine.run_simulated_close_check(trigger_reason="manual-test")

            self.assertFalse(result["ok"])
            self.assertIn("no active exit gate", result["message"].lower())
            self.assertIsNone(trade_store.created_trade)

    def test_execution_login_route_uses_separate_callback_surface(self) -> None:
        app = create_app(
            {
                "TESTING": True,
                "WTF_CSRF_ENABLED": False,
                "RUNTIME_TARGET": "local",
                "SCHWAB_TRADING_CLIENT_ID": "trade-client",
                "SCHWAB_TRADING_CLIENT_SECRET": "trade-secret",
                "SCHWAB_TRADING_REDIRECT_URI": "https://127.0.0.1:5015/auth/schwab-trading/callback",
            }
        )
        client = app.test_client()
        service = get_schwab_trading_auth_service(app)

        with patch.object(service, "build_authorization_url", return_value="https://example.com/trading-auth"):
            response = client.get("/auth/schwab-trading/login", follow_redirects=False)

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers["Location"], "https://example.com/trading-auth")
        with client.session_transaction() as session_state:
            self.assertFalse(session_state["schwab_trading_authenticated"])
            self.assertEqual(session_state["schwab_trading_account"], "")

    def test_execution_auth_debug_reports_missing_token_fields(self) -> None:
        with TemporaryDirectory() as temp_dir:
            config = build_config(schwab_trading_token_path=str(Path(temp_dir) / "schwab_trading_token.json"))
            service = SchwabTradingAuthService(config=config)
            service.token_store.save({"auth_state": "refresh_expired", "last_auth_error": "Unauthorized"})

            debug_status = service.get_debug_status()
            connection_status = service.get_connection_status()

            self.assertTrue(debug_status["token_file_exists"])
            self.assertTrue(debug_status["token_loaded"])
            self.assertFalse(debug_status["token_valid"])
            self.assertEqual(debug_status["last_auth_error"], "Unauthorized")
            self.assertFalse(connection_status["connected"])

    def test_refreshable_execution_token_chain_is_not_reported_disconnected(self) -> None:
        with TemporaryDirectory() as temp_dir:
            config = build_config(schwab_trading_token_path=str(Path(temp_dir) / "schwab_trading_token.json"))
            service = SchwabTradingAuthService(config=config)
            service.token_store.save(
                {
                    "access_token": "abc",
                    "refresh_token": "def",
                    "expires_at": "2026-05-14T17:00:00",
                    "refresh_expires_at": "2026-05-21T17:00:00",
                    "auth_state": "connected",
                    "last_auth_error": "",
                }
            )

            with patch.object(service, "_utcnow", return_value=datetime(2026, 5, 14, 17, 5, 0)):
                connection_status = service.get_connection_status()

            self.assertTrue(connection_status["connected"])
            self.assertTrue(connection_status["usable_token_chain"])
            self.assertEqual(connection_status["status_label"], "Refreshing execution token")

    def test_reconcile_expired_talos_trades_expires_only_after_close(self) -> None:
        with TemporaryDirectory() as temp_dir:
            trade_store = StubTradeStore()
            trade_store.trades_by_mode["simulated"] = [
                {
                    "id": 171,
                    "trade_number": 171,
                    "trade_mode": "simulated",
                    "system_name": "Talos",
                    "status": "open",
                    "contracts": 7,
                    "remaining_contracts": 7,
                    "expiration_date": "2026-05-13",
                    "notes_entry": "Talos simulated ownership",
                },
                {
                    "id": 173,
                    "trade_number": 173,
                    "trade_mode": "simulated",
                    "system_name": "Talos",
                    "status": "open",
                    "contracts": 7,
                    "remaining_contracts": 7,
                    "expiration_date": "2026-05-14",
                    "notes_entry": "Talos simulated ownership",
                },
            ]
            engine = TalosEngine(
                trade_store=trade_store,
                apollo_service=StubApolloService(),
                open_trade_manager=StubOpenTradeManager(),
                execution_auth_service=DisconnectedExecutionAuth(),
                config=build_config(),
                state_path=Path(temp_dir) / "talos_state.json",
            )
            engine.initialize()
            trade_store.expired_trade_calls.clear()

            with patch.object(engine, "_now", return_value=datetime(2026, 5, 14, 14, 55, tzinfo=engine.display_timezone)):
                report_before_close = engine.reconcile_expired_talos_trades(caller_source="test-before-close")

            self.assertTrue(report_before_close["changed"])
            self.assertEqual([item["trade_id"] for item in trade_store.expired_trade_calls], [171])

            trade_store.expired_trade_calls.clear()
            with patch.object(engine, "_now", return_value=datetime(2026, 5, 14, 15, 1, tzinfo=engine.display_timezone)):
                report_after_close = engine.reconcile_expired_talos_trades(caller_source="test-after-close")

            self.assertTrue(report_after_close["changed"])
            self.assertEqual([item["trade_id"] for item in trade_store.expired_trade_calls], [171, 173])

    def test_persist_token_state_keeps_existing_execution_token_fields(self) -> None:
        with TemporaryDirectory() as temp_dir:
            config = build_config(schwab_trading_token_path=str(Path(temp_dir) / "schwab_trading_token.json"))
            service = SchwabTradingAuthService(config=config)
            service.token_store.save(
                {
                    "access_token": "abc",
                    "refresh_token": "def",
                    "expires_at": "2026-05-14T18:00:00",
                    "auth_state": "connected",
                    "last_auth_error": "",
                }
            )

            service._persist_token_state({}, auth_state="refresh_expired", last_auth_error="Unauthorized")
            stored = service.token_store.load() or {}

            self.assertEqual(stored["access_token"], "abc")
            self.assertEqual(stored["refresh_token"], "def")
            self.assertEqual(stored["expires_at"], "2026-05-14T18:00:00")
            self.assertEqual(stored["auth_state"], "refresh_expired")
            self.assertEqual(stored["last_auth_error"], "Unauthorized")

    def test_talos_dashboard_loads_all_shared_journal_talos_trades(self) -> None:
        with TemporaryDirectory() as temp_dir:
            trade_store = StubTradeStore()
            trade_store.trades_by_mode = {
                "real": [],
                "simulated": [
                    {
                        "id": 167,
                        "trade_number": 167,
                        "trade_mode": "simulated",
                        "system_name": "Talos",
                        "candidate_profile": "Fortress",
                        "status": "open",
                        "contracts": 4,
                        "remaining_contracts": 4,
                        "actual_entry_credit": 0.6,
                        "max_loss": 5760.0,
                    }
                ],
                "talos": [
                    {
                        "id": 136,
                        "trade_number": 128,
                        "trade_mode": "talos",
                        "system_name": "Apollo",
                        "candidate_profile": "Fortress",
                        "status": "open",
                        "contracts": 1,
                        "remaining_contracts": 1,
                        "actual_entry_credit": 0.6,
                        "notes_entry": "Talos scaled Apollo quantity 10 -> 1 to fit the hard Black Swan cap ($7,307.46 -> $730.75 projected Black Swan loss).",
                    },
                    {
                        "id": 137,
                        "trade_number": 129,
                        "trade_mode": "talos",
                        "system_name": "Apollo",
                        "candidate_profile": "Aggressive",
                        "status": "open",
                        "contracts": 7,
                        "remaining_contracts": 7,
                        "actual_entry_credit": 2.4,
                        "notes_entry": "Talos scaled Apollo quantity 8 -> 7 to fit the hard Black Swan cap ($13,194.03 -> $11,544.78 projected Black Swan loss).",
                    },
                    {
                        "id": 138,
                        "trade_number": 130,
                        "trade_mode": "talos",
                        "system_name": "Apollo",
                        "candidate_profile": "Standard",
                        "status": "open",
                        "contracts": 1,
                        "remaining_contracts": 1,
                        "actual_entry_credit": 1.2,
                        "notes_entry": "Talos scaled Apollo quantity 8 -> 1 to fit the hard Black Swan cap ($9,631.64 -> $1,203.96 projected Black Swan loss).",
                    },
                    {
                        "id": 201,
                        "trade_number": 201,
                        "trade_mode": "talos",
                        "system_name": "Apollo",
                        "candidate_profile": "Fortress",
                        "status": "closed",
                        "contracts": 1,
                        "remaining_contracts": 0,
                    },
                ],
            }
            engine = TalosEngine(
                trade_store=trade_store,
                apollo_service=StubApolloService(),
                open_trade_manager=StubOpenTradeManager(),
                execution_auth_service=DisconnectedExecutionAuth(),
                config=build_config(),
                state_path=Path(temp_dir) / "talos_state.json",
            )
            engine.initialize()

            payload = engine.get_dashboard_payload(force_refresh=False)

            self.assertEqual(payload["trade_summary"]["total"], 5)
            self.assertEqual(payload["trade_summary"]["open"], 4)
            self.assertEqual(payload["trade_summary"]["simulated"], 1)
            self.assertEqual({item["trade_number"] for item in payload["recent_trades"]}, {167, 128, 129, 130, 201})
            self.assertAlmostEqual(payload["account"]["open_exposure"], 19239.49)


if __name__ == "__main__":
    unittest.main()