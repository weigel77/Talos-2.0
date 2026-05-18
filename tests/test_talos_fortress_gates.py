from services.open_trade_manager import OpenTradeManager


def build_manager() -> OpenTradeManager:
    manager = object.__new__(OpenTradeManager)
    manager._talos_gate_test_hook = "live"
    return manager


def test_talos_gate_close_quantity_uses_quarter_round_down_with_minimum_one() -> None:
    manager = build_manager()

    assert manager._resolve_talos_gate_close_quantity(0) == 0
    assert manager._resolve_talos_gate_close_quantity(1) == 1
    assert manager._resolve_talos_gate_close_quantity(3) == 1
    assert manager._resolve_talos_gate_close_quantity(8) == 2


def test_extract_talos_fired_gates_reads_automation_status_and_close_events() -> None:
    manager = build_manager()
    trade = {
        "automation_status": "owned=talos|talos_exit_gates=gate_1,gate_3",
        "close_events": [
            {"notes_exit": "Talos simulated close | gate=gate_2"},
            {"notes_exit": "Talos simulated close | gate=gate_4"},
        ],
    }

    assert manager._extract_talos_fired_gates(trade) == {"gate_1", "gate_2", "gate_3", "gate_4"}


def test_update_talos_trade_gate_state_appends_and_normalizes_gate_state() -> None:
    manager = build_manager()
    trade = {
        "automation_status": "owned=talos|talos_exit_gates=gate_1,gate_3",
        "close_events": [],
    }

    updated = manager.update_talos_trade_gate_state(trade, "gate_2")

    assert updated["automation_status"] == "owned=talos|talos_exit_gates=gate_1,gate_2,gate_3"


def test_update_talos_trade_gate_state_ignores_unknown_gate_keys() -> None:
    manager = build_manager()
    trade = {
        "automation_status": "owned=talos",
        "close_events": [],
    }

    updated = manager.update_talos_trade_gate_state(trade, "not-a-gate")

    assert updated == trade