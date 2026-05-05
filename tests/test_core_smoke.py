from app import create_app, load_retained_trade_rows
from services.performance_dashboard_service import PerformanceDashboardService
from services.trade_store import resolve_trade_system_name


class StubTradeStore:
    def __init__(self, rows_by_mode):
        self.rows_by_mode = rows_by_mode

    def list_trades(self, trade_mode):
        return list(self.rows_by_mode.get(trade_mode, []))


def test_core_routes_smoke() -> None:
    app = create_app({"TESTING": True, "WTF_CSRF_ENABLED": False})
    client = app.test_client()

    for route in ("/", "/apollo", "/journal", "/performance"):
        response = client.get(route, follow_redirects=True)
        assert response.status_code == 200


def test_removed_routes_stay_unavailable() -> None:
    app = create_app({"TESTING": True, "WTF_CSRF_ENABLED": False})
    client = app.test_client()

    for route in ("/kairos", "/research"):
        response = client.get(route, follow_redirects=False)
        assert response.status_code == 404


def test_talos_trade_mode_resolves_to_talos_system() -> None:
    trade = {"trade_mode": "talos", "system_name": "Apollo"}

    assert resolve_trade_system_name(trade) == "Talos"


def test_kairos_marker_overrides_apollo_label() -> None:
    trade = {
        "trade_mode": "real",
        "system_name": "Apollo",
        "notes_entry": "Prefilled from Kairos best candidate card.",
    }

    assert resolve_trade_system_name(trade) == "Kairos"


def test_journal_loader_keeps_only_retained_systems() -> None:
    store = StubTradeStore(
        {
            "real": [
                {"trade_number": 1, "trade_mode": "real", "system_name": "Apollo"},
                {"trade_number": 89, "trade_mode": "real", "system_name": "Apollo", "notes_entry": "Prefilled from Kairos best candidate card."},
                {"trade_number": 90, "trade_mode": "real", "system_name": "", "notes_entry": "Manual import without system marker."},
            ],
            "talos": [
                {"trade_number": 200, "trade_mode": "talos", "system_name": "Apollo"},
            ],
        }
    )

    rows = load_retained_trade_rows(store, "real")

    assert [row["trade_number"] for row in rows] == [1, 200]


def test_performance_loader_excludes_kairos_and_other_records() -> None:
    store = StubTradeStore(
        {
            "real": [
                {
                    "trade_mode": "real",
                    "system_name": "Apollo",
                    "status": "closed",
                    "entry_date": "2024-01-03",
                    "expiration_date": "2024-01-05",
                    "gross_pnl": 125.0,
                    "contracts": 1,
                },
                {
                    "trade_mode": "real",
                    "system_name": "Apollo",
                    "notes_entry": "Prefilled from Kairos best candidate card.",
                    "status": "closed",
                    "entry_date": "2024-01-04",
                    "expiration_date": "2024-01-05",
                    "gross_pnl": -25.0,
                    "contracts": 1,
                },
            ],
            "simulated": [
                {
                    "trade_mode": "simulated",
                    "system_name": "",
                    "status": "closed",
                    "entry_date": "2024-01-05",
                    "expiration_date": "2024-01-08",
                    "gross_pnl": 10.0,
                    "contracts": 1,
                },
            ],
            "talos": [
                {
                    "trade_mode": "talos",
                    "system_name": "Apollo",
                    "status": "closed",
                    "entry_date": "2024-01-08",
                    "expiration_date": "2024-01-10",
                    "gross_pnl": 55.0,
                    "contracts": 1,
                },
            ],
        }
    )

    records = PerformanceDashboardService(store).load_records()

    assert [record["system"] for record in records] == ["Apollo", "Talos"]