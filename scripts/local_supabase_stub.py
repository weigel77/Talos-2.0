"""Small local Supabase-compatible stub for hosted Talos browser verification.

This is intentionally minimal. It provides just enough Auth and PostgREST-like
behavior for local hosted-shell debugging when the real Supabase project
configuration is unavailable in the workspace.
"""

from __future__ import annotations

import json
import os
import threading
from copy import deepcopy
from pathlib import Path
from typing import Any

from flask import Flask, jsonify, request


DEFAULT_DATA_PATH = Path("instance") / "hosted_supabase_stub.json"
AUTO_ID_FIELDS = {
    "journal_trades": "id",
    "journal_trade_close_events": "id",
    "active_trade_alert_log": "id",
}


def build_seed_apollo_payload() -> dict[str, Any]:
    return {
        "title": "Apollo Gate 1 -- SPX Structure",
        "status": "Allowed",
        "status_class": "allowed",
        "run_timestamp": "Local hosted stub",
        "execution_source_label": "Hosted snapshot",
        "live_data_provider": "Schwab",
        "provider_name": "Schwab",
        "structure_grade": "Fortress-ready",
        "macro_grade": "Neutral",
        "trade_candidates_count": 1,
        "trade_candidates_valid_count": 1,
        "trade_candidates_count_label": "1 candidate",
        "next_market_day": "Next session",
        "spx_value": "5,300.00",
        "spx_as_of": "Stubbed",
        "vix_value": "14.20",
        "vix_as_of": "Stubbed",
        "option_chain_status": "Available",
        "option_chain_heading_date": "Stubbed next expiry",
        "reasons": [],
        "trade_candidates_items": [
            {
                "mode_key": "fortress",
                "mode_label": "Fortress",
                "mode_descriptor": "Talos 3.2 fortress-only candidate",
                "short_strike": "5280",
                "long_strike": "5270",
                "position_label": "Put Credit",
                "available": True,
                "premium_per_contract": "$1.15",
                "recommended_contract_size": "1",
                "distance_to_short": "20 pts",
                "em_multiple": "0.74x",
                "credit_efficiency": "11.5%",
                "total_premium": "$115",
                "premium_probability": "84%",
                "routine_loss": "$220",
                "routine_probability": "12%",
                "black_swan_loss": "$885",
                "tail_probability": "4%",
                "max_theoretical_risk": "$885",
                "max_probability": "4%",
                "risk_cap_status": "Within risk cap",
                "rationale": [
                    "Fortress profile only",
                    "Hosted Talos 3.2 verification snapshot",
                ],
                "exit_plan": [
                    "Monitor short strike pressure",
                    "Reduce or close if the buffer compresses",
                ],
                "diagnostics": [],
            }
        ],
    }


def build_seed_data() -> dict[str, list[dict[str, Any]]]:
    return {
        "hosted_runtime_state": [],
        "management_runtime_settings": [
            {
                "singleton_id": 1,
                "notifications_enabled": True,
                "last_morning_snapshot_date": None,
                "last_eod_summary_date": None,
                "last_background_run_at": None,
            }
        ],
        "journal_trades": [],
        "journal_trade_close_events": [],
        "active_trades": [],
        "active_trade_alert_log": [],
    }


class StubStore:
    def __init__(self, data_path: Path) -> None:
        self.data_path = data_path
        self._lock = threading.RLock()
        self._tables = self._load()

    def _load(self) -> dict[str, list[dict[str, Any]]]:
        if self.data_path.exists():
            try:
                payload = json.loads(self.data_path.read_text(encoding="utf-8"))
                if isinstance(payload, dict):
                    return {str(key): [dict(item) for item in value or []] for key, value in payload.items()}
            except Exception:
                pass
        data = build_seed_data()
        self._write(data)
        return data

    def _write(self, data: dict[str, list[dict[str, Any]]]) -> None:
        self.data_path.parent.mkdir(parents=True, exist_ok=True)
        self.data_path.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")

    def select(self, table: str, *, filters: dict[str, str], order: str | None, limit: int | None) -> list[dict[str, Any]]:
        with self._lock:
            rows = [deepcopy(row) for row in self._tables.get(table, [])]
            for key, raw_filter in (filters or {}).items():
                rows = [row for row in rows if _matches_filter(row.get(key), raw_filter)]
            if order:
                rows = _apply_order(rows, order)
            if limit is not None:
                rows = rows[: max(int(limit), 0)]
            return rows

    def insert(self, table: str, payload: dict[str, Any]) -> list[dict[str, Any]]:
        with self._lock:
            row = deepcopy(payload)
            id_field = AUTO_ID_FIELDS.get(table)
            if id_field and row.get(id_field) in {None, ""}:
                current_max = max((int(item.get(id_field) or 0) for item in self._tables.get(table, [])), default=0)
                row[id_field] = current_max + 1
            self._tables.setdefault(table, []).append(row)
            self._write(self._tables)
            return [deepcopy(row)]

    def update(self, table: str, payload: dict[str, Any], *, filters: dict[str, str]) -> list[dict[str, Any]]:
        with self._lock:
            updated: list[dict[str, Any]] = []
            rows = self._tables.get(table, [])
            for index, row in enumerate(rows):
                if all(_matches_filter(row.get(key), raw_filter) for key, raw_filter in (filters or {}).items()):
                    merged = deepcopy(row)
                    merged.update(deepcopy(payload))
                    rows[index] = merged
                    updated.append(deepcopy(merged))
            self._write(self._tables)
            return updated

    def delete(self, table: str, *, filters: dict[str, str]) -> None:
        with self._lock:
            rows = self._tables.get(table, [])
            self._tables[table] = [
                deepcopy(row)
                for row in rows
                if not all(_matches_filter(row.get(key), raw_filter) for key, raw_filter in (filters or {}).items())
            ]
            self._write(self._tables)


def _coerce_scalar(value: Any) -> Any:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    text = str(value).strip()
    if text.lower() in {"true", "false"}:
        return text.lower() == "true"
    try:
        if "." in text:
            return float(text)
        return int(text)
    except Exception:
        return text


def _matches_filter(value: Any, raw_filter: str) -> bool:
    text = str(raw_filter or "")
    if text.startswith("eq."):
        return _coerce_scalar(value) == _coerce_scalar(text[3:])
    if text.startswith("gte."):
        return str(value or "") >= text[4:]
    if text.startswith("lte."):
        return str(value or "") <= text[4:]
    if text.startswith("in.(") and text.endswith(")"):
        options = [item.strip() for item in text[4:-1].split(",") if item.strip()]
        coerced_options = {_coerce_scalar(item) for item in options}
        return _coerce_scalar(value) in coerced_options
    return str(value or "") == text


def _apply_order(rows: list[dict[str, Any]], order_spec: str) -> list[dict[str, Any]]:
    ordered = list(rows)
    specs = [item.strip() for item in str(order_spec or "").split(",") if item.strip()]
    for spec in reversed(specs):
        field, _, direction = spec.partition(".")
        descending = direction.lower().startswith("desc")
        ordered.sort(key=lambda row: (row.get(field) is None, row.get(field)), reverse=descending)
    return ordered


def create_app() -> Flask:
    app = Flask(__name__)
    data_path = Path(os.getenv("TALOS_SUPABASE_STUB_DATA") or DEFAULT_DATA_PATH)
    store = StubStore(data_path)

    @app.get("/health")
    def health() -> Any:
        return jsonify({"ok": True})

    @app.get("/auth/v1/settings")
    def auth_settings() -> Any:
        return jsonify({"disable_signup": True, "external": {}})

    @app.post("/auth/v1/token")
    def auth_token() -> Any:
        payload = request.get_json(silent=True) or {}
        email = str(payload.get("email") or "copilot-hosted-debug@example.com").strip().lower()
        return jsonify(
            {
                "access_token": "stub-access-token",
                "refresh_token": "stub-refresh-token",
                "token_type": "bearer",
                "expires_in": 3600,
                "user": {
                    "id": "stub-user",
                    "email": email,
                    "user_metadata": {"full_name": "Hosted Debug"},
                },
            }
        )

    @app.get("/auth/v1/user")
    def auth_user() -> Any:
        return jsonify(
            {
                "id": "stub-user",
                "email": "copilot-hosted-debug@example.com",
                "user_metadata": {"full_name": "Hosted Debug"},
            }
        )

    @app.get("/rest/v1/")
    def rest_root() -> Any:
        return jsonify({"swagger": "2.0", "info": {"title": "Talos local supabase stub"}})

    @app.route("/rest/v1/<path:table>", methods=["GET", "POST", "PATCH", "DELETE"])
    def rest_table(table: str) -> Any:
        normalized_table = str(table or "").strip("/")
        query = request.args.to_dict(flat=True)
        filters = {key: value for key, value in query.items() if key not in {"select", "order", "limit"}}
        limit = int(query["limit"]) if query.get("limit") else None

        if request.method == "GET":
            return jsonify(store.select(normalized_table, filters=filters, order=query.get("order"), limit=limit))
        if request.method == "POST":
            if normalized_table.startswith("rpc/"):
                return jsonify({"ok": True})
            payload = request.get_json(silent=True) or {}
            return jsonify(store.insert(normalized_table, payload))
        if request.method == "PATCH":
            payload = request.get_json(silent=True) or {}
            return jsonify(store.update(normalized_table, payload, filters=filters))
        store.delete(normalized_table, filters=filters)
        return ("", 204)

    return app


if __name__ == "__main__":
    application = create_app()
    host = os.getenv("TALOS_SUPABASE_STUB_HOST", "127.0.0.1")
    port = int(os.getenv("TALOS_SUPABASE_STUB_PORT", "54321"))
    application.run(host=host, port=port, debug=False)