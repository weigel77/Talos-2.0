"""SQLite-backed persistent trade storage for Horme."""

from __future__ import annotations

import json
import logging
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Optional
from zoneinfo import ZoneInfo

VALID_TRADE_MODES = {"real", "simulated", "talos"}
VALID_STATUSES = {"open", "closed", "expired", "cancelled"}
VALID_CLOSE_EVENT_TYPES = {"reduce", "close", "expire"}
VALID_CLOSE_METHODS = {"Reduce", "Close", "Expire", "Manage Trade Prefill"}
VALID_SYSTEM_NAMES = {"Apollo", "Talos", "Fortress"}
RETAINED_SYSTEM_NAMES = {"Apollo", "Talos"}
VALID_CANDIDATE_PROFILES = {"Legacy", "Aggressive", "Fortress", "Standard", "Prime", "Subprime"}
DEFAULT_SYSTEM_NAME = "Apollo"
UNKNOWN_SYSTEM_NAME = "Other"
DEFAULT_CANDIDATE_PROFILE = "Legacy"
CHICAGO_TZ = ZoneInfo("America/Chicago")
LEGACY_JOURNAL_NAME_DEFAULT = "Apollo Main"
JOURNAL_NAME_DEFAULT = "Shared Supabase Journal"
DISTANCE_SOURCE_ORIGINAL = "original"
DISTANCE_SOURCE_DERIVED = "derived"
DISTANCE_SOURCE_ESTIMATED = "estimated_fallback"
DISTANCE_SOURCE_UNRESOLVED = "unresolved"
EXPECTED_MOVE_SOURCE_ORIGINAL = "original"
EXPECTED_MOVE_SOURCE_RECOVERED_CANDIDATE = "recovered_candidate"
EXPECTED_MOVE_SOURCE_RECOVERED_SNAPSHOT = "recovered_snapshot"
EXPECTED_MOVE_SOURCE_ESTIMATED = "estimated_calibrated"
EXPECTED_MOVE_SOURCE_ESTIMATED_LEGACY = "estimated_fallback"
EXPECTED_MOVE_SOURCE_ATM_STRADDLE = "same_day_atm_straddle"
EXPECTED_MOVE_SOURCE_UNRESOLVED = "unresolved"
EXPECTED_MOVE_CONFIDENCE_HIGH = "high"
EXPECTED_MOVE_CONFIDENCE_LOW = "low"
EXPECTED_MOVE_CONFIDENCE_NONE = "none"
EXPECTED_MOVE_USAGE_ACTUAL = "actual_manual"
EXPECTED_MOVE_USAGE_ESTIMATED = "calibrated_estimated"
EXPECTED_MOVE_USAGE_EXCLUDED = "excluded"
MATERIAL_DISTANCE_DISCREPANCY_FLOOR = 2.0
MATERIAL_DISTANCE_DISCREPANCY_RATIO = 0.05
APOLLO_DISTANCE_LOOKBACK_DAYS = 7
EXPECTED_MOVE_ESTIMATE_DIVISOR = 16.0
EXPECTED_MOVE_CALIBRATION_FACTOR = 0.68
EXPECTED_MOVE_ESTIMATED_WEIGHT = 0.6
EXPECTED_MOVE_HISTORY_LOOKBACK_DAYS = 7
BLACK_SWAN_LOSS_THRESHOLD = 0.40
LOGGER = logging.getLogger(__name__)

DATE_FIELDS = {"trade_date", "expiration_date"}
DATETIME_FIELDS = {"entry_datetime", "exit_datetime"}
INTEGER_FIELDS = {"trade_number", "contracts"}
REAL_FIELDS = {
    "spx_at_entry",
    "vix_at_entry",
    "spx_entry",
    "vix_entry",
    "expected_move",
    "expected_move_raw_estimate",
    "expected_move_calibrated",
    "short_strike",
    "long_strike",
    "spread_width",
    "candidate_credit_estimate",
    "actual_entry_credit",
    "net_credit_per_contract",
    "credit_received",
    "actual_exit_value",
    "distance_to_short",
    "expected_move_used",
    "short_delta",
    "em_multiple_floor",
    "percent_floor",
    "actual_distance_to_short",
    "actual_em_multiple",
        "premium_per_contract",
        "total_premium",
        "max_theoretical_risk",
        "risk_efficiency",
        "target_em",
    "spx_at_exit",
    "gross_pnl",
    "pnl",
    "max_risk",
    "max_loss",
    "max_drawdown",
    "roi_on_risk",
    "hours_held",
}
DUPLICATE_SIGNATURE_FIELDS = (
    "trade_mode",
    "system_name",
    "journal_name",
    "candidate_profile",
    "status",
    "trade_date",
    "expiration_date",
    "underlying_symbol",
    "short_strike",
    "long_strike",
    "contracts",
    "actual_entry_credit",
    "actual_exit_value",
)
EDITABLE_FIELDS = {
    "trade_number",
    "trade_mode",
    "system_name",
    "journal_name",
    "system_version",
    "candidate_profile",
    "status",
    "trade_date",
    "entry_datetime",
    "expiration_date",
    "underlying_symbol",
    "spx_at_entry",
    "vix_at_entry",
    "structure_grade",
    "macro_grade",
    "expected_move",
    "expected_move_used",
    "expected_move_source",
    "option_type",
    "short_strike",
    "long_strike",
    "spread_width",
    "contracts",
    "candidate_credit_estimate",
    "actual_entry_credit",
    "net_credit_per_contract",
    "distance_to_short",
    "em_multiple_floor",
    "percent_floor",
    "boundary_rule_used",
    "actual_distance_to_short",
    "actual_em_multiple",
    "pass_type",
    "premium_per_contract",
    "total_premium",
    "max_theoretical_risk",
    "risk_efficiency",
    "target_em",
    "prefill_source",
    "automation_status",
    "fallback_used",
    "fallback_rule_name",
    "short_delta",
    "notes_entry",
    "exit_datetime",
    "spx_at_exit",
    "actual_exit_value",
    "close_method",
    "close_reason",
    "notes_exit",
}

SCHEMA = """
CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_number INTEGER,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    trade_mode TEXT NOT NULL CHECK (trade_mode IN ('real', 'simulated', 'talos')),
    system_name TEXT NOT NULL,
    journal_name TEXT NOT NULL DEFAULT 'Shared Supabase Journal',
    system_version TEXT,
    candidate_profile TEXT,
    status TEXT NOT NULL CHECK (status IN ('open', 'closed', 'expired', 'cancelled')),
    trade_date TEXT,
    entry_datetime TEXT,
    expiration_date TEXT,
    underlying_symbol TEXT,
    spx_at_entry REAL,
    vix_at_entry REAL,
    structure_grade TEXT,
    macro_grade TEXT,
    expected_move REAL,
    expected_move_used REAL,
    expected_move_raw_estimate REAL,
    expected_move_calibrated REAL,
    expected_move_confidence TEXT,
    option_type TEXT,
    short_strike REAL,
    long_strike REAL,
    spread_width REAL,
    contracts INTEGER,
    candidate_credit_estimate REAL,
    actual_entry_credit REAL,
    net_credit_per_contract REAL,
    distance_to_short REAL,
    distance_source TEXT,
    expected_move_source TEXT,
    em_multiple_floor REAL,
    percent_floor REAL,
    boundary_rule_used TEXT,
    actual_distance_to_short REAL,
    actual_em_multiple REAL,
    pass_type TEXT,
    premium_per_contract REAL,
    total_premium REAL,
    max_theoretical_risk REAL,
    total_max_loss REAL,
    risk_efficiency REAL,
    target_em REAL,
    prefill_source TEXT,
    automation_status TEXT,
    last_status TEXT,
    last_action_sent TEXT,
    last_alert_timestamp TEXT,
    fallback_used TEXT,
    fallback_rule_name TEXT,
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
    win_loss_result TEXT,
    system TEXT,
    spx_entry REAL,
    vix_entry REAL,
    structure TEXT,
    credit_received REAL,
    max_loss REAL,
    result TEXT,
    pnl REAL,
    max_drawdown REAL,
    exit_type TEXT,
    macro_flag TEXT
);

CREATE TABLE IF NOT EXISTS trade_close_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_id INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    event_type TEXT NOT NULL CHECK (event_type IN ('reduce', 'close', 'expire')),
    event_datetime TEXT,
    contracts_closed INTEGER NOT NULL,
    actual_exit_value REAL,
    spx_at_exit REAL,
    close_method TEXT,
    close_reason TEXT,
    notes_exit TEXT,
    FOREIGN KEY (trade_id) REFERENCES trades(id) ON DELETE CASCADE
);
"""

LEARNING_SCHEMA_COLUMNS = {
    "system": "TEXT",
    "spx_entry": "REAL",
    "vix_entry": "REAL",
    "structure": "TEXT",
    "credit_received": "REAL",
    "max_loss": "REAL",
    "result": "TEXT",
    "pnl": "REAL",
    "max_drawdown": "REAL",
    "exit_type": "TEXT",
    "macro_flag": "TEXT",
}


@dataclass
class TradeSummary:
    total_trades: int
    open_trades: int
    closed_trades: int
    total_pnl: float
    average_pnl: float
    win_count: int
    loss_count: int

    def as_dict(self) -> Dict[str, Any]:
        return {
            "total_trades": self.total_trades,
            "open_trades": self.open_trades,
            "closed_trades": self.closed_trades,
            "total_pnl": self.total_pnl,
            "average_pnl": self.average_pnl,
            "win_count": self.win_count,
            "loss_count": self.loss_count,
        }


class TradeStore:
    """Repository wrapper around the single Horme trades database."""

    def __init__(self, database_path: str | Path, market_data_service: Any | None = None) -> None:
        self.database_path = Path(database_path)
        self.market_data_service = market_data_service

    def initialize(self) -> None:
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as connection:
            connection.executescript(SCHEMA)
            self._apply_migrations(connection)
            connection.commit()

    def _apply_migrations(self, connection: sqlite3.Connection) -> None:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS trade_close_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_id INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                event_type TEXT NOT NULL CHECK (event_type IN ('reduce', 'close', 'expire')),
                event_datetime TEXT,
                contracts_closed INTEGER NOT NULL,
                actual_exit_value REAL,
                spx_at_exit REAL,
                close_method TEXT,
                close_reason TEXT,
                notes_exit TEXT,
                FOREIGN KEY (trade_id) REFERENCES trades(id) ON DELETE CASCADE
            )
            """
        )
        self._ensure_trade_mode_schema(connection)
        columns = {row["name"] for row in connection.execute("PRAGMA table_info(trades)").fetchall()}
        if "trade_number" not in columns:
            connection.execute("ALTER TABLE trades ADD COLUMN trade_number INTEGER")
        if "journal_name" not in columns:
            connection.execute(
                "ALTER TABLE trades ADD COLUMN journal_name TEXT NOT NULL DEFAULT 'Shared Supabase Journal'"
            )
        if "distance_source" not in columns:
            connection.execute("ALTER TABLE trades ADD COLUMN distance_source TEXT")
        if "expected_move_source" not in columns:
            connection.execute("ALTER TABLE trades ADD COLUMN expected_move_source TEXT")
        if "expected_move_used" not in columns:
            connection.execute("ALTER TABLE trades ADD COLUMN expected_move_used REAL")
        if "expected_move_raw_estimate" not in columns:
            connection.execute("ALTER TABLE trades ADD COLUMN expected_move_raw_estimate REAL")
        if "expected_move_calibrated" not in columns:
            connection.execute("ALTER TABLE trades ADD COLUMN expected_move_calibrated REAL")
        if "expected_move_confidence" not in columns:
            connection.execute("ALTER TABLE trades ADD COLUMN expected_move_confidence TEXT")
        if "em_multiple_floor" not in columns:
            connection.execute("ALTER TABLE trades ADD COLUMN em_multiple_floor REAL")
        if "percent_floor" not in columns:
            connection.execute("ALTER TABLE trades ADD COLUMN percent_floor REAL")
        if "boundary_rule_used" not in columns:
            connection.execute("ALTER TABLE trades ADD COLUMN boundary_rule_used TEXT")
        if "actual_distance_to_short" not in columns:
            connection.execute("ALTER TABLE trades ADD COLUMN actual_distance_to_short REAL")
        if "actual_em_multiple" not in columns:
            connection.execute("ALTER TABLE trades ADD COLUMN actual_em_multiple REAL")
        if "net_credit_per_contract" not in columns:
            connection.execute("ALTER TABLE trades ADD COLUMN net_credit_per_contract REAL")
        if "pass_type" not in columns:
            connection.execute("ALTER TABLE trades ADD COLUMN pass_type TEXT")
        if "premium_per_contract" not in columns:
            connection.execute("ALTER TABLE trades ADD COLUMN premium_per_contract REAL")
        if "total_premium" not in columns:
            connection.execute("ALTER TABLE trades ADD COLUMN total_premium REAL")
        if "max_theoretical_risk" not in columns:
            connection.execute("ALTER TABLE trades ADD COLUMN max_theoretical_risk REAL")
        if "total_max_loss" not in columns:
            connection.execute("ALTER TABLE trades ADD COLUMN total_max_loss REAL")
        if "risk_efficiency" not in columns:
            connection.execute("ALTER TABLE trades ADD COLUMN risk_efficiency REAL")
        if "target_em" not in columns:
            connection.execute("ALTER TABLE trades ADD COLUMN target_em REAL")
        if "prefill_source" not in columns:
            connection.execute("ALTER TABLE trades ADD COLUMN prefill_source TEXT")
        if "automation_status" not in columns:
            connection.execute("ALTER TABLE trades ADD COLUMN automation_status TEXT")
        if "last_status" not in columns:
            connection.execute("ALTER TABLE trades ADD COLUMN last_status TEXT")
        if "last_action_sent" not in columns:
            connection.execute("ALTER TABLE trades ADD COLUMN last_action_sent TEXT")
        if "last_alert_timestamp" not in columns:
            connection.execute("ALTER TABLE trades ADD COLUMN last_alert_timestamp TEXT")
        if "fallback_used" not in columns:
            connection.execute("ALTER TABLE trades ADD COLUMN fallback_used TEXT")
        if "fallback_rule_name" not in columns:
            connection.execute("ALTER TABLE trades ADD COLUMN fallback_rule_name TEXT")
        for column_name, column_type in LEARNING_SCHEMA_COLUMNS.items():
            if column_name not in columns:
                connection.execute(f"ALTER TABLE trades ADD COLUMN {column_name} {column_type}")
        connection.execute(
            "UPDATE trades SET journal_name = ? WHERE journal_name IS NULL OR TRIM(journal_name) = ''",
            (JOURNAL_NAME_DEFAULT,),
        )
        self._normalize_existing_rows(connection)
        connection.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_trades_trade_number ON trades(trade_number) WHERE trade_number IS NOT NULL"
        )

    def _ensure_trade_mode_schema(self, connection: sqlite3.Connection) -> None:
        row = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'trades'"
        ).fetchone()
        create_sql = str((dict(row) if row else {}).get("sql") or "")
        if not create_sql or "'talos'" in create_sql:
            return

        migration_sql = SCHEMA.split("CREATE TABLE IF NOT EXISTS trade_close_events", 1)[0]
        migration_sql = migration_sql.replace("CREATE TABLE IF NOT EXISTS trades", "CREATE TABLE trades__new", 1)
        connection.execute("PRAGMA foreign_keys = OFF")
        try:
            connection.execute("DROP TABLE IF EXISTS trades__new")
            connection.executescript(migration_sql)
            old_columns = [row["name"] for row in connection.execute("PRAGMA table_info(trades)").fetchall()]
            new_columns = [row["name"] for row in connection.execute("PRAGMA table_info(trades__new)").fetchall()]
            common_columns = [column for column in old_columns if column in new_columns]
            column_sql = ", ".join(common_columns)
            connection.execute(f"INSERT INTO trades__new ({column_sql}) SELECT {column_sql} FROM trades")
            connection.execute("DROP TABLE trades")
            connection.execute("ALTER TABLE trades__new RENAME TO trades")
        finally:
            connection.execute("PRAGMA foreign_keys = ON")

    def _normalize_existing_rows(self, connection: sqlite3.Connection) -> None:
        rows = connection.execute("SELECT * FROM trades ORDER BY id ASC").fetchall()
        next_number = 1
        for row in rows:
            trade_record = dict(row)
            existing_trade_number = row["trade_number"]
            normalized_trade_number = int(existing_trade_number) if existing_trade_number not in {None, ""} else next_number
            next_number = max(next_number, normalized_trade_number + 1)

            normalized_system_name = normalize_system_name(row["system_name"])
            normalized_profile = resolve_trade_candidate_profile(trade_record)
            expected_move_metadata = resolve_trade_expected_move(
                trade_record,
                snapshot_lookup=self._lookup_expected_move_preview,
            )
            normalized_trade = {
                **trade_record,
                "expected_move": expected_move_metadata.get("value"),
                "expected_move_used": expected_move_metadata.get("used_value"),
                "expected_move_source": expected_move_metadata.get("source"),
                "expected_move_raw_estimate": expected_move_metadata.get("raw_estimate"),
                "expected_move_calibrated": expected_move_metadata.get("calibrated_value"),
                "expected_move_confidence": expected_move_metadata.get("confidence"),
            }
            distance_metadata = resolve_trade_distance(normalized_trade)
            recalculated = calculate_trade_metrics(normalized_trade, distance_metadata=distance_metadata)
            learning_fields = build_learning_trade_fields(
                {
                    **normalized_trade,
                    **recalculated,
                    "distance_source": distance_metadata.get("source"),
                    "system_name": normalized_system_name,
                    "candidate_profile": normalized_profile,
                },
                distance_metadata=distance_metadata,
            )
            credit_model = resolve_trade_credit_model(normalized_trade)
            connection.execute(
                """
                UPDATE trades
                SET trade_number = ?,
                    system_name = ?,
                    candidate_profile = ?,
                    expected_move = ?,
                    expected_move_used = ?,
                    expected_move_source = ?,
                    expected_move_raw_estimate = ?,
                    expected_move_calibrated = ?,
                    expected_move_confidence = ?,
                    distance_to_short = ?,
                    distance_source = ?,
                    win_loss_result = ?,
                    gross_pnl = ?,
                    max_risk = ?,
                    roi_on_risk = ?,
                    hours_held = ?,
                    system = ?,
                    spx_entry = ?,
                    vix_entry = ?,
                    structure = ?,
                    credit_received = ?,
                    max_loss = ?,
                    result = ?,
                    pnl = ?,
                    max_drawdown = ?,
                    exit_type = ?,
                    macro_flag = ?
                WHERE id = ?
                """,
                (
                    normalized_trade_number,
                    normalized_system_name,
                    normalized_profile,
                    expected_move_metadata.get("value"),
                    expected_move_metadata.get("used_value"),
                    expected_move_metadata.get("source"),
                    expected_move_metadata.get("raw_estimate"),
                    expected_move_metadata.get("calibrated_value"),
                    expected_move_metadata.get("confidence"),
                    learning_fields.get("distance_to_short"),
                    distance_metadata.get("source"),
                    recalculated.get("win_loss_result"),
                    recalculated.get("gross_pnl"),
                    recalculated.get("max_risk"),
                    recalculated.get("roi_on_risk"),
                    recalculated.get("hours_held"),
                    learning_fields.get("system"),
                    learning_fields.get("spx_entry"),
                    learning_fields.get("vix_entry"),
                    learning_fields.get("structure"),
                    learning_fields.get("credit_received"),
                    learning_fields.get("max_loss"),
                    learning_fields.get("result"),
                    learning_fields.get("pnl"),
                    learning_fields.get("max_drawdown"),
                    learning_fields.get("exit_type"),
                    learning_fields.get("macro_flag"),
                    row["id"],
                ),
            )
            # Additional fields for credit model
            connection.execute(
                """
                UPDATE trades
                SET candidate_credit_estimate = ?,
                    actual_entry_credit = ?,
                    net_credit_per_contract = ?,
                    premium_per_contract = ?,
                    total_premium = ?,
                    max_theoretical_risk = ?,
                    total_max_loss = ?
                WHERE id = ?
                """,
                (
                    credit_model.get("candidate_credit_estimate"),
                    credit_model.get("actual_entry_credit"),
                    credit_model.get("net_credit_per_contract"),
                    credit_model.get("premium_per_contract"),
                    credit_model.get("total_premium"),
                    credit_model.get("max_theoretical_risk"),
                    credit_model.get("max_theoretical_risk"),
                    row["id"],
                ),
            )
            self._backfill_legacy_close_events(connection, trade_record)

    def backfill_total_max_loss(self) -> Dict[str, Any]:
        report = {
            "processed_trade_count": 0,
            "updated_trade_count": 0,
            "unresolved_trade_count": 0,
            "unresolved_trades": [],
        }
        with closing(self._connect()) as connection:
            rows = connection.execute("SELECT * FROM trades ORDER BY id ASC").fetchall()
            for row in rows:
                trade = dict(row)
                report["processed_trade_count"] += 1
                total_max_loss = resolve_trade_credit_model(trade).get("max_theoretical_risk")
                if total_max_loss is None:
                    report["unresolved_trade_count"] += 1
                    if len(report["unresolved_trades"]) < 25:
                        report["unresolved_trades"].append(
                            {
                                "trade_id": trade.get("id"),
                                "trade_number": trade.get("trade_number"),
                                "system_name": trade.get("system_name"),
                                "trade_date": trade.get("trade_date"),
                            }
                        )
                    continue

                rounded_total_max_loss = round(total_max_loss, 2)
                existing_total_max_loss = to_float(
                    trade.get("total_max_loss")
                    if trade.get("total_max_loss") is not None
                    else trade.get("max_theoretical_risk")
                )
                if existing_total_max_loss == rounded_total_max_loss:
                    continue

                connection.execute(
                    """
                    UPDATE trades
                    SET total_max_loss = ?,
                        max_theoretical_risk = ?,
                        max_loss = ?,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        rounded_total_max_loss,
                        rounded_total_max_loss,
                        rounded_total_max_loss,
                        current_timestamp(),
                        trade.get("id"),
                    ),
                )
                report["updated_trade_count"] += 1
            connection.commit()
        return report

    def list_trades(self, trade_mode: str) -> list[Dict[str, Any]]:
        with closing(self._connect()) as connection:
            rows = connection.execute(
                "SELECT * FROM trades WHERE trade_mode = ? ORDER BY COALESCE(entry_datetime, trade_date, created_at) DESC, id DESC",
                (normalize_trade_mode(trade_mode),),
            ).fetchall()
            return [self._attach_trade_state(connection, dict(row)) for row in rows]

    def get_trade(self, trade_id: int) -> Optional[Dict[str, Any]]:
        with closing(self._connect()) as connection:
            row = connection.execute("SELECT * FROM trades WHERE id = ?", (trade_id,)).fetchone()
            return self._attach_trade_state(connection, dict(row)) if row else None

    def get_trade_by_number(self, trade_number: int) -> Optional[Dict[str, Any]]:
        with closing(self._connect()) as connection:
            row = connection.execute("SELECT * FROM trades WHERE trade_number = ?", (trade_number,)).fetchone()
            return self._attach_trade_state(connection, dict(row)) if row else None

    def next_trade_number(self) -> int:
        with closing(self._connect()) as connection:
            return self._next_trade_number(connection)

    def create_trade(self, payload: Dict[str, Any]) -> int:
        normalized = normalize_trade_payload(
            payload,
            historical_price_lookup=self._lookup_prior_expiration_close,
            expected_move_snapshot_lookup=self._lookup_expected_move_preview,
        )
        timestamp = current_timestamp()
        normalized["created_at"] = timestamp
        normalized["updated_at"] = timestamp

        with closing(self._connect()) as connection:
            trade_number = normalized.get("trade_number")
            if trade_number is None:
                normalized["trade_number"] = self._next_trade_number(connection)
            else:
                self._ensure_trade_number_available(connection, trade_number)
            columns = list(normalized.keys())
            placeholders = ", ".join("?" for _ in columns)
            sql = f"INSERT INTO trades ({', '.join(columns)}) VALUES ({placeholders})"
            cursor = connection.execute(sql, [normalized[column] for column in columns])
            connection.commit()
            return int(cursor.lastrowid)

    def update_trade(self, trade_id: int, payload: Dict[str, Any]) -> None:
        existing = self.get_trade(trade_id)
        if not existing:
            raise ValueError("Trade not found.")
        close_events_payload = payload.get("close_events") if "close_events" in payload else None
        normalized = normalize_trade_payload(
            payload,
            existing=existing,
            historical_price_lookup=self._lookup_prior_expiration_close,
            expected_move_snapshot_lookup=self._lookup_expected_move_preview,
        )
        normalized["updated_at"] = current_timestamp()
        if close_events_payload is not None:
            normalized.update(
                {
                    "status": "cancelled" if normalized.get("status") == "cancelled" else "open",
                    "exit_datetime": None,
                    "spx_at_exit": None,
                    "actual_exit_value": None,
                    "close_method": None,
                }
            )

        with closing(self._connect()) as connection:
            trade_number = normalized.get("trade_number") or existing.get("trade_number")
            if trade_number is None:
                trade_number = self._next_trade_number(connection)
            self._ensure_trade_number_available(connection, int(trade_number), exclude_id=trade_id)
            normalized["trade_number"] = int(trade_number)
            synchronized_events = None
            if close_events_payload is not None:
                synchronized_events = self._normalize_close_events_payload(
                    close_events_payload,
                    trade_contracts=normalized.get("contracts"),
                )
            assignments = ", ".join(f"{column} = ?" for column in normalized.keys())
            values = [normalized[column] for column in normalized.keys()] + [trade_id]
            connection.execute(f"UPDATE trades SET {assignments} WHERE id = ?", values)
            if synchronized_events is not None:
                self._replace_close_events(connection, trade_id, synchronized_events)
                self._refresh_trade_after_close_events(connection, trade_id)
            connection.commit()

    def delete_trade(self, trade_id: int) -> None:
        with closing(self._connect()) as connection:
            connection.execute("DELETE FROM trades WHERE id = ?", (trade_id,))
            connection.commit()

    def reduce_trade(self, trade_id: int, payload: Dict[str, Any]) -> None:
        with closing(self._connect()) as connection:
            row = connection.execute("SELECT * FROM trades WHERE id = ?", (trade_id,)).fetchone()
            if not row:
                raise ValueError("Trade not found.")
            trade = self._attach_trade_state(connection, dict(row))
            if trade.get("derived_status_raw") in {"closed", "expired", "cancelled"}:
                raise ValueError("Only open trades can be reduced.")

            remaining_contracts = int(trade.get("remaining_contracts") or 0)
            contracts_closed = to_int(payload.get("contracts_closed"))
            actual_exit_value = to_float(payload.get("actual_exit_value"))
            if contracts_closed is None or contracts_closed <= 0:
                raise ValueError("Reduce quantity must be at least 1 contract.")
            if remaining_contracts <= 0 or contracts_closed > remaining_contracts:
                raise ValueError("Reduce quantity cannot exceed the remaining open contracts.")
            if actual_exit_value is None or actual_exit_value < 0:
                raise ValueError("Reduce exit value must be zero or greater.")

            self._insert_close_event(
                connection,
                trade_id=trade_id,
                event_type="reduce",
                contracts_closed=contracts_closed,
                actual_exit_value=actual_exit_value,
                event_datetime=payload.get("event_datetime") or current_timestamp(),
                spx_at_exit=payload.get("spx_at_exit"),
                close_method=payload.get("close_method") or "Reduce",
                close_reason=payload.get("close_reason") or f"Reduced {contracts_closed} contract{'s' if contracts_closed != 1 else ''}",
                notes_exit=payload.get("notes_exit") or "",
            )
            self._refresh_trade_after_close_events(connection, trade_id)
            connection.commit()

    def expire_trade(self, trade_id: int, payload: Dict[str, Any] | None = None) -> None:
        payload = payload or {}
        with closing(self._connect()) as connection:
            row = connection.execute("SELECT * FROM trades WHERE id = ?", (trade_id,)).fetchone()
            if not row:
                raise ValueError("Trade not found.")
            trade = self._attach_trade_state(connection, dict(row))
            if trade.get("derived_status_raw") in {"closed", "expired", "cancelled"}:
                raise ValueError("Only open trades can be expired.")

            remaining_contracts = int(trade.get("remaining_contracts") or 0)
            if remaining_contracts <= 0:
                raise ValueError("Trade has no remaining open contracts to expire.")

            actual_exit_value = payload.get("actual_exit_value")
            if actual_exit_value in {None, ""}:
                actual_exit_value = 0.0
            actual_exit_value = to_float(actual_exit_value)
            if actual_exit_value is None or actual_exit_value < 0:
                raise ValueError("Expire exit value must be zero or greater.")

            self._insert_close_event(
                connection,
                trade_id=trade_id,
                event_type="expire",
                contracts_closed=remaining_contracts,
                actual_exit_value=actual_exit_value,
                event_datetime=payload.get("event_datetime") or current_timestamp(),
                spx_at_exit=payload.get("spx_at_exit"),
                close_method=payload.get("close_method") or "Expire",
                close_reason=payload.get("close_reason") or "Expired Worthless",
                notes_exit=payload.get("notes_exit") or "",
            )
            self._refresh_trade_after_close_events(connection, trade_id)
            connection.commit()

    def summarize(self, trade_mode: str) -> Dict[str, Any]:
        rows = self.list_trades(trade_mode)
        total_pnl = sum(float(row.get("gross_pnl") or 0.0) for row in rows)
        pnl_values = [float(row.get("gross_pnl") or 0.0) for row in rows if row.get("gross_pnl") is not None]
        return TradeSummary(
            total_trades=len(rows),
            open_trades=sum(1 for row in rows if row.get("derived_status_raw") in {"open", "reduced"}),
            closed_trades=sum(1 for row in rows if row.get("derived_status_raw") not in {"open", "reduced"}),
            total_pnl=total_pnl,
            average_pnl=(sum(pnl_values) / len(pnl_values)) if pnl_values else 0.0,
            win_count=sum(1 for row in rows if row.get("win_loss_result") == "Win"),
            loss_count=sum(1 for row in rows if row.get("win_loss_result") in {"Loss", "Black Swan"}),
        ).as_dict()

    def build_real_trade_outcome_profile(self) -> Dict[str, Any]:
        return build_real_trade_outcome_profile(self.list_trades("real"))

    def backfill_distance_sources(self) -> Dict[str, Any]:
        report = {
            "processed_trade_count": 0,
            "updated_trade_count": 0,
            "source_counts": {
                DISTANCE_SOURCE_ORIGINAL: 0,
                DISTANCE_SOURCE_DERIVED: 0,
                DISTANCE_SOURCE_ESTIMATED: 0,
                DISTANCE_SOURCE_UNRESOLVED: 0,
            },
            "material_discrepancy_count": 0,
            "material_discrepancies": [],
        }
        with closing(self._connect()) as connection:
            rows = connection.execute("SELECT * FROM trades ORDER BY id ASC").fetchall()
            for row in rows:
                trade = dict(row)
                report["processed_trade_count"] += 1
                distance_metadata = resolve_trade_distance(
                    trade,
                    historical_price_lookup=self._lookup_prior_expiration_close,
                )
                source = str(distance_metadata.get("source") or DISTANCE_SOURCE_UNRESOLVED)
                report["source_counts"][source] = report["source_counts"].get(source, 0) + 1

                if distance_metadata.get("discrepancy_is_material"):
                    report["material_discrepancy_count"] += 1
                    if len(report["material_discrepancies"]) < 20:
                        report["material_discrepancies"].append(
                            {
                                "trade_id": trade.get("id"),
                                "trade_number": trade.get("trade_number"),
                                "stored_distance": distance_metadata.get("stored_value"),
                                "derived_distance": distance_metadata.get("derived_value"),
                                "selected_distance": distance_metadata.get("value"),
                                "selected_source": source,
                                "discrepancy_points": distance_metadata.get("discrepancy_points"),
                            }
                        )

                existing_distance = to_float(trade.get("distance_to_short"))
                existing_source = normalize_distance_source(trade.get("distance_source"))
                new_distance = distance_metadata.get("value")
                if new_distance is not None:
                    new_distance = round(new_distance, 2)
                if existing_distance == new_distance and existing_source == source:
                    continue

                connection.execute(
                    """
                    UPDATE trades
                    SET distance_to_short = ?,
                        distance_source = ?,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        new_distance,
                        source,
                        current_timestamp(),
                        trade.get("id"),
                    ),
                )
                report["updated_trade_count"] += 1
            connection.commit()
        return report

    def backfill_expected_move_sources(self) -> Dict[str, Any]:
        report = {
            "processed_trade_count": 0,
            "updated_trade_count": 0,
            "source_counts": {
                EXPECTED_MOVE_SOURCE_ORIGINAL: 0,
                EXPECTED_MOVE_SOURCE_RECOVERED_CANDIDATE: 0,
                EXPECTED_MOVE_SOURCE_RECOVERED_SNAPSHOT: 0,
                EXPECTED_MOVE_SOURCE_ESTIMATED: 0,
                EXPECTED_MOVE_SOURCE_ATM_STRADDLE: 0,
                EXPECTED_MOVE_SOURCE_UNRESOLVED: 0,
            },
            "unresolved_trade_count": 0,
            "unresolved_trades": [],
        }
        with closing(self._connect()) as connection:
            rows = connection.execute("SELECT * FROM trades ORDER BY id ASC").fetchall()
            for row in rows:
                trade = dict(row)
                report["processed_trade_count"] += 1
                expected_move_metadata = resolve_trade_expected_move(
                    trade,
                    snapshot_lookup=self._lookup_expected_move_preview,
                    historical_context_lookup=self._lookup_historical_expected_move_context,
                )
                source = str(expected_move_metadata.get("source") or EXPECTED_MOVE_SOURCE_UNRESOLVED)
                report["source_counts"][source] = report["source_counts"].get(source, 0) + 1
                if source == EXPECTED_MOVE_SOURCE_UNRESOLVED:
                    report["unresolved_trade_count"] += 1
                    if len(report["unresolved_trades"]) < 25:
                        report["unresolved_trades"].append(
                            {
                                "trade_id": trade.get("id"),
                                "trade_number": trade.get("trade_number"),
                                "system_name": trade.get("system_name"),
                                "trade_date": trade.get("trade_date"),
                                "expiration_date": trade.get("expiration_date"),
                            }
                        )

                existing_expected_move = to_float(trade.get("expected_move"))
                existing_expected_move_used = to_float(trade.get("expected_move_used"))
                existing_source = normalize_expected_move_source(trade.get("expected_move_source"))
                existing_raw_estimate = to_float(trade.get("expected_move_raw_estimate"))
                existing_calibrated = to_float(trade.get("expected_move_calibrated"))
                existing_confidence = str(trade.get("expected_move_confidence") or EXPECTED_MOVE_CONFIDENCE_NONE).strip().lower()
                new_expected_move = expected_move_metadata.get("value")
                new_expected_move_used = expected_move_metadata.get("used_value")
                new_raw_estimate = expected_move_metadata.get("raw_estimate")
                new_calibrated = expected_move_metadata.get("calibrated_value")
                new_confidence = str(expected_move_metadata.get("confidence") or EXPECTED_MOVE_CONFIDENCE_NONE)
                if new_expected_move is not None:
                    new_expected_move = round(new_expected_move, 2)
                if new_expected_move_used is not None:
                    new_expected_move_used = round(new_expected_move_used, 2)
                if new_raw_estimate is not None:
                    new_raw_estimate = round(new_raw_estimate, 2)
                if new_calibrated is not None:
                    new_calibrated = round(new_calibrated, 2)
                if (
                    existing_expected_move == new_expected_move
                    and existing_expected_move_used == new_expected_move_used
                    and existing_source == source
                    and existing_raw_estimate == new_raw_estimate
                    and existing_calibrated == new_calibrated
                    and existing_confidence == new_confidence
                ):
                    continue

                connection.execute(
                    """
                    UPDATE trades
                    SET expected_move = ?,
                        expected_move_used = ?,
                        expected_move_source = ?,
                        expected_move_raw_estimate = ?,
                        expected_move_calibrated = ?,
                        expected_move_confidence = ?,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        new_expected_move,
                        new_expected_move_used,
                        source,
                        new_raw_estimate,
                        new_calibrated,
                        new_confidence,
                        current_timestamp(),
                        trade.get("id"),
                    ),
                )
                report["updated_trade_count"] += 1
            connection.commit()
        return report

    def _lookup_prior_expiration_close(self, values: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if self.market_data_service is None or not is_apollo_trade(values):
            return None

        expiration_date = parse_date_value(values.get("expiration_date"))
        if expiration_date is None:
            return None

        lookup_end = expiration_date - timedelta(days=1)
        lookup_start = lookup_end - timedelta(days=APOLLO_DISTANCE_LOOKBACK_DAYS)
        if lookup_end < lookup_start:
            return None

        try:
            history_frame = self.market_data_service.get_history_with_changes(
                "^GSPC",
                lookup_start,
                lookup_end,
                query_type="apollo_distance_backfill",
            )
        except Exception as exc:  # pragma: no cover - provider/network variability
            LOGGER.warning("Apollo distance fallback lookup failed for expiration %s: %s", expiration_date, exc)
            return None

        if history_frame is None or getattr(history_frame, "empty", True):
            return None

        for _, row in history_frame.sort_values("Date").iloc[::-1].iterrows():
            reference_date = row.get("Date")
            if hasattr(reference_date, "date"):
                reference_date = reference_date.date()
            close_value = to_float(row.get("Close"))
            if reference_date is None or close_value is None or reference_date >= expiration_date:
                continue
            return {
                "reference_price": close_value,
                "reference_date": reference_date.isoformat() if hasattr(reference_date, "isoformat") else str(reference_date),
            }
        return None

    def _lookup_expected_move_preview(self, values: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        preview_dir = self.database_path.parent / "trade_import_previews"
        if not preview_dir.exists():
            return None

        target_signature = build_expected_move_lookup_signature(values)
        if target_signature is None:
            return None

        for preview_path in sorted(preview_dir.glob("*.json")):
            try:
                payload = json.loads(preview_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError, TypeError, ValueError):
                continue
            for row in payload.get("importable_rows") or []:
                if not isinstance(row, dict):
                    continue
                if build_expected_move_lookup_signature(row) != target_signature:
                    continue
                preview_expected_move = first_float(row.get("expected_move"), row.get("daily_move_anchor"))
                if is_valid_expected_move(preview_expected_move):
                    return {
                        "expected_move": preview_expected_move,
                        "preview_file": preview_path.name,
                    }
        return None

    def _lookup_historical_expected_move_context(self, values: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if self.market_data_service is None:
            return None

        target_date = determine_expected_move_reference_date(values)
        if target_date is None:
            return None

        spx_close = self._lookup_historical_close("^GSPC", target_date, query_type="expected_move_backfill_spx")
        vix_close = self._lookup_historical_close("^VIX", target_date, query_type="expected_move_backfill_vix")
        if spx_close is None and vix_close is None:
            return None
        return {
            "reference_spx": spx_close.get("close") if spx_close else None,
            "reference_vix": vix_close.get("close") if vix_close else None,
            "reference_spx_date": spx_close.get("reference_date") if spx_close else None,
            "reference_vix_date": vix_close.get("reference_date") if vix_close else None,
        }

    def _lookup_historical_close(self, ticker: str, target_date: Any, *, query_type: str) -> Optional[Dict[str, Any]]:
        reference_date = parse_date_value(target_date)
        if reference_date is None:
            return None

        lookup_start = reference_date - timedelta(days=EXPECTED_MOVE_HISTORY_LOOKBACK_DAYS)
        try:
            history_frame = self.market_data_service.get_history_with_changes(
                ticker,
                lookup_start,
                reference_date,
                query_type=query_type,
            )
        except Exception as exc:  # pragma: no cover - provider/network variability
            LOGGER.warning("Expected move historical lookup failed for %s on %s: %s", ticker, reference_date, exc)
            return None

        if history_frame is None or getattr(history_frame, "empty", True):
            return None

        for _, row in history_frame.sort_values("Date").iloc[::-1].iterrows():
            row_date = row.get("Date")
            if hasattr(row_date, "date"):
                row_date = row_date.date()
            close_value = to_float(row.get("Close"))
            if row_date is None or close_value is None or row_date > reference_date:
                continue
            return {
                "close": close_value,
                "reference_date": row_date.isoformat() if hasattr(row_date, "isoformat") else str(row_date),
            }
        return None

    def find_recent_duplicate(self, payload: Dict[str, Any], window_seconds: int = 15) -> Optional[Dict[str, Any]]:
        normalized = normalize_trade_payload(payload)
        threshold = timestamp_seconds_ago(window_seconds)
        with closing(self._connect()) as connection:
            row = connection.execute(
                """
                SELECT *
                FROM trades
                WHERE created_at >= ?
                  AND trade_mode = ?
                  AND system_name = ?
                  AND journal_name = ?
                  AND COALESCE(candidate_profile, '') = COALESCE(?, '')
                  AND COALESCE(underlying_symbol, '') = COALESCE(?, '')
                  AND COALESCE(short_strike, -1) = COALESCE(?, -1)
                  AND COALESCE(long_strike, -1) = COALESCE(?, -1)
                  AND COALESCE(contracts, -1) = COALESCE(?, -1)
                  AND status = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (
                    threshold,
                    normalized.get("trade_mode"),
                    normalized.get("system_name"),
                    normalized.get("journal_name"),
                    normalized.get("candidate_profile"),
                    normalized.get("underlying_symbol"),
                    normalized.get("short_strike"),
                    normalized.get("long_strike"),
                    normalized.get("contracts"),
                    normalized.get("status"),
                ),
            ).fetchone()
        return dict(row) if row else None

    def find_duplicate_trade(self, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        normalized = normalize_trade_payload(payload)
        target_signature = build_trade_duplicate_signature(normalized, already_normalized=True)
        with closing(self._connect()) as connection:
            rows = connection.execute(
                "SELECT * FROM trades WHERE trade_mode = ? ORDER BY id DESC",
                (normalized["trade_mode"],),
            ).fetchall()

        for row in rows:
            candidate = dict(row)
            if build_trade_duplicate_signature(candidate, already_normalized=True) == target_signature:
                return candidate
        return None

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database_path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    def _list_close_events(self, connection: sqlite3.Connection, trade_id: int) -> list[Dict[str, Any]]:
        rows = connection.execute(
            "SELECT * FROM trade_close_events WHERE trade_id = ? ORDER BY COALESCE(event_datetime, created_at) ASC, id ASC",
            (trade_id,),
        ).fetchall()
        return [dict(row) for row in rows]

    def _attach_trade_state(self, connection: sqlite3.Connection, trade: Dict[str, Any]) -> Dict[str, Any]:
        if not trade:
            return trade
        events = self._list_close_events(connection, int(trade.get("id"))) if trade.get("id") is not None else []
        summary = summarize_trade_close_events(trade, events)
        trade.update(summary)
        trade["close_events"] = events
        return trade

    def _insert_close_event(
        self,
        connection: sqlite3.Connection,
        *,
        trade_id: int,
        event_type: str,
        contracts_closed: int,
        actual_exit_value: float,
        event_datetime: Any,
        spx_at_exit: Any,
        close_method: Any,
        close_reason: Any,
        notes_exit: Any,
    ) -> None:
        normalized_event_type = str(event_type or "").strip().lower()
        if normalized_event_type not in VALID_CLOSE_EVENT_TYPES:
            raise ValueError("Close event type must be reduce, close, or expire.")
        parsed_event_datetime = parse_datetime_value(event_datetime)
        connection.execute(
            """
            INSERT INTO trade_close_events (
                trade_id, created_at, event_type, event_datetime, contracts_closed, actual_exit_value,
                spx_at_exit, close_method, close_reason, notes_exit
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                trade_id,
                current_timestamp(),
                normalized_event_type,
                parsed_event_datetime.isoformat(timespec="minutes") if parsed_event_datetime else None,
                int(contracts_closed),
                float(actual_exit_value),
                to_float(spx_at_exit),
                str(close_method or "").strip() or None,
                str(close_reason or "").strip() or None,
                str(notes_exit or "").strip() or None,
            ),
        )

    def _normalize_close_events_payload(
        self,
        close_events: Any,
        *,
        trade_contracts: Any,
    ) -> list[Dict[str, Any]]:
        original_contracts = to_int(trade_contracts)
        if original_contracts is None or original_contracts <= 0:
            if close_events:
                raise ValueError("Contracts must be set before saving close events.")
            return []

        normalized_rows: list[Dict[str, Any]] = []
        total_closed = 0
        for index, row in enumerate(close_events or [], start=1):
            event_id = to_int((row or {}).get("id"))
            contracts_raw = (row or {}).get("contracts_closed")
            exit_value_raw = (row or {}).get("actual_exit_value")
            method_raw = (row or {}).get("close_method")
            if event_id is None and all(value in {None, ""} for value in (contracts_raw, exit_value_raw, method_raw)):
                continue

            contracts_closed = to_int(contracts_raw)
            if contracts_closed is None or contracts_closed <= 0:
                raise ValueError(f"Close event #{index}: Contracts closed must be a positive integer.")

            close_method = normalize_close_method(method_raw)
            actual_exit_value = 0.0 if close_method == "Expire" and exit_value_raw in {None, ""} else to_float(exit_value_raw)
            if actual_exit_value is None:
                raise ValueError(f"Close event #{index}: Close credit is required.")
            if actual_exit_value < 0:
                raise ValueError(f"Close event #{index}: Close credit must be zero or greater.")

            total_closed += contracts_closed
            normalized_rows.append(
                {
                    "id": event_id,
                    "contracts_closed": contracts_closed,
                    "actual_exit_value": actual_exit_value,
                    "event_datetime": (row or {}).get("event_datetime"),
                    "close_method": close_method,
                    "notes_exit": str((row or {}).get("notes_exit") or "").strip(),
                    "event_type": derive_close_event_type(close_method),
                }
            )

        if total_closed > original_contracts:
            raise ValueError("The total closed quantity cannot exceed the original contracts.")
        return normalized_rows

    def _replace_close_events(self, connection: sqlite3.Connection, trade_id: int, close_events: list[Dict[str, Any]]) -> None:
        existing_events = {
            int(row["id"]): dict(row)
            for row in connection.execute("SELECT * FROM trade_close_events WHERE trade_id = ?", (trade_id,)).fetchall()
        }
        retained_ids: set[int] = set()

        for row in close_events:
            event_id = row.get("id")
            if event_id is not None:
                existing_row = existing_events.get(int(event_id))
                if not existing_row:
                    raise ValueError("One or more close events could not be matched to this trade.")
                retained_ids.add(int(event_id))
                connection.execute(
                    """
                    UPDATE trade_close_events
                    SET event_type = ?,
                        event_datetime = ?,
                        contracts_closed = ?,
                        actual_exit_value = ?,
                        close_method = ?,
                        close_reason = ?,
                        notes_exit = ?
                    WHERE id = ? AND trade_id = ?
                    """,
                    (
                        row["event_type"],
                        parse_datetime_value(row.get("event_datetime")).isoformat(timespec="minutes") if parse_datetime_value(row.get("event_datetime")) else existing_row.get("event_datetime"),
                        row["contracts_closed"],
                        row["actual_exit_value"],
                        row["close_method"],
                        default_close_reason(row["close_method"], row["contracts_closed"]),
                        row.get("notes_exit") or existing_row.get("notes_exit") or "",
                        int(event_id),
                        trade_id,
                    ),
                )
                continue

            self._insert_close_event(
                connection,
                trade_id=trade_id,
                event_type=row["event_type"],
                contracts_closed=row["contracts_closed"],
                actual_exit_value=row["actual_exit_value"],
                event_datetime=row.get("event_datetime") or current_timestamp(),
                spx_at_exit=None,
                close_method=row["close_method"],
                close_reason=default_close_reason(row["close_method"], row["contracts_closed"]),
                notes_exit=row.get("notes_exit") or "",
            )

        for event_id in existing_events:
            if event_id not in retained_ids:
                connection.execute("DELETE FROM trade_close_events WHERE id = ? AND trade_id = ?", (event_id, trade_id))

    def _refresh_trade_after_close_events(self, connection: sqlite3.Connection, trade_id: int) -> None:
        row = connection.execute("SELECT * FROM trades WHERE id = ?", (trade_id,)).fetchone()
        if not row:
            return
        trade = dict(row)
        events = self._list_close_events(connection, trade_id)
        summary = summarize_trade_close_events(trade, events)
        connection.execute(
            """
            UPDATE trades
            SET updated_at = ?,
                status = ?,
                exit_datetime = ?,
                spx_at_exit = ?,
                actual_exit_value = ?,
                close_method = ?,
                close_reason = ?,
                notes_exit = ?,
                gross_pnl = ?,
                max_risk = ?,
                roi_on_risk = ?,
                hours_held = ?,
                win_loss_result = ?
            WHERE id = ?
            """,
            (
                current_timestamp(),
                summary.get("status"),
                summary.get("exit_datetime"),
                summary.get("spx_at_exit"),
                summary.get("actual_exit_value"),
                summary.get("close_method"),
                summary.get("close_reason"),
                summary.get("notes_exit"),
                summary.get("gross_pnl"),
                summary.get("max_risk"),
                summary.get("roi_on_risk"),
                summary.get("hours_held"),
                summary.get("win_loss_result"),
                trade_id,
            ),
        )

    def _backfill_legacy_close_events(self, connection: sqlite3.Connection, trade: Dict[str, Any]) -> None:
        trade_id = to_int(trade.get("id"))
        original_contracts = to_int(trade.get("contracts")) or 0
        stored_status = str(trade.get("status") or "").strip().lower()
        if trade_id is None or original_contracts <= 0:
            return
        existing = connection.execute(
            "SELECT 1 FROM trade_close_events WHERE trade_id = ? LIMIT 1",
            (trade_id,),
        ).fetchone()
        if existing:
            return
        if stored_status not in {"closed", "expired"}:
            return

        actual_exit_value = to_float(trade.get("actual_exit_value"))
        event_type = "expire" if stored_status == "expired" else "close"
        if actual_exit_value is None and event_type != "expire":
            return

        self._insert_close_event(
            connection,
            trade_id=trade_id,
            event_type=event_type,
            contracts_closed=original_contracts,
            actual_exit_value=0.0 if actual_exit_value is None else actual_exit_value,
            event_datetime=trade.get("exit_datetime") or trade.get("updated_at") or trade.get("created_at"),
            spx_at_exit=trade.get("spx_at_exit"),
            close_method=trade.get("close_method") or ("Expire" if event_type == "expire" else "Legacy Close"),
            close_reason=trade.get("close_reason") or ("Expired Worthless" if event_type == "expire" else "Backfilled legacy close event"),
            notes_exit=trade.get("notes_exit") or "",
        )

    def _next_trade_number(self, connection: sqlite3.Connection) -> int:
        row = connection.execute("SELECT COALESCE(MAX(trade_number), 0) AS max_trade_number FROM trades").fetchone()
        return int((row["max_trade_number"] or 0) + 1)

    def _ensure_trade_number_available(
        self,
        connection: sqlite3.Connection,
        trade_number: int,
        *,
        exclude_id: Optional[int] = None,
    ) -> None:
        params: list[Any] = [int(trade_number)]
        sql = "SELECT id FROM trades WHERE trade_number = ?"
        if exclude_id is not None:
            sql += " AND id <> ?"
            params.append(exclude_id)
        existing = connection.execute(sql, params).fetchone()
        if existing:
            raise ValueError("Trade number must be unique.")


def normalize_trade_mode(value: Any) -> str:
    trade_mode = str(value or "").strip().lower()
    if trade_mode not in VALID_TRADE_MODES:
        raise ValueError("Trade mode must be 'real', 'simulated', or 'talos'.")
    return trade_mode


def normalize_system_name(value: Any) -> str:
    text = str(value or "").strip()
    lowered = text.lower()
    if "talos" in lowered:
        return "Talos"
    if "kairos" in lowered:
        return "Kairos"
    if "fortress" in lowered or "aegis" in lowered:
        return "Fortress"
    if "apollo" in lowered:
        return "Apollo"
    return DEFAULT_SYSTEM_NAME


def normalize_journal_name(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return JOURNAL_NAME_DEFAULT
    if text.lower() in {LEGACY_JOURNAL_NAME_DEFAULT.lower(), JOURNAL_NAME_DEFAULT.lower()}:
        return JOURNAL_NAME_DEFAULT
    return text


def _trade_marker_fields(trade: Dict[str, Any]) -> tuple[str, ...]:
    return (
        str(trade.get("trade_mode") or ""),
        str(trade.get("system_name") or ""),
        str(trade.get("system") or ""),
        str(trade.get("journal_name") or ""),
        str(trade.get("source") or ""),
        str(trade.get("engine") or ""),
        str(trade.get("category") or ""),
        str(trade.get("prefill_source") or ""),
        str(trade.get("automation_status") or ""),
        str(trade.get("pass_type") or ""),
        str(trade.get("boundary_rule_used") or ""),
        str(trade.get("notes_entry") or ""),
        str(trade.get("notes_exit") or ""),
        str(trade.get("close_reason") or ""),
    )


def _trade_matches_any_marker(trade: Dict[str, Any], markers: tuple[str, ...]) -> bool:
    marker_fields = tuple(value.lower() for value in _trade_marker_fields(trade) if value)
    return any(marker in value for value in marker_fields for marker in markers)


def resolve_trade_system_name(trade: Dict[str, Any]) -> str:
    talos_markers = ("talos",)
    kairos_markers = (
        "kairos",
        "prefilled from kairos",
        "kairos best candidate",
        "kairos tape",
        "kairos live",
        "kairos sim",
    )
    apollo_markers = ("apollo",)
    trade_mode = str(trade.get("trade_mode") or "").strip().lower()
    if trade_mode == "talos" or _trade_matches_any_marker(trade, talos_markers):
        return "Talos"
    if trade_mode == "kairos" or _trade_matches_any_marker(trade, kairos_markers):
        return "Kairos"
    raw_explicit_system = str(trade.get("system_name") or trade.get("system") or "").strip()
    if raw_explicit_system:
        explicit_system = normalize_system_name(raw_explicit_system)
        if explicit_system == "Apollo":
            return "Apollo"
    if _trade_matches_any_marker(trade, apollo_markers) and trade_mode in {"real", "simulated", ""}:
        return "Apollo"
    return UNKNOWN_SYSTEM_NAME


def is_retained_trade_system(trade: Dict[str, Any]) -> bool:
    return resolve_trade_system_name(trade) in RETAINED_SYSTEM_NAMES


def normalize_structure_label(value: Any) -> Optional[str]:
    text = str(value or "").strip()
    lowered = text.lower()
    if not lowered:
        return None
    if lowered == "good":
        return "Good"
    if lowered == "neutral":
        return "Neutral"
    if lowered == "poor":
        return "Poor"
    return text.title()


def normalize_macro_flag(value: Any) -> Optional[str]:
    text = str(value or "").strip()
    lowered = text.lower()
    if not lowered:
        return None
    if lowered == "none":
        return "None"
    if lowered == "minor":
        return "Minor"
    if lowered == "major":
        return "Major"
    return text.title()


def normalize_candidate_profile(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return DEFAULT_CANDIDATE_PROFILE
    lowered = text.lower()
    for option in VALID_CANDIDATE_PROFILES:
        if option.lower() == lowered:
            return option
    kairos_slot_profile = detect_kairos_slot_profile(text)
    if kairos_slot_profile:
        return kairos_slot_profile
    if "aggressive" in lowered:
        return "Aggressive"
    if "fortress" in lowered:
        return "Fortress"
    if "standard" in lowered:
        return "Standard"
    if "subprime" in lowered:
        return "Subprime"
    if "prime" in lowered:
        return "Prime"
    if "legacy" in lowered:
        return "Legacy"
    return DEFAULT_CANDIDATE_PROFILE


def detect_kairos_slot_profile(value: Any) -> str:
    lowered = str(value or "").strip().lower()
    if not lowered:
        return ""
    if "best available" in lowered or "subprime" in lowered:
        return "Subprime"
    if "window open trade" in lowered or "window open" in lowered or "kairos window" in lowered or lowered == "prime":
        return "Prime"
    return ""


def resolve_trade_candidate_profile(trade: Dict[str, Any]) -> str:
    system_name = normalize_system_name(trade.get("system_name"))
    if system_name == "Kairos":
        for value in (
            trade.get("candidate_profile"),
            trade.get("pass_type"),
            trade.get("notes_entry"),
            trade.get("notes_exit"),
            trade.get("close_reason"),
        ):
            detected = detect_kairos_slot_profile(value)
            if detected:
                return detected
    return normalize_candidate_profile(trade.get("candidate_profile"))


def normalize_close_method(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text == "reduce":
        return "Reduce"
    if text in {"close", "buy to close", "btc"}:
        return "Close"
    if text in {"expire", "expiration"}:
        return "Expire"
    if text in {"manage trade prefill", "manage_trade_prefill"}:
        return "Manage Trade Prefill"
    raise ValueError("Close method must be Reduce, Close, Expire, or Manage Trade Prefill.")


def default_close_reason(close_method: str, contracts_closed: int) -> str:
    normalized_method = normalize_close_method(close_method)
    if normalized_method == "Expire":
        return "Expired Worthless"
    if normalized_method in {"Close", "Manage Trade Prefill"}:
        return f"Closed {contracts_closed} contract{'s' if contracts_closed != 1 else ''}"
    return f"Reduced {contracts_closed} contract{'s' if contracts_closed != 1 else ''}"


def derive_close_event_type(close_method: str) -> str:
    normalized_method = normalize_close_method(close_method)
    if normalized_method == "Reduce":
        return "reduce"
    if normalized_method == "Expire":
        return "expire"
    return "close"


def build_trade_duplicate_signature(payload: Dict[str, Any], already_normalized: bool = False) -> tuple[Any, ...]:
    normalized = payload if already_normalized else normalize_trade_payload(payload)
    return tuple(_signature_value(normalized.get(field)) for field in DUPLICATE_SIGNATURE_FIELDS)


def normalize_trade_payload(
    payload: Dict[str, Any],
    existing: Optional[Dict[str, Any]] = None,
    historical_price_lookup: Optional[Callable[[Dict[str, Any]], Optional[Dict[str, Any]]]] = None,
    expected_move_snapshot_lookup: Optional[Callable[[Dict[str, Any]], Optional[Dict[str, Any]]]] = None,
) -> Dict[str, Any]:
    combined = dict(existing or {})
    for field in EDITABLE_FIELDS:
        if field in payload:
            combined[field] = payload.get(field)

    normalized: Dict[str, Any] = {}
    for field in EDITABLE_FIELDS:
        normalized[field] = coerce_field_value(field, combined.get(field))

    if normalized.get("trade_number") is not None and normalized["trade_number"] <= 0:
        raise ValueError("Trade number must be a positive integer.")

    normalized["trade_mode"] = normalize_trade_mode(normalized.get("trade_mode"))
    normalized["system_name"] = normalize_system_name(normalized.get("system_name"))
    normalized["candidate_profile"] = normalize_candidate_profile(normalized.get("candidate_profile"))

    normalized["journal_name"] = str(normalized.get("journal_name") or JOURNAL_NAME_DEFAULT).strip()
    if not normalized["journal_name"]:
        raise ValueError("Journal name is required.")

    normalized["status"] = derive_trade_status(normalized, existing=existing)

    provided_expected_move_source = normalize_expected_move_source(combined.get("expected_move_source"))
    expected_move_metadata = resolve_trade_expected_move(
        normalized,
        snapshot_lookup=expected_move_snapshot_lookup,
    )
    normalized["expected_move"] = expected_move_metadata.get("value")
    normalized["expected_move_source"] = expected_move_metadata.get("source")
    normalized["expected_move_raw_estimate"] = expected_move_metadata.get("raw_estimate")
    normalized["expected_move_calibrated"] = expected_move_metadata.get("calibrated_value")
    normalized["expected_move_confidence"] = expected_move_metadata.get("confidence")

    normalized["distance_source"] = combined.get("distance_source")
    distance_metadata = resolve_trade_distance(normalized, historical_price_lookup=historical_price_lookup)
    normalized["distance_to_short"] = distance_metadata.get("value")
    normalized["distance_source"] = distance_metadata.get("source")

    normalized["expected_move_used"] = to_float(normalized.get("expected_move_used"))
    if normalized["expected_move_used"] is None:
        normalized["expected_move_used"] = expected_move_metadata.get("used_value")
    if normalized["expected_move_used"] is None:
        normalized["expected_move_used"] = normalized.get("expected_move")

    normalized["actual_distance_to_short"] = to_float(normalized.get("actual_distance_to_short"))
    if normalized["actual_distance_to_short"] is None:
        normalized["actual_distance_to_short"] = normalized.get("distance_to_short")

    normalized["actual_em_multiple"] = to_float(normalized.get("actual_em_multiple"))
    if normalized["actual_em_multiple"] is None:
        expected_move_used = to_float(normalized.get("expected_move_used"))
        actual_distance = to_float(normalized.get("actual_distance_to_short"))
        if expected_move_used not in {None, 0.0} and actual_distance is not None:
            normalized["actual_em_multiple"] = round(actual_distance / expected_move_used, 4)

    normalized["em_multiple_floor"] = to_float(normalized.get("em_multiple_floor"))
    normalized["percent_floor"] = to_float(normalized.get("percent_floor"))
    normalized["boundary_rule_used"] = str(normalized.get("boundary_rule_used") or "").strip()
    normalized["fallback_used"] = normalize_yes_no_flag(normalized.get("fallback_used"))
    normalized["fallback_rule_name"] = str(normalized.get("fallback_rule_name") or "").strip()

    credit_model = resolve_trade_credit_model(normalized)
    normalized["candidate_credit_estimate"] = credit_model.get("candidate_credit_estimate")
    normalized["actual_entry_credit"] = credit_model.get("actual_entry_credit")
    normalized["net_credit_per_contract"] = credit_model.get("net_credit_per_contract")
    normalized["premium_per_contract"] = credit_model.get("premium_per_contract")
    normalized["total_premium"] = credit_model.get("total_premium")
    normalized["max_theoretical_risk"] = credit_model.get("max_theoretical_risk")
    normalized["total_max_loss"] = credit_model.get("max_theoretical_risk")
    normalized["max_loss"] = credit_model.get("max_theoretical_risk")

    derived = calculate_trade_metrics(normalized, distance_metadata=distance_metadata)
    normalized.update(derived)
    normalized.update(build_learning_trade_fields(normalized, distance_metadata=distance_metadata))
    return normalized


def calculate_trade_metrics(values: Dict[str, Any], distance_metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    credit_model = resolve_trade_credit_model(values)
    entry_credit = credit_model.get("net_credit_per_contract")
    exit_value = to_float(values.get("actual_exit_value"))
    contracts = to_int(values.get("contracts"))
    spread_width = to_float(values.get("spread_width"))
    total_premium = credit_model.get("total_premium")

    gross_pnl = None
    if total_premium is not None and exit_value is not None and contracts is not None:
        gross_pnl = total_premium - (exit_value * contracts * 100)

    max_risk = credit_model.get("max_theoretical_risk")
    if max_risk is None and spread_width is not None and entry_credit is not None and contracts is not None:
        max_risk = (spread_width - entry_credit) * contracts * 100

    roi_on_risk = None
    if gross_pnl is not None and max_risk is not None and max_risk > 0:
        roi_on_risk = gross_pnl / max_risk

    distance_to_short = (distance_metadata or resolve_trade_distance(values)).get("value")

    hours_held = None
    entry_datetime = parse_datetime_value(values.get("entry_datetime"))
    exit_datetime = parse_datetime_value(values.get("exit_datetime"))
    if entry_datetime and exit_datetime:
        if (entry_datetime.tzinfo is None) != (exit_datetime.tzinfo is None):
            if entry_datetime.tzinfo is None:
                exit_datetime = exit_datetime.replace(tzinfo=None)
            else:
                entry_datetime = entry_datetime.replace(tzinfo=None)
        hours_held = (exit_datetime - entry_datetime).total_seconds() / 3600

    derived_status = derive_trade_status(values)
    win_loss_result = None
    if derived_status in {"closed", "expired"}:
        win_loss_result = classify_closed_trade_outcome(
            gross_pnl=gross_pnl,
            max_theoretical_risk=max_risk,
            explicit_result=values.get("win_loss_result") or values.get("result"),
            close_reason=values.get("close_reason"),
        )

    return {
        "candidate_credit_estimate": credit_model.get("candidate_credit_estimate"),
        "actual_entry_credit": credit_model.get("actual_entry_credit"),
        "net_credit_per_contract": credit_model.get("net_credit_per_contract"),
        "premium_per_contract": credit_model.get("premium_per_contract"),
        "total_premium": total_premium,
        "max_theoretical_risk": credit_model.get("max_theoretical_risk"),
        "total_max_loss": credit_model.get("max_theoretical_risk"),
        "max_loss": credit_model.get("max_theoretical_risk"),
        "gross_pnl": gross_pnl,
        "max_risk": max_risk,
        "roi_on_risk": roi_on_risk,
        "hours_held": hours_held,
        "win_loss_result": win_loss_result,
        "distance_to_short": distance_to_short,
    }


def resolve_trade_credit_model(values: Dict[str, Any]) -> Dict[str, Any]:
    contracts = to_int(values.get("contracts"))
    spread_width = to_float(values.get("spread_width"))
    explicit_net_credit = to_float(values.get("net_credit_per_contract"))
    explicit_premium_per_contract = to_float(values.get("premium_per_contract"))
    explicit_total_premium = to_float(values.get("total_premium"))
    candidate_credit = to_float(values.get("candidate_credit_estimate"))
    actual_entry_credit = to_float(values.get("actual_entry_credit"))
    legacy_credit_received = to_float(values.get("credit_received"))

    net_credit_per_contract = None
    premium_per_contract = explicit_premium_per_contract
    total_premium = explicit_total_premium
    source = "unavailable"

    if _looks_like_net_credit_per_contract(explicit_net_credit, spread_width):
        net_credit_per_contract = explicit_net_credit
        premium_per_contract = None
        total_premium = None
        source = "net_credit_per_contract"
    else:
        raw_credit_value = first_float(actual_entry_credit, candidate_credit, legacy_credit_received)
        if raw_credit_value is not None:
            premium_per_contract = None
            total_premium = None
            if _looks_like_net_credit_per_contract(raw_credit_value, spread_width):
                net_credit_per_contract = raw_credit_value
                source = "per_contract_credit"
            else:
                if _should_treat_legacy_credit_as_total_premium(
                    values,
                    raw_credit_value=raw_credit_value,
                    contracts=contracts,
                    spread_width=spread_width,
                ) and contracts not in {None, 0}:
                    total_premium = raw_credit_value
                    premium_per_contract = raw_credit_value / contracts
                    net_credit_per_contract = raw_credit_value / (contracts * 100)
                    source = "legacy_total_premium"
                else:
                    premium_per_contract = raw_credit_value
                    net_credit_per_contract = raw_credit_value / 100
                    source = "legacy_premium_per_contract"

    if premium_per_contract is None and net_credit_per_contract is not None:
        premium_per_contract = net_credit_per_contract * 100
    if total_premium is None and premium_per_contract is not None and contracts not in {None, 0}:
        total_premium = premium_per_contract * contracts
    if net_credit_per_contract is None and total_premium is not None and contracts not in {None, 0}:
        net_credit_per_contract = total_premium / (contracts * 100)
    if premium_per_contract is None and total_premium is not None and contracts not in {None, 0}:
        premium_per_contract = total_premium / contracts

    max_theoretical_risk = None
    if spread_width is not None and net_credit_per_contract is not None and contracts not in {None, 0}:
        max_theoretical_risk = max((spread_width - net_credit_per_contract) * contracts * 100, 0.0)

    candidate_credit_estimate_value = _normalize_credit_field_value(
        original_value=candidate_credit,
        net_credit_per_contract=net_credit_per_contract,
    )
    actual_entry_credit_value = _normalize_credit_field_value(
        original_value=actual_entry_credit,
        net_credit_per_contract=net_credit_per_contract,
    )
    if candidate_credit_estimate_value is None:
        candidate_credit_estimate_value = net_credit_per_contract
    if actual_entry_credit_value is None:
        actual_entry_credit_value = net_credit_per_contract

    return {
        "source": source,
        "candidate_credit_estimate": round(candidate_credit_estimate_value, 4) if candidate_credit_estimate_value is not None else None,
        "actual_entry_credit": round(actual_entry_credit_value, 4) if actual_entry_credit_value is not None else None,
        "net_credit_per_contract": round(net_credit_per_contract, 4) if net_credit_per_contract is not None else None,
        "premium_per_contract": round(premium_per_contract, 2) if premium_per_contract is not None else None,
        "total_premium": round(total_premium, 2) if total_premium is not None else None,
        "max_theoretical_risk": round(max_theoretical_risk, 2) if max_theoretical_risk is not None else None,
    }


def _looks_like_net_credit_per_contract(value: Optional[float], spread_width: Optional[float]) -> bool:
    if value is None or value <= 0 or value > 10:
        return False
    if spread_width is None:
        return True
    return value <= spread_width


def _should_treat_legacy_credit_as_total_premium(
    values: Dict[str, Any],
    *,
    raw_credit_value: float,
    contracts: Optional[int],
    spread_width: Optional[float],
) -> bool:
    if contracts in {None, 0}:
        return False
    if to_float(values.get("total_premium")) is not None:
        return True
    if raw_credit_value >= 100:
        candidate_as_credit = raw_credit_value / (contracts * 100)
        return spread_width is None or candidate_as_credit <= spread_width
    return False


def _normalize_credit_field_value(*, original_value: Optional[float], net_credit_per_contract: Optional[float]) -> Optional[float]:
    if net_credit_per_contract is None:
        return None
    if original_value is None or original_value > 10:
        return net_credit_per_contract
    return original_value


def summarize_trade_close_events(trade: Dict[str, Any], events: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    normalized_events = list(events or [])
    original_contracts = to_int(trade.get("contracts")) or 0
    credit_model = resolve_trade_credit_model(trade)
    distance_metadata = resolve_trade_distance(trade)
    distance_to_short = distance_metadata.get("value")

    if not normalized_events:
        base_metrics = calculate_trade_metrics(trade)
        derived_status_raw = derive_trade_status(trade)
        inferred_closed_contracts = original_contracts if derived_status_raw in {"closed", "expired"} and original_contracts > 0 else 0
        inferred_remaining_contracts = max(original_contracts - inferred_closed_contracts, 0) if original_contracts > 0 else None
        if derived_status_raw == "cancelled":
            derived_status_label = "Cancelled"
        elif derived_status_raw == "expired":
            derived_status_label = "Expired"
        elif inferred_closed_contracts == 0:
            derived_status_label = "Open"
        else:
            derived_status_label = "Closed"
        win_loss_result = base_metrics.get("win_loss_result") if derived_status_raw in {"closed", "expired"} else None
        return {
            **base_metrics,
            "status": derived_status_raw,
            "derived_status_raw": derived_status_raw,
            "derived_status_label": derived_status_label,
            "original_contracts": original_contracts,
            "closed_contracts": inferred_closed_contracts,
            "contracts_closed": inferred_closed_contracts,
            "remaining_contracts": inferred_remaining_contracts,
            "contracts_remaining": inferred_remaining_contracts,
            "weighted_exit_value": to_float(trade.get("actual_exit_value")) if derived_status_raw == "closed" else None,
            "realized_pnl": base_metrics.get("gross_pnl") if derived_status_raw == "closed" else None,
            "win_loss_result": win_loss_result,
            "distance_display": distance_to_short,
            "distance_source": distance_metadata.get("source"),
            "distance_is_estimated": distance_metadata.get("estimated"),
            "has_expire_event": derived_status_raw == "expired",
        }

    total_closed_contracts = 0
    total_close_cost = 0.0
    last_event: Dict[str, Any] | None = None
    for event in normalized_events:
        contracts_closed = max(to_int(event.get("contracts_closed")) or 0, 0)
        total_closed_contracts += contracts_closed
        total_close_cost += (to_float(event.get("actual_exit_value")) or 0.0) * contracts_closed * 100
        last_event = event

    if original_contracts > 0:
        total_closed_contracts = min(total_closed_contracts, original_contracts)
    remaining_contracts = max(original_contracts - total_closed_contracts, 0) if original_contracts else 0
    has_expire_event = any(str(event.get("event_type") or "").strip().lower() == "expire" for event in normalized_events)
    is_reduced = total_closed_contracts > 0 and remaining_contracts > 0
    is_closed = total_closed_contracts >= original_contracts > 0
    stored_status = "expired" if is_closed and has_expire_event else "closed" if is_closed else "open"
    derived_status_raw = "reduced" if is_reduced else stored_status
    weighted_exit_value = (total_close_cost / total_closed_contracts / 100) if total_closed_contracts > 0 else to_float(trade.get("actual_exit_value"))

    gross_pnl = None
    if credit_model.get("total_premium") is not None:
        gross_pnl = credit_model.get("total_premium") - total_close_cost

    max_risk = credit_model.get("max_theoretical_risk")

    roi_on_risk = None
    if gross_pnl is not None and max_risk not in {None, 0}:
        roi_on_risk = gross_pnl / max_risk

    hours_held = None
    entry_datetime = parse_datetime_value(trade.get("entry_datetime"))
    exit_datetime = parse_datetime_value((last_event or {}).get("event_datetime"))
    if entry_datetime and exit_datetime:
        if (entry_datetime.tzinfo is None) != (exit_datetime.tzinfo is None):
            if entry_datetime.tzinfo is None:
                exit_datetime = exit_datetime.replace(tzinfo=None)
            else:
                entry_datetime = entry_datetime.replace(tzinfo=None)
        hours_held = (exit_datetime - entry_datetime).total_seconds() / 3600

    if remaining_contracts > 0:
        win_loss_result = None
    else:
        win_loss_result = classify_closed_trade_outcome(
            gross_pnl=gross_pnl,
            max_theoretical_risk=max_risk,
            explicit_result=trade.get("win_loss_result") or trade.get("result"),
            close_reason=(last_event or {}).get("close_reason") or trade.get("close_reason"),
        )

    return {
        "status": stored_status,
        "derived_status_raw": derived_status_raw,
        "derived_status_label": "Reduced" if is_reduced else stored_status.title(),
        "original_contracts": original_contracts,
        "closed_contracts": total_closed_contracts,
        "contracts_closed": total_closed_contracts,
        "remaining_contracts": remaining_contracts,
        "contracts_remaining": remaining_contracts,
        "actual_exit_value": weighted_exit_value,
        "weighted_exit_value": weighted_exit_value,
        "exit_datetime": (last_event or {}).get("event_datetime") or trade.get("exit_datetime"),
        "spx_at_exit": (last_event or {}).get("spx_at_exit") if (last_event or {}).get("spx_at_exit") not in {None, ""} else trade.get("spx_at_exit"),
        "close_method": (last_event or {}).get("close_method") or trade.get("close_method"),
        "close_reason": (last_event or {}).get("close_reason") or trade.get("close_reason"),
        "notes_exit": (last_event or {}).get("notes_exit") or trade.get("notes_exit"),
        "candidate_credit_estimate": credit_model.get("candidate_credit_estimate"),
        "actual_entry_credit": credit_model.get("actual_entry_credit"),
        "net_credit_per_contract": credit_model.get("net_credit_per_contract"),
        "premium_per_contract": credit_model.get("premium_per_contract"),
        "total_premium": credit_model.get("total_premium"),
        "total_close_cost": total_close_cost,
        "gross_pnl": gross_pnl,
        "realized_pnl": gross_pnl,
        "max_risk": max_risk,
        "max_theoretical_risk": credit_model.get("max_theoretical_risk"),
        "roi_on_risk": roi_on_risk,
        "hours_held": hours_held,
        "win_loss_result": win_loss_result,
        "distance_display": distance_to_short,
        "distance_source": distance_metadata.get("source"),
        "distance_is_estimated": distance_metadata.get("estimated"),
        "has_expire_event": has_expire_event,
        "has_partial_close": is_reduced,
    }


def derive_trade_status(values: Dict[str, Any], existing: Optional[Dict[str, Any]] = None) -> str:
    close_method_value = values.get("close_method")
    close_method = normalize_close_method(close_method_value) if close_method_value not in {None, ""} else None
    contracts = to_int(values.get("contracts")) or 0
    has_exit_evidence = _trade_has_exit_evidence(values, close_method=close_method)
    existing_status = str((existing or {}).get("status") or values.get("status") or "").strip().lower()
    if close_method == "Expire":
        return "expired"
    if existing_status == "cancelled" and not has_exit_evidence:
        return "cancelled"
    if contracts > 0 and has_exit_evidence:
        return "closed"
    return "open"


def _trade_has_exit_evidence(values: Dict[str, Any], *, close_method: Optional[str] = None) -> bool:
    normalized_close_method = close_method
    if normalized_close_method is None:
        close_method_value = values.get("close_method")
        if close_method_value not in {None, ""}:
            normalized_close_method = normalize_close_method(close_method_value)
    return any(
        (
            parse_datetime_value(values.get("exit_datetime")) is not None,
            to_float(values.get("actual_exit_value")) is not None,
            normalized_close_method is not None,
        )
    )


def calculate_trade_distance(values: Dict[str, Any]) -> Optional[float]:
    return resolve_trade_distance(values).get("value")


def is_objective_black_swan_loss(*, gross_pnl: Any, max_theoretical_risk: Any) -> bool:
    gross_pnl_value = to_float(gross_pnl)
    max_risk_value = to_float(max_theoretical_risk)
    if gross_pnl_value is None or gross_pnl_value >= 0:
        return False
    if max_risk_value in {None, 0}:
        return False
    return abs(gross_pnl_value) > (max_risk_value * BLACK_SWAN_LOSS_THRESHOLD)


def classify_closed_trade_outcome(
    *,
    gross_pnl: Any,
    max_theoretical_risk: Any,
    explicit_result: Any = None,
    close_reason: Any = None,
) -> Optional[str]:
    gross_pnl_value = to_float(gross_pnl)
    if is_objective_black_swan_loss(gross_pnl=gross_pnl_value, max_theoretical_risk=max_theoretical_risk):
        return "Black Swan"
    if gross_pnl_value is not None:
        if gross_pnl_value > 0:
            return "Win"
        if gross_pnl_value < 0:
            return "Loss"
        return "Flat"

    normalized_result = str(explicit_result or "").strip().lower().replace("_", " ")
    if normalized_result == "black swan":
        return "Black Swan"
    if normalized_result == "win":
        return "Win"
    if normalized_result == "loss":
        return "Loss"
    if normalized_result in {"flat", "scratched", "scratch"}:
        return "Flat"

    close_reason_text = str(close_reason or "").strip().lower()
    if "black swan" in close_reason_text:
        return "Black Swan"
    return None


def build_real_trade_outcome_profile(trades: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    real_trades = [
        trade
        for trade in (trades or [])
        if str(trade.get("trade_mode") or "").strip().lower() == "real"
        and str(trade.get("derived_status_raw") or trade.get("status") or "").strip().lower() not in {"open", "reduced"}
    ]
    wins = [trade for trade in real_trades if str(trade.get("win_loss_result") or "").strip() == "Win"]
    losses = [trade for trade in real_trades if str(trade.get("win_loss_result") or "").strip() == "Loss"]
    black_swans = [trade for trade in real_trades if str(trade.get("win_loss_result") or "").strip() == "Black Swan"]

    win_roi_values = [to_float(trade.get("roi_on_risk")) for trade in wins if to_float(trade.get("roi_on_risk")) is not None]
    loss_roi_values = [to_float(trade.get("roi_on_risk")) for trade in losses if to_float(trade.get("roi_on_risk")) is not None]
    black_swan_roi_values = [to_float(trade.get("roi_on_risk")) for trade in black_swans if to_float(trade.get("roi_on_risk")) is not None]

    average_win_percentage = average_numeric(win_roi_values)
    average_loss_percentage = average_numeric(loss_roi_values)
    average_black_swan_percentage = average_numeric(black_swan_roi_values)

    return {
        "trade_count": len(real_trades),
        "win_count": len(wins),
        "loss_count": len(losses),
        "black_swan_count": len(black_swans),
        "average_win_percentage": average_win_percentage,
        "average_loss_percentage": average_loss_percentage,
        "average_black_swan_percentage": average_black_swan_percentage,
        "routine_loss_percentage": abs(average_loss_percentage),
        "black_swan_loss_percentage": abs(average_black_swan_percentage),
    }


def average_numeric(values: Iterable[Optional[float]]) -> float:
    cleaned = [float(value) for value in values if value is not None]
    if not cleaned:
        return 0.0
    return sum(cleaned) / len(cleaned)


def resolve_trade_distance(
    values: Dict[str, Any],
    historical_price_lookup: Optional[Callable[[Dict[str, Any]], Optional[Dict[str, Any]]]] = None,
) -> Dict[str, Any]:
    short_strike = to_float(values.get("short_strike"))
    stored_distance = to_float(
        values.get("distance_to_short") if values.get("distance_to_short") is not None else values.get("distance_display")
    )
    stored_source = normalize_distance_source(values.get("distance_source"))
    derived_distance = calculate_entry_distance(values)
    discrepancy_points = None
    discrepancy_is_material = False

    if is_valid_distance(stored_distance) and derived_distance is not None:
        discrepancy_points = round(abs(stored_distance - derived_distance), 2)
        discrepancy_is_material = discrepancy_points > max(
            MATERIAL_DISTANCE_DISCREPANCY_FLOOR,
            round(derived_distance * MATERIAL_DISTANCE_DISCREPANCY_RATIO, 2),
        )

    if is_valid_distance(stored_distance) and stored_source not in {DISTANCE_SOURCE_DERIVED, DISTANCE_SOURCE_ESTIMATED}:
        resolved_source = stored_source if stored_source != DISTANCE_SOURCE_UNRESOLVED else DISTANCE_SOURCE_ORIGINAL
        return {
            "value": round(stored_distance, 2),
            "source": resolved_source,
            "estimated": resolved_source == DISTANCE_SOURCE_ESTIMATED,
            "stored_value": round(stored_distance, 2),
            "derived_value": round(derived_distance, 2) if derived_distance is not None else None,
            "discrepancy_points": discrepancy_points,
            "discrepancy_is_material": discrepancy_is_material,
        }

    if derived_distance is not None:
        return {
            "value": round(derived_distance, 2),
            "source": DISTANCE_SOURCE_DERIVED,
            "estimated": False,
            "stored_value": None,
            "derived_value": round(derived_distance, 2),
            "discrepancy_points": None,
            "discrepancy_is_material": False,
        }

    if is_valid_distance(stored_distance):
        return {
            "value": round(stored_distance, 2),
            "source": stored_source,
            "estimated": stored_source == DISTANCE_SOURCE_ESTIMATED,
            "stored_value": round(stored_distance, 2),
            "derived_value": None,
            "discrepancy_points": None,
            "discrepancy_is_material": False,
        }

    if historical_price_lookup is not None and short_strike is not None and is_apollo_trade(values):
        historical_reference = historical_price_lookup(values) or {}
        reference_price = first_float(
            historical_reference.get("reference_price"),
            historical_reference.get("close"),
        )
        estimated_distance = calculate_distance_from_reference(reference_price, short_strike, values.get("option_type"))
        if estimated_distance is not None:
            return {
                "value": round(estimated_distance, 2),
                "source": DISTANCE_SOURCE_ESTIMATED,
                "estimated": True,
                "stored_value": None,
                "derived_value": None,
                "discrepancy_points": None,
                "discrepancy_is_material": False,
                "reference_price": round(reference_price, 2) if reference_price is not None else None,
                "reference_date": historical_reference.get("reference_date"),
            }

    return {
        "value": None,
        "source": DISTANCE_SOURCE_UNRESOLVED,
        "estimated": False,
        "stored_value": round(stored_distance, 2) if is_valid_distance(stored_distance) else None,
        "derived_value": round(derived_distance, 2) if derived_distance is not None else None,
        "discrepancy_points": discrepancy_points,
        "discrepancy_is_material": discrepancy_is_material,
    }


def resolve_trade_expected_move(
    values: Dict[str, Any],
    snapshot_lookup: Optional[Callable[[Dict[str, Any]], Optional[Dict[str, Any]]]] = None,
    historical_context_lookup: Optional[Callable[[Dict[str, Any]], Optional[Dict[str, Any]]]] = None,
) -> Dict[str, Any]:
    stored_expected_move = to_float(values.get("expected_move"))
    stored_source = normalize_expected_move_source(values.get("expected_move_source"))
    stored_raw_estimate = to_float(values.get("expected_move_raw_estimate"))
    stored_calibrated = to_float(values.get("expected_move_calibrated"))
    stored_used_value = to_float(values.get("expected_move_used"))

    reference_spx = first_float(values.get("spx_at_entry"), values.get("spx_entry"))
    reference_vix = first_float(values.get("vix_at_entry"), values.get("vix_entry"))
    historical_reference = None
    if (reference_spx is None or reference_vix is None) and historical_context_lookup is not None:
        historical_reference = historical_context_lookup(values) or {}
        if reference_spx is None:
            reference_spx = first_float(historical_reference.get("reference_spx"), historical_reference.get("spx_close"))
        if reference_vix is None:
            reference_vix = first_float(historical_reference.get("reference_vix"), historical_reference.get("vix_close"))

    formula_estimate = calculate_expected_move_from_spx_vix(reference_spx, reference_vix)

    def build_metadata(
        *,
        value: Optional[float],
        source: str,
        estimated: bool,
        raw_estimate: Optional[float] = None,
        calibrated_value: Optional[float] = None,
        used_value: Optional[float] = None,
        stored_value: Optional[float] = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        normalized_source = normalize_expected_move_source(source)
        payload = {
            "value": round(value, 2) if is_valid_expected_move(value) else None,
            "used_value": round(used_value, 2) if is_valid_expected_move(used_value) else None,
            "source": normalized_source,
            "estimated": estimated,
            "stored_value": round(stored_value, 2) if is_valid_expected_move(stored_value) else None,
            "raw_estimate": round(raw_estimate, 2) if is_valid_expected_move(raw_estimate) else None,
            "calibrated_value": round(calibrated_value, 2) if is_valid_expected_move(calibrated_value) else None,
            "confidence": expected_move_confidence_for_source(normalized_source),
            "usage_category": classify_expected_move_usage(normalized_source, used_value),
        }
        if extra:
            payload.update(extra)
        return payload

    if is_valid_expected_move(stored_expected_move) and stored_source not in {
        EXPECTED_MOVE_SOURCE_RECOVERED_CANDIDATE,
        EXPECTED_MOVE_SOURCE_RECOVERED_SNAPSHOT,
        EXPECTED_MOVE_SOURCE_ESTIMATED,
    }:
        resolved_source = stored_source if stored_source != EXPECTED_MOVE_SOURCE_UNRESOLVED else EXPECTED_MOVE_SOURCE_ORIGINAL
        return build_metadata(
            value=stored_expected_move,
            used_value=stored_expected_move,
            source=resolved_source,
            estimated=False,
            raw_estimate=formula_estimate,
            calibrated_value=stored_expected_move,
            stored_value=stored_expected_move,
        )

    if is_kairos_trade(values):
        candidate_expected_move = calculate_expected_move_from_spx_vix(
            first_float(values.get("spx_at_entry"), values.get("spx_entry")),
            first_float(values.get("vix_at_entry"), values.get("vix_entry")),
        )
        if is_valid_expected_move(candidate_expected_move):
            return build_metadata(
                value=candidate_expected_move,
                used_value=candidate_expected_move,
                source=EXPECTED_MOVE_SOURCE_RECOVERED_CANDIDATE,
                estimated=False,
                raw_estimate=candidate_expected_move,
                calibrated_value=candidate_expected_move,
                stored_value=stored_expected_move,
            )

    if snapshot_lookup is not None:
        snapshot_payload = snapshot_lookup(values) or {}
        snapshot_expected_move = first_float(
            snapshot_payload.get("expected_move"),
            snapshot_payload.get("daily_move_anchor"),
        )
        if is_valid_expected_move(snapshot_expected_move):
            return build_metadata(
                value=snapshot_expected_move,
                used_value=snapshot_expected_move,
                source=EXPECTED_MOVE_SOURCE_RECOVERED_SNAPSHOT,
                estimated=False,
                raw_estimate=formula_estimate,
                calibrated_value=snapshot_expected_move,
                stored_value=stored_expected_move,
                extra={"snapshot_reference": snapshot_payload},
            )

    if is_valid_expected_move(stored_expected_move) and stored_source == EXPECTED_MOVE_SOURCE_ATM_STRADDLE:
        return build_metadata(
            value=stored_expected_move,
            used_value=stored_expected_move,
            source=EXPECTED_MOVE_SOURCE_ATM_STRADDLE,
            estimated=False,
            raw_estimate=formula_estimate,
            calibrated_value=stored_expected_move,
            stored_value=stored_expected_move,
        )

    raw_estimate = formula_estimate
    if stored_source == EXPECTED_MOVE_SOURCE_ESTIMATED and is_valid_expected_move(stored_expected_move):
        raw_estimate = stored_expected_move
    elif not is_valid_expected_move(raw_estimate):
        raw_estimate = stored_raw_estimate

    calibrated_estimate = calibrate_expected_move_estimate(raw_estimate)
    if not is_valid_expected_move(calibrated_estimate):
        calibrated_estimate = stored_calibrated
    if not is_valid_expected_move(calibrated_estimate) and is_valid_expected_move(stored_used_value):
        calibrated_estimate = stored_used_value

    if is_valid_expected_move(raw_estimate):
        return build_metadata(
            value=raw_estimate,
            used_value=calibrated_estimate,
            source=EXPECTED_MOVE_SOURCE_ESTIMATED,
            estimated=True,
            raw_estimate=raw_estimate,
            calibrated_value=calibrated_estimate,
            stored_value=stored_expected_move,
            extra={
                "reference_spx": round(reference_spx, 2) if reference_spx is not None else None,
                "reference_vix": round(reference_vix, 2) if reference_vix is not None else None,
                "historical_reference": historical_reference,
            },
        )

    return build_metadata(
        value=None,
        used_value=None,
        source=EXPECTED_MOVE_SOURCE_UNRESOLVED,
        estimated=False,
        raw_estimate=stored_raw_estimate,
        calibrated_value=stored_calibrated,
        stored_value=stored_expected_move,
    )


def calculate_expected_move_from_spx_vix(spx_value: Any, vix_value: Any) -> Optional[float]:
    spot = to_float(spx_value)
    vix = to_float(vix_value)
    if spot is None or vix is None or spot <= 0 or vix <= 0:
        return None
    return spot * (vix / 100.0) / EXPECTED_MOVE_ESTIMATE_DIVISOR


def determine_expected_move_reference_date(values: Dict[str, Any]) -> Optional[Any]:
    trade_date = parse_date_value(values.get("trade_date"))
    if trade_date is not None:
        return trade_date
    entry_datetime = parse_datetime_value(values.get("entry_datetime"))
    if entry_datetime is not None:
        return entry_datetime.date()
    expiration_date = parse_date_value(values.get("expiration_date"))
    return expiration_date


def calculate_entry_distance(values: Dict[str, Any]) -> Optional[float]:
    spx_at_entry = first_float(values.get("spx_at_entry"), values.get("spx_entry"))
    short_strike = to_float(values.get("short_strike"))
    return calculate_distance_from_reference(spx_at_entry, short_strike, values.get("option_type"))


def calculate_distance_from_reference(reference_price: Any, short_strike: Any, option_type: Any) -> Optional[float]:
    entry_reference = to_float(reference_price)
    strike_value = to_float(short_strike)
    if entry_reference is None or strike_value is None:
        return None
    option_label = str(option_type or "").strip().lower()
    if "call" in option_label:
        return abs(strike_value - entry_reference)
    return abs(entry_reference - strike_value)


def build_learning_trade_fields(values: Dict[str, Any], distance_metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    distance_metadata = distance_metadata or resolve_trade_distance(values)
    credit_model = resolve_trade_credit_model(values)
    return {
        "system": normalize_system_name(values.get("system") or values.get("system_name")),
        "spx_entry": first_float(values.get("spx_entry"), values.get("spx_at_entry")),
        "vix_entry": first_float(values.get("vix_entry"), values.get("vix_at_entry")),
        "structure": normalize_structure_label(values.get("structure") or values.get("structure_grade")),
        "distance_to_short": distance_metadata.get("value"),
        "expected_move": to_float(values.get("expected_move")),
        "credit_received": first_float(values.get("credit_received"), credit_model.get("actual_entry_credit")),
        "max_loss": first_float(values.get("max_loss"), values.get("max_risk"), credit_model.get("max_theoretical_risk")),
        "result": normalize_learning_result(values.get("result"), values.get("win_loss_result"), values.get("gross_pnl")),
        "pnl": first_float(values.get("pnl"), values.get("gross_pnl")),
        "max_drawdown": to_float(values.get("max_drawdown")),
        "exit_type": normalize_exit_type(values.get("exit_type") or values.get("close_method")),
        "macro_flag": normalize_macro_flag(values.get("macro_flag") or values.get("macro_grade")),
    }


def normalize_learning_result(*values: Any) -> Optional[str]:
    for value in values:
        if value in {None, ""}:
            continue
        text = str(value).strip().lower()
        if text == "win":
            return "win"
        if text in {"loss", "black swan", "black_swan"}:
            return "loss"
        if text == "flat":
            return None
        try:
            numeric = to_float(value)
        except (TypeError, ValueError):
            numeric = None
        if numeric is not None:
            if numeric > 0:
                return "win"
            if numeric < 0:
                return "loss"
    return None


def normalize_exit_type(value: Any) -> Optional[str]:
    text = str(value or "").strip().lower()
    if not text:
        return None
    if text == "reduce":
        return "Reduce"
    if text == "close":
        return "Close"
    if text == "expire":
        return "Expire"
    return str(value).strip().title()


def first_float(*values: Any) -> Optional[float]:
    for value in values:
        numeric = to_float(value)
        if numeric is not None:
            return numeric
    return None


def is_valid_distance(value: Optional[float]) -> bool:
    return value is not None and value >= 0


def is_valid_expected_move(value: Optional[float]) -> bool:
    return value is not None and value > 0


def calibrate_expected_move_estimate(value: Any) -> Optional[float]:
    estimate = to_float(value)
    if not is_valid_expected_move(estimate):
        return None
    return estimate * EXPECTED_MOVE_CALIBRATION_FACTOR


def is_actual_expected_move_source(source: Any) -> bool:
    normalized = normalize_expected_move_source(source)
    return normalized in {
        EXPECTED_MOVE_SOURCE_ORIGINAL,
        EXPECTED_MOVE_SOURCE_RECOVERED_CANDIDATE,
        EXPECTED_MOVE_SOURCE_RECOVERED_SNAPSHOT,
        EXPECTED_MOVE_SOURCE_ATM_STRADDLE,
    }


def is_estimated_expected_move_source(source: Any) -> bool:
    return normalize_expected_move_source(source) == EXPECTED_MOVE_SOURCE_ESTIMATED


def classify_expected_move_usage(source: Any, value: Any = None) -> str:
    if is_valid_expected_move(to_float(value)) and is_actual_expected_move_source(source):
        return EXPECTED_MOVE_USAGE_ACTUAL
    if is_valid_expected_move(to_float(value)) and is_estimated_expected_move_source(source):
        return EXPECTED_MOVE_USAGE_ESTIMATED
    return EXPECTED_MOVE_USAGE_EXCLUDED


def expected_move_confidence_for_source(source: Any) -> str:
    if is_actual_expected_move_source(source):
        return EXPECTED_MOVE_CONFIDENCE_HIGH
    if is_estimated_expected_move_source(source):
        return EXPECTED_MOVE_CONFIDENCE_LOW
    return EXPECTED_MOVE_CONFIDENCE_NONE


def expected_move_learning_weight(source: Any, confidence: Any = None) -> float:
    normalized_confidence = str(confidence or "").strip().lower()
    if normalized_confidence == EXPECTED_MOVE_CONFIDENCE_LOW or is_estimated_expected_move_source(source):
        return EXPECTED_MOVE_ESTIMATED_WEIGHT
    if normalized_confidence == EXPECTED_MOVE_CONFIDENCE_HIGH or is_actual_expected_move_source(source):
        return 1.0
    return 0.0


def normalize_distance_source(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in {DISTANCE_SOURCE_ORIGINAL, DISTANCE_SOURCE_DERIVED, DISTANCE_SOURCE_ESTIMATED, DISTANCE_SOURCE_UNRESOLVED}:
        return text
    if text == "calculated":
        return DISTANCE_SOURCE_DERIVED
    if text == "stored-fallback":
        return DISTANCE_SOURCE_ORIGINAL
    if text == "missing":
        return DISTANCE_SOURCE_UNRESOLVED
    return DISTANCE_SOURCE_UNRESOLVED


def normalize_expected_move_source(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in {
        EXPECTED_MOVE_SOURCE_ORIGINAL,
        EXPECTED_MOVE_SOURCE_RECOVERED_CANDIDATE,
        EXPECTED_MOVE_SOURCE_RECOVERED_SNAPSHOT,
        EXPECTED_MOVE_SOURCE_ESTIMATED,
        EXPECTED_MOVE_SOURCE_ESTIMATED_LEGACY,
        EXPECTED_MOVE_SOURCE_ATM_STRADDLE,
        EXPECTED_MOVE_SOURCE_UNRESOLVED,
    }:
        if text == EXPECTED_MOVE_SOURCE_ESTIMATED_LEGACY:
            return EXPECTED_MOVE_SOURCE_ESTIMATED
        return text
    return EXPECTED_MOVE_SOURCE_UNRESOLVED


def normalize_yes_no_flag(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in {"1", "true", "yes", "y"}:
        return "yes"
    if text in {"0", "false", "no", "n"}:
        return "no"
    return "no"


def is_apollo_trade(values: Dict[str, Any]) -> bool:
    return normalize_system_name(values.get("system_name") or values.get("system")) == "Apollo"


def is_kairos_trade(values: Dict[str, Any]) -> bool:
    return normalize_system_name(values.get("system_name") or values.get("system")) == "Kairos"


def build_expected_move_lookup_signature(values: Dict[str, Any]) -> Optional[tuple[Any, ...]]:
    if not values:
        return None
    trade_mode = values.get("trade_mode")
    normalized_trade_mode = None
    if trade_mode not in {None, ""}:
        normalized_trade_mode = normalize_trade_mode(trade_mode)
    return (
        _signature_value(normalized_trade_mode),
        _signature_value(normalize_system_name(values.get("system_name") or values.get("system"))),
        _signature_value(values.get("journal_name") or JOURNAL_NAME_DEFAULT),
        _signature_value(values.get("trade_date")),
        _signature_value(values.get("expiration_date")),
        _signature_value(to_float(values.get("short_strike"))),
        _signature_value(to_float(values.get("long_strike"))),
        _signature_value(to_int(values.get("contracts"))),
        _signature_value(first_float(values.get("actual_entry_credit"), values.get("candidate_credit_estimate"))),
    )


def _signature_value(value: Any) -> Any:
    if value in {None, ""}:
        return None
    return value


def coerce_field_value(field: str, value: Any) -> Any:
    if value in {None, ""}:
        return None
    if field in INTEGER_FIELDS:
        return to_int(value)
    if field in REAL_FIELDS:
        return to_float(value)
    if field in DATE_FIELDS:
        parsed = parse_date_value(value)
        return parsed.isoformat() if parsed else None
    if field in DATETIME_FIELDS:
        parsed = parse_datetime_value(value)
        return parsed.isoformat(timespec="minutes") if parsed else None
    return str(value).strip()


def to_float(value: Any) -> Optional[float]:
    if value in {None, ""}:
        return None
    return float(value)


def to_int(value: Any) -> Optional[int]:
    if value in {None, ""}:
        return None
    return int(float(value))


def parse_date_value(value: Any) -> Optional[datetime.date]:
    if value in {None, ""}:
        return None
    if hasattr(value, "isoformat") and not isinstance(value, str):
        return value
    return datetime.fromisoformat(str(value)).date()


def parse_datetime_value(value: Any) -> Optional[datetime]:
    if value in {None, ""}:
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    return datetime.fromisoformat(text)


def current_timestamp() -> str:
    return datetime.now(CHICAGO_TZ).isoformat(timespec="seconds")


def timestamp_seconds_ago(seconds: int) -> str:
    reference = datetime.now(CHICAGO_TZ) - timedelta(seconds=max(seconds, 0))
    return reference.replace(microsecond=0).isoformat(timespec="seconds")


def blank_trade_form(trade_mode: str) -> Dict[str, Any]:
    return {
        "trade_number": "",
        "trade_mode": normalize_trade_mode(trade_mode),
        "system_name": DEFAULT_SYSTEM_NAME,
        "journal_name": JOURNAL_NAME_DEFAULT,
        "system_version": "",
        "candidate_profile": DEFAULT_CANDIDATE_PROFILE,
        "status": "open",
        "trade_date": "",
        "entry_datetime": "",
        "expiration_date": "",
        "underlying_symbol": "SPX",
        "spx_at_entry": "",
        "vix_at_entry": "",
        "structure_grade": "",
        "macro_grade": "",
        "expected_move": "",
        "expected_move_used": "",
        "expected_move_source": "",
        "option_type": "Put Credit Spread",
        "short_strike": "",
        "long_strike": "",
        "spread_width": "",
        "contracts": "",
        "candidate_credit_estimate": "",
        "actual_entry_credit": "",
        "distance_to_short": "",
        "em_multiple_floor": "",
        "percent_floor": "",
        "boundary_rule_used": "",
        "actual_distance_to_short": "",
        "actual_em_multiple": "",
        "pass_type": "",
        "premium_per_contract": "",
        "total_premium": "",
        "max_theoretical_risk": "",
        "risk_efficiency": "",
        "target_em": "",
        "prefill_source": "",
        "automation_status": "",
        "fallback_used": "no",
        "fallback_rule_name": "",
        "short_delta": "",
        "notes_entry": "",
        "exit_datetime": "",
        "spx_at_exit": "",
        "actual_exit_value": "",
        "close_method": "",
        "close_reason": "",
        "notes_exit": "",
    }


def form_trade_record(trade: Dict[str, Any]) -> Dict[str, Any]:
    values = {field: format_form_value(trade.get(field)) for field in EDITABLE_FIELDS}
    if trade.get("derived_status_label"):
        values["status"] = format_form_value(trade.get("derived_status_label"))
    values["expected_move_source"] = trade.get("expected_move_source")
    values["distance_source"] = trade.get("distance_source")
    values["fallback_used"] = normalize_yes_no_flag(trade.get("fallback_used"))
    return values


def format_form_value(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def summarize_trade_row(trade: Dict[str, Any]) -> Dict[str, Any]:
    return {
        **trade,
        "trade_date_display": trade.get("trade_date") or "—",
        "expiration_date_display": trade.get("expiration_date") or "—",
        "candidate_profile_display": trade.get("candidate_profile") or "—",
        "gross_pnl_display": trade.get("gross_pnl"),
        "max_risk_display": trade.get("max_risk"),
        "roi_on_risk_display": trade.get("roi_on_risk"),
        "win_loss_result_display": trade.get("win_loss_result") or "—",
    }
