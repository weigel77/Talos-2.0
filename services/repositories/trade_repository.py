"""Trade journal repository abstraction and adapters."""

from __future__ import annotations

import logging
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from threading import RLock
from typing import Any, Dict, List, Protocol, runtime_checkable

from services.trade_store import TradeStore
from services.trade_store import (
    EDITABLE_FIELDS,
    JOURNAL_NAME_DEFAULT,
    build_real_trade_outcome_profile,
    build_trade_duplicate_signature,
    current_timestamp,
    default_close_reason,
    derive_close_event_type,
    normalize_close_method,
    normalize_trade_mode,
    normalize_trade_payload,
    parse_datetime_value,
    summarize_trade_close_events,
    timestamp_seconds_ago,
    to_float,
    to_int,
)
from services.runtime.supabase_integration import SupabaseRequestError, SupabaseRuntimeContext, SupabaseTableGateway

LOGGER = logging.getLogger(__name__)


@runtime_checkable
class TradeRepository(Protocol):
    """Abstract trade journal persistence contract."""

    @property
    def database_path(self) -> str:
        ...

    def initialize(self) -> None:
        ...

    def next_trade_number(self) -> int:
        ...

    def find_recent_duplicate(self, values: Dict[str, Any], window_seconds: int = 15) -> Dict[str, Any] | None:
        ...

    def create_trade(self, values: Dict[str, Any]) -> int:
        ...

    def get_trade(self, trade_id: int) -> Dict[str, Any] | None:
        ...

    def get_trade_by_number(self, trade_number: int) -> Dict[str, Any] | None:
        ...

    def update_trade(self, trade_id: int, values: Dict[str, Any]) -> None:
        ...

    def delete_trade(self, trade_id: int) -> None:
        ...

    def reduce_trade(self, trade_id: int, values: Dict[str, Any]) -> None:
        ...

    def expire_trade(self, trade_id: int, values: Dict[str, Any]) -> None:
        ...

    def find_duplicate_trade(self, values: Dict[str, Any]) -> Dict[str, Any] | None:
        ...

    def list_trades(self, trade_mode: str) -> List[Dict[str, Any]]:
        ...

    def summarize(self, trade_mode: str) -> Dict[str, Any]:
        ...

    def build_real_trade_outcome_profile(self) -> Dict[str, Any]:
        ...


class SQLiteTradeRepository:
    """Adapter that exposes the trade-store API through a repository seam."""

    def __init__(self, store: TradeStore) -> None:
        self._store = store

    @property
    def database_path(self) -> str:
        return str(self._store.database_path)

    def initialize(self) -> None:
        self._store.initialize()

    def next_trade_number(self) -> int:
        return self._store.next_trade_number()

    def find_recent_duplicate(self, values: Dict[str, Any], window_seconds: int = 15) -> Dict[str, Any] | None:
        return self._store.find_recent_duplicate(values, window_seconds=window_seconds)

    def create_trade(self, values: Dict[str, Any]) -> int:
        return self._store.create_trade(values)

    def get_trade(self, trade_id: int) -> Dict[str, Any] | None:
        return self._store.get_trade(trade_id)

    def get_trade_by_number(self, trade_number: int) -> Dict[str, Any] | None:
        return self._store.get_trade_by_number(trade_number)

    def update_trade(self, trade_id: int, values: Dict[str, Any]) -> None:
        self._store.update_trade(trade_id, values)

    def delete_trade(self, trade_id: int) -> None:
        self._store.delete_trade(trade_id)

    def reduce_trade(self, trade_id: int, values: Dict[str, Any]) -> None:
        self._store.reduce_trade(trade_id, values)

    def expire_trade(self, trade_id: int, values: Dict[str, Any]) -> None:
        self._store.expire_trade(trade_id, values)

    def find_duplicate_trade(self, values: Dict[str, Any]) -> Dict[str, Any] | None:
        return self._store.find_duplicate_trade(values)

    def list_trades(self, trade_mode: str) -> List[Dict[str, Any]]:
        return self._store.list_trades(trade_mode)

    def summarize(self, trade_mode: str) -> Dict[str, Any]:
        return self._store.summarize(trade_mode)

    def build_real_trade_outcome_profile(self) -> Dict[str, Any]:
        return self._store.build_real_trade_outcome_profile()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._store, name)


class MirroredTradeRepository:
    """Local journal repository that mirrors writes into Supabase while serving local runtime reads from SQLite."""

    MIRROR_READ_SYNC_TTL_SECONDS = 20
    MIRROR_READ_SYNC_RETRY_BACKOFF_SECONDS = 30

    def __init__(self, *, local_repository: TradeRepository, remote_repository: TradeRepository) -> None:
        self._local_repository = local_repository
        self._remote_repository = remote_repository
        self._sync_lock = RLock()
        self._last_read_hydrate_ts = 0.0
        self._last_read_hydrate_failure_ts = 0.0

    @property
    def database_path(self) -> str:
        return self._local_repository.database_path

    def initialize(self) -> None:
        self._local_repository.initialize()
        self._remote_repository.initialize()
        self._hydrate_local_read_model_from_remote(force=True)

    def next_trade_number(self) -> int:
        try:
            return max(self._local_repository.next_trade_number(), self._remote_repository.next_trade_number())
        except Exception:
            return self._local_repository.next_trade_number()

    def find_recent_duplicate(self, values: Dict[str, Any], window_seconds: int = 15) -> Dict[str, Any] | None:
        duplicate = self._remote_repository.find_recent_duplicate(values, window_seconds=window_seconds)
        if duplicate is not None:
            return duplicate
        return self._local_repository.find_recent_duplicate(values, window_seconds=window_seconds)

    def create_trade(self, values: Dict[str, Any]) -> int:
        with self._sync_lock:
            remote_payload = dict(values)
            remote_trade_id = self._remote_repository.create_trade(remote_payload)
            remote_trade = self._remote_repository.get_trade(remote_trade_id) or {**remote_payload, "id": remote_trade_id}
            return self._upsert_local_from_remote(remote_trade)

    def get_trade(self, trade_id: int) -> Dict[str, Any] | None:
        self._hydrate_local_read_model_from_remote()
        return self._local_repository.get_trade(trade_id)

    def get_trade_by_number(self, trade_number: int) -> Dict[str, Any] | None:
        self._hydrate_local_read_model_from_remote()
        return self._local_repository.get_trade_by_number(trade_number)

    def update_trade(self, trade_id: int, values: Dict[str, Any]) -> None:
        with self._sync_lock:
            local_trade = self._require_local_trade(trade_id)
            remote_trade = self._ensure_remote_trade_for_local_trade(local_trade)
            remote_values = self._translate_close_event_ids_for_remote(local_trade, remote_trade, values)
            self._remote_repository.update_trade(int(remote_trade["id"]), remote_values)
            refreshed_remote_trade = self._remote_repository.get_trade(int(remote_trade["id"])) or remote_trade
            self._synchronize_local_trade(trade_id, refreshed_remote_trade)

    def delete_trade(self, trade_id: int) -> None:
        with self._sync_lock:
            local_trade = self._require_local_trade(trade_id)
            remote_trade = self._find_remote_trade_by_number(local_trade.get("trade_number"))
            if remote_trade is not None:
                self._remote_repository.delete_trade(int(remote_trade["id"]))
            self._local_repository.delete_trade(trade_id)

    def reduce_trade(self, trade_id: int, values: Dict[str, Any]) -> None:
        with self._sync_lock:
            local_trade = self._require_local_trade(trade_id)
            remote_trade = self._ensure_remote_trade_for_local_trade(local_trade)
            self._remote_repository.reduce_trade(int(remote_trade["id"]), values)
            refreshed_remote_trade = self._remote_repository.get_trade(int(remote_trade["id"])) or remote_trade
            self._synchronize_local_trade(trade_id, refreshed_remote_trade)

    def expire_trade(self, trade_id: int, values: Dict[str, Any]) -> None:
        with self._sync_lock:
            local_trade = self._require_local_trade(trade_id)
            remote_trade = self._ensure_remote_trade_for_local_trade(local_trade)
            self._remote_repository.expire_trade(int(remote_trade["id"]), values)
            refreshed_remote_trade = self._remote_repository.get_trade(int(remote_trade["id"])) or remote_trade
            self._synchronize_local_trade(trade_id, refreshed_remote_trade)

    def find_duplicate_trade(self, values: Dict[str, Any]) -> Dict[str, Any] | None:
        duplicate = self._remote_repository.find_duplicate_trade(values)
        if duplicate is not None:
            return duplicate
        return self._local_repository.find_duplicate_trade(values)

    def list_trades(self, trade_mode: str) -> List[Dict[str, Any]]:
        self._hydrate_local_read_model_from_remote()
        return self._local_repository.list_trades(trade_mode)

    def summarize(self, trade_mode: str) -> Dict[str, Any]:
        self._hydrate_local_read_model_from_remote()
        return self._local_repository.summarize(trade_mode)

    def build_real_trade_outcome_profile(self) -> Dict[str, Any]:
        self._hydrate_local_read_model_from_remote()
        return self._local_repository.build_real_trade_outcome_profile()

    def _hydrate_local_read_model_from_remote(self, *, force: bool = False) -> None:
        now = time.monotonic()
        if not force:
            if (now - self._last_read_hydrate_ts) < self.MIRROR_READ_SYNC_TTL_SECONDS:
                return
            if self._last_read_hydrate_failure_ts and (now - self._last_read_hydrate_failure_ts) < self.MIRROR_READ_SYNC_RETRY_BACKOFF_SECONDS:
                return

        with self._sync_lock:
            now = time.monotonic()
            if not force:
                if (now - self._last_read_hydrate_ts) < self.MIRROR_READ_SYNC_TTL_SECONDS:
                    return
                if self._last_read_hydrate_failure_ts and (now - self._last_read_hydrate_failure_ts) < self.MIRROR_READ_SYNC_RETRY_BACKOFF_SECONDS:
                    return

            try:
                from services.local_trade_sync_service import sync_hosted_trade_journal_to_local

                for mode in ("real", "simulated", "talos"):
                    sync_hosted_trade_journal_to_local(
                        hosted_repository=self._remote_repository,
                        local_repository=self._local_repository,
                        trade_modes=(mode,),
                        replace_local_state=True,
                    )
                self._last_read_hydrate_ts = now
                self._last_read_hydrate_failure_ts = 0.0
            except Exception as exc:  # pragma: no cover - defensive mirror hydration guard
                self._last_read_hydrate_failure_ts = now
                LOGGER.warning("Unable to hydrate local trade mirror from Supabase: %s", exc)

    def _require_local_trade(self, trade_id: int) -> Dict[str, Any]:
        trade = self._local_repository.get_trade(trade_id)
        if trade is None:
            raise ValueError("Trade not found.")
        return trade

    def _upsert_local_from_remote(self, remote_trade: Dict[str, Any]) -> int:
        trade_number = to_int(remote_trade.get("trade_number"))
        if trade_number is None:
            raise ValueError("Mirrored trade is missing a trade number.")
        existing_local_trade = self._local_repository.get_trade_by_number(trade_number)
        if existing_local_trade is not None:
            self._synchronize_local_trade(int(existing_local_trade["id"]), remote_trade)
            return int(existing_local_trade["id"])

        create_payload = _build_trade_sync_payload(remote_trade)
        close_events = create_payload.pop("close_events", [])
        local_trade_id = self._local_repository.create_trade(create_payload)
        if close_events:
            self._local_repository.update_trade(local_trade_id, {**create_payload, "close_events": close_events})
        return local_trade_id

    def _synchronize_local_trade(self, local_trade_id: int, remote_trade: Dict[str, Any]) -> None:
        payload = _build_trade_sync_payload(remote_trade)
        self._local_repository.update_trade(local_trade_id, payload)

    def _ensure_remote_trade_for_local_trade(self, local_trade: Dict[str, Any]) -> Dict[str, Any]:
        remote_trade = self._find_remote_trade_by_number(local_trade.get("trade_number"))
        if remote_trade is not None:
            return remote_trade

        payload = _build_trade_sync_payload(local_trade)
        close_events = payload.pop("close_events", [])
        remote_trade_id = self._remote_repository.create_trade(payload)
        if close_events:
            self._remote_repository.update_trade(remote_trade_id, {**payload, "close_events": close_events})
        return self._remote_repository.get_trade(remote_trade_id) or {**payload, "id": remote_trade_id}

    def _find_remote_trade_by_number(self, trade_number: Any) -> Dict[str, Any] | None:
        normalized_trade_number = to_int(trade_number)
        if normalized_trade_number is None:
            return None
        remote_lookup = getattr(self._remote_repository, "get_trade_by_number", None)
        if callable(remote_lookup):
            return remote_lookup(normalized_trade_number)
        for mode in ("real", "simulated", "talos"):
            for trade in self._remote_repository.list_trades(mode):
                if to_int(trade.get("trade_number")) == normalized_trade_number:
                    return trade
        return None

    def _translate_close_event_ids_for_remote(
        self,
        local_trade: Dict[str, Any],
        remote_trade: Dict[str, Any],
        values: Dict[str, Any],
    ) -> Dict[str, Any]:
        close_events = values.get("close_events") if "close_events" in values else None
        if close_events is None:
            return dict(values)

        local_events = list(local_trade.get("close_events") or [])
        remote_events = list(remote_trade.get("close_events") or [])
        local_to_remote_id: dict[int, int] = {}
        for local_event, remote_event in zip(local_events, remote_events):
            local_event_id = to_int((local_event or {}).get("id"))
            remote_event_id = to_int((remote_event or {}).get("id"))
            if local_event_id is None or remote_event_id is None:
                continue
            local_to_remote_id[local_event_id] = remote_event_id

        remote_values = dict(values)
        remote_values["close_events"] = []
        for row in close_events or []:
            translated_row = dict(row or {})
            local_event_id = to_int(translated_row.get("id"))
            if local_event_id is not None and local_event_id in local_to_remote_id:
                translated_row["id"] = local_to_remote_id[local_event_id]
            remote_values["close_events"].append(translated_row)
        return remote_values

    def __getattr__(self, name: str) -> Any:
        return getattr(self._local_repository, name)


def _build_trade_sync_payload(trade: Dict[str, Any]) -> Dict[str, Any]:
    payload = {
        field: trade.get(field)
        for field in EDITABLE_FIELDS
        if field in trade
    }
    payload["close_events"] = [_build_trade_close_event_sync_payload(event) for event in (trade.get("close_events") or [])]
    return payload


def _build_trade_close_event_sync_payload(event: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "contracts_closed": event.get("contracts_closed"),
        "actual_exit_value": event.get("actual_exit_value"),
        "event_datetime": event.get("event_datetime") or event.get("created_at"),
        "close_method": event.get("close_method"),
        "notes_exit": event.get("notes_exit"),
    }


class SupabaseTradeRepository:
    """Supabase-backed trade repository for hosted Delphi runtime composition."""

    TRADES_TABLE = "journal_trades"
    CLOSE_EVENTS_TABLE = "journal_trade_close_events"
    TRADE_IDENTITY_REPAIR_RPC = "sync_journal_trade_identity_sequence"
    CLOSE_EVENT_IDENTITY_REPAIR_RPC = "sync_journal_trade_close_event_identity_sequence"
    TALOS_COMPATIBLE_TRADE_MODE = "simulated"
    TALOS_JOURNAL_PREFIX = "Talos::"

    def __init__(
        self,
        *,
        context: SupabaseRuntimeContext,
        gateway: SupabaseTableGateway,
        database_path: str | Path,
        market_data_service: Any | None = None,
    ) -> None:
        self.context = context
        self.gateway = gateway
        self._database_path = str(database_path)
        self.market_data_service = market_data_service
        self._trade_storage_available: bool | None = None
        self._unsupported_columns_by_table: dict[str, set[str]] = {
            self.TRADES_TABLE: set(),
            self.CLOSE_EVENTS_TABLE: set(),
        }

    @property
    def database_path(self) -> str:
        return self._database_path

    def initialize(self) -> None:
        return None

    def is_trade_storage_available(self) -> bool:
        if self._trade_storage_available is not None:
            return self._trade_storage_available
        try:
            self.gateway.select(self.TRADES_TABLE, limit=1, columns="id")
            self.gateway.select(self.CLOSE_EVENTS_TABLE, limit=1, columns="id")
        except SupabaseRequestError as exc:
            if self._is_missing_table_error(exc, self.TRADES_TABLE) or self._is_missing_table_error(exc, self.CLOSE_EVENTS_TABLE):
                self._trade_storage_available = False
                return False
            raise
        self._trade_storage_available = True
        return True

    def next_trade_number(self) -> int:
        rows = self._select_rows(self.TRADES_TABLE, order="trade_number.desc,id.desc", limit=1)
        trade_number = to_int(rows[0].get("trade_number")) if rows else None
        return (trade_number or 0) + 1

    def find_recent_duplicate(self, values: Dict[str, Any], window_seconds: int = 15) -> Dict[str, Any] | None:
        normalized = normalize_trade_payload(values)
        threshold = parse_datetime_value(timestamp_seconds_ago(window_seconds))
        rows = self._select_rows(
            self.TRADES_TABLE,
            filters={"trade_mode": f"eq.{self._encode_trade_mode_filter(normalized.get('trade_mode'))}"},
            order="id.desc",
            limit=25,
        )
        for row in rows:
            row = self._decode_trade_row(dict(row))
            created_at = parse_datetime_value(row.get("created_at"))
            if threshold is not None and (created_at is None or created_at < threshold):
                continue
            if not self._matches_recent_duplicate_candidate(normalized, row):
                continue
            return dict(row)
        return None

    def create_trade(self, values: Dict[str, Any]) -> int:
        normalized = normalize_trade_payload(values)
        normalized = self._encode_trade_payload(normalized)
        timestamp = current_timestamp()
        normalized["created_at"] = timestamp
        normalized["updated_at"] = timestamp
        trade_number = normalized.get("trade_number")
        if trade_number is None:
            normalized["trade_number"] = self.next_trade_number()
        else:
            self._ensure_trade_number_available(int(trade_number))
        inserted = self._insert_trade_row(normalized)
        if not inserted:
            raise ValueError("Supabase trade insert did not return a row.")
        return int(inserted[0]["id"])

    def _insert_trade_row(self, normalized: Dict[str, Any]) -> list[dict[str, Any]]:
        serialized = self._serialize_payload(self.TRADES_TABLE, normalized)
        return self._insert_with_identity_recovery(
            table=self.TRADES_TABLE,
            payload=serialized,
            repair_rpc=self.TRADE_IDENTITY_REPAIR_RPC,
            resolve_next_id=self._resolve_next_trade_id,
        )

    def _insert_with_identity_recovery(
        self,
        *,
        table: str,
        payload: Dict[str, Any],
        repair_rpc: str,
        resolve_next_id,
    ) -> list[dict[str, Any]]:
        try:
            return self._insert_table_payload(table, payload)
        except SupabaseRequestError as exc:
            if not self._is_duplicate_primary_key_error(exc, table=table):
                raise
            LOGGER.warning("Supabase %s identity sequence appears out of sync. Attempting repair RPC.", table)
            try:
                self.gateway.rpc(repair_rpc, {})
                return self._insert_table_payload(table, payload)
            except SupabaseRequestError as repair_exc:
                LOGGER.warning(
                    "Supabase %s repair RPC was unavailable. Falling back to an explicit id insert. Details: %s",
                    table,
                    repair_exc,
                )
                fallback_payload = dict(payload)
                fallback_payload["id"] = resolve_next_id()
                return self._insert_table_payload(table, fallback_payload)

    def _resolve_next_trade_id(self) -> int:
        rows = self._select_rows(self.TRADES_TABLE, order="id.desc", limit=1, columns="id")
        current_max = to_int(rows[0].get("id")) if rows else None
        return (current_max or 0) + 1

    def _resolve_next_close_event_id(self) -> int:
        rows = self._select_rows(self.CLOSE_EVENTS_TABLE, order="id.desc", limit=1, columns="id")
        current_max = to_int(rows[0].get("id")) if rows else None
        return (current_max or 0) + 1

    def get_trade(self, trade_id: int) -> Dict[str, Any] | None:
        rows = self._select_rows(self.TRADES_TABLE, filters={"id": f"eq.{int(trade_id)}"}, limit=1)
        return self._attach_trade_state(self._decode_trade_row(dict(rows[0]))) if rows else None

    def get_trade_by_number(self, trade_number: int) -> Dict[str, Any] | None:
        rows = self._select_rows(self.TRADES_TABLE, filters={"trade_number": f"eq.{int(trade_number)}"}, limit=1)
        return self._attach_trade_state(self._decode_trade_row(dict(rows[0]))) if rows else None

    def update_trade(self, trade_id: int, values: Dict[str, Any]) -> None:
        existing = self.get_trade(trade_id)
        if not existing:
            raise ValueError("Trade not found.")
        close_events_payload = values.get("close_events") if "close_events" in values else None
        normalized = normalize_trade_payload(values, existing=existing)
        normalized = self._encode_trade_payload(normalized)
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
        trade_number = normalized.get("trade_number") or existing.get("trade_number")
        if trade_number is None:
            trade_number = self.next_trade_number()
        self._ensure_trade_number_available(int(trade_number), exclude_id=int(trade_id))
        normalized["trade_number"] = int(trade_number)
        self._update_table_payload(
            self.TRADES_TABLE,
            normalized,
            filters={"id": f"eq.{int(trade_id)}"},
        )
        if close_events_payload is not None:
            synchronized_events = self._normalize_close_events_payload(close_events_payload, trade_contracts=normalized.get("contracts"))
            self._replace_close_events(int(trade_id), synchronized_events)
            self._refresh_trade_after_close_events(int(trade_id))

    def delete_trade(self, trade_id: int) -> None:
        self.gateway.delete(self.TRADES_TABLE, filters={"id": f"eq.{int(trade_id)}"})

    def reduce_trade(self, trade_id: int, values: Dict[str, Any]) -> None:
        trade = self.get_trade(trade_id)
        if not trade:
            raise ValueError("Trade not found.")
        if trade.get("derived_status_raw") in {"closed", "expired", "cancelled"}:
            raise ValueError("Only open trades can be reduced.")
        remaining_contracts = int(trade.get("remaining_contracts") or 0)
        contracts_closed = to_int(values.get("contracts_closed"))
        actual_exit_value = to_float(values.get("actual_exit_value"))
        if contracts_closed is None or contracts_closed <= 0:
            raise ValueError("Reduce quantity must be at least 1 contract.")
        if remaining_contracts <= 0 or contracts_closed > remaining_contracts:
            raise ValueError("Reduce quantity cannot exceed the remaining open contracts.")
        if actual_exit_value is None or actual_exit_value < 0:
            raise ValueError("Reduce exit value must be zero or greater.")
        self._insert_close_event(
            trade_id=int(trade_id),
            event_type="reduce",
            contracts_closed=contracts_closed,
            actual_exit_value=actual_exit_value,
            event_datetime=values.get("event_datetime") or current_timestamp(),
            spx_at_exit=values.get("spx_at_exit"),
            close_method=values.get("close_method") or "Reduce",
            close_reason=values.get("close_reason") or f"Reduced {contracts_closed} contract{'s' if contracts_closed != 1 else ''}",
            notes_exit=values.get("notes_exit") or "",
        )
        self._refresh_trade_after_close_events(int(trade_id))

    def expire_trade(self, trade_id: int, values: Dict[str, Any]) -> None:
        payload = values or {}
        trade = self.get_trade(trade_id)
        if not trade:
            raise ValueError("Trade not found.")
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
            trade_id=int(trade_id),
            event_type="expire",
            contracts_closed=remaining_contracts,
            actual_exit_value=actual_exit_value,
            event_datetime=payload.get("event_datetime") or current_timestamp(),
            spx_at_exit=payload.get("spx_at_exit"),
            close_method=payload.get("close_method") or "Expire",
            close_reason=payload.get("close_reason") or "Expired Worthless",
            notes_exit=payload.get("notes_exit") or "",
        )
        self._refresh_trade_after_close_events(int(trade_id))

    def find_duplicate_trade(self, values: Dict[str, Any]) -> Dict[str, Any] | None:
        normalized = normalize_trade_payload(values)
        target_signature = build_trade_duplicate_signature(normalized, already_normalized=True)
        for row in self._select_rows(self.TRADES_TABLE, filters={"trade_mode": f"eq.{self._encode_trade_mode_filter(normalized['trade_mode'])}"}, order="id.desc"):
            candidate = self._decode_trade_row(dict(row))
            if build_trade_duplicate_signature(candidate, already_normalized=True) == target_signature:
                return candidate
        return None

    def list_trades(self, trade_mode: str) -> List[Dict[str, Any]]:
        normalized_trade_mode = normalize_trade_mode(trade_mode)
        rows = self._select_rows(self.TRADES_TABLE, filters={"trade_mode": f"eq.{self._encode_trade_mode_filter(normalized_trade_mode)}"})
        close_events_by_trade_id = self._select_close_events_for_trade_ids(
            int(row.get("id") or 0)
            for row in rows
            if row.get("id") is not None
        )
        trades = [
            self._attach_trade_state(
                self._decode_trade_row(dict(row)),
                close_events=close_events_by_trade_id.get(int(row.get("id") or 0), []),
            )
            for row in rows
            if self._matches_requested_trade_mode(dict(row), normalized_trade_mode)
        ]
        trades.sort(key=self._sort_key, reverse=True)
        return trades

    def summarize(self, trade_mode: str) -> Dict[str, Any]:
        rows = self.list_trades(trade_mode)
        total_pnl = sum(float(row.get("gross_pnl") or 0.0) for row in rows)
        pnl_values = [float(row.get("gross_pnl") or 0.0) for row in rows if row.get("gross_pnl") is not None]
        return {
            "total_trades": len(rows),
            "open_trades": sum(1 for row in rows if row.get("derived_status_raw") in {"open", "reduced"}),
            "closed_trades": sum(1 for row in rows if row.get("derived_status_raw") not in {"open", "reduced"}),
            "total_pnl": total_pnl,
            "average_pnl": (sum(pnl_values) / len(pnl_values)) if pnl_values else 0.0,
            "win_count": sum(1 for row in rows if row.get("win_loss_result") == "Win"),
            "loss_count": sum(1 for row in rows if row.get("win_loss_result") in {"Loss", "Black Swan"}),
        }

    def build_real_trade_outcome_profile(self) -> Dict[str, Any]:
        return build_real_trade_outcome_profile(self.list_trades("real"))

    def _attach_trade_state(self, trade: Dict[str, Any], *, close_events: list[Dict[str, Any]] | None = None) -> Dict[str, Any]:
        events = close_events if close_events is not None else (self._list_close_events(int(trade.get("id"))) if trade.get("id") is not None else [])
        summary = summarize_trade_close_events(trade, events)
        trade.update(summary)
        trade["close_events"] = events
        return trade

    def _list_close_events(self, trade_id: int) -> list[Dict[str, Any]]:
        rows = self._select_rows(
            self.CLOSE_EVENTS_TABLE,
            filters={"trade_id": f"eq.{int(trade_id)}"},
            order="event_datetime.asc.nullslast,id.asc",
        )
        return [dict(row) for row in rows]

    def _select_close_events_for_trade_ids(self, trade_ids: Any) -> dict[int, list[Dict[str, Any]]]:
        normalized_trade_ids = sorted({int(trade_id) for trade_id in trade_ids if int(trade_id) > 0})
        if not normalized_trade_ids:
            return {}
        rows = self._select_rows(
            self.CLOSE_EVENTS_TABLE,
            filters={"trade_id": f"in.({','.join(str(trade_id) for trade_id in normalized_trade_ids)})"},
            order="trade_id.asc,event_datetime.asc.nullslast,id.asc",
        )
        events_by_trade_id: dict[int, list[Dict[str, Any]]] = {trade_id: [] for trade_id in normalized_trade_ids}
        for row in rows:
            trade_id = int(row.get("trade_id") or 0)
            events_by_trade_id.setdefault(trade_id, []).append(dict(row))
        return events_by_trade_id

    def _select_rows(
        self,
        table: str,
        *,
        filters: dict[str, str] | None = None,
        order: str | None = None,
        limit: int | None = None,
        columns: str = "*",
    ) -> list[dict[str, Any]]:
        try:
            return self.gateway.select(table, filters=filters, order=order, limit=limit, columns=columns)
        except SupabaseRequestError as exc:
            if self._is_missing_table_error(exc, table):
                if table in {self.TRADES_TABLE, self.CLOSE_EVENTS_TABLE}:
                    self._trade_storage_available = False
                LOGGER.warning(
                    "Supabase table %s is unavailable; returning empty hosted trade data until migrations are applied.",
                    table,
                )
                return []
            raise

    @staticmethod
    def _is_missing_table_error(error: SupabaseRequestError, table: str) -> bool:
        detail = str(error)
        return "PGRST205" in detail and (f"'{table}'" in detail or f"'public.{table}'" in detail)

    def _insert_close_event(
        self,
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
        parsed_event_datetime = parse_datetime_value(event_datetime)
        self._insert_with_identity_recovery(
            table=self.CLOSE_EVENTS_TABLE,
            payload={
                "trade_id": int(trade_id),
                "created_at": current_timestamp(),
                "event_type": str(event_type or "").strip().lower(),
                "event_datetime": parsed_event_datetime.isoformat(timespec="minutes") if parsed_event_datetime else None,
                "contracts_closed": int(contracts_closed),
                "actual_exit_value": float(actual_exit_value),
                "spx_at_exit": to_float(spx_at_exit),
                "close_method": str(close_method or "").strip() or None,
                "close_reason": str(close_reason or "").strip() or None,
                "notes_exit": str(notes_exit or "").strip() or None,
            },
            repair_rpc=self.CLOSE_EVENT_IDENTITY_REPAIR_RPC,
            resolve_next_id=self._resolve_next_close_event_id,
        )

    def _normalize_close_events_payload(self, close_events: Any, *, trade_contracts: Any) -> list[Dict[str, Any]]:
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

    def _replace_close_events(self, trade_id: int, close_events: list[Dict[str, Any]]) -> None:
        existing_events = {int(row["id"]): dict(row) for row in self._list_close_events(trade_id)}
        retained_ids: set[int] = set()
        for row in close_events:
            event_id = row.get("id")
            if event_id is not None:
                existing_row = existing_events.get(int(event_id))
                if not existing_row:
                    raise ValueError("One or more close events could not be matched to this trade.")
                retained_ids.add(int(event_id))
                parsed_event_datetime = parse_datetime_value(row.get("event_datetime"))
                self._update_table_payload(
                    self.CLOSE_EVENTS_TABLE,
                    {
                        "event_type": row["event_type"],
                        "event_datetime": parsed_event_datetime.isoformat(timespec="minutes") if parsed_event_datetime else existing_row.get("event_datetime"),
                        "contracts_closed": row["contracts_closed"],
                        "actual_exit_value": row["actual_exit_value"],
                        "close_method": row["close_method"],
                        "close_reason": default_close_reason(row["close_method"], row["contracts_closed"]),
                        "notes_exit": row.get("notes_exit") or existing_row.get("notes_exit") or "",
                    },
                    filters={"id": f"eq.{int(event_id)}", "trade_id": f"eq.{int(trade_id)}"},
                )
                continue
            self._insert_close_event(
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
                self.gateway.delete(self.CLOSE_EVENTS_TABLE, filters={"id": f"eq.{int(event_id)}", "trade_id": f"eq.{int(trade_id)}"})

    def _refresh_trade_after_close_events(self, trade_id: int) -> None:
        trade = self._get_trade_row(trade_id)
        if not trade:
            return
        events = self._list_close_events(trade_id)
        summary = summarize_trade_close_events(trade, events)
        self._update_table_payload(
            self.TRADES_TABLE,
            {
                "updated_at": current_timestamp(),
                "status": summary.get("status"),
                "exit_datetime": summary.get("exit_datetime"),
                "spx_at_exit": summary.get("spx_at_exit"),
                "actual_exit_value": summary.get("actual_exit_value"),
                "close_method": summary.get("close_method"),
                "close_reason": summary.get("close_reason"),
                "notes_exit": summary.get("notes_exit"),
                "gross_pnl": summary.get("gross_pnl"),
                "max_risk": summary.get("max_risk"),
                "roi_on_risk": summary.get("roi_on_risk"),
                "hours_held": summary.get("hours_held"),
                "win_loss_result": summary.get("win_loss_result"),
            },
            filters={"id": f"eq.{int(trade_id)}"},
        )

    def _get_trade_row(self, trade_id: int) -> Dict[str, Any] | None:
        rows = self.gateway.select(self.TRADES_TABLE, filters={"id": f"eq.{int(trade_id)}"}, limit=1)
        return self._decode_trade_row(dict(rows[0])) if rows else None

    def _ensure_trade_number_available(self, trade_number: int, exclude_id: int | None = None) -> None:
        rows = self.gateway.select(self.TRADES_TABLE, filters={"trade_number": f"eq.{int(trade_number)}"})
        for row in rows:
            if exclude_id is not None and int(row.get("id") or 0) == int(exclude_id):
                continue
            raise ValueError("Trade number already exists.")

    def _encode_trade_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        encoded = dict(payload)
        if normalize_trade_mode(encoded.get("trade_mode") or "") != "talos":
            return encoded
        original_journal_name = str(encoded.get("journal_name") or JOURNAL_NAME_DEFAULT).strip() or JOURNAL_NAME_DEFAULT
        encoded["trade_mode"] = self.TALOS_COMPATIBLE_TRADE_MODE
        encoded["journal_name"] = f"{self.TALOS_JOURNAL_PREFIX}{original_journal_name}"
        return encoded

    def _decode_trade_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        decoded = dict(row)
        journal_name = str(decoded.get("journal_name") or "")
        if str(decoded.get("trade_mode") or "").strip().lower() == self.TALOS_COMPATIBLE_TRADE_MODE and journal_name.startswith(self.TALOS_JOURNAL_PREFIX):
            decoded["trade_mode"] = "talos"
            decoded["journal_name"] = journal_name[len(self.TALOS_JOURNAL_PREFIX) :] or JOURNAL_NAME_DEFAULT
        return decoded

    def _encode_trade_mode_filter(self, trade_mode: Any) -> str:
        normalized_trade_mode = normalize_trade_mode(str(trade_mode or ""))
        if normalized_trade_mode == "talos":
            return self.TALOS_COMPATIBLE_TRADE_MODE
        return normalized_trade_mode

    def _matches_requested_trade_mode(self, row: Dict[str, Any], requested_trade_mode: str) -> bool:
        journal_name = str(row.get("journal_name") or "")
        is_talos_encoded = journal_name.startswith(self.TALOS_JOURNAL_PREFIX)
        if requested_trade_mode == "talos":
            return is_talos_encoded
        if requested_trade_mode == "simulated":
            return not is_talos_encoded
        return True

    def _insert_table_payload(self, table: str, payload: Dict[str, Any]) -> list[dict[str, Any]]:
        serialized = self._serialize_payload(table, payload)
        while True:
            try:
                return self.gateway.insert(table, serialized)
            except SupabaseRequestError as exc:
                missing_column = self._extract_missing_column(exc, table)
                if missing_column is None or missing_column not in serialized:
                    raise
                self._mark_column_unsupported(table, missing_column)
                serialized.pop(missing_column, None)

    def _update_table_payload(self, table: str, payload: Dict[str, Any], *, filters: dict[str, str]) -> None:
        serialized = self._serialize_payload(table, payload)
        while serialized:
            try:
                self.gateway.update(table, serialized, filters=filters)
                return
            except SupabaseRequestError as exc:
                missing_column = self._extract_missing_column(exc, table)
                if missing_column is None or missing_column not in serialized:
                    raise
                self._mark_column_unsupported(table, missing_column)
                serialized.pop(missing_column, None)

    def _serialize_payload(self, table: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        unsupported_columns = self._unsupported_columns_by_table.setdefault(table, set())
        return {
            key: value
            for key, value in payload.items()
            if key not in unsupported_columns
        }

    def _mark_column_unsupported(self, table: str, column_name: str) -> None:
        self._unsupported_columns_by_table.setdefault(table, set()).add(column_name)

    @staticmethod
    def _extract_missing_column(error: SupabaseRequestError, table: str) -> str | None:
        detail = str(error or "")
        if "PGRST204" not in detail:
            return None
        match = re.search(r"Could not find the '([^']+)' column of '([^']+)' in the schema cache", detail)
        if not match:
            return None
        column_name, table_name = match.groups()
        if table_name not in {table, f"public.{table}"}:
            return None
        return column_name

    @staticmethod
    def _is_duplicate_primary_key_error(error: SupabaseRequestError, *, table: str) -> bool:
        detail = str(error or "")
        return "23505" in detail and f"{table}_pkey" in detail

    @staticmethod
    def _filter_value(value: Any) -> str:
        return "" if value in {None, ""} else str(value)

    @staticmethod
    def _numeric_filter_value(value: Any) -> str:
        integer_value = to_int(value)
        if integer_value is not None and str(value).strip() not in {"", "None"}:
            if to_float(value) == float(integer_value):
                return str(integer_value)
        normalized = to_float(value)
        return "-1" if normalized is None else str(normalized)

    @staticmethod
    def _sort_key(trade: Dict[str, Any]) -> tuple[datetime, int]:
        sort_value = parse_datetime_value(trade.get("entry_datetime"))
        if sort_value is None:
            sort_value = parse_datetime_value(trade.get("trade_date"))
        if sort_value is None:
            sort_value = parse_datetime_value(trade.get("created_at"))
        if sort_value is None:
            sort_value = datetime.min.replace(tzinfo=timezone.utc)
        elif sort_value.tzinfo is None:
            sort_value = sort_value.replace(tzinfo=timezone.utc)
        return (sort_value, int(trade.get("id") or 0))

    @staticmethod
    def _matches_recent_duplicate_candidate(normalized: Dict[str, Any], candidate: Dict[str, Any]) -> bool:
        return (
            str(candidate.get("trade_mode") or "") == str(normalized.get("trade_mode") or "")
            and str(candidate.get("system_name") or "") == str(normalized.get("system_name") or "")
            and str(candidate.get("journal_name") or "") == str(normalized.get("journal_name") or "")
            and str(candidate.get("candidate_profile") or "") == str(normalized.get("candidate_profile") or "")
            and str(candidate.get("underlying_symbol") or "") == str(normalized.get("underlying_symbol") or "")
            and to_float(candidate.get("short_strike")) == to_float(normalized.get("short_strike"))
            and to_float(candidate.get("long_strike")) == to_float(normalized.get("long_strike"))
            and to_int(candidate.get("contracts")) == to_int(normalized.get("contracts"))
            and str(candidate.get("status") or "") == str(normalized.get("status") or "")
        )