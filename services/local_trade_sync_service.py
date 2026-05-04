"""Reconcile journal trades between hosted Supabase and local SQLite stores."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Sequence

from services.repositories.trade_repository import TradeRepository
from services.trade_store import EDITABLE_FIELDS


SYNCABLE_TRADE_MODES: tuple[str, ...] = ("real", "simulated")


@dataclass(frozen=True)
class TradeModeSyncResult:
    mode: str
    hosted_count: int
    local_before_count: int
    inserted_count: int
    duplicate_skip_count: int
    local_after_count: int
    imported_trade_numbers: tuple[int, ...]
    renumbered_trade_numbers: tuple[tuple[int, int], ...]


@dataclass(frozen=True)
class HostedToLocalTradeSyncResult:
    mode_results: tuple[TradeModeSyncResult, ...]

    def for_mode(self, mode: str) -> TradeModeSyncResult | None:
        normalized = str(mode or "").strip().lower()
        for result in self.mode_results:
            if result.mode == normalized:
                return result
        return None


@dataclass(frozen=True)
class LocalToHostedTradeModeSyncResult:
    mode: str
    local_count: int
    hosted_before_count: int
    inserted_count: int
    duplicate_skip_count: int
    hosted_after_count: int
    exported_trade_numbers: tuple[int, ...]
    renumbered_trade_numbers: tuple[tuple[int, int], ...]


@dataclass(frozen=True)
class LocalToHostedTradeSyncResult:
    mode_results: tuple[LocalToHostedTradeModeSyncResult, ...]

    def for_mode(self, mode: str) -> LocalToHostedTradeModeSyncResult | None:
        normalized = str(mode or "").strip().lower()
        for result in self.mode_results:
            if result.mode == normalized:
                return result
        return None


@dataclass(frozen=True)
class TradeJournalReconciliationResult:
    hosted_to_local: HostedToLocalTradeSyncResult
    local_to_hosted: LocalToHostedTradeSyncResult


def sync_hosted_trade_journal_to_local(
    *,
    hosted_repository: TradeRepository,
    local_repository: TradeRepository,
    trade_modes: Sequence[str] = SYNCABLE_TRADE_MODES,
    replace_local_state: bool = False,
) -> HostedToLocalTradeSyncResult:
    if replace_local_state:
        mode_results = _replace_target_trade_journal(
            source_repository=hosted_repository,
            target_repository=local_repository,
            trade_modes=trade_modes,
        )
    else:
        mode_results = _sync_trade_journal_direction(
            source_repository=hosted_repository,
            target_repository=local_repository,
            trade_modes=trade_modes,
            renumber_source_on_collision=False,
            preserve_source_trade_numbers=True,
        )
    return HostedToLocalTradeSyncResult(
        mode_results=tuple(
            TradeModeSyncResult(
                mode=result["mode"],
                hosted_count=result["source_count"],
                local_before_count=result["target_before_count"],
                inserted_count=result["inserted_count"],
                duplicate_skip_count=result["duplicate_skip_count"],
                local_after_count=result["target_after_count"],
                imported_trade_numbers=result["synced_trade_numbers"],
                renumbered_trade_numbers=result["renumbered_trade_numbers"],
            )
            for result in mode_results
        )
    )


def sync_local_trade_journal_to_hosted(
    *,
    local_repository: TradeRepository,
    hosted_repository: TradeRepository,
    trade_modes: Sequence[str] = SYNCABLE_TRADE_MODES,
) -> LocalToHostedTradeSyncResult:
    mode_results = _sync_trade_journal_direction(
        source_repository=local_repository,
        target_repository=hosted_repository,
        trade_modes=trade_modes,
        renumber_source_on_collision=True,
        preserve_source_trade_numbers=False,
    )
    return LocalToHostedTradeSyncResult(
        mode_results=tuple(
            LocalToHostedTradeModeSyncResult(
                mode=result["mode"],
                local_count=result["source_count"],
                hosted_before_count=result["target_before_count"],
                inserted_count=result["inserted_count"],
                duplicate_skip_count=result["duplicate_skip_count"],
                hosted_after_count=result["target_after_count"],
                exported_trade_numbers=result["synced_trade_numbers"],
                renumbered_trade_numbers=result["renumbered_trade_numbers"],
            )
            for result in mode_results
        )
    )


def reconcile_trade_journal(
    *,
    hosted_repository: TradeRepository,
    local_repository: TradeRepository,
    trade_modes: Sequence[str] = SYNCABLE_TRADE_MODES,
) -> TradeJournalReconciliationResult:
    local_to_hosted = sync_local_trade_journal_to_hosted(
        local_repository=local_repository,
        hosted_repository=hosted_repository,
        trade_modes=trade_modes,
    )
    hosted_to_local = sync_hosted_trade_journal_to_local(
        hosted_repository=hosted_repository,
        local_repository=local_repository,
        trade_modes=trade_modes,
        replace_local_state=True,
    )
    return TradeJournalReconciliationResult(
        hosted_to_local=hosted_to_local,
        local_to_hosted=local_to_hosted,
    )


def _sync_trade_journal_direction(
    *,
    source_repository: TradeRepository,
    target_repository: TradeRepository,
    trade_modes: Sequence[str],
    renumber_source_on_collision: bool,
    preserve_source_trade_numbers: bool,
) -> list[dict[str, Any]]:
    mode_results: list[TradeModeSyncResult] = []
    for raw_mode in trade_modes:
        mode = str(raw_mode or "").strip().lower()
        if not mode:
            continue
        source_rows = list(source_repository.list_trades(mode))
        target_rows = list(target_repository.list_trades(mode))
        target_trade_numbers = {
            int(row.get("trade_number"))
            for row in target_rows
            if row.get("trade_number") not in {None, ""}
        }
        inserted_trade_numbers: list[int] = []
        renumbered_trade_numbers: list[tuple[int, int]] = []
        duplicate_skip_count = 0

        for source_trade in sorted(source_rows, key=_trade_order_key):
            trade_number = _to_int(source_trade.get("trade_number"))
            if trade_number is not None and trade_number in target_trade_numbers:
                continue
            payload = _build_sync_payload(source_trade)
            if target_repository.find_duplicate_trade(payload):
                duplicate_skip_count += 1
                continue
            insert_payload = _prepare_insert_payload(payload)
            close_events = insert_payload.pop("close_events", [])
            inserted_id: int
            try:
                inserted_id = target_repository.create_trade(insert_payload)
            except ValueError as exc:
                if not _is_trade_number_collision(exc) or insert_payload.get("trade_number") in {None, ""}:
                    raise
                original_trade_number = _to_int(insert_payload.get("trade_number"))
                if preserve_source_trade_numbers and original_trade_number is not None:
                    conflicting_target_trade = _get_trade_by_number(target_repository, original_trade_number)
                    conflicting_target_trade_id = _to_int((conflicting_target_trade or {}).get("id"))
                    if conflicting_target_trade_id is not None:
                        replacement_trade_number = target_repository.next_trade_number()
                        target_repository.update_trade(
                            conflicting_target_trade_id,
                            {**_build_sync_payload(conflicting_target_trade), "trade_number": replacement_trade_number},
                        )
                        renumbered_trade_numbers.append((original_trade_number, replacement_trade_number))
                        inserted_id = target_repository.create_trade(insert_payload)
                        if close_events:
                            target_repository.update_trade(inserted_id, {**insert_payload, "close_events": close_events})
                        target_trade_number = _to_int((target_repository.get_trade(inserted_id) or {}).get("trade_number")) or original_trade_number
                        target_trade_numbers.add(target_trade_number)
                        inserted_trade_numbers.append(original_trade_number)
                        continue
                insert_payload.pop("trade_number", None)
                inserted_id = target_repository.create_trade(insert_payload)
                inserted_trade = target_repository.get_trade(inserted_id) or {}
                target_trade_number = _to_int(inserted_trade.get("trade_number"))
                if original_trade_number is not None and target_trade_number is not None:
                    renumbered_trade_numbers.append((original_trade_number, target_trade_number))
                    if renumber_source_on_collision:
                        source_trade_id = _to_int(source_trade.get("id"))
                        if source_trade_id is not None:
                            updated_source_payload = _build_sync_payload(inserted_trade)
                            source_repository.update_trade(source_trade_id, updated_source_payload)
            if close_events:
                target_repository.update_trade(inserted_id, {**insert_payload, "close_events": close_events})
            if trade_number is not None:
                target_trade_number = _to_int((target_repository.get_trade(inserted_id) or {}).get("trade_number")) or trade_number
                target_trade_numbers.add(target_trade_number)
                inserted_trade_numbers.append(trade_number)

        target_after_count = len(target_repository.list_trades(mode))
        mode_results.append(
            {
                "mode": mode,
                "source_count": len(source_rows),
                "target_before_count": len(target_rows),
                "inserted_count": len(inserted_trade_numbers),
                "duplicate_skip_count": duplicate_skip_count,
                "target_after_count": target_after_count,
                "synced_trade_numbers": tuple(inserted_trade_numbers),
                "renumbered_trade_numbers": tuple(renumbered_trade_numbers),
            }
        )

    return mode_results


def _replace_target_trade_journal(
    *,
    source_repository: TradeRepository,
    target_repository: TradeRepository,
    trade_modes: Sequence[str],
) -> list[dict[str, Any]]:
    normalized_modes = [str(raw_mode or "").strip().lower() for raw_mode in trade_modes if str(raw_mode or "").strip()]
    source_rows_by_mode = {mode: list(source_repository.list_trades(mode)) for mode in normalized_modes}
    target_rows_by_mode = {mode: list(target_repository.list_trades(mode)) for mode in normalized_modes}

    target_rows_to_delete = [
        trade
        for mode in normalized_modes
        for trade in target_rows_by_mode[mode]
    ]
    for target_trade in sorted(target_rows_to_delete, key=_trade_order_key, reverse=True):
        target_trade_id = _to_int(target_trade.get("id"))
        if target_trade_id is not None:
            target_repository.delete_trade(target_trade_id)

    inserted_trade_numbers_by_mode: dict[str, list[int]] = {mode: [] for mode in normalized_modes}
    for mode in normalized_modes:
        for source_trade in sorted(source_rows_by_mode[mode], key=_trade_order_key):
            payload = _prepare_insert_payload(_build_sync_payload(source_trade))
            close_events = payload.pop("close_events", [])
            inserted_id = target_repository.create_trade(payload)
            if close_events:
                target_repository.update_trade(inserted_id, {**payload, "close_events": close_events})
            trade_number = _to_int(source_trade.get("trade_number"))
            if trade_number is not None:
                inserted_trade_numbers_by_mode[mode].append(trade_number)

    mode_results: list[dict[str, Any]] = []
    for mode in normalized_modes:
        mode_results.append(
            {
                "mode": mode,
                "source_count": len(source_rows_by_mode[mode]),
                "target_before_count": len(target_rows_by_mode[mode]),
                "inserted_count": len(inserted_trade_numbers_by_mode[mode]),
                "duplicate_skip_count": 0,
                "target_after_count": len(target_repository.list_trades(mode)),
                "synced_trade_numbers": tuple(inserted_trade_numbers_by_mode[mode]),
                "renumbered_trade_numbers": (),
            }
        )
    return mode_results


def _build_sync_payload(trade: dict[str, Any]) -> dict[str, Any]:
    payload = {
        field: trade.get(field)
        for field in EDITABLE_FIELDS
        if field in trade
    }
    payload["close_events"] = [_build_sync_close_event_payload(event) for event in (trade.get("close_events") or [])]
    return payload


def _build_sync_close_event_payload(event: dict[str, Any]) -> dict[str, Any]:
    return {
        "contracts_closed": event.get("contracts_closed"),
        "actual_exit_value": event.get("actual_exit_value"),
        "event_datetime": event.get("event_datetime") or event.get("created_at"),
        "close_method": _normalize_close_method_for_sync(event.get("close_method")),
        "notes_exit": event.get("notes_exit"),
    }


def _prepare_insert_payload(payload: dict[str, Any]) -> dict[str, Any]:
    prepared = dict(payload)
    close_events = prepared.get("close_events") or []
    if close_events:
        for field_name in (
            "status",
            "exit_datetime",
            "spx_at_exit",
            "actual_exit_value",
            "close_method",
            "close_reason",
            "notes_exit",
            "gross_pnl",
            "max_risk",
            "roi_on_risk",
            "hours_held",
            "win_loss_result",
        ):
            prepared.pop(field_name, None)
    return prepared


def _normalize_close_method_for_sync(value: Any) -> Any:
    normalized = str(value or "").strip()
    if normalized == "Legacy Close":
        return "Close"
    if normalized == "Expiration":
        return "Expire"
    return value


def _trade_order_key(trade: dict[str, Any]) -> tuple[int, int]:
    trade_number = _to_int(trade.get("trade_number")) or 0
    trade_id = _to_int(trade.get("id")) or 0
    return (trade_number, trade_id)


def _to_int(value: Any) -> int | None:
    try:
        if value in {None, ""}:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _is_trade_number_collision(error: Exception) -> bool:
    message = str(error).strip().lower()
    if "trade number" not in message:
        return False
    return "unique" in message or "already exists" in message


def _get_trade_by_number(repository: TradeRepository, trade_number: int) -> dict[str, Any] | None:
    lookup = getattr(repository, "get_trade_by_number", None)
    if callable(lookup):
        return lookup(int(trade_number))
    for mode in SYNCABLE_TRADE_MODES:
        for trade in repository.list_trades(mode):
            if _to_int(trade.get("trade_number")) == int(trade_number):
                return trade
    return None