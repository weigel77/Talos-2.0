"""Repository abstractions and local storage adapters for Delphi 5.0."""

from .apollo_snapshot_repository import ApolloSnapshotRepository, JsonFileApolloSnapshotRepository
from .import_preview_repository import FileSystemImportPreviewRepository, ImportPreviewRepository
from .management_state_repository import (
    ACTIVE_TRADE_ALERT_LOG_TABLE,
    ACTIVE_TRADES_TABLE,
    JOURNAL_TRADES_TABLE,
    MANAGEMENT_SCHEMA,
    MANAGEMENT_RUNTIME_SETTINGS_TABLE,
    OpenTradeManagementStateRepository,
    SupabaseOpenTradeManagementStateRepository,
    SQLiteOpenTradeManagementStateRepository,
)
from .token_repository import JsonFileTokenRepository, TokenRepository
from .trade_repository import SQLiteTradeRepository, SupabaseTradeRepository, TradeRepository

__all__ = [
    "ApolloSnapshotRepository",
    "ACTIVE_TRADE_ALERT_LOG_TABLE",
    "ACTIVE_TRADES_TABLE",
    "FileSystemImportPreviewRepository",
    "ImportPreviewRepository",
    "JOURNAL_TRADES_TABLE",
    "JsonFileApolloSnapshotRepository",
    "JsonFileTokenRepository",
    "MANAGEMENT_SCHEMA",
    "MANAGEMENT_RUNTIME_SETTINGS_TABLE",
    "OpenTradeManagementStateRepository",
    "SupabaseOpenTradeManagementStateRepository",
    "SQLiteOpenTradeManagementStateRepository",
    "SQLiteTradeRepository",
    "SupabaseTradeRepository",
    "TokenRepository",
    "TradeRepository",
]