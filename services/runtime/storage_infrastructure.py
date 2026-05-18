"""Filesystem-backed storage composition for local and hosted Delphi runtimes."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Protocol, runtime_checkable
import os

from flask import Flask

from config import AppConfig

from .settings_binding import RuntimeSettings


def get_persistent_data_dir() -> Path:
    appdata = os.getenv("APPDATA")
    if appdata:
        return Path(appdata) / "Horme"
    return Path.home() / ".horme"


@dataclass(frozen=True)
class StorageInfrastructure:
    """Resolved storage locations for the active runtime target."""

    instance_path: Path
    persistent_data_dir: Path
    trade_database_path: Path
    kairos_replay_storage_dir: Path
    kairos_legacy_storage_dirs: tuple[Path, ...]
    import_preview_root: Path
    app_log_path: Path
    schwab_token_path: Path

    def apply_to_app(self, app: Flask) -> None:
        app.config.setdefault("TRADE_DATABASE", str(self.trade_database_path))
        app.config.setdefault("KAIROS_REPLAY_STORAGE_DIR", str(self.kairos_replay_storage_dir))
        app.config.setdefault("APP_LOG_PATH", str(self.app_log_path))


@runtime_checkable
class StorageInfrastructureComposer(Protocol):
    """Compose concrete storage locations for a runtime target."""

    def compose(self, app: Flask, settings: RuntimeSettings) -> StorageInfrastructure:
        ...


class LocalFileSystemStorageComposer:
    """Local filesystem storage composer that preserves current instance-path behavior."""

    def __init__(
        self,
        config: AppConfig,
        *,
        persistent_data_dir_resolver: Callable[[], Path] = get_persistent_data_dir,
        workspace_root_resolver: Callable[[], Path] | None = None,
    ) -> None:
        self.config = config
        self.persistent_data_dir_resolver = persistent_data_dir_resolver
        self.workspace_root_resolver = workspace_root_resolver or self._default_workspace_root

    def compose(self, app: Flask, settings: RuntimeSettings) -> StorageInfrastructure:
        instance_path = Path(app.instance_path)
        persistent_data_dir = self.persistent_data_dir_resolver()
        trade_database_path = Path(app.config.get("TRADE_DATABASE") or (instance_path / "horme_trades.db"))
        kairos_replay_storage_dir = Path(
            app.config.get("KAIROS_REPLAY_STORAGE_DIR")
            or self.config.kairos_replay_storage_dir
            or (persistent_data_dir / "kairos_replays")
        )
        app_log_path = Path(app.config.get("APP_LOG_PATH") or self.config.app_log_path or (Path(app.root_path) / "market_lookup.log")).expanduser()
        schwab_token_path = Path(self.config.schwab_shared_market_token_path)
        legacy_candidates = [instance_path / "kairos_replays"]
        if not app.config.get("TESTING"):
            workspace_root = self.workspace_root_resolver()
            legacy_candidates.extend(
                [
                    workspace_root / "instance" / "kairos_replays",
                    persistent_data_dir / "kairos_replays",
                ]
            )
        kairos_legacy_storage_dirs = self._dedupe_legacy_dirs(kairos_replay_storage_dir, legacy_candidates)
        return StorageInfrastructure(
            instance_path=instance_path,
            persistent_data_dir=persistent_data_dir,
            trade_database_path=trade_database_path,
            kairos_replay_storage_dir=kairos_replay_storage_dir,
            kairos_legacy_storage_dirs=kairos_legacy_storage_dirs,
            import_preview_root=instance_path,
            app_log_path=app_log_path,
            schwab_token_path=schwab_token_path,
        )

    @staticmethod
    def _default_workspace_root() -> Path:
        return Path(__file__).resolve().parents[2]

    @staticmethod
    def _dedupe_legacy_dirs(storage_dir: Path, candidates: list[Path]) -> tuple[Path, ...]:
        resolved_storage_dir = storage_dir.resolve()
        resolved_seen: set[Path] = set()
        legacy_dirs: list[Path] = []
        for candidate in candidates:
            resolved_candidate = candidate.resolve()
            if resolved_candidate == resolved_storage_dir or resolved_candidate in resolved_seen:
                continue
            resolved_seen.add(resolved_candidate)
            legacy_dirs.append(candidate)
        return tuple(legacy_dirs)


class HostedFileSystemStorageComposerSkeleton(LocalFileSystemStorageComposer):
    """Hosted storage skeleton that still uses local files until hosted stores are introduced."""

    def compose(self, app: Flask, settings: RuntimeSettings) -> StorageInfrastructure:
        infrastructure = super().compose(app, settings)
        return StorageInfrastructure(
            instance_path=infrastructure.instance_path,
            persistent_data_dir=infrastructure.persistent_data_dir,
            trade_database_path=infrastructure.trade_database_path,
            kairos_replay_storage_dir=infrastructure.kairos_replay_storage_dir,
            kairos_legacy_storage_dirs=(),
            import_preview_root=infrastructure.import_preview_root,
            app_log_path=infrastructure.app_log_path,
            schwab_token_path=infrastructure.schwab_token_path,
        )