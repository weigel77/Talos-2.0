"""Kairos replay repository abstraction and adapters."""

from __future__ import annotations

from copy import deepcopy
import json
from datetime import datetime, timezone
from pathlib import Path
from time import monotonic
from typing import Any, Dict, List, Protocol, Tuple, runtime_checkable

from services.kairos_scenario_repository import KairosScenarioRepository
from services.runtime.supabase_integration import SupabaseRequestError, SupabaseRuntimeContext, SupabaseTableGateway


@runtime_checkable
class KairosBundleRepository(Protocol):
    """Abstract persistence contract for Kairos replay bundles."""

    def ensure_storage_dir(self) -> Path:
        ...

    def build_catalog_path(self) -> Path:
        ...

    def build_bundle_path(self, scenario_key: str) -> Path:
        ...

    def save_bundle(self, scenario_key: str, payload: Dict[str, Any]) -> bool:
        ...

    def load_bundle_payloads(self) -> List[Tuple[Path, Dict[str, Any]]]:
        ...

    def load_bundle_payload(self, scenario_key: str) -> Dict[str, Any] | None:
        ...

    def list_catalog_entries(self) -> List[Dict[str, Any]]:
        ...


class FileSystemKairosScenarioRepository:
    """Adapter that keeps the existing file-backed Kairos repository intact."""

    def __init__(self, repository: KairosScenarioRepository) -> None:
        self._repository = repository
        self.write_count: int = 0

    def ensure_storage_dir(self) -> Path:
        return self._repository.ensure_storage_dir()

    def build_catalog_path(self) -> Path:
        return self._repository.build_catalog_path()

    def build_bundle_path(self, scenario_key: str) -> Path:
        return self._repository.build_bundle_path(scenario_key)

    def save_bundle(self, scenario_key: str, payload: Dict[str, Any]) -> bool:
        self._repository.save_bundle(scenario_key, payload)
        self.write_count += 1
        return True

    def load_bundle_payloads(self) -> List[Tuple[Path, Dict[str, Any]]]:
        return self._repository.load_bundle_payloads()

    def load_bundle_payload(self, scenario_key: str) -> Dict[str, Any] | None:
        return self._repository.load_bundle_payload(scenario_key)

    def list_catalog_entries(self) -> List[Dict[str, Any]]:
        return self._repository.list_catalog_entries()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._repository, name)


class SupabaseKairosScenarioRepository:
    """Supabase-backed repository for Kairos replay bundles."""

    TABLE = "kairos_simulation_tapes"
    CACHE_TTL_SECONDS = 30.0

    def __init__(
        self,
        *,
        context: SupabaseRuntimeContext,
        gateway: SupabaseTableGateway,
    ) -> None:
        self.context = context
        self.gateway = gateway
        self._storage_available: bool | None = None
        self._cached_rows: List[Dict[str, Any]] | None = None
        self._cached_payloads: Dict[str, Dict[str, Any]] = {}
        self._cache_loaded_at: float | None = None

    def ensure_storage_dir(self) -> Path:
        return Path("supabase") / self.TABLE

    def build_catalog_path(self) -> Path:
        return self.ensure_storage_dir() / "_kairos_catalog.json"

    def build_bundle_path(self, scenario_key: str) -> Path:
        return self.ensure_storage_dir() / f"{scenario_key}.json"

    def save_bundle(self, scenario_key: str, payload: Dict[str, Any]) -> bool:
        if not self._ensure_storage_available():
            return False
        row_payload = {
            **self._extract_metadata(scenario_key, payload),
            "bundle_payload": self._normalize_payload(payload),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        existing = self.load_bundle_payload(scenario_key)
        if existing is not None:
            self.gateway.update(
                self.TABLE,
                row_payload,
                filters={"scenario_key": f"eq.{scenario_key}"},
            )
            self._invalidate_cache()
            return True
        row_payload["created_at"] = row_payload["updated_at"]
        self.gateway.insert(self.TABLE, row_payload)
        self._invalidate_cache()
        return True

    def load_bundle_payloads(self) -> List[Tuple[Path, Dict[str, Any]]]:
        if not self._ensure_storage_available():
            return []
        payloads: List[Tuple[Path, Dict[str, Any]]] = []
        for row in self._get_cached_rows():
            scenario_key = str(row.get("scenario_key") or "").strip()
            payload = deepcopy(self._cached_payloads.get(scenario_key))
            if not scenario_key or payload is None:
                continue
            payloads.append((self.build_bundle_path(scenario_key), payload))
        return payloads

    def load_bundle_payload(self, scenario_key: str) -> Dict[str, Any] | None:
        if not self._ensure_storage_available():
            return None
        normalized_key = str(scenario_key or "").strip()
        if not normalized_key:
            return None
        self._get_cached_rows()
        payload = self._cached_payloads.get(normalized_key)
        return deepcopy(payload) if payload is not None else None

    def list_catalog_entries(self) -> List[Dict[str, Any]]:
        if not self._ensure_storage_available():
            return []
        entries: List[Dict[str, Any]] = []
        for row in self._get_cached_rows():
            scenario_key = str(row.get("scenario_key") or "").strip()
            if not scenario_key:
                continue
            entries.append(
                {
                    "id": scenario_key,
                    "scenario_key": scenario_key,
                    "session_date": row.get("session_date") or "",
                    "label": row.get("label") or scenario_key,
                    "source": row.get("source") or "",
                    "source_family_tag": row.get("source_family_tag") or "",
                    "source_type": row.get("source_type") or "",
                    "created_at": row.get("created_at") or "",
                    "session_status": row.get("session_status") or "unknown",
                    "bar_count": int(row.get("bar_count") or 0),
                    "bundle_path": self.build_bundle_path(scenario_key).name,
                }
            )
        return entries

    def _get_cached_rows(self) -> List[Dict[str, Any]]:
        if self._cached_rows is not None and self._cache_loaded_at is not None:
            if (monotonic() - self._cache_loaded_at) < self.CACHE_TTL_SECONDS:
                return [dict(row) for row in self._cached_rows]

        rows = self._select_rows(order="session_date.desc,updated_at.desc")
        self._cached_rows = [dict(row) for row in rows]
        self._cached_payloads = {}
        for row in self._cached_rows:
            scenario_key = str(row.get("scenario_key") or "").strip()
            payload = self._deserialize_payload(row.get("bundle_payload"))
            if scenario_key and payload is not None:
                self._cached_payloads[scenario_key] = payload
        self._cache_loaded_at = monotonic()
        return [dict(row) for row in self._cached_rows]

    def _invalidate_cache(self) -> None:
        self._cached_rows = None
        self._cached_payloads = {}
        self._cache_loaded_at = None

    def _ensure_storage_available(self) -> bool:
        if self._storage_available is not None:
            return self._storage_available
        try:
            self.gateway.select(self.TABLE, limit=1, columns="scenario_key")
        except SupabaseRequestError as exc:
            self._storage_available = False
            return False
        self._storage_available = True
        return True

    def _select_rows(
        self,
        *,
        filters: Dict[str, str] | None = None,
        order: str | None = None,
        limit: int | None = None,
    ) -> List[Dict[str, Any]]:
        try:
            rows = self.gateway.select(self.TABLE, filters=filters, order=order, limit=limit)
        except SupabaseRequestError as exc:
            self._storage_available = False
            self._invalidate_cache()
            return []
        return [dict(row) for row in rows]

    @staticmethod
    def _normalize_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
        normalized = json.loads(json.dumps(payload, default=str))
        return normalized if isinstance(normalized, dict) else {}

    @staticmethod
    def _deserialize_payload(payload: Any) -> Dict[str, Any] | None:
        if isinstance(payload, dict):
            return payload
        if isinstance(payload, str):
            try:
                decoded = json.loads(payload)
            except json.JSONDecodeError:
                return None
            return decoded if isinstance(decoded, dict) else None
        return None

    @staticmethod
    def _extract_metadata(scenario_key: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        template = payload.get("template") if isinstance(payload.get("template"), dict) else {}
        return {
            "scenario_key": scenario_key,
            "session_date": template.get("session_date") or None,
            "label": template.get("scenario_name") or scenario_key,
            "source": template.get("source_label") or "",
            "source_family_tag": template.get("source_family_tag") or "",
            "source_type": template.get("source_type") or "",
            "session_status": template.get("session_status") or "unknown",
            "bar_count": int(template.get("bar_count") or 0),
        }

    @classmethod
    def _is_missing_table_error(cls, error: SupabaseRequestError) -> bool:
        message = str(error)
        return cls.TABLE in message and "schema cache" in message