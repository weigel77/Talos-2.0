from __future__ import annotations

import threading
from collections import deque
from datetime import UTC, datetime, timedelta
from typing import Any, Deque, Dict, Optional


class RuntimeMetricsService:
    def __init__(self, *, retention_minutes: int = 30, max_events: int = 500) -> None:
        self.retention = timedelta(minutes=retention_minutes)
        self._events: Deque[Dict[str, Any]] = deque(maxlen=max_events)
        self._lock = threading.Lock()

    def record(
        self,
        name: str,
        duration_ms: float,
        *,
        cache_hit: Optional[bool] = None,
        detail: str = "",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        event = {
            "recorded_at": datetime.now(UTC),
            "name": str(name or "unknown").strip() or "unknown",
            "duration_ms": round(float(duration_ms), 2),
            "cache_hit": cache_hit,
            "detail": str(detail or "").strip(),
            "metadata": dict(metadata or {}),
        }
        with self._lock:
            self._events.append(event)
            self._prune_locked(event["recorded_at"])

    def build_report(self, *, top_n: int = 5) -> Dict[str, Any]:
        now = datetime.now(UTC)
        with self._lock:
            self._prune_locked(now)
            events = list(self._events)

        aggregates: Dict[str, Dict[str, Any]] = {}
        duplicates: Dict[tuple[str, str], Dict[str, Any]] = {}
        for event in events:
            name = str(event.get("name") or "unknown")
            aggregate = aggregates.setdefault(
                name,
                {
                    "name": name,
                    "count": 0,
                    "cache_hits": 0,
                    "cache_misses": 0,
                    "total_ms": 0.0,
                    "max_ms": 0.0,
                    "last_detail": "",
                },
            )
            aggregate["count"] += 1
            aggregate["total_ms"] += float(event.get("duration_ms") or 0.0)
            aggregate["max_ms"] = max(float(aggregate["max_ms"] or 0.0), float(event.get("duration_ms") or 0.0))
            aggregate["last_detail"] = str(event.get("detail") or aggregate["last_detail"] or "")
            if event.get("cache_hit") is True:
                aggregate["cache_hits"] += 1
            elif event.get("cache_hit") is False:
                aggregate["cache_misses"] += 1

            detail = str(event.get("detail") or "")
            if detail:
                key = (name, detail)
                duplicate_entry = duplicates.setdefault(
                    key,
                    {"name": name, "detail": detail, "count": 0, "cache_misses": 0, "max_ms": 0.0},
                )
                duplicate_entry["count"] += 1
                if event.get("cache_hit") is False:
                    duplicate_entry["cache_misses"] += 1
                duplicate_entry["max_ms"] = max(float(duplicate_entry["max_ms"] or 0.0), float(event.get("duration_ms") or 0.0))

        metric_rows = []
        for aggregate in aggregates.values():
            count = int(aggregate["count"] or 0)
            metric_rows.append(
                {
                    "name": aggregate["name"],
                    "count": count,
                    "cache_hits": int(aggregate["cache_hits"] or 0),
                    "cache_misses": int(aggregate["cache_misses"] or 0),
                    "avg_ms": round((float(aggregate["total_ms"] or 0.0) / count), 2) if count else 0.0,
                    "max_ms": round(float(aggregate["max_ms"] or 0.0), 2),
                    "last_detail": str(aggregate["last_detail"] or ""),
                }
            )
        metric_rows.sort(key=lambda item: (-float(item.get("max_ms") or 0.0), -int(item.get("count") or 0), item.get("name") or ""))

        duplicate_rows = [
            {
                "name": item["name"],
                "detail": item["detail"],
                "count": int(item["count"] or 0),
                "cache_misses": int(item["cache_misses"] or 0),
                "max_ms": round(float(item["max_ms"] or 0.0), 2),
            }
            for item in duplicates.values()
            if int(item.get("count") or 0) > 1
        ]
        duplicate_rows.sort(key=lambda item: (-int(item.get("cache_misses") or 0), -int(item.get("count") or 0), -float(item.get("max_ms") or 0.0)))

        slowest_calls = metric_rows[:top_n]
        return {
            "generated_at": now.isoformat(timespec="seconds") + "Z",
            "event_count": len(events),
            "slowest_calls": slowest_calls,
            "metrics": metric_rows,
            "duplicate_calls": duplicate_rows[:top_n],
        }

    def _prune_locked(self, now: datetime) -> None:
        while self._events and (now - self._events[0]["recorded_at"]) > self.retention:
            self._events.popleft()


_RUNTIME_METRICS_SERVICE = RuntimeMetricsService()


def get_runtime_metrics_service() -> RuntimeMetricsService:
    return _RUNTIME_METRICS_SERVICE