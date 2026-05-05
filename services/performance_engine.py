"""Delphi 4.0 Learning Engine Phase 1 performance summaries."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional

from .repositories.trade_repository import TradeRepository
from .trade_store import (
    build_learning_trade_fields,
    normalize_macro_flag,
    normalize_structure_label,
    resolve_trade_system_name,
)

SYSTEM_GROUPS = ("Apollo", "Talos")
STRUCTURE_GROUPS = ("Good", "Neutral", "Poor")
VIX_BUCKETS = ("<18", "18-22", "22-26", "26+")


@dataclass(frozen=True)
class PerformanceSummary:
    total_trades: int
    win_rate: float
    avg_win: float
    avg_loss: float
    total_pnl: float

    def as_dict(self) -> Dict[str, float | int]:
        return {
            "total_trades": self.total_trades,
            "win_rate": round(self.win_rate, 4),
            "avg_win": round(self.avg_win, 2),
            "avg_loss": round(self.avg_loss, 2),
            "total_pnl": round(self.total_pnl, 2),
        }


class PerformanceEngine:
    """Load trades, derive features, and build grouped performance summaries."""

    def __init__(self, store: TradeRepository) -> None:
        self.store = store

    def load_trades(self) -> list[Dict[str, Any]]:
        trades: list[Dict[str, Any]] = []
        for trade_mode in ("real", "simulated", "talos"):
            for trade in self.store.list_trades(trade_mode):
                enriched = self._build_enriched_trade(trade)
                if enriched.get("system") in SYSTEM_GROUPS:
                    trades.append(enriched)
        return trades

    def get_overall_performance(self) -> Dict[str, float | int]:
        return summarize_trade_group(self.load_trades()).as_dict()

    def get_performance_by_system(self) -> Dict[str, Dict[str, float | int]]:
        trades = self.load_trades()
        return {
            system: summarize_trade_group([trade for trade in trades if trade.get("system") == system]).as_dict()
            for system in SYSTEM_GROUPS
        }

    def get_performance_by_structure(self) -> Dict[str, Dict[str, float | int]]:
        trades = self.load_trades()
        return {
            structure: summarize_trade_group([trade for trade in trades if trade.get("structure") == structure]).as_dict()
            for structure in STRUCTURE_GROUPS
        }

    def get_performance_by_vix_bucket(self) -> Dict[str, Dict[str, float | int]]:
        trades = self.load_trades()
        return {
            bucket: summarize_trade_group([trade for trade in trades if trade.get("vix_bucket") == bucket]).as_dict()
            for bucket in VIX_BUCKETS
        }

    def _build_enriched_trade(self, trade: Dict[str, Any]) -> Dict[str, Any]:
        base = build_learning_trade_fields(trade)
        credit_received = coerce_float(base.get("credit_received"))
        distance_to_short = coerce_float(base.get("distance_to_short"))
        max_loss = coerce_float(base.get("max_loss"))
        expected_move = coerce_float(base.get("expected_move"))
        max_drawdown = coerce_float(base.get("max_drawdown"))
        pnl = coerce_float(base.get("pnl"))
        vix_entry = coerce_float(base.get("vix_entry"))
        result = normalize_result_label(base.get("result"))

        return {
            **trade,
            **base,
            "trade_mode": str(trade.get("trade_mode") or "").strip().lower(),
            "system": resolve_trade_system_name({**trade, **base}),
            "structure": normalize_structure_label(base.get("structure") or trade.get("structure_grade")),
            "macro_flag": normalize_macro_flag(base.get("macro_flag") or trade.get("macro_grade")),
            "credit_received": credit_received,
            "distance_to_short": distance_to_short,
            "expected_move": expected_move,
            "max_loss": max_loss,
            "max_drawdown": max_drawdown,
            "pnl": pnl,
            "result": result,
            "vix_entry": vix_entry,
            "vix_bucket": classify_vix_bucket(vix_entry),
            "credit_per_point": safe_divide(credit_received, distance_to_short),
            "risk_efficiency": safe_divide(credit_received, max_loss),
            "safety_ratio": safe_divide(distance_to_short, expected_move),
            "stress_ratio": safe_divide(max_drawdown, credit_received),
        }


def summarize_trade_group(trades: Iterable[Dict[str, Any]]) -> PerformanceSummary:
    items = list(trades)
    realized = [trade for trade in items if trade.get("result") in {"win", "loss"}]
    wins = [trade for trade in realized if trade.get("result") == "win" and coerce_float(trade.get("pnl")) is not None]
    losses = [trade for trade in realized if trade.get("result") == "loss" and coerce_float(trade.get("pnl")) is not None]
    total_pnl = sum(coerce_float(trade.get("pnl")) or 0.0 for trade in items)
    win_rate = (len(wins) / len(realized)) if realized else 0.0
    avg_win = average([coerce_float(trade.get("pnl")) or 0.0 for trade in wins])
    avg_loss = average([coerce_float(trade.get("pnl")) or 0.0 for trade in losses])
    return PerformanceSummary(
        total_trades=len(items),
        win_rate=win_rate,
        avg_win=avg_win,
        avg_loss=avg_loss,
        total_pnl=total_pnl,
    )


def normalize_result_label(value: Any) -> Optional[str]:
    text = str(value or "").strip().lower()
    if text == "win":
        return "win"
    if text == "loss":
        return "loss"
    return None


def classify_vix_bucket(vix_entry: Optional[float]) -> str:
    if vix_entry is None:
        return "26+"
    if vix_entry < 18:
        return "<18"
    if vix_entry < 22:
        return "18-22"
    if vix_entry < 26:
        return "22-26"
    return "26+"


def safe_divide(numerator: Optional[float], denominator: Optional[float]) -> Optional[float]:
    if numerator is None or denominator in {None, 0}:
        return None
    return numerator / denominator


def average(values: Iterable[float]) -> float:
    items = list(values)
    return sum(items) / len(items) if items else 0.0


def coerce_float(value: Any) -> Optional[float]:
    if value in {None, ""}:
        return None
    return float(value)