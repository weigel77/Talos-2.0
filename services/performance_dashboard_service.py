"""Performance dashboard aggregation for Horme trade analytics."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
import math
from typing import Any, Dict, Iterable, Optional

from .repositories.trade_repository import TradeRepository
from .trade_store import (
    EXPECTED_MOVE_CONFIDENCE_NONE,
    EXPECTED_MOVE_SOURCE_ESTIMATED,
    EXPECTED_MOVE_USAGE_ACTUAL,
    EXPECTED_MOVE_USAGE_ESTIMATED,
    EXPECTED_MOVE_USAGE_EXCLUDED,
    classify_closed_trade_outcome,
    classify_expected_move_usage,
    expected_move_learning_weight,
    normalize_expected_move_source,
    parse_datetime_value,
    normalize_system_name,
    resolve_trade_candidate_profile,
    resolve_trade_credit_model,
    resolve_trade_distance,
    resolve_trade_expected_move,
)
from .em_policy_engine import build_em_policy_payload

PERFORMANCE_FILTER_GROUPS = {
    "system": ["Apollo", "Kairos", "Aegis"],
    "profile": ["Legacy", "Aggressive", "Fortress", "Standard", "Prime", "Subprime"],
    "result": ["Win", "Loss", "Black Swan", "Scratched"],
    "trade_mode": ["Real", "Simulated"],
    "macro_grade": ["None", "Minor", "Major"],
    "structure_grade": ["Good", "Neutral", "Poor"],
    "timeframe": ["All", "YTD", "Last Month", "Last Qtr", "Current Month", "1 Yr", "Custom"],
}

PERFORMANCE_DEFAULT_FILTERS = {
    "trade_mode": ("real",),
    "timeframe": ("all",),
}

VIX_BUCKETS = ["<18", "18-22", "22-26", "26+"]
MIN_EXPECTED_MOVE_USED = 1.0
SAFETY_RATIO_BUCKETS = [
    {"key": "<1.2", "label": "<1.2x EM", "minimum": None, "maximum": 1.2},
    {"key": "1.2-1.4", "label": "1.2-1.4x EM", "minimum": 1.2, "maximum": 1.4},
    {"key": "1.4-1.6", "label": "1.4-1.6x EM", "minimum": 1.4, "maximum": 1.6},
    {"key": "1.6-1.8", "label": "1.6-1.8x EM", "minimum": 1.6, "maximum": 1.8},
    {"key": "1.8-2.0", "label": "1.8-2.0x EM", "minimum": 1.8, "maximum": 2.0},
    {"key": "2.0-2.2", "label": "2.0-2.2x EM", "minimum": 2.0, "maximum": 2.2},
    {"key": "2.2+", "label": "2.2+x EM", "minimum": 2.2, "maximum": None},
]
DEFAULT_VIX_REGIME_EDGES = [18.0, 22.0, 28.0]
RISK_EXIT_REASON_KEYWORDS = ("risk", "defense", "defence", "stop", "breach", "touch", "threat", "close now", "short strike", "long strike")
MAX_ANALYTIC_EM_MULTIPLE = 10.0


@dataclass(frozen=True)
class PerformanceFilters:
    system: tuple[str, ...]
    profile: tuple[str, ...]
    result: tuple[str, ...]
    trade_mode: tuple[str, ...]
    macro_grade: tuple[str, ...]
    structure_grade: tuple[str, ...]
    timeframe: tuple[str, ...]
    expiration_start: str = ""
    expiration_end: str = ""

    def as_dict(self) -> Dict[str, list[str]]:
        return {
            "system": list(self.system),
            "profile": list(self.profile),
            "result": list(self.result),
            "trade_mode": list(self.trade_mode),
            "macro_grade": list(self.macro_grade),
            "structure_grade": list(self.structure_grade),
            "timeframe": list(self.timeframe),
            "expiration_start": [self.expiration_start] if self.expiration_start else [],
            "expiration_end": [self.expiration_end] if self.expiration_end else [],
        }


@dataclass(frozen=True)
class OutcomeSummary:
    open_records: tuple[Dict[str, Any], ...]
    closed_records: tuple[Dict[str, Any], ...]
    wins: tuple[Dict[str, Any], ...]
    losses: tuple[Dict[str, Any], ...]
    black_swans: tuple[Dict[str, Any], ...]
    scratched: tuple[Dict[str, Any], ...]

    @property
    def loss_events(self) -> tuple[Dict[str, Any], ...]:
        return self.losses + self.black_swans

    @property
    def scored_outcomes(self) -> tuple[Dict[str, Any], ...]:
        return self.wins + self.loss_events

    @property
    def closed_outcomes(self) -> tuple[Dict[str, Any], ...]:
        return self.wins + self.losses + self.black_swans + self.scratched

    def outcome_count(self, label: str) -> int:
        if label == "Win":
            return len(self.wins)
        if label == "Loss":
            return len(self.losses)
        if label == "Black Swan":
            return len(self.black_swans)
        if label == "Scratched":
            return len(self.scratched)
        return 0


class PerformanceDashboardService:
    """Build filter-aware dashboard payloads from the shared trade database."""

    def __init__(self, store: TradeRepository) -> None:
        self.store = store

    def build_dashboard(self, filters: Optional[Dict[str, Iterable[str]]] = None) -> Dict[str, Any]:
        records = self.load_records()
        return build_dashboard_payload(records, filters=filters)

    def load_records(self) -> list[Dict[str, Any]]:
        records: list[Dict[str, Any]] = []
        for trade_mode in ("real", "simulated"):
            for trade in self.store.list_trades(trade_mode):
                records.append(build_performance_record(trade))
        return records


def build_dashboard_payload(records: list[Dict[str, Any]], filters: Optional[Dict[str, Iterable[str]]] = None) -> Dict[str, Any]:
    normalized_filters = normalize_performance_filters(filters)
    filtered_records = apply_performance_filters(records, normalized_filters)
    outcome_summary = summarize_outcomes(filtered_records)
    safety_optimization = build_safety_ratio_optimization(filtered_records)
    metrics = build_metric_payload(filtered_records, outcomes=outcome_summary)
    chart_data = build_chart_payload(filtered_records, outcomes=outcome_summary, safety_optimization=safety_optimization)
    learning = build_learning_payload(filtered_records, outcomes=outcome_summary, safety_optimization=safety_optimization)
    learning["profile_em_performance"] = build_profile_em_performance_payload(filtered_records)
    learning["distance_efficiency"] = build_distance_efficiency_payload(filtered_records)
    learning["time_of_entry_edge"] = build_time_of_entry_edge_payload(filtered_records)
    learning["exit_efficiency"] = build_exit_efficiency_payload(filtered_records)
    learning["vix_regime_performance"] = build_vix_regime_performance_payload(filtered_records)
    learning["expected_move_breach_analysis"] = build_expected_move_breach_analysis_payload(filtered_records)
    learning["data_limitations"] = build_performance_data_limitations(learning)
    return {
        "filters": normalized_filters.as_dict(),
        "filter_groups": PERFORMANCE_FILTER_GROUPS,
        "records_total": len(records),
        "records_filtered": len(filtered_records),
        "metrics": metrics,
        "charts": chart_data,
        "learning": learning,
    }


def normalize_performance_filters(filters: Optional[Dict[str, Iterable[str]]] = None) -> PerformanceFilters:
    filters = filters or {}
    normalized: Dict[str, tuple[str, ...]] = {}
    for key, options in PERFORMANCE_FILTER_GROUPS.items():
        allowed = {normalize_filter_value(key, option) for option in options}
        requested_values = filters.get(key)
        if key in filters and not requested_values:
            normalized[key] = tuple()
            continue
        normalized_values = [normalize_filter_value(key, value) for value in (requested_values or []) if normalize_filter_value(key, value) in allowed]
        if normalized_values:
            normalized[key] = tuple(normalized_values)
            continue
        default_values = tuple(value for value in PERFORMANCE_DEFAULT_FILTERS.get(key, ()) if value in allowed)
        normalized[key] = default_values or tuple(sorted(allowed))
    return PerformanceFilters(
        **normalized,
        expiration_start=_normalize_date_filter_value(filters.get("expiration_start")),
        expiration_end=_normalize_date_filter_value(filters.get("expiration_end")),
    )


def build_performance_record(trade: Dict[str, Any]) -> Dict[str, Any]:
    normalized_trade_mode = str(trade.get("trade_mode") or "").strip().lower()
    if normalized_trade_mode == "real":
        trade_mode = "Real"
    else:
        trade_mode = "Simulated"
    profile = resolve_trade_candidate_profile(trade)
    system = normalize_system_name(trade.get("system_name"))
    result = classify_trade_result(trade)
    gross_pnl = coerce_float(trade.get("gross_pnl") if trade.get("gross_pnl") is not None else trade.get("pnl"))
    trade_date = pick_performance_date(trade)
    derived_status = str(trade.get("derived_status_label") or trade.get("status") or "").strip().title() or "Open"
    expected_move_metadata = resolve_trade_expected_move(trade)
    expected_move = coerce_float(expected_move_metadata.get("value"))
    distance_metadata = resolve_trade_distance(trade)
    distance_to_short = coerce_float(distance_metadata.get("value"))
    credit_model = resolve_trade_credit_model(trade)
    actual_entry_credit, entry_credit_source = derive_entry_credit(trade)
    max_loss = derive_max_loss(trade, actual_entry_credit=actual_entry_credit)
    roi_on_risk = coerce_float(trade.get("roi_on_risk"))
    if roi_on_risk is None:
        roi_on_risk = compute_ratio(gross_pnl, max_loss)
    vix_at_entry = coerce_float(trade.get("vix_at_entry") if trade.get("vix_at_entry") is not None else trade.get("vix_entry"))
    expected_move_used = coerce_float(trade.get("expected_move_used"))
    if expected_move_used is None:
        expected_move_used = coerce_float(expected_move_metadata.get("used_value"))
    expected_move_source = normalize_expected_move_source(
        trade.get("expected_move_source") if trade.get("expected_move_source") not in {None, ""} else expected_move_metadata.get("source")
    )
    expected_move_raw_estimate = coerce_float(trade.get("expected_move_raw_estimate"))
    if expected_move_raw_estimate is None:
        expected_move_raw_estimate = coerce_float(expected_move_metadata.get("raw_estimate"))
    expected_move_calibrated = coerce_float(trade.get("expected_move_calibrated"))
    if expected_move_calibrated is None:
        expected_move_calibrated = coerce_float(expected_move_metadata.get("calibrated_value"))
    expected_move_confidence = str(
        trade.get("expected_move_confidence")
        or expected_move_metadata.get("confidence")
        or EXPECTED_MOVE_CONFIDENCE_NONE
    ).strip().lower()
    expected_move_usage = classify_expected_move_usage(expected_move_source, expected_move_used)
    expected_move_weight = expected_move_learning_weight(expected_move_source, expected_move_confidence)
    safety_ratio = compute_ratio(distance_to_short, expected_move_used)
    credit_per_point = compute_ratio(actual_entry_credit, distance_to_short)
    risk_efficiency = compute_risk_efficiency(total_premium=coerce_float(credit_model.get("total_premium")), max_loss=max_loss)
    credit_efficiency_pct = (risk_efficiency * 100.0) if risk_efficiency is not None else None
    actual_distance_to_short = coerce_float(trade.get("actual_distance_to_short"))
    actual_em_multiple = coerce_float(trade.get("actual_em_multiple"))
    close_events = [dict(item) for item in (trade.get("close_events") or []) if isinstance(item, dict)]
    return {
        "id": trade.get("id"),
        "trade_number": trade.get("trade_number"),
        "system": system,
        "profile": profile,
        "result": result,
        "trade_mode": trade_mode,
        "macro_grade": normalize_macro_grade(trade.get("macro_grade") if trade.get("macro_grade") is not None else trade.get("macro_flag")),
        "structure_grade": normalize_structure_grade(trade.get("structure_grade") if trade.get("structure_grade") is not None else trade.get("structure")),
        "status": derived_status,
        "gross_pnl": gross_pnl,
        "trade_date": trade_date,
        "trade_date_label": trade_date or "—",
        "entry_datetime": trade.get("entry_datetime"),
        "expiration_date": trade.get("expiration_date"),
        "journal_name": str(trade.get("journal_name") or "Apollo Main"),
        "close_reason": str(trade.get("close_reason") or ""),
        "close_method": str(trade.get("close_method") or ""),
        "close_events": close_events,
        "spx_at_entry": coerce_float(trade.get("spx_at_entry") if trade.get("spx_at_entry") is not None else trade.get("spx_entry")),
        "short_strike": coerce_float(trade.get("short_strike")),
        "expected_move": expected_move,
        "expected_move_source": expected_move_source,
        "expected_move_raw_estimate": expected_move_raw_estimate,
        "expected_move_calibrated": expected_move_calibrated,
        "expected_move_confidence": expected_move_confidence,
        "expected_move_usage": expected_move_usage,
        "expected_move_weight": expected_move_weight,
        "distance_to_short": distance_to_short,
        "distance_source": distance_metadata.get("source"),
        "distance_has_material_discrepancy": bool(distance_metadata.get("discrepancy_is_material")),
        "actual_entry_credit": actual_entry_credit,
        "entry_credit_source": entry_credit_source,
        "premium_per_contract": coerce_float(credit_model.get("premium_per_contract")),
        "total_premium": coerce_float(credit_model.get("total_premium")),
        "max_loss": max_loss,
        "roi_on_risk": roi_on_risk,
        "vix_at_entry": vix_at_entry,
        "vix_bucket": classify_vix_bucket(vix_at_entry),
        "safety_ratio": safety_ratio,
        "credit_per_point": credit_per_point,
        "risk_efficiency": risk_efficiency,
        "credit_efficiency_pct": credit_efficiency_pct,
        "expected_move_used": expected_move_used,
        "em_multiple_floor": coerce_float(trade.get("em_multiple_floor")),
        "percent_floor": coerce_float(trade.get("percent_floor")),
        "boundary_rule_used": str(trade.get("boundary_rule_used") or ""),
        "actual_distance_to_short": actual_distance_to_short,
        "actual_em_multiple": actual_em_multiple,
        "spx_at_exit": coerce_float(trade.get("spx_at_exit")),
        "long_strike": coerce_float(trade.get("long_strike")),
        "option_type": str(trade.get("option_type") or ""),
        "has_partial_close": bool(trade.get("has_partial_close")),
        "has_expire_event": bool(trade.get("has_expire_event")),
        "original_contracts": to_int_or_none(trade.get("original_contracts") if trade.get("original_contracts") is not None else trade.get("contracts")),
        "remaining_contracts": to_int_or_none(trade.get("remaining_contracts")),
        "fallback_used": str(trade.get("fallback_used") or "no").strip().lower() or "no",
        "fallback_rule_name": str(trade.get("fallback_rule_name") or ""),
        "realized_pnl": gross_pnl,
        "vix_entry": vix_at_entry,
    }


def apply_performance_filters(records: list[Dict[str, Any]], filters: PerformanceFilters) -> list[Dict[str, Any]]:
    active = filters.as_dict()
    reference_date = date.today()
    filtered: list[Dict[str, Any]] = []
    for record in records:
        if normalize_filter_value("system", record.get("system")) not in active["system"]:
            continue
        if normalize_filter_value("profile", record.get("profile")) not in active["profile"]:
            continue
        result_key = normalize_filter_value("result", record.get("result"))
        if result_key and result_key not in active["result"]:
            continue
        if normalize_filter_value("trade_mode", record.get("trade_mode")) not in active["trade_mode"]:
            continue
        if normalize_filter_value("macro_grade", record.get("macro_grade")) not in active["macro_grade"]:
            continue
        if normalize_filter_value("structure_grade", record.get("structure_grade")) not in active["structure_grade"]:
            continue
        if not record_matches_timeframe(record, filters=filters, reference_date=reference_date):
            continue
        filtered.append(record)
    return filtered


def record_matches_timeframe(record: Dict[str, Any], *, filters: PerformanceFilters, reference_date: date) -> bool:
    selected = [str(value or "").strip().lower() for value in filters.timeframe if str(value or "").strip()]
    if not selected or "all" in selected:
        return True
    if "custom" in selected:
        expiration_date = parse_trade_date(record.get("expiration_date")) or parse_trade_date(record.get("trade_date"))
        if expiration_date is None:
            return False
        start_date = parse_trade_date(filters.expiration_start)
        end_date = parse_trade_date(filters.expiration_end)
        if start_date is not None and expiration_date < start_date:
            return False
        if end_date is not None and expiration_date > end_date:
            return False
        return True
    trade_date = parse_trade_date(record.get("trade_date"))
    if trade_date is None:
        return False
    return any(_date_in_timeframe(trade_date, timeframe_key=value, reference_date=reference_date) for value in selected)


def _normalize_date_filter_value(value: Iterable[str] | str | None) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return parse_trade_date(value).isoformat() if parse_trade_date(value) is not None else ""
    for item in value:
        normalized = _normalize_date_filter_value(str(item or "").strip())
        if normalized:
            return normalized
    return ""


def parse_trade_date(value: Any) -> date | None:
    if not value:
        return None
    if isinstance(value, date):
        return value
    raw = str(value).strip()
    if not raw:
        return None
    try:
        return date.fromisoformat(raw[:10])
    except ValueError:
        return None


def _date_in_timeframe(trade_date: date, *, timeframe_key: str, reference_date: date) -> bool:
    normalized = str(timeframe_key or "all").strip().lower()
    if normalized == "all":
        return True
    if normalized == "ytd":
        return date(reference_date.year, 1, 1) <= trade_date <= reference_date
    if normalized == "current-month":
        return trade_date.year == reference_date.year and trade_date.month == reference_date.month
    if normalized == "1-yr":
        return (reference_date - timedelta(days=365)) <= trade_date <= reference_date
    if normalized == "last-month":
        first_day_current_month = date(reference_date.year, reference_date.month, 1)
        last_day_previous_month = first_day_current_month - timedelta(days=1)
        return trade_date.year == last_day_previous_month.year and trade_date.month == last_day_previous_month.month
    if normalized == "last-qtr":
        current_quarter = ((reference_date.month - 1) // 3) + 1
        if current_quarter == 1:
            year = reference_date.year - 1
            quarter = 4
        else:
            year = reference_date.year
            quarter = current_quarter - 1
        start_month = ((quarter - 1) * 3) + 1
        end_month = start_month + 2
        return trade_date.year == year and start_month <= trade_date.month <= end_month
    return True


def summarize_outcomes(records: list[Dict[str, Any]]) -> OutcomeSummary:
    open_records = []
    closed_records = []
    wins = []
    losses = []
    black_swans = []
    scratched = []

    for record in records:
        if record["status"] in {"Open", "Reduced"}:
            open_records.append(record)
            continue

        closed_records.append(record)
        if record["result"] == "Win":
            wins.append(record)
        elif record["result"] == "Loss":
            losses.append(record)
        elif record["result"] == "Black Swan":
            black_swans.append(record)
        elif record["result"] == "Scratched":
            scratched.append(record)

    return OutcomeSummary(
        open_records=tuple(open_records),
        closed_records=tuple(closed_records),
        wins=tuple(wins),
        losses=tuple(losses),
        black_swans=tuple(black_swans),
        scratched=tuple(scratched),
    )


def build_metric_payload(records: list[Dict[str, Any]], *, outcomes: Optional[OutcomeSummary] = None) -> Dict[str, Any]:
    outcomes = outcomes or summarize_outcomes(records)
    total_trades = len(records)
    open_trades = len(outcomes.open_records)
    closed_trades = len(outcomes.closed_records)
    win_count = len(outcomes.wins)
    loss_count = len(outcomes.losses)
    black_swan_count = len(outcomes.black_swans)
    scratched_count = len(outcomes.scratched)
    total_loss_outcomes = len(outcomes.loss_events)
    scored_outcomes = len(outcomes.scored_outcomes)
    win_rate = (win_count / scored_outcomes) if scored_outcomes else 0.0
    loss_rate = (total_loss_outcomes / scored_outcomes) if scored_outcomes else 0.0
    net_pnl = sum(record["gross_pnl"] or 0.0 for record in records)
    average_win = average(record["gross_pnl"] for record in outcomes.wins)
    average_loss = abs(average(record["gross_pnl"] for record in outcomes.losses))
    average_black_swan_loss = abs(average(record["gross_pnl"] for record in outcomes.black_swans))
    expectancy_average_loss = abs(average(record["gross_pnl"] for record in outcomes.loss_events))
    expectancy = calculate_expectancy(win_rate=win_rate, average_win=average_win, loss_rate=loss_rate, average_loss=expectancy_average_loss)
    black_swan_impact = sum(record["gross_pnl"] or 0.0 for record in outcomes.black_swans)
    absolute_loss_total = abs(sum(record["gross_pnl"] or 0.0 for record in outcomes.loss_events))
    expectancy_scale = max(50.0, abs(expectancy) * 1.75, average_win, average_loss, average_black_swan_loss)
    real_closed_wins = [record for record in outcomes.wins if str(record.get("trade_mode") or "").strip().lower() == "real"]
    real_closed_losses = [record for record in outcomes.losses if str(record.get("trade_mode") or "").strip().lower() == "real"]
    real_closed_black_swans = [record for record in outcomes.black_swans if str(record.get("trade_mode") or "").strip().lower() == "real"]
    average_win_roi = average(record.get("roi_on_risk") for record in real_closed_wins)
    average_loss_roi = abs(average(record.get("roi_on_risk") for record in real_closed_losses))
    average_black_swan_roi = abs(average(record.get("roi_on_risk") for record in real_closed_black_swans))
    credit_efficiency_records = [record for record in records if record.get("credit_efficiency_pct") is not None]

    return {
        "totals": {
            "total_trades": total_trades,
            "open_trades": open_trades,
            "closed_trades": closed_trades,
            "closed_outcomes": scored_outcomes,
            "all_closed_trades": closed_trades,
            "wins": win_count,
            "losses": loss_count,
            "loss_outcomes": total_loss_outcomes,
            "black_swan_count": black_swan_count,
            "scratched_count": scratched_count,
        },
        "win_rate": {
            "value": round(win_rate * 100, 2),
            "ratio": round(win_rate, 4),
            "wins": win_count,
            "losses": total_loss_outcomes,
            "closed_outcomes": scored_outcomes,
            "scratched_excluded": scratched_count,
        },
        "expectancy": {
            "value": round(expectancy, 2),
            "scale": round(expectancy_scale, 2),
        },
        "net_pnl": {
            "value": round(net_pnl, 2),
        },
        "outcome_mix": {
            "win_roi": round(average_win_roi * 100, 2),
            "loss_roi": round(average_loss_roi * 100, 2),
            "black_swan_roi": round(average_black_swan_roi * 100, 2),
            "win_count": len(real_closed_wins),
            "loss_count": len(real_closed_losses),
            "black_swan_count": len(real_closed_black_swans),
            "closed_real_outcomes": len(real_closed_wins) + len(real_closed_losses) + len(real_closed_black_swans),
            "source": "Average ROI by outcome category using real closed trades only",
        },
        "credit_efficiency": {
            "value": round(average(record.get("credit_efficiency_pct") for record in credit_efficiency_records), 2) if credit_efficiency_records else 0.0,
            "qualified_trade_count": len(credit_efficiency_records),
            "formula": "Premium received ÷ max theoretical loss",
            "source": "Uses actual entry credit when available; otherwise falls back to candidate credit estimate.",
        },
        "average_win_loss": {
            "average_win": round(average_win, 2),
            "average_loss": round(average_loss, 2),
            "average_black_swan_loss": round(average_black_swan_loss, 2),
            "ratio": round((average_win / average_loss), 2) if average_loss else None,
            "win_count": win_count,
            "loss_count": loss_count,
            "black_swan_count": black_swan_count,
        },
        "black_swan": {
            "count": black_swan_count,
            "impact": round(black_swan_impact, 2),
            "impact_ratio": round((abs(black_swan_impact) / absolute_loss_total), 4) if absolute_loss_total else 0.0,
        },
    }


def build_chart_payload(
    records: list[Dict[str, Any]],
    *,
    outcomes: Optional[OutcomeSummary] = None,
    safety_optimization: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    outcomes = outcomes or summarize_outcomes(records)
    safety_optimization = safety_optimization or build_safety_ratio_optimization(records)
    safety_ratio_records = summarize_safety_ratio_records(records)["included_records"]
    return {
        "equity_curve": build_equity_curve(records),
        "profile_avg_pnl": build_group_average_pnl_chart(records, key="profile", labels=PERFORMANCE_FILTER_GROUPS["profile"]),
        "macro_avg_pnl": build_group_average_pnl_chart(records, key="macro_grade", labels=PERFORMANCE_FILTER_GROUPS["macro_grade"]),
        "structure_avg_pnl": build_group_average_pnl_chart(records, key="structure_grade", labels=PERFORMANCE_FILTER_GROUPS["structure_grade"]),
        "outcome_composition": build_outcome_composition(outcomes),
        "credit_efficiency_by_system": build_credit_efficiency_breakdown(records, group_key="system", labels=PERFORMANCE_FILTER_GROUPS["system"]),
        "credit_efficiency_by_profile": build_credit_efficiency_breakdown(records, group_key="profile", labels=PERFORMANCE_FILTER_GROUPS["profile"]),
        "credit_efficiency_by_vix_bucket": build_credit_efficiency_breakdown(records, group_key="vix_bucket", labels=VIX_BUCKETS),
        "safety_ratio_by_system": build_group_average_chart(safety_ratio_records, group_key="system", value_key="safety_ratio", labels=PERFORMANCE_FILTER_GROUPS["system"]),
        "premium_em_by_profile": build_group_premium_em_summary(records, group_key="profile", labels=PERFORMANCE_FILTER_GROUPS["profile"]),
        "premium_em_by_vix_bucket": build_group_premium_em_summary(records, group_key="vix_bucket", labels=VIX_BUCKETS),
        "safety_ratio_expectancy_curve": build_safety_ratio_expectancy_curve(safety_optimization),
    }


def build_learning_payload(
    records: list[Dict[str, Any]],
    *,
    outcomes: Optional[OutcomeSummary] = None,
    safety_optimization: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    outcomes = outcomes or summarize_outcomes(records)
    safety_optimization = safety_optimization or build_safety_ratio_optimization(records)
    safety_ratio_summary = summarize_safety_ratio_records(records)
    premium_em_records = [record for record in safety_ratio_summary["included_records"] if record.get("premium_per_contract") is not None]
    em_policy = build_em_policy_payload(records)
    risk_efficiency_fallback_count = sum(1 for record in records if record.get("entry_credit_source") == "candidate_credit_estimate")
    credit_efficiency_records = [record for record in records if record.get("credit_efficiency_pct") is not None]
    return {
        "overview": {
            "avg_safety_ratio": average(record.get("safety_ratio") for record in safety_ratio_summary["included_records"]),
            "avg_premium_per_em": average(compute_ratio(record.get("premium_per_contract"), record.get("expected_move_used")) for record in premium_em_records),
            "avg_risk_efficiency": average(record.get("risk_efficiency") for record in records),
            "avg_credit_efficiency_pct": average(record.get("credit_efficiency_pct") for record in credit_efficiency_records),
            "qualified_credit_efficiency_trades": len(credit_efficiency_records),
            "classified_vix_trades": sum(1 for record in premium_em_records if record.get("vix_bucket") is not None),
            "safety_ratio_summary": {
                "included_trade_count": safety_ratio_summary["included_trade_count"],
                "excluded_trade_count": safety_ratio_summary["excluded_trade_count"],
                "exclusion_summary": safety_ratio_summary["exclusion_summary"],
            },
            "expected_move_usage_summary": summarize_expected_move_usage(outcomes.closed_records),
            "expected_move_weighting": {
                "actual_manual": 1.0,
                "calibrated_estimated": 0.6,
            },
            "risk_efficiency_definition": "Premium collected divided by maximum theoretical risk at entry.",
            "risk_efficiency_credit_note": "Uses actual entry credit when available; otherwise falls back to candidate credit estimate.",
            "risk_efficiency_fallback_count": risk_efficiency_fallback_count,
            "credit_efficiency_definition": "Premium received ÷ max theoretical loss",
            "credit_efficiency_credit_note": "Uses actual entry credit when available; otherwise falls back to candidate credit estimate.",
            "credit_efficiency_fallback_count": risk_efficiency_fallback_count,
        },
        "safety_optimization": safety_optimization,
        "adaptive_safety_guidance": safety_optimization.get("adaptive_guidance") or {},
        "em_policy": em_policy,
        "trade_mode_split": build_trade_mode_split(records),
        "result_audit": {
            "counts": {
                "open": len(outcomes.open_records),
                "win": len(outcomes.wins),
                "loss": len(outcomes.losses),
                "black_swan": len(outcomes.black_swans),
                "scratched": len(outcomes.scratched),
            },
            "rules": [
                "Open and Reduced trades stay outside win-rate and expectancy scoring.",
                "A closed trade becomes Black Swan when realized loss is greater than 40% of stored maximum theoretical loss.",
                "Closed trades with explicit win_loss_result of Win, Loss, Black Swan, or Scratched remain the fallback if realized P/L is unavailable.",
                "Closed trades with positive gross P/L classify as Win and negative gross P/L classify as Loss when the Black Swan threshold is not met.",
                "Closed trades with zero realized P/L classify as Scratched and are excluded from win-rate and expectancy scoring.",
            ],
        },
    }


def build_profile_em_performance_payload(records: list[Dict[str, Any]]) -> Dict[str, Any]:
    eligible_records: list[Dict[str, Any]] = []
    excluded_trade_count = 0
    for record in records:
        if record.get("status") in {"Open", "Reduced"}:
            excluded_trade_count += 1
            continue
        entry_em_multiple, source = resolve_profile_entry_em_multiple(record)
        if entry_em_multiple is None or entry_em_multiple <= 0 or entry_em_multiple > MAX_ANALYTIC_EM_MULTIPLE:
            excluded_trade_count += 1
            continue
        eligible_records.append({**record, "entry_em_multiple": entry_em_multiple, "entry_em_multiple_source": source})

    profile_items = []
    for profile_label in determine_profile_em_labels(eligible_records):
        profile_records = [record for record in eligible_records if record.get("profile") == profile_label]
        if not profile_records:
            continue
        intervals = build_distribution_intervals([record["entry_em_multiple"] for record in profile_records], round_step=0.1)
        profile_items.append(
            {
                "profile": profile_label,
                "qualified_trade_count": len(profile_records),
                "interval_range": describe_numeric_span([record["entry_em_multiple"] for record in profile_records], suffix="x EM"),
                "buckets": build_interval_performance_rows(profile_records, value_key="entry_em_multiple", intervals=intervals, value_suffix="x EM"),
            }
        )

    return {
        "qualified_trade_count": len(eligible_records),
        "excluded_trade_count": excluded_trade_count,
        "profiles": profile_items,
        "method_note": "Buckets are derived from each profile's own stored EM-multiple distribution, not a universal ladder.",
        "limitation_note": "Profiles without enough closed trades stay visible but may only show one low-confidence interval.",
    }


def build_distance_efficiency_payload(records: list[Dict[str, Any]]) -> Dict[str, Any]:
    eligible_records = []
    excluded_trade_count = 0
    for record in records:
        if record.get("status") in {"Open", "Reduced"}:
            excluded_trade_count += 1
            continue
        distance_to_short = coerce_float(record.get("actual_distance_to_short"))
        if distance_to_short is None or distance_to_short <= 0:
            distance_to_short = coerce_float(record.get("distance_to_short"))
        premium_per_contract = coerce_float(record.get("premium_per_contract"))
        if distance_to_short is None or distance_to_short <= 0 or premium_per_contract is None:
            excluded_trade_count += 1
            continue
        eligible_records.append({**record, "distance_efficiency_score": premium_per_contract / distance_to_short})

    intervals = build_distribution_intervals([record["distance_efficiency_score"] for record in eligible_records], round_step=0.1)
    return {
        "qualified_trade_count": len(eligible_records),
        "excluded_trade_count": excluded_trade_count,
        "buckets": build_interval_performance_rows(eligible_records, value_key="distance_efficiency_score", intervals=intervals, value_suffix=" premium/point"),
        "definition": "Premium per contract divided by short-strike distance at entry.",
        "limitation_note": "Uses stored premium and distance only; it does not estimate unrecorded fill quality or intraday repricing.",
    }


def build_time_of_entry_edge_payload(records: list[Dict[str, Any]]) -> Dict[str, Any]:
    systems = []
    qualified_trade_count = 0
    excluded_trade_count = 0
    for system_label in PERFORMANCE_FILTER_GROUPS["system"]:
        eligible_records = []
        for record in records:
            if record.get("system") != system_label or record.get("status") in {"Open", "Reduced"}:
                continue
            entry_minutes = parse_entry_minutes(record.get("entry_datetime"))
            if entry_minutes is None:
                continue
            eligible_records.append({**record, "entry_minutes": entry_minutes})
        missing_for_system = sum(
            1
            for record in records
            if record.get("system") == system_label
            and record.get("status") not in {"Open", "Reduced"}
            and parse_entry_minutes(record.get("entry_datetime")) is None
        )
        excluded_trade_count += missing_for_system
        qualified_trade_count += len(eligible_records)
        if not eligible_records:
            continue
        intervals = build_distribution_intervals([record["entry_minutes"] for record in eligible_records], round_step=15.0)
        systems.append(
            {
                "system": system_label,
                "qualified_trade_count": len(eligible_records),
                "buckets": build_interval_performance_rows(
                    eligible_records,
                    value_key="entry_minutes",
                    intervals=intervals,
                    label_builder=format_time_interval_label,
                ),
            }
        )

    return {
        "qualified_trade_count": qualified_trade_count,
        "excluded_trade_count": excluded_trade_count,
        "systems": systems,
        "method_note": "Entry windows are derived separately for each system from stored entry timestamps.",
        "limitation_note": "Trades without stored entry timestamps are excluded from this block.",
    }


def build_exit_efficiency_payload(records: list[Dict[str, Any]]) -> Dict[str, Any]:
    eligible_records = [record for record in records if record.get("status") not in {"Open", "Reduced"}]
    categories = [
        ("Full Hold to Expiration", lambda record: classify_exit_efficiency_category(record) == "full_hold_to_expiration"),
        ("Partial Exit Used", lambda record: classify_exit_efficiency_category(record) == "partial_exit_used"),
        ("Staged / Laddered Exit", lambda record: classify_exit_efficiency_category(record) == "staged_exit"),
        ("Early Risk Exit", lambda record: classify_exit_efficiency_category(record) == "early_risk_exit"),
        ("Early Non-Risk Close", lambda record: classify_exit_efficiency_category(record) == "early_non_risk_close"),
    ]
    rows = []
    for label, predicate in categories:
        grouped_records = [record for record in eligible_records if predicate(record)]
        if not grouped_records:
            continue
        summary = summarize_bucket_performance(grouped_records)
        rows.append(
            {
                "label": label,
                "trade_count": len(grouped_records),
                "win_rate": summary["win_rate"],
                "expectancy": summary["expectancy"],
                "average_pnl": summary["average_pnl"],
                "average_realized_pnl": summary["average_pnl"],
                "average_loss_avoided": round(average(compute_loss_avoided(record) for record in grouped_records if compute_loss_avoided(record) is not None), 2),
                "confidence": confidence_label(len(grouped_records)),
            }
        )
    return {
        "qualified_trade_count": len(eligible_records),
        "excluded_trade_count": max(len(records) - len(eligible_records), 0),
        "categories": rows,
        "limitation_note": "Loss avoided is only computed where stored max theoretical risk and realized P/L are both available; it does not reconstruct unrecorded alternate exits.",
    }


def build_vix_regime_performance_payload(records: list[Dict[str, Any]]) -> Dict[str, Any]:
    eligible_records = [
        record
        for record in records
        if record.get("status") not in {"Open", "Reduced"} and record.get("vix_at_entry") is not None
    ]
    intervals = build_vix_regime_intervals([coerce_float(record.get("vix_at_entry")) for record in eligible_records])
    return {
        "qualified_trade_count": len(eligible_records),
        "excluded_trade_count": sum(1 for record in records if record.get("status") not in {"Open", "Reduced"} and record.get("vix_at_entry") is None),
        "buckets": build_interval_performance_rows(eligible_records, value_key="vix_at_entry", intervals=intervals),
        "method_note": "VIX regimes use stored VIX-at-entry values only and adapt to the observed range when the sample is narrow.",
    }


def build_expected_move_breach_analysis_payload(records: list[Dict[str, Any]]) -> Dict[str, Any]:
    systems = []
    qualified_trade_count = 0
    excluded_trade_count = 0
    for system_label in PERFORMANCE_FILTER_GROUPS["system"]:
        eligible_records = []
        for record in records:
            if record.get("system") != system_label or record.get("status") in {"Open", "Reduced"}:
                continue
            entry_em_multiple, _ = resolve_profile_entry_em_multiple(record)
            if entry_em_multiple is None or entry_em_multiple <= 0 or entry_em_multiple > MAX_ANALYTIC_EM_MULTIPLE:
                continue
            eligible_records.append({**record, "entry_em_multiple": entry_em_multiple})
        excluded_trade_count += sum(
            1
            for record in records
            if record.get("system") == system_label
            and record.get("status") not in {"Open", "Reduced"}
            and (
                resolve_profile_entry_em_multiple(record)[0] is None
                or resolve_profile_entry_em_multiple(record)[0] <= 0
                or resolve_profile_entry_em_multiple(record)[0] > MAX_ANALYTIC_EM_MULTIPLE
            )
        )
        qualified_trade_count += len(eligible_records)
        if not eligible_records:
            continue
        intervals = build_distribution_intervals([record["entry_em_multiple"] for record in eligible_records], round_step=0.1)
        bucket_rows = []
        for interval in intervals:
            grouped_records = [record for record in eligible_records if interval_contains_value(interval, record.get("entry_em_multiple"))]
            if not grouped_records:
                continue
            summary = summarize_bucket_performance(grouped_records)
            short_breaches = [record for record in grouped_records if is_short_strike_breached(record)]
            long_breaches = [record for record in grouped_records if is_long_strike_breached(record)]
            bucket_rows.append(
                {
                    "label": build_interval_label(interval, suffix="x EM"),
                    "trade_count": len(grouped_records),
                    "short_breach_rate": round((len(short_breaches) / len(grouped_records)) * 100, 2),
                    "long_breach_rate": round((len(long_breaches) / len(grouped_records)) * 100, 2),
                    "expectancy": summary["expectancy"],
                    "average_pnl": summary["average_pnl"],
                    "confidence": confidence_label(len(grouped_records)),
                }
            )
        systems.append({"system": system_label, "buckets": bucket_rows, "qualified_trade_count": len(eligible_records)})

    return {
        "qualified_trade_count": qualified_trade_count,
        "excluded_trade_count": excluded_trade_count,
        "systems": systems,
        "method_note": "Breach rates use recorded exit prices and close-event exit spots, so they reflect terminal breach evidence rather than full intraday pathing.",
    }


def build_performance_data_limitations(learning_payload: Dict[str, Any]) -> list[str]:
    notes = []
    if (learning_payload.get("time_of_entry_edge") or {}).get("excluded_trade_count"):
        notes.append("Time of Entry Edge excludes trades missing stored entry timestamps.")
    notes.append("Exit Efficiency uses current stored close-event history only and does not infer unlogged discretionary exits.")
    notes.append("Expected Move Deviation / Breach Analysis uses recorded exit-time prices, not full intraday excursion data.")
    if (learning_payload.get("vix_regime_performance") or {}).get("excluded_trade_count"):
        notes.append("VIX Regime Performance excludes trades that do not have stored VIX-at-entry values.")
    return notes


def summarize_bucket_performance(records: list[Dict[str, Any]]) -> Dict[str, float]:
    outcomes = summarize_outcomes(records)
    scored_count = len(outcomes.scored_outcomes)
    win_rate = (len(outcomes.wins) / scored_count) if scored_count else 0.0
    loss_rate = (len(outcomes.loss_events) / scored_count) if scored_count else 0.0
    average_win = average(record.get("gross_pnl") for record in outcomes.wins)
    average_loss = abs(average(record.get("gross_pnl") for record in outcomes.loss_events))
    expectancy = calculate_expectancy(
        win_rate=win_rate,
        average_win=average_win,
        loss_rate=loss_rate,
        average_loss=average_loss,
    ) if scored_count else 0.0
    return {
        "win_rate": round(win_rate * 100, 2),
        "expectancy": round(expectancy, 2),
        "average_pnl": round(average(record.get("gross_pnl") for record in records), 2),
    }


def build_interval_performance_rows(
    records: list[Dict[str, Any]],
    *,
    value_key: str,
    intervals: list[Dict[str, Any]],
    value_suffix: str = "",
    label_builder=None,
) -> list[Dict[str, Any]]:
    rows = []
    for interval in intervals:
        grouped_records = [record for record in records if interval_contains_value(interval, record.get(value_key))]
        if not grouped_records:
            continue
        summary = summarize_bucket_performance(grouped_records)
        label = label_builder(interval) if callable(label_builder) else build_interval_label(interval, suffix=value_suffix)
        rows.append(
            {
                "label": label,
                "trade_count": len(grouped_records),
                "win_rate": summary["win_rate"],
                "expectancy": summary["expectancy"],
                "average_pnl": summary["average_pnl"],
                "confidence": confidence_label(len(grouped_records)),
            }
        )
    return rows


def build_distribution_intervals(values: Iterable[Optional[float]], *, round_step: float) -> list[Dict[str, Any]]:
    numeric_values = sorted(value for value in (coerce_float(item) for item in values) if value is not None and math.isfinite(value))
    if not numeric_values:
        return []
    bucket_count = determine_bucket_count(len(numeric_values))
    minimum = round_down(numeric_values[0], round_step)
    maximum = round_up(numeric_values[-1], round_step)
    edges = [minimum]
    for index in range(1, bucket_count):
        quantile_index = min(max(math.ceil((len(numeric_values) * index) / bucket_count) - 1, 0), len(numeric_values) - 1)
        candidate = round_down(numeric_values[quantile_index], round_step)
        if candidate > edges[-1]:
            edges.append(candidate)
    if maximum <= edges[-1]:
        maximum = round_up(edges[-1] + round_step, round_step)
    edges.append(maximum)

    intervals = []
    for index in range(len(edges) - 1):
        lower = edges[index]
        upper = edges[index + 1]
        if upper <= lower:
            continue
        intervals.append({
            "minimum": lower,
            "maximum": upper,
            "inclusive_upper": index == len(edges) - 2,
        })
    return intervals or [{"minimum": minimum, "maximum": maximum, "inclusive_upper": True}]


def build_vix_regime_intervals(values: Iterable[Optional[float]]) -> list[Dict[str, Any]]:
    numeric_values = sorted(value for value in (coerce_float(item) for item in values) if value is not None and math.isfinite(value))
    if not numeric_values:
        return []
    minimum = numeric_values[0]
    maximum = numeric_values[-1]
    edges = [minimum]
    for edge in DEFAULT_VIX_REGIME_EDGES:
        if minimum < edge < maximum:
            edges.append(edge)
    edges.append(maximum)
    if len(edges) <= 2:
        return build_distribution_intervals(numeric_values, round_step=1.0)
    intervals = []
    for index in range(len(edges) - 1):
        lower = edges[index]
        upper = edges[index + 1]
        intervals.append({
            "minimum": lower,
            "maximum": upper,
            "inclusive_upper": index == len(edges) - 2,
        })
    return intervals


def determine_bucket_count(sample_size: int) -> int:
    if sample_size >= 12:
        return 4
    if sample_size >= 6:
        return 3
    if sample_size >= 3:
        return 2
    return 1


def interval_contains_value(interval: Dict[str, Any], value: Any) -> bool:
    numeric_value = coerce_float(value)
    if numeric_value is None:
        return False
    minimum = coerce_float(interval.get("minimum"))
    maximum = coerce_float(interval.get("maximum"))
    if minimum is None or maximum is None:
        return False
    if interval.get("inclusive_upper"):
        return minimum <= numeric_value <= maximum
    return minimum <= numeric_value < maximum


def build_interval_label(interval: Dict[str, Any], *, suffix: str = "") -> str:
    minimum = coerce_float(interval.get("minimum")) or 0.0
    maximum = coerce_float(interval.get("maximum")) or minimum
    suffix_text = f" {suffix}" if suffix else ""
    if interval.get("inclusive_upper"):
        return f"{minimum:.1f}-{maximum:.1f}{suffix_text}"
    return f"{minimum:.1f}-{maximum:.1f}{suffix_text}"


def describe_numeric_span(values: Iterable[Optional[float]], *, suffix: str = "") -> str:
    numeric_values = sorted(value for value in (coerce_float(item) for item in values) if value is not None and math.isfinite(value))
    if not numeric_values:
        return "No range"
    suffix_text = f" {suffix}" if suffix else ""
    return f"{numeric_values[0]:.1f} to {numeric_values[-1]:.1f}{suffix_text}"


def format_time_interval_label(interval: Dict[str, Any]) -> str:
    minimum = int(coerce_float(interval.get("minimum")) or 0)
    maximum = int(coerce_float(interval.get("maximum")) or minimum)
    return f"{format_minutes_label(minimum)}-{format_minutes_label(maximum)}"


def format_minutes_label(total_minutes: int) -> str:
    hour = max(total_minutes, 0) // 60
    minute = max(total_minutes, 0) % 60
    return f"{hour:02d}:{minute:02d}"


def parse_entry_minutes(value: Any) -> Optional[float]:
    if value in {None, ""}:
        return None
    text = str(value).strip()
    if "T" in text:
        time_portion = text.split("T", 1)[1]
        hour_text = time_portion[:2]
        minute_text = time_portion[3:5] if len(time_portion) >= 5 else "00"
        if hour_text.isdigit() and minute_text.isdigit():
            return float((int(hour_text) * 60) + int(minute_text))
    parsed = parse_datetime_value(value)
    if parsed is None:
        return None
    return float((parsed.hour * 60) + parsed.minute)


def classify_exit_efficiency_category(record: Dict[str, Any]) -> str:
    close_events = [item for item in (record.get("close_events") or []) if isinstance(item, dict)]
    reduce_events = [event for event in close_events if str(event.get("event_type") or "").strip().lower() == "reduce"]
    if record.get("has_expire_event") and not reduce_events:
        return "full_hold_to_expiration"
    if len(reduce_events) >= 2:
        return "staged_exit"
    if len(reduce_events) == 1:
        return "partial_exit_used"
    if is_early_risk_exit(record):
        return "early_risk_exit"
    return "early_non_risk_close"


def is_early_risk_exit(record: Dict[str, Any]) -> bool:
    close_reason = str(record.get("close_reason") or "").strip().lower()
    close_method = str(record.get("close_method") or "").strip().lower()
    expiration_date = parse_trade_date(record.get("expiration_date"))
    exit_datetime = parse_datetime_value(record.get("exit_datetime"))
    exited_early = bool(expiration_date and exit_datetime and exit_datetime.date() < expiration_date)
    keyword_match = any(keyword in close_reason for keyword in RISK_EXIT_REASON_KEYWORDS)
    risk_result = str(record.get("result") or "").strip().lower() in {"loss", "black swan"}
    return exited_early and (keyword_match or close_method == "close" or risk_result)


def compute_loss_avoided(record: Dict[str, Any]) -> Optional[float]:
    max_risk = coerce_float(record.get("max_theoretical_risk") if record.get("max_theoretical_risk") is not None else record.get("max_loss"))
    realized_pnl = coerce_float(record.get("gross_pnl"))
    if max_risk is None or realized_pnl is None or realized_pnl >= 0:
        return None
    realized_loss = abs(realized_pnl)
    return round(max(max_risk - realized_loss, 0.0), 2)


def is_short_strike_breached(record: Dict[str, Any]) -> bool:
    exit_price = resolve_terminal_underlying_price(record)
    short_strike = coerce_float(record.get("short_strike"))
    option_type = str(record.get("option_type") or "").strip().lower()
    if exit_price is None or short_strike is None:
        return False
    if "call" in option_type:
        return exit_price >= short_strike
    return exit_price <= short_strike


def is_long_strike_breached(record: Dict[str, Any]) -> bool:
    exit_price = resolve_terminal_underlying_price(record)
    long_strike = coerce_float(record.get("long_strike"))
    option_type = str(record.get("option_type") or "").strip().lower()
    if exit_price is None or long_strike is None:
        return False
    if "call" in option_type:
        return exit_price >= long_strike
    return exit_price <= long_strike


def resolve_terminal_underlying_price(record: Dict[str, Any]) -> Optional[float]:
    close_events = [item for item in (record.get("close_events") or []) if isinstance(item, dict)]
    for event in reversed(close_events):
        event_price = coerce_float(event.get("spx_at_exit"))
        if event_price is not None:
            return event_price
    return coerce_float(record.get("spx_at_exit"))


def round_down(value: float, step: float) -> float:
    return math.floor(value / step) * step


def round_up(value: float, step: float) -> float:
    return math.ceil(value / step) * step


def to_int_or_none(value: Any) -> Optional[int]:
    numeric = coerce_float(value)
    if numeric is None:
        return None
    return int(numeric)


def build_equity_curve(records: list[Dict[str, Any]]) -> Dict[str, Any]:
    dated_records = [record for record in records if record["trade_date"] and record["gross_pnl"] is not None]
    dated_records.sort(key=lambda record: (record["trade_date"], int(record.get("trade_number") or 0), int(record.get("id") or 0)))
    cumulative = 0.0
    points = []
    for record in dated_records:
        cumulative += record["gross_pnl"] or 0.0
        points.append(
            {
                "label": record["trade_date_label"],
                "trade_number": record.get("trade_number"),
                "value": round(record["gross_pnl"] or 0.0, 2),
                "cumulative": round(cumulative, 2),
                "result": record.get("result") or "Closed",
            }
        )
    return {"points": points, "min": min((point["cumulative"] for point in points), default=0.0), "max": max((point["cumulative"] for point in points), default=0.0)}


def build_group_sum_chart(records: list[Dict[str, Any]], *, key: str, labels: list[str]) -> Dict[str, Any]:
    items = []
    for label in labels:
        value = sum(record["gross_pnl"] or 0.0 for record in records if record.get(key) == label)
        items.append({"label": label, "value": round(value, 2)})
    return {"items": items}


def build_group_average_chart(records: list[Dict[str, Any]], *, group_key: str, value_key: str, labels: list[str]) -> Dict[str, Any]:
    items = []
    for label in labels:
        values = [record.get(value_key) for record in records if record.get(group_key) == label and record.get(value_key) is not None]
        items.append({"label": label, "value": round(average(values), 4) if values else 0.0})
    return {"items": items}


def build_credit_efficiency_breakdown(records: list[Dict[str, Any]], *, group_key: str, labels: list[str]) -> Dict[str, Any]:
    items = []
    for label in labels:
        grouped_records = [record for record in records if record.get(group_key) == label and record.get("credit_efficiency_pct") is not None]
        items.append(
            {
                "label": label,
                "avg_credit_efficiency_pct": round(average(record.get("credit_efficiency_pct") for record in grouped_records), 2) if grouped_records else 0.0,
                "trade_count": len(grouped_records),
            }
        )
    return {"items": items}


def build_group_average_pnl_chart(records: list[Dict[str, Any]], *, key: str, labels: list[str]) -> Dict[str, Any]:
    items = []
    for label in labels:
        grouped_records = [record for record in records if record.get(key) == label and record.get("gross_pnl") is not None]
        items.append(
            {
                "label": label,
                "value": round(average(record.get("gross_pnl") for record in grouped_records), 2) if grouped_records else 0.0,
                "trade_count": len(grouped_records),
            }
        )
    return {"items": items}


def build_group_expectancy_chart(records: list[Dict[str, Any]], *, group_key: str, labels: list[str]) -> Dict[str, Any]:
    items = []
    for label in labels:
        grouped_records = [record for record in records if record.get(group_key) == label]
        grouped_outcomes = summarize_outcomes(grouped_records)
        scored_count = len(grouped_outcomes.scored_outcomes)
        win_rate = (len(grouped_outcomes.wins) / scored_count) if scored_count else 0.0
        loss_rate = (len(grouped_outcomes.loss_events) / scored_count) if scored_count else 0.0
        average_win = average(record.get("gross_pnl") for record in grouped_outcomes.wins)
        average_loss = abs(average(record.get("gross_pnl") for record in grouped_outcomes.loss_events))
        expectancy = calculate_expectancy(win_rate=win_rate, average_win=average_win, loss_rate=loss_rate, average_loss=average_loss) if scored_count else 0.0
        items.append({"label": label, "value": round(expectancy, 2), "trade_count": len(grouped_records)})
    return {"items": items}


def build_group_premium_em_summary(records: list[Dict[str, Any]], *, group_key: str, labels: list[str]) -> Dict[str, Any]:
    items = []
    for label in labels:
        grouped_records = [record for record in records if record.get(group_key) == label]
        valid_records = [record for record in grouped_records if is_valid_premium_em_record(record)]
        items.append(
            {
                "label": label,
                "trade_count": len(valid_records),
                "avg_premium_per_contract": round(average(record.get("premium_per_contract") for record in valid_records), 2) if valid_records else 0.0,
                "avg_em_multiple": round(average(record.get("safety_ratio") for record in valid_records), 4) if valid_records else 0.0,
                "premium_per_em": round(average(compute_ratio(record.get("premium_per_contract"), record.get("expected_move_used")) for record in valid_records), 4) if valid_records else 0.0,
            }
        )
    return {"items": items}


def build_safety_ratio_expectancy_curve(safety_optimization: Dict[str, Any]) -> Dict[str, Any]:
    items = [
        {
            "label": item["label"],
            "value": round(item.get("expectancy") or 0.0, 2),
            "trade_count": item.get("trade_count") or 0,
            "confidence": item.get("confidence") or "No Confidence",
        }
        for item in safety_optimization.get("bucket_results") or []
    ]
    return {"items": items}


def build_safety_ratio_optimization(records: list[Dict[str, Any]]) -> Dict[str, Any]:
    safety_ratio_summary = summarize_safety_ratio_records(records, closed_only=True)
    closed_records = safety_ratio_summary["candidate_records"]
    eligible_records = safety_ratio_summary["included_records"]
    excluded_trade_count = safety_ratio_summary["excluded_trade_count"]
    bucket_results = build_safety_ratio_bucket_results(eligible_records)
    overall_best_expectancy = select_best_safety_bucket(bucket_results, metric_key="expectancy")
    overall_best_win_rate = select_best_safety_bucket(bucket_results, metric_key="win_rate")

    vix_eligible_records = [record for record in eligible_records if record.get("vix_bucket") is not None]
    vix_excluded_count = len(eligible_records) - len(vix_eligible_records)
    vix_bucket_results = []
    vix_guidance: Dict[str, Dict[str, Any]] = {}
    for label in VIX_BUCKETS:
        grouped_records = [record for record in vix_eligible_records if record.get("vix_bucket") == label]
        grouped_bucket_results = build_safety_ratio_bucket_results(grouped_records)
        best_expectancy = select_best_safety_bucket(grouped_bucket_results, metric_key="expectancy")
        best_win_rate = select_best_safety_bucket(grouped_bucket_results, metric_key="win_rate")
        guidance_entry = {
            "vix_bucket": label,
            "sample_size": len(grouped_records),
            "recommended_range": best_expectancy.get("key") if best_expectancy else None,
            "confidence": confidence_label(best_expectancy.get("weighted_trade_count") if best_expectancy else 0),
            "supporting_trade_count": best_expectancy.get("trade_count") if best_expectancy else 0,
            "weighted_supporting_trade_count": round(best_expectancy.get("weighted_trade_count") or 0.0, 2) if best_expectancy else 0.0,
            "best_by_expectancy": build_safety_bucket_pick(best_expectancy),
            "best_by_win_rate": build_safety_bucket_pick(best_win_rate),
            "bucket_results": grouped_bucket_results,
        }
        vix_bucket_results.append(guidance_entry)
        vix_guidance[label] = {
            "recommended_range": guidance_entry["recommended_range"],
            "confidence": normalize_confidence_key(guidance_entry["confidence"]),
            "supporting_trade_count": guidance_entry["supporting_trade_count"],
            "weighted_supporting_trade_count": guidance_entry["weighted_supporting_trade_count"],
            "sample_size": guidance_entry["sample_size"],
            "best_by_win_rate": best_win_rate.get("key") if best_win_rate else None,
        }

    recommendation = {
        "overall_optimal_safety_range": overall_best_expectancy.get("key") if overall_best_expectancy else None,
        "overall_confidence": normalize_confidence_key(confidence_label(overall_best_expectancy.get("weighted_trade_count") if overall_best_expectancy else 0)),
        "overall_supporting_trade_count": overall_best_expectancy.get("trade_count") if overall_best_expectancy else 0,
        "overall_weighted_supporting_trade_count": round(overall_best_expectancy.get("weighted_trade_count") or 0.0, 2) if overall_best_expectancy else 0.0,
        "overall_best_by_win_rate": overall_best_win_rate.get("key") if overall_best_win_rate else None,
        "vix_guidance": vix_guidance,
    }

    return {
        "bucket_results": bucket_results,
        "closed_trade_count": len(closed_records),
        "eligible_trade_count": len(eligible_records),
        "excluded_trade_count": excluded_trade_count,
        "distance_source_counts": summarize_distance_sources(closed_records),
        "expected_move_source_counts": summarize_expected_move_sources(closed_records),
        "expected_move_usage_summary": summarize_expected_move_usage(closed_records),
        "exclusion_summary": {
            **safety_ratio_summary["exclusion_summary"],
        },
        "vix_excluded_count": vix_excluded_count,
        "overall_best_by_expectancy": build_safety_bucket_pick(overall_best_expectancy),
        "overall_best_by_win_rate": build_safety_bucket_pick(overall_best_win_rate),
        "vix_bucket_results": vix_bucket_results,
        "adaptive_guidance": {
            "overall": build_safety_bucket_pick(overall_best_expectancy),
            "overall_best_by_win_rate": build_safety_bucket_pick(overall_best_win_rate),
            "by_vix_bucket": vix_bucket_results,
            "recommendation": recommendation,
        },
    }


def build_safety_ratio_bucket_results(records: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
    items = []
    for bucket_order, bucket in enumerate(SAFETY_RATIO_BUCKETS):
        grouped_records = [record for record in records if classify_safety_ratio_bucket(record.get("safety_ratio")) == bucket["key"]]
        grouped_outcomes = summarize_outcomes(grouped_records)
        scored_count = len(grouped_outcomes.scored_outcomes)
        weighted_trade_count = sum(expected_move_sample_weight(record) for record in grouped_records)
        weighted_scored_count = sum(expected_move_sample_weight(record) for record in grouped_outcomes.scored_outcomes)
        win_count = len(grouped_outcomes.wins)
        loss_count = len(grouped_outcomes.losses)
        black_swan_count = len(grouped_outcomes.black_swans)
        scratched_count = len(grouped_outcomes.scratched)
        average_win = weighted_average(grouped_outcomes.wins, value_key="gross_pnl")
        average_loss = abs(weighted_average(grouped_outcomes.loss_events, value_key="gross_pnl"))
        win_rate = (sum(expected_move_sample_weight(record) for record in grouped_outcomes.wins) / weighted_scored_count) if weighted_scored_count else 0.0
        loss_rate = (sum(expected_move_sample_weight(record) for record in grouped_outcomes.loss_events) / weighted_scored_count) if weighted_scored_count else 0.0
        expectancy = calculate_expectancy(
            win_rate=win_rate,
            average_win=average_win,
            loss_rate=loss_rate,
            average_loss=average_loss,
        ) if weighted_scored_count else 0.0
        items.append(
            {
                "bucket_order": bucket_order,
                "key": bucket["key"],
                "label": bucket["label"],
                "trade_count": len(grouped_records),
                "weighted_trade_count": round(weighted_trade_count, 2),
                "scored_trade_count": scored_count,
                "weighted_scored_trade_count": round(weighted_scored_count, 2),
                "win_count": win_count,
                "loss_count": loss_count,
                "black_swan_count": black_swan_count,
                "scratched_count": scratched_count,
                "win_rate": round(win_rate * 100, 2),
                "avg_win": round(average_win, 2),
                "avg_loss": round(average_loss, 2),
                "total_pnl": round(sum(record.get("gross_pnl") or 0.0 for record in grouped_records), 2),
                "expectancy": round(expectancy, 2),
                "confidence": confidence_label(weighted_trade_count),
            }
        )
    return items


def build_safety_bucket_pick(bucket: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not bucket:
        return None
    return {
        "key": bucket["key"],
        "label": bucket["label"],
        "confidence": bucket["confidence"],
        "supporting_trade_count": bucket["trade_count"],
        "weighted_supporting_trade_count": round(bucket.get("weighted_trade_count") or 0.0, 2),
        "sample_size": bucket["trade_count"],
        "expectancy": bucket["expectancy"],
        "win_rate": bucket["win_rate"],
    }


def select_best_safety_bucket(bucket_results: list[Dict[str, Any]], *, metric_key: str) -> Optional[Dict[str, Any]]:
    candidates = [item for item in bucket_results if item.get("weighted_trade_count", 0) > 0 and item.get("weighted_scored_trade_count", 0) > 0]
    if not candidates:
        return None
    if metric_key == "win_rate":
        return max(candidates, key=lambda item: (item.get("win_rate") or 0.0, item.get("weighted_trade_count") or 0.0, item.get("expectancy") or 0.0, -(item.get("bucket_order") or 0)))
    return max(candidates, key=lambda item: (item.get("expectancy") or 0.0, item.get("weighted_trade_count") or 0.0, item.get("win_rate") or 0.0, -(item.get("bucket_order") or 0)))


def classify_safety_ratio_bucket(safety_ratio: Optional[float]) -> Optional[str]:
    if safety_ratio is None:
        return None
    for bucket in SAFETY_RATIO_BUCKETS:
        minimum = bucket["minimum"]
        maximum = bucket["maximum"]
        if minimum is None and maximum is not None and safety_ratio < maximum:
            return bucket["key"]
        if minimum is not None and maximum is None and safety_ratio >= minimum:
            return bucket["key"]
        if minimum is not None and maximum is not None and minimum <= safety_ratio < maximum:
            return bucket["key"]
    return None


def confidence_label(sample_size: float) -> str:
    if sample_size >= 8:
        return "High Confidence"
    if sample_size >= 4:
        return "Medium Confidence"
    if sample_size >= 1:
        return "Low Confidence"
    return "No Confidence"


def normalize_confidence_key(label: str) -> str:
    return str(label or "No Confidence").replace(" Confidence", "").strip().lower() or "no"


def build_trade_mode_split(records: list[Dict[str, Any]]) -> Dict[str, Any]:
    items = []
    for label in PERFORMANCE_FILTER_GROUPS["trade_mode"]:
        grouped_records = [record for record in records if record.get("trade_mode") == label]
        grouped_outcomes = summarize_outcomes(grouped_records)
        scored_count = len(grouped_outcomes.scored_outcomes)
        win_rate = (len(grouped_outcomes.wins) / scored_count) if scored_count else 0.0
        loss_rate = (len(grouped_outcomes.loss_events) / scored_count) if scored_count else 0.0
        average_win = average(record.get("gross_pnl") for record in grouped_outcomes.wins)
        average_loss = abs(average(record.get("gross_pnl") for record in grouped_outcomes.loss_events))
        expectancy = calculate_expectancy(win_rate=win_rate, average_win=average_win, loss_rate=loss_rate, average_loss=average_loss) if scored_count else 0.0
        items.append(
            {
                "label": label,
                "trade_count": len(grouped_records),
                "open_trades": len(grouped_outcomes.open_records),
                "net_pnl": round(sum(record.get("gross_pnl") or 0.0 for record in grouped_records), 2),
                "win_rate": round(win_rate * 100, 2),
                "expectancy": round(expectancy, 2),
            }
        )
    return {"items": items}


def determine_profile_em_labels(records: list[Dict[str, Any]]) -> list[str]:
    known_labels = PERFORMANCE_FILTER_GROUPS["profile"]
    observed_labels = {str(record.get("profile") or "").strip() for record in records if str(record.get("profile") or "").strip()}
    ordered = [label for label in known_labels if label in observed_labels]
    dynamic_labels = sorted(label for label in observed_labels if label not in known_labels)
    return ordered + dynamic_labels


def resolve_profile_entry_em_multiple(record: Dict[str, Any]) -> tuple[Optional[float], str]:
    actual_em_multiple = coerce_float(record.get("actual_em_multiple"))
    if actual_em_multiple is not None and actual_em_multiple > 0:
        return actual_em_multiple, "actual_em_multiple"
    safety_ratio = coerce_float(record.get("safety_ratio"))
    if safety_ratio is not None and safety_ratio > 0:
        return safety_ratio, "safety_ratio"
    return None, "unavailable"


def summarize_distance_sources(records: Iterable[Dict[str, Any]]) -> Dict[str, int]:
    counts = {
        "original": 0,
        "derived": 0,
        "estimated_fallback": 0,
        "unresolved": 0,
    }
    for record in records:
        source = str(record.get("distance_source") or "unresolved")
        counts[source] = counts.get(source, 0) + 1
    return counts


def summarize_expected_move_sources(records: Iterable[Dict[str, Any]]) -> Dict[str, int]:
    counts = {
        "original": 0,
        "recovered_candidate": 0,
        "recovered_snapshot": 0,
        "estimated_calibrated": 0,
        "same_day_atm_straddle": 0,
        "unresolved": 0,
    }
    for record in records:
        source = normalize_expected_move_source(record.get("expected_move_source"))
        counts[source] = counts.get(source, 0) + 1
    return counts


def summarize_expected_move_usage(records: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    counts = {
        EXPECTED_MOVE_USAGE_ACTUAL: 0,
        EXPECTED_MOVE_USAGE_ESTIMATED: 0,
        EXPECTED_MOVE_USAGE_EXCLUDED: 0,
    }
    weighted_support = 0.0
    for record in records:
        usage = classify_expected_move_usage(record.get("expected_move_source"), record.get("expected_move_used"))
        counts[usage] = counts.get(usage, 0) + 1
        weighted_support += expected_move_sample_weight(record)
    counts["weighted_support"] = round(weighted_support, 2)
    return counts


def expected_move_sample_weight(record: Dict[str, Any]) -> float:
    return float(record.get("expected_move_weight") or expected_move_learning_weight(record.get("expected_move_source"), record.get("expected_move_confidence")))


def summarize_safety_ratio_records(records: Iterable[Dict[str, Any]], *, closed_only: bool = False) -> Dict[str, Any]:
    candidate_records = []
    included_records = []
    exclusion_summary = {
        "open_trade_count": 0,
        "distance_unresolved_count": 0,
        "distance_conflict_count": 0,
        "expected_move_missing_count": 0,
        "expected_move_excluded_count": 0,
        "expected_move_too_small_count": 0,
        "ratio_unavailable_count": 0,
    }

    for record in records:
        if closed_only and record.get("status") in {"Open", "Reduced"}:
            exclusion_summary["open_trade_count"] += 1
            continue

        candidate_records.append(record)
        exclusion_reason = classify_safety_ratio_exclusion(record)
        if exclusion_reason is None:
            included_records.append(record)
            continue
        exclusion_summary[exclusion_reason] += 1

    excluded_trade_count = sum(exclusion_summary.values())
    return {
        "candidate_records": candidate_records,
        "included_records": included_records,
        "included_trade_count": len(included_records),
        "excluded_trade_count": excluded_trade_count,
        "exclusion_summary": exclusion_summary,
    }


def classify_safety_ratio_exclusion(record: Dict[str, Any]) -> Optional[str]:
    distance_to_short = coerce_float(record.get("distance_to_short"))
    if distance_to_short is None:
        return "distance_unresolved_count"
    if record.get("distance_has_material_discrepancy"):
        return "distance_conflict_count"
    short_strike = coerce_float(record.get("short_strike"))
    spx_at_entry = coerce_float(record.get("spx_at_entry"))
    if spx_at_entry is None and short_strike is not None and abs(distance_to_short - short_strike) <= 0.01:
        return "distance_conflict_count"

    expected_move_used = coerce_float(record.get("expected_move_used"))
    if expected_move_used is None:
        return "expected_move_missing_count"
    if record.get("expected_move_usage") == EXPECTED_MOVE_USAGE_EXCLUDED:
        return "expected_move_excluded_count"
    if expected_move_used < MIN_EXPECTED_MOVE_USED:
        return "expected_move_too_small_count"
    if record.get("safety_ratio") is None:
        return "ratio_unavailable_count"
    return None


def is_valid_premium_em_record(record: Dict[str, Any]) -> bool:
    if record.get("premium_per_contract") is None:
        return False
    return classify_safety_ratio_exclusion(record) is None


def weighted_average(
    records: Iterable[Dict[str, Any]],
    *,
    value_key: str,
    weight_resolver=expected_move_sample_weight,
) -> float:
    weighted_total = 0.0
    total_weight = 0.0
    for record in records:
        value = record.get(value_key)
        weight = weight_resolver(record)
        if value is None or weight <= 0:
            continue
        weighted_total += float(value) * weight
        total_weight += weight
    if total_weight <= 0:
        return 0.0
    return weighted_total / total_weight


def build_outcome_composition(outcomes: OutcomeSummary) -> Dict[str, Any]:
    items = []
    for label in PERFORMANCE_FILTER_GROUPS["result"]:
        count = outcomes.outcome_count(label)
        items.append({"label": label, "value": count})
    return {"items": items, "total": sum(item["value"] for item in items)}


def calculate_expectancy(*, win_rate: float, average_win: float, loss_rate: float, average_loss: float) -> float:
    return (win_rate * average_win) - (loss_rate * average_loss)


def classify_trade_result(trade: Dict[str, Any]) -> str:
    current_status = str(trade.get("derived_status_raw") or trade.get("status") or "").strip().lower()
    if current_status in {"open", "reduced"}:
        return ""
    result = classify_closed_trade_outcome(
        gross_pnl=trade.get("gross_pnl") if trade.get("gross_pnl") is not None else trade.get("pnl"),
        max_theoretical_risk=trade.get("total_max_loss") if trade.get("total_max_loss") is not None else trade.get("max_theoretical_risk") if trade.get("max_theoretical_risk") is not None else trade.get("max_loss"),
        explicit_result=trade.get("win_loss_result") or trade.get("result"),
        close_reason=trade.get("close_reason"),
    )
    if result == "Flat":
        return "Scratched"
    if result in {"Win", "Loss", "Black Swan", "Scratched"}:
        return result

    gross_pnl = coerce_float(trade.get("gross_pnl") if trade.get("gross_pnl") is not None else trade.get("pnl"))
    if gross_pnl is not None:
        if gross_pnl > 0:
            return "Win"
        if gross_pnl < 0:
            return "Loss"
        return "Scratched"
    return ""


def normalize_macro_grade(value: Any) -> str:
    text = str(value or "").strip().lower()
    if "major" in text:
        return "Major"
    if "minor" in text:
        return "Minor"
    return "None"


def normalize_structure_grade(value: Any) -> str:
    text = str(value or "").strip().lower()
    if any(keyword in text for keyword in ("poor", "bad", "reject", "blocked", "fail")):
        return "Poor"
    if any(keyword in text for keyword in ("good", "allowed", "strong")):
        return "Good"
    return "Neutral"


def normalize_filter_value(group: str, value: Any) -> str:
    text = str(value or "").strip().lower()
    if group in {"result", "trade_mode", "macro_grade", "structure_grade", "profile", "system", "timeframe"}:
        return text.replace(" ", "-")
    return text


def pick_performance_date(trade: Dict[str, Any]) -> str:
    expiration_date = str(trade.get("expiration_date") or "").strip()
    if expiration_date:
        return expiration_date.split("T", 1)[0]
    trade_date = str(trade.get("trade_date") or "").strip()
    if trade_date:
        return trade_date.split("T", 1)[0]
    entry_datetime = str(trade.get("entry_datetime") or "").strip()
    if entry_datetime:
        return entry_datetime.split("T", 1)[0]
    created_at = str(trade.get("created_at") or "").strip()
    if created_at:
        return created_at.split("T", 1)[0]
    return ""


def derive_max_loss(trade: Dict[str, Any], *, actual_entry_credit: Optional[float]) -> Optional[float]:
    credit_model = resolve_trade_credit_model(trade)
    existing_max_loss = coerce_float(
        trade.get("max_theoretical_risk")
        if trade.get("max_theoretical_risk") is not None
        else (trade.get("max_risk") if trade.get("max_risk") is not None else trade.get("max_loss"))
    )
    if existing_max_loss is not None and existing_max_loss > 0:
        return existing_max_loss
    derived = credit_model.get("max_theoretical_risk")
    return round(derived, 2) if derived is not None else None


def derive_entry_credit(trade: Dict[str, Any]) -> tuple[Optional[float], str]:
    credit_model = resolve_trade_credit_model(trade)
    actual_entry_credit = coerce_float(credit_model.get("actual_entry_credit"))
    if actual_entry_credit is not None:
        return actual_entry_credit, str(credit_model.get("source") or "actual_entry_credit")
    return None, "unavailable"


def compute_risk_efficiency(*, total_premium: Optional[float], max_loss: Optional[float]) -> Optional[float]:
    if total_premium is None or max_loss in {None, 0}:
        return None
    return compute_ratio(total_premium, max_loss)


def classify_vix_bucket(vix_at_entry: Optional[float]) -> Optional[str]:
    if vix_at_entry is None:
        return None
    if vix_at_entry < 18:
        return "<18"
    if vix_at_entry < 22:
        return "18-22"
    if vix_at_entry < 26:
        return "22-26"
    return "26+"


def compute_ratio(numerator: Optional[float], denominator: Optional[float]) -> Optional[float]:
    if numerator is None or denominator in {None, 0}:
        return None
    return round(numerator / denominator, 6)


def average(values: Iterable[Optional[float]]) -> float:
    cleaned = [float(value) for value in values if value is not None and math.isfinite(float(value))]
    if not cleaned:
        return 0.0
    return sum(cleaned) / len(cleaned)


def coerce_float(value: Any) -> Optional[float]:
    if value in {None, "", "—"}:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None
