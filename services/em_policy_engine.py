"""Dynamic EM threshold guidance derived from completed Talos trades."""

from __future__ import annotations

import math
from typing import Any, Dict, Iterable, List, Optional

from .trade_store import classify_expected_move_usage, expected_move_learning_weight


SUPPORTED_SYSTEMS = ("Apollo", "Talos")
EM_MULTIPLE_BUCKETS = (
    {"key": "<1.2", "label": "<1.2", "minimum": None, "maximum": 1.2},
    {"key": "1.2-1.4", "label": "1.2-1.4", "minimum": 1.2, "maximum": 1.4},
    {"key": "1.4-1.6", "label": "1.4-1.6", "minimum": 1.4, "maximum": 1.6},
    {"key": "1.6-1.8", "label": "1.6-1.8", "minimum": 1.6, "maximum": 1.8},
    {"key": "1.8-2.0", "label": "1.8-2.0", "minimum": 1.8, "maximum": 2.0},
    {"key": "2.0-2.2", "label": "2.0-2.2", "minimum": 2.0, "maximum": 2.2},
    {"key": "2.2-2.5", "label": "2.2-2.5", "minimum": 2.2, "maximum": 2.5},
    {"key": "2.5+", "label": "2.5+", "minimum": 2.5, "maximum": None},
)
VIX_BUCKETS = ("<18", "18-22", "22-26", "26+")


def build_em_policy_payload(records: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    return EmPolicyEngine().build_policy(records)


class EmPolicyEngine:
    """Analyze completed trades and surface guidance-only EM recommendations."""

    def build_policy(self, records: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
        items = list(records or [])
        result: Dict[str, Any] = {
            "qualified_trade_count": 0,
            "excluded_trade_count": 0,
            "excluded_reasons": {},
            "expected_move_usage_summary": {
                "actual_manual": 0,
                "calibrated_estimated": 0,
                "excluded": 0,
                "weighted_support": 0.0,
            },
        }
        for system in SUPPORTED_SYSTEMS:
            analysis = self._analyze_system(system, items)
            result[system.lower()] = analysis
            result["qualified_trade_count"] += analysis["qualified_trade_count"]
            result["excluded_trade_count"] += analysis["excluded_trade_count"]
            self._merge_counts(result["excluded_reasons"], analysis["excluded_reasons"])
            self._merge_expected_move_usage(result["expected_move_usage_summary"], analysis["expected_move_usage_summary"])
        return result

    def _analyze_system(self, system: str, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        system_records = [record for record in records if str(record.get("system") or "").strip() == system]
        qualified_records: List[Dict[str, Any]] = []
        excluded_reasons: Dict[str, int] = {}

        for record in system_records:
            exclusion_reason = self._classify_exclusion_reason(record)
            if exclusion_reason:
                excluded_reasons[exclusion_reason] = excluded_reasons.get(exclusion_reason, 0) + 1
                continue
            qualified_records.append(self._normalize_record(record))

        bucket_results = self._build_bucket_results(qualified_records)
        overall_best = self._select_best_bucket(bucket_results)
        overall_best_win_rate = self._select_best_bucket(bucket_results, metric_key="win_rate")
        vix_guidance, vix_bucket_results = self._build_vix_guidance(qualified_records)
        structure_guidance = self._build_structure_guidance(qualified_records)
        fallback_effect = self._build_fallback_effect(qualified_records)

        return {
            "system": system,
            "qualified_trade_count": len(qualified_records),
            "excluded_trade_count": sum(excluded_reasons.values()),
            "excluded_reasons": excluded_reasons,
            "expected_move_usage_summary": self._summarize_expected_move_usage(qualified_records, len(excluded_reasons) and excluded_reasons or {}),
            "overall_recommended_em_range": overall_best.get("key") if overall_best else None,
            "overall_confidence": self._confidence_key(overall_best.get("weighted_trade_count") if overall_best else 0),
            "overall_recommendation_label": overall_best.get("label") if overall_best else None,
            "overall_supporting_trade_count": overall_best.get("trade_count") if overall_best else 0,
            "overall_weighted_supporting_trade_count": round(overall_best.get("weighted_trade_count") or 0.0, 2) if overall_best else 0.0,
            "overall_best_by_win_rate": self._build_bucket_pick(overall_best_win_rate),
            "bucket_results": bucket_results,
            "vix_guidance": vix_guidance,
            "vix_bucket_results": vix_bucket_results,
            "structure_guidance": structure_guidance,
            "fallback_effect": fallback_effect,
            "learning_status": {
                "qualified_trade_count": len(qualified_records),
                "excluded_trade_count": sum(excluded_reasons.values()),
                "excluded_reasons": excluded_reasons,
            },
        }

    def _build_vix_guidance(self, records: List[Dict[str, Any]]) -> tuple[Dict[str, Dict[str, Any]], List[Dict[str, Any]]]:
        guidance: Dict[str, Dict[str, Any]] = {}
        bucket_results: List[Dict[str, Any]] = []
        for label in VIX_BUCKETS:
            grouped_records = [record for record in records if record.get("vix_bucket") == label]
            grouped_bucket_results = self._build_bucket_results(grouped_records)
            best_bucket = self._select_best_bucket(grouped_bucket_results)
            confidence = self._confidence_key(best_bucket.get("weighted_trade_count") if best_bucket else 0)
            guidance[label] = {
                "recommended_range": best_bucket.get("key") if best_bucket else None,
                "confidence": confidence,
                "supporting_trade_count": best_bucket.get("trade_count") if best_bucket else 0,
                "weighted_supporting_trade_count": round(best_bucket.get("weighted_trade_count") or 0.0, 2) if best_bucket else 0.0,
                "sample_size": len(grouped_records),
            }
            bucket_results.append(
                {
                    "vix_bucket": label,
                    "recommended_range": best_bucket.get("key") if best_bucket else None,
                    "confidence": confidence,
                    "sample_size": len(grouped_records),
                    "supporting_trade_count": best_bucket.get("trade_count") if best_bucket else 0,
                    "weighted_supporting_trade_count": round(best_bucket.get("weighted_trade_count") or 0.0, 2) if best_bucket else 0.0,
                    "best_bucket": self._build_bucket_pick(best_bucket),
                    "bucket_results": grouped_bucket_results,
                }
            )
        return guidance, bucket_results

    def _build_structure_guidance(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        structure_labels = sorted({str(record.get("structure_grade") or "").strip() for record in records if record.get("structure_grade")})
        guidance: List[Dict[str, Any]] = []
        for label in structure_labels:
            grouped_records = [record for record in records if record.get("structure_grade") == label]
            best_bucket = self._select_best_bucket(self._build_bucket_results(grouped_records))
            guidance.append(
                {
                    "structure_grade": label,
                    "qualified_trade_count": len(grouped_records),
                    "recommended_range": best_bucket.get("key") if best_bucket else None,
                    "confidence": self._confidence_key(best_bucket.get("weighted_trade_count") if best_bucket else 0),
                }
            )
        return guidance

    def _build_fallback_effect(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        strict_only_records = [record for record in records if record.get("fallback_used") == "no"]
        fallback_records = [record for record in records if record.get("fallback_used") == "yes"]
        strict_expectancy = self._calculate_expectancy_from_records(strict_only_records)
        fallback_expectancy = self._calculate_expectancy_from_records(fallback_records)
        impact = "insufficient-data"
        message = "Awaiting more fallback-tagged trades."
        if strict_only_records and fallback_records:
            difference = fallback_expectancy - strict_expectancy
            if difference > 25:
                impact = "helpful"
                message = "Fallback trades are outperforming strict-only trades on expectancy."
            elif difference < -25:
                impact = "harmful"
                message = "Fallback trades are underperforming strict-only trades on expectancy."
            else:
                impact = "neutral"
                message = "Fallback trades are tracking close to strict-only expectancy."
        elif strict_only_records:
            message = "No fallback-tagged completed trades yet."

        return {
            "strict_only_trade_count": len(strict_only_records),
            "fallback_trade_count": len(fallback_records),
            "strict_only_expectancy": round(strict_expectancy, 2),
            "fallback_expectancy": round(fallback_expectancy, 2),
            "impact_label": impact,
            "impact_message": message,
        }

    def _build_bucket_results(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        for bucket in EM_MULTIPLE_BUCKETS:
            grouped_records = [record for record in records if self._classify_em_bucket(record.get("actual_em_multiple")) == bucket["key"]]
            win_records = [record for record in grouped_records if record.get("result") == "win"]
            loss_records = [record for record in grouped_records if record.get("result") == "loss"]
            black_swan_records = [record for record in grouped_records if record.get("result") == "black_swan"]
            scratched_records = [record for record in grouped_records if record.get("result") == "scratched"]
            scored_count = len(win_records) + len(loss_records) + len(black_swan_records)
            weighted_trade_count = sum(self._sample_weight(record) for record in grouped_records)
            weighted_scored_count = sum(self._sample_weight(record) for record in [*win_records, *loss_records, *black_swan_records])
            win_rate = (sum(self._sample_weight(record) for record in win_records) / weighted_scored_count) if weighted_scored_count else 0.0
            loss_rate = (sum(self._sample_weight(record) for record in [*loss_records, *black_swan_records]) / weighted_scored_count) if weighted_scored_count else 0.0
            avg_win = self._weighted_average(win_records)
            avg_loss = abs(self._weighted_average([*loss_records, *black_swan_records]))
            expectancy = ((win_rate * avg_win) - (loss_rate * avg_loss)) if weighted_scored_count else 0.0
            items.append(
                {
                    "key": bucket["key"],
                    "label": bucket["label"],
                    "trade_count": len(grouped_records),
                    "weighted_trade_count": round(weighted_trade_count, 2),
                    "win_count": len(win_records),
                    "loss_count": len(loss_records),
                    "black_swan_count": len(black_swan_records),
                    "scratched_count": len(scratched_records),
                    "win_rate": round(win_rate * 100, 2),
                    "avg_win": round(avg_win, 2),
                    "avg_loss": round(avg_loss, 2),
                    "total_pnl": round(sum(record.get("realized_pnl") or 0.0 for record in grouped_records), 2),
                    "expectancy": round(expectancy, 2),
                    "confidence": self._confidence_key(weighted_trade_count),
                    "scored_trade_count": scored_count,
                    "weighted_scored_trade_count": round(weighted_scored_count, 2),
                }
            )
        return items

    def _select_best_bucket(self, bucket_results: List[Dict[str, Any]], metric_key: str = "expectancy") -> Optional[Dict[str, Any]]:
        populated = [item for item in bucket_results if item.get("weighted_trade_count", 0) > 0 and item.get("weighted_scored_trade_count", 0) > 0]
        if not populated:
            return None
        if metric_key == "win_rate":
            return max(populated, key=lambda item: (item.get("win_rate") or 0.0, item.get("weighted_trade_count") or 0.0, item.get("expectancy") or 0.0))
        return max(populated, key=lambda item: (item.get("expectancy") or 0.0, item.get("weighted_trade_count") or 0.0, item.get("win_rate") or 0.0))

    def _build_bucket_pick(self, bucket: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not bucket:
            return None
        return {
            "key": bucket["key"],
            "label": bucket["label"],
            "confidence": bucket["confidence"],
            "supporting_trade_count": bucket["trade_count"],
            "weighted_supporting_trade_count": round(bucket.get("weighted_trade_count") or 0.0, 2),
            "expectancy": bucket["expectancy"],
            "win_rate": bucket["win_rate"],
        }

    def _calculate_expectancy_from_records(self, records: List[Dict[str, Any]]) -> float:
        bucket_results = self._build_bucket_results(records)
        trade_count = sum(item.get("weighted_trade_count", 0.0) for item in bucket_results)
        if trade_count <= 0:
            return 0.0
        total_weighted_expectancy = sum((item.get("expectancy") or 0.0) * (item.get("weighted_trade_count") or 0.0) for item in bucket_results)
        return total_weighted_expectancy / trade_count

    def _classify_exclusion_reason(self, record: Dict[str, Any]) -> Optional[str]:
        status = str(record.get("status") or "").strip().lower()
        if status in {"open", "reduced", ""}:
            return "open trade"
        if not self._is_finite_positive(record.get("expected_move_used")):
            return "missing expected_move_used"
        if not self._is_finite_positive(record.get("actual_em_multiple")):
            return "missing actual_em_multiple"
        if self._normalize_result(record.get("result")) is None:
            return "missing result"
        return None

    def _normalize_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "system": str(record.get("system") or "").strip(),
            "actual_em_multiple": self._to_float(record.get("actual_em_multiple")),
            "expected_move_used": self._to_float(record.get("expected_move_used")),
            "expected_move_source": str(record.get("expected_move_source") or "").strip(),
            "expected_move_confidence": str(record.get("expected_move_confidence") or "").strip().lower(),
            "expected_move_weight": self._sample_weight(record),
            "em_multiple_floor": self._to_float(record.get("em_multiple_floor")),
            "percent_floor": self._to_float(record.get("percent_floor")),
            "boundary_rule_used": str(record.get("boundary_rule_used") or "").strip(),
            "actual_distance_to_short": self._to_float(record.get("actual_distance_to_short")),
            "fallback_used": self._normalize_yes_no(record.get("fallback_used")),
            "fallback_rule_name": str(record.get("fallback_rule_name") or "").strip(),
            "structure_grade": str(record.get("structure_grade") or "").strip(),
            "macro_grade": str(record.get("macro_grade") or "").strip(),
            "vix_entry": self._to_float(record.get("vix_entry") if record.get("vix_entry") is not None else record.get("vix_at_entry")),
            "vix_bucket": self._classify_vix_bucket(self._to_float(record.get("vix_entry") if record.get("vix_entry") is not None else record.get("vix_at_entry"))),
            "result": self._normalize_result(record.get("result")),
            "realized_pnl": self._to_float(record.get("realized_pnl") if record.get("realized_pnl") is not None else record.get("gross_pnl")),
        }

    def _classify_em_bucket(self, value: Optional[float]) -> Optional[str]:
        if value is None:
            return None
        for bucket in EM_MULTIPLE_BUCKETS:
            minimum = bucket["minimum"]
            maximum = bucket["maximum"]
            if minimum is None and maximum is not None and value < maximum:
                return bucket["key"]
            if minimum is not None and maximum is None and value >= minimum:
                return bucket["key"]
            if minimum is not None and maximum is not None and minimum <= value < maximum:
                return bucket["key"]
        return None

    def _classify_vix_bucket(self, value: Optional[float]) -> Optional[str]:
        if value is None:
            return None
        if value < 18:
            return "<18"
        if value < 22:
            return "18-22"
        if value < 26:
            return "22-26"
        return "26+"

    def _confidence_key(self, sample_size: float) -> str:
        if sample_size >= 8:
            return "high"
        if sample_size >= 4:
            return "medium"
        if sample_size >= 1:
            return "low"
        return "none"

    @staticmethod
    def _normalize_result(value: Any) -> Optional[str]:
        text = str(value or "").strip().lower().replace(" ", "_")
        if text == "win":
            return "win"
        if text == "loss":
            return "loss"
        if text == "black_swan":
            return "black_swan"
        if text == "scratched":
            return "scratched"
        return None

    @staticmethod
    def _normalize_yes_no(value: Any) -> str:
        text = str(value or "").strip().lower()
        return "yes" if text in {"1", "true", "yes", "y"} else "no"

    @staticmethod
    def _to_float(value: Any) -> Optional[float]:
        if value in {None, "", "—"}:
            return None
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            return None
        return parsed if math.isfinite(parsed) else None

    @staticmethod
    def _is_finite_positive(value: Any) -> bool:
        if value in {None, "", "—"}:
            return False
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            return False
        return math.isfinite(parsed) and parsed > 0

    @staticmethod
    def _average(values: Iterable[Optional[float]]) -> float:
        cleaned = [float(value) for value in values if value is not None and math.isfinite(float(value))]
        if not cleaned:
            return 0.0
        return sum(cleaned) / len(cleaned)

    @staticmethod
    def _weighted_average(records: Iterable[Dict[str, Any]]) -> float:
        weighted_total = 0.0
        total_weight = 0.0
        for record in records:
            value = EmPolicyEngine._to_float(record.get("realized_pnl"))
            weight = EmPolicyEngine._sample_weight(record)
            if value is None or weight <= 0:
                continue
            weighted_total += value * weight
            total_weight += weight
        if total_weight <= 0:
            return 0.0
        return weighted_total / total_weight

    @staticmethod
    def _sample_weight(record: Dict[str, Any]) -> float:
        stored_weight = record.get("expected_move_weight")
        if stored_weight not in {None, ""}:
            try:
                parsed = float(stored_weight)
            except (TypeError, ValueError):
                parsed = None
            if parsed is not None and math.isfinite(parsed):
                return parsed
        return expected_move_learning_weight(record.get("expected_move_source"), record.get("expected_move_confidence"))

    def _summarize_expected_move_usage(self, records: List[Dict[str, Any]], excluded_reasons: Dict[str, int]) -> Dict[str, Any]:
        summary = {
            "actual_manual": 0,
            "calibrated_estimated": 0,
            "excluded": sum(excluded_reasons.values()),
            "weighted_support": 0.0,
        }
        for record in records:
            usage = classify_expected_move_usage(record.get("expected_move_source"), record.get("expected_move_used"))
            summary[usage] = summary.get(usage, 0) + 1
            summary["weighted_support"] += self._sample_weight(record)
        summary["weighted_support"] = round(summary["weighted_support"], 2)
        return summary

    @staticmethod
    def _merge_counts(target: Dict[str, int], source: Dict[str, int]) -> None:
        for key, value in source.items():
            target[key] = target.get(key, 0) + int(value or 0)

    @staticmethod
    def _merge_expected_move_usage(target: Dict[str, Any], source: Dict[str, Any]) -> None:
        for key, value in source.items():
            if key == "weighted_support":
                target[key] = round(float(target.get(key, 0.0)) + float(value or 0.0), 2)
                continue
            target[key] = target.get(key, 0) + int(value or 0)