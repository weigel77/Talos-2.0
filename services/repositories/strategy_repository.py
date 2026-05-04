"""Canonical Apollo and Kairos strategy profile storage and loaders."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict


DEFAULT_STRATEGY_VERSION = "8.0.7"


@dataclass(frozen=True)
class StrategyProfileSnapshot:
    profile_name: str
    version: str
    parameters: Dict[str, Any]
    description: str = ""
    enabled: bool = True


def build_default_strategy_snapshots() -> Dict[str, StrategyProfileSnapshot]:
    return {
        "apollo": StrategyProfileSnapshot(
            profile_name="Apollo",
            version=DEFAULT_STRATEGY_VERSION,
            description="Canonical Apollo candidate thresholds for Delphi 8.0.7.",
            enabled=True,
            parameters={
                "target_widths": [5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0],
                "min_net_credit": 1.0,
                "shared_min_risk_efficiency": 0.05,
                "aggressive_min_risk_efficiency": 0.05,
                "standard_min_risk_efficiency": 0.05,
                "fortress_min_risk_efficiency": 0.05,
                "min_credit_to_width": 0.05,
                "max_short_bid_ask_width": 1.2,
                "realistic_max_loss_factor": 0.6,
                "fortress_base_width": 15.0,
                "fortress_base_contracts": 10,
                "fortress_neighborhood_range": 30.0,
                "fortress_allowed_widths": [10.0, 15.0, 20.0],
                "fortress_allowed_contracts": list(range(5, 21)),
                "fortress_max_loss_cap_dollars": 15000.0,
                "mode_max_loss_caps": {
                    "fortress": 0.10,
                    "standard": 0.15,
                    "aggressive": 0.20,
                },
            },
        ),
        "kairos": StrategyProfileSnapshot(
            profile_name="Kairos",
            version=DEFAULT_STRATEGY_VERSION,
            description="Canonical Kairos live/sim candidate thresholds for Delphi 8.0.7.",
            enabled=True,
            parameters={
                "timing_lock_release": "08:45",
                "timing_late_start": "11:45",
                "timing_end": "12:30",
                "live_scan_interval_seconds": 120,
                "candidate_account_risk_percent": 0.06,
                "candidate_min_credit_dollars": 60.0,
                "candidate_spread_width_points": 5,
                "live_spread_width_scan_points": [5, 10, 15],
                "candidate_min_distance_percent": 1.0,
                "hybrid_expected_move_multiple": 2.0,
                "candidate_max_short_delta": 0.15,
                "candidate_min_contracts": 1,
                "candidate_max_contracts": 10,
                "candidate_profiles": [
                    {
                        "key": "standard",
                        "label": "Standard",
                        "distance_multiplier": 1.00,
                        "credit_factor": 1.00,
                        "descriptor": "Balanced distance and credit assumptions.",
                    },
                    {
                        "key": "fortress",
                        "label": "Fortress",
                        "distance_multiplier": 1.20,
                        "credit_factor": 0.94,
                        "descriptor": "Wider buffer with lower modeled premium.",
                    },
                    {
                        "key": "aggressive",
                        "label": "Aggressive",
                        "distance_multiplier": 0.85,
                        "credit_factor": 1.06,
                        "descriptor": "Closer short strike with higher modeled premium.",
                    },
                ],
                "exit_gates": [
                    {"key": "structure-break", "label": "Structure Break", "fraction": 0.20, "summary": "Structure break detected"},
                    {"key": "below-vwap-failed-reclaim", "label": "Below VWAP / Failed Reclaim", "fraction": 0.20, "summary": "Below VWAP and failed reclaim"},
                    {"key": "short-strike-proximity", "label": "Within 15 Points of Short Strike", "fraction": 0.40, "summary": "Within 15 points of short strike"},
                    {"key": "long-strike-touch", "label": "Long Strike Touch", "fraction": 1.00, "summary": "Long strike touched"},
                ],
            },
        ),
    }

