"""Kairos Phase 2 watchtower session manager, classifier, and manual simulation engine."""

from __future__ import annotations

from copy import deepcopy
import json
import inspect
import logging
from math import ceil
from pathlib import Path
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from enum import Enum
from threading import RLock, Timer
from time import monotonic
from typing import Any, Callable, Dict, List, Optional
from zoneinfo import ZoneInfo

import pandas as pd

from config import AppConfig, HOSTED_APP_VERSION, get_app_config

from .kairos_simulation_runner import KAIROS_RUNNER_SCENARIOS, SESSION_BAR_COUNT, KairosTapeBar, KairosTapeScenario, validate_tape_scenario
from .kairos_scenario_repository import KairosScenarioRepository
from .market_calendar_service import MarketCalendarService
from .market_data import MarketDataAuthenticationError, MarketDataError, MarketDataReauthenticationRequired, MarketDataService
from .options_chain_service import OptionsChainService
from .pushover_service import PushoverService
from .repositories.scenario_repository import FileSystemKairosScenarioRepository, KairosBundleRepository
from .repositories.trade_repository import TradeRepository
from .runtime.notifications import NotificationDelivery, PushoverNotificationDelivery
from .runtime.scheduler import RuntimeJobHandle, RuntimeScheduler, ThreadingTimerScheduler


LOGGER = logging.getLogger(__name__)


class KairosMode(str, Enum):
    """Supported Kairos runtime modes."""

    LIVE = "Live"
    SIMULATION = "Simulation"


class KairosState(str, Enum):
    """Supported Kairos session states."""

    INACTIVE = "Inactive"
    ACTIVATED = "Activated"
    SCANNING = "Scanning"
    NOT_ELIGIBLE = "Not Eligible"
    WATCHING = "Watching"
    SETUP_FORMING = "Setup Forming"
    WINDOW_OPEN = "Window Open"
    WINDOW_CLOSING = "Window Closing"
    EXPIRED = "Expired"
    SESSION_COMPLETE = "Session Complete"
    STOPPED = "Stopped"


@dataclass
class KairosSimulationPreset:
    """Reusable simulation presets for fast Kairos testing."""

    name: str
    market_session_status: str
    spx_value: float
    vix_value: float
    timing_status: str
    structure_status: str
    momentum_status: str


@dataclass
class KairosRuntimeConfig:
    """Mutable runtime configuration for live and simulation control."""

    mode: KairosMode = KairosMode.LIVE
    simulated_market_session_status: str = "Open"
    simulated_spx_value: float = 6125.0
    simulated_vix_value: float = 19.4
    simulated_timing_status: str = "Eligible"
    simulated_structure_status: str = "Developing"
    simulated_momentum_status: str = "Improving"
    simulation_scan_interval_seconds: int = 15
    simulation_scenario_name: str = "Custom"


class KairosRunnerStatus(str, Enum):
    """Supported statuses for the scripted full-day simulation runner."""

    IDLE = "Idle"
    RUNNING = "Running"
    PAUSED = "Paused"
    COMPLETED = "Completed"
    ENDED = "Ended"


@dataclass
class KairosRunnerEvent:
    """Lightweight timeline entry for the scripted simulation runner."""

    timestamp: datetime
    simulated_time_label: str
    title: str
    detail: str

    def to_payload(self, timezone: ZoneInfo) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp.isoformat(),
            "timestamp_display": format_display_datetime(self.timestamp, timezone),
            "timestamp_short": format_display_time(self.timestamp, timezone),
            "simulated_time_label": self.simulated_time_label,
            "title": self.title,
            "detail": self.detail,
        }


@dataclass
class KairosSimulatedTradeLockIn:
    """Accepted simulated Kairos trade snapshot captured at decision time."""

    decision_timestamp: datetime
    scenario_name: str
    simulated_time_label: str
    bar_number: int
    current_spx: float
    current_vix: float
    kairos_state: str
    profile_label: str
    spread_type: str
    short_strike: int
    long_strike: int
    width: int
    daily_move_anchor: float
    distance_points: float
    distance_percent: float
    estimated_credit_per_contract: float
    estimated_total_premium_received: float
    estimated_max_loss: float
    estimated_short_delta: float
    estimated_otm_probability: float
    contracts: int
    rationale: str
    original_credit: float = 0.0
    current_credit: float = 0.0
    realized_close_cost_total: float = 0.0
    exit_events: List[Dict[str, Any]] = field(default_factory=list)
    pending_exit_recommendation: Dict[str, Any] | None = None
    entry_metadata: Dict[str, Any] = field(default_factory=dict)
    exit_metadata: Dict[str, Any] = field(default_factory=dict)
    final_spx_close: float | None = None
    result_status: str = "Open"
    simulated_pnl_dollars: float | None = None
    settlement_note: str = ""
    settlement_badge_label: str = "Locked In"
    result_tone: str = "open"
    remaining_contracts: int = 0
    partially_reduced: bool = False
    fully_closed: bool = False
    expired: bool = False
    closed_early: bool = False
    short_strike_touched_at: str | None = None
    long_strike_touched_at: str | None = None
    exit_status_summary: str = "Active"
    exit_gate_states: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    pending_exit_gate_key: str | None = None
    pending_exit_gate_contracts: int = 0
    pending_exit_gate_debit_per_contract: float = 0.0
    pending_exit_gate_total_debit: float = 0.0
    pending_exit_gate_note: str = ""
    realized_exit_debit_dollars: float = 0.0
    closed_contracts: int = 0
    current_exit_debit_per_contract: float = 0.0
    current_unrealized_close_cost: float = 0.0
    vwap_breach_bar_number: int | None = None
    exit_management_note: str = ""

    def to_payload(self, timezone: ZoneInfo) -> Dict[str, Any]:
        current_credit = round(self.current_credit or 0.0, 2)
        return {
            "decision_timestamp": self.decision_timestamp.isoformat(),
            "decision_timestamp_display": format_display_datetime(self.decision_timestamp, timezone),
            "scenario_name": self.scenario_name,
            "simulated_time_label": self.simulated_time_label,
            "bar_number": self.bar_number,
                "chain_time_label": self.simulated_time_label,
                "current_spx": round(self.current_spx, 2),
            "current_spx_display": format_number_value(self.current_spx),
            "current_vix": round(self.current_vix, 2),
            "current_vix_display": format_number_value(self.current_vix),
            "kairos_state": self.kairos_state,
            "profile_label": self.profile_label,
            "spread_type": self.spread_type,
            "short_strike": self.short_strike,
            "long_strike": self.long_strike,
            "width": self.width,
            "width_display": f"{self.width} pts",
            "daily_move_anchor": round(self.daily_move_anchor, 2),
            "daily_move_anchor_display": f"{self.daily_move_anchor:.2f} pts",
            "distance_points": round(self.distance_points, 2),
            "distance_points_display": f"{self.distance_points:.2f} pts",
            "distance_percent": round(self.distance_percent, 2),
            "distance_percent_display": f"{self.distance_percent:.2f}%",
            "estimated_credit_per_contract": round(self.estimated_credit_per_contract, 2),
            "estimated_credit_per_contract_display": format_currency_value(self.estimated_credit_per_contract),
            "estimated_total_premium_received": round(self.estimated_total_premium_received, 2),
            "estimated_total_premium_received_display": format_currency_value(self.estimated_total_premium_received),
            "estimated_max_loss": round(self.estimated_max_loss, 2),
            "estimated_max_loss_display": format_currency_value(self.estimated_max_loss),
            "estimated_short_delta": round(self.estimated_short_delta, 2),
            "estimated_short_delta_display": f"{self.estimated_short_delta:.2f}",
            "estimated_otm_probability": round(self.estimated_otm_probability, 2),
            "estimated_otm_probability_display": format_percent_value(self.estimated_otm_probability),
            "contracts": self.contracts,
            "original_credit": round(self.original_credit or self.estimated_total_premium_received, 2),
            "original_credit_display": format_currency_value(self.original_credit or self.estimated_total_premium_received),
            "current_credit": current_credit,
            "current_credit_display": format_currency_value(current_credit),
            "remaining_contracts": self.remaining_contracts,
            "closed_contracts": self.closed_contracts,
            "partially_reduced": self.partially_reduced,
            "fully_closed": self.fully_closed,
            "expired": self.expired,
            "closed_early": self.closed_early,
            "short_strike_touched_at": self.short_strike_touched_at,
            "long_strike_touched_at": self.long_strike_touched_at,
            "exit_status_summary": self.exit_status_summary,
            "exit_gate_states": [
                {
                    "gate_key": gate_key,
                    "label": gate_state.get("label", gate_key),
                    "status": gate_state.get("status", "pending"),
                    "status_display": str(gate_state.get("status", "pending")).replace("-", " ").title(),
                    "contracts": gate_state.get("contracts", 0),
                    "debit_display": format_currency_value(gate_state.get("total_debit", 0.0)),
                    "time_label": gate_state.get("time_label") or "—",
                    "note": gate_state.get("note") or "",
                }
                for gate_key, gate_state in self.exit_gate_states.items()
            ],
            "pending_exit_gate_key": self.pending_exit_gate_key,
            "pending_exit_gate_contracts": self.pending_exit_gate_contracts,
            "pending_exit_gate_debit_per_contract": round(self.pending_exit_gate_debit_per_contract, 2),
            "pending_exit_gate_debit_per_contract_display": format_currency_value(self.pending_exit_gate_debit_per_contract),
            "pending_exit_gate_total_debit": round(self.pending_exit_gate_total_debit, 2),
            "pending_exit_gate_total_debit_display": format_currency_value(self.pending_exit_gate_total_debit),
            "pending_exit_gate_note": self.pending_exit_gate_note,
            "pending_exit_recommendation": self.pending_exit_recommendation,
            "realized_exit_debit_dollars": round(self.realized_exit_debit_dollars, 2),
            "realized_exit_debit_display": format_currency_value(self.realized_exit_debit_dollars),
            "realized_close_cost_total": round(self.realized_close_cost_total or self.realized_exit_debit_dollars, 2),
            "realized_close_cost_total_display": format_currency_value(self.realized_close_cost_total or self.realized_exit_debit_dollars),
            "retained_credit_dollars": current_credit,
            "retained_credit_display": format_currency_value(current_credit),
            "current_exit_debit_per_contract": round(self.current_exit_debit_per_contract, 2),
            "current_exit_debit_per_contract_display": format_currency_value(self.current_exit_debit_per_contract),
            "current_unrealized_close_cost": round(self.current_unrealized_close_cost, 2),
            "current_unrealized_close_cost_display": format_currency_value(self.current_unrealized_close_cost),
            "exit_management_note": self.exit_management_note,
            "exit_events": [dict(item) for item in self.exit_events],
            "entry_metadata": dict(self.entry_metadata),
            "exit_metadata": dict(self.exit_metadata),
            "rationale": self.rationale,
            "is_settled": self.final_spx_close is not None,
            "final_spx_close": round(self.final_spx_close, 2) if self.final_spx_close is not None else None,
            "final_spx_close_display": format_number_value(self.final_spx_close) if self.final_spx_close is not None else "—",
            "result_status": self.result_status,
            "result_tone": self.result_tone,
            "simulated_pnl_dollars": round(self.simulated_pnl_dollars, 2) if self.simulated_pnl_dollars is not None else None,
            "simulated_pnl_display": format_currency_value(self.simulated_pnl_dollars) if self.simulated_pnl_dollars is not None else "—",
            "simulated_pnl_signed_whole_display": format_signed_currency_whole(self.simulated_pnl_dollars) if self.simulated_pnl_dollars is not None else "$0",
            "settlement_note": self.settlement_note,
            "settlement_badge_label": self.settlement_badge_label,
            "status": "Expired" if self.expired else "Fully Closed" if self.fully_closed else "Active",
        }


@dataclass
class KairosRunnerState:
    """Mutable state for the scripted full-day simulation runner."""

    status: KairosRunnerStatus = KairosRunnerStatus.IDLE
    scenario_key: Optional[str] = None
    run_mode: str = "Fast Run"
    current_bar_index: int = -1
    simulated_time_label: str = "—"
    current_step_label: str = "Tape idle"
    current_step_note: str = "Choose a scenario to begin the full-day simulator."
    pause_reason: Optional[str] = None
    pause_detail: str = ""
    pause_event_filters: List[str] = field(default_factory=list)
    pause_event_label: Optional[str] = None
    pause_bar_number: Optional[int] = None
    pause_simulated_time: Optional[str] = None
    candidate_event_id: Optional[str] = None
    selected_candidate_profile_key: Optional[str] = None
    last_candidate_action_note: str = ""
    suppressed_pause_event_kind: Optional[str] = None
    suppressed_pause_event_label: Optional[str] = None
    active_trade_lock_in: Optional[KairosSimulatedTradeLockIn] = None
    trade_lock_history: List[KairosSimulatedTradeLockIn] = field(default_factory=list)
    started_at: Optional[datetime] = None
    last_advanced_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    event_log: List[KairosRunnerEvent] = field(default_factory=list)
    best_trade_override_requested: bool = False
    pause_on_exit_gates: bool = False


def get_current_credit(trade_lock_in: KairosSimulatedTradeLockIn) -> float:
    """Return the remaining credit after all accepted close costs."""
    original_credit = trade_lock_in.original_credit or trade_lock_in.estimated_total_premium_received
    realized_close_cost = trade_lock_in.realized_close_cost_total or trade_lock_in.realized_exit_debit_dollars
    return round(max(0.0, original_credit - realized_close_cost), 2)


@dataclass(frozen=True)
class KairosHistoricalReplayTemplate:
    """Metadata for a real-day replay template imported into Kairos Sim."""

    scenario_key: str
    scenario_name: str
    session_date: date
    source_key: str
    source_label: str
    symbol: str
    tape_source: str
    vix_source_label: str
    description: str
    source_type: str = "real_import"
    source_family_tag: str = "REAL IMPORT"
    session_type: str = "Imported Real Day"
    session_status: str = "complete"
    created_at: str = ""
    bar_count: int = SESSION_BAR_COUNT
    market_session: str = "Regular"
    replay_schema_version: int = 2
    storage_version: int = 2
    session_summary: Dict[str, Any] = field(default_factory=dict)

    def to_payload(self) -> Dict[str, Any]:
        return {
            "scenario_key": self.scenario_key,
            "scenario_name": self.scenario_name,
            "session_date": self.session_date.isoformat(),
            "source_key": self.source_key,
            "source_label": self.source_label,
            "symbol": self.symbol,
            "tape_source": self.tape_source,
            "vix_source_label": self.vix_source_label,
            "description": self.description,
            "source_type": self.source_type,
            "source_family_tag": self.source_family_tag,
            "session_type": self.session_type,
            "session_status": self.session_status,
            "created_at": self.created_at,
            "bar_count": self.bar_count,
            "market_session": self.market_session,
            "replay_schema_version": self.replay_schema_version,
            "storage_version": self.storage_version,
            "session_summary": dict(self.session_summary),
        }


@dataclass
class KairosStateTransition:
    """State change metadata for UI display."""

    prior_state: Optional[str]
    current_state: str
    is_transition: bool
    label: str

    def to_payload(self) -> Dict[str, Any]:
        return {
            "prior_state": self.prior_state,
            "prior_state_key": slugify_label(self.prior_state or ""),
            "current_state": self.current_state,
            "current_state_key": slugify_label(self.current_state),
            "is_transition": self.is_transition,
            "label": self.label,
        }


@dataclass
class KairosScanResult:
    """Structured result for a single Kairos scan cycle."""

    timestamp: datetime
    scan_sequence_number: int
    mode: str
    kairos_state: str
    prior_kairos_state: Optional[str]
    vix_status: str
    timing_status: str
    structure_status: str
    momentum_status: str
    readiness_state: str
    summary_text: str
    reasons: List[str]
    state_transition: KairosStateTransition
    is_window_open: bool
    is_window_closing: bool
    is_expired: bool
    trigger_reason: str
    market_session_status: str
    spx_value: str = "—"
    vix_value: str = "—"
    simulation_scenario_name: Optional[str] = None
    classification_note: str = ""

    def to_payload(self, timezone: ZoneInfo, now: datetime) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp.isoformat(),
            "timestamp_display": format_display_datetime(self.timestamp, timezone),
            "timestamp_short": format_display_time(self.timestamp, timezone),
            "scan_sequence_number": self.scan_sequence_number,
            "mode": self.mode,
            "mode_key": slugify_label(self.mode),
            "is_simulated": self.mode == KairosMode.SIMULATION.value,
            "kairos_state": self.kairos_state,
            "kairos_state_key": slugify_label(self.kairos_state),
            "prior_kairos_state": self.prior_kairos_state,
            "prior_kairos_state_key": slugify_label(self.prior_kairos_state or ""),
            "vix_status": self.vix_status,
            "timing_status": self.timing_status,
            "structure_status": self.structure_status,
            "momentum_status": self.momentum_status,
            "readiness_state": self.readiness_state,
            "summary_text": self.summary_text,
            "reasons": list(self.reasons),
            "state_transition": self.state_transition.to_payload(),
            "transition_label": self.state_transition.label,
            "is_window_open": self.is_window_open,
            "is_window_closing": self.is_window_closing,
            "is_expired": self.is_expired,
            "trigger_reason": self.trigger_reason,
            "market_session_status": self.market_session_status,
            "spx_value": self.spx_value,
            "vix_value": self.vix_value,
            "simulation_scenario_name": self.simulation_scenario_name,
            "classification_note": self.classification_note,
            "seconds_ago": max(0, int((now - self.timestamp).total_seconds())),
        }


@dataclass
class KairosSessionRecord:
    """Mutable in-memory record of the current Kairos day session."""

    session_date: Optional[date] = None
    activated_at: Optional[datetime] = None
    last_scan_at: Optional[datetime] = None
    next_scan_at: Optional[datetime] = None
    current_state: str = KairosState.INACTIVE.value
    total_scans_completed: int = 0
    scan_engine_running: bool = False
    armed_for_day: bool = False
    stopped_manually: bool = False
    auto_ended: bool = False
    session_complete: bool = False
    window_found: bool = False
    latest_scan: Optional[KairosScanResult] = None
    scan_log: List[KairosScanResult] = field(default_factory=list)
    status_note: str = "Kairos is inactive for today."


class KairosService:
    """Manage Kairos activation, classification cadence, simulation state, and intraday logs."""

    MARKET_OPEN = time(8, 30)
    MARKET_CLOSE = time(15, 0)
    TIMING_LOCK_RELEASE = time(8, 45)
    TIMING_LATE_START = time(11, 45)
    TIMING_END = time(12, 30)
    LIVE_SCAN_INTERVAL_SECONDS = 120
    RUNNER_TICK_INTERVAL_SECONDS = 0.05
    RUNNER_FAST_BATCH_SIZE = 24
    RUNNER_EVENT_LOG_LIMIT = 24
    RUNNER_TAPE_SOURCE = "/ES 1m Tape"
    HISTORICAL_REPLAY_SYMBOL = "SPY"
    HISTORICAL_REPLAY_TARGET_SYMBOL = "^GSPC"
    HISTORICAL_REPLAY_SOURCE_OPTIONS = (
        {"label": "Auto (Schwab first)", "value": "auto"},
        {"label": "Schwab historical fetch", "value": "schwab"},
        {"label": "Polygon / Massive JSON", "value": "polygon-json"},
    )
    HISTORICAL_REPLAY_FILE_GLOB = "*.json"
    REPLAY_STORAGE_VERSION = 2
    REPLAY_SCHEMA_VERSION = 2
    LIVE_TAPE_SOURCE_TYPE = "live_spx_tape"
    LIVE_TAPE_SOURCE_FAMILY_TAG = "LIVE TAPE"
    LIVE_TAPE_SOURCE_LABEL = "Live SPX 1m"
    LIVE_TAPE_SESSION_TYPE = "Recorded Live Tape"
    REAL_IMPORT_SOURCE_TYPE = "real_import"
    REAL_IMPORT_SOURCE_FAMILY_TAG = "REAL IMPORT"
    SYNTHETIC_SOURCE_TYPE = "synthetic"
    SYNTHETIC_SOURCE_FAMILY_TAG = "SYNTHETIC"
    PARTIAL_TAPE_MIN_BAR_COUNT = 30
    KAIROS_CANDIDATE_ACCOUNT_RISK_PERCENT = 0.06
    KAIROS_CANDIDATE_MIN_CREDIT_DOLLARS = 60.0
    KAIROS_CANDIDATE_SPREAD_WIDTH_POINTS = 5
    LIVE_KAIROS_SPREAD_WIDTH_SCAN_POINTS = (5, 10, 15)
    KAIROS_CANDIDATE_MIN_DISTANCE_PERCENT = 1.0
    KAIROS_HYBRID_EXPECTED_MOVE_MULTIPLE = 2.0
    KAIROS_CANDIDATE_MAX_SHORT_DELTA = 0.15
    KAIROS_CANDIDATE_MIN_CONTRACTS = 1
    KAIROS_CANDIDATE_MAX_CONTRACTS = 10
    DEFAULT_ROUTINE_LOSS_PERCENTAGE = 0.28
    DEFAULT_BLACK_SWAN_LOSS_PERCENTAGE = 0.60
    KAIROS_SLOT_LABELS = {
        "best-available": "Best Candidate",
        "kairos-window": "Prime",
    }
    KAIROS_CANDIDATE_PROFILES = (
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
    )
    RUNNER_PAUSE_EVENT_OPTIONS = (
        {"value": "setup-forming", "label": "Pause on Subprime Improving", "state": KairosState.SETUP_FORMING.value},
        {"value": "window-open", "label": "Pause on Prime", "state": KairosState.WINDOW_OPEN.value},
        {"value": "window-closing", "label": "Pause on Subprime Weakening", "state": KairosState.WINDOW_CLOSING.value},
        {"value": "non-go", "label": "Pause on Expired / Not Eligible", "state": "non-go"},
    )
    SIMULATION_INTERVAL_OPTIONS = (10, 15, 30, 120)
    SIMULATION_MARKET_SESSION_OPTIONS = ("Open", "Closed")
    TIMING_STATUS_OPTIONS = ("Locked", "Eligible", "Late", "Closed")
    STRUCTURE_STATUS_OPTIONS = (
        "Bullish Confirmation",
        "Developing",
        "Chop / Unclear",
        "Weakening",
        "Failed",
    )
    MOMENTUM_STATUS_OPTIONS = ("Improving", "Steady", "Weakening", "Expired")
    PERSISTED_SCENARIO_SYNC_INTERVAL_SECONDS = 30.0
    KAIROS_EXIT_GATES = (
        {
            "key": "structure-break",
            "label": "Structure Break",
            "fraction": 0.20,
            "summary": "Structure break detected",
        },
        {
            "key": "below-vwap-failed-reclaim",
            "label": "Below VWAP / Failed Reclaim",
            "fraction": 0.20,
            "summary": "Below VWAP and failed reclaim",
        },
        {
            "key": "short-strike-proximity",
            "label": "Within 15 Points of Short Strike",
            "fraction": 0.40,
            "summary": "Within 15 points of short strike",
        },
        {
            "key": "long-strike-touch",
            "label": "Long Strike Touch",
            "fraction": 1.00,
            "summary": "Long strike touched",
        },
    )
    ACTIVE_STATES = {
        KairosState.WATCHING.value,
        KairosState.SETUP_FORMING.value,
        KairosState.WINDOW_OPEN.value,
        KairosState.WINDOW_CLOSING.value,
    }
    SIMULATION_PRESETS = {
        "Pre-8:45 Waiting": KairosSimulationPreset(
            name="Pre-8:45 Waiting",
            market_session_status="Open",
            spx_value=6118.0,
            vix_value=19.2,
            timing_status="Locked",
            structure_status="Developing",
            momentum_status="Steady",
        ),
        "Favorable Subprime Improving": KairosSimulationPreset(
            name="Favorable Subprime Improving",
            market_session_status="Open",
            spx_value=6127.0,
            vix_value=19.6,
            timing_status="Eligible",
            structure_status="Developing",
            momentum_status="Improving",
        ),
        "Prime": KairosSimulationPreset(
            name="Prime",
            market_session_status="Open",
            spx_value=6132.0,
            vix_value=20.1,
            timing_status="Eligible",
            structure_status="Bullish Confirmation",
            momentum_status="Improving",
        ),
        "Subprime Weakening": KairosSimulationPreset(
            name="Subprime Weakening",
            market_session_status="Open",
            spx_value=6120.0,
            vix_value=18.6,
            timing_status="Late",
            structure_status="Weakening",
            momentum_status="Weakening",
        ),
        "Not Eligible Low VIX": KairosSimulationPreset(
            name="Not Eligible Low VIX",
            market_session_status="Open",
            spx_value=6112.0,
            vix_value=17.3,
            timing_status="Eligible",
            structure_status="Chop / Unclear",
            momentum_status="Steady",
        ),
        "Structure Failed": KairosSimulationPreset(
            name="Structure Failed",
            market_session_status="Open",
            spx_value=6096.0,
            vix_value=19.7,
            timing_status="Eligible",
            structure_status="Failed",
            momentum_status="Expired",
        ),
        "Session Expired": KairosSimulationPreset(
            name="Session Expired",
            market_session_status="Closed",
            spx_value=6101.0,
            vix_value=19.0,
            timing_status="Closed",
            structure_status="Weakening",
            momentum_status="Expired",
        ),
    }
    SIMULATION_PRESET_ALIASES = {
        "Favorable Setup Forming": "Favorable Subprime Improving",
        "Window Open": "Prime",
        "Window Closing": "Subprime Weakening",
    }

    def __init__(
        self,
        market_data_service: MarketDataService,
        market_calendar_service: MarketCalendarService | None = None,
        options_chain_service: OptionsChainService | None = None,
        pushover_service: PushoverService | None = None,
        *,
        config: AppConfig | None = None,
        replay_storage_dir: str | Path | None = None,
        scenario_repository: KairosBundleRepository | None = None,
        trade_store: TradeRepository | None = None,
        scan_interval_seconds: int | None = None,
        strategy_parameters: Dict[str, Any] | None = None,
        timer_factory: Callable[..., Timer] | None = None,
        scheduler: RuntimeScheduler | None = None,
        now_provider: Callable[[], datetime] | None = None,
    ) -> None:
        self.config = config or get_app_config()
        self.display_timezone = ZoneInfo(self.config.app_timezone)
        self.market_data_service = market_data_service
        self.market_calendar_service = market_calendar_service or MarketCalendarService(self.config)
        shared_live_provider = getattr(self.market_data_service, "live_provider", None)
        self.options_chain_service = options_chain_service or OptionsChainService(
            self.config,
            provider=shared_live_provider,
        )
        self.pushover_service = pushover_service
        self.notification_delivery = (
            PushoverNotificationDelivery(pushover_service) if isinstance(pushover_service, PushoverService) else pushover_service
        )
        parameters = strategy_parameters or {}
        self.TIMING_LOCK_RELEASE = self._resolve_strategy_time(parameters.get("timing_lock_release"), self.TIMING_LOCK_RELEASE)
        self.TIMING_LATE_START = self._resolve_strategy_time(parameters.get("timing_late_start"), self.TIMING_LATE_START)
        self.TIMING_END = self._resolve_strategy_time(parameters.get("timing_end"), self.TIMING_END)
        self.LIVE_SCAN_INTERVAL_SECONDS = int(parameters.get("live_scan_interval_seconds") or self.LIVE_SCAN_INTERVAL_SECONDS)
        self.KAIROS_CANDIDATE_ACCOUNT_RISK_PERCENT = float(parameters.get("candidate_account_risk_percent") or self.KAIROS_CANDIDATE_ACCOUNT_RISK_PERCENT)
        self.KAIROS_CANDIDATE_MIN_CREDIT_DOLLARS = float(parameters.get("candidate_min_credit_dollars") or self.KAIROS_CANDIDATE_MIN_CREDIT_DOLLARS)
        self.KAIROS_CANDIDATE_SPREAD_WIDTH_POINTS = int(parameters.get("candidate_spread_width_points") or self.KAIROS_CANDIDATE_SPREAD_WIDTH_POINTS)
        self.LIVE_KAIROS_SPREAD_WIDTH_SCAN_POINTS = tuple(int(item) for item in (parameters.get("live_spread_width_scan_points") or self.LIVE_KAIROS_SPREAD_WIDTH_SCAN_POINTS))
        self.KAIROS_CANDIDATE_MIN_DISTANCE_PERCENT = float(parameters.get("candidate_min_distance_percent") or self.KAIROS_CANDIDATE_MIN_DISTANCE_PERCENT)
        self.KAIROS_HYBRID_EXPECTED_MOVE_MULTIPLE = float(parameters.get("hybrid_expected_move_multiple") or self.KAIROS_HYBRID_EXPECTED_MOVE_MULTIPLE)
        self.KAIROS_CANDIDATE_MAX_SHORT_DELTA = float(parameters.get("candidate_max_short_delta") or self.KAIROS_CANDIDATE_MAX_SHORT_DELTA)
        self.KAIROS_CANDIDATE_MIN_CONTRACTS = int(parameters.get("candidate_min_contracts") or self.KAIROS_CANDIDATE_MIN_CONTRACTS)
        self.KAIROS_CANDIDATE_MAX_CONTRACTS = int(parameters.get("candidate_max_contracts") or self.KAIROS_CANDIDATE_MAX_CONTRACTS)
        self.KAIROS_CANDIDATE_PROFILES = tuple(dict(item) for item in (parameters.get("candidate_profiles") or self.KAIROS_CANDIDATE_PROFILES))
        self.KAIROS_EXIT_GATES = tuple(dict(item) for item in (parameters.get("exit_gates") or self.KAIROS_EXIT_GATES))
        self.live_scan_interval_seconds = int(scan_interval_seconds) if scan_interval_seconds is not None else self.LIVE_SCAN_INTERVAL_SECONDS
        self.timer_factory = timer_factory or Timer
        self.scheduler = scheduler
        self.now_provider = now_provider or (lambda: datetime.now(self.display_timezone))
        self._lock = RLock()
        self._timer: RuntimeJobHandle | None = None
        self._runner_timer: RuntimeJobHandle | None = None
        self._session = KairosSessionRecord()
        self._runtime = KairosRuntimeConfig()
        self._runner = KairosRunnerState()
        self._runner_scenario: KairosTapeScenario | None = None
        self._historical_replay_scenarios: Dict[str, KairosTapeScenario] = {}
        self._historical_replay_templates: Dict[str, KairosHistoricalReplayTemplate] = {}
        self._historical_replay_catalog: Dict[str, Dict[str, Any]] = {}
        self._persisted_scenarios_dirty = False
        self._persisted_scenarios_last_synced_at: float | None = None
        self._scenario_repository_write_count_at_last_sync: int = 0
        self._cached_runner_candidate_context_key: tuple[Any, ...] | None = None
        self._cached_runner_candidate_context: Dict[str, Any] | None = None
        self._live_best_trade_override_requested = False
        self._live_active_trade: KairosSimulatedTradeLockIn | None = None
        self._live_trade_history: List[KairosSimulatedTradeLockIn] = []
        self._replay_storage_dir = Path(replay_storage_dir) if replay_storage_dir is not None else None
        self._scenario_repository = scenario_repository or (
            FileSystemKairosScenarioRepository(KairosScenarioRepository(self._replay_storage_dir))
            if self._replay_storage_dir is not None
            else None
        )
        self.trade_store = trade_store
        self._load_persisted_kairos_scenarios()

    @staticmethod
    def _resolve_strategy_time(value: Any, fallback: time) -> time:
        raw = str(value or "").strip()
        if not raw:
            return fallback
        for fmt in ("%H:%M", "%H:%M:%S"):
            try:
                return datetime.strptime(raw, fmt).time()
            except ValueError:
                continue
        try:
            return time.fromisoformat(raw)
        except ValueError:
            return fallback

    def _build_real_trade_outcome_profile(self) -> Dict[str, Any]:
        if self.trade_store is None:
            return {
                "routine_loss_percentage": self.DEFAULT_ROUTINE_LOSS_PERCENTAGE,
                "black_swan_loss_percentage": self.DEFAULT_BLACK_SWAN_LOSS_PERCENTAGE,
                "loss_count": 0,
                "black_swan_count": 0,
            }
        profile = self.trade_store.build_real_trade_outcome_profile()
        return {
            "routine_loss_percentage": float(profile.get("routine_loss_percentage") or self.DEFAULT_ROUTINE_LOSS_PERCENTAGE),
            "black_swan_loss_percentage": float(profile.get("black_swan_loss_percentage") or self.DEFAULT_BLACK_SWAN_LOSS_PERCENTAGE),
            "loss_count": int(profile.get("loss_count") or 0),
            "black_swan_count": int(profile.get("black_swan_count") or 0),
        }

    def _load_persisted_kairos_scenarios(self) -> None:
        repository = self._scenario_repository
        if repository is None:
            return

        self._historical_replay_scenarios = {}
        self._historical_replay_templates = {}
        self._historical_replay_catalog = {}
        for item in repository.list_catalog_entries():
            scenario_key = str(item.get("scenario_key") or "").strip().lower()
            if scenario_key:
                self._historical_replay_catalog[scenario_key] = dict(item)
        for path, payload in repository.load_bundle_payloads():
            payload, updated = self._normalize_persisted_live_tape_payload(payload)
            if updated:
                scenario_key = str((payload.get("template") or {}).get("scenario_key") or path.stem)
                repository.save_bundle(scenario_key, payload)
            try:
                scenario, template = self._load_historical_replay_bundle_payload(payload)
            except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
                continue
            if template.session_status not in {"complete", "recovered"}:
                continue
            self._register_persisted_replay_scenario(scenario, template)
        self._persisted_scenarios_dirty = False
        self._persisted_scenarios_last_synced_at = monotonic()
        self._scenario_repository_write_count_at_last_sync = getattr(self._scenario_repository, "write_count", 0)

    def _sync_persisted_scenarios_locked(self, *, force: bool = False) -> None:
        if not force and not self._persisted_scenarios_dirty and self._persisted_scenarios_last_synced_at is not None:
            repo_write_count = getattr(self._scenario_repository, "write_count", 0)
            if repo_write_count == self._scenario_repository_write_count_at_last_sync:
                if (monotonic() - self._persisted_scenarios_last_synced_at) < self.PERSISTED_SCENARIO_SYNC_INTERVAL_SECONDS:
                    return
        self._load_persisted_kairos_scenarios()

    def _mark_persisted_scenarios_dirty_locked(self) -> None:
        self._persisted_scenarios_dirty = True
        self._persisted_scenarios_last_synced_at = None

    def _normalize_persisted_live_tape_payload(self, payload: Dict[str, Any]) -> tuple[Dict[str, Any], bool]:
        template_payload = payload.get("template") or {}
        scenario_payload = payload.get("scenario") or {}
        if not isinstance(template_payload, dict) or not isinstance(scenario_payload, dict):
            return payload, False
        if str(template_payload.get("source_type") or "") != self.LIVE_TAPE_SOURCE_TYPE:
            return payload, False

        bars = scenario_payload.get("bars") or []
        bar_count = int(template_payload.get("bar_count") or len(bars))
        session_status = str(template_payload.get("session_status") or "").strip().lower()
        if bar_count < SESSION_BAR_COUNT or session_status in {"complete", "recovered"}:
            return payload, False

        normalized_payload = dict(payload)
        normalized_template = dict(template_payload)
        normalized_template["bar_count"] = bar_count
        normalized_template["session_status"] = "complete"
        normalized_payload["template"] = normalized_template
        return normalized_payload, True

    def _persist_historical_replay_locked(self, scenario: KairosTapeScenario, template: KairosHistoricalReplayTemplate) -> bool:
        return self._persist_replay_bundle_payload_locked(
            scenario.key,
            self._build_replay_bundle_payload(
                scenario_key=scenario.key,
                scenario_name=scenario.name,
                description=scenario.description,
                bars=[
                    {
                        "bar_index": bar.bar_index,
                        "minute_offset": bar.minute_offset,
                        "simulated_time": bar.simulated_time.isoformat(),
                        "open": bar.open,
                        "high": bar.high,
                        "low": bar.low,
                        "close": bar.close,
                        "volume": bar.volume,
                    }
                    for bar in scenario.bars
                ],
                vix_series=list(scenario.vix_series),
                template=template,
            ),
        )

    def _persist_replay_bundle_payload_locked(self, scenario_key: str, payload: Dict[str, Any]) -> bool:
        repository = self._scenario_repository
        if repository is None:
            return False
        saved = repository.save_bundle(scenario_key, payload)
        if saved:
            self._mark_persisted_scenarios_dirty_locked()
        return saved

    def _build_replay_bundle_payload(
        self,
        *,
        scenario_key: str,
        scenario_name: str,
        description: str,
        bars: List[Dict[str, Any]],
        vix_series: List[float],
        template: KairosHistoricalReplayTemplate,
    ) -> Dict[str, Any]:
        return {
            "template": template.to_payload(),
            "scenario": {
                "key": scenario_key,
                "name": scenario_name,
                "description": description,
                "bars": bars,
                "vix_series": list(vix_series),
            },
        }

    def _load_historical_replay_bundle(self, path: Path) -> tuple[KairosTapeScenario, KairosHistoricalReplayTemplate]:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return self._load_historical_replay_bundle_payload(payload)

    def _load_historical_replay_bundle_payload(self, payload: Dict[str, Any]) -> tuple[KairosTapeScenario, KairosHistoricalReplayTemplate]:
        template_payload = payload["template"]
        scenario_payload = payload["scenario"]
        scenario = KairosTapeScenario(
            key=str(scenario_payload["key"]),
            name=str(scenario_payload["name"]),
            description=str(scenario_payload["description"]),
            bars=tuple(
                KairosTapeBar(
                    bar_index=int(bar_payload["bar_index"]),
                    minute_offset=int(bar_payload["minute_offset"]),
                    simulated_time=time.fromisoformat(str(bar_payload["simulated_time"])),
                    open=float(bar_payload["open"]),
                    high=float(bar_payload["high"]),
                    low=float(bar_payload["low"]),
                    close=float(bar_payload["close"]),
                    volume=int(bar_payload["volume"]),
                )
                for bar_payload in scenario_payload["bars"]
            ),
            vix_series=tuple(float(value) for value in scenario_payload["vix_series"]),
        )
        validate_tape_scenario(scenario)
        template = KairosHistoricalReplayTemplate(
            scenario_key=str(template_payload["scenario_key"]),
            scenario_name=str(template_payload["scenario_name"]),
            session_date=date.fromisoformat(str(template_payload["session_date"])),
            source_key=str(template_payload["source_key"]),
            source_label=str(template_payload["source_label"]),
            symbol=str(template_payload["symbol"]),
            tape_source=str(template_payload["tape_source"]),
            vix_source_label=str(template_payload["vix_source_label"]),
            description=str(template_payload["description"]),
            source_type=str(template_payload.get("source_type") or self.REAL_IMPORT_SOURCE_TYPE),
            source_family_tag=str(template_payload.get("source_family_tag") or self.REAL_IMPORT_SOURCE_FAMILY_TAG),
            session_type=str(template_payload.get("session_type") or "Imported Real Day"),
            session_status=str(template_payload.get("session_status") or "complete"),
            created_at=str(template_payload.get("created_at") or ""),
            bar_count=int(template_payload.get("bar_count") or len(scenario.bars)),
            market_session=str(template_payload.get("market_session") or "Regular"),
            replay_schema_version=int(template_payload.get("replay_schema_version") or 1),
            storage_version=int(template_payload.get("storage_version") or 1),
            session_summary=dict(template_payload.get("session_summary") or {}),
        )
        return scenario, template

    def _register_persisted_replay_scenario(self, scenario: KairosTapeScenario, template: KairosHistoricalReplayTemplate) -> None:
        self._historical_replay_scenarios[scenario.key] = scenario
        self._historical_replay_templates[template.scenario_key] = template
        self._historical_replay_catalog[template.scenario_key] = {
            "id": template.scenario_key,
            "scenario_key": template.scenario_key,
            "session_date": template.session_date.isoformat(),
            "label": template.scenario_name,
            "source": template.source_label,
            "source_family_tag": template.source_family_tag,
            "source_type": template.source_type,
            "created_at": template.created_at,
            "session_status": template.session_status,
            "bar_count": template.bar_count,
        }

    @staticmethod
    def _build_live_tape_scenario_key(session_date: date) -> str:
        return f"live-spx-tape-{session_date.isoformat()}"

    def _extract_live_tape_vix_anchor_locked(self, session_date: date, now: datetime) -> float:
        relevant_scan = next(
            (
                scan for scan in self._session.scan_log
                if scan.mode == KairosMode.LIVE.value and scan.timestamp.astimezone(self.display_timezone).date() == session_date
            ),
            None,
        )
        if relevant_scan is not None:
            vix_value = self._coerce_float(str(relevant_scan.vix_value).replace(",", ""), fallback=0.0)
            if vix_value > 0:
                return round(vix_value, 2)
        vix_snapshot = self._safe_latest_snapshot("^VIX", query_type="kairos_live_tape_vix")
        latest_vix = coerce_snapshot_number(vix_snapshot, "Latest Value")
        if latest_vix is not None and latest_vix > 0:
            return round(latest_vix, 2)
        return round(max(0.0, self._runtime.simulated_vix_value), 2)

    def _summarize_session_metadata(
        self,
        bar_payloads: List[Dict[str, Any]],
        *,
        latest_scan: KairosScanResult | None,
        vix_open: float | None,
    ) -> Dict[str, Any]:
        if not bar_payloads:
            return {}

        open_spx = round(float(bar_payloads[0].get("open") or 0.0), 2)
        close_spx = round(float(bar_payloads[-1].get("close") or 0.0), 2)
        session_high = round(max(float(item.get("high") or 0.0) for item in bar_payloads), 2)
        session_low = round(min(float(item.get("low") or 0.0) for item in bar_payloads), 2)
        session_range = round(session_high - session_low, 2)
        net_change = close_spx - open_spx
        trend_type = "Chop"
        if abs(net_change) >= max(6.0, session_range * 0.2):
            trend_type = "Up" if net_change > 0 else "Down"
        return {
            "opening_spx": open_spx,
            "closing_spx": close_spx,
            "session_high": session_high,
            "session_low": session_low,
            "total_range_points": session_range,
            "trend_type": trend_type,
            "final_structure_status": latest_scan.structure_status if latest_scan is not None else "",
            "vix_open": round(float(vix_open), 2) if vix_open is not None else None,
            "window_found": bool(self._session.window_found),
        }

    def _build_live_tape_template_locked(
        self,
        *,
        session_date: date,
        bar_payloads: List[Dict[str, Any]],
        session_status: str,
        created_at: datetime,
        session_summary: Dict[str, Any],
    ) -> KairosHistoricalReplayTemplate:
        return KairosHistoricalReplayTemplate(
            scenario_key=self._build_live_tape_scenario_key(session_date),
            scenario_name=f"Live SPX Tape {session_date.isoformat()}",
            session_date=session_date,
            source_key=self.LIVE_TAPE_SOURCE_TYPE,
            source_label=self.LIVE_TAPE_SOURCE_LABEL,
            symbol="^GSPC",
            tape_source=self.LIVE_TAPE_SOURCE_LABEL,
            vix_source_label="Kairos first live scan / latest snapshot",
            description=f"Recorded live SPX 1-minute session tape for {session_date.isoformat()}.",
            source_type=self.LIVE_TAPE_SOURCE_TYPE,
            source_family_tag=self.LIVE_TAPE_SOURCE_FAMILY_TAG,
            session_type=self.LIVE_TAPE_SESSION_TYPE,
            session_status=session_status,
            created_at=created_at.isoformat(),
            bar_count=len(bar_payloads),
            market_session="Regular",
            replay_schema_version=self.REPLAY_SCHEMA_VERSION,
            storage_version=self.REPLAY_STORAGE_VERSION,
            session_summary=session_summary,
        )

    def _persist_completed_live_tape_locked(self, now: datetime, *, session_date: date | None = None, session_status: str = "complete") -> bool:
        repository = self._scenario_repository
        if repository is None:
            return False

        target_date = session_date or self._session.session_date or now.date()
        bar_payloads = self._build_live_bar_map_bars_locked(session_date=target_date, query_type="kairos_live_tape_persist")
        if not bar_payloads:
            return False
        if session_status == "partial":
            if len(bar_payloads) >= SESSION_BAR_COUNT:
                session_status = "complete"
            elif len(bar_payloads) < self.PARTIAL_TAPE_MIN_BAR_COUNT:
                return False

        vix_anchor = self._extract_live_tape_vix_anchor_locked(target_date, now)
        session_summary = self._summarize_session_metadata(bar_payloads, latest_scan=self._session.latest_scan, vix_open=vix_anchor)
        template = self._build_live_tape_template_locked(
            session_date=target_date,
            bar_payloads=bar_payloads,
            session_status=session_status,
            created_at=now,
            session_summary=session_summary,
        )
        bundle_payload = self._build_replay_bundle_payload(
            scenario_key=template.scenario_key,
            scenario_name=template.scenario_name,
            description=template.description,
            bars=[
                {
                    "bar_index": int(item.get("bar_index") or index),
                    "minute_offset": int(item.get("bar_index") or index),
                    "simulated_time": self._coerce_historical_replay_timestamp(item.get("timestamp_iso") or "").astimezone(self.display_timezone).time().isoformat()
                    if item.get("timestamp_iso")
                    else datetime.combine(target_date, self.MARKET_OPEN, tzinfo=self.display_timezone).time().isoformat(),
                    "open": float(item.get("open") or 0.0),
                    "high": float(item.get("high") or 0.0),
                    "low": float(item.get("low") or 0.0),
                    "close": float(item.get("close") or 0.0),
                    "volume": int(item.get("volume") or 0),
                }
                for index, item in enumerate(bar_payloads)
            ],
            vix_series=[vix_anchor for _ in bar_payloads],
            template=template,
        )
        self._persist_replay_bundle_payload_locked(template.scenario_key, bundle_payload)
        if session_status in {"complete", "recovered"} and len(bar_payloads) >= SESSION_BAR_COUNT:
            scenario, loaded_template = self._load_historical_replay_bundle_payload(bundle_payload)
            self._register_persisted_replay_scenario(scenario, loaded_template)
        else:
            self._historical_replay_catalog[template.scenario_key] = {
                "id": template.scenario_key,
                "scenario_key": template.scenario_key,
                "session_date": template.session_date.isoformat(),
                "label": template.scenario_name,
                "source": template.source_label,
                "source_family_tag": template.source_family_tag,
                "source_type": template.source_type,
                "created_at": template.created_at,
                "session_status": template.session_status,
                "bar_count": template.bar_count,
            }
        return True

    def _recover_or_finalize_previous_live_tape_locked(self, now: datetime) -> None:
        repository = self._scenario_repository
        if repository is None:
            return

        for _, payload in repository.load_bundle_payloads():
            template_payload = payload.get("template") or {}
            if str(template_payload.get("source_type") or "") != self.LIVE_TAPE_SOURCE_TYPE:
                continue
            if str(template_payload.get("session_status") or "") != "partial":
                continue
            raw_session_date = str(template_payload.get("session_date") or "")
            if not raw_session_date:
                continue
            try:
                session_date = date.fromisoformat(raw_session_date)
            except ValueError:
                continue
            if session_date >= now.date():
                continue
            self._persist_completed_live_tape_locked(now, session_date=session_date, session_status="recovered")

    def shutdown(self) -> None:
        """Stop any active timer so tests or app shutdown can exit cleanly."""
        with self._lock:
            if self._runtime.mode == KairosMode.LIVE and self._session.session_date is not None:
                self._persist_completed_live_tape_locked(self._now(), session_date=self._session.session_date, session_status="partial")
            self._cancel_timer_locked()
            self._cancel_runner_timer_locked()
            self._session.scan_engine_running = False
            self._runner_scenario = None

    def request_best_trade_override(self) -> Dict[str, Any]:
        """Request the best available Kairos trade candidate outside normal timing/state gating."""
        now = self._now()
        with self._lock:
            self._refresh_session_locked(now)
            if self._runtime.mode == KairosMode.SIMULATION:
                if not self._can_request_sim_best_trade_override_locked():
                    self._session.status_note = self._sim_best_trade_override_disabled_reason_locked()
                    return self._build_payload_locked(now)
                self._runner.best_trade_override_requested = True
                self._runner.last_candidate_action_note = "Best Trade Override generated for the current paused replay moment."
                self._session.status_note = self._runner.last_candidate_action_note
                return self._build_payload_locked(now)

            if self._session.session_date is None:
                self._session.session_date = now.date()
            self._session.status_note = "Best candidate refreshed from the current Schwab chain snapshot."
            return self._build_payload_locked(now)

    def open_live_trade(self, payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
        """Open a live Kairos trade from the current advisory candidate."""
        payload = payload or {}
        now = self._now()
        with self._lock:
            self._refresh_session_locked(now)
            if self._runtime.mode != KairosMode.LIVE:
                self._session.status_note = "Live trade management is only available in Kairos Live."
                return self._build_payload_locked(now)
            if self._live_active_trade is not None and self._live_active_trade.remaining_contracts > 0:
                self._session.status_note = "A live Kairos trade is already active. Manage or close it before opening another one."
                return self._build_payload_locked(now)

            latest_scan = self._session.latest_scan
            candidate_context = self._build_live_trade_candidate_context_locked(now, latest_scan)
            if candidate_context is None:
                self._session.status_note = "No live Kairos candidate is available because SPX or VIX data could not be resolved."
                return self._build_payload_locked(now)
            if not candidate_context.get("chain_success", True):
                self._session.status_note = candidate_context.get("chain_message") or "No live Kairos candidate is available because the same-day Schwab option chain is unavailable."
                return self._build_payload_locked(now)

            requested_key = str(payload.get("profile_key") or "").strip().lower()
            selected_candidate = None
            if requested_key:
                selected_candidate = next(
                    (item for item in candidate_context["profiles"] if item.get("available") and item["mode_key"] == requested_key),
                    None,
                )
            if selected_candidate is None:
                selected_candidate = self._select_best_trade_candidate_profile(candidate_context["profiles"])
            if selected_candidate is None:
                self._session.status_note = "No qualified live Kairos candidate is available to open right now."
                return self._build_payload_locked(now)

            self._live_active_trade = self._build_managed_trade_from_candidate_locked(
                decision_timestamp=now,
                candidate_context=candidate_context,
                selected_candidate=selected_candidate,
                source_label="Live",
            )
            self._live_trade_history.insert(0, self._live_active_trade)
            self._session.status_note = f"Live {selected_candidate['mode_label']} trade opened at {candidate_context['simulated_time_label']}. Journal draft is ready for review."
            payload = self._build_payload_locked(now)
            payload["journal_prefill"] = self.build_journal_prefill_for_live_trade(self._live_active_trade)
            return payload

    def accept_live_exit_recommendation(self) -> Dict[str, Any]:
        """Accept the pending live Kairos exit recommendation."""
        now = self._now()
        with self._lock:
            self._refresh_session_locked(now)
            trade_lock_in = self._live_active_trade
            if trade_lock_in is None or trade_lock_in.pending_exit_recommendation is None:
                self._session.status_note = "No pending live Kairos exit recommendation is available to accept."
                return self._build_payload_locked(now)

            current_spx, current_vwap, current_time_label, structure_status, time_remaining_ratio, bar_number = self._build_live_trade_management_context_locked(now)
            recommendation = dict(trade_lock_in.pending_exit_recommendation)
            self.apply_trade_partial_close(
                trade_lock_in,
                recommendation=recommendation,
                current_time_label=current_time_label,
                current_spx=current_spx,
            )
            gate_key = recommendation.get("gate_key") or ""
            gate_state = trade_lock_in.exit_gate_states.get(gate_key, {})
            gate_state.update(
                {
                    "status": "accepted",
                    "contracts": recommendation.get("contracts_to_close", 0),
                    "time_label": current_time_label,
                    "note": recommendation.get("note") or trade_lock_in.pending_exit_gate_note,
                    "total_debit": recommendation.get("estimated_total_debit", 0.0),
                }
            )
            trade_lock_in.exit_status_summary = gate_state.get("label", "Exit Managed")
            self._clear_pending_exit_gate_locked(trade_lock_in)

            payload = self._build_payload_locked(now)
            if trade_lock_in.fully_closed:
                self._session.status_note = f"Live Kairos exit accepted: {gate_state.get('label', 'Kairos exit')}. The trade is fully closed."
                self._clear_live_trade_state_locked()
                payload = self._build_payload_locked(now)
            else:
                self._session.status_note = f"Live Kairos exit accepted: {gate_state.get('label', 'Kairos exit')}."
            return payload

    def configure_runtime(self, payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
        """Update Kairos live or simulation controls."""
        payload = payload or {}
        now = self._now()
        mode_changed = False
        rerun_scan = False

        with self._lock:
            self._refresh_session_locked(now)
            original_mode = self._runtime.mode
            preset_name = str(payload.get("preset_name") or "").strip()
            manual_simulation_change = False
            if preset_name:
                manual_simulation_change = True
                self._apply_simulation_preset_locked(preset_name)

            mode_value = payload.get("mode")
            if mode_value is not None:
                self._runtime.mode = self._coerce_mode(mode_value)
                mode_changed = self._runtime.mode != original_mode

            if payload.get("simulation_market_session_status") is not None:
                manual_simulation_change = True
                self._runtime.simulated_market_session_status = self._coerce_choice(
                    payload.get("simulation_market_session_status"),
                    self.SIMULATION_MARKET_SESSION_OPTIONS,
                )
            if payload.get("simulated_spx_value") is not None:
                manual_simulation_change = True
                self._runtime.simulated_spx_value = self._coerce_float(
                    payload.get("simulated_spx_value"),
                    fallback=self._runtime.simulated_spx_value,
                )
            if payload.get("simulated_vix_value") is not None:
                manual_simulation_change = True
                self._runtime.simulated_vix_value = self._coerce_float(
                    payload.get("simulated_vix_value"),
                    fallback=self._runtime.simulated_vix_value,
                )
            if payload.get("simulation_timing_status") is not None:
                manual_simulation_change = True
                self._runtime.simulated_timing_status = self._coerce_choice(
                    payload.get("simulation_timing_status"),
                    self.TIMING_STATUS_OPTIONS,
                )
            if payload.get("simulation_structure_status") is not None:
                manual_simulation_change = True
                self._runtime.simulated_structure_status = self._coerce_choice(
                    payload.get("simulation_structure_status"),
                    self.STRUCTURE_STATUS_OPTIONS,
                )
            if payload.get("simulation_momentum_status") is not None:
                manual_simulation_change = True
                self._runtime.simulated_momentum_status = self._coerce_choice(
                    payload.get("simulation_momentum_status"),
                    self.MOMENTUM_STATUS_OPTIONS,
                )
            if payload.get("simulation_scan_interval_seconds") is not None:
                manual_simulation_change = True
                self._runtime.simulation_scan_interval_seconds = self._coerce_interval(
                    payload.get("simulation_scan_interval_seconds")
                )

            if preset_name:
                self._runtime.simulation_scenario_name = preset_name
            elif any(
                payload.get(key) is not None
                for key in (
                    "simulation_market_session_status",
                    "simulated_spx_value",
                    "simulated_vix_value",
                    "simulation_timing_status",
                    "simulation_structure_status",
                    "simulation_momentum_status",
                )
            ):
                self._runtime.simulation_scenario_name = "Custom"

            if mode_changed:
                self._end_runner_locked(
                    now,
                    note=f"Kairos switched to {self._runtime.mode.value} Mode. The full-day simulation runner was stopped.",
                    keep_log=False,
                )
                self._cancel_timer_locked()
                if self._runtime.mode == KairosMode.SIMULATION:
                    self._clear_live_trade_state_locked()
                self._session = KairosSessionRecord(
                    session_date=now.date(),
                    current_state=KairosState.INACTIVE.value,
                    status_note=f"Kairos switched to {self._runtime.mode.value} Mode. Activate to begin a new session.",
                )
            elif manual_simulation_change and self._runner.status in {KairosRunnerStatus.RUNNING, KairosRunnerStatus.PAUSED}:
                self._end_runner_locked(
                    now,
                    note="Manual simulation controls were changed, so the scripted day runner was ended.",
                    keep_log=True,
                )
                self._session.status_note = "Manual simulation controls were updated. The scripted day runner was ended so Kairos can follow the manual values."
            elif self._session.armed_for_day and self._runtime.mode == KairosMode.SIMULATION:
                self._session.status_note = "Simulation controls updated. Kairos is reclassifying the active simulation session."
                self._session.next_scan_at = now + timedelta(seconds=self._active_scan_interval_seconds())
                if self._runner.status != KairosRunnerStatus.RUNNING:
                    self._schedule_next_scan_locked()
                rerun_scan = True
            else:
                self._session.status_note = self._build_runtime_note_locked()

        if rerun_scan:
            return self.run_scan_cycle(trigger_reason="simulation-config")
        return self.get_dashboard_payload()

    def activate_for_today(self, *, force_refresh: bool = False) -> Dict[str, Any]:
        """Activate Kairos for the current session and run the first scan."""
        if self._runtime.mode == KairosMode.LIVE:
            return self.initialize_live_kairos_on_page_load()
        now = self._now()
        with self._lock:
            self._refresh_session_locked(now)
            if self._session.armed_for_day and not self._session.session_complete and not self._session.stopped_manually:
                return self._build_payload_locked(now)

            self._cancel_timer_locked()
            self._session = KairosSessionRecord(
                session_date=now.date(),
                activated_at=now,
                current_state=KairosState.ACTIVATED.value,
                armed_for_day=True,
                status_note=(
                    "Kairos armed in Simulation Mode. Off-hours testing and accelerated scans are enabled."
                    if self._runtime.mode == KairosMode.SIMULATION
                    else "Kairos armed in Live Mode. Best candidate is refreshing now."
                ),
            )

        return self.run_scan_cycle(trigger_reason=f"{slugify_label(self._runtime.mode.value)}-activation", force_refresh=force_refresh)

    def stop_for_today(self) -> Dict[str, Any]:
        """Manually stop Kairos scanning for the current day."""
        now = self._now()
        with self._lock:
            self._refresh_session_locked(now)
            self._cancel_timer_locked()
            self._end_runner_locked(now, note="Kairos was stopped manually, so the scripted day runner was ended.", keep_log=True)
            if self._session.current_state == KairosState.INACTIVE.value and not self._session.session_date:
                self._session.status_note = "Kairos is already inactive."
                return self._build_payload_locked(now)

            self._session.scan_engine_running = False
            self._session.armed_for_day = False
            self._session.stopped_manually = True
            self._session.current_state = KairosState.STOPPED.value
            self._session.next_scan_at = None
            self._session.status_note = f"Kairos stopped manually for the current {self._runtime.mode.value.lower()} session."
            return self._build_payload_locked(now)

    def start_simulation_runner(self, payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
        """Start the scripted full-day simulation runner using a deterministic scenario."""
        payload = payload or {}
        now = self._now()
        with self._lock:
            self._refresh_session_locked(now)
            base_scenario = self._resolve_runner_scenario_locked(payload.get("scenario_key"))
            scenario = self._build_anchored_runner_scenario_locked(base_scenario)
            self._cancel_timer_locked()
            self._end_runner_locked(now, note="", keep_log=False)
            self._runtime.mode = KairosMode.SIMULATION
            self._clear_live_trade_state_locked()
            self._runtime.simulation_scenario_name = scenario.name
            self._runner_scenario = scenario
            self._session = KairosSessionRecord(
                session_date=now.date(),
                activated_at=now,
                current_state=KairosState.ACTIVATED.value,
                armed_for_day=True,
                status_note=f"Kairos full-day runner started: {scenario.name}.",
            )
            self._runner = KairosRunnerState(
                status=KairosRunnerStatus.RUNNING,
                scenario_key=scenario.key,
                pause_event_filters=self._normalize_runner_pause_filters(payload),
                started_at=now,
                last_advanced_at=now,
                current_step_note=scenario.description,
            )
            self._append_runner_event_locked(
                now,
                simulated_time_label="—",
                title="Runner started",
                detail=f"Scenario selected: {scenario.name}.",
            )
        return self._advance_runner_step("runner-start")

    def pause_simulation_runner(self) -> Dict[str, Any]:
        """Pause the scripted full-day simulation runner."""
        now = self._now()
        with self._lock:
            self._refresh_session_locked(now)
            if self._runner.status != KairosRunnerStatus.RUNNING:
                return self._build_payload_locked(now)
            self._cancel_runner_timer_locked()
            self._runner.status = KairosRunnerStatus.PAUSED
            self._runner.last_advanced_at = now
            self._runner.pause_reason = "manual_pause"
            self._runner.pause_detail = "Paused manually by the operator."
            self._runner.pause_event_label = "Manual Pause"
            self._runner.pause_bar_number = self._runner.current_bar_index + 1 if self._runner.current_bar_index >= 0 else None
            self._runner.pause_simulated_time = self._runner.simulated_time_label
            self._append_runner_event_locked(now, self._runner.simulated_time_label, "Runner paused", "The scripted day progression is paused at the current simulated step.")
            self._session.status_note = "Kairos full-day runner paused."
            return self._build_payload_locked(now)

    def select_simulation_trade_candidate(self, payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
        """Select a qualified trade candidate while the runner is paused on Window Open."""
        payload = payload or {}
        now = self._now()
        with self._lock:
            self._refresh_session_locked(now)
            scenario = self._current_runner_scenario_locked()
            metrics = self._build_runner_metrics_locked(scenario, self._runner.current_bar_index) if scenario is not None else None
            candidate_context = self._resolve_active_runner_trade_candidate_context_locked(self._session.latest_scan, metrics)
            if candidate_context is None:
                self._session.status_note = "No active Kairos trade candidate set is available to select."
                return self._build_payload_locked(now)

            requested_key = str(payload.get("profile_key") or "").strip().lower()
            candidate_map = {item["mode_key"]: item for item in candidate_context["profiles"] if item.get("available")}
            if requested_key not in candidate_map:
                self._session.status_note = "Select a qualified Kairos profile before taking a simulated trade."
                return self._build_payload_locked(now)

            self._runner.selected_candidate_profile_key = requested_key
            action_label = "Best Trade Override" if candidate_context.get("is_override") else "Window Open decision"
            self._runner.last_candidate_action_note = f"Selected {candidate_map[requested_key]['mode_label']} for this {action_label}."
            self._session.status_note = self._runner.last_candidate_action_note
            return self._build_payload_locked(now)

    def take_simulation_trade_candidate(self, payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
        """Lock in a simulated trade from the currently paused Window Open candidate set and resume the runner."""
        payload = payload or {}
        now = self._now()
        with self._lock:
            self._refresh_session_locked(now)
            scenario = self._current_runner_scenario_locked()
            metrics = self._build_runner_metrics_locked(scenario, self._runner.current_bar_index) if scenario is not None else None
            candidate_context = self._resolve_active_runner_trade_candidate_context_locked(self._session.latest_scan, metrics)
            if candidate_context is None:
                self._session.status_note = "No active Kairos trade candidate set is available to lock in."
                return self._build_payload_locked(now)
            if self._runner.active_trade_lock_in is not None:
                self._session.status_note = "A simulated Kairos trade is already active for this run."
                return self._build_payload_locked(now)

            requested_key = str(payload.get("profile_key") or self._runner.selected_candidate_profile_key or "").strip().lower()
            candidate_map = {item["mode_key"]: item for item in candidate_context["profiles"] if item.get("available")}
            selected_candidate = candidate_map.get(requested_key)
            if selected_candidate is None:
                self._session.status_note = "Select a qualified Kairos profile before taking a simulated trade."
                return self._build_payload_locked(now)

            trade_lock_in = self._build_managed_trade_from_candidate_locked(
                decision_timestamp=now,
                candidate_context=candidate_context,
                selected_candidate=selected_candidate,
                source_label="Simulation",
            )
            self._runner.active_trade_lock_in = trade_lock_in
            self._runner.trade_lock_history.insert(0, trade_lock_in)
            self._runner.selected_candidate_profile_key = selected_candidate["mode_key"]
            self._runner.pause_on_exit_gates = self._coerce_bool(payload.get("pause_on_exit_gates"))
            action_prefix = "Best Trade Override" if candidate_context.get("is_override") else "Simulated"
            self._runner.last_candidate_action_note = f"{action_prefix} {selected_candidate['mode_label']} trade locked in at {candidate_context['simulated_time_label']}."
            self._append_runner_event_locked(
                now,
                candidate_context["simulated_time_label"],
                "Simulated trade locked in",
                f"{selected_candidate['mode_label']} {selected_candidate['strike_pair']} put credit spread accepted{' via Best Trade Override' if candidate_context.get('is_override') else ''}.",
            )
            self._session.status_note = self._runner.last_candidate_action_note
            self._resume_simulation_runner_locked(
                now,
                suppress_repeats=True,
                resume_detail_override=(
                    "Best Trade Override accepted; continued from the paused replay moment with a simulated trade locked in."
                    if candidate_context.get("is_override")
                    else "Simulated trade locked in; continued past this Window Open decision."
                ),
            )
            self._schedule_next_runner_step_locked()
            return self._build_payload_locked(now)

    def _build_managed_trade_from_candidate_locked(
        self,
        *,
        decision_timestamp: datetime,
        candidate_context: Dict[str, Any],
        selected_candidate: Dict[str, Any],
        source_label: str,
    ) -> KairosSimulatedTradeLockIn:
        trade_lock_in = KairosSimulatedTradeLockIn(
            decision_timestamp=decision_timestamp,
            scenario_name=candidate_context["scenario_name"],
            simulated_time_label=candidate_context["simulated_time_label"],
            bar_number=candidate_context["bar_number"],
            current_spx=candidate_context["spot"],
            current_vix=candidate_context["vix"],
            kairos_state=self._session.latest_scan.kairos_state if self._session.latest_scan is not None else KairosState.WINDOW_OPEN.value,
            profile_label=selected_candidate["mode_label"],
            spread_type=selected_candidate["spread_type"],
            short_strike=selected_candidate["short_strike"],
            long_strike=selected_candidate["long_strike"],
            width=selected_candidate["spread_width"],
            daily_move_anchor=float(candidate_context.get("daily_move_anchor") or 0.0),
            distance_points=selected_candidate["distance_points"],
            distance_percent=selected_candidate["distance_percent"],
            estimated_credit_per_contract=selected_candidate["credit_estimate_dollars"],
            estimated_total_premium_received=selected_candidate["premium_received_dollars"],
            estimated_max_loss=selected_candidate["max_loss_dollars"],
            estimated_short_delta=selected_candidate["estimated_short_delta"],
            estimated_otm_probability=selected_candidate["estimated_otm_probability"],
            contracts=selected_candidate["recommended_contracts"],
            original_credit=selected_candidate["premium_received_dollars"],
            current_credit=selected_candidate["premium_received_dollars"],
            remaining_contracts=selected_candidate["recommended_contracts"],
            rationale=selected_candidate["rationale"],
            exit_gate_states={
                item["key"]: {
                    "label": item["label"],
                    "status": "pending",
                    "contracts": 0,
                    "time_label": None,
                    "note": "",
                    "total_debit": 0.0,
                }
                for item in self.KAIROS_EXIT_GATES
            },
            entry_metadata={
                "source_label": source_label,
                "mode_key": selected_candidate["mode_key"],
                "strike_pair": selected_candidate["strike_pair"],
                "timestamp": decision_timestamp.isoformat(),
                "chain_source_label": candidate_context.get("chain_source_label") or "Estimated Model",
                "price_basis_label": candidate_context.get("price_basis_label") or "Simulated",
                "chain_status_label": candidate_context.get("chain_status_label") or "Available",
                "expected_move_source_label": candidate_context.get("daily_move_anchor_source_label") or "Estimated Model",
                "expected_move_formula_label": candidate_context.get("daily_move_anchor_formula_label") or "SPX × (VIX / 100) / 16",
                "expected_move_contracts_label": candidate_context.get("daily_move_anchor_contracts_label") or "—",
                "expected_move_used": selected_candidate.get("expected_move_used"),
                "expected_move_source": selected_candidate.get("expected_move_source") or "same_day_atm_straddle",
                "em_multiple_floor": selected_candidate.get("em_multiple_floor"),
                "percent_floor": selected_candidate.get("percent_floor"),
                "boundary_rule_used": selected_candidate.get("boundary_rule_used") or "",
                "actual_distance_to_short": selected_candidate.get("actual_distance_to_short"),
                "actual_em_multiple": selected_candidate.get("actual_em_multiple"),
                "fallback_used": selected_candidate.get("fallback_used") or "no",
                "fallback_rule_name": selected_candidate.get("fallback_rule_name") or "",
            },
        )
        trade_lock_in.exit_events.append(
            self._build_trade_event(
                event_key=f"{slugify_label(source_label)}-trade-open",
                label="Trade Opened",
                kind="trade-open",
                time_label=candidate_context["simulated_time_label"],
                current_spx=candidate_context["spot"],
                bar_number=candidate_context["bar_number"],
                timestamp=decision_timestamp,
                detail=(
                    f"{source_label} {selected_candidate['mode_label']} {selected_candidate['strike_pair']} put credit spread opened "
                    f"for {selected_candidate['recommended_contracts']} contracts."
                ),
            )
        )
        return trade_lock_in

    def _build_trade_event(
        self,
        *,
        event_key: str,
        label: str,
        kind: str,
        time_label: str,
        current_spx: float | None,
        bar_number: int | None,
        timestamp: datetime | None,
        detail: str,
    ) -> Dict[str, Any]:
        return {
            "event_key": event_key,
            "label": label,
            "kind": kind,
            "time_label": time_label,
            "current_spx": round(float(current_spx), 2) if current_spx is not None else None,
            "bar_number": bar_number,
            "timestamp": timestamp.isoformat() if timestamp is not None else None,
            "detail": detail,
        }

    def build_exit_recommendation(
        self,
        trade_lock_in: KairosSimulatedTradeLockIn,
        *,
        gate_key: str,
        title: str,
        note: str,
        current_spx: float,
        current_time_label: str,
        contracts_to_close: int,
        debit_per_contract: float,
        bar_number: int | None,
    ) -> Dict[str, Any]:
        estimated_total_debit = round(debit_per_contract * contracts_to_close, 2)
        resulting_remaining_contracts = max(0, trade_lock_in.remaining_contracts - contracts_to_close)
        resulting_current_credit = round(get_current_credit(trade_lock_in) - estimated_total_debit, 2)
        return {
            "visible": True,
            "gate_key": gate_key,
            "title": title,
            "contracts_to_close": contracts_to_close,
            "contracts_to_close_display": str(contracts_to_close),
            "estimated_debit_per_contract": round(debit_per_contract, 2),
            "estimated_debit_per_contract_display": format_currency_value(debit_per_contract),
            "estimated_total_debit": estimated_total_debit,
            "estimated_total_debit_display": format_currency_value(estimated_total_debit),
            "estimated_total_close_cost_display": format_currency_value(estimated_total_debit),
            "close_price_math_display": f"{contracts_to_close} x {format_currency_value(debit_per_contract)} = {format_currency_value(estimated_total_debit)}",
            "current_spx": round(current_spx, 2),
            "current_spx_display": format_number_value(current_spx),
            "remaining_contracts": trade_lock_in.remaining_contracts,
            "resulting_remaining_contracts": resulting_remaining_contracts,
            "resulting_remaining_contracts_display": str(resulting_remaining_contracts),
            "resulting_current_credit": resulting_current_credit,
            "resulting_current_credit_display": format_currency_value(resulting_current_credit),
            "estimated_otm_probability": round(trade_lock_in.estimated_otm_probability, 2),
            "estimated_otm_probability_display": format_percent_value(trade_lock_in.estimated_otm_probability),
            "note": note,
            "current_time_label": current_time_label,
            "bar_number": bar_number,
        }

    def apply_trade_partial_close(
        self,
        trade_lock_in: KairosSimulatedTradeLockIn,
        *,
        recommendation: Dict[str, Any],
        current_time_label: str,
        current_spx: float,
    ) -> None:
        contracts_to_close = max(0, min(int(recommendation.get("contracts_to_close") or 0), trade_lock_in.remaining_contracts))
        total_debit = round(float(recommendation.get("estimated_total_debit") or 0.0), 2)
        trade_lock_in.realized_exit_debit_dollars = round(trade_lock_in.realized_exit_debit_dollars + total_debit, 2)
        trade_lock_in.realized_close_cost_total = round(trade_lock_in.realized_close_cost_total + total_debit, 2)
        trade_lock_in.closed_contracts = min(trade_lock_in.contracts, trade_lock_in.closed_contracts + contracts_to_close)
        trade_lock_in.remaining_contracts = max(0, trade_lock_in.remaining_contracts - contracts_to_close)
        trade_lock_in.partially_reduced = 0 < trade_lock_in.remaining_contracts < trade_lock_in.contracts
        trade_lock_in.fully_closed = trade_lock_in.remaining_contracts == 0
        trade_lock_in.closed_early = trade_lock_in.fully_closed
        trade_lock_in.current_credit = get_current_credit(trade_lock_in)
        trade_lock_in.exit_management_note = recommendation.get("note") or ""
        trade_lock_in.exit_events.append(
            self._build_trade_event(
                event_key=f"{recommendation.get('gate_key') or 'exit'}-{'full' if trade_lock_in.fully_closed else 'partial'}",
                label="Full Close" if trade_lock_in.fully_closed else "Partial Close",
                kind="trade-full-close" if trade_lock_in.fully_closed else "trade-partial-close",
                time_label=current_time_label,
                current_spx=current_spx,
                bar_number=recommendation.get("bar_number"),
                timestamp=self._now(),
                detail=(
                    f"Closed {contracts_to_close} contract{'s' if contracts_to_close != 1 else ''} for {format_currency_value(total_debit)}. "
                    f"Current credit is now {format_currency_value(trade_lock_in.current_credit)}."
                ),
            )
        )

    def build_journal_prefill_for_live_trade(self, trade_lock_in: KairosSimulatedTradeLockIn | None) -> Dict[str, Any]:
        if trade_lock_in is None:
            return {}
        timestamp = trade_lock_in.decision_timestamp.astimezone(self.display_timezone)
        trade_date = timestamp.date().isoformat()
        entry_time = timestamp.strftime("%Y-%m-%dT%H:%M")
        latest_scan = self._session.latest_scan
        entry_metadata = trade_lock_in.entry_metadata or {}
        return {
            "system_name": "Kairos",
            "journal_name": "Kairos Live",
            "system_version": "3.0",
            "candidate_profile": trade_lock_in.profile_label,
            "status": "open",
            "trade_date": trade_date,
            "entry_datetime": entry_time,
            "expiration_date": trade_date,
            "underlying_symbol": "SPX",
            "spx_at_entry": round(trade_lock_in.current_spx, 2),
            "vix_at_entry": round(trade_lock_in.current_vix, 2),
            "structure_grade": latest_scan.structure_status if latest_scan is not None else trade_lock_in.kairos_state,
            "macro_grade": latest_scan.momentum_status if latest_scan is not None else "",
            "expected_move": round(trade_lock_in.daily_move_anchor, 2),
            "expected_move_used": entry_metadata.get("expected_move_used", round(trade_lock_in.daily_move_anchor, 2)),
            "expected_move_source": entry_metadata.get("expected_move_source") or "same_day_atm_straddle",
            "option_type": trade_lock_in.spread_type,
            "short_strike": trade_lock_in.short_strike,
            "long_strike": trade_lock_in.long_strike,
            "spread_width": trade_lock_in.width,
            "contracts": trade_lock_in.contracts,
            "candidate_credit_estimate": round(trade_lock_in.estimated_credit_per_contract / 100.0, 4),
            "actual_entry_credit": round(trade_lock_in.estimated_credit_per_contract / 100.0, 4),
            "net_credit_per_contract": round(trade_lock_in.estimated_credit_per_contract / 100.0, 4),
            "premium_per_contract": round(trade_lock_in.estimated_credit_per_contract, 2),
            "total_premium": round(trade_lock_in.estimated_total_premium_received, 2),
            "distance_to_short": round(trade_lock_in.distance_points, 2),
            "actual_distance_to_short": entry_metadata.get("actual_distance_to_short", round(trade_lock_in.distance_points, 2)),
            "actual_em_multiple": entry_metadata.get("actual_em_multiple"),
            "em_multiple_floor": entry_metadata.get("em_multiple_floor"),
            "percent_floor": entry_metadata.get("percent_floor"),
            "boundary_rule_used": entry_metadata.get("boundary_rule_used") or "",
            "fallback_used": entry_metadata.get("fallback_used") or "no",
            "fallback_rule_name": entry_metadata.get("fallback_rule_name") or "",
            "short_delta": round(trade_lock_in.estimated_short_delta, 2),
            "notes_entry": "Prefilled from Kairos Live accepted trade. Review before saving.",
            "prefill_source": "kairos-live",
        }

    def _build_time_remaining_ratio(self, *, current_bar_number: int | None = None, current_time: datetime | None = None) -> float:
        if current_bar_number is not None:
            return max(0.0, min(1.0, (SESSION_BAR_COUNT - max(1, current_bar_number)) / max(1, SESSION_BAR_COUNT - 1)))
        if current_time is None:
            return 0.5
        session_start = current_time.replace(hour=8, minute=30, second=0, microsecond=0)
        session_end = current_time.replace(hour=15, minute=0, second=0, microsecond=0)
        total_seconds = max(1.0, (session_end - session_start).total_seconds())
        elapsed_seconds = min(total_seconds, max(0.0, (current_time - session_start).total_seconds()))
        return max(0.0, min(1.0, 1.0 - (elapsed_seconds / total_seconds)))

    def _estimate_exit_debit_per_contract(
        self,
        trade_lock_in: KairosSimulatedTradeLockIn,
        *,
        current_spx: float,
        time_remaining_ratio: float,
    ) -> float:
        intrinsic_points = min(float(trade_lock_in.width), max(float(trade_lock_in.short_strike) - current_spx, 0.0))
        intrinsic_dollars = intrinsic_points * 100.0
        distance_to_short = max(-trade_lock_in.width, current_spx - float(trade_lock_in.short_strike))
        pressure = max(0.18, min(1.35, 1.05 - (distance_to_short / 22.0)))
        time_value = trade_lock_in.estimated_credit_per_contract * time_remaining_ratio * pressure
        return round(min(float(trade_lock_in.width * 100), max(0.0, intrinsic_dollars + time_value)), 2)

    def _build_live_trade_management_context_locked(self, now: datetime) -> tuple[float, float, str, str, float, int]:
        latest_scan = self._session.latest_scan
        current_spx = self._coerce_float(str((latest_scan.spx_value if latest_scan is not None else "0")).replace(",", ""), fallback=0.0)
        if current_spx <= 0:
            spx_snapshot = self._safe_latest_snapshot("^GSPC", query_type="kairos_live_trade_spx")
            current_spx = coerce_snapshot_number(spx_snapshot, "Latest Value") or 0.0
        current_vwap = current_spx
        current_time_label = format_display_time(now, self.display_timezone)
        structure_status = latest_scan.structure_status if latest_scan is not None else "Developing"
        current_bar_number = max(1, int((((now.hour * 60) + now.minute) - ((8 * 60) + 30)) + 1))
        return current_spx, current_vwap, current_time_label, structure_status, self._build_time_remaining_ratio(current_time=now), current_bar_number

    def _update_trade_mark_to_market_locked(
        self,
        trade_lock_in: KairosSimulatedTradeLockIn,
        *,
        current_spx: float,
        current_time_label: str,
        current_vwap: float,
        structure_status: str,
        time_remaining_ratio: float,
        current_bar_number: int,
    ) -> None:
        trade_lock_in.current_spx = round(current_spx, 2)
        trade_lock_in.current_credit = get_current_credit(trade_lock_in)
        exit_debit_per_contract = self._estimate_exit_debit_per_contract(
            trade_lock_in,
            current_spx=current_spx,
            time_remaining_ratio=time_remaining_ratio,
        )
        trade_lock_in.current_exit_debit_per_contract = exit_debit_per_contract
        trade_lock_in.current_unrealized_close_cost = round(exit_debit_per_contract * trade_lock_in.remaining_contracts, 2)
        trade_lock_in.exit_status_summary = self._evaluate_kairos_exit_state_locked(trade_lock_in, current_spx, current_time_label)
        recommendation = self.evaluate_kairos_exit_gates(
            trade_lock_in,
            current_spx=current_spx,
            current_vwap=current_vwap,
            current_time_label=current_time_label,
            structure_status=structure_status,
            debit_per_contract=exit_debit_per_contract,
            current_bar_number=current_bar_number,
        )
        if recommendation is not None:
            trade_lock_in.pending_exit_recommendation = recommendation

    def evaluate_kairos_exit_gates(
        self,
        trade_lock_in: KairosSimulatedTradeLockIn,
        *,
        current_spx: float,
        current_vwap: float,
        current_time_label: str,
        structure_status: str,
        debit_per_contract: float,
        current_bar_number: int,
    ) -> Dict[str, Any] | None:
        if trade_lock_in.final_spx_close is not None or trade_lock_in.remaining_contracts <= 0 or trade_lock_in.pending_exit_recommendation is not None:
            return None

        if current_spx < current_vwap and trade_lock_in.vwap_breach_bar_number is None:
            trade_lock_in.vwap_breach_bar_number = current_bar_number
        elif current_spx >= current_vwap:
            trade_lock_in.vwap_breach_bar_number = None

        gate_candidates: List[tuple[str, str]] = []
        if current_spx <= trade_lock_in.long_strike:
            gate_candidates.append(("long-strike-touch", f"SPX touched the long strike at {current_time_label}. Close the remaining spread."))
        if (current_spx - float(trade_lock_in.short_strike)) <= 15.0:
            gate_candidates.append(("short-strike-proximity", f"SPX is within 15 points of the short strike at {current_time_label}. Reduce risk materially."))
        if trade_lock_in.vwap_breach_bar_number is not None and current_bar_number > trade_lock_in.vwap_breach_bar_number and current_spx < current_vwap:
            gate_candidates.append(("below-vwap-failed-reclaim", f"SPX stayed below VWAP after the initial breach through {current_time_label}. Trim size."))
        if structure_status in {"Weakening", "Failed"}:
            gate_candidates.append(("structure-break", f"Kairos structure degraded to {structure_status} at {current_time_label}. Trim the position."))

        for gate_key, note in gate_candidates:
            gate_state = trade_lock_in.exit_gate_states.get(gate_key)
            if gate_state is None or gate_state.get("status") != "pending":
                continue
            gate_definition = next((item for item in self.KAIROS_EXIT_GATES if item["key"] == gate_key), None)
            if gate_definition is None:
                continue
            contracts_to_close = self._resolve_exit_gate_contracts_to_close(trade_lock_in, gate_definition["fraction"])
            if contracts_to_close <= 0:
                gate_state["status"] = "accepted"
                continue
            recommendation = self.build_exit_recommendation(
                trade_lock_in,
                gate_key=gate_key,
                title=gate_state.get("label", gate_key),
                note=note,
                current_spx=current_spx,
                current_time_label=current_time_label,
                contracts_to_close=contracts_to_close,
                debit_per_contract=debit_per_contract,
                bar_number=current_bar_number,
            )
            trade_lock_in.pending_exit_recommendation = recommendation
            trade_lock_in.pending_exit_gate_key = gate_key
            trade_lock_in.pending_exit_gate_contracts = contracts_to_close
            trade_lock_in.pending_exit_gate_debit_per_contract = debit_per_contract
            trade_lock_in.pending_exit_gate_total_debit = recommendation["estimated_total_debit"]
            trade_lock_in.pending_exit_gate_note = note
            trade_lock_in.exit_management_note = note
            return recommendation
        return None

    def accept_simulation_exit_gate(self) -> Dict[str, Any]:
        """Accept the currently pending Kairos Sim exit gate and resume the replay."""
        now = self._now()
        with self._lock:
            self._refresh_session_locked(now)
            if not self._has_pending_exit_gate_locked():
                self._session.status_note = "No pending Kairos exit action is available to accept."
                return self._build_payload_locked(now)

            trade_lock_in = self._runner.active_trade_lock_in
            scenario = self._current_runner_scenario_locked()
            metrics = self._build_runner_metrics_locked(scenario, self._runner.current_bar_index) if scenario is not None else None
            if trade_lock_in is None or metrics is None or metrics.get("current_bar") is None:
                return self._build_payload_locked(now)

            gate_key = trade_lock_in.pending_exit_gate_key or ""
            gate_state = trade_lock_in.exit_gate_states.get(gate_key, {})
            current_time_label = format_simulated_clock(metrics["current_bar"].simulated_time)
            recommendation = dict(trade_lock_in.pending_exit_recommendation or self._build_sim_trade_exit_prompt_locked(metrics))

            self.apply_trade_partial_close(
                trade_lock_in,
                recommendation=recommendation,
                current_time_label=current_time_label,
                current_spx=float(metrics.get("current_close") or 0.0),
            )
            gate_state.update(
                {
                    "status": "accepted",
                    "contracts": recommendation.get("contracts_to_close", 0),
                    "time_label": current_time_label,
                    "note": recommendation.get("note") or trade_lock_in.pending_exit_gate_note,
                    "total_debit": recommendation.get("estimated_total_debit", 0.0),
                }
            )
            trade_lock_in.exit_status_summary = gate_state.get("label", "Exit Managed")
            self._append_runner_event_locked(
                now,
                current_time_label,
                f"Exit accepted: {gate_state.get('label', 'Kairos exit')}",
                recommendation.get("note") or trade_lock_in.pending_exit_gate_note,
            )
            self._clear_pending_exit_gate_locked(trade_lock_in)

            if trade_lock_in.fully_closed:
                self._finalize_closed_early_trade_locked(trade_lock_in, current_spx=float(metrics.get("current_close") or 0.0), current_time_label=current_time_label)

            self._resume_simulation_runner_locked(
                now,
                suppress_repeats=True,
                resume_detail_override="Kairos exit gate accepted; the replay resumed with updated simulated exposure.",
            )
            self._session.status_note = "Kairos exit gate accepted."
            self._schedule_next_runner_step_locked()
            return self._build_payload_locked(now)

    def skip_simulation_exit_gate(self) -> Dict[str, Any]:
        """Skip the currently pending Kairos Sim exit gate and resume the replay."""
        now = self._now()
        with self._lock:
            self._refresh_session_locked(now)
            if not self._has_pending_exit_gate_locked():
                self._session.status_note = "No pending Kairos exit action is available to skip."
                return self._build_payload_locked(now)

            trade_lock_in = self._runner.active_trade_lock_in
            if trade_lock_in is None:
                return self._build_payload_locked(now)

            gate_key = trade_lock_in.pending_exit_gate_key or ""
            gate_state = trade_lock_in.exit_gate_states.get(gate_key, {})
            gate_state.update(
                {
                    "status": "skipped",
                    "contracts": trade_lock_in.pending_exit_gate_contracts,
                    "time_label": self._runner.simulated_time_label,
                    "note": trade_lock_in.pending_exit_gate_note,
                    "total_debit": 0.0,
                }
            )
            trade_lock_in.exit_management_note = f"Skipped {gate_state.get('label', 'Kairos exit gate')} at {self._runner.simulated_time_label}."
            trade_lock_in.exit_events.append(
                self._build_trade_event(
                    event_key=f"{gate_key or 'exit'}-skipped",
                    label=f"Skipped {gate_state.get('label', 'Kairos exit gate')}",
                    kind="trade-skip",
                    time_label=self._runner.simulated_time_label,
                    current_spx=trade_lock_in.current_spx,
                    bar_number=self._runner.current_bar_index + 1 if self._runner.current_bar_index >= 0 else None,
                    timestamp=now,
                    detail=trade_lock_in.pending_exit_gate_note,
                )
            )
            self._append_runner_event_locked(
                now,
                self._runner.simulated_time_label,
                f"Exit skipped: {gate_state.get('label', 'Kairos exit')}",
                trade_lock_in.pending_exit_gate_note,
            )
            self._clear_pending_exit_gate_locked(trade_lock_in)
            self._resume_simulation_runner_locked(
                now,
                suppress_repeats=True,
                resume_detail_override="Kairos exit gate skipped; the replay resumed without reducing the simulated position.",
            )
            self._session.status_note = trade_lock_in.exit_management_note
            self._schedule_next_runner_step_locked()
            return self._build_payload_locked(now)

    def ignore_simulation_trade_candidate(self) -> Dict[str, Any]:
        """Decline the current Window Open candidate set and resume the runner without a trade."""
        now = self._now()
        with self._lock:
            self._refresh_session_locked(now)
            scenario = self._current_runner_scenario_locked()
            metrics = self._build_runner_metrics_locked(scenario, self._runner.current_bar_index) if scenario is not None else None
            candidate_context = self._resolve_active_runner_trade_candidate_context_locked(self._session.latest_scan, metrics)
            if candidate_context is None:
                self._session.status_note = "No active Kairos trade candidate set is available to ignore."
                return self._build_payload_locked(now)

            if candidate_context.get("is_override"):
                self._runner.best_trade_override_requested = False
                self._runner.candidate_event_id = None
                self._runner.selected_candidate_profile_key = None
                self._runner.last_candidate_action_note = f"Best Trade Override dismissed at {candidate_context['simulated_time_label']}."
                self._session.status_note = self._runner.last_candidate_action_note
                return self._build_payload_locked(now)

            self._runner.last_candidate_action_note = f"Window Open candidates declined at {candidate_context['simulated_time_label']}."
            self._append_runner_event_locked(
                now,
                candidate_context["simulated_time_label"],
                "Candidates ignored",
                "Window Open candidates were declined and the runner continued without a trade.",
            )
            self._session.status_note = self._runner.last_candidate_action_note
            self._resume_simulation_runner_locked(
                now,
                suppress_repeats=True,
                resume_detail_override="Window Open candidates ignored; continued without a simulated trade.",
            )
            self._schedule_next_runner_step_locked()
            return self._build_payload_locked(now)

    def resume_simulation_runner(self, payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
        """Resume the scripted full-day simulation runner."""
        payload = payload or {}
        now = self._now()
        with self._lock:
            self._refresh_session_locked(now)
            if self._runner.status != KairosRunnerStatus.PAUSED or self._runner.scenario_key is None:
                return self._build_payload_locked(now)
            suppress_repeats = self._coerce_bool(payload.get("ignore_successive_condition"))
            self._resume_simulation_runner_locked(now, suppress_repeats=suppress_repeats)
            self._runner.last_candidate_action_note = ""
            self._session.status_note = "Kairos full-day runner resumed."
            self._schedule_next_runner_step_locked()
            return self._build_payload_locked(now)

    def restart_simulation_runner(self, payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
        """Restart the scripted full-day simulation runner from the first step."""
        payload = payload or {}
        scenario_key = payload.get("scenario_key")
        if not scenario_key:
            with self._lock:
                scenario_key = self._runner.scenario_key
        return self.start_simulation_runner({"scenario_key": scenario_key})

    def end_simulation_runner(self) -> Dict[str, Any]:
        """End the scripted full-day simulation runner without restarting it."""
        now = self._now()
        with self._lock:
            self._refresh_session_locked(now)
            if self._runner.status == KairosRunnerStatus.IDLE:
                return self._build_payload_locked(now)
            self._end_runner_locked(now, note="Kairos full-day runner ended manually.", keep_log=True)
            self._session.scan_engine_running = False
            self._session.armed_for_day = False
            self._session.stopped_manually = True
            self._session.next_scan_at = None
            self._session.current_state = KairosState.STOPPED.value
            self._session.status_note = "Kairos full-day runner ended manually."
            return self._build_payload_locked(now)

    def import_historical_replay_template(self, payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
        """Import a historical replay scenario from Schwab or JSON and return the updated dashboard payload."""
        payload = payload or {}
        now = self._now()
        session_date = self._coerce_historical_replay_date(payload.get("session_date"))
        requested_source = self._coerce_historical_replay_source(payload.get("source") or "auto")
        raw_json = str(payload.get("json_payload") or payload.get("raw_json") or "").strip()
        with self._lock:
            self._refresh_session_locked(now)
            scenario, template = self._resolve_historical_replay_import(
                session_date=session_date,
                requested_source=requested_source,
                raw_json=raw_json,
            )
            self._persist_historical_replay_locked(scenario, template)
            self._register_persisted_replay_scenario(scenario, template)
            self._mark_persisted_scenarios_dirty_locked()
            self._session.status_note = f"Historical replay ready: {template.scenario_name} ({template.source_label})."
            return self._build_payload_locked(now)

    def get_dashboard_payload(self) -> Dict[str, Any]:
        """Return the current Kairos dashboard payload."""
        now = self._now()
        with self._lock:
            self._refresh_session_locked(now)
            return self._build_payload_locked(now)

    def build_management_context(self) -> Dict[str, Any]:
        """Return the minimal Kairos session context needed by Manage Trades."""
        now = self._now()
        with self._lock:
            self._refresh_session_locked(now)
            latest_scan = self._session.latest_scan

        default_status = "Initializing" if self._runtime.mode == KairosMode.LIVE else "Activation"
        return {
            "current_structure_status": str(latest_scan.structure_status if latest_scan is not None else default_status),
            "current_momentum_status": str(latest_scan.momentum_status if latest_scan is not None else default_status),
        }

    def initialize_live_kairos_on_page_load(self, *, force_refresh: bool = False) -> Dict[str, Any]:
        """Initialize the Live Kairos workspace when the live page is opened."""
        now = self._now()
        with self._lock:
            self._refresh_session_locked(now)
            if self._runtime.mode != KairosMode.LIVE:
                return self._build_payload_locked(now)

            market_is_open = self.market_calendar_service.is_tradable_market_day(now.date()) and self._get_effective_market_session_status(now) == "Open"
            if not market_is_open:
                self._cancel_timer_locked()
                if self._session.session_date != now.date():
                    self._session = KairosSessionRecord(session_date=now.date(), activated_at=now)
                self._session.session_date = now.date()
                self._session.activated_at = self._session.activated_at or now
                self._session.current_state = KairosState.INACTIVE.value
                self._session.armed_for_day = False
                self._session.scan_engine_running = False
                self._session.session_complete = False
                self._session.stopped_manually = False
                self._session.auto_ended = False
                self._session.next_scan_at = None
                self._session.status_note = self._get_live_market_state_message(now)
                return self._build_payload_locked(now)

            if self._session.armed_for_day and not self._session.session_complete and not self._session.stopped_manually:
                return self._build_payload_locked(now)

            self._cancel_timer_locked()
            if self._session.session_date == now.date():
                self._session.activated_at = self._session.activated_at or now
                self._session.current_state = KairosState.ACTIVATED.value
                self._session.armed_for_day = True
                self._session.scan_engine_running = False
                self._session.session_complete = False
                self._session.stopped_manually = False
                self._session.auto_ended = False
                self._session.next_scan_at = None
                self._session.status_note = "Live Kairos initialized from the workspace. Best candidate is refreshing now."
            else:
                self._session = KairosSessionRecord(
                    session_date=now.date(),
                    activated_at=now,
                    current_state=KairosState.ACTIVATED.value,
                    armed_for_day=True,
                    status_note="Live Kairos initialized from the workspace. Best candidate is refreshing now.",
                )

        return self.run_scan_cycle(trigger_reason="live-workspace-open", force_refresh=force_refresh)

    def run_scan_cycle(self, trigger_reason: str = "scheduled", *, force_refresh: bool = False) -> Dict[str, Any]:
        """Execute one Kairos scan cycle and reschedule when appropriate."""
        now = self._now()
        with self._lock:
            self._refresh_session_locked(now)
            self._cancel_timer_locked()
            if not self._session.armed_for_day or self._session.session_complete or self._session.stopped_manually:
                return self._build_payload_locked(now)
            if self._runtime.mode == KairosMode.LIVE and self._get_effective_market_session_status(now) == "Ended":
                self._complete_session_locked(now, auto_ended=True, reason="Kairos auto-ended at the close of the live market session.")
                return self._build_payload_locked(now)
            if self._session.scan_engine_running:
                return self._build_payload_locked(now)
            self._session.scan_engine_running = True
            self._session.current_state = KairosState.SCANNING.value

        live_scan_results = self._run_live_intraday_backfill(now, trigger_reason=trigger_reason, force_refresh=force_refresh) if self._runtime.mode == KairosMode.LIVE else None
        scan_result = (live_scan_results[-1] if live_scan_results else None) or self._evaluate_scan(now, trigger_reason=trigger_reason, force_refresh=force_refresh)

        with self._lock:
            if self._session.stopped_manually:
                self._session.scan_engine_running = False
                return self._build_payload_locked(self._now())

            if live_scan_results is not None:
                self._session.scan_log = list(reversed(live_scan_results))
                self._session.latest_scan = scan_result
                self._session.last_scan_at = scan_result.timestamp
                self._session.total_scans_completed = scan_result.scan_sequence_number
                self._session.current_state = scan_result.kairos_state
                self._session.window_found = any(item.is_window_open for item in live_scan_results)
            else:
                self._session.latest_scan = scan_result
                self._session.last_scan_at = scan_result.timestamp
                self._session.total_scans_completed = scan_result.scan_sequence_number
                self._session.current_state = scan_result.kairos_state
                self._session.window_found = self._session.window_found or scan_result.is_window_open
                self._session.scan_log.insert(0, scan_result)
            self._session.scan_engine_running = False
            self._session.status_note = scan_result.summary_text

            if self._runtime.mode == KairosMode.LIVE:
                self._update_live_trade_management_for_scan_locked(now, scan_result)
                self._send_window_open_notification_locked(now, scan_result)
                self._persist_completed_live_tape_locked(now, session_date=self._session.session_date or now.date(), session_status="partial")

            if scan_result.is_expired and self._runtime.mode == KairosMode.LIVE:
                self._session.armed_for_day = False
                self._session.session_complete = True
                self._session.next_scan_at = None
                self._session.status_note = f"{scan_result.summary_text} Kairos ended scanning for the live market day."
            elif self._runner.status == KairosRunnerStatus.RUNNING:
                self._session.next_scan_at = None
            else:
                self._session.next_scan_at = scan_result.timestamp + timedelta(seconds=self._active_scan_interval_seconds())
                self._schedule_next_scan_locked()

            return self._build_payload_locked(self._now())

    def _run_live_intraday_backfill(
        self,
        now: datetime,
        *,
        trigger_reason: str,
        force_refresh: bool = False,
    ) -> List[KairosScanResult] | None:
        evaluator = self._evaluate_live_intraday_backfill
        if "force_refresh" in inspect.signature(evaluator).parameters:
            return evaluator(now, trigger_reason=trigger_reason, force_refresh=force_refresh)
        return evaluator(now, trigger_reason=trigger_reason)

    def _send_window_open_notification_locked(self, now: datetime, scan_result: KairosScanResult) -> None:
        """Non-trade Kairos notifications are suppressed so only real-trade alerts remain."""
        return

    def _evaluate_scan(self, now: datetime, *, trigger_reason: str, force_refresh: bool = False) -> KairosScanResult:
        session_status = self._get_effective_market_session_status(now)
        prior_state = self._derive_prior_state()

        if self._runtime.mode == KairosMode.SIMULATION:
            spx_numeric = self._runtime.simulated_spx_value
            vix_numeric = self._runtime.simulated_vix_value
            spx_value = f"{spx_numeric:,.2f}"
            vix_value = f"{vix_numeric:,.2f}"
            vix_status = self._evaluate_vix_regime(vix_numeric)
            timing_status = self._coerce_choice(self._runtime.simulated_timing_status, self.TIMING_STATUS_OPTIONS)
            structure_status = self._coerce_choice(self._runtime.simulated_structure_status, self.STRUCTURE_STATUS_OPTIONS)
            momentum_status = self._coerce_choice(self._runtime.simulated_momentum_status, self.MOMENTUM_STATUS_OPTIONS)
            classification_note = "Simulation controls are driving the current Kairos scan; live market data is not being used."
            scenario_name = self._runtime.simulation_scenario_name
        else:
            spx_snapshot = self._safe_latest_snapshot("^GSPC", query_type="kairos_latest_spx", force_refresh=force_refresh)
            vix_snapshot = self._safe_latest_snapshot("^VIX", query_type="kairos_latest_vix", force_refresh=force_refresh)
            spx_value = format_snapshot_value(spx_snapshot)
            vix_value = format_snapshot_value(vix_snapshot)
            vix_numeric = coerce_snapshot_number(vix_snapshot, "Latest Value")
            vix_status = self._evaluate_vix_regime(vix_numeric)
            live_bar_payloads = self._build_live_bar_map_bars_locked(query_type="kairos_latest_state")
            if live_bar_payloads:
                metrics = self._build_live_bar_metrics_from_payloads(live_bar_payloads, len(live_bar_payloads) - 1)
                return self._build_live_intraday_scan_result(
                    timestamp=now,
                    scan_sequence_number=self._session.total_scans_completed + 1,
                    metrics=metrics,
                    vix_status=vix_status,
                    vix_display=vix_value,
                    prior_state=prior_state,
                    trigger_reason=trigger_reason,
                )
            spx_change = coerce_snapshot_number(spx_snapshot, "Daily Percent Change")
            timing_status = self._evaluate_timing_status(now, session_status)
            structure_status = self._evaluate_structure_status(spx_change)
            momentum_status = self._evaluate_live_momentum_status(
                timing_status=timing_status,
                structure_status=structure_status,
                vix_status=vix_status,
                prior_state=prior_state,
            )
            classification_note = "Live Mode is using daily SPX percent-change fallback because the intraday SPX tape was unavailable."
            scenario_name = None

        state = self._classify_overall_state(
            session_status=session_status,
            vix_status=vix_status,
            timing_status=timing_status,
            structure_status=structure_status,
            momentum_status=momentum_status,
            prior_state=prior_state,
        )
        readiness = state
        summary = self._build_summary_text(state)
        reasons = self._build_reasons(
            mode=self._runtime.mode,
            session_status=session_status,
            vix_status=vix_status,
            timing_status=timing_status,
            structure_status=structure_status,
            momentum_status=momentum_status,
            vix_value=vix_value,
            scenario_name=scenario_name,
        )
        transition = self._build_state_transition(prior_state, state)

        return KairosScanResult(
            timestamp=now,
            scan_sequence_number=self._session.total_scans_completed + 1,
            mode=self._runtime.mode.value,
            kairos_state=state,
            prior_kairos_state=prior_state,
            vix_status=vix_status,
            timing_status=timing_status,
            structure_status=structure_status,
            momentum_status=momentum_status,
            readiness_state=readiness,
            summary_text=summary,
            reasons=reasons,
            state_transition=transition,
            is_window_open=state == KairosState.WINDOW_OPEN.value,
            is_window_closing=state == KairosState.WINDOW_CLOSING.value,
            is_expired=state == KairosState.EXPIRED.value,
            trigger_reason=trigger_reason,
            market_session_status=session_status,
            spx_value=spx_value,
            vix_value=vix_value,
            simulation_scenario_name=scenario_name,
            classification_note=classification_note,
        )

    def _evaluate_live_intraday_backfill(self, now: datetime, *, trigger_reason: str, force_refresh: bool = False) -> List[KairosScanResult] | None:
        if self._runtime.mode != KairosMode.LIVE:
            return None

        frame = self._load_live_spx_bar_frame_locked(
            session_date=self._session.session_date or now.date(),
            query_type="kairos_live_intraday_backfill",
            force_refresh=force_refresh,
        )
        bar_payloads = self._build_live_bar_map_bars_from_frame(frame)
        if not bar_payloads:
            return None

        vix_snapshot = self._safe_latest_snapshot("^VIX", query_type="kairos_live_intraday_backfill_vix", force_refresh=force_refresh)
        vix_numeric = coerce_snapshot_number(vix_snapshot, "Latest Value")
        vix_status = self._evaluate_vix_regime(vix_numeric)
        vix_display = format_number_value(vix_numeric) if vix_numeric is not None else "—"
        results: List[KairosScanResult] = []
        prior_state: str | None = None

        for index, bar in enumerate(bar_payloads):
            timestamp_iso = str(bar.get("timestamp_iso") or "").strip()
            try:
                timestamp = datetime.fromisoformat(timestamp_iso)
            except ValueError:
                continue
            if timestamp > now:
                break

            metrics = self._build_live_bar_metrics_from_payloads(bar_payloads, index)
            scan_result = self._build_live_intraday_scan_result(
                timestamp=timestamp,
                scan_sequence_number=index + 1,
                metrics=metrics,
                vix_status=vix_status,
                vix_display=vix_display,
                prior_state=prior_state,
                trigger_reason=trigger_reason,
            )
            results.append(scan_result)
            prior_state = scan_result.kairos_state

        return results or None

    def _build_live_bar_metrics_from_payloads(self, bar_payloads: List[Dict[str, Any]], bar_index: int) -> Dict[str, Any]:
        tape_slice = bar_payloads[: bar_index + 1]
        current_bar = tape_slice[-1]
        session_open = float(tape_slice[0].get("open") or 0.0)
        current_close = float(current_bar.get("close") or 0.0)
        session_high = max(float(item.get("high") or current_close) for item in tape_slice)
        session_low = min(float(item.get("low") or current_close) for item in tape_slice)
        current_vwap = float(current_bar.get("vwap") or current_close)
        recent_anchor = tape_slice[max(0, len(tape_slice) - 6)]
        net_change_percent = percent_change(current_close, session_open)
        close_vs_vwap_percent = percent_change(current_close, current_vwap)
        recent_change_percent = percent_change(current_close, float(recent_anchor.get("close") or current_close))
        return {
            "current_close": current_close,
            "session_high": session_high,
            "session_low": session_low,
            "vwap": current_vwap,
            "current_vix": 0.0,
            "session_posture": self._derive_runner_session_posture(
                net_change_percent=net_change_percent,
                close_vs_vwap_percent=close_vs_vwap_percent,
                recent_change_percent=recent_change_percent,
            ),
            "net_change_percent": net_change_percent,
            "close_vs_vwap_percent": close_vs_vwap_percent,
            "recent_change_percent": recent_change_percent,
            "current_bar": current_bar,
            "cumulative_volume": sum(int(item.get("volume") or 0) for item in tape_slice),
        }

    def _refresh_session_locked(self, now: datetime) -> None:
        if self._session.session_date and self._session.session_date != now.date():
            if self._runtime.mode == KairosMode.LIVE:
                self._persist_completed_live_tape_locked(now, session_date=self._session.session_date, session_status="partial")
                self._recover_or_finalize_previous_live_tape_locked(now)
            self._cancel_timer_locked()
            self._cancel_runner_timer_locked()
            self._session = KairosSessionRecord()
            self._runner = KairosRunnerState()
            self._runner_scenario = None
            self._clear_live_trade_state_locked()
            return

        self._recover_or_finalize_previous_live_tape_locked(now)

        if self._runtime.mode == KairosMode.LIVE and self._session.armed_for_day and self._get_effective_market_session_status(now) == "Ended":
            self._complete_session_locked(now, auto_ended=True, reason="Kairos auto-ended at the close of the live market session.")

    def _complete_session_locked(self, now: datetime, *, auto_ended: bool, reason: str) -> None:
        self._cancel_timer_locked()
        self._cancel_runner_timer_locked()
        self._session.armed_for_day = False
        self._session.scan_engine_running = False
        self._session.session_complete = True
        self._session.auto_ended = auto_ended
        self._session.next_scan_at = None
        if self._session.current_state not in {KairosState.EXPIRED.value, KairosState.STOPPED.value}:
            self._session.current_state = KairosState.SESSION_COMPLETE.value
        self._session.status_note = reason
        if self._session.session_date is None:
            self._session.session_date = now.date()
        if self._runtime.mode == KairosMode.LIVE:
            self._persist_completed_live_tape_locked(now, session_date=self._session.session_date, session_status="complete")

    def _schedule_next_scan_locked(self) -> None:
        self._cancel_timer_locked()
        if self._runner.status == KairosRunnerStatus.RUNNING or not self._session.armed_for_day or self._session.next_scan_at is None:
            return
        delay_seconds = max(0.0, (self._session.next_scan_at - self._now()).total_seconds())
        self._timer = self._runtime_scheduler().schedule(delay_seconds, self._scheduled_scan_callback, daemon=True)

    def _schedule_next_runner_step_locked(self) -> None:
        self._cancel_runner_timer_locked()
        if self._runner.status != KairosRunnerStatus.RUNNING:
            return
        scenario = self._current_runner_scenario_locked()
        if scenario is None or self._runner.current_bar_index >= len(scenario.bars) - 1:
            return
        self._runner_timer = self._runtime_scheduler().schedule(
            self.RUNNER_TICK_INTERVAL_SECONDS,
            self._scheduled_runner_callback,
            daemon=True,
        )

    def _runtime_scheduler(self) -> RuntimeScheduler:
        return self.scheduler or ThreadingTimerScheduler(self.timer_factory)

    def _scheduled_scan_callback(self) -> None:
        self.run_scan_cycle(trigger_reason="scheduled")

    def _scheduled_runner_callback(self) -> None:
        self._advance_runner_step("runner-scheduled")

    def _cancel_timer_locked(self) -> None:
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None

    def _cancel_runner_timer_locked(self) -> None:
        if self._runner_timer is not None:
            self._runner_timer.cancel()
            self._runner_timer = None

    def _advance_runner_step(self, trigger_reason: str) -> Dict[str, Any]:
        now = self._now()
        processed_bars = 0

        while True:
            with self._lock:
                self._refresh_session_locked(now)
                if self._runner.status != KairosRunnerStatus.RUNNING:
                    return self._build_payload_locked(now)
                scenario = self._current_runner_scenario_locked()
                if scenario is None:
                    self._end_runner_locked(now, note="Kairos full-day runner ended because its scenario could not be resolved.", keep_log=False)
                    return self._build_payload_locked(now)

                next_bar_index = self._runner.current_bar_index + 1
                if next_bar_index >= len(scenario.bars):
                    self._complete_runner_locked(now, scenario.name)
                    return self._build_payload_locked(now)

                current_bar = scenario.bars[next_bar_index]
                metrics = self._build_runner_metrics_locked(scenario, next_bar_index)
                self._apply_runner_bar_to_runtime_locked(scenario, current_bar, metrics)
                self._runner.current_bar_index = next_bar_index
                self._runner.simulated_time_label = format_simulated_clock(current_bar.simulated_time)
                self._runner.last_advanced_at = now
                self._session.armed_for_day = True
                self._session.stopped_manually = False
                self._session.session_complete = False

            self.run_scan_cycle(trigger_reason=f"{trigger_reason}-bar-{next_bar_index + 1}")
            processed_bars += 1

            with self._lock:
                scenario = self._current_runner_scenario_locked()
                if scenario is None:
                    return self._build_payload_locked(self._now())

                metrics = self._build_runner_metrics_locked(scenario, self._runner.current_bar_index)
                latest_scan = self._session.latest_scan
                self._session.status_note = f"Kairos fast tape runner processed bar {self._runner.current_bar_index + 1} of {len(scenario.bars)}."

                self._update_active_sim_trade_status_locked(metrics)
                self._clear_runner_pause_suppression_if_needed_locked(latest_scan)
                exit_gate_pause = self._evaluate_runner_exit_gate_pause_locked(latest_scan, metrics)
                if exit_gate_pause is not None:
                    self._cancel_runner_timer_locked()
                    self._runner.status = KairosRunnerStatus.PAUSED
                    self._runner.pause_reason = exit_gate_pause["reason_key"]
                    self._runner.pause_detail = exit_gate_pause["detail"]
                    self._runner.pause_event_label = exit_gate_pause["event_label"]
                    self._runner.pause_bar_number = self._runner.current_bar_index + 1
                    self._runner.pause_simulated_time = self._runner.simulated_time_label
                    self._append_runner_event_locked(
                        self._now(),
                        self._runner.simulated_time_label,
                        f"Paused on {exit_gate_pause['event_label']}",
                        exit_gate_pause["detail"],
                    )
                    self._session.status_note = f"Kairos paused on exit gate: {exit_gate_pause['event_label']}."
                    return self._build_payload_locked(self._now())

                decision_pause = self._resolve_runner_trade_decision_pause_locked(latest_scan, metrics)
                if decision_pause is not None:
                    self._cancel_runner_timer_locked()
                    self._runner.status = KairosRunnerStatus.PAUSED
                    self._runner.pause_reason = decision_pause["reason_key"]
                    self._runner.pause_detail = decision_pause["detail"]
                    self._runner.pause_event_label = decision_pause["event_label"]
                    self._runner.pause_bar_number = self._runner.current_bar_index + 1
                    self._runner.pause_simulated_time = self._runner.simulated_time_label
                    self._append_runner_event_locked(
                        self._now(),
                        self._runner.simulated_time_label,
                        "Decision pause",
                        decision_pause["detail"],
                    )
                    self._session.status_note = "Kairos paused automatically at Prime for trade review."
                    return self._build_payload_locked(self._now())

                event_pause = self._resolve_runner_event_pause_locked(latest_scan)
                if event_pause is not None:
                    self._cancel_runner_timer_locked()
                    self._runner.status = KairosRunnerStatus.PAUSED
                    self._runner.pause_reason = event_pause["reason_key"]
                    self._runner.pause_detail = event_pause["detail"]
                    self._runner.pause_event_label = event_pause["event_label"]
                    self._runner.pause_bar_number = self._runner.current_bar_index + 1
                    self._runner.pause_simulated_time = self._runner.simulated_time_label
                    self._append_runner_event_locked(
                        self._now(),
                        self._runner.simulated_time_label,
                        f"Paused on {event_pause['event_label']}",
                        event_pause["detail"],
                    )
                    self._session.status_note = f"Kairos paused on event: {event_pause['event_label']}."
                    return self._build_payload_locked(self._now())

                if latest_scan is not None and (
                    self._runner.current_bar_index == 0
                    or (self._runner.current_bar_index + 1) % 60 == 0
                    or self._runner.current_bar_index == len(scenario.bars) - 1
                ):
                    self._append_runner_event_locked(
                        self._now(),
                        self._runner.simulated_time_label,
                        f"Bar {self._runner.current_bar_index + 1}",
                        f"{metrics['session_posture']} with close {metrics['current_close']:.2f} and VWAP {metrics['vwap']:.2f}. Kairos state: {latest_scan.kairos_state}.",
                    )

                if self._runner.current_bar_index >= len(scenario.bars) - 1:
                    self._complete_runner_locked(self._now(), scenario.name)
                    return self._build_payload_locked(self._now())

                if processed_bars >= self.RUNNER_FAST_BATCH_SIZE:
                    self._schedule_next_runner_step_locked()
                    return self._build_payload_locked(self._now())

    def _complete_runner_locked(self, now: datetime, scenario_name: str) -> None:
        self._cancel_runner_timer_locked()
        self._settle_active_trade_lock_in_locked()
        self._runner.status = KairosRunnerStatus.COMPLETED
        self._runner.completed_at = now
        self._runner.pause_reason = None
        self._runner.pause_detail = ""
        self._runner.pause_event_label = None
        self._runner.pause_bar_number = None
        self._runner.pause_simulated_time = None
        self._runner.suppressed_pause_event_kind = None
        self._runner.suppressed_pause_event_label = None
        self._append_runner_event_locked(now, self._runner.simulated_time_label, "Runner completed", f"Scenario completed: {scenario_name}.")
        self._complete_session_locked(now, auto_ended=False, reason=f"Kairos full-day runner completed: {scenario_name}.")

    def _end_runner_locked(self, now: datetime, *, note: str, keep_log: bool) -> None:
        if self._runner.status == KairosRunnerStatus.IDLE and not keep_log:
            self._cancel_runner_timer_locked()
            self._runner = KairosRunnerState()
            self._runner_scenario = None
            return
        scenario_key = self._runner.scenario_key
        event_log = list(self._runner.event_log) if keep_log else []
        self._cancel_runner_timer_locked()
        self._runner = KairosRunnerState(
            status=KairosRunnerStatus.ENDED if keep_log and scenario_key else KairosRunnerStatus.IDLE,
            scenario_key=scenario_key if keep_log else None,
            run_mode=self._runner.run_mode if keep_log else "Fast Run",
            current_bar_index=self._runner.current_bar_index if keep_log else -1,
            simulated_time_label=self._runner.simulated_time_label if keep_log else "—",
            current_step_label=self._runner.current_step_label if keep_log else "Tape idle",
            current_step_note=self._runner.current_step_note if keep_log else "Choose a scenario to begin the full-day simulator.",
            pause_reason=self._runner.pause_reason if keep_log else None,
            pause_detail=self._runner.pause_detail if keep_log else "",
            pause_event_filters=list(self._runner.pause_event_filters) if keep_log else [],
            pause_event_label=self._runner.pause_event_label if keep_log else None,
            pause_bar_number=self._runner.pause_bar_number if keep_log else None,
            pause_simulated_time=self._runner.pause_simulated_time if keep_log else None,
            candidate_event_id=self._runner.candidate_event_id if keep_log else None,
            selected_candidate_profile_key=self._runner.selected_candidate_profile_key if keep_log else None,
            last_candidate_action_note=self._runner.last_candidate_action_note if keep_log else "",
            suppressed_pause_event_kind=self._runner.suppressed_pause_event_kind if keep_log else None,
            suppressed_pause_event_label=self._runner.suppressed_pause_event_label if keep_log else None,
            active_trade_lock_in=self._runner.active_trade_lock_in if keep_log else None,
            trade_lock_history=list(self._runner.trade_lock_history) if keep_log else [],
            started_at=self._runner.started_at if keep_log else None,
            last_advanced_at=now if keep_log else None,
            completed_at=now if keep_log else None,
            event_log=event_log,
            best_trade_override_requested=False,
            pause_on_exit_gates=self._runner.pause_on_exit_gates if keep_log else False,
        )
        if keep_log and note:
            self._append_runner_event_locked(now, self._runner.simulated_time_label, "Runner ended", note)
        if not keep_log:
            self._runner_scenario = None

    def _current_runner_scenario_locked(self) -> KairosTapeScenario | None:
        if not self._runner.scenario_key:
            return None
        return self._runner_scenario or self._historical_replay_scenarios.get(self._runner.scenario_key) or KAIROS_RUNNER_SCENARIOS.get(self._runner.scenario_key)

    def _resolve_runner_scenario_locked(self, scenario_key: Any) -> KairosTapeScenario:
        key = str(scenario_key or "").strip().lower()
        if key in self._historical_replay_scenarios:
            return self._historical_replay_scenarios[key]
        if key in KAIROS_RUNNER_SCENARIOS:
            return KAIROS_RUNNER_SCENARIOS[key]
        if self._historical_replay_scenarios:
            return next(iter(self._historical_replay_scenarios.values()))
        return next(iter(KAIROS_RUNNER_SCENARIOS.values()))

    def _build_runner_scenario_options_locked(self) -> List[Dict[str, Any]]:
        self._sync_persisted_scenarios_locked()
        options: List[Dict[str, Any]] = []
        seen_values: set[str] = set()
        if self._runtime.mode == KairosMode.SIMULATION:
            imported_templates = sorted(
                self._historical_replay_templates.values(),
                key=lambda item: item.session_date,
                reverse=True,
            )
            for template in imported_templates:
                if template.scenario_key in seen_values:
                    continue
                seen_values.add(template.scenario_key)
                options.append(
                    {
                        "label": template.scenario_name,
                        "value": template.scenario_key,
                        "description": template.description,
                        "source_label": template.source_label,
                        "source_family_tag": template.source_family_tag,
                        "source_type": template.source_type,
                        "is_imported": True,
                        "session_date": template.session_date.isoformat(),
                        "session_status": template.session_status,
                        "session_summary": dict(template.session_summary),
                    }
                )
        for item in KAIROS_RUNNER_SCENARIOS.values():
            if item.key in seen_values:
                continue
            seen_values.add(item.key)
            options.append(
                {
                    "label": item.name,
                    "value": item.key,
                    "description": item.description,
                    "source_label": "Synthetic",
                    "source_family_tag": self.SYNTHETIC_SOURCE_FAMILY_TAG,
                    "source_type": self.SYNTHETIC_SOURCE_TYPE,
                    "is_imported": False,
                    "session_date": None,
                    "session_status": "complete",
                    "session_summary": {},
                }
            )
        return options

    def _build_historical_replay_controls_payload_locked(self) -> Dict[str, Any]:
        self._sync_persisted_scenarios_locked()
        provider_meta = self.market_data_service.get_provider_metadata()
        last_import = next(
            iter(
                sorted(
                    self._historical_replay_templates.values(),
                    key=lambda item: item.session_date,
                    reverse=True,
                )
            ),
            None,
        )
        schwab_available = provider_meta.get("live_provider_key") == "schwab"
        schwab_ready = bool(schwab_available and (not provider_meta.get("requires_auth") or provider_meta.get("authenticated")))
        return {
            "default_source": "schwab" if schwab_available else "polygon-json",
            "source_options": [dict(item) for item in self.HISTORICAL_REPLAY_SOURCE_OPTIONS],
            "schwab_available": schwab_available,
            "schwab_ready": schwab_ready,
            "schwab_provider_label": provider_meta.get("live_provider_name", "Unknown Provider"),
            "preferred_symbol": self.HISTORICAL_REPLAY_SYMBOL,
            "last_imported": last_import.to_payload() if last_import is not None else None,
            "imported_scenarios": [
                dict(item)
                for item in sorted(
                    self._historical_replay_catalog.values(),
                    key=lambda entry: (str(entry.get("session_date") or ""), str(entry.get("label") or "")),
                    reverse=True,
                )
            ],
        }

    def _coerce_historical_replay_source(self, value: Any) -> str:
        normalized = str(value or "auto").strip().lower()
        valid_values = {item["value"] for item in self.HISTORICAL_REPLAY_SOURCE_OPTIONS}
        return normalized if normalized in valid_values else "auto"

    @staticmethod
    def _coerce_historical_replay_date(value: Any) -> date:
        raw_value = str(value or "").strip()
        if not raw_value:
            raise ValueError("Choose a historical session date before importing a Kairos replay template.")
        try:
            return date.fromisoformat(raw_value)
        except ValueError as exc:
            raise ValueError("Historical replay dates must be provided in YYYY-MM-DD format.") from exc

    def _resolve_historical_replay_import(
        self,
        *,
        session_date: date,
        requested_source: str,
        raw_json: str,
    ) -> tuple[KairosTapeScenario, KairosHistoricalReplayTemplate]:
        failures: List[str] = []

        if requested_source in {"auto", "schwab"}:
            try:
                return self._import_historical_replay_from_schwab(session_date)
            except (ValueError, MarketDataError, MarketDataAuthenticationError, MarketDataReauthenticationRequired) as exc:
                if requested_source == "schwab":
                    raise
                failures.append(str(exc))

        if requested_source in {"auto", "polygon-json"}:
            if not raw_json:
                if requested_source == "polygon-json":
                    raise ValueError("Paste a Polygon / Massive-style JSON payload before using the JSON replay import.")
            else:
                try:
                    return self._import_historical_replay_from_json(session_date, raw_json)
                except ValueError as exc:
                    if requested_source == "polygon-json":
                        raise
                    failures.append(str(exc))

        if failures:
            raise ValueError("Schwab historical import was unavailable. Paste Polygon / Massive JSON to use the fallback path.")
        raise ValueError("No historical replay source could be resolved.")

    def _import_historical_replay_from_schwab(self, session_date: date) -> tuple[KairosTapeScenario, KairosHistoricalReplayTemplate]:
        provider_meta = self.market_data_service.get_provider_metadata()
        if provider_meta.get("live_provider_key") != "schwab":
            raise ValueError(
                f"Schwab historical replay import is unavailable because the active live provider is {provider_meta.get('live_provider_name', 'Unknown Provider')}."
            )
        if provider_meta.get("requires_auth") and not provider_meta.get("authenticated"):
            raise ValueError("Connect to Schwab before importing a historical Kairos replay day.")

        spy_frame = self.market_data_service.get_intraday_candles_for_date(
            self.HISTORICAL_REPLAY_SYMBOL,
            target_date=session_date,
            interval_minutes=1,
            query_type="kairos_replay_spy_day",
        )
        spy_bars = self._normalize_historical_intraday_frame(spy_frame, session_date=session_date, symbol=self.HISTORICAL_REPLAY_SYMBOL)
        spx_anchor = self._resolve_historical_replay_spx_anchor(session_date, spy_bars=spy_bars)
        replay_bars = self._rebase_historical_spy_bars_to_spx(spy_bars, spx_anchor=spx_anchor)

        vix_source_label = "Flat VIX fallback"
        try:
            vix_frame = self.market_data_service.get_intraday_candles_for_date(
                "^VIX",
                target_date=session_date,
                interval_minutes=1,
                query_type="kairos_replay_vix_day",
            )
            vix_series = self._normalize_historical_vix_frame(vix_frame, session_date=session_date)
            vix_source_label = "Schwab ^VIX 1m"
        except MarketDataError:
            vix_series = tuple(round(self._runtime.simulated_vix_value, 2) for _ in range(SESSION_BAR_COUNT))

        scenario = self._build_historical_replay_scenario(
            session_date=session_date,
            source_key="schwab",
            source_label="Schwab historical 1m",
            bars=replay_bars,
            vix_series=vix_series,
        )
        session_summary = self._summarize_session_metadata(
            [
                {
                    "open": bar.open,
                    "high": bar.high,
                    "low": bar.low,
                    "close": bar.close,
                    "volume": bar.volume,
                }
                for bar in replay_bars
            ],
            latest_scan=None,
            vix_open=vix_series[0] if vix_series else None,
        )
        template = KairosHistoricalReplayTemplate(
            scenario_key=scenario.key,
            scenario_name=scenario.name,
            session_date=session_date,
            source_key="schwab",
            source_label="Schwab historical 1m",
            symbol=self.HISTORICAL_REPLAY_SYMBOL,
            tape_source="Synthetic SPX rebased from SPY 1m",
            vix_source_label=vix_source_label,
            description=f"Real SPY session replay imported from Schwab and rebased to SPX for {session_date.isoformat()}.",
            source_type=self.REAL_IMPORT_SOURCE_TYPE,
            source_family_tag=self.REAL_IMPORT_SOURCE_FAMILY_TAG,
            session_type="Imported Real Day",
            session_status="complete",
            created_at=self._now().isoformat(),
            bar_count=len(replay_bars),
            market_session="Regular",
            replay_schema_version=self.REPLAY_SCHEMA_VERSION,
            storage_version=self.REPLAY_STORAGE_VERSION,
            session_summary=session_summary,
        )
        return scenario, template

    def _import_historical_replay_from_json(self, session_date: date, raw_json: str) -> tuple[KairosTapeScenario, KairosHistoricalReplayTemplate]:
        parsed = self._parse_polygon_json_payload(raw_json)
        spy_bars = self._normalize_historical_intraday_rows(parsed, session_date=session_date, symbol=self.HISTORICAL_REPLAY_SYMBOL)
        spx_anchor = self._resolve_historical_replay_spx_anchor(session_date, spy_bars=spy_bars)
        bars = self._rebase_historical_spy_bars_to_spx(spy_bars, spx_anchor=spx_anchor)
        vix_series = tuple(round(self._runtime.simulated_vix_value, 2) for _ in range(SESSION_BAR_COUNT))
        scenario = self._build_historical_replay_scenario(
            session_date=session_date,
            source_key="polygon-json",
            source_label="Polygon / Massive JSON",
            bars=bars,
            vix_series=vix_series,
        )
        session_summary = self._summarize_session_metadata(
            [
                {
                    "open": bar.open,
                    "high": bar.high,
                    "low": bar.low,
                    "close": bar.close,
                    "volume": bar.volume,
                }
                for bar in bars
            ],
            latest_scan=None,
            vix_open=vix_series[0] if vix_series else None,
        )
        template = KairosHistoricalReplayTemplate(
            scenario_key=scenario.key,
            scenario_name=scenario.name,
            session_date=session_date,
            source_key="polygon-json",
            source_label="Polygon / Massive JSON",
            symbol=self.HISTORICAL_REPLAY_SYMBOL,
            tape_source="Synthetic SPX rebased from SPY 1m",
            vix_source_label="Flat VIX fallback",
            description=f"Real SPY session replay imported from Polygon / Massive JSON and rebased to SPX for {session_date.isoformat()}.",
            source_type=self.REAL_IMPORT_SOURCE_TYPE,
            source_family_tag=self.REAL_IMPORT_SOURCE_FAMILY_TAG,
            session_type="Imported Real Day",
            session_status="complete",
            created_at=self._now().isoformat(),
            bar_count=len(bars),
            market_session="Regular",
            replay_schema_version=self.REPLAY_SCHEMA_VERSION,
            storage_version=self.REPLAY_STORAGE_VERSION,
            session_summary=session_summary,
        )
        return scenario, template

    def _resolve_historical_replay_spx_anchor(self, session_date: date, *, spy_bars: List[KairosTapeBar]) -> float:
        try:
            historical_bar = self.market_data_service.get_single_day_bar(
                self.HISTORICAL_REPLAY_TARGET_SYMBOL,
                target_date=session_date,
                query_type="kairos_replay_spx_anchor",
            )
            historical_open = self._coerce_float(historical_bar.get("Open"), fallback=0.0)
            if historical_open > 0:
                return round(historical_open, 2)
        except MarketDataError:
            pass

        snapshot = self._safe_latest_snapshot(self.HISTORICAL_REPLAY_TARGET_SYMBOL, query_type="kairos_replay_spx_anchor_snapshot")
        snapshot_anchor = coerce_snapshot_number(snapshot, "Latest Value")
        if snapshot_anchor is not None and snapshot_anchor > 0:
            return round(snapshot_anchor, 2)

        if self._runtime.simulated_spx_value > 0:
            return round(self._runtime.simulated_spx_value, 2)

        if spy_bars and spy_bars[0].open > 0:
            return round(spy_bars[0].open, 2)

        raise ValueError(f"Unable to resolve an SPX anchor for the {session_date.isoformat()} replay import.")

    def _rebase_historical_spy_bars_to_spx(self, spy_bars: List[KairosTapeBar], *, spx_anchor: float) -> List[KairosTapeBar]:
        if not spy_bars:
            raise ValueError("Historical replay import requires regular-session SPY bars before rebasing to SPX.")
        spy_anchor = round(spy_bars[0].open, 8)
        if spy_anchor <= 0:
            raise ValueError("Historical replay import requires a positive SPY opening price to rebase into SPX space.")

        rebased_bars: List[KairosTapeBar] = []
        for bar in spy_bars:
            rebased_bars.append(
                KairosTapeBar(
                    bar_index=bar.bar_index,
                    minute_offset=bar.minute_offset,
                    simulated_time=bar.simulated_time,
                    open=round(spx_anchor * (bar.open / spy_anchor), 2),
                    high=round(spx_anchor * (bar.high / spy_anchor), 2),
                    low=round(spx_anchor * (bar.low / spy_anchor), 2),
                    close=round(spx_anchor * (bar.close / spy_anchor), 2),
                    volume=bar.volume,
                )
            )
        return rebased_bars

    def _build_historical_replay_scenario(
        self,
        *,
        session_date: date,
        source_key: str,
        source_label: str,
        bars: List[KairosTapeBar],
        vix_series: tuple[float, ...],
    ) -> KairosTapeScenario:
        scenario_key = f"historical-spy-{session_date.isoformat()}-{source_key}".lower()
        scenario = KairosTapeScenario(
            key=scenario_key,
            name=f"SPY Real Day {session_date.isoformat()}",
            description=f"Regular-session SPY 1-minute replay imported from {source_label} and rebased into synthetic SPX tape for {session_date.isoformat()}.",
            bars=tuple(bars),
            vix_series=vix_series,
        )
        validate_tape_scenario(scenario)
        return scenario

    def _normalize_historical_intraday_frame(self, frame: pd.DataFrame, *, session_date: date, symbol: str) -> List[KairosTapeBar]:
        rows = frame.to_dict(orient="records") if frame is not None else []
        return self._normalize_historical_intraday_rows(rows, session_date=session_date, symbol=symbol)

    def _normalize_historical_intraday_rows(self, rows: List[Dict[str, Any]], *, session_date: date, symbol: str) -> List[KairosTapeBar]:
        session_anchor = datetime.combine(session_date, time(8, 30), tzinfo=self.display_timezone)
        row_by_offset: Dict[int, Dict[str, Any]] = {}

        for row in rows:
            candle_time = self._coerce_historical_replay_timestamp(
                row.get("Datetime")
                or row.get("datetime")
                or row.get("t")
                or row.get("timestamp")
            )
            if candle_time is None:
                continue
            local_time = candle_time.astimezone(self.display_timezone)
            minute_offset = int((local_time - session_anchor).total_seconds() // 60)
            if minute_offset < 0 or minute_offset >= SESSION_BAR_COUNT:
                continue
            row_by_offset[minute_offset] = {
                "open": self._coerce_float(row.get("Open") if "Open" in row else row.get("open") if "open" in row else row.get("o"), fallback=0.0),
                "high": self._coerce_float(row.get("High") if "High" in row else row.get("high") if "high" in row else row.get("h"), fallback=0.0),
                "low": self._coerce_float(row.get("Low") if "Low" in row else row.get("low") if "low" in row else row.get("l"), fallback=0.0),
                "close": self._coerce_float(row.get("Close") if "Close" in row else row.get("close") if "close" in row else row.get("c"), fallback=0.0),
                "volume": int(max(1, round(self._coerce_float(row.get("Volume") if "Volume" in row else row.get("volume") if "volume" in row else row.get("v"), fallback=1.0)))),
            }

        missing_offsets = [offset for offset in range(SESSION_BAR_COUNT) if offset not in row_by_offset]
        if missing_offsets:
            raise ValueError(
                f"{symbol} historical replay import requires exactly {SESSION_BAR_COUNT} regular-session 1-minute bars. "
                f"Missing {len(missing_offsets)} bars after filtering extended hours for {session_date.isoformat()}."
            )

        bars: List[KairosTapeBar] = []
        for offset in range(SESSION_BAR_COUNT):
            candle = row_by_offset[offset]
            bar_time = (session_anchor + timedelta(minutes=offset)).time()
            bars.append(
                KairosTapeBar(
                    bar_index=offset,
                    minute_offset=offset,
                    simulated_time=bar_time,
                    open=round(candle["open"], 2),
                    high=round(candle["high"], 2),
                    low=round(candle["low"], 2),
                    close=round(candle["close"], 2),
                    volume=candle["volume"],
                )
            )
        return bars

    def _normalize_historical_vix_frame(self, frame: pd.DataFrame, *, session_date: date) -> tuple[float, ...]:
        session_anchor = datetime.combine(session_date, time(8, 30), tzinfo=self.display_timezone)
        values_by_offset: Dict[int, float] = {}
        for row in frame.to_dict(orient="records"):
            candle_time = self._coerce_historical_replay_timestamp(row.get("Datetime") or row.get("datetime") or row.get("t"))
            if candle_time is None:
                continue
            local_time = candle_time.astimezone(self.display_timezone)
            minute_offset = int((local_time - session_anchor).total_seconds() // 60)
            if 0 <= minute_offset < SESSION_BAR_COUNT:
                values_by_offset[minute_offset] = round(self._coerce_float(row.get("Close") if "Close" in row else row.get("close") if "close" in row else row.get("c"), fallback=self._runtime.simulated_vix_value), 2)

        if len(values_by_offset) != SESSION_BAR_COUNT:
            raise MarketDataError("Schwab did not return a full regular-session ^VIX minute series for this day.")
        return tuple(values_by_offset[offset] for offset in range(SESSION_BAR_COUNT))

    def _parse_polygon_json_payload(self, raw_json: str) -> List[Dict[str, Any]]:
        try:
            payload = json.loads(raw_json)
        except json.JSONDecodeError as exc:
            raise ValueError("Polygon / Massive JSON replay import could not be parsed.") from exc

        if isinstance(payload, dict):
            candidates = payload.get("results")
            if isinstance(candidates, list):
                return [item for item in candidates if isinstance(item, dict)]
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        raise ValueError("Polygon / Massive replay JSON must be a list of aggregate bars or a dict with a results array.")

    def _coerce_historical_replay_timestamp(self, value: Any) -> datetime | None:
        if value in (None, ""):
            return None
        if isinstance(value, datetime):
            return value if value.tzinfo is not None else value.replace(tzinfo=self.display_timezone)
        if isinstance(value, (int, float)):
            epoch_value = float(value)
            if epoch_value > 1_000_000_000_000:
                epoch_value /= 1000.0
            return datetime.fromtimestamp(epoch_value, tz=ZoneInfo("UTC"))
        raw_value = str(value).strip()
        try:
            parsed = datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
        except ValueError:
            return None
        return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=self.display_timezone)

    def _current_runner_tape_source_locked(self, scenario: KairosTapeScenario | None = None) -> str:
        active_scenario = scenario or self._current_runner_scenario_locked()
        if active_scenario is None:
            return self.RUNNER_TAPE_SOURCE
        imported_template = self._historical_replay_templates.get(active_scenario.key)
        if imported_template is not None:
            return imported_template.tape_source
        return self.RUNNER_TAPE_SOURCE

    def _build_anchored_runner_scenario_locked(self, scenario: KairosTapeScenario) -> KairosTapeScenario:
        spx_snapshot = self._safe_latest_snapshot("^GSPC", query_type="kairos_latest_spx")
        vix_snapshot = self._safe_latest_snapshot("^VIX", query_type="kairos_latest_vix")
        spx_anchor = coerce_snapshot_number(spx_snapshot, "Latest Value")
        vix_anchor = coerce_snapshot_number(vix_snapshot, "Latest Value")

        base_open = scenario.bars[0].open if scenario.bars else 0.0
        price_shift = (spx_anchor - base_open) if spx_anchor is not None else 0.0
        anchored_vix = round(vix_anchor if vix_anchor is not None else scenario.vix_series[0], 2)

        anchored_bars = tuple(
            KairosTapeBar(
                bar_index=bar.bar_index,
                minute_offset=bar.minute_offset,
                simulated_time=bar.simulated_time,
                open=round(bar.open + price_shift, 2),
                high=round(bar.high + price_shift, 2),
                low=round(bar.low + price_shift, 2),
                close=round(bar.close + price_shift, 2),
                volume=bar.volume,
            )
            for bar in scenario.bars
        )
        anchored_vix_series = tuple(anchored_vix for _ in scenario.vix_series)

        return KairosTapeScenario(
            key=scenario.key,
            name=scenario.name,
            description=scenario.description,
            bars=anchored_bars,
            vix_series=anchored_vix_series,
            checkpoints=scenario.checkpoints,
        )

    def _append_runner_event_locked(self, now: datetime, simulated_time_label: str, title: str, detail: str) -> None:
        self._runner.event_log.insert(
            0,
            KairosRunnerEvent(
                timestamp=now,
                simulated_time_label=simulated_time_label or "—",
                title=title,
                detail=detail,
            ),
        )
        self._runner.event_log = self._runner.event_log[: self.RUNNER_EVENT_LOG_LIMIT]

    def _normalize_runner_pause_filters(self, payload: Dict[str, Any] | None) -> List[str]:
        payload = payload or {}
        raw_filters = payload.get("pause_events")
        if raw_filters is None:
            return []
        if isinstance(raw_filters, str):
            candidates = [item.strip().lower() for item in raw_filters.split(",") if item.strip()]
        elif isinstance(raw_filters, (list, tuple, set)):
            candidates = [str(item).strip().lower() for item in raw_filters if str(item).strip()]
        else:
            candidates = [str(raw_filters).strip().lower()]
        valid_values = {item["value"] for item in self.RUNNER_PAUSE_EVENT_OPTIONS}
        normalized: List[str] = []
        for value in candidates:
            if value in valid_values and value not in normalized:
                normalized.append(value)
        return normalized

    def _resolve_runner_event_pause_locked(self, latest_scan: KairosScanResult | None) -> Dict[str, str] | None:
        if self._runner.active_trade_lock_in is not None:
            return None
        if latest_scan is None or not self._runner.pause_event_filters:
            return None

        marker_type = self._resolve_bar_map_marker_type(latest_scan)
        if marker_type is None:
            return None

        marker_kind = marker_type["kind"]
        if marker_kind == "window-open":
            return None
        if marker_kind not in self._runner.pause_event_filters:
            return None
        if marker_kind == self._runner.suppressed_pause_event_kind:
            return None

        return {
            "reason_key": f"pause_on_{marker_kind}",
            "event_kind": marker_kind,
            "event_label": marker_type["label"],
            "detail": latest_scan.summary_text,
        }

    def _resolve_runner_trade_decision_pause_locked(
        self,
        latest_scan: KairosScanResult | None,
        metrics: Dict[str, Any] | None,
    ) -> Dict[str, str] | None:
        if self._runner.active_trade_lock_in is not None:
            return None
        if latest_scan is None or latest_scan.kairos_state != KairosState.WINDOW_OPEN.value:
            return None
        if metrics is None or metrics.get("current_bar") is None:
            return None
        if self._runner.suppressed_pause_event_kind == "window-open":
            return None

        candidate_context = self._build_runner_trade_candidate_context_locked(latest_scan, metrics)
        if candidate_context is None:
            return None

        ready_count = sum(1 for item in candidate_context["profiles"] if item.get("available"))
        if ready_count <= 0:
            return {
                "reason_key": "awaiting_user_continue",
                "event_kind": "awaiting-user-continue",
                "event_label": "Awaiting User Continue",
                "detail": "Prime was reached, but no qualified candidates are available. Continue the replay to let the session develop further.",
            }

        detail = (
            f"Qualified Kairos trade candidates are available ({ready_count} ready). Review the candidate card to take or skip the setup."
            if ready_count
            else "Prime was reached, but no qualified candidates are available. Continue the replay to let the session develop further."
        )

        return {
            "reason_key": "pause_on_window-open",
            "event_kind": "window-open",
            "event_label": "Prime",
            "detail": detail,
        }

    def _settle_active_trade_lock_in_locked(self) -> None:
        trade_lock_in = self._runner.active_trade_lock_in
        scenario = self._current_runner_scenario_locked()
        if trade_lock_in is None or scenario is None or trade_lock_in.final_spx_close is not None or not scenario.bars:
            return

        final_spx_close = round(scenario.bars[-1].close, 2)
        spread_width_dollars = float(trade_lock_in.width * 100 * trade_lock_in.contracts)
        intrinsic_value = max(float(trade_lock_in.short_strike) - final_spx_close, 0.0)
        capped_loss_value = min(float(trade_lock_in.width), intrinsic_value)
        exercise_cost = round(capped_loss_value * 100.0 * trade_lock_in.remaining_contracts, 2)
        simulated_pnl = round(trade_lock_in.estimated_total_premium_received - trade_lock_in.realized_exit_debit_dollars - exercise_cost, 2)

        if final_spx_close <= trade_lock_in.long_strike:
            result_status = "Black Swan"
            result_tone = "black-swan"
            settlement_note = "Classified as Black Swan because SPX finished through the long put strike and the spread settled at maximum loss."
            trade_lock_in.exit_status_summary = "Max Loss / Long Touch"
            trade_lock_in.expired = False
            trade_lock_in.fully_closed = True
        elif simulated_pnl < 0:
            result_status = "Loss"
            result_tone = "loss"
            settlement_note = "Classified as Loss because SPX finished between the strikes and intrinsic value exceeded the modeled credit retained."
            trade_lock_in.exit_status_summary = "Other Exit"
            trade_lock_in.expired = False
            trade_lock_in.fully_closed = True
        else:
            result_status = "Win"
            result_tone = "win"
            settlement_note = "Classified as Win because the modeled credit held through settlement without a net loss."
            trade_lock_in.exit_status_summary = "Expired Worthless"
            trade_lock_in.expired = True
            trade_lock_in.fully_closed = True

        trade_lock_in.final_spx_close = final_spx_close
        trade_lock_in.result_status = result_status
        trade_lock_in.simulated_pnl_dollars = max(-spread_width_dollars, simulated_pnl)
        trade_lock_in.current_credit = get_current_credit(trade_lock_in)
        trade_lock_in.settlement_note = settlement_note
        trade_lock_in.result_tone = result_tone
        trade_lock_in.settlement_badge_label = f"Settled - {result_status} ({format_signed_currency_whole(trade_lock_in.simulated_pnl_dollars)})"
        trade_lock_in.closed_contracts = trade_lock_in.contracts
        trade_lock_in.remaining_contracts = 0
        trade_lock_in.exit_events.append(
            self._build_trade_event(
                event_key="sim-settlement",
                label="Expired Worthless" if trade_lock_in.expired else "Max Loss" if result_status == "Black Swan" else "Settlement",
                kind="trade-expired" if trade_lock_in.expired else "trade-max-loss" if result_status == "Black Swan" else "trade-full-close",
                time_label=self._runner.simulated_time_label,
                current_spx=final_spx_close,
                bar_number=len(scenario.bars),
                timestamp=self._now(),
                detail=settlement_note,
            )
        )
        self._append_runner_event_locked(
            self._now(),
            self._runner.simulated_time_label,
            "Simulated trade settled",
            f"Final SPX {final_spx_close:.2f}. Result: {trade_lock_in.result_status} with P/L {trade_lock_in.simulated_pnl_dollars:.2f}.",
        )

    def _update_active_sim_trade_status_locked(self, metrics: Dict[str, Any] | None) -> None:
        trade_lock_in = self._runner.active_trade_lock_in
        if trade_lock_in is None or trade_lock_in.final_spx_close is not None or metrics is None or metrics.get("current_bar") is None:
            return

        current_spx = float(metrics.get("current_close") or 0.0)
        current_bar = metrics["current_bar"]
        current_time_label = format_simulated_clock(current_bar.simulated_time)
        trade_lock_in.current_spx = round(current_spx, 2)
        trade_lock_in.current_credit = get_current_credit(trade_lock_in)
        exit_debit_per_contract = self._estimate_simulated_exit_debit_per_contract_locked(trade_lock_in, current_spx=current_spx, metrics=metrics)
        trade_lock_in.current_exit_debit_per_contract = exit_debit_per_contract
        trade_lock_in.current_unrealized_close_cost = round(exit_debit_per_contract * trade_lock_in.remaining_contracts, 2)
        trade_lock_in.exit_status_summary = self._evaluate_kairos_exit_state_locked(trade_lock_in, current_spx, current_time_label)

    def _update_live_trade_management_for_scan_locked(self, now: datetime, scan_result: KairosScanResult) -> None:
        trade_lock_in = self._live_active_trade
        if trade_lock_in is None or trade_lock_in.final_spx_close is not None:
            return

        current_spx, current_vwap, current_time_label, structure_status, time_remaining_ratio, current_bar_number = self._build_live_trade_management_context_locked(now)
        self._update_trade_mark_to_market_locked(
            trade_lock_in,
            current_spx=current_spx,
            current_time_label=current_time_label,
            current_vwap=current_vwap,
            structure_status=structure_status,
            time_remaining_ratio=time_remaining_ratio,
            current_bar_number=current_bar_number,
        )
        if trade_lock_in.pending_exit_recommendation is not None:
            self._session.status_note = trade_lock_in.pending_exit_recommendation.get("note") or self._session.status_note
        if scan_result.is_expired or self._get_effective_market_session_status(now) == "Ended":
            self._settle_live_trade_locked(now, current_spx=current_spx, current_time_label=current_time_label)

    def _settle_live_trade_locked(self, now: datetime, *, current_spx: float, current_time_label: str) -> None:
        trade_lock_in = self._live_active_trade
        if trade_lock_in is None or trade_lock_in.final_spx_close is not None:
            return

        final_spx_close = round(current_spx, 2)
        spread_width_dollars = float(trade_lock_in.width * 100 * trade_lock_in.contracts)
        intrinsic_value = max(float(trade_lock_in.short_strike) - final_spx_close, 0.0)
        capped_loss_value = min(float(trade_lock_in.width), intrinsic_value)
        exercise_cost = round(capped_loss_value * 100.0 * trade_lock_in.remaining_contracts, 2)
        realized_pnl = round(trade_lock_in.original_credit - trade_lock_in.realized_close_cost_total - exercise_cost, 2)

        trade_lock_in.final_spx_close = final_spx_close
        trade_lock_in.closed_contracts = trade_lock_in.contracts
        trade_lock_in.remaining_contracts = 0
        trade_lock_in.fully_closed = True
        trade_lock_in.simulated_pnl_dollars = max(-spread_width_dollars, realized_pnl)
        trade_lock_in.current_credit = get_current_credit(trade_lock_in)

        if final_spx_close <= trade_lock_in.long_strike:
            trade_lock_in.result_status = "Black Swan"
            trade_lock_in.result_tone = "black-swan"
            trade_lock_in.exit_status_summary = "Max Loss / Long Touch"
            trade_lock_in.settlement_note = "Live Kairos trade finished through the long strike and is marked as maximum loss."
            trade_lock_in.exit_events.append(
                self._build_trade_event(
                    event_key="live-max-loss",
                    label="Max Loss",
                    kind="trade-max-loss",
                    time_label=current_time_label,
                    current_spx=final_spx_close,
                    bar_number=max(1, self._session.total_scans_completed),
                    timestamp=now,
                    detail=trade_lock_in.settlement_note,
                )
            )
        else:
            trade_lock_in.result_status = "Win" if trade_lock_in.simulated_pnl_dollars >= 0 else "Loss"
            trade_lock_in.result_tone = "win" if trade_lock_in.simulated_pnl_dollars >= 0 else "loss"
            trade_lock_in.expired = final_spx_close > trade_lock_in.short_strike
            trade_lock_in.exit_status_summary = "Expired Worthless" if trade_lock_in.expired else "Other Exit"
            trade_lock_in.settlement_note = (
                "Live Kairos trade expired worthless at the session close."
                if trade_lock_in.expired
                else "Live Kairos trade finished in the money at the session close."
            )
            trade_lock_in.exit_events.append(
                self._build_trade_event(
                    event_key="live-expiration" if trade_lock_in.expired else "live-final-close",
                    label="Expired Worthless" if trade_lock_in.expired else "Session Close",
                    kind="trade-expired" if trade_lock_in.expired else "trade-full-close",
                    time_label=current_time_label,
                    current_spx=final_spx_close,
                    bar_number=max(1, self._session.total_scans_completed),
                    timestamp=now,
                    detail=trade_lock_in.settlement_note,
                )
            )

    def _evaluate_kairos_exit_state_locked(
        self,
        trade_lock_in: KairosSimulatedTradeLockIn,
        current_spx: float,
        current_time_label: str,
    ) -> str:
        if current_spx <= trade_lock_in.short_strike and trade_lock_in.short_strike_touched_at is None:
            trade_lock_in.short_strike_touched_at = current_time_label
        if current_spx <= trade_lock_in.long_strike and trade_lock_in.long_strike_touched_at is None:
            trade_lock_in.long_strike_touched_at = current_time_label

        if trade_lock_in.long_strike_touched_at is not None:
            return "Max Loss / Long Touch"
        if trade_lock_in.pending_exit_recommendation is not None:
            return trade_lock_in.pending_exit_recommendation.get("title", "Exit Gate Pending")
        if trade_lock_in.pending_exit_gate_key is not None:
            pending_gate = trade_lock_in.exit_gate_states.get(trade_lock_in.pending_exit_gate_key, {})
            return pending_gate.get("label", "Exit Gate Pending")
        if trade_lock_in.closed_early:
            return "Closed Early"
        if trade_lock_in.final_spx_close is not None and trade_lock_in.expired:
            return "Expired Worthless"
        if trade_lock_in.final_spx_close is not None and trade_lock_in.fully_closed:
            return trade_lock_in.exit_status_summary or "Other Exit"
        for gate_key in ("long-strike-touch", "short-strike-proximity", "below-vwap-failed-reclaim", "structure-break"):
            gate_state = trade_lock_in.exit_gate_states.get(gate_key)
            if gate_state and gate_state.get("status") == "accepted":
                return gate_state.get("label", "Exit Managed")
        return "Active"

    def _build_sim_trade_exit_snapshot_locked(self, metrics: Dict[str, Any] | None) -> Dict[str, Any]:
        trade_lock_in = self._runner.active_trade_lock_in
        if trade_lock_in is None:
            return {"visible": False}

        current_spx = float(metrics.get("current_close") or trade_lock_in.current_spx) if metrics is not None else trade_lock_in.current_spx
        current_time_label = trade_lock_in.simulated_time_label
        if metrics is not None and metrics.get("current_bar") is not None:
            current_time_label = format_simulated_clock(metrics["current_bar"].simulated_time)

        distance_to_short_strike = round(current_spx - float(trade_lock_in.short_strike), 2)
        trade_status = "Expired" if trade_lock_in.expired else "Fully Closed" if trade_lock_in.fully_closed else "Active"
        return {
            "visible": True,
            "trade_status": trade_status,
            "original_credit": round(trade_lock_in.original_credit or trade_lock_in.estimated_total_premium_received, 2),
            "original_credit_display": format_currency_value(trade_lock_in.original_credit or trade_lock_in.estimated_total_premium_received),
            "current_credit": round(get_current_credit(trade_lock_in), 2),
            "current_credit_display": format_currency_value(get_current_credit(trade_lock_in)),
            "estimated_otm_probability_display": format_percent_value(trade_lock_in.estimated_otm_probability),
            "current_simulated_time": current_time_label,
            "distance_to_short_strike": distance_to_short_strike,
            "distance_to_short_strike_display": f"{distance_to_short_strike:.2f} pts",
            "current_spx": round(current_spx, 2),
            "current_spx_display": format_number_value(current_spx),
            "short_strike": trade_lock_in.short_strike,
            "long_strike": trade_lock_in.long_strike,
            "remaining_contracts": trade_lock_in.remaining_contracts,
            "closed_contracts": trade_lock_in.closed_contracts,
            "realized_close_cost_total_display": format_currency_value(trade_lock_in.realized_close_cost_total or trade_lock_in.realized_exit_debit_dollars),
            "realized_exit_debit_display": format_currency_value(trade_lock_in.realized_exit_debit_dollars),
            "retained_credit_display": format_currency_value(get_current_credit(trade_lock_in)),
            "current_exit_debit_per_contract_display": format_currency_value(trade_lock_in.current_exit_debit_per_contract),
            "realized_status_summary": trade_lock_in.exit_status_summary,
            "partially_reduced": trade_lock_in.partially_reduced,
            "fully_closed": trade_lock_in.fully_closed,
            "expired": trade_lock_in.expired,
            "short_strike_touched_at": trade_lock_in.short_strike_touched_at,
            "long_strike_touched_at": trade_lock_in.long_strike_touched_at,
            "exit_management_note": trade_lock_in.exit_management_note,
            "exit_gate_states": [
                {
                    "label": item.get("label", key),
                    "status": item.get("status", "pending"),
                    "status_display": str(item.get("status", "pending")).replace("-", " ").title(),
                    "contracts": item.get("contracts", 0),
                }
                for key, item in trade_lock_in.exit_gate_states.items()
            ],
        }

    def _build_sim_trade_exit_prompt_locked(self, metrics: Dict[str, Any] | None) -> Dict[str, Any]:
        trade_lock_in = self._runner.active_trade_lock_in
        if trade_lock_in is None or trade_lock_in.pending_exit_recommendation is None:
            return {"visible": False}

        return dict(trade_lock_in.pending_exit_recommendation)

    def _build_live_trade_exit_snapshot_locked(self, now: datetime) -> Dict[str, Any]:
        trade_lock_in = self._live_active_trade
        if trade_lock_in is None:
            return {"visible": False}

        current_spx = trade_lock_in.current_spx
        current_time_label = format_display_time(now, self.display_timezone)
        distance_to_short_strike = round(current_spx - float(trade_lock_in.short_strike), 2)
        trade_status = "Expired" if trade_lock_in.expired else "Fully Closed" if trade_lock_in.fully_closed else "Active"
        return {
            "visible": True,
            "trade_status": trade_status,
            "original_credit_display": format_currency_value(trade_lock_in.original_credit or trade_lock_in.estimated_total_premium_received),
            "current_credit_display": format_currency_value(get_current_credit(trade_lock_in)),
            "estimated_otm_probability_display": format_percent_value(trade_lock_in.estimated_otm_probability),
            "current_simulated_time": current_time_label,
            "distance_to_short_strike_display": f"{distance_to_short_strike:.2f} pts",
            "current_spx_display": format_number_value(current_spx),
            "short_strike": trade_lock_in.short_strike,
            "long_strike": trade_lock_in.long_strike,
            "remaining_contracts": trade_lock_in.remaining_contracts,
            "closed_contracts": trade_lock_in.closed_contracts,
            "realized_close_cost_total_display": format_currency_value(trade_lock_in.realized_close_cost_total or trade_lock_in.realized_exit_debit_dollars),
            "realized_exit_debit_display": format_currency_value(trade_lock_in.realized_exit_debit_dollars),
            "retained_credit_display": format_currency_value(get_current_credit(trade_lock_in)),
            "current_exit_debit_per_contract_display": format_currency_value(trade_lock_in.current_exit_debit_per_contract),
            "realized_status_summary": trade_lock_in.exit_status_summary,
            "partially_reduced": trade_lock_in.partially_reduced,
            "fully_closed": trade_lock_in.fully_closed,
            "expired": trade_lock_in.expired,
            "exit_management_note": trade_lock_in.exit_management_note,
        }

    def _build_live_trade_exit_prompt_locked(self, now: datetime) -> Dict[str, Any]:
        trade_lock_in = self._live_active_trade
        if trade_lock_in is None or trade_lock_in.pending_exit_recommendation is None:
            return {"visible": False}
        return dict(trade_lock_in.pending_exit_recommendation)

    def _resume_simulation_runner_locked(
        self,
        now: datetime,
        *,
        suppress_repeats: bool,
        resume_detail_override: str | None = None,
    ) -> None:
        pause_marker_kind = self._runner_pause_marker_kind_locked()
        self._runner.status = KairosRunnerStatus.RUNNING
        self._runner.last_advanced_at = now
        self._runner.pause_reason = None
        self._runner.pause_detail = ""
        self._runner.candidate_event_id = None
        self._runner.best_trade_override_requested = False
        if suppress_repeats and pause_marker_kind is not None:
            self._runner.suppressed_pause_event_kind = pause_marker_kind
            self._runner.suppressed_pause_event_label = self._runner.pause_event_label
        elif not suppress_repeats:
            self._runner.suppressed_pause_event_kind = None
            self._runner.suppressed_pause_event_label = None
        self._runner.pause_event_label = None
        self._runner.pause_bar_number = None
        self._runner.pause_simulated_time = None
        resume_detail = resume_detail_override or "The scripted day progression is running again."
        if resume_detail_override is None and self._runner.suppressed_pause_event_label:
            resume_detail = f"Repeated {self._runner.suppressed_pause_event_label} pauses are suppressed until the state changes."
        self._append_runner_event_locked(now, self._runner.simulated_time_label, "Runner resumed", resume_detail)
        if not self._runner.last_candidate_action_note:
            self._session.status_note = "Kairos full-day runner resumed."

    def _runner_pause_event_selected_locked(self, event_kind: str) -> bool:
        return event_kind in self._runner.pause_event_filters

    def _has_valid_runner_pause_state_locked(self) -> bool:
        if self._runner.status != KairosRunnerStatus.PAUSED or not self._runner.pause_reason:
            return False
        if self._runner.pause_reason == "manual_pause":
            return True
        if self._runner.pause_reason == "awaiting_user_continue":
            return True
        if self._runner.pause_reason == "pause_on_window-open":
            return True
        if self._runner.pause_reason.startswith("pause_on_"):
            return self._runner.pause_reason.removeprefix("pause_on_") in self._runner.pause_event_filters
        return self._runner.pause_reason == "error"

    def _runner_pause_marker_kind_locked(self) -> str | None:
        pause_reason = self._runner.pause_reason or ""
        if pause_reason.startswith("pause_on_"):
            return pause_reason.removeprefix("pause_on_")
        return None

    def _clear_runner_pause_suppression_if_needed_locked(self, latest_scan: KairosScanResult | None) -> None:
        suppressed_kind = self._runner.suppressed_pause_event_kind
        if suppressed_kind is None:
            return
        marker_type = self._resolve_bar_map_marker_type(latest_scan) if latest_scan is not None else None
        current_kind = marker_type["kind"] if marker_type is not None else None
        if current_kind == suppressed_kind:
            return
        self._runner.suppressed_pause_event_kind = None
        self._runner.suppressed_pause_event_label = None

    def _apply_runner_bar_to_runtime_locked(self, scenario: KairosTapeScenario, bar: KairosTapeBar, metrics: Dict[str, Any]) -> None:
        timing_status = self._evaluate_timing_status_for_time(bar.simulated_time, "Open")
        structure_status = self._derive_runner_structure_status(metrics, timing_status)
        momentum_status = self._derive_runner_momentum_status(metrics, timing_status, structure_status)
        self._runtime.mode = KairosMode.SIMULATION
        self._runtime.simulation_scenario_name = scenario.name
        self._runtime.simulated_market_session_status = "Open"
        self._runtime.simulated_spx_value = bar.close
        self._runtime.simulated_vix_value = metrics["current_vix"]
        self._runtime.simulated_timing_status = timing_status
        self._runtime.simulated_structure_status = structure_status
        self._runtime.simulated_momentum_status = momentum_status
        self._runner.current_step_label = f"Bar {bar.bar_index + 1} / {len(scenario.bars)}"
        self._runner.current_step_note = f"{format_simulated_clock(bar.simulated_time)} tape playback in {self._runner.run_mode} mode."

    def _build_runner_metrics_locked(self, scenario: KairosTapeScenario, bar_index: int) -> Dict[str, Any]:
        if bar_index < 0:
            return {
                "current_close": 0.0,
                "session_high": 0.0,
                "session_low": 0.0,
                "vwap": 0.0,
                "current_vix": 0.0,
                "session_posture": "Awaiting tape",
                "net_change_percent": 0.0,
                "close_vs_vwap_percent": 0.0,
                "recent_change_percent": 0.0,
                "current_bar": None,
                "cumulative_volume": 0,
            }

        tape_slice = scenario.bars[: bar_index + 1]
        current_bar = tape_slice[-1]
        session_open = scenario.bars[0].open
        session_high = max(item.high for item in tape_slice)
        session_low = min(item.low for item in tape_slice)
        cumulative_volume = sum(item.volume for item in tape_slice)
        vwap_numerator = sum((((item.high + item.low + item.close) / 3.0) * item.volume) for item in tape_slice)
        vwap = vwap_numerator / cumulative_volume if cumulative_volume > 0 else current_bar.close
        recent_anchor = tape_slice[max(0, len(tape_slice) - 6)]
        net_change_percent = percent_change(current_bar.close, session_open)
        close_vs_vwap_percent = percent_change(current_bar.close, vwap)
        recent_change_percent = percent_change(current_bar.close, recent_anchor.close)
        session_posture = self._derive_runner_session_posture(
            net_change_percent=net_change_percent,
            close_vs_vwap_percent=close_vs_vwap_percent,
            recent_change_percent=recent_change_percent,
        )
        return {
            "current_close": current_bar.close,
            "session_high": session_high,
            "session_low": session_low,
            "vwap": vwap,
            "current_vix": scenario.vix_series[bar_index],
            "session_posture": session_posture,
            "net_change_percent": net_change_percent,
            "close_vs_vwap_percent": close_vs_vwap_percent,
            "recent_change_percent": recent_change_percent,
            "current_bar": current_bar,
            "cumulative_volume": cumulative_volume,
        }

    def _derive_runner_session_posture(self, *, net_change_percent: float, close_vs_vwap_percent: float, recent_change_percent: float) -> str:
        if net_change_percent >= 0.18 and close_vs_vwap_percent > 0.05 and recent_change_percent >= -0.02:
            return "Trend Up Above VWAP"
        if net_change_percent <= -0.18 and close_vs_vwap_percent < -0.05 and recent_change_percent <= 0.02:
            return "Trend Down Below VWAP"
        return "Balanced / Chop"

    def _derive_runner_structure_status(self, metrics: Dict[str, Any], timing_status: str) -> str:
        net_change_percent = metrics["net_change_percent"]
        close_vs_vwap_percent = metrics["close_vs_vwap_percent"]
        recent_change_percent = metrics["recent_change_percent"]
        if timing_status == "Closed":
            return "Failed" if net_change_percent < -0.35 else "Weakening"
        if net_change_percent <= -0.45 and close_vs_vwap_percent < -0.08:
            return "Failed"
        if net_change_percent <= -0.12 or close_vs_vwap_percent < -0.05:
            return "Weakening"
        if abs(net_change_percent) < 0.08 and abs(close_vs_vwap_percent) < 0.04:
            return "Chop / Unclear"
        if net_change_percent >= 0.18 and close_vs_vwap_percent > 0.05 and recent_change_percent >= -0.02:
            return "Bullish Confirmation"
        return "Developing"

    def _derive_runner_momentum_status(self, metrics: Dict[str, Any], timing_status: str, structure_status: str) -> str:
        recent_change_percent = metrics["recent_change_percent"]
        close_vs_vwap_percent = metrics["close_vs_vwap_percent"]
        if timing_status == "Closed" or structure_status == "Failed":
            return "Expired"
        if recent_change_percent >= 0.08 and close_vs_vwap_percent >= -0.02:
            return "Improving"
        if recent_change_percent <= -0.08 or close_vs_vwap_percent < -0.08:
            return "Weakening"
        return "Steady"

    def _classify_live_intraday_state(
        self,
        *,
        session_status: str,
        vix_status: str,
        timing_status: str,
        structure_status: str,
        momentum_status: str,
        prior_state: Optional[str],
    ) -> str:
        previously_active = prior_state in self.ACTIVE_STATES
        if session_status != "Open" or timing_status == "Closed":
            return KairosState.NOT_ELIGIBLE.value if not previously_active else KairosState.EXPIRED.value
        if structure_status == "Chop / Unclear":
            return KairosState.NOT_ELIGIBLE.value
        if structure_status == "Failed":
            return KairosState.EXPIRED.value if previously_active else KairosState.NOT_ELIGIBLE.value
        if momentum_status == "Expired":
            return KairosState.EXPIRED.value if previously_active else KairosState.NOT_ELIGIBLE.value
        if timing_status == "Late":
            return KairosState.WINDOW_CLOSING.value
        if structure_status == "Weakening" or momentum_status == "Weakening":
            return KairosState.WINDOW_CLOSING.value if previously_active else KairosState.SETUP_FORMING.value
        if timing_status == "Locked":
            return KairosState.SETUP_FORMING.value if structure_status in {"Bullish Confirmation", "Developing"} else KairosState.NOT_ELIGIBLE.value
        if structure_status == "Bullish Confirmation" and momentum_status in {"Improving", "Steady"}:
            return KairosState.WINDOW_OPEN.value
        if structure_status in {"Bullish Confirmation", "Developing"}:
            return KairosState.SETUP_FORMING.value
        return KairosState.NOT_ELIGIBLE.value

    def _build_live_intraday_scan_result(
        self,
        *,
        timestamp: datetime,
        scan_sequence_number: int,
        metrics: Dict[str, Any],
        vix_status: str,
        vix_display: str,
        prior_state: Optional[str],
        trigger_reason: str,
    ) -> KairosScanResult:
        timing_status = self._evaluate_timing_status_for_time(timestamp.astimezone(self.display_timezone).time(), "Open")
        structure_status = self._derive_runner_structure_status(metrics, timing_status)
        momentum_status = self._derive_runner_momentum_status(metrics, timing_status, structure_status)
        state = self._classify_live_intraday_state(
            session_status="Open",
            vix_status=vix_status,
            timing_status=timing_status,
            structure_status=structure_status,
            momentum_status=momentum_status,
            prior_state=prior_state,
        )
        transition = self._build_state_transition(prior_state, state)
        current_close = self._coerce_float(metrics.get("current_close"), fallback=0.0)
        return KairosScanResult(
            timestamp=timestamp,
            scan_sequence_number=scan_sequence_number,
            mode=KairosMode.LIVE.value,
            kairos_state=state,
            prior_kairos_state=prior_state,
            vix_status=vix_status,
            timing_status=timing_status,
            structure_status=structure_status,
            momentum_status=momentum_status,
            readiness_state=state,
            summary_text=self._build_summary_text(state),
            reasons=self._build_reasons(
                mode=KairosMode.LIVE,
                session_status="Open",
                vix_status=vix_status,
                timing_status=timing_status,
                structure_status=structure_status,
                momentum_status=momentum_status,
                vix_value=vix_display,
                scenario_name=None,
            ),
            state_transition=transition,
            is_window_open=state == KairosState.WINDOW_OPEN.value,
            is_window_closing=state == KairosState.WINDOW_CLOSING.value,
            is_expired=state == KairosState.EXPIRED.value,
            trigger_reason=trigger_reason,
            market_session_status="Open",
            spx_value=format_number_value(current_close),
            vix_value=vix_display,
            simulation_scenario_name=None,
            classification_note=(
                "Live intraday scan log and current Kairos state are derived from the same SPX tape, VWAP posture, "
                "and shared live structure/window classifier."
            ),
        )

    def _safe_latest_snapshot(self, ticker: str, *, query_type: str, force_refresh: bool = False) -> Dict[str, Any] | None:
        try:
            reader = (
                self.market_data_service.get_fresh_latest_snapshot
                if force_refresh and hasattr(self.market_data_service, "get_fresh_latest_snapshot")
                else self.market_data_service.get_latest_snapshot
            )
            return reader(ticker, query_type=query_type)
        except (AttributeError, MarketDataError):
            return None
        except Exception:
            return None

    def _evaluate_vix_regime(self, vix_value: float | None) -> str:
        if vix_value is None:
            return "Unavailable"
        if vix_value < 19:
            return "Caution"
        return "Favorable"

    def _evaluate_timing_status(self, now: datetime, session_status: str) -> str:
        return self._evaluate_timing_status_for_time(now.time(), session_status)

    def _evaluate_timing_status_for_time(self, current_time: time, session_status: str) -> str:
        if session_status != "Open":
            return "Closed"
        if current_time < self.TIMING_LOCK_RELEASE:
            return "Locked"
        if current_time < self.TIMING_LATE_START:
            return "Eligible"
        if current_time < self.TIMING_END:
            return "Late"
        return "Closed"

    def _evaluate_structure_status(self, spx_change_percent: float | None) -> str:
        if spx_change_percent is None:
            return "Chop / Unclear"
        if spx_change_percent <= -0.65:
            return "Failed"
        if spx_change_percent < -0.2:
            return "Weakening"
        if spx_change_percent < 0.18:
            return "Chop / Unclear"
        if spx_change_percent < 0.55:
            return "Developing"
        return "Bullish Confirmation"

    def _evaluate_live_momentum_status(
        self,
        *,
        timing_status: str,
        structure_status: str,
        vix_status: str,
        prior_state: Optional[str],
    ) -> str:
        if timing_status == "Closed" or structure_status == "Failed":
            return "Expired"
        if structure_status == "Weakening" or timing_status == "Late":
            return "Weakening"
        if structure_status == "Bullish Confirmation" and vix_status in {"Favorable", "Caution"}:
            return "Improving"
        if structure_status == "Developing" and prior_state in {KairosState.WATCHING.value, KairosState.SETUP_FORMING.value}:
            return "Improving"
        return "Steady"

    def _classify_overall_state(
        self,
        *,
        session_status: str,
        vix_status: str,
        timing_status: str,
        structure_status: str,
        momentum_status: str,
        prior_state: Optional[str],
    ) -> str:
        previously_active = prior_state in self.ACTIVE_STATES
        if session_status != "Open" or timing_status == "Closed":
            return KairosState.NOT_ELIGIBLE.value if not previously_active else KairosState.EXPIRED.value
        if vix_status == "Not Eligible" or timing_status == "Locked":
            return KairosState.NOT_ELIGIBLE.value
        if structure_status == "Failed" or momentum_status == "Expired":
            return KairosState.EXPIRED.value
        if timing_status == "Late":
            return KairosState.WINDOW_CLOSING.value if previously_active or structure_status in {"Bullish Confirmation", "Developing", "Weakening"} else KairosState.EXPIRED.value
        if structure_status == "Weakening" or momentum_status == "Weakening":
            return KairosState.WINDOW_CLOSING.value if previously_active or structure_status == "Weakening" else KairosState.WATCHING.value
        if structure_status == "Bullish Confirmation" and momentum_status in {"Improving", "Steady"}:
            return KairosState.WINDOW_OPEN.value
        if structure_status == "Developing":
            return KairosState.SETUP_FORMING.value
        if structure_status == "Chop / Unclear":
            return KairosState.WATCHING.value
        return KairosState.WATCHING.value

    def _build_summary_text(self, overall_state: str) -> str:
        if overall_state == KairosState.NOT_ELIGIBLE.value:
            return "Not eligible: the current Kairos combination is not tradable yet."
        if overall_state == KairosState.WATCHING.value:
            return "Watching: the setup is incomplete but still worth monitoring."
        if overall_state == KairosState.SETUP_FORMING.value:
            return "Subprime improving: timing and volatility are usable while structure continues to build."
        if overall_state == KairosState.WINDOW_OPEN.value:
            return "Prime: timing, volatility, and structure align under the current Kairos classifier."
        if overall_state == KairosState.WINDOW_CLOSING.value:
            return "Subprime weakening: the setup is fading and should be treated as unstable."
        return "Expired: the current Kairos opportunity window is no longer valid."

    def _build_reasons(
        self,
        *,
        mode: KairosMode,
        session_status: str,
        vix_status: str,
        timing_status: str,
        structure_status: str,
        momentum_status: str,
        vix_value: str,
        scenario_name: Optional[str],
    ) -> List[str]:
        reasons: List[str] = []
        if mode == KairosMode.SIMULATION:
            reasons.append(
                f"Simulation Mode active{' via ' + scenario_name if scenario_name and scenario_name != 'Custom' else ''}; this scan is not reflecting live market conditions."
            )

        if vix_status == "Favorable":
            reasons.append(f"VIX favorable at {vix_value}; the volatility regime supports Kairos timing sensitivity.")
        elif vix_status == "Caution":
            reasons.append(f"VIX {vix_value} is usable but below the preferred Kairos threshold, so caution is warranted.")
        elif vix_status == "Not Eligible":
            reasons.append(f"VIX {vix_value} is below the Kairos minimum volatility threshold.")
        else:
            reasons.append("Live VIX data is unavailable, so the volatility input remains provisional.")

        if timing_status == "Locked":
            reasons.append("Timing is still locked before the 8:45 CT release window.")
        elif timing_status == "Eligible":
            reasons.append("Timing is inside the active Kairos intraday session.")
        elif timing_status == "Late":
            reasons.append("Timing has moved into the late-session decay zone.")
        else:
            reasons.append(f"Market session status is {session_status.lower()}, so the timing window is closed.")

        if structure_status == "Bullish Confirmation":
            reasons.append("Structure shows provisional bullish confirmation.")
        elif structure_status == "Developing":
            reasons.append("Structure is improving but has not fully confirmed yet.")
        elif structure_status == "Chop / Unclear":
            reasons.append("Structure remains choppy and directionally unclear.")
        elif structure_status == "Weakening":
            reasons.append("Structure has weakened from a stronger earlier posture.")
        else:
            reasons.append("Structure failed materially, invalidating the setup.")

        if momentum_status == "Improving":
            reasons.append("Momentum is improving into the current scan.")
        elif momentum_status == "Steady":
            reasons.append("Momentum is steady but not accelerating yet.")
        elif momentum_status == "Weakening":
            reasons.append("Momentum is weakening, which usually means the window is narrowing.")
        else:
            reasons.append("Momentum has expired alongside the current setup.")

        return reasons[:5]

    def _build_state_transition(self, prior_state: Optional[str], current_state: str) -> KairosStateTransition:
        comparable_prior = prior_state if prior_state not in {None, KairosState.ACTIVATED.value, KairosState.SCANNING.value} else None
        is_transition = comparable_prior is not None and comparable_prior != current_state
        if comparable_prior is None:
            label = "Initial classification"
        elif is_transition:
            label = f"{comparable_prior} -> {current_state}"
        else:
            label = f"State unchanged: {current_state}"
        return KairosStateTransition(
            prior_state=comparable_prior,
            current_state=current_state,
            is_transition=is_transition,
            label=label,
        )

    def _get_effective_market_session_status(self, now: datetime) -> str:
        if self._runtime.mode == KairosMode.SIMULATION:
            return self._runtime.simulated_market_session_status
        return self._get_live_market_session_status(now)

    def _get_live_market_session_status(self, now: datetime) -> str:
        if not self.market_calendar_service.is_tradable_market_day(now.date()):
            return "Closed"
        current_time = now.time()
        if current_time < self.MARKET_OPEN:
            return "Closed"
        if current_time >= self.MARKET_CLOSE:
            return "Ended"
        return "Open"

    def _build_payload_locked(self, now: datetime) -> Dict[str, Any]:
        market_session_status = self._get_effective_market_session_status(now)
        current_scan = self._session.latest_scan.to_payload(self.display_timezone, now) if self._session.latest_scan else default_scan_payload(now, self.display_timezone, market_session_status, self._runtime.mode.value)
        next_scan_seconds = max(0, int((self._session.next_scan_at - now).total_seconds())) if self._session.next_scan_at else None
        session_status = self._derive_session_status_label(market_session_status)
        session_status_key = slugify_label(session_status)
        simulation_runner_payload = self._build_simulation_runner_payload_locked()
        latest_scan_result = self._session.latest_scan
        live_candidate_context = None
        if self._runtime.mode == KairosMode.LIVE and market_session_status == "Open":
            live_candidate_context = self._build_live_trade_candidate_context_locked(now, latest_scan_result)
        live_best_trade_payload = self._build_live_best_trade_override_payload_locked(
            now,
            latest_scan=latest_scan_result,
            candidate_context=live_candidate_context,
        )
        live_trade_manager_payload = self._build_live_trade_manager_payload_locked(now)
        live_workspace_payload = self._build_live_workspace_payload_locked(
            now,
            latest_scan=latest_scan_result,
            candidate_context=live_candidate_context,
            live_best_trade=live_best_trade_payload,
        )
        current_scan = self._augment_scan_payload_for_display(current_scan)
        current_state_display = self._map_kairos_lifecycle_state(
            raw_state=self._session.current_state,
            simulation_runner=simulation_runner_payload,
            live_best_trade=live_best_trade_payload,
            live_trade_manager=live_trade_manager_payload,
        )
        lifecycle_items = [
            {
                "label": "Initialization" if self._runtime.mode == KairosMode.LIVE else "Activation",
                "value": "Active" if self._session.activated_at is not None and self._session.session_date == now.date() else "Pending",
                "status_key": bool_to_status_key(self._session.activated_at is not None and self._session.session_date == now.date()),
            },
            {
                "label": "Scanning",
                "value": "Active" if self._session.armed_for_day and not self._session.session_complete and not self._session.stopped_manually else "Standby",
                "status_key": bool_to_status_key(self._session.armed_for_day and not self._session.session_complete and not self._session.stopped_manually),
            },
            {
                "label": "Subprime Improving",
                "value": "Reached" if self._has_seen_scan_state({KairosState.SETUP_FORMING.value, KairosState.WINDOW_OPEN.value, KairosState.WINDOW_CLOSING.value, KairosState.EXPIRED.value, KairosState.SESSION_COMPLETE.value}) else "Pending",
                "status_key": bool_to_status_key(self._has_seen_scan_state({KairosState.SETUP_FORMING.value, KairosState.WINDOW_OPEN.value, KairosState.WINDOW_CLOSING.value, KairosState.EXPIRED.value, KairosState.SESSION_COMPLETE.value})),
            },
            {
                "label": "Prime",
                "value": "Reached" if self._session.window_found else "Pending",
                "status_key": bool_to_status_key(self._session.window_found),
            },
            {
                "label": "Trade Recommend",
                "value": self._derive_trade_recommendation_stage(live_best_trade_payload, simulation_runner_payload),
                "status_key": self._derive_trade_recommendation_status_key(live_best_trade_payload, simulation_runner_payload),
            },
            {
                "label": "Trade Filled",
                "value": self._derive_trade_filled_stage(live_trade_manager_payload, simulation_runner_payload),
                "status_key": self._derive_trade_filled_status_key(live_trade_manager_payload, simulation_runner_payload),
            },
            {
                "label": "Trade Status",
                "value": self._derive_trade_status_stage(live_trade_manager_payload, simulation_runner_payload),
                "status_key": self._derive_trade_status_status_key(live_trade_manager_payload, simulation_runner_payload),
            },
            {
                "label": "Completed",
                "value": "Complete" if self._session.session_complete or self._session.stopped_manually or self._session.auto_ended else "Pending",
                "status_key": bool_to_status_key(self._session.session_complete or self._session.stopped_manually or self._session.auto_ended),
            },
        ]
        scan_log_count = len(self._session.scan_log)
        return {
            "title": "Kairos",
            "subtitle": "The God of the Right Moment",
            "mode": self._runtime.mode.value,
            "mode_key": slugify_label(self._runtime.mode.value),
            "mode_badge_text": f"{self._runtime.mode.value.upper()} MODE",
            "is_simulation_mode": self._runtime.mode == KairosMode.SIMULATION,
            "session_date": self._session.session_date.isoformat() if self._session.session_date else None,
            "session_date_display": self._session.session_date.isoformat() if self._session.session_date else "Not armed",
            "current_state": self._session.current_state,
            "current_state_key": slugify_label(self._session.current_state),
            "current_state_display": current_state_display,
            "current_state_display_key": slugify_label(current_state_display),
            "session_status": session_status,
            "session_status_key": session_status_key,
            "market_session_status": market_session_status,
            "market_session_status_key": slugify_label(market_session_status),
            "armed_for_day": self._session.armed_for_day,
            "scan_engine_running": self._session.scan_engine_running,
            "session_complete": self._session.session_complete,
            "stopped_manually": self._session.stopped_manually,
            "auto_ended": self._session.auto_ended,
            "window_found": self._session.window_found,
            "status_note": self._session.status_note,
            "activated_at": serialize_optional_datetime(self._session.activated_at, self.display_timezone),
            "last_scan_at": serialize_optional_datetime(self._session.last_scan_at, self.display_timezone),
            "next_scan_at": serialize_optional_datetime(self._session.next_scan_at, self.display_timezone),
            "last_scan_display": format_optional_datetime(self._session.last_scan_at, self.display_timezone),
            "next_scan_display": format_optional_datetime(self._session.next_scan_at, self.display_timezone),
            "countdown_seconds": next_scan_seconds,
            "countdown_display": format_countdown(next_scan_seconds),
            "total_scans_completed": self._session.total_scans_completed,
            "scan_interval_seconds": self._active_scan_interval_seconds(),
            "live_scan_interval_seconds": self.live_scan_interval_seconds,
            "latest_scan": current_scan,
            "latest_transition": current_scan.get("state_transition"),
            "scan_log": [self._augment_scan_payload_for_display(item.to_payload(self.display_timezone, now)) for item in self._session.scan_log],
            "scan_log_count": scan_log_count,
            "scan_log_note": (
                f"Showing all {scan_log_count} session records (newest first)."
                if scan_log_count
                else "No session records yet."
            ),
            "bar_map": self._build_bar_map_payload_locked(),
            "lifecycle_items": lifecycle_items,
            "classification_note": current_scan.get("classification_note") or self._build_runtime_note_locked(),
            "simulation_controls": self._build_simulation_controls_payload_locked(),
            "simulation_runner": simulation_runner_payload,
            "best_trade_override": live_best_trade_payload,
            "live_workspace": live_workspace_payload,
            "live_trade_manager": live_trade_manager_payload,
            "placeholder_logic_note": "Kairos Phase 3C reuses the current classifier while the runner digests a full /ES-style intraday candle tape.",
        }

    def _derive_session_status_label(self, market_session_status: str) -> str:
        if self._session.session_complete:
            return "Ended"
        if self._session.stopped_manually:
            return "Inactive"
        if self._session.scan_engine_running:
            return "Scanning"
        if self._session.armed_for_day:
            return "Armed"
        if market_session_status == "Ended":
            return "Ended"
        return "Inactive"

    def _build_simulation_controls_payload_locked(self) -> Dict[str, Any]:
        return {
            "mode": self._runtime.mode.value,
            "market_session_status": self._runtime.simulated_market_session_status,
            "simulated_spx_value": self._runtime.simulated_spx_value,
            "simulated_vix_value": self._runtime.simulated_vix_value,
            "timing_status": self._runtime.simulated_timing_status,
            "structure_status": self._runtime.simulated_structure_status,
            "momentum_status": self._runtime.simulated_momentum_status,
            "scan_interval_seconds": self._runtime.simulation_scan_interval_seconds,
            "scenario_name": self._runtime.simulation_scenario_name,
            "mode_options": [
                {"label": "Live Mode", "value": KairosMode.LIVE.value},
                {"label": "Simulation Mode", "value": KairosMode.SIMULATION.value},
            ],
            "market_session_options": list(self.SIMULATION_MARKET_SESSION_OPTIONS),
            "timing_options": list(self.TIMING_STATUS_OPTIONS),
            "structure_options": list(self.STRUCTURE_STATUS_OPTIONS),
            "momentum_options": list(self.MOMENTUM_STATUS_OPTIONS),
            "scan_interval_options": [
                {"label": "10 seconds", "value": 10},
                {"label": "15 seconds", "value": 15},
                {"label": "30 seconds", "value": 30},
                {"label": "2 minutes", "value": 120},
            ],
            "preset_options": [{"label": name, "value": name} for name in self.SIMULATION_PRESETS],
            "historical_replay": self._build_historical_replay_controls_payload_locked(),
        }

    def _build_simulation_runner_payload_locked(self) -> Dict[str, Any]:
        scenario = self._current_runner_scenario_locked()
        total_bars = len(scenario.bars) if scenario is not None else SESSION_BAR_COUNT
        current_bar_number = max(0, self._runner.current_bar_index + 1)
        progress_percent = int((current_bar_number / total_bars) * 100) if total_bars > 0 else 0
        metrics = self._build_runner_metrics_locked(scenario, self._runner.current_bar_index) if scenario is not None else None
        current_bar = metrics["current_bar"] if metrics is not None else None
        latest_scan = self._session.latest_scan
        return {
            "status": self._runner.status.value,
            "status_key": slugify_label(self._runner.status.value),
            "is_running": self._runner.status == KairosRunnerStatus.RUNNING,
            "is_paused": self._runner.status == KairosRunnerStatus.PAUSED,
            "tape_source": self._current_runner_tape_source_locked(scenario),
            "run_mode": self._runner.run_mode,
            "scenario_key": self._runner.scenario_key,
            "scenario_name": scenario.name if scenario is not None else "No scenario selected",
            "scenario_description": scenario.description if scenario is not None else "Choose a scenario to begin the full-day simulator.",
            "scenario_options": self._build_runner_scenario_options_locked(),
            "simulated_time_display": self._runner.simulated_time_label,
            "progress_percent": progress_percent,
            "current_bar_index": self._runner.current_bar_index,
            "current_bar_number": current_bar_number,
            "completed_steps": current_bar_number,
            "total_steps": total_bars,
            "total_bars": total_bars,
            "current_step_label": self._runner.current_step_label,
            "current_step_note": self._runner.current_step_note,
            "pause_reason": self._runner.pause_reason,
            "pause_reason_key": slugify_label(self._runner.pause_reason or ""),
            "pause_detail": self._runner.pause_detail,
            "pause_event_filters": list(self._runner.pause_event_filters),
            "pause_event_options": [dict(item) for item in self.RUNNER_PAUSE_EVENT_OPTIONS],
            "pause_status": {
                "event_label": self._runner.pause_event_label,
                "bar_number": self._runner.pause_bar_number,
                "simulated_time": self._runner.pause_simulated_time,
                "summary": self._runner.pause_detail,
                "suppression_active": self._runner.suppressed_pause_event_kind is not None,
                "suppressed_event_label": self._runner.suppressed_pause_event_label,
                "suppression_note": (
                    f"Repeated {self._runner.suppressed_pause_event_label} pauses suppressed until state changes."
                    if self._runner.suppressed_pause_event_label
                    else ""
                ),
            },
            "tick_interval_seconds": self.RUNNER_TICK_INTERVAL_SECONDS,
            "current_candle": {
                "open": round(current_bar.open, 2),
                "high": round(current_bar.high, 2),
                "low": round(current_bar.low, 2),
                "close": round(current_bar.close, 2),
                "volume": current_bar.volume,
            }
            if current_bar is not None
            else None,
            "session_metrics": {
                "current_close": round(metrics["current_close"], 2),
                "session_high": round(metrics["session_high"], 2),
                "session_low": round(metrics["session_low"], 2),
                "vwap": round(metrics["vwap"], 2),
                "current_vix": round(metrics["current_vix"], 2),
                "current_volume": current_bar.volume if current_bar is not None else 0,
                "cumulative_volume": metrics["cumulative_volume"],
                "session_posture": metrics["session_posture"],
            }
            if metrics is not None
            else None,
            "active_trade_lock_in": self._runner.active_trade_lock_in.to_payload(self.display_timezone) if self._runner.active_trade_lock_in is not None else None,
            "trade_exit_status": self._build_sim_trade_exit_snapshot_locked(metrics),
            "trade_exit_prompt": self._build_sim_trade_exit_prompt_locked(metrics),
            "trade_lock_history_count": len(self._runner.trade_lock_history),
            "trade_candidate_card": self._build_runner_trade_candidate_card_locked(latest_scan, metrics),
            "can_request_best_trade_override": self._can_request_sim_best_trade_override_locked(),
            "best_trade_override_disabled_reason": self._sim_best_trade_override_disabled_reason_locked(),
            "event_log": [item.to_payload(self.display_timezone) for item in self._runner.event_log],
        }

    def _build_live_best_trade_override_payload_locked(
        self,
        now: datetime,
        *,
        latest_scan: KairosScanResult | None = None,
        candidate_context: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        hidden_payload = {
            "visible": False,
            "status": "idle",
            "title": "Best Trade Available",
            "eyebrow": "Live advisory / auto-refresh",
            "message": "Live Kairos is initializing the current Schwab-chain workspace.",
            "caution_text": "",
            "summary_chips": [],
            "candidate": None,
            "chain_source_label": "Schwab Live",
            "price_basis_label": "Live bid/ask + midpoint hybrid",
            "chain_status_label": "Unavailable",
            "can_open_trade": False,
            "open_trade_disabled_reason": "No live best-available trade is ready to open.",
        }
        if self._runtime.mode != KairosMode.LIVE:
            return hidden_payload
        if self._session.session_date is None and self._live_active_trade is None:
            return hidden_payload

        if self._get_effective_market_session_status(now) != "Open":
            market_message = self._get_live_market_state_message(now)
            return {
                **hidden_payload,
                "visible": True,
                "status": "market-closed",
                "message": market_message,
                "caution_text": "Live Kairos only uses same-day Schwab chain data during the regular market session.",
                "summary_chips": [
                    {"label": "CHAIN SOURCE", "value": "Schwab Live"},
                    {"label": "PRICE BASIS", "value": "Live bid/ask + midpoint hybrid"},
                    {"label": "WIDTH SCAN", "value": f"up to {max(self.LIVE_KAIROS_SPREAD_WIDTH_SCAN_POINTS)} pts"},
                ],
                "chain_status_label": "Market closed",
                "open_trade_disabled_reason": market_message,
            }

        candidate_context = candidate_context or self._build_live_trade_candidate_context_locked(now, latest_scan)
        if candidate_context is None:
            return {
                **hidden_payload,
                "visible": True,
                "status": "unavailable",
                "message": "The current best Kairos candidate is unavailable because SPX or VIX data could not be resolved.",
                "caution_text": "Live market data is incomplete, so no sane override candidate can be produced right now.",
            }
        if not candidate_context.get("chain_success", True):
            return {
                **hidden_payload,
                "visible": True,
                "status": "unavailable",
                "message": candidate_context.get("chain_message") or "The current best Kairos candidate is unavailable because the same-day Schwab option chain could not be resolved.",
                "caution_text": "Live Kairos will not fall back to estimated chains when Schwab same-day contracts are unavailable.",
                "summary_chips": self._build_trade_candidate_summary_chips(candidate_context),
                "chain_source_label": candidate_context.get("chain_source_label") or "Schwab (Unavailable)",
                "price_basis_label": candidate_context.get("price_basis_label") or "Unavailable",
                "chain_status_label": candidate_context.get("chain_status_label") or "Unavailable",
            }

        best_candidate = self._build_best_override_candidate(candidate_context, latest_scan=latest_scan, relax_final_gating=True)
        caution_text = self._build_trade_override_caution_text(latest_scan)
        if best_candidate is None:
            return {
                **hidden_payload,
                "visible": True,
                "status": "no-candidates",
                "message": "No live-chain candidate could be ranked from the current Schwab chain snapshot.",
                "caution_text": caution_text,
                "summary_chips": self._build_trade_candidate_summary_chips(candidate_context),
                "chain_source_label": candidate_context.get("chain_source_label") or "Schwab Live",
                "price_basis_label": candidate_context.get("price_basis_label") or "Live bid/ask + midpoint hybrid",
                "chain_status_label": candidate_context.get("chain_status_label") or "Available",
            }

        is_fully_valid = bool(best_candidate.get("is_fully_valid"))
        return {
            **hidden_payload,
            "visible": True,
            "status": "ready" if is_fully_valid else "stand-aside",
            "message": (
                "Best Candidate reflects the strongest currently modeled Kairos spread from the live Schwab chain."
                if is_fully_valid
                else "Best Candidate ranked the strongest currently available live Schwab chain candidate, but it still falls short of Kairos minimum standards."
            ),
            "caution_text": caution_text,
            "summary_chips": self._build_trade_candidate_summary_chips(candidate_context),
            "candidate": best_candidate,
            "chain_source_label": candidate_context.get("chain_source_label") or "Schwab Live",
            "price_basis_label": candidate_context.get("price_basis_label") or "Live bid/ask + midpoint hybrid",
            "chain_status_label": candidate_context.get("chain_status_label") or "Available",
            "chain_time_label": candidate_context.get("chain_time_label") or candidate_context.get("simulated_time_label"),
            "can_open_trade": is_fully_valid and self._live_active_trade is None,
            "open_trade_disabled_reason": (
                "A live Kairos trade is already active."
                if self._live_active_trade is not None and self._live_active_trade.remaining_contracts > 0
                else "Stand Aside candidates are advisory only until they fully meet Kairos minimum standards."
            ),
        }

    def _build_live_trade_manager_payload_locked(self, now: datetime) -> Dict[str, Any]:
        if self._runtime.mode != KairosMode.LIVE:
            return {"visible": False, "active_trade": None, "exit_prompt": {"visible": False}, "exit_status": {"visible": False}}
        return {
            "visible": True,
            "active_trade": self._live_active_trade.to_payload(self.display_timezone) if self._live_active_trade is not None else None,
            "exit_prompt": self._build_live_trade_exit_prompt_locked(now),
            "exit_status": self._build_live_trade_exit_snapshot_locked(now),
            "history_count": len(self._live_trade_history),
        }

    def _build_live_workspace_payload_locked(
        self,
        now: datetime,
        *,
        latest_scan: KairosScanResult | None,
        candidate_context: Dict[str, Any] | None,
        live_best_trade: Dict[str, Any],
    ) -> Dict[str, Any]:
        readiness_label = self._map_scan_state_label(latest_scan.readiness_state) if latest_scan is not None else "Scanning"
        stamps = [
            {"label": "Last Scan", "value": format_optional_datetime(self._session.last_scan_at, self.display_timezone)},
            {"label": "Structure", "value": latest_scan.structure_status if latest_scan is not None else "Awaiting scan"},
            {"label": "Countdown", "value": format_countdown(max(0, int((self._session.next_scan_at - now).total_seconds()))) if self._session.next_scan_at else "—"},
            {"label": "Total Scans Today", "value": str(self._session.total_scans_completed)},
            {"label": "Scan Interval", "value": f"{self._active_scan_interval_seconds()} sec"},
        ]
        best_candidate = live_best_trade.get("candidate") if isinstance(live_best_trade, dict) else None

        best_card = self._build_live_candidate_card_payload_locked(
            slot_key="best-available",
            slot_label=self.KAIROS_SLOT_LABELS["best-available"],
            card_note="Strongest currently modeled same-day live-chain candidate under the current session tape grade.",
            candidate=best_candidate,
            candidate_context=candidate_context,
            latest_scan=latest_scan,
            fallback_message=(live_best_trade.get("message") if isinstance(live_best_trade, dict) else "No Kairos candidate is ready right now."),
        )
        return {
            "visible": self._runtime.mode == KairosMode.LIVE,
            "state_pill_label": self._map_scan_state_label(self._session.current_state),
            "state_pill_key": slugify_label(self._map_scan_state_label(self._session.current_state)),
            "readiness_pill_label": readiness_label,
            "readiness_pill_key": slugify_label(readiness_label),
            "stamps": stamps,
            "summary_text": latest_scan.summary_text if latest_scan is not None else self._session.status_note,
            "classification_note": latest_scan.classification_note if latest_scan is not None else self._build_runtime_note_locked(),
            "credit_map": self._build_live_credit_map_locked(candidate_context=candidate_context, cards=[best_card]),
            "candidate_cards": [best_card],
        }

    def _build_live_candidate_card_payload_locked(
        self,
        *,
        slot_key: str,
        slot_label: str,
        card_note: str,
        candidate: Dict[str, Any] | None,
        candidate_context: Dict[str, Any] | None,
        latest_scan: KairosScanResult | None,
        fallback_message: str,
    ) -> Dict[str, Any]:
        default_mode_key = "standard"
        if not candidate:
            return {
                "slot_key": slot_key,
                "slot_label": slot_label,
                "mode_key": default_mode_key,
                "card_note": card_note,
                "available": False,
                "tradeable": False,
                "headline": slot_label,
                "descriptor": card_note,
                "structure_label": "Not Recommended",
                "status_label": "Unavailable",
                "strike_label": "No valid trade",
                "net_credit": "—",
                "contract_size": "—",
                "distance_to_short": "—",
                "em_multiple": "—",
                "premium_received": "—",
                "routine_loss": "—",
                "black_swan_loss": "—",
                "routine_probability": "—",
                "tail_probability": "—",
                "max_loss": "—",
                "max_loss_per_contract": "—",
                "estimated_otm_probability": "—",
                "rationale": fallback_message,
                "detail_rows": [],
                "header_stamps": [
                    {"label": "Confidence", "value": "—"},
                    {"label": "Fit Score", "value": "—"},
                ],
                "message": fallback_message,
                "can_open_trade": False,
                "open_trade_profile_key": "",
                "prefill_enabled": False,
                "prefill_fields": {},
            }

        mode_key = str(candidate.get("mode_key") or default_mode_key)
        max_loss_value = float(candidate.get("max_loss_dollars") or 0.0)
        outcome_profile = self._build_real_trade_outcome_profile()
        routine_loss = max_loss_value * abs(float(outcome_profile.get("routine_loss_percentage") or self.DEFAULT_ROUTINE_LOSS_PERCENTAGE))
        black_swan_loss = max_loss_value * abs(float(outcome_profile.get("black_swan_loss_percentage") or self.DEFAULT_BLACK_SWAN_LOSS_PERCENTAGE))
        tradeable = bool(candidate.get("available") and candidate.get("is_fully_valid"))
        can_open_trade = tradeable and self._live_active_trade is None
        detail_rows = list(candidate.get("detail_rows") or [])
        prefill_fields = self._build_live_candidate_prefill_fields_locked(
            slot_label=slot_label,
            candidate=candidate,
            candidate_context=candidate_context,
            latest_scan=latest_scan,
        )
        structure_label = self._derive_live_candidate_structure_label(latest_scan)
        return {
            "slot_key": slot_key,
            "slot_label": slot_label,
            "mode_key": mode_key,
            "card_note": card_note,
            "available": True,
            "tradeable": tradeable,
            "headline": slot_label,
            "descriptor": candidate.get("mode_descriptor") or card_note,
            "structure_label": structure_label,
            "status_label": "Ready" if tradeable else "Not Recommended",
            "strike_label": f"{candidate.get('short_strike') or '—'} / {candidate.get('long_strike') or '—'} Put Spread",
            "net_credit": candidate.get("credit_estimate_display") or "—",
            "contract_size": candidate.get("contract_size_display") or "—",
            "distance_to_short": candidate.get("distance_to_short_display") or "—",
            "em_multiple": candidate.get("em_multiple_display") or "—",
            "premium_received": candidate.get("premium_received_display") or candidate.get("credit_estimate_display") or "—",
            "routine_loss": format_currency_value(routine_loss),
            "black_swan_loss": format_currency_value(black_swan_loss),
            "routine_probability": candidate.get("estimated_otm_probability_display") or "—",
            "tail_probability": candidate.get("estimated_otm_probability_display") or "—",
            "routine_loss_percentage": format_percent_value(abs(float(outcome_profile.get("routine_loss_percentage") or 0.0)) * 100),
            "black_swan_loss_percentage": format_percent_value(abs(float(outcome_profile.get("black_swan_loss_percentage") or 0.0)) * 100),
            "routine_loss_count": int(outcome_profile.get("loss_count") or 0),
            "black_swan_loss_count": int(outcome_profile.get("black_swan_count") or 0),
            "loss_model_source": "Historical real-trade ROI averages (ordinary losses exclude Black Swans)",
            "max_loss": candidate.get("max_loss_display") or "—",
            "max_loss_per_contract": candidate.get("max_loss_per_contract_display") or "—",
            "estimated_otm_probability": candidate.get("estimated_otm_probability_display") or "—",
            "rationale": candidate.get("rationale") or fallback_message,
            "detail_rows": detail_rows,
            "header_stamps": [
                {"label": "Confidence", "value": candidate.get("confidence_label") or "—"},
                {"label": "Fit Score", "value": candidate.get("fit_score_display") or "—"},
            ],
            "message": candidate.get("no_trade_message") or fallback_message,
            "can_open_trade": can_open_trade,
            "open_trade_profile_key": candidate.get("mode_key") or "",
            "prefill_enabled": bool(prefill_fields),
            "prefill_fields": prefill_fields,
            "short_strike_value": candidate.get("short_strike"),
            "long_strike_value": candidate.get("long_strike"),
            "premium_received_value": float(candidate.get("premium_received_dollars") or 0.0),
            "plot_marker": True,
            "marker_label": "BC",
        }

    def _derive_session_tape_structure_label(self, latest_scan: KairosScanResult | None) -> str:
        raw_state = latest_scan.kairos_state if latest_scan is not None else self._session.current_state
        if raw_state == KairosState.WINDOW_OPEN.value:
            return "Prime"
        if raw_state in {KairosState.SETUP_FORMING.value, KairosState.WINDOW_CLOSING.value}:
            return "Subprime"
        return "Subprime"

    def _derive_live_candidate_structure_label(self, latest_scan: KairosScanResult | None) -> str:
        raw_state = latest_scan.kairos_state if latest_scan is not None else self._session.current_state
        if raw_state == KairosState.WINDOW_OPEN.value:
            return "Prime"
        if raw_state in {KairosState.SETUP_FORMING.value, KairosState.WINDOW_CLOSING.value}:
            return "Subprime"
        return "Not Recommended"

    def _build_live_candidate_prefill_fields_locked(
        self,
        *,
        slot_label: str,
        candidate: Dict[str, Any],
        candidate_context: Dict[str, Any] | None,
        latest_scan: KairosScanResult | None,
    ) -> Dict[str, Any]:
        if not candidate:
            return {}

        session_date = self._session.session_date.isoformat() if self._session.session_date is not None else ""
        spot_value = float((candidate_context or {}).get("spot") or 0.0)
        vix_value = float((candidate_context or {}).get("vix") or 0.0)
        credit_estimate = round(float(candidate.get("credit_estimate_dollars") or 0.0), 2)
        total_premium = round(float(candidate.get("premium_received_dollars") or 0.0), 2)
        net_credit_per_contract = round((credit_estimate / 100.0), 4) if credit_estimate else 0.0

        session_tape_structure = self._derive_session_tape_structure_label(latest_scan)
        return {
            "system_name": "Kairos",
            "journal_name": "Horme",
            "system_version": HOSTED_APP_VERSION,
            "candidate_profile": session_tape_structure,
            "expiration_date": session_date,
            "underlying_symbol": "SPX",
            "spx_at_entry": round(spot_value, 2) if spot_value > 0 else "",
            "vix_at_entry": round(vix_value, 2) if vix_value > 0 else "",
            "structure_grade": session_tape_structure,
            "macro_grade": latest_scan.momentum_status if latest_scan is not None else "",
            "expected_move": candidate.get("expected_move_used") or "",
            "expected_move_used": candidate.get("expected_move_used") or "",
            "expected_move_source": candidate.get("expected_move_source") or "same_day_atm_straddle",
            "option_type": "Put Credit Spread",
            "short_strike": candidate.get("short_strike") or "",
            "long_strike": candidate.get("long_strike") or "",
            "spread_width": candidate.get("spread_width") or "",
            "contracts": candidate.get("recommended_contracts") or "",
            "candidate_credit_estimate": net_credit_per_contract or "",
            "actual_entry_credit": net_credit_per_contract or "",
            "net_credit_per_contract": net_credit_per_contract or "",
            "premium_per_contract": credit_estimate or "",
            "total_premium": total_premium or "",
            "distance_to_short": candidate.get("distance_points") or "",
            "em_multiple_floor": candidate.get("em_multiple_floor") or "",
            "percent_floor": candidate.get("percent_floor") or "",
            "boundary_rule_used": candidate.get("boundary_rule_used") or "",
            "actual_distance_to_short": candidate.get("actual_distance_to_short") or candidate.get("distance_points") or "",
            "actual_em_multiple": candidate.get("actual_em_multiple") or candidate.get("daily_move_multiple") or "",
            "fallback_used": candidate.get("fallback_used") or "no",
            "fallback_rule_name": candidate.get("fallback_rule_name") or "",
            "short_delta": candidate.get("estimated_short_delta") or "",
            "notes_entry": "Prefilled from Kairos best candidate card.",
            "prefill_source": "kairos-candidate",
        }

    def _build_live_credit_map_locked(self, *, candidate_context: Dict[str, Any] | None, cards: List[Dict[str, Any]]) -> Dict[str, Any]:
        spot = self._coerce_float(candidate_context.get("spot") if candidate_context else None, fallback=0.0)
        expected_move = self._coerce_float(candidate_context.get("daily_move_anchor") if candidate_context else None, fallback=0.0)
        if spot <= 0:
            return {"available": False, "guides": [], "markers": [], "regions": [], "slot_statuses": []}

        standard_boundary = self._build_live_kairos_hybrid_boundary(
            spot=spot,
            expected_move=expected_move,
            em_multiple_floor=self.KAIROS_HYBRID_EXPECTED_MOVE_MULTIPLE,
        )
        lower_em = spot - expected_move if expected_move > 0 else None
        window_boundary = spot - float(standard_boundary["hybrid_threshold"])
        plot_left_x = 130.0
        plot_right_x = 920.0
        baseline_y = 164.0
        floor_y = 146.0
        peak_y = 46.0
        spot_line_top_y = 28.0
        card_specs = [item for item in cards if item.get("plot_marker") and item.get("short_strike_value") not in {None, ""} and item.get("long_strike_value") not in {None, ""}]
        relevant_values = [spot, window_boundary]
        if lower_em is not None:
            relevant_values.extend([lower_em, spot + expected_move])
        for item in card_specs:
            relevant_values.extend([float(item["short_strike_value"]), float(item["long_strike_value"])])

        left_reach = max((spot - value) for value in relevant_values if value <= spot) if any(value <= spot for value in relevant_values) else 0.0
        right_reach = max((value - spot) for value in relevant_values if value >= spot) if any(value >= spot for value in relevant_values) else 0.0
        data_span = max(max(relevant_values) - min(relevant_values), 1.0)
        base_padding = max(data_span * 0.08, 8.0)
        left_span = max(left_reach + base_padding, 1.0)
        right_span = max(right_reach + base_padding, left_span * 0.58)
        left_bound = spot - left_span
        right_bound = spot + right_span

        def to_x(value: float | None) -> float:
            if value is None:
                return plot_left_x + ((plot_right_x - plot_left_x) / 2)
            proportion = (value - left_bound) / max(right_bound - left_bound, 1.0)
            proportion = max(0.0, min(1.0, proportion))
            return plot_left_x + ((plot_right_x - plot_left_x) * proportion)

        def build_profile_paths(spot_x: float) -> tuple[str, str]:
            left_span_px = max(spot_x - plot_left_x, 1.0)
            right_span_px = max(plot_right_x - spot_x, 1.0)
            line_path = (
                f"M{plot_left_x:.1f} {floor_y:.1f} "
                f"C{(plot_left_x + left_span_px * 0.42):.1f} {floor_y:.1f}, {(plot_left_x + left_span_px * 0.78):.1f} {(peak_y + 58.0):.1f}, {spot_x:.1f} {peak_y:.1f} "
                f"C{(spot_x + right_span_px * 0.22):.1f} {(peak_y + 58.0):.1f}, {(spot_x + right_span_px * 0.58):.1f} {floor_y:.1f}, {plot_right_x:.1f} {floor_y:.1f}"
            )
            area_path = f"{line_path} L{plot_right_x:.1f} {baseline_y:.1f} L{plot_left_x:.1f} {baseline_y:.1f} Z"
            return line_path, area_path

        max_premium = max([float(item.get("premium_received_value") or 0.0) for item in card_specs] + [1.0])
        guides = []
        for ratio in (1.0, 0.5, 0.2):
            y = 154.0 - (ratio * 92.0)
            guides.append({"y": round(y, 1), "label": format_currency_value(max_premium * ratio)})
        guides.append({"y": 164.0, "label": "$0"})

        spot_x = round(to_x(spot), 1)
        profile_line_path, profile_area_path = build_profile_paths(spot_x)
        markers = []
        regions = []
        slot_statuses = []
        marker_y_lookup = {"best-available": 58.0, "kairos-window": 82.0}
        for item in cards:
            slot_statuses.append(
                {
                    "label": item.get("slot_label") or "Trade Slot",
                    "value": "Ready" if item.get("tradeable") else ("Advisory" if item.get("available") else "Unavailable"),
                    "status_key": "qualified" if item.get("tradeable") else ("did-not-qualify" if item.get("available") else "inactive"),
                }
            )
            if not item.get("plot_marker") or item.get("short_strike_value") in {None, ""} or item.get("long_strike_value") in {None, ""}:
                continue
            short_x = round(to_x(float(item["short_strike_value"])), 1)
            long_x = round(to_x(float(item["long_strike_value"])), 1)
            y = marker_y_lookup.get(str(item.get("slot_key") or ""), 58.0)
            markers.append(
                {
                    "slot_key": item.get("slot_key") or "trade-slot",
                    "mode_key": item.get("mode_key") or "standard",
                    "mode_label": item.get("slot_label") or "Trade Slot",
                    "marker_label": item.get("marker_label") or "K",
                    "short_strike_label": format_number_value(item.get("short_strike_value")),
                    "long_strike_label": format_number_value(item.get("long_strike_value")),
                    "premium_label": item.get("premium_received") or "—",
                    "x": short_x,
                    "y": y,
                    "tooltip": (
                        f"{item.get('slot_label') or 'Trade Slot'} · "
                        f"{format_number_value(item.get('short_strike_value'))} / {format_number_value(item.get('long_strike_value'))} · "
                        f"{item.get('premium_received') or '—'}"
                    ),
                }
            )
            regions.append(
                {
                    "mode_key": item.get("mode_key") or "standard",
                    "mode_label": item.get("slot_label") or "Trade Slot",
                    "short_x": short_x,
                    "long_x": long_x,
                    "x": min(short_x, long_x),
                    "width": max(abs(short_x - long_x), 6.0),
                    "short_strike_label": format_number_value(item.get("short_strike_value")),
                    "long_strike_label": format_number_value(item.get("long_strike_value")),
                }
            )

        barrier_markers = []
        if lower_em is not None:
            barrier_markers.append(
                {
                    "label": "Expected Move Barrier",
                    "short_label": "EM Barrier",
                    "value_label": format_number_value(lower_em),
                    "css_class": "apollo-risk-reference-em",
                    "x": round(to_x(lower_em), 1),
                    "line_top_y": 38.0,
                    "line_bottom_y": 176.0,
                    "layout_key": "em-barrier",
                }
            )
        barrier_markers.append(
            {
                "label": "Prime Boundary",
                "short_label": "Window Boundary",
                "value_label": format_number_value(window_boundary),
                "css_class": "apollo-risk-reference-danger",
                "x": round(to_x(window_boundary), 1),
                "line_top_y": 38.0,
                "line_bottom_y": 176.0,
                "layout_key": "window-boundary",
            }
        )

        top_label_lanes = self._assign_credit_map_label_lanes(
            [
                {"key": "spot", "x": spot_x, "priority": 0},
                *[
                    {
                        "key": f"marker:{marker.get('slot_key')}",
                        "x": float(marker.get("x") or 0.0),
                        "priority": 1 if marker.get("slot_key") == "best-available" else 3,
                    }
                    for marker in markers
                ],
                *[
                    {
                        "key": f"barrier:{barrier.get('layout_key')}",
                        "x": float(barrier.get("x") or 0.0),
                        "priority": 2 if barrier.get("layout_key") == "window-boundary" else 4,
                    }
                    for barrier in barrier_markers
                ],
            ],
            min_gap=88.0,
            lane_count=4,
        )
        bottom_label_lanes = self._assign_credit_map_label_lanes(
            [
                {"key": "spot", "x": spot_x, "priority": 0},
                *[
                    {
                        "key": f"barrier:{barrier.get('layout_key')}",
                        "x": float(barrier.get("x") or 0.0),
                        "priority": 1 if barrier.get("layout_key") == "window-boundary" else 2,
                    }
                    for barrier in barrier_markers
                ],
            ],
            min_gap=72.0,
            lane_count=3,
        )

        for barrier in barrier_markers:
            barrier_key = f"barrier:{barrier.get('layout_key')}"
            barrier["label_y"] = 18.0 + (top_label_lanes.get(barrier_key, 0) * 12.0)
            barrier["value_y"] = 208.0 - (bottom_label_lanes.get(barrier_key, 0) * 12.0)
            barrier["tooltip"] = f"{barrier['label']} · {barrier['value_label']}"

        sorted_markers = sorted(markers, key=lambda item: float(item.get("x") or 0.0))
        prior_marker_layouts: List[Dict[str, Any]] = []
        for index, marker in enumerate(sorted_markers):
            marker_x = float(marker.get("x") or 0.0)
            marker_y = float(marker.get("y") or 0.0)
            if len(sorted_markers) > 1 and abs(marker_x - float(sorted_markers[1 - index].get("x") or 0.0)) < 110.0:
                side = "right" if index == 0 else "left"
            else:
                side = "right" if marker_x <= spot_x else "left"
            vertical_offset = 0.0
            for prior in prior_marker_layouts:
                if prior["side"] == side and abs(marker_x - prior["x"]) < 120.0:
                    vertical_offset += 12.0
            base_text_y = min(marker_y + 14.0 + vertical_offset, 132.0)
            marker["leader_x"] = round(marker_x + (16.0 if side == "right" else -16.0), 1)
            marker["label_x"] = round(marker_x + (26.0 if side == "right" else -26.0), 1)
            marker["text_anchor"] = "start" if side == "right" else "end"
            marker["title_y"] = round(base_text_y, 1)
            marker["detail_y"] = round(base_text_y + 14.0, 1)
            marker["premium_y"] = round(base_text_y + 28.0, 1)
            prior_marker_layouts.append({"side": side, "x": marker_x})

        if markers:
            positioning_note = (
                "Current live candidates are plotted against the current spot, the same-day ATM expected-move barrier, "
                f"and the hybrid boundary ({standard_boundary['boundary_binding_source']})."
            )
        else:
            positioning_note = (
                "No valid live-chain trades are currently plotted; the map still shows spot, the same-day ATM expected-move barrier, "
                f"and the Kairos hybrid boundary ({standard_boundary['boundary_binding_source']})."
            )

        return {
            "available": True,
            "positioning_note": positioning_note,
            "boundary_rule_used": standard_boundary["boundary_rule_used"],
            "boundary_binding_source": standard_boundary["boundary_binding_source"],
            "slot_statuses": slot_statuses,
            "guides": guides,
            "regions": regions,
            "markers": markers,
            "barrier_markers": barrier_markers,
            "plot_left_x": plot_left_x,
            "plot_right_x": plot_right_x,
            "baseline_y": baseline_y,
            "spot_x": spot_x,
            "spot_line_top_y": spot_line_top_y,
            "spot_label": format_number_value(spot),
            "spot_top_label_y": 18.0 + (top_label_lanes.get("spot", 0) * 12.0),
            "spot_bottom_label_y": 208.0 - (bottom_label_lanes.get("spot", 0) * 12.0),
            "profile_line_path": profile_line_path,
            "profile_area_path": profile_area_path,
        }

    @staticmethod
    def _assign_credit_map_label_lanes(items: List[Dict[str, Any]], *, min_gap: float, lane_count: int) -> Dict[str, int]:
        lane_map: Dict[str, int] = {}
        lanes: List[List[Dict[str, Any]]] = [[] for _ in range(max(lane_count, 1))]
        ordered_items = sorted(items, key=lambda item: (int(item.get("priority") or 0), float(item.get("x") or 0.0)))
        for item in ordered_items:
            item_x = float(item.get("x") or 0.0)
            selected_lane = None
            for lane_index, lane_items in enumerate(lanes):
                if all(abs(item_x - float(existing.get("x") or 0.0)) >= min_gap for existing in lane_items):
                    selected_lane = lane_index
                    break
            if selected_lane is None:
                selected_lane = min(
                    range(len(lanes)),
                    key=lambda lane_index: sum(
                        max(0.0, min_gap - abs(item_x - float(existing.get("x") or 0.0)))
                        for existing in lanes[lane_index]
                    ),
                )
            lane_map[str(item.get("key") or "")] = selected_lane
            lanes[selected_lane].append(item)
        return lane_map

    def _build_runner_trade_candidate_card_locked(
        self,
        latest_scan: KairosScanResult | None,
        metrics: Dict[str, Any] | None,
    ) -> Dict[str, Any]:
        hidden_payload = {
            "visible": False,
            "status": "idle",
            "title": "Trade Candidate Card",
            "eyebrow": "Synthetic / model-based",
            "message": "This card appears when the runner pauses on Prime.",
            "profiles": [],
            "summary_chips": [],
            "selected_profile_key": None,
            "selected_profile_label": None,
            "can_take_trade": False,
            "can_ignore_candidate": False,
            "can_resume_without_trade": False,
            "action_note": self._runner.last_candidate_action_note,
            "active_trade": self._runner.active_trade_lock_in.to_payload(self.display_timezone) if self._runner.active_trade_lock_in is not None else None,
            "disclaimer": "Simulation only. Estimated values only. No live option chain, execution, or fills are used here.",
            "is_override": False,
            "chain_source_label": "Estimated Model",
            "price_basis_label": "Simulated",
            "chain_status_label": "Modeled",
        }

        candidate_context = self._resolve_active_runner_trade_candidate_context_locked(latest_scan, metrics)
        if candidate_context is None:
            return hidden_payload

        profiles = candidate_context["profiles"]
        if candidate_context.get("is_override"):
            best_candidate = self._build_best_override_candidate(candidate_context, latest_scan=latest_scan, relax_final_gating=True)
            profiles = [best_candidate] if best_candidate is not None else []
        selected_profile_key = self._sync_runner_trade_candidate_selection_locked(candidate_context["event_id"], profiles)
        selected_profile = next((item for item in profiles if item["mode_key"] == selected_profile_key and item.get("available")), None)
        has_qualified_profiles = any(item.get("available") for item in profiles)
        take_trade_disabled_reason = ""
        if self._runner.active_trade_lock_in is not None:
            take_trade_disabled_reason = "A simulated Kairos trade is already active."
        elif not has_qualified_profiles:
            take_trade_disabled_reason = "No qualified profile is available to take."
        elif selected_profile is None:
            take_trade_disabled_reason = "Select a qualified profile before taking a trade."

        return {
            "visible": True,
            "status": "ready" if has_qualified_profiles else "no-candidates",
            "title": "Best Trade Override" if candidate_context.get("is_override") else "Trade Candidate Card",
            "eyebrow": "Advisory / override" if candidate_context.get("is_override") else "Synthetic / model-based",
            "message": (
                "Best Trade Override generated the strongest currently modeled spread for this paused replay moment."
                if candidate_context.get("is_override") and has_qualified_profiles
                else "Synthetic Kairos candidate set generated from simulated SPX, VIX, time, and modeled spread distance."
                if has_qualified_profiles
                else "No qualifying Kairos candidates found."
            ),
            "profiles": profiles,
            "summary_chips": self._build_trade_candidate_summary_chips(candidate_context),
            "selected_profile_key": selected_profile_key,
            "selected_profile_label": selected_profile["mode_label"] if selected_profile is not None else None,
            "can_take_trade": bool(selected_profile is not None and self._runner.active_trade_lock_in is None),
            "can_ignore_candidate": True,
            "can_resume_without_trade": True,
            "take_trade_disabled_reason": take_trade_disabled_reason,
            "action_note": self._runner.last_candidate_action_note,
            "active_trade": self._runner.active_trade_lock_in.to_payload(self.display_timezone) if self._runner.active_trade_lock_in is not None else None,
            "disclaimer": (
                f"Simulation only. {self._build_trade_override_caution_text(latest_scan)}"
                if candidate_context.get("is_override")
                else "Simulation only. Estimated credit and sizing only. No live option chain, execution, exits, or fills are used here."
            ),
            "is_override": bool(candidate_context.get("is_override")),
            "chain_source_label": candidate_context.get("chain_source_label") or "Estimated Model",
            "price_basis_label": candidate_context.get("price_basis_label") or "Simulated",
            "chain_status_label": candidate_context.get("chain_status_label") or "Modeled",
        }

    def _evaluate_runner_trade_candidate_context_locked(
        self,
        latest_scan: KairosScanResult | None,
        metrics: Dict[str, Any] | None,
    ) -> Dict[str, Any] | None:
        if (
            not self._has_valid_runner_pause_state_locked()
            or self._runner.pause_reason != "pause_on_window-open"
        ):
            return None

        return self._build_runner_trade_candidate_context_locked(latest_scan, metrics)

    def _resolve_active_runner_trade_candidate_context_locked(
        self,
        latest_scan: KairosScanResult | None,
        metrics: Dict[str, Any] | None,
    ) -> Dict[str, Any] | None:
        standard_context = self._evaluate_runner_trade_candidate_context_locked(latest_scan, metrics)
        if standard_context is not None:
            return standard_context
        return self._evaluate_runner_best_trade_override_context_locked(latest_scan, metrics)

    def _evaluate_runner_best_trade_override_context_locked(
        self,
        latest_scan: KairosScanResult | None,
        metrics: Dict[str, Any] | None,
    ) -> Dict[str, Any] | None:
        if not self._runner.best_trade_override_requested or not self._can_request_sim_best_trade_override_locked():
            return None
        if metrics is None or metrics.get("current_bar") is None:
            return None
        current_bar = metrics["current_bar"]
        scenario = self._current_runner_scenario_locked()
        cache_key = (
            self._runner.scenario_key,
            self._runner.current_bar_index,
            self._runner.pause_reason,
            True,
            latest_scan.kairos_state if latest_scan is not None else None,
            latest_scan.structure_status if latest_scan is not None else None,
            latest_scan.vix_status if latest_scan is not None else None,
        )
        if self._cached_runner_candidate_context_key == cache_key and self._cached_runner_candidate_context is not None:
            return deepcopy(self._cached_runner_candidate_context)
        context = self._enrich_candidate_context_profiles(self._build_trade_candidate_context_from_inputs_locked(
            event_id=f"{self._build_runner_trade_candidate_event_id_locked_for_bar(self._runner.pause_bar_number or max(0, self._runner.current_bar_index + 1))}:override",
            scenario_name=scenario.name if scenario is not None else "No scenario selected",
            simulated_time_label=format_simulated_clock(current_bar.simulated_time),
            bar_number=self._runner.current_bar_index + 1,
            spot=float(metrics.get("current_close") or 0.0),
            vix=float(metrics.get("current_vix") or 0.0),
            decision_time=current_bar.simulated_time,
            is_override=True,
        ), latest_scan=latest_scan)
        self._cached_runner_candidate_context_key = cache_key
        self._cached_runner_candidate_context = deepcopy(context)
        return context

    def _build_runner_trade_candidate_context_locked(
        self,
        latest_scan: KairosScanResult | None,
        metrics: Dict[str, Any] | None,
    ) -> Dict[str, Any] | None:
        if latest_scan is None or latest_scan.kairos_state != KairosState.WINDOW_OPEN.value or metrics is None or metrics.get("current_bar") is None:
            return None

        scenario = self._current_runner_scenario_locked()
        current_bar = metrics["current_bar"]
        cache_key = (
            self._runner.scenario_key,
            self._runner.current_bar_index,
            self._runner.pause_reason,
            False,
            latest_scan.kairos_state,
            latest_scan.structure_status,
            latest_scan.vix_status,
        )
        if self._cached_runner_candidate_context_key == cache_key and self._cached_runner_candidate_context is not None:
            return deepcopy(self._cached_runner_candidate_context)
        context = self._enrich_candidate_context_profiles(self._build_trade_candidate_context_from_inputs_locked(
            event_id=self._build_runner_trade_candidate_event_id_locked(),
            scenario_name=scenario.name if scenario is not None else "No scenario selected",
            simulated_time_label=format_simulated_clock(current_bar.simulated_time),
            bar_number=self._runner.current_bar_index + 1,
            spot=float(metrics.get("current_close") or 0.0),
            vix=float(metrics.get("current_vix") or 0.0),
            decision_time=current_bar.simulated_time,
            is_override=False,
        ), latest_scan=latest_scan)
        self._cached_runner_candidate_context_key = cache_key
        self._cached_runner_candidate_context = deepcopy(context)
        return context

    def _build_live_trade_candidate_context_locked(
        self,
        now: datetime,
        latest_scan: KairosScanResult | None,
    ) -> Dict[str, Any] | None:
        spx_snapshot = self._safe_latest_snapshot("^GSPC", query_type="kairos_live_best_trade_spx")
        vix_snapshot = self._safe_latest_snapshot("^VIX", query_type="kairos_live_best_trade_vix")
        spot = coerce_snapshot_number(spx_snapshot, "Latest Value")
        vix = coerce_snapshot_number(vix_snapshot, "Latest Value")
        if spot is None or vix is None:
            return None
        scenario_name = latest_scan.simulation_scenario_name if latest_scan is not None and latest_scan.simulation_scenario_name else "Live market"
        option_chain = self._get_live_kairos_option_chain(now)
        if not option_chain.get("success"):
            return {
                "event_id": f"live-best-trade:{now.isoformat()}",
                "profiles": [],
                "spot": float(spot),
                "vix": float(vix),
                "daily_move_anchor": 0.0,
                "risk_cap": float(self.config.apollo_account_value or 135000.0) * self.KAIROS_CANDIDATE_ACCOUNT_RISK_PERCENT,
                "scenario_name": scenario_name,
                "simulated_time_label": format_display_time(now, self.display_timezone),
                "chain_time_label": format_display_time(now, self.display_timezone),
                "bar_number": max(1, self._session.total_scans_completed),
                "is_override": True,
                "chain_success": False,
                "chain_source_label": "Schwab (Unavailable)",
                "price_basis_label": "Unavailable",
                "chain_status_label": option_chain.get("failure_label") or "Unavailable",
                "chain_message": option_chain.get("message") or "Same-day Schwab option chain unavailable.",
                "daily_move_anchor_source_label": "Unavailable",
                "daily_move_anchor_formula_label": "Same-day ATM straddle unavailable",
                "daily_move_anchor_contracts_label": "—",
                "source_badges": [
                    {"label": "CHAIN SOURCE", "value": "Schwab (Unavailable)"},
                    {"label": "PRICE BASIS", "value": "Unavailable"},
                ],
            }
        return self._enrich_candidate_context_profiles(self._build_live_trade_candidate_context_from_real_chain_locked(
            event_id=f"live-best-trade:{now.date().isoformat()}:{self._session.total_scans_completed}",
            scenario_name=scenario_name,
            simulated_time_label=format_display_time(now, self.display_timezone),
            bar_number=max(1, self._session.total_scans_completed),
            spot=float(spot),
            vix=float(vix),
            decision_time=now.time(),
            is_override=True,
            option_chain=option_chain,
        ), latest_scan=latest_scan)

    def _build_trade_candidate_context_from_inputs_locked(
        self,
        *,
        event_id: str,
        scenario_name: str,
        simulated_time_label: str,
        bar_number: int,
        spot: float,
        vix: float,
        decision_time: time,
        is_override: bool,
    ) -> Dict[str, Any]:
        if spot <= 0 or vix <= 0:
            return {
                "event_id": event_id,
                "profiles": [],
                "spot": spot,
                "vix": vix,
                "daily_move_anchor": 0.0,
                "daily_move_anchor_source_label": "Estimated Model",
                "daily_move_anchor_formula_label": "Unavailable",
                "daily_move_anchor_contracts_label": "—",
                "risk_cap": 0.0,
                "scenario_name": scenario_name,
                "simulated_time_label": simulated_time_label,
                "bar_number": bar_number,
                "is_override": is_override,
            }

        daily_move_anchor = spot * (vix / 100.0) / 16.0
        account_size = float(self.config.apollo_account_value or 135000.0)
        risk_cap = account_size * self.KAIROS_CANDIDATE_ACCOUNT_RISK_PERCENT
        time_factor = self._calculate_kairos_candidate_time_factor(decision_time)
        return {
            "event_id": event_id,
            "profiles": [
                self._build_synthetic_kairos_candidate_profile(
                    profile=profile,
                    spot=spot,
                    vix=vix,
                    simulated_time=decision_time,
                    daily_move_anchor=daily_move_anchor,
                    time_factor=time_factor,
                    risk_cap=risk_cap,
                )
                for profile in self.KAIROS_CANDIDATE_PROFILES
            ],
            "spot": spot,
            "vix": vix,
            "daily_move_anchor": daily_move_anchor,
            "daily_move_anchor_source_label": "Modeled VIX estimate",
            "daily_move_anchor_formula_label": "SPX × (VIX / 100) / 16",
            "daily_move_anchor_contracts_label": "VIX rule-of-16 model",
            "risk_cap": risk_cap,
            "scenario_name": scenario_name,
            "simulated_time_label": simulated_time_label,
            "chain_time_label": simulated_time_label,
            "bar_number": bar_number,
            "is_override": is_override,
            "chain_success": True,
            "chain_source_label": "Estimated Model",
            "price_basis_label": "Simulated",
            "chain_status_label": "Modeled",
            "source_badges": [
                {"label": "CHAIN SOURCE", "value": "Estimated Model"},
                {"label": "PRICE BASIS", "value": "Simulated"},
            ],
        }

    def _build_trade_candidate_summary_chips(self, candidate_context: Dict[str, Any]) -> List[Dict[str, Any]]:
        summary_chips = list(candidate_context.get("source_badges") or [])
        if candidate_context.get("chain_time_label"):
            summary_chips.append({"label": "CHAIN TIME", "value": candidate_context["chain_time_label"]})
        expected_move_value = self._coerce_float(candidate_context.get("daily_move_anchor"), fallback=0.0)
        standard_boundary = self._build_live_kairos_hybrid_boundary(
            spot=self._coerce_float(candidate_context.get("spot"), fallback=0.0),
            expected_move=expected_move_value,
            em_multiple_floor=self.KAIROS_HYBRID_EXPECTED_MOVE_MULTIPLE,
        )
        return summary_chips + [
            {"label": "SPX", "value": format_number_value(candidate_context["spot"])} ,
            {"label": "VIX", "value": format_number_value(candidate_context["vix"])} ,
            {"label": "Time", "value": candidate_context["simulated_time_label"]},
            {"label": "EM SOURCE", "value": candidate_context.get("daily_move_anchor_source_label") or "—"},
            {"label": "Expected move", "value": f"{expected_move_value:.2f} pts" if expected_move_value > 0 else "—"},
            {"label": "Percent floor", "value": f"{standard_boundary['percent_floor_points']:.2f} pts"},
            {"label": "EM floor", "value": f"{standard_boundary['em_floor_points']:.2f} pts"},
            {"label": "Boundary source", "value": str(standard_boundary["boundary_binding_source"])},
            {"label": "Risk cap", "value": format_currency_value(candidate_context["risk_cap"], decimals=0)},
        ]

    @staticmethod
    def _structure_fit_score(structure_status: str) -> float:
        return {
            "Bullish Confirmation": 1.0,
            "Developing": 0.75,
            "Chop / Unclear": 0.4,
            "Weakening": 0.2,
            "Failed": 0.0,
        }.get(structure_status, 0.35)

    @staticmethod
    def _timing_fit_score(timing_status: str, kairos_state: str) -> float:
        if timing_status == "Eligible" and kairos_state == KairosState.WINDOW_OPEN.value:
            return 1.0
        if timing_status == "Eligible":
            return 0.75
        if timing_status == "Locked":
            return 0.45
        if timing_status == "Late":
            return 0.35
        return 0.0

    @staticmethod
    def _vix_fit_score(vix_status: str) -> float:
        if vix_status == "Not Eligible":
            return 0.15
        if vix_status == "Caution":
            return 0.7
        return 1.0

    def _compute_fit_score(
        self,
        candidate: Dict[str, Any],
        *,
        latest_scan: KairosScanResult | None,
        candidate_context: Dict[str, Any],
    ) -> int:
        required_distance_points = float(
            candidate.get("hybrid_threshold")
            or self._build_live_kairos_hybrid_boundary(
                spot=float(candidate_context.get("spot") or 0.0),
                expected_move=float(candidate_context.get("daily_move_anchor") or 0.0),
                em_multiple_floor=float(candidate.get("em_multiple_floor") or self.KAIROS_HYBRID_EXPECTED_MOVE_MULTIPLE),
            )["hybrid_threshold"]
        )
        credit_component = min(1.0, float(candidate.get("credit_estimate_dollars") or 0.0) / self.KAIROS_CANDIDATE_MIN_CREDIT_DOLLARS)
        distance_component = min(1.0, float(candidate.get("distance_points") or 0.0) / required_distance_points) if required_distance_points > 0 else 0.0
        short_delta = abs(float(candidate.get("estimated_short_delta") or 0.0))
        if short_delta <= self.KAIROS_CANDIDATE_MAX_SHORT_DELTA:
            delta_component = 1.0
        else:
            delta_component = max(0.0, 1.0 - ((short_delta - self.KAIROS_CANDIDATE_MAX_SHORT_DELTA) / max(self.KAIROS_CANDIDATE_MAX_SHORT_DELTA, 0.01)))
        timing_component = self._timing_fit_score(latest_scan.timing_status if latest_scan is not None else "Eligible", latest_scan.kairos_state if latest_scan is not None else KairosState.WINDOW_OPEN.value)
        structure_component = self._structure_fit_score(latest_scan.structure_status if latest_scan is not None else "Developing")
        vix_component = self._vix_fit_score(latest_scan.vix_status if latest_scan is not None else "Caution")
        score = (
            (credit_component * 0.25)
            + (distance_component * 0.2)
            + (delta_component * 0.2)
            + (timing_component * 0.1)
            + (structure_component * 0.15)
            + (vix_component * 0.1)
        ) * 100.0
        return int(round(max(0.0, min(100.0, score))))

    def _estimate_otm_probability(self, candidate: Dict[str, Any], candidate_context: Dict[str, Any]) -> float:
        explicit_probability = candidate.get("estimated_otm_probability")
        if explicit_probability is not None:
            return float(explicit_probability)
        short_delta = candidate.get("estimated_short_delta")
        if short_delta is not None:
            return max(0.05, min(0.99, 1.0 - abs(float(short_delta))))
        distance_ratio = float(candidate.get("distance_points") or 0.0) / max(float(candidate_context.get("daily_move_anchor") or 1.0), 1.0)
        return max(0.05, min(0.99, 1.0 - abs(self._estimate_short_delta(distance_ratio))))

    def _summarize_candidate_eligibility_gaps(
        self,
        candidate: Dict[str, Any],
        *,
        latest_scan: KairosScanResult | None,
        candidate_context: Dict[str, Any],
    ) -> Dict[str, Any]:
        required_distance_points = float(
            candidate.get("hybrid_threshold")
            or self._build_live_kairos_hybrid_boundary(
                spot=float(candidate_context.get("spot") or 0.0),
                expected_move=float(candidate_context.get("daily_move_anchor") or 0.0),
                em_multiple_floor=float(candidate.get("em_multiple_floor") or self.KAIROS_HYBRID_EXPECTED_MOVE_MULTIPLE),
            )["hybrid_threshold"]
        )
        actual_distance_points = float(candidate.get("distance_points") or 0.0)
        credit_estimate = float(candidate.get("credit_estimate_dollars") or 0.0)
        short_delta = abs(float(candidate.get("estimated_short_delta") or 0.0))
        timing_status = latest_scan.timing_status if latest_scan is not None else "Eligible"
        structure_status = latest_scan.structure_status if latest_scan is not None else "Developing"
        kairos_state = latest_scan.kairos_state if latest_scan is not None else KairosState.WINDOW_OPEN.value
        vix_status = latest_scan.vix_status if latest_scan is not None else "Caution"

        credit_gap = max(0.0, self.KAIROS_CANDIDATE_MIN_CREDIT_DOLLARS - credit_estimate)
        distance_gap = max(0.0, required_distance_points - actual_distance_points)
        delta_gap = max(0.0, short_delta - self.KAIROS_CANDIDATE_MAX_SHORT_DELTA)
        timing_ok = timing_status == "Eligible" and kairos_state == KairosState.WINDOW_OPEN.value
        structure_ok = structure_status == "Bullish Confirmation"
        vix_ok = vix_status != "Not Eligible"

        gaps: List[str] = []
        if credit_gap > 0:
            gaps.append(f"Insufficient credit: short by {format_currency_value(credit_gap)} per contract.")
        if distance_gap > 0:
            gaps.append(
                f"Strike too close: distance short by {distance_gap:.2f} points versus {candidate.get('boundary_rule_used') or 'hybrid boundary'}."
            )
        if delta_gap > 0:
            gaps.append(f"Delta too high: exceeds target by {delta_gap:.2f}.")
        if not timing_ok:
            gaps.append("Timing not ideal: Kairos is not yet in a confirmed Prime state.")
        if not structure_ok:
            gaps.append(f"Structure not confirmed: current structure is {structure_status}.")
        if not vix_ok:
            gaps.append("VIX outside preferred range.")
        for reason in candidate.get("rejection_reasons") or []:
            if reason and reason not in gaps:
                gaps.append(str(reason))

        return {
            "credit_gap": round(credit_gap, 2),
            "distance_gap": round(distance_gap, 2),
            "delta_gap": round(delta_gap, 2),
            "timing_ok": timing_ok,
            "structure_ok": structure_ok,
            "vix_ok": vix_ok,
            "fully_valid": credit_gap <= 0 and distance_gap <= 0 and delta_gap <= 0 and timing_ok and structure_ok and vix_ok and bool(candidate.get("available")),
            "messages": gaps,
        }

    def _build_closest_valid_trade_guidance(self, gap_summary: Dict[str, Any], candidate: Dict[str, Any]) -> List[str]:
        guidance: List[str] = []
        if gap_summary.get("distance_gap", 0.0) > 0:
            guidance.append(f"{gap_summary['distance_gap']:.2f} more points of distance")
        if gap_summary.get("credit_gap", 0.0) > 0:
            guidance.append(f"{format_currency_value(gap_summary['credit_gap'])} more credit per contract")
        if gap_summary.get("delta_gap", 0.0) > 0:
            guidance.append(
                f"delta improvement from {float(candidate.get('estimated_short_delta') or 0.0):.2f} to <= {self.KAIROS_CANDIDATE_MAX_SHORT_DELTA:.2f}"
            )
        return guidance

    def _derive_candidate_confidence_label(
        self,
        candidate: Dict[str, Any],
        *,
        latest_scan: KairosScanResult | None,
        candidate_context: Dict[str, Any],
    ) -> str:
        confidence_score = 0.0
        if candidate_context.get("chain_success", True):
            confidence_score += 0.35
        if candidate.get("estimated_short_delta") not in (None, ""):
            confidence_score += 0.25
        if latest_scan is not None and latest_scan.timing_status == "Eligible":
            confidence_score += 0.15
        if latest_scan is not None and latest_scan.structure_status in {"Bullish Confirmation", "Developing"}:
            confidence_score += 0.15
        classification_note = (latest_scan.classification_note if latest_scan is not None else "") or ""
        if "provisional" not in classification_note.lower():
            confidence_score += 0.1
        if confidence_score >= 0.8:
            return "High"
        if confidence_score >= 0.55:
            return "Moderate"
        return "Low"

    def _decorate_candidate_profile_with_insights(
        self,
        candidate: Dict[str, Any],
        *,
        latest_scan: KairosScanResult | None,
        candidate_context: Dict[str, Any],
    ) -> Dict[str, Any]:
        gap_summary = self._summarize_candidate_eligibility_gaps(candidate, latest_scan=latest_scan, candidate_context=candidate_context)
        fit_score = self._compute_fit_score(candidate, latest_scan=latest_scan, candidate_context=candidate_context)
        otm_probability = self._estimate_otm_probability(candidate, candidate_context)
        confidence_label = self._derive_candidate_confidence_label(candidate, latest_scan=latest_scan, candidate_context=candidate_context)
        closest_guidance = self._build_closest_valid_trade_guidance(gap_summary, candidate)

        detail_rows = list(candidate.get("detail_rows") or [])
        existing_labels = {str(row.get("label") or "") for row in detail_rows}
        for row in [
            {"label": "FIT SCORE", "value": f"{fit_score} / 100"},
            {"label": "CONFIDENCE", "value": confidence_label},
            {"label": "Est. OTM probability", "value": format_percent_value(otm_probability)},
        ]:
            if row["label"] not in existing_labels:
                detail_rows.append(row)

        return {
            **candidate,
            "fit_score": fit_score,
            "fit_score_display": f"{fit_score} / 100",
            "confidence_label": confidence_label,
            "estimated_otm_probability": round(otm_probability, 2),
            "estimated_otm_probability_display": format_percent_value(otm_probability),
            "eligibility_gaps": gap_summary["messages"],
            "closest_valid_guidance": closest_guidance,
            "is_fully_valid": gap_summary["fully_valid"],
            "detail_rows": detail_rows,
        }

    def _enrich_candidate_context_profiles(
        self,
        candidate_context: Dict[str, Any],
        *,
        latest_scan: KairosScanResult | None,
    ) -> Dict[str, Any]:
        profiles = [
            self._decorate_candidate_profile_with_insights(profile, latest_scan=latest_scan, candidate_context=candidate_context)
            for profile in candidate_context.get("profiles") or []
        ]
        return {
            **candidate_context,
            "profiles": profiles,
        }

    def _build_best_override_candidate(
        self,
        candidate_context: Dict[str, Any],
        *,
        latest_scan: KairosScanResult | None,
        relax_final_gating: bool = True,
    ) -> Dict[str, Any] | None:
        enriched_context = self._enrich_candidate_context_profiles(candidate_context, latest_scan=latest_scan)
        profiles = list(enriched_context.get("profiles") or [])
        if not profiles:
            return None
        qualified_profiles = [item for item in profiles if item.get("available")]
        candidate_pool = qualified_profiles or (profiles if relax_final_gating else [])
        if not candidate_pool:
            return None
        candidate_pool.sort(
            key=lambda item: (
                int(item.get("fit_score") or 0),
                float(item.get("credit_estimate_dollars") or 0.0),
                float(item.get("distance_points") or 0.0),
            ),
            reverse=True,
        )
        return candidate_pool[0]

    def _get_live_kairos_option_chain(self, now: datetime) -> Dict[str, Any]:
        if self._runtime.mode != KairosMode.LIVE:
            return {"success": False, "message": "Live Schwab chain is only used in Live Mode."}
        if self._get_effective_market_session_status(now) != "Open":
            return {"success": False, "message": "Same-day Schwab option chain is only available while the live market session is open.", "failure_label": "Market closed"}
        return self.options_chain_service.get_spx_option_chain_summary(now.date())

    def _get_live_market_state_message(self, now: datetime) -> str:
        market_status = self._get_live_market_session_status(now)
        if market_status == "Ended":
            return "Market closed - Live Kairos standing by."
        return "Market closed - Live Kairos standing by."

    def _build_live_kairos_hybrid_boundary(
        self,
        *,
        spot: float,
        expected_move: float,
        em_multiple_floor: float,
    ) -> Dict[str, Any]:
        percent_floor_points = max(spot * (self.KAIROS_CANDIDATE_MIN_DISTANCE_PERCENT / 100.0), 5.0)
        em_floor_points = max(0.0, expected_move * em_multiple_floor)
        hybrid_threshold = max(percent_floor_points, em_floor_points)
        if em_floor_points > percent_floor_points:
            binding_source = "EM Floor"
        else:
            binding_source = "Percent Floor"
        return {
            "percent_floor": round(self.KAIROS_CANDIDATE_MIN_DISTANCE_PERCENT, 2),
            "percent_floor_points": round(percent_floor_points, 2),
            "em_multiple_floor": round(em_multiple_floor, 2),
            "em_floor_points": round(em_floor_points, 2),
            "hybrid_threshold": round(hybrid_threshold, 2),
            "boundary_binding_source": binding_source,
            "boundary_rule_used": (
                f"max({self.KAIROS_CANDIDATE_MIN_DISTANCE_PERCENT:.1f}% spot floor, {em_multiple_floor:.2f}x expected move)"
            ),
        }

    def _build_live_trade_candidate_context_from_real_chain_locked(
        self,
        *,
        event_id: str,
        scenario_name: str,
        simulated_time_label: str,
        bar_number: int,
        spot: float,
        vix: float,
        decision_time: time,
        is_override: bool,
        option_chain: Dict[str, Any],
    ) -> Dict[str, Any]:
        account_size = float(self.config.apollo_account_value or 135000.0)
        risk_cap = account_size * self.KAIROS_CANDIDATE_ACCOUNT_RISK_PERCENT
        time_factor = self._calculate_kairos_candidate_time_factor(decision_time)
        normalized_puts = self._normalize_live_option_contracts(option_chain.get("puts") or [])
        normalized_calls = self._normalize_live_option_contracts(option_chain.get("calls") or [])
        expected_move_details = self._estimate_live_expected_move_from_chain(
            normalized_puts=normalized_puts,
            normalized_calls=normalized_calls,
            spot=spot,
        )
        daily_move_anchor = float(expected_move_details.get("expected_move") or 0.0)
        return {
            "event_id": event_id,
            "profiles": [
                self._scan_live_kairos_candidates_for_widths(
                    profile=profile,
                    spot=spot,
                    vix=vix,
                    simulated_time=decision_time,
                    daily_move_anchor=daily_move_anchor,
                    time_factor=time_factor,
                    risk_cap=risk_cap,
                    normalized_puts=normalized_puts,
                    daily_move_anchor_source_label=str(expected_move_details.get("source_label") or "Same-day ATM straddle"),
                    daily_move_anchor_contracts_label=str(expected_move_details.get("contracts_label") or "—"),
                )
                for profile in self.KAIROS_CANDIDATE_PROFILES
            ],
            "spot": spot,
            "vix": vix,
            "daily_move_anchor": daily_move_anchor,
            "daily_move_anchor_source_label": str(expected_move_details.get("source_label") or "Same-day ATM straddle"),
            "daily_move_anchor_formula_label": str(expected_move_details.get("formula_label") or "ATM put premium + ATM call premium"),
            "daily_move_anchor_contracts_label": str(expected_move_details.get("contracts_label") or "—"),
            "percent_floor": round(self.KAIROS_CANDIDATE_MIN_DISTANCE_PERCENT, 2),
            "risk_cap": risk_cap,
            "scenario_name": scenario_name,
            "simulated_time_label": simulated_time_label,
            "chain_time_label": simulated_time_label,
            "bar_number": bar_number,
            "is_override": is_override,
            "chain_success": True,
            "chain_source_label": "Schwab Live",
            "price_basis_label": "Live bid/ask + midpoint hybrid",
            "chain_status_label": "Available",
            "source_badges": [
                {"label": "CHAIN SOURCE", "value": "Schwab Live"},
                {"label": "PRICE BASIS", "value": "Live bid/ask + midpoint hybrid"},
                {"label": "WIDTH SCAN", "value": f"up to {max(self.LIVE_KAIROS_SPREAD_WIDTH_SCAN_POINTS)} pts"},
            ],
        }

    @staticmethod
    def _rank_live_width_candidate(candidate: Dict[str, Any]) -> tuple[int, float, float, float]:
        return (
            int(bool(candidate.get("available"))),
            float(candidate.get("credit_estimate_dollars") or 0.0),
            float(candidate.get("estimated_otm_probability") or 0.0),
            float(candidate.get("distance_points") or 0.0),
        )

    def _scan_live_kairos_candidates_for_widths(
        self,
        *,
        profile: Dict[str, Any],
        spot: float,
        vix: float,
        simulated_time: time,
        daily_move_anchor: float,
        time_factor: float,
        risk_cap: float,
        normalized_puts: List[Dict[str, Any]],
        daily_move_anchor_source_label: str,
        daily_move_anchor_contracts_label: str,
    ) -> Dict[str, Any]:
        hybrid_boundary = self._build_live_kairos_hybrid_boundary(
            spot=spot,
            expected_move=daily_move_anchor,
            em_multiple_floor=self.KAIROS_HYBRID_EXPECTED_MOVE_MULTIPLE,
        )
        target_distance = float(hybrid_boundary["hybrid_threshold"])
        projected_short_strike = int((spot - target_distance) / 5.0) * 5
        blocked_handle = projected_short_strike % 100 in {0, 50}
        short_target_strike = projected_short_strike - 5 if blocked_handle else projected_short_strike
        short_put = self._find_live_contract_at_or_below_strike(normalized_puts, short_target_strike)
        rejection_reasons: List[str] = []
        if short_put is None:
            rejection_reasons.append("No same-day Schwab short-leg contract was available at the required distance.")
            return self._build_unavailable_live_candidate_profile(
                profile,
                simulated_time,
                target_distance,
                risk_cap,
                rejection_reasons,
                hybrid_boundary=hybrid_boundary,
            )

        width_candidates: List[Dict[str, Any]] = []
        for spread_width in self.LIVE_KAIROS_SPREAD_WIDTH_SCAN_POINTS:
            long_put = self._find_live_contract_by_strike(normalized_puts, short_put["strike"] - spread_width)
            if long_put is None:
                continue
            width_candidates.append(
                self._build_live_kairos_candidate_from_real_chain(
                    profile=profile,
                    spot=spot,
                    vix=vix,
                    simulated_time=simulated_time,
                    daily_move_anchor=daily_move_anchor,
                    hybrid_boundary=hybrid_boundary,
                    time_factor=time_factor,
                    risk_cap=risk_cap,
                    spread_width=spread_width,
                    short_put=short_put,
                    long_put=long_put,
                    daily_move_anchor_source_label=daily_move_anchor_source_label,
                    daily_move_anchor_contracts_label=daily_move_anchor_contracts_label,
                )
            )

        if not width_candidates:
            rejection_reasons.append(
                f"No matching same-day Schwab long-leg contract was available for widths {', '.join(str(width) for width in self.LIVE_KAIROS_SPREAD_WIDTH_SCAN_POINTS)} points."
            )
            return self._build_unavailable_live_candidate_profile(
                profile,
                simulated_time,
                target_distance,
                risk_cap,
                rejection_reasons,
                hybrid_boundary=hybrid_boundary,
                short_strike=short_put["strike"],
                scanned_widths=self.LIVE_KAIROS_SPREAD_WIDTH_SCAN_POINTS,
            )

        qualified_candidates = [item for item in width_candidates if item.get("available")]
        candidate_pool = qualified_candidates or width_candidates
        candidate_pool.sort(key=self._rank_live_width_candidate, reverse=True)
        return candidate_pool[0]

    def _build_live_kairos_candidate_from_real_chain(
        self,
        *,
        profile: Dict[str, Any],
        spot: float,
        vix: float,
        simulated_time: time,
        daily_move_anchor: float,
        hybrid_boundary: Dict[str, Any],
        time_factor: float,
        risk_cap: float,
        spread_width: int,
        short_put: Dict[str, Any],
        long_put: Dict[str, Any],
        daily_move_anchor_source_label: str,
        daily_move_anchor_contracts_label: str,
    ) -> Dict[str, Any]:
        target_distance = float(hybrid_boundary["hybrid_threshold"])
        rejection_reasons: List[str] = []

        conservative_credit = max(0.0, (short_put.get("bid") or 0.0) - (long_put.get("ask") or 0.0))
        credit = self._calculate_live_credit(short_put, long_put)
        credit_dollars = round(max(0.0, credit * 100.0), 2)
        conservative_credit_dollars = round(max(0.0, conservative_credit * 100.0), 2)
        distance_points = max(0.0, spot - short_put["strike"])
        distance_percent = (distance_points / spot) * 100.0 if spot else 0.0
        distance_ratio = (distance_points / daily_move_anchor) if daily_move_anchor > 0 else 0.0
        short_delta = short_put.get("delta_abs")
        estimated_short_delta = round(short_delta if short_delta is not None else self._estimate_short_delta(distance_ratio), 2)
        estimated_otm_probability = max(0.05, min(0.99, 1.0 - abs(estimated_short_delta)))
        per_contract_max_loss = round(max(0.0, (spread_width * 100.0) - credit_dollars), 2)
        contract_budget = int(risk_cap // per_contract_max_loss) if per_contract_max_loss > 0 else self.KAIROS_CANDIDATE_MAX_CONTRACTS
        recommended_contracts = min(self.KAIROS_CANDIDATE_MAX_CONTRACTS, contract_budget) if contract_budget > 0 else 0
        total_credit = round(credit_dollars * recommended_contracts, 2)
        max_loss_dollars = round(per_contract_max_loss * recommended_contracts, 2)

        if distance_points < target_distance:
            rejection_reasons.append(
                f"Distance is inside the hybrid boundary ({hybrid_boundary['boundary_rule_used']})."
            )
        if credit_dollars < self.KAIROS_CANDIDATE_MIN_CREDIT_DOLLARS:
            rejection_reasons.append("Current live candidate is below the minimum credit threshold.")
        if contract_budget < 1:
            rejection_reasons.append("Risk budget is too small for one contract.")

        available = not rejection_reasons
        rationale = (
            f"Same-day Schwab chain selected the {int(short_put['strike'])} / {int(long_put['strike'])} spread. Talos entry pricing uses conservative short bid minus long ask, while Kairos ranking keeps the midpoint sanity floor."
            if available
            else rejection_reasons[0]
        )
        return {
            "mode_key": profile["key"],
            "mode_label": profile["label"],
            "mode_descriptor": profile["descriptor"],
            "position_label": "Put Credit Spread",
            "spread_type": "Put Credit Spread",
            "available": available,
            "qualification_status": "Qualified" if available else "Did not qualify",
            "status_key": "qualified" if available else "did-not-qualify",
            "short_strike": int(short_put["strike"]),
            "long_strike": int(long_put["strike"]),
            "strike_pair": f"{int(short_put['strike'])} / {int(long_put['strike'])}",
            "spread_width": spread_width,
            "spread_width_display": f"{spread_width} pts",
            "simulated_time": format_simulated_clock(simulated_time),
            "credit_estimate_dollars": credit_dollars,
            "conservative_credit_dollars": conservative_credit_dollars,
            "credit_estimate_display": format_currency_value(credit_dollars),
            "premium_received_dollars": total_credit,
            "premium_received_display": format_currency_value(total_credit),
            "recommended_contracts": recommended_contracts,
            "recommended_contracts_display": str(recommended_contracts),
            "contract_size_display": str(recommended_contracts),
            "distance_points": round(distance_points, 2),
            "distance_points_display": f"{distance_points:.2f} pts",
            "distance_to_short_display": f"{distance_points:.2f} pts",
            "distance_percent": round(distance_percent, 2),
            "distance_percent_display": f"{distance_percent:.2f}%",
            "daily_move_multiple": round(distance_ratio, 2),
            "daily_move_multiple_display": f"{distance_ratio:.2f}x",
            "em_multiple_display": f"{distance_ratio:.2f}x EM",
            "expected_move_used": round(daily_move_anchor, 2),
            "expected_move_source": "same_day_atm_straddle",
            "em_multiple_floor": hybrid_boundary["em_multiple_floor"],
            "percent_floor": hybrid_boundary["percent_floor"],
            "percent_floor_points": hybrid_boundary["percent_floor_points"],
            "em_floor_points": hybrid_boundary["em_floor_points"],
            "hybrid_threshold": hybrid_boundary["hybrid_threshold"],
            "boundary_binding_source": hybrid_boundary["boundary_binding_source"],
            "boundary_rule_used": hybrid_boundary["boundary_rule_used"],
            "actual_distance_to_short": round(distance_points, 2),
            "actual_em_multiple": round(distance_ratio, 2),
            "fallback_used": "no",
            "fallback_rule_name": "",
            "short_leg_bid": round(float(short_put.get("bid") or 0.0), 2),
            "long_leg_ask": round(float(long_put.get("ask") or 0.0), 2),
            "estimated_short_delta": estimated_short_delta,
            "estimated_short_delta_display": f"{estimated_short_delta:.2f}",
            "estimated_otm_probability": round(estimated_otm_probability, 2),
            "estimated_otm_probability_display": format_percent_value(estimated_otm_probability),
            "max_loss_per_contract": per_contract_max_loss,
            "max_loss_per_contract_display": format_currency_value(per_contract_max_loss),
            "max_loss_dollars": max_loss_dollars,
            "max_loss_display": format_currency_value(max_loss_dollars),
            "risk_cap_display": format_currency_value(risk_cap, decimals=0),
            "time_factor": round(time_factor, 2),
            "vix_factor": round(min(1.30, max(0.85, vix / 20.0)), 2),
            "selection_enabled": available,
            "rationale": rationale,
            "detail_rows": [
                {"label": "CHAIN SOURCE", "value": "Schwab Live"},
                {"label": "PRICE BASIS", "value": "Live bid/ask + midpoint hybrid"},
                {"label": "EXPECTED MOVE", "value": f"{daily_move_anchor:.2f} pts" if daily_move_anchor > 0 else "—"},
                {"label": "EM SOURCE", "value": daily_move_anchor_source_label},
                {"label": "EM INPUTS", "value": daily_move_anchor_contracts_label},
                {"label": "Percent floor", "value": f"{hybrid_boundary['percent_floor']:.2f}% ({hybrid_boundary['percent_floor_points']:.2f} pts)"},
                {"label": "EM floor", "value": f"{hybrid_boundary['em_multiple_floor']:.2f}x ({hybrid_boundary['em_floor_points']:.2f} pts)"},
                {"label": "Boundary source", "value": str(hybrid_boundary["boundary_binding_source"])},
                {"label": "Spread type", "value": "Put Credit Spread"},
                {"label": "Short strike", "value": str(int(short_put["strike"]))},
                {"label": "Long strike", "value": str(int(long_put["strike"]))},
                {"label": "SPREAD WIDTH", "value": f"{spread_width} pts"},
                {"label": "WIDTH SCAN", "value": f"up to {max(self.LIVE_KAIROS_SPREAD_WIDTH_SCAN_POINTS)} pts"},
                {"label": "Hybrid boundary", "value": f"{target_distance:.2f} pts"},
                {"label": "Distance from SPX", "value": f"{distance_points:.2f} pts"},
                {"label": "Distance from SPX %", "value": f"{distance_percent:.2f}%"},
                {"label": "Estimated OTM probability", "value": format_percent_value(estimated_otm_probability)},
                {"label": "Live credit per contract", "value": format_currency_value(credit_dollars)},
                {"label": "Live total premium received", "value": format_currency_value(total_credit)},
                {"label": "Live max loss", "value": format_currency_value(max_loss_dollars)},
                {"label": "Suggested contracts", "value": str(recommended_contracts)},
            ],
            "no_trade_message": rejection_reasons[0] if rejection_reasons else "",
            "rejection_reasons": rejection_reasons,
        }

    def _build_unavailable_live_candidate_profile(
        self,
        profile: Dict[str, Any],
        simulated_time: time,
        target_distance: float,
        risk_cap: float,
        rejection_reasons: List[str],
        *,
        hybrid_boundary: Dict[str, Any] | None = None,
        short_strike: float | None = None,
        scanned_widths: tuple[int, ...] | None = None,
    ) -> Dict[str, Any]:
        width_choices = tuple(scanned_widths or self.LIVE_KAIROS_SPREAD_WIDTH_SCAN_POINTS)
        default_width = width_choices[0] if width_choices else self.KAIROS_CANDIDATE_SPREAD_WIDTH_POINTS
        hybrid_boundary = hybrid_boundary or {
            "percent_floor": round(self.KAIROS_CANDIDATE_MIN_DISTANCE_PERCENT, 2),
            "percent_floor_points": round(target_distance, 2),
            "em_multiple_floor": round(self.KAIROS_HYBRID_EXPECTED_MOVE_MULTIPLE, 2),
            "em_floor_points": 0.0,
            "hybrid_threshold": round(target_distance, 2),
            "boundary_binding_source": "Percent Floor",
            "boundary_rule_used": (
                f"max({self.KAIROS_CANDIDATE_MIN_DISTANCE_PERCENT:.1f}% spot floor, {self.KAIROS_HYBRID_EXPECTED_MOVE_MULTIPLE:.2f}x expected move)"
            ),
        }
        return {
            "mode_key": profile["key"],
            "mode_label": profile["label"],
            "mode_descriptor": profile["descriptor"],
            "position_label": "Put Credit Spread",
            "spread_type": "Put Credit Spread",
            "available": False,
            "qualification_status": "Did not qualify",
            "status_key": "did-not-qualify",
            "short_strike": int(short_strike) if short_strike is not None else 0,
            "long_strike": int(short_strike - default_width) if short_strike is not None else 0,
            "strike_pair": "—",
            "spread_width": default_width,
            "spread_width_display": f"{default_width} pts",
            "simulated_time": format_simulated_clock(simulated_time),
            "credit_estimate_dollars": 0.0,
            "credit_estimate_display": format_currency_value(0.0),
            "premium_received_dollars": 0.0,
            "premium_received_display": format_currency_value(0.0),
            "recommended_contracts": 0,
            "recommended_contracts_display": "0",
            "contract_size_display": "0",
            "distance_points": round(target_distance, 2),
            "distance_points_display": f"{target_distance:.2f} pts",
            "distance_to_short_display": f"{target_distance:.2f} pts",
            "distance_percent": 0.0,
            "distance_percent_display": "0.00%",
            "daily_move_multiple": 0.0,
            "daily_move_multiple_display": "0.00x",
            "em_multiple_display": "0.00x EM",
            "expected_move_source": "same_day_atm_straddle",
            "em_multiple_floor": hybrid_boundary["em_multiple_floor"],
            "percent_floor": hybrid_boundary["percent_floor"],
            "percent_floor_points": hybrid_boundary["percent_floor_points"],
            "em_floor_points": hybrid_boundary["em_floor_points"],
            "hybrid_threshold": hybrid_boundary["hybrid_threshold"],
            "boundary_binding_source": hybrid_boundary["boundary_binding_source"],
            "boundary_rule_used": hybrid_boundary["boundary_rule_used"],
            "actual_distance_to_short": round(target_distance, 2),
            "actual_em_multiple": 0.0,
            "fallback_used": "no",
            "fallback_rule_name": "",
            "estimated_short_delta": 0.0,
            "estimated_short_delta_display": "0.00",
            "estimated_otm_probability": 0.0,
            "estimated_otm_probability_display": format_percent_value(0.0),
            "max_loss_per_contract": 0.0,
            "max_loss_per_contract_display": format_currency_value(0.0),
            "max_loss_dollars": 0.0,
            "max_loss_display": format_currency_value(0.0),
            "risk_cap_display": format_currency_value(risk_cap, decimals=0),
            "time_factor": 0.0,
            "vix_factor": 0.0,
            "selection_enabled": False,
            "rationale": rejection_reasons[0] if rejection_reasons else "No same-day Schwab chain candidate was available.",
            "detail_rows": [
                {"label": "CHAIN SOURCE", "value": "Schwab Live"},
                {"label": "PRICE BASIS", "value": "Live bid/ask + midpoint hybrid"},
                {"label": "SPREAD WIDTH", "value": f"{default_width} pts"},
                {"label": "WIDTH SCAN", "value": f"up to {max(width_choices)} pts" if width_choices else f"{default_width} pts"},
                {"label": "Percent floor", "value": f"{hybrid_boundary['percent_floor']:.2f}% ({hybrid_boundary['percent_floor_points']:.2f} pts)"},
                {"label": "EM floor", "value": f"{hybrid_boundary['em_multiple_floor']:.2f}x ({hybrid_boundary['em_floor_points']:.2f} pts)"},
                {"label": "Boundary source", "value": str(hybrid_boundary["boundary_binding_source"])},
                {"label": "Target distance", "value": f"{target_distance:.2f} pts"},
                {"label": "Risk cap", "value": format_currency_value(risk_cap, decimals=0)},
            ],
            "no_trade_message": rejection_reasons[0] if rejection_reasons else "No same-day Schwab chain candidate was available.",
            "rejection_reasons": rejection_reasons,
        }

    def _normalize_live_option_contracts(self, contracts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        def maybe_float(value: Any) -> float | None:
            try:
                return float(value)
            except (TypeError, ValueError):
                return None

        normalized: List[Dict[str, Any]] = []
        for contract in contracts:
            strike = maybe_float(contract.get("strike"))
            if strike is None:
                continue
            delta = maybe_float(contract.get("delta"))
            normalized.append(
                {
                    "strike": strike,
                    "bid": maybe_float(contract.get("bid")) or 0.0,
                    "ask": maybe_float(contract.get("ask")) or 0.0,
                    "last": maybe_float(contract.get("last")) or 0.0,
                    "mark": maybe_float(contract.get("mark")) or 0.0,
                    "delta": delta,
                    "delta_abs": abs(delta) if delta is not None else None,
                    "open_interest": maybe_float(contract.get("open_interest")) or 0.0,
                    "total_volume": maybe_float(contract.get("total_volume")) or 0.0,
                }
            )
        return sorted(normalized, key=lambda item: item["strike"], reverse=True)

    def _find_live_contract_at_or_below_strike(self, contracts: List[Dict[str, Any]], strike: float) -> Dict[str, Any] | None:
        eligible = [item for item in contracts if item["strike"] <= strike]
        return eligible[0] if eligible else None

    def _find_live_contract_at_or_above_strike(self, contracts: List[Dict[str, Any]], strike: float) -> Dict[str, Any] | None:
        eligible = [item for item in contracts if item["strike"] >= strike]
        return eligible[-1] if eligible else None

    def _find_live_contract_by_strike(self, contracts: List[Dict[str, Any]], strike: float) -> Dict[str, Any] | None:
        return next((item for item in contracts if int(item["strike"]) == int(strike)), None)

    @staticmethod
    def _find_nearest_live_contract(contracts: List[Dict[str, Any]], strike: float) -> Dict[str, Any] | None:
        if not contracts:
            return None
        return min(contracts, key=lambda item: abs(float(item.get("strike") or 0.0) - strike))

    def _estimate_live_expected_move_from_chain(
        self,
        *,
        normalized_puts: List[Dict[str, Any]],
        normalized_calls: List[Dict[str, Any]],
        spot: float,
    ) -> Dict[str, Any]:
        selected_put = self._find_live_contract_at_or_below_strike(normalized_puts, spot) or self._find_nearest_live_contract(normalized_puts, spot)
        selected_call = self._find_live_contract_at_or_above_strike(normalized_calls, spot) or self._find_nearest_live_contract(normalized_calls, spot)

        put_anchor = self._premium_anchor(selected_put) if selected_put is not None else 0.0
        call_anchor = self._premium_anchor(selected_call) if selected_call is not None else 0.0
        expected_move = 0.0
        source_label = "Same-day ATM straddle"
        formula_label = "ATM put premium + ATM call premium using midpoint when bid/ask are both available, else mark/bid/ask/last"
        contracts_label = "—"

        if put_anchor > 0 and call_anchor > 0:
            expected_move = put_anchor + call_anchor
            contracts_label = f"{int(selected_put['strike'])}P {put_anchor:.2f} + {int(selected_call['strike'])}C {call_anchor:.2f}"
        elif put_anchor > 0:
            expected_move = put_anchor * 2.0
            source_label = "Same-day ATM put fallback"
            formula_label = "2 × ATM put premium using midpoint when bid/ask are both available, else mark/bid/ask/last"
            contracts_label = f"2 × {int(selected_put['strike'])}P {put_anchor:.2f}"
        elif call_anchor > 0:
            expected_move = call_anchor * 2.0
            source_label = "Same-day ATM call fallback"
            formula_label = "2 × ATM call premium using midpoint when bid/ask are both available, else mark/bid/ask/last"
            contracts_label = f"2 × {int(selected_call['strike'])}C {call_anchor:.2f}"

        return {
            "expected_move": round(expected_move, 2) if expected_move > 0 else 0.0,
            "source_label": source_label if expected_move > 0 else "Unavailable",
            "formula_label": formula_label if expected_move > 0 else "Same-day ATM straddle unavailable",
            "contracts_label": contracts_label,
        }

    def _calculate_live_credit(self, short_put: Dict[str, Any], long_put: Dict[str, Any]) -> float:
        conservative_credit = (short_put.get("bid") or 0.0) - (long_put.get("ask") or 0.0)
        mid_credit = self._premium_anchor(short_put) - self._premium_anchor(long_put)
        return max(0.0, conservative_credit, mid_credit)

    @staticmethod
    def _premium_anchor(contract: Dict[str, Any]) -> float:
        bid = float(contract.get("bid") or 0.0)
        ask = float(contract.get("ask") or 0.0)
        mark = float(contract.get("mark") or 0.0)
        if bid > 0 and ask > 0:
            return (bid + ask) / 2.0
        return max(mark, bid, ask, float(contract.get("last") or 0.0), 0.0)

    def _map_kairos_lifecycle_state(
        self,
        *,
        raw_state: str,
        simulation_runner: Dict[str, Any],
        live_best_trade: Dict[str, Any],
        live_trade_manager: Dict[str, Any],
    ) -> str:
        active_trade = live_trade_manager.get("active_trade") or simulation_runner.get("active_trade_lock_in")
        if active_trade is not None:
            if active_trade.get("is_settled") or active_trade.get("fully_closed") or active_trade.get("expired"):
                return "Completed"
            if active_trade.get("closed_contracts") or (live_trade_manager.get("exit_status") or {}).get("visible"):
                return "Trade Status"
            return "Trade Filled"
        candidate_ready = bool((live_best_trade.get("visible") and live_best_trade.get("candidate")) or (simulation_runner.get("trade_candidate_card") or {}).get("visible"))
        if candidate_ready:
            return "Trade Recommend"
        if self._runtime.mode == KairosMode.LIVE and live_best_trade.get("status") == "market-closed":
            return "Market Closed"
        if raw_state == KairosState.ACTIVATED.value:
            return "Initializing" if self._runtime.mode == KairosMode.LIVE else "Activation"
        if raw_state in {KairosState.SCANNING.value, KairosState.NOT_ELIGIBLE.value, KairosState.WATCHING.value}:
            return "Scanning"
        if raw_state == KairosState.SETUP_FORMING.value:
            return "Subprime Improving"
        if raw_state == KairosState.WINDOW_OPEN.value:
            return "Prime"
        if raw_state in {KairosState.WINDOW_CLOSING.value, KairosState.EXPIRED.value, KairosState.SESSION_COMPLETE.value, KairosState.STOPPED.value}:
            return "Completed"
        return raw_state

    def _augment_scan_payload_for_display(self, scan_payload: Dict[str, Any]) -> Dict[str, Any]:
        payload = dict(scan_payload)
        state_display = self._map_scan_state_label(payload.get("kairos_state"))
        prior_display = self._map_scan_state_label(payload.get("prior_kairos_state")) if payload.get("prior_kairos_state") else None
        payload["kairos_state_display"] = state_display
        payload["kairos_state_display_key"] = slugify_label(state_display)
        payload["prior_kairos_state_display"] = prior_display
        payload["prior_kairos_state_display_key"] = slugify_label(prior_display or "")
        payload["readiness_state_display"] = self._map_scan_state_label(payload.get("readiness_state"))
        transition = dict(payload.get("state_transition") or {})
        current_state_label = self._map_scan_state_label(transition.get("current_state")) if transition.get("current_state") else None
        prior_state_label = self._map_scan_state_label(transition.get("prior_state")) if transition.get("prior_state") else None
        transition["current_state_display"] = current_state_label
        transition["prior_state_display"] = prior_state_label
        if prior_state_label and current_state_label and prior_state_label != current_state_label:
            transition["label_display"] = f"{prior_state_label} -> {current_state_label}"
        elif current_state_label:
            transition["label_display"] = transition.get("label") if transition.get("label") in {"Initial classification", "No scans yet"} else f"State unchanged: {current_state_label}"
        payload["state_transition"] = transition
        payload["transition_label_display"] = transition.get("label_display") or payload.get("transition_label")
        return payload

    def _map_scan_state_label(self, raw_state: str | None) -> str:
        mapping = {
            KairosState.ACTIVATED.value: "Activation",
            KairosState.SCANNING.value: "Scanning",
            KairosState.NOT_ELIGIBLE.value: "Scanning",
            KairosState.WATCHING.value: "Scanning",
            KairosState.SETUP_FORMING.value: "Subprime Improving",
            KairosState.WINDOW_OPEN.value: "Prime",
            KairosState.WINDOW_CLOSING.value: "Subprime Weakening",
            KairosState.EXPIRED.value: "Completed",
            KairosState.SESSION_COMPLETE.value: "Completed",
            KairosState.STOPPED.value: "Completed",
        }
        return mapping.get(str(raw_state or ""), str(raw_state or "Inactive"))

    def _has_seen_scan_state(self, states: set[str]) -> bool:
        return any(item.kairos_state in states for item in self._session.scan_log)

    @staticmethod
    def _derive_trade_recommendation_stage(live_best_trade: Dict[str, Any], simulation_runner: Dict[str, Any]) -> str:
        if live_best_trade.get("visible") and live_best_trade.get("candidate"):
            return "Visible"
        if (simulation_runner.get("trade_candidate_card") or {}).get("visible"):
            return "Visible"
        return "Pending"

    @staticmethod
    def _derive_trade_recommendation_status_key(live_best_trade: Dict[str, Any], simulation_runner: Dict[str, Any]) -> str:
        return "positive" if KairosService._derive_trade_recommendation_stage(live_best_trade, simulation_runner) == "Visible" else "neutral"

    @staticmethod
    def _derive_trade_filled_stage(live_trade_manager: Dict[str, Any], simulation_runner: Dict[str, Any]) -> str:
        active_trade = live_trade_manager.get("active_trade") or simulation_runner.get("active_trade_lock_in")
        return "Filled" if active_trade is not None else "Pending"

    @staticmethod
    def _derive_trade_filled_status_key(live_trade_manager: Dict[str, Any], simulation_runner: Dict[str, Any]) -> str:
        return "positive" if KairosService._derive_trade_filled_stage(live_trade_manager, simulation_runner) == "Filled" else "neutral"

    @staticmethod
    def _derive_trade_status_stage(live_trade_manager: Dict[str, Any], simulation_runner: Dict[str, Any]) -> str:
        active_trade = live_trade_manager.get("active_trade") or simulation_runner.get("active_trade_lock_in")
        if active_trade is None:
            return "Pending"
        if active_trade.get("is_settled") or active_trade.get("fully_closed") or active_trade.get("expired"):
            return "Complete"
        return (live_trade_manager.get("exit_status") or simulation_runner.get("trade_exit_status") or {}).get("trade_status") or "Monitoring"

    @staticmethod
    def _derive_trade_status_status_key(live_trade_manager: Dict[str, Any], simulation_runner: Dict[str, Any]) -> str:
        return "positive" if KairosService._derive_trade_status_stage(live_trade_manager, simulation_runner) != "Pending" else "neutral"

    def _select_best_trade_candidate_profile(self, profiles: List[Dict[str, Any]]) -> Dict[str, Any] | None:
        available_profiles = [item for item in profiles if item.get("available")]
        return available_profiles[0] if available_profiles else None

    def _build_trade_override_caution_text(self, latest_scan: KairosScanResult | None) -> str:
        if latest_scan is None:
            return "No fresh Kairos scan is available, so treat this override as a rough advisory only."

        cautions: List[str] = []
        if latest_scan.kairos_state != KairosState.WINDOW_OPEN.value:
            cautions.append(f"Current Kairos state is {self._map_scan_state_label(latest_scan.kairos_state)}.")
        if latest_scan.timing_status != "Eligible":
            cautions.append(f"Timing is {latest_scan.timing_status}.")
        if latest_scan.structure_status not in {"Bullish Confirmation", "Developing"}:
            cautions.append(f"Structure is {latest_scan.structure_status}.")
        if latest_scan.momentum_status not in {"Improving", "Steady"}:
            cautions.append(f"Momentum is {latest_scan.momentum_status}.")
        if latest_scan.vix_status == "Not Eligible":
            cautions.append("VIX regime is outside the preferred Kairos band.")
        return " ".join(cautions) if cautions else "Current state is within the usual Kairos preference range."

    def _can_request_sim_best_trade_override_locked(self) -> bool:
        return (
            self._has_valid_runner_pause_state_locked()
            and bool(self._runner.scenario_key)
            and self._runner.pause_reason not in {None, "pause_on_window-open"}
            and self._runner.active_trade_lock_in is None
        )

    def _sim_best_trade_override_disabled_reason_locked(self) -> str:
        if self._runner.status == KairosRunnerStatus.RUNNING:
            return "Pause the replay before requesting a Best Trade Override."
        if self._runner.status != KairosRunnerStatus.PAUSED:
            return "Best Trade Override is only available while the replay is paused."
        if not self._runner.scenario_key:
            return "Load a replay scenario before requesting a Best Trade Override."
        if self._runner.pause_reason == "pause_on_window-open":
            return "Best Trade Override is disabled at Prime because the standard Kairos candidate flow is already active there."
        if self._runner.active_trade_lock_in is not None:
            return "A simulated trade is already active for this replay run."
        return "Best Trade Override is available."

    def _build_runner_trade_candidate_event_id_locked(self) -> str:
        return self._build_runner_trade_candidate_event_id_locked_for_bar(self._runner.pause_bar_number or max(0, self._runner.current_bar_index + 1))

    def _build_runner_trade_candidate_event_id_locked_for_bar(self, bar_number: int) -> str:
        scenario_key = self._runner.scenario_key or "custom"
        pause_reason = self._runner.pause_reason or "pause_on_window-open"
        return f"{scenario_key}:{pause_reason}:{bar_number}"

    def _sync_runner_trade_candidate_selection_locked(self, event_id: str, profiles: List[Dict[str, Any]]) -> str | None:
        if self._runner.candidate_event_id != event_id:
            self._runner.candidate_event_id = event_id
            self._runner.selected_candidate_profile_key = None
            self._runner.last_candidate_action_note = ""
        available_keys = [item["mode_key"] for item in profiles if item.get("available")]
        if self._runner.selected_candidate_profile_key in available_keys:
            return self._runner.selected_candidate_profile_key
        self._runner.selected_candidate_profile_key = available_keys[0] if available_keys else None
        return self._runner.selected_candidate_profile_key

    def _build_synthetic_kairos_candidate_profile(
        self,
        *,
        profile: Dict[str, Any],
        spot: float,
        vix: float,
        simulated_time: time,
        daily_move_anchor: float,
        time_factor: float,
        risk_cap: float,
    ) -> Dict[str, Any]:
        spread_width = self.KAIROS_CANDIDATE_SPREAD_WIDTH_POINTS
        hybrid_boundary = self._build_live_kairos_hybrid_boundary(
            spot=spot,
            expected_move=daily_move_anchor,
            em_multiple_floor=float(profile["distance_multiplier"]),
        )
        target_distance = float(hybrid_boundary["hybrid_threshold"])
        projected_short_strike = int((spot - target_distance) / 5.0) * 5
        blocked_handle = projected_short_strike % 100 in {0, 50}
        short_strike = projected_short_strike - 5 if blocked_handle else projected_short_strike
        long_strike = short_strike - spread_width
        distance_points = max(0.0, spot - short_strike)
        distance_percent = (distance_points / spot) * 100.0 if spot else 0.0
        distance_ratio = (distance_points / daily_move_anchor) if daily_move_anchor > 0 else 0.0
        estimated_short_delta = self._estimate_short_delta(distance_ratio)
        estimated_otm_probability = max(0.05, min(0.99, 1.0 - abs(estimated_short_delta)))
        vix_factor = min(1.30, max(0.85, vix / 20.0))
        credit_rate = (0.13 / (max(distance_ratio, 0.65) ** 1.35)) * vix_factor * time_factor * float(profile["credit_factor"])
        credit_dollars = round(max(0.0, min((spread_width * 100.0) * credit_rate, (spread_width * 100.0) * 0.45)), 2)
        per_contract_max_loss = round(max(0.0, (spread_width * 100.0) - credit_dollars), 2)
        contract_budget = int(risk_cap // per_contract_max_loss) if per_contract_max_loss > 0 else self.KAIROS_CANDIDATE_MAX_CONTRACTS
        recommended_contracts = min(self.KAIROS_CANDIDATE_MAX_CONTRACTS, contract_budget) if contract_budget > 0 else 0
        total_modeled_credit = round(credit_dollars * recommended_contracts, 2)
        max_loss_dollars = round(per_contract_max_loss * recommended_contracts, 2)

        rejection_reasons: List[str] = []
        if long_strike <= 0:
            rejection_reasons.append("Spread would price below zero.")
        if blocked_handle:
            rejection_reasons.append("Short strike landed on a blocked 00/50 handle and had to be adjusted.")
        if distance_points < target_distance:
            rejection_reasons.append(
                f"Distance is inside the hybrid boundary ({hybrid_boundary['boundary_rule_used']})."
            )
        if credit_dollars < self.KAIROS_CANDIDATE_MIN_CREDIT_DOLLARS:
            rejection_reasons.append("Current modeled candidate is below the minimum credit threshold.")
        if contract_budget < 1:
            rejection_reasons.append("Risk budget is too small for one contract.")

        available = not rejection_reasons
        qualification_status = "Qualified" if available else "Did not qualify"
        rationale = (
            f"Cleared credit and distance thresholds with {distance_ratio:.2f}x daily-move spacing and {distance_percent:.2f}% SPX buffer."
            if available
            else rejection_reasons[0]
        )
        return {
            "mode_key": profile["key"],
            "mode_label": profile["label"],
            "mode_descriptor": profile["descriptor"],
            "position_label": "Put Credit Spread",
            "spread_type": "Put Credit Spread",
            "available": available,
            "qualification_status": qualification_status,
            "status_key": "qualified" if available else "did-not-qualify",
            "short_strike": short_strike,
            "long_strike": long_strike,
            "strike_pair": f"{short_strike} / {long_strike}",
            "spread_width": spread_width,
            "spread_width_display": f"{spread_width} pts",
            "simulated_time": format_simulated_clock(simulated_time),
            "credit_estimate_dollars": credit_dollars,
            "credit_estimate_display": format_currency_value(credit_dollars),
            "premium_received_dollars": total_modeled_credit,
            "premium_received_display": format_currency_value(total_modeled_credit),
            "recommended_contracts": recommended_contracts,
            "recommended_contracts_display": str(recommended_contracts),
            "contract_size_display": str(recommended_contracts),
            "distance_points": round(distance_points, 2),
            "distance_points_display": f"{distance_points:.2f} pts",
            "distance_to_short_display": f"{distance_points:.2f} pts",
            "distance_percent": round(distance_percent, 2),
            "distance_percent_display": f"{distance_percent:.2f}%",
            "daily_move_multiple": round(distance_ratio, 2),
            "daily_move_multiple_display": f"{distance_ratio:.2f}x",
            "em_multiple_display": f"{distance_ratio:.2f}x EM",
            "expected_move_used": round(daily_move_anchor, 2),
            "expected_move_source": "vix_rule_of_16",
            "em_multiple_floor": hybrid_boundary["em_multiple_floor"],
            "percent_floor": hybrid_boundary["percent_floor"],
            "percent_floor_points": hybrid_boundary["percent_floor_points"],
            "em_floor_points": hybrid_boundary["em_floor_points"],
            "hybrid_threshold": hybrid_boundary["hybrid_threshold"],
            "boundary_binding_source": hybrid_boundary["boundary_binding_source"],
            "boundary_rule_used": hybrid_boundary["boundary_rule_used"],
            "actual_distance_to_short": round(distance_points, 2),
            "actual_em_multiple": round(distance_ratio, 2),
            "fallback_used": "no",
            "fallback_rule_name": "",
            "estimated_short_delta": round(estimated_short_delta, 2),
            "estimated_short_delta_display": f"{estimated_short_delta:.2f}",
            "estimated_otm_probability": round(estimated_otm_probability, 2),
            "estimated_otm_probability_display": format_percent_value(estimated_otm_probability),
            "max_loss_per_contract": per_contract_max_loss,
            "max_loss_per_contract_display": format_currency_value(per_contract_max_loss),
            "max_loss_dollars": max_loss_dollars,
            "max_loss_display": format_currency_value(max_loss_dollars),
            "risk_cap_display": format_currency_value(risk_cap, decimals=0),
            "time_factor": round(time_factor, 2),
            "vix_factor": round(vix_factor, 2),
            "selection_enabled": available,
            "rationale": rationale,
            "detail_rows": [
                {"label": "Spread type", "value": "Put Credit Spread"},
                {"label": "Short strike", "value": str(short_strike)},
                {"label": "Long strike", "value": str(long_strike)},
                {"label": "Width", "value": f"{spread_width} pts"},
                {"label": "Percent floor", "value": f"{hybrid_boundary['percent_floor']:.2f}% ({hybrid_boundary['percent_floor_points']:.2f} pts)"},
                {"label": "EM floor", "value": f"{hybrid_boundary['em_multiple_floor']:.2f}x ({hybrid_boundary['em_floor_points']:.2f} pts)"},
                {"label": "Boundary source", "value": str(hybrid_boundary["boundary_binding_source"])},
                {"label": "Hybrid boundary", "value": f"{target_distance:.2f} pts"},
                {"label": "Distance from SPX", "value": f"{distance_points:.2f} pts"},
                {"label": "Distance from SPX %", "value": f"{distance_percent:.2f}%"},
                {"label": "Estimated OTM probability", "value": format_percent_value(estimated_otm_probability)},
                {"label": "Estimated credit per contract", "value": format_currency_value(credit_dollars)},
                {"label": "Estimated total premium received", "value": format_currency_value(total_modeled_credit)},
                {"label": "Estimated max loss", "value": format_currency_value(max_loss_dollars)},
                {"label": "Suggested contracts", "value": str(recommended_contracts)},
            ],
            "no_trade_message": rejection_reasons[0] if rejection_reasons else "",
            "rejection_reasons": rejection_reasons,
        }

    @staticmethod
    def _estimate_short_delta(distance_ratio: float) -> float:
        normalized_ratio = max(distance_ratio, 0.65)
        return round(max(0.05, min(0.45, 0.28 / (normalized_ratio ** 1.1))), 2)

    def _has_pending_exit_gate_locked(self) -> bool:
        trade_lock_in = self._runner.active_trade_lock_in
        return bool(
            trade_lock_in is not None
            and trade_lock_in.pending_exit_gate_key is not None
            and self._runner.status == KairosRunnerStatus.PAUSED
            and self._runner.pause_reason == "pause_on_exit-gate"
        )

    def _clear_pending_exit_gate_locked(self, trade_lock_in: KairosSimulatedTradeLockIn) -> None:
        trade_lock_in.pending_exit_gate_key = None
        trade_lock_in.pending_exit_gate_contracts = 0
        trade_lock_in.pending_exit_gate_debit_per_contract = 0.0
        trade_lock_in.pending_exit_gate_total_debit = 0.0
        trade_lock_in.pending_exit_gate_note = ""
        trade_lock_in.pending_exit_recommendation = None

    def _estimate_simulated_exit_debit_per_contract_locked(
        self,
        trade_lock_in: KairosSimulatedTradeLockIn,
        *,
        current_spx: float,
        metrics: Dict[str, Any],
    ) -> float:
        return self._estimate_exit_debit_per_contract(
            trade_lock_in,
            current_spx=current_spx,
            time_remaining_ratio=self._build_time_remaining_ratio(current_bar_number=self._runner.current_bar_index + 1),
        )

    def _resolve_exit_gate_contracts_to_close(self, trade_lock_in: KairosSimulatedTradeLockIn, fraction: float) -> int:
        desired_contracts = max(1, int(ceil(trade_lock_in.contracts * fraction)))
        return max(0, min(trade_lock_in.remaining_contracts, desired_contracts))

    def _evaluate_runner_exit_gate_pause_locked(
        self,
        latest_scan: KairosScanResult | None,
        metrics: Dict[str, Any] | None,
    ) -> Dict[str, str] | None:
        if not self._runner.pause_on_exit_gates:
            return None

        trade_lock_in = self._runner.active_trade_lock_in
        if trade_lock_in is None or trade_lock_in.final_spx_close is not None or metrics is None or metrics.get("current_bar") is None:
            return None
        if trade_lock_in.pending_exit_gate_key is not None or trade_lock_in.remaining_contracts <= 0:
            return None

        current_spx = float(metrics.get("current_close") or 0.0)
        current_vwap = float(metrics.get("vwap") or current_spx)
        current_time_label = format_simulated_clock(metrics["current_bar"].simulated_time)

        if current_spx < current_vwap and trade_lock_in.vwap_breach_bar_number is None:
            trade_lock_in.vwap_breach_bar_number = self._runner.current_bar_index
        elif current_spx >= current_vwap:
            trade_lock_in.vwap_breach_bar_number = None

        recommendation = self.evaluate_kairos_exit_gates(
            trade_lock_in,
            current_spx=current_spx,
            current_vwap=current_vwap,
            current_time_label=current_time_label,
            structure_status=latest_scan.structure_status if latest_scan is not None else "Developing",
            debit_per_contract=self._estimate_simulated_exit_debit_per_contract_locked(trade_lock_in, current_spx=current_spx, metrics=metrics),
            current_bar_number=self._runner.current_bar_index + 1,
        )
        if recommendation is not None:
            return {
                "reason_key": "pause_on_exit-gate",
                "event_kind": recommendation["gate_key"],
                "event_label": recommendation["title"],
                "detail": recommendation["note"],
            }
        return None

    def _finalize_closed_early_trade_locked(self, trade_lock_in: KairosSimulatedTradeLockIn, *, current_spx: float, current_time_label: str) -> None:
        realized_pnl = round(trade_lock_in.current_credit, 2)
        trade_lock_in.final_spx_close = round(current_spx, 2)
        trade_lock_in.simulated_pnl_dollars = realized_pnl
        trade_lock_in.result_status = "Win" if realized_pnl >= 0 else "Loss"
        trade_lock_in.result_tone = "win" if realized_pnl >= 0 else "loss"
        trade_lock_in.settlement_badge_label = f"Closed Early ({format_signed_currency_whole(realized_pnl)})"
        trade_lock_in.settlement_note = f"Fully closed early at {current_time_label} after Kairos exit management reduced the position to zero contracts."
        trade_lock_in.exit_status_summary = "Closed Early"
        trade_lock_in.exit_metadata = {
            "closed_early": True,
            "time_label": current_time_label,
            "current_spx": round(current_spx, 2),
        }

    def _calculate_kairos_candidate_time_factor(self, simulated_time: time) -> float:
        session_start_minutes = (8 * 60) + 45
        session_end_minutes = (11 * 60) + 45
        current_minutes = (simulated_time.hour * 60) + simulated_time.minute
        normalized = (min(max(current_minutes, session_start_minutes), session_end_minutes) - session_start_minutes) / (session_end_minutes - session_start_minutes)
        return max(0.78, min(1.08, 1.08 - (normalized * 0.28)))

    def _build_bar_map_payload_locked(self) -> Dict[str, Any]:
        scenario = self._current_runner_scenario_locked() if self._runtime.mode == KairosMode.SIMULATION else None
        processed_count = 0
        bar_payloads: List[Dict[str, Any]] = []
        marker_payloads: List[Dict[str, Any]] = []
        latest_time_label = "—"

        if scenario is not None and self._runner.current_bar_index >= 0:
            processed_bars = list(scenario.bars[: self._runner.current_bar_index + 1])
            processed_count = len(processed_bars)
            cumulative_volume = 0
            vwap_numerator = 0.0
            for index, bar in enumerate(processed_bars):
                cumulative_volume += bar.volume
                typical_price = (bar.high + bar.low + bar.close) / 3.0
                vwap_numerator += typical_price * bar.volume
                current_vwap = vwap_numerator / cumulative_volume if cumulative_volume else bar.close
                bar_payloads.append(
                    {
                        "bar_index": bar.bar_index,
                        "timestamp": format_simulated_clock(bar.simulated_time),
                        "open": round(bar.open, 2),
                        "high": round(bar.high, 2),
                        "low": round(bar.low, 2),
                        "close": round(bar.close, 2),
                        "volume": bar.volume,
                        "vwap": round(current_vwap, 2),
                        "timestamp_iso": None,
                        "is_up": bar.close >= bar.open,
                        "is_active": index == processed_count - 1,
                    }
                )
            latest_time_label = format_simulated_clock(processed_bars[-1].simulated_time)
            marker_payloads = self._build_bar_map_markers_locked(processed_count, bar_payloads)
        elif self._runtime.mode == KairosMode.LIVE:
            bar_payloads = self._build_live_bar_map_bars_locked()
            processed_count = len(bar_payloads)
            latest_time_label = bar_payloads[-1]["timestamp"] if bar_payloads else "—"
            marker_payloads = self._build_bar_map_markers_locked(processed_count, bar_payloads)

        if self._runtime.mode == KairosMode.SIMULATION:
            source_label = self._current_runner_tape_source_locked(scenario)
            note = (
                f"{source_label} candles render from left to right as the scripted session advances."
                if processed_count
                else "Start the full-day simulator to populate the /ES session tape viewer."
            )
        else:
            note = (
                "Live SPX intraday candles are rendering with Kairos scan and trade annotations."
                if processed_count
                else "Live bar ingestion is temporarily unavailable because intraday SPX candles could not be resolved."
            )
            source_label = "Live SPX 1m"

        return {
            "title": "Kairos Session Tape",
            "mode": self._runtime.mode.value,
            "mode_key": slugify_label(self._runtime.mode.value),
            "mode_label": f"{self._runtime.mode.value} Tape",
            "tape_source": source_label,
            "supports_live_mode": True,
            "total_bars": SESSION_BAR_COUNT,
            "processed_bars": processed_count,
            "progress_percent": int((processed_count / SESSION_BAR_COUNT) * 100) if SESSION_BAR_COUNT else 0,
            "latest_timestamp": latest_time_label,
            "bars": bar_payloads,
            "event_markers": marker_payloads,
            "vwap_overlay_enabled": bool(bar_payloads),
            "event_marker_hook_enabled": True,
            "note": note,
        }

    def _load_live_spx_bar_frame_locked(self, *, session_date: date | None = None, query_type: str = "kairos_live_bar_map", force_refresh: bool = False) -> pd.DataFrame | None:
        try:
            same_day_reader = (
                self.market_data_service.get_fresh_same_day_intraday_candles
                if force_refresh and hasattr(self.market_data_service, "get_fresh_same_day_intraday_candles")
                else self.market_data_service.get_same_day_intraday_candles
            )
            dated_reader = (
                self.market_data_service.get_fresh_intraday_candles_for_date
                if force_refresh and hasattr(self.market_data_service, "get_fresh_intraday_candles_for_date")
                else self.market_data_service.get_intraday_candles_for_date
            )
            requested_session_date = session_date or self._now().date()
            session_request_resolver = getattr(self.market_data_service, "resolve_intraday_session_request", None)
            if callable(session_request_resolver):
                session_request = session_request_resolver(requested_session_date, current_time=self._now())
                resolved_session_date = session_request.get("resolved_session_date", requested_session_date)
                normalization_reason = str(session_request.get("normalization_reason") or "")
            else:
                resolved_session_date = requested_session_date
                normalization_reason = ""
            if resolved_session_date != requested_session_date:
                LOGGER.info(
                    "Kairos intraday session normalized | query_type=%s | requested_session_date=%s | resolved_session_date=%s | reason=%s",
                    query_type,
                    requested_session_date.isoformat(),
                    resolved_session_date.isoformat(),
                    normalization_reason or "latest valid tradable session",
                )
            if resolved_session_date == self._now().date() and requested_session_date == self._now().date():
                return same_day_reader("^GSPC", interval_minutes=1, query_type=query_type)
            return dated_reader(
                "^GSPC",
                target_date=resolved_session_date,
                interval_minutes=1,
                query_type=query_type,
            )
        except MarketDataError:
            return None

    def _build_live_bar_map_bars_from_frame(self, frame: pd.DataFrame | None) -> List[Dict[str, Any]]:
        if frame is None or frame.empty:
            return []

        normalized_rows = frame.to_dict(orient="records")
        bar_payloads: List[Dict[str, Any]] = []
        cumulative_volume = 0
        vwap_numerator = 0.0
        for index, row in enumerate(normalized_rows[:SESSION_BAR_COUNT]):
            timestamp_value = row.get("Datetime") or row.get("datetime") or row.get("timestamp")
            if timestamp_value is None:
                continue
            timestamp = pd.Timestamp(timestamp_value)
            if timestamp.tzinfo is None:
                timestamp = timestamp.tz_localize("UTC")
            local_timestamp = timestamp.tz_convert(self.display_timezone).to_pydatetime()
            open_value = self._coerce_float(row.get("Open") if "Open" in row else row.get("open"), fallback=0.0)
            high_value = self._coerce_float(row.get("High") if "High" in row else row.get("high"), fallback=open_value)
            low_value = self._coerce_float(row.get("Low") if "Low" in row else row.get("low"), fallback=open_value)
            close_value = self._coerce_float(row.get("Close") if "Close" in row else row.get("close"), fallback=open_value)
            volume_value = int(max(1, round(self._coerce_float(row.get("Volume") if "Volume" in row else row.get("volume"), fallback=1.0))))
            cumulative_volume += volume_value
            typical_price = (high_value + low_value + close_value) / 3.0
            vwap_numerator += typical_price * volume_value
            bar_payloads.append(
                {
                    "bar_index": index,
                    "timestamp": format_display_time(local_timestamp, self.display_timezone),
                    "timestamp_iso": local_timestamp.isoformat(),
                    "open": round(open_value, 2),
                    "high": round(high_value, 2),
                    "low": round(low_value, 2),
                    "close": round(close_value, 2),
                    "volume": volume_value,
                    "vwap": round(vwap_numerator / cumulative_volume, 2) if cumulative_volume else round(close_value, 2),
                    "is_up": close_value >= open_value,
                    "is_active": index == len(normalized_rows[:SESSION_BAR_COUNT]) - 1,
                }
            )
        return bar_payloads

    def _build_live_bar_map_bars_locked(self, *, session_date: date | None = None, query_type: str = "kairos_live_bar_map") -> List[Dict[str, Any]]:
        bar_payloads = self._build_live_bar_map_bars_from_frame(
            self._load_live_spx_bar_frame_locked(session_date=session_date, query_type=query_type)
        )
        cutoff = self._now()
        filtered_payloads: List[Dict[str, Any]] = []
        for item in bar_payloads:
            timestamp_iso = str(item.get("timestamp_iso") or "").strip()
            try:
                timestamp = datetime.fromisoformat(timestamp_iso)
            except ValueError:
                continue
            if timestamp > cutoff:
                break
            filtered_payloads.append(dict(item, is_active=False))
        if filtered_payloads:
            filtered_payloads[-1]["is_active"] = True
        return filtered_payloads

    def build_tape_trade_annotations(self, trade_lock_in: KairosSimulatedTradeLockIn | None, bar_payloads: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if trade_lock_in is None or not trade_lock_in.exit_events:
            return []

        annotations: List[Dict[str, Any]] = []
        timestamp_map = {
            str(item.get("timestamp_iso") or ""): int(item.get("bar_index") or 0)
            for item in bar_payloads
            if item.get("timestamp_iso")
        }
        for event in trade_lock_in.exit_events:
            bar_index = None
            if event.get("bar_number") is not None:
                bar_index = max(0, int(event.get("bar_number") or 1) - 1)
            elif event.get("timestamp"):
                event_timestamp = pd.Timestamp(event["timestamp"])
                if event_timestamp.tzinfo is None:
                    event_timestamp = event_timestamp.tz_localize(self.display_timezone)
                event_iso = event_timestamp.tz_convert(self.display_timezone).to_pydatetime().isoformat()
                if event_iso in timestamp_map:
                    bar_index = timestamp_map[event_iso]
                else:
                    eligible = [item for item in bar_payloads if item.get("timestamp_iso") and str(item["timestamp_iso"]) <= event_iso]
                    if eligible:
                        bar_index = int(eligible[-1]["bar_index"])
            if bar_index is None or bar_index < 0 or bar_index >= len(bar_payloads):
                continue
            annotations.append(
                {
                    "bar_index": bar_index,
                    "bar_number": bar_index + 1,
                    "timestamp": event.get("time_label") or bar_payloads[bar_index].get("timestamp") or "—",
                    "label": event.get("label") or "Trade Event",
                    "kind": event.get("kind") or "trade-open",
                    "detail": event.get("detail") or "",
                }
            )
        return annotations

    def _build_bar_map_markers_locked(self, processed_count: int, bar_payloads: List[Dict[str, Any]] | None = None) -> List[Dict[str, Any]]:
        if processed_count <= 0:
            return []

        bar_payloads = bar_payloads or []
        seen: set[tuple[int, str]] = set()
        stack_counts: Dict[int, int] = {}
        markers: List[Dict[str, Any]] = []
        for scan in sorted(self._session.scan_log, key=lambda item: item.scan_sequence_number):
            if self._runtime.mode == KairosMode.SIMULATION:
                bar_index = max(0, scan.scan_sequence_number - 1)
            else:
                bar_index = self._resolve_live_bar_index_for_timestamp(scan.timestamp, bar_payloads)
            if bar_index >= processed_count:
                continue
            if bar_index < 0:
                continue

            marker_type = self._resolve_bar_map_marker_type(scan)
            if marker_type is None:
                continue

            key = (bar_index, marker_type["kind"])
            if key in seen:
                continue
            seen.add(key)
            stack_level = stack_counts.get(bar_index, 0)
            stack_counts[bar_index] = stack_level + 1
            markers.append(
                {
                    "bar_index": bar_index,
                    "bar_number": bar_index + 1,
                    "timestamp": format_display_time(scan.timestamp, self.display_timezone),
                    "label": marker_type["label"],
                    "kind": marker_type["kind"],
                    "detail": scan.state_transition.label or scan.summary_text,
                    "stack_level": stack_level,
                }
            )

        trade_annotations = self.build_tape_trade_annotations(
            self._runner.active_trade_lock_in if self._runtime.mode == KairosMode.SIMULATION else self._live_active_trade,
            bar_payloads,
        )
        for marker in trade_annotations:
            key = (marker["bar_index"], marker["kind"])
            if key in seen:
                continue
            seen.add(key)
            stack_level = stack_counts.get(marker["bar_index"], 0)
            stack_counts[marker["bar_index"]] = stack_level + 1
            markers.append({**marker, "stack_level": stack_level})
        return markers

    def _resolve_live_bar_index_for_timestamp(self, scan_timestamp: datetime, bar_payloads: List[Dict[str, Any]]) -> int:
        if not bar_payloads:
            return -1
        scan_iso = scan_timestamp.astimezone(self.display_timezone).isoformat() if scan_timestamp.tzinfo else scan_timestamp.replace(tzinfo=self.display_timezone).isoformat()
        eligible = [item for item in bar_payloads if item.get("timestamp_iso") and str(item["timestamp_iso"]) <= scan_iso]
        if not eligible:
            return -1
        return int(eligible[-1].get("bar_index") or 0)

    def _resolve_bar_map_marker_type(self, scan: KairosScanResult) -> Dict[str, str] | None:
        state = scan.kairos_state
        if state == KairosState.SETUP_FORMING.value:
            return {"label": "Subprime Improving", "kind": "setup-forming"}
        if state == KairosState.WINDOW_OPEN.value:
            return {"label": "Prime", "kind": "window-open"}
        if state == KairosState.WINDOW_CLOSING.value:
            return {"label": "Subprime Weakening", "kind": "window-closing"}
        if state in {KairosState.NOT_ELIGIBLE.value, KairosState.EXPIRED.value}:
            return {"label": state, "kind": "non-go"}
        return None

    def _clear_live_trade_state_locked(self) -> None:
        self._live_best_trade_override_requested = False
        self._live_active_trade = None
        self._live_trade_history = []

    def _build_runtime_note_locked(self) -> str:
        if self._runtime.mode == KairosMode.SIMULATION:
            return "Simulation Mode is configured and ready for off-hours Kairos testing."
        return "Live Mode is configured. Market-closed protections remain active until the real session opens."

    def _derive_prior_state(self) -> Optional[str]:
        if self._session.latest_scan is not None:
            return self._session.latest_scan.kairos_state
        if self._session.current_state not in {KairosState.INACTIVE.value, KairosState.ACTIVATED.value, KairosState.SCANNING.value}:
            return self._session.current_state
        return None

    def _active_scan_interval_seconds(self) -> int:
        if self._runtime.mode == KairosMode.SIMULATION:
            return self._runtime.simulation_scan_interval_seconds
        return self.live_scan_interval_seconds

    def _apply_simulation_preset_locked(self, preset_name: str) -> None:
        resolved_name = self.SIMULATION_PRESET_ALIASES.get(preset_name, preset_name)
        preset = self.SIMULATION_PRESETS.get(resolved_name)
        if preset is None:
            return
        self._runtime.mode = KairosMode.SIMULATION
        self._runtime.simulated_market_session_status = preset.market_session_status
        self._runtime.simulated_spx_value = preset.spx_value
        self._runtime.simulated_vix_value = preset.vix_value
        self._runtime.simulated_timing_status = preset.timing_status
        self._runtime.simulated_structure_status = preset.structure_status
        self._runtime.simulated_momentum_status = preset.momentum_status
        self._runtime.simulation_scenario_name = preset.name

    def _coerce_mode(self, value: Any) -> KairosMode:
        text = str(value or "").strip().lower()
        if text == "simulation":
            return KairosMode.SIMULATION
        return KairosMode.LIVE

    def _coerce_bool(self, value: Any) -> bool:
        if isinstance(value, bool):
            return value
        return str(value or "").strip().lower() in {"1", "true", "yes", "on"}

    def _coerce_choice(self, value: Any, choices: tuple[str, ...]) -> str:
        text = str(value or "").strip()
        for choice in choices:
            if choice.lower() == text.lower():
                return choice
        return choices[0]

    def _coerce_float(self, value: Any, *, fallback: float) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return fallback

    def _coerce_interval(self, value: Any) -> int:
        try:
            interval = int(value)
        except (TypeError, ValueError):
            return self._runtime.simulation_scan_interval_seconds
        return interval if interval in self.SIMULATION_INTERVAL_OPTIONS else self._runtime.simulation_scan_interval_seconds

    def _now(self) -> datetime:
        current = self.now_provider()
        return current.astimezone(self.display_timezone) if current.tzinfo else current.replace(tzinfo=self.display_timezone)


def format_display_datetime(value: datetime, timezone: ZoneInfo) -> str:
    local_value = value.astimezone(timezone) if value.tzinfo else value.replace(tzinfo=timezone)
    return local_value.strftime("%b %d, %Y %I:%M:%S %p %Z")


def format_display_time(value: datetime, timezone: ZoneInfo) -> str:
    local_value = value.astimezone(timezone) if value.tzinfo else value.replace(tzinfo=timezone)
    return local_value.strftime("%I:%M:%S %p %Z").lstrip("0")


def serialize_optional_datetime(value: datetime | None, timezone: ZoneInfo) -> str | None:
    if value is None:
        return None
    local_value = value.astimezone(timezone) if value.tzinfo else value.replace(tzinfo=timezone)
    return local_value.isoformat()


def format_optional_datetime(value: datetime | None, timezone: ZoneInfo) -> str:
    if value is None:
        return "—"
    return format_display_datetime(value, timezone)


def format_simulated_clock(value: time) -> str:
    return value.strftime("%I:%M %p").lstrip("0")


def percent_change(current_value: float, reference_value: float) -> float:
    if reference_value == 0:
        return 0.0
    return ((current_value - reference_value) / reference_value) * 100.0


def default_scan_payload(now: datetime, timezone: ZoneInfo, market_session_status: str, mode: str) -> Dict[str, Any]:
    is_live_mode = mode == KairosMode.LIVE.value
    if is_live_mode and market_session_status != "Open":
        return {
            "timestamp": now.isoformat(),
            "timestamp_display": format_display_datetime(now, timezone),
            "timestamp_short": format_display_time(now, timezone),
            "scan_sequence_number": 0,
            "mode": mode,
            "mode_key": slugify_label(mode),
            "is_simulated": False,
            "kairos_state": KairosState.INACTIVE.value,
            "kairos_state_key": slugify_label(KairosState.INACTIVE.value),
            "prior_kairos_state": None,
            "prior_kairos_state_key": "",
            "vix_status": "Market Closed",
            "timing_status": "Market Closed",
            "structure_status": "Standing By",
            "momentum_status": "Standing By",
            "readiness_state": KairosState.NOT_ELIGIBLE.value,
            "summary_text": "Market closed - Live Kairos standing by.",
            "reasons": ["Live Kairos initializes automatically when the workspace opens and will resume scanning during the regular market session."],
            "state_transition": {
                "prior_state": None,
                "prior_state_key": "",
                "current_state": KairosState.INACTIVE.value,
                "current_state_key": slugify_label(KairosState.INACTIVE.value),
                "is_transition": False,
                "label": "No scans yet",
            },
            "transition_label": "No scans yet",
            "is_window_open": False,
            "is_window_closing": False,
            "is_expired": False,
            "trigger_reason": "none",
            "market_session_status": market_session_status,
            "spx_value": "—",
            "vix_value": "—",
            "simulation_scenario_name": None,
            "classification_note": "Live Kairos initializes automatically on page load.",
            "seconds_ago": 0,
        }
    if is_live_mode:
        return {
            "timestamp": now.isoformat(),
            "timestamp_display": format_display_datetime(now, timezone),
            "timestamp_short": format_display_time(now, timezone),
            "scan_sequence_number": 0,
            "mode": mode,
            "mode_key": slugify_label(mode),
            "is_simulated": False,
            "kairos_state": KairosState.ACTIVATED.value,
            "kairos_state_key": slugify_label(KairosState.ACTIVATED.value),
            "prior_kairos_state": None,
            "prior_kairos_state_key": "",
            "vix_status": "Initializing",
            "timing_status": "Initializing",
            "structure_status": "Initializing",
            "momentum_status": "Initializing",
            "readiness_state": KairosState.NOT_ELIGIBLE.value,
            "summary_text": "Live Kairos is initializing the current session scan.",
            "reasons": ["Live Kairos initializes automatically when the workspace opens and begins scanning as soon as the live session is ready."],
            "state_transition": {
                "prior_state": None,
                "prior_state_key": "",
                "current_state": KairosState.ACTIVATED.value,
                "current_state_key": slugify_label(KairosState.ACTIVATED.value),
                "is_transition": False,
                "label": "No scans yet",
            },
            "transition_label": "No scans yet",
            "is_window_open": False,
            "is_window_closing": False,
            "is_expired": False,
            "trigger_reason": "none",
            "market_session_status": market_session_status,
            "spx_value": "—",
            "vix_value": "—",
            "simulation_scenario_name": None,
            "classification_note": "Live Kairos initializes automatically on page load.",
            "seconds_ago": 0,
        }
    return {
        "timestamp": now.isoformat(),
        "timestamp_display": format_display_datetime(now, timezone),
        "timestamp_short": format_display_time(now, timezone),
        "scan_sequence_number": 0,
        "mode": mode,
        "mode_key": slugify_label(mode),
        "is_simulated": mode == KairosMode.SIMULATION.value,
        "kairos_state": KairosState.INACTIVE.value,
        "kairos_state_key": slugify_label(KairosState.INACTIVE.value),
        "prior_kairos_state": None,
        "prior_kairos_state_key": "",
        "vix_status": "Awaiting scan",
        "timing_status": "Awaiting activation",
        "structure_status": "Awaiting scan",
        "momentum_status": "Awaiting scan",
        "readiness_state": KairosState.NOT_ELIGIBLE.value,
        "summary_text": "Activate Kairos for today to begin the classification cycle.",
        "reasons": ["Kairos has not run a scan for the current session yet."],
        "state_transition": {
            "prior_state": None,
            "prior_state_key": "",
            "current_state": KairosState.INACTIVE.value,
            "current_state_key": slugify_label(KairosState.INACTIVE.value),
            "is_transition": False,
            "label": "No scans yet",
        },
        "transition_label": "No scans yet",
        "is_window_open": False,
        "is_window_closing": False,
        "is_expired": False,
        "trigger_reason": "none",
        "market_session_status": market_session_status,
        "spx_value": "—",
        "vix_value": "—",
        "simulation_scenario_name": None,
        "classification_note": "Kairos is waiting for activation.",
        "seconds_ago": 0,
    }


def format_snapshot_value(snapshot: Dict[str, Any] | None) -> str:
    if not snapshot:
        return "—"
    value = snapshot.get("Latest Value")
    if value is None:
        return "—"
    try:
        return f"{float(value):,.2f}"
    except (TypeError, ValueError):
        return str(value)


def format_number_value(value: Any, *, decimals: int = 2) -> str:
    try:
        return f"{float(value):,.{decimals}f}"
    except (TypeError, ValueError):
        return "—"


def format_currency_value(value: Any, *, decimals: int = 2) -> str:
    try:
        return f"${float(value):,.{decimals}f}"
    except (TypeError, ValueError):
        return "—"


def format_percent_value(value: Any, *, decimals: int = 0) -> str:
    try:
        return f"{float(value) * 100:,.{decimals}f}%"
    except (TypeError, ValueError):
        return "—"


def format_signed_currency_whole(value: Any) -> str:
    try:
        numeric_value = round(float(value))
    except (TypeError, ValueError):
        return "$0"
    sign = "+" if numeric_value >= 0 else "-"
    return f"{sign}${abs(numeric_value):,.0f}"


def coerce_snapshot_number(snapshot: Dict[str, Any] | None, key: str) -> float | None:
    if not snapshot:
        return None
    value = snapshot.get(key)
    if value in (None, "", "—"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def format_countdown(total_seconds: int | None) -> str:
    if total_seconds is None:
        return "—"
    minutes, seconds = divmod(max(total_seconds, 0), 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours:d}:{minutes:02d}:{seconds:02d}"
    return f"{minutes:02d}:{seconds:02d}"


def slugify_label(value: str) -> str:
    return str(value or "").strip().lower().replace(" ", "-")


def yes_no(value: bool) -> str:
    return "Yes" if value else "No"


def bool_to_status_key(value: bool) -> str:
    return "positive" if value else "neutral"
