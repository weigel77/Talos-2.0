"""Apollo staged pre-check workflow."""

from __future__ import annotations

import copy
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, time, timedelta
from threading import Lock
from typing import Any, Dict, List
from zoneinfo import ZoneInfo

from config import AppConfig, get_app_config

from .apollo_candidate_service import ApolloCandidateService
from .apollo_structure_service import ApolloStructureService
from .market_calendar_service import MarketCalendarService
from .macro_service import MacroService
from .market_data import MarketDataError, MarketDataService
from .options_chain_service import OptionsChainService


class ApolloService:
    """Run the staged Apollo pre-check workflow."""

    def __init__(
        self,
        market_data_service: MarketDataService,
        macro_service: MacroService | None = None,
        market_calendar_service: MarketCalendarService | None = None,
        options_chain_service: OptionsChainService | None = None,
        config: AppConfig | None = None,
    ) -> None:
        self.config = config or get_app_config()
        self.market_data_service = market_data_service
        self.macro_service = macro_service or MacroService(self.config)
        self.structure_service = ApolloStructureService(market_data_service=market_data_service, config=self.config)
        self.market_calendar_service = market_calendar_service or MarketCalendarService(self.config)
        self.options_chain_service = options_chain_service or OptionsChainService(self.config, provider=self._resolve_option_chain_provider())
        self.candidate_service = ApolloCandidateService(self.config)
        self.display_timezone = ZoneInfo(self.config.app_timezone)
        self._management_context_cache: Dict[str, Any] | None = None
        self._management_context_cache_expires_at: datetime | None = None
        self._management_context_cache_lock = Lock()

    def run_precheck(self, *, force_refresh: bool = False, caller_source: str = "apollo-precheck") -> Dict[str, Any]:
        """Execute the first-stage Apollo workflow."""
        checked_at = datetime.now(self.display_timezone)
        reasons: List[str] = []
        calendar_context = self.market_calendar_service.get_next_market_day_context(checked_at)

        if not self.config.apollo_enabled:
            return {
                "title": "Apollo Gate 1 -- SPX Structure",
                "provider_name": self.market_data_service.get_provider_metadata().get("live_provider_name", "Unknown Provider"),
                "spx": None,
                "vix": None,
                "macro": self.macro_service.get_macro_status(calendar_context["next_market_day"]),
                "market_calendar": calendar_context,
                "local_datetime": checked_at,
                "apollo_status": "blocked",
                "reasons": ["Apollo workflow is disabled by configuration."],
            }

        spx_data = None
        vix_data = None
        latest_snapshot_reader = (
            self.market_data_service.get_fresh_latest_snapshot
            if force_refresh and hasattr(self.market_data_service, "get_fresh_latest_snapshot")
            else self.market_data_service.get_latest_snapshot
        )
        with ThreadPoolExecutor(max_workers=2) as executor:
            spx_future = executor.submit(latest_snapshot_reader, "^GSPC", query_type="apollo_latest_spx")
            vix_future = executor.submit(latest_snapshot_reader, "^VIX", query_type="apollo_latest_vix")
            try:
                spx_data = spx_future.result()
                reasons.append("Live SPX data retrieved successfully.")
            except MarketDataError as exc:
                reasons.append(f"Unable to retrieve live SPX data: {exc}")

            try:
                vix_data = vix_future.result()
                reasons.append("Live VIX data retrieved successfully.")
            except MarketDataError as exc:
                reasons.append(f"Unable to retrieve live VIX data: {exc}")

        timing_status = self._evaluate_timing_window(checked_at)
        reasons.extend([reason for reason in timing_status["reasons"] if reason not in reasons])

        provider_name = self.market_data_service.get_result_metadata(spx_data or vix_data).get(
            "provider_name",
            self.market_data_service.get_provider_metadata().get("live_provider_name", "Unknown Provider"),
        )
        with ThreadPoolExecutor(max_workers=3) as executor:
            macro_future = executor.submit(self.macro_service.get_macro_status, calendar_context["next_market_day"])
            structure_future = executor.submit(
                self.structure_service.analyze_same_day_spx_structure,
                caller_source=caller_source,
            )
            option_chain_future = executor.submit(
                self.options_chain_service.get_spx_option_chain_summary,
                calendar_context["next_market_day"],
                caller_context=caller_source,
            )
            macro_status = macro_future.result()
            structure = structure_future.result()
            option_chain = option_chain_future.result()

        if not macro_status.get("available", True):
            reasons.append("Macro calendar check unavailable.")
        elif macro_status.get("fallback_used"):
            reasons.append(f"Macro source fallback used: {macro_status.get('source_name', 'Fallback source')}.")
        if macro_status.get("has_major_macro"):
            reasons.append("Major macro event detected.")
        elif macro_status.get("grade") == "Minor":
            reasons.append("Minor macro event(s) detected.")
        else:
            reasons.append("No significant macro events detected for the target market day.")

        for item in macro_status.get("macro_events") or []:
            if isinstance(item, dict):
                summary = f"{item.get('impact', 'None')}: {item.get('title', 'Event')} at {item.get('time', 'Time unavailable')}."
            else:
                summary = str(item)
            if summary not in reasons:
                reasons.append(summary)

        apollo_status = self._determine_status(
            spx_data=spx_data,
            vix_data=vix_data,
            macro_status=macro_status,
            timing_level=timing_status["level"],
        )
        trade_candidates = self.candidate_service.build_trade_candidates(
            option_chain=option_chain,
            structure=structure,
            macro=macro_status,
        )
        if structure.get("available"):
            reasons.append(
                f"Apollo final structure grade: {structure.get('grade', 'Neutral')} using {structure.get('source_used', 'SPX')}."
            )
            if structure.get("base_grade") and structure.get("base_grade") != structure.get("grade"):
                reasons.append(
                    f"Daily RSI oversold boost applied: {structure.get('base_grade')} -> {structure.get('grade')}"
                )
            elif structure.get("rsi_note"):
                reasons.append(str(structure.get("rsi_note")))
            if structure.get("source_used") != structure.get("preferred_source") and structure.get("fallback_reason"):
                reasons.append(f"Structure fallback reason: {structure.get('fallback_reason')}")
        else:
            reasons.append(str(structure.get("message") or "Apollo structure grading was unavailable."))
        if option_chain.get("success"):
            reasons.append(f"SPX option chain retrieved for {calendar_context['next_market_day'].isoformat()}.")
        else:
            reasons.append(f"SPX option chain unavailable: {option_chain.get('message', 'Unknown error')}")
        reasons.append(f"Holiday filter applied: {calendar_context.get('holiday_filter_applied_label', 'No')}.")
        if calendar_context.get("skipped_holiday_name"):
            reasons.append(f"Skipped holiday: {calendar_context['skipped_holiday_name']}.")
        if trade_candidates.get("candidate_count", 0):
            reasons.append(f"Apollo generated {trade_candidates.get('candidate_count', 0)} Gate 3 trade candidate(s).")
        else:
            reasons.append("Apollo did not find a qualifying Gate 3 trade candidate.")

        return {
            "title": "Apollo Gate 1 -- SPX Structure",
            "provider_name": provider_name,
            "spx": self._build_market_item("SPX", spx_data),
            "vix": self._build_market_item("VIX", vix_data),
            "header_market_snapshots": {
                "^GSPC": spx_data or {},
                "^VIX": vix_data or {},
            },
            "macro": macro_status,
            "structure": structure,
            "market_calendar": calendar_context,
            "option_chain": option_chain,
            "trade_candidates": trade_candidates,
            "local_datetime": checked_at,
            "apollo_status": apollo_status,
            "reasons": reasons,
        }

    def build_management_context(self, *, caller_source: str = "apollo-management-context") -> Dict[str, Any]:
        if self.config.runtime_target == "local":
            now = datetime.now(self.display_timezone)
            with self._management_context_cache_lock:
                if self._management_context_cache is not None and self._management_context_cache_expires_at is not None and now <= self._management_context_cache_expires_at:
                    return copy.deepcopy(self._management_context_cache)

        try:
            structure = self.structure_service.analyze_same_day_spx_structure()
        except Exception:
            structure = {}
        context = {
            "current_structure_grade": str(structure.get("final_grade") or structure.get("grade") or "Not available"),
            "current_macro_grade": "Not available",
            "precheck": {},
        }
        if self.config.runtime_target == "local":
            with self._management_context_cache_lock:
                self._management_context_cache = copy.deepcopy(context)
                self._management_context_cache_expires_at = datetime.now(self.display_timezone) + timedelta(seconds=5)
        return context

    def _resolve_option_chain_provider(self):
        provider = getattr(self.market_data_service, "live_provider", None)
        if getattr(provider, "provider_key", "") == "schwab":
            return provider
        return None

    def _evaluate_timing_window(self, current_time: datetime) -> Dict[str, Any]:
        """Return a simple timing-window assessment for Apollo."""
        if current_time.weekday() >= 5:
            return {"level": "blocked", "reasons": ["Current local day is outside the standard Apollo weekday window."]}

        market_open = time(8, 30)
        market_close = time(15, 0)
        if current_time.time() < market_open or current_time.time() > market_close:
            return {"level": "caution", "reasons": ["Current local time is outside the preferred Apollo timing window."]}

        return {"level": "allowed", "reasons": ["Current local time is inside the preferred Apollo timing window."]}

    @staticmethod
    def _determine_status(
        spx_data: Dict[str, Any] | None,
        vix_data: Dict[str, Any] | None,
        macro_status: Dict[str, Any],
        timing_level: str,
    ) -> str:
        """Determine the overall Apollo pre-check status."""
        if spx_data is None or vix_data is None:
            return "blocked"
        if timing_level == "blocked":
            return "blocked"
        if macro_status.get("has_major_macro") or timing_level == "caution" or not macro_status.get("available", True):
            return "caution"
        return "allowed"

    def _build_market_item(self, label: str, payload: Dict[str, Any] | None) -> Dict[str, Any] | None:
        """Format a market snapshot for Apollo display."""
        if payload is None:
            return None

        metadata = self.market_data_service.get_result_metadata(payload)
        return {
            "label": label,
            "value": payload.get("Latest Value"),
            "as_of": payload.get("As Of"),
            "provider_name": metadata.get("provider_name"),
        }
