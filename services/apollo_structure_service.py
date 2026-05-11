"""Apollo stage-four structure analysis for SPX 5-minute candles with session fallback."""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Any, Dict, List
from zoneinfo import ZoneInfo

import pandas as pd

from config import AppConfig, get_app_config

from .market_calendar_service import MarketCalendarService
from .market_data import MarketDataError, MarketDataService
from .technical_indicators import calculate_wilder_rsi

LOGGER = logging.getLogger(__name__)


class ApolloStructureService:
    """Retrieve same-day SPX structure inputs and grade them with trading-style classification."""

    STRUCTURE_SOURCES = {
        "spx": {"symbol": "^GSPC", "label": "SPX"},
        "es": {"symbol": "/ES", "label": "/ES"},
    }

    NOT_AVAILABLE_MESSAGE = (
        "A valid 5-minute SPX session could not be retrieved from the current provider, "
        "so Apollo structure could not be graded."
    )
    MAX_SESSION_LOOKBACK_DAYS = 7
    MIN_STRUCTURE_CANDLES = 12
    DAILY_RSI_PERIOD = 14
    DAILY_RSI_LOOKBACK_DAYS = 370
    RSI_OVERSOLD_THRESHOLD = 30.0
    RSI_GRADE_UPGRADE_MAP = {
        "Poor": "Neutral",
        "Neutral": "Good",
        "Good": "Good",
    }

    def __init__(self, market_data_service: MarketDataService, config: AppConfig | None = None) -> None:
        self.config = config or get_app_config()
        self.market_data_service = market_data_service
        self.display_timezone = ZoneInfo(self.config.app_timezone)
        self.market_calendar_service = MarketCalendarService(self.config)

    def analyze_same_day_spx_structure(self, *, caller_source: str = "apollo-structure") -> Dict[str, Any]:
        """Return current or fallback-session SPX structure metrics, chart data, and classification output."""
        provider_name = self.market_data_service.get_provider_metadata().get("live_provider_name", "Unknown Provider")
        preferred_source = (self.config.apollo_structure_source or "spx").strip().lower()
        attempted_sources: List[Dict[str, str]] = []
        fallback_reason: str | None = None
        local_now = datetime.now(self.display_timezone)
        requested_session_date = local_now.date()
        session_request_resolver = getattr(self.market_data_service, "resolve_intraday_session_request", None)
        if callable(session_request_resolver):
            session_request = session_request_resolver(requested_session_date, current_time=local_now)
            normalized_session_date = session_request.get("resolved_session_date", requested_session_date)
            normalization_reason = str(session_request.get("normalization_reason") or "")
        else:
            normalized_session_date = requested_session_date
            normalization_reason = ""
        LOGGER.warning(
            "Apollo structure refresh | caller=%s | requested_session_date=%s | resolved_session_date=%s",
            caller_source,
            requested_session_date.isoformat(),
            normalized_session_date.isoformat(),
        )
        requested_status = self.market_calendar_service._get_market_day_status(requested_session_date)
        if normalized_session_date != requested_session_date:
            LOGGER.info(
                "Apollo structure session normalized | caller=%s | requested_session_date=%s | resolved_session_date=%s | reason=%s",
                caller_source,
                requested_session_date.isoformat(),
                normalized_session_date.isoformat(),
                normalization_reason or "latest valid tradable session",
            )

        last_error: str | None = None
        for source_key in self._get_source_sequence():
            source_config = self.STRUCTURE_SOURCES[source_key]
            session_result = self._get_latest_valid_intraday_session(
                source_key=source_key,
                source_label=source_config["label"],
                symbol=source_config["symbol"],
                requested_session_date=normalized_session_date,
                attempted_sources=attempted_sources,
                caller_source=caller_source,
            )
            if session_result is None:
                continue

            frame = session_result["frame"]
            last_error = None
            if source_key != preferred_source and attempted_sources:
                fallback_reason = attempted_sources[-2]["reason"] if len(attempted_sources) >= 2 else None

            frame = self._calculate_indicators(frame)
            metrics = self._build_metrics(frame)
            classification = self._classify_structure(frame, metrics)
            rsi_context = self._build_daily_rsi_context(session_result["session_date"])
            final_grade = self.apply_rsi_modifier(classification["grade"], rsi_context.get("value"))
            final_summary = self._build_final_summary(
                base_summary=classification["summary"],
                base_grade=classification["grade"],
                final_grade=final_grade,
                rsi_context=rsi_context,
            )
            session_note = self._build_session_note(
                requested_session_date=requested_session_date,
                session_date=session_result["session_date"],
                requested_status=requested_status,
            )

            return {
                "available": True,
                "provider_name": provider_name,
                "preferred_source": self.STRUCTURE_SOURCES.get(preferred_source, self.STRUCTURE_SOURCES["spx"])["label"],
                "attempted_sources": attempted_sources,
                "fallback_reason": fallback_reason,
                "source_used": source_config["label"],
                "grade": final_grade,
                "base_grade": classification["grade"],
                "final_grade": final_grade,
                "summary": final_summary,
                "base_summary": classification["summary"],
                "message": None,
                "session_date_used": session_result["session_date"],
                "session_note": session_note,
                "metrics": metrics,
                "trend_classification": classification["trend_classification"],
                "damage_classification": classification["damage_classification"],
                "rules": classification["rules"],
                "chart": self._build_chart_payload(frame),
                "rsi_modifier_label": rsi_context["modifier_label"],
                "rsi_modifier_applied": rsi_context["modifier_applied"],
                "rsi_value": rsi_context["value"],
                "rsi_period": rsi_context["period"],
                "rsi_history_points": rsi_context["history_points"],
                "rsi_note": rsi_context["note"],
                "rsi_detail": rsi_context["detail"],
            }

        return self._build_unavailable_result(
            provider_name=provider_name,
            detail=last_error,
            attempted_sources=attempted_sources,
        )

    def _get_latest_valid_intraday_session(
        self,
        source_key: str,
        source_label: str,
        symbol: str,
        requested_session_date: date,
        attempted_sources: List[Dict[str, str]],
        caller_source: str,
    ) -> Dict[str, Any] | None:
        last_error: str | None = None
        for session_date in self._session_candidates(requested_session_date):
            LOGGER.info(
                "Apollo structure attempt | caller=%s | source_key=%s | symbol=%s | session_date=%s | query_type=%s",
                caller_source,
                source_key,
                symbol,
                session_date.isoformat(),
                f"apollo_structure_{source_key}_5m",
            )
            try:
                candles = self.market_data_service.get_intraday_candles_for_date(
                    symbol,
                    target_date=session_date,
                    interval_minutes=5,
                    query_type=f"apollo_structure_{source_key}_5m",
                )
            except MarketDataError as exc:
                last_error = str(exc)
                attempted_sources.append(
                    {
                        "source": source_label,
                        "status": "failed",
                        "reason": f"{session_date.isoformat()}: {exc}",
                    }
                )
                continue

            frame = self._normalize_candle_frame(candles)
            if frame.empty:
                last_error = f"No usable 5-minute candles were returned for {source_label} on {session_date.isoformat()}."
                attempted_sources.append({"source": source_label, "status": "failed", "reason": last_error})
                continue
            if len(frame) < self.MIN_STRUCTURE_CANDLES:
                last_error = (
                    f"Only {len(frame)} usable 5-minute candles were returned for {source_label} on {session_date.isoformat()}, "
                    "which is insufficient for structure grading."
                )
                attempted_sources.append({"source": source_label, "status": "failed", "reason": last_error})
                continue

            retrieval_reason = (
                "Retrieved successfully."
                if session_date == requested_session_date
                else f"Retrieved successfully from prior trading session {session_date.isoformat()}."
            )
            attempted_sources.append({"source": source_label, "status": "used", "reason": retrieval_reason})
            return {"frame": frame, "session_date": session_date}

        if last_error is not None:
            LOGGER.warning("Apollo structure lookup exhausted fallback dates | source=%s | reason=%s", source_label, last_error)
        return None

    def _session_candidates(self, requested_session_date: date) -> List[date]:
        return [requested_session_date - timedelta(days=offset) for offset in range(self.MAX_SESSION_LOOKBACK_DAYS + 1)]

    def _normalize_candle_frame(self, candles: pd.DataFrame) -> pd.DataFrame:
        frame = candles.copy()
        frame["Datetime"] = pd.to_datetime(frame["Datetime"], errors="coerce")
        return frame.dropna(subset=["Datetime", "Open", "High", "Low", "Close"]).reset_index(drop=True)

    def _build_session_note(
        self,
        requested_session_date: date,
        session_date: date,
        requested_status: Dict[str, Any],
    ) -> str | None:
        if session_date == requested_session_date:
            return None
        if requested_status.get("holiday_name"):
            reason = "market closed today"
        elif requested_status.get("is_weekend"):
            reason = "weekend session fallback"
        else:
            reason = "no valid intraday session returned today"
        return f"Using prior trading session: {session_date.strftime('%a %b %d, %Y')} ({reason})."

    def _get_source_sequence(self) -> List[str]:
        primary = (self.config.apollo_structure_source or "spx").strip().lower()
        fallback = (self.config.apollo_structure_fallback_source or "").strip().lower()
        sequence: List[str] = []
        for candidate in (primary, fallback):
            if candidate in self.STRUCTURE_SOURCES and candidate not in sequence:
                sequence.append(candidate)
        if not sequence:
            sequence.append("spx")
        return sequence

    def _calculate_indicators(self, frame: pd.DataFrame) -> pd.DataFrame:
        working = frame.copy()
        working["EMA8"] = working["Close"].ewm(span=8, adjust=False).mean()
        working["EMA21"] = working["Close"].ewm(span=21, adjust=False).mean()

        return working

    def _build_metrics(self, frame: pd.DataFrame) -> Dict[str, Any]:
        latest_row = frame.iloc[-1]
        session_high = float(frame["High"].max())
        session_low = float(frame["Low"].min())
        current_price = float(latest_row["Close"])
        session_range = session_high - session_low
        range_position = ((current_price - session_low) / session_range) if session_range > 0 else 0.5
        recent_completed = frame.iloc[:-1].tail(6).copy()

        return {
            "session_high": round(session_high, 2),
            "session_low": round(session_low, 2),
            "current_price": round(current_price, 2),
            "range_position": round(range_position, 4),
            "ema8": self._round_optional(latest_row.get("EMA8")),
            "ema21": self._round_optional(latest_row.get("EMA21")),
            "recent_completed_count": int(len(recent_completed)),
            "recent_price_action": self._summarize_recent_price_action(recent_completed),
            "session_start": self._format_time(frame.iloc[0]["Datetime"]),
            "session_end": self._format_time(latest_row["Datetime"]),
        }

    def _classify_structure(self, frame: pd.DataFrame, metrics: Dict[str, Any]) -> Dict[str, Any]:
        latest_row = frame.iloc[-1]
        latest_ema8 = self._coerce_float(latest_row.get("EMA8"))
        latest_ema21 = self._coerce_float(latest_row.get("EMA21"))
        range_position = float(metrics["range_position"])

        recent_window = self._get_recent_window(frame, size=8)
        recent_completed = frame.iloc[:-1].tail(6).copy()

        trend_inputs = self._analyze_trend_inputs(recent_window)
        damage_inputs = self._analyze_damage_inputs(frame, recent_completed, range_position)

        trend_classification = self._classify_trend(latest_ema8, latest_ema21, trend_inputs)
        damage_classification = self._classify_damage(damage_inputs)
        grade = self._determine_grade(trend_classification, damage_classification, damage_inputs)
        summary = self._build_summary(grade, trend_classification, damage_classification, damage_inputs)
        rules = self._build_rules(
            latest_ema8=latest_ema8,
            latest_ema21=latest_ema21,
            trend_inputs=trend_inputs,
            damage_inputs=damage_inputs,
        )

        return {
            "grade": grade,
            "summary": summary,
            "trend_classification": trend_classification,
            "damage_classification": damage_classification,
            "rules": rules,
        }

    def _build_rules(
        self,
        latest_ema8: float | None,
        latest_ema21: float | None,
        trend_inputs: Dict[str, Any],
        damage_inputs: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        return [
            self._build_rule(
                label="EMA 8 is above EMA 21",
                status="Pass" if latest_ema8 is not None and latest_ema21 is not None and latest_ema8 > latest_ema21 else "Fail",
                detail=(
                    f"EMA 8 {latest_ema8:.2f} is above EMA 21 {latest_ema21:.2f}."
                    if latest_ema8 is not None and latest_ema21 is not None and latest_ema8 > latest_ema21
                    else self._ema_detail(latest_ema8, latest_ema21, preferred="above")
                ),
                bucket="trend",
            ),
            self._build_rule(
                label="EMA 8 is below EMA 21",
                status="Pass" if latest_ema8 is not None and latest_ema21 is not None and latest_ema8 < latest_ema21 else "Fail",
                detail=(
                    f"EMA 8 {latest_ema8:.2f} is below EMA 21 {latest_ema21:.2f}."
                    if latest_ema8 is not None and latest_ema21 is not None and latest_ema8 < latest_ema21
                    else self._ema_detail(latest_ema8, latest_ema21, preferred="below")
                ),
                bucket="trend",
            ),
            self._build_rule(
                label="Price is mostly above both EMAs",
                status="Pass" if trend_inputs["above_both_ratio"] >= 0.65 and trend_inputs["latest_above_both"] else "Fail",
                detail=(
                    f"{trend_inputs['above_both_ratio'] * 100:.1f}% of the recent window closed above both EMAs."
                    if trend_inputs["above_both_ratio"] >= 0.65 and trend_inputs["latest_above_both"]
                    else f"Only {trend_inputs['above_both_ratio'] * 100:.1f}% of the recent window closed above both EMAs."
                ),
                bucket="trend",
            ),
            self._build_rule(
                label="Price is mostly below both EMAs",
                status="Pass" if trend_inputs["below_both_ratio"] >= 0.65 and trend_inputs["latest_below_both"] else "Fail",
                detail=(
                    f"{trend_inputs['below_both_ratio'] * 100:.1f}% of the recent window closed below both EMAs."
                    if trend_inputs["below_both_ratio"] >= 0.65 and trend_inputs["latest_below_both"]
                    else f"Only {trend_inputs['below_both_ratio'] * 100:.1f}% of the recent window closed below both EMAs."
                ),
                bucket="trend",
            ),
            self._build_rule(
                label="EMAs are flat/crossing or price is flipping around both",
                status="Pass" if trend_inputs["is_chop"] else "Fail",
                detail=self._trend_chop_detail(trend_inputs),
                bucket="trend",
            ),
            self._build_rule(
                label="No sharp selloff is visible in the session",
                status="Pass" if not damage_inputs["sharp_selloff"] else "Fail",
                detail=(
                    f"Max pullback from the session high measured {damage_inputs['max_drawdown']:.2f} points, which is controlled."
                    if not damage_inputs["sharp_selloff"]
                    else f"A sharp selloff occurred with a {damage_inputs['max_drawdown']:.2f}-point pullback from the session high."
                ),
                bucket="damage",
            ),
            self._build_rule(
                label="Recent lows have stabilized instead of continuing lower",
                status="Pass" if damage_inputs["stabilized_lows"] else "Fail",
                detail=(
                    "Recent lows stabilized rather than extending to fresh local lows."
                    if damage_inputs["stabilized_lows"]
                    else "Recent lows are still extending lower."
                ),
                bucket="damage",
            ),
            self._build_rule(
                label="Sustained lower lows and downward continuation are present",
                status="Pass" if damage_inputs["continued_lower_lows"] else "Fail",
                detail=(
                    "Recent price action shows sustained lower lows and continued downward movement."
                    if damage_inputs["continued_lower_lows"]
                    else "Recent price action does not show sustained lower lows and continued downward movement."
                ),
                bucket="damage",
            ),
        ]

    def _build_daily_rsi_context(self, target_session_date: date) -> Dict[str, Any]:
        history_start = target_session_date - timedelta(days=self.DAILY_RSI_LOOKBACK_DAYS)
        try:
            history = self.market_data_service.get_history_with_changes(
                "^GSPC",
                history_start,
                target_session_date,
                query_type="apollo_daily_rsi",
            )
        except MarketDataError as exc:
            LOGGER.info("Apollo daily RSI unavailable for %s: %s", target_session_date.isoformat(), exc)
            return {
                "modifier_label": "None",
                "modifier_applied": False,
                "value": None,
                "period": self.DAILY_RSI_PERIOD,
                "history_points": 0,
                "note": "Daily RSI unavailable; base structure kept.",
                "detail": str(exc),
            }

        closes = pd.to_numeric(history.get("Close"), errors="coerce").dropna().reset_index(drop=True)
        rsi_value = calculate_wilder_rsi(closes.tolist(), period=self.DAILY_RSI_PERIOD)
        if rsi_value is None:
            return {
                "modifier_label": "None",
                "modifier_applied": False,
                "value": None,
                "period": self.DAILY_RSI_PERIOD,
                "history_points": int(len(closes)),
                "note": "Daily RSI unavailable; base structure kept.",
                "detail": "Not enough daily closes were available to compute RSI.",
            }

        modifier_applied = rsi_value < self.RSI_OVERSOLD_THRESHOLD
        rsi_display = round(rsi_value, 2)
        return {
            "modifier_label": "Oversold Boost" if modifier_applied else "None",
            "modifier_applied": modifier_applied,
            "value": rsi_display,
            "period": self.DAILY_RSI_PERIOD,
            "history_points": int(len(closes)),
            "note": (
                f"Daily RSI {rsi_display:.2f} is oversold and upgrades structure by one level."
                if modifier_applied
                else f"Daily RSI {rsi_display:.2f} does not trigger a structure upgrade."
            ),
            "detail": None,
        }

    @classmethod
    def apply_rsi_modifier(cls, base_grade: str, rsi_value: float | None) -> str:
        normalized_grade = str(base_grade or "").title()
        if normalized_grade not in cls.RSI_GRADE_UPGRADE_MAP:
            return base_grade
        if rsi_value is None or rsi_value >= cls.RSI_OVERSOLD_THRESHOLD:
            return normalized_grade
        return cls.RSI_GRADE_UPGRADE_MAP[normalized_grade]

    @staticmethod
    def _build_final_summary(base_summary: str, base_grade: str, final_grade: str, rsi_context: Dict[str, Any]) -> str:
        if base_grade == final_grade or not rsi_context.get("modifier_applied"):
            return base_summary
        rsi_value = rsi_context.get("value")
        if rsi_value is None:
            return base_summary
        return f"{base_summary} Daily RSI {rsi_value:.2f} applied an oversold boost from {base_grade} to {final_grade}."

    @staticmethod
    def _determine_grade(trend_classification: str, damage_classification: str, damage_inputs: Dict[str, Any]) -> str:
        if trend_classification == "Bullish" and damage_classification == "Stable":
            return "Good"
        if trend_classification == "Bearish" and (
            damage_classification == "Breakdown" or damage_inputs["sustained_downtrend"]
        ):
            return "Poor"
        return "Neutral"

    def _build_summary(
        self,
        grade: str,
        trend_classification: str,
        damage_classification: str,
        damage_inputs: Dict[str, Any],
    ) -> str:
        if grade == "Good":
            return (
                f"Structure graded Good: market remained {trend_classification.lower()} and {damage_classification.lower()}, "
                "with trend support intact and no meaningful structural damage."
            )
        if grade == "Poor":
            return (
                f"Structure graded Poor: market remained {trend_classification.lower()} with {damage_classification.lower()}, "
                "showing sustained downward continuation."
            )
        if trend_classification == "Bullish" and damage_classification == "Pullback":
            return "Structure graded Neutral: market stayed bullish but absorbed a pullback without fully restoring stable structure."
        if trend_classification == "Chop" and damage_classification == "Stable":
            return "Structure graded Neutral: market rotated in chop while holding stable structure instead of breaking down."
        if trend_classification == "Chop" and damage_classification == "Pullback":
            return "Structure graded Neutral: market showed bearish pullback but stabilized into chop without continued breakdown."
        if trend_classification == "Bearish" and not damage_inputs["sustained_downtrend"]:
            return "Structure graded Neutral: market leaned bearish, but the session did not confirm a sustained breakdown trend."
        return f"Structure graded Neutral: trend classified as {trend_classification.lower()} with {damage_classification.lower()} damage, leaving the session mixed overall."

    def _build_chart_payload(self, frame: pd.DataFrame) -> Dict[str, Any]:
        points: List[Dict[str, Any]] = []
        for row in frame.itertuples(index=False):
            timestamp = pd.Timestamp(row.Datetime)
            if timestamp.tzinfo is None:
                timestamp = timestamp.tz_localize(self.display_timezone)
            else:
                timestamp = timestamp.tz_convert(self.display_timezone)
            points.append(
                {
                    "time": timestamp.isoformat(),
                    "label": timestamp.strftime("%I:%M %p").lstrip("0"),
                    "open": round(float(row.Open), 2),
                    "high": round(float(row.High), 2),
                    "low": round(float(row.Low), 2),
                    "close": round(float(row.Close), 2),
                    "ema8": self._round_optional(getattr(row, "EMA8", None)),
                    "ema21": self._round_optional(getattr(row, "EMA21", None)),
                }
            )

        return {
            "available": True,
            "points": points,
            "current_price": round(float(frame.iloc[-1]["Close"]), 2),
            "session_high": round(float(frame["High"].max()), 2),
            "session_low": round(float(frame["Low"].min()), 2),
        }

    def _build_unavailable_result(
        self,
        provider_name: str,
        detail: str | None = None,
        attempted_sources: List[Dict[str, str]] | None = None,
    ) -> Dict[str, Any]:
        preferred_source = (self.config.apollo_structure_source or "spx").strip().lower()
        return {
            "available": False,
            "provider_name": provider_name,
            "preferred_source": self.STRUCTURE_SOURCES.get(preferred_source, self.STRUCTURE_SOURCES["spx"])["label"],
            "attempted_sources": attempted_sources or [],
            "fallback_reason": detail,
            "source_used": "Not available",
            "grade": "Not available",
            "base_grade": "Not available",
            "final_grade": "Not available",
            "summary": self.NOT_AVAILABLE_MESSAGE,
            "base_summary": self.NOT_AVAILABLE_MESSAGE,
            "message": self.NOT_AVAILABLE_MESSAGE,
            "session_date_used": None,
            "session_note": None,
            "trend_classification": "Not available",
            "damage_classification": "Not available",
            "metrics": {
                "session_high": None,
                "session_low": None,
                "current_price": None,
                "range_position": None,
                "ema8": None,
                "ema21": None,
                "recent_completed_count": 0,
                "recent_price_action": "Unavailable",
                "session_start": "—",
                "session_end": "—",
            },
            "rules": [],
            "chart": {"available": False, "points": []},
            "detail": detail,
            "rsi_modifier_label": "None",
            "rsi_modifier_applied": False,
            "rsi_value": None,
            "rsi_period": self.DAILY_RSI_PERIOD,
            "rsi_history_points": 0,
            "rsi_note": "Daily RSI unavailable; base structure kept.",
            "rsi_detail": detail,
        }

    @staticmethod
    def _build_rule(label: str, status: str, detail: str, bucket: str) -> Dict[str, Any]:
        return {
            "label": label,
            "passed": status == "Pass",
            "evaluated": status != "Not evaluated",
            "status": status,
            "status_class": status.strip().lower().replace(" ", "-"),
            "detail": detail,
            "bucket": bucket,
        }

    @staticmethod
    def _get_recent_window(frame: pd.DataFrame, size: int = 8) -> pd.DataFrame:
        if frame.empty:
            return frame.copy()
        return frame.tail(min(size, len(frame))).copy()

    def _analyze_trend_inputs(self, frame: pd.DataFrame) -> Dict[str, Any]:
        if frame.empty:
            return {
                "above_both_ratio": 0.0,
                "below_both_ratio": 0.0,
                "latest_above_both": False,
                "latest_below_both": False,
                "ema_cross_count": 0,
                "price_flip_count": 0,
                "is_flat": True,
                "is_chop": True,
            }

        close = pd.to_numeric(frame["Close"], errors="coerce")
        ema8 = pd.to_numeric(frame["EMA8"], errors="coerce")
        ema21 = pd.to_numeric(frame["EMA21"], errors="coerce")
        valid = pd.DataFrame({"close": close, "ema8": ema8, "ema21": ema21}).dropna().reset_index(drop=True)
        if valid.empty:
            return {
                "above_both_ratio": 0.0,
                "below_both_ratio": 0.0,
                "latest_above_both": False,
                "latest_below_both": False,
                "ema_cross_count": 0,
                "price_flip_count": 0,
                "is_flat": True,
                "is_chop": True,
            }

        above_both = valid["close"] > valid[["ema8", "ema21"]].max(axis=1)
        below_both = valid["close"] < valid[["ema8", "ema21"]].min(axis=1)
        price_states = above_both.astype(int) - below_both.astype(int)
        ema_sign = (valid["ema8"] - valid["ema21"]).apply(lambda value: 1 if value > 0 else (-1 if value < 0 else 0))
        ema_cross_count = int((ema_sign.diff().fillna(0) != 0).sum())
        price_flip_count = int((price_states.diff().fillna(0) != 0).sum())
        session_range = max(float(valid["close"].max() - valid["close"].min()), 1.0)
        latest_spread = abs(float(valid.iloc[-1]["ema8"] - valid.iloc[-1]["ema21"]))
        is_flat = latest_spread <= max(session_range * 0.08, 2.5)
        is_chop = is_flat or ema_cross_count >= 2 or price_flip_count >= 3

        return {
            "above_both_ratio": float(above_both.mean()),
            "below_both_ratio": float(below_both.mean()),
            "latest_above_both": bool(above_both.iloc[-1]),
            "latest_below_both": bool(below_both.iloc[-1]),
            "ema_cross_count": ema_cross_count,
            "price_flip_count": price_flip_count,
            "is_flat": is_flat,
            "is_chop": is_chop,
        }

    @staticmethod
    def _classify_trend(latest_ema8: float | None, latest_ema21: float | None, trend_inputs: Dict[str, Any]) -> str:
        if latest_ema8 is None or latest_ema21 is None:
            return "Chop"
        if (
            latest_ema8 > latest_ema21
            and trend_inputs["above_both_ratio"] >= 0.65
            and trend_inputs["latest_above_both"]
            and not trend_inputs["is_chop"]
        ):
            return "Bullish"
        if (
            latest_ema8 < latest_ema21
            and trend_inputs["below_both_ratio"] >= 0.65
            and trend_inputs["latest_below_both"]
            and not trend_inputs["is_chop"]
        ):
            return "Bearish"
        return "Chop"

    def _analyze_damage_inputs(
        self,
        frame: pd.DataFrame,
        recent_completed: pd.DataFrame,
        range_position: float,
    ) -> Dict[str, Any]:
        running_peak = pd.to_numeric(frame["High"], errors="coerce").cummax()
        closes = pd.to_numeric(frame["Close"], errors="coerce")
        max_drawdown = float((running_peak - closes).max()) if not frame.empty else 0.0
        session_range = float(frame["High"].max() - frame["Low"].min()) if not frame.empty else 0.0
        lower_high_lower_low = self._has_lower_high_lower_low_sequence(recent_completed)
        bearish_pressure = self._has_three_bearish_local_low_candles(recent_completed)
        sharp_selloff = max_drawdown >= max(session_range * 0.35, 12.0)

        recent_lows = recent_completed.tail(3)["Low"].tolist() if not recent_completed.empty else []
        recent_closes = recent_completed.tail(3)["Close"].tolist() if not recent_completed.empty else []
        stabilized_lows = len(recent_lows) < 3 or not (recent_lows[2] < recent_lows[1] < recent_lows[0])
        lower_close_sequence = len(recent_closes) >= 3 and (recent_closes[2] < recent_closes[1] < recent_closes[0])
        continued_lower_lows = lower_high_lower_low or bearish_pressure or lower_close_sequence
        sustained_downtrend = continued_lower_lows and range_position <= 0.45

        return {
            "max_drawdown": round(max_drawdown, 2),
            "session_range": round(session_range, 2),
            "sharp_selloff": sharp_selloff,
            "stabilized_lows": stabilized_lows,
            "continued_lower_lows": continued_lower_lows,
            "sustained_downtrend": sustained_downtrend,
        }

    @staticmethod
    def _classify_damage(damage_inputs: Dict[str, Any]) -> str:
        if damage_inputs["continued_lower_lows"] and damage_inputs["sustained_downtrend"]:
            return "Breakdown"
        if damage_inputs["sharp_selloff"]:
            return "Pullback"
        return "Stable"

    @staticmethod
    def _trend_chop_detail(trend_inputs: Dict[str, Any]) -> str:
        if trend_inputs["is_flat"]:
            return "EMA spread is relatively flat, which supports a chop classification."
        if trend_inputs["ema_cross_count"] >= 2:
            return f"EMAs crossed {trend_inputs['ema_cross_count']} times in the recent window, which supports chop."
        if trend_inputs["price_flip_count"] >= 3:
            return f"Price flipped around both EMAs {trend_inputs['price_flip_count']} times in the recent window, which supports chop."
        return "Recent price action was directional enough that chop was not the dominant classification."

    @staticmethod
    def _has_lower_high_lower_low_sequence(frame: pd.DataFrame) -> bool:
        if len(frame) < 6:
            return False
        highs = frame["High"].tolist()
        lows = frame["Low"].tolist()
        return all(highs[index] < highs[index - 1] and lows[index] < lows[index - 1] for index in range(1, len(frame)))

    @staticmethod
    def _has_three_bearish_local_low_candles(frame: pd.DataFrame) -> bool:
        if len(frame) < 3:
            return False
        for start_index in range(0, len(frame) - 2):
            window = frame.iloc[start_index : start_index + 3]
            bearish = True
            near_lows = True
            lower_lows = True
            previous_low = None
            for row in window.itertuples(index=False):
                candle_range = float(row.High) - float(row.Low)
                close_location = ((float(row.Close) - float(row.Low)) / candle_range) if candle_range > 0 else 1.0
                bearish = bearish and float(row.Close) < float(row.Open)
                near_lows = near_lows and close_location <= 0.25
                if previous_low is not None and float(row.Low) >= previous_low:
                    lower_lows = False
                previous_low = float(row.Low)
            if bearish and near_lows and lower_lows:
                return True
        return False

    @staticmethod
    def _summarize_recent_price_action(frame: pd.DataFrame) -> str:
        if frame.empty:
            return "Insufficient completed candles for recent price-action analysis."
        latest_close = float(frame.iloc[-1]["Close"])
        highest_high = float(frame["High"].max())
        lowest_low = float(frame["Low"].min())
        return (
            f"Last {len(frame)} completed candles span {lowest_low:.2f} to {highest_high:.2f} "
            f"with the most recent completed close at {latest_close:.2f}."
        )

    @staticmethod
    def _coerce_float(value: Any) -> float | None:
        if value is None or pd.isna(value):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @classmethod
    def _round_optional(cls, value: Any) -> float | None:
        numeric = cls._coerce_float(value)
        return round(numeric, 2) if numeric is not None else None

    @staticmethod
    def _format_time(value: Any) -> str:
        timestamp = pd.Timestamp(value)
        return timestamp.strftime("%I:%M %p").lstrip("0")

    @staticmethod
    def _ema_detail(latest_ema8: float | None, latest_ema21: float | None, preferred: str) -> str:
        if latest_ema8 is None or latest_ema21 is None:
            return "EMA values were unavailable from the session data."
        comparator = "above" if preferred == "above" else "below"
        return f"EMA 8 {latest_ema8:.2f} is not {comparator} EMA 21 {latest_ema21:.2f}."
