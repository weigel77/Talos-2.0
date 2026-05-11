"""Live macro calendar service for Apollo Gate 2."""

from __future__ import annotations

import copy
from datetime import date, datetime
import logging
import re
from threading import Lock
from typing import Any, Dict, Iterable, List
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

from config import AppConfig, get_app_config


LOGGER = logging.getLogger(__name__)


class MacroSourceError(RuntimeError):
    """Source-specific macro fetch/parsing failure with structured diagnostics."""

    def __init__(self, message: str, diagnostic: Dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.diagnostic = diagnostic or {}


class MacroService:
    """Return normalized macro-event status for Apollo checks."""

    MARKETWATCH_URL = "https://www.marketwatch.com/economy-politics/calendar"
    TRADING_ECONOMICS_URL = "https://tradingeconomics.com/calendar"
    FOREX_FACTORY_URL = "https://www.forexfactory.com/calendar"
    REQUEST_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Upgrade-Insecure-Requests": "1",
    }
    REQUEST_TIMEOUT = 20
    SOURCE_TIMEZONE = ZoneInfo("America/New_York")
    STATUS_CACHE_TTL_SECONDS = 120

    MAJOR_PATTERNS = [
        ("FOMC", re.compile(r"federal funds rate|interest rate decision|rate decision|fomc announcement|fomc decision|fed funds target", re.IGNORECASE)),
        ("CPI", re.compile(r"\bcore cpi\b|\bcpi\b|consumer price index|inflation rate yoy|inflation rate mom", re.IGNORECASE)),
        ("Non-Farm Payrolls", re.compile(r"non[-\s]?farm payroll|\bnfp\b|jobs report|employment situation|jobs[-\s]?nfp", re.IGNORECASE)),
    ]
    MINOR_PATTERNS = [
        ("FOMC Minutes", re.compile(r"fomc minutes|fed(?:eral reserve)?(?:'s)? .*minutes|minutes of .*fomc|minutes of fed", re.IGNORECASE)),
        ("Jobless Claims", re.compile(r"jobless claims|initial claims|continuing claims|unemployment claims", re.IGNORECASE)),
        ("Consumer Sentiment", re.compile(r"consumer sentiment|michigan sentiment|confidence", re.IGNORECASE)),
        ("Consumer Credit", re.compile(r"consumer credit", re.IGNORECASE)),
        ("PMI", re.compile(r"\bpmi\b|ism manufacturing|ism services|manufacturing index|services index", re.IGNORECASE)),
        ("Housing", re.compile(r"housing starts|building permits|new home sales|existing home sales|pending home sales|house price", re.IGNORECASE)),
        ("Factory Orders", re.compile(r"factory orders|durable goods", re.IGNORECASE)),
        ("Fed Speaker", re.compile(r"federal reserve (?!chair).*speak|fed governor.*speak|fed president.*speak|fomc member.*speak|speaker.*federal reserve", re.IGNORECASE)),
    ]

    def __init__(self, config: AppConfig | None = None) -> None:
        self.config = config or get_app_config()
        self.display_timezone = ZoneInfo(self.config.app_timezone)
        self._status_cache: dict[tuple[str, str], dict[str, Any]] = {}
        self._status_cache_lock = Lock()

    def get_macro_status(self, target_date: date | None = None) -> Dict[str, Any]:
        """Return the current macro-event status from live calendar sources."""
        checked_at = datetime.now(self.display_timezone)
        provider = (self.config.macro_provider or "marketwatch").lower()
        target_date = target_date or checked_at.date()
        checked_dates = sorted({checked_at.date(), target_date})
        cache_key = (provider, ",".join(item.isoformat() for item in checked_dates))

        with self._status_cache_lock:
            cached = self._status_cache.get(cache_key)
            if cached and checked_at.timestamp() <= float(cached.get("expires_at") or 0.0):
                return copy.deepcopy(cached.get("payload") or {})

        attempts: List[Dict[str, Any]] = []

        source_loaders = self._resolve_source_loaders(provider)
        for source_key, label, loader in source_loaders:
            try:
                load_result = loader(checked_dates)
                events = load_result.get("events") or []
                diagnostic = self._merge_attempt_diagnostic(
                    label,
                    load_result.get("diagnostic") or {},
                    status="success",
                    detail=f"Loaded {len(events)} matching US event row(s).",
                )
                attempts.append(diagnostic)
                payload = self._build_status_from_events(
                    source_key=source_key,
                    source_label=label,
                    checked_at=checked_at,
                    target_date=target_date,
                    checked_dates=checked_dates,
                    events=events,
                    attempts=attempts,
                    selected_attempt=diagnostic,
                )
                self._store_cached_status(cache_key, payload, checked_at=checked_at)
                return payload
            except MacroSourceError as exc:  # pragma: no cover - network failures are environment-dependent
                diagnostic = self._merge_attempt_diagnostic(
                    label,
                    exc.diagnostic,
                    status="failed",
                    detail=str(exc),
                )
                LOGGER.warning(
                    "Macro source %s failed: status=%s final_url=%s reason=%s snippet=%s",
                    label,
                    diagnostic.get("response_status", "n/a"),
                    diagnostic.get("final_url", "n/a"),
                    diagnostic.get("failure_reason", str(exc)),
                    diagnostic.get("body_snippet", ""),
                )
                attempts.append(diagnostic)
            except Exception as exc:  # pragma: no cover - defensive fallback
                LOGGER.warning("Macro source %s failed: %s", label, exc)
                attempts.append(
                    self._merge_attempt_diagnostic(
                        label,
                        {},
                        status="failed",
                        detail=str(exc),
                    )
                )

        unavailable_payload = {
            "has_major_macro": False,
            "macro_events": [],
            "source_name": "MarketWatch unavailable → no fallback source succeeded",
            "checked_at": checked_at,
            "target_date": target_date,
            "grade": "None",
            "explanation": "Macro check unavailable. MarketWatch could not be read live and backup sources also failed.",
            "configured": True,
            "available": False,
            "checked_dates": [item.isoformat() for item in checked_dates],
            "source_attempts": attempts,
            "diagnostic": attempts[0] if attempts else {},
        }
        self._store_cached_status(cache_key, unavailable_payload, checked_at=checked_at)
        return unavailable_payload

    def _store_cached_status(self, cache_key: tuple[str, str], payload: Dict[str, Any], *, checked_at: datetime) -> None:
        with self._status_cache_lock:
            self._status_cache[cache_key] = {
                "expires_at": checked_at.timestamp() + self.STATUS_CACHE_TTL_SECONDS,
                "payload": copy.deepcopy(payload),
            }

    def _resolve_source_loaders(self, provider: str) -> List[tuple[str, str, Any]]:
        if provider in {"marketwatch", "live", "default"}:
            return [
                ("marketwatch", "MarketWatch", self._load_marketwatch_events),
                ("tradingeconomics", "TradingEconomics", self._load_tradingeconomics_events),
                ("forexfactory", "ForexFactory", self._load_forexfactory_events),
            ]
        if provider == "tradingeconomics":
            return [("tradingeconomics", "TradingEconomics", self._load_tradingeconomics_events)]
        if provider == "forexfactory":
            return [("forexfactory", "ForexFactory", self._load_forexfactory_events)]
        return [
            ("marketwatch", "MarketWatch", self._load_marketwatch_events),
            ("tradingeconomics", "TradingEconomics", self._load_tradingeconomics_events),
        ]

    def _build_status_from_events(
        self,
        source_key: str,
        source_label: str,
        checked_at: datetime,
        target_date: date,
        checked_dates: List[date],
        events: List[Dict[str, Any]],
        attempts: List[Dict[str, Any]],
        selected_attempt: Dict[str, Any],
    ) -> Dict[str, Any]:
        target_events = [item for item in events if item.get("date") == target_date]
        today_events = [item for item in events if item.get("date") == checked_dates[0]]
        grade = self._derive_grade(target_events)
        fallback_used = source_key != "marketwatch"

        if fallback_used:
            source_name = f"{source_label} (MarketWatch fallback)"
        else:
            source_name = "MarketWatch"

        if not target_events:
            explanation = (
                f"No significant US macro events were detected for {target_date.isoformat()}. "
                f"Checked {checked_dates[0].isoformat()} and {target_date.isoformat()}."
            )
        else:
            labels = ", ".join(item["title"] for item in target_events[:3])
            explanation = (
                f"{grade} macro setup for {target_date.isoformat()} based on {len(target_events)} meaningful US event(s): {labels}."
            )
        if today_events and checked_dates[0] != target_date:
            explanation += f" Current-day US events reviewed: {len(today_events)}."

        return {
            "has_major_macro": grade == "Major",
            "macro_events": [self._to_display_event(item) for item in target_events],
            "source_name": source_name,
            "checked_at": checked_at,
            "target_date": target_date,
            "grade": grade,
            "explanation": explanation,
            "configured": True,
            "available": True,
            "checked_dates": [item.isoformat() for item in checked_dates],
            "source_attempts": attempts,
            "fallback_used": fallback_used,
            "diagnostic": selected_attempt,
        }

    def _load_marketwatch_events(self, checked_dates: Iterable[date]) -> Dict[str, Any]:
        response = self._request(self.MARKETWATCH_URL)
        if response.status_code >= 400:
            raise MacroSourceError(
                f"HTTP {response.status_code} from MarketWatch calendar",
                diagnostic=self._build_response_diagnostic(
                    source="MarketWatch",
                    response=response,
                    parser_strategy="marketwatch-economic-calendar-tables",
                    body_snippet=self._body_snippet(response.text),
                    failure_reason=f"HTTP {response.status_code} from MarketWatch calendar",
                ),
            )
        if "Please enable JS" in response.text or "captcha-delivery" in response.text:
            raise MacroSourceError(
                "MarketWatch blocked the request with anti-bot protection",
                diagnostic=self._build_response_diagnostic(
                    source="MarketWatch",
                    response=response,
                    parser_strategy="marketwatch-economic-calendar-tables",
                    body_snippet=self._body_snippet(response.text),
                    failure_reason="MarketWatch blocked the request with anti-bot protection",
                ),
            )
        events = self._parse_marketwatch_html(response.text, checked_dates)
        return {
            "events": events,
            "diagnostic": self._build_response_diagnostic(
                source="MarketWatch",
                response=response,
                parser_strategy="marketwatch-economic-calendar-tables",
                event_count=len(events),
                detail=f"Loaded {len(events)} matching US event row(s).",
            ),
        }

    def _load_tradingeconomics_events(self, checked_dates: Iterable[date]) -> Dict[str, Any]:
        response = self._request(self.TRADING_ECONOMICS_URL)
        if response.status_code >= 400:
            raise MacroSourceError(
                f"HTTP {response.status_code} from TradingEconomics calendar",
                diagnostic=self._build_response_diagnostic(
                    source="TradingEconomics",
                    response=response,
                    parser_strategy="tradingeconomics-table-rows",
                    body_snippet=self._body_snippet(response.text),
                    failure_reason=f"HTTP {response.status_code} from TradingEconomics calendar",
                ),
            )
        events = self._parse_tradingeconomics_html(response.text, checked_dates)
        if events is None:
            raise MacroSourceError(
                "TradingEconomics returned no parseable table rows",
                diagnostic=self._build_response_diagnostic(
                    source="TradingEconomics",
                    response=response,
                    parser_strategy="tradingeconomics-table-rows",
                    body_snippet=self._body_snippet(response.text),
                    failure_reason="TradingEconomics returned no parseable table rows",
                ),
            )
        return {
            "events": events,
            "diagnostic": self._build_response_diagnostic(
                source="TradingEconomics",
                response=response,
                parser_strategy="tradingeconomics-table-rows",
                event_count=len(events),
                detail=f"Loaded {len(events)} matching US event row(s).",
            ),
        }

    def _load_forexfactory_events(self, checked_dates: Iterable[date]) -> Dict[str, Any]:
        response = self._request(self.FOREX_FACTORY_URL)
        if response.status_code >= 400:
            raise MacroSourceError(
                f"HTTP {response.status_code} from ForexFactory calendar",
                diagnostic=self._build_response_diagnostic(
                    source="ForexFactory",
                    response=response,
                    parser_strategy="forexfactory-unavailable",
                    body_snippet=self._body_snippet(response.text),
                    failure_reason=f"HTTP {response.status_code} from ForexFactory calendar",
                ),
            )
        raise MacroSourceError(
            "ForexFactory parsing is unavailable in the current environment",
            diagnostic=self._build_response_diagnostic(
                source="ForexFactory",
                response=response,
                parser_strategy="forexfactory-unavailable",
                body_snippet=self._body_snippet(response.text),
                failure_reason="ForexFactory parsing is unavailable in the current environment",
            ),
        )

    def _request(self, url: str) -> requests.Response:
        with requests.Session() as session:
            session.headers.update(self.REQUEST_HEADERS)
            return session.get(url, timeout=self.REQUEST_TIMEOUT, allow_redirects=True)

    def _parse_marketwatch_html(self, html: str, checked_dates: Iterable[date]) -> List[Dict[str, Any]]:
        """Parse the current MarketWatch economic calendar table layout."""
        soup = BeautifulSoup(html, "html.parser")
        events: List[Dict[str, Any]] = []
        checked = set(checked_dates)
        table_headers = {"time", "time (et)", "report", "period", "actual", "median forecast", "previous"}

        for table in soup.find_all("table"):
            header_cells = [self._clean_text(item.get_text(" ", strip=True)).lower() for item in table.find_all("th")[:6]]
            header_set = set(header_cells)
            if not header_cells or "report" not in header_set or not ({"time", "time (et)"} & header_set):
                continue
            if len(set(header_cells).intersection(table_headers)) < 2:
                continue

            current_date: date | None = None
            for row in table.find_all("tr"):
                cells = row.find_all(["td", "th"])
                if not cells:
                    continue
                cell_texts = [self._clean_text(cell.get_text(" ", strip=True)) for cell in cells]
                first_text = cell_texts[0] if cell_texts else ""
                parsed_date = self._parse_marketwatch_row_date(first_text, checked)
                if parsed_date is not None:
                    current_date = parsed_date
                    continue

                if current_date is None or current_date not in checked or len(cell_texts) < 2:
                    continue

                title = cell_texts[1]
                if not title or title.lower() == "none scheduled":
                    continue

                combined_text = " ".join(filter(None, [title, cell_texts[2] if len(cell_texts) > 2 else "", cell_texts[3] if len(cell_texts) > 3 else ""]))
                impact, reason = self._classify_event(combined_text)
                if impact == "None":
                    continue

                events.append(
                    {
                        "title": combined_text.strip(),
                        "date": current_date,
                        "time": self._format_display_time(first_text, current_date),
                        "impact": impact,
                        "reason": reason,
                        "source": "MarketWatch",
                    }
                )
        return self._sort_events(events)

    def _parse_tradingeconomics_html(self, html: str, checked_dates: Iterable[date]) -> List[Dict[str, Any]] | None:
        soup = BeautifulSoup(html, "html.parser")
        rows = soup.select("tr[data-country]")
        if not rows:
            return None

        checked = set(checked_dates)
        events: List[Dict[str, Any]] = []
        for row in rows:
            country = self._clean_text(row.get("data-country") or "").lower()
            if country != "united states":
                continue

            row_date = self._extract_date_from_row(row)
            if row_date is None or row_date not in checked:
                continue

            event_link = row.select_one("a.calendar-event")
            if event_link is None:
                continue
            title = self._clean_text(event_link.get_text(" ", strip=True))
            reference = row.select_one("span.calendar-reference")
            if reference is not None:
                title = f"{title} {self._clean_text(reference.get_text(' ', strip=True))}".strip()

            combined_text = " ".join(
                filter(
                    None,
                    [
                        title,
                        self._clean_text(row.get("data-category") or ""),
                        self._clean_text(row.get("data-event") or ""),
                    ],
                )
            )
            impact, reason = self._classify_event(combined_text)
            if impact == "None":
                continue

            events.append(
                {
                    "title": title,
                    "date": row_date,
                    "time": self._format_display_time(self._extract_row_time(row), row_date),
                    "impact": impact,
                    "reason": reason,
                    "source": "TradingEconomics",
                }
            )

        return self._sort_events(events)

    def _classify_event(self, text: str) -> tuple[str, str]:
        normalized = self._clean_text(text)
        for label, pattern in self.MAJOR_PATTERNS:
            if pattern.search(normalized):
                return "Major", label
        for label, pattern in self.MINOR_PATTERNS:
            if pattern.search(normalized):
                return "Minor", label
        return "Minor", "Other US Macro"

    def _derive_grade(self, events: List[Dict[str, Any]]) -> str:
        if any(item.get("impact") == "Major" for item in events):
            return "Major"
        if any(item.get("impact") == "Minor" for item in events):
            return "Minor"
        return "None"

    def _extract_date_from_row(self, row: Any) -> date | None:
        first_cell = row.find("td")
        if first_cell is None:
            return None
        classes = " ".join(first_cell.get("class") or [])
        return self._extract_iso_date_from_text(classes)

    @staticmethod
    def _extract_iso_date_from_text(text: str) -> date | None:
        match = re.search(r"(20\d{2}-\d{2}-\d{2})", text)
        if not match:
            return None
        return datetime.strptime(match.group(1), "%Y-%m-%d").date()

    def _extract_row_time(self, row: Any) -> str:
        span = row.select_one("span.calendar-date-1, span.calendar-date-2")
        if span is not None:
            return self._clean_text(span.get_text(" ", strip=True))
        first_cell = row.find("td")
        if first_cell is None:
            return "Time unavailable"
        return self._clean_text(first_cell.get_text(" ", strip=True)) or "Time unavailable"

    def _parse_marketwatch_row_date(self, text: str, checked_dates: Iterable[date]) -> date | None:
        match = re.search(r"\b(?:MONDAY|TUESDAY|WEDNESDAY|THURSDAY|FRIDAY|SATURDAY|SUNDAY),\s+([A-Z][a-z]+)\s+(\d{1,2})\b", text, flags=re.IGNORECASE)
        if not match:
            return None
        month_name = match.group(1)
        day_value = int(match.group(2))
        return self._infer_year_for_month_day(month_name, day_value, checked_dates)

    @staticmethod
    def _infer_year_for_month_day(month_name: str, day_value: int, checked_dates: Iterable[date]) -> date | None:
        try:
            month_number = datetime.strptime(month_name, "%B").month
        except ValueError:
            try:
                month_number = datetime.strptime(month_name, "%b").month
            except ValueError:
                return None

        references = list(checked_dates) or [date.today()]
        candidate_years = {item.year - 1 for item in references} | {item.year for item in references} | {item.year + 1 for item in references}
        candidates: List[date] = []
        for year in candidate_years:
            try:
                candidates.append(date(year, month_number, day_value))
            except ValueError:
                continue
        if not candidates:
            return None
        return min(candidates, key=lambda candidate: min(abs((candidate - ref).days) for ref in references))

    @staticmethod
    def _extract_time_text(text: str) -> str:
        match = re.search(r"\b\d{1,2}:\d{2}\s*[AP]M\b", text, flags=re.IGNORECASE)
        if match:
            return match.group(0).upper().replace("  ", " ")
        return "Time unavailable"

    def _format_display_time(self, time_text: str, event_date: date) -> str:
        clean = self._clean_text(time_text)
        if not clean or clean.lower() == "time unavailable":
            return "Time unavailable"
        if clean.lower() in {"all day", "tentative"}:
            return clean.title()
        try:
            parsed = datetime.strptime(clean.upper(), "%I:%M %p")
            localized = datetime.combine(event_date, parsed.time(), tzinfo=self.SOURCE_TIMEZONE).astimezone(self.display_timezone)
            return localized.strftime("%I:%M %p %Z").lstrip("0")
        except ValueError:
            return clean

    @staticmethod
    def _to_display_event(event: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "title": event.get("title", "—"),
            "time": event.get("time", "Time unavailable"),
            "impact": event.get("impact", "None"),
            "reason": event.get("reason", ""),
        }

    @staticmethod
    def _clean_text(value: str) -> str:
        return re.sub(r"\s+", " ", str(value or "")).strip()

    @staticmethod
    def _body_snippet(value: str, limit: int = 220) -> str:
        return re.sub(r"\s+", " ", str(value or "")).strip()[:limit]

    def _build_response_diagnostic(
        self,
        source: str,
        response: requests.Response,
        parser_strategy: str,
        event_count: int = 0,
        detail: str = "",
        body_snippet: str = "",
        failure_reason: str = "",
    ) -> Dict[str, Any]:
        return {
            "source": source,
            "response_status": response.status_code,
            "final_url": response.url,
            "parser_strategy": parser_strategy,
            "event_count": event_count,
            "failure_reason": failure_reason,
            "body_snippet": body_snippet,
            "detail": detail,
        }

    @staticmethod
    def _merge_attempt_diagnostic(source: str, diagnostic: Dict[str, Any], status: str, detail: str) -> Dict[str, Any]:
        return {
            "source": source,
            "status": status,
            "detail": detail,
            "response_status": diagnostic.get("response_status", "—"),
            "final_url": diagnostic.get("final_url", "—"),
            "parser_strategy": diagnostic.get("parser_strategy", "—"),
            "event_count": diagnostic.get("event_count", 0),
            "failure_reason": diagnostic.get("failure_reason", ""),
            "body_snippet": diagnostic.get("body_snippet", ""),
        }

    @staticmethod
    def _sort_events(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return sorted(events, key=lambda item: (item.get("date"), item.get("time", ""), item.get("title", "")))
