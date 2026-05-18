"""Separate Schwab trading auth and account retrieval for the Talos execution engine."""

from __future__ import annotations

import base64
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

import requests

from config import AppConfig, get_app_config
from services.providers.base_provider import ProviderAuthRequiredError, ProviderError, ProviderReauthenticationRequiredError
from services.repositories.token_repository import JsonFileTokenRepository, TokenRepository


class SchwabTradingAuthService:
    """Manage the separate Schwab trading OAuth flow and trader-account summary."""

    ACCESS_TOKEN_EXPIRY_BUFFER_SECONDS = 60

    def __init__(self, config: AppConfig | None = None, token_store: TokenRepository | None = None) -> None:
        self.config = config or get_app_config()
        self.token_store = token_store or JsonFileTokenRepository(self.config.schwab_trading_token_path)

    @staticmethod
    def _utcnow() -> datetime:
        return datetime.now(timezone.utc).replace(tzinfo=None)

    @staticmethod
    def build_state_token() -> str:
        return secrets.token_urlsafe(32)

    def build_authorization_url(self, state: Optional[str] = None) -> str:
        self._validate_oauth_config()
        params = {
            "client_id": self.config.schwab_trading_client_id,
            "redirect_uri": self.config.schwab_trading_redirect_uri,
            "response_type": "code",
        }
        if state:
            params["state"] = state
        return requests.Request("GET", self.config.schwab_trading_auth_url, params=params).prepare().url

    def exchange_code_for_tokens(self, authorization_code: str) -> Dict[str, Any]:
        self._validate_oauth_config()
        response = requests.post(
            self.config.schwab_trading_token_url,
            data={
                "grant_type": "authorization_code",
                "code": authorization_code,
                "redirect_uri": self.config.schwab_trading_redirect_uri,
                "client_id": self.config.schwab_trading_client_id,
                "client_secret": self.config.schwab_trading_client_secret,
            },
            headers=self._build_token_headers(),
            timeout=30,
        )
        return self._store_token_response(response)

    def refresh_access_token(self) -> Dict[str, Any]:
        self._validate_oauth_config()
        tokens = self.token_store.load() or {}
        refresh_token = str(tokens.get("refresh_token") or "").strip()
        if not refresh_token:
            raise ProviderAuthRequiredError("Schwab trading authentication required")
        if self._is_refresh_token_expired(tokens):
            self._persist_token_state(tokens, auth_state="refresh_expired", last_auth_error="Schwab trading refresh expired.")
            raise ProviderReauthenticationRequiredError("Schwab trading refresh expired")

        refresh_attempted_at = self._utcnow().isoformat()
        self._persist_debug_metadata(
            tokens,
            last_refresh_attempt_at=refresh_attempted_at,
            last_refresh_result="attempted",
            token_preserved_on_refresh_failure="yes" if bool(refresh_token) else "no",
        )

        response = requests.post(
            self.config.schwab_trading_token_url,
            data={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": self.config.schwab_trading_client_id,
                "client_secret": self.config.schwab_trading_client_secret,
            },
            headers=self._build_token_headers(),
            timeout=30,
        )
        return self._store_token_response(
            response,
            existing_tokens=tokens,
            existing_refresh_token=refresh_token,
            source="refresh",
            refresh_attempted_at=refresh_attempted_at,
        )

    def get_valid_access_token(self) -> str:
        tokens = self.token_store.load() or {}
        if not tokens:
            raise ProviderAuthRequiredError("Schwab trading authentication required")

        access_token = str(tokens.get("access_token") or "").strip()
        auth_state = str(tokens.get("auth_state") or "").strip().lower()
        expires_at = self._parse_datetime(tokens.get("expires_at"))
        access_expired = (expires_at is None) or (self._utcnow() >= expires_at)
        refresh_expired = self._is_refresh_token_expired(tokens)

        if auth_state == "refresh_expired" and refresh_expired:
            raise ProviderReauthenticationRequiredError("Schwab trading refresh expired")
        if auth_state == "refresh_expired" and access_token and not access_expired:
            self._persist_token_state(tokens, auth_state="connected", last_auth_error="")
            return access_token

        if access_expired:
            tokens = self.refresh_access_token()
            access_token = str(tokens.get("access_token") or "").strip()

        if not access_token:
            raise ProviderAuthRequiredError("Schwab trading authentication required")

        return access_token

    def recover_from_unauthorized_response(self) -> str:
        tokens = self.refresh_access_token()
        access_token = str(tokens.get("access_token") or "").strip()
        if not access_token:
            raise ProviderReauthenticationRequiredError("Schwab trading refresh failed")
        return access_token

    def logout(self) -> None:
        self.token_store.clear()

    def get_connection_status(self) -> Dict[str, Any]:
        tokens = self.token_store.load() or {}
        access_token = str(tokens.get("access_token") or "").strip()
        refresh_token = str(tokens.get("refresh_token") or "").strip()
        expires_at = self._parse_datetime(tokens.get("expires_at"))
        access_valid = bool(access_token and expires_at is not None and not self._is_access_token_expired(expires_at))
        refresh_expired = self._is_refresh_token_expired(tokens)
        usable_refresh_chain = bool(refresh_token) and not refresh_expired
        last_auth_error = str(tokens.get("last_auth_error") or "").strip()

        if not access_token and not usable_refresh_chain:
            return {
                "connected": False,
                "requires_login": True,
                "requires_refresh": False,
                "status_label": "Execution auth disconnected",
                "status_meta": "Manual fallback active",
                "token_expiration": None,
                "token_expiration_display": "—",
                "refresh_token_expiration": None,
                "refresh_token_expiration_display": "—",
                "usable_token_chain": False,
                "state_key": "disconnected",
            }

        if not usable_refresh_chain and not access_valid:
            return {
                "connected": False,
                "requires_login": True,
                "requires_refresh": False,
                "status_label": "Reconnect required",
                "status_meta": last_auth_error or "Execution refresh token expired or missing.",
                "token_expiration": expires_at.isoformat() if expires_at is not None else None,
                "token_expiration_display": expires_at.isoformat(sep=" ", timespec="minutes") if expires_at is not None else "—",
                "refresh_token_expiration": str(tokens.get("refresh_expires_at") or "") or None,
                "refresh_token_expiration_display": self._format_datetime_display(tokens.get("refresh_expires_at")),
                "usable_token_chain": False,
                "state_key": "reconnect-required",
            }

        if access_valid:
            return {
                "connected": True,
                "requires_login": False,
                "requires_refresh": False,
                "status_label": "Connected",
                "status_meta": "Schwab trading",
                "token_expiration": expires_at.isoformat(),
                "token_expiration_display": expires_at.isoformat(sep=" ", timespec="minutes"),
                "refresh_token_expiration": str(tokens.get("refresh_expires_at") or "") or None,
                "refresh_token_expiration_display": self._format_datetime_display(tokens.get("refresh_expires_at")),
                "usable_token_chain": True,
                "state_key": "connected",
            }

        return {
            "connected": True,
            "requires_login": False,
            "requires_refresh": True,
            "status_label": "Refreshing execution token",
            "status_meta": last_auth_error or "Refresh token valid; Talos can refresh without re-login.",
            "token_expiration": expires_at.isoformat() if expires_at is not None else None,
            "token_expiration_display": expires_at.isoformat(sep=" ", timespec="minutes") if expires_at is not None else "—",
            "refresh_token_expiration": str(tokens.get("refresh_expires_at") or "") or None,
            "refresh_token_expiration_display": self._format_datetime_display(tokens.get("refresh_expires_at")),
            "usable_token_chain": True,
            "state_key": "refreshing",
        }

    def get_debug_status(self) -> Dict[str, Any]:
        file_path = getattr(self.token_store, "file_path", None)
        tokens = self.token_store.load() or {}
        access_token = str(tokens.get("access_token") or "").strip()
        refresh_token = str(tokens.get("refresh_token") or "").strip()
        expires_at = self._parse_datetime(tokens.get("expires_at"))
        refresh_expires_at = self._parse_datetime(tokens.get("refresh_expires_at"))
        token_valid = bool(access_token and expires_at is not None and not self._is_access_token_expired(expires_at))
        return {
            "token_file_path": str(file_path or self.config.schwab_trading_token_path),
            "token_file_exists": bool(file_path and file_path.exists()),
            "token_loaded": bool(tokens),
            "access_token_loaded": bool(access_token),
            "refresh_token_loaded": bool(refresh_token),
            "token_valid": token_valid,
            "auth_state": str(tokens.get("auth_state") or "").strip() or "missing",
            "last_auth_error": str(tokens.get("last_auth_error") or "").strip() or "—",
            "token_expiration_display": expires_at.isoformat(sep=" ", timespec="minutes") if expires_at is not None else "—",
            "refresh_token_expiration_display": refresh_expires_at.isoformat(sep=" ", timespec="minutes") if refresh_expires_at is not None else "—",
            "last_refresh_attempt_display": self._format_datetime_display(tokens.get("last_refresh_attempt_at")),
            "last_refresh_result": str(tokens.get("last_refresh_result") or "Not attempted").strip() or "Not attempted",
            "token_preserved_on_refresh_failure": str(tokens.get("token_preserved_on_refresh_failure") or "unknown").strip() or "unknown",
        }

    def get_account_summary(self) -> Dict[str, Any]:
        identity = self.get_account_identity()
        primary_account = {
            "accountNumber": identity.get("account_number_masked") or identity.get("account_number") or "",
        }
        account_hash = str(identity.get("account_hash") or "").strip()

        account_detail_response = self._authorized_get(
            f"{self.config.schwab_trading_base_url}/accounts/{account_hash}",
            params={"fields": "positions"},
        )
        if account_detail_response.status_code >= 400:
            raise ProviderError(
                f"Unable to retrieve Schwab trading account balances right now ({account_detail_response.status_code}).",
                is_transient=account_detail_response.status_code >= 500,
            )

        account_payload = account_detail_response.json()
        securities_account = (account_payload or {}).get("securitiesAccount") if isinstance(account_payload, dict) else None
        if not isinstance(securities_account, dict):
            raise ProviderError("Schwab trading returned an unexpected account payload.")

        balances = securities_account.get("currentBalances") or securities_account.get("initialBalances") or {}
        liquidation_value = self._coerce_float(
            balances.get("liquidationValue")
            or balances.get("netLiquidationValue")
            or balances.get("equity")
            or balances.get("cashBalance")
        )
        buying_power = self._coerce_float(
            balances.get("buyingPower")
            or balances.get("dayTradingBuyingPower")
            or balances.get("cashAvailableForTrading")
        )
        timestamp = self._parse_timestamp(
            balances.get("timestamp")
            or balances.get("asOfTime")
            or balances.get("lastUpdated")
        ) or datetime.now(timezone.utc)

        return {
            "account_number_masked": str(primary_account.get("accountNumber") or "").strip(),
            "account_hash": account_hash,
            "account_type": str(securities_account.get("type") or securities_account.get("accountType") or "Schwab").strip(),
            "liquidation_value": liquidation_value,
            "buying_power": buying_power,
            "timestamp": timestamp.isoformat(),
            "as_of_display": timestamp.astimezone().strftime("%Y-%m-%d %I:%M %p %Z"),
        }

    def get_account_identity(self) -> Dict[str, Any]:
        account_numbers_response = self._authorized_get(f"{self.config.schwab_trading_base_url}/accounts/accountNumbers", params={})
        if account_numbers_response.status_code >= 400:
            raise ProviderError(
                f"Unable to retrieve Schwab trading account numbers right now ({account_numbers_response.status_code}).",
                is_transient=account_numbers_response.status_code >= 500,
            )

        account_numbers_payload = account_numbers_response.json()
        account_entries = account_numbers_payload if isinstance(account_numbers_payload, list) else []
        if not account_entries:
            raise ProviderError("Schwab trading returned no linked accounts for this session.")

        primary_account = account_entries[0] if isinstance(account_entries[0], dict) else {}
        account_hash = str(primary_account.get("hashValue") or primary_account.get("accountHash") or "").strip()
        if not account_hash:
            raise ProviderError("Schwab trading returned an account entry without a usable account hash.")
        account_number = str(primary_account.get("accountNumber") or "").strip()
        return {
            "account_hash": account_hash,
            "account_number": account_number,
            "account_number_masked": account_number,
        }

    def _authorized_get(self, url: str, *, params: Dict[str, Any]) -> requests.Response:
        try:
            access_token = self.get_valid_access_token()
        except ProviderAuthRequiredError:
            raise
        except ProviderReauthenticationRequiredError:
            raise
        response = requests.get(
            url,
            params=params,
            headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
            timeout=30,
        )
        if response.status_code != 401:
            return response

        refreshed_token = self.recover_from_unauthorized_response()
        return requests.get(
            url,
            params=params,
            headers={"Authorization": f"Bearer {refreshed_token}", "Accept": "application/json"},
            timeout=30,
        )

    def _store_token_response(
        self,
        response: requests.Response,
        *,
        existing_tokens: Dict[str, Any] | None = None,
        existing_refresh_token: str | None = None,
        source: str = "exchange",
        refresh_attempted_at: str = "",
    ) -> Dict[str, Any]:
        payload = response.json() if response.content else {}
        if response.status_code >= 400:
            detail = ""
            if isinstance(payload, dict):
                detail = str(payload.get("error_description") or payload.get("error") or "").strip()
            persisted_tokens = dict(existing_tokens or self.token_store.load() or {})
            self._persist_token_state(
                persisted_tokens,
                auth_state="refresh_expired" if response.status_code in {400, 401} else "error",
                last_auth_error=detail or "Schwab trading authentication failed.",
            )
            if source == "refresh":
                self._persist_debug_metadata(
                    persisted_tokens,
                    last_refresh_attempt_at=refresh_attempted_at or self._utcnow().isoformat(),
                    last_refresh_result="failed",
                    token_preserved_on_refresh_failure="yes" if bool(str((persisted_tokens or {}).get("refresh_token") or "").strip()) else "no",
                )
            if response.status_code in {400, 401}:
                raise ProviderReauthenticationRequiredError(detail or "Schwab trading authentication required")
            raise ProviderError(detail or f"Schwab trading token request failed ({response.status_code}).", is_transient=response.status_code >= 500)

        token_payload = payload if isinstance(payload, dict) else {}
        refresh_token = str(token_payload.get("refresh_token") or existing_refresh_token or (existing_tokens or {}).get("refresh_token") or "").strip()
        expires_in_seconds = int(token_payload.get("expires_in") or 1800)
        refresh_expires_in_seconds = int(token_payload.get("refresh_token_expires_in") or 604800)
        now = self._utcnow()
        persisted = {
            **dict(existing_tokens or {}),
            **token_payload,
            "refresh_token": refresh_token,
            "expires_at": (now + timedelta(seconds=expires_in_seconds)).isoformat(),
            "refresh_expires_at": (now + timedelta(seconds=refresh_expires_in_seconds)).isoformat() if refresh_token else "",
            "auth_state": "connected",
            "last_auth_error": "",
            "last_refresh_attempt_at": refresh_attempted_at or dict(existing_tokens or {}).get("last_refresh_attempt_at") or "",
            "last_refresh_result": "succeeded" if source == "refresh" else str(dict(existing_tokens or {}).get("last_refresh_result") or "Not needed after login"),
            "token_preserved_on_refresh_failure": "yes" if refresh_token else "no",
        }
        self.token_store.save(persisted)
        return persisted

    def _persist_token_state(self, tokens: Dict[str, Any], *, auth_state: str, last_auth_error: str) -> None:
        persisted = dict(self.token_store.load() or {})
        persisted.update(dict(tokens or {}))
        persisted["auth_state"] = auth_state
        persisted["last_auth_error"] = last_auth_error
        self.token_store.save(persisted)

    def _persist_debug_metadata(self, tokens: Dict[str, Any], **fields: Any) -> None:
        persisted = dict(self.token_store.load() or {})
        persisted.update(dict(tokens or {}))
        persisted.update(fields)
        self.token_store.save(persisted)

    def _validate_oauth_config(self) -> None:
        if not self.config.schwab_trading_client_id or not self.config.schwab_trading_client_secret or not self.config.schwab_trading_redirect_uri:
            raise RuntimeError("Schwab trading execution auth is not configured.")

    def _build_token_headers(self) -> Dict[str, str]:
        encoded = base64.b64encode(
            f"{self.config.schwab_trading_client_id}:{self.config.schwab_trading_client_secret}".encode("utf-8")
        ).decode("utf-8")
        return {
            "Authorization": f"Basic {encoded}",
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        }

    def _is_refresh_token_expired(self, tokens: Dict[str, Any]) -> bool:
        refresh_expires_at = self._parse_datetime(tokens.get("refresh_expires_at"))
        return refresh_expires_at is not None and self._utcnow() >= refresh_expires_at

    def _is_access_token_expired(self, expires_at: datetime) -> bool:
        return self._utcnow() >= (expires_at - timedelta(seconds=self.ACCESS_TOKEN_EXPIRY_BUFFER_SECONDS))

    def _parse_datetime(self, value: Any) -> datetime | None:
        raw_value = str(value or "").strip()
        if not raw_value:
            return None
        try:
            parsed = datetime.fromisoformat(raw_value)
        except ValueError:
            return None
        if parsed.tzinfo is not None:
            parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
        return parsed

    def _format_datetime_display(self, value: Any) -> str:
        parsed = self._parse_datetime(value)
        if parsed is None:
            return "—"
        return parsed.isoformat(sep=" ", timespec="minutes")

    def _parse_timestamp(self, value: Any) -> datetime | None:
        raw_value = str(value or "").strip()
        if not raw_value:
            return None
        try:
            parsed = datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
        except ValueError:
            return None
        return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)

    def _coerce_float(self, value: Any) -> float | None:
        try:
            if value in {None, ""}:
                return None
            return float(value)
        except (TypeError, ValueError):
            return None