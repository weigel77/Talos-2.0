"""OAuth helpers for Schwab authorization and token refresh."""

from __future__ import annotations

import base64
import logging
import secrets
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urlencode

import requests

from config import AppConfig

from .repositories.token_repository import JsonFileTokenRepository, TokenRepository
from .providers.base_provider import (
    ProviderAuthRequiredError,
    ProviderConfigurationError,
    ProviderReauthenticationRequiredError,
)


LOGGER = logging.getLogger(__name__)


class SchwabAuthService:
    """Manage Schwab OAuth URLs, token exchange, and refresh operations."""

    DEFAULT_ACCESS_TOKEN_LIFETIME_SECONDS = 1800
    DEFAULT_REFRESH_TOKEN_LIFETIME = timedelta(days=7)
    ACCESS_TOKEN_EXPIRY_BUFFER_SECONDS = 60
    REFRESH_TOKEN_EXPIRY_BUFFER_SECONDS = 300

    def __init__(self, config: AppConfig, token_store: Optional[TokenRepository] = None) -> None:
        self.config = config
        self.token_store = token_store or JsonFileTokenRepository(config.schwab_shared_market_token_path)

    def build_state_token(self) -> str:
        """Return a random state token for the OAuth redirect."""
        return secrets.token_urlsafe(32)

    @staticmethod
    def _utcnow() -> datetime:
        return datetime.now(timezone.utc).replace(tzinfo=None)

    def build_authorization_url(self, state: Optional[str] = None) -> str:
        """Build the Schwab OAuth authorization URL."""
        self._validate_oauth_config()
        query = {
            "client_id": self.config.schwab_client_id,
            "redirect_uri": self.config.schwab_redirect_uri,
            "response_type": "code",
        }
        if state:
            query["state"] = state
        return f"{self.config.schwab_auth_url}?{urlencode(query)}"

    def exchange_code_for_tokens(self, authorization_code: str) -> Dict[str, Any]:
        """Exchange an OAuth authorization code for access and refresh tokens."""
        self._validate_oauth_config()
        response = requests.post(
            self.config.schwab_token_url,
            data={
                "grant_type": "authorization_code",
                "code": authorization_code,
                "redirect_uri": self.config.schwab_redirect_uri,
                "client_id": self.config.schwab_client_id,
                "client_secret": self.config.schwab_client_secret,
            },
            headers=self._build_token_headers(),
            timeout=30,
        )
        return self._store_token_response(response)

    def refresh_access_token(self) -> Dict[str, Any]:
        """Refresh the Schwab access token using the stored refresh token."""
        self._validate_oauth_config()
        tokens = self.token_store.load() or {}
        refresh_token = str(tokens.get("refresh_token") or "").strip()
        if not refresh_token:
            self._log_auth_event(
                "refresh_failed",
                level=logging.WARNING,
                reason="missing_refresh_token",
                token_file_exists=self._token_file_exists(),
            )
            raise ProviderAuthRequiredError("Schwab authentication required")
        if self._is_refresh_token_expired(tokens):
            self._persist_token_state(
                tokens,
                auth_state="refresh_expired",
                last_auth_error="Schwab refresh token expired. Please log in again.",
            )
            self._log_auth_event(
                "refresh_failed",
                level=logging.WARNING,
                reason="refresh_token_expired",
                refresh_expires_at=tokens.get("refresh_expires_at"),
                token_file_exists=self._token_file_exists(),
            )
            raise ProviderReauthenticationRequiredError("Schwab refresh expired")

        self._log_auth_event(
            "refresh_attempted",
            refresh_expires_at=tokens.get("refresh_expires_at"),
            token_file_exists=self._token_file_exists(),
        )

        response = requests.post(
            self.config.schwab_token_url,
            data={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": self.config.schwab_client_id,
                "client_secret": self.config.schwab_client_secret,
            },
            headers=self._build_token_headers(),
            timeout=30,
        )
        try:
            refreshed_tokens = self._store_token_response(
                response,
                existing_tokens=tokens,
                existing_refresh_token=refresh_token,
            )
        except ProviderReauthenticationRequiredError as exc:
            self._log_auth_event(
                "refresh_failed",
                level=logging.WARNING,
                reason=type(exc).__name__,
                status_code=response.status_code,
                token_file_exists=self._token_file_exists(),
            )
            raise
        self._log_auth_event(
            "refresh_succeeded",
            expires_at=refreshed_tokens.get("expires_at"),
            refresh_expires_at=refreshed_tokens.get("refresh_expires_at"),
            token_file_exists=self._token_file_exists(),
        )
        return refreshed_tokens

    def get_valid_access_token(self) -> str:
        """Return a valid access token, refreshing it if needed."""
        tokens = self.token_store.load()
        if not tokens:
            raise ProviderAuthRequiredError("Schwab authentication required")

        access_token = str(tokens.get("access_token") or "").strip()
        auth_state = str(tokens.get("auth_state") or "").strip().lower()
        expires_at = self._parse_datetime(tokens.get("expires_at"))
        access_expired = (expires_at is None) or (self._utcnow() >= expires_at)
        refresh_expired = self._is_refresh_token_expired(tokens)

        if auth_state == "refresh_expired" and refresh_expired:
            raise ProviderReauthenticationRequiredError("Schwab refresh expired")
        if auth_state == "refresh_expired" and access_token and not access_expired:
            self._persist_token_state(tokens, auth_state="connected", last_auth_error="")
            return access_token

        if access_expired:
            try:
                tokens = self.refresh_access_token()
            except ProviderAuthRequiredError:
                raise
            except ProviderReauthenticationRequiredError:
                raise
            except Exception as exc:
                self._persist_token_state(
                    tokens,
                    auth_state="refresh_failed",
                    last_auth_error="Unable to refresh Schwab token right now.",
                )
                raise ProviderReauthenticationRequiredError("Schwab token refresh failed") from exc
            access_token = str(tokens.get("access_token") or "").strip()

        if not access_token:
            if str(tokens.get("refresh_token") or "").strip() and not self._is_refresh_token_expired(tokens):
                self._persist_token_state(
                    tokens,
                    auth_state="refresh_failed",
                    last_auth_error="Schwab access token missing. Automatic refresh will be retried.",
                )
                raise ProviderReauthenticationRequiredError("Schwab token refresh failed")
            raise ProviderAuthRequiredError("Schwab authentication required")

        return access_token

    def recover_from_unauthorized_response(self) -> str:
        """Refresh and return a new access token after an upstream 401 response."""
        try:
            tokens = self.refresh_access_token()
        except ProviderAuthRequiredError:
            raise
        except ProviderReauthenticationRequiredError:
            raise
        except Exception as exc:
            persisted_tokens = self.token_store.load() or {}
            self._persist_token_state(
                persisted_tokens,
                auth_state="refresh_failed",
                last_auth_error="Unable to refresh Schwab token right now.",
            )
            raise ProviderReauthenticationRequiredError("Schwab token refresh failed") from exc

        access_token = str(tokens.get("access_token") or "").strip()
        if not access_token:
            self._persist_token_state(
                tokens,
                auth_state="refresh_expired",
                last_auth_error="Schwab did not return a usable access token. Please log in again.",
            )
            raise ProviderReauthenticationRequiredError("Schwab refresh expired")
        return access_token

    def is_authenticated(self) -> bool:
        """Return whether a token payload currently exists."""
        tokens = self.token_store.load()
        return bool(tokens and tokens.get("access_token"))

    def clear_tokens(self) -> None:
        """Remove any stored tokens."""
        self.token_store.clear()

    def mark_reauthentication_required(self, message: str) -> None:
        """Persist a login-required status without deleting the current token payload."""
        tokens = self.token_store.load() or {}
        if not tokens:
            return
        self._persist_token_state(tokens, auth_state="refresh_expired", last_auth_error=message)

    def get_connection_status(self) -> Dict[str, Any]:
        """Return a UI-friendly snapshot of the current Schwab connection state."""
        tokens = self.token_store.load() or {}
        access_token = str(tokens.get("access_token") or "").strip()
        refresh_token = str(tokens.get("refresh_token") or "").strip()
        auth_state = str(tokens.get("auth_state") or "connected").strip().lower() or "connected"
        last_auth_error = str(tokens.get("last_auth_error") or "").strip()

        if not tokens:
            return {
                "connected": False,
                "requires_login": True,
                "requires_refresh": False,
                "status_label": "Schwab login required",
                "status_meta": "Schwab not connected",
                "auth_state": "login_required",
            }

        access_expires_at = self._parse_datetime(tokens.get("expires_at"))
        access_expired = (access_expires_at is None) or (self._utcnow() >= access_expires_at)
        refresh_available = bool(refresh_token)
        refresh_expired = self._is_refresh_token_expired(tokens)

        if auth_state == "refresh_expired" and refresh_expired:
            return {
                "connected": False,
                "requires_login": True,
                "requires_refresh": False,
                "status_label": "Schwab refresh expired",
                "status_meta": last_auth_error or "Please log in to Schwab again.",
                "auth_state": auth_state,
            }

        if access_expired:
            if refresh_available and not refresh_expired:
                return {
                    "connected": False,
                    "requires_login": False,
                    "requires_refresh": False,
                    "status_label": "Refreshing Schwab token",
                    "status_meta": last_auth_error or "Schwab will refresh automatically on the next request.",
                    "auth_state": "refreshing",
                }
            return {
                "connected": False,
                "requires_login": True,
                "requires_refresh": False,
                "status_label": "Schwab refresh expired",
                "status_meta": last_auth_error or "Please log in to Schwab again.",
                "auth_state": "refresh_expired",
            }

        return {
            "connected": bool(access_token),
            "requires_login": not bool(access_token),
            "requires_refresh": False,
            "status_label": "Connected" if access_token else "Schwab login required",
            "status_meta": "Schwab" if access_token else "Schwab not connected",
            "auth_state": auth_state,
        }

    def _store_token_response(
        self,
        response: requests.Response,
        existing_tokens: Optional[Dict[str, Any]] = None,
        existing_refresh_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Validate a token response and persist the normalized payload."""
        if response.status_code >= 400:
            self._log_auth_event("token_write_failed", level=logging.WARNING, status_code=response.status_code)
            if existing_tokens:
                message = (
                    "Schwab refresh token was rejected. Please log in again."
                    if response.status_code in {400, 401}
                    else f"Unable to refresh Schwab token right now ({response.status_code})."
                )
                self._persist_token_state(
                    existing_tokens,
                    auth_state="refresh_expired" if response.status_code in {400, 401} else "refresh_failed",
                    last_auth_error=message,
                )
            raise ProviderReauthenticationRequiredError(
                f"Unable to authenticate with Schwab right now ({response.status_code}). Please log in again."
            )

        payload = response.json()
        access_token = payload.get("access_token")
        refresh_token = payload.get("refresh_token") or existing_refresh_token
        expires_in = int(payload.get("expires_in", self.DEFAULT_ACCESS_TOKEN_LIFETIME_SECONDS))
        expires_at = self._utcnow() + timedelta(seconds=max(expires_in - self.ACCESS_TOKEN_EXPIRY_BUFFER_SECONDS, 60))
        refresh_expires_at = self._resolve_refresh_expires_at(
            payload=payload,
            existing_tokens=existing_tokens or {},
            refresh_token=refresh_token,
        )

        normalized = {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "expires_at": expires_at.isoformat(),
            "refresh_expires_at": refresh_expires_at,
            "token_type": payload.get("token_type", "Bearer"),
            "scope": payload.get("scope"),
            "auth_state": "connected",
            "last_auth_error": "",
        }
        try:
            self.token_store.save(normalized)
        except OSError as exc:
            self._log_auth_event("token_write_failed", level=logging.WARNING, reason=type(exc).__name__)
            raise ProviderReauthenticationRequiredError(
                "Schwab authentication succeeded, but the token file could not be written."
            ) from exc
        self._log_auth_event("token_write_success")
        return normalized

    def _validate_oauth_config(self) -> None:
        """Validate the required Schwab OAuth settings."""
        missing = [
            name
            for name, value in {
                "SCHWAB_CLIENT_ID": self.config.schwab_client_id,
                "SCHWAB_CLIENT_SECRET": self.config.schwab_client_secret,
                "SCHWAB_REDIRECT_URI": self.config.schwab_redirect_uri,
                "SCHWAB_AUTH_URL": self.config.schwab_auth_url,
                "SCHWAB_TOKEN_URL": self.config.schwab_token_url,
            }.items()
            if not value
        ]
        if missing:
            raise ProviderConfigurationError(
                f"Missing Schwab OAuth configuration: {', '.join(missing)}. Update your environment variables and try again."
            )

    def _build_token_headers(self) -> Dict[str, str]:
        """Build token endpoint headers with a basic authorization header."""
        encoded = base64.b64encode(
            f"{self.config.schwab_client_id}:{self.config.schwab_client_secret}".encode("utf-8")
        ).decode("utf-8")
        return {
            "Authorization": f"Basic {encoded}",
            "Content-Type": "application/x-www-form-urlencoded",
        }

    @staticmethod
    def _parse_datetime(value: Optional[str]) -> Optional[datetime]:
        """Parse an ISO timestamp if present."""
        if not value:
            return None
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None

    def _resolve_refresh_expires_at(
        self,
        *,
        payload: Dict[str, Any],
        existing_tokens: Dict[str, Any],
        refresh_token: Optional[str],
    ) -> str:
        refresh_expiry_raw = payload.get("refresh_token_expires_at")
        if refresh_expiry_raw:
            parsed = self._parse_datetime(str(refresh_expiry_raw))
            if parsed is not None:
                return parsed.isoformat()

        refresh_expires_in = payload.get("refresh_token_expires_in")
        if refresh_expires_in is not None:
            refresh_seconds = int(refresh_expires_in)
            refresh_expires_at = self._utcnow() + timedelta(
                seconds=max(refresh_seconds - self.REFRESH_TOKEN_EXPIRY_BUFFER_SECONDS, 60)
            )
            return refresh_expires_at.isoformat()

        existing_refresh_token = str(existing_tokens.get("refresh_token") or "").strip()
        existing_refresh_expires_at = self._parse_datetime(existing_tokens.get("refresh_expires_at"))
        if refresh_token and existing_refresh_token == refresh_token and existing_refresh_expires_at is not None:
            return existing_refresh_expires_at.isoformat()

        fallback_refresh_expires_at = self._utcnow() + self.DEFAULT_REFRESH_TOKEN_LIFETIME
        return fallback_refresh_expires_at.isoformat()

    def _is_refresh_token_expired(self, tokens: Dict[str, Any]) -> bool:
        refresh_token = str(tokens.get("refresh_token") or "").strip()
        if not refresh_token:
            return True
        refresh_expires_at = self._parse_datetime(tokens.get("refresh_expires_at"))
        if refresh_expires_at is None:
            return False
        return self._utcnow() >= refresh_expires_at

    def _persist_token_state(self, tokens: Dict[str, Any], *, auth_state: str, last_auth_error: str) -> None:
        normalized = dict(tokens)
        normalized["auth_state"] = auth_state
        normalized["last_auth_error"] = last_auth_error
        try:
            self.token_store.save(normalized)
        except OSError:
            self._log_auth_event("token_state_write_failed", level=logging.WARNING, auth_state=auth_state)

    def _log_auth_event(self, event: str, *, level: int = logging.INFO, **details: Any) -> None:
        payload = {
            "env": self.config.app_display_name,
            "port": self.config.app_port,
            "redirect_uri": self.config.schwab_redirect_uri,
            "token_target_path": str(self.token_store.file_path),
        }
        payload.update(details)
        LOGGER.log(level, "Schwab auth %s | %s", event, " | ".join(f"{key}={value}" for key, value in payload.items()))

    def _token_file_exists(self) -> bool:
        file_path = getattr(self.token_store, "file_path", None)
        if isinstance(file_path, Path):
            return file_path.exists()
        if isinstance(file_path, str) and file_path and not file_path.startswith("supabase://"):
            return Path(file_path).exists()
        return False