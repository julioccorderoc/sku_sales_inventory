"""Microsoft Graph OAuth — app-only (files) and delegated (chat) grants.

Ported from the supply_chain_agent app registration `jules-outlook-access-julio-mail`.
Raw httpx, no msal dependency.

Two identities, two grants:

- **App-only** (`acquire_app_token`, client-credentials): carries the
  APPLICATION permissions the tenant admin consented. This is the lane the
  Excel workbooks use — it needs no user, so nothing to re-mint and nothing
  that lapses on a password change.
- **Delegated** (`acquire_delegated_token`, refresh-token): acts as the
  mailbox user. Microsoft caps a delegated refresh token at 90 days from
  ISSUE, so this lane goes dark on a fixed date no matter how often it runs.
  Only the Teams chat transport uses it.
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass

import requests

from src import settings

logger = logging.getLogger(__name__)

LOGIN_AUTHORITY = "https://login.microsoftonline.com"
GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"
DEFAULT_SCOPE = "https://graph.microsoft.com/.default"


class MicrosoftAuthError(RuntimeError):
    """Graph OAuth credentials are missing or the grant was rejected."""

    def __init__(self, message: str, *, remediation: str = ""):
        super().__init__(message)
        self.remediation = remediation


@dataclass
class _CachedToken:
    access_token: str
    expires_at: float
    kind: str = ""

    def is_valid(self, skew_seconds: int = 120) -> bool:
        return bool(self.access_token) and time.time() < (self.expires_at - skew_seconds)


@dataclass
class _TokenCache:
    app: _CachedToken | None = None
    delegated: _CachedToken | None = None


_cache = _TokenCache()


def _token_endpoint() -> str:
    return f"{LOGIN_AUTHORITY}/{settings.MS_TENANT_ID}/oauth2/v2.0/token"


def _post_token_request(payload: dict[str, str]) -> dict:
    try:
        response = requests.post(_token_endpoint(), data=payload, timeout=30)
    except requests.exceptions.RequestException as e:
        raise MicrosoftAuthError(f"Microsoft token endpoint unreachable: {e}") from e

    if response.status_code != 200:
        raise MicrosoftAuthError(
            f"Microsoft token request failed ({response.status_code}): {response.text[:400]}",
            remediation=_remediation_for(response.text),
        )
    return response.json()


def _remediation_for(body: str) -> str:
    """Turn an opaque OAuth error body into the half an operator can act on."""
    try:
        parsed = json.loads(body)
    except (TypeError, ValueError):
        parsed = {}
    error = str(parsed.get("error") or "")
    if error == "invalid_grant":
        return (
            "The delegated refresh token is no longer redeemable (Microsoft's hard "
            "90-day cap). Re-mint it with scripts/bootstrap_graph_token.py from the "
            "supply_chain_agent project, then update MS_REFRESH_TOKEN here."
        )
    if error in ("invalid_client", "unauthorized_client"):
        return (
            "MS_CLIENT_SECRET is expired, rotated or wrong. Rotate it in Entra -> "
            "Certificates & secrets, then update MS_CLIENT_SECRET here."
        )
    return (
        "Check MS_TENANT_ID / MS_CLIENT_ID / MS_CLIENT_SECRET for whitespace or typos."
    )


def _require(*values: tuple[str, str]) -> None:
    missing = [name for name, value in values if not value]
    if missing:
        raise MicrosoftAuthError(
            f"Microsoft Graph credentials incomplete — set {', '.join(missing)} in .env"
        )


def acquire_app_token(force_refresh: bool = False) -> str:
    """Client-credentials grant for the app identity (no user)."""
    if not force_refresh and _cache.app and _cache.app.is_valid():
        return _cache.app.access_token

    _require(
        ("MS_TENANT_ID", settings.MS_TENANT_ID),
        ("MS_CLIENT_ID", settings.MS_CLIENT_ID),
        ("MS_CLIENT_SECRET", settings.MS_CLIENT_SECRET),
    )

    body = _post_token_request(
        {
            "grant_type": "client_credentials",
            "client_id": settings.MS_CLIENT_ID,
            "client_secret": settings.MS_CLIENT_SECRET,
            "scope": DEFAULT_SCOPE,
        }
    )
    token = _CachedToken(
        access_token=body["access_token"],
        expires_at=time.time() + int(body.get("expires_in", 3600)),
        kind="app",
    )
    _cache.app = token
    logger.debug("🔑 Acquired Microsoft Graph app-only token (expires in %ss).", body.get("expires_in"))
    return token.access_token


def acquire_delegated_token(force_refresh: bool = False) -> str:
    """Refresh-token grant acting as the mailbox user."""
    if not force_refresh and _cache.delegated and _cache.delegated.is_valid():
        return _cache.delegated.access_token

    _require(
        ("MS_TENANT_ID", settings.MS_TENANT_ID),
        ("MS_CLIENT_ID", settings.MS_CLIENT_ID),
        ("MS_CLIENT_SECRET", settings.MS_CLIENT_SECRET),
        ("MS_REFRESH_TOKEN", settings.MS_REFRESH_TOKEN),
    )

    body = _post_token_request(
        {
            "grant_type": "refresh_token",
            "client_id": settings.MS_CLIENT_ID,
            "client_secret": settings.MS_CLIENT_SECRET,
            "refresh_token": settings.MS_REFRESH_TOKEN,
            "scope": DEFAULT_SCOPE,
        }
    )
    token = _CachedToken(
        access_token=body["access_token"],
        expires_at=time.time() + int(body.get("expires_in", 3600)),
        kind="delegated",
    )
    _cache.delegated = token
    logger.debug("🔑 Acquired Microsoft Graph delegated token (expires in %ss).", body.get("expires_in"))
    return token.access_token


def credentials_available() -> bool:
    """True when the app-only lane has everything it needs to talk to Graph."""
    return all(
        [
            settings.MS_TENANT_ID,
            settings.MS_CLIENT_ID,
            settings.MS_CLIENT_SECRET,
        ]
    )


def reset_cache() -> None:
    """Drop cached tokens — test hook."""
    _cache.app = None
    _cache.delegated = None
