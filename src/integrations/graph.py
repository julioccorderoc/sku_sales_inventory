"""Thin Microsoft Graph HTTP client.

One place that knows how to: attach a bearer token, retry the two failure
modes Graph actually produces (429 with `Retry-After`, transient 5xx), and
raise an error a human can act on. The Excel and Teams lanes both ride it.

No pagination is done implicitly — `get_all` walks `@odata.nextLink` when a
caller asks for it, because every other read in this project is bounded.
"""
from __future__ import annotations

import logging
import time
from typing import Any, Callable

import requests

from src.integrations.microsoft_auth import GRAPH_BASE_URL

logger = logging.getLogger(__name__)

MAX_ATTEMPTS = 4
BACKOFF_BASE_SECONDS = 2.0
MAX_RETRY_SLEEP_SECONDS = 60.0


class GraphError(RuntimeError):
    """A Graph call failed after retries, or was refused outright."""

    def __init__(self, message: str, *, status_code: int | None = None, body: str = ""):
        super().__init__(message)
        self.status_code = status_code
        self.body = body


class GraphClient:
    """Minimal authenticated Graph client.

    `token_provider` is called lazily on every request so a cached token can be
    swapped underneath without rebuilding the client.
    """

    def __init__(
        self,
        token_provider: Callable[[], str],
        *,
        base_url: str = GRAPH_BASE_URL,
        timeout: float = 60.0,
    ):
        self._token_provider = token_provider
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout

    # -- internals ---------------------------------------------------------

    def _request(
        self,
        method: str,
        path: str,
        *,
        json_body: Any = None,
        params: dict[str, Any] | None = None,
    ) -> Any:
        url = path if path.startswith("http") else f"{self._base_url}{path}"

        for attempt in range(1, MAX_ATTEMPTS + 1):
            response = requests.request(
                method,
                url,
                headers={
                    "Authorization": f"Bearer {self._token_provider()}",
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                },
                json=json_body,
                params=params,
                timeout=self._timeout,
            )

            if response.status_code == 429 or response.status_code >= 500:
                if attempt < MAX_ATTEMPTS:
                    delay = self._retry_delay(response, attempt)
                    logger.warning(
                        "⚠️  Graph %s %s → HTTP %s. Retry %s/%s in %.1fs.",
                        method, _short(path), response.status_code, attempt, MAX_ATTEMPTS, delay,
                    )
                    time.sleep(delay)
                    continue
                raise GraphError(
                    f"Graph {method} {_short(path)} failed after {MAX_ATTEMPTS} attempts "
                    f"(HTTP {response.status_code})",
                    status_code=response.status_code,
                    body=response.text[:800],
                )

            if response.status_code >= 400:
                raise GraphError(
                    f"Graph {method} {_short(path)} → HTTP {response.status_code}: "
                    f"{_error_message(response)}",
                    status_code=response.status_code,
                    body=response.text[:800],
                )

            if not response.content:
                return None
            try:
                return response.json()
            except ValueError:
                return None

        raise GraphError(f"Graph {method} {_short(path)} exhausted retries")

    @staticmethod
    def _retry_delay(response: requests.Response, attempt: int) -> float:
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                return min(float(retry_after), MAX_RETRY_SLEEP_SECONDS)
            except ValueError:
                pass
        return min(BACKOFF_BASE_SECONDS ** attempt, MAX_RETRY_SLEEP_SECONDS)

    # -- public surface ----------------------------------------------------

    def get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        return self._request("GET", path, params=params)

    def get_optional(self, path: str, params: dict[str, Any] | None = None) -> Any | None:
        """GET that treats 404 as `None` — "the item is not there" is an answer."""
        try:
            return self._request("GET", path, params=params)
        except GraphError as e:
            if e.status_code == 404:
                return None
            raise

    def post(self, path: str, json_body: Any = None) -> Any:
        return self._request("POST", path, json_body=json_body)

    def patch(self, path: str, json_body: Any = None) -> Any:
        return self._request("PATCH", path, json_body=json_body)

    def get_all(
        self, path: str, params: dict[str, Any] | None = None, max_items: int = 10_000
    ) -> list[dict]:
        """Walk `@odata.nextLink` and return every page's `value` flattened."""
        items: list[dict] = []
        page = self._request("GET", path, params=params)
        while page and len(items) < max_items:
            items.extend(page.get("value", []) or [])
            next_link = page.get("@odata.nextLink")
            if not next_link:
                break
            page = self._request("GET", next_link)
        return items[:max_items]


def _short(path: str) -> str:
    """Trim a Graph path for logs — enough to identify, not enough to flood."""
    cleaned = path.replace(GRAPH_BASE_URL, "")
    return cleaned if len(cleaned) <= 120 else f"{cleaned[:117]}..."


def _error_message(response: requests.Response) -> str:
    try:
        payload = response.json()
    except ValueError:
        return response.text[:300]
    error = payload.get("error") or {}
    if isinstance(error, dict):
        code = error.get("code", "")
        message = error.get("message", "")
        return f"{code} {message}".strip() or response.text[:300]
    return str(error)[:300]
