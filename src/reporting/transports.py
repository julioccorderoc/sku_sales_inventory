"""Where a finished report goes.

A `Report` carries both renderings of the same data — HTML and, when the
destination can show one, an Adaptive Card. Each transport takes the one it can
use, so a report never has to know where it is going.

Three destinations, one interface:

- `TeamsWorkflowWebhookTransport` — POSTs to a Power Automate "When a Teams
  webhook request is received" URL. No Graph permission and no admin consent:
  the URL is the credential. This is the lane the Supply Chain channel uses,
  and its action renders an Adaptive Card.
- `GraphChatTransport` — posts over the delegated Graph grant to a chat. Kept
  because the credentials already carry `ChatMessage.Send`; it cannot address a
  *channel* (that needs `ChannelMessage.Send`, which the app does not have).
  Chat messages accept HTML, so this lane keeps the full tables.
- `FileTransport` — writes the HTML to `output/`. Always available, so a run
  with no live destination still produces something inspectable.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

import requests

from src import settings
from src.reporting.cards import simple_card

logger = logging.getLogger(__name__)

WEBHOOK_TIMEOUT_SECONDS = 30


@dataclass
class Report:
    """One message: a title, its HTML, and optionally its Adaptive Card."""

    title: str
    html: str
    card: dict[str, Any] | None = None


class TeamsTransport(Protocol):
    def send(self, report: Report) -> None: ...


class TransportError(RuntimeError):
    """The message could not be delivered."""


class TeamsWorkflowWebhookTransport:
    """Fire-and-forget POST to a Power Automate Teams workflow URL."""

    def __init__(self, url: str, payload_mode: str = "adaptive_card",
                 timeout: float = WEBHOOK_TIMEOUT_SECONDS):
        self.url = url
        self.payload_mode = payload_mode
        self.timeout = timeout

    def build_payload(self, report: Report) -> dict[str, Any]:
        if self.payload_mode == "adaptive_card":
            return {"type": "message", "attachments": [self._attachment(report)]}
        if self.payload_mode == "text":
            return {"text": report.html}
        return {"message": report.html, "title": report.title}

    @staticmethod
    def _attachment(report: Report) -> dict[str, Any]:
        card = report.card or simple_card(report.title, report.html)
        return {
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": card,
        }

    def send(self, report: Report) -> None:
        if not self.url:
            raise TransportError("TEAMS_WEBHOOK_URL is not set.")

        payload = self.build_payload(report)
        try:
            response = requests.post(self.url, json=payload, timeout=self.timeout)
        except requests.exceptions.RequestException as e:
            raise TransportError(f"Teams webhook unreachable: {e}") from e

        if response.status_code >= 400:
            raise TransportError(
                f"Teams webhook refused the payload (HTTP {response.status_code}): "
                f"{response.text[:300]}"
            )
        logger.info("📨 Teams message delivered (HTTP %s).", response.status_code)


class GraphChatTransport:
    """Post to a Teams chat over the delegated Graph grant.

    Addresses a CHAT, never a channel — `POST /chats/{id}/messages` is the only
    message endpoint the app's delegated scopes can reach. Chat messages take
    HTML, so the full tables survive on this lane.
    """

    def __init__(self, chat_id: str, client: Any):
        self.chat_id = chat_id
        self.client = client

    def send(self, report: Report) -> None:
        if not self.chat_id:
            raise TransportError("TEAMS_CHAT_ID is not set.")
        self.client.post(
            f"/chats/{self.chat_id}/messages",
            json_body={"body": {"contentType": "html", "content": report.html}},
        )
        logger.info("📨 Teams chat message delivered.")


class FileTransport:
    """Write the report next to the CSV/JSON outputs — no network, no secret."""

    def __init__(self, directory: Path | None = None):
        self.directory = directory or settings.OUTPUT_DIR

    def send(self, report: Report) -> None:
        self.directory.mkdir(parents=True, exist_ok=True)
        slug = _slug(report.title) or "report"
        path = self.directory / f"{slug}.html"
        path.write_text(report.html, encoding="utf-8")
        logger.info("📄 Report written to %s (no live Teams destination configured).", path)


class NullTransport:
    """Deliberately drops the message — the OFF position."""

    def send(self, report: Report) -> None:
        logger.info("🔇 Teams transport disabled; report dropped (%s).", report.title)


def build_transport(kind: str | None = None) -> TeamsTransport:
    """Pick a transport from settings, degrading to file output when unset."""
    kind = (kind or settings.TEAMS_TRANSPORT or "file").lower()

    if kind == "webhook":
        if not settings.TEAMS_WEBHOOK_URL:
            logger.warning(
                "⚠️  TEAMS_TRANSPORT=webhook but TEAMS_WEBHOOK_URL is empty — "
                "falling back to writing reports to output/."
            )
            return FileTransport()
        return TeamsWorkflowWebhookTransport(
            settings.TEAMS_WEBHOOK_URL, payload_mode=settings.TEAMS_WEBHOOK_PAYLOAD
        )

    if kind == "graph_chat":
        from src.integrations.graph import GraphClient
        from src.integrations.microsoft_auth import acquire_delegated_token

        return GraphChatTransport(
            settings.TEAMS_CHAT_ID,
            GraphClient(token_provider=acquire_delegated_token),
        )

    if kind == "none":
        return NullTransport()

    if kind != "file":
        logger.warning("⚠️  Unknown TEAMS_TRANSPORT=%r — using file output.", kind)
    return FileTransport()


def _slug(text: str) -> str:
    cleaned = "".join(ch.lower() if ch.isalnum() else "-" for ch in (text or ""))
    return "-".join(part for part in cleaned.split("-") if part)
