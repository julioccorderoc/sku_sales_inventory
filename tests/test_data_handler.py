"""
Tests for post_to_webhook retry logic in src/data_handler.py.

Uses mocked requests.post and time.sleep to verify:
- Success on first attempt → single call, no sleep
- 5xx response → retries with exponential backoff, succeeds on retry
- 5xx exhausting all retries → logs error, does not raise
- ConnectionError → retries with exponential backoff
- 4xx response → no retry, logs error immediately
- Exponential backoff delays follow WEBHOOK_RETRY_BACKOFF ** attempt
- No-op when WEBHOOK_URL is unset
"""
import pytest
from unittest.mock import patch, MagicMock
import requests

from src import data_handler


@pytest.fixture
def dummy_records():
    record = MagicMock()
    record.model_dump.return_value = {"id": "test_id", "sku": "1001"}
    return [record]


@pytest.fixture
def dummy_metadata():
    return {"date": "2026-03-03", "totalRecords": 1}


def _ok_response():
    r = MagicMock()
    r.status_code = 200
    r.raise_for_status.return_value = None
    return r


def _5xx_response(code=500):
    r = MagicMock()
    r.status_code = code
    return r


def _4xx_response(code=422):
    r = MagicMock()
    r.status_code = code
    r.raise_for_status.side_effect = requests.exceptions.HTTPError(response=r)
    return r


PATCHES = {
    "url": ("src.data_handler.settings.WEBHOOK_URL", "https://example.com/webhook"),
    "retries": ("src.data_handler.settings.WEBHOOK_MAX_RETRIES", 3),
    "backoff": ("src.data_handler.settings.WEBHOOK_RETRY_BACKOFF", 2.0),
}


class TestPostToWebhookRetry:
    def _run(self, mock_post_side_effect, dummy_records, dummy_metadata):
        """Helper: run post_to_webhook with patched settings and return (mock_post, mock_sleep)."""
        with patch("src.data_handler.requests.post", side_effect=mock_post_side_effect) as mock_post, \
             patch("src.data_handler.time.sleep") as mock_sleep, \
             patch(*PATCHES["url"]), \
             patch(*PATCHES["retries"]), \
             patch(*PATCHES["backoff"]):
            data_handler.post_to_webhook(dummy_records, dummy_metadata)
        return mock_post, mock_sleep

    def test_success_on_first_attempt(self, dummy_records, dummy_metadata):
        mock_post, mock_sleep = self._run([_ok_response()], dummy_records, dummy_metadata)
        mock_post.assert_called_once()
        mock_sleep.assert_not_called()

    def test_5xx_retries_then_succeeds(self, dummy_records, dummy_metadata):
        mock_post, mock_sleep = self._run(
            [_5xx_response(503), _ok_response()], dummy_records, dummy_metadata
        )
        assert mock_post.call_count == 2
        mock_sleep.assert_called_once_with(2.0)  # backoff ** 1

    def test_5xx_exhausts_all_retries(self, dummy_records, dummy_metadata):
        mock_post, mock_sleep = self._run(
            [_5xx_response()] * 3, dummy_records, dummy_metadata
        )
        assert mock_post.call_count == 3

    def test_connection_error_retries_then_succeeds(self, dummy_records, dummy_metadata):
        mock_post, mock_sleep = self._run(
            [requests.exceptions.ConnectionError(), _ok_response()],
            dummy_records, dummy_metadata,
        )
        assert mock_post.call_count == 2
        mock_sleep.assert_called_once_with(2.0)

    def test_connection_error_exhausts_all_retries(self, dummy_records, dummy_metadata):
        mock_post, mock_sleep = self._run(
            [requests.exceptions.ConnectionError()] * 3, dummy_records, dummy_metadata
        )
        assert mock_post.call_count == 3

    def test_no_retry_on_4xx(self, dummy_records, dummy_metadata):
        mock_post, mock_sleep = self._run([_4xx_response()], dummy_records, dummy_metadata)
        mock_post.assert_called_once()
        mock_sleep.assert_not_called()

    def test_exponential_backoff_delays(self, dummy_records, dummy_metadata):
        """All 3 attempts fail → sleeps after attempt 1 (2s) and attempt 2 (4s), not after 3."""
        mock_post, mock_sleep = self._run(
            [_5xx_response()] * 3, dummy_records, dummy_metadata
        )
        assert mock_sleep.call_count == 2
        calls = [c.args[0] for c in mock_sleep.call_args_list]
        assert calls == [2.0, 4.0]  # 2^1, 2^2

    def test_skips_when_no_webhook_url(self, dummy_records, dummy_metadata):
        with patch("src.data_handler.requests.post") as mock_post, \
             patch("src.data_handler.settings.WEBHOOK_URL", None):
            data_handler.post_to_webhook(dummy_records, dummy_metadata)
        mock_post.assert_not_called()

    def test_does_not_raise_after_exhausted_retries(self, dummy_records, dummy_metadata):
        """Pipeline must not surface an unhandled exception after all retries fail."""
        with patch("src.data_handler.requests.post", return_value=_5xx_response()), \
             patch("src.data_handler.time.sleep"), \
             patch(*PATCHES["url"]), \
             patch(*PATCHES["retries"]), \
             patch(*PATCHES["backoff"]):
            data_handler.post_to_webhook(dummy_records, dummy_metadata)  # must not raise
