"""Tests for the webhook dispatcher."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from unittest.mock import patch, MagicMock
import time


def test_fire_webhook_sends_post():
    """fire_webhook sends POST to the configured URL with correct payload."""
    with patch.dict(os.environ, {"WEBHOOK_RFQ_CREATED_URL": "https://example.com/hook"}):
        with patch("src.core.webhooks.urllib.request.urlopen") as mock_open:
            from src.core.webhooks import fire_webhook
            fire_webhook("rfq.created", {
                "rfq_id": "abc123",
                "agency": "CDCR",
                "item_count": 5,
            })
            # Webhook fires in a thread — give it a moment
            time.sleep(0.5)
            assert mock_open.called
            call_args = mock_open.call_args
            req = call_args[0][0]
            assert req.full_url == "https://example.com/hook"
            import json
            body = json.loads(req.data.decode("utf-8"))
            assert body["event"] == "rfq.created"
            assert body["rfq_id"] == "abc123"
            assert body["agency"] == "CDCR"


def test_fire_webhook_no_raise_when_unconfigured():
    """fire_webhook does NOT raise if no URL is configured."""
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("WEBHOOK_TEST_URL", None)
        from src.core.webhooks import fire_webhook
        # Should not raise
        fire_webhook("test", {"msg": "hello"})
