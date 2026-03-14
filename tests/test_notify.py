"""Tests for SMS notification on new RFQ."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from unittest.mock import patch, MagicMock


def test_sms_called_with_correct_fields():
    """When Twilio is configured, SMS is sent with correct RFQ fields."""
    with patch.dict(os.environ, {
        "TWILIO_ACCOUNT_SID": "fake_sid",
        "TWILIO_AUTH_TOKEN": "fake_token",
        "TWILIO_PHONE_NUMBER": "+10000000000",
        "NOTIFY_PHONE": "+11111111111",
        "NOTIFY_SMS": "true",
    }):
        # Reload to pick up env vars
        import importlib
        import src.agents.notify_agent as na
        importlib.reload(na)

        mock_client = MagicMock()
        with patch("src.agents.notify_agent.Client", return_value=mock_client) as mock_cls:
            na.notify_new_rfq_sms({
                "id": "abc123",
                "solicitation_number": "SOL-999",
                "agency": "CDCR",
                "items": [{"d": "Gloves"}, {"d": "Masks"}],
                "due_date": "2026-04-01",
            })
            mock_client.messages.create.assert_called_once()
            call_kwargs = mock_client.messages.create.call_args
            body = call_kwargs.kwargs.get("body") or call_kwargs[1].get("body", "")
            assert "SOL-999" in body
            assert "CDCR" in body
            assert "2 items" in body


def test_sms_does_not_raise_when_unconfigured():
    """When Twilio is NOT configured, function logs and returns without error."""
    with patch.dict(os.environ, {
        "TWILIO_ACCOUNT_SID": "",
        "TWILIO_AUTH_TOKEN": "",
        "TWILIO_PHONE_NUMBER": "",
        "NOTIFY_PHONE": "",
    }, clear=False):
        import importlib
        import src.agents.notify_agent as na
        importlib.reload(na)

        # Should not raise
        na.notify_new_rfq_sms({"id": "test", "solicitation_number": "X"})
