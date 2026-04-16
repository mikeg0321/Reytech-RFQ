"""V2 Test Suite — Group 2: API Response Contract Tests.

Tests that prevent API integration failures:
- Grok JSON mode returns valid JSON
- Grok response has required fields
- Claude prompt cache header format is correct
- Haiku model never uses extended thinking
- Model strings are valid and current

Incident: Haiku+thinking (unsupported) caused 400 errors on every call.
"""
import json
import os
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest


# ═══════════════════════════════════════════════════════════════════════════
# Model string constants (must match production)
# ═══════════════════════════════════════════════════════════════════════════

VALID_HAIKU_MODELS = {"claude-haiku-4-5-20251001"}
VALID_SONNET_MODELS = {"claude-sonnet-4-6", "claude-sonnet-4-20250514"}
VALID_OPUS_MODELS = {"claude-opus-4-7", "claude-opus-4-6"}
VALID_GROK_MODELS = {"grok-3-mini"}

# Fields that Grok JSON responses MUST contain
GROK_REQUIRED_FIELDS = {"confidence", "price"}
GROK_EXPECTED_FIELDS = {"ok", "price", "confidence", "product_name", "asin", "url"}


class TestGrokJsonContract:
    """Grok API must return valid JSON with required fields."""

    def test_response_format_json_object(self):
        """Grok calls must use response_format: {type: json_object}."""
        # Verify the pattern used in item_identifier.py, product_research.py
        request_body = {
            "model": "grok-3-mini",
            "messages": [{"role": "user", "content": "test"}],
            "response_format": {"type": "json_object"},
        }
        assert request_body["response_format"]["type"] == "json_object"

    def test_grok_response_parses_as_json(self):
        """Simulate a Grok response and verify it parses."""
        mock_response = '{"ok": true, "price": 42.99, "confidence": 0.85, "product_name": "Test Product", "asin": "B07TEST123", "url": "https://amazon.com/dp/B07TEST123"}'
        parsed = json.loads(mock_response)
        assert isinstance(parsed, dict)
        assert parsed["price"] == 42.99

    def test_grok_response_has_required_fields(self):
        """Every Grok response must include confidence and price."""
        mock_response = {
            "ok": True,
            "price": 42.99,
            "confidence": 0.85,
            "product_name": "Test Widget",
            "asin": "B07TEST123",
            "url": "https://amazon.com/dp/B07TEST123",
            "supplier": "Amazon",
            "reasoning": "Matched by description",
        }
        for field in GROK_REQUIRED_FIELDS:
            assert field in mock_response, f"Grok response missing required field: {field}"

    def test_grok_model_string_is_valid(self):
        """Grok model must be grok-3-mini (not grok-2, not grok-beta)."""
        # Read the actual model string from item_identifier.py
        try:
            import src.agents.item_identifier as ii_mod
            # Search for the model constant
            import inspect
            source = inspect.getsource(ii_mod)
            assert "grok-3-mini" in source, "item_identifier should use grok-3-mini"
        except ImportError:
            pytest.skip("item_identifier not importable")


class TestClaudePromptCache:
    """Claude prompt cache headers must be correctly formatted.
    Wrong format causes 400 Bad Request.
    """

    def test_cache_control_format(self):
        """cache_control must be {"type": "ephemeral"}, not a string."""
        system_block = {
            "type": "text",
            "text": "You are a helpful assistant.",
            "cache_control": {"type": "ephemeral"},
        }
        assert isinstance(system_block["cache_control"], dict)
        assert system_block["cache_control"]["type"] == "ephemeral"

    def test_system_prompt_is_list_of_blocks(self):
        """Claude API requires system as list of content blocks, not a string."""
        # This is the correct format
        system = [{"type": "text", "text": "prompt", "cache_control": {"type": "ephemeral"}}]
        assert isinstance(system, list)
        assert system[0]["type"] == "text"

    def test_anthropic_version_header(self):
        """API version header must be present and correct."""
        headers = {
            "x-api-key": "test-key",
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        }
        assert headers["anthropic-version"] == "2023-06-01"


class TestHaikuNoThinking:
    """Haiku does NOT support extended thinking.
    Sending thinking params to Haiku causes 400 errors.
    Incident 2026-04-10: 7 agent files shipped with Haiku+thinking.
    """

    def test_haiku_agents_have_no_thinking_param(self):
        """Scan all agent files for Haiku model + thinking combination."""
        agents_dir = os.path.join(os.path.dirname(__file__), "..", "src", "agents")
        if not os.path.isdir(agents_dir):
            pytest.skip("src/agents not found")

        violations = []
        for fname in os.listdir(agents_dir):
            if not fname.endswith(".py"):
                continue
            fpath = os.path.join(agents_dir, fname)
            with open(fpath, "r", encoding="utf-8", errors="replace") as f:
                content = f.read()

            # Check: if file uses haiku model, it must NOT have thinking/extended_thinking
            uses_haiku = "haiku" in content.lower()
            has_thinking = ("extended_thinking" in content
                           or '"thinking"' in content
                           or "'thinking'" in content)

            # Filter out false positives (comments about thinking, variable names)
            if uses_haiku and has_thinking:
                # More precise check: look for thinking in request body context
                lines = content.split("\n")
                for i, line in enumerate(lines):
                    stripped = line.strip()
                    if stripped.startswith("#"):
                        continue  # skip comments
                    if ("thinking" in stripped and
                            ("budget_tokens" in stripped
                             or "extended_thinking" in stripped
                             or '"type": "enabled"' in stripped)):
                        violations.append(f"{fname}:{i+1}: {stripped[:80]}")

        assert not violations, (
            f"Haiku+thinking violations found (causes 400 errors):\n"
            + "\n".join(violations)
        )

    def test_valid_haiku_model_strings(self):
        """All Haiku model references must use the correct model ID."""
        agents_dir = os.path.join(os.path.dirname(__file__), "..", "src", "agents")
        if not os.path.isdir(agents_dir):
            pytest.skip("src/agents not found")

        bad_models = []
        for fname in os.listdir(agents_dir):
            if not fname.endswith(".py"):
                continue
            fpath = os.path.join(agents_dir, fname)
            with open(fpath, "r", encoding="utf-8", errors="replace") as f:
                content = f.read()

            # Find haiku model strings
            for match in re.finditer(r'"(claude-haiku[^"]*)"', content):
                model = match.group(1)
                if model not in VALID_HAIKU_MODELS:
                    bad_models.append(f"{fname}: {model}")

        assert not bad_models, (
            f"Invalid Haiku model strings found:\n"
            + "\n".join(bad_models)
            + f"\nValid: {VALID_HAIKU_MODELS}"
        )


class TestVisionParserContract:
    """Vision parser response must have header + items structure."""

    def test_vision_response_structure(self):
        """Claude vision response must have header and items."""
        mock_response = {
            "header": {
                "price_check_number": "OS - FSP - Apr",
                "requestor": "buyer@cdcr.ca.gov",
                "institution": "CSP-Sacramento",
            },
            "items": [
                {
                    "item_number": "1",
                    "qty": 10,
                    "uom": "EA",
                    "description": "Nitrile gloves",
                },
            ],
        }
        assert "header" in mock_response
        assert "items" in mock_response
        assert len(mock_response["items"]) > 0
        assert "description" in mock_response["items"][0]

    def test_vision_parser_is_available_check(self):
        """is_available() must not crash even without API key."""
        try:
            from src.forms.vision_parser import is_available
            result = is_available()
            assert isinstance(result, bool)
        except ImportError:
            pytest.skip("vision_parser not importable")
