"""Substrate test: FORM_REGISTRY → consumer-driven-contract gate.

Closes the "operator generated a package and didn't notice a required
form was missing" class. Three properties enforced here:

1. Every CCHCS default required_form has a registered renderer
   (BACKWARD: contract demand → renderer provided).
2. Every registered renderer key is a valid FormCode literal
   (FORWARD: provider supply → schema-validated).
3. Every registered renderer accepts the uniform Renderer signature
   (quote, identity=None, *, today=None, flatten=True) -> bytes.

Pact / Schemathesis-style consumer-driven contract testing applied at
the build-time test boundary, not runtime. A drift between Spine demand
and Spine supply fails CI.
"""
from __future__ import annotations

import inspect

import pytest

from src.spine import ALL_FORM_CODES, CCHCS_DEFAULT_REQUIRED_FORMS
from src.spine.agency_forms import FORM_REGISTRY


# ── Backward: CCHCS demand → Spine supply ────────────────────────────


def test_cchcs_default_set_fully_registered():
    """Every form in CCHCS_DEFAULT_REQUIRED_FORMS must have a renderer.

    The CCHCS default is the empirical truth — these 4 forms ship on
    every CCHCS bid. If any is missing from FORM_REGISTRY, no CCHCS
    bid can succeed via /package. This is the production gate.
    """
    for code in CCHCS_DEFAULT_REQUIRED_FORMS:
        assert code in FORM_REGISTRY, (
            f"CCHCS default form {code!r} has no registered renderer. "
            f"Register it in src/spine/agency_forms/__init__.py before "
            f"shipping any CCHCS bid through /package."
        )


# ── Forward: provider supply → schema-validated ──────────────────────


def test_every_registered_form_code_is_in_form_code_literal():
    """No orphan keys in FORM_REGISTRY.

    A key here that isn't in the FormCode literal is dead code — the
    contract can never legally name it, so the renderer can never be
    invoked through the gate. This test catches the typo class.
    """
    for code in FORM_REGISTRY:
        assert code in ALL_FORM_CODES, (
            f"FORM_REGISTRY key {code!r} is not a valid FormCode literal. "
            f"Either add it to src/spine/email_contract.py:FormCode or "
            f"remove from FORM_REGISTRY."
        )


# ── Signature uniformity ─────────────────────────────────────────────


def test_every_registered_renderer_accepts_uniform_signature():
    """All renderers must be callable as (quote, identity, *, today, flatten).

    The /package endpoint and any future bundler iterate FORM_REGISTRY
    with a uniform call shape. A renderer that doesn't accept these
    parameters breaks the gate.
    """
    for code, fn in FORM_REGISTRY.items():
        sig = inspect.signature(fn)
        params = sig.parameters
        # Required: a positional `quote` (any name, but first).
        positional = [
            p for p in params.values()
            if p.kind in (
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            )
        ]
        assert len(positional) >= 1, (
            f"FORM_REGISTRY[{code!r}] must accept a positional `quote` "
            f"arg; signature is {sig}"
        )
        # `identity` may be positional-or-keyword OR keyword-only;
        # either way the registry binds it positionally as 2nd arg.
        # `today` and `flatten` must be accepted as kwargs (the adapter
        # for Quote PDF accepts-and-ignores them).
        kwargs_acceptable = {p.name for p in params.values()
                             if p.kind in (
                                 inspect.Parameter.KEYWORD_ONLY,
                                 inspect.Parameter.POSITIONAL_OR_KEYWORD,
                             )}
        for required_kw in ("today", "flatten"):
            assert required_kw in kwargs_acceptable, (
                f"FORM_REGISTRY[{code!r}] must accept `{required_kw}` "
                f"as kwarg for uniform calling; signature is {sig}"
            )


# ── Self-consistency with the FormCode literal as a whole ────────────


@pytest.mark.parametrize("code", ALL_FORM_CODES)
def test_form_code_in_literal_either_registered_or_explicitly_deferred(code):
    """Every FormCode literal member is either:

    a. registered in FORM_REGISTRY (Spine renderer exists), OR
    b. listed below as known-deferred (renderer not yet ported from
       legacy or not yet needed).

    This forces an explicit decision when a new FormCode is added:
    register a renderer, or document why it's not yet registered.
    Silent "I added the literal but forgot the renderer" is the gap
    this test closes.
    """
    # All known FormCode literals were ported through 2026-05-27 batch.
    # If you add a new FormCode and can't register the renderer yet,
    # add it here with a comment explaining why.
    KNOWN_DEFERRED: set = set()
    if code in FORM_REGISTRY:
        return  # registered — OK
    assert code in KNOWN_DEFERRED, (
        f"FormCode {code!r} is neither in FORM_REGISTRY nor in the "
        f"KNOWN_DEFERRED set in this test. Either register a renderer "
        f"or add to KNOWN_DEFERRED with a comment explaining why."
    )
