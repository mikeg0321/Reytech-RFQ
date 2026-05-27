"""Buyer-supplied template resolution for CCHCS 703C / 704C variants.

Chrome MCP audit 2026-05-27 / 703c+704c: CCHCS sometimes ships
alternate 703C / 704C templates with each bid email rather than
expecting Reytech to use bundled blanks. The Spine renderer for
these forms therefore needs to find the right template at
RENDER time, not import time.

Resolution order (first match wins):
  1. Env override — `SPINE_703C_TEMPLATE_PATH` / `SPINE_704C_TEMPLATE_PATH`.
     Lets operators pin a known-good blank per deployment without
     touching the contract.
  2. Contract `attachment_refs` — looks for a filename containing
     "703c" / "704c" (case-insensitive, accepts "703C", "AMS_703c",
     etc).
  3. Raise SpineFormFillError. No silent fallback to 703b — the
     contract's required_forms list is the operator's intent;
     missing a template should fail loudly.

Mirror of the resolution logic forms_render.py uses inline for the
Format-B render path. Extracted here so the standalone 703c/704c
adapters in FORM_REGISTRY can share it.
"""
from __future__ import annotations

import os
import re
from pathlib import Path
from typing import TYPE_CHECKING, Optional

from src.spine.agency_forms._identity import SpineFormFillError

if TYPE_CHECKING:
    from src.spine.email_contract import EmailContract


def _filename_matches_form_code(name: str, form_code: str) -> bool:
    """Case-insensitive match — accepts variants like 'AMS_703C_blank',
    '703c-template.pdf', etc. Requires the form code as a standalone
    token (not a substring of a different code)."""
    n = (name or "").lower()
    code = form_code.lower()
    # Match the form code surrounded by non-alphanumeric boundaries
    # so '703b' doesn't match '703c' and vice versa.
    return bool(re.search(rf"(^|[^a-z0-9]){re.escape(code)}([^a-z0-9]|$)", n))


def resolve_template_path(
    form_code: str,
    contract: "EmailContract | None",
    env_var_name: str,
) -> str:
    """Resolve a CCHCS 703C / 704C template path.

    Args:
        form_code: '703c' or '704c'. Used to filter attachment_refs.
        contract:  EmailContract with attachment_refs. May be None when
                   the operator triggers a standalone render outside
                   the ingest-driven flow — env-override path is the
                   only resolution then.
        env_var_name: 'SPINE_703C_TEMPLATE_PATH' or
                   'SPINE_704C_TEMPLATE_PATH'. Checked first.

    Returns:
        Absolute filesystem path to a template PDF.

    Raises:
        SpineFormFillError if no template can be resolved.
    """
    # 1. Env override
    env_path = (os.environ.get(env_var_name) or "").strip()
    if env_path:
        p = Path(env_path)
        if p.is_file():
            return str(p.resolve())
        raise SpineFormFillError(
            f"{env_var_name}={env_path!r} is set but the path is not a "
            f"readable file. Either point it at a real {form_code} "
            f"template PDF or unset it to fall back to attachment_refs."
        )

    # 2. Contract attachment_refs
    if contract is not None:
        refs = list(getattr(contract, "attachment_refs", None) or [])
        for ref in refs:
            if not ref:
                continue
            name = os.path.basename(str(ref))
            if _filename_matches_form_code(name, form_code):
                p = Path(ref)
                if p.is_file():
                    return str(p.resolve())
                # The contract pointed at a file that doesn't exist —
                # this is a real config bug, surface it.
                raise SpineFormFillError(
                    f"contract.attachment_refs has {ref!r} as the "
                    f"{form_code} template, but the file is not "
                    f"readable. Either fix the path or set "
                    f"{env_var_name}."
                )

    # 3. No path found
    raise SpineFormFillError(
        f"{form_code.upper()} template not resolvable. Set "
        f"{env_var_name} to a bundled blank, or attach a file with "
        f"'{form_code}' in its name to the buyer's email so it "
        f"appears in contract.attachment_refs. "
        f"contract={'present' if contract else 'None'}."
    )


__all__ = ["resolve_template_path"]
