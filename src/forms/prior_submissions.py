"""prior_submissions — DB-backed store of blessed prior PDFs for
mirror-fill substrate.

PR mr-wolf #4b. Operational completion of PR #4 (`mirror_fill.py` +
`form_registry.py`). Before this module, `fill_703a` had to read a
prior 703B from `data/prior_submissions/703b/` on disk — but the
Railway data dir resets on deploy, so the path was fragile + required
operator hand-placement of files. This module replaces that with a
durable SQLite-backed store that survives redeploys (the DB lives on
the /data persistent volume).

Flow:

  1. Schema — `prior_submissions` table, mirrors `rfq_files`'s pattern
     (single-table BLOB store, no FK constraints, idx by lookup key).
  2. `capture(form_id, pdf_bytes, ...)` — called from the Mark Sent
     hook on RFQ → sent transitions. Stores a copy of each generated
     PDF keyed by `(form_id, agency_key)`.
  3. `latest_for(form_id, *, agency_key=None) -> bytes | None` —
     called by `fill_703a` to look up the most-recent captured prior.
     Falls back to any-agency match when no agency-specific prior
     exists.

Filesystem fallback (`data/prior_submissions/<form_id>/*.pdf`) is
kept for backwards-compat with PR #4's initial wire — operators who
hand-placed PDFs there before this PR shipped don't lose those.
`latest_for` tries DB first, FS second.
"""
from __future__ import annotations

import io
import logging
import os
import uuid
from datetime import datetime
from typing import Optional

log = logging.getLogger("reytech.prior_submissions")


# ── Schema init ─────────────────────────────────────────────────────


def _init_prior_submissions_table() -> None:
    """Create the `prior_submissions` table on import. Mirrors the
    `_init_rfq_files_table` pattern in `src/api/dashboard.py` — idempotent
    CREATE IF NOT EXISTS, indexes via separate statements, fails
    silently on schema-init errors (boot must never block).

    Schema:
      id           — UUID (TEXT primary key)
      form_id      — "703a" / "703b" / "704b" / "bidpkg" / "ams708" / etc.
                     (matches src.forms.form_registry.all_form_ids())
      agency_key   — "cchcs" / "calvet" / "dgs" / "" (case-insensitive)
      pdf_data     — BLOB; the captured PDF bytes
      filename     — original filename (e.g. "10846357_703B_Reytech.pdf")
      source_rfq_id      — rid of the RFQ whose generate-package produced this
      source_quote_number — Reytech quote # (e.g., "R26Q42")
      captured_at  — ISO datetime
      blessed      — INT 0/1; operator-marked as canonical. `latest_for`
                     prefers blessed=1 over blessed=0 when both exist.
    """
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS prior_submissions (
                    id                  TEXT PRIMARY KEY,
                    form_id             TEXT NOT NULL,
                    agency_key          TEXT DEFAULT '',
                    pdf_data            BLOB NOT NULL,
                    filename            TEXT NOT NULL,
                    source_rfq_id       TEXT DEFAULT '',
                    source_quote_number TEXT DEFAULT '',
                    captured_at         TEXT NOT NULL,
                    blessed             INTEGER DEFAULT 0
                )
            """)
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_prior_submissions_form_agency "
                "ON prior_submissions(form_id, agency_key)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_prior_submissions_captured "
                "ON prior_submissions(captured_at)"
            )
    except Exception as e:
        log.debug("prior_submissions table init suppressed: %s", e)


# ── Public API ──────────────────────────────────────────────────────


def capture(
    form_id: str,
    pdf_data,
    *,
    agency_key: str = "",
    source_rfq_id: str = "",
    source_quote_number: str = "",
    filename: str = "",
    blessed: bool = False,
) -> str:
    """Store a PDF as a prior submission for future mirror-fill.

    `pdf_data` — bytes OR a file-path / path-like (we'll read it).

    Returns the new row's id (UUID-prefixed `ps_<hex>`). Returns ""
    on failure — never raises (capture is fire-and-forget from the
    Mark Sent hook; a DB error must not block status flips).

    Use `blessed=True` only for operator-explicit "this is the
    canonical prior for this form" actions; the auto-capture path
    leaves it 0 and lets `latest_for` rank by `captured_at` DESC.
    """
    if not form_id or not pdf_data:
        return ""
    try:
        if hasattr(pdf_data, "read"):
            blob = pdf_data.read()
        elif isinstance(pdf_data, bytes):
            blob = pdf_data
        elif isinstance(pdf_data, (str, bytes)) or hasattr(pdf_data, "__fspath__"):
            with open(pdf_data, "rb") as f:
                blob = f.read()
        else:
            log.warning("prior_submissions.capture: unsupported pdf_data type %r", type(pdf_data))
            return ""
    except Exception as e:
        log.warning("prior_submissions.capture: PDF read failed: %s", e)
        return ""
    if not blob:
        return ""

    row_id = f"ps_{uuid.uuid4().hex[:12]}"
    now = datetime.now().isoformat()
    canonical_form = str(form_id).strip()
    canonical_agency = (agency_key or "").strip().lower()
    canonical_filename = filename or f"{canonical_form}_{row_id}.pdf"

    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute(
                "INSERT INTO prior_submissions "
                "(id, form_id, agency_key, pdf_data, filename, "
                " source_rfq_id, source_quote_number, captured_at, blessed) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    row_id, canonical_form, canonical_agency, blob,
                    canonical_filename, source_rfq_id, source_quote_number,
                    now, 1 if blessed else 0,
                ),
            )
        log.info(
            "prior_submissions.capture: form_id=%s agency=%s bytes=%d source_rfq=%s",
            canonical_form, canonical_agency or "(any)",
            len(blob), source_rfq_id[:12] if source_rfq_id else "(none)",
        )
        return row_id
    except Exception as e:
        log.warning("prior_submissions.capture INSERT failed: %s", e)
        return ""


def latest_for(form_id: str, *, agency_key: Optional[str] = None) -> Optional[bytes]:
    """Return the bytes of the most-recent prior submission for
    `form_id`. Lookup order:

      1. Blessed prior with matching `agency_key` (exact agency match)
      2. Latest captured prior with matching `agency_key`
      3. Blessed prior with ANY agency (when caller didn't pin one)
      4. Latest captured prior with ANY agency
      5. Filesystem fallback at `data/prior_submissions/<form_id>/`
         (PR #4 left-over from before this module; supports operators
         who hand-placed PDFs prior to auto-capture going live)
      6. None — caller falls back to its own non-prior behavior

    `agency_key` — case-insensitive. Pass None to skip agency match
    entirely (use the global pool); pass "" to match priors that have
    no agency stamp.
    """
    if not form_id:
        return None
    canonical_form = str(form_id).strip()
    canonical_agency = (agency_key or "").strip().lower() if agency_key is not None else None

    try:
        from src.core.db import get_db
    except Exception:
        return _latest_for_from_filesystem(canonical_form)

    try:
        with get_db() as conn:
            # Tier 1+2: agency match. Skip when caller passed None
            # (global pool requested).
            if canonical_agency is not None:
                row = conn.execute(
                    "SELECT pdf_data FROM prior_submissions "
                    "WHERE form_id = ? AND agency_key = ? "
                    "ORDER BY blessed DESC, captured_at DESC LIMIT 1",
                    (canonical_form, canonical_agency),
                ).fetchone()
                if row and row["pdf_data"]:
                    return bytes(row["pdf_data"])
            # Tier 3+4: any-agency fallback.
            row = conn.execute(
                "SELECT pdf_data FROM prior_submissions "
                "WHERE form_id = ? "
                "ORDER BY blessed DESC, captured_at DESC LIMIT 1",
                (canonical_form,),
            ).fetchone()
            if row and row["pdf_data"]:
                return bytes(row["pdf_data"])
    except Exception as e:
        log.debug("prior_submissions.latest_for DB lookup failed: %s", e)

    return _latest_for_from_filesystem(canonical_form)


def _latest_for_from_filesystem(form_id: str) -> Optional[bytes]:
    """Backwards-compat: PR #4 reads from `data/prior_submissions/<form_id>/`.
    Returns the newest .pdf by mtime, or None when the directory is
    empty / missing."""
    try:
        from src.core.paths import DATA_DIR
    except Exception:
        return None
    priors_dir = os.path.join(DATA_DIR, "prior_submissions", form_id)
    if not os.path.isdir(priors_dir):
        return None
    candidates = [
        os.path.join(priors_dir, f)
        for f in os.listdir(priors_dir)
        if f.lower().endswith(".pdf")
    ]
    if not candidates:
        return None
    candidates.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    try:
        with open(candidates[0], "rb") as f:
            return f.read()
    except Exception as e:
        log.debug("prior_submissions FS fallback read failed: %s", e)
        return None


def count_for(form_id: str, *, agency_key: Optional[str] = None) -> int:
    """Diagnostic: how many priors are stored for `(form_id, agency_key)`.
    Used by operator UI + tests to verify the auto-capture loop is
    actually populating the table. Returns 0 on any error."""
    if not form_id:
        return 0
    canonical_form = str(form_id).strip()
    try:
        from src.core.db import get_db
        with get_db() as conn:
            if agency_key is not None:
                row = conn.execute(
                    "SELECT COUNT(*) AS n FROM prior_submissions "
                    "WHERE form_id = ? AND agency_key = ?",
                    (canonical_form, (agency_key or "").strip().lower()),
                ).fetchone()
            else:
                row = conn.execute(
                    "SELECT COUNT(*) AS n FROM prior_submissions "
                    "WHERE form_id = ?",
                    (canonical_form,),
                ).fetchone()
            if row:
                return int(row["n"])
    except Exception as e:
        log.debug("prior_submissions.count_for failed: %s", e)
    return 0


# ── Auto-capture from rfq_files at Mark Sent time ───────────────────


def capture_from_rfq_generated_files(
    rfq_id: str,
    *,
    agency_key: str = "",
    source_quote_number: str = "",
) -> int:
    """Sweep `rfq_files` for `category='generated'` rows attached to
    `rfq_id` and copy each into `prior_submissions` with the form_id
    inferred from the filename pattern (`<sol>_<FORMID>_Reytech.pdf`).

    Called from the Mark Sent flow once an RFQ transitions to status
    'sent' — operator-blessed, every form in this packet is now a
    canonical prior for future mirror-fills of the same form.

    Returns count of priors captured. Never raises — failures log
    + return 0, never block the status transition.
    """
    if not rfq_id:
        return 0
    try:
        from src.core.db import get_db
    except Exception:
        return 0

    # Filename → form_id inference. The package generator writes
    # filenames like `<sol>_703B_Reytech.pdf` / `<sol>_704B_Reytech.pdf`
    # / `<sol>_BidPackage_Reytech.pdf`. We map the middle token to a
    # form_id via the registry. Anything unrecognized is skipped
    # (we don't want random captures polluting the prior pool).
    try:
        from src.forms.form_registry import all_form_ids
        known_forms = {f.lower(): f for f in all_form_ids()}
    except Exception:
        known_forms = {}

    captured = 0
    try:
        with get_db() as conn:
            rows = conn.execute(
                "SELECT id, filename, data FROM rfq_files "
                "WHERE rfq_id = ? AND category = 'generated' "
                "AND filename LIKE '%.pdf'",
                (rfq_id,),
            ).fetchall()
        for r in rows:
            filename = r["filename"] or ""
            inferred = _form_id_from_filename(filename, known_forms)
            if not inferred:
                log.debug(
                    "prior_submissions: no form_id match for filename=%r — skip",
                    filename,
                )
                continue
            data = r["data"]
            if not data:
                continue
            new_id = capture(
                inferred,
                bytes(data),
                agency_key=agency_key,
                source_rfq_id=rfq_id,
                source_quote_number=source_quote_number,
                filename=filename,
                blessed=False,  # operator can flip blessed via admin UI later
            )
            if new_id:
                captured += 1
        log.info(
            "prior_submissions.capture_from_rfq_generated_files: rfq=%s "
            "agency=%s captured=%d",
            rfq_id[:12], agency_key or "(any)", captured,
        )
    except Exception as e:
        log.warning("capture_from_rfq_generated_files failed: %s", e)
    return captured


def _form_id_from_filename(filename: str, known_forms: dict) -> str:
    """Extract the canonical form_id token from `<sol>_<FORMID>_Reytech.pdf`.
    Returns "" when no known form_id matches the middle token. Case-
    tolerant — preserves the registry's canonical casing
    (`dsh_attA` etc.) when matched."""
    if not filename or not known_forms:
        return ""
    base = os.path.basename(filename)
    # Strip extension + optional _Reytech / .pdf trailing tokens.
    name_no_ext, _ = os.path.splitext(base)
    parts = name_no_ext.split("_")
    if len(parts) < 2:
        return ""
    # Try every internal token against the registry, longest match first.
    candidates = parts[1:-1] if len(parts) >= 3 else parts[1:]
    for token in candidates:
        if token.lower() in known_forms:
            return known_forms[token.lower()]
    # Try the full middle-slice as one token (`BidPackage` → `bidpkg`).
    middle = "_".join(parts[1:-1]).lower()
    aliases = {
        "bidpackage": "bidpkg",
        "bid_package": "bidpkg",
        "703a": "703a",
        "703b": "703b",
        "703c": "703c",
        "704b": "704b",
        "ams_708": "ams708",
        "genai_708": "ams708",
    }
    if middle in aliases:
        canonical = aliases[middle]
        if canonical in known_forms:
            return known_forms[canonical]
    return ""


# Init on import — mirrors `_init_rfq_files_table` pattern.
_init_prior_submissions_table()
