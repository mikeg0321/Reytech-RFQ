"""forms_drift_monitor.py — Phase 1.6 PR3i.

Walk recent emails + attachments, diff against the system's known
form patterns + agency configs, surface drift before it costs a quote.

Three drift signals:
  1. New form mentions — capitalized form names appearing in emails
     that aren't in FORM_TEXT_PATTERNS (e.g., a buyer started requiring
     "STD 999" that we don't recognize)
  2. Revised buyer templates — known buyer fingerprints that have a
     NEW fingerprint variant. Often a buyer rev'd CalRecycle 74 from
     "Revised 01/23" to "Revised 06/26" — our profile is stale.
  3. Agency rule deltas — agencies seen in incoming with required
     forms that differ from DEFAULT_AGENCY_CONFIGS

Cheap to run — single SQL pass + light regex on email subjects/bodies.
Designed to fire monthly via the existing scheduler pattern in
dashboard.py.
"""

import json
import logging
import os
import re
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Optional

log = logging.getLogger("reytech.forms_drift")


# ─── Heuristic patterns for "looks like a form name" ──────────────────────
# Form references in CA gov emails follow predictable shapes:
#   STD 123, STD-123, Form #456, GSPD-05-105, AMS 703B, etc.
_FORM_NAME_PATTERNS = [
    re.compile(r'\bSTD\s*[-#]?\s*(\d{3,4}[A-Z]?)\b', re.IGNORECASE),
    re.compile(r'\bForm\s*[-#]?\s*(\d{2,4}[A-Z]?)\b', re.IGNORECASE),
    re.compile(r'\b(GSPD-\d{2}-\d{3})\b'),
    re.compile(r'\b(AMS\s*\d{3}[A-Z]?)\b', re.IGNORECASE),
    re.compile(r'\b(CV\s*\d{3,4})\b', re.IGNORECASE),
    re.compile(r'\b(OBS\s*\d{4})\b', re.IGNORECASE),
    re.compile(r'\bCalRecycle\s*(\d{2,3})\b', re.IGNORECASE),
]


def scan_forms_drift(days: int = 30) -> dict:
    """Scan the last N days of incoming emails+attachments for drift.

    Returns:
        {
          "scanned_emails": int,
          "scanned_attachments": int,
          "lookback_days": int,
          "scanned_at": iso string,
          "new_form_mentions": [{token, mention_count, sample_subjects[]}],
          "revised_templates": [...],  # PR3c candidate fingerprints with
                                       # form_type matching a known profile
          "agency_anomalies": [{agency, observed_required, configured_required,
                                missing[], extra[]}],
        }
    """
    cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
    report = {
        "scanned_emails": 0,
        "scanned_attachments": 0,
        "lookback_days": days,
        "scanned_at": datetime.utcnow().isoformat() + "Z",
        "cutoff_date": cutoff,
        "new_form_mentions": [],
        "revised_templates": [],
        "agency_anomalies": [],
    }

    # 1. New form mentions
    known = _known_form_tokens()
    mentions = _scan_email_corpus_for_form_names(cutoff, known)
    report["scanned_emails"] = mentions["scanned_emails"]
    report["new_form_mentions"] = mentions["new_mentions"]

    # 2. Revised buyer templates
    revisions = _scan_revised_templates(cutoff)
    report["scanned_attachments"] = revisions["scanned_attachments"]
    report["revised_templates"] = revisions["revised"]

    # 3. Agency anomalies
    report["agency_anomalies"] = _scan_agency_anomalies(cutoff)

    return report


def latest_report() -> Optional[dict]:
    """Return the most recently saved drift report from disk."""
    path = _report_path()
    if not os.path.isfile(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        log.debug("latest_report read failed: %s", e)
        return None


def save_report(report: dict) -> str:
    """Persist a drift report to disk + return the path."""
    path = _report_path()
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)
    except Exception as e:
        log.debug("save_report failed: %s", e)
    return path


# ─── Internals ─────────────────────────────────────────────────────────────

def _data_dir() -> str:
    try:
        from src.api import dashboard
        return getattr(dashboard, "DATA_DIR", "data")
    except Exception:
        return "data"


def _report_path() -> str:
    return os.path.join(_data_dir(), "forms_drift", "latest_report.json")


def _known_form_tokens() -> set:
    """Collect every token from FORM_TEXT_PATTERNS — what we currently recognize."""
    try:
        from src.core.agency_config import FORM_TEXT_PATTERNS
        toks = set()
        for patterns in FORM_TEXT_PATTERNS.values():
            for p in patterns:
                toks.add(p.upper().strip())
        return toks
    except Exception as e:
        log.debug("_known_form_tokens failed: %s", e)
        return set()


def _scan_email_corpus_for_form_names(cutoff: str, known_tokens: set) -> dict:
    """Walk emails in last N days, extract form-like tokens not in known set."""
    new_mentions = defaultdict(lambda: {"count": 0, "samples": []})
    scanned = 0
    try:
        from src.core.db import get_db
        with get_db() as conn:
            # Probe rfqs.body_text + price_checks (no body column on PCs;
            # use subject as proxy)
            try:
                rows = conn.execute(
                    """SELECT email_subject, body_text FROM rfqs
                       WHERE COALESCE(received_at,'') >= ?
                       ORDER BY received_at DESC LIMIT 500""",
                    (cutoff,)
                ).fetchall()
            except Exception:
                rows = []
            for r in rows:
                scanned += 1
                text = ((r["email_subject"] or "") + "\n" +
                        (r["body_text"] or ""))
                for token in _extract_form_tokens(text):
                    if token.upper() in known_tokens:
                        continue
                    nm = new_mentions[token.upper()]
                    nm["count"] += 1
                    if len(nm["samples"]) < 3 and r["email_subject"]:
                        nm["samples"].append(r["email_subject"][:100])

            try:
                rows = conn.execute(
                    """SELECT email_subject FROM price_checks
                       WHERE COALESCE(received_at,'') >= ?
                       ORDER BY received_at DESC LIMIT 500""",
                    (cutoff,)
                ).fetchall()
            except Exception:
                rows = []
            for r in rows:
                scanned += 1
                text = (r["email_subject"] or "")
                for token in _extract_form_tokens(text):
                    if token.upper() in known_tokens:
                        continue
                    nm = new_mentions[token.upper()]
                    nm["count"] += 1
                    if len(nm["samples"]) < 3 and text:
                        nm["samples"].append(text[:100])
    except Exception as e:
        log.debug("_scan_email_corpus failed: %s", e)

    out = []
    for token, info in sorted(new_mentions.items(),
                               key=lambda x: -x[1]["count"]):
        if info["count"] >= 2:  # filter one-off typos
            out.append({
                "token": token,
                "mention_count": info["count"],
                "sample_subjects": info["samples"],
            })
    return {"scanned_emails": scanned, "new_mentions": out[:30]}


def _extract_form_tokens(text: str) -> list:
    """Pull form-name tokens from arbitrary text."""
    if not text:
        return []
    found = []
    for pat in _FORM_NAME_PATTERNS:
        for m in pat.finditer(text):
            tok = m.group(0).upper()
            tok = re.sub(r"\s+", " ", tok).strip()
            if tok not in found:
                found.append(tok)
    return found


def _scan_revised_templates(cutoff: str) -> dict:
    """Look for buyer_template_candidates with form_type_guess matching a
    known profile but a different fingerprint — likely a template revision."""
    revised = []
    scanned = 0
    try:
        from src.core.db import get_db
        from src.forms.profile_registry import load_profiles
        profiles = load_profiles() or {}

        # Build map of form_type → known fingerprints
        ft_to_fps = defaultdict(set)
        for p in profiles.values():
            if getattr(p, "fingerprint", "") and getattr(p, "form_type", ""):
                ft_to_fps[p.form_type].add(p.fingerprint)

        with get_db() as conn:
            try:
                rows = conn.execute(
                    """SELECT id, fingerprint, agency_key, form_type_guess,
                              sample_filename, seen_count, last_seen_at
                       FROM buyer_template_candidates
                       WHERE COALESCE(last_seen_at,'') >= ?
                       ORDER BY last_seen_at DESC LIMIT 200""",
                    (cutoff,)
                ).fetchall()
            except Exception:
                rows = []
            for r in rows:
                scanned += 1
                ft = (r["form_type_guess"] or "").strip()
                if not ft or ft not in ft_to_fps:
                    continue
                # Form_type known but fingerprint isn't ours → revision
                if r["fingerprint"] not in ft_to_fps[ft]:
                    revised.append({
                        "fingerprint": r["fingerprint"][:16],
                        "agency_key": r["agency_key"],
                        "form_type_guess": ft,
                        "sample_filename": r["sample_filename"],
                        "seen_count": r["seen_count"],
                        "last_seen_at": r["last_seen_at"],
                        "known_fingerprints_for_form": [
                            fp[:16] for fp in ft_to_fps[ft]
                        ],
                    })
    except Exception as e:
        log.debug("_scan_revised_templates failed: %s", e)
    return {"scanned_attachments": scanned, "revised": revised}


def _scan_agency_anomalies(cutoff: str) -> list:
    """Compare observed required-forms (from email contracts) per agency
    against DEFAULT_AGENCY_CONFIGS. Flag deltas."""
    anomalies = []
    try:
        from src.core.db import get_db
        from src.core.agency_config import (
            match_agency, get_agency_config,
        )

        observed = defaultdict(lambda: defaultdict(int))
        with get_db() as conn:
            for tbl in ("rfqs", "price_checks"):
                try:
                    rows = conn.execute(
                        f"""SELECT agency, requirements_json FROM {tbl}
                            WHERE COALESCE(received_at,'') >= ?
                            AND requirements_json IS NOT NULL
                            ORDER BY received_at DESC LIMIT 500""",
                        (cutoff,)
                    ).fetchall()
                except Exception:
                    rows = []
                for r in rows:
                    raw = r["requirements_json"] or ""
                    try:
                        d = json.loads(raw)
                    except Exception:
                        continue
                    forms = d.get("forms_required") or []
                    if not forms:
                        continue
                    try:
                        akey, _ = match_agency({"agency": r["agency"]})
                    except Exception:
                        akey = (r["agency"] or "other").lower()
                    for f in forms:
                        observed[akey][f] += 1

        for akey, counts in observed.items():
            try:
                cfg = get_agency_config(akey)
            except Exception:
                cfg = {}
            configured = set(cfg.get("required_forms", []) or [])
            obs_set = {f for f, c in counts.items() if c >= 2}
            missing = sorted(obs_set - configured)
            extra = sorted(configured - obs_set)
            if missing or extra:
                anomalies.append({
                    "agency": akey,
                    "observed_required": sorted(obs_set),
                    "configured_required": sorted(configured),
                    "missing_from_config": missing,
                    "in_config_but_unseen": extra,
                })
    except Exception as e:
        log.debug("_scan_agency_anomalies failed: %s", e)
    return anomalies
