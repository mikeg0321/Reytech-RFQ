"""Quote Orchestrator — THE single connector for the RFQ/PC workflow.

This is the platform. Routes, pollers, and jobs call `QuoteOrchestrator.run`
and nothing else. Everything that used to be hand-wired (pick a profile,
fill forms, run QA, sign, check compliance) is now driven by an explicit
state machine with preconditions, postconditions, and a persisted audit log.

Design principles (see feedback_build_platforms_not_modules.md):
- A form is a schema (YAML profile), not code.
- An agency is a ruleset (`agency_config` + `agency_rules` table), not code.
- A quote is a state machine with preconditions — each stage refuses to
  advance if the prior stage's outputs are incomplete.
- Every decision is auditable via `quote_audit_log`.

Public API:
    from src.core.quote_orchestrator import QuoteOrchestrator, QuoteRequest

    result = QuoteOrchestrator().run(QuoteRequest(
        source="uploads/buyer_rfq.pdf",    # path | dict | None
        agency_key="calvet",                 # optional; inferred if empty
        doc_type="rfq",
        buyer_email_text="",                 # optional; fed to ComplianceValidator
        target_stage="qa_pass",              # stop here; operator reviews
    ))
    result.ok                -> bool
    result.quote             -> Quote (Pydantic, with in-memory audit trail)
    result.package           -> PackageResult | None (merged PDF + per-form bytes)
    result.compliance_report -> dict (deterministic + LLM gap findings)
    result.blockers          -> list[str]
    result.stage_history     -> list[dict] (every transition attempted)
"""
from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Any, Optional, Union

from src.core.quote_model import Quote, QuoteStatus, DocType
from src.core import agency_config
from src.forms.profile_registry import FormProfile

log = logging.getLogger("reytech.quote_orchestrator")

_PST = timezone(timedelta(hours=-8))


# ── Stage definitions ────────────────────────────────────────────────────────

# Stages mirror QuoteStatus but are explicitly ordered. The orchestrator
# refuses to skip stages — you can only advance one at a time, and each
# advance checks preconditions. (See _STAGE_ORDER below.)
_STAGE_ORDER = [
    "draft",       # QuoteStatus.DRAFT
    "parsed",      # QuoteStatus.PARSED — items extracted
    "priced",      # QuoteStatus.PRICED — every non-no_bid item has unit_cost > 0
    "qa_pass",     # QuoteStatus.QA_PASS — forms filled + readback + compliance OK
    "generated",   # QuoteStatus.GENERATED — merged package signed
    "sent",        # QuoteStatus.SENT — emailed to buyer
]


def _stage_index(stage: str) -> int:
    try:
        return _STAGE_ORDER.index(stage)
    except ValueError:
        return -1


# ── Request / Result ─────────────────────────────────────────────────────────

@dataclass
class QuoteRequest:
    """Input envelope for a single run of the orchestrator."""
    source: Union[str, dict, None] = None
    doc_type: str = "pc"                 # "pc" | "rfq"
    agency_key: str = ""                 # optional; resolved from email/PDF if empty
    buyer_email_text: str = ""           # used by ComplianceValidator
    solicitation_number: str = ""        # if known up-front
    target_stage: str = "qa_pass"        # where the pipeline should stop
    profile_ids: Optional[list[str]] = None   # explicit override; else derived
    actor: str = "system"                # for audit log
    signature_image_path: str = ""


@dataclass
class StageAttempt:
    """One attempted stage transition — recorded whether it succeeded or not."""
    stage_from: str
    stage_to: str
    outcome: str           # "advanced" | "blocked" | "skipped" | "error"
    reasons: list[str] = field(default_factory=list)
    at: str = ""

    def to_dict(self) -> dict:
        return {
            "stage_from": self.stage_from,
            "stage_to": self.stage_to,
            "outcome": self.outcome,
            "reasons": self.reasons,
            "at": self.at,
        }


@dataclass
class OrchestratorResult:
    """Output envelope — everything a caller needs to decide what to do next.

    blockers / warnings / notes are three distinct channels:
      * blockers — fatal: the run did not produce the requested target stage
      * warnings — non-fatal but operator-relevant problems (missing optional
        profile, audit row dropped, agency fell back, etc.)
      * notes    — operational success metadata (e.g., "sent to X (N bytes)")
        that dashboards should NOT count as warning signal
    """
    ok: bool = False
    quote: Optional[Quote] = None
    package: Any = None                          # PackageResult from package_engine
    compliance_report: dict = field(default_factory=dict)
    profiles_used: list[str] = field(default_factory=list)
    blockers: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    stage_history: list[StageAttempt] = field(default_factory=list)
    final_stage: str = "draft"


# ── The orchestrator ─────────────────────────────────────────────────────────

class QuoteOrchestrator:
    """One connector. One state machine. One audit log.

    Integrates with the existing quote_engine pipeline steps — this class
    does NOT duplicate parse/fill/validate logic, it sequences them with
    enforced preconditions and a persistent audit trail.
    """

    def __init__(self, *, persist_audit: bool = True):
        self.persist_audit = persist_audit

    # ── Public: one-shot run ──

    def run(self, request: QuoteRequest) -> OrchestratorResult:
        """Drive a quote from its current stage up to `request.target_stage`.

        Never raises on business-logic failure — returns an OrchestratorResult
        with blockers/warnings populated so callers can surface them. The
        top-level guard also catches unexpected programming errors (renamed
        attributes, unhandled KeyError from a malformed dict, etc.) and turns
        them into a clean blocker so a route caller never sees a 500.
        """
        result = OrchestratorResult()
        try:
            return self._run_unguarded(request, result)
        except Exception as e:
            log.exception("orchestrator run aborted by unexpected exception")
            result.blockers.append(
                f"orchestrator run aborted: {type(e).__name__}: {e}"
            )
            result.ok = False
            if result.quote is not None:
                result.final_stage = result.quote.status.value
            return result

    def _run_unguarded(
        self, request: QuoteRequest, result: OrchestratorResult
    ) -> OrchestratorResult:
        target_idx = _stage_index(request.target_stage)
        if target_idx < 0:
            result.blockers.append(f"Unknown target_stage: {request.target_stage}")
            return result

        # Validate doc_type up-front. Three downstream paths
        # (Quote(doc_type=DocType(...)), from_legacy_dict, quote_engine.ingest)
        # all crash on an unknown value and previously a routing typo turned
        # into a 500 instead of a clean blocker on the result envelope.
        valid_doc_types = {dt.value for dt in DocType}
        if request.doc_type not in valid_doc_types:
            result.blockers.append(
                f"Unknown doc_type: {request.doc_type!r} "
                f"(valid: {sorted(valid_doc_types)})"
            )
            return result

        # Stage 0 → 1: ingest
        quote = self._ingest(request, result)
        if quote is None:
            return result  # blockers already populated
        result.quote = quote

        # If _ingest returned a quote BUT populated blockers (terminal-legacy
        # WON/LOST refusal — see _ingest line ~262), short-circuit. The dict
        # source path returns the quote for inspection rather than None, so
        # without this guard run() would proceed to _resolve_agency,
        # _resolve_profiles, and the stage loop — adding cascading
        # "cannot skip stages" rows on top of the real ingest blocker.
        if result.blockers:
            result.final_stage = quote.status.value
            return result

        if target_idx < 1:
            result.final_stage = quote.status.value
            result.ok = True
            return result

        # Resolve agency + profiles BEFORE advancing stages. Many preconditions
        # depend on knowing which agency we're building for.
        self._resolve_agency(quote, request, result)

        # Hard gate: if the operator is targeting qa_pass or beyond and we
        # could not resolve an agency, refuse to advance. Otherwise the quote
        # falls back to a generic doc-type profile and ships without anyone
        # noticing it's missing the agency-specific package. Operator can
        # override by setting request.agency_key explicitly.
        qa_pass_idx = _stage_index("qa_pass")
        if not quote.header.agency_key and target_idx >= qa_pass_idx:
            reason = (
                f"agency unresolved — cannot advance past 'priced' to "
                f"'{request.target_stage}' without an agency. Set "
                "request.agency_key explicitly to override."
            )
            result.blockers.append(reason)
            # Record a synthetic stage-attempt so the persistent audit log
            # and the dashboard timeline both show WHERE the quote stopped.
            # Previously the gate fired silently — operators saw a quote
            # stuck at PRICED with no audit row explaining it.
            gate_attempt = StageAttempt(
                stage_from=quote.status.value,
                stage_to="qa_pass",
                outcome="blocked",
                reasons=[reason],
                at=datetime.now(_PST).isoformat(),
            )
            result.stage_history.append(gate_attempt)
            self._persist_audit(quote, gate_attempt, request.actor, result)
            result.final_stage = quote.status.value
            return result

        profiles = self._resolve_profiles(quote, request, result)
        result.profiles_used = [p.id for p in profiles]

        # Sequential stage advancement. Each attempt is audited.
        for next_stage in _STAGE_ORDER[1:target_idx + 1]:
            attempt = self._try_advance(quote, next_stage, request, profiles, result)
            result.stage_history.append(attempt)
            self._persist_audit(quote, attempt, request.actor, result)

            # "skipped" = already at/past this stage (e.g. ingest already
            # transitioned to PARSED) — that's a no-op, not a failure.
            if attempt.outcome in ("blocked", "error"):
                result.blockers.extend(attempt.reasons)
                result.final_stage = quote.status.value
                return result

        result.final_stage = quote.status.value
        result.ok = True
        return result

    # ── Stage 1: ingest ──

    def _ingest(self, request: QuoteRequest, result: OrchestratorResult) -> Optional[Quote]:
        """Bring source into a Quote. Advances DRAFT → PARSED if items present."""
        try:
            from src.core import quote_engine
        except Exception as e:
            result.blockers.append(f"quote_engine import failed: {e}")
            return None

        # Dict source: legacy round-trip via Quote.from_legacy_dict
        if isinstance(request.source, dict):
            try:
                quote = Quote.from_legacy_dict(request.source, doc_type=request.doc_type)
            except Exception as e:
                # Honor the run() docstring contract: never raise on bad
                # business input. Surface as a blocker so the route gets a
                # clean OrchestratorResult instead of a 500.
                result.blockers.append(f"ingest: legacy dict parse failed: {type(e).__name__}: {e}")
                log.error("orchestrator ingest: from_legacy_dict raised", exc_info=True)
                return None
            if request.solicitation_number:
                quote.header.solicitation_number = request.solicitation_number
            if request.agency_key:
                quote.header.agency_key = request.agency_key
            target_status = QuoteStatus.PARSED if quote.line_items else QuoteStatus.DRAFT
            try:
                quote.transition(target_status)
            except ValueError as e:
                # The quote came in at a terminal status (WON/LOST) — the
                # transition validator (PR #169) refused to re-open it.
                # That's the right call, but _ingest must surface it as a
                # blocker, not let it propagate and 500 the caller.
                result.blockers.append(
                    f"ingest: cannot reuse {quote.status.value} quote "
                    f"(closed quotes are immutable; clone instead): {e}"
                )
                return quote
            return quote

        # PDF source: delegate to quote_engine.ingest (wraps parse_engine)
        if isinstance(request.source, str):
            try:
                quote, warnings = quote_engine.ingest(request.source, doc_type=request.doc_type)
            except Exception as e:
                result.blockers.append(f"ingest: PDF parse failed: {type(e).__name__}: {e}")
                log.error("orchestrator ingest: quote_engine.ingest raised", exc_info=True)
                return None
            if request.solicitation_number and not quote.header.solicitation_number:
                quote.header.solicitation_number = request.solicitation_number
            if request.agency_key and not quote.header.agency_key:
                quote.header.agency_key = request.agency_key
            for w in warnings:
                result.warnings.append(f"parse: {w.field}: {w.message}")
                if getattr(w, "severity", "") == "error":
                    result.blockers.append(f"parse error on {w.field}: {w.message}")
            if quote.line_items:
                try:
                    quote.transition(QuoteStatus.PARSED)
                except ValueError as e:
                    result.blockers.append(
                        f"ingest: cannot reuse {quote.status.value} quote "
                        f"(closed quotes are immutable; clone instead): {e}"
                    )
            return quote if not result.blockers else None

        # No source: a blank Quote (for tests or operator-started drafts)
        quote = Quote(doc_type=DocType(request.doc_type))
        if request.agency_key:
            quote.header.agency_key = request.agency_key
        if request.solicitation_number:
            quote.header.solicitation_number = request.solicitation_number
        return quote

    # ── Agency resolution ──

    def _resolve_agency(self, quote: Quote, request: QuoteRequest, result: OrchestratorResult) -> None:
        """Fill quote.header.agency_key if blank, using agency_config matchers.

        agency_config.match_agency() reads specific keys (requestor_email,
        institution, email_subject, ship_to, etc.) — we pack what we know
        into that shape instead of a free-form blob.
        """
        if quote.header.agency_key:
            return

        rfq_data = {
            "agency": quote.header.agency_key,
            "agency_name": "",
            "requestor_email": quote.buyer.requestor_email or "",
            "email_sender": quote.buyer.requestor_email or "",
            "institution": quote.header.institution_key or "",
            "delivery_location": quote.ship_to.display() if quote.ship_to else "",
            "ship_to": quote.ship_to.display() if quote.ship_to else "",
            "solicitation_number": quote.header.solicitation_number or "",
            "email_subject": request.buyer_email_text[:500] if request.buyer_email_text else "",
        }

        if not any(str(v).strip() for v in rfq_data.values()):
            result.warnings.append("agency: no source signals to classify against")
            return

        try:
            key, _cfg = agency_config.match_agency(rfq_data)
            # match_agency falls back to "other" (a real config with minimal
            # forms) instead of returning empty — so a hit on "other" actually
            # means "I gave up". Don't claim that as a resolution; leave
            # agency_key empty so the qa_pass+ gate in run() fires. If an
            # operator legitimately wants to ship as "other", they pass
            # request.agency_key="other" explicitly and _resolve_agency
            # returns at the top.
            if key and key != "other":
                quote.header.agency_key = key
                log.info("orchestrator: resolved agency=%s", key)
            else:
                result.warnings.append(
                    f"agency: no confident match from agency_config "
                    f"(fell back to '{key or 'none'}')"
                )
        except Exception as e:
            result.warnings.append(f"agency match failed: {e}")

    # ── Profile resolution (agency-aware) ──

    def _resolve_profiles(self, quote: Quote, request: QuoteRequest, result: OrchestratorResult) -> list[FormProfile]:
        """Pick the form-profile set for this quote.

        Resolution order:
          1. Explicit `request.profile_ids` (operator override).
          2. `agency_config.required_forms` for the quote's agency, mapped
             to profile IDs via the registry's form_type field.
          3. Doc-type default (704a/704b) as a last resort.
        """
        from src.core import quote_engine

        try:
            profiles_registry = quote_engine.get_profiles()
        except Exception as e:
            # Honor the no-raise contract — surface as a blocker so the
            # caller gets a clean OrchestratorResult, not a 500.
            result.blockers.append(f"profile registry load failed: {type(e).__name__}: {e}")
            log.error("orchestrator: get_profiles raised", exc_info=True)
            return []

        # 1. Explicit override
        if request.profile_ids:
            out = []
            for pid in request.profile_ids:
                if pid in profiles_registry:
                    out.append(profiles_registry[pid])
                else:
                    result.warnings.append(f"profile override missing from registry: {pid}")
            if out:
                return out
            # Operator explicitly asked for these profiles. None matched.
            # Falling through to agency-driven resolution would silently
            # build a different package than the operator requested — exactly
            # the kind of "looks like it worked" failure this orchestrator
            # exists to eliminate. Block instead.
            result.blockers.append(
                f"no requested profile_ids matched the registry: "
                f"{request.profile_ids}"
            )
            return []

        # 2. Agency-driven
        agency_key = quote.header.agency_key or ""
        cfg = agency_config.DEFAULT_AGENCY_CONFIGS.get(agency_key, {})
        required_form_ids = cfg.get("required_forms") or []

        matched: list[FormProfile] = []
        missing: list[str] = []
        for form_id in required_form_ids:
            profile = _best_profile_for_form(form_id, profiles_registry)
            if profile:
                matched.append(profile)
            else:
                missing.append(form_id)

        if missing:
            result.warnings.append(
                f"agency '{agency_key}' requires forms with no profile yet: {missing}"
            )

        if matched:
            return matched

        # 3. Fallback to doc-type default
        try:
            fallback = quote_engine.pick_profile(quote)
            return [fallback]
        except Exception as e:
            result.blockers.append(f"no profile resolvable: {e}")
            return []

    # ── Stage advancement ──

    def _try_advance(
        self,
        quote: Quote,
        to_stage: str,
        request: QuoteRequest,
        profiles: list[FormProfile],
        result: OrchestratorResult,
    ) -> StageAttempt:
        """Check preconditions, run the transition, check postconditions.

        Returns a StageAttempt reflecting what happened. Does not mutate
        `result.blockers` — caller decides how to surface failures.
        """
        attempt = StageAttempt(
            stage_from=quote.status.value,
            stage_to=to_stage,
            outcome="blocked",
            at=datetime.now(_PST).isoformat(),
        )

        current_idx = _stage_index(quote.status.value)
        target_idx = _stage_index(to_stage)
        if target_idx <= current_idx:
            attempt.outcome = "skipped"
            attempt.reasons.append(f"already at or past {to_stage} (current={quote.status.value})")
            return attempt
        if target_idx != current_idx + 1:
            attempt.outcome = "blocked"
            attempt.reasons.append(
                f"cannot skip stages: current={quote.status.value}, requested={to_stage}"
            )
            return attempt

        ok, reasons = _preconditions_for(to_stage, quote, profiles)
        if not ok:
            attempt.reasons = reasons
            return attempt

        try:
            self._run_transition(quote, to_stage, request, profiles, result)
        except Exception as e:
            attempt.outcome = "error"
            attempt.reasons.append(f"transition raised: {type(e).__name__}: {e}")
            log.error("orchestrator transition error %s → %s: %s", quote.status.value, to_stage, e, exc_info=True)
            return attempt

        if quote.status.value == to_stage:
            attempt.outcome = "advanced"
        else:
            attempt.outcome = "blocked"
            attempt.reasons.append(
                f"transition ran but status is {quote.status.value}, expected {to_stage}"
            )
        return attempt

    def _run_transition(
        self,
        quote: Quote,
        to_stage: str,
        request: QuoteRequest,
        profiles: list[FormProfile],
        result: OrchestratorResult,
    ) -> None:
        """Actually run the stage. Caller has already checked preconditions."""
        from src.core import quote_engine

        if to_stage == "parsed":
            quote.transition(QuoteStatus.PARSED)
            return

        if to_stage == "priced":
            quote_engine.enrich_pricing(quote, apply=True)

            # Strict completeness gate — mirrors the generated stage. PRICED
            # is supposed to mean "every non-no_bid item has unit_cost > 0"
            # (see _STAGE_ORDER comment). Previously this was only enforced
            # at the qa_pass precondition, so a quote with $0 items would
            # show priced=advanced in the audit log and only fail when the
            # operator clicked through to qa_pass — wasted click + misleading
            # audit row. Refuse to enter PRICED unless every line is priced.
            unpriced = [
                it.line_no for it in quote.line_items
                if not it.no_bid and float(it.unit_cost or 0) <= 0
            ]
            if unpriced:
                raise RuntimeError(
                    f"priced incomplete: {len(unpriced)}/{len(quote.line_items)} "
                    f"items have no price (lines: {unpriced[:10]})"
                )

            quote.transition(QuoteStatus.PRICED)
            return

        if to_stage == "qa_pass":
            # Fill + validate each profile. Collect reports.
            per_form: list[dict] = []
            for profile in profiles:
                try:
                    draft = quote_engine.draft(quote, profile_id=profile.id, run_qa=True)
                except Exception as e:
                    per_form.append({
                        "profile_id": profile.id,
                        "filled": False,
                        "error": str(e),
                    })
                    continue
                per_form.append({
                    "profile_id": profile.id,
                    "filled": True,
                    "qa_passed": draft.qa_report.passed,
                    "warnings": [str(w) for w in getattr(draft.qa_report, "warnings", [])],
                    "errors": [str(e) for e in getattr(draft.qa_report, "errors", [])],
                    "bytes": len(draft.pdf_bytes) if draft.pdf_bytes else 0,
                })

            # Lazy import ComplianceValidator — may not exist yet.
            compliance_gap: dict = {"checked": False}
            try:
                from src.agents.compliance_validator import validate_package
                compliance_gap = validate_package(
                    quote=quote,
                    per_form_reports=per_form,
                    buyer_email_text=request.buyer_email_text,
                )
            except ImportError:
                log.debug("compliance_validator not available yet — skipping gap check")
            except Exception as e:
                compliance_gap = {"checked": False, "error": str(e)}

            result.compliance_report = {
                "per_form": per_form,
                "gap": compliance_gap,
            }

            any_fill_error = any(not r.get("filled", False) for r in per_form)
            any_qa_fail = any(r.get("filled") and not r.get("qa_passed", False) for r in per_form)
            compliance_blocked = compliance_gap.get("blockers", []) if isinstance(compliance_gap, dict) else []

            if any_fill_error or any_qa_fail or compliance_blocked:
                # Surface concrete reasons via the audit row instead of a
                # silent return. Previously this produced a useless generic
                # "transition ran but status is priced, expected qa_pass"
                # blocker — operators had to dig into result.compliance_report
                # to find out which form/check actually failed.
                problems: list[str] = []
                for r in per_form:
                    pid = r.get("profile_id", "?")
                    if not r.get("filled", False):
                        problems.append(f"{pid}: fill failed: {r.get('error', 'unknown')}")
                    elif not r.get("qa_passed", False):
                        errs = r.get("errors") or ["qa failed (no errors reported)"]
                        for e in errs:
                            problems.append(f"{pid}: qa: {e}")
                for b in compliance_blocked:
                    problems.append(f"compliance: {b}")
                # result.compliance_report is already populated above —
                # raise lets _try_advance record outcome="error" with the
                # specific reasons, while preserving the structured report.
                raise RuntimeError("qa_pass incomplete: " + " ; ".join(problems))
            quote.transition(QuoteStatus.QA_PASS)
            return

        if to_stage == "generated":
            pkg = quote_engine.finalize(
                quote,
                profile_ids=[p.id for p in profiles],
                sign_after=True,
                signature_image_path=request.signature_image_path,
                run_qa=False,  # already ran in qa_pass
                merge=True,
            )
            result.package = pkg

            # Strict completeness gate. finalize() warns on missing profiles
            # but proceeds — that path silently shipped incomplete packages
            # to operators. Block here unless every requested profile produced
            # bytes > 0 and there are no fill errors.
            expected = {p.id for p in profiles}
            produced = {a.profile_id for a in pkg.artifacts if a.pdf_bytes and len(a.pdf_bytes) > 0}
            missing_artifacts = expected - produced
            problems: list[str] = []
            if pkg.errors:
                problems.extend(f"fill error: {e}" for e in pkg.errors)
            if missing_artifacts:
                problems.append(
                    f"missing or empty artifacts for required profiles: {sorted(missing_artifacts)}"
                )
            if not pkg.merged_pdf:
                problems.append("merged_pdf is empty (nothing to send)")
            # finalize() flips ok=False on signing failures but leaves the
            # UNSIGNED merged_pdf populated. Without this gate the orchestrator
            # silently advanced and the sent stage emailed an unsigned package.
            # Surface the specific warning(s) so the operator sees WHY.
            if not getattr(pkg, "ok", True):
                signing_warnings = [
                    w for w in (getattr(pkg, "warnings", []) or [])
                    if "sign" in w.lower()
                ]
                if signing_warnings:
                    problems.extend(f"package not ok: {w}" for w in signing_warnings)
                else:
                    problems.append(
                        f"package not ok (warnings: {getattr(pkg, 'warnings', [])})"
                    )
            if problems:
                # Raise so _try_advance records outcome="error" with reasons.
                raise RuntimeError("generated incomplete: " + " ; ".join(problems))

            quote.transition(QuoteStatus.GENERATED)
            return

        if to_stage == "sent":
            send_info = self._send_package(quote, request, result)
            # Success metadata goes to notes — warnings is for problems.
            # Putting "sent to X" in warnings polluted any dashboard
            # counting warnings to flag problematic runs.
            result.notes.append(
                f"sent to {send_info['to']} ({send_info['bytes']} bytes)"
            )
            quote.transition(QuoteStatus.SENT)
            return

        raise ValueError(f"No transition handler for stage: {to_stage}")

    # ── Email send (sent stage) ──

    def _send_package(
        self,
        quote: Quote,
        request: QuoteRequest,
        result: OrchestratorResult,
    ) -> dict:
        """Email the merged package to the buyer.

        Idempotency: callers should not invoke this once status is SENT — the
        orchestrator's stage-advancement guard already prevents re-entry.

        Raises RuntimeError with a clear reason on any blocker so _try_advance
        records outcome="error" with the message in the audit row.
        """
        to_addr = (quote.buyer.requestor_email or "").strip()
        if not to_addr:
            raise RuntimeError("sent: quote.buyer.requestor_email is empty")
        # Both header fields fed the email subject/body via fallback ("quote",
        # "buyer") — refuse to ship a real email with placeholder text.
        sol_check = (quote.header.solicitation_number or "").strip()
        if not sol_check:
            raise RuntimeError("sent: quote.header.solicitation_number is empty")
        if not (quote.header.agency_key or "").strip():
            raise RuntimeError("sent: quote.header.agency_key is empty")
        pkg = result.package
        if not pkg or not getattr(pkg, "merged_pdf", None):
            raise RuntimeError("sent: no merged package available (run generated first)")
        if not os.environ.get("GMAIL_PASSWORD") or not os.environ.get("GMAIL_ADDRESS"):
            raise RuntimeError("sent: GMAIL_ADDRESS/GMAIL_PASSWORD not configured")

        # Lazy imports — keep the orchestrator startable in environments
        # that don't ship the email transport (tests, CI).
        try:
            from src.agents.email_poller import EmailSender
        except Exception as e:
            raise RuntimeError(f"sent: EmailSender unavailable: {e}")

        # Persist the merged PDF to a tmp path so EmailSender (file-based) can attach it.
        # Cleaned up in finally — long-running prod processes were leaking ~PDF/quote.
        import tempfile
        sol = (quote.header.solicitation_number or "quote").strip()
        # Sanitize filename — solicitation numbers are operator-controlled.
        safe_sol = re.sub(r"[^A-Za-z0-9_.-]+", "_", sol)[:40] or "quote"
        fd, attachment_path = tempfile.mkstemp(
            prefix=f"reytech_pkg_{safe_sol}_", suffix=".pdf",
        )
        try:
            with os.fdopen(fd, "wb") as tmp:
                tmp.write(pkg.merged_pdf)

            agency = (quote.header.agency_key or "buyer").strip().upper()
            first_name = (quote.buyer.requestor_name or "there").split(" ")[0] if hasattr(quote.buyer, "requestor_name") else "there"
            subject = f"Reytech Inc. - Quote Response - Solicitation #{sol}"
            body = (
                f"Dear {first_name},\n\n"
                f"Please find attached our quote response for {agency} Solicitation #{sol}.\n\n"
                "All items are quoted F.O.B. Destination, freight prepaid and included. "
                "Pricing is valid for 45 calendar days from the due date.\n\n"
                "Please let us know if you have any questions.\n\n"
                "Respectfully,"
            )
            draft = {
                "to": to_addr,
                "subject": subject,
                "body": body,
                "attachments": [attachment_path],
                "solicitation": sol,
            }

            sender = EmailSender({})
            try:
                sender.send(draft)
            except Exception as e:
                raise RuntimeError(f"sent: SMTP send failed: {e}")

            return {
                "to": to_addr,
                "subject": subject,
                "bytes": len(pkg.merged_pdf),
            }
        finally:
            try:
                os.unlink(attachment_path)
            except OSError:
                pass

    # ── Audit log persistence ──

    def _persist_audit(
        self,
        quote: Quote,
        attempt: StageAttempt,
        actor: str,
        result: Optional["OrchestratorResult"] = None,
    ) -> None:
        """Write one row to quote_audit_log. Failures are logged + surfaced
        as a warning on `result` (when passed), never raised.

        Best-effort — the DB may not be initialized in test contexts. The
        in-memory audit trail on `quote.provenance` is the always-on log.

        Failure visibility: previously failures were `log.debug`, which made
        a broken DB silently drop every audit row. Now they're `log.error`
        and (when `result` is passed) appended to `result.warnings` so the
        operator sees "audit row dropped" in the response and can chase the
        underlying DB issue instead of trusting an empty dashboard.
        """
        if not self.persist_audit:
            return
        try:
            from src.core.db import get_db
        except Exception:
            # DB module not available (test context, partial install). The
            # in-memory provenance trail on the Quote is still authoritative.
            return
        try:
            with get_db() as conn:
                conn.execute(
                    """INSERT INTO quote_audit_log
                       (quote_doc_id, doc_type, agency_key, stage_from, stage_to,
                        outcome, reasons_json, actor, at)
                       VALUES (?,?,?,?,?,?,?,?,?)""",
                    (
                        quote.doc_id or "",
                        quote.doc_type.value,
                        quote.header.agency_key or "",
                        attempt.stage_from,
                        attempt.stage_to,
                        attempt.outcome,
                        json.dumps(attempt.reasons),
                        actor,
                        attempt.at,
                    ),
                )
        except Exception as e:
            log.error(
                "audit row dropped (stage_from=%s stage_to=%s outcome=%s): %s",
                attempt.stage_from, attempt.stage_to, attempt.outcome, e,
            )
            if result is not None:
                result.warnings.append(
                    f"audit row dropped for {attempt.stage_from}→{attempt.stage_to} ({attempt.outcome}): {e}"
                )


# ── Preconditions per stage ─────────────────────────────────────────────────

def _preconditions_for(stage: str, quote: Quote, profiles: list[FormProfile]) -> tuple[bool, list[str]]:
    """Return (ok, [reasons]) — whether `quote` may advance to `stage`."""
    reasons: list[str] = []

    if stage == "parsed":
        if not quote.line_items:
            reasons.append("parsed: quote has no line items")
        return (len(reasons) == 0, reasons)

    if stage == "priced":
        if not quote.line_items:
            reasons.append("priced: no line items to price")
        return (len(reasons) == 0, reasons)

    if stage == "qa_pass":
        if not quote.line_items:
            reasons.append("qa_pass: no line items")
        unpriced = [
            it.line_no for it in quote.line_items
            if not it.no_bid and float(it.unit_cost) <= 0
        ]
        if unpriced:
            reasons.append(f"qa_pass: {len(unpriced)} unpriced items (lines: {unpriced[:5]})")
        if not profiles:
            reasons.append("qa_pass: no form profiles resolved")
        return (len(reasons) == 0, reasons)

    if stage == "generated":
        if quote.status != QuoteStatus.QA_PASS:
            reasons.append(f"generated: must be qa_pass first (current={quote.status.value})")
        if not profiles:
            reasons.append("generated: no form profiles resolved")
        return (len(reasons) == 0, reasons)

    if stage == "sent":
        if quote.status != QuoteStatus.GENERATED:
            reasons.append(f"sent: must be generated first (current={quote.status.value})")
        return (len(reasons) == 0, reasons)

    # Unknown target stages are blocked.
    reasons.append(f"no preconditions defined for stage: {stage}")
    return (False, reasons)


# ── Profile/form-id matching ────────────────────────────────────────────────

# agency_config uses form_id strings like "704b", "calrecycle74", "dvbe843".
# profile_registry uses id strings like "704b_reytech_standard". This map
# picks the canonical profile for each agency_config form_id.
_FORM_ID_TO_PROFILE_ID = {
    "703a": "703a_reytech_standard",
    "703b": "703b_reytech_standard",     # not yet built
    "703c": "703c_reytech_standard",     # not yet built
    "704a": "704a_reytech_standard",
    "704b": "704b_reytech_standard",
    "quote": "quote_reytech_letterhead",
    "calrecycle74": "calrecycle74_reytech_standard",
    "dvbe843": "dvbe843_reytech_standard",
    "std204": "std204_reytech_standard",
    "std205": "std205_reytech_standard",
    "std1000": "std1000_reytech_standard",
    "bidder_decl": "bidder_decl_reytech_standard",
    "darfur_act": "darfur_reytech_standard",
    "cv012_cuf": "cv012_cuf_reytech_standard",
    "barstow_cuf": "barstow_cuf_reytech_standard",    # not yet built
    "sellers_permit": "sellers_permit_reytech",
    "drug_free": "drug_free_reytech_standard",
    "obs_1600": "obs_1600_reytech_standard",          # not yet built
    "w9": "w9_reytech",                               # not yet built
    "bidpkg": "",  # Bid package is a container — handled by package_engine, not a profile
}


def _best_profile_for_form(form_id: str, registry: dict[str, FormProfile]) -> Optional[FormProfile]:
    """Map an agency_config form_id to a loaded profile.

    Returns None if no profile for this form_id exists yet — the caller
    surfaces this as a warning and the FormProfiler agent is the remedy.
    """
    pid = _FORM_ID_TO_PROFILE_ID.get(form_id, "")
    if pid and pid in registry:
        return registry[pid]
    return None
