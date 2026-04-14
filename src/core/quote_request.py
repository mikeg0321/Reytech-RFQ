"""QuoteRequest — canonical interface over PC and RFQ records.

Phase 1 of the PC↔RFQ unification. Zero data migration. The wrapper
reads EITHER a PC record OR an RFQ record and exposes a common
interface that every downstream step calls:

    qr = QuoteRequest.from_pc(pc_dict)
    qr.get_items()          # normalized item list
    qr.get_agency()         # canonical agency key
    qr.get_required_forms() # from agency_config + classifier
    qr.get_solicitation()
    qr.get_classification() # RequestClassification if classifier ran
    qr.get_buyer_email()
    qr.get_status()
    qr.get_created_at()

By centralizing these reads, the three different field-path variants
(`pc.items` vs `pc.pc_data.items` vs `pc.pc_data["items"]`) collapse
into ONE canonical reader, fixing the "links were not putting correct
information" bug class.

Downstream code never reaches into PC/RFQ dicts directly — it goes
through the wrapper. When Phase 5 deletes the parallel route modules,
the underlying storage can unify to a single schema without breaking
any callers.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

log = logging.getLogger("reytech.quote_request")


@dataclass
class QuoteRequest:
    """Canonical buyer-request wrapper. Holds the raw record + a
    kind ('pc' | 'rfq') so callers can still branch on origin if
    needed — but should prefer the accessor methods below."""
    kind: str                   # "pc" | "rfq"
    raw: Dict[str, Any]         # the underlying PC or RFQ dict
    record_id: str = ""

    # ── Constructors ────────────────────────────────────────────────

    @classmethod
    def from_pc(cls, pc: Dict[str, Any]) -> "QuoteRequest":
        if not isinstance(pc, dict):
            pc = {}
        return cls(
            kind="pc",
            raw=pc,
            record_id=pc.get("id") or pc.get("pc_id") or "",
        )

    @classmethod
    def from_rfq(cls, rfq: Dict[str, Any]) -> "QuoteRequest":
        if not isinstance(rfq, dict):
            rfq = {}
        return cls(
            kind="rfq",
            raw=rfq,
            record_id=rfq.get("id") or rfq.get("rfq_id") or "",
        )

    # ── Canonical item reader — THE fix for field-path drift ────────

    def get_items(self) -> List[Dict[str, Any]]:
        """Return the item list from the record, handling every known
        storage shape:
          - rfq["line_items"]             (current RFQ)
          - rfq["items"]                  (legacy)
          - pc["items"]                   (current PC)
          - pc["pc_data"]["items"]        (nested)
          - pc["pc_data"] as JSON string
          - pc["data_json"] as JSON string (Orders V2 leftover)

        Returns a list (possibly empty). Never raises.
        """
        r = self.raw
        # RFQ-native path
        items = r.get("line_items")
        if isinstance(items, list) and items:
            return items
        # Direct items key
        items = r.get("items")
        if isinstance(items, list) and items:
            return items
        # pc_data nested shape
        pc_data = r.get("pc_data")
        if isinstance(pc_data, dict):
            items = pc_data.get("items")
            if isinstance(items, list) and items:
                return items
        # pc_data as JSON string
        if isinstance(pc_data, str) and pc_data:
            try:
                pd = json.loads(pc_data)
                if isinstance(pd, dict):
                    items = pd.get("items")
                    if isinstance(items, list) and items:
                        return items
            except Exception:
                pass
        # data_json fallback
        dj = r.get("data_json")
        if isinstance(dj, str) and dj:
            try:
                dd = json.loads(dj)
                if isinstance(dd, dict):
                    items = dd.get("line_items") or dd.get("items")
                    if isinstance(items, list) and items:
                        return items
            except Exception:
                pass
        return []

    def write_items(self, items: List[Dict[str, Any]]) -> None:
        """Canonical items writer. Every caller that mutates the item
        list should go through this so the record converges on a
        single storage path and legacy aliases stop drifting.

        For a PC record this writes `raw["items"]`; for an RFQ it
        writes `raw["line_items"]`. Both shapes are what the
        `_save_single_pc` / `_save_rfq` layers persist. Stale nested
        copies (`pc_data.items`, `data_json.items`, the opposite
        top-level key) are normalized so a subsequent `get_items()`
        can't pick up a stale read.
        """
        if items is None:
            items = []
        if not isinstance(items, list):
            # Defensive — never silently coerce, but don't crash either.
            log.debug("write_items: non-list ignored (%r)", type(items))
            return

        r = self.raw
        if self.kind == "rfq":
            r["line_items"] = items
            # RFQ callers historically sometimes also set 'items';
            # keep the shape aligned so get_items() stays stable.
            if "items" in r:
                r["items"] = items
        else:
            r["items"] = items
            # pc_data.items is a legacy nested mirror — keep it in sync
            # so a reader that still hits that path sees the new list.
            pc_data = r.get("pc_data")
            if isinstance(pc_data, dict):
                pc_data["items"] = items
            elif isinstance(pc_data, str) and pc_data:
                # JSON-string form: reserialize with the new items list
                try:
                    pd = json.loads(pc_data)
                    if isinstance(pd, dict):
                        pd["items"] = items
                        r["pc_data"] = json.dumps(pd, default=str)
                except Exception as e:
                    log.debug("write_items: pc_data reserialize failed: %s", e)

        # data_json is a serialized snapshot of the whole record (see
        # _save_single_pc). It's the stalest of all the aliases — just
        # drop it so the next save writes a fresh one from the live
        # top-level items list.
        r.pop("data_json", None)

    # ── Agency + forms ─────────────────────────────────────────────

    def get_agency(self) -> str:
        """Canonical agency key (matches agency_config keys).
        Falls back to the legacy string stored on the record."""
        r = self.raw
        # Classifier result is the authoritative source when present
        cls = r.get("_classification")
        if isinstance(cls, dict):
            a = cls.get("agency")
            if a:
                return a
        # Legacy fields
        for key in ("agency", "agency_key", "matched_agency"):
            v = r.get(key)
            if v:
                return str(v).lower().replace(" ", "_")
        return "other"

    def get_required_forms(self) -> List[str]:
        """Forms that MUST be generated. Primary source: the
        classifier result. Secondary: agency_config lookup via the
        agency key."""
        r = self.raw
        cls = r.get("_classification")
        if isinstance(cls, dict):
            forms = cls.get("required_forms")
            if isinstance(forms, list) and forms:
                return list(forms)
        # Fall back to agency_config
        try:
            from src.core.agency_config import DEFAULT_AGENCY_CONFIGS
            cfg = DEFAULT_AGENCY_CONFIGS.get(self.get_agency(), {})
            forms = cfg.get("required_forms", [])
            if forms:
                return list(forms)
        except Exception:
            pass
        return []

    # ── Request identity ────────────────────────────────────────────

    def get_solicitation(self) -> str:
        r = self.raw
        cls = r.get("_classification")
        if isinstance(cls, dict):
            s = cls.get("solicitation_number")
            if s:
                return str(s)
        for key in ("solicitation_number", "sol_number", "rfq_number", "pc_number"):
            v = r.get(key)
            if v:
                return str(v).strip()
        return ""

    def get_institution(self) -> str:
        r = self.raw
        cls = r.get("_classification")
        if isinstance(cls, dict):
            i = cls.get("institution")
            if i:
                return str(i)
        for key in ("institution", "department", "buyer_institution"):
            v = r.get(key)
            if v:
                return str(v).strip()
        return ""

    def get_buyer_email(self) -> str:
        r = self.raw
        for key in ("requestor_email", "buyer_email", "email",
                    "from_email", "sender_email"):
            v = r.get(key)
            if v and isinstance(v, str):
                return v.strip().lower()
        return ""

    def get_buyer_name(self) -> str:
        r = self.raw
        for key in ("requestor_name", "requestor", "buyer_name",
                    "from_name"):
            v = r.get(key)
            if v:
                return str(v).strip()
        return ""

    # ── Lifecycle ───────────────────────────────────────────────────

    def get_status(self) -> str:
        return str(self.raw.get("status", "") or "").lower()

    def get_created_at(self) -> str:
        return str(self.raw.get("created_at", "") or "")

    def get_due_date(self) -> str:
        r = self.raw
        for key in ("due_date", "quote_due_date", "response_deadline"):
            v = r.get(key)
            if v:
                return str(v)
        return ""

    # ── Shape + classification ─────────────────────────────────────

    def get_shape(self) -> str:
        """Request shape per classifier (cchcs_packet, pc_704_docx, etc).
        Falls back to inferring from `kind` when the classifier hasn't
        been run on this record."""
        cls = self.raw.get("_classification")
        if isinstance(cls, dict):
            s = cls.get("shape")
            if s:
                return str(s)
        # Inferred fallback — just says "pc" or "rfq"
        return self.kind

    def get_classification(self) -> Optional[Dict[str, Any]]:
        """The full RequestClassification dict stored on the record, or
        None if the classifier hasn't been run yet."""
        cls = self.raw.get("_classification")
        if isinstance(cls, dict):
            return cls
        return None

    def is_quote_only(self) -> bool:
        """True for simple PC worksheets (just need prices back), False
        for full RFQ packages (need prices + compliance forms)."""
        cls = self.raw.get("_classification")
        if isinstance(cls, dict):
            return bool(cls.get("is_quote_only", False))
        return self.kind == "pc"

    # ── Display / debugging ─────────────────────────────────────────

    def summary(self) -> str:
        """One-line description for logs and UI."""
        parts = [f"{self.kind} {self.record_id[:8]}"]
        sol = self.get_solicitation()
        if sol:
            parts.append(f"sol={sol}")
        agency = self.get_agency()
        if agency and agency != "other":
            parts.append(f"agency={agency}")
        inst = self.get_institution()
        if inst:
            parts.append(f"inst={inst[:30]}")
        nitems = len(self.get_items())
        parts.append(f"items={nitems}")
        return " | ".join(parts)


__all__ = ["QuoteRequest"]
