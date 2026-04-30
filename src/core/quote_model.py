"""Canonical Quote model — single source of truth for PC and RFQ data.

One workflow, one model. PC vs RFQ is a label (doc_type), not a different code path.
The profile registry, fill engine, and QA engine all operate on Quote objects
regardless of whether the source was a PC or an RFQ.

Usage:
    from src.core.quote_model import Quote, QuoteStatus

    # Create from scratch
    q = Quote(doc_type="pc", doc_id="abc123", ...)

    # Migrate from legacy dict
    q = Quote.from_legacy_dict(old_pc_dict, doc_type="pc")

    # Serialize back for legacy consumers
    d = q.to_legacy_dict()

    # Pricing — operator sets cost + markup, model computes the rest
    q.set_price(line_no=1, unit_cost=Decimal("10.00"), markup_pct=Decimal("35"))

    # Status transition with validation
    q.transition(QuoteStatus.PRICED)
"""
import copy
import logging
from datetime import date, datetime, time, timezone, timedelta
from decimal import Decimal, ROUND_HALF_UP
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, computed_field, model_validator

log = logging.getLogger(__name__)

_PST = timezone(timedelta(hours=-8))


# ── Enums ────────────────────────────────────────────────────────────────────

class QuoteStatus(str, Enum):
    DRAFT = "draft"
    PARSED = "parsed"
    PRICED = "priced"
    QA_PASS = "qa_pass"
    GENERATED = "generated"
    SENT = "sent"
    WON = "won"
    LOST = "lost"

    # Legacy statuses mapped on import
    NEW = "new"
    ENRICHED = "enriched"
    READY = "ready"
    AUTO_DRAFTED = "auto_drafted"
    CONVERTED = "converted"
    DISMISSED = "dismissed"


_PIPELINE_STATUSES = [
    QuoteStatus.DRAFT,
    QuoteStatus.PARSED,
    QuoteStatus.PRICED,
    QuoteStatus.QA_PASS,
    QuoteStatus.GENERATED,
    QuoteStatus.SENT,
]
_TERMINAL_STATUSES = {QuoteStatus.WON, QuoteStatus.LOST}


class DocType(str, Enum):
    PC = "pc"
    RFQ = "rfq"


class PriceSource(str, Enum):
    CATALOG = "catalog"
    AMAZON = "amazon"
    SCPRS = "scprs"
    MANUAL = "manual"
    ORACLE = "oracle"
    SUPPLIER = "supplier"
    UNKNOWN = ""


# ── Sub-models ───────────────────────────────────────────────────────────────

class Address(BaseModel):
    """Shipping or billing address."""
    name: str = ""
    street: str = ""
    city: str = ""
    state: str = "CA"
    zip_code: str = ""
    full: str = ""  # Freeform fallback when structured fields aren't parsed

    def display(self) -> str:
        if self.full:
            return self.full
        parts = [self.name, self.street, self.city, self.state, self.zip_code]
        return ", ".join(p for p in parts if p)


class QuoteHeader(BaseModel):
    """Solicitation-level metadata."""
    solicitation_number: str = ""
    agency_key: str = ""
    institution_key: str = ""
    due_date: Optional[date] = None
    due_time: Optional[time] = None
    due_time_explicit: bool = False
    payment_terms: str = "Net 45"
    shipping_terms: str = "FOB Destination"
    delivery_days: str = "7-14 business days"


class BuyerInfo(BaseModel):
    """Buyer/requestor contact info."""
    requestor_name: str = ""
    requestor_email: str = ""
    requestor_phone: str = ""
    department: str = ""
    notes: str = ""


class VendorInfo(BaseModel):
    """Reytech static vendor info — loaded from config at construction."""
    name: str = "Reytech Inc."
    representative: str = "Michael Guadan"
    email: str = "sales@reytechinc.com"
    phone: str = "949-229-1575"
    address: Address = Address(
        name="Reytech Inc.",
        street="30 Carnoustie Way",
        city="Trabuco Canyon",
        state="CA",
        zip_code="92679",
        full="30 Carnoustie Way, Trabuco Canyon, CA 92679",
    )
    fein: str = ""
    sellers_permit: str = ""
    sb_cert: str = "2002605"
    dvbe_cert: str = "2002605"
    cert_expiration: str = ""
    signature_image_path: str = ""


class LineItem(BaseModel):
    """Single line item with pricing."""
    line_no: int = 0
    item_no: str = ""           # MFG#, not ASIN (per feedback_item_identity)
    upc: str = ""               # 12-digit UPC; strongest identifier for oracle lookup
    description: str = ""
    qty: Decimal = Decimal("1")
    uom: str = "EA"
    qty_per_uom: Decimal = Decimal("1")
    unit_cost: Decimal = Decimal("0")
    markup_pct: Decimal = Decimal("35")
    price_source: str = ""
    confidence: float = 1.0
    supplier: str = ""
    source_url: str = ""
    asin: str = ""
    notes: str = ""
    no_bid: bool = False

    # Reference prices (informational, not used for cost)
    amazon_price: Decimal = Decimal("0")
    scprs_price: Decimal = Decimal("0")
    catalog_cost: Decimal = Decimal("0")

    @computed_field
    @property
    def unit_price(self) -> Decimal:
        """Computed: cost * (1 + markup/100), rounded to 2 decimals."""
        if self.unit_cost <= 0:
            return Decimal("0")
        return (self.unit_cost * (1 + self.markup_pct / 100)).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )

    @computed_field
    @property
    def extension(self) -> Decimal:
        """Computed: unit_price * qty, rounded to 2 decimals."""
        return (self.unit_price * self.qty).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )


class ComplianceInfo(BaseModel):
    """Certification and compliance flags."""
    sb_cert_submitted: bool = True
    dvbe_cert_submitted: bool = True
    darfur_compliant: bool = True
    drug_free_workplace: bool = True
    std204_submitted: bool = False
    cal_recycle: bool = False


class AuditEntry(BaseModel):
    """Single entry in the audit trail."""
    timestamp: str = ""
    action: str = ""
    detail: str = ""
    by: str = ""


class Provenance(BaseModel):
    """Where the data came from and how it's been modified."""
    source: str = ""          # "email" | "manual" | "paste_url"
    parsed_from_files: list[str] = Field(default_factory=list)
    classifier_shape: str = ""
    parse_warnings: list[str] = Field(default_factory=list)
    last_modified_by: str = ""
    audit_trail: list[AuditEntry] = Field(default_factory=list)
    created_at: str = ""


# ── Main Quote Model ─────────────────────────────────────────────────────────

class Quote(BaseModel):
    """Canonical quote — one model for PCs and RFQs alike."""

    # Identity
    doc_type: DocType = DocType.PC
    doc_id: str = ""

    # Structured data
    header: QuoteHeader = Field(default_factory=QuoteHeader)
    buyer: BuyerInfo = Field(default_factory=BuyerInfo)
    ship_to: Address = Field(default_factory=Address)
    bill_to: Address = Field(default_factory=Address)
    vendor: VendorInfo = Field(default_factory=VendorInfo)
    line_items: list[LineItem] = Field(default_factory=list)
    compliance: ComplianceInfo = Field(default_factory=ComplianceInfo)
    status: QuoteStatus = QuoteStatus.DRAFT
    provenance: Provenance = Field(default_factory=Provenance)

    # Operational fields (carried through but not part of the schema's core)
    templates: dict = Field(default_factory=dict)
    output_files: list = Field(default_factory=list)
    extra: dict = Field(default_factory=dict)  # Catch-all for legacy fields

    # ── Computed totals ──

    @computed_field
    @property
    def subtotal(self) -> Decimal:
        return sum((it.extension for it in self.line_items if not it.no_bid), Decimal("0"))

    @computed_field
    @property
    def item_count(self) -> int:
        return len([it for it in self.line_items if not it.no_bid])

    # ── Typed mutation methods ──

    def set_price(self, line_no: int, unit_cost: Decimal, markup_pct: Decimal = Decimal("35")):
        """Set pricing on a line item. Validates 3x sanity rule."""
        for item in self.line_items:
            if item.line_no == line_no:
                # 3x sanity check against reference prices
                ref = max(item.scprs_price, item.catalog_cost, item.amazon_price)
                if ref > 0 and unit_cost > ref * 3:
                    log.warning(
                        "3x sanity: line %d cost $%.2f > 3x ref $%.2f — capping",
                        line_no, unit_cost, ref,
                    )
                    unit_cost = ref  # Cap to reference (CLAUDE.md: auto-correct to ref)

                item.unit_cost = unit_cost
                item.markup_pct = markup_pct
                item.price_source = "manual"
                self._audit(f"set_price line {line_no}: cost=${unit_cost}, markup={markup_pct}%")
                return
        raise ValueError(f"Line {line_no} not found")

    def update_buyer(self, **kwargs):
        """Update buyer info fields."""
        for k, v in kwargs.items():
            if hasattr(self.buyer, k):
                setattr(self.buyer, k, v)
        self._audit(f"update_buyer: {list(kwargs.keys())}")

    def add_item(self, item: LineItem):
        """Add a line item, auto-assigning line_no."""
        max_no = max((it.line_no for it in self.line_items), default=0)
        item.line_no = max_no + 1
        self.line_items.append(item)
        self._audit(f"add_item line {item.line_no}: {item.description[:50]}")

    def remove_item(self, line_no: int):
        """Remove a line item by line_no."""
        before = len(self.line_items)
        self.line_items = [it for it in self.line_items if it.line_no != line_no]
        if len(self.line_items) < before:
            self._audit(f"remove_item line {line_no}")

    def transition(self, new_status: QuoteStatus):
        """Transition status with prerequisite validation.

        Permissive forward-allowing model. Refuses two clearly bug-shaped
        cases that previously slipped through silently:

          * Re-opening a terminal status (WON / LOST) into the active
            pipeline — closed quotes don't get re-opened, clone instead.
          * Backward jumps in the main pipeline by more than one stage —
            a stale callback firing post-send shouldn't wipe SENT back to
            DRAFT.

        Forward jumps (DRAFT → PRICED), self-transitions, single-step
        backward (re-pricing after QA), and pipeline → terminal (close)
        all remain allowed.
        """
        cur = self.status
        if cur != new_status:
            if cur in _TERMINAL_STATUSES and new_status not in _TERMINAL_STATUSES:
                self._audit(
                    f"transition refused {cur.value} → {new_status.value} "
                    "(cannot re-open terminal status)"
                )
                raise ValueError(
                    f"illegal transition from terminal status "
                    f"{cur.value} to {new_status.value}"
                )
            if cur in _PIPELINE_STATUSES and new_status in _PIPELINE_STATUSES:
                ci = _PIPELINE_STATUSES.index(cur)
                ni = _PIPELINE_STATUSES.index(new_status)
                if ni < ci - 1:
                    self._audit(
                        f"transition refused {cur.value} → {new_status.value} "
                        "(backward jump skips stages)"
                    )
                    raise ValueError(
                        f"illegal backward transition: "
                        f"{cur.value} → {new_status.value} (skipped stages)"
                    )
        self._audit(f"transition {cur.value} → {new_status.value}")
        self.status = new_status

    def _audit(self, action: str, by: str = "system"):
        self.provenance.audit_trail.append(AuditEntry(
            timestamp=datetime.now(_PST).isoformat(),
            action=action,
            by=by,
        ))

    # ── Legacy migration ──

    @classmethod
    def from_legacy_dict(cls, d: dict, doc_type: str = "pc") -> "Quote":
        """Convert a legacy PC or RFQ dict into a Quote object.

        Handles both field-name conventions (PC vs RFQ) transparently.
        Unknown fields go into quote.extra for lossless round-trip.
        """
        d = copy.deepcopy(d)  # Never mutate the input

        # Header
        h = d.get("header") or d.get("parsed", {}).get("header") or {}
        header = QuoteHeader(
            solicitation_number=(
                h.get("pc_number") or d.get("solicitation_number")
                or d.get("rfq_number") or d.get("pc_number") or ""
            ),
            agency_key=h.get("agency") or d.get("agency_name") or d.get("agency") or "",
            institution_key=h.get("institution") or d.get("institution") or "",
            delivery_days=d.get("delivery_option") or "7-14 business days",
        )

        # Parse due date
        due_str = h.get("due_date") or d.get("due_date") or ""
        if due_str:
            for fmt in ("%m/%d/%y", "%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
                try:
                    header.due_date = datetime.strptime(due_str.strip(), fmt).date()
                    break
                except ValueError:
                    continue

        due_time_str = h.get("due_time") or d.get("due_time") or ""
        if due_time_str:
            for tfmt in ("%I:%M %p", "%I:%M%p", "%H:%M"):
                try:
                    t = datetime.strptime(due_time_str.strip(), tfmt)
                    header.due_time = time(t.hour, t.minute)
                    header.due_time_explicit = True
                    break
                except ValueError:
                    continue

        # Buyer
        buyer = BuyerInfo(
            requestor_name=h.get("requestor") or d.get("requestor_name") or d.get("requestor") or "",
            requestor_email=d.get("requestor_email") or "",
            requestor_phone=h.get("phone") or d.get("phone") or "",
            department=d.get("department") or "",
            notes=h.get("notes") or d.get("notes") or d.get("custom_notes") or "",
        )

        # Ship to
        ship_str = h.get("ship_to") or d.get("ship_to") or d.get("delivery_location") or ""
        zip_str = h.get("zip_code") or d.get("delivery_zip") or ""
        ship_to = Address(full=ship_str, zip_code=zip_str)

        # Line items — handle both PC and RFQ field conventions
        raw_items = d.get("line_items") or d.get("items") or d.get("parsed", {}).get("line_items") or []
        line_items = []
        for i, it in enumerate(raw_items):
            if isinstance(it, str):
                continue  # Skip malformed

            pricing = it.get("pricing") or {}
            line_items.append(LineItem(
                line_no=i + 1,
                item_no=it.get("part_number") or it.get("mfg_number") or it.get("item_number") or "",
                description=it.get("description") or "",
                qty=Decimal(str(it.get("qty") or it.get("quantity") or 1)),
                uom=it.get("uom") or it.get("unit") or it.get("unit_of_issue") or "EA",
                unit_cost=Decimal(str(
                    pricing.get("unit_cost") or it.get("unit_cost")
                    or it.get("supplier_cost") or it.get("vendor_cost") or 0
                )),
                markup_pct=Decimal(str(
                    pricing.get("markup_pct") or it.get("markup_pct")
                    or d.get("default_markup") or 35
                )),
                price_source=pricing.get("price_source") or it.get("price_source") or "",
                confidence=float(pricing.get("confidence") or it.get("confidence") or 1.0),
                supplier=it.get("supplier") or "",
                source_url=(
                    pricing.get("amazon_url") or it.get("source_url") or it.get("url") or ""
                ),
                asin=pricing.get("amazon_asin") or it.get("asin") or "",
                no_bid=bool(it.get("no_bid")),
                amazon_price=Decimal(str(pricing.get("amazon_price") or it.get("amazon_price") or 0)),
                scprs_price=Decimal(str(
                    pricing.get("scprs_price") or it.get("scprs_last_price")
                    or it.get("scprs_price") or 0
                )),
                catalog_cost=Decimal(str(pricing.get("catalog_cost") or it.get("catalog_cost") or 0)),
            ))

        # Status mapping
        raw_status = d.get("status", "draft")
        try:
            status = QuoteStatus(raw_status)
        except ValueError:
            status = QuoteStatus.DRAFT

        # Provenance
        provenance = Provenance(
            source=d.get("source") or "email",
            created_at=d.get("created_at") or "",
            parsed_from_files=[d.get("source_pdf")] if d.get("source_pdf") else [],
        )

        # Vendor info (from REYTECH_INFO if available)
        vendor = VendorInfo()
        try:
            from src.forms.price_check import REYTECH_INFO
            vendor = VendorInfo(
                name=REYTECH_INFO.get("company_name", "Reytech Inc."),
                representative=REYTECH_INFO.get("representative", "Michael Guadan"),
                email=REYTECH_INFO.get("email", "sales@reytechinc.com"),
                phone=REYTECH_INFO.get("phone", "949-229-1575"),
                fein=REYTECH_INFO.get("fein", ""),
                sellers_permit=REYTECH_INFO.get("sellers_permit", ""),
                sb_cert=REYTECH_INFO.get("sb_mb", "2002605"),
                dvbe_cert=REYTECH_INFO.get("dvbe", "2002605"),
            )
        except Exception:
            pass

        # Capture all unknown fields for lossless round-trip
        known_keys = {
            "id", "pc_number", "solicitation_number", "rfq_number", "institution",
            "agency_name", "agency", "ship_to", "delivery_location", "delivery_zip",
            "status", "tax_enabled", "tax_rate", "delivery_option", "custom_notes",
            "price_buffer", "default_markup", "parsed", "items", "line_items",
            "source_pdf", "source", "created_at", "header", "requestor_name",
            "requestor_email", "department", "phone", "notes", "due_date", "due_time",
            "requestor", "templates", "output_files", "award_method",
        }
        extra = {k: v for k, v in d.items() if k not in known_keys}

        return cls(
            doc_type=DocType(doc_type),
            doc_id=d.get("id", ""),
            header=header,
            buyer=buyer,
            ship_to=ship_to,
            vendor=vendor,
            line_items=line_items,
            compliance=ComplianceInfo(),
            status=status,
            provenance=provenance,
            templates=d.get("templates") or {},
            output_files=d.get("output_files") or [],
            extra=extra,
        )

    def to_legacy_dict(self) -> dict:
        """Convert back to a legacy dict for existing route handlers.

        Produces the union of PC and RFQ field conventions so either consumer works.
        """
        items_out = []
        for it in self.line_items:
            items_out.append({
                "item_number": it.item_no or str(it.line_no),
                "line_number": str(it.line_no),
                "part_number": it.item_no,
                "mfg_number": it.item_no,
                "description": it.description,
                "qty": float(it.qty),
                "quantity": float(it.qty),
                "uom": it.uom,
                "unit": it.uom,
                "unit_of_issue": it.uom,
                "unit_cost": float(it.unit_cost),
                "supplier_cost": float(it.unit_cost),
                "vendor_cost": float(it.unit_cost),
                "unit_price": float(it.unit_price),
                "price_per_unit": float(it.unit_price),
                "extension": float(it.extension),
                "no_bid": it.no_bid,
                "asin": it.asin,
                "source_url": it.source_url,
                "url": it.source_url,
                "supplier": it.supplier,
                "price_source": it.price_source,
                "confidence": it.confidence,
                "amazon_price": float(it.amazon_price),
                "scprs_last_price": float(it.scprs_price),
                "scprs_price": float(it.scprs_price),
                "catalog_cost": float(it.catalog_cost),
                "pricing": {
                    "unit_cost": float(it.unit_cost),
                    "markup_pct": float(it.markup_pct),
                    "unit_price": float(it.unit_price),
                    "extension": float(it.extension),
                    "amazon_price": float(it.amazon_price),
                    "scprs_price": float(it.scprs_price),
                    "catalog_cost": float(it.catalog_cost),
                    "price_source": it.price_source,
                    "confidence": it.confidence,
                    "recommended_price": float(it.unit_price),
                    "amazon_url": it.source_url if "amazon" in it.source_url else "",
                    "amazon_asin": it.asin,
                },
            })

        d = {
            "id": self.doc_id,
            "status": self.status.value,
            "institution": self.header.institution_key,
            "agency_name": self.header.agency_key,
            "agency": self.header.agency_key,
            "solicitation_number": self.header.solicitation_number,
            "rfq_number": self.header.solicitation_number,
            "pc_number": self.header.solicitation_number,
            "due_date": self.header.due_date.strftime("%m/%d/%Y") if self.header.due_date else "",
            "due_time": self.header.due_time.strftime("%I:%M %p") if self.header.due_time else "",
            "ship_to": self.ship_to.display(),
            "delivery_location": self.ship_to.display(),
            "delivery_zip": self.ship_to.zip_code,
            "requestor_name": self.buyer.requestor_name,
            "requestor": self.buyer.requestor_name,
            "requestor_email": self.buyer.requestor_email,
            "phone": self.buyer.requestor_phone,
            "department": self.buyer.department,
            "notes": self.buyer.notes,
            "custom_notes": self.buyer.notes,
            "delivery_option": self.header.delivery_days,
            "source": self.provenance.source,
            "created_at": self.provenance.created_at,
            "source_pdf": self.provenance.parsed_from_files[0] if self.provenance.parsed_from_files else "",
            "line_items": items_out,
            "items": items_out,
            "templates": self.templates,
            "output_files": self.output_files,
            "header": {
                "institution": self.header.institution_key,
                "pc_number": self.header.solicitation_number,
                "due_date": self.header.due_date.strftime("%m/%d/%Y") if self.header.due_date else "",
                "due_time": self.header.due_time.strftime("%I:%M %p") if self.header.due_time else "",
                "requestor": self.buyer.requestor_name,
                "phone": self.buyer.requestor_phone,
                "ship_to": self.ship_to.display(),
                "zip_code": self.ship_to.zip_code,
                "notes": self.buyer.notes,
            },
        }

        # Merge back extra fields for lossless round-trip
        for k, v in self.extra.items():
            if k not in d:
                d[k] = v

        return d
