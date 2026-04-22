"""Cross-template regression guard — every user-facing agency render must
go through `|agency_display`.

This test codifies platform fix #1 from project_orders_module_audit_2026_04_21
(closes RFQ-3, PC-14, O-8 as one class). It walks every `src/templates/*.html`
and fails if any Jinja expression emits an agency/institution-type key
without piping through the filter.

Exemptions — raw agency is legitimate in these contexts:
  - `|lower` / `|upper` / `|e` / `|urlencode` / `|tojson` — already typed
    for a non-display consumer (JS data attrs, URL params, JSON bodies)
  - HTML attribute values (`value="…"`, `data-…="…"`, `onclick="…"`) — the
    form-submit / JS-arg path, not rendered text
  - Inside `<script>` blocks — JS constants bound via tojson
  - Comments (`{# … #}`)

Rule of thumb: if it's text the user reads in a browser, it goes through
the filter. If it's a value the server or JS consumes, it stays raw.
"""
from __future__ import annotations

import pathlib
import re

REPO = pathlib.Path(__file__).resolve().parents[1]
TEMPLATES = REPO / "src" / "templates"

# A Jinja expression that mentions agency/agency_name/agency_key somewhere.
# Matches `{{ …agency… }}` but not `{% … agency … %}` (flow control).
_EXPR_RE = re.compile(r"\{\{([^{}]*?\bagency(?:_name|_key)?\b[^{}]*?)\}\}")

# Strip quoted string literals — their contents are data, not variables.
_QUOTED = re.compile(r"'[^']*'|\"[^\"]*\"")

# Attribute access or bare variable: `.agency`, `a.agency_name`, `agency`.
_ATTR_OR_BARE = re.compile(r"\bagency(?:_name|_key)?\b")

# Dict access via get(): `.get('agency', …)`, case the key lives inside
# a quoted literal (survives QUOTED-strip only if we look first).
_GET_ACCESS = re.compile(r"\.get\s*\(\s*['\"]agency(?:_name|_key)?['\"]")

# Filters / sinks that mark an expression as "not display text".
_SAFE_SINKS = (
    "|agency_display",
    "|lower",
    "|upper",         # legacy — keep allowed but prefer agency_display
    "|e",             # HTML-escape (usually JS arg)
    "|urlencode",
    "|tojson",
)

# Per-file exemptions — these expressions appear inside HTML attribute
# values or <script> blocks and are consumed by backend/JS, not rendered.
# Keys are (template_stem, substring_fingerprint) — one entry per known-
# exempt site, vetted by hand. New additions should be justified in the
# PR description.
_ATTR_EXEMPTIONS: set[tuple[str, str]] = {
    # Hidden form inputs + pagination URL params — value submitted back.
    ("quotes.html", 'name="agency" value="{{ agency_filter }}"'),
    ("quotes.html", "&agency={{ agency_filter|default('') }}"),
    # Edit-in-place input on the prospect page — user-editable identity.
    ("prospect_detail.html", 'data-field="agency" value="{{ agency }}"'),
    # Option VALUE on orders queue filter (label uses the filter; value
    # stays raw so the route comparison works).
    ("orders.html", 'value="{{ a }}" '),
}


def _is_inside_attr_or_script(source: str, expr_start: int) -> bool:
    """Return True if the Jinja expression at `expr_start` is inside
    an HTML attribute value or a <script> block — exempt from display rules."""
    head = source[:expr_start]
    # Inside <script>…</script>?
    last_script_open = head.rfind("<script")
    last_script_close = head.rfind("</script>")
    if last_script_open > last_script_close:
        return True
    # Inside an attribute value? Crude but effective: the last unmatched
    # `="` or `='` before us, with no `>` since, means we're in an attr.
    last_gt = head.rfind(">")
    tail_since_gt = head[last_gt + 1:] if last_gt >= 0 else head
    # Count unclosed attr-value openers in the current tag.
    opens = tail_since_gt.count('="') + tail_since_gt.count("='")
    closes = 0
    # Every `"` or `'` that closes an attr in the same tag counts.
    # Simplest heuristic: count quotes on the segment since last `=`.
    last_eq = tail_since_gt.rfind("=")
    if last_eq == -1:
        return False
    after_eq = tail_since_gt[last_eq + 1:]
    if not after_eq.startswith(('"', "'")):
        return False
    quote = after_eq[0]
    # Are we inside the attr value? Check if there's a matching close
    # quote between `after_eq[1:]` and our position (expr_start maps to
    # end of tail_since_gt).
    remaining = after_eq[1:]
    return quote not in remaining


def _has_safe_sink(expr_body: str) -> bool:
    return any(sink in expr_body for sink in _SAFE_SINKS)


def _emits_agency_value(expr_body: str) -> bool:
    """True if this expression's OUTPUT includes an agency value.

    Mere string-literal mentions of the word "agency" (column labels,
    comments) and predicate-only accesses inside `… if pred else …`
    don't count. We detect true emissions by:

      1. stripping all quoted strings, then
      2. splitting on ` if ` / ` else ` — agency tokens that survive in
         the value branches (not the predicate) are real emissions.
    """
    # Quick win: `.get("agency", …)` inside the value branch = emission.
    # We still need to exclude the predicate-only case, so defer to the
    # branch-split logic below.
    stripped = _QUOTED.sub("", expr_body)
    # Jinja ternary: `value_if_true if predicate else value_if_false`.
    # We want to check value_if_true AND value_if_false, NOT predicate.
    if " if " in stripped and " else " in stripped:
        # Split on ` if `, take [0] (the true-branch value). Then split
        # the rest on ` else `, take [1] (the false-branch value).
        left, _, rest = stripped.partition(" if ")
        _, _, right = rest.partition(" else ")
        value_positions = left + " " + right
    else:
        value_positions = stripped

    if _ATTR_OR_BARE.search(value_positions):
        return True

    # For .get('agency', …), the key is inside a quoted literal that
    # got stripped. Check the original expression, but only in the
    # value branches of a ternary if present.
    if " if " in expr_body and " else " in expr_body:
        left, _, rest = expr_body.partition(" if ")
        _, _, right = rest.partition(" else ")
        return bool(_GET_ACCESS.search(left) or _GET_ACCESS.search(right))
    return bool(_GET_ACCESS.search(expr_body))


def test_every_template_agency_render_pipes_through_filter():
    offenders: list[str] = []
    for tpl in sorted(TEMPLATES.glob("*.html")):
        source = tpl.read_text(encoding="utf-8")
        for match in _EXPR_RE.finditer(source):
            expr_body = match.group(1)
            expr_start = match.start()
            # Skip HTML-attribute + <script> contexts — not display text.
            if _is_inside_attr_or_script(source, expr_start):
                continue
            # Skip expressions where `agency` appears only in string
            # literals or ternary predicates — not an actual emission.
            if not _emits_agency_value(expr_body):
                continue
            if _has_safe_sink(expr_body):
                continue
            # Per-file exemption fingerprint match.
            exempt = False
            for tpl_name, fingerprint in _ATTR_EXEMPTIONS:
                if tpl.name == tpl_name and fingerprint in source:
                    # The fingerprint covers a specific line; require
                    # our match fall within it.
                    fp_start = source.find(fingerprint)
                    fp_end = fp_start + len(fingerprint)
                    if fp_start <= expr_start <= fp_end:
                        exempt = True
                        break
            if exempt:
                continue
            # Compute line number for a useful error.
            line_no = source.count("\n", 0, expr_start) + 1
            offenders.append(f"{tpl.relative_to(REPO)}:{line_no}  {{{{{expr_body}}}}}")

    assert not offenders, (
        "Platform fix #1 (RFQ-3/PC-14/O-8): every user-facing agency render "
        "must go through `|agency_display`. Raw leaks found:\n  "
        + "\n  ".join(offenders)
    )


def test_filter_is_registered_on_jinja_env(app):
    """Smoke — the filter itself must be wired or every site above fails at
    render time instead of at test time."""
    assert "agency_display" in app.jinja_env.filters
