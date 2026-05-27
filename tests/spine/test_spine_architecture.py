"""The Spine — architectural tests.

These are the FORCING FUNCTION. They fail the build if any source file
under src/spine/ violates a Charter invariant. The discipline that
failed for three months as policy is enforced here by the test runner.

If you find one of these failing, do NOT loosen the test. Either fix
the source, or — if the invariant is genuinely wrong — update
src/spine/SPINE_CHARTER.md first, then re-derive the test from the new
charter.
"""
from __future__ import annotations

import ast
import json
import re
from pathlib import Path

import pytest

SPINE_DIR = Path(__file__).resolve().parents[2] / "src" / "spine"


# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────


def _spine_py_files() -> list[Path]:
    """Every .py file under src/spine/, including subpackages."""
    return sorted(SPINE_DIR.rglob("*.py"))


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


# ──────────────────────────────────────────────────────────────────────
# 1. No legacy imports inside src/spine/.
# ──────────────────────────────────────────────────────────────────────

# Allowed top-level packages the Spine may import from.
_SPINE_INTERNAL = {"src.spine"}

# Whitelisted leaf utilities. Adding to this list requires explicit
# justification in SPINE_CHARTER.md — every entry is a potential
# correctness dependency on legacy code.
_EXTERNAL_WHITELIST: set[str] = {
    # (empty in foundation PR — leaf utils wire in later PRs)
}

# Per-FILE sanctioned legacy imports. Scoped to a single Spine file so
# the exception cannot leak into the rest of the substrate — every other
# Spine file still gets zero legacy imports.
#
# packet_render.py is the Spine→legacy CCHCS packet adapter. It is BY
# DESIGN a boundary-crosser: rather than re-implement document filling
# (the Spine's own from-scratch agency_forms renderers produced the
# 2026-05-18 "trash" output), it delegates to the verified legacy filler
# that fills the buyer's actual packet PDF. Documented in SPINE_CHARTER.md
# §"Sanctioned Boundary — The CCHCS Packet Adapter".
_FILE_SCOPED_LEGACY_IMPORTS: dict[str, set[str]] = {
    "packet_render.py": {
        "src.core.paths",                  # DATA_DIR/OUTPUT_DIR — path constants
        "src.forms.cchcs_packet_parser",   # parse the buyer's packet
        "src.forms.cchcs_packet_filler",   # fill the buyer's packet
    },
    # Second sanctioned adapter (Job #1 PR-3) — the CCHCS standalone
    # form-set adapter. Delegates 703B/703C + 704B + Bid Package to the
    # verified legacy fillers. See SPINE_CHARTER.md "Second adapter —
    # forms_render.py".
    "forms_render.py": {
        "src.core.paths",                  # DATA_DIR/OUTPUT_DIR — path constants
        "src.forms.reytech_filler_v4",     # the verified 703B/703C/704B/bidpkg fillers
    },
    # Pillar-4 adapter renderers in src/spine/agency_forms/. Each file
    # is a thin adapter: it maps a Spine Quote + EmailContract onto the
    # call shape of a verified legacy filler (reytech_filler_v4 for the
    # AMS 703/704 forms, cchcs_attachment_fillers for the CCHCS bid-
    # package attachments) and delegates. Same boundary justification as
    # forms_render.py / packet_render.py: re-implementing a verified
    # renderer to satisfy import purity would make the Spine depend on
    # a worse, unverified renderer. See SPINE_CHARTER.md "Adapter
    # renderers in agency_forms/".
    #
    # Whitelisted PER FILE — a new adapter dropped into agency_forms/
    # without an entry here still fails this test. Architect approval
    # is required to extend this list (CLAUDE.md §0 LAW 4).
    "cchcs_703c.py": {
        "src.forms.reytech_filler_v4",     # fill_703c, load_config, get_pst_date
    },
    "cchcs_704c.py": {
        "src.forms.reytech_filler_v4",     # fill_704b, load_config, get_pst_date
    },
    "calrecycle_74.py": {
        "src.forms.cchcs_attachment_fillers",  # fill_calrecycle_74
    },
    "cuf.py": {
        "src.forms.cchcs_attachment_fillers",  # fill_cuf
    },
    "darfur.py": {
        "src.forms.cchcs_attachment_fillers",  # fill_darfur_act
    },
    "dvbe_843.py": {
        "src.forms.cchcs_attachment_fillers",  # fill_dvbe_843
    },
    "std_1000.py": {
        "src.forms.cchcs_attachment_fillers",  # fill_std_1000
    },
    "std_204.py": {
        "src.forms.cchcs_attachment_fillers",  # fill_std204
    },
}

# Stdlib + well-known third-party packages always OK.
_ALWAYS_OK_PREFIXES = (
    "pydantic",
    "pytest",
    "sqlite3",
    "json",
    "datetime",
    "pathlib",
    "typing",
    "enum",
    "threading",
    "re",
    "abc",
    "collections",
    "functools",
    "itertools",
    "logging",
    "os",
    "sys",
    "io",
    "uuid",
    "decimal",
    "copy",
    "ast",
    "hashlib",
    "secrets",
    "warnings",
    "contextlib",
    "dataclasses",
    "__future__",
)


def _is_legacy_import(module: str) -> bool:
    """True if `module` is from the legacy substrate."""
    if module is None:
        return False
    if module.startswith(_ALWAYS_OK_PREFIXES):
        return False
    if module in _EXTERNAL_WHITELIST:
        return False
    # Anything under src.* that is NOT src.spine.* is legacy.
    if module.startswith("src.") and not any(
        module == p or module.startswith(p + ".") for p in _SPINE_INTERNAL
    ):
        return True
    return False


def test_no_legacy_imports():
    """Spine files must not import from legacy modules.

    The whole point of the Spine is that its correctness does not
    depend on legacy correctness. Importing src.core.quote_model or
    src.forms.reytech_filler_v4 would re-couple the substrates and
    defeat the carve-out.
    """
    offenders: list[tuple[str, int, str]] = []
    for fp in _spine_py_files():
        try:
            tree = ast.parse(_read(fp))
        except SyntaxError as e:
            pytest.fail(f"{fp.relative_to(SPINE_DIR.parent.parent)}: SyntaxError: {e}")

        allowed = _FILE_SCOPED_LEGACY_IMPORTS.get(fp.name, frozenset())

        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if _is_legacy_import(alias.name) and alias.name not in allowed:
                        offenders.append((str(fp), node.lineno, f"import {alias.name}"))
            elif isinstance(node, ast.ImportFrom):
                if (
                    node.module
                    and _is_legacy_import(node.module)
                    and node.module not in allowed
                ):
                    names = ", ".join(a.name for a in node.names)
                    offenders.append(
                        (str(fp), node.lineno, f"from {node.module} import {names}")
                    )

    if offenders:
        msg = "\n".join(f"  {f}:{ln}: {what}" for f, ln, what in offenders)
        pytest.fail(
            "Spine files must not import legacy modules. Offenders:\n"
            + msg
            + "\n\nIf this import is genuinely necessary, add it to "
            "_EXTERNAL_WHITELIST in tests/spine/test_spine_architecture.py "
            "AND document the dependency in src/spine/SPINE_CHARTER.md."
        )


# ──────────────────────────────────────────────────────────────────────
# 2. No alias fields in the Spine model.
# ──────────────────────────────────────────────────────────────────────

# Banned field names — these are the aliases that caused the 5/15
# meltdown. Source-grep on any line that declares a Pydantic Field
# or column with one of these names fails the build.
_BANNED_FIELD_NAMES = (
    "bid_price",
    "price_per_unit",
    "our_price",
    "recommended_price",
    "sell_price",
    "shipping_amount",
    "shipping_option",
    "delivery_option",
    "tax_rate_pct",       # the legacy alias — use tax_rate_bps only.
    "tax_enabled",        # the legacy toggle — tax is mandatory.
    "default_markup",     # the falsy-OR fallback class.
    "price_buffer",
    "markup_pct",         # NOT stored — computed display only.
)

# Regex pattern: capture "<banned>:" or "<banned> =" or "'<banned>':"
# only as a field declaration (not as a string inside a docstring
# or a banned-list literal).
def _banned_field_regex(name: str) -> re.Pattern[str]:
    # Match: start-of-line whitespace + name + (':' or '=' or '"' bareword
    # appearing as a dict-key declaration) — but not the name appearing
    # inside a string literal or comment about banning it.
    return re.compile(
        rf"^\s*{re.escape(name)}\s*[:=]",
        re.MULTILINE,
    )


def test_no_alias_fields_in_spine_model():
    """The Spine model must not declare any banned alias field."""
    model_py = SPINE_DIR / "model.py"
    src = _read(model_py)

    # Strip docstrings and comments before scanning so the "what NOT to
    # use" prose in docstrings doesn't false-positive.
    tree = ast.parse(src)

    # Walk class bodies and collect actual field assignments.
    declared_names: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            for stmt in node.body:
                # `name: type = default`  → AnnAssign
                if isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name):
                    declared_names.append((stmt.lineno, stmt.target.id))
                # `name = value`  → Assign
                elif isinstance(stmt, ast.Assign):
                    for target in stmt.targets:
                        if isinstance(target, ast.Name):
                            declared_names.append((stmt.lineno, target.id))

    offenders = [(ln, n) for ln, n in declared_names if n in _BANNED_FIELD_NAMES]

    if offenders:
        msg = "\n".join(f"  model.py:{ln}: banned field {n!r}" for ln, n in offenders)
        pytest.fail(
            "Spine model declares banned alias fields:\n"
            + msg
            + "\n\nThese aliases caused the 2026-05-15 meltdown. They are "
            "structurally banned by the Spine charter. If you genuinely "
            "need a new field, name it un-ambiguously (NOT one of the "
            "legacy aliases) and add a charter justification."
        )


# ──────────────────────────────────────────────────────────────────────
# 3. extra='forbid' on every Pydantic model in the Spine.
# ──────────────────────────────────────────────────────────────────────


def test_extra_forbid_on_every_spine_model():
    """Every BaseModel under src/spine/ MUST set extra='forbid'.

    Without this, unknown fields silently vanish on round-trip (the
    persistence P0 class). With it, unknown fields RAISE at
    construction, which is what we want.
    """
    offenders: list[tuple[str, str]] = []

    for fp in _spine_py_files():
        if fp.name in ("__init__.py", "db.py"):
            # db.py declares no Pydantic models; __init__.py is just re-exports.
            continue

        src = _read(fp)
        tree = ast.parse(src)

        for node in ast.walk(tree):
            if not isinstance(node, ast.ClassDef):
                continue
            # Inherits from BaseModel (directly or as `pydantic.BaseModel`)?
            inherits_basemodel = any(
                (isinstance(b, ast.Name) and b.id == "BaseModel")
                or (isinstance(b, ast.Attribute) and b.attr == "BaseModel")
                for b in node.bases
            )
            if not inherits_basemodel:
                continue

            # Look for `model_config = ConfigDict(extra="forbid", ...)`.
            has_forbid = False
            for stmt in node.body:
                if isinstance(stmt, ast.Assign):
                    for target in stmt.targets:
                        if isinstance(target, ast.Name) and target.id == "model_config":
                            value = stmt.value
                            if isinstance(value, ast.Call):
                                for kw in value.keywords:
                                    if (
                                        kw.arg == "extra"
                                        and isinstance(kw.value, ast.Constant)
                                        and kw.value.value == "forbid"
                                    ):
                                        has_forbid = True

            if not has_forbid:
                offenders.append((fp.name, node.name))

    if offenders:
        msg = "\n".join(f"  {f}: class {c} missing extra='forbid'" for f, c in offenders)
        pytest.fail(
            "Every Pydantic BaseModel in src/spine/ must declare "
            "model_config = ConfigDict(extra='forbid', ...). Offenders:\n"
            + msg
        )


# ──────────────────────────────────────────────────────────────────────
# 4. Exactly one writer for spine_quotes.
# ──────────────────────────────────────────────────────────────────────

# Patterns that count as "writing to spine_quotes": an actual SQL
# string containing INSERT INTO/UPDATE/DELETE FROM/REPLACE INTO with
# spine_quotes as the target table. Case-sensitive (uppercase SQL is
# the project convention — caught by a separate lint if violated) and
# whitespace-tolerant but bounded to the same statement (no [^;]*
# spanning unrelated code; we require the keyword and the table name
# to appear within a small window of each other).
_WRITER_SQL_PATTERN = re.compile(
    r"\b(INSERT(?:\s+OR\s+REPLACE)?\s+INTO|UPDATE|DELETE\s+FROM|REPLACE\s+INTO)"
    r"\s+spine_quotes\b"
)


def test_exactly_one_writer_for_spine_quotes():
    """Only `db._persist_state` may write to spine_quotes."""
    write_sites: list[tuple[str, int, str]] = []

    for fp in _spine_py_files():
        src = _read(fp)
        for i, line in enumerate(src.splitlines(), start=1):
            # Skip schema-definition matches (CREATE TABLE).
            if "CREATE TABLE" in line.upper():
                continue
            if _WRITER_SQL_PATTERN.search(line):
                write_sites.append((str(fp), i, line.strip()))

    # Also scan multi-line strings via the full file body, but
    # de-dupe by line range so we don't double-count.
    for fp in _spine_py_files():
        src = _read(fp)
        for m in _WRITER_SQL_PATTERN.finditer(src):
            # Skip if inside CREATE TABLE block — those are schema, not writes.
            ctx = src[max(0, m.start() - 100): m.end()].upper()
            if "CREATE TABLE" in ctx:
                continue
            lineno = src.count("\n", 0, m.start()) + 1
            entry = (str(fp), lineno, src[m.start(): m.end()].strip().replace("\n", " "))
            if entry not in write_sites:
                write_sites.append(entry)

    # All legitimate write sites must be inside db._persist_state.
    db_py_path = str(SPINE_DIR / "db.py")
    illegitimate = [
        (f, ln, sql) for f, ln, sql in write_sites
        if not f.replace("\\", "/").endswith("src/spine/db.py")
    ]

    if illegitimate:
        msg = "\n".join(f"  {f}:{ln}: {sql}" for f, ln, sql in illegitimate)
        pytest.fail(
            "spine_quotes may only be written by db._persist_state. "
            "Illegitimate write sites:\n" + msg
        )

    # And inside db.py, the only function calling .execute() with a
    # write SQL on spine_quotes should be _persist_state.
    db_src = (SPINE_DIR / "db.py").read_text(encoding="utf-8")
    db_tree = ast.parse(db_src)

    write_functions: set[str] = set()
    for node in ast.walk(db_tree):
        if not isinstance(node, ast.FunctionDef):
            continue
        body_src = ast.get_source_segment(db_src, node) or ""
        if _WRITER_SQL_PATTERN.search(body_src) and "CREATE TABLE" not in body_src.upper():
            write_functions.add(node.name)

    if write_functions and write_functions != {"_persist_state"}:
        pytest.fail(
            f"Only `_persist_state` may write to spine_quotes. "
            f"Found writers: {sorted(write_functions)!r}"
        )


# ──────────────────────────────────────────────────────────────────────
# 5. Charter document exists.
# ──────────────────────────────────────────────────────────────────────


def test_charter_document_present():
    """SPINE_CHARTER.md must exist and be non-trivial.

    The model's invariants are derived from the charter. If someone
    deletes the charter, the architectural tests lose their grounding.
    """
    charter = SPINE_DIR / "SPINE_CHARTER.md"
    assert charter.exists(), "src/spine/SPINE_CHARTER.md is missing."
    text = charter.read_text(encoding="utf-8")
    assert len(text) >= 1000, (
        f"SPINE_CHARTER.md is only {len(text)} chars — looks stubbed. "
        "The charter is the canonical statement of the Spine's invariants."
    )
    # Sanity: the document mentions the invariants by name. Case-
    # insensitive so the charter can use sentence-cased section
    # headers ("Integer cents") without breaking the test.
    text_lower = text.lower()
    for keyword in (
        'extra="forbid"',
        "integer cents",
        "tax",
        "append-only",
        "shipping",
        "zero legacy imports",
    ):
        assert keyword.lower() in text_lower, (
            f"SPINE_CHARTER.md does not mention {keyword!r} — has the "
            "charter drifted from the invariants?"
        )


# ──────────────────────────────────────────────────────────────────────
# 6. The LAW 3 convergence ratchet.
# ──────────────────────────────────────────────────────────────────────
#
# CLAUDE.md §0 LAW 3: convergence is measured by COUNT, not lines. The
# count of quote-write paths and quote substrates may only ratchet DOWN.
# A rise fails the build. The baseline lives in convergence_baseline.json
# and is lowered only by a deletion commit.

_BASELINE_PATH = Path(__file__).resolve().parent / "convergence_baseline.json"
_REPO_ROOT = Path(__file__).resolve().parents[2]
_SPINE_WRITER_COUNT = 1

# Legacy quote-write paths in the four named directories. Deleting one of
# these functions (a LAW 2 migration) mechanically lowers the live count.
_LEGACY_WRITE_PATHS = (
    ("src/core/db.py", "upsert_quote"),
    ("src/forms/quote_generator.py", "_log_quote"),
    ("src/forms/quote_generator.py", "update_quote_status"),
    ("src/core/quote_lifecycle_shared.py", "set_quote_status_atomic"),
    ("src/api/data_layer.py", "save_rfqs"),
    ("src/api/data_layer.py", "_save_single_rfq"),
    ("src/api/data_layer.py", "_save_price_checks"),
    ("src/api/data_layer.py", "_save_single_pc"),
)


def _load_baseline() -> dict:
    assert _BASELINE_PATH.exists(), (
        "tests/spine/convergence_baseline.json is missing — the LAW 3 "
        "ratchet has no grounding. See CLAUDE.md §0."
    )
    return json.loads(_BASELINE_PATH.read_text(encoding="utf-8"))


def _count_live_write_paths() -> int:
    """Count quote-write paths that still exist on disk (LAW 3a)."""
    live = _SPINE_WRITER_COUNT
    for rel, func in _LEGACY_WRITE_PATHS:
        fp = _REPO_ROOT / rel
        if not fp.exists():
            continue
        tree = ast.parse(fp.read_text(encoding="utf-8"))
        if any(
            isinstance(n, ast.FunctionDef) and n.name == func
            for n in ast.walk(tree)
        ):
            live += 1
    return live


def _count_substrates() -> int:
    """Count distinct quote substrates still present in the tree (LAW 3b)."""
    n = 1 if (SPINE_DIR / "model.py").exists() else 0
    if (_REPO_ROOT / "src/core/quote_contract.py").exists():
        n += 1
    if (_REPO_ROOT / "src/api/data_layer.py").exists():
        n += 1
    return n


def test_convergence_ratchet_write_paths():
    """Quote-write paths must not exceed the baseline (LAW 3)."""
    baseline = _load_baseline()
    current = _count_live_write_paths()
    assert current <= baseline["quote_write_paths"], (
        f"Quote-write paths rose to {current}, baseline is "
        f"{baseline['quote_write_paths']}. CLAUDE.md §0 LAW 3: this number "
        f"may only go DOWN. A new write path was added without a deletion."
    )


def test_convergence_ratchet_substrates():
    """Quote substrates must not exceed the baseline (LAW 3)."""
    baseline = _load_baseline()
    current = _count_substrates()
    assert current <= baseline["quote_substrates"], (
        f"Quote substrates rose to {current}, baseline is "
        f"{baseline['quote_substrates']}. A fourth substrate requires "
        f"Architect AND Closer sign-off (CLAUDE.md §0 LAW 1/4)."
    )


def test_convergence_baseline_not_silently_raised():
    """The baseline file itself may only ratchet DOWN.

    Guards the ratchet against the obvious cheat: editing the JSON
    numbers upward instead of deleting code.
    """
    baseline = _load_baseline()
    ceiling = {
        "quote_write_paths": 9,
        "quote_substrates": 3,
        "tracked_working_directories": 138,
    }
    for key, cap in ceiling.items():
        assert baseline[key] <= cap, (
            f"convergence_baseline.json[{key}] = {baseline[key]} exceeds the "
            f"2026-05-21 ceiling of {cap}. The baseline only ratchets DOWN."
        )
