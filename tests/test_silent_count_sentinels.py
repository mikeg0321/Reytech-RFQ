"""Pin that COUNT(*) failure → -1 sentinel sites also emit log.warning.

Background: prod `/health` was silently returning `active_pcs:-1` for an
unknown duration because the `except Exception: result = -1` pattern had
no logging — the underlying exception (an ImportError typo) was discarded.

This test grand-pins every place that uses the same shape so a future
regression of the same class is *loud*, not silent.

Sites covered (5):
  * src/api/modules/routes_health.py    db-bloat per-table COUNT(*)
  * src/api/modules/routes_v1.py        /api/v1/health row_counts
  * src/core/data_integrity.py          integrity table check
  * src/core/ops_monitor.py             backup verification + bloat check (2 sites)

Memory: feedback_assert_sentinel_value_not_just_shape.md
"""
from __future__ import annotations

import os
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def _read(path):
    import pathlib
    return pathlib.Path(path).read_text(encoding="utf-8")


def _has_warn_near_sentinel(src: str, sentinel_token: str) -> bool:
    """For every line that contains `sentinel_token`, check whether
    a `log.warning(` call appears within 3 lines before it (same `except`
    block). Returns True only if EVERY occurrence has an adjacent warn."""
    lines = src.split("\n")
    found_any = False
    for i, line in enumerate(lines):
        if sentinel_token in line:
            window = "\n".join(lines[max(0, i - 4): i + 1])
            if "log.warning(" not in window:
                return False
            found_any = True
    return found_any


class TestRoutesHealthDbBloatWarns:
    def test_warn_near_minus_one(self):
        src = _read("src/api/modules/routes_health.py")
        # Sentinel pattern: `cnt = -1` (line 2711 today)
        assert _has_warn_near_sentinel(src, "cnt = -1"), \
            "routes_health.py: COUNT(*) sentinel missing adjacent log.warning"


class TestRoutesV1HealthWarns:
    def test_warn_near_minus_one(self):
        src = _read("src/api/modules/routes_v1.py")
        assert _has_warn_near_sentinel(src, 'db_info["row_counts"][t] = -1'), \
            "routes_v1.py: row_counts sentinel missing adjacent log.warning"


class TestDataIntegrityWarns:
    def test_warn_near_minus_one(self):
        src = _read("src/core/data_integrity.py")
        assert _has_warn_near_sentinel(src, "results[table] = -1"), \
            "data_integrity.py: results sentinel missing adjacent log.warning"


class TestOpsMonitorWarns:
    def test_warn_near_backup_count_sentinel(self):
        src = _read("src/core/ops_monitor.py")
        assert _has_warn_near_sentinel(
            src, 'result["checks"][f"{table}_count"] = -1'
        ), "ops_monitor.py backup: sentinel missing adjacent log.warning"

    def test_warn_near_bloat_count_sentinel(self):
        src = _read("src/core/ops_monitor.py")
        assert _has_warn_near_sentinel(src, "tables[row[0]] = -1"), \
            "ops_monitor.py bloat: sentinel missing adjacent log.warning"


class TestAllSitesUseExceptionVariable:
    """Each `except Exception as _ce:` should bind the exception so the
    log.warning can include it. A bare `except Exception:` immediately
    followed by `log.warning(...)` with no exception arg is the same
    silent-failure mode in a new disguise."""

    SITES = [
        ("src/api/modules/routes_health.py", "cnt = -1"),
        ("src/api/modules/routes_v1.py", 'db_info["row_counts"][t] = -1'),
        ("src/core/data_integrity.py", "results[table] = -1"),
        ("src/core/ops_monitor.py", 'result["checks"][f"{table}_count"] = -1'),
        ("src/core/ops_monitor.py", "tables[row[0]] = -1"),
    ]

    def test_every_site_binds_exception(self):
        for path, token in self.SITES:
            src = _read(path)
            lines = src.split("\n")
            for i, line in enumerate(lines):
                if token in line:
                    window = "\n".join(lines[max(0, i - 4): i + 1])
                    assert re.search(r"except Exception as \w+:", window), (
                        f"{path}: sentinel `{token}` adjacent except does "
                        f"not bind the exception name"
                    )
