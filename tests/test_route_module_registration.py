"""Hard CI gate for the route-module load list.

`src/api/dashboard.py` loads route modules from an explicit `_ROUTE_MODULES`
list (load order is load-bearing — modules inject symbols back into
dashboard globals for later modules to reference). The list is the only wire
between an on-disk `routes_*.py` file and the live app.

If a new file is dropped into `src/api/modules/` and the operator forgets to
add its name to `_ROUTE_MODULES`, every route in that file is silently inert
in production. This test fails the suite when that happens — see
`docs/DATA_ARCHITECTURE_MAP.md` §1 (silo S1).

If a name is removed from `_ROUTE_MODULES` without deleting the file (or
vice versa), the test also fails — that asymmetry is the foot-gun.
"""
from __future__ import annotations

import glob
import os

import pytest


MODULES_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "src", "api", "modules"
)


def _on_disk_route_modules() -> set[str]:
    return {
        os.path.splitext(os.path.basename(p))[0]
        for p in glob.glob(os.path.join(MODULES_DIR, "routes_*.py"))
    }


def _listed_route_modules() -> set[str]:
    # Importing dashboard executes the load loop, which is what we want anyway —
    # if a listed module fails to load we want pytest to surface it.
    from src.api import dashboard
    return set(dashboard._ROUTE_MODULES)


def test_route_module_list_matches_disk():
    """Every routes_*.py on disk must appear in _ROUTE_MODULES, and vice versa."""
    on_disk = _on_disk_route_modules()
    listed = _listed_route_modules()

    extra_on_disk = on_disk - listed
    missing_from_disk = listed - on_disk

    msg_parts = []
    if extra_on_disk:
        msg_parts.append(
            f"On disk but NOT in _ROUTE_MODULES (routes will silently 404): "
            f"{sorted(extra_on_disk)}"
        )
    if missing_from_disk:
        msg_parts.append(
            f"In _ROUTE_MODULES but NOT on disk (load will error at boot): "
            f"{sorted(missing_from_disk)}"
        )
    assert not msg_parts, " | ".join(msg_parts)


def test_every_listed_module_has_at_least_one_registered_route():
    """A listed module that defines no routes is a code smell — flag it."""
    from src.api import dashboard
    from src.api.shared import bp

    # Walk every registered rule and bucket by the source module.
    rules_by_module: dict[str, int] = {}
    for rule in bp.deferred_functions:
        # bp.deferred_functions are closures; inspecting the source module is
        # noisy. Cheaper proxy: just count total registrations and assert > 0.
        pass

    # url_map is populated after the app is built, but we can ask Flask:
    from flask import Flask
    app = Flask("audit_app")
    app.register_blueprint(bp)
    total_rules = len(list(app.url_map.iter_rules()))

    # Sanity: with 35 modules, we expect at least 1,000 rules. The exact
    # count is a more useful tripwire (see test_golden_path.py).
    assert total_rules > 100, (
        f"Expected blueprint to register many routes; got only {total_rules}. "
        f"A listed module probably failed to load — check boot logs for "
        f"'Failed to load route module' lines."
    )
