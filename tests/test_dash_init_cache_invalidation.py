"""Home-page /api/dashboard/init cache must invalidate on PC writes.

2026-05-06 Mike P0: marked a PC as duplicate; "Today's Queue → Ready to
Review" stayed at 2 because the combined-init endpoint holds a 90-second
in-process cache and no PC-write path was tearing it down. Result: the
home-page counter reads pre-mark state for up to 90s after every status
change.

These tests pin the invalidation hook in `_save_single_pc` and
`_save_price_checks` so the regression can't return.
"""

import os


def _read(path):
    p = os.path.join(os.path.dirname(__file__), "..", path)
    with open(p, encoding="utf-8") as f:
        return f.read()


def test_save_single_pc_invalidates_dash_init_cache():
    """_save_single_pc must clear the dashboard combined-init cache after
    every successful write. Without this, status changes (duplicate,
    dismiss, mark-won) won't reflect on the home page until the cache
    naturally expires."""
    src = _read("src/api/data_layer.py")
    # The hook must reach into routes_prd28 and zero the cache.
    assert "from src.api.modules import routes_prd28 as _rprd" in src
    assert "_rprd._dash_init_cache[\"data\"] = None" in src
    assert "_rprd._dash_init_cache[\"ts\"] = 0" in src


def test_save_price_checks_also_invalidates():
    """The bulk save path (_save_price_checks) must also invalidate.
    Otherwise a multi-PC operation can leave the home page stale."""
    src = _read("src/api/data_layer.py")
    # Two invalidation blocks expected — one in _save_single_pc, one in
    # _save_price_checks.
    occurrences = src.count("_rprd._dash_init_cache[\"data\"] = None")
    assert occurrences >= 2, (
        f"Expected dash_init_cache invalidation in BOTH save paths "
        f"(_save_single_pc + _save_price_checks); found {occurrences} occurrence(s)."
    )


def test_invalidation_is_defensive():
    """Module load order means routes_prd28 may not be loaded yet when
    data_layer fires. The invalidation must be wrapped in try/except so
    a missing module doesn't break the save."""
    src = _read("src/api/data_layer.py")
    # The hasattr guard protects against partial module init.
    assert "hasattr(_rprd, \"_dash_init_cache\")" in src
