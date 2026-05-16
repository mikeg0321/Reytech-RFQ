"""Spin up a local Flask server with the Spine routes + Russ Test fixture.

Usage:
    py scripts/spine_serve_local.py
    # then open http://127.0.0.1:5057/spine/quotes/rfq_0ebe242f_test/edit

The server is isolated from prod: separate DB file under _diag/, auth
bypassed for local exploration. Use this to eyeball the operator UI
before pushing anything that touches templates.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from flask import Flask  # noqa: E402

from src.api.modules.routes_spine import make_spine_blueprint  # noqa: E402
from src.spine import init_db, write_quote  # noqa: E402
from src.spine_bridge import translate_legacy_quote  # noqa: E402


def main() -> int:
    diag = ROOT / "_diag"
    diag.mkdir(exist_ok=True)
    db_path = str(diag / "spine_local.db")
    init_db(db_path)

    # Seed the Russ Test fixture.
    fixture_path = ROOT / "tests" / "spine" / "fixtures" / "legacy_russ_no_bid_test.json"
    legacy = json.loads(fixture_path.read_text(encoding="utf-8"))
    result = translate_legacy_quote(legacy)
    if not result.ok:
        print("Russ fixture translation failed:")
        for issue in result.errors():
            print(f"  {issue.field_path}: {issue.detail}")
        return 1
    write_quote(db_path, result.quote,
                actor="local_dev",
                note="seeded by scripts/spine_serve_local.py")

    # Build the Flask app.
    template_dir = ROOT / "src" / "templates"
    app = Flask(__name__, template_folder=str(template_dir))
    app.config["TESTING"] = False

    @app.context_processor
    def _ctx():
        return {"csrf_token_value": "local-dev-no-csrf"}

    bp = make_spine_blueprint(db_path, auth_decorator=None)
    app.register_blueprint(bp)

    print(f"Spine local server starting on http://127.0.0.1:5057")
    print(f"  Edit Russ Test: http://127.0.0.1:5057/spine/quotes/rfq_0ebe242f_test/edit")
    print(f"  Quote PDF     : http://127.0.0.1:5057/spine/quotes/rfq_0ebe242f_test/pdf")
    print(f"  Event log     : http://127.0.0.1:5057/spine/quotes/rfq_0ebe242f_test/events")
    print(f"  DB            : {db_path}")
    print(f"\nCtrl+C to stop.\n")

    app.run(host="127.0.0.1", port=5057, debug=False, use_reloader=False)
    return 0


if __name__ == "__main__":
    sys.exit(main())
