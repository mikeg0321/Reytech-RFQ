"""Phase 1.2: every URL in the growth tab bar must resolve to a registered
route. Mike reported 'top hyperlink 404' on /growth-intel — this guards
against regressing it.
"""

import re

import pytest


def _tab_urls():
    """Pull URLs out of the growth tabs partial via simple regex."""
    with open("src/templates/partials/_growth_tabs.html", "r", encoding="utf-8") as f:
        body = f.read()
    return re.findall(r"\('[^']+', '([^']+)', '[^']+'\)", body)


@pytest.mark.parametrize("url", _tab_urls())
def test_every_growth_tab_url_is_registered(client, url):
    # Use HEAD to test reachability cheaply; fall back to GET if HEAD not allowed
    r = client.get(url, follow_redirects=False)
    # Accept 200 (page) or 30x (intentional redirect to canonical URL).
    # Anything 4xx means a broken nav link Mike will see.
    assert r.status_code < 400, (
        f"Growth tab URL {url} returned {r.status_code} — fix _growth_tabs.html"
    )
