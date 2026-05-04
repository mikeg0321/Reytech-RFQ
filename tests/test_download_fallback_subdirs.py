"""Download endpoint must find files even when URL sol slug doesn't match dir.

P0 2026-05-04: RFQ 7d3c0fee Auralis. solicitation_number = "GOOD" (a known
parser-junk value), but generation fell back to rfq_number "RFQ-Auralis"
for the output dir. The file-link rendering in rfq_detail.html uses
r.solicitation_number directly — URL becomes /api/download/GOOD/... while
the file lives at /data/output/RFQ-Auralis/...  Mid-quote 404, no clean
recovery path, structural mismatch.

These tests pin the fallback ladder:
  1. Direct path: OUTPUT_DIR/sol/filename
  2. FS subdir scan: OUTPUT_DIR/*/filename — catches sol-slug drift
  3. DB lookup by rfq_id / sol
  4. DB lookup by filename only (last resort)
"""
from __future__ import annotations

import os


class TestDownloadFallbackSubdirs:
    def test_404_when_truly_missing(self, auth_client):
        """Sanity: a filename that exists nowhere returns 404."""
        r = auth_client.get("/api/download/SOMEDIR/does_not_exist.pdf")
        assert r.status_code == 404

    def test_finds_file_in_correct_subdir(self, auth_client, tmp_path,
                                            monkeypatch):
        """Happy path: URL sol matches the dir → file served directly."""
        from src.api.modules import routes_rfq_admin
        out_dir = tmp_path / "output"
        sub = out_dir / "RFQ-Auralis"
        sub.mkdir(parents=True)
        target = sub / "RFQ-Auralis_Quote_Reytech.pdf"
        target.write_bytes(b"%PDF-1.4\nfake quote\n%%EOF")
        monkeypatch.setattr(routes_rfq_admin, "OUTPUT_DIR", str(out_dir))
        r = auth_client.get(
            "/api/download/RFQ-Auralis/RFQ-Auralis_Quote_Reytech.pdf")
        assert r.status_code == 200
        assert r.data.startswith(b"%PDF")

    def test_falls_back_to_subdir_scan_when_sol_slug_wrong(
            self, auth_client, tmp_path, monkeypatch):
        """The 7d3c0fee Auralis case: URL slug 'GOOD' doesn't match dir
        'RFQ-Auralis' but the filename is unique → scan finds it."""
        from src.api.modules import routes_rfq_admin
        out_dir = tmp_path / "output"
        sub = out_dir / "RFQ-Auralis"
        sub.mkdir(parents=True)
        target = sub / "RFQ_Package_CalVetDVA_RFQ-Auralis_ReytechInc.pdf"
        target.write_bytes(b"%PDF-1.4\nmerged package\n%%EOF")
        monkeypatch.setattr(routes_rfq_admin, "OUTPUT_DIR", str(out_dir))
        # URL uses junk sol "GOOD" — pre-fix this 404'd
        r = auth_client.get(
            "/api/download/GOOD/RFQ_Package_CalVetDVA_RFQ-Auralis_ReytechInc.pdf")
        assert r.status_code == 200, (
            f"Sub-dir scan didn't catch the sol-slug mismatch — "
            f"Mike still 404s on his merged package mid-quote. "
            f"Status={r.status_code} body={r.data[:200]!r}"
        )
        assert r.data.startswith(b"%PDF")

    def test_path_traversal_still_blocked(self, auth_client, tmp_path,
                                            monkeypatch):
        """Sanity: the fallback uses os.path.basename(filename) so traversal
        attempts (../, /, \\) can't escape OUTPUT_DIR. Pre-existing guard at
        line 1186-1187 — pinned here so the new fallback can't reintroduce."""
        from src.api.modules import routes_rfq_admin
        out_dir = tmp_path / "output"
        out_dir.mkdir()
        # Create a sensitive file OUTSIDE OUTPUT_DIR
        (tmp_path / "secret.pdf").write_bytes(b"%PDF-1.4\nsecret\n%%EOF")
        monkeypatch.setattr(routes_rfq_admin, "OUTPUT_DIR", str(out_dir))
        r = auth_client.get("/api/download/x/..%2Fsecret.pdf")
        assert r.status_code == 404
