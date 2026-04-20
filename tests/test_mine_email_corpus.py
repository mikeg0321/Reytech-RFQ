"""Smoke guards for tools/mine_email_corpus.py refresh mode.

We only exercise the pure-python helpers — `mine_inbox()` itself hits
Gmail, which is out of scope for a unit test. The helpers being tested:

  - `load_existing_gmail_ids()` — reads an existing JSONL and returns
    the set of gmail_ids present. Used to dedupe on refresh.
  - `_parse_args()` — argparse surface: --refresh, --since, --before,
    --inbox. Guarantees the CLI contract doesn't silently drift.
"""
from __future__ import annotations

import importlib
import json
import os
import sys

import pytest


@pytest.fixture(scope="module")
def miner_mod():
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    tools_dir = os.path.join(root, "tools")
    if tools_dir not in sys.path:
        sys.path.insert(0, tools_dir)
    if "mine_email_corpus" in sys.modules:
        del sys.modules["mine_email_corpus"]
    return importlib.import_module("mine_email_corpus")


def test_load_existing_gmail_ids_missing_file(miner_mod, tmp_path):
    assert miner_mod.load_existing_gmail_ids(
        str(tmp_path / "nope.jsonl")) == set()


def test_load_existing_gmail_ids_reads_all(miner_mod, tmp_path):
    jsonl = tmp_path / "sales_corpus.jsonl"
    with open(jsonl, "w", encoding="utf-8") as f:
        for gid in ("aaa", "bbb", "ccc"):
            f.write(json.dumps({"gmail_id": gid, "subject": "x"}) + "\n")

    ids = miner_mod.load_existing_gmail_ids(str(jsonl))
    assert ids == {"aaa", "bbb", "ccc"}


def test_load_existing_gmail_ids_tolerates_junk_lines(miner_mod, tmp_path):
    jsonl = tmp_path / "sales_corpus.jsonl"
    with open(jsonl, "w", encoding="utf-8") as f:
        f.write(json.dumps({"gmail_id": "aaa"}) + "\n")
        f.write("not json at all\n")
        f.write(json.dumps({"subject": "no-id"}) + "\n")  # no gmail_id key
        f.write(json.dumps({"gmail_id": "bbb"}) + "\n")

    ids = miner_mod.load_existing_gmail_ids(str(jsonl))
    assert ids == {"aaa", "bbb"}


def test_parse_args_defaults(miner_mod):
    a = miner_mod._parse_args([])
    assert a.refresh is False
    assert a.since is None
    assert a.before is None
    assert a.inbox == "both"


def test_parse_args_refresh_flags(miner_mod):
    a = miner_mod._parse_args(
        ["--refresh", "--since", "2026/04/01", "--inbox", "sales"])
    assert a.refresh is True
    assert a.since == "2026/04/01"
    assert a.inbox == "sales"


def test_parse_args_rejects_bad_inbox(miner_mod):
    with pytest.raises(SystemExit):
        miner_mod._parse_args(["--inbox", "marketing"])
