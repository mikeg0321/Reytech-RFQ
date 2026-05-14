"""PR-AU — two-query union for Gmail poller.

PREQ 10847262 (Mohammad Chechi, CDCR, SATF) surfaced a substrate gap:
the existing poller query `in:inbox after:30d ...` misses any RFQ
that Gmail filters auto-archived. Sister email PREQ 10846357 from
same sender same day was in inbox and processed; 10847262 was
archived → never seen by the poller.

PR-AU runs TWO Gmail queries and unions the UID list:
  1. Existing: `in:inbox after:30d <noise filter>`
  2. NEW:      `has:attachment from:.ca.gov after:30d -in:trash -in:spam <noise>`

Tests pin:
  1. The poller source contains BOTH query strings.
  2. The archived-query targets buyer-domain emails with attachments.
  3. The archived-query excludes trash + spam.
  4. The archived-query reuses the same noise filter.
  5. A two-query union de-dupes UIDs that appear in both lists.
"""
from __future__ import annotations

import inspect


def test_poller_source_contains_inbox_query():
    """Sanity: existing in:inbox query still in source."""
    from src.agents import email_poller

    src = inspect.getsource(email_poller)
    assert "in:inbox after:" in src


def test_poller_source_contains_archived_buyer_query():
    """PR-AU: archived-buyer query added."""
    from src.agents import email_poller

    src = inspect.getsource(email_poller)
    # The archived query must scan attachment-bearing buyer-domain
    # emails. The exact phrasing matters for Gmail to interpret it.
    assert "has:attachment from:.ca.gov" in src, (
        "PR-AU archived-buyer query missing — without it, archived "
        "RFQs (e.g. PREQ 10847262) are never picked up by the poller."
    )


def test_archived_query_excludes_trash_and_spam():
    """The archived-buyer broadening must NOT pick up trashed or
    spam-flagged messages — that would re-process operator-rejected
    emails on every poll."""
    from src.agents import email_poller

    src = inspect.getsource(email_poller)
    assert "-in:trash" in src
    assert "-in:spam" in src


def test_poller_uses_two_gmail_queries():
    """Two separate `list_message_ids` calls — defensive over one
    OR-paren query that could silently fail and zero the whole poll."""
    from src.agents import email_poller

    src = inspect.getsource(email_poller)
    # Count list_message_ids call sites in check_for_rfqs. Multi-line
    # invocations break a literal "list_message_ids(self._gmail_service"
    # substring, so grep more flexibly: count any `list_message_ids(`
    # within the check_for_rfqs scope. The audit_missed_emails path
    # also has one — total module-wide is 3 (1 existing + 1 PR-AU + 1
    # audit). The PR-AU split is the key one we're pinning.
    call_count = src.count("list_message_ids(")
    assert call_count >= 3, (
        f"Expected at least 3 list_message_ids call sites in poller "
        f"source (inbox + archived-buyer + audit_missed_emails); "
        f"found {call_count}. PR-AU broadening may have been reverted."
    )
    # Plus pin both query variable names exist
    assert "inbox_query" in src
    assert "archived_query" in src


def test_archived_query_failure_does_not_kill_poll():
    """If Gmail rejects the archived query (broken syntax, rate limit,
    etc.), the poller must NOT silently zero the whole poll. The
    inbox query result must still flow through. Pinned by a guard
    that catches the archived-query exception."""
    from src.agents import email_poller

    src = inspect.getsource(email_poller)
    # The defensive wrapper logs and uses [] as fallback for the
    # archived query.
    assert "PR-AU archived-query failed" in src, (
        "Archived-query exception is no longer caught — a single Gmail "
        "API hiccup could silently zero the poll."
    )


def test_archived_query_reuses_noise_filter():
    """The archived query must apply the same noise-source filter as
    the inbox query. A `from:.ca.gov` email from `noreply@dgs.ca.gov`
    should still be excluded by the existing `-from:noreply` clause."""
    from src.agents import email_poller

    src = inspect.getsource(email_poller)
    # Both queries must reference the noise_filter symbol so they
    # stay in lockstep.
    assert "noise_filter" in src
    # The noise filter content must include the standard exclusions.
    assert "-from:noreply" in src
    assert "-from:mailer-daemon" in src


def test_union_dedups_overlapping_uids():
    """Logical check: if a UID appears in BOTH inbox AND archived
    result sets (rare but possible — Gmail may report a message in
    inbox AND have `has:attachment` match), the union must not
    process it twice. The poller does this via a `seen` set check."""
    from src.agents import email_poller

    src = inspect.getsource(email_poller)
    # The dedup pattern uses a set comprehension and a list-comp
    # filter.
    assert "seen = set(inbox_ids)" in src
    assert "if m not in seen" in src
