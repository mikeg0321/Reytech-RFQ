"""
scprs_schedule.py — SCPRS Update Times & Adaptive Check Schedule

SCPRS (California PeopleSoft procurement system) updates daily Mon-Fri.
PeopleSoft batch jobs run overnight; data is available by early morning PST.

Schedule phases for sent quotes/RFQs:
  Phase 1 (biz days 1-2):  Check once per day at first SCPRS window
  Phase 2 (biz days 5-45): Check 3x/day at all SCPRS windows
  Phase 3 (day 45+):       Expire — stop checking indefinitely

Config:
  SCPRS_UPDATE_TIMES env var: comma-separated HH:MM in Pacific time
  Default: "07:00,09:30,12:00,17:00"
    7:00am  — catch overnight batch results
    9:30am  — catch early morning manual entries
    12:00pm — catch late morning activity
    5:00pm  — catch afternoon activity
"""

import logging
import os
from datetime import datetime, timedelta, timezone, time as dt_time
from typing import Optional

log = logging.getLogger("scprs_schedule")

# ── Pacific timezone offset (simplified — handles PST/PDT) ──────────────────

def _pacific_now() -> datetime:
    """Current time in US/Pacific (PST/PDT aware via zoneinfo)."""
    from zoneinfo import ZoneInfo
    return datetime.now(ZoneInfo("America/Los_Angeles")).replace(tzinfo=None)


# ── Configuration ────────────────────────────────────────────────────────────

def _parse_update_times(env_val: str) -> list[dt_time]:
    """Parse SCPRS_UPDATE_TIMES env var. Returns sorted list of time objects."""
    times = []
    for part in env_val.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            h, m = part.split(":")
            t = dt_time(int(h), int(m))
            times.append(t)
        except (ValueError, TypeError):
            log.warning("SCPRS_SCHEDULE: Invalid time '%s' in SCPRS_UPDATE_TIMES, skipping", part)

    # Guard: clamp to 2-6 times per day
    if len(times) < 2:
        log.warning("SCPRS_SCHEDULE: Need at least 2 update times, got %d. Using defaults.", len(times))
        return _DEFAULT_TIMES
    if len(times) > 6:
        log.warning("SCPRS_SCHEDULE: Max 6 update times, got %d. Truncating.", len(times))
        times = times[:6]

    return sorted(times)


_DEFAULT_TIMES = [dt_time(7, 0), dt_time(9, 30), dt_time(12, 0), dt_time(17, 0)]

_env_times = os.environ.get("SCPRS_UPDATE_TIMES", "").strip()
if _env_times:
    SCPRS_UPDATE_TIMES_PT = _parse_update_times(_env_times)
    log.info("SCPRS_SCHEDULE: Using custom update times: %s",
             [t.strftime("%H:%M") for t in SCPRS_UPDATE_TIMES_PT])
else:
    SCPRS_UPDATE_TIMES_PT = _DEFAULT_TIMES

# Phase config
DAILY_CHECK_PHASE_DAYS = int(os.environ.get("AWARD_DAILY_PHASE_DAYS", "2"))
EXPIRY_DAYS = int(os.environ.get("AWARD_EXPIRY_DAYS", "45"))
CHECK_WINDOW_MINUTES = 15  # Time window around SCPRS update to trigger check
MAX_SCPRS_PER_RUN = int(os.environ.get("SCPRS_MAX_SEARCHES", "20"))
MAX_SCPRS_PER_HOUR = 45  # Hard ceiling — halt if exceeded

log.info("SCPRS_SCHEDULE: times=%s daily_phase=%d_biz_days expiry=%d_days window=%dmin",
         [t.strftime("%H:%M") for t in SCPRS_UPDATE_TIMES_PT],
         DAILY_CHECK_PHASE_DAYS, EXPIRY_DAYS, CHECK_WINDOW_MINUTES)


# ── Business Day Utilities ──────────────────────────────────────────────────

def business_days_since(reference_date: datetime, from_date: datetime = None) -> int:
    """Count business days (Mon-Fri) between reference_date and from_date (default: now PT).

    Args:
        reference_date: The starting date to count from.
        from_date: End date. Defaults to current Pacific time.

    Returns:
        Number of business days elapsed.
    """
    if from_date is None:
        from_date = _pacific_now()

    # Strip timezone info for comparison
    ref = reference_date.replace(tzinfo=None) if reference_date.tzinfo else reference_date
    end = from_date.replace(tzinfo=None) if from_date.tzinfo else from_date

    if ref > end:
        return 0

    count = 0
    current = ref
    while current.date() < end.date():
        current += timedelta(days=1)
        if current.weekday() < 5:  # Mon=0 ... Fri=4
            count += 1
    return count


def next_business_day(from_date: datetime = None) -> datetime:
    """Return the next business day (Mon-Fri) at 7:00 AM PT."""
    d = from_date or _pacific_now()
    d = d.replace(hour=7, minute=0, second=0, microsecond=0)
    d += timedelta(days=1)
    while d.weekday() >= 5:  # Skip weekends
        d += timedelta(days=1)
    return d


# ── Check Schedule Logic ───────────────────────────────────────────────────

def is_scprs_check_time(now: datetime = None) -> bool:
    """True if current Pacific time is within CHECK_WINDOW_MINUTES of an SCPRS update time.

    Used by the award tracker loop to decide whether to run a check cycle.
    """
    now = now or _pacific_now()
    now_minutes = now.hour * 60 + now.minute

    for update_time in SCPRS_UPDATE_TIMES_PT:
        target_minutes = update_time.hour * 60 + update_time.minute
        delta = abs(now_minutes - target_minutes)
        if delta <= CHECK_WINDOW_MINUTES:
            return True
    return False


def current_scprs_window(now: datetime = None) -> Optional[str]:
    """Return the current SCPRS window label (e.g., '07:00') if in window, else None."""
    now = now or _pacific_now()
    now_minutes = now.hour * 60 + now.minute

    for update_time in SCPRS_UPDATE_TIMES_PT:
        target_minutes = update_time.hour * 60 + update_time.minute
        if abs(now_minutes - target_minutes) <= CHECK_WINDOW_MINUTES:
            return update_time.strftime("%H:%M")
    return None


def seconds_until_next_window(now: datetime = None) -> int:
    """Calculate seconds until the next SCPRS update window.

    Used by the tracker loop to sleep efficiently.
    Returns seconds to sleep (minimum 60).
    On weekdays: capped at 8 hours (intra-day sleep between windows).
    On weekends/Friday night: full sleep until Monday morning (no cap).
    """
    now = now or _pacific_now()
    now_minutes = now.hour * 60 + now.minute

    candidates = []
    for update_time in SCPRS_UPDATE_TIMES_PT:
        target_minutes = update_time.hour * 60 + update_time.minute
        # Minutes until this window opens (target - window margin)
        window_start = target_minutes - CHECK_WINDOW_MINUTES
        delta = window_start - now_minutes
        if delta < 0:
            delta += 24 * 60  # Wrap to next day
        candidates.append(delta)

    if not candidates:
        return 3600  # Fallback: 1 hour

    min_minutes = min(candidates)

    # Check if all remaining windows today have passed (all candidates wrapped to next day)
    last_window_minutes = max(t.hour * 60 + t.minute for t in SCPRS_UPDATE_TIMES_PT)
    past_all_windows_today = now_minutes > last_window_minutes + CHECK_WINDOW_MINUTES

    # If it's a weekend, add days until Monday
    days_to_add = 0
    if now.weekday() == 5:  # Saturday
        days_to_add = 2
    elif now.weekday() == 6:  # Sunday
        days_to_add = 1
    # Friday past all windows: next window would be Saturday 7am — skip to Monday
    elif now.weekday() == 4 and past_all_windows_today:
        days_to_add = 2

    total_seconds = (min_minutes * 60) + (days_to_add * 86400)

    # Clamp: min 60s. Only cap intra-day sleeps (no weekend days to skip).
    # Weekend sleeps need the full duration to reach Monday morning.
    if days_to_add > 0:
        return max(60, total_seconds)
    else:
        return max(60, min(total_seconds, 8 * 3600))


def get_check_phase(sent_at: datetime, now: datetime = None) -> str:
    """Determine which monitoring phase a record is in.

    Args:
        sent_at: When the quote/RFQ was sent.
        now: Current time (default: Pacific now).

    Returns:
        'daily'     — biz days 1-4, check once per day
        'intensive' — biz days 5-45, check 3x per day
        'expired'   — day 45+, stop checking
    """
    now = now or _pacific_now()
    biz_days = business_days_since(sent_at, now)
    total_days = (now - sent_at.replace(tzinfo=None)).days if sent_at else 0

    if total_days >= EXPIRY_DAYS:
        return "expired"
    elif biz_days <= DAILY_CHECK_PHASE_DAYS:
        return "daily"
    else:
        return "intensive"


def should_check_record(sent_at: datetime, last_checked: datetime = None,
                        last_checked_window: str = "", now: datetime = None) -> tuple[bool, str]:
    """Determine if a record should be checked in this cycle.

    Args:
        sent_at: When the quote/RFQ was sent.
        last_checked: When the record was last checked.
        last_checked_window: The SCPRS window label of the last check (e.g., '07:00').
        now: Current time (default: Pacific now).

    Returns:
        (should_check: bool, reason: str)
        reason explains why we're checking or skipping.
    """
    now = now or _pacific_now()
    phase = get_check_phase(sent_at, now)

    if phase == "expired":
        return False, f"expired (>{EXPIRY_DAYS} days since sent)"

    # Must be a business day
    if now.weekday() >= 5:
        return False, "weekend — SCPRS doesn't update"

    # Must be in an SCPRS window
    window = current_scprs_window(now)
    if not window:
        return False, "not in SCPRS update window"

    if phase == "daily":
        # Check once per day — skip if already checked today
        if last_checked and last_checked.date() == now.date():
            return False, f"daily phase: already checked today at {last_checked.strftime('%H:%M')}"
        biz_days = business_days_since(sent_at, now)
        return True, f"daily phase (biz day {biz_days}/{DAILY_CHECK_PHASE_DAYS})"

    elif phase == "intensive":
        # Check 3x/day — skip if already checked in this SCPRS window
        if last_checked_window == window and last_checked and last_checked.date() == now.date():
            return False, f"intensive phase: already checked in {window} window today"
        biz_days = business_days_since(sent_at, now)
        total_days = (now - sent_at.replace(tzinfo=None)).days
        return True, f"intensive phase (biz day {biz_days}, calendar day {total_days}/{EXPIRY_DAYS}, window {window})"

    return False, f"unknown phase: {phase}"


def get_next_check_time(sent_at: datetime, last_check: datetime = None,
                        check_count: int = 0, now: datetime = None) -> datetime:
    """Calculate when this record should next be checked.

    Used by post_send_pipeline to set check_after on new queue entries.

    Args:
        sent_at: When the quote/RFQ was sent.
        last_check: When the record was last checked (None = never).
        check_count: How many times it's been checked so far.
        now: Current time (default: Pacific now).

    Returns:
        datetime of next check, or None if expired.
    """
    now = now or _pacific_now()
    phase = get_check_phase(sent_at, now)

    if phase == "expired":
        return None

    if phase == "daily":
        # Next check: next business day at first SCPRS window
        nbd = next_business_day(now)
        first_window = SCPRS_UPDATE_TIMES_PT[0]
        return nbd.replace(hour=first_window.hour, minute=first_window.minute, second=0)

    elif phase == "intensive":
        # Next check: next SCPRS window (could be today or next biz day)
        now_minutes = now.hour * 60 + now.minute
        for update_time in SCPRS_UPDATE_TIMES_PT:
            target_minutes = update_time.hour * 60 + update_time.minute
            if target_minutes > now_minutes + CHECK_WINDOW_MINUTES:
                # Later today
                return now.replace(hour=update_time.hour, minute=update_time.minute, second=0)

        # All windows passed today — next business day, first window
        nbd = next_business_day(now)
        first_window = SCPRS_UPDATE_TIMES_PT[0]
        return nbd.replace(hour=first_window.hour, minute=first_window.minute, second=0)

    return None


# ── Rate Limiting ───────────────────────────────────────────────────────────

_hourly_search_count = 0
_hourly_reset_time = None


def check_rate_limit(searches_this_run: int = 0) -> tuple[bool, str]:
    """Check if we're within SCPRS rate limits.

    Returns:
        (ok: bool, message: str)
    """
    global _hourly_search_count, _hourly_reset_time

    now = _pacific_now()

    # Reset hourly counter if it's been more than an hour
    if _hourly_reset_time is None or (now - _hourly_reset_time).total_seconds() > 3600:
        _hourly_search_count = 0
        _hourly_reset_time = now

    if searches_this_run >= MAX_SCPRS_PER_RUN:
        return False, f"per-run limit reached ({searches_this_run}/{MAX_SCPRS_PER_RUN})"

    if _hourly_search_count + searches_this_run >= MAX_SCPRS_PER_HOUR:
        return False, f"hourly limit reached ({_hourly_search_count + searches_this_run}/{MAX_SCPRS_PER_HOUR})"

    return True, "ok"


def record_searches(count: int):
    """Record N SCPRS searches for rate limiting."""
    global _hourly_search_count
    _hourly_search_count += count
    log.debug("SCPRS_RATE: %d searches this hour (limit: %d)", _hourly_search_count, MAX_SCPRS_PER_HOUR)
