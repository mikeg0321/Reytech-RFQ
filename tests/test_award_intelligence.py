"""
End-to-end audit test for Award Intelligence system.
Tests: schedule, classification, windows, weekends, rate limits.
"""
import sys
from datetime import datetime, timedelta, time as dt_time

from src.core.scprs_schedule import (
    SCPRS_UPDATE_TIMES_PT, EXPIRY_DAYS, DAILY_CHECK_PHASE_DAYS,
    MAX_SCPRS_PER_RUN, MAX_SCPRS_PER_HOUR,
    business_days_since, get_check_phase, is_scprs_check_time,
    current_scprs_window, should_check_record, seconds_until_next_window,
    get_next_check_time, next_business_day, check_rate_limit,
)
from src.agents.award_tracker import _classify_loss_reason

errors = 0
tests = 0


def check(name, condition, detail=""):
    global errors, tests
    tests += 1
    if condition:
        print(f"  PASS: {name}")
    else:
        print(f"  FAIL: {name} {detail}")
        errors += 1


print("=== FULL E2E AUDIT TEST SUITE ===\n")

# ── Config ──────────────────────────────────────────────────────────────
print("-- Config --")
check("3 update times", len(SCPRS_UPDATE_TIMES_PT) == 3)
check("Times 7/12/17",
      SCPRS_UPDATE_TIMES_PT == [dt_time(7, 0), dt_time(12, 0), dt_time(17, 0)])
check("Expiry 45 days", EXPIRY_DAYS == 45)
check("Daily phase 4 days", DAILY_CHECK_PHASE_DAYS == 4)
check("Max per run 15", MAX_SCPRS_PER_RUN == 15)
check("Max per hour 45", MAX_SCPRS_PER_HOUR == 45)

# ── Business Days ────────────────────────────────────────────────────────
print("\n-- Business Days --")
check("Mon-Fri = 4", business_days_since(datetime(2026, 3, 23, 9, 0), datetime(2026, 3, 27, 17, 0)) == 4)
check("Same day = 0", business_days_since(datetime(2026, 3, 26, 9, 0), datetime(2026, 3, 26, 17, 0)) == 0)
check("Over weekend = 5", business_days_since(datetime(2026, 3, 20, 9, 0), datetime(2026, 3, 27, 17, 0)) == 5)
check("2 weeks = 10", business_days_since(datetime(2026, 3, 13, 9, 0), datetime(2026, 3, 27, 17, 0)) == 10)
check("Future = 0", business_days_since(datetime(2026, 3, 30, 9, 0), datetime(2026, 3, 27, 17, 0)) == 0)

# ── Phases ───────────────────────────────────────────────────────────────
print("\n-- Phases --")
check("1d = daily", get_check_phase(datetime(2026, 3, 26, 10, 0), datetime(2026, 3, 27, 10, 0)) == "daily")
check("4d = daily", get_check_phase(datetime(2026, 3, 23, 10, 0), datetime(2026, 3, 27, 10, 0)) == "daily")
check("5d = intensive", get_check_phase(datetime(2026, 3, 20, 10, 0), datetime(2026, 3, 27, 10, 0)) == "intensive")
check("12d = intensive", get_check_phase(datetime(2026, 3, 12, 10, 0), datetime(2026, 3, 27, 10, 0)) == "intensive")
check("46 cal = expired", get_check_phase(datetime(2026, 2, 9, 10, 0), datetime(2026, 3, 27, 10, 0)) == "expired")
check("44 cal = intensive", get_check_phase(datetime(2026, 2, 11, 10, 0), datetime(2026, 3, 27, 10, 0)) == "intensive")

# ── Windows ──────────────────────────────────────────────────────────────
print("\n-- Windows --")
check("7:05 in", is_scprs_check_time(datetime(2026, 3, 26, 7, 5)))
check("6:46 in", is_scprs_check_time(datetime(2026, 3, 26, 6, 46)))
check("7:16 out", not is_scprs_check_time(datetime(2026, 3, 26, 7, 16)))
check("12:00 in", is_scprs_check_time(datetime(2026, 3, 26, 12, 0)))
check("12:14 in", is_scprs_check_time(datetime(2026, 3, 26, 12, 14)))
check("12:16 out", not is_scprs_check_time(datetime(2026, 3, 26, 12, 16)))
check("17:00 in", is_scprs_check_time(datetime(2026, 3, 26, 17, 0)))
check("9:00 out", not is_scprs_check_time(datetime(2026, 3, 26, 9, 0)))
check("label 7am", current_scprs_window(datetime(2026, 3, 26, 7, 5)) == "07:00")
check("label noon", current_scprs_window(datetime(2026, 3, 26, 12, 10)) == "12:00")
check("label 5pm", current_scprs_window(datetime(2026, 3, 26, 17, 3)) == "17:00")
check("label none 9am", current_scprs_window(datetime(2026, 3, 26, 9, 0)) is None)

# ── should_check_record ─────────────────────────────────────────────────
print("\n-- Adaptive Schedule --")
check("Daily unchecked in window",
      should_check_record(datetime(2026, 3, 26, 10, 0), now=datetime(2026, 3, 27, 7, 5))[0])
check("Daily checked today",
      not should_check_record(datetime(2026, 3, 26, 10, 0),
                              last_checked=datetime(2026, 3, 27, 7, 0),
                              now=datetime(2026, 3, 27, 7, 5))[0])
check("Daily checked yesterday",
      should_check_record(datetime(2026, 3, 26, 10, 0),
                          last_checked=datetime(2026, 3, 26, 7, 0),
                          now=datetime(2026, 3, 27, 7, 5))[0])
check("Intensive diff window",
      should_check_record(datetime(2026, 3, 12, 10, 0),
                          last_checked=datetime(2026, 3, 26, 7, 5),
                          last_checked_window="07:00",
                          now=datetime(2026, 3, 26, 12, 5))[0])
check("Intensive same window",
      not should_check_record(datetime(2026, 3, 12, 10, 0),
                              last_checked=datetime(2026, 3, 26, 12, 3),
                              last_checked_window="12:00",
                              now=datetime(2026, 3, 26, 12, 5))[0])
check("Weekend rejected",
      not should_check_record(datetime(2026, 3, 26, 10, 0),
                              now=datetime(2026, 3, 28, 7, 5))[0])
check("Out of window rejected",
      not should_check_record(datetime(2026, 3, 26, 10, 0),
                              now=datetime(2026, 3, 27, 9, 0))[0])
check("Expired rejected",
      not should_check_record(datetime(2026, 1, 15, 10, 0),
                              now=datetime(2026, 3, 27, 7, 5))[0])

# ── Sleep Duration (weekend fix) ────────────────────────────────────────
print("\n-- Sleep Duration (weekend bug fix) --")
thu_8 = seconds_until_next_window(datetime(2026, 3, 26, 8, 0))
check(f"Thu 8am: {thu_8}s ({thu_8/3600:.1f}h) <= 8h", 60 <= thu_8 <= 8 * 3600)

sat_12 = seconds_until_next_window(datetime(2026, 3, 28, 12, 0))
check(f"Sat noon: {sat_12}s ({sat_12/3600:.1f}h) > 8h", sat_12 > 8 * 3600, f"got {sat_12}s")

sun_9 = seconds_until_next_window(datetime(2026, 3, 29, 9, 0))
check(f"Sun 9am: {sun_9}s ({sun_9/3600:.1f}h) > 8h", sun_9 > 8 * 3600, f"got {sun_9}s")

fri_18 = seconds_until_next_window(datetime(2026, 3, 27, 18, 0))
check(f"Fri 6pm: {fri_18}s ({fri_18/3600:.1f}h) > 8h", fri_18 > 8 * 3600, f"got {fri_18}s")

# ── Rate Limits ─────────────────────────────────────────────────────────
print("\n-- Rate Limits --")
check("0 OK", check_rate_limit(0)[0])
check("14 OK", check_rate_limit(14)[0])
check("15 BLOCKED", not check_rate_limit(15)[0])

# ── Next Check Time ─────────────────────────────────────────────────────
print("\n-- Next Check Time --")
nct = get_next_check_time(datetime(2026, 3, 26, 10, 0), now=datetime(2026, 3, 27, 7, 5))
check("Daily returns future", nct is not None and nct > datetime(2026, 3, 27, 7, 5))

nct2 = get_next_check_time(datetime(2026, 3, 12, 10, 0), now=datetime(2026, 3, 26, 8, 0))
check("Intensive returns noon today", nct2 is not None and nct2.hour == 12)

nct3 = get_next_check_time(datetime(2026, 1, 15, 10, 0), now=datetime(2026, 3, 27, 7, 5))
check("Expired returns None", nct3 is None)

# ── Next Business Day ───────────────────────────────────────────────────
print("\n-- Next Business Day --")
nbd_fri = next_business_day(datetime(2026, 3, 27, 10, 0))
check(f"Fri->Mon {nbd_fri.strftime('%A %m/%d')}", nbd_fri.weekday() == 0 and nbd_fri.day == 30)
nbd_thu = next_business_day(datetime(2026, 3, 26, 10, 0))
check(f"Thu->Fri {nbd_thu.strftime('%A %m/%d')}", nbd_thu.weekday() == 4 and nbd_thu.day == 27)
nbd_sat = next_business_day(datetime(2026, 3, 28, 10, 0))
check(f"Sat->Mon {nbd_sat.strftime('%A %m/%d')}", nbd_sat.weekday() == 0 and nbd_sat.day == 30)

# ── Loss Classification ─────────────────────────────────────────────────
print("\n-- Loss Classification --")
check("mth priority", _classify_loss_reason(5.0, [], [{"x": 1}]) == "margin_too_high")
check("mth over relationship", _classify_loss_reason(-5.0, [], [{"x": 1}]) == "margin_too_high")
check("relationship -2.1%", _classify_loss_reason(-2.1, [], []) == "relationship_incumbent")
check("price_higher -2.0%", _classify_loss_reason(-2.0, [], []) == "price_higher")
check("cost_too_high majority",
      _classify_loss_reason(10.0,
                            [{"matched": True, "our_cost": 100, "winner_unit_price": 80}] * 3,
                            []) == "cost_too_high")
check("price_higher default", _classify_loss_reason(10.0, [], []) == "price_higher")
check("cost needs >50%",
      _classify_loss_reason(10.0, [
          {"matched": True, "our_cost": 100, "winner_unit_price": 80},
          {"matched": True, "our_cost": 50, "winner_unit_price": 90},
      ], []) == "price_higher")

print(f"\n=== {tests} TESTS: {tests - errors} PASSED, {errors} FAILED ===")
if errors:
    sys.exit(1)
