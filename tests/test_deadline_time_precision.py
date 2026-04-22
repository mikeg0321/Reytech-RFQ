"""GRILL-Q3: deadline-time-precision (due_time + hours-granular urgency).

Covers:
- queue_helpers.normalize_queue_item surfaces due_time / _hours_left / _time_explicit
- validation.validate_header_field accepts due_time for save-field endpoints
- _time_explicit flips False when only due_date is set (triggers ⚠ ASSUMED badge)
"""


def test_queue_helper_surfaces_due_time_and_hours_left():
    from src.core.queue_helpers import normalize_queue_item

    raw = {
        "solicitation_number": "R26Q0101",
        "due_date": "04/25/2026",
        "due_time": "14:00",
        "_hours_left": 6.5,
        "_urgency": "urgent",
        "_days_left": 0,
    }
    out = normalize_queue_item(raw, "rfq", "rfq_001")
    assert out["due_time"] == "14:00"
    assert out["_hours_left"] == 6.5
    assert out["_time_explicit"] is True


def test_queue_helper_flags_assumed_time_when_only_date_set():
    from src.core.queue_helpers import normalize_queue_item

    raw = {
        "solicitation_number": "R26Q0102",
        "due_date": "04/25/2026",
        "due_time": "",
        "_hours_left": 12.0,
        "_urgency": "urgent",
    }
    out = normalize_queue_item(raw, "rfq", "rfq_002")
    assert out["due_time"] == ""
    assert out["_time_explicit"] is False


def test_validation_accepts_due_time_header_field():
    from src.core.validation import validate_header_field

    # validate_header_field returns (value, error). due_time must be accepted
    # (no error) so the save-field endpoint on pc/rfq detail can persist it.
    val, err = validate_header_field("due_time", "14:00")
    assert err is None
    assert val == "14:00"
    val, err = validate_header_field("due_time", "")
    assert err is None
    assert val == ""


def test_queue_helper_handles_missing_hours_left_gracefully():
    """Legacy records without _hours_left still normalize — None passes through."""
    from src.core.queue_helpers import normalize_queue_item

    raw = {"solicitation_number": "R26Q0103", "due_date": "04/25/2026"}
    out = normalize_queue_item(raw, "rfq", "rfq_003")
    assert out["_hours_left"] is None
    assert out["_time_explicit"] is False
