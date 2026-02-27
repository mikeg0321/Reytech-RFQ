"""
validators.py — Input validation framework for API endpoints.
Sprint 6 (M1): Reusable validators for common patterns.

Usage:
    from src.core.validators import validate_required, validate_email, sanitize_string, ValidationError

    @bp.route("/api/contact", methods=["POST"])
    def create_contact():
        data = request.get_json(force=True, silent=True) or {}
        try:
            name = validate_required(data, "name", max_len=200)
            email = validate_email(data.get("email", ""))
            notes = sanitize_string(data.get("notes", ""), max_len=2000)
        except ValidationError as e:
            return jsonify({"ok": False, "error": str(e)}), 400
"""
import re
import logging

log = logging.getLogger("reytech.validation")


class ValidationError(ValueError):
    """Raised when input validation fails."""
    pass


def validate_required(data: dict, field: str, max_len: int = 500) -> str:
    """Validate a required string field exists and is non-empty."""
    val = data.get(field)
    if val is None or (isinstance(val, str) and not val.strip()):
        raise ValidationError(f"'{field}' is required")
    if isinstance(val, str):
        val = val.strip()
        if len(val) > max_len:
            raise ValidationError(f"'{field}' exceeds max length ({max_len})")
    return val


def validate_optional(data: dict, field: str, default="", max_len: int = 500):
    """Validate an optional string field. Returns default if missing."""
    val = data.get(field)
    if val is None:
        return default
    if isinstance(val, str):
        val = val.strip()
        if len(val) > max_len:
            raise ValidationError(f"'{field}' exceeds max length ({max_len})")
    return val


def validate_email(email: str) -> str:
    """Basic email validation. Returns cleaned email or raises."""
    if not email:
        return ""
    email = email.strip().lower()
    if email and not re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email):
        raise ValidationError(f"Invalid email format: {email[:50]}")
    return email


def validate_number(data: dict, field: str, min_val=None, max_val=None,
                    default=None, allow_none=True) -> float:
    """Validate a numeric field."""
    val = data.get(field)
    if val is None:
        if not allow_none and default is None:
            raise ValidationError(f"'{field}' is required")
        return default
    try:
        num = float(val)
    except (TypeError, ValueError):
        raise ValidationError(f"'{field}' must be a number")
    if min_val is not None and num < min_val:
        raise ValidationError(f"'{field}' must be >= {min_val}")
    if max_val is not None and num > max_val:
        raise ValidationError(f"'{field}' must be <= {max_val}")
    return num


def validate_enum(data: dict, field: str, allowed: list, default=None) -> str:
    """Validate a field against allowed values."""
    val = data.get(field, default)
    if val is None and default is None:
        raise ValidationError(f"'{field}' is required")
    if val is not None and val not in allowed:
        raise ValidationError(f"'{field}' must be one of: {', '.join(str(a) for a in allowed)}")
    return val


def sanitize_string(val: str, max_len: int = 2000) -> str:
    """Clean a string: strip whitespace, limit length, remove null bytes."""
    if not isinstance(val, str):
        return ""
    val = val.strip().replace('\x00', '')
    if len(val) > max_len:
        val = val[:max_len]
    return val


def validate_id(val: str, prefix: str = None) -> str:
    """Validate an ID string (alphanumeric + hyphens)."""
    if not val or not isinstance(val, str):
        raise ValidationError("ID is required")
    val = val.strip()
    if not re.match(r'^[a-zA-Z0-9_-]+$', val):
        raise ValidationError(f"Invalid ID format: {val[:30]}")
    if prefix and not val.startswith(prefix):
        raise ValidationError(f"ID must start with '{prefix}'")
    return val


def validate_date(val: str, field: str = "date") -> str:
    """Validate ISO date format (YYYY-MM-DD)."""
    if not val:
        return ""
    val = val.strip()
    if not re.match(r'^\d{4}-\d{2}-\d{2}$', val):
        raise ValidationError(f"'{field}' must be YYYY-MM-DD format")
    return val
