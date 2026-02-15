"""
Structured logging configuration for Reytech RFQ system.
Import and call setup_logging() once at app startup.
"""
import logging
import logging.handlers
import os
import json
from datetime import datetime

DATA_DIR = os.environ.get("REYTECH_DATA_DIR", os.path.join(os.path.dirname(__file__), "data"))
LOG_DIR = os.path.join(DATA_DIR, "logs")


class JSONFormatter(logging.Formatter):
    """Structured JSON log lines for machine parsing."""
    def format(self, record):
        entry = {
            "ts": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            "module": record.module,
            "func": record.funcName,
            "line": record.lineno,
        }
        if record.exc_info and record.exc_info[0]:
            entry["exception"] = self.formatException(record.exc_info)
        # Merge extra fields
        for key in ("route", "method", "pcid", "rid", "quote_number",
                     "agency", "total", "items", "duration_ms", "user"):
            if hasattr(record, key):
                entry[key] = getattr(record, key)
        return json.dumps(entry, default=str)


class HumanFormatter(logging.Formatter):
    """Readable console format with color."""
    COLORS = {
        "DEBUG": "\033[36m", "INFO": "\033[32m",
        "WARNING": "\033[33m", "ERROR": "\033[31m",
        "CRITICAL": "\033[35m",
    }
    RESET = "\033[0m"

    def format(self, record):
        color = self.COLORS.get(record.levelname, "")
        ts = datetime.utcnow().strftime("%H:%M:%S")
        return f"{color}{ts} [{record.levelname[0]}] {record.name}: {record.getMessage()}{self.RESET}"


def setup_logging(level=None, json_logs=None):
    """
    Configure logging for the full application.
    
    Args:
        level: Override log level (default: from LOG_LEVEL env or INFO)
        json_logs: Force JSON format (default: True in production, False in dev)
    """
    if level is None:
        level = os.environ.get("LOG_LEVEL", "INFO").upper()
    if json_logs is None:
        json_logs = os.environ.get("RAILWAY_ENVIRONMENT") is not None

    os.makedirs(LOG_DIR, exist_ok=True)

    root = logging.getLogger()
    root.setLevel(getattr(logging, level, logging.INFO))
    root.handlers.clear()

    # Console handler
    console = logging.StreamHandler()
    console.setFormatter(JSONFormatter() if json_logs else HumanFormatter())
    root.addHandler(console)

    # File handler — rotates at 5MB, keeps 5 backups
    try:
        fh = logging.handlers.RotatingFileHandler(
            os.path.join(LOG_DIR, "reytech.log"),
            maxBytes=5_000_000, backupCount=5,
        )
        fh.setFormatter(JSONFormatter())
        root.addHandler(fh)
    except (OSError, PermissionError):
        pass  # skip file logging if dir not writable

    # Quiet noisy libs
    for name in ("urllib3", "werkzeug", "PIL", "reportlab"):
        logging.getLogger(name).setLevel(logging.WARNING)

    logging.getLogger("reytech").info("Logging initialized", extra={"level": level})
