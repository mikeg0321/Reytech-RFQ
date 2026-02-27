"""
structured_log.py — JSON structured logging for Railway/production.
Sprint 5.3 (M5): Machine-parseable log format for log aggregation and alerting.

Usage:
    from src.core.structured_log import setup_structured_logging
    setup_structured_logging()  # Call once at app startup
"""
import json
import logging
import sys
import os
import traceback
from datetime import datetime


class JsonFormatter(logging.Formatter):
    """Format log records as single-line JSON for Railway log aggregation."""

    def format(self, record):
        log_entry = {
            "ts": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }

        # Add module/function context
        if record.funcName and record.funcName != "<module>":
            log_entry["func"] = f"{record.module}.{record.funcName}"

        # Add exception info
        if record.exc_info and record.exc_info[0]:
            log_entry["error"] = {
                "type": record.exc_info[0].__name__,
                "message": str(record.exc_info[1]),
                "trace": traceback.format_exception(*record.exc_info)[-3:]
            }

        # Add extra fields (passed via log.info("msg", extra={...}))
        skip = {"message", "args", "created", "exc_info", "exc_text",
                "filename", "funcName", "levelname", "levelno", "lineno",
                "module", "msecs", "msg", "name", "pathname", "process",
                "processName", "relativeCreated", "stack_info", "thread",
                "threadName", "taskName"}
        for k, v in record.__dict__.items():
            if k not in skip and not k.startswith("_"):
                log_entry[k] = v

        return json.dumps(log_entry, default=str)


def setup_structured_logging(level: str = None):
    """
    Configure structured JSON logging for production.
    Falls back to standard formatting in development.
    """
    env = os.environ.get("RAILWAY_ENVIRONMENT", os.environ.get("FLASK_ENV", "development"))
    is_production = env in ("production", "railway")

    log_level = level or os.environ.get("LOG_LEVEL", "INFO" if is_production else "DEBUG")

    root = logging.getLogger()
    root.setLevel(getattr(logging, log_level.upper(), logging.INFO))

    # Remove existing handlers
    root.handlers.clear()

    handler = logging.StreamHandler(sys.stdout)

    if is_production:
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(logging.Formatter(
            "%(asctime)s [%(name)s] %(levelname)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        ))

    root.addHandler(handler)

    # Quiet noisy libraries
    for lib in ["urllib3", "werkzeug", "httpcore", "httpx", "openai"]:
        logging.getLogger(lib).setLevel(logging.WARNING)

    logging.getLogger("reytech").info(
        "Logging configured: level=%s format=%s env=%s",
        log_level, "json" if is_production else "text", env
    )
