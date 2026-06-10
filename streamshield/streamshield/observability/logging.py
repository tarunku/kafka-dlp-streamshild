"""
StreamShield structured logging.

All SDK components use named loggers from this module. Applications can configure
the root 'streamshield' logger to control output format, level, and destination.

Named loggers:
    streamshield.auth       — token refresh, Secret Manager
    streamshield.schema     — schema fetch, cache, registration
    streamshield.dlp        — tokenization, de-tokenization, batch stats
    streamshield.producer   — send, flush, delivery callbacks
    streamshield.consumer   — poll, commit, process loop
    streamshield.dlq        — DLQ routing events
    streamshield.topic      — topic creation, validation

The SDK never calls print(). All output goes through these loggers.

Usage (application side):
    import logging
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    # or for JSON output:
    # configure your preferred JSON formatter on the 'streamshield' logger
"""

import logging

# ── Named loggers — one per SDK subsystem ────────────────────────────────────

def get_logger(name: str) -> logging.Logger:
    """Return a logger namespaced under 'streamshield.<name>'."""
    return logging.getLogger(f"streamshield.{name}")


# Convenience pre-created loggers used by each module
auth_logger     = get_logger("auth")
schema_logger   = get_logger("schema")
dlp_logger      = get_logger("dlp")
producer_logger = get_logger("producer")
consumer_logger = get_logger("consumer")
dlq_logger      = get_logger("dlq")
topic_logger    = get_logger("topic")


def configure_json_logging(level: int = logging.INFO) -> None:
    """
    Configure the root 'streamshield' logger with a simple JSON-like formatter.
    Call this at application startup if you want structured log output.

    For production, use your organisation's log aggregation formatter instead.
    """
    import json

    class JsonFormatter(logging.Formatter):
        def format(self, record: logging.LogRecord) -> str:
            log_entry = {
                "level": record.levelname,
                "logger": record.name,
                "message": record.getMessage(),
            }
            if record.exc_info:
                log_entry["exception"] = self.formatException(record.exc_info)
            return json.dumps(log_entry)

    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())

    root = logging.getLogger("streamshield")
    root.setLevel(level)
    root.addHandler(handler)
    root.propagate = False
