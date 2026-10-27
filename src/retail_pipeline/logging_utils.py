import json
import logging

logger = logging.getLogger("retail_pipeline")


def log_event(event: str, **details) -> None:
    """Write a structured application log event."""

    payload = {
        "event": event,
        **details,
    }

    logger.info(
        json.dumps(
            payload,
            default=str,
            sort_keys=True,
        )
    )