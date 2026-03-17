import logging

from app.handlers.sale_handler import handle_sale

log = logging.getLogger("dispatcher")


def dispatch_event(row):
    event_type = row["event_type"]

    if event_type == "sale":
        return handle_sale(row)

    if event_type in ("product", "stock"):
        log.warning(f"event_type={event_type} not implemented yet — skipping")
        return f"skipped:{event_type}"

    raise ValueError(f"Unsupported event_type: {event_type}")