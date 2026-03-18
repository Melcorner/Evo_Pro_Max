import logging

from app.handlers.sale_handler import handle_sale

log = logging.getLogger("dispatcher")


def dispatch_event(row):
    event_type = row["event_type"]

    if event_type == "sale":
        return handle_sale(row)

    if event_type == "product":
        log.warning(f"event_type=product not implemented yet — skipping")
        return "skipped:product"

    raise ValueError(f"Unsupported event_type: {event_type}")