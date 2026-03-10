from sale_handler import handle_sale


def dispatch_event(row):
    event_type = row["event_type"]

    if event_type == "sale":
        return handle_sale(row)

    raise ValueError(f"Unsupported event_type: {event_type}")