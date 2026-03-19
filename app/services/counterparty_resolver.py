import logging

from app.clients.moysklad_client import MoySkladClient

log = logging.getLogger("counterparty_resolver")


def resolve_counterparty_for_sale(payload: dict, tenant_id: str, default_ms_agent_id: str | None) -> tuple[str | None, str]:
    """
    Возвращает (ms_agent_id, resolution_source).

    resolution_source:
      - default_agent
      - found_by_email
      - found_by_phone
      - created_counterparty
      - default_agent_on_error
    """
    customer = payload.get("customer") or {}
    if not isinstance(customer, dict):
        return default_ms_agent_id, "default_agent"

    name = customer.get("name")
    phone = customer.get("phone")
    email = customer.get("email")
    inn = customer.get("inn")

    if not any(v not in (None, "") for v in (name, phone, email, inn)):
        return default_ms_agent_id, "default_agent"

    client = MoySkladClient(tenant_id)

    try:
        if email:
            row = client.find_counterparty_by_email(email)
            if row and row.get("id"):
                return row["id"], "found_by_email"

        if phone:
            row = client.find_counterparty_by_phone(phone)
            if row and row.get("id"):
                return row["id"], "found_by_phone"

        created = client.create_counterparty(
            name=name,
            phone=phone,
            email=email,
            inn=inn,
        )
        created_id = created.get("id")
        if created_id:
            return created_id, "created_counterparty"

        if default_ms_agent_id:
            log.warning(
                "Counterparty create returned no id tenant_id=%s customer=%s, fallback to default agent",
                tenant_id,
                customer,
            )
            return default_ms_agent_id, "default_agent_on_error"

        raise Exception("Counterparty create returned no id")

    except Exception as e:
        if default_ms_agent_id:
            log.warning(
                "Counterparty resolve failed tenant_id=%s err=%s customer=%s, fallback to default agent",
                tenant_id,
                e,
                customer,
            )
            return default_ms_agent_id, "default_agent_on_error"
        raise
