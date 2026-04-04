import logging
from typing import Optional, List

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, ConfigDict

from app.db import get_connection, adapt_query as aq
from app.stores.mapping_store import MappingStore
from app.clients.moysklad_client import MoySkladClient
from app.clients.evotor_client import EvotorClient

router = APIRouter(tags=["MoySklad Webhooks"])
log = logging.getLogger("api.webhooks.moysklad")


# ---------------------------------------------------------------------------
# Pydantic модели для webhook МойСклад
# ---------------------------------------------------------------------------

class MoySkladMeta(BaseModel):
    model_config = ConfigDict(extra="allow")

    href: str
    type: Optional[str] = None


class MoySkladWebhookEvent(BaseModel):
    model_config = ConfigDict(extra="allow")

    meta: MoySkladMeta
    updatedFields: Optional[List[str]] = None


class MoySkladWebhook(BaseModel):
    """
    Формат webhook от МойСклад:
    {
        "events": [
            {
                "meta": {
                    "href": "https://api.moysklad.ru/api/remap/1.2/entity/demand/...",
                    "type": "demand"
                },
                "updatedFields": ["positions"]
            }
        ]
    }
    """
    model_config = ConfigDict(extra="allow")

    events: List[MoySkladWebhookEvent]


# ---------------------------------------------------------------------------
# Вспомогательные функции
# ---------------------------------------------------------------------------

def _load_tenant(tenant_id: str) -> dict:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(aq("SELECT * FROM tenants WHERE id = ?"), (tenant_id,))
    row = cur.fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="Tenant not found")
    return dict(row)


def _extract_ms_id_from_href(href: str) -> Optional[str]:
    """Извлекает UUID из href МойСклад."""
    if not href:
        return None
    parts = href.rstrip("/").split("/")
    return parts[-1] if parts else None


def _get_document_positions(ms_client: MoySkladClient, doc_type: str, doc_id: str) -> List[str]:
    """
    Получает список ms_product_id из позиций документа МойСклад.
    Поддерживает: demand, supply, inventory, loss, enter.
    При неуспешном HTTP-запросе бросает исключение — не маскирует ошибку под пустой список.
    """
    import requests

    url = f"{ms_client.BASE_URL}/entity/{doc_type}/{doc_id}/positions"
    r = requests.get(url, headers=ms_client._headers(), timeout=15)

    if not r.ok:
        log.error(
            "Failed to fetch positions doc_type=%s doc_id=%s status=%s body=%s",
            doc_type, doc_id, r.status_code, r.text,
        )
        r.raise_for_status()

    rows = r.json().get("rows", [])
    product_ids = []

    for row in rows:
        assortment = row.get("assortment", {})
        meta = assortment.get("meta", {})
        href = meta.get("href", "")
        ms_id = _extract_ms_id_from_href(href)
        if ms_id:
            product_ids.append(ms_id)

    log.info("Extracted %s product_ids from %s/%s", len(product_ids), doc_type, doc_id)
    return product_ids


def _sync_stock_for_products(
    tenant_id: str,
    ms_product_ids: List[str],
) -> dict:
    """
    Синхронизирует остатки для списка товаров МойСклад → Эвотор.
    Возвращает статистику.
    """
    store = MappingStore()
    ms_client = MoySkladClient(tenant_id)
    evotor_client = EvotorClient(tenant_id)

    synced = 0
    skipped = 0
    failed = 0
    errors = []

    for ms_id in ms_product_ids:
        evotor_id = store.get_by_ms_id(
            tenant_id=tenant_id,
            entity_type="product",
            ms_id=ms_id,
        )

        if not evotor_id:
            log.warning(
                "No mapping for ms_id=%s tenant_id=%s — skipping stock sync",
                ms_id, tenant_id,
            )
            skipped += 1
            continue

        try:
            quantity = ms_client.get_product_stock(ms_id)
        except Exception as e:
            log.error("Failed to get stock ms_id=%s err=%s", ms_id, e)
            failed += 1
            errors.append({"ms_id": ms_id, "error": str(e)})
            continue

        try:
            evotor_client.update_product_stock(evotor_id, quantity)
            log.info(
                "Stock synced ms_id=%s evotor_id=%s quantity=%s",
                ms_id, evotor_id, quantity,
            )
            synced += 1
        except Exception as e:
            log.error("Failed to update Evotor stock evotor_id=%s err=%s", evotor_id, e)
            failed += 1
            errors.append({"ms_id": ms_id, "evotor_id": evotor_id, "error": str(e)})

    return {
        "synced": synced,
        "skipped": skipped,
        "failed": failed,
        "errors": errors,
    }


# ---------------------------------------------------------------------------
# Webhook endpoint
# ---------------------------------------------------------------------------

# Поддерживаемые типы документов → триггеры изменения остатков
STOCK_TRIGGER_TYPES = {
    "demand",    # Отгрузка — остатки уменьшились
    "supply",    # Приёмка — остатки увеличились
    "inventory", # Инвентаризация — остатки скорректированы
    "loss",      # Списание — остатки уменьшились
    "enter",     # Оприходование — остатки увеличились
}


@router.post("/webhooks/moysklad/{tenant_id}")
async def moysklad_webhook(tenant_id: str, body: MoySkladWebhook):
    """
    Принимает webhook от МойСклад при изменении документов.

    При создании/изменении отгрузки, приёмки, инвентаризации, списания или оприходования —
    автоматически синхронизирует остатки затронутых товаров в Эвотор.
    """
    tenant = _load_tenant(tenant_id)

    if not tenant.get("sync_completed_at"):
        log.warning("MoySklad webhook received but sync not completed tenant_id=%s", tenant_id)
        return {"status": "skipped", "reason": "initial sync not completed"}

    total_synced = 0
    total_skipped = 0
    total_failed = 0
    processed_docs = []

    ms_client = MoySkladClient(tenant_id)

    for event in body.events:
        href = event.meta.href
        doc_type = event.meta.type or _extract_doc_type_from_href(href)
        doc_id = _extract_ms_id_from_href(href)

        if not doc_type or doc_type not in STOCK_TRIGGER_TYPES:
            log.info("Skipping non-stock doc_type=%s href=%s", doc_type, href)
            continue

        if not doc_id:
            log.warning("Cannot extract doc_id from href=%s", href)
            continue

        log.info(
            "Processing MoySklad webhook doc_type=%s doc_id=%s tenant_id=%s",
            doc_type, doc_id, tenant_id,
        )

        try:
            ms_product_ids = _get_document_positions(ms_client, doc_type, doc_id)
        except Exception as e:
            log.error(
                "Cannot fetch positions doc_type=%s doc_id=%s tenant_id=%s err=%s",
                doc_type, doc_id, tenant_id, e,
            )
            total_failed += 1
            processed_docs.append({
                "doc_type": doc_type,
                "doc_id": doc_id,
                "error": str(e),
                "synced": 0,
                "failed": 0,
            })
            continue

        if not ms_product_ids:
            log.info("No products found in doc_type=%s doc_id=%s", doc_type, doc_id)
            continue

        result = _sync_stock_for_products(tenant_id, ms_product_ids)

        total_synced += result["synced"]
        total_skipped += result["skipped"]
        total_failed += result["failed"]
        processed_docs.append({
            "doc_type": doc_type,
            "doc_id": doc_id,
            "products": len(ms_product_ids),
            "synced": result["synced"],
            "failed": result["failed"],
        })

    log.info(
        "MoySklad webhook processed tenant_id=%s docs=%s synced=%s failed=%s",
        tenant_id, len(processed_docs), total_synced, total_failed,
    )

    return {
        "status": "ok" if total_failed == 0 else "partial",
        "docs_processed": len(processed_docs),
        "synced": total_synced,
        "skipped": total_skipped,
        "failed": total_failed,
        "details": processed_docs,
    }


def _extract_doc_type_from_href(href: str) -> Optional[str]:
    """Извлекает тип документа из href если не передан в meta.type."""
    if not href:
        return None
    parts = href.rstrip("/").split("/")
    try:
        entity_idx = parts.index("entity")
        return parts[entity_idx + 1]
    except (ValueError, IndexError):
        return None