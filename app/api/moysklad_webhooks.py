import logging
from typing import Optional, List

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.db import get_connection
from app.stores.mapping_store import MappingStore
from app.clients.moysklad_client import MoySkladClient
from app.clients.evotor_client import EvotorClient

router = APIRouter()
log = logging.getLogger("api.webhooks.moysklad")


# ---------------------------------------------------------------------------
# Pydantic модели для webhook МойСклад
# ---------------------------------------------------------------------------

class MoySkladMeta(BaseModel):
    href: str
    type: Optional[str] = None

    class Config:
        extra = "allow"


class MoySkladWebhookEvent(BaseModel):
    meta: MoySkladMeta
    updatedFields: Optional[List[str]] = None

    class Config:
        extra = "allow"


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
    events: List[MoySkladWebhookEvent]

    class Config:
        extra = "allow"


# ---------------------------------------------------------------------------
# Вспомогательные функции
# ---------------------------------------------------------------------------

def _load_tenant(tenant_id: str) -> dict:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM tenants WHERE id = ?", (tenant_id,))
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
    Поддерживает: demand, supply, inventory.
    """
    import requests

    url = f"{MoySkladClient.BASE_URL}/entity/{doc_type}/{doc_id}/positions"
    r = requests.get(url, headers=ms_client._headers(), timeout=15)

    if not r.ok:
        log.error(f"Failed to fetch positions doc_type={doc_type} doc_id={doc_id} status={r.status_code}")
        return []

    rows = r.json().get("rows", [])
    product_ids = []

    for row in rows:
        assortment = row.get("assortment", {})
        meta = assortment.get("meta", {})
        href = meta.get("href", "")
        ms_id = _extract_ms_id_from_href(href)
        if ms_id:
            product_ids.append(ms_id)

    log.info(f"Extracted {len(product_ids)} product_ids from {doc_type}/{doc_id}")
    return product_ids


def _sync_stock_for_products(
    tenant_id: str,
    tenant: dict,
    ms_product_ids: List[str]
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
        # Ищем evotor_id через маппинг
        evotor_id = store.get_by_ms_id(
            tenant_id=tenant_id,
            entity_type="product",
            ms_id=ms_id
        )

        if not evotor_id:
            log.warning(f"No mapping for ms_id={ms_id} tenant_id={tenant_id} — skipping stock sync")
            skipped += 1
            continue

        # Получаем остаток из МойСклад
        try:
            quantity = ms_client.get_product_stock(ms_id)
        except Exception as e:
            log.error(f"Failed to get stock ms_id={ms_id} err={e}")
            failed += 1
            errors.append({"ms_id": ms_id, "error": str(e)})
            continue

        # Обновляем остаток в Эвотор
        try:
            evotor_client.update_product_stock(evotor_id, quantity)
            log.info(f"Stock synced ms_id={ms_id} evotor_id={evotor_id} quantity={quantity}")
            synced += 1
        except Exception as e:
            log.error(f"Failed to update Evotor stock evotor_id={evotor_id} err={e}")
            failed += 1
            errors.append({"ms_id": ms_id, "evotor_id": evotor_id, "error": str(e)})

    return {
        "synced": synced,
        "skipped": skipped,
        "failed": failed,
        "errors": errors
    }


# ---------------------------------------------------------------------------
# Webhook endpoint
# ---------------------------------------------------------------------------

# Поддерживаемые типы документов → триггеры изменения остатков
STOCK_TRIGGER_TYPES = {
    "demand",       # Отгрузка — остатки уменьшились
    "supply",       # Приёмка — остатки увеличились
    "inventory",    # Инвентаризация — остатки скорректированы
    "loss",         # Списание — остатки уменьшились
    "enter",        # Оприходование — остатки увеличились
}


@router.post("/webhooks/moysklad/{tenant_id}")
async def moysklad_webhook(tenant_id: str, body: MoySkladWebhook):
    """
    Принимает webhook от МойСклад при изменении документов.

    При создании/изменении отгрузки, приёмки или инвентаризации —
    автоматически синхронизирует остатки затронутых товаров в Эвотор.

    Настройка в МойСклад:
    Настройки → Вебхуки → Создать
    URL: https://{your-domain}/webhooks/moysklad/{tenant_id}
    Сущность: Отгрузка / Приёмка / Инвентаризация
    Событие: Создание, Изменение
    """
    tenant = _load_tenant(tenant_id)

    if not tenant.get("sync_completed_at"):
        log.warning(f"MoySklad webhook received but sync not completed tenant_id={tenant_id}")
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
            log.info(f"Skipping non-stock doc_type={doc_type} href={href}")
            continue

        if not doc_id:
            log.warning(f"Cannot extract doc_id from href={href}")
            continue

        log.info(f"Processing MoySklad webhook doc_type={doc_type} doc_id={doc_id} tenant_id={tenant_id}")

        # Получаем товары из позиций документа
        ms_product_ids = _get_document_positions(ms_client, doc_type, doc_id)

        if not ms_product_ids:
            log.info(f"No products found in doc_type={doc_type} doc_id={doc_id}")
            continue

        # Синхронизируем остатки
        result = _sync_stock_for_products(tenant_id, tenant, ms_product_ids)

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
        f"MoySklad webhook processed tenant_id={tenant_id} "
        f"docs={len(processed_docs)} synced={total_synced} failed={total_failed}"
    )

    return {
        "status": "ok",
        "docs_processed": len(processed_docs),
        "synced": total_synced,
        "skipped": total_skipped,
        "failed": total_failed,
        "details": processed_docs
    }


def _extract_doc_type_from_href(href: str) -> Optional[str]:
    """Извлекает тип документа из href если не передан в meta.type."""
    if not href:
        return None
    parts = href.rstrip("/").split("/")
    # href: .../entity/demand/uuid
    try:
        entity_idx = parts.index("entity")
        return parts[entity_idx + 1]
    except (ValueError, IndexError):
        return None