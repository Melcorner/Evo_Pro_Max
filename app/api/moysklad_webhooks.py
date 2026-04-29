import hashlib
import hmac
import json
import logging
import os
import time
from typing import Optional, List

from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel, ConfigDict

from app.api.sync import _get_ms_product_stock_for_store, _upsert_stock_status
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


def _get_ms_webhook_secret() -> str:
    return os.getenv("MS_WEBHOOK_SECRET", "").strip()


def _normalize_signature(signature: str | None) -> str | None:
    """
    Нормализует подпись из заголовка X-Lognex-Signature.

    Поддерживает:
    - обычный hex digest;
    - значение с префиксом sha256=...
    """
    if not signature:
        return None

    value = signature.strip()

    if value.lower().startswith("sha256="):
        value = value.split("=", 1)[1].strip()

    return value.lower() or None


def _verify_signature(body: bytes, signature: str | None) -> bool:
    """
    Проверяет подпись webhook от МойСклад через HMAC-SHA256.

    Важно:
    - без MS_WEBHOOK_SECRET запрос не принимаем;
    - значение подписи не логируем;
    - сравнение выполняем через hmac.compare_digest.
    """
    secret = _get_ms_webhook_secret()

    if not secret:
        log.error("moysklad.webhook.signature: MS_WEBHOOK_SECRET is not set")
        return False

    normalized_signature = _normalize_signature(signature)

    log.info("moysklad.webhook.signature: present=%s", bool(normalized_signature))

    if not normalized_signature:
        return False

    expected = hmac.new(
        secret.encode("utf-8"),
        body,
        hashlib.sha256,
    ).hexdigest()

    return hmac.compare_digest(expected, normalized_signature)


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
            doc_type,
            doc_id,
            r.status_code,
            r.text[:300],
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


def _load_stock_sync_stores(tenant_id: str) -> list[dict]:
    """Возвращает магазины, для которых можно обновлять остатки МойСклад → Эвотор."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            aq("""
            SELECT evotor_store_id, ms_store_id
            FROM tenant_stores
            WHERE tenant_id = ?
              AND sync_completed_at IS NOT NULL
            ORDER BY is_primary DESC, created_at ASC
            """),
            (tenant_id,),
        )
        rows = [dict(r) for r in cur.fetchall()]
        if rows:
            return rows

        # Fallback для старой tenant-level конфигурации.
        cur.execute(
            aq("""
            SELECT evotor_store_id, ms_store_id
            FROM tenants
            WHERE id = ? AND evotor_store_id IS NOT NULL AND TRIM(evotor_store_id) <> ''
            """),
            (tenant_id,),
        )
        row = cur.fetchone()
        return [dict(row)] if row else []
    finally:
        conn.close()


def _sync_stock_for_products(
    tenant_id: str,
    ms_product_ids: List[str],
) -> dict:
    """
    Синхронизирует остатки для списка товаров МойСклад → Эвотор.

    В multi-store режиме один ms_id может иметь разные остатки для разных складов
    МойСклад, поэтому mapping и EvotorClient выбираются строго по evotor_store_id.
    """
    mapping_store = MappingStore()
    ms_client = MoySkladClient(tenant_id)
    stores = _load_stock_sync_stores(tenant_id)

    synced = 0
    skipped = 0
    failed = 0
    errors = []

    if not stores:
        log.warning("No synced stores for tenant_id=%s — skipping stock sync", tenant_id)
        return {"synced": 0, "skipped": len(ms_product_ids), "failed": 0, "errors": []}

    for ms_id in ms_product_ids:
        product_had_mapping = False

        for store_row in stores:
            evotor_store_id = store_row.get("evotor_store_id")
            ms_store_id = store_row.get("ms_store_id")
            if not evotor_store_id:
                continue

            evotor_id = mapping_store.get_by_ms_id(
                tenant_id=tenant_id,
                entity_type="product",
                ms_id=ms_id,
                evotor_store_id=evotor_store_id,
            )

            if not evotor_id:
                continue

            product_had_mapping = True

            try:
                if ms_store_id:
                    quantity = _get_ms_product_stock_for_store(ms_client.token, ms_id, ms_store_id)
                else:
                    quantity = ms_client.get_product_stock(ms_id)
            except Exception as e:
                log.error(
                    "Failed to get stock ms_id=%s store=%s ms_store=%s err=%s",
                    ms_id, evotor_store_id, ms_store_id, e,
                )
                failed += 1
                errors.append({
                    "ms_id": ms_id,
                    "evotor_store_id": evotor_store_id,
                    "error": str(e),
                })
                continue

            try:
                evotor_client = EvotorClient(tenant_id, store_id=evotor_store_id)
                evotor_client.update_product_stock(evotor_id, quantity)
                log.info(
                    "Stock synced ms_id=%s evotor_id=%s store=%s ms_store=%s quantity=%s",
                    ms_id, evotor_id, evotor_store_id, ms_store_id, quantity,
                )
                synced += 1
            except Exception as e:
                log.error(
                    "Failed to update Evotor stock evotor_id=%s store=%s err=%s",
                    evotor_id, evotor_store_id, e,
                )
                failed += 1
                errors.append({
                    "ms_id": ms_id,
                    "evotor_id": evotor_id,
                    "evotor_store_id": evotor_store_id,
                    "error": str(e),
                })

        if not product_had_mapping:
            log.warning(
                "No store-aware mapping for ms_id=%s tenant_id=%s — skipping stock sync",
                ms_id, tenant_id,
            )
            skipped += 1

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
async def moysklad_webhook(
    tenant_id: str,
    request: Request,
    x_lognex_signature: str | None = Header(default=None),
):
    """
    Принимает webhook от МойСклад при изменении документов.

    При создании/изменении отгрузки, приёмки, инвентаризации, списания или оприходования —
    автоматически синхронизирует остатки затронутых товаров в Эвотор.
    """
    raw_body = await request.body()

    if not _verify_signature(raw_body, x_lognex_signature):
        raise HTTPException(status_code=401, detail="Invalid MoySklad webhook signature")

    try:
        payload = json.loads(raw_body.decode("utf-8"))
        body = MoySkladWebhook.model_validate(payload)
    except Exception as e:
        log.error("Invalid webhook body tenant_id=%s err=%s", tenant_id, e)
        raise HTTPException(status_code=400, detail=f"Invalid webhook body: {e}")

    tenant = _load_tenant(tenant_id)

    if not tenant.get("sync_completed_at"):
        log.warning("MoySklad webhook received but sync not completed tenant_id=%s", tenant_id)
        return {"status": "skipped", "reason": "initial sync not completed"}

    started_at: int | None = None
    stock_sync_started = False
    total_synced = 0
    total_skipped = 0
    total_failed = 0
    total_products = 0
    processed_docs = []

    ms_client = MoySkladClient(tenant_id)

    try:
        for event in body.events:
            href = event.meta.href
            doc_type = event.meta.type or _extract_doc_type_from_href(href)
            doc_id = _extract_ms_id_from_href(href)

            # Обработка создания/обновления товара
            if doc_type == "product" and doc_id:
                log.info("MoySklad product event tenant_id=%s product_id=%s", tenant_id, doc_id)
                
                try:
                    from app.api.sync import sync_ms_to_evotor_store
                    from app.db import get_connection as _gc, adapt_query as _aq

                    _conn = _gc()
                    try:
                        _cur = _conn.cursor()
                        _cur.execute(
                            _aq("""
                            SELECT evotor_store_id
                            FROM tenant_stores
                            WHERE tenant_id = ?
                              AND sync_completed_at IS NOT NULL
                            """),
                            (tenant_id,),
                        )
                        store_ids = [r["evotor_store_id"] for r in _cur.fetchall()]
                    finally:
                        _conn.close()

                    for _sid in store_ids:
                        try:
                            _res = sync_ms_to_evotor_store(tenant_id, _sid)
                            log.info(
                                "Product sync after MS webhook store=%s synced=%s",
                                _sid,
                                _res.get("synced", 0),
                            )
                        except Exception as _e:
                            log.error("Product sync failed store=%s err=%s", _sid, _e)
                
                except Exception as e:
                    log.error("Failed to handle product webhook err=%s", e)
                
                continue

            if not doc_type or doc_type not in STOCK_TRIGGER_TYPES:
                log.info("Skipping non-stock doc_type=%s href=%s", doc_type, href)
                continue

            if not stock_sync_started:
                started_at = int(time.time())
                _upsert_stock_status(
                    tenant_id=tenant_id,
                    status="in_progress",
                    started_at=started_at,
                    last_error=None,
                    synced_items_count=0,
                    total_items_count=0,
                )
                stock_sync_started = True

            if not doc_id:
                log.warning("Cannot extract doc_id from href=%s", href)
                total_failed += 1
                processed_docs.append({
                    "doc_type": doc_type,
                    "doc_id": None,
                    "products": 0,
                    "synced": 0,
                    "skipped": 0,
                    "failed": 1,
                    "error": "cannot extract doc_id",
                })
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
                    "products": 0,
                    "synced": 0,
                    "skipped": 0,
                    "failed": 1,
                    "error": str(e),
                })
                continue

            if not ms_product_ids:
                processed_docs.append({
                    "doc_type": doc_type,
                    "doc_id": doc_id,
                    "products": 0,
                    "synced": 0,
                    "skipped": 0,
                    "failed": 0,
                    "error": None,
                })
                log.info("No products found in doc_type=%s doc_id=%s", doc_type, doc_id)
                continue

            total_products += len(ms_product_ids)

            result = _sync_stock_for_products(tenant_id, ms_product_ids)

            total_synced += result["synced"]
            total_skipped += result["skipped"]
            total_failed += result["failed"]

            doc_error = None
            if result["errors"]:
                doc_error = "; ".join(err.get("error", "unknown error") for err in result["errors"][:3])

            processed_docs.append({
                "doc_type": doc_type,
                "doc_id": doc_id,
                "products": len(ms_product_ids),
                "synced": result["synced"],
                "skipped": result["skipped"],
                "failed": result["failed"],
                "error": doc_error,
            })

        if not stock_sync_started:
            log.info("MoySklad webhook contains no stock-trigger documents tenant_id=%s", tenant_id)
            return {
                "status": "ok",
                "docs_processed": 0,
                "products_total": 0,
                "synced": 0,
                "skipped": 0,
                "failed": 0,
                "details": [],
            }

        final_status = "ok" if total_failed == 0 else "error"
        final_error = None
        if total_failed > 0:
            final_error = f"Webhook stock sync failed: failed={total_failed}, synced={total_synced}"

        _upsert_stock_status(
            tenant_id=tenant_id,
            status=final_status,
            started_at=started_at,
            last_sync_at=int(time.time()) if total_synced > 0 else None,
            last_error=final_error,
            synced_items_count=total_synced,
            total_items_count=total_products,
        )

        log.info(
            "MoySklad webhook processed tenant_id=%s docs=%s synced=%s skipped=%s failed=%s",
            tenant_id, len(processed_docs), total_synced, total_skipped, total_failed,
        )

        return {
            "status": "ok" if total_failed == 0 else "partial",
            "docs_processed": len(processed_docs),
            "products_total": total_products,
            "synced": total_synced,
            "skipped": total_skipped,
            "failed": total_failed,
            "details": processed_docs,
        }

    except Exception as e:
        if stock_sync_started:
            _upsert_stock_status(
                tenant_id=tenant_id,
                status="error",
                started_at=started_at,
                last_sync_at=int(time.time()) if total_synced > 0 else None,
                last_error=f"Webhook stock sync exception: {type(e).__name__}: {e}",
                synced_items_count=total_synced,
                total_items_count=total_products,
            )
        raise


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
