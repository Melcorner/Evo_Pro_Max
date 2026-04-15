import json
import os
import time
import uuid
import logging
import hmac
from typing import Optional, List

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, ConfigDict

from app.db import get_connection, adapt_query as aq

router = APIRouter(tags=["Evotor Webhooks"])
log = logging.getLogger("api.webhooks")


def _get_evotor_webhook_secret() -> str:
    return os.getenv("EVOTOR_WEBHOOK_SECRET", "").strip()


def _verify_evotor_signature(request_headers: dict) -> bool:
    """
    Эвотор передаёт токен в заголовке Authorization: Bearer <token>.
    Проверяем, что токен совпадает с EVOTOR_WEBHOOK_SECRET.
    Если секрет не настроен — пропускаем проверку (для локальной разработки).
    """
    secret = _get_evotor_webhook_secret()
    if not secret:
        log.warning("EVOTOR_WEBHOOK_SECRET not set — skipping signature verification")
        return True

    auth_header = (
        request_headers.get("authorization")
        or request_headers.get("Authorization")
        or ""
    ).strip()

    if not auth_header.startswith("Bearer "):
        return False

    token = auth_header.removeprefix("Bearer ").strip()
    return hmac.compare_digest(token, secret)


class EvotorPosition(BaseModel):
    model_config = ConfigDict(extra="allow")

    product_id: str
    product_name: Optional[str] = None
    quantity: float
    price: float
    sum: Optional[float] = None


class EvotorBody(BaseModel):
    model_config = ConfigDict(extra="allow")

    positions: List[EvotorPosition]
    sum: Optional[float] = None


class EvotorWebhook(BaseModel):
    model_config = ConfigDict(extra="allow")

    type: str
    id: str
    store_id: Optional[str] = None
    device_id: Optional[str] = None
    body: Optional[EvotorBody] = None


LIKELY_CUSTOMER_KEYS = (
    "buyer", "customer", "client", "contractor",
    "customerInfo", "buyerInfo", "customer_info", "buyer_info",
    "buyerRequisites", "buyer_requisites",
    "paymentCustomer", "payment_customer",
)


def _pick_first_direct(d: dict, aliases: list[str]):
    if not isinstance(d, dict):
        return None
    aliases_lower = {alias.lower() for alias in aliases}
    for key, value in d.items():
        if value in (None, "", [], {}):
            continue
        if str(key).lower() in aliases_lower:
            return value
    return None


def _safe_float(value):
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _extract_customer(candidate_root: dict | None) -> dict | None:
    if not isinstance(candidate_root, dict):
        return None

    candidate_dicts: list[dict] = []
    for key in LIKELY_CUSTOMER_KEYS:
        value = candidate_root.get(key)
        if isinstance(value, dict):
            candidate_dicts.append(value)
    candidate_dicts.append(candidate_root)

    for candidate in candidate_dicts:
        name  = _pick_first_direct(candidate, ["name", "fullName", "fio", "customerName", "buyerName", "clientName"])
        phone = _pick_first_direct(candidate, ["phone", "phoneNumber", "customerPhone", "buyerPhone", "clientPhone", "tel", "telephone", "mobilePhone"])
        email = _pick_first_direct(candidate, ["email", "eMail", "mail", "customerEmail", "buyerEmail", "clientEmail"])
        inn   = _pick_first_direct(candidate, ["inn", "customerInn", "buyerInn", "clientInn", "payerInn"])

        if any(v not in (None, "") for v in (name, phone, email, inn)):
            return {
                "name":  str(name).strip()  if name  not in (None, "") else None,
                "phone": str(phone).strip() if phone not in (None, "") else None,
                "email": str(email).strip() if email not in (None, "") else None,
                "inn":   str(inn).strip()   if inn   not in (None, "") else None,
                "raw":   candidate,
            }
    return None

def _fetch_full_evotor_document(store_id: str, doc_id: str, evotor_token: str) -> dict | None:
    try:
        import requests as req
        r = req.get(
            f"https://api.evotor.ru/stores/{store_id}/documents/{doc_id}",
            headers={"X-Authorization": evotor_token},
            timeout=10,
        )
        if r.ok:
            return r.json()
        log.warning("Failed to fetch full document store_id=%s doc_id=%s status=%s", store_id, doc_id, r.status_code)
        return None
    except Exception as e:
        log.exception("Error fetching full document store_id=%s doc_id=%s err=%s", store_id, doc_id, e)
        return None


def _tax_type_to_percent(tax_type: str | None) -> int | None:
    mapping = {
        "VAT_0": 0, "VAT_10": 10, "VAT_18": 20,
        "VAT_20": 20, "VAT_10_110": 10, "VAT_20_120": 20,
        "NO_VAT": 0,
    }
    return mapping.get(str(tax_type).upper(), None) if tax_type else None

def _normalize_receipt_created(body_dict: dict) -> tuple[str, str, dict] | tuple[None, None, None]:
    data = body_dict.get("data") or {}
    # Если items пришли без деталей — запрашиваем полный документ через API
    items_raw = data.get("items", [])
    if items_raw and len(items_raw) > 0 and len(items_raw[0]) == 1:
        store_id = data.get("storeId")
        doc_id = data.get("id")
        evotor_token = body_dict.get("_evotor_token")  # передаётся из обработчика
        if store_id and doc_id and evotor_token:
            full_doc = _fetch_full_evotor_document(store_id, doc_id, evotor_token)
            if full_doc:
                # Переписываем data из полного документа
                body = full_doc.get("body", {})
                positions = body.get("positions", [])
                # Конвертируем positions в формат items
                new_items = []
                for pos in positions:
                    new_items.append({
                        "id": pos.get("product_id"),
                        "name": pos.get("product_name"),
                        "quantity": pos.get("quantity"),
                        "price": pos.get("price"),
                        "sumPrice": pos.get("sum"),
                        "resultPrice": pos.get("result_price"),
                        "resultSum": pos.get("result_sum"),
                        "discount": pos.get("sum", 0) - pos.get("result_sum", pos.get("sum", 0)),
                        "tax": pos.get("tax", {}).get("type"),
                        "taxPercent": _tax_type_to_percent(pos.get("tax", {}).get("type")),
                        "measureName": pos.get("measure_name"),
                    })
                data = dict(data)
                data["items"] = new_items
                log.info("Enriched receipt from full document store_id=%s doc_id=%s positions=%d",
                         store_id, doc_id, len(new_items))
    receipt_doc_type = (data.get("type") or "").strip().upper()

    if receipt_doc_type != "SELL":
        return None, None, None

    positions = []
    computed_total_sum = 0.0
    has_any_result_sum = False

    for item in data.get("items", []):
        product_id = item.get("id") or item.get("productId") or item.get("product_id")
        if not product_id:
            log.warning("Skipping receipt item without product_id: %s", item)
            continue

        quantity = _safe_float(item.get("quantity"))
        price = _safe_float(item.get("price"))
        line_sum = _safe_float(item.get("sumPrice"))
        result_price = _safe_float(item.get("resultPrice", item.get("result_price")))
        result_sum = _safe_float(item.get("resultSum", item.get("result_sum")))
        raw_discount = _safe_float(item.get("discount"))

        # если нет количества или оно некорректное — позиция невалидна
        if quantity is None or quantity <= 0:
            log.warning(
                "Skipping receipt item with invalid quantity product_id=%s quantity=%s raw=%s",
                product_id,
                quantity,
                item,
            )
            continue

        # если sumPrice не передан, пробуем вычислить
        if line_sum is None and price is not None:
            line_sum = quantity * price

        # если result_sum не передан, пробуем вычислить через discount
        if result_sum is None and raw_discount is not None and line_sum is not None:
            result_sum = max(0.0, line_sum - raw_discount)

        # если result_price не передан, пробуем вычислить из result_sum
        if result_price is None and result_sum is not None and quantity > 0:
            result_price = result_sum / quantity

        # если нет ни одной осмысленной денежной величины — позиция невалидна
        if (
            (price is None or price <= 0)
            and (line_sum is None or line_sum <= 0)
            and (result_sum is None or result_sum <= 0)
        ):
            log.warning(
                "Skipping receipt item with no valid price/sum product_id=%s raw=%s",
                product_id,
                item,
            )
            continue

        if result_sum is not None:
            computed_total_sum += result_sum
            has_any_result_sum = True
        elif line_sum is not None:
            computed_total_sum += line_sum

        positions.append({
            "product_id": product_id,
            "product_name": item.get("name"),
            "quantity": quantity,
            "price": price or 0.0,
            "sum": line_sum or 0.0,
            "result_price": result_price,
            "result_sum": result_sum,
            "position_discount": item.get("positionDiscount", item.get("position_discount")),
            "doc_distributed_discount": item.get("docDistributedDiscount", item.get("doc_distributed_discount")),
            "discount": raw_discount,
            "tax": item.get("tax"),
            "tax_percent": item.get("taxPercent", item.get("tax_percent")),
            "raw": item,
        })

    # если после фильтрации валидных позиций не осталось — событие пропускаем
    if not positions:
        log.warning(
            "Skipping ReceiptCreated because no valid positions found receipt_id=%s source_type=%s",
            body_dict.get("id") or data.get("id"),
            body_dict.get("type"),
        )
        return None, None, None

    document_sum = computed_total_sum if positions else None
    if not has_any_result_sum:
        total_amount = _safe_float(data.get("totalAmount"))
        if total_amount is not None and total_amount > 0:
            document_sum = total_amount

    normalized_payload = {
        "id": body_dict.get("id") or data.get("id") or str(uuid.uuid4()),
        "type": "SELL",
        "store_id": body_dict.get("store_id") or body_dict.get("storeId") or data.get("storeId"),
        "device_id": body_dict.get("device_id") or body_dict.get("deviceId") or data.get("deviceId"),
        "customer": _extract_customer(data),
        "body": {
            "positions": positions,
            "sum": document_sum,
            "doc_discounts": data.get("docDiscounts", data.get("doc_discounts", [])) or [],
            "total_discount": _safe_float(data.get("totalDiscount")),
            "total_tax": _safe_float(data.get("totalTax")),
        },
        "source_event_type": body_dict.get("type"),
        "source_data": data,
    }

    return "sale", normalized_payload["id"], normalized_payload


def _query_single_tenant_id(sql: str, params: tuple) -> str | None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(aq(sql), params)
        rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        return None
    if len(rows) > 1:
        raise HTTPException(
            status_code=409,
            detail="Ambiguous tenant resolution for Evotor webhook. Resolve duplicate Evotor binding first.",
        )
    return rows[0]["id"]


def _resolve_tenant_id_by_store_id(store_id: str | None) -> str | None:
    if not store_id:
        return None
    # Сначала ищем через tenant_stores
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            aq("SELECT tenant_id FROM tenant_stores WHERE evotor_store_id = ?"),
            (store_id,),
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    if len(rows) == 1:
        return rows[0]["tenant_id"]
    if len(rows) > 1:
        raise HTTPException(
            status_code=409,
            detail="Ambiguous tenant resolution for Evotor webhook.",
        )
    # Fallback: старая колонка (тенанты до миграции)
    return _query_single_tenant_id(
        "SELECT id FROM tenants WHERE evotor_store_id = ?",
        (store_id,),
    )


def _resolve_tenant_id_by_user_id(user_id: str | None) -> str | None:
    if not user_id:
        return None
    return _query_single_tenant_id(
        "SELECT id FROM tenants WHERE evotor_user_id = ?",
        (user_id,),
    )


def _extract_store_id(body_dict: dict, normalized_payload: dict | None = None) -> str | None:
    data = body_dict.get("data") or {}
    if normalized_payload:
        value = normalized_payload.get("store_id")
        if value:
            return str(value)
    for value in (
        body_dict.get("store_id"),
        body_dict.get("storeId"),
        data.get("store_id"),
        data.get("storeId"),
    ):
        if value:
            return str(value)
    return None


def _extract_user_id(body_dict: dict) -> str | None:
    data = body_dict.get("data") or {}
    for value in (
        body_dict.get("userId"),
        body_dict.get("userUuid"),
        data.get("userId"),
        data.get("userUuid"),
    ):
        if value:
            return str(value)
    return None


def _resolve_tenant_id(
    explicit_tenant_id: str | None,
    body_dict: dict,
    normalized_payload: dict | None = None,
) -> str:
    if explicit_tenant_id:
        return explicit_tenant_id

    store_id = _extract_store_id(body_dict, normalized_payload=normalized_payload)
    if store_id:
        tenant_id = _resolve_tenant_id_by_store_id(store_id)
        if tenant_id:
            return tenant_id

    user_id = _extract_user_id(body_dict)
    if user_id:
        tenant_id = _resolve_tenant_id_by_user_id(user_id)
        if tenant_id:
            return tenant_id

    raise HTTPException(
        status_code=404,
        detail="Unable to resolve tenant for Evotor webhook. Configure evotor_store_id or evotor_user_id for this tenant.",
    )


@router.post("/webhooks/evotor")
@router.post("/webhooks/evotor/{tenant_id}")
async def evotor_webhook(raw_body: EvotorWebhook, request: Request, tenant_id: str | None = None):
    if not _verify_evotor_signature(dict(request.headers)):
        auth = request.headers.get("authorization", "")
        token_preview = auth[:30] if auth else "<missing>"
        log.warning(
            "Evotor webhook signature verification failed tenant_id=%s ip=%s user_agent=%s auth_preview=%s",
            tenant_id,
            request.client.host if request.client else "unknown",
            request.headers.get("user-agent", ""),
            token_preview,
        )
        raise HTTPException(status_code=401, detail="Invalid webhook signature")

    body_dict = raw_body.model_dump()
    log.info(
        "RAW EVOTOR BODY tenant_id=%s body=%s",
        tenant_id,
        json.dumps(body_dict, ensure_ascii=False),
    )

    # Событие установки приложения — сохраняем токен клиента
    if "token" in body_dict and ("userUuid" in body_dict or "userId" in body_dict):
        resolved_tenant_id = _resolve_tenant_id(tenant_id, body_dict)
        evotor_token   = body_dict.get("token")
        evotor_user_id = body_dict.get("userId") or body_dict.get("userUuid")

        log.info(
            "Install event tenant_id=%s userId=%s token_exists=%s",
            resolved_tenant_id, evotor_user_id, bool(evotor_token),
        )

        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                aq("""
                UPDATE tenants
                SET evotor_user_id = ?, evotor_token = ?
                WHERE id = ?
                """),
                (evotor_user_id, evotor_token, resolved_tenant_id),
            )
            conn.commit()
        finally:
            conn.close()

        # Если store_id есть в теле — добавляем запись в tenant_stores
        install_store_id = _extract_store_id(body_dict)
        if install_store_id:
            import uuid as _uuid_mod, time as _time_mod
            conn2 = get_connection()
            try:
                cur2 = conn2.cursor()
                cur2.execute(
                    aq("""
                    INSERT INTO tenant_stores (id, tenant_id, evotor_store_id, is_primary, created_at)
                    VALUES (?, ?, ?, 1, ?)
                    ON CONFLICT (evotor_store_id) DO UPDATE SET tenant_id = EXCLUDED.tenant_id
                    """),
                    (str(_uuid_mod.uuid4()), resolved_tenant_id, install_store_id, int(_time_mod.time())),
                )
                conn2.commit()
                log.info("tenant_stores upserted tenant_id=%s store_id=%s", resolved_tenant_id, install_store_id)
            except Exception as _e:
                log.warning("Failed to upsert tenant_stores err=%s", _e)
            finally:
                conn2.close()

        log.info("Evotor token saved tenant_id=%s", resolved_tenant_id)
        return {"status": "accepted", "tenant_id": resolved_tenant_id}

    event_type_raw = body_dict.get("type") or "sale"

    if event_type_raw == "ReceiptCreated":
        # Получаем evotor_token из tenant для запроса полного документа
        try:
            store_id_for_token = (body_dict.get("data") or {}).get("storeId")
            user_id_for_token = body_dict.get("userId") or body_dict.get("userUuid")
            conn = get_connection()
            try:
                cur = conn.cursor()
                if store_id_for_token:
                    # Сначала через tenant_stores
                    cur.execute(
                        aq("""
                        SELECT t.evotor_token
                        FROM tenant_stores ts
                        JOIN tenants t ON t.id = ts.tenant_id
                        WHERE ts.evotor_store_id = ?
                        LIMIT 1
                        """),
                        (store_id_for_token,),
                    )
                    row = cur.fetchone()
                    # Fallback
                    if not row:
                        cur.execute(
                            aq("SELECT evotor_token FROM tenants WHERE evotor_store_id = ?"),
                            (store_id_for_token,),
                        )
                        row = cur.fetchone()
                elif user_id_for_token:
                    cur.execute(aq("SELECT evotor_token FROM tenants WHERE evotor_user_id = ?"), (user_id_for_token,))
                    row = cur.fetchone()
                else:
                    row = None
                if row and row["evotor_token"]:
                    body_dict["_evotor_token"] = row["evotor_token"]
            finally:
                conn.close()
        except Exception as e:
            log.warning("Failed to get evotor_token for receipt enrichment err=%s", e)
        event_type, event_id, normalized_payload = _normalize_receipt_created(body_dict)

        if not event_type:
            log.warning(
                "Skipping ReceiptCreated tenant_id=%s raw_type=%s reason=no valid sale payload",
                tenant_id,
                event_type_raw,
            )
            return {"status": "skipped", "reason": "no valid sale positions"}

        payload_to_store = normalized_payload
    else:
        normalized_payload = None
        event_id = body_dict.get("id") or str(uuid.uuid4())
        event_type_map = {
            "SELL": "sale", "sell": "sale",
            "Receipt": "sale", "receipt": "sale",
        }
        event_type     = event_type_map.get(event_type_raw, event_type_raw.lower())
        payload_to_store = body_dict

    resolved_tenant_id = _resolve_tenant_id(
        tenant_id, body_dict, normalized_payload=normalized_payload,
    )

    log.info(
        "Webhook parsed tenant_id=%s event_type=%s event_key=%s",
        resolved_tenant_id, event_type, event_id,
    )

    if event_type not in ("sale",):
        log.warning("Unsupported event_type=%s raw=%s — skipping", event_type, event_type_raw)
        return {"status": "skipped", "reason": f"unsupported event_type: {event_type_raw}"}

    now = int(time.time())
    event_store_id = str(uuid.uuid4())

    conn = get_connection()
    try:
        cur = conn.cursor()

        cur.execute(aq("SELECT id FROM tenants WHERE id = ?"), (resolved_tenant_id,))
        if cur.fetchone() is None:
            raise HTTPException(status_code=404, detail="tenant not found")

        cur.execute(
            aq("""
            SELECT 1 FROM processed_events
            WHERE tenant_id = ? AND event_key = ?
            """),
            (resolved_tenant_id, event_id),
        )
        if cur.fetchone() is not None:
            log.info("Already processed tenant_id=%s event_key=%s", resolved_tenant_id, event_id)
            return {"status": "already_processed", "tenant_id": resolved_tenant_id}

        cur.execute(
            aq("""
            SELECT id FROM event_store
            WHERE tenant_id = ? AND event_key = ?
            LIMIT 1
            """),
            (resolved_tenant_id, event_id),
        )
        existing_queued = cur.fetchone()
        if existing_queued is not None:
            log.info("Already queued tenant_id=%s event_key=%s", resolved_tenant_id, event_id)
            return {
                "status": "already_queued",
                "event_id": existing_queued["id"],
                "tenant_id": resolved_tenant_id,
            }

        cur.execute(
            aq("""
            INSERT INTO event_store (
                id, tenant_id, event_type, event_key, payload_json,
                status, retries, next_retry_at, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, 'NEW', 0, NULL, ?, ?)
            """),
            (
                event_store_id,
                resolved_tenant_id,
                event_type,
                event_id,
                json.dumps(payload_to_store, ensure_ascii=False),
                now,
                now,
            ),
        )
        conn.commit()

    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        # Обрабатываем гонку дублей (unique constraint) — возвращаем already_queued
        err_str = str(e).lower()
        if "unique" in err_str or "duplicate" in err_str:
            cur2 = conn.cursor()
            cur2.execute(
                aq("SELECT id FROM event_store WHERE tenant_id = ? AND event_key = ? LIMIT 1"),
                (resolved_tenant_id, event_id),
            )
            existing = cur2.fetchone()
            log.info("Race-duplicate insert tenant_id=%s event_key=%s", resolved_tenant_id, event_id)
            return {
                "status": "already_queued",
                "event_id": existing["id"] if existing else None,
                "tenant_id": resolved_tenant_id,
            }
        log.exception("DB error on event insert tenant_id=%s event_key=%s", resolved_tenant_id, event_id)
        raise HTTPException(status_code=500, detail="Internal database error")
    finally:
        conn.close()

    log.info(
        "Event stored NEW event_id=%s tenant_id=%s event_key=%s",
        event_store_id, resolved_tenant_id, event_id,
    )

    return {"status": "accepted", "event_id": event_store_id, "tenant_id": resolved_tenant_id}