import json
import logging

from fastapi import APIRouter, HTTPException, Request
from app.db import get_connection

router = APIRouter(tags=["Evotor Service"])
log = logging.getLogger("api.evotor")


@router.post("/api/v1/user/token")
async def user_token(request: Request):
    """
    Эвотор передаёт токен облака для авторизации запросов к REST API Эвотор.

    Корректный путь резолва:
    1) по userId/userUuid, если tenant уже связан с пользователем Эвотор,
    2) иначе по единственному tenant без evotor_user_id.

    Не используем fallback по evotor_api_key == token: это разные сущности.
    """
    try:
        body = await request.json()
    except Exception as e:
        log.error("Failed to parse /user/token body: %s", e)
        raise HTTPException(status_code=400, detail="invalid json body")

    user_id = body.get("userId") or body.get("userUuid")
    token = body.get("token")

    log.info(
        "POST /user/token userId=%s token_exists=%s",
        user_id,
        bool(token),
    )

    if not user_id:
        raise HTTPException(status_code=400, detail="userId or userUuid is required")
    if not token:
        raise HTTPException(status_code=400, detail="token is required")

    conn = get_connection()
    try:
        cursor = conn.cursor()

        # 1. Сначала ищем уже привязанный tenant по evotor_user_id.
        cursor.execute(
            """
            SELECT id
            FROM tenants
            WHERE evotor_user_id = ?
            ORDER BY created_at DESC
            """,
            (user_id,),
        )
        rows = cursor.fetchall()

        if len(rows) > 1:
            raise HTTPException(
                status_code=409,
                detail="Ambiguous tenant mapping for userId. Resolve duplicate evotor_user_id first.",
            )

        target_tenant_id = None
        if len(rows) == 1:
            target_tenant_id = rows[0]["id"]
        else:
            # 2. Иначе допускаем привязку только к одному "свободному" tenant.
            cursor.execute(
                """
                SELECT id
                FROM tenants
                WHERE evotor_user_id IS NULL OR TRIM(evotor_user_id) = ''
                ORDER BY created_at DESC
                """
            )
            free_rows = cursor.fetchall()

            if len(free_rows) == 1:
                target_tenant_id = free_rows[0]["id"]
            elif len(free_rows) == 0:
                raise HTTPException(status_code=404, detail="tenant not found")
            else:
                raise HTTPException(
                    status_code=409,
                    detail="Multiple tenants without evotor_user_id. Cannot safely bind token automatically.",
                )

        cursor.execute(
            """
            UPDATE tenants
            SET evotor_token = ?, evotor_user_id = ?
            WHERE id = ?
            """,
            (token, user_id, target_tenant_id),
        )
        conn.commit()

        log.info(
            "Evotor cloud token saved tenant_id=%s userId=%s token_exists=%s",
            target_tenant_id,
            user_id,
            bool(token),
        )
        return {"status": "ok", "tenant_id": target_tenant_id}

    finally:
        conn.close()


@router.post("/api/v1/user/create")
async def user_create(request: Request):
    """
    Эвотор отправляет регистрационные данные нового пользователя.
    """
    try:
        body = await request.json()
    except Exception as e:
        log.error("Failed to parse /user/create body: %s", e)
        raise HTTPException(status_code=400, detail="invalid json body")

    log.info("POST /user/create body=%s", json.dumps(body, ensure_ascii=False))

    user_id = body.get("userId") or body.get("id")

    return {
        "status": "ok",
        "userId": user_id,
    }


@router.post("/api/v1/user/verify")
async def user_verify(request: Request):
    """
    Эвотор отправляет данные для авторизации пользователя.
    """
    try:
        body = await request.json()
    except Exception as e:
        log.error("Failed to parse /user/verify body: %s", e)
        raise HTTPException(status_code=400, detail="invalid json body")

    log.info("POST /user/verify body=%s", json.dumps(body, ensure_ascii=False))

    return {"status": "ok"}


@router.put("/")
async def receive_documents(request: Request):
    """
    Эвотор передаёт документы (продажи) в сторонний сервис.
    Логируем payload для анализа формата.
    """
    try:
        body = await request.json()
    except Exception as e:
        log.error("Failed to parse PUT / body: %s", e)
        raise HTTPException(status_code=400, detail="invalid json body")

    log.info("PUT / (documents) body=%s", json.dumps(body, ensure_ascii=False))

    return {"status": "ok"}


@router.post("/api/v1/subscription/event")
async def subscription_event(request: Request):
    """
    Эвотор отправляет события об изменении подписки.
    """
    try:
        body = await request.json()
    except Exception as e:
        log.error("Failed to parse /subscription/event body: %s", e)
        raise HTTPException(status_code=400, detail="invalid json body")

    log.info("POST /subscription/event body=%s", json.dumps(body, ensure_ascii=False))

    return {"status": "ok"}
