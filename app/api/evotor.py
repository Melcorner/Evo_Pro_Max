import json
import time
import logging

from fastapi import APIRouter, Request
from app.db import get_connection

router = APIRouter()
log = logging.getLogger("api.evotor")


@router.post("/api/v1/user/token")
async def user_token(request: Request):
    """
    Эвотор передаёт токен облака для авторизации запросов к REST API Эвотор.
    Сохраняем токен по userId.
    """
    try:
        body = await request.json()
    except Exception as e:
        log.error(f"Failed to parse /user/token body: {e}")
        return {"status": "error"}

    log.info(f"POST /user/token body={json.dumps(body, ensure_ascii=False)}")

    user_id = body.get("userId") or body.get("userUuid")
    token = body.get("token")

    if user_id and token:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE tenants
            SET evotor_token = ?, evotor_user_id = ?
            WHERE evotor_user_id = ?
        """, (token, user_id, user_id))

        if cursor.rowcount == 0:
            # Если tenant ещё не привязан — ищем по токену
            cursor.execute("""
                UPDATE tenants
                SET evotor_token = ?, evotor_user_id = ?
                WHERE evotor_token IS NULL
                LIMIT 1
            """, (token, user_id))

        conn.commit()
        conn.close()
        log.info(f"Evotor cloud token saved userId={user_id}")

    return {"status": "ok"}


@router.post("/api/v1/user/create")
async def user_create(request: Request):
    """
    Эвотор отправляет регистрационные данные нового пользователя.
    """
    try:
        body = await request.json()
    except Exception as e:
        log.error(f"Failed to parse /user/create body: {e}")
        return {"status": "error"}

    log.info(f"POST /user/create body={json.dumps(body, ensure_ascii=False)}")

    # Возвращаем userId который будет использоваться в дальнейших запросах
    user_id = body.get("userId") or body.get("id")

    return {
        "status": "ok",
        "userId": user_id
    }


@router.post("/api/v1/user/verify")
async def user_verify(request: Request):
    """
    Эвотор отправляет данные для авторизации пользователя.
    """
    try:
        body = await request.json()
    except Exception as e:
        log.error(f"Failed to parse /user/verify body: {e}")
        return {"status": "error"}

    log.info(f"POST /user/verify body={json.dumps(body, ensure_ascii=False)}")

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
        log.error(f"Failed to parse PUT / body: {e}")
        return {"status": "error"}

    log.info(f"PUT / (documents) body={json.dumps(body, ensure_ascii=False)}")

    return {"status": "ok"}


@router.post("/api/v1/subscription/event")
async def subscription_event(request: Request):
    """
    Эвотор отправляет события об изменении подписки.
    """
    try:
        body = await request.json()
    except Exception as e:
        log.error(f"Failed to parse /subscription/event body: {e}")
        return {"status": "error"}

    log.info(f"POST /subscription/event body={json.dumps(body, ensure_ascii=False)}")

    return {"status": "ok"}