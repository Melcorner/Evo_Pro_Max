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
    Сохраняем токен по userId или по evotor_api_key.
    """
    try:
        body = await request.json()
    except Exception as e:
        log.error(f"Failed to parse /user/token body: {e}")
        raise HTTPException(status_code=400, detail="invalid json body")

    log.info(f"POST /user/token userId={body.get('userId') or body.get('userUuid')} token_exists={bool(body.get('token'))}")

    user_id = body.get("userId") or body.get("userUuid")
    token = body.get("token")

    if not token:
        raise HTTPException(status_code=400, detail="token is required")

    conn = get_connection()
    try:
        cursor = conn.cursor()

        updated = 0

        # 1. Сначала пытаемся найти tenant по user_id
        if user_id:
            cursor.execute("""
                UPDATE tenants
                SET evotor_token = ?, evotor_user_id = ?
                WHERE evotor_user_id = ?
            """, (token, user_id, user_id))
            updated = cursor.rowcount

        # 2. Если не нашли — пробуем привязать по evotor_api_key
        if updated == 0:
            cursor.execute("""
                UPDATE tenants
                SET evotor_token = ?, evotor_user_id = ?
                WHERE evotor_api_key = ?
            """, (token, user_id, token))
            updated = cursor.rowcount

        conn.commit()

        if updated == 0:
            raise HTTPException(status_code=404, detail="tenant not found")

        log.info(f"Evotor cloud token saved userId={user_id} token_exists={bool(token)}")
        return {"status": "ok"}

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
        log.error(f"Failed to parse /user/create body: {e}")
        raise HTTPException(status_code=400, detail="invalid json body")

    log.info(f"POST /user/create body={json.dumps(body, ensure_ascii=False)}")

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
        raise HTTPException(status_code=400, detail="invalid json body")

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
        raise HTTPException(status_code=400, detail="invalid json body")

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
        raise HTTPException(status_code=400, detail="invalid json body")

    log.info(f"POST /subscription/event body={json.dumps(body, ensure_ascii=False)}")

    return {"status": "ok"}