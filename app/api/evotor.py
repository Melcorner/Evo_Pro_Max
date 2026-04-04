import json
import logging
import time
import uuid

from fastapi import APIRouter, HTTPException, Request

from app.db import get_connection, adapt_query as aq
from app.clients.evotor_client import fetch_stores_by_token

router = APIRouter(tags=["Evotor Service"])
log = logging.getLogger("api.evotor")


@router.post("/api/v1/user/token")
async def user_token(request: Request):
    """
    Эвотор передаёт cloud token после установки/подключения приложения.
    Сохраняем подключение аккаунта Эвотор и список его магазинов.
    """
    try:
        body = await request.json()
    except Exception as e:
        log.error("Failed to parse /user/token body: %s", e)
        raise HTTPException(status_code=400, detail="invalid json body")

    user_id = body.get("userId") or body.get("userUuid")
    token   = body.get("token")

    log.info("POST /user/token userId=%s token_exists=%s", user_id, bool(token))

    if not user_id:
        raise HTTPException(status_code=400, detail="userId is required")
    if not token:
        raise HTTPException(status_code=400, detail="token is required")

    try:
        stores = fetch_stores_by_token(token)
    except Exception as e:
        log.exception("Failed to fetch Evotor stores for userId=%s", user_id)
        raise HTTPException(status_code=502, detail=f"failed to fetch evotor stores: {e}")

    now = int(time.time())

    conn = get_connection()
    try:
        cur = conn.cursor()

        cur.execute(
            aq("SELECT id FROM evotor_connections WHERE evotor_user_id = ?"),
            (user_id,),
        )
        existing = cur.fetchone()

        if existing:
            connection_id = existing["id"]
            cur.execute(
                aq("""
                UPDATE evotor_connections
                SET evotor_token = ?,
                    stores_json  = ?,
                    updated_at   = ?
                WHERE id = ?
                """),
                (token, json.dumps(stores, ensure_ascii=False), now, connection_id),
            )
        else:
            connection_id = str(uuid.uuid4())
            cur.execute(
                aq("""
                INSERT INTO evotor_connections (
                    id, evotor_user_id, evotor_token, stores_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """),
                (connection_id, user_id, token, json.dumps(stores, ensure_ascii=False), now, now),
            )

        conn.commit()
    finally:
        conn.close()

    return {
        "status": "ok",
        "connection_id": connection_id,
        "evotor_user_id": user_id,
        "stores_count": len(stores),
    }