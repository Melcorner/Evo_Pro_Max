import logging

from fastapi import APIRouter
from app.db import get_connection
from app.stores.error_store import list_errors

log = logging.getLogger("api")
router = APIRouter()


@router.get("/errors")
def get_errors(limit: int = 50, offset: int = 0):
    conn = get_connection()
    rows = list_errors(conn, limit=limit, offset=offset)
    conn.close()
    return rows
