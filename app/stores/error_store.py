import json
import time
import uuid


def insert_error(conn, row, error_code, message, response_body=None):
    """
    Сохраняет запись об ошибке в таблицу errors.
    row - это строка события из event_store
    """
    cursor = conn.cursor()

    payload_snapshot = row["payload_json"]
    if isinstance(payload_snapshot, (dict, list)):
        payload_snapshot = json.dumps(payload_snapshot, ensure_ascii=False)

    cursor.execute("""
        INSERT INTO errors (
            id,
            event_id,
            tenant_id,
            error_code,
            message,
            payload_snapshot,
            response_body,
            created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        str(uuid.uuid4()),
        row["id"],
        row["tenant_id"],
        error_code,
        message,
        payload_snapshot,
        response_body,
        int(time.time())
    ))

def list_errors(conn, limit=50, offset=0):
    cursor = conn.cursor()

    cursor.execute("""
        SELECT *
        FROM errors
        ORDER BY created_at DESC
        LIMIT ? OFFSET ?
    """, (limit, offset))

    rows = cursor.fetchall()

    return [dict(r) for r in rows]