from fastapi import APIRouter
from app.database import get_connection
import sqlite3

router = APIRouter()

@router.get("/lineage/{dataset_id}")
def get_lineage(dataset_id: str):
    conn = get_connection()
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("""
        SELECT dataset_id, source, operation, timestamp, metadata
        FROM lineage_log
        WHERE dataset_id = ?
        ORDER BY timestamp
    """, (dataset_id,))

    rows = cursor.fetchall()
    conn.close()

    return [dict(row) for row in rows]
