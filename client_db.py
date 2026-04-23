import json
import os
import sqlite3
from datetime import datetime


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CLIENT_DB_FILE = os.path.join(BASE_DIR, "client_data.sqlite")


def _connect():
    conn = sqlite3.connect(CLIENT_DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with _connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS client_json (
                license_key TEXT NOT NULL,
                file_name TEXT NOT NULL,
                payload TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (license_key, file_name)
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_client_json_key ON client_json(license_key)")


def get_json(license_key: str, file_name: str, default=None):
    init_db()
    key = str(license_key or "default").strip().upper() or "DEFAULT"
    name = str(file_name or "").strip()
    if not name:
        return default
    with _connect() as conn:
        row = conn.execute(
            "SELECT payload FROM client_json WHERE license_key = ? AND file_name = ?",
            (key, name),
        ).fetchone()
    if not row:
        return default
    try:
        value = json.loads(row["payload"])
        return value
    except Exception:
        return default


def set_json(license_key: str, file_name: str, payload):
    init_db()
    key = str(license_key or "default").strip().upper() or "DEFAULT"
    name = str(file_name or "").strip()
    if not name:
        return
    now = datetime.now().isoformat(timespec="seconds")
    text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO client_json (license_key, file_name, payload, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(license_key, file_name) DO UPDATE SET
                payload=excluded.payload,
                updated_at=excluded.updated_at
            """,
            (key, name, text, now),
        )


def list_client_files(license_key: str) -> dict:
    init_db()
    key = str(license_key or "default").strip().upper() or "DEFAULT"
    with _connect() as conn:
        rows = conn.execute(
            "SELECT file_name, payload, updated_at FROM client_json WHERE license_key = ? ORDER BY file_name",
            (key,),
        ).fetchall()
    result = {}
    for row in rows:
        try:
            payload = json.loads(row["payload"])
        except Exception:
            payload = None
        result[row["file_name"]] = {
            "payload": payload,
            "updated_at": row["updated_at"],
        }
    return result


def all_client_files() -> dict:
    init_db()
    with _connect() as conn:
        rows = conn.execute(
            "SELECT license_key, file_name, payload, updated_at FROM client_json ORDER BY license_key, file_name"
        ).fetchall()
    result = {}
    for row in rows:
        key = row["license_key"]
        result.setdefault(key, {})
        try:
            payload = json.loads(row["payload"])
        except Exception:
            payload = None
        result[key][row["file_name"]] = {
            "payload": payload,
            "updated_at": row["updated_at"],
        }
    return result
