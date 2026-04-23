import json
import os
import sqlite3
from datetime import datetime


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LICENSE_JSON_FILE = os.path.join(BASE_DIR, "licenses.json")
LICENSE_DB_FILE = os.path.join(BASE_DIR, "licenses.sqlite")


def _connect():
    conn = sqlite3.connect(LICENSE_DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with _connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS licenses (
                license_key TEXT PRIMARY KEY,
                username TEXT,
                customer_name TEXT,
                status TEXT DEFAULT 'active',
                payload TEXT NOT NULL,
                created_at TEXT,
                updated_at TEXT
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_licenses_username ON licenses(username)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_licenses_status ON licenses(status)")


def _entry_status(entry: dict) -> str:
    if entry.get("revoked"):
        return "revoked"
    expires_at = str(entry.get("expires_at") or "").strip()
    if expires_at:
        try:
            if datetime.fromisoformat(expires_at) < datetime.now():
                return "expired"
        except ValueError:
            return "invalid"
    return "active"


def _upsert_entry(conn, entry: dict):
    key = str(entry.get("key") or "").strip().upper()
    if not key:
        return
    entry["key"] = key
    payload = json.dumps(entry, ensure_ascii=False, separators=(",", ":"))
    now = datetime.now().isoformat(timespec="seconds")
    conn.execute(
        """
        INSERT INTO licenses (license_key, username, customer_name, status, payload, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(license_key) DO UPDATE SET
            username=excluded.username,
            customer_name=excluded.customer_name,
            status=excluded.status,
            payload=excluded.payload,
            updated_at=excluded.updated_at
        """,
        (
            key,
            str(entry.get("username") or "").strip(),
            str(entry.get("customer_name") or "").strip(),
            _entry_status(entry),
            payload,
            str(entry.get("created_at") or now),
            now,
        ),
    )


def migrate_json_if_needed():
    init_db()
    with _connect() as conn:
        count = conn.execute("SELECT COUNT(*) FROM licenses").fetchone()[0]
        if count:
            return
        if not os.path.exists(LICENSE_JSON_FILE):
            return
        try:
            with open(LICENSE_JSON_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            return
        for entry in data.get("licenses") or []:
            if isinstance(entry, dict):
                _upsert_entry(conn, dict(entry))


def load_store() -> dict:
    migrate_json_if_needed()
    with _connect() as conn:
        rows = conn.execute(
            "SELECT payload FROM licenses ORDER BY COALESCE(created_at, updated_at, license_key) DESC"
        ).fetchall()
    items = []
    for row in rows:
        try:
            item = json.loads(row["payload"])
            if isinstance(item, dict):
                items.append(item)
        except Exception:
            pass
    return {"licenses": items}


def save_store(store: dict):
    init_db()
    items = [dict(item) for item in (store.get("licenses") or []) if isinstance(item, dict)]
    keys = {str(item.get("key") or "").strip().upper() for item in items if item.get("key")}
    with _connect() as conn:
        for item in items:
            _upsert_entry(conn, item)
        if keys:
            placeholders = ",".join("?" for _ in keys)
            conn.execute(f"DELETE FROM licenses WHERE license_key NOT IN ({placeholders})", tuple(keys))
        else:
            conn.execute("DELETE FROM licenses")

