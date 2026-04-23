import argparse
import json
import math
import os
import secrets
import sys
from datetime import datetime, timedelta

from flask import Flask, redirect, render_template_string, request, url_for

from client_db import get_json as client_get_json, list_client_files, set_json as client_set_json
from license_db import load_store, save_store


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LICENSE_STORE_FILE = os.path.join(BASE_DIR, "licenses.json")
CLIENT_DATA_ROOT = os.path.join(BASE_DIR, "client_data")


def load_json_file(path: str, default):
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            return json.load(f)
    except Exception:
        return default


def load_client_payload(key: str, file_name: str, default):
    slug = license_storage_slug(key)
    value = client_get_json(slug, file_name, None)
    if value is not None:
        return value
    value = load_json_file(os.path.join(CLIENT_DATA_ROOT, slug, file_name), default)
    if value is not default:
        try:
            client_set_json(slug, file_name, value)
        except Exception:
            pass
    return value


def generate_key() -> str:
    return "-".join(secrets.token_hex(2).upper() for _ in range(4))


def normalize_duration_unit(unit: str) -> str:
    unit = str(unit or "").strip().lower()
    if unit in {"segundo", "segundos", "second", "seconds", "s"}:
        return "seconds"
    if unit in {"minuto", "minutos", "minute", "minutes", "m"}:
        return "minutes"
    if unit in {"hora", "horas", "hour", "hours", "h"}:
        return "hours"
    return "days"


def duration_label(value: int, unit: str) -> str:
    unit = normalize_duration_unit(unit)
    labels = {
        "seconds": ("segundo", "segundos"),
        "minutes": ("minuto", "minutos"),
        "hours": ("hora", "horas"),
        "days": ("dia", "dias"),
    }
    singular, plural = labels[unit]
    return f"{value} {singular if value == 1 else plural}"


def duration_delta(value: int, unit: str) -> timedelta:
    value = max(1, int(value or 1))
    unit = normalize_duration_unit(unit)
    if unit == "seconds":
        return timedelta(seconds=value)
    if unit == "minutes":
        return timedelta(minutes=value)
    if unit == "hours":
        return timedelta(hours=value)
    return timedelta(days=value)


def license_storage_slug(key: str) -> str:
    clean = "".join(ch if ch.isalnum() else "_" for ch in str(key or "").strip().upper()).strip("_")
    return clean or "default"


def format_dt_br(iso_text: str) -> str:
    text = str(iso_text or "").strip()
    if not text:
        return "-"
    try:
        return datetime.fromisoformat(text).strftime("%d/%m/%Y %H:%M:%S")
    except ValueError:
        return text


def format_money(value, currency: str = "BRLX") -> str:
    try:
        return f"{float(value):.2f} {currency}"
    except Exception:
        return f"-- {currency}"


def collect_license_usage(entry: dict) -> dict:
    key = str(entry.get("key") or "").strip().upper()
    slug = license_storage_slug(key)
    folder = os.path.join(CLIENT_DATA_ROOT, slug)
    exists = os.path.isdir(folder)

    db_files = list_client_files(slug)
    saldo_raw = load_client_payload(key, "saldo_timeline.json", [])
    order_log = load_client_payload(key, "automation_orders_log.json", [])
    placar = load_client_payload(key, "placar_telegram.json", {})
    analytics = load_client_payload(key, "signal_analytics.json", [])
    telegram_cfg = load_client_payload(key, "telegram_config.json", {})
    automation_cfg = load_client_payload(key, "automatizado_config.json", {})

    latest_balance = None
    initial_balance = None
    currency = "BRLX"
    saldo_points = saldo_raw.get("points") if isinstance(saldo_raw, dict) else saldo_raw
    if isinstance(saldo_points, list) and saldo_points:
        first = saldo_points[0] or {}
        last = saldo_points[-1] or {}
        initial_balance = first.get("balance")
        latest_balance = last.get("balance")
        currency = str(last.get("currency") or first.get("currency") or "BRLX")

    sent_count = 0
    error_count = 0
    win_count = 0
    loss_count = 0
    last_event = None
    total_amount = 0.0
    if isinstance(order_log, list):
        for item in order_log:
            status = str(item.get("status") or "").lower()
            outcome = str(item.get("outcome") or "").lower()
            if status in {"sent", "pending"}:
                sent_count += 1
            if status == "error":
                error_count += 1
            if outcome == "win":
                win_count += 1
            elif outcome == "loss":
                loss_count += 1
            try:
                if status in {"sent", "pending"} and item.get("amount") is not None:
                    total_amount += float(item.get("amount") or 0)
            except Exception:
                pass
        if order_log:
            last_event = order_log[-1]

    telegram_wins = 0
    telegram_losses = 0
    if isinstance(placar, dict):
        for item in placar.values():
            if isinstance(item, dict):
                telegram_wins += int(item.get("wins") or 0)
                telegram_losses += int(item.get("losses") or 0)

    analytics_count = len(analytics) if isinstance(analytics, list) else 0
    telegram_ready = bool(
        isinstance(telegram_cfg, dict)
        and str(telegram_cfg.get("bot_token") or "").strip()
        and str(telegram_cfg.get("chat_id") or "").strip()
    )
    enabled_markets = 0
    bot_running = False
    if isinstance(automation_cfg, dict):
        bot_running = bool((automation_cfg.get("_meta") or {}).get("running"))
        for value in automation_cfg.values():
            if isinstance(value, dict) and value.get("enabled"):
                enabled_markets += 1

    return {
        "folder": folder,
        "exists": exists or bool(db_files),
        "db_files": len(db_files),
        "balance": format_money(latest_balance, currency) if latest_balance is not None else "-",
        "balance_delta": (
            f"{float(latest_balance) - float(initial_balance):+.2f} {currency}"
            if latest_balance is not None and initial_balance is not None else "-"
        ),
        "orders_sent": sent_count,
        "orders_error": error_count,
        "wins": win_count,
        "losses": loss_count,
        "telegram_wins": telegram_wins,
        "telegram_losses": telegram_losses,
        "analytics_count": analytics_count,
        "telegram_ready": "sim" if telegram_ready else "nao",
        "bot_running": "sim" if bot_running else "nao",
        "enabled_markets": enabled_markets,
        "total_amount": format_money(total_amount, currency) if total_amount else "-",
        "last_event_time": format_dt_br((last_event or {}).get("ts")),
        "last_event_status": ((last_event or {}).get("status") or "-"),
        "last_event_detail": ((last_event or {}).get("detail") or "-"),
    }


def license_status(entry: dict) -> str:
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


def create_entry(
    customer_name: str,
    plan_name: str,
    duration_value: int,
    duration_unit: str,
    notes: str,
    username: str = "",
    password: str = "",
) -> dict:
    store = load_store()
    now = datetime.now()
    amount = max(1, int(duration_value or 1))
    unit = normalize_duration_unit(duration_unit)
    username = (username or customer_name or f"cliente{len(store.get('licenses') or []) + 1}").strip()
    username = username.replace(" ", "").lower() or f"cliente{len(store.get('licenses') or []) + 1}"
    password = (password or secrets.token_urlsafe(6)).strip()
    entry = {
        "key": generate_key(),
        "username": username,
        "password": password,
        "customer_name": customer_name.strip(),
        "plan_name": (plan_name or duration_label(amount, unit)).strip(),
        "notes": (notes or "").strip(),
        "created_at": now.isoformat(timespec="seconds"),
        "expires_at": None,
        "duration_value": amount,
        "duration_unit": unit,
        "duration_label": duration_label(amount, unit),
        "revoked": False,
        "device_fingerprint": "",
        "device_name": "",
        "bound_ip": "",
        "bound_device_hash": "",
        "last_ip": "",
        "last_user_agent": "",
        "last_device_hash": "",
        "activated_at": None,
        "last_seen_at": None,
    }
    store.setdefault("licenses", []).append(entry)
    save_store(store)
    return entry


def cmd_create(args):
    entry = create_entry(args.customer, args.plan, args.amount, args.unit, args.notes, args.username, args.password)
    print("License gerada:")
    print(f"KEY={entry['key']}")
    print(f"USUARIO={entry['username']}")
    print(f"SENHA={entry['password']}")
    print(f"CLIENTE={entry['customer_name'] or '-'}")
    print(f"PLANO={entry['plan_name']}")
    print(f"VALIDADE={entry['duration_label']}")
    print("EXPIRA=começa a contar na primeira ativação")


def cmd_list(_args):
    store = load_store()
    items = store.get("licenses") or []
    if not items:
        print("Nenhuma license cadastrada.")
        return
    for entry in items:
        label = entry.get("duration_label") or "-"
        if label == "-":
            expires_at = str(entry.get("expires_at") or "").strip()
            created_at = str(entry.get("created_at") or "").strip()
            try:
                if expires_at and created_at:
                    delta = datetime.fromisoformat(expires_at) - datetime.fromisoformat(created_at)
                    days = max(1, delta.days)
                    label = duration_label(days, "days")
            except ValueError:
                pass
        print(
            f"{entry.get('key')} | "
            f"{license_status(entry)} | "
            f"{entry.get('customer_name') or '-'} | "
            f"user {entry.get('username') or '-'} | "
            f"senha {entry.get('password') or '-'} | "
            f"{entry.get('plan_name') or '-'} | "
            f"validade {label} | "
            f"expira {entry.get('expires_at') or '-'} | "
            f"device {entry.get('device_name') or '-'}"
        )


def cmd_revoke(args):
    store = load_store()
    target = args.key.strip().upper()
    for entry in store.get("licenses") or []:
        if str(entry.get("key") or "").upper() != target:
            continue
        entry["revoked"] = True
        entry["revoked_at"] = datetime.now().isoformat(timespec="seconds")
        save_store(store)
        print(f"License revogada: {target}")
        return
    print(f"License nao encontrada: {target}")


def revoke_key(key: str) -> tuple[bool, str]:
    store = load_store()
    target = key.strip().upper()
    for entry in store.get("licenses") or []:
        if str(entry.get("key") or "").upper() != target:
            continue
        entry["revoked"] = True
        entry["revoked_at"] = datetime.now().isoformat(timespec="seconds")
        save_store(store)
        return True, f"License revogada: {target}"
    return False, f"License nao encontrada: {target}"


def release_device_lock(key: str) -> tuple[bool, str]:
    store = load_store()
    target = key.strip().upper()
    for entry in store.get("licenses") or []:
        if str(entry.get("key") or "").upper() != target:
            continue
        entry["bound_ip"] = ""
        entry["bound_device_hash"] = ""
        entry["last_ip"] = ""
        entry["last_user_agent"] = ""
        entry["last_device_hash"] = ""
        entry["device_fingerprint"] = ""
        entry["device_name"] = ""
        entry["released_at"] = datetime.now().isoformat(timespec="seconds")
        save_store(store)
        return True, f"IP/dispositivo liberado: {target}"
    return False, f"License nao encontrada: {target}"


def edit_license_entry(
    key: str,
    *,
    customer_name: str = "",
    username: str = "",
    password: str = "",
    plan_name: str = "",
    notes: str = "",
    set_duration_value: int = 0,
    set_duration_unit: str = "days",
    add_duration_value: int = 0,
    add_duration_unit: str = "days",
) -> tuple[bool, str]:
    store = load_store()
    target = str(key or "").strip().upper()
    for entry in store.get("licenses") or []:
        if str(entry.get("key") or "").strip().upper() != target:
            continue

        if customer_name.strip():
            entry["customer_name"] = customer_name.strip()
        if username.strip():
            entry["username"] = username.strip().replace(" ", "").lower()
        if password.strip():
            entry["password"] = password.strip()
        if plan_name.strip():
            entry["plan_name"] = plan_name.strip()
        entry["notes"] = notes.strip()

        set_amount = max(0, int(set_duration_value or 0))
        set_unit = normalize_duration_unit(set_duration_unit or "days")
        if set_amount > 0:
            entry["duration_value"] = set_amount
            entry["duration_unit"] = set_unit
            entry["duration_label"] = duration_label(set_amount, set_unit)
            if str(entry.get("activated_at") or "").strip():
                base_dt = datetime.now()
                entry["expires_at"] = (base_dt + duration_delta(set_amount, set_unit)).isoformat(timespec="seconds")
            else:
                entry["expires_at"] = None

        add_amount = max(0, int(add_duration_value or 0))
        add_unit = normalize_duration_unit(add_duration_unit or "days")
        if add_amount > 0:
            extra_delta = duration_delta(add_amount, add_unit)
            expires_at = str(entry.get("expires_at") or "").strip()
            if expires_at:
                try:
                    base_dt = max(datetime.now(), datetime.fromisoformat(expires_at))
                except ValueError:
                    base_dt = datetime.now()
                entry["expires_at"] = (base_dt + extra_delta).isoformat(timespec="seconds")
            else:
                current_value = max(0, int(entry.get("duration_value") or 0))
                current_unit = normalize_duration_unit(entry.get("duration_unit") or "days")
                if current_unit == add_unit:
                    entry["duration_value"] = current_value + add_amount
                else:
                    current_delta = duration_delta(current_value or 1, current_unit) if current_value else timedelta()
                    merged_delta = current_delta + extra_delta
                    merged_days = max(1, int(math.ceil(merged_delta.total_seconds() / 86400.0)))
                    entry["duration_value"] = merged_days
                    entry["duration_unit"] = "days"

        duration_value = max(1, int(entry.get("duration_value") or 1))
        duration_unit = normalize_duration_unit(entry.get("duration_unit") or "days")
        entry["duration_label"] = duration_label(duration_value, duration_unit)
        entry["updated_at"] = datetime.now().isoformat(timespec="seconds")
        save_store(store)
        return True, f"Licenca atualizada: {target}"
    return False, f"License nao encontrada: {target}"


ADMIN_TEMPLATE = """
<!doctype html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Admin de Licencas</title>
  <style>
    :root {
      --bg:#0f1117; --card:#1a1d27; --border:#2a2d3a; --text:#e2e8f0;
      --muted:#94a3b8; --green:#22c55e; --red:#ef4444; --yellow:#f59e0b; --blue:#3b82f6;
    }
    * { box-sizing:border-box; }
    body { margin:0; background:var(--bg); color:var(--text); font-family:Segoe UI,system-ui,sans-serif; padding:24px; }
    .shell { max-width:1120px; margin:0 auto; }
    .grid { display:grid; grid-template-columns: 360px 1fr; gap:18px; }
    .stats-grid { display:grid; grid-template-columns: repeat(4, 1fr); gap:14px; margin-bottom:18px; }
    .card { background:var(--card); border:1px solid var(--border); border-radius:16px; padding:18px; }
    .mini-card { background:var(--card); border:1px solid var(--border); border-radius:14px; padding:14px 16px; }
    h1 { margin:0 0 18px; font-size:1.5rem; }
    h2 { margin:0 0 12px; font-size:1rem; }
    h3 { margin:0 0 10px; font-size:.96rem; }
    .muted { color:var(--muted); }
    .big { font-size:1.45rem; font-weight:800; margin-top:6px; }
    label { display:block; margin:10px 0 6px; font-size:.82rem; color:var(--muted); }
    input, textarea, select { width:100%; border:1px solid var(--border); border-radius:10px; padding:11px 12px; background:#11141d; color:var(--text); }
    textarea { min-height:84px; resize:vertical; }
    button { border:0; border-radius:10px; padding:11px 14px; font-weight:700; cursor:pointer; }
    .btn-primary { background:var(--blue); color:#fff; width:100%; margin-top:14px; }
    .btn-danger { background:rgba(239,68,68,.14); color:#ffb4b4; border:1px solid rgba(239,68,68,.4); }
    .flash { padding:12px 14px; border-radius:12px; margin-bottom:16px; border:1px solid var(--border); }
    .flash.ok { background:rgba(34,197,94,.12); color:#9ae6b4; border-color:rgba(34,197,94,.35); }
    .flash.err { background:rgba(239,68,68,.12); color:#ffb4b4; border-color:rgba(239,68,68,.35); }
    .duration-row { display:grid; grid-template-columns: 1fr 160px; gap:10px; }
    table { width:100%; border-collapse:collapse; }
    th, td { text-align:left; padding:10px 8px; border-bottom:1px solid var(--border); vertical-align:top; font-size:.9rem; }
    th { color:var(--muted); font-size:.76rem; text-transform:uppercase; letter-spacing:.06em; }
    .status { font-weight:700; }
    .status.active { color:var(--green); }
    .status.expired { color:var(--yellow); }
    .status.revoked, .status.invalid { color:var(--red); }
    code { font-size:.84rem; word-break:break-all; }
    .actions { display:flex; gap:8px; flex-wrap:wrap; }
    .license-details { display:grid; grid-template-columns: repeat(2, 1fr); gap:14px; margin-top:18px; }
    .detail-box { border:1px solid var(--border); border-radius:14px; padding:14px; background:rgba(255,255,255,.02); }
    .detail-grid { display:grid; grid-template-columns: repeat(2, 1fr); gap:10px 14px; }
    .detail-item { font-size:.86rem; }
    .detail-item strong { display:block; font-size:.74rem; text-transform:uppercase; letter-spacing:.05em; color:var(--muted); margin-bottom:4px; }
    .edit-grid { display:grid; grid-template-columns: repeat(2, 1fr); gap:10px 14px; margin-top:14px; }
    .edit-grid .full { grid-column:1 / -1; }
    .btn-secondary { background:#111827; color:var(--text); border:1px solid var(--border); width:100%; margin-top:10px; }
    .tabs { display:flex; gap:8px; flex-wrap:wrap; margin:0 0 18px; padding:8px; border:1px solid var(--border); border-radius:14px; background:rgba(255,255,255,.02); }
    .tab-btn { background:#111827; color:var(--muted); border:1px solid var(--border); }
    .tab-btn.active { background:var(--blue); color:white; border-color:var(--blue); }
    .tab-panel { display:none; }
    .tab-panel.active { display:block; }
    @media (max-width: 860px) {
      .grid { grid-template-columns: 1fr; }
      .stats-grid { grid-template-columns: 1fr 1fr; }
      .license-details { grid-template-columns: 1fr; }
      .detail-grid { grid-template-columns: 1fr; }
      .edit-grid { grid-template-columns: 1fr; }
      .duration-row { grid-template-columns: 1fr; }
      body { padding:14px; }
    }
  </style>
</head>
<body>
  <div class="shell">
    <h1>Admin de Licencas</h1>
    {% if flash %}
      <div class="flash {{ 'ok' if flash.ok else 'err' }}">{{ flash.message }}</div>
    {% endif %}
    <div class="tabs">
      <button class="tab-btn active" type="button" onclick="showTab('criar', this)">Criar conta</button>
      <button class="tab-btn" type="button" onclick="showTab('licencas', this)">Licenças</button>
      <button class="tab-btn" type="button" onclick="showTab('visao', this)">Visão geral</button>
      <button class="tab-btn" type="button" onclick="showTab('saldos', this)">Saldos</button>
      <button class="tab-btn" type="button" onclick="showTab('ordens', this)">Ordens</button>
      <button class="tab-btn" type="button" onclick="showTab('telegram', this)">Telegram</button>
      <button class="tab-btn" type="button" onclick="showTab('analytics', this)">Analytics</button>
      <button class="tab-btn" type="button" onclick="showTab('banco', this)">SQLite</button>
    </div>
    <div class="stats-grid">
      <div class="mini-card">
        <div class="muted">Licencas</div>
        <div class="big">{{ summary.total }}</div>
      </div>
      <div class="mini-card">
        <div class="muted">Ativas</div>
        <div class="big" style="color:var(--green)">{{ summary.active }}</div>
      </div>
      <div class="mini-card">
        <div class="muted">Ordens enviadas</div>
        <div class="big">{{ summary.orders_sent }}</div>
      </div>
      <div class="mini-card">
        <div class="muted">Saldo somado</div>
        <div class="big">{{ summary.total_balance }}</div>
      </div>
    </div>
    <section id="tab-criar" class="tab-panel active">
      <div class="card">
        <h2>Gerar Licenca</h2>
        <div class="muted">Esse painel roda so no seu PC em <code>127.0.0.1</code>.</div>
        <form method="post" action="{{ url_for('create_license') }}">
          <label>Cliente</label>
          <input name="customer_name" placeholder="Nome do cliente">
          <label>Usuario de login</label>
          <input name="username" placeholder="ex: joao123">
          <label>Senha inicial</label>
          <input name="password" placeholder="deixe vazio para gerar automaticamente">
          <label>Plano</label>
          <input name="plan_name" placeholder="Mensal, Semanal, Teste...">
          <label>Validade</label>
          <div class="duration-row">
            <input name="duration_value" type="number" min="1" value="30">
            <select name="duration_unit">
              <option value="minutes">Minutos</option>
              <option value="hours">Horas</option>
              <option value="days" selected>Dias</option>
              <option value="seconds">Segundos</option>
            </select>
          </div>
          <label>Observacoes</label>
          <textarea name="notes" placeholder="Notas internas"></textarea>
          <button class="btn-primary" type="submit">Gerar licenca</button>
        </form>
      </div>
    </section>
    <section id="tab-licencas" class="tab-panel">
      <div class="card">
        <h2>Licencas Cadastradas</h2>
        {% if not licenses %}
          <div class="muted">Nenhuma licenca cadastrada ainda.</div>
        {% else %}
          <div style="overflow-x:auto">
            <table>
              <thead>
                <tr>
                  <th>Key</th>
                  <th>Status</th>
                  <th>Cliente</th>
                  <th>Usuario</th>
                  <th>Senha</th>
                  <th>Plano</th>
                  <th>Validade</th>
                  <th>Expira</th>
                  <th>Saldo</th>
                  <th>Ordens</th>
                  <th>IP</th>
                  <th>Ultimo acesso</th>
                  <th>Acoes</th>
                </tr>
              </thead>
              <tbody>
                {% for item in licenses %}
                <tr>
                  <td><code>{{ item.key }}</code></td>
                  <td><span class="status {{ item.status }}">{{ item.status }}</span></td>
                  <td>{{ item.customer_name or '-' }}</td>
                  <td><code>{{ item.username or '-' }}</code></td>
                  <td><code>{{ item.password or '-' }}</code></td>
                  <td>{{ item.plan_name or '-' }}</td>
                  <td>{{ item.duration_label or '-' }}</td>
                  <td>{{ item.expires_at or '-' }}</td>
                  <td>{{ item.admin.balance }}</td>
                  <td>{{ item.admin.orders_sent }}</td>
                  <td>{{ item.bound_ip or item.last_ip or '-' }}</td>
                  <td>{{ item.last_seen_at or '-' }}</td>
                  <td>
                    <div class="actions">
                      <form method="post" action="{{ url_for('release_license_device', license_key=item.key) }}" onsubmit="return confirm('Liberar IP/dispositivo de {{ item.key }}?');">
                        <button class="tab-btn" type="submit">Liberar IP</button>
                      </form>
                      {% if item.status != 'revoked' %}
                      <form method="post" action="{{ url_for('revoke_license', license_key=item.key) }}" onsubmit="return confirm('Revogar {{ item.key }}?');">
                        <button class="btn-danger" type="submit">Revogar</button>
                      </form>
                      {% endif %}
                    </div>
                  </td>
                </tr>
                {% if item.notes %}
                <tr>
                  <td colspan="12" class="muted">Obs.: {{ item.notes }}</td>
                </tr>
                {% endif %}
                {% endfor %}
              </tbody>
            </table>
          </div>
        {% endif %}
      </div>
    </section>
    <section id="tab-visao" class="tab-panel">
    {% if licenses %}
    <div class="license-details">
      {% for item in licenses %}
      <div class="detail-box">
        <h3><code>{{ item.key }}</code></h3>
        <div class="muted" style="margin-bottom:10px">{{ item.customer_name or '-' }} · {{ item.plan_name or '-' }} · <span class="status {{ item.status }}">{{ item.status }}</span></div>
        <div class="detail-grid">
          <div class="detail-item"><strong>Validade</strong>{{ item.duration_label or '-' }}</div>
          <div class="detail-item"><strong>Expira</strong>{{ item.expires_at or '-' }}</div>
          <div class="detail-item"><strong>Saldo atual</strong>{{ item.admin.balance }}</div>
          <div class="detail-item"><strong>Delta saldo</strong>{{ item.admin.balance_delta }}</div>
          <div class="detail-item"><strong>Ordens enviadas</strong>{{ item.admin.orders_sent }}</div>
          <div class="detail-item"><strong>Ordens com erro</strong>{{ item.admin.orders_error }}</div>
          <div class="detail-item"><strong>Wins / Losses</strong>{{ item.admin.wins }} / {{ item.admin.losses }}</div>
          <div class="detail-item"><strong>Placar Telegram</strong>{{ item.admin.telegram_wins }} / {{ item.admin.telegram_losses }}</div>
          <div class="detail-item"><strong>Telegram pronto</strong>{{ item.admin.telegram_ready }}</div>
          <div class="detail-item"><strong>Bot rodando</strong>{{ item.admin.bot_running }}</div>
          <div class="detail-item"><strong>Mercados ativos</strong>{{ item.admin.enabled_markets }}</div>
          <div class="detail-item"><strong>Registros SQLite</strong>{{ item.admin.db_files }}</div>
          <div class="detail-item"><strong>IP vinculado</strong>{{ item.bound_ip or '-' }}</div>
          <div class="detail-item"><strong>Ultimo IP</strong>{{ item.last_ip or '-' }}</div>
          <div class="detail-item"><strong>Ultimo acesso</strong>{{ item.last_seen_at or '-' }}</div>
          <div class="detail-item"><strong>Dispositivo</strong>{{ item.device_name or '-' }}</div>
          <div class="detail-item"><strong>Volume apostado</strong>{{ item.admin.total_amount }}</div>
          <div class="detail-item"><strong>Analytics</strong>{{ item.admin.analytics_count }}</div>
          <div class="detail-item"><strong>Ultimo evento</strong>{{ item.admin.last_event_time }}</div>
          <div class="detail-item"><strong>Status ultimo evento</strong>{{ item.admin.last_event_status }}</div>
          <div class="detail-item" style="grid-column:1 / -1"><strong>Detalhe ultimo evento</strong>{{ item.admin.last_event_detail }}</div>
          <div class="detail-item" style="grid-column:1 / -1"><strong>Pasta cliente</strong><code>{{ item.admin.folder }}</code></div>
        </div>
        <form method="post" action="{{ url_for('edit_license', license_key=item.key) }}">
          <div class="edit-grid">
            <div>
              <label>Cliente</label>
              <input name="customer_name" value="{{ item.customer_name or '' }}" placeholder="Nome do cliente">
            </div>
            <div>
              <label>Plano</label>
              <input name="plan_name" value="{{ item.plan_name or '' }}" placeholder="Mensal, VIP, Teste...">
            </div>
            <div>
              <label>Usuario</label>
              <input name="username" value="{{ item.username or '' }}" placeholder="usuario de login">
            </div>
            <div>
              <label>Nova senha</label>
              <input name="password" value="" placeholder="deixe vazio para manter a senha atual">
              <div class="muted" style="font-size:.78rem;margin-top:6px">Senha atual: <code>{{ item.password or '-' }}</code></div>
            </div>
            <div>
              <label>Validade total</label>
              <input name="set_duration_value" type="number" min="0" value="{{ item.duration_value or 0 }}">
            </div>
            <div>
              <label>Unidade da validade</label>
              <select name="set_duration_unit">
                <option value="minutes" {% if item.duration_unit == 'minutes' %}selected{% endif %}>Minutos</option>
                <option value="hours" {% if item.duration_unit == 'hours' %}selected{% endif %}>Horas</option>
                <option value="days" {% if item.duration_unit in ('', 'days', None) %}selected{% endif %}>Dias</option>
                <option value="seconds" {% if item.duration_unit == 'seconds' %}selected{% endif %}>Segundos</option>
              </select>
            </div>
            <div>
              <label>Adicionar tempo</label>
              <input name="add_duration_value" type="number" min="0" value="0">
            </div>
            <div>
              <label>Unidade</label>
              <select name="add_duration_unit">
                <option value="minutes">Minutos</option>
                <option value="hours">Horas</option>
                <option value="days" selected>Dias</option>
                <option value="seconds">Segundos</option>
              </select>
            </div>
            <div class="full">
              <label>Observacoes</label>
              <textarea name="notes" placeholder="Notas internas">{{ item.notes or '' }}</textarea>
            </div>
          </div>
          <button class="btn-secondary" type="submit">Salvar edicao da licenca</button>
        </form>
      </div>
      {% endfor %}
    </div>
    {% endif %}
    </section>
    <section id="tab-saldos" class="tab-panel">
      <div class="card">
        <h2>Saldos por cliente</h2>
        <div style="overflow-x:auto">
          <table>
            <thead><tr><th>Cliente</th><th>Key</th><th>Saldo atual</th><th>Delta do dia</th><th>Volume apostado</th></tr></thead>
            <tbody>
              {% for item in licenses %}
              <tr><td>{{ item.customer_name or '-' }}</td><td><code>{{ item.key }}</code></td><td>{{ item.admin.balance }}</td><td>{{ item.admin.balance_delta }}</td><td>{{ item.admin.total_amount }}</td></tr>
              {% endfor %}
            </tbody>
          </table>
        </div>
      </div>
    </section>
    <section id="tab-ordens" class="tab-panel">
      <div class="card">
        <h2>Ordens</h2>
        <div style="overflow-x:auto">
          <table>
            <thead><tr><th>Cliente</th><th>Enviadas</th><th>Erros</th><th>Wins</th><th>Losses</th><th>Ultimo evento</th><th>Detalhe</th></tr></thead>
            <tbody>
              {% for item in licenses %}
              <tr>
                <td>{{ item.customer_name or item.username or item.key }}</td>
                <td>{{ item.admin.orders_sent }}</td>
                <td>{{ item.admin.orders_error }}</td>
                <td>{{ item.admin.wins }}</td>
                <td>{{ item.admin.losses }}</td>
                <td>{{ item.admin.last_event_time }} - {{ item.admin.last_event_status }}</td>
                <td>{{ item.admin.last_event_detail }}</td>
              </tr>
              {% endfor %}
            </tbody>
          </table>
        </div>
      </div>
    </section>
    <section id="tab-telegram" class="tab-panel">
      <div class="card">
        <h2>Telegram</h2>
        <div style="overflow-x:auto">
          <table>
            <thead><tr><th>Cliente</th><th>Pronto</th><th>Placar Telegram</th><th>Bot rodando</th><th>Mercados ativos</th></tr></thead>
            <tbody>
              {% for item in licenses %}
              <tr><td>{{ item.customer_name or item.username or item.key }}</td><td>{{ item.admin.telegram_ready }}</td><td>{{ item.admin.telegram_wins }} / {{ item.admin.telegram_losses }}</td><td>{{ item.admin.bot_running }}</td><td>{{ item.admin.enabled_markets }}</td></tr>
              {% endfor %}
            </tbody>
          </table>
        </div>
      </div>
    </section>
    <section id="tab-analytics" class="tab-panel">
      <div class="card">
        <h2>Analytics</h2>
        <div style="overflow-x:auto">
          <table>
            <thead><tr><th>Cliente</th><th>Sinais salvos</th><th>Wins</th><th>Losses</th><th>Ordens enviadas</th><th>Erros de ordem</th></tr></thead>
            <tbody>
              {% for item in licenses %}
              <tr><td>{{ item.customer_name or item.username or item.key }}</td><td>{{ item.admin.analytics_count }}</td><td>{{ item.admin.wins }}</td><td>{{ item.admin.losses }}</td><td>{{ item.admin.orders_sent }}</td><td>{{ item.admin.orders_error }}</td></tr>
              {% endfor %}
            </tbody>
          </table>
        </div>
      </div>
    </section>
    <section id="tab-banco" class="tab-panel">
      <div class="card">
        <h2>Banco SQLite</h2>
        <p class="muted">Licencas ficam em <code>licenses.sqlite</code>. Dados dos clientes ficam em <code>client_data.sqlite</code>. Os JSONs antigos continuam como fallback de migracao.</p>
        <div style="overflow-x:auto">
          <table>
            <thead><tr><th>Cliente</th><th>Key</th><th>Registros no SQLite</th><th>Pasta antiga</th></tr></thead>
            <tbody>
              {% for item in licenses %}
              <tr><td>{{ item.customer_name or item.username or '-' }}</td><td><code>{{ item.key }}</code></td><td>{{ item.admin.db_files }}</td><td><code>{{ item.admin.folder }}</code></td></tr>
              {% endfor %}
            </tbody>
          </table>
        </div>
      </div>
    </section>
  </div>
  <script>
    function showTab(name, btn) {
      document.querySelectorAll('.tab-panel').forEach(el => el.classList.remove('active'));
      document.querySelectorAll('.tab-btn').forEach(el => el.classList.remove('active'));
      const panel = document.getElementById('tab-' + name);
      if (panel) panel.classList.add('active');
      if (btn) btn.classList.add('active');
    }
  </script>
</body>
</html>
"""


def create_admin_app():
    app = Flask(__name__)

    @app.get("/")
    def index():
        flash = None
        if request.args.get("msg"):
            flash = {
                "ok": request.args.get("ok") == "1",
                "message": request.args.get("msg"),
            }
        items = load_store().get("licenses") or []
        items = sorted(items, key=lambda item: str(item.get("created_at") or ""), reverse=True)
        summary_total_balance = 0.0
        summary_active = 0
        summary_orders_sent = 0
        for item in items:
            item["status"] = license_status(item)
            if not item.get("duration_label"):
                value = item.get("duration_value") or 0
                unit = item.get("duration_unit") or "days"
                if value:
                    item["duration_label"] = duration_label(int(value), unit)
            item["expires_at"] = format_dt_br(item.get("expires_at"))
            item["admin"] = collect_license_usage(item)
            if item["status"] == "active":
                summary_active += 1
            summary_orders_sent += int(item["admin"].get("orders_sent") or 0)
            try:
                bal_text = str(item["admin"].get("balance") or "")
                summary_total_balance += float(bal_text.split()[0]) if bal_text and bal_text != "-" else 0.0
            except Exception:
                pass
        summary = {
            "total": len(items),
            "active": summary_active,
            "orders_sent": summary_orders_sent,
            "total_balance": format_money(summary_total_balance) if summary_total_balance else "-",
        }
        return render_template_string(ADMIN_TEMPLATE, licenses=items, flash=flash, summary=summary)

    @app.post("/create")
    def create_license():
        customer_name = str(request.form.get("customer_name") or "").strip()
        username = str(request.form.get("username") or "").strip()
        password = str(request.form.get("password") or "").strip()
        plan_name = str(request.form.get("plan_name") or "").strip()
        notes = str(request.form.get("notes") or "").strip()
        try:
            duration_value = max(1, int(request.form.get("duration_value") or 30))
        except ValueError:
            duration_value = 30
        duration_unit = normalize_duration_unit(request.form.get("duration_unit") or "days")
        entry = create_entry(customer_name, plan_name, duration_value, duration_unit, notes, username, password)
        return redirect(url_for(
            "index",
            ok=1,
            msg=f"Licenca gerada: {entry['key']} | usuario: {entry['username']} | senha: {entry['password']}",
        ))

    @app.post("/revoke/<license_key>")
    def revoke_license(license_key: str):
        ok, message = revoke_key(license_key)
        return redirect(url_for("index", ok=1 if ok else 0, msg=message))

    @app.post("/release-device/<license_key>")
    def release_license_device(license_key: str):
        ok, message = release_device_lock(license_key)
        return redirect(url_for("index", ok=1 if ok else 0, msg=message))

    @app.post("/edit/<license_key>")
    def edit_license(license_key: str):
        try:
            set_duration_value = max(0, int(request.form.get("set_duration_value") or 0))
        except ValueError:
            set_duration_value = 0
        try:
            add_duration_value = max(0, int(request.form.get("add_duration_value") or 0))
        except ValueError:
            add_duration_value = 0
        ok, message = edit_license_entry(
            license_key,
            customer_name=str(request.form.get("customer_name") or ""),
            username=str(request.form.get("username") or ""),
            password=str(request.form.get("password") or ""),
            plan_name=str(request.form.get("plan_name") or ""),
            notes=str(request.form.get("notes") or ""),
            set_duration_value=set_duration_value,
            set_duration_unit=str(request.form.get("set_duration_unit") or "days"),
            add_duration_value=add_duration_value,
            add_duration_unit=str(request.form.get("add_duration_unit") or "days"),
        )
        return redirect(url_for("index", ok=1 if ok else 0, msg=message))

    return app


def cmd_serve(args):
    app = create_admin_app()
    print(f"Admin de licencas em http://127.0.0.1:{args.port}")
    app.run(host="127.0.0.1", port=args.port, debug=False, use_reloader=False)


def build_parser():
    parser = argparse.ArgumentParser(description="Gerador local de license key")
    sub = parser.add_subparsers(dest="command", required=True)

    create = sub.add_parser("create", help="Cria uma nova license")
    create.add_argument("--customer", default="", help="Nome do cliente")
    create.add_argument("--username", default="", help="Usuario de login do cliente")
    create.add_argument("--password", default="", help="Senha inicial do cliente")
    create.add_argument("--plan", default="", help="Nome do plano")
    create.add_argument("--amount", type=int, default=30, help="Quantidade da validade")
    create.add_argument(
        "--unit",
        choices=["seconds", "minutes", "hours", "days"],
        default="days",
        help="Unidade da validade",
    )
    create.add_argument("--notes", default="", help="Observacoes")
    create.set_defaults(func=cmd_create)

    ls = sub.add_parser("list", help="Lista licenses")
    ls.set_defaults(func=cmd_list)

    revoke = sub.add_parser("revoke", help="Revoga uma license")
    revoke.add_argument("key", help="License key")
    revoke.set_defaults(func=cmd_revoke)

    serve = sub.add_parser("serve", help="Abre o admin local em Flask")
    serve.add_argument("--port", type=int, default=8787, help="Porta local do admin")
    serve.set_defaults(func=cmd_serve)

    return parser


def main(argv=None):
    parser = build_parser()
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv:
        argv = ["serve"]
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
