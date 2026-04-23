"""
Servidor Flask - Oddsync (Rodovia 5min + Rua 4m40s)
Conecta em tempo real via Pusher WebSocket e serve historico via web.
"""

import json
import hashlib
import html
import math
import os
import random
import re
import secrets
import socket
import time
import threading
import queue
import subprocess
import base64
import contextlib
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from collections import deque

import websocket
from flask import Flask, render_template, Response, stream_with_context, request, jsonify, session, redirect, url_for, render_template_string, has_request_context

from client_db import get_json as _client_db_get_json, set_json as _client_db_set_json
from license_db import load_store as _sqlite_load_license_store, save_store as _sqlite_save_license_store


TELEGRAM_SEND_MIN_GAP_SECONDS = 0.9
TELEGRAM_SEND_MAX_RETRIES = 3
TELEGRAM_RETRYABLE_HTTP_CODES = {429, 500, 502, 503, 504}
_telegram_send_queue = queue.Queue()
_telegram_sender_lock = threading.Lock()
_telegram_sender_started = False
_telegram_delivery_lock = threading.Lock()
_telegram_delivery_state = {"last_ts": 0.0}
_automation_open_position_check_lock = threading.Lock()
_automation_open_position_last_check: dict[str, float] = {}


def _strip_telegram_html(text: str) -> str:
    plain = re.sub(r"<[^>]+>", "", str(text or ""))
    plain = html.unescape(plain)
    plain = "\n".join(line.rstrip() for line in plain.splitlines())
    return plain.strip()


def _telegram_parse_error_details(ex: Exception) -> tuple[str, int | None, float | None, bool]:
    error = str(ex) or "Falha ao enviar mensagem para o Telegram."
    http_code = None
    retry_after = None
    parse_error = False

    if isinstance(ex, urllib.error.HTTPError):
        http_code = int(ex.code)
        body_text = ""
        try:
            body_text = ex.read().decode("utf-8", errors="replace")
        except Exception:
            body_text = ""

        payload = {}
        if body_text:
            try:
                payload = json.loads(body_text)
            except Exception:
                payload = {}

        description = str(payload.get("description") or "").strip()
        if description:
            error = description
        elif body_text.strip():
            error = body_text.strip()

        params = payload.get("parameters") or {}
        retry_raw = params.get("retry_after")
        try:
            if retry_raw is not None:
                retry_after = float(retry_raw)
        except (TypeError, ValueError):
            retry_after = None

        lowered = error.lower()
        parse_error = http_code == 400 and (
            "can't parse entities" in lowered
            or "unsupported start tag" in lowered
            or "can't find end tag" in lowered
        )

    return error, http_code, retry_after, parse_error


def _telegram_wait_delivery_slot():
    elapsed = time.time() - float(_telegram_delivery_state.get("last_ts") or 0.0)
    wait_seconds = TELEGRAM_SEND_MIN_GAP_SECONDS - elapsed
    if wait_seconds > 0:
        time.sleep(wait_seconds)


def _telegram_sender_worker():
    while True:
        payload = _telegram_send_queue.get()
        previous_key = str(getattr(_thread_license_context, "license_key", "") or "")
        try:
            payload_key = str(payload.get("license_key") or "").strip().upper()
            if payload_key:
                _thread_license_context.license_key = payload_key
            ok, error = _telegram_send_now(
                payload.get("text") or "",
                market_key=payload.get("market_key"),
                parse_mode=payload.get("parse_mode"),
            )
            if ok and payload.get("post_send_status"):
                _set_telegram_status(
                    str(payload.get("post_send_status") or "sent"),
                    str(payload.get("post_send_message") or "Mensagem enviada com sucesso."),
                    market_key=payload.get("market_key"),
                    sent=bool(payload.get("post_send_mark_sent", True)),
                )
            if not ok and error:
                print(f"[Telegram] Erro ao enviar: {error}", flush=True)
        except Exception as ex:
            print(f"[Telegram] Falha inesperada no envio: {ex}", flush=True)
        finally:
            if previous_key:
                _thread_license_context.license_key = previous_key
            elif hasattr(_thread_license_context, "license_key"):
                delattr(_thread_license_context, "license_key")
            _telegram_send_queue.task_done()


def _ensure_telegram_sender_worker():
    global _telegram_sender_started
    with _telegram_sender_lock:
        if _telegram_sender_started:
            return
        threading.Thread(
            target=_telegram_sender_worker,
            daemon=True,
            name="telegram-send-worker",
        ).start()
        _telegram_sender_started = True



def send_telegram(
    text: str,
    market_key: str | None = None,
    parse_mode: str | None = "HTML",
    *,
    wait: bool = False,
    post_send_status: str | None = None,
    post_send_message: str | None = None,
    post_send_mark_sent: bool = True,
):
    """Envia mensagem para o Telegram em background usando a config salva no painel."""
    license_key = _current_license_key()

    if wait:
        previous_key = str(getattr(_thread_license_context, "license_key", "") or "")
        try:
            if license_key:
                _thread_license_context.license_key = license_key
            ok, error = _telegram_send_now(text, market_key=market_key, parse_mode=parse_mode)
            if ok and post_send_status:
                _set_telegram_status(
                    post_send_status,
                    post_send_message or "Mensagem enviada com sucesso.",
                    market_key=market_key,
                    sent=post_send_mark_sent,
                )
            if not ok and error:
                print(f"[Telegram] Erro ao enviar: {error}", flush=True)
            return ok, error
        except Exception as ex:
            print(f"[Telegram] Falha inesperada no envio: {ex}", flush=True)
            return False, str(ex)
        finally:
            if previous_key:
                _thread_license_context.license_key = previous_key
            elif hasattr(_thread_license_context, "license_key"):
                delattr(_thread_license_context, "license_key")

    _ensure_telegram_sender_worker()
    _telegram_send_queue.put({
        "text": text,
        "market_key": market_key,
        "parse_mode": parse_mode,
        "license_key": license_key,
        "post_send_status": post_send_status,
        "post_send_message": post_send_message,
        "post_send_mark_sent": post_send_mark_sent,
    })


def _atomic_json_write(path: str, payload, *, indent: int = 2):
    base_dir = os.path.dirname(path) or "."
    filename = os.path.basename(path)
    last_error = None

    for attempt in range(6):
        tmp_path = os.path.join(
            base_dir,
            f".{filename}.{os.getpid()}.{threading.get_ident()}.{int(time.time() * 1000)}.{attempt}.tmp",
        )
        try:
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=indent)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, path)
            return
        except PermissionError as ex:
            last_error = ex
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except Exception:
                pass
            time.sleep(0.05 * (attempt + 1))
        except Exception:
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except Exception:
                pass
            raise

    if last_error is not None:
        raise last_error


# Dedup de sinais: não envia 2 sinais para o mesmo mercado em < 90s
_last_signal_ts: dict = {}

# Dedup de sinais AO VIVO: envia no máximo 1x por rodada
_live_signal_mid: dict = {}  # market_id da rodada em que o sinal ao vivo foi enviado
_live_signal_rec: dict = {}  # previsão enviada no sinal ao vivo


# AJUSTES RAPIDOS

# Quantos dias de historico o bot tenta manter/carregar.
HISTORY_LOOKBACK_DAYS = 14

# Quantidade alvo de registros guardados por mercado.
HISTORY_TARGET_RECORDS = 2500

# Se True, tenta puxar historico antigo em background.
AUTO_BACKFILL_HISTORY = True

# Quantos segundos espera apos iniciar o app para comecar o backfill.
BACKFILL_START_DELAY_SECONDS = 20

# De quanto em quanto tempo tenta o backfill de novo.
BACKFILL_RETRY_INTERVAL_SECONDS = 30

# Se True, permite mandar sinal logo na abertura da rodada.
SEND_OPENING_SIGNALS = False

# Stop de perdas consecutivas: pausa o bot por N rodadas apos X derrotas seguidas.
CONSECUTIVE_LOSS_STOP_ENABLED = False
CONSECUTIVE_LOSS_THRESHOLD = 1       # derrotas seguidas para pausar
CONSECUTIVE_LOSS_PAUSE_SECONDS = 600   # segundos de pausa (10 min)

# Stop diario de saldo: para o bot se o saldo cair X% em relacao ao inicio do dia.
DAILY_BALANCE_STOP_ENABLED = True
DAILY_BALANCE_STOP_PCT = 15.0        # pct de queda do saldo para parar (ex: 15 = 15%)

# Guarda de ritmo para sinais AO VIVO (complementa a guarda de ABERTURA).
LIVE_RHYTHM_GUARD_ENABLED = True
LIVE_RHYTHM_GUARD_MIN_STREAK_AGAINST = 3  # derrotas seguidas de ritmo antes de bloquear
LIVE_RHYTHM_GUARD_GAP_PCT = 35           # % de distancia da meta para bloquear

# Janela de comparacao do mesmo horario semanal, em segundos.
WEEKLY_SLOT_WINDOW_SECONDS = 3600

# Total de segundos de uma semana.
WEEK_SECONDS = 7 * 24 * 3600

# Minimo de amostras para usar o padrao do mesmo horario semanal.
MIN_WEEKLY_SLOT_SAMPLES = 4

# Quantas threads usa para puxar historico mais rapido.
HISTORY_FETCH_WORKERS = 16

# Quantos ids tenta buscar por lote no backfill.
HISTORY_FETCH_BATCH_SIZE = 240

# Fallback de refresh para mercados de preco quando o WS nao entrega currentPrice.
PRICE_MARKET_POLL_SECONDS = 3

# Percentual minimo da rodada ja passada para avaliar sinal AO VIVO.
LIVE_MIN_ELAPSED_RATIO = 0.40

# Minimo de segundos ja passados para avaliar AO VIVO.
LIVE_MIN_ELAPSED_SECONDS = 100

# Minimo de segundos restantes para ainda permitir AO VIVO.
LIVE_MIN_REMAINING_SECONDS = 60

# Minimo de amostras para um padrao entrar no backtest/ranking.
MIN_PATTERN_BACKTEST_SAMPLES = 8

# Acerto minimo do padrao no historico para ser considerado.
MIN_PATTERN_BACKTEST_ACC = 58

# Confianca minima do sinal AO VIVO.
LIVE_SIGNAL_MIN_CONF = 90

# Odd maxima aceita para enviar qualquer sinal. Sinal bloqueado se odd > este valor.
LIVE_MAX_ODD = 9.99

# Faixa de odd do book aceita para disparar sinal (multiplier).
# Sinal so e enviado se o melhor ask do book estiver dentro desta faixa.
SIGNAL_BOOK_ODD_MIN = 1.25   # ex: 1.25x
SIGNAL_BOOK_ODD_MAX = 1.40   # ex: 1.40x

# Maximo de entradas por hora. Apos atingir, bot para ate o proximo horario.
SIGNAL_MAX_ENTRIES_PER_HOUR = 3

# ── Aprendizado adaptativo de bias ──
BIAS_MIN_SAMPLES       = 20   # mínimo de amostras por hora/meta para aplicar bias
BIAS_COMBO_MIN_SAMPLES = 15   # mínimo para combos hora×meta
BIAS_MIN_DEVIATION     = 6.0  # desvio mínimo de 50% (pp) para considerar
BIAS_UPDATE_INTERVAL   = 3600 # recalcula a cada 60 min (segundos)

# Confianca minima do padrao historico para confirmar o AO VIVO.
LIVE_PATTERN_CONFIRM_MIN_CONF = 58

# Janela minima/maxima para medir se o contador acelerou ou desacelerou.
LIVE_TREND_LOOKBACK_MIN_SECONDS = 18
LIVE_TREND_LOOKBACK_MAX_SECONDS = 55

# Quanto a confianca do AO VIVO sobe quando o ritmo recente acelera a favor.
LIVE_TREND_ACCEL_BOOST = 4

# Quanto a confianca do AO VIVO cai quando o ritmo recente desacelera contra.
LIVE_TREND_DECAY_PENALTY = 7

# Quantos segundos analisa para medir a pressao da odd.
ODD_PRESSURE_LOOKBACK_SECONDS = 90

# Variacao minima da odd para considerar que houve pressao.
ODD_PRESSURE_MIN_DELTA = 0.01

# Valor minimo aceito pela API para enviar uma ordem.
AUTOMATION_MIN_ORDER_TOTAL = 1.01

# Multiplicador minimo da odd para a automacao aceitar entrar.
# Ex.: 1.20x = nao entra se a odd for < 1.20x.
AUTOMATION_MIN_ENTRY_MULTIPLIER = 1.20

# Multiplicador maximo da odd para a automacao aceitar entrar.
# Ex.: 1.55x = nao entra se a odd for > 1.55x.
AUTOMATION_MAX_ENTRY_MULTIPLIER = 1.55

# Odd minima para disparar uma 2a ordem quando aparecer "odd bugada" no book.
# Ex.: 1.50x = so duplica a entrada se a ask real estiver muito boa.

# Valor usado no botao de teste minimo.
AUTOMATION_TEST_ORDER_AMOUNT = AUTOMATION_MIN_ORDER_TOTAL
AUTOMATION_TEST_ORDER_COOLDOWN_SECONDS = 8
_automation_test_order_lock = threading.Lock()
_automation_test_order_recent: dict[str, float] = {}

# Se True, tenta procurar odd rapida/bugada no inicio.
AUTOMATION_SNIPER_ENABLED = True

# Preco maximo da odd no sniper. Ex.: 0.33 ~= 3x.
AUTOMATION_SNIPER_MAX_PRICE = 0.33

# Ate quantos segundos de rodada o sniper tenta pegar a odd.
AUTOMATION_SNIPER_MAX_ELAPSED_SECONDS = 45

# Quantas ofertas do topo do book o sniper analisa.
AUTOMATION_SNIPER_ORDERBOOK_LIMIT = 5

# Soma maxima de MAIS + ATE para tentar arbitragem.
AUTOMATION_ARB_DEFAULT_MAX_SUM = 0.97
AUTOMATION_DEFAULT_API_BASE_URLS = (
    "https://app.palpitano.com/api/v1",
    "https://www.app.palpitano.com/api/v1",
)


def _normalize_automation_api_base_url(raw: str | None) -> str | None:
    text = str(raw or "").strip()
    if not text:
        return None
    text = text.strip(" ,;")
    if not text:
        return None
    if "://" not in text:
        text = f"https://{text}"
    try:
        parsed = urllib.parse.urlparse(text)
    except Exception:
        return None
    if not parsed.netloc:
        return None
    host = parsed.netloc.strip().strip("/")
    if not host or any(ch in host for ch in ",; "):
        return None
    host_only = host.split("@")[-1].split(":")[0].strip().lower()
    if "." not in host_only and host_only not in {"localhost", "127.0.0.1"}:
        return None
    path = (parsed.path or "").rstrip("/")
    if not path:
        path = "/api/v1"
    elif not path.endswith("/api/v1"):
        path = f"{path}/api/v1"
    return urllib.parse.urlunparse((parsed.scheme or "https", host, path, "", "", ""))


def _automation_api_base_urls() -> list[str]:
    raw_value = (
        os.getenv("ODDSYNC_ORDERBOOK_API_BASE_URLS")
        or os.getenv("ODDSYNC_ORDERBOOK_API_BASE_URL")
        or ""
    )
    parts: list[str] = []
    if raw_value:
        cleaned = str(raw_value).replace("\r", ",").replace("\n", ",").replace(";", ",")
        parts.extend(piece for piece in cleaned.split(",") if piece.strip())
    parts.extend(AUTOMATION_DEFAULT_API_BASE_URLS)

    urls: list[str] = []
    seen: set[str] = set()
    for part in parts:
        normalized = _normalize_automation_api_base_url(part)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        urls.append(normalized)
    return urls or list(AUTOMATION_DEFAULT_API_BASE_URLS)

# Intervalo em segundos entre checagens de arbitragem.
AUTOMATION_ARB_CHECK_SECONDS = 2

# Intervalo em segundos entre checagens de cashout.
AUTOMATION_CASHOUT_CHECK_SECONDS = 2

# Tempo minimo entre uma tentativa de cashout e outra.
AUTOMATION_CASHOUT_COOLDOWN_SECONDS = 8

# Margem de seguranca antes da meta de cashout.
AUTOMATION_CASHOUT_NEAR_BUFFER_PCT = 0.5

# A cada quantas mensagens o Telegram tenta mandar dica de banca.
TELEGRAM_BANKROLL_TIP_EVERY = 4
# Lista de frases rotativas de gestao de banca no Telegram.
TELEGRAM_BANKROLL_TIPS = [
    "use valor fixo e nao aumenta a mao no impulso.",
    "se o sinal passou, espera o proximo e evita entrar atrasado.",
    "prefira horarios e locais com padrao mais forte no historico.",
    "se o dia ficou ruim, reduz o risco e evita recuperar no emocional.",
]

# Se bater esse saldo de vitorias no dia, pode mandar lembrete de meta.
TELEGRAM_DAILY_GOAL_NET_WINS = 2

# Se chegar nesse saldo de derrotas no dia, pode mandar alerta de stop.
TELEGRAM_DAILY_STOP_NET_LOSSES = -2

# Intervalo entre lembretes automativos de meta no Telegram.
TELEGRAM_GOAL_REMINDER_INTERVAL_SECONDS = 600

_sent_signal_mid: dict = {}
_sent_signal_rec: dict = {}
_sent_signal_kind: dict = {}
_sent_signal_conf: dict = {}
_sent_signal_odd: dict = {}
_sent_signal_strategy: dict = {}
_bankroll_tip_count: dict = {}
_bankroll_tip_index: dict = {}
_goal_reminder_ts: dict = {}
_auto_order_mid: dict = {}
_auto_arb_last_try_ts: dict = {}
_auto_cashout_last_try_ts: dict = {}
_automation_notify_state = {"running": None, "ts": 0.0}
_telegram_result_notified_mid: dict = {}
_AUTOMATION_UNSET = object()
_pending_signal_lock = threading.RLock()
_order_attempt_dedupe_lock = threading.RLock()
_recent_order_attempts: dict[str, float] = {}
AUTOMATION_ORDER_ATTEMPT_DEDUPE_SECONDS = 20.0


def _runtime_scope_key(key: str | None = None) -> str:
    clean_key = str(key or _current_license_key() or "").strip().upper()
    return clean_key or "__global__"


def _runtime_bucket(store: dict, key: str | None = None) -> dict:
    scope = _runtime_scope_key(key)
    bucket = store.get(scope)
    if not isinstance(bucket, dict):
        bucket = {}
        store[scope] = bucket
    return bucket


@contextlib.contextmanager
def _license_thread_context(key: str | None):
    previous_key = str(getattr(_thread_license_context, "license_key", "") or "")
    clean_key = str(key or "").strip().upper()
    try:
        if clean_key:
            _thread_license_context.license_key = clean_key
        yield clean_key
    finally:
        if previous_key:
            _thread_license_context.license_key = previous_key
        elif hasattr(_thread_license_context, "license_key"):
            delattr(_thread_license_context, "license_key")


def _history_cutoff(now_dt: datetime | None = None) -> datetime:
    now_dt = now_dt or datetime.now()
    return now_dt - timedelta(days=HISTORY_LOOKBACK_DAYS, seconds=WEEKLY_SLOT_WINDOW_SECONDS)


def _entry_result(entry: dict) -> str | None:
    result = str(entry.get("result") or "").upper()
    if "MAIS" in result or "SOBE" in result or "UP" in result:
        return "MAIS"
    if "ATE" in result or "ATÉ" in result or "DESCE" in result or "DOWN" in result:
        return "ATE"
    return None


def _display_result(mk: str, internal: str | None) -> str | None:
    """Retorna o label de exibição correto para o mercado (ex: SOBE/DESCE para bitcoin)."""
    if internal is None:
        return None
    labels = MARKET_CONFIGS.get(mk, {}).get("outcome_labels", {})
    return labels.get(internal, internal)


def _entry_datetime(entry: dict) -> datetime | None:
    date_str = str(entry.get("date") or "").strip()
    time_str = str(entry.get("time") or "").strip()
    if not date_str or not time_str:
        return None

    time_str = time_str.split(".")[0]
    if len(time_str) == 5:
        time_str = f"{time_str}:00"
    elif len(time_str) > 8:
        time_str = time_str[:8]

    try:
        return datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def _history_key(entry: dict) -> str:
    market_id = entry.get("market_id")
    if market_id:
        return str(market_id)
    return "|".join([
        str(entry.get("date") or ""),
        str(entry.get("time") or ""),
        str(entry.get("location") or ""),
        str(entry.get("result") or ""),
    ])


def _pending_signal_payload() -> dict:
    sent_mid = _runtime_bucket(_sent_signal_mid)
    sent_rec = _runtime_bucket(_sent_signal_rec)
    sent_kind = _runtime_bucket(_sent_signal_kind)
    sent_conf = _runtime_bucket(_sent_signal_conf)
    sent_odd = _runtime_bucket(_sent_signal_odd)
    sent_strategy = _runtime_bucket(_sent_signal_strategy)
    return {
        mk: {
            "market_id": str(sent_mid.get(mk) or ""),
            "rec": sent_rec.get(mk),
            "kind": sent_kind.get(mk),
            "conf": sent_conf.get(mk),
            "odd": sent_odd.get(mk),
            "strategy": sent_strategy.get(mk),
        }
        for mk in MARKET_CONFIGS
        if sent_mid.get(mk)
    }


def _largest_id_gap(known_ids: list[int]) -> tuple[int, int, int]:
    if len(known_ids) < 2:
        return 0, 0, 0

    ordered = sorted(set(known_ids), reverse=True)
    best_high = 0
    best_low = 0
    best_gap = 0
    for high_id, low_id in zip(ordered, ordered[1:]):
        gap = high_id - low_id
        if gap > best_gap:
            best_high = high_id
            best_low = low_id
            best_gap = gap
    return best_high, best_low, best_gap


def _merge_history_entry(base: dict, new_entry: dict) -> dict:
    merged = dict(base)
    for key, value in new_entry.items():
        if value is not None and value != "":
            merged[key] = value
    if not base.get("historical") or not new_entry.get("historical"):
        merged.pop("historical", None)
    return merged


def _sort_key(entry: dict) -> tuple:
    entry_dt = _entry_datetime(entry) or datetime.min
    market_id = str(entry.get("market_id") or "")
    return entry_dt, market_id


def _rebuild_last_results(mk: str):
    last_results[mk].clear()
    for entry in histories[mk]:
        result = _entry_result(entry)
        if not result:
            continue
        last_results[mk].append(result)
        maxlen = last_results[mk].maxlen
        if maxlen is not None and len(last_results[mk]) >= maxlen:
            break


def _normalize_history(mk: str):
    cutoff = _history_cutoff()
    dedup: dict[str, dict] = {}
    undated: list[dict] = []

    for entry in list(histories[mk]):
        entry_dt = _entry_datetime(entry)
        if entry_dt is None:
            undated.append(entry)
            continue

        key = _history_key(entry)
        if key in dedup:
            dedup[key] = _merge_history_entry(dedup[key], entry)
        else:
            dedup[key] = dict(entry)

    ordered = sorted(dedup.values(), key=_sort_key, reverse=True)
    if len(ordered) >= HISTORY_TARGET_RECORDS:
        recent_count = sum(
            1
            for entry in ordered
            if (_entry_datetime(entry) or datetime.min) >= cutoff
        )
        keep_count = max(HISTORY_TARGET_RECORDS, recent_count)
        ordered = ordered[:keep_count]

    if undated:
        ordered.extend(undated)

    histories[mk].clear()
    histories[mk].extend(ordered)
    _rebuild_last_results(mk)


def _weekly_slot_pool(hist: list[dict], now_dt: datetime) -> list[str]:
    now_week_pos = (
        now_dt.weekday() * 86400
        + now_dt.hour * 3600
        + now_dt.minute * 60
        + now_dt.second
    )
    cutoff = _history_cutoff(now_dt)
    pool: list[str] = []

    for entry in hist:
        result = _entry_result(entry)
        if not result:
            continue

        entry_dt = _entry_datetime(entry)
        if entry_dt is None or entry_dt > now_dt or entry_dt < cutoff:
            continue

        entry_week_pos = (
            entry_dt.weekday() * 86400
            + entry_dt.hour * 3600
            + entry_dt.minute * 60
            + entry_dt.second
        )
        delta_seconds = (now_week_pos - entry_week_pos) % WEEK_SECONDS
        if delta_seconds <= WEEKLY_SLOT_WINDOW_SECONDS:
            pool.append(result)

    return pool


def _majority_probability(results: list[str]) -> tuple[str, int, int]:
    total = len(results)
    mais_count = sum(1 for result in results if result == "MAIS")
    prob_mais = (mais_count + 1) / (total + 2)
    rec = "MAIS" if prob_mais >= 0.5 else "ATE"
    conf_pct = int(round(max(prob_mais, 1 - prob_mais) * 100))
    raw_mais_pct = int(round((mais_count / total) * 100)) if total else 0
    return rec, conf_pct, raw_mais_pct


def _current_context_value(mk: str, field: str, fallback):
    value = states[mk].get(field)
    return fallback if value in (None, "") else value


def _format_odd_value(value) -> str | None:
    try:
        return f"{float(value):.2f}"
    except (TypeError, ValueError):
        return None


def _odd_multiplier_text(value) -> str | None:
    mult = _odd_multiplier_num(value)
    if mult is None:
        return None
    return f"{mult:.2f}x"


def _odd_multiplier_num(value) -> float | None:
    try:
        price = float(value)
    except (TypeError, ValueError):
        return None
    if price <= 0:
        return None
    return 1.0 / price


def _format_order_odd_display(value) -> str | None:
    odd = _format_odd_value(value)
    if not odd:
        return None
    mult = _odd_multiplier_text(value)
    return f"{odd} ({mult})" if mult else odd


def _expected_win_profit(amount: float | None, price) -> float | None:
    if amount is None or price is None:
        return None
    try:
        total = float(amount)
        odd_price = float(price)
    except (TypeError, ValueError):
        return None
    if total <= 0 or odd_price <= 0:
        return None
    return round(total * ((1.0 / odd_price) - 1.0), 2)


def _extract_market_odds(payload_data: dict) -> dict[str, float]:
    odds: dict[str, float] = {}
    for selection in (payload_data or {}).get("selections", []):
        label = str(selection.get("label") or selection.get("code") or "")
        side = _entry_result({"result": label})
        odd_value = _as_float(selection.get("price"))
        if side and odd_value is not None:
            odds[side] = odd_value
    return odds


def _extract_selection_maps(
    payload_data: dict,
) -> tuple[dict[str, str], dict[str, int]]:
    selection_codes: dict[str, str] = {}
    selection_ids: dict[str, int] = {}
    for selection in (payload_data or {}).get("selections", []):
        label = str(selection.get("label") or selection.get("code") or "")
        side = _entry_result({"result": label})
        code = selection.get("code")
        sel_id = selection.get("id")
        if side and code:
            selection_codes[side] = str(code)
        if side and sel_id is not None:
            try:
                selection_ids[side] = int(sel_id)
            except (TypeError, ValueError):
                pass
    return selection_codes, selection_ids


def _fetch_market_odds(market_id) -> dict[str, float]:
    if not market_id:
        return {}

    payload = _api_get(
        f"https://app.palpitano.com/app/getSpecificMarketStats?id={market_id}"
    )
    if not payload:
        return {}

    return _extract_market_odds(payload.get("data") or {})


def _record_market_odds(
    mk: str,
    market_id,
    odds: dict[str, float],
    now_ts: float | None = None,
):
    if not market_id or not odds:
        return
    odds_history[mk].append({
        "market_id": str(market_id),
        "ts": now_ts or time.time(),
        "odds": dict(odds),
    })
    states[mk]["odds"] = {
        side: _format_odd_value(value)
        for side, value in odds.items()
    }


def _odd_pressure_text(
    mk: str,
    market_id,
    rec: str,
    current_odd: float | None = None,
) -> str:
    if not market_id or not rec:
        return "Pressao da odd: sem leitura"

    samples = [
        sample
        for sample in odds_history[mk]
        if sample.get("market_id") == str(market_id)
        and (sample.get("odds") or {}).get(rec) is not None
    ]
    if len(samples) < 2:
        return "Pressao da odd: aguardando historico"

    now_ts = time.time()
    base_sample = samples[0]
    for sample in samples:
        if now_ts - float(sample.get("ts") or now_ts) <= ODD_PRESSURE_LOOKBACK_SECONDS:
            base_sample = sample
            break

    base_odd = _as_float((base_sample.get("odds") or {}).get(rec))
    if current_odd is None:
        current_odd = _as_float((samples[-1].get("odds") or {}).get(rec))
    if base_odd is None or current_odd is None:
        return "Pressao da odd: sem leitura"

    delta = current_odd - base_odd
    elapsed = max(0, int(now_ts - float(base_sample.get("ts") or now_ts)))
    if abs(delta) < ODD_PRESSURE_MIN_DELTA:
        return (
            f"Pressao da odd: estavel "
            f"{base_odd:.2f}->{current_odd:.2f} em {elapsed}s"
        )

    direction = "subindo" if delta > 0 else "caindo"
    return (
        f"Pressao da odd: {direction} "
        f"{base_odd:.2f}->{current_odd:.2f} ({delta:+.2f}) em {elapsed}s"
    )


def _odds_site_payload(mk: str) -> dict:
    st = states[mk]
    odds = dict(st.get("odds") or {})
    market_id = st.get("market_id")
    return {
        "odds": odds,
        "odd_pressure": {
            rec: _odd_pressure_text(
                mk,
                market_id,
                rec,
                _as_float(odds.get(rec)),
            )
            for rec in ("MAIS", "ATE")
        },
    }


def _fetch_signal_odd(market_id, rec: str) -> str | None:
    if not market_id or not rec:
        return None

    return _format_odd_value(_fetch_market_odds(market_id).get(str(rec).upper()))


def _check_signal_book_odd(mk: str, market_id, rec: str) -> tuple[bool, str]:
    """Bloqueio de odd do book desativado: sempre permite sinal."""
    return True, "book odd guard desativado"


def _signals_sent_last_hour(mk: str) -> int:
    """Conta quantos sinais com aposta confirmada foram enviados na ultima hora.
    Sinais anteriores ao startup do bot nao sao contabilizados."""
    cutoff = max(datetime.now() - timedelta(hours=1), _bot_startup_time)
    with _analytics_lock:
        count = 0
        for e in _analytics_log:
            if e.get("market") != mk or e.get("blocked"):
                continue
            if not e.get("bet_placed"):
                continue
            dt = _entry_datetime(e)
            if dt is not None and dt >= cutoff:
                count += 1
        return count


def _compact_strategy_label(text: str | None) -> str | None:
    value = " ".join(str(text or "").replace("\n", " ").split()).strip()
    if not value:
        return None
    return value[:220].rstrip() + "..." if len(value) > 220 else value


def _remember_sent_signal(
    mk: str,
    market_id,
    rec: str,
    kind: str,
    conf_pct: int,
    odd: str | None = None,
    strategy: str | None = None,
):
    if not market_id or not rec:
        return
    _runtime_bucket(_sent_signal_mid)[mk] = str(market_id)
    _runtime_bucket(_sent_signal_rec)[mk] = rec
    _runtime_bucket(_sent_signal_kind)[mk] = kind
    _runtime_bucket(_sent_signal_conf)[mk] = int(conf_pct)
    _runtime_bucket(_sent_signal_odd)[mk] = odd
    _runtime_bucket(_sent_signal_strategy)[mk] = _compact_strategy_label(strategy)
    _save_pending_signal_cache()
    _analytics_add_entry(mk, market_id, rec, kind, int(conf_pct), odd, _compact_strategy_label(strategy))


def _consume_sent_signal(mk: str, market_id):
    sent_mid = _runtime_bucket(_sent_signal_mid)
    sent_rec = _runtime_bucket(_sent_signal_rec)
    sent_kind = _runtime_bucket(_sent_signal_kind)
    sent_conf = _runtime_bucket(_sent_signal_conf)
    sent_odd = _runtime_bucket(_sent_signal_odd)
    sent_strategy = _runtime_bucket(_sent_signal_strategy)
    if str(sent_mid.get(mk) or "") != str(market_id or ""):
        return None

    payload = {
        "rec": sent_rec.get(mk),
        "kind": sent_kind.get(mk),
        "conf": sent_conf.get(mk),
        "odd": sent_odd.get(mk),
        "strategy": sent_strategy.get(mk),
    }
    sent_mid.pop(mk, None)
    sent_rec.pop(mk, None)
    sent_kind.pop(mk, None)
    sent_conf.pop(mk, None)
    sent_odd.pop(mk, None)
    sent_strategy.pop(mk, None)
    _save_pending_signal_cache()
    return payload


def _peek_sent_signal(mk: str, market_id):
    sent_mid = _runtime_bucket(_sent_signal_mid)
    if str(sent_mid.get(mk) or "") != str(market_id or ""):
        return None
    return {
        "rec": _runtime_bucket(_sent_signal_rec).get(mk),
        "kind": _runtime_bucket(_sent_signal_kind).get(mk),
        "conf": _runtime_bucket(_sent_signal_conf).get(mk),
        "odd": _runtime_bucket(_sent_signal_odd).get(mk),
        "strategy": _runtime_bucket(_sent_signal_strategy).get(mk),
    }


def _telegram_bankroll_note(mk: str, force: bool = False) -> str:
    if not TELEGRAM_BANKROLL_TIPS or TELEGRAM_BANKROLL_TIP_EVERY <= 0:
        return ""

    score = telegram_score.get(mk) or {}
    wins = int(score.get("wins") or 0)
    losses = int(score.get("losses") or 0)
    net_score = wins - losses

    if False and net_score >= TELEGRAM_DAILY_GOAL_NET_WINS:
        return (
            "\n\n"
            "<i>💰 Gestao de banca: se ja bateu sua meta do dia, "
            "para e protege o lucro.</i>"
        )

    if net_score <= TELEGRAM_DAILY_STOP_NET_LOSSES:
        return (
            "\n\n"
            "<i>🛑 Gestao de banca: se o dia ja ficou pesado, "
            "respeita o stop e nao tenta recuperar no emocional.</i>"
        )

    _bankroll_tip_count[mk] = _bankroll_tip_count.get(mk, 0) + 1
    if (
        not force
        and _bankroll_tip_count[mk] % TELEGRAM_BANKROLL_TIP_EVERY != 0
    ):
        return ""

    tip_index = _bankroll_tip_index.get(mk, 0) % len(TELEGRAM_BANKROLL_TIPS)
    _bankroll_tip_index[mk] = tip_index + 1
    return f"\n\n<i>💡 Gestao de banca: {TELEGRAM_BANKROLL_TIPS[tip_index]}</i>"


def _send_result_telegram(
    mk: str,
    entry: dict,
    *,
    sent_signal: dict | None = None,
) -> bool:
    market_id = str(entry.get("market_id") or "")
    sent_signal = sent_signal or _peek_sent_signal(mk, market_id)
    if not sent_signal or not sent_signal.get("rec"):
        return False  # so envia resultado se houve sinal enviado nesta rodada
    telegram_result_notified_mid = _runtime_bucket(_telegram_result_notified_mid)
    if market_id and telegram_result_notified_mid.get(mk) == market_id:
        return False

    _check_day_reset()
    won = _entry_result(entry)
    market_name = MARKET_CONFIGS[mk]["name"]
    thr_str = str(entry.get("value_needed") or "?")
    is_price_tg = bool(MARKET_CONFIGS[mk].get("is_price_market"))
    if is_price_tg:
        _vn2 = entry.get("value_needed")
        _cnt2 = entry.get("count")
        thr_str = f"R$ {float(_vn2):.2f}" if _vn2 is not None else "?"
    loc_str = str(entry.get("location") or "").split(" — ")[0]
    ts = entry.get("time") or datetime.now().strftime("%H:%M:%S")
    final_cnt = entry.get("count")
    score_map = _load_score() if _has_session_license_context() else telegram_score
    score = score_map[mk]
    sinal_str = ""
    _out_labels = MARKET_CONFIGS[mk].get("outcome_labels", {"MAIS": "MAIS", "ATE": "ATÉ"})
    _lbl_mais = _out_labels.get("MAIS", "MAIS")
    _lbl_ate  = _out_labels.get("ATE", "ATÉ")

    if sent_signal and sent_signal.get("rec"):
        sinal_icon = f"🟢 {_lbl_mais}" if sent_signal["rec"] == "MAIS" else f"🔴 {_lbl_ate}"
        sinal_kind = sent_signal.get("kind") or "SINAL"
        sinal_conf = sent_signal.get("conf")
        sinal_odd = sent_signal.get("odd")
        sinal_strategy = sent_signal.get("strategy")
        sinal_str = f"Sinal avaliado: <b>{sinal_kind}</b> {sinal_icon}"
        if sinal_conf:
            sinal_str += f" ({sinal_conf}%)"
        if sinal_odd:
            sinal_str += f" | Odd <b>{sinal_odd}</b>"
        if sinal_strategy:
            sinal_str += f"\nEstratégia: <b>{sinal_strategy}</b>"

    effective_rec = sent_signal.get("rec") if sent_signal else None
    confirmed_bet = bool(
        effective_rec and _automation_has_confirmed_order(mk, market_id, rec=effective_rec)
    )
    blocked_order = (
        _automation_order_log_find_latest(
            mk,
            market_id,
            rec=effective_rec,
            statuses=("blocked",),
        )
        if effective_rec else
        None
    )
    if effective_rec and won and confirmed_bet:
        if won == effective_rec:
            acerto_icon = "✅"
            acerto_str = "Acertou!"
        else:
            acerto_icon = "❌"
            acerto_str = "Errou"
        total_sc = score["wins"] + score["losses"]
        acc_str = f"{int(score['wins']/total_sc*100)}%" if total_sc else "0%"
        placar_str = (
            f"{acerto_icon} {acerto_str}  |  Hoje: ✅ {score['wins']} "
            f"❌ {score['losses']} ({acc_str})"
        )
    elif effective_rec and won:
        resultado_sinal = "acertou" if won == effective_rec else "errou"
        total_sc = score["wins"] + score["losses"]
        acc_str = f"{int(score['wins']/total_sc*100)}%" if total_sc else "0%"
        blocked_detail = ""
        if blocked_order:
            blocked_reason = str(
                blocked_order.get("detail")
                or blocked_order.get("execution")
                or ""
            ).strip()
            if blocked_reason:
                blocked_detail = f"\nMotivo: {blocked_reason}"
        placar_str = (
            f"ℹ️ Sinal {resultado_sinal}, mas sem aposta confirmada nesta rodada.\n"
            f"Hoje: ✅ {score['wins']} ❌ {score['losses']} ({acc_str})"
            f"{blocked_detail}"
        )
    else:
        placar_str = (
            f"Hoje: ✅ {score['wins']} ❌ {score['losses']}"
            if (score["wins"] + score["losses"])
            else "Sem sinais hoje ainda"
        )

    result_line = (f"🟢 {_lbl_mais}" if won == "MAIS" else f"🔴 {_lbl_ate}") if won else "⚪ ?"
    sinal_block = f"{sinal_str}\n" if sinal_str else ""
    _cnt_tg = (f"R$ {float(final_cnt):.2f}" if is_price_tg and final_cnt is not None else str(final_cnt))
    msg = (
        f"📊 <b>RESULTADO — {market_name}</b>\n"
        f"🕐 {ts}  |  📍 {loc_str}\n"
        f"\n"
        f"{sinal_block}"
        f"{_telegram_operational_context(mk, sent_signal.get('conf') if sent_signal else None, effective_rec)}\n"
        f"\n"
        f"Meta: <b>{thr_str}</b>  →  Contagem: <b>{_cnt_tg}</b>\n"
        f"Resultado: {result_line}\n"
        f"\n"
        f"{placar_str}\n"
        f"\n"
        f"<i>⚠️ Apenas probabilidade. Aposte por conta e risco.</i>"
    )
    msg += _telegram_bankroll_note(mk)
    send_telegram(msg)
    if market_id:
        telegram_result_notified_mid[mk] = market_id
    return True


def _streak_flip_strategy(
    results: list[str],
    min_streak: int,
    base_conf: int,
    max_conf: int,
) -> tuple[str | None, int, str]:
    if not results:
        return None, 0, "sem historico"

    streak = 1
    for result in results[1:]:
        if result != results[0]:
            break
        streak += 1

    if streak < min_streak:
        return None, 0, f"streak {streak} menor que {min_streak}"

    rec = "ATE" if results[0] == "MAIS" else "MAIS"
    conf = min(max_conf, base_conf + (streak - min_streak) * 4)
    return rec, conf, f"virada apos streak de {streak}x {results[0]}"


def _loc_hour_avg_strategy(
    hist: list[dict],
    hour: int,
    location: str | None,
    value_needed,
    min_samples: int,
    min_gap: int,
    base_conf: int,
    max_conf: int,
) -> tuple[str | None, int, str]:
    if not location or value_needed in (None, ""):
        return None, 0, "local/meta indisponivel"

    try:
        meta = float(value_needed)
    except (TypeError, ValueError):
        return None, 0, "meta invalida"

    counts = []
    for entry in hist:
        if entry.get("location") != location or entry.get("count") is None:
            continue
        entry_dt = _entry_datetime(entry)
        if entry_dt is None or entry_dt.hour != hour:
            continue
        try:
            counts.append(float(entry["count"]))
        except (TypeError, ValueError):
            continue

    if len(counts) < min_samples:
        return None, 0, f"local+hora com {len(counts)} amostras (< {min_samples})"

    avg_count = sum(counts) / len(counts)
    gap = avg_count - meta
    if abs(gap) < min_gap:
        return None, 0, f"media {avg_count:.1f} perto da meta {meta:.0f}"

    rec = "MAIS" if gap > 0 else "ATE"
    conf = min(
        max_conf,
        base_conf + int(abs(gap)) + min(8, len(counts) - min_samples),
    )
    return rec, conf, (
        f"{len(counts)} rodadas no mesmo local+hora | "
        f"media {avg_count:.1f} vs meta {meta:.0f}"
    )


def _as_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _hour_window_label(hour: int | None) -> str:
    try:
        start = int(hour if hour is not None else datetime.now().hour) % 24
    except Exception:
        start = datetime.now().hour
    end = (start + 1) % 24
    return f"{start:02d}:00-{end:02d}:00"


def _history_hour(entry: dict) -> int | None:
    entry_dt = _entry_datetime(entry)
    return entry_dt.hour if entry_dt else None


def _history_weekday(entry: dict) -> int | None:
    entry_dt = _entry_datetime(entry)
    return entry_dt.weekday() if entry_dt else None


def _pattern_streak_flip(prior_results: list[str], min_streak: int) -> str | None:
    if len(prior_results) < min_streak:
        return None

    last = prior_results[-1]
    streak = 1
    for result in reversed(prior_results[:-1]):
        if result != last:
            break
        streak += 1

    if streak < min_streak:
        return None
    return "ATE" if last == "MAIS" else "MAIS"


def _pattern_ate_streak_deep_deviation(
    prior_results: list[str],
    prior_entries: list[tuple],
    min_streak: int = 2,
    deviation_threshold: float = -40.0,
) -> str | None:
    """Retorna MAIS quando as ultimas min_streak rodadas foram ATE
    e o desvio (count - value_needed) da rodada anterior foi < deviation_threshold.
    Acuracia real: 2x ATE=82.8% (n=58), 3x ATE=90.5% (n=21)."""
    if len(prior_results) < min_streak:
        return None
    if any(r != "ATE" for r in prior_results[-min_streak:]):
        return None
    if not prior_entries:
        return None
    _, last_entry, _ = prior_entries[-1]
    count = _as_float(last_entry.get("count"))
    value_needed = _as_float(last_entry.get("value_needed"))
    if count is None or value_needed is None:
        return None
    if (count - value_needed) >= deviation_threshold:
        return None
    return "MAIS"


def _pattern_checker(prior_results: list[str]) -> str | None:
    if len(prior_results) < 4:
        return None

    recent4 = prior_results[-4:]
    if not all(recent4[i] != recent4[i + 1] for i in range(3)):
        return None

    return "ATE" if recent4[-1] == "MAIS" else "MAIS"


def _pattern_count_extremes(
    prior_counts: list[float],
    value_needed,
    n_max: int = 20,
    n_min: int = 10,
) -> str | None:
    """Compara value_needed com extremos das contagens historicas recentes.
    - Se meta > maximo das ultimas n_max contagens -> ATE (80% acerto no historico)
    - Se meta < minimo das ultimas n_min contagens -> MAIS (86% acerto no historico)
    """
    vn = _as_float(value_needed)
    if vn is None:
        return None
    if len(prior_counts) >= n_max:
        recent_max = max(prior_counts[-n_max:])
        if vn > recent_max:
            return "ATE"
    if len(prior_counts) >= n_min:
        recent_min = min(prior_counts[-n_min:])
        if vn < recent_min:
            return "MAIS"
    return None


def _pattern_loc_hour(
    stats_by_loc_hour: dict,
    location: str | None,
    hour: int | None,
    value_needed,
    min_samples: int = 8,
    min_gap: int = 4,
) -> str | None:
    if not location or hour is None:
        return None

    meta = _as_float(value_needed)
    if meta is None:
        return None

    stat = stats_by_loc_hour.get((location, hour))
    if not stat or stat["total"] < min_samples:
        return None

    avg_count = stat["sum"] / stat["total"]
    if abs(avg_count - meta) < min_gap:
        return None

    return "MAIS" if avg_count > meta else "ATE"


def _pattern_weekday_loc_hour(
    stats_by_weekday_loc_hour: dict,
    weekday: int | None,
    location: str | None,
    hour: int | None,
    value_needed,
    min_samples: int = 6,
    min_gap: int = 3,
) -> str | None:
    if weekday is None or not location or hour is None:
        return None

    meta = _as_float(value_needed)
    if meta is None:
        return None

    stat = stats_by_weekday_loc_hour.get((weekday, location, hour))
    if not stat or stat["total"] < min_samples:
        return None

    avg_count = stat["sum"] / stat["total"]
    if abs(avg_count - meta) < min_gap:
        return None

    return "MAIS" if avg_count > meta else "ATE"


def _pattern_weekday_loc_hour_result(
    stats_by_weekday_loc_hour_results: dict,
    weekday: int | None,
    location: str | None,
    hour: int | None,
    min_samples: int = 6,
    min_edge: int = 1,
) -> str | None:
    if weekday is None or not location or hour is None:
        return None

    stat = stats_by_weekday_loc_hour_results.get((weekday, location, hour))
    if not stat or stat["total"] < min_samples:
        return None

    if abs(stat["mais"] - stat["ate"]) < min_edge:
        return None

    return "MAIS" if stat["mais"] >= stat["ate"] else "ATE"


def _pattern_weekday_hour_majority(
    stats_by_weekday_hour: dict,
    weekday: int | None,
    hour: int | None,
    min_samples: int = 8,
    min_edge: int = 2,
) -> str | None:
    if weekday is None or hour is None:
        return None

    stat = stats_by_weekday_hour.get((weekday, hour))
    if not stat or stat["total"] < min_samples:
        return None

    if abs(stat["mais"] - stat["ate"]) < min_edge:
        return None

    return "MAIS" if stat["mais"] >= stat["ate"] else "ATE"


def _pattern_hour_majority(
    stats_by_hour: dict,
    hour: int | None,
    min_samples: int = 10,
    min_edge: int = 3,
) -> str | None:
    if hour is None:
        return None

    stat = stats_by_hour.get(hour)
    if not stat or stat["total"] < min_samples:
        return None

    if abs(stat["mais"] - stat["ate"]) < min_edge:
        return None

    return "MAIS" if stat["mais"] >= stat["ate"] else "ATE"


def _pattern_recent_gap(
    prior_entries: list[tuple[datetime, dict, str]],
    now_dt: datetime,
    location: str | None,
    window_minutes: int = 60,
    min_samples: int = 6,
    min_abs_gap: int = 3,
    same_location_only: bool = False,
) -> str | None:
    cutoff = now_dt - timedelta(minutes=window_minutes)
    gaps: list[float] = []

    for entry_dt, entry, _actual in reversed(prior_entries):
        if entry_dt < cutoff:
            break
        if same_location_only and location and entry.get("location") != location:
            continue
        gap = _as_float(entry.get("count"))
        meta = _as_float(entry.get("value_needed"))
        if gap is None or meta is None:
            continue
        gaps.append(gap - meta)

    if len(gaps) < min_samples:
        return None

    avg_gap = sum(gaps) / len(gaps)
    if abs(avg_gap) < min_abs_gap:
        return None

    return "MAIS" if avg_gap > 0 else "ATE"


def _pattern_accuracy(pattern_stats: dict, key: str) -> tuple[int, int]:
    stat = pattern_stats.get(key) or {}
    total = int(stat.get("wins") or 0) + int(stat.get("losses") or 0)
    if total <= 0:
        return 0, total
    acc = int(round((int(stat.get("wins") or 0) / total) * 100))
    return acc, total


def _build_pattern_confluence(
    mk: str,
    pattern_stats: dict,
    weekday: int | None,
) -> tuple[str | None, int, str]:
    streak3_sig = pattern_stats["streak3"]["signal"]
    streak2_sig = pattern_stats["streak2"]["signal"]
    loc_sig = pattern_stats["loc_hour"]["signal"]
    weekday_loc_sig = pattern_stats["weekday_loc_hour"]["signal"]
    weekday_loc_res_sig = pattern_stats["weekday_loc_hour_result"]["signal"]
    weekday_hour_sig = pattern_stats["weekday_hour"]["signal"]
    recent30_loc_gap_sig = pattern_stats["recent30_loc_gap"]["signal"]
    recent60_loc_gap_sig = pattern_stats["recent60_loc_gap"]["signal"]

    streak3_acc, streak3_total = _pattern_accuracy(pattern_stats, "streak3")
    streak2_acc, streak2_total = _pattern_accuracy(pattern_stats, "streak2")
    loc_acc, loc_total = _pattern_accuracy(pattern_stats, "loc_hour")
    weekday_loc_acc, weekday_loc_total = _pattern_accuracy(pattern_stats, "weekday_loc_hour")
    weekday_loc_res_acc, weekday_loc_res_total = _pattern_accuracy(pattern_stats, "weekday_loc_hour_result")
    weekday_hour_acc, weekday_hour_total = _pattern_accuracy(pattern_stats, "weekday_hour")
    recent30_loc_gap_acc, recent30_loc_gap_total = _pattern_accuracy(pattern_stats, "recent30_loc_gap")
    recent60_loc_gap_acc, recent60_loc_gap_total = _pattern_accuracy(pattern_stats, "recent60_loc_gap")

    sunday_bonus = 2 if weekday == 6 else 0
    weekday_text = "domingo" if weekday == 6 else "dia+hora"

    if mk == "rua":
        if (
            streak2_sig
            and loc_sig
            and recent60_loc_gap_sig
            and streak2_sig == loc_sig == recent60_loc_gap_sig
            and streak2_total >= MIN_PATTERN_BACKTEST_SAMPLES
            and loc_total >= MIN_PATTERN_BACKTEST_SAMPLES
            and recent60_loc_gap_total >= 40
            and recent60_loc_gap_acc >= 58
        ):
            conf_base = int(round(streak2_acc * 0.42 + loc_acc * 0.33 + recent60_loc_gap_acc * 0.25))
            used_parts = [f"{streak2_acc}%", f"{loc_acc}%", f"{recent60_loc_gap_acc}% ritmo60m"]
            detail_parts = ["virada 2", "local/hora", "ritmo local 60m"]

            if (
                weekday_loc_sig == streak2_sig
                and weekday_loc_total >= 20
                and weekday_loc_acc >= 60
            ):
                conf_base = int(round(conf_base * 0.82 + weekday_loc_acc * 0.18))
                used_parts.append(f"{weekday_loc_acc}%")
                detail_parts.append(weekday_text)

            if (
                recent30_loc_gap_sig == streak2_sig
                and recent30_loc_gap_total >= 24
                and recent30_loc_gap_acc >= 55
            ):
                conf_base = int(round(conf_base * 0.86 + recent30_loc_gap_acc * 0.14))
                used_parts.append(f"{recent30_loc_gap_acc}% ritmo30m")
                detail_parts.append("ritmo local 30m")

            conf = min(94, max(82, conf_base + 3 + sunday_bonus))
            return streak2_sig, conf, (
                f"Rua: confluencia elite "
                f"({' + '.join(detail_parts)} | {'/'.join(used_parts)})"
            )

        if (
            streak3_sig
            and loc_sig
            and streak3_sig == loc_sig
            and streak3_total >= MIN_PATTERN_BACKTEST_SAMPLES
            and loc_total >= MIN_PATTERN_BACKTEST_SAMPLES
        ):
            conf_base = int(round(streak3_acc * 0.58 + loc_acc * 0.42))
            used_parts = [f"{streak3_acc}%", f"{loc_acc}%"]
            detail_parts = ["virada 3", "local/hora"]
            if (
                weekday_loc_sig == streak3_sig
                and weekday_loc_total >= 20
                and weekday_loc_acc >= 60
            ):
                conf_base = int(round(conf_base * 0.82 + weekday_loc_acc * 0.18))
                used_parts.append(f"{weekday_loc_acc}%")
                detail_parts.append(weekday_text)
            conf = min(92, max(80, conf_base + 4 + sunday_bonus))
            return streak3_sig, conf, (
                f"Rua: confluencia elite "
                f"({' + '.join(detail_parts)} | {'/'.join(used_parts)})"
            )

        if (
            loc_sig
            and weekday_loc_sig
            and loc_sig == weekday_loc_sig
            and loc_total >= MIN_PATTERN_BACKTEST_SAMPLES
            and weekday_loc_total >= 12
        ):
            conf = min(86, max(74, int(round(loc_acc * 0.62 + weekday_loc_acc * 0.38)) + sunday_bonus))
            return loc_sig, conf, (
                f"Rua: contexto local/hora + {weekday_text} "
                f"({loc_acc}%/{weekday_loc_acc}%)"
            )

    if mk == "rodovia":
        # Nível 1: streak2 + 3 contextos alinhados (requisitos ajustados para dados reais)
        if (
            streak2_sig
            and weekday_loc_sig
            and weekday_hour_sig
            and weekday_loc_res_sig
            and streak2_sig == weekday_loc_sig == weekday_hour_sig == weekday_loc_res_sig
            and streak2_total >= MIN_PATTERN_BACKTEST_SAMPLES
            and weekday_loc_total >= 15
            and weekday_hour_total >= 25
            and weekday_loc_res_total >= 15
        ):
            conf = min(
                88,
                max(
                    74,
                    int(round(
                        streak2_acc * 0.30
                        + weekday_loc_acc * 0.24
                        + weekday_hour_acc * 0.22
                        + weekday_loc_res_acc * 0.24
                    )) + sunday_bonus,
                ),
            )
            return streak2_sig, conf, (
                f"Rodovia: streak2 + dia/local/hora + dia/hora + dominante "
                f"({streak2_acc}%/{weekday_loc_acc}%/{weekday_hour_acc}%/{weekday_loc_res_acc}%)"
            )

        # Nível 2: streak2 + local/hora + 2 contextos dia
        if (
            streak2_sig
            and weekday_loc_sig
            and weekday_hour_sig
            and loc_sig
            and streak2_sig == loc_sig == weekday_loc_sig == weekday_hour_sig
            and streak2_total >= MIN_PATTERN_BACKTEST_SAMPLES
            and loc_total >= MIN_PATTERN_BACKTEST_SAMPLES
            and weekday_loc_total >= 15
            and weekday_hour_total >= 25
        ):
            conf = min(
                86,
                max(
                    72,
                    int(round(
                        streak2_acc * 0.34
                        + loc_acc * 0.22
                        + weekday_loc_acc * 0.24
                        + weekday_hour_acc * 0.20
                    )) + sunday_bonus,
                ),
            )
            return streak2_sig, conf, (
                f"Rodovia: streak2 + local/hora + dia/local/hora + dia/hora "
                f"({streak2_acc}%/{loc_acc}%/{weekday_loc_acc}%/{weekday_hour_acc}%)"
            )

        # Nível 3: streak2 + 2 contextos dia
        if (
            streak2_sig
            and weekday_loc_sig
            and weekday_hour_sig
            and streak2_sig == weekday_loc_sig == weekday_hour_sig
            and streak2_total >= MIN_PATTERN_BACKTEST_SAMPLES
            and weekday_loc_total >= 15
            and weekday_hour_total >= 25
        ):
            conf = min(
                84,
                max(
                    71,
                    int(round(
                        streak2_acc * 0.40
                        + weekday_loc_acc * 0.32
                        + weekday_hour_acc * 0.28
                    )) + sunday_bonus,
                ),
            )
            return streak2_sig, conf, (
                f"Rodovia: streak2 + dia/local/hora + dia/hora "
                f"({streak2_acc}%/{weekday_loc_acc}%/{weekday_hour_acc}%)"
            )

        # Nível 4: streak2 + dominante + ritmo 60m
        if (
            streak2_sig
            and weekday_loc_res_sig
            and recent60_loc_gap_sig
            and streak2_sig == weekday_loc_res_sig == recent60_loc_gap_sig
            and streak2_total >= MIN_PATTERN_BACKTEST_SAMPLES
            and weekday_loc_res_total >= 15
            and recent60_loc_gap_total >= 25
        ):
            conf = min(
                83,
                max(
                    69,
                    int(round(
                        streak2_acc * 0.40
                        + weekday_loc_res_acc * 0.34
                        + recent60_loc_gap_acc * 0.26
                    )) + sunday_bonus,
                ),
            )
            return streak2_sig, conf, (
                f"Rodovia: streak2 + dia/local/hora + ritmo local 60m "
                f"({streak2_acc}%/{weekday_loc_res_acc}%/{recent60_loc_gap_acc}%)"
            )

        # Nível 5: local/hora + dia da semana (fallback)
        if (
            loc_sig
            and weekday_loc_sig
            and loc_sig == weekday_loc_sig
            and loc_total >= MIN_PATTERN_BACKTEST_SAMPLES
            and weekday_loc_total >= 12
            and weekday_loc_acc >= 57
        ):
            conf = min(84, max(68, int(round(loc_acc * 0.58 + weekday_loc_acc * 0.42)) + 2 + sunday_bonus))
            return loc_sig, conf, (
                f"Rodovia: contexto local/hora + {weekday_text} "
                f"({loc_acc}%/{weekday_loc_acc}%)"
            )

    return None, 0, "sem confluencia forte"


def _record_live_count_sample(
    mk: str,
    count,
    remaining,
    sample_ts: float | None = None,
):
    cnt = _as_float(count)
    rem = _as_float(remaining)
    if cnt is None or rem is None:
        return
    sample = {
        "ts": float(sample_ts or time.time()),
        "count": float(cnt),
        "remaining": float(rem),
    }
    hist = count_history[mk]
    if hist:
        last = hist[-1]
        if int(last.get("count") or -1) == int(cnt) and int(last.get("remaining") or -1) == int(rem):
            return
    hist.append(sample)


def _live_count_trend(
    mk: str,
    current_count: int,
    remaining: int,
    elapsed: int,
    overall_rate_sec: float,
) -> tuple[float | None, int, str]:
    hist = count_history[mk]
    if len(hist) < 2 or elapsed <= 0:
        return None, 0, "sem historico recente do contador"

    total = int(MARKET_CONFIGS[mk]["duration"])
    current_elapsed = total - int(remaining)
    if current_elapsed <= 0:
        return None, 0, "sem janela recente do contador"

    base_sample = None
    for sample in reversed(hist):
        sample_elapsed = total - int(sample.get("remaining") or 0)
        delta_elapsed = current_elapsed - sample_elapsed
        if delta_elapsed < LIVE_TREND_LOOKBACK_MIN_SECONDS:
            continue
        if delta_elapsed > LIVE_TREND_LOOKBACK_MAX_SECONDS:
            break
        if int(sample.get("count") or current_count) >= int(current_count):
            continue
        base_sample = sample
        break

    if base_sample is None:
        return None, 0, "sem base recente para aceleracao"

    base_count = float(base_sample.get("count") or 0.0)
    base_remaining = int(base_sample.get("remaining") or remaining)
    base_elapsed = total - base_remaining
    delta_elapsed = max(current_elapsed - base_elapsed, 1)
    delta_count = max(float(current_count) - base_count, 0.0)
    recent_rate_sec = delta_count / delta_elapsed
    if overall_rate_sec <= 0:
        return recent_rate_sec, 0, (
            f"ritmo recente {int(delta_count)}/{delta_elapsed}s = {(recent_rate_sec * 60):.1f}/min"
        )

    trend_ratio = recent_rate_sec / overall_rate_sec
    trend_conf = min(12, max(0, int(abs(trend_ratio - 1.0) * 20)))
    trend_text = (
        f"ritmo recente {int(delta_count)}/{delta_elapsed}s = {(recent_rate_sec * 60):.1f}/min "
        f"vs medio {(overall_rate_sec * 60):.1f}/min"
    )
    return trend_ratio, trend_conf, trend_text


def _compute_best_pattern_recommendation(
    mk: str,
    parsed_hist: list[tuple[dict, str]],
    location: str | None,
    value_needed,
    cur_hour: int,
) -> tuple[str | None, int, str]:
    if len(parsed_hist) < 8:
        return None, 0, "pouco historico para ranking de padroes"

    pattern_stats = {
        "streak3": {
            "name": "Virada apos 3 iguais",
            "wins": 0,
            "losses": 0,
            "signal": None,
        },
        "streak2": {
            "name": "Virada apos 2 iguais",
            "wins": 0,
            "losses": 0,
            "signal": None,
        },
        "loc_hour": {
            "name": "Local + hora vs meta",
            "wins": 0,
            "losses": 0,
            "signal": None,
        },
        "weekday_loc_hour": {
            "name": "Dia + local + hora vs meta",
            "wins": 0,
            "losses": 0,
            "signal": None,
        },
        "weekday_loc_hour_result": {
            "name": "Dia + local + hora dominante",
            "wins": 0,
            "losses": 0,
            "signal": None,
        },
        "weekday_hour": {
            "name": "Dia + hora dominante",
            "wins": 0,
            "losses": 0,
            "signal": None,
        },
        "recent60_loc_gap": {
            "name": "Ritmo local 60m",
            "wins": 0,
            "losses": 0,
            "signal": None,
        },
        "recent30_loc_gap": {
            "name": "Ritmo local 30m",
            "wins": 0,
            "losses": 0,
            "signal": None,
        },
        "checker": {
            "name": "Padrao xadrez",
            "wins": 0,
            "losses": 0,
            "signal": None,
        },
        "hour_majority": {
            "name": "Hora dominante",
            "wins": 0,
            "losses": 0,
            "signal": None,
        },
        "ate_streak2_deep_dev": {
            "name": "2x ATE + desvio profundo",
            "wins": 0,
            "losses": 0,
            "signal": None,
        },
        "ate_streak3_deep_dev": {
            "name": "3x ATE + desvio profundo",
            "wins": 0,
            "losses": 0,
            "signal": None,
        },
        "count_extremes": {
            "name": "Meta vs extremo historico (80-86%)",
            "wins": 0,
            "losses": 0,
            "signal": None,
        },
    }

    prior_results: list[str] = []
    prior_entries: list[tuple[datetime, dict, str]] = []
    prior_counts: list[float] = []
    stats_by_loc_hour: dict = {}
    stats_by_weekday_loc_hour: dict = {}
    stats_by_weekday_loc_hour_results: dict = {}
    stats_by_weekday_hour: dict = {}
    stats_by_hour: dict = {}
    loc_hour_min_samples = int(MARKET_CONFIGS[mk].get("pattern_loc_hour_min_samples", 8))
    loc_hour_min_gap     = int(MARKET_CONFIGS[mk].get("pattern_loc_hour_min_gap", 4))
    weekday_loc_min_samples = int(MARKET_CONFIGS[mk].get("pattern_weekday_loc_hour_min_samples", 6))
    weekday_loc_min_gap     = int(MARKET_CONFIGS[mk].get("pattern_weekday_loc_hour_min_gap", 3))

    def score_pattern(key: str, signal: str | None, actual: str):
        if not signal:
            return
        if signal == actual:
            pattern_stats[key]["wins"] += 1
        else:
            pattern_stats[key]["losses"] += 1

    for entry, actual in reversed(parsed_hist):
        hour = _history_hour(entry)
        weekday = _history_weekday(entry)

        score_pattern("streak3", _pattern_streak_flip(prior_results, 3), actual)
        score_pattern("streak2", _pattern_streak_flip(prior_results, 2), actual)
        score_pattern(
            "loc_hour",
            _pattern_loc_hour(
                stats_by_loc_hour,
                entry.get("location"),
                hour,
                entry.get("value_needed"),
                min_samples=loc_hour_min_samples,
                min_gap=loc_hour_min_gap,
            ),
            actual,
        )
        score_pattern(
            "weekday_loc_hour",
            _pattern_weekday_loc_hour(
                stats_by_weekday_loc_hour,
                weekday,
                entry.get("location"),
                hour,
                entry.get("value_needed"),
                min_samples=weekday_loc_min_samples,
                min_gap=weekday_loc_min_gap,
            ),
            actual,
        )
        score_pattern(
            "weekday_loc_hour_result",
            _pattern_weekday_loc_hour_result(
                stats_by_weekday_loc_hour_results,
                weekday,
                entry.get("location"),
                hour,
                min_samples=max(6, weekday_loc_min_samples),
                min_edge=1,
            ),
            actual,
        )
        score_pattern(
            "weekday_hour",
            _pattern_weekday_hour_majority(
                stats_by_weekday_hour,
                weekday,
                hour,
                min_samples=8,
                min_edge=2,
            ),
            actual,
        )
        score_pattern(
            "recent60_loc_gap",
            _pattern_recent_gap(
                prior_entries,
                _entry_datetime(entry) or datetime.now(),
                entry.get("location"),
                window_minutes=60,
                min_samples=6,
                min_abs_gap=3,
                same_location_only=True,
            ),
            actual,
        )
        score_pattern(
            "recent30_loc_gap",
            _pattern_recent_gap(
                prior_entries,
                _entry_datetime(entry) or datetime.now(),
                entry.get("location"),
                window_minutes=30,
                min_samples=3,
                min_abs_gap=2,
                same_location_only=True,
            ),
            actual,
        )
        score_pattern("checker", _pattern_checker(prior_results), actual)
        score_pattern(
            "hour_majority",
            _pattern_hour_majority(stats_by_hour, hour),
            actual,
        )
        score_pattern(
            "ate_streak2_deep_dev",
            _pattern_ate_streak_deep_deviation(prior_results, prior_entries, 2, -40.0),
            actual,
        )
        score_pattern(
            "ate_streak3_deep_dev",
            _pattern_ate_streak_deep_deviation(prior_results, prior_entries, 3, -40.0),
            actual,
        )
        score_pattern(
            "count_extremes",
            _pattern_count_extremes(prior_counts, entry.get("value_needed")),
            actual,
        )

        prior_results.append(actual)
        entry_dt = _entry_datetime(entry)
        if entry_dt is not None:
            prior_entries.append((entry_dt, entry, actual))

        count = _as_float(entry.get("count"))
        if count is not None:
            prior_counts.append(count)

        loc = entry.get("location")
        if loc and hour is not None and count is not None:
            loc_key = (loc, hour)
            if loc_key not in stats_by_loc_hour:
                stats_by_loc_hour[loc_key] = {"total": 0, "sum": 0.0}
            stats_by_loc_hour[loc_key]["total"] += 1
            stats_by_loc_hour[loc_key]["sum"] += count

        if loc and weekday is not None and hour is not None and count is not None:
            weekday_loc_key = (weekday, loc, hour)
            if weekday_loc_key not in stats_by_weekday_loc_hour:
                stats_by_weekday_loc_hour[weekday_loc_key] = {"total": 0, "sum": 0.0}
            stats_by_weekday_loc_hour[weekday_loc_key]["total"] += 1
            stats_by_weekday_loc_hour[weekday_loc_key]["sum"] += count

        if loc and weekday is not None and hour is not None:
            weekday_loc_res_key = (weekday, loc, hour)
            if weekday_loc_res_key not in stats_by_weekday_loc_hour_results:
                stats_by_weekday_loc_hour_results[weekday_loc_res_key] = {"total": 0, "mais": 0, "ate": 0}
            stats_by_weekday_loc_hour_results[weekday_loc_res_key]["total"] += 1
            if actual == "MAIS":
                stats_by_weekday_loc_hour_results[weekday_loc_res_key]["mais"] += 1
            else:
                stats_by_weekday_loc_hour_results[weekday_loc_res_key]["ate"] += 1

        if hour is not None:
            if hour not in stats_by_hour:
                stats_by_hour[hour] = {"total": 0, "mais": 0, "ate": 0}
            stats_by_hour[hour]["total"] += 1
            if actual == "MAIS":
                stats_by_hour[hour]["mais"] += 1
            else:
                stats_by_hour[hour]["ate"] += 1

        if weekday is not None and hour is not None:
            weekday_hour_key = (weekday, hour)
            if weekday_hour_key not in stats_by_weekday_hour:
                stats_by_weekday_hour[weekday_hour_key] = {"total": 0, "mais": 0, "ate": 0}
            stats_by_weekday_hour[weekday_hour_key]["total"] += 1
            if actual == "MAIS":
                stats_by_weekday_hour[weekday_hour_key]["mais"] += 1
            else:
                stats_by_weekday_hour[weekday_hour_key]["ate"] += 1

    pattern_stats["streak3"]["signal"] = _pattern_streak_flip(prior_results, 3)
    pattern_stats["streak2"]["signal"] = _pattern_streak_flip(prior_results, 2)
    pattern_stats["ate_streak2_deep_dev"]["signal"] = _pattern_ate_streak_deep_deviation(
        prior_results, prior_entries, 2, -40.0
    )
    pattern_stats["ate_streak3_deep_dev"]["signal"] = _pattern_ate_streak_deep_deviation(
        prior_results, prior_entries, 3, -40.0
    )
    pattern_stats["count_extremes"]["signal"] = _pattern_count_extremes(
        prior_counts, value_needed
    )
    now_weekday = datetime.now().weekday()
    pattern_stats["loc_hour"]["signal"] = _pattern_loc_hour(
        stats_by_loc_hour,
        location,
        cur_hour,
        value_needed,
        min_samples=loc_hour_min_samples,
        min_gap=loc_hour_min_gap,
    )
    pattern_stats["weekday_loc_hour"]["signal"] = _pattern_weekday_loc_hour(
        stats_by_weekday_loc_hour,
        now_weekday,
        location,
        cur_hour,
        value_needed,
        min_samples=weekday_loc_min_samples,
        min_gap=weekday_loc_min_gap,
    )
    pattern_stats["weekday_loc_hour_result"]["signal"] = _pattern_weekday_loc_hour_result(
        stats_by_weekday_loc_hour_results,
        now_weekday,
        location,
        cur_hour,
        min_samples=max(6, weekday_loc_min_samples),
        min_edge=1,
    )
    pattern_stats["weekday_hour"]["signal"] = _pattern_weekday_hour_majority(
        stats_by_weekday_hour,
        now_weekday,
        cur_hour,
        min_samples=8,
        min_edge=2,
    )
    pattern_stats["recent60_loc_gap"]["signal"] = _pattern_recent_gap(
        prior_entries,
        datetime.now(),
        location,
        window_minutes=60,
        min_samples=6,
        min_abs_gap=3,
        same_location_only=True,
    )
    pattern_stats["recent30_loc_gap"]["signal"] = _pattern_recent_gap(
        prior_entries,
        datetime.now(),
        location,
        window_minutes=30,
        min_samples=3,
        min_abs_gap=2,
        same_location_only=True,
    )
    pattern_stats["checker"]["signal"] = _pattern_checker(prior_results)
    pattern_stats["hour_majority"]["signal"] = _pattern_hour_majority(
        stats_by_hour,
        cur_hour,
    )

    confluence_rec, confluence_conf, confluence_reason = _build_pattern_confluence(
        mk,
        pattern_stats,
        now_weekday,
    )
    if confluence_rec:
        return confluence_rec, confluence_conf, confluence_reason

    # Fallback: melhor padrão isolado ativo (igual aos outros mercados)
    ranked = []
    for stat in pattern_stats.values():
        total = stat["wins"] + stat["losses"]
        if total <= 0:
            continue
        acc = int(round((stat["wins"] / total) * 100))
        ranked.append({
            "name": stat["name"],
            "wins": stat["wins"],
            "losses": stat["losses"],
            "total": total,
            "acc": acc,
            "signal": stat["signal"],
        })

    ranked.sort(key=lambda item: (item["acc"], item["total"]), reverse=True)
    if not ranked:
        return None, 0, f"{mk.capitalize()}: sem ranking de padroes ainda"

    for item in ranked:
        if (
            item["signal"]
            and item["total"] >= MIN_PATTERN_BACKTEST_SAMPLES
            and item["acc"] >= MIN_PATTERN_BACKTEST_ACC
        ):
            conf = max(MIN_PATTERN_BACKTEST_ACC, min(90, item["acc"]))
            return item["signal"], conf, (
                f"{mk.capitalize()}: melhor padrao ativo = {item['name']} "
                f"({item['acc']}%, {item['wins']}V/{item['losses']}D, "
                f"{item['total']} amostras)"
            )

    best = ranked[0]
    return None, 0, (
        f"{mk.capitalize()}: nenhum padrao ativo acima de "
        f"{MIN_PATTERN_BACKTEST_ACC}% | melhor geral: {best['name']} "
        f"{best['acc']}% ({best['total']} amostras)"
    )


def _compute_recommendation(
    mk: str,
    location: str | None = None,
    value_needed=None,
):
    """
    Analisa histórico e retorna (rec, conf_pct, reason) ou (None, 0, reason).
    Detecta: xadrez (alternância), streak longo, frequência por hora, tendência recente.
    Só retorna recomendação se confiança >= min_conf (padrão 57%).
    """
    cutoff = _history_cutoff()
    parsed_hist = [
        (entry, result)
        for entry in histories[mk]
        if (result := _entry_result(entry))
        and (_entry_datetime(entry) or datetime.min) >= cutoff
    ]
    hist = [entry for entry, _ in parsed_hist]
    if len(hist) < 4:
        return None, 0, "histórico insuficiente (< 4 rodadas)"

    now_dt = datetime.now()
    results = [result for _, result in parsed_hist]
    if len(results) < 4:
        return None, 0, "historico insuficiente (< 4 rodadas)"
    cur_hour = now_dt.hour
    recent10 = results[:10]

    location = _current_context_value(mk, "location", location)
    value_needed = _current_context_value(mk, "value_needed", value_needed)

    if mk in ("rodovia", "rua"):
        rec, conf, reason = _compute_best_pattern_recommendation(
            mk,
            parsed_hist,
            location,
            value_needed,
            cur_hour,
        )
        if rec:
            return rec, conf, reason
        return None, 0, reason

    slot_pool = _weekly_slot_pool(hist, now_dt)
    if len(slot_pool) >= MIN_WEEKLY_SLOT_SAMPLES:
        slot_rec, slot_conf, slot_mais = _majority_probability(slot_pool)

        if len(recent10) >= 5:
            recent_rec, recent_conf, recent_mais = _majority_probability(recent10)
            slot_prob_mais = (slot_conf / 100) if slot_rec == "MAIS" else (1 - slot_conf / 100)
            recent_prob_mais = (
                (recent_conf / 100) if recent_rec == "MAIS" else (1 - recent_conf / 100)
            )
            combined_prob_mais = slot_prob_mais * 0.7 + recent_prob_mais * 0.3
            rec = "MAIS" if combined_prob_mais >= 0.5 else "ATE"
            conf = int(round(max(combined_prob_mais, 1 - combined_prob_mais) * 100))
            if conf < 57:
                return None, conf, (
                    f"janela semanal 1h sem vantagem clara ({conf}%; "
                    f"{len(slot_pool)} amostras, {slot_mais}% MAIS)"
                )
            return rec, conf, (
                f"mesmo horario semanal 1h: {len(slot_pool)} amostras ({slot_mais}% MAIS) "
                f"+ ultimas {len(recent10)} ({recent_mais}% MAIS)"
            )

        if slot_conf < 57:
            return None, slot_conf, (
                f"janela semanal 1h sem vantagem clara ({slot_conf}%; "
                f"{len(slot_pool)} amostras, {slot_mais}% MAIS)"
            )
        return slot_rec, slot_conf, (
            f"mesmo horario semanal 1h: {len(slot_pool)} amostras ({slot_mais}% MAIS)"
        )

    # ── 1. Detecção de padrão xadrez (alternância) ──
    # Verifica últimos 6: se todos alternam, é xadrez
    recent6 = results[:6]
    alternating = len(recent6) >= 4 and all(
        recent6[i] != recent6[i + 1] for i in range(len(recent6) - 1)
    )
    if alternating:
        rec = "ATE" if recent6[0] == "MAIS" else "MAIS"
        return rec, 72, f"padrão xadrez detectado (últimos {len(recent6)} alternam)"

    # ── 2. Streak longo (4+ iguais consecutivos) ──
    streak = 1
    for i in range(1, min(8, len(results))):
        if results[i] == results[0]:
            streak += 1
        else:
            break
    if streak >= 4:
        # Após streak longo, probabilidade de virar aumenta
        rec = "ATE" if results[0] == "MAIS" else "MAIS"
        return rec, 65, f"streak de {streak}× {results[0]} consecutivos — provável virada"

    # ── 3. Frequência por hora (peso 60%) + últimas 10 gerais (peso 40%) ──
    hour_pool = [r for e, r in zip(hist, results)
                 if e.get("time") and int(e["time"][:2]) == cur_hour]
    if len(hour_pool) >= 3 and len(recent10) >= 5:
        mais_h  = sum(1 for r in hour_pool  if r == "MAIS") / len(hour_pool)
        mais_r  = sum(1 for r in recent10   if r == "MAIS") / len(recent10)
        combined = mais_h * 0.6 + mais_r * 0.4
        rec      = "MAIS" if combined >= 0.5 else "ATE"
        conf     = int(round(max(combined, 1 - combined) * 100))
        if conf < 57:
            return None, conf, f"sem padrão claro ({conf}% — abaixo do mínimo)"
        reason = (
            f"{len(hour_pool)} rodadas das {cur_hour:02d}h "
            f"({int(mais_h*100)}% MAIS) + últimas {len(recent10)} "
            f"({int(mais_r*100)}% MAIS)"
        )
        return rec, conf, reason

    if len(hour_pool) >= 4:
        mais_h = sum(1 for r in hour_pool if r == "MAIS") / len(hour_pool)
        rec    = "MAIS" if mais_h >= 0.5 else "ATE"
        conf   = int(round(max(mais_h, 1 - mais_h) * 100))
        if conf < 57:
            return None, conf, f"sem padrão claro no horário ({conf}%)"
        return rec, conf, f"{len(hour_pool)} rodadas das {cur_hour:02d}h ({int(mais_h*100)}% MAIS)"

    if len(recent10) >= 6:
        mais_r = sum(1 for r in recent10 if r == "MAIS") / len(recent10)
        rec    = "MAIS" if mais_r >= 0.5 else "ATE"
        conf   = int(round(max(mais_r, 1 - mais_r) * 100))
        if conf < 57:
            return None, conf, f"sem padrão claro nas últimas rodadas ({conf}%)"
        return rec, conf, f"últimas {len(recent10)} rodadas ({int(mais_r*100)}% MAIS)"

    return None, 0, "histórico insuficiente para análise"


def _compute_live_peak_context(
    mk: str,
    value_needed,
) -> tuple[str | None, int, str]:
    st = states[mk]
    hour = datetime.now().hour
    hist = list(histories[mk])
    return _loc_hour_avg_strategy(
        hist,
        hour,
        st.get("location"),
        value_needed,
        int(MARKET_CONFIGS[mk].get("live_peak_min_samples", 8)),
        int(MARKET_CONFIGS[mk].get("live_peak_min_gap", 4)),
        int(MARKET_CONFIGS[mk].get("live_peak_base_conf", 60)),
        int(MARKET_CONFIGS[mk].get("live_peak_max_conf", 86)),
    )


def _live_window_requirements(
    mk: str,
    value_needed,
) -> tuple[int, int, tuple[str | None, int, str]]:
    total = MARKET_CONFIGS[mk]["duration"]
    min_elapsed = MARKET_CONFIGS[mk].get("live_min_elapsed_seconds")
    if min_elapsed is None:
        min_elapsed = max(
            LIVE_MIN_ELAPSED_SECONDS,
            int(total * LIVE_MIN_ELAPSED_RATIO),
        )
    else:
        min_elapsed = int(min_elapsed)

    min_remaining = int(
        MARKET_CONFIGS[mk].get(
            "betting_min_remaining_seconds",
            LIVE_MIN_REMAINING_SECONDS,
        )
    )
    peak_rec, peak_conf, peak_reason = _compute_live_peak_context(mk, value_needed)
    late_safe_remaining = MARKET_CONFIGS[mk].get("late_safe_min_remaining_seconds")
    if (
        late_safe_remaining is not None
        and peak_rec
        and peak_conf >= 60
    ):
        min_remaining = min(min_remaining, int(late_safe_remaining))

    return min_elapsed, min_remaining, (peak_rec, peak_conf, peak_reason)


# ─────────────────────────────────────────────────────────────────
#  Bias adaptativo: helpers (dados lidos de _live_bias_config)
# ─────────────────────────────────────────────────────────────────

def _calc_bias_delta(direction: str, pct_mais: float, label: str, pace_conf: int):
    """
    Calcula ajuste de confiança com base na taxa histórica.
    Retorna (new_conf, blocked, reason).
    """
    deviation = pct_mais - 50.0
    if abs(deviation) < BIAS_MIN_DEVIATION:
        return pace_conf, False, None
    strength = min(20, max(1, int(abs(deviation) * 0.5)))
    is_mais_bias = deviation > 0
    if (is_mais_bias and direction == "MAIS") or (not is_mais_bias and direction == "ATE"):
        # A favor do bias — boost leve
        return min(97, pace_conf + max(1, int(strength * 0.6))), False, None
    else:
        # Contra o bias — penalidade
        new_conf = pace_conf - strength
        blocked  = new_conf < LIVE_SIGNAL_MIN_CONF
        reason   = (f"{label}: {pct_mais:.0f}% MAIS — desfavorece {direction}" if blocked else None)
        return max(0, new_conf), blocked, reason


def _apply_adaptive_bias(mk: str, pace_rec: str, pace_conf: int, cur_hour: int, meta_int: int):
    """
    Aplica bias de hora, meta e combos lido do bias_config recalculado a cada hora.
    Retorna (pace_rec_ou_None, pace_conf, reason_ou_None).
    """
    with _bias_config_lock:
        cfg = dict(_live_bias_config.get(mk, {}))
    if not cfg:
        return pace_rec, pace_conf, None

    bucket_size = cfg.get("meta_bucket_size", 10)
    meta_bucket = meta_int // bucket_size * bucket_size

    # 1. Hour bias
    hdata = cfg.get("hours", {}).get(str(cur_hour))
    if hdata and hdata.get("samples", 0) >= BIAS_MIN_SAMPLES:
        pace_conf, blocked, reason = _calc_bias_delta(
            pace_rec, hdata["pct_mais"],
            f"{mk}: {cur_hour}h ({hdata['samples']}r)", pace_conf,
        )
        if blocked:
            return None, pace_conf, reason

    # 2. Meta bias
    mdata = cfg.get("meta", {}).get(str(meta_bucket))
    if mdata and mdata.get("samples", 0) >= BIAS_MIN_SAMPLES:
        pace_conf, blocked, reason = _calc_bias_delta(
            pace_rec, mdata["pct_mais"],
            f"{mk}: meta{meta_int} ({mdata['samples']}r)", pace_conf,
        )
        if blocked:
            return None, pace_conf, reason

    # 3. Combo hora × meta (exige padrão mais forte: >15 pp)
    combo_key = f"{cur_hour}_{meta_bucket}"
    cdata = cfg.get("combos", {}).get(combo_key)
    if cdata and cdata.get("samples", 0) >= BIAS_COMBO_MIN_SAMPLES:
        pct = cdata["pct_mais"]
        if abs(pct - 50) > 15:
            pace_conf, blocked, reason = _calc_bias_delta(
                pace_rec, pct,
                f"{mk}: {cur_hour}h+meta{meta_int} ({cdata['samples']}r)", pace_conf,
            )
            if blocked:
                return None, pace_conf, reason

    return pace_rec, pace_conf, None


def _apply_streak_bias(mk: str, pace_rec: str, pace_conf: int):
    """
    Mean-reversion após 3 resultados consecutivos iguais.
    Retorna (pace_rec_ou_None, pace_conf, reason_ou_None).
    """
    recent = list(last_results[mk])[:3]
    if len(recent) < 3 or len(set(recent)) != 1:
        return pace_rec, pace_conf, None
    streak_dir = recent[0]
    if streak_dir != pace_rec:
        return pace_rec, pace_conf, None
    penalties = {"aves": 20, "rua": 12, "rodovia": 8}
    penalty   = penalties.get(mk, 0)
    if not penalty:
        return pace_rec, pace_conf, None
    new_conf = max(0, pace_conf - penalty)
    if new_conf < LIVE_SIGNAL_MIN_CONF:
        return None, new_conf, f"{mk}: reversão após 3× {streak_dir} consecutivos"
    return pace_rec, new_conf, None


def _compute_live_proba(mk: str):
    """
    Calcula probabilidade ao vivo usando contagem atual + tempo restante.
    Projeta o valor final baseado no ritmo atual e compara com a meta.
    Retorna (rec, conf_pct, reason) ou (None, 0, reason).
    """
    st        = states[mk]
    count     = st.get("current_count")
    remaining = st.get("remaining_seconds")
    meta      = st.get("value_needed")

    if count is None or remaining is None or meta is None:
        return None, 0, "dados insuficientes"

    count     = int(count)
    meta      = int(meta)
    remaining = int(remaining)

    # Contagem já passou da meta → certeza de MAIS
    if count > meta:
        return "MAIS", 96, f"contagem {count} já passou da meta {meta}"

    total   = MARKET_CONFIGS[mk]["duration"]
    elapsed = total - remaining
    min_elapsed, min_remaining, peak_ctx = _live_window_requirements(mk, meta)
    peak_rec, peak_conf, peak_reason = peak_ctx
    if remaining < min_remaining:
        return None, 0, (
            f"fora da janela de aposta "
            f"({remaining}s restantes, minimo {min_remaining}s)"
        )

    if elapsed < min_elapsed or count == 0:
        return None, 0, f"cedo demais para projetar ({elapsed}s/{min_elapsed}s)"

    rate_sec = count / max(elapsed, 1)
    rate_min = rate_sec * 60
    projected = count + rate_sec * remaining
    needed_count = max(meta + 1 - count, 0)
    needed_rate_sec = needed_count / max(remaining, 1)
    needed_rate_min = needed_rate_sec * 60
    trend_ratio, trend_conf, trend_text = _live_count_trend(
        mk,
        count,
        remaining,
        elapsed,
        rate_sec,
    )

    gap_pct = (projected - meta) / max(meta, 1) * 100
    pace_ratio = rate_sec / needed_rate_sec if needed_rate_sec > 0 else float("inf")
    pace_text = (
        f"ritmo {count}/{elapsed}s = {rate_min:.1f}/min | "
        f"necessario {needed_rate_min:.1f}/min | "
        f"projeta {int(projected)} (meta {meta}, {gap_pct:+.0f}%)"
    )
    peak_text = (
        f"pico local+hora {peak_rec}/{peak_conf}% ({peak_reason})"
        if peak_rec else
        peak_reason
    )

    pace_rec = None
    pace_conf = 0
    if gap_pct >= 18 and pace_ratio >= 1.12:
        pace_rec = "MAIS"
        pace_conf = min(94, 64 + int(abs(gap_pct) * 0.6) + int((pace_ratio - 1) * 20))
    elif gap_pct <= -18 and pace_ratio <= 0.88:
        pace_rec = "ATE"
        pace_conf = min(94, 64 + int(abs(gap_pct) * 0.6) + int((1 - pace_ratio) * 20))
    else:
        if peak_rec and peak_conf >= 60:
            return None, 0, f"sem vantagem forte no ritmo | {pace_text} | {trend_text} | {peak_text}"
        return None, 0, f"sem vantagem forte | {pace_text} | {trend_text}"

    # ── Bias adaptativo: hora, meta, combos (recalculado a cada hora) ──
    cur_hour = datetime.now().hour
    meta_int  = int(meta) if meta is not None else 0

    pace_rec, pace_conf, _bias_reason = _apply_adaptive_bias(mk, pace_rec, pace_conf, cur_hour, meta_int)
    if pace_rec is None:
        return None, 0, _bias_reason or f"bloqueado por bias adaptativo | {pace_text}"

    # Streak mean-reversion (comportamental, separado do bias historico)
    pace_rec, pace_conf, _streak_reason = _apply_streak_bias(mk, pace_rec, pace_conf)
    if pace_rec is None:
        return None, 0, f"{_streak_reason} | {pace_text}"

    if trend_ratio is not None:
        if pace_rec == "MAIS":
            if trend_ratio >= 1.12:
                pace_conf = min(97, pace_conf + min(LIVE_TREND_ACCEL_BOOST, trend_conf))
            elif trend_ratio <= 0.88 and gap_pct < 28:
                pace_conf = max(0, pace_conf - max(3, min(LIVE_TREND_DECAY_PENALTY, trend_conf or LIVE_TREND_DECAY_PENALTY)))
                if pace_conf < LIVE_SIGNAL_MIN_CONF - 6:
                    return None, 0, f"ritmo desacelerando contra o MAIS | {pace_text} | {trend_text}"
        elif pace_rec == "ATE":
            if trend_ratio <= 0.90:
                pace_conf = min(97, pace_conf + min(LIVE_TREND_ACCEL_BOOST, trend_conf))
            elif trend_ratio >= 1.10 and gap_pct > -28:
                pace_conf = max(0, pace_conf - max(3, min(LIVE_TREND_DECAY_PENALTY, trend_conf or LIVE_TREND_DECAY_PENALTY)))
                if pace_conf < LIVE_SIGNAL_MIN_CONF - 6:
                    return None, 0, f"ritmo reacelerando contra o ATE | {pace_text} | {trend_text}"

    if peak_rec and peak_conf >= 60:
        if peak_rec == pace_rec:
            pace_conf = min(
                97,
                max(
                    pace_conf,
                    int(round(pace_conf * 0.72 + peak_conf * 0.28)) + (3 if mk == "rua" else 1),
                ),
            )
            return pace_rec, pace_conf, f"{pace_text} | {trend_text} | {peak_text}"
        if peak_conf >= 64:
            return None, 0, (
                f"ritmo {pace_rec}/{pace_conf}% conflita com "
                f"{peak_rec}/{peak_conf}% no pico | {pace_text} | {trend_text}"
            )
        return pace_rec, pace_conf, f"{pace_text} | {trend_text} | pico divergente fraco ({peak_reason})"

    return pace_rec, pace_conf, f"{pace_text} | {trend_text}"


def _live_proba_public_payload(mk: str) -> dict:
    rec, conf_pct, reason = _compute_live_proba(mk)
    return {
        "rec": rec,
        "conf_pct": int(conf_pct or 0),
        "reason": str(reason or ""),
    }


def _dispatch_signal_delivery(
    mk: str,
    message: str,
    *,
    market_id,
    rec: str,
    kind: str,
    conf_pct: int,
    log_ts: str,
    log_reason: str,
):
    sent_telegram_keys: set[str] = set()
    default_license_key = str(_current_license_key() or "").strip().upper()
    if default_license_key:
        sent_telegram_keys.add(default_license_key)
        with _license_thread_context(default_license_key):
            send_telegram(message, market_key=mk)
    else:
        send_telegram(message, market_key=mk)

    running_keys = _iter_running_automation_license_keys(market_key=mk)
    for license_key in running_keys:
        clean_key = str(license_key or "").strip().upper()
        if clean_key not in sent_telegram_keys:
            sent_telegram_keys.add(clean_key)

            def _telegram_worker(target_key=clean_key):
                with _license_thread_context(target_key):
                    send_telegram(message, market_key=mk)

            threading.Thread(target=_telegram_worker, daemon=True).start()

        def _automation_worker(target_key=clean_key):
            with _license_thread_context(target_key):
                _automation_execute_signal(
                    mk,
                    market_id,
                    rec,
                    kind,
                    conf_pct,
                    license_key=target_key,
                )

        threading.Thread(target=_automation_worker, daemon=True).start()

    print(
        f"[{log_ts}:{mk}] Sinal {kind}: {rec} | {conf_pct}% | {log_reason}",
        flush=True,
    )


def _try_send_live_signal(mk: str):
    """Envia sinal ao vivo baseado em contagem atual + tempo restante (uma vez por rodada)."""
    st        = states[mk]
    market_id = st.get("market_id")
    remaining = st.get("remaining_seconds")
    meta      = st.get("value_needed")
    if remaining is None:
        return

    total   = MARKET_CONFIGS[mk]["duration"]
    elapsed = total - int(remaining)
    min_elapsed, min_remaining, _ = _live_window_requirements(mk, meta)

    if elapsed < min_elapsed:
        return

    # Só envia uma vez por rodada
    if _live_signal_mid.get(mk) == market_id:
        return

    rec, live_conf_pct, live_reason = _compute_live_proba(mk)
    if rec is None:
        return

    hist_rec, hist_conf_pct, hist_reason = _compute_recommendation(
        mk,
        location=st.get("location"),
        value_needed=st.get("value_needed"),
    )
    if hist_rec is None:
        hist_rec = rec
        hist_conf_pct = live_conf_pct
        hist_reason = "sem confirmacao historica"

    if hist_rec != rec:
        hist_reason = f"divergente do historico ({hist_rec}/{hist_conf_pct}%)"

    conf_pct = min(
        97,
        max(live_conf_pct, int(round(live_conf_pct * 0.7 + hist_conf_pct * 0.3))),
    )
    signal_odd = _fetch_signal_odd(market_id, rec)
    odd_pressure = _odd_pressure_text(
        mk,
        market_id,
        rec,
        current_odd=_as_float(signal_odd),
    )
    reason = (
        f"odd {signal_odd or '?'} | {odd_pressure} | {live_reason} | "
        f"confirmado por padrao {hist_conf_pct}% ({hist_reason})"
    )

    market_signal_min = int(round(_market_signal_min_confidence(mk)))

    _live_signal_mid[mk] = market_id
    _live_signal_rec[mk] = rec
    _remember_sent_signal(
        mk,
        market_id,
        rec,
        "AO VIVO",
        conf_pct,
        odd=signal_odd,
        strategy=f"Ritmo ao vivo + {hist_reason}",
    )

    ts        = datetime.now().strftime("%H:%M:%S")
    rec_icon  = "🟢 MAIS" if rec == "MAIS" else "🔴 ATÉ"
    loc_str   = (st.get("location") or "").split(" — ")[0]
    mname     = MARKET_CONFIGS[mk]["name"]
    conf_icon = "🔥" if conf_pct >= 80 else "📈"
    _check_day_reset()
    score       = telegram_score[mk]
    total_sc    = score["wins"] + score["losses"]
    placar_hoje = f"✅ {score['wins']}  ❌ {score['losses']}" if total_sc else "Primeiro sinal do dia"

    msg = (
        f"📡 <b>SINAL AO VIVO — {mname}</b>\n"
        f"📍 {loc_str}  |  ⏱ {int(remaining)}s restantes\n"
        f"\n"
        f"▶️ Entrada: <b>{rec_icon}</b>\n"
        f"{conf_icon} Confiança: <b>{conf_pct}%</b>  (odd {signal_odd or '?'})\n"
        f"\n"
        f"{_telegram_operational_context(mk, conf_pct, rec)}\n"
        f"\n"
        f"{_build_why_block(mk, rec, reason)}\n"
        f"\n"
        f"📊 Hoje: {placar_hoje}\n"
        f"\n"
        f"<i>⚠️ Apenas probabilidade. Aposte por conta e risco.</i>"
    )
    msg += _telegram_bankroll_note(mk)

    threading.Thread(
        target=_dispatch_signal_delivery,
        kwargs={
            "mk": mk,
            "message": msg,
            "market_id": market_id,
            "rec": rec,
            "kind": "AO VIVO",
            "conf_pct": conf_pct,
            "log_ts": ts,
            "log_reason": reason,
        },
        daemon=True,
    ).start()


def _telegram_hour_window_snapshot(
    mk: str,
    rec: str | None = None,
    *,
    location: str | None = None,
    value_needed=None,
) -> dict:
    st = states.get(mk) or {}
    hour = datetime.now().hour
    target_location = location if location is not None else st.get("location")
    target_meta = _as_float(value_needed if value_needed is not None else st.get("value_needed"))
    entries = []
    for entry in list(histories.get(mk) or []):
        if target_location and entry.get("location") != target_location:
            continue
        entry_dt = _entry_datetime(entry)
        if entry_dt is None or entry_dt.hour != hour:
            continue
        entries.append(entry)

    counts = []
    results = []
    near_meta = 0
    for entry in entries:
        count_val = _as_float(entry.get("count"))
        if count_val is not None:
            counts.append(count_val)
            if target_meta is not None and abs(count_val - target_meta) <= 15 and count_val < target_meta:
                near_meta += 1
        result = str(entry.get("result") or "").upper()
        if result in {"MAIS", "ATE"}:
            results.append(result)

    total = len(entries)
    avg_count = (sum(counts) / len(counts)) if counts else None
    same_side = 0
    opp_side = 0
    side_pct = None
    if rec in {"MAIS", "ATE"} and results:
        same_side = sum(1 for item in results if item == rec)
        opp_side = sum(1 for item in results if item != rec)
        side_pct = int(round((same_side / len(results)) * 100))

    return {
        "label": _hour_window_label(hour),
        "total": total,
        "avg_count": avg_count,
        "meta": target_meta,
        "same_side": same_side,
        "opp_side": opp_side,
        "side_pct": side_pct,
        "near_meta": near_meta,
    }


def _telegram_live_pace_snapshot(mk: str) -> dict:
    st = states.get(mk) or {}
    count = _as_float(st.get("current_count"))
    remaining = _as_float(st.get("remaining_seconds"))
    meta = _as_float(st.get("value_needed"))
    total = _as_float((MARKET_CONFIGS.get(mk) or {}).get("duration"))
    if count is None or remaining is None or meta is None or total is None:
        return {}
    elapsed = max(total - remaining, 0.0)
    if elapsed <= 0:
        return {}
    rate_min = (count / max(elapsed, 1.0)) * 60.0
    needed = max(meta + 1 - count, 0.0)
    need_rate_min = (needed / max(remaining, 1.0)) * 60.0 if remaining > 0 else 0.0
    projected = count + (count / max(elapsed, 1.0)) * remaining
    return {
        "count": int(round(count)),
        "elapsed": int(round(elapsed)),
        "remaining": int(round(remaining)),
        "meta": int(round(meta)),
        "rate_min": rate_min,
        "need_rate_min": need_rate_min,
        "projected": int(round(projected)),
    }


def _build_why_block(mk: str, rec: str, reason: str) -> str:
    """
    Constrói o bloco 'Por que entramos?' para o Telegram.
    Para mercados de preço incorpora análise técnica do próprio mercado.
    Para outros mercados usa só o reason do histórico.
    """
    is_price = bool(MARKET_CONFIGS[mk].get("is_price_market"))
    lines = []

    lines.append(f"<b>📖 Por que entramos?</b>")
    hour_snapshot = _telegram_hour_window_snapshot(mk, rec)
    if int(hour_snapshot.get("total") or 0) > 0:
        lines.append(
            f"• Janela {hour_snapshot['label']}: "
            f"{int(hour_snapshot['total'])} rodadas no mesmo local"
        )
        avg_count = hour_snapshot.get("avg_count")
        meta = hour_snapshot.get("meta")
        side_pct = hour_snapshot.get("side_pct")
        if avg_count is not None and meta is not None:
            summary = f"• Média {avg_count:.1f} vs meta {meta:.0f}"
            if side_pct is not None:
                summary += f" | {rec} {side_pct}%"
            lines.append(summary)
        elif side_pct is not None:
            lines.append(f"• Janela favorece {rec}: {side_pct}%")
        if int(hour_snapshot.get("near_meta") or 0) > 0:
            lines.append(f"• Chegou perto da meta e nao bateu {int(hour_snapshot['near_meta'])}x")

    pace_snapshot = _telegram_live_pace_snapshot(mk)
    if pace_snapshot:
        lines.append(
            f"• Ritmo agora: {pace_snapshot['count']} em {pace_snapshot['elapsed']}s = "
            f"{pace_snapshot['rate_min']:.1f}/min | precisa {pace_snapshot['need_rate_min']:.1f}/min | "
            f"projeta {pace_snapshot['projected']}"
        )
    elif reason:
        short_reason = str(reason or "").split(" | ")
        if short_reason:
            lines.append(f"• Leitura: {short_reason[0]}")

    if is_price:
        try:
            tech = _fetch_price_market_technicals(mk)
            if tech:
                overall = tech.get("overall", "—")
                rsi     = tech.get("rsi")
                macd_d  = tech.get("macd_dir", "")
                stoch   = tech.get("stoch")
                atr     = tech.get("atr")
                sma_sig = tech.get("sma_signal", "—")

                # Align technical bias with rec
                tech_dir = "SOBE" if "Compra" in overall else ("DESCE" if "Venda" in overall else "Neutro")
                align_icon = "✅" if (
                    (tech_dir == "SOBE" and rec == "MAIS") or
                    (tech_dir == "DESCE" and rec == "ATE")
                ) else ("⚠️" if tech_dir == "Neutro" else "⚡")

                lines.append(f"• Análise técnica ({align_icon} {overall}):")
                if rsi is not None:
                    rsi_icon = "🔴" if rsi >= 70 else ("🟢" if rsi <= 30 else "🟡")
                    lines.append(f"  — RSI(14): {rsi} {rsi_icon} {tech.get('rsi_signal','')}")
                if stoch is not None:
                    lines.append(f"  — Stoch(14): {stoch} ({tech.get('stoch_signal','')})")
                if macd_d:
                    lines.append(f"  — MACD: tendência de {macd_d}")
                if sma_sig and sma_sig != "—":
                    lines.append(f"  — SMAs: {sma_sig}")
                if atr is not None:
                    lines.append(f"  — Volatilidade ATR(14): {atr}")
        except Exception as _te:
            pass

        # Volatility from last round
        try:
            st = states[mk]
            init_p = st.get("initial_price")
            cur_p  = st.get("current_count") or st.get("value_needed")
            if init_p and cur_p:
                pct = (float(cur_p) - float(init_p)) / float(init_p) * 100
                arrow = "📈" if pct >= 0 else "📉"
                lines.append(f"• Preço rodada: {arrow} {pct:+.2f}% (abertura R$ {float(init_p):.2f})")
        except Exception:
            pass

    return "\n".join(lines)


def _send_signal(
    mk: str,
    rem,
    location: str | None = None,
    value_needed=None,
    market_id=None,
):
    """Envia sinal de recomendação para o Telegram no início de uma rodada."""
    # Dedup: não envia 2 sinais para o mesmo mercado em < 90s
    if not SEND_OPENING_SIGNALS:
        print(
            f"[{datetime.now().strftime('%H:%M:%S')}:{mk}] "
            f"Sinal de abertura desativado; aguardando AO VIVO com ritmo + padrao",
            flush=True,
        )
        return

    now = time.time()
    if now - _last_signal_ts.get(mk, 0) < 90:
        return
    _last_signal_ts[mk] = now

    ts  = datetime.now().strftime("%H:%M:%S")
    rec, conf_pct, reason = _compute_recommendation(
        mk,
        location=location,
        value_needed=value_needed,
    )

    if rec is None:
        print(f"[{ts}:{mk}] Sinal NAO enviado: {reason}", flush=True)
        return

    signal_odd = _fetch_signal_odd(
        market_id or states[mk].get("market_id"),
        rec,
    )
    odd_pressure = _odd_pressure_text(
        mk,
        market_id or states[mk].get("market_id"),
        rec,
        current_odd=_as_float(signal_odd),
    )
    strategy_reason = reason
    reason = f"odd {signal_odd or '?'} | {odd_pressure} | {reason}"

    market_signal_min = int(round(_market_signal_min_confidence(mk)))

    _remember_sent_signal(
        mk,
        market_id or states[mk].get("market_id"),
        rec,
        "ABERTURA",
        conf_pct,
        odd=signal_odd,
        strategy=strategy_reason,
    )
    _check_day_reset()
    rec_icon    = "🟢 MAIS" if rec == "MAIS" else "🔴 ATÉ"
    loc_str     = (location or states[mk].get("location") or "").split(" — ")[0]
    mins        = int(rem) // 60 if rem else "?"
    secs        = int(rem) % 60  if rem else "?"
    dur_str     = f"{mins}m{secs:02d}s" if rem else "?"
    score       = telegram_score[mk]
    total_sc    = score["wins"] + score["losses"]
    placar_hoje = f"✅ {score['wins']}  ❌ {score['losses']}" if total_sc else "Primeiro sinal do dia"
    market_name = MARKET_CONFIGS[mk]["name"]
    conf_icon   = "🔥" if conf_pct >= 75 else ("✅" if conf_pct >= 65 else "⚡")

    signal_msg = (
        f"🎯 <b>SINAL — {market_name}</b>\n"
        f"📍 {loc_str}  |  ⏱ {dur_str}\n"
        f"\n"
        f"▶️ Entrada: <b>{rec_icon}</b>\n"
        f"{conf_icon} Confiança: <b>{conf_pct}%</b>  (odd {signal_odd or '?'})\n"
        f"\n"
        f"{_telegram_operational_context(mk, conf_pct, rec)}\n"
        f"\n"
        f"{_build_why_block(mk, rec, strategy_reason)}\n"
        f"\n"
        f"📊 Hoje: {placar_hoje}\n"
        f"\n"
        f"<i>⚠️ Apenas probabilidade. Aposte por conta e risco.</i>"
    )
    signal_msg += _telegram_bankroll_note(mk)

    threading.Thread(
        target=_dispatch_signal_delivery,
        kwargs={
            "mk": mk,
            "message": signal_msg,
            "market_id": market_id or states[mk].get("market_id"),
            "rec": rec,
            "kind": "ABERTURA",
            "conf_pct": conf_pct,
            "log_ts": ts,
            "log_reason": reason,
        },
        daemon=True,
    ).start()

# ──────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────
PUSHER_KEY  = "l2zxrvk0g4lfl35q8cxr"
WS_HOST     = "ws.palpitano.com"
MARKETS_API = "https://app.palpitano.com/app/getLastAvailableMarkets"
UA_HEADER   = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36"

# ── Telegram ──────────────────────────────────
# Opcional. Em producao cada cliente pode configurar o proprio bot pelo painel.
TELEGRAM_TOKEN   = str(os.getenv("ODDSYNC_TELEGRAM_TOKEN") or "").strip()
TELEGRAM_CHAT_ID = str(os.getenv("ODDSYNC_TELEGRAM_CHAT_ID") or "").strip()
WS_URL      = (
    f"wss://{WS_HOST}/app/{PUSHER_KEY}"
    "?protocol=7&client=py&version=7.0.6&flash=false"
)
WS_ORIGIN_URL = "https://app.palpitano.com"
_ws_resolve_cache_lock = threading.RLock()
_ws_resolve_cache: dict[str, str] = {}
_PROXY_ENV_KEYS = (
    "ALL_PROXY",
    "all_proxy",
    "HTTP_PROXY",
    "http_proxy",
    "HTTPS_PROXY",
    "https_proxy",
    "NO_PROXY",
    "no_proxy",
)
_BROKEN_PROXY_MARKERS = (
    "127.0.0.1:9",
    "localhost:9",
)


def _resolve_host_ipv4(host: str, port: int = 443) -> str | None:
    name = str(host or "").strip().lower()
    if not name:
        return None
    with _ws_resolve_cache_lock:
        cached = _ws_resolve_cache.get(name)
    if cached:
        return cached
    try:
        infos = socket.getaddrinfo(name, port, socket.AF_INET, socket.SOCK_STREAM)
    except socket.gaierror:
        return None
    except Exception:
        return None
    for info in infos:
        sockaddr = info[4] if len(info) > 4 else None
        if not sockaddr:
            continue
        ip = str(sockaddr[0] or "").strip()
        if ip:
            with _ws_resolve_cache_lock:
                _ws_resolve_cache[name] = ip
            return ip
    return None


def _build_ws_runtime_options() -> tuple[str, list[str], dict]:
    headers = [
        f"Origin: {WS_ORIGIN_URL}",
        f"Host: {WS_HOST}",
        f"User-Agent: {UA_HEADER}",
        "Cache-Control: no-cache",
        "Pragma: no-cache",
    ]
    return WS_URL, headers, {}


def _build_host_fallback_request(url: str, *, headers: dict | None = None) -> urllib.request.Request:
    parsed = urllib.parse.urlparse(str(url or "").strip())
    host = str(parsed.hostname or "").strip()
    if not host:
        return urllib.request.Request(url, headers=headers or {})
    fallback_ip = _resolve_host_ipv4(host, parsed.port or (443 if parsed.scheme == "https" else 80))
    final_headers = dict(headers or {})
    if not fallback_ip:
        return urllib.request.Request(url, headers=final_headers)
    netloc = fallback_ip
    if parsed.port:
        netloc = f"{fallback_ip}:{parsed.port}"
    fallback_url = urllib.parse.urlunparse(
        (
            parsed.scheme,
            netloc,
            parsed.path,
            parsed.params,
            parsed.query,
            parsed.fragment,
        )
    )
    final_headers.setdefault("Host", host)
    return urllib.request.Request(fallback_url, headers=final_headers)


def _sanitize_broken_proxy_env() -> bool:
    removed_any = False
    for key in _PROXY_ENV_KEYS:
        value = str(os.environ.get(key) or "").strip().lower()
        if not value:
            continue
        if any(marker in value for marker in _BROKEN_PROXY_MARKERS):
            del os.environ[key]
            removed_any = True
    return removed_any


@contextlib.contextmanager
def _bypass_proxy_env():
    saved: dict[str, str] = {}
    removed: list[str] = []
    for key in _PROXY_ENV_KEYS:
        if key in os.environ:
            saved[key] = os.environ[key]
            removed.append(key)
            del os.environ[key]
    try:
        yield
    finally:
        for key in removed:
            if key in saved:
                os.environ[key] = saved[key]


def _urlopen_no_proxy(req, timeout: float = 10):
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    return opener.open(req, timeout=timeout)


if _sanitize_broken_proxy_env():
    print("[NET] Proxy local quebrado removido do ambiente", flush=True)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BIAS_CONFIG_FILE     = os.path.join(BASE_DIR, "bias_config.json")  # gerado automaticamente a cada hora
ACCESS_CONFIG_FILE   = os.path.join(BASE_DIR, "access_config.json")

MARKET_CONFIGS = {
    "bitcoin": {
        "slug_keyword": "bitcoin",
        "name": "Bitcoin (5 min)",
        "history_file": os.path.join(BASE_DIR, "historico_bitcoin.json"),
        "duration": 300,
        "betting_min_remaining_seconds": 180,
        "live_min_elapsed_seconds": 70,
        "is_price_market": True,
        "outcome_labels": {"MAIS": "SOBE", "ATE": "DESCE"},
    },
    "rodovia": {
        "slug_keyword": "rodovia",
        "name": "Rodovia (5 min)",
        "history_file": os.path.join(BASE_DIR, "historico_rodovia.json"),
        "duration": 300,
        "betting_min_remaining_seconds": 180,
        "live_min_elapsed_seconds": 70,
        "pattern_loc_hour_min_samples": 3,
        "pattern_loc_hour_min_gap": 4,
        "pattern_weekday_loc_hour_min_samples": 3,
        "pattern_weekday_loc_hour_min_gap": 3,
        "outcome_labels": {"MAIS": "MAIS", "ATE": "ATÉ"},
    },
    "rua": {
        "slug_keyword": "rua",
        "name": "Rua (4m 40s)",
        "history_file": os.path.join(BASE_DIR, "historico_rua.json"),
        "duration": 280,
        "late_safe_min_remaining_seconds": 20,
        "live_peak_min_samples": 10,
        "live_peak_min_gap": 5,
        "live_peak_base_conf": 62,
        "live_peak_max_conf": 88,
        "pattern_loc_hour_min_samples": 3,
        "pattern_loc_hour_min_gap": 4,
        "pattern_weekday_loc_hour_min_samples": 3,
        "pattern_weekday_loc_hour_min_gap": 3,
        "outcome_labels": {"MAIS": "MAIS", "ATE": "ATÉ"},
    },
}

# ─────────────────────────────────────────────────────────────────
#  Auto-aprendizado adaptativo: recalcula bias a cada hora
# ─────────────────────────────────────────────────────────────────

def _recompute_bias_config():
    """
    Lê os arquivos de histórico de todos os mercados e recalcula as
    estatísticas de bias por hora, meta e combo hora×meta.
    Atualiza _live_bias_config em memória e grava bias_config.json atomicamente.
    """
    import collections as _col

    _meta_bucket_sizes = {"rodovia": 20, "rua": 10}
    new_config = {}

    for mk, mk_cfg in MARKET_CONFIGS.items():
        hist_file = mk_cfg.get("history_file")
        if not hist_file or not os.path.exists(hist_file):
            continue
        try:
            with open(hist_file, encoding="utf-8") as _f:
                data = json.load(_f)
        except Exception as _exc:
            print(f"[BiasConfig:{mk}] Erro ao ler histórico: {_exc}", flush=True)
            continue

        bucket_size = _meta_bucket_sizes.get(mk, 10)
        hour_m  = _col.defaultdict(int)
        hour_a  = _col.defaultdict(int)
        meta_m  = _col.defaultdict(int)
        meta_a  = _col.defaultdict(int)
        combo_m = _col.defaultdict(int)
        combo_a = _col.defaultdict(int)

        for r in data:
            try:
                h   = int(r["time"].split(":")[0])
                res = r.get("result", "")
                mv  = r.get("value_needed") or 0
                mb  = int(float(mv)) // bucket_size * bucket_size
                ck  = f"{h}_{mb}"
                if res == "MAIS":
                    hour_m[h] += 1; meta_m[mb] += 1; combo_m[ck] += 1
                elif res == "ATE":
                    hour_a[h] += 1; meta_a[mb] += 1; combo_a[ck] += 1
            except Exception:
                continue

        hours_cfg: dict = {}
        for h in range(24):
            m, a = hour_m[h], hour_a[h]; t = m + a
            if t >= BIAS_MIN_SAMPLES:
                hours_cfg[str(h)] = {"pct_mais": round(m / t * 100, 1), "samples": t}

        meta_cfg: dict = {}
        for mb in sorted(set(list(meta_m) + list(meta_a))):
            m, a = meta_m[mb], meta_a[mb]; t = m + a
            if t >= BIAS_MIN_SAMPLES:
                meta_cfg[str(mb)] = {"pct_mais": round(m / t * 100, 1), "samples": t}

        combo_cfg: dict = {}
        for ck in set(list(combo_m) + list(combo_a)):
            m, a = combo_m[ck], combo_a[ck]; t = m + a
            if t >= BIAS_COMBO_MIN_SAMPLES:
                combo_cfg[ck] = {"pct_mais": round(m / t * 100, 1), "samples": t}

        new_config[mk] = {
            "meta_bucket_size": bucket_size,
            "hours":  hours_cfg,
            "meta":   meta_cfg,
            "combos": combo_cfg,
        }

    new_config["_generated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Salva atomicamente
    try:
        _tmp = BIAS_CONFIG_FILE + ".tmp"
        with open(_tmp, "w", encoding="utf-8") as _f:
            json.dump(new_config, _f, indent=2, ensure_ascii=False)
        os.replace(_tmp, BIAS_CONFIG_FILE)
    except Exception as _exc:
        print(f"[BiasConfig] Erro ao salvar: {_exc}", flush=True)

    # Atualiza em memória
    with _bias_config_lock:
        _live_bias_config.clear()
        for _k, _v in new_config.items():
            if not _k.startswith("_"):
                _live_bias_config[_k] = _v

    _mk_info = ", ".join(
        f"{mk}: {len(v.get('combos', {}))} combos"
        for mk, v in new_config.items()
        if not mk.startswith("_")
    )
    print(f"[BiasConfig] Recalculado {new_config['_generated_at']} — {_mk_info}", flush=True)


def _bias_updater_loop():
    """Thread daemon: recalcula o bias_config a cada hora."""
    while True:
        try:
            time.sleep(BIAS_UPDATE_INTERVAL)
            _recompute_bias_config()
        except Exception as _exc:
            print(f"[BiasConfig] Erro no loop: {_exc}", flush=True)


# ──────────────────────────────────────────────
# Per-market runtime state
# ──────────────────────────────────────────────
def _mk_state():
    return {
        "connected": False, "current_count": None, "remaining_seconds": None,
        "status": "aguardando", "last_updated": None, "market_id": None,
        "value_needed": None, "location": None, "slug": None, "odds": {},
        "selection_codes": {}, "selection_ids": {},
        "initial_price": None, "final_price": None,
        "live_page_url": None,
    }

states       = {k: _mk_state()       for k in MARKET_CONFIGS}
histories    = {k: deque() for k in MARKET_CONFIGS}
last_results = {k: deque(maxlen=10)  for k in MARKET_CONFIGS}
# Bias adaptativo: config gerada em background a cada hora
_live_bias_config: dict = {}
_bias_config_lock  = threading.Lock()
odds_history = {k: deque(maxlen=60)  for k in MARKET_CONFIGS}
count_history = {k: deque(maxlen=80) for k in MARKET_CONFIGS}
sse_clients  = {k: []                for k in MARKET_CONFIGS}
sse_locks    = {k: threading.Lock()  for k in MARKET_CONFIGS}
history_locks = {k: threading.RLock() for k in MARKET_CONFIGS}
# Placar diário Telegram — reseta automaticamente por dia
def _today(): return datetime.now().strftime("%Y-%m-%d")
def _load_score():
    today = _today()
    try:
        data = _load_private_json("placar_telegram.json", {})
        result = {}
        for k in MARKET_CONFIGS:
            entry = data.get(k, {})
            if entry.get("date") == today:
                result[k] = entry
            else:
                result[k] = {"date": today, "wins": 0, "losses": 0}
        return result
    except Exception:
        return {k: {"date": today, "wins": 0, "losses": 0} for k in MARKET_CONFIGS}
def _save_score(score_map: dict | None = None):
    _save_private_json("placar_telegram.json", score_map if score_map is not None else telegram_score, indent=2)


def _reset_score_today() -> dict:
    today = _today()
    if _has_session_license_context():
        score_map = _load_score()
        for k in MARKET_CONFIGS:
            score_map[k] = {"date": today, "wins": 0, "losses": 0}
        _save_score(score_map)
        return score_map
    for k in MARKET_CONFIGS:
        telegram_score[k] = {"date": today, "wins": 0, "losses": 0}
    _save_score()
    return telegram_score


def _check_day_reset():
    today = _today()
    for k in MARKET_CONFIGS:
        if telegram_score[k].get("date") != today:
            telegram_score[k] = {"date": today, "wins": 0, "losses": 0}
            print(f"[Placar:{k}] Novo dia — placar zerado", flush=True)
    _save_score()
telegram_score = _load_score()


license_lock = threading.RLock()
LICENSE_STORE_FILE = os.path.join(BASE_DIR, "licenses.json")
LICENSE_STATE_FILE = os.path.join(BASE_DIR, "license_state.json")


def _default_license_state() -> dict:
    return {
        "license_key": "",
        "activated_at": None,
        "device_fingerprint": "",
    }


def _device_fingerprint() -> str:
    raw = f"{socket.gethostname()}|{os.environ.get('USERNAME','')}|{BASE_DIR}"
    return hashlib.sha256(raw.encode("utf-8", errors="ignore")).hexdigest()[:16].upper()


def _request_ip() -> str:
    if not has_request_context():
        return ""
    forwarded = str(request.headers.get("CF-Connecting-IP") or request.headers.get("X-Forwarded-For") or "").strip()
    if forwarded:
        return forwarded.split(",")[0].strip()
    return str(request.remote_addr or "").strip()


def _request_user_agent() -> str:
    if not has_request_context():
        return ""
    return str(request.headers.get("User-Agent") or "").strip()[:240]


def _request_device_hash() -> str:
    raw = _request_user_agent()
    if not raw.strip():
        return ""
    return hashlib.sha256(raw.encode("utf-8", errors="ignore")).hexdigest()[:16].upper()


def _license_device_allowed(entry: dict) -> tuple[bool, str]:
    return True, ""


def _touch_license_access(entry: dict, now_iso: str):
    current_ip = _request_ip()
    user_agent = _request_user_agent()
    device_hash = _request_device_hash()
    if current_ip:
        entry["last_ip"] = current_ip
    if user_agent:
        entry["last_user_agent"] = user_agent
    if device_hash:
        entry["last_device_hash"] = device_hash
    entry["last_seen_at"] = now_iso


def _load_license_store() -> dict:
    return _sqlite_load_license_store()


def _save_license_store_unsafe():
    _sqlite_save_license_store(license_store)


def _refresh_license_store_unsafe():
    global license_store
    license_store = _load_license_store()
    return license_store


def _load_license_state() -> dict:
    try:
        with open(LICENSE_STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            state = _default_license_state()
            state.update(data)
            return state
    except Exception:
        pass
    return _default_license_state()


def _save_license_state_unsafe():
    _atomic_json_write(LICENSE_STATE_FILE, license_state, indent=2)


def _find_license_entry(key: str) -> dict | None:
    clean_key = str(key or "").strip().upper()
    if not clean_key:
        return None
    for entry in (license_store.get("licenses") or []):
        if str(entry.get("key") or "").upper() == clean_key:
            return entry
    return None


def _password_hash(password: str, salt: str | None = None) -> str:
    salt = salt or secrets.token_hex(8)
    digest = hashlib.sha256(f"{salt}:{password}".encode("utf-8", errors="ignore")).hexdigest()
    return f"sha256${salt}${digest}"


def _verify_license_password(entry: dict, password: str) -> bool:
    password = str(password or "")
    stored_hash = str(entry.get("password_hash") or "").strip()
    if stored_hash.startswith("sha256$"):
        try:
            _, salt, digest = stored_hash.split("$", 2)
        except ValueError:
            return False
        expected = hashlib.sha256(f"{salt}:{password}".encode("utf-8", errors="ignore")).hexdigest()
        return secrets.compare_digest(expected, digest)
    plain = str(entry.get("password") or "").strip()
    return bool(plain) and secrets.compare_digest(plain, password)


def _looks_like_masked_secret(value: str) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    return bool(re.fullmatch(r"[\*\.\u2022\u25cf\u00b7]+", text))


def _current_license_key() -> str:
    if has_request_context():
        session_key = str(session.get("license_key") or "").strip().upper()
        if session_key:
            return session_key
    thread_key = str(getattr(_thread_license_context, "license_key", "") or "").strip().upper()
    if thread_key:
        return thread_key
    return str(license_state.get("license_key") or "").strip().upper()


def _has_session_license_context() -> bool:
    if has_request_context():
        return bool(str(session.get("license_key") or "").strip())
    thread_key = str(getattr(_thread_license_context, "license_key", "") or "").strip()
    return bool(thread_key)


def _license_status_for(entry: dict | None, *, now: datetime | None = None) -> str:
    if not entry:
        return "missing"
    if entry.get("revoked"):
        return "revoked"
    expires_at = str(entry.get("expires_at") or "").strip()
    now_dt = now or datetime.now()
    if expires_at:
        try:
            if datetime.fromisoformat(expires_at) < now_dt:
                return "expired"
        except ValueError:
            return "invalid"
    return "active"


def _license_public_payload() -> dict:
    with license_lock:
        _refresh_license_store_unsafe()
        key = _current_license_key()
        entry = _find_license_entry(key)
        status = _license_status_for(entry)
        active = status == "active"
        masked = ""
        remaining_seconds = None
        remaining_label = ""
        expires_at = (entry or {}).get("expires_at")
        if expires_at:
            try:
                expires_dt = datetime.fromisoformat(str(expires_at))
                remaining_seconds = max(0, int((expires_dt - datetime.now()).total_seconds()))
                if remaining_seconds <= 0:
                    remaining_label = "expirada"
                else:
                    days, rem = divmod(remaining_seconds, 86400)
                    hours, rem = divmod(rem, 3600)
                    minutes, seconds = divmod(rem, 60)
                    if days > 0:
                        remaining_label = f"{days}d {hours:02d}h"
                    elif hours > 0:
                        remaining_label = f"{hours:02d}h {minutes:02d}m"
                    elif minutes > 0:
                        remaining_label = f"{minutes:02d}m {seconds:02d}s"
                    else:
                        remaining_label = f"{seconds}s"
            except ValueError:
                remaining_seconds = None
                remaining_label = ""
        if key:
            masked = f"{key[:4]}-****-****-{key[-4:]}" if len(key) >= 8 else key
        return {
            "active": active,
            "status": status,
            "license_key": masked,
            "customer_name": (entry or {}).get("customer_name") or "",
            "plan_name": (entry or {}).get("plan_name") or "",
            "expires_at": expires_at,
            "remaining_seconds": remaining_seconds,
            "remaining_label": remaining_label,
            "notes": (entry or {}).get("notes") or "",
            "device_fingerprint": str((entry or {}).get("device_fingerprint") or license_state.get("device_fingerprint") or ""),
            "bound_ip": str((entry or {}).get("bound_ip") or ""),
            "last_ip": str((entry or {}).get("last_ip") or ""),
            "last_seen_at": str((entry or {}).get("last_seen_at") or ""),
        }


def _empty_license_public_payload() -> dict:
    return {
        "active": False,
        "status": "invalid",
        "license_key": "",
        "customer_name": "",
        "plan_name": "",
        "expires_at": None,
        "remaining_seconds": None,
        "remaining_label": "",
        "notes": "",
        "device_fingerprint": "",
        "bound_ip": "",
        "last_ip": "",
        "last_seen_at": "",
    }


def _license_is_active() -> bool:
    return bool(_license_public_payload().get("active"))


def _license_error_message() -> str:
    status = _license_public_payload().get("status") or "missing"
    if status == "expired":
        return "Licenca expirada. Renove a chave para continuar operando."
    if status == "revoked":
        return "Licenca revogada. Entre em contato para reativacao."
    if status == "invalid":
        return "Licenca invalida. Verifique a chave cadastrada."
    return "Licenca nao ativada. Cadastre uma license key antes de operar."


def _generate_license_key() -> str:
    return "-".join(secrets.token_hex(2).upper() for _ in range(4))


def _activate_license_key(key: str, username: str = "", password: str = "") -> tuple[bool, str, dict]:
    global _active_runtime_license_key
    clean_key = str(key or "").strip().upper()
    if not clean_key:
        return False, "Informe uma license key.", _empty_license_public_payload()
    username = str(username or "").strip()

    with license_lock:
        _refresh_license_store_unsafe()
        entry = _find_license_entry(clean_key)
        if not entry:
            return False, "License key nao encontrada.", _empty_license_public_payload()
        entry_user = str(entry.get("username") or "").strip()
        if not entry_user:
            entry_user = str(entry.get("customer_name") or "cliente").strip()
        if entry_user and not secrets.compare_digest(entry_user.lower(), username.lower()):
            return False, "Usuario ou license key incorretos.", _empty_license_public_payload()
        if entry.get("password_hash") or entry.get("password"):
            if not _verify_license_password(entry, password):
                return False, "Senha incorreta.", _empty_license_public_payload()
        elif not secrets.compare_digest(str(password or "").strip().upper(), clean_key):
            return False, "Senha incorreta.", _empty_license_public_payload()
        status = _license_status_for(entry)
        if status == "revoked":
            return False, "License key revogada.", _empty_license_public_payload()
        if status == "expired":
            return False, "License key expirada.", _empty_license_public_payload()
        if status != "active":
            return False, "License key invalida.", _empty_license_public_payload()
        allowed, device_message = _license_device_allowed(entry)
        if not allowed:
            return False, device_message, _empty_license_public_payload()

        now_iso = datetime.now().isoformat(timespec="seconds")
        if not entry.get("activated_at"):
            amount = max(1, int(entry.get("duration_value") or 30))
            unit = str(entry.get("duration_unit") or "days")
            delta_kwargs = {
                "seconds": {"seconds": amount},
                "minutes": {"minutes": amount},
                "hours": {"hours": amount},
                "days": {"days": amount},
            }.get(unit, {"days": amount})
            entry["activated_at"] = now_iso
            entry["expires_at"] = (datetime.now() + timedelta(**delta_kwargs)).isoformat(timespec="seconds")
        entry["device_fingerprint"] = _device_fingerprint()
        entry["device_name"] = _request_user_agent() or "navegador"
        _touch_license_access(entry, now_iso)

        _save_license_store_unsafe()
        if has_request_context():
            session.clear()
            session["license_key"] = clean_key
            session["username"] = entry_user or username or entry.get("customer_name") or "cliente"
            session.permanent = True
        else:
            license_state["license_key"] = clean_key
            license_state["activated_at"] = now_iso
            license_state["device_fingerprint"] = _device_fingerprint()
            _save_license_state_unsafe()
    if not has_request_context():
        _reload_private_runtime_state()
        with _runtime_context_lock:
            _active_runtime_license_key = clean_key
    return True, "Licenca ativada com sucesso.", _license_public_payload()


license_store = _load_license_store()
license_state = _load_license_state()
if not str(license_state.get("device_fingerprint") or "").strip():
    license_state["device_fingerprint"] = _device_fingerprint()
    _save_license_state_unsafe()
_thread_license_context = threading.local()


def _license_block_response(status_code: int = 403):
    return jsonify({
        "success": False,
        "message": _license_error_message(),
        "license": _license_public_payload(),
    }), status_code


PRIVATE_DATA_ROOT = os.path.join(BASE_DIR, "client_data")


def _license_storage_slug_for(key: str) -> str:
    clean_key = str(key or "").strip().upper()
    if not clean_key:
        return "default"
    slug = re.sub(r"[^A-Z0-9]+", "_", clean_key).strip("_")
    return slug or "default"


def _license_storage_slug() -> str:
    return _license_storage_slug_for(_current_license_key())


def _ensure_private_data_dir() -> str:
    target = os.path.join(PRIVATE_DATA_ROOT, _license_storage_slug())
    os.makedirs(target, exist_ok=True)
    return target


def _private_data_path(filename: str) -> str:
    return os.path.join(_ensure_private_data_dir(), filename)


SQLITE_CLIENT_JSON_FILES = {
    "placar_telegram.json",
    "telegram_config.json",
    "ultimo_sinal_telegram.json",
    "automatizado_config.json",
    "saldo_timeline.json",
    "automation_orders_log.json",
    "signal_analytics.json",
}


def _client_data_key() -> str:
    return _license_storage_slug()


def _write_private_json_mirror(filename: str, payload, *, indent: int = 2):
    try:
        _atomic_json_write(_private_data_path(filename), payload, indent=indent)
    except Exception:
        pass


def _load_private_json(filename: str, default):
    if filename in SQLITE_CLIENT_JSON_FILES:
        value = _client_db_get_json(_client_data_key(), filename, None)
        if value is not None:
            _write_private_json_mirror(filename, value)
            return value
        legacy_path = _private_data_path(filename)
        if os.path.exists(legacy_path):
            try:
                with open(legacy_path, "r", encoding="utf-8-sig") as f:
                    value = json.load(f)
                _client_db_set_json(_client_data_key(), filename, value)
                _write_private_json_mirror(filename, value)
                return value
            except Exception:
                return default
        return default
    try:
        with open(_private_data_path(filename), "r", encoding="utf-8-sig") as f:
            return json.load(f)
    except Exception:
        return default


def _save_private_json(filename: str, payload, *, indent: int = 2):
    if filename in SQLITE_CLIENT_JSON_FILES:
        _client_db_set_json(_client_data_key(), filename, payload)
        _write_private_json_mirror(filename, payload, indent=indent)
        return
    _atomic_json_write(_private_data_path(filename), payload, indent=indent)


def _migrate_legacy_private_files():
    legacy_files = [
        "placar_telegram.json",
        "telegram_config.json",
        "ultimo_sinal_telegram.json",
        "automatizado_config.json",
        "saldo_timeline.json",
        "automation_orders_log.json",
        "automation_orders_log.txt",
        "signal_analytics.json",
    ]
    target_dir = _ensure_private_data_dir()
    if _license_storage_slug() != "default":
        return
    for filename in legacy_files:
        legacy_path = os.path.join(BASE_DIR, filename)
        target_path = os.path.join(target_dir, filename)
        if not os.path.exists(legacy_path) or os.path.exists(target_path):
            continue
        try:
            os.replace(legacy_path, target_path)
        except Exception:
            pass


_migrate_legacy_private_files()


telegram_lock = threading.RLock()
automation_lock = threading.RLock()
balance_timeline_lock = threading.RLock()
automation_order_log_lock = threading.RLock()
automation_order_log: list = []
MAX_AUTOMATION_ORDER_LOG_ENTRIES = 800

# ──────────────────────────────────────────────
# Analytics log
# ──────────────────────────────────────────────
_analytics_lock = threading.Lock()
_analytics_log: list = []
_bot_startup_time = datetime.now()  # sinais anteriores a este instante nao contam no limite/hora
MAX_ANALYTICS_ENTRIES = 500


def _expire_stale_analytics():
    """
    Marca como 'expired' entradas pendentes com mais de 1h (rodada encerrada com certeza).
    Chamado no startup e periodicamente para limpar lixo de sessões anteriores.
    """
    cutoff = datetime.now() - timedelta(hours=1)
    changed = False
    with _analytics_lock:
        for entry in _analytics_log:
            if entry.get("outcome") is None and not entry.get("blocked"):
                try:
                    ts = datetime.fromisoformat(entry["ts"])
                    if ts < cutoff:
                        entry["outcome"] = "expired"
                        changed = True
                except Exception:
                    pass
        if changed:
            _save_analytics_unsafe()


def _expire_stale_pending():
    """Alias chamado dentro de _load_analytics (antes do lock existir)."""
    _expire_stale_analytics()


def _load_analytics():
    global _analytics_log
    try:
        data = _load_private_json("signal_analytics.json", [])
        if isinstance(data, list):
            _analytics_log = data[-MAX_ANALYTICS_ENTRIES:]
        else:
            _analytics_log = []
    except Exception:
        _analytics_log = []
    # Expira pendentes de sessões antigas (sinal enviado há >1h = rodada já terminou)
    _expire_stale_pending()


def _save_analytics_unsafe():
    try:
        _save_private_json("signal_analytics.json", _analytics_log)
    except Exception:
        pass


def _analytics_is_real_bet(entry: dict | None) -> bool:
    """Retorna True quando a entrada representa uma aposta realmente enviada."""
    if not isinstance(entry, dict):
        return False
    return bool(entry.get("bet_placed"))


def _load_automation_order_log():
    global automation_order_log
    try:
        data = _load_private_json("automation_orders_log.json", [])
        automation_order_log = data[-MAX_AUTOMATION_ORDER_LOG_ENTRIES:] if isinstance(data, list) else []
    except Exception:
        automation_order_log = []


def _load_automation_order_log_entries() -> list[dict]:
    try:
        data = _load_private_json("automation_orders_log.json", [])
        return data[-MAX_AUTOMATION_ORDER_LOG_ENTRIES:] if isinstance(data, list) else []
    except Exception:
        return []


def _save_automation_order_log_unsafe():
    try:
        _save_private_json("automation_orders_log.json", automation_order_log)
    except Exception:
        pass


def _format_automation_order_log_line(entry: dict) -> str:
    ts = str(entry.get("ts") or "").replace("T", " ")
    market = str(entry.get("market") or "--").upper()
    market_id = str(entry.get("market_id") or "--")
    status = str(entry.get("status") or "--").upper()
    rec = str(entry.get("rec") or "--")
    kind = str(entry.get("kind") or "--")
    amount = entry.get("amount")
    currency = str(entry.get("currency") or "BRLX")
    odd = str(entry.get("odd") or "--")
    outcome = str(entry.get("outcome") or "--").upper()
    api_status = entry.get("api_status")
    conf = entry.get("conf_pct")
    amount_txt = f"{float(amount):.2f} {currency}" if _as_float(amount) is not None else f"-- {currency}"
    api_txt = f"api={api_status}" if api_status is not None else "api=--"
    conf_txt = f"conf={int(conf)}%" if conf is not None else "conf=--"
    detail = str(entry.get("detail") or entry.get("execution") or "").strip()
    detail_txt = f" | {detail}" if detail else ""
    return (
        f"[{ts}] {market}#{market_id} {status} {kind} {rec} "
        f"amount={amount_txt} odd={odd} {conf_txt} {api_txt} outcome={outcome}{detail_txt}"
    )


def _write_automation_order_log_txt_unsafe():
    try:
        lines = [_format_automation_order_log_line(item) for item in automation_order_log[-MAX_AUTOMATION_ORDER_LOG_ENTRIES:]]
        text = "\n".join(lines)
        if text:
            text += "\n"
        with open(_private_data_path("automation_orders_log.txt"), "w", encoding="utf-8") as f:
            f.write(text)
    except Exception:
        pass


def _automation_order_log_public(mk: str | None = None, *, limit: int = 8) -> list[dict]:
    if _has_session_license_context():
        raw_entries = _load_automation_order_log_entries()
        entries = [
            dict(item) for item in raw_entries
            if not mk or item.get("market") == mk
        ]
    else:
        with automation_order_log_lock:
            entries = [
                dict(item) for item in automation_order_log
                if not mk or item.get("market") == mk
            ]
    return list(reversed(entries[-max(1, limit):]))


def _automation_wagered_summary(mk: str | None = None) -> dict:
    if _has_session_license_context():
        entries = _load_automation_order_log_entries()
    else:
        with automation_order_log_lock:
            entries = list(automation_order_log)

    total_amount = 0.0
    total_orders = 0
    for entry in entries:
        if mk and entry.get("market") != mk:
            continue
        status = str(entry.get("status") or "").lower()
        if status not in {"sent", "pending", "resolved"}:
            continue
        amount = _as_float(entry.get("amount"))
        if amount is None or amount <= 0:
            continue
        total_amount += amount
        total_orders += 1

    return {
        "amount": round(total_amount, 2),
        "orders": total_orders,
    }


def _automation_order_log_add(
    mk: str,
    *,
    market_id,
    status: str,
    rec: str | None = None,
    kind: str | None = None,
    amount=None,
    currency: str | None = None,
    odd: str | None = None,
    conf_pct: int | None = None,
    detail: str | None = None,
    api_status=None,
    execution: str | None = None,
    accepted: bool | None = None,
    confirmed: bool | None = None,
) -> str:
    now = datetime.now()
    amount_num = _as_float(amount)
    log_id = f"{now.strftime('%Y%m%d%H%M%S%f')}-{mk}-{market_id or 'na'}-{status.lower()}"
    entry = {
        "id": log_id,
        "ts": now.strftime("%Y-%m-%dT%H:%M:%S"),
        "date": now.strftime("%Y-%m-%d"),
        "time": now.strftime("%H:%M:%S"),
        "market": mk,
        "market_name": MARKET_CONFIGS.get(mk, {}).get("name") or mk,
        "market_id": str(market_id or ""),
        "status": str(status or "").lower() or "info",
        "rec": str(rec or "").upper() or None,
        "kind": str(kind or "").strip() or None,
        "amount": round(amount_num, 2) if amount_num is not None else None,
        "currency": str(currency or "BRLX").upper(),
        "odd": str(odd or "").strip() or None,
        "conf_pct": int(conf_pct) if conf_pct is not None else None,
        "detail": str(detail or "").strip() or None,
        "api_status": api_status,
        "execution": str(execution or "").strip() or None,
        "accepted_by_api": accepted,
        "confirmed_position": confirmed,
        "outcome": None,
        "resolved_at": None,
    }
    with automation_order_log_lock:
        automation_order_log.append(entry)
        if len(automation_order_log) > MAX_AUTOMATION_ORDER_LOG_ENTRIES:
            automation_order_log[:] = automation_order_log[-MAX_AUTOMATION_ORDER_LOG_ENTRIES:]
        _save_automation_order_log_unsafe()
        _write_automation_order_log_txt_unsafe()
    return log_id


def _automation_order_log_update_latest(
    mk: str,
    market_id,
    *,
    rec: str | None = None,
    statuses: tuple[str, ...] | None = None,
    **updates,
) -> bool:
    mid = str(market_id or "")
    wanted_rec = str(rec or "").upper() or None
    with automation_order_log_lock:
        for entry in reversed(automation_order_log):
            if entry.get("market") != mk or entry.get("market_id") != mid:
                continue
            if statuses and str(entry.get("status") or "").lower() not in statuses:
                continue
            if wanted_rec and str(entry.get("rec") or "").upper() != wanted_rec:
                continue
            for key, value in updates.items():
                entry[key] = value
            _save_automation_order_log_unsafe()
            _write_automation_order_log_txt_unsafe()
            return True
    return False


def _automation_order_log_find_latest(
    mk: str,
    market_id,
    *,
    rec: str | None = None,
    statuses: tuple[str, ...] | None = None,
) -> dict | None:
    mid = str(market_id or "")
    wanted_rec = str(rec or "").upper() or None
    with automation_order_log_lock:
        for entry in reversed(automation_order_log):
            if entry.get("market") != mk or entry.get("market_id") != mid:
                continue
            if statuses and str(entry.get("status") or "").lower() not in statuses:
                continue
            if wanted_rec and str(entry.get("rec") or "").upper() != wanted_rec:
                continue
            return dict(entry)
    return None


def _automation_order_log_resolve(mk: str, market_id, outcome: str, *, rec: str | None = None):
    resolved_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if _automation_order_log_update_latest(
        mk,
        market_id,
        rec=rec,
        statuses=("sent", "pending"),
        outcome=outcome,
        resolved_at=resolved_at,
    ):
        return
    _automation_order_log_add(
        mk,
        market_id=market_id,
        status="resolved",
        rec=rec,
        detail="Resultado conciliado sem log previo de envio.",
        confirmed=True,
    )
    _automation_order_log_update_latest(
        mk,
        market_id,
        rec=rec,
        statuses=("resolved",),
        outcome=outcome,
        resolved_at=resolved_at,
    )
    _broadcast_automation(mk)


def _automation_has_confirmed_order(mk: str, market_id, *, rec: str | None = None) -> bool:
    mid = str(market_id or "")
    wanted_rec = str(rec or "").upper() or None
    with automation_order_log_lock:
        for entry in reversed(automation_order_log):
            if entry.get("market") != mk or entry.get("market_id") != mid:
                continue
            if wanted_rec and str(entry.get("rec") or "").upper() != wanted_rec:
                continue
            status = str(entry.get("status") or "").lower()
            if status not in {"sent", "pending", "resolved"}:
                continue
            if bool(entry.get("confirmed_position")) or bool(entry.get("confirmed")):
                return True
    return False


def _analytics_add_entry(
    mk: str,
    market_id,
    rec: str,
    kind: str,
    conf: int,
    odd,
    strategy,
    entry_odd: str | None = None,
):
    with _analytics_lock:
        entry = {
            "ts": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "date": datetime.now().strftime("%Y-%m-%d"),
            "time": datetime.now().strftime("%H:%M:%S"),
            "market": mk,
            "market_name": MARKET_CONFIGS[mk]["name"],
            "market_id": str(market_id or ""),
            "rec": rec,
            "kind": kind,
            "conf": int(conf or 0),
            "odd": str(odd or ""),
            "entry_odd": str(entry_odd or odd or ""),
            "strategy": str(strategy or ""),
            "outcome": None,
            "blocked": False,
            "blocked_reason": None,
            "bet_placed": False,
        }
        _analytics_log.append(entry)
        if len(_analytics_log) > MAX_ANALYTICS_ENTRIES:
            _analytics_log[:] = _analytics_log[-MAX_ANALYTICS_ENTRIES:]
        _save_analytics_unsafe()


def _analytics_add_blocked(
    mk: str,
    market_id,
    rec: str,
    kind: str,
    conf: int,
    odd,
    strategy,
    reason: str,
):
    """Registra no analytics uma entrada que foi bloqueada antes de ser enviada."""
    with _analytics_lock:
        entry = {
            "ts": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "date": datetime.now().strftime("%Y-%m-%d"),
            "time": datetime.now().strftime("%H:%M:%S"),
            "market": mk,
            "market_name": MARKET_CONFIGS[mk]["name"],
            "market_id": str(market_id or ""),
            "rec": rec,
            "kind": kind,
            "conf": int(conf or 0),
            "odd": str(odd or ""),
            "entry_odd": "",
            "strategy": str(strategy or ""),
            "outcome": "blocked",
            "blocked": True,
            "blocked_reason": reason,
        }
        _analytics_log.append(entry)
        if len(_analytics_log) > MAX_ANALYTICS_ENTRIES:
            _analytics_log[:] = _analytics_log[-MAX_ANALYTICS_ENTRIES:]
        _save_analytics_unsafe()


def _analytics_mark_bet_placed(mk: str, market_id):
    """Marca bet_placed=True na entrada mais recente do mercado (ordem confirmada)."""
    mid = str(market_id or "")
    with _analytics_lock:
        for entry in reversed(_analytics_log):
            if (
                entry.get("market") == mk
                and entry.get("market_id") == mid
                and not entry.get("blocked")
                and not entry.get("bet_placed")
            ):
                entry["bet_placed"] = True
                _save_analytics_unsafe()
                return


def _analytics_resolve(mk: str, market_id, outcome: str, entry_odd: str | None = None):
    mid = str(market_id or "")
    with _analytics_lock:
        for entry in reversed(_analytics_log):
            if (
                entry.get("market") == mk
                and entry.get("market_id") == mid
                and entry.get("outcome") is None
            ):
                entry["outcome"] = outcome
                if entry_odd:
                    entry["entry_odd"] = str(entry_odd)
                _save_analytics_unsafe()
                return


_load_analytics()
_load_automation_order_log()

# Rastreia timestamp de fim de pausa por mercado: {mk: unix_timestamp}
_consecutive_loss_pause: dict = {}


def _consecutive_losses_for_market(mk: str) -> int:
    """Conta quantas derrotas consecutivas o mercado tem no analytics (mais recentes primeiro)."""
    count = 0
    with _analytics_lock:
        entries = [
            e for e in reversed(_analytics_log)
            if e.get("market") == mk
            and _analytics_is_real_bet(e)
            and e.get("outcome") in ("win", "loss")
        ]
    for e in entries:
        if e["outcome"] == "loss":
            count += 1
        else:
            break
    return count


def _is_consecutive_loss_paused(mk: str) -> bool:
    """Retorna True se o mercado esta em pausa por perda."""
    if not CONSECUTIVE_LOSS_STOP_ENABLED:
        return False
    resume_at = _consecutive_loss_pause.get(mk, 0)
    return time.time() < resume_at


def _set_consecutive_loss_pause(mk: str):
    """Ativa pausa de CONSECUTIVE_LOSS_PAUSE_SECONDS a partir de agora."""
    _consecutive_loss_pause[mk] = time.time() + CONSECUTIVE_LOSS_PAUSE_SECONDS


def _tick_consecutive_loss_pause(mk: str):
    """Legado: nao faz nada, pausa agora e por tempo."""
    pass


def _daily_balance_drop_pct(mk: str) -> float | None:
    """Calcula a queda percentual do saldo desde o inicio do dia atual."""
    if not DAILY_BALANCE_STOP_ENABLED:
        return None
    today = _balance_timeline_today()
    with balance_timeline_lock:
        _ensure_balance_timeline_today_locked(today)
        points = [
            p for p in (balance_timeline_data.get("points") or [])
            if isinstance(p, dict) and str(p.get("ts") or "").startswith(today)
        ]
    if not points:
        return None
    first_balance = _as_float(points[0].get("balance"))
    last_balance = _as_float(points[-1].get("balance"))
    if not first_balance or first_balance <= 0 or last_balance is None:
        return None
    drop = (first_balance - last_balance) / first_balance * 100
    return round(drop, 2)


def _is_daily_balance_stopped(mk: str) -> bool:
    """Retorna True se o saldo do dia caiu mais que o limite configurado."""
    if not DAILY_BALANCE_STOP_ENABLED:
        return False
    drop = _daily_balance_drop_pct(mk)
    return drop is not None and drop >= DAILY_BALANCE_STOP_PCT


def _default_automation_meta() -> dict:
    return {
        "running": False,
        "active_profile": "a",
        "execution_profile_mode": "a",
        "market_selection_mode": "manual",
        "last_running_notification_state": None,
        "last_running_notification_ts": 0.0,
    }


def _normalize_automation_profile_key(value) -> str:
    text = str(value or "").strip().lower()
    if text in {"b", "profile_b"}:
        return "b"
    return "a"


def _normalize_execution_profile_mode(value, *, fallback: str = "a") -> str:
    text = str(value or "").strip().lower()
    if text in {"both", "a+b", "ab"}:
        return "both"
    if text in {"b", "profile_b"}:
        return "b"
    if text in {"a", "active", "profile_a", "erro"}:
        return "a"
    return _normalize_execution_profile_mode(fallback) if fallback != "a" else "a"


def _execution_profile_label(mode: str) -> str:
    normalized = _normalize_execution_profile_mode(mode)
    if normalized == "both":
        return "Perfil A+B"
    if normalized == "b":
        return "Perfil B"
    return "Perfil A"


def _normalize_market_selection_mode(value) -> str:
    text = str(value or "").strip().lower()
    if text in {"semaforo", "auto", "automatico", "automatic"}:
        return "semaforo"
    return "manual"


def _automation_uses_semaforo(meta_map: dict | None = None) -> bool:
    if isinstance(meta_map, dict):
        source_meta = meta_map
    elif _has_session_license_context():
        source_meta = _load_automation_meta(_load_automation_raw())
    else:
        source_meta = automation_meta
    return _normalize_market_selection_mode(source_meta.get("market_selection_mode")) == "semaforo"


def _mask_secret(value: str | None) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if len(text) <= 8:
        return "*" * len(text)
    return f"{text[:4]}...{text[-4:]}"


def _default_telegram_cfg() -> dict:
    use_env_defaults = not _has_session_license_context()
    default_token = TELEGRAM_TOKEN if use_env_defaults else ""
    default_chat_id = TELEGRAM_CHAT_ID if use_env_defaults else ""
    return {
        "enabled": bool(default_token and default_chat_id),
        "bot_token": default_token,
        "chat_id": default_chat_id,
        "markets": {
            mk: {"signal_min_confidence": 57.0}
            for mk in MARKET_CONFIGS
        },
        "last_status": "idle",
        "last_message": "Telegram opcional. Configure se quiser enviar sinais para grupo/canal.",
        "last_sent_at": None,
        "last_market": None,
    }


def _balance_timeline_today() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _balance_point_day(point: dict) -> str:
    return str((point or {}).get("ts") or "")[:10]


def _ensure_balance_timeline_today_locked(today: str | None = None) -> bool:
    today = today or _balance_timeline_today()
    current_date = str(balance_timeline_data.get("date") or "").strip()
    if current_date == today:
        return False
    balance_timeline_data["date"] = today
    return True


def _load_balance_timeline() -> dict:
    today = _balance_timeline_today()
    payload = {"date": today, "currency": "BRLX", "points": []}
    try:
        raw = _load_private_json("saldo_timeline.json", None)
    except Exception:
        return payload
    if raw is None:
        return payload

    points: list[dict] = []
    currency = "BRLX"

    if isinstance(raw, list):
        for item in raw:
            if not isinstance(item, dict):
                continue
            ts = str(item.get("ts") or "").strip()
            balance = _as_float(item.get("balance"))
            item_currency = str(item.get("currency") or "").strip().upper() or "BRLX"
            if not ts or balance is None:
                continue
            points.append({
                "ts": ts,
                "balance": round(balance, 2),
                "currency": item_currency,
            })
        points.sort(key=lambda item: str(item.get("ts") or ""))
        if points:
            currency = str(points[-1].get("currency") or "BRLX").upper()
        return {"date": today, "currency": currency, "points": points}

    if isinstance(raw, dict):
        currency = str(raw.get("currency") or "BRLX").upper()
        for item in raw.get("points") or []:
            if not isinstance(item, dict):
                continue
            ts = str(item.get("ts") or "").strip()
            balance = _as_float(item.get("balance"))
            item_currency = str(item.get("currency") or currency).strip().upper() or currency
            if not ts or balance is None:
                continue
            points.append({
                "ts": ts,
                "balance": round(balance, 2),
                "currency": item_currency,
            })
        points.sort(key=lambda item: str(item.get("ts") or ""))
        if points:
            currency = str(points[-1].get("currency") or currency).upper()
        return {"date": today, "currency": currency, "points": points}
    return payload


def _compress_timeline_points(points: list[dict], *, recent_seconds: int = 24 * 60 * 60) -> list[dict]:
    if len(points) <= 360:
        return list(points)
    now_ts = time.time()
    recent_cutoff = now_ts - recent_seconds
    older_points = []
    recent_points = []
    for point in points:
        try:
            point_ts = datetime.fromisoformat(str(point.get("ts") or "")).timestamp()
        except Exception:
            point_ts = None
        if point_ts is not None and point_ts >= recent_cutoff:
            recent_points.append(point)
        else:
            older_points.append(point)
    compressed = _sample_evenly_points(older_points, 180) + recent_points
    if len(compressed) > 420:
        compressed = _sample_evenly_points(compressed, 420)
    return compressed


def _save_balance_timeline():
    with balance_timeline_lock:
        _save_private_json(
            "saldo_timeline.json",
            {
                "date": balance_timeline_data.get("date") or _balance_timeline_today(),
                "currency": balance_timeline_data.get("currency") or "BRLX",
                "points": balance_timeline_data.get("points") or [],
            },
        )


def _sample_evenly_points(points: list[dict], max_points: int) -> list[dict]:
    if max_points <= 0:
        return []
    if len(points) <= max_points:
        return list(points)
    if max_points == 1:
        return [points[-1]]

    sampled = []
    last_idx = len(points) - 1
    seen = set()
    for i in range(max_points):
        idx = round(i * last_idx / (max_points - 1))
        if idx in seen:
            continue
        seen.add(idx)
        sampled.append(points[idx])
    return sampled


def _balance_timeline_public_payload(currency_hint: str | None = None) -> dict:
    if _has_session_license_context():
        payload = _load_balance_timeline()
        points = [
            {
                "ts": str(item.get("ts") or ""),
                "balance": _as_float(item.get("balance")),
            }
            for item in (payload.get("points") or [])
            if isinstance(item, dict)
            and str(item.get("ts") or "").strip()
            and _as_float(item.get("balance")) is not None
        ]
        currency = str(
            currency_hint
            or payload.get("currency")
            or "BRLX"
        ).upper()
        points = _compress_timeline_points(points)
        return {"currency": currency, "points": points}

    with balance_timeline_lock:
        changed = _ensure_balance_timeline_today_locked()
        points = [
            {
                "ts": str(item.get("ts") or ""),
                "balance": _as_float(item.get("balance")),
            }
            for item in (balance_timeline_data.get("points") or [])
            if isinstance(item, dict)
            and str(item.get("ts") or "").strip()
            and _as_float(item.get("balance")) is not None
        ]
        currency = str(
            currency_hint
            or balance_timeline_data.get("currency")
            or "BRLX"
        ).upper()
        if changed:
            _save_balance_timeline()
    points = _compress_timeline_points(points)

    return {"currency": currency, "points": points}


def _automation_volume_timeline_public(mk: str, currency_hint: str | None = None) -> dict:
    if _has_session_license_context():
        raw_entries = _load_automation_order_log_entries()
    else:
        with automation_order_log_lock:
            raw_entries = list(automation_order_log)

    points: list[dict] = []
    currency = str(currency_hint or "BRLX").upper()
    for entry in raw_entries:
        if str(entry.get("market") or "") != str(mk or ""):
            continue
        status = str(entry.get("status") or "").lower()
        if status not in {"sent", "pending", "resolved"}:
            continue
        ts = str(entry.get("ts") or "").strip()
        amount = _as_float(entry.get("amount"))
        if not ts or amount is None or amount <= 0:
            continue
        currency = str(entry.get("currency") or currency or "BRLX").upper()
        points.append({
            "ts": ts,
            "amount": round(amount, 2),
        })

    points.sort(key=lambda item: str(item.get("ts") or ""))
    return {"currency": currency, "points": points}


def _record_balance_snapshot(balance: float | None, currency: str | None = None):
    amount = _as_float(balance)
    if amount is None:
        return
    now = datetime.now()
    now_iso = now.strftime("%Y-%m-%dT%H:%M:%S")
    chosen_currency = str(currency or "BRLX").upper()
    with balance_timeline_lock:
        _ensure_balance_timeline_today_locked(now.strftime("%Y-%m-%d"))
        points = balance_timeline_data.setdefault("points", [])
        last = points[-1] if points else None
        if isinstance(last, dict):
            last_balance = _as_float(last.get("balance"))
            last_ts = str(last.get("ts") or "")
            try:
                last_dt = datetime.fromisoformat(last_ts)
            except Exception:
                last_dt = None
            if (
                last_balance is not None
                and abs(last_balance - amount) < 0.0001
                and last_dt is not None
                and (now - last_dt).total_seconds() < 30
            ):
                balance_timeline_data["currency"] = chosen_currency
                return
        points.append({
            "ts": now_iso,
            "balance": round(amount, 2),
            "currency": chosen_currency,
        })
        if len(points) > 5000:
            del points[:-5000]
        balance_timeline_data["currency"] = chosen_currency
        _save_balance_timeline()


def _load_telegram_config() -> dict:
    cfg = _default_telegram_cfg()
    try:
        raw = _load_private_json("telegram_config.json", {})
    except Exception:
        return cfg

    if not isinstance(raw, dict):
        return cfg

    cfg["enabled"] = bool(raw.get("enabled", cfg["enabled"]))
    cfg["bot_token"] = str(raw.get("bot_token") or cfg["bot_token"]).strip()
    cfg["chat_id"] = str(raw.get("chat_id") or cfg["chat_id"]).strip()
    cfg["last_status"] = str(raw.get("last_status") or cfg["last_status"])
    cfg["last_message"] = str(raw.get("last_message") or cfg["last_message"])
    cfg["last_sent_at"] = raw.get("last_sent_at")
    cfg["last_market"] = raw.get("last_market")

    raw_markets = raw.get("markets")
    if isinstance(raw_markets, dict):
        for mk in MARKET_CONFIGS:
            entry = raw_markets.get(mk)
            if not isinstance(entry, dict):
                continue
            cfg["markets"][mk]["signal_min_confidence"] = max(
                50.0,
                min(95.0, _as_float(entry.get("signal_min_confidence")) or 57.0),
            )
    return cfg


def _save_telegram_config(cfg: dict | None = None):
    with telegram_lock:
        _save_private_json("telegram_config.json", cfg if cfg is not None else telegram_cfg)


def _save_pending_signal_cache():
    with _pending_signal_lock:
        _save_private_json("ultimo_sinal_telegram.json", _pending_signal_payload())


def _load_pending_signal_cache():
    try:
        raw = _load_private_json("ultimo_sinal_telegram.json", {})
    except Exception:
        return
    if not isinstance(raw, dict):
        return

    for mk in MARKET_CONFIGS:
        entry = raw.get(mk)
        if not isinstance(entry, dict):
            continue
        market_id = str(entry.get("market_id") or "").strip()
        rec = str(entry.get("rec") or "").strip()
        if not market_id or not rec:
            continue
        _runtime_bucket(_sent_signal_mid)[mk] = market_id
        _runtime_bucket(_sent_signal_rec)[mk] = rec
        _runtime_bucket(_sent_signal_kind)[mk] = str(entry.get("kind") or "").strip() or "SINAL"
        try:
            _runtime_bucket(_sent_signal_conf)[mk] = int(round(float(entry.get("conf") or 0)))
        except Exception:
            _runtime_bucket(_sent_signal_conf)[mk] = 0
        _runtime_bucket(_sent_signal_odd)[mk] = str(entry.get("odd") or "").strip() or None
        _runtime_bucket(_sent_signal_strategy)[mk] = _compact_strategy_label(entry.get("strategy"))


def _market_signal_min_confidence(mk: str) -> float:
    if _has_session_license_context():
        cfg = _load_telegram_config()
        market_cfg = (cfg.get("markets") or {}).get(mk) or {}
        signal_min = _as_float(market_cfg.get("signal_min_confidence"))
    else:
        with telegram_lock:
            market_cfg = (telegram_cfg.get("markets") or {}).get(mk) or {}
            signal_min = _as_float(market_cfg.get("signal_min_confidence"))
    return max(50.0, min(95.0, signal_min or 57.0))


def _automation_count_range_start(value) -> int | None:
    num = _as_float(value)
    if num is None:
        return None
    if num < 0:
        return 0
    return int(num // 100) * 100


def _automation_operational_gate(mk: str, rec: str, conf_pct: int, cfg: dict | None = None) -> dict:
    cfg = dict(cfg or {})
    now_dt = datetime.now()
    now_hour = now_dt.hour
    today = now_dt.strftime("%Y-%m-%d")
    current_meta = _as_float((states.get(mk) or {}).get("value_needed"))

    with balance_timeline_lock:
        raw_points = list((balance_timeline_data or {}).get("points") or [])
    points = []
    for item in raw_points:
        try:
            ts = datetime.fromisoformat(str(item.get("ts") or ""))
            balance = _as_float(item.get("balance"))
            if balance is None:
                continue
            points.append({"dt": ts, "balance": float(balance)})
        except Exception:
            continue
    points.sort(key=lambda item: item["dt"])

    current_day_delta = 0.0
    by_hour_delta = {hour: 0.0 for hour in range(24)}
    by_hour_moves = {hour: 0 for hour in range(24)}
    if len(points) >= 2:
        for idx in range(1, len(points)):
            prev = points[idx - 1]
            cur = points[idx]
            delta = float(cur["balance"]) - float(prev["balance"])
            if cur["dt"].strftime("%Y-%m-%d") != today:
                continue
            current_day_delta += delta
            hour = cur["dt"].hour
            by_hour_delta[hour] = by_hour_delta.get(hour, 0.0) + delta
            by_hour_moves[hour] = by_hour_moves.get(hour, 0) + 1

    populated_hours = [hour for hour, moves in by_hour_moves.items() if moves > 0]
    best_hour = None
    if populated_hours:
        best_hour = max(populated_hours, key=lambda hour: by_hour_delta.get(hour, 0.0))
    current_hour_delta = by_hour_delta.get(now_hour, 0.0)
    current_hour_moves = by_hour_moves.get(now_hour, 0)

    with _analytics_lock:
        analytics_entries = list(_analytics_log)
    resolved_hour_entries = []
    for entry in analytics_entries:
        if entry.get("market") != mk:
            continue
        if entry.get("blocked"):
            continue
        if entry.get("outcome") not in ("win", "loss"):
            continue
        if str(entry.get("date") or "") != today:
            continue
        time_str = str(entry.get("time") or "")
        try:
            hour = int(time_str[:2])
        except Exception:
            continue
        if hour == now_hour:
            resolved_hour_entries.append(entry)
    hour_wins = sum(1 for entry in resolved_hour_entries if entry.get("outcome") == "win")
    hour_total = len(resolved_hour_entries)
    hour_accuracy = int(round((hour_wins / hour_total) * 100)) if hour_total else 0
    top_strategy = "--"
    if hour_total:
        strategy_counts: dict[str, int] = {}
        for entry in resolved_hour_entries:
            strategy_name = str(entry.get("strategy") or "").strip() or "--"
            strategy_counts[strategy_name] = strategy_counts.get(strategy_name, 0) + 1
        top_strategy = max(strategy_counts.items(), key=lambda item: item[1])[0]

    range_target = None
    if current_meta is not None:
        entries = []
        for entry in list(histories.get(mk) or []):
            count_num = _as_float(entry.get("count"))
            meta_num = _as_float(entry.get("value_needed"))
            if count_num is None:
                continue
            entries.append({
                "count": count_num,
                "meta": meta_num,
                "result": str(entry.get("result") or "").upper(),
            })
        range_buckets: dict[tuple[int, int], dict] = {}
        for entry in entries:
            start = _automation_count_range_start(entry["count"])
            if start is None:
                continue
            bucket_key = (start, start + 100)
            bucket = range_buckets.setdefault(bucket_key, {
                "start": start,
                "end": start + 100,
                "total": 0,
                "beat": 0,
                "fail": 0,
                "near_miss": 0,
            })
            bucket["total"] += 1
            if entry["result"] == "MAIS":
                bucket["beat"] += 1
            elif entry["result"] == "ATE":
                bucket["fail"] += 1
            if entry["meta"] is not None and abs(entry["count"] - entry["meta"]) <= 15 and entry["count"] < entry["meta"]:
                bucket["near_miss"] += 1
        target_start = _automation_count_range_start(current_meta)
        if target_start is not None:
            range_target = range_buckets.get((target_start, target_start + 100))

    trend_score = 1 if current_day_delta > 0 else (-1 if current_day_delta < 0 else 0)
    hour_flow_score = 1 if current_hour_moves > 0 and current_hour_delta > 0 else (-1 if current_hour_moves > 0 and current_hour_delta < 0 else 0)
    strategy_score = 2 if hour_total >= 3 and hour_accuracy >= 60 else (1 if hour_total >= 3 and hour_accuracy >= 52 else (-1 if hour_total >= 3 else 0))
    best_hour_score = 1 if best_hour is not None and best_hour == now_hour else (-1 if best_hour is not None else 0)
    range_score = 0
    if range_target and int(range_target.get("total") or 0) >= 3:
        if int(range_target.get("beat") or 0) > int(range_target.get("fail") or 0):
            range_score = 1
        elif int(range_target.get("fail") or 0) > int(range_target.get("beat") or 0):
            range_score = -1
    score = trend_score + hour_flow_score + strategy_score + best_hour_score + range_score

    tone = "good" if score >= 3 else ("bad" if score <= -1 else "warn")
    label = "ligar_agora" if tone == "good" else ("melhor_esperar" if tone == "bad" else "operar_leve")
    warn_conf_min = max(
        int(round(_market_signal_min_confidence(mk))),
        int(round(_as_float(cfg.get("safe_min_confidence")) or 70.0)),
    )
    blocked = False
    reason = ""
    if tone == "bad":
        blocked = True
        reason = f"semaforo:red score={score} dia={current_day_delta:.2f} hora={current_hour_delta:.2f} acc={hour_accuracy}%"
        if range_target:
            reason += f" faixa={range_target['start']}-{range_target['end']}"
    elif tone == "warn" and conf_pct < warn_conf_min:
        blocked = True
        reason = (
            f"semaforo:yellow score={score} conf={conf_pct}% abaixo do minimo "
            f"operacional {warn_conf_min}%"
        )

    reasons = [
        f"dia {current_day_delta:+.2f}",
        f"hora {now_hour:02d}h {current_hour_delta:+.2f}",
        f"acc hora {hour_accuracy}%",
    ]
    if top_strategy and top_strategy != "--":
        reasons.append(f"estrategia {top_strategy}")
    if range_target:
        reasons.append(
            f"faixa {range_target['start']}-{range_target['end']} "
            f"bateu {range_target['beat']}x falhou {range_target['fail']}x"
        )

    return {
        "blocked": blocked,
        "tone": tone,
        "label": label,
        "score": score,
        "reason": reason,
        "summary": " | ".join(reasons),
        "warn_conf_min": warn_conf_min,
        "hour_accuracy": hour_accuracy,
    }


def _automation_dual_market_plan(cfg_map: dict | None = None) -> dict:
    cfg_map = cfg_map if isinstance(cfg_map, dict) else automation_cfg
    lane_keys = [mk for mk in ("rodovia", "rua") if mk in MARKET_CONFIGS]
    lanes: dict[str, dict] = {}
    for lane_key in lane_keys:
        state = states.get(lane_key) or {}
        available = bool(
            state.get("connected")
            and state.get("market_id")
            and str(state.get("status") or "").lower() not in {"erro", "reconectando"}
        )
        lane_cfg = dict(cfg_map.get(lane_key) or _default_automation_cfg())
        gate = _automation_operational_gate(lane_key, "MAIS", 100, lane_cfg) if available else {
            "tone": "offline",
            "score": -99,
            "summary": "Mercado fechado ou sem conexao no momento.",
            "label": "fechado",
        }
        lanes[lane_key] = {
            "market": lane_key,
            "label": MARKET_CONFIGS.get(lane_key, {}).get("name") or lane_key.capitalize(),
            "tone": gate.get("tone") or "warn",
            "score": int(gate.get("score") or 0),
            "summary": str(gate.get("summary") or ""),
            "status": str(gate.get("label") or ""),
            "enabled": bool(lane_cfg.get("enabled")),
            "available": available,
        }

    if not lanes:
        return {"mode": "none", "label": "Sem leitura", "summary": "Mercados comparaveis indisponiveis.", "lanes": {}}

    comparable_lanes = [lane for lane in lanes.values() if lane.get("available")]
    rodovia = lanes.get("rodovia")
    rua = lanes.get("rua")
    ordered = sorted(comparable_lanes, key=lambda item: (int(item.get("score") or 0), item.get("market") == "rodovia"), reverse=True)
    best = ordered[0] if ordered else None
    second = ordered[1] if len(ordered) > 1 else None

    mode = "none"
    label = "Melhor esperar"
    summary = "As duas leituras estao fracas neste momento."
    if not comparable_lanes:
        closed = [lane["label"] for lane in lanes.values() if not lane.get("available")]
        summary = " | ".join(f"{name} fechado/sem conexao" for name in closed) if closed else summary
    elif rodovia and rua and rodovia.get("available") and rua.get("available"):
        if rodovia["tone"] == "good" and rua["tone"] == "good":
            mode = "both"
            label = "Rodar os dois"
            summary = "Rodovia e Rua estao em janela favoravel ao mesmo tempo."
        elif best and best["tone"] == "good":
            mode = best["market"]
            label = f"Rodar so {best['label']}"
            summary = f"{best['label']} esta mais forte agora. {second['label']} ficou atras no semaforo." if second else f"{best['label']} esta mais forte agora."
        elif best and best["tone"] == "warn" and second and second["tone"] == "warn":
            mode = best["market"]
            label = f"Operar leve em {best['label']}"
            summary = f"As duas estao mornas, mas {best['label']} ficou um pouco melhor no horario atual."
        elif best and best["tone"] == "warn":
            mode = best["market"]
            label = f"Rodar so {best['label']}"
            summary = f"{best['label']} ainda tem leitura melhor que a outra, mas pede cautela."
    elif best:
        mode = best["market"]
        label = f"Rodar {best['label']}"
        offline_parts = [
            f"{lane['label']} fechado/sem conexao"
            for lane in lanes.values()
            if lane.get("market") != best.get("market") and not lane.get("available")
        ]
        summary = f"So {best['label']} esta disponivel para comparacao."
        if offline_parts:
            summary += " " + " | ".join(offline_parts)

    return {
        "mode": mode,
        "label": label,
        "summary": summary,
        "lanes": lanes,
    }


def _automation_effective_enabled_markets(
    cfg_map: dict | None = None,
    meta_map: dict | None = None,
) -> dict[str, bool]:
    cfg_map = cfg_map if isinstance(cfg_map, dict) else automation_cfg
    meta_map = meta_map if isinstance(meta_map, dict) else automation_meta
    selection_mode = _normalize_market_selection_mode(meta_map.get("market_selection_mode"))
    effective = {
        market_key: bool((cfg_map.get(market_key) or {}).get("enabled"))
        for market_key in MARKET_CONFIGS
    }
    if selection_mode != "semaforo":
        return effective

    for market_key in ("rodovia", "rua"):
        if market_key not in effective:
            continue
        state = states.get(market_key) or {}
        available = bool(
            state.get("connected")
            and state.get("market_id")
            and str(state.get("status") or "").lower() not in {"erro", "reconectando"}
        )
        if not available:
            effective[market_key] = False
            continue
        gate = _automation_operational_gate(
            market_key,
            "MAIS",
            100,
            dict(cfg_map.get(market_key) or _default_automation_cfg()),
        )
        effective[market_key] = str(gate.get("tone") or "").lower() in {"good", "warn"}
    return effective


def _telegram_operational_context(mk: str, conf_pct: int | None = None, rec: str | None = None) -> str:
    if not _automation_uses_semaforo():
        return ""
    gate = _automation_operational_gate(mk, rec or "MAIS", int(conf_pct or 0), automation_cfg.get(mk) or {})
    plan = _automation_dual_market_plan(automation_cfg)
    hour_snapshot = _telegram_hour_window_snapshot(mk, rec)
    tone_map = {
        "good": "🟢 Ligar agora",
        "warn": "🟡 Operar leve",
        "bad": "🔴 Melhor esperar",
    }
    mode_map = {
        "both": "Rodar os dois",
        "rodovia": "So Rodovia",
        "rua": "So Rua",
        "none": "Melhor esperar",
    }
    line1 = (
        f"🚦 Semaforo: <b>{tone_map.get(str(gate.get('tone') or ''), '⚪ Sem leitura')}</b> "
        f"(score {int(gate.get('score') or 0):+d})"
    )
    line2 = f"🧭 Mercados: <b>{mode_map.get(str(plan.get('mode') or ''), plan.get('label') or 'Sem leitura')}</b>"
    line3_parts = []
    closed_lanes = [
        str(lane.get("label") or lane.get("market") or "").strip()
        for lane in (plan.get("lanes") or {}).values()
        if not bool(lane.get("available"))
    ]
    if closed_lanes:
        line3_parts.append("fechado: " + ", ".join(closed_lanes))
    if int(hour_snapshot.get("total") or 0) > 0:
        line3_parts.append(
            f"janela {hour_snapshot['label']} com {int(hour_snapshot['total'])} rodadas"
        )
    hour_accuracy = gate.get("hour_accuracy")
    if hour_accuracy is not None:
        line3_parts.append(f"acc hora {int(hour_accuracy)}%")
    summary = " | ".join(line3_parts).strip()
    if summary:
        line3 = f"📎 Leitura: <b>{summary}</b>"
        return f"{line1}\n{line2}\n{line3}"
    return f"{line1}\n{line2}"


def _telegram_health_payload(mk: str) -> dict:
    if _has_session_license_context():
        cfg = dict(_load_telegram_config())
    else:
        with telegram_lock:
            cfg = dict(telegram_cfg)
    bot_token = str(cfg.get("bot_token") or "").strip()
    chat_id = str(cfg.get("chat_id") or "").strip()
    enabled = bool(cfg.get("enabled"))
    signal_min = _market_signal_min_confidence(mk)
    return {
        "enabled": enabled,
        "bot_token_configured": bool(bot_token),
        "chat_id_configured": bool(chat_id),
        "ready": bool(enabled and bot_token and chat_id),
        "market_name": MARKET_CONFIGS.get(mk, {}).get("name") or mk,
        "signal_min_confidence": signal_min,
    }


def _telegram_recent_signal_payload(mk: str) -> dict:
    with _analytics_lock:
        analytics_entries = [
            dict(entry) for entry in _analytics_log
            if str(entry.get("market") or "") == str(mk or "")
        ]
    latest_signal = analytics_entries[-1] if analytics_entries else None

    with automation_order_log_lock:
        order_entries = [
            dict(entry) for entry in automation_order_log
            if str(entry.get("market") or "") == str(mk or "")
        ]
    latest_order = order_entries[-1] if order_entries else None

    if not latest_signal and not latest_order:
        return {}

    payload = {
        "time": None,
        "rec": None,
        "kind": None,
        "conf_pct": None,
        "strategy": None,
        "status": None,
        "summary": None,
        "detail": None,
        "blocked_reason": None,
    }

    if latest_signal:
        payload.update({
            "time": latest_signal.get("ts") or latest_signal.get("time"),
            "rec": latest_signal.get("rec"),
            "kind": latest_signal.get("kind"),
            "conf_pct": latest_signal.get("conf"),
            "strategy": latest_signal.get("strategy"),
            "status": "bloqueado" if latest_signal.get("blocked") else "sinal",
            "blocked_reason": latest_signal.get("blocked_reason"),
        })

    if latest_order:
        order_status = str(latest_order.get("status") or "").lower()
        payload["time"] = latest_order.get("ts") or payload.get("time")
        payload["rec"] = latest_order.get("rec") or payload.get("rec")
        payload["kind"] = latest_order.get("kind") or payload.get("kind")
        payload["conf_pct"] = latest_order.get("conf_pct") if latest_order.get("conf_pct") is not None else payload.get("conf_pct")
        payload["status"] = order_status or payload.get("status")
        payload["detail"] = latest_order.get("detail") or latest_order.get("execution") or payload.get("detail")
        if order_status == "blocked" and not payload.get("blocked_reason"):
            payload["blocked_reason"] = latest_order.get("execution") or latest_order.get("detail")

    status_label_map = {
        "sent": "enviado",
        "pending": "pendente",
        "blocked": "bloqueado",
        "error": "erro",
        "sinal": "sinal gerado",
    }
    status_label = status_label_map.get(str(payload.get("status") or "").lower(), payload.get("status") or "--")
    rec_txt = str(payload.get("rec") or "--")
    conf_txt = f"{int(payload['conf_pct'])}%" if payload.get("conf_pct") is not None else "--"
    payload["summary"] = f"{status_label} | {rec_txt} | conf {conf_txt}"
    return payload


def _telegram_public_payload(mk: str) -> dict:
    if _has_session_license_context():
        cfg = dict(_load_telegram_config())
    else:
        with telegram_lock:
            cfg = dict(telegram_cfg)
    return {
        "enabled": bool(cfg.get("enabled")),
        "bot_token_masked": _mask_secret(cfg.get("bot_token")),
        "chat_id_masked": _mask_secret(cfg.get("chat_id")),
        "has_bot_token": bool(str(cfg.get("bot_token") or "").strip()),
        "has_chat_id": bool(str(cfg.get("chat_id") or "").strip()),
        "signal_min_confidence": _market_signal_min_confidence(mk),
        "last_status": cfg.get("last_status") or "idle",
        "last_message": cfg.get("last_message") or "Telegram pronto.",
        "last_sent_at": cfg.get("last_sent_at"),
        "last_market": cfg.get("last_market"),
        "setup_status": _telegram_health_payload(mk),
        "recent_signal": _telegram_recent_signal_payload(mk),
    }


def _broadcast_telegram(mk: str):
    broadcast(mk, "telegram_update", _telegram_public_payload(mk), license_key=_current_license_key())


def _broadcast_all_telegram():
    for market_key in MARKET_CONFIGS:
        _broadcast_telegram(market_key)


def _refresh_telegram_status_after_config(mk: str, *, base_message: str | None = None):
    """
    Ajusta last_status/last_message para bater com a config atual.
    Evita ficar com status READY quando o Telegram esta pausado.
    """
    if mk not in MARKET_CONFIGS:
        mk = next(iter(MARKET_CONFIGS.keys()))

    health = _telegram_health_payload(mk)
    enabled = bool(health.get("enabled"))
    bot_ok = bool(health.get("bot_token_configured"))
    chat_ok = bool(health.get("chat_id_configured"))

    if not enabled:
        status = "idle"
        suffix = "Telegram pausado."
    elif not (bot_ok and chat_ok):
        status = "error"
        missing = []
        if not bot_ok:
            missing.append("token")
        if not chat_ok:
            missing.append("chat ID")
        suffix = f"Telegram ligado, mas falta {(' e '.join(missing))}."
    else:
        status = "ready"
        suffix = "Telegram ativo e pronto."

    message = (str(base_message).strip() + " " + suffix) if base_message else suffix
    request_cfg = _load_telegram_config() if _has_session_license_context() else None
    with telegram_lock:
        target_cfg = request_cfg if request_cfg is not None else telegram_cfg
        target_cfg["last_status"] = status
        target_cfg["last_message"] = message
        target_cfg["last_market"] = mk
        try:
            _save_telegram_config(target_cfg)
        except Exception as ex:
            print(f"[Telegram] Erro ao salvar config/status: {ex}", flush=True)

    _broadcast_all_telegram()


def _set_telegram_status(
    status: str,
    message: str,
    *,
    market_key: str | None = None,
    sent: bool = False,
):
    request_cfg = _load_telegram_config() if _has_session_license_context() else None
    with telegram_lock:
        target_cfg = request_cfg if request_cfg is not None else telegram_cfg
        target_cfg["last_status"] = status
        target_cfg["last_message"] = message
        target_cfg["last_market"] = market_key
        if sent:
            target_cfg["last_sent_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            _save_telegram_config(target_cfg)
        except Exception as ex:
            print(f"[Telegram] Erro ao salvar config/status: {ex}", flush=True)
    _broadcast_all_telegram()


def _telegram_send_now(
    text: str,
    *,
    market_key: str | None = None,
    parse_mode: str | None = "HTML",
) -> tuple[bool, str | None]:
    request_cfg = _load_telegram_config() if _has_session_license_context() else None
    with telegram_lock:
        target_cfg = request_cfg if request_cfg is not None else telegram_cfg
        enabled = bool(target_cfg.get("enabled"))
        bot_token = str(target_cfg.get("bot_token") or "").strip()
        chat_id = str(target_cfg.get("chat_id") or "").strip()

    market_name = MARKET_CONFIGS.get(market_key or "", {}).get("name") or market_key or "geral"
    if not enabled:
        _set_telegram_status("idle", "Bot do Telegram pausado.", market_key=market_key)
        return False, "Bot do Telegram pausado."
    if not bot_token or not chat_id:
        _set_telegram_status(
            "error",
            "Falta salvar o token ou o chat ID do Telegram.",
            market_key=market_key,
        )
        return False, "Falta salvar o token ou o chat ID do Telegram."

    use_plain_fallback = False
    last_error = "Falha ao enviar mensagem para o Telegram."

    for attempt in range(TELEGRAM_SEND_MAX_RETRIES):
        effective_text = _strip_telegram_html(text) if use_plain_fallback else text
        effective_parse_mode = None if use_plain_fallback else parse_mode

        try:
            url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
            body = {
                "chat_id": chat_id,
                "text": effective_text,
            }
            if effective_parse_mode:
                body["parse_mode"] = effective_parse_mode
            req = urllib.request.Request(
                url,
                data=json.dumps(body).encode(),
                headers={"Content-Type": "application/json"},
            )

            with _telegram_delivery_lock:
                _telegram_wait_delivery_slot()
                try:
                    with urllib.request.urlopen(req, timeout=10):
                        pass
                finally:
                    _telegram_delivery_state["last_ts"] = time.time()

            _set_telegram_status(
                "sent",
                f"Mensagem enviada com sucesso para {market_name}.",
                market_key=market_key,
                sent=True,
            )
            return True, None
        except Exception as ex:
            error, http_code, retry_after, parse_error = _telegram_parse_error_details(ex)
            last_error = error

            if parse_error and not use_plain_fallback and effective_parse_mode:
                use_plain_fallback = True
                continue

            if attempt >= TELEGRAM_SEND_MAX_RETRIES - 1:
                break

            should_retry = False
            if http_code in TELEGRAM_RETRYABLE_HTTP_CODES:
                should_retry = True
            elif isinstance(ex, urllib.error.URLError):
                should_retry = True

            if not should_retry:
                break

            delay = retry_after if retry_after is not None else min(6.0, 1.5 * (attempt + 1))
            time.sleep(max(0.5, delay))

    _set_telegram_status("error", last_error, market_key=market_key)
    return False, last_error


def _load_automation_raw() -> dict:
    try:
        raw = _load_private_json("automatizado_config.json", {})
        return raw if isinstance(raw, dict) else {}
    except Exception:
        return {}


def _load_automation_raw_for_key(key: str) -> dict:
    try:
        raw = _client_db_get_json(_license_storage_slug_for(key), "automatizado_config.json", {})
        return raw if isinstance(raw, dict) else {}
    except Exception:
        return {}


def _iter_running_automation_license_keys(*, market_key: str | None = None) -> list[str]:
    store = _load_license_store()
    keys: list[str] = []
    seen: set[str] = set()
    for entry in (store.get("licenses") or []):
        if _license_status_for(entry) != "active":
            continue
        key = str(entry.get("key") or "").strip().upper()
        if not key or key in seen:
            continue
        raw = _load_automation_raw_for_key(key)
        meta = _load_automation_meta(raw)
        if not bool(meta.get("running")):
            continue
        if market_key:
            cfg_map = _load_automation_config(raw)
            if not bool((cfg_map.get(market_key) or {}).get("enabled")):
                continue
        seen.add(key)
        keys.append(key)
    return keys


def _masked_license_key(key: str | None) -> str:
    clean_key = str(key or "").strip().upper()
    if not clean_key:
        return "-"
    return f"{clean_key[:4]}...{clean_key[-4:]}" if len(clean_key) > 8 else clean_key


def _running_license_with_same_api_credentials(current_key: str | None, mk: str, cfg: dict) -> tuple[str, str] | None:
    current_api = str(cfg.get("api_key") or "").strip()
    current_private = str(cfg.get("private_key") or "").strip()
    clean_current_key = str(current_key or "").strip().upper()
    if not current_api or not current_private:
        return None

    store = _load_license_store()
    customer_by_key = {
        str(entry.get("key") or "").strip().upper(): str(entry.get("customer_name") or "").strip()
        for entry in (store.get("licenses") or [])
    }

    for other_key in _iter_running_automation_license_keys(market_key=mk):
        if other_key == clean_current_key:
            continue
        other_raw = _load_automation_raw_for_key(other_key)
        other_cfg = (_load_automation_config(other_raw).get(mk) or {})
        if (
            str(other_cfg.get("api_key") or "").strip() == current_api
            and str(other_cfg.get("private_key") or "").strip() == current_private
        ):
            return other_key, customer_by_key.get(other_key) or ""
    return None


def _automation_credential_signature(cfg: dict) -> str:
    api_key = str(cfg.get("api_key") or "").strip()
    private_key = str(cfg.get("private_key") or "").strip()
    if not api_key or not private_key:
        return ""
    raw = f"{api_key}:{private_key}"
    return hashlib.sha256(raw.encode("utf-8", errors="ignore")).hexdigest()[:24]


def _mark_order_attempt_if_new(cfg: dict, mk: str, market_id, rec: str, kind: str) -> bool:
    signature = _automation_credential_signature(cfg)
    if not signature or not market_id or not rec:
        return True
    attempt_key = f"{signature}|{mk}|{market_id}|{str(rec).upper()}|{str(kind or '').upper()}"
    now_ts = time.time()
    with _order_attempt_dedupe_lock:
        expired = [
            key for key, ts in _recent_order_attempts.items()
            if now_ts - float(ts or 0.0) > AUTOMATION_ORDER_ATTEMPT_DEDUPE_SECONDS
        ]
        for key in expired:
            _recent_order_attempts.pop(key, None)
        if attempt_key in _recent_order_attempts:
            return False
        _recent_order_attempts[attempt_key] = now_ts
    return True


def _clear_order_attempt(cfg: dict, mk: str, market_id, rec: str, kind: str):
    signature = _automation_credential_signature(cfg)
    if not signature or not market_id or not rec:
        return
    attempt_key = f"{signature}|{mk}|{market_id}|{str(rec).upper()}|{str(kind or '').upper()}"
    with _order_attempt_dedupe_lock:
        _recent_order_attempts.pop(attempt_key, None)


def _default_automation_cfg() -> dict:
    return {
        "enabled": False,
        "api_key": "",
        "private_key": "",
        "stake_mode": "fixed",
        "stake_value": 4.0,
        "currency": "BRLX",
        "safe_mode_enabled": True,
        "safe_min_confidence": 90.0,
        "signal_min_confidence": 90.0,
        "odd_min": 1.25,
        "odd_max": 1.55,
        "sniper_enabled": False,
        "sniper_only_enabled": False,
        "sniper_min_multiplier": 3.0,
        "arbitrage_enabled": False,
        "arbitrage_max_sum": AUTOMATION_ARB_DEFAULT_MAX_SUM,
        "cashout_enabled": False,
        "cashout_profit_pct": 20.0,
        "open_position": None,
        "balance": None,
        "last_order_market_id": None,
        "last_order_side": None,
        "last_order_kind": None,
        "last_order_amount": None,
        "last_order_odd": None,
        "last_order_status": "idle",
        "last_order_message": None,
        "last_order_time": None,
    }


def _load_automation_config(raw: dict | None = None) -> dict:
    defaults = {mk: _default_automation_cfg() for mk in MARKET_CONFIGS}
    raw = raw if isinstance(raw, dict) else _load_automation_raw()

    for mk in MARKET_CONFIGS:
        entry = raw.get(mk)
        if not isinstance(entry, dict):
            continue
        merged = defaults[mk]
        merged.update(entry)
        merged["enabled"] = bool(merged.get("enabled"))
        merged["stake_mode"] = (
            "percent" if str(merged.get("stake_mode")) == "percent" else "fixed"
        )
        merged["stake_value"] = max(0.01, _as_float(merged.get("stake_value")) or 0.0)
        merged["currency"] = str(merged.get("currency") or "BRLX").upper()
        merged["safe_mode_enabled"] = bool(merged.get("safe_mode_enabled"))
        merged["safe_min_confidence"] = max(
            50.0,
            min(95.0, _as_float(merged.get("safe_min_confidence")) or 70.0),
        )
        merged["sniper_enabled"] = bool(
            merged.get("sniper_enabled") or merged.get("sniper_only_enabled")
        )
        merged["sniper_min_multiplier"] = max(
            1.5,
            _as_float(merged.get("sniper_min_multiplier")) or 3.0,
        )
        merged["arbitrage_enabled"] = bool(merged.get("arbitrage_enabled"))
        merged["arbitrage_max_sum"] = min(
            0.99,
            max(
                0.50,
                _as_float(merged.get("arbitrage_max_sum")) or AUTOMATION_ARB_DEFAULT_MAX_SUM,
            ),
        )
        merged["cashout_enabled"] = bool(merged.get("cashout_enabled"))
        merged["cashout_profit_pct"] = max(
            1.0,
            _as_float(merged.get("cashout_profit_pct")) or 20.0,
        )
        if not isinstance(merged.get("open_position"), dict):
            merged["open_position"] = None
        merged["balance"] = _as_float(merged.get("balance"))
        defaults[mk] = merged
    return defaults


def _load_automation_meta(raw: dict | None = None) -> dict:
    raw = raw if isinstance(raw, dict) else _load_automation_raw()
    meta = _default_automation_meta()
    extra = raw.get("_meta")
    if isinstance(extra, dict):
        meta["running"] = bool(extra.get("running"))
        meta["active_profile"] = _normalize_automation_profile_key(extra.get("active_profile"))
        meta["execution_profile_mode"] = _normalize_execution_profile_mode(
            extra.get("execution_profile_mode"),
            fallback=meta["active_profile"],
        )
        meta["market_selection_mode"] = _normalize_market_selection_mode(
            extra.get("market_selection_mode")
        )
        meta["last_running_notification_state"] = (
            bool(extra.get("last_running_notification_state"))
            if extra.get("last_running_notification_state") is not None
            else None
        )
        try:
            meta["last_running_notification_ts"] = float(extra.get("last_running_notification_ts") or 0.0)
        except (TypeError, ValueError):
            meta["last_running_notification_ts"] = 0.0
    return meta


def _save_automation_config(cfg_map: dict | None = None, meta_map: dict | None = None):
    with automation_lock:
        payload = {"_meta": dict(meta_map if meta_map is not None else automation_meta)}
        payload.update(cfg_map if cfg_map is not None else automation_cfg)
        _save_private_json("automatizado_config.json", payload)


def _broadcast_all_automation():
    for market_key in MARKET_CONFIGS:
        _broadcast_automation(market_key)


def _automation_enabled_market_names() -> list[str]:
    with automation_lock:
        effective_enabled = _automation_effective_enabled_markets(automation_cfg, automation_meta)
        return [
            MARKET_CONFIGS[market_key]["name"]
            for market_key in MARKET_CONFIGS
            if bool(effective_enabled.get(market_key))
        ]


def _send_automation_running_telegram(running: bool):
    now_ts = time.time()
    persisted_state = None
    persisted_ts = 0.0
    with automation_lock:
        persisted_state = automation_meta.get("last_running_notification_state")
        try:
            persisted_ts = float(automation_meta.get("last_running_notification_ts") or 0.0)
        except (TypeError, ValueError):
            persisted_ts = 0.0
    if (
        _automation_notify_state.get("running") is running
        and now_ts - float(_automation_notify_state.get("ts") or 0.0) < 15
    ):
        return
    if persisted_state is running and now_ts - persisted_ts < 90:
        _automation_notify_state["running"] = running
        _automation_notify_state["ts"] = now_ts
        return
    _automation_notify_state["running"] = running
    _automation_notify_state["ts"] = now_ts
    with automation_lock:
        automation_meta["last_running_notification_state"] = running
        automation_meta["last_running_notification_ts"] = now_ts
        _save_automation_config(automation_cfg, automation_meta)

    enabled_names = _automation_enabled_market_names()
    enabled_text = ", ".join(enabled_names) if enabled_names else "nenhum mercado habilitado"
    if running:
        msg = (
            "<b>BOT AUTOMATIZADO INICIADO</b>\n"
            f"Mercados ativos: <b>{enabled_text}</b>\n"
            "\n"
            "<i>O bot esta ativo no servidor e segue operando mesmo sem a tela aberta.</i>"
        )
    else:
        msg = (
            "<b>BOT AUTOMATIZADO PAUSADO</b>\n"
            f"Mercados ativos antes da pausa: <b>{enabled_text}</b>\n"
            "\n"
            "<i>Nenhuma ordem automatica sera enviada ate iniciar novamente.</i>"
        )
    send_telegram(msg)


def _conf_bar(conf_pct: int | None, length: int = 8) -> str:
    if conf_pct is None:
        return "░" * length
    filled = round((conf_pct / 100) * length)
    return "▓" * filled + "░" * (length - filled)


def _format_currency_amount(value: float | None, currency: str = "BRLX") -> str:
    number = _as_float(value)
    if number is None:
        return f"-- {currency}"
    formatted = f"{number:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"{formatted} {currency}"


def _send_automation_order_telegram(
    mk: str,
    *,
    status: str,
    market_id,
    rec: str | None,
    kind: str | None,
    amount: float | None,
    odd: str | None = None,
    expected_profit: float | None = None,
    conf_pct: int | None = None,
    detail: str | None = None,
):
    market_name = MARKET_CONFIGS.get(mk, {}).get("name") or mk.capitalize()
    rec_icon = "🟢 MAIS" if rec == "MAIS" else ("🔴 ATÉ" if rec == "ATE" else "⚪ --")
    request_raw = _load_automation_raw() if _has_session_license_context() else None
    current_cfg_map = _load_automation_config(request_raw) if request_raw is not None else automation_cfg
    currency = current_cfg_map.get(mk, {}).get("currency") or "BRLX"
    amount_txt = _format_currency_amount(amount, currency)
    odd_txt = odd or "--"
    conf_txt = f"{conf_pct}%" if conf_pct is not None else "--"
    sep = "━━━━━━━━━━━━━━━━"
    st = states.get(mk) or {}
    current_count = _as_float(st.get("current_count"))
    value_needed = _as_float(st.get("value_needed"))
    remaining_seconds = _as_float(st.get("remaining_seconds"))
    source_line = f"\n👤 Origem: <b>{_masked_license_key(_current_license_key())}</b>"

    def _fmt_live_value(value: float | None) -> str:
        if value is None:
            return "--"
        if abs(value - round(value)) < 0.01:
            return str(int(round(value)))
        return f"{value:.2f}"

    def _fmt_mmss(seconds: float | None) -> str:
        if seconds is None:
            return "--"
        total_seconds = max(0, int(round(seconds)))
        minutes, secs = divmod(total_seconds, 60)
        return f"{minutes:02d}:{secs:02d}"

    live_snapshot_line = ""
    if current_count is not None or value_needed is not None or remaining_seconds is not None:
        missing_to_meta = None
        if current_count is not None and value_needed is not None:
            missing_to_meta = max(value_needed - current_count, 0.0)
        live_snapshot_line = (
            f"\n📍 Contador atual: <b>{_fmt_live_value(current_count)}</b>"
            f"  •  Meta: <b>{_fmt_live_value(value_needed)}</b>"
        )
        extra_parts = []
        if missing_to_meta is not None:
            extra_parts.append(f"🎯 Falta para meta: <b>{_fmt_live_value(missing_to_meta)}</b>")
        if remaining_seconds is not None:
            extra_parts.append(f"⏱ Tempo restante: <b>{_fmt_mmss(remaining_seconds)}</b>")
        if conf_pct is not None:
            extra_parts.append(f"📈 Prob. acerto: <b>{conf_pct}%</b>")
        if extra_parts:
            live_snapshot_line += "\n" + "  /  ".join(extra_parts)

    if status == "accepted":
        panel_status = "pending"
        panel_message = f"Ordem recebida pela API em {market_name}. Aguardando confirmacao da posicao."
        if detail:
            panel_message = f"{panel_message} {detail}"
        msg = (
            f"📨 <b>ORDEM RECEBIDA PELA API</b>\n"
            f"{sep}\n"
            f"{rec_icon}  •  <b>{kind or '--'}</b>\n"
            f"📊 Confiança: <b>{_conf_bar(conf_pct)} {conf_txt}</b>\n"
            f"💵 Investido: <b>{amount_txt}</b>\n"
            f"🎯 Odd ref.: <b>{odd_txt}</b>\n"
            f"{live_snapshot_line}\n"
            f"{sep}\n"
            f"Mercado: {market_name}  -  <b>#{market_id}</b>\n"
            f"{source_line}\n"
            f"⏳ <i>Aguardando confirmação da posição no mercado...</i>"
        )
        if detail:
            msg += f"\n🔒 <i>{detail}</i>"
    elif status == "sent":
        panel_status = "sent"
        panel_message = f"Ordem confirmada no mercado em {market_name}."
        if detail:
            panel_message = f"{panel_message} {detail}"
        profit_line = ""
        if expected_profit is not None:
            profit_line = f"\n💰 Lucro esp.: <b>+{_format_currency_amount(expected_profit, currency)}</b>"
        balance = _as_float((current_cfg_map.get(mk) or {}).get("balance"))
        balance_line = f"\n🏦 Saldo atual: <b>{_format_currency_amount(balance, currency)}</b>" if balance is not None else ""
        if balance is not None and expected_profit is not None:
            new_balance = balance + expected_profit
            balance_line += f"  →  <b>{_format_currency_amount(new_balance, currency)}</b> se ganhar"
        msg = (
            f"🤖 <b>ORDEM CONFIRMADA NO MERCADO ✅</b>\n"
            f"{sep}\n"
            f"{rec_icon}  •  <b>{kind or '--'}</b>\n"
            f"📊 Confiança: <b>{_conf_bar(conf_pct)} {conf_txt}</b>\n"
            f"💵 Investido: <b>{amount_txt}</b>\n"
            f"🎯 Odd entrada: <b>{odd_txt}</b>"
            f"{profit_line}"
            f"{balance_line}\n"
            f"{live_snapshot_line}\n"
            f"{sep}\n"
            f"Mercado: {market_name}  -  <b>#{market_id}</b>"
            f"{source_line}"
        )
        if detail:
            msg += f"\n🔒 <i>{detail}</i>"
    else:
        panel_status = "error"
        panel_message = detail or "Falha ao enviar ordem automatica."
        msg = (
            f"🤖 <b>ORDEM NÃO ENVIADA ❌</b>\n"
            f"{sep}\n"
            f"{rec_icon}  •  <b>{kind or '--'}</b>\n"
            f"💵 Valor: <b>{amount_txt}</b>\n"
            f"🎯 Odd ref.: <b>{odd_txt}</b>\n"
            f"{sep}\n"
            f"Mercado: {market_name}  -  <b>#{market_id}</b>\n"
            f"{source_line}\n"
            f"⚠️ <i>{detail or 'Sem detalhe.'}</i>"
        )
    if status == "accepted":
        _set_telegram_status(panel_status, panel_message, market_key=mk)
    send_telegram(
        msg,
        market_key=mk,
        post_send_status=panel_status,
        post_send_message=panel_message,
        post_send_mark_sent=True,
    )


def _send_automation_result_telegram(
    mk: str,
    *,
    won: bool,
    rec: str | None,
    position: dict | None,
    score: dict,
):
    """Notificação de resultado da ordem automática (win/loss + P&L)."""
    market_name = MARKET_CONFIGS.get(mk, {}).get("name") or mk.capitalize()
    currency = automation_cfg.get(mk, {}).get("currency") or "BRLX"
    sep = "━━━━━━━━━━━━━━━━"

    if won:
        header = f"🏆 <b>ORDEM GANHA ✅</b>"
        pl_color = "+"
    else:
        header = f"💀 <b>ORDEM PERDIDA ❌</b>"
        pl_color = ""

    rec_icon = "🟢 MAIS" if rec == "MAIS" else ("🔴 ATÉ" if rec == "ATE" else "⚪ --")

    pos_lines = ""
    if position:
        invested = position.get("invested_total")
        share_amount = position.get("share_amount")
        entry_odd = position.get("entry_odd") or "--"
        kind = position.get("kind") or "SINAL"
        conf_pct = position.get("conf_pct")
        conf_txt = f"{conf_pct}%" if conf_pct is not None else "--"

        if won:
            # shares resolvem em 1.0 → lucro = shares - invested
            profit = round(share_amount - invested, 2) if (share_amount is not None and invested is not None) else position.get("expected_profit")
        else:
            profit = round(-invested, 2) if invested is not None else None

        profit_txt = (f"{pl_color}{_format_currency_amount(profit, currency)}" if profit is not None else f"{pl_color}-- {currency}")
        invested_txt = _format_currency_amount(invested, currency)

        pos_lines = (
            f"\n{rec_icon}  •  <b>{kind}</b>"
            f"\n📊 Confiança: <b>{_conf_bar(conf_pct)} {conf_txt}</b>"
            f"\n💵 Investido: <b>{invested_txt}</b>"
            f"\n🎯 Odd entrada: <b>{entry_odd}</b>"
            f"\n{'💰' if won else '🔻'} Resultado: <b>{profit_txt}</b>"
        )

    total_sc = score.get("wins", 0) + score.get("losses", 0)
    acc_str = f"{int(score['wins'] / total_sc * 100)}%" if total_sc else "0%"
    placar = f"✅ {score.get('wins',0)} ❌ {score.get('losses',0)} ({acc_str})"

    msg = (
        f"{header}\n"
        f"{sep}"
        f"{pos_lines}\n"
        f"{sep}\n"
        f"{_telegram_operational_context(mk, position.get('conf_pct') if position else None, rec)}\n"
        f"{sep}\n"
        f"Mercado: {market_name}\n"
        f"📅 Hoje: {placar}"
    )
    send_telegram(msg, market_key=mk)


def _automation_public_payload(mk: str) -> dict:
    if _has_session_license_context():
        raw = _load_automation_raw()
        current_automation_cfg = _load_automation_config(raw)
        current_automation_meta = _load_automation_meta(raw)
        cfg = dict(current_automation_cfg.get(mk) or _default_automation_cfg())
        meta = dict(current_automation_meta)
    else:
        with automation_lock:
            current_automation_cfg = automation_cfg
            current_automation_meta = automation_meta
            cfg = dict(automation_cfg.get(mk) or _default_automation_cfg())
            meta = dict(automation_meta)
    active_profile = _normalize_automation_profile_key(meta.get("active_profile"))
    execution_profile_mode = _normalize_execution_profile_mode(
        meta.get("execution_profile_mode"),
        fallback=active_profile,
    )
    setup = _automation_health_payload(cfg, meta, mk, current_automation_cfg)
    profile_a_selected = execution_profile_mode in {"a", "both"}
    profile_b_selected = execution_profile_mode in {"b", "both"}
    profile_a = {
        "configured": bool(str(cfg.get("api_key") or "").strip() and str(cfg.get("private_key") or "").strip()),
        "selected_for_execution": profile_a_selected,
        "runtime": (
            "desabilitado" if not bool(cfg.get("enabled"))
            else "parado" if not bool(meta.get("running"))
            else "licenca_invalida" if not _license_is_active()
            else "faltando credenciais" if not bool(str(cfg.get("api_key") or "").strip() and str(cfg.get("private_key") or "").strip())
            else "iniciado"
        ),
        "api_auth_state": setup.get("api_auth_state") or "pending",
        "balance_loaded": bool(setup.get("balance_loaded")),
        "balance": cfg.get("balance"),
        "currency": cfg.get("currency") or "BRLX",
        "stake_mode": cfg.get("stake_mode") or "fixed",
        "stake_value": cfg.get("stake_value") or 2.0,
        "safe_min_confidence": cfg.get("safe_min_confidence") or 70.0,
        "last_order_market_id": cfg.get("last_order_market_id"),
        "last_order_side": cfg.get("last_order_side"),
        "last_order_kind": cfg.get("last_order_kind"),
        "last_order_amount": cfg.get("last_order_amount"),
        "last_order_volume": _automation_last_order_volume(cfg),
        "last_order_odd": cfg.get("last_order_odd"),
        "last_order_status": cfg.get("last_order_status"),
        "last_order_message": cfg.get("last_order_message"),
        "last_order_time": cfg.get("last_order_time"),
    }
    profile_b = {
        "configured": False,
        "selected_for_execution": profile_b_selected,
        "runtime": "fora",
        "api_auth_state": "pending",
        "balance_loaded": False,
        "balance": None,
        "currency": cfg.get("currency") or "BRLX",
        "stake_mode": "fixed",
        "stake_value": cfg.get("stake_value") or 2.0,
        "safe_min_confidence": cfg.get("safe_min_confidence") or 70.0,
        "last_order_market_id": None,
        "last_order_side": None,
        "last_order_kind": None,
        "last_order_amount": None,
        "last_order_volume": None,
        "last_order_odd": None,
        "last_order_status": "idle",
        "last_order_message": "Conta unica em uso. Perfil B nao e utilizado.",
        "last_order_time": None,
    }
    effective_enabled_markets = _automation_effective_enabled_markets(current_automation_cfg, meta)
    return {
        "enabled": bool(cfg.get("enabled")),
        "running": bool(meta.get("running")),
        "active_profile": active_profile,
        "execution_profile_mode": execution_profile_mode,
        "market_selection_mode": _normalize_market_selection_mode(meta.get("market_selection_mode")),
        "execution_profile_label": _execution_profile_label(execution_profile_mode),
        "enabled_markets": effective_enabled_markets,
        "manual_enabled_markets": {
            market_key: bool((current_automation_cfg.get(market_key) or {}).get("enabled"))
            for market_key in MARKET_CONFIGS
        },
        "api_key_masked": _mask_secret(cfg.get("api_key")),
        "private_key_masked": _mask_secret(cfg.get("private_key")),
        "has_private_key": bool(str(cfg.get("private_key") or "").strip()),
        "stake_mode": cfg.get("stake_mode") or "fixed",
        "stake_value": cfg.get("stake_value") or 2.0,
        "currency": cfg.get("currency") or "BRLX",
        "safe_mode_enabled": bool(cfg.get("safe_mode_enabled")),
        "safe_min_confidence": cfg.get("safe_min_confidence") or 70.0,
        "signal_min_confidence": _market_signal_min_confidence(mk),
        "sniper_enabled": bool(cfg.get("sniper_enabled")),
        "sniper_min_multiplier": cfg.get("sniper_min_multiplier") or 3.0,
        "arbitrage_enabled": bool(cfg.get("arbitrage_enabled")),
        "arbitrage_max_sum": cfg.get("arbitrage_max_sum") or AUTOMATION_ARB_DEFAULT_MAX_SUM,
        "cashout_enabled": bool(cfg.get("cashout_enabled")),
        "cashout_profit_pct": cfg.get("cashout_profit_pct") or 20.0,
        "open_position": cfg.get("open_position"),
        "balance": cfg.get("balance"),
        "last_balance_at": cfg.get("last_balance_at"),
        "balance_timeline": _balance_timeline_public_payload(cfg.get("currency") or "BRLX"),
        "volume_timeline": _automation_volume_timeline_public(mk, cfg.get("currency") or "BRLX"),
        "market_plan": _automation_dual_market_plan(current_automation_cfg),
        "last_order_market_id": cfg.get("last_order_market_id"),
        "last_order_side": cfg.get("last_order_side"),
        "last_order_kind": cfg.get("last_order_kind"),
        "last_order_amount": cfg.get("last_order_amount"),
        "last_order_volume": _automation_last_order_volume(cfg),
        "last_order_odd": cfg.get("last_order_odd"),
        "last_order_status": cfg.get("last_order_status"),
        "last_order_message": cfg.get("last_order_message"),
        "last_order_time": cfg.get("last_order_time"),
        "order_log": _automation_order_log_public(mk),
        "license": _license_public_payload(),
        "profiles": {
            "a": profile_a,
            "b": profile_b,
        },
        "setup_status": setup,
    }


def _broadcast_automation(mk: str):
    broadcast(mk, "automation_update", _automation_public_payload(mk), license_key=_current_license_key())


def _promote_runtime_automation_state_if_active(cfg_map: dict, meta_map: dict, *, license_key: str | None = None):
    global automation_cfg, automation_meta
    current_key = str(license_key or _current_license_key() or "").strip().upper()
    with _runtime_context_lock:
        active_key = str(_active_runtime_license_key or "").strip().upper()
    if not current_key or not active_key or current_key != active_key:
        return
    with automation_lock:
        automation_cfg = cfg_map
        automation_meta = meta_map


def _set_automation_status(
    mk: str,
    status: str,
    message: str,
    *,
    market_id=None,
    side=None,
    kind=None,
    amount=None,
    odd=_AUTOMATION_UNSET,
    response_payload=None,
    balance=None,
):
    request_raw = _load_automation_raw() if _has_session_license_context() else None
    with automation_lock:
        current_cfg_map = _load_automation_config(request_raw) if request_raw is not None else automation_cfg
        current_meta = _load_automation_meta(request_raw) if request_raw is not None else automation_meta
        cfg = current_cfg_map.get(mk)
        if not cfg:
            return
        cfg["last_order_status"] = status
        cfg["last_order_message"] = message
        cfg["last_order_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if market_id is not None:
            cfg["last_order_market_id"] = str(market_id)
        if side is not None:
            cfg["last_order_side"] = side
        if kind is not None:
            cfg["last_order_kind"] = kind
        if amount is not None:
            cfg["last_order_amount"] = amount
        if odd is not _AUTOMATION_UNSET:
            cfg["last_order_odd"] = odd
        if response_payload is not None:
            cfg["last_order_response"] = response_payload
        if balance is not None:
            cfg["balance"] = balance
            cfg["last_balance_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        _save_automation_config(current_cfg_map, current_meta)
    _promote_runtime_automation_state_if_active(current_cfg_map, current_meta)
    if balance is not None:
        _record_balance_snapshot(balance, (cfg.get("currency") or "BRLX"))
    _broadcast_automation(mk)


def _automation_store_balance_only(mk: str, balance: float, currency: str | None = None):
    request_raw = _load_automation_raw() if _has_session_license_context() else None
    with automation_lock:
        current_cfg_map = _load_automation_config(request_raw) if request_raw is not None else automation_cfg
        current_meta = _load_automation_meta(request_raw) if request_raw is not None else automation_meta
        cfg = current_cfg_map.get(mk)
        if not cfg:
            return
        if currency:
            cfg["currency"] = currency
        cfg["balance"] = balance
        cfg["last_balance_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        _save_automation_config(current_cfg_map, current_meta)
        chosen_currency = cfg.get("currency") or "BRLX"
    _promote_runtime_automation_state_if_active(current_cfg_map, current_meta)
    _record_balance_snapshot(balance, chosen_currency)
    _broadcast_automation(mk)


def _automation_extract_order_odd(payload: dict | None) -> str | None:
    def walk(node):
        if isinstance(node, dict):
            for key in ("price", "avgPrice", "averagePrice", "executedPrice"):
                odd = _format_order_odd_display(node.get(key))
                if odd:
                    return odd
            for value in node.values():
                odd = walk(value)
                if odd:
                    return odd
        elif isinstance(node, list):
            for item in node:
                odd = walk(item)
                if odd:
                    return odd
        return None

    return walk(payload or {})


def _automation_extract_order_price_num(payload: dict | None) -> float | None:
    def walk(node):
        if isinstance(node, dict):
            for key in ("price", "avgPrice", "averagePrice", "executedPrice"):
                value = _as_float(node.get(key))
                if value is not None and value > 0:
                    return value
            for value in node.values():
                found = walk(value)
                if found is not None:
                    return found
        elif isinstance(node, list):
            for item in node:
                found = walk(item)
                if found is not None:
                    return found
        return None

    return walk(payload or {})


def _automation_extract_order_share_amount(payload: dict | None) -> float | None:
    def walk(node):
        if isinstance(node, dict):
            for key in ("amount", "filledAmount", "quantity"):
                value = _as_float(node.get(key))
                if value is not None and value > 0:
                    return value
            for value in node.values():
                found = walk(value)
                if found is not None:
                    return found
        elif isinstance(node, list):
            for item in node:
                found = walk(item)
                if found is not None:
                    return found
        return None

    return walk(payload or {})


def _automation_last_order_volume(cfg: dict | None) -> float | None:
    if not isinstance(cfg, dict):
        return None

    position = cfg.get("open_position")
    if isinstance(position, dict):
        share_amount = _as_float(position.get("share_amount"))
        last_market_id = str(cfg.get("last_order_market_id") or "")
        pos_market_id = str(position.get("market_id") or "")
        if share_amount is not None and share_amount > 0 and (
            not last_market_id or not pos_market_id or last_market_id == pos_market_id
        ):
            return round(share_amount, 4)

    response_payload = cfg.get("last_order_response")
    share_amount = _automation_extract_order_share_amount(response_payload)
    if share_amount is not None and share_amount > 0:
        return round(share_amount, 4)

    return None


def _automation_orderbook_top_ask(
    market_id,
    selection_id,
) -> dict | None:
    if not market_id or not selection_id:
        return None

    payload = _api_get(
        f"https://app.palpitano.com/api/v1/orderbook?marketId={market_id}&limit={AUTOMATION_SNIPER_ORDERBOOK_LIMIT}"
    )
    if not payload:
        return None

    books = ((payload.get("data") or {}).get("books") or {})
    book = books.get(str(selection_id)) or books.get(selection_id) or {}
    asks = book.get("asks") or []
    if not asks:
        return None

    # Ordena ascending por preco (menor preco = maior odd = melhor para o comprador)
    parsed = []
    for a in asks:
        p = _as_float(a.get("price"))
        q = _as_float(a.get("amount") or a.get("quantity"))
        if p and p > 0 and q and q > 0:
            parsed.append((p, q))
    if not parsed:
        return None
    parsed.sort(key=lambda x: x[0])
    price, qty = parsed[0]

    return {
        "price": price,
        "display": _format_order_odd_display(price) or _format_odd_value(price),
        "quantity": qty,
        "notional": price * qty,
    }


def _automation_orderbook_top_bid(
    market_id,
    selection_id,
) -> dict | None:
    if not market_id or not selection_id:
        return None

    payload = _api_get(
        f"https://app.palpitano.com/api/v1/orderbook?marketId={market_id}&limit={AUTOMATION_SNIPER_ORDERBOOK_LIMIT}"
    )
    if not payload:
        return None

    books = ((payload.get("data") or {}).get("books") or {})
    book = books.get(str(selection_id)) or books.get(selection_id) or {}
    bids = book.get("bids") or []
    if not bids:
        return None

    top = bids[0]
    price = _as_float(top.get("price"))
    qty = _as_float(top.get("amount") or top.get("quantity"))
    if price is None or price <= 0 or qty is None or qty <= 0:
        return None

    return {
        "price": price,
        "display": _format_order_odd_display(price) or _format_odd_value(price),
        "quantity": qty,
        "notional": price * qty,
    }


def _automation_sniper_candidate(
    mk: str,
    market_id,
    selection_id,
    amount: float,
    max_price: float | None = None,
) -> dict | None:
    if not AUTOMATION_SNIPER_ENABLED:
        return None

    st = states[mk]
    if str(st.get("market_id") or "") != str(market_id or ""):
        return None

    remaining = _as_float(st.get("remaining_seconds"))
    if remaining is None:
        return None
    elapsed = max(0, MARKET_CONFIGS[mk]["duration"] - int(remaining))
    if elapsed > AUTOMATION_SNIPER_MAX_ELAPSED_SECONDS:
        return None

    top_ask = _automation_orderbook_top_ask(market_id, selection_id)
    if not top_ask:
        return None
    limit_price = max_price if max_price is not None and max_price > 0 else AUTOMATION_SNIPER_MAX_PRICE
    if top_ask["price"] > limit_price:
        return None
    if top_ask["notional"] + 1e-9 < amount:
        return None

    share_amount = round(amount / top_ask["price"], 2)
    if share_amount <= 0:
        return None

    return {
        "price": top_ask["price"],
        "price_display": top_ask["display"],
        "share_amount": share_amount,
        "elapsed": elapsed,
    }


def _automation_set_open_position(mk: str, position: dict | None):
    request_raw = _load_automation_raw() if _has_session_license_context() else None
    with automation_lock:
        current_cfg_map = _load_automation_config(request_raw) if request_raw is not None else automation_cfg
        current_meta = _load_automation_meta(request_raw) if request_raw is not None else automation_meta
        cfg = current_cfg_map.get(mk)
        if not cfg:
            return
        cfg["open_position"] = position if isinstance(position, dict) else None
        _save_automation_config(current_cfg_map, current_meta)
    _broadcast_automation(mk)


def _automation_reconcile_open_position(
    mk: str,
    *,
    min_check_interval: float = 8.0,
) -> bool:
    request_raw = _load_automation_raw() if _has_session_license_context() else None
    with automation_lock:
        current_cfg_map = _load_automation_config(request_raw) if request_raw is not None else automation_cfg
        current_meta = _load_automation_meta(request_raw) if request_raw is not None else automation_meta
        cfg = dict(current_cfg_map.get(mk) or _default_automation_cfg())

    open_position = cfg.get("open_position")
    if not isinstance(open_position, dict):
        return False
    if (
        not str(cfg.get("api_key") or "").strip()
        or not str(cfg.get("private_key") or "").strip()
    ):
        return False

    market_id = open_position.get("market_id")
    selection_id = open_position.get("selection_id")
    if not market_id or not selection_id:
        _automation_set_open_position(mk, None)
        return True

    cache_key = f"{_current_license_key() or 'runtime'}:{mk}:{market_id}:{selection_id}"
    now_ts = time.time()
    with _automation_open_position_check_lock:
        last_ts = float(_automation_open_position_last_check.get(cache_key) or 0.0)
        if now_ts - last_ts < max(1.0, float(min_check_interval)):
            return False
        _automation_open_position_last_check[cache_key] = now_ts

    live_position = _automation_fetch_open_position(cfg, market_id, selection_id)
    if not live_position:
        with automation_lock:
            latest_cfg_map = _load_automation_config(_load_automation_raw()) if _has_session_license_context() else automation_cfg
            latest_meta = _load_automation_meta(_load_automation_raw()) if _has_session_license_context() else automation_meta
            live_cfg = latest_cfg_map.get(mk)
            if not live_cfg:
                return False
            if not isinstance(live_cfg.get("open_position"), dict):
                return False
            live_cfg["open_position"] = None
            current_status = str(live_cfg.get("last_order_status") or "").lower()
            running = bool(latest_meta.get("running"))
            if current_status in {"sent", "pending"}:
                live_cfg["last_order_status"] = "ready" if running else "idle"
                live_cfg["last_order_message"] = (
                    "Posicao anterior nao aparece mais como aberta na API."
                )
                live_cfg["last_order_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            _save_automation_config(latest_cfg_map, latest_meta)
        _promote_runtime_automation_state_if_active(
            latest_cfg_map,
            latest_meta,
            license_key=_current_license_key(),
        )
        _broadcast_automation(mk)
        return True

    refreshed = dict(open_position)
    live_quantity = _as_float(live_position.get("quantity"))
    live_avg_price = _as_float(live_position.get("avg_price"))
    if live_quantity is not None and live_quantity > 0:
        refreshed["share_amount"] = round(live_quantity, 4)
    if live_avg_price is not None and live_avg_price > 0:
        refreshed["entry_price_num"] = live_avg_price
        refreshed["entry_odd"] = _format_order_odd_display(live_avg_price) or refreshed.get("entry_odd")
    refreshed["invested_total"] = _automation_confirmed_position_invested(
        live_position,
        _as_float(refreshed.get("share_amount")),
        _as_float(refreshed.get("entry_price_num")),
        _as_float(refreshed.get("invested_total")) or _as_float(refreshed.get("amount")) or 0.0,
    )
    expected_profit = _expected_win_profit(
        _as_float(refreshed.get("invested_total")),
        _as_float(refreshed.get("entry_price_num")),
    )
    if expected_profit is not None:
        refreshed["expected_profit"] = expected_profit

    if refreshed != open_position:
        _automation_set_open_position(mk, refreshed)
        return True
    return False


def _automation_fetch_open_position(
    cfg: dict,
    market_id,
    selection_id,
) -> dict | None:
    if not market_id or not selection_id:
        return None

    resp = _automation_api_request(
        cfg,
        f"/positions?marketId={market_id}&status=OPEN&perPage=100&page=1",
        method="GET",
    )
    if not resp.get("success"):
        return None

    container = resp.get("data")
    if isinstance(container, dict):
        items = container.get("items")
    elif isinstance(container, list):
        items = container
    else:
        items = resp.get("items")
    if not isinstance(items, list):
        return None

    for item in items:
        if not isinstance(item, dict):
            continue
        if str(item.get("selectionId") or "") != str(selection_id):
            continue
        qty = (
            _as_float(item.get("quantity"))
            or _as_float(item.get("filledAmount"))
            or _as_float(item.get("shareAmount"))
            or _as_float(item.get("shares"))
            or _as_float(item.get("positionSize"))
        )
        avg_price = (
            _as_float(item.get("avgPrice"))
            or _as_float(item.get("price"))
            or _as_float(item.get("entryPrice"))
            or _as_float(item.get("averagePrice"))
        )
        if qty is None and avg_price is None:
            continue
        return {
            "quantity": round(qty, 4) if qty is not None else None,
            "avg_price": avg_price,
            "raw": item,
        }
    return None


def _automation_position_execution_pct(position_data: dict | None) -> float | None:
    raw = (position_data or {}).get("raw") if isinstance(position_data, dict) else None
    if not isinstance(raw, dict):
        return None
    for key in (
        "executedPercent",
        "executedPercentage",
        "filledPercent",
        "fillPercent",
        "percentFilled",
        "executionPercent",
        "progress",
    ):
        value = _as_float(raw.get(key))
        if value is not None:
            return value
    return None


def _automation_position_has_execution(position_data: dict | None) -> bool:
    raw = (position_data or {}).get("raw") if isinstance(position_data, dict) else None
    share_amount = _as_float((position_data or {}).get("quantity"))
    if share_amount is not None and share_amount > 0:
        return True

    execution_pct = _automation_position_execution_pct(position_data)
    if execution_pct is not None and execution_pct > 0:
        return True

    if isinstance(raw, dict):
        for key in (
            "filledAmount",
            "executedAmount",
            "filledValue",
            "executedValue",
            "invested",
            "investedAmount",
            "spent",
        ):
            value = _as_float(raw.get(key))
            if value is not None and value > 0:
                return True

        status_text = " ".join(
            str(raw.get(key) or "")
            for key in ("status", "orderStatus", "state", "label")
        ).strip().lower()
        if status_text and any(token in status_text for token in ("aguard", "waiting", "pending", "aplicad", "placed")):
            return False

    return False


def _automation_confirmed_position_invested(
    position_data: dict | None,
    share_amount: float | None,
    avg_price: float | None,
    fallback_amount: float,
) -> float:
    raw = (position_data or {}).get("raw") if isinstance(position_data, dict) else None
    if isinstance(raw, dict):
        for key in (
            "invested",
            "investedAmount",
            "filledValue",
            "executedValue",
            "spent",
        ):
            value = _as_float(raw.get(key))
            if value is not None and value > 0:
                return round(value, 2)
    if share_amount is not None and share_amount > 0 and avg_price is not None and avg_price > 0:
        return round(share_amount * avg_price, 2)
    return round(fallback_amount, 2)


def _automation_build_open_position(
    mk: str,
    *,
    market_id,
    selection_id,
    rec: str,
    kind: str,
    amount: float,
    price_num: float | None,
    odd_display: str | None,
    conf_pct: int | None,
    cfg: dict,
    position_data: dict | None = None,
) -> dict | None:
    pos = position_data if isinstance(position_data, dict) else _automation_fetch_open_position(cfg, market_id, selection_id)
    share_amount = pos.get("quantity") if pos else None
    avg_price = pos.get("avg_price") if pos else None
    execution_pct = _automation_position_execution_pct(pos)

    final_price = avg_price or price_num
    if final_price is None or final_price <= 0:
        return None

    if pos:
        if not _automation_position_has_execution(pos):
            return None
        if share_amount is not None and share_amount > 0:
            pass
        elif execution_pct is not None and execution_pct > 0:
            invested_total = _automation_confirmed_position_invested(pos, None, final_price, amount)
            if invested_total <= 0:
                return None
            share_amount = round(invested_total / final_price, 4)
        else:
            return None
    elif share_amount is None or share_amount <= 0:
        share_amount = round(amount / final_price, 4)

    if share_amount is None or share_amount <= 0:
        return None

    invested_total = _automation_confirmed_position_invested(pos, share_amount, final_price, amount)
    expected_profit = _expected_win_profit(invested_total, final_price)
    return {
        "market_id": str(market_id),
        "selection_id": int(selection_id),
        "rec": rec,
        "kind": kind,
        "invested_total": invested_total,
        "share_amount": round(share_amount, 4),
        "entry_price": round(final_price, 4),
        "entry_odd": odd_display or _format_order_odd_display(final_price),
        "expected_profit": expected_profit,
        "conf_pct": conf_pct,
        "opened_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def _automation_confirm_position_async(
    mk: str,
    *,
    market_id,
    selection_id,
    rec: str,
    kind: str,
    total_amount: float,
    order_price_num: float | None,
    order_odd: str | None,
    conf_pct: int | None,
    execution_detail: str,
    currency: str,
    response_status,
    license_key: str | None = None,
):
    def _worker():
        nonlocal order_odd
        with _license_thread_context(license_key):
            try:
                expected_profit = _expected_win_profit(total_amount, order_price_num)
                for _ in range(20):
                    time.sleep(1.0)
                    request_raw = _load_automation_raw() if _has_session_license_context() else None
                    with automation_lock:
                        current_cfg_map = _load_automation_config(request_raw) if request_raw is not None else automation_cfg
                        cfg_live = dict(current_cfg_map.get(mk) or _default_automation_cfg())
                    confirmed_pos = _automation_fetch_open_position(cfg_live, market_id, selection_id)
                    if not confirmed_pos:
                        continue
                    open_position = _automation_build_open_position(
                        mk,
                        market_id=market_id,
                        selection_id=selection_id,
                        rec=rec,
                        kind=kind,
                        amount=total_amount,
                        price_num=order_price_num,
                        odd_display=order_odd,
                        conf_pct=conf_pct,
                        cfg=cfg_live,
                        position_data=confirmed_pos,
                    )
                    if not open_position:
                        continue

                    _automation_set_open_position(mk, open_position)
                    order_odd = open_position.get("entry_odd") or order_odd
                    executed_amount = _as_float(open_position.get("invested_total")) or total_amount
                    expected_profit = open_position.get("expected_profit")
                    expected_profit_txt = (
                        f"{expected_profit:.2f} {currency}"
                        if expected_profit is not None else
                        f"-- {currency}"
                    )
                    conf_txt = f" | {conf_pct}%" if conf_pct is not None else ""
                    success_message = (
                        f"Ordem automatica enviada: {kind} {rec} | "
                        f"{executed_amount:.2f} {currency} | odd {order_odd or '--'} | "
                        f"lucro esp. {expected_profit_txt}{conf_txt} | "
                        f"{execution_detail}"
                    )
                    _automation_order_log_update_latest(
                        mk,
                        market_id,
                        rec=rec,
                        statuses=("pending",),
                        status="sent",
                        amount=executed_amount,
                        odd=order_odd,
                        detail=success_message,
                        api_status=response_status,
                        execution=execution_detail,
                        accepted_by_api=True,
                        confirmed_position=True,
                    )
                    _set_automation_status(
                        mk,
                        "sent",
                        success_message,
                        market_id=market_id,
                        side=rec,
                        kind=kind,
                        amount=executed_amount,
                        odd=order_odd,
                        response_payload={
                            "status": response_status,
                            "endpoint": "orders",
                            "odd": order_odd,
                            "expected_profit": expected_profit,
                            "executed_amount": executed_amount,
                            "execution": execution_detail,
                            "confirm_async": True,
                        },
                    )
                    _send_automation_order_telegram(
                        mk,
                        status="sent",
                        market_id=market_id,
                        rec=rec,
                        kind=kind,
                        amount=executed_amount,
                        odd=order_odd,
                        expected_profit=expected_profit,
                        conf_pct=conf_pct,
                        detail=f"confirmada com atraso | {execution_detail}",
                    )
                    _automation_refresh_balance_async(mk)
                    _analytics_mark_bet_placed(mk, market_id)
                    print(
                        f"[AUTO:{mk}] confirmacao tardia market={market_id} {rec} "
                        f"{executed_amount:.2f} {currency} odd={order_odd or '--'}",
                        flush=True,
                    )
                    return

                timeout_detail = (
                    "API aceitou a ordem, mas a posicao ainda nao confirmou. "
                    "Status pendente de confirmacao no mercado."
                )
                _set_automation_status(
                    mk,
                    "pending",
                    timeout_detail,
                    market_id=market_id,
                    side=rec,
                    kind=kind,
                    amount=total_amount,
                    odd=order_odd,
                    response_payload={
                        "status": response_status,
                        "endpoint": "orders",
                        "odd": order_odd,
                        "expected_profit": expected_profit,
                        "execution": execution_detail,
                        "confirm_async": "timeout",
                    },
                )
                _automation_order_log_update_latest(
                    mk,
                    market_id,
                    rec=rec,
                    statuses=("pending",),
                    detail=timeout_detail,
                    api_status=response_status,
                    execution=execution_detail,
                    accepted_by_api=True,
                    confirmed_position=False,
                )
                print(f"[AUTO:{mk}] confirmacao pendente market={market_id} apos espera", flush=True)
            except Exception as ex:
                print(f"[AUTO:{mk}] erro confirmacao assinc market={market_id}: {ex}", flush=True)

    threading.Thread(target=_worker, daemon=True).start()


def _send_automation_cashout_telegram(
    mk: str,
    *,
    status: str,
    market_id,
    rec: str | None,
    sold_amount: float | None,
    exit_odd: str | None = None,
    payout_value: float | None = None,
    profit_value: float | None = None,
    profit_pct: float | None = None,
    detail: str | None = None,
):
    market_name = MARKET_CONFIGS.get(mk, {}).get("name") or mk.capitalize()
    rec_icon = "🟢 MAIS" if rec == "MAIS" else ("🔴 ATÉ" if rec == "ATE" else "⚪ --")
    currency = automation_cfg.get(mk, {}).get("currency") or "BRLX"
    sold_txt = _format_currency_amount(sold_amount, currency)
    payout_txt = _format_currency_amount(payout_value, currency)
    profit_txt = _format_currency_amount(profit_value, currency)
    profit_sign = "+" if (profit_value or 0) >= 0 else ""
    pct_txt = f"{profit_pct:.1f}%" if profit_pct is not None else "--"
    odd_txt = exit_odd or "--"
    sep = "━━━━━━━━━━━━━━━━"

    if status == "sent":
        msg = (
            f"💸 <b>CASHOUT AUTOMÁTICO ✅</b>\n"
            f"{sep}\n"
            f"Saindo de {rec_icon}\n"
            f"💵 Investido: <b>{sold_txt}</b>  →  recebendo <b>{payout_txt}</b>\n"
            f"💰 Lucro travado: <b>{profit_sign}{profit_txt}</b> ({pct_txt})\n"
            f"🎯 Odd saída: <b>{odd_txt}</b>\n"
            f"{sep}\n"
            f"Mercado: {market_name}  -  <b>#{market_id}</b>"
        )
        if detail:
            msg += f"\n<i>{detail}</i>"
    else:
        msg = (
            f"💸 <b>CASHOUT NÃO ENVIADO ❌</b>\n"
            f"{sep}\n"
            f"Saindo de {rec_icon}\n"
            f"💵 Investido: <b>{sold_txt}</b>  →  estimado <b>{payout_txt}</b>\n"
            f"💰 Lucro atual: <b>{profit_sign}{profit_txt}</b> ({pct_txt})\n"
            f"🎯 Odd saída: <b>{odd_txt}</b>\n"
            f"{sep}\n"
            f"Mercado: {market_name}  -  <b>#{market_id}</b>\n"
            f"⚠️ <i>{detail or 'Sem detalhe.'}</i>"
        )
    send_telegram(msg, market_key=mk)


def _send_automation_arbitrage_telegram(
    mk: str,
    *,
    status: str,
    market_id,
    total_spend: float | None,
    share_amount: float | None,
    more_odd: str | None,
    ate_odd: str | None,
    sum_price: float | None,
    locked_profit: float | None,
    locked_profit_pct: float | None,
    detail: str | None = None,
):
    market_name = MARKET_CONFIGS.get(mk, {}).get("name") or mk.capitalize()
    currency = automation_cfg.get(mk, {}).get("currency") or "BRLX"
    total_txt = _format_currency_amount(total_spend, currency)
    share_txt = f"{share_amount:.2f}" if share_amount is not None else "--"
    sum_txt = f"{sum_price:.2f}" if sum_price is not None else "--"
    profit_txt = _format_currency_amount(locked_profit, currency)
    pct_txt = f"{locked_profit_pct:.1f}%" if locked_profit_pct is not None else "--"
    sep = "━━━━━━━━━━━━━━━━"

    if status == "sent":
        msg = (
            f"🔀 <b>ARBITRAGEM SNIPER ✅</b>\n"
            f"{sep}\n"
            f"🟢 MAIS <b>{more_odd or '--'}</b>  +  🔴 ATÉ <b>{ate_odd or '--'}</b>\n"
            f"📊 Soma: <b>{sum_txt}</b>  •  Shares: <b>{share_txt}</b>\n"
            f"💵 Capital total: <b>{total_txt}</b>\n"
            f"💰 Lucro travado: <b>+{profit_txt}</b> ({pct_txt})\n"
            f"{sep}\n"
            f"Mercado: {market_name}  -  <b>#{market_id}</b>"
        )
        if detail:
            msg += f"\n<i>{detail}</i>"
    else:
        msg = (
            f"🔀 <b>ARBITRAGEM NÃO ENVIADA ❌</b>\n"
            f"{sep}\n"
            f"🟢 MAIS <b>{more_odd or '--'}</b>  +  🔴 ATÉ <b>{ate_odd or '--'}</b>\n"
            f"💵 Capital total: <b>{total_txt}</b>\n"
            f"💰 Lucro estimado: <b>+{profit_txt}</b> ({pct_txt})\n"
            f"{sep}\n"
            f"Mercado: {market_name}  -  <b>#{market_id}</b>\n"
            f"⚠️ <i>{detail or 'Sem detalhe.'}</i>"
        )
    send_telegram(msg, market_key=mk)


def _automation_health_payload(cfg: dict, meta: dict, mk: str, cfg_map: dict | None = None) -> dict:
    cfg_map = cfg_map if isinstance(cfg_map, dict) else automation_cfg
    wagered = _automation_wagered_summary(mk)
    api_key_ok = bool(str(cfg.get("api_key") or "").strip())
    private_key_ok = bool(str(cfg.get("private_key") or "").strip())
    response_payload = cfg.get("last_order_response") or {}
    status_code = response_payload.get("status")
    try:
        status_code = int(status_code) if status_code is not None else None
    except (TypeError, ValueError):
        status_code = None

    balance_known = cfg.get("last_balance_at") is not None and cfg.get("balance") is not None
    last_message = str(cfg.get("last_order_message") or "").lower()
    response_message = str(response_payload.get("message") or "").lower()
    auth_invalid = (
        status_code in (401, 403)
        or "authorization" in last_message
        or "unauthor" in last_message
        or "authorization" in response_message
        or "unauthor" in response_message
    )

    if api_key_ok and private_key_ok and (balance_known or (status_code and 200 <= status_code < 300)):
        api_auth_state = "valid"
    elif api_key_ok and private_key_ok and auth_invalid:
        api_auth_state = "invalid"
    else:
        api_auth_state = "pending"

    def market_runtime(market_key: str) -> str:
        market_cfg = dict(cfg_map.get(market_key) or _default_automation_cfg())
        if not bool(market_cfg.get("enabled")):
            return "desligado"
        if not bool(meta.get("running")):
            return "parado"
        if (
            not str(market_cfg.get("api_key") or "").strip()
            or not str(market_cfg.get("private_key") or "").strip()
        ):
            return "faltando credenciais"
        return "pronto"

    return {
        "api_key_configured": api_key_ok,
        "private_key_configured": private_key_ok,
        "api_auth_state": api_auth_state,
        "balance_loaded": balance_known,
        "balance_positive": (_as_float(cfg.get("balance")) or 0.0) > 0,
        "running": bool(meta.get("running")),
        "market_enabled": bool(cfg.get("enabled")),
        "safe_mode_enabled": bool(cfg.get("safe_mode_enabled")),
        "safe_min_confidence": cfg.get("safe_min_confidence") or 70.0,
        "sniper_enabled": bool(cfg.get("sniper_enabled")),
        "sniper_min_multiplier": cfg.get("sniper_min_multiplier") or 3.0,
        "arbitrage_enabled": bool(cfg.get("arbitrage_enabled")),
        "arbitrage_max_sum": cfg.get("arbitrage_max_sum") or AUTOMATION_ARB_DEFAULT_MAX_SUM,
        "cashout_enabled": bool(cfg.get("cashout_enabled")),
        "cashout_profit_pct": cfg.get("cashout_profit_pct") or 20.0,
        "execution_profile_label": _execution_profile_label(meta.get("execution_profile_mode")),
        "signal_min_confidence": _market_signal_min_confidence(mk),
        "rodovia_enabled": bool((cfg_map.get("rodovia") or {}).get("enabled")),
        "rua_enabled": bool((cfg_map.get("rua") or {}).get("enabled")),
        "rodovia_runtime": market_runtime("rodovia"),
        "rua_runtime": market_runtime("rua"),
        "market_name": MARKET_CONFIGS.get(mk, {}).get("name") or mk.capitalize(),
        "wagered_amount": wagered.get("amount") or 0.0,
        "wagered_orders": wagered.get("orders") or 0,
    }


def _automation_api_request(
    cfg: dict,
    path: str,
    *,
    method: str = "GET",
    body: dict | None = None,
    timeout: float = 15,
) -> dict:
    headers = {
        "Accept": "application/json",
        "User-Agent": UA_HEADER,
    }
    api_key = str(cfg.get("api_key") or "").strip()
    private_key = str(cfg.get("private_key") or "").strip()
    if api_key and private_key:
        token = base64.b64encode(f"{api_key}:{private_key}".encode("utf-8")).decode("ascii")
        headers["Authorization"] = f"Bearer {token}"

    data = None
    if body is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(body).encode("utf-8")

    last_error = None
    for base_url in _automation_api_base_urls():
        req = urllib.request.Request(
            f"{base_url}{path}",
            data=data,
            headers=headers,
            method=method,
        )
        try:
            with _urlopen_no_proxy(req, timeout=timeout) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
                try:
                    payload = json.loads(raw) if raw else {"success": True}
                except json.JSONDecodeError:
                    return {
                        "success": False,
                        "message": raw or "Resposta invalida da API.",
                        "status": resp.status,
                    }
                if not isinstance(payload, dict):
                    payload = {"success": bool(200 <= resp.status < 300), "data": payload}
                payload.setdefault("success", bool(200 <= resp.status < 300))
                payload.setdefault("status", resp.status)
                return payload
        except urllib.error.HTTPError as ex:
            raw = ex.read().decode("utf-8", errors="replace")
            payload: dict[str, object]
            try:
                payload = json.loads(raw) if raw else {}
            except json.JSONDecodeError:
                payload = {"message": raw or str(ex)}
            if not isinstance(payload, dict):
                payload = {"message": raw or str(ex)}
            payload["success"] = bool(payload.get("success", False))
            payload["status"] = payload.get("status", ex.code)
            payload["message"] = str(payload.get("message") or str(ex))
            return payload
        except urllib.error.URLError as ex:
            last_error = ex
            reason = getattr(ex, "reason", None)
            errno = getattr(reason, "errno", None)
            lowered = str(ex).lower()
            is_name_error = errno in {11001, -2} or "getaddrinfo failed" in lowered or "name or service not known" in lowered
            if is_name_error:
                continue
            return {"success": False, "message": str(ex)}
        except Exception as ex:
            last_error = ex
            return {"success": False, "message": str(ex)}

    if last_error is not None:
        return {
            "success": False,
            "message": (
                "Falha ao resolver o host da API de saldo. "
                "Confira DNS/rede ou configure ODDSYNC_ORDERBOOK_API_BASE_URL com um host valido."
            ),
        }
    return {"success": False, "message": "Falha ao conectar na API."}


def _automation_parse_balance_value(
    payload: dict,
    currency: str = "BRLX",
) -> tuple[float | None, str | None]:
    data = payload.get("data") if isinstance(payload, dict) else None
    wanted = str(currency or "BRLX").upper()

    def pick_amount(obj):
        if isinstance(obj, (int, float, str)):
            return _as_float(obj)
        if not isinstance(obj, dict):
            return None
        for key in (
            "available",
            "balance",
            "amount",
            "value",
            wanted,
            wanted.lower(),
            "BRLX",
            "brlx",
            "BRL",
            "brl",
        ):
            amount = _as_float(obj.get(key))
            if amount is not None:
                return amount
        for nested_key in ("wallet", "balance", "balances", "fiat"):
            nested_amount = pick_amount(obj.get(nested_key))
            if nested_amount is not None:
                return nested_amount
        return None

    if isinstance(data, list):
        preferred = None
        for item in data:
            if not isinstance(item, dict):
                amount = _as_float(item)
                if amount is not None:
                    return amount, wanted
                continue
            item_currency = str(
                item.get("currency") or item.get("code") or item.get("symbol") or ""
            ).upper()
            amount = pick_amount(item)
            if amount is None:
                continue
            if item_currency == wanted:
                return amount, item_currency
            if preferred is None:
                preferred = (amount, item_currency or wanted)
        if preferred is not None:
            return preferred

    direct = pick_amount(data)
    if direct is not None:
        return direct, wanted
    return None, None


def _automation_refresh_balance(mk: str) -> dict:
    request_raw = _load_automation_raw() if _has_session_license_context() else None
    with automation_lock:
        current_cfg_map = _load_automation_config(request_raw) if request_raw is not None else automation_cfg
        current_meta = _load_automation_meta(request_raw) if request_raw is not None else automation_meta
        cfg = dict(current_cfg_map.get(mk) or _default_automation_cfg())

    if not str(cfg.get("api_key") or "").strip():
        _set_automation_status(
            mk,
            "error",
            "Informe a API key antes de buscar saldo.",
        )
        return _automation_public_payload(mk)

    resp = _automation_api_request(cfg, "/balance", method="GET", timeout=6)
    amount, balance_currency = _automation_parse_balance_value(
        resp,
        cfg.get("currency") or "BRLX",
    )
    if resp.get("success") and amount is not None:
        last_status = str(cfg.get("last_order_status") or "").lower()
        last_message = str(cfg.get("last_order_message") or "")
        if amount <= 0:
            _set_automation_status(
                mk,
                "error",
                f"Sem saldo disponivel: {amount:.2f} {balance_currency or cfg.get('currency') or 'BRLX'}",
                balance=amount,
                response_payload={
                    "status": resp.get("status"),
                    "endpoint": "balance",
                },
            )
        else:
            if last_status in ("pending", "sent"):
                _automation_store_balance_only(
                    mk,
                    amount,
                    balance_currency or cfg.get("currency") or "BRLX",
                )
                latest_request_raw = _load_automation_raw() if _has_session_license_context() else None
                with automation_lock:
                    latest_cfg_map = (
                        _load_automation_config(latest_request_raw)
                        if latest_request_raw is not None else automation_cfg
                    )
                    latest_meta = (
                        _load_automation_meta(latest_request_raw)
                        if latest_request_raw is not None else automation_meta
                    )
                    live_cfg = latest_cfg_map.get(mk)
                    if not live_cfg:
                        live_cfg = _default_automation_cfg()
                        latest_cfg_map[mk] = live_cfg
                    live_cfg["last_order_status"] = last_status
                    if last_message:
                        live_cfg["last_order_message"] = last_message
                    live_cfg["last_order_response"] = {
                        "status": resp.get("status"),
                        "endpoint": "balance",
                        "preserved_order_status": last_status,
                    }
                    _save_automation_config(latest_cfg_map, latest_meta)
                _promote_runtime_automation_state_if_active(latest_cfg_map, latest_meta)
                _broadcast_automation(mk)
            else:
                latest_request_raw = _load_automation_raw() if _has_session_license_context() else None
                latest_meta = (
                    _load_automation_meta(latest_request_raw)
                    if latest_request_raw is not None else automation_meta
                )
                _set_automation_status(
                    mk,
                    "ready" if latest_meta.get("running") else "idle",
                    f"Saldo atualizado: {amount:.2f} {balance_currency or cfg.get('currency') or 'BRLX'}",
                    balance=amount,
                    response_payload={
                        "status": resp.get("status"),
                        "endpoint": "balance",
                    },
                )
    else:
        _set_automation_status(
            mk,
            "error",
            (
                "Nao consegui ler saldo. "
                f"{resp.get('message') or 'Confira API key/private conforme a doc do orderbook.'}"
            ),
            response_payload={
                "status": resp.get("status"),
                "message": resp.get("message"),
                "endpoint": "balance",
            },
        )
    return _automation_public_payload(mk)


def _automation_refresh_balance_async(mk: str):
    license_key = _current_license_key()

    def _worker():
        previous_key = str(getattr(_thread_license_context, "license_key", "") or "")
        try:
            if license_key:
                _thread_license_context.license_key = license_key
            _automation_refresh_balance(mk)
        except Exception as ex:
            print(f"[AUTO:{mk}] erro refresh saldo async: {ex}", flush=True)
        finally:
            if previous_key:
                _thread_license_context.license_key = previous_key
            elif hasattr(_thread_license_context, "license_key"):
                delattr(_thread_license_context, "license_key")

    threading.Thread(target=_worker, daemon=True).start()


def _automation_calc_stake(cfg: dict) -> tuple[float | None, str | None]:
    stake_value = _as_float(cfg.get("stake_value"))
    if stake_value is None or stake_value <= 0:
        return None, "Valor de entrada invalido."

    if cfg.get("stake_mode") == "percent":
        balance = _as_float(cfg.get("balance"))
        if balance is None or balance <= 0:
            return None, "Sem saldo disponivel para calcular percentual da banca."
        amount = round(balance * stake_value / 100.0, 2)
    else:
        amount = round(stake_value, 2)

    if amount <= 0:
        return None, "Stake calculada ficou zerada."
    if amount < AUTOMATION_MIN_ORDER_TOTAL:
        return None, (
            f"Stake minima da API: {AUTOMATION_MIN_ORDER_TOTAL:.2f} "
            f"{cfg.get('currency') or 'BRLX'}."
        )

    balance = _as_float(cfg.get("balance"))
    if balance is not None and balance <= 0:
        return None, f"Sem saldo disponivel ({balance:.2f} {cfg.get('currency') or 'BRLX'})."
    if balance is not None and balance > 0 and balance < AUTOMATION_MIN_ORDER_TOTAL:
        return None, (
            f"Saldo abaixo do minimo da API: {balance:.2f} "
            f"{cfg.get('currency') or 'BRLX'} < {AUTOMATION_MIN_ORDER_TOTAL:.2f} "
            f"{cfg.get('currency') or 'BRLX'}."
        )
    if balance is not None and balance > 0 and amount > balance:
        return None, (
            f"Stake {amount:.2f} {cfg.get('currency') or 'BRLX'} "
            f"maior que saldo {balance:.2f} {cfg.get('currency') or 'BRLX'}."
        )

    return amount, None


def _automation_trade_ref(
    mk: str,
    rec: str,
    market_id=None,
) -> int | None:
    st = states[mk]
    if market_id and str(st.get("market_id") or "") != str(market_id):
        payload = _api_get(
            f"https://app.palpitano.com/app/getSpecificMarketStats?id={market_id}"
        )
        if payload and payload.get("data"):
            dd = payload["data"]
            selection_codes, selection_ids = _extract_selection_maps(dd)
            return selection_ids.get(rec)
        return None

    return (st.get("selection_ids") or {}).get(rec)


def _automation_pick_test_rec(
    mk: str,
    market_id=None,
) -> str | None:
    st = states[mk]
    selection_ids = dict(st.get("selection_ids") or {})
    if market_id and str(st.get("market_id") or "") != str(market_id):
        payload = _api_get(
            f"https://app.palpitano.com/app/getSpecificMarketStats?id={market_id}"
        )
        if payload and payload.get("data"):
            _, selection_ids = _extract_selection_maps(payload["data"])

    for side in ("MAIS", "ATE"):
        if selection_ids.get(side):
            return side
    for side in selection_ids:
        return side
    return None


def _automation_arbitrage_candidate(
    mk: str,
    market_id,
    total_budget: float,
    max_sum: float,
) -> dict | None:
    st = states[mk]
    if str(st.get("market_id") or "") != str(market_id or ""):
        return None

    remaining = _as_float(st.get("remaining_seconds"))
    if remaining is None:
        return None
    elapsed = max(0, MARKET_CONFIGS[mk]["duration"] - int(remaining))
    if elapsed > AUTOMATION_SNIPER_MAX_ELAPSED_SECONDS:
        return None

    selection_ids = dict(st.get("selection_ids") or {})
    more_id = selection_ids.get("MAIS")
    ate_id = selection_ids.get("ATE")
    if not more_id or not ate_id:
        return None

    more_ask = _automation_orderbook_top_ask(market_id, more_id)
    ate_ask = _automation_orderbook_top_ask(market_id, ate_id)
    if not more_ask or not ate_ask:
        return None

    price_sum = more_ask["price"] + ate_ask["price"]
    if price_sum <= 0 or price_sum > max_sum:
        return None

    min_share_for_limits = max(
        AUTOMATION_MIN_ORDER_TOTAL / more_ask["price"],
        AUTOMATION_MIN_ORDER_TOTAL / ate_ask["price"],
    )
    share_by_budget = total_budget / price_sum
    share_by_book = min(more_ask["quantity"], ate_ask["quantity"])
    share_amount = min(share_by_budget, share_by_book)
    share_amount = math.floor(share_amount * 100) / 100.0
    if share_amount <= 0 or share_amount + 1e-9 < min_share_for_limits:
        return None

    more_cost = round(share_amount * more_ask["price"], 2)
    ate_cost = round(share_amount * ate_ask["price"], 2)
    total_spend = round(more_cost + ate_cost, 2)
    locked_profit = round(share_amount - total_spend, 2)
    if total_spend <= 0 or locked_profit <= 0:
        return None

    return {
        "elapsed": elapsed,
        "share_amount": share_amount,
        "more": {
            "selection_id": int(more_id),
            "price": more_ask["price"],
            "display": more_ask["display"],
            "cost": more_cost,
        },
        "ate": {
            "selection_id": int(ate_id),
            "price": ate_ask["price"],
            "display": ate_ask["display"],
            "cost": ate_cost,
        },
        "sum_price": round(price_sum, 4),
        "total_spend": total_spend,
        "locked_profit": locked_profit,
        "locked_profit_pct": round((locked_profit / total_spend) * 100.0, 2),
    }


def _automation_place_limit_buy(
    cfg: dict,
    *,
    selection_id: int,
    price: float,
    share_amount: float,
) -> dict:
    return _automation_api_request(
        cfg,
        "/orders",
        method="POST",
        body={
            "selectionId": int(selection_id),
            "side": "BUY",
            "type": "LIMIT",
            "price": f"{price:.2f}",
            "amount": f"{share_amount:.2f}",
        },
    )


def _automation_try_flatten_limit_buy(
    cfg: dict,
    *,
    selection_id: int,
    share_amount: float,
) -> dict:
    return _automation_api_request(
        cfg,
        "/orders",
        method="POST",
        body={
            "selectionId": int(selection_id),
            "side": "SELL",
            "type": "MARKET",
            "amount": f"{share_amount:.2f}",
        },
    )


def _automation_submit_order(
    mk: str,
    market_id,
    rec: str,
    kind: str,
    amount: float,
    *,
    conf_pct: int | None = None,
    dedupe_market: bool = True,
) -> bool:
    if not market_id or not rec:
        return False
    if not _license_is_active():
        detail = _license_error_message()
        _set_automation_status(
            mk,
            "error",
            detail,
            market_id=market_id,
            side=rec,
            kind=kind,
            amount=amount,
        )
        return False

    request_raw = _load_automation_raw() if _has_session_license_context() else None
    with automation_lock:
        current_cfg_map = _load_automation_config(request_raw) if request_raw is not None else automation_cfg
        cfg = dict(current_cfg_map.get(mk) or _default_automation_cfg())

    current_key = _current_license_key()
    duplicate_owner = _running_license_with_same_api_credentials(current_key, mk, cfg)
    if duplicate_owner:
        other_key, other_customer = duplicate_owner
        owner_label = other_customer or _masked_license_key(other_key)
        detail = (
            f"Credenciais da API ja estao em uso por outra licenca ativa ({owner_label} / {_masked_license_key(other_key)}). "
            "Bloqueei a ordem para evitar aposta duplicada na mesma conta."
        )
        _set_automation_status(
            mk,
            "error",
            detail,
            market_id=market_id,
            side=rec,
            kind=kind,
            amount=amount,
        )
        _send_automation_order_telegram(
            mk,
            status="error",
            market_id=market_id or "--",
            rec=rec,
            kind=kind,
            amount=amount,
            conf_pct=conf_pct,
            detail=detail,
        )
        return False

    if not _mark_order_attempt_if_new(cfg, mk, market_id, rec, kind):
        detail = (
            "Tentativa duplicada bloqueada para a mesma conta/API nesta rodada. "
            "Ignorei o segundo disparo para evitar aposta repetida."
        )
        _set_automation_status(
            mk,
            "error",
            detail,
            market_id=market_id,
            side=rec,
            kind=kind,
            amount=amount,
        )
        _send_automation_order_telegram(
            mk,
            status="error",
            market_id=market_id or "--",
            rec=rec,
            kind=kind,
            amount=amount,
            conf_pct=conf_pct,
            detail=detail,
        )
        return False

    if (
        not str(cfg.get("api_key") or "").strip()
        or not str(cfg.get("private_key") or "").strip()
    ):
        _clear_order_attempt(cfg, mk, market_id, rec, kind)
        detail = "Automacao ligada, mas API key/private nao foram preenchidas."
        _set_automation_status(
            mk,
            "error",
            detail,
            market_id=market_id,
            side=rec,
            kind=kind,
            amount=amount,
        )
        _send_automation_order_telegram(
            mk,
            status="error",
            market_id=market_id or "--",
            rec=rec,
            kind=kind,
            amount=amount,
            conf_pct=conf_pct,
            detail=detail,
        )
        return False

    selection_id = _automation_trade_ref(mk, rec, market_id)
    if not selection_id:
        _clear_order_attempt(cfg, mk, market_id, rec, kind)
        detail = "Nao achei selectionId da rodada atual para enviar ordem."
        _set_automation_status(
            mk,
            "error",
            detail,
            market_id=market_id,
            side=rec,
            kind=kind,
            amount=amount,
        )
        _send_automation_order_telegram(
            mk,
            status="error",
            market_id=market_id or "--",
            rec=rec,
            kind=kind,
            amount=amount,
            conf_pct=conf_pct,
            detail=detail,
        )
        return False

    sniper_min_multiplier = max(
        1.5,
        _as_float(cfg.get("sniper_min_multiplier")) or 3.0,
    )
    sniper_max_price = min(
        AUTOMATION_SNIPER_MAX_PRICE,
        round(1.0 / sniper_min_multiplier, 4),
    )
    sniper_enabled = bool(cfg.get("sniper_enabled"))
    sniper = None
    if sniper_enabled:
        sniper = _automation_sniper_candidate(
            mk,
            market_id,
            selection_id,
            amount,
            max_price=sniper_max_price,
        )
    sniper_target_txt = f"{sniper_min_multiplier:.1f}x+"

    # Faixa efetiva de entrada (hard limit global): 1.20x ate 1.55x.
    min_entry_multiplier = max(
        1.01,
        _as_float(cfg.get("odd_min")) or _as_float(AUTOMATION_MIN_ENTRY_MULTIPLIER) or 1.10,
    )
    max_entry_multiplier = _as_float(cfg.get("odd_max")) or _as_float(AUTOMATION_MAX_ENTRY_MULTIPLIER) or 1.55
    min_entry_multiplier = max(min_entry_multiplier, _as_float(AUTOMATION_MIN_ENTRY_MULTIPLIER) or 1.20)
    max_entry_multiplier = min(max_entry_multiplier, _as_float(AUTOMATION_MAX_ENTRY_MULTIPLIER) or 1.55)
    if max_entry_multiplier < min_entry_multiplier:
        max_entry_multiplier = min_entry_multiplier
    max_entry_price = round(1.0 / min_entry_multiplier, 4)
    min_entry_price = round(1.0 / max_entry_multiplier, 4)
    protected_ask = _automation_orderbook_top_ask(market_id, selection_id)
    if not protected_ask:
        # Fallback 1: usa preco ja guardado no estado
        _state_odds = states[mk].get("odds") or {}
        _raw_odd = _as_float(_state_odds.get(rec))
        if not _raw_odd:
            # Fallback 2: busca direto na API
            _live_odds = _fetch_market_odds(market_id)
            _raw_odd = _live_odds.get(rec)
        if _raw_odd and _raw_odd > 1.0:
            _fb_price = round(1.0 / _raw_odd, 4)
            protected_ask = {
                "price": _fb_price,
                "display": _format_order_odd_display(_fb_price) or _format_odd_value(_fb_price),
                "quantity": 0,
                "notional": 0,
            }
            print(f"[AUTO:{mk}] orderbook vazio, usando odd de estado {_raw_odd:.2f}x como fallback", flush=True)
        else:
            # Fallback 3: orderbook vazio e sem odd conhecida — agenda limit no preco maximo
            min_entry_price_fb = round(1.0 / (max_entry_multiplier or 1.65), 4)
            protected_ask = {
                "price": min_entry_price_fb,
                "display": _format_order_odd_display(min_entry_price_fb) or _format_odd_value(min_entry_price_fb),
                "quantity": 0,
                "notional": 0,
            }
            print(f"[AUTO:{mk}] orderbook vazio sem fallback, agendando limit a {max_entry_multiplier:.2f}x market={market_id}", flush=True)
    if not protected_ask:
        protected_ask = {
            "price": max_entry_price,
            "display": _format_order_odd_display(max_entry_price) or _format_odd_value(max_entry_price),
            "quantity": 0,
            "notional": 0,
        }

    if sniper:
        limit_order_price = sniper["price"]
    else:
        # Escolhe odd-alvo aleatoria na faixa configurada, evitando ficar preso no minimo.
        target_multiplier = round(random.uniform(min_entry_multiplier, max_entry_multiplier), 2)
        if target_multiplier < min_entry_multiplier:
            target_multiplier = min_entry_multiplier
        if target_multiplier > max_entry_multiplier:
            target_multiplier = max_entry_multiplier
        limit_order_price = round(1.0 / target_multiplier, 4)

    entry_multiplier = _odd_multiplier_num(limit_order_price)
    if entry_multiplier is not None and entry_multiplier < min_entry_multiplier:
        _clear_order_attempt(cfg, mk, market_id, rec, kind)
        detail = (
            f"Sinal ignorado: odd {entry_multiplier:.2f}x abaixo da minima {min_entry_multiplier:.2f}x."
        )
        _set_automation_status(
            mk,
            "ready",
            detail,
            market_id=market_id,
            side=rec,
            kind=kind,
            amount=amount,
            odd=_format_order_odd_display(limit_order_price),
        )
        return False
    if entry_multiplier is not None and entry_multiplier > max_entry_multiplier:
        _clear_order_attempt(cfg, mk, market_id, rec, kind)
        detail = (
            f"Sinal ignorado: odd {entry_multiplier:.2f}x acima da maxima {max_entry_multiplier:.2f}x."
        )
        _set_automation_status(
            mk,
            "ready",
            detail,
            market_id=market_id,
            side=rec,
            kind=kind,
            amount=amount,
            odd=_format_order_odd_display(limit_order_price),
        )
        return False

    share_amount = round(
        amount / limit_order_price,
        2,
    )
    if share_amount <= 0:
        _clear_order_attempt(cfg, mk, market_id, rec, kind)
        _set_automation_status(
            mk,
            "error",
            "Nao foi possivel calcular a quantidade da ordem automatica.",
            market_id=market_id,
            side=rec,
            kind=kind,
            amount=amount,
        )
        return False

    order_body = {
        "selectionId": int(selection_id),
        "side": "BUY",
        "type": "LIMIT",
        "price": f"{limit_order_price:.2f}",
        "amount": f"{share_amount:.2f}",
    }
    execution_detail = (
        f"topo real do book {protected_ask['display']} | alvo aleatorio {entry_multiplier:.2f}x"
        if (not sniper and protected_ask)
        else f"limit a {limit_order_price:.2f}"
    )
    if sniper:
        order_body = {
            "selectionId": int(selection_id),
            "side": "BUY",
            "type": "LIMIT",
            "price": f"{sniper['price']:.2f}",
            "amount": f"{sniper['share_amount']:.2f}",
        }
        execution_detail = (
            f"sniper orderbook no inicio ({sniper['elapsed']}s, "
            f"pegando {sniper['price_display']} | alvo {sniper_target_txt})"
        )

    resp = _automation_api_request(
        cfg,
        "/orders",
        method="POST",
        body=order_body,
    )
    total_amount = round(amount, 2)
    used_sniper = bool(sniper and resp.get("success"))
    if sniper and not resp.get("success"):
        execution_detail = (
            f"sniper falhou sem fallback. "
            f"Topo visto: {sniper['price_display']}"
        )

    currency = cfg.get("currency") or "BRLX"
    live_signal_price = _fetch_signal_odd(market_id, rec)
    order_price_num = (
        _automation_extract_order_price_num(resp)
        or (sniper["price"] if used_sniper and sniper else None)
        or (protected_ask["price"] if protected_ask else None)
        or (limit_order_price if not sniper else None)
        or _as_float(live_signal_price)
    )
    order_odd = (
        _automation_extract_order_odd(resp)
        or (sniper["price_display"] if used_sniper and sniper else None)
        or (protected_ask["display"] if protected_ask else None)
        or _format_order_odd_display(limit_order_price if not sniper else None)
        or _format_order_odd_display(live_signal_price)
    )
    expected_profit = _expected_win_profit(total_amount, order_price_num)
    expected_profit_txt = (
        f"{expected_profit:.2f} {currency}"
        if expected_profit is not None else
        f"-- {currency}"
    )
    conf_txt = f" | {conf_pct}%" if conf_pct is not None else ""

    if resp.get("success"):
        if dedupe_market:
            _runtime_bucket(_auto_order_mid)[mk] = str(market_id)
        _send_automation_order_telegram(
            mk,
            status="accepted",
            market_id=market_id,
            rec=rec,
            kind=kind,
            amount=total_amount,
            odd=order_odd,
            conf_pct=conf_pct,
            detail=execution_detail,
        )
        confirmed_pos = None
        for _ in range(4):
            confirmed_pos = _automation_fetch_open_position(cfg, market_id, selection_id)
            if confirmed_pos:
                break
            time.sleep(0.7)
        open_position = _automation_build_open_position(
            mk,
            market_id=market_id,
            selection_id=selection_id,
            rec=rec,
            kind=kind,
            amount=total_amount,
            price_num=order_price_num,
            odd_display=order_odd,
            conf_pct=conf_pct,
            cfg=cfg,
            position_data=confirmed_pos,
        )
        if not open_position:
            pending_detail = (
                "API aceitou a ordem, mas a confirmacao da posicao esta atrasada. "
                "Vou acompanhar em background."
            )
            _automation_order_log_add(
                mk,
                market_id=market_id,
                status="pending",
                rec=rec,
                kind=kind,
                amount=total_amount,
                currency=currency,
                odd=order_odd,
                conf_pct=conf_pct,
                detail=pending_detail,
                api_status=resp.get("status"),
                execution=execution_detail,
                accepted=True,
                confirmed=False,
            )
            _set_automation_status(
                mk,
                "pending",
                pending_detail,
                market_id=market_id,
                side=rec,
                kind=kind,
                amount=total_amount,
                odd=order_odd,
                response_payload={
                    "status": resp.get("status"),
                    "endpoint": "orders",
                    "odd": order_odd,
                    "expected_profit": expected_profit,
                    "execution": execution_detail,
                    "message": str(resp.get("message") or ""),
                },
            )
            _automation_confirm_position_async(
                mk,
                market_id=market_id,
                selection_id=selection_id,
                rec=rec,
                kind=kind,
                total_amount=total_amount,
                order_price_num=order_price_num,
                order_odd=order_odd,
                conf_pct=conf_pct,
                execution_detail=execution_detail,
                currency=currency,
                response_status=resp.get("status"),
                license_key=_current_license_key(),
            )
            print(
                f"[AUTO:{mk}] ordem pendente de confirmacao market={market_id} "
                f"{rec} {total_amount:.2f} {currency} | resposta={resp}",
                flush=True,
            )
            return True

        _automation_set_open_position(mk, open_position)
        order_odd = open_position.get("entry_odd") or order_odd
        executed_amount = _as_float(open_position.get("invested_total")) or total_amount
        expected_profit = open_position.get("expected_profit")
        expected_profit_txt = (
            f"{expected_profit:.2f} {currency}"
            if expected_profit is not None else
            f"-- {currency}"
        )
        success_message = (
            f"Ordem automatica enviada: {kind} {rec} | "
            f"{executed_amount:.2f} {currency} | odd {order_odd or '--'} | "
            f"lucro esp. {expected_profit_txt}{conf_txt} | "
            f"{execution_detail}"
        )
        _automation_order_log_add(
            mk,
            market_id=market_id,
            status="sent",
            rec=rec,
            kind=kind,
            amount=executed_amount,
            currency=currency,
            odd=order_odd,
            conf_pct=conf_pct,
            detail=success_message,
            api_status=resp.get("status"),
            execution=execution_detail,
            accepted=True,
            confirmed=True,
        )
        _set_automation_status(
            mk,
            "sent",
            success_message,
            market_id=market_id,
            side=rec,
            kind=kind,
            amount=executed_amount,
            odd=order_odd,
            response_payload={
                "status": resp.get("status"),
                "endpoint": "orders",
                "odd": order_odd,
                "expected_profit": expected_profit,
                "executed_amount": executed_amount,
                "execution": execution_detail,
            },
        )
        _send_automation_order_telegram(
            mk,
            status="sent",
            market_id=market_id,
            rec=rec,
            kind=kind,
            amount=executed_amount,
            odd=order_odd,
            expected_profit=expected_profit,
            conf_pct=conf_pct,
            detail=execution_detail,
        )
        _automation_refresh_balance_async(mk)
        _set_automation_status(
            mk,
            "sent",
            success_message,
            market_id=market_id,
            side=rec,
            kind=kind,
            amount=executed_amount,
            odd=order_odd,
            response_payload={
                "status": resp.get("status"),
                "endpoint": "orders",
                "odd": order_odd,
                "expected_profit": expected_profit,
                "executed_amount": executed_amount,
                "execution": execution_detail,
            },
        )
        print(
            f"[AUTO:{mk}] ordem enviada market={market_id} "
            f"{rec} {executed_amount:.2f} {currency} odd={order_odd or '--'} "
            f"conf={conf_pct or '--'} exec={execution_detail}",
            flush=True,
        )
        _analytics_mark_bet_placed(mk, market_id)
        return True

    _clear_order_attempt(cfg, mk, market_id, rec, kind)
    _set_automation_status(
        mk,
        "error",
        (
            "Falha ao enviar ordem automatica. "
            f"{resp.get('message') or 'Confirme o formato de auth da API.'}"
        ),
        market_id=market_id,
        side=rec,
        kind=kind,
        amount=amount,
        odd=order_odd,
        response_payload={
            "status": resp.get("status"),
            "message": resp.get("message"),
            "endpoint": "orders",
            "odd": order_odd,
            "execution": execution_detail,
        },
    )
    _automation_order_log_add(
        mk,
        market_id=market_id,
        status="error",
        rec=rec,
        kind=kind,
        amount=amount,
        currency=cfg.get("currency") or "BRLX",
        odd=order_odd,
        conf_pct=conf_pct,
        detail=str(resp.get("message") or "Falha ao enviar ordem automatica."),
        api_status=resp.get("status"),
        execution=execution_detail,
        accepted=False,
        confirmed=False,
    )
    _send_automation_order_telegram(
        mk,
        status="error",
        market_id=market_id,
        rec=rec,
        kind=kind,
        amount=amount,
        odd=order_odd,
        conf_pct=conf_pct,
        detail=(
            f"{resp.get('message') or 'Falha sem detalhe da API.'} | "
            f"{execution_detail}"
        ),
    )
    print(
        f"[AUTO:{mk}] falha ao enviar ordem market={market_id}: {resp}",
        flush=True,
    )
    return False


def _automation_execute_signal(
    mk: str,
    market_id,
    rec: str,
    kind: str,
    conf_pct: int,
    license_key: str | None = None,
):
    with _license_thread_context(license_key):
        if not market_id or not rec:
            return

        def _release_auto_mid():
            with automation_lock:
                auto_order_mid = _runtime_bucket(_auto_order_mid)
                if str(auto_order_mid.get(mk) or "") == str(market_id):
                    auto_order_mid[mk] = None

        request_raw = _load_automation_raw() if _has_session_license_context() else None
        with automation_lock:
            current_cfg_map = _load_automation_config(request_raw) if request_raw is not None else automation_cfg
            current_meta = _load_automation_meta(request_raw) if request_raw is not None else automation_meta
            cfg = dict(current_cfg_map.get(mk) or _default_automation_cfg())
            if not current_meta.get("running"):
                return
            effective_enabled_markets = _automation_effective_enabled_markets(current_cfg_map, current_meta)
            if not effective_enabled_markets.get(mk, bool(cfg.get("enabled"))):
                plan = _automation_dual_market_plan(current_cfg_map)
                plan_label = str(plan.get("label") or "Sem leitura")
                detail = (
                    f"Entrada pulada: selecao de mercado no modo semaforo. "
                    f"Decisao atual: {plan_label}. {MARKET_CONFIGS.get(mk, {}).get('name') or mk} ficou fora nesta rodada."
                )
                _set_automation_status(
                    mk,
                    "idle",
                    detail,
                    market_id=market_id,
                    side=rec,
                    kind=kind,
                )
                _automation_order_log_add(
                    mk,
                    market_id=market_id,
                    status="blocked",
                    rec=rec,
                    kind=kind,
                    amount=None,
                    currency=str(cfg.get("currency") or "BRLX"),
                    conf_pct=conf_pct,
                    detail=detail,
                    execution=f"market_selection:{plan.get('mode') or 'none'}",
                    accepted=False,
                    confirmed=False,
                )
                _analytics_add_blocked(
                    mk,
                    market_id,
                    rec,
                    kind,
                    conf_pct,
                    None,
                    "Selecao de mercado pelo semaforo",
                    detail,
                )
                _broadcast_automation(mk)
                return
            auto_order_mid = _runtime_bucket(_auto_order_mid)
            if str(auto_order_mid.get(mk) or "") == str(market_id):
                return

        if _automation_uses_semaforo(current_meta):
            gate = _automation_operational_gate(mk, rec, conf_pct, cfg)
        else:
            gate = {"blocked": False}
        if gate.get("blocked"):
            detail = (
                f"Semaforo operacional bloqueou a entrada: {gate.get('label')} | "
                f"{gate.get('summary')}"
            )
            _set_automation_status(
                mk,
                "idle",
                detail,
                market_id=market_id,
                side=rec,
                kind=kind,
            )
            _automation_order_log_add(
                mk,
                market_id=market_id,
                status="blocked",
                rec=rec,
                kind=kind,
                amount=None,
                currency=str(cfg.get("currency") or "BRLX"),
                conf_pct=conf_pct,
                detail=detail,
                execution=gate.get("reason"),
                accepted=False,
                confirmed=False,
            )
            _analytics_add_blocked(
                mk,
                market_id,
                rec,
                kind,
                conf_pct,
                None,
                f"Semaforo horario ({gate.get('label')})",
                gate.get("reason") or detail,
            )
            _broadcast_automation(mk)
            return

        with automation_lock:
            auto_order_mid = _runtime_bucket(_auto_order_mid)
            if str(auto_order_mid.get(mk) or "") == str(market_id):
                return
            auto_order_mid[mk] = str(market_id)

        # Bloqueios de stop e guarda de ritmo desativados por solicitacao.
        if (
            not str(cfg.get("api_key") or "").strip()
            or not str(cfg.get("private_key") or "").strip()
        ):
            detail = "Automacao ligada, mas API key/private nao foram preenchidas."
            _set_automation_status(
                mk,
                "error",
                detail,
                market_id=market_id,
                side=rec,
                kind=kind,
            )
            _send_automation_order_telegram(
                mk,
                status="error",
                market_id=market_id or "--",
                rec=rec,
                kind=kind,
                amount=None,
                conf_pct=conf_pct,
                detail=detail,
            )
            _release_auto_mid()
            return

        if cfg.get("stake_mode") == "percent" and _as_float(cfg.get("balance")) is None:
            _automation_refresh_balance(mk)
            latest_request_raw = _load_automation_raw() if _has_session_license_context() else None
            with automation_lock:
                latest_cfg_map = _load_automation_config(latest_request_raw) if latest_request_raw is not None else automation_cfg
                cfg = dict(latest_cfg_map.get(mk) or _default_automation_cfg())

        amount, amount_error = _automation_calc_stake(cfg)
        if amount is None:
            detail = amount_error or "Nao foi possivel calcular stake."
            _set_automation_status(
                mk,
                "error",
                detail,
                market_id=market_id,
                side=rec,
                kind=kind,
            )
            _send_automation_order_telegram(
                mk,
                status="error",
                market_id=market_id or "--",
                rec=rec,
                kind=kind,
                amount=None,
                conf_pct=conf_pct,
                detail=detail,
            )
            _release_auto_mid()
            return

        success = _automation_submit_order(
            mk,
            market_id,
            rec,
            kind,
            amount,
            conf_pct=conf_pct,
            dedupe_market=True,
        )
        if not success:
            _release_auto_mid()

def _reload_private_runtime_state():
    global telegram_cfg, telegram_score, balance_timeline_data, automation_cfg, automation_meta

    with _pending_signal_lock:
        _sent_signal_mid.clear()
        _sent_signal_rec.clear()
        _sent_signal_kind.clear()
        _sent_signal_conf.clear()
        _sent_signal_odd.clear()
        _sent_signal_strategy.clear()

    with telegram_lock:
        telegram_cfg = _load_telegram_config()
        telegram_score = _load_score()
        _load_pending_signal_cache()

    with balance_timeline_lock:
        balance_timeline_data = _load_balance_timeline()

    with automation_lock:
        _automation_raw = _load_automation_raw()
        automation_cfg = _load_automation_config(_automation_raw)
        automation_meta = _load_automation_meta(_automation_raw)

    _load_analytics()
    _load_automation_order_log()
    if "broadcast" in globals():
        _broadcast_all_telegram()
        _broadcast_all_automation()


_reload_private_runtime_state()


def _automation_try_arbitrage(mk: str):
    request_raw = _load_automation_raw() if _has_session_license_context() else None
    with automation_lock:
        current_cfg_map = _load_automation_config(request_raw) if request_raw is not None else automation_cfg
        current_meta = _load_automation_meta(request_raw) if request_raw is not None else automation_meta
        cfg = dict(current_cfg_map.get(mk) or _default_automation_cfg())

    if not current_meta.get("running"):
        return
    if not cfg.get("enabled") or not cfg.get("arbitrage_enabled"):
        return
    if (
        not str(cfg.get("api_key") or "").strip()
        or not str(cfg.get("private_key") or "").strip()
    ):
        return
    if isinstance(cfg.get("open_position"), dict):
        return

    market_id = states[mk].get("market_id")
    if not market_id:
        return
    if str(_runtime_bucket(_auto_order_mid).get(mk) or "") == str(market_id):
        return

    now_ts = time.time()
    auto_arb_last_try_ts = _runtime_bucket(_auto_arb_last_try_ts)
    if now_ts - auto_arb_last_try_ts.get(mk, 0.0) < AUTOMATION_ARB_CHECK_SECONDS:
        return
    auto_arb_last_try_ts[mk] = now_ts

    if cfg.get("stake_mode") == "percent" and _as_float(cfg.get("balance")) is None:
        _automation_refresh_balance(mk)
    request_raw = _load_automation_raw() if _has_session_license_context() else None
    with automation_lock:
        current_cfg_map = _load_automation_config(request_raw) if request_raw is not None else automation_cfg
        cfg = dict(current_cfg_map.get(mk) or _default_automation_cfg())

    current_key = _current_license_key()
    duplicate_owner = _running_license_with_same_api_credentials(current_key, mk, cfg)
    if duplicate_owner:
        other_key, other_customer = duplicate_owner
        owner_label = other_customer or _masked_license_key(other_key)
        detail = (
            f"Credenciais da API ja estao em uso por outra licenca ativa ({owner_label} / {_masked_license_key(other_key)}). "
            "Bloqueei a ordem para evitar aposta duplicada na mesma conta."
        )
        _set_automation_status(
            mk,
            "error",
            detail,
            market_id=market_id,
            side=rec,
            kind=kind,
            amount=amount,
        )
        _send_automation_order_telegram(
            mk,
            status="error",
            market_id=market_id or "--",
            rec=rec,
            kind=kind,
            amount=amount,
            conf_pct=conf_pct,
            detail=detail,
        )
        return False

    total_budget, budget_error = _automation_calc_stake(cfg)
    if total_budget is None:
        return

    candidate = _automation_arbitrage_candidate(
        mk,
        market_id,
        total_budget,
        min(
            0.99,
            max(
                0.50,
                _as_float(cfg.get("arbitrage_max_sum")) or AUTOMATION_ARB_DEFAULT_MAX_SUM,
            ),
        ),
    )
    if not candidate:
        return

    first_leg_key = "more" if candidate["more"]["price"] <= candidate["ate"]["price"] else "ate"
    second_leg_key = "ate" if first_leg_key == "more" else "more"
    first_leg = candidate[first_leg_key]
    second_leg = candidate[second_leg_key]

    first_resp = _automation_place_limit_buy(
        cfg,
        selection_id=first_leg["selection_id"],
        price=first_leg["price"],
        share_amount=candidate["share_amount"],
    )
    if not first_resp.get("success"):
        return

    second_resp = _automation_place_limit_buy(
        cfg,
        selection_id=second_leg["selection_id"],
        price=second_leg["price"],
        share_amount=candidate["share_amount"],
    )
    if not second_resp.get("success"):
        flatten_resp = _automation_try_flatten_limit_buy(
            cfg,
            selection_id=first_leg["selection_id"],
            share_amount=candidate["share_amount"],
        )
        message = (
            "Falha na 2a perna da arbitragem sniper. "
            f"1a perna executada e tentativa de zerar risco: "
            f"{flatten_resp.get('message') or 'sem detalhe'}"
        )
        odd_summary = (
            f"M {candidate['more']['display']} + A {candidate['ate']['display']} "
            f"(soma {candidate['sum_price']:.2f})"
        )
        _set_automation_status(
            mk,
            "error",
            message,
            market_id=market_id,
            side="MAIS+ATE",
            kind="ARBITRAGEM",
            amount=candidate["total_spend"],
            odd=odd_summary,
            response_payload={
                "arb": True,
                "first_leg": first_resp,
                "second_leg": second_resp,
                "flatten": flatten_resp,
                "sum_price": candidate["sum_price"],
                "locked_profit": candidate["locked_profit"],
            },
        )
        _send_automation_arbitrage_telegram(
            mk,
            status="error",
            market_id=market_id,
            total_spend=candidate["total_spend"],
            share_amount=candidate["share_amount"],
            more_odd=candidate["more"]["display"],
            ate_odd=candidate["ate"]["display"],
            sum_price=candidate["sum_price"],
            locked_profit=candidate["locked_profit"],
            locked_profit_pct=candidate["locked_profit_pct"],
            detail=message,
        )
        print(
            f"[AUTO:{mk}] arbitragem falhou market={market_id} "
            f"2a perna rejeitada: {second_resp}",
            flush=True,
        )
        return

    _runtime_bucket(_auto_order_mid)[mk] = str(market_id)
    odd_summary = (
        f"M {candidate['more']['display']} + A {candidate['ate']['display']} "
        f"(soma {candidate['sum_price']:.2f})"
    )
    message = (
        f"Arbitragem sniper enviada: soma {candidate['sum_price']:.2f} | "
        f"capital {candidate['total_spend']:.2f} {cfg.get('currency') or 'BRLX'} | "
        f"lucro travado {candidate['locked_profit']:.2f} "
        f"{cfg.get('currency') or 'BRLX'} ({candidate['locked_profit_pct']:.1f}%)"
    )
    _set_automation_status(
        mk,
        "sent",
        message,
        market_id=market_id,
        side="MAIS+ATE",
        kind="ARBITRAGEM",
        amount=candidate["total_spend"],
        odd=odd_summary,
        response_payload={
            "arb": True,
            "first_leg": first_resp,
            "second_leg": second_resp,
            "sum_price": candidate["sum_price"],
            "locked_profit": candidate["locked_profit"],
            "locked_profit_pct": candidate["locked_profit_pct"],
        },
    )
    _send_automation_arbitrage_telegram(
        mk,
        status="sent",
        market_id=market_id,
        total_spend=candidate["total_spend"],
        share_amount=candidate["share_amount"],
        more_odd=candidate["more"]["display"],
        ate_odd=candidate["ate"]["display"],
        sum_price=candidate["sum_price"],
        locked_profit=candidate["locked_profit"],
        locked_profit_pct=candidate["locked_profit_pct"],
        detail=f"arbitragem encontrada em {candidate['elapsed']}s de rodada",
    )
    _automation_refresh_balance_async(mk)
    _set_automation_status(
        mk,
        "sent",
        message,
        market_id=market_id,
        side="MAIS+ATE",
        kind="ARBITRAGEM",
        amount=candidate["total_spend"],
        odd=odd_summary,
        response_payload={
            "arb": True,
            "first_leg": first_resp,
            "second_leg": second_resp,
            "sum_price": candidate["sum_price"],
            "locked_profit": candidate["locked_profit"],
            "locked_profit_pct": candidate["locked_profit_pct"],
        },
    )
    print(
        f"[AUTO:{mk}] arbitragem enviada market={market_id} "
        f"soma={candidate['sum_price']:.2f} lucro={candidate['locked_profit']:.2f}",
        flush=True,
    )


def bankroll_goal_reminder_worker():
    while True:
        try:
            _check_day_reset()
            now_ts = time.time()
            for mk, cfg in MARKET_CONFIGS.items():
                score = telegram_score.get(mk) or {}
                wins = int(score.get("wins") or 0)
                losses = int(score.get("losses") or 0)
                net_score = wins - losses

                if net_score < TELEGRAM_DAILY_GOAL_NET_WINS:
                    continue

                if (
                    now_ts - _goal_reminder_ts.get(mk, 0)
                    < TELEGRAM_GOAL_REMINDER_INTERVAL_SECONDS
                ):
                    continue

                _goal_reminder_ts[mk] = now_ts
                total_sc = wins + losses
                acc_str = f"{int(wins / total_sc * 100)}%" if total_sc else "0%"
                msg = (
                    f"💰 <b>META DO DIA — {cfg['name']}</b>\n"
                    f"📊 Hoje: ✅ {wins} ❌ {losses} ({acc_str})\n"
                    f"\n"
                    f"<i>Se voce ja bateu sua meta hoje, considera parar "
                    f"e proteger o lucro. Nao devolve no overtrading.</i>"
                )
                send_telegram(msg, market_key=mk)
        except Exception as ex:
            print(f"[Telegram] erro lembrete de meta: {ex}", flush=True)

        time.sleep(30)


def result_telegram_fallback_worker():
    while True:
        try:
            for mk in MARKET_CONFIGS:
                with history_locks[mk]:
                    latest = dict(histories[mk][0]) if histories[mk] else None
                if not latest:
                    continue
                market_id = str(latest.get("market_id") or "")
                if not market_id or not _entry_result(latest):
                    continue
                if _telegram_result_notified_mid.get(mk) == market_id:
                    continue
                if _send_result_telegram(mk, latest):
                    print(
                        f"[Telegram:{mk}] resultado reenviado via fallback market={market_id}",
                        flush=True,
                    )
        except Exception as ex:
            print(f"[Telegram] erro fallback resultado: {ex}", flush=True)
        time.sleep(12)


def _automation_try_cashout(mk: str):
    request_raw = _load_automation_raw() if _has_session_license_context() else None
    with automation_lock:
        current_cfg_map = _load_automation_config(request_raw) if request_raw is not None else automation_cfg
        cfg = dict(current_cfg_map.get(mk) or _default_automation_cfg())

    open_position = cfg.get("open_position")
    if not isinstance(open_position, dict):
        return
    if not cfg.get("cashout_enabled"):
        return
    if (
        not str(cfg.get("api_key") or "").strip()
        or not str(cfg.get("private_key") or "").strip()
    ):
        return

    market_id = open_position.get("market_id")
    selection_id = open_position.get("selection_id")
    share_amount = _as_float(open_position.get("share_amount"))
    invested_total = _as_float(open_position.get("invested_total"))
    if not market_id or not selection_id or share_amount is None or share_amount <= 0:
        return
    if invested_total is None or invested_total <= 0:
        return

    position_live = _automation_fetch_open_position(cfg, market_id, selection_id)
    if position_live and _as_float(position_live.get("quantity")):
        share_amount = _as_float(position_live.get("quantity")) or share_amount

    top_bid = _automation_orderbook_top_bid(market_id, selection_id)
    if not top_bid:
        return

    payout_value = round(share_amount * top_bid["price"], 2)
    profit_value = round(payout_value - invested_total, 2)
    profit_pct = round((profit_value / invested_total) * 100.0, 2)
    target_pct = max(1.0, _as_float(cfg.get("cashout_profit_pct")) or 20.0)
    trigger_pct = max(0.0, target_pct - AUTOMATION_CASHOUT_NEAR_BUFFER_PCT)
    if profit_pct < trigger_pct:
        return

    now_ts = time.time()
    auto_cashout_last_try_ts = _runtime_bucket(_auto_cashout_last_try_ts)
    if now_ts - auto_cashout_last_try_ts.get(mk, 0.0) < AUTOMATION_CASHOUT_COOLDOWN_SECONDS:
        return
    auto_cashout_last_try_ts[mk] = now_ts

    amount_to_sell = round(share_amount, 2)
    if amount_to_sell <= 0:
        return

    resp = _automation_api_request(
        cfg,
        "/orders",
        method="POST",
        body={
            "selectionId": int(selection_id),
            "side": "SELL",
            "type": "MARKET",
            "amount": f"{amount_to_sell:.2f}",
        },
    )
    if resp.get("success"):
        message = (
            f"Cashout automatico enviado: {open_position.get('kind') or 'ABERTURA'} "
            f"{open_position.get('rec') or '--'} | retorno est. {payout_value:.2f} "
            f"{cfg.get('currency') or 'BRLX'} | lucro {profit_value:.2f} "
            f"{cfg.get('currency') or 'BRLX'} ({profit_pct:.1f}%)"
        )
        response_payload = {
            "status": resp.get("status"),
            "endpoint": "orders",
            "cashout": True,
            "payout_value": payout_value,
            "profit_value": profit_value,
            "profit_pct": profit_pct,
            "exit_odd": top_bid["display"],
        }
        _set_automation_status(
            mk,
            "sent",
            message,
            market_id=market_id,
            side=open_position.get("rec"),
            kind="CASHOUT",
            amount=payout_value,
            odd=top_bid["display"],
            response_payload=response_payload,
        )
        _send_automation_cashout_telegram(
            mk,
            status="sent",
            market_id=market_id,
            rec=open_position.get("rec"),
            sold_amount=invested_total,
            exit_odd=top_bid["display"],
            payout_value=payout_value,
            profit_value=profit_value,
            profit_pct=profit_pct,
            detail=(
                f"alvo {target_pct:.1f}% | disparo rapido em {trigger_pct:.1f}% "
                f"usando topo da bid"
            ),
        )
        _automation_set_open_position(mk, None)
        _automation_refresh_balance_async(mk)
        _set_automation_status(
            mk,
            "sent",
            message,
            market_id=market_id,
            side=open_position.get("rec"),
            kind="CASHOUT",
            amount=payout_value,
            odd=top_bid["display"],
            response_payload=response_payload,
        )
        print(
            f"[AUTO:{mk}] cashout enviado market={market_id} "
            f"retorno={payout_value:.2f} lucro={profit_value:.2f} "
            f"pct={profit_pct:.1f} odd={top_bid['display']}",
            flush=True,
        )
        return

    _set_automation_status(
        mk,
        "error",
        (
            "Falha ao enviar cashout automatico. "
            f"{resp.get('message') or 'Sem detalhe da API.'}"
        ),
        market_id=market_id,
        side=open_position.get("rec"),
        kind="CASHOUT",
        amount=payout_value,
        odd=top_bid["display"],
        response_payload={
            "status": resp.get("status"),
            "endpoint": "orders",
            "cashout": True,
            "message": resp.get("message"),
            "payout_value": payout_value,
            "profit_value": profit_value,
            "profit_pct": profit_pct,
            "exit_odd": top_bid["display"],
        },
    )
    _send_automation_cashout_telegram(
        mk,
        status="error",
        market_id=market_id,
        rec=open_position.get("rec"),
        sold_amount=invested_total,
        exit_odd=top_bid["display"],
        payout_value=payout_value,
        profit_value=profit_value,
        profit_pct=profit_pct,
        detail=resp.get("message") or "Sem detalhe da API.",
    )
    print(
        f"[AUTO:{mk}] falha no cashout market={market_id}: {resp}",
        flush=True,
    )


def automation_cashout_worker():
    while True:
        try:
            for license_key in _iter_running_automation_license_keys():
                for mk in MARKET_CONFIGS:
                    with _license_thread_context(license_key):
                        _automation_try_cashout(mk)
        except Exception as ex:
            print(f"[AUTO] erro cashout worker: {ex}", flush=True)
        time.sleep(AUTOMATION_CASHOUT_CHECK_SECONDS)


def automation_arbitrage_worker():
    while True:
        try:
            for license_key in _iter_running_automation_license_keys():
                for mk in MARKET_CONFIGS:
                    with _license_thread_context(license_key):
                        _automation_try_arbitrage(mk)
        except Exception as ex:
            print(f"[AUTO] erro arbitragem worker: {ex}", flush=True)
        time.sleep(AUTOMATION_ARB_CHECK_SECONDS)
# Dedup thread-safe: lock + timestamp por mercado
_settlement_locks = {k: threading.Lock() for k in MARKET_CONFIGS}
_create_locks     = {k: threading.Lock() for k in MARKET_CONFIGS}
last_settlement_ts = {k: 0.0 for k in MARKET_CONFIGS}
last_create_ts     = {k: 0.0 for k in MARKET_CONFIGS}

# Channel routing: channel_name -> market_key
channel_to_market: dict = {}
current_channels:  dict = {k: None for k in MARKET_CONFIGS}
channel_lock = threading.Lock()
ws_instance  = None


# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────
def _extract_location(title: str, description: str = "") -> str | None:
    """Extrai localização do campo description ou title."""
    for text in (description, title):
        if not text:
            continue
        for sep in ("•", "-"):
            idx = text.find(sep)
            if idx != -1:
                candidate = text[idx + 1:].split("\n")[0].strip().rstrip(".")
                if len(candidate) > 6 and ("KM" in candidate.upper() or "—" in candidate):
                    return candidate
    return None


def load_history(mk: str):
    path = MARKET_CONFIGS[mk]["history_file"]
    if not os.path.exists(path):
        return
    with history_locks[mk]:
        try:
            with open(path, "r", encoding="utf-8") as f:
                entries = json.load(f)
            histories[mk].clear()
            histories[mk].extend(entries)
            _normalize_history(mk)
            save_history(mk)
            print(f"[JSON:{mk}] {len(entries)} rodadas carregadas", flush=True)
        except Exception as ex:
            print(f"[JSON:{mk}] Erro: {ex}", flush=True)


def save_history(mk: str):
    with history_locks[mk]:
        try:
            _normalize_history(mk)
            path = MARKET_CONFIGS[mk]["history_file"]
            _atomic_json_write(path, list(histories[mk]))
        except Exception as ex:
            print(f"[JSON:{mk}] Erro ao salvar: {ex}", flush=True)


def broadcast(mk: str, event_type: str, data: dict, *, license_key: str | None = None):
    payload = f"event: {event_type}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"
    with sse_locks[mk]:
        dead = []
        target_key = str(license_key or "").strip().upper()
        for client in sse_clients[mk]:
            client_key = str((client or {}).get("license_key") or "").strip().upper()
            q = (client or {}).get("queue")
            if q is None:
                dead.append(client)
                continue
            if target_key and client_key and client_key != target_key:
                continue
            try:
                q.put_nowait(payload)
            except queue.Full:
                dead.append(client)
        for client in dead:
            if client in sse_clients[mk]:
                sse_clients[mk].remove(client)


def publish_history(mk: str):
    broadcast(mk, "history_loaded", {
        "history":      list(histories[mk]),
        "last_results": list(last_results[mk]),
        "history_count": len(histories[mk]),
        "history_target": HISTORY_TARGET_RECORDS,
    })


# ──────────────────────────────────────────────
# API helpers
# ──────────────────────────────────────────────
def _api_get(url: str) -> dict | None:
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA_HEADER})
        with _urlopen_no_proxy(req, timeout=10) as r:
            d = json.loads(r.read())
        return d if d.get("success") else None
    except urllib.error.URLError as ex:
        reason = getattr(ex, "reason", None)
        errno = getattr(reason, "errno", None)
        lowered = str(ex).lower()
        is_name_error = errno in {11001, -2} or "getaddrinfo failed" in lowered or "name or service not known" in lowered
        if not is_name_error:
            return None
        try:
            req = _build_host_fallback_request(
                url,
                headers={"User-Agent": UA_HEADER},
            )
            with _urlopen_no_proxy(req, timeout=10) as r:
                d = json.loads(r.read())
            return d if d.get("success") else None
        except Exception:
            return None
    except Exception:
        try:
            safe_url = str(url).replace("'", "''")
            cmd = [
                "powershell",
                "-Command",
                (
                    "$ProgressPreference='SilentlyContinue'; "
                    f"(Invoke-WebRequest -UseBasicParsing '{safe_url}').Content"
                ),
            ]
            res = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=15,
                check=False,
            )
            if res.returncode != 0 or not str(res.stdout).strip():
                return None
            d = json.loads(res.stdout)
            return d if d.get("success") else None
        except Exception:
            return None


def _extract_latest_market_price(payload_data: dict | None) -> float | None:
    if not isinstance(payload_data, dict):
        return None

    meta = payload_data.get("metadata") or {}
    decimals_raw = meta.get("fiatDecimals", 2)
    try:
        decimals = max(0, int(decimals_raw))
    except (TypeError, ValueError):
        decimals = 2

    for item in reversed(payload_data.get("graphData") or []):
        if not isinstance(item, dict):
            continue
        price_val = _as_float(item.get("price"))
        if price_val is not None:
            return round(price_val, decimals)

    for raw in (
        payload_data.get("currentPrice"),
        payload_data.get("price"),
        meta.get("currentPrice"),
        meta.get("price"),
    ):
        price_val = _as_float(raw)
        if price_val is not None:
            return round(price_val, decimals)

    return None


def get_market_data(slug_kw: str) -> dict | None:
    """Busca dados completos do mercado cujo slug contém slug_kw."""
    d = _api_get(MARKETS_API)
    if not d:
        return None
    for m in d["data"]["markets"]:
        if slug_kw in m.get("slug", "").lower():
            num_id = m["id"].split("-")[0]
            s = _api_get(f"https://app.palpitano.com/app/getSpecificMarketStats?id={num_id}")
            if s:
                dd   = s["data"]
                meta = dd.get("metadata", {})
                loc  = _extract_location(dd.get("title", ""), dd.get("description", ""))
                selection_codes, selection_ids = _extract_selection_maps(dd)
                print(f"[API:{slug_kw}] market {num_id} loc={loc!r}", flush=True)
                is_price_mkt = bool(MARKET_CONFIGS.get(slug_kw, MARKET_CONFIGS.get(
                    next((k for k, v in MARKET_CONFIGS.items() if v["slug_keyword"] == slug_kw), ""), {}
                )).get("is_price_market"))
                initial_price = round(float(meta["initialPrice"]), 2) if is_price_mkt and meta.get("initialPrice") is not None else None
                final_price   = round(float(meta["finalPrice"]),   2) if is_price_mkt and meta.get("finalPrice")   is not None else None
                current_price = _extract_latest_market_price(dd) if is_price_mkt else None
                value_needed  = initial_price if is_price_mkt else meta.get("valueNeeded")
                print(f"[API:{slug_kw}] market {num_id} initial={initial_price} current={current_price} final={final_price} loc={loc!r}", flush=True)
                return {
                    "slug":              dd["slug"],
                    "numeric_id":        str(dd["id"]),
                    "live_channel":      meta.get("channel", f"markets-live-stream-{dd['id']}"),
                    "remaining_seconds": dd.get("remainingSeconds"),
                    "value_needed":      value_needed,
                    "location":          loc,
                    "odds":              _extract_market_odds(dd),
                    "selection_codes":   selection_codes,
                    "selection_ids":     selection_ids,
                    "initial_price":     initial_price,
                    "final_price":       final_price,
                    "current_price":     current_price,
                    "fiat_decimals":     meta.get("fiatDecimals", 0),
                    "last_results_raw":  dd.get("lastResults", []),
                }
            return {
                "slug": m["slug"], "numeric_id": num_id,
                "live_channel": f"markets-live-stream-{num_id}",
                "remaining_seconds": None, "value_needed": None,
                "location": None, "odds": {},
                "selection_codes": {}, "selection_ids": {},
                "current_price": None,
            }
    return None


def _fetch_historical_entry(mk: str, tid: int, cutoff: datetime) -> tuple[int, dict | None, bool]:
    slug_kw = MARKET_CONFIGS[mk]["slug_keyword"]
    s = _api_get(f"https://app.palpitano.com/app/getSpecificMarketStats?id={tid}")
    if not s:
        return tid, None, False

    dd = s["data"]
    if slug_kw not in dd.get("slug", "").lower():
        return tid, None, False

    winner_id = dd.get("winnerId")
    if not winner_id:
        return tid, None, False

    selections = dd.get("selections", [])
    wlabel = next((x.get("label", "") for x in selections if x.get("id") == winner_id), None)
    if not wlabel:
        return tid, None, False

    meta = dd.get("metadata", {})
    won = "MAIS" if ("MAIS" in wlabel.upper() or "OVER" in wlabel.upper() or "SOBE" in wlabel.upper() or "UP" in wlabel.upper()) else "ATE"
    closes = dd.get("closesAt", "")
    is_price_market = bool(MARKET_CONFIGS[mk].get("is_price_market"))
    if is_price_market:
        initial_price = meta.get("initialPrice")
        final_price   = meta.get("finalPrice")
        # If API didn't give us a winner but we have both prices, infer
        if final_price is not None and initial_price is not None:
            try:
                won = "MAIS" if float(final_price) > float(initial_price) else "ATE"
            except (TypeError, ValueError):
                pass
        entry = {
            "time": closes[11:19] if len(closes) >= 19 else closes,
            "date": closes[:10] if len(closes) >= 10 else datetime.now().strftime("%Y-%m-%d"),
            "count": final_price,
            "result": won,
            "market_id": str(dd["id"]),
            "value_needed": initial_price,
            "location": None,
            "historical": True,
        }
    else:
        entry = {
            "time": closes[11:19] if len(closes) >= 19 else closes,
            "date": closes[:10] if len(closes) >= 10 else datetime.now().strftime("%Y-%m-%d"),
            "count": meta.get("valueFinal"),
            "result": won,
            "market_id": str(dd["id"]),
            "value_needed": meta.get("valueNeeded"),
            "location": _extract_location(dd.get("title", ""), dd.get("description", "")),
            "historical": True,
        }

    entry_dt = _entry_datetime(entry)
    if entry_dt is not None and entry_dt < cutoff:
        return tid, entry, True

    return tid, entry, False


def fetch_historical_rounds(
    mk: str,
    limit: int | None = None,
    scan_back: int | None = None,
):
    """Busca rodadas passadas e popula historico. Roda em thread separada."""
    slug_kw = MARKET_CONFIGS[mk]["slug_keyword"]
    if limit is None:
        lookback_limit = int(
            (
                HISTORY_LOOKBACK_DAYS * 86400 + WEEKLY_SLOT_WINDOW_SECONDS
            ) / MARKET_CONFIGS[mk]["duration"]
        ) + 240
        missing_target = max(0, HISTORY_TARGET_RECORDS - len(histories[mk])) + 300
        limit = max(lookback_limit, missing_target)
    if scan_back is None:
        scan_back = limit * 6
    d = _api_get(MARKETS_API)
    if not d:
        return
    current_id = None
    for m in d["data"]["markets"]:
        if slug_kw in m.get("slug", "").lower():
            current_id = int(m["id"].split("-")[0])
            break
    if not current_id:
        return

    already = {str(e.get("market_id")) for e in histories[mk] if e.get("market_id")}
    cutoff = _history_cutoff()
    known_ids = [
        int(entry["market_id"])
        for entry in histories[mk]
        if str(entry.get("market_id") or "").isdigit()
    ]
    start_offset = 1
    if known_ids and len(histories[mk]) >= HISTORY_TARGET_RECORDS:
        start_offset = max(1, current_id - min(known_ids) + 1)
    elif known_ids:
        gap_high_id, gap_low_id, gap_size = _largest_id_gap(known_ids)
        if gap_size > HISTORY_FETCH_BATCH_SIZE:
            start_offset = max(1, current_id - gap_high_id + 1)
            scan_back = max(scan_back, current_id - gap_low_id - 1)
            print(
                f"[API:{mk}] preenchendo maior gap "
                f"{gap_high_id}->{gap_low_id} ({gap_size} ids)",
                flush=True,
            )

    entries = []
    stop_scan = False

    with ThreadPoolExecutor(max_workers=HISTORY_FETCH_WORKERS) as executor:
        for batch_start in range(start_offset, scan_back + 1, HISTORY_FETCH_BATCH_SIZE):
            if len(entries) >= limit or stop_scan:
                break

            batch_end = min(batch_start + HISTORY_FETCH_BATCH_SIZE - 1, scan_back)
            tids = [
                current_id - offset
                for offset in range(batch_start, batch_end + 1)
                if str(current_id - offset) not in already
            ]
            if not tids:
                continue

            futures = {
                executor.submit(_fetch_historical_entry, mk, tid, cutoff): tid
                for tid in tids
            }
            results_by_tid: dict[int, tuple[dict | None, bool]] = {}

            for future in as_completed(futures):
                tid = futures[future]
                try:
                    _, entry, is_before_cutoff = future.result()
                except Exception as ex:
                    print(f"[API:{mk}] erro historico id={tid}: {ex}", flush=True)
                    continue
                results_by_tid[tid] = (entry, is_before_cutoff)

            for tid in tids:
                entry, is_before_cutoff = results_by_tid.get(tid, (None, False))
                if entry:
                    entries.append(entry)
                    already.add(str(tid))
                    if len(entries) >= limit:
                        break
                if (
                    is_before_cutoff
                    and len(histories[mk]) + len(entries) >= HISTORY_TARGET_RECORDS
                ):
                    stop_scan = True
                    break

            if entries:
                with history_locks[mk]:
                    histories[mk].extend(entries)
                    save_history(mk)
                publish_history(mk)

            oldest_preview = _entry_datetime(entries[-1]) if entries else None
            oldest_str = oldest_preview.strftime("%Y-%m-%d %H:%M:%S") if oldest_preview else "?"
            print(
                f"[API:{mk}] backfill {batch_end}/{scan_back} ids | "
                f"+{len(entries)} novas | mais antiga {oldest_str}",
                flush=True,
            )

    if not entries:
        return

    with history_locks[mk]:
        histories[mk].extend(entries)
        save_history(mk)
    print(f"[API:{mk}] {len(entries)} rodadas históricas carregadas", flush=True)
    publish_history(mk)


def history_backfill_worker(mk: str):
    if BACKFILL_START_DELAY_SECONDS > 0:
        print(
            f"[API:{mk}] backfill vai iniciar em "
            f"{BACKFILL_START_DELAY_SECONDS}s com servidor ja online",
            flush=True,
        )
        time.sleep(BACKFILL_START_DELAY_SECONDS)

    while True:
        try:
            fetch_historical_rounds(mk)
        except Exception as ex:
            print(f"[API:{mk}] erro no backfill: {ex}", flush=True)

        if len(histories[mk]) >= HISTORY_TARGET_RECORDS:
            print(
                f"[API:{mk}] alvo de {HISTORY_TARGET_RECORDS} registros atingido",
                flush=True,
            )
            return

        time.sleep(BACKFILL_RETRY_INTERVAL_SECONDS)


def bootstrap_history_once(mk: str):
    if len(histories[mk]) >= HISTORY_TARGET_RECORDS:
        return

    try:
        print(
            f"[API:{mk}] bootstrap historico {len(histories[mk])}/{HISTORY_TARGET_RECORDS}",
            flush=True,
        )
        fetch_historical_rounds(mk, limit=240, scan_back=6000)
    except Exception as ex:
        print(f"[API:{mk}] erro bootstrap historico: {ex}", flush=True)


# ──────────────────────────────────────────────
# WebSocket channel management
# ──────────────────────────────────────────────
def subscribe_market(ws, mk: str, mdata: dict):
    global channel_to_market, current_channels
    slug    = mdata["slug"]
    num_id  = mdata["numeric_id"]
    live_ch = mdata["live_channel"]
    channels = [f"markets-{slug}", f"markets-{num_id}", live_ch]

    with channel_lock:
        old = current_channels.get(mk)
        if old and old not in channels:
            old_n = old.split("-")[-1]
            for och in [old, f"markets-{old_n}",
                        f"markets-live-cars-stream-{old_n}",
                        f"markets-live-people-stream-{old_n}"]:
                try:
                    ws.send(json.dumps({"event": "pusher:unsubscribe", "data": {"channel": och}}))
                    channel_to_market.pop(och, None)
                except Exception:
                    pass

        for ch in channels:
            ws.send(json.dumps({"event": "pusher:subscribe", "data": {"channel": ch}}))
            channel_to_market[ch] = mk
            print(f"[WS:{mk}] ↳ {ch}", flush=True)

        current_channels[mk] = f"markets-{slug}"
        st = states[mk]
        st["market_id"] = num_id
        st["slug"] = slug
        st["live_page_url"] = f"https://app.palpitano.com/live/{num_id}-market/{slug}"
        if mdata.get("remaining_seconds") is not None:
            st["remaining_seconds"] = int(mdata["remaining_seconds"])
        if mdata.get("value_needed") is not None:
            st["value_needed"] = mdata["value_needed"]
        if mdata.get("location") is not None:
            st["location"] = mdata["location"]
        if mdata.get("selection_codes") is not None:
            st["selection_codes"] = dict(mdata.get("selection_codes") or {})
        if mdata.get("selection_ids") is not None:
            st["selection_ids"] = dict(mdata.get("selection_ids") or {})
        # Bitcoin: store initial/final price
        if mdata.get("initial_price") is not None:
            st["initial_price"] = mdata["initial_price"]
        if mdata.get("final_price") is not None:
            st["final_price"] = mdata["final_price"]
        # For price markets, prefer the freshest graph price and fall back to opening price.
        if MARKET_CONFIGS[mk].get("is_price_market"):
            if mdata.get("current_price") is not None:
                st["current_count"] = mdata["current_price"]
            elif mdata.get("initial_price") is not None and st.get("current_count") is None:
                st["current_count"] = mdata["initial_price"]
        count_history[mk].clear()
        if mdata.get("odds"):
            odds_history[mk].clear()
            _record_market_odds(mk, num_id, mdata["odds"])


def channel_watcher():
    global ws_instance
    time.sleep(12)
    while True:
        for mk, cfg in MARKET_CONFIGS.items():
            mdata = get_market_data(cfg["slug_keyword"])
            if not mdata:
                continue
            new_ch = f"markets-{mdata['slug']}"
            if new_ch != current_channels.get(mk):
                print(f"[API:{mk}] Novo market: {new_ch}", flush=True)
                if ws_instance:
                    try:
                        subscribe_market(ws_instance, mk, mdata)
                        broadcast(mk, "channel_change", {
                            "market_id":   mdata["numeric_id"],
                            "slug":        mdata["slug"],
                            "channel":     new_ch,
                            "location":    mdata.get("location"),
                            "value_needed": mdata.get("value_needed"),
                            **_odds_site_payload(mk),
                        })
                    except Exception as e:
                        print(f"[API:{mk}] re-subscrever erro: {e}", flush=True)
            elif mdata.get("remaining_seconds") is not None:
                rem = int(mdata["remaining_seconds"])
                st  = states[mk]
                st["remaining_seconds"] = rem
                # Mercados de preco: sincroniza abertura e ultimo preco conhecido via API.
                if MARKET_CONFIGS[mk].get("is_price_market"):
                    if mdata.get("initial_price") is not None and st.get("initial_price") is None:
                        st["initial_price"] = float(mdata["initial_price"])
                        st["value_needed"]  = float(mdata["initial_price"])
                    if mdata.get("current_price") is not None:
                        try:
                            st["current_count"] = round(float(mdata["current_price"]), 2)
                        except (TypeError, ValueError):
                            st["current_count"] = mdata["current_price"]
                if mdata.get("odds") and st.get("market_id"):
                    _record_market_odds(mk, st.get("market_id"), mdata["odds"])
                if st.get("current_count") is not None:
                    broadcast(mk, "count_update", {
                        "count":     st["current_count"],
                        "remaining": rem,
                        "time":      datetime.now().strftime("%H:%M:%S"),
                        "live_hint": _live_proba_public_payload(mk),
                        **_odds_site_payload(mk),
                    })
        time.sleep(15)


# ──────────────────────────────────────────────
# WebSocket handlers
# ──────────────────────────────────────────────
def price_market_poll_worker():
    while True:
        time.sleep(PRICE_MARKET_POLL_SECONDS)
        for mk, cfg in MARKET_CONFIGS.items():
            if not cfg.get("is_price_market"):
                continue

            st = states[mk]
            market_id = st.get("market_id")
            if not market_id:
                continue

            s = _api_get(f"https://app.palpitano.com/app/getSpecificMarketStats?id={market_id}")
            if not s:
                continue

            dd = s.get("data") or {}
            meta = dd.get("metadata") or {}
            latest_price = _extract_latest_market_price(dd)
            if latest_price is None:
                continue

            try:
                latest_price = round(float(latest_price), 2)
            except (TypeError, ValueError):
                continue

            initial_price = _as_float(meta.get("initialPrice"))
            if initial_price is not None:
                initial_price = round(initial_price, 2)
                st["initial_price"] = initial_price
                st["value_needed"] = initial_price

            remaining = dd.get("remainingSeconds")
            if remaining is not None:
                st["remaining_seconds"] = remaining

            prev_price = _as_float(st.get("current_count"))
            if prev_price is not None and abs(prev_price - latest_price) < 0.005 and remaining is None:
                continue

            ts = datetime.now().strftime("%H:%M:%S")
            st.update({
                "current_count": latest_price,
                "last_updated": ts,
                "status": "ao_vivo",
            })
            _record_live_count_sample(mk, latest_price, st.get("remaining_seconds"))
            broadcast(mk, "count_update", {
                "count": latest_price,
                "remaining": st.get("remaining_seconds"),
                "time": ts,
                "value_needed": st.get("value_needed"),
                "live_hint": _live_proba_public_payload(mk),
                **_odds_site_payload(mk),
            })


def on_open(ws):
    global ws_instance
    ws_instance = ws
    print("[WS] Conectado", flush=True)
    for mk, cfg in MARKET_CONFIGS.items():
        mdata = get_market_data(cfg["slug_keyword"])
        if mdata:
            subscribe_market(ws, mk, mdata)
            states[mk]["connected"] = True
        else:
            print(f"[WS] Market não encontrado: {mk}", flush=True)


def on_message(ws, message):
    try:
        msg = json.loads(message)
    except json.JSONDecodeError:
        return

    event   = msg.get("event", "")
    channel = msg.get("channel", "")
    raw     = msg.get("data", {})
    data    = json.loads(raw) if isinstance(raw, str) else raw

    if event == "pusher:ping":
        ws.send(json.dumps({"event": "pusher:pong", "data": {}}))
        return
    if event == "pusher:connection_established":
        return
    if event == "pusher_internal:subscription_succeeded":
        print(f"[WS] ✓ {channel}", flush=True)
        return

    mk = channel_to_market.get(channel)
    if not mk:
        return

    ts = datetime.now().strftime("%H:%M:%S")

    # Normalize nested payload
    inner_event = event
    payload: dict = {}
    if isinstance(data, dict):
        if "message" in data and isinstance(data["message"], dict):
            inner_event = data["message"].get("event", event)
            payload     = data["message"].get("data", {}) or {}
        elif "message" in data and isinstance(data["message"], str):
            try:
                m = json.loads(data["message"])
                inner_event = m.get("event", event)
                payload     = m.get("data", {}) or {}
            except Exception:
                payload = data
        else:
            payload = data
    if not isinstance(payload, dict):
        payload = {}

    st = states[mk]

    # ── Contagem ao vivo ──
    if inner_event in ("value.updated", "price.updated") or "currentTotal" in payload or "currentPrice" in payload:
        is_price_mkt = bool(MARKET_CONFIGS[mk].get("is_price_market"))
        if is_price_mkt:
            count = payload.get("currentPrice")
            if count is None:
                count = payload.get("currentTotal") or payload.get("value") or payload.get("count")
        else:
            count = payload.get("currentTotal")
            if count is None:
                count = payload.get("value") or payload.get("count")
        remaining = payload.get("remainingSeconds", st["remaining_seconds"])
        if count is not None:
            if is_price_mkt:
                try:
                    count = round(float(count), 2)
                except (TypeError, ValueError):
                    pass
            st.update({"current_count": count, "remaining_seconds": remaining,
                       "last_updated": ts, "status": "ao_vivo"})
            _record_live_count_sample(mk, count, remaining)
            broadcast(mk, "count_update", {
                "count": count, "remaining": remaining, "time": ts,
                "value_needed": st.get("value_needed"),
                "live_hint": _live_proba_public_payload(mk),
                **_odds_site_payload(mk),
            })
            print(f"[{ts}:{mk}] *** {count} | {remaining}s ***", flush=True)
            _try_send_live_signal(mk)

    # ── Odds update ──
    if inner_event == "markets.odds.update":
        rem = payload.get("remainingSeconds") or payload.get("closesIn")
        if rem is not None:
            st["remaining_seconds"] = rem
            live_odds = _fetch_market_odds(st.get("market_id"))
            if live_odds:
                _record_market_odds(mk, st.get("market_id"), live_odds)
            if st["current_count"] is not None:
                broadcast(mk, "count_update", {
                    "count": st["current_count"], "remaining": rem, "time": ts,
                    "live_hint": _live_proba_public_payload(mk),
                    **_odds_site_payload(mk),
                })

    # ── Settlement ──
    if inner_event == "markets.settlement":
        with _settlement_locks[mk]:
            now_t = time.time()
            if now_t - last_settlement_ts[mk] < 10:
                return  # duplicata, ignorar
            last_settlement_ts[mk] = now_t
    if inner_event == "markets.settlement":
        rv = payload.get("result") or payload.get("settlement", {})
        if isinstance(rv, dict):
            outcome   = rv.get("outcome") or rv.get("result")
            final_cnt = (rv.get("finalCount") or rv.get("finalPrice")
                        or rv.get("count") or st["current_count"])
        else:
            outcome, final_cnt = rv, st["current_count"]
        is_price_mkt = bool(MARKET_CONFIGS[mk].get("is_price_market"))
        if is_price_mkt and final_cnt is not None:
            try:
                final_cnt = round(float(final_cnt), 2)
            except (TypeError, ValueError):
                pass

        threshold = st.get("value_needed")
        won = None
        if outcome:
            ou = str(outcome).upper()
            won = "MAIS" if ("MAIS" in ou or "OVER" in ou or "MORE" in ou or "SOBE" in ou or "UP" in ou) else "ATE"
        if won is None and final_cnt is not None and threshold is not None:
            try:
                won = "MAIS" if float(final_cnt) > float(threshold) else "ATE"
            except (TypeError, ValueError):
                won = "MAIS" if int(final_cnt) > int(threshold) else "ATE"
        if won is None:
            s2 = _api_get(f"https://app.palpitano.com/app/getSpecificMarketStats?id={st.get('market_id')}")
            if s2:
                dd2  = s2["data"]
                wid  = dd2.get("winnerId")
                sels = dd2.get("selections", [])
                wl   = next((x.get("label", "") for x in sels if x.get("id") == wid), "")
                if wl:
                    won = "MAIS" if ("MAIS" in wl.upper() or "SOBE" in wl.upper() or "UP" in wl.upper() or "OVER" in wl.upper()) else "ATE"
                if is_price_mkt and final_cnt is None:
                    fp = dd2.get("metadata", {}).get("finalPrice")
                    if fp is not None:
                        final_cnt = round(float(fp), 2)
                if not is_price_mkt:
                    st["value_needed"] = dd2.get("metadata", {}).get("valueNeeded", threshold)

        entry = {
            "time": ts, "count": final_cnt, "result": won,
            "market_id": st.get("market_id"), "value_needed": st.get("value_needed"),
            "location": st.get("location"),
            "date": datetime.now().strftime("%Y-%m-%d"),
        }
        market_id = st.get("market_id")
        sent_signal = _consume_sent_signal(mk, market_id)
        effective_rec = sent_signal.get("rec") if sent_signal else None
        with automation_lock:
            cfg = automation_cfg.get(mk) or {}
            open_position = cfg.get("open_position")
        if isinstance(open_position, dict) and str(open_position.get("market_id") or "") == str(market_id or ""):
            closed_position = open_position  # salva antes de limpar
            _automation_set_open_position(mk, None)
        else:
            closed_position = None

        # fallback: se o sinal nao foi consumido mas havia posicao aberta, usa o rec dela
        if effective_rec is None and isinstance(closed_position, dict):
            effective_rec = closed_position.get("rec") or None

        if str(_live_signal_mid.get(mk) or "") == str(market_id or ""):
            _live_signal_mid[mk] = None
            _live_signal_rec[mk] = None

        with history_locks[mk]:
            histories[mk].appendleft(entry)
            if won:
                last_results[mk].appendleft(won)
            save_history(mk)
        st["status"] = "encerrado"
        broadcast(mk, "settlement", entry)
        print(f"[{ts}:{mk}] RESULTADO: {won} (final: {final_cnt})", flush=True)

        # ── Notificação / score ──
        _check_day_reset()
        score = telegram_score[mk]
        sinal_odd = None
        if sent_signal and sent_signal.get("rec"):
            sinal_odd = sent_signal.get("odd")
        confirmed_bet = bool(
            effective_rec and (
                isinstance(closed_position, dict)
                or _automation_has_confirmed_order(mk, market_id, rec=effective_rec)
            )
        )

        if effective_rec and won and confirmed_bet:
            if won == effective_rec:
                score["wins"] += 1
                _analytics_resolve(mk, market_id, "win", entry_odd=sinal_odd)
                _automation_order_log_resolve(mk, market_id, "win", rec=effective_rec)
            else:
                score["losses"] += 1
                _analytics_resolve(mk, market_id, "loss", entry_odd=sinal_odd)
                _automation_order_log_resolve(mk, market_id, "loss", rec=effective_rec)
            _save_score()
            if closed_position:
                _send_automation_result_telegram(
                    mk,
                    won=(won == effective_rec),
                    rec=effective_rec,
                    position=closed_position,
                    score=score,
                )
        elif effective_rec and won:
            pending_entry = _automation_order_log_find_latest(
                mk,
                market_id,
                rec=effective_rec,
                statuses=("pending",),
            )
            updated_pending = _automation_order_log_update_latest(
                mk,
                market_id,
                rec=effective_rec,
                statuses=("pending",),
                status="error",
                detail="Mercado encerrou sem confirmar posicao. Ordem nao foi contabilizada como aposta.",
                confirmed=False,
                outcome=None,
                resolved_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            )
            if updated_pending and pending_entry:
                _send_automation_order_telegram(
                    mk,
                    status="error",
                    market_id=market_id,
                    rec=effective_rec,
                    kind=pending_entry.get("kind"),
                    amount=_as_float(pending_entry.get("amount")),
                    odd=pending_entry.get("odd"),
                    conf_pct=pending_entry.get("conf_pct"),
                    detail="Mercado encerrou sem confirmar a posicao. A ordem pendente nao executou e nao foi contabilizada.",
                )
            _analytics_resolve(
                mk,
                market_id,
                "win" if won == effective_rec else "loss",
                entry_odd=sinal_odd,
            )
        elif closed_position and won:
            # posicao encerrada sem sinal consumido — envia resultado mesmo assim
            pos_rec = closed_position.get("rec")
            if pos_rec:
                _automation_order_log_resolve(mk, market_id, "win" if won == pos_rec else "loss", rec=pos_rec)
                _send_automation_result_telegram(
                    mk,
                    won=(won == pos_rec),
                    rec=pos_rec,
                    position=closed_position,
                    score=score,
                )

        _send_result_telegram(mk, entry, sent_signal=sent_signal)

        def _resub(mk=mk, cfg=MARKET_CONFIGS[mk]):
            time.sleep(10)
            # Atualiza saldo após resultado da rodada
            with automation_lock:
                _has_api = bool(
                    str((automation_cfg.get(mk) or {}).get("api_key") or "").strip()
                )
            if _has_api:
                _automation_refresh_balance_async(mk)
            mdata = get_market_data(cfg["slug_keyword"])
            if not mdata:
                return
            if f"markets-{mdata['slug']}" != current_channels.get(mk) and ws_instance:
                subscribe_market(ws_instance, mk, mdata)
                broadcast(mk, "channel_change", {
                    "market_id":    mdata["numeric_id"],
                    "slug":         mdata["slug"],
                    "channel":      f"markets-{mdata['slug']}",
                    "location":     mdata.get("location"),
                    "value_needed": mdata.get("value_needed"),
                    **_odds_site_payload(mk),
                })
            # Sempre envia sinal (dedup interno impede duplicata)
            _send_signal(
                mk,
                mdata.get("remaining_seconds"),
                mdata.get("location"),
                mdata.get("value_needed"),
                mdata.get("numeric_id"),
            )
        threading.Thread(target=_resub, daemon=True).start()

    # ── Nova rodada ──
    if inner_event == "markets.create":
        with _create_locks[mk]:
            now_t = time.time()
            if now_t - last_create_ts[mk] < 10:
                return  # duplicata, ignorar
            last_create_ts[mk] = now_t
        rem = payload.get("remainingSeconds") or payload.get("eventDuration")
        st["odds"] = {}
        odds_history[mk].clear()
        count_history[mk].clear()
        st.update({"status": "ao_vivo", "current_count": 0, "remaining_seconds": rem})
        # Bitcoin: reset prices and try to get initialPrice from payload/API
        if MARKET_CONFIGS[mk].get("is_price_market"):
            st["final_price"] = None
            init_p = (payload.get("initialPrice")
                      or payload.get("metadata", {}).get("initialPrice"))
            if init_p is not None:
                st["initial_price"] = float(init_p)
                st["value_needed"]  = float(init_p)
            else:
                def _refresh_init_price(mk_inner=mk):
                    mdata = get_market_data(MARKET_CONFIGS[mk_inner]["slug_keyword"])
                    if mdata and mdata.get("initial_price") is not None:
                        states[mk_inner]["initial_price"] = float(mdata["initial_price"])
                        states[mk_inner]["value_needed"]  = float(mdata["initial_price"])
                        print(f"[{mk_inner}] initialPrice atualizado via API: {mdata['initial_price']}", flush=True)
                threading.Thread(target=_refresh_init_price, daemon=True).start()
        _record_live_count_sample(mk, 0, rem)
        broadcast(mk, "round_start", {
            "remaining": rem,
            "time": ts,
            **_odds_site_payload(mk),
        })
        print(f"[{ts}:{mk}] Nova rodada. {rem}s", flush=True)
        _send_signal(
            mk,
            rem,
            st.get("location"),
            st.get("value_needed"),
            st.get("market_id"),
        )


def on_error(ws, error):
    print(f"[WS] Erro: {error}", flush=True)
    if "403" in str(error):
        print(
            "[WS] Handshake bloqueado pelo Cloudflare/WAF do ws.palpitano.com. "
            "A API HTTP continua respondendo, mas o stream ao vivo pode ficar indisponivel.",
            flush=True,
        )
    for mk in MARKET_CONFIGS:
        states[mk].update({"connected": False, "status": "erro"})
        broadcast(mk, "status_change", {"status": "erro_conexao"})


def on_close(ws, code, msg):
    print(f"[WS] Fechado ({code}). Reconectando...", flush=True)
    for mk in MARKET_CONFIGS:
        states[mk].update({"connected": False, "status": "reconectando"})
        broadcast(mk, "status_change", {"status": "reconectando"})


def ws_thread():
    global ws_instance
    while True:
        try:
            _sanitize_broken_proxy_env()
            runtime_ws_url, runtime_headers, runtime_sslopt = _build_ws_runtime_options()
            with _bypass_proxy_env():
                app_ws = websocket.WebSocketApp(
                    runtime_ws_url,
                    on_open=on_open, on_message=on_message,
                    on_error=on_error, on_close=on_close,
                    header=runtime_headers or None,
                )
                ws_instance = app_ws
                app_ws.run_forever(
                    ping_interval=25,
                    ping_timeout=10,
                    sslopt=runtime_sslopt or None,
                    http_no_proxy=[WS_HOST, "app.palpitano.com", "www.app.palpitano.com"],
                    http_proxy_host=None,
                    http_proxy_port=None,
                )
        except Exception as e:
            print(f"[WS] Exceção: {e}", flush=True)
        ws_instance = None
        time.sleep(5)


# ──────────────────────────────────────────────
# Flask
# ──────────────────────────────────────────────
app = Flask(__name__)
app.secret_key = os.getenv("ODDSYNC_SESSION_SECRET") or hashlib.sha256(
    f"{BASE_DIR}|oddsync-session".encode("utf-8", errors="ignore")
).hexdigest()
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    PERMANENT_SESSION_LIFETIME=timedelta(days=30),
)


@app.after_request
def _disable_api_response_cache(response):
    if request.path.startswith("/api/"):
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    return response

_runtime_context_lock = threading.RLock()
_active_runtime_license_key = ""

LOGIN_TEMPLATE = """
<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Oddsync - Login</title>
  <style>
    :root{--bg:#0f1117;--card:#1a1d27;--border:#2a2d3a;--text:#e2e8f0;--muted:#8ca0c4;--blue:#3b82f6;--red:#f87171}
    *{box-sizing:border-box} body{margin:0;min-height:100vh;display:grid;place-items:center;background:radial-gradient(circle at top,#172033,#0f1117 58%);font-family:Segoe UI,Arial,sans-serif;color:var(--text);padding:20px}
    .box{width:min(440px,100%);background:var(--card);border:1px solid var(--border);border-radius:20px;padding:28px;box-shadow:0 24px 80px rgba(0,0,0,.38)}
    h1{margin:0 0 8px;font-size:1.8rem}.muted{color:var(--muted);font-size:.95rem;margin-bottom:22px;line-height:1.45}
    label{display:block;color:#b8c7e6;font-size:.8rem;margin:14px 0 6px;text-transform:uppercase;letter-spacing:.08em}
    input{width:100%;background:#11141d;border:1px solid var(--border);border-radius:12px;color:var(--text);padding:13px 14px;font-size:1rem;outline:none}
    input:focus{border-color:var(--blue);box-shadow:0 0 0 3px rgba(59,130,246,.16)}
    button{width:100%;margin-top:18px;border:0;border-radius:12px;background:var(--blue);color:white;padding:13px;font-weight:800;cursor:pointer}
    .err{color:#fecaca;background:rgba(248,113,113,.12);border:1px solid rgba(248,113,113,.35);padding:10px 12px;border-radius:12px;margin-bottom:14px}
    .brand{font-weight:900;color:white}.hint{font-size:.78rem;color:var(--muted);margin-top:14px;text-align:center}
    .remember{display:flex;align-items:center;gap:10px;margin-top:14px;color:#c9d5ee;font-size:.9rem}
    .remember input{width:18px;height:18px;padding:0;accent-color:var(--blue)}
  </style>
</head>
<body>
  <form class="box" method="post" action="/login" id="login-form">
    <h1><span class="brand">Oddsync</span></h1>
    <div class="muted">Entre com o usuário, senha e license key que foram criados no gerador de licenças.</div>
    {% if error %}<div class="err">{{ error }}</div>{% endif %}
    <label>Usuário</label>
    <input name="username" id="username" autocomplete="username" autofocus required>
    <label>Senha</label>
    <input name="password" id="password" type="password" autocomplete="current-password" required>
    <label>License key</label>
    <input name="license_key" id="license_key" placeholder="XXXX-XXXX-XXXX-XXXX" required>
    <label class="remember">
      <input type="checkbox" id="remember-login" checked>
      <span>Lembrar neste navegador</span>
    </label>
    <button type="submit">Entrar</button>
    <div class="hint">A validade começa na primeira ativação da key.</div>
    <div class="hint">Se marcado, usuário e key ficam salvos só neste navegador.</div>
  </form>
  <script>
    (function () {
      const storageKey = "oddsync_saved_login_v1";
      const form = document.getElementById("login-form");
      const remember = document.getElementById("remember-login");
      const username = document.getElementById("username");
      const password = document.getElementById("password");
      const licenseKey = document.getElementById("license_key");

      try {
        const raw = window.localStorage.getItem(storageKey);
        if (raw) {
          const saved = JSON.parse(raw);
          username.value = typeof saved.username === "string" ? saved.username : "";
          password.value = "";
          licenseKey.value = typeof saved.license_key === "string" ? saved.license_key : "";
          remember.checked = saved.remember !== false;
          if (Object.prototype.hasOwnProperty.call(saved, "password")) {
            window.localStorage.setItem(storageKey, JSON.stringify({
              remember: saved.remember !== false,
              username: typeof saved.username === "string" ? saved.username : "",
              license_key: typeof saved.license_key === "string" ? saved.license_key : ""
            }));
          }
        }
      } catch (err) {
        console.warn("Nao foi possivel carregar login salvo.", err);
      }

      form.addEventListener("submit", function () {
        try {
          if (!remember.checked) {
            window.localStorage.removeItem(storageKey);
            return;
          }
          window.localStorage.setItem(storageKey, JSON.stringify({
            remember: true,
            username: username.value || "",
            license_key: licenseKey.value || ""
          }));
        } catch (err) {
          console.warn("Nao foi possivel salvar login.", err);
        }
      });
    })();
  </script>
</body>
</html>
"""


def _load_or_create_access_config() -> dict:
    env_user = str(os.getenv("ODDSYNC_ACCESS_USER") or "").strip()
    env_pass = str(os.getenv("ODDSYNC_ACCESS_PASSWORD") or "").strip()
    if env_pass:
        return {
            "enabled": True,
            "username": env_user or "oddsync",
            "password": env_pass,
            "source": "env",
        }

    cfg = {}
    try:
        if os.path.exists(ACCESS_CONFIG_FILE):
            with open(ACCESS_CONFIG_FILE, "r", encoding="utf-8") as f:
                cfg = json.load(f) or {}
    except Exception:
        cfg = {}

    changed = False
    if "enabled" not in cfg:
        cfg["enabled"] = True
        changed = True
    if not str(cfg.get("username") or "").strip():
        cfg["username"] = "oddsync"
        changed = True
    if not str(cfg.get("password") or "").strip():
        cfg["password"] = secrets.token_urlsafe(10)
        changed = True

    if changed:
        _atomic_json_write(ACCESS_CONFIG_FILE, cfg, indent=2)

    cfg["source"] = "file"
    return cfg


ACCESS_CONFIG = _load_or_create_access_config()


def _access_password_enabled() -> bool:
    if _account_login_enabled():
        return False
    raw = str(os.getenv("ODDSYNC_ACCESS_ENABLED", "")).strip().lower()
    if raw in {"0", "false", "no", "off"}:
        return False
    return bool(ACCESS_CONFIG.get("enabled", True))


def _account_login_enabled() -> bool:
    raw = str(os.getenv("ODDSYNC_ACCOUNT_LOGIN", "1")).strip().lower()
    return raw not in {"0", "false", "no", "off"}


def _ensure_runtime_context_for_key(key: str, *, force_reload: bool = False):
    global _active_runtime_license_key
    clean_key = str(key or "").strip().upper()
    if not clean_key:
        return
    with _runtime_context_lock:
        if _active_runtime_license_key == clean_key and not force_reload:
            return
        previous_key = str(_active_runtime_license_key or "").strip().upper()
        if previous_key and previous_key != clean_key:
            previous_thread_key = str(getattr(_thread_license_context, "license_key", "") or "")
            try:
                _thread_license_context.license_key = previous_key
                previous_raw = _load_automation_raw()
                previous_cfg_map = _load_automation_config(previous_raw)
                previous_meta = _load_automation_meta(previous_raw)
                if previous_meta.get("running"):
                    previous_meta["running"] = False
                    for market_key in MARKET_CONFIGS:
                        cfg = previous_cfg_map.get(market_key)
                        if not cfg:
                            continue
                        cfg["last_order_status"] = "idle"
                        cfg["last_order_message"] = "Bot pausado por troca de sessao/licenca ativa."
                        cfg["last_order_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    _save_automation_config(previous_cfg_map, previous_meta)
            finally:
                if previous_thread_key:
                    _thread_license_context.license_key = previous_thread_key
                elif hasattr(_thread_license_context, "license_key"):
                    delattr(_thread_license_context, "license_key")
        license_state["license_key"] = clean_key
        _save_license_state_unsafe()
        _reload_private_runtime_state()
        _active_runtime_license_key = clean_key


def _runtime_running_for_key(key: str) -> bool:
    clean_key = str(key or "").strip().upper()
    if not clean_key:
        return False
    try:
        raw = _client_db_get_json(_license_storage_slug_for(clean_key), "automatizado_config.json", {})
        meta = _load_automation_meta(raw)
        return bool(meta.get("running"))
    except Exception:
        return False


def _active_runtime_status() -> tuple[str, bool]:
    with _runtime_context_lock:
        active_key = str(_active_runtime_license_key or "").strip().upper()
    return active_key, _runtime_running_for_key(active_key)


def _sync_runtime_state_for_current_session(*, force: bool = False, allow_takeover: bool = False):
    if not has_request_context():
        return
    session_key = str(session.get("license_key") or "").strip().upper()
    if not session_key:
        return
    with _runtime_context_lock:
        active_key = str(_active_runtime_license_key or "").strip().upper()
        should_sync = (
            not active_key
            or active_key == session_key
            or allow_takeover
        )
        should_force_reload = (
            force
            or allow_takeover
            or active_key == session_key
        )
    if should_sync:
        _ensure_runtime_context_for_key(session_key, force_reload=should_force_reload)


def _unauthorized_access_response():
    return Response(
        "Acesso protegido. Informe usuario e senha do Oddsync.",
        401,
        {"WWW-Authenticate": 'Basic realm="Oddsync", charset="UTF-8"'},
    )


@app.before_request
def _require_panel_password():
    if _account_login_enabled():
        allowed_paths = {"/login", "/api/login", "/logout"}
        if request.path in allowed_paths:
            return None
        if request.path.startswith("/static/"):
            return None
        if session.get("license_key"):
            _refresh_license_store_unsafe()
            entry = _find_license_entry(session.get("license_key"))
            allowed, device_message = _license_device_allowed(entry or {})
            if _license_status_for(entry) == "active" and allowed:
                _touch_license_access(entry, datetime.now().isoformat(timespec="seconds"))
                _save_license_store_unsafe()
                return None
            if request.path.startswith("/api/"):
                return jsonify({"success": False, "message": device_message or "Licenca expirada, revogada ou bloqueada."}), 401
            session.clear()
            return redirect(url_for("login_page"))
        if request.path.startswith("/api/"):
            return jsonify({"success": False, "message": "Login necessario."}), 401
        return redirect(url_for("login_page"))

    if not _access_password_enabled():
        return None
    if request.method == "OPTIONS":
        return None

    auth = request.authorization
    expected_user = str(ACCESS_CONFIG.get("username") or "oddsync")
    expected_pass = str(ACCESS_CONFIG.get("password") or "")
    if (
        auth
        and secrets.compare_digest(str(auth.username or ""), expected_user)
        and secrets.compare_digest(str(auth.password or ""), expected_pass)
    ):
        return None
    return _unauthorized_access_response()


@app.route("/login", methods=["GET", "POST"])
def login_page():
    if request.method == "GET":
        return render_template_string(LOGIN_TEMPLATE, error="")

    username = str(request.form.get("username") or "").strip()
    password = str(request.form.get("password") or "")
    license_key = str(request.form.get("license_key") or request.form.get("key") or "").strip()
    ok, message, _data = _activate_license_key(license_key, username, password)
    if not ok:
        session.clear()
        return render_template_string(LOGIN_TEMPLATE, error=message), 401
    return redirect(url_for("index"))


@app.post("/api/login")
def api_login():
    payload = request.get_json(silent=True) or {}
    ok, message, data = _activate_license_key(
        payload.get("license_key") or payload.get("key"),
        payload.get("username"),
        payload.get("password"),
    )
    if not ok and has_request_context():
        session.clear()
    return jsonify({"success": ok, "message": message, "data": data}), (200 if ok else 401)


@app.route("/logout", methods=["GET", "POST"])
def logout():
    session.clear()
    return redirect(url_for("login_page"))


@app.route("/")
def index():
    return render_template(
        "index.html",
        markets=MARKET_CONFIGS,
        states={k: {**states[k], "duration": MARKET_CONFIGS[k]["duration"]} for k in MARKET_CONFIGS},
        histories={k: [] for k in MARKET_CONFIGS},
        last_results_map={k: list(last_results[k]) for k in MARKET_CONFIGS},
        telegram_map={k: _telegram_public_payload(k) for k in MARKET_CONFIGS},
        license_info=_license_public_payload(),
    )



def _sse_gen(mk: str):
    q: queue.Queue = queue.Queue(maxsize=50)
    client = {
        "license_key": str(session.get("license_key") or "").strip().upper() if has_request_context() else "",
        "queue": q,
    }
    with sse_locks[mk]:
        sse_clients[mk].append(client)

    def gen():
        yield (
            "event: init\ndata: "
            + json.dumps({
                "state":        states[mk],
                "history":      list(histories[mk]),
                "last_results": list(last_results[mk]),
                "history_count": len(histories[mk]),
                "history_target": HISTORY_TARGET_RECORDS,
                "automation": _automation_public_payload(mk),
                "telegram": _telegram_public_payload(mk),
                "live_hint": _live_proba_public_payload(mk),
                **_odds_site_payload(mk),
            }, ensure_ascii=False)
            + "\n\n"
        )
        try:
            while True:
                try:
                    yield q.get(timeout=20)
                except queue.Empty:
                    yield ": keep-alive\n\n"
        except GeneratorExit:
            pass
        finally:
            with sse_locks[mk]:
                if client in sse_clients[mk]:
                    sse_clients[mk].remove(client)

    return Response(
        stream_with_context(gen()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.route("/stream/<mk>")
def stream_market(mk: str):
    market_key = str(mk or "").strip().lower()
    if market_key not in MARKET_CONFIGS:
        return ("mercado nao encontrado", 404)
    return _sse_gen(market_key)



# ──────────────────────────────────────────────
# Petróleo — Análise Técnica + Notícias
# ──────────────────────────────────────────────
import importlib as _importlib

def _lazy_yf():
    return _importlib.import_module("yfinance")

def _lazy_fp():
    return _importlib.import_module("feedparser")

_tech_cache: dict = {}     # mk -> {"data": {...}, "ts": float}
_news_cache: dict = {}     # mk -> {"data": [...], "ts": float}
_TECH_TTL  = 60            # segundos entre refreshes de tecnicos
_NEWS_TTL  = 120           # segundos entre refreshes de noticias

OIL_TICKER = "BZ=F"       # Brent Crude Futures no Yahoo Finance
OIL_RSS_FEEDS = [
    "https://feeds.reuters.com/reuters/businessNews",
    "https://rss.app/feeds/v1.1/_ViCnBqBWGJPZT4lF.json",  # fallback placeholder
]
BITCOIN_RSS_FEEDS = [
    "https://feeds.feedburner.com/CoinDesk",
    "https://cointelegraph.com/rss",
]

def _compute_rsi(closes: list, period: int = 14) -> float | None:
    if len(closes) < period + 1:
        return None
    gains, losses = [], []
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i - 1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - (100 / (1 + rs)), 2)

def _compute_macd(closes: list) -> tuple[float | None, float | None, str]:
    def _ema(data, n):
        k = 2 / (n + 1)
        em = data[0]
        for v in data[1:]:
            em = v * k + em * (1 - k)
        return em
    if len(closes) < 35:
        return None, None, "dados insuficientes"
    ema12 = _ema(closes[-26:], 12)
    ema26 = _ema(closes[-26:], 26)
    macd_line = ema12 - ema26
    signal_line = _ema([macd_line] * 9, 9)  # simplified; good enough for direction
    hist = macd_line - signal_line
    direction = "alta" if hist > 0 else "baixa"
    return round(macd_line, 4), round(signal_line, 4), direction

def _compute_stoch(highs, lows, closes, k=14) -> float | None:
    if len(closes) < k:
        return None
    recent_highs = highs[-k:]
    recent_lows  = lows[-k:]
    hh = max(recent_highs)
    ll = min(recent_lows)
    if hh == ll:
        return 50.0
    return round((closes[-1] - ll) / (hh - ll) * 100, 2)

def _compute_atr(highs, lows, closes, period=14) -> float | None:
    if len(closes) < period + 1:
        return None
    trs = []
    for i in range(1, len(closes)):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1]),
        )
        trs.append(tr)
    return round(sum(trs[-period:]) / period, 4)

def _fetch_price_market_technicals(mk: str) -> dict:
    """Analise para mercado de preco baseada no historico do proprio mercado."""
    now = time.time()
    cache = _tech_cache.get(mk) or {}
    if cache.get("ts") and now - cache["ts"] < _TECH_TTL:
        return cache.get("data", {})
    try:
        with history_locks[mk]:
            entries = [e for e in histories[mk] if e.get("result") in ("MAIS", "ATE")]

        if not entries:
            hf = MARKET_CONFIGS[mk]["history_file"]
            if os.path.exists(hf):
                with open(hf, encoding="utf-8") as f:
                    all_hist = json.load(f)
                entries = [e for e in all_hist if e.get("result") in ("MAIS", "ATE")]

        if not entries:
            return {}

        recent50 = entries[:50]
        recent10 = entries[:10]
        last5    = entries[:5]

        def _sobe_pct(arr):
            if not arr: return 50.0
            return round(sum(1 for e in arr if e["result"] == "MAIS") / len(arr) * 100, 1)

        rate50 = _sobe_pct(recent50)
        rate10 = _sobe_pct(recent10)
        rate5  = _sobe_pct(last5)

        # Viés geral: baseado na taxa SOBE das últimas 50 rodadas
        if rate50 >= 65:
            overall = "Compra Forte" if rate50 >= 72 else "Compra"
        elif rate50 <= 35:
            overall = "Venda Forte" if rate50 <= 28 else "Venda"
        else:
            overall = "Neutro"

        # RSI-like: taxa SOBE nas últimas 50 rodadas (escala 0-100)
        rsi = rate50
        if rsi >= 70:
            rsi_signal = "Sobrecomprado"
        elif rsi <= 30:
            rsi_signal = "Sobrevendido"
        elif rsi >= 55:
            rsi_signal = "Alta"
        elif rsi <= 45:
            rsi_signal = "Baixa"
        else:
            rsi_signal = "Neutro"

        # Momento (10r vs 50r): diferença de taxa recente vs longa
        macd_v   = round(rate10 - rate50, 2)
        macd_dir = "alta" if macd_v >= 0 else "baixa"
        macd_signal = "Alta" if macd_dir == "alta" else "Baixa"

        # Últimas 5 rodadas: taxa SOBE (0-100)
        stoch = rate5
        if stoch >= 80:
            stoch_signal = "Sobrecomprado"
        elif stoch <= 20:
            stoch_signal = "Sobrevendido"
        elif stoch >= 55:
            stoch_signal = "Alta"
        else:
            stoch_signal = "Baixa"

        # SMA-like: média dos preços de abertura (value_needed)
        prices5  = [e["value_needed"] for e in entries[:5]  if e.get("value_needed") is not None]
        prices20 = [e["value_needed"] for e in entries[:20] if e.get("value_needed") is not None]
        sma5  = round(sum(prices5)  / len(prices5),  2) if prices5  else None
        sma20 = round(sum(prices20) / len(prices20), 2) if prices20 else None

        cur_price = (
            _as_float(states[mk].get("current_count"))
            or _as_float(states[mk].get("value_needed"))
            or (prices5[0] if prices5 else None)
        )

        sma_signal = None
        if sma5 and sma20 and cur_price:
            if cur_price > sma5 > sma20:
                sma_signal = "Compra"
            elif cur_price < sma5 < sma20:
                sma_signal = "Venda"
            elif cur_price > sma20:
                sma_signal = "Compra Fraca"
            else:
                sma_signal = "Venda Fraca"

        # Volatilidade média por rodada: |preço_final - preço_inicial|
        changes = []
        for e in recent50:
            cnt = e.get("count")
            vn  = e.get("value_needed")
            if cnt is not None and vn is not None:
                try:
                    changes.append(abs(float(cnt) - float(vn)))
                except (TypeError, ValueError):
                    pass
        atr = round(sum(changes) / len(changes), 2) if changes else None

        buy_count  = sum(1 for e in recent50 if e["result"] == "MAIS")
        sell_count = sum(1 for e in recent50 if e["result"] == "ATE")

        result = {
            "market":           mk,
            "price":            cur_price,
            "rsi":              rsi,
            "rsi_signal":       rsi_signal,
            "macd":             macd_v,
            "macd_signal_line": 0.0,
            "macd_dir":         macd_dir,
            "macd_signal":      macd_signal,
            "stoch":            stoch,
            "stoch_signal":     stoch_signal,
            "atr":              atr,
            "sma5":             sma5,
            "sma20":            sma20,
            "sma_signal":       sma_signal,
            "overall":          overall,
            "buy_count":        buy_count,
            "sell_count":       sell_count,
            "rounds_analyzed":  len(recent50),
            "updated":          datetime.now().strftime("%H:%M:%S"),
        }
        _tech_cache[mk] = {"data": result, "ts": now}
        return result
    except Exception as e:
        print(f"[TechAnalysis:{mk}] Erro: {e}", flush=True)
        return cache.get("data", {})


def _fetch_market_news(mk: str) -> list:
    """Busca noticias publicas de acordo com o mercado."""
    now = time.time()
    cache = _news_cache.get(mk) or {}
    if cache.get("ts") and now - cache["ts"] < _NEWS_TTL:
        return cache.get("data", [])
    results = []
    try:
        fp = _lazy_fp()
        if mk == "bitcoin":
            feeds = BITCOIN_RSS_FEEDS
            keywords = {"bitcoin", "btc", "crypto", "cryptocurrency", "blockchain", "etf"}
            empty_message_label = "bitcoin/cripto"
        else:
            feeds = [
                "https://feeds.reuters.com/reuters/businessNews",
                "https://feeds.bbci.co.uk/news/business/rss.xml",
            ]
            keywords = {"oil", "petrol", "brent", "crude", "opec", "wti", "energia", "energy"}
            empty_message_label = "petroleo/energia"
        for url in feeds:
            try:
                feed = fp.parse(url)
                for entry in (feed.entries or [])[:30]:
                    title = entry.get("title", "")
                    summary = entry.get("summary", "")
                    combined = (title + " " + summary).lower()
                    if any(kw in combined for kw in keywords):
                        link = entry.get("link", "")
                        published = entry.get("published", "")
                        results.append({
                            "title":     title,
                            "summary":   summary[:200],
                            "link":      link,
                            "published": published,
                            "source":    feed.feed.get("title", url),
                        })
            except Exception:
                pass
        # Deduplicate by title
        seen, deduped = set(), []
        for r in results:
            if r["title"] not in seen:
                seen.add(r["title"])
                deduped.append(r)
        results = deduped[:15]
    except Exception as e:
        print(f"[News:{mk}] Erro: {e}", flush=True)
        results = cache.get("data", [])

    _news_cache[mk] = {"data": results, "ts": now}
    return results


@app.route("/api/technicals/<mk>")
def api_technicals_market(mk: str):
    if mk not in MARKET_CONFIGS:
        return jsonify({})
    if not MARKET_CONFIGS[mk].get("is_price_market"):
        return jsonify({})
    return jsonify(_fetch_price_market_technicals(mk))


@app.route("/api/news/<mk>")
def api_news_market(mk: str):
    if mk not in MARKET_CONFIGS:
        return jsonify([])
    if not MARKET_CONFIGS[mk].get("is_price_market"):
        return jsonify([])
    force = request.args.get("force") == "1"
    if force:
        _news_cache.pop(mk, None)
    return jsonify(_fetch_market_news(mk))


@app.route("/api/state")
def api_state():
    return jsonify({
        k: {
            "state":        states[k],
            "history":      list(histories[k]),
            "last_results": list(last_results[k]),
            "automation":   _automation_public_payload(k),
            "telegram":     _telegram_public_payload(k),
        }
        for k in MARKET_CONFIGS
    })


@app.route("/api/overview")
def api_overview():
    today = _today()
    if _has_session_license_context():
        score_data = _load_score()
        try:
            analytics_data = _load_private_json("signal_analytics.json", [])
            log_copy = analytics_data if isinstance(analytics_data, list) else []
        except Exception:
            log_copy = []
        balance_payload = _load_balance_timeline()
        order_log_entries = _load_automation_order_log_entries()
    else:
        _check_day_reset()
        score_data = telegram_score
        with _analytics_lock:
            log_copy = list(_analytics_log)
        with balance_timeline_lock:
            balance_payload = {
                "date": balance_timeline_data.get("date") or today,
                "currency": balance_timeline_data.get("currency") or "BRLX",
                "points": list(balance_timeline_data.get("points") or []),
            }
        with automation_order_log_lock:
            order_log_entries = list(automation_order_log)

    mk_icons = {"rodovia": "Rodovia", "rua": "Rua"}
    mk_data = {}
    for mk, cfg in MARKET_CONFIGS.items():
        st = states[mk]
        score = score_data.get(mk) or {}
        lr = list(last_results[mk])[:6]
        mk_data[mk] = {
            "name": cfg["name"],
            "icon": mk_icons.get(mk, mk),
            "connected": st.get("connected", False),
            "count": st.get("current_count"),
            "meta": st.get("value_needed"),
            "location": st.get("location"),
            "last_results": lr,
            "score": {
                "wins":   score.get("wins", 0),
                "losses": score.get("losses", 0),
            },
            "is_price_market": cfg.get("is_price_market", False),
        }

    today_signals = [
        e for e in log_copy
        if e.get("date") == today and not e.get("blocked")
    ]
    recent = list(reversed(today_signals[-15:]))

    total_wins   = sum(mk_data[mk]["score"]["wins"]   for mk in MARKET_CONFIGS)
    total_losses = sum(mk_data[mk]["score"]["losses"] for mk in MARKET_CONFIGS)
    total_sc     = total_wins + total_losses
    accuracy     = int(round(total_wins / total_sc * 100)) if total_sc else 0

    today_points = [
        p for p in (balance_payload.get("points") or [])
        if str(p.get("ts") or "").startswith(today)
    ]
    balance_currency = str(balance_payload.get("currency") or "BRLX").upper()
    start_balance = _as_float(today_points[0].get("balance")) if today_points else None
    current_balance = _as_float(today_points[-1].get("balance")) if today_points else None
    pnl_value = None
    if start_balance is not None and current_balance is not None:
        pnl_value = round(current_balance - start_balance, 2)

    last_win = None
    for entry in reversed(order_log_entries):
        if str(entry.get("outcome") or "").lower() != "win":
            continue
        amount = _as_float(entry.get("amount"))
        last_win = {
            "market": entry.get("market"),
            "market_name": entry.get("market_name"),
            "time": entry.get("resolved_at") or entry.get("time") or entry.get("ts"),
            "amount": round(amount, 2) if amount is not None else None,
            "currency": str(entry.get("currency") or balance_currency or "BRLX").upper(),
            "rec": entry.get("rec"),
            "odd": entry.get("odd"),
        }
        break

    return jsonify({
        "markets":        mk_data,
        "recent_signals": recent,
        "totals": {
            "wins":     total_wins,
            "losses":   total_losses,
            "accuracy": accuracy,
            "signals_today": len(today_signals),
        },
        "day_balance": {
            "currency": balance_currency,
            "start": start_balance,
            "current": current_balance,
            "pnl": pnl_value,
            "started_at": str(today_points[0].get("ts") or "") if today_points else None,
            "updated_at": str(today_points[-1].get("ts") or "") if today_points else None,
        },
        "last_win": last_win,
    })


@app.route("/api/analytics")
def api_analytics():
    if _has_session_license_context():
        try:
            analytics_data = _load_private_json("signal_analytics.json", [])
            log_copy = analytics_data if isinstance(analytics_data, list) else []
        except Exception:
            log_copy = []
    else:
        with _analytics_lock:
            log_copy = list(_analytics_log)

    def _strategy_short(s: str) -> str:
        s = (s or "").strip()
        if not s:
            return "Desconhecida"
        low = s.lower()
        if "xadrez" in low or "checker" in low:
            return "Xadrez"
        if "streak" in low or "virada" in low:
            return "Streak/Virada"
        if "ao vivo" in low or "ritmo" in low:
            return "Ao Vivo"
        if "semanal" in low or "weekly" in low or "mesmo horario" in low:
            return "Horário Semanal"
        if "local" in low and "hora" in low:
            return "Local+Hora"
        if "hora dominante" in low or "hour_majority" in low:
            return "Hora Dominante"
        if "padrao" in low or "padrão" in low or "confluencia" in low:
            return "Confluência"
        if "recente" in low or "recent" in low:
            return "Ritmo Recente"
        return s[:40]

    result = {}
    for mk in MARKET_CONFIGS:
        entries = [e for e in log_copy if e.get("market") == mk]
        real_bet_entries = [e for e in entries if _analytics_is_real_bet(e)]
        resolved = [e for e in real_bet_entries if e.get("outcome") in ("win", "loss")]
        blocked_entries = [e for e in entries if e.get("blocked")]
        wins = sum(1 for e in resolved if e["outcome"] == "win")
        losses = sum(1 for e in resolved if e["outcome"] == "loss")
        total = wins + losses

        strat_map: dict = {}
        for e in resolved:
            key = _strategy_short(e.get("strategy", ""))
            if key not in strat_map:
                strat_map[key] = {"wins": 0, "losses": 0}
            strat_map[key]["wins" if e["outcome"] == "win" else "losses"] += 1
        by_strategy = sorted(
            [{"name": k, "wins": v["wins"], "losses": v["losses"],
              "total": v["wins"] + v["losses"],
              "accuracy": int(round(v["wins"] / (v["wins"] + v["losses"]) * 100)) if (v["wins"] + v["losses"]) else 0}
             for k, v in strat_map.items()],
            key=lambda x: x["total"], reverse=True,
        )

        kind_map: dict = {}
        for e in resolved:
            k2 = e.get("kind") or "SINAL"
            if k2 not in kind_map:
                kind_map[k2] = {"wins": 0, "losses": 0}
            kind_map[k2]["wins" if e["outcome"] == "win" else "losses"] += 1
        by_kind = [{"kind": k, "wins": v["wins"], "losses": v["losses"]} for k, v in kind_map.items()]

        date_map: dict = {}
        for e in resolved:
            d = e.get("date", "")
            if d not in date_map:
                date_map[d] = {"wins": 0, "losses": 0}
            date_map[d]["wins" if e["outcome"] == "win" else "losses"] += 1
        timeline = [{"date": d, "wins": v["wins"], "losses": v["losses"]}
                    for d, v in sorted(date_map.items())]

        conf_map: dict = {}
        for e in resolved:
            bracket = f"{(e.get('conf', 0) // 10) * 10}-{(e.get('conf', 0) // 10) * 10 + 9}%"
            if bracket not in conf_map:
                conf_map[bracket] = {"wins": 0, "losses": 0, "floor": (e.get("conf", 0) // 10) * 10}
            conf_map[bracket]["wins" if e["outcome"] == "win" else "losses"] += 1
        by_confidence = sorted(
            [{"bracket": b, **v} for b, v in conf_map.items()],
            key=lambda x: x["floor"],
        )

        hour_map: dict = {}
        hour_map_today: dict = {}
        today_key = datetime.now().strftime("%Y-%m-%d")
        for e in resolved:
            time_str = str(e.get("time") or "")
            try:
                hour = int(time_str[:2])
            except (TypeError, ValueError):
                continue
            for target_map in (hour_map, hour_map_today if str(e.get("date") or "") == today_key else None):
                if target_map is None:
                    continue
                if hour not in target_map:
                    target_map[hour] = {
                        "wins": 0,
                        "losses": 0,
                        "total": 0,
                        "strategy_counts": {},
                        "rec_counts": {},
                    }
                stat = target_map[hour]
                stat["wins" if e["outcome"] == "win" else "losses"] += 1
                stat["total"] += 1

                strategy_name = _strategy_short(e.get("strategy", ""))
                stat["strategy_counts"][strategy_name] = stat["strategy_counts"].get(strategy_name, 0) + 1

                rec_name = str(e.get("rec") or "--").upper()
                stat["rec_counts"][rec_name] = stat["rec_counts"].get(rec_name, 0) + 1

        def _build_hour_strategy_rows(source_map: dict) -> list[dict]:
            rows = []
            for hour in sorted(source_map):
                stat = source_map[hour]
                total_hour = stat["total"]
                top_strategy = "--"
                top_strategy_total = 0
                if stat["strategy_counts"]:
                    top_strategy, top_strategy_total = max(
                        stat["strategy_counts"].items(),
                        key=lambda item: item[1],
                    )
                top_rec = "--"
                if stat["rec_counts"]:
                    top_rec = max(stat["rec_counts"].items(), key=lambda item: item[1])[0]
                rows.append({
                    "hour": hour,
                    "wins": stat["wins"],
                    "losses": stat["losses"],
                    "total": total_hour,
                    "accuracy": int(round((stat["wins"] / total_hour) * 100)) if total_hour else 0,
                    "strategy": top_strategy,
                    "strategy_total": top_strategy_total,
                    "rec": top_rec,
                })
            return rows

        by_hour_strategy = _build_hour_strategy_rows(hour_map)
        by_hour_strategy_today = _build_hour_strategy_rows(hour_map_today)

        # Contagem de bloqueios por motivo
        block_map: dict = {}
        for e in blocked_entries:
            reason = (e.get("blocked_reason") or "outro").split(":")[0]
            block_map[reason] = block_map.get(reason, 0) + 1
        by_blocked_reason = [{"reason": r, "count": c} for r, c in sorted(block_map.items(), key=lambda x: -x[1])]

        # Stop consecutivo atual
        cons_losses = _consecutive_losses_for_market(mk)
        cons_pause  = _consecutive_loss_pause.get(mk, 0)

        result[mk] = {
            "total": total,
            "wins": wins,
            "losses": losses,
            "pending": sum(1 for e in real_bet_entries if e.get("outcome") is None and not e.get("blocked")),
            "blocked": len(blocked_entries),
            "accuracy": int(round(wins / total * 100)) if total else 0,
            "consecutive_losses": cons_losses,
            "consecutive_loss_pause": cons_pause,
            "signals_total": sum(1 for e in entries if not e.get("blocked")),
            "bets_total": len(real_bet_entries),
            "by_strategy": by_strategy,
            "by_kind": by_kind,
            "timeline": timeline,
            "by_confidence": by_confidence,
            "by_hour_strategy": by_hour_strategy,
            "by_hour_strategy_today": by_hour_strategy_today,
            "by_blocked_reason": by_blocked_reason,
            "recent": list(reversed(entries[-30:])),
        }

    return jsonify(result)


@app.route("/api/telegram/<mk>", methods=["GET", "POST"])
def api_telegram(mk: str):
    if mk not in MARKET_CONFIGS:
        return jsonify({"success": False, "message": "Mercado invalido."}), 404

    if request.method == "POST":
        payload = request.get_json(silent=True) or {}
        request_cfg = _load_telegram_config() if _has_session_license_context() else None
        with telegram_lock:
            target_cfg = request_cfg if request_cfg is not None else telegram_cfg
            target_cfg["enabled"] = bool(payload.get("enabled", target_cfg.get("enabled")))

            bot_token = str(payload.get("bot_token") or "").strip()
            if bot_token:
                target_cfg["bot_token"] = bot_token

            chat_id = str(payload.get("chat_id") or "").strip()
            if chat_id:
                target_cfg["chat_id"] = chat_id

            markets_cfg = target_cfg.setdefault("markets", {})
            market_cfg = markets_cfg.setdefault(mk, {})
            signal_min = _as_float(payload.get("signal_min_confidence"))
            if signal_min is not None:
                market_cfg["signal_min_confidence"] = max(50.0, min(95.0, signal_min))

            _save_telegram_config(target_cfg)

        _refresh_telegram_status_after_config(mk, base_message="Configuracao do Telegram salva.")
        _sync_runtime_state_for_current_session()
        _broadcast_all_automation()

    return jsonify({"success": True, "data": _telegram_public_payload(mk)})


@app.route("/api/telegram/<mk>/start", methods=["POST"])
def api_telegram_start(mk: str):
    if mk not in MARKET_CONFIGS:
        return jsonify({"success": False, "message": "Mercado invalido."}), 404

    missing_creds = False
    request_cfg = _load_telegram_config() if _has_session_license_context() else None
    with telegram_lock:
        target_cfg = request_cfg if request_cfg is not None else telegram_cfg
        bot_token = str(target_cfg.get("bot_token") or "").strip()
        chat_id = str(target_cfg.get("chat_id") or "").strip()
        if not bot_token or not chat_id:
            target_cfg["enabled"] = False
            _save_telegram_config(target_cfg)
            missing_creds = True
        else:
            target_cfg["enabled"] = True
            _save_telegram_config(target_cfg)

    if missing_creds:
        _refresh_telegram_status_after_config(mk, base_message="Nao foi possivel iniciar.")
        return jsonify({
            "success": False,
            "message": "Salve o token e o chat ID antes de iniciar o Telegram.",
            "data": _telegram_public_payload(mk),
        }), 400

    _refresh_telegram_status_after_config(mk, base_message="Telegram iniciado.")
    _sync_runtime_state_for_current_session()
    _broadcast_all_automation()
    return jsonify({"success": True, "data": _telegram_public_payload(mk)})


@app.route("/api/telegram/<mk>/stop", methods=["POST"])
def api_telegram_stop(mk: str):
    if mk not in MARKET_CONFIGS:
        return jsonify({"success": False, "message": "Mercado invalido."}), 404

    request_cfg = _load_telegram_config() if _has_session_license_context() else None
    with telegram_lock:
        target_cfg = request_cfg if request_cfg is not None else telegram_cfg
        target_cfg["enabled"] = False
        _save_telegram_config(target_cfg)

    _refresh_telegram_status_after_config(mk, base_message="Telegram pausado.")
    _sync_runtime_state_for_current_session()
    _broadcast_all_automation()
    return jsonify({"success": True, "data": _telegram_public_payload(mk)})


@app.route("/api/telegram/<mk>/send", methods=["POST"])
def api_telegram_send(mk: str):
    if mk not in MARKET_CONFIGS:
        return jsonify({"success": False, "message": "Mercado invalido."}), 404

    payload = request.get_json(silent=True) or {}
    text = str(payload.get("text") or "").strip()
    parse_mode = payload.get("parse_mode")
    if not text:
        return jsonify({"success": False, "message": "Digite uma mensagem antes de enviar."}), 400

    ok, error = _telegram_send_now(text, market_key=mk, parse_mode=parse_mode)
    if not ok:
        return jsonify({"success": False, "message": error or "Falha ao enviar mensagem."}), 400
    return jsonify({"success": True, "data": _telegram_public_payload(mk)})


@app.route("/api/telegram/score/reset", methods=["POST"])
def api_telegram_score_reset():
    score_data = _reset_score_today()
    return jsonify({
        "success": True,
        "data": {k: score_data[k] for k in MARKET_CONFIGS},
        "message": "Placar do dia zerado.",
    })


@app.route("/api/license", methods=["GET", "POST", "DELETE"])
def api_license():
    if request.method == "GET":
        return jsonify({"success": True, "data": _license_public_payload()})

    if request.method == "DELETE":
        if has_request_context():
            session.clear()
        else:
            with license_lock:
                license_state["license_key"] = ""
                license_state["activated_at"] = None
                license_state["device_fingerprint"] = _device_fingerprint()
                _save_license_state_unsafe()
            _reload_private_runtime_state()
        return jsonify({
            "success": True,
            "message": "License key removida deste dispositivo.",
            "data": _license_public_payload(),
        })

    payload = request.get_json(silent=True) or {}
    requested_key = str(payload.get("key") or "").strip()
    if has_request_context():
        current_key = str(session.get("license_key") or "").strip().upper()
        if current_key and requested_key and requested_key.upper() == current_key and _license_is_active():
            return jsonify({
                "success": True,
                "message": "Sessao da license key mantida.",
                "data": _license_public_payload(),
            })
        if requested_key and requested_key.upper() != current_key:
            session.clear()
    ok, message, data = _activate_license_key(requested_key)
    if not ok and has_request_context():
        session.clear()
    return jsonify({"success": ok, "message": message, "data": data}), (200 if ok else 400)


@app.route("/api/automation/<mk>", methods=["GET", "POST"])
def api_automation(mk: str):
    if mk not in MARKET_CONFIGS:
        return jsonify({"success": False, "message": "Mercado invalido."}), 404

    if request.method == "GET":
        try:
            _automation_reconcile_open_position(mk)
        except Exception as ex:
            print(f"[AUTO:{mk}] erro ao reconciliar posicao aberta no GET: {ex}", flush=True)
        return jsonify({"success": True, "data": _automation_public_payload(mk)})
    if not _license_is_active():
        return _license_block_response()

    payload = request.get_json(silent=True) or {}
    request_raw = _load_automation_raw() if _has_session_license_context() else None
    with automation_lock:
        current_cfg_map = _load_automation_config(request_raw) if request_raw is not None else automation_cfg
        current_meta = _load_automation_meta(request_raw) if request_raw is not None else automation_meta
        cfg = current_cfg_map.get(mk)
        if not cfg:
            cfg = _default_automation_cfg()
            current_cfg_map[mk] = cfg

        if "active_profile" in payload:
            current_meta["active_profile"] = _normalize_automation_profile_key(payload.get("active_profile"))
        if "execution_profile_mode" in payload:
            current_meta["execution_profile_mode"] = _normalize_execution_profile_mode(
                payload.get("execution_profile_mode"),
                fallback=current_meta.get("active_profile") or "a",
            )

        if "enabled" in payload:
            cfg["enabled"] = bool(payload.get("enabled"))

        api_key = str(payload.get("api_key") or "").strip()
        private_key = str(payload.get("private_key") or "").strip()
        if _looks_like_masked_secret(api_key):
            api_key = ""
        if _looks_like_masked_secret(private_key):
            private_key = ""

        stake_mode = str(payload.get("stake_mode") or cfg.get("stake_mode") or "fixed")
        stake_mode = "percent" if stake_mode == "percent" else "fixed"
        stake_value = _as_float(payload.get("stake_value"))
        currency = str(payload.get("currency") or cfg.get("currency") or "BRLX").strip().upper()
        safe_mode_enabled = (
            bool(payload.get("safe_mode_enabled"))
            if "safe_mode_enabled" in payload else
            bool(cfg.get("safe_mode_enabled"))
        )
        safe_min_confidence = _as_float(payload.get("safe_min_confidence"))
        if safe_min_confidence is None or safe_min_confidence <= 0:
            safe_min_confidence = _as_float(cfg.get("safe_min_confidence")) or 70.0
        sniper_enabled = (
            bool(payload.get("sniper_enabled"))
            if "sniper_enabled" in payload else
            bool(cfg.get("sniper_enabled") or cfg.get("sniper_only_enabled"))
        )
        sniper_min_multiplier = _as_float(payload.get("sniper_min_multiplier"))
        if sniper_min_multiplier is None or sniper_min_multiplier <= 0:
            sniper_min_multiplier = _as_float(cfg.get("sniper_min_multiplier")) or 3.0
        arbitrage_enabled = (
            bool(payload.get("arbitrage_enabled"))
            if "arbitrage_enabled" in payload else
            bool(cfg.get("arbitrage_enabled"))
        )
        arbitrage_max_sum = _as_float(payload.get("arbitrage_max_sum"))
        if arbitrage_max_sum is None or arbitrage_max_sum <= 0:
            arbitrage_max_sum = _as_float(cfg.get("arbitrage_max_sum")) or AUTOMATION_ARB_DEFAULT_MAX_SUM
        cashout_enabled = bool(payload.get("cashout_enabled")) if "cashout_enabled" in payload else bool(cfg.get("cashout_enabled"))
        cashout_profit_pct = _as_float(payload.get("cashout_profit_pct"))
        if cashout_profit_pct is None or cashout_profit_pct <= 0:
            cashout_profit_pct = _as_float(cfg.get("cashout_profit_pct")) or 20.0

        # Credenciais e stake sao compartilhados entre os mercados.
        for market_key in MARKET_CONFIGS:
            shared_cfg = current_cfg_map.get(market_key)
            if not shared_cfg:
                shared_cfg = _default_automation_cfg()
                current_cfg_map[market_key] = shared_cfg
            shared_cfg["stake_mode"] = stake_mode
            if stake_value is not None and stake_value > 0:
                shared_cfg["stake_value"] = stake_value
            if currency:
                shared_cfg["currency"] = currency
            if api_key:
                shared_cfg["api_key"] = api_key
            if private_key:
                shared_cfg["private_key"] = private_key

        cfg = current_cfg_map[mk]
        cfg["safe_mode_enabled"] = safe_mode_enabled
        cfg["safe_min_confidence"] = max(50.0, min(95.0, safe_min_confidence))
        cfg["sniper_enabled"] = sniper_enabled
        cfg["sniper_min_multiplier"] = max(1.5, sniper_min_multiplier)
        cfg["arbitrage_enabled"] = arbitrage_enabled
        cfg["arbitrage_max_sum"] = min(0.99, max(0.50, arbitrage_max_sum))
        cfg["cashout_enabled"] = cashout_enabled
        cfg["cashout_profit_pct"] = max(1.0, cashout_profit_pct)

        if cfg["enabled"]:
            if (
                not str(cfg.get("api_key") or "").strip()
                or not str(cfg.get("private_key") or "").strip()
            ):
                cfg["enabled"] = False
                cfg["last_order_status"] = "error"
                cfg["last_order_message"] = "Preencha API key e private key antes de habilitar."
            else:
                cfg["last_order_status"] = "ready"
                cfg["last_order_message"] = "Mercado habilitado. Use Iniciar para o bot comecar a apostar."
                if cfg.get("safe_mode_enabled"):
                    cfg["last_order_message"] += (
                        f" Modo seguro >= {int(round(cfg.get('safe_min_confidence') or 70))}%."
                    )
                if cfg.get("sniper_enabled"):
                    cfg["last_order_message"] += (
                        f" Sniper ligado sem fallback ({float(cfg.get('sniper_min_multiplier') or 3.0):.1f}x+)."
                    )
                if cfg.get("arbitrage_enabled"):
                    cfg["last_order_message"] += (
                        f" Arbitragem sniper <= {float(cfg.get('arbitrage_max_sum') or AUTOMATION_ARB_DEFAULT_MAX_SUM):.2f}."
                    )
        else:
            cfg["last_order_status"] = "idle"
            cfg["last_order_message"] = "Mercado desabilitado para automacao."

        cfg["last_order_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        _save_automation_config(current_cfg_map, current_meta)

    _sync_runtime_state_for_current_session()
    _broadcast_all_automation()
    return jsonify({"success": True, "data": _automation_public_payload(mk)})


@app.route("/api/automation/<mk>/refresh-balance", methods=["POST"])
def api_automation_refresh_balance(mk: str):
    if mk not in MARKET_CONFIGS:
        return jsonify({"success": False, "message": "Mercado invalido."}), 404
    try:
        _automation_reconcile_open_position(mk, min_check_interval=0.0)
    except Exception as ex:
        print(f"[AUTO:{mk}] erro ao reconciliar posicao antes do refresh saldo: {ex}", flush=True)
    _automation_refresh_balance_async(mk)
    return jsonify({"success": True, "data": _automation_public_payload(mk)})


@app.route("/api/automation/<mk>/clear-keys", methods=["POST"])
def api_automation_clear_keys(mk: str):
    if mk not in MARKET_CONFIGS:
        return jsonify({"success": False, "message": "Mercado invalido."}), 404
    if not _license_is_active():
        return _license_block_response()
    request_raw = _load_automation_raw() if _has_session_license_context() else None
    with automation_lock:
        current_cfg_map = _load_automation_config(request_raw) if request_raw is not None else automation_cfg
        current_meta = _load_automation_meta(request_raw) if request_raw is not None else automation_meta
        for market_key in MARKET_CONFIGS:
            cfg = current_cfg_map.get(market_key)
            if not cfg:
                cfg = _default_automation_cfg()
                current_cfg_map[market_key] = cfg
            cfg["api_key"] = ""
            cfg["private_key"] = ""
            cfg["balance"] = None
            cfg["last_balance_at"] = None
            cfg["enabled"] = False
            cfg["last_order_status"] = "idle"
            cfg["last_order_message"] = "Credenciais limpas. Cole API key e private key para operar."
            cfg["last_order_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        current_meta["running"] = False
        _save_automation_config(current_cfg_map, current_meta)
    _sync_runtime_state_for_current_session()
    _broadcast_all_automation()
    data = _automation_public_payload(mk)
    data["credentials_cleared"] = True
    return jsonify({"success": True, "message": "Credenciais limpas.", "data": data})


@app.route("/api/automation/<mk>/orders-log", methods=["GET"])
def api_automation_orders_log(mk: str):
    if mk not in MARKET_CONFIGS:
        return jsonify({"success": False, "message": "Mercado invalido."}), 404
    return jsonify({"success": True, "data": _automation_order_log_public(mk, limit=25)})


@app.route("/api/automation/<mk>/test-order", methods=["POST"])
def api_automation_test_order(mk: str):
    if mk not in MARKET_CONFIGS:
        return jsonify({"success": False, "message": "Mercado invalido."}), 404
    if not _license_is_active():
        return _license_block_response()

    license_key = _current_license_key() or "DEFAULT"
    request_id = str(
        request.headers.get("X-Request-Id")
        or ((request.get_json(silent=True) or {}).get("request_id"))
        or ""
    ).strip()
    dedupe_key = f"{license_key}:{mk}:{request_id}" if request_id else f"{license_key}:{mk}:cooldown"
    now_ts = time.time()
    with _automation_test_order_lock:
        expired_keys = [
            key for key, ts in _automation_test_order_recent.items()
            if now_ts - ts > max(30.0, AUTOMATION_TEST_ORDER_COOLDOWN_SECONDS * 4)
        ]
        for key in expired_keys:
            _automation_test_order_recent.pop(key, None)
        previous_ts = _automation_test_order_recent.get(dedupe_key)
        if previous_ts and now_ts - previous_ts < AUTOMATION_TEST_ORDER_COOLDOWN_SECONDS:
            return jsonify({
                "success": False,
                "message": "Teste duplicado bloqueado. Aguarde alguns segundos antes de tentar novamente.",
                "data": _automation_public_payload(mk),
            }), 409
        _automation_test_order_recent[dedupe_key] = now_ts

    payload = request.get_json(silent=True) or {}
    amount = _as_float(payload.get("amount"))
    if amount is None or amount <= 0:
        amount = AUTOMATION_TEST_ORDER_AMOUNT
    amount = max(AUTOMATION_MIN_ORDER_TOTAL, round(amount, 2))

    market_id = payload.get("market_id") or states[mk].get("market_id")
    rec = str(payload.get("rec") or "").strip().upper()
    if rec == "ATÉ":
        rec = "ATE"
    if rec not in ("MAIS", "ATE"):
        rec = _automation_pick_test_rec(mk, market_id)

    request_raw = _load_automation_raw() if _has_session_license_context() else None
    with automation_lock:
        current_cfg_map = _load_automation_config(request_raw) if request_raw is not None else automation_cfg
        cfg = dict(current_cfg_map.get(mk) or _default_automation_cfg())

    if not str(cfg.get("api_key") or "").strip() or not str(cfg.get("private_key") or "").strip():
        _set_automation_status(
            mk,
            "error",
            "Preencha API key e private key antes do teste real.",
            market_id=market_id,
            side=rec,
            kind="TESTE",
            amount=amount,
        )
        return jsonify({
            "success": False,
            "message": "Credenciais nao preenchidas.",
            "data": _automation_public_payload(mk),
        }), 400

    if _as_float(cfg.get("balance")) is None:
        _automation_refresh_balance(mk)
        with automation_lock:
            refreshed_cfg_map = _load_automation_config(_load_automation_raw()) if _has_session_license_context() else automation_cfg
            cfg = dict(refreshed_cfg_map.get(mk) or _default_automation_cfg())

    balance = _as_float(cfg.get("balance"))
    currency = cfg.get("currency") or "BRLX"
    if balance is not None and balance <= 0:
        _set_automation_status(
            mk,
            "error",
            f"Sem saldo disponivel para teste ({balance:.2f} {currency}).",
            market_id=market_id,
            side=rec,
            kind="TESTE",
            amount=amount,
        )
        _send_automation_order_telegram(
            mk,
            status="error",
            market_id=market_id or "--",
            rec=rec,
            kind="TESTE",
            amount=amount,
            detail="Sem saldo disponivel para executar o teste real.",
        )
        return jsonify({
            "success": False,
            "message": "Sem saldo disponivel.",
            "data": _automation_public_payload(mk),
        }), 400

    if balance is not None and balance < AUTOMATION_MIN_ORDER_TOTAL:
        _set_automation_status(
            mk,
            "error",
            (
                f"Saldo abaixo do minimo da API para teste: "
                f"{balance:.2f} {currency} < {AUTOMATION_MIN_ORDER_TOTAL:.2f} {currency}."
            ),
            market_id=market_id,
            side=rec,
            kind="TESTE",
            amount=amount,
        )
        return jsonify({
            "success": False,
            "message": (
                f"Saldo abaixo do minimo da API ({AUTOMATION_MIN_ORDER_TOTAL:.2f} {currency})."
            ),
            "data": _automation_public_payload(mk),
        }), 400

    if balance is not None and amount > balance:
        _set_automation_status(
            mk,
            "error",
            f"Teste minimo {amount:.2f} {currency} maior que saldo {balance:.2f} {currency}.",
            market_id=market_id,
            side=rec,
            kind="TESTE",
            amount=amount,
        )
        return jsonify({
            "success": False,
            "message": "Valor do teste maior que o saldo.",
            "data": _automation_public_payload(mk),
        }), 400

    if not market_id:
        _set_automation_status(
            mk,
            "error",
            "Sem rodada ativa para executar o teste real.",
            kind="TESTE",
            amount=amount,
        )
        _send_automation_order_telegram(
            mk,
            status="error",
            market_id="--",
            rec=rec,
            kind="TESTE",
            amount=amount,
            detail="Sem rodada ativa na aba selecionada para testar agora.",
        )
        return jsonify({
            "success": False,
            "message": "Sem rodada ativa.",
            "data": _automation_public_payload(mk),
        }), 400

    if not rec:
        _set_automation_status(
            mk,
            "error",
            "Nao achei lado disponivel para o teste real.",
            market_id=market_id,
            kind="TESTE",
            amount=amount,
        )
        return jsonify({
            "success": False,
            "message": "Sem selecao disponivel para testar.",
            "data": _automation_public_payload(mk),
        }), 400

    _set_automation_status(
        mk,
        "ready",
        f"Enviando ordem de teste real de {amount:.2f} {currency}...",
        market_id=market_id,
        side=rec,
        kind="TESTE",
        amount=amount,
    )
    success = _automation_submit_order(
        mk,
        market_id,
        rec,
        "TESTE",
        amount,
        conf_pct=None,
        dedupe_market=False,
    )
    status_code = 200 if success else 400
    return jsonify({
        "success": success,
        "message": (
            f"Teste real enviado com {amount:.2f} {currency}."
            if success else
            "Teste real nao foi aceito pela API."
        ),
        "data": _automation_public_payload(mk),
    }), status_code


@app.route("/api/automation/global", methods=["GET", "POST"])
def api_automation_global():
    if request.method == "GET":
        if _has_session_license_context():
            request_raw = _load_automation_raw()
            current_cfg_map = _load_automation_config(request_raw)
            current_meta = _load_automation_meta(request_raw)
        else:
            current_cfg_map = automation_cfg
            current_meta = automation_meta
        return jsonify({
            "success": True,
            "data": {
                "running": bool(current_meta.get("running")),
                "market_selection_mode": _normalize_market_selection_mode(current_meta.get("market_selection_mode")),
                "enabled_markets": _automation_effective_enabled_markets(current_cfg_map, current_meta),
                "manual_enabled_markets": {
                    market_key: bool((current_cfg_map.get(market_key) or {}).get("enabled"))
                    for market_key in MARKET_CONFIGS
                },
            },
        })
    if not _license_is_active():
        return _license_block_response()

    payload = request.get_json(silent=True) or {}
    requested_running = payload.get("running") if "running" in payload else None
    should_notify_running = False
    request_raw = _load_automation_raw() if _has_session_license_context() else None
    current_cfg_map = _load_automation_config(request_raw) if request_raw is not None else automation_cfg
    current_meta = _load_automation_meta(request_raw) if request_raw is not None else automation_meta
    if requested_running is True:
        current_key = _current_license_key()
        for market_key in MARKET_CONFIGS:
            cfg = dict(current_cfg_map.get(market_key) or _default_automation_cfg())
            if not bool(cfg.get("enabled")):
                continue
            duplicate_owner = _running_license_with_same_api_credentials(current_key, market_key, cfg)
            if duplicate_owner:
                other_key, other_customer = duplicate_owner
                owner_label = other_customer or _masked_license_key(other_key)
                return jsonify({
                    "success": False,
                    "message": (
                        f"Essa API ja esta em uso por outra licenca ativa ({owner_label} / {_masked_license_key(other_key)}). "
                        "Pare a outra licenca ou troque as credenciais para evitar aposta duplicada."
                    ),
                }), 409
    running_now = bool(current_meta.get("running"))
    with automation_lock:
        if "running" in payload:
            previous_running = bool(current_meta.get("running"))
            current_meta["running"] = bool(payload.get("running"))
            running_now = bool(current_meta.get("running"))
            should_notify_running = previous_running != running_now
        if "market_selection_mode" in payload:
            current_meta["market_selection_mode"] = _normalize_market_selection_mode(
                payload.get("market_selection_mode")
            )

        enabled_markets = payload.get("enabled_markets")
        if isinstance(enabled_markets, dict):
            for market_key in MARKET_CONFIGS:
                if market_key in enabled_markets:
                    cfg = current_cfg_map.get(market_key)
                    if not cfg:
                        cfg = _default_automation_cfg()
                        current_cfg_map[market_key] = cfg
                    cfg["enabled"] = bool(enabled_markets.get(market_key))

        for market_key in MARKET_CONFIGS:
            cfg = current_cfg_map.get(market_key)
            if not cfg:
                continue
            if not cfg.get("enabled"):
                cfg["last_order_status"] = "idle"
                cfg["last_order_message"] = "Mercado desabilitado para automacao."
            elif not running_now:
                cfg["last_order_status"] = "idle"
                cfg["last_order_message"] = "Bot pausado. Nenhuma ordem automatica sera enviada."
            elif (
                not str(cfg.get("api_key") or "").strip()
                or not str(cfg.get("private_key") or "").strip()
            ):
                cfg["last_order_status"] = "error"
                cfg["last_order_message"] = "Preencha API key e private key antes de operar."
            else:
                cfg["last_order_status"] = "ready"
                cfg["last_order_message"] = "Bot iniciado. Aguardando sinal habilitado."
                if cfg.get("safe_mode_enabled"):
                    cfg["last_order_message"] += (
                        f" Modo seguro >= {int(round(cfg.get('safe_min_confidence') or 70))}%."
                    )
                if cfg.get("sniper_enabled"):
                    cfg["last_order_message"] += (
                        f" Sniper ligado sem fallback ({float(cfg.get('sniper_min_multiplier') or 3.0):.1f}x+)."
                    )
                if cfg.get("arbitrage_enabled"):
                    cfg["last_order_message"] += (
                        f" Arbitragem sniper <= {float(cfg.get('arbitrage_max_sum') or AUTOMATION_ARB_DEFAULT_MAX_SUM):.2f}."
                    )
            cfg["last_order_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        _save_automation_config(current_cfg_map, current_meta)

    _sync_runtime_state_for_current_session(force=bool(isinstance(requested_running, bool)))
    _broadcast_all_automation()
    if should_notify_running:
        _send_automation_running_telegram(running_now)
    return jsonify({
        "success": True,
        "data": {
            "running": bool(current_meta.get("running")),
            "market_selection_mode": _normalize_market_selection_mode(current_meta.get("market_selection_mode")),
            "enabled_markets": _automation_effective_enabled_markets(current_cfg_map, current_meta),
            "manual_enabled_markets": {
                market_key: bool((current_cfg_map.get(market_key) or {}).get("enabled"))
                for market_key in MARKET_CONFIGS
            },
        },
    })


def market_status_monitor_worker():
    """
    A cada 5 min verifica se cada mercado está disponível na API.
    - Se offline: notifica Telegram uma vez e continua avisando periodicamente.
    - Se voltar: notifica retorno e envia sinal de abertura.
    """
    CHECK_INTERVAL = 300  # 5 minutos
    NOTIFY_REPEAT  = 900  # re-notifica offline a cada 15 min

    # estado por mercado
    _offline_since: dict[str, float] = {}   # mk -> timestamp que ficou offline
    _last_notify:   dict[str, float] = {}   # mk -> último timestamp que notificamos

    time.sleep(60)  # aguarda o servidor estabilizar antes do primeiro check

    while True:
        try:
            for mk, cfg in MARKET_CONFIGS.items():
                mdata = get_market_data(cfg["slug_keyword"])
                name  = cfg["name"]
                now   = time.time()

                if mdata is None:
                    # Mercado offline
                    if mk not in _offline_since:
                        _offline_since[mk] = now
                        print(f"[STATUS:{mk}] Offline — notificando Telegram", flush=True)

                    elapsed = int(now - _offline_since[mk])
                    last    = _last_notify.get(mk, 0)

                    if now - last >= NOTIFY_REPEAT:
                        mins = elapsed // 60
                        dur_str = f"{mins} min" if mins > 0 else "agora"
                        msg = (
                            f"🔴 <b>{name}</b> está <b>fora do ar</b>\n"
                            f"⏱ Sem mercado disponível há {dur_str}\n"
                            f"\n"
                            f"<i>O bot retomará sinais automaticamente quando o mercado voltar.</i>"
                        )
                        send_telegram(msg)
                        _last_notify[mk] = now

                else:
                    # Mercado disponível
                    if mk in _offline_since:
                        # Voltou online — notifica e envia sinal
                        down_secs = int(now - _offline_since.pop(mk))
                        _last_notify.pop(mk, None)
                        mins = down_secs // 60
                        dur_str = f"{mins} min" if mins > 0 else "alguns instantes"
                        print(f"[STATUS:{mk}] Voltou online após {down_secs}s", flush=True)
                        msg = (
                            f"🟢 <b>{name}</b> voltou ao ar!\n"
                            f"⏱ Ficou offline por ~{dur_str}\n"
                            f"\n"
                            f" {mdata.get('location', '').split(' - ')[0]}\n"
                            f"🎯 Meta: <b>{mdata.get('value_needed', '?')}</b>\n"
                            f"\n"
                            f"<i>Sinais retomados automaticamente.</i>"
                        )
                        send_telegram(msg)

                        # Re-subscreve no WebSocket se necessário
                        if ws_instance:
                            slug = mdata.get("slug", "")
                            if f"markets-{slug}" != current_channels.get(mk):
                                subscribe_market(ws_instance, mk, mdata)

                        # Envia sinal de abertura
                        _send_signal(
                            mk,
                            mdata.get("remaining_seconds"),
                            mdata.get("location"),
                            mdata.get("value_needed"),
                            mdata.get("numeric_id"),
                        )

        except Exception as e:
            print(f"[STATUS] Erro no monitor: {e}", flush=True)

        time.sleep(CHECK_INTERVAL)


def daily_summary_worker():
    """Envia resumo diário no Telegram às 23h."""
    _sent_today: str | None = None
    while True:
        try:
            now = datetime.now()
            today = now.strftime("%Y-%m-%d")
            if now.hour == 23 and _sent_today != today:
                _sent_today = today
                today_entries = [
                    e for e in _analytics_log
                    if e.get("date") == today and not e.get("blocked")
                ]
                today_real_bets = [e for e in today_entries if _analytics_is_real_bet(e)]
                wins   = sum(1 for e in today_real_bets if e.get("outcome") == "win")
                losses = sum(1 for e in today_real_bets if e.get("outcome") == "loss")
                pending = sum(1 for e in today_real_bets if e.get("outcome") is None)
                blocked = sum(1 for e in _analytics_log if e.get("date") == today and e.get("blocked"))
                total_decided = wins + losses
                acc_str = f"{int(wins/total_decided*100)}%" if total_decided > 0 else "--"
                # Saldo: primeiro e último do dia
                with balance_timeline_lock:
                    day_points = [
                        p for p in (balance_timeline_data.get("points") or [])
                        if str(p.get("ts") or "").startswith(today)
                    ]
                currency = (balance_timeline_data.get("currency") or "BRLX") if day_points else "BRLX"
                if day_points:
                    bal_start = _as_float(day_points[0].get("balance"))
                    bal_end   = _as_float(day_points[-1].get("balance"))
                    if bal_start and bal_end:
                        pnl = bal_end - bal_start
                        pnl_sign = "+" if pnl >= 0 else ""
                        pnl_icon = "📈" if pnl >= 0 else "📉"
                        pnl_str = f"{pnl_icon} P&L: <b>{pnl_sign}{pnl:.2f} {currency}</b> ({bal_start:.2f} → {bal_end:.2f})"
                    else:
                        pnl_str = "P&L: sem dados de saldo"
                else:
                    pnl_str = "P&L: sem dados de saldo"
                # Por mercado
                market_lines = ""
                for mk in MARKET_CONFIGS:
                    mk_entries = [e for e in today_entries if e.get("market") == mk]
                    mk_wins  = sum(1 for e in mk_entries if e.get("outcome") == "win")
                    mk_loss  = sum(1 for e in mk_entries if e.get("outcome") == "loss")
                    if mk_wins + mk_loss == 0:
                        continue
                    mk_acc = f"{int(mk_wins/(mk_wins+mk_loss)*100)}%"
                    market_lines += f"  • {MARKET_CONFIGS[mk]['name']}: ✅{mk_wins} ❌{mk_loss} ({mk_acc})\n"
                sep = "━━━━━━━━━━━━━━━━"
                msg = (
                    f"🌙 <b>RESUMO DO DIA — {today}</b>\n"
                    f"{sep}\n"
                    f"✅ Acertos: <b>{wins}</b>  ❌ Erros: <b>{losses}</b>  ⏳ Pendentes: <b>{pending}</b>\n"
                    f"📊 Acurácia: <b>{acc_str}</b>  •  🚫 Bloqueados: <b>{blocked}</b>\n"
                    f"{pnl_str}\n"
                )
                if market_lines:
                    msg += f"{sep}\n{market_lines}"
                msg += f"{sep}\n<i>⚠️ Apenas probabilidade. Aposte por conta e risco.</i>"
                send_telegram(msg)
        except Exception as ex:
            print(f"[DAILY_SUMMARY] erro: {ex}", flush=True)
        time.sleep(60)


if __name__ == "__main__":
    import sys, io
    if hasattr(sys.stdout, "buffer"):
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "buffer"):
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")
    for mk in MARKET_CONFIGS:
        load_history(mk)
        with history_locks[mk]:
            latest = histories[mk][0] if histories[mk] else None
        if latest and latest.get("market_id"):
            _telegram_result_notified_mid[mk] = str(latest.get("market_id"))

    if AUTO_BACKFILL_HISTORY:
        print(
            f"[API] backfill em background ligado "
            f"({HISTORY_LOOKBACK_DAYS} dias)",
            flush=True,
        )
    else:
        print(
            "[API] backfill automatico desativado; usando JSON local + rodadas novas",
            flush=True,
        )
    threading.Thread(target=ws_thread,       daemon=True).start()
    threading.Thread(target=channel_watcher, daemon=True).start()
    threading.Thread(target=price_market_poll_worker, daemon=True).start()
    threading.Thread(target=result_telegram_fallback_worker, daemon=True).start()
    threading.Thread(target=automation_arbitrage_worker, daemon=True).start()
    threading.Thread(target=automation_cashout_worker, daemon=True).start()
    threading.Thread(target=daily_summary_worker, daemon=True).start()
    threading.Thread(target=market_status_monitor_worker, daemon=True).start()
    # ── Bias adaptativo: calcula na inicialização e agenda atualização horária ──
    _recompute_bias_config()
    threading.Thread(target=_bias_updater_loop, daemon=True, name="bias-updater").start()
    # threading.Thread(target=bankroll_goal_reminder_worker, daemon=True).start()
    for mk in MARKET_CONFIGS:
        # Sempre busca histórico novo (só adiciona rodadas ainda não salvas)
        if AUTO_BACKFILL_HISTORY:
            threading.Thread(
                target=history_backfill_worker,
                args=(mk,),
                daemon=True,
            ).start()
    server_host = os.getenv("APP_HOST", "0.0.0.0").strip() or "0.0.0.0"
    try:
        server_port = int(os.getenv("PORT", os.getenv("APP_PORT", "5000")))
    except (TypeError, ValueError):
        server_port = 5000

    server_urls = [f"http://localhost:{server_port}"]
    if server_host in {"0.0.0.0", "::"}:
        lan_ip = ""
        try:
            lan_ip = socket.gethostbyname(socket.gethostname())
        except Exception:
            lan_ip = ""
        if not lan_ip or lan_ip.startswith("127."):
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                    sock.connect(("8.8.8.8", 80))
                    lan_ip = sock.getsockname()[0]
            except Exception:
                lan_ip = ""
        if lan_ip and lan_ip not in {"127.0.0.1", "0.0.0.0"}:
            server_urls.append(f"http://{lan_ip}:{server_port}")
    elif server_host not in {"127.0.0.1", "localhost"}:
        server_urls.append(f"http://{server_host}:{server_port}")

    print("Servidor iniciado em:", flush=True)
    for url in server_urls:
        print(f" - {url}", flush=True)
    if _access_password_enabled():
        print(
            "Acesso protegido:",
            f"usuario={ACCESS_CONFIG.get('username')}",
            f"senha={ACCESS_CONFIG.get('password')}",
            flush=True,
        )

    app.run(
        host=server_host,
        port=server_port,
        debug=False,
        use_reloader=False,
        threaded=True,
    )
