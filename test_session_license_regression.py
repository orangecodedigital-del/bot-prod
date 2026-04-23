import copy
import importlib.util
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))


def load_app_module():
    app_path = ROOT / "app.py"
    spec = importlib.util.spec_from_file_location("appmod_regression", app_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def pick_two_active_licenses(mod):
    store = mod._load_license_store()
    active = [
        entry for entry in (store.get("licenses") or [])
        if mod._license_status_for(entry) == "active" and not entry.get("revoked")
    ]
    if len(active) < 2:
        raise RuntimeError("Preciso de pelo menos duas licencas ativas para o teste agressivo.")
    for entry in active:
        if not (entry.get("username") and (entry.get("password") or entry.get("password_hash"))):
            raise RuntimeError(f"Licenca sem credenciais completas para teste: {entry.get('key')}")
    return active[0], active[1]


class RegressionSuite:
    PRIVATE_FILES = [
        "telegram_config.json",
        "automatizado_config.json",
        "placar_telegram.json",
        "saldo_timeline.json",
        "signal_analytics.json",
        "ultimo_sinal_telegram.json",
        "automation_orders_log.json",
    ]

    def __init__(self):
        self.mod = load_app_module()
        self.results = []
        self.failures = []
        self.orig_store = copy.deepcopy(self.mod._load_license_store())
        self.orig_runtime_key = str(getattr(self.mod, "_active_runtime_license_key", "") or "")
        self.orig_license_state = copy.deepcopy(self.mod.license_state)
        self.orig_global_cfg = copy.deepcopy(self.mod.automation_cfg)
        self.orig_global_meta = copy.deepcopy(self.mod.automation_meta)
        self.orig_global_tg = copy.deepcopy(self.mod.telegram_cfg)
        self.orig_global_score = copy.deepcopy(self.mod.telegram_score)
        self.orig_balance_timeline = copy.deepcopy(self.mod.balance_timeline_data)
        self.orig_send_now = self.mod._telegram_send_now
        self.orig_api_request = self.mod._automation_api_request
        self.orig_submit_order = self.mod._automation_submit_order
        self.license_a, self.license_b = pick_two_active_licenses(self.mod)
        self.key_a = str(self.license_a.get("key") or "").strip().upper()
        self.key_b = str(self.license_b.get("key") or "").strip().upper()
        self.slug_a = "".join(ch if ch.isalnum() else "_" for ch in self.key_a).strip("_") or "default"
        self.slug_b = "".join(ch if ch.isalnum() else "_" for ch in self.key_b).strip("_") or "default"
        self.snapshot_a = self._snapshot_private_state(self.key_a)
        self.snapshot_b = self._snapshot_private_state(self.key_b)

    def _snapshot_private_state(self, license_key):
        snapshot = {}
        previous = str(getattr(self.mod._thread_license_context, "license_key", "") or "")
        try:
            self.mod._thread_license_context.license_key = license_key
            for filename in self.PRIVATE_FILES:
                snapshot[filename] = copy.deepcopy(self.mod._load_private_json(filename, None))
        finally:
            if previous:
                self.mod._thread_license_context.license_key = previous
            elif hasattr(self.mod._thread_license_context, "license_key"):
                delattr(self.mod._thread_license_context, "license_key")
        return snapshot

    def _restore_private_state(self, license_key, snapshot):
        previous = str(getattr(self.mod._thread_license_context, "license_key", "") or "")
        try:
            self.mod._thread_license_context.license_key = license_key
            for filename, payload in snapshot.items():
                if payload is None:
                    continue
                self.mod._save_private_json(filename, copy.deepcopy(payload))
        finally:
            if previous:
                self.mod._thread_license_context.license_key = previous
            elif hasattr(self.mod._thread_license_context, "license_key"):
                delattr(self.mod._thread_license_context, "license_key")

    def check(self, name, condition, detail=""):
        ok = bool(condition)
        self.results.append((name, ok, detail))
        print(f"{'OK' if ok else 'FAIL'} | {name} | {detail}")
        if not ok:
            self.failures.append(name)

    def login_api(self, client, entry):
        response = client.post(
            "/api/login",
            json={
                "license_key": entry["key"],
                "username": entry["username"],
                "password": entry["password"],
            },
        )
        self.check(f"login {entry['key']} success", response.status_code == 200 and response.json["success"], response.get_data(as_text=True))
        return response

    def run(self):
        mod = self.mod
        mod._telegram_send_now = lambda *args, **kwargs: (True, None)

        def fake_api_request(cfg, path, **kwargs):
            if path == "/balance":
                marker = str(cfg.get("api_key") or "")
                amount = 321.11 if marker.endswith("A") else 654.22
                return {"success": True, "status": 200, "data": {"balance": amount, "currency": "BRLX"}}
            return {"success": True, "status": 200, "data": {}}

        mod._automation_api_request = fake_api_request
        order_calls = []
        mod._automation_submit_order = lambda *args, **kwargs: order_calls.append((args, kwargs)) or True

        client_a = mod.app.test_client()
        client_b = mod.app.test_client()

        self.login_api(client_a, self.license_a)
        self.login_api(client_b, self.license_b)

        resp_a = client_a.get("/api/license")
        resp_b = client_b.get("/api/license")
        self.check("client A sees own masked license", resp_a.json["data"]["license_key"].startswith(self.key_a[:4]), resp_a.json["data"]["license_key"])
        self.check("client B sees own masked license", resp_b.json["data"]["license_key"].startswith(self.key_b[:4]), resp_b.json["data"]["license_key"])

        automation_payload_a = {
            "enabled": True,
            "api_key": "session-key-A",
            "private_key": "session-private-A",
            "stake_mode": "fixed",
            "stake_value": 23,
            "currency": "BRLX",
            "safe_min_confidence": 84,
            "active_profile": "b",
            "execution_profile_mode": "both",
        }
        resp = client_a.post("/api/automation/rodovia", json=automation_payload_a)
        self.check("client A save automation success", resp.status_code == 200 and resp.json["success"], resp.get_data(as_text=True))
        self.check("client A automation payload profile", resp.json["data"]["active_profile"] == "b", resp.json["data"]["active_profile"])

        previous = str(getattr(mod._thread_license_context, "license_key", "") or "")
        try:
            mod._thread_license_context.license_key = self.key_a
            raw_a = mod._load_automation_raw()
        finally:
            if previous:
                mod._thread_license_context.license_key = previous
            elif hasattr(mod._thread_license_context, "license_key"):
                delattr(mod._thread_license_context, "license_key")
        self.check("license A disk stake persisted", raw_a["rodovia"]["stake_value"] == 23, raw_a["rodovia"]["stake_value"])
        self.check("license A disk profile persisted", raw_a["_meta"]["active_profile"] == "b", raw_a["_meta"]["active_profile"])

        resp = client_b.get("/api/automation/rodovia")
        self.check("client B isolated from A stake", resp.json["data"]["stake_value"] != 23, resp.json["data"]["stake_value"])

        mod.automation_cfg["rodovia"]["stake_value"] = 1
        mod.automation_meta["active_profile"] = "a"
        resp = client_a.post("/api/automation/rodovia/refresh-balance")
        self.check("client A refresh balance success", resp.status_code == 200 and resp.json["success"], resp.get_data(as_text=True))
        self.check("runtime restored A stake after refresh", mod.automation_cfg["rodovia"]["stake_value"] == 23, mod.automation_cfg["rodovia"]["stake_value"])
        self.check("runtime restored A profile after refresh", mod.automation_meta["active_profile"] == "b", mod.automation_meta["active_profile"])

        mod._thread_license_context.license_key = self.key_a
        cfg_a = mod._load_automation_config(mod._load_automation_raw())
        meta_a = mod._load_automation_meta(mod._load_automation_raw())
        meta_a["running"] = False
        cfg_a["rodovia"]["enabled"] = True
        cfg_a["rodovia"]["api_key"] = "owner-key-A"
        cfg_a["rodovia"]["private_key"] = "owner-private-A"
        mod._save_automation_config(cfg_a, meta_a)
        start_a = client_a.post("/api/automation/global", json={"running": True})
        self.check("client A starts runtime successfully", start_a.status_code == 200 and start_a.json["success"], start_a.get_data(as_text=True))
        active_key, active_running = mod._active_runtime_status()
        self.check("runtime owner is license A after start", active_key == self.key_a and active_running is True, f"key={active_key} running={active_running}")

        resp = client_a.post("/api/automation/global", json={"enabled_markets": {"rodovia": False, "rua": True}})
        self.check("client A disable market success", resp.status_code == 200 and resp.json["success"], resp.get_data(as_text=True))
        self.check("runtime disabled A market", mod.automation_cfg["rodovia"]["enabled"] is False, mod.automation_cfg["rodovia"]["enabled"])

        order_calls.clear()
        mod.automation_meta["running"] = True
        mod._automation_execute_signal("rodovia", "777", "MAIS", "AO VIVO", 91)
        self.check("disabled market blocks execution submit", len(order_calls) == 0, f"calls={len(order_calls)}")

        telegram_payload_b = {
            "enabled": True,
            "bot_token": "bot-token-B",
            "chat_id": "chat-id-B",
            "signal_min_confidence": 63,
        }
        resp = client_b.post("/api/telegram/rodovia", json=telegram_payload_b)
        self.check("client B save telegram success", resp.status_code == 200 and resp.json["success"], resp.get_data(as_text=True))
        previous = str(getattr(mod._thread_license_context, "license_key", "") or "")
        try:
            mod._thread_license_context.license_key = self.key_b
            tg_b = mod._load_telegram_config()
        finally:
            if previous:
                mod._thread_license_context.license_key = previous
            elif hasattr(mod._thread_license_context, "license_key"):
                delattr(mod._thread_license_context, "license_key")
        self.check("client B telegram persisted on own license", tg_b["chat_id"] == "chat-id-B", tg_b["chat_id"])
        self.check("client B telegram save does not steal runtime", mod._active_runtime_status()[0] == self.key_a, mod._active_runtime_status()[0])

        resp = client_a.get("/api/telegram/rodovia")
        self.check("client A telegram isolated from B chat id", resp.json["data"]["chat_id_value"] != "chat-id-B", resp.json["data"]["chat_id_value"])

        score_reset_b = client_b.post("/api/telegram/score/reset")
        self.check("client B score reset success", score_reset_b.status_code == 200 and score_reset_b.json["success"], score_reset_b.get_data(as_text=True))
        previous = str(getattr(mod._thread_license_context, "license_key", "") or "")
        try:
            mod._thread_license_context.license_key = self.key_b
            score_b = mod._load_private_json("placar_telegram.json", {})
            mod._thread_license_context.license_key = self.key_a
            score_a = mod._load_private_json("placar_telegram.json", {})
        finally:
            if previous:
                mod._thread_license_context.license_key = previous
            elif hasattr(mod._thread_license_context, "license_key"):
                delattr(mod._thread_license_context, "license_key")
        self.check("client B score reset stays on own license", (score_b.get("rodovia") or {}).get("wins", 0) == 0, score_b)
        self.check("client A score untouched by B reset", (score_a.get("rodovia") or {}).get("wins", 0) == 1, score_a)

        for client, label in ((client_a, "A"), (client_b, "B")):
            cache_resp = client.get("/api/automation/rodovia")
            self.check(f"client {label} api cache-control no-store", "no-store" in str(cache_resp.headers.get("Cache-Control") or ""), cache_resp.headers.get("Cache-Control"))

        today = mod._today()
        previous = str(getattr(mod._thread_license_context, "license_key", "") or "")
        try:
            mod._thread_license_context.license_key = self.key_a
            mod._save_private_json("placar_telegram.json", {"rodovia": {"date": today, "wins": 1, "losses": 0}, "rua": {"date": today, "wins": 0, "losses": 0}})
            mod._save_private_json("signal_analytics.json", [{"date": today, "market": "rodovia", "blocked": False, "outcome": "win"}])
            mod._thread_license_context.license_key = self.key_b
            mod._save_private_json("placar_telegram.json", {"rodovia": {"date": today, "wins": 7, "losses": 3}, "rua": {"date": today, "wins": 0, "losses": 0}})
            mod._save_private_json("signal_analytics.json", [{"date": today, "market": "rodovia", "blocked": False, "outcome": "loss"}])
        finally:
            if previous:
                mod._thread_license_context.license_key = previous
            elif hasattr(mod._thread_license_context, "license_key"):
                delattr(mod._thread_license_context, "license_key")

        overview_a = client_a.get("/api/overview").json
        overview_b = client_b.get("/api/overview").json
        self.check("overview remains session-specific", overview_a["totals"]["wins"] != overview_b["totals"]["wins"], f"A={overview_a['totals']} B={overview_b['totals']}")

        analytics_a = client_a.get("/api/analytics").json
        analytics_b = client_b.get("/api/analytics").json
        recent_a = analytics_a["rodovia"]["recent"][0]["outcome"] if analytics_a["rodovia"]["recent"] else ""
        recent_b = analytics_b["rodovia"]["recent"][0]["outcome"] if analytics_b["rodovia"]["recent"] else ""
        self.check("analytics remains session-specific", recent_a != recent_b, f"A={recent_a} B={recent_b}")

        resp = client_a.post("/api/license", json={"key": self.key_a})
        self.check("same active license does not clear session", resp.status_code == 200 and resp.json["success"], resp.get_data(as_text=True))
        with client_a.session_transaction() as sess:
            self.check("session A retained after same key post", str(sess.get("license_key") or "").upper() == self.key_a, sess.get("license_key"))

        mod._thread_license_context.license_key = self.key_b
        cfg_b = mod._load_automation_config(mod._load_automation_raw())
        meta_b = mod._load_automation_meta(mod._load_automation_raw())
        meta_b["running"] = False
        cfg_b["rodovia"]["enabled"] = True
        cfg_b["rodovia"]["api_key"] = "owner-key-B"
        cfg_b["rodovia"]["private_key"] = "owner-private-B"
        mod._save_automation_config(cfg_b, meta_b)
        start_b = client_b.post("/api/automation/global", json={"running": True})
        self.check("client B cannot steal active runtime from A", start_b.status_code == 409, start_b.get_data(as_text=True))
        active_key, active_running = mod._active_runtime_status()
        self.check("runtime owner remains license A after B conflict", active_key == self.key_a and active_running is True, f"key={active_key} running={active_running}")

        mod._thread_license_context.license_key = self.key_a
        cfg_a = mod._load_automation_config(mod._load_automation_raw())
        meta_a = mod._load_automation_meta(mod._load_automation_raw())
        meta_a["running"] = True
        mod._save_automation_config(cfg_a, meta_a)
        mod._ensure_runtime_context_for_key(self.key_a, force_reload=True)
        mod._ensure_runtime_context_for_key(self.key_b, force_reload=True)
        mod._thread_license_context.license_key = self.key_a
        raw_a = mod._load_automation_raw()
        self.check("switching active runtime pauses previous license", raw_a["_meta"]["running"] is False, raw_a["_meta"]["running"])

        mirror_a = ROOT / "client_data" / self.slug_a / "telegram_config.json"
        mirror_b = ROOT / "client_data" / self.slug_b / "automatizado_config.json"
        self.check("mirror json exists for license A", mirror_a.exists(), str(mirror_a))
        self.check("mirror json exists for license B", mirror_b.exists(), str(mirror_b))

        logout_resp = client_a.post("/logout", follow_redirects=False)
        self.check("logout returns redirect", logout_resp.status_code in {302, 303}, logout_resp.status_code)
        after_logout = client_a.get("/api/license")
        self.check("logged-out session blocked from api", after_logout.status_code == 401, after_logout.status_code)

    def cleanup(self):
        mod = self.mod
        mod._telegram_send_now = self.orig_send_now
        mod._automation_api_request = self.orig_api_request
        mod._automation_submit_order = self.orig_submit_order
        mod.license_store = copy.deepcopy(self.orig_store)
        mod._save_license_store_unsafe()
        self._restore_private_state(self.key_a, self.snapshot_a)
        self._restore_private_state(self.key_b, self.snapshot_b)
        mod.automation_cfg = self.orig_global_cfg
        mod.automation_meta = self.orig_global_meta
        mod.telegram_cfg = self.orig_global_tg
        mod.telegram_score = self.orig_global_score
        mod.balance_timeline_data = self.orig_balance_timeline
        mod.license_state.clear()
        mod.license_state.update(self.orig_license_state)
        if self.orig_runtime_key:
            mod._ensure_runtime_context_for_key(self.orig_runtime_key, force_reload=True)
        else:
            mod._active_runtime_license_key = ""


def main():
    suite = RegressionSuite()
    try:
        suite.run()
    finally:
        suite.cleanup()
    print("\nSUMMARY")
    print("total", len(suite.results))
    print("fails", len(suite.failures))
    if suite.failures:
        print("failed_names", suite.failures)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
