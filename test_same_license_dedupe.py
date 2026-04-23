import importlib.util
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))


def load_app_module():
    app_path = ROOT / "app.py"
    spec = importlib.util.spec_from_file_location("appmod_same_license_dedupe", app_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def pick_active_license(mod):
    store = mod._load_license_store()
    for entry in (store.get("licenses") or []):
        if mod._license_status_for(entry) == "active" and not entry.get("revoked"):
            return entry
    raise RuntimeError("Preciso de pelo menos uma licenca ativa para o teste de dedupe.")


def main():
    mod = load_app_module()
    entry = pick_active_license(mod)
    key = str(entry.get("key") or "").strip().upper()

    with mod._license_thread_context(key):
        raw = mod._load_automation_raw()
        cfg_map = mod._load_automation_config(raw)
        meta = mod._load_automation_meta(raw)
        meta["running"] = True

        cfg = dict(cfg_map.get("rodovia") or mod._default_automation_cfg())
        cfg["enabled"] = True
        cfg["api_key"] = "dedupe-key"
        cfg["private_key"] = "dedupe-private"
        cfg["stake_mode"] = "fixed"
        cfg["stake_value"] = 5
        cfg_map["rodovia"] = cfg
        mod._save_automation_config(cfg_map, meta)

        calls = []
        orig_calc_stake = mod._automation_calc_stake
        orig_set_status = mod._set_automation_status
        orig_send_tg = mod._send_automation_order_telegram
        orig_submit = mod._automation_submit_order
        try:
            mod._automation_calc_stake = lambda cfg: (5.0, None)
            mod._set_automation_status = lambda *args, **kwargs: None
            mod._send_automation_order_telegram = lambda *args, **kwargs: None

            def fake_submit(*args, **kwargs):
                calls.append((args, kwargs))
                return True

            mod._automation_submit_order = fake_submit
            mod._runtime_bucket(mod._auto_order_mid).clear()

            mod._automation_execute_signal("rodovia", "market-dedupe-1", "MAIS", "AO VIVO", 91, license_key=key)
            mod._automation_execute_signal("rodovia", "market-dedupe-1", "MAIS", "AO VIVO", 91, license_key=key)
        finally:
            mod._automation_calc_stake = orig_calc_stake
            mod._set_automation_status = orig_set_status
            mod._send_automation_order_telegram = orig_send_tg
            mod._automation_submit_order = orig_submit

    total = len(calls)
    print(f"submit_calls={total}")
    if total != 1:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
