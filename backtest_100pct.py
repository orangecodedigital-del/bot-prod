"""
Busca exaustiva por padrões com 100% de acerto em historico_rua.json
Testa combinações de: resultado anterior, count, desvio, streak, odd
"""
import json, itertools
from pathlib import Path

data = json.loads(Path("historico_rua.json").read_text(encoding="utf-8"))
rounds = [r for r in data if r.get("result") in ("ATE", "MAIS")]
print(f"Total rodadas com resultado: {len(rounds)}")

def as_float(v):
    try: return float(v)
    except: return None

# Monta histórico estruturado
history = []
for r in rounds:
    res    = r.get("result")
    count  = as_float(r.get("count") or r.get("value_needed") or r.get("pcount"))
    entry  = as_float(r.get("entry") or r.get("odd") or r.get("book_odd"))
    dev    = as_float(r.get("deviation") or r.get("desvio"))
    vn     = as_float(r.get("value_needed"))
    history.append({"r": res, "count": count, "entry": entry, "dev": dev, "vn": vn})

print("Campos disponíveis (1 amostra):", rounds[0].keys())

results = []

# ── 1. Streak N de mesmo resultado
for target_result in ["ATE", "MAIS"]:
    for streak_len in range(2, 8):
        for predict in ["ATE", "MAIS"]:
            wins = losses = 0
            for i in range(streak_len, len(history)):
                prev = history[i-streak_len:i]
                if all(p["r"] == target_result for p in prev):
                    if history[i]["r"] == predict:
                        wins += 1
                    else:
                        losses += 1
            n = wins + losses
            if n >= 10 and losses == 0:
                results.append({
                    "pattern": f"streak_{streak_len}x_{target_result}→{predict}",
                    "wins": wins, "losses": losses, "n": n, "acc": 100.0
                })

# ── 2. Count > threshold → resultado
count_vals = [c["count"] for c in history if c["count"] is not None]
if count_vals:
    cmin, cmax = min(count_vals), max(count_vals)
    step = (cmax - cmin) / 30
    thresholds = [cmin + step * i for i in range(1, 30)]
    for thr in thresholds:
        for predict in ["ATE", "MAIS"]:
            wins = losses = 0
            for h in history:
                if h["count"] is not None and h["count"] > thr:
                    if h["r"] == predict: wins += 1
                    else: losses += 1
            n = wins + losses
            if n >= 10 and losses == 0:
                results.append({
                    "pattern": f"count>{thr:.1f}→{predict}",
                    "wins": wins, "losses": losses, "n": n, "acc": 100.0
                })

# ── 3. Count < threshold → resultado
        for predict in ["ATE", "MAIS"]:
            wins = losses = 0
            for h in history:
                if h["count"] is not None and h["count"] < thr:
                    if h["r"] == predict: wins += 1
                    else: losses += 1
            n = wins + losses
            if n >= 10 and losses == 0:
                results.append({
                    "pattern": f"count<{thr:.1f}→{predict}",
                    "wins": wins, "losses": losses, "n": n, "acc": 100.0
                })

# ── 4. Desvio < threshold → resultado
dev_vals = [h["dev"] for h in history if h["dev"] is not None]
if dev_vals:
    dmin, dmax = min(dev_vals), max(dev_vals)
    dstep = (dmax - dmin) / 30
    dthresholds = [dmin + dstep * i for i in range(1, 30)]
    for thr in dthresholds:
        for predict in ["ATE", "MAIS"]:
            wins = losses = 0
            for h in history:
                if h["dev"] is not None and h["dev"] < thr:
                    if h["r"] == predict: wins += 1
                    else: losses += 1
            n = wins + losses
            if n >= 10 and losses == 0:
                results.append({
                    "pattern": f"dev<{thr:.1f}→{predict}",
                    "wins": wins, "losses": losses, "n": n, "acc": 100.0
                })
        for predict in ["ATE", "MAIS"]:
            wins = losses = 0
            for h in history:
                if h["dev"] is not None and h["dev"] > thr:
                    if h["r"] == predict: wins += 1
                    else: losses += 1
            n = wins + losses
            if n >= 10 and losses == 0:
                results.append({
                    "pattern": f"dev>{thr:.1f}→{predict}",
                    "wins": wins, "losses": losses, "n": n, "acc": 100.0
                })

# ── 5. Streak + desvio combinados
for target_result in ["ATE", "MAIS"]:
    for streak_len in range(2, 6):
        for dev_thr in [-60, -50, -40, -30, -20]:
            for predict in ["ATE", "MAIS"]:
                wins = losses = 0
                for i in range(streak_len, len(history)):
                    prev = history[i-streak_len:i]
                    cur_dev = history[i]["dev"]
                    if (all(p["r"] == target_result for p in prev)
                            and cur_dev is not None and cur_dev < dev_thr):
                        if history[i]["r"] == predict: wins += 1
                        else: losses += 1
                n = wins + losses
                if n >= 10 and losses == 0:
                    results.append({
                        "pattern": f"streak_{streak_len}x_{target_result}+dev<{dev_thr}→{predict}",
                        "wins": wins, "losses": losses, "n": n, "acc": 100.0
                    })

# ── 6. Count extremes: value_needed > max(últimas N) ou < min(últimas N)
for n_window in [5, 10, 15, 20, 25, 30]:
    for predict in ["ATE", "MAIS"]:
        wins = losses = 0
        cnt_hist = []
        for h in history:
            vn = h["count"]
            if vn is not None and len(cnt_hist) >= n_window:
                recent_max = max(cnt_hist[-n_window:])
                if vn > recent_max:
                    if h["r"] == predict: wins += 1
                    else: losses += 1
            if vn is not None:
                cnt_hist.append(vn)
        n = wins + losses
        if n >= 10 and losses == 0:
            results.append({
                "pattern": f"count>max_last{n_window}→{predict}",
                "wins": wins, "losses": losses, "n": n, "acc": 100.0
            })
        # min
        wins = losses = 0
        cnt_hist = []
        for h in history:
            vn = h["count"]
            if vn is not None and len(cnt_hist) >= n_window:
                recent_min = min(cnt_hist[-n_window:])
                if vn < recent_min:
                    if h["r"] == predict: wins += 1
                    else: losses += 1
            if vn is not None:
                cnt_hist.append(vn)
        n = wins + losses
        if n >= 10 and losses == 0:
            results.append({
                "pattern": f"count<min_last{n_window}→{predict}",
                "wins": wins, "losses": losses, "n": n, "acc": 100.0
            })

# ── Resultado
print(f"\n{'='*60}")
print(f"Padrões com 100% de acerto (n >= 10): {len(results)}")
if results:
    results.sort(key=lambda x: -x["n"])
    for r in results:
        print(f"  {r['pattern']:55s}  n={r['n']:4d}  wins={r['wins']}")
else:
    print("  Nenhum padrão com 100% encontrado (n >= 10)")

# Mostra melhores (>= 95%) para referência
print(f"\n{'='*60}")
print("Melhores padrões (>= 95%, n >= 15) para referência:")
best = []
# re-run streaks for >= 95%
for target_result in ["ATE", "MAIS"]:
    for streak_len in range(2, 8):
        for predict in ["ATE", "MAIS"]:
            wins = losses = 0
            for i in range(streak_len, len(history)):
                prev = history[i-streak_len:i]
                if all(p["r"] == target_result for p in prev):
                    if history[i]["r"] == predict: wins += 1
                    else: losses += 1
            n = wins + losses
            if n >= 15:
                acc = 100 * wins / n
                if acc >= 95:
                    best.append({"pattern": f"streak_{streak_len}x_{target_result}→{predict}", "n": n, "acc": acc})

for target_result in ["ATE", "MAIS"]:
    for streak_len in range(2, 6):
        for dev_thr in [-60, -50, -40, -30, -20]:
            for predict in ["ATE", "MAIS"]:
                wins = losses = 0
                for i in range(streak_len, len(history)):
                    prev = history[i-streak_len:i]
                    cur_dev = history[i]["dev"]
                    if (all(p["r"] == target_result for p in prev)
                            and cur_dev is not None and cur_dev < dev_thr):
                        if history[i]["r"] == predict: wins += 1
                        else: losses += 1
                n = wins + losses
                if n >= 15:
                    acc = 100 * wins / n
                    if acc >= 95:
                        best.append({"pattern": f"streak_{streak_len}x_{target_result}+dev<{dev_thr}→{predict}", "n": n, "acc": acc})

for n_window in [5, 10, 15, 20, 25, 30]:
    for predict in ["ATE", "MAIS"]:
        wins = losses = 0
        cnt_hist = []
        for h in history:
            vn = h["count"]
            if vn is not None and len(cnt_hist) >= n_window:
                recent_max = max(cnt_hist[-n_window:])
                if vn > recent_max:
                    if h["r"] == predict: wins += 1
                    else: losses += 1
            if vn is not None:
                cnt_hist.append(vn)
        n = wins + losses
        if n >= 15:
            acc = 100 * wins / n
            if acc >= 95:
                best.append({"pattern": f"count>max_last{n_window}→{predict}", "n": n, "acc": acc})
        wins = losses = 0
        cnt_hist = []
        for h in history:
            vn = h["count"]
            if vn is not None and len(cnt_hist) >= n_window:
                recent_min = min(cnt_hist[-n_window:])
                if vn < recent_min:
                    if h["r"] == predict: wins += 1
                    else: losses += 1
            if vn is not None:
                cnt_hist.append(vn)
        n = wins + losses
        if n >= 15:
            acc = 100 * wins / n
            if acc >= 95:
                best.append({"pattern": f"count<min_last{n_window}→{predict}", "n": n, "acc": acc})

best.sort(key=lambda x: (-x["acc"], -x["n"]))
for b in best:
    print(f"  {b['pattern']:55s}  n={b['n']:4d}  acc={b['acc']:.1f}%")
