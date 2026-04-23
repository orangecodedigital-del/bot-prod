import json
from collections import defaultdict

with open('historico_rua.json', encoding='utf-8') as f:
    data = json.load(f)

entries = [e for e in data if e.get('result') in ('MAIS','ATE') and e.get('value_needed') is not None and e.get('count') is not None]
entries.sort(key=lambda x: x.get('round_id', x.get('id', 0)))

results = [e['result'] for e in entries]
values  = [e['value_needed'] for e in entries]
counts  = [e['count'] for e in entries]
devs    = [counts[i] - values[i] for i in range(len(entries))]

MIN_OCCURRENCES = 5  # mínimo para considerar um padrão

# ─── 1. Sequências puras (N iguais → próximo) ──────────────────────────────
print("=== PADRÕES DE SEQUÊNCIA PURA ===")
for streak_len in range(2, 9):
    for label in ('MAIS', 'ATE'):
        next_mais = 0; next_ate = 0
        for i in range(streak_len, len(results)):
            if all(results[i-streak_len+j] == label for j in range(streak_len)):
                if results[i] == 'MAIS': next_mais += 1
                else: next_ate += 1
        total = next_mais + next_ate
        if total >= MIN_OCCURRENCES:
            pct_mais = next_mais/total*100
            pct_ate  = next_ate/total*100
            flag = " <<<" if max(pct_mais, pct_ate) >= 70 else ""
            print(f"  {streak_len}x {label} → MAIS {pct_mais:.1f}%  ATE {pct_ate:.1f}%  (n={total}){flag}")

# ─── 2. Padrões alternados (MAIS ATE MAIS ATE → next?) ─────────────────────
print("\n=== PADRÕES ALTERNADOS ===")
alternados_4_mais = 0; alternados_4_ate = 0
alternados_3_mais = 0; alternados_3_ate = 0
for i in range(4, len(results)):
    if results[i-4]=='MAIS' and results[i-3]=='ATE' and results[i-2]=='MAIS' and results[i-1]=='ATE':
        if results[i]=='MAIS': alternados_4_mais += 1
        else: alternados_4_ate += 1
    if results[i-3]=='ATE' and results[i-2]=='MAIS' and results[i-1]=='ATE':
        if results[i]=='MAIS': alternados_3_mais += 1
        else: alternados_3_ate += 1

t = alternados_4_mais + alternados_4_ate
if t >= MIN_OCCURRENCES:
    print(f"  MAIS ATE MAIS ATE → MAIS {alternados_4_mais/t*100:.1f}%  ATE {alternados_4_ate/t*100:.1f}%  (n={t})")
t = alternados_3_mais + alternados_3_ate
if t >= MIN_OCCURRENCES:
    print(f"  ATE MAIS ATE → MAIS {alternados_3_mais/t*100:.1f}%  ATE {alternados_3_ate/t*100:.1f}%  (n={t})")

# ─── 3. Sequência + faixa de valor ─────────────────────────────────────────
print("\n=== SEQUÊNCIA + FAIXA VALUE_NEEDED ===")
faixas = [('<190', lambda v: v<190), ('190-230', lambda v: 190<=v<230),
          ('230-270', lambda v: 230<=v<270), ('270-310', lambda v: 270<=v<310),
          ('310-350', lambda v: 310<=v<350), ('>350', lambda v: v>=350)]

for streak_len in range(2, 6):
    for label in ('MAIS', 'ATE'):
        for fname, fcond in faixas:
            nm = 0; na = 0
            for i in range(streak_len, len(results)):
                if all(results[i-streak_len+j] == label for j in range(streak_len)) and fcond(values[i]):
                    if results[i]=='MAIS': nm += 1
                    else: na += 1
            total = nm + na
            if total >= MIN_OCCURRENCES:
                pct = max(nm, na)/total*100
                if pct >= 70:
                    winner = 'MAIS' if nm >= na else 'ATE'
                    print(f"  {streak_len}x {label} + valor {fname} → {winner} {pct:.1f}%  (n={total})")

# ─── 4. Desvio da rodada anterior como preditor ─────────────────────────────
print("\n=== DESVIO ANTERIOR COMO PREDITOR ===")
# Se a rodada anterior teve desvio muito negativo (ATE grande), o que vem depois?
buckets = {
    'dev<-40': lambda d: d < -40,
    '-40<dev<-20': lambda d: -40 <= d < -20,
    '-20<dev<0': lambda d: -20 <= d < 0,
    '0<dev<20': lambda d: 0 <= d < 20,
    'dev>20': lambda d: d >= 20,
}
for bname, bcond in buckets.items():
    nm = 0; na = 0
    for i in range(1, len(entries)):
        if bcond(devs[i-1]):
            if results[i]=='MAIS': nm += 1
            else: na += 1
    total = nm + na
    if total >= MIN_OCCURRENCES:
        pct = max(nm, na)/total*100
        winner = 'MAIS' if nm >= na else 'ATE'
        flag = " <<<" if pct >= 65 else ""
        print(f"  Desvio anterior {bname} → {winner} {pct:.1f}%  (n={total}){flag}")

# ─── 5. Padrões N-gram de 3 resultados ──────────────────────────────────────
print("\n=== PADRÕES N-GRAM (3 rodadas) COM ALTA ACURÁCIA ===")
ngrams_3 = defaultdict(lambda: {'MAIS':0,'ATE':0})
for i in range(3, len(results)):
    key = (results[i-3], results[i-2], results[i-1])
    ngrams_3[key][results[i]] += 1

for key, counts_dict in sorted(ngrams_3.items(), key=lambda x: -max(x[1].values())/(sum(x[1].values()))):
    total = sum(counts_dict.values())
    if total < MIN_OCCURRENCES: continue
    best = max(counts_dict, key=counts_dict.get)
    pct = counts_dict[best]/total*100
    if pct >= 70:
        print(f"  {' '.join(key)} → {best} {pct:.1f}%  (n={total})")

# ─── 6. Padrões N-gram de 4 resultados ──────────────────────────────────────
print("\n=== PADRÕES N-GRAM (4 rodadas) COM ALTA ACURÁCIA ===")
ngrams_4 = defaultdict(lambda: {'MAIS':0,'ATE':0})
for i in range(4, len(results)):
    key = (results[i-4], results[i-3], results[i-2], results[i-1])
    ngrams_4[key][results[i]] += 1

for key, counts_dict in sorted(ngrams_4.items(), key=lambda x: -max(x[1].values())/(sum(x[1].values()))):
    total = sum(counts_dict.values())
    if total < MIN_OCCURRENCES: continue
    best = max(counts_dict, key=counts_dict.get)
    pct = counts_dict[best]/total*100
    if pct >= 75:
        print(f"  {' '.join(key)} → {best} {pct:.1f}%  (n={total})")

# ─── 7. Sequência + desvio anterior ─────────────────────────────────────────
print("\n=== SEQUÊNCIA + DESVIO ANTERIOR ===")
for streak_len in range(2, 5):
    for label in ('MAIS', 'ATE'):
        for bname, bcond in buckets.items():
            nm = 0; na = 0
            for i in range(streak_len, len(results)):
                if all(results[i-streak_len+j] == label for j in range(streak_len)) and bcond(devs[i-1]):
                    if results[i]=='MAIS': nm += 1
                    else: na += 1
            total = nm + na
            if total >= MIN_OCCURRENCES and max(nm,na)/total >= 0.75:
                winner = 'MAIS' if nm >= na else 'ATE'
                print(f"  {streak_len}x {label} + desvio anterior {bname} → {winner} {max(nm,na)/total*100:.1f}%  (n={total})")

# ─── 8. PADRÃO 100% ─────────────────────────────────────────────────────────
print("\n=== PADRÕES EXATOS 100% (qualquer tamanho, n>=3) ===")
found_any = False
for n in range(2, 8):
    ngrams = defaultdict(lambda: {'MAIS':0,'ATE':0})
    for i in range(n, len(results)):
        key = tuple(results[i-n:i])
        ngrams[key][results[i]] += 1
    for key, cd in ngrams.items():
        total = sum(cd.values())
        if total < 3: continue
        best = max(cd, key=cd.get)
        pct = cd[best]/total*100
        if pct == 100.0:
            print(f"  n={n}: {' '.join(key)} → {best} 100%  (n={total})")
            found_any = True

if not found_any:
    print("  Nenhum padrão com 100% encontrado (n>=3)")
    # Mostra os mais próximos de 100%
    print("\n=== TOP 10 PADRÕES MAIS PRÓXIMOS DE 100% (n>=5) ===")
    all_patterns = []
    for n in range(2, 8):
        ngrams = defaultdict(lambda: {'MAIS':0,'ATE':0})
        for i in range(n, len(results)):
            key = tuple(results[i-n:i])
            ngrams[key][results[i]] += 1
        for key, cd in ngrams.items():
            total = sum(cd.values())
            if total < 5: continue
            best = max(cd, key=cd.get)
            pct = cd[best]/total*100
            all_patterns.append((pct, total, n, key, best))
    all_patterns.sort(reverse=True)
    for pct, total, n, key, best in all_patterns[:10]:
        print(f"  n={n}: {' '.join(key)} → {best} {pct:.1f}%  (n={total})")
