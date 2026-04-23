import json
from collections import defaultdict

f=open('historico_rua.json',encoding='utf-8'); d=json.load(f); f.close()
entries=[e for e in d if e.get('result') in ('MAIS','ATE') and e.get('count') is not None and e.get('value_needed') is not None and e.get('date') and e.get('time')]
entries.sort(key=lambda x: (x['date'], x['time']))

print("=== value_needed > max_count das N rodadas anteriores no dia -> ATE ===")
for N in [5, 10, 15, 20, 30]:
    wins=0; losses=0
    by_day = defaultdict(list)
    for e in entries:
        by_day[e['date']].append(e)
    for day, rounds in by_day.items():
        for i in range(N, len(rounds)):
            window = rounds[i-N:i]
            max_count = max(r['count'] for r in window)
            vn = rounds[i]['value_needed']
            result = rounds[i]['result']
            if vn > max_count:
                if result == 'ATE': wins+=1
                else: losses+=1
    total=wins+losses
    if total:
        print(f"  N={N:2d}: {wins}W/{losses}L = {wins/total*100:.1f}%  (n={total})")

print()
print("=== value_needed > max_count GLOBAL do dia ate agora -> ATE ===")
wins=0; losses=0
by_day2 = defaultdict(list)
for e in entries:
    by_day2[e['date']].append(e)
for day, rounds in by_day2.items():
    running_max = 0
    for r in rounds:
        vn = r['value_needed']
        result = r['result']
        if running_max > 0 and vn > running_max:
            if result == 'ATE': wins+=1
            else: losses+=1
        running_max = max(running_max, r['count'])
total=wins+losses
if total:
    print(f"  Max acumulado do dia: {wins}W/{losses}L = {wins/total*100:.1f}%  (n={total})")

print()
print("=== Margem: value_needed >= max_count * fator ===")
for fator in [0.90, 0.95, 1.00, 1.05]:
    wins=0; losses=0
    for day, rounds in by_day2.items():
        running_max = 0
        for r in rounds:
            vn = r['value_needed']
            result = r['result']
            if running_max > 0 and vn >= running_max * fator:
                if result == 'ATE': wins+=1
                else: losses+=1
            running_max = max(running_max, r['count'])
    total=wins+losses
    if total:
        print(f"  fator={fator:.2f}: {wins}W/{losses}L = {wins/total*100:.1f}%  (n={total})")
