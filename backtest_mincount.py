import json
from collections import defaultdict

f=open('historico_rua.json',encoding='utf-8'); d=json.load(f); f.close()
entries=[e for e in d if e.get('result') in ('MAIS','ATE') and e.get('count') is not None and e.get('value_needed') is not None and e.get('date') and e.get('time')]
entries.sort(key=lambda x: (x['date'], x['time']))

by_day = defaultdict(list)
for e in entries:
    by_day[e['date']].append(e)

print('=== value_needed < min_count das N rodadas anteriores -> MAIS ===')
for N in [5,10,15,20,30]:
    wins=0; losses=0
    for day, rounds in by_day.items():
        for i in range(N, len(rounds)):
            window = rounds[i-N:i]
            min_count = min(r['count'] for r in window)
            vn = rounds[i]['value_needed']
            result = rounds[i]['result']
            if vn < min_count:
                if result == 'MAIS': wins+=1
                else: losses+=1
    total=wins+losses
    if total:
        print(f'  N={N:2d}: {wins}W/{losses}L = {wins/total*100:.1f}%  (n={total})')

print()
print('=== value_needed <= min_count * fator (abaixo do min por margem) -> MAIS ===')
for fator in [0.95, 0.90, 0.85]:
    wins=0; losses=0
    for day, rounds in by_day.items():
        running_min = None
        for r in rounds:
            vn = r['value_needed']
            result = r['result']
            if running_min is not None and vn <= running_min * fator:
                if result == 'MAIS': wins+=1
                else: losses+=1
            running_min = min(running_min, r['count']) if running_min is not None else r['count']
    total=wins+losses
    if total:
        print(f'  fator={fator:.2f}: {wins}W/{losses}L = {wins/total*100:.1f}%  (n={total})')
