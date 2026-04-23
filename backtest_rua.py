import json
from collections import defaultdict

with open('historico_rua.json', encoding='utf-8') as f:
    data = json.load(f)

entries = [e for e in data if e.get('result') in ('MAIS','ATE') and e.get('value_needed') is not None and e.get('count') is not None]
entries.sort(key=lambda x: x.get('round_id', x.get('id', 0)))

total = len(entries)
mais = sum(1 for e in entries if e['result'] == 'MAIS')
ate = total - mais
print(f"=== RESUMO GERAL ===")
print(f"Total de rodadas: {total}")
print(f"MAIS: {mais} ({mais/total*100:.1f}%)  |  ATE: {ate} ({ate/total*100:.1f}%)")

# Desvio real
devs = [e['count'] - e['value_needed'] for e in entries]
devs_mais = [e['count'] - e['value_needed'] for e in entries if e['result'] == 'MAIS']
devs_ate = [e['count'] - e['value_needed'] for e in entries if e['result'] == 'ATE']
print(f"\nDesvio médio geral: {sum(devs)/len(devs):.2f}")
print(f"Desvio médio MAIS (count > value_needed): {sum(devs_mais)/len(devs_mais):.2f}")
print(f"Desvio médio ATE  (count < value_needed): {sum(devs_ate)/len(devs_ate):.2f}")

# Filtro ±22
in_range = [e for e in entries if abs(e['count'] - e['value_needed']) <= 22]
out_range = [e for e in entries if abs(e['count'] - e['value_needed']) > 22]
print(f"\nRodadas com |desvio| <= 22: {len(in_range)} ({len(in_range)/total*100:.1f}%)")
print(f"Rodadas com |desvio| > 22:  {len(out_range)} ({len(out_range)/total*100:.1f}%)")

# Sequências
print(f"\n=== SEQUÊNCIAS ===")
results = [e['result'] for e in entries]

# Continuação após 2 seguidos
cont_after_2 = {'total': 0, 'continued': 0}
for i in range(2, len(results)):
    if results[i-2] == results[i-1]:
        cont_after_2['total'] += 1
        if results[i] == results[i-1]:
            cont_after_2['continued'] += 1
if cont_after_2['total']:
    r = cont_after_2['continued']/cont_after_2['total']
    print(f"Após 2 iguais consecutivos -> continua igual: {cont_after_2['continued']}/{cont_after_2['total']} ({r*100:.1f}%)")

# Reversão após 3+ seguidos
rev_after_3 = {'total': 0, 'reversed': 0}
for i in range(3, len(results)):
    if results[i-3] == results[i-2] == results[i-1]:
        rev_after_3['total'] += 1
        if results[i] != results[i-1]:
            rev_after_3['reversed'] += 1
if rev_after_3['total']:
    r = rev_after_3['reversed']/rev_after_3['total']
    print(f"Após 3+ iguais consecutivos -> reverte: {rev_after_3['reversed']}/{rev_after_3['total']} ({r*100:.1f}%)")

# Após 4+
rev_after_4 = {'total': 0, 'reversed': 0}
for i in range(4, len(results)):
    if results[i-4] == results[i-3] == results[i-2] == results[i-1]:
        rev_after_4['total'] += 1
        if results[i] != results[i-1]:
            rev_after_4['reversed'] += 1
if rev_after_4['total']:
    r = rev_after_4['reversed']/rev_after_4['total']
    print(f"Após 4+ iguais consecutivos -> reverte: {rev_after_4['reversed']}/{rev_after_4['total']} ({r*100:.1f}%)")

# Faixas de value_needed
print(f"\n=== FAIXAS DE VALUE_NEEDED ===")
faixas = [
    ('<190', lambda v: v < 190),
    ('190-230', lambda v: 190 <= v < 230),
    ('230-270', lambda v: 230 <= v < 270),
    ('270-310', lambda v: 270 <= v < 310),
    ('310-350', lambda v: 310 <= v < 350),
    ('>350', lambda v: v >= 350),
]
for nome, cond in faixas:
    bucket = [e for e in entries if cond(e['value_needed'])]
    if bucket:
        m = sum(1 for e in bucket if e['result'] == 'MAIS')
        print(f"  {nome:12s}: {len(bucket):4d} rodadas  MAIS {m/len(bucket)*100:.1f}%  ATE {(len(bucket)-m)/len(bucket)*100:.1f}%  desvio médio: {sum(e['count']-e['value_needed'] for e in bucket)/len(bucket):.1f}")

# Sequência atual
print(f"\n=== SINAL ATUAL ===")
last = results[-1]
streak = 1
for i in range(len(results)-2, -1, -1):
    if results[i] == last:
        streak += 1
    else:
        break
print(f"Últimas rodadas: {results[-6:]}")
print(f"Sequência atual: {streak}x {last}")

# Sugestão com base nos dados reais
if streak >= 3:
    # Calcula taxa de reversão para esse tamanho de streak
    total_occ = 0
    rev_occ = 0
    for i in range(streak, len(results)):
        if all(results[i-streak+j] == last for j in range(streak)):
            total_occ += 1
            if results[i] != last:
                rev_occ += 1
    if total_occ:
        rev_rate = rev_occ / total_occ
        opposite = 'ATE' if last == 'MAIS' else 'MAIS'
        print(f"Após sequência de {streak}x {last}: {rev_occ}/{total_occ} reverteu ({rev_rate*100:.1f}%)")
        signal = opposite if rev_rate > 0.5 else last
        print(f"Sinal sugerido: {signal} (reversão {'favorável' if rev_rate > 0.5 else 'desfavorável'})")
    else:
        print("Dados insuficientes para streak atual")
elif streak == 2:
    total_occ = 0
    cont_occ = 0
    for i in range(2, len(results)):
        if results[i-2] == results[i-1] == last:
            total_occ += 1
            if results[i] == last:
                cont_occ += 1
    if total_occ:
        cont_rate = cont_occ / total_occ
        print(f"Após 2x {last}: continua {cont_occ}/{total_occ} ({cont_rate*100:.1f}%)")
        signal = last if cont_rate > 0.5 else ('ATE' if last == 'MAIS' else 'MAIS')
        print(f"Sinal sugerido: {signal}")
else:
    print(f"Apenas 1 rodada de streak — sem padrão definido")

# Desvio por faixa (extra)
print(f"\n=== TOP FAIXAS MAIS FAVORÁVEIS ===")
sorted_faixas = []
for nome, cond in faixas:
    bucket = [e for e in entries if cond(e['value_needed'])]
    if len(bucket) >= 20:
        m = sum(1 for e in bucket if e['result'] == 'MAIS')
        sorted_faixas.append((nome, m/len(bucket)*100, len(bucket)))
sorted_faixas.sort(key=lambda x: -x[1])
for nome, pct, cnt in sorted_faixas:
    print(f"  {nome:12s}: {pct:.1f}% MAIS  (n={cnt})")
