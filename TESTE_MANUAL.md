# Checklist de Teste Manual

## 1. Login salvo no navegador
- Abrir `/login`.
- Marcar `Lembrar neste navegador`, preencher usuario, senha e key, entrar.
- Fechar e abrir a tela de login.
- Esperado: usuario e key aparecem preenchidos; senha nao deve ficar salva.

## 2. Layout das abas
- Abrir `Rodovia` e navegar por `Ao Vivo`, `Analytics`, `Automatizado`, `Telegram`, `Historico`.
- Repetir em `Rua`.
- Esperado: nenhuma aba vazia por quebra de layout; cards e botoes aparecem completos.

## 3. Salvar sem iniciar
- Na aba `Automatizado`, preencher API key, private key, stake e mercados.
- Clicar `Salvar configuracao`.
- Recarregar a pagina.
- Esperado: placeholders mascarados continuam mostrando credenciais salvas; bot permanece parado.

## 4. Clique duplo em Teste minimo
- Com credenciais validas e saldo, clicar `Teste minimo (1.01)` duas vezes seguidas.
- Esperado: apenas uma tentativa real; a outra deve ser bloqueada por duplicidade/cooldown.

## 5. Iniciar e parar bot
- Clicar `Iniciar bot`.
- Confirmar status `INICIADO`.
- Clicar `Parar bot`.
- Esperado: status muda sem precisar salvar novamente.

## 6. Fluxo SSE vivo
- Abrir uma aba ao vivo e deixar parada por 30s.
- Desligar/religar a conexao do servidor ou bloquear a stream temporariamente.
- Esperado: a UI mostra que o fluxo esta desatualizado/reconectando.

## 7. Mudanca de market
- Deixar uma rodada acabar e esperar a troca para novo `market #`.
- Esperado: market id, localidade, contagem e meta mudam juntos; nao ficam dados da rodada anterior misturados.

## 8. Barra MAIS/ATE
- Observar a barra de probabilidade durante a rodada.
- Comparar com logs/sinais do backend.
- Esperado: quando houver leitura forte do backend, a UI acompanha o mesmo lado/confiança.

## 9. Telegram parcial
- Salvar apenas token ou apenas chat ID.
- Tentar iniciar Telegram.
- Esperado: warning claro; nao deve dizer que esta pronto.

## 10. Arbitragem
- Com ambiente seguro de teste, simular oportunidade de arbitragem.
- Esperado: se a segunda perna falhar, status deve registrar erro e tentativa de flatten.

## 11. Duas sessoes com a mesma API
- Abrir duas sessoes com licencas diferentes usando a mesma API.
- Tentar iniciar automacao nas duas.
- Esperado: a segunda deve ser bloqueada.

## 12. Resultado e saldo
- Aguardar fechamento da rodada apos ordem/teste.
- Esperado: saldo atualiza, log resolve status e Telegram/resultado ficam coerentes.

## Sinais de alerta
- Botao conclui sem mudar status.
- Aba fica vazia ou corta cards.
- `pending` preso por muito tempo.
- Saldo nao muda apos ordem.
- UI indica `MAIS`, mas log/backend operam `ATE`.
