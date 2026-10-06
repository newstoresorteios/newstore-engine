
# Lotomania Daily Checker (Python)

Agendável (cron) para apurar sorteios **fechados e ainda não realizados**, conforme a **última dezena na ordem de extração** da Lotomania.
Funciona com Supabase Postgres (usa `POSTGRES_URL`).

## Como funciona
1. Lê todos os registros `draws.status = 'closed' AND realized_at IS NULL` dos tipos principal, adicional e secundario (tipo nulo é principal legado).
2. Consulta o concurso mais recente e caminha pelo histórico até provar o primeiro concurso elegível após o fechamento. A comparação usa Brasília e 21h como horário de referência da apuração.
3. Exige 20 dezenas distintas em `dezenasSorteadasOrdemSorteio`, no intervalo 00–99. Usa a última; não usa a lista ordenada numericamente nem tenta a penúltima.
4. Procura o comprador pela reserva vinculada ao número `sold` do mesmo sorteio. Se a reserva não resolver, não houver reserva vinculada ou o número estiver em estado legado (`available`, `reserved` etc.), usa a participação paga de compatibilidade e, por fim, os pagamentos `approved`/`paid`/`pago` do MESMO sorteio que contenham o número. Zero usuários distintos: sem comprador. Um usuário distinto: é o comprador. Mais de um usuário distinto: erro `ambiguous_paid_owner`, o sorteio não é gravado e o processo retorna código 1. Várias linhas do mesmo usuário não são ambiguidade.
5. Antes de apurar, exige a grade exata do sorteio: 100 números distintos em `public.numbers`, de 0 a 99. Qualquer outra grade (500, 1000, incompleta ou inconsistente) é recusada com o log `unsupported_result_grid`: nada é gravado, nenhuma comunicação é enviada, o sorteio conta como falha e os demais continuam. Não existe regra automática para outras grades.
   Em seguida bloqueia e revalida o sorteio antes de gravar `status`, número, usuário, nome e `realized_at`. Não abre um novo sorteio.
6. `COMMIT=false` executa a atualização em transação e faz rollback. Não envia comunicações de resultado. Desabilite também `PUSH_AUTOMATION_SCAN_ENABLED` em verificações sem envio: o scanner é independente do `COMMIT`.
7. `COMMIT=true` confirma o resultado antes dos e-mails e evento de push. Falha de comunicação não desfaz o resultado.
8. Timeout/conexão e HTTP transitório recebem até 3 tentativas, com pausas de 1s e 2s. Payload inválido, concurso incorreto e HTTP 404 não recebem fallback de fonte/concurso.
9. Se qualquer sorteio falhar, os demais continuam sendo processados, mas o processo retorna código 1. Aguardar concurso ainda não elegível não é erro.

Se a dezena não tiver comprador resolvido, o comportamento existente grava a dezena com usuário/nome nulos. Isso exige conferência de dados; não significa que outro número possa ser escolhido. Registros já marcados `sorteado` ficam fora da seleção, mesmo incompletos. Não reabra ou reconstrua resultados históricos sem validar fechamento, concurso e propriedade dos números.

## Variáveis de ambiente
- `POSTGRES_URL` (obrigatório)
- `COMMIT` (`true`/`false`; padrão `false`)
- `CHECK_LAST_K` é legado e não é lido pelo fluxo atual.
- `LOTOMANIA_ENDPOINT` (padrão `https://servicebus2.caixa.gov.br/portaldeloterias/api/lotomania`)

### Push Automation
O engine nao envia Push diretamente. Ele emite eventos para o backend, que decide o envio real.

Exemplo de configuracao para producao:

```env
PUSH_AUTOMATION_EVENTS_ENABLED=true
PUSH_AUTOMATION_SCAN_ENABLED=true
PUSH_AUTOMATION_EVENT_KEYS=NEW_DRAW_PUBLISHED,DRAW_REMAINING_NUMBERS_75,DRAW_REMAINING_NUMBERS_50,DRAW_REMAINING_NUMBERS_20,DRAW_REMAINING_NUMBERS_10,WINNER_DEFINED,BALANCE_EXPIRING_30_DAYS,BALANCE_EXPIRING_15_DAYS,BALANCE_EXPIRING_10_DAYS,BALANCE_EXPIRING_7_DAYS,BALANCE_EXPIRED
PUSH_AUTOMATION_NO_BACKFILL=true
PUSH_AUTOMATION_MAX_EVENTS_PER_SCAN=5
PUSH_AUTOMATION_MAX_EVENTS_PER_KEY_PER_SCAN=2
PUSH_AUTOMATION_REQUIRE_OCCURRED_AT=true
PUSH_AUTOMATION_DEFAULT_LOOKBACK_HOURS=24
PUSH_AUTOMATION_ALLOW_LARGE_BATCH=false
PUSH_AUTOMATION_PREVIEW_ONLY=false
PUSH_AUTOMATION_WINNER_LOOKBACK_HOURS=24
PUSH_AUTOMATION_WINNER_MAX_EVENTS_PER_SCAN=2
PUSH_AUTOMATION_REMAINING_LOOKBACK_HOURS=24
PUSH_AUTOMATION_REMAINING_MAX_EVENTS_PER_SCAN=1
PUSH_AUTOMATION_BALANCE_LOOKBACK_HOURS=24
PUSH_AUTOMATION_BALANCE_MAX_EVENTS_PER_SCAN=5
TRAY_COUPON_VALID_DAYS=180
BACKEND_INTERNAL_API_BASE=https://newstore-backend.onrender.com
PUSH_INTERNAL_EVENTS_TOKEN=
```

Use em `PUSH_INTERNAL_EVENTS_TOKEN` o mesmo valor configurado no backend. Nao coloque token real em arquivos versionados.
Use `PUSH_AUTOMATION_PREVIEW_ONLY=true` para validar candidatos sem chamar o backend. Por padrao, o scanner bloqueia backfill historico, exige `occurred_at`, aplica janela de 24h e limita lotes grandes.

Balance automation usa `users.coupon_value_cents` como saldo, `users.coupon_updated_at` como base temporal e calcula o vencimento com `TRAY_COUPON_VALID_DAYS`.

### Email Automation

O scanner de e-mail roda separadamente do scanner de push. A engine detecta
publicacao, thresholds de numeros restantes, fechamento de sorteios e os
estagios 30/20/10/7/3 dias ou vencido do saldo. O saldo vem exclusivamente da
view `public.user_coupon_balance_expiry`; a engine nao recalcula a validade.
Deduplicacao e SMTP permanecem no backend.

```env
EMAIL_AUTOMATION_SCAN_ENABLED=true
EMAIL_AUTOMATION_DEFAULT_LOOKBACK_HOURS=24
EMAIL_AUTOMATION_PUBLISHED_LOOKBACK_HOURS=24
EMAIL_AUTOMATION_CLOSED_LOOKBACK_HOURS=72
EMAIL_BALANCE_AUTOMATION_EFFECTIVE_FROM=2026-08-06T00:00:00Z
EMAIL_BALANCE_EXPIRED_BACKFILL_ENABLED=false
EMAIL_AUTOMATION_DRY_RUN=false
EMAIL_AUTOMATION_BACKEND_CONNECT_TIMEOUT_SECONDS=10
EMAIL_AUTOMATION_BACKEND_READ_TIMEOUT_SECONDS=45
```

O workflow dedicado `.github/workflows/email-automation-scan.yml` reutiliza
`POSTGRES_URL`, `BACKEND_INTERNAL_API_BASE` e `PUSH_INTERNAL_EVENTS_TOKEN`, roda
a cada 10 minutos e possui limite total de 10 minutos. Antes de habilitar uma
publicacao real, execute `EMAIL_AUTOMATION_DRY_RUN=true python
run_email_automation_scan.py`. Sem `EMAIL_BALANCE_AUTOMATION_EFFECTIVE_FROM`,
eventos ja vencidos sao bloqueados por padrao; backfill exige ativacao explicita.

## Local
```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
# configure/exporte as variáveis do processo (main.py não carrega .env automaticamente)
export COMMIT=false
export PUSH_AUTOMATION_SCAN_ENABLED=false
python main.py
```

Testes locais, sem banco e sem envio:
```bash
python -m unittest discover -v
```

## Agendamento efetivo
O workflow `.github/workflows/lotomania-result.yml` contém o cron D+1 às 10h de Brasília (terça, quinta e sábado). A existência desse YAML não prova execução: confira também o estado ativo/desabilitado em GitHub Actions. Os scanners de e-mail/push não executam a apuração da Caixa. O README abaixo descreve uma alternativa Render; não prova que exista um Cron Job provisionado. Evite dois agendadores concorrentes.

## Render (Cron Job)
- Build: `pip install -r requirements.txt`
- Start: `python main.py`
- Schedule: `0 2 * * *`
- Set as variáveis de ambiente no painel do Render.
