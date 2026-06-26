
# Lotomania Daily Checker (Python)

Agendável (cron) para fechar um **sorteio aberto** na sua base conforme o **último** número sorteado da Lotomania (com fallback para o **penúltimo**).
Funciona com Supabase Postgres (usa `POSTGRES_URL`).

## Como funciona
1. Lê o **sorteio aberto** em `draws` (`status = 'open'`). Se houver mais de um, processa todos.
2. Busca o resultado **mais recente** da Lotomania em JSON.
3. Extrai os **2 últimos números na ordem do sorteio**.
4. Calcula o **conjunto de números pagos** para cada draw, unindo:
   - `reservations.status = 'paid'`
   - `reservations JOIN payments ON payments.id = reservations.payment_id AND payments.status = 'approved'`
5. Se o **último número** estiver pago, ele é o vencedor; senão tenta o **penúltimo**. (Controle por `CHECK_LAST_K`, padrão 2.)
6. Em **modo dry‑run** (`COMMIT=false`), apenas loga o resultado. Em **modo commit** (`COMMIT=true`), atualiza `draws`.

## Variáveis de ambiente
- `POSTGRES_URL` (obrigatório)
- `COMMIT` (`true`/`false`; padrão `false`)
- `CHECK_LAST_K` (padrão `2`)
- `LOTOMANIA_ENDPOINT` (padrão `https://servicebus2.caixa.gov.br/portaldeloterias/api/lotomania`)

### Push Automation
O engine nao envia Push diretamente. Ele emite eventos para o backend, que decide o envio real.

Exemplo de configuracao para producao:

```env
PUSH_AUTOMATION_EVENTS_ENABLED=true
PUSH_AUTOMATION_SCAN_ENABLED=true
PUSH_AUTOMATION_EVENT_KEYS=NEW_DRAW_PUBLISHED,DRAW_REMAINING_NUMBERS_20,DRAW_REMAINING_NUMBERS_10,WINNER_DEFINED,BALANCE_EXPIRING_30_DAYS,BALANCE_EXPIRING_10_DAYS,BALANCE_EXPIRING_7_DAYS,BALANCE_EXPIRED
BACKEND_INTERNAL_API_BASE=https://newstore-backend.onrender.com
PUSH_INTERNAL_EVENTS_TOKEN=
```

Use em `PUSH_INTERNAL_EVENTS_TOKEN` o mesmo valor configurado no backend. Nao coloque token real em arquivos versionados.

## Local
```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
# copie .env.example para .env e edite
python main.py
```

## Render (Cron Job)
- Build: `pip install -r requirements.txt`
- Start: `python main.py`
- Schedule: `0 2 * * *`
- Set as variáveis de ambiente no painel do Render.
