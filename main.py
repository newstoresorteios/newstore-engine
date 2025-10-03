import os
import smtplib
from email.message import EmailMessage
import psycopg2
from psycopg2.extras import RealDictCursor
import requests
from datetime import datetime, timezone, timedelta

# >>> utilidades para limpar a URL do Postgres e mascarar senha nos logs
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode
import re
# <<<

# --------- ENV ---------
DB_URL = os.getenv("POSTGRES_URL", "")
COMMIT = os.getenv("COMMIT", "false").lower() in ("1", "true", "yes")
LOT_ENDPOINT = os.getenv("LOTOMANIA_ENDPOINT", "https://servicebus2.caixa.gov.br/portaldeloterias/api/lotomania")

# SMTP (Brevo)
SMTP_HOST = os.getenv("SMTP_HOST", "smtp-relay.brevo.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")              # ex.: 9712be001@smtp-brevo.com
SMTP_PASS = os.getenv("SMTP_PASS", "")              # chave SMTP (senha-mestre)
SMTP_FROM = "contato@newstorerj.com.br"             # FIXO conforme solicitado
SMTP_NAME = os.getenv("SMTP_NAME", "NewStore Sorteios")
APP_NAME  = os.getenv("APP_NAME",  "NewStore Sorteios")

# Aviso administrativo quando fechar sorteio
ADMIN_EMAIL = os.getenv("ADMIN_EMAIL", "newrecreio@gmail.com")

# --------- helpers (PG URL) ---------
def _mask_pg_url(u: str) -> str:
    """Mascara a senha ao imprimir a URL no log."""
    return re.sub(r'://([^:]+):[^@]+@', r'://\1:***@', u or "")

def _clean_pg_url(u: str) -> str:
    """
    Remove parâmetros não suportados pelo libpq/psycopg2 (ex.: 'supa').
    Mantém apenas chaves comuns/seguras.
    """
    if not u:
        return u
    parts = urlsplit(u)
    q = dict(parse_qsl(parts.query or "", keep_blank_values=True))
    allowed = {
        "sslmode", "ssl", "sslrootcert", "connect_timeout",
        "target_session_attrs", "application_name", "options"
    }
    q = {k: v for k, v in q.items() if k in allowed}
    new_query = urlencode(q)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, new_query, parts.fragment))

# --------- E-MAIL ---------
def _smtp_send(to_email: str, subject: str, body: str):
    if not (SMTP_USER and SMTP_PASS and to_email):
        print("[email] SMTP config incompleta ou destinatário vazio; pulando envio.")
        return
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = f"{SMTP_NAME} <{SMTP_FROM}>"
    msg["To"] = to_email
    msg.set_content(body)

    print(f"[email] -> {to_email} | subj='{subject}'")
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=20) as s:
        s.starttls()
        s.login(SMTP_USER, SMTP_PASS)
        if COMMIT:
            s.send_message(msg)
            print("[email] OK (enviado)")
        else:
            print("[email] DRY-RUN (não enviado)")

def send_winner_email(to_email: str, to_name: str, draw_label: str, draw_id: int, winner_number: int):
    subj = f"🎉 {APP_NAME}: Você venceu {draw_label}!"
    body = f"""Olá, {to_name or 'Participante'}!

Parabéns! Você é o vencedor de {draw_label} (#{draw_id}).
Número vencedor: {winner_number:02d}

Nossa equipe entrará em contato com as próximas instruções.
Se você não reconhece esta mensagem, por favor, ignore.

Atenciosamente,
{APP_NAME}
"""
    _smtp_send(to_email, subj, body)

def send_draw_closed_admin(draw_label: str, draw_id: int, winner_number: int, winner_name: str, winner_email: str):
    """E-mail para o admin informando fechamento do sorteio."""
    if not ADMIN_EMAIL:
        print("[email-admin] ADMIN_EMAIL vazio; pulando.")
        return
    subj = f"✅ {APP_NAME}: {draw_label} (#{draw_id}) SORTEADO"
    body = f"""{draw_label} (#{draw_id}) foi realizado e marcado como SORTEADO.

Número sorteado (vencedor): {winner_number:02d}

Ganhador:
- Nome:  {winner_name or '-'}
- E-mail: {winner_email or '-'}

Data/Hora (UTC): {datetime.utcnow().isoformat()}Z
"""
    _smtp_send(ADMIN_EMAIL, subj, body)

def send_loser_email(to_email: str, to_name: str, draw_label: str, draw_id: int, winner_number: int, winner_name: str):
    subj = f"{APP_NAME}: Resultado de {draw_label} (#{draw_id})"
    vencedor_txt = (winner_name or "participante") + f" com o número {winner_number:02d}"
    body = f"""Olá, {to_name or 'Participante'}!

O sorteio {draw_label} (#{draw_id}) foi realizado.
Vencedor: {vencedor_txt}.

Infelizmente você não foi contemplado, mais sorte da próxima vez!

Acompanhe nossos próximos sorteios!
{APP_NAME}
"""
    _smtp_send(to_email, subj, body)

# --------- DB helpers ---------
def db():
    # limpa a URL e loga (mascarado)
    pg_url = _clean_pg_url(DB_URL)
    print("[db] usando POSTGRES_URL:", _mask_pg_url(pg_url))
    return psycopg2.connect(pg_url, cursor_factory=RealDictCursor, sslmode="require")

def get_pending_draws(conn):
    """
    Retorna:
      - todos os 'open' (podem ou não fechar agora);
      - e os 'closed' com realized_at IS NULL (precisam ser finalizados e comunicados).
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, status, opened_at
              FROM draws
             WHERE status IN ('open','closed')
               AND (status = 'open' OR realized_at IS NULL)
             ORDER BY id ASC
        """)
        return cur.fetchall() or []

def get_open_draws_with_meta(conn):
    """(mantido para compat, não usado)"""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, opened_at
              FROM draws
             WHERE status = 'open'
             ORDER BY id ASC
        """)
        return cur.fetchall() or []

def get_draw_label(conn, draw_id: int) -> str:
    """
    Tenta obter um rótulo amigável do sorteio (title/name/label/product_name).
    Se não existir, usa 'Sorteio #<id>'.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name
              FROM information_schema.columns
             WHERE table_schema='public' AND table_name='draws'
        """)
        cols = {row["column_name"] for row in cur.fetchall()}

        candidates = ["title", "name", "label", "product_name"]
        for c in candidates:
            if c in cols:
                cur.execute(f"SELECT {c} FROM draws WHERE id = %s", (draw_id,))
                row = cur.fetchone()
                if row:
                    val = (row.get(c) or "").strip()
                    if val:
                        return val
    return f"Sorteio #{draw_id}"

# --- Total de vagas (lê app_config key/value e kv_store k/v; fallback 100)
def _get_total_slots_from_config(conn) -> int:
    def _load_app_config(cur):
        cur.execute("SELECT key, value FROM app_config")
        rows = cur.fetchall() or []
        return { (str(r["key"] or "").strip().lower()): r["value"] for r in rows }

    def _load_kv_store(cur):
        # kv_store tem colunas k/v (conforme seu schema)
        cur.execute("SELECT k, v FROM kv_store")
        rows = cur.fetchall() or []
        return { (str(r["k"] or "").strip().lower()): r["v"] for r in rows }

    try:
        with conn.cursor() as cur:
            kv = {}
            try:
                kv.update(_load_app_config(cur))
            except Exception as e:
                print("[config] app_config não lida:", repr(e))
            try:
                kv.update(_load_kv_store(cur))
            except Exception as e:
                print("[config] kv_store não lida:", repr(e))

        for k in ("total_numbers","ticket_count","ticket_total","max_number","range_max"):
            v = kv.get(k)
            if v is None:
                continue
            try:
                n = int(str(v))
                if n > 0:
                    print(f"[config] {k}={n}")
                    return n
            except Exception:
                pass

        print("[config] nenhuma chave numérica válida encontrada; usando fallback 100")
        return 100
    except Exception as e:
        print("[config] erro ao ler configs:", repr(e), "-> usando fallback 100")
        return 100

def get_sold_count(conn, draw_id: int) -> int:
    """
    Conta quantos números estão efetivamente 'vendidos' (reservations paid OU payment approved/paid).
    Suporta schema com 'number' (int) ou 'numbers' (int[]).
    """
    with conn.cursor() as cur:
        # Descobre as colunas disponíveis em reservations
        cur.execute("""
            SELECT column_name, data_type
              FROM information_schema.columns
             WHERE table_schema = 'public'
               AND table_name   = 'reservations'
        """)
        cols = {row["column_name"]: row["data_type"] for row in cur.fetchall()}

        if "number" in cols:
            query = """
                SELECT COUNT(DISTINCT r.number) AS sold
                  FROM reservations r
             LEFT JOIN payments p ON p.id = r.payment_id
                 WHERE r.draw_id = %s
                   AND (r.status = 'paid' OR p.status IN ('approved','paid'))
            """
            params = (draw_id,)
        elif "numbers" in cols:
            # unnests numbers[] para contar distintos
            query = """
                WITH flat AS (
                    SELECT UNNEST(r.numbers) AS num
                      FROM reservations r
                 LEFT JOIN payments p ON p.id = r.payment_id
                     WHERE r.draw_id = %s
                       AND (r.status = 'paid' OR p.status IN ('approved','paid'))
                )
                SELECT COUNT(DISTINCT num) AS sold FROM flat
            """
            params = (draw_id,)
        else:
            raise RuntimeError("Tabela reservations não possui colunas 'number' nem 'numbers'.")

        cur.execute(query, params)
        row = cur.fetchone()
        return int(row["sold"] or 0)

def set_draw_sorteado(conn, draw_id: int, winner_number: int, winner_user_id):
    """
    Marca o sorteio como SORTEADO (encerra + realiza) apenas quando estava 'open':
    - status='sorteado'
    - winner_number, winner_user_id (pode ser NULL)
    - closed_at=NOW(), realized_at=NOW()
    """
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE draws
               SET status = 'sorteado',
                   winner_number = %s,
                   winner_user_id = %s,
                   closed_at = NOW(),
                   realized_at = NOW()
             WHERE id = %s
               AND status = 'open'
        """, (winner_number, winner_user_id, draw_id))

def set_draw_sorteado_any_status(conn, draw_id: int, winner_number: int, winner_user_id, winner_name):
    """
    Finaliza independente do status atual (open/closed):
      - status='sorteado'
      - winner_number / winner_user_id / winner_name
      - closed_at = COALESCE(closed_at, NOW())
      - realized_at = NOW()
    """
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE draws
               SET status = 'sorteado',
                   winner_number = %s,
                   winner_user_id = %s,
                   winner_name   = %s,
                   closed_at = COALESCE(closed_at, NOW()),
                   realized_at = NOW()
             WHERE id = %s
        """, (winner_number, winner_user_id, winner_name, draw_id))

def open_new_draw(conn):
    """Abre um novo sorteio 'open'. Ajuste os campos se sua tabela exigir mais colunas."""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO draws (status, opened_at)
            VALUES ('open', NOW())
            RETURNING id
        """)
        new_id = cur.fetchone()["id"]
        print(f"[draw] novo sorteio aberto: #{new_id}")
        return new_id

def get_user_email(conn, user_id: int):
    with conn.cursor() as cur:
        cur.execute("SELECT name, email FROM users WHERE id = %s", (user_id,))
        row = cur.fetchone()
        if not row: return None, None
        return row["name"], row["email"]

def paid_user_for_number(conn, draw_id: int, number: int):
    with conn.cursor() as cur:
        # Descobre as colunas disponíveis
        cur.execute("""
            SELECT column_name, data_type
              FROM information_schema.columns
             WHERE table_schema = 'public'
               AND table_name   = 'reservations'
        """)
        cols = {row["column_name"]: row["data_type"] for row in cur.fetchall()}

        if "number" in cols:
            query = """
                SELECT r.user_id
                  FROM reservations r
             LEFT JOIN payments p ON p.id = r.payment_id
                 WHERE r.draw_id = %s
                   AND r.number  = %s
                   AND (r.status = 'paid' OR p.status IN ('approved','paid'))
                 LIMIT 1
            """
            params = (draw_id, number)
        elif "numbers" in cols:
            query = """
                SELECT r.user_id
                  FROM reservations r
             LEFT JOIN payments p ON p.id = r.payment_id
                 WHERE r.draw_id = %s
                   AND ( %s = ANY(r.numbers) OR r.numbers @> ARRAY[%s]::int[] )
                   AND (r.status = 'paid' OR p.status IN ('approved','paid'))
                 LIMIT 1
            """
            params = (draw_id, number, number)
        else:
            raise RuntimeError("Tabela reservations não possui colunas 'number' nem 'numbers'.")

        cur.execute(query, params)
        r = cur.fetchone()
        return r["user_id"] if r else None

def get_participants(conn, draw_id: int):
    """
    Participantes com participação válida (reservations paid OU payments approved/paid).
    Retorna lista de dicts {id, name, email}.
    """
    with conn.cursor() as cur:
        cur.execute("""
            WITH elig AS (
                SELECT DISTINCT u.id, u.name, u.email
                  FROM users u
                  JOIN reservations r ON r.user_id = u.id
             LEFT JOIN payments p ON p.id = r.payment_id
                 WHERE r.draw_id = %s
                   AND (r.status = 'paid' OR p.status IN ('approved','paid'))
            )
            SELECT id, name, email
              FROM elig
             WHERE COALESCE(NULLIF(email,''), '') <> ''
        """, (draw_id,))
        return cur.fetchall() or []

# --------- Loto helper ---------
def get_last_lotomania_number():
    # espera JSON com lista de dezenas; usamos APENAS o ÚLTIMO número sorteado
    r = requests.get(LOT_ENDPOINT, timeout=20, headers={"Accept": "application/json"})
    r.raise_for_status()
    j = r.json()
    dezenas = j.get("listaDezenas") or j.get("dezenas") or []
    if not dezenas:
        raise RuntimeError("Sem dezenas no payload da Lotomania")
    ultimo = int(str(dezenas[-1]).lstrip("0") or "0")  # "07" -> 7
    print(f"[lotomania] Último número sorteado: {ultimo}")
    return ultimo

# --------- Main ---------
def run():
    print("[run] iniciando", datetime.now(timezone.utc).isoformat())
    conn = db()
    try:
        draws = get_pending_draws(conn)
        print(f"[run] sorteios pendentes: {[{'id': d['id'], 'status': d['status']} for d in draws]}")
        if not draws:
            return 0

        total_slots = _get_total_slots_from_config(conn)
        print(f"[run] total_slots (capacidade): {total_slots}")

        last_number = get_last_lotomania_number()

        for d in draws:
            draw_id = d["id"]
            status = d["status"]
            draw_label = get_draw_label(conn, draw_id)
            opened_at = d.get("opened_at")
            age_days = None
            if opened_at:
                # opened_at já vem com tz? assume naive->utc igual
                age_days = (datetime.now(timezone.utc) - opened_at.replace(tzinfo=timezone.utc)).days
            print(f"[draw {draw_id}] status={status} label='{draw_label}' age_days={age_days}")

            finalize_now = False
            open_will_be_finalized = False

            if status == "closed":
                # Já fechado, mas sem realized_at: finalize e comunique agora
                finalize_now = True
            else:
                sold = get_sold_count(conn, draw_id)
                sold_out = sold >= total_slots
                print(f"[draw {draw_id}] vendidos={sold} / {total_slots} -> sold_out={sold_out}")

                # Regra:
                # - NÃO fecha se não vendeu tudo e < 7 dias
                # - Fecha se sold_out OU >= 7 dias
                finalize_now = sold_out or (age_days is not None and age_days >= 7)
                open_will_be_finalized = finalize_now

            if not finalize_now:
                print(f"[draw {draw_id}] NÃO será finalizado agora.")
                continue

            # Determina vencedor pelo último número da Lotomania
            winner_user_id = paid_user_for_number(conn, draw_id, last_number)

            # Dados do vencedor (se houver)
            winner_name, winner_email = (None, None)
            if winner_user_id:
                winner_name, winner_email = get_user_email(conn, winner_user_id)

            # Marca sorteado (funciona para open/closed) — agora persiste também winner_name
            set_draw_sorteado_any_status(conn, draw_id, last_number, winner_user_id, winner_name)

            # E-mail para vencedor (se houver)
            if winner_user_id and winner_email:
                send_winner_email(winner_email, winner_name or "Participante", draw_label, draw_id, last_number)
            elif winner_user_id and not winner_email:
                print(f"[email] usuário {winner_user_id} sem e-mail; não foi possível notificar vencedor.")

            # Admin sempre recebe (com nome/numero do vencedor quando houver)
            send_draw_closed_admin(draw_label, draw_id, last_number, winner_name, winner_email)

            # Participantes não contemplados (com info do vencedor)
            parts = get_participants(conn, draw_id)
            loser_list = [p for p in parts if p["id"] != (winner_user_id or -1)]
            print(f"[draw {draw_id}] enviando e-mail de 'não contemplado' para {len(loser_list)} participantes")
            for p in loser_list:
                if p.get("email"):
                    send_loser_email(
                        p["email"],
                        p.get("name") or "Participante",
                        draw_label,
                        draw_id,
                        last_number,
                        winner_name or "-"
                    )

            # Abre novo sorteio apenas quando finalizamos um 'open' agora
            if open_will_be_finalized:
                open_new_draw(conn)

        if COMMIT:
            conn.commit()
            print("[run] COMMIT aplicado.")
        else:
            conn.rollback()
            print("[run] DRY-RUN (rollback).")

        return 0
    except Exception as e:
        print("[run] erro:", repr(e))
        try:
            conn.rollback()
        except:
            pass
        return 1
    finally:
        conn.close()

if __name__ == "__main__":
    exit(run())
