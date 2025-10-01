import os
import smtplib
from email.message import EmailMessage
import psycopg2
from psycopg2.extras import RealDictCursor
import requests
from datetime import datetime, timezone, timedelta

# >>> NOVO: utilidades para limpar a URL do Postgres e mascarar senha nos logs
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode
import re
# <<< NOVO

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

# --------- helpers NOVOS (PG URL) ---------
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
    # >>> NOVO: limpa a URL e loga (mascarado)
    pg_url = _clean_pg_url(DB_URL)
    print("[db] usando POSTGRES_URL:", _mask_pg_url(pg_url))
    # <<< NOVO
    return psycopg2.connect(pg_url, cursor_factory=RealDictCursor, sslmode="require")

def get_open_draws_with_meta(conn):
    """
    Retorna sorteios 'open' com id e opened_at.
    """
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

# --- Total de vagas (tenta descobrir por config; fallback 100: 00..99)
def _get_total_slots_from_config(conn) -> int:
    with conn.cursor() as cur:
        # tenta app_config
        cur.execute("""
            SELECT key, value FROM app_config
             WHERE key IN (
                'total_numbers','ticket_count','max_number','range_max','ticket_total'
             )
        """)
        rows = cur.fetchall() or []
        kv = { (r["key"] or "").lower(): r["value"] for r in rows }
        # chaves em ordem de preferência
        for k in ("total_numbers","ticket_count","ticket_total","max_number","range_max"):
            v = kv.get(k)
            if v is None:
                continue
            try:
                n = int(v)
                if n > 0:
                    return n
            except:
                pass

        # tenta kv_store
        cur.execute("""
            SELECT key, value FROM kv_store
             WHERE key IN (
                'total_numbers','ticket_count','ticket_total','max_number','range_max'
             )
        """)
        rows = cur.fetchall() or []
        kv = { (r["key"] or "").lower(): r["value"] for r in rows }
        for k in ("total_numbers","ticket_count","ticket_total","max_number","range_max"):
            v = kv.get(k)
            if v is None:
                continue
            try:
                n = int(v)
                if n > 0:
                    return n
            except:
                pass

    return 100  # fallback padrão: 00..99

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
    Marca o sorteio como SORTEADO (encerra + realiza):
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
        opens = get_open_draws_with_meta(conn)
        print(f"[run] sorteios abertos: {[d['id'] for d in opens]}")
        if not opens:
            return 0

        total_slots = _get_total_slots_from_config(conn)
        print(f"[run] total_slots (capacidade): {total_slots}")

        last_number = get_last_lotomania_number()

        for d in opens:
            draw_id = d["id"]
            draw_label = get_draw_label(conn, draw_id)
            opened_at = d.get("opened_at")
            age_days = None
            if opened_at:
                # opened_at já vem com tz? assume naive->utc igual
                age_days = (datetime.now(timezone.utc) - opened_at.replace(tzinfo=timezone.utc)).days
            print(f"[draw {draw_id}] label='{draw_label}' age_days={age_days}")

            sold = get_sold_count(conn, draw_id)
            sold_out = sold >= total_slots
            print(f"[draw {draw_id}] vendidos={sold} / {total_slots} -> sold_out={sold_out}")

            # --- Regra pedida:
            # - Se NÃO vendeu todos os números: NÃO fecha, a menos que tenha >=7 dias aberto.
            # - Se vendeu todos os números: pode fechar imediatamente.
            can_close = sold_out or (age_days is not None and age_days >= 7)
            if not can_close:
                print(f"[draw {draw_id}] NÃO será fechado (ainda não vendeu tudo e não completou 7 dias).")
                continue

            # Determina vencedor pelo último número da Lotomania
            winner_user_id = paid_user_for_number(conn, draw_id, last_number)

            # Dados do vencedor (se houver)
            winner_name, winner_email = (None, None)
            if winner_user_id:
                winner_name, winner_email = get_user_email(conn, winner_user_id)

            # Marca sorteado (fecha + realized_at)
            set_draw_sorteado(conn, draw_id, last_number, winner_user_id)

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

            # Opcional: abrir um novo sorteio
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
