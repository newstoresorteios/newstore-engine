import os
import smtplib
from email.message import EmailMessage
import psycopg2
from psycopg2.extras import RealDictCursor
import requests
from datetime import datetime, timezone

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
        print("[email] SMTP config incompleta; pulando envio.")
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
        s.send_message(msg)
    print("[email] OK")

def send_winner_email(to_email: str, to_name: str, draw_id: int, winner_number: int):
    subj = f"🎉 {APP_NAME}: Você venceu o Sorteio #{draw_id}!"
    body = f"""Olá, {to_name or 'Participante'}!

Parabéns! Você é o vencedor do Sorteio #{draw_id}.
Número vencedor: {winner_number}

Nossa equipe entrará em contato com as próximas instruções.
Se você não reconhece esta mensagem, por favor, ignore.

Atenciosamente,
{APP_NAME}
"""
    _smtp_send(to_email, subj, body)

def send_draw_closed_admin(draw_id: int, winner_number: int, winner_name: str, winner_email: str):
    """E-mail para o admin informando fechamento do sorteio."""
    if not ADMIN_EMAIL:
        print("[email-admin] ADMIN_EMAIL vazio; pulando.")
        return
    subj = f"✅ {APP_NAME}: Sorteio #{draw_id} FECHADO"
    body = f"""Sorteio #{draw_id} foi fechado.

Número sorteado (vencedor): {winner_number}

Ganhador:
- Nome:  {winner_name or '-'}
- E-mail: {winner_email or '-'}

Data/Hora (UTC): {datetime.utcnow().isoformat()}Z
"""
    _smtp_send(ADMIN_EMAIL, subj, body)

# --------- DB helpers ---------
def db():
    # >>> NOVO: limpa a URL e loga (mascarado)
    pg_url = _clean_pg_url(DB_URL)
    print("[db] usando POSTGRES_URL:", _mask_pg_url(pg_url))
    # <<< NOVO
    return psycopg2.connect(pg_url, cursor_factory=RealDictCursor, sslmode="require")

def get_open_draws(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id FROM draws
            WHERE status = 'open'
            ORDER BY id ASC
        """)
        return [row["id"] for row in cur.fetchall()]

def set_winner_and_close(conn, draw_id: int, winner_number: int, winner_user_id: int):
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE draws
               SET status = 'closed',
                   winner_number = %s,
                   winner_user_id = %s,
                   closed_at = NOW()
             WHERE id = %s
               AND status = 'open'
        """, (winner_number, winner_user_id, draw_id))

def set_result_no_winner_and_close(conn, draw_id: int, winner_number: int):
    """Fecha o sorteio registrando o número sorteado, mesmo sem vencedor pago."""
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE draws
               SET status = 'closed',
                   winner_number = %s,
                   winner_user_id = NULL,
                   closed_at = NOW()
             WHERE id = %s
               AND status = 'open'
        """, (winner_number, draw_id))

def mark_draw_as_sorteado(conn, draw_id: int):
    """Marca o sorteio como 'sorteado' e registra realized_at (sem remover closed_at)."""
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE draws
               SET status = 'sorteado',
                   realized_at = NOW()
             WHERE id = %s
        """, (draw_id,))

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
            # Coluna única 'number' (int)
            query = """
                SELECT r.user_id
                  FROM reservations r
             LEFT JOIN payments p ON p.id = r.payment_id
                 WHERE r.draw_id = %s
                   AND r.number  = %s
                   AND (r.status = 'paid' OR p.status = 'approved')
                 LIMIT 1
            """
            params = (draw_id, number)
        elif "numbers" in cols:
            # Coluna 'numbers' (int[]) -> checar se o inteiro está contido no array
            query = """
                SELECT r.user_id
                  FROM reservations r
             LEFT JOIN payments p ON p.id = r.payment_id
                 WHERE r.draw_id = %s
                   AND ( %s = ANY(r.numbers) OR r.numbers @> ARRAY[%s]::int[] )
                   AND (r.status = 'paid' OR p.status = 'approved')
                 LIMIT 1
            """
            params = (draw_id, number, number)
        else:
            raise RuntimeError("Tabela reservations não possui colunas 'number' nem 'numbers'.")

        cur.execute(query, params)
        r = cur.fetchone()
        return r["user_id"] if r else None

# --------- Loto helper ---------
def get_last_lotomania_number():
    # tenta obter a ORDEM real do sorteio; se não houver, cai para a lista simples
    r = requests.get(LOT_ENDPOINT, timeout=20, headers={"Accept": "application/json"})
    r.raise_for_status()
    j = r.json()

    # chaves comuns que trazem a ordem de extração
    ordem_keys = [
        "dezenasOrdemSorteio",
        "listaDezenasOrdemSorteio",
        "dezenasSorteadasOrdem",
        "listaDezenasSorteadasOrdem",
        "dezenasSorteadasOrdemSorteio",
    ]

    seq = None
    for k in ordem_keys:
        if k in j and j[k]:
            seq = j[k]
            break

    if seq:
        # último EXTRAÍDO (na ordem do sorteio)
        ultimo = int(str(seq[-1]).lstrip("0") or "0")
        print(f"[lotomania] (ordem) último número sorteado: {ultimo}")
        return ultimo

    # fallback: sem ordem explícita; usa lista simples (pode não refletir a última bolinha)
    dezenas = j.get("listaDezenas") or j.get("dezenas") or []
    if not dezenas:
        raise RuntimeError("Sem dezenas no payload da Lotomania")
    print("[lotomania] Aviso: API sem ordem de sorteio; usando lista simples (fallback).")
    ultimo = int(str(dezenas[-1]).lstrip("0") or "0")
    print(f"[lotomania] (fallback) último número sorteado: {ultimo}")
    return ultimo

# --------- Main ---------
def run():
    print("[run] iniciando", datetime.now(timezone.utc).isoformat())
    conn = db()
    try:
        draws = get_open_draws(conn)
        print(f"[run] sorteios abertos: {draws}")
        if not draws:
            return 0

        last_number = get_last_lotomania_number()

        for draw_id in draws:
            user_id = paid_user_for_number(conn, draw_id, last_number)
            if not user_id:
                print(f"[draw {draw_id}] último número {last_number} NÃO está pago; mesmo assim fecha e registra como sorteado.")
                # fecha sem vencedor (mantendo número sorteado)
                set_result_no_winner_and_close(conn, draw_id, last_number)
                # aviso admin (sem vencedor)
                send_draw_closed_admin(draw_id, last_number, None, None)
            else:
                # fechar sorteio com vencedor
                print(f"[draw {draw_id}] vencedor -> user {user_id}, número {last_number}")
                set_winner_and_close(conn, draw_id, last_number, user_id)

                # e-mails
                name, email = get_user_email(conn, user_id)
                if email:
                    send_winner_email(email, name or "Participante", draw_id, last_number)
                else:
                    print(f"[email] usuário {user_id} sem e-mail; não foi possível notificar.")

                # aviso administrativo sempre que fechar
                send_draw_closed_admin(draw_id, last_number, name, email)

            # Em ambos os casos: marcar como 'sorteado' + realized_at e abrir novo sorteio
            mark_draw_as_sorteado(conn, draw_id)
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
        conn.rollback()
        return 1
    finally:
        conn.close()

if __name__ == "__main__":
    exit(run())