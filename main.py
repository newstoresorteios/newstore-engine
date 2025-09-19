import os
import smtplib
from email.message import EmailMessage
import psycopg2
from psycopg2.extras import RealDictCursor
import requests
from datetime import datetime, timezone

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
    return psycopg2.connect(DB_URL, cursor_factory=RealDictCursor, sslmode="require")

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

def get_user_email(conn, user_id: int):
    with conn.cursor() as cur:
        cur.execute("SELECT name, email FROM users WHERE id = %s", (user_id,))
        row = cur.fetchone()
        if not row: return None, None
        return row["name"], row["email"]

def paid_user_for_number(conn, draw_id: int, number: int):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT r.user_id
              FROM reservations r
         LEFT JOIN payments p ON p.id = r.payment_id
             WHERE r.draw_id = %s
               AND r.number  = %s
               AND (r.status = 'paid' OR p.status = 'approved')
             LIMIT 1
        """, (draw_id, number))
        r = cur.fetchone()
        return r["user_id"] if r else None

# --------- Loto helper ---------
def get_last_lotomania_number():
    # espera JSON com lista de dezenas; usamos APENAS o ÚLTIMO número sorteado
    r = requests.get(LOT_ENDPOINT, timeout=20, headers={"Accept": "application/json"})
    r.raise_for_status()
    j = r.json()
    # exemplo comum: j["listaDezenas"] -> ["00".."99"]
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
        draws = get_open_draws(conn)
        print(f"[run] sorteios abertos: {draws}")
        if not draws:
            return 0

        last_number = get_last_lotomania_number()

        for draw_id in draws:
            user_id = paid_user_for_number(conn, draw_id, last_number)
            if not user_id:
                print(f"[draw {draw_id}] último número {last_number} NÃO está pago; mantém aberto.")
                continue

            # fechar sorteio
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
