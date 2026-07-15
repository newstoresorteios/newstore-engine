import os
import smtplib
from email.message import EmailMessage
import psycopg2
from psycopg2.extras import RealDictCursor
import requests
from datetime import date, datetime, timezone
from push_automation_events import notify_push_automation_event
from push_automation_scan import run_push_automation_scan

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
    Retorna todos os sorteios fechados ainda sem resultado, em ordem de fechamento.
    draw_type nulo continua sendo tratado como principal legado.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT d.id,
                   d.status,
                   d.opened_at,
                   d.closed_at,
                   COALESCE(d.draw_type, 'principal') AS draw_type,
                   NULLIF(BTRIM(to_jsonb(d)->>'product_name'), '') AS product_name
              FROM draws d
             WHERE d.status = 'closed'
               AND d.realized_at IS NULL
               AND COALESCE(d.draw_type, 'principal') IN (
                   'principal',
                   'adicional',
                   'secundario'
               )
             ORDER BY d.closed_at ASC NULLS LAST, d.id ASC
        """)
        return cur.fetchall() or []

def get_open_draws_with_meta(conn):
    """(mantido para compat, não usado)"""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, opened_at
              FROM draws
             WHERE status = 'open'
               AND COALESCE(draw_type, 'principal') = 'principal'
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
    Conta quantos números estão efetivamente vendidos no draw.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) AS sold
              FROM public.numbers
             WHERE draw_id = %s
               AND status = 'sold'
        """, (draw_id,))
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
    Finaliza somente um sorteio que continua fechado e ainda não foi realizado:
      - status='sorteado'
      - winner_number / winner_user_id / winner_name
      - closed_at = COALESCE(closed_at, NOW())
      - realized_at = NOW()

    Retorna a quantidade de linhas atualizadas para impedir comunicação duplicada.
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
               AND status = 'closed'
               AND realized_at IS NULL
        """, (winner_number, winner_user_id, winner_name, draw_id))
        return cur.rowcount

def open_new_draw(conn):
    """Abre um novo sorteio 'open'. Ajuste os campos se sua tabela exigir mais colunas."""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO draws (status, opened_at, draw_type)
            VALUES ('open', NOW(), 'principal')
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

def _winner_name_from_user(name, email):
    clean_name = (name or "").strip()
    if clean_name:
        return clean_name
    clean_email = (email or "").strip()
    return clean_email or None

def _winner_identity_from_row(row):
    if not row or not row.get("user_id"):
        return None, None, None
    return (
        row["user_id"],
        _winner_name_from_user(row.get("name"), row.get("email")),
        row.get("email"),
    )

def paid_user_for_number_fallback(conn, draw_id: int, number: int):
    """Compatibility fallback when public.numbers has sold row without reservation_id."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT r.user_id, u.name, u.email
              FROM public.reservations r
         LEFT JOIN public.payments p ON p.id = r.payment_id AND p.draw_id = %s
         LEFT JOIN public.users u ON u.id = r.user_id
             WHERE r.draw_id = %s
               AND %s = ANY(r.numbers)
               AND (r.status = 'paid' OR p.status IN ('approved','paid'))
             LIMIT 1
        """, (draw_id, draw_id, number))
        r = cur.fetchone()
        user_id, winner_name, winner_email = _winner_identity_from_row(r)
        print("[winner] fallback lookup", {
            "draw_id": draw_id,
            "winner_number": number,
            "fallback_used": True,
            "winner_user_id": user_id,
        })
        return user_id, winner_name, winner_email

def winner_for_number(conn, draw_id: int, number: int):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT n, status, reservation_id
              FROM public.numbers
             WHERE draw_id = %s
               AND n = %s
             LIMIT 1
        """, (draw_id, number))
        number_row = cur.fetchone()

        if not number_row:
            print("[winner] number not found", {
                "draw_id": draw_id,
                "winner_number": number,
            })
            return None, None, None

        number_status = (number_row.get("status") or "").strip().lower()
        reservation_id = number_row.get("reservation_id")
        print("[winner] number lookup", {
            "draw_id": draw_id,
            "winner_number": number,
            "number_status": number_status,
            "reservation_id": reservation_id,
        })

        if number_status != "sold":
            print("[winner] number is not sold", {
                "draw_id": draw_id,
                "winner_number": number,
                "number_status": number_status,
            })
            return None, None, None

        if reservation_id:
            cur.execute("""
                SELECT r.user_id, u.name, u.email
                  FROM public.reservations r
             LEFT JOIN public.users u ON u.id = r.user_id
                 WHERE r.id = %s
                   AND r.draw_id = %s
                 LIMIT 1
            """, (reservation_id, draw_id))
            reservation_row = cur.fetchone()
            user_id, winner_name, winner_email = _winner_identity_from_row(reservation_row)
            if user_id:
                print("[winner] reservation resolved", {
                    "draw_id": draw_id,
                    "winner_number": number,
                    "reservation_id": reservation_id,
                    "winner_user_id": user_id,
                })
                return user_id, winner_name, winner_email

            print("[winner] reservation not resolved", {
                "draw_id": draw_id,
                "winner_number": number,
                "reservation_id": reservation_id,
            })
            return None, None, None

    print("[winner] sold number without reservation_id; using fallback", {
        "draw_id": draw_id,
        "winner_number": number,
    })
    return paid_user_for_number_fallback(conn, draw_id, number)

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
def _parse_lotomania_result_date(value):
    """Converte somente formatos de data conhecidos, sem inventar horário."""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value

    raw = str(value or "").strip()
    if not raw:
        return None
    for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            pass
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def get_last_lotomania_result():
    # Espera JSON com lista de dezenas; usamos APENAS o ÚLTIMO número sorteado.
    r = requests.get(LOT_ENDPOINT, timeout=20, headers={"Accept": "application/json"})
    r.raise_for_status()
    j = r.json()
    if not isinstance(j, dict):
        raise RuntimeError("Payload inválido da Lotomania")

    dezenas = j.get("listaDezenas") or j.get("dezenas") or []
    if not isinstance(dezenas, (list, tuple)) or not dezenas:
        raise RuntimeError("Sem dezenas no payload da Lotomania")

    try:
        ultimo = int(str(dezenas[-1]).strip().lstrip("0") or "0")  # "07" -> 7
    except (TypeError, ValueError) as exc:
        raise RuntimeError("Última dezena inválida no payload da Lotomania") from exc
    if ultimo < 0 or ultimo > 99:
        raise RuntimeError("Última dezena fora do intervalo 00-99")

    contest_number = j.get("numero")
    try:
        contest_number = int(contest_number) if contest_number is not None else None
    except (TypeError, ValueError):
        contest_number = None

    return {
        "winner_number": ultimo,
        "contest_number": contest_number,
        "result_date": _parse_lotomania_result_date(j.get("dataApuracao")),
    }


def get_last_lotomania_number():
    """Compatibilidade para consumidores antigos."""
    return get_last_lotomania_result()["winner_number"]

# --------- Push automation scanner ---------
def _run_push_automation_scan_safely(conn):
    if os.getenv("PUSH_AUTOMATION_SCAN_ENABLED", "false").lower() != "true":
        return

    try:
        run_push_automation_scan(conn)
    except Exception as exc:
        print("[push-automation] scan failed:", exc)

# --------- Main ---------
def _normalize_result_draw_type(value):
    draw_type = str(value or "principal").strip().lower()
    return "adicional" if draw_type in ("adicional", "secundario") else "principal"


def _result_before_draw_close(lotomania_result: dict, draw: dict) -> bool:
    result_date = lotomania_result.get("result_date")
    closed_at = draw.get("closed_at")
    if not isinstance(result_date, date) or not isinstance(closed_at, (date, datetime)):
        return False
    closed_date = closed_at.date() if isinstance(closed_at, datetime) else closed_at
    return result_date < closed_date


def _winner_defined_event(draw: dict, winner_number: int, winner_user_id, winner_name):
    draw_id = int(draw["id"])
    draw_type = _normalize_result_draw_type(draw.get("draw_type"))
    is_additional = draw_type == "adicional"
    metadata = {
        "draw_id": draw_id,
        "draw_type": draw_type,
        "winner_number": winner_number,
        "winner_user_id": winner_user_id,
        "winner_name": winner_name,
        "is_additional_draw": is_additional,
    }
    product_name = str(draw.get("product_name") or "").strip()
    if product_name:
        metadata["product_name"] = product_name
    return {
        "event_key": "WINNER_DEFINED",
        "reference_type": "additional_draw" if is_additional else "draw",
        "reference_key": (
            f"additional_draw:{draw_id}:winner_defined"
            if is_additional
            else f"draw:{draw_id}:winner_defined"
        ),
        "metadata": metadata,
    }


def _send_result_communications(
    draw: dict,
    draw_label: str,
    winner_number: int,
    winner_user_id,
    winner_name,
    winner_email,
    loser_list,
):
    winner_event = _winner_defined_event(
        draw,
        winner_number,
        winner_user_id,
        winner_name,
    )
    try:
        response = notify_push_automation_event(**winner_event)
        if isinstance(response, dict) and response.get("ok") is False:
            print("[push-automation] winner_defined not sent:", {
                "reference_key": winner_event["reference_key"],
                "reason": response.get("reason"),
                "status": response.get("status"),
            })
    except Exception as event_exc:
        print("[push-automation] winner_defined failed after commit:", {
            "reference_key": winner_event["reference_key"],
            "message": str(event_exc) or "event_failed",
        })

    if winner_user_id and winner_email:
        try:
            send_winner_email(
                winner_email,
                winner_name or "Participante",
                draw_label,
                int(draw["id"]),
                winner_number,
            )
        except Exception as email_exc:
            print("[email] erro apos commit:", repr(email_exc))
    elif winner_user_id:
        print(f"[email] usuário {winner_user_id} sem e-mail; não foi possível notificar vencedor.")

    try:
        send_draw_closed_admin(
            draw_label,
            int(draw["id"]),
            winner_number,
            winner_name,
            winner_email,
        )
    except Exception as email_exc:
        print("[email] erro apos commit:", repr(email_exc))

    print(f"[draw {draw['id']}] enviando e-mail de 'não contemplado' para {len(loser_list)} participantes")
    for participant in loser_list:
        if not participant.get("email"):
            continue
        try:
            send_loser_email(
                participant["email"],
                participant.get("name") or "Participante",
                draw_label,
                int(draw["id"]),
                winner_number,
                winner_name or "-",
            )
        except Exception as email_exc:
            print("[email] erro apos commit:", repr(email_exc))


def _process_pending_draw(conn, draw: dict, lotomania_result: dict) -> bool:
    draw_id = int(draw["id"])
    draw_type = _normalize_result_draw_type(draw.get("draw_type"))
    winner_number = int(lotomania_result["winner_number"])
    contest_number = lotomania_result.get("contest_number")
    result_date = lotomania_result.get("result_date")
    print("[result] processing_draw", {
        "draw_id": draw_id,
        "draw_type": draw_type,
        "winner_number": winner_number,
        "contest_number": contest_number,
        "result_at": result_date.isoformat() if isinstance(result_date, date) else None,
        "status": draw.get("status"),
    })

    if _result_before_draw_close(lotomania_result, draw):
        print("[result] result_before_draw_close", {
            "draw_id": draw_id,
            "draw_type": draw_type,
            "contest_number": contest_number,
            "result_at": result_date.isoformat(),
            "closed_at": draw["closed_at"].isoformat(),
        })
        return False

    product_name = str(draw.get("product_name") or "").strip()
    draw_label = product_name or get_draw_label(conn, draw_id)
    winner_user_id, winner_name, winner_email = winner_for_number(
        conn,
        draw_id,
        winner_number,
    )
    if winner_user_id is None:
        print("[result] draw_without_buyer", {
            "draw_id": draw_id,
            "draw_type": draw_type,
            "winner_number": winner_number,
        })

    updated_count = set_draw_sorteado_any_status(
        conn,
        draw_id,
        winner_number,
        winner_user_id,
        winner_name,
    )
    if updated_count != 1:
        conn.rollback()
        print("[result] already_processed_or_changed", {
            "draw_id": draw_id,
            "draw_type": draw_type,
            "status": draw.get("status"),
        })
        return False

    if not COMMIT:
        conn.rollback()
        print("[result] draw_skipped", {
            "draw_id": draw_id,
            "draw_type": draw_type,
            "reason": "dry_run",
        })
        return True

    conn.commit()
    print("[result] draw_processed", {
        "draw_id": draw_id,
        "draw_type": draw_type,
        "winner_number": winner_number,
        "winner_user_id": winner_user_id,
        "status": "sorteado",
    })
    try:
        participants = get_participants(conn, draw_id)
        loser_list = [
            participant
            for participant in participants
            if participant["id"] != (winner_user_id or -1)
        ]
    except Exception as participant_exc:
        loser_list = []
        print("[email] participantes indisponíveis após commit:", {
            "draw_id": draw_id,
            "reason": str(participant_exc) or participant_exc.__class__.__name__,
        })
    finally:
        try:
            conn.rollback()  # encerra apenas a transação de leitura pós-commit
        except Exception:
            pass
    _send_result_communications(
        draw,
        draw_label,
        winner_number,
        winner_user_id,
        winner_name,
        winner_email,
        loser_list,
    )
    return True


def run():
    print("[run] iniciando", datetime.now(timezone.utc).isoformat())
    conn = db()
    try:
        draws = get_pending_draws(conn)
        print("[result] pending_draws_found", {
            "count": len(draws),
            "draw_ids": [int(draw["id"]) for draw in draws],
        })
        if not draws:
            _run_push_automation_scan_safely(conn)
            return 0

        try:
            lotomania_result = get_last_lotomania_result()
        except Exception as api_exc:
            conn.rollback()
            print("[result] api_result_unavailable", {
                "reason": str(api_exc) or api_exc.__class__.__name__,
            })
            _run_push_automation_scan_safely(conn)
            return 1

        result_date = lotomania_result.get("result_date")
        print("[result] lotomania_result_loaded", {
            "winner_number": lotomania_result["winner_number"],
            "contest_number": lotomania_result.get("contest_number"),
            "result_at": result_date.isoformat() if isinstance(result_date, date) else None,
        })

        for draw in draws:
            try:
                _process_pending_draw(conn, draw, lotomania_result)
            except Exception as draw_exc:
                try:
                    conn.rollback()
                except Exception:
                    pass
                print("[result] draw_skipped", {
                    "draw_id": draw.get("id"),
                    "draw_type": _normalize_result_draw_type(draw.get("draw_type")),
                    "reason": str(draw_exc) or draw_exc.__class__.__name__,
                })

        _run_push_automation_scan_safely(conn)
        return 0
    except Exception as exc:
        print("[run] erro:", repr(exc))
        try:
            conn.rollback()
        except Exception:
            pass
        return 1
    finally:
        conn.close()

if __name__ == "__main__":
    exit(run())
