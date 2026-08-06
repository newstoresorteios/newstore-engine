import os
import smtplib
from email.message import EmailMessage
import psycopg2
from psycopg2.extras import RealDictCursor
import requests
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
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
try:
    BRASILIA_TZ = ZoneInfo("America/Sao_Paulo")
except ZoneInfoNotFoundError:
    # Windows sem base IANA local; Brasília usa UTC-03:00 desde 2019.
    BRASILIA_TZ = timezone(timedelta(hours=-3), name="America/Sao_Paulo")
LOTOMANIA_DRAW_TIME_BRT = time(21, 0)

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
        print("[email] send_failed", {"reason": "smtp_config_or_recipient_missing"})
        return False
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = f"{SMTP_NAME} <{SMTP_FROM}>"
    msg["To"] = to_email
    msg.set_content(body)

    print("[email] send_attempt")
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=20) as s:
        s.starttls()
        s.login(SMTP_USER, SMTP_PASS)
        if COMMIT:
            s.send_message(msg)
            print("[email] sent")
            return True
    print("[email] send_failed", {"reason": "dry_run"})
    return False

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
    return _smtp_send(to_email, subj, body)

def send_draw_closed_admin(draw_label: str, draw_id: int, winner_number: int, winner_name: str, winner_email: str):
    """E-mail para o admin informando fechamento do sorteio."""
    if not ADMIN_EMAIL:
        print("[email-admin] ADMIN_EMAIL vazio; pulando.")
        return False
    subj = f"✅ {APP_NAME}: {draw_label} (#{draw_id}) SORTEADO"
    body = f"""{draw_label} (#{draw_id}) foi realizado e marcado como SORTEADO.

Número sorteado (vencedor): {winner_number:02d}

Ganhador:
- Nome:  {winner_name or '-'}
- E-mail: {winner_email or '-'}

Data/Hora (UTC): {datetime.utcnow().isoformat()}Z
"""
    return _smtp_send(ADMIN_EMAIL, subj, body)

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
    return _smtp_send(to_email, subj, body)

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


def lock_pending_draw_for_result(conn, draw_id: int):
    """Serializa a definição do resultado e revalida o estado atual do draw."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, status, realized_at, closed_at
              FROM draws
             WHERE id = %s
               AND status = 'closed'
               AND realized_at IS NULL
             FOR UPDATE
        """, (draw_id,))
        row = cur.fetchone()
        if not row:
            return None
        if row.get("status") != "closed" or row.get("realized_at") is not None:
            return None
        return row


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
    """Converte somente formatos de data conhecidos."""
    if isinstance(value, datetime):
        if value.tzinfo is not None and value.utcoffset() is not None:
            return value.astimezone(BRASILIA_TZ).date()
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
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if parsed.tzinfo is not None and parsed.utcoffset() is not None:
            parsed = parsed.astimezone(BRASILIA_TZ)
        return parsed.date()
    except ValueError:
        return None


def _parse_lotomania_contest_number(value, field_name):
    try:
        contest_number = int(value)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"{field_name} inválido no payload da Lotomania") from exc
    if contest_number <= 0:
        raise RuntimeError(f"{field_name} inválido no payload da Lotomania")
    return contest_number


def _parse_lotomania_payload(payload):
    if not isinstance(payload, dict):
        raise RuntimeError("Payload inválido da Lotomania")

    ordered_numbers = payload.get("dezenasSorteadasOrdemSorteio")
    if not isinstance(ordered_numbers, (list, tuple)) or not ordered_numbers:
        raise RuntimeError(
            "Campo dezenasSorteadasOrdemSorteio ausente, inválido ou vazio"
        )

    parsed_numbers = []
    for value in ordered_numbers:
        try:
            number = int(str(value).strip())
        except (TypeError, ValueError) as exc:
            raise RuntimeError(
                "Dezena inválida em dezenasSorteadasOrdemSorteio"
            ) from exc
        if number < 0 or number > 99:
            raise RuntimeError(
                "Dezena fora do intervalo 00-99 em dezenasSorteadasOrdemSorteio"
            )
        parsed_numbers.append(number)

    result_date = _parse_lotomania_result_date(payload.get("dataApuracao"))
    if result_date is None:
        raise RuntimeError("Data de apuração inválida no payload da Lotomania")

    contest_number = _parse_lotomania_contest_number(
        payload.get("numero"),
        "Número do concurso",
    )
    previous_raw = payload.get("numeroConcursoAnterior")
    previous_contest_number = (
        _parse_lotomania_contest_number(previous_raw, "Concurso anterior")
        if previous_raw not in (None, "")
        else None
    )

    return {
        "winner_number": parsed_numbers[-1],
        "contest_number": contest_number,
        "previous_contest_number": previous_contest_number,
        "result_date": result_date,
    }


def get_lotomania_result(contest_number=None):
    endpoint = LOT_ENDPOINT.rstrip("/")
    if contest_number is not None:
        requested_contest = _parse_lotomania_contest_number(
            contest_number,
            "Número do concurso solicitado",
        )
        endpoint = f"{endpoint}/{requested_contest}"
    else:
        requested_contest = None

    r = requests.get(endpoint, timeout=20, headers={"Accept": "application/json"})
    r.raise_for_status()
    result = _parse_lotomania_payload(r.json())
    if (
        requested_contest is not None
        and result["contest_number"] != requested_contest
    ):
        raise RuntimeError(
            "API da Lotomania retornou concurso diferente do solicitado"
        )
    return result


def get_last_lotomania_result():
    return get_lotomania_result()


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


def _as_brasilia_datetime(value):
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            return value.replace(tzinfo=BRASILIA_TZ)
        return value.astimezone(BRASILIA_TZ)
    if isinstance(value, date):
        return datetime.combine(value, time.min, tzinfo=BRASILIA_TZ)
    return None


def _lotomania_result_at(lotomania_result: dict):
    explicit_result_at = lotomania_result.get("result_at")
    if explicit_result_at is not None:
        parsed = _as_brasilia_datetime(explicit_result_at)
        if parsed is None:
            raise RuntimeError("Horário do resultado da Lotomania inválido")
        return parsed

    result_date = lotomania_result.get("result_date")
    if not isinstance(result_date, date):
        raise RuntimeError("Data do resultado da Lotomania ausente ou inválida")
    return datetime.combine(
        result_date,
        LOTOMANIA_DRAW_TIME_BRT,
        tzinfo=BRASILIA_TZ,
    )


def _result_before_draw_close(lotomania_result: dict, draw: dict) -> bool:
    result_at = _lotomania_result_at(lotomania_result)
    closed_at = _as_brasilia_datetime(draw.get("closed_at"))
    if closed_at is None:
        raise RuntimeError("closed_at ausente ou inválido para resolver resultado")
    return result_at < closed_at


def resolve_first_eligible_lotomania_result(
    draw: dict,
    latest_result: dict,
    contest_cache=None,
    result_fetcher=None,
):
    """
    Caminha para trás a partir do concurso mais recente até encontrar o primeiro
    cuja apuração ocorreu no mesmo instante ou depois do fechamento do draw.
    """
    closed_at = _as_brasilia_datetime(draw.get("closed_at"))
    if closed_at is None:
        raise RuntimeError("closed_at ausente ou inválido para resolver resultado")

    cache = contest_cache if contest_cache is not None else {}
    fetch_result = result_fetcher or get_lotomania_result
    candidate = latest_result
    seen = set()

    while True:
        contest_number = _parse_lotomania_contest_number(
            candidate.get("contest_number"),
            "Número do concurso",
        )
        if contest_number in seen:
            raise RuntimeError("Ciclo detectado no histórico de concursos da Lotomania")
        seen.add(contest_number)
        cache[contest_number] = candidate

        candidate_at = _lotomania_result_at(candidate)
        if candidate_at < closed_at:
            return None

        previous_number = candidate.get("previous_contest_number")
        if previous_number in (None, ""):
            if contest_number == 1:
                return candidate
            raise RuntimeError(
                "Concurso anterior ausente; não é possível provar o primeiro resultado elegível"
            )
        previous_number = _parse_lotomania_contest_number(
            previous_number,
            "Concurso anterior",
        )
        if previous_number >= contest_number:
            raise RuntimeError("Sequência de concursos da Lotomania inválida")

        previous_result = cache.get(previous_number)
        if previous_result is None:
            previous_result = fetch_result(previous_number)
            returned_number = _parse_lotomania_contest_number(
                previous_result.get("contest_number"),
                "Número do concurso",
            )
            if returned_number != previous_number:
                raise RuntimeError(
                    "API da Lotomania retornou concurso diferente do solicitado"
                )
            cache[previous_number] = previous_result

        if _lotomania_result_at(previous_result) < closed_at:
            return candidate
        candidate = previous_result


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


def _log_email_failure(draw_id: int, category: str, error):
    details = {
        "draw_id": draw_id,
        "category": category,
        "error_type": error.__class__.__name__,
    }
    smtp_code = getattr(error, "smtp_code", None)
    if isinstance(smtp_code, int):
        details["smtp_code"] = smtp_code
    print("[email] send_failed", details)


def _send_result_communications(
    draw: dict,
    draw_label: str,
    winner_number: int,
    winner_user_id,
    winner_name,
    winner_email,
    loser_list,
):
    draw_id = int(draw["id"])
    summary = {
        "winner_email": "skipped",
        "admin_email": "skipped",
        "loser_emails": {"sent": 0, "failed": 0},
    }
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
            sent = send_winner_email(
                winner_email,
                winner_name or "Participante",
                draw_label,
                draw_id,
                winner_number,
            )
            summary["winner_email"] = "sent" if sent is not False else "failed"
        except Exception as email_exc:
            summary["winner_email"] = "failed"
            _log_email_failure(draw_id, "winner_email", email_exc)
    elif winner_user_id:
        print("[email] winner_email_skipped", {
            "draw_id": draw_id,
            "reason": "winner_without_email",
        })

    try:
        sent = send_draw_closed_admin(
            draw_label,
            draw_id,
            winner_number,
            winner_name,
            winner_email,
        )
        summary["admin_email"] = "sent" if sent is not False else "failed"
    except Exception as email_exc:
        summary["admin_email"] = "failed"
        _log_email_failure(draw_id, "admin_email", email_exc)

    print(f"[draw {draw_id}] enviando e-mail de 'não contemplado' para {len(loser_list)} participantes")
    for participant in loser_list:
        if not participant.get("email"):
            continue
        try:
            sent = send_loser_email(
                participant["email"],
                participant.get("name") or "Participante",
                draw_label,
                draw_id,
                winner_number,
                winner_name or "-",
            )
            if sent is False:
                summary["loser_emails"]["failed"] += 1
            else:
                summary["loser_emails"]["sent"] += 1
        except Exception as email_exc:
            summary["loser_emails"]["failed"] += 1
            _log_email_failure(draw_id, "loser_email", email_exc)
    print("[email] result_delivery_summary", {
        "draw_id": draw_id,
        **summary,
    })
    return summary


def _process_pending_draw(conn, draw: dict, lotomania_result: dict) -> bool:
    draw_id = int(draw["id"])
    draw_type = _normalize_result_draw_type(draw.get("draw_type"))
    winner_number = int(lotomania_result["winner_number"])
    contest_number = lotomania_result.get("contest_number")
    result_at = _lotomania_result_at(lotomania_result)
    print("[result] processing_draw", {
        "draw_id": draw_id,
        "draw_type": draw_type,
        "winner_number": winner_number,
        "contest_number": contest_number,
        "result_at": result_at.isoformat(),
        "status": draw.get("status"),
    })

    locked_draw = lock_pending_draw_for_result(conn, draw_id)
    if locked_draw is None:
        conn.rollback()
        print("[result] already_processed_or_changed", {
            "draw_id": draw_id,
            "draw_type": draw_type,
            "status": draw.get("status"),
        })
        return False
    draw = {**draw, **locked_draw}

    if _result_before_draw_close(lotomania_result, draw):
        print("[result] result_before_draw_close", {
            "draw_id": draw_id,
            "draw_type": draw_type,
            "contest_number": contest_number,
            "result_at": result_at.isoformat(),
            "closed_at": draw["closed_at"].isoformat(),
        })
        conn.rollback()
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

        result_at = _lotomania_result_at(lotomania_result)
        print("[result] lotomania_result_loaded", {
            "winner_number": lotomania_result["winner_number"],
            "contest_number": lotomania_result.get("contest_number"),
            "result_at": result_at.isoformat(),
        })
        contest_cache = {
            int(lotomania_result["contest_number"]): lotomania_result,
        }

        for draw in draws:
            try:
                eligible_result = resolve_first_eligible_lotomania_result(
                    draw,
                    lotomania_result,
                    contest_cache=contest_cache,
                )
                if eligible_result is None:
                    print("[result] awaiting_first_eligible_contest", {
                        "draw_id": draw.get("id"),
                        "draw_type": _normalize_result_draw_type(draw.get("draw_type")),
                        "closed_at": (
                            draw["closed_at"].isoformat()
                            if isinstance(draw.get("closed_at"), (date, datetime))
                            else None
                        ),
                        "latest_contest_number": lotomania_result.get("contest_number"),
                    })
                    continue
                _process_pending_draw(conn, draw, eligible_result)
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
