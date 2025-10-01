#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import smtplib
from email.message import EmailMessage
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timezone

# ---- ENV ----
DB_URL = os.getenv("POSTGRES_URL", "")

# SMTP
SMTP_HOST = os.getenv("SMTP_HOST", "smtp-relay.brevo.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASS", "")
SMTP_FROM = os.getenv("SMTP_FROM", "contato@newstorerj.com.br")
SMTP_NAME = os.getenv("SMTP_NAME", "NewStore Sorteios")

# Sandbox opcional: redireciona todos os envios para um único endereço
EMAIL_SANDBOX_TO = os.getenv("EMAIL_SANDBOX_TO", "")

# Fallback (se não houver participantes elegíveis)
NOTIFY_FALLBACK_TO = os.getenv("NOTIFY_FALLBACK_TO", "")

# Link do stream da Caixa
YOUTUBE_STREAMS_URL = os.getenv(
    "YOUTUBE_STREAMS_URL",
    "https://www.youtube.com/@canalcaixa/streams",
)

# Modo commit/dry-run
COMMIT = os.getenv("COMMIT", "false").lower() in ("1", "true", "yes")
DRY_RUN = not COMMIT

# Fuse anti-acidente (opcional)
ENVIRONMENT = os.getenv("ENVIRONMENT", "production").lower()
ALLOW_PROD_DRYRUN = os.getenv("ALLOW_PROD_DRYRUN", "").lower() in ("1", "true", "yes")


def log(*a):
    print("[notify_start]", *a)


def db_connect():
    if not DB_URL:
        raise RuntimeError("POSTGRES_URL não configurada")
    conn = psycopg2.connect(DB_URL, cursor_factory=RealDictCursor)
    # leitura apenas (por segurança)
    conn.set_session(readonly=True, autocommit=False)
    return conn


def get_open_draw(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            select id, opened_at
              from draws
             where status = 'open'
             order by id desc
             limit 1
            """
        )
        row = cur.fetchone()
        return row


def get_recipients_for_open_draw(conn, draw_id):
    """
    Envia para quem tem participação válida:
    - reservations.status = 'paid' OU payments.status IN ('approved','paid')
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            with elegiveis as (
                select distinct u.id, u.name, u.email
                  from users u
                  join reservations r on r.user_id = u.id
                 where r.draw_id = %s
                   and r.status in ('paid')
                union
                select distinct u.id, u.name, u.email
                  from users u
                  join payments p on p.user_id = u.id
                 where p.draw_id = %s
                   and p.status in ('approved','paid')
            )
            select id, name, email
              from elegiveis
             where coalesce(nullif(email,''), '') <> ''
            """,
            (draw_id, draw_id),
        )
        rows = cur.fetchall() or []
        return rows


def build_email_subject(draw_id):
    return f"[NewStore] Sorteio começa às 20:00 – Assista ao vivo (Sorteio #{draw_id})"


def build_email_body(draw_id):
    return (
        f"Olá!\n\n"
        f"O sorteio #{draw_id} começa hoje às 20:00.\n"
        f"Acompanhe ao vivo no canal da Caixa:\n\n"
        f"{YOUTUBE_STREAMS_URL}\n\n"
        f"Boa sorte! 🍀\n"
        f"— Equipe NewStore\n"
    )


def send_email(to_addrs, subject, body):
    if not to_addrs:
        return

    # Dry-run: bloqueia absolutamente
    if DRY_RUN:
        log("DRY-RUN: envio BLOQUEADO ->", {"to": to_addrs, "subject": subject})
        return

    # Sandbox: redireciona todos para um único destinatário
    if EMAIL_SANDBOX_TO:
        log("SANDBOX ativo: redirecionando todos os envios para", EMAIL_SANDBOX_TO)
        to_addrs = [EMAIL_SANDBOX_TO]

    msg = EmailMessage()
    msg["From"] = f"{SMTP_NAME} <{SMTP_FROM}>"
    msg["To"] = ", ".join(to_addrs)
    msg["Subject"] = subject
    msg.set_content(body)

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=20) as s:
        s.starttls()
        if SMTP_USER and SMTP_PASS:
            s.login(SMTP_USER, SMTP_PASS)
        s.send_message(msg)
        log("E-mail enviado para", to_addrs)


def main():
    log("IN", {"at": datetime.now(timezone.utc).isoformat(), "commit": COMMIT})

    # Fuse: impede dry-run em produção sem permissão explícita
    if ENVIRONMENT == "production" and DRY_RUN and not ALLOW_PROD_DRYRUN:
        log("FUSE: abortado (COMMIT=false em produção sem ALLOW_PROD_DRYRUN=1)")
        return

    conn = db_connect()
    try:
        open_draw = get_open_draw(conn)
        if not open_draw:
            log("Não há sorteio com status 'open'. Nada a notificar.")
            if NOTIFY_FALLBACK_TO:
                subj = "[NewStore] Aviso 20:00 – não há sorteio 'open' hoje"
                body = (
                    "Rotina de aviso 20:00 executada, porém não há sorteio com status 'open'.\n"
                    "Sem ações."
                )
                send_email([NOTIFY_FALLBACK_TO], subj, body)
            return

        draw_id = open_draw["id"]
        recipients = get_recipients_for_open_draw(conn, draw_id)
        emails = sorted({r["email"] for r in recipients if r.get("email")})

        if not emails and NOTIFY_FALLBACK_TO:
            emails = [NOTIFY_FALLBACK_TO]

        if not emails:
            log("Nenhum destinatário encontrado (participantes ou fallback).")
            return

        subject = build_email_subject(draw_id)
        body = build_email_body(draw_id)
        send_email(emails, subject, body)

    finally:
        conn.close()
        log("DONE")


if __name__ == "__main__":
    main()
