import io
import unittest
from contextlib import redirect_stdout

import main

APPROVED = {"approved", "paid", "pago"}


class FakeDb:
    """Simula as consultas de winner_for_number sem banco real (filtra por draw_id e numero)."""

    def __init__(self, numbers=None, reservations=None, payments=None, users=None):
        self.numbers = numbers or []
        self.reservations = reservations or []
        self.payments = payments or []
        self.users = users or {}
        self.sql_log = []
        self._one = None
        self._all = []

    # conexao / cursor
    def cursor(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def fetchone(self):
        return self._one

    def fetchall(self):
        return self._all

    def _user(self, user_id):
        return self.users.get(user_id, {})

    def execute(self, sql, params=None):
        sql = " ".join(sql.split())
        self.sql_log.append((sql, params))
        self._one, self._all = None, []
        if "FROM public.numbers" in sql:
            draw_id, n = params
            for row in self.numbers:
                if row["draw_id"] == draw_id and row["n"] == n:
                    self._one = {"n": n, "status": row["status"], "reservation_id": row.get("reservation_id")}
        elif "WHERE r.id = %s AND r.draw_id = %s" in sql:
            res_id, draw_id = params
            for r in self.reservations:
                if r["id"] == res_id and r["draw_id"] == draw_id:
                    u = self._user(r["user_id"])
                    self._one = {"user_id": r["user_id"], "name": u.get("name"), "email": u.get("email")}
        elif "FROM public.reservations r" in sql:  # fallback legado
            assert "LIMIT" not in sql and "SELECT DISTINCT r.user_id" in sql
            payment_draw, draw_id, n = params
            seen = {}
            for r in self.reservations:
                if r["draw_id"] != draw_id or n not in r["numbers"]:
                    continue
                linked = [p for p in self.payments if p["id"] == r.get("payment_id") and p["draw_id"] == payment_draw]
                if r["status"] == "paid" or any(p["status"] in ("approved", "paid") for p in linked):
                    u = self._user(r["user_id"])
                    seen[r["user_id"]] = {"user_id": r["user_id"], "name": u.get("name"), "email": u.get("email")}
            self._all = list(seen.values())
        elif "FROM public.payments p" in sql:  # fallback por pagamentos
            assert "p.draw_id = %s" in sql and "= ANY(p.numbers)" in sql
            draw_id, n = params
            seen = {}
            for p in self.payments:
                if (p["draw_id"] == draw_id and n in p["numbers"]
                        and (p["status"] or "").lower() in APPROVED and p["user_id"] is not None):
                    u = self._user(p["user_id"])
                    seen[p["user_id"]] = {"user_id": p["user_id"], "name": u.get("name"), "email": u.get("email")}
            self._all = list(seen.values())
        else:  # pragma: no cover
            raise AssertionError(f"consulta inesperada: {sql[:80]}")


USERS = {
    3: {"name": "Felipe N.", "email": "f@example.com"},
    7: {"name": "Cliente A", "email": "a@example.com"},
    8: {"name": "Cliente B", "email": "b@example.com"},
}


def resolve(db, draw_id, n):
    with redirect_stdout(io.StringIO()):
        return main.winner_for_number(db, draw_id, n)


def pay(pid, draw_id, user_id, numbers, status="approved"):
    return {"id": pid, "draw_id": draw_id, "user_id": user_id, "numbers": numbers, "status": status}


class WinnerResolutionTests(unittest.TestCase):
    def test_sold_with_valid_reservation_keeps_existing_behavior(self):
        db = FakeDb(
            numbers=[{"draw_id": 133, "n": 33, "status": "sold", "reservation_id": "r1"}],
            reservations=[{"id": "r1", "draw_id": 133, "user_id": 7, "status": "paid", "numbers": [33]}],
            users=USERS,
        )
        self.assertEqual(resolve(db, 133, 33), (7, "Cliente A", "a@example.com"))
        self.assertFalse(any("FROM public.payments p" in sql for sql, _ in db.sql_log))

    def test_sold_without_reservation_keeps_legacy_reservation_fallback(self):
        db = FakeDb(
            numbers=[{"draw_id": 133, "n": 33, "status": "sold"}],
            reservations=[{"id": "r1", "draw_id": 133, "user_id": 7, "status": "paid", "numbers": [33]}],
            users=USERS,
        )
        self.assertEqual(resolve(db, 133, 33), (7, "Cliente A", "a@example.com"))

    def test_sold_without_reservation_but_approved_payment_still_resolves(self):
        db = FakeDb(
            numbers=[{"draw_id": 133, "n": 33, "status": "sold"}],
            payments=[pay("p1", 133, 7, [10, 33])],
            users=USERS,
        )
        self.assertEqual(resolve(db, 133, 33), (7, "Cliente A", "a@example.com"))

    def test_sold_with_unresolvable_reservation_falls_back_to_payment(self):
        db = FakeDb(
            numbers=[{"draw_id": 133, "n": 33, "status": "sold", "reservation_id": "missing"}],
            payments=[pay("p1", 133, 8, [33])],
            users=USERS,
        )
        self.assertEqual(resolve(db, 133, 33), (8, "Cliente B", "b@example.com"))

    def test_available_with_single_approved_payment_resolves_buyer(self):
        db = FakeDb(numbers=[{"draw_id": 133, "n": 33, "status": "available"}],
                    payments=[pay("p1", 133, 7, [33])], users=USERS)
        self.assertEqual(resolve(db, 133, 33), (7, "Cliente A", "a@example.com"))

    def test_reserved_with_single_approved_payment_resolves_buyer(self):
        db = FakeDb(numbers=[{"draw_id": 133, "n": 33, "status": "reserved", "reservation_id": "r9"}],
                    payments=[pay("p1", 133, 7, [33], status="PAID")], users=USERS)
        self.assertEqual(resolve(db, 133, 33), (7, "Cliente A", "a@example.com"))

    def test_inconsistent_state_without_approved_payment_has_no_buyer(self):
        for status in ("available", "reserved", "blocked", ""):
            with self.subTest(status=status):
                db = FakeDb(numbers=[{"draw_id": 133, "n": 33, "status": status}],
                            payments=[pay("p1", 133, 7, [33], status="pending"),
                                      pay("p2", 133, 8, [33], status="failed")], users=USERS)
                self.assertEqual(resolve(db, 133, 33), (None, None, None))

    def test_payment_for_same_number_in_another_draw_is_ignored(self):
        db = FakeDb(numbers=[{"draw_id": 133, "n": 33, "status": "available"}],
                    payments=[pay("p1", 999, 7, [33])], users=USERS)
        self.assertEqual(resolve(db, 133, 33), (None, None, None))
        sql, params = next((s, p) for s, p in db.sql_log if "FROM public.payments p" in s)
        self.assertEqual(params, (133, 33))

    def test_two_distinct_approved_users_are_not_chosen(self):
        db = FakeDb(numbers=[{"draw_id": 133, "n": 33, "status": "available"}],
                    payments=[pay("p1", 133, 7, [33]), pay("p2", 133, 8, [33])], users=USERS)
        with self.assertRaisesRegex(RuntimeError, "ambiguous_paid_owner"):
            resolve(db, 133, 33)

    def test_two_payments_of_the_same_user_are_not_ambiguous(self):
        db = FakeDb(numbers=[{"draw_id": 133, "n": 33, "status": "available"}],
                    payments=[pay("p1", 133, 7, [33]), pay("p2", 133, 7, [33, 34])], users=USERS)
        self.assertEqual(resolve(db, 133, 33)[0], 7)

    def test_number_missing_from_grid_still_has_no_buyer(self):
        db = FakeDb(numbers=[], payments=[pay("p1", 133, 7, [33])], users=USERS)
        self.assertEqual(resolve(db, 133, 33), (None, None, None))

    def test_draw_110_scenario_resolves_user_3(self):
        db = FakeDb(
            numbers=[{"draw_id": 110, "n": 79, "status": "available", "reservation_id": None}],
            payments=[pay("1776726034139", 110, 3, [45, 46, 52, 54, 79, 80])],
            users=USERS,
        )
        self.assertEqual(resolve(db, 110, 79), (3, "Felipe N.", "f@example.com"))

    def test_ambiguity_makes_the_draw_fail_without_writing(self):
        # um draw ambiguo deve falhar no run() e nao gravar resultado
        from unittest.mock import patch
        from test_main_results import VALID_GRID, draw, lotomania, FakeConnection

        db = FakeDb(numbers=[{"draw_id": 133, "n": 33, "status": "available"}],
                    payments=[pay("p1", 133, 7, [33]), pay("p2", 133, 8, [33])], users=USERS)
        conn = FakeConnection()
        conn.cursor = db.cursor
        with patch.object(main, "COMMIT", True), \
             patch.object(main, "db", return_value=conn), \
             patch.object(main, "get_pending_draws", return_value=[draw(133)]), \
             patch.object(main, "get_last_lotomania_result", return_value=lotomania()), \
             patch.object(main, "resolve_first_eligible_lotomania_result", side_effect=lambda _d, latest, **_k: latest), \
             patch.object(main, "lock_pending_draw_for_result", return_value=draw(133)), \
             patch.object(main, "get_draw_number_grid", return_value=VALID_GRID), \
             patch.object(main, "set_draw_sorteado_any_status") as setter, \
             patch.object(main, "_send_result_communications") as comms, \
             patch.object(main, "_run_push_automation_scan_safely"), \
             redirect_stdout(io.StringIO()):
            code = main.run()
        self.assertEqual(code, 1)
        setter.assert_not_called()
        comms.assert_not_called()
        self.assertEqual(conn.commit_count, 0)


def paid_reservation(rid, draw_id, user_id, numbers, status="paid"):
    return {"id": rid, "draw_id": draw_id, "user_id": user_id, "status": status, "numbers": numbers}


class LegacyFallbackHardeningTests(unittest.TestCase):
    """sold sem reservation_id usa o fallback legado: nunca escolher um dono arbitrario."""

    def sold(self, draw_id=133, n=33):
        return [{"draw_id": draw_id, "n": n, "status": "sold"}]

    def test_zero_owners_has_no_buyer(self):
        db = FakeDb(numbers=self.sold(), users=USERS)
        self.assertEqual(resolve(db, 133, 33), (None, None, None))

    def test_single_owner_is_returned(self):
        db = FakeDb(numbers=self.sold(), reservations=[paid_reservation("r1", 133, 7, [33])], users=USERS)
        self.assertEqual(resolve(db, 133, 33), (7, "Cliente A", "a@example.com"))

    def test_several_reservations_of_same_user_are_not_ambiguous(self):
        db = FakeDb(numbers=self.sold(),
                    reservations=[paid_reservation("r1", 133, 7, [33]), paid_reservation("r2", 133, 7, [33, 40])],
                    users=USERS)
        self.assertEqual(resolve(db, 133, 33)[0], 7)

    def test_two_distinct_users_raise_ambiguous_paid_owner(self):
        db = FakeDb(numbers=self.sold(),
                    reservations=[paid_reservation("r1", 133, 7, [33]), paid_reservation("r2", 133, 8, [33])],
                    users=USERS)
        with self.assertRaisesRegex(RuntimeError, "ambiguous_paid_owner"):
            resolve(db, 133, 33)

    def test_legacy_query_is_scoped_to_draw_and_ignores_other_draws(self):
        db = FakeDb(numbers=self.sold(),
                    reservations=[paid_reservation("r1", 999, 7, [33]), paid_reservation("r2", 133, 8, [33])],
                    users=USERS)
        self.assertEqual(resolve(db, 133, 33)[0], 8)
        sql, params = next((s, p) for s, p in db.sql_log if "FROM public.reservations r" in s)
        self.assertEqual(params, (133, 133, 33))
        self.assertIn("r.draw_id = %s", sql)

    def test_other_draw_with_same_number_does_not_create_ambiguity(self):
        db = FakeDb(numbers=self.sold(),
                    reservations=[paid_reservation("r1", 999, 7, [33]), paid_reservation("r2", 133, 8, [33])],
                    payments=[pay("p1", 999, 7, [33])], users=USERS)
        self.assertEqual(resolve(db, 133, 33)[0], 8)

    def test_legacy_without_owner_still_tries_payment_fallback(self):
        db = FakeDb(numbers=self.sold(), payments=[pay("p1", 133, 7, [33])], users=USERS)
        self.assertEqual(resolve(db, 133, 33)[0], 7)

    def test_normal_reservation_path_does_not_touch_legacy_or_payment_queries(self):
        db = FakeDb(numbers=[{"draw_id": 133, "n": 33, "status": "sold", "reservation_id": "r1"}],
                    reservations=[paid_reservation("r1", 133, 7, [33])], users=USERS)
        self.assertEqual(resolve(db, 133, 33)[0], 7)
        self.assertEqual(len(db.sql_log), 2)

    def test_ambiguous_draw_is_not_updated_other_draws_continue_and_exit_is_1(self):
        from unittest.mock import Mock, patch
        from test_main_results import VALID_GRID, FakeConnection, draw, lotomania

        db = FakeDb(
            numbers=self.sold(133, 33) + self.sold(134, 33),
            reservations=[
                paid_reservation("a1", 133, 7, [33]), paid_reservation("a2", 133, 8, [33]),   # ambiguo
                paid_reservation("b1", 134, 7, [33]),                                          # unico
            ],
            users=USERS,
        )
        conn = FakeConnection()
        conn.cursor = db.cursor
        by_id = {133: draw(133), 134: draw(134)}
        setter = Mock(return_value=1)
        comms = Mock()
        with patch.object(main, "COMMIT", True), \
             patch.object(main, "db", return_value=conn), \
             patch.object(main, "get_pending_draws", return_value=[by_id[133], by_id[134]]), \
             patch.object(main, "get_last_lotomania_result", return_value=lotomania()), \
             patch.object(main, "resolve_first_eligible_lotomania_result", side_effect=lambda _d, latest, **_k: latest), \
             patch.object(main, "lock_pending_draw_for_result", side_effect=lambda _c, i: by_id[i]), \
             patch.object(main, "get_draw_number_grid", return_value=VALID_GRID), \
             patch.object(main, "get_draw_label", return_value="Sorteio"), \
             patch.object(main, "set_draw_sorteado_any_status", setter), \
             patch.object(main, "_send_result_communications", comms), \
             patch.object(main, "_run_push_automation_scan_safely"), \
             redirect_stdout(io.StringIO()):
            code = main.run()
        self.assertEqual(code, 1)
        self.assertEqual([call.args[1] for call in setter.call_args_list], [134])  # 133 ambiguo: sem UPDATE
        self.assertEqual(comms.call_count, 1)
        self.assertEqual(conn.commit_count, 1)


if __name__ == "__main__":
    unittest.main()
