import re
import unittest
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import push_automation_scan as scan
from push_automation_scan import (
    ADDITIONAL_WINNER_TEMPORAL_COLUMNS,
    WINNER_TEMPORAL_COLUMNS,
    _empty_event_key_stats,
    emit_additional_winner_defined_events,
    emit_winner_defined_events,
)

NOW = datetime(2026, 10, 7, 12, 0, tzinfo=timezone.utc)

# colunas reais de public.draws
REAL_DRAWS_COLUMNS = {
    "id": "integer",
    "status": "text",
    "opened_at": "timestamptz",
    "closed_at": "timestamptz",
    "realized_at": "timestamptz",
    "winner_user_id": "integer",
    "winner_name": "text",
    "created_at": "timestamptz",
    "winner_number": "integer",
    "product_name": "text",
    "product_link": "text",
    "autopay_ran_at": "timestamptz",
    "draw_type": "text",
}


class SimDrawsCursor:
    """Avalia a coluna temporal escolhida pelo SQL do scanner (WHERE col >= NOW() - N horas)."""

    def __init__(self, rows):
        self.rows = rows
        self.queries = []
        self._result = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        sql = " ".join(sql.split())
        self.queries.append((sql, params))
        match = re.search(r'AND "(\w+)" >= NOW\(\) - \(%s \* INTERVAL \'1 hour\'\)', sql)
        selected = list(self.rows)
        selected = [r for r in selected if r.get("status") == "sorteado"] if "status = 'sorteado'" in sql else selected
        if "COALESCE(draw_type, 'principal') = 'principal'" in sql:
            selected = [r for r in selected if (r.get("draw_type") or "principal") == "principal"]
        if "draw_type IN ('adicional', 'secundario')" in sql:
            selected = [r for r in selected if r.get("draw_type") in ("adicional", "secundario")]
        if match and "ignored_count" not in sql:
            col, hours = match.group(1), params[0]
            cutoff = NOW - timedelta(hours=hours)
            selected = [r for r in selected if r.get(col) is not None and r[col] >= cutoff]
        if "ignored_count" in sql:
            col = re.search(r'\("(\w+)" IS NULL', sql).group(1)
            cutoff = NOW - timedelta(hours=params[0])
            self._result = [{"ignored_count": sum(1 for r in selected if r.get(col) is None or r[col] < cutoff)}]
        elif "COUNT(*) AS candidates_count" in sql:
            self._result = [{"candidates_count": len(selected)}]
        else:
            order = re.search(r'ORDER BY "(\w+)"', sql)
            if order:
                selected.sort(key=lambda r: (r.get(order.group(1)) is None, r.get(order.group(1))), reverse=False)
            self._result = selected

    def fetchall(self):
        return self._result

    def fetchone(self):
        return self._result[0] if self._result else {}


class SimConn:
    def __init__(self, rows):
        self.cursor_instance = SimDrawsCursor(rows)

    def cursor(self):
        return self.cursor_instance


def ctx(no_backfill=True, lookback=24):
    return {
        "scan_id": "push-scan:test",
        "config": {
            "allow_large_batch": True,
            "max_events_per_scan": 20,
            "max_events_per_key_per_scan": 20,
            "require_occurred_at": False,
            "no_backfill": no_backfill,
            "winner_lookback_hours": lookback,
            "winner_max_events_per_scan": 20,
        },
        "events_by_key": defaultdict(_empty_event_key_stats),
    }


def principal(draw_id, created_at, realized_at, **extra):
    row = {"id": draw_id, "status": "sorteado", "draw_type": "principal", "winner_number": 42,
           "winner_user_id": 7, "created_at": created_at, "realized_at": realized_at}
    row.update(extra)
    return row


def run_scan(rows, columns=None, emitter=emit_winner_defined_events, config=None):
    captured = {}

    def fake_process(_ctx, candidates, checked, group, *_a, **_k):
        captured["candidates"] = candidates
        captured["group"] = group
        return {"checked": checked, "events_candidates": len(candidates), "events_attempted": 0,
                "events_blocked": 0, "events_skipped": 0, "events_sent_to_backend": 0}

    conn = SimConn(rows)
    with patch.object(scan, "_table_columns", return_value=columns or REAL_DRAWS_COLUMNS), \
         patch.object(scan, "_process_candidates", side_effect=fake_process):
        emitter(conn, config or ctx())
    captured["sql"] = [sql for sql, _ in conn.cursor_instance.queries]
    return captured


class PrincipalWinnerTemporalTest(unittest.TestCase):
    def test_realized_at_is_the_first_temporal_option(self):
        self.assertEqual(WINNER_TEMPORAL_COLUMNS[0], "realized_at")
        self.assertEqual(WINNER_TEMPORAL_COLUMNS[1:], (
            "winner_defined_at", "drawn_at", "finished_at", "updated_at", "created_at",
        ))

    def test_old_created_at_with_recent_realized_at_is_a_candidate(self):
        rows = [principal(201, created_at=NOW - timedelta(days=40), realized_at=NOW - timedelta(hours=2))]
        captured = run_scan(rows)
        self.assertEqual([c["reference_key"] for c in captured["candidates"]], ["draw:201:winner_defined"])
        candidate = captured["candidates"][0]
        self.assertEqual(candidate["event_key"], "WINNER_DEFINED")
        self.assertEqual(candidate["reference_type"], "draw")
        self.assertEqual(candidate["occurred_at"], NOW - timedelta(hours=2))  # realized_at, nao created_at
        self.assertEqual(candidate["metadata"]["winner_number"], 42)

    def test_recent_created_at_with_old_realized_at_is_ignored_by_lookback(self):
        rows = [principal(202, created_at=NOW - timedelta(hours=1), realized_at=NOW - timedelta(hours=30))]
        captured = run_scan(rows)
        self.assertEqual(captured["candidates"], [])

    def test_realized_at_has_priority_over_created_at_in_sql(self):
        captured = run_scan([principal(203, NOW - timedelta(days=3), NOW - timedelta(hours=1))])
        where = " ".join(captured["sql"])
        self.assertIn('"realized_at" >= NOW()', where)
        self.assertNotIn('"created_at" >= NOW()', where)
        self.assertIn('ORDER BY "realized_at"', where)

    def test_only_realized_draws_in_window_are_selected_among_mixed_draws(self):
        rows = [
            principal(211, NOW - timedelta(days=90), NOW - timedelta(hours=3)),
            principal(212, NOW - timedelta(days=90), NOW - timedelta(hours=26)),   # fora do lookback
            principal(213, NOW - timedelta(days=90), None, winner_number=None, winner_user_id=None),
        ]
        captured = run_scan(rows)
        self.assertEqual([c["reference_key"] for c in captured["candidates"]], ["draw:211:winner_defined"])

    def test_missing_realized_at_still_uses_the_existing_fallbacks(self):
        base = {k: v for k, v in REAL_DRAWS_COLUMNS.items() if k != "realized_at"}
        cases = [
            (dict(base, winner_defined_at="timestamptz"), "winner_defined_at"),
            (dict(base, drawn_at="timestamptz"), "drawn_at"),
            (dict(base, updated_at="timestamptz"), "updated_at"),
            (base, "created_at"),
        ]
        for columns, expected in cases:
            with self.subTest(expected=expected):
                row = principal(220, NOW - timedelta(hours=2), None, winner_defined_at=NOW - timedelta(hours=2),
                                drawn_at=NOW - timedelta(hours=2), updated_at=NOW - timedelta(hours=2))
                captured = run_scan([row], columns=columns)
                self.assertIn(f'"{expected}" >= NOW()', " ".join(captured["sql"]))
                self.assertEqual(len(captured["candidates"]), 1)

    def test_reference_key_and_type_are_unchanged_for_dedupe(self):
        captured = run_scan([principal(230, NOW - timedelta(days=9), NOW - timedelta(hours=1))])
        candidate = captured["candidates"][0]
        self.assertEqual(candidate["reference_key"], "draw:230:winner_defined")
        self.assertEqual(candidate["reference_type"], "draw")
        self.assertEqual(captured["group"], "winner_defined")

    def test_principal_scan_never_returns_additional_draws(self):
        rows = [dict(principal(240, NOW, NOW - timedelta(hours=1)), draw_type="adicional")]
        self.assertEqual(run_scan(rows)["candidates"], [])


class PrincipalWinnerTypeFilterTest(unittest.TestCase):
    """O scan principal considera somente draw_type = principal (NULL = principal legado)."""

    def recent(self, draw_id, draw_type):
        return principal(draw_id, NOW - timedelta(days=30), NOW - timedelta(hours=2), draw_type=draw_type)

    def keys(self, rows, emitter=emit_winner_defined_events):
        return [c["reference_key"] for c in run_scan(rows, emitter=emitter)["candidates"]]

    def test_principal_enters(self):
        self.assertEqual(self.keys([self.recent(301, "principal")]), ["draw:301:winner_defined"])

    def test_null_draw_type_enters_as_legacy_principal(self):
        self.assertEqual(self.keys([self.recent(302, None)]), ["draw:302:winner_defined"])

    def test_adicional_and_secundario_do_not_enter_the_principal_scan(self):
        rows = [self.recent(303, "adicional"), self.recent(304, "secundario")]
        self.assertEqual(self.keys(rows), [])

    def test_mixed_draws_only_principal_and_legacy_are_emitted(self):
        rows = [self.recent(305, "principal"), self.recent(306, "adicional"),
                self.recent(307, None), self.recent(308, "secundario")]
        self.assertEqual(sorted(self.keys(rows)), ["draw:305:winner_defined", "draw:307:winner_defined"])

    def test_filter_is_part_of_the_executed_sql(self):
        sql = " ".join(run_scan([self.recent(309, "principal")])["sql"])
        self.assertIn("COALESCE(draw_type, 'principal') = 'principal'", sql)

    def test_additional_draws_are_handled_only_by_the_additional_scan(self):
        rows = [self.recent(310, "adicional"), self.recent(311, "secundario"), self.recent(312, "principal"),
                self.recent(313, None)]
        self.assertEqual(sorted(self.keys(rows, emitter=emit_additional_winner_defined_events)),
                         ["additional_draw:310:winner_defined", "additional_draw:311:winner_defined"])

    def test_one_additional_draw_yields_one_key_across_both_scans(self):
        rows = [self.recent(314, "adicional")]
        principal_keys = self.keys(rows)
        additional_keys = self.keys(rows, emitter=emit_additional_winner_defined_events)
        self.assertEqual(principal_keys, [])
        self.assertEqual(additional_keys, ["additional_draw:314:winner_defined"])

    def test_realized_at_priority_still_holds_with_the_type_filter(self):
        row = principal(315, NOW - timedelta(days=60), NOW - timedelta(hours=1), draw_type=None)
        captured = run_scan([row])
        self.assertIn('"realized_at" >= NOW()', " ".join(captured["sql"]))
        self.assertEqual(len(captured["candidates"]), 1)

    def test_schema_without_draw_type_column_keeps_working_without_filter(self):
        columns = {k: v for k, v in REAL_DRAWS_COLUMNS.items() if k != "draw_type"}
        row = principal(316, NOW - timedelta(days=5), NOW - timedelta(hours=1))
        row.pop("draw_type")
        captured = run_scan([row], columns=columns)
        self.assertEqual([c["reference_key"] for c in captured["candidates"]], ["draw:316:winner_defined"])
        self.assertNotIn("COALESCE(draw_type", " ".join(captured["sql"]))


class AdditionalWinnerTemporalUnchangedTest(unittest.TestCase):
    def test_additional_temporal_columns_are_unchanged(self):
        self.assertEqual(ADDITIONAL_WINNER_TEMPORAL_COLUMNS,
                         ("realized_at", "winner_defined_at", "updated_at", "created_at"))

    def test_additional_still_uses_realized_at_and_its_own_reference_key(self):
        row = {"id": 250, "draw_type": "adicional", "winner_number": 5, "winner_user_id": 9,
               "realized_at": NOW - timedelta(hours=2), "created_at": NOW - timedelta(days=30),
               "product_name": "Adicional"}
        captured = run_scan([row], emitter=emit_additional_winner_defined_events)
        candidate = captured["candidates"][0]
        self.assertEqual(candidate["reference_key"], "additional_draw:250:winner_defined")
        self.assertEqual(candidate["reference_type"], "additional_draw")
        self.assertIn('"realized_at" >= NOW()', " ".join(captured["sql"]))

    def test_additional_outside_lookback_is_still_ignored(self):
        row = {"id": 251, "draw_type": "adicional", "winner_number": 5, "winner_user_id": 9,
               "realized_at": NOW - timedelta(hours=40), "created_at": NOW - timedelta(hours=1)}
        self.assertEqual(run_scan([row], emitter=emit_additional_winner_defined_events)["candidates"], [])


if __name__ == "__main__":
    unittest.main()
