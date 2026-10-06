import io
import unittest
from contextlib import redirect_stdout

import main
import test_main_results as base
from test_main_results import VALID_GRID, CursorConnection, SequenceCursor, draw


def grid(total, low, high):
    return {"total": total, "min": low, "max": high}


class ResultGridTests(unittest.TestCase):
    def scenario(self, draws, grids=None, winners=None):
        winners = winners or {item["id"]: (7, "Cliente", "c@example.com") for item in draws}
        output = io.StringIO()
        with redirect_stdout(output):
            outcome = base.ResultProcessingTests("run_scenario").run_scenario(
                draws, winners=winners, grids=grids
            )
        outcome["log"] = output.getvalue()
        return outcome

    def test_grid_00_99_is_processed_normally(self):
        outcome = self.scenario([draw(133)])
        self.assertEqual(outcome["result_code"], 0)
        self.assertEqual(outcome["update_calls"], [(133, 33, 7, "Cliente")])
        self.assertNotIn("unsupported_result_grid", outcome["log"])

    def test_unsupported_grids_are_refused_without_side_effects(self):
        cases = {
            "0-499": grid(500, 0, 499),
            "0-999": grid(1000, 0, 999),
            "100 rows with inconsistent range": grid(100, 1, 100),
            "100 rows starting at 0 but ending at 120": grid(100, 0, 120),
            "empty grid": grid(0, None, None),
            "99 rows": grid(99, 0, 98),
        }
        for name, bad_grid in cases.items():
            with self.subTest(name=name):
                outcome = self.scenario([draw(133)], grids={133: bad_grid})
                self.assertEqual(outcome["result_code"], 1)
                self.assertEqual(outcome["winner_calls"], [])
                self.assertEqual(outcome["update_calls"], [])
                self.assertEqual(outcome["conn"].commit_count, 0)
                outcome["events"].assert_not_called()
                self.assertIn("unsupported_result_grid", outcome["log"])

    def test_log_reports_grid_details(self):
        outcome = self.scenario([draw(133)], grids={133: grid(500, 0, 499)})
        line = next(l for l in outcome["log"].splitlines() if "unsupported_result_grid" in l and "numbers_total" in l)
        for fragment in ("'draw_id': 133", "'numbers_total': 500", "'numbers_min': 0", "'numbers_max': 499"):
            self.assertIn(fragment, line)

    def test_invalid_grid_does_not_block_valid_draw(self):
        outcome = self.scenario(
            [draw(133), draw(134)],
            grids={133: grid(500, 0, 499), 134: VALID_GRID},
        )
        self.assertEqual(outcome["result_code"], 1)
        self.assertEqual(outcome["update_calls"], [(134, 33, 7, "Cliente")])
        self.assertEqual(outcome["winner_calls"], [(134, 33)])

    def test_valid_draw_first_then_invalid_still_returns_1(self):
        outcome = self.scenario(
            [draw(133), draw(134)],
            grids={133: VALID_GRID, 134: grid(1000, 0, 999)},
        )
        self.assertEqual(outcome["result_code"], 1)
        self.assertEqual([c[0] for c in outcome["update_calls"]], [133])

    def test_grid_lookup_reads_count_min_max_for_the_draw(self):
        cursor = SequenceCursor([{"total": 100, "min_n": 0, "max_n": 99}])
        conn = CursorConnection(cursor)
        self.assertEqual(main.get_draw_number_grid(conn, 133), VALID_GRID)
        sql, params = cursor.executions[0]
        self.assertIn("COUNT(*)", sql)
        self.assertIn("MIN(n)", sql)
        self.assertIn("MAX(n)", sql)
        self.assertIn("public.numbers", sql)
        self.assertEqual(params, (133,))

    def test_is_supported_requires_exact_100_numbers_from_0_to_99(self):
        self.assertTrue(main.is_supported_result_grid(VALID_GRID))
        for bad in (grid(500, 0, 499), grid(100, 1, 100), grid(100, 0, 100), grid(101, 0, 99), grid(None, None, None)):
            self.assertFalse(main.is_supported_result_grid(bad))


if __name__ == "__main__":
    unittest.main()
