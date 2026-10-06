import unittest
from unittest.mock import Mock, patch

import requests

import main
from test_main_results import FakeConnection, draw, lotomania


def payload():
    return {
        "numero": 2982,
        "numeroConcursoAnterior": 2981,
        "dataApuracao": "30/09/2026",
        "dezenasSorteadasOrdemSorteio": [
            "33", "57", "77", "72", "27", "32", "61", "13", "91", "07",
            "95", "02", "64", "59", "03", "80", "08", "04", "98", "16",
        ],
    }


def response(status=200):
    result = requests.Response()
    result.status_code = status
    result.url = main.LOT_ENDPOINT
    import json
    result._content = json.dumps(payload()).encode()
    return result


class ResultReliabilityTests(unittest.TestCase):
    def test_complete_caixa_result_keeps_last_drawn_number(self):
        self.assertEqual(main._parse_lotomania_payload(payload())["winner_number"], 16)

    def test_partial_or_duplicate_draw_order_is_not_a_valid_result(self):
        for numbers in (["16"], ["16"] * 20, list(map(str, range(21)))):
            with self.subTest(numbers=numbers):
                data = payload()
                data["dezenasSorteadasOrdemSorteio"] = numbers
                with self.assertRaises(RuntimeError):
                    main._parse_lotomania_payload(data)

    def test_retry_recovers_from_timeout_without_changing_contest(self):
        with patch.object(main.requests, "get", side_effect=[requests.Timeout(), response()]) as get, \
             patch("time.sleep"):
            result = main.get_lotomania_result(2982)
        self.assertEqual(result["winner_number"], 16)
        self.assertEqual([call.args[0] for call in get.call_args_list], [
            f"{main.LOT_ENDPOINT}/2982", f"{main.LOT_ENDPOINT}/2982",
        ])

    def test_retry_recovers_from_temporary_http_error(self):
        with patch.object(main.requests, "get", side_effect=[response(503), response()]), \
             patch("time.sleep"):
            self.assertEqual(main.get_lotomania_result()["contest_number"], 2982)

    def test_retry_is_bounded_and_does_not_hide_outage(self):
        with patch.object(main.requests, "get", side_effect=requests.Timeout()) as get, \
             patch("time.sleep"):
            with self.assertRaises(requests.Timeout):
                main.get_lotomania_result()
        self.assertEqual(get.call_count, 3)

    def test_http_404_is_not_retried(self):
        with patch.object(main.requests, "get", return_value=response(404)) as get, \
             patch("time.sleep"):
            with self.assertRaises(requests.HTTPError):
                main.get_lotomania_result()
        self.assertEqual(get.call_count, 1)

    def test_draw_failure_makes_job_fail_but_next_draw_is_processed(self):
        conn = FakeConnection()
        with patch.object(main, "db", return_value=conn), \
             patch.object(main, "get_pending_draws", return_value=[draw(133), draw(134)]), \
             patch.object(main, "get_last_lotomania_result", return_value=lotomania()), \
             patch.object(main, "resolve_first_eligible_lotomania_result", side_effect=lambda *_a, **_k: lotomania()), \
             patch.object(main, "_process_pending_draw", side_effect=[RuntimeError("query failed"), True]) as process, \
             patch.object(main, "_run_push_automation_scan_safely"):
            code = main.run()
        self.assertEqual(process.call_count, 2)
        self.assertTrue(conn.closed)
        self.assertEqual(code, 1)


if __name__ == "__main__":
    unittest.main()
