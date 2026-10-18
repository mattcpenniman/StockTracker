"""Validate load accounting and application-level failures, not just HTTP status."""
import io
from email.message import Message
import json
import time
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

from analytics.portfolio_load import request, run_stage, summarize


class PortfolioLoadTests(unittest.TestCase):
    def response(self, payload):
        class Response(io.BytesIO):
            status = 200
            headers = {"X-Worker-Pid": "123"}
        return Response(json.dumps(payload).encode())

    def test_partial_http_200_is_failure(self):
        with patch("analytics.portfolio_load.urlopen", return_value=self.response({
            "ok": True, "count": 1, "portfolio_symbol_count": 2,
            "states": [{}], "errors": [{"symbol": "MISSING"}],
        })):
            record = request("http://localhost", "state", "2026-03-17", 20, 1)
        self.assertFalse(record["valid"])
        self.assertEqual(record["status"], 200)
        self.assertEqual(summarize([record])["failed"], 1)

    def test_quality_observations_are_reported_separately(self):
        with patch("analytics.portfolio_load.urlopen", return_value=self.response({
            "ok": True, "count": 1, "portfolio_symbol_count": 1,
            "future_states": [{}], "errors": [], "benchmark_errors": [{"symbol": "SPY"}],
            "data_quality": {"stale_symbols": ["A"], "incomplete_window_symbols": ["A"]},
        })):
            record = request("http://localhost", "futurestate", "2026-03-17", 20, 1)
        self.assertTrue(record["valid"])
        self.assertEqual(record["worker_pid"], "123")
        summary = summarize([record])
        self.assertEqual(summary["benchmark_error_observations"], 1)
        self.assertEqual(summary["stale_symbol_observations"], 1)
        self.assertEqual(summary["incomplete_window_observations"], 1)

    def test_http_and_invalid_json_errors_are_retained(self):
        for failure in (HTTPError("http://localhost", 500, "failed", Message(), None), TimeoutError("timed out")):
            with self.subTest(failure=failure), patch("analytics.portfolio_load.urlopen", side_effect=failure):
                record = request("http://localhost", "state", "2026-03-17", 20, 1)
            self.assertFalse(record["valid"])
            self.assertIn("error", record)
        response = self.response({})
        response.write(b"not JSON")
        response.seek(0)
        with patch("analytics.portfolio_load.urlopen", return_value=response):
            record = request("http://localhost", "state", "2026-03-17", 20, 1)
        self.assertFalse(record["valid"])
        self.assertIn("JSONDecodeError", record["error"])

    def test_overload_is_bounded_and_every_arrival_accounted_for(self):
        def slow_request(base, endpoint, asof, days, timeout):
            time.sleep(.04)
            return {"endpoint": endpoint, "date": asof, "status": 200, "valid": True, "latency_ms": 40}

        with patch("analytics.portfolio_load.request", side_effect=slow_request):
            stage = run_stage("http://localhost", [("state", "2026-03-17", 20)], 1000, .08, 2, 1)
        self.assertEqual(stage["offered_requests"], 80)
        self.assertGreater(stage["dropped_capacity"] + stage["dropped_scheduler"], 0)
        self.assertEqual(stage["offered_requests"], stage["summary"]["requests"]
                         + stage["dropped_capacity"] + stage["dropped_scheduler"])
        self.assertLessEqual(stage["summary"]["requests"], 6)


if __name__ == "__main__":
    unittest.main()
