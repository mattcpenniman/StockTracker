"""Independent arithmetic oracles, not merely Python/native parity."""
import os
import unittest
from unittest.mock import patch

import numpy as np
import pandas as pd

from analytics import indicators
from analytics.config import AnalyticsSettings
from analytics.service import AnalyticsService
from analytics.utils import clean_json_value, parse_asof
from tests.test_historical import HistoryRepository
from tests.test_state_snapshot import typed, TypedHistoryRepository


def bars(dates, closes):
    return pd.DataFrame(dict(timestamp=pd.to_datetime(dates, utc=True), open=closes,
                             high=closes, low=closes, close=closes, volume=1000))


class YtdTests(unittest.TestCase):
    @property
    def backends(self):
        return ["python"] + (["rust"] if indicators._rust_feature_columns else [])

    def test_hand_calculated_boundaries_full_frame_and_every_snapshot(self):
        fixtures = [
            # Dec 31, 2021 was open; Dec 31, 2022/2023 fell on weekends.
            # 110/100 - 1 = 10%; 99/110 - 1 = -10%; 100/125 - 1 = -20%.
            (["2021-12-31", "2022-01-03", "2022-12-30", "2023-01-03",
              "2023-12-29", "2024-01-02", "2024-12-31", "2025-01-02"],
             [100., 110., 110., 99., 125., 100., 200., 250.],
             [None, .10, .10, -.10, 125 / 110 - 1, -.20, .60, .25]),
            # Dec 31, 2010 was open despite the Saturday New Year holiday.
            (["2010-12-31", "2011-01-03"], [80., 60.], [None, -.25]),
            # Friday Jan 1 was a holiday; Monday is the first 2021 session.
            (["2020-12-31", "2021-01-04"], [80., 100.], [None, .25]),
            # IPO / truncated history: no first-in-year substitute.
            (["2024-06-03", "2024-06-04"], [100., 120.], [None, None]),
            # Prior-year observations exist, but the final session is absent.
            (["2023-12-28", "2024-01-02"], [100., 120.], [None, None]),
            # Never bridge a whole missing calendar year.
            (["2022-12-30", "2024-01-02"], [100., 120.], [None, None]),
            # Consistently split-adjusted history across the boundary:
            # old raw close 200 / 2 = 100, new raw close 110: +10%, not -45%.
            (["2023-12-29", "2024-01-02"], [100., 110.], [None, .10]),
            # Same economic prices on a 2x scale, same price return.
            (["2023-12-29", "2024-01-02"], [200., 220.], [None, .10]),
        ]
        for backend in self.backends:
            with patch.dict(os.environ, ANALYTICS_BACKEND=backend):
                for dates, closes, expected in fixtures:
                    frame = bars(dates, closes)
                    with self.subTest(backend=backend, dates=dates, closes=closes):
                        full = indicators.compute_feature_frame(frame, "1Day", AnalyticsSettings())
                        for i, value in enumerate(expected):
                            actual = clean_json_value(full.iloc[i].ytd_return)
                            if value is None:
                                self.assertIsNone(actual)
                            else:
                                self.assertAlmostEqual(actual, value, places=12)
                            prefix = frame.iloc[:i + 1]
                            for source in (prefix, typed(prefix)):
                                snapshot = indicators.compute_feature_snapshot(source, "1Day", AnalyticsSettings())
                                self.assertEqual(clean_json_value(snapshot["ytd_return"]), actual)

    def test_historical_single_and_typed_state_use_daily_oracle_and_quality(self):
        dates = ["2023-12-29", "2024-01-02", "2024-12-31", "2025-01-02"]
        expected = [None, .10, .50, -.20]
        for repo_class in (HistoryRepository, TypedHistoryRepository):
            repo = repo_class()
            for sid in range(4):
                repo.frames[sid, "1Day"] = bars(dates, [100., 110., 150., 120.])
                repo.frames[sid, "15Min"] = bars(dates, [900., 900., 900., 900.])
            for backend in self.backends:
                with self.subTest(repo=repo_class, backend=backend), patch.dict(os.environ, ANALYTICS_BACKEND=backend):
                    service = AnalyticsService(repo)
                    cutoffs = [(pd.Timestamp(d) + pd.Timedelta(days=1)).strftime("%Y-%m-%dT05:00:00Z") for d in dates]
                    history = service.get_historical_portfolio(["A"], cutoffs, AnalyticsSettings(), 1)
                    for date, row, value in zip(cutoffs, history["dates"], expected):
                        state = row["features"]["states"][0]
                        single = service.get_batch_state(["A"], "1Day", parse_asof(date), AnalyticsSettings())["states"][0]
                        self.assertEqual(state, single)
                        self.assertEqual(state["price"]["close"], 900.)
                        if value is None:
                            self.assertIsNone(state["returns"]["ytd"])
                            self.assertEqual(state["data_quality"]["ytd"]["reason"], "missing_previous_year_end_close")
                        else:
                            self.assertAlmostEqual(state["returns"]["ytd"], value)
                            self.assertEqual(state["data_quality"]["ytd"]["status"], "qualified")

    @unittest.skipUnless(indicators._rust_feature_columns, "native extension")
    def test_native_kernel_without_calendar_flags_fails_closed(self):
        frame = bars(["2023-12-29", "2024-01-02"], [100., 110.])
        arrays = typed(frame).native_inputs()
        result = indicators._rust_feature_columns(*arrays, 252 ** .5, 20, 5, .005)
        self.assertTrue(np.isnan(result["ytd_return"]).all())

    def test_old_extension_cannot_silently_return_old_ytd_formula(self):
        frame = bars(["2023-12-29", "2024-01-02"], [100., 110.])
        def old(*args, **kwargs):
            raise TypeError("unexpected keyword argument 'year_end'")
        with patch.dict(os.environ, ANALYTICS_BACKEND="auto"), patch.object(indicators, "_rust_feature_columns", old):
            self.assertAlmostEqual(indicators.compute_feature_frame(frame, "1Day", AnalyticsSettings()).iloc[-1].ytd_return, .1)
            self.assertAlmostEqual(indicators.compute_feature_snapshot(typed(frame), "1Day", AnalyticsSettings())["ytd_return"], .1)
