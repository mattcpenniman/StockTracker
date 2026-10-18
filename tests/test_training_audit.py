import copy
import unittest

from analytics.training_audit import classify, matrix, render_comparison
from analytics.coverage import historical_coverage


class TrainingAuditTests(unittest.TestCase):
    def sample(self):
        # A Monday midnight cutoff legitimately uses Friday daily bars.
        stamp = '2026-03-13T04:00:00Z'
        coverage = historical_coverage('2026-03-16T00:00:00Z', stamp)
        state = {'symbol': 'TEST', 'data_quality': {
            'has_sufficient_history_200': True,
            'indicator_bar_timestamp': stamp,
            'historical_coverage': {'indicators': copy.deepcopy(coverage), 'price': copy.deepcopy(coverage)},
            'benchmark_coverage': {b: {'aligned': True, 'historical_coverage': copy.deepcopy(coverage)}
                                   for b in ('SPY', 'QQQ')}}}
        label = {'symbol': 'TEST', 'data_quality': {'historical_coverage': coverage},
                 'anchor_price': {'timestamp': stamp}, 'window': {'complete_window': True}}
        return state, label

    def test_covered_history_and_market_closure(self):
        state, label = self.sample()
        self.assertEqual(classify(state, label), [])

    def test_stale_intraday_does_not_invalidate_daily_history(self):
        state, label = self.sample()
        state['data_quality']['historical_coverage']['price']['status'] = 'stale_trailing'
        self.assertEqual(classify(state, label), ['price:stale_trailing'])

    def test_missing_intraday_quality_fails_closed(self):
        state, label = self.sample()
        del state['data_quality']['historical_coverage']['price']
        self.assertEqual(classify(state, label), ['price:missing'])

    def test_benchmark_misalignment(self):
        state, label = self.sample()
        state['data_quality']['benchmark_coverage']['SPY']['aligned'] = False
        self.assertEqual(classify(state, label), ['benchmark:SPY'])

    def test_daily_fallback_is_visible_for_timed_snapshot(self):
        state, label = self.sample()
        state['requested_asof'] = '2026-03-16T20:00:00Z'
        state['data_quality']['price_source_timeframe'] = '1Day'
        self.assertEqual(classify(state, label), ['price:source_fallback_or_missing'])
        state['requested_asof'] = '2026-03-16T00:00:00Z'
        self.assertEqual(classify(state, label), [])

    def test_partial_label_horizon_and_mismatched_anchor(self):
        state, label = self.sample()
        label['window']['complete_window'] = False
        label['anchor_price']['timestamp'] = '2026-03-12T04:00:00Z'
        self.assertEqual(classify(state, label), ['label:partial_horizon', 'label:indicator_anchor_mismatch'])

    def test_error_only_symbol_is_retained(self):
        rows = matrix({'errors': [{'symbol': 'BAD', 'error': 'no bars'}]}, {}, '2026-03-17')
        self.assertEqual(rows[0]['symbol'], 'BAD')
        self.assertFalse(rows[0]['screen_pass'])

    def test_missing_and_short_history_cannot_pass(self):
        state, label = self.sample()
        state['data_quality']['has_sufficient_history_200'] = False
        self.assertIn('indicators:insufficient_200_bar_history', classify(state, label))
        self.assertIn('label:missing', classify(state, None))

    def test_comparison_cannot_silently_truncate_cutoffs(self):
        with self.assertRaisesRegex(ValueError, 'same cutoffs'):
            render_comparison({'cases': []}, {'cases': [{}]}, {})


if __name__ == '__main__':
    unittest.main()
