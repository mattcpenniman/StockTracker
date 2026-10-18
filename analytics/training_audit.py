"""Historical feature/label audit with separate explicit bounded preparation.

The default audit is read-only and performs no provider I/O or preparation.
The report is a readiness screen, not certification of interior/session coverage.
Preserves raw responses for reproducing every classification.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen


def request_json(base_url, path, payload=None):
    data = None if payload is None else json.dumps(payload).encode()
    request = Request(base_url.rstrip('/') + path, data=data,
                      headers={'Content-Type': 'application/json'})
    with urlopen(request, timeout=600) as response:
        return json.load(response)


def classify(state, label):
    """Fail closed on absent quality fields, errors and partial label windows."""
    reasons = []
    quality = state.get('data_quality', {}) if state else {}
    if quality.get('has_sufficient_history_200') is not True:
        reasons.append('indicators:insufficient_200_bar_history')
    coverage = quality.get('historical_coverage', {})
    requested = state.get('requested_asof') if state else None
    if requested:
        cutoff = datetime.fromisoformat(requested.replace('Z', '+00:00'))
        expected = '1Day' if cutoff.time().isoformat() == '00:00:00' else '15Min'
        if quality.get('price_source_timeframe') != expected:
            reasons.append('price:source_fallback_or_missing')
    for component in ('indicators', 'price'):
        item = coverage.get(component, {})
        if item.get('status') != 'within_tolerance':
            reasons.append(component + ':' + item.get('status', 'missing'))
    benchmarks = quality.get('benchmark_coverage', {})
    for symbol in ('SPY', 'QQQ'):
        item = benchmarks.get(symbol, {})
        if not item.get('aligned') or item.get('historical_coverage', {}).get('status') != 'within_tolerance':
            reasons.append('benchmark:' + symbol)
    if not label:
        reasons.append('label:missing')
    else:
        lq = label.get('data_quality', {})
        if lq.get('historical_coverage', {}).get('status') != 'within_tolerance':
            reasons.append('label:anchor_age')
        if label.get('window', {}).get('complete_window') is not True:
            reasons.append('label:partial_horizon')
        if quality.get('indicator_bar_timestamp') != label.get('anchor_price', {}).get('timestamp'):
            reasons.append('label:indicator_anchor_mismatch')
    return reasons


def matrix(state_response, label_response, requested):
    states = {item['symbol']: item for item in state_response.get('states', [])}
    labels = {item['symbol']: item for item in label_response.get('future_states', [])}
    symbols = sorted(set(states) | set(labels) |
                     {item['symbol'] for response in (state_response, label_response)
                      for item in response.get('errors', [])})
    rows = []
    for symbol in symbols:
        state, label = states.get(symbol), labels.get(symbol)
        quality = state.get('data_quality', {}) if state else {}
        reasons = classify(state, label)
        rows.append(dict(symbol=symbol, requested=requested,
                         indicator_timestamp=quality.get('indicator_bar_timestamp'),
                         indicator_source=quality.get('indicator_source_timeframe'),
                         indicator_bar_count=quality.get('bar_count'),
                         price_timestamp=quality.get('price_bar_timestamp'),
                         price_source=quality.get('price_source_timeframe'),
                         coverage=quality.get('historical_coverage'),
                         benchmarks=quality.get('benchmark_coverage'),
                         label_anchor=label.get('anchor_price') if label else None,
                         label_window=label.get('window') if label else None,
                         readiness_issues=reasons, screen_pass=not reasons))
    return rows


def render_comparison(before, after, receipts):
    """Portable Vector evidence: all symbol/cutoff anchors plus preparation receipts."""
    if len(before['cases']) != len(after['cases']):
        raise ValueError('Before/after reports must contain the same cutoffs')
    lines = ['# VEC-812 historical coverage audit', '',
             '## Environment', '',
             f"Observed: {before['observed_at']} → {after['observed_at']}",
             f"Endpoint: {after['base_url']}",
             '```json', json.dumps(after['health']['runtime']['build'], indent=2), '```', '',
             '## Interpretation', '',
             'Pass means the conservative API coverage screen passed: at least 200 daily bars, '
             'recent indicator/price anchors, aligned recent SPY/QQQ, matching daily label anchor '
             'and complete future horizon. This does not certify interior/session completeness. '
             'Market closures do not require exact-calendar-date bars. Source timeframes and '
             'timestamps must be retained. Midnight selects daily prices; other cutoffs prefer '
             '15-minute prices. A stored stale intraday bar is retained by that policy even when '
             'daily history exists. No source-selection change was made.', '',
             'DOC-228/source-34d31ae remains the original performance baseline. These are '
             'post-preparation quality observations on a later deployed revision, not replacement timings.', '',
             '## Complete before/after matrix', '',
             'D=daily indicator timestamp; P=price timestamp/source; B=SPY/QQQ daily timestamps; '
             'L=label anchor and realized/requested future bars. All timestamps UTC.']
    for old, new in zip(before['cases'], after['cases']):
        if old['summary']['requested'] != new['summary']['requested']:
            raise ValueError('Before/after cutoff order must match')
        requested = new['summary']['requested']
        old_rows = {r['symbol']: r for r in matrix(old['state'], old['labels'], requested)}
        if set(old_rows) != {row['symbol'] for row in new['matrix']}:
            raise ValueError('Before/after symbol membership differs; compare audit reports directly')
        lines += ['', '### ' + requested, '',
                  '```json', json.dumps(new['summary'], indent=2), '```', '',
                  '| Symbol | D before → after | P before → after (source) | B before → after (SPY; QQQ) | L after; bars | Issues before → after |',
                  '|---|---|---|---|---|---|']
        for row in new['matrix']:
            prior = old_rows[row['symbol']]
            def benchmarks(item):
                return '; '.join(str((item.get('benchmarks') or {}).get(b, {}).get('indicator_bar_timestamp'))
                                 for b in ('SPY', 'QQQ'))
            window = row['label_window'] or {}
            lines.append('| ' + ' | '.join([
                row['symbol'], f"{prior['indicator_timestamp']} → {row['indicator_timestamp']}",
                f"{prior['price_timestamp']} ({prior['price_source']}) → {row['price_timestamp']} ({row['price_source']})",
                benchmarks(prior) + ' → ' + benchmarks(row),
                f"{(row['label_anchor'] or {}).get('timestamp')}; {window.get('future_bar_count')}/{window.get('requested_future_bar_count')}",
                ', '.join(prior['readiness_issues']) + ' → ' + (', '.join(row['readiness_issues']) or 'pass')]) + ' |')
    lines += ['', '## Explicit preparation receipts', '', '```json',
              json.dumps(receipts, indent=2), '```', '']
    return '\n'.join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base-url', default='http://127.0.0.1:5005')
    parser.add_argument('--dates', default='2026-03-17,2026-06-16,2026-09-16')
    parser.add_argument('--variants', default='date,midnight,close',
                        help='date=end-of-day UTC; midnight=00:00Z; close=20:00Z (explicit)')
    parser.add_argument('--days', type=int, default=20)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--prepare-from', type=Path,
                        help='Explicitly prepare stale anchors from a prior audit; writes operation receipts instead of auditing')
    parser.add_argument('--prepare-plan', type=Path,
                        help='Explicit JSON array of bounded coverage/prepare request bodies')
    parser.add_argument('--render-from', type=Path, help='Render an existing after report as Vector-ready Markdown')
    parser.add_argument('--before-file', type=Path)
    parser.add_argument('--receipts', type=Path, nargs='*', default=[])
    args = parser.parse_args()
    if args.render_from:
        if not args.before_file:
            parser.error('--render-from requires --before-file')
        receipts = {path.name: json.loads(path.read_text()) for path in args.receipts}
        args.output.write_text(render_comparison(json.loads(args.before_file.read_text()),
                                                json.loads(args.render_from.read_text()), receipts))
        return
    if args.prepare_from and args.prepare_plan:
        parser.error('Choose one preparation input')
    if args.prepare_plan:
        receipts = []
        for payload in json.loads(args.prepare_plan.read_text()):
            response = request_json(args.base_url, '/api/coverage/prepare', payload)
            receipts.append(dict(request=payload, response=response))
            args.output.write_text(json.dumps(receipts, indent=2) + '\n')
            print(json.dumps(receipts[-1]), flush=True)
        return
    if args.prepare_from:
        from datetime import timedelta
        previous = json.loads(args.prepare_from.read_text())
        windows = {}
        for case in previous['cases']:
            cutoff = datetime.fromisoformat(case['summary']['requested'][:10]).replace(tzinfo=timezone.utc)
            for row in case['matrix']:
                if 'price:stale_trailing' in row['readiness_issues']:
                    windows.setdefault(('15Min', cutoff), set()).add(row['symbol'])
                for benchmark, quality in (row.get('benchmarks') or {}).items():
                    if quality.get('historical_coverage', {}).get('stale'):
                        windows.setdefault(('1Day', cutoff), set()).add(benchmark)
        receipts = []
        for (timeframe, cutoff), symbols in windows.items():
            symbols = sorted(symbols)
            for offset in range(0, len(symbols), 5):
                payload = dict(symbols=symbols[offset:offset+5], timeframe=timeframe,
                               include_benchmarks=False,
                               start=(cutoff - timedelta(days=7)).isoformat(),
                               end=(cutoff + timedelta(days=1)).isoformat())
                response = request_json(args.base_url, '/api/coverage/prepare', payload)
                receipts.append(dict(request=payload, response=response))
                # Persist each completed operation even if a later provider call fails.
                args.output.write_text(json.dumps(receipts, indent=2) + '\n')
                print(json.dumps(receipts[-1]), flush=True)
        return
    report = {'observed_at': datetime.now(timezone.utc).isoformat(),
              'base_url': args.base_url, 'health': request_json(args.base_url, '/api/health/analytics'),
              'completeness_verified': False, 'cases': []}
    for date in args.dates.split(','):
        for variant in args.variants.split(','):
            requested = {'date': date, 'midnight': date + 'T00:00:00Z',
                         'close': date + 'T20:00:00Z'}[variant]
            query = urlencode(dict(timeframe='1D', asof=requested))
            state = request_json(args.base_url, '/api/portfolio/state?' + query)
            label = request_json(args.base_url, '/api/portfolio/futurestate?' + query + f'&days={args.days}')
            rows = matrix(state, label, requested)
            summary = dict(requested=requested, portfolio_count=state.get('portfolio_symbol_count'),
                           state_count=state.get('count'), label_count=label.get('count'),
                           screen_pass_count=sum(row['screen_pass'] for row in rows),
                           issues=dict(Counter(issue for row in rows for issue in row['readiness_issues'])),
                           state_errors=state.get('errors'), label_errors=label.get('errors'),
                           benchmark_errors=state.get('benchmark_errors'))
            summary['response_complete'] = bool(
                state.get('ok') and label.get('ok') and state.get('portfolio_symbol_count', 0) > 0
                and state.get('count') == label.get('count') == len(rows) == state.get('portfolio_symbol_count')
                and not state.get('errors') and not label.get('errors'))
            summary['screen_pass'] = summary['response_complete'] and all(row['screen_pass'] for row in rows)
            report['cases'].append(dict(summary=summary, matrix=rows, state=state, labels=label))
            print(json.dumps(summary), flush=True)
    args.output.write_text(json.dumps(report, indent=2) + '\n')


if __name__ == '__main__':
    main()
