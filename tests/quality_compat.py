"""Project older performance baselines onto fields whose contract is unchanged.

VEC-827 quality guarantees are independently tested in test_data_quality.py.
"""


def legacy_quality_contract(value):
    added_or_redefined = {
        "metric_history", "history_basis", "ohlcv_validation", "session_coverage",
        "corporate_action_consistency", "completeness_verified", "historical_coverage",
        "session_warning_symbols", "corporate_action_warning_symbols",
        "stale_symbols",
    }
    if isinstance(value, dict):
        return {key: legacy_quality_contract(item) for key, item in value.items() if key not in added_or_redefined}
    if isinstance(value, list):
        return [legacy_quality_contract(item) for item in value]
    return value
