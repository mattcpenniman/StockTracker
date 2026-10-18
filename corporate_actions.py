"""Reviewed unit conversions, never factors inferred from investment returns.

The registry is deliberately small. Provider coherence is not certification of
all economic actions. New actions require an issuer source and basis validation.
"""
from datetime import datetime, timezone
from math import isclose
import hashlib
import json

POLICY_REVISION = "issuer-units-v1"
AZN_ACTION = {
    "id": "AZN-2026-02-02-ADS-ordinary",
    "effective_at": "2026-02-02T05:00:00+00:00",
    "kind": "ads_conversion",
    "old_units_per_new_unit": 2,
    "price_factor": 2,
    "volume_factor": 0.5,
    "factor_verified": True,
    "sources": [
        "https://www.astrazeneca.com/content/dam/az/PDF/2025/Circular-and-Notice-of-General-Meeting.pdf",
        "https://www.astrazeneca.com/investor-relations/faqs.html",
    ],
    "source_detail": "Circular dated 29 September 2025, Part III.1 (p14): ADSs on a two for one basis; FAQ confirms implementation 2 February 2026.",
}
SNX_ACTION = {
    "id": "SNX-2020-12-01-Concentrix",
    "effective_at": "2020-12-01T05:00:00+00:00",
    "kind": "distribution_review",
    "status": "unresolved",
    "distribution_verified": True,
    "distributed_symbol": "CNXC",
    "distributed_shares_per_parent_share": 1,
    "sources": ["https://www.annualreports.com/HostedData/AnnualReportArchive/s/NYSE_SNX_2020.pdf"],
    "source_detail": "SYNNEX FY2020 Form 10-K, Item 1, printed p1: separation completed December 1, 2020, one Concentrix share for every SYNNEX share.",
    "reason": "Concentrix spin-off: parent-only price return excludes distributed shares; no certified price factor or total-return reconstruction.",
}


def timestamp(bar):
    return datetime.fromisoformat(str(bar["t"]).replace("Z", "+00:00")).astimezone(timezone.utc)


def action_manifest(symbol):
    return {"policy_revision": POLICY_REVISION, "price_basis": "current_share_units",
            "return_semantics": "price_return_without_distribution_reinvestment",
            "reviewed_actions": [], "unresolved_actions": [dict(SNX_ACTION)] if symbol == "SNX" else [],
            "all_economic_actions_verified": False}


def normalize_actions(symbol, timeframe, bars, mode, *, fetch_raw, start, end):
    """Compare configured provider observations to raw, then convert exactly once.

    Both raw and split are unit-normalized; split retains provider split handling.
    Dividend-adjusted modes are rejected for AZN until independently reconciled.
    Raw comparisons cover every pre-action row, not just a boundary price jump.
    An unexpected provider factor fails the whole staged publication.
    """
    if symbol != "AZN":
        return bars, None
    if mode not in ("raw", "split"):
        raise ValueError("AZN conversion supports raw/split price-return modes only; dividend/all basis is unresolved.")
    boundary = datetime.fromisoformat(AZN_ACTION["effective_at"])
    before = [bar for bar in bars if timestamp(bar) < boundary]
    evidence = {**AZN_ACTION, "timeframe": timeframe, "status": "normalized",
                "pre_action_bar_count": len(before), "provider_price_factors": [],
                "volume_rounding": "nearest_integer_half_up", "trade_count_basis": "unchanged"}
    if not before:
        return bars, evidence
    raw_bars = bars if mode == "raw" else fetch_raw(symbol, timeframe, start, end)
    evidence["raw_reference_sha256"] = hashlib.sha256(json.dumps(
        sorted(raw_bars, key=timestamp), sort_keys=True, default=str).encode()).hexdigest()
    raw = {timestamp(bar): bar for bar in raw_bars}
    if len(raw) != len(raw_bars):
        raise ValueError("Duplicate raw observations in AZN conversion reference.")
    converted, factors = [], set()
    for bar in bars:
        if timestamp(bar) >= boundary:
            converted.append(dict(bar))
            continue
        reference = raw.get(timestamp(bar))
        if reference is None:
            raise ValueError("Missing raw observation in AZN conversion reference.")
        # Only the issuer-certified 2:1 conversion or unchanged provider units
        # are accepted. This is a same-timestamp basis comparison, not a return.
        keys = ["o", "h", "l", "c"]
        if bar.get("vw") is not None and reference.get("vw") is not None:
            keys.append("vw")
        candidates = [factor for factor in (1, 2) if all(
            isclose(float(bar[key]), float(reference[key]) * factor, rel_tol=1e-8, abs_tol=1e-4)
            for key in keys) and
            abs(float(bar["v"]) - float(reference["v"]) / factor) <= 0.5]
        if not candidates:
            raise ValueError(f"Unresolved AZN provider adjustment at {bar['t']}; expected certified ADS units or ordinary units.")
        provider_factor = candidates[0]
        factors.add(provider_factor)
        remaining = 2 / provider_factor
        adjusted = dict(bar)
        for key in ("o", "h", "l", "c", "vw"):
            if adjusted.get(key) is not None:
                adjusted[key] = float(adjusted[key]) * remaining
        adjusted["v"] = int(float(adjusted["v"]) / remaining + 0.5)
        converted.append(adjusted)
    evidence["provider_price_factors"] = sorted(factors)
    return converted, evidence
