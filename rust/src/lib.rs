use numpy::{IntoPyArray, PyReadonlyArray1};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::collections::{BTreeMap, VecDeque};

type Columns = BTreeMap<&'static str, Vec<f64>>;

fn zip(a: &[f64], b: &[f64], f: impl Fn(f64, f64) -> f64) -> Vec<f64> {
    a.iter().zip(b).map(|(&x, &y)| f(x, y)).collect()
}

fn ratio(a: f64, b: f64) -> f64 {
    if b == 0.0 {
        f64::NAN
    } else {
        a / b
    }
}

fn shift(a: &[f64], lag: usize) -> Vec<f64> {
    (0..a.len())
        .map(|i| if i >= lag { a[i - lag] } else { f64::NAN })
        .collect()
}

// adjust=False, min_periods semantics; inputs have only leading NaNs.
fn ewm(a: &[f64], alpha: f64, min_periods: usize) -> Vec<f64> {
    let mut value = f64::NAN;
    let mut count = 0;
    a.iter()
        .map(|&x| {
            if !x.is_nan() {
                value = if count == 0 || value == x {
                    x
                } else {
                    (1.0 - alpha) * value + alpha * x
                };
                count += 1;
            }
            if count >= min_periods {
                value
            } else {
                f64::NAN
            }
        })
        .collect()
}

// Two-pass variance avoids cancellation for large prices/volumes and flat windows.
fn rolling(a: &[f64], window: usize, min_periods: usize, op: &str) -> Vec<f64> {
    if op == "mean" {
        return rolling_mean(a, window, min_periods);
    }
    if op == "median" {
        return rolling_median(a, window, min_periods);
    }
    let mut scratch = Vec::with_capacity(window);
    (0..a.len())
        .map(|i| {
            scratch.clear();
            scratch.extend(
                a[i.saturating_add(1).saturating_sub(window)..=i]
                    .iter()
                    .copied()
                    .filter(|x| x.is_finite()),
            );
            if scratch.len() < min_periods {
                return f64::NAN;
            }
            if scratch.iter().all(|&x| x == scratch[0]) {
                return if op == "std" { 0.0 } else { scratch[0] };
            }
            let (mut sum, mut compensation) = (0.0, 0.0);
            for &x in &scratch {
                let corrected = x - compensation;
                let next = sum + corrected;
                compensation = (next - sum) - corrected;
                sum = next;
            }
            let mean = sum / scratch.len() as f64;
            if op == "std" {
                (scratch.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / scratch.len() as f64)
                    .sqrt()
            } else {
                mean
            }
        })
        .collect()
}

fn compensated_add(sum: &mut f64, compensation: &mut f64, value: f64) {
    let corrected = value - *compensation;
    let next = *sum + corrected;
    *compensation = (next - *sum) - corrected;
    *sum = next;
}

fn rolling_mean(a: &[f64], window: usize, min_periods: usize) -> Vec<f64> {
    let (mut sum, mut add_comp, mut remove_comp) = (0.0, 0.0, 0.0);
    let (mut count, mut same_count) = (0, 0);
    let mut previous = f64::NAN;
    a.iter()
        .enumerate()
        .map(|(i, &x)| {
            if i >= window && a[i - window].is_finite() {
                compensated_add(&mut sum, &mut remove_comp, -a[i - window]);
                count -= 1;
            }
            if x.is_finite() {
                compensated_add(&mut sum, &mut add_comp, x);
                count += 1;
                same_count = if x == previous { same_count + 1 } else { 1 };
                previous = x;
            }
            if count < min_periods {
                f64::NAN
            } else if same_count >= count {
                previous
            } else {
                sum / count as f64
            }
        })
        .collect()
}

fn rolling_median(a: &[f64], window: usize, min_periods: usize) -> Vec<f64> {
    let mut sorted: Vec<f64> = Vec::with_capacity(window);
    a.iter()
        .enumerate()
        .map(|(i, &x)| {
            if i >= window && a[i - window].is_finite() {
                let position = sorted
                    .binary_search_by(|v| v.total_cmp(&a[i - window]))
                    .unwrap();
                sorted.remove(position);
            }
            if x.is_finite() {
                let position = sorted
                    .binary_search_by(|v| v.total_cmp(&x))
                    .unwrap_or_else(|p| p);
                sorted.insert(position, x);
            }
            let n = sorted.len();
            if n < min_periods {
                f64::NAN
            } else {
                (sorted[(n - 1) / 2] + sorted[n / 2]) / 2.0
            }
        })
        .collect()
}

// Monotonic queues make all high/low lookbacks O(n), including custom windows.
fn extrema(a: &[f64], window: usize, maximum: bool) -> Vec<f64> {
    let mut queue: VecDeque<usize> = VecDeque::new();
    (0..a.len())
        .map(|i| {
            while queue.front().is_some_and(|&j| i - j >= window) {
                queue.pop_front();
            }
            while queue
                .back()
                .is_some_and(|&j| if maximum { a[j] <= a[i] } else { a[j] >= a[i] })
            {
                queue.pop_back();
            }
            queue.push_back(i);
            if i + 1 >= window && !a[i + 1 - window].is_nan() {
                a[queue[0]]
            } else {
                f64::NAN
            }
        })
        .collect()
}

fn calculate(
    close: &[f64],
    high: &[f64],
    low: &[f64],
    volume: &[f64],
    years: &[i32],
    annualization: f64,
    breakout: usize,
    slope: usize,
    buffer: f64,
) -> Columns {
    let mut c = Columns::new();
    for (name, lag) in [
        ("return_1", 1),
        ("return_5", 5),
        ("return_20", 20),
        ("return_60", 60),
    ] {
        c.insert(name, zip(close, &shift(close, lag), |x, y| x / y - 1.0));
    }
    let mut year = years[0];
    let mut start = close[0];
    c.insert(
        "ytd_return",
        close
            .iter()
            .zip(years)
            .map(|(&x, &y)| {
                if y != year {
                    year = y;
                    start = x;
                }
                x / start - 1.0
            })
            .collect(),
    );
    for (name, window) in [("sma_20", 20), ("sma_50", 50), ("sma_200", 200)] {
        c.insert(name, rolling(close, window, window, "mean"));
    }
    for (name, period) in [("ema_12", 12), ("ema_26", 26)] {
        c.insert(name, ewm(close, 2.0 / (period + 1) as f64, period));
    }
    for (name, source) in [("sma20_slope", "sma_20"), ("sma50_slope", "sma_50")] {
        c.insert(
            name,
            zip(&c[source], &shift(&c[source], slope), |x, y| {
                (x - y) / slope as f64
            }),
        );
    }
    let delta = zip(close, &shift(close, 1), |x, y| x - y);
    let gains: Vec<_> = delta
        .iter()
        .map(|&x| if x.is_nan() { x } else { x.max(0.0) })
        .collect();
    let losses: Vec<_> = delta
        .iter()
        .map(|&x| if x.is_nan() { x } else { (-x).max(0.0) })
        .collect();
    c.insert(
        "rsi_14",
        zip(
            &ewm(&gains, 1.0 / 14.0, 14),
            &ewm(&losses, 1.0 / 14.0, 14),
            |g, l| {
                if g.is_nan() || l.is_nan() {
                    f64::NAN
                } else if g == 0.0 {
                    0.0
                } else if l == 0.0 {
                    100.0
                } else {
                    100.0 - 100.0 / (1.0 + g / l)
                }
            },
        ),
    );
    c.insert("macd", zip(&c["ema_12"], &c["ema_26"], |x, y| x - y));
    c.insert("macd_signal", ewm(&c["macd"], 2.0 / 10.0, 9));
    c.insert(
        "macd_hist",
        zip(&c["macd"], &c["macd_signal"], |x, y| x - y),
    );
    let tr: Vec<_> = (0..close.len())
        .map(|i| {
            let range = high[i] - low[i];
            if i == 0 {
                range
            } else {
                range
                    .max((high[i] - close[i - 1]).abs())
                    .max((low[i] - close[i - 1]).abs())
            }
        })
        .collect();
    c.insert("atr_14", ewm(&tr, 1.0 / 14.0, 14));
    c.insert("atr_14_pct", zip(&c["atr_14"], close, ratio));
    c.insert("stddev_20", rolling(&c["return_1"], 20, 20, "std"));
    c.insert(
        "realized_vol_20",
        c["stddev_20"].iter().map(|x| x * annualization).collect(),
    );
    for (name, source) in [
        ("return_20_over_vol_20", "return_20"),
        ("return_60_over_vol_20", "return_60"),
    ] {
        c.insert(name, zip(&c[source], &c["realized_vol_20"], ratio));
    }
    for (hi, lo, window) in [
        ("high_20", "low_20", 20),
        ("high_55", "low_55", 55),
        ("high_252", "low_252", 252),
    ] {
        c.insert(hi, extrema(high, window, true));
        c.insert(lo, extrema(low, window, false));
    }
    for (hi, lo, window) in [
        ("prior_high_20", "prior_low_20", 20),
        ("prior_high_55", "prior_low_55", 55),
        ("prior_high_breakout", "prior_low_breakdown", breakout),
    ] {
        c.insert(hi, shift(&extrema(high, window, true), 1));
        c.insert(lo, shift(&extrema(low, window, false), 1));
    }
    for (name, source) in [
        ("distance_from_high_20", "high_20"),
        ("distance_from_low_20", "low_20"),
        ("distance_from_high_252", "high_252"),
        ("distance_from_low_252", "low_252"),
        ("distance_from_sma_20", "sma_20"),
        ("distance_from_sma_50", "sma_50"),
        ("distance_from_sma_200", "sma_200"),
    ] {
        c.insert(name, zip(close, &c[source], |x, y| x / y - 1.0));
    }
    let range = zip(&c["high_20"], &c["low_20"], |x, y| x - y);
    c.insert(
        "range_position_20",
        zip(&zip(close, &c["low_20"], |x, y| x - y), &range, ratio),
    );
    c.insert("avg_volume_20", rolling(volume, 20, 20, "mean"));
    c.insert("volume_ratio_20", zip(volume, &c["avg_volume_20"], ratio));
    c.insert(
        "volume_zscore",
        zip(
            &zip(volume, &c["avg_volume_20"], |x, y| x - y),
            &rolling(volume, 20, 20, "std"),
            ratio,
        ),
    );
    let returns = &c["return_1"];
    c.insert(
        "return_zscore",
        zip(
            &zip(returns, &rolling(returns, 20, 20, "mean"), |x, y| x - y),
            &c["stddev_20"],
            ratio,
        ),
    );
    c.insert(
        "breakout_level",
        c["prior_high_breakout"]
            .iter()
            .map(|x| x * (1.0 + buffer))
            .collect(),
    );
    c.insert(
        "breakdown_level",
        c["prior_low_breakdown"]
            .iter()
            .map(|x| x * (1.0 - buffer))
            .collect(),
    );
    c.insert(
        "breakout_strength",
        zip(
            &zip(close, &c["breakout_level"], |x, y| x - y),
            &c["atr_14"],
            ratio,
        ),
    );
    c.insert(
        "breakdown_strength",
        zip(
            &zip(&c["breakdown_level"], close, |x, y| x - y),
            &c["atr_14"],
            ratio,
        ),
    );
    c.insert(
        "trend_strength",
        zip(
            &zip(&c["sma_20"], &c["sma_50"], |x, y| x - y),
            &c["atr_14"],
            ratio,
        ),
    );
    c.insert(
        "extension_from_mean",
        zip(&zip(close, &c["sma_20"], |x, y| x - y), &c["atr_14"], ratio),
    );
    c.insert(
        "volatility_baseline",
        rolling(&c["realized_vol_20"], 60, 20, "median"),
    );
    c.insert(
        "volatility_ratio",
        zip(&c["realized_vol_20"], &c["volatility_baseline"], ratio),
    );
    c
}

#[pyfunction]
#[allow(clippy::too_many_arguments)]
fn feature_columns<'py>(
    py: Python<'py>,
    close: PyReadonlyArray1<'py, f64>,
    high: PyReadonlyArray1<'py, f64>,
    low: PyReadonlyArray1<'py, f64>,
    volume: PyReadonlyArray1<'py, f64>,
    years: PyReadonlyArray1<'py, i32>,
    annualization: f64,
    breakout: usize,
    slope: usize,
    buffer: f64,
) -> PyResult<Bound<'py, PyDict>> {
    let (close, high, low, volume, years) = (
        close.as_slice()?,
        high.as_slice()?,
        low.as_slice()?,
        volume.as_slice()?,
        years.as_slice()?,
    );
    if breakout == 0
        || slope == 0
        || [high.len(), low.len(), volume.len(), years.len()]
            .iter()
            .any(|&n| n != close.len())
    {
        return Err(PyValueError::new_err(
            "Equal-length arrays and positive lookbacks are required",
        ));
    }
    let columns = if close.is_empty() {
        Columns::new()
    } else {
        py.allow_threads(|| {
            calculate(
                close,
                high,
                low,
                volume,
                years,
                annualization,
                breakout,
                slope,
                buffer,
            )
        })
    };
    let result = PyDict::new(py);
    for (name, values) in columns {
        result.set_item(name, values.into_pyarray(py))?;
    }
    Ok(result)
}

#[pymodule]
fn apollo_indicators(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(feature_columns, m)?)?;
    Ok(())
}
