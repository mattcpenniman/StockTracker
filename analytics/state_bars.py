"""Request-local typed state transport; timestamps never pass through floats."""
from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass
class StateBars:
    timestamps: np.ndarray
    values: tuple[np.ndarray, ...]

    @classmethod
    def from_rows(cls, rows):
        if not rows:
            return cls(np.array([], dtype=np.int64), tuple(np.array([], dtype=float) for _ in range(5)))
        columns = tuple(zip(*rows))
        # The SQL source is ordered and numeric. Coercion retains the full-frame
        # path's handling of NULL/nonfinite numeric data for Python fallback.
        return cls(np.asarray(columns[0], dtype=np.int64),
                   tuple(np.ascontiguousarray(pd.to_numeric(np.asarray(c), errors="coerce"))
                         for c in columns[1:]))

    def __len__(self):
        return len(self.timestamps)

    @property
    def empty(self):
        return not len(self)

    def latest(self):
        return {"timestamp": pd.Timestamp(int(self.timestamps[-1]), unit="us", tz="UTC"),
                **{name: values[-1] for name, values in zip(("open", "high", "low", "close", "volume"), self.values)}}

    def to_frame(self):
        return pd.DataFrame({"timestamp": pd.to_datetime(self.timestamps, unit="us", utc=True),
                             **dict(zip(("open", "high", "low", "close", "volume"), self.values))})

    def native_inputs(self):
        _, high, low, close, volume = self.values
        years = (self.timestamps.astype("datetime64[us]").astype("datetime64[Y]").astype(np.int64) + 1970).astype(np.int32)
        return *(np.ascontiguousarray(a, dtype=np.float64) for a in (close, high, low, volume)), years
