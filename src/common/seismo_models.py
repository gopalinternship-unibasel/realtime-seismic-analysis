from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Iterable, Optional

import numpy as np


def estimate_completeness_magnitude(magnitudes: Iterable[float], bin_width: float = 0.1) -> Optional[float]:
    values = np.array([float(m) for m in magnitudes if m is not None and not math.isnan(float(m))])
    if values.size < 5:
        return None
    lower = math.floor(values.min() / bin_width) * bin_width
    upper = math.ceil(values.max() / bin_width) * bin_width + bin_width
    bins = np.arange(lower, upper + bin_width, bin_width)
    hist, edges = np.histogram(values, bins=bins)
    if hist.size == 0 or hist.max() == 0:
        return None
    return round(float(edges[int(hist.argmax())]), 2)


def estimate_b_value(magnitudes: Iterable[float], mc: float, bin_width: float = 0.1) -> tuple[Optional[float], Optional[float], int]:
    values = np.array([float(m) for m in magnitudes if m is not None and not math.isnan(float(m)) and float(m) >= mc])
    n = int(values.size)
    if n < 5:
        return None, None, n
    denominator = float(values.mean() - (mc - bin_width / 2.0))
    if denominator <= 0:
        return None, None, n
    b_value = math.log10(math.e) / denominator
    sigma = b_value / math.sqrt(n)
    return round(b_value, 4), round(sigma, 4), n


@dataclass
class OmoriFit:
    k_value: float
    c_value: float
    p_value: float
    next_hour_rate: float
    next_six_hour_expected: float
    r_squared: float
    aftershock_count: int


def fit_omori_utsu(event_hours_after_mainshock: Iterable[float], c_days: float = 0.05) -> Optional[OmoriFit]:
    """Fit n(t)=K*(t+c)^(-p) from hourly aftershock counts using log-linear LS.

    t is measured in days. c is fixed to keep the two-parameter fit stable for small demos.
    """
    hours = np.array([float(h) for h in event_hours_after_mainshock if h is not None and float(h) > 0.0])
    if hours.size < 3:
        return None

    max_hour = max(1, int(math.ceil(hours.max())))
    counts, edges = np.histogram(hours, bins=np.arange(0, max_hour + 2, 1))
    mids_days = ((edges[:-1] + edges[1:]) / 2.0) / 24.0
    mask = counts > 0
    if mask.sum() < 2:
        return None

    x = np.log10(mids_days[mask] + c_days)
    y = np.log10(counts[mask])
    slope, intercept = np.polyfit(x, y, 1)
    p_value = -float(slope)
    k_value = float(10 ** intercept)

    y_pred = slope * x + intercept
    ss_res = float(np.sum((y - y_pred) ** 2))
    ss_tot = float(np.sum((y - y.mean()) ** 2))
    r_squared = 1.0 if ss_tot == 0.0 else 1.0 - ss_res / ss_tot

    next_hour_day = (max_hour + 1) / 24.0
    next_hour_rate = k_value * ((next_hour_day + c_days) ** (-p_value))
    next_six_hour_expected = sum(
        k_value * ((((max_hour + h) / 24.0) + c_days) ** (-p_value)) for h in range(1, 7)
    )
    return OmoriFit(
        k_value=round(k_value, 4),
        c_value=round(c_days, 4),
        p_value=round(p_value, 4),
        next_hour_rate=round(float(next_hour_rate), 4),
        next_six_hour_expected=round(float(next_six_hour_expected), 4),
        r_squared=round(float(r_squared), 4),
        aftershock_count=int(hours.size),
    )
