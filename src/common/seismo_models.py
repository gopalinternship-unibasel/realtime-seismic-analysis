from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Iterable, Optional

import numpy as np

from src.common.geo import haversine_km

AFTERSHOCK_RADIUS_KM = 300.0
AFTERSHOCK_WINDOW_DAYS = 7
# Fewer aftershocks than MIN_AFTERSHOCKS_FOR_P: K is fitted with p fixed at the generic value
# of Reasenberg & Jones (1989); fewer than MIN_AFTERSHOCKS: no forecast.
MIN_AFTERSHOCKS = 3
MIN_AFTERSHOCKS_FOR_P = 5
GENERIC_P = 1.08
# The grid p is searched on. The fitted p is used only when its 95 % profile-likelihood interval
# (every p whose log-likelihood is within P_PROFILE_DROP of the best) lies inside the grid and is
# at most P_MAX_INTERVAL_WIDTH wide; otherwise the aftershock times do not pin p down closely
# enough, and the generic p is used instead.
P_GRID_MIN = 0.30
P_GRID_MAX = 2.50
# Half the 95 % quantile of the chi-square distribution with one degree of freedom (3.84 / 2).
P_PROFILE_DROP = 1.92
# A design choice: p known to about +-0.3. In synthetic sequences drawn from the model (p 0.9 to
# 1.4, c = 0.05 d) and observed for 7 days, the median interval width was 1.2 to 1.3 for 5
# aftershocks (clipped at the grid edges), 0.9 to 1.0 for 10, 0.6 to 0.7 for 20, 0.5 to 0.55 for
# 30 and 0.4 for 50; 0 to 2 % of the 20-aftershock sequences and about 90 to 100 % of the
# 30-aftershock ones passed.
P_MAX_INTERVAL_WIDTH = 0.6


def _bin_index(values: np.ndarray, bin_width: float) -> np.ndarray:
    """Magnitude bin numbers on an integer grid (bin k is centred on k * bin_width).

    Catalogue magnitudes are rounded (2.3, 2.4, ...) and sit exactly on float bin edges, where
    np.histogram's accumulated float edges push some of them into the neighbouring bin
    (2.3 lands in the 2.2 bin, for example). Integer bin numbers avoid that.
    """
    return np.round(values / bin_width + 1e-9).astype(int)


def _clean(magnitudes: Iterable[float]) -> np.ndarray:
    return np.array([float(m) for m in magnitudes if m is not None and not math.isnan(float(m))])


def estimate_completeness_magnitude(magnitudes: Iterable[float], bin_width: float = 0.1) -> Optional[float]:
    """Completeness magnitude Mc by maximum curvature: the most populated magnitude bin."""
    values = _clean(magnitudes)
    if values.size < 5:
        return None
    bins, counts = np.unique(_bin_index(values, bin_width), return_counts=True)
    return round(float(bins[int(counts.argmax())] * bin_width), 2)


def estimate_b_value(magnitudes: Iterable[float], mc: float, bin_width: float = 0.1) -> tuple[Optional[float], Optional[float], int]:
    """Aki-Utsu maximum-likelihood b-value for binned magnitudes at or above Mc, with sigma = b / sqrt(n)."""
    values = _clean(magnitudes)
    binned = _bin_index(values, bin_width)
    above = binned[binned >= _bin_index(np.array([mc]), bin_width)[0]]
    n = int(above.size)
    if n < 5:
        return None, None, n
    denominator = float(above.mean() * bin_width - (mc - bin_width / 2.0))
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
    forecast_origin_hours: float = 0.0
    p_estimated: bool = True


def omori_integral(c_days: float, p_value: float, t0_days: float, t1_days: float) -> float:
    """Integral of (t + c)^-p dt from t0 to t1 (days)."""
    if abs(p_value - 1.0) < 1e-9:
        return math.log((t1_days + c_days) / (t0_days + c_days))
    q = 1.0 - p_value
    return ((t1_days + c_days) ** q - (t0_days + c_days) ** q) / q


def omori_rate(k_value: float, c_days: float, p_value: float, hours_after: float) -> float:
    """Aftershocks per hour at ``hours_after`` hours after the mainshock.

    Omori-Utsu: n(t) = K (t + c)^-p aftershocks per day, t in days since the mainshock.
    """
    return float(k_value * ((hours_after / 24.0 + c_days) ** (-p_value)) / 24.0)


def omori_expected(k_value: float, c_days: float, p_value: float, from_hours: float, to_hours: float) -> float:
    """Expected number of aftershocks between ``from_hours`` and ``to_hours`` after the mainshock."""
    return float(k_value * omori_integral(c_days, p_value, from_hours / 24.0, to_hours / 24.0))


KM_PER_DEGREE = 111.19


def aftershock_search_box(
    lon: float, lat: float, radius_km: float = AFTERSHOCK_RADIUS_KM
) -> tuple[float, float, list[tuple[float, float]]]:
    """A latitude range and one or two longitude ranges that contain every point within ``radius_km``.

    The SQL pre-filter must be a superset of the haversine test in ``aftershock_hours``. A fixed
    +-4 degree longitude box was too narrow at high latitude (300 km is 5.4 degrees of longitude
    at 60 N, so Alaskan aftershocks east or west of the mainshock were lost) and broke at the
    antimeridian (Fiji, Tonga, the Aleutians), where the box must wrap from +180 to -180.
    """
    margin = 1.05  # a little wider than needed, so rounding never drops an event on the edge
    dlat = radius_km / KM_PER_DEGREE * margin
    lat_min, lat_max = max(-90.0, lat - dlat), min(90.0, lat + dlat)
    # The widest longitude span of the circle is at the latitude closest to a pole.
    extreme_lat = max(abs(lat_min), abs(lat_max))
    cos_lat = math.cos(math.radians(extreme_lat))
    if extreme_lat >= 89.0 or cos_lat <= 0:
        return lat_min, lat_max, [(-180.0, 180.0)]
    dlon = radius_km / (KM_PER_DEGREE * cos_lat) * margin
    if dlon >= 180.0:
        return lat_min, lat_max, [(-180.0, 180.0)]
    lo, hi = lon - dlon, lon + dlon
    if lo < -180.0:
        return lat_min, lat_max, [(-180.0, hi), (lo + 360.0, 180.0)]
    if hi > 180.0:
        return lat_min, lat_max, [(lo, 180.0), (-180.0, hi - 360.0)]
    return lat_min, lat_max, [(lo, hi)]


def search_box_params(lon: float, lat: float, radius_km: float = AFTERSHOCK_RADIUS_KM) -> tuple[float, ...]:
    """(lat_min, lat_max, lon_lo_1, lon_hi_1, lon_lo_2, lon_hi_2) for
    ``latitude BETWEEN ? AND ? AND (longitude BETWEEN ? AND ? OR longitude BETWEEN ? AND ?)``.
    Without a wrap the second longitude range repeats the first."""
    lat_min, lat_max, ranges = aftershock_search_box(lon, lat, radius_km)
    first, second = ranges[0], ranges[-1]
    return lat_min, lat_max, first[0], first[1], second[0], second[1]


def find_larger_events(
    shocks: Iterable[tuple],
    radius_km: float = AFTERSHOCK_RADIUS_KM,
    window_days: float = AFTERSHOCK_WINDOW_DAYS,
) -> dict[str, Optional[str]]:
    """Window declustering of the M >= 5 events: event id -> id of a larger event of its sequence, or None.

    ``shocks`` are ``(event_id, source, event_time, magnitude, lon, lat)``. An event has a larger
    event when another event of the same source with a strictly larger magnitude lies within
    ``radius_km`` and ``window_days`` of it: before it (the event is that event's aftershock, and
    the larger event's Omori-Utsu fit already counts it) or after it (the event was a foreshock).
    These are the same space and time windows the aftershock search uses, so no event is both
    a mainshock and an aftershock of a larger one. When several larger events qualify, the
    largest (then the earliest) is returned. Only events mapped to None get a forecast.
    """
    by_source: dict[str, list[tuple]] = {}
    for shock in shocks:
        by_source.setdefault(shock[1], []).append(shock)
    window_seconds = window_days * 86400.0
    result: dict[str, Optional[str]] = {}
    for events in by_source.values():
        events.sort(key=lambda s: s[2])
        for event_id, _source, t, mag, lon, lat in events:
            best = None
            for other_id, _s, t2, mag2, lon2, lat2 in events:
                if other_id == event_id or mag2 is None or mag is None or float(mag2) <= float(mag):
                    continue
                if abs((t2 - t).total_seconds()) > window_seconds:
                    continue
                if haversine_km(float(lon), float(lat), float(lon2), float(lat2)) > radius_km:
                    continue
                if best is None or (float(mag2), -t2.timestamp()) > (float(best[1]), -best[2].timestamp()):
                    best = (other_id, mag2, t2)
            result[event_id] = best[0] if best else None
    return result


def aftershock_hours(
    main_time,
    main_lon: float,
    main_lat: float,
    candidates: Iterable[tuple],
    radius_km: float = AFTERSHOCK_RADIUS_KM,
) -> list[float]:
    """Hours after the mainshock for candidate events ``(event_time, lon, lat)`` within ``radius_km``."""
    hours_after = []
    for event_time, lon, lat in candidates:
        if lon is None or lat is None:
            continue
        if haversine_km(float(main_lon), float(main_lat), float(lon), float(lat)) <= radius_km:
            hours = (event_time - main_time).total_seconds() / 3600.0
            if hours > 0:
                hours_after.append(hours)
    return hours_after


def fit_omori_utsu(
    event_hours_after_mainshock: Iterable[float],
    c_days: float = 0.05,
    forecast_from_hours: Optional[float] = None,
    observed_until_hours: Optional[float] = None,
    min_aftershocks: int = MIN_AFTERSHOCKS,
) -> Optional[OmoriFit]:
    """Maximum-likelihood fit of the Omori-Utsu law n(t) = K (t + c)^-p to aftershock times.

    t is measured in days and c is fixed (two-parameter fit, stable for small sequences).
    The aftershocks are treated as a Poisson process observed from the mainshock until
    ``observed_until_hours`` (default: the last aftershock). For each p the likelihood is
    maximised by K = n / integral(0..T), and p is chosen on a 0.01 grid in [0.3, 2.5]
    (Ogata 1983, with c held fixed). Quiet time after the last aftershock therefore lowers
    the forecast, unlike a least-squares fit to the non-empty hourly bins only. Sequences
    with fewer than MIN_AFTERSHOCKS_FOR_P aftershocks keep p at GENERIC_P and fit K only, and
    so does a sequence whose 95 % profile-likelihood interval of p reaches an edge of the grid
    or is wider than P_MAX_INTERVAL_WIDTH: the data do not pin p down closely enough then. The
    M6.8 off Timor-Leste of 21 Nov 2026 had 5 aftershocks; its best p was 0.30 on 25 Nov
    (interval 0.30 to 1.00) and 0.32 on 26 Nov (interval 0.30 to 1.05), so a test of the best p
    alone against the grid edge missed it on 26 Nov. When the interval reaches an edge the
    generic p can lie outside it (all aftershocks within minutes, then a quiet week, gives an
    interval of 1.96 to the upper edge): such a sequence still gets the generic p, because no p
    inside the grid is pinned down. The grid edge also catches a p that is well determined but
    within about 0.1 of 0.30 or 0.1 to 0.2 of 2.50 (seen in simulation with 200 to 500
    aftershocks); how often real sequences have such a p was not checked.

    The forecast is the expected number of aftershocks in the hour and the six hours
    after ``forecast_from_hours`` (the time the forecast is made, e.g. "now"); it never
    starts before the end of the observation window.
    """
    hours = np.sort(np.array([float(h) for h in event_hours_after_mainshock if h is not None and float(h) > 0.0]))
    n = int(hours.size)
    if n < min_aftershocks:
        return None

    t_end_hours = max(float(hours[-1]), float(observed_until_hours or 0.0))
    t_days = hours / 24.0
    t_end = t_end_hours / 24.0
    sum_log = float(np.sum(np.log(t_days + c_days)))

    def scores(p_values):
        """(log-likelihood, p, K) for each p, with K at its maximum-likelihood value for that p."""
        found = []
        for p_value in p_values:
            integral = omori_integral(c_days, float(p_value), 0.0, t_end)
            if integral <= 0:
                continue
            k_value = n / integral
            log_likelihood = n * math.log(k_value) - float(p_value) * sum_log - n
            found.append((log_likelihood, float(p_value), k_value))
        return found

    p_estimated = n >= MIN_AFTERSHOCKS_FOR_P
    best = None
    if p_estimated:
        scored = scores(np.round(np.arange(P_GRID_MIN, P_GRID_MAX + 0.0001, 0.01), 2))
        if scored:
            best = max(scored, key=lambda s: s[0])
            plausible = [p for log_likelihood, p, _k in scored if best[0] - log_likelihood <= P_PROFILE_DROP]
            low, high = min(plausible), max(plausible)
            if low <= P_GRID_MIN + 1e-9 or high >= P_GRID_MAX - 1e-9 or high - low > P_MAX_INTERVAL_WIDTH + 1e-9:
                p_estimated = False
        else:
            p_estimated = False
    if not p_estimated:
        generic = scores([GENERIC_P])
        best = generic[0] if generic else None
    if best is None:
        return None
    _ll, p_value, k_value = best

    # Goodness of fit: observed vs expected aftershocks per hour over the observation window.
    n_bins = max(1, int(math.ceil(t_end_hours)))
    observed, _edges = np.histogram(hours, bins=np.arange(0, n_bins + 1, 1))
    expected = np.array([omori_expected(k_value, c_days, p_value, h, h + 1) for h in range(n_bins)])
    ss_res = float(np.sum((observed - expected) ** 2))
    ss_tot = float(np.sum((observed - observed.mean()) ** 2))
    r_squared = 1.0 if ss_tot == 0.0 else 1.0 - ss_res / ss_tot

    origin = t_end_hours if forecast_from_hours is None else max(t_end_hours, float(forecast_from_hours))
    next_hour = omori_expected(k_value, c_days, p_value, origin, origin + 1)
    next_six = omori_expected(k_value, c_days, p_value, origin, origin + 6)
    return OmoriFit(
        k_value=round(k_value, 4),
        c_value=round(c_days, 4),
        p_value=round(p_value, 4),
        next_hour_rate=round(next_hour, 4),
        next_six_hour_expected=round(next_six, 4),
        r_squared=round(r_squared, 4),
        aftershock_count=n,
        forecast_origin_hours=round(float(origin), 4),
        p_estimated=p_estimated,
    )
