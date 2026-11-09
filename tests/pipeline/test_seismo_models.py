import math
from datetime import datetime, timedelta, timezone

import numpy as np

from src.common.seismo_models import (
    aftershock_hours,
    estimate_b_value,
    estimate_completeness_magnitude,
    fit_omori_utsu,
    omori_expected,
    omori_integral,
    omori_rate,
)


def gutenberg_richter_sample(b: float, mc: float, n: int, seed: int = 7) -> list[float]:
    """Magnitudes above mc with b-value b, rounded to 0.1 like a real catalogue."""
    rng = np.random.default_rng(seed)
    beta = b * math.log(10)
    mags = (mc - 0.05) + rng.exponential(1.0 / beta, size=n)
    return [round(float(m), 1) for m in mags]


def test_b_value_maximum_likelihood_recovers_b():
    for b_true in (0.8, 1.0, 1.3):
        mags = gutenberg_richter_sample(b=b_true, mc=2.0, n=4000)
        mc = estimate_completeness_magnitude(mags)
        b, sigma, n = estimate_b_value(mags, mc)
        assert mc == 2.0
        assert abs(b - b_true) < 0.08
        assert sigma == round(b / math.sqrt(n), 4)


def test_completeness_magnitude_is_not_shifted_by_float_bin_edges():
    # Rounded magnitudes sit on bin edges; float edges used to move 2.3 into the 2.2 bin.
    mags = [2.2] * 5 + [2.3] * 4 + [2.4] * 6 + [2.5] * 3
    assert estimate_completeness_magnitude(mags) == 2.4


def test_b_value_needs_five_events_above_mc():
    assert estimate_b_value([2.0, 2.1, 2.2, 2.3], 2.0) == (None, None, 4)
    assert estimate_completeness_magnitude([2.0, 2.1]) is None


def synthetic_aftershock_hours(k: float, c: float, p: float, days: float, seed: int = 11) -> list[float]:
    """Poisson aftershock times (hours) drawn from n(t) = K (t + c)^-p by inverse-transform sampling."""
    rng = np.random.default_rng(seed)
    total = omori_integral(c, p, 0.0, days)
    n = rng.poisson(k * total)
    q = 1.0 - p
    t_days = (c**q + rng.random(n) * total * q) ** (1.0 / q) - c
    return sorted(float(t) * 24.0 for t in t_days)


def test_omori_mle_recovers_decay_exponent():
    for p_true in (0.9, 1.1, 1.4):
        hours = synthetic_aftershock_hours(k=150.0, c=0.05, p=p_true, days=7)
        fit = fit_omori_utsu(hours, observed_until_hours=7 * 24)
        assert fit is not None
        assert abs(fit.p_value - p_true) < 0.1
        assert fit.aftershock_count == len(hours)
        assert fit.next_six_hour_expected > fit.next_hour_rate > 0
        assert fit.r_squared > 0.8


def test_omori_forecast_matches_the_fitted_rate():
    hours = synthetic_aftershock_hours(k=150.0, c=0.05, p=1.1, days=2)
    fit = fit_omori_utsu(hours, observed_until_hours=48)
    expected = omori_expected(fit.k_value, fit.c_value, fit.p_value, 48, 54)
    assert abs(fit.next_six_hour_expected - expected) < 1e-3
    # omori_rate is per hour: integrating it over an hour gives about the expected count.
    assert abs(omori_rate(fit.k_value, fit.c_value, fit.p_value, 48.5) - omori_expected(fit.k_value, fit.c_value, fit.p_value, 48, 49)) < 1e-3


def test_omori_forecast_starts_at_requested_time():
    hours = synthetic_aftershock_hours(k=150.0, c=0.05, p=1.1, days=1)
    at_last_event = fit_omori_utsu(hours)
    two_days_later = fit_omori_utsu(hours, forecast_from_hours=72)
    assert two_days_later.forecast_origin_hours == 72
    assert at_last_event.forecast_origin_hours == round(max(hours), 4)
    # Omori-Utsu decays, so a later forecast origin means fewer expected aftershocks.
    assert two_days_later.next_six_hour_expected < at_last_event.next_six_hour_expected
    # Never forecast from before the last observed aftershock.
    assert fit_omori_utsu(hours, forecast_from_hours=1).forecast_origin_hours == at_last_event.forecast_origin_hours


def test_quiet_time_after_the_last_aftershock_lowers_the_forecast():
    # Five aftershocks early on, then five quiet days (a typical sparse live sequence).
    hours = [2.0, 5.0, 9.0, 30.0, 70.0]
    fit = fit_omori_utsu(hours, forecast_from_hours=124, observed_until_hours=124)
    assert fit is not None
    # The old least-squares fit on non-empty hourly bins gave p=0 and 6 aftershocks in 6 hours here.
    assert fit.p_value > 0.3
    assert fit.next_six_hour_expected < 0.5


def test_short_sequences_use_the_generic_p():
    assert fit_omori_utsu([1.0, 2.0]) is None
    short = fit_omori_utsu([2.0, 30.0, 70.0], forecast_from_hours=124, observed_until_hours=124)
    assert short is not None and not short.p_estimated
    assert short.p_value == 1.08
    # The old fit returned p=0, R²=1 and 6 expected aftershocks in 6 hours for this sequence.
    assert short.next_six_hour_expected < 0.2
    assert fit_omori_utsu([1.0, 2.0, 3.0, 4.0, 5.0]).p_estimated


def test_aftershock_hours_keeps_events_within_radius_after_mainshock():
    t0 = datetime(2026, 11, 1, tzinfo=timezone.utc)
    candidates = [
        (t0 + timedelta(hours=2), -122.30, 37.70),  # ~10 km away
        (t0 + timedelta(hours=5), -118.20, 34.00),  # Los Angeles, ~500 km away
        (t0 - timedelta(hours=1), -122.35, 37.62),  # before the mainshock
        (t0 + timedelta(hours=3), None, 37.6),
    ]
    assert aftershock_hours(t0, -122.35, 37.62, candidates, radius_km=300) == [2.0]
