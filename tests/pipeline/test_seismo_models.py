import math
from datetime import datetime, timedelta, timezone

import numpy as np

from src.common.geo import haversine_km
from src.common.seismo_models import (
    aftershock_hours,
    aftershock_search_box,
    estimate_b_value,
    estimate_completeness_magnitude,
    find_larger_events,
    fit_omori_utsu,
    omori_expected,
    omori_integral,
    omori_rate,
    search_box_params,
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
    # Five aftershocks leave p open (95 % interval 0.30 to 1.61), so K is fitted with the generic p;
    # the quiet days still count, through the observed time of 124 hours.
    assert not fit.p_estimated and fit.p_value == 1.08
    assert abs(fit.k_value - 5 / omori_integral(0.05, 1.08, 0.0, 124 / 24.0)) < 1e-3
    assert fit.next_six_hour_expected < 0.5


def test_short_sequences_use_the_generic_p():
    assert fit_omori_utsu([1.0, 2.0]) is None
    short = fit_omori_utsu([2.0, 30.0, 70.0], forecast_from_hours=124, observed_until_hours=124)
    assert short is not None and not short.p_estimated
    assert short.p_value == 1.08
    # The old fit returned p=0, R²=1 and 6 expected aftershocks in 6 hours for this sequence.
    assert short.next_six_hour_expected < 0.2
    # Five aftershocks that thin out over time still leave p open (95 % interval 0.30 to 1.97).
    decaying = fit_omori_utsu([0.5, 1.5, 4.0, 10.0, 30.0])
    assert not decaying.p_estimated and decaying.p_value == 1.08
    # 43 aftershocks of a p = 1.1 sequence pin p down (interval 0.97 to 1.38, 0.41 wide): it is fitted.
    longer_hours = synthetic_aftershock_hours(k=10.0, c=0.05, p=1.1, days=7)
    longer = fit_omori_utsu(longer_hours, observed_until_hours=7 * 24)
    assert len(longer_hours) == 43
    assert longer.p_estimated and abs(longer.p_value - 1.1) < 0.2


def test_p_falls_back_to_the_generic_p_when_its_95_percent_interval_is_too_wide():
    # 10 aftershocks of a p = 1.1 sequence: the interval 0.81 to 1.72 lies inside the grid but is
    # 0.91 wide, more than P_MAX_INTERVAL_WIDTH (0.6).
    hours = synthetic_aftershock_hours(k=3.0, c=0.05, p=1.1, days=7)
    fit = fit_omori_utsu(hours, observed_until_hours=7 * 24)
    assert len(hours) == 10
    assert not fit.p_estimated and fit.p_value == 1.08


def test_p_falls_back_to_the_generic_p_when_its_95_percent_interval_reaches_the_grid_edge():
    # The M6.8 off Timor-Leste of 21 Nov 2026: 5 aftershocks. On 25 Nov (observed for 108.59 hours) its
    # best p was 0.30, the grid's lower edge (interval 0.30 to 1.00); on 26 Nov (121.02 hours) it was
    # 0.32 and shown as "fitted", although its 95 % interval ran from 0.30 to 1.05. Both must use the
    # generic p; only the 26 Nov case failed under the rule of commit a57f641.
    timor_hours = [6.23, 43.56, 45.09, 45.9, 65.5]
    timor_25_nov = fit_omori_utsu(timor_hours, forecast_from_hours=108.59, observed_until_hours=108.59)
    assert not timor_25_nov.p_estimated and timor_25_nov.p_value == 1.08
    timor_26_nov = fit_omori_utsu(timor_hours, forecast_from_hours=121.02, observed_until_hours=121.02)
    assert not timor_26_nov.p_estimated and timor_26_nov.p_value == 1.08
    timor = fit_omori_utsu(timor_hours)
    # All aftershocks within minutes, then a quiet week: the interval reaches the upper edge.
    burst = fit_omori_utsu([0.05, 0.1, 0.15, 0.2, 0.25], observed_until_hours=7 * 24)
    assert not burst.p_estimated and burst.p_value == 1.08
    # A steady rate (no decay at all) cannot fix p either.
    steady = fit_omori_utsu([1.0, 2.0, 3.0, 4.0, 5.0])
    assert not steady.p_estimated and steady.p_value == 1.08
    # K is refitted for the generic p: n over the integral of (t + c)^-p across the observed time.
    expected_k = 5 / omori_integral(0.05, 1.08, 0.0, 65.5 / 24.0)
    assert abs(timor.k_value - expected_k) < 1e-3


def test_aftershock_hours_keeps_events_within_radius_after_mainshock():
    t0 = datetime(2026, 11, 1, tzinfo=timezone.utc)
    candidates = [
        (t0 + timedelta(hours=2), -122.30, 37.70),  # ~10 km away
        (t0 + timedelta(hours=5), -118.20, 34.00),  # Los Angeles, ~500 km away
        (t0 - timedelta(hours=1), -122.35, 37.62),  # before the mainshock
        (t0 + timedelta(hours=3), None, 37.6),
    ]
    assert aftershock_hours(t0, -122.35, 37.62, candidates, radius_km=300) == [2.0]


def in_box(box, lon, lat):
    lat_min, lat_max, ranges = box
    return lat_min <= lat <= lat_max and any(lo <= lon <= hi for lo, hi in ranges)


def destination(lon, lat, bearing_deg, km):
    """Point ``km`` away from (lon, lat) along ``bearing_deg`` on a sphere (R = 6371 km)."""
    d, b = km / 6371.0, math.radians(bearing_deg)
    p1, l1 = math.radians(lat), math.radians(lon)
    p2 = math.asin(math.sin(p1) * math.cos(d) + math.cos(p1) * math.sin(d) * math.cos(b))
    l2 = l1 + math.atan2(math.sin(b) * math.sin(d) * math.cos(p1), math.cos(d) - math.sin(p1) * math.sin(p2))
    return (math.degrees(l2) + 540.0) % 360.0 - 180.0, math.degrees(p2)


def test_aftershock_search_box_covers_the_whole_radius_at_high_latitude_and_the_antimeridian():
    # The old +-4 degree box: 300 km east of an Alaskan mainshock at 62 N is about 5.7 degrees away.
    east = destination(-150.0, 62.0, 90.0, 290.0)
    assert abs(east[0] - -150.0) > 4.0
    for lon, lat in [(-150.0, 62.0), (179.8, -17.5), (-179.9, 52.0), (10.0, 0.0), (0.0, 88.5), (120.0, -70.0)]:
        box = aftershock_search_box(lon, lat, 300.0)
        for bearing in range(0, 360, 15):
            for km in (100.0, 250.0, 299.0):
                p_lon, p_lat = destination(lon, lat, bearing, km)
                assert haversine_km(lon, lat, p_lon, p_lat) <= 300.0
                assert in_box(box, p_lon, p_lat), (lon, lat, bearing, km, p_lon, p_lat, box)
    # Across the antimeridian the box has two longitude ranges, and the SQL parameters carry both.
    lat_min, lat_max, ranges = aftershock_search_box(179.8, -17.5, 300.0)
    assert len(ranges) == 2 and ranges[0][1] == 180.0 and ranges[1][0] == -180.0
    params = search_box_params(179.8, -17.5, 300.0)
    assert params[:2] == (lat_min, lat_max) and params[2:4] == ranges[0] and params[4:6] == ranges[1]
    # Without a wrap the second range repeats the first.
    assert search_box_params(10.0, 0.0)[2:4] == search_box_params(10.0, 0.0)[4:6]


T_MAIN = datetime(2026, 11, 9, 4, 0, tzinfo=timezone.utc)


def test_m5_events_in_the_sequence_of_a_larger_event_are_not_mainshocks():
    shocks = [
        ("m7", "replay", T_MAIN, 7.0, 100.0, -5.0),
        ("fore", "replay", T_MAIN - timedelta(hours=2), 5.2, 100.1, -5.1),  # foreshock 15 km away
        ("after1", "replay", T_MAIN + timedelta(hours=1), 5.4, 99.8, -5.3),
        ("after2", "replay", T_MAIN + timedelta(days=6), 5.0, 101.5, -4.0),  # 200 km, 6 days later
        ("far", "replay", T_MAIN + timedelta(hours=3), 5.5, 110.0, -5.0),  # 1,100 km away
        ("late", "replay", T_MAIN + timedelta(days=8), 5.1, 100.0, -5.0),  # after the 7-day window
        ("live", "usgs-live", T_MAIN + timedelta(hours=1), 5.3, 100.0, -5.0),  # other source
        ("twin", "replay", T_MAIN + timedelta(hours=5), 5.5, 110.1, -5.1),  # as large as "far"
    ]
    larger = find_larger_events(shocks, radius_km=300.0, window_days=7)
    assert larger["m7"] is None
    assert larger["fore"] == "m7" and larger["after1"] == "m7" and larger["after2"] == "m7"
    assert larger["far"] is None and larger["twin"] is None  # equal magnitudes: a doublet keeps both
    assert larger["late"] is None and larger["live"] is None
    # The largest qualifying event wins: "after1" (M5.4) is larger than "fore" but M7 is chosen.
    assert find_larger_events(shocks[:3])["fore"] == "m7"
