"""
Unit tests for MayaCardio Window and Heart-Rate Processing Engine (Section 2).
Tests against hand-computed vectors, edge cases, and condition verification.
"""

import math
import pytest
from src.window import compute_window, find_peaks, compute_heart_rate


def test_window_complete_and_means():
    """Verify window metrics for a complete window with known hand-computed values."""
    w = 0
    # Sensor 1: 400 samples with 3 channels [ch1, ch2, ch3]
    # ch1 = 10, ch2 = 20, ch3 = 30 for all samples
    s1_data = {i: [10, 20, 30] for i in range(400)}
    # Sensor 2: 400 samples with 2 channels [ch1, ch2]
    # ch1 = 3, ch2 = 4 (magnitude sqrt(3^2 + 4^2) = 5.0)
    s2_data = {i: [3, 4] for i in range(400)}

    res = compute_window(window=w, sensor_1_data=s1_data, sensor_2_data=s2_data, mode="offline")

    assert res["window"] == 0
    assert res["mode"] == "offline"
    assert res["missing_1"] == 0
    assert res["missing_2"] == 0
    assert res["complete"] is True
    assert res["mean_1"] == [10.0, 20.0, 30.0]
    assert res["min_1"] == [10, 20, 30]
    assert res["max_1"] == [10, 20, 30]
    assert res["mean_2"] == [3.0, 4.0]
    assert res["min_2"] == [3, 4]
    assert res["max_2"] == [3, 4]
    assert res["magnitude"] == 5.0
    # w < 4 => heart_rate must be None
    assert res["heart_rate"] is None


def test_window_missing_samples():
    """Verify missing count and complete=False when samples are missing."""
    w = 1  # samples 400 to 799
    # Sensor 1 missing 50 samples (indices 400 to 449)
    s1_data = {i: [100, 200, 300] for i in range(450, 800)}
    # Sensor 2 missing 1 sample (index 799)
    s2_data = {i: [1000, 2000] for i in range(400, 799)}

    res = compute_window(window=w, sensor_1_data=s1_data, sensor_2_data=s2_data, mode="online")

    assert res["window"] == 1
    assert res["mode"] == "online"
    assert res["missing_1"] == 50
    assert res["missing_2"] == 1
    assert res["complete"] is False
    assert res["mean_1"] == [100.0, 200.0, 300.0]
    assert res["min_1"] == [100, 200, 300]
    assert res["max_1"] == [100, 200, 300]


def test_window_all_samples_missing():
    """Verify metrics when all samples in window are missing."""
    w = 2  # samples 800 to 1199
    s1_data = {}
    s2_data = {}

    res = compute_window(window=w, sensor_1_data=s1_data, sensor_2_data=s2_data)

    assert res["missing_1"] == 400
    assert res["missing_2"] == 400
    assert res["complete"] is False
    assert res["mean_1"] is None
    assert res["min_1"] is None
    assert res["max_1"] is None
    assert res["mean_2"] is None
    assert res["min_2"] is None
    assert res["max_2"] is None
    assert res["magnitude"] is None
    assert res["heart_rate"] is None


def test_peak_conditions_hand_computed():
    """
    Test each of the 4 peak conditions:
      (a) the 800 samples before i AND the sample after i are available;
      (b) x[i] > x[i-1] and x[i] > x[i+1];
      (c) x[i] > 0.5 * max(x[i-800 .. i-1]);
      (d) i - previous_peak_index >= 100.
    """
    # 1. Baseline: 800 prior samples at value 100, candidate at index 800
    s1_data = {i: [0, 100, 0] for i in range(800)}
    # Peak candidate at i = 800 with value 150 (> 0.5 * 100 = 50, > 100)
    s1_data[800] = [0, 150, 0]
    s1_data[801] = [0, 100, 0]

    peaks = find_peaks(s1_data, start_index=800, end_index=801)
    assert peaks == [800]

    # Condition (a) violation: missing sample 500 in prior 800
    s1_missing_prior = dict(s1_data)
    del s1_missing_prior[500]
    assert find_peaks(s1_missing_prior, start_index=800, end_index=801) == []

    # Condition (a) violation: missing sample i+1
    s1_missing_next = dict(s1_data)
    del s1_missing_next[801]
    assert find_peaks(s1_missing_next, start_index=800, end_index=801) == []

    # Condition (b) violation: x[i] <= x[i-1]
    s1_not_greater_prev = dict(s1_data)
    s1_not_greater_prev[800] = [0, 100, 0]  # equal to 100
    assert find_peaks(s1_not_greater_prev, start_index=800, end_index=801) == []

    # Condition (b) violation: x[i] <= x[i+1]
    s1_not_greater_next = dict(s1_data)
    s1_not_greater_next[801] = [0, 150, 0]  # equal to 150
    assert find_peaks(s1_not_greater_next, start_index=800, end_index=801) == []

    # Condition (c) violation: max prior is 1000, x[i] is 400 (not > 0.5 * 1000)
    s1_low_peak = dict(s1_data)
    s1_low_peak[700] = [0, 1000, 0]
    s1_low_peak[800] = [0, 400, 0]
    s1_low_peak[799] = [0, 100, 0]
    s1_low_peak[801] = [0, 100, 0]
    assert find_peaks(s1_low_peak, start_index=800, end_index=801) == []

    # Condition (d) violation: second peak at distance 99 (< 100)
    s1_double = dict(s1_data)
    # first peak at 800
    # candidate peak at 899 (899 - 800 = 99 < 100)
    for j in range(802, 905):
        s1_double[j] = [0, 100, 0]
    s1_double[899] = [0, 150, 0]
    assert find_peaks(s1_double, start_index=800, end_index=900) == [800]

    # Condition (d) satisfied: second peak at distance 100 (900 - 800 = 100 >= 100)
    s1_double[899] = [0, 100, 0]
    s1_double[900] = [0, 150, 0]
    peaks2 = find_peaks(s1_double, start_index=800, end_index=902)
    assert peaks2 == [800, 900]


def test_heart_rate_calculation_60_and_90_bpm():
    """
    Test heart rate calculation with synthesized signals:
    - 60 bpm: gap = 400 samples (1.0 second)
    - 90 bpm: gap = 266.67 samples (400 / 1.5 samples)
    """
    # Create a 6-second signal (2400 samples, indices 0 to 2399)
    # Window 4 covers samples 400*0 .. 400*5 - 1 = 0 .. 1999 for heart rate calculation
    s1_data = {}
    for i in range(2500):
        s1_data[i] = [0, 10, 0]  # baseline

    # Add peaks for 60 bpm at indices 800, 1200, 1600 (gaps = 400 samples = 1.0s)
    for p in [800, 1200, 1600]:
        s1_data[p] = [0, 50, 0]

    # Window 4: samples in last 5s are 400*(4-4) .. 400*5 - 1 = 0 .. 1999
    # All 2000 samples available. Peaks are [800, 1200, 1600].
    # Gaps = 1.0s, 1.0s. Mean gap = 1.0s. Heart rate = 60 / 1.0 = 60.0 bpm.
    hr = compute_heart_rate(window=4, sensor_1_data=s1_data)
    assert hr == 60.0

    # Test 90 bpm: gap = 267 samples and 266 samples (mean = 266.667 samples = 0.66667s)
    # 60 / (266.667 / 400) = 90.0 bpm
    s1_data_90 = {}
    for i in range(2500):
        s1_data_90[i] = [0, 10, 0]
    for p in [800, 1067, 1333, 1600]:
        s1_data_90[p] = [0, 50, 0]

    hr_90 = compute_heart_rate(window=4, sensor_1_data=s1_data_90)
    assert hr_90 == 90.0


def test_heart_rate_no_rate_conditions():
    """
    NO RATE rules:
    - heart_rate = null if w < 4
    - OR any sample in those 5 s is missing
    - OR fewer than 2 peaks
    """
    s1_data = {i: [0, 10, 0] for i in range(2500)}
    s1_data[800] = [0, 50, 0]
    s1_data[1200] = [0, 50, 0]

    # w < 4
    for w in range(4):
        assert compute_heart_rate(window=w, sensor_1_data=s1_data) is None

    # Missing sample in the 5-second window (e.g. sample 1000 deleted)
    s1_missing = dict(s1_data)
    del s1_missing[1000]
    assert compute_heart_rate(window=4, sensor_1_data=s1_missing) is None

    # Fewer than 2 peaks (only 1 peak at 800)
    s1_one_peak = {i: [0, 10, 0] for i in range(2500)}
    s1_one_peak[800] = [0, 50, 0]
    assert compute_heart_rate(window=4, sensor_1_data=s1_one_peak) is None


def test_ramp_mode_vector():
    """Verify Section 6.1 ramp mode values and window statistics."""
    w = 0
    # Sensor 1: c at sample i = (3i + c) mod 8388608
    s1_data = {
        i: [(3 * i + c) % 8388608 for c in range(3)]
        for i in range(400)
    }
    # Sensor 2: c at sample i = (2i + c) mod 8388608
    s2_data = {
        i: [(2 * i + c) % 8388608 for c in range(2)]
        for i in range(400)
    }

    res = compute_window(window=w, sensor_1_data=s1_data, sensor_2_data=s2_data)

    assert res["complete"] is True
    # For Sensor 1 channel 0: i=0..399 -> 3*i. Mean of 3*i is 3 * (399/2) = 598.5. Min = 0, Max = 1197.
    assert res["mean_1"][0] == 598.5
    assert res["min_1"][0] == 0
    assert res["max_1"][0] == 1197

    # For Sensor 1 channel 1: 3*i + 1. Mean = 599.5. Min = 1, Max = 1198.
    assert res["mean_1"][1] == 599.5
    assert res["min_1"][1] == 1
    assert res["max_1"][1] == 1198

    # For Sensor 2 channel 0: 2*i. Mean = 2 * 199.5 = 399.0. Min = 0, Max = 798.
    assert res["mean_2"][0] == 399.0
    assert res["min_2"][0] == 0
    assert res["max_2"][0] == 798


def test_magnitude_precision():
    """Verify Pythagorean magnitude: mean of sqrt(ch1^2 + ch2^2) over available samples."""
    # 2 samples: [300, 400] (mag = 500.0) and [500, 1200] (mag = 1300.0)
    # Mean mag = (500 + 1300) / 2 = 900.0
    s2_data = {
        0: [300, 400],
        1: [500, 1200],
    }
    s1_data = {0: [1, 2, 3], 1: [1, 2, 3]}

    res = compute_window(window=0, sensor_1_data=s1_data, sensor_2_data=s2_data)
    assert res["magnitude"] == 900.0

