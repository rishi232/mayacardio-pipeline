"""
MayaCardio Window and Heart-Rate Processing Engine (Section 2).

This module implements the exact fixed processing logic required by Section 2
of the MayaCardio Software Systems Engineering assessment.

CRITICAL RULES:
- Window w contains samples 400w .. 400w+399 of EACH sensor.
- missing_1: count of sensor-1 samples unavailable in window [400w .. 400w+399].
- missing_2: count of sensor-2 samples unavailable in window [400w .. 400w+399].
- complete: (missing_1 == 0 AND missing_2 == 0).
- mean, min, max: per channel, over AVAILABLE samples (null if none).
- magnitude: mean of sqrt(ch1^2 + ch2^2) over AVAILABLE SENSOR-2 samples.
- heart_rate: bpm from sensor 1 channel 2 according to exact peak detection rule.
- mode: "online" (cloud) or "offline" (device).
- Round all decimal outputs to 1 decimal place.
- Pure function, no platform-specific branches. Identical online and offline.
"""

from __future__ import annotations
import math
from typing import Any, Dict, List, Mapping, Optional, Sequence


def find_peaks(
    sensor_1_data: Mapping[int, Sequence[int]],
    start_index: int = 800,
    end_index: Optional[int] = None,
    previous_peaks: Optional[List[int]] = None,
) -> List[int]:
    """
    Detect peaks in Sensor 1 Channel 2 according to Section 2:

    sample i is a peak iff ALL of:
      (a) the 800 samples before i AND the sample after i are available;
      (b) x[i] > x[i-1] and x[i] > x[i+1];
      (c) x[i] > 0.5 * max(x[i-800 .. i-1]);
      (d) i - previous_peak_index >= 100.
    """
    peaks: List[int] = list(previous_peaks) if previous_peaks else []
    last_peak_index: Optional[int] = peaks[-1] if peaks else None

    # Determine scanning range. Sample i must be at least 800 to have 800 prior samples.
    # Also cannot start before max(start_index, 800).
    scan_start = max(start_index, 800)
    
    # If starting from an existing peaks list, ensure we don't start before the sample after last scanned
    if last_peak_index is not None and scan_start <= last_peak_index:
        scan_start = last_peak_index + 1

    if end_index is None:
        if not sensor_1_data:
            return peaks
        # The maximum possible index for i such that i+1 could be available
        end_index = max(sensor_1_data.keys())

    # Scan sequentially through sample indices
    for i in range(scan_start, end_index + 1):
        # Condition (d): i - previous_peak_index >= 100
        if last_peak_index is not None and (i - last_peak_index) < 100:
            continue

        # Condition (a): sample i, i+1, and all 800 samples before i (i-800 .. i-1) must be available
        if i not in sensor_1_data or (i + 1) not in sensor_1_data or (i - 1) not in sensor_1_data:
            continue

        # Fast check: all 800 preceding samples in sensor_1_data
        # Note: sensor 1 channel 2 is index 1 (channels are 0, 1, 2)
        xi = sensor_1_data[i][1]
        x_prev = sensor_1_data[i - 1][1]
        x_next = sensor_1_data[i + 1][1]

        # Condition (b): x[i] > x[i-1] and x[i] > x[i+1]
        if not (xi > x_prev and xi > x_next):
            continue

        # Check availability of all 800 samples before i: [i-800 .. i-1]
        all_prior_available = True
        max_prior = -float("inf")
        for k in range(i - 800, i):
            if k not in sensor_1_data:
                all_prior_available = False
                break
            val = sensor_1_data[k][1]
            if val > max_prior:
                max_prior = val

        if not all_prior_available:
            continue

        # Condition (c): x[i] > 0.5 * max(x[i-800 .. i-1])
        if not (xi > 0.5 * max_prior):
            continue

        # All four conditions satisfied!
        peaks.append(i)
        last_peak_index = i

    return peaks


def compute_heart_rate(
    window: int,
    sensor_1_data: Mapping[int, Sequence[int]],
    peaks: Optional[List[int]] = None,
) -> Optional[float]:
    """
    Compute heart rate for window w according to Section 2:

    RATE: for window w, take peaks in the last 5 s, i.e. samples
          400*(w-4) .. 400*(w+1)-1.
          heart_rate = 60 / mean(gap_between_consecutive_peaks_in_seconds)
    NO RATE: heart_rate = null if w < 4, OR any sample in those 5 s is
             missing, OR fewer than 2 peaks.
    Round decimal outputs to 1 decimal place.
    """
    if window < 4:
        return None

    range_start = 400 * (window - 4)
    range_end = 400 * (window + 1) - 1  # inclusive

    # Check if any sample in those 5 seconds (2000 samples) is missing from sensor 1
    # Note: 400 * 5 = 2000 samples
    for idx in range(range_start, range_end + 1):
        if idx not in sensor_1_data:
            return None

    # Get peaks
    if peaks is None:
        # Detect peaks up to the end of this window
        peaks = find_peaks(sensor_1_data, start_index=800, end_index=range_end)

    # Filter peaks in the last 5 seconds: range_start <= p <= range_end
    window_peaks = [p for p in peaks if range_start <= p <= range_end]

    if len(window_peaks) < 2:
        return None

    # Consecutive gaps in seconds: at 400 Hz, gap in seconds = (p[k+1] - p[k]) / 400.0
    gaps_sec = [
        (window_peaks[k + 1] - window_peaks[k]) / 400.0
        for k in range(len(window_peaks) - 1)
    ]
    mean_gap_sec = sum(gaps_sec) / len(gaps_sec)

    if mean_gap_sec <= 0:
        return None

    bpm = 60.0 / mean_gap_sec
    return round(bpm, 1)


def compute_window(
    window: int,
    sensor_1_data: Mapping[int, Sequence[int]],
    sensor_2_data: Mapping[int, Sequence[int]],
    mode: str = "offline",
    session_id: Optional[str] = None,
    peaks: Optional[List[int]] = None,
) -> Dict[str, Any]:
    """
    Compute complete Section 2 result for window w.

    Window w contains samples 400w .. 400w+399 of EACH sensor.
    For every window produce:
        missing_1   : count of sensor-1 samples unavailable in window
        missing_2   : count of sensor-2 samples unavailable in window
        complete    : (missing_1 == 0 AND missing_2 == 0)
        mean,min,max: per channel, over AVAILABLE samples (null if none)
        magnitude   : mean of sqrt(ch1^2 + ch2^2) over AVAILABLE SENSOR-2 samples
        heart_rate  : bpm from sensor 1 channel 2
        mode        : "online" (cloud) or "offline" (device)
    Round all decimal outputs to 1 decimal place.
    """
    win_start = 400 * window
    win_end = win_start + 400  # non-inclusive range

    # Gather available samples for window w
    s1_avail: List[Sequence[int]] = []
    missing_1 = 0
    for idx in range(win_start, win_end):
        if idx in sensor_1_data:
            s1_avail.append(sensor_1_data[idx])
        else:
            missing_1 += 1

    s2_avail: List[Sequence[int]] = []
    missing_2 = 0
    for idx in range(win_start, win_end):
        if idx in sensor_2_data:
            s2_avail.append(sensor_2_data[idx])
        else:
            missing_2 += 1

    complete = (missing_1 == 0 and missing_2 == 0)

    # Mean, min, max for Sensor 1 (3 channels)
    if s1_avail:
        mean_1: Optional[List[float]] = []
        min_1: Optional[List[int]] = []
        max_1: Optional[List[int]] = []
        for ch in range(3):
            ch_vals = [s[ch] for s in s1_avail]
            mean_1.append(round(sum(ch_vals) / len(ch_vals), 1))
            min_1.append(min(ch_vals))
            max_1.append(max(ch_vals))
    else:
        mean_1 = None
        min_1 = None
        max_1 = None

    # Mean, min, max for Sensor 2 (2 channels)
    if s2_avail:
        mean_2: Optional[List[float]] = []
        min_2: Optional[List[int]] = []
        max_2: Optional[List[int]] = []
        for ch in range(2):
            ch_vals = [s[ch] for s in s2_avail]
            mean_2.append(round(sum(ch_vals) / len(ch_vals), 1))
            min_2.append(min(ch_vals))
            max_2.append(max(ch_vals))
    else:
        mean_2 = None
        min_2 = None
        max_2 = None

    # Magnitude over available Sensor-2 samples: mean of sqrt(ch1^2 + ch2^2)
    if s2_avail:
        mags = [math.sqrt(s[0] ** 2 + s[1] ** 2) for s in s2_avail]
        magnitude: Optional[float] = round(sum(mags) / len(mags), 1)
    else:
        magnitude = None

    # Heart rate from Sensor 1 Channel 2
    heart_rate = compute_heart_rate(window, sensor_1_data, peaks=peaks)

    result: Dict[str, Any] = {
        "session_id": session_id,
        "window": window,
        "mode": mode,
        "missing_1": missing_1,
        "missing_2": missing_2,
        "complete": complete,
        "mean_1": mean_1,
        "min_1": min_1,
        "max_1": max_1,
        "mean_2": mean_2,
        "min_2": min_2,
        "max_2": max_2,
        "mean": {"sensor_1": mean_1, "sensor_2": mean_2},
        "min": {"sensor_1": min_1, "sensor_2": min_2},
        "max": {"sensor_1": max_1, "sensor_2": max_2},
        "magnitude": magnitude,
        "heart_rate": heart_rate,
    }

    return result
