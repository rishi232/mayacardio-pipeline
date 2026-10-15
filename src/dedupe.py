"""
MayaCardio Deduplication Index.

Maintains an on-disk and in-memory interval index of received (session_id, sensor, index)
ranges. Samples delivered multiple times are identified and filtered out in O(log N) time.
Can be reconstructed from the WAL at any time.
"""

from __future__ import annotations
import bisect
import json
import os
from typing import Dict, List, Optional, Tuple


class IntervalIndex:
    """
    Tracks disjoint sorted intervals [start, end] inclusive of received indices.
    Efficient for continuous streams (e.g. 1.4 million samples are represented by a few intervals).
    """

    def __init__(self) -> None:
        # List of [start, end] inclusive intervals, sorted by start
        self.intervals: List[List[int]] = []

    def contains(self, index: int) -> bool:
        """Check if index is already recorded."""
        if not self.intervals:
            return False
        # Find interval with start <= index
        idx = bisect.bisect_right(self.intervals, [index, float("inf")]) - 1
        if idx >= 0:
            start, end = self.intervals[idx]
            if start <= index <= end:
                return True
        return False

    def add_range(self, start: int, end: int) -> List[Tuple[int, int]]:
        """
        Add range [start, end] inclusive.
        Returns the list of newly added sub-ranges (that were not previously recorded).
        """
        if start > end:
            return []

        # Find overlapping or adjacent intervals
        # An interval [s, e] is adjacent/overlapping if e >= start - 1 and s <= end + 1
        new_intervals: List[List[int]] = []
        cur_start, cur_end = start, end
        added_new: List[Tuple[int, int]] = []

        inserted = False
        i = 0
        while i < len(self.intervals):
            s, e = self.intervals[i]
            if e < cur_start - 1:
                # Completely before
                new_intervals.append([s, e])
                i += 1
            elif s > cur_end + 1:
                # Completely after
                if not inserted:
                    new_intervals.append([cur_start, cur_end])
                    inserted = True
                new_intervals.append([s, e])
                i += 1
            else:
                # Overlap or adjacent: merge
                cur_start = min(cur_start, s)
                cur_end = max(cur_end, e)
                i += 1

        if not inserted:
            new_intervals.append([cur_start, cur_end])

        self.intervals = new_intervals
        return added_new

    def filter_new(self, indices: List[int]) -> List[int]:
        """Given a list of indices, return only those not already in the index."""
        return [i for i in indices if not self.contains(i)]

    def to_list(self) -> List[List[int]]:
        return [list(iv) for iv in self.intervals]

    def from_list(self, data: List[List[int]]) -> None:
        self.intervals = [list(iv) for iv in data]


class DedupeManager:
    """
    Manages deduplication indices across sessions and sensors.
    Persists interval index checkpoints to disk.
    """

    def __init__(self, index_dir: str) -> None:
        self.index_dir = index_dir
        os.makedirs(self.index_dir, exist_ok=True)
        # (session_id, sensor) -> IntervalIndex
        self.indices: Dict[Tuple[str, int], IntervalIndex] = {}

    def get_index(self, session_id: str, sensor: int) -> IntervalIndex:
        key = (session_id, sensor)
        if key not in self.indices:
            idx = IntervalIndex()
            # Try to load existing checkpoint from disk
            path = self._index_file_path(session_id, sensor)
            if os.path.exists(path):
                try:
                    with open(path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                        idx.from_list(data)
                except Exception:
                    pass  # If corrupt, will be rebuilt from WAL
            self.indices[key] = idx
        return self.indices[key]

    def _index_file_path(self, session_id: str, sensor: int) -> str:
        # Sanitize session_id for filesystem safety
        safe_session = "".join(c for c in session_id if c.isalnum() or c in "-_")
        return os.path.join(self.index_dir, f"dedupe_{safe_session}_{sensor}.json")

    def save_checkpoint(self, session_id: str, sensor: int) -> None:
        """Atomic write of interval index checkpoint."""
        key = (session_id, sensor)
        if key not in self.indices:
            return
        path = self._index_file_path(session_id, sensor)
        tmp_path = path + ".tmp"
        data = self.indices[key].to_list()
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, path)

    def record_sample(self, session_id: str, sensor: int, index: int) -> bool:
        """
        Record a single sample index.
        Returns True if newly added, False if duplicate.
        """
        idx = self.get_index(session_id, sensor)
        if idx.contains(index):
            return False
        idx.add_range(index, index)
        return True

    def record_range(self, session_id: str, sensor: int, start: int, end: int) -> None:
        idx = self.get_index(session_id, sensor)
        idx.add_range(start, end)
