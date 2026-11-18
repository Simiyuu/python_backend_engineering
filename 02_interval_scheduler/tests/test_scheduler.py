import random
import sys
from pathlib import Path

import pytest

SRC_DIR = Path(__file__).resolve().parent.parent / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from scheduler import (
    TimeWindow,
    merge_maintenance_windows,
    merge_windows_naive_quadratic,
    select_max_nonoverlapping_jobs,
)

def test_invalid_interval_raises_value_error():
    with pytest.raises(ValueError, match="Invalid interval"):
        TimeWindow(start=15, end=10, label="broken")

def test_empty_inputs_return_empty_lists():
    assert merge_maintenance_windows([]) == []
    assert select_max_nonoverlapping_jobs([]) == []

def test_merge_overlapping_and_touching_windows():
    windows = [
        TimeWindow(10, 14, "core"),
        TimeWindow(1, 5, "edge"),
        TimeWindow(4, 8, "ran"),
        TimeWindow(8, 10, "fiber"),
        TimeWindow(20, 25, "backup"),
    ]
    merged = merge_maintenance_windows(windows)

    assert len(merged) == 2
    assert (merged[0].start, merged[0].end) == (1, 14)
    assert (merged[1].start, merged[1].end) == (20, 25)

def test_differential_equivalence_against_quadratic_reference():
    rng = random.Random(99)
    for _ in range(25):
        raw = []
        for _ in range(30):
            s = rng.randint(0, 100)
            e = s + rng.randint(0, 20)
            raw.append(TimeWindow(s, e))

        fast_res = [(w.start, w.end) for w in merge_maintenance_windows(raw)]
        slow_res = [(w.start, w.end) for w in merge_windows_naive_quadratic(raw)]

        assert fast_res == slow_res
