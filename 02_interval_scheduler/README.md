# Project 02: Network Maintenance & Interval Scheduler

An interval-merging and greedy job-scheduling engine applying $0(N \log N)$ sort-and-sweep and greedy finishing-time selection to telecom maintenance windows.

## Key Engineering Concepts
- **Contiguous Outage Consolidation (`merge_maintenance_windows`):** Sorts maintenance windows by start time ($0(N \log N)$) and merges overlapping or adjacent windows in a single linear sweep.
- **Greedy Exclusive Resource Allocation (`select_max_nonoverlapping_jobs`):** Sorts candidate jobs by finish time ($0(N \log N)$) to maximize non-conflicting maintenance tasks, supporting both touching and strictly disjoint boundary rules.
- **Differential Testing & Empirical Complexity Analysis:** Validates algorithmic equivalence against an $0(N^2)$ reference merger via randomized `pytest` suites and measures asymptotic speedup with `timeit`

## Usage
```bash
# Run 0(N log N) vs 0(N^2) complexity benchmark
python 02_interval_scheduler/src/benchmark.py

# Execute unit and differential test suite
pytest -v
