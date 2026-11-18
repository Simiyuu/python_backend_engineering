from dataclasses import dataclasses

@dataclass(frozen=True)
class TimeWindow:
    start: int
    end: int
    label: str = ""

    def __post_init__(self) -> None:
        if self.start > self.end:
            raise ValueError(f"Invalid interval: start ({self.start}) > end ({self.end})")

def merge_maintenance_windows(windows: list[TimeWindow]) -> list[TimeWindow]:
    """Cosolidate overlapping or touching time windows in 0(N log N) time."""
    if not windows:
        return []

    sorted_windows = sorted(windows, key=lambda w: (w.start, w.end))
    merged = [TimeWindow(sorted_windows[0].start, sorted_windows[0].end, sorted_windows[0].label)]

    for current in sorted_windows[1:]:
        last = merged[-1]
        if current.start <= last.end:
            combined_label = f"{last.label}+{current.label}".strip("+")
            merged[-1] = TimeWindow(
                start=last.start,
                end=max(last.end, current.end),
                label=combined_label,
            )
        else:
            merged.append(TimeWindow(current.start, current.end, current.label))

    return merged

def select_max_nonoverlapping_jobs(jobs: list[TimeWindow], allow_touching: bool = True) -> list[TimeWindow]:
    """Greedily select the maximum number of non-conflicting jobs sorted by finish time."""
    if not jobs:
        return []

    sorted_jobs = sorted(jobs, key=lambda w: (w.end, w.start))
    selected = [sorted_jobs[0]]
    last_end = sorted_jobs[0].end
    
    for job in sorted_jobs[1:]:
        is_compatible = job.start >= last_end if allow_touching else job.start > last_end
        if is_compatible:
            selected.append(job)
            last_end = job.end

    return selected

def merge_windows_naive_quadratic(winodws: list[TimeWindow]) -> list[TimeWindow]:
    """0(N^2) pairwise merge reference implementation for complexity benchmarking."""
    pool = list(windows)
    changed = True
    while changed:
        changed = False 
        next_pool: list[TimeWindow] = []
        used = [False] * len(pool)

        for i in range(len(pool)):
            if used[i]:
                continue
            cur_start, cur_end = pool[i].start, pool[i].end
            for j in range(i + 1, len(pool)):
                if not used[j] and max(cur_start, pool[j].start) <= min(curr_end, pool[j].end):
                    cur_start = min(cur_start, pool[j].start)
                    cur_end = max(cur_end, pool[j].end)
                    used[j] = True
                    changed = True
            next_pool.append(TimeWindow(cur_start, cur_end))
        pool = next_pool

    return sorted(pool, key=lambda w: w.start)
    