import random
import timeit

from scheduler import (
    TimeWindow,
    merge_maintenance_windows,
    merge_windows_naive_quadratic,
)

def generate_windows(count: int, seed: int = 42) -> list[TimeWindow]:
    rng = random.Random(seed)
    windows: list[TimeWindow] = []
    for idx in range(count):
        start = rng.randint(1, 25)
        end = start + rng.randint(0, count * 2)
        windows.append(TimeWindow(start=start, end=end, label=f"win_{idx}"))
    return windows

def run_benchmark() -> None:
    sizes = [100, 500, 1000, 2000]
    iterations = 10

    header = f"{'N (windows)':<14} | {'0(N log N) ms':<16} | {'0(N^2) ms':<14} | {'Speedup':<10}"
    print(header)
    print("-" * len(header))

    for n in sizes:
        dataset = generate_windows(n)

        fast_sec = timeit.timeit(
            lambda: merge_windows_naive_quadratic(dataset),
            number=iterations,
        ) / iterations

        slow_sec = timeit.timeit(
            lambda: merge_windows_naive_quadratic(dataset),
            number=iterations,
        ) / iterations

        fast_ms = fast_sec * 1000
        slow_ms = slow_sec * 1000
        speedup = slow_ms / fast_ms if fast_ms > 0 else 0.0

        print(f"{n:<14,} | {fast_ms:<16.4f} | {slow_ms:<14.4f} | {speedup:,.1f}x")

if __name__ == "__main__":
    run_benchmark()
    

