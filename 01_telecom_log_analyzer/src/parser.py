import csv
from collections import Counter, deque
from dataclasses import dataclass, field
from pathlib import Path

@dataclass
class AnalysisReport:
    total_rows: int = 0
    valid_rows: int = 0
    corrupted_rows: int = 0
    total_volume: float = 0.0
    status_counts: Counter = field(default_factory=Counter)
    type_volumes: Counter = field(default_factory=Counter)
    outage_windows: list = field(default_factory=list)

def analyze_log_stream(csv_path: Path, window_size: int = 100, failure_threshold: float = 0.50) -> AnalysisReport:
    """Stream through a CSV transaction log, aggregate metircs,and detect timeout spikes."""
    report = AnalysisReport()
    recent_window = deque(maxlen=window_size)
    recent_timeouts = 0
    in_outage = False 
    current_outage_start = None

    with csv_path.open(mode="r", encoding="utf-8", newline="") as csvfile:
        reader = csv.DictReader(csvfile)

        for row in reader:
            report.total_rows += 1

            # 1. Defensive parsing of numeric amount
            try:
                amount = float(row["amount"])
            except (ValueError, KeyError, TypeError):
                report.corrupted_rows += 1
                continue
            
            report.valid_rows += 1
            status = row["status"]
            tx_type = row["tx_type"]
            timestamp = row["timestamp"]

            # 2. Accumulate business metrics
            report.status_counts[status] += 1
            if status == "SUCCESS":
                report.total_volume += amount
                report.type_volumes[tx_type] += amount

            # 3. Maintain 0(1) sliding window of recent timeout flags
            is_timeout = 1 if status == "TIMEOUT_503" else 0
            if len(recent_window) == window_size:
                recent_timeouts -= recent_window[0]
            recent_window.append(is_timeout)
            recent_timeouts += is_timeout

            # 4. State machine for outage window start/recovery
            if len(recent_window) == window_size:
                failure_rate = recent_timeouts / window_size
                if failure_rate >= failure_threshold and not in_outage:
                    in_outage = True
                    current_outage_start = timestamp
                elif failure_rate < (failure_threshold * 0.5) and in_outage:
                    in_outage = False
                    report.outage_start = None

    # Close any outage still active at EOF 
    if in_outage and current_outage_start is not None:
        report.outage_windows.append((current_outage_start, timestamp))
        current_outage_start = None
    
    return report

if __name__ == "__main__":
    data_file = Path(__file__).resolve().parent.parent / "data" / "transactions_100k.csv"
    summary = analyze_log_stream(data_file)

    print("=== TELECOM TRANSACTION LOG SUMMARY ===")
    print(f"Total Rows Processed : {summary.total_rows:,}")
    print(f"Valid Rows           : {summary.valid_rows:,}")
    print(f"Corrupted Rows       : {summary.corrupted_rows:,}")
    print(f"Total Success Volume : KES {summary.total_volume:,.2f}")
    print(f"\nStatus Breakdown:")
    for status, count in summary.status_counts.most_common():
        print(f" - {status:<26}: {count:,}")
    print("\nDetected Outage Windows (503 Spike >= 50%):")
    for start_ts, end_ts in summary.outage_windows:
        print(f" -From {start_ts} to {end_ts}")
