import csv
import sys
from pathlib import Path

# Ensure src/is importable regardless of where pytest is invoked
SRC_DIR = Path(__file__).resolve().parent.parent / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from parser import analyze_log_stream

def write_sample_csv(file_path: Path, rows: list[dict]) -> Path:
    fieldnames = ["tx_id", "timestamp", "account_id", "tx_type", "amount", "status"]
    with file_path.open(mode="w", encoding="utf-8", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return file_path

def test_empty_csv_returns_zero_report(tmp_path: Path):
    csv_file = write_sample_csv(tmp_path / "empty.csv", [])
    report = analyze_log_stream(csv_file, window_size=5)

    assert report.total_rows == 0
    assert report.valid_rows == 0
    assert report.corrupted_rows == 0
    assert report.total_volume == 0.0
    assert report.outage_windows == []

def test_valid_aggregation_and_corrupted_row_handling(tmp_path: Path):
    rows = [
        {"tx_id": "TXN1", "timestamp": "2026-11-17T08:00:01", "account_id": "ACC_1", "tx_type": "C2B_PAYMENT", "amount": "1500.50", "status": "SUCCESS"},
        {"tx_id": "TXN2", "timestamp": "2026-11-17T08:00:02", "account_id": "ACC_2", "tx_type": "P2P_TRANSFER", "amount": "500.00", "status": "SUCCESS"},
        {"tx_id": "TXN3", "timestamp": "2026-11-17T08:00:03", "account_id": "ACC_3", "tx_type": "C2B_PAYMENT", "amount": "9999.00", "status": "FAILED_INSUFFICIENT_FUNDS"},
        {"tx_id": "TXN4", "timestamp": "2026-11-17T08:00:04", "account_id": "ACC_4", "tx_type": "AIRTIME_TOPUP", "amount": "CORRUPTED_AMT", "status": "SUCCESS"},
    ]
    csv_file = write_sample_csv(tmp_path / "mixed.csv", rows)
    report = analyze_log_stream(csv_file, window_size=4)

    assert report.total_rows == 4
    assert report.valid_rows == 3
    assert report.corrupted_rows == 1
    assert report.total_volume == 2000.50
    assert report.status_counts["SUCCESS"] == 2
    assert report.status_counts["FAILED_INSUFFICIENT_FUNDS"] == 1
    assert report.type_volumes["C2B_PAYMENT"] == 1500.50
    assert report.type_volumes["P2P_TRANSFER"] == 500.00

def test_outage_detection_and_hysteresis_recovery(tmp_path: Path):
    # Window size = 4, threshold = 0.50 (>= 2 timeouts triggers outage, < 1 timeout recovers)
    statuses = [
        "SUCCESS", "SUCCESS", "SUCCESS", "SUCCESS",        # Healthy baseline
        "TIMEOUT_503", "TIMEOUT_503",
        "TIMEOUT_503", "SUCCESS",
        "SUCCESS", "SUCCESS", "SUCCESS",
    ]
    rows = [
        { 
        "tx_id": f"TXN{i}",
        "timestamp": f"2026-11-17T08:00:{i:02d}",
        "account_id": "ACC_1",
        "tx_type": "C2B_PAYMENT",
        "amount": "100.00",
        "status": status,
        }
        for i, status in enumerate(statuses)
    ]
    csv_file = write_sample_csv(tmp_path / "outage.csv", rows)
    report = analyze_log_stream(csv_file, window_size=4, failure_threshold=0.50)

    assert len(report.outage_windows) == 1
    start_ts, end_ts = report.outage_windows[0]
    assert start_ts == "2026-11-17T08:00:05"
    assert end_ts == "2026-11-17T08:00:10"

def test_outage_active_at_eof_closes_cleanly(tmp_path: Path):
    rows = [
        {
            "tx_id": f"TXN{i}",
            "timestamp": f"2026-11-17T09:00:{i:02d}",
            "account_id": "ACC_1",
            "tx_type": "C2B_PAYMENT",
            "amount": "250.00",
            "status": "TIMEOUT_503",
        }
        for i in range(4)
    ]
    csv_file = write_sample_csv(tmp_path / "eof_outage.csv", rows)
    report = analyze_log_stream(csv_file, window_size=4, failure_threshold=0.50)

    assert len(report.outage_windows) == 1
    assert report.outage_windows[0] == ("2026-11-17T09:00:03", "2026-11-17T09:00:03")

    