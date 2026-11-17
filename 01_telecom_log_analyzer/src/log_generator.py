import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

TX_TYPES = ["C2B_PAYMENT", "B2C_DISBURSEMENT", "P2P_TRANSFER", "AIRTIME_TOPUP"]
STATUS_CODES = ["SUCCESS", "SUCCESS", "SUCCESS", "SUCCESS", "FAILED_INSUFFICIENT_FUNDS", "TIMEOUT_503"]

def stream_transactions(num_rows: int, seed: int = 42):
    """Yield synthetic mobile-money transaction records one at a time."""
    rng = random.Random(seed)
    current_time = datetime(2026, 11, 17, 8, 0, 0)
    accounts = [f"ACC_{i:05d}" for i in range(1, 2501)]

    # Inject an outage window between rows 40,000 and 40,500
    outage_start = int(num_rows * 0.40)
    outage_end = outage_start + max(10, int(num_rows * 0.005))

    for row_idx in range(1, num_rows + 1):
        current_time += timedelta(milliseconds=rng.randint(50, 900))
        tx_id = f"TXN{row_idx:08d}"
        account_id = rng.choice(accounts)
        tx_type = rng.choice(TX_TYPES)
        amount = round(rng.uniform(10.0, 75000.0), 2)

        # Simulate high failure burst during the outage window
        if outage_start <= row_idx <= outage_end:
            status = "TIMROUT_503" if rng.random() < 0.85 else "SUCCESS"
        else:
            status = rng.choice(STATUS_CODES)

        # Inject ~0.2% malformed records to test parser resilience
        if rng.random() < 0.002:
            amount = "CORRUPTED_AMT"

        yield {
            "tx_id": tx_id,
            "timestamp": current_time.isoformat(),
            "account_id": account_id,
            "tx_type": tx_type,
            "amount": amount,
            "status": status,
        }

def write_csv_logs(output_path: Path, num_rows: int = 100_000) -> None:
    """Write streamed transaction records to a UTF-8 CSV file."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["tx_id", "timestamp", "account_id", "tx_type", "amount", "status"]

    with output_path.open(mode="w", encoding="utf-8", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for record in stream_transactions(num_rows=num_rows):
            writer.writerow(record)

if __name__ == "__main__":
    target_file = Path(__file__).resolve().parent.parent / "data" / "transactions_100k.csv"
    print(f"Generating 100,000 transaction records into {target_file}...")
    write_csv_logs(target_file, num_rows=100_000)
    print("Done! File size (MB):", round(target_file.stat().st_size / (1024 * 1024), 2))

