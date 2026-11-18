import argparse
import cProfile
import json
import pstats
import time
from io import StringIO
from pathlib import Path

from log_generator import write_csv_logs
from parser import analyze_log_stream

def build_parser() -> argparse.ArgumentParser:
    cli_parser = argparse.ArgumentParser(
        prog="telecom-log-analyzer",
        description="Streaming telecom transaction log generator, anomaly detector, and profiler.",
    )
    subparsers = cli_parser.add_subparsers(dest="command", required=True)

    # Subcommand 1: generate
    gen_cmd = subparsers.add_parser("generate", help="Generate synthetic CSV transaction logs")
    gen_cmd.add_argument("--rows", type=int, default=100_000, help="Number of rows to generate (default: 100000)")
    gen_cmd.add_argument("--output", type=Path, required=True, help="Destination CSV file path")


    # Subcommand 2: analyze
    analyze_cmd = subparsers.add_parser("analyze", help="Analyze a transaction log CSV stream")
    analyze_cmd.add_argument("--input", type=Path, required=True, help="Path to input CSV log file")
    analyze_cmd.add_argument("--window", type=int, default=100, help="Sliding window size (default: 100)")
    analyze_cmd.add_argument("--threshold", type=float, default=0.50, help="Failure rate threshold 0.0-1.0 (default: 0.50)")
    analyze_cmd.add_argument("--json", action="store_true", help="Output report as formatted JSON")

    # Subcommand 3: benchmark
    bench_cmd = subparsers.add_parser("benchmark", help="Profile streaming parser throughput with cProfile")
    bench_cmd.add_argument("--input", type=Path, required=True, help="Path to input CSV log file")
    bench_cmd.add_argument("--top", type=int, default=10, help="Number of top profile hotspots to display")

    return cli_parser

def handle_analyze(args: argparse.Namespace) -> None:
    report = analyze_log_stream(
        csv_path=args.input,
        window_size=args.window,
        failure_threshold=args.threshold,
    )

    if args.json:
        payload = {
            "total_rows": report.total_rows,
            "valid_rows": report.valid_rows,
            "corrupted_rows": report.corrupted_rows,
            "total_volume_kes": round(report.total_volume, 2),
            "status_counts": dict(report.status_counts),
            "type_volumes": {k: round(v, 2) for k, v in report.type_volumes.items()},
            "outage_windows": [
                {"start": start, "end": end} for start, end in report.outage_windows
            ],
        }
        print(json.dumps(payload, indent=2))
        return

    print("=== TELECOM LOG ANALYSIS REPORT ===")
    print(f"Total Rows     : {report.total_rows:,}")
    print(f"Valid Rows     : {report.valid_rows:,}")
    print(f"Corrupted Rows : {report.corrupted_rows:,}")
    print(f"Success Volume : KES {report.total_volume:,.2f}")
    print(f"Outages Found  : {len(report.outage_windows)}")
    for start, end in report.outage_windows:
        print(f"  *{start} -> {end}")

def handle_benchmark(args: argparse.Namespace) -> None:
    profiler = cProfile.Profile()
    t0 = time.perf_counter()

    profiler.enable()
    report = analyze_log_stream(csv_path=args.input)
    profiler.disable()

    elapsed = time.perf_counter() - t0
    throughput = report.total_rows / elapsed if elapsed > 0 else 0

    print(f"Processed {report.total_rows:,} rows in {elapsed:.4f}s ({throughput:,.0f} rows/sec)\n")
    stream = StringIO()
    stats = pstats.Stats(profiler, stream=stream).sort_stats("tottime")
    stats.print_stats(args.top)
    print(stream.getvalue())

def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "generate":
        write_csv_logs(output_path=args.output, num_rows=args.rows)
        print(f"Generated {args.rows:,} records at {args.output}")
    elif args.command == "analyze":
        handle_analyze(args)
    elif args.command == "benchmark":
        handle_benchmark(args)

if __name__ == "__main__":
    main()
