# Project 01: Streaming Telecom Transaction Log Analyzer

A constant-memory Python streaming log processor, sliding-window outage detector, and performance profiler built for high-volume mobile-money and telecom transaction logs.

## Key Engineering Concepts
- **Constant-Memory Streaming (`yield` & `csv.DictReader`):** Generate and parses 100,000+ transaction records without loading datasets into RAM.
- **$0(1)$ Sliding-Window Anomaly Detection (`collections.deque`):** Detects `TIMEOUT_503` error spikes crossing configurable threshold with hysteresis recovery to prevent alert flapping.
- **Defensive Ingestion:** Isolates and counts malformed/corrupted records without halting stream processing.
- **CLI & Profiling (`argparse` & `cProfile`):** Supportssynthetic log generation, machine-readable JSON reporting, and throughput profiling.

## Usage
```bash
# Generate 100,000 synthetic transaction records
python 01_telecom_log_analyzer/src/cli.py generate --rows 100000 --output 01_telecom_log_analyzer/data/transactions_100k.csv

# Analyze log stream and output JSON summary
python 01_telecom_log_analyzer/src/cli.py analyze --input 01_telecom_log_analyzer/data/transactions_100k.csv --json

# Run cProfile throughput benchmark
python 01_telecom_log_analyzer/src/cli.py benchmark --input 01_telecom_log_analyzer/data/transactions_100k.csv

# Execute unit and CLI integration test suite 
pytest -v

