import json
import sys
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parent.parent / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from cli import main

def test_cli_generate_and_analyze_json_pipeline(tmp_path: Path, monkeypatch, capsys):
    generated_csv = tmp_path / "pipeline_test.csv"

    # 1. Test `generate` subcommand
    monkeypatch.setattr(
        sys,
        "argv",
        ["cli.py", "generate", "--rows", "500", "--output", str(generated_csv)],
    )
    main()
    gen_out = capsys.readouterr().out
    assert generated_csv.exists()
    assert "Generated 500 records" in gen_out

    # 2. Test `analyze --json` subcommand on the generated file
    monkeypatch.setattr(
        sys,
        "argv",
        ["cli.py", "analyze", "--input", str(generated_csv), "--window", "20", "--json"],
    )
    main()
    analyze_out = capsys.readouterr().out
    payload = json.loads(analyze_out)

    assert payload["total_rows"] == 500
    assert payload["valid_rows"] + payload["corrupted_rows"] == 500
    assert "SUCCESS" in payload["status_counts"]
    assert isinstance(payload["outage_windows"], list)

def test_cli_benchmark_outputs_throughput(tmp_path: Path, monkeypatch, capsys):
    bench_csv = tmp_path / "bench_test.csv"

    monkeypatch.setattr(
        sys,
        "argv",
        ["cli.py", "generate", "--rows", "200", "--output", str(bench_csv)],
    )
    main()
    capsys.readouterr() # Clear generate output

    monkeypatch.setattr(
        sys,
        "argv",
        ["cli.py", "benchmark", "--input", str(bench_csv), "--top", "5"],
    )
    main()
    bench_out = capsys.readouterr().out

    assert "Processed 200 rows in" in bench_out
    assert "rows/sec" in bench_out