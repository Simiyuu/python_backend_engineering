import sys
from pathlib import Path

import pytest

SRC_DIR = Path(__file__).resolve().parent.parent / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from dag_runner import (
    CyclicDependencyError,
    PipelineDAG,
    UnknownDependencyError,
)

def test_diamond_dag_topological_order_and_parallel_stages():
    dag = PipelineDAG()
    dag.add_task("ingest_cdr")
    dag.add_task("validate_voice", dependencies=["ingest_cdr"])
    dag.add_task("validate_mpesa", dependencies=["ingest_cdr"])
    dag.add_task("reconcile_ledger", dependencies=["validate_voice", "validate_mpesa"])
    dag.add_task("publish_report", dependencies=["reconcile_ledger"])

    assert dag.resolve_execution_order() == [
        "ingest_cdr",
        "validate_mpesa",
        "validate_voice",
        "reconcile_ledger",
        "publish_report",
    ]

    assert dag.resolve_parallel_stages() == [
        ["ingest_cdr"],
        ["validate_mpesa", "validate_voice"],
        ["reconcile_ledger"],
        ["publish_report"],
    ]

def test_execute_runs_callables_in_valid_dependency_order():
    execution_trace: list[str] = []
    dag = PipelineDAG()

    dag.add_task(
        "extract",
        action=lambda: execution_trace.append("extract") or "raw_rows=1000",
    )
    dag.add_task(
        "transform",
        dependencies=["extract"],
        action=lambda: execution_trace.append("transform") or "clean_rows=995",
    )
    dag.add_task(
        "load",
        dependencies=["transform"],
        action=lambda: execution_trace.append("load") or "committed",
    )

    outputs = dag.execute()

    assert execution_trace == ["extract", "transform", "load"]
    assert outputs == {
        "extract": "raw_rows=1000",
        "transform": "clean_rows=995",
        "load": "committed",
    }

def test_cycle_detection_blocks_execution_before_running_any_task():
    executed: list[str] = []
    dag = PipelineDAG()

    dag.add_task("healthy_root", action=lambda: executed.append("healthy_root") or "ok")
    dag.add_task("task_a", dependencies=["task_c"])
    dag.add_task("task_b", dependencies=["task_a"])
    dag.add_task("task_c", dependencies=["task_b"])

    with pytest.raises(CyclicDependencyError, match="task_a.*task_b.*task_c"):
        dag.resolve_execution_order()

    with pytest.raises(CyclicDependencyError):
        dag.resolve_parallel_stages()

    with pytest.raises(CyclicDependencyError):
        dag.execute()

    # Fail-fast guarantee: healthy_root must not execute when another subgraph has a cycle
    assert executed == []

def test_self_dependency_unknown_dependency_and_duplicate_tasks():
    dag = PipelineDAG()
    dag.add_task("step_1")

    with pytest.raises(ValueError, match="already registered"):
        dag.add_task("step_1")

    with pytest.raises(CyclicDependencyError, match="cannot depend on itself"):
        dag.add_task("self_loop", dependencies=["self_loop"])

    dag.add_task("dangling", dependencies=["non_existent_task"])
    with pytest.raises(UnknownDependencyError, match="non_existent_task"):
        dag.resolve_execution_order()
