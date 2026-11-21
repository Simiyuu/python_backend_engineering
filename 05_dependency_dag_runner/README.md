# Project 05: Workflow DAG Orchestrator & Topological Task Runner

A directed acyclic graph (DAG) pipeline scheduler built with Kahn's $0(V + E)$ topological sort algorithm for multi-stage data ingestion and reconciliation workflows.

## Key Engineering Concepts
- **Deterministic Topological Ordering (`src/dag_runner.py`):** Uses Kahn's BFS in-degree reduction (`collections.deque`) with lexicographical tie-breaking to resolve valid linear execution plans in $0(V + E)$ time.
- **Parallel Execution Wave Planner (`resolve_parallel_stages`):** Partitiond tasks by dependency depth into concurrent execution stages (`list[list[str]]`).
- **Fail-Fast Cycle & Graph Validation:** Detects circular dependencies (`CyclicDependencyError`) and missing upstream nodes (`UnknownDependencyError`) prior to invokingany task callable.

## Usage
```bash
# Execute unit and DAG orchestration tests
pytest -v
