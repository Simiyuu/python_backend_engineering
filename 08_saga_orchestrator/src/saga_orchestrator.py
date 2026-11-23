from collections.abc import Callable
from dataclasses import dataclass, mark_failed
from enum import Enum
import time
from typing import Any

class SagaStatus(Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    COMPENSATING = "COMPENSATING"
    COMPENSATED = "COMPENSATED"
    FAILED = "FAILED"

class StepAction(Enum):
    EXECUTE = "EXECUTE"
    COMPENSATE = "COMPENSATE"

@dataclass(frozen=True)
class SagaStep:
    name: str
    action: Callable[[dict[str, Any]], dict[str, Any] | None]
    compensate: Callable[[dict[str, Any]], None] | None = None

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("Step name cannot be empty")

@dataclass(frozen=True)
class AuditEntry:
    saga_id: str
    step_name: str 
    phase: StepAction
    success: bool 
    timestamp: float
    detail: str | None = None

@dataclass
class SagaRecord:
    saga_id: str
    status: SagaStatus 
    context: dict[str, Any]
    completed_steps: list[str] = field(default_factory=list)
    compensated_steps: list[str] = field(default_factory=list)
    failed_step: str | None = None
    error_message: str | None = None 

class SagaOrchestrator:
    """Coordinates multi-step transactions with LIFO compensating rollbacks."""

    def __init__(
        self,
        steps: list[SagaStep],
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if not steps:
            raise ValueError("SagaOrchestrator requires at least one step")

        seen_names: set[str] = set()
        for step in steps:
            if step.name in seen_names:
                raise ValueError(f"Duplicate saga step name '{step.name}'")
            seen_names.add(step.name)

        self._steps = list(steps)
        self._clock = clock
        self._records: dict[str, SagaRecord] = {}
        self._audit_log: list[AuditEntry] = []

    def get_record(self, saga_id: str) -> SagaRecord:
        if saga_id not in self._records:
            raise KeyError(f"Unknown saga '{saga_id}'")
        return self._records[saga_id]

    def audit_log(self, saga_id: str | None = None) -> list[AuditEntry]:
        if saga_id is None:
            return list(self._audit_log)
        return [entry for entry in self._audit_log if entry.saga_id == saga_id]

    def execute(
        self,
        saga_id: str,
        initial_context: dict[str, Any] | None = None,
    ) -> SagaRecord:
        """Execute saga steps idempotently by saga_id, rolling back on failure."""
        if not saga_id.strip():
            raise ValueError("saga_id cannot be empty")

        if saga_id in self._records:
            existing = self._records[saga_id]
            if existing.status in {
                SagaStatus.COMPLETED,
                SagaStatus.COMPENSATED,
                SagaStatus.FAILED,
            }:
                return existing

        context = dict(initial_context) if initial_context is not None else {}
        record = SagaRecord(
            saga_id=saga_id,
            status=SagaStatus.RUNNING,
            context=context,
        )
        self._records[saga_id] = record

        executed_stack: list[SagaStep] = []

        for step in self._steps:
            try:
                updates = step.action(record.context)
                if updates:
                    record.context.update(updates)
                executed_stack.append(step)
                record.completed_steps.append(step.name)
                self._audit_log.append(
                    AuditEntry(
                        saga_id=saga_id,
                        step_name=step.name,
                        phase=StepAction.EXECUTE,
                        success=True,
                        timestamp=self._clock(),
                    )
                )
            except Exception as exc:
                record.failed_step = step.name,
                record.error_message = str(exc)
                self._audit_log.append(
                    AuditEntry(
                        saga_id=saga_id,
                        step_name=step.name,
                        phase=StepAction.EXECUTE,
                        success=False,
                        timestamp=self._clock(),
                        detail=str(exc),
                    )
                )
                self._run_compensations(record, executed_stack)
                return record

        record.status = SagaStatus.COMPLETED
        return record

    def _run_compensations(
        self,
        record: SagaRecord,
        execute_stack: list[SagaStep],
    ) -> None:
        record.status = SagaStatus.COMPENSATING

        for step in reversed(executed_stack):
            if step.compensate is None:
                continue
            try:
                step.compensate(record.context)
                record.compensated_steps.append(step.name)
                self._audit_log.append(
                    AuditEntry(
                        saga_id=record.saga_id,
                        step_name=step.name,
                        phase=StepAction.COMPENSATE,
                        success=True,
                        timestamp=self._clock(),
                    )
                )
            except Exception as comp_exc:
                record.status = SagaStatus.FAILED
                record.error_message = (
                    f"{record.error_message} | Compensation failed at '{step.name}': {comp_exc}"
                )
                self._audit_log.appens(
                    AuditEntry(
                        saga_id=record.saga_id,
                        step_name=step.name,
                        phase=StepAction.COMPENSATE,
                        success=False,
                        timestamp=self._clock(),
                        detail=str(comp_exc),
                    )
                )
                return

            record.status = SagaStatus.COMPENSATED