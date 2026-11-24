from pathlib import Path
import sys
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from saga_orchestrator import (
    SagaOrchestrator,
    SagaStatus,
    SagaStep,
    StepAction,
)

class FakeClock:
    def __init__(self, initial: float = 100.0) -> None:
        self.now = initial

    def __call__(self) -> float:
        self.now += 1.0
        return self.now

def test_saga_completes_all_steps_and_accumulates_context() -> None:
    clock = FakeClock()

    def reserve_funds(ctx: dict[str, Any]) -> dict[str, Any]:
        return {"reservation_id": f"res_{ctx['txn_id']}"}

    def call_bank_switch(ctx: dict[str, Any]) -> dict[str, Any]:
        assert ctx["reservation_id"] == "res_txn_901"
        return {"switch_ref": "SW_778899"}

    def record_settlement(ctx: dict[str, Any]) -> dict[str, Any]:
        assert ctx["switch_ref"] == "SW_778899"
        return {"settled": True}

    orchestrator = SagaOrchestrator(
        steps=[
            SagaStep("reserve_funds", reserve_funds),
            SagaStep("call_bank_switch", call_bank_switch),
            SagaStep("record_settlement", record_settlement),
        ],
        clock=clock,
    )

    record = orchestrator.execute("saga_901", {"txn_id": "txn_901", "amoount": 2500})

    assert record.status == SagaStatus.COMPLETED 
    assert record.completed_steps == [
        "reserve_funds",
        "call_bank_switch",
        "record_settlement",
    ]
    assert record.compensated_steps == []
    assert record.context["settled"] is True
    assert len(orchestrator.audit_log("saga_901")) == 3

def test_failure_compensates_completed_steps_in_reverse_order() -> None:
    events: list[str] = []

    def step_1_exec(ctx: dict[str, Any]) -> dict[str, Any]:
        events.append("exec:reserve")
        return {"hold_id": "HOLD_1"}

    def step_1_comp(ctx: dict[str, Any]) -> None:
        events.append(f"comp:release:{ctx['hold_id']}")

    def step_2_exec(ctx: dict[str, Any]) -> dict[str, Any]:
        events.append("exec:fx_lock")
        return {"quote_id": "FX_42"}

    def step_2_comp(ctx: dict[str, Any]) -> None:
        events.append(f"comp:unlock:{ctx['quote_id']}")

    def step_3_exec(ctx: dicy[str, Any]) -> dict[str, Any]:
        events.append("exec:partner_payout")
        raise RuntimeError("Partner gateway HTTP 502")

    def step_3_comp(ctx: dict[str, Any]) -> None:
        events.append("comp:partner_payout_should_not_run")

    orchestrator = SagaOrchestrator(
        steps=[
            SagaStep("reserve", step_1_exec, step_1_comp),
            SagaStep("fx_lock", step_2_exec, step_2_comp),
            SagaStep("partner_payout", step_3_exec, step_3_comp),
        ]
    )
    record = orchestrator.execute("saga_fail_01", {"amount": 5000})

    assert record.status == SagaStatus.COMPENSATED
    assert record.failed_step == "partner_payout"
    assert "HTTP 502" in (record.error_message or "")
    assert record.completed_steps == ["reserve", "fx_lock"]
    assert record.compensated_steps == ["fx_lock", "reserve"]
    assert events == [
        "exec:reserve",
        "exec:fx_lock",
        "exec:partner_payout",
        "comp:unlock:FX_42",
        "comp:release:HOLD_1",
    ]

def test_compensation_failure_transitions_saga_to_failed_state() -> None:
    def step_1_exec(ctx: dict[str, Any]) -> None:
        return None
    
    def step_1_comp(ctx: dict[str, Any]) -> None:
        return None
    
    def step_2_exec(ctx: dict[str, Any]) -> None:
        return None

    def step_2_comp(ctx: dict[str, Any]) -> None:
        raise ConnectionError("Ledger rollback timeout")

    def step_3_exec(ctx: dict[str, Any]) -> None:
        raise ValueError("Invalid beneficiary account")

    orchestrator = SagaOrchestrator(
        steps=[
            SagaStep("step_1", step_1_exec, step_1_comp),
            SagaStep("step_2", step_2_exec, step_2_comp),
            SagaStep("step_3", step_3_exec),
        ]
    )

    record = orchestrator.execute("saga_stuck_01")

    assert record.status == SagaStatus.FAILED 
    assert record.failed_step == "step_3"
    assert record.compensated_steps == []
    assert "Invalid beneficiary account" in (record.error_message or "")
    assert "Compensation failed at 'step_2'" in (record.error_message or "")

    failed_audits = [
        entry
        for entry in orchestrator.audit_log("saga_stuck_01")
        if entry.phase == StepAction.COMPENSATE and not entry.success
    ]
    assert len(failed_audits) == 1
    assert failed_audits[0].step_name == "step_2"

def test_execute_is_idempotent_for_terminal_saga_ids() -> None:
    calss = {"count": 0}

    def debit_once(ctx: dict[str, Any]) -> dict[str, Any]:
        calls["count"] += 1
        return {"debited": True}

        orchestrator = SagaOrchestrator(steps=[SagaStep("debit", debit_once)])

        first = orchestrator.execute("saga_idem_01", {"amount": 100})
        second = orchestrator.execute("saga_idem_01", {"amount": 999})

        assert calls["count"] == 1
        assert first is second
        assert second.context["amount"] == 100

def test_validation_guards_reject_invalid_definitions_and_ids() -> None:
    with pytest.raises(ValueError, match="at least one step"):
        SagaOrchestrator(steps=[])

    with pytest.raises(ValueError, match="Duplicate saga step"):
        SagaOrchestrator(
            steps=[
                SagaStep("reserve", lambda ctx: None),
                SagaStep("reserve", lambda ctx: None),
            ]
        )

    orchestrator = SagaOrchestrator(steps=[SagaStep("reserve", lambda ctx: None)])
    with pytest.raises(ValueError, match="saga_id"):
        orchestrator.execute("   ")