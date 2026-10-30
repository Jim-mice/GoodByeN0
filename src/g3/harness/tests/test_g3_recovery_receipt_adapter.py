import hashlib
import json

import pytest

from n0_autonomous.g3_start_gate import adapt_post_profile_recovery_receipt


def _sha(value):
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest().upper()


def _fixture():
    receipt = {
        "schema": "N0_POST_PROFILE_TIMING_RECOVERY_RECEIPT_V1",
        "recovery_id": "SYNTH-RECOVERY",
        "status": "PASS",
        "recovery_limit": 0.02,
        "required_consecutive_controls": 3,
        "lease_id": "SYNTH-LEASE",
        "gpu_identity": {"gpu_uuid": "GPU-SYNTH"},
        "controls": [
            {"index": 1, "passed": False, "consecutive_passes": 0,
             "baseline_check": {"pass": False, "recovery_limit": 0.02, "baseline_id": "BASE"},
             "health_check": {"status": "PASS"}},
            *[{
                "index": index, "passed": True, "consecutive_passes": streak,
                "baseline_check": {"pass": True, "recovery_limit": 0.02, "baseline_id": "BASE"},
                "health_check": {"status": "PASS"},
            } for streak, index in enumerate((2, 3, 4), 1)],
        ],
    }
    receipt["recovery_receipt_sha256"] = _sha(receipt)
    file_sha = "A" * 64
    execution = {"integration": {"release_id": "R3-INTEGRATION", "manifest_sha256": "B" * 64},
                 "receipt_file_sha256": file_sha}
    termination = {"terminated_tree": True, "active_processes_after": 0, "returncode": 0,
                   "pid": 1234, "launch_generation": "launch-1"}
    expected = dict(execution["integration"])
    return receipt, file_sha, execution, expected, termination


def test_adapter_accepts_real_receipt_shape_only_with_parent_termination_witness():
    receipt, file_sha, execution, expected, termination = _fixture()
    adapted = adapt_post_profile_recovery_receipt(receipt=receipt, receipt_file_sha256=file_sha,
        execution_identity=execution, expected_integration_identity=expected, termination_witness=termination)
    assert adapted["status"] == "PASS"
    assert adapted["terminated_tree"] is True
    assert len(adapted["controls"]) == 3
    assert adapted["integration_manifest_sha256"] == "B" * 64
    assert adapted["source_recovery_id"] == "SYNTH-RECOVERY"


@pytest.mark.parametrize("mutator, error", [
    (lambda r, e, t: t.update(terminated_tree=False), "REFERENCE_RECOVERY_PROCESS_TREE_NOT_PROVEN_TERMINATED"),
    (lambda r, e, t: e["integration"].update(manifest_sha256="C" * 64), "REFERENCE_RECOVERY_EXECUTION_IDENTITY_MISMATCH"),
    (lambda r, e, t: r["controls"][-1].update(passed=False), "REFERENCE_RECOVERY_INTERNAL_SHA_MISMATCH"),
])
def test_adapter_rejects_identity_tree_and_receipt_tampering(mutator, error):
    receipt, file_sha, execution, expected, termination = _fixture()
    mutator(receipt, execution, termination)
    with pytest.raises(ValueError, match=error):
        adapt_post_profile_recovery_receipt(receipt=receipt, receipt_file_sha256=file_sha,
            execution_identity=execution, expected_integration_identity=expected, termination_witness=termination)


def test_adapter_rejects_failure_inside_final_three_controls():
    receipt, file_sha, execution, expected, termination = _fixture()
    receipt["controls"][-2]["health_check"]["status"] = "BLOCKED"
    receipt["recovery_receipt_sha256"] = _sha({k: v for k, v in receipt.items() if k != "recovery_receipt_sha256"})
    with pytest.raises(ValueError, match="REFERENCE_RECOVERY_CONTROL_INCOMPLETE"):
        adapt_post_profile_recovery_receipt(receipt=receipt, receipt_file_sha256=file_sha,
            execution_identity=execution, expected_integration_identity=expected, termination_witness=termination)
