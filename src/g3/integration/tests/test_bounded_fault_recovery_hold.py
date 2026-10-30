from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

WORKER = Path(__file__).resolve().parents[1] / "run_r5_oj_worker.py"
SPEC = importlib.util.spec_from_file_location("r12_3_worker_under_test", WORKER)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class BoundedFaultRecoveryHoldTests(unittest.TestCase):
    def test_recovers_missing_gate_path_only_from_matching_isolated_queue(self):
        with tempfile.TemporaryDirectory() as tmp:
            queue = Path(tmp) / "queue"
            request_id = "N0-OJ-123456"
            receipt = queue / "candidate_supervision" / request_id / "fault_receipt.json"
            receipt.parent.mkdir(parents=True)
            receipt.write_text(json.dumps({"request_id": request_id}), encoding="utf-8")
            resolved = MODULE.resolve_fault_receipt_path({"fault_request_id": request_id}, queue)
            self.assertEqual(resolved, receipt.resolve())

    def test_rejects_request_path_traversal_and_missing_receipt(self):
        with tempfile.TemporaryDirectory() as tmp:
            queue = Path(tmp) / "queue"
            with self.assertRaisesRegex(ValueError, "INVALID_FAULT_REQUEST_ID"):
                MODULE.resolve_fault_receipt_path({"fault_request_id": "..\\outside"}, queue)
            with self.assertRaisesRegex(FileNotFoundError, "FAULT_RECEIPT_MISSING"):
                MODULE.resolve_fault_receipt_path({"fault_request_id": "N0-OJ-missing"}, queue)

    def test_failed_recovery_gate_retains_fault_identity_for_controller_retry(self):
        gate = {"fault_request_id": "N0-OJ-123456",
                "fault_receipt_path": "D:/isolated/queue/candidate_supervision/N0-OJ-123456/fault_receipt.json",
                "candidate_sha256": "a" * 64,
                "fault_classification": "CANDIDATE_CORRECTNESS_FAILURE"}
        result = MODULE.fault_recovery_gate_document(gate, "b" * 64, False,
                                                    "REFERENCE_DRIFT", "2026-10-24T10:00:00Z")
        self.assertEqual(result["status"], "HOLD")
        self.assertEqual(result["fault_request_id"], gate["fault_request_id"])
        self.assertEqual(result["fault_receipt_path"], gate["fault_receipt_path"])
        self.assertEqual(result["fault_classification"], gate["fault_classification"])

    def test_nonready_gate_exits_worker_in_hold_without_unbounded_retry(self):
        self.assertEqual(MODULE.worker_gate_action("READY"), "CONTINUE")
        for status in ("HOLD", "IN_PROGRESS", None, "UNKNOWN"):
            self.assertEqual(MODULE.worker_gate_action(status), "HOLD_AND_EXIT")


if __name__ == "__main__":
    unittest.main()
