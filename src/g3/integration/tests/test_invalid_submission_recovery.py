from __future__ import annotations

import unittest

from n0_integration import bootstrap_recovery as bootstrap
from n0_integration import service_supervisor as supervisor


class InvalidSubmissionRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.request = {
            "request_id": "req-invalid-1",
            "episode_id": "ep-1",
            "candidate_id": "C0003",
            "candidate_sha256": "a" * 64,
        }
        self.diagnostic = {"reason": "CUDA_HEADER_NOT_ALLOWLISTED:cooperative_groups.h",
                          "status": "INVALID_SUBMISSION"}
        self.receipt = {
            "schema": "N0_OJ_R5_IMMUTABLE_RECEIPT_V1",
            **self.request,
            "verdict": "INVALID_SUBMISSION",
            "score": None,
            "candidate_failure_diagnostics": self.diagnostic,
            "diagnostics": {"candidate_failure": self.diagnostic},
        }
        self.gate = {
            "schema": "N0_GPU_FAULT_RECOVERY_GATE_V1",
            "status": "HOLD",
            "reason": "CANDIDATE_PROCESS_TERMINATION_REQUIRES_DEVICE_REQUALIFICATION",
            "fault_request_id": self.request["request_id"],
            "fault_candidate_sha256": self.request["candidate_sha256"],
            "fault_classification": {"classification": "INVALID_SUBMISSION"},
        }
        self.fault = {
            "request_id": self.request["request_id"],
            "candidate_sha256": self.request["candidate_sha256"],
            "fault": {
                "schema": "N0_CUDA_FAULT_V1",
                "classification": "INVALID_SUBMISSION",
                "domain": "SUPERVISOR",
                "attribution": "UNRESOLVED",
                "stage": "POST_EVALUATION_RECOVERY_GATE",
                "score_allowed": False,
            },
            "worker_identity": {
                "request_id": self.request["request_id"],
                "candidate_sha256": self.request["candidate_sha256"],
                "episode_id": self.request["episode_id"],
            },
            "job_result": {"returncode": 0, "active_processes_after": 0,
                            "terminated_tree": False, "timed_out": False},
        }
        self.status = {"state": "HANDOFF_REQUIRED", "detail": {
            "shutdown": "FRESH_PROCESS_FAULT_RECOVERY_HANDOFF",
            "fault_request_id": self.request["request_id"],
        }}
        self.settled = {
            "request_id": self.request["request_id"],
            "episode_id": self.request["episode_id"],
            "candidate_id": self.request["candidate_id"],
            "candidate_sha256": self.request["candidate_sha256"],
            "state": "COMPLETED",
            "receipt_sha256": "b" * 64,
            "verdict": "INVALID_SUBMISSION",
            "score": None,
            "running_requests": 0,
            "candidate_failure_diagnostics": self.diagnostic,
        }

    def test_exact_scoreless_invalid_submission_is_recoverable(self):
        bootstrap._validate_fault_receipt_classification(
            self.gate, self.request, self.receipt, self.fault)
        self.assertEqual("PASS", supervisor.validate_fault_recovery_handoff(
            self.status, self.gate, self.settled, self.fault))

    def test_identity_or_diagnostic_mismatch_is_rejected(self):
        for mutate in (
            lambda: self.fault["worker_identity"].update(candidate_sha256="c" * 64),
            lambda: self.receipt["diagnostics"].update(candidate_failure={"status": "OTHER"}),
            lambda: self.gate["fault_classification"].update(classification="OTHER"),
        ):
            with self.subTest(mutate=mutate):
                self.setUp()
                mutate()
                with self.assertRaises(RuntimeError):
                    bootstrap._validate_fault_receipt_classification(
                        self.gate, self.request, self.receipt, self.fault)

    def test_service_handoff_rejects_score_or_live_descendant(self):
        self.settled["score"] = 1.0
        self.assertEqual("FAULT_HANDOFF_RECEIPT_CLASSIFICATION_MISMATCH",
                         supervisor.validate_fault_recovery_handoff(
                             self.status, self.gate, self.settled, self.fault))
        self.settled["score"] = None
        self.fault["job_result"]["active_processes_after"] = 1
        self.assertEqual("FAULT_HANDOFF_RECEIPT_CLASSIFICATION_MISMATCH",
                         supervisor.validate_fault_recovery_handoff(
                             self.status, self.gate, self.settled, self.fault))

if __name__ == "__main__":
    unittest.main()
