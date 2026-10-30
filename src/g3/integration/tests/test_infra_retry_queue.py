from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(r"D:\projects\kernelagent\runtime")
RELEASE = ROOT / "integrations" / "N0_PRODUCTION_INTEGRATION_R12_9_POOL20_R5_R7_3_HARNESS"
HARNESS = ROOT / "harness" / "releases" / "N0_AUTONOMOUS_INFORMATION_ACQUISITION_R6_1_FORMAL_V1"
FIXTURES = RELEASE / "tests" / "fixtures"
sys.path[:0] = [str(HARNESS), str(RELEASE)]
import formal_runtime
from n0_autonomous.parallel_runtime import N0PersistentQueue
from n0_integration.oj_r5_worker import candidate_compile_diagnostic


class InfrastructureRetryQueueTests(unittest.TestCase):
    def test_compile_failure_is_candidate_attributed_only_with_candidate_source_line(self):
        candidate = candidate_compile_diagnostic(RuntimeError("candidate.cu(42): error: identifier 'x' is undefined"))
        infrastructure = candidate_compile_diagnostic(RuntimeError("binding.cpp(42): error: host toolchain unavailable"))
        self.assertEqual(candidate["category"], "CANDIDATE_COMPILE_FAILURE")
        self.assertEqual(candidate["line"], 42)
        self.assertIsNone(infrastructure)

    def test_ptxas_candidate_generated_ptx_error_is_candidate_compile_failure(self):
        message = ("Error building extension: tmpxft_123_candidate.ptx, line 303; error : "
            "Instruction 'ld' requires vector type with .L2::evict_last modifier\n"
            "ptxas fatal   : Ptx assembly aborted due to errors")
        result = candidate_compile_diagnostic(RuntimeError(message))
        self.assertEqual(result["category"], "CANDIDATE_COMPILE_FAILURE")
        self.assertEqual(result["compiler_domain"], "PTXAS")
        self.assertEqual(result["compiler_location"]["line"], 303)
        self.assertEqual(result["attribution"], "PTXAS_REJECTED_CANDIDATE_GENERATED_PTX")

    def test_ptxas_error_without_candidate_ptx_remains_infrastructure(self):
        message = "ptxas fatal : internal tool error in system_file.ptx, line 8; error : failed"
        self.assertIsNone(candidate_compile_diagnostic(RuntimeError(message)))

    def test_one_retry_is_same_source_idempotent_and_budget_neutral(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            store = N0PersistentQueue(root / "queue")
            files = {name:(FIXTURES / name).read_bytes() for name in
                     ("task_definition.json", "candidate_abi.json", "visible_observation.json")}
            seed = (ROOT / "harness" / "releases" / "N0_AUTONOMOUS_INFORMATION_ACQUISITION_R6" /
                    "protocol" / "SEED_CANDIDATE.cu").read_bytes()
            files["candidate.cu"] = seed
            episode = "N0DIAG-RETRY-ISOLATED"
            store.register_episode(episode_id=episode, replicate_slot=episode, treatment_id="N0_AUTONOMOUS",
                approved_allocation={"authorized":True,"episode_id":episode,"replicate_slot":episode},
                max_candidates=30, workspace_files=files)
            source = (Path(r"D:\projects\kernelagent\manual_episode_200018\results\C0002_pending.cu")).read_text(encoding="utf-8")
            submitted = store.submit_continue(episode_id=episode, invocation_id="DIAG-CANDIDATE-1",
                current_source=source, frozen_identity_rule=lambda new, old:new != old)
            before = next(e["candidates_used"] for e in store.snapshot()["episodes"] if e["episode_id"] == episode)
            original = next(r for r in store.snapshot()["oj_requests"] if r["request_id"] == submitted["request_id"])
            hold = {"verdict":"OJ_INFRASTRUCTURE_HOLD_CANDIDATE_TIMING_UNSTABLE",
                    "candidate_sha256":original["candidate_sha256"],"score":None,
                    "measurement_evidence":{"candidate":{"summary":{"cv":0.111}}}}
            first = formal_runtime.retry_infrastructure_measurement_once(store, original, hold)
            second = formal_runtime.retry_infrastructure_measurement_once(store, original, hold)
            snap = store.snapshot()
            after = next(e["candidates_used"] for e in snap["episodes"] if e["episode_id"] == episode)
            retry = next(r for r in snap["oj_requests"] if r["request_id"] == first["request_id"])
            self.assertEqual(before, 1)
            self.assertEqual(after, before)
            self.assertEqual(first["candidate_budget_delta"], 0)
            self.assertIsNone(second)
            self.assertEqual(retry["candidate_sha256"], original["candidate_sha256"])
            self.assertEqual(retry["candidate_source_path"], original["candidate_source_path"])
            self.assertEqual(retry["state"], "QUEUED")
            event = store._connect()
            try:
                count = event.execute("SELECT COUNT(*) FROM episode_events WHERE event_id=?",
                                      ("oj-infra-retry:" + original["request_id"],)).fetchone()[0]
            finally:
                event.close()
            self.assertEqual(count, 1)


if __name__ == "__main__":
    unittest.main()
