from __future__ import annotations

import json
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(r"D:\projects\kernelagent\runtime")
HARNESS = ROOT / "harness" / "releases" / "N0_AUTONOMOUS_INFORMATION_ACQUISITION_R6_1_FORMAL_V1"
R5_INTEGRATION = ROOT / "integrations" / "N0_PRODUCTION_INTEGRATION_R11_ONLINE_BEST_FEEDBACK"
FIXTURES = Path(__file__).resolve().parent / "fixtures"
sys.path[:0] = [str(HARNESS), str(R5_INTEGRATION), str(Path(__file__).resolve().parents[1])]
import formal_runtime as formal
from n0_autonomous.parallel_runtime import N0PersistentQueue
from n0_autonomous.engine import canonical_bytes, sha256_bytes
from n0_integration.oj_r5_worker import candidate_compile_diagnostic


class V11InitializationTests(unittest.TestCase):
    def test_compile_error_is_attributed_from_wrapped_compiler_output(self):
        import subprocess
        inner = subprocess.CalledProcessError(2, ["nvcc"],
            output=b"candidate.cu(67): error: expected a declaration")
        outer = RuntimeError("Error building extension 'n0_candidate'")
        outer.__cause__ = inner
        result = candidate_compile_diagnostic(outer)
        self.assertEqual(result["category"], "CANDIDATE_COMPILE_FAILURE")
        self.assertEqual(result["line"], 67)
        self.assertIn("expected a declaration", result["compiler_diagnostic"])

    def test_three_valid_and_one_hold_commit_once_without_budget_or_provider_requests(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            queue_root = root / "queue"
            store = N0PersistentQueue(queue_root)
            files = {name: (FIXTURES / name).read_bytes() for name in
                     ("task_definition.json", "candidate_abi.json", "visible_observation.json")}
            files["candidate.cu"] = (FIXTURES / "candidate.cu").read_bytes()
            for slot, episode_id in enumerate(formal.EPISODE_IDS, 1):
                allocation = {"authorized": True, "episode_id": episode_id, "replicate_slot": f"SYNTH-{slot}"}
                paths = store.register_episode(episode_id=episode_id, replicate_slot=f"SYNTH-{slot}",
                    treatment_id="N0_AUTONOMOUS", approved_allocation=allocation, max_candidates=30,
                    workspace_files=files)
                request_id = f"SYNTH-C0000-{slot}"
                is_hold = slot == 4
                receipt = {"schema": "N0_OJ_R5_IMMUTABLE_RECEIPT_V1", "request_id": request_id,
                    "episode_id": episode_id, "candidate_id": "C0000", "candidate_sha256": formal.SEED_SHA.lower(),
                    "task_sha256": "8D975D7CEE6946513DCFB6BA84C971E53A890F9D9C6F0A4FB828904941F25975",
                    "release_id": "N0_DIAGNOSTIC_FIXTURE", "manifest_sha256": "a" * 64,
                    "measurement_protocol": "N0_DIAGNOSTIC_FIXTURE_V1", "gpu_identity": "RTX5060_DIAGNOSTIC",
                    "terminal": True,
                    "verdict": "OJ_INFRASTRUCTURE_HOLD_REFERENCE_CONTROL_MISMATCH" if is_hold else "VALID_MEASUREMENT",
                    "score": None if is_hold else 1.0 + slot / 100,
                    "profile": {"status": "UNAVAILABLE" if is_hold else "PASS"},
                    "measurement_evidence": {"reference_trust": {"pass": False if is_hold else True,
                        "limit": 0.02, "policy":"N0_POST_EVALUATOR_REFERENCE_PAIRED_V2",
                        "relative_difference": 0.061 if is_hold else 0.01}},
                    "timing": {"status": "DIAGNOSTIC_FIXTURE", "samples_ms": [1.0, 1.0, 1.0]},
                    "diagnostics": {"status": "DIAGNOSTIC_FIXTURE"},
                    "lineage": {"version": "SYNTHETIC_TEST_ONLY"}}
                receipt["receipt_sha256"] = sha256_bytes(canonical_bytes(receipt))
                receipt_sha256 = sha256_bytes(canonical_bytes(receipt))
                src_path = Path("episodes") / episode_id / "archive" / "candidates" / ("C0000-" + formal.SEED_SHA.lower() + ".cu")
                archive = queue_root / src_path
                archive.parent.mkdir(parents=True, exist_ok=True)
                archive.write_bytes(files["candidate.cu"])
                db = store._connect()
                try:
                    db.execute("UPDATE episodes SET state='INFRASTRUCTURE_HOLD' WHERE episode_id=?", (episode_id,))
                    db.execute("INSERT INTO oj_requests(request_id,episode_id,invocation_id,candidate_id,candidate_sha256,state,queue_sequence,receipt_sha256,receipt_json,candidate_source_path,created_at,updated_at) VALUES(?,?,?,?,?,'COMPLETED',?,?,?,?,?,?)",
                        (request_id, episode_id, "FORMAL_INIT_C0000", "C0000", formal.SEED_SHA.lower(),
                         slot, receipt_sha256, canonical_bytes(receipt).decode("utf-8"), src_path.as_posix(), 1.0, 1.0))
                finally:
                    db.close()
            init_path = root / "N0_FORMAL_INITIALIZATION_V1.json"
            init_path.write_text(json.dumps({"schema": "N0_FORMAL_INITIALIZATION_V1", "state": "INITIALIZATION_HOLD",
                "episode_ids": formal.EPISODE_IDS, "seed_sha256": formal.SEED_SHA, "candidate_budget": 30,
                "protocol_sha256": json.loads(formal.PROTOCOL_PATH.read_bytes().decode("utf-8"))["parent_protocol"]["sha256"],
                "agent_requests_permitted": False, "readiness_report_sha256": "SYNTH"}, indent=2), encoding="utf-8")
            with patch.object(formal, "QUEUE_ROOT", queue_root), patch.object(formal, "INIT_PATH", init_path):
                result = formal.settle_initialization()
                self.assertEqual(result["state"], "INITIALIZATION_HOLD")
                self.assertFalse(result["agent_requests_permitted"])
                self.assertEqual(len(result["c0000"]), 4)
                rep04 = next(x for x in result["c0000"] if x["episode_id"] == "N0_FORMAL_REP04")
                self.assertEqual(rep04["initialization_status"], "C0000_BASELINE_SCORE_UNAVAILABLE_INFRA_HOLD")
                self.assertIsNone(rep04["score"])
                before = store.snapshot()
                self.assertEqual(len(before["provider_requests"]), 0)
                self.assertEqual([x["state"] for x in before["oj_requests"]], ["CONSUMED"] * 4)
                self.assertTrue(all(x["candidates_used"] == 0 and x["max_candidates"] == 30 for x in before["episodes"]))
                again = formal.settle_initialization()
                after = store.snapshot()
                self.assertEqual(again["c0000"], result["c0000"])
                self.assertEqual(len(after["oj_requests"]), 4)
                self.assertEqual(len(after["provider_requests"]), 0)
                for episode_id in formal.EPISODE_IDS:
                    observation = json.loads((store.episode_paths(episode_id).workspace / "visible_observation.json").read_text(encoding="utf-8"))
                    self.assertEqual(observation["candidate_id"], "C0000")
                    if episode_id == "N0_FORMAL_REP04":
                        self.assertEqual(observation["classification"], "CONTROLLER_ONLY_INFRASTRUCTURE_HOLD")
                        self.assertIsNone(observation["score"])
                    brief_records = formal.formal_candidates(store, episode_id)
                    from n0_autonomous.engine import build_history_brief
                    brief = json.loads(build_history_brief({"episode_id": episode_id, "task_id": "KB_L1_40_LAYERNORM",
                        "current_candidate_id": "C0001", "workspace_files": {n: (store.episode_paths(episode_id).workspace / n).read_text(encoding="utf-8")
                            for n in ("task_definition.json", "candidate_abi.json", "candidate.cu", "visible_observation.json")},
                        "candidates": brief_records, "unsettled_requests": [], "receipt_conflicts": []}, candidate_budget=30))
                    self.assertEqual(len(brief["settled_candidates"]), 1)

    def test_candidate_reference_hold_is_neutral_and_non_scoring(self):
        receipt = {"verdict": "OJ_INFRASTRUCTURE_HOLD_REFERENCE_CONTROL_MISMATCH", "score": None,
            "measurement_evidence": {"reference_trust": {"pass": False, "limit": 0.02,
              "policy":"N0_POST_EVALUATOR_REFERENCE_PAIRED_V2", "relative_difference": 0.06168}}}
        self.assertTrue(formal._eligible_reference_control_hold(receipt, candidate_id="C0001"))
        self.assertTrue(formal._eligible_reference_control_hold(receipt, candidate_id="C0000"))
        receipt["verdict"] = "OJ_INFRASTRUCTURE_HOLD_CUDA_CONTEXT_LOST"
        self.assertFalse(formal._eligible_reference_control_hold(receipt, candidate_id="C0001"))
        receipt["verdict"] = "OJ_INFRASTRUCTURE_HOLD_REFERENCE_CONTROL_MISMATCH"
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = N0PersistentQueue(root / "queue")
            files = {name: (FIXTURES / name).read_bytes() for name in
                     ("task_definition.json", "candidate_abi.json", "visible_observation.json")}
            files["candidate.cu"] = (FIXTURES / "candidate.cu").read_bytes()
            store.register_episode(episode_id="N0SYNTH-HOLD", replicate_slot="SYNTH-HOLD",
                treatment_id="N0_AUTONOMOUS", approved_allocation={"authorized": True,
                    "episode_id": "N0SYNTH-HOLD", "replicate_slot": "SYNTH-HOLD"}, max_candidates=30,
                workspace_files=files)
            row = {"candidate_id": "C0001", "candidate_sha256": formal.SEED_SHA, "request_id": "SYNTH-C1",
                   "receipt_sha256": "b"*64}
            hidden = {**receipt, "request_id": "SYNTH-C1", "episode_id": "N0SYNTH-HOLD", "candidate_id": "C0001",
                "candidate_sha256": formal.SEED_SHA, "task_sha256": "8D975D7CEE6946513DCFB6BA84C971E53A890F9D9C6F0A4FB828904941F25975",
                "diagnostics": {"internal_reference_drift_percent": 6.168}, "score": None}
            observation = formal._write_observation(store, "N0SYNTH-HOLD", row, hidden, "NOT_PROMOTED", "f"*64)
            self.assertEqual(observation["classification"], "CONTROLLER_ONLY_INFRASTRUCTURE_HOLD")
            self.assertIsNone(observation["score"])
            self.assertNotIn("internal_reference_drift_percent", json.dumps(observation))

    def test_diagnostic_feedback_is_never_labeled_as_formal_science(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = N0PersistentQueue(root / "queue")
            files = {name: (FIXTURES / name).read_bytes() for name in
                     ("task_definition.json", "candidate_abi.json", "visible_observation.json")}
            files["candidate.cu"] = (FIXTURES / "candidate.cu").read_bytes()
            store.register_episode(episode_id="N0DIAG-CLASSIFICATION", replicate_slot="DIAG-CLASSIFICATION",
                treatment_id="N0_AUTONOMOUS", approved_allocation={"authorized": True,
                    "episode_id": "N0DIAG-CLASSIFICATION", "replicate_slot": "DIAG-CLASSIFICATION"},
                max_candidates=30, workspace_files=files)
            receipt = {"verdict":"VALID_MEASUREMENT", "score":1.25,
                "promotion_confidence_status":"UNCERTAIN", "request_id":"DIAG-REQUEST",
                "candidate_id":"C0001", "candidate_sha256":formal.SEED_SHA}
            row = {"request_id":"DIAG-REQUEST", "candidate_id":"C0001",
                "candidate_sha256":formal.SEED_SHA, "receipt_sha256":"a"*64}
            feedback = formal._write_observation(store, "N0DIAG-CLASSIFICATION", row, receipt,
                "NOT_PROMOTED_UNCERTAIN", "b"*64, "UNCERTAIN")
            self.assertEqual(feedback["classification"], "NON_SCIENTIFIC_GPU_DIAGNOSTIC")
            self.assertEqual(feedback["score"], 1.25)
            self.assertEqual(feedback["promotion"], "NOT_PROMOTED_UNCERTAIN")
            self.assertEqual(feedback["promotion_confidence_status"], "UNCERTAIN")

    def test_candidate_compile_failure_feedback_preserves_error_and_has_no_score(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = N0PersistentQueue(root / "queue")
            files = {name:(FIXTURES / name).read_bytes() for name in
                     ("task_definition.json", "candidate_abi.json", "visible_observation.json")}
            files["candidate.cu"] = (FIXTURES / "candidate.cu").read_bytes()
            store.register_episode(episode_id="N0DIAG-COMPILE-FAIL", replicate_slot="DIAG-COMPILE-FAIL",
                treatment_id="N0_AUTONOMOUS", approved_allocation={"authorized":True,
                    "episode_id":"N0DIAG-COMPILE-FAIL","replicate_slot":"DIAG-COMPILE-FAIL"},
                max_candidates=30, workspace_files=files)
            row = {"request_id":"COMPILE-FAIL-1","candidate_id":"C0001",
                   "candidate_sha256":formal.SEED_SHA,"receipt_sha256":"a"*64}
            diagnostic = {"category":"CANDIDATE_COMPILE_FAILURE","source":"candidate.cu",
                          "line":42,"compiler_diagnostic":"candidate.cu(42): error: undefined symbol"}
            receipt = {"request_id":"COMPILE-FAIL-1","episode_id":"N0DIAG-COMPILE-FAIL",
                "candidate_id":"C0001","candidate_sha256":formal.SEED_SHA,
                "verdict":"CANDIDATE_COMPILE_FAILURE","score":None,
                "candidate_failure_diagnostics":diagnostic,"diagnostics":{"candidate_failure":diagnostic}}
            observation = formal._write_observation(store,"N0DIAG-COMPILE-FAIL",row,receipt,
                "NOT_PROMOTED","f"*64,"UNCERTAIN")
            self.assertIsNone(observation["score"])
            self.assertEqual(observation["candidate_state"],"CANDIDATE_COMPILE_FAILURE")
            self.assertEqual(observation["receipt"]["candidate_failure_diagnostics"],diagnostic)


if __name__ == "__main__":
    unittest.main()
