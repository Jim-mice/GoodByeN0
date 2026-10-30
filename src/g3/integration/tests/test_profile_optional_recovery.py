from __future__ import annotations
import hashlib, json, sys, tempfile, unittest
from pathlib import Path
from unittest import mock

ROOT = Path(r"D:\projects\kernelagent\runtime")
RELEASE = ROOT / "integrations" / "N0_PRODUCTION_INTEGRATION_R8_3_MEASUREMENT_HOLD_CONTINUATION"
FORMAL_RELEASE = ROOT / "integrations" / "N0_PRODUCTION_INTEGRATION_R12_9_POOL20_R5_R7_3_HARNESS"
HARNESS = ROOT / "harness" / "releases" / "N0_AUTONOMOUS_INFORMATION_ACQUISITION_R6_1_FORMAL_V1"
sys.path[:0] = [str(FORMAL_RELEASE), str(RELEASE), str(HARNESS)]
from formal_runtime import _eligible_profile_only_hold, _eligible_raw_timer_evidence_hold
import formal_runtime
from n0_integration.ncu_profile import METRICS, NcuProfileRunner, unit_contract_mismatch, normalize_profile_value
from n0_integration.oj_r5_worker import run_profile_safely, validate_r5_receipt, diagnostic_profile_suppressed
from n0_autonomous.engine import canonical_bytes
from run_r5_oj_worker import run_post_receipt_profile


class ProfileOptionalRecoveryTests(unittest.TestCase):
    def test_harness_projects_separate_successful_recovery_without_rewriting_profile_receipt(self):
        request_id = "N0-OJ-recovery-projection-test"
        candidate_sha = "a" * 64
        oj_sha = "b" * 64
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            profile_root = root / "state" / "n0_oj_profiles" / "profile_receipts"
            recovery_root = root / "state" / "n0_oj_profiles" / "profile_recovery_receipts"
            profile_root.mkdir(parents=True)
            recovery_root.mkdir(parents=True)
            profile = {"request_id":request_id,"candidate_sha256":candidate_sha,
                "oj_receipt_sha256":oj_sha,"status":"PASS","recovery_status":"HOLD",
                "profile":{"status":"PASS","score_inclusion":False}}
            profile["profile_receipt_sha256"] = hashlib.sha256(canonical_bytes(profile)).hexdigest()
            profile_path = profile_root / (request_id + ".json")
            profile_raw = canonical_bytes(profile) + b"\n"
            profile_path.write_bytes(profile_raw)
            recovery = {"recovery_id":"recovery-1","parent_profile_receipt_id":request_id,
                "parent_profile_receipt_sha256":profile["profile_receipt_sha256"],"status":"PASS"}
            recovery["recovery_receipt_sha256"] = hashlib.sha256(canonical_bytes(recovery)).hexdigest()
            (recovery_root / "recovery-1.json").write_bytes(canonical_bytes(recovery) + b"\n")
            row = {"request_id":request_id,"candidate_sha256":candidate_sha,"receipt_sha256":oj_sha}
            receipt = {"profile":{"status":"QUEUED"}}
            with mock.patch.object(formal_runtime, "ROOT", root):
                projected = formal_runtime._wait_post_receipt_profile(row, receipt, timeout_seconds=0.2)
            self.assertEqual(profile_path.read_bytes(), profile_raw)
            self.assertEqual(projected["profile_original_recovery_status"], "HOLD")
            self.assertEqual(projected["recovery_status"], "PASS")
            self.assertEqual(projected["recovery_receipt_projection"]["recovery_receipt_sha256"],
                             recovery["recovery_receipt_sha256"])

    def test_profile_suppression_is_limited_to_explicit_diagnostic_episode(self):
        with mock.patch.dict("os.environ", {"N0_DISABLE_DIAGNOSTIC_PROFILE":"1"}):
            self.assertTrue(diagnostic_profile_suppressed({"episode_id":"N0DIAG-control"}))
            self.assertFalse(diagnostic_profile_suppressed({"episode_id":"N0_MANUAL_FORMAL"}))
        with mock.patch.dict("os.environ", {}, clear=True):
            self.assertFalse(diagnostic_profile_suppressed({"episode_id":"N0DIAG-control"}))

    def test_raw_timer_evidence_hold_can_continue_only_as_unscored_measurement(self):
        receipt = {"verdict":"OJ_INFRASTRUCTURE_HOLD_RAW_TIMER_EVIDENCE_INVALID", "score":None,
                   "terminal":True, "diagnostics":{"status":"OJ_INFRASTRUCTURE_HOLD_RAW_TIMER_EVIDENCE_INVALID",
                   "official_evaluator":{"compiled":True,"correctness":True}}}
        self.assertTrue(_eligible_raw_timer_evidence_hold(receipt, candidate_id="C0002"))
        self.assertFalse(_eligible_raw_timer_evidence_hold(receipt, candidate_id="C0000"))
        receipt["diagnostics"]["official_evaluator"]["correctness"] = False
        self.assertFalse(_eligible_raw_timer_evidence_hold(receipt, candidate_id="C0002"))

    def test_synthetic_three_profile_holds_are_eligible_without_production_database(self):
        for index in range(3):
            receipt = {"verdict": "OJ_INFRASTRUCTURE_HOLD_PROFILE_UNAVAILABLE", "score": None,
                "profile": {"status": "HOLD", "identity_validation": "PASS"},
                "measurement_evidence": {"reference_trust": {"pass": True, "limit": 0.02, "policy":"N0_POST_EVALUATOR_REFERENCE_PAIRED_V2"}},
                "official_kernelbench_result": {"correctness": True},
                "timing": {"candidate_runtime_ms": 1.25 + index / 100,
                    "kernelbench_reference_runtime_ms": 7.3, "samples_ms": [1.24, 1.25, 1.26]}}
            self.assertTrue(_eligible_profile_only_hold(receipt, candidate_id=f"C{index + 1:04d}"))

    def test_profile_unit_prefixes_are_accepted_but_wrong_units_are_not(self):
        expected = {metric: ("byte" if metric == "dram__bytes.sum" else
                             "ms" if metric == "gpu__time_duration.sum" else "unit") for metric in METRICS}
        observed = dict(expected)
        observed["dram__bytes.sum"] = "Mbyte"
        self.assertEqual(unit_contract_mismatch(observed, expected), {})
        observed["gpu__time_duration.sum"] = "Kbyte"
        self.assertIn("gpu__time_duration.sum", unit_contract_mismatch(observed, expected))

    def test_ncu_duration_ms_us_normalizes_to_frozen_whitelist_unit(self):
        expected = {metric: ("byte" if metric == "dram__bytes.sum" else
                             "ms" if metric == "gpu__time_duration.sum" else "unit") for metric in METRICS}
        observed = dict(expected)
        observed["gpu__time_duration.sum"] = "us"
        self.assertEqual(unit_contract_mismatch(observed, expected), {})
        self.assertAlmostEqual(normalize_profile_value("gpu__time_duration.sum", "1250", "us", "ms"), 1.25)
        self.assertAlmostEqual(normalize_profile_value("gpu__time_duration.sum", "1.25", "ms", "ms"), 1.25)

    def test_profile_kernel_filter_uses_actual_build_kernel_names(self):
        runner = object.__new__(NcuProfileRunner)
        pattern = runner._kernel_pattern({"kernel_names": ["n0_reduce_kernel", "n0_finalize_kernel", "n0_norm_kernel"]})
        for name in ("n0_reduce_kernel", "n0_finalize_kernel", "n0_norm_kernel"):
            self.assertIn(name, pattern)

    def test_valid_measurement_remains_valid_when_profile_is_unavailable(self):
        request = {"request_id": "r", "episode_id": "e", "candidate_id": "C0001", "candidate_sha256": "a" * 64}
        receipt = {**request, "verdict": "VALID_MEASUREMENT", "score": 1.2,
                   "profile": {"status": "UNAVAILABLE", "reason": "NCU_PROFILE_FAILED"},
                   "measurement_evidence": {"reference_trust": {"pass": True, "limit": 0.02, "policy":"N0_POST_EVALUATOR_REFERENCE_PAIRED_V2"}}}
        self.assertTrue(validate_r5_receipt(request, receipt))
        receipt["score"] = None
        self.assertFalse(validate_r5_receipt(request, receipt))

    def test_queued_profile_does_not_enter_oj_score_and_requires_terminal_receipt(self):
        request = {"request_id":"req-profile-queued","episode_id":"N0DIAG-test","candidate_id":"C0001",
                   "candidate_sha256":"a"*64}
        receipt = {**request,"verdict":"VALID_MEASUREMENT","score":1.5,
                   "profile":{"status":"QUEUED","profile_receipt_id":request["request_id"],"score_inclusion":False},
                   "measurement_evidence":{"reference_trust":{"pass":True,"limit":0.02,
                       "policy":"N0_POST_EVALUATOR_REFERENCE_PAIRED_V2"}}}
        self.assertTrue(validate_r5_receipt(request, receipt))
        class Store:
            def snapshot(self):
                return {"oj_requests":[{**request,"state":"RUNNING","receipt_json":None,"receipt_sha256":None}]}
        with tempfile.TemporaryDirectory() as temp:
            job_root=Path(temp)/"profile_jobs"
            job_root.mkdir()
            (job_root/(request["request_id"]+".json")).write_text(json.dumps({"request":request}),encoding="utf-8")
            result=run_post_receipt_profile(Store(),Path(temp),None,Path(temp)/"gpu.lock",{}, {})
            self.assertIsNone(result)

    def test_profile_runner_exception_is_non_scoring_metadata_not_worker_failure(self):
        request = {"request_id": "r", "episode_id": "e", "candidate_id": "C0001"}
        result = run_profile_safely(lambda *_: (_ for _ in ()).throw(PermissionError("fixture")), request, "src", {}, None)
        self.assertEqual(result["status"], "UNAVAILABLE")
        self.assertEqual(result["reason"], "PROFILE_RUNNER_EXCEPTION")
        self.assertEqual(result["exception_type"], "PermissionError")

    def test_observation_recovery_archives_prior_content_without_overwriting_initial_archive(self):
        class Paths:
            pass
        class Store:
            pass
        with tempfile.TemporaryDirectory() as td:
            paths = Paths()
            paths.workspace = Path(td) / "workspace"
            paths.archive = Path(td) / "archive"
            paths.workspace.mkdir()
            initial_archive = paths.archive / "initial" / "visible_observation.initial.json"
            initial_archive.parent.mkdir(parents=True)
            initial_raw = b'{"template":"immutable initial archive"}\n'
            initial_archive.write_bytes(initial_raw)
            old_path = paths.workspace / "visible_observation.json"
            old_raw = b'{"episode_id":"e","candidate_id":"C0000","score":1.0}\n'
            old_path.write_bytes(old_raw)
            store = Store()
            store.episode_paths = lambda _episode_id: paths
            receipt = {"request_id":"r1","episode_id":"e","candidate_id":"C0001",
                       "candidate_sha256":"a"*64,"task_sha256":"b"*64,
                       "verdict":"OJ_INFRASTRUCTURE_HOLD_PROFILE_UNAVAILABLE","score":None,
                       "profile":{"reason":"PRIVATE_NCU_DETAIL"}}
            row = {"candidate_id":"C0001","candidate_sha256":"a"*64,"request_id":"r1",
                   "receipt_sha256":"c"*64}
            with mock.patch.object(formal_runtime, "append_stable_event", lambda *args, **kwargs: None):
                observation = formal_runtime._write_observation(store, "e", row, receipt, "NOT_PROMOTED", "d"*64)
                formal_runtime._write_observation(store, "e", row, receipt, "NOT_PROMOTED", "d"*64)
            archived = paths.archive / "observations" / ("visible_observation." + hashlib.sha256(old_raw).hexdigest().lower() + ".json")
            self.assertEqual(archived.read_bytes(), old_raw)
            self.assertEqual(initial_archive.read_bytes(), initial_raw)
            self.assertIsNone(observation["score"])
            self.assertNotIn("PRIVATE_NCU_DETAIL", old_path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
