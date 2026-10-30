from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from n0_integration.promotion_recheck import classify_paired_recheck, assess_best_promotion
from n0_integration.baseline_qualification import environment_fingerprint, validate_frozen_baseline
from n0_integration.oj_r5_worker import ExternalGpuMonitor
from n0_integration.reference_protocol import enforce_post_evaluator_control_limit
import run_r5_oj_worker
from unittest.mock import patch
from datetime import datetime, timezone, timedelta


def pairs(differences, *, processes=None, score_version="score-v1"):
    processes = processes or [f"proc-{i}" for i in range(len(differences))]
    return [{"status": "PASS", "baseline_id": "base-A", "score_version": score_version,
             "independent_process_id": p,
             "abba_samples_ms":[{"role":"INCUMBENT","runtime_ms":10.0},
                {"role":"CHALLENGER","runtime_ms":10.0+d},
                {"role":"CHALLENGER","runtime_ms":10.0+d},
                {"role":"INCUMBENT","runtime_ms":10.0}]} for p, d in zip(processes, differences)]


class IndependentPromotionRecheckTests(unittest.TestCase):
    def test_ten_independent_faster_pairs_confirm_improvement(self):
        result = classify_paired_recheck(pairs([-0.2] * 10), baseline_id="base-A", score_version="score-v1")
        self.assertEqual(result["status"], "CONFIRMED_IMPROVEMENT")
        self.assertEqual(result["one_sided_sign_test_p_if_all_agree"], 1/1024)
        self.assertLess(result["familywise_upper_bound_30_candidates"], 0.05)

    def test_ten_independent_slower_pairs_confirm_regression(self):
        result = classify_paired_recheck(pairs([0.2] * 10), baseline_id="base-A", score_version="score-v1")
        self.assertEqual(result["status"], "CONFIRMED_REGRESSION")

    def test_mixed_or_tied_pairs_are_uncertain(self):
        result = classify_paired_recheck(pairs([-0.2] * 7 + [0, 0.2, -0.1]), baseline_id="base-A", score_version="score-v1")
        self.assertEqual(result["status"], "UNCERTAIN")

    def test_correlated_events_in_one_process_do_not_count_as_repeats(self):
        result = classify_paired_recheck(pairs([-0.2] * 10, processes=["one"] * 10), baseline_id="base-A", score_version="score-v1")
        self.assertEqual(result["status"], "UNCERTAIN")
        self.assertEqual(result["independent_processes"], 1)

    def test_baseline_mismatch_is_not_comparable(self):
        data = pairs([-0.2] * 10)
        data[0]["baseline_id"] = "base-old"
        result = classify_paired_recheck(data, baseline_id="base-A", score_version="score-v1")
        self.assertEqual(result["status"], "UNCERTAIN")
        self.assertEqual(result["valid_pairs"], 9)

    def test_baseline_requires_measured_scope_and_expiry(self):
        now = datetime(2026, 10, 24, tzinfo=timezone.utc)
        scope = {"task_sha256":"task", "reference_source_sha256":"ref", "kernelbench_commit":"kb",
            "timing_source_sha256":"timer", "gpu_identity":{"uuid":"gpu"},
            "environment_identity":{"torch":"pinned","driver":"pinned"}}
        baseline = {**scope, "status":"QUALIFIED_FROZEN_BASELINE", "baseline_id":"base-A",
            "qualification":{"protocol":"N0_FROZEN_REFERENCE_QUALIFICATION_V2",
                "environment_fingerprint":environment_fingerprint(scope), "independent_process_sessions":100,
                "raw_evidence_manifest_sha256":"a"*64,
                "valid_from_utc":(now-timedelta(days=1)).isoformat(),
                "valid_until_utc":(now+timedelta(days=20)).isoformat()}}
        result = validate_frozen_baseline(baseline, expected_scope=scope, now_utc=now)
        self.assertEqual(result["status"], "PASS")
        expired = validate_frozen_baseline(baseline, expected_scope=scope,
            now_utc=now+timedelta(days=21))
        self.assertEqual(expired["status"], "BLOCKED")
        self.assertIn("BASELINE_EXPIRED", expired["failures"])

    def test_existing_unqualified_baseline_is_fail_closed(self):
        result = validate_frozen_baseline({"status":"NOT_QUALIFIED_PENDING_VALIDATION"},
            expected_scope={"task_sha256":"x"})
        self.assertEqual(result["status"], "BLOCKED")

    def test_gpu_monitor_distinguishes_same_lease_workers_from_uncoordinated_pids(self):
        monitor = ExternalGpuMonitor(owned_pids={1},
            coordinated_pid_resolver=lambda pid: {"pid":pid,"lease_path":"same"} if pid == 2 else None)
        snapshot = {"fields":["RTX","GPU-X","610.88","45","88","2000","1800","12000","60","P0"],
            "apps":["1, worker, [N/A]","2, worker, [N/A]","3, foreign, [N/A]"]}
        result = monitor.classify_snapshot(snapshot)
        self.assertEqual([x["pid"] for x in result["owned_compute_pids"]], [1])
        self.assertEqual([x["pid"] for x in result["coordinated_same_lease_participants"]], [2])
        self.assertEqual([x["pid"] for x in result["uncoordinated_compute_pids"]], [3])

    def test_gpu_monitor_marks_confirmed_exited_same_lease_candidate_pid_as_stale(self):
        participant={"pid":2,"attribution":"CANDIDATE_EVAL_CHILD_BOUND_TO_SHARED_GPU_LEASE",
            "lease_path":"same"}
        monitor=ExternalGpuMonitor(owned_pids={1},
            coordinated_pid_resolver=lambda pid: participant if pid==2 else None,
            process_liveness_resolver=lambda pid: False)
        snapshot={"fields":["RTX","GPU-X","610.88","45","0","656","1800","12000","30","P0"],
            "apps":["2, candidate, [N/A]"]}
        first=monitor.classify_snapshot(snapshot)
        self.assertEqual([x["pid"] for x in first["coordinated_same_lease_participants"]],[2])
        # The child exits between nvidia-smi samples, but Windows briefly retains its GPU app row.
        monitor.coordinated_pid_resolver=lambda pid: None
        stale=monitor.classify_snapshot(snapshot)
        self.assertEqual(stale["uncoordinated_compute_pids"],[])
        self.assertEqual(stale["recently_exited_coordinated_compute_pids"][0]["pid"],2)
        self.assertEqual(stale["recently_exited_coordinated_compute_pids"][0]["process_state"],"CONFIRMED_EXITED")

    def test_gpu_monitor_keeps_live_or_unverified_pid_fail_closed_after_same_lease_observation(self):
        participant={"pid":2,"attribution":"CANDIDATE_EVAL_CHILD_BOUND_TO_SHARED_GPU_LEASE",
            "lease_path":"same"}
        for process_state in (True,None):
            monitor=ExternalGpuMonitor(owned_pids={1},
                coordinated_pid_resolver=lambda pid: participant if pid==2 else None,
                process_liveness_resolver=lambda pid,state=process_state: state)
            snapshot={"fields":["RTX","GPU-X","610.88","45","0","656","1800","12000","30","P0"],
                "apps":["2, candidate, [N/A]"]}
            monitor.classify_snapshot(snapshot)
            monitor.coordinated_pid_resolver=lambda pid: None
            result=monitor.classify_snapshot(snapshot)
            self.assertEqual(result["recently_exited_coordinated_compute_pids"],[])
            self.assertEqual([x["pid"] for x in result["uncoordinated_compute_pids"]],[2])

    def test_gpu_monitor_keeps_unseen_pid_fail_closed(self):
        monitor=ExternalGpuMonitor(owned_pids={1},coordinated_pid_resolver=lambda pid: None,
            process_liveness_resolver=lambda pid: False)
        result=monitor.classify_snapshot({"fields":["RTX","GPU-X","610.88","45","0","656","1800","12000","30","P0"],
            "apps":["3, foreign, [N/A]"]})
        self.assertEqual(result["recently_exited_coordinated_compute_pids"],[])
        self.assertEqual([x["pid"] for x in result["uncoordinated_compute_pids"]],[3])

    def test_post_evaluator_reference_guard_is_tighter_than_legacy_five_percent(self):
        for difference in (0.0043, 0.0115, 0.02):
            result = enforce_post_evaluator_control_limit({"pass":True,"limit":0.05,
                "relative_difference":difference})
            self.assertTrue(result["pass"])
            self.assertEqual(result["limit"], 0.02)
        result = enforce_post_evaluator_control_limit({"pass":True,"limit":0.05,
            "relative_difference":0.0201})
        self.assertFalse(result["pass"])
        self.assertEqual(result["legacy_path_gate_limit"], 0.05)

    def test_startup_allows_only_same_lease_worker_contexts(self):
        lease = Path(r"D:\\shared\\gpu.lock")
        with patch.object(run_r5_oj_worker,"_is_n0_lease_participant",
                side_effect=lambda pid,path: {"pid":pid,"lease_path":str(path)} if pid==2 else None):
            result = run_r5_oj_worker.classify_startup_contexts(
                ["1, current, [N/A]","2, sibling, [N/A]","3, foreign, [N/A]"],
                current_pid=1,lease_path=lease)
        self.assertEqual(len(result["coordinated"]),1)
        self.assertEqual(result["uncoordinated"],[{"pid":3,"raw":"3, foreign, [N/A]"}])

    def test_descendant_profile_pid_is_attributed_through_exact_lease_worker(self):
        lease = Path(r"D:\shared\gpu.lock")
        items = {
            30:{"ProcessId":30,"ParentProcessId":20,"ExecutablePath":"python.exe","CommandLine":"python ncu_target.py"},
            20:{"ProcessId":20,"ParentProcessId":10,"ExecutablePath":"python.exe","CommandLine":"python profile_launcher.py"},
            10:{"ProcessId":10,"ParentProcessId":1,"ExecutablePath":"python.exe",
                "CommandLine":f"python run_r5_oj_worker.py --gpu-lease-path {lease}"},
        }
        with patch.object(run_r5_oj_worker,"_process_identity",side_effect=lambda pid:items.get(pid,{})):
            result = run_r5_oj_worker._is_n0_lease_participant(30,lease)
        self.assertEqual(result["lease_owner_pid"],10)
        self.assertEqual(result["attribution"],"PROCESS_ANCESTOR_BOUND_TO_SHARED_GPU_LEASE")
        with patch.object(run_r5_oj_worker,"_process_identity",side_effect=lambda pid:{
                30:{"ProcessId":30,"ParentProcessId":99,"CommandLine":"python foreign.py"},99:{}}.get(pid,{})):
            self.assertIsNone(run_r5_oj_worker._is_n0_lease_participant(30,lease))

    def test_single_evaluation_ci_cannot_promote_without_recheck(self):
        measured = {"baseline_id":"base-A","score_version":"N0_FROZEN_REFERENCE_SCORE_V1",
            "candidate_runtime_ci95_ms":[1.0,1.01]}
        previous = {"baseline_id":"base-A","score_version":"N0_FROZEN_REFERENCE_SCORE_V1",
            "candidate_runtime_ci95_ms":[1.1,1.2]}
        self.assertEqual(assess_best_promotion(measured, previous)[0], "UNCERTAIN")

    def test_recomputed_ten_process_recheck_drives_best_confidence(self):
        recheck = classify_paired_recheck(pairs([-0.2]*10,score_version="N0_FROZEN_REFERENCE_SCORE_V1"), baseline_id="base-A", score_version="N0_FROZEN_REFERENCE_SCORE_V1")
        measured = {"baseline_id":"base-A","score_version":"N0_FROZEN_REFERENCE_SCORE_V1",
            "independent_process_recheck":recheck}
        previous = {"baseline_id":"base-A","score_version":"N0_FROZEN_REFERENCE_SCORE_V1"}
        self.assertEqual(assess_best_promotion(measured, previous)[0], "CONFIRMED_IMPROVEMENT")


if __name__ == "__main__":
    unittest.main()
