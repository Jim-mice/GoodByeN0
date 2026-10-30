import importlib.util
import json
import sqlite3
import tempfile
import unittest
from unittest import mock
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


worker = load_module("r12103_worker", ROOT / "run_r5_oj_worker.py")
supervisor = load_module("r12103_supervisor", ROOT / "n0_integration" / "service_supervisor.py")
bootstrap = load_module("r12103_bootstrap", ROOT / "n0_integration" / "bootstrap_recovery.py")


class FaultRecoveryHandoffTests(unittest.TestCase):
    def make_handoff(self):
        request_id = "N0-OJ-DIAG-FAULT-001"
        status = {"state": "HANDOFF_REQUIRED", "detail": {
            "fault_request_id": request_id,
            "shutdown": "FRESH_PROCESS_FAULT_RECOVERY_HANDOFF"}}
        gate = {"schema": "N0_GPU_FAULT_RECOVERY_GATE_V1", "status": "HOLD",
                "fault_request_id": request_id}
        settled = {"request_id": request_id, "state": "CONSUMED", "receipt_sha256": "a" * 64,
                   "verdict": "OJ_INFRASTRUCTURE_HOLD_REFERENCE_BASELINE_DRIFT",
                   "score": None, "running_requests": 0}
        fault = {"request_id": request_id,
                 "fault": {"classification": "OJ_INFRASTRUCTURE_HOLD_REFERENCE_BASELINE_DRIFT"},
                 "job_result": {"active_processes_after": 0}}
        return status, gate, settled, fault

    def test_candidate_fault_requires_fresh_process_handoff(self):
        self.assertTrue(worker.requires_fault_recovery_handoff({
            "schema": "N0_GPU_FAULT_RECOVERY_GATE_V1", "status": "HOLD"}))
        self.assertFalse(worker.requires_fault_recovery_handoff({
            "schema": "N0_GPU_FAULT_RECOVERY_GATE_V1", "status": "READY"}))
        self.assertFalse(worker.requires_fault_recovery_handoff({
            "schema": "N0_POST_PROFILE_TIMING_GATE_V1", "status": "HOLD"}))

    def test_recovery_receipt_attempts_are_immutable_and_request_scoped(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            old = worker.fault_recovery_receipt_path(root, "attempt-old")
            new = worker.fault_recovery_receipt_path(root, "attempt-new")
            old.write_text("prior-hold", encoding="utf-8")
            with new.open("xb") as stream:
                stream.write(b"new-attempt")
            self.assertEqual(old.read_text(encoding="utf-8"), "prior-hold")
            self.assertNotEqual(old, new)
            with self.assertRaises(ValueError):
                worker.fault_recovery_receipt_path(root, "..\\escape")

    def test_supervisor_accepts_only_settled_matching_fault_handoff(self):
        status, gate, settled, fault = self.make_handoff()
        self.assertEqual(supervisor.serving_handoff_kind(status), "CANDIDATE_FAULT")
        self.assertEqual(supervisor.validate_fault_recovery_handoff(status, gate, settled, fault), "PASS")

    def test_candidate_correctness_receipt_allows_device_requalification_handoff(self):
        status, gate, settled, fault = self.make_handoff()
        gate["reason"] = "CANDIDATE_PROCESS_TERMINATION_REQUIRES_DEVICE_REQUALIFICATION"
        gate["fault_classification"] = {"classification": "CANDIDATE_CORRECTNESS_FAILURE"}
        settled["verdict"] = "CANDIDATE_CORRECTNESS_FAILURE"
        fault["fault"]["classification"] = "CANDIDATE_CORRECTNESS_FAILURE"
        self.assertEqual(supervisor.validate_fault_recovery_handoff(status, gate, settled, fault), "PASS")
        gate["fault_classification"]["classification"] = "OJ_INFRASTRUCTURE_HOLD_REFERENCE_BASELINE_DRIFT"
        self.assertEqual(supervisor.validate_fault_recovery_handoff(status, gate, settled, fault),
                         "FAULT_HANDOFF_RECEIPT_CLASSIFICATION_MISMATCH")

    def test_candidate_correctness_receipt_is_rejected_without_matching_requalification_gate(self):
        status, gate, settled, fault = self.make_handoff()
        settled["verdict"] = "CANDIDATE_CORRECTNESS_FAILURE"
        fault["fault"]["classification"] = "CANDIDATE_CORRECTNESS_FAILURE"
        self.assertEqual(supervisor.validate_fault_recovery_handoff(status, gate, settled, fault),
                         "FAULT_HANDOFF_RECEIPT_CLASSIFICATION_MISMATCH")

    def test_candidate_cuda_runtime_failure_allows_only_matching_device_requalification_handoff(self):
        status, gate, settled, fault = self.make_handoff()
        request_id = settled["request_id"]
        candidate_sha = "d" * 64
        episode_id = "N0-DIAG-EP-02"
        diagnosis = {"schema": "N0_CUDA_FAULT_V1", "classification": "CANDIDATE_EXECUTION_ILLEGAL_ADDRESS",
                     "attribution": "CANDIDATE", "classifier_version": "N0_CUDA_FAULT_CLASSIFIER_V1",
                     "stage": "EVALUATION_CHILD", "score_allowed": False,
                     "domain": "CUDA_DRIVER", "official_name": "CUDA_ERROR_ILLEGAL_ADDRESS", "raw_value": 700}
        gate.update({"reason": "CANDIDATE_PROCESS_TERMINATION_REQUIRES_DEVICE_REQUALIFICATION",
                     "fault_classification": diagnosis})
        settled.update({"verdict": "CANDIDATE_CUDA_RUNTIME_FAILURE", "candidate_sha256": candidate_sha,
                        "episode_id": episode_id, "candidate_failure_diagnostics": diagnosis})
        fault.update({"candidate_sha256": candidate_sha,
                      "worker_identity": {"request_id": request_id, "candidate_sha256": candidate_sha,
                                          "episode_id": episode_id},
                      "fault": diagnosis})
        self.assertEqual(supervisor.validate_fault_recovery_handoff(status, gate, settled, fault), "PASS")
        for mutate in (
            lambda: settled.update(candidate_sha256="e" * 64),
            lambda: settled.update(candidate_failure_diagnostics={**diagnosis, "attribution": "INFRASTRUCTURE"}),
            lambda: gate.update(fault_classification={**diagnosis, "classification": "UNKNOWN_CUDA_FAILURE"}),
            lambda: fault.update(worker_identity={**fault["worker_identity"], "episode_id": "N0-WRONG"}),
        ):
            old_gate = dict(gate); old_settled = dict(settled); old_fault = dict(fault)
            mutate()
            self.assertEqual(supervisor.validate_fault_recovery_handoff(status, gate, settled, fault),
                             "FAULT_HANDOFF_RECEIPT_CLASSIFICATION_MISMATCH")
            gate.clear(); gate.update(old_gate); settled.clear(); settled.update(old_settled)
            fault.clear(); fault.update(old_fault)

    def test_existing_hold_qualification_requires_matching_cuda_fault_receipt(self):
        diagnosis = {"schema": "N0_CUDA_FAULT_V1", "classification": "CANDIDATE_EXECUTION_ILLEGAL_ADDRESS",
                     "attribution": "CANDIDATE", "classifier_version": "N0_CUDA_FAULT_CLASSIFIER_V1",
                     "stage": "EVALUATION_CHILD", "score_allowed": False}
        gate = {"reason": "CANDIDATE_PROCESS_TERMINATION_REQUIRES_DEVICE_REQUALIFICATION",
                "fault_classification": diagnosis}
        request = {"request_id": "N0-OJ-DIAG-002", "candidate_sha256": "e" * 64,
                   "episode_id": "N0-DIAG-EP-02"}
        receipt = {"verdict": "CANDIDATE_CUDA_RUNTIME_FAILURE", "score": None,
                   "candidate_failure_diagnostics": diagnosis}
        fault = {"fault": diagnosis, "worker_identity": {"request_id": request["request_id"],
                 "candidate_sha256": request["candidate_sha256"], "episode_id": request["episode_id"]}}
        bootstrap._validate_fault_receipt_classification(gate, request, receipt, fault)
        for change in ("attribution", "classification", "stage", "score_allowed"):
            altered = dict(diagnosis)
            altered[change] = "INFRASTRUCTURE" if change != "score_allowed" else True
            with self.assertRaises(bootstrap.BootstrapHoldError):
                bootstrap._validate_fault_receipt_classification(gate, request,
                    {**receipt, "candidate_failure_diagnostics": altered}, fault)

    def test_legacy_r6_cuda_receipt_uses_matching_outer_hash_without_rewriting_it(self):
        diagnosis = {"schema": "N0_CUDA_FAULT_V1", "classification": "CANDIDATE_EXECUTION_ILLEGAL_ADDRESS",
                     "attribution": "CANDIDATE", "classifier_version": "N0_CUDA_FAULT_CLASSIFIER_V1",
                     "stage": "EVALUATION_CHILD", "score_allowed": False}
        body = {"request_id": "N0-OJ-DIAG-LEGACY", "episode_id": "N0-DIAG-EP-03",
                "candidate_id": "C0002", "candidate_sha256": "f" * 64,
                "verdict": "CANDIDATE_CUDA_RUNTIME_FAILURE", "score": None,
                "candidate_failure_diagnostics": diagnosis, "receipt_sha256": "a" * 64}
        canonical = lambda value: json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
        row = {"state": "CONSUMED", "request_id": body["request_id"], "episode_id": body["episode_id"],
               "candidate_id": body["candidate_id"], "candidate_sha256": body["candidate_sha256"],
               "receipt_json": json.dumps(body), "receipt_sha256": __import__("hashlib").sha256(canonical(body)).hexdigest()}
        checked = bootstrap._receipt_checks(row)
        self.assertEqual(checked["embedded_sha_status"], "LEGACY_R6_CUDA_FAULT_DOUBLE_HASH")
        row["receipt_sha256"] = "0" * 64
        with self.assertRaises(bootstrap.BootstrapHoldError):
            bootstrap._receipt_checks(row)
        body["verdict"] = "CANDIDATE_CORRECTNESS_FAILURE"
        row["receipt_json"] = json.dumps(body)
        row["receipt_sha256"] = __import__("hashlib").sha256(canonical(body)).hexdigest()
        with self.assertRaises(bootstrap.BootstrapHoldError):
            bootstrap._receipt_checks(row)

    def test_handoff_rejects_request_identity_mismatch(self):
        status, gate, settled, fault = self.make_handoff()
        status["detail"]["fault_request_id"] = "N0-OJ-WRONG"
        self.assertEqual(supervisor.validate_fault_recovery_handoff(status, gate, settled, fault),
                         "FAULT_HANDOFF_REQUEST_ID_MISMATCH")

    def test_handoff_rejects_live_or_unsettled_work(self):
        status, gate, settled, fault = self.make_handoff()
        settled["state"] = "QUEUED"
        self.assertEqual(supervisor.validate_fault_recovery_handoff(status, gate, settled, fault),
                         "FAULT_HANDOFF_RECEIPT_NOT_DURABLE")
        settled["state"] = "CONSUMED"
        settled["running_requests"] = 1
        self.assertEqual(supervisor.validate_fault_recovery_handoff(status, gate, settled, fault),
                         "OJ_REQUEST_STILL_RUNNING_DURING_FAULT_HANDOFF")

    def test_handoff_rejects_score_or_live_candidate_tree(self):
        status, gate, settled, fault = self.make_handoff()
        settled["score"] = 1.25
        self.assertEqual(supervisor.validate_fault_recovery_handoff(status, gate, settled, fault),
                         "FAULT_HANDOFF_RECEIPT_CLASSIFICATION_MISMATCH")
        settled["score"] = None
        fault["job_result"]["active_processes_after"] = 1
        self.assertEqual(supervisor.validate_fault_recovery_handoff(status, gate, settled, fault),
                         "FAULT_HANDOFF_CANDIDATE_TREE_NOT_PROVEN_EMPTY")

    def test_profile_handoff_remains_distinct_and_compatible(self):
        status = {"state": "HANDOFF_REQUIRED", "detail": {
            "shutdown": "FRESH_PROCESS_RECOVERY_HANDOFF", "profile_request_id": "N0-OJ-PROFILE-1"}}
        self.assertEqual(supervisor.serving_handoff_kind(status), "PROFILE")
        self.assertEqual(supervisor.serving_exit_kind(75, False), "HANDOFF")

    def test_exit_code_zero_without_stop_is_not_orderly(self):
        self.assertEqual(supervisor.serving_exit_kind(0, False), "UNEXPECTED_EXIT")
        self.assertEqual(supervisor.serving_exit_kind(0, True), "ORDERLY_STOP")
        self.assertEqual(supervisor.serving_exit_kind(23, False), "UNEXPECTED_EXIT")
        self.assertEqual(supervisor.serving_exit_kind(20, False), "CHILD_HOLD")

    def test_worker_persists_hold_with_supervisor_child_hold_exit_code(self):
        self.assertEqual(worker.serving_worker_exit_code("HOLD", False), 20)
        self.assertEqual(worker.serving_worker_exit_code("HANDOFF_REQUIRED", True), 75)
        self.assertEqual(worker.serving_worker_exit_code("STOPPED", False), 0)

    def test_supervisor_requires_exact_worker_pid_parent_and_python_identity(self):
        queue = Path("D:/diag-queue")
        command = f'"{supervisor.PYTHON}" -u "{supervisor.WORKER}" --queue-root {queue}'
        status = {"pid": 3001, "detail": {"process_identity": {
            "pid": 3001, "parent_pid": 2001, "sys_executable": str(supervisor.PYTHON),
            "command_line": command,
            "queue_root": str(queue)}}}
        record = {"pid": 3001, "command": [str(supervisor.PYTHON), "-u", str(supervisor.WORKER)],
            "worker_identity_evidence": {"captured_while_alive": True,"launcher_pid":3001,"worker_pid":3001,
                "supervisor_pid":2001,"worker_process":{"ProcessId":3001,"ParentProcessId":2001,"CommandLine":command},
                "launcher_process":{"ProcessId":3001,"ParentProcessId":2001,"CommandLine":command}}}
        self.assertEqual(supervisor.validate_serving_child_identity(status, record, 2001, queue), "PASS")
        record["pid"] = 4001
        self.assertEqual(supervisor.validate_serving_child_identity(status, record, 2001, queue),
                         "SERVING_CHILD_LAUNCH_CHAIN_IDENTITY_MISMATCH")

    def test_supervisor_accepts_only_observed_windows_venv_launcher_chain(self):
        queue = Path("D:/diag-queue")
        command = f'"{supervisor.PYTHON}" -u "{supervisor.WORKER}" --queue-root {queue}'
        actual = {"pid": 3010, "parent_pid": 3001, "sys_executable": str(supervisor.PYTHON),
            "command_line": command, "queue_root": str(queue)}
        launcher = {"ProcessId":3001,"ParentProcessId":2001,"CommandLine":command}
        worker = {"ProcessId":3010,"ParentProcessId":3001,"CommandLine":command}
        record = {"pid":3001,"command":[str(supervisor.PYTHON),"-u",str(supervisor.WORKER)],
            "worker_identity_evidence":{"captured_while_alive":True,"launcher_pid":3001,"worker_pid":3010,
                "supervisor_pid":2001,"launcher_process":launcher,"worker_process":worker}}
        status = {"pid":3010,"detail":{"process_identity":actual}}
        self.assertEqual(supervisor.validate_serving_child_identity(status, record, 2001, queue), "PASS")
        record["worker_identity_evidence"]["worker_process"]["ParentProcessId"] = 9999
        self.assertEqual(supervisor.validate_serving_child_identity(status, record, 2001, queue),
                         "SERVING_CHILD_OBSERVED_PID_MISMATCH")

    def test_18_queued_requests_remain_unchanged_after_read_only_audit(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            db = root / "n0_runtime.sqlite3"
            con = sqlite3.connect(db)
            con.execute("CREATE TABLE oj_requests(request_id TEXT PRIMARY KEY,state TEXT,receipt_sha256 TEXT,receipt_json TEXT)")
            con.execute("CREATE TABLE episodes(episode_id TEXT PRIMARY KEY,candidates_used INTEGER,state TEXT)")
            con.execute("INSERT INTO episodes VALUES('N0-DIAG-EP-01',37,'WAITING_OJ')")
            con.execute("INSERT INTO oj_requests VALUES(?,?,?,?)", (
                "N0-OJ-DIAG-FAULT-001", "CONSUMED", "a" * 64,
                json.dumps({"verdict": "OJ_INFRASTRUCTURE_HOLD_REFERENCE_BASELINE_DRIFT", "score": None})))
            queued = [f"N0-OJ-QUEUED-{i:02d}" for i in range(18)]
            con.executemany("INSERT INTO oj_requests VALUES(?,?,?,?)", [(x, "QUEUED", None, None) for x in queued])
            con.commit()
            before = con.execute("SELECT request_id,state,receipt_sha256 FROM oj_requests ORDER BY request_id").fetchall()
            budget_before = con.execute("SELECT episode_id,candidates_used,state FROM episodes").fetchall()
            con.close()
            settled = supervisor.request_state(root, "N0-OJ-DIAG-FAULT-001")
            self.assertEqual(settled["state"], "CONSUMED")
            self.assertEqual(settled["verdict"], "OJ_INFRASTRUCTURE_HOLD_REFERENCE_BASELINE_DRIFT")
            con = sqlite3.connect(db)
            after = con.execute("SELECT request_id,state,receipt_sha256 FROM oj_requests ORDER BY request_id").fetchall()
            self.assertEqual(before, after)
            self.assertEqual(con.execute("SELECT episode_id,candidates_used,state FROM episodes").fetchall(), budget_before)
            self.assertEqual(con.execute("SELECT COUNT(*) FROM oj_requests WHERE state='QUEUED'").fetchone()[0], 18)
            con.close()

    def run_supervisor_flow(self, recovery_passes, use_profile_handoff=False):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            queue = root / "queue"
            profile = root / "profile"
            queue.mkdir(); profile.mkdir()
            qualification = root / "qualification.json"; qualification.write_text("{}", encoding="utf-8")
            whitelist = root / "whitelist.json"; whitelist.write_text("{}", encoding="utf-8")
            db = sqlite3.connect(queue / "n0_runtime.sqlite3")
            db.execute("CREATE TABLE oj_requests(request_id TEXT PRIMARY KEY,state TEXT,receipt_sha256 TEXT,receipt_json TEXT)")
            db.execute("CREATE TABLE episodes(episode_id TEXT PRIMARY KEY,candidates_used INTEGER,state TEXT)")
            db.execute("INSERT INTO episodes VALUES('N0-DIAG-EP-01',37,'WAITING_OJ')")
            request_id = "N0-OJ-DIAG-PROFILE-001" if use_profile_handoff else "N0-OJ-DIAG-FAULT-002"
            receipt = {"verdict": "VALID_MEASUREMENT", "score": 2.0} if use_profile_handoff else {
                "verdict": "OJ_INFRASTRUCTURE_HOLD_REFERENCE_BASELINE_DRIFT", "score": None}
            db.execute("INSERT INTO oj_requests VALUES(?,?,?,?)", (request_id, "CONSUMED", "b" * 64,
                      json.dumps(receipt)))
            queued = [f"N0-OJ-QUEUED-{i:02d}" for i in range(18)]
            db.executemany("INSERT INTO oj_requests VALUES(?,?,?,?)", [(x, "QUEUED", None, None) for x in queued])
            db.commit(); db.close()
            gate_path = profile / "timing_gate.json"
            gate_path.write_text(json.dumps({"schema": "N0_POST_PROFILE_TIMING_GATE_V1", "status": "READY"}),
                                 encoding="utf-8")
            fault_path = queue / "candidate_supervision" / request_id / "fault_receipt.json"
            recovery_calls = 0; serving_calls = 0

            def child_status(pid, state, detail):
                detail = dict(detail)
                command = f'"{supervisor.PYTHON}" -u "{supervisor.WORKER}" --queue-root {queue}'
                detail["process_identity"] = {"pid": pid, "parent_pid": __import__("os").getpid(),
                    "sys_executable": str(supervisor.PYTHON),
                    "command_line": command,
                    "queue_root": str(queue)}
                return {"state": state, "pid": pid, "detail": detail}

            def child_record(pid, returncode):
                command = f'"{supervisor.PYTHON}" -u "{supervisor.WORKER}" --queue-root {queue}'
                return {"pid": pid, "returncode": returncode, "descendants_after_exit": [],
                        "command": [str(supervisor.PYTHON), "-u", str(supervisor.WORKER),
                                    "--queue-root", str(queue)],
                        "worker_identity_evidence":{"captured_while_alive":True,"launcher_pid":pid,
                            "worker_pid":pid,"supervisor_pid":__import__("os").getpid(),
                            "worker_process":{"ProcessId":pid,"ParentProcessId":__import__("os").getpid(),"CommandLine":command},
                            "launcher_process":{"ProcessId":pid,"ParentProcessId":__import__("os").getpid(),"CommandLine":command}}}

            def fake_launch(args, env, *, recovery_only):
                nonlocal recovery_calls, serving_calls
                if recovery_only:
                    recovery_calls += 1
                    if use_profile_handoff:
                        gate = {"schema": "N0_POST_PROFILE_TIMING_GATE_V1", "status": "READY",
                                "last_profile_receipt_id": request_id}
                    else:
                        rec_id = "RECOVERY-001"
                        rec_path = profile / "fault_recovery_receipts" / (rec_id + ".json")
                        rec_path.parent.mkdir(parents=True, exist_ok=True)
                        rec_body = {"schema": "N0_CANDIDATE_FAULT_RECOVERY_RECEIPT_V1", "recovery_id": rec_id,
                                    "fault_request_id": request_id, "status": "PASS" if recovery_passes else "HOLD"}
                        import hashlib
                        canonical = json.dumps(rec_body, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
                        rec_sha = hashlib.sha256(canonical).hexdigest()
                        rec_body["recovery_receipt_sha256"] = rec_sha
                        rec_path.write_text(json.dumps(rec_body), encoding="utf-8")
                        gate = {"schema": "N0_GPU_FAULT_RECOVERY_GATE_V1",
                                "status": "READY" if recovery_passes else "HOLD", "fault_request_id": request_id,
                                "recovery_receipt_id": rec_id, "recovery_receipt_path": str(rec_path),
                                "recovery_receipt_sha256": rec_sha}
                    gate_path.write_text(json.dumps(gate), encoding="utf-8")
                    recovery_pid = 2000 + recovery_calls
                    (queue / "service_status.json").write_text(json.dumps(child_status(recovery_pid,
                        "READY" if recovery_passes else "HOLD", {"shutdown":"RECOVERY_ONLY_NO_QUEUE_CLAIM",
                            "post_profile_recovery":{"recovery_receipt_path":str(rec_path) if not use_profile_handoff else "",
                                "recovery_receipt_file_sha256":"f"*64,"recovery_receipt_sha256":gate.get("recovery_receipt_sha256")}})),
                        encoding="utf-8")
                    return 0, child_record(recovery_pid, 0)
                serving_calls += 1
                if serving_calls == 1:
                    if use_profile_handoff:
                        status = child_status(3001,"HANDOFF_REQUIRED", {"profile_request_id": request_id,
                                  "shutdown": "FRESH_PROCESS_RECOVERY_HANDOFF"})
                        gate = {"schema": "N0_POST_PROFILE_TIMING_GATE_V1", "status": "HANDOFF_REQUIRED",
                                "last_profile_receipt_id": request_id}
                    else:
                        status = child_status(3001,"HANDOFF_REQUIRED", {"fault_request_id": request_id,
                                  "shutdown": "FRESH_PROCESS_FAULT_RECOVERY_HANDOFF"})
                        fault_path.parent.mkdir(parents=True, exist_ok=True)
                        fault_path.write_text(json.dumps({"request_id": request_id,
                            "fault": {"classification": "OJ_INFRASTRUCTURE_HOLD_REFERENCE_BASELINE_DRIFT"},
                            "job_result": {"active_processes_after": 0}}), encoding="utf-8")
                        gate = {"schema": "N0_GPU_FAULT_RECOVERY_GATE_V1", "status": "HOLD",
                                "fault_request_id": request_id}
                    (queue / "service_status.json").write_text(json.dumps(status), encoding="utf-8")
                    gate_path.write_text(json.dumps(gate), encoding="utf-8")
                    return 75, child_record(3001,75)
                (queue / "STOP_REQUESTED").write_text("test", encoding="utf-8")
                (queue / "service_status.json").write_text(json.dumps(child_status(3002,"STOPPED",
                    {"shutdown":"ORDERLY_STOP_MARKER"})),encoding="utf-8")
                return 0, child_record(3002,0)

            args = ["supervisor", "--queue-root", str(queue), "--qualification-out", str(qualification),
                    "--profile-whitelist", str(whitelist), "--gpu-lease-path", str(root / "lease.lock")]
            fake_proc = [{"ProcessId": __import__("os").getpid(), "ParentProcessId": 100,
                          "ExecutablePath": "python.exe", "CommandLine": "supervisor"}]
            recovery_context = {"fault_request_id":request_id,"candidate_sha256":"c"*64,
                "fault_receipt_sha256":"d"*64,"oj_receipt_sha256":"b"*64,
                "fault_gate_sha256":"e"*64,"gpu_lease_path":str(root/"lease.lock"),
                "queue_snapshot_sha256":"q"*64,
                "queue_snapshot":{"sha256":"q"*64,"request_count":19,"episode_count":1,
                    "queued_count":18,"hold_unknown_count":0,"running_count":0,"candidate_budget_used":37}}
            def fake_existing_hold(*_args, **_kwargs):
                if use_profile_handoff:
                    raise AssertionError("profile handoff must retain its separate recovery path")
                return recovery_context
            def fake_validate_recovery(_path, context, _status, expected_sha, expected_worker_state="READY"):
                current_gate = json.loads(gate_path.read_text(encoding="utf-8"))
                if current_gate.get("status") != "READY":
                    raise supervisor.BootstrapHoldError("RECOVERY_RECEIPT_NOT_PASS")
                return {"receipt":{"recovery_id":current_gate["recovery_receipt_id"]},
                    "recovery_receipt_sha256":current_gate["recovery_receipt_sha256"],
                    "recovery_receipt_file_sha256":expected_sha,"consecutive_passes":3}
            with mock.patch.object(supervisor.sys, "argv", args), \
                 mock.patch.object(supervisor, "process_snapshot", return_value=fake_proc), \
                 mock.patch.object(supervisor, "launch_child", side_effect=fake_launch), \
                 mock.patch.object(supervisor, "validate_existing_hold", side_effect=fake_existing_hold), \
                 mock.patch.object(supervisor, "validate_recovery_receipt", side_effect=fake_validate_recovery), \
                 mock.patch.dict(supervisor.os.environ, {"N0_PROFILE_STATE_ROOT": str(profile)}):
                result = supervisor.main()
            db = sqlite3.connect(queue / "n0_runtime.sqlite3")
            counts = dict(db.execute("SELECT state,COUNT(*) FROM oj_requests GROUP BY state"))
            budget_after = db.execute("SELECT episode_id,candidates_used,state FROM episodes").fetchall()
            db.close()
            return result, recovery_calls, serving_calls, counts, json.loads((queue / "service_status.json").read_text()), budget_after

    def test_fresh_process_fault_recovery_success_keeps_18_queued_and_resumes(self):
        result, recoveries, servings, counts, status, budget = self.run_supervisor_flow(True)
        self.assertEqual((result, recoveries, servings), (0, 1, 2))
        self.assertEqual(counts.get("QUEUED"), 18)
        self.assertEqual(status["state"], "STOPPED")
        self.assertEqual(budget, [("N0-DIAG-EP-01", 37, "WAITING_OJ")])

    def test_fresh_process_fault_recovery_failure_stays_hold_and_does_not_resume(self):
        result, recoveries, servings, counts, status, budget = self.run_supervisor_flow(False)
        self.assertEqual((result, recoveries, servings), (22, 1, 1))
        self.assertEqual(counts.get("QUEUED"), 18)
        self.assertEqual(status["state"], "HOLD")
        self.assertEqual(budget, [("N0-DIAG-EP-01", 37, "WAITING_OJ")])

    def test_normal_profile_handoff_path_remains_supported(self):
        result, recoveries, servings, counts, status, budget = self.run_supervisor_flow(True, use_profile_handoff=True)
        self.assertEqual((result, recoveries, servings), (0, 1, 2))
        self.assertEqual(counts.get("QUEUED"), 18)
        self.assertEqual(status["state"], "STOPPED")
        self.assertEqual(budget, [("N0-DIAG-EP-01", 37, "WAITING_OJ")])


if __name__ == "__main__":
    unittest.main(verbosity=2)
