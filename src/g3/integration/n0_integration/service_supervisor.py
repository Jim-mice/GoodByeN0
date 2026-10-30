from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(r"D:\projects\kernelagent\runtime")
INTEGRATION = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(INTEGRATION))
from n0_integration.bootstrap_recovery import (BootstrapHoldError, canonical_bytes,
    queue_snapshot, sha_bytes, sha_file, validate_existing_hold, validate_recovery_receipt,
    validate_rescue_authorization, utc_now)
PYTHON = ROOT / ".venv_n0_r6" / "Scripts" / "python.exe"
WORKER = INTEGRATION / "run_r5_oj_worker.py"
RELEASE_ID = INTEGRATION.name
MANIFEST = INTEGRATION / "RELEASE_MANIFEST.json"


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest().upper()


def same_sha256(left: object, right: object) -> bool:
    """Compare already-authenticated SHA-256 hex digests without case sensitivity."""
    a, b = str(left or "").strip(), str(right or "").strip()
    if len(a) != 64 or len(b) != 64:
        return False
    try:
        int(a, 16)
        int(b, 16)
    except ValueError:
        return False
    return a.casefold() == b.casefold()


def atomic_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def process_snapshot() -> list[dict]:
    script = "$p=Get-CimInstance Win32_Process | Select-Object ProcessId,ParentProcessId,CreationDate,Name,ExecutablePath,CommandLine; $p | ConvertTo-Json -Compress"
    result = subprocess.run(["powershell.exe", "-NoProfile", "-NonInteractive", "-Command", script],
                            capture_output=True, text=True, timeout=20, encoding="utf-8", errors="replace")
    if result.returncode:
        raise RuntimeError("PROCESS_TREE_SNAPSHOT_FAILED:" + result.stderr[-500:])
    value = json.loads(result.stdout or "[]")
    return value if isinstance(value, list) else ([value] if value else [])


def descendants(rows: list[dict], root_pid: int) -> list[dict]:
    found = set()
    changed = True
    while changed:
        changed = False
        for row in rows:
            pid = int(row.get("ProcessId") or 0)
            parent = int(row.get("ParentProcessId") or 0)
            if pid and pid != root_pid and (parent == root_pid or parent in found) and pid not in found:
                found.add(pid)
                changed = True
    return [row for row in rows if int(row.get("ProcessId") or 0) in found]


def wait_process_tree_empty(pid: int, process: subprocess.Popen, timeout: float = 15.0) -> list[dict]:
    deadline = time.monotonic() + timeout
    last = []
    while time.monotonic() < deadline:
        rows = process_snapshot()
        last = descendants(rows, pid)
        if process.poll() is not None and not last:
            return []
        time.sleep(0.25)
    return last


def request_state(queue_root: Path, request_id: str) -> dict:
    import sqlite3
    database = queue_root / "n0_runtime.sqlite3"
    if not database.is_file():
        raise RuntimeError("OJ_QUEUE_DATABASE_MISSING_DURING_HANDOFF")
    con = sqlite3.connect(database.resolve().as_uri() + "?mode=ro", uri=True, timeout=10)
    try:
        row = con.execute("SELECT request_id,state,receipt_sha256,receipt_json FROM oj_requests WHERE request_id=?",
                          (request_id,)).fetchone()
        running = con.execute("SELECT COUNT(*) FROM oj_requests WHERE state='RUNNING'").fetchone()[0]
    finally:
        con.close()
    if not row:
        raise RuntimeError("PROFILE_HANDOFF_REQUEST_NOT_FOUND")
    if row[1] not in {"COMPLETED", "CONSUMED"} or not row[2]:
        raise RuntimeError("PROFILE_HANDOFF_RECEIPT_NOT_DURABLE")
    if running:
        raise RuntimeError("OJ_REQUEST_STILL_RUNNING_DURING_PROFILE_HANDOFF")
    receipt = json.loads(row[3]) if row[3] else {}
    return {"request_id": row[0], "state": row[1], "receipt_sha256": row[2],
            "verdict": receipt.get("verdict"), "score": receipt.get("score"), "running_requests": running,
            "candidate_id": receipt.get("candidate_id"), "candidate_sha256": receipt.get("candidate_sha256"),
            "episode_id": receipt.get("episode_id"),
            "candidate_failure_diagnostics": receipt.get("candidate_failure_diagnostics")}


def validate_fault_recovery_handoff(status: dict, gate: dict, settled: dict, fault: dict) -> str:
    """Validate a settled candidate-fault handoff before starting fresh recovery."""
    detail = status.get("detail") or {}
    request_id = str(gate.get("fault_request_id") or "")
    if status.get("state") != "HANDOFF_REQUIRED" or detail.get("shutdown") != "FRESH_PROCESS_FAULT_RECOVERY_HANDOFF":
        return "FAULT_HANDOFF_STATE_MISMATCH"
    if gate.get("schema") != "N0_GPU_FAULT_RECOVERY_GATE_V1" or gate.get("status") != "HOLD":
        return "FAULT_HANDOFF_GATE_MISMATCH"
    if not request_id or detail.get("fault_request_id") != request_id or settled.get("request_id") != request_id:
        return "FAULT_HANDOFF_REQUEST_ID_MISMATCH"
    if settled.get("state") not in {"COMPLETED", "CONSUMED"} or not settled.get("receipt_sha256"):
        return "FAULT_HANDOFF_RECEIPT_NOT_DURABLE"
    if settled.get("running_requests") != 0:
        return "OJ_REQUEST_STILL_RUNNING_DURING_FAULT_HANDOFF"
    verdict = settled.get("verdict")
    fault_classification = (fault.get("fault") or {}).get("classification")
    gate_classification = (gate.get("fault_classification") or {}).get("classification")
    if settled.get("score") is not None:
        return "FAULT_HANDOFF_RECEIPT_CLASSIFICATION_MISMATCH"
    if verdict == "OJ_INFRASTRUCTURE_HOLD_REFERENCE_BASELINE_DRIFT":
        if fault_classification != verdict:
            return "FAULT_HANDOFF_RECEIPT_CLASSIFICATION_MISMATCH"
    elif (gate.get("reason") == "CANDIDATE_PROCESS_TERMINATION_REQUIRES_DEVICE_REQUALIFICATION"
          and verdict == "CANDIDATE_CORRECTNESS_FAILURE"
          and fault_classification == verdict and gate_classification == verdict):
        # A terminal correctness result is valid, but the device still needs
        # fresh-process requalification before the next queued request.
        pass
    elif (gate.get("reason") == "CANDIDATE_PROCESS_TERMINATION_REQUIRES_DEVICE_REQUALIFICATION"
          and verdict in {"OJ_INFRASTRUCTURE_HOLD_REFERENCE_CONTROL_MISMATCH",
                          "OJ_INFRASTRUCTURE_HOLD_CANDIDATE_TIMING_UNSTABLE"}):
        # An immutable, scoreless infrastructure outcome is *not* a Candidate
        # correctness/latency result. Its post-evaluation abort receipt requests
        # fresh Reference controls before ANY further measurement. R9.2 omitted
        # this terminal classification, leaving an otherwise valid handoff in
        # permanent HOLD. Keep every identity and device-safety check in place.
        identity = fault.get("worker_identity") or {}
        fault_detail = fault.get("fault") or {}
        job = fault.get("job_result") or {}
        candidate_sha = str(settled.get("candidate_sha256") or "")
        episode_id = str(settled.get("episode_id") or "")
        if (gate_classification != verdict or fault_classification != verdict or
                gate.get("fault_candidate_sha256") != candidate_sha or
                fault.get("schema") != "N0_CANDIDATE_ABORT_RECEIPT_V1" or
                fault.get("request_id") != request_id or
                fault.get("candidate_sha256") != candidate_sha or
                fault_detail.get("schema") != "N0_CUDA_FAULT_V1" or
                fault_detail.get("domain") != "SUPERVISOR" or
                fault_detail.get("attribution") != "UNRESOLVED" or
                fault_detail.get("stage") != "POST_EVALUATION_RECOVERY_GATE" or
                fault_detail.get("score_allowed") is not False or
                settled.get("candidate_failure_diagnostics") is not None or
                not episode_id or not candidate_sha or
                identity.get("request_id") != request_id or
                identity.get("episode_id") != episode_id or
                identity.get("candidate_sha256") != candidate_sha or
                job.get("returncode") != 0 or job.get("active_processes_after") != 0 or
                job.get("terminated_tree") is not True or
                job.get("timed_out") is not False):
            return "FAULT_HANDOFF_RECEIPT_CLASSIFICATION_MISMATCH"
    elif (gate.get("reason") == "CANDIDATE_PROCESS_TERMINATION_REQUIRES_DEVICE_REQUALIFICATION"
          and verdict == "CANDIDATE_CUDA_RUNTIME_FAILURE"):
        # A diagnosed CUDA candidate fault is a valid terminal candidate result,
        # but the device must be requalified before any queued request can run.
        identity = fault.get("worker_identity") or {}
        diagnostic = settled.get("candidate_failure_diagnostics") or {}
        candidate_sha = str(settled.get("candidate_sha256") or "")
        episode_id = str(settled.get("episode_id") or "")
        classification = str(fault_classification or "")
        if (not classification.startswith("CANDIDATE_EXECUTION_") or
                gate_classification != classification or
                not isinstance(fault.get("fault"), dict) or
                fault.get("fault", {}).get("schema") != "N0_CUDA_FAULT_V1" or
                fault.get("fault", {}).get("attribution") != "CANDIDATE" or
                fault.get("fault", {}).get("classifier_version") != "N0_CUDA_FAULT_CLASSIFIER_V1" or
                fault.get("fault", {}).get("stage") != "EVALUATION_CHILD" or
                fault.get("fault", {}).get("score_allowed") is not False or
                diagnostic != fault.get("fault") or
                not candidate_sha or fault.get("candidate_sha256") != candidate_sha or
                identity.get("candidate_sha256") != candidate_sha or
                not episode_id or identity.get("episode_id") != episode_id or
                identity.get("request_id") != request_id):
            return "FAULT_HANDOFF_RECEIPT_CLASSIFICATION_MISMATCH"
    elif (gate.get("reason") == "CANDIDATE_PROCESS_TERMINATION_REQUIRES_DEVICE_REQUALIFICATION"
          and verdict == "INVALID_SUBMISSION"):
        identity = fault.get("worker_identity") or {}
        diagnostic = settled.get("candidate_failure_diagnostics") or {}
        job = fault.get("job_result") or {}
        if (fault_classification != verdict or gate_classification != verdict or
                fault.get("request_id") != request_id or
                fault.get("candidate_sha256") != settled.get("candidate_sha256") or
                identity.get("request_id") != request_id or
                identity.get("candidate_sha256") != settled.get("candidate_sha256") or
                identity.get("episode_id") != settled.get("episode_id") or
                not diagnostic or diagnostic.get("status") != verdict or
                fault.get("fault", {}).get("schema") != "N0_CUDA_FAULT_V1" or
                fault.get("fault", {}).get("domain") != "SUPERVISOR" or
                fault.get("fault", {}).get("attribution") != "UNRESOLVED" or
                fault.get("fault", {}).get("stage") != "POST_EVALUATION_RECOVERY_GATE" or
                fault.get("fault", {}).get("score_allowed") is not False or
                job.get("returncode") != 0 or job.get("active_processes_after") != 0 or
                job.get("terminated_tree") is not False):
            return "FAULT_HANDOFF_RECEIPT_CLASSIFICATION_MISMATCH"
    else:
        return "FAULT_HANDOFF_RECEIPT_CLASSIFICATION_MISMATCH"
    job = fault.get("job_result") or {}
    if fault.get("request_id") != request_id or job.get("active_processes_after") != 0:
        return "FAULT_HANDOFF_CANDIDATE_TREE_NOT_PROVEN_EMPTY"
    if fault_classification != verdict and verdict != "CANDIDATE_CUDA_RUNTIME_FAILURE":
        return "FAULT_HANDOFF_FAULT_RECEIPT_MISMATCH"
    return "PASS"


def serving_handoff_kind(status: dict) -> str | None:
    detail = status.get("detail") or {}
    marker = detail.get("shutdown")
    if status.get("state") != "HANDOFF_REQUIRED":
        return None
    if marker == "FRESH_PROCESS_FAULT_RECOVERY_HANDOFF":
        return "CANDIDATE_FAULT"
    if marker == "FRESH_PROCESS_RECOVERY_HANDOFF":
        return "PROFILE"
    return None


def serving_exit_kind(returncode: int, stop_requested: bool) -> str:
    if returncode == 75:
        return "HANDOFF"
    if returncode == 20:
        return "CHILD_HOLD"
    if returncode == 0 and stop_requested:
        return "ORDERLY_STOP"
    return "UNEXPECTED_EXIT"


def validate_serving_child_identity(status: dict, child: dict, supervisor_pid: int, queue_root: Path) -> str:
    detail = status.get("detail") or {}
    process = detail.get("process_identity") or {}
    try:
        status_pid = int(status.get("pid") or 0)
        launcher_pid = int(child.get("pid") or 0)
        recorded_pid = int(process.get("pid") or 0)
        parent_pid = int(process.get("parent_pid") or 0)
    except (TypeError, ValueError):
        return "SERVING_CHILD_PID_INVALID"
    if not status_pid or status_pid != recorded_pid:
        return "SERVING_CHILD_PID_MISMATCH"
    proof = child.get("worker_identity_evidence") or {}
    if proof.get("captured_while_alive") is not True or int(proof.get("worker_pid") or 0) != recorded_pid:
        return "SERVING_CHILD_LIVE_IDENTITY_EVIDENCE_MISSING"
    if int(proof.get("launcher_pid") or 0) != launcher_pid or int(proof.get("supervisor_pid") or 0) != int(supervisor_pid):
        return "SERVING_CHILD_LAUNCH_CHAIN_IDENTITY_MISMATCH"
    worker_row = proof.get("worker_process") or {}
    launcher_row = proof.get("launcher_process") or {}
    try:
        observed_pid = int(worker_row.get("ProcessId") or 0)
        observed_parent = int(worker_row.get("ParentProcessId") or 0)
        observed_launcher = int(launcher_row.get("ProcessId") or 0)
        launcher_parent = int(launcher_row.get("ParentProcessId") or 0)
    except (TypeError, ValueError):
        return "SERVING_CHILD_LAUNCH_CHAIN_INVALID"
    if observed_pid != recorded_pid or parent_pid != observed_parent:
        return "SERVING_CHILD_OBSERVED_PID_MISMATCH"
    if launcher_pid == recorded_pid:
        if observed_parent != int(supervisor_pid):
            return "SERVING_CHILD_PARENT_PID_MISMATCH"
    elif observed_parent != launcher_pid or observed_launcher != launcher_pid or launcher_parent != int(supervisor_pid):
        return "SERVING_CHILD_PARENT_PID_MISMATCH"
    if str(Path(str(process.get("sys_executable") or "")).resolve()).casefold() != str(PYTHON.resolve()).casefold():
        return "SERVING_CHILD_PYTHON_IDENTITY_MISMATCH"
    command_line = str(process.get("command_line") or "")
    if str(WORKER).casefold() not in command_line.casefold():
        return "SERVING_CHILD_COMMAND_IDENTITY_MISMATCH"
    if str(Path(str(process.get("queue_root") or "")).resolve()).casefold() != str(queue_root.resolve()).casefold():
        return "SERVING_CHILD_QUEUE_IDENTITY_MISMATCH"
    worker_command = str(worker_row.get("CommandLine") or "")
    launcher_command = str(launcher_row.get("CommandLine") or "")
    if str(WORKER).casefold() not in worker_command.casefold():
        return "SERVING_CHILD_OBSERVED_COMMAND_MISMATCH"
    if launcher_pid != recorded_pid and str(WORKER).casefold() not in launcher_command.casefold():
        return "SERVING_CHILD_LAUNCHER_COMMAND_MISMATCH"
    return "PASS"


def launch_child(args, env: dict[str, str], *, recovery_only: bool,
                 bootstrap_qualification_only: bool = False) -> tuple[int, dict]:
    command = [str(PYTHON), "-u", str(WORKER), "--queue-root", str(args.queue_root),
               "--qualification-out", str(args.qualification_out), "--profile-whitelist", str(args.profile_whitelist),
               "--gpu-lease-path", str(args.gpu_lease_path), "--idle-seconds", str(args.idle_seconds)]
    if recovery_only:
        command.append("--recovery-only")
    if bootstrap_qualification_only:
        command.append("--bootstrap-qualification-only")
    child_env = dict(env)
    child_env["N0_LAUNCH_GENERATION"] = uuid.uuid4().hex
    generation = child_env["N0_LAUNCH_GENERATION"]
    log_root = Path(args.queue_root).resolve() / "supervisor_children"
    log_root.mkdir(parents=True, exist_ok=True)
    child_log_path = log_root / (generation + ".stdout_stderr.log")
    child_log = child_log_path.open("xb")
    proc = subprocess.Popen(command, cwd=str(INTEGRATION), env=child_env, close_fds=True,
                            stdout=child_log, stderr=subprocess.STDOUT,
                            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    status_path = Path(args.queue_root).resolve() / "service_status.json"
    identity_evidence = None
    identity_deadline = time.monotonic() + 20.0
    try:
        while proc.poll() is None:
            if identity_evidence is None and status_path.is_file():
                try:
                    status = json.loads(status_path.read_text(encoding="utf-8"))
                    process = (status.get("detail") or {}).get("process_identity") or {}
                    worker_pid = int(process.get("pid") or 0)
                    if worker_pid and int(status.get("pid") or 0) == worker_pid:
                        rows = process_snapshot()
                        by_pid = {int(row.get("ProcessId") or 0): row for row in rows}
                        worker_row = by_pid.get(worker_pid)
                        launcher_row = by_pid.get(int(proc.pid))
                        if worker_row and launcher_row:
                            worker_parent = int(worker_row.get("ParentProcessId") or 0)
                            launcher_parent = int(launcher_row.get("ParentProcessId") or 0)
                            direct = worker_pid == int(proc.pid) and worker_parent == os.getpid()
                            wrapped = worker_pid != int(proc.pid) and worker_parent == int(proc.pid) and launcher_parent == os.getpid()
                            if direct or wrapped:
                                identity_evidence = {"captured_while_alive": True, "captured_at_utc": now(),
                                    "supervisor_pid": os.getpid(), "launcher_pid": int(proc.pid),
                                    "worker_pid": worker_pid, "worker_process": worker_row,
                                    "launcher_process": launcher_row, "relationship": "DIRECT" if direct else "WINDOWS_VENV_LAUNCHER_PARENT"}
                except (OSError, ValueError, TypeError, json.JSONDecodeError):
                    pass
            if identity_evidence is None and time.monotonic() >= identity_deadline:
                break
            time.sleep(0.1)
        return_code = proc.wait()
    except BaseException:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=10)
        raise
    child_log.close()
    leftovers = wait_process_tree_empty(proc.pid, proc)
    if leftovers:
        raise RuntimeError("OJ_CHILD_PROCESS_TREE_NOT_EMPTY:" + json.dumps(leftovers, default=str))
    try:
        with child_log_path.open("rb") as stream:
            stream.seek(max(0, child_log_path.stat().st_size - 8192))
            output_tail = stream.read().decode("utf-8", errors="replace")
    except OSError as exc:
        output_tail = "CHILD_OUTPUT_READ_FAILED:" + type(exc).__name__ + ":" + str(exc)
    return return_code, {"pid": proc.pid, "returncode": return_code, "command": command,
                         "output_log_path": str(child_log_path), "output_log_sha256": sha_file(child_log_path),
                         "output_tail": output_tail,
                         "worker_identity_evidence": identity_evidence,
                         "finished_at_utc": now(), "descendants_after_exit": leftovers}


def write_unique_document(root: Path, prefix: str, body: dict) -> dict:
    receipt_id = uuid.uuid4().hex
    document = {**body, "receipt_id": receipt_id}
    document["receipt_sha256"] = hashlib.sha256(canonical_bytes(document)).hexdigest().upper()
    raw = canonical_bytes(document) + b"\n"
    path = root / (receipt_id + ".json")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    return {"path": str(path), "receipt_id": receipt_id,
            "receipt_sha256": document["receipt_sha256"],
            "receipt_file_sha256": sha_bytes(raw), "document": document}


def read_verified_document(path: Path, expected_file_sha: str) -> dict:
    if not path.is_file():
        raise BootstrapHoldError("QUALIFICATION_RECEIPT_MISSING")
    raw = path.read_bytes()
    if sha_bytes(raw) != str(expected_file_sha).upper():
        raise BootstrapHoldError("QUALIFICATION_RECEIPT_FILE_SHA_MISMATCH")
    document = json.loads(raw)
    receipt_sha = str(document.get("receipt_sha256") or "").upper()
    body = {k: v for k, v in document.items() if k != "receipt_sha256"}
    if not receipt_sha or hashlib.sha256(canonical_bytes(body)).hexdigest().upper() != receipt_sha:
        raise BootstrapHoldError("QUALIFICATION_RECEIPT_CONTENT_SHA_MISMATCH")
    if path.name != str(document.get("receipt_id") or "") + ".json":
        raise BootstrapHoldError("QUALIFICATION_RECEIPT_ID_PATH_MISMATCH")
    document["_verified_file_sha256"] = sha_bytes(raw)
    document["_verified_path"] = str(path.resolve())
    return document


def qualify_existing_fault_hold(args, env: dict[str, str], *, queue_root: Path, profile_root: Path,
                                gate_path: Path, status_path: Path, event,
                                supervisor_pid: int) -> int:
    process_rows = process_snapshot()
    recovery_child = None
    try:
        context = validate_existing_hold(queue_root, gate_path, args.gpu_lease_path, queue_root / "service_handoff_events.jsonl", process_rows)
        context_for_child = {k: v for k, v in context.items() if k != "queue_snapshot"}
        context_for_child["queue_snapshot_sha256"] = context["queue_snapshot"]["sha256"]
        child_env = dict(env)
        child_env["N0_FAULT_RECOVERY_CONTEXT_JSON"] = json.dumps(context_for_child, sort_keys=True, separators=(",", ":"))
        child_env["N0_PRESERVE_FAULT_GATE"] = "1"
        gate_before = gate_path.read_bytes()
        event("EXISTING_HOLD_BOOTSTRAP_VALIDATED", context=context_for_child)
        code, record = launch_child(args, child_env, recovery_only=True, bootstrap_qualification_only=True)
        recovery_child = record
        status = json.loads(status_path.read_text(encoding="utf-8")) if status_path.is_file() else {}
        child_identity = validate_serving_child_identity(status, record, supervisor_pid, queue_root)
        gate_after = gate_path.read_bytes() if gate_path.is_file() else b""
        if gate_after != gate_before:
            raise BootstrapHoldError("FAULT_GATE_CHANGED_DURING_QUALIFICATION")
        snapshot_after = queue_snapshot(queue_root)
        if snapshot_after["sha256"] != context["queue_snapshot"]["sha256"]:
            raise BootstrapHoldError("OJ_QUEUE_CHANGED_DURING_QUALIFICATION")
        if status.get("state") != "RECOVERY_QUALIFIED" or child_identity != "PASS" or code != 0:
            raise BootstrapHoldError("RECOVERY_CHILD_NOT_QUALIFIED:" + child_identity + ":" + str(code))
        recovery = (status.get("detail") or {}).get("post_profile_recovery") or {}
        recovery_path = Path(str(recovery.get("recovery_receipt_path") or "")).resolve()
        if not recovery_path.is_relative_to((profile_root / "fault_recovery_receipts").resolve()):
            raise BootstrapHoldError("RECOVERY_RECEIPT_PATH_OUTSIDE_PROFILE_ROOT")
        recovery_validation = validate_recovery_receipt(recovery_path, context_for_child, status,
            recovery.get("recovery_receipt_file_sha256"))
        qualification = {"schema": "N0_EXISTING_HOLD_BOOTSTRAP_QUALIFICATION_V1", "status": "PASS",
            "run_id": args.pool20_run_id, "fault_request_id": context["fault_request_id"],
            "candidate_sha256": context["candidate_sha256"], "fault_receipt_sha256": context["fault_receipt_sha256"],
            "oj_receipt_sha256": context["oj_receipt_sha256"], "fault_gate_sha256": context["fault_gate_sha256"],
            "queue_snapshot_sha256": context["queue_snapshot"]["sha256"],
            "queue_snapshot_counts": {k: context["queue_snapshot"][k] for k in
                ("request_count", "episode_count", "queued_count", "hold_unknown_count", "running_count", "candidate_budget_used")},
            "recovery_receipt_path": str(recovery_path),
            "recovery_receipt_sha256": recovery_validation["recovery_receipt_sha256"],
            "recovery_receipt_file_sha256": recovery_validation["recovery_receipt_file_sha256"],
            "recovery_id": recovery_validation["receipt"]["recovery_id"],
            "recovery_status": "PASS", "required_consecutive_controls": 3,
            "consecutive_passes": recovery_validation["consecutive_passes"],
            "recovery_child": record, "recovery_child_status": status,
            "recovery_child_identity": child_identity, "qualified_at_utc": now(),
            "expires_at_utc": (utc_now().timestamp() + 300)}
        write = write_unique_document(profile_root / "bootstrap_qualifications", "qualification", qualification)
        atomic_json(status_path, {"schema": "N0_OJ_WORKER_STATUS_V1", "state": "RECOVERY_QUALIFIED",
            "pid": supervisor_pid, "heartbeat_utc": now(), "queue_root": str(queue_root),
            "release_id": RELEASE_ID, "manifest_sha256": sha(MANIFEST.read_bytes()),
            "detail": {"shutdown": "RECOVERY_QUALIFICATION_ONLY_NO_QUEUE_CLAIM",
                       "qualification_receipt_path": write["path"],
            "qualification_receipt_file_sha256": write["receipt_file_sha256"],
                       "queue_snapshot_unchanged": True}})
        event("EXISTING_HOLD_BOOTSTRAP_QUALIFIED", qualification_receipt_path=write["path"],
              qualification_receipt_file_sha256=write["receipt_file_sha256"], queue_snapshot=write["document"]["queue_snapshot_sha256"])
        return 0
    except Exception as exc:
        failure = type(exc).__name__ + ":" + str(exc)
        evidence = {"schema": "N0_EXISTING_HOLD_BOOTSTRAP_QUALIFICATION_V1", "status": "HOLD",
                    "run_id": args.pool20_run_id, "failure": failure, "qualified_at_utc": now()}
        try:
            write = write_unique_document(profile_root / "bootstrap_qualifications", "qualification", evidence)
            atomic_json(status_path, {"schema": "N0_OJ_WORKER_STATUS_V1", "state": "HOLD",
                "pid": supervisor_pid, "heartbeat_utc": now(), "queue_root": str(queue_root),
                "release_id": RELEASE_ID, "manifest_sha256": sha(MANIFEST.read_bytes()),
                "detail": {"shutdown": "EXISTING_HOLD_BOOTSTRAP_NOT_QUALIFIED", "failure": failure,
                           "qualification_receipt_path": write["path"],
                           "qualification_receipt_file_sha256": write["receipt_file_sha256"]}})
        except Exception as write_exc:
            event("EXISTING_HOLD_BOOTSTRAP_RECEIPT_WRITE_FAILED", error=type(write_exc).__name__ + ":" + str(write_exc))
        event("EXISTING_HOLD_BOOTSTRAP_BLOCKED", failure=failure, recovery_child=recovery_child)
        return 20


def authorize_serving_start(auth_path: Path, *, queue_root: Path, profile_root: Path,
                            gate_path: Path, args, event) -> dict:
    """Hold SQLite's writer reservation from final snapshot check through gate transition.

    Queue mutations (claim/submit/settle) use this same SQLite database.  A
    BEGIN IMMEDIATE reservation prevents a writer from changing the qualified
    snapshot between RescueValidateOnly validation and the READY transition.
    The transaction itself performs no queue or budget mutation.
    """
    db_path = queue_root / "n0_runtime.sqlite3"
    db = None
    try:
        db = sqlite3.connect(db_path, timeout=15, isolation_level=None)
        db.execute("BEGIN IMMEDIATE")
    except sqlite3.Error as exc:
        if db is not None:
            db.close()
        raise BootstrapHoldError("START_AUTHORIZATION_QUEUE_LOCK_FAILED:" + type(exc).__name__) from exc
    try:
        result = _authorize_serving_start_under_queue_lock(
            auth_path, queue_root=queue_root, profile_root=profile_root,
            gate_path=gate_path, args=args, event=event)
        db.execute("COMMIT")
        return result
    except BaseException:
        if db.in_transaction:
            db.execute("ROLLBACK")
        raise
    finally:
        db.close()


def _authorize_serving_start_under_queue_lock(auth_path: Path, *, queue_root: Path, profile_root: Path,
                                               gate_path: Path, args, event) -> dict:
    try:
        auth = json.loads(auth_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        raise BootstrapHoldError("START_AUTHORIZATION_INVALID_JSON")
    qualification_path = Path(str(auth.get("qualification_receipt_path") or "")).resolve()
    if not qualification_path.is_relative_to((profile_root / "bootstrap_qualifications").resolve()):
        raise BootstrapHoldError("QUALIFICATION_PATH_OUTSIDE_PROFILE_ROOT")
    qualification = read_verified_document(qualification_path, auth.get("qualification_receipt_file_sha256"))
    if qualification.get("status") != "PASS":
        raise BootstrapHoldError("BOOTSTRAP_QUALIFICATION_NOT_PASS")
    if not args.pool20_run_id or args.pool20_run_id != qualification.get("run_id"):
        raise BootstrapHoldError("POOL20_RUN_ID_NOT_EXPLICIT_OR_MISMATCHED")
    age = (utc_now().timestamp() - float(qualification.get("expires_at_utc") or 0))
    if age > 0 or age < -600:
        raise BootstrapHoldError("BOOTSTRAP_QUALIFICATION_EXPIRED_OR_FUTURE_DATED")
    current_gate_raw = gate_path.read_bytes() if gate_path.is_file() else b""
    if sha_bytes(current_gate_raw) != qualification.get("fault_gate_sha256"):
        raise BootstrapHoldError("FAULT_GATE_CHANGED_AFTER_QUALIFICATION")
    process_rows = process_snapshot()
    context = validate_existing_hold(queue_root, gate_path, args.gpu_lease_path,
                                     queue_root / "service_handoff_events.jsonl", process_rows)
    current_snapshot = context["queue_snapshot"]
    if current_snapshot["sha256"] != qualification.get("queue_snapshot_sha256"):
        raise BootstrapHoldError("OJ_QUEUE_CHANGED_AFTER_QUALIFICATION")
    rescue_auth = validate_rescue_authorization(auth_path, qualification, current_snapshot)
    if rescue_auth.get("run_id") != args.pool20_run_id:
        raise BootstrapHoldError("RESCUE_VALIDATEONLY_RUN_ID_MISMATCH")
    auth_id = str(rescue_auth.get("authorization_id"))
    used_path = profile_root / "bootstrap_start_authorizations" / (auth_id + ".json")
    if used_path.exists():
        raise BootstrapHoldError("START_AUTHORIZATION_ALREADY_USED")
    auth_raw = auth_path.read_bytes()
    auth_sha = sha_bytes(auth_raw)
    consumed_doc = {
        "schema": "N0_POOL20_RECOVERY_START_AUTHORIZATION_CONSUMED_V1",
        "authorization_id": auth_id, "authorization_file_sha256": auth_sha,
        "qualification_receipt_file_sha256": qualification["_verified_file_sha256"],
        "rescue_validateonly_receipt_sha256": rescue_auth["rescue_validateonly_receipt_sha256"],
        "authorized_by": rescue_auth["authorized_by"], "consumed_at_utc": now()}
    consumed_doc["receipt_sha256"] = hashlib.sha256(canonical_bytes(consumed_doc)).hexdigest().upper()
    consumed_raw = canonical_bytes(consumed_doc) + b"\n"
    used_path.parent.mkdir(parents=True, exist_ok=True)
    with used_path.open("xb") as stream:
        stream.write(consumed_raw)
        stream.flush()
        os.fsync(stream.fileno())
    gate = json.loads(current_gate_raw)
    gate.update({"status": "READY", "bootstrap_qualification_receipt_file_sha256": qualification["_verified_file_sha256"],
                 "rescue_validateonly_receipt_sha256": rescue_auth["rescue_validateonly_receipt_sha256"],
                 "start_authorization_id": auth_id, "start_authorized_by": rescue_auth["authorized_by"],
                 "start_authorized_at_utc": now()})
    atomic_json(gate_path, gate)
    event("EXISTING_HOLD_START_AUTHORIZED", authorization_id=auth_id,
          qualification_receipt_file_sha256=qualification["_verified_file_sha256"],
          rescue_validateonly_receipt_sha256=rescue_auth["rescue_validateonly_receipt_sha256"])
    return {"authorization_id": auth_id, "qualification": qualification, "rescue_validateonly": rescue_auth}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--queue-root", required=True, type=Path)
    parser.add_argument("--qualification-out", required=True, type=Path)
    parser.add_argument("--profile-whitelist", required=True, type=Path)
    parser.add_argument("--gpu-lease-path", required=True, type=Path)
    parser.add_argument("--idle-seconds", type=float, default=0.5)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--qualify-existing-hold", action="store_true")
    mode.add_argument("--serve-authorized-json", type=Path)
    parser.add_argument("--pool20-run-id")
    args = parser.parse_args()
    for path in (PYTHON, WORKER, MANIFEST, args.profile_whitelist):
        if not path.is_file():
            raise RuntimeError("SUPERVISOR_REQUIRED_FILE_MISSING:" + str(path))
    manifest_raw = MANIFEST.read_bytes()
    manifest = json.loads(manifest_raw)
    sidecar = MANIFEST.with_name("RELEASE_MANIFEST.json.sha256")
    expected = sidecar.read_text(encoding="ascii").split()[0].upper() if sidecar.is_file() else ""
    if manifest.get("release_id") != RELEASE_ID or not expected or sha(manifest_raw) != expected:
        raise RuntimeError("SUPERVISOR_INTEGRATION_MANIFEST_IDENTITY_MISMATCH")
    queue_root = args.queue_root.resolve()
    profile_root = Path(os.environ.get("N0_PROFILE_STATE_ROOT", str(ROOT / "state" / "n0_oj_profiles"))).resolve()
    gate_path = profile_root / "timing_gate.json"
    status_path = queue_root / "service_status.json"
    env = dict(os.environ)
    env["N0_PROFILE_FRESH_PROCESS_HANDOFF"] = "1"
    env["N0_GPU_LEASE_PATH"] = str(args.gpu_lease_path.resolve())
    env["N0_OJ_QUEUE_ROOT"] = str(queue_root)
    env["N0_PROFILE_STATE_ROOT"] = str(profile_root)
    env["N0_GPU_TIMING_GATE_PATH"] = str(gate_path)
    env["PATH"] = str(PYTHON.parent) + os.pathsep + env.get("PATH", "")
    event_path = queue_root / "service_handoff_events.jsonl"
    own_process = next((row for row in process_snapshot() if int(row.get("ProcessId") or 0) == os.getpid()), None)
    if not own_process:
        raise RuntimeError("SUPERVISOR_PROCESS_IDENTITY_UNAVAILABLE")
    identity = {"supervisor_pid": os.getpid(), "supervisor_parent_pid": int(own_process.get("ParentProcessId") or 0),
                "supervisor_executable_path": own_process.get("ExecutablePath"),
                "supervisor_command_line": own_process.get("CommandLine"), "release_id": RELEASE_ID,
                "manifest_sha256": sha(manifest_raw), "queue_root": str(queue_root),
                "gpu_lease_path": str(args.gpu_lease_path.resolve()), "started_at_utc": now()}

    def event(kind: str, **details) -> None:
        event_path.parent.mkdir(parents=True, exist_ok=True)
        with event_path.open("a", encoding="utf-8", newline="\n") as stream:
            stream.write(json.dumps({"time_utc": now(), "event": kind, **details}, sort_keys=True) + "\n")
            stream.flush()
            os.fsync(stream.fileno())

    event("SUPERVISOR_STARTED", identity=identity)
    if args.qualify_existing_hold:
        if not args.pool20_run_id:
            event("EXISTING_HOLD_BOOTSTRAP_BLOCKED", failure="POOL20_RUN_ID_REQUIRED")
            return 20
        return qualify_existing_fault_hold(args, env, queue_root=queue_root, profile_root=profile_root,
                                           gate_path=gate_path, status_path=status_path, event=event,
                                           supervisor_pid=os.getpid())
    if args.serve_authorized_json:
        try:
            authorization = authorize_serving_start(args.serve_authorized_json, queue_root=queue_root,
                profile_root=profile_root, gate_path=gate_path, args=args, event=event)
            event("AUTHORIZED_SERVING_START_VALIDATED", authorization_id=authorization["authorization_id"])
        except Exception as exc:
            failure = type(exc).__name__ + ":" + str(exc)
            atomic_json(status_path, {"schema": "N0_OJ_WORKER_STATUS_V1", "state": "HOLD",
                "pid": os.getpid(), "heartbeat_utc": now(), "queue_root": str(queue_root),
                "release_id": RELEASE_ID, "manifest_sha256": sha(manifest_raw),
                "detail": {"supervisor_identity": identity, "shutdown": "EXPLICIT_START_AUTHORIZATION_REJECTED",
                           "failure": failure}})
            event("AUTHORIZED_SERVING_START_BLOCKED", failure=failure)
            return 20
    initial_gate = json.loads(gate_path.read_text(encoding="utf-8")) if gate_path.is_file() else {"status": "READY"}
    if (initial_gate.get("schema") == "N0_GPU_FAULT_RECOVERY_GATE_V1" and
            initial_gate.get("status") == "HOLD" and not args.serve_authorized_json):
        atomic_json(status_path, {"schema": "N0_OJ_WORKER_STATUS_V1", "state": "HOLD",
            "pid": os.getpid(), "heartbeat_utc": now(), "queue_root": str(queue_root),
            "release_id": RELEASE_ID, "manifest_sha256": sha(manifest_raw),
            "detail": {"supervisor_identity": identity,
                       "shutdown": "EXISTING_FAULT_HOLD_REQUIRES_QUALIFICATION_ONLY"}})
        event("SUPERVISOR_HOLD", reason="EXISTING_FAULT_HOLD_REQUIRES_QUALIFICATION_ONLY")
        return 20
    while True:
        if (queue_root / "STOP_REQUESTED").exists():
            atomic_json(status_path, {"schema": "N0_OJ_WORKER_STATUS_V1", "state": "STOPPED",
                "pid": os.getpid(), "heartbeat_utc": now(), "queue_root": str(queue_root),
                "release_id": RELEASE_ID, "manifest_sha256": sha(manifest_raw),
                "detail": {"supervisor_identity": identity, "shutdown": "ORDERLY_STOP_MARKER"}})
            event("SUPERVISOR_STOPPED", identity=identity)
            return 0
        gate = json.loads(gate_path.read_text(encoding="utf-8")) if gate_path.is_file() else {"status": "READY"}
        if gate.get("status") != "READY":
            event("FRESH_PROCESS_RECOVERY_STARTED", gate=gate)
            code, record = launch_child(args, env, recovery_only=True)
            event("FRESH_PROCESS_RECOVERY_FINISHED", child=record)
            recovery_status = json.loads(status_path.read_text(encoding="utf-8")) if status_path.is_file() else {}
            recovery_identity = validate_serving_child_identity(recovery_status, record, os.getpid(), queue_root)
            gate = json.loads(gate_path.read_text(encoding="utf-8")) if gate_path.is_file() else {"status": "MISSING"}
            if code != 0 or recovery_identity != "PASS" or gate.get("status") != "READY":
                atomic_json(status_path, {"schema": "N0_OJ_WORKER_STATUS_V1", "state": "HOLD",
                    "pid": os.getpid(), "heartbeat_utc": now(), "queue_root": str(queue_root),
                    "release_id": RELEASE_ID, "manifest_sha256": sha(manifest_raw),
                    "detail": {"supervisor_identity": identity, "recovery_child": record, "timing_gate": gate,
                               "recovery_child_identity": recovery_identity,
                               "shutdown": "FRESH_PROCESS_RECOVERY_NOT_READY"}})
                event("SUPERVISOR_HOLD", recovery_child=record, timing_gate=gate,
                      recovery_child_identity=recovery_identity)
                return 20
        code, record = launch_child(args, env, recovery_only=False)
        event("SERVING_WORKER_EXITED", child=record)
        exit_kind = serving_exit_kind(code, (queue_root / "STOP_REQUESTED").exists())
        if exit_kind == "HANDOFF":
            try:
                status = json.loads(status_path.read_text(encoding="utf-8"))
                detail = status.get("detail") or {}
                child_identity = validate_serving_child_identity(status, record, os.getpid(), queue_root)
                if child_identity != "PASS":
                    raise RuntimeError(child_identity)
                handoff_kind = serving_handoff_kind(status)
                if handoff_kind == "CANDIDATE_FAULT":
                    gate = json.loads(gate_path.read_text(encoding="utf-8")) if gate_path.is_file() else {}
                    request_id = str(detail.get("fault_request_id") or "")
                    settled = request_state(queue_root, request_id)
                    fault_path = (queue_root / "candidate_supervision" / request_id / "fault_receipt.json").resolve()
                    if not fault_path.is_file():
                        raise RuntimeError("FAULT_HANDOFF_RECEIPT_MISSING")
                    fault = json.loads(fault_path.read_text(encoding="utf-8"))
                    decision = validate_fault_recovery_handoff(status, gate, settled, fault)
                    if decision != "PASS":
                        raise RuntimeError(decision)
                    context = validate_existing_hold(queue_root, gate_path, args.gpu_lease_path,
                        event_path, process_snapshot())
                    if not same_sha256(context.get("oj_receipt_sha256"), settled.get("receipt_sha256")):
                        raise RuntimeError("FAULT_HANDOFF_OJ_RECEIPT_SHA_CHANGED")
                    event("FAULT_RECOVERY_HANDOFF_VERIFIED", child=record, settled_request=settled,
                          fault_receipt_sha256=context["fault_receipt_sha256"], gate=gate)
                    recovery_env = dict(env)
                    recovery_env["N0_FAULT_RECOVERY_CONTEXT_JSON"] = json.dumps(
                        {k: v for k, v in context.items() if k != "queue_snapshot"} |
                        {"queue_snapshot_sha256": context["queue_snapshot"]["sha256"]},
                        sort_keys=True, separators=(",", ":"))
                    code, recovery_record = launch_child(args, recovery_env, recovery_only=True)
                    recovery_status = json.loads(status_path.read_text(encoding="utf-8")) if status_path.is_file() else {}
                    recovery_identity = validate_serving_child_identity(recovery_status, recovery_record,
                                                                         os.getpid(), queue_root)
                    gate = json.loads(gate_path.read_text(encoding="utf-8")) if gate_path.is_file() else {}
                    recovery = (recovery_status.get("detail") or {}).get("post_profile_recovery") or {}
                    recovery_path = Path(str(recovery.get("recovery_receipt_path") or "")).resolve()
                    try:
                        recovery_validation = validate_recovery_receipt(recovery_path, context, recovery_status,
                            recovery.get("recovery_receipt_file_sha256"), expected_worker_state="READY")
                        recovery_error = None
                    except Exception as exc:
                        recovery_validation = {}
                        recovery_error = type(exc).__name__ + ":" + str(exc)
                    if (code != 0 or recovery_identity != "PASS" or gate.get("status") != "READY" or
                            recovery_validation.get("receipt", {}).get("recovery_id") != gate.get("recovery_receipt_id") or
                            not same_sha256(recovery_validation.get("recovery_receipt_sha256"), gate.get("recovery_receipt_sha256")) or
                            recovery_error is not None):
                        atomic_json(status_path, {"schema": "N0_OJ_WORKER_STATUS_V1", "state": "HOLD",
                            "pid": os.getpid(), "heartbeat_utc": now(), "queue_root": str(queue_root),
                            "release_id": RELEASE_ID, "manifest_sha256": sha(manifest_raw),
                            "detail": {"supervisor_identity": identity, "recovery_child": recovery_record,
                                       "timing_gate": gate, "recovery_receipt": recovery,
                                       "recovery_child_identity": recovery_identity,
                                       "recovery_validation_error": recovery_error,
                                       "shutdown": "FRESH_PROCESS_FAULT_RECOVERY_NOT_READY"}})
                        event("SUPERVISOR_HOLD", recovery_child=recovery_record, timing_gate=gate,
                              recovery_receipt=recovery,recovery_child_identity=recovery_identity)
                        return 22
                    event("FRESH_PROCESS_FAULT_RECOVERY_FINISHED", child=recovery_record,
                          timing_gate=gate, recovery_receipt_sha256=recovery_validation.get("recovery_receipt_sha256"),
                          recovery_receipt_file_sha256=recovery_validation.get("recovery_receipt_file_sha256"))
                    continue
                request_id = str(detail.get("profile_request_id") or "")
                if handoff_kind != "PROFILE" or not request_id:
                    raise RuntimeError("HANDOFF_CHILD_EXIT_IDENTITY_MISMATCH")
                settled = request_state(queue_root, request_id)
                gate = json.loads(gate_path.read_text(encoding="utf-8")) if gate_path.is_file() else {}
                if gate.get("status") != "HANDOFF_REQUIRED" or gate.get("last_profile_receipt_id") != request_id:
                    raise RuntimeError("HANDOFF_PROFILE_GATE_IDENTITY_MISMATCH")
                event("PROFILE_HANDOFF_VERIFIED", child=record, settled_request=settled, gate=gate)
            except Exception as exc:
                atomic_json(status_path, {"schema": "N0_OJ_WORKER_STATUS_V1", "state": "HOLD",
                    "pid": os.getpid(), "heartbeat_utc": now(), "queue_root": str(queue_root),
                    "release_id": RELEASE_ID, "manifest_sha256": sha(manifest_raw),
                    "detail": {"supervisor_identity": identity, "error": type(exc).__name__ + ":" + str(exc),
                               "shutdown": "PROFILE_HANDOFF_VALIDATION_FAILED"}})
                event("SUPERVISOR_HOLD", error=type(exc).__name__ + ":" + str(exc))
                return 21
            code, recovery_record = launch_child(args, env, recovery_only=True)
            recovery_status = json.loads(status_path.read_text(encoding="utf-8")) if status_path.is_file() else {}
            recovery_identity = validate_serving_child_identity(recovery_status, recovery_record,
                                                                 os.getpid(), queue_root)
            gate = json.loads(gate_path.read_text(encoding="utf-8")) if gate_path.is_file() else {}
            event("FRESH_PROCESS_RECOVERY_FINISHED", child=recovery_record, timing_gate=gate)
            if code != 0 or recovery_identity != "PASS" or gate.get("status") != "READY":
                atomic_json(status_path, {"schema": "N0_OJ_WORKER_STATUS_V1", "state": "HOLD",
                    "pid": os.getpid(), "heartbeat_utc": now(), "queue_root": str(queue_root),
                    "release_id": RELEASE_ID, "manifest_sha256": sha(manifest_raw),
                    "detail": {"supervisor_identity": identity, "recovery_child": recovery_record,
                               "timing_gate": gate, "recovery_child_identity": recovery_identity,
                               "shutdown": "FRESH_PROCESS_RECOVERY_NOT_READY"}})
                event("SUPERVISOR_HOLD", recovery_child=recovery_record, timing_gate=gate,
                      recovery_child_identity=recovery_identity)
                return 22
            continue
        status = json.loads(status_path.read_text(encoding="utf-8")) if status_path.is_file() else {}
        child_identity = validate_serving_child_identity(status, record, os.getpid(), queue_root)
        if child_identity != "PASS":
            atomic_json(status_path, {"schema": "N0_OJ_WORKER_STATUS_V1", "state": "HOLD",
                "pid": os.getpid(), "heartbeat_utc": now(), "queue_root": str(queue_root),
                "release_id": RELEASE_ID, "manifest_sha256": sha(manifest_raw),
                "detail": {"supervisor_identity": identity, "worker_exit": record,
                           "worker_child_identity": child_identity,
                           "shutdown": "SERVING_CHILD_IDENTITY_UNVERIFIABLE"}})
            event("SUPERVISOR_HOLD", worker_exit=record, worker_child_identity=child_identity)
            return 24
        if exit_kind == "CHILD_HOLD":
            if status.get("state") != "HOLD":
                atomic_json(status_path, {"schema": "N0_OJ_WORKER_STATUS_V1", "state": "HOLD",
                    "pid": os.getpid(), "heartbeat_utc": now(), "queue_root": str(queue_root),
                    "release_id": RELEASE_ID, "manifest_sha256": sha(manifest_raw),
                    "detail": {"supervisor_identity": identity, "worker_exit": record,
                               "shutdown": "CHILD_HOLD_EXIT_WITHOUT_HOLD_STATUS"}})
                event("SUPERVISOR_HOLD", worker_exit=record, reason="CHILD_HOLD_EXIT_WITHOUT_HOLD_STATUS")
                return 24
            atomic_json(status_path, {"schema": "N0_OJ_WORKER_STATUS_V1", "state": "HOLD",
                "pid": os.getpid(), "heartbeat_utc": now(), "queue_root": str(queue_root),
                "release_id": RELEASE_ID, "manifest_sha256": sha(manifest_raw),
                "detail": {"supervisor_identity": identity, "worker_exit": record,
                           "worker_hold_detail": status.get("detail"),
                           "shutdown": "SERVING_WORKER_REQUESTED_HOLD"}})
            event("SUPERVISOR_HOLD", worker_exit=record, worker_hold_detail=status.get("detail"))
            return 20
        if exit_kind == "ORDERLY_STOP":
            atomic_json(status_path, {"schema": "N0_OJ_WORKER_STATUS_V1", "state": "STOPPED",
                "pid": os.getpid(), "heartbeat_utc": now(), "queue_root": str(queue_root),
                "release_id": RELEASE_ID, "manifest_sha256": sha(manifest_raw),
                "detail": {"supervisor_identity": identity, "worker_exit": record,
                           "shutdown": "ORDERLY_STOP_MARKER"}})
            event("SUPERVISOR_STOPPED", worker_exit=record)
            return 0
        atomic_json(status_path, {"schema": "N0_OJ_WORKER_STATUS_V1", "state": "DEGRADED",
            "pid": os.getpid(), "heartbeat_utc": now(), "queue_root": str(queue_root),
            "release_id": RELEASE_ID, "manifest_sha256": sha(manifest_raw),
            "detail": {"supervisor_identity": identity, "worker_exit": record,
                       "last_worker_state": status.get("state"), "shutdown": "UNEXPECTED_SERVING_CHILD_EXIT"}})
        event("SUPERVISOR_DEGRADED", worker_exit=record, last_worker_state=status.get("state"))
        return 23


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(type(exc).__name__ + ":" + str(exc), file=sys.stderr, flush=True)
        sys.exit(24)
