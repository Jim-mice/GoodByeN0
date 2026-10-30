from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


MAX_QUALIFICATION_AGE_SECONDS = 300
REQUIRED_CONTROLS = 3
MAX_CONTROLS = 5
MAX_REFERENCE_DEVIATION = 0.02


class BootstrapHoldError(RuntimeError):
    pass


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def sha_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest().upper()


def sha_file(path: Path) -> str:
    return sha_bytes(path.read_bytes())


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("TIMESTAMP_MISSING_TIMEZONE")
    return parsed.astimezone(timezone.utc)


def _fail(code: str) -> None:
    raise BootstrapHoldError(code)


def queue_snapshot(queue_root: Path) -> dict[str, Any]:
    db_path = queue_root / "n0_runtime.sqlite3"
    if not db_path.is_file():
        _fail("OJ_DATABASE_MISSING")
    con = sqlite3.connect(db_path.resolve().as_uri() + "?mode=ro", uri=True, timeout=10)
    con.row_factory = sqlite3.Row
    try:
        integrity = con.execute("PRAGMA integrity_check").fetchone()[0]
        if integrity != "ok":
            _fail("OJ_DATABASE_INTEGRITY_FAILED")
        request_rows = [dict(r) for r in con.execute(
            "SELECT request_id,episode_id,invocation_id,candidate_id,candidate_sha256,state,receipt_sha256,"
            "worker_id,worker_pid,worker_start_utc,lease_id,queue_sequence FROM oj_requests ORDER BY request_id")]
        episode_rows = [dict(r) for r in con.execute(
            "SELECT episode_id,state,candidates_used,max_candidates,approved_allocation_sha256 "
            "FROM episodes ORDER BY episode_id")]
        state_counts = dict(con.execute("SELECT state,COUNT(*) FROM oj_requests GROUP BY state").fetchall())
    except sqlite3.Error as exc:
        _fail("OJ_READ_ONLY_SNAPSHOT_FAILED:" + type(exc).__name__)
    finally:
        con.close()
    payload = {"requests": request_rows, "episodes": episode_rows, "request_state_counts": state_counts}
    return {"sha256": sha_bytes(canonical_bytes(payload)), "integrity_check": integrity,
            "request_count": len(request_rows), "episode_count": len(episode_rows),
            "queued_count": state_counts.get("QUEUED", 0),
            "hold_unknown_count": state_counts.get("HOLD_UNKNOWN", 0),
            "running_count": state_counts.get("RUNNING", 0),
            "candidate_budget_used": sum(int(r.get("candidates_used") or 0) for r in episode_rows),
            "payload": payload}


def _receipt_checks(row: dict[str, Any]) -> dict[str, Any]:
    if row.get("state") not in {"COMPLETED", "CONSUMED"}:
        _fail("FAULT_REQUEST_NOT_TERMINAL")
    if not row.get("receipt_sha256") or not row.get("receipt_json"):
        _fail("FAULT_REQUEST_RECEIPT_MISSING")
    try:
        receipt = json.loads(row["receipt_json"])
    except (TypeError, json.JSONDecodeError):
        _fail("OJ_RECEIPT_JSON_INVALID")
    if sha_bytes(canonical_bytes(receipt)) != str(row["receipt_sha256"]).upper():
        _fail("OJ_RECEIPT_CONTENT_SHA_MISMATCH")
    inner_sha = receipt.get("receipt_sha256")
    inner_body = {k: v for k, v in receipt.items() if k != "receipt_sha256"}
    embedded_sha_valid = bool(inner_sha and sha_bytes(canonical_bytes(inner_body)) == str(inner_sha).upper())
    embedded_sha_status = "VALID" if embedded_sha_valid else "INVALID"
    if not embedded_sha_valid:
        diagnostic = receipt.get("candidate_failure_diagnostics") or {}
        legacy_r6_candidate_fault = (
            receipt.get("verdict") == "CANDIDATE_CUDA_RUNTIME_FAILURE" and
            receipt.get("score") is None and
            str(diagnostic.get("classification") or "").startswith("CANDIDATE_EXECUTION_") and
            diagnostic.get("schema") == "N0_CUDA_FAULT_V1" and
            diagnostic.get("attribution") == "CANDIDATE" and
            diagnostic.get("classifier_version") == "N0_CUDA_FAULT_CLASSIFIER_V1" and
            diagnostic.get("stage") == "EVALUATION_CHILD" and
            diagnostic.get("score_allowed") is False
        )
        if not legacy_r6_candidate_fault:
            _fail("OJ_RECEIPT_EMBEDDED_SHA_MISMATCH")
        # R6 wrapped an evaluator result that already carried receipt_sha256,
        # then hashed the whole object again without removing the old value.
        # The immutable DB hash above remains the authority; the fault/gate
        # identity is verified separately before any requalification is issued.
        embedded_sha_status = "LEGACY_R6_CUDA_FAULT_DOUBLE_HASH"
    for key, expected in (("request_id", row["request_id"]), ("episode_id", row["episode_id"]),
                          ("candidate_id", row["candidate_id"]), ("candidate_sha256", row["candidate_sha256"])):
        if receipt.get(key) != expected:
            _fail("OJ_RECEIPT_IDENTITY_MISMATCH:" + key)
    if receipt.get("score") is not None:
        _fail("FAULT_REQUEST_MUST_NOT_HAVE_SCORE")
    evidence = receipt.get("measurement_evidence") or {}
    baseline_id = evidence.get("baseline_id") or (evidence.get("reference_control") or {}).get("baseline_id")
    return {"receipt": receipt, "receipt_sha256": str(row["receipt_sha256"]).upper(),
            "embedded_sha_status": embedded_sha_status, "baseline_id": baseline_id}


def _validate_fault_receipt_classification(gate: dict[str, Any], request: dict[str, Any],
                                           receipt: dict[str, Any], fault: dict[str, Any]) -> None:
    verdict = receipt.get("verdict")
    diagnostic = receipt.get("candidate_failure_diagnostics") or {}
    fault_detail = fault.get("fault") or {}
    classification = fault_detail.get("classification")
    if receipt.get("score") is not None:
        _fail("FAULT_REQUEST_MUST_NOT_HAVE_SCORE")
    if verdict == "OJ_INFRASTRUCTURE_HOLD_REFERENCE_BASELINE_DRIFT":
        if classification != verdict:
            _fail("FAULT_RECEIPT_CLASSIFICATION_MISMATCH")
        return
    if verdict in {"OJ_INFRASTRUCTURE_HOLD_REFERENCE_CONTROL_MISMATCH",
                   "OJ_INFRASTRUCTURE_HOLD_CANDIDATE_TIMING_UNSTABLE"}:
        # Infrastructure Reference disagreement is a scoreless OJ outcome,
        # NOT a Candidate diagnosis. An already-authenticated immutable receipt
        # may legitimately have no candidate_failure_diagnostics. The Abort
        # Receipt independently binds the same request, Candidate and device
        # recovery stage; the normal process-tree, Lease and 3/3 Reference
        # qualification checks still apply in validate_existing_hold().
        identity = fault.get("worker_identity") or {}
        job = fault.get("job_result") or {}
        diagnostic_status = (receipt.get("diagnostics") or {}).get("status")
        if (gate.get("reason") != "CANDIDATE_PROCESS_TERMINATION_REQUIRES_DEVICE_REQUALIFICATION" or
                gate.get("fault_request_id") != request.get("request_id") or
                gate.get("fault_candidate_sha256") != request.get("candidate_sha256") or
                gate.get("fault_classification", {}).get("classification") != verdict or
                classification != verdict or
                receipt.get("schema") != "N0_OJ_R5_IMMUTABLE_RECEIPT_V1" or
                (verdict == "OJ_INFRASTRUCTURE_HOLD_REFERENCE_CONTROL_MISMATCH" and
                 receipt.get("verdict") != diagnostic_status) or
                (verdict == "OJ_INFRASTRUCTURE_HOLD_CANDIDATE_TIMING_UNSTABLE" and
                 diagnostic_status not in (None, verdict)) or
                receipt.get("candidate_failure_diagnostics") is not None or
                any(receipt.get(k) != request.get(k) for k in
                    ("request_id", "episode_id", "candidate_id", "candidate_sha256")) or
                fault.get("schema") != "N0_CANDIDATE_ABORT_RECEIPT_V1" or
                fault.get("request_id") != request.get("request_id") or
                fault.get("candidate_sha256") != request.get("candidate_sha256") or
                fault_detail.get("schema") != "N0_CUDA_FAULT_V1" or
                fault_detail.get("domain") != "SUPERVISOR" or
                fault_detail.get("attribution") != "UNRESOLVED" or
                fault_detail.get("stage") != "POST_EVALUATION_RECOVERY_GATE" or
                fault_detail.get("score_allowed") is not False or
                identity.get("request_id") != request.get("request_id") or
                identity.get("episode_id") != request.get("episode_id") or
                identity.get("candidate_sha256") != request.get("candidate_sha256") or
                job.get("returncode") != 0 or job.get("active_processes_after") != 0 or
                job.get("terminated_tree") is not True or job.get("timed_out") is not False):
            _fail("FAULT_RECEIPT_CLASSIFICATION_MISMATCH")
        return
    if verdict == "CANDIDATE_CORRECTNESS_FAILURE":
        # Historical receipts may omit this optional diagnostic field.  The
        # authoritative verdict/content hash and request identity are checked
        # independently; an omitted field is never synthesized here. A supplied
        # diagnostic must explicitly agree with the correctness verdict.
        optional_diagnostic = receipt.get("candidate_failure_diagnostics")
        diagnostic_ok = (receipt.get("schema") == "N0_OJ_R5_IMMUTABLE_RECEIPT_V1" and
                         (optional_diagnostic is None or
                          (isinstance(optional_diagnostic, dict) and
                           optional_diagnostic.get("classification") == verdict)))
        worker_identity = fault.get("worker_identity") or {}
        identity_ok = (gate.get("fault_request_id") == request.get("request_id") and
                       (not gate.get("fault_candidate_sha256") or
                        gate.get("fault_candidate_sha256") == request.get("candidate_sha256")) and
                       receipt.get("request_id") == request.get("request_id") and
                       receipt.get("episode_id") == request.get("episode_id") and
                       receipt.get("candidate_id") == request.get("candidate_id") and
                       receipt.get("candidate_sha256") == request.get("candidate_sha256") and
                       fault.get("request_id") == request.get("request_id") and
                       fault.get("candidate_sha256") == request.get("candidate_sha256") and
                       worker_identity.get("request_id") == request.get("request_id") and
                       worker_identity.get("candidate_sha256") == request.get("candidate_sha256") and
                       worker_identity.get("episode_id") == request.get("episode_id"))
        if (gate.get("reason") != "CANDIDATE_PROCESS_TERMINATION_REQUIRES_DEVICE_REQUALIFICATION" or
                classification != verdict or
                (gate.get("fault_classification") or {}).get("classification") != verdict or
                not diagnostic_ok or not identity_ok or receipt.get("score") is not None):
            _fail("FAULT_RECEIPT_CLASSIFICATION_MISMATCH")
        return
    if verdict == "CANDIDATE_CUDA_RUNTIME_FAILURE":
        worker_identity = fault.get("worker_identity") or {}
        if (gate.get("reason") != "CANDIDATE_PROCESS_TERMINATION_REQUIRES_DEVICE_REQUALIFICATION" or
                not str(classification or "").startswith("CANDIDATE_EXECUTION_") or
                gate.get("fault_classification", {}).get("classification") != classification or
                diagnostic != fault_detail or fault_detail.get("schema") != "N0_CUDA_FAULT_V1" or
                fault_detail.get("attribution") != "CANDIDATE" or
                fault_detail.get("classifier_version") != "N0_CUDA_FAULT_CLASSIFIER_V1" or
                fault_detail.get("stage") != "EVALUATION_CHILD" or
                fault_detail.get("score_allowed") is not False or
                worker_identity.get("request_id") != request.get("request_id") or
                worker_identity.get("candidate_sha256") != request.get("candidate_sha256") or
                worker_identity.get("episode_id") != request.get("episode_id")):
            _fail("FAULT_RECEIPT_CLASSIFICATION_MISMATCH")
        return
    if verdict == "INVALID_SUBMISSION":
        # A rejected submission can exit normally without launching a CUDA
        # kernel. Preserve the fresh-context device requalification gate, but
        # accept only the exact, scoreless supervisor receipt and identity
        # chain produced by the frozen R6.5 evaluator.
        worker_identity = fault.get("worker_identity") or {}
        candidate_failure = receipt.get("candidate_failure_diagnostics") or {}
        diagnostic = (receipt.get("diagnostics") or {}).get("candidate_failure") or {}
        if (gate.get("reason") != "CANDIDATE_PROCESS_TERMINATION_REQUIRES_DEVICE_REQUALIFICATION" or
                classification != verdict or
                gate.get("fault_classification", {}).get("classification") != verdict or
                fault_detail.get("schema") != "N0_CUDA_FAULT_V1" or
                fault_detail.get("domain") != "SUPERVISOR" or
                fault_detail.get("attribution") != "UNRESOLVED" or
                fault_detail.get("stage") != "POST_EVALUATION_RECOVERY_GATE" or
                fault_detail.get("score_allowed") is not False or
                not candidate_failure or candidate_failure.get("status") != verdict or
                candidate_failure != diagnostic or
                worker_identity.get("request_id") != request.get("request_id") or
                worker_identity.get("candidate_sha256") != request.get("candidate_sha256") or
                worker_identity.get("episode_id") != request.get("episode_id") or
                fault.get("request_id") != request.get("request_id") or
                fault.get("candidate_sha256") != request.get("candidate_sha256")):
            _fail("FAULT_RECEIPT_CLASSIFICATION_MISMATCH")
        return
    _fail("FAULT_RECEIPT_VERDICT_UNSUPPORTED_FOR_DEVICE_REQUALIFICATION")


def _event_child_for_request(events_path: Path, request: dict[str, Any], fault: dict[str, Any]) -> dict[str, Any]:
    if not events_path.is_file():
        _fail("SUPERVISOR_EXIT_EVIDENCE_MISSING")
    try:
        events = [json.loads(line) for line in events_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    except (OSError, json.JSONDecodeError):
        _fail("SUPERVISOR_EVENT_LOG_INVALID")
    candidates = [e.get("child") or {} for e in events if e.get("event") == "SERVING_WORKER_EXITED"]
    candidates = [c for c in candidates if c.get("pid") and c.get("returncode") is not None]
    if not candidates:
        _fail("SERVING_WORKER_EXIT_EVIDENCE_MISSING")
    child = candidates[-1]
    argv = child.get("command") or []
    if not any(str(arg).casefold().endswith("run_r5_oj_worker.py") for arg in argv):
        _fail("SERVING_WORKER_EXIT_COMMAND_UNVERIFIED")
    if child.get("descendants_after_exit") != []:
        _fail("SERVING_WORKER_DESCENDANTS_NOT_PROVEN_EMPTY")
    if child.get("returncode") not in {0, 20, 75}:
        _fail("SERVING_WORKER_EXIT_CODE_UNVERIFIED")
    try:
        row_pid = int(request.get("worker_pid") or 0)
        event_pid = int(child.get("pid") or 0)
    except (TypeError, ValueError):
        _fail("FAULT_WORKER_PID_INVALID")
    if not row_pid or not event_pid:
        _fail("FAULT_WORKER_PID_LINEAGE_MISMATCH")
    if row_pid != event_pid:
        identity = child.get("worker_identity_evidence") or {}
        launcher = identity.get("launcher_process") or {}
        worker = identity.get("worker_process") or {}
        try:
            launcher_pid = int(identity.get("launcher_pid") or 0)
            worker_pid = int(identity.get("worker_pid") or 0)
            supervisor_pid = int(identity.get("supervisor_pid") or 0)
            launcher_parent = int(launcher.get("ParentProcessId") or 0)
            worker_parent = int(worker.get("ParentProcessId") or 0)
        except (TypeError, ValueError):
            _fail("FAULT_WORKER_PID_LINEAGE_MISMATCH")
        if not (identity.get("captured_while_alive") is True and launcher_pid == event_pid and
                worker_pid == row_pid and int(launcher.get("ProcessId") or 0) == event_pid and
                int(worker.get("ProcessId") or 0) == row_pid and worker_parent == event_pid and
                supervisor_pid > 0 and launcher_parent == supervisor_pid and
                int((fault.get("worker_identity") or {}).get("parent_pid") or 0) == event_pid):
            _fail("FAULT_WORKER_PID_LINEAGE_MISMATCH")
    worker_identity = fault.get("worker_identity") or {}
    if not worker_identity:
        _fail("FAULT_WORKER_IDENTITY_MISSING")
    try:
        identity_pid = int(worker_identity.get("pid") or 0)
    except (TypeError, ValueError):
        _fail("FAULT_WORKER_PID_INVALID")
    if identity_pid != row_pid:
        _fail("FAULT_WORKER_PID_LINEAGE_MISMATCH")
    if worker_identity.get("request_id") != request.get("request_id"):
        _fail("FAULT_WORKER_REQUEST_ID_MISMATCH")
    if worker_identity.get("candidate_sha256") != request.get("candidate_sha256"):
        _fail("FAULT_WORKER_CANDIDATE_SHA_MISMATCH")
    return child


def validate_existing_hold(queue_root: Path, gate_path: Path, lease_path: Path,
                           events_path: Path, process_rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not gate_path.is_file():
        _fail("TIMING_GATE_MISSING")
    gate_raw = gate_path.read_bytes()
    try:
        gate = json.loads(gate_raw)
    except json.JSONDecodeError:
        _fail("TIMING_GATE_INVALID_JSON")
    if gate.get("schema") != "N0_GPU_FAULT_RECOVERY_GATE_V1" or gate.get("status") != "HOLD":
        _fail("EXISTING_FAULT_HOLD_REQUIRED")
    request_id = str(gate.get("fault_request_id") or "")
    if not request_id or Path(request_id).name != request_id:
        _fail("FAULT_REQUEST_ID_INVALID")
    db_path = queue_root / "n0_runtime.sqlite3"
    con = sqlite3.connect(db_path.resolve().as_uri() + "?mode=ro", uri=True, timeout=10)
    con.row_factory = sqlite3.Row
    try:
        row_value = con.execute("SELECT * FROM oj_requests WHERE request_id=?", (request_id,)).fetchone()
        running = int(con.execute("SELECT COUNT(*) FROM oj_requests WHERE state='RUNNING'").fetchone()[0])
    except sqlite3.Error as exc:
        _fail("FAULT_REQUEST_READ_FAILED:" + type(exc).__name__)
    finally:
        con.close()
    if row_value is None:
        _fail("FAULT_REQUEST_NOT_FOUND")
    request = dict(row_value)
    if running:
        _fail("OJ_QUEUE_HAS_RUNNING_REQUESTS")
    receipt_info = _receipt_checks(request)
    if gate.get("fault_candidate_sha256") and gate.get("fault_candidate_sha256") != request.get("candidate_sha256"):
        _fail("GATE_CANDIDATE_SHA_MISMATCH")
    fault_path = (queue_root / "candidate_supervision" / request_id / "fault_receipt.json").resolve()
    expected_path = (queue_root.resolve() / "candidate_supervision" / request_id / "fault_receipt.json").resolve()
    configured_path = Path(str(gate.get("fault_receipt_path") or "")).resolve()
    if configured_path != expected_path or not fault_path.is_file():
        _fail("FAULT_RECEIPT_PATH_OR_FILE_INVALID")
    fault_raw = fault_path.read_bytes()
    fault_sha = sha_bytes(fault_raw)
    if gate.get("fault_receipt_sha256") and str(gate["fault_receipt_sha256"]).upper() != fault_sha:
        _fail("GATE_FAULT_RECEIPT_SHA_MISMATCH")
    try:
        fault = json.loads(fault_raw)
    except json.JSONDecodeError:
        _fail("FAULT_RECEIPT_INVALID_JSON")
    if fault.get("request_id") != request_id or fault.get("candidate_sha256") != request.get("candidate_sha256"):
        _fail("FAULT_RECEIPT_REQUEST_OR_CANDIDATE_MISMATCH")
    if fault.get("fault", {}).get("classification") != gate.get("fault_classification", {}).get("classification"):
        _fail("FAULT_RECEIPT_CLASSIFICATION_MISMATCH")
    _validate_fault_receipt_classification(gate, request, receipt_info["receipt"], fault)
    if fault.get("fault", {}).get("score_allowed") is not False:
        _fail("FAULT_RECEIPT_SCORE_POLICY_MISSING")
    job = fault.get("job_result") or {}
    normally_exited_submission = (
        request.get("state") in {"COMPLETED", "CONSUMED"} and
        receipt_info["receipt"].get("verdict") == "INVALID_SUBMISSION" and
        (receipt_info["receipt"].get("candidate_failure_diagnostics") or {}).get("status") == "INVALID_SUBMISSION" and
        job.get("returncode") == 0 and job.get("active_processes_after") == 0 and
        job.get("terminated_tree") is False
    )
    normally_exited_infrastructure = (
        request.get("state") in {"COMPLETED", "CONSUMED"} and
        receipt_info["receipt"].get("verdict") in {
            "OJ_INFRASTRUCTURE_HOLD_REFERENCE_CONTROL_MISMATCH",
            "OJ_INFRASTRUCTURE_HOLD_CANDIDATE_TIMING_UNSTABLE",
        } and
        (gate.get("fault_classification") or {}).get("classification") == receipt_info["receipt"].get("verdict") and
        (fault.get("fault") or {}).get("classification") == receipt_info["receipt"].get("verdict") and
        job.get("returncode") == 0 and
        job.get("active_processes_after") == 0 and
        job.get("terminated_tree") is False and
        job.get("timed_out") is False
    )
    if (job.get("active_processes_after") != 0 or job.get("returncode") != 0 or
            (job.get("terminated_tree") is not True and
             not normally_exited_submission and not normally_exited_infrastructure)):
        _fail("CANDIDATE_EVALUATION_PROCESS_EXIT_UNPROVEN")
    if not job.get("process_id"):
        _fail("CANDIDATE_EVALUATION_PID_MISSING")
    child = _event_child_for_request(events_path, request, fault)
    lease_evidence = fault.get("gpu_lease_evidence") or {}
    if not lease_evidence:
        _fail("GPU_LEASE_OWNERSHIP_EVIDENCE_MISSING")
    expected_lease = Path(lease_path).resolve()
    try:
        lease_pid = int((lease_evidence.get("owner") or {}).get("pid") or 0)
    except (TypeError, ValueError):
        _fail("GPU_LEASE_OWNER_PID_INVALID")
    owner = lease_evidence.get("owner") or {}
    if (Path(str(lease_evidence.get("path") or "")).resolve() != expected_lease or
            lease_evidence.get("captured_while_held") is not True or
            lease_pid != int(request.get("worker_pid") or 0) or
            owner.get("request_id") != request_id or
            owner.get("candidate_sha256") != request.get("candidate_sha256") or
            owner.get("episode_id") != request.get("episode_id") or
            lease_evidence.get("logical_request_lease_id") != request.get("lease_id") or
            not owner.get("lease_id")):
        _fail("GPU_LEASE_OWNERSHIP_EVIDENCE_MISMATCH")
    live_ids = {int(r.get("ProcessId") or r.get("pid") or 0) for r in process_rows}
    for pid in (int(request.get("worker_pid") or 0), int(job["process_id"]), int(child["pid"])):
        if pid and pid in live_ids:
            _fail("RECOVERY_RELEVANT_PROCESS_STILL_LIVE")
    snapshot = queue_snapshot(queue_root)
    if snapshot["running_count"] != 0:
        _fail("OJ_QUEUE_HAS_RUNNING_REQUESTS")
    return {"schema": "N0_EXISTING_HOLD_CONTEXT_V1", "fault_request_id": request_id,
            "candidate_sha256": request["candidate_sha256"], "episode_id": request["episode_id"],
            "candidate_id": request["candidate_id"], "oj_receipt_sha256": receipt_info["receipt_sha256"],
            "baseline_id": receipt_info.get("baseline_id"),
            "fault_receipt_path": str(fault_path), "fault_receipt_sha256": fault_sha,
            "fault_gate_sha256": sha_bytes(gate_raw), "fault_gate": gate,
            "serving_worker_pid": int(child["pid"]), "candidate_evaluation_pid": int(job["process_id"]),
            "worker_identity": fault["worker_identity"], "gpu_lease_evidence": lease_evidence,
            "gpu_lease_path": str(expected_lease), "queue_snapshot": snapshot,
            "original_supervisor_exit": child, "validated_at_utc": utc_now().isoformat()}


def validate_recovery_receipt(receipt_path: Path, context: dict[str, Any],
                              worker_status: dict[str, Any], expected_file_sha: str,
                              expected_worker_state: str = "RECOVERY_QUALIFIED",
                              max_age_seconds: int = MAX_QUALIFICATION_AGE_SECONDS) -> dict[str, Any]:
    if not receipt_path.is_file():
        _fail("RECOVERY_RECEIPT_MISSING")
    raw = receipt_path.read_bytes()
    try:
        receipt = json.loads(raw)
    except json.JSONDecodeError:
        _fail("RECOVERY_RECEIPT_INVALID_JSON")
    recovery_id = str(receipt.get("recovery_id") or "")
    if not recovery_id or receipt_path.name != recovery_id + ".json":
        _fail("RECOVERY_RECEIPT_IDENTITY_INVALID")
    inner = str(receipt.get("recovery_receipt_sha256") or "").upper()
    body = {k: v for k, v in receipt.items() if k not in {"recovery_receipt_sha256", "recovery_receipt_file_sha256"}}
    if not inner or sha_bytes(canonical_bytes(body)) != inner:
        _fail("RECOVERY_RECEIPT_CONTENT_SHA_MISMATCH")
    file_sha = sha_bytes(raw)
    if str(expected_file_sha or "").upper() != file_sha:
        _fail("RECOVERY_RECEIPT_FILE_SHA_MISMATCH")
    if receipt.get("status") != "PASS":
        _fail("RECOVERY_RECEIPT_NOT_PASS")
    if (receipt.get("fault_request_id") != context.get("fault_request_id") or
            receipt.get("fault_candidate_sha256") != context.get("candidate_sha256") or
            receipt.get("fault_receipt_sha256") != context.get("fault_receipt_sha256") or
            receipt.get("oj_receipt_sha256") != context.get("oj_receipt_sha256") or
            receipt.get("fault_gate_sha256") != context.get("fault_gate_sha256")):
        _fail("RECOVERY_RECEIPT_FAULT_IDENTITY_MISMATCH")
    if receipt.get("required_consecutive_controls") != REQUIRED_CONTROLS:
        _fail("RECOVERY_CONTROL_COUNT_POLICY_MISMATCH")
    controls = receipt.get("controls") or []
    if len(controls) < REQUIRED_CONTROLS or len(controls) > MAX_CONTROLS:
        _fail("RECOVERY_CONTROL_SAMPLE_COUNT_INVALID")
    streak = 0
    for index, control in enumerate(controls):
        trust = control.get("trust") or {}
        health = control.get("health") or {}
        deviation = trust.get("relative_deviation")
        passed = (trust.get("pass") is True and isinstance(deviation, (int, float)) and
                  abs(float(deviation)) <= MAX_REFERENCE_DEVIATION and health.get("status") == "PASS")
        streak = streak + 1 if passed else 0
        if int(control.get("consecutive_passes", -1)) != streak:
            _fail("RECOVERY_CONSECUTIVE_CONTROL_SEQUENCE_INVALID")
        if passed and context.get("baseline_id") and trust.get("baseline_id") != context["baseline_id"]:
            _fail("RECOVERY_BASELINE_IDENTITY_MISMATCH")
    if streak < REQUIRED_CONTROLS:
        _fail("RECOVERY_CONTROLS_NOT_CONSECUTIVELY_PASSING")
    lease = receipt.get("recovery_lease") or {}
    process = (worker_status.get("detail") or {}).get("process_identity") or {}
    if (lease.get("released") is not True or not lease.get("lease_id") or
            int(lease.get("pid") or 0) != int(process.get("pid") or -1) or
            Path(str(lease.get("path") or "")).resolve() != Path(context["gpu_lease_path"]).resolve()):
        _fail("RECOVERY_GPU_LEASE_RELEASE_EVIDENCE_INVALID")
    if worker_status.get("state") != expected_worker_state:
        _fail("RECOVERY_CHILD_STATUS_NOT_QUALIFIED")
    completed = receipt.get("completed_at_utc")
    if not completed:
        _fail("RECOVERY_RECEIPT_TIMESTAMP_MISSING")
    age = (utc_now() - parse_utc(completed)).total_seconds()
    if age < -5 or age > max_age_seconds:
        _fail("RECOVERY_RECEIPT_EXPIRED_OR_FUTURE_DATED")
    return {"receipt": receipt, "recovery_receipt_sha256": inner,
            "recovery_receipt_file_sha256": file_sha, "consecutive_passes": streak,
            "age_seconds": age}


def validate_rescue_authorization(authorization_path: Path, qualification: dict[str, Any],
                                  current_snapshot: dict[str, Any], max_age_seconds: int = MAX_QUALIFICATION_AGE_SECONDS) -> dict[str, Any]:
    if not authorization_path.is_file():
        _fail("START_AUTHORIZATION_MISSING")
    try:
        auth = json.loads(authorization_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        _fail("START_AUTHORIZATION_INVALID_JSON")
    if auth.get("schema") != "N0_POOL20_RECOVERY_START_AUTHORIZATION_V1" or auth.get("explicit_authorization") is not True:
        _fail("START_AUTHORIZATION_SCHEMA_OR_EXPLICIT_FLAG_INVALID")
    if not auth.get("authorized_by") or not auth.get("authorization_id"):
        _fail("START_AUTHORIZATION_ACTOR_OR_ID_MISSING")
    qualification_path = Path(str(auth.get("qualification_receipt_path") or "")).resolve()
    if not qualification_path.is_file():
        _fail("START_AUTHORIZATION_QUALIFICATION_RECEIPT_MISSING")
    if sha_file(qualification_path) != auth.get("qualification_receipt_file_sha256"):
        _fail("START_AUTHORIZATION_QUALIFICATION_RECEIPT_SHA_MISMATCH")
    if auth.get("qualification_receipt_file_sha256") != qualification.get("_verified_file_sha256"):
        _fail("START_AUTHORIZATION_QUALIFICATION_IDENTITY_MISMATCH")
    rescue_path = Path(str(auth.get("rescue_validateonly_receipt_path") or "")).resolve()
    if not rescue_path.is_file():
        _fail("RESCUE_VALIDATEONLY_RECEIPT_MISSING")
    if sha_file(rescue_path) != str(auth.get("rescue_validateonly_receipt_sha256") or "").upper():
        _fail("RESCUE_VALIDATEONLY_RECEIPT_SHA_MISMATCH")
    try:
        rescue = json.loads(rescue_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        _fail("RESCUE_VALIDATEONLY_RECEIPT_INVALID_JSON")
    if rescue.get("status") != "PASS" or rescue.get("cross_store_status") != "PASS":
        _fail("RESCUE_VALIDATEONLY_NOT_PASS")
    if (rescue.get("run_id") != auth.get("run_id") or auth.get("run_id") != qualification.get("run_id") or
            rescue.get("queue_snapshot_sha256") != current_snapshot.get("sha256") or
            rescue.get("queue_snapshot_sha256") != qualification.get("queue_snapshot_sha256")):
        _fail("RESCUE_VALIDATEONLY_CROSS_STORE_IDENTITY_MISMATCH")
    if auth.get("rescue_validateonly_status") != "PASS" or auth.get("cross_store_status") != "PASS":
        _fail("RESCUE_VALIDATEONLY_NOT_PASS")
    if auth.get("queue_snapshot_sha256") != current_snapshot.get("sha256"):
        _fail("START_AUTHORIZATION_QUEUE_SNAPSHOT_STALE")
    episode_ids = auth.get("episode_ids") or []
    if auth.get("episode_count") != 20 or len(episode_ids) != 20 or len(set(episode_ids)) != 20:
        _fail("START_AUTHORIZATION_EPISODE_SET_INVALID")
    run_episodes = set(episode_ids)
    episode_rows = current_snapshot.get("payload", {}).get("episodes", [])
    selected = [r for r in episode_rows if r.get("episode_id") in run_episodes]
    # Bind to this Run's current episode rows.  The previous constant (37)
    # described an earlier snapshot and rejected R8_03's valid total (40).
    expected_budget = sum(int(r.get("candidates_used") or 0) for r in selected)
    if (auth.get("candidate_budget_used") != expected_budget or
            rescue.get("candidate_budget_used") != expected_budget):
        _fail("START_AUTHORIZATION_BUDGET_SNAPSHOT_INVALID")
    if len(selected) != 20 or sum(int(r.get("candidates_used") or 0) for r in selected) != expected_budget:
        _fail("START_AUTHORIZATION_EPISODE_STATE_MISMATCH")
    if not rescue.get("cross_store_state_sha256") or not rescue.get("worker_registry_sha256"):
        _fail("RESCUE_VALIDATEONLY_CROSS_STORE_EVIDENCE_MISSING")
    unresolved = auth.get("unresolved_request_ids_in_run") or []
    if unresolved:
        _fail("START_AUTHORIZATION_RUN_HAS_UNRESOLVED_REQUESTS")
    issued = auth.get("issued_at_utc")
    if not issued:
        _fail("START_AUTHORIZATION_TIMESTAMP_MISSING")
    age = (utc_now() - parse_utc(issued)).total_seconds()
    if age < -5 or age > max_age_seconds:
        _fail("START_AUTHORIZATION_EXPIRED_OR_FUTURE_DATED")
    return auth
