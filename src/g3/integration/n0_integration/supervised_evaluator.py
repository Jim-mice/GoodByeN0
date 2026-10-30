"""External wall-clock/Job Object boundary for one untrusted Candidate request."""
from __future__ import annotations
import hashlib, json, os, uuid
from pathlib import Path
import subprocess, tempfile
from typing import Any
from datetime import datetime, timezone
import sys

from .cuda_faults import WindowsJobProcess, classify_candidate_runtime_message, classify_fault

EVAL_TIMEOUT_SECONDS = 900
OUTPUT_LIMIT_BYTES = 16 * 1024 * 1024

_COMPILER_ENV_ALLOWLIST = {"PATH", "PATHEXT", "SYSTEMROOT", "WINDIR", "COMSPEC", "CUDA_PATH",
    "CUDA_HOME", "VCToolsInstallDir", "VCINSTALLDIR", "VSINSTALLDIR", "VisualStudioVersion",
    "VSCMD_ARG_TGT_ARCH", "VSCMD_VER", "INCLUDE", "LIB", "LIBPATH", "WindowsSdkDir",
    "WindowsSDKVersion", "UniversalCRTSdkDir", "UCRTVersion", "DISTUTILS_USE_SDK", "PYTHONUTF8"}


def _qualified_compiler_environment(oj_root: Path) -> dict[str, str]:
    contract = json.loads((Path(oj_root) / "CUDA_COMPILER_CONTRACT.json").read_text(encoding="utf-8"))
    host = Path(contract["host_compiler"]["path"]).resolve()
    vc_root = host.parents[6]
    vcvarsall = vc_root / "Auxiliary" / "Build" / "vcvarsall.bat"
    if not host.is_file() or not vcvarsall.is_file():
        raise RuntimeError("FROZEN_HOST_COMPILER_ENVIRONMENT_UNAVAILABLE")
    with tempfile.TemporaryDirectory(prefix="n0-vc-env-") as temporary:
        batch = Path(temporary) / "capture_env.cmd"
        batch.write_text(f'@echo off\r\ncall "{vcvarsall}" x64\r\nif errorlevel 1 exit /b %errorlevel%\r\nset\r\n',
                         encoding="ascii")
        completed = subprocess.run(["cmd.exe", "/d", "/u", "/c", str(batch)],
            capture_output=True, timeout=30, check=True)
    text = completed.stdout.decode("utf-16le", "replace")
    result = {}
    for row in text.splitlines():
        if "=" not in row or row.startswith("="):
            continue
        key, value = row.split("=", 1)
        if key in _COMPILER_ENV_ALLOWLIST:
            result[key] = value
    if not result.get("VSCMD_ARG_TGT_ARCH") or not result.get("INCLUDE") or not result.get("LIB"):
        raise RuntimeError("FROZEN_HOST_COMPILER_ENVIRONMENT_INCOMPLETE")
    result["DISTUTILS_USE_SDK"] = "1"
    return result


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    temp.write_text(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    os.replace(temp, path)


def _execution_provenance(request: dict[str, Any], lease_path: Path) -> dict[str, Any]:
    """Capture the serving PID and the actual locked GPU lease owner while evaluation is inside it."""
    path = Path(lease_path).resolve()
    # GpuExclusiveLease locks byte 0 with msvcrt.locking and stores the JSON
    # owner record from byte 1 onward. Re-reading byte 0 through a second
    # Windows file handle raises PermissionError while the lease is held.
    # Read only the unlocked owner-record region.
    with path.open("rb", buffering=0) as stream:
        stream.seek(1)
        owner_bytes = stream.read()
    if not owner_bytes:
        raise RuntimeError("GPU_LEASE_OWNER_RECORD_MISSING_DURING_EVALUATION")
    owner_record = json.loads(owner_bytes.decode("utf-8"))
    owner = owner_record.get("lease") or {}
    expected = {"pid": os.getpid(), "request_id": request.get("request_id"),
                "episode_id": request.get("episode_id"), "candidate_sha256": request.get("candidate_sha256")}
    if any(owner.get(key) != value for key, value in expected.items()):
        raise RuntimeError("GPU_LEASE_OWNER_IDENTITY_MISMATCH_DURING_EVALUATION")
    return {"worker_identity": {"pid": os.getpid(), "parent_pid": os.getppid(),
                "request_id": request.get("request_id"), "episode_id": request.get("episode_id"),
                "candidate_sha256": request.get("candidate_sha256"),
                "logical_request_lease_id": request.get("lease_id"),
                "sys_executable": str(Path(sys.executable).resolve()),
                "command_line": " ".join(sys.argv)},
            "gpu_lease_evidence": {"path": str(path), "captured_while_held": True,
                "logical_request_lease_id": request.get("lease_id"), "owner": owner,
                "acquired_at_unix": owner_record.get("acquired_at_unix"),
                "health_evidence": owner_record.get("health_evidence")}}


def _begin_gpu_gate(request: dict[str, Any], root: Path) -> tuple[Path, dict[str, Any] | None]:
    gate = Path(os.environ.get("N0_GPU_TIMING_GATE_PATH", "D:/projects/kernelagent/runtime/state/n0_oj_profiles/timing_gate.json"))
    previous = json.loads(gate.read_text(encoding="utf-8")) if gate.is_file() else None
    if previous and previous.get("status") != "READY":
        raise RuntimeError("GPU_RECOVERY_GATE_NOT_READY_BEFORE_CHILD_START")
    _atomic_json(gate, {"schema":"N0_GPU_FAULT_RECOVERY_GATE_V1","status":"IN_PROGRESS",
        "reason":"CANDIDATE_EVALUATOR_CHILD_ACTIVE","fault_request_id":request["request_id"],
        "candidate_sha256":request.get("candidate_sha256"),"queue_root":str(root.parents[1]),
        "fault_receipt_path":str(root / "fault_receipt.json"),"child_pid":None,
        "supervisor_pid":os.getpid(),"started_at_utc":datetime.now(timezone.utc).isoformat()})
    return gate, previous


def _restore_gpu_gate(path: Path, previous: dict[str, Any] | None) -> None:
    if previous is None:
        path.unlink(missing_ok=True)
    else:
        _atomic_json(path, previous)


def make_supervised_evaluator(*, queue_root: Path, integration_root: Path,
                              harness_root: Path, oj_root: Path, repository_root: Path,
                              qualification: dict[str, Any], hardware_identity: dict[str, Any],
                              python: Path, lease_path: Path, health_check, monitor_factory=None) :
    queue_root = Path(queue_root).resolve()
    def evaluate(request: dict[str, Any]) -> dict[str, Any]:
        request_id = str(request["request_id"])
        root = queue_root / "candidate_supervision" / request_id
        root.mkdir(parents=True, exist_ok=True)
        config_path, result_path = root / "child_config.json", root / "child_result.json"
        stdout_path, stderr_path = root / "child.stdout.log", root / "child.stderr.log"
        for stale in (result_path, config_path, stdout_path, stderr_path):
            stale.unlink(missing_ok=True)
        from n0_integration.oj_r5_worker import query_device
        gpu_snapshot = query_device()
        monitor = monitor_factory() if monitor_factory else None
        if monitor:
            monitor.start()
        config = {"schema": "N0_CANDIDATE_EVAL_CHILD_V1", "request": request,
            "queue_root": str(queue_root), "integration_root": str(Path(integration_root).resolve()),
            "harness_root": str(Path(harness_root).resolve()), "oj_root": str(Path(oj_root).resolve()),
            "repository_root": str(Path(repository_root).resolve()), "qualification": qualification,
            "hardware_identity": hardware_identity, "lease_path": str(Path(lease_path).resolve()),
            "gpu_snapshot": gpu_snapshot}
        _atomic_json(config_path, config)
        command = [str(Path(python).resolve()), "-I", str(Path(integration_root).resolve() / "n0_integration" / "candidate_eval_child.py"),
                   str(config_path), str(result_path), "--gpu-lease-path", str(Path(lease_path).resolve())]
        child_env = {key: os.environ[key] for key in
            ("PATH", "PATHEXT", "SYSTEMROOT", "WINDIR", "COMSPEC", "CUDA_PATH", "CUDA_HOME") if key in os.environ}
        child_env.update(_qualified_compiler_environment(Path(oj_root)))
        child_env.update({"TEMP": str(root), "TMP": str(root),
            "N0_GPU_LEASE_PATH": str(Path(lease_path).resolve()), "PYTHONUTF8": "1",
            "N0_CANDIDATE_EVAL_CHILD": "1",
            "N0_SUPERVISOR_GPU_SNAPSHOT_JSON": json.dumps(gpu_snapshot, separators=(",", ":")),
            "PYTHONDONTWRITEBYTECODE": "1"})
        if os.environ.get("N0_DISABLE_DIAGNOSTIC_PROFILE") == "1":
            child_env["N0_DISABLE_DIAGNOSTIC_PROFILE"] = "1"
        gate_path, prior_gate = _begin_gpu_gate(request, root)
        def child_started(pid: int) -> None:
            current_gate = json.loads(gate_path.read_text(encoding="utf-8"))
            if current_gate.get("fault_request_id") != request_id or current_gate.get("status") != "IN_PROGRESS":
                raise RuntimeError("CANDIDATE_START_GATE_IDENTITY_CHANGED")
            current_gate["child_pid"] = pid
            current_gate["child_started_unix"] = __import__("time").time()
            _atomic_json(gate_path, current_gate)
        try:
            process_result = WindowsJobProcess(timeout_seconds=EVAL_TIMEOUT_SECONDS,
                output_limit_bytes=OUTPUT_LIMIT_BYTES, process_limit=24,
                memory_limit_bytes=8 * 1024**3).run(command, cwd=str(root), env=child_env,
                    stdout_path=str(stdout_path), stderr_path=str(stderr_path), on_started=child_started)
        except BaseException as exc:
            monitor_events = monitor.stop() if monitor else []
            monitor_evidence = monitor.evidence() if monitor else {}
            fault = classify_fault("SUPERVISOR", type(exc).__name__, "EVALUATION_SUPERVISOR",
                                   f"{type(exc).__name__}:{exc}")
            _atomic_json(root / "fault_receipt.json", {**_execution_provenance(request, lease_path), "schema": "N0_CANDIDATE_ABORT_RECEIPT_V1",
                "request_id": request_id, "candidate_sha256": request.get("candidate_sha256"),
                "fault": fault, "gpu_recovery_required": True,"gpu_monitor_events":monitor_events,
                "gpu_monitor_evidence":monitor_evidence})
            _hold_gpu_gate(root, request_id, fault)
            return _infrastructure_receipt(request, fault)
        monitor_events = monitor.stop() if monitor else []
        monitor_evidence = monitor.evidence() if monitor else {}
        if process_result.active_processes_after != 0:
            fault = classify_fault("SUPERVISOR", "CHILD_TREE_NOT_PROVEN_EMPTY", "EVALUATION_CHILD_EXIT",
                f"active_processes_after={process_result.active_processes_after}")
            _atomic_json(root / "fault_receipt.json", {**_execution_provenance(request, lease_path), "schema": "N0_CANDIDATE_ABORT_RECEIPT_V1",
                "request_id": request_id, "candidate_sha256": request.get("candidate_sha256"),
                "fault": fault, "gpu_recovery_required": True, "job_result": process_result.__dict__,
                "gpu_monitor_events":monitor_events,"gpu_monitor_evidence":monitor_evidence})
            _hold_gpu_gate(root, request_id, fault)
            return _infrastructure_receipt(request, fault)
        if process_result.timed_out or process_result.returncode != 0 or not result_path.is_file():
            domain = "SUPERVISOR" if process_result.timed_out else "WINDOWS_PROCESS_EXIT"
            value = "CANDIDATE_EVALUATION_DEADLINE" if process_result.timed_out else process_result.returncode
            fault = classify_fault(domain, value, "EVALUATION_CHILD",
                (process_result.stderr + "\n" + process_result.stdout)[:65536])
            _atomic_json(root / "fault_receipt.json", {**_execution_provenance(request, lease_path), "schema": "N0_CANDIDATE_ABORT_RECEIPT_V1",
                "request_id": request_id, "candidate_sha256": request.get("candidate_sha256"),
                "fault": fault, "gpu_recovery_required": True, "job_result": process_result.__dict__,
                "gpu_monitor_events":monitor_events,"gpu_monitor_evidence":monitor_evidence})
            _hold_gpu_gate(root, request_id, fault)
            return _infrastructure_receipt(request, fault)
        child = json.loads(result_path.read_text(encoding="utf-8"))
        if child.get("status") != "CHILD_COMPLETED" or not isinstance(child.get("result"), dict):
            exception_text = "\n".join(str(child.get(k,"")) for k in
                ("exception_message","exception_stdout","exception_stderr"))
            candidate_fault = classify_candidate_runtime_message(exception_text,
                "EVALUATION_CHILD", require_cuda_marker=True)
            fault = candidate_fault or classify_fault("PYTHON_EXCEPTION",
                child.get("exception_type", "CHILD_RESULT_INVALID"), "EVALUATION_CHILD", exception_text)
            _atomic_json(root / "fault_receipt.json", {**_execution_provenance(request, lease_path), "schema": "N0_CANDIDATE_ABORT_RECEIPT_V1",
                "request_id": request_id, "candidate_sha256": request.get("candidate_sha256"),
                "fault": fault, "gpu_recovery_required": True,
                "job_result": process_result.__dict__,"gpu_monitor_events":monitor_events,
                "gpu_monitor_evidence":monitor_evidence})
            _hold_gpu_gate(root, request_id, fault)
            if candidate_fault:
                result = _infrastructure_receipt(request, fault)
                result["verdict"] = "CANDIDATE_CUDA_RUNTIME_FAILURE"
                result["candidate_failure_diagnostics"] = candidate_fault
                result["diagnostics"] = {"fault": candidate_fault,
                    "candidate_failure": candidate_fault, "stage": "EVALUATION_CHILD"}
                result["supervisor_execution_evidence"] = {
                    "child_pid": process_result.process_id,
                    "child_started_unix": process_result.started_unix,
                    "child_finished_unix": process_result.finished_unix,
                    "active_processes_after": process_result.active_processes_after,
                    "timed_out": process_result.timed_out,
                    "terminated_tree": process_result.terminated_tree,
                    "gpu_lease_path": str(Path(lease_path).resolve()),
                    "job_memory_limit_bytes": 8*1024**3, "active_process_limit": 24}
                result["supervisor_gpu_monitor_evidence"] = monitor_evidence
                from n0_autonomous.engine import canonical_bytes
                result["receipt_sha256"] = hashlib.sha256(canonical_bytes(result)).hexdigest()
                return result
            return _infrastructure_receipt(request, fault)
        result = child["result"]
        foreign = monitor_events or (monitor_evidence or {}).get("events") or []
        if foreign:
            fault = classify_fault("SUPERVISOR", "UNCOORDINATED_GPU_ACTIVITY", "EVALUATION_MONITOR",
                                   json.dumps(foreign, ensure_ascii=False)[:65536])
            result["verdict"] = "OJ_INFRASTRUCTURE_HOLD_EXTERNAL_GPU_INTERFERENCE"
            result["score"] = None
            result["supervisor_gpu_monitor_evidence"] = monitor_evidence
            result.setdefault("diagnostics", {})["fault"] = fault
            _atomic_json(root / "fault_receipt.json", {**_execution_provenance(request, lease_path), "schema":"N0_CANDIDATE_ABORT_RECEIPT_V1",
                "request_id":request_id,"candidate_sha256":request.get("candidate_sha256"),
                "fault":fault,"job_result":process_result.__dict__,"gpu_monitor_events":monitor_events,
                "gpu_monitor_evidence":monitor_evidence})
            _hold_gpu_gate(root, request_id, fault)
        else:
            result["supervisor_gpu_monitor_evidence"] = monitor_evidence
        result["supervisor_execution_evidence"] = {"child_pid":process_result.process_id,
            "child_started_unix":process_result.started_unix,"child_finished_unix":process_result.finished_unix,
            "active_processes_after":process_result.active_processes_after,"timed_out":process_result.timed_out,
            "terminated_tree":process_result.terminated_tree,"gpu_lease_path":str(Path(lease_path).resolve()),
            "job_memory_limit_bytes":8*1024**3,"active_process_limit":24}
        if (not foreign and process_result.returncode == 0 and
                result.get("verdict") in {"VALID_MEASUREMENT", "CANDIDATE_COMPILE_FAILURE", "CANDIDATE_SYNTAX_FAILURE"}):
            _restore_gpu_gate(gate_path, prior_gate)
        else:
            fault = ((result.get("diagnostics") or {}).get("fault") or
                (result.get("candidate_failure_diagnostics") if result.get("verdict")=="CANDIDATE_CUDA_RUNTIME_FAILURE" else None) or
                classify_fault("SUPERVISOR", str(result.get("verdict") or "CHILD_RESULT_NOT_TRUSTED"),
                    "POST_EVALUATION_RECOVERY_GATE", "fresh-context Reference recovery required"))
            _atomic_json(root / "fault_receipt.json", {**_execution_provenance(request, lease_path), "schema":"N0_CANDIDATE_ABORT_RECEIPT_V1",
                "request_id":request_id,"candidate_sha256":request.get("candidate_sha256"),
                "fault":fault,"job_result":process_result.__dict__,
                "gpu_monitor_events":monitor_events,"gpu_monitor_evidence":monitor_evidence})
            _hold_gpu_gate(root, request_id, fault)
        from n0_autonomous.engine import canonical_bytes
        result.pop("receipt_sha256", None)
        result["receipt_sha256"] = hashlib.sha256(canonical_bytes(result)).hexdigest()
        return result
    return evaluate


def _hold_gpu_gate(root: Path, request_id: str, fault: dict[str, Any]) -> None:
    gate = Path(os.environ.get("N0_GPU_TIMING_GATE_PATH", "D:/projects/kernelagent/runtime/state/n0_oj_profiles/timing_gate.json"))
    fault_path = root / "fault_receipt.json"
    fault_doc = json.loads(fault_path.read_text(encoding="utf-8")) if fault_path.is_file() else {}
    _atomic_json(gate, {"schema": "N0_GPU_FAULT_RECOVERY_GATE_V1", "status": "HOLD",
        "reason": "CANDIDATE_PROCESS_TERMINATION_REQUIRES_DEVICE_REQUALIFICATION",
        "fault_request_id": request_id, "fault_classification": fault,
        "fault_candidate_sha256": fault_doc.get("candidate_sha256"),
        "fault_receipt_path": str(fault_path.resolve()),
        "fault_receipt_sha256": hashlib.sha256(fault_path.read_bytes()).hexdigest().upper() if fault_path.is_file() else None,
        "worker_pid": (fault_doc.get("worker_identity") or {}).get("pid"),
        "gpu_lease_id": ((fault_doc.get("gpu_lease_evidence") or {}).get("owner") or {}).get("lease_id")})


def _infrastructure_receipt(request: dict[str, Any], fault: dict[str, Any]) -> dict[str, Any]:
    receipt = {"schema": "N0_OJ_R5_IMMUTABLE_RECEIPT_V1", "request_id": request["request_id"],
        "episode_id": request["episode_id"], "candidate_id": request["candidate_id"],
        "candidate_sha256": request["candidate_sha256"], "terminal": True,
        "verdict": "OJ_INFRASTRUCTURE_HOLD_CANDIDATE_PROCESS", "score": None,
        "timing": {"candidate_runtime_ms": None}, "measurement_evidence": None,
        "candidate_failure_diagnostics": None, "diagnostics": {"fault": fault},
        "gpu_recovery_required": True}
    from n0_autonomous.engine import canonical_bytes
    receipt["receipt_sha256"] = hashlib.sha256(canonical_bytes(receipt)).hexdigest()
    return receipt
