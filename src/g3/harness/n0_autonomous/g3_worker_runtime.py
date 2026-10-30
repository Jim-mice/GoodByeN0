"""Isolated G3 Episode controller over the authoritative N0PersistentQueue.

The diagnostic mode is explicitly non-scientific. It uses the same released
Prompt/Tool/Provider/OJ/Profile path, while keeping its queue and state under a
new run root. No request with an ambiguous outcome is replayed.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import signal
import subprocess
import sys
import time
import uuid
from pathlib import Path

_STOP_EVENT = None


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest().upper()


def _atomic(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    temp.write_text(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    os.replace(temp, path)


def _process_identity(pid: int) -> dict | None:
    command = (f"$p=Get-CimInstance Win32_Process -Filter 'ProcessId={int(pid)}'; "
               "if($null -eq $p){'{}'}else{$p | Select-Object ProcessId,ParentProcessId,CreationDate,ExecutablePath,CommandLine | ConvertTo-Json -Compress}")
    result = subprocess.run(["powershell.exe", "-NoProfile", "-NonInteractive", "-Command", command],
        capture_output=True, text=True, timeout=10, encoding="utf-8", errors="replace")
    if result.returncode:
        raise RuntimeError("G3_PROCESS_IDENTITY_QUERY_FAILED")
    try:
        value = json.loads(result.stdout.strip() or "{}")
    except json.JSONDecodeError as exc:
        raise RuntimeError("G3_PROCESS_IDENTITY_JSON_INVALID") from exc
    return value if isinstance(value, dict) and value.get("ProcessId") else None


def _configure(args) -> tuple[Path, Path, Path, Path, Path, Path]:
    run_root = Path(args.run_root).resolve()
    stage_root = Path(args.stage_root).resolve()
    bindings = json.loads((stage_root / "COMPONENT_BINDINGS.json").read_text(encoding="utf-8"))
    components = bindings.get("components", {})
    integration = (stage_root / components["integration"]["relative_path"]).resolve()
    harness = (stage_root / components["harness"]["relative_path"]).resolve()
    provider = (stage_root / components["provider"]["relative_path"]).resolve()
    oj = (stage_root / components["oj"]["relative_path"]).resolve()
    python = Path(args.python).resolve()
    for path in (integration / "run_r5_oj_worker.py", integration / "n0_integration" / "service_supervisor.py",
                 harness / "RELEASE_MANIFEST.json", provider / "R20_PROVIDER_RELEASE_MANIFEST.json",
                 oj / "RELEASE_MANIFEST.json", python):
        if not path.is_file():
            raise RuntimeError("G3_RUNTIME_COMPONENT_MISSING:" + str(path))
    os.environ.update({
        "N0_G3_RUNTIME_ROOT": str(run_root),
        "N0_G3_HARNESS_RELEASE_ROOT": str(harness),
        "N0_G3_OJ_RELEASE_ROOT": str(oj),
        "N0_G3_PROVIDER_RELEASE_ROOT": str(provider),
        "N0_G3_KERNELBENCH_ROOT": str(Path(r"D:\projects\kernelagent\runtime\harness\releases\N0_AUTONOMOUS_INFORMATION_ACQUISITION_R6\third_party\KernelBench_repo")),
        "N0_G3_PYTHON": str(python),
        "N0_G3_PROTOCOL_PATH": str(harness / "protocol" / "G3_EXPERIMENT_CONFIGURATION_R1.json"),
        "N0_OJ_QUEUE_ROOT": str(run_root / "queue"),
        "N0_PROVIDER_QUEUE_ROOT": str(run_root / "provider_queue"),
        "N0_PROFILE_STATE_ROOT": str(run_root / "profile"),
        "N0_GPU_LEASE_PATH": str(Path(args.gpu_lease).resolve()),
        "N0_PROVIDER_RELEASE_ID": json.loads((provider / "R20_PROVIDER_RELEASE_MANIFEST.json").read_text(encoding="utf-8"))["release_id"],
        "N0_PROVIDER_MANIFEST_SHA256": _sha(provider / "R20_PROVIDER_RELEASE_MANIFEST.json"),
        "PYTHONPATH": os.pathsep.join([str(harness), str(integration), str(provider), str(oj),
            str(Path(r"D:\projects\kernelagent\runtime\harness\releases\N0_AUTONOMOUS_INFORMATION_ACQUISITION_R6\third_party\KernelBench_repo\src"))]),
    })
    return run_root, stage_root, integration, harness, provider, oj


def _verified_manifest(path: Path, expected_release: str) -> dict:
    raw = path.read_bytes()
    doc = json.loads(raw)
    sidecar = path.with_name(path.name + ".sha256")
    if not sidecar.is_file() or sidecar.read_text(encoding="ascii").split()[0].upper() != hashlib.sha256(raw).hexdigest().upper():
        raise RuntimeError("G3_COMPONENT_MANIFEST_SHA_MISMATCH:" + str(path))
    if doc.get("release_id") != expected_release:
        raise RuntimeError("G3_COMPONENT_RELEASE_ID_MISMATCH:" + str(path))
    for item in doc.get("source_files", []):
        member = path.parent / item["path"]
        if not member.is_file() or _sha(member) != item["sha256"].upper():
            raise RuntimeError("G3_COMPONENT_MEMBER_SHA_MISMATCH:" + item["path"])
    return doc


def _environment(args, roots):
    run_root, stage_root, integration, harness, provider, oj = roots
    bindings = json.loads((stage_root / "COMPONENT_BINDINGS.json").read_text(encoding="utf-8"))
    components = bindings.get("components", {})
    hbind, ibind, pbind, obind = (components[k] for k in ("harness", "integration", "provider", "oj"))
    hdoc = _verified_manifest(harness / "RELEASE_MANIFEST.json", hbind["release_id"])
    idoc = _verified_manifest(integration / "RELEASE_MANIFEST.json", ibind["release_id"])
    pdoc = json.loads((provider / "R20_PROVIDER_RELEASE_MANIFEST.json").read_text(encoding="utf-8"))
    provider_sha = _sha(provider / "R20_PROVIDER_RELEASE_MANIFEST.json")
    if (pdoc.get("release_id") != pbind.get("release_id")
            or provider_sha != pbind.get("manifest_sha256")):
        raise RuntimeError("G3_PROVIDER_MANIFEST_BINDING_MISMATCH")
    if (idoc.get("release_id") != ibind.get("release_id")
            or _sha(integration / "RELEASE_MANIFEST.json") != ibind.get("manifest_sha256")):
        raise RuntimeError("G3_INTEGRATION_MANIFEST_BINDING_MISMATCH")
    if (hdoc.get("release_id") != hbind.get("release_id")
            or _sha(harness / "RELEASE_MANIFEST.json") != hbind.get("manifest_sha256")):
        raise RuntimeError("G3_HARNESS_COMPONENT_BINDING_MISMATCH")
    if (oj.name != obind.get("release_id") or _sha(oj / "RELEASE_MANIFEST.json") != obind.get("manifest_sha256")):
        raise RuntimeError("G3_OJ_COMPONENT_BINDING_MISMATCH")
    if "G3" not in pdoc.get("treatment_allowlist", []):
        raise RuntimeError("G3_PROVIDER_ROUTE_NOT_ALLOWLISTED")
    _verified_manifest(oj / "RELEASE_MANIFEST.json", oj.name)
    return {"harness_release_id": hdoc["release_id"],
            "harness_manifest_sha256": _sha(harness / "RELEASE_MANIFEST.json"),
            "integration_release_id": integration.name,
            "integration_manifest_sha256": _sha(integration / "RELEASE_MANIFEST.json"),
            "oj_release_id": oj.name, "oj_manifest_sha256": _sha(oj / "RELEASE_MANIFEST.json"),
            "provider_release_id": pdoc["release_id"],
            "provider_manifest_sha256": provider_sha,
            "treatment_id": "G3", "classification": "NON_SCIENTIFIC_DIAGNOSTIC"}


def _start_services(args, roots) -> dict:
    run_root, stage_root, integration, harness, provider, oj = roots
    queue = run_root / "queue"
    profile = run_root / "profile"
    lease = Path(args.gpu_lease).resolve()
    logs = run_root / "logs"
    logs.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ)
    env["N0_PROVIDER_RELEASE_ID"] = json.loads((provider / "R20_PROVIDER_RELEASE_MANIFEST.json").read_text(encoding="utf-8"))["release_id"]
    env["N0_PROFILE_STATE_ROOT"] = str(profile)
    env["N0_GPU_TIMING_GATE_PATH"] = str(profile / "timing_gate.json")
    env["N0_OJ_QUEUE_ROOT"] = str(queue)
    env["N0_G3_HARNESS_RELEASE_ROOT"] = str(harness)
    env["N0_G3_OJ_RELEASE_ROOT"] = str(oj)
    env["N0_G3_PYTHON"] = str(Path(args.python).resolve())
    env["N0_G3_RUNTIME_ROOT"] = str(run_root)
    env["N0_G3_PROVIDER_RELEASE_ROOT"] = str(provider)
    provider_cmd = [str(Path(args.python).resolve()), "-u", str(stage_root / "g3_provider_queue_worker.py"),
                    "--root", str(run_root / "provider_queue")]
    provider_out = (logs / "provider.stdout.log").open("ab")
    provider_err = (logs / "provider.stderr.log").open("ab")
    provider_proc = subprocess.Popen(provider_cmd, cwd=str(stage_root), env=env, stdout=provider_out, stderr=provider_err,
                                     creationflags=getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0))
    supervisor_cmd = [str(Path(args.python).resolve()), "-u", str(integration / "n0_integration" / "service_supervisor.py"),
                      "--queue-root", str(queue), "--qualification-out", str(run_root / "qualification" / "initial.json"),
                      "--profile-whitelist", str(integration / "N0_PROFILE_WHITELIST_NONSCIENTIFIC_V1.json"),
                      "--gpu-lease-path", str(lease), "--idle-seconds", "0.5"]
    sup_out = (logs / "oj-supervisor.stdout.log").open("ab")
    sup_err = (logs / "oj-supervisor.stderr.log").open("ab")
    supervisor = subprocess.Popen(supervisor_cmd, cwd=str(integration), env=env, stdout=sup_out, stderr=sup_err,
                                  creationflags=getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0))
    deadline = time.monotonic() + 300
    status_path = queue / "service_status.json"
    try:
        while time.monotonic() < deadline:
            if supervisor.poll() is not None:
                raise RuntimeError("G3_OJ_SUPERVISOR_EXITED:" + str(supervisor.returncode))
            if status_path.is_file():
                status = json.loads(status_path.read_text(encoding="utf-8"))
                if status.get("state") == "RUNNING" and isinstance(status.get("pid"), int):
                    return {"provider_dispatcher_pid": provider_proc.pid, "supervisor_pid": supervisor.pid,
                            "oj_service_status": status, "queue_root": str(queue), "profile_root": str(profile)}
            time.sleep(0.5)
        raise TimeoutError("G3_OJ_SUPERVISOR_START_TIMEOUT")
    except BaseException:
        # Do not orphan the provider dispatcher when OJ startup fails. This
        # run-scoped process was created above with this exact queue root.
        for proc in (supervisor, provider_proc):
            if proc.poll() is None:
                try:
                    proc.terminate()
                    proc.wait(timeout=5)
                except Exception:
                    try:
                        subprocess.run(["taskkill.exe", "/PID", str(proc.pid), "/T", "/F"],
                                       capture_output=True, text=True, timeout=10, check=False)
                        proc.wait(timeout=5)
                    except Exception:
                        pass
        for stream in (provider_out, provider_err, sup_out, sup_err):
            try:
                stream.close()
            except Exception:
                pass
        raise


def _register_and_seed(args, roots):
    run_root, stage_root, integration, harness, provider, oj = roots
    sys.path[:0] = [str(harness), str(integration), str(provider), str(oj),
                    str(harness / "third_party" / "KernelBench_repo" / "src")]
    import formal_runtime as fr
    from n0_autonomous.engine import canonical_bytes
    from n0_autonomous.parallel_runtime import N0PersistentQueue, N0_COMPILER_CONTRACT_SHA256
    fr.ROOT = run_root
    fr.PROJECT = stage_root
    fr.INTEGRATION = integration
    fr.BUNDLE_ROOT = stage_root
    fr.HARNESS = harness
    fr.OJ = oj
    fr.R20 = provider
    fr.PYTHON = Path(args.python).resolve()
    fr.QUEUE_ROOT = run_root / "queue"
    fr.PROVIDER_ROOT = run_root / "provider_queue"
    fr.GPU_LEASE = Path(args.gpu_lease).resolve()
    fr.PROFILE_STATE_ROOT = run_root / "profile"
    fr.PROTOCOL_PATH = harness / "protocol" / "G3_EXPERIMENT_CONFIGURATION_R1.json"
    episode_id = args.episode_id
    queue = N0PersistentQueue(fr.QUEUE_ROOT, max_active_episodes=4, worker_count=4, oj_gpu_active_jobs_max=1)
    snap = queue.snapshot()
    if snap["episodes"]:
        if len(snap["episodes"]) != 1 or snap["episodes"][0]["episode_id"] != episode_id:
            raise RuntimeError("G3_DIAGNOSTIC_QUEUE_ALREADY_BOUND_TO_DIFFERENT_EPISODE")
        return fr, queue, episode_id
    template = harness / "tests" / "fixtures" / "N0_AUTONOMOUS_INFORMATION_ACQUISITION_R4" / "template"
    files = {name: (template / name).read_bytes() for name in
             ("task_definition.json", "candidate_abi.json", "candidate.cu", "visible_observation.json")}
    abi = json.loads(files["candidate_abi.json"].decode("utf-8"))
    abi["compiler_contract_sha256"] = N0_COMPILER_CONTRACT_SHA256
    abi["oj_release_id"] = oj.name
    abi["oj_manifest_sha256"] = _sha(oj / "RELEASE_MANIFEST.json")
    files["candidate_abi.json"] = (json.dumps(abi, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
    allocation = {"authorized": True, "group": "G3", "episode_id": episode_id,
                  "replicate_slot": "G3-ENGINEERING-DIAGNOSTIC-01", "treatment_id": "G3",
                  "hardware_information": "FULL", "profile_information": "FULL",
                  "expert_knowledge": "ABSENT", "max_candidates": 30,
                  "classification": "NON_SCIENTIFIC_DIAGNOSTIC"}
    paths = queue.register_episode(episode_id=episode_id, replicate_slot=allocation["replicate_slot"],
        treatment_id="G3", approved_allocation=allocation, max_candidates=30, workspace_files=files)
    fr._queue_c0000(queue, episode_id, 1, _sha(fr.PROTOCOL_PATH))
    return fr, queue, episode_id


def _wait_and_settle_seed(fr, queue, episode_id: str, timeout: int = 900) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if (fr.QUEUE_ROOT.parent / "STOP_REQUESTED").is_file():
            return {"state": "STOPPED_BEFORE_SEED_SETTLEMENT", "episode_id": episode_id}
        row = next(x for x in queue.snapshot()["oj_requests"] if x["episode_id"] == episode_id and x["candidate_id"] == "C0000")
        if row["state"] == "COMPLETED":
            receipt = json.loads(row["receipt_json"])
            profile = fr._wait_post_receipt_profile(row, receipt)
            status = fr._validate_c0000_receipt(row, receipt, episode_id, profile)
            item = {"episode_id": episode_id, "request_id": row["request_id"], "row": row,
                    "receipt": receipt, "profile_receipt": profile, "initialization_status": status,
                    "protocol_sha256": _sha(fr.PROTOCOL_PATH)}
            fr._commit_c0000_consumption_batch(queue, [item])
            fr._write_observation(queue, episode_id, row, receipt, "BASELINE",
                                  _sha(fr.PROTOCOL_PATH), profile_receipt=profile)
            fr.write_episode_summary(queue, episode_id)
            return {"request_id": row["request_id"], "receipt_sha256": row["receipt_sha256"],
                    "verdict": receipt.get("verdict"), "score": receipt.get("score"),
                    "profile_status": (profile or {}).get("status", (receipt.get("profile") or {}).get("status"))}
        if row["state"] in {"HOLD_UNKNOWN", "FAILED_FINAL"}:
            raise RuntimeError("G3_SEED_REQUEST_TERMINAL_UNTRUSTED:" + str(row["state"]))
        time.sleep(0.5)
    raise TimeoutError("G3_C0000_SEED_TIMEOUT")


def _worker_main(args) -> int:
    global _STOP_EVENT
    roots = _configure(args)
    release_identity = _environment(args, roots)
    run_root, stage_root, integration, harness, provider, oj = roots
    run_root.mkdir(parents=True, exist_ok=True)
    status_file = run_root / "worker_status.json"
    stop_file = run_root / "STOP_REQUESTED"
    _atomic(status_file, {"state": "STARTING", "pid": os.getpid(), "episode_id": args.episode_id,
                          "release_identity": release_identity, "started_at_utc": time.time()})
    def stop_signal(_sig, _frame):
        if _STOP_EVENT is not None:
            _STOP_EVENT.set()
        stop_file.write_text("operator stop\n", encoding="utf-8")
    signal.signal(signal.SIGINT, stop_signal)
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, stop_signal)
    fr, queue, episode_id = _register_and_seed(args, roots)
    seed = _wait_and_settle_seed(fr, queue, episode_id)
    if seed.get("state") == "STOPPED_BEFORE_SEED_SETTLEMENT":
        _atomic(status_file, {"state": seed["state"], "pid": os.getpid(), "episode_id": episode_id,
                              "release_identity": release_identity, "heartbeat_unix": time.time()})
        return 0
    _atomic(status_file, {"state": "RUNNING_AGENT", "pid": os.getpid(), "episode_id": episode_id,
                          "seed": seed, "release_identity": release_identity, "heartbeat_unix": time.time()})
    import threading
    stop_event = threading.Event()
    _STOP_EVENT = stop_event
    while not stop_file.exists():
        result = fr._run_episode(episode_id, stop_event)
        snap = queue.snapshot()
        _atomic(status_file, {"state": result.get("state"), "pid": os.getpid(), "episode_id": episode_id,
                              "last_result": result, "candidate_budget_used": next(x["candidates_used"] for x in snap["episodes"] if x["episode_id"] == episode_id),
                              "queue": {"requests": len(snap["oj_requests"]), "depth": snap["queue_depth"], "running": snap["gpu_active_jobs"]},
                              "release_identity": release_identity, "heartbeat_unix": time.time()})
        if result.get("state") in {"SCIENTIFICALLY_COMPLETED", "BUDGET_EXHAUSTED", "INFRASTRUCTURE_HOLD"}:
            return 0 if result.get("state") != "INFRASTRUCTURE_HOLD" else 2
        if result.get("state") == "STOP_REQUESTED_AFTER_CURRENT_INVOCATION":
            return 0
        if stop_file.exists():
            break
        time.sleep(0.5)
    _atomic(status_file, {"state": "STOPPED_AFTER_CURRENT_INVOCATION", "pid": os.getpid(),
                          "episode_id": episode_id, "heartbeat_unix": time.time(), "release_identity": release_identity})
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["start", "status", "stop", "resume", "worker"])
    parser.add_argument("--stage-root", required=True)
    parser.add_argument("--run-root", required=True)
    parser.add_argument("--python", required=True)
    parser.add_argument("--gpu-lease", required=True)
    parser.add_argument("--episode-id", default="G3-ENG-E01")
    parser.add_argument("--diagnostic", action="store_true")
    args = parser.parse_args()
    roots = _configure(args)
    run_root = roots[0]
    status_path = run_root / "worker_status.json"
    if args.action == "worker":
        if not args.diagnostic:
            raise RuntimeError("G3_RUNTIME_DIRECT_WORKER_REQUIRES_DIAGNOSTIC_OR_STARTGATE")
        return _worker_main(args)
    if args.action == "status":
        status = json.loads(status_path.read_text(encoding="utf-8")) if status_path.is_file() else {"state": "NOT_STARTED"}
        print(json.dumps(status, ensure_ascii=False, indent=2))
        return 0
    if args.action == "stop":
        run_root.mkdir(parents=True, exist_ok=True)
        stop = run_root / "STOP_REQUESTED"
        if not stop.exists():
            stop.write_text("operator requested stop\n", encoding="utf-8")
        print(json.dumps({"state": "STOP_REQUESTED", "path": str(stop)}, indent=2))
        return 0
    if args.action == "resume":
        run_root, stage_root, integration, harness, provider, oj = roots
        status = json.loads(status_path.read_text(encoding="utf-8")) if status_path.is_file() else {}
        old_pid = status.get("pid")
        if isinstance(old_pid, int) and _process_identity(old_pid):
            raise RuntimeError("G3_RESUME_REFUSED_WORKER_STILL_ALIVE")
        sys.path[:0] = [str(harness)]
        from n0_autonomous.parallel_runtime import N0PersistentQueue
        queue = N0PersistentQueue(run_root / "queue", max_active_episodes=4, worker_count=4, oj_gpu_active_jobs_max=1)
        snap = queue.snapshot()
        if len(snap["episodes"]) != 1 or snap["episodes"][0]["episode_id"] != args.episode_id:
            raise RuntimeError("G3_RESUME_EPISODE_IDENTITY_INVALID")
        if any(x["state"] == "HOLD_UNKNOWN" for x in snap["oj_requests"]):
            raise RuntimeError("G3_RESUME_BLOCKED_OJ_OUTCOME_UNKNOWN_NO_REPLAY")
        if any(x["state"] in {"QUEUED", "DISPATCHED", "OUTCOME_UNKNOWN"} for x in snap["provider_requests"]):
            raise RuntimeError("G3_RESUME_BLOCKED_PROVIDER_OUTCOME_UNKNOWN_NO_REPLAY")
        if any(x["state"] == "RUNNING" for x in snap["oj_requests"]):
            raise RuntimeError("G3_RESUME_BLOCKED_OJ_REQUEST_STILL_RUNNING")
        if (run_root / "STOP_REQUESTED").exists():
            raise RuntimeError("G3_RESUME_STOP_REQUESTED_MUST_BE_ARCHIVED_BY_CONTROLLER")
        args.action = "start"
        args.diagnostic = True
    if args.action == "start":
        if not args.diagnostic:
            raise RuntimeError("G3_FORMAL_START_BLOCKED_UNTIL_FINAL_REFERENCE_QUALIFICATION_AND_STARTGATE")
        if status_path.is_file():
            prior = json.loads(status_path.read_text(encoding="utf-8"))
            if prior.get("state") in {"RUNNING_AGENT", "STARTING"}:
                pid = prior.get("pid")
                if isinstance(pid, int):
                    observed = _process_identity(pid)
                    if observed:
                        command_line = str(observed.get("CommandLine") or "").casefold()
                        if "n0_autonomous.g3_worker_runtime" in command_line and str(run_root).casefold() in command_line:
                            raise RuntimeError("G3_WORKER_ALREADY_ACTIVE")
                        raise RuntimeError("G3_WORKER_PID_REUSED_OR_IDENTITY_UNVERIFIABLE")
        identity = _environment(args, roots)
        run_root.mkdir(parents=True, exist_ok=True)
        identity_path = run_root / "RUN_IDENTITY.json"
        if identity_path.is_file():
            prior_identity = json.loads(identity_path.read_text(encoding="utf-8"))
            if prior_identity.get("release_identity") != identity:
                queue_db = run_root / "queue" / "n0_runtime.sqlite3"
                if queue_db.is_file():
                    import sqlite3
                    con = sqlite3.connect(queue_db.resolve().as_uri() + "?mode=ro", uri=True)
                    try:
                        episodes = con.execute("SELECT episode_id,candidates_used FROM episodes").fetchall()
                        requests = con.execute("SELECT request_id,candidate_id,candidate_sha256,state,receipt_json FROM oj_requests").fetchall()
                        providers = con.execute("SELECT COUNT(*) FROM provider_requests").fetchone()[0]
                        slots = con.execute("SELECT COUNT(*) FROM candidate_slots").fetchone()[0]
                        safe_seed_transition = (
                            len(episodes) == 1 and episodes[0][0] == args.episode_id and int(episodes[0][1]) == 0
                            and len(requests) == 1 and requests[0][1] == "C0000"
                            and str(requests[0][2]).upper() == "CBD7E93B47AD3957E4E133BDE45C0AF1E933DD9BEC3525C27637049337FB2D91"
                            and requests[0][3] == "QUEUED" and requests[0][4] is None
                            and providers == 0 and slots == 0)
                    finally:
                        con.close()
                    if episodes and not safe_seed_transition:
                        raise RuntimeError("G3_DIAGNOSTIC_RELEASE_CHANGE_AFTER_WORK_STARTED")
                archive = run_root / ("RUN_IDENTITY.previous." + str(prior_identity.get("release_identity", {}).get("harness_manifest_sha256", "unknown")) + ".json")
                if not archive.exists():
                    archive.write_text(json.dumps(prior_identity, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
                if queue_db.is_file() and safe_seed_transition:
                    transition = {"schema": "G3_DIAGNOSTIC_RELEASE_TRANSITION_V1", "run_id": run_root.name,
                        "episode_id": args.episode_id, "reason": "C0000_QUEUE_DISPATCH_COMPATIBILITY_FIX",
                        "old_release_identity": prior_identity.get("release_identity"), "new_release_identity": identity,
                        "preserved_request_id": requests[0][0], "request_state": "QUEUED", "candidate_budget_used": 0,
                        "provider_requests": 0, "candidate_slots": 0, "no_candidate_execution_claimed": True,
                        "created_at_unix": time.time()}
                    transition_path = run_root / ("release_transition_" + identity["harness_manifest_sha256"] + ".json")
                    if transition_path.exists():
                        saved = json.loads(transition_path.read_text(encoding="utf-8"))
                        compare = {k:v for k,v in transition.items() if k != "created_at_unix"}
                        old_compare = {k:v for k,v in saved.items() if k != "created_at_unix"}
                        if old_compare != compare:
                            raise RuntimeError("G3_DIAGNOSTIC_RELEASE_TRANSITION_CONFLICT")
                    else:
                        _atomic(transition_path, transition)
        _atomic(identity_path, {"schema": "N0_G3_DIAGNOSTIC_RUN_V1", "run_id": run_root.name,
            "classification": "NON_SCIENTIFIC_DIAGNOSTIC", "release_identity": identity, "created_at_unix": time.time()})
        old_start = run_root / "start_receipt.json"
        if old_start.is_file():
            prior_start_raw = old_start.read_bytes()
            prior_start_path = run_root / ("start_receipt.previous." + hashlib.sha256(prior_start_raw).hexdigest().upper() + ".json")
            if not prior_start_path.exists():
                prior_start_path.write_bytes(prior_start_raw)
        prior_start = run_root / "start_receipt.json"
        if prior_start.is_file():
            prior_services = json.loads(prior_start.read_text(encoding="utf-8")).get("services", {})
        else:
            prior_services = {}
        oj_status_path = run_root / "queue" / "service_status.json"
        oj_status = json.loads(oj_status_path.read_text(encoding="utf-8")) if oj_status_path.is_file() else {}
        provider_pid = prior_services.get("provider_dispatcher_pid")
        supervisor_pid = prior_services.get("supervisor_pid")
        oj_pid = ((oj_status.get("detail") or {}).get("process_identity") or {}).get("pid") or oj_status.get("pid")
        reusable = oj_status.get("state") == "RUNNING" and all(isinstance(pid, int) for pid in (provider_pid, supervisor_pid))
        if reusable:
            observed = [_process_identity(int(pid)) for pid in (provider_pid, supervisor_pid, oj_pid)]
            if any(x is None for x in observed):
                reusable = False
            else:
                expected_fragments = ["g3_provider_queue_worker.py", "service_supervisor.py", "run_r5_oj_worker.py"]
                for item, fragment in zip(observed, expected_fragments):
                    command_line = str(item.get("CommandLine") or "").casefold()
                    if fragment not in command_line or str(run_root).casefold() not in command_line:
                        reusable = False
                        break
        if reusable:
            services = {"provider_dispatcher_pid": provider_pid, "supervisor_pid": supervisor_pid,
                        "oj_service_status": oj_status, "queue_root": str(run_root / "queue"),
                        "profile_root": str(run_root / "profile"), "reused_existing_services": True}
        else:
            services = _start_services(args, roots)
        env = dict(os.environ)
        cmd = [str(Path(args.python).resolve()), "-u", "-m", "n0_autonomous.g3_worker_runtime", "worker",
               "--stage-root", str(roots[1]), "--run-root", str(run_root), "--python", str(Path(args.python).resolve()),
               "--gpu-lease", str(Path(args.gpu_lease).resolve()), "--episode-id", args.episode_id, "--diagnostic"]
        out = (run_root / "logs" / "worker.stdout.log").open("ab")
        err = (run_root / "logs" / "worker.stderr.log").open("ab")
        worker = subprocess.Popen(cmd, cwd=str(roots[2].parent), env=env, stdout=out, stderr=err,
                                  creationflags=getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0))
        result = {"state": "STARTED", "worker_pid": worker.pid, "services": services,
                  "release_identity": identity, "run_root": str(run_root)}
        _atomic(run_root / "start_receipt.json", result)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
