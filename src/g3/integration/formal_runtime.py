from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(os.environ.get("N0_G3_RUNTIME_ROOT", r"D:\projects\kernelagent\runtime")).resolve()
PROJECT = ROOT.parent
INTEGRATION = Path(__file__).resolve().parent
R5_INTEGRATION = INTEGRATION
BUNDLE_ROOT = INTEGRATION.parent
COMPONENT_BINDINGS = json.loads((BUNDLE_ROOT / "COMPONENT_BINDINGS.json").read_text(encoding="utf-8"))
HARNESS = BUNDLE_ROOT / COMPONENT_BINDINGS["components"]["harness"]["relative_path"]
HARNESS_PARENT = ROOT / "harness" / "releases" / "N0_AUTONOMOUS_INFORMATION_ACQUISITION_R6"
OJ = BUNDLE_ROOT / COMPONENT_BINDINGS["components"]["oj"]["relative_path"]
R20 = Path(os.environ.get("N0_G3_PROVIDER_RELEASE_ROOT", str(ROOT / "provider" / "releases" / "N0_PARATERA_PROVIDER_R20"))).resolve()
PYTHON = Path(os.environ.get("N0_G3_PYTHON", str(ROOT / ".venv_n0_r6" / "Scripts" / "python.exe"))).resolve()
QUEUE_ROOT = Path(os.environ.get("N0_OJ_QUEUE_ROOT", str(ROOT / "state" / "n0_oj_formal"))).resolve()
PROVIDER_ROOT = Path(os.environ.get("N0_PROVIDER_QUEUE_ROOT", str(ROOT / "state" / "n0_provider_formal"))).resolve()
GPU_LEASE = Path(os.environ.get("N0_GPU_LEASE_PATH", str(ROOT / "state" / "n0_gpu_device.lock"))).resolve()
PROFILE_STATE_ROOT = Path(os.environ.get("N0_PROFILE_STATE_ROOT", str(ROOT / "state" / "n0_oj_profiles"))).resolve()
PROTOCOL_PATH = Path(os.environ.get("N0_G3_PROTOCOL_PATH", str(ROOT / "protocol" / "N0_FORMAL_PROTOCOL_V1_2.json"))).resolve()
READINESS_PATH = ROOT / "reports" / "n0_timing_qualification" / "N0_FORMAL_READINESS_REPORT_CURRENT_V3.json"
INIT_PATH = ROOT / "state" / "N0_FORMAL_INITIALIZATION_V1.json"
SEED_SHA = "CBD7E93B47AD3957E4E133BDE45C0AF1E933DD9BEC3525C27637049337FB2D91"
OJ_SHA = COMPONENT_BINDINGS["components"]["oj"]["manifest_sha256"]
R20_SHA = "DDB118AA234A7AC6B7AF08D3BA123ACFDBE603815267E6AA80E0F7E5BE16159F"
HARNESS_SHA = COMPONENT_BINDINGS["components"]["harness"]["manifest_sha256"]
EPISODE_IDS = [f"N0_FORMAL_REP{i:02d}" for i in range(1, 5)]
R8_PARENT_SHA = "EF8EFD57A5C9629205090268268DC6A14B934F1B54D1B634CBB1AD39F3B17D6F"
R8_1_SHA = "3EFA0A4D4D023BAB30644DE988971D8D3C91DE11446EBC0CCCD957A1C25ACFBA"
PARTIAL_V1_2_PROTOCOL_SHA = "64999708EBCDF684802427239F025A1A54EFD19CE1ECD18734458AB9434CA1E7"
_ATOMIC_LOCK = threading.RLock()


def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest().upper()


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def current_process_identity() -> dict[str, Any]:
    q = (f"$p=Get-CimInstance Win32_Process -Filter 'ProcessId={os.getpid()}'; "
         "if($null -eq $p){'{}'}else{$p | Select-Object ProcessId,ParentProcessId,CreationDate,ExecutablePath,CommandLine | ConvertTo-Json -Compress}")
    proc = subprocess.run(["powershell.exe", "-NoProfile", "-Command", q], capture_output=True, text=True, timeout=10)
    if proc.returncode:
        raise RuntimeError("FORMAL_RUNNER_PROCESS_IDENTITY_QUERY_FAILED")
    result = json.loads(proc.stdout.strip() or "{}")
    expected_script = (INTEGRATION / "formal_runtime.py").resolve()
    actual_script = Path(sys.argv[0]).resolve()
    command_line = str(result.get("CommandLine", "")).replace('"', "")
    if (int(result.get("ProcessId", -1)) != os.getpid()
            or actual_script != expected_script
            or sys.argv[1:] not in (["run"], ["run-recovered"])
            or str(expected_script) not in command_line):
        raise RuntimeError("FORMAL_RUNNER_PROCESS_IDENTITY_MISMATCH")
    return {"pid": os.getpid(), "parent_pid": result.get("ParentProcessId"),
            "creation_date": result.get("CreationDate"), "executable_path": result.get("ExecutablePath"),
            "sys_executable": str(Path(sys.executable).resolve()), "command_line": result.get("CommandLine")}


def atomic(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode("utf-8")
    with _ATOMIC_LOCK:
        tmp = path.with_name(path.name + f".{os.getpid()}.{threading.get_ident()}.{uuid.uuid4().hex}.tmp")
        with tmp.open("wb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        last_error = None
        for attempt in range(40):
            try:
                os.replace(tmp, path)
                return
            except PermissionError as exc:
                last_error = exc
                time.sleep(0.05)
        raise RuntimeError("ATOMIC_REPLACE_RETRY_EXHAUSTED:" + str(path)) from last_error


def append_stable_event(store, episode_id: str, event: dict[str, Any], event_id: str) -> None:
    db = store._connect()
    try:
        db.execute("BEGIN IMMEDIATE")
        prior = db.execute("SELECT event_json FROM episode_events WHERE event_id=?", (event_id,)).fetchone()
        if prior:
            old = json.loads(prior[0])
            comparable = {k: v for k, v in old.items() if k not in {"event_id", "episode_id", "created_at"}}
            if old.get("episode_id") != episode_id or comparable != event:
                raise RuntimeError("FORMAL_EVENT_IDEMPOTENCY_CONFLICT:" + event_id)
        else:
            store._insert_event_tx(db, episode_id, event, event_id)
        db.execute("COMMIT")
    except BaseException:
        if db.in_transaction:
            db.execute("ROLLBACK")
        raise
    finally:
        db.close()
    store.reconcile_episode_exports(episode_id)


def retry_infrastructure_measurement_once(store, row: dict[str, Any], receipt: dict[str, Any]) -> dict[str, Any] | None:
    """Queue one same-source infrastructure retry without charging Candidate budget."""
    allowed = {"OJ_INFRASTRUCTURE_HOLD_REFERENCE_CONTROL_MISMATCH",
               "OJ_INFRASTRUCTURE_HOLD_REFERENCE_BASELINE_DRIFT",
               "OJ_INFRASTRUCTURE_HOLD_POST_EVALUATOR_CONTROL_UNAVAILABLE",
               "OJ_INFRASTRUCTURE_HOLD_RAW_TIMER_EVIDENCE_INVALID",
               "OJ_INFRASTRUCTURE_HOLD_CANDIDATE_TIMING_UNSTABLE"}
    if (receipt.get("verdict") not in allowed or receipt.get("candidate_sha256") != row.get("candidate_sha256")
            or str(row.get("invocation_id", "")).startswith("INFRA-RETRY-")):
        return None
    key = "timing-retry:" + str(row["request_id"])
    db = store._connect()
    try:
        db.execute("BEGIN IMMEDIATE")
        prior = db.execute("SELECT value FROM runtime_meta WHERE key=?", (key,)).fetchone()
        if prior:
            db.execute("COMMIT")
            return None
        seq = int(db.execute("SELECT value FROM runtime_meta WHERE key='queue_sequence'").fetchone()[0]) + 1
        db.execute("UPDATE runtime_meta SET value=? WHERE key='queue_sequence'", (str(seq),))
        request_id = "N0-OJ-RETRY-" + uuid.uuid4().hex
        invocation = "INFRA-RETRY-" + str(row["request_id"])
        now_ts = time.time()
        db.execute("INSERT INTO oj_requests(request_id,episode_id,invocation_id,candidate_id,candidate_sha256,state,queue_sequence,candidate_source_path,created_at,updated_at) VALUES(?,?,?,?,?,'QUEUED',?,?,?,?)",
                   (request_id, row["episode_id"], invocation, row["candidate_id"], row["candidate_sha256"], seq,
                    row["candidate_source_path"], now_ts, now_ts))
        db.execute("INSERT INTO runtime_meta(key,value) VALUES(?,?)", (key, request_id))
        event = {"event_type":"OJ_INFRASTRUCTURE_RETRY_QUEUED", "request_id":request_id,
                 "retry_of_request_id":row["request_id"], "candidate_id":row["candidate_id"],
                 "candidate_sha256":row["candidate_sha256"], "candidate_budget_delta":0,
                 "trigger_verdict":receipt["verdict"], "retry_limit":1}
        store._insert_event_tx(db, row["episode_id"], event, "oj-infra-retry:" + str(row["request_id"]))
        db.execute("UPDATE episodes SET state='WAITING_OJ',updated_at=? WHERE episode_id=?",
                   (now_ts, row["episode_id"]))
        db.execute("COMMIT")
    except BaseException:
        if db.in_transaction:
            db.execute("ROLLBACK")
        raise
    finally:
        db.close()
    store.reconcile_episode_exports(row["episode_id"])
    return {"request_id":request_id, "retry_of_request_id":row["request_id"], "state":"QUEUED",
            "candidate_budget_delta":0}


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def verify_readiness(*, require_fresh: bool = True) -> dict[str, Any]:
    doc = load_json(READINESS_PATH)
    age = (datetime.now(timezone.utc) - datetime.fromisoformat(doc["computed_at_utc"]).astimezone(timezone.utc)).total_seconds()
    ids = [x["id"] for x in doc.get("checks", [])]
    if doc.get("overall_status") != "PRELAUNCH_READY" or age < 0 or (require_fresh and age > 300) or len(ids) != len(set(ids)):
        raise RuntimeError("PRELAUNCH_READINESS_NOT_CURRENT_OR_NOT_PASS")
    if any(x.get("status") != "PASS" for x in doc["checks"]):
        raise RuntimeError("PRELAUNCH_READINESS_HAS_BLOCKED_CHECK")
    protocol = load_json(PROTOCOL_PATH)
    if protocol.get("state") != "FROZEN_ACTIVE_BATCH_AMENDMENT" or protocol.get("schema") != "N0_FORMAL_PROTOCOL_V1_2":
        raise RuntimeError("FORMAL_PROTOCOL_NOT_FROZEN")
    if protocol.get("release_manifest_sha256", {}).get("harness_r6_1_formal") != sha((HARNESS / "RELEASE_MANIFEST.json").read_bytes()):
        raise RuntimeError("FORMAL_HARNESS_NOT_BOUND_BY_PROTOCOL")
    bound = protocol.get("release_manifest_sha256", {})
    if bound.get("integration_r8") != R8_PARENT_SHA or bound.get("integration_r8_1") != R8_1_SHA:
        raise RuntimeError("FORMAL_INTEGRATION_COMPATIBILITY_PARENT_NOT_BOUND_BY_PROTOCOL")
    current = json.loads((INTEGRATION / "RELEASE_MANIFEST.json").read_text(encoding="utf-8"))
    ancestors = [current.get("parent", {}).get("manifest_sha256")]
    if ancestors[0] != bound.get("integration_r8_1"):
        parent_path = ROOT / "integrations" / str(current.get("parent", {}).get("release_id")) / "RELEASE_MANIFEST.json"
        if not parent_path.is_file():
            raise RuntimeError("FORMAL_INTEGRATION_PARENT_MANIFEST_MISSING")
        parent_doc = json.loads(parent_path.read_text(encoding="utf-8"))
        if sha(parent_path.read_bytes()) != ancestors[0] or parent_doc.get("parent", {}).get("manifest_sha256") != bound.get("integration_r8_1"):
            raise RuntimeError("FORMAL_INTEGRATION_COMPATIBILITY_ANCESTRY_INVALID")
    if protocol.get("candidate", {}).get("promotion_labels") != {
            "C0000": "BASELINE", "strictly_higher_trusted_score": "PROMOTED",
            "tie_or_lower_or_unscored": "NOT_PROMOTED"}:
        raise RuntimeError("CANDIDATE_PROMOTION_SEMANTICS_NOT_FROZEN")
    required_checks = set(doc.get("required_prelaunch_check_ids", []))
    if len(required_checks) != len(doc.get("required_prelaunch_check_ids", [])) or required_checks != {x.get("id") for x in doc.get("checks", [])}:
        raise RuntimeError("PRELAUNCH_CHECK_ID_SET_INVALID")
    return doc


def verify_live_services() -> dict[str, Any]:
    from n0_integration.readiness import verify_manifest
    oj_manifest = OJ / "RELEASE_MANIFEST.json"
    provider_manifest = R20 / "R20_PROVIDER_RELEASE_MANIFEST.json"
    integration_manifest = INTEGRATION / "RELEASE_MANIFEST.json"
    if sha(oj_manifest.read_bytes()) != OJ_SHA or sha(provider_manifest.read_bytes()) != R20_SHA:
        raise RuntimeError("FORMAL_OJ_OR_PROVIDER_MANIFEST_MISMATCH")
    integration_sha = sha(integration_manifest.read_bytes())
    sidecar = (INTEGRATION / "RELEASE_MANIFEST.json.sha256").read_text(encoding="ascii").split()[0].upper()
    if integration_sha != sidecar or verify_manifest(integration_manifest, "N0_PRODUCTION_INTEGRATION_R8_3_MEASUREMENT_HOLD_CONTINUATION").get("status") != "PASS":
        raise RuntimeError("FORMAL_INTEGRATION_MANIFEST_INVALID")
    if verify_manifest(HARNESS / "RELEASE_MANIFEST.json", "N0_AUTONOMOUS_INFORMATION_ACQUISITION_R6_1_FORMAL_V1").get("status") != "PASS":
        raise RuntimeError("FORMAL_HARNESS_MANIFEST_INVALID")
    oj_status = subprocess.run(["powershell.exe", "-NoProfile", "-Command",
        "& 'D:\\projects\\kernelagent\\runtime\\integrations\\N0_PRODUCTION_INTEGRATION_R8_3_MEASUREMENT_HOLD_CONTINUATION\\Start-N0OJFormal.ps1' -Action Status | ConvertFrom-Json | ConvertTo-Json -Compress -Depth 4"],
        capture_output=True, text=True, timeout=20)
    provider_status = subprocess.run(["powershell.exe", "-NoProfile", "-Command",
        "& 'D:\\projects\\kernelagent\\runtime\\integrations\\N0_PRODUCTION_INTEGRATION_R5\\Start-N0ProviderFormal.ps1' -Action Status | ConvertFrom-Json | ConvertTo-Json -Compress -Depth 4"],
        capture_output=True, text=True, timeout=20)
    if oj_status.returncode or provider_status.returncode:
        raise RuntimeError("FORMAL_SHARED_SERVICE_STATUS_NOT_VERIFIED")
    oj_doc, provider_doc = json.loads(oj_status.stdout), json.loads(provider_status.stdout)
    if oj_doc.get("state") != "RUNNING_VERIFIED" or provider_doc.get("state") != "RUNNING_VERIFIED":
        raise RuntimeError("FORMAL_SHARED_SERVICE_NOT_RUNNING_VERIFIED")
    return {"oj": oj_doc, "provider": provider_doc,
            "oj_manifest_sha256": sha(oj_manifest.read_bytes()),
            "provider_manifest_sha256": sha(provider_manifest.read_bytes()),
            "integration_manifest_sha256": integration_sha,
            "harness_manifest_sha256": sha((HARNESS / "RELEASE_MANIFEST.json").read_bytes())}


def assert_formal_queues_clear() -> None:
    sys.path[:0] = [str(HARNESS), str(R5_INTEGRATION)]
    from n0_autonomous.parallel_runtime import N0PersistentQueue
    store = N0PersistentQueue(QUEUE_ROOT)
    snap = store.snapshot()
    if snap["episodes"] or snap["oj_requests"] or snap["provider_requests"]:
        raise RuntimeError("FORMAL_QUEUE_NOT_EMPTY_BEFORE_INITIALIZATION")
    for folder in ("queue", "processing"):
        if list((PROVIDER_ROOT / folder).glob("*.json")):
            raise RuntimeError("FORMAL_PROVIDER_QUEUE_HAS_UNSETTLED_REQUESTS")
    if store.is_aborted():
        raise RuntimeError("FORMAL_QUEUE_BATCH_ALREADY_ABORTED")


def _allocation(episode_id: str, slot: int, plan_sha: str, protocol_sha: str, created_at_utc: str) -> dict[str, Any]:
    return {"authorized": True, "episode_id": episode_id, "replicate_slot": f"N0_FORMAL_SLOT_{slot:02d}",
        "treatment_id": "N0_AUTONOMOUS", "max_candidates": 30, "seed_candidate_id": "C0000",
        "seed_sha256": SEED_SHA, "allocation_plan_sha256": plan_sha,
        "protocol_sha256": protocol_sha, "authority": "EXPLICIT_USER_AUTHORIZED_CONDITIONAL_FORMAL_LAUNCH",
        "created_at_utc": created_at_utc}


def _queue_c0000(store, episode_id: str, slot: int, protocol_sha: str) -> dict[str, Any]:
    from n0_autonomous.engine import canonical_bytes
    paths = store.episode_paths(episode_id)
    source = (paths.workspace / "candidate.cu").read_bytes()
    if sha(source) != SEED_SHA:
        raise RuntimeError("FORMAL_C0000_SOURCE_SHA_MISMATCH")
    candidate_rel = Path("episodes") / episode_id / "archive" / "candidates" / ("C0000-" + SEED_SHA.lower() + ".cu")
    candidate_abs = store.root / candidate_rel
    candidate_abs.parent.mkdir(parents=True, exist_ok=True)
    if candidate_abs.exists():
        if candidate_abs.read_bytes() != source:
            raise RuntimeError("FORMAL_C0000_ARCHIVE_SOURCE_CONFLICT")
    else:
        with candidate_abs.open("xb") as out:
            out.write(source); out.flush(); os.fsync(out.fileno())
    identity = canonical_bytes({"episode_id": episode_id, "candidate_id": "C0000", "seed_sha256": SEED_SHA,
                                "protocol_sha256": protocol_sha})
    request_id = "N0-OJ-FORMAL-C0000-" + hashlib.sha256(identity).hexdigest()[:24]
    invocation = "FORMAL_INIT_C0000"
    activation_path = ROOT / "state" / "n0_formal_agents" / "N0_FORMAL_PROTOCOL_V1_2_ACTIVATION.json"
    if activation_path.exists():
        prior_activation = load_json(activation_path)
        if (prior_activation.get("protocol_sha256") != sha(protocol_raw)
                or prior_activation.get("integration_release_id") != INTEGRATION.name):
            raise RuntimeError("FORMAL_RECOVERY_ACTIVATION_IDENTITY_CONFLICT")
        prior_recoveries = prior_activation.get("episode_recovery_events", {})
        if prior_recoveries != recovery_data:
            raise RuntimeError("FORMAL_RECOVERY_ACTIVATION_CONTENT_CONFLICT")
        current_states = {x["episode_id"]: x for x in store.snapshot()["episodes"]}
        if all(current_states[eid]["state"] == "RUNNING" for eid in EPISODE_IDS):
            return prior_activation
    db = store._connect()
    try:
        db.execute("BEGIN IMMEDIATE")
        ep = db.execute("SELECT * FROM episodes WHERE episode_id=?", (episode_id,)).fetchone()
        if not ep or ep["candidates_used"] != 0 or ep["last_candidate_sha256"].upper() != SEED_SHA:
            raise RuntimeError("FORMAL_C0000_EPISODE_STATE_INVALID")
        prior = db.execute("SELECT * FROM oj_requests WHERE episode_id=? AND candidate_id='C0000'", (episode_id,)).fetchone()
        if prior:
            if prior["request_id"] != request_id or prior["candidate_sha256"].upper() != SEED_SHA:
                raise RuntimeError("FORMAL_C0000_REQUEST_IDENTITY_CONFLICT")
            result = dict(prior)
            db.execute("COMMIT")
            return result
        if ep["state"] != "ALLOCATED":
            raise RuntimeError("FORMAL_C0000_EPISODE_STATE_INVALID")
        seq = int(db.execute("SELECT value FROM runtime_meta WHERE key='queue_sequence'").fetchone()[0]) + 1
        db.execute("UPDATE runtime_meta SET value=? WHERE key='queue_sequence'", (str(seq),))
        now_ts = time.time()
        db.execute("INSERT INTO oj_requests(request_id,episode_id,invocation_id,candidate_id,candidate_sha256,state,queue_sequence,candidate_source_path,created_at,updated_at) VALUES(?,?,?,?,?,'QUEUED',?,?,?,?)",
                   (request_id, episode_id, invocation, "C0000", SEED_SHA.lower(), seq, candidate_rel.as_posix(), now_ts, now_ts))
        db.execute("UPDATE episodes SET state='WAITING_OJ',updated_at=? WHERE episode_id=? AND state='ALLOCATED'", (now_ts, episode_id))
        store._insert_event_tx(db, episode_id, {"event_type": "FORMAL_C0000_QUEUED", "request_id": request_id,
            "candidate_id": "C0000", "candidate_sha256": SEED_SHA, "queue_sequence": seq,
            "protocol_sha256": protocol_sha, "agent_budget_consumed": False}, "formal-c0000:" + episode_id)
        db.execute("COMMIT")
        result = {"request_id": request_id, "episode_id": episode_id, "candidate_id": "C0000",
                  "candidate_sha256": SEED_SHA, "state": "QUEUED", "queue_sequence": seq}
    except BaseException:
        if db.in_transaction: db.execute("ROLLBACK")
        raise
    finally:
        db.close()
    store.reconcile_episode_exports(episode_id)
    return result


def initialize() -> dict[str, Any]:
    readiness = verify_readiness()
    services = verify_live_services()
    plan_path = ROOT / "allocations" / "N0_FORMAL_ALLOCATION_PLAN_V1.json"
    plan_raw = plan_path.read_bytes()
    plan = json.loads(plan_raw.decode("utf-8"))
    protocol_raw = PROTOCOL_PATH.read_bytes()
    protocol = json.loads(protocol_raw.decode("utf-8"))
    if INIT_PATH.exists():
        init = load_json(INIT_PATH)
        if init.get("state") == "C0000_QUEUED":
            return init
        if (init.get("state") != "PREPARING" or init.get("allocation_plan_sha256") != sha(plan_raw)
                or init.get("protocol_sha256") != sha(protocol_raw)):
            raise RuntimeError("FORMAL_INITIALIZATION_ALREADY_EXISTS_NO_REINITIALIZATION")
        if init.get("services", {}).get("oj_manifest_sha256") != services.get("oj_manifest_sha256"):
            raise RuntimeError("FORMAL_INITIALIZATION_SERVICE_IDENTITY_CHANGED")
    else:
        assert_formal_queues_clear()
        init = None
    if [x["episode_id"] for x in plan["episodes"]] != EPISODE_IDS:
        raise RuntimeError("FORMAL_ALLOCATION_PLAN_IDENTITY_MISMATCH")
    template = PROJECT / "episodes" / "N0_AUTONOMOUS_INFORMATION_ACQUISITION_R4" / "template"
    seed = (HARNESS / "protocol" / "SEED_CANDIDATE.cu").read_bytes()
    if sha(seed) != SEED_SHA:
        raise RuntimeError("FORMAL_SEED_IDENTITY_MISMATCH")
    files = {name: (template / name).read_bytes() for name in
             ("task_definition.json", "candidate_abi.json", "visible_observation.json")}
    files["candidate.cu"] = seed
    abi = json.loads(files["candidate_abi.json"].decode("utf-8"))
    abi.update({"oj_release_id": "N0_KERNELBENCH_L1_40_LAYERNORM_R5_REFERENCE_TRUST",
                "oj_manifest_sha256": services["oj_manifest_sha256"],
                "compiler_contract_sha256": protocol["compiler"]["contract_sha256"]})
    files["candidate_abi.json"] = (json.dumps(abi, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
    if init is None:
        init = {"schema": "N0_FORMAL_INITIALIZATION_V1", "state": "PREPARING",
            "classification": "FORMAL_SCIENTIFIC_BATCH", "episode_ids": EPISODE_IDS,
            "allocation_plan_sha256": sha(plan_raw), "protocol_sha256": sha(protocol_raw),
            "readiness_report_sha256": sha(READINESS_PATH.read_bytes()), "services": services,
            "seed_sha256": SEED_SHA, "candidate_budget": 30, "agent_requests_permitted": False,
            "created_at_utc": now(), "c0000": []}
        atomic(INIT_PATH, init)
    sys.path[:0] = [str(HARNESS), str(R5_INTEGRATION)]
    from n0_autonomous.parallel_runtime import N0PersistentQueue
    store = N0PersistentQueue(QUEUE_ROOT)
    for slot, episode_id in enumerate(EPISODE_IDS, 1):
        allocation = _allocation(episode_id, slot, sha(plan_raw), sha(protocol_raw), init["created_at_utc"])
        allocation_path = ROOT / "allocations" / "FORMAL_ALLOCATION_RECORDS" / (episode_id + ".json")
        if allocation_path.exists() and load_json(allocation_path) != allocation:
            raise RuntimeError("FORMAL_ALLOCATION_RECORD_CONFLICT")
        if not allocation_path.exists():
            atomic(allocation_path, allocation)
        store.register_episode(episode_id=episode_id, replicate_slot=allocation["replicate_slot"],
            treatment_id="N0_AUTONOMOUS", approved_allocation=allocation, max_candidates=30,
            workspace_files=files)
        allocation_copy = store.episode_paths(episode_id).root / "allocation.json"
        if allocation_copy.exists() and load_json(allocation_copy) != allocation:
            raise RuntimeError("FORMAL_EPISODE_ALLOCATION_COPY_CONFLICT")
        if not allocation_copy.exists():
            atomic(allocation_copy, allocation)
        append_stable_event(store, episode_id, {"event_type": "FORMAL_EPISODE_ALLOCATED",
            "allocation_sha256": sha(canonical_json(allocation)), "plan_sha256": sha(plan_raw),
            "protocol_sha256": sha(protocol_raw), "harness_release_id": HARNESS.name,
            "agent_requests_permitted": False}, event_id="formal-allocation:" + episode_id)
    queued = []
    for slot, episode_id in enumerate(EPISODE_IDS, 1):
        queued.append(_queue_c0000(store, episode_id, slot, sha(protocol_raw)))
    init["state"] = "C0000_QUEUED"
    init["c0000"] = queued
    init["agent_requests_permitted"] = False
    init["queued_at_utc"] = now()
    atomic(INIT_PATH, init)
    return init


def canonical_json(value: Any) -> bytes:
    from n0_autonomous.engine import canonical_bytes
    return canonical_bytes(value)


def formal_candidates(store, episode_id: str) -> list[dict[str, Any]]:
    from n0_autonomous.engine import canonical_bytes, sha256_bytes
    from n0_integration.history_bridge import candidate_record_from_oj_row
    snap = store.snapshot()
    all_rows = [r for r in snap["oj_requests"] if r["episode_id"] == episode_id and r["state"] == "CONSUMED"]
    latest_by_candidate = {}
    for item in all_rows:
        prior = latest_by_candidate.get(item["candidate_id"])
        if prior is None or (item.get("created_at", 0), item.get("queue_sequence", 0)) > (prior.get("created_at", 0), prior.get("queue_sequence", 0)):
            latest_by_candidate[item["candidate_id"]] = item
    rows = sorted(latest_by_candidate.values(), key=lambda r: int(r["candidate_id"][1:]))
    records: list[dict[str, Any]] = []
    best: float | None = None
    best_measurement: dict[str, Any] | None = None

    def timing_identity(record):
        receipt = record.get("oj_receipt") or {}
        return {"baseline_id": receipt.get("baseline_id"),
                "score_version": receipt.get("score_version"),
                "candidate_runtime_ms": receipt.get("candidate_runtime_ms"),
                "candidate_sha256": record.get("source_sha256")}

    from n0_integration.online_best import classify_online_best
    for row in rows:
        base = candidate_record_from_oj_row(store, row)
        score = base["ledger_entry"]["score"]
        raw_verdict = base["oj_receipt"].get("verdict")
        if base["candidate_id"] == "C0000":
            if raw_verdict == "OJ_INFRASTRUCTURE_HOLD_REFERENCE_CONTROL_MISMATCH" and score is None:
                promotion = "BASELINE_SCORE_UNAVAILABLE"
                base["ledger_entry"]["initialization_status"] = "C0000_BASELINE_SCORE_UNAVAILABLE_INFRA_HOLD"
                base["ledger_entry"]["evaluation_status"] = "C0000_BASELINE_SCORE_UNAVAILABLE_INFRA_HOLD"
            else:
                promotion = "BASELINE"
                base["ledger_entry"]["evaluation_status"] = raw_verdict
            if score is not None:
                best = score
                best_measurement = timing_identity(base)
        elif score is not None:
            measured = timing_identity(base)
            if best is not None and best_measurement is None:
                promotion, reason = "INCOMPARABLE_BASELINE", "PRIOR_BEST_IDENTITY_MISSING"
            elif (best_measurement is not None and measured.get("baseline_id") != best_measurement.get("baseline_id")):
                promotion, reason = "INCOMPARABLE_BASELINE", "INCOMPARABLE_BASELINE"
            elif (best_measurement is not None and measured.get("score_version") != best_measurement.get("score_version")):
                promotion, reason = "INCOMPARABLE_SCORE_VERSION", "INCOMPARABLE_SCORE_VERSION"
            else:
                decision, reason = classify_online_best(measured, best_measurement)
                if decision == "ONLINE_PROVISIONAL_BEST":
                    promotion = "ONLINE_PROVISIONAL_BEST"
                    confidence_status = "PROVISIONAL"
                    best = score
                    best_measurement = measured
                elif decision == "CONFIRMED_REGRESSION":
                    promotion = "CONFIRMED_REGRESSION"
                    confidence_status = "CONFIRMED_REGRESSION"
                else:
                    promotion = "UNCERTAIN"
                    confidence_status = "UNCERTAIN"
            if promotion in {"INCOMPARABLE_BASELINE", "INCOMPARABLE_SCORE_VERSION"}:
                base["ledger_entry"]["score"] = None
                base["ledger_entry"]["evaluation_status"] = "SCORE_NOT_COMPARABLE_INFRA_HOLD"
            base["ledger_entry"]["promotion_reason"] = reason
        else:
            promotion = "NOT_PROMOTED"
            base["ledger_entry"]["evaluation_status"] = (
                "MEASUREMENT_UNAVAILABLE_INFRA_HOLD"
                if raw_verdict in {"OJ_INFRASTRUCTURE_HOLD_REFERENCE_CONTROL_MISMATCH", "OJ_INFRASTRUCTURE_HOLD_REFERENCE_BASELINE_DRIFT", "OJ_INFRASTRUCTURE_HOLD_PROFILE_UNAVAILABLE", "OJ_INFRASTRUCTURE_HOLD_RAW_TIMER_EVIDENCE_INVALID", "OJ_INFRASTRUCTURE_HOLD_CANDIDATE_TIMING_UNSTABLE"} and score is None
                else raw_verdict)
        ledger = {k: v for k, v in base["ledger_entry"].items() if k != "ledger_entry_sha256"}
        ledger["promotion"] = promotion
        ledger["best_status"] = ("PROVISIONAL" if promotion == "ONLINE_PROVISIONAL_BEST"
                                 else "CURRENT" if promotion == "BASELINE" else "UNCHANGED")
        if base["candidate_id"] == "C0000":
            confidence_status = "UNCERTAIN"
        elif score is None:
            confidence_status = "UNCERTAIN"
        ledger["promotion_confidence_status"] = confidence_status
        if promotion in {"NOT_PROMOTED_UNCERTAIN", "NOT_PROMOTED", "UNCERTAIN", "CONFIRMED_REGRESSION"} and "promotion_reason" not in ledger:
            ledger["promotion_reason"] = "SCORE_NOT_GREATER_OR_UNSCORED"
        ledger["ledger_entry_sha256"] = sha256_bytes(canonical_bytes(ledger))
        base["ledger_entry"] = ledger
        records.append(base)
    return records


def _write_observation(store, episode_id: str, row: dict[str, Any], receipt: dict[str, Any],
                       promotion: str, protocol_sha: str,
                       confidence_status: str | None = None,
                       profile_receipt: dict[str, Any] | None = None) -> dict[str, Any]:
    paths = store.episode_paths(episode_id)
    old_path = paths.workspace / "visible_observation.json"
    settlement_id = "formal-settled:" + str(row["request_id"])
    event_row = None
    if hasattr(store, "_connect"):
        db = store._connect()
        try:
            event_row = db.execute("SELECT event_json FROM episode_events WHERE event_id=?", (settlement_id,)).fetchone()
        finally:
            db.close()
    if event_row:
        prior_event = json.loads(event_row[0])
        expected_sha = prior_event.get("observation_sha256")
        candidates = [old_path] + sorted((paths.archive / "observations").glob("visible_observation.*.json"))
        for candidate_path in candidates:
            if candidate_path.is_file() and sha(candidate_path.read_bytes()) == expected_sha:
                prior_observation = json.loads(candidate_path.read_text(encoding="utf-8"))
                if (prior_observation.get("oj_request_id") != row["request_id"]
                        or prior_observation.get("oj_receipt_sha256") != row.get("receipt_sha256")
                        or prior_observation.get("candidate_sha256") != row.get("candidate_sha256")):
                    raise RuntimeError("FORMAL_SETTLEMENT_OBSERVATION_IDENTITY_MISMATCH")
                return prior_observation
        raise RuntimeError("FORMAL_SETTLEMENT_OBSERVATION_ARCHIVE_MISSING")
    old_sha = sha(old_path.read_bytes()) if old_path.exists() else None
    reference_measurement_hold = receipt.get("verdict") == "OJ_INFRASTRUCTURE_HOLD_REFERENCE_CONTROL_MISMATCH" and receipt.get("score") is None
    profile_measurement_hold = receipt.get("verdict") == "OJ_INFRASTRUCTURE_HOLD_PROFILE_UNAVAILABLE" and receipt.get("score") is None
    raw_timer_measurement_hold = receipt.get("verdict") == "OJ_INFRASTRUCTURE_HOLD_RAW_TIMER_EVIDENCE_INVALID" and receipt.get("score") is None
    frozen_baseline_measurement_hold = receipt.get("verdict") == "OJ_INFRASTRUCTURE_HOLD_REFERENCE_BASELINE_DRIFT" and receipt.get("score") is None
    score_identity_measurement_hold = receipt.get("verdict") in {"OJ_INFRASTRUCTURE_HOLD_SCORE_UNCERTAINTY_INVALID", "OJ_INFRASTRUCTURE_HOLD_BASELINE_MISMATCH", "OJ_INFRASTRUCTURE_HOLD_SCORE_VERSION_MISMATCH"} and receipt.get("score") is None
    score_comparability_hold = promotion in {"INCOMPARABLE_BASELINE", "INCOMPARABLE_SCORE_VERSION"}
    generic_infrastructure_hold = str(receipt.get("verdict", "")).startswith("OJ_INFRASTRUCTURE_HOLD") and receipt.get("score") is None
    non_scoring_measurement_hold = (generic_infrastructure_hold or reference_measurement_hold or profile_measurement_hold or
                                    raw_timer_measurement_hold or frozen_baseline_measurement_hold or
                                    score_identity_measurement_hold or score_comparability_hold)
    if non_scoring_measurement_hold:
        visible_status = "MEASUREMENT_UNAVAILABLE_INFRA_HOLD"
        visible_receipt = {"request_id":receipt.get("request_id"), "episode_id":episode_id,
            "candidate_id":row["candidate_id"], "candidate_sha256":row["candidate_sha256"],
            "task_sha256":receipt.get("task_sha256"), "verdict":visible_status, "score":None,
            "terminal":True, "classification":"CONTROLLER_ONLY_INFRASTRUCTURE_HOLD",
            "message":"Controller hold. No performance result is available; this observation must not be sent to Agent."}
    else:
        visible_status = receipt.get("evaluation_status", receipt["verdict"])
        visible_receipt = dict(receipt)
    if profile_receipt is not None and not non_scoring_measurement_hold:
        visible_receipt["profile"] = profile_receipt.get("profile") or {
            "status": profile_receipt.get("status"), "reason": profile_receipt.get("recovery_failure")}
        visible_receipt["profile_recovery_status"] = profile_receipt.get("recovery_status")
        visible_receipt["profile_receipt_sha256"] = profile_receipt.get("profile_receipt_sha256")
        visible_receipt["profile_original_recovery_status"] = profile_receipt.get("profile_original_recovery_status",
            profile_receipt.get("recovery_status"))
        visible_receipt["profile_recovery_receipt"] = profile_receipt.get("recovery_receipt_projection")
        visible_receipt["profile_source_oj_receipt_sha256"] = receipt.get("receipt_sha256")
        visible_receipt["profile_projection"] = "SEPARATE_IMMUTABLE_POST_RECEIPT_PROFILE_V1"
    observation = {"schema": "N0_VISIBLE_EVALUATION_OBSERVATION_V1", "episode_id": episode_id,
        "candidate_id": row["candidate_id"], "candidate_sha256": row["candidate_sha256"],
        "oj_request_id": row["request_id"], "oj_receipt_sha256": row["receipt_sha256"],
        "verdict": visible_status, "score": None if non_scoring_measurement_hold else receipt.get("score"),
        "candidate_runtime_ms": None if non_scoring_measurement_hold else receipt.get("candidate_runtime_ms"),
        "baseline_id": None if non_scoring_measurement_hold else receipt.get("baseline_id"),
        "score_version": None if non_scoring_measurement_hold else receipt.get("score_version"),
        "candidate_state": visible_status if non_scoring_measurement_hold else receipt.get("verdict"),
        "promotion": False if non_scoring_measurement_hold else promotion,
        "promotion_reason": "INFRASTRUCTURE_HOLD_NO_PROMOTION" if non_scoring_measurement_hold else None,
        "best_status": "HOLD" if non_scoring_measurement_hold else ("PROVISIONAL" if promotion == "ONLINE_PROVISIONAL_BEST" else "UNCHANGED"),
        "promotion_confidence_status": "HOLD" if non_scoring_measurement_hold else (confidence_status or receipt.get("promotion_confidence_status", "UNCERTAIN")),
        "evaluation_status": visible_status,
        "receipt": visible_receipt, "post_receipt_profile": profile_receipt,
        "protocol_sha256": protocol_sha,
        "classification": ("CONTROLLER_ONLY_INFRASTRUCTURE_HOLD" if non_scoring_measurement_hold else
            ("NON_SCIENTIFIC_GPU_DIAGNOSTIC" if episode_id.startswith("N0DIAG-") else "FORMAL_SCIENTIFIC_EPISODE"))}
    history = [] if non_scoring_measurement_hold else formal_candidates(store, episode_id)
    settled = next((item for item in reversed(history)
        if item.get("candidate_id") == row.get("candidate_id")
        and item.get("oj_receipt", {}).get("request_id") == row.get("request_id")), None)
    if settled is not None:
        observation["promotion_reason"] = settled.get("ledger_entry", {}).get("promotion_reason")
    best_candidates = [item for item in history if item["ledger_entry"].get("promotion") in {"BASELINE", "ONLINE_PROVISIONAL_BEST"}]
    if best_candidates:
        best_record = best_candidates[-1]
        best_receipt = best_record["oj_receipt"]
        observation["current_best"] = {"candidate_id":best_record["candidate_id"],
            "candidate_sha256":best_record["source_sha256"],
            "candidate_runtime_ms":best_receipt.get("candidate_runtime_ms"),
            "score":best_receipt.get("score"), "baseline_id":best_receipt.get("baseline_id"),
            "score_version":best_receipt.get("score_version"),
            "status":"ONLINE_PROVISIONAL" if best_record["ledger_entry"].get("promotion") == "ONLINE_PROVISIONAL_BEST" else "ONLINE_BASELINE"}
    else:
        observation["current_best"] = {"candidate_id":None,"status":"UNAVAILABLE"}
    raw = (json.dumps(observation, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode("utf-8")
    if old_path.exists() and json.loads(old_path.read_text(encoding="utf-8")).get("oj_receipt_sha256") == row["receipt_sha256"]:
        if old_path.read_bytes() != raw:
            raise RuntimeError("FORMAL_OBSERVATION_IDEMPOTENCY_CONFLICT")
    else:
        if old_path.exists():
            prior_archive = paths.archive / "observations" / ("visible_observation." + str(old_sha).lower() + ".json")
            prior_archive.parent.mkdir(parents=True, exist_ok=True)
            prior_raw = old_path.read_bytes()
            if prior_archive.exists():
                if prior_archive.read_bytes() != prior_raw:
                    raise RuntimeError("OBSERVATION_VERSION_ARCHIVE_CONFLICT")
            else:
                with prior_archive.open("xb") as saved:
                    saved.write(prior_raw); saved.flush(); os.fsync(saved.fileno())
        atomic(old_path, observation)
    append_stable_event(store, episode_id, {"event_type": "FORMAL_CANDIDATE_SETTLED",
        "request_id": row["request_id"], "candidate_id": row["candidate_id"],
        "candidate_sha256": row["candidate_sha256"], "receipt_sha256": row["receipt_sha256"],
        "verdict": receipt["verdict"], "evaluation_status": visible_status,
        "score": receipt.get("score"), "promotion": promotion,
        "promotion_confidence_status": confidence_status or receipt.get("promotion_confidence_status", "UNCERTAIN"),
        "observation_sha256": sha(raw)},
        event_id="formal-settled:" + row["request_id"])
    return observation


def _wait_post_receipt_profile(row: dict[str, Any], receipt: dict[str, Any], *, timeout_seconds: float = 660.0) -> dict[str, Any] | None:
    """Wait for the separately hashed Profile and, when needed, its recovery Receipt."""
    profile = receipt.get("profile") or {}
    if profile.get("status") != "QUEUED":
        return None
    path = PROFILE_STATE_ROOT / "profile_receipts" / (str(row["request_id"]) + ".json")
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if path.is_file():
            from n0_autonomous.engine import canonical_bytes
            result = json.loads(path.read_text(encoding="utf-8"))
            body = {k: v for k, v in result.items() if k != "profile_receipt_sha256"}
            if (result.get("request_id") != row.get("request_id")
                    or result.get("candidate_sha256", "").upper() != row.get("candidate_sha256", "").upper()
                    or result.get("oj_receipt_sha256") != row.get("receipt_sha256")
                    or sha(canonical_bytes(body)).lower() != str(result.get("profile_receipt_sha256", "")).lower()):
                raise RuntimeError("POST_RECEIPT_PROFILE_RECEIPT_IDENTITY_OR_HASH_MISMATCH")
            if result.get("recovery_status") == "PASS":
                result["recovery_receipt_projection"] = None
                return result
            recovery_root = PROFILE_STATE_ROOT / "profile_recovery_receipts"
            if recovery_root.is_dir():
                for recovery_path in sorted(recovery_root.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True):
                    recovery = json.loads(recovery_path.read_text(encoding="utf-8"))
                    recovery_body = {k: v for k, v in recovery.items() if k != "recovery_receipt_sha256"}
                    if (recovery.get("parent_profile_receipt_id") != row["request_id"]
                            or recovery.get("parent_profile_receipt_sha256") != result.get("profile_receipt_sha256")):
                        continue
                    if sha(canonical_bytes(recovery_body)).lower() != str(recovery.get("recovery_receipt_sha256", "")).lower():
                        raise RuntimeError("POST_RECEIPT_PROFILE_RECOVERY_RECEIPT_HASH_MISMATCH")
                    if recovery.get("status") == "PASS":
                        # Keep the immutable Profile document digest intact; this is a
                        # verified Harness projection of the separate recovery receipt.
                        result["profile_original_recovery_status"] = result.get("recovery_status")
                        result["recovery_status"] = "PASS"
                        result["recovery_receipt_projection"] = {
                            "recovery_id": recovery.get("recovery_id"),
                            "recovery_receipt_sha256": recovery.get("recovery_receipt_sha256"),
                            "status": recovery.get("status"),
                        }
                        return result
            time.sleep(0.25)
            continue
        time.sleep(0.25)
    raise TimeoutError("POST_RECEIPT_PROFILE_RESULT_NOT_READY_OR_WORKER_RECOVERY_REQUIRED")


def write_episode_summary(store, episode_id: str) -> dict[str, Any]:
    snap = store.snapshot()
    ep = next(x for x in snap["episodes"] if x["episode_id"] == episode_id)
    records = formal_candidates(store, episode_id)
    best = None
    for record in records:
        score = record["ledger_entry"].get("score")
        promotion = record["ledger_entry"].get("promotion")
        if promotion in {"BASELINE", "PROMOTED"} and score is not None and (best is None or score > best["score"]):
            best = {"candidate_id": record["candidate_id"], "source_sha256": record["source_sha256"], "score": score}
    summary = {"schema": "N0_FORMAL_EPISODE_SUMMARY_V1", "episode_id": episode_id,
        "treatment_id": ep["treatment_id"], "replicate_slot": ep["replicate_slot"],
        "episode_state": ep["state"], "candidate_budget": {"used": ep["candidates_used"],
            "max": ep["max_candidates"], "remaining": ep["max_candidates"] - ep["candidates_used"]},
        "current_source_sha256": ep["last_candidate_sha256"], "best_candidate": best,
        "settled_candidates": [{"candidate_id": r["candidate_id"], "source_sha256": r["source_sha256"],
            "verdict": r["ledger_entry"].get("evaluation_status", r["oj_receipt"].get("verdict")),
            "raw_oj_verdict": r["oj_receipt"].get("verdict"), "score": r["ledger_entry"].get("score"),
            "promotion": r["ledger_entry"].get("promotion"), "request_id": r["oj_receipt"].get("request_id"),
            "promotion_confidence_status": r["ledger_entry"].get("promotion_confidence_status", "UNCERTAIN"),
            "receipt_sha256": r["oj_receipt"].get("receipt_sha256")} for r in records],
        "strict_validity": "NOT_ADJUDICATED_UNTIL_PROTOCOL_TERMINAL_SETTLEMENT", "updated_at_utc": now()}
    atomic(store.episode_paths(episode_id).summary, summary)
    return summary


def _eligible_reference_control_hold(receipt: dict[str, Any], *, candidate_id: str) -> bool:
    trust = (receipt.get("measurement_evidence") or {}).get("reference_trust") or {}
    return (isinstance(candidate_id, str) and candidate_id.startswith("C")
            and receipt.get("verdict") == "OJ_INFRASTRUCTURE_HOLD_REFERENCE_CONTROL_MISMATCH"
            and receipt.get("score") is None and trust.get("pass") is False
            and trust.get("limit") == 0.02 and trust.get("policy") == "N0_POST_EVALUATOR_REFERENCE_PAIRED_V2"
            and isinstance(trust.get("relative_difference"), (int, float))
            and trust.get("relative_difference") > 0.02
            and (candidate_id == "C0000" or candidate_id.startswith("C")))


def _eligible_profile_only_hold(receipt: dict[str, Any], *, candidate_id: str) -> bool:
    if (not isinstance(candidate_id, str) or not candidate_id.startswith("C")
            or receipt.get("verdict") != "OJ_INFRASTRUCTURE_HOLD_PROFILE_UNAVAILABLE"
            or receipt.get("score") is not None):
        return False
    profile = receipt.get("profile") or {}
    evidence = receipt.get("measurement_evidence") or {}
    trust = evidence.get("reference_trust") or {}
    official = receipt.get("official_kernelbench_result") or {}
    timing = receipt.get("timing") or {}
    samples = timing.get("samples_ms") or (evidence.get("candidate") or {}).get("raw_samples_ms")
    return (profile.get("status") == "HOLD"
            and profile.get("identity_validation") == "PASS"
            and trust.get("pass") is True and trust.get("limit") == 0.02
            and trust.get("policy") == "N0_POST_EVALUATOR_REFERENCE_PAIRED_V2"
            and official.get("correctness") is True
            and isinstance(timing.get("candidate_runtime_ms"), (int, float)) and timing.get("candidate_runtime_ms", 0) > 0
            and isinstance(timing.get("kernelbench_reference_runtime_ms"), (int, float)) and timing.get("kernelbench_reference_runtime_ms", 0) > 0
            and isinstance(samples, list) and bool(samples))


def _eligible_raw_timer_evidence_hold(receipt: dict[str, Any], *, candidate_id: str) -> bool:
    diagnostics = receipt.get("diagnostics") or {}
    official = diagnostics.get("official_evaluator") or {}
    return (isinstance(candidate_id, str) and candidate_id.startswith("C") and candidate_id != "C0000"
            and receipt.get("verdict") == "OJ_INFRASTRUCTURE_HOLD_RAW_TIMER_EVIDENCE_INVALID"
            and receipt.get("score") is None and receipt.get("terminal") is True
            and official.get("compiled") is True and official.get("correctness") is True
            and diagnostics.get("status") == "OJ_INFRASTRUCTURE_HOLD_RAW_TIMER_EVIDENCE_INVALID")


def _validate_c0000_receipt(row: dict[str, Any], receipt: dict[str, Any], episode_id: str,
                            profile_receipt: dict[str, Any] | None = None) -> str:
    from n0_autonomous.engine import canonical_bytes
    if (receipt.get("request_id") != row["request_id"] or receipt.get("episode_id") != episode_id
            or receipt.get("candidate_id") != "C0000" or receipt.get("candidate_sha256", "").upper() != SEED_SHA
            or receipt.get("task_sha256") != "8D975D7CEE6946513DCFB6BA84C971E53A890F9D9C6F0A4FB828904941F25975"
            or receipt.get("terminal") is not True
            or sha(canonical_bytes(receipt)).lower() != str(row["receipt_sha256"]).lower()
            or sha(canonical_bytes({k: v for k, v in receipt.items() if k != "receipt_sha256"})).lower()
               != str(receipt.get("receipt_sha256", "")).lower()):
        raise RuntimeError("FORMAL_C0000_RECEIPT_IDENTITY_OR_HASH_MISMATCH:" + episode_id)
    verdict = receipt.get("verdict")
    if verdict == "VALID_MEASUREMENT":
        profile_status = (profile_receipt or {}).get("status") if (receipt.get("profile") or {}).get("status") == "QUEUED" else (receipt.get("profile") or {}).get("status")
        recovery_status = (profile_receipt or {}).get("recovery_status") if profile_receipt else "PASS"
        if (not isinstance(receipt.get("score"), (int, float)) or isinstance(receipt.get("score"), bool)
                or profile_status not in {"PASS", "UNAVAILABLE"} or recovery_status != "PASS"
                or (receipt.get("measurement_evidence", {}).get("reference_trust") or {}).get("pass") is not True):
            raise RuntimeError("FORMAL_C0000_VALID_RECEIPT_NOT_TRUSTED:" + episode_id)
        return "C0000_BASELINE_VALID_MEASUREMENT"
    if _eligible_reference_control_hold(receipt, candidate_id="C0000"):
        return "C0000_BASELINE_SCORE_UNAVAILABLE_INFRA_HOLD"
    raise RuntimeError("FORMAL_C0000_RECEIPT_NOT_INITIALIZATION_ELIGIBLE:" + episode_id + ":" + str(verdict))


def _commit_c0000_consumption_batch(store, outcomes: list[dict[str, Any]]) -> None:
    """Consume the four already-terminal C0000 receipts in one SQLite transaction."""
    db = store._connect()
    try:
        db.execute("BEGIN IMMEDIATE")
        for item in outcomes:
            row = db.execute("SELECT * FROM oj_requests WHERE request_id=?", (item["request_id"],)).fetchone()
            if not row or row["episode_id"] != item["episode_id"] or row["candidate_id"] != "C0000":
                raise RuntimeError("FORMAL_C0000_BATCH_ROW_IDENTITY_CHANGED")
            if row["state"] == "COMPLETED" and row["receipt_consumed_at"] is None:
                receipt = json.loads(row["receipt_json"])
                event = {"event_type": "OJ_RESULT_CONSUMED", "request_id": row["request_id"],
                    "candidate_id": row["candidate_id"], "candidate_sha256": row["candidate_sha256"],
                    "receipt_sha256": row["receipt_sha256"], "receipt": receipt}
                event_id = "oj-result-consumed:" + row["request_id"]
                prior = db.execute("SELECT event_json FROM episode_events WHERE event_id=?", (event_id,)).fetchone()
                if prior:
                    old = json.loads(prior[0])
                    comparable = {k: v for k, v in old.items() if k not in {"event_id", "episode_id", "created_at"}}
                    if old.get("episode_id") != row["episode_id"] or comparable != event:
                        raise RuntimeError("FORMAL_C0000_CONSUMPTION_EVENT_CONFLICT")
                else:
                    store._insert_event_tx(db, row["episode_id"], event, event_id)
                changed = db.execute("UPDATE oj_requests SET state='CONSUMED',receipt_consumed_at=?,updated_at=? WHERE request_id=? AND state='COMPLETED' AND receipt_consumed_at IS NULL",
                    (time.time(), time.time(), row["request_id"]))
                if changed.rowcount != 1:
                    raise RuntimeError("FORMAL_C0000_CONSUMPTION_RACE")
            elif row["state"] == "CONSUMED" and row["receipt_consumed_at"] is not None:
                receipt = json.loads(row["receipt_json"])
                event = db.execute("SELECT event_json FROM episode_events WHERE event_id=?",
                                   ("oj-result-consumed:" + row["request_id"],)).fetchone()
                if not event:
                    raise RuntimeError("FORMAL_C0000_CONSUMED_WITHOUT_AUDIT_EVENT")
                old = json.loads(event[0])
                comparable = {k: v for k, v in old.items() if k not in {"event_id", "episode_id", "created_at"}}
                expected = {"event_type": "OJ_RESULT_CONSUMED", "request_id": row["request_id"],
                    "candidate_id": row["candidate_id"], "candidate_sha256": row["candidate_sha256"],
                    "receipt_sha256": row["receipt_sha256"], "receipt": receipt}
                if old.get("episode_id") != row["episode_id"] or comparable != expected:
                    raise RuntimeError("FORMAL_C0000_CONSUMED_AUDIT_IDENTITY_MISMATCH")
            else:
                raise RuntimeError("FORMAL_C0000_BATCH_NOT_TERMINAL_OR_CONSUMABLE")
            db.execute("UPDATE episodes SET state='RUNNING',updated_at=? WHERE episode_id=? AND state IN ('INFRASTRUCTURE_HOLD','RUNNING','WAITING_OJ')",
                       (time.time(), item["episode_id"]))
            reopen_event = {"event_type": "FORMAL_EPISODE_INITIALIZATION_REOPENED_V1_1",
                "protocol_sha256": item["protocol_sha256"], "candidate_id": "C0000",
                "request_id": row["request_id"], "receipt_sha256": row["receipt_sha256"],
                "initialization_status": item["initialization_status"], "agent_requests_permitted": False}
            reopen_id = "formal-init-reopen-v1-1:" + item["episode_id"]
            prior = db.execute("SELECT event_json FROM episode_events WHERE event_id=?", (reopen_id,)).fetchone()
            if prior:
                old = json.loads(prior[0])
                comparable = {k: v for k, v in old.items() if k not in {"event_id", "episode_id", "created_at"}}
                if old.get("episode_id") != item["episode_id"] or comparable != reopen_event:
                    raise RuntimeError("FORMAL_EPISODE_REOPEN_EVENT_CONFLICT")
            else:
                store._insert_event_tx(db, item["episode_id"], reopen_event, reopen_id)
        db.execute("COMMIT")
    except BaseException:
        if db.in_transaction:
            db.execute("ROLLBACK")
        raise
    finally:
        db.close()
    for item in outcomes:
        store.reconcile_episode_exports(item["episode_id"])


def settle_initialization() -> dict[str, Any]:
    if not INIT_PATH.is_file():
        raise RuntimeError("FORMAL_INITIALIZATION_NOT_CREATED")
    init = load_json(INIT_PATH)
    if init.get("state") == "FORMAL_INITIALIZED" and init.get("agent_requests_permitted") is True:
        return init
    if (init.get("state") == "INITIALIZATION_HOLD" and init.get("agent_requests_permitted") is False
            and init.get("controller_hold_reason")):
        return init
    if init.get("state") not in {"INITIALIZATION_HOLD", "INITIALIZATION_COMMITTING", "C0000_QUEUED"} or init.get("agent_requests_permitted") is not False:
        raise RuntimeError("FORMAL_INITIALIZATION_STATE_INVALID")
    protocol_raw = PROTOCOL_PATH.read_bytes()
    protocol_sha = sha(protocol_raw)
    protocol = json.loads(protocol_raw.decode("utf-8"))
    if protocol.get("schema") != "N0_FORMAL_PROTOCOL_V1_2" or protocol.get("parent_protocol", {}).get("protocol_id") != "N0_FORMAL_PROTOCOL_V1_1" or protocol.get("parent_protocol", {}).get("sha256") != "DDBAADCD4AC2D4A67FA4E6A32F8EB7B4777F6BA69F413C741C0AD0004BE51CDB":
        raise RuntimeError("FORMAL_PROTOCOL_V1_2_PARENT_OR_IDENTITY_INVALID")
    if init.get("protocol_sha256") not in {protocol["parent_protocol"]["sha256"], protocol_sha}:
        raise RuntimeError("FORMAL_INITIALIZATION_PROTOCOL_LINEAGE_CONFLICT")
    if init.get("episode_ids") != EPISODE_IDS or init.get("seed_sha256", "").upper() != SEED_SHA or init.get("candidate_budget") != 30:
        raise RuntimeError("FORMAL_INITIALIZATION_BATCH_IDENTITY_INVALID")
    sys.path[:0] = [str(HARNESS), str(R5_INTEGRATION)]
    from n0_autonomous.parallel_runtime import N0PersistentQueue
    store = N0PersistentQueue(QUEUE_ROOT)
    snap = store.snapshot()
    if len(snap["provider_requests"]) != 0:
        raise RuntimeError("FORMAL_PROVIDER_REQUEST_ALREADY_EXISTS_BEFORE_INITIALIZATION_BARRIER")
    if len(snap["episodes"]) != 4 or {x["episode_id"] for x in snap["episodes"]} != set(EPISODE_IDS):
        raise RuntimeError("FORMAL_EPISODE_ALLOCATION_SET_MISMATCH")
    if len(snap["oj_requests"]) != 4 or any(x["candidate_id"] != "C0000" for x in snap["oj_requests"]):
        raise RuntimeError("FORMAL_OJ_REQUEST_SET_NOT_EXACTLY_FOUR_C0000")
    outcomes = []
    for episode_id in EPISODE_IDS:
        rows = [r for r in snap["oj_requests"] if r["episode_id"] == episode_id and r["candidate_id"] == "C0000"]
        if len(rows) != 1:
            raise RuntimeError("FORMAL_C0000_REQUEST_NOT_UNIQUE:" + episode_id)
        row = rows[0]
        if row["state"] in {"QUEUED", "RUNNING"}:
            init["last_checked_at_utc"] = now()
            init["pending"] = [{"episode_id": episode_id, "request_id": row["request_id"], "state": row["state"]}]
            atomic(INIT_PATH, init)
            return init
        if row["state"] not in {"COMPLETED", "CONSUMED"} or not row.get("receipt_json") or not row.get("receipt_sha256"):
            raise RuntimeError("FORMAL_C0000_QUEUE_OR_RECEIPT_NOT_SETTLED:" + episode_id)
        receipt = json.loads(row["receipt_json"])
        profile_receipt = _wait_post_receipt_profile(row, receipt)
        initialization_status = _validate_c0000_receipt(row, receipt, episode_id, profile_receipt)
        ws_source = store.episode_paths(episode_id).workspace.joinpath("candidate.cu").read_bytes()
        archived = store.root / Path(row["candidate_source_path"])
        if sha(ws_source).upper() != SEED_SHA or not archived.is_file() or sha(archived.read_bytes()).upper() != SEED_SHA:
            raise RuntimeError("FORMAL_C0000_SEED_SOURCE_IDENTITY_MISMATCH:" + episode_id)
        ep = next(x for x in snap["episodes"] if x["episode_id"] == episode_id)
        if ep["candidates_used"] != 0 or ep["max_candidates"] != 30 or ep["last_candidate_sha256"].upper() != SEED_SHA:
            raise RuntimeError("FORMAL_C0000_BUDGET_OR_WORKSPACE_IDENTITY_MISMATCH:" + episode_id)
        outcomes.append({"episode_id": episode_id, "request_id": row["request_id"], "state": row["state"],
            "receipt_sha256": row["receipt_sha256"], "receipt_json": row["receipt_json"], "row": row,
            "receipt": receipt, "profile_receipt": profile_receipt,
            "initialization_status": initialization_status, "protocol_sha256": protocol_sha})
    if len({x["row"]["candidate_sha256"].upper() for x in outcomes}) != 1:
        raise RuntimeError("FORMAL_INITIAL_SEED_IDENTITY_OR_COUNT_MISMATCH")
    if init.get("state") != "INITIALIZATION_COMMITTING":
        init["pre_amendment_protocol_sha256"] = init.get("protocol_sha256")
        init["protocol_sha256"] = protocol_sha
        init["protocol_amendment"] = {"protocol_id": "N0_FORMAL_PROTOCOL_V1_1",
            "parent_protocol_sha256": protocol["parent_protocol"]["sha256"],
            "amendment_approval_path": "D:/projects/kernelagent/runtime/protocol/N0_FORMAL_PROTOCOL_V1_1_APPROVAL.json",
            "approval_sha256": sha((ROOT / "protocol" / "N0_FORMAL_PROTOCOL_V1_1_APPROVAL.json").read_bytes()),
            "integration_release_id": INTEGRATION.name}
        init["state"] = "INITIALIZATION_COMMITTING"
        init["agent_requests_permitted"] = False
        init["settlement_started_at_utc"] = now()
        init["c0000"] = [{"episode_id": x["episode_id"], "request_id": x["request_id"],
            "candidate_id": "C0000", "candidate_sha256": SEED_SHA, "state": x["state"],
            "raw_verdict": x["receipt"].get("verdict"), "initialization_status": x["initialization_status"],
            "score": x["receipt"].get("score"), "receipt_sha256": x["receipt_sha256"]} for x in outcomes]
        atomic(INIT_PATH, init)
    elif init.get("protocol_sha256") != protocol_sha:
        raise RuntimeError("FORMAL_INITIALIZATION_COMMIT_PROTOCOL_HASH_CONFLICT")
    _commit_c0000_consumption_batch(store, [{**x, "protocol_sha256": protocol_sha} for x in outcomes])
    settled = []
    for item in outcomes:
        row = next(r for r in store.snapshot()["oj_requests"] if r["request_id"] == item["request_id"])
        receipt = json.loads(row["receipt_json"])
        promotion = "BASELINE" if item["initialization_status"] == "C0000_BASELINE_VALID_MEASUREMENT" else "BASELINE_SCORE_UNAVAILABLE"
        _write_observation(store, item["episode_id"], row, receipt, promotion, protocol_sha,
            profile_receipt=item.get("profile_receipt"))
        records = formal_candidates(store, item["episode_id"])
        brief_validation = {"episode_id": item["episode_id"], "task_id": "KB_L1_40_LAYERNORM",
            "workspace_files": {name: (store.episode_paths(item["episode_id"]).workspace / name).read_text(encoding="utf-8")
                for name in ("task_definition.json", "candidate_abi.json", "candidate.cu", "visible_observation.json")},
            "candidates": records, "current_candidate_id": "C0001", "unsettled_requests": [], "receipt_conflicts": []}
        from n0_autonomous.engine import build_history_brief
        build_history_brief(brief_validation, candidate_budget=30)
        append_stable_event(store, item["episode_id"], {"event_type": "FORMAL_C0000_INITIAL_HISTORY_VALIDATED_V1_1",
            "candidate_id": "C0000", "candidate_sha256": SEED_SHA, "request_id": row["request_id"],
            "receipt_sha256": row["receipt_sha256"], "raw_verdict": receipt["verdict"],
            "initialization_status": item["initialization_status"], "score": receipt.get("score"),
            "agent_budget_used": 0,
            "observation_sha256": sha((store.episode_paths(item["episode_id"]).workspace / "visible_observation.json").read_bytes())},
            event_id="formal-c0000-history-v1-1:" + item["episode_id"])
        write_episode_summary(store, item["episode_id"])
        settled.append({"episode_id": item["episode_id"], "request_id": row["request_id"], "state": "CONSUMED",
            "candidate_id": "C0000", "candidate_sha256": SEED_SHA, "receipt_sha256": row["receipt_sha256"],
            "raw_verdict": receipt["verdict"], "initialization_status": item["initialization_status"],
            "score": receipt.get("score"), "profile_status": (receipt.get("profile") or {}).get("status"),
            "promotion": promotion, "history_validation": "PASS",
            "observation_sha256": sha((store.episode_paths(item["episode_id"]).workspace / "visible_observation.json").read_bytes())})
    if len(set(x["candidate_sha256"] for x in settled)) != 1 or len(settled) != 4:
        raise RuntimeError("FORMAL_INITIAL_SEED_IDENTITY_OR_COUNT_MISMATCH")
    unscored_holds = [x for x in settled if x.get("score") is None and str(x.get("raw_verdict", "")).startswith("OJ_INFRASTRUCTURE_HOLD")]
    if unscored_holds:
        init.update({"state":"INITIALIZATION_HOLD", "agent_requests_permitted":False,
            "c0000":settled, "initial_history_validated":False,
            "controller_hold_reason":"C0000_INFRASTRUCTURE_HOLD_NO_AGENT_FEEDBACK",
            "held_episode_ids":[x["episode_id"] for x in unscored_holds]})
        atomic(INIT_PATH, init)
        for item in unscored_holds:
            record_hold(store, item["episode_id"], item["raw_verdict"],
                        {"request_id":item["request_id"], "receipt_sha256":item["receipt_sha256"], "agent_requests_permitted":False})
        return init
    final_validation = validate_initialized_batch(store, {"schema": "N0_FORMAL_INITIALIZATION_V1",
        "state": "FORMAL_INITIALIZED", "agent_requests_permitted": True, "episode_ids": EPISODE_IDS,
        "seed_sha256": SEED_SHA, "candidate_budget": 30, "seed_identity_equal_across_episodes": True,
        "initial_history_validated": True, "c0000": settled})
    init.update({"state": "FORMAL_INITIALIZED", "agent_requests_permitted": True,
        "c0000": settled, "initial_history_validated": True, "initialized_at_utc": now(),
        "seed_identity_equal_across_episodes": True,
        "initialization_condition_limitation": "REP04 C0000 has no trusted performance score due to the preserved Reference Control infrastructure HOLD; no other Episode score was substituted."})
    init.pop("pending", None)
    init["initialization_validation"] = final_validation
    atomic(INIT_PATH, init)
    return init


def validate_initialized_batch(store, init: dict[str, Any]) -> dict[str, Any]:
    from n0_autonomous.engine import build_history_brief, canonical_bytes
    if (init.get("schema") != "N0_FORMAL_INITIALIZATION_V1" or init.get("state") != "FORMAL_INITIALIZED"
            or init.get("agent_requests_permitted") is not True or init.get("episode_ids") != EPISODE_IDS
            or init.get("seed_sha256", "").upper() != SEED_SHA or init.get("candidate_budget") != 30
            or init.get("seed_identity_equal_across_episodes") is not True or init.get("initial_history_validated") is not True):
        raise RuntimeError("FORMAL_INITIALIZATION_GATE_NOT_OPEN")
    rows = store.snapshot()["oj_requests"]
    results = []
    for eid in EPISODE_IDS:
        item = next((x for x in init.get("c0000", []) if x.get("episode_id") == eid), None)
        row = next((x for x in rows if x.get("episode_id") == eid and x.get("candidate_id") == "C0000"), None)
        if (not item or not row or row.get("request_id") != item.get("request_id")
                or row.get("state") != "CONSUMED" or row.get("receipt_consumed_at") is None
                or row.get("receipt_sha256") != item.get("receipt_sha256")
                or row.get("candidate_sha256", "").upper() != SEED_SHA):
            raise RuntimeError("FORMAL_C0000_QUEUE_IDENTITY_OR_CONSUMPTION_INVALID:" + eid)
        receipt = json.loads(row["receipt_json"])
        body = {k: v for k, v in receipt.items() if k != "receipt_sha256"}
        if (sha(canonical_bytes(body)).lower() != str(receipt.get("receipt_sha256", "")).lower()
                or receipt.get("request_id") != row["request_id"] or receipt.get("episode_id") != eid
                or receipt.get("candidate_id") != "C0000" or receipt.get("candidate_sha256", "").upper() != SEED_SHA
                or not receipt.get("terminal")):
            raise RuntimeError("FORMAL_C0000_RECEIPT_NOT_TRUSTED:" + eid)
        item_status = item.get("initialization_status")
        valid_scored = (receipt.get("verdict") == "VALID_MEASUREMENT" and isinstance(receipt.get("score"), (int, float))
            and not isinstance(receipt.get("score"), bool) and (receipt.get("profile") or {}).get("status") == "PASS"
            and (receipt.get("measurement_evidence", {}).get("reference_trust") or {}).get("pass") is True
            and item_status == "C0000_BASELINE_VALID_MEASUREMENT")
        valid_unscored_hold = (_eligible_reference_control_hold(receipt, candidate_id="C0000")
            and item_status == "C0000_BASELINE_SCORE_UNAVAILABLE_INFRA_HOLD")
        if not valid_scored and not valid_unscored_hold:
            raise RuntimeError("FORMAL_C0000_RECEIPT_NOT_TRUSTED_OR_APPROVED_UNSCORED_HOLD:" + eid)
        candidate_path = store.root / Path(row["candidate_source_path"])
        if not candidate_path.is_file() or sha(candidate_path.read_bytes()) != SEED_SHA:
            raise RuntimeError("FORMAL_C0000_SOURCE_ARCHIVE_INVALID:" + eid)
        paths = store.episode_paths(eid)
        workspace = {name: (paths.workspace / name).read_text(encoding="utf-8") for name in
                     ("task_definition.json", "candidate_abi.json", "candidate.cu", "visible_observation.json")}
        records = formal_candidates(store, eid)
        brief = {"episode_id": eid, "task_id": "KB_L1_40_LAYERNORM", "workspace_files": workspace,
                 "candidates": records, "current_candidate_id": f"C{len(records):04d}", "unsettled_requests": [], "receipt_conflicts": []}
        build_history_brief(brief, candidate_budget=30)
        results.append({"episode_id": eid, "request_id": row["request_id"], "receipt_sha256": row["receipt_sha256"],
                        "candidate_sha256": row["candidate_sha256"], "score": receipt.get("score"),
                        "initialization_status": item_status, "status": "PASS"})
    if len({x["candidate_sha256"] for x in results}) != 1:
        raise RuntimeError("FORMAL_C0000_SHARED_SEED_NOT_IDENTICAL")
    return {"status": "PASS", "episodes": results, "agent_budget_used": 0}


def record_hold(store, episode_id: str, reason: str, detail: dict[str, Any]) -> None:
    db = store._connect()
    try:
        db.execute("BEGIN IMMEDIATE")
        db.execute("UPDATE episodes SET state='INFRASTRUCTURE_HOLD',updated_at=? WHERE episode_id=? AND state NOT IN ('SCIENTIFICALLY_COMPLETED','INFRASTRUCTURE_HOLD')",
                   (time.time(), episode_id))
        store._insert_event_tx(db, episode_id, {"event_type": "FORMAL_EPISODE_INFRASTRUCTURE_HOLD",
            "reason": reason, "detail": detail}, "formal-hold:" + episode_id + ":" + reason)
        db.execute("COMMIT")
    except BaseException:
        if db.in_transaction: db.execute("ROLLBACK")
        raise
    finally:
        db.close()
    store.reconcile_episode_exports(episode_id)


def recover_formal_holds() -> dict[str, Any]:
    """Append the V1.2 recovery events after strict identity checks; never rerun prior work."""
    import shutil
    protocol_raw = PROTOCOL_PATH.read_bytes()
    protocol = json.loads(protocol_raw)
    if protocol.get("schema") != "N0_FORMAL_PROTOCOL_V1_2" or protocol.get("parent_protocol", {}).get("sha256") != "DDBAADCD4AC2D4A67FA4E6A32F8EB7B4777F6BA69F413C741C0AD0004BE51CDB":
        raise RuntimeError("FORMAL_V1_2_PROTOCOL_IDENTITY_INVALID")
    sys.path[:0] = [str(HARNESS), str(R5_INTEGRATION)]
    from n0_autonomous.parallel_runtime import N0PersistentQueue
    from n0_autonomous.engine import AuditJournal, canonical_bytes
    store = N0PersistentQueue(QUEUE_ROOT)
    snap = store.snapshot()
    if any(x["state"] in {"QUEUED", "RUNNING", "COMPLETED", "HOLD_UNKNOWN"} for x in snap["oj_requests"]):
        raise RuntimeError("FORMAL_RECOVERY_BLOCKED_BY_UNSETTLED_OJ")
    if any(x["state"] not in {"CONSUMED", "FAILED_FINAL"} for x in snap["provider_requests"]):
        raise RuntimeError("FORMAL_RECOVERY_BLOCKED_BY_UNSETTLED_PROVIDER")
    if len(snap["episodes"]) != 4 or {x["episode_id"] for x in snap["episodes"]} != set(EPISODE_IDS):
        raise RuntimeError("FORMAL_RECOVERY_EPISODE_SET_MISMATCH")
    init = load_json(INIT_PATH)
    if init.get("state") != "FORMAL_INITIALIZED" or init.get("agent_requests_permitted") is not True:
        raise RuntimeError("FORMAL_RECOVERY_INITIALIZATION_BARRIER_NOT_OPEN")
    events = []
    profile_rows = []
    now_utc = now()
    for eid in EPISODE_IDS[:3]:
        rows = [x for x in snap["oj_requests"] if x["episode_id"] == eid and x["candidate_id"] == "C0001"]
        if len(rows) != 1 or rows[0]["state"] != "CONSUMED" or not rows[0].get("receipt_json"):
            raise RuntimeError("PROFILE_RECOVERY_CANDIDATE_NOT_UNIQUELY_CONSUMED:" + eid)
        row = rows[0]
        receipt = json.loads(row["receipt_json"])
        from n0_autonomous.engine import canonical_bytes
        if (sha(canonical_bytes(receipt)).lower() != row["receipt_sha256"].lower()
                or sha(canonical_bytes({k: v for k, v in receipt.items() if k != "receipt_sha256"})).lower() != str(receipt.get("receipt_sha256", "")).lower()
                or not _eligible_profile_only_hold(receipt, candidate_id="C0001")):
            raise RuntimeError("PROFILE_RECOVERY_RECEIPT_NOT_ELIGIBLE:" + eid)
        source_path = store.root / row["candidate_source_path"]
        if (receipt.get("request_id") != row["request_id"] or receipt.get("candidate_id") != "C0001"
                or receipt.get("episode_id") != eid or receipt.get("candidate_sha256", "").lower() != row["candidate_sha256"].lower()
                or not source_path.is_file() or sha(source_path.read_bytes()).lower() != row["candidate_sha256"].lower()
                or receipt.get("oj_manifest_sha256", "").upper() != OJ_SHA):
            raise RuntimeError("PROFILE_RECOVERY_RECEIPT_IDENTITY_MISMATCH:" + eid)
        ep = next(x for x in snap["episodes"] if x["episode_id"] == eid)
        if ep["candidates_used"] != 1 or ep["max_candidates"] != 30:
            raise RuntimeError("PROFILE_RECOVERY_BUDGET_MISMATCH:" + eid)
        profile_rows.append((eid, row, receipt, ep))
    # REP04's only incomplete invocation is the one whose consumed final
    # Provider response was rejected as FINAL_CONTROL_JSON_INVALID.
    eid = EPISODE_IDS[3]
    paths = store.episode_paths(eid)
    audit = AuditJournal(paths.logs)
    audit_events = audit.events()
    failed = [x for x in audit_events if x.get("event_type") == "FORMAL_INVOCATION_FAILED" and x.get("error") == "N0Error:FINAL_CONTROL_JSON_INVALID"]
    if len(failed) != 1:
        raise RuntimeError("REP04_INVALID_CONTROL_EVIDENCE_NOT_UNIQUE")
    original_invocation = failed[0].get("invocation_id")
    starts = [x for x in audit_events if x.get("event_type") == "FORMAL_INVOCATION_STARTED" and x.get("invocation_id") == original_invocation]
    finished = [x for x in audit_events if x.get("event_type") == "FORMAL_INVOCATION_FINISHED" and x.get("invocation_id") == original_invocation]
    if len(starts) != 1 or finished:
        raise RuntimeError("REP04_INVALID_CONTROL_INVOCATION_LINEAGE_MISMATCH")
    reqs = [x for x in snap["provider_requests"] if x["episode_id"] == eid and x["invocation_id"] == original_invocation]
    if not reqs or any(x["state"] != "CONSUMED" or not x.get("response_id") or not x.get("response_sha256") or x.get("response_consumed_at") is None for x in reqs):
        raise RuntimeError("REP04_PROVIDER_RESPONSE_NOT_DURABLY_CONSUMED")
    last_req = max(reqs, key=lambda x: int(x["request_index"]))
    ep4 = next(x for x in snap["episodes"] if x["episode_id"] == eid)
    if ep4["candidates_used"] != 0 or ep4["max_candidates"] != 30:
        raise RuntimeError("REP04_BUDGET_MISMATCH")
    workspace_source = (paths.workspace / "candidate.cu").read_bytes()
    source_sha = sha(workspace_source)
    edit_events = [x for x in audit_events if x.get("event_type") == "WORKSPACE_EDIT_COMMITTED"]
    if edit_events and source_sha.lower() != str(edit_events[-1].get("after_sha256", "")).lower():
        raise RuntimeError("REP04_WORKSPACE_DOES_NOT_MATCH_LAST_DURABLE_EDIT")
    recovery_data = {
        eid: {"type": "RECOVER_AFTER_INVALID_FINAL_CONTROL", "original_invocation_id": original_invocation,
              "provider_request_id": last_req["request_id"], "provider_response_id": last_req["response_id"],
              "provider_response_sha256": last_req["response_sha256"], "old_response_replayed": False,
              "new_invocation_required": True, "working_source_sha256": source_sha, "candidate_budget_delta": 0}
    }
    for peid, row, receipt, ep in profile_rows:
        recovery_data[peid] = {"type": "RECOVER_FROM_PROFILE_INFRA_HOLD", "candidate_id": "C0001",
            "candidate_sha256": row["candidate_sha256"], "oj_request_id": row["request_id"],
            "oj_receipt_sha256": row["receipt_sha256"], "original_verdict": receipt["verdict"],
            "score": None, "profile_status": "UNAVAILABLE", "profile_failure_reason": (receipt.get("profile") or {}).get("reason"),
            "budget_before": ep["candidates_used"], "budget_after": ep["candidates_used"], "oj_rerun": False}
    db = store._connect()
    try:
        db.execute("BEGIN IMMEDIATE")
        for target, detail in recovery_data.items():
            event_type = detail["type"]
            if event_type == "RECOVER_FROM_PROFILE_INFRA_HOLD":
                event = {"event_type": event_type, "protocol_id": "N0_FORMAL_PROTOCOL_V1_2",
                         "protocol_sha256": sha(protocol_raw), **detail}
                event_id = "v12-profile-recovery:" + target + ":" + detail["oj_request_id"]
            else:
                event = {"event_type": "RECOVER_AFTER_INVALID_FINAL_CONTROL", "protocol_id": "N0_FORMAL_PROTOCOL_V1_2",
                         "protocol_sha256": sha(protocol_raw), **detail}
                event_id = "v12-invalid-control-recovery:" + target + ":" + detail["original_invocation_id"]
            old = db.execute("SELECT event_json FROM episode_events WHERE event_id=?", (event_id,)).fetchone()
            if old:
                prior = json.loads(old[0])
                comp = {k:v for k,v in prior.items() if k not in {"event_id","episode_id","created_at"}}
                expected_comp = {k:v for k,v in event.items() if k != "protocol_sha256"}
                prior_comp = {k:v for k,v in comp.items() if k != "protocol_sha256"}
                if prior.get("episode_id") != target or prior_comp != expected_comp:
                    raise RuntimeError("FORMAL_RECOVERY_EVENT_IDEMPOTENCY_CONFLICT:" + target)
                prior_protocol_sha = prior.get("protocol_sha256")
                if prior_protocol_sha != sha(protocol_raw):
                    if prior_protocol_sha != PARTIAL_V1_2_PROTOCOL_SHA:
                        raise RuntimeError("FORMAL_RECOVERY_PRIOR_PROTOCOL_HASH_UNRECOGNIZED:" + target)
                    continuation_id = "v12-recovery-continuation:" + target + ":" + event_id
                    continuation = {"event_type":"FORMAL_RECOVERY_CONTINUED_AFTER_PARTIAL_EXPORT_FAILURE",
                        "protocol_id":"N0_FORMAL_PROTOCOL_V1_2","protocol_sha256":sha(protocol_raw),
                        "prior_protocol_sha256":prior_protocol_sha,"original_recovery_event_id":event_id,
                        "original_recovery_request_id":detail.get("oj_request_id") or detail.get("provider_request_id"),
                        "provider_response_replayed":False,"oj_request_replayed":False,"candidate_budget_delta":0,
                        "reason":"OBSERVATION_VERSION_ARCHIVE_CONFLICT_BEFORE_PROTOCOL_ACTIVATION"}
                    prior_continuation = db.execute("SELECT event_json FROM episode_events WHERE event_id=?", (continuation_id,)).fetchone()
                    if prior_continuation:
                        saved = json.loads(prior_continuation[0])
                        saved_comp = {k:v for k,v in saved.items() if k not in {"event_id","episode_id","created_at"}}
                        if saved.get("episode_id") != target or saved_comp != continuation:
                            raise RuntimeError("FORMAL_RECOVERY_CONTINUATION_IDEMPOTENCY_CONFLICT:" + target)
                    else:
                        store._insert_event_tx(db, target, continuation, continuation_id)
            else:
                store._insert_event_tx(db, target, event, event_id)
            state_row = db.execute("SELECT state FROM episodes WHERE episode_id=?", (target,)).fetchone()
            if state_row is None:
                raise RuntimeError("FORMAL_RECOVERY_EPISODE_MISSING:" + target)
            if state_row[0] == "INFRASTRUCTURE_HOLD":
                db.execute("UPDATE episodes SET state='RUNNING',updated_at=? WHERE episode_id=? AND state='INFRASTRUCTURE_HOLD'", (time.time(), target))
            elif state_row[0] != "RUNNING" or old is None:
                raise RuntimeError("FORMAL_RECOVERY_STATE_TRANSITION_INVALID:" + target + ":" + str(state_row[0]))
        db.execute("COMMIT")
    except BaseException:
        if db.in_transaction: db.execute("ROLLBACK")
        raise
    finally:
        db.close()
    for target in EPISODE_IDS:
        store.reconcile_episode_exports(target)
    for peid, row, receipt, _ep in profile_rows:
        _write_observation(store, peid, row, receipt, "NOT_PROMOTED", sha(protocol_raw))
        write_episode_summary(store, peid)
    existing_recovery_audit = [x for x in audit.events() if x.get("event_type") == "FORMAL_INVOCATION_RECOVERED_FROM_INVALID_CONTROL"
                               and x.get("original_invocation_id") == original_invocation]
    if not existing_recovery_audit:
        audit.append({"event_type":"FORMAL_INVOCATION_RECOVERED_FROM_INVALID_CONTROL", "episode_id":eid,
            "original_invocation_id":original_invocation, "provider_request_id":last_req["request_id"],
            "provider_response_id":last_req["response_id"], "provider_response_sha256":last_req["response_sha256"],
            "old_response_replayed":False, "next_invocation_id":original_invocation.rsplit("INV",1)[0]+"INV0002",
            "candidate_budget_used":0, "timestamp_utc":now_utc})
    episode_snapshot = {x["episode_id"]: x for x in store.snapshot()["episodes"]}
    activation = {"schema":"N0_FORMAL_PROTOCOL_V1_2_ACTIVATION_V1", "protocol_id":"N0_FORMAL_PROTOCOL_V1_2",
        "protocol_sha256":sha(protocol_raw), "integration_release_id":INTEGRATION.name,
        "integration_manifest_sha256":sha((INTEGRATION/"RELEASE_MANIFEST.json").read_bytes()),
        "effective_at_utc":now_utc,"episode_recovery_events":recovery_data,
        "continued_partial_recovery_protocol_sha256":PARTIAL_V1_2_PROTOCOL_SHA,
        "candidate_budgets":{eid:episode_snapshot[eid]["candidates_used"] for eid in EPISODE_IDS},
        "provider_response_replays":0,"oj_reruns":0}
    activation_path = ROOT / "state" / "n0_formal_agents" / "N0_FORMAL_PROTOCOL_V1_2_ACTIVATION.json"
    atomic(activation_path, activation)
    return activation


def recover_raw_timer_measurement_holds() -> dict[str, Any]:
    """Continue only fully settled, score-null raw-timer Holds; never reissue work."""
    import re
    sys.path[:0] = [str(HARNESS), str(R5_INTEGRATION)]
    from n0_autonomous.parallel_runtime import N0PersistentQueue
    from n0_autonomous.engine import AuditJournal, canonical_bytes
    store = N0PersistentQueue(QUEUE_ROOT)
    snap = store.snapshot()
    recovered = []
    active = {"QUEUED", "RUNNING", "COMPLETED", "HOLD_UNKNOWN", "OUTCOME_UNKNOWN"}
    for ep in snap["episodes"]:
        eid = ep["episode_id"]
        if ep.get("state") != "INFRASTRUCTURE_HOLD":
            continue
        records = formal_candidates(store, eid)
        if not records:
            continue
        record = records[-1]
        receipt = record["oj_receipt"]
        cid = record["candidate_id"]
        if not _eligible_raw_timer_evidence_hold(receipt, candidate_id=cid):
            continue
        request_id = receipt["request_id"]
        row = next((x for x in snap["oj_requests"] if x["request_id"] == request_id), None)
        receipt_body = {k: v for k, v in receipt.items() if k != "receipt_sha256"}
        if (not row or row.get("state") != "CONSUMED" or row.get("receipt_consumed_at") is None
                or sha(canonical_bytes(receipt)).lower() != str(row.get("receipt_sha256", "")).lower()
                or sha(canonical_bytes(receipt_body)).lower() != str(receipt.get("receipt_sha256", "")).lower()
                or row.get("candidate_sha256", "").upper() != record["source_sha256"].upper()):
            raise RuntimeError("RAW_TIMER_HOLD_RECEIPT_NOT_CONSUMED_OR_IDENTITY_MISMATCH:" + eid)
        if any(x.get("episode_id") == eid and x.get("state") in active for x in snap["oj_requests"]):
            raise RuntimeError("RAW_TIMER_HOLD_HAS_UNSETTLED_OJ:" + eid)
        if any(x.get("episode_id") == eid and x.get("state") not in {"CONSUMED", "FAILED_FINAL"} for x in snap["provider_requests"]):
            raise RuntimeError("RAW_TIMER_HOLD_HAS_UNSETTLED_PROVIDER:" + eid)
        paths = store.episode_paths(eid)
        audit = AuditJournal(paths.logs)
        events = audit.events()
        finished = {x.get("invocation_id") for x in events if x.get("event_type") == "FORMAL_INVOCATION_FINISHED"}
        submission_events = [x for x in events if x.get("event_type") == "CONTROL_DECISION"
                             and x.get("decision") == "CONTINUE"
                             and (x.get("submission") or {}).get("request_id") == request_id]
        if not submission_events or submission_events[-1].get("invocation_id") not in finished:
            raise RuntimeError("RAW_TIMER_HOLD_INVOCATION_NOT_DURABLY_FINISHED:" + eid)
        last_turn = submission_events[-1].get("invocation_id")
        later_starts = [x for x in events if x.get("event_type") == "FORMAL_INVOCATION_STARTED"
                        and x.get("invocation_id") != last_turn
                        and str(x.get("invocation_id", "")) > str(last_turn)]
        if later_starts:
            raise RuntimeError("RAW_TIMER_HOLD_HAS_LATER_INVOCATION:" + eid)
        used = int(ep["candidates_used"])
        if used != int(cid[1:]) or used > 30:
            raise RuntimeError("RAW_TIMER_HOLD_BUDGET_SEQUENCE_MISMATCH:" + eid)
        event_id = "v12-raw-timer-resume:" + eid + ":" + request_id
        event = {"event_type": "RECOVER_FROM_NONSCORING_RAW_TIMER_HOLD", "protocol_id": "N0_FORMAL_PROTOCOL_V1_2",
                 "protocol_sha256": sha(PROTOCOL_PATH.read_bytes()), "episode_id": eid, "candidate_id": cid,
                 "candidate_sha256": record["source_sha256"], "oj_request_id": request_id,
                 "oj_receipt_sha256": receipt["receipt_sha256"], "score": None,
                 "budget_before": used, "budget_after": used, "candidate_budget_delta": 0,
                 "oj_rerun": False, "old_receipt_rewritten": False,
                 "reason": "PERFORMANCE_MEASUREMENT_EVIDENCE_UNAVAILABLE; CONTINUE_UNDER_V1_1_NONSCORING_HOLD_RULE"}
        db = store._connect()
        try:
            db.execute("BEGIN IMMEDIATE")
            prior = db.execute("SELECT event_json FROM episode_events WHERE event_id=?", (event_id,)).fetchone()
            if prior:
                saved = json.loads(prior[0])
                comparable = {k: v for k, v in saved.items() if k not in {"event_id", "created_at"}}
                if comparable != event:
                    raise RuntimeError("RAW_TIMER_RECOVERY_EVENT_IDEMPOTENCY_CONFLICT:" + eid)
            else:
                store._insert_event_tx(db, eid, event, event_id)
            state_row = db.execute("SELECT state FROM episodes WHERE episode_id=?", (eid,)).fetchone()
            if state_row is None:
                raise RuntimeError("RAW_TIMER_RECOVERY_EPISODE_MISSING:" + eid)
            if state_row[0] == "INFRASTRUCTURE_HOLD":
                db.execute("UPDATE episodes SET state='RUNNING',updated_at=? WHERE episode_id=? AND state='INFRASTRUCTURE_HOLD'", (time.time(), eid))
            elif state_row[0] != "RUNNING" or not prior:
                raise RuntimeError("RAW_TIMER_RECOVERY_STATE_CONFLICT:" + eid + ":" + str(state_row[0]))
            db.execute("COMMIT")
        except BaseException:
            if db.in_transaction:
                db.execute("ROLLBACK")
            raise
        finally:
            db.close()
        store.reconcile_episode_exports(eid)
        _write_observation(store, eid, row, receipt, "NOT_PROMOTED", sha(PROTOCOL_PATH.read_bytes()))
        write_episode_summary(store, eid)
        recovered.append({"episode_id": eid, "candidate_id": cid, "request_id": request_id,
                          "receipt_sha256": receipt["receipt_sha256"], "score": None,
                          "candidate_budget_used": used, "oj_rerun": False, "provider_replay": False,
                          "recovery_event_id": event_id})
    return {"schema": "N0_NONSCORING_MEASUREMENT_HOLD_RECOVERY_V1", "protocol_sha256": sha(PROTOCOL_PATH.read_bytes()),
            "recovered": recovered, "attempts": len(recovered)}


def _formal_prompt_bundle():
    from n0_autonomous.prompt_bundle import load_prompt_bundle
    return load_prompt_bundle(HARNESS)


def _run_episode(episode_id: str, stop_event: threading.Event) -> dict[str, Any]:
    sys.path[:0] = [str(HARNESS), str(R5_INTEGRATION), str(R20), str(OJ),
                    str(HARNESS / "third_party" / "KernelBench_repo" / "src")]
    from n0_autonomous.parallel_runtime import N0PersistentQueue, InvocationFinalizer
    from n0_autonomous.engine import AuditJournal, WorkspaceTools, HistoryService, KnowledgeService, N0ToolServer
    from n0_autonomous.agent_loop import N0AgentHarness
    from n0_autonomous.working_note import WorkingNoteStore
    from n0_autonomous.provider_adapter import N0R5ProviderQueueAdapter
    from n0_integration.provider_r20_bridge import R20N0Bridge
    from n0_integration.history_bridge import candidate_record_from_oj_row
    store = N0PersistentQueue(QUEUE_ROOT)
    paths = store.episode_paths(episode_id)
    audit = AuditJournal(paths.logs)
    pending = [x for x in audit.events() if x.get("event_type") == "FORMAL_INVOCATION_STARTED"]
    completed_ids = {x.get("invocation_id") for x in audit.events() if x.get("event_type") == "FORMAL_INVOCATION_FINISHED"}
    recovered_ids = {x.get("original_invocation_id") for x in audit.events() if x.get("event_type") == "FORMAL_INVOCATION_RECOVERED_FROM_INVALID_CONTROL"}
    if any(x.get("invocation_id") not in completed_ids and x.get("invocation_id") not in recovered_ids for x in pending):
        record_hold(store, episode_id, "INCOMPLETE_INVOCATION_NO_AUTOMATIC_REPLAY", {"incomplete_invocation_ids": [x.get("invocation_id") for x in pending if x.get("invocation_id") not in completed_ids]})
        return {"episode_id": episode_id, "state": "INFRASTRUCTURE_HOLD"}
    ws = WorkspaceTools(paths.workspace)
    ws.audit = audit
    bundle = _formal_prompt_bundle()
    provider_sha = sha((R20 / "R20_PROVIDER_RELEASE_MANIFEST.json").read_bytes())
    provider_doc = json.loads((R20 / "R20_PROVIDER_RELEASE_MANIFEST.json").read_text(encoding="utf-8"))
    bridge = R20N0Bridge(queue_root=PROVIDER_ROOT, provider_release_root=R20,
        expected_manifest_sha256=provider_sha, timeout_seconds=1800, expected_treatment="G3")
    cap = {"status": "PASS", "treatment_id": "G3", "group": "G3", "task_id": "KB_L1_40_LAYERNORM",
           "hardware_information": "FULL", "profile_information": "FULL", "expert_knowledge": "ABSENT",
           "provider_release_id": provider_doc["release_id"], "manifest_sha256": provider_sha,
           "allowed_treatments": provider_doc.get("treatment_allowlist", [])}
    adapter = N0R5ProviderQueueAdapter(store=store, queue_capability=cap, enqueue=bridge.enqueue,
        provider_release_id=provider_doc["release_id"], provider_manifest_sha256=provider_sha, treatment_id="G3")
    whitelist = load_json(R5_INTEGRATION / "N0_PROFILE_WHITELIST_NONSCIENTIFIC_V1.json")
    knowledge = KnowledgeService(HARNESS / "knowledge" / "hardware" / "atoms_v2.json")
    finalizer = InvocationFinalizer(store, ws, frozen_identity_rule=lambda new, old: bool(old and new != old))
    import re
    turn_numbers = [int(m.group(1)) for e in audit.events() if (m := re.fullmatch(r"FORMAL-" + re.escape(episode_id) + r"-INV(\d+)", str(e.get("invocation_id", ""))))]
    turn = max(turn_numbers, default=0) + 1
    while not stop_event.is_set():
        if store.is_aborted():
            return {"episode_id": episode_id, "state": "INFRA_ABORTED", "reason": store.is_aborted().get("reason")}
        snap = store.snapshot()
        ep_row = next(x for x in snap["episodes"] if x["episode_id"] == episode_id)
        if ep_row["state"] in {"SCIENTIFICALLY_COMPLETED", "INFRASTRUCTURE_HOLD"}:
            return {"episode_id": episode_id, "state": ep_row["state"]}
        if ep_row["candidates_used"] >= 30:
            db = store._connect()
            try:
                db.execute("UPDATE episodes SET state='BUDGET_EXHAUSTED',updated_at=? WHERE episode_id=? AND state NOT IN ('SCIENTIFICALLY_COMPLETED','INFRASTRUCTURE_HOLD')", (time.time(), episode_id))
            finally:
                db.close()
            store.append_episode_event(episode_id, {"event_type": "FORMAL_BUDGET_EXHAUSTED", "agent_candidates_used": 30}, event_id="formal-budget-exhausted:" + episode_id)
            return {"episode_id": episode_id, "state": "BUDGET_EXHAUSTED"}
        rows = [x for x in snap["oj_requests"] if x["episode_id"] == episode_id]
        unconsumed = [x for x in rows if x["state"] in {"QUEUED", "RUNNING", "COMPLETED", "HOLD_UNKNOWN"}]
        if unconsumed:
            row = unconsumed[-1]
            if row["state"] == "COMPLETED":
                try:
                    pending_receipt = json.loads(row["receipt_json"])
                    profile_receipt = _wait_post_receipt_profile(row, pending_receipt)
                    receipt = store.consume_oj_result(row["request_id"], row["receipt_sha256"])
                except Exception as exc:
                    record_hold(store, episode_id, "OJ_RESULT_CONSUMPTION_FAILED_NO_REPLAY", {"request_id": row["request_id"], "error": type(exc).__name__ + ":" + str(exc)})
                    return {"episode_id": episode_id, "state": "INFRASTRUCTURE_HOLD"}
                eligible_candidate_hold = (_eligible_reference_control_hold(receipt, candidate_id=row["candidate_id"])
                    or _eligible_profile_only_hold(receipt, candidate_id=row["candidate_id"])
                    or _eligible_raw_timer_evidence_hold(receipt, candidate_id=row["candidate_id"]))
                if str(receipt.get("verdict", "")).startswith("OJ_INFRASTRUCTURE_HOLD") and not eligible_candidate_hold:
                    retry = retry_infrastructure_measurement_once(store, row, receipt)
                    if retry is not None:
                        continue
                    record_hold(store, episode_id, receipt["verdict"], {"request_id": row["request_id"], "receipt_sha256": row["receipt_sha256"]})
                    return {"episode_id": episode_id, "state": "INFRASTRUCTURE_HOLD"}
                records = formal_candidates(store, episode_id)
                record = next(x for x in records if x["candidate_id"] == row["candidate_id"])
                promotion = record["ledger_entry"]["promotion"]
                _write_observation(store, episode_id, row, receipt, promotion, sha(PROTOCOL_PATH.read_bytes()),
                    record["ledger_entry"].get("promotion_confidence_status", "UNCERTAIN"), profile_receipt)
                write_episode_summary(store, episode_id)
                continue
            if row["state"] == "HOLD_UNKNOWN":
                record_hold(store, episode_id, "OJ_RESULT_UNKNOWN_NO_REPLAY", {"request_id": row["request_id"], "error": row.get("error")})
                return {"episode_id": episode_id, "state": "INFRASTRUCTURE_HOLD"}
            time.sleep(1.0)
            continue
        candidates = formal_candidates(store, episode_id)
        for rec in candidates:
            candidate_id = rec["candidate_id"]
            receipt = rec["oj_receipt"]
            eligible_candidate_hold = (_eligible_reference_control_hold(receipt, candidate_id=candidate_id)
                or _eligible_profile_only_hold(receipt, candidate_id=candidate_id)
                or _eligible_raw_timer_evidence_hold(receipt, candidate_id=candidate_id))
            if str(receipt.get("verdict", "")).startswith("OJ_INFRASTRUCTURE_HOLD") and not eligible_candidate_hold:
                record_hold(store, episode_id, receipt["verdict"], {"request_id": receipt["request_id"]})
                return {"episode_id": episode_id, "state": "INFRASTRUCTURE_HOLD"}
        current_id = f"C{ep_row['candidates_used'] + 1:04d}"
        workspace_files = {k: v for k, v in ws.required_context().items() if k != "candidate_sha256"}
        audit.append({"event_type": "FORMAL_INVOCATION_STARTED", "episode_id": episode_id,
            "invocation_id": f"FORMAL-{episode_id}-INV{turn:04d}", "turn_id": f"TURN{turn:04d}",
            "current_candidate_sha256": sha((paths.workspace / "candidate.cu").read_bytes()), "candidate_budget_used": ep_row["candidates_used"]})
        episode = {"episode_id": episode_id, "task_id": "KB_L1_40_LAYERNORM", "workspace_files": workspace_files,
            "candidates": candidates, "current_candidate_id": current_id,
            "unsettled_requests": [], "receipt_conflicts": []}
        history = HistoryService(episode_id, candidates, profile_whitelist=whitelist)
        tools = N0ToolServer(history, knowledge, audit, ws)
        agent = N0AgentHarness(system_prompt=bundle.system_prompt, task_user_message=bundle.task_user_message,
            tools=bundle.tools, model="DeepSeek-V4-Pro-0813", reasoning_effort="high", tool_server=tools,
            audit=audit, dispatch=adapter.bind_invocation(episode_id=episode_id,
                invocation_id=f"FORMAL-{episode_id}-INV{turn:04d}"), max_api_requests_per_invocation=16,
            max_tool_calls_per_invocation=32, working_note_store=WorkingNoteStore(paths.working_note),
            control_finalizer=finalizer, batch_control=store)
        observation = (paths.workspace / "visible_observation.json").read_text(encoding="utf-8")
        try:
            outcome = agent.run_outer_invocation(episode=episode, candidate_budget=30,
                visible_observation=observation, invocation_id=f"FORMAL-{episode_id}-INV{turn:04d}",
                turn_id=f"TURN{turn:04d}", audit_events=audit.events())
        except Exception as exc:
            audit.append({"event_type": "FORMAL_INVOCATION_FAILED", "episode_id": episode_id,
                "invocation_id": f"FORMAL-{episode_id}-INV{turn:04d}", "error": type(exc).__name__ + ":" + str(exc)})
            if store.is_aborted():
                return {"episode_id": episode_id, "state": "INFRA_ABORTED", "reason": store.is_aborted().get("reason")}
            record_hold(store, episode_id, "PROVIDER_OR_HARNESS_FAILURE_NO_AUTOMATIC_REPLAY", {"error": type(exc).__name__ + ":" + str(exc)})
            return {"episode_id": episode_id, "state": "INFRASTRUCTURE_HOLD"}
        audit.append({"event_type": "FORMAL_INVOCATION_FINISHED", "episode_id": episode_id,
            "invocation_id": f"FORMAL-{episode_id}-INV{turn:04d}", "decision": outcome["decision"],
            "provider_request_id": outcome.get("request_id"), "provider_response_id": outcome.get("response_id"),
            "candidate_submission": outcome.get("submission")})
        if outcome["decision"] == "STOP":
            return {"episode_id": episode_id, "state": "SCIENTIFICALLY_COMPLETED", "decision": "STOP",
                    "request_id": outcome.get("request_id"), "response_id": outcome.get("response_id")}
        submission = outcome.get("submission") or {}
        if submission.get("state") == "QUEUED":
            turn += 1
            continue
        if submission.get("status") == "NO_NEW_LOGICAL_CANDIDATE":
            turn += 1
            continue
        record_hold(store, episode_id, "CONTINUE_SUBMISSION_STATE_UNEXPECTED", {"submission": submission})
        return {"episode_id": episode_id, "state": "INFRASTRUCTURE_HOLD"}
    return {"episode_id": episode_id, "state": "STOP_REQUESTED_AFTER_CURRENT_INVOCATION"}


def run_formal_agents() -> dict[str, Any]:
    if not INIT_PATH.is_file():
        raise RuntimeError("FORMAL_INITIALIZATION_STATUS_MISSING")
    init = load_json(INIT_PATH)
    if init.get("state") != "FORMAL_INITIALIZED" or init.get("agent_requests_permitted") is not True:
        raise RuntimeError("FOUR_C0000_INITIALIZATION_RECORDS_REQUIRED_BEFORE_AGENT_REQUESTS")
    readiness = verify_readiness(require_fresh=False)
    if sha(READINESS_PATH.read_bytes()) != init.get("readiness_report_sha256"):
        raise RuntimeError("FORMAL_READINESS_EVIDENCE_CHANGED_AFTER_INITIALIZATION")
    services = verify_live_services()
    sys.path[:0] = [str(HARNESS)]
    from n0_autonomous.parallel_runtime import N0PersistentQueue
    init_validation = validate_initialized_batch(N0PersistentQueue(QUEUE_ROOT), init)
    (ROOT / "state" / "n0_formal_agents").mkdir(parents=True, exist_ok=True)
    status_path = ROOT / "state" / "n0_formal_agents" / "service_status.json"
    stop_event = threading.Event()
    service = {"schema": "N0_FORMAL_AGENT_SERVICE_V1", "state": "RUNNING", "pid": os.getpid(),
        "parent_pid": os.getppid(), "process_started_at_utc": now(), "harness_release_id": HARNESS.name,
        "harness_manifest_sha256": sha((HARNESS / "RELEASE_MANIFEST.json").read_bytes()),
        "protocol_sha256": sha(PROTOCOL_PATH.read_bytes()), "service_id": uuid.uuid4().hex,
        "process_identity": current_process_identity(), "worker_count": 4,
        "integration_manifest_sha256": services["integration_manifest_sha256"],
        "episode_ids": EPISODE_IDS, "services": services, "c0000_gate_validation": init_validation,
        "episodes": {}}
    atomic(status_path, service)
    threads = []
    results: dict[str, Any] = {}
    lock = threading.Lock()

    def one(eid: str) -> None:
        try:
            result = _run_episode(eid, stop_event)
        except BaseException as exc:
            result = {"episode_id": eid, "state": "INFRASTRUCTURE_HOLD", "error": type(exc).__name__ + ":" + str(exc)}
            try:
                sys.path[:0] = [str(HARNESS)]
                from n0_autonomous.parallel_runtime import N0PersistentQueue
                record_hold(N0PersistentQueue(QUEUE_ROOT), eid, "FORMAL_WORKER_UNCAUGHT_FAILURE", result)
            except Exception:
                pass
        with lock:
            results[eid] = result
            service["episodes"] = dict(results)
            service["heartbeat_utc"] = now()
            service["state"] = "FORMAL_RUNNING" if any(x.get("state") not in {"SCIENTIFICALLY_COMPLETED", "BUDGET_EXHAUSTED", "INFRASTRUCTURE_HOLD"} for x in results.values()) or len(results) < 4 else "FORMAL_SETTLEMENT_PENDING"
            atomic(status_path, service)

    for eid in EPISODE_IDS:
        thread = threading.Thread(target=one, args=(eid,), name="N0-" + eid, daemon=False)
        threads.append(thread)
        thread.start()
    while any(t.is_alive() for t in threads):
        service["heartbeat_utc"] = now()
        service["episodes"] = dict(results)
        atomic(status_path, service)
        time.sleep(2.0)
    for thread in threads:
        thread.join()
    states = [x.get("state") for x in results.values()]
    service["state"] = "FORMAL_RUNNING" if len(results) != 4 or any(x not in {"SCIENTIFICALLY_COMPLETED", "BUDGET_EXHAUSTED", "INFRASTRUCTURE_HOLD"} for x in states) else "FORMAL_TERMINAL_EPISODES"
    service["episodes"] = results
    service["heartbeat_utc"] = now()
    atomic(status_path, service)
    return service


def activate_compatible_runtime() -> dict[str, Any]:
    activation_path = ROOT / "state" / "n0_formal_agents" / "N0_FORMAL_PROTOCOL_V1_2_ACTIVATION.json"
    prior_raw = activation_path.read_bytes()
    prior = json.loads(prior_raw)
    manifest_path = INTEGRATION / "RELEASE_MANIFEST.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if (prior.get("protocol_sha256") != sha(PROTOCOL_PATH.read_bytes())
            or prior.get("integration_manifest_sha256") != manifest.get("parent", {}).get("manifest_sha256")
            or manifest.get("parent", {}).get("release_id") != "N0_PRODUCTION_INTEGRATION_R8_2_SETTLED_HISTORY_RESUME"):
        raise RuntimeError("COMPATIBILITY_ACTIVATION_PARENT_MISMATCH")
    archive = activation_path.with_name("N0_FORMAL_PROTOCOL_V1_2_ACTIVATION_R8_2_ARCHIVE.json")
    if archive.exists():
        if archive.read_bytes() != prior_raw:
            raise RuntimeError("PRIOR_ACTIVATION_ARCHIVE_CONFLICT")
    else:
        archive.write_bytes(prior_raw)
    updated = dict(prior)
    updated["previous_activation_sha256"] = sha(prior_raw)
    updated["integration_release_id"] = INTEGRATION.name
    updated["integration_manifest_sha256"] = sha(manifest_path.read_bytes())
    updated["compatibility_activation"] = {"effective_at_utc": now(), "science_protocol_changed": False,
        "candidate_budget_delta": 0, "provider_requests_replayed": 0, "oj_requests_replayed": 0,
        "reason": "R8_3_STATUS_WRITER_AND_SCORE_NULL_MEASUREMENT_HOLD_CONTINUATION"}
    atomic(activation_path, updated)
    return {"activation_path": str(activation_path), "archive_path": str(archive),
            "activation_sha256": sha(activation_path.read_bytes()),
            "integration_manifest_sha256": updated["integration_manifest_sha256"],
            "protocol_sha256": updated["protocol_sha256"]}


def run_recovered_pair() -> dict[str, Any]:
    targets = ["N0_FORMAL_REP01", "N0_FORMAL_REP02"]
    init = load_json(INIT_PATH)
    if init.get("state") != "FORMAL_INITIALIZED" or init.get("agent_requests_permitted") is not True:
        raise RuntimeError("FORMAL_INITIALIZATION_BARRIER_NOT_OPEN")
    verify_readiness(require_fresh=False)
    services = verify_live_services()
    sys.path[:0] = [str(HARNESS)]
    from n0_autonomous.parallel_runtime import N0PersistentQueue
    store = N0PersistentQueue(QUEUE_ROOT)
    validate_initialized_batch(store, init)
    snapshot = store.snapshot()
    if any(next(x for x in snapshot["episodes"] if x["episode_id"] == eid)["state"] != "RUNNING" for eid in targets):
        raise RuntimeError("RECOVERED_PAIR_EPISODE_NOT_RUNNING")
    if any(x.get("episode_id") in targets and x.get("state") in {"QUEUED", "RUNNING", "COMPLETED", "HOLD_UNKNOWN"} for x in snapshot["oj_requests"]):
        raise RuntimeError("RECOVERED_PAIR_HAS_UNSETTLED_OJ")
    if any(x.get("episode_id") in targets and x.get("state") not in {"CONSUMED", "FAILED_FINAL"} for x in snapshot["provider_requests"]):
        raise RuntimeError("RECOVERED_PAIR_HAS_UNSETTLED_PROVIDER")
    for eid in targets:
        audit = (store.episode_paths(eid).logs / "information_events.jsonl").read_text(encoding="utf-8")
        rows = [json.loads(line) for line in audit.splitlines() if line.strip()]
        finished = {x.get("invocation_id") for x in rows if x.get("event_type") == "FORMAL_INVOCATION_FINISHED"}
        pending = [x.get("invocation_id") for x in rows if x.get("event_type") == "FORMAL_INVOCATION_STARTED"
                   and x.get("invocation_id") not in finished]
        if pending:
            raise RuntimeError("RECOVERED_PAIR_HAS_INCOMPLETE_INVOCATION:" + eid)
    status_path = ROOT / "state" / "n0_formal_agents" / "service_status_recovered_rep01_rep02.json"
    service = {"schema":"N0_FORMAL_AGENT_SUBSET_SERVICE_V1", "state":"RUNNING", "pid":os.getpid(),
        "parent_pid":os.getppid(), "process_started_at_utc":now(), "process_identity":current_process_identity(),
        "integration_release_id":INTEGRATION.name, "integration_manifest_sha256":sha((INTEGRATION/"RELEASE_MANIFEST.json").read_bytes()),
        "protocol_sha256":sha(PROTOCOL_PATH.read_bytes()), "worker_count":2, "episode_ids":targets,
        "services":services, "episodes":{}}
    atomic(status_path, service)
    stop_event = threading.Event()
    results: dict[str, Any] = {}
    lock = threading.RLock()
    def one(eid: str) -> None:
        try:
            result = _run_episode(eid, stop_event)
        except BaseException as exc:
            result = {"episode_id":eid, "state":"INFRASTRUCTURE_HOLD", "error":type(exc).__name__+":"+str(exc)}
            try:
                record_hold(N0PersistentQueue(QUEUE_ROOT), eid, "FORMAL_WORKER_UNCAUGHT_FAILURE", result)
            except Exception:
                pass
        with lock:
            results[eid] = result
            service["episodes"] = dict(results)
            service["heartbeat_utc"] = now()
            atomic(status_path, service)
    threads = [threading.Thread(target=one, args=(eid,), name="N0-R8_3-"+eid, daemon=False) for eid in targets]
    for thread in threads:
        thread.start()
    while any(thread.is_alive() for thread in threads):
        with lock:
            service["episodes"] = dict(results)
            service["heartbeat_utc"] = now()
            atomic(status_path, service)
        time.sleep(2.0)
    for thread in threads:
        thread.join()
    with lock:
        service["episodes"] = results
        service["heartbeat_utc"] = now()
        service["state"] = "FORMAL_SUBSET_SETTLED"
        atomic(status_path, service)
    return service


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["initialize", "settle", "recover", "recover-measurement-holds", "activate-compatible-runtime", "run-recovered", "run", "status"])
    args = parser.parse_args()
    sys.path[:0] = [str(HARNESS), str(R5_INTEGRATION)]
    if args.action == "initialize":
        result = initialize()
    elif args.action == "settle":
        result = settle_initialization()
    elif args.action == "recover":
        result = recover_formal_holds()
    elif args.action == "recover-measurement-holds":
        result = recover_raw_timer_measurement_holds()
    elif args.action == "activate-compatible-runtime":
        result = activate_compatible_runtime()
    elif args.action == "run-recovered":
        result = run_recovered_pair()
    elif args.action == "run":
        result = run_formal_agents()
    elif args.action == "status":
        result = load_json(INIT_PATH) if INIT_PATH.is_file() else {"state": "NOT_INITIALIZED"}
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2))


if __name__ == "__main__":
    main()
