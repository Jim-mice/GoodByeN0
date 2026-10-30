from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .readiness import evidence_file_gate, sha256, verify_manifest, service_evidence

PRELAUNCH_CHECKS = (
    "release_harness_r6", "release_oj_r5", "release_provider_r20",
    "provider_formal_queue", "oj_formal_queue_pinned_python",
    "cuda_extension_runtime", "nonscientific_timing_qualification",
    "profile_history_roundtrip", "gpu_lease_and_interference_monitor",
    "four_worker_nonscientific_loop", "protocol_freeze_candidate",
    "formal_allocation_plan", "provider_recovery_test", "legacy_transaction_isolation",
)


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
        return value if isinstance(value, dict) else None
    except Exception:
        return None


def _machine_check(check_id: str, evidence: Path, *, schema: str | None = None) -> dict[str, Any]:
    result = evidence_file_gate(evidence, schema)
    return {"id": check_id, "status": result["status"], "evidence_path": str(evidence),
            "evidence_sha256": result.get("sha256"), "detail": result,
            "checked_at_utc": datetime.now(timezone.utc).isoformat()}


def _live_oj_identity(root: Path, evidence_root: Path) -> dict[str, Any]:
    status_path = root / "state" / "n0_oj_formal" / "service_status.json"
    doc = _read_json(status_path)
    if not doc:
        return {"status": "BLOCKED", "reason": "OJ_SERVICE_STATUS_MISSING"}
    expected_python = (root / ".venv_n0_r6" / "Scripts" / "python.exe").resolve()
    try:
        import subprocess
        pid = int(doc["pid"])
        command = ("$p=Get-CimInstance Win32_Process -Filter 'ProcessId=" + str(pid) + "'; "
                   "if ($null -eq $p) { '{}' } else { $p | Select-Object ProcessId,ParentProcessId,CreationDate,ExecutablePath,CommandLine | ConvertTo-Json -Compress }")
        proc = subprocess.run(["powershell.exe", "-NoProfile", "-Command", command],
                              capture_output=True, text=True, timeout=12)
        actual = json.loads(proc.stdout.strip() or "{}")
        parent = {}
        if actual:
            parent_command = ("$p=Get-CimInstance Win32_Process -Filter 'ProcessId=" + str(int(actual.get("ParentProcessId", 0))) + "'; "
                              "if ($null -eq $p) { '{}' } else { $p | Select-Object ProcessId,ParentProcessId,CreationDate,ExecutablePath,CommandLine | ConvertTo-Json -Compress }")
            parent_result = subprocess.run(["powershell.exe", "-NoProfile", "-Command", parent_command],
                                           capture_output=True, text=True, timeout=12)
            parent = json.loads(parent_result.stdout.strip() or "{}")
    except Exception as exc:
        return {"status": "BLOCKED", "reason": "OJ_CIM_IDENTITY_QUERY_FAILED:" + type(exc).__name__}
    age = None
    try:
        age = (datetime.now(timezone.utc) - datetime.fromisoformat(doc["heartbeat_utc"].replace("Z", "+00:00"))).total_seconds()
    except Exception:
        pass
    checks = {
        "state_running": doc.get("state") == "RUNNING",
        "queue_root_matches": Path(doc.get("queue_root", "")).resolve() == (root / "state" / "n0_oj_formal").resolve(),
        "release_matches": doc.get("release_id") == "N0_KERNELBENCH_L1_40_LAYERNORM_R5_REFERENCE_TRUST",
        "heartbeat_fresh": age is not None and 0 <= age <= 15,
        "process_live": bool(actual),
        "creation_matches": bool(actual) and str(actual.get("CreationDate")) == str(doc.get("process_creation_date")),
        "sys_executable_pinned": (doc.get("detail") or {}).get("environment_identity", {}).get("sys_executable")
            and Path((doc.get("detail") or {}).get("environment_identity", {}).get("sys_executable")).resolve() == expected_python,
        "absolute_python_launch_verified": bool(actual) and (
            Path(actual.get("ExecutablePath", "")).resolve() == expected_python or
            (bool(parent) and Path(parent.get("ExecutablePath", "")).resolve() == expected_python
             and "run_r5_oj_worker.py" in str(parent.get("CommandLine", ""))
             and str(root / "state" / "n0_oj_formal").casefold() in str(parent.get("CommandLine", "")).casefold())),
    }
    return {"status": "PASS" if all(checks.values()) else "BLOCKED", "checks": checks,
            "expected_python": str(expected_python), "actual_process": actual, "launcher_process": parent,
            "heartbeat_age_seconds": age}


def _formal_state(root: Path) -> dict[str, Any]:
    db_path = root / "state" / "n0_formal" / "n0_runtime.sqlite3"
    if not db_path.is_file():
        return {"episodes": [], "abort": None}
    try:
        with sqlite3.connect(db_path) as db:
            tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            episodes = []
            if "episodes" in tables:
                episodes = [{"episode_id": row[0], "state": row[1]} for row in
                            db.execute("SELECT episode_id,state FROM episodes WHERE episode_id LIKE 'N0_FORMAL_REP%' ORDER BY episode_id")]
            abort = None
            if "runtime_meta" in tables:
                row = db.execute("SELECT value FROM runtime_meta WHERE key='science_batch_abort'").fetchone()
                abort = json.loads(row[0]) if row else None
            return {"episodes": episodes, "abort": abort}
    except Exception as exc:
        return {"episodes": [], "abort": {"status": "UNKNOWN", "error": type(exc).__name__ + ":" + str(exc)}}


def compute_staged_readiness(*, evidence_root: Path) -> dict[str, Any]:
    """Stage-aware readiness; prelaunch never depends on formal receipts/allocations."""
    root = Path(evidence_root).resolve()
    integration = root / "integrations" / "N0_PRODUCTION_INTEGRATION_R5"
    reports = root / "reports" / "n0_timing_qualification"
    release_specs = (
        ("release_harness_r6", root / "harness/releases/N0_AUTONOMOUS_INFORMATION_ACQUISITION_R6/RELEASE_MANIFEST.json", "N0_AUTONOMOUS_INFORMATION_ACQUISITION_R6"),
        ("release_oj_r5", root / "oj/releases/N0_KERNELBENCH_L1_40_LAYERNORM_R5_REFERENCE_TRUST/RELEASE_MANIFEST.json", "N0_KERNELBENCH_L1_40_LAYERNORM_R5_REFERENCE_TRUST"),
        ("release_provider_r20", root / "provider/releases/N0_PARATERA_PROVIDER_R20/R20_PROVIDER_RELEASE_MANIFEST.json", "N0_PARATERA_PROVIDER_R20"),
    )
    checks: list[dict[str, Any]] = []
    for check_id, manifest, release_id in release_specs:
        result = verify_manifest(manifest, release_id)
        checks.append({"id": check_id, "status": result["status"], "evidence_path": str(manifest),
                       "evidence_sha256": result.get("manifest_sha256"), "detail": result,
                       "checked_at_utc": datetime.now(timezone.utc).isoformat()})

    provider_status = root / "state" / "n0_provider_formal" / "service_status.json"
    provider_manifest = root / "provider" / "releases" / "N0_PARATERA_PROVIDER_R20" / "R20_PROVIDER_RELEASE_MANIFEST.json"
    try:
        from .provider_service import snapshot_r20_service
        provider_snapshot = snapshot_r20_service(queue_root=root / "state" / "n0_provider_formal",
            release_root=provider_manifest.parent, manifest_sha256=sha256(provider_manifest),
            output_path=reports / "N0_PROVIDER_FORMAL_LIVE_IDENTITY_V1.json")
        provider = {"status": provider_snapshot.get("status"), "snapshot": provider_snapshot}
    except Exception as exc:
        provider = {"status": "BLOCKED", "reason": type(exc).__name__ + ":" + str(exc)}
    checks.append({"id": "provider_formal_queue", "status": provider["status"], "evidence_path": str(provider_status),
                   "evidence_sha256": sha256(provider_status) if provider_status.is_file() else None,
                   "detail": provider, "checked_at_utc": datetime.now(timezone.utc).isoformat()})

    oj = _live_oj_identity(root, root)
    checks.append({"id": "oj_formal_queue_pinned_python", "status": oj["status"],
                   "evidence_path": str(root / "state/n0_oj_formal/service_status.json"),
                   "evidence_sha256": sha256(root / "state/n0_oj_formal/service_status.json") if (root / "state/n0_oj_formal/service_status.json").is_file() else None,
                   "detail": oj, "checked_at_utc": datetime.now(timezone.utc).isoformat()})

    static_specs = (
        ("cuda_extension_runtime", root / "qualification/cuda_extension_live.json", None),
        ("nonscientific_timing_qualification", root / "qualification/oj_timing_qualification.json", None),
        ("profile_history_roundtrip", root / "qualification/profile_query_roundtrip.json", None),
        ("gpu_lease_and_interference_monitor", root / "qualification/gpu_exclusivity.json", None),
        ("four_worker_nonscientific_loop", root / "qualification/four_worker_e2e.json", None),
        ("protocol_freeze_candidate", root / "protocol/N0_FORMAL_PROTOCOL_V1.json", None),
        ("formal_allocation_plan", root / "allocations/N0_FORMAL_ALLOCATION_PLAN_V1.json", None),
        ("provider_recovery_test", root / "qualification/provider_recovery.json", None),
        ("legacy_transaction_isolation", root / "qualification/old_transactions_reconciled.json", None),
    )
    for check_id, path, schema in static_specs:
        checks.append(_machine_check(check_id, path, schema=schema))

    ids = [item["id"] for item in checks]
    if len(ids) != len(set(ids)) or set(ids) != set(PRELAUNCH_CHECKS):
        raise RuntimeError("PRELAUNCH_CHECK_ID_SET_INVALID")
    formal = _formal_state(root)
    states = [row["state"] for row in formal["episodes"]]
    abort = formal["abort"]
    init = _read_json(root / "state" / "n0_formal" / "initialization_transaction.json")
    if abort:
        status, stage = "INFRA_ABORTED", "TERMINAL"
    elif len(states) == 4 and all(state == "SCIENTIFICALLY_COMPLETED" for state in states):
        status, stage = "FORMAL_COMPLETED", "TERMINAL"
    elif init and init.get("state") == "HOLD":
        status, stage = "HOLD", "FORMAL_INITIALIZING"
    elif init and init.get("state") in {"STARTED", "EPISODES_REGISTERED", "C0000_PENDING", "C0000_SETTLED"}:
        status, stage = "FORMAL_INITIALIZING", "FORMAL_INITIALIZING"
    elif (init and init.get("state") == "COMPLETE" and len(states) == 4
          and any(state in {"RUNNING", "WAITING_PROVIDER", "WAITING_OJ"} for state in states)):
        status, stage = "FORMAL_RUNNING", "FORMAL_RUNNING"
    elif all(item["status"] == "PASS" for item in checks):
        status, stage = "PRELAUNCH_READY", "PRELAUNCH_READY"
    else:
        status, stage = "FORMAL_BLOCKED", "PRELAUNCH"
    return {"schema": "N0_FORMAL_READINESS_REPORT_V5", "release_id": "N0_PRODUCTION_INTEGRATION_R5",
            "stage": stage, "overall_status": status, "checks": checks,
            "required_prelaunch_check_ids": list(PRELAUNCH_CHECKS), "formal_episodes": formal["episodes"],
            "initialization_transaction": init, "batch_abort": abort,
            "evidence_root": str(root), "computed_at_utc": datetime.now(timezone.utc).isoformat(),
            "prior_report_text_used_as_authority": False,
            "formal_c0000_or_formal_episode_allocation_required_for_prelaunch": False}
