"""Read-only, fail-closed preflight for a new G3 batch."""
from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any

from .prompt_bundle import EXPECTED_TOOLS, RELEASE_ID, load_prompt_bundle


def _canonical_sha256(value: Any) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest().upper()


def adapt_post_profile_recovery_receipt(*, receipt: dict[str, Any], receipt_file_sha256: str,
                                        execution_identity: dict[str, Any],
                                        expected_integration_identity: dict[str, str],
                                        termination_witness: dict[str, Any]) -> dict[str, Any]:
    """Validate an Integration recovery receipt without rewriting it.

    The process-tree witness must come from the parent that observed the child
    exit. A recovery child cannot attest to its own post-exit state.
    """
    if receipt.get("schema") != "N0_POST_PROFILE_TIMING_RECOVERY_RECEIPT_V1":
        raise ValueError("REFERENCE_RECOVERY_SCHEMA_INVALID")
    if receipt.get("status") != "PASS" or receipt.get("recovery_limit") != 0.02:
        raise ValueError("REFERENCE_RECOVERY_NOT_PASS_OR_THRESHOLD_MISMATCH")
    if receipt.get("required_consecutive_controls") != 3:
        raise ValueError("REFERENCE_RECOVERY_CONSECUTIVE_RULE_MISMATCH")
    if not isinstance(receipt_file_sha256, str) or not re.fullmatch(r"[0-9A-Fa-f]{64}", receipt_file_sha256):
        raise ValueError("REFERENCE_RECOVERY_FILE_SHA_MISSING")
    body = {k: v for k, v in receipt.items()
            if k not in {"recovery_receipt_sha256", "recovery_receipt_file_sha256", "recovery_receipt_path"}}
    internal_sha = receipt.get("recovery_receipt_sha256")
    if not isinstance(internal_sha, str) or _canonical_sha256(body) != internal_sha.upper():
        raise ValueError("REFERENCE_RECOVERY_INTERNAL_SHA_MISMATCH")
    controls = receipt.get("controls")
    if not isinstance(controls, list) or len(controls) < 3:
        raise ValueError("REFERENCE_RECOVERY_CONTROLS_MISSING")
    last = controls[-3:]
    if [x.get("consecutive_passes") for x in last] != [1, 2, 3]:
        raise ValueError("REFERENCE_RECOVERY_NOT_THREE_CONSECUTIVE_PASSES")
    baseline_ids = set()
    for control in last:
        trust = control.get("baseline_check") or {}
        health = control.get("health_check") or {}
        if (control.get("passed") is not True or trust.get("pass") is not True
                or trust.get("recovery_limit") != 0.02 or health.get("status") != "PASS"):
            raise ValueError("REFERENCE_RECOVERY_CONTROL_INCOMPLETE")
        baseline_ids.add(trust.get("baseline_id"))
    if len(baseline_ids) != 1 or None in baseline_ids:
        raise ValueError("REFERENCE_RECOVERY_BASELINE_ID_MISMATCH")
    gpu = receipt.get("gpu_identity") or {}
    if not receipt.get("lease_id") or not gpu.get("gpu_uuid"):
        raise ValueError("REFERENCE_RECOVERY_DEVICE_OR_LEASE_IDENTITY_MISSING")
    expected_integration = execution_identity.get("integration") or {}
    if (not expected_integration.get("release_id") or not expected_integration.get("manifest_sha256")
            or str(execution_identity.get("receipt_file_sha256", "")).upper() != receipt_file_sha256.upper()
            or expected_integration.get("release_id") != expected_integration_identity.get("release_id")
            or str(expected_integration.get("manifest_sha256", "")).upper()
            != str(expected_integration_identity.get("manifest_sha256", "")).upper()):
        raise ValueError("REFERENCE_RECOVERY_EXECUTION_IDENTITY_MISMATCH")
    if (termination_witness.get("terminated_tree") is not True
            or termination_witness.get("active_processes_after") != 0
            or termination_witness.get("returncode") != 0
            or not isinstance(termination_witness.get("pid"), int)
            or not termination_witness.get("launch_generation")):
        raise ValueError("REFERENCE_RECOVERY_PROCESS_TREE_NOT_PROVEN_TERMINATED")
    return {
        "schema": "G3_REFERENCE_QUALIFICATION_ADAPTER_V1",
        "status": "PASS",
        "max_deviation_percent": 2.0,
        "terminated_tree": True,
        "controls": [{"status": "PASS", "source_control_index": x.get("index"),
                      "baseline_id": (x.get("baseline_check") or {}).get("baseline_id")}
                     for x in last],
        "source_recovery_id": receipt.get("recovery_id"),
        "source_receipt_sha256": receipt_file_sha256.upper(),
        "source_internal_receipt_sha256": internal_sha.upper(),
        "integration_release_id": expected_integration["release_id"],
        "integration_manifest_sha256": expected_integration["manifest_sha256"].upper(),
        "termination_witness": dict(termination_witness),
    }


def validate_g3_start(*, release_root: Path, qualification: dict[str, Any] | None,
                      worker_ids: list[str], gpu_lease_available: bool,
                      provider_configured: bool, unresolved_requests: int,
                      stop_requested: bool = False) -> dict[str, Any]:
    root = Path(release_root)
    blockers: list[str] = []
    try:
        bundle = load_prompt_bundle(root)
    except Exception as exc:
        bundle = None
        blockers.append("RELEASE_OR_PROMPT_INVALID:" + type(exc).__name__ + ":" + str(exc))
    manifest_path = root / "RELEASE_MANIFEST.json"
    if manifest_path.is_file():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        for entry in manifest.get("source_files", []):
            path = root / entry["path"]
            if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest().upper() != entry["sha256"]:
                blockers.append("RELEASE_FILE_HASH_MISMATCH:" + entry["path"])
                break
    else:
        manifest = {}
        blockers.append("RELEASE_MANIFEST_MISSING")
    config_path = root / "protocol" / "G3_EXPERIMENT_CONFIGURATION_R1.json"
    if not config_path.is_file():
        blockers.append("G3_EXPERIMENT_CONFIGURATION_MISSING")
    else:
        config = json.loads(config_path.read_text(encoding="utf-8"))
        expected = {"group": "G3", "hardware_information": "FULL", "profile_information": "FULL",
                    "expert_knowledge": "ABSENT", "task_id": "KB_L1_40_LAYERNORM",
                    "workers": 10, "episodes_per_worker": 1, "max_logical_candidates_per_episode": 30}
        for key, value in expected.items():
            if config.get(key) != value:
                blockers.append("G3_CONFIG_MISMATCH:" + key)
        if config.get("model_visible_tools") != EXPECTED_TOOLS:
            blockers.append("G3_TOOL_SET_INVALID")
        runtime_entry = config.get("worker_runtime_entrypoint")
        if not isinstance(runtime_entry, str) or not runtime_entry:
            blockers.append("G3_PRODUCTION_WORKER_RUNTIME_NOT_BOUND")
        elif runtime_entry not in {x.get("path") for x in manifest.get("source_files", [])}:
            blockers.append("G3_PRODUCTION_WORKER_RUNTIME_NOT_IN_RELEASE_MANIFEST")
    if bundle is not None:
        visible = bundle.system_prompt + "\n" + bundle.task_user_message + "\n" + json.dumps(bundle.tools)
        if re.search(r"(?i)query_expert|expert knowledge|candidate budget|remaining budget|30 logical submissions", visible):
            blockers.append("EXPERT_OR_BUDGET_DISCLOSURE")
        history = next(x for x in bundle.tools if x["function"]["name"] == "query_candidate_history")
        fields = history["function"]["parameters"]["properties"]["fields"]["items"].get("enum", [])
        if "profile" not in fields:
            blockers.append("G3_PROFILE_HISTORY_FIELD_MISSING")
    if (root / "knowledge" / "expert").exists():
        blockers.append("EXPERT_ASSET_PRESENT_IN_RELEASE_ROOT")
    if not qualification or qualification.get("status") != "PASS":
        blockers.append("REFERENCE_QUALIFICATION_NOT_PASS")
    else:
        controls = qualification.get("controls")
        if not isinstance(controls, list) or len(controls) != 3 or any(x.get("status") != "PASS" for x in controls):
            blockers.append("REFERENCE_QUALIFICATION_NOT_THREE_CONSECUTIVE_FULL_PASSES")
        if qualification.get("max_deviation_percent") != 2.0:
            blockers.append("REFERENCE_THRESHOLD_IDENTITY_MISMATCH")
        if qualification.get("terminated_tree") is not True:
            blockers.append("QUALIFICATION_PROCESS_TREE_NOT_TERMINATED")
        expected_integration = (manifest.get("runtime_bindings") or {}).get("integration") or {}
        if (qualification.get("integration_release_id") != expected_integration.get("release_id")
                or str(qualification.get("integration_manifest_sha256", "")).upper()
                != str(expected_integration.get("manifest_sha256", "")).upper()):
            blockers.append("QUALIFICATION_INTEGRATION_IDENTITY_MISMATCH")
    if len(worker_ids) != 10 or len(set(worker_ids)) != 10:
        blockers.append("WORKER_REGISTRY_NOT_TEN_UNIQUE_IDENTITIES")
    if not gpu_lease_available:
        blockers.append("GPU_LEASE_UNAVAILABLE")
    if not provider_configured:
        blockers.append("PROVIDER_CONFIGURATION_UNAVAILABLE")
    if unresolved_requests != 0:
        blockers.append("UNRESOLVED_OJ_REQUESTS_PRESENT")
    if stop_requested:
        blockers.append("STOP_REQUESTED")
    return {"status": "PASS" if not blockers else "BLOCKED", "release_id": RELEASE_ID,
            "formal_start": False, "blockers": blockers}
