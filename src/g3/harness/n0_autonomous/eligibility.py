"""Fail-closed distinction between offline runnable and formal-episode eligible."""
from __future__ import annotations

import json
from pathlib import Path


def formal_eligibility(protocol_path: Path, profile_whitelist_path: Path,
                       *, gpu_qualification: dict | None = None,
                       oj_qualification: dict | None = None,
                       protocol_amendment_approval: dict | None = None) -> dict:
    protocol = json.loads(Path(protocol_path).read_text(encoding="utf-8"))
    profile = json.loads(Path(profile_whitelist_path).read_text(encoding="utf-8"))
    blockers = []
    if protocol.get("status") != "FROZEN": blockers.append("N0_PROTOCOL_NOT_FROZEN")
    if not isinstance(protocol.get("candidate_budget", {}).get("max_logical_candidates"), int): blockers.append("CANDIDATE_BUDGET_NOT_EXPLICITLY_FROZEN")
    if protocol.get("stop_rules") != "FROZEN": blockers.append("STOP_RULES_NOT_FROZEN")
    if protocol.get("limits", {}).get("context_handling") != "PROVIDER_AUTHORITATIVE_NO_LOCAL_LENGTH_GATE": blockers.append("PROVIDER_AUTHORITATIVE_CONTEXT_POLICY_MISSING")
    if profile.get("status") != "FROZEN" or not isinstance(profile.get("allowed_metrics"), list): blockers.append("PROFILE_WHITELIST_NOT_FROZEN")
    if not gpu_qualification or gpu_qualification.get("status") != "PASS": blockers.append("GPU_HEALTH_QUALIFICATION_MISSING")
    if not oj_qualification or oj_qualification.get("status") != "PASS": blockers.append("OJ_QUALIFICATION_MISSING")
    if not protocol_amendment_approval or protocol_amendment_approval.get("status") != "APPROVED": blockers.append("SCIENTIFIC_PROTOCOL_APPROVAL_MISSING")
    return {"status":"ELIGIBLE" if not blockers else "BLOCKED","formal_episode_start":False if blockers else True,"blockers":blockers}
