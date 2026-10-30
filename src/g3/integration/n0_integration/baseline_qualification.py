from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any


def environment_fingerprint(identity: dict[str, Any]) -> str:
    raw = json.dumps(identity, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(raw).hexdigest().upper()


def validate_frozen_baseline(baseline: dict[str, Any], *, expected_scope: dict[str, Any],
                             now_utc: datetime | None = None) -> dict[str, Any]:
    """Fail closed unless a baseline has measured provenance and explicit lifetime."""
    now_utc = now_utc or datetime.now(timezone.utc)
    failures = []
    qualification = baseline.get("qualification") or {}
    if baseline.get("status") != "QUALIFIED_FROZEN_BASELINE":
        failures.append("BASELINE_STATUS_NOT_QUALIFIED")
    for key in ("task_sha256", "reference_source_sha256", "kernelbench_commit", "timing_source_sha256"):
        if baseline.get(key) != expected_scope.get(key):
            failures.append("BASELINE_SCOPE_MISMATCH:" + key)
    if baseline.get("gpu_identity") != expected_scope.get("gpu_identity"):
        failures.append("BASELINE_SCOPE_MISMATCH:gpu_identity")
    if baseline.get("environment_identity") != expected_scope.get("environment_identity"):
        failures.append("BASELINE_SCOPE_MISMATCH:environment_identity")
    if qualification.get("environment_fingerprint") != environment_fingerprint(expected_scope):
        failures.append("BASELINE_ENVIRONMENT_FINGERPRINT_MISMATCH")
    if qualification.get("protocol") != "N0_FROZEN_REFERENCE_QUALIFICATION_V2":
        failures.append("BASELINE_QUALIFICATION_PROTOCOL_MISSING")
    if int(qualification.get("independent_process_sessions", 0) or 0) < 100:
        failures.append("BASELINE_INSUFFICIENT_INDEPENDENT_SESSIONS")
    if not qualification.get("raw_evidence_manifest_sha256"):
        failures.append("BASELINE_RAW_EVIDENCE_MANIFEST_MISSING")
    try:
        started = datetime.fromisoformat(qualification["valid_from_utc"].replace("Z", "+00:00"))
        expires = datetime.fromisoformat(qualification["valid_until_utc"].replace("Z", "+00:00"))
        if started.tzinfo is None or expires.tzinfo is None or expires <= started:
            raise ValueError
        if now_utc < started:
            failures.append("BASELINE_NOT_YET_VALID")
        if now_utc >= expires:
            failures.append("BASELINE_EXPIRED")
        if (expires - started).total_seconds() > 30 * 86400:
            failures.append("BASELINE_VALIDITY_EXCEEDS_30_DAYS")
    except (KeyError, TypeError, ValueError):
        failures.append("BASELINE_VALIDITY_WINDOW_INVALID")
    return {"status": "PASS" if not failures else "BLOCKED", "failures": failures,
            "baseline_id": baseline.get("baseline_id"),
            "environment_fingerprint": environment_fingerprint(expected_scope),
            "checked_at_utc": now_utc.isoformat(),
            "valid_until_utc": qualification.get("valid_until_utc"),
            "scope": expected_scope}
