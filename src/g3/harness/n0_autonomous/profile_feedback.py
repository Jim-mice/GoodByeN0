"""Authentication and projection for post-receipt NCU feedback.

This is an additive Harness interface: it does not alter OJ scores, budget
settlement, frozen prompts, or the immutable OJ receipt.
"""
from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path
from typing import Any

DEFAULT_ALLOWED_METRICS = {
    "launch__registers_per_thread", "launch__shared_mem_per_block",
    "sm__warps_active.avg.pct_of_peak_sustained_active", "gpu__time_duration.sum",
    "dram__bytes.sum",
}


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def validate_profile_receipt(receipt: dict[str, Any], *, request_id: str, episode_id: str,
                             candidate_id: str, candidate_sha256: str,
                             oj_receipt_sha256: str, allowed_metrics: set[str]) -> dict[str, Any]:
    if not isinstance(receipt, dict) or receipt.get("schema") != "N0_POST_RECEIPT_PROFILE_RECEIPT_V1":
        raise ValueError("PROFILE_RECEIPT_SCHEMA_INVALID")
    claimed = str(receipt.get("profile_receipt_sha256", "")).lower()
    body = {k: v for k, v in receipt.items() if k != "profile_receipt_sha256"}
    actual = hashlib.sha256(canonical_bytes(body)).hexdigest()
    if not claimed or claimed != actual:
        raise ValueError("PROFILE_RECEIPT_SHA256_MISMATCH")
    expected = {"request_id": request_id, "profile_receipt_id": request_id,
                "episode_id": episode_id, "candidate_id": candidate_id,
                "candidate_sha256": candidate_sha256, "oj_receipt_sha256": oj_receipt_sha256}
    for key, value in expected.items():
        if str(receipt.get(key, "")).lower() != str(value).lower():
            raise ValueError("PROFILE_RECEIPT_IDENTITY_MISMATCH:" + key)
    if receipt.get("score_inclusion") is not False:
        raise ValueError("PROFILE_RECEIPT_SCORE_INCLUSION_INVALID")
    status = receipt.get("status")
    profile = receipt.get("profile") if isinstance(receipt.get("profile"), dict) else {}
    if status == "PASS":
        if profile.get("status") != "PASS" or profile.get("score_inclusion") is not False:
            raise ValueError("PROFILE_PASS_INNER_STATUS_INVALID")
        if str(profile.get("request_id", "")).lower() != request_id.lower():
            raise ValueError("PROFILE_PASS_REQUEST_ID_MISMATCH")
        if str(profile.get("candidate_sha256", "")).lower() != candidate_sha256.lower():
            raise ValueError("PROFILE_PASS_CANDIDATE_SHA_MISMATCH")
        raw_metrics = profile.get("metrics")
        if not isinstance(raw_metrics, dict) or not raw_metrics:
            raise ValueError("PROFILE_PASS_METRICS_MISSING")
        if allowed_metrics - set(raw_metrics):
            raise ValueError("PROFILE_PASS_METRIC_SET_INCOMPLETE")
        projected = {k: raw_metrics[k] for k in sorted(allowed_metrics) if k in raw_metrics}
        if not projected:
            raise ValueError("PROFILE_PASS_NO_ALLOWLISTED_METRICS")
        return {"status": "PASS", "request_id": request_id, "episode_id": episode_id,
                "candidate_id": candidate_id, "candidate_sha256": candidate_sha256,
                "oj_receipt_sha256": oj_receipt_sha256, "profile_receipt_sha256": claimed,
                "measurement_protocol": profile.get("measurement_protocol"),
                "shape": profile.get("shape"), "metrics": projected}
    if status == "UNAVAILABLE":
        reason = str(profile.get("reason") or receipt.get("recovery_failure") or "PROFILE_UNAVAILABLE")
        return {"status": "UNAVAILABLE", "request_id": request_id, "episode_id": episode_id,
                "candidate_id": candidate_id, "candidate_sha256": candidate_sha256,
                "oj_receipt_sha256": oj_receipt_sha256, "profile_receipt_sha256": claimed,
                "reason": reason, "metrics": {}}
    raise ValueError("PROFILE_RECEIPT_TERMINAL_STATUS_INVALID")


def wait_profile_feedback(receipt_path: Path, *, request_id: str, episode_id: str,
                          candidate_id: str, candidate_sha256: str, oj_receipt_sha256: str,
                          allowed_metrics: set[str], timeout_seconds: float = 660.0,
                          poll_seconds: float = 0.25) -> dict[str, Any]:
    path = Path(receipt_path)
    deadline = time.monotonic() + max(0.0, timeout_seconds)
    while True:
        if path.is_file():
            try:
                receipt = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise ValueError("PROFILE_RECEIPT_READ_FAILED:" + type(exc).__name__) from exc
            return validate_profile_receipt(receipt, request_id=request_id, episode_id=episode_id,
                candidate_id=candidate_id, candidate_sha256=candidate_sha256,
                oj_receipt_sha256=oj_receipt_sha256, allowed_metrics=allowed_metrics)
        if time.monotonic() >= deadline:
            raise TimeoutError("PROFILE_RECEIPT_NOT_DURABLE_BEFORE_AGENT_DECISION:" + request_id)
        time.sleep(poll_seconds)


def render_visible_observation(observation: dict[str, Any], feedback: dict[str, Any]) -> str:
    """Attach authenticated profiling to the existing model-visible field."""
    if not isinstance(observation, dict) or not isinstance(feedback, dict):
        raise ValueError("PROFILE_OBSERVATION_INPUT_INVALID")
    if feedback.get("status") not in {"PASS", "UNAVAILABLE", "NOT_APPLICABLE"}:
        raise ValueError("PROFILE_FEEDBACK_STATUS_INVALID")
    result = dict(observation)
    result["profile_status"] = feedback["status"]
    result["profile_feedback"] = feedback
    return json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True)
