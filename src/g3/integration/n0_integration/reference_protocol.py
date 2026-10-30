from __future__ import annotations
import math
from typing import Any

POST_EVALUATOR_CONTROL_LIMIT = 0.02
POST_EVALUATOR_CONTROL_POLICY = "N0_POST_EVALUATOR_REFERENCE_PAIRED_V2"


def enforce_post_evaluator_control_limit(trust: dict[str, Any]) -> dict[str, Any]:
    """Apply the locally calibrated, tightened post-evaluator path guard."""
    relative = trust.get("relative_difference")
    valid = isinstance(relative, (int, float)) and math.isfinite(float(relative)) and float(relative) >= 0
    return {**trust, "policy": POST_EVALUATOR_CONTROL_POLICY,
        "legacy_path_gate_limit": trust.get("limit"),
        "limit": POST_EVALUATOR_CONTROL_LIMIT,
        "pass": bool(trust.get("pass") is True and valid and float(relative) <= POST_EVALUATOR_CONTROL_LIMIT)}
