"""Online Best settlement for the simplified R11 feedback contract."""
from __future__ import annotations

POLICY = "N0_ONLINE_BEST_MARGIN_1P5_PERCENT_V1"
SCORE_VERSION = "N0_FROZEN_REFERENCE_SCORE_ONLINE_V2"
MARGIN = 0.015


def classify_online_best(measured: dict, incumbent: dict | None) -> tuple[str, str]:
    if measured.get("score_version") != SCORE_VERSION or not measured.get("baseline_id"):
        return "UNCERTAIN", "SCORE_IDENTITY_MISSING"
    if incumbent is None:
        return "ONLINE_PROVISIONAL_BEST", "INITIAL_VALID_MEASUREMENT"
    if measured.get("baseline_id") != incumbent.get("baseline_id"):
        return "UNCERTAIN", "INCOMPARABLE_BASELINE"
    if measured.get("score_version") != incumbent.get("score_version"):
        return "UNCERTAIN", "INCOMPARABLE_SCORE_VERSION"
    if measured.get("candidate_sha256") == incumbent.get("candidate_sha256"):
        return "UNCERTAIN", "SAME_SOURCE_ALREADY_SETTLED"
    candidate = measured.get("candidate_runtime_ms")
    best = incumbent.get("candidate_runtime_ms")
    if not isinstance(candidate, (int, float)) or not isinstance(best, (int, float)) or candidate <= 0 or best <= 0:
        return "UNCERTAIN", "RUNTIME_MISSING_OR_INVALID"
    ratio = float(candidate) / float(best)
    if ratio <= 1.0 - MARGIN:
        return "ONLINE_PROVISIONAL_BEST", "CANDIDATE_RUNTIME_IMPROVED_BEYOND_FROZEN_MARGIN"
    if ratio >= 1.0 + MARGIN:
        return "CONFIRMED_REGRESSION", "CANDIDATE_RUNTIME_REGRESSED_BEYOND_FROZEN_MARGIN"
    return "UNCERTAIN", "DIFFERENCE_WITHIN_FROZEN_NOISE_GUARD"
