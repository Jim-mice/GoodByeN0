"""Independent-process paired promotion protocol for the staged R9 release."""
from __future__ import annotations

from typing import Any

REQUIRED_INDEPENDENT_PAIRS = 10
PROTOCOL = "N0_ABBA_INDEPENDENT_PROCESS_SIGN_TEST_V1"


def classify_paired_recheck(pairs: list[dict[str, Any]], *, baseline_id: str,
                            score_version: str, required_pairs: int = REQUIRED_INDEPENDENT_PAIRS) -> dict[str, Any]:
    """Use ten process-independent ABBA sessions; all ten signs must agree.

    Each item represents one fresh process/lease session with raw ABBA order
    incumbent/challenger/challenger/incumbent. Session means form the sign test;
    CUDA Events inside a process are not treated as independent sessions.
    """
    valid = []
    for item in pairs:
        if not isinstance(item, dict) or item.get("status") != "PASS":
            continue
        if item.get("baseline_id") != baseline_id or item.get("score_version") != score_version:
            continue
        if not item.get("independent_process_id"):
            continue
        samples = item.get("abba_samples_ms")
        if not isinstance(samples, list) or len(samples) != 4:
            continue
        expected_order = ["INCUMBENT", "CHALLENGER", "CHALLENGER", "INCUMBENT"]
        if [x.get("role") for x in samples if isinstance(x, dict)] != expected_order:
            continue
        times = [x.get("runtime_ms") for x in samples]
        if any(not isinstance(x, (float, int)) or x <= 0 for x in times):
            continue
        incumbent = (float(times[0]) + float(times[3])) / 2
        challenger = (float(times[1]) + float(times[2])) / 2
        valid.append({"status":"PASS","baseline_id":baseline_id,"score_version":score_version,
                      "independent_process_id": item["independent_process_id"],
                      "incumbent_runtime_ms": incumbent,
                      "challenger_runtime_ms": challenger,
                      "difference_ms": challenger - incumbent,
                      "abba_samples_ms": samples})
    unique = {x["independent_process_id"] for x in valid}
    if len(unique) < required_pairs or len(valid) != len(unique):
        return {"status": "UNCERTAIN", "protocol": PROTOCOL,
                "reason": "INSUFFICIENT_UNIQUE_VALID_PROCESS_PAIRS",
                "valid_pairs": len(valid), "required_pairs": required_pairs,
                "independent_processes": len(unique), "pairs": valid,
                "one_sided_sign_test_p_if_all_agree": 0.5 ** required_pairs,
                "familywise_upper_bound_30_candidates": min(1.0,30*(0.5**required_pairs))}
    signs = [x["difference_ms"] for x in valid]
    if len(signs) != required_pairs:
        return {"status": "UNCERTAIN", "protocol": PROTOCOL,
                "reason": "PAIR_COUNT_MUST_MATCH_FROZEN_PROTOCOL",
                "valid_pairs": len(signs), "required_pairs": required_pairs,
                "pairs": valid}
    if all(x < 0 for x in signs):
        status, reason = "CONFIRMED_IMPROVEMENT", "ALL_INDEPENDENT_PAIRED_SESSIONS_FASTER"
    elif all(x > 0 for x in signs):
        status, reason = "CONFIRMED_REGRESSION", "ALL_INDEPENDENT_PAIRED_SESSIONS_SLOWER"
    else:
        status, reason = "UNCERTAIN", "MIXED_OR_TIED_INDEPENDENT_PAIR_SIGNS"
    return {"status": status, "protocol": PROTOCOL, "reason": reason,
            "baseline_id": baseline_id, "score_version": score_version,
            "valid_pairs": len(valid), "required_pairs": required_pairs,
            "independent_processes": len(unique), "pairs": valid,
            "one_sided_sign_test_p_if_all_agree": 0.5 ** required_pairs,
            "familywise_upper_bound_30_candidates": min(1.0,30*(0.5**required_pairs))}


def assess_best_promotion(measured: dict[str, Any], previous: dict[str, Any] | None) -> tuple[str, str]:
    if not measured.get("baseline_id") or measured.get("score_version") != "N0_FROZEN_REFERENCE_SCORE_V1":
        return "UNCERTAIN", "SCORE_IDENTITY_MISSING"
    if previous is None:
        return "UNCERTAIN", "NO_COMPARABLE_INCUMBENT"
    if measured.get("baseline_id") != previous.get("baseline_id"):
        return "UNCERTAIN", "INCOMPARABLE_BASELINE"
    if measured.get("score_version") != previous.get("score_version"):
        return "UNCERTAIN", "INCOMPARABLE_SCORE_VERSION"
    recheck = measured.get("independent_process_recheck") or {}
    status = recheck.get("status")
    if status not in {"CONFIRMED_IMPROVEMENT", "CONFIRMED_REGRESSION", "UNCERTAIN"}:
        return "UNCERTAIN", "INDEPENDENT_PROCESS_RECHECK_REQUIRED"
    if (recheck.get("baseline_id") != measured.get("baseline_id")
            or recheck.get("score_version") != measured.get("score_version")
            or recheck.get("protocol") != PROTOCOL
            or recheck.get("independent_processes", 0) < REQUIRED_INDEPENDENT_PAIRS
            or recheck.get("familywise_upper_bound_30_candidates", 1.0) > 0.05):
        return "UNCERTAIN", "RECHECK_IDENTITY_OR_SAMPLE_COUNT_INVALID"
    verified = classify_paired_recheck(recheck.get("pairs") or [],
        baseline_id=measured.get("baseline_id"), score_version=measured.get("score_version"))
    if verified.get("status") != status:
        return "UNCERTAIN", "RECHECK_RAW_PAIR_RECOMPUTATION_MISMATCH"
    return status, recheck.get("reason", "RECHECK_STATUS")
