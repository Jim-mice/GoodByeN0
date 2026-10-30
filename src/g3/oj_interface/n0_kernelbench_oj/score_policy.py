"""Versioned online scoring and conservative provisional Best policy."""
from __future__ import annotations

import math
import statistics

SCORE_VERSION = "N0_FROZEN_REFERENCE_SCORE_ONLINE_V2"
PROMOTION_POLICY = "N0_ONLINE_BEST_MARGIN_1P5_PERCENT_V1"
PROMOTION_MARGIN = 0.015
MAX_CANDIDATE_EVENT_CV = 0.03


def assess_candidate_sample_stability(candidate_samples_ms: list[float]) -> dict:
    """Reject visibly unstable single-call Event batches while retaining every sample."""
    xs = [float(x) for x in candidate_samples_ms]
    if len(xs) != 10 or any(not math.isfinite(x) or x <= 0 for x in xs):
        return {"status":"INVALID_CANDIDATE_TIMING", "pass":False, "sample_count":len(xs)}
    mean = statistics.mean(xs)
    sd = statistics.stdev(xs)
    cv = sd / mean
    return {"status":"PASS" if cv <= MAX_CANDIDATE_EVENT_CV else "HOLD_CANDIDATE_TIMING_UNSTABLE",
            "pass":cv <= MAX_CANDIDATE_EVENT_CV,"cv":cv,"limit":MAX_CANDIDATE_EVENT_CV,
            "mean_ms":mean,"sample_sd_ms":sd,"sample_count":len(xs),
            "interpretation":"WITHIN_CALL_EVENT_STABILITY_ONLY; NOT_INDEPENDENT_SESSION_CONFIDENCE"}


def settle_measurement(*, baseline_id: str, frozen_reference_ms: float,
                       candidate_samples_ms: list[float], prior_best: dict | None,
                       prior_score_version: str = SCORE_VERSION) -> dict:
    """Score with one frozen numerator; ten events are one call, not ten sessions.

    The 1.5% guard is a frozen operational margin. It is deliberately larger
    than the measured short-run Candidate variation in the qualification set;
    it is not a statistical confidence interval or a claim of independent runs.
    """
    if not baseline_id or not math.isfinite(frozen_reference_ms) or frozen_reference_ms <= 0:
        return {"status": "INVALID_TIMING_BASELINE", "score": None, "promotion": False}
    xs = [float(x) for x in candidate_samples_ms]
    if len(xs) != 10 or any(not math.isfinite(x) or x <= 0 for x in xs):
        return {"status": "INVALID_CANDIDATE_TIMING", "score": None, "promotion": False}
    mean = statistics.mean(xs)
    sd = statistics.stdev(xs)
    stability = assess_candidate_sample_stability(xs)
    if not stability["pass"]:
        return {"status":"OJ_INFRASTRUCTURE_HOLD_CANDIDATE_TIMING_UNSTABLE","score":None,
                "candidate_runtime_ms":mean,"candidate_runtime_sd_ms":sd,
                "candidate_event_cv":stability.get("cv"),"candidate_timing_stability":stability,
                "promotion":False,"promotion_status":"UNCERTAIN"}
    result = {"status": "VALID_MEASUREMENT", "score": frozen_reference_ms / mean,
              "score_version": SCORE_VERSION, "baseline_id": baseline_id,
              "candidate_runtime_ms": mean, "candidate_runtime_sd_ms": sd,
              "candidate_event_cv": sd / mean, "measurement_confidence": "SINGLE_CALL_EVENT_REPEATABILITY_ONLY",
              "candidate_timing_stability":stability,
              "promotion": False, "promotion_status": "INITIAL_BASELINE" if prior_best is None else "NOT_PROMOTED",
              "promotion_policy": PROMOTION_POLICY, "promotion_margin_fraction": PROMOTION_MARGIN}
    if prior_best is None:
        result.update(promotion=True, promotion_status="INITIAL_ONLINE_BEST", best_status="ONLINE_BASELINE")
        return result
    if prior_score_version != SCORE_VERSION or prior_best.get("score_version") != SCORE_VERSION:
        result.update(status="OJ_INFRASTRUCTURE_HOLD_SCORE_VERSION_MISMATCH", score=None,
                      promotion_status="INCOMPARABLE_SCORE_VERSION")
        return result
    if prior_best.get("baseline_id") != baseline_id:
        result.update(status="OJ_INFRASTRUCTURE_HOLD_BASELINE_MISMATCH", score=None,
                      promotion_status="INCOMPARABLE_BASELINE")
        return result
    prior_runtime = prior_best.get("candidate_runtime_ms")
    if not isinstance(prior_runtime, (int, float)) or not math.isfinite(float(prior_runtime)) or prior_runtime <= 0:
        result.update(status="OJ_INFRASTRUCTURE_HOLD_PRIOR_RUNTIME_INVALID", score=None,
                      promotion_status="PRIOR_RUNTIME_INVALID")
        return result
    ratio = mean / float(prior_runtime)
    if ratio <= 1.0 - PROMOTION_MARGIN:
        result.update(promotion=True, promotion_status="ONLINE_PROVISIONAL_BEST",
                      best_status="PROVISIONAL_IMPROVEMENT", promotion_runtime_ratio=ratio)
    elif ratio >= 1.0 + PROMOTION_MARGIN:
        result.update(promotion_status="CONFIRMED_REGRESSION", best_status="UNCHANGED",
                      promotion_runtime_ratio=ratio)
    else:
        result.update(promotion_status="UNCERTAIN", best_status="UNCHANGED",
                      promotion_runtime_ratio=ratio)
    return result
