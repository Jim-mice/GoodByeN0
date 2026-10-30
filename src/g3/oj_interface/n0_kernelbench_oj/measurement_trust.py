"""Raw KernelBench timer capture and independently recomputed Reference gate."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import inspect
import math
import statistics
import uuid

TIMER_PROTOCOL = {
    "method": "cuda_event",
    "warmup": 3,
    "trials_after_discard": 10,
    "discard_first": 1,
    "l2_clear": "PINNED_KERNELBENCH_TIMING_IMPLEMENTATION",
}
MAX_CONTROL_TO_EVALUATOR_RELATIVE_DIFFERENCE = 0.05


@contextmanager
def capture_timer_calls(timing_module):
    """Capture returned trial samples while delegating unchanged to upstream."""
    original_getter = timing_module.get_timing_function
    calls = []

    def instrumented_getter(method="cuda_event"):
        function = original_getter(method)
        signature = inspect.signature(function)

        def instrumented(*args, **kwargs):
            bound = signature.bind_partial(*args, **kwargs)
            bound.apply_defaults()
            started = datetime.now(timezone.utc).isoformat()
            samples = function(*args, **kwargs)
            calls.append({
                "capture_id": uuid.uuid4().hex,
                "captured_at_utc": started,
                "timer": method,
                "effective_parameters": {
                    "num_warmup": int(bound.arguments.get("num_warmup", 3)),
                    "num_trials": int(bound.arguments.get("num_trials", 10)),
                    "discard_first": int(bound.arguments.get("discard_first", 1)),
                },
                "samples_after_discard_ms": [float(value) for value in samples],
            })
            return samples

        return instrumented

    timing_module.get_timing_function = instrumented_getter
    try:
        yield calls
    finally:
        timing_module.get_timing_function = original_getter


def summarize_samples(samples):
    values = [float(value) for value in samples]
    if len(values) != TIMER_PROTOCOL["trials_after_discard"] or any(not math.isfinite(x) or x <= 0 for x in values):
        raise ValueError("INVALID_RAW_TIMER_SAMPLES")
    mean = statistics.mean(values)
    return {
        "sample_count": len(values),
        "mean_ms": mean,
        "population_stddev_ms": statistics.pstdev(values),
        "cv": statistics.pstdev(values) / mean,
        "min_ms": min(values),
        "max_ms": max(values),
    }


def build_timer_evidence(calls, *, timing_source_sha256, role):
    if len(calls) != 1:
        raise ValueError("UNEXPECTED_TIMER_CALL_COUNT")
    call = calls[0]
    if call["timer"] != TIMER_PROTOCOL["method"] or call["effective_parameters"] != {
        "num_warmup": TIMER_PROTOCOL["warmup"],
        "num_trials": TIMER_PROTOCOL["trials_after_discard"],
        "discard_first": TIMER_PROTOCOL["discard_first"],
    }:
        raise ValueError("TIMER_PROTOCOL_IDENTITY_MISMATCH")
    samples = call["samples_after_discard_ms"]
    return {
        "schema": "N0_RAW_TIMER_EVIDENCE_V1",
        "capture_id": call["capture_id"],
        "captured_at_utc": call["captured_at_utc"],
        "role": role,
        "protocol": dict(TIMER_PROTOCOL),
        "timing_source_sha256": timing_source_sha256,
        "raw_samples_ms": samples,
        "summary": summarize_samples(samples),
    }


def assess_reference_path_agreement(control_evidence, evaluator_evidence):
    """Recompute the frozen 5% control/evaluator gate from raw samples.

    Relative difference uses the evaluator Reference mean as denominator, matching
    the existing R4 qualification report. This function never changes score math.
    """
    try:
        if not isinstance(control_evidence.get("capture_id"), str) or not control_evidence.get("capture_id"):
            raise ValueError("CONTROL_CAPTURE_ID_MISSING")
        if not isinstance(evaluator_evidence.get("capture_id"), str) or not evaluator_evidence.get("capture_id"):
            raise ValueError("EVALUATOR_CAPTURE_ID_MISSING")
        if control_evidence["capture_id"] == evaluator_evidence["capture_id"]:
            raise ValueError("DUPLICATE_REFERENCE_CAPTURE_ID")
        control = summarize_samples(control_evidence["raw_samples_ms"])
        evaluator = summarize_samples(evaluator_evidence["raw_samples_ms"])
        if control_evidence["protocol"] != TIMER_PROTOCOL or evaluator_evidence["protocol"] != TIMER_PROTOCOL:
            raise ValueError("TIMER_PROTOCOL_IDENTITY_MISMATCH")
        if control_evidence["timing_source_sha256"] != evaluator_evidence["timing_source_sha256"]:
            raise ValueError("TIMER_SOURCE_IDENTITY_MISMATCH")
        difference = abs(control["mean_ms"] - evaluator["mean_ms"])
        relative = difference / evaluator["mean_ms"]
        return {
            "policy": "N0_REFERENCE_CONTROL_AGREEMENT_V1",
            "limit": MAX_CONTROL_TO_EVALUATOR_RELATIVE_DIFFERENCE,
            "control_mean_ms_recomputed": control["mean_ms"],
            "control_cv_within_call": control["cv"],
            "evaluator_reference_mean_ms_recomputed": evaluator["mean_ms"],
            "evaluator_reference_cv_within_call": evaluator["cv"],
            "relative_difference": relative,
            "pass": relative <= MAX_CONTROL_TO_EVALUATOR_RELATIVE_DIFFERENCE,
        }
    except (KeyError, TypeError, ValueError, OverflowError, ZeroDivisionError) as exc:
        return {"policy": "N0_REFERENCE_CONTROL_AGREEMENT_V1", "pass": False,
                "failure": str(exc), "limit": MAX_CONTROL_TO_EVALUATOR_RELATIVE_DIFFERENCE}


def assess_against_frozen_baseline(evidence, frozen_baseline):
    """Compare a live 10-sample Reference with an empirical calibration band."""
    try:
        stats = summarize_samples(evidence["raw_samples_ms"])
        reference_ms = float(frozen_baseline["runtime_stats_ms"]["mean"])
        limit = float(frozen_baseline["monitoring"]["relative_deviation_p99"])
        relative = abs(stats["mean_ms"] - reference_ms) / reference_ms
        if not math.isfinite(limit) or limit <= 0:
            raise ValueError("FROZEN_BASELINE_MONITOR_LIMIT_INVALID")
        return {"policy": "N0_EMPIRICAL_FROZEN_REFERENCE_MONITOR_V1",
                "baseline_id": frozen_baseline["baseline_id"],
                "live_mean_ms": stats["mean_ms"], "baseline_mean_ms": reference_ms,
                "relative_deviation": relative, "empirical_p99_limit": limit,
                "pass": relative <= limit}
    except (KeyError, TypeError, ValueError, OverflowError, ZeroDivisionError) as exc:
        return {"policy": "N0_EMPIRICAL_FROZEN_REFERENCE_MONITOR_V1",
                "pass": False, "failure": str(exc)}
