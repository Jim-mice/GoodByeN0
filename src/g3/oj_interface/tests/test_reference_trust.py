from types import SimpleNamespace

import pytest

from n0_kernelbench_oj.measurement_trust import (
    TIMER_PROTOCOL,
    assess_reference_path_agreement,
    build_timer_evidence,
    capture_timer_calls,
    summarize_samples,
)


def evidence(samples, role="REFERENCE"):
    calls = [{
        "capture_id": role,
        "captured_at_utc": "2026-10-20T00:00:00+00:00",
        "timer": "cuda_event",
        "effective_parameters": {"num_warmup": 3, "num_trials": 10, "discard_first": 1},
        "samples_after_discard_ms": list(samples),
    }]
    return build_timer_evidence(calls, timing_source_sha256="a" * 64, role=role)


def test_reference_agreement_within_five_percent_passes():
    control = evidence([10.0] * 10, "CONTROL")
    evaluator = evidence([10.5] * 10, "EVALUATOR")
    assert assess_reference_path_agreement(control, evaluator)["pass"] is True


def test_reference_agreement_holds_above_five_percent():
    control = evidence([10.0] * 10, "CONTROL")
    evaluator = evidence([10.6] * 10, "EVALUATOR")
    gate = assess_reference_path_agreement(control, evaluator)
    assert gate["pass"] is False
    assert gate["limit"] == 0.05


def test_malformed_or_nonfinite_samples_are_rejected():
    with pytest.raises(ValueError, match="INVALID_RAW_TIMER_SAMPLES"):
        summarize_samples([1.0] * 9)
    with pytest.raises(ValueError, match="INVALID_RAW_TIMER_SAMPLES"):
        summarize_samples([1.0] * 9 + [float("nan")])


def test_timer_capture_delegates_and_restores_original_getter():
    original_function = lambda fn, args, num_warmup=3, num_trials=10, discard_first=1: [2.0] * 10
    module = SimpleNamespace(get_timing_function=lambda method="cuda_event": original_function)
    original_getter = module.get_timing_function
    with capture_timer_calls(module) as calls:
        values = module.get_timing_function("cuda_event")(lambda: None, [], num_trials=10)
    assert values == [2.0] * 10
    assert len(calls) == 1
    assert calls[0]["effective_parameters"] == {"num_warmup": 3, "num_trials": 10, "discard_first": 1}
    assert module.get_timing_function is original_getter


def test_timer_capture_restores_getter_on_upstream_error():
    def fail(*args, **kwargs):
        raise RuntimeError("upstream timing failed")
    module = SimpleNamespace(get_timing_function=lambda method="cuda_event": fail)
    original_getter = module.get_timing_function
    with pytest.raises(RuntimeError, match="upstream timing failed"):
        with capture_timer_calls(module):
            module.get_timing_function("cuda_event")(lambda: None, [])
    assert module.get_timing_function is original_getter


def test_timer_evidence_requires_frozen_protocol_and_source_identity():
    control = evidence([10.0] * 10, "CONTROL")
    altered = dict(evidence([10.0] * 10, "EVALUATOR"))
    altered["timing_source_sha256"] = "b" * 64
    assert assess_reference_path_agreement(control, altered)["pass"] is False
    assert TIMER_PROTOCOL["trials_after_discard"] == 10


def test_same_reference_capture_cannot_masquerade_as_independent_control():
    control = evidence([10.0] * 10, "CONTROL")
    evaluator = dict(control)
    evaluator["role"] = "EVALUATOR"
    assert assess_reference_path_agreement(control, evaluator)["pass"] is False
