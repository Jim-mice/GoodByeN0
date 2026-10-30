"""KernelBench L1 #40 adapter. No GPU code runs unless an approved gate is supplied."""
from __future__ import annotations

import hashlib
import importlib
import json
import math
import os
import platform
import sys
import subprocess
from pathlib import Path
from typing import Any
from .cuda_only import validate_cuda_candidate, CUDAOnlyError
from .cuda_only import load_fixed_extension
from .measurement_trust import (capture_timer_calls, build_timer_evidence,
                                assess_reference_path_agreement, assess_against_frozen_baseline)


REPOSITORY_COMMIT = "423217d9fda91e0c2d67e4a43bf62f96f6d104f1"
TASK_RELATIVE = Path("KernelBench/level1/40_LayerNorm.py")
TASK_SHA256 = "8D975D7CEE6946513DCFB6BA84C971E53A890F9D9C6F0A4FB828904941F25975"
EVAL_RELATIVE = Path("src/kernelbench/eval.py")
EVAL_SHA256 = "B42CE1268EC64EE7274DB307AB98945C1E868792B54689554014B28C41787EE6"
SCORE_RELATIVE = Path("src/kernelbench/score.py")
SCORE_SHA256 = "1154098FAD9BEE20CD3DE6A946E61FEB3F08DC3F56356358F57E24678EDF8A84"
TIMING_RELATIVE = Path("src/kernelbench/timing.py")
TIMING_SHA256 = "EAABDAD5DD042CFAAC7159713B5F0C17A9244E65622B504EB3CA909D780854C5"
KERNELBENCH_FP32_TOLERANCE = 1e-4


class OJGateError(RuntimeError):
    pass


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest().upper()


def verify_upstream_source(repository_root: Path) -> dict[str, Any]:
    root = Path(repository_root)
    commit = subprocess.check_output(["git", "-C", str(root), "rev-parse", "HEAD"], text=True).strip()
    if commit != REPOSITORY_COMMIT:
        raise OJGateError("KERNELBENCH_COMMIT_MISMATCH")
    status = subprocess.check_output(["git", "-C", str(root), "status", "--porcelain"], text=True).strip()
    if status:
        raise OJGateError("KERNELBENCH_WORKTREE_DIRTY")
    hashes = {
        "task": hashlib.sha256(subprocess.check_output(["git", "-C", str(root), "show", f"HEAD:{TASK_RELATIVE.as_posix()}"])).hexdigest().upper(),
        "eval": hashlib.sha256(subprocess.check_output(["git", "-C", str(root), "show", f"HEAD:{EVAL_RELATIVE.as_posix()}"])).hexdigest().upper(),
        "score": hashlib.sha256(subprocess.check_output(["git", "-C", str(root), "show", f"HEAD:{SCORE_RELATIVE.as_posix()}"])).hexdigest().upper(),
        "timing": hashlib.sha256(subprocess.check_output(["git", "-C", str(root), "show", f"HEAD:{TIMING_RELATIVE.as_posix()}"])).hexdigest().upper(),
    }
    expected = {"task": TASK_SHA256, "eval": EVAL_SHA256, "score": SCORE_SHA256, "timing": TIMING_SHA256}
    if hashes != expected:
        raise OJGateError("KERNELBENCH_SOURCE_HASH_MISMATCH")
    return {"commit": commit, "hashes": hashes, "status": "PASS"}


def validate_candidate_syntax(candidate_source: str) -> dict[str, Any]:
    """CPU-only CUDA source boundary; does not import or execute Candidate code."""
    try:
        result = validate_cuda_candidate(candidate_source)
    except CUDAOnlyError as exc:
        return {"status":"INVALID_SUBMISSION","reason":str(exc)}
    return {**result,"status":"CPU_SYNTAX_PASS"}


def classify_evaluator_result(*, compiled: bool, correctness: bool, metadata: dict[str, Any], runtime_ms: Any) -> str:
    """Keep evaluator/runtime faults out of Candidate scientific failure labels."""
    if not compiled:
        if metadata.get("compilation_error_name") in {"SyntaxError", "CandidateSyntaxError"}:
            return "CANDIDATE_COMPILE_FAILURE"
        return "OJ_INFRASTRUCTURE_HOLD_COMPILATION_ATTRIBUTION_UNKNOWN"
    if "runtime_error" in metadata or "error_during_performance" in metadata:
        return "OJ_INFRASTRUCTURE_HOLD_ATTRIBUTION_UNKNOWN"
    if not correctness:
        if any(k in metadata for k in ("correctness_issue", "correctness_issue_name", "max_difference")):
            return "CANDIDATE_CORRECTNESS_FAILURE"
        return "OJ_INFRASTRUCTURE_HOLD_ATTRIBUTION_UNKNOWN"
    if runtime_ms is None:
        return "OJ_INFRASTRUCTURE_HOLD_PERFORMANCE_RESULT_MISSING"
    return "VALID_MEASUREMENT"


def layernorm_reference_numpy(x, weight=None, bias=None, eps: float = 1e-5,
                             normalized_shape: tuple[int, ...] = (64, 256, 256)):
    """Small CPU semantic reference for tests only; never a formal result."""
    import numpy as np
    x = np.asarray(x, dtype=np.float32)
    if len(normalized_shape) > x.ndim or tuple(x.shape[-len(normalized_shape):]) != tuple(normalized_shape):
        raise ValueError("input trailing dimensions must equal normalized_shape")
    axes = tuple(range(x.ndim - len(normalized_shape), x.ndim))
    mean = np.mean(x, axis=axes, keepdims=True, dtype=np.float32)
    var = np.mean((x - mean) * (x - mean), axis=axes, keepdims=True, dtype=np.float32)
    y = (x - mean) / np.sqrt(var + np.float32(eps))
    if weight is None:
        weight = np.ones(normalized_shape, dtype=np.float32)
    if bias is None:
        bias = np.zeros(normalized_shape, dtype=np.float32)
    return y * np.asarray(weight, dtype=np.float32) + np.asarray(bias, dtype=np.float32)


def _require_gate(qualification: dict[str, Any], hardware_identity: dict[str, Any]) -> None:
    if qualification.get("status") != "APPROVED_FOR_N0_GPU_QUALIFICATION":
        raise OJGateError("GPU_QUALIFICATION_NOT_APPROVED")
    if qualification.get("task_sha256", "").upper() != TASK_SHA256 or qualification.get("kernelbench_commit") != REPOSITORY_COMMIT:
        raise OJGateError("QUALIFICATION_TASK_IDENTITY_MISMATCH")
    expected_gpu = qualification.get("gpu_identity")
    if not expected_gpu or hardware_identity != expected_gpu:
        raise OJGateError("GPU_IDENTITY_MISMATCH")
    if qualification.get("driver_health") != "PASS" or qualification.get("oj_worker_health") != "PASS":
        raise OJGateError("GPU_OR_OJ_HEALTH_GATE_FAILED")


def generate_reference_baseline(*, repository_root: Path, qualification: dict[str, Any],
                                hardware_identity: dict[str, Any], device: int = 0) -> dict[str, Any]:
    """GPU-only operation; caller must have completed explicit qualification."""
    _require_gate(qualification, hardware_identity)
    upstream = verify_upstream_source(repository_root)
    frozen_path = Path(__file__).resolve().parent.parent / "REFERENCE_BASELINE.json"
    if not frozen_path.is_file():
        raise OJGateError("FROZEN_REFERENCE_BASELINE_MISSING")
    frozen = json.loads(frozen_path.read_text(encoding="utf-8"))
    if (frozen.get("status") != "QUALIFIED_FROZEN_BASELINE" or
            frozen.get("task_sha256", "").upper() != TASK_SHA256 or
            frozen.get("kernelbench_commit") != REPOSITORY_COMMIT or
            frozen.get("gpu_identity") != hardware_identity or
            frozen.get("timing_source_sha256") != upstream["hashes"]["timing"]):
        raise OJGateError("FROZEN_REFERENCE_BASELINE_IDENTITY_MISMATCH")
    import torch
    if not torch.cuda.is_available():
        raise OJGateError("CUDA_UNAVAILABLE")
    if torch.cuda.get_device_name(device) != hardware_identity.get("device_name"):
        raise OJGateError("LIVE_GPU_IDENTITY_MISMATCH")
    runtime_environment = {"python_version": platform.python_version(),
                           "torch_version": str(torch.__version__),
                           "torch_cuda_version": str(torch.version.cuda),
                           "kernelbench_commit": REPOSITORY_COMMIT,
                           "timing_source_sha256": upstream["hashes"]["timing"]}
    if any((frozen.get("environment_identity") or {}).get(key) != value for key, value in runtime_environment.items()):
        raise OJGateError("FROZEN_REFERENCE_BASELINE_ENVIRONMENT_MISMATCH")
    sys.path.insert(0, str(Path(repository_root) / "src"))
    timing = importlib.import_module("kernelbench.timing")
    reference_source = (Path(repository_root) / TASK_RELATIVE).read_text(encoding="utf-8")
    with capture_timer_calls(timing) as timer_calls:
        stats = timing.measure_ref_program_time(
            ref_arch_name="40_LayerNorm.py", ref_arch_src=reference_source,
            num_warmup=3, num_trials=10, discard_first=1, timing_method="cuda_event",
            use_torch_compile=False, device=device, precision="fp32", verbose=False,
        )
    try:
        timing_evidence = build_timer_evidence(timer_calls, timing_source_sha256=upstream["hashes"]["timing"],
                                               role="INDEPENDENT_REFERENCE_CONTROL")
    except (KeyError, TypeError, ValueError) as exc:
        raise OJGateError("REFERENCE_CONTROL_RAW_TIMING_EVIDENCE_INVALID:" + str(exc)) from exc
    return {"status":"BASELINE_MEASURED","task_sha256":TASK_SHA256,"kernelbench_commit":REPOSITORY_COMMIT,
            "gpu_identity":hardware_identity,"timing_method":"cuda_event","warmup":3,"trials":10,
            "discard_first":1,"runtime_stats_ms":stats,"measurement_evidence":timing_evidence,
            "frozen_baseline":frozen,
            "baseline_sha256":frozen["baseline_id"],
            "environment_identity":runtime_environment}



def _matches_kernelbench_mean(raw_mean_ms: float, published_mean_ms: float) -> bool:
    """Pinned KernelBench formats its published mean via float(f'{np.mean(x):.3g}').

    The raw evidence is independently recomputed from the same CUDA Event samples.
    A fixed 0.1% relative tolerance rejects some legitimately rounded values.
    """
    try:
        raw = float(raw_mean_ms)
        published = float(published_mean_ms)
        return (math.isfinite(raw) and raw > 0.0 and
                math.isfinite(published) and published > 0.0 and
                float(f"{raw:.3g}") == published)
    except (ValueError, TypeError, OverflowError):
        return False


def precompile_candidate_extension(*, candidate_source: str, repository_root: Path,
                                   qualification: dict[str, Any], hardware_identity: dict[str, Any],
                                   device: int = 0) -> dict[str, Any]:
    """Build candidate before an independent Reference Control timing window.

    Call only while holding the same GPU lease used by the evaluator.
    Does not qualify timing, consume a Candidate budget, or produce an OJ score.
    Does not change pinned nvcc flags or KernelBench sources.
    """
    _require_gate(qualification, hardware_identity)
    verify_upstream_source(repository_root)
    syntax = validate_candidate_syntax(candidate_source)
    if syntax["status"] != "CPU_SYNTAX_PASS":
        return {"status": "SKIPPED_SYNTAX_REJECTED", "syntax": syntax}
    import torch
    if not torch.cuda.is_available():
        raise OJGateError("CUDA_UNAVAILABLE")
    if torch.cuda.get_device_name(device) != hardware_identity.get("device_name"):
        raise OJGateError("LIVE_GPU_IDENTITY_MISMATCH")
    release_root = Path(__file__).resolve().parent
    nvcc = Path(r"C:\Program Files\NVIDIA GPU Computing Toolkit\CUDA\v13.4\bin\nvcc.exe")
    contract = json.loads((release_root.parent / "CUDA_COMPILER_CONTRACT.json").read_text(encoding="utf-8"))
    contract_sha256 = sha256_file(release_root.parent / "CUDA_COMPILER_CONTRACT.json")
    if contract.get("status") != "FROZEN_PENDING_QUALIFICATION" or qualification.get("compiler_contract_sha256") != contract_sha256:
        raise OJGateError("CUDA_COMPILER_CONTRACT_IDENTITY_OR_QUALIFICATION_MISMATCH")
    module, build = load_fixed_extension(candidate_source=candidate_source,
        binding_source=release_root / "binding.cpp",
        build_root=Path(r"D:\projects\kernelagent\runtime\tmp\n0_cuda_build"),
        nvcc=nvcc, expected_nvcc_sha256=contract["toolkit"]["nvcc_sha256"],
        expected_ptxas_sha256=contract["toolkit"]["ptxas_sha256"])
    return {"status": "PRECOMPILED", "candidate_source_sha256": syntax["source_sha256"],
            "compiler_contract_sha256": contract_sha256,
            "build_identity": build, "module": module}


def evaluate_candidate(*, candidate_source: str, repository_root: Path, qualification: dict[str, Any],
                       hardware_identity: dict[str, Any], baseline: dict[str, Any], device: int = 0,
                       prepared_extension: dict[str, Any] | None = None) -> dict[str, Any]:
    """Run the pinned upstream KernelBench evaluator, without importing V7 OJ scoring."""
    _require_gate(qualification, hardware_identity)
    upstream = verify_upstream_source(repository_root)
    syntax = validate_candidate_syntax(candidate_source)
    if syntax["status"] != "CPU_SYNTAX_PASS":
        return {"status":syntax["status"],"syntax":syntax,"task_sha256":TASK_SHA256}
    if baseline.get("status") != "BASELINE_MEASURED" or baseline.get("task_sha256", "").upper() != TASK_SHA256 or baseline.get("gpu_identity") != hardware_identity:
        raise OJGateError("REFERENCE_BASELINE_MISSING_OR_IDENTITY_MISMATCH")
    frozen = baseline.get("frozen_baseline")
    if (not isinstance(frozen, dict) or frozen.get("status") != "QUALIFIED_FROZEN_BASELINE" or
            not isinstance(frozen.get("baseline_id"), str) or not frozen.get("baseline_id") or
            not isinstance(frozen.get("runtime_stats_ms"), dict) or
            not math.isfinite(float(frozen["runtime_stats_ms"].get("mean", 0))) or
            float(frozen["runtime_stats_ms"].get("mean", 0)) <= 0):
        raise OJGateError("FROZEN_REFERENCE_BASELINE_INVALID")
    control_evidence = baseline.get("measurement_evidence")
    if not isinstance(control_evidence, dict) or control_evidence.get("schema") != "N0_RAW_TIMER_EVIDENCE_V1":
        return {"status":"OJ_INFRASTRUCTURE_HOLD_REFERENCE_EVIDENCE_MISSING","alignment":"KernelBench-derived",
                "candidate_source_sha256":syntax["source_sha256"],"task_sha256":TASK_SHA256}
    if control_evidence.get("timing_source_sha256") != TIMING_SHA256:
        return {"status":"OJ_INFRASTRUCTURE_HOLD_REFERENCE_EVIDENCE_IDENTITY_MISMATCH","alignment":"KernelBench-derived",
                "candidate_source_sha256":syntax["source_sha256"],"task_sha256":TASK_SHA256}
    import torch
    if not torch.cuda.is_available():
        raise OJGateError("CUDA_UNAVAILABLE")
    if torch.cuda.get_device_name(device) != hardware_identity.get("device_name"):
        raise OJGateError("LIVE_GPU_IDENTITY_MISMATCH")
    sys.path.insert(0, str(Path(repository_root) / "src"))
    evaluator = importlib.import_module("kernelbench.eval")
    reference_source = (Path(repository_root) / TASK_RELATIVE).read_text(encoding="utf-8")
    release_root = Path(__file__).resolve().parent
    nvcc = Path(r"C:\Program Files\NVIDIA GPU Computing Toolkit\CUDA\v13.4\bin\nvcc.exe")
    contract = json.loads((release_root.parent / "CUDA_COMPILER_CONTRACT.json").read_text(encoding="utf-8"))
    contract_sha256 = sha256_file(release_root.parent / "CUDA_COMPILER_CONTRACT.json")
    if contract.get("status") != "FROZEN_PENDING_QUALIFICATION" or qualification.get("compiler_contract_sha256") != contract_sha256:
        raise OJGateError("CUDA_COMPILER_CONTRACT_IDENTITY_OR_QUALIFICATION_MISMATCH")
    if prepared_extension is None:
        module, build_identity = load_fixed_extension(candidate_source=candidate_source,
            binding_source=release_root / "binding.cpp", build_root=Path(r"D:\projects\kernelagent\runtime\tmp\n0_cuda_build"),
            nvcc=nvcc, expected_nvcc_sha256=contract["toolkit"]["nvcc_sha256"],
            expected_ptxas_sha256=contract["toolkit"]["ptxas_sha256"])
    else:
        if (prepared_extension.get("status") != "PRECOMPILED" or
                prepared_extension.get("candidate_source_sha256", "").lower() != syntax["source_sha256"].lower() or
                prepared_extension.get("compiler_contract_sha256") != contract_sha256 or
                prepared_extension.get("build_identity", {}).get("binding_sha256") != sha256_file(release_root / "binding.cpp") or
                prepared_extension.get("build_identity", {}).get("wrapper_sha256") != sha256_file(release_root / "wrapper.py")):
            raise OJGateError("PRECOMPILED_EXTENSION_IDENTITY_MISMATCH")
        module = prepared_extension["module"]
        build_identity = dict(prepared_extension["build_identity"])
    build_identity["wrapper_sha256"] = sha256_file(release_root / "wrapper.py")
    build_identity["binding_sha256"] = sha256_file(release_root / "binding.cpp")
    build_identity["compiler_contract_sha256"] = contract_sha256
    os.environ["N0_EXTENSION_MODULE"] = module.__name__
    artifact_root = str(Path(build_identity["artifact_path"]).resolve().parent)
    if artifact_root not in sys.path:
        sys.path.insert(0, artifact_root)
    wrapper_source = (release_root / "wrapper.py").read_text(encoding="utf-8")
    with capture_timer_calls(importlib.import_module("kernelbench.timing")) as timer_calls:
        result = evaluator.eval_kernel_against_ref(
            original_model_src=reference_source,
            custom_model_src=wrapper_source,
            seed_num=42,
            num_correct_trials=1,
            num_perf_trials=10,
            measure_performance=True,
            timing_method="cuda_event",
            verbose=False,
            device=device,
            backend="cuda",
            precision=torch.float32,
            check_for_excessive_speedup=True,
            excessive_speedup_threshold=10,
        )
    verdict = classify_evaluator_result(compiled=bool(result.compiled),correctness=bool(result.correctness),
                                        metadata=result.metadata or {},runtime_ms=result.runtime)
    if verdict != "VALID_MEASUREMENT":
        return {"status":verdict,"alignment":"KernelBench-derived","task_sha256":TASK_SHA256,
                "candidate_source_sha256":syntax["source_sha256"],"gpu_identity":hardware_identity,
                "build_identity":build_identity,
                "compiled":bool(result.compiled),"correctness":bool(result.correctness),
                "official_evaluator_metadata":result.metadata,"measurement":None,"upstream_identity":upstream}
    candidate_ms = float(result.runtime)
    baseline_ms = float(frozen["runtime_stats_ms"]["mean"])
    evaluator_reference_ms = float(result.ref_runtime) if result.ref_runtime is not None else None
    if evaluator_reference_ms is None or evaluator_reference_ms <= 0:
        return {"status":"OJ_INFRASTRUCTURE_HOLD_REFERENCE_TIMING_MISSING","alignment":"KernelBench-derived",
                "candidate_source_sha256":syntax["source_sha256"],"build_identity":build_identity,
                "official_kernelbench_result":{"compiled":bool(result.compiled),"correctness":bool(result.correctness),
                                                "candidate_runtime_ms":candidate_ms,"reference_runtime_ms":result.ref_runtime}}
    if len(timer_calls) != 2:
        return {"status":"OJ_INFRASTRUCTURE_HOLD_RAW_TIMER_CALL_COUNT","alignment":"KernelBench-derived",
                "candidate_source_sha256":syntax["source_sha256"],"build_identity":build_identity,
                "official_kernelbench_result":{"compiled":bool(result.compiled),"correctness":bool(result.correctness),
                                                "candidate_runtime_ms":candidate_ms,"reference_runtime_ms":result.ref_runtime},
                "timer_call_count":len(timer_calls)}
    try:
        candidate_evidence = build_timer_evidence([timer_calls[0]], timing_source_sha256=upstream["hashes"]["timing"],
                                                  role="EVALUATOR_CANDIDATE")
        evaluator_reference_evidence = build_timer_evidence([timer_calls[1]], timing_source_sha256=upstream["hashes"]["timing"],
                                                            role="EVALUATOR_REFERENCE")
        if not _matches_kernelbench_mean(candidate_evidence["summary"]["mean_ms"], candidate_ms):
            raise ValueError("CANDIDATE_SUMMARY_RAW_SAMPLE_MISMATCH")
        if not _matches_kernelbench_mean(evaluator_reference_evidence["summary"]["mean_ms"], evaluator_reference_ms):
            raise ValueError("REFERENCE_SUMMARY_RAW_SAMPLE_MISMATCH")
        reference_trust = assess_reference_path_agreement(control_evidence, evaluator_reference_evidence)
        control_drift = assess_against_frozen_baseline(control_evidence, frozen)
        evaluator_drift = assess_against_frozen_baseline(evaluator_reference_evidence, frozen)
    except (KeyError, TypeError, ValueError) as exc:
        return {"status":"OJ_INFRASTRUCTURE_HOLD_RAW_TIMER_EVIDENCE_INVALID","alignment":"KernelBench-derived",
                "candidate_source_sha256":syntax["source_sha256"],"build_identity":build_identity,
                "failure":str(exc),"official_kernelbench_result":{"compiled":bool(result.compiled),
                "correctness":bool(result.correctness),"candidate_runtime_ms":candidate_ms,
                "reference_runtime_ms":evaluator_reference_ms}}
    candidate_ms = float(candidate_evidence["summary"]["mean_ms"])
    evaluator_reference_ms = float(evaluator_reference_evidence["summary"]["mean_ms"])
    raw_evidence = {"schema":"N0_REFERENCE_PATH_EVIDENCE_V3","baseline_sha256":baseline.get("baseline_sha256"),
                    "baseline_id":frozen["baseline_id"],"score_version":"N0_FROZEN_REFERENCE_SCORE_ONLINE_V2",
                    "candidate":candidate_evidence,"evaluator_reference":evaluator_reference_evidence,
                    "reference_trust":reference_trust,"control_baseline_drift":control_drift,
                    "evaluator_baseline_drift":evaluator_drift}
    from n0_kernelbench_oj.score_policy import assess_candidate_sample_stability
    candidate_stability = assess_candidate_sample_stability(candidate_evidence["raw_samples_ms"])
    raw_evidence["candidate_timing_stability"] = candidate_stability
    if candidate_stability.get("pass") is not True:
        return {"status":"OJ_INFRASTRUCTURE_HOLD_CANDIDATE_TIMING_UNSTABLE",
                "alignment":"KernelBench-derived","kernelbench_commit":REPOSITORY_COMMIT,
                "task_sha256":TASK_SHA256,"candidate_source_sha256":syntax["source_sha256"],
                "gpu_identity":hardware_identity,"build_identity":build_identity,
                "measurement_evidence":raw_evidence,
                "official_kernelbench_result":{"compiled":bool(result.compiled),
                    "correctness":bool(result.correctness),"candidate_runtime_ms":candidate_ms,
                    "reference_runtime_ms":evaluator_reference_ms},
                "candidate_timing_stability":candidate_stability}
    if control_drift.get("pass") is not True or evaluator_drift.get("pass") is not True:
        return {"status":"OJ_INFRASTRUCTURE_HOLD_REFERENCE_BASELINE_DRIFT","alignment":"KernelBench-derived",
                "kernelbench_commit":REPOSITORY_COMMIT,"task_sha256":TASK_SHA256,
                "candidate_source_sha256":syntax["source_sha256"],"build_identity":build_identity,
                "measurement_evidence":raw_evidence,
                "official_kernelbench_result":{"compiled":bool(result.compiled),"correctness":bool(result.correctness),
                    "candidate_runtime_ms":candidate_ms,"reference_runtime_ms":evaluator_reference_ms}}
    if reference_trust.get("pass") is not True:
        return {"status":"OJ_INFRASTRUCTURE_HOLD_REFERENCE_CONTROL_MISMATCH","alignment":"KernelBench-derived",
                "kernelbench_commit":REPOSITORY_COMMIT,"task_sha256":TASK_SHA256,
                "candidate_source_sha256":hashlib.sha256(candidate_source.encode("utf-8")).hexdigest(),
                "build_identity":build_identity,"measurement_evidence":raw_evidence,
                "official_kernelbench_result":{"compiled":bool(result.compiled),"correctness":bool(result.correctness),
                    "candidate_runtime_ms":candidate_ms,"reference_runtime_ms":evaluator_reference_ms,
                    "runtime_stats":result.runtime_stats,"reference_runtime_stats":result.ref_runtime_stats}}
    return {
        "status":"VALID_MEASUREMENT" if candidate_ms > 0 else "OJ_INFRASTRUCTURE_HOLD_PERFORMANCE_RESULT_MISSING",
        "alignment":"KernelBench-derived",
        "kernelbench_commit":REPOSITORY_COMMIT,
        "task_sha256":TASK_SHA256,
        "evaluator_source_sha256":EVAL_SHA256,
        "candidate_source_sha256":hashlib.sha256(candidate_source.encode("utf-8")).hexdigest(),
        "build_identity":build_identity,
        "gpu_identity":hardware_identity,
        "correctness_trials":1,
        # The pinned evaluator maps torch.float32 to 1e-4 and supplies that
        # value to both atol and rtol in torch.allclose.
        "correctness_tolerance":{"atol":KERNELBENCH_FP32_TOLERANCE,"rtol":KERNELBENCH_FP32_TOLERANCE},
        "timing":{"method":"cuda_event","warmup":3,"trials":10,"discard_first":1,
                  "candidate_runtime_ms":candidate_ms,"kernelbench_reference_runtime_ms":evaluator_reference_ms,
                  "kernelbench_speedup":baseline_ms/candidate_ms if candidate_ms > 0 else None,
                  "score_version":"N0_FROZEN_REFERENCE_SCORE_ONLINE_V2","baseline_id":frozen["baseline_id"],
                  "frozen_reference_ms":baseline_ms,
                  "seed_reference_control_ms":baseline_ms,
                  "seed_control_speedup":baseline_ms/candidate_ms if candidate_ms > 0 else None},
        "official_kernelbench_result":{"compiled":bool(result.compiled),"correctness":bool(result.correctness),
                                         "metadata":result.metadata,"runtime_stats":result.runtime_stats},
        "measurement_evidence":raw_evidence,
        "upstream_identity":upstream,
        "diagnostic_profile":{"status":"SEPARATE_NOT_RUN"}
    }
