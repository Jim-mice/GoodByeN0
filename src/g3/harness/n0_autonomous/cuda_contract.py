"""CPU-only CUDA candidate boundary and frozen compiler command construction."""
from __future__ import annotations

import hashlib
import re
from pathlib import Path
from typing import Any


class CUDAContractError(ValueError):
    pass


FORBIDDEN = {
    "PYTHON_OR_BINDING": re.compile(r"\b(PYBIND11_MODULE|TORCH_LIBRARY|PYBIND11_PLUGIN)\b|torch|\bat::", re.I),
    "ALTERNATE_BACKEND": re.compile(r"\b(triton|nvrtc|tilelang|cutlass|cute)\b", re.I),
}
REQUIRED_ABI = ("n0_workspace_bytes", "n0_layernorm_forward")
PERFORMANCE_FLAGS = ("-O3", "-std=c++17", "-gencode=arch=compute_120,code=sm_120",
                     "--ftz=false", "--prec-div=true", "--prec-sqrt=true", "--fmad=true")
FORBIDDEN_FLAGS = ("--use_fast_math", "-G", "--maxrregcount")


def sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest().upper()


def _launch_args(config: str) -> list[str]:
    args=[]; start=0; depth=0
    for index,char in enumerate(config):
        if char == "(": depth += 1
        elif char == ")": depth -= 1
        elif char == "," and depth == 0:
            args.append(config[start:index].strip()); start=index+1
    args.append(config[start:].strip())
    return args


def validate_candidate_source(source: str) -> dict[str, Any]:
    if not isinstance(source, str) or not source.strip():
        raise CUDAContractError("CUDA_SOURCE_EMPTY")
    for label, pattern in FORBIDDEN.items():
        if pattern.search(source):
            raise CUDAContractError("CUDA_SOURCE_FORBIDDEN_CONSTRUCT:" + label)
    if re.search(r"\bcudaStreamCreate(?:WithFlags|WithPriority)?\s*\(", source):
        raise CUDAContractError("CUDA_SOURCE_INDEPENDENT_STREAM_FORBIDDEN")
    if not all(name in source for name in REQUIRED_ABI):
        raise CUDAContractError("CUDA_EXTENSION_ABI_ENTRYPOINT_MISSING")
    if "__global__" not in source:
        raise CUDAContractError("CUDA_KERNEL_REQUIRED")
    launches=re.findall(r"<<<(.*?)>>>",source,re.S)
    if not launches:
        raise CUDAContractError("CUDA_KERNEL_LAUNCH_REQUIRED")
    for launch in launches:
        args=_launch_args(launch)
        if len(args)!=4 or args[3]!="stream":
            raise CUDAContractError("CUDA_LAUNCH_MUST_USE_ABI_STREAM")
    names=re.findall(r"\b__global__\s+\w+\s+(\w+)\s*\(",source)
    return {"source_sha256": sha256_bytes(source.encode("utf-8")),
            "bytes": len(source.encode("utf-8")),
            "kernel_names": names, "kernel_count": len(names), "launch_expressions": launches,
            "abi": "N0_LAYERNORM_EXTENSION_ABI_V1", "status": "STATIC_PASS"}


def validate_flags(flags: list[str]) -> None:
    if tuple(flags) != PERFORMANCE_FLAGS:
        raise CUDAContractError("COMPILER_FLAGS_NOT_EXACT_FROZEN_CONTRACT")
    if any(flag == forbidden or flag.startswith(forbidden + "=") for flag in flags for forbidden in FORBIDDEN_FLAGS):
        raise CUDAContractError("FORBIDDEN_COMPILER_FLAG")


def compile_command(nvcc: Path, source: Path, output: Path, flags: list[str]) -> list[str]:
    validate_flags(flags)
    return [str(nvcc), *flags, "-c", str(source), "-o", str(output)]
