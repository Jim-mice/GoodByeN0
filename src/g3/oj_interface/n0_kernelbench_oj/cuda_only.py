"""Static CUDA-only source gate and deterministic extension build plan."""
from __future__ import annotations

import hashlib
import json
import os
import re
import sys
from pathlib import Path
from typing import Any


PERFORMANCE_FLAGS = ["-O3", "-std=c++17", "-gencode=arch=compute_120,code=sm_120",
                     "--ftz=false", "--prec-div=true", "--prec-sqrt=true", "--fmad=true"]
FORBIDDEN = [re.compile(r"\b(PYBIND11_MODULE|TORCH_LIBRARY|PYBIND11_PLUGIN)\b|torch|\bat::", re.I),
             re.compile(r"\b(triton|nvrtc|tilelang|cutlass|cute)\b", re.I),
             re.compile(r"\b(system|popen|_popen|CreateProcess)\s*\(")]


class CUDAOnlyError(ValueError):
    pass


_CUDA_DECL_ATTRIBUTES = {
    "__launch_bounds__", "__align__", "__attribute__", "__declspec",
    "__cluster_dims__", "__maxnreg__", "__min_warps_per_eu__",
}


def _mask_cpp_comments_and_literals(source: str) -> str:
    """Mask comments and literals while preserving offsets and newlines."""
    pattern = re.compile(r"//[^\r\n]*|/\*[\s\S]*?\*/|\"(?:\\.|[^\"\\])*\"|'(?:\\.|[^'\\])*'")
    def mask(match: re.Match[str]) -> str:
        return "".join("\n" if ch == "\n" else "\r" if ch == "\r" else " " for ch in match.group(0))
    return pattern.sub(mask, source)


def extract_cuda_kernel_names(source: str) -> dict[str, list[str]]:
    """Return declared CUDA global names and syntactic launch-site names.

    CUDA declaration attributes such as ``__launch_bounds__(N)`` are calls in
    the declaration grammar, so a simple ``__global__ <type> <name>`` regex
    mistakes the attribute for the function name. This bounded scanner records
    top-level call-like identifiers between each ``__global__`` and its body,
    skips known declaration attributes, and takes the final declaration name.
    """
    text = _mask_cpp_comments_and_literals(source)
    token_re = re.compile(r"[A-Za-z_]\w*|[(){};]")
    declared: list[str] = []
    for marker in re.finditer(r"\b__global__\b", text):
        tail = text[marker.end():]
        tokens = list(token_re.finditer(tail))
        depth = 0
        call_names: list[str] = []
        for index, token in enumerate(tokens):
            value = token.group(0)
            if value == "(" and depth == 0:
                previous = tokens[index - 1].group(0) if index else ""
                if re.fullmatch(r"[A-Za-z_]\w*", previous):
                    call_names.append(previous)
                depth = 1
            elif value == "(":
                depth += 1
            elif value == ")":
                depth = max(0, depth - 1)
            elif depth == 0 and value in ("{", ";"):
                break
        names = [name for name in call_names if name not in _CUDA_DECL_ATTRIBUTES]
        if names:
            declared.append(names[-1])
    launch_names = re.findall(r"\b([A-Za-z_]\w*)\s*<<<", text)
    return {
        "kernel_names": list(dict.fromkeys(declared)),
        "launch_kernel_names": list(dict.fromkeys(launch_names)),
    }


def validate_cuda_candidate(source: str) -> dict[str, Any]:
    if not isinstance(source, str) or not source.strip() or len(source.encode("utf-8")) > 200000:
        raise CUDAOnlyError("CUDA_SOURCE_EMPTY_OR_TOO_LARGE")
    if any(p.search(source) for p in FORBIDDEN):
        raise CUDAOnlyError("FORBIDDEN_BACKEND_OR_BUILD_ESCAPE")
    allowed_headers = {"cuda_runtime.h", "cstdint", "cstddef", "cmath", "math.h", "stdint.h", "stddef.h", "float.h"}
    for header in re.findall(r"^\s*#\s*include\s*[<\"]([^>\"]+)[>\"]", source, re.M):
        if header not in allowed_headers:
            raise CUDAOnlyError("CUDA_HEADER_NOT_ALLOWLISTED:" + header)
    if re.search(r"\bcudaStreamCreate(?:WithFlags|WithPriority)?\s*\(", source):
        raise CUDAOnlyError("INDEPENDENT_CUDA_STREAM_FORBIDDEN")
    code = _mask_cpp_comments_and_literals(source)
    if "__global__" not in code or "n0_workspace_bytes" not in code or "n0_layernorm_forward" not in code:
        raise CUDAOnlyError("CUDA_KERNEL_OR_ABI_ENTRYPOINT_MISSING")
    launches=re.findall(r"<<<(.*?)>>>",code,re.S)
    for launch in launches:
        args=[]; start=0; depth=0
        for index,char in enumerate(launch):
            if char == "(": depth += 1
            elif char == ")": depth -= 1
            elif char == "," and depth == 0:
                args.append(launch[start:index].strip()); start=index+1
        args.append(launch[start:].strip())
        if len(args)!=4 or args[3]!="stream": raise CUDAOnlyError("CUDA_LAUNCH_MUST_USE_ABI_STREAM")
    if not launches: raise CUDAOnlyError("CUDA_KERNEL_LAUNCH_REQUIRED")
    kernel_info = extract_cuda_kernel_names(code)
    names = kernel_info["kernel_names"]
    launch_names = kernel_info["launch_kernel_names"]
    if not names or not launch_names or any(name not in names for name in launch_names):
        raise CUDAOnlyError("CUDA_KERNEL_DECLARATION_OR_LAUNCH_NAME_MISMATCH")
    return {"source_sha256":hashlib.sha256(source.encode("utf-8")).hexdigest().upper(),
            "kernel_names":names,"kernel_count":len(names),"launch_kernel_names":launch_names,
            "launch_expressions":launches,"status":"STATIC_PASS"}


def build_plan(*, nvcc: Path, source: Path, binding: Path, output: Path, flags: list[str] | None = None) -> dict[str, Any]:
    actual = list(PERFORMANCE_FLAGS if flags is None else flags)
    if actual != PERFORMANCE_FLAGS or any(x in actual for x in ("--use_fast_math", "-G")) or any(x.startswith("--maxrregcount") for x in actual):
        raise CUDAOnlyError("COMPILER_FLAGS_NOT_FROZEN")
    command = [str(nvcc), *actual, "-c", str(source), "-o", str(output)]
    return {"command":command,"command_sha256":hashlib.sha256(json.dumps(command,separators=(",",":"),ensure_ascii=False).encode("utf-8")).hexdigest().upper(),
            "binding_source":str(binding),"status":"PLAN_ONLY_NO_COMPILATION"}


def load_fixed_extension(*, candidate_source: str, binding_source: Path, build_root: Path,
                         nvcc: Path, expected_nvcc_sha256: str, expected_ptxas_sha256: str,
                         flags: list[str] | None = None):
    """Build only through the fixed PyTorch extension path; the caller must pass the GPU gate first."""
    info = validate_cuda_candidate(candidate_source)
    actual_flags = list(PERFORMANCE_FLAGS if flags is None else flags)
    plan = build_plan(nvcc=nvcc, source=Path("candidate.cu"), binding=binding_source,
                      output=Path("candidate.obj"), flags=actual_flags)
    if hashlib.sha256(Path(nvcc).read_bytes()).hexdigest().upper() != expected_nvcc_sha256.upper():
        raise CUDAOnlyError("NVCC_BINARY_SHA_MISMATCH")
    ptxas=Path(nvcc).with_name("ptxas.exe")
    if hashlib.sha256(ptxas.read_bytes()).hexdigest().upper() != expected_ptxas_sha256.upper():
        raise CUDAOnlyError("PTXAS_BINARY_SHA_MISMATCH")
    contract_path = Path(__file__).resolve().parents[1] / "CUDA_COMPILER_CONTRACT.json"
    contract = json.loads(contract_path.read_text(encoding="utf-8"))
    host = contract.get("host_compiler", {})
    host_path = Path(host.get("path", "")).resolve()
    if (not host_path.is_file()
            or hashlib.sha256(host_path.read_bytes()).hexdigest().upper() != str(host.get("sha256", "")).upper()):
        raise CUDAOnlyError("HOST_COMPILER_BINARY_SHA_MISMATCH")
    os.environ["CUDA_HOME"] = str(Path(nvcc).resolve().parents[1])
    os.environ["TORCH_CUDA_ARCH_LIST"] = "12.0"
    os.environ["MAX_JOBS"] = "1"
    os.environ["CXX"] = str(host_path)
    os.environ["CUDAHOSTCXX"] = os.environ["CXX"]
    if "VSCMD_ARG_TGT_ARCH" in os.environ:
        os.environ["DISTUTILS_USE_SDK"] = "1"
    host_bin = str(host_path.parent)
    path_parts = os.environ.get("PATH", "").split(os.pathsep)
    if host_bin not in path_parts:
        os.environ["PATH"] = host_bin + os.pathsep + os.environ.get("PATH", "")
    try:
        import torch
        from torch.utils import cpp_extension
        # The pinned Windows Python reports the OEM codec as cp1 although the
        # active Visual Studio shell emits CP936. Decode compiler version text
        # lossily as UTF-8 so ASCII version digits remain inspectable; this
        # keeps PyTorch's compiler compatibility check enabled.
        cpp_extension.SUBPROCESS_DECODE_ARGS = ("utf-8", "replace")
        from torch.utils.cpp_extension import load
    except Exception as exc:
        raise CUDAOnlyError("PYTORCH_EXTENSION_TOOLCHAIN_UNAVAILABLE") from exc
    binding_sha = hashlib.sha256(Path(binding_source).read_bytes()).hexdigest().upper()
    contract_sha = hashlib.sha256(contract_path.read_bytes()).hexdigest().upper()
    build_identity = {
        "candidate_source_sha256": info["source_sha256"],
        "binding_sha256": binding_sha,
        "wrapper_sha256": hashlib.sha256((Path(__file__).resolve().parent / "wrapper.py").read_bytes()).hexdigest().upper(),
        "compiler_contract_sha256": contract_sha,
        "nvcc_sha256": expected_nvcc_sha256.upper(),
        "ptxas_sha256": expected_ptxas_sha256.upper(),
        "host_compiler_sha256": str(host.get("sha256", "")).upper(),
        "torch_version": torch.__version__,
        "cuda_flags": actual_flags,
        "host_flags": ["/O2", "/std:c++20"],
    }
    cache_sha = hashlib.sha256(json.dumps(build_identity, sort_keys=True, separators=(",", ":")).encode()).hexdigest().upper()
    root = Path(build_root).resolve() / cache_sha
    root.mkdir(parents=True, exist_ok=True)
    candidate_path = root / "candidate.cu"
    raw = candidate_source.encode("utf-8")
    if candidate_path.exists() and candidate_path.read_bytes() != raw:
        raise CUDAOnlyError("IMMUTABLE_BUILD_SOURCE_CONFLICT")
    if not candidate_path.exists():
        with candidate_path.open("xb") as stream:
            stream.write(raw); stream.flush(); os.fsync(stream.fileno())
    name = "n0_layernorm_" + cache_sha[:16].lower()
    module = load(name=name, sources=[str(binding_source.resolve()), str(candidate_path)],
                  # PyTorch 2.14 headers use C++20 in the host binding. Candidate CUDA
                  # translation units remain on the frozen CUDA C++17 contract below.
                  extra_cflags=["/O2", "/std:c++20"], extra_cuda_cflags=actual_flags,
                  # Keep NVCC/Ninja output attached to the exception so the R11
                  # Worker can attribute a source-line compile failure safely.
                  # MAX_JOBS and compiler flags remain controller-pinned.
                  build_directory=str(root), verbose=False, with_cuda=True)
    ninja = root / "build.ninja"
    if not ninja.is_file():
        raise CUDAOnlyError("ACTUAL_BUILD_COMMAND_RECORD_MISSING")
    build_bytes = ninja.read_bytes()
    if host_bin.encode("utf-8") not in build_bytes and os.environ.get("VSCMD_ARG_TGT_ARCH") != "x64":
        raise CUDAOnlyError("HOST_TOOLCHAIN_ENVIRONMENT_NOT_ACTIVE")
    sys.path.insert(0, str(root)) if str(root) not in sys.path else None
    environment_names=("CUDA_HOME","TORCH_CUDA_ARCH_LIST","MAX_JOBS","CXX","CUDAHOSTCXX","PATH","INCLUDE","LIB","LIBPATH")
    build_environment={key:os.environ.get(key,"OMITTED") for key in environment_names}
    artifacts = [p for p in root.iterdir() if p.is_file() and p.suffix.lower() in {".pyd", ".dll"}]
    if len(artifacts) != 1:
        raise CUDAOnlyError("EXTENSION_ARTIFACT_IDENTITY_AMBIGUOUS")
    return module, {**info, **build_identity, "build_identity_sha256":cache_sha,
                    "build_plan_sha256":plan["command_sha256"],
                    "build_ninja_sha256":hashlib.sha256(build_bytes).hexdigest().upper(),
                    "build_environment":build_environment,
                    "build_environment_sha256":hashlib.sha256(json.dumps(build_environment,sort_keys=True,separators=(",",":"),ensure_ascii=False).encode("utf-8")).hexdigest().upper(),
                    "artifact_sha256":hashlib.sha256(artifacts[0].read_bytes()).hexdigest().upper(),
                    "artifact_path":str(artifacts[0]), "torch_version":torch.__version__}
