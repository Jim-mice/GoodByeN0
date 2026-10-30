"""Versioned, domain-aware CUDA and process fault records for R12."""
from __future__ import annotations

import ctypes
import ctypes.util
import os
import re
import subprocess
import _winapi
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

FAULT_SCHEMA = "N0_CUDA_FAULT_V1"

# Values follow CUDA Runtime API cudaError_t and Driver API CUresult domains.
# Names/descriptions are obtained from the installed API when available; this
# table only supplies a stable class, never converts one API domain to another.
# Numeric values are pinned to the CUDA Runtime enum shipped by the qualified
# Toolkit (13.4). Runtime/Driver numeric spaces must never be mixed.
_RUNTIME = {
    1: "INVALID_ARGUMENT", 2: "RESOURCE_OUT_OF_MEMORY",
    3: "CUDA_INITIALIZATION_FAILURE", 9: "INVALID_CONFIGURATION",
    98: "INVALID_DEVICE_FUNCTION", 700: "ILLEGAL_ADDRESS",
    701: "LAUNCH_OUT_OF_RESOURCES", 702: "LAUNCH_TIMEOUT",
    710: "DEVICE_SIDE_ASSERT", 714: "HARDWARE_STACK_ERROR",
    715: "ILLEGAL_INSTRUCTION", 716: "MISALIGNED_ADDRESS",
    717: "INVALID_ADDRESS_SPACE", 719: "LAUNCH_FAILURE",
    801: "UNSUPPORTED_OPERATION"}

_RUNTIME_NAME_CLASSES = {
    "cudaErrorInvalidValue": "INVALID_ARGUMENT",
    "cudaErrorMemoryAllocation": "RESOURCE_OUT_OF_MEMORY",
    "cudaErrorInitializationError": "CUDA_INITIALIZATION_FAILURE",
    "cudaErrorInvalidConfiguration": "INVALID_CONFIGURATION",
    "cudaErrorInvalidDeviceFunction": "INVALID_DEVICE_FUNCTION",
    "cudaErrorIllegalAddress": "ILLEGAL_ADDRESS",
    "cudaErrorLaunchOutOfResources": "LAUNCH_OUT_OF_RESOURCES",
    "cudaErrorLaunchTimeout": "LAUNCH_TIMEOUT",
    "cudaErrorAssert": "DEVICE_SIDE_ASSERT",
    "cudaErrorHardwareStackError": "HARDWARE_STACK_ERROR",
    "cudaErrorIllegalInstruction": "ILLEGAL_INSTRUCTION",
    "cudaErrorMisalignedAddress": "MISALIGNED_ADDRESS",
    "cudaErrorInvalidAddressSpace": "INVALID_ADDRESS_SPACE",
    "cudaErrorLaunchFailure": "LAUNCH_FAILURE",
    "cudaErrorNotSupported": "UNSUPPORTED_OPERATION"}


def _cuda_api(domain: str, code: int) -> tuple[str | None, str | None]:
    """Ask the installed Runtime/Driver API for its official name and text."""
    libraries: list[str] = []
    if domain == "CUDA_RUNTIME" and os.name == "nt":
        try:
            import torch
            libraries.extend(str(p) for p in (getattr(torch, "__path__", []) or []))
            libdir = os.path.join(os.path.dirname(torch.__file__), "lib")
            libraries.extend(os.path.join(libdir, n) for n in os.listdir(libdir)
                             if re.fullmatch(r"cudart64_\d+\.dll", n))
        except Exception:
            pass
    elif domain == "CUDA_DRIVER":
        libraries.append("nvcuda.dll" if os.name == "nt" else "libcuda.so.1")
    for library in libraries:
        try:
            dll = ctypes.WinDLL(library) if os.name == "nt" else ctypes.CDLL(library)
            if domain == "CUDA_RUNTIME":
                name_fn, text_fn = dll.cudaGetErrorName, dll.cudaGetErrorString
            else:
                name_fn, text_fn = dll.cuGetErrorName, dll.cuGetErrorString
            name_fn.argtypes = [ctypes.c_int, ctypes.POINTER(ctypes.c_char_p)]
            text_fn.argtypes = [ctypes.c_int, ctypes.POINTER(ctypes.c_char_p)]
            name_fn.restype = text_fn.restype = ctypes.c_int
            name = ctypes.c_char_p(); text = ctypes.c_char_p()
            if name_fn(code, ctypes.byref(name)) == 0 and text_fn(code, ctypes.byref(text)) == 0:
                return (name.value.decode("utf-8", "replace") if name.value else None,
                        text.value.decode("utf-8", "replace") if text.value else None)
        except Exception:
            continue
    return None, None


def classify_fault(domain: str, value: int | str | None, stage: str,
                   raw_message: str = "") -> dict[str, Any]:
    """Return a lossless fault envelope; unknown values always fail closed."""
    if domain not in {"NVCC_EXIT", "CUDA_RUNTIME", "CUDA_DRIVER", "WINDOWS_PROCESS_EXIT",
                      "PYTHON_EXCEPTION", "SUPERVISOR", "SYSTEM_RESOURCE", "UNKNOWN"}:
        raise ValueError("FAULT_DOMAIN_INVALID")
    official_name = official_description = None
    numeric = value if isinstance(value, int) else None
    if numeric is not None and domain in {"CUDA_RUNTIME", "CUDA_DRIVER"}:
        official_name, official_description = _cuda_api(domain, numeric)
    known_class = None
    if domain == "CUDA_RUNTIME" and numeric is not None:
        known_class = _RUNTIME_NAME_CLASSES.get(official_name or "") or _RUNTIME.get(numeric)
    if domain == "NVCC_EXIT":
        if numeric not in (None, 0) and re.search(r"\b(?:fatal\s+)?error\s*:", raw_message, re.IGNORECASE):
            known_class = "COMPILER_DIAGNOSTIC"
    if domain == "PYTHON_EXCEPTION":
        known_class = "PYTHON_EXCEPTION"
    if domain in {"SUPERVISOR", "SYSTEM_RESOURCE"}:
        known_class = str(value) if value else "SUPERVISOR_FAILURE"
    if domain == "CUDA_DRIVER" and official_name:
        known_class = "DRIVER_" + re.sub(r"[^A-Z0-9]+", "_", official_name.upper()).strip("_")
    return {"schema": FAULT_SCHEMA, "classifier_version": "N0_CUDA_FAULT_CLASSIFIER_V1",
            "domain": domain, "raw_value": value, "numeric_value": numeric,
            "official_name": official_name, "official_description": official_description,
            "stage": stage, "classification": known_class or "UNKNOWN_FAIL_CLOSED",
            "attribution": "UNRESOLVED",
            "raw_message": str(raw_message)[:65536], "score_allowed": False}


def classify_candidate_runtime_message(message: str, stage: str, *, require_cuda_marker: bool = False) -> dict[str, Any] | None:
    """Recognize known Candidate-attributable launch/execution CUDA failures."""
    text = str(message or "")
    if require_cuda_marker and not re.search(
            r"\b(?:cuda\s+error|cudaError[A-Za-z0-9_]*|CUDA_ERROR_[A-Z0-9_]+|cu[A-Z][A-Za-z0-9_]*\s*\()",
            text, re.IGNORECASE):
        return None
    official_names = re.findall(r"\b(?:cudaError[A-Za-z0-9_]+|CUDA_ERROR_[A-Z0-9_]+)\b", text)
    rules = (
        (("cudaErrorInvalidConfiguration", "invalid configuration argument"), "CANDIDATE_LAUNCH_INVALID_CONFIGURATION"),
        (("cudaErrorInvalidDeviceFunction", "invalid device function"), "CANDIDATE_LAUNCH_INVALID_DEVICE_FUNCTION"),
        (("cudaErrorLaunchOutOfResources", "launch out of resources"), "CANDIDATE_LAUNCH_OUT_OF_RESOURCES"),
        (("cudaErrorInvalidValue", "invalid argument"), "CANDIDATE_LAUNCH_INVALID_ARGUMENT"),
        (("cudaErrorNotSupported", "operation not supported"), "CANDIDATE_LAUNCH_UNSUPPORTED_OPERATION"),
        (("cudaErrorIllegalAddress", "illegal memory access", "an illegal memory access was encountered"), "CANDIDATE_EXECUTION_ILLEGAL_ADDRESS"),
        (("cudaErrorMisalignedAddress", "misaligned address"), "CANDIDATE_EXECUTION_MISALIGNED_ADDRESS"),
        (("cudaErrorIllegalInstruction", "illegal instruction"), "CANDIDATE_EXECUTION_ILLEGAL_INSTRUCTION"),
        (("cudaErrorAssert", "device-side assert"), "CANDIDATE_EXECUTION_DEVICE_ASSERT"),
        (("cudaErrorLaunchFailure", "unspecified launch failure"), "CANDIDATE_EXECUTION_LAUNCH_FAILURE"),
        (("cudaErrorLaunchTimeout", "launch timed out"), "CANDIDATE_EXECUTION_LAUNCH_TIMEOUT"),
        (("cudaErrorHardwareStackError", "hardware stack error"), "CANDIDATE_EXECUTION_HARDWARE_STACK_ERROR"),
        (("cudaErrorInvalidAddressSpace", "invalid address space"), "CANDIDATE_EXECUTION_INVALID_ADDRESS_SPACE"),
        (("cudaErrorMemoryAllocation", "out of memory"), "CANDIDATE_RESOURCE_OUT_OF_MEMORY"),
        (("cudaErrorLaunchTimeout", "device unavailable", "device lost", "driver shutting down"), None),
    )
    lowered = text.casefold()
    for phrases, classification in rules:
        if classification and any(phrase.casefold() in lowered for phrase in phrases):
            official = next((name for name in official_names if any(p.casefold() == name.casefold() for p in phrases)), None)
            driver_name = next((name for name in official_names if name.startswith("CUDA_ERROR_")), None)
            domain = "CUDA_DRIVER" if driver_name and re.search(r"\bcu[A-Z][A-Za-z0-9_]*", text) else "CUDA_RUNTIME"
            code_match = (re.search(r"\bReturning\s+(-?\d+)\s*\(", text, re.IGNORECASE)
                          or re.search(r"\b(?:code|error)\s*[=:]?\s*(-?\d+)\b", text, re.IGNORECASE))
            code = int(code_match.group(1)) if code_match else None
            api_name = official
            api_description = None
            if code is not None:
                api_name, api_description = _cuda_api(domain, code)
            return {"schema": FAULT_SCHEMA, "classifier_version": "N0_CUDA_FAULT_CLASSIFIER_V1",
                "domain": domain, "stage": stage, "raw_value": code if code is not None else official,
                "official_name": api_name or driver_name or official, "official_description": api_description,
                "classification": classification, "attribution": "CANDIDATE",
                "raw_message": text[:65536], "score_allowed": False}
    return None


def fault_recovery_decision(*, active_processes_after: int | None, health_checks: Sequence[str],
                            reference_checks: Sequence[bool], required_consecutive: int = 3) -> dict[str, Any]:
    """Pure fail-closed recovery state machine used after an aborted evaluator."""
    if active_processes_after != 0:
        return {"status": "HOLD", "reason": "CANDIDATE_PROCESS_TREE_NOT_PROVEN_EMPTY", "consecutive": 0}
    streak = 0
    for health, reference_ok in zip(health_checks, reference_checks):
        if health == "PASS" and reference_ok is True:
            streak += 1
            if streak >= required_consecutive:
                return {"status": "READY", "reason": "PROCESS_TREE_EMPTY_AND_REFERENCE_HEALTH_QUALIFIED",
                        "consecutive": streak}
        else:
            streak = 0
    return {"status": "HOLD", "reason": "REFERENCE_OR_DEVICE_HEALTH_CHECK_NOT_QUALIFIED", "consecutive": streak}


@dataclass
class JobProcessResult:
    returncode: int | None
    timed_out: bool
    stdout: str
    stderr: str
    terminated_tree: bool
    active_processes_after: int | None
    process_id: int | None = None
    started_unix: float | None = None
    finished_unix: float | None = None


class WindowsJobProcess:
    """Run a child under a kill-on-close Windows Job Object and wall deadline."""
    def __init__(self, *, timeout_seconds: float, output_limit_bytes: int = 16 * 1024 * 1024,
                 process_limit: int = 16, memory_limit_bytes: int = 8 * 1024**3):
        self.timeout_seconds = timeout_seconds
        self.output_limit_bytes = output_limit_bytes
        self.process_limit = process_limit
        self.memory_limit_bytes = memory_limit_bytes

    def run(self, command: Sequence[str], *, cwd: str, env: dict[str, str],
            stdout_path: str, stderr_path: str, on_started=None) -> JobProcessResult:
        if os.name != "nt":
            raise RuntimeError("WINDOWS_JOB_OBJECT_REQUIRED")
        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel.CreateJobObjectW.argtypes = [ctypes.c_void_p, ctypes.c_wchar_p]
        kernel.CreateJobObjectW.restype = ctypes.c_void_p
        kernel.SetInformationJobObject.argtypes = [ctypes.c_void_p, ctypes.c_int, ctypes.c_void_p, ctypes.c_ulong]
        kernel.SetInformationJobObject.restype = ctypes.c_int
        kernel.AssignProcessToJobObject.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
        kernel.AssignProcessToJobObject.restype = ctypes.c_int
        kernel.ResumeThread.argtypes = [ctypes.c_void_p]
        kernel.ResumeThread.restype = ctypes.c_ulong
        kernel.TerminateJobObject.argtypes = [ctypes.c_void_p, ctypes.c_uint]
        kernel.TerminateJobObject.restype = ctypes.c_int
        kernel.CloseHandle.argtypes = [ctypes.c_void_p]
        kernel.CloseHandle.restype = ctypes.c_int
        kernel.QueryInformationJobObject.argtypes = [ctypes.c_void_p, ctypes.c_int, ctypes.c_void_p,
            ctypes.c_ulong, ctypes.POINTER(ctypes.c_ulong)]
        kernel.QueryInformationJobObject.restype = ctypes.c_int
        class IO_COUNTERS(ctypes.Structure):
            _fields_ = [(x, ctypes.c_ulonglong) for x in ("ReadOperationCount", "WriteOperationCount",
                "OtherOperationCount", "ReadTransferCount", "WriteTransferCount", "OtherTransferCount")]
        class BASIC_LIMIT(ctypes.Structure):
            _fields_ = [("PerProcessUserTimeLimit", ctypes.c_longlong), ("PerJobUserTimeLimit", ctypes.c_longlong),
                ("LimitFlags", ctypes.c_ulong), ("MinimumWorkingSetSize", ctypes.c_size_t),
                ("MaximumWorkingSetSize", ctypes.c_size_t), ("ActiveProcessLimit", ctypes.c_ulong),
                ("Affinity", ctypes.c_size_t), ("PriorityClass", ctypes.c_ulong), ("SchedulingClass", ctypes.c_ulong)]
        class EXT_LIMIT(ctypes.Structure):
            _fields_ = [("BasicLimitInformation", BASIC_LIMIT), ("IoInfo", IO_COUNTERS),
                ("ProcessMemoryLimit", ctypes.c_size_t), ("JobMemoryLimit", ctypes.c_size_t),
                ("PeakProcessMemoryUsed", ctypes.c_size_t), ("PeakJobMemoryUsed", ctypes.c_size_t)]
        job = kernel.CreateJobObjectW(None, None)
        if not job:
            raise ctypes.WinError(ctypes.get_last_error())
        process = None; timed_out = False; tree_terminated = False; active_after = None
        class ACCOUNTING(ctypes.Structure):
            _fields_ = [("TotalUserTime", ctypes.c_longlong), ("TotalKernelTime", ctypes.c_longlong),
                ("ThisPeriodTotalUserTime", ctypes.c_longlong), ("ThisPeriodTotalKernelTime", ctypes.c_longlong),
                ("TotalPageFaultCount", ctypes.c_ulong), ("TotalProcesses", ctypes.c_ulong),
                ("ActiveProcesses", ctypes.c_ulong), ("TotalTerminatedProcesses", ctypes.c_ulong)]
        def active_count() -> int | None:
            info = ACCOUNTING()
            ok = kernel.QueryInformationJobObject(job, 1, ctypes.byref(info), ctypes.sizeof(info), None)
            return int(info.ActiveProcesses) if ok else None
        process_handle = thread_handle = None; child_pid=None; started_unix=time.time()
        try:
            limits = EXT_LIMIT(); limits.BasicLimitInformation.LimitFlags = 0x00002000 | 0x00000008 | 0x00000200
            limits.BasicLimitInformation.ActiveProcessLimit = self.process_limit
            limits.ProcessMemoryLimit = self.memory_limit_bytes
            limits.JobMemoryLimit = self.memory_limit_bytes
            if not kernel.SetInformationJobObject(job, 9, ctypes.byref(limits), ctypes.sizeof(limits)):
                raise ctypes.WinError(ctypes.get_last_error())
            import os as _os
            import subprocess as _subprocess
            startup = _subprocess.STARTUPINFO()
            executable = str(command[0])
            command_line = _subprocess.list2cmdline(list(command))
            process_handle, thread_handle, child_pid, _ = _winapi.CreateProcess(executable, command_line,
                None, None, False, 0x00000004 | 0x08000000 | 0x00000400, env, cwd, startup)
            if not kernel.AssignProcessToJobObject(job, ctypes.c_void_p(process_handle)):
                kernel.TerminateJobObject(job, 0xE0000001)
                raise ctypes.WinError(ctypes.get_last_error())
            if kernel.ResumeThread(ctypes.c_void_p(thread_handle)) == 0xFFFFFFFF:
                kernel.TerminateJobObject(job, 0xE0000001)
                raise ctypes.WinError(ctypes.get_last_error())
            if on_started is not None:
                try:
                    on_started(int(child_pid))
                except BaseException:
                    kernel.TerminateJobObject(job, 0xE0000006)
                    _winapi.WaitForSingleObject(process_handle, 5000)
                    deadline_empty = time.monotonic() + 5.0
                    while active_count() not in (0, None) and time.monotonic() < deadline_empty:
                        time.sleep(0.05)
                    raise
            deadline = time.monotonic() + self.timeout_seconds
            while _winapi.WaitForSingleObject(process_handle, 50) == _winapi.WAIT_TIMEOUT:
                if time.monotonic() >= deadline:
                    timed_out = True
                    kernel.TerminateJobObject(job, 0xE0000002)
                    tree_terminated = True
                    break
                time.sleep(0.01)
            if _winapi.WaitForSingleObject(process_handle, 5000) == _winapi.WAIT_TIMEOUT:
                kernel.TerminateJobObject(job, 0xE0000004)
                tree_terminated = True
                _winapi.WaitForSingleObject(process_handle, 5000)
            returncode = int(_winapi.GetExitCodeProcess(process_handle))
            deadline_empty = time.monotonic() + 5.0
            active_after = active_count()
            while active_after not in (0, None) and time.monotonic() < deadline_empty:
                time.sleep(0.05); active_after = active_count()
            if active_after not in (0, None):
                kernel.TerminateJobObject(job, 0xE0000005)
                tree_terminated = True
                deadline_empty = time.monotonic() + 5.0
                while active_after not in (0, None) and time.monotonic() < deadline_empty:
                    time.sleep(0.05); active_after = active_count()
            Path(stdout_path).touch(exist_ok=True); Path(stderr_path).touch(exist_ok=True)
            return JobProcessResult(returncode, timed_out, "", "", tree_terminated, active_after,
                                    int(child_pid) if child_pid else None, started_unix, time.time())
        finally:
            # Closing a kill-on-close Job is the final child-tree containment action.
            if thread_handle:
                _winapi.CloseHandle(thread_handle)
            if process_handle:
                _winapi.CloseHandle(process_handle)
            kernel.CloseHandle(job)
