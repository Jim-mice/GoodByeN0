from __future__ import annotations
import hashlib, json, os, re, subprocess, threading, time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

GPU_NAME="NVIDIA GeForce RTX 5060 Laptop GPU"
GPU_UUID="GPU-15840d68-8974-35a8-6886-d7a36fb0a04d"

def candidate_compile_diagnostic(exc: BaseException) -> dict[str, Any] | None:
    """Attribute explicit Candidate source or Candidate-generated PTX compiler errors."""
    parts: list[str] = []
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        parts.append(str(current))
        for name in ("output", "stdout", "stderr"):
            value = getattr(current, name, None)
            if value:
                if isinstance(value, bytes):
                    value = value.decode("utf-8", errors="replace")
                parts.append(str(value))
        current = current.__cause__ or current.__context__
    message = "\n".join(parts)
    match = re.search(r"candidate\.cu(?:\((\d+)(?:,\d+)?\)|:(\d+)(?::\d+)?)", message, re.IGNORECASE)
    if match and re.search(r"\berror\b", message, re.IGNORECASE):
        return {"category":"CANDIDATE_COMPILE_FAILURE","source":"candidate.cu",
                "line":int(match.group(1) or match.group(2)),"compiler_diagnostic":message,
                "attribution":"COMPILER_REPORTED_CANDIDATE_SOURCE_LINE"}
    ptxas_error = re.search(r"\bptxas\b", message, re.IGNORECASE) and re.search(
        r"\b(?:error\s*:|Ptx assembly aborted|assembly aborted due to errors)\b", message, re.IGNORECASE)
    ptx_location = re.search(r"(?P<file>(?:[^\r\n]*[\\/])?(?:tmpxft_[^\r\n\\/]*candidate\.ptx|candidate\.ptx)),\s*line\s+(?P<line>\d+)\s*;\s*error\s*:",
                             message, re.IGNORECASE)
    if ptxas_error and ptx_location:
        return {"category":"CANDIDATE_COMPILE_FAILURE","source":"candidate.cu",
                "line":None,"compiler_location":{"file":ptx_location.group("file"),
                    "line":int(ptx_location.group("line")),"format":"GENERATED_CANDIDATE_PTX"},
                "compiler_domain":"PTXAS","compiler_diagnostic":message,
                "attribution":"PTXAS_REJECTED_CANDIDATE_GENERATED_PTX"}
    return None

def _sha(b:bytes)->str: return hashlib.sha256(b).hexdigest().upper()

def _append_durable_jsonl(path:Path,record:dict[str,Any])->None:
    path=Path(path); path.parent.mkdir(parents=True,exist_ok=True)
    raw=(json.dumps(record,ensure_ascii=False,sort_keys=True,separators=(",",":"))+"\n").encode("utf-8")
    with path.open("ab") as stream:
        stream.write(raw); stream.flush(); os.fsync(stream.fileno())

def run_profile_safely(profile_runner,request,source,build,monitor):
    try:
        return profile_runner(request,source,build,monitor)
    except Exception as exc:
        return {"status":"UNAVAILABLE","reason":"PROFILE_RUNNER_EXCEPTION",
                "exception_type":type(exc).__name__,"exception_message":str(exc),
                "request_id":request.get("request_id"),"episode_id":request.get("episode_id"),
                "candidate_id":request.get("candidate_id"),
                "candidate_sha256":hashlib.sha256(source.encode("utf-8")).hexdigest().upper()}

def diagnostic_profile_suppressed(request:dict[str,Any])->bool:
    return (str(request.get("episode_id","")).startswith("N0DIAG-")
        and os.environ.get("N0_DISABLE_DIAGNOSTIC_PROFILE")=="1")

def query_device()->dict[str,Any]:
    if os.environ.get('N0_CANDIDATE_EVAL_CHILD')=='1':
        snapshot=os.environ.get('N0_SUPERVISOR_GPU_SNAPSHOT_JSON')
        if not snapshot: raise RuntimeError('SUPERVISOR_GPU_SNAPSHOT_MISSING')
        value=json.loads(snapshot)
        if not isinstance(value,dict) or not value.get('fields'):
            raise RuntimeError('SUPERVISOR_GPU_SNAPSHOT_INVALID')
        return {**value,'snapshot_source':'SUPERVISOR_PRE_EVALUATION_SNAPSHOT_CACHED_IN_CHILD'}
    p=subprocess.run(["nvidia-smi","--query-gpu=name,uuid,driver_version,temperature.gpu,utilization.gpu,memory.used,clocks.sm,clocks.mem,power.draw,pstate","--format=csv,noheader,nounits"],capture_output=True,text=True,check=True,timeout=15)
    fields=[x.strip() for x in next(__import__('csv').reader([p.stdout.strip()]))]
    a=subprocess.run(["nvidia-smi","--query-compute-apps=pid,process_name,used_memory","--format=csv,noheader,nounits"],capture_output=True,text=True,timeout=15)
    return {"fields":fields,"apps":[x.strip() for x in a.stdout.splitlines() if x.strip()],"apps_returncode":a.returncode}

def _process_is_alive(pid:int)->bool|None:
    """Return False only when Windows proves the PID is gone; unknown stays fail-closed."""
    if os.name!="nt" or not isinstance(pid,int) or pid<=0: return None
    try:
        import ctypes
        from ctypes import wintypes
        kernel=ctypes.WinDLL("kernel32",use_last_error=True)
        kernel.OpenProcess.argtypes=[wintypes.DWORD,wintypes.BOOL,wintypes.DWORD]
        kernel.OpenProcess.restype=wintypes.HANDLE
        kernel.GetExitCodeProcess.argtypes=[wintypes.HANDLE,ctypes.POINTER(wintypes.DWORD)]
        kernel.GetExitCodeProcess.restype=wintypes.BOOL
        kernel.CloseHandle.argtypes=[wintypes.HANDLE]
        kernel.CloseHandle.restype=wintypes.BOOL
        handle=kernel.OpenProcess(0x00100000|0x1000,False,pid)  # SYNCHRONIZE | QUERY_LIMITED_INFORMATION
        if not handle:
            return False if ctypes.get_last_error() in (87,1168) else None
        try:
            code=wintypes.DWORD()
            if not kernel.GetExitCodeProcess(handle,ctypes.byref(code)): return None
            return code.value==259  # STILL_ACTIVE
        finally:
            kernel.CloseHandle(handle)
    except Exception:
        return None

class ExternalGpuMonitor:
    """Fail closed if a visible compute process outside this transaction appears."""
    def __init__(self, *, owned_pids:set[int]|None=None, interval_seconds:float=0.5,
                 coordinated_pid_resolver:Callable[[int],dict[str,Any]|None]|None=None,
                 process_liveness_resolver:Callable[[int],bool|None]|None=None,
                 recently_exited_grace_seconds:float=5.0):
        self.owned_pids=owned_pids or {os.getpid()}; self.interval=interval_seconds
        self.coordinated_pid_resolver=coordinated_pid_resolver
        self.process_liveness_resolver=process_liveness_resolver or _process_is_alive
        self.recently_exited_grace_seconds=max(0.0,float(recently_exited_grace_seconds))
        self.recent_coordinated_pids={}
        self.events=[]; self.observations=[]; self.stop_event=threading.Event(); self.thread=None
    def register_owned_pid(self, pid:int)->None:
        if not isinstance(pid,int) or pid<=0: raise ValueError("GPU_MONITOR_PID_INVALID")
        self.owned_pids.add(pid)
    def classify_snapshot(self,snap:dict[str,Any])->dict[str,Any]:
        fields=snap["fields"]; foreign=[]; coordinated=[]; owned=[]; stale_coordinated=[]
        for line in snap["apps"]:
            try: pid=int(line.split(',',1)[0].strip())
            except Exception: pid=-1
            if pid in self.owned_pids: owned.append({"pid":pid,"raw":line})
            elif pid>0 and self.coordinated_pid_resolver:
                participant=self.coordinated_pid_resolver(pid)
                if participant is not None:
                    coordinated.append(participant)
                    if participant.get("attribution")=="CANDIDATE_EVAL_CHILD_BOUND_TO_SHARED_GPU_LEASE":
                        self.recent_coordinated_pids[pid]={"participant":participant,"observed_monotonic":time.monotonic()}
                else:
                    previous=self.recent_coordinated_pids.get(pid)
                    age=(time.monotonic()-previous["observed_monotonic"]) if previous else None
                    alive=self.process_liveness_resolver(pid) if previous and age<=self.recently_exited_grace_seconds else None
                    if previous and age<=self.recently_exited_grace_seconds and alive is False:
                        stale_coordinated.append({"pid":pid,"raw":line,
                            "last_same_lease_attribution":previous["participant"],
                            "seconds_since_attribution":age,"process_state":"CONFIRMED_EXITED",
                            "reason":"WINDOWS_GPU_APP_LIST_RETAINED_EXITED_CANDIDATE_PID"})
                    else:
                        foreign.append({"pid":pid,"raw":line,
                            "recent_same_lease_attribution":previous["participant"] if previous else None,
                            "process_state":"ALIVE_OR_UNKNOWN" if previous else "NOT_PREVIOUSLY_ATTRIBUTED"})
            else: foreign.append({"pid":pid,"raw":line})
        if int(fields[4])>5 and not snap["apps"]:
            foreign.append({"reason":"GPU_BUSY_WITHOUT_ATTRIBUTABLE_COMPUTE_PID"})
        return {"time_utc":datetime.now(timezone.utc).isoformat(),"gpu":fields,
            "owned_compute_pids":owned,"coordinated_same_lease_participants":coordinated,
            "recently_exited_coordinated_compute_pids":stale_coordinated,
            "uncoordinated_compute_pids":foreign}
    def _run(self):
        while not self.stop_event.wait(self.interval):
            try:
                observation=self.classify_snapshot(query_device())
                foreign=observation["uncoordinated_compute_pids"]
                self.observations.append(observation)
                if foreign:
                    self.events.append({"time_utc":observation["time_utc"],"gpu":observation["gpu"],"foreign":foreign})
            except Exception as exc:
                self.events.append({"time_utc":datetime.now(timezone.utc).isoformat(),"probe_error":type(exc).__name__+":"+str(exc)})
    def start(self): self.thread=threading.Thread(target=self._run,daemon=True); self.thread.start()
    def stop(self):
        self.stop_event.set()
        if self.thread: self.thread.join(timeout=2)
        return list(self.events)
    def evidence(self):
        return {"schema":"N0_EXTERNAL_GPU_MONITOR_EVIDENCE_V2",
            "owned_pids":sorted(self.owned_pids),"observation_count":len(self.observations),
            "observations":list(self.observations),"events":list(self.events)}

def make_r5_queue_evaluator(*, queue_root:Path, repository_root:Path, release_root:Path,
                            qualification:dict[str,Any], hardware_identity:dict[str,Any],
                            profile_runner:Callable[[dict[str,Any],str,dict[str,Any],ExternalGpuMonitor],dict[str,Any]]|None=None,
                            monitor_factory:Callable[[],ExternalGpuMonitor]=ExternalGpuMonitor):
    """Create the R5 evaluator callback used only inside the shared GPU lease.

    A fresh Reference Control is measured immediately before each official
    evaluator call. Profile is auxiliary. A valid, trusted Timing result retains its official Score when Profile is unavailable.
    """
    from n0_kernelbench_oj.adapter import generate_reference_baseline,evaluate_candidate,precompile_candidate_extension
    from n0_kernelbench_oj.measurement_trust import assess_reference_path_agreement
    from n0_kernelbench_oj.adapter import sha256_file
    oj_release_doc=json.loads((Path(release_root)/"RELEASE_MANIFEST.json").read_text(encoding="utf-8"))
    oj_release_id=str(oj_release_doc.get("release_id") or "")
    if not oj_release_id:
        raise RuntimeError("OJ_RELEASE_IDENTITY_MISSING")
    def evaluate(request:dict[str,Any])->dict[str,Any]:
        src_path=(Path(queue_root).resolve()/Path(request["candidate_source_path"])).resolve()
        try: src_path.relative_to(Path(queue_root).resolve())
        except ValueError: raise ValueError("OJ_SOURCE_PATH_ESCAPE")
        raw=src_path.read_bytes(); source=raw.decode('utf-8')
        source_sha=_sha(raw).lower()
        if source_sha!=str(request.get("candidate_sha256","")).lower():
            raise ValueError("OJ_CANDIDATE_SHA_MISMATCH")
        snap=query_device(); f=snap['fields']
        if f[0]!=GPU_NAME or f[1]!=GPU_UUID or int(f[4])>5:
            raise RuntimeError("OJ_GPU_PRECHECK_HOLD")
        monitor=monitor_factory(); monitor.start()
        capture_path=Path(queue_root).resolve()/"measurement_capture"/(request["request_id"]+".jsonl")
        capture_sequence=0
        def persist_capture(stage:str,value:dict[str,Any])->None:
            nonlocal capture_sequence
            capture_sequence+=1
            _append_durable_jsonl(capture_path,{"schema":"N0_R5_RAW_CAPTURE_EVENT_V1","sequence":capture_sequence,
                "stage":stage,"request_id":request["request_id"],"episode_id":request["episode_id"],
                "candidate_id":request["candidate_id"],"candidate_sha256":source_sha,
                "captured_at_utc":datetime.now(timezone.utc).isoformat(),"payload":value})
        persist_capture("DEVICE_SNAPSHOT_BEFORE",snap)
        result=None
        prepared_extension=None
        profile={"status":"UNAVAILABLE","reason":"PROFILE_RUNNER_NOT_CONFIGURED"}
        try:
            try:
                prepared_extension=precompile_candidate_extension(candidate_source=source,
                    repository_root=Path(repository_root),qualification=qualification,
                    hardware_identity=hardware_identity,device=0)
                persist_capture("CANDIDATE_PRECOMPILE",{k:v for k,v in prepared_extension.items() if k!="module"})
            except Exception as exc:
                prepared_extension=None
                diagnostic=candidate_compile_diagnostic(exc)
                result=({"status":"CANDIDATE_COMPILE_FAILURE","candidate_failure":diagnostic,
                    "candidate_source_sha256":source_sha} if diagnostic else
                    {"status":"OJ_INFRASTRUCTURE_HOLD_CANDIDATE_PRECOMPILE_FAILED",
                    "precompile_error":{"type":type(exc).__name__,"message":str(exc)},
                    "candidate_source_sha256":source_sha})
            precompile_status=(prepared_extension or {}).get('status')
            if result is None and precompile_status not in {"PRECOMPILED","SKIPPED_SYNTAX_REJECTED"}:
                result={"status":"OJ_INFRASTRUCTURE_HOLD_CANDIDATE_PRECOMPILE_FAILED",
                    "precompile_result":{k:v for k,v in (prepared_extension or {}).items() if k!="module"},
                    "candidate_source_sha256":source_sha}
            pre_baseline=None
            if result is None:
                pre_baseline=generate_reference_baseline(repository_root=Path(repository_root),qualification=qualification,
                    hardware_identity=hardware_identity,device=0)
                persist_capture("PRE_EVALUATOR_REFERENCE_CONTROL",pre_baseline)
                result=evaluate_candidate(candidate_source=source,repository_root=Path(repository_root),
                    qualification=qualification,hardware_identity=hardware_identity,baseline=pre_baseline,device=0,
                    prepared_extension=prepared_extension if precompile_status=="PRECOMPILED" else None)
                if str((result or {}).get("status", "")).startswith("OJ_INFRASTRUCTURE_HOLD"):
                    from n0_integration.cuda_faults import classify_candidate_runtime_message
                    metadata=(result or {}).get("official_evaluator_metadata") or {}
                    def diagnostic_text(value):
                        if isinstance(value,dict): return "\n".join(diagnostic_text(v) for v in value.values())
                        if isinstance(value,(list,tuple)): return "\n".join(diagnostic_text(v) for v in value)
                        return str(value) if value is not None else ""
                    runtime_text="\n".join(diagnostic_text(metadata.get(key)) for key in
                        ("runtime_error","error_during_performance","correctness_issue","correctness_issue_name"))
                    cuda_fault=classify_candidate_runtime_message(runtime_text,"EVALUATOR_KERNEL_EXECUTION")
                    if cuda_fault:
                        result={**result,"status":"CANDIDATE_CUDA_RUNTIME_FAILURE",
                            "candidate_failure":cuda_fault,"runtime_fault":cuda_fault}
            persist_capture("FROZEN_EVALUATOR_RESULT",result)
            build=result.get('build_identity') or {}
            trust_status=result.get('status')
            evidence=result.get('measurement_evidence') or {}
            evaluator_reference=evidence.get('evaluator_reference')
            if evaluator_reference and trust_status in {"VALID_MEASUREMENT","OJ_INFRASTRUCTURE_HOLD_REFERENCE_CONTROL_MISMATCH","OJ_INFRASTRUCTURE_HOLD_REFERENCE_BASELINE_DRIFT"}:
                try:
                    post_baseline=generate_reference_baseline(repository_root=Path(repository_root),qualification=qualification,
                        hardware_identity=hardware_identity,device=0)
                    persist_capture("POST_EVALUATOR_REFERENCE_CONTROL",post_baseline)
                    post_evidence=post_baseline.get('measurement_evidence')
                    post_trust=assess_reference_path_agreement(post_evidence,evaluator_reference)
                    # Tighten the legacy gate using local fixed-source evidence.
                    from n0_integration.reference_protocol import enforce_post_evaluator_control_limit
                    post_trust=enforce_post_evaluator_control_limit(post_trust)
                    from n0_kernelbench_oj.measurement_trust import assess_against_frozen_baseline
                    frozen=(pre_baseline or {}).get('frozen_baseline') or {}
                    post_drift=assess_against_frozen_baseline(post_evidence,frozen)
                    prior_trust=evidence.get('reference_trust') or {}
                    control_drift=evidence.get('control_baseline_drift') or {}
                    evaluator_drift=evidence.get('evaluator_baseline_drift') or {}
                    evidence={**evidence,"schema":"N0_REFERENCE_PATH_EVIDENCE_V3",
                      "pre_evaluator_control":pre_baseline.get('measurement_evidence'),
                      "post_evaluator_control":post_evidence,
                      "pre_evaluator_reference_trust":prior_trust,
                      "reference_trust":post_trust,"post_control_baseline_drift":post_drift,
                      "control_pairing_policy":"CONTROL_CAPTURED_IMMEDIATELY_AFTER_FROZEN_KERNELBENCH_EVALUATOR; controls gate the session while the versioned frozen Reference supplies the score numerator"}
                    result["measurement_evidence"]=evidence
                    baseline=post_baseline
                    official=result.get('official_kernelbench_result') or {}
                    prior_timing=result.get('timing') or {}
                    candidate_ms=float(official.get('candidate_runtime_ms') or prior_timing.get('candidate_runtime_ms') or 0)
                    evaluator_ms=float(official.get('reference_runtime_ms') or prior_timing.get('kernelbench_reference_runtime_ms') or 0)
                    correctness=official.get('correctness') is True
                    if (post_trust.get('pass') is True and post_drift.get('pass') is True and
                            control_drift.get('pass') is True and evaluator_drift.get('pass') is True and
                            candidate_ms>0 and evaluator_ms>0 and correctness):
                        result["status"]="VALID_MEASUREMENT"
                        prior_timing=result.get('timing') or {}
                        frozen= (pre_baseline or {}).get('frozen_baseline') or {}
                        frozen_reference_ms=float(prior_timing.get('frozen_reference_ms') or
                            (frozen.get('runtime_stats_ms') or {}).get('mean') or 0)
                        baseline_id=prior_timing.get('baseline_id') or frozen.get('baseline_id')
                        if frozen_reference_ms<=0 or not baseline_id:
                            raise ValueError("FROZEN_SCORE_IDENTITY_MISSING")
                        result["timing"]={"method":"cuda_event","warmup":3,"trials":10,"discard_first":1,
                          "candidate_runtime_ms":candidate_ms,"kernelbench_reference_runtime_ms":evaluator_ms,
                          "kernelbench_speedup":frozen_reference_ms/candidate_ms,
                          "score_version":"N0_FROZEN_REFERENCE_SCORE_ONLINE_V2","baseline_id":baseline_id,
                          "frozen_reference_ms":frozen_reference_ms,
                          "seed_reference_control_ms":post_evidence['summary']['mean_ms'],
                          "seed_control_speedup":post_evidence['summary']['mean_ms']/candidate_ms}
                        result["gpu_identity"]=hardware_identity
                    else:
                        result["status"]=("OJ_INFRASTRUCTURE_HOLD_REFERENCE_BASELINE_DRIFT"
                            if not (post_drift.get('pass') and control_drift.get('pass') and evaluator_drift.get('pass'))
                            else "OJ_INFRASTRUCTURE_HOLD_REFERENCE_CONTROL_MISMATCH")
                        result.pop("timing",None)
                except Exception as exc:
                    baseline=pre_baseline
                    result["measurement_evidence"]={**evidence,"post_evaluator_control_error":type(exc).__name__+":"+str(exc),
                      "reference_trust":{"pass":False,"limit":0.02,
                        "policy":"N0_POST_EVALUATOR_REFERENCE_PAIRED_V2","failure":"POST_EVALUATOR_CONTROL_UNAVAILABLE"}}
                    result["status"]="OJ_INFRASTRUCTURE_HOLD_POST_EVALUATOR_CONTROL_UNAVAILABLE"
                    result.pop("timing",None)
                trust_status=result.get('status')
            diagnostic_profile_disabled=diagnostic_profile_suppressed(request)
        finally:
            interference=monitor.stop()
            try:
                persist_capture("DEVICE_SNAPSHOT_AFTER",query_device())
                persist_capture("INTERFERENCE_MONITOR_EVIDENCE",monitor.evidence())
            except Exception as capture_exc:
                _append_durable_jsonl(capture_path,{"schema":"N0_R5_RAW_CAPTURE_ERROR_V1",
                    "stage":"POST_EVALUATOR_CAPTURE_FAILED","request_id":request["request_id"],
                    "candidate_sha256":source_sha,"error":type(capture_exc).__name__+":"+str(capture_exc),
                    "captured_at_utc":datetime.now(timezone.utc).isoformat()})
        build=(result or {}).get('build_identity') or {}
        trust_status=(result or {}).get('status','OJ_EVALUATION_DID_NOT_RETURN')
        if interference:
            trust_status="OJ_INFRASTRUCTURE_HOLD_EXTERNAL_GPU_INTERFERENCE"
        if trust_status=="VALID_MEASUREMENT" and profile_runner:
            if diagnostic_profile_disabled:
                profile={"status":"UNAVAILABLE","reason":"DISABLED_FOR_TIMING_DIAGNOSTIC",
                    "profile_policy":"N0_DIAGNOSTIC_PROFILE_SUPPRESSION_V1","score_inclusion":False,
                    "request_id":request.get("request_id"),"candidate_sha256":source_sha}
            else:
                profile={"status":"QUEUED","profile_receipt_id":request["request_id"],
                    "profile_policy":"N0_POST_RECEIPT_INDEPENDENT_PROFILE_V1","score_inclusion":False,
                    "request_id":request.get("request_id"),"candidate_sha256":source_sha}
                job_root=Path(queue_root).resolve()/"profile_jobs"
                job_root.mkdir(parents=True,exist_ok=True)
                job_path=job_root/(request["request_id"]+".json")
                job={"schema":"N0_POST_RECEIPT_PROFILE_JOB_V1","status":"WAITING_FOR_TERMINAL_OJ_RECEIPT",
                    "request":request,"source":source,"build_identity":build,"candidate_sha256":source_sha,
                    "profile_receipt_id":request["request_id"],"created_at_utc":datetime.now(timezone.utc).isoformat()}
                raw=(json.dumps(job,ensure_ascii=False,sort_keys=True,separators=(",",":"))+"\n").encode("utf-8")
                if job_path.exists() and job_path.read_bytes()!=raw:
                    raise RuntimeError("POST_RECEIPT_PROFILE_JOB_IDEMPOTENCY_CONFLICT")
                if not job_path.exists():
                    with job_path.open("xb") as stream:
                        stream.write(raw); stream.flush(); os.fsync(stream.fileno())
        timing=(result or {}).get('timing') or {}
        evidence=(result or {}).get('measurement_evidence')
        candidate_ci95=None
        if trust_status=="VALID_MEASUREMENT" and isinstance(evidence,dict):
            try:
                from n0_kernelbench_oj.score_policy import settle_measurement
                interval=settle_measurement(baseline_id=timing['baseline_id'],
                    frozen_reference_ms=timing['frozen_reference_ms'],
                    candidate_samples_ms=evidence['candidate']['raw_samples_ms'],prior_best=None)
                candidate_ci95=interval.get('candidate_runtime_ci95_ms')
            except Exception:
                trust_status="OJ_INFRASTRUCTURE_HOLD_SCORE_UNCERTAINTY_INVALID"
        baseline_evidence=(baseline or {}).get('measurement_evidence') if 'baseline' in locals() else None
        task_sha="8D975D7CEE6946513DCFB6BA84C971E53A890F9D9C6F0A4FB828904941F25975"
        timing_record={"method":"cuda_event","warmup":3,"trials":10,"discard_first":1,
                       "candidate_runtime_ms":timing.get('candidate_runtime_ms'),
                       "kernelbench_reference_runtime_ms":timing.get('kernelbench_reference_runtime_ms'),
                       "reference_control_ms":timing.get('seed_reference_control_ms'),
                       "samples_ms":(evidence or {}).get('candidate',{}).get('raw_samples_ms')}
        candidate_failure=(result or {}).get("candidate_failure") or (result or {}).get("precompile_error") or (result or {}).get("syntax")
        diagnostics={"status":"NONE" if trust_status=="VALID_MEASUREMENT" else trust_status,
                     "official_evaluator":(result or {}).get('official_kernelbench_result'),
                     "candidate_failure":candidate_failure,
                     "external_gpu_interference":interference}
        lineage={"parent_sha256":request.get('parent_sha256'),"version":"N0_NON_SCIENTIFIC_OR_FORMAL_REQUEST"}
        receipt={
          "schema":"N0_OJ_R5_IMMUTABLE_RECEIPT_V1","request_id":request['request_id'],
          "episode_id":request['episode_id'],"candidate_id":request['candidate_id'],
          "candidate_sha256":source_sha,"terminal":True,"task_sha256":task_sha,
          "measurement_protocol":"KERNELBENCH_L1_40_CUDA_EVENT_N0_FROZEN_REFERENCE_SCORE_ONLINE_V2",
          "oj_release_id":oj_release_id,
          "oj_manifest_sha256":sha256_file(Path(release_root)/"RELEASE_MANIFEST.json"),
          "verdict":trust_status,"score":timing.get('kernelbench_speedup') if trust_status=="VALID_MEASUREMENT" else None,
          "score_version":timing.get('score_version'),"baseline_id":timing.get('baseline_id'),
          "candidate_runtime_ci95_ms":candidate_ci95,
          "promotion_confidence_status":"PENDING_HARNESS_ONLINE_BEST_SETTLEMENT",
          "promotion_confidence_protocol":"N0_ONLINE_BEST_MARGIN_1P5_PERCENT_V1",
          "best_settlement_status":"PENDING_HARNESS",
          "independent_process_recheck":None,
          "candidate_runtime_ms":timing.get('candidate_runtime_ms'),
          "kernelbench_reference_runtime_ms":timing.get('kernelbench_reference_runtime_ms'),
          "reference_control_ms":timing.get('seed_reference_control_ms'),
          "measurement_evidence":evidence,
          "candidate_failure_diagnostics":candidate_failure,
          "raw_capture_journal_path":str(capture_path),
          "raw_capture_journal_sha256":_sha(capture_path.read_bytes()) if capture_path.is_file() else None,
          "raw_capture_event_count":capture_sequence,
          "reference_control_evidence":baseline_evidence,
          "capture_id":(evidence or {}).get('candidate',{}).get('capture_id'),
          "timer_identity":{"timer":(evidence or {}).get('candidate',{}).get('protocol',{}).get('method'),
                             "timing_source_sha256":(evidence or {}).get('candidate',{}).get('timing_source_sha256')},
          "cuda_event_samples_ms":(evidence or {}).get('candidate',{}).get('raw_samples_ms'),
          "timing":timing_record,"diagnostics":diagnostics,"lineage":lineage,
          "official_kernelbench_result":(result or {}).get('official_kernelbench_result'),
          "build_identity":build,"gpu_identity":(result or {}).get('gpu_identity',hardware_identity),
          "profile":profile,"external_gpu_interference":interference,
          "gpu_monitor_evidence":monitor.evidence(),
          "created_at_utc":datetime.now(timezone.utc).isoformat(),
          "scientific_classification":("NON_SCIENTIFIC_DIAGNOSTIC" if str(request['episode_id']).startswith('N0DIAG-')
            else ("NON_SCIENTIFIC_SMOKE" if str(request['episode_id']).startswith('N0_SMOKE_') else "FORMAL_CANDIDATE"))
        }
        import hashlib
        from n0_autonomous.engine import canonical_bytes
        receipt["release_id"]=receipt["oj_release_id"]
        receipt["manifest_sha256"]=receipt["oj_manifest_sha256"]
        receipt.pop("receipt_sha256", None)
        receipt["receipt_sha256"]=hashlib.sha256(canonical_bytes(receipt)).hexdigest()
        return receipt
    return evaluate

def validate_r5_receipt(request:dict[str,Any],receipt:dict[str,Any])->bool:
    if not isinstance(receipt,dict): return False
    for k in ('request_id','episode_id','candidate_id','candidate_sha256'):
        if receipt.get(k)!=request.get(k): return False
    if receipt.get('verdict')=='VALID_MEASUREMENT':
        if receipt.get('score') is None or receipt.get('profile',{}).get('status') not in {'PASS','UNAVAILABLE','QUEUED'}: return False
        ev=receipt.get('measurement_evidence') or {}
        trust=ev.get('reference_trust') or {}
        return (trust.get('pass') is True and trust.get('limit')==0.02
                and trust.get('policy')=='N0_POST_EVALUATOR_REFERENCE_PAIRED_V2')
    return receipt.get('score') is None
