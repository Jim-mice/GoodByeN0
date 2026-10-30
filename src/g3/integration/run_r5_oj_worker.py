from __future__ import annotations
import argparse, hashlib, importlib.util, json, os, subprocess, sys, time, uuid
from datetime import datetime, timezone
from pathlib import Path

ROOT=Path(os.environ.get('N0_G3_RUNTIME_ROOT',r'D:\projects\kernelagent\runtime')).resolve()
INTEGRATION=Path(__file__).resolve().parent
RELEASE_ID=INTEGRATION.name
R6=Path(os.environ.get('N0_G3_HARNESS_RELEASE_ROOT',str(ROOT/'harness'/'releases'/'N0_AUTONOMOUS_INFORMATION_ACQUISITION_R7_4_PROFILE_RECEIPT_HANDOFF'))).resolve()
OJ=Path(os.environ.get('N0_G3_OJ_RELEASE_ROOT',str(ROOT/'oj'/'releases'/'N0_KERNELBENCH_L1_40_LAYERNORM_R6_6_KERNEL_NAME_NCU_FILTER'))).resolve()
R6_ID=R6.name
OJ_ID=OJ.name
KERNELBENCH=Path(os.environ.get('N0_G3_KERNELBENCH_ROOT',str(ROOT/'harness'/'releases'/'N0_AUTONOMOUS_INFORMATION_ACQUISITION_R6'/'third_party'/'KernelBench_repo'))).resolve()
STORE_MODULE=R6
PYTHON=Path(os.environ.get('N0_G3_PYTHON',str(ROOT/'.venv_n0_r6'/'Scripts'/'python.exe'))).resolve()
NCU=Path(r'C:\Program Files\NVIDIA Corporation\Nsight Compute 2026.3.0\target\windows-desktop-win7-x64\ncu.exe')
PROFILE_STATE_ROOT=Path(os.environ.get('N0_PROFILE_STATE_ROOT',str(ROOT/'state'/'n0_oj_profiles'))).resolve()
SEED=R6/'protocol'/'SEED_CANDIDATE.cu'
SEED_QUAL=R6/'qualification'/'seed_gpu_qualification.json'
MANIFEST_SHA=hashlib.sha256((INTEGRATION/'RELEASE_MANIFEST.json').read_bytes()).hexdigest().upper()

def sha(raw:bytes)->str: return hashlib.sha256(raw).hexdigest().upper()
def now()->str: return datetime.now(timezone.utc).isoformat()

def resolve_fault_receipt_path(gate:dict,queue_root:Path)->Path:
    """Resolve only the request-scoped receipt under this worker's isolated queue."""
    request_id=str(gate.get('fault_request_id') or '')
    if not request_id or Path(request_id).name!=request_id or request_id in {'.','..'}:
        raise ValueError('INVALID_FAULT_REQUEST_ID')
    candidate=Path(gate.get('fault_receipt_path') or '')
    if candidate.is_file(): return candidate.resolve()
    expected=(queue_root.resolve()/'candidate_supervision'/request_id/'fault_receipt.json')
    if expected.is_file(): return expected.resolve()
    raise FileNotFoundError('FAULT_RECEIPT_MISSING:'+str(expected))

def fault_recovery_gate_document(gate:dict,receipt_sha256:str,passed:bool,failure,updated_at:str,
                                recovery_id:str|None=None,recovery_path:Path|None=None)->dict:
    """Keep fault identity/path durable across failed recovery attempts."""
    return {'schema':'N0_GPU_FAULT_RECOVERY_GATE_V1','status':'READY' if passed else 'HOLD',
        'fault_request_id':gate.get('fault_request_id'),'fault_receipt_path':gate.get('fault_receipt_path'),
        'fault_classification':gate.get('fault_classification') or (gate.get('fault') or {}).get('classification'),
        'fault_candidate_sha256':gate.get('fault_candidate_sha256') or gate.get('candidate_sha256'),
        'fault_receipt_sha256':gate.get('fault_receipt_sha256'),
        'worker_pid':gate.get('worker_pid'),'gpu_lease_id':gate.get('gpu_lease_id'),
        'recovery_receipt_id':recovery_id,
        'recovery_receipt_path':str(recovery_path) if recovery_path else None,
        'recovery_receipt_sha256':receipt_sha256,'updated_at_utc':updated_at,'failure':failure}

def worker_gate_action(status)->str:
    """A failed bounded recovery attempt pauses this worker; it never polls/retries in place."""
    return 'CONTINUE' if status=='READY' else 'HOLD_AND_EXIT'

def serving_worker_exit_code(exit_state:str,handoff_requested:bool)->int:
    """Keep the process exit code aligned with the durable service status."""
    if handoff_requested: return 75
    if exit_state=='HOLD': return 20
    return 0

def recover_unknown_inflight_for_serving(store,recovery_only:bool,owner_liveness)->int:
    """Queue recovery is forbidden during qualification-only bootstrap."""
    if recovery_only: return 0
    return store.recover_unknown_inflight(owner_liveness)

def requires_fault_recovery_handoff(gate:dict)->bool:
    """Candidate-fault recovery must run in a fresh Worker process."""
    return (gate.get('schema')=='N0_GPU_FAULT_RECOVERY_GATE_V1' and
            gate.get('status')!='READY')

def fault_recovery_receipt_path(receipt_root:Path,recovery_id:str)->Path:
    if not recovery_id or Path(recovery_id).name!=recovery_id or recovery_id in {'.','..'}:
        raise ValueError('INVALID_FAULT_RECOVERY_ID')
    return receipt_root/(recovery_id+'.json')
def atomic_json(path:Path,value):
    path.parent.mkdir(parents=True,exist_ok=True); tmp=path.with_name(path.name+'.'+uuid.uuid4().hex+'.tmp')
    tmp.write_text(json.dumps(value,indent=2,ensure_ascii=False)+'\n',encoding='utf-8')
    last=None
    for attempt in range(5):
      try: os.replace(tmp,path); return
      except PermissionError as exc: last=exc; time.sleep(0.05*(attempt+1))
    fallback=path.with_name(path.stem+'.'+str(os.getpid())+path.suffix)
    fallback.write_text(json.dumps(value,indent=2,ensure_ascii=False)+'\n',encoding='utf-8')
    try: tmp.unlink(missing_ok=True)
    except OSError: pass
    print('STATUS_REPLACE_FALLBACK:'+type(last).__name__,file=sys.stderr,flush=True)

def live_device():
    from n0_integration.oj_r5_worker import query_device
    data=query_device(); f=data['fields']
    return {"device_name":f[0],"gpu_uuid":f[1],"driver_version":f[2],"temperature_c":int(f[3]),
            "utilization_percent":int(f[4]),"memory_used_mib":int(f[5]),"sm_clock_mhz":int(f[6]),
            "memory_clock_mhz":int(f[7]),"power_w":float(f[8]),"pstate":f[9],"compute_apps":data['apps']}

def make_qualification(device:dict, output:Path)->dict:
    seed=json.loads(SEED_QUAL.read_text(encoding='utf-8'))
    contract=OJ/'CUDA_COMPILER_CONTRACT.json'; contract_sha=sha(contract.read_bytes())
    manifest=OJ/'RELEASE_MANIFEST.json'
    frozen_baseline_path=OJ/'REFERENCE_BASELINE.json'
    frozen_baseline=json.loads(frozen_baseline_path.read_text(encoding='utf-8')) if frozen_baseline_path.is_file() else {}
    from n0_kernelbench_oj.adapter import verify_upstream_source, TASK_SHA256, REPOSITORY_COMMIT, TASK_RELATIVE
    from n0_integration.readiness import verify_manifest
    upstream=verify_upstream_source(KERNELBENCH)
    import torch
    expected_environment={"python_version":".".join(map(str,sys.version_info[:3])),
      "torch_version":torch.__version__,"torch_cuda_version":torch.version.cuda,
      "kernelbench_commit":REPOSITORY_COMMIT,"timing_source_sha256":upstream.get("hashes",{}).get("timing"),
      "driver_version":device["driver_version"],"compiler_contract_sha256":contract_sha}
    from n0_integration.baseline_qualification import validate_frozen_baseline
    baseline_scope={"task_sha256":TASK_SHA256,"reference_source_sha256":sha((KERNELBENCH/TASK_RELATIVE).read_bytes()),
      "kernelbench_commit":REPOSITORY_COMMIT,"timing_source_sha256":upstream.get("hashes",{}).get("timing"),
      "gpu_identity":{"device_name":device["device_name"],"gpu_uuid":device["gpu_uuid"],
        "driver_version":device["driver_version"],"compute_capability":[12,0]},
      "environment_identity":expected_environment}
    baseline_validation=validate_frozen_baseline(frozen_baseline,expected_scope=baseline_scope)
    lease_path=Path(os.environ.get("N0_GPU_LEASE_PATH",str(ROOT/"state"/"n0_gpu_device.lock")))
    context_attribution=classify_startup_contexts(device.get("compute_apps",[]),
      current_pid=os.getpid(),lease_path=lease_path)
    coordinated_startup_contexts=context_attribution["coordinated"]
    uncoordinated_startup_contexts=context_attribution["uncoordinated"]
    checks={
      "prior_non_scientific_seed_gpu_qualification": seed.get('qualification_type')=='NON_SCIENTIFIC_GPU_QUALIFICATION' and seed.get('seed_sha256','').upper()==sha(SEED.read_bytes()) and seed.get('task_sha256','').upper()==TASK_SHA256 and seed.get('kernelbench_commit')==REPOSITORY_COMMIT,
      "r6_manifest_identity": verify_manifest(manifest,OJ_ID).get('status')=='PASS',
      "qualified_frozen_baseline_identity": baseline_validation.get("status")=="PASS",
      "frozen_toolchain_identity": json.loads(contract.read_text(encoding='utf-8')).get('status')=='FROZEN_PENDING_QUALIFICATION' and contract_sha=='303AF7065B56C8A29ED370391681F06C1F31EF5066632D82B7141BF0AEB9C1FA',
      "kernelbench_identity":upstream.get('status')=='PASS' and upstream.get('commit')==REPOSITORY_COMMIT,
      "live_gpu_identity":device['device_name']=='NVIDIA GeForce RTX 5060 Laptop GPU' and device['gpu_uuid']=='GPU-15840d68-8974-35a8-6886-d7a36fb0a04d' and device['driver_version']=='610.88',
      "gpu_idle_and_attributable":device['utilization_percent']<=5 and not uncoordinated_startup_contexts,
      "ncu_available":NCU.is_file() and PYTHON.is_file(),
    }
    result={"classification":"NON_SCIENTIFIC_QUEUE_INTEGRATION_ONLY","status":"PASS" if all(checks.values()) else "BLOCKED",
      "checks":checks,"checked_at_utc":now(),"device_snapshot":device,"upstream":upstream,
      "frozen_baseline_qualification_validation":baseline_validation,
      "startup_cuda_context_attribution":{"current_worker_pid":os.getpid(),
        "coordinated_same_lease_contexts":coordinated_startup_contexts,
        "uncoordinated_contexts":uncoordinated_startup_contexts},
      "r6_manifest_sha256":sha(manifest.read_bytes()),"frozen_baseline_sha256":sha(frozen_baseline_path.read_bytes()) if frozen_baseline_path.is_file() else None,"compiler_contract_sha256":contract_sha,
      "seed_qualification_sha256":sha(SEED_QUAL.read_bytes()),"ncu_binary_sha256":sha(NCU.read_bytes()) if NCU.exists() else None}
    atomic_json(output,result)
    if result['status']!='PASS': raise RuntimeError('NON_SCIENTIFIC_GPU_QUALIFICATION_GATE_FAILED')
    return {"status":"APPROVED_FOR_N0_GPU_QUALIFICATION","task_sha256":TASK_SHA256,"kernelbench_commit":REPOSITORY_COMMIT,
      "gpu_identity":{"device_name":device['device_name'],"gpu_uuid":device['gpu_uuid'],"driver_version":device['driver_version'],"compute_capability":[12,0]},
      "driver_health":"PASS","oj_worker_health":"PASS","compiler_contract_sha256":contract_sha,
      "qualification_evidence_path":str(output),"qualification_evidence_sha256":sha(output.read_bytes()),
      "scope":"NON_SCIENTIFIC_QUEUE_INTEGRATION_ONLY"}

def _process_identity(pid:int)->dict:
    query=(f"$p=Get-CimInstance Win32_Process -Filter 'ProcessId={pid}'; "
           "if($null -eq $p){'{}'}else{$p | Select-Object ProcessId,ParentProcessId,CreationDate,ExecutablePath,CommandLine | ConvertTo-Json -Compress}")
    try:
        out=subprocess.run(['powershell.exe','-NoProfile','-Command',query],capture_output=True,text=True,timeout=8,check=True)
        return json.loads(out.stdout.strip() or '{}')
    except Exception:
        return {}

def _process_descendants(root_pid:int)->list[dict]:
    if not isinstance(root_pid,int) or root_pid<=0: return [{"reason":"SUPERVISOR_PID_UNAVAILABLE"}]
    query=("Get-CimInstance Win32_Process | Select-Object ProcessId,ParentProcessId,Name,ExecutablePath,CommandLine | "
           "ConvertTo-Json -Compress")
    try:
        result=subprocess.run(['powershell.exe','-NoProfile','-Command',query],capture_output=True,text=True,
                              timeout=15,check=True)
        rows=json.loads(result.stdout.strip() or '[]')
        if isinstance(rows,dict): rows=[rows]
        children=[]; parents={root_pid}; remaining=list(rows)
        while True:
            found=[row for row in remaining if int(row.get('ParentProcessId') or 0) in parents]
            if not found: break
            children.extend(found); parents.update(int(row.get('ProcessId') or 0) for row in found)
            remaining=[row for row in remaining if row not in found]
        return children
    except Exception as exc:
        return [{"reason":"PROCESS_TREE_QUERY_FAILED","error":type(exc).__name__+":"+str(exc)}]

def _is_n0_lease_participant(pid:int,lease_path:Path|None=None)->dict|None:
    """Attribute a GPU context to a worker only when its process tree names this Lease path."""
    expected=os.path.normcase(str((lease_path or (ROOT/'state'/'n0_gpu_device.lock')).resolve()))
    seen=set(); current=pid; first=None
    for _ in range(16):
        if current in seen or current<=0: break
        seen.add(current)
        item=_process_identity(current)
        if not item: break
        if first is None: first=item
        command=str(item.get('CommandLine') or '')
        normalized=os.path.normcase(command.replace('"',''))
        if ('run_r5_oj_worker.py' in command and '--gpu-lease-path' in command
                and expected in normalized):
            return {"pid":pid,"parent_pid":(first or {}).get('ParentProcessId'),
                "executable_path":(first or {}).get('ExecutablePath'),"command_line":(first or {}).get('CommandLine'),
                "lease_owner_pid":current,"lease_owner_command_line":command,"lease_path":str((lease_path or (ROOT/'state'/'n0_gpu_device.lock')).resolve()),
                "attribution":"PROCESS_ANCESTOR_BOUND_TO_SHARED_GPU_LEASE",
                "execution_contract":"all_evaluation_and_profile_work_under_shared_gpu_lease"}
        if ('candidate_eval_child.py' in command and '--gpu-lease-path' in command
                and expected in normalized):
            return {"pid":pid,"parent_pid":(first or {}).get("ParentProcessId"),
                "executable_path":(first or {}).get("ExecutablePath"),"command_line":(first or {}).get("CommandLine"),
                "lease_owner_pid":None,"lease_path":str((lease_path or (ROOT/'state'/'n0_gpu_device.lock')).resolve()),
                "attribution":"CANDIDATE_EVAL_CHILD_BOUND_TO_SHARED_GPU_LEASE",
                "execution_contract":"candidate_evaluation_child_of_lease_owner"}
        try: current=int(item.get('ParentProcessId') or 0)
        except (TypeError,ValueError): break
    return None

def classify_startup_contexts(apps:list[str],*,current_pid:int,lease_path:Path)->dict:
    coordinated=[]; uncoordinated=[]
    for app in apps:
      try: app_pid=int(str(app).split(",",1)[0].strip())
      except Exception: app_pid=-1
      if app_pid==current_pid: continue
      participant=_is_n0_lease_participant(app_pid,lease_path) if app_pid>0 else None
      if participant is not None: coordinated.append(participant)
      else: uncoordinated.append({"pid":app_pid,"raw":app})
    return {"coordinated":coordinated,"uncoordinated":uncoordinated}

def health_check(*,ignore_post_profile_gate:bool=False):
    gate_path=PROFILE_STATE_ROOT/'timing_gate.json'
    if not ignore_post_profile_gate and gate_path.is_file():
        try:
            gate=json.loads(gate_path.read_text(encoding='utf-8'))
            if gate.get('status')!='READY':
                return {'status':'BLOCKED','gpu_identity':'GPU-15840d68-8974-35a8-6886-d7a36fb0a04d',
                    'uncoordinated_gpu_workloads':[],'post_profile_gate':gate,
                    'failure':'POST_PROFILE_REFERENCE_RECOVERY_NOT_QUALIFIED'}
        except Exception as exc:
            return {'status':'BLOCKED','gpu_identity':'GPU-15840d68-8974-35a8-6886-d7a36fb0a04d',
                'uncoordinated_gpu_workloads':[],'failure':'POST_PROFILE_GATE_READ_FAILED:'+type(exc).__name__}
    from n0_integration.oj_r5_worker import query_device
    audit_path=os.environ.get('N0_HEALTH_GATE_AUDIT_PATH')
    lease_path=Path(os.environ.get("N0_GPU_LEASE_PATH",str(ROOT/'state'/'n0_gpu_device.lock')))
    observations=[]; clean=0; started=time.monotonic(); deadline=started+5.0
    while True:
        d=query_device(); f=d['fields']; foreign=[]; coordinated=[]
        for row in d['apps']:
            try: pid=int(row.split(',',1)[0].strip())
            except Exception: pid=-1
            if pid==os.getpid():
                continue
            participant=_is_n0_lease_participant(pid,lease_path) if pid>0 else None
            if participant is not None: coordinated.append(participant)
            else: foreign.append({"pid":pid,"raw":row,"process_identity":_process_identity(pid) if pid>0 else {}})
        identity_ok=f[0]=='NVIDIA GeForce RTX 5060 Laptop GPU' and f[1]=='GPU-15840d68-8974-35a8-6886-d7a36fb0a04d'
        util=int(f[4]); clean=clean+1 if identity_ok and util<=5 and not foreign else 0
        sample={"checked_at_utc":now(),"device_snapshot":{"fields":f,"apps":d['apps']},
            "uncoordinated_gpu_workloads":foreign,"coordinated_n0_lease_participants":coordinated,
            "gpu_identity_ok":identity_ok,"utilization_percent":util,"consecutive_quiet_samples":clean}
        observations.append(sample)
        if audit_path:
            from n0_integration.oj_r5_worker import _append_durable_jsonl
            _append_durable_jsonl(Path(audit_path),{"schema":"N0_GPU_LEASE_HEALTH_SAMPLE_V1","worker_pid":os.getpid(),**sample})
        if not identity_ok or foreign:
            break
        if clean>=3:
            break
        if time.monotonic()>=deadline:
            break
        time.sleep(0.5)
    status='PASS' if identity_ok and not foreign and clean>=3 else 'BLOCKED'
    return {"status":status,"gpu_identity":f[1],"uncoordinated_gpu_workloads":foreign,
        "coordinated_n0_lease_participants":coordinated,"device_snapshot":{"fields":f,"apps":d['apps']},
        "settle_protocol":"THREE_CONSECUTIVE_SNAPSHOTS_UTIL_LE_5_PERCENT_500MS_INTERVAL_MAX_5S",
        "settle_samples":len(observations),"settle_duration_seconds":round(time.monotonic()-started,3),
        "observations":observations,"checked_at_utc":now()}

def process_liveness(row):
    pid=row.get('worker_pid')
    if not isinstance(pid,int): return 'UNKNOWN'
    q=f"$p=Get-CimInstance Win32_Process -Filter 'ProcessId={pid}'; if ($null -eq $p) {{ '{{}}' }} else {{ $p | Select-Object ProcessId,ParentProcessId,CreationDate,ExecutablePath,CommandLine | ConvertTo-Json -Compress }}"
    try:
        p=subprocess.run(['powershell.exe','-NoProfile','-Command',q],capture_output=True,text=True,timeout=10)
        d=json.loads(p.stdout.strip() or '{}')
        if not d: return 'DEAD'
        if str(row.get('worker_start_utc',''))==str(d.get('CreationDate','')): return 'ALIVE'
        return 'UNKNOWN'
    except Exception: return 'UNKNOWN'

def build_post_receipt_profile_receipt(*, row:dict, profile:dict, started:str,
        finished:str, duration_seconds:float, lease_info:dict|None,
        monitor_events:list, recovery_pending:bool, recovery_pass:bool,
        failure:str|None, checks:list)->dict:
    rid=str(row['request_id'])
    profile_identity=(profile.get('request_id')==rid and profile.get('episode_id')==row['episode_id'] and
                      profile.get('candidate_id')==row['candidate_id'] and
                      str(profile.get('candidate_sha256','')).lower()==str(row['candidate_sha256']).lower())
    profile_pass=(profile.get('status')=='PASS' and profile_identity and profile.get('score_inclusion') is False)
    body={'schema':'N0_POST_RECEIPT_PROFILE_RECEIPT_V1','profile_receipt_id':rid,
        'request_id':rid,'episode_id':row['episode_id'],'candidate_id':row['candidate_id'],
        'candidate_sha256':row['candidate_sha256'],'oj_receipt_sha256':row['receipt_sha256'],
        'status':'PASS' if profile_pass else 'UNAVAILABLE','profile':profile,
        'score_inclusion':False,'started_at_utc':started,'finished_at_utc':finished,
        'profile_duration_seconds':duration_seconds,'lease':lease_info,'gpu_monitor_events':monitor_events,
        'recovery_status':'PENDING_FRESH_PROCESS' if recovery_pending else ('PASS' if recovery_pass else 'HOLD'),
        'recovery_failure':failure if failure else ('FRESH_PROCESS_RECOVERY_REQUIRED' if recovery_pending else (None if recovery_pass else 'THREE_REFERENCE_CONTROLS_OR_DEVICE_HEALTH_FAILED')),
        'reference_recovery_controls':checks,'next_scored_measurement_allowed':recovery_pass}
    from n0_autonomous.engine import canonical_bytes
    body['profile_receipt_sha256']=hashlib.sha256(canonical_bytes(body)).hexdigest()
    return body


def run_post_receipt_profile(store, queue_root:Path, profile_runner, lease_path:Path,
                             qualification:dict, hardware_identity:dict)->dict|None:
    """Run NCU only after the immutable OJ receipt is durable, under a new lease."""
    snap=store.snapshot()
    rows={x['request_id']:x for x in snap['oj_requests']}
    jobs=Path(queue_root)/'profile_jobs'
    if not jobs.is_dir(): return None
    profile_root=PROFILE_STATE_ROOT/'profile_receipts'
    profile_root.mkdir(parents=True,exist_ok=True)
    for job_path in sorted(jobs.glob('*.json')):
        job=json.loads(job_path.read_text(encoding='utf-8'))
        request=job.get('request') or {}; rid=request.get('request_id')
        row=rows.get(rid)
        if not row or row.get('state') not in {'COMPLETED','CONSUMED'} or not row.get('receipt_json') or not row.get('receipt_sha256'):
            continue
        oj_receipt=json.loads(row['receipt_json'])
        from n0_autonomous.engine import canonical_bytes
        stored_receipt_sha=hashlib.sha256(canonical_bytes(oj_receipt)).hexdigest()
        if (stored_receipt_sha!=row['receipt_sha256'] or
                any(oj_receipt.get(k)!=row.get(k) for k in ('request_id','episode_id','candidate_id','candidate_sha256')) or
                any(request.get(k)!=row.get(k) for k in ('request_id','episode_id','candidate_id','candidate_sha256')) or
                oj_receipt.get('profile',{}).get('status')!='QUEUED' or
                job.get('candidate_sha256','').lower()!=str(row.get('candidate_sha256','')).lower() or
                request.get('candidate_sha256','').lower()!=str(row.get('candidate_sha256','')).lower()):
            raise RuntimeError('POST_RECEIPT_PROFILE_JOB_OJ_RECEIPT_IDENTITY_MISMATCH')
        receipt_path=profile_root/(rid+'.json')
        if receipt_path.is_file():
            from n0_autonomous.engine import canonical_bytes
            saved=json.loads(receipt_path.read_text(encoding='utf-8'))
            saved_body={k:v for k,v in saved.items() if k!='profile_receipt_sha256'}
            if (saved.get('request_id')!=rid or saved.get('oj_receipt_sha256')!=row['receipt_sha256']
                    or hashlib.sha256(canonical_bytes(saved_body)).hexdigest()!=saved.get('profile_receipt_sha256')):
                raise RuntimeError('POST_RECEIPT_PROFILE_RECOVERY_SIDECAR_IDENTITY_MISMATCH')
            atomic_json(PROFILE_STATE_ROOT/'timing_gate.json',{
                'schema':'N0_POST_PROFILE_TIMING_GATE_V1','status':'READY' if saved.get('recovery_status')=='PASS' else 'HOLD',
                'last_profile_receipt_id':rid,'last_profile_receipt_sha256':saved.get('profile_receipt_sha256'),
                'recovered_after_worker_restart':True,'updated_at_utc':now()})
            job_path.unlink(missing_ok=True)
            continue
        from n0_autonomous.gpu_lease import GpuExclusiveLease
        from n0_integration.oj_r5_worker import ExternalGpuMonitor,run_profile_safely,query_device
        from n0_kernelbench_oj.adapter import generate_reference_baseline
        from n0_kernelbench_oj.measurement_trust import assess_against_frozen_baseline
        fresh_process_handoff=os.environ.get('N0_PROFILE_FRESH_PROCESS_HANDOFF')=='1'
        started=now(); started_tick=time.monotonic(); monitor=ExternalGpuMonitor(
            coordinated_pid_resolver=lambda pid:_is_n0_lease_participant(pid,lease_path))
        profile={'status':'UNAVAILABLE','reason':'PROFILE_STAGE_NOT_STARTED'}; checks=[]; lease_info=None
        try:
            lease=GpuExclusiveLease(lease_path,health_check=health_check,
                owner={'worker_id':'N0_POST_RECEIPT_PROFILE','request_id':rid,
                       'oj_receipt_sha256':row['receipt_sha256'],'candidate_sha256':row['candidate_sha256']})
            with lease:
                lease_info={'lease_id':lease.owner['lease_id'],'pid':os.getpid(),'acquired_at_utc':now()}
                from n0_integration.oj_r5_worker import _append_durable_jsonl
                cost_log=PROFILE_STATE_ROOT/'profile_cost_events.jsonl'
                _append_durable_jsonl(cost_log,{'schema':'N0_OJ_AUXILIARY_PROFILE_COST_V1','event':'PROFILE_STAGE_STARTED',
                    'request_id':rid,'oj_receipt_sha256':row['receipt_sha256'],'candidate_sha256':row['candidate_sha256'],
                    'lease_id':lease.owner['lease_id'],'worker_pid':os.getpid(),'time_utc':now()})
                monitor.start()
                profile=run_profile_safely(profile_runner,request,job['source'],job['build_identity'],monitor)
                monitor_events=monitor.stop()
                if not fresh_process_handoff:
                    for index in range(3):
                        measured=generate_reference_baseline(repository_root=KERNELBENCH,qualification=qualification,
                            hardware_identity=hardware_identity,device=0)
                        trust=assess_against_frozen_baseline(measured['measurement_evidence'],measured['frozen_baseline'])
                        trust['post_profile_limit']=0.02
                        trust['pass']=bool(trust.get('pass') is True and trust.get('relative_deviation',1.0)<=0.02)
                        device=query_device(); fields=device['fields']
                        foreign=[]
                        for app in device['apps']:
                            try: app_pid=int(app.split(',',1)[0].strip())
                            except Exception: app_pid=-1
                            if app_pid!=os.getpid() and not (app_pid>0 and _is_n0_lease_participant(app_pid,lease_path)):
                                foreign.append(app)
                        quiet=(fields[0]==hardware_identity['device_name'] and fields[1]==hardware_identity['gpu_uuid']
                               and int(fields[4])<=5 and not foreign)
                        checks.append({'index':index+1,'time_utc':now(),'mean_ms':measured['measurement_evidence']['summary']['mean_ms'],
                            'raw_samples_ms':measured['measurement_evidence']['raw_samples_ms'],
                            'baseline_check':trust,'device_snapshot':device,'quiet_and_identity_ok':quiet})
        except Exception as exc:
            monitor_events=monitor.stop()
            failure=type(exc).__name__+':'+str(exc)
        else:
            failure=None
        recovery_pending=fresh_process_handoff
        recovery_pass=(not recovery_pending and len(checks)==3 and all(x['baseline_check'].get('pass') is True and
                       x['quiet_and_identity_ok'] for x in checks) and not monitor_events)
        body=build_post_receipt_profile_receipt(row=row,profile=profile,started=started,finished=now(),
            duration_seconds=round(time.monotonic()-started_tick,6),lease_info=lease_info,
            monitor_events=monitor_events,recovery_pending=recovery_pending,recovery_pass=recovery_pass,
            failure=failure,checks=checks)
        from n0_autonomous.engine import canonical_bytes
        raw=canonical_bytes(body)+b'\n'
        try:
            from n0_integration.oj_r5_worker import _append_durable_jsonl
            _append_durable_jsonl(PROFILE_STATE_ROOT/'profile_cost_events.jsonl',{
                'schema':'N0_OJ_AUXILIARY_PROFILE_COST_V1','event':'PROFILE_STAGE_FINISHED',
                'request_id':rid,'oj_receipt_sha256':row['receipt_sha256'],
                'profile_receipt_sha256':body['profile_receipt_sha256'],'worker_pid':os.getpid(),
                'duration_seconds':body['profile_duration_seconds'],'recovery_status':body['recovery_status'],'time_utc':now()})
        except Exception:
            pass
        with receipt_path.open('xb') as stream:
            stream.write(raw); stream.flush(); os.fsync(stream.fileno())
        gate={'schema':'N0_POST_PROFILE_TIMING_GATE_V1','status':'HANDOFF_REQUIRED' if recovery_pending else ('READY' if recovery_pass else 'HOLD'),
            'last_profile_receipt_id':rid,'last_profile_receipt_sha256':body['profile_receipt_sha256'],
            'reference_checks_passed':sum(1 for x in checks if x.get('baseline_check',{}).get('pass') is True),
            'updated_at_utc':now()}
        atomic_json(PROFILE_STATE_ROOT/'timing_gate.json',gate)
        job_path.unlink(missing_ok=True)
        return body
    return None

def recover_post_profile_timing_gate(lease_path:Path,qualification:dict,hardware_identity:dict,
                                     *,max_controls:int=5,required_consecutive:int=3,
                                     preserve_fault_gate:bool=False,bootstrap_context:dict|None=None)->dict|None:
    """Requalify a HOLD device using measured controls; never clear HOLD by sleep or idle-only probes."""
    gate_path=PROFILE_STATE_ROOT/'timing_gate.json'
    if not gate_path.is_file(): return None
    gate=json.loads(gate_path.read_text(encoding='utf-8'))
    if gate.get('status')=='READY': return None
    if gate.get('schema')=='N0_GPU_FAULT_RECOVERY_GATE_V1':
        if bootstrap_context is not None:
            if (gate.get('status')!='HOLD' or gate.get('fault_request_id')!=bootstrap_context.get('fault_request_id') or
                    hashlib.sha256(gate_path.read_bytes()).hexdigest().upper()!=bootstrap_context.get('fault_gate_sha256')):
                return {'status':'HOLD','reason':'BOOTSTRAP_FAULT_GATE_IDENTITY_CHANGED'}
        queue_root=Path(os.environ.get('N0_OJ_QUEUE_ROOT',str(ROOT/'state'/'n0_oj_queue')))
        try:
            fault_path=resolve_fault_receipt_path(gate,queue_root)
        except (ValueError,FileNotFoundError) as exc:
            return {'status':'HOLD','reason':str(exc),'fault_request_id':gate.get('fault_request_id')}
        if str(gate.get('fault_receipt_path') or '')!=str(fault_path):
            if preserve_fault_gate:
                return {'status':'HOLD','reason':'BOOTSTRAP_FAULT_RECEIPT_PATH_MISMATCH',
                        'fault_request_id':gate.get('fault_request_id')}
            gate={**gate,'fault_receipt_path':str(fault_path)}
            atomic_json(gate_path,gate)
        if gate.get('status')=='IN_PROGRESS':
            descendants=_process_descendants(int(gate.get('supervisor_pid') or 0))
            if descendants:
                return {'status':'HOLD','reason':'ABANDONED_CANDIDATE_PROCESS_TREE_STILL_PRESENT',
                        'supervisor_pid':gate.get('supervisor_pid'),'descendants':descendants}
            if not fault_path.is_file():
                from n0_integration.cuda_faults import classify_fault
                fault=classify_fault('SUPERVISOR','WORKER_EXIT_DURING_CANDIDATE_EVALUATION',
                    'WORKER_CRASH_RECOVERY','Supervisor exited while the evaluator Job Object was active')
                fault_path.parent.mkdir(parents=True,exist_ok=True)
                atomic_json(fault_path,{'schema':'N0_CANDIDATE_ABORT_RECEIPT_V1',
                    'request_id':gate.get('fault_request_id'),'candidate_sha256':gate.get('candidate_sha256'),
                    'fault':fault,'gpu_recovery_required':True,
                    'job_result':{'process_id':gate.get('child_pid'),'active_processes_after':0,
                        'timed_out':False,'terminated_tree':True,'recovered_after_supervisor_exit':True},
                    'worker_descendants_after_exit':descendants,'recovered_at_utc':now()})
            gate={**gate,'status':'HOLD','reason':'WORKER_EXIT_DURING_CANDIDATE_EVALUATION',
                  'fault_receipt_path':str(fault_path),'detected_at_utc':now()}
            atomic_json(gate_path,gate)
        if not fault_path.is_file(): return {'status':'HOLD','reason':'FAULT_RECEIPT_MISSING'}
        fault_receipt=json.loads(fault_path.read_text(encoding='utf-8'))
        if bootstrap_context is not None:
            if (hashlib.sha256(fault_path.read_bytes()).hexdigest().upper()!=bootstrap_context.get('fault_receipt_sha256') or
                    fault_receipt.get('candidate_sha256')!=bootstrap_context.get('candidate_sha256')):
                return {'status':'HOLD','reason':'BOOTSTRAP_FAULT_RECEIPT_IDENTITY_CHANGED',
                        'fault_request_id':gate.get('fault_request_id')}
        if fault_receipt.get('request_id') not in (None,gate.get('fault_request_id')):
            return {'status':'HOLD','reason':'FAULT_RECEIPT_IDENTITY_MISMATCH',
                    'fault_request_id':gate.get('fault_request_id')}
        job_result=fault_receipt.get('job_result') or {}
        if job_result.get('active_processes_after') != 0:
            return {'status':'HOLD','reason':'CANDIDATE_PROCESS_TREE_NOT_PROVEN_EMPTY',
                    'active_processes_after':job_result.get('active_processes_after')}
        from n0_autonomous.gpu_lease import GpuExclusiveLease
        from n0_kernelbench_oj.adapter import generate_reference_baseline
        from n0_kernelbench_oj.measurement_trust import assess_against_frozen_baseline
        from n0_autonomous.engine import canonical_bytes
        from n0_integration.cuda_faults import fault_recovery_decision
        recovery_id=uuid.uuid4().hex; controls=[]; streak=0; failure=None
        lease=GpuExclusiveLease(lease_path,health_check=lambda:health_check(ignore_post_profile_gate=True),
            owner={'worker_id':'N0_CANDIDATE_FAULT_RECOVERY','fault_request_id':gate.get('fault_request_id'),
                   'recovery_id':recovery_id})
        recovery_lease_owner=None
        try:
            with lease:
                recovery_lease_owner=dict(lease.owner)
                for index in range(max_controls):
                    measured=generate_reference_baseline(repository_root=KERNELBENCH,
                        qualification=qualification,hardware_identity=hardware_identity,device=0)
                    frozen=(measured or {}).get('frozen_baseline') or {}
                    trust=assess_against_frozen_baseline((measured or {}).get('measurement_evidence') or {},frozen)
                    relative=float(trust.get('relative_deviation',float('inf')))
                    trust['recovery_limit']=0.02
                    trust['pass']=bool(trust.get('pass') is True and abs(relative)<=0.02)
                    health=health_check(ignore_post_profile_gate=True)
                    passed=trust.get('pass') is True and health.get('status')=='PASS'
                    streak=streak+1 if passed else 0
                    controls.append({'index':index,'measurement':measured,
                        'health':health,'trust':trust,'consecutive_passes':streak})
                    if streak>=required_consecutive: break
        except Exception as exc:
            failure=type(exc).__name__+':'+str(exc)
        released=(not lease._acquired)
        recovery_decision=fault_recovery_decision(
            active_processes_after=job_result.get('active_processes_after'),
            health_checks=[str((x.get('health') or {}).get('status')) for x in controls],
            reference_checks=[(x.get('trust') or {}).get('pass') is True for x in controls],
            required_consecutive=required_consecutive)
        passed=recovery_decision.get('status')=='READY' and failure is None
        body={'schema':'N0_CANDIDATE_FAULT_RECOVERY_RECEIPT_V1','recovery_id':recovery_id,
            'fault_request_id':gate.get('fault_request_id'),'fault_receipt_sha256':sha(fault_path.read_bytes()),
            'fault_candidate_sha256':fault_receipt.get('candidate_sha256'),
            'oj_receipt_sha256':(bootstrap_context or {}).get('oj_receipt_sha256'),
            'fault_gate_sha256':(bootstrap_context or {}).get('fault_gate_sha256'),
            'baseline_id':((controls[-1].get('trust') or {}).get('baseline_id') if controls else None),
            'status':'PASS' if passed else 'HOLD','required_consecutive_controls':required_consecutive,
            'controls':controls,'decision':recovery_decision,'failure':failure,'completed_at_utc':now(),
            'recovery_worker_pid':os.getpid(),'recovery_lease':{'path':str(Path(lease_path).resolve()),
                'pid':(recovery_lease_owner or {}).get('pid'),'lease_id':(recovery_lease_owner or {}).get('lease_id'),
                'released':released}}
        receipt_root=PROFILE_STATE_ROOT/'fault_recovery_receipts'; receipt_root.mkdir(parents=True,exist_ok=True)
        body['recovery_receipt_sha256']=hashlib.sha256(canonical_bytes(body)).hexdigest()
        recovery_path=fault_recovery_receipt_path(receipt_root,recovery_id)
        receipt_raw=canonical_bytes(body)+b'\n'
        file_sha=hashlib.sha256(receipt_raw).hexdigest().upper()
        with recovery_path.open('xb') as stream:
            stream.write(receipt_raw); stream.flush(); os.fsync(stream.fileno())
        if not preserve_fault_gate:
            atomic_json(gate_path,fault_recovery_gate_document(gate,body['recovery_receipt_sha256'],passed,
                failure,now(),recovery_id,recovery_path))
        body['recovery_receipt_path']=str(recovery_path)
        body['recovery_receipt_file_sha256']=file_sha
        return body
    profile_id=gate.get('last_profile_receipt_id')
    if not profile_id: return {'status':'HOLD','reason':'PROFILE_RECEIPT_ID_MISSING'}
    profile_path=PROFILE_STATE_ROOT/'profile_receipts'/(str(profile_id)+'.json')
    if not profile_path.is_file(): return {'status':'HOLD','reason':'PROFILE_RECEIPT_MISSING'}
    profile_receipt=json.loads(profile_path.read_text(encoding='utf-8'))
    from n0_autonomous.engine import canonical_bytes
    profile_body={k:v for k,v in profile_receipt.items() if k!='profile_receipt_sha256'}
    if (profile_receipt.get('request_id')!=profile_id or profile_receipt.get('profile_receipt_id')!=profile_id or
            not profile_receipt.get('oj_receipt_sha256') or
            hashlib.sha256(canonical_bytes(profile_body)).hexdigest()!=profile_receipt.get('profile_receipt_sha256') or
            profile_receipt.get('profile_receipt_sha256')!=gate.get('last_profile_receipt_sha256')):
        return {'status':'HOLD','reason':'PROFILE_RECEIPT_IDENTITY_OR_HASH_MISMATCH','profile_receipt_id':profile_id}
    from n0_autonomous.gpu_lease import GpuExclusiveLease
    from n0_kernelbench_oj.adapter import generate_reference_baseline
    from n0_kernelbench_oj.measurement_trust import assess_against_frozen_baseline
    recovery_id=uuid.uuid4().hex
    started=now(); started_tick=time.monotonic(); controls=[]; streak=0; failure=None
    lease=GpuExclusiveLease(lease_path,health_check=lambda:health_check(ignore_post_profile_gate=True),
        owner={'worker_id':'N0_POST_PROFILE_REFERENCE_RECOVERY','profile_receipt_id':profile_id,
               'profile_receipt_sha256':profile_receipt.get('profile_receipt_sha256'),'recovery_id':recovery_id})
    try:
        with lease:
            for index in range(max_controls):
                measured=generate_reference_baseline(repository_root=KERNELBENCH,qualification=qualification,
                    hardware_identity=hardware_identity,device=0)
                trust=assess_against_frozen_baseline(measured['measurement_evidence'],measured['frozen_baseline'])
                relative=float(trust.get('relative_deviation',float('inf')))
                trust['recovery_limit']=0.02
                trust['pass']=bool(trust.get('pass') is True and relative<=0.02)
                health=health_check(ignore_post_profile_gate=True)
                passed=trust['pass'] is True and health.get('status')=='PASS'
                streak=streak+1 if passed else 0
                controls.append({'index':index+1,'time_utc':now(),'mean_ms':measured['measurement_evidence']['summary']['mean_ms'],
                    'raw_samples_ms':measured['measurement_evidence']['raw_samples_ms'],'baseline_check':trust,
                    'health_check':health,'passed':passed,'consecutive_passes':streak})
                if streak>=required_consecutive: break
    except Exception as exc:
        failure=type(exc).__name__+':'+str(exc)
    recovered=streak>=required_consecutive and failure is None
    body={'schema':'N0_POST_PROFILE_TIMING_RECOVERY_RECEIPT_V1','recovery_id':recovery_id,
        'parent_profile_receipt_id':profile_id,'parent_profile_receipt_sha256':profile_receipt.get('profile_receipt_sha256'),
        'status':'PASS' if recovered else 'HOLD','recovery_limit':0.02,'required_consecutive_controls':required_consecutive,
        'max_controls':max_controls,'controls':controls,'failure':failure,
        'started_at_utc':started,'finished_at_utc':now(),'duration_seconds':round(time.monotonic()-started_tick,6),
        'lease_id':lease.owner.get('lease_id'),'gpu_identity':hardware_identity}
    body['recovery_receipt_sha256']=hashlib.sha256(canonical_bytes(body)).hexdigest()
    recovery_root=PROFILE_STATE_ROOT/'profile_recovery_receipts'; recovery_root.mkdir(parents=True,exist_ok=True)
    recovery_path=recovery_root/(recovery_id+'.json')
    with recovery_path.open('xb') as stream:
        stream.write(canonical_bytes(body)+b'\n'); stream.flush(); os.fsync(stream.fileno())
    atomic_json(gate_path,{'schema':'N0_POST_PROFILE_TIMING_GATE_V1','status':'READY' if recovered else 'HOLD',
        'last_profile_receipt_id':profile_id,'last_profile_receipt_sha256':profile_receipt.get('profile_receipt_sha256'),
        'last_recovery_receipt_id':recovery_id,'last_recovery_receipt_sha256':body['recovery_receipt_sha256'],
        'reference_checks_passed':streak,'recovery_controls_attempted':len(controls),'updated_at_utc':now()})
    return {'status':'READY' if recovered else 'HOLD','recovery_receipt_id':recovery_id,
            'recovery_receipt_sha256':body['recovery_receipt_sha256'],'controls_attempted':len(controls),
            'consecutive_passes':streak,'failure':failure}

def status_write(path,*,state,pid,started,queue_root,manifest_sha,detail=None):
    atomic_json(path,{"schema":"N0_OJ_WORKER_STATUS_V1","state":state,"pid":pid,"process_creation_date":started,
      "heartbeat_utc":now(),"launch_generation":os.environ.get('N0_LAUNCH_GENERATION'),"queue_root":str(queue_root),
      "release_id":RELEASE_ID,"manifest_sha256":manifest_sha,"detail":detail})

def require_torch_runtime():
    expected = PYTHON.resolve()
    actual = Path(sys.executable).resolve()
    if os.path.normcase(str(actual)) != os.path.normcase(str(expected)):
        raise RuntimeError('OJ_WORKER_INTERPRETER_MISMATCH_BEFORE_QUEUE_CLAIM:' + str(actual))
    if importlib.util.find_spec('torch') is None:
        raise RuntimeError('OJ_WORKER_PYTORCH_MISSING_BEFORE_QUEUE_CLAIM')
    import torch
    if not torch.cuda.is_available():
        raise RuntimeError('OJ_WORKER_CUDA_UNAVAILABLE_BEFORE_QUEUE_CLAIM')
    if tuple(torch.cuda.get_device_capability(0)) != (12, 0):
        raise RuntimeError('OJ_WORKER_GPU_COMPUTE_CAPABILITY_MISMATCH_BEFORE_QUEUE_CLAIM')
    from n0_integration.oj_r5_worker import query_device
    device = query_device()
    fields = device.get('fields') or []
    if len(fields) < 2 or fields[1] != 'GPU-15840d68-8974-35a8-6886-d7a36fb0a04d':
        raise RuntimeError('OJ_WORKER_GPU_UUID_MISMATCH_BEFORE_QUEUE_CLAIM')
    if importlib.util.find_spec('torch.utils.cpp_extension') is None:
        raise RuntimeError('OJ_WORKER_EXTENSION_DEPENDENCY_MISSING_BEFORE_QUEUE_CLAIM')
    contract = OJ / 'CUDA_COMPILER_CONTRACT.json'
    if not contract.is_file():
        raise RuntimeError('OJ_WORKER_COMPILER_CONTRACT_MISSING_BEFORE_QUEUE_CLAIM')
    contract_doc = json.loads(contract.read_text(encoding='utf-8-sig'))
    toolkit = contract_doc.get('toolkit') or {}
    nvcc = Path(str(toolkit.get('nvcc_path', '')))
    ptxas = nvcc.parent / 'ptxas.exe'
    host_compiler = Path(str((contract_doc.get('host_compiler') or {}).get('path', '')))
    if not nvcc.is_file():
        raise RuntimeError('OJ_WORKER_NVCC_MISSING_BEFORE_QUEUE_CLAIM')
    if not ptxas.is_file() or not host_compiler.is_file() or not (PYTHON.parent / 'ninja.exe').is_file():
        raise RuntimeError('OJ_WORKER_EXTENSION_BUILD_DEPENDENCY_MISSING_BEFORE_QUEUE_CLAIM')
    return {'sys_executable': str(actual), 'torch_version': torch.__version__,
            'torch_cuda_version': torch.version.cuda, 'cuda_available': True,
            'gpu_name': torch.cuda.get_device_name(0), 'gpu_uuid': fields[1],
            'compute_capability': list(torch.cuda.get_device_capability(0)),
            'compiler_contract_sha256': sha(contract.read_bytes()), 'nvcc_path': str(nvcc),
            'ptxas_path': str(ptxas), 'host_compiler_path': str(host_compiler),
            'ninja_path': str(PYTHON.parent / 'ninja.exe')}

def main():
    p=argparse.ArgumentParser(); p.add_argument('--queue-root',required=True); p.add_argument('--once',action='store_true'); p.add_argument('--recovery-only',action='store_true'); p.add_argument('--bootstrap-qualification-only',action='store_true'); p.add_argument('--qualification-out',required=True); p.add_argument('--profile-whitelist',required=True); p.add_argument('--gpu-lease-path',default=str(ROOT/'state'/'n0_gpu_device.lock')); p.add_argument('--idle-seconds',type=float,default=1.0); a=p.parse_args()
    if a.bootstrap_qualification_only and not a.recovery_only:
      raise RuntimeError('BOOTSTRAP_QUALIFICATION_REQUIRES_RECOVERY_ONLY')
    os.environ['N0_GPU_LEASE_PATH']=str(Path(a.gpu_lease_path).resolve())
    environment_identity = require_torch_runtime()
    sys.path.insert(0,str(INTEGRATION)); sys.path.insert(0,str(R6)); sys.path.insert(0,str(OJ)); sys.path.insert(0,str(KERNELBENCH/'src'))
    # PyTorch's extension loader discovers Ninja through PATH, not the active
    # interpreter's Scripts directory. Keep the pinned environment explicit.
    os.environ['PATH']=str(PYTHON.parent)+os.pathsep+os.environ.get('PATH','')
    from n0_autonomous.parallel_runtime import N0PersistentQueue
    from n0_autonomous.gpu_lease import run_one_oj_request
    from n0_integration.oj_r5_worker import make_r5_queue_evaluator,validate_r5_receipt
    from n0_integration.ncu_profile import NcuProfileRunner
    queue_root=Path(a.queue_root).resolve(); queue_root.mkdir(parents=True,exist_ok=True)
    os.environ['N0_OJ_QUEUE_ROOT']=str(queue_root)
    os.environ['N0_HEALTH_GATE_AUDIT_PATH']=str(queue_root/'health_gate_observations.jsonl')
    os.environ['N0_PROFILE_STATE_ROOT']=str(PROFILE_STATE_ROOT)
    os.environ['N0_GPU_TIMING_GATE_PATH']=str(PROFILE_STATE_ROOT/'timing_gate.json')
    report_root=ROOT/'reports'/'n0_timing_qualification'; status_path=queue_root/'service_status.json'
    proc_doc=json.loads(subprocess.check_output(['powershell.exe','-NoProfile','-Command',f"$p=Get-CimInstance Win32_Process -Filter 'ProcessId={os.getpid()}'; $p | Select-Object ProcessId,ParentProcessId,CreationDate,ExecutablePath,CommandLine | ConvertTo-Json -Compress"],text=True).strip())
    started=str(proc_doc['CreationDate'])
    process_identity={'pid':os.getpid(),'parent_pid':proc_doc.get('ParentProcessId'),
      'creation_date':proc_doc.get('CreationDate'),'executable_path':proc_doc.get('ExecutablePath'),
      'command_line':proc_doc.get('CommandLine'),'sys_executable':str(Path(sys.executable).resolve()),
      'started_at_utc':now(),'queue_root':str(Path(a.queue_root).resolve()),
      'gpu_lease_path':str(Path(a.gpu_lease_path).resolve())}
    os.environ['N0_LAUNCH_GENERATION']=uuid.uuid4().hex
    device=live_device(); qual=make_qualification(device,Path(a.qualification_out))
    whitelist=json.loads(Path(a.profile_whitelist).read_text(encoding='utf-8'))
    profile_runner=NcuProfileRunner(root=PROFILE_STATE_ROOT,python=PYTHON,ncu=NCU,
      target_script=INTEGRATION/'n0_integration'/'ncu_target.py',whitelist=whitelist,gpu_lease_path=Path(a.gpu_lease_path))
    evaluator=make_r5_queue_evaluator(queue_root=queue_root,repository_root=KERNELBENCH,release_root=OJ,
      qualification=qual,hardware_identity=qual['gpu_identity'],profile_runner=profile_runner,
      monitor_factory=lambda:__import__("n0_integration.oj_r5_worker",fromlist=["ExternalGpuMonitor"]).ExternalGpuMonitor(
        coordinated_pid_resolver=lambda pid:_is_n0_lease_participant(pid,Path(a.gpu_lease_path))))
    from n0_integration.supervised_evaluator import make_supervised_evaluator
    evaluator=make_supervised_evaluator(queue_root=queue_root,integration_root=INTEGRATION,
      harness_root=R6,oj_root=OJ,repository_root=KERNELBENCH,qualification=qual,
      hardware_identity=qual['gpu_identity'],python=PYTHON,lease_path=Path(a.gpu_lease_path),
      health_check=health_check,monitor_factory=lambda:__import__("n0_integration.oj_r5_worker",
        fromlist=["ExternalGpuMonitor"]).ExternalGpuMonitor(
          coordinated_pid_resolver=lambda pid:_is_n0_lease_participant(pid,Path(a.gpu_lease_path))))
    status_write(status_path,state='RUNNING',pid=os.getpid(),started=started,queue_root=queue_root,manifest_sha=MANIFEST_SHA,
      detail={'environment_identity':environment_identity,'process_identity':process_identity})
    if a.recovery_only:
      recovery_context=json.loads(os.environ.get('N0_FAULT_RECOVERY_CONTEXT_JSON','{}')) or None
      recovery=recover_post_profile_timing_gate(Path(a.gpu_lease_path),qual,qual['gpu_identity'],
        preserve_fault_gate=a.bootstrap_qualification_only,bootstrap_context=recovery_context)
      gate_path=PROFILE_STATE_ROOT/'timing_gate.json'
      gate=json.loads(gate_path.read_text(encoding='utf-8')) if gate_path.is_file() else {'status':'READY'}
      final_state=('RECOVERY_QUALIFIED' if a.bootstrap_qualification_only and recovery and recovery.get('status')=='PASS'
                   else ('READY' if gate.get('status')=='READY' else 'HOLD'))
      status_write(status_path,state=final_state,pid=os.getpid(),started=started,queue_root=queue_root,manifest_sha=MANIFEST_SHA,
        detail={'environment_identity':environment_identity,'process_identity':process_identity,
          'post_profile_recovery':recovery,'timing_gate':gate,'queue_claimed':False,
          'shutdown':'BOOTSTRAP_QUALIFICATION_ONLY' if a.bootstrap_qualification_only else 'RECOVERY_ONLY_NO_QUEUE_CLAIM'})
      if final_state=='HOLD': return 20
      return 0
    store=N0PersistentQueue(queue_root,max_active_episodes=20,worker_count=20,oj_gpu_active_jobs_max=1)
    recover_unknown_inflight_for_serving(store,False,process_liveness)
    stop=queue_root/'STOP_REQUESTED'; handoff_requested=False
    exit_state='STOPPED'; exit_detail={'shutdown':'ORDERLY_STOP_MARKER'}
    try:
      while True:
        if stop.exists(): break
        try:
          gate_path=PROFILE_STATE_ROOT/'timing_gate.json'
          gate=json.loads(gate_path.read_text(encoding='utf-8')) if gate_path.is_file() else {'status':'READY'}
          if requires_fault_recovery_handoff(gate):
            fault_request_id=str(gate.get('fault_request_id') or '')
            if not fault_request_id:
              raise RuntimeError('FAULT_RECOVERY_HANDOFF_REQUEST_ID_MISSING')
            handoff_requested=True; exit_state='HANDOFF_REQUIRED'
            exit_detail={'fault_request_id':fault_request_id,'queue_claimed':False,
              'shutdown':'FRESH_PROCESS_FAULT_RECOVERY_HANDOFF'}
            status_write(status_path,state='HANDOFF_REQUIRED',pid=os.getpid(),started=started,
              queue_root=queue_root,manifest_sha=MANIFEST_SHA,
              detail={'environment_identity':environment_identity,'process_identity':process_identity,**exit_detail})
            break
          # All N0 queues use the same cross-process physical-device lease.
          recovery=recover_post_profile_timing_gate(Path(a.gpu_lease_path),qual,qual['gpu_identity'])
          gate_path=PROFILE_STATE_ROOT/'timing_gate.json'
          gate=json.loads(gate_path.read_text(encoding='utf-8')) if gate_path.is_file() else {'status':'READY'}
          if worker_gate_action(gate.get('status'))=='HOLD_AND_EXIT':
            exit_state='HOLD'; exit_detail={'post_profile_recovery':recovery,'timing_gate':gate,
              'queue_claimed':False,'shutdown':'BOUNDED_RECOVERY_ATTEMPT_EXHAUSTED'}
            status_write(status_path,state='HOLD',pid=os.getpid(),started=started,queue_root=queue_root,manifest_sha=MANIFEST_SHA,
              detail={'environment_identity':environment_identity,'process_identity':process_identity,
                'post_profile_recovery':recovery,'timing_gate':gate,'queue_claimed':False})
            break
          # A queued profiling stage is itself GPU work. Only start it after the
          # fault-recovery gate has established that no abandoned child remains.
          pre_claim_profile=run_post_receipt_profile(store,queue_root,profile_runner,Path(a.gpu_lease_path),qual,qual['gpu_identity'])
          if pre_claim_profile and pre_claim_profile.get('recovery_status')=='PENDING_FRESH_PROCESS':
            handoff_requested=True; exit_state='HANDOFF_REQUIRED'
            exit_detail={'queue_claimed':False,'profile_request_id':pre_claim_profile.get('request_id'),
              'shutdown':'FRESH_PROCESS_RECOVERY_HANDOFF'}
            break
          recovery=recover_post_profile_timing_gate(Path(a.gpu_lease_path),qual,qual['gpu_identity'])
          gate=json.loads(gate_path.read_text(encoding='utf-8')) if gate_path.is_file() else {'status':'READY'}
          if worker_gate_action(gate.get('status'))=='HOLD_AND_EXIT':
            exit_state='HOLD'; exit_detail={'post_profile_recovery':recovery,'timing_gate':gate,
              'queue_claimed':False,'pre_claim_profile_request':pre_claim_profile.get('request_id') if pre_claim_profile else None,
              'shutdown':'BOUNDED_RECOVERY_ATTEMPT_EXHAUSTED'}
            status_write(status_path,state='HOLD',pid=os.getpid(),started=started,queue_root=queue_root,manifest_sha=MANIFEST_SHA,
              detail={'environment_identity':environment_identity,'process_identity':process_identity,
                'post_profile_recovery':recovery,'timing_gate':gate,'queue_claimed':False,
                'pre_claim_profile_request':pre_claim_profile.get('request_id') if pre_claim_profile else None})
            break
          result=run_one_oj_request(store,worker_id='N0-OJ-R5-EXCLUSIVE-1',lease_path=Path(a.gpu_lease_path),
            worker_pid=os.getpid(),worker_start_utc=started,health_check=health_check,evaluator=evaluator,receipt_validator=validate_r5_receipt)
          post_profile=run_post_receipt_profile(store,queue_root,profile_runner,Path(a.gpu_lease_path),qual,qual['gpu_identity'])
          if post_profile and post_profile.get('recovery_status')=='PENDING_FRESH_PROCESS':
            handoff_requested=True; exit_state='HANDOFF_REQUIRED'
            exit_detail={'last_request_result':result,'profile_request_id':post_profile.get('request_id'),
              'shutdown':'FRESH_PROCESS_RECOVERY_HANDOFF'}
            status_write(status_path,state='HANDOFF_REQUIRED',pid=os.getpid(),started=started,queue_root=queue_root,manifest_sha=MANIFEST_SHA,
              detail={'environment_identity':environment_identity,'process_identity':process_identity,**exit_detail})
            break
          status_write(status_path,state='RUNNING',pid=os.getpid(),started=started,queue_root=queue_root,manifest_sha=MANIFEST_SHA,
            detail={'environment_identity':environment_identity,'process_identity':process_identity,'last_request_result':result,
              'post_receipt_profile_status':post_profile.get('status') if post_profile else None,
              'post_receipt_profile_recovery_status':post_profile.get('recovery_status') if post_profile else None,
              'pre_claim_recovered_profile_request':pre_claim_profile.get('request_id') if pre_claim_profile else None,
              'post_profile_recovery':recovery})
        except Exception as exc:
          exit_state='DEGRADED'; exit_detail={'error':type(exc).__name__+':'+str(exc),
            'shutdown':'WORKER_EXCEPTION'}
          status_write(status_path,state='DEGRADED',pid=os.getpid(),started=started,queue_root=queue_root,manifest_sha=MANIFEST_SHA,
            detail={'environment_identity':environment_identity,'process_identity':process_identity,'error':type(exc).__name__+':'+str(exc)})
          raise
        if a.once: break
        time.sleep(max(0.2,a.idle_seconds))
    finally:
      exit_detail.update({'environment_identity':environment_identity,'process_identity':process_identity})
      status_write(status_path,state=exit_state,pid=os.getpid(),started=started,queue_root=queue_root,manifest_sha=MANIFEST_SHA,
        detail=exit_detail)

    return serving_worker_exit_code(exit_state,handoff_requested)
    if exit_state=='HOLD': return 20

if __name__=='__main__': sys.exit(main() or 0)
