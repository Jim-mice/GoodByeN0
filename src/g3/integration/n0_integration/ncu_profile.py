from __future__ import annotations
import csv, hashlib, json, os, re, subprocess, sys, time
from io import StringIO
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

METRICS=("launch__registers_per_thread","launch__shared_mem_per_block",
         "sm__warps_active.avg.pct_of_peak_sustained_active","gpu__time_duration.sum","dram__bytes.sum")

def parse_ncu_csv(path:Path)->tuple[list[dict[str,str]],dict[str,str]]:
    """Parse NCU stdout with diagnostic preamble and its separate unit row."""
    lines=Path(path).read_text(encoding='utf-8-sig').splitlines()
    header_index=None; headers=None
    for i,line in enumerate(lines):
        try: row=next(csv.reader([line]))
        except Exception: continue
        if 'Kernel Name' in row and set(METRICS).issubset(row):
            header_index=i; headers=row; break
    if header_index is None or header_index+1>=len(lines):
        raise ValueError('NCU_CSV_HEADER_OR_UNIT_ROW_MISSING')
    units=next(csv.reader([lines[header_index+1]]))
    if len(units)!=len(headers): raise ValueError('NCU_CSV_UNIT_ROW_WIDTH_MISMATCH')
    records=[]
    for line in lines[header_index+2:]:
        if not line.strip(): continue
        try: values=next(csv.reader([line]))
        except Exception as exc: raise ValueError('NCU_CSV_DATA_ROW_PARSE_FAILED') from exc
        if len(values)!=len(headers): raise ValueError('NCU_CSV_DATA_ROW_WIDTH_MISMATCH')
        records.append(dict(zip(headers,values)))
    return records,{name:(units[i] or '').strip() for i,name in enumerate(headers)}

def sha(path:Path)->str: return hashlib.sha256(path.read_bytes()).hexdigest().upper()

def unit_contract_mismatch(units:dict[str,str], expected_units:dict[str,str])->dict[str,tuple[str,str]]:
    allowed_units={"dram__bytes.sum":{"byte","Kbyte","Mbyte","Gbyte","Tbyte"}}
    time_units={"ns","us","µs","μs","ms","s"}
    mismatch={m:(expected_units[m],units[m]) for m in METRICS
              if m not in allowed_units and m!="gpu__time_duration.sum" and units[m]!=expected_units[m]}
    if units.get("gpu__time_duration.sum") not in time_units:
        mismatch["gpu__time_duration.sum"]=(expected_units["gpu__time_duration.sum"],units.get("gpu__time_duration.sum",""))
    mismatch.update({m:("byte-prefix",units[m]) for m in allowed_units if units[m] not in allowed_units[m]})
    return mismatch

def normalize_profile_value(metric:str,value:str,reported_unit:str,expected_unit:str):
    """Normalize known duration units while retaining the reported unit as provenance."""
    if metric!="gpu__time_duration.sum":
        return value
    if reported_unit==expected_unit:
        return float(value)
    to_ms={"ns":1e-6,"us":1e-3,"µs":1e-3,"μs":1e-3,"ms":1.0,"s":1e3}
    if reported_unit not in to_ms or expected_unit not in to_ms:
        raise ValueError("NCU_TIME_UNIT_UNSUPPORTED")
    return float(value)*to_ms[reported_unit]/to_ms[expected_unit]

class NcuProfileRunner:
    def __init__(self, *, root:Path, python:Path, ncu:Path, target_script:Path, whitelist:dict[str,Any], gpu_lease_path:Path|None=None):
        self.root=Path(root); self.python=Path(python); self.ncu=Path(ncu); self.target=Path(target_script)
        self.gpu_lease_path=Path(gpu_lease_path) if gpu_lease_path is not None else None
        self.whitelist=whitelist
        if whitelist.get('status')!='FROZEN' or set(whitelist.get('allowed_metrics',[]))!=set(METRICS):
            raise ValueError('PROFILE_WHITELIST_NOT_FROZEN_OR_METRICS_MISMATCH')
        for f in (self.python,self.ncu,self.target):
            if not f.is_file(): raise FileNotFoundError(str(f))

    def _kernel_pattern(self, build:dict[str,Any])->str:
        names=[str(x) for x in build.get('launch_kernel_names',build.get('kernel_names',[]))
               if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*",str(x))]
        if not names: raise ValueError('NCU_CANDIDATE_LAUNCH_NAMES_MISSING')
        return 'regex:.*('+'|'.join(re.escape(x) for x in names)+').*'

    def __call__(self, request:dict[str,Any], source:str, build:dict[str,Any], monitor)->dict[str,Any]:
        rid=request['request_id']; episode=request['episode_id']; cid=request['candidate_id']
        candidate_sha=hashlib.sha256(source.encode('utf-8')).hexdigest().upper()
        extension=Path(build.get('artifact_path','')).resolve()
        if not extension.is_file() or sha(extension)!=str(build.get('artifact_sha256','')).upper():
            return {"status":"HOLD","reason":"PROFILE_EXTENSION_IDENTITY_MISMATCH","request_id":rid,"candidate_sha256":candidate_sha}
        out=self.root/'profiles'/episode/rid; out.mkdir(parents=True,exist_ok=True)
        report=out/'candidate.ncu-rep'; stdout=out/'ncu.csv'; run_stdout=out/'ncu_run.stdout.txt'; stderr=out/'ncu.stderr.txt'; pidfile=out/'target.pid'
        kernel_filter=self._kernel_pattern(build)
        declared_names=list(dict.fromkeys(str(x) for x in build.get('kernel_names',[])))
        launch_names=list(dict.fromkeys(str(x) for x in build.get('launch_kernel_names',declared_names)))
        cmd=[str(self.ncu),'--target-processes','all','--kernel-name-base','demangled',
             '--kernel-name',kernel_filter,'--metrics',','.join(METRICS),
             '--launch-count',str(max(3, len(launch_names) * 2)),'--export',str(report),'--force-overwrite',
             str(self.python),str(self.target),'--extension',str(extension),'--pid-file',str(pidfile)]
        audit={'ncu_argv':cmd,'kernel_filter':kernel_filter,'expected_kernel_names':declared_names,
               'ncu_profile_argv':cmd,'ncu_import_argv':None,
               'expected_launch_kernel_names':launch_names,'actual_kernel_names':[],
               'coverage_basis':'NCU_OBSERVED_KERNELS_MATCHED_TO_CANDIDATE_LAUNCH_SITES'}
        env=os.environ.copy(); env['TEMP']=str(self.root/'tmp'); env['TMP']=env['TEMP']; Path(env['TEMP']).mkdir(parents=True,exist_ok=True)
        started=datetime.now(timezone.utc).isoformat(); proc=None
        logged_pids=set()
        try:
            # NCU can emit more than a Windows anonymous pipe buffer while it
            # serializes replay metadata.  Waiting on poll() without draining
            # PIPE deadlocks NCU and leaves a valid report beside a stuck
            # process.  File-backed streams preserve full diagnostics without
            # introducing a reader thread or truncation.
            with run_stdout.open('w',encoding='utf-8',newline='') as out_file, stderr.open('w',encoding='utf-8',newline='') as err_file:
                proc=subprocess.Popen(cmd,stdout=out_file,stderr=err_file,text=True,env=env,cwd=str(self.root))
                monitor.register_owned_pid(proc.pid)
                from .process_audit import append_process_audit
                process_log=self.root/'process_identity.jsonl'
                append_process_audit(process_log,pid=proc.pid,role='ncu_profiler',task_identity={
                    'episode_id':episode,'candidate_id':cid,'candidate_sha256':candidate_sha,'oj_request_id':rid,
                    'gpu_lease_path':str(self.gpu_lease_path) if self.gpu_lease_path else None})
                logged_pids.add(proc.pid)
                deadline=time.monotonic()+600
                while proc.poll() is None:
                    if pidfile.is_file():
                        try:
                            target_pid=int(pidfile.read_text(encoding='ascii').strip())
                            monitor.register_owned_pid(target_pid)
                            if target_pid not in logged_pids:
                                append_process_audit(process_log,pid=target_pid,role='ncu_cuda_target',task_identity={
                                    'episode_id':episode,'candidate_id':cid,'candidate_sha256':candidate_sha,'oj_request_id':rid,
                                    'gpu_lease_path':str(self.gpu_lease_path) if self.gpu_lease_path else None})
                                logged_pids.add(target_pid)
                        except Exception: pass
                    try:
                        import psutil
                        for child in psutil.Process(proc.pid).children(recursive=True):
                            monitor.register_owned_pid(child.pid)
                            if child.pid not in logged_pids:
                                try:
                                    append_process_audit(process_log,pid=child.pid,role='ncu_child',task_identity={
                                        'episode_id':episode,'candidate_id':cid,'candidate_sha256':candidate_sha,'oj_request_id':rid,
                                        'gpu_lease_path':str(self.gpu_lease_path) if self.gpu_lease_path else None})
                                    logged_pids.add(child.pid)
                                except Exception: pass
                    except Exception: pass
                    if time.monotonic()>deadline:
                        proc.terminate()
                        try: proc.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            proc.kill(); proc.wait(timeout=10)
                        raise TimeoutError('NCU_PROFILE_TIMEOUT')
                    time.sleep(0.2)
                proc.wait(timeout=10)
            if proc.returncode!=0 or not report.is_file():
                reason='NO_KERNELS_MATCHED_FILTER' if proc.returncode==0 else 'NCU_PROFILE_FAILED'
                return {"status":"UNAVAILABLE","reason":reason,"returncode":proc.returncode,
                        "stdout_path":str(run_stdout),"stderr_path":str(stderr),"report_path":str(report) if report.is_file() else None,
                        "stdout_sha256":sha(run_stdout),"stderr_sha256":sha(stderr),"request_id":rid,"candidate_sha256":candidate_sha,
                        **audit}
            import_cmd=[str(self.ncu),'--import',str(report),'--csv','--page','raw','--metrics',','.join(METRICS)]
            audit['ncu_import_argv']=import_cmd
            with stdout.open('w',encoding='utf-8',newline='') as out_file, stderr.open('a',encoding='utf-8',newline='') as err_file:
                imported=subprocess.run(import_cmd,stdout=out_file,stderr=err_file,text=True,cwd=str(self.root),timeout=60)
            if imported.returncode!=0:
                return {"status":"HOLD","reason":"NCU_REPORT_IMPORT_FAILED","returncode":imported.returncode,
                        "request_id":rid,"candidate_sha256":candidate_sha,"report_path":str(report),
                        "report_sha256":sha(report),"stderr_sha256":sha(stderr)}
            try: rows,unit_map=parse_ncu_csv(stdout)
            except Exception as exc:
                return {"status":"UNAVAILABLE","reason":"NCU_PROFILE_CSV_SCHEMA_MISMATCH:"+str(exc),"request_id":rid,
                        "candidate_sha256":candidate_sha,"stdout_sha256":sha(stdout),"stderr_sha256":sha(stderr)}
            required={'Kernel Name',*METRICS}
            if not rows or not required.issubset(rows[0]):
                if not rows:
                    return {"status":"UNAVAILABLE","reason":"NO_KERNELS_MATCHED_FILTER","returncode":proc.returncode,
                            "request_id":rid,"candidate_sha256":candidate_sha,"report_path":str(report),
                            "report_sha256":sha(report),"stdout_path":str(run_stdout),"stderr_path":str(stderr),
                            "stdout_sha256":sha(run_stdout),"stderr_sha256":sha(stderr),**audit}
                return {"status":"HOLD","reason":"NCU_PROFILE_CSV_SCHEMA_MISMATCH","columns":list(rows[0]) if rows else [],
                        "stdout_sha256":sha(stdout),"request_id":rid,"candidate_sha256":candidate_sha,**audit}
            reported_units={m:unit_map.get(m,'') for m in METRICS}
            if any(not u or u.lower() in {'n/a','not available'} for u in reported_units.values()):
                return {"status":"UNAVAILABLE","reason":"NCU_UNIT_METADATA_MISSING","units":reported_units,
                        "stdout_sha256":sha(stdout),"request_id":rid,"candidate_sha256":candidate_sha}
            expected_units={m:self.whitelist['metrics_per_kernel'][m]['unit'] for m in METRICS}
            mismatch=unit_contract_mismatch(reported_units,expected_units)
            if mismatch:
                return {"status":"UNAVAILABLE","reason":"NCU_UNIT_CONTRACT_MISMATCH","unit_mismatch":mismatch,
                        "expected_units":expected_units,"observed_units":reported_units,"stdout_path":str(stdout),"stderr_path":str(stderr),
                        "stdout_sha256":sha(stdout),"stderr_sha256":sha(stderr),"request_id":rid,"candidate_sha256":candidate_sha}
            units=expected_units
            selected=[]
            for row in rows:
                name=(row.get('Kernel Name') or '').strip()
                if not name: continue
                selected.append({"kernel_name":name,"metrics":{m:{
                    "value":normalize_profile_value(m,row.get(m,""),reported_units[m],units[m]),
                    "unit":units[m],"reported_unit":reported_units[m]} for m in METRICS}})
            def matched_kernel(ncu_name:str)->str|None:
                matches=[k for k in launch_names if re.search(r'(?<![A-Za-z0-9_])'+re.escape(k)+r'(?![A-Za-z0-9_])',ncu_name)]
                return matches[0] if len(matches)==1 else None
            observed=[matched_kernel(r['kernel_name']) for r in selected]
            audit['actual_kernel_names']=list(dict.fromkeys(x for x in observed if x))
            audit['kernel_coverage']={'basis':'observed_candidate_kernel_names',
                'possible_launch_names':launch_names,'observed_kernel_names':audit['actual_kernel_names'],
                'complete':bool(audit['actual_kernel_names']) and all(x is not None for x in observed)}
            if not selected:
                return {"status":"UNAVAILABLE","reason":"NO_KERNELS_MATCHED_FILTER","request_id":rid,
                        "candidate_sha256":candidate_sha,"report_path":str(report),"report_sha256":sha(report),**audit}
            if any(x is None for x in observed):
                return {"status":"UNAVAILABLE","reason":"NCU_KERNEL_COVERAGE_UNEXPECTED_KERNEL","expected":launch_names,
                        "observed":[x['kernel_name'] for x in selected],"request_id":rid,"candidate_sha256":candidate_sha,**audit}
            metrics={m:[{"kernel_name":r["kernel_name"],"value":r["metrics"][m]["value"],"unit":units[m]} for r in selected] for m in METRICS}
            return {"status":"PASS","request_id":rid,"episode_id":episode,"candidate_id":cid,
                    "candidate_sha256":candidate_sha,"shape":[16,64,256,256],"measurement_protocol":"N0_NCU_POST_TIMING_V1",
                    "profiler":{"name":"Nsight Compute","version":"2026.3.0.0","binary_sha256":sha(self.ncu)},
                    "metrics":metrics,"metrics_per_kernel":selected,"metric_names":list(METRICS),
                    "unit_contract":units,"reported_units":reported_units,
                    "source_sha256":candidate_sha,"build_identity_sha256":build.get('build_identity_sha256'),
                    "extension_sha256":sha(extension),"report_path":str(report),"report_sha256":sha(report),
                    "csv_path":str(stdout),"csv_sha256":sha(stdout),"ncu_run_stdout_path":str(run_stdout),
                    "ncu_run_stdout_sha256":sha(run_stdout),"stderr_sha256":sha(stderr),
                    "command_sha256":hashlib.sha256(json.dumps(cmd,separators=(',',':')).encode()).hexdigest().upper(),
                    "started_at_utc":started,"finished_at_utc":datetime.now(timezone.utc).isoformat(),**audit,
                    "score_inclusion":False}
        except Exception as exc:
            if proc is not None and proc.poll() is None:
                proc.terminate()
                try: proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill(); proc.wait(timeout=10)
            out_text=''; err_text=''
            if proc is not None and (proc.stdout is not None or proc.stderr is not None):
                try: out_text,err_text=proc.communicate(timeout=10)
                except Exception as collect_exc:
                    err_text=f"{err_text}\nPIPE_COLLECTION_ERROR:{type(collect_exc).__name__}:{collect_exc}"
                stdout.write_text(out_text,encoding='utf-8'); stderr.write_text(err_text,encoding='utf-8')
            return {"status":"HOLD","reason":type(exc).__name__+':'+str(exc),
                    "request_id":rid,"episode_id":episode,"candidate_id":cid,
                    "candidate_sha256":candidate_sha,"build_identity_sha256":build.get('build_identity_sha256'),
                    "report_path":str(report) if report.is_file() else None,
                    "report_sha256":sha(report) if report.is_file() else None,
                    "stdout_path":str(stdout),"stdout_sha256":sha(stdout),"ncu_run_stdout_path":str(run_stdout),
                    "ncu_run_stdout_sha256":sha(run_stdout),
                    "stderr_path":str(stderr),"stderr_sha256":sha(stderr),
                    "target_pid":pidfile.read_text(encoding='ascii').strip() if pidfile.is_file() else None,
                    "process_returncode":proc.returncode if proc is not None else None,**audit}
