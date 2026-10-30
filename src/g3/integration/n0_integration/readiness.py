from __future__ import annotations
import hashlib, json, sqlite3, subprocess, os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REQUIRED_IDS=(
 "harness_release_integrity","oj_release_integrity","provider_release_integrity",
 "provider_formal_queue_live","oj_formal_queue_live","cuda_extension_live",
 "c0000_trusted_receipt","oj_timing_qualification","profile_query_roundtrip",
 "gpu_lease_and_external_interference","four_worker_full_loop","formal_protocol_frozen",
 "formal_allocations","provider_recovery_semantics","old_transactions_reconciled")

def sha256(path:Path)->str:
 h=hashlib.sha256()
 with path.open('rb') as f:
  for chunk in iter(lambda:f.read(1024*1024),b''): h.update(chunk)
 return h.hexdigest().upper()

def verify_manifest(path:Path,release_id:str)->dict[str,Any]:
 try: d=json.loads(path.read_text(encoding='utf-8'))
 except Exception as e: return {"status":"BLOCKED","reason":"MANIFEST_UNREADABLE:"+type(e).__name__}
 if d.get('release_id')!=release_id: return {"status":"BLOCKED","reason":"RELEASE_ID_MISMATCH"}
 sources=d.get('source_sha256')
 if isinstance(sources,dict): entries=[{"path":k,"sha256":v} for k,v in sources.items()]
 else: entries=d.get('source_files')
 if not isinstance(entries,list) or not entries: return {"status":"BLOCKED","reason":"MANIFEST_SOURCE_LIST_MISSING"}
 bad=[]
 for row in entries:
  if not isinstance(row,dict) or not isinstance(row.get('path'),str) or not isinstance(row.get('sha256'),str):
   bad.append("<invalid-entry>"); continue
  rel,want=row['path'],row['sha256']
  f=(path.parent/rel).resolve()
  try: f.relative_to(path.parent.resolve())
  except ValueError: bad.append(rel); continue
  if not f.is_file() or sha256(f).lower()!=str(want).lower(): bad.append(rel)
 return {"status":"PASS" if not bad else "BLOCKED","manifest_sha256":sha256(path),"checked":len(entries),"mismatches":bad}

def provider_smoke_evidence(path:Path)->dict[str,Any]:
 try: d=json.loads(path.read_text(encoding='utf-8'))
 except Exception: return {"status":"BLOCKED","reason":"SMOKE_RESULTS_UNREADABLE"}
 rows=d.get('results',[])
 if d.get('classification')!='NON_SCIENTIFIC_REAL_PROVIDER_SMOKE' or len(rows)!=8:
  return {"status":"BLOCKED","reason":"SMOKE_SCOPE_OR_COUNT_MISMATCH"}
 good=all(r.get('state')=='COMPLETED' and r.get('http_status')==200 and r.get('response_id')
          and r.get('stored_result_file_sha256') and r.get('provider_response_payload_sha256')
          and r.get('provider_request_id_status') in {'PROVIDED','NOT_PROVIDED_BY_PROVIDER','NOT_RETURNED_BY_PROVIDER'} for r in rows)
 first=rows[:4]; follow=rows[4:]
 tool_roundtrip=(all(len(r.get('tool_calls',[]))==1 for r in first)
                 and all(r.get('finish_reason')=='stop' for r in follow))
 return {"status":"PASS_NON_SCIENTIFIC_ONLY" if good and tool_roundtrip else "BLOCKED",
         "rows":len(rows),"http_200":sum(r.get('http_status')==200 for r in rows),
         "native_request_id_missing":sum(r.get('provider_request_id_status') in {'NOT_PROVIDED_BY_PROVIDER','NOT_RETURNED_BY_PROVIDER'} for r in rows),
         "tool_roundtrip":tool_roundtrip,"formal":False,"source_sha256":sha256(path)}

def process_identity(pid:int, expected_fragment:str|None=None)->dict[str,Any]:
 """Use live CIM command line and creation date; status JSON alone is never authority."""
 if not isinstance(pid,int) or pid<=0: return {"status":"STOPPED","reason":"PID_INVALID"}
 try:
  p=subprocess.run(["powershell.exe","-NoProfile","-Command",
   f"$p=Get-CimInstance Win32_Process -Filter 'ProcessId={pid}'; if ($null -eq $p) {{ '{{}}' }} else {{ $p | Select-Object ProcessId,ParentProcessId,CreationDate,ExecutablePath,CommandLine | ConvertTo-Json -Compress }}"],
   capture_output=True,text=True,timeout=12)
  if p.returncode: return {"status":"UNKNOWN","reason":"CIM_QUERY_FAILED","stderr":p.stderr[-1000:]}
  raw=json.loads(p.stdout.strip() or "{}")
  if not raw: return {"status":"STOPPED","pid":pid,"reason":"PROCESS_NOT_FOUND"}
  if expected_fragment and expected_fragment.casefold() not in str(raw.get("CommandLine","")).casefold():
   return {"status":"IDENTITY_MISMATCH","pid":pid,"process":raw}
  return {"status":"RUNNING","pid":pid,"process":raw}
 except Exception as exc: return {"status":"UNKNOWN","reason":type(exc).__name__+":"+str(exc)}

def service_evidence(path:Path, *, queue_root:Path, expected_release:str,
                     expected_manifest_sha256:str, command_fragment:str)->dict[str,Any]:
 try: d=json.loads(path.read_text(encoding='utf-8'))
 except Exception as exc: return {"status":"BLOCKED","reason":"SERVICE_STATUS_UNREADABLE:"+type(exc).__name__}
 if Path(d.get('queue_root','')).resolve()!=Path(queue_root).resolve(): return {"status":"BLOCKED","reason":"QUEUE_ROOT_MISMATCH"}
 if d.get('release_id')!=expected_release or str(d.get('manifest_sha256','')).lower()!=expected_manifest_sha256.lower():
  return {"status":"BLOCKED","reason":"SERVICE_RELEASE_IDENTITY_MISMATCH"}
 generation=d.get('launch_generation')
 if not generation: return {"status":"BLOCKED","reason":"LAUNCH_GENERATION_MISSING"}
 try: age=(datetime.now(timezone.utc)-datetime.fromisoformat(d.get('heartbeat_utc','').replace('Z','+00:00'))).total_seconds()
 except Exception: return {"status":"BLOCKED","reason":"SERVICE_HEARTBEAT_INVALID"}
 if age<0 or age>15: return {"status":"BLOCKED","reason":"HEARTBEAT_STALE","heartbeat_age_seconds":age}
 proc=process_identity(d.get('pid'),command_fragment)
 return {"status":"PASS" if proc.get('status')=='RUNNING' and proc.get('process',{}).get('CreationDate')==d.get('process_creation_date') else "BLOCKED",
         "process":proc,"heartbeat_age_seconds":age,"launch_generation":generation,"queue_root":str(queue_root)}

def evidence_file_gate(path:Path, expected_schema:str|None=None)->dict[str,Any]:
 try: raw=path.read_bytes(); data=json.loads(raw.decode('utf-8'))
 except Exception as exc: return {"status":"BLOCKED","reason":"EVIDENCE_FILE_MISSING_OR_INVALID","error":type(exc).__name__}
 if expected_schema and data.get('schema')!=expected_schema: return {"status":"BLOCKED","reason":"SCHEMA_MISMATCH","actual":data.get('schema')}
 accepted={'PASS','VALID_MEASUREMENT','QUALIFIED'}
 if expected_schema=='N0_OJ_R5_IMMUTABLE_RECEIPT_V1':
  accepted={'VALID_MEASUREMENT'}
  if not (data.get('score') is not None and data.get('candidate_sha256') and data.get('request_id')
          and isinstance(data.get('measurement_evidence'),dict)
          and data.get('profile',{}).get('status')=='PASS'):
   return {"status":"BLOCKED","reason":"C0000_RECEIPT_INCOMPLETE"}
 actual=data.get('status',data.get('verdict'))
 if actual not in accepted: return {"status":"BLOCKED","reason":"EVIDENCE_STATUS_NOT_PASS","actual":actual}
 return {"status":"PASS","path":str(path.resolve()),"sha256":hashlib.sha256(raw).hexdigest().upper(),"schema":data.get('schema')}

def compute_readiness(*, evidence_root:Path, formal_store:Path|None=None)->dict[str,Any]:
 """Calculate state from current machine evidence; never consumes a prior report."""
 root=Path(evidence_root).resolve(); checks=[]
 def put(cid,status,evidence,detail):
  if cid not in REQUIRED_IDS: raise ValueError('UNKNOWN_CHECK_ID:'+cid)
  if status not in {'PASS','BLOCKED','UNKNOWN'}: raise ValueError('READINESS_STATUS_INVALID:'+str(status))
  checks.append({"id":cid,"status":status,"evidence":evidence,"detail":detail,"checked_at_utc":datetime.now(timezone.utc).isoformat()})
 rels=[("harness_release_integrity",root/'harness/releases/N0_AUTONOMOUS_INFORMATION_ACQUISITION_R6/RELEASE_MANIFEST.json','N0_AUTONOMOUS_INFORMATION_ACQUISITION_R6'),
       ("oj_release_integrity",root/'oj/releases/N0_KERNELBENCH_L1_40_LAYERNORM_R5_REFERENCE_TRUST/RELEASE_MANIFEST.json','N0_KERNELBENCH_L1_40_LAYERNORM_R5_REFERENCE_TRUST'),
       ("provider_release_integrity",root/'provider/releases/N0_PARATERA_PROVIDER_R20/R20_PROVIDER_RELEASE_MANIFEST.json','N0_PARATERA_PROVIDER_R20')]
 for cid,path,rid in rels:
  r=verify_manifest(path,rid); put(cid,r['status'],str(path),json.dumps(r,ensure_ascii=False))
 smoke=root/'reports/n0_timing_qualification/N0_PROVIDER_SMOKE_RESULTS.json'
 s=provider_smoke_evidence(smoke)
 formal_provider=root/'state/n0_provider_formal/service_status.json'
 try:
  from .provider_service import snapshot_r20_service
  snapshot=snapshot_r20_service(queue_root=root/'state/n0_provider_formal',release_root=rels[2][1].parent,
      manifest_sha256=sha256(rels[2][1].parent/'R20_PROVIDER_RELEASE_MANIFEST.json'),output_path=formal_provider)
 except Exception as exc: snapshot={"status":"BLOCKED","reason":type(exc).__name__+":"+str(exc)}
 provider_state=service_evidence(formal_provider,queue_root=root/'state/n0_provider_formal',expected_release='N0_PARATERA_PROVIDER_R20',
    expected_manifest_sha256=rels[2][1].parent.joinpath('R20_PROVIDER_RELEASE_MANIFEST.json').exists() and sha256(rels[2][1].parent/'R20_PROVIDER_RELEASE_MANIFEST.json') or '',
    command_fragment='provider_queue_worker.py')
 if snapshot.get('status')!='PASS': provider_state={"status":"BLOCKED","reason":snapshot.get('reason','LIVE_SERVICE_SNAPSHOT_FAILED')}
 put('provider_formal_queue_live',provider_state['status'],str(formal_provider),json.dumps({"historical_smoke":s,"formal_service":provider_state},ensure_ascii=False))
 gate_specs={
  'oj_formal_queue_live':('state/n0_oj_formal/service_status.json',None),
  'cuda_extension_live':('qualification/cuda_extension_live.json',None),
  'c0000_trusted_receipt':('episodes/N0_FORMAL_REP01/C0000/receipt.json','N0_OJ_R5_IMMUTABLE_RECEIPT_V1'),
  'oj_timing_qualification':('qualification/oj_timing_qualification.json',None),
  'profile_query_roundtrip':('qualification/profile_query_roundtrip.json',None),
  'gpu_lease_and_external_interference':('qualification/gpu_exclusivity.json',None),
  'four_worker_full_loop':('qualification/four_worker_e2e.json',None),
  'formal_protocol_frozen':('protocol/N0_FORMAL_PROTOCOL_V1.json',None),
  'formal_allocations':('allocations/N0_FORMAL_ALLOCATIONS_V1.json',None),
  'provider_recovery_semantics':('qualification/provider_recovery.json',None),
  'old_transactions_reconciled':('qualification/old_transactions_reconciled.json',None),
 }
 for cid,(rel,schema) in gate_specs.items():
  f=root/rel
  if cid=='oj_formal_queue_live':
   r=service_evidence(f,queue_root=root/'state/n0_oj_formal',expected_release='N0_KERNELBENCH_L1_40_LAYERNORM_R5_REFERENCE_TRUST',
      expected_manifest_sha256=sha256(rels[1][1]),command_fragment='run_r5_oj_worker.py') if f.is_file() else {"status":"BLOCKED","reason":"SERVICE_STATUS_MISSING"}
  else: r=evidence_file_gate(f,schema)
  put(cid,r['status'],str(f),json.dumps(r,ensure_ascii=False))
 ids=[c['id'] for c in checks]
 if len(ids)!=len(set(ids)): raise ValueError('DUPLICATE_READINESS_CHECK_ID')
 missing=sorted(set(REQUIRED_IDS)-set(ids))
 if missing: raise ValueError('READINESS_CHECKS_MISSING:'+','.join(missing))
 formal=[]
 if formal_store and Path(formal_store).is_file():
  try:
   db=sqlite3.connect(formal_store); rows=db.execute('SELECT episode_id,state FROM episodes').fetchall(); db.close()
   formal=[{"episode_id":x[0],"state":x[1]} for x in rows if str(x[0]).startswith('N0_FORMAL_REP')]
  except Exception: formal=[]
 abort=None
 if formal_store and Path(formal_store).is_file():
  try:
   db=sqlite3.connect(formal_store); row=db.execute("SELECT value FROM runtime_meta WHERE key='science_batch_abort'").fetchone(); db.close(); abort=json.loads(row[0]) if row else None
  except Exception: abort=None
 statuses={c['status'] for c in checks}
 if abort: state='INFRA_ABORTED'
 elif formal and all(x['state']=='SCIENTIFICALLY_COMPLETED' for x in formal) and len(formal)==4: state='FORMAL_COMPLETED'
 elif formal and any(x['state'] in {'ALLOCATED','RUNNING','WAITING_PROVIDER','WAITING_OJ'} for x in formal): state='FORMAL_RUNNING'
 elif all(c['status']=='PASS' for c in checks): state='READY'
 else: state='BLOCKED'
 return {"schema":"N0_FORMAL_READINESS_REPORT_V2","generated_at_utc":datetime.now(timezone.utc).isoformat(),
         "overall_status":state,"required_check_ids":list(REQUIRED_IDS),"checks":checks,
         "formal_episodes":formal,"batch_abort":abort,"evidence_root":str(root)}
