from __future__ import annotations
import hashlib,json,os,sys,time
from pathlib import Path

R=Path(r"D:\projects\kernelagent\runtime")
H=R/"harness/releases/N0_AUTONOMOUS_INFORMATION_ACQUISITION_R7_3_POOL20_R5_FILELIST_FIX"
I=R/"integrations/N0_PRODUCTION_INTEGRATION_R12_10_4_R9_6_R8_03_NATURAL_EXIT_HANDOFF_20261027"
OJ=R/"oj/releases/N0_KERNELBENCH_L1_40_LAYERNORM_R6_5_FAULT_CONTAINED"
sys.path[:0]=[str(H),str(I),str(OJ),str(H/"third_party/KernelBench_repo/src")]
from n0_autonomous.parallel_runtime import N0PersistentQueue
import formal_runtime

QUEUE_ENV=os.environ.get("N0_OJ_QUEUE_ROOT")
if not QUEUE_ENV: raise RuntimeError("POOL20_RUN_SCOPED_OJ_QUEUE_REQUIRED")
QUEUE=Path(QUEUE_ENV).resolve()
LEASE=Path(os.environ.get("N0_GPU_LEASE_PATH",str(R/"state/n0_gpu_device.lock"))).resolve()
PROTOCOL_SHA=hashlib.sha256((I/"ONLINE_FEEDBACK_PROTOCOL_V1.json").read_bytes()).hexdigest().upper()

def sha(b:bytes)->str:return hashlib.sha256(b).hexdigest().upper()
def json_write_once(path:Path,obj):
    path.parent.mkdir(parents=True,exist_ok=True);raw=(json.dumps(obj,ensure_ascii=False,indent=2,sort_keys=True)+"\n").encode()
    if path.exists():
        if path.read_bytes()!=raw:raise RuntimeError("POOL20_SHARED_OJ_IMMUTABLE_CONFLICT:"+str(path))
        return
    with path.open("xb") as f:f.write(raw);f.flush();os.fsync(f.fileno())

def queue_store():return N0PersistentQueue(QUEUE,max_active_episodes=20,worker_count=20,oj_gpu_active_jobs_max=1)

def register_episode(episode_id:str,workspace:Path):
    store=queue_store();snap=store.snapshot()
    if any(x["episode_id"]==episode_id for x in snap["episodes"]):
        paths=store.episode_paths(episode_id)
        for name in ("task_definition.json","candidate_abi.json","candidate.cu"):
            if (paths.workspace/name).read_bytes()!=(workspace/name).read_bytes():raise RuntimeError("POOL20_QUEUE_EPISODE_WORKSPACE_CONFLICT:"+name)
        return {"status":"ALREADY_REGISTERED","episode_id":episode_id}
    files={name:(workspace/name).read_bytes() for name in ("task_definition.json","candidate_abi.json","candidate.cu")}
    files["visible_observation.json"]=(json.dumps({"schema":"N0_VISIBLE_EVALUATION_OBSERVATION_V1","episode_id":episode_id,
        "candidate_id":"C0000","candidate_sha256":sha(files["candidate.cu"]),"verdict":"SEED_PENDING",
        "score":None,"classification":"CONTROLLER_BOOTSTRAP"},indent=2)+"\n").encode()
    store.register_episode(episode_id=episode_id,replicate_slot=episode_id,treatment_id="N0_AUTONOMOUS",
        approved_allocation={"authorized":True,"episode_id":episode_id,"replicate_slot":episode_id},
        max_candidates=30,workspace_files=files)
    return {"status":"REGISTERED","episode_id":episode_id}

def _enqueue_seed(store,episode_id:str)->str:
    from n0_autonomous.engine import canonical_bytes
    seed=H/"protocol/SEED_CANDIDATE.cu";raw=seed.read_bytes();digest=sha(raw).lower()
    if digest.upper()!=formal_runtime.SEED_SHA:raise RuntimeError("POOL20_SEED_SHA_MISMATCH")
    paths=store.episode_paths(episode_id);dest=paths.archive/"candidates"/("C0000-"+digest+".cu")
    dest.parent.mkdir(parents=True,exist_ok=True)
    if dest.exists() and dest.read_bytes()!=raw:raise RuntimeError("POOL20_SEED_ARCHIVE_CONFLICT")
    if not dest.exists():dest.write_bytes(raw)
    request_id="N0-OJ-POOL20-SEED-"+hashlib.sha256((episode_id+digest).encode()).hexdigest()[:24]
    db=store._connect()
    try:
        db.execute("BEGIN IMMEDIATE")
        old=db.execute("SELECT request_id FROM oj_requests WHERE episode_id=? AND candidate_id='C0000'",(episode_id,)).fetchone()
        if old:
            if old[0]!=request_id:raise RuntimeError("POOL20_SEED_REQUEST_ID_CONFLICT")
            db.execute("COMMIT");return request_id
        ep=db.execute("SELECT state,candidates_used,max_candidates FROM episodes WHERE episode_id=?",(episode_id,)).fetchone()
        if not ep or ep["state"] not in {"ALLOCATED","RUNNING","WAITING_PROVIDER"} or ep["candidates_used"]!=0 or ep["max_candidates"]!=30:
            raise RuntimeError("POOL20_SEED_QUEUE_STATE_INVALID")
        seq=int(db.execute("SELECT value FROM runtime_meta WHERE key='queue_sequence'").fetchone()[0])+1;db.execute("UPDATE runtime_meta SET value=? WHERE key='queue_sequence'",(str(seq),))
        now=time.time();rel=dest.relative_to(QUEUE).as_posix()
        db.execute("INSERT INTO oj_requests(request_id,episode_id,invocation_id,candidate_id,candidate_sha256,state,queue_sequence,candidate_source_path,created_at,updated_at) VALUES(?,?,?,'C0000',?,'QUEUED',?,?,?,?)",
            (request_id,episode_id,"POOL20_C0000_BOOTSTRAP",digest,seq,rel,now,now))
        store._insert_event_tx(db,episode_id,{"event_type":"POOL20_SHARED_OJ_SEED_QUEUED","request_id":request_id,
            "candidate_id":"C0000","candidate_sha256":digest,"candidate_budget_delta":0},"pool20-seed:"+episode_id)
        db.execute("COMMIT")
    except BaseException:
        if db.in_transaction:db.execute("ROLLBACK")
        raise
    finally:db.close()
    store.reconcile_episode_exports(episode_id)
    return request_id

def _wait_row(store,request_id:str,timeout_s:int=3600):
    deadline=time.monotonic()+timeout_s
    while time.monotonic()<deadline:
        row=next((x for x in store.snapshot()["oj_requests"] if x["request_id"]==request_id),None)
        if not row:raise RuntimeError("POOL20_OJ_REQUEST_DISAPPEARED:"+request_id)
        if row["state"] in {"COMPLETED","CONSUMED","HOLD_UNKNOWN"}:return row
        if (QUEUE/"service_status.json").is_file():
            try:status=json.loads((QUEUE/"service_status.json").read_text(encoding="utf-8"))
            except Exception:status={"state":"UNKNOWN"}
            if status.get("state") in {"HOLD","DEGRADED","STOPPED"}:
                raise RuntimeError("POOL20_CONTROLLER_ONLY_OJ_SERVICE_"+str(status.get("state")))
        time.sleep(0.5)
    raise TimeoutError("POOL20_OJ_REQUEST_WAIT_TIMEOUT:"+request_id)

def _settle(store,episode_id,row,local_results:Path,cid:str|None=None,result_path_override:Path|None=None):
    if row["state"]=="HOLD_UNKNOWN":raise RuntimeError("POOL20_CONTROLLER_ONLY_OJ_HOLD_UNKNOWN:"+row["request_id"])
    receipt=json.loads(row["receipt_json"]) if row["state"]=="CONSUMED" else store.consume_oj_result(row["request_id"],row["receipt_sha256"])
    row=next(x for x in store.snapshot()["oj_requests"] if x["request_id"]==row["request_id"])
    records=formal_runtime.formal_candidates(store,episode_id)
    record=next(x for x in records if x["candidate_id"]==row["candidate_id"])
    obs=formal_runtime._write_observation(store,episode_id,row,receipt,
        record["ledger_entry"].get("promotion"),PROTOCOL_SHA,
        record["ledger_entry"].get("promotion_confidence_status"))
    ep=store.episode_paths(episode_id)
    (local_results.parent/"workspace/visible_observation.json").write_text(json.dumps(obs,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    timing=receipt.get("timing") or {}
    output={"status":receipt.get("verdict"),"candidate_source_sha256":receipt.get("candidate_sha256"),
        "task_sha256":receipt.get("task_sha256"),"gpu_identity":receipt.get("gpu_identity"),
        "score":receipt.get("score"),"candidate_runtime_ms":receipt.get("candidate_runtime_ms"),
        "reference_runtime_ms":receipt.get("reference_runtime_ms"),
        "timing":{"candidate_runtime_ms":receipt.get("candidate_runtime_ms"),
                  "kernelbench_reference_runtime_ms":receipt.get("reference_runtime_ms"),
                  "kernelbench_speedup":receipt.get("score"),**timing},
        "official_kernelbench_result":receipt.get("official_kernelbench_result"),
        "candidate_failure_diagnostics":receipt.get("candidate_failure_diagnostics"),
        "failure":receipt.get("failure"),"oj_receipt":receipt,"oj_receipt_sha256":row["receipt_sha256"],
        "candidate_record":record,"agent_observation":obs,"candidate_id":row["candidate_id"],
        "request_id":row["request_id"],"baseline_id":receipt.get("baseline_id"),
        "score_version":receipt.get("score_version")}
    result_path=result_path_override or (local_results/("c0000_result.json" if row["candidate_id"]=="C0000" else row["candidate_id"].lower()+"_result.json"))
    json_write_once(result_path,output)
    envelope_path=(result_path.with_name(result_path.stem+"_shared_oj_envelope.json") if result_path_override else local_results/("c0000_shared_oj_envelope.json" if row["candidate_id"]=="C0000" else row["candidate_id"].lower()+"_shared_oj_envelope.json"))
    json_write_once(envelope_path,
        {"request_id":row["request_id"],"receipt_sha256":row["receipt_sha256"],"receipt":receipt,
         "candidate_record":record,"agent_observation":obs})
    return result_path,output

def _verified_receipt(row:dict)->dict:
    receipt=json.loads(row.get("receipt_json") or "{}")
    actual=hashlib.sha256(json.dumps(receipt,sort_keys=True,separators=(",",":"),ensure_ascii=False).encode("utf-8")).hexdigest().lower()
    if actual!=str(row.get("receipt_sha256") or "").lower():raise RuntimeError("POOL20_AUTHORITY_RECEIPT_SHA_MISMATCH:"+str(row.get("request_id")))
    if any(receipt.get(k)!=row.get(k) for k in ("request_id","episode_id","candidate_id","candidate_sha256")):
        raise RuntimeError("POOL20_AUTHORITY_RECEIPT_IDENTITY_MISMATCH:"+str(row.get("request_id")))
    return receipt

def _consume_infra_receipt(store,row:dict,results:Path,cid:str,source_sha:str)->tuple[dict,dict]:
    receipt=_verified_receipt(row)
    if row.get("candidate_id")!=cid or str(row.get("candidate_sha256","")).lower()!=source_sha.lower():
        raise RuntimeError("POOL20_INFRA_HOLD_SOURCE_OR_CANDIDATE_MISMATCH:"+str(row.get("request_id")))
    if not str(receipt.get("verdict","")).startswith("OJ_INFRASTRUCTURE_HOLD_") or receipt.get("score") is not None:
        raise RuntimeError("POOL20_RETRY_SOURCE_NOT_INFRASTRUCTURE_HOLD:"+str(row.get("request_id")))
    if row.get("state")=="COMPLETED":
        store.consume_oj_result(row["request_id"],row["receipt_sha256"])
        row=next(x for x in store.snapshot()["oj_requests"] if x["request_id"]==row["request_id"])
        receipt=_verified_receipt(row)
    if row.get("state")!="CONSUMED":raise RuntimeError("POOL20_INFRA_HOLD_NOT_SETTLEABLE:"+str(row.get("request_id")))
    audit={"schema":"N0_POOL20_INFRASTRUCTURE_HOLD_AUDIT_V1","request_id":row["request_id"],"episode_id":row["episode_id"],"candidate_id":cid,"candidate_sha256":source_sha,"receipt_sha256":row["receipt_sha256"],"verdict":receipt.get("verdict"),"score":None,"candidate_budget_delta":0,"receipt":receipt}
    json_write_once(results/(cid.lower()+"_infra_hold_"+row["request_id"]+".json"),audit)
    return row,receipt

def _existing_retry(store,original:dict)->dict|None:
    db=store._connect()
    try:
        item=db.execute("SELECT value FROM runtime_meta WHERE key=?",("timing-retry:"+str(original["request_id"]),)).fetchone()
        if not item:return None
        retry=db.execute("SELECT * FROM oj_requests WHERE request_id=?",(item[0],)).fetchone()
        if not retry:raise RuntimeError("POOL20_INFRA_RETRY_LEDGER_TARGET_MISSING:"+str(original["request_id"]))
        retry=dict(retry)
    finally:db.close()
    if retry.get("episode_id")!=original.get("episode_id") or retry.get("candidate_id")!=original.get("candidate_id") or retry.get("candidate_sha256")!=original.get("candidate_sha256") or retry.get("invocation_id")!="INFRA-RETRY-"+str(original["request_id"]):
        raise RuntimeError("POOL20_INFRA_RETRY_IDENTITY_MISMATCH:"+str(original["request_id"]))
    return retry

def _retry_infrastructure_once(store,original:dict,receipt:dict,results:Path,cid:str,source_sha:str)->tuple[Path,dict]:
    retry=_existing_retry(store,original)
    if retry is None:
        queued=formal_runtime.retry_infrastructure_measurement_once(store,original,receipt)
        if queued is None:raise RuntimeError("POOL20_CONTROLLER_ONLY_INFRASTRUCTURE_HOLD:RETRY_NOT_AVAILABLE:"+str(original["request_id"]))
        retry=_existing_retry(store,original)
        if retry is None or retry.get("request_id")!=queued.get("request_id"):
            raise RuntimeError("POOL20_INFRA_RETRY_NOT_DURABLE:"+str(original["request_id"]))
    if retry.get("state") in {"QUEUED","RUNNING"}:
        waited=_wait_row(store,retry["request_id"])
        retry=next(x for x in store.snapshot()["oj_requests"] if x["request_id"]==waited["request_id"])
    if retry.get("state")=="HOLD_UNKNOWN":raise RuntimeError("POOL20_CONTROLLER_ONLY_OJ_HOLD_UNKNOWN:"+retry["request_id"])
    retry_receipt=_verified_receipt(retry) if retry.get("state") in {"COMPLETED","CONSUMED"} else None
    if retry_receipt is None:raise RuntimeError("POOL20_INFRA_RETRY_NOT_TERMINAL:"+retry["request_id"])
    if retry_receipt.get("verdict")!="VALID_MEASUREMENT" or retry_receipt.get("score") is None:
        _consume_infra_receipt(store,retry,results,cid,source_sha)
        raise RuntimeError("POOL20_CONTROLLER_ONLY_INFRASTRUCTURE_HOLD:RETRY_EXHAUSTED:"+str(retry_receipt.get("verdict")))
    result_path=results/(cid.lower()+"_retry_"+retry["request_id"]+".json")
    return _settle(store,original["episode_id"],retry,results,cid,result_path)

def evaluate_seed(episode_id:str,workspace:Path,results:Path):
    register_episode(episode_id,workspace);store=queue_store();rid=_enqueue_seed(store,episode_id);row=_wait_row(store,rid)
    path,result=_settle(store,episode_id,row,results)
    if result.get("status")!="VALID_MEASUREMENT" or result.get("score") is None:
        raise RuntimeError("POOL20_C0000_BASELINE_NOT_TRUSTED:"+str(result.get("status")))
    return path

def evaluate_candidate(episode_id:str,cid:str,source_path:Path,workspace:Path,results:Path):
    store=queue_store();source=source_path.read_text(encoding="utf-8")
    workspace_candidate=store.episode_paths(episode_id).workspace/"candidate.cu"
    workspace_candidate.write_text(source,encoding="utf-8")
    submission=store.submit_continue(episode_id=episode_id,invocation_id="POOL20-"+cid,
        current_source=source,frozen_identity_rule=lambda new,old:new!=old)
    state=submission.get("state")
    if state not in {"QUEUED","RUNNING","COMPLETED","CONSUMED"}:
        raise RuntimeError("POOL20_CANDIDATE_NOT_RESUMABLE:"+json.dumps(submission))
    if submission.get("candidate_id")!=cid:raise RuntimeError("POOL20_CANDIDATE_ID_SEQUENCE_MISMATCH:"+str(submission))
    # Reuse the same request identity. RUNNING means the shared OJ already claimed it;
    # wait for its authoritative terminal row instead of treating the resume as an error.
    row=_wait_row(store,submission["request_id"]) if state in {"QUEUED","RUNNING"} else submission
    source_sha=sha(source.encode("utf-8"))
    receipt=_verified_receipt(row) if row.get("state") in {"COMPLETED","CONSUMED"} else None
    if receipt and str(receipt.get("verdict","")).startswith("OJ_INFRASTRUCTURE_HOLD_") and receipt.get("score") is None:
        original,receipt=_consume_infra_receipt(store,row,results,cid,source_sha)
        return _retry_infrastructure_once(store,original,receipt,results,cid,source_sha)
    path,result=_settle(store,episode_id,row,results,cid)
    if str(result.get("status","")).startswith("OJ_INFRASTRUCTURE_HOLD_") and result.get("score") is None:
        latest=next(x for x in store.snapshot()["oj_requests"] if x["request_id"]==row["request_id"])
        original,receipt=_consume_infra_receipt(store,latest,results,cid,source_sha)
        return _retry_infrastructure_once(store,original,receipt,results,cid,source_sha)
    return path,result
