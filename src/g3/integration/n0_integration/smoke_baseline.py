from __future__ import annotations
import hashlib, os, time, uuid
from pathlib import Path

SEED_SHA="CBD7E93B47AD3957E4E133BDE45C0AF1E933DD9BEC3525C27637049337FB2D91"

def enqueue_non_scientific_c0000(store, episode_id:str)->dict:
    """Queue the common Seed as C0000 without consuming Agent Candidate budget.

    Restricted to explicitly labeled non-scientific smoke Episodes. This is an
    integration implementation of the separate baseline transaction, not a
    formal allocation or scientific score adjudication.
    """
    paths=store.episode_paths(episode_id); allocation_path=paths.root/"allocation.json"
    if not allocation_path.is_file(): raise ValueError("SMOKE_ALLOCATION_MISSING")
    import json
    allocation=json.loads(allocation_path.read_text(encoding="utf-8"))
    if allocation.get("classification")!="NON_SCIENTIFIC_SMOKE": raise ValueError("SMOKE_BASELINE_ONLY")
    source_path=paths.workspace/"candidate.cu"; raw=source_path.read_bytes()
    source_sha=hashlib.sha256(raw).hexdigest().upper()
    if source_sha.upper()!=SEED_SHA.upper(): raise ValueError("SMOKE_C0000_SEED_SHA_MISMATCH")
    request_id="N0-OJ-"+uuid.uuid4().hex
    invocation="SMOKE-C0000-BASELINE"
    candidate_rel=Path("episodes")/episode_id/"archive"/"candidates"/("C0000-"+source_sha.lower()+".cu")
    candidate_path=store.root/candidate_rel; candidate_path.parent.mkdir(parents=True,exist_ok=True)
    if candidate_path.exists():
        if candidate_path.read_bytes()!=raw: raise ValueError("SMOKE_C0000_SOURCE_CONFLICT")
    else:
        temp=candidate_path.with_name(candidate_path.name+"."+uuid.uuid4().hex+".tmp")
        with temp.open("xb") as f: f.write(raw); f.flush(); os.fsync(f.fileno())
        os.replace(temp,candidate_path)
    db=store._connect(); now=time.time()
    try:
        db.execute("BEGIN IMMEDIATE")
        ep=db.execute("SELECT * FROM episodes WHERE episode_id=?",(episode_id,)).fetchone()
        if not ep or ep["state"]!="ALLOCATED" or ep["candidates_used"]!=0 or str(ep["last_candidate_sha256"]).upper()!=SEED_SHA.upper():
            raise ValueError("SMOKE_C0000_EPISODE_STATE_INVALID")
        if db.execute("SELECT 1 FROM oj_requests WHERE episode_id=? AND candidate_id='C0000'",(episode_id,)).fetchone():
            raise ValueError("SMOKE_C0000_ALREADY_QUEUED")
        seq=int(db.execute("SELECT value FROM runtime_meta WHERE key='queue_sequence'").fetchone()[0])+1
        db.execute("UPDATE runtime_meta SET value=? WHERE key='queue_sequence'",(str(seq),))
        db.execute("INSERT INTO oj_requests(request_id,episode_id,invocation_id,candidate_id,candidate_sha256,state,queue_sequence,candidate_source_path,created_at,updated_at) VALUES(?,?,?,?,?,'QUEUED',?,?,?,?)",
          (request_id,episode_id,invocation,"C0000",SEED_SHA.lower(),seq,candidate_rel.as_posix(),now,now))
        db.execute("UPDATE episodes SET state='WAITING_OJ',updated_at=? WHERE episode_id=? AND state='ALLOCATED'",(now,episode_id))
        event={"event_type":"NON_SCIENTIFIC_C0000_BASELINE_QUEUED","request_id":request_id,"candidate_id":"C0000",
          "candidate_sha256":SEED_SHA.lower(),"candidate_source_path":candidate_rel.as_posix(),"queue_sequence":seq,
          "agent_budget_consumed":False,"classification":"NON_SCIENTIFIC_SMOKE"}
        store._insert_event_tx(db,episode_id,event,"smoke-c0000:"+episode_id)
        db.execute("COMMIT")
    except BaseException:
        if db.in_transaction: db.execute("ROLLBACK")
        raise
    finally: db.close()
    store.reconcile_episode_exports(episode_id)
    return {"request_id":request_id,"episode_id":episode_id,"invocation_id":invocation,"candidate_id":"C0000",
      "candidate_sha256":SEED_SHA.lower(),"candidate_source_path":candidate_rel.as_posix(),"state":"QUEUED",
      "queue_sequence":seq,"candidate_budget_consumed":False,"classification":"NON_SCIENTIFIC_SMOKE"}
