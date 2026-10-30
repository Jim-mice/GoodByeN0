from __future__ import annotations
import json
from pathlib import Path
from typing import Any

def candidate_record_from_oj_row(store, row:dict[str,Any])->dict[str,Any]:
    from n0_autonomous.engine import canonical_bytes,sha256_bytes
    root=Path(store.root); src=(root/row['candidate_source_path']).resolve()
    try: src.relative_to(root)
    except ValueError as exc: raise ValueError('HISTORY_SOURCE_PATH_ESCAPE') from exc
    source=src.read_text(encoding='utf-8'); receipt=json.loads(row['receipt_json'])
    artifacts={k:receipt.get(k) for k in ('timing','profile','diagnostics','lineage')}
    artifact_sha={k:sha256_bytes(canonical_bytes(v)) for k,v in artifacts.items()}
    score=receipt.get('score') if receipt.get('verdict')=='VALID_MEASUREMENT' else None
    ledger={"candidate_id":row['candidate_id'],"candidate_sha256":row['candidate_sha256'],
      "request_id":row['request_id'],"verdict":receipt.get('verdict'),"score":float(score) if score is not None else None,
      "promotion":"UNKNOWN","receipt_sha256":receipt.get('receipt_sha256'),"artifact_sha256":artifact_sha}
    ledger['ledger_entry_sha256']=sha256_bytes(canonical_bytes(ledger))
    return {"candidate_id":row['candidate_id'],"source":source,"source_sha256":row['candidate_sha256'],
      "oj_receipt":receipt,"timing":artifacts['timing'],"profile":artifacts['profile'],
      "diagnostics":artifacts['diagnostics'],"lineage":artifacts['lineage'],"ledger_entry":ledger}

def consume_and_query_profile(*, store, request_id:str, whitelist:dict[str,Any])->dict[str,Any]:
    snap=store.snapshot(); row=next((r for r in snap['oj_requests'] if r['request_id']==request_id),None)
    if not row or row.get('state') not in {'COMPLETED','CONSUMED'} or not row.get('receipt_json') or not row.get('receipt_sha256'):
        raise ValueError('OJ_RECEIPT_NOT_DURABLY_SETTLED')
    if row['state']=='COMPLETED': store.consume_oj_result(request_id,row['receipt_sha256'])
    snap=store.snapshot(); row=next(r for r in snap['oj_requests'] if r['request_id']==request_id)
    record=candidate_record_from_oj_row(store,row)
    from n0_autonomous.engine import HistoryService
    history=HistoryService(row['episode_id'],[record],profile_whitelist=whitelist)
    response=history.query_candidate_history({"candidate_ids":[row['candidate_id']],"fields":["profile"]})
    return {"status":"PASS" if response.get('data',{}).get('records',[{}])[0].get('profile',{}).get('status')=='PASS' else 'BLOCKED',
      "episode_id":row['episode_id'],"candidate_id":row['candidate_id'],"request_id":request_id,
      "queue_state":row['state'],"queue_receipt_sha256":row['receipt_sha256'],
      "history_response":response,"profile_source":"IMMUTABLE_OJ_RECEIPT",
      "source_sha256":record['source_sha256']}
