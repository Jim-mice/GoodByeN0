from __future__ import annotations
import json
from typing import Any
from .provider_r20_bridge import ProviderOutcomeUnknown

class ReconciledR20Dispatch:
    """R6 dispatch callback that first reconciles the same durable request index.

    Existing OUTCOME_UNKNOWN rows are never resubmitted. Only a complete,
    identity-matched R20 result may be persisted/consumed through reconcile_only.
    """
    def __init__(self, *, adapter, bridge, store, episode_id:str, invocation_id:str):
        self.adapter=adapter; self.bridge=bridge; self.store=store
        self.episode_id=episode_id; self.invocation_id=invocation_id; self.index=0
    def __call__(self,payload:bytes)->dict[str,Any]:
        self.index+=1
        digest=__import__('hashlib').sha256(payload).hexdigest()
        db=self.store._connect()
        try: row=db.execute("SELECT * FROM provider_requests WHERE episode_id=? AND invocation_id=? AND request_index=?",
                            (self.episode_id,self.invocation_id,self.index)).fetchone()
        finally: db.close()
        if not row:
            result=self.adapter.dispatch(episode_id=self.episode_id,invocation_id=self.invocation_id,
                request_index=self.index,payload=payload)
            if result.get("provider_error"): return result
            return {"request_id":result["request_id"],"attempt_id":result["attempt_id"],
                "response_id":result["response_id"],"response_body":result["response_body"],"dispatched":True,
                "provider_attempt_ids":result.get("provider_attempt_ids",[]),"provider_request_id":result.get("provider_request_id"),
                "queue_release_id":result.get("queue_release_id"),"queue_manifest_sha256":result.get("queue_manifest_sha256")}
        if row["payload_sha256"]!=digest:
            raise ProviderOutcomeUnknown("R6_EXISTING_REQUEST_PAYLOAD_SHA_MISMATCH_NO_REPLAY")
        if not row["attempt_id"]:
            raise ProviderOutcomeUnknown("R6_EXISTING_REQUEST_HAS_NO_ATTEMPT_ID")
        if row["state"]=="OUTCOME_UNKNOWN":
            result=self.bridge.reconcile_only(store=self.store,request_id=row["request_id"],attempt_id=row["attempt_id"],
                episode_id=self.episode_id,payload_sha256=digest)
            if result.get("status")!="RESPONSE_CONSUMED":
                raise ProviderOutcomeUnknown("R20_SAVED_RESPONSE_NOT_CONSUMED:"+str(result.get("status")))
            return {"request_id":row["request_id"],"attempt_id":row["attempt_id"],"response_id":result["response_id"],
                "response_body":result["response_body"],"dispatched":True,"provider_attempt_ids":result.get("provider_attempt_ids",[]),
                "provider_request_id":result.get("provider_request_id"),"queue_release_id":self.adapter.provider_release_id,
                "queue_manifest_sha256":self.adapter.provider_manifest_sha256}
        if row["state"]=="RESPONSE_RECEIVED":
            response=self.store.consume_provider_response(row["request_id"],row["response_id"],row["response_sha256"])
            return {"request_id":row["request_id"],"attempt_id":row["attempt_id"],"response_id":row["response_id"],
                "response_body":response,"dispatched":True,"provider_attempt_ids":[],"provider_request_id":None,
                "queue_release_id":self.adapter.provider_release_id,"queue_manifest_sha256":self.adapter.provider_manifest_sha256}
        if row["state"]=="CONSUMED":
            path=(self.store.root/row["response_path"]).resolve()
            path.relative_to(self.store.root)
            response=path.read_bytes()
            if __import__('hashlib').sha256(response).hexdigest()!=row["response_sha256"]:
                raise ProviderOutcomeUnknown("R6_SAVED_RESPONSE_SHA_MISMATCH_NO_REPLAY")
            return {"request_id":row["request_id"],"attempt_id":row["attempt_id"],"response_id":row["response_id"],
                "response_body":response,"dispatched":True,"provider_attempt_ids":[],"provider_request_id":None,
                "queue_release_id":self.adapter.provider_release_id,"queue_manifest_sha256":self.adapter.provider_manifest_sha256}
        raise ProviderOutcomeUnknown("R6_EXISTING_REQUEST_STATE_NOT_RECONCILABLE:"+str(row["state"]))
