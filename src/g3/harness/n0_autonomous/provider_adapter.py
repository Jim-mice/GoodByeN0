"""R5 Provider Queue boundary. It never talks to HTTP and never bypasses queue identity."""
from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Callable

from .engine import N0Error, canonical_bytes, sha256_bytes


class N0R5ProviderQueueAdapter:
    def __init__(self, *, store, queue_capability: dict[str, Any], enqueue: Callable[..., dict[str, Any]],
                 provider_release_id: str, provider_manifest_sha256: str,
                 treatment_id: str = "N0_AUTONOMOUS"):
        self.store=store
        self.queue_capability=queue_capability
        self.enqueue=enqueue
        self.provider_release_id=provider_release_id
        self.provider_manifest_sha256=provider_manifest_sha256
        self.treatment_id=treatment_id
        if treatment_id not in {"N0_AUTONOMOUS", "G3"}:
            raise N0Error("N0_TREATMENT_NOT_SUPPORTED_BY_PROVIDER_ADAPTER")
        if not callable(enqueue): raise N0Error("N0_R5_PROVIDER_QUEUE_ADAPTER_NOT_BOUND")

    def preflight(self) -> dict[str, Any]:
        cap=self.queue_capability
        if cap.get("status")!="PASS" or cap.get("treatment_id")!=self.treatment_id:
            raise N0Error("N0_TREATMENT_NOT_AUTHORIZED_BY_PROVIDER_QUEUE")
        if self.treatment_id == "G3" and (cap.get("group") != "G3"
                or cap.get("task_id") != "KB_L1_40_LAYERNORM"
                or cap.get("hardware_information") != "FULL"
                or cap.get("profile_information") != "FULL"
                or cap.get("expert_knowledge") != "ABSENT"):
            raise N0Error("G3_PROVIDER_CAPABILITY_IDENTITY_INVALID")
        if cap.get("provider_release_id")!=self.provider_release_id or cap.get("manifest_sha256")!=self.provider_manifest_sha256:
            raise N0Error("N0_PROVIDER_RELEASE_IDENTITY_MISMATCH")
        if self.treatment_id not in cap.get("allowed_treatments",[]):
            raise N0Error("N0_TREATMENT_NOT_ALLOWLISTED")
        return {"status":"PASS","provider_release_id":self.provider_release_id,
                "manifest_sha256":self.provider_manifest_sha256,"treatment_id":self.treatment_id}

    def dispatch(self, *, episode_id: str, invocation_id: str, request_index: int, payload: bytes) -> dict[str, Any]:
        self.preflight()
        if not re.fullmatch(r"[A-Za-z0-9_.-]{1,128}",episode_id) or not re.fullmatch(r"[A-Za-z0-9_.-]{1,128}",invocation_id):
            raise N0Error("N0_PROVIDER_EPISODE_OR_INVOCATION_ID_INVALID")
        if not isinstance(payload,bytes) or not payload:
            raise N0Error("N0_PROVIDER_PAYLOAD_BYTES_REQUIRED")
        try: json.loads(payload.decode("utf-8"))
        except Exception as exc: raise N0Error("N0_PROVIDER_PAYLOAD_JSON_INVALID") from exc
        identity=canonical_bytes({"episode_id":episode_id,"invocation_id":invocation_id,
                                  "request_index":request_index,"payload_sha256":sha256_bytes(payload),
                                  "provider_release_id":self.provider_release_id,
                                  "provider_manifest_sha256":self.provider_manifest_sha256})
        request_id="N0-PROV-"+hashlib.sha256(identity).hexdigest()[:32]
        row=self.store.begin_provider_request(request_id=request_id,episode_id=episode_id,
            invocation_id=invocation_id,request_index=request_index,payload=payload)
        if row.get("state")!="QUEUED": raise N0Error("N0_PROVIDER_REQUEST_ALREADY_DISPATCHED_OR_UNKNOWN")
        attempt_id="N0-ATT-"+hashlib.sha256((request_id+":"+row["payload_sha256"]).encode()).hexdigest()[:32]
        dispatched = False
        def mark_dispatched() -> None:
            nonlocal dispatched
            if not dispatched:
                self.store.mark_provider_dispatched(request_id,attempt_id)
                dispatched = True
        result=self.enqueue(request_id=request_id,attempt_id=attempt_id,episode_id=episode_id,
            treatment_id=self.treatment_id,payload=payload,payload_sha256=row["payload_sha256"],
            provider_release_id=self.provider_release_id,provider_manifest_sha256=self.provider_manifest_sha256,
            mark_dispatched=mark_dispatched)
        # For synchronous test transports, a returned response proves dispatch.
        # Real queue bridges call mark_dispatched immediately after the durable
        # Provider Queue commit and before waiting for any remote outcome.
        if not dispatched:
            mark_dispatched()
        if not isinstance(result,dict) or result.get("request_id")!=request_id or result.get("attempt_id")!=attempt_id:
            raise N0Error("N0_PROVIDER_QUEUE_RESPONSE_IDENTITY_UNKNOWN")
        queue_payload_sha=result.get("queue_request_body_sha256")
        if queue_payload_sha is not None and queue_payload_sha.lower()!=row["payload_sha256"].lower():
            raise N0Error("N0_PROVIDER_QUEUE_PAYLOAD_SHA_MISMATCH")
        status_code=result.get("status_code",200)
        if isinstance(status_code,int) and status_code>=400:
            error_body=result.get("error_body",result.get("response_body"))
            if not isinstance(error_body,bytes) or not error_body:
                raise N0Error("N0_PROVIDER_HTTP_ERROR_BODY_MISSING_OUTCOME_UNKNOWN")
            digest=self.store.record_provider_error_response(request_id,attempt_id,status_code,error_body)
            return {"request_id":request_id,"attempt_id":attempt_id,"provider_error":True,
                    "status_code":status_code,"error_body":error_body,"error_body_sha256":digest}
        response_body=result.get("response_body")
        response_id=result.get("response_id")
        if not isinstance(response_body,bytes) or not response_body or not isinstance(response_id,str):
            raise N0Error("N0_PROVIDER_RESPONSE_NOT_COMPLETE")
        try:
            response_object=json.loads(response_body.decode("utf-8"))
        except Exception as exc:
            raise N0Error("N0_PROVIDER_RESPONSE_JSON_INVALID") from exc
        if not isinstance(response_object,dict) or response_object.get("id")!=response_id:
            raise N0Error("N0_PROVIDER_RESPONSE_ID_MISMATCH")
        digest=self.store.record_provider_response(request_id,attempt_id,response_id,response_body)
        provider_attempt_ids=result.get("provider_attempt_ids",[])
        if provider_attempt_ids is not None and (not isinstance(provider_attempt_ids,list)
                or any(not isinstance(x,str) or not x for x in provider_attempt_ids)):
            raise N0Error("N0_PROVIDER_ATTEMPT_LINEAGE_INVALID")
        self.store.append_episode_event(episode_id,{"event_type":"N0_PROVIDER_QUEUE_LINEAGE",
            "request_id":request_id,"logical_attempt_id":attempt_id,
            "provider_attempt_ids":provider_attempt_ids or [],
            "queue_request_body_sha256":queue_payload_sha,
            "queue_release_id":self.provider_release_id,
            "queue_manifest_sha256":self.provider_manifest_sha256,
            "provider_request_id":result.get("provider_request_id"),
            "response_id":response_id,"response_body_sha256":digest})
        consumed=self.store.consume_provider_response(request_id,response_id,digest)
        try:
            response=json.loads(consumed.decode("utf-8"))
        except Exception as exc:
            raise N0Error("N0_PROVIDER_RESPONSE_JSON_INVALID") from exc
        return {"request_id":request_id,"attempt_id":attempt_id,"response_id":response_id,
                "response_sha256":digest,"response":response,"response_body":consumed,
                "provider_attempt_ids":provider_attempt_ids or [],
                "provider_request_id":result.get("provider_request_id"),
                "queue_release_id":self.provider_release_id,
                "queue_manifest_sha256":self.provider_manifest_sha256,
                "dispatched":True}

    def bind_invocation(self, *, episode_id: str, invocation_id: str) -> Callable[[bytes], dict[str, Any]]:
        request_index=0
        def send(payload: bytes) -> dict[str, Any]:
            nonlocal request_index
            request_index+=1
            result=self.dispatch(episode_id=episode_id,invocation_id=invocation_id,
                                 request_index=request_index,payload=payload)
            if result.get("provider_error"):
                return result
            return {"request_id":result["request_id"],"attempt_id":result["attempt_id"],
                    "response_id":result["response_id"],"response_body":result["response_body"],
                    "dispatched":True,"provider_attempt_ids":result.get("provider_attempt_ids",[]),
                    "provider_request_id":result.get("provider_request_id"),
                    "queue_release_id":self.provider_release_id,
                    "queue_manifest_sha256":self.provider_manifest_sha256}
        return send


# Compatibility import for R4 tests and integrations; R5 Release identity remains explicit.
N0R4ProviderQueueAdapter = N0R5ProviderQueueAdapter
