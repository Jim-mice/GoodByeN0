from __future__ import annotations

import hashlib
import base64
import json
import os
import time
from pathlib import Path
from typing import Any


class ProviderOutcomeUnknown(RuntimeError):
    """A queued request has no complete, identity-verified response. Never replay it."""


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _required_provider_window(treatment_id: str) -> str | None:
    """Pin G3 to the credential lane previously verified for this model."""
    return "B" if treatment_id == "G3" else None

def _r20_http_body_sha(record:dict[str,Any])->str:
    """Reproduce R20's frozen ParateraChatCompletions.build_body serialization."""
    payload=record.get("request_payload")
    if not isinstance(payload,dict): raise ProviderOutcomeUnknown("R20_REQUEST_PAYLOAD_MISSING")
    allowed={"model","messages","reasoning_effort","tools","tool_choice","max_tokens"}
    if set(payload)-allowed: raise ProviderOutcomeUnknown("R20_PROVIDER_ADAPTER_WOULD_DROP_REQUEST_FIELDS")
    if payload.get("model")!="DeepSeek-V4-Pro-0813" or not isinstance(payload.get("messages"),list):
        raise ProviderOutcomeUnknown("R20_PROVIDER_REQUEST_BODY_INVALID")
    body={"model":payload["model"],"messages":payload["messages"],
          "reasoning_effort":payload.get("reasoning_effort","high")}
    if payload.get("max_tokens") is not None: body["max_tokens"]=int(payload["max_tokens"])
    if payload.get("tools") is not None: body["tools"]=payload["tools"]
    if payload.get("tool_choice") is not None: body["tool_choice"]=payload["tool_choice"]
    wire=json.dumps(body,ensure_ascii=False,separators=(",",":")).encode("utf-8")
    return _sha(wire)


class R20N0Bridge:
    """Bridge the R6 queue callback to an already-running R20 formal queue.

    The queue root is mandatory and must be a dedicated D: formal root. This
    adapter does not start a Dispatcher, issue HTTP, or retry a request.
    R20 owns its pre-existing timeout/retry semantics.
    """
    def __init__(self, *, queue_root: Path, provider_release_root: Path,
                 expected_manifest_sha256: str, timeout_seconds: int = 1800,
                 poll_seconds: float = 0.2, expected_treatment: str = "N0_AUTONOMOUS"):
        self.root = Path(queue_root).resolve()
        if str(self.root.drive).upper() != "D:":
            raise ValueError("N0_FORMAL_PROVIDER_QUEUE_MUST_BE_ON_D")
        if timeout_seconds != 1800 or poll_seconds <= 0:
            raise ValueError("R20_TIMEOUT_OR_POLL_POLICY_MISMATCH")
        self.release_root=Path(provider_release_root).resolve()
        self.expected_provider_release_id = os.environ.get("N0_PROVIDER_RELEASE_ID", "N0_PARATERA_PROVIDER_R20_G3_TREATMENT_ROUTE_R5_SSE_CASEFIX")
        self.manifest_path=self.release_root/"R20_PROVIDER_RELEASE_MANIFEST.json"
        manifest_raw=self.manifest_path.read_bytes()
        if _sha(manifest_raw).lower()!=expected_manifest_sha256.lower():
            raise ValueError("R20_MANIFEST_IDENTITY_MISMATCH")
        self.expected_manifest_sha256=expected_manifest_sha256.lower()
        if expected_treatment not in {"N0_AUTONOMOUS", "G3"}:
            raise ValueError("R20_TREATMENT_NOT_SUPPORTED")
        self.expected_treatment=expected_treatment
        manifest=json.loads(manifest_raw.decode("utf-8"))
        if manifest.get("release_id")!=self.expected_provider_release_id:
            raise ValueError("R20_RELEASE_ID_MISMATCH")
        if expected_treatment not in manifest.get("treatment_allowlist", []):
            raise ValueError("R20_TREATMENT_NOT_ALLOWLISTED_BY_RELEASE")
        for rel,want in manifest.get("source_sha256",{}).items():
            f=(self.release_root/rel).resolve()
            try: f.relative_to(self.release_root)
            except ValueError as exc: raise ValueError("R20_MANIFEST_PATH_ESCAPE") from exc
            if not f.is_file() or _sha(f.read_bytes()).lower()!=str(want).lower():
                raise ValueError("R20_SOURCE_IDENTITY_MISMATCH:"+rel)
        import sys
        sys.path.insert(0,str(self.release_root)) if str(self.release_root) not in sys.path else None
        self.timeout_seconds = timeout_seconds
        self.poll_seconds = poll_seconds

    def result_path(self, request_id: str) -> Path:
        return self.root / "results" / (request_id + ".json")

    def read_verified_result(self, *, request_id: str, attempt_id: str,
                             episode_id: str, payload_sha256: str) -> dict[str, Any]:
        path = self.result_path(request_id)
        if not path.is_file():
            raise ProviderOutcomeUnknown("R20_RESULT_NOT_PERSISTED")
        try:
            record = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            raise ProviderOutcomeUnknown("R20_RESULT_JSON_INVALID") from exc
        if (record.get("request_id") != request_id
                or record.get("episode_id") != episode_id
                or record.get("treatment") != self.expected_treatment
                or record.get("request_body_sha256") != payload_sha256):
            raise ProviderOutcomeUnknown("R20_RESULT_QUEUE_IDENTITY_MISMATCH")
        queued_payload=record.get("request_payload")
        if not isinstance(queued_payload,dict): raise ProviderOutcomeUnknown("R20_REQUEST_PAYLOAD_MISSING")
        allowed={"model","messages","reasoning_effort","tools","tool_choice","max_tokens"}
        if (set(queued_payload)-allowed or queued_payload.get("model")!="DeepSeek-V4-Pro-0813"
                or not isinstance(queued_payload.get("messages"),list)):
            raise ProviderOutcomeUnknown("R20_REQUEST_PAYLOAD_SHAPE_INVALID")
        if record.get("status") in {"RETRY_EXHAUSTED", "FAILED_FINAL"} and isinstance(record.get("last_http_status"), int):
            telemetry = record.get("last_attempt_telemetry") or {}
            if (telemetry.get("request_sha256") != payload_sha256
                    or telemetry.get("attempt_id") != record.get("last_attempt_id")
                    or telemetry.get("http_status") != record.get("last_http_status")):
                raise ProviderOutcomeUnknown("R20_HTTP_ERROR_ATTEMPT_IDENTITY_MISMATCH")
            body = telemetry.get("http_error_body_redacted")
            body_sha = telemetry.get("http_error_body_sha256")
            if not isinstance(body, str) or not isinstance(body_sha, str):
                raise ProviderOutcomeUnknown("R20_HTTP_ERROR_BODY_NOT_PERSISTED")
            body_bytes = body.encode("utf-8")
            if _sha(body_bytes) != body_sha:
                # Provider telemetry hashes UTF-8 bytes of the redacted body; JSON serialization preserves it.
                raise ProviderOutcomeUnknown("R20_HTTP_ERROR_BODY_SHA_MISMATCH")
            return {"request_id": request_id, "attempt_id": attempt_id,
                    "status_code": record["last_http_status"], "error_body": body_bytes,
                    "error_body_sha256": body_sha, "attempt_ids": record.get("attempt_ids", []),
                    "current_attempt_id": record.get("last_attempt_id"),
                    "attempt_lineage_id": attempt_id, "request_body_sha256": payload_sha256,
                    "telemetry": telemetry, "status": record.get("status")}
        if record.get("status") != "COMPLETED":
            raise ProviderOutcomeUnknown("R20_REQUEST_NOT_COMPLETED:" + str(record.get("status")))
        telemetry = record.get("telemetry") or {}
        artifact = telemetry.get("response_artifact_path")
        artifact_sha = telemetry.get("response_artifact_sha256")
        if telemetry.get("response_artifact_status") not in {"PERSISTED_ATOMICALLY", "ALREADY_PRESENT_IDENTICAL"}:
            raise ProviderOutcomeUnknown("R20_RESPONSE_ARTIFACT_NOT_DURABLY_PERSISTED")
        if not isinstance(artifact, str) or not isinstance(artifact_sha, str):
            raise ProviderOutcomeUnknown("R20_RESPONSE_ARTIFACT_IDENTITY_MISSING")
        response_path = Path(artifact).resolve()
        try:
            response_path.relative_to(self.root)
        except ValueError as exc:
            raise ProviderOutcomeUnknown("R20_RESPONSE_ARTIFACT_OUTSIDE_FORMAL_QUEUE") from exc
        raw = response_path.read_bytes()
        if _sha(raw) != artifact_sha:
            raise ProviderOutcomeUnknown("R20_RESPONSE_ARTIFACT_SHA_MISMATCH")
        try:
            artifact_record = json.loads(raw.decode("utf-8"))
        except Exception as exc:
            raise ProviderOutcomeUnknown("R20_RESPONSE_ARTIFACT_JSON_INVALID") from exc
        response_id = record.get("response_id")
        # R20 persists an immutable transport envelope, not the raw HTTP JSON.
        # Verify the envelope and extract its Base64 body without rewriting it.
        if artifact_record.get("schema") == "NOVELTY_V7_PROVIDER_RESPONSE_ARTIFACT_V1":
            if artifact_record.get("attempt_id") != record.get("current_attempt_id"):
                raise ProviderOutcomeUnknown("R20_RESPONSE_ARTIFACT_ATTEMPT_MISMATCH")
            # R20 stores request_payload in JSON result files, whose nested
            # object keys are sorted when persisted. Re-serializing that
            # parsed object cannot reproduce the original HTTP bytes. The
            # queue admission hash, HTTP telemetry hash, and immutable
            # response-sidecar request hash must each equal the original R6
            # payload hash; this binds the exact body without reconstructing it.
            if (artifact_record.get("request_sha256") != payload_sha256
                    or telemetry.get("request_sha256") != payload_sha256):
                raise ProviderOutcomeUnknown("R20_RESPONSE_ARTIFACT_HTTP_REQUEST_SHA_MISMATCH")
            if artifact_record.get("body_complete") is not True:
                raise ProviderOutcomeUnknown("R20_RESPONSE_ARTIFACT_BODY_INCOMPLETE")
            encoded=artifact_record.get("body_base64")
            if not isinstance(encoded,str): raise ProviderOutcomeUnknown("R20_RESPONSE_ARTIFACT_BODY_MISSING")
            try: response_raw=base64.b64decode(encoded,validate=True)
            except Exception as exc: raise ProviderOutcomeUnknown("R20_RESPONSE_ARTIFACT_BASE64_INVALID") from exc
            if _sha(response_raw)!=artifact_record.get("body_sha256"):
                raise ProviderOutcomeUnknown("R20_RESPONSE_ARTIFACT_BODY_SHA_MISMATCH")
            if len(response_raw)!=artifact_record.get("body_bytes_received"):
                raise ProviderOutcomeUnknown("R20_RESPONSE_ARTIFACT_BODY_LENGTH_MISMATCH")
            if artifact_record.get("http_status")!=telemetry.get("http_status"):
                raise ProviderOutcomeUnknown("R20_RESPONSE_ARTIFACT_HTTP_STATUS_MISMATCH")
        else:
            # Legacy R20 stores may place the raw response directly in the artifact.
            response_raw=raw
        response_mode = telemetry.get("response_mode")
        if response_mode == "SSE":
            if (telemetry.get("sse_done_received") is not True
                    or not isinstance(telemetry.get("sse_event_count"), int)
                    or telemetry.get("sse_event_count", 0) < 1):
                raise ProviderOutcomeUnknown("R20_SSE_COMPLETION_EVIDENCE_INVALID")
            if _sha(response_raw) != telemetry.get("raw_response_sha256"):
                raise ProviderOutcomeUnknown("R20_SSE_RAW_RESPONSE_SHA_MISMATCH")
            try:
                # Reuse the parser from the manifest-verified R20 release. The
                # archived bytes remain raw SSE; only the value handed to the
                # Chat Completions consumer is normalized JSON.
                from provider_staging.paratera_http_worker import _aggregate_sse
                parse_telemetry: dict[str, Any] = {}
                body = _aggregate_sse(response_raw, parse_telemetry)
            except Exception as exc:
                raise ProviderOutcomeUnknown("R20_SSE_RESPONSE_BODY_INVALID") from exc
            if (parse_telemetry.get("sse_done_received") is not True
                    or parse_telemetry.get("sse_event_count") != telemetry.get("sse_event_count")
                    or body != record.get("payload")):
                raise ProviderOutcomeUnknown("R20_SSE_NORMALIZED_PAYLOAD_MISMATCH")
            response_body = json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        else:
            try:
                body=json.loads(response_raw.decode("utf-8"))
            except Exception as exc:
                raise ProviderOutcomeUnknown("R20_RESPONSE_BODY_INVALID") from exc
            response_body = response_raw
        if not response_id or body.get("id") != response_id or telemetry.get("response_id") != response_id:
            raise ProviderOutcomeUnknown("R20_RESPONSE_ID_MISMATCH")
        if telemetry.get("body_complete") is not True or telemetry.get("helper_terminal_event_received") is not True:
            raise ProviderOutcomeUnknown("R20_RESPONSE_COMPLETENESS_UNPROVEN")
        attempts = record.get("attempt_ids") or []
        if (not attempts or record.get("current_attempt_id") not in attempts
                or record.get("attempt_lineage_id") != attempt_id):
            raise ProviderOutcomeUnknown("R20_ATTEMPT_LINEAGE_MISSING")
        status_code=telemetry.get("http_status")
        if not isinstance(status_code,int) or not 100 <= status_code <= 599:
            raise ProviderOutcomeUnknown("R20_HTTP_STATUS_MISSING_OR_INVALID")
        if record.get("provider_request_id") is None:
            external_id_status = "NOT_PROVIDED_BY_PROVIDER"
        else:
            external_id_status = "PROVIDED"
        return {
            "request_id": request_id,
            "attempt_id": attempt_id,
            "response_id": response_id,
            "status_code": status_code,
            "response_body": response_body,
            "raw_response_sha256": _sha(response_raw),
            "normalized_response_sha256": _sha(response_body),
            "response_mode": response_mode or "JSON",
            "provider_request_id": record.get("provider_request_id"),
            "provider_request_id_status": external_id_status,
            "provider_attempt_ids": attempts,
            "queue_request_body_sha256": record.get("request_body_sha256"),
            "provider_wire_request_sha256": telemetry.get("request_sha256"),
            "response_artifact_sha256": artifact_sha,
            "queue_result_path": str(path),
        }

    def enqueue(self, *, request_id: str, attempt_id: str, episode_id: str,
                treatment_id: str, payload: bytes, payload_sha256: str,
                provider_release_id: str, provider_manifest_sha256: str,
                mark_dispatched) -> dict[str, Any]:
        if treatment_id != self.expected_treatment:
            raise ValueError("N0_TREATMENT_IDENTITY_MISMATCH")
        if _sha(payload) != payload_sha256:
            raise ValueError("R6_PROVIDER_PAYLOAD_SHA_MISMATCH")
        try:
            body = json.loads(payload.decode("utf-8"))
        except Exception as exc:
            raise ValueError("R6_PROVIDER_PAYLOAD_INVALID") from exc
        if (provider_release_id != self.expected_provider_release_id
                or provider_manifest_sha256.lower()!=self.expected_manifest_sha256):
            raise ValueError("R20_RELEASE_ID_MISMATCH")
        lineage = attempt_id
        from canonical_harness.provider_queue import SharedProviderQueue
        queue = SharedProviderQueue(self.root)
        queued = queue.submit(episode_id=episode_id, treatment=treatment_id,
            model="DeepSeek-V4-Pro-0813", request_body=body,
            attempt_lineage_id=lineage, attempts_already_used=0, max_attempts=3,
            request_id=request_id,
            # The jlapi credential lane (windows A/C) has returned an explicit
            # model-not-available HTTP 400 for this model. A prior successful
            # G3 diagnostic used jlapi2. Pin G3 to window B (jlapi2) so lane
            # selection is deterministic; preserve the request body and model.
            required_window=_required_provider_window(treatment_id))
        if queued.get("request_id") != request_id or queued.get("request_body_sha256") != payload_sha256:
            raise ProviderOutcomeUnknown("R20_QUEUE_ACCEPTANCE_IDENTITY_MISMATCH")
        if queued.get("status") not in {"QUEUED", "RUNNING", "COMPLETED"}:
            raise ProviderOutcomeUnknown("R20_QUEUE_NOT_IN_RECOVERABLE_STATE")
        # Once durably admitted, R6 records OUTCOME_UNKNOWN. A later timeout
        # must be reconciled from the R20 result artifact, never resubmitted.
        mark_dispatched()
        deadline = time.monotonic() + self.timeout_seconds + 60
        result_path = self.result_path(request_id)
        while time.monotonic() < deadline:
            if result_path.is_file():
                return self.read_verified_result(request_id=request_id, attempt_id=attempt_id,
                    episode_id=episode_id, payload_sha256=payload_sha256)
            time.sleep(self.poll_seconds)
        raise ProviderOutcomeUnknown("R20_WAIT_EXPIRED_RESULT_MAY_STILL_ARRIVE_NO_REPLAY")

    def reconcile_only(self, *, store, request_id: str, attempt_id: str,
                       episode_id: str, payload_sha256: str) -> dict[str, Any]:
        """Persist and consume an already-arrived response; never submits a request."""
        result = self.read_verified_result(request_id=request_id, attempt_id=attempt_id,
            episode_id=episode_id, payload_sha256=payload_sha256)
        status = result.get("status_code")
        if isinstance(status, int) and status >= 400:
            store.record_provider_error_response(request_id, attempt_id, status, result["response_body"])
            return {"status": "PROVIDER_HTTP_ERROR_RECONCILED", **result}
        digest = store.record_provider_response(request_id, attempt_id, result["response_id"], result["response_body"])
        response = store.consume_provider_response(request_id, result["response_id"], digest)
        return {"status": "RESPONSE_CONSUMED", "response_sha256": digest,
                "response_body": response, **{k: result[k] for k in ("response_id", "provider_request_id", "provider_request_id_status", "provider_attempt_ids")}}
