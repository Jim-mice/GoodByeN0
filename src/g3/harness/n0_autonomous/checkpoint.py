"""Minimal atomic N0 transaction checkpoint; never re-dispatches an unknown request."""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from .engine import N0Error, canonical_bytes, sha256_bytes


class CheckpointStore:
    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def load(self) -> dict[str, Any] | None:
        if not self.path.exists():
            return None
        value = json.loads(self.path.read_text(encoding="utf-8"))
        checksum = value.pop("checkpoint_sha256", None)
        if checksum != sha256_bytes(canonical_bytes(value)):
            raise N0Error("CHECKPOINT_SHA_MISMATCH")
        value["checkpoint_sha256"] = checksum
        return value

    def commit(self, state: dict[str, Any]) -> dict[str, Any]:
        old = self.load()
        next_revision = 1 if old is None else int(old["revision"]) + 1
        if old and state.get("provider_request_id") == old.get("provider_request_id") and state.get("phase") == "DISPATCH_REQUIRED":
            raise N0Error("UNKNOWN_OR_EXISTING_PROVIDER_REQUEST_MUST_NOT_BE_REDISPATCHED")
        payload = dict(state)
        consumed = list(old.get("consumed_provider_response_ids", [])) if old else []
        phase = payload.get("phase")
        if phase == "RESPONSE_RECEIVED":
            if (not payload.get("provider_request_id") or not payload.get("provider_attempt_id")
                    or not payload.get("request_body_sha256") or not payload.get("provider_response_id")
                    or not payload.get("response_body_sha256")):
                raise N0Error("RECEIVED_RESPONSE_IDENTITY_INCOMPLETE")
            if payload["provider_response_id"] in consumed:
                raise N0Error("PROVIDER_RESPONSE_ALREADY_CONSUMED")
            if not old or old.get("phase") != "MODEL_TURN_PENDING":
                raise N0Error("RESPONSE_HAS_NO_PENDING_CHECKPOINT")
            if payload.get("provider_request_id") != old.get("provider_request_id"):
                raise N0Error("RESPONSE_REQUEST_DOES_NOT_MATCH_PENDING_REQUEST")
            if payload.get("request_body_sha256") != old.get("request_body_sha256"):
                raise N0Error("RESPONSE_PAYLOAD_SHA_DOES_NOT_MATCH_PENDING_REQUEST")
        if phase == "INVOCATION_COMPLETE":
            if not old or old.get("phase") != "RESPONSE_RECEIVED":
                raise N0Error("NO_SAVED_RESPONSE_AT_CONSUMPTION_BOUNDARY")
            if (payload.get("provider_request_id") != old.get("provider_request_id")
                    or payload.get("provider_response_id") != old.get("provider_response_id")):
                raise N0Error("CONSUMED_RESPONSE_IDENTITY_MISMATCH")
            response_id = old["provider_response_id"]
            if response_id in consumed:
                raise N0Error("PROVIDER_RESPONSE_ALREADY_CONSUMED")
            consumed.append(response_id)
        payload["consumed_provider_response_ids"] = consumed
        payload["revision"] = next_revision
        payload["checkpoint_sha256"] = sha256_bytes(canonical_bytes(payload))
        raw = canonical_bytes(payload)
        temp = self.path.with_name(self.path.name + ".tmp")
        with temp.open("wb") as f:
            f.write(raw); f.flush(); os.fsync(f.fileno())
        os.replace(temp, self.path)
        return payload

    def recovery_action(self) -> str:
        state = self.load()
        if state is None:
            return "START_NEW_INVOCATION"
        phase = state.get("phase")
        if phase == "MODEL_TURN_PENDING":
            return "WAIT_FOR_OR_CONSUME_SAME_REQUEST_ONLY"
        if phase == "RESPONSE_RECEIVED":
            return "CONSUME_SAVED_RESPONSE_ONCE"
        if phase == "INVOCATION_COMPLETE":
            return "START_FRESH_INVOCATION"
        if phase == "OUTCOME_UNKNOWN":
            return "HOLD_NO_AUTOMATIC_REPLAY"
        return "HOLD_UNKNOWN_CHECKPOINT_PHASE"
