"""Enforce H/P policy at the actual Harness tool-dispatch boundary."""
from __future__ import annotations

from typing import Any

from experiment_policy import (
    HARDWARE_TOOL, InformationRouter, ProtocolError,
    scrub_common_information,
)


class PolicyToolBridge:
    def __init__(self, *, ordinary_tool_server: Any, information_router: InformationRouter,
                 audit_append=None, current_candidate_id=None):
        self.ordinary = ordinary_tool_server
        self.router = information_router
        self.audit_append = audit_append or (lambda _: None)
        self.current_candidate_id = current_candidate_id
        self.workspace = getattr(ordinary_tool_server, "workspace", None)

    def call(self, name: str, arguments: dict[str, Any], *, episode_id: str,
             invocation_id: str, turn_id: str, request_id: str, response_id: str,
             tool_call_id: str, visible_content_sha256s: list[str] | None = None,
             current_candidate_id: str | None = None) -> dict[str, Any]:
        tool_name = name
        if current_candidate_id is None and callable(self.current_candidate_id):
            current_candidate_id = self.current_candidate_id(episode_id)
        if tool_name == HARDWARE_TOOL:
            return self.router.call(tool_name, arguments, episode_id=episode_id,
                invocation_id=invocation_id, turn_id=turn_id, tool_call_id=tool_call_id,
                current_candidate_id=current_candidate_id)
        if tool_name == "query_candidate_history":
            fields = arguments.get("fields", [])
            if "profile" in fields:
                if self.router.treatment.profile == "0":
                    self.audit_append({"event_type": "POLICY_SIDE_CHANNEL_BLOCKED", "tool_name": tool_name,
                        "episode_id": episode_id, "invocation_id": invocation_id, "turn_id": turn_id,
                        "tool_call_id": tool_call_id, "field": "profile"})
                    raise ProtocolError("PROFILE_FIELD_NOT_AUTHORIZED")
        result = self.ordinary.call(tool_name, arguments, episode_id=episode_id,
            invocation_id=invocation_id, turn_id=turn_id, request_id=request_id,
            response_id=response_id, tool_call_id=tool_call_id,
            visible_content_sha256s=visible_content_sha256s)
        verified_profiles = {}
        if tool_name == "query_candidate_history" and "profile" in arguments.get("fields", []):
            data = result.get("data") if isinstance(result, dict) else None
            records = data.get("records") if isinstance(data, dict) else None
            if result.get("status") != "OK" or not isinstance(records, list):
                raise ProtocolError("PROFILE_HISTORY_ENVELOPE_NOT_DIRECTLY_VERIFIABLE")
            for record in records:
                candidate_id = record.get("candidate_id") if isinstance(record, dict) else None
                if not isinstance(candidate_id, str):
                    raise ProtocolError("PROFILE_HISTORY_CANDIDATE_ID_MISSING")
                verified = self.router.call("__profile_history_internal__", {"candidate_id": candidate_id},
                    episode_id=episode_id, invocation_id=invocation_id, turn_id=turn_id,
                    tool_call_id=tool_call_id + ":" + candidate_id,
                    current_candidate_id=current_candidate_id)
                verified_profiles[candidate_id] = verified
        if tool_name in {"read_workspace_file", "query_candidate_history"}:
            cleaned = scrub_common_information(result)
            if verified_profiles:
                clean_records = cleaned.get("data", {}).get("records", [])
                for record in clean_records:
                    cid = record.get("candidate_id")
                    if cid in verified_profiles:
                        record["profile"] = verified_profiles[cid]
            return cleaned
        return result
