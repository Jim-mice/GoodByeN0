"""Queue-adapter boundary for a single N0 outer invocation.

`dispatch` is injected by a formally released Provider Queue adapter. This
module does not perform HTTP or retry. Tests use a deterministic local fake.
"""
from __future__ import annotations

import json
from typing import Any, Callable

from .engine import (AuditJournal, InvocationTranscript, N0Error, N0ToolServer,
                     build_first_request_messages, build_provider_request_body,
                     canonical_bytes, sha256_bytes, sha256_text)


class ProviderOutcomeUnknown(RuntimeError):
    pass


class ProviderContextWindowExceeded(RuntimeError):
    """An explicit upstream context-window error; never an Agent STOP or Candidate verdict."""
    pass


def is_explicit_context_window_error(status_code: Any, error_body: bytes) -> bool:
    if status_code != 400 or not isinstance(error_body,bytes):
        return False
    try: text=json.dumps(json.loads(error_body.decode("utf-8")),ensure_ascii=False).lower()
    except Exception: text=error_body.decode("utf-8",errors="replace").lower()
    return "contextwindowexceedederror" in text or (
        "context window" in text and ("exceeds" in text or "maximum length" in text))


class N0AgentHarness:
    def __init__(self, *, system_prompt: str, task_user_message: str, tools: list[dict[str, Any]],
                 model: str, reasoning_effort: str, tool_server: N0ToolServer, audit: AuditJournal,
                 dispatch: Callable[[bytes], dict[str, Any]], max_api_requests_per_invocation: int,
                 max_tool_calls_per_invocation: int = 32,
                 working_note_store: Any | None = None,
                 control_finalizer: Callable[..., dict[str, Any]] | None = None,
                 batch_control: Any | None = None):
        if (not isinstance(max_api_requests_per_invocation, int) or isinstance(max_api_requests_per_invocation, bool)
                or max_api_requests_per_invocation < 1):
            raise N0Error("MAX_API_REQUESTS_MUST_BE_EXPLICIT")
        if (not isinstance(max_tool_calls_per_invocation,int) or isinstance(max_tool_calls_per_invocation,bool)
                or max_tool_calls_per_invocation < 1):
            raise N0Error("MAX_TOOL_CALLS_MUST_BE_EXPLICIT")
        if working_note_store is None:
            raise N0Error("WORKING_NOTE_STORE_REQUIRED")
        if not callable(control_finalizer):
            raise N0Error("CONTROL_FINALIZER_REQUIRED")
        if batch_control is None or not callable(getattr(batch_control,"is_aborted",None)) or not callable(getattr(batch_control,"abort",None)):
            raise N0Error("BATCH_ABORT_CONTROLLER_REQUIRED")
        self.system_prompt = system_prompt; self.task_user_message = task_user_message
        self.tools = tools; self.model = model; self.reasoning_effort = reasoning_effort
        self.tool_server = tool_server; self.audit = audit; self.dispatch = dispatch
        self.max_api_requests = max_api_requests_per_invocation
        self.max_tool_calls = max_tool_calls_per_invocation
        self.working_note_store = working_note_store
        self.control_finalizer = control_finalizer
        self.batch_control = batch_control

    def _build_initial_messages(self, *, episode: dict[str, Any], candidate_budget: int,
                                visible_observation: str, invocation_id: str,
                                audit_events: list[dict[str, Any]] | None,
                                working_note: str | None) -> list[dict[str, Any]]:
        from .engine import build_history_brief
        if self.tool_server.workspace is None: raise N0Error("WORKSPACE_NOT_BOUND")
        self.tool_server.workspace.begin_invocation(invocation_id)
        required=self.tool_server.workspace.required_context()
        episode_workspace=episode.get("workspace_files")
        if not isinstance(episode_workspace,dict): raise N0Error("EPISODE_WORKSPACE_SNAPSHOT_MISSING")
        mandatory={"candidate.cu","task_definition.json","candidate_abi.json","visible_observation.json"}
        if not mandatory<=set(episode_workspace): raise N0Error("EPISODE_WORKSPACE_SNAPSHOT_INCOMPLETE")
        current_snapshot={k:v for k,v in required.items() if k!="candidate_sha256"}
        if set(episode_workspace)!=set(current_snapshot): raise N0Error("EPISODE_WORKSPACE_SNAPSHOT_FILE_SET_MISMATCH")
        for name,value in current_snapshot.items():
            if episode_workspace.get(name)!=value: raise N0Error("EPISODE_WORKSPACE_SNAPSHOT_MISMATCH:"+name)
        if visible_observation!=required["visible_observation.json"]:
            raise N0Error("LATEST_OBSERVATION_WORKSPACE_MISMATCH")
        if working_note is None: working_note=self.working_note_store.latest()
        brief=build_history_brief(episode,candidate_budget=candidate_budget,
            audit_events=self.audit.events() if audit_events is None else audit_events,working_note=working_note)
        parsed=json.loads(brief)
        footprint=json.dumps({"previously_retrieved":parsed["previously_retrieved"],
            "last_history_query":parsed["last_history_query"]},ensure_ascii=False,sort_keys=True,separators=(",",":"))
        return build_first_request_messages(self.system_prompt,self.task_user_message,
            task_definition=required["task_definition.json"],candidate_abi=required["candidate_abi.json"],
            candidate_source=required["candidate.cu"],candidate_sha256=required["candidate_sha256"],
            visible_observation=visible_observation,
            history_brief=brief,previously_retrieved=footprint,working_note=working_note,
            tools=self.tools)

    def run_outer_invocation(self, *, episode: dict[str, Any], candidate_budget: int,
                             visible_observation: str, invocation_id: str,
                             turn_id: str, audit_events: list[dict[str, Any]], working_note: str | None = None) -> dict[str, Any]:
        settled_agent_candidates = sum(c.get("candidate_id") != "C0000" for c in episode.get("candidates", []))
        if settled_agent_candidates >= candidate_budget:
            self.audit.append({"event_type":"EXTERNAL_CANDIDATE_CUTOFF","episode_id":episode["episode_id"],
                "invocation_id":invocation_id,"turn_id":turn_id,"provider_dispatched":False,
                "settled_agent_candidates":settled_agent_candidates})
            return {"status":"EPISODE_EXTERNAL_CUTOFF","provider_dispatched":False,
                    "candidate_submission":False}
        try:
            messages = self._build_initial_messages(episode=episode,candidate_budget=candidate_budget,
                visible_observation=visible_observation,invocation_id=invocation_id,audit_events=audit_events,
                working_note=working_note)
        except Exception as exc:
            self.audit.append({"event_type":"INITIAL_CONTEXT_REJECTED","episode_id":episode["episode_id"],
                "invocation_id":invocation_id,"turn_id":turn_id,"provider_dispatched":False,
                "error":type(exc).__name__+":"+str(exc)})
            raise
        transcript = InvocationTranscript(messages)
        invocation_tool_calls: list[dict[str, str]] = []
        for request_index in range(1, self.max_api_requests + 1):
            abort=self.batch_control.is_aborted()
            if abort:
                raise N0Error("SCIENCE_BATCH_ABORTED:"+str(abort.get("reason")))
            body = build_provider_request_body(transcript.messages, model=self.model,
                                               reasoning_effort=self.reasoning_effort, tools=self.tools)
            request_bytes = json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            response = self.dispatch(request_bytes)
            if isinstance(response,dict) and response.get("provider_error") is True:
                status_code=response.get("status_code")
                error_body=response.get("error_body")
                if not isinstance(error_body,bytes): error_body=canonical_bytes(response.get("error",{}))
                explicit_context=is_explicit_context_window_error(status_code,error_body)
                self.audit.record_provider_exchange(request_id=response.get("request_id"),attempt_id=response.get("attempt_id"),
                    response_id=response.get("response_id"),request_body=request_bytes,response_body=error_body,
                    dispatched=True,metadata={"status":"PROVIDER_CONTEXT_WINDOW_EXCEEDED" if explicit_context else "PROVIDER_HTTP_ERROR",
                    "status_code":status_code,"request_index":request_index,"episode_id":episode["episode_id"],
                    "invocation_id":invocation_id,"turn_id":turn_id,"error_sha256":sha256_bytes(error_body)})
                if explicit_context:
                    event={"event_type":"PROVIDER_CONTEXT_WINDOW_EXCEEDED","episode_id":episode["episode_id"],
                        "invocation_id":invocation_id,"turn_id":turn_id,"request_id":response.get("request_id"),
                        "attempt_id":response.get("attempt_id"),"status_code":status_code,
                        "request_body_sha256":sha256_bytes(request_bytes),"error_body_sha256":sha256_bytes(error_body),
                        "error_body":error_body.decode("utf-8",errors="replace"),"candidate_budget_used":
                        sum(c.get("candidate_id")!="C0000" for c in episode.get("candidates",[]))}
                    self.batch_control.abort("PROVIDER_CONTEXT_WINDOW_EXCEEDED",event)
                    raise ProviderContextWindowExceeded("PROVIDER_CONTEXT_WINDOW_EXCEEDED")
                raise N0Error("PROVIDER_HTTP_ERROR:"+str(status_code))
            request_id = response.get("request_id")
            attempt_id = response.get("attempt_id")
            raw_response = response.get("response_body")
            if not isinstance(raw_response, bytes):
                self.audit.record_provider_exchange(request_id=request_id,attempt_id=attempt_id,response_id=None,
                    request_body=request_bytes,response_body=None,dispatched=bool(response.get("dispatched")),
                    metadata={"status":"OUTCOME_UNKNOWN","request_index":request_index,
                              "episode_id":episode["episode_id"],"invocation_id":invocation_id,"turn_id":turn_id})
                raise ProviderOutcomeUnknown("provider outcome unknown; no automatic replay")
            exchange = json.loads(raw_response.decode("utf-8"))
            response_id = exchange.get("id")
            self.audit.record_provider_exchange(request_id=request_id,attempt_id=attempt_id,response_id=response_id,
                request_body=request_bytes,response_body=raw_response,dispatched=bool(response.get("dispatched")),
                metadata={"status":"RESPONSE_RECEIVED","request_index":request_index,"usage":exchange.get("usage"),
                          "episode_id":episode["episode_id"],"invocation_id":invocation_id,"turn_id":turn_id,
                          "queue_release_id":response.get("queue_release_id"),
                          "queue_manifest_sha256":response.get("queue_manifest_sha256"),
                          "provider_request_id":response.get("provider_request_id"),
                          "provider_attempt_ids":response.get("provider_attempt_ids",[])})
            choices = exchange.get("choices") or []
            if len(choices) != 1:
                raise N0Error("PROVIDER_CHOICE_COUNT_INVALID")
            choice = choices[0]
            if choice.get("finish_reason") == "length":
                raise N0Error("PROVIDER_OUTPUT_TRUNCATED")
            message = choice.get("message")
            if not isinstance(message, dict):
                raise N0Error("PROVIDER_MESSAGE_MISSING")
            calls = message.get("tool_calls") or []
            if not calls:
                content = message.get("content")
                try:
                    control = json.loads(content) if isinstance(content, str) else None
                except Exception as exc:
                    raise N0Error("FINAL_CONTROL_JSON_INVALID") from exc
                if (not isinstance(control, dict) or set(control) - {"decision", "reason", "working_note"}
                        or control.get("decision") not in {"CONTINUE", "STOP"}
                        or not isinstance(control.get("reason"), str) or not control["reason"] or len(control["reason"]) > 1000):
                    raise N0Error("FINAL_CONTROL_SCHEMA_INVALID")
                note = control.get("working_note")
                if note is not None and (not isinstance(note, str) or len(note) > 1000):
                    raise N0Error("WORKING_NOTE_INVALID_OR_TOO_LONG")
                transcript.append_assistant(message)
                completed = transcript.finish()
                submission = self.control_finalizer(episode_id=episode["episode_id"],
                    invocation_id=invocation_id,turn_id=turn_id,decision=control["decision"],reason=control["reason"])
                if self.working_note_store is not None:
                    self.working_note_store.record(note, episode_id=episode["episode_id"], turn_id=turn_id)
                action = {"decision":control["decision"],"reason":control["reason"],
                          "current_candidate_id":episode.get("current_candidate_id"),
                          "candidate_edit_tool_call_ids":[x["tool_call_id"] for x in invocation_tool_calls if x["tool_name"] == "edit_candidate"]}
                for prior_call in invocation_tool_calls:
                    self.audit.record_followup_action(prior_call["tool_call_id"],action,invocation_id=invocation_id)
                self.audit.append({"event_type":"CONTROL_DECISION","episode_id":episode["episode_id"],
                    "invocation_id":invocation_id,"turn_id":turn_id,"request_id":request_id,
                    "response_id":response_id,"decision":control["decision"],"reason":control["reason"],
                    "working_note":note,"working_note_sha256":sha256_text(note) if note is not None else None,
                    "response_body_sha256":sha256_bytes(raw_response),"submission":submission})
                return {"status":"INVOCATION_COMPLETE","decision":control["decision"],"reason":control["reason"],
                        "working_note":note,"messages":completed["messages"],"request_id":request_id,"response_id":response_id,
                        "raw_response_sha256":sha256_bytes(raw_response),"submission":submission}
            transcript.append_assistant(message)
            if len(invocation_tool_calls) + len(calls) > self.max_tool_calls:
                self.audit.append({"event_type":"TOOL_CALL_BUDGET_REJECTED","episode_id":episode["episode_id"],
                    "invocation_id":invocation_id,"turn_id":turn_id,"request_id":request_id,
                    "response_id":response_id,"received_tool_calls":len(calls),
                    "already_received_tool_calls":len(invocation_tool_calls),"limit":self.max_tool_calls,
                    "tool_calls_executed_from_response":False,"response_body_sha256":sha256_bytes(raw_response)})
                raise N0Error("MAX_TOOL_CALLS_PER_INVOCATION_REACHED")
            for call in calls:
                fn = call.get("function") or {}
                tool_name = fn.get("name", "")
                call_id = call.get("id", "")
                try:
                    arguments = json.loads(fn.get("arguments", "{}"))
                    if not isinstance(arguments, dict):
                        raise ValueError("tool arguments must be a JSON object")
                    visible_shas = [sha256_text(m["content"]) for m in transcript.messages if m.get("role") == "tool"]
                    result = self.tool_server.call(tool_name,arguments,episode_id=episode["episode_id"],
                        invocation_id=invocation_id,turn_id=turn_id,request_id=request_id,response_id=response_id,
                        tool_call_id=call_id,visible_content_sha256s=visible_shas)
                except Exception as exc:
                    result = {"status":"ERROR","error_code":"TOOL_ARGUMENTS_INVALID","detail":str(exc)}
                    self.audit.record_invalid_tool_call(episode_id=episode["episode_id"],invocation_id=invocation_id,
                        turn_id=turn_id,request_id=request_id,response_id=response_id,tool_call_id=call_id,
                        tool_name=tool_name,raw_arguments=fn.get("arguments"),error=str(exc))
                transcript.append_tool_result(call_id,canonical_bytes(result).decode("utf-8"))
                invocation_tool_calls.append({"tool_call_id":call_id,"tool_name":tool_name})
            if request_index == self.max_api_requests:
                raise N0Error("MAX_API_REQUESTS_PER_INVOCATION_REACHED")
        raise N0Error("INVOCATION_LOOP_FAILED")
