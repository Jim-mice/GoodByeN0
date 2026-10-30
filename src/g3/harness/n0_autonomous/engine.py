"""Deterministic N0 history, retrieval, audit, and workspace tools.

This module has no Provider, CUDA, OJ queue, or network dependency. Production
adapters must call it with already authenticated episode-local records.
"""
from __future__ import annotations

import base64
import hashlib
import json
import math
import os
import re
import time
from pathlib import Path
from typing import Any, Iterable


class N0Error(ValueError):
    pass


class PendingStateError(N0Error):
    pass


class SnapshotChangedError(N0Error):
    pass


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_text(value: str) -> str:
    return sha256_bytes(value.encode("utf-8"))


_N0_TASK_SHA256 = "8D975D7CEE6946513DCFB6BA84C971E53A890F9D9C6F0A4FB828904941F25975"
_N0_TASK_DEFINITION_SHA256 = "3F0EB778C8F6673F1B6589060742CAF56FD21AB57A9DA48C9DECFED79E198709"
_N0_COMPILER_CONTRACT_SHA256 = "303AF7065B56C8A29ED370391681F06C1F31EF5066632D82B7141BF0AEB9C1FA"
_N0_SEED_SHA256 = "CBD7E93B47AD3957E4E133BDE45C0AF1E933DD9BEC3525C27637049337FB2D91"


def _measurement_context(candidate: dict[str, Any]) -> tuple[Any, ...] | None:
    receipt = candidate.get("oj_receipt", {})
    keys = ("task_sha256", "gpu_identity", "release_id", "manifest_sha256", "measurement_protocol")
    values = tuple(receipt.get(k) for k in keys)
    return values if all(v not in (None, "", "UNKNOWN") for v in values) else None


def _safe_id(value: str) -> bool:
    return bool(re.fullmatch(r"C\d{4}", value))


def _workspace_sha(files: dict[str, str]) -> str:
    items = []
    for name, content in sorted(files.items()):
        p = Path(name)
        if p.is_absolute() or ".." in p.parts:
            raise N0Error("WORKSPACE_PATH_INVALID")
        items.append({"path": p.as_posix(), "sha256": sha256_text(content), "bytes": len(content.encode("utf-8"))})
    return sha256_bytes(canonical_bytes(items))


def _validate_candidate(candidate: dict[str, Any]) -> None:
    cid = candidate.get("candidate_id")
    source = candidate.get("source")
    if not _safe_id(cid) or not isinstance(source, str):
        raise N0Error("CANDIDATE_ID_OR_SOURCE_INVALID")
    source_sha = sha256_text(source)
    if candidate.get("source_sha256") != source_sha:
        raise N0Error("CANDIDATE_SOURCE_SHA_MISMATCH:" + str(cid))
    receipt = candidate.get("oj_receipt")
    if not isinstance(receipt, dict) or receipt.get("terminal") is not True:
        raise PendingStateError("CANDIDATE_OJ_NOT_TERMINAL:" + str(cid))
    if receipt.get("candidate_sha256") != source_sha:
        raise N0Error("OJ_CANDIDATE_SHA_MISMATCH:" + str(cid))
    if receipt.get("task_sha256") != _N0_TASK_SHA256:
        raise N0Error("OJ_TASK_IDENTITY_MISMATCH:" + str(cid))
    if not receipt.get("gpu_identity") or not receipt.get("release_id") or not receipt.get("manifest_sha256") or not receipt.get("measurement_protocol"):
        raise N0Error("OJ_MEASUREMENT_IDENTITY_MISSING:" + str(cid))
    artifact_fields = ("timing", "profile", "diagnostics", "lineage")
    artifact_hashes = {}
    for field in artifact_fields:
        value = candidate.get(field)
        if receipt.get(field) != value:
            raise N0Error("OJ_RECEIPT_ARTIFACT_MISMATCH:" + str(cid) + ":" + field)
        artifact_hashes[field] = sha256_bytes(canonical_bytes(value))
    receipt_sha = receipt.get("receipt_sha256")
    receipt_body = {k: v for k, v in receipt.items() if k != "receipt_sha256"}
    if receipt_sha != sha256_bytes(canonical_bytes(receipt_body)):
        raise N0Error("OJ_RECEIPT_SHA_MISMATCH:" + str(cid))
    if not receipt.get("request_id") or not receipt.get("verdict") or not receipt.get("release_id") or not receipt.get("manifest_sha256") or not receipt.get("measurement_protocol"):
        raise N0Error("OJ_RECEIPT_IDENTITY_MISSING:" + str(cid))
    ledger = candidate.get("ledger_entry")
    if not isinstance(ledger, dict):
        raise N0Error("LEDGER_ENTRY_MISSING:" + str(cid))
    ledger_body = {k: v for k, v in ledger.items() if k != "ledger_entry_sha256"}
    if ledger.get("ledger_entry_sha256") != sha256_bytes(canonical_bytes(ledger_body)):
        raise N0Error("LEDGER_ENTRY_SHA_MISMATCH:" + str(cid))
    if (ledger.get("candidate_id") != cid or ledger.get("candidate_sha256") != source_sha
            or ledger.get("request_id") != receipt.get("request_id") or ledger.get("verdict") != receipt.get("verdict")):
        raise N0Error("LEDGER_OJ_IDENTITY_MISMATCH:" + str(cid))
    if ledger.get("receipt_sha256") != receipt_sha or ledger.get("artifact_sha256") != artifact_hashes:
        raise N0Error("LEDGER_ARTIFACT_IDENTITY_MISMATCH:" + str(cid))
    if _trusted_score(candidate) != ledger.get("score"):
        raise N0Error("LEDGER_OJ_SCORE_MISMATCH:" + str(cid))
    if receipt.get("verdict") == "VALID_MEASUREMENT" and _trusted_score(candidate) is None:
        raise N0Error("VALID_MEASUREMENT_SCORE_OR_CONTEXT_UNTRUSTED:" + str(cid))
    if receipt.get("verdict") != "VALID_MEASUREMENT" and receipt.get("score") is not None:
        raise N0Error("NON_MEASUREMENT_RECEIPT_MUST_NOT_HAVE_SCORE:" + str(cid))


def _trusted_score(candidate: dict[str, Any]) -> float | None:
    receipt = candidate["oj_receipt"]
    if receipt.get("verdict") != "VALID_MEASUREMENT" or receipt.get("task_sha256") != _N0_TASK_SHA256 or _measurement_context(candidate) is None:
        return None
    score = receipt.get("score")
    if not isinstance(score, (int, float)) or isinstance(score, bool) or not math.isfinite(float(score)):
        return None
    return float(score)


def successful_retrieval_footprint(events: Iterable[dict[str, Any]]) -> tuple[list[str], str | None]:
    """Build a minimal footprint only from completed, hashed successful calls."""
    fields: dict[str, set[str]] = {}
    knowledge: set[str] = set()
    last: str | None = None
    for event in events:
        if event.get("status") != "SUCCESS" or not event.get("information_acquired") or not event.get("result_sha256"):
            continue
        tool = event.get("tool_name")
        args = event.get("arguments") or {}
        if tool == "query_candidate_history":
            for cid in args.get("candidate_ids", []):
                fields.setdefault(cid, set()).update(args.get("fields", []))
                if args.get("fields"):
                    last = f"{cid}/{args['fields'][-1]}"
        elif tool == "query_hardware_skill":
            knowledge.add(str(event.get("query_key", args.get("query", ""))))
    lines = [f"{cid}: {', '.join(sorted(values))}" for cid, values in sorted(fields.items())]
    if knowledge:
        lines.append("Knowledge: " + ", ".join(sorted(knowledge)))
    return lines, last


def _sanitize_common_observation(value: Any) -> Any:
    """Remove Profile payloads from automatic context; G3 retrieves them by history query."""
    hidden = {"profile", "profile_feedback", "profile_status", "profile_receipt", "profile_receipt_sha256",
              "profile_metrics", "ncu", "ncu_metrics", "ncu_receipt", "ncu_report"}
    if isinstance(value, dict):
        return {k: _sanitize_common_observation(v) for k, v in value.items() if k.lower() not in hidden}
    if isinstance(value, list):
        return [_sanitize_common_observation(v) for v in value]
    return value


def build_history_brief(
    episode: dict[str, Any],
    *,
    candidate_budget: int,
    audit_events: Iterable[dict[str, Any]] = (),
    working_note: str | None = None,
) -> str:
    """Render EPISODE_STATE_V1; refuses an unverified or unsettled ledger."""
    if not isinstance(candidate_budget, int) or isinstance(candidate_budget, bool) or candidate_budget < 1:
        raise N0Error("CANDIDATE_BUDGET_MUST_BE_EXPLICIT_POSITIVE_INTEGER")
    if episode.get("unsettled_requests"):
        raise PendingStateError("EPISODE_HAS_UNSETTLED_REQUESTS")
    if episode.get("receipt_conflicts"):
        raise PendingStateError("EPISODE_HAS_RECEIPT_CONFLICTS")
    if episode.get("episode_id") is None or episode.get("task_id") != "KB_L1_40_LAYERNORM":
        raise N0Error("EPISODE_OR_TASK_ID_MISMATCH")
    workspace = episode.get("workspace_files")
    candidates = episode.get("candidates")
    if not isinstance(workspace, dict) or not isinstance(candidates, list):
        raise N0Error("BRIEF_SOURCE_STATE_INCOMPLETE")
    for candidate in candidates:
        _validate_candidate(candidate)
    ordered = sorted(candidates, key=lambda c: c["candidate_id"])
    expected = [f"C{i:04d}" for i in range(len(ordered))]
    ids = [c["candidate_id"] for c in ordered]
    if ids != expected:
        raise PendingStateError("CANDIDATE_SEQUENCE_GAP_OR_DUPLICATE")
    if ordered and ordered[0]["candidate_id"] != "C0000":
        raise PendingStateError("COMMON_SEED_C0000_MISSING")
    if not ordered:
        raise PendingStateError("COMMON_SEED_BASELINE_NOT_MEASURED")
    if ordered[0]["source_sha256"].upper() != _N0_SEED_SHA256:
        raise PendingStateError("COMMON_SEED_C0000_SHA_MISMATCH")
    seed_receipt = ordered[0]["oj_receipt"]
    seed_score_available = seed_receipt.get("verdict") == "VALID_MEASUREMENT" and _trusted_score(ordered[0]) is not None
    seed_infra_score_unavailable = (seed_receipt.get("verdict") == "OJ_INFRASTRUCTURE_HOLD_REFERENCE_CONTROL_MISMATCH"
                                    and seed_receipt.get("score") is None and _trusted_score(ordered[0]) is None)
    if not seed_score_available and not seed_infra_score_unavailable:
        raise PendingStateError("COMMON_SEED_BASELINE_NOT_VALID_OR_APPROVED_INFRASTRUCTURE_SCORE_UNAVAILABLE")
    used = sum(c["candidate_id"] != "C0000" for c in ordered)
    if used > candidate_budget:
        raise N0Error("CANDIDATE_BUDGET_EXCEEDED")
    current_id = episode.get("current_candidate_id")
    if not _safe_id(current_id or ""):
        raise N0Error("CURRENT_CANDIDATE_ID_INVALID")
    if ordered and current_id <= ordered[-1]["candidate_id"]:
        raise PendingStateError("CURRENT_CANDIDATE_NOT_AFTER_SETTLED_HISTORY")
    best = None
    for c in ordered:
        score = _trusted_score(c)
        if score is not None and (best is None or score > best["score"]):
            best = {"candidate_id": c["candidate_id"], "score": score, "sha256": c["source_sha256"][:12]}
    summaries = []
    for c in ordered:
        receipt = c["oj_receipt"]
        score = _trusted_score(c)
        raw_verdict = receipt.get("verdict", "UNKNOWN")
        seed_score_unavailable = c["candidate_id"] == "C0000" and raw_verdict == "OJ_INFRASTRUCTURE_HOLD_REFERENCE_CONTROL_MISMATCH" and score is None
        candidate_reference_hold = c["candidate_id"] != "C0000" and raw_verdict == "OJ_INFRASTRUCTURE_HOLD_REFERENCE_CONTROL_MISMATCH" and score is None
        verdict = ("C0000_BASELINE_SCORE_UNAVAILABLE_INFRA_HOLD" if seed_score_unavailable
                   else "MEASUREMENT_UNAVAILABLE_INFRA_HOLD" if candidate_reference_hold else raw_verdict)
        promotion = c["ledger_entry"].get("promotion", "UNKNOWN")
        if seed_score_unavailable:
            promotion = "BASELINE_SCORE_UNAVAILABLE"
        summaries.append({
            "candidate_id": c["candidate_id"],
            "source_sha256": c["source_sha256"],
            "verdict": verdict,
            "score": score,
            "promoted": promotion in {True, "PROMOTED", "YES"},
            "promotion_status": promotion,
            "evaluation_status": (verdict if seed_score_unavailable or candidate_reference_hold
                                  else receipt.get("evaluation_status", receipt.get("status", verdict)))
        })
    footprint, last_query = successful_retrieval_footprint(audit_events)
    note = working_note
    if note is not None and (not isinstance(note, str) or len(note) > 1000):
        raise N0Error("WORKING_NOTE_INVALID_OR_TOO_LONG")
    brief = {
        "format": "EPISODE_STATE_V1",
        "episode_id": episode["episode_id"],
        "task_id": episode["task_id"],
        "current_candidate_id": current_id,
        "workspace_sha256": _workspace_sha(workspace),
        "best_candidate": best or "NONE",
        "settled_candidates": summaries,
        "previously_retrieved": footprint,
        "last_history_query": last_query,
        "retrieval_footprint_note": "Records successful past returns only; it does not assert that old content is present in this invocation context.",
        "working_note": note,
        "working_note_sha256": sha256_text(note) if note is not None else None,
    }
    return json.dumps(brief, ensure_ascii=False, indent=2)


def build_first_request_messages(
    system_prompt: str,
    task_user_message: str,
    *,
    task_definition: str,
    candidate_abi: str,
    candidate_source: str,
    candidate_sha256: str,
    visible_observation: str,
    history_brief: str,
    previously_retrieved: str,
    working_note: str | None,
    tools: list[dict[str, Any]],
) -> list[dict[str, str]]:
    """Build complete mandatory context into the first User message without local length gates."""
    required = {
        "task_definition": task_definition,
        "candidate_abi": candidate_abi,
        "candidate_source": candidate_source,
        "visible_observation": visible_observation,
        "history_brief": history_brief,
    }
    if any(not isinstance(v, str) or not v for v in required.values()):
        raise N0Error("REQUIRED_INITIAL_CONTEXT_MISSING")
    if sha256_text(candidate_source) != candidate_sha256:
        raise N0Error("CURRENT_CANDIDATE_SHA_MISMATCH")
    try:
        observation_obj = json.loads(visible_observation)
    except (TypeError, json.JSONDecodeError) as exc:
        raise N0Error("VISIBLE_OBSERVATION_JSON_INVALID") from exc
    visible_observation = json.dumps(_sanitize_common_observation(observation_obj),
                                    ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    state = json.loads(history_brief)
    if state.get("format") != "EPISODE_STATE_V1":
        raise N0Error("HISTORY_BRIEF_FORMAT_INVALID")
    brief_fields={"episode_id","task_id","current_candidate_id","workspace_sha256","best_candidate",
                  "settled_candidates","previously_retrieved","last_history_query","retrieval_footprint_note",
                  "working_note","working_note_sha256"}
    if not brief_fields <= set(state):
        raise N0Error("HISTORY_BRIEF_FIELDS_INCOMPLETE")
    if not isinstance(state["settled_candidates"],list) or not isinstance(state["previously_retrieved"],list):
        raise N0Error("HISTORY_BRIEF_FIELDS_INVALID")
    if previously_retrieved != json.dumps({"previously_retrieved":state.get("previously_retrieved"),
            "last_history_query":state.get("last_history_query")},ensure_ascii=False,sort_keys=True,separators=(",",":")):
        raise N0Error("HISTORY_BRIEF_RETRIEVAL_FOOTPRINT_MISMATCH")
    if state.get("working_note") != working_note:
        raise N0Error("HISTORY_BRIEF_WORKING_NOTE_MISMATCH")
    tool_names = [x.get("function",{}).get("name") for x in tools]
    if tool_names != ["read_workspace_file","edit_candidate","query_candidate_history","query_hardware_skill"]:
        raise N0Error("N0_R5_TOOL_SET_INVALID")
    available_tools = [{"name":x["function"]["name"],"description":x["function"]["description"],
                        "parameters":x["function"]["parameters"]} for x in tools]
    dynamic = {
        "task_definition": task_definition,
        "candidate_abi": candidate_abi,
        "current_candidate": {"source": candidate_source, "sha256": candidate_sha256},
        "latest_complete_evaluation_observation": visible_observation,
        "history_brief": state,
    }
    content = task_user_message + "\n\nAVAILABLE_TOOLS_V1\n" + json.dumps(available_tools,ensure_ascii=False,sort_keys=True,separators=(",",":")) + "\n\nN0_MANDATORY_INVOCATION_CONTEXT_JSON\n" + json.dumps(
        dynamic, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    messages = [{"role": "system", "content": system_prompt}, {"role": "user", "content": content}]
    return messages


class InvocationTranscript:
    """Accumulates tool turns inside one outer invocation; next instance starts fresh."""
    def __init__(self, messages: list[dict[str, Any]]):
        self.messages = [dict(m) for m in messages]
        self.pending_tool_ids: list[str] = []
        self.seen_tool_call_ids: set[str] = set()
        self.ordered_tool_call_ids: list[str] = []
        self.closed = False

    def append_assistant(self, message: dict[str, Any]) -> None:
        if self.closed:
            raise N0Error("INVOCATION_ALREADY_CLOSED")
        calls = message.get("tool_calls") or []
        for call in calls:
            call_id = call.get("id")
            if not isinstance(call_id, str) or not call_id or call_id in self.seen_tool_call_ids:
                raise N0Error("TOOL_CALL_ID_MISSING_OR_REUSED")
            self.seen_tool_call_ids.add(call_id)
            self.ordered_tool_call_ids.append(call_id)
            self.pending_tool_ids.append(call_id)
        self.messages.append(dict(message))

    def append_tool_result(self, tool_call_id: str, content: str) -> None:
        if self.closed or tool_call_id not in self.pending_tool_ids:
            raise N0Error("TOOL_RESULT_WITHOUT_UNIQUE_PENDING_CALL")
        expected = self.pending_tool_ids[0]
        if tool_call_id != expected:
            raise N0Error("TOOL_RESULT_ORDER_MISMATCH")
        self.pending_tool_ids.pop(0)
        self.messages.append({"role":"tool","tool_call_id":tool_call_id,"content":content})

    def finish(self) -> dict[str, Any]:
        if self.pending_tool_ids:
            raise PendingStateError("UNANSWERED_TOOL_CALLS")
        self.closed = True
        return {"messages":self.messages,"tool_call_ids":list(self.ordered_tool_call_ids),"tool_results":sum(m.get("role")=="tool" for m in self.messages)}


def build_provider_request_body(messages: list[dict[str, Any]], *, model: str = "DeepSeek-V4-Pro-0813",
                                reasoning_effort: str = "high", tools: list[dict[str, Any]]) -> dict[str, Any]:
    """R19-style Chat Completions body: model, messages, reasoning, tools, auto only."""
    if not model or not reasoning_effort or not isinstance(tools, list):
        raise N0Error("PROVIDER_CONFIG_INCOMPLETE")
    body = {"model":model,"messages":messages,"reasoning_effort":reasoning_effort,"tools":tools,"tool_choice":"auto"}
    if any(k in body for k in ("max_tokens", "max_completion_tokens", "temperature", "top_p")):
        raise N0Error("UNFROZEN_PROVIDER_PARAMETER_PRESENT")
    return body


_HISTORY_FIELDS = {"source", "evaluation", "timing", "profile", "diagnostics", "lineage"}


class HistoryService:
    def __init__(self, episode_id: str, candidates: list[dict[str, Any]], *, max_result_bytes: int | None = None,
                 profile_whitelist: dict[str, Any] | None = None):
        self.episode_id = episode_id
        self.candidates = {c.get("candidate_id"): c for c in candidates}
        if max_result_bytes is not None and (not isinstance(max_result_bytes, int) or isinstance(max_result_bytes, bool) or max_result_bytes < 2048):
            raise N0Error("MAX_RESULT_BYTES_TOO_SMALL_FOR_PAGINATION_METADATA")
        self.max_result_bytes = max_result_bytes
        self.profile_whitelist = profile_whitelist or {"status": "NOT_FROZEN", "allowed_metrics": []}
        if len(self.candidates) != len(candidates):
            raise N0Error("DUPLICATE_CANDIDATE_ID")

    def snapshot_sha256(self) -> str:
        identities = []
        for cid, c in sorted(self.candidates.items()):
            _validate_candidate(c)
            normalized_record = {k:v for k,v in c.items() if k != "source"}
            identities.append({"candidate_id": cid, "source_sha256": c["source_sha256"],
                               "record_sha256": sha256_bytes(canonical_bytes(normalized_record))})
        return sha256_bytes(canonical_bytes({"episode_id": self.episode_id, "candidates": identities}))

    def _projection(self, cid: str, fields: list[str]) -> dict[str, Any]:
        c = self.candidates.get(cid)
        if c is None:
            raise N0Error("CANDIDATE_NOT_IN_CURRENT_EPISODE:" + cid)
        _validate_candidate(c)
        out: dict[str, Any] = {"candidate_id": cid}
        for field in fields:
            if field == "source":
                out[field] = c["source"]
                out["source_sha256"] = c["source_sha256"]
            elif field == "evaluation":
                receipt = c["oj_receipt"]
                out[field] = {
                    "terminal": receipt.get("terminal"),
                    "verdict": receipt.get("verdict"),
                    "score": receipt.get("score"),
                    "speedup": receipt.get("speedup"),
                    "candidate_sha256": receipt.get("candidate_sha256"),
                    "request_id": receipt.get("request_id"),
                    "receipt_sha256": receipt.get("receipt_sha256"),
                    "release_id": receipt.get("release_id"),
                    "manifest_sha256": receipt.get("manifest_sha256"),
                    "measurement_protocol": receipt.get("measurement_protocol"),
                    "task_sha256": receipt.get("task_sha256"),
                    "gpu_identity": receipt.get("gpu_identity"),
                    "promotion": c["ledger_entry"].get("promotion", "UNKNOWN"),
                }
            elif field == "timing":
                out[field] = c.get("timing", {"status": "UNKNOWN"})
            elif field == "profile":
                if self.profile_whitelist.get("status") != "FROZEN":
                    raise N0Error("PROFILE_WHITELIST_NOT_FROZEN")
                profile = c.get("profile", {})
                allowed = set(self.profile_whitelist.get("allowed_metrics", []))
                metrics = profile.get("metrics", {}) if isinstance(profile, dict) else {}
                out[field] = {"status": profile.get("status", "NONE"),
                              "shape": profile.get("shape"),
                              "measurement_protocol": profile.get("measurement_protocol"),
                              "metrics": {k: metrics[k] for k in sorted(allowed & set(metrics))}}
            elif field == "diagnostics":
                out[field] = c.get("diagnostics", {"status": "NONE"})
            elif field == "lineage":
                out[field] = c.get("lineage", {"parent_sha256": None, "version": "UNKNOWN"})
        return out

    def _page(self, payload: bytes, snapshot: str, object_sha: str, offset: int) -> dict[str, Any]:
        if self.max_result_bytes is None:
            raise N0Error("EXPLICIT_PAGINATION_NOT_CONFIGURED")
        if self.snapshot_sha256() != snapshot:
            raise SnapshotChangedError("SNAPSHOT_CHANGED")
        chunk_size = max(128, (self.max_result_bytes - 1800) * 3 // 4)
        total = len(payload)
        pages = max(1, (total + chunk_size - 1) // chunk_size)
        if offset < 0 or offset >= pages:
            raise N0Error("PAGE_OUT_OF_RANGE")
        start = offset * chunk_size
        chunk = payload[start:start + chunk_size]
        result = {
            "status": "PAGE", "episode_id": self.episode_id, "snapshot_sha256": snapshot,
            "object_sha256": object_sha, "offset": offset, "page": offset + 1, "total_pages": pages,
            "total_bytes": total, "byte_start": start, "byte_end_exclusive": start + len(chunk),
            "page_sha256": sha256_bytes(chunk), "data_base64": base64.b64encode(chunk).decode("ascii")
        }
        if offset + 1 < pages:
            result["next_page_token"] = base64.urlsafe_b64encode(canonical_bytes({"episode_id":self.episode_id,"snapshot_sha256":snapshot,"object_sha256":object_sha,"offset":offset+1})).decode("ascii").rstrip("=")
        return result

    def _paginate(self, obj: dict[str, Any], page_token: str | None = None) -> dict[str, Any]:
        snapshot = self.snapshot_sha256()
        payload = canonical_bytes(obj)
        objsha = sha256_bytes(payload)
        if page_token:
            try:
                token = json.loads(base64.urlsafe_b64decode(page_token.encode("ascii") + b"=" * (-len(page_token) % 4)))
            except Exception as exc:
                raise N0Error("PAGE_TOKEN_INVALID") from exc
            if (token.get("episode_id") != self.episode_id or token.get("snapshot_sha256") != snapshot
                    or token.get("object_sha256") != objsha or not isinstance(token.get("offset"), int)
                    or self.snapshot_sha256() != snapshot):
                raise SnapshotChangedError("PAGE_SNAPSHOT_OR_OBJECT_MISMATCH")
            return self._page(payload, snapshot, objsha, int(token.get("offset", -1)))
        direct = {"status": "OK", "snapshot_sha256": snapshot, "object_sha256": objsha, "total_bytes": len(payload), "data": obj}
        if self.max_result_bytes is None or (len(payload) <= self.max_result_bytes and len(canonical_bytes(direct)) <= self.max_result_bytes):
            return direct
        chunk_size = max(128, (self.max_result_bytes - 1800) * 3 // 4)
        pages = max(1, (len(payload) + chunk_size - 1) // chunk_size)
        first_token = base64.urlsafe_b64encode(canonical_bytes({"episode_id": self.episode_id, "snapshot_sha256": snapshot, "object_sha256": objsha, "offset": 0})).decode("ascii").rstrip("=")
        return {"status": "SIZE_LIMIT", "snapshot_sha256": snapshot, "object_sha256": objsha,
                "total_bytes": len(payload), "page_bytes": chunk_size, "total_pages": pages,
                "first_page_token": first_token, "reconstruction": "concatenate decoded data_base64 pages by byte_start; verify page_sha256 and final object_sha256"}

    def query_candidate_history(self, request: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(request, dict) or set(request) - {"candidate_ids", "fields", "page_token"}:
            raise N0Error("HISTORY_QUERY_PROPERTIES_INVALID")
        ids = request.get("candidate_ids")
        fields = request.get("fields")
        if not isinstance(ids, list) or not 1 <= len(ids) <= 4 or any(not _safe_id(x) for x in ids):
            raise N0Error("CANDIDATE_IDS_MUST_CONTAIN_1_TO_4_IDS")
        if len(set(ids)) != len(ids):
            raise N0Error("DUPLICATE_ID_WITHIN_REQUEST")
        if not isinstance(fields, list) or not fields or len(set(fields)) != len(fields) or not set(fields) <= _HISTORY_FIELDS:
            raise N0Error("HISTORY_FIELDS_INVALID")
        if request.get("page_token") is not None and not isinstance(request.get("page_token"), str):
            raise N0Error("PAGE_TOKEN_INVALID")
        obj = {"episode_id": self.episode_id, "candidate_ids": ids, "fields": fields,
               "records": [self._projection(cid, fields) for cid in ids]}
        return self._paginate(obj, request.get("page_token"))

class KnowledgeService:
    _RANK_FIELD_WEIGHTS = {
        "id": 4.0, "aliases": 4.0, "title": 3.4, "mechanism": 2.8,
        "claim": 2.5, "source": 1.6, "scope": 1.5, "applicability": 2.0,
        "limitations": 1.8,
    }

    def __init__(self, hardware_path: Path, *, max_result_bytes: int | None = None):
        self.hardware_path = Path(hardware_path)
        self.max_result_bytes = max_result_bytes
        self.hardware_sha256 = sha256_bytes(self.hardware_path.read_bytes())

    @staticmethod
    def _tokens(text: str) -> set[str]:
        split_camel = re.sub(r"([a-z])([A-Z])", r"\1 \2", text)
        raw = re.findall(r"[a-z0-9]+", split_camel.lower())
        tokens = {t for t in raw if len(t) > 1}
        for item in re.findall(r"[a-zA-Z0-9_.]+", text):
            if any(mark in item for mark in (".", "_")):
                compact = item.lower().strip("._")
                if len(compact) > 1:
                    tokens.add(compact)
        synonyms = {
            "sm_120": {"compute", "capability", "12.0", "blackwell"},
            "blackwell": {"compute", "capability", "12.0"},
            "float8": {"v8", "256-bit", "vector"},
            "v8": {"float8", "256-bit", "vector"},
            "l2": {"l2", "cache"},
            "sm_count": {"sm", "count", "multiprocessor"},
            "multiprocessor": {"sm", "count"},
            "bandwidth": {"throughput", "gb/s"},
            "gddr7": {"memory", "bandwidth"},
            "welford": {"variance", "reduction", "numerical"},
        }
        for token in tuple(tokens):
            tokens.update(synonyms.get(token, ()))
        return tokens

    _RETRIEVAL_STOPWORDS = {
        "cuda", "gpu", "kernel", "performance", "optimize", "optimized", "optimization",
        "how", "what", "why", "which", "the", "for", "with", "and", "from", "does",
        "using", "use", "support", "supported", "exact", "value", "values", "please",
        "example", "information", "query", "knowledge", "guide", "nvidia", "laptop",
    }

    @classmethod
    def _intent_guard(cls, query: str, identifier: str, item: dict[str, Any]) -> bool:
        raw_q = query.lower()
        q = re.sub(r"([a-z])([A-Z])", r"\1 \2", query).lower().replace("_", " ")
        fid = identifier.lower()
        title = " ".join(str(item.get(k, "")) for k in ("category", "title", "mechanism", "claim", "model_visible_text")).lower()
        exact = query.strip().lower() == identifier.lower()
        if exact:
            return True
        has_l2 = "l2" in cls._tokens(query) or "l2cache" in q.replace(" ", "")
        wants_policy = has_l2 and any(x in q for x in ("persisting l2", "l2 persistence", "persistent l2", "access policy", "evict", "policy window", "set aside", "no allocate", "no_allocate"))
        wants_capacity = has_l2 and not wants_policy and bool(re.search(r"(?:l2.{0,24}(?:size|capacity|bytes|mb|gb)|(?:size|capacity|bytes|mb|gb).{0,24}l2|rtx5060l\s+sku\s+l2)", q))
        wants_ptx_vector = any(x in q for x in ("ld global", "st global", "v8", "float8", "256 bit", "256-bit", "vector load", "vectorized load", "vectorized store", "vectorized global load", "vectorized global store"))
        wants_ptx_cache = any(x in q for x in (".nc", ".wt", ".cg", ".cs", " st.wt", " st.cg", " st.cs", " no allocate", "no_allocate", "evict_", "evict ", "cache operator", "cache modifier", "l1::", "l2::", "ldcs", "stcs"))
        wants_resource = any(x in q for x in ("occupancy", "register", "shared memory", "max threads", "resident block", "resident warp", "threads per sm", "blocks per sm", "warp count", "resource limit"))
        wants_max_threads_block = any(x in q for x in ("maxthreadsperblock", "max threads per block", "maximum threads per block", "threads in one block"))
        wants_threads_per_sm = any(x in q for x in ("maxthreadspermultiprocessor", "max threads per multiprocessor", "max threads per sm", "maximum resident threads per sm", "resident threads per sm"))
        wants_sm_count = any(x in q for x in ("multiprocessorcount", "number of sms", "sm count", "how many sms", "number of streaming multiprocessors"))
        wants_shared_optin = "cudadevattribmaxsharedmemoryperblockoptin" in q.replace(" ", "") or "maxsharedmemoryperblockoptin" in q.replace(" ", "")
        wants_partial_warp = any(x in q for x in ("partial warp", "partial-warp", "incomplete warp", "active lane mask", "shuffle mask", "shfl sync"))
        wants_code_example = any(x in q for x in ("code example", "implementation", "show code", "source code", "sample kernel"))
        wants_cc_only = any(x in q for x in ("compute capability", "sm_120", "cc 12.0")) and not wants_resource and not wants_ptx_vector
        wants_sm_count_only = any(x in q for x in ("sm count", "multiprocessor count", "number of sms", "sms count")) and not wants_resource
        wants_store_intrinsic = any(x in raw_q for x in ("__stwt", "__stcs", "__stcg", "__stwb")) or any(x in q for x in ("store cache hint", "write-through store intrinsic", "store intrinsic"))
        if "maxthreadsperblock" in q.replace(" ", ""):
            return "cc12.max_threads_per_block" in fid
        if wants_max_threads_block and not any(x in q for x in ("shared", "register", "occupancy", "bandwidth", "sm count", "l2 cache")):
            return "cc12.max_threads_per_block" in fid
        if "maxthreadspermultiprocessor" in q.replace(" ", "") or "cudaDevAttrMaxThreadsPerMultiProcessor".lower() in raw_q:
            return "cc12.max_threads_per_sm" in fid or "cuda.runtime.max_threads_per_sm" in fid
        if "cudadevattrmultiprocessorcount" in q.replace(" ", "") or "cudaDeviceProp.multiProcessorCount".lower() in raw_q:
            return "rtx5060l.sku.sm_count" in fid or "cuda.runtime.sm_count" in fid
        if "maxsharedmemoryperblockoptin" in q.replace(" ", ""):
            return "cc12.shared_memory" in fid or "cuda.runtime.max_shared_memory_per_block_optin" in fid
        if wants_partial_warp:
            return "cuda_v2.shuffle" in fid
        wants_double_words = any(x in raw_q for x in ("__double2hiint", "__double2loint", "__hiloint2double")) or ("double" in q and any(x in q for x in ("high", "low", "upper", "lower", "word")))
        wants_block_size_choice = "block size" in q and any(x in q for x in ("fastest", "always", "best"))
        wants_shared_max_per_block = "shared memory" in q and "per block" in q and any(x in q for x in ("max", "maximum", "limit"))
        wants_shared_per_sm = "shared memory" in q and ("per sm" in q or "per multiprocessor" in q)
        wants_measure_bandwidth = any(x in q for x in ("achieved bandwidth", "measured bandwidth", "measure bandwidth", "actual kernel bandwidth", "copy read write mix"))
        wants_ptx_vector_cache = wants_ptx_vector and wants_ptx_cache
        if wants_measure_bandwidth:
            return "cuda.effective_bandwidth_formula" in fid
        if wants_ptx_vector_cache:
            return any(x in fid for x in ("ptx.global.v8.f32", "ptx.global.cache_operators", "cuda.vector_alignment"))
        broad_multi_property = any(x in q for x in ("sm count", "multiprocessor", "memory bandwidth", "register", "l2 cache", "fp32", "fp64", "max threads per sm", "threads per sm"))
        if wants_shared_per_sm and not broad_multi_property:
            return fid == "cc12.shared_memory"
        if wants_policy:
            return any(x in fid for x in ("l2_access_policy", "l2_persistence", "l2_no_allocate", "ptx.global.cache_operators"))
        if wants_double_words:
            return "cuda.math_double_word_casts" in fid
        if wants_shared_max_per_block and not broad_multi_property:
            return fid == "cc12.shared_memory"
        if identifier == "cuda.store_cache_hint_intrinsics" and wants_ptx_cache and not ("__st" in q or "intrinsic" in q or "overload" in q):
            return False
        if any(x in q for x in ("how many bytes", "byte conversion", "convert kb")) and not any(x in q for x in ("l2", "shared memory", "global memory", "gpu", "cache", "capacity")):
            return False
        if wants_cc_only:
            return "compute_capability" in fid
        if wants_sm_count_only:
            return "sm_count" in fid
        if wants_capacity and not broad_multi_property:
            if "rtx5060l.sku.l2_capacity" in fid and not any(x in q for x in ("rtx 5060", "rtx5060", "laptop", "gb206")):
                return False
            if "l2_capacity" in fid:
                return True
            if "cuda.runtime.l2_cache_size" in fid:
                return True
            return False
        if wants_policy and "l2_capacity" in fid:
            return False
        if not wants_policy and has_l2 and any(x in fid for x in ("l2_access_policy", "l2_persistence", "l2_no_allocate")):
            return False
        if wants_ptx_vector and not wants_resource and fid.startswith("cc12."):
            return False
        if "ptx.global.v8.f32" in fid and not wants_ptx_vector:
            return False
        if "ptx.global.cache_operators" in fid and not wants_ptx_cache:
            return False
        if "ptx.global.l2_no_allocate_absent" in fid and not any(x in q for x in ("no allocate", "no_allocate", "l2::no_allocate")):
            return False
        if any(x in fid for x in ("cuda.l2_access_policy", "cuda.l2_persistence_limits")) and not wants_policy and not any(x in q for x in ("cudastreamattributeaccesspolicywindow", "cudaaccessproperty", "persistingl2")):
            return False
        return True

    @classmethod
    def _rank_retrieval_candidates(cls, query: str, candidates: list[tuple[str, dict[str, Any], dict[str, str]]], *, limit: int) -> list[dict[str, Any]]:
        """Fielded lexical ranker with a relevance floor and no low-score fillers."""
        qtokens = cls._tokens(query)
        core = qtokens - cls._RETRIEVAL_STOPWORDS
        if not core:
            core = qtokens
        docs = []
        document_frequency: dict[str, int] = {}
        strong_document_frequency: dict[str, int] = {}
        for identifier, item, fields in candidates:
            if not cls._intent_guard(query, identifier, item):
                continue
            field_tokens = {name: cls._tokens(value) for name, value in fields.items() if value}
            union = set().union(*field_tokens.values()) if field_tokens else set()
            for token in union:
                document_frequency[token] = document_frequency.get(token, 0) + 1
            strong_union = set().union(*(field_tokens.get(name, set()) for name in ("id", "aliases", "title", "mechanism")))
            for token in strong_union:
                strong_document_frequency[token] = strong_document_frequency.get(token, 0) + 1
            docs.append((identifier, item, fields, field_tokens, union))
        total_weight = sum(1.0 + math.log((len(docs) + 1) / (document_frequency.get(token, 0) + 1)) for token in core) or 1.0
        scored = []
        query_phrase = " ".join(sorted(qtokens))
        normalized_query = re.sub(r"[^a-z0-9]+", " ", query.lower()).strip()
        for identifier, item, fields, field_tokens, union in docs:
            matched = core & union
            strong = set().union(*(field_tokens.get(name, set()) for name in ("id", "aliases", "title", "mechanism")))
            strong_hits = core & strong
            alias_hits = core & field_tokens.get("aliases", set())
            rare_hits = {token for token in strong_hits if strong_document_frequency.get(token, 0) <= max(2, len(docs) * 0.04)}
            weighted_match = 0.0
            for token in matched:
                idf = 1.0 + math.log((len(docs) + 1) / (document_frequency.get(token, 0) + 1))
                boost = max((cls._RANK_FIELD_WEIGHTS.get(name, 1.0) for name in fields if token in field_tokens.get(name, set())), default=0.0)
                weighted_match += idf * boost
            normalized_fields = " ".join(re.sub(r"[^a-z0-9]+", " ", v.lower()).strip() for v in fields.values())
            phrase_match = bool(normalized_query and normalized_query in normalized_fields)
            identifier_match = query.strip().lower() == identifier.lower()
            coverage = sum(1.0 + math.log((len(docs) + 1) / (document_frequency.get(token, 0) + 1)) for token in matched) / total_weight
            score = weighted_match / max(1.0, math.sqrt(len(core)))
            score += 12.0 * len(rare_hits)
            score += 4.0 * len(alias_hits)
            if phrase_match:
                score += 5.0
            if identifier_match:
                score += 1000.0
            minimum_hits = 1 if len(core) <= 2 else 2
            eligible = identifier_match or bool(rare_hits) or bool(alias_hits) or (len(strong_hits) >= minimum_hits and coverage >= (0.22 if len(core) <= 3 else 0.16))
            if eligible:
                scored.append((score, coverage, identifier, item, union, rare_hits, alias_hits))
        if not scored:
            return []
        scored.sort(key=lambda row: (-row[0], -row[1], row[2]))
        best = scored[0][0]
        floor = max(1.35, best * 0.25)
        selected = []
        selected_tokens = []
        for score, coverage, identifier, item, content_tokens, rare_hits, alias_hits in scored:
            specific_alias = {token for token in alias_hits if any(char.isdigit() for char in token) or token in {"welford", "roofline", "shuffle", "atomicadd", "cooperative_groups", "syncthreads"}}
            if score < floor and not rare_hits and not specific_alias:
                continue
            # Collapse only near-identical answer text; distinct complementary methods remain separate.
            answer_tokens = cls._tokens(" ".join(str(item.get(k, "")) for k in ("claim", "model_visible_text", "title", "mechanism", "source")))
            if any(len(answer_tokens & prior) >= 7 and len(answer_tokens & prior) / max(1, len(answer_tokens | prior)) >= 0.88 for prior in selected_tokens):
                continue
            selected.append(item)
            selected_tokens.append(answer_tokens)
            if len(selected) >= limit:
                break
        return selected

    @staticmethod
    def _paginate_static(obj: dict[str, Any], *, max_bytes: int | None, page_token: str | None, scope: str, snapshot: str) -> dict[str, Any]:
        payload = canonical_bytes(obj); object_sha = sha256_bytes(payload)
        if page_token:
            if max_bytes is None:
                raise N0Error("EXPLICIT_PAGINATION_NOT_CONFIGURED")
            try:
                token = json.loads(base64.urlsafe_b64decode(page_token.encode("ascii") + b"=" * (-len(page_token) % 4)))
            except Exception as exc:
                raise N0Error("PAGE_TOKEN_INVALID") from exc
            if token.get("scope") != scope or token.get("snapshot_sha256") != snapshot or token.get("object_sha256") != object_sha:
                raise SnapshotChangedError("KNOWLEDGE_SNAPSHOT_OR_OBJECT_MISMATCH")
            if not isinstance(token.get("offset"), int) or token.get("offset") < 0:
                raise N0Error("PAGE_TOKEN_OFFSET_INVALID")
            chunk_size = max(128, (max_bytes - 1800) * 3 // 4)
            start = token["offset"] * chunk_size
            chunk = payload[start:start + chunk_size]
            pages = max(1, (len(payload) + chunk_size - 1) // chunk_size)
            if start >= len(payload):
                raise N0Error("PAGE_OUT_OF_RANGE")
            result = {"status":"PAGE","episode_id":scope,"snapshot_sha256":snapshot,"object_sha256":object_sha,
                    "offset":token["offset"],"page":token["offset"]+1,"total_pages":pages,"total_bytes":len(payload),
                    "byte_start":start,"byte_end_exclusive":start+len(chunk),"page_sha256":sha256_bytes(chunk),
                    "data_base64":base64.b64encode(chunk).decode("ascii")}
            if token["offset"] + 1 < pages:
                result["next_page_token"] = base64.urlsafe_b64encode(canonical_bytes({"scope":scope,"snapshot_sha256":snapshot,"object_sha256":object_sha,"offset":token["offset"]+1})).decode("ascii").rstrip("=")
            return result
        direct = {"status":"OK","snapshot_sha256":snapshot,"object_sha256":object_sha,"total_bytes":len(payload),"data":obj}
        if max_bytes is None or (len(payload) <= max_bytes and len(canonical_bytes(direct)) <= max_bytes):
            return direct
        chunk_size = max(128, (max_bytes - 1800) * 3 // 4); pages = max(1, (len(payload) + chunk_size - 1) // chunk_size)
        token = base64.urlsafe_b64encode(canonical_bytes({"scope":scope,"snapshot_sha256":snapshot,"object_sha256":object_sha,"offset":0})).decode("ascii").rstrip("=")
        return {"status":"SIZE_LIMIT","snapshot_sha256":snapshot,"object_sha256":object_sha,"total_bytes":len(payload),"page_bytes":chunk_size,"total_pages":pages,"first_page_token":token}

    def query_hardware_skill(self, request: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(request, dict) or set(request) - {"query", "page_token"}:
            raise N0Error("HARDWARE_QUERY_PROPERTIES_INVALID")
        if request.get("page_token") is not None and not isinstance(request.get("page_token"), str):
            raise N0Error("PAGE_TOKEN_INVALID")
        query = request.get("query")
        if not isinstance(query, str) or not query.strip() or len(query) > 500:
            raise N0Error("HARDWARE_QUERY_INVALID")
        doc = json.loads(self.hardware_path.read_text(encoding="utf-8"))
        facts = [f for f in doc.get("facts", []) if f.get("confidence_status") == "SUPPORTED"]
        candidates = []
        for fact in facts:
            fid = str(fact.get("fact_id", ""))
            fields = {
                "id": fid,
                "aliases": " ".join(map(str, fact.get("aliases", []))) if isinstance(fact.get("aliases"), list) else str(fact.get("aliases", "")),
                "title": str(fact.get("category", "")),
                "claim": " ".join(str(fact.get(k, "")) for k in ("claim", "model_visible_text")),
                "scope": " ".join(str(fact.get(k, "")) for k in ("scope", "target_hardware")),
                "limitations": str(fact.get("limitations", "")),
            }
            candidates.append((fid, fact, fields))
        selected = self._rank_retrieval_candidates(query, candidates, limit=5)
        results = []
        for fact in selected:
            fid = str(fact.get("fact_id", ""))
            raw = canonical_bytes(fact)
            results.append({"knowledge_id": fid, "fact": fact, "content_sha256": sha256_bytes(raw)})
        obj = {"knowledge_source_sha256": self.hardware_sha256, "query": query, "status": "OK" if results else "NO_RELEVANT_FACT", "results": results}
        return self._paginate_static(obj, max_bytes=self.max_result_bytes, page_token=request.get("page_token"), scope="hardware", snapshot=self.hardware_sha256)

class AuditJournal:
    """Append-only evidence journal; refuses to overwrite raw request identities."""
    def __init__(self, episode_root: Path):
        self.root = Path(episode_root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.events_path = self.root / "information_events.jsonl"

    def events(self) -> list[dict[str, Any]]:
        if not self.events_path.exists():
            return []
        return [json.loads(line) for line in self.events_path.read_text(encoding="utf-8").splitlines() if line.strip()]

    def cached_tool_result(self, *, tool_call_id: str, tool_name: str, arguments: dict[str, Any]) -> tuple[bool, Any]:
        """Recover a previous result by stable call ID; never re-execute ambiguous side effects."""
        matches=[x for x in self.events() if x.get("tool_call_id")==tool_call_id and x.get("tool_name")]
        if not matches: return False,None
        prior=matches[0]
        if prior.get("tool_name")!=tool_name or prior.get("arguments")!=arguments:
            raise N0Error("TOOL_CALL_ID_REUSED_WITH_DIFFERENT_OPERATION")
        raw_path=Path(prior.get("raw_exchange_path",""))
        if not raw_path.is_file(): raise N0Error("TOOL_CALL_RESULT_MISSING_DO_NOT_REEXECUTE")
        raw=raw_path.read_bytes()
        if sha256_bytes(raw)!=prior.get("raw_exchange_sha256"):
            raise N0Error("TOOL_CALL_RAW_RESULT_HASH_MISMATCH_DO_NOT_REEXECUTE")
        exchange=json.loads(raw.decode("utf-8"))
        if (exchange.get("tool_call_id")!=tool_call_id or exchange.get("tool_name")!=tool_name
                or exchange.get("arguments")!=arguments):
            raise N0Error("TOOL_CALL_RAW_RESULT_IDENTITY_MISMATCH_DO_NOT_REEXECUTE")
        self.append({"event_type":"DUPLICATE_TOOL_CALL_RECOVERED_FROM_RECEIPT","tool_call_id":tool_call_id,
                     "tool_name":tool_name,"raw_exchange_sha256":prior["raw_exchange_sha256"],
                     "raw_exchange_path":str(raw_path)})
        return True,exchange.get("result")

    def append(self, event: dict[str, Any]) -> None:
        event = dict(event)
        event.setdefault("timestamp_utc", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
        line = canonical_bytes(event) + b"\n"
        with self.events_path.open("ab") as f:
            f.write(line); f.flush(); os.fsync(f.fileno())

    def record_tool(self, *, episode_id: str, invocation_id: str, turn_id: str, request_id: str | None,
                    response_id: str | None, tool_call_id: str, tool_name: str, arguments: dict[str, Any],
                    status: str, result: Any, visible_content_sha256s: list[str] | None = None,
                    information_acquired: bool | None = None,
                    estimated_tokens: int | None = None, actual_tokens: int | None = None,
                    candidate_action: str | None = None) -> dict[str, Any]:
        old = self.events()
        call_fingerprint = sha256_bytes(canonical_bytes({"tool_name":tool_name,"arguments":arguments}))
        acquired = (status == "SUCCESS") if information_acquired is None else bool(information_acquired)
        prior_same_id = next((x for x in old if x.get("tool_call_id") == tool_call_id), None)
        prior_requests = [x for x in old if x.get("call_fingerprint") == call_fingerprint]
        prior_success = [x for x in prior_requests if x.get("status") == "SUCCESS" and x.get("information_acquired")]
        prior_failed = [x for x in prior_requests if x.get("status") != "SUCCESS" or not x.get("information_acquired")]
        result_bytes = canonical_bytes(result)
        result_sha = sha256_bytes(result_bytes)
        prior_content = any(x.get("result_sha256") == result_sha and x.get("information_acquired") for x in old)
        visible = set(visible_content_sha256s or [])
        identity_complete = bool(episode_id and invocation_id and turn_id and request_id and response_id and tool_call_id)
        result_status = result.get("status") if isinstance(result, dict) else None
        if isinstance(result, dict) and isinstance(result.get("data"), dict):
            result_status = result["data"].get("status", result_status)
        if not identity_complete:
            classification = "EVIDENCE_INSUFFICIENT"
        elif prior_same_id:
            classification = "SAME_TOOL_CALL_ID_REUSE_REQUIRES_REPLAY_RECONCILIATION"
        elif status != "SUCCESS":
            classification = "FAILURE_RETRY" if prior_failed else "UNKNOWN_FAILURE_OR_INCOMPLETE"
        elif not acquired:
            classification = "SUCCESSFUL_TOOL_NO_INFORMATION_RETURNED"
        elif prior_success:
            if prior_success[-1].get("result_sha256") != result_sha:
                classification = "CONTENT_CHANGED_RELOAD"
            elif any(x.get("invocation_id") == invocation_id for x in prior_success):
                classification = "SAME_INVOCATION_REPEAT"
            else:
                classification = "CROSS_INVOCATION_RETRIEVAL"
        elif prior_content and status == "SUCCESS":
            classification = "CONTENT_RETRIEVAL_REPEAT"
        elif status == "SUCCESS":
            classification = "FIRST_INFORMATION_ACQUISITION"
        else:
            classification = "UNKNOWN_FAILURE_OR_INCOMPLETE"
        raw_dir = self.root / "tool_exchanges"
        raw_dir.mkdir(parents=True, exist_ok=True)
        raw_path = raw_dir / f"{len(old)+1:08d}_{call_fingerprint[:16]}.json"
        raw_exchange = {"tool_name":tool_name,"tool_call_id":tool_call_id,"arguments":arguments,"status":status,"result":result}
        raw_bytes = canonical_bytes(raw_exchange)
        with raw_path.open("xb") as f:
            f.write(raw_bytes); f.flush(); os.fsync(f.fileno())
        event = {
            "episode_id":episode_id,"invocation_id":invocation_id,"turn_id":turn_id,
            "provider_request_id":request_id,"provider_response_id":response_id,"tool_call_id":tool_call_id,
            "tool_name":tool_name,"arguments":arguments,"status":status,"result_sha256":result_sha,
            "information_acquired":acquired,"result_status":result_status,
            "result_bytes":len(result_bytes),
            "estimated_tokens":estimated_tokens if estimated_tokens is not None else "NOT_COMPUTED",
            "token_estimate_method":"PROVIDER_USAGE" if actual_tokens is not None else "NOT_COMPUTED",
            "actual_tokens":actual_tokens if actual_tokens is not None else "UNKNOWN",
            "previously_requested":bool(prior_requests),"content_in_current_provider_context":("UNKNOWN" if visible_content_sha256s is None else result_sha in visible),
            "content_seen_in_prior_turn":prior_content,"classification":classification,
            "call_fingerprint":call_fingerprint,"candidate_action_after_tool":candidate_action,
            "raw_exchange_path":str(raw_path),"raw_exchange_sha256":sha256_bytes(raw_bytes)
        }
        self.append(event)
        return event

    def record_provider_exchange(self, *, request_id: str, attempt_id: str, response_id: str | None,
                                 request_body: bytes, response_body: bytes | None, dispatched: bool,
                                 metadata: dict[str, Any] | None = None) -> dict[str, Any]:
        if not re.fullmatch(r"[A-Za-z0-9_.-]{1,160}", request_id):
            raise N0Error("REQUEST_ID_INVALID")
        base = self.root / "provider_raw"; req_dir = base / "requests"; resp_dir = base / "responses"
        req_dir.mkdir(parents=True, exist_ok=True); resp_dir.mkdir(parents=True, exist_ok=True)
        request_path = req_dir / (request_id + ".body")
        if request_path.exists():
            if request_path.read_bytes() != request_body: raise N0Error("REQUEST_ID_RAW_BODY_CONFLICT")
        else:
            with request_path.open("xb") as f:
                f.write(request_body); f.flush(); os.fsync(f.fileno())
        response_path = None
        response_sha = None
        if response_body is not None:
            error_status=(metadata or {}).get("status") in {"PROVIDER_CONTEXT_WINDOW_EXCEEDED","PROVIDER_HTTP_ERROR"}
            if response_id is not None:
                if not re.fullmatch(r"[A-Za-z0-9_.-]{1,160}", response_id):
                    raise N0Error("RESPONSE_ID_INVALID")
                response_path = resp_dir / (response_id + ".body")
            elif error_status:
                response_path = resp_dir / (request_id + ".http-error.body")
            else:
                raise N0Error("RESPONSE_ID_REQUIRED_FOR_RESPONSE_BODY")
            if response_path.exists():
                if response_path.read_bytes() != response_body: raise N0Error("RESPONSE_ID_RAW_BODY_CONFLICT")
            else:
                with response_path.open("xb") as f:
                    f.write(response_body); f.flush(); os.fsync(f.fileno())
            response_sha = sha256_bytes(response_body)
        receipt = {"request_id":request_id,"attempt_id":attempt_id,"response_id":response_id,"dispatched":bool(dispatched),
                  "request_body_path":str(request_path),"request_body_sha256":sha256_bytes(request_body),
                  "response_body_path":str(response_path) if response_path else None,"response_body_sha256":response_sha,
                  "response_bytes":len(response_body) if response_body is not None else None,"metadata":metadata or {}}
        receipt_path = base / "exchanges" / (request_id + ".json")
        receipt_path.parent.mkdir(parents=True, exist_ok=True)
        receipt_bytes=canonical_bytes(receipt)
        if receipt_path.exists():
            if receipt_path.read_bytes() != receipt_bytes: raise N0Error("PROVIDER_EXCHANGE_RECEIPT_CONFLICT")
        else:
            with receipt_path.open("xb") as f:
                f.write(receipt_bytes); f.flush(); os.fsync(f.fileno())
        return receipt

    def record_followup_action(self, tool_call_id: str, action: dict[str, Any], *, invocation_id: str | None = None) -> None:
        self.append({"event_type":"TOOL_FOLLOWUP_ACTION","tool_call_id":tool_call_id,
                     "invocation_id":invocation_id,"action":action,
                     "action_sha256":sha256_bytes(canonical_bytes(action))})

    def record_invalid_tool_call(self, *, episode_id: str, invocation_id: str, turn_id: str,
                                 request_id: str, response_id: str, tool_call_id: str,
                                 tool_name: str, raw_arguments: Any, error: str) -> dict[str, Any]:
        return self.record_tool(episode_id=episode_id,invocation_id=invocation_id,turn_id=turn_id,
            request_id=request_id,response_id=response_id,tool_call_id=tool_call_id,tool_name=tool_name,
            arguments={"raw_provider_arguments":raw_arguments},status="FAILURE",
            result={"status":"ERROR","error_code":"TOOL_ARGUMENTS_INVALID","detail":error},
            information_acquired=False)


class WorkspaceTools:
    ALLOWLIST = {"task_definition.json", "candidate.cu", "candidate_abi.json", "visible_observation.json"}

    def __init__(self, workspace_root: Path, *, audit: AuditJournal | None = None):
        self.root = Path(workspace_root).resolve()
        self.audit = audit
        self._read_this_invocation: set[str] = set()
        self._invocation_id: str | None = None
        self._edit_index = 0

    def begin_invocation(self, invocation_id: str | None = None) -> None:
        self._read_this_invocation.clear()
        if invocation_id is not None and not re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", invocation_id):
            raise N0Error("INVOCATION_ID_INVALID")
        self._invocation_id = invocation_id
        self._edit_index = 0
        if invocation_id is not None and self.audit is not None:
            versions = [int(event.get("edit_version", 0)) for event in self.audit.events()
                        if event.get("event_type") == "WORKSPACE_EDIT_COMMITTED"
                        and event.get("invocation_id") == invocation_id]
            self._edit_index = max(versions, default=0)

    def required_context(self) -> dict[str, str]:
        """Return controller-injected task, ABI, and candidate without generating tool calls."""
        names = ("task_definition.json", "candidate_abi.json", "candidate.cu", "visible_observation.json")
        result = {}
        for name in names:
            path = (self.root / name).resolve()
            if path.parent != self.root or not path.is_file():
                raise N0Error("REQUIRED_WORKSPACE_CONTEXT_MISSING:" + name)
            raw = path.read_bytes()
            try:
                result[name] = raw.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise N0Error("REQUIRED_WORKSPACE_CONTEXT_NOT_UTF8:" + name) from exc
        from .cuda_contract import validate_candidate_source
        validate_candidate_source(result["candidate.cu"])
        result["candidate_sha256"] = sha256_bytes(result["candidate.cu"].encode("utf-8"))
        if (json.loads(result["task_definition.json"]).get("task_sha256", "").upper() != _N0_TASK_SHA256
                or sha256_text(result["task_definition.json"]).upper() != _N0_TASK_DEFINITION_SHA256):
            raise N0Error("TASK_DEFINITION_SHA_MISMATCH")
        abi=json.loads(result["candidate_abi.json"])
        if (abi.get("abi_id")!="N0_LAYERNORM_EXTENSION_ABI_V1"
                or abi.get("compiler_contract_sha256")!=_N0_COMPILER_CONTRACT_SHA256):
            raise N0Error("CANDIDATE_ABI_OR_COMPILER_CONTRACT_IDENTITY_MISMATCH")
        try:
            json.loads(result["candidate_abi.json"])
            json.loads(result["visible_observation.json"])
        except Exception as exc:
            raise N0Error("ABI_OR_OBSERVATION_JSON_INVALID") from exc
        for name in self.ALLOWLIST:
            path=self.root/name
            if path.exists() and name not in names:
                try: result[name]=path.read_bytes().decode("utf-8")
                except UnicodeDecodeError as exc: raise N0Error("WORKSPACE_FILE_NOT_UTF8:"+name) from exc
        return result

    def read_workspace_file(self, path: str) -> dict[str, Any]:
        if path not in self.ALLOWLIST:
            raise N0Error("WORKSPACE_FILE_NOT_ALLOWLISTED")
        p = (self.root / path).resolve()
        if p.parent != self.root:
            raise N0Error("WORKSPACE_PATH_ESCAPE")
        raw = p.read_bytes()
        text = raw.decode("utf-8")
        self._read_this_invocation.add(path)
        return {"path":path,"content":text,"sha256":sha256_bytes(raw),"bytes":len(raw)}

    def edit_candidate(self, *, expected_sha256: str, source: str, tool_call_id: str | None = None) -> dict[str, Any]:
        if len(source) > 200000 or not re.fullmatch(r"[0-9a-f]{64}", expected_sha256):
            raise N0Error("EDIT_ARGUMENT_INVALID")
        if self._invocation_id is None:
            raise N0Error("EDIT_INVOCATION_ID_REQUIRED")
        from .cuda_contract import validate_candidate_source
        validate_candidate_source(source)
        target = self.root / "candidate.cu"
        new_bytes = source.encode("utf-8")
        after_sha=sha256_bytes(new_bytes)
        version_dir = self.root / ".n0_edits" / self._invocation_id
        version_dir.mkdir(parents=True, exist_ok=True)
        existing_event=None
        if self.audit is not None and tool_call_id is not None:
            existing_event=next((event for event in self.audit.events()
                if event.get("event_type")=="WORKSPACE_EDIT_COMMITTED"
                and event.get("invocation_id")==self._invocation_id
                and event.get("tool_call_id")==tool_call_id),None)
        version_path=None
        edit_number=None
        if existing_event is not None:
            if existing_event.get("before_sha256")!=expected_sha256 or existing_event.get("after_sha256")!=after_sha:
                raise N0Error("EDIT_TOOL_CALL_ID_CONFLICT")
            version_path=(self.root / existing_event.get("artifact","")).resolve()
            if version_path.parent!=version_dir.resolve() or not version_path.is_file() or sha256_bytes(version_path.read_bytes())!=after_sha:
                raise N0Error("EDIT_AUDIT_VERSION_ARTIFACT_MISMATCH")
            edit_number=int(existing_event["edit_version"])
        else:
            edit_number=max(self._edit_index+1,1)
            version_path=version_dir/f"edit-{edit_number:04d}-{after_sha}.cu"
            before=target.read_bytes()
            current_sha=sha256_bytes(before)
            if version_path.exists():
                if version_path.read_bytes()!=new_bytes:
                    raise N0Error("EDIT_VERSION_CONTENT_CONFLICT")
                if current_sha==expected_sha256:
                    temp=self.root/(".candidate."+str(os.getpid())+"."+str(time.time_ns())+".tmp")
                    with temp.open("xb") as f:
                        f.write(new_bytes); f.flush(); os.fsync(f.fileno())
                    os.replace(temp,target)
                elif current_sha!=after_sha:
                    raise N0Error("WORKSPACE_CANDIDATE_SHA_CONFLICT")
            else:
                if current_sha!=expected_sha256:
                    raise N0Error("WORKSPACE_CANDIDATE_SHA_CONFLICT")
                with version_path.open("xb") as f:
                    f.write(new_bytes); f.flush(); os.fsync(f.fileno())
                temp=self.root/(".candidate."+str(os.getpid())+"."+str(time.time_ns())+".tmp")
                with temp.open("xb") as f:
                    f.write(new_bytes); f.flush(); os.fsync(f.fileno())
                os.replace(temp,target)
        self._edit_index=max(self._edit_index,edit_number or 0)
        result = {"status":"EDITED","path":"candidate.cu","before_sha256":expected_sha256,
                  "after_sha256":after_sha,"bytes":len(new_bytes),
                  "edit_version":edit_number,"version_artifact":str(version_path.relative_to(self.root).as_posix())}
        if self.audit is not None:
            if existing_event is None:
                self.audit.append({"event_type":"WORKSPACE_EDIT_COMMITTED","invocation_id":self._invocation_id,
                    "tool_call_id":tool_call_id,"edit_version":edit_number,"before_sha256":expected_sha256,"after_sha256":after_sha,
                    "bytes":len(new_bytes),"artifact":result["version_artifact"]})
        return result


class N0ToolServer:
    def __init__(self, history: HistoryService, knowledge: KnowledgeService, audit: AuditJournal,
                 workspace: WorkspaceTools | None = None):
        self.history = history; self.knowledge = knowledge; self.audit = audit; self.workspace = workspace

    def call(self, name: str, arguments: dict[str, Any], *, episode_id: str, invocation_id: str,
             turn_id: str, request_id: str | None, response_id: str | None, tool_call_id: str,
             visible_content_sha256s: list[str] | None = None) -> dict[str, Any]:
        found,cached=self.audit.cached_tool_result(tool_call_id=tool_call_id,tool_name=name,arguments=arguments)
        if found:
            return cached
        try:
            contracts={
                "read_workspace_file":({"path"},{"path"}),
                "edit_candidate":({"expected_sha256","source"},{"expected_sha256","source"}),
                "query_candidate_history":({"candidate_ids","fields"},{"candidate_ids","fields","page_token"}),
                "query_hardware_skill":({"query"},{"query","page_token"}),
            }
            if name in contracts:
                required,allowed=contracts[name]
                if not isinstance(arguments,dict) or not required <= set(arguments) or set(arguments)-allowed:
                    raise N0Error("TOOL_ARGUMENTS_SCHEMA_INVALID")
            if name == "query_candidate_history":
                result = self.history.query_candidate_history(arguments)
            elif name == "query_hardware_skill":
                result = self.knowledge.query_hardware_skill(arguments)
            elif name == "read_workspace_file":
                if self.workspace is None:
                    raise N0Error("WORKSPACE_NOT_BOUND")
                result = self.workspace.read_workspace_file(arguments.get("path", ""))
            elif name == "edit_candidate":
                if self.workspace is None:
                    raise N0Error("WORKSPACE_NOT_BOUND")
                result = self.workspace.edit_candidate(expected_sha256=arguments.get("expected_sha256", ""), source=arguments.get("source", ""),tool_call_id=tool_call_id)
            else:
                raise N0Error("TOOL_NOT_AVAILABLE")
            tool_success = result.get("status") in {"OK", "PAGE", "SIZE_LIMIT", "NO_RELEVANT_FACT", "EDITED"}
            tool_success = tool_success or (name == "read_workspace_file" and "content" in result)
            status = "SUCCESS" if tool_success else "FAILURE"
        except Exception as exc:
            result = {"status":"ERROR","error_code":str(exc)}
            status = "FAILURE"
        data_returned = False
        if status == "SUCCESS":
            if name in {"query_candidate_history", "query_hardware_skill"}:
                if result.get("status") == "PAGE":
                    data_returned = bool(result.get("data_base64"))
                elif result.get("status") == "OK":
                    payload = result.get("data", {})
                    if name == "query_candidate_history":
                        data_returned = bool(payload.get("records"))
                    else:
                        data_returned = payload.get("status") == "OK" and bool(payload.get("results"))
            elif name == "read_workspace_file":
                data_returned = "content" in result
        self.audit.record_tool(episode_id=episode_id,invocation_id=invocation_id,turn_id=turn_id,
            request_id=request_id,response_id=response_id,tool_call_id=tool_call_id,tool_name=name,
            arguments=arguments,status=status,result=result,visible_content_sha256s=visible_content_sha256s,
            information_acquired=data_returned)
        return result
