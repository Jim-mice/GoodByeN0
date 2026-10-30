"""Offline-first policy core for the N0 H/P factorial protocol.

This module is an overlay derived from staged Harness R7.4. It is not wired
into any active Run. Internal treatment metadata and audit details must never
be copied into model-visible messages.
"""
from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable


class ProtocolError(ValueError):
    pass


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest().upper()


def digest_json(value: Any) -> str:
    return sha256(canonical_bytes(value))


LEVELS = ("0", "0.5", "1")
GROUPS = tuple(f"H{h.replace('.', '')}_P{p.replace('.', '')}" for h in LEVELS for p in LEVELS)
CORE_TOOLS = ("read_workspace_file", "edit_candidate", "query_candidate_history")
HARDWARE_TOOL = "query_hardware_skill"
PROFILE_FIELD = "profile"
NO_INFORMATION_RESULT = {
    "status": "NO_RELEVANT_INFORMATION",
    "message": "No matching result is available for this query.",
    "results": [],
}
MAX_FORMAL_CANDIDATES = 30


@dataclass(frozen=True)
class Treatment:
    hardware: str
    profile: str

    def __post_init__(self) -> None:
        if self.hardware not in LEVELS or self.profile not in LEVELS:
            raise ProtocolError("TREATMENT_LEVEL_INVALID")

    @property
    def group_id_internal(self) -> str:
        return f"H{self.hardware.replace('.', '')}_P{self.profile.replace('.', '')}"


def _clone(value: Any) -> Any:
    return json.loads(json.dumps(value, ensure_ascii=False))


def treatment_tools(base_tools: list[dict[str, Any]], treatment: Treatment) -> list[dict[str, Any]]:
    """Return the exact model-visible tool set for one factorial cell.

    Expert is absent. Profile visibility is controlled only by Candidate
    History fields; there is no separate Profile tool.
    """
    by_name = {t.get("function", {}).get("name"): t for t in base_tools}
    if not all(name in by_name for name in CORE_TOOLS):
        raise ProtocolError("CORE_TOOL_SCHEMA_MISSING")
    if HARDWARE_TOOL not in by_name:
        raise ProtocolError("HARDWARE_TOOL_SCHEMA_MISSING")
    result = [_clone(by_name[name]) for name in CORE_TOOLS]
    # The inherited R7.4 edit description exposed the externally enforced
    # Candidate limit. Replace it with the same operation contract without
    # mentioning the hidden controller cutoff.
    next(x for x in result if x["function"]["name"] == "edit_candidate")["function"]["description"] = (
        "Create an immutable working version of candidate.cu. Provide the full SHA256 "
        "of the current source and the complete replacement CUDA C++ source. Returns "
        "the saved version identity and before/after SHA256 values. A final CONTINUE "
        "submits the current source; STOP leaves intermediate edits unevaluated."
    )
    history = result[2]
    history["function"]["description"] = (
        "Query Candidate records from this Episode. Select source, evaluation, timing, "
        "diagnostics or lineage fields; profile is available only when assigned. "
        "Results contain only requested records and "
        "their identity metadata; repeated queries are allowed."
    )
    props = history.get("function", {}).get("parameters", {}).get("properties", {})
    fields = props.get("fields", {})
    if isinstance(fields, dict) and isinstance(fields.get("items"), dict):
        enum = fields["items"].get("enum")
        if isinstance(enum, list):
            fields["items"]["enum"] = ([x for x in enum if x != PROFILE_FIELD]
                                         if treatment.profile == "0" else list(enum))
    if treatment.hardware != "0":
        hardware_tool = _clone(by_name[HARDWARE_TOOL])
        # Keep the interface identical at H=.5/H=1 without exposing internal
        # R5 envelope fields or receipt provenance in the model-facing schema.
        hardware_tool["function"]["description"] = (
            "Query the hardware information available in this Episode. "
            "Returns supported facts when available, otherwise an honest no-information result."
        )
        result.append(hardware_tool)
    names = [x.get("function", {}).get("name") for x in result]
    if any(name not in CORE_TOOLS + (HARDWARE_TOOL,) for name in names) or len(names) != len(set(names)):
        raise ProtocolError("TOOL_SET_INVALID")
    return result


def validate_schema_factorial(base_tools: list[dict[str, Any]]) -> dict[str, Any]:
    schemas = {Treatment(h, p).group_id_internal: treatment_tools(base_tools, Treatment(h, p))
               for h in LEVELS for p in LEVELS}
    for h in LEVELS:
        a, b = schemas[Treatment(h, "0.5").group_id_internal], schemas[Treatment(h, "1").group_id_internal]
        if canonical_bytes(a) != canonical_bytes(b):
            raise ProtocolError("PROFILE_05_1_SCHEMA_MISMATCH")
    for p in LEVELS:
        a, b = schemas[Treatment("0.5", p).group_id_internal], schemas[Treatment("1", p).group_id_internal]
        if canonical_bytes(a) != canonical_bytes(b):
            raise ProtocolError("HARDWARE_05_1_SCHEMA_MISMATCH")
    for treatment, tools in ((Treatment(h, p), schemas[Treatment(h, p).group_id_internal])
                             for h in LEVELS for p in LEVELS):
        names = [x["function"]["name"] for x in tools]
        if (HARDWARE_TOOL in names) != (treatment.hardware != "0"):
            raise ProtocolError("HARDWARE_VISIBILITY_INVALID")
        history = next(x for x in tools if x["function"]["name"] == "query_candidate_history")
        profile_allowed = PROFILE_FIELD in history["function"]["parameters"]["properties"]["fields"]["items"].get("enum", [])
        if profile_allowed != (treatment.profile != "0"):
            raise ProtocolError("PROFILE_HISTORY_VISIBILITY_INVALID")
    return schemas


def public_prompt_guard(system_prompt: str, user_prompt: str, tools: list[dict[str, Any]]) -> None:
    """Reject direct treatment and budget disclosure in model-facing material."""
    material = system_prompt + "\n" + user_prompt + "\n" + json.dumps(tools, ensure_ascii=False)
    patterns = (
        r"\bH(?:0|05|1)_P(?:0|05|1)\b", r"\b(?:H|P)\s*=\s*(?:0|0\.5|1)\b",
        r"\b(?:30|thirty)\s+(?:formal\s+)?candidates?\b", r"\b(?:remaining|max(?:imum)?)\s+(?:candidate\s+)?budget\b",
        r"\bcandidate\s+budget\s*[:=]",
    )
    for pattern in patterns:
        if re.search(pattern, material, re.IGNORECASE):
            raise ProtocolError("MODEL_VISIBLE_POLICY_OR_BUDGET_LEAK:" + pattern)


def public_state(state: Any) -> Any:
    """Recursively remove internal budget and treatment/profile side channels."""
    hidden = {"budget", "budget_used", "budget_max", "budget_remaining", "candidate_budget",
              "profile", "profile_feedback", "profile_status", "treatment", "treatment_id", "group_id",
        "hardware_level", "profile_level", "ncu_receipt", "ncu_metrics",
        "device_name", "device_model", "driver_version", "cuda_version", "gpu_uuid",
        "sm_clock", "gpu_clock", "power_watts", "temperature_c", "pstate"}
    if isinstance(state, dict):
        return {k: public_state(v) for k, v in state.items() if k.lower() not in hidden}
    if isinstance(state, list):
        return [public_state(x) for x in state]
    if isinstance(state, str):
        state = re.sub(r"(?i)\b(?:you have|remaining|maximum|max)\s+\d+\s+(?:formal\s+)?candidates?\b", "", state)
        state = re.sub(r"(?i)\b\d+\s*/\s*30\b", "", state)
    return state


def sanitize_oj_observation(observation: dict[str, Any]) -> dict[str, Any]:
    """Keep common correctness/performance feedback while removing profile data."""
    result = public_state(observation)
    if not isinstance(result, dict):
        raise ProtocolError("OJ_OBSERVATION_NOT_OBJECT")
    return result


_FORBIDDEN_VISIBLE_KEYS = {
    "profile", "profile_feedback", "profile_status", "profile_receipt", "profile_receipt_sha256",
    "profile_receipt_id", "profile_metrics", "ncu", "ncu_metrics", "ncu_receipt",
    "ncu_report", "gpu_identity", "gpu_uuid", "gpu_clock", "gpu_name", "gpu_device",
    "memory_clock", "compute_capability", "hardware_knowledge", "hardware_source_id",
    "hardware_facts", "hardware_source_sha256", "knowledge_source_sha256", "device_snapshot",
    "device_properties", "nvidia_smi", "gpu_telemetry", "utilization_percent",
    "treatment", "treatment_id", "group_id", "budget", "candidate_budget",
    "budget_used", "budget_max", "budget_remaining",
}
_HARDWARE_LEAK_TEXT = re.compile(
    r"(?i)(RTX\s*5060(?:\s*Laptop)?|SM[_\s-]?120|compute capability\s*12(?:\.0)?|"
    r"GPU\s*(?:core|memory)?\s*clock|memory bandwidth|multiprocessor count)"
)


def scrub_common_information(value: Any) -> Any:
    """Remove H/P-only fields and known H facts from common Agent channels."""
    if isinstance(value, dict):
        return {k: scrub_common_information(v) for k, v in value.items()
                if k.lower() not in _FORBIDDEN_VISIBLE_KEYS}
    if isinstance(value, list):
        return [scrub_common_information(x) for x in value]
    if isinstance(value, str):
        return _HARDWARE_LEAK_TEXT.sub("[hardware detail withheld]", value)
    return value


def workspace_read_for_agent(relative_path: str, files: dict[str, str]) -> str:
    """Policy-aware read interface; H/P corpora are never workspace files."""
    p = Path(relative_path)
    if p.is_absolute() or ".." in p.parts or p.as_posix().startswith(("knowledge/", "profiles/", "profile_receipts/")):
        raise ProtocolError("WORKSPACE_PATH_NOT_AVAILABLE")
    key = p.as_posix()
    if key not in files:
        raise ProtocolError("WORKSPACE_FILE_NOT_FOUND")
    text = files[key]
    try:
        parsed = json.loads(text)
    except (ValueError, TypeError):
        return str(scrub_common_information(text))
    return json.dumps(scrub_common_information(parsed), ensure_ascii=False, sort_keys=True)


def build_agent_request_messages(*, system_prompt: str, task_user_message: str,
                                 task_definition: str, candidate_abi: str,
                                 candidate_source: str, candidate_sha256: str,
                                 visible_observation: dict[str, Any],
                                 public_episode_history: dict[str, Any],
                                 tools: list[dict[str, Any]]) -> list[dict[str, str]]:
    """Build context without internal treatment metadata or budget counters."""
    if sha256(candidate_source.encode("utf-8")).upper() != candidate_sha256.upper():
        raise ProtocolError("CURRENT_CANDIDATE_SHA_MISMATCH")
    if not all(isinstance(x, str) and x for x in (system_prompt, task_user_message, task_definition, candidate_abi)):
        raise ProtocolError("PROMPT_CONTEXT_INCOMPLETE")
    names = [x.get("function", {}).get("name") for x in tools]
    if len(names) != len(set(names)):
        raise ProtocolError("MODEL_TOOL_SET_INVALID")
    if any(name not in CORE_TOOLS + (HARDWARE_TOOL,) for name in names):
        raise ProtocolError("UNAUTHORIZED_MODEL_TOOL")
    public_history = scrub_common_information(public_episode_history)
    visible = scrub_common_information(visible_observation)
    dynamic = {
        "task_definition": task_definition,
        "candidate_abi": candidate_abi,
        "current_candidate": {"source": candidate_source, "sha256": candidate_sha256},
        "latest_complete_evaluation_observation": visible,
        "episode_history": public_history,
    }
    public_prompt_guard(system_prompt, task_user_message, tools)
    available = [{"name": x["function"]["name"], "description": x["function"]["description"],
                  "parameters": x["function"]["parameters"]} for x in tools]
    user = (task_user_message + "\n\nAVAILABLE_TOOLS_V1\n" +
            json.dumps(available, ensure_ascii=False, sort_keys=True, separators=(",", ":")) +
            "\n\nN0_MANDATORY_INVOCATION_CONTEXT_JSON\n" +
            json.dumps(dynamic, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
    body = json.dumps({"system": system_prompt, "user": user, "dynamic": dynamic,
                       "available_tools": available}, ensure_ascii=False)
    if re.search(r"(?i)(?:budget|remaining\s+\d+\s+candidates|30\s+candidates|H0_P0|H05_P1)", body):
        # The word budget is forbidden from serialized prompt/context, but not
        # from protocol code or the external controller.
        raise ProtocolError("AGENT_CONTEXT_BLINDNESS_VALIDATION_FAILED")
    return [{"role": "system", "content": system_prompt}, {"role": "user", "content": user}]


def _agent_no_information() -> dict[str, Any]:
    return _clone(NO_INFORMATION_RESULT)


def make_r5_hardware_validator(expected_source_sha256: str) -> Callable[[dict[str, Any]], dict[str, Any]]:
    """Return a fail-closed validator for the frozen R5 hardware service envelope.

    The service's content_sha256 is over canonical JSON of each fact. The
    source hash binds the response to the enrolled R5 asset. No unverified
    result may be shown under H=1.
    """
    if not re.fullmatch(r"[0-9a-fA-F]{64}", expected_source_sha256 or ""):
        raise ProtocolError("HARDWARE_SOURCE_SHA_INVALID")
    expected = expected_source_sha256.lower()

    def validate(result: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(result, dict) or result.get("status") != "OK":
            raise ProtocolError("HARDWARE_ENVELOPE_STATUS_INVALID")
        if result.get("snapshot_sha256", "").lower() != expected:
            raise ProtocolError("HARDWARE_SOURCE_IDENTITY_MISMATCH")
        data = result.get("data")
        if not isinstance(data, dict) or data.get("knowledge_source_sha256", "").lower() != expected:
            raise ProtocolError("HARDWARE_DATA_SOURCE_IDENTITY_MISMATCH")
        if result.get("object_sha256", "").lower() != digest_json(data).lower():
            raise ProtocolError("HARDWARE_OBJECT_SHA_MISMATCH")
        status = data.get("status")
        facts = data.get("results")
        if not isinstance(facts, list):
            raise ProtocolError("HARDWARE_RESULTS_INVALID")
        if status == "NO_RELEVANT_FACT":
            if facts:
                raise ProtocolError("HARDWARE_EMPTY_STATUS_CONFLICT")
        elif status == "OK":
            if not facts:
                raise ProtocolError("HARDWARE_OK_WITHOUT_FACTS")
            for item in facts:
                if not isinstance(item, dict) or not isinstance(item.get("fact"), dict):
                    raise ProtocolError("HARDWARE_FACT_INVALID")
                fact = item["fact"]
                if fact.get("confidence_status") != "SUPPORTED" or not item.get("knowledge_id"):
                    raise ProtocolError("HARDWARE_FACT_NOT_VERIFIED")
                if item.get("knowledge_id") != fact.get("fact_id"):
                    raise ProtocolError("HARDWARE_FACT_ID_MISMATCH")
                if item.get("content_sha256", "").lower() != digest_json(fact).lower():
                    raise ProtocolError("HARDWARE_FACT_SHA_MISMATCH")
        else:
            raise ProtocolError("HARDWARE_RESULT_STATUS_INVALID")
        return _clone(result)

    return validate


class InformationRouter:
    """Routes H/P queries and records internal, receipt-bound audit events."""
    def __init__(self, treatment: Treatment, *, hardware_query: Callable[[str], dict[str, Any]],
                 profile_lookup: Callable[[str], dict[str, Any] | None],
                 profile_validator: Callable[[dict[str, Any], str], dict[str, Any]],
                 hardware_validator: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
                 profile_status_lookup: Callable[[str], str] | None = None,
                 audit_append: Callable[[dict[str, Any]], None] | None = None):
        self.treatment = treatment
        self.hardware_query = hardware_query
        self.profile_lookup = profile_lookup
        self.profile_validator = profile_validator
        self.hardware_validator = hardware_validator
        self.profile_status_lookup = profile_status_lookup or (lambda _: "COLLECTION_FAILED")
        self.audit_append = audit_append or (lambda _: None)

    def call(self, tool_name: str, arguments: dict[str, Any], *, episode_id: str,
             invocation_id: str, turn_id: str, tool_call_id: str,
             current_candidate_id: str) -> dict[str, Any]:
        internal: dict[str, Any] = {"episode_id": episode_id, "invocation_id": invocation_id,
            "turn_id": turn_id, "tool_call_id": tool_call_id, "tool_name": tool_name,
            "treatment_internal": self.treatment.group_id_internal}
        try:
            if tool_name == HARDWARE_TOOL:
                if self.treatment.hardware == "0":
                    raise ProtocolError("TOOL_NOT_IN_VISIBLE_SCHEMA")
                query = arguments.get("query")
                if not isinstance(query, str) or not query.strip():
                    raise ProtocolError("HARDWARE_QUERY_INVALID")
                if self.treatment.hardware == "0.5":
                    output = _agent_no_information()
                    internal_status = "POLICY_NO_RELEVANT"
                else:
                    raw = self.hardware_query(query)
                    if self.hardware_validator is None:
                        raise ProtocolError("HARDWARE_PROVENANCE_VALIDATOR_REQUIRED")
                    verified = self.hardware_validator(raw)
                    if not isinstance(verified, dict) or verified.get("status") != "OK":
                        raise ProtocolError("HARDWARE_SOURCE_RESULT_INVALID")
                    data = verified["data"]
                    internal.update(hardware_source_sha256=verified["snapshot_sha256"],
                        hardware_object_sha256=verified["object_sha256"],
                        hardware_result_sha256=digest_json(data))
                    if data["status"] == "NO_RELEVANT_FACT":
                        output = _agent_no_information()
                        internal_status = "POLICY_NO_RELEVANT"
                    else:
                        output = {"status": "OK", "results": [
                            {"knowledge_id": item["knowledge_id"], "fact": item["fact"]}
                            for item in data["results"]]}
                        internal_status = "DELIVERED_VALID"
                internal.update(status=internal_status, query_sha256=sha256(query.encode("utf-8")),
                                result_sha256=digest_json(output))
                self.audit_append(internal)
                return output
            if tool_name == "__profile_history_internal__":
                if self.treatment.profile == "0":
                    raise ProtocolError("TOOL_NOT_IN_VISIBLE_SCHEMA")
                candidate_id = arguments.get("candidate_id")
                if not isinstance(candidate_id, str) or not re.fullmatch(r"C\d{4}", candidate_id):
                    raise ProtocolError("PROFILE_CANDIDATE_ID_INVALID")
                candidate_index = int(candidate_id[1:])
                current_index = int(current_candidate_id[1:]) if re.fullmatch(r"C\d{4}", current_candidate_id or "") else -1
                if candidate_index >= current_index:
                    raise ProtocolError("PROFILE_QUERY_MUST_TARGET_PRIOR_CANDIDATE")
                if self.treatment.profile == "0.5":
                    output = _agent_no_information()
                    internal_status = "POLICY_NO_RELEVANT"
                else:
                    receipt = self.profile_lookup(candidate_id)
                    if receipt is None:
                        status = self.profile_status_lookup(candidate_id)
                        if status not in {"NOT_ELIGIBLE", "COLLECTION_FAILED", "GENERATED_NOT_DELIVERED", "DELIVERED_UNUSABLE"}:
                            raise ProtocolError("PROFILE_ABSENCE_STATUS_INVALID")
                        output = {"status": status, "message": "No matching result is available for this query.", "results": []}
                        internal_status = status
                    else:
                        projected = self.profile_validator(receipt, candidate_id)
                        output = {"status": "OK", "metrics": projected["metrics"]}
                        internal_status = "DELIVERED_VALID"
                        internal.update(profile_receipt_sha256=receipt.get("profile_receipt_sha256"),
                            profile_request_id=projected.get("request_id"),
                            profile_oj_receipt_sha256=projected.get("oj_receipt_sha256"),
                            profile_candidate_sha256=projected.get("candidate_sha256"),
                            profile_metrics_sha256=digest_json(projected["metrics"]))
                internal.update(status=internal_status, candidate_id=candidate_id,
                                result_sha256=digest_json(output))
                self.audit_append(internal)
                return output
            raise ProtocolError("TOOL_NOT_ROUTED_BY_POLICY")
        except Exception as exc:
            internal.update(status="POLICY_ERROR", error_class=type(exc).__name__,
                            error_code=str(exc), result_sha256=None)
            self.audit_append(internal)
            raise


def validate_profile_for_candidate(receipt: dict[str, Any], *, episode_id: str,
                                   candidate_id: str, candidate_sha256: str,
                                   request_id: str, oj_receipt_sha256: str,
                                   allowed_metrics: set[str],
                                   validate_existing: Callable[..., dict[str, Any]]) -> dict[str, Any]:
    """Use R7.4 receipt validation and add explicit Candidate/episode binding."""
    result = validate_existing(receipt, request_id=request_id, episode_id=episode_id,
        candidate_id=candidate_id, candidate_sha256=candidate_sha256,
        oj_receipt_sha256=oj_receipt_sha256, allowed_metrics=allowed_metrics)
    # The validator supplied by the staged R7.4 parent owns its metric allowlist.
    # Validate fields here rather than accepting a truthy PASS alone.
    if (result.get("status") != "PASS" or result.get("candidate_id") != candidate_id
            or result.get("episode_id") != episode_id
            or result.get("candidate_sha256", "").lower() != candidate_sha256.lower()
            or result.get("request_id") != request_id
            or result.get("oj_receipt_sha256", "").lower() != oj_receipt_sha256.lower()
            or not isinstance(result.get("metrics"), dict) or not result["metrics"]):
        raise ProtocolError("PROFILE_DELIVERY_BINDING_INVALID")
    return result


class BudgetLedger:
    """SQLite-backed Candidate state machine with fail-closed retry semantics."""
    def __init__(self, path: Path, episode_id: str, limit: int = MAX_FORMAL_CANDIDATES):
        if limit != MAX_FORMAL_CANDIDATES:
            raise ProtocolError("FORMAL_CANDIDATE_LIMIT_MUST_MATCH_FROZEN_PROTOCOL")
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.episode_id = episode_id
        self.limit = limit
        with self._connect() as con:
            con.execute("CREATE TABLE IF NOT EXISTS candidate_slots (episode_id TEXT NOT NULL, request_id TEXT NOT NULL, candidate_id TEXT NOT NULL, candidate_sha256 TEXT NOT NULL, status TEXT NOT NULL, verdict TEXT, dispatch_id TEXT, evidence_sha256 TEXT, receipt_sha256 TEXT, retry_count INTEGER NOT NULL DEFAULT 0, settled_at REAL, PRIMARY KEY(episode_id,request_id), UNIQUE(episode_id,candidate_id))")
            con.execute("CREATE TABLE IF NOT EXISTS candidate_events (event_id INTEGER PRIMARY KEY AUTOINCREMENT, episode_id TEXT NOT NULL, request_id TEXT NOT NULL, event_type TEXT NOT NULL, event_sha256 TEXT NOT NULL, evidence_sha256 TEXT, created_at REAL NOT NULL)")
            con.execute("CREATE TABLE IF NOT EXISTS episode_stop (episode_id TEXT PRIMARY KEY, stop_kind TEXT NOT NULL, reason TEXT NOT NULL, event_sha256 TEXT NOT NULL, created_at REAL NOT NULL)")

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(str(self.path), timeout=30, isolation_level=None)
        con.execute("PRAGMA busy_timeout=30000")
        return con

    def _counts(self, con: sqlite3.Connection) -> tuple[int, int]:
        row = con.execute("SELECT COUNT(*), SUM(CASE WHEN status='SETTLED' THEN 1 ELSE 0 END) FROM candidate_slots WHERE episode_id=?", (self.episode_id,)).fetchone()
        return int(row[0] or 0), int(row[1] or 0)

    def _active_count(self, con: sqlite3.Connection) -> int:
        row = con.execute("SELECT COUNT(*) FROM candidate_slots WHERE episode_id=? AND status IN ('RESERVED','DISPATCHED','OUTCOME_UNKNOWN')", (self.episode_id,)).fetchone()
        return int(row[0] or 0)

    def _append_event(self, con: sqlite3.Connection, request_id: str, event_type: str,
                      payload: dict[str, Any], evidence_sha256: str | None = None) -> None:
        body = {"episode_id": self.episode_id, "request_id": request_id,
                "event_type": event_type, "payload": payload,
                "evidence_sha256": evidence_sha256}
        con.execute("INSERT INTO candidate_events(episode_id,request_id,event_type,event_sha256,evidence_sha256,created_at) VALUES(?,?,?,?,?,?)",
            (self.episode_id, request_id, event_type, digest_json(body), evidence_sha256, time.time()))

    def reserve(self, *, request_id: str, candidate_id: str, candidate_sha256: str) -> dict[str, Any]:
        if not request_id or not re.fullmatch(r"C\d{4}", candidate_id) or not re.fullmatch(r"[0-9a-fA-F]{64}", candidate_sha256):
            raise ProtocolError("CANDIDATE_RESERVATION_IDENTITY_INVALID")
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            existing = con.execute("SELECT candidate_id,candidate_sha256,status,retry_count FROM candidate_slots WHERE episode_id=? AND request_id=?", (self.episode_id, request_id)).fetchone()
            if existing:
                if existing[0] != candidate_id or existing[1].lower() != candidate_sha256.lower():
                    raise ProtocolError("REQUEST_ID_REUSED_WITH_DIFFERENT_CANDIDATE")
                con.commit()
                return {"status": existing[2], "idempotent": True, "provider_allowed": False,
                        "retry_count": existing[3], "requires_reconciliation": existing[2] == "OUTCOME_UNKNOWN"}
            _, settled = self._counts(con)
            if settled + self._active_count(con) >= self.limit:
                con.execute("INSERT OR IGNORE INTO episode_stop VALUES(?,?,?,?,?)", (self.episode_id, "BUDGET_CUTOFF", "FORMAL_CANDIDATE_LIMIT_REACHED", sha256(b"BUDGET_CUTOFF"), time.time()))
                con.commit()
                return {"status": "BUDGET_CUTOFF", "idempotent": False, "provider_allowed": False}
            stop = con.execute("SELECT stop_kind,reason FROM episode_stop WHERE episode_id=?", (self.episode_id,)).fetchone()
            if stop:
                raise ProtocolError("EPISODE_STOPPED:" + str(stop[0]))
            con.execute("INSERT INTO candidate_slots(episode_id,request_id,candidate_id,candidate_sha256,status) VALUES(?,?,?,?,?)", (self.episode_id, request_id, candidate_id, candidate_sha256.lower(), "RESERVED"))
            self._append_event(con, request_id, "RESERVED", {"candidate_id": candidate_id,
                "candidate_sha256": candidate_sha256.lower()})
            con.commit()
            return {"status": "RESERVED", "idempotent": False, "provider_allowed": True}
        except Exception:
            if con.in_transaction:
                con.rollback()
            raise
        finally:
            con.close()

    def mark_dispatched(self, *, request_id: str, candidate_id: str,
                        candidate_sha256: str, dispatch_id: str) -> dict[str, Any]:
        if not dispatch_id:
            raise ProtocolError("DISPATCH_ID_REQUIRED")
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            row = con.execute("SELECT candidate_id,candidate_sha256,status,dispatch_id FROM candidate_slots WHERE episode_id=? AND request_id=?", (self.episode_id, request_id)).fetchone()
            if not row or row[0] != candidate_id or row[1].lower() != candidate_sha256.lower():
                raise ProtocolError("DISPATCH_RESERVATION_IDENTITY_MISMATCH")
            if row[2] == "DISPATCHED":
                if row[3] != dispatch_id:
                    raise ProtocolError("DUPLICATE_DISPATCH_IDENTITY_CONFLICT")
                con.commit()
                return {"status": "DISPATCHED", "dispatch_allowed": False, "idempotent": True}
            if row[2] != "RESERVED":
                raise ProtocolError("DISPATCH_STATE_INVALID:" + str(row[2]))
            con.execute("UPDATE candidate_slots SET status='DISPATCHED',dispatch_id=? WHERE episode_id=? AND request_id=? AND status='RESERVED'", (dispatch_id, self.episode_id, request_id))
            self._append_event(con, request_id, "DISPATCHED", {"dispatch_id": dispatch_id,
                "candidate_id": candidate_id, "candidate_sha256": candidate_sha256.lower()})
            con.commit()
            return {"status": "DISPATCHED", "dispatch_allowed": True, "idempotent": False}
        except Exception:
            if con.in_transaction:
                con.rollback()
            raise
        finally:
            con.close()

    def record_infrastructure_failure(self, *, request_id: str, candidate_id: str,
                                      candidate_sha256: str, evidence_sha256: str,
                                      execution_possible: bool,
                                      no_execution_proof: dict[str, Any] | None = None,
                                      no_execution_validator: Callable[..., bool] | None = None) -> dict[str, Any]:
        if not re.fullmatch(r"[0-9a-fA-F]{64}", evidence_sha256 or ""):
            raise ProtocolError("INFRA_FAILURE_EVIDENCE_SHA_REQUIRED")
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            row = con.execute("SELECT candidate_id,candidate_sha256,status,evidence_sha256 FROM candidate_slots WHERE episode_id=? AND request_id=?", (self.episode_id, request_id)).fetchone()
            if not row or row[0] != candidate_id or row[1].lower() != candidate_sha256.lower():
                raise ProtocolError("INFRA_FAILURE_IDENTITY_MISMATCH")
            if not execution_possible and row[2] != "RESERVED":
                if not isinstance(no_execution_proof, dict) or no_execution_validator is None:
                    raise ProtocolError("NO_EXECUTION_RECONCILIATION_REQUIRED")
                if digest_json(no_execution_proof).lower() != evidence_sha256.lower():
                    raise ProtocolError("NO_EXECUTION_PROOF_SHA_MISMATCH")
                dispatch_row = con.execute("SELECT dispatch_id FROM candidate_slots WHERE episode_id=? AND request_id=?",
                    (self.episode_id, request_id)).fetchone()
                proof_identity = {"episode_id": self.episode_id, "request_id": request_id,
                    "candidate_id": candidate_id, "candidate_sha256": candidate_sha256.lower(),
                    "dispatch_id": dispatch_row[0]}
                if not no_execution_validator(no_execution_proof, **proof_identity):
                    raise ProtocolError("NO_EXECUTION_PROOF_NOT_AUTHORITATIVE")
            state = "OUTCOME_UNKNOWN" if execution_possible else "RETRYABLE_INFRA"
            permitted_prior = {"DISPATCHED", state}
            if not execution_possible:
                permitted_prior.update({"RESERVED", "OUTCOME_UNKNOWN"})
            if row[2] not in permitted_prior:
                raise ProtocolError("INFRA_FAILURE_STATE_INVALID:" + str(row[2]))
            if row[2] == state and row[3] != evidence_sha256.lower():
                raise ProtocolError("INFRA_FAILURE_EVIDENCE_CONFLICT")
            con.execute("UPDATE candidate_slots SET status=?,evidence_sha256=? WHERE episode_id=? AND request_id=?", (state, evidence_sha256.lower(), self.episode_id, request_id))
            self._append_event(con, request_id, state, {"previous_status": row[2],
                "execution_possible": execution_possible, "candidate_id": candidate_id,
                "candidate_sha256": candidate_sha256.lower()}, evidence_sha256.lower())
            con.commit()
            return {"status": state, "logical_used": self.snapshot()["internal_used"],
                    "retry_requires_authoritative_no_execution": state == "OUTCOME_UNKNOWN"}
        except Exception:
            if con.in_transaction:
                con.rollback()
            raise
        finally:
            con.close()

    def authorize_same_request_retry(self, *, request_id: str, candidate_id: str,
                                     candidate_sha256: str, no_execution_evidence_sha256: str,
                                     authorization_receipt: dict[str, Any],
                                     authorization_validator: Callable[..., bool] | None) -> dict[str, Any]:
        if not re.fullmatch(r"[0-9a-fA-F]{64}", no_execution_evidence_sha256 or ""):
            raise ProtocolError("NO_EXECUTION_EVIDENCE_SHA_REQUIRED")
        if authorization_validator is None or not isinstance(authorization_receipt, dict):
            raise ProtocolError("RETRY_AUTHORIZATION_VALIDATOR_REQUIRED")
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            row = con.execute("SELECT candidate_id,candidate_sha256,status,evidence_sha256,retry_count FROM candidate_slots WHERE episode_id=? AND request_id=?", (self.episode_id, request_id)).fetchone()
            if not row or row[0] != candidate_id or row[1].lower() != candidate_sha256.lower():
                raise ProtocolError("RETRY_IDENTITY_MISMATCH")
            if row[2] != "RETRYABLE_INFRA" or row[3] != no_execution_evidence_sha256.lower():
                raise ProtocolError("RETRY_REQUIRES_MATCHING_PROOF_OF_NO_EXECUTION")
            auth_body = {"episode_id": self.episode_id, "request_id": request_id,
                "candidate_id": candidate_id, "candidate_sha256": candidate_sha256.lower(),
                "no_execution_evidence_sha256": no_execution_evidence_sha256.lower(),
                "next_retry_count": row[4] + 1}
            if not authorization_validator(authorization_receipt, **auth_body):
                raise ProtocolError("RETRY_AUTHORIZATION_INVALID")
            con.execute("UPDATE candidate_slots SET status='RESERVED',dispatch_id=NULL,retry_count=retry_count+1 WHERE episode_id=? AND request_id=?", (self.episode_id, request_id))
            self._append_event(con, request_id, "SAME_REQUEST_RETRY_AUTHORIZED", auth_body,
                no_execution_evidence_sha256.lower())
            con.commit()
            return {"status": "RESERVED", "same_request_id": request_id, "retry_count": row[4] + 1,
                    "new_candidate_budget_slot": False}
        except Exception:
            if con.in_transaction:
                con.rollback()
            raise
        finally:
            con.close()

    def reconcile_unknown(self, *, request_id: str, candidate_id: str,
                          candidate_sha256: str, authoritative_outcome: str,
                          evidence_sha256: str, verdict: str | None = None,
                          receipt_sha256: str | None = None,
                          no_execution_proof: dict[str, Any] | None = None,
                          no_execution_validator: Callable[..., bool] | None = None) -> dict[str, Any]:
        if authoritative_outcome == "NOT_EXECUTED":
            return self.record_infrastructure_failure(request_id=request_id, candidate_id=candidate_id,
                candidate_sha256=candidate_sha256, evidence_sha256=evidence_sha256, execution_possible=False,
                no_execution_proof=no_execution_proof, no_execution_validator=no_execution_validator)
        if authoritative_outcome == "TERMINAL":
            if not verdict or not receipt_sha256 or not isinstance(no_execution_proof, dict) or no_execution_validator is None:
                raise ProtocolError("TERMINAL_RECONCILIATION_REQUIRES_VERDICT_RECEIPT_AND_VALIDATOR")
            if digest_json(no_execution_proof).lower() != evidence_sha256.lower():
                raise ProtocolError("TERMINAL_RECONCILIATION_PROOF_SHA_MISMATCH")
            identity = {"episode_id": self.episode_id, "request_id": request_id,
                "candidate_id": candidate_id, "candidate_sha256": candidate_sha256.lower(),
                "verdict": verdict, "receipt_sha256": receipt_sha256.lower()}
            if not no_execution_validator(no_execution_proof, **identity):
                raise ProtocolError("TERMINAL_RECONCILIATION_NOT_AUTHORITATIVE")
            return self.settle(request_id=request_id, candidate_id=candidate_id,
                candidate_sha256=candidate_sha256, verdict=verdict,
                receipt_sha256=receipt_sha256, reconciliation_evidence_sha256=evidence_sha256,
                reconciliation_validated=True)
        raise ProtocolError("AUTHORITATIVE_OUTCOME_REQUIRED")

    def settle(self, *, request_id: str, candidate_id: str, candidate_sha256: str,
               verdict: str, receipt_sha256: str,
               reconciliation_evidence_sha256: str | None = None,
               reconciliation_validated: bool = False) -> dict[str, Any]:
        if not verdict or "INFRASTRUCTURE" in verdict or verdict.startswith("OJ_INFRASTRUCTURE"):
            raise ProtocolError("INFRASTRUCTURE_EVENT_CANNOT_CONSUME_CANDIDATE_SLOT")
        if not re.fullmatch(r"[0-9a-fA-F]{64}", receipt_sha256 or ""):
            raise ProtocolError("AUTHORITATIVE_RECEIPT_SHA_REQUIRED")
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            row = con.execute("SELECT candidate_id,candidate_sha256,status,verdict,receipt_sha256 FROM candidate_slots WHERE episode_id=? AND request_id=?", (self.episode_id, request_id)).fetchone()
            if not row or row[0] != candidate_id or row[1].lower() != candidate_sha256.lower():
                raise ProtocolError("SETTLEMENT_RESERVATION_IDENTITY_MISMATCH")
            if row[2] == "SETTLED":
                if row[3] != verdict or row[4] != receipt_sha256.lower():
                    raise ProtocolError("IDEMPOTENT_SETTLEMENT_EVIDENCE_CONFLICT")
                con.commit()
                return {"status": "SETTLED", "idempotent": True, "used": self._counts(con)[1]}
            allowed_states = {"DISPATCHED"}
            if row[2] == "OUTCOME_UNKNOWN":
                if not re.fullmatch(r"[0-9a-fA-F]{64}", reconciliation_evidence_sha256 or "") or not reconciliation_validated:
                    raise ProtocolError("UNKNOWN_OUTCOME_REQUIRES_VALIDATED_AUTHORITY_EVIDENCE")
                allowed_states.add("OUTCOME_UNKNOWN")
            if row[2] not in allowed_states:
                raise ProtocolError("SETTLEMENT_STATE_NOT_EXECUTED_OR_AUTHORIZED:" + str(row[2]))
            con.execute("UPDATE candidate_slots SET status='SETTLED',verdict=?,receipt_sha256=?,settled_at=? WHERE episode_id=? AND request_id=? AND status=?", (verdict, receipt_sha256.lower(), time.time(), self.episode_id, request_id, row[2]))
            self._append_event(con, request_id, "SETTLED", {"previous_status": row[2],
                "candidate_id": candidate_id, "candidate_sha256": candidate_sha256.lower(),
                "verdict": verdict, "receipt_sha256": receipt_sha256.lower(),
                "reconciliation_evidence_sha256": reconciliation_evidence_sha256.lower() if reconciliation_evidence_sha256 else None},
                receipt_sha256.lower())
            used = self._counts(con)[1]
            if used >= self.limit:
                con.execute("INSERT OR IGNORE INTO episode_stop VALUES(?,?,?,?,?)", (self.episode_id, "BUDGET_CUTOFF", "FORMAL_CANDIDATE_LIMIT_REACHED", sha256(b"BUDGET_CUTOFF"), time.time()))
            con.commit()
            return {"status": "SETTLED", "idempotent": False, "used": used,
                    "episode_stopped": used >= self.limit, "stop_kind": "BUDGET_CUTOFF" if used >= self.limit else None}
        except Exception:
            if con.in_transaction:
                con.rollback()
            raise
        finally:
            con.close()

    def stop(self, *, kind: str, reason: str) -> None:
        if kind not in {"AGENT_STOP", "INFRASTRUCTURE_ABORT", "OPERATOR_STOP"} or not reason:
            raise ProtocolError("EPISODE_STOP_KIND_INVALID")
        body = {"episode_id": self.episode_id, "kind": kind, "reason": reason}
        with self._connect() as con:
            con.execute("BEGIN IMMEDIATE")
            existing = con.execute("SELECT stop_kind,reason,event_sha256 FROM episode_stop WHERE episode_id=?", (self.episode_id,)).fetchone()
            if existing and existing != (kind, reason, digest_json(body)):
                raise ProtocolError("EPISODE_STOP_CONFLICT")
            con.execute("INSERT OR IGNORE INTO episode_stop VALUES(?,?,?,?,?)", (self.episode_id, kind, reason, digest_json(body), time.time()))
            con.commit()

    def snapshot(self) -> dict[str, Any]:
        with self._connect() as con:
            rows = con.execute("SELECT request_id,candidate_id,candidate_sha256,status,verdict,dispatch_id,evidence_sha256,receipt_sha256,retry_count FROM candidate_slots WHERE episode_id=? ORDER BY candidate_id", (self.episode_id,)).fetchall()
            events = con.execute("SELECT request_id,event_type,event_sha256,evidence_sha256,created_at FROM candidate_events WHERE episode_id=? ORDER BY event_id", (self.episode_id,)).fetchall()
            stop = con.execute("SELECT stop_kind,reason,event_sha256 FROM episode_stop WHERE episode_id=?", (self.episode_id,)).fetchone()
        settled = sum(r[3] == "SETTLED" for r in rows)
        return {"episode_id": self.episode_id, "internal_used": settled, "internal_limit": self.limit,
                "slots": [{"request_id": r[0], "candidate_id": r[1], "candidate_sha256": r[2], "status": r[3], "verdict": r[4], "dispatch_id": r[5], "evidence_sha256": r[6], "receipt_sha256": r[7], "retry_count": r[8]} for r in rows],
                "events": [{"request_id": e[0], "event_type": e[1], "event_sha256": e[2], "evidence_sha256": e[3], "created_at": e[4]} for e in events],
                "stop": ({"kind": stop[0], "reason": stop[1], "event_sha256": stop[2]} if stop else None)}


def budget_cutoff_action(ledger: BudgetLedger, *, settled_used: int) -> str:
    """Called by the outer controller after a terminal Candidate settlement."""
    if settled_used >= ledger.limit:
        snap = ledger.snapshot()
        if snap["internal_used"] != ledger.limit or snap["stop"] is None or snap["stop"]["kind"] != "BUDGET_CUTOFF":
            raise ProtocolError("BUDGET_CUTOFF_STATE_NOT_DURABLE")
        return "TERMINATE_WITHOUT_PROVIDER_CALL"
    return "CONTINUE_AGENT_LOOP"


def make_pilot_randomization(seed: int, *, replicates_per_cell: int = 2) -> dict[str, Any]:
    """Build 54 engineering episodes as two independently shuffled balanced blocks.

    This is a reproducible engineering plan only; task keys remain provisional
    until task definitions and Protocol §13 are frozen.
    """
    if not isinstance(seed, int) or isinstance(seed, bool) or replicates_per_cell != 2:
        raise ProtocolError("PILOT_RANDOMIZATION_PARAMETERS_INVALID")
    import random
    rng = random.Random(seed)
    treatments = [(h, p) for h in LEVELS for p in LEVELS]
    blocks: list[list[dict[str, Any]]] = []
    ordinal = 0
    for block_no in (1, 2):
        block = [{"task_key_provisional": t, "h": h, "p": p, "replicate": block_no}
                 for t in ("T1", "T2", "T3") for h, p in treatments]
        rng.shuffle(block)
        for entry in block:
            ordinal += 1
            entry["pilot_episode_order"] = ordinal
            entry["time_block"] = block_no
            entry["episode_id_placeholder"] = f"PILOT-{ordinal:03d}"
            entry["formal_sample"] = False
        blocks.append(block)
    counts: dict[tuple[str, str, str], int] = {}
    for block in blocks:
        for e in block:
            key = (e["task_key_provisional"], e["h"], e["p"])
            counts[key] = counts.get(key, 0) + 1
    if len(blocks) != 2 or len(blocks[0]) != 27 or len(blocks[1]) != 27 or set(counts.values()) != {2}:
        raise ProtocolError("PILOT_RANDOMIZATION_BALANCE_INVALID")
    plan = {"schema": "N0_HP_9X3X2_ENGINEERING_PILOT_RANDOMIZATION_V1",
            "seed": seed, "formal_sample": False, "episodes": [x for b in blocks for x in b],
            "cell_counts": {"|".join(k): v for k, v in sorted(counts.items())},
            "block_rule": "two blocks; each task x treatment cell appears once per block",
            "policy_release_required_before_start": True}
    plan["plan_sha256"] = digest_json(plan)
    return plan


def enrollment_allowed(task_specs: dict[str, dict[str, Any]], *, protocol_frozen: bool) -> None:
    if not protocol_frozen:
        raise ProtocolError("FORMAL_ENROLLMENT_BLOCKED_PROTOCOL_NOT_FROZEN")
    if set(task_specs) != {"T1", "T2", "T3"}:
        raise ProtocolError("THREE_TASK_REGISTRY_INCOMPLETE")
    for task_id, spec in task_specs.items():
        required = {"official_task_id", "source_sha256", "input_spec_sha256", "reference_sha256",
                    "tolerance", "forbidden_implementations", "adapter_sha256", "status"}
        if not required <= set(spec) or spec.get("status") != "FROZEN":
            raise ProtocolError("TASK_NOT_FROZEN:" + task_id)


def validate_task_source(path: Path, expected_sha256: str) -> None:
    if sha256(Path(path).read_bytes()).upper() != expected_sha256.upper():
        raise ProtocolError("TASK_SOURCE_SHA_MISMATCH")
