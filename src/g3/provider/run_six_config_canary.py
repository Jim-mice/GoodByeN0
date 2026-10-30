from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path

from paratera_chat import ALLOWED_EFFORTS, ALLOWED_MODELS, ParateraChatCompletions, ProviderError


OUT_ROOT = Path(r"C:\NOVELTY_V7_SHARED\audit\model_canaries_v3")
VISIBLE = ("candidate.cu", "visible_observation.json", "visible_history.json", "auxiliary_context.txt")
MARK_1 = "// V7_NONSCIENTIFIC_PROVIDER_CANARY_TURN_1"
MARK_2 = "// V7_NONSCIENTIFIC_PROVIDER_CANARY_TURN_2"
TOOLS = [
    {"type": "function", "function": {"name": "read_workspace_file", "description": "Read a visible file from the isolated canary workspace.", "parameters": {"type": "object", "properties": {"relative_path": {"type": "string"}}, "required": ["relative_path"], "additionalProperties": False}}},
    {"type": "function", "function": {"name": "write_candidate", "description": "Append exactly one listed non-scientific marker as a new line in the canary CUDA candidate file.", "parameters": {"type": "object", "properties": {"marker": {"type": "string", "enum": ["// V7_NONSCIENTIFIC_PROVIDER_CANARY_TURN_1", "// V7_NONSCIENTIFIC_PROVIDER_CANARY_TURN_2"]}}, "required": ["marker"], "additionalProperties": False}}},
    {"type": "function", "function": {"name": "workspace_pwd", "description": "Return the isolated canary workspace path.", "parameters": {"type": "object", "properties": {}, "additionalProperties": False}}},
]


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def dispatch(name: str, args: dict, root: Path) -> dict:
    if name == "read_workspace_file":
        rel = args.get("relative_path")
        if not isinstance(rel, str) or rel not in VISIBLE:
            return {"ok": False, "error": "PATH_NOT_ALLOWED"}
        target = root / rel
        if not target.is_file():
            return {"ok": False, "error": "FILE_NOT_FOUND"}
        raw = target.read_bytes()
        return {"ok": True, "relative_path": rel, "sha256": sha(raw), "content_utf8": raw.decode("utf-8", errors="strict")}
    if name == "write_candidate":
        addition = args.get("marker")
        if addition not in {MARK_1, MARK_2}:
            return {"ok": False, "error": "INVALID_APPEND_TEXT"}
        target = root / "candidate.cu"
        with target.open("ab") as handle:
            handle.write(("\n" + addition).encode("utf-8"))
        raw = target.read_bytes()
        return {"ok": True, "sha256": sha(raw), "byte_count": len(raw)}
    if name == "workspace_pwd":
        return {"ok": True, "workspace": str(root.resolve())}
    return {"ok": False, "error": "UNKNOWN_TOOL"}


def call_stage(client: ParateraChatCompletions, model: str, effort: str, messages: list[dict], root: Path, label: str, evidence: list[dict]) -> list[dict]:
    for call_index in range(1, 5):
        stamp = datetime.now(timezone.utc).isoformat()
        try:
            payload, ev = client.complete(model, effort, messages, tools=TOOLS, tool_choice="auto", max_tokens=384)
        except ProviderError as exc:
            evidence.append({"stage": label, "timestamp_utc": stamp, "status": "PROVIDER_ERROR", "safe_error": str(exc)})
            return messages
        choice = (payload.get("choices") or [{}])[0]
        assistant = choice.get("message") or {}
        safe_payload = {k: v for k, v in payload.items() if k != "provider_specific_fields"}
        if isinstance(safe_payload.get("choices"), list):
            safe_payload["choices"] = [{**choice_item, "message": {k: v for k, v in (choice_item.get("message") or {}).items() if k != "provider_specific_fields"}} for choice_item in safe_payload["choices"]]
        record = {"stage": label, "timestamp_utc": stamp, "status": "RESPONSE", "response_id": ev.response_id or "UNAVAILABLE", "request_id": ev.request_id or "UNAVAILABLE", "requested_model": model, "returned_model": ev.returned_model or "UNAVAILABLE", "requested_effort": effort, "usage": ev.usage or "UNAVAILABLE", "finish_reason": choice.get("finish_reason"), "raw_response": safe_payload}
        evidence.append(record)
        messages.append(assistant)
        tool_calls = assistant.get("tool_calls") or []
        if not tool_calls:
            return messages
        for call in tool_calls:
            fn = call.get("function") or {}
            name = fn.get("name", "")
            try:
                args = json.loads(fn.get("arguments") or "{}")
            except json.JSONDecodeError:
                args = {}
            result = dispatch(name, args, root)
            evidence.append({"stage": label, "tool_call_id": call.get("id", "UNAVAILABLE"), "tool_name": name, "arguments": args, "tool_result": result})
            messages.append({"role": "tool", "tool_call_id": call.get("id"), "content": json.dumps(result, ensure_ascii=False)})
    evidence.append({"stage": label, "status": "MAX_AGENT_TURNS_REACHED"})
    return messages


def run_one(model: str, effort: str) -> dict:
    client = ParateraChatCompletions()
    tag = re.sub(r"[^A-Za-z0-9_-]", "_", model) + "_" + effort.upper()
    root = OUT_ROOT / tag / "workspace"
    root.mkdir(parents=True, exist_ok=True)
    (root / "candidate.cu").write_text('extern "C" __global__ void canary_kernel() { }\n', encoding="utf-8")
    (root / "visible_observation.json").write_text(json.dumps({"kind": "NONSCIENTIFIC_CANARY", "feedback": "No OJ evaluation was run."}, indent=2), encoding="utf-8")
    (root / "visible_history.json").write_text(json.dumps({"turns": [], "notice": "Temporary test-only history."}, indent=2), encoding="utf-8")
    (root / "auxiliary_context.txt").write_text("Temporary non-scientific provider integration test context.\n", encoding="utf-8")
    before_sha = sha((root / "candidate.cu").read_bytes())
    events: list[dict] = []
    system = "You are in a non-scientific tool protocol test. Use only the provided tools and only this isolated workspace. Never claim a file action unless the tool result confirms it."
    messages = [{"role": "system", "content": system}, {"role": "user", "content": f"First, attempt to read missing_probe.txt using read_workspace_file and recover from its expected PATH_NOT_ALLOWED result. Then use tools to read each of these exact visible files: {', '.join(VISIBLE)}; call workspace_pwd; use write_candidate with marker exactly {MARK_1} (do not add escape sequences); read candidate.cu again to verify the edit; then summarize briefly."}]
    call_stage(client, model, effort, messages, root, "TURN_1", events)
    after_1 = (root / "candidate.cu").read_bytes()
    messages.append({"role": "user", "content": "Mock feedback only (no real OJ was called): shape_A=1.01x, shape_B=0.99x, shape_C=1.02x; correctness status=not evaluated. Read visible_history.json and candidate.cu, then use write_candidate with marker exactly " + MARK_2 + " (do not add escape sequences). Verify candidate.cu with a tool and finish with exactly one JSON object matching this Canonical control schema: {\"decision\":\"CONTINUE\",\"reason\":\"brief reason\"}."})
    call_stage(client, model, effort, messages, root, "TURN_2", events)
    after_2 = (root / "candidate.cu").read_bytes()
    tools = [e for e in events if "tool_name" in e]
    reads = {e.get("arguments", {}).get("relative_path") for e in tools if e.get("tool_name") == "read_workspace_file" and e.get("tool_result", {}).get("ok")}
    failed_indexes = [i for i, e in enumerate(tools) if e.get("tool_name") == "read_workspace_file" and e.get("tool_result", {}).get("error") in {"FILE_NOT_FOUND", "PATH_NOT_ALLOWED"}]
    recovered = any(any(later.get("tool_name") == "read_workspace_file" and later.get("tool_result", {}).get("ok") for later in tools[i + 1:]) for i in failed_indexes)
    failed_read = bool(failed_indexes)
    final_texts = [m.get("content") for m in messages if m.get("role") == "assistant" and isinstance(m.get("content"), str)]
    control_ok = bool(final_texts and re.search(r'\{\s*"decision"\s*:\s*"CONTINUE"\s*,\s*"reason"\s*:\s*"[^"\r\n]*"\s*\}', final_texts[-1]))
    saved = {"schema": "NOVELTY_V7_SIX_CONFIG_CANARY_03", "model": model, "effort": effort, "workspace": str(root), "source_before_sha": before_sha, "source_after_turn1_sha": sha(after_1), "source_after_turn2_sha": sha(after_2), "turn1_edit_observed": sha(after_1) != before_sha and MARK_1.encode() in after_1, "turn2_edit_observed": sha(after_2) != sha(after_1) and MARK_2.encode() in after_2, "all_visible_files_read": set(VISIBLE).issubset(reads), "controlled_failure_returned_and_recovery_observed": failed_read and recovered and set(VISIBLE).issubset(reads), "valid_continue_control": control_ok, "tool_calls": tools, "events": events, "scientific_episode": False, "real_oj_called": False}
    (root.parent / "canary_record.json").write_text(json.dumps(saved, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return saved


def main() -> int:
    summary = []
    model_filter = {v for v in os.environ.get("NOVELTY_V7_CANARY_MODELS", "").split(",") if v}
    models = sorted(model_filter.intersection(ALLOWED_MODELS)) if model_filter else sorted(ALLOWED_MODELS)
    for model in models:
        for effort in sorted(ALLOWED_EFFORTS):
            result = run_one(model, effort)
            summary.append(result)
            print(json.dumps({"model": result["model"], "effort": result["effort"], "turn1_edit": result["turn1_edit_observed"], "turn2_edit": result["turn2_edit_observed"], "all_visible_files_read": result["all_visible_files_read"], "failure_recovery": result["controlled_failure_returned_and_recovery_observed"], "valid_continue_control": result["valid_continue_control"]}, ensure_ascii=False), flush=True)
    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    (OUT_ROOT / "summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    passed = sum(r["turn1_edit_observed"] and r["turn2_edit_observed"] and r["all_visible_files_read"] and r["controlled_failure_returned_and_recovery_observed"] and r["valid_continue_control"] for r in summary)
    print(f"CANARIES_PASS={passed}/{len(summary)}")
    print(f"CANARY_ROOT={OUT_ROOT}")
    return 0 if passed == 6 else 1


if __name__ == "__main__":
    raise SystemExit(main())
