from __future__ import annotations

import base64
import hashlib
import json
import os
import sys
from pathlib import Path

from n0_integration.provider_r20_bridge import R20N0Bridge


def _frame(value: dict) -> bytes:
    return ("data: " + json.dumps(value, separators=(",", ":")) + "\n\n").encode()


def test_complete_sse_artifact_is_normalized_without_losing_raw_evidence(tmp_path, monkeypatch):
    provider_root = Path(os.environ["N0_G3_PROVIDER_RELEASE_ROOT"]).resolve()
    sys.path.insert(0, str(provider_root))
    from provider_staging.paratera_http_worker import _aggregate_sse

    request_id = "N0-PROV-SSE-BRIDGE-TEST"
    lineage_id = "N0-ATT-SSE-BRIDGE-TEST"
    http_attempt_id = "http-attempt-sse-bridge-test"
    episode_id = "G3-SSE-BRIDGE-TEST-E01"
    request_sha = "a" * 64
    request = {"model": "DeepSeek-V4-Pro-0813", "messages": [{"role": "user", "content": "test"}],
               "reasoning_effort": "high", "tools": [], "tool_choice": "auto"}
    chunks = [
        _frame({"id": "chatcmpl-sse-test", "model": request["model"], "choices": [
            {"index": 0, "delta": {"role": "assistant"}, "finish_reason": None}]}),
        _frame({"choices": [{"index": 0, "delta": {"tool_calls": [{"index": 0, "id": "call-test",
            "type": "function", "function": {"name": "query_candidate_history",
            "arguments": "{\\\"candidate_ids\\\":[\\\"C0000\\\"],\\\"fields\\\":[\\\"profile\\\"]}"}}]},
            "finish_reason": None}]}),
        _frame({"choices": [{"index": 0, "delta": {}, "finish_reason": "tool_calls"}]}),
        b"data: [DONE]\n\n",
    ]
    raw_sse = b"".join(chunks)
    parse_telemetry: dict = {}
    normalized = _aggregate_sse(raw_sse, parse_telemetry)
    artifact = {
        "schema": "NOVELTY_V7_PROVIDER_RESPONSE_ARTIFACT_V1", "attempt_id": http_attempt_id,
        "request_sha256": request_sha, "body_complete": True,
        "body_base64": base64.b64encode(raw_sse).decode("ascii"),
        "body_sha256": hashlib.sha256(raw_sse).hexdigest(), "body_bytes_received": len(raw_sse),
        "http_status": 200,
    }
    artifact_raw = json.dumps(artifact, sort_keys=True, separators=(",", ":")).encode()
    artifact_path = tmp_path / "attempt.response.json"
    artifact_path.write_bytes(artifact_raw)
    result = {
        "request_id": request_id, "episode_id": episode_id, "treatment": "G3",
        "request_body_sha256": request_sha, "request_payload": request, "status": "COMPLETED",
        "current_attempt_id": http_attempt_id, "attempt_ids": [http_attempt_id],
        "attempt_lineage_id": lineage_id, "response_id": normalized["id"],
        "payload": normalized, "provider_request_id": "provider-response-sse-test",
        "telemetry": {
            "response_artifact_status": "PERSISTED_ATOMICALLY", "response_artifact_path": str(artifact_path),
            "response_artifact_sha256": hashlib.sha256(artifact_raw).hexdigest(),
            "response_id": normalized["id"], "request_sha256": request_sha,
            "body_complete": True, "helper_terminal_event_received": True, "http_status": 200,
            "response_mode": "SSE", "sse_done_received": True,
            "sse_event_count": parse_telemetry["sse_event_count"],
            "raw_response_sha256": hashlib.sha256(raw_sse).hexdigest(),
        },
    }
    (tmp_path / "results").mkdir()
    (tmp_path / "results" / (request_id + ".json")).write_text(json.dumps(result), encoding="utf-8")

    bridge = R20N0Bridge.__new__(R20N0Bridge)
    bridge.root = tmp_path.resolve()
    bridge.expected_treatment = "G3"
    bridge.release_root = provider_root
    verified = bridge.read_verified_result(request_id=request_id, attempt_id=lineage_id,
        episode_id=episode_id, payload_sha256=request_sha)

    assert verified["response_mode"] == "SSE"
    assert verified["raw_response_sha256"] == hashlib.sha256(raw_sse).hexdigest()
    assert hashlib.sha256(verified["response_body"]).hexdigest() == verified["normalized_response_sha256"]
    completion = json.loads(verified["response_body"])
    assert completion == normalized
    assert completion["choices"][0]["finish_reason"] == "tool_calls"
    assert completion["choices"][0]["message"]["tool_calls"][0]["id"] == "call-test"
    assert completion["choices"][0]["message"]["tool_calls"][0]["function"]["name"] == "query_candidate_history"
    assert "usage" not in completion
