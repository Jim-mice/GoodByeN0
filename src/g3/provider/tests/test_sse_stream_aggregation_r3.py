from __future__ import annotations

import hashlib
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "provider_staging"))
sys.path.insert(0, str(ROOT / "canonical_harness"))
import paratera_http_worker as http_worker
from paratera_chat import ParateraChatCompletions
from provider_queue import _retry_delay_seconds
from provider_retry import retryable_provider_error


class StreamResponse:
    status = 200
    headers = {"Content-Type": "text/event-stream", "Transfer-Encoding": "chunked", "x-request-id": "srv-123"}

    def __init__(self, lines):
        self.lines = list(lines)

    def readline(self):
        return self.lines.pop(0) if self.lines else b""

    def close(self):
        pass


def frame(value):
    return ("data: " + json.dumps(value, separators=(",", ":")) + "\n\n").encode()


class SseAggregationTests(unittest.TestCase):
    def setUp(self):
        os.environ["jlapi"] = "unit-test-secret"

    def tearDown(self):
        os.environ.pop("jlapi", None)

    def test_real_wire_stream_tool_call_and_usage_are_aggregated(self):
        body = {"model": "DeepSeek-V4-Pro-0813", "messages": [], "tools": [], "tool_choice": "auto"}
        logical_sha = hashlib.sha256(json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()
        tool0 = {"id": "call_abc", "type": "function", "function": {"name": "query_candidate_history", "arguments": "{\"candidate_ids\":[\"C0000\"],\"fields\":[\"timing\","}}
        tool1 = {"index": 0, "function": {"arguments": "\"profile\",\"diagnostics\"]}"}}
        lines = [
            frame({"id": "chatcmpl-1", "object": "chat.completion.chunk", "created": 17, "model": body["model"], "choices": [{"index": 0, "delta": {"role": "assistant"}, "finish_reason": None}]}),
            frame({"choices": [{"index": 0, "delta": {"tool_calls": [{"index": 0, **tool0}]}, "finish_reason": None}]}),
            frame({"choices": [{"index": 0, "delta": {"tool_calls": [tool1]}, "finish_reason": None}]}),
            frame({"choices": [{"index": 0, "delta": {}, "finish_reason": "tool_calls"}]}),
            frame({"choices": [], "usage": {"prompt_tokens": 25, "completion_tokens": 9, "total_tokens": 34}}),
            b"data: [DONE]\n\n",
        ]
        observed = {}

        def opener(request, timeout):
            observed["wire_body"] = json.loads(request.data.decode())
            observed["accept"] = request.get_header("Accept")
            return StreamResponse(lines)

        with tempfile.TemporaryDirectory() as td:
            artifact = Path(td) / "attempt.response.json"
            result = http_worker.perform({"url": "https://example.invalid/v1/chat/completions", "body": body,
                                          "request_sha256": logical_sha, "attempt_id": "attempt-1",
                                          "request_id": "req-1", "response_artifact_path": str(artifact)}, opener=opener)
            self.assertEqual(result["terminal_status"], "RESPONSE_RECEIVED")
            payload = result["payload"]
            call = payload["choices"][0]["message"]["tool_calls"][0]
            self.assertEqual(call["id"], "call_abc")
            self.assertEqual(call["function"]["name"], "query_candidate_history")
            self.assertEqual(json.loads(call["function"]["arguments"]), {"candidate_ids": ["C0000"], "fields": ["timing", "profile", "diagnostics"]})
            self.assertEqual(payload["usage"]["total_tokens"], 34)
            self.assertTrue(observed["wire_body"]["stream"])
            self.assertEqual(observed["accept"], "text/event-stream")
            self.assertEqual(result["telemetry"]["request_sha256_actual"], logical_sha)
            self.assertNotEqual(result["telemetry"]["wire_request_sha256"], logical_sha)
            self.assertTrue(result["telemetry"]["sse_done_received"])
            loaded = ParateraChatCompletions._load_response_artifact(artifact, "attempt-1", logical_sha)
            self.assertEqual(loaded["payload"]["choices"][0]["finish_reason"], "tool_calls")
            self.assertEqual(loaded["body_sha256"], result["telemetry"]["raw_response_sha256"])

    def test_usage_is_absent_when_provider_did_not_send_usage(self):
        payload = http_worker._aggregate_sse(
            frame({"id": "r", "model": "m", "choices": [{"index": 0, "delta": {"role": "assistant", "content": "ok"}, "finish_reason": None}]}) +
            frame({"choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]}) + b"data: [DONE]\n\n", {})
        self.assertNotIn("usage", payload)

    def test_stream_without_done_is_unknown_and_not_retryable(self):
        telemetry = {}
        with self.assertRaisesRegex(ValueError, "SSE_DONE_MARKER_MISSING"):
            http_worker._aggregate_sse(frame({"choices": [{"index": 0, "delta": {"role": "assistant"}, "finish_reason": "stop"}]}), telemetry)
        exc = RuntimeError("incomplete stream")
        exc.telemetry = {"terminal_status": "SSE_STREAM_INCOMPLETE_OUTCOME_UNKNOWN"}
        self.assertEqual(retryable_provider_error(exc), (False, 0.0))
        self.assertIsNone(_retry_delay_seconds({"http_status": None, "terminal_status": "SSE_STREAM_INCOMPLETE_OUTCOME_UNKNOWN"}, 1))


if __name__ == "__main__":
    unittest.main(verbosity=2)
