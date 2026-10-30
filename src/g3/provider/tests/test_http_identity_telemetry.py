import hashlib
import json
import os
from unittest.mock import patch

from provider_staging.paratera_http_worker import perform


class _Headers(dict):
    def get(self, key, default=None):
        return super().get(key, default)


class _Response:
    status = 200
    _body = json.dumps({"id": "resp-test", "model": "DeepSeek-V4-Pro-0813",
                        "choices": [{"index": 0, "message": {"role": "assistant", "content": "ok"},
                                     "finish_reason": "stop"}]}, separators=(",", ":")).encode()
    headers = _Headers({"Content-Length": str(len(_body)), "x-request-id": "provider-test-id",
                        "Content-Type": "application/json"})

    def read(self, size=-1):
        if getattr(self, "done", False):
            return b""
        self.done = True
        return self._body

    def getcode(self):
        return self.status


def test_sender_records_credential_fingerprint_and_exact_request_without_secret(tmp_path):
    token = "diagnostic-secret-never-persist"
    body = {"model": "DeepSeek-V4-Pro-0813", "messages": [{"role": "user", "content": "probe"}],
            "reasoning_effort": "high", "tools": [], "tool_choice": "auto"}
    expected_sha = hashlib.sha256(json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()
    payload = {"url": "https://llmapi.paratera.com/v1/chat/completions", "body": body,
               "attempt_id": "attempt-test", "request_id": "request-test", "key_alias": "jlapi2",
               "request_sha256": expected_sha}
    seen = {}

    def opener(request, timeout):
        seen["url"] = request.full_url
        seen["method"] = request.get_method()
        seen["authorization"] = request.get_header("Authorization")
        seen["body"] = json.loads(request.data.decode("utf-8"))
        seen["accept"] = request.get_header("Accept")
        return _Response()

    with patch.dict(os.environ, {"jlapi": token, "N0_PROVIDER_RELEASE_ID": "provider-test-release",
                                 "N0_PROVIDER_MANIFEST_SHA256": "A" * 64}, clear=True):
        result = perform(payload, opener=opener)

    telemetry = result["telemetry"]
    assert result["terminal_status"] == "RESPONSE_RECEIVED"
    assert seen["url"] == payload["url"] and seen["method"] == "POST"
    assert seen["authorization"] == "Bearer " + token
    assert seen["body"] == {**body, "stream": True}
    assert seen["accept"] == "text/event-stream"
    assert telemetry["request_id"] == "request-test"
    assert telemetry["attempt_id"] == "attempt-test"
    assert telemetry["key_alias"] == "jlapi2"
    assert telemetry["credential_sha256_prefix"] == hashlib.sha256(token.encode()).hexdigest()[:12]
    assert telemetry["request_sha256_actual"] == expected_sha
    assert telemetry["wire_request_sha256"] == hashlib.sha256(json.dumps(seen["body"], ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()
    assert telemetry["provider_release_id"] == "provider-test-release"
    assert token not in json.dumps(telemetry, ensure_ascii=False)


def test_actual_request_hash_mismatch_fails_before_network():
    called = False
    payload = {"url": "https://llmapi.paratera.com/v1/chat/completions",
               "body": {"model": "DeepSeek-V4-Pro-0813", "messages": []},
               "attempt_id": "attempt-test", "request_id": "request-test", "request_sha256": "bad"}

    def opener(request, timeout):
        nonlocal called
        called = True
        raise AssertionError("NETWORK_MUST_NOT_BE_CALLED")

    with patch.dict(os.environ, {"jlapi": "diagnostic-secret"}, clear=True):
        result = perform(payload, opener=opener)
    assert result["terminal_status"] == "REQUEST_HASH_MISMATCH"
    assert called is False
