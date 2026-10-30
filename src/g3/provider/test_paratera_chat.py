from __future__ import annotations

import json
import os
import io
import urllib.error
import unittest
import json
import queue
import threading
from unittest.mock import patch

from paratera_chat import ParateraChatCompletions, ProviderError
from paratera_http_worker import perform


class Response:
    status = 200

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def __init__(self):
        self.body = json.dumps({"id": "resp-test", "model": "DeepSeek-V4-Pro-0813", "choices": [{"message": {"content": "ok"}}], "usage": {"prompt_tokens": 2, "completion_tokens": 1}}).encode()
        self.headers = {"x-request-id": "req-test", "Content-Length": str(len(self.body))}
        self.done = False

    def read(self, size=-1):
        if self.done:
            return b""
        self.done = True
        return self.body

    def getcode(self):
        return self.status

    def close(self):
        pass


class _Stdout:
    def __init__(self):
        self.lines = queue.Queue()

    def __iter__(self):
        while True:
            line = self.lines.get()
            if line is None:
                return
            yield line


class _Stdin:
    def __init__(self, process):
        self.process = process
        self.parts = []

    def write(self, text):
        self.parts.append(text)

    def flush(self):
        pass

    def close(self):
        payload = json.loads("".join(self.parts))
        result = perform(payload, opener=self.process.opener)
        self.process.stdout.lines.put(json.dumps({"event": "TERMINAL", **result}) + "\n")
        self.process.stdout.lines.put(None)
        self.process.done.set()


class _Process:
    def __init__(self, opener):
        self.opener = opener
        self.stdin = _Stdin(self)
        self.stdout = _Stdout()
        self.stderr = io.BytesIO()
        self.done = threading.Event()
        self.returncode = None
        self.pid = 12345
        self.args = ["fake-http-worker"]

    def wait(self, timeout=None):
        if not self.done.wait(timeout):
            raise TimeoutError("fake worker timeout")
        self.returncode = 0
        return 0

    def kill(self):
        self.returncode = -9
        self.done.set()


class ProviderTests(unittest.TestCase):
    def test_exact_env_key_and_no_fallback(self):
        with patch.dict(os.environ, {"jlapi": "test-secret"}, clear=True):
            client = ParateraChatCompletions()
            self.assertTrue(client._token)
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(ProviderError, "PARATERA_CREDENTIAL_MISSING"):
                ParateraChatCompletions()

    def test_request_records_model_effort_and_response_evidence(self):
        opener = lambda *a, **k: Response()
        with patch.dict(os.environ, {"jlapi": "test-secret"}, clear=True), patch("paratera_chat.subprocess.Popen", side_effect=lambda *a, **k: _Process(opener)):
            payload, evidence = ParateraChatCompletions().complete("DeepSeek-V4-Pro-0813", "high", [{"role": "user", "content": "probe"}])
        self.assertEqual(evidence.telemetry["request_reasoning_effort"], "high")
        self.assertEqual(evidence.telemetry["model_id"], "DeepSeek-V4-Pro-0813")
        self.assertEqual(evidence.request_id, "req-test")
        self.assertEqual(evidence.response_id, "resp-test")
        self.assertEqual(evidence.usage["prompt_tokens"], 2)
        self.assertEqual(payload["model"], "DeepSeek-V4-Pro-0813")

    def test_wrong_model_and_effort_fail_closed_before_network(self):
        with patch.dict(os.environ, {"jlapi": "test-secret"}, clear=True), patch("urllib.request.urlopen") as open_mock:
            client = ParateraChatCompletions()
            with self.assertRaisesRegex(ProviderError, "MODEL_NOT_ALLOWLISTED"):
                client.complete("DeepSeek-V4-Flash", "low", [])
            with self.assertRaisesRegex(ProviderError, "EFFORT_NOT_ALLOWLISTED"):
                client.complete("DeepSeek-V4-Pro", "medium", [])
            open_mock.assert_not_called()

    def test_http_error_is_bounded_and_secret_redacted(self):
        secret = "never-print-this-token"
        error = urllib.error.HTTPError("https://example.invalid", 400, "bad", {}, io.BytesIO(json.dumps({"error": {"code": "bad_tool_schema", "message": "bad " + secret}}).encode()))
        opener = lambda *a, **k: (_ for _ in ()).throw(error)
        with patch.dict(os.environ, {"jlapi": secret}, clear=True), patch("paratera_chat.subprocess.Popen", side_effect=lambda *a, **k: _Process(opener)):
            with self.assertRaises(ProviderError) as caught:
                ParateraChatCompletions().complete("Qwen3.8-Max", "high", [{"role": "user", "content": "test"}])
        self.assertIn("bad_tool_schema", caught.exception.telemetry["http_error_body_redacted"])
        self.assertNotIn(secret, str(caught.exception))


if __name__ == "__main__":
    unittest.main()
