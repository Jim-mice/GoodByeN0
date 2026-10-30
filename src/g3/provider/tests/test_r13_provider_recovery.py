from __future__ import annotations

import hashlib
import importlib.util
import io
import json
import os
import sys
import tempfile
import unittest
from http.client import IncompleteRead
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PROVIDER = ROOT / "provider_staging"
sys.path.insert(0, str(PROVIDER))
sys.path.insert(0, str(ROOT))
# R11 and R13 intentionally contain same-named Provider modules. unittest
# discovery order must not silently bind these tests to the R11 copy.
sys.modules.pop("paratera_http_worker", None)
sys.modules.pop("paratera_chat", None)
def _load_local(module_name: str, path: Path):
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module
http_worker = _load_local("r13_paratera_http_worker", PROVIDER / "paratera_http_worker.py")
chat_module = _load_local("paratera_chat", PROVIDER / "paratera_chat.py")
CompletionEvidence = chat_module.CompletionEvidence
ParateraChatCompletions = chat_module.ParateraChatCompletions
from canonical_harness.paratera_agent import MAX_AGENT_REQUESTS_PER_TURN, ParateraAgentAdapter
from canonical_harness.recovery_api import reconcile_provider_attempt


class FakeResponse:
    def __init__(self, chunks, headers):
        self.chunks = list(chunks)
        self.headers = headers
        self.status = 200
        self.closed = False

    def getcode(self):
        return self.status

    def read(self, size=-1):
        if not self.chunks:
            return b""
        value = self.chunks.pop(0)
        if isinstance(value, Exception):
            raise value
        return value

    def close(self):
        self.closed = True


class FakeWorkerProcess:
    def __init__(self, returncode=37, stdout_text="", stderr_text=""):
        self.returncode = returncode
        self.stdout = io.StringIO(stdout_text)
        self.stderr = io.StringIO(stderr_text)
        self.stdin = io.StringIO()
        self.pid = 424242
        self.args = ["fake-worker"]

    def wait(self, timeout=None):
        return self.returncode

    def kill(self):
        self.returncode = -9


class R13ProviderRecoveryTests(unittest.TestCase):
    def setUp(self):
        os.environ["jlapi"] = "r13-test-secret-never-write"

    def tearDown(self):
        os.environ.pop("jlapi", None)

    def test_declared_length_body_survives_incomplete_read_after_all_bytes(self):
        body = json.dumps({"id": "resp-complete", "model": "DeepSeek-V4-Pro",
                           "choices": [{"message": {"role": "assistant", "content": "ok"},
                                        "finish_reason": "stop"}]}).encode()
        split = len(body) // 2
        response = FakeResponse([body[:split], IncompleteRead(body[split:], len(body) - split)],
                                {"Content-Length": str(len(body)), "x-request-id": "provider-r"})
        with tempfile.TemporaryDirectory() as td:
            artifact_path = Path(td) / "attempt.response.json"
            logical = {}
            logical_sha = hashlib.sha256(json.dumps(logical, separators=(",", ":")).encode()).hexdigest()
            result = http_worker.perform({"url": "https://example.invalid", "body": logical,
                                          "attempt_id": "attempt-a", "request_sha256": logical_sha,
                                          "response_artifact_path": str(artifact_path)},
                                         opener=lambda *a, **k: response)
            self.assertEqual(result["terminal_status"], "RESPONSE_RECEIVED")
            self.assertTrue(result["telemetry"]["body_complete"])
            self.assertTrue(result["telemetry"]["read_exception_after_declared_length"])
            self.assertEqual(result["payload"]["id"], "resp-complete")
            self.assertTrue(artifact_path.is_file())
            artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
            self.assertEqual(artifact["body_sha256"], hashlib.sha256(body).hexdigest())
            self.assertEqual(artifact["body_bytes_received"], len(body))
            self.assertEqual(artifact["request_sha256"], logical_sha)
            self.assertNotIn("r13-test-secret-never-write", artifact_path.read_text(encoding="utf-8"))

    def test_parent_recovers_complete_response_when_terminal_pipe_is_lost(self):
        model = "DeepSeek-V4-Pro"
        effort = "high"
        messages = [{"role": "user", "content": "test"}]
        body = ParateraChatCompletions.build_body(model, effort, messages, max_tokens=128)
        request_sha = ParateraChatCompletions.request_sha256(model, effort, messages, max_tokens=128)
        payload = {"id": "resp-sidecar", "model": model,
                   "choices": [{"message": {"role": "assistant", "content": "recovered"},
                                "finish_reason": "stop"}], "usage": {"completion_tokens": 2}}
        raw = json.dumps(payload, separators=(",", ":")).encode()
        with tempfile.TemporaryDirectory() as td:
            artifact_path = Path(td) / "attempt.response.json"
            worker_result = http_worker.perform({"url": "https://example.invalid", "body": body,
                                                  "attempt_id": "attempt-sidecar", "request_sha256": request_sha,
                                                  "response_artifact_path": str(artifact_path)},
                                                opener=lambda *a, **k: FakeResponse([raw], {"Content-Length": str(len(raw)), "x-request-id": "prov-123"}))
            self.assertEqual(worker_result["terminal_status"], "RESPONSE_RECEIVED")
            original_popen = chat_module.subprocess.Popen
            chat_module.subprocess.Popen = lambda *a, **k: FakeWorkerProcess(returncode=37, stdout_text="", stderr_text="pipe ended")
            try:
                client = ParateraChatCompletions(timeout_seconds=3600)
                returned, evidence = client.complete(model, effort, messages, max_tokens=128,
                                                     attempt_id="attempt-sidecar", response_artifact_path=artifact_path)
            finally:
                chat_module.subprocess.Popen = original_popen
            self.assertEqual(returned["id"], "resp-sidecar")
            self.assertEqual(evidence.request_id, "prov-123")
            self.assertTrue(evidence.telemetry["response_handoff_recovered"])
            self.assertEqual(evidence.telemetry["helper_returncode"], 37)
            self.assertFalse(evidence.telemetry["helper_terminal_event_received"])

    def test_incomplete_artifact_is_diagnostic_and_never_recoverable(self):
        body = b'{"choices":'
        with tempfile.TemporaryDirectory() as td:
            artifact_path = Path(td) / "partial.response.json"
            logical = {}
            logical_sha = hashlib.sha256(json.dumps(logical, separators=(",", ":")).encode()).hexdigest()
            result = http_worker.perform({"url": "https://example.invalid", "body": logical,
                                          "attempt_id": "attempt-partial", "request_sha256": logical_sha,
                                          "response_artifact_path": str(artifact_path)},
                                         opener=lambda *a, **k: FakeResponse([body, IncompleteRead(b"", 10)],
                                                                            {"Content-Length": "20"}))
            self.assertEqual(result["terminal_status"], "HTTP_BODY_INCOMPLETE")
            self.assertFalse(result["telemetry"]["body_complete"])
            record = json.loads(artifact_path.read_text(encoding="utf-8"))
            self.assertEqual(record["body_sha256"], hashlib.sha256(body).hexdigest())
            with self.assertRaisesRegex(ValueError, "BODY_INCOMPLETE"):
                ParateraChatCompletions._load_response_artifact(artifact_path, "attempt-partial", logical_sha)

    def test_recovery_reconciles_verified_response_sidecar_without_retry(self):
        model = "DeepSeek-V4-Pro"; effort = "high"
        messages = [{"role":"user","content":"saved original request"}]
        request_sha = ParateraChatCompletions.request_sha256(model, effort, messages, max_tokens=128)
        payload = {"id":"resp-reconcile","model":model,"choices":[{"message":{"role":"assistant","content":"saved"},"finish_reason":"stop"}]}
        body = json.dumps(payload, separators=(",", ":")).encode()
        with tempfile.TemporaryDirectory() as td:
            episode = Path(td) / "episode"; trace = episode / "provider_trace"
            artifact_dir = trace / "http_responses"; artifact_dir.mkdir(parents=True)
            (trace / "provider_attempts.jsonl").write_text(json.dumps({"event":"REQUEST_START","attempt_id":"att-reconcile","request_sha256":request_sha,"messages":messages})+"\n",encoding="utf-8")
            result = http_worker.perform({"url":"https://example.invalid","body":ParateraChatCompletions.build_body(model,effort,messages,max_tokens=128),
                                          "attempt_id":"att-reconcile","request_sha256":request_sha,
                                          "response_artifact_path":str(artifact_dir / "att-reconcile.response.json")},
                                         opener=lambda *a,**k: FakeResponse([body],{"Content-Length":str(len(body))}))
            self.assertEqual(result["terminal_status"],"RESPONSE_RECEIVED")
            recovered = reconcile_provider_attempt(episode,"att-reconcile")
            self.assertEqual(recovered["status"],"SAVED_RESPONSE_AVAILABLE")
            self.assertEqual(recovered["source"],"VERIFIED_HTTP_RESPONSE_SIDECAR")
            self.assertEqual(recovered["attempt_record"]["raw_response"]["id"],"resp-reconcile")

    def test_h_retrieval_tool_loop_exceeds_old_cap_and_stays_bounded(self):
        self.assertGreater(MAX_AGENT_REQUESTS_PER_TURN, 8)
        with tempfile.TemporaryDirectory() as td:
            episode = Path(td) / "DSV4_HR_CPU_TEST"
            workspace = episode / "model_workspace"
            workspace.mkdir(parents=True)
            files = {"task.md": "task", "candidate.cu": "seed",
                     "visible_observation.json": "{}", "visible_history.json": "{}",
                     "auxiliary_context.txt": "context", "tool_usage_guide.md": "optional tool guide"}
            for name, content in files.items():
                (workspace / name).write_text(content, encoding="utf-8")
            agent = ParateraAgentAdapter("DeepSeek-V4-Pro-0813")
            calls = {"n": 0}

            def fake_complete(model, effort, messages, **kwargs):
                calls["n"] += 1
                index = calls["n"]
                if index <= 6:
                    target = list(files)[index - 1]
                    fn = "read_workspace_file"
                    args = {"relative_path": target}
                elif index <= 15:
                    fn = "query_hardware_skill"
                    args = {"query": "target GPU memory capacity"}
                else:
                    fn = None
                    args = None
                if fn:
                    message = {"role": "assistant", "tool_calls": [{"id": "tc-" + str(index),
                               "type": "function", "function": {"name": fn,
                               "arguments": json.dumps(args)}}]}
                    finish = "tool_calls"
                else:
                    message = {"role": "assistant", "content": "STOP"}
                    finish = "stop"
                response = {"id": "response-" + str(index), "model": model,
                            "choices": [{"message": message, "finish_reason": finish}],
                            "usage": {"completion_tokens": 3}}
                evidence = CompletionEvidence("provider-id", response["id"], model, model, effort,
                                              response["usage"], response,
                                              {"http_status": 200, "body_complete": True,
                                               "response_bytes_received": len(json.dumps(response))})
                return response, evidence

            agent.client.complete = fake_complete
            result, metadata = agent(workspace, ("query_hardware_skill",), "read visible guide")
            self.assertEqual(result, "STOP")
            self.assertEqual(calls["n"], 16)
            self.assertEqual(len(metadata["provider_requests"]), 16)
            self.assertTrue(metadata["mcp_initialized"])
            query_events = [x for x in metadata["provider_tool_events"] if x.get("tool_name") == "query_hardware_skill"]
            self.assertEqual(len(query_events), 9)
            self.assertTrue(all(x.get("status") == "completed" for x in query_events))
            loop_checkpoint = json.loads(Path(metadata["agent_loop_checkpoint"]).read_text(encoding="utf-8"))
            self.assertEqual(loop_checkpoint["status"], "COMPLETE")
            self.assertLessEqual(loop_checkpoint["provider_requests_used"], 32)


if __name__ == "__main__":
    unittest.main()
