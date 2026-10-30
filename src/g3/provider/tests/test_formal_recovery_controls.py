"""CPU-only checks for treatment-scoped tool schemas and durable failed-call evidence."""
from __future__ import annotations

import json
import os
import subprocess
import tempfile
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from canonical_harness import paratera_agent as module
from canonical_harness.paratera_agent import ParateraAgentAdapter
from canonical_harness.v7_evaluator_adapter import V7EvaluatorAdapter
from canonical_harness.observation_builder import build_observation
from canonical_harness.schemas import State, Treatment
import paratera_chat as chat_module
from paratera_chat import CompletionEvidence, ProviderError, ParateraChatCompletions


class FakeClient:
    def __init__(self, calls):
        self.calls = calls
        self.index = 0

    def complete(self, model, effort, messages, tools=None, tool_choice=None, max_tokens=512):
        call_index = self.index
        self.index += 1
        if call_index == 1:
            raise ProviderError("PARATERA_TRANSPORT_FAILURE")
        return ({"model": model, "choices": [{"finish_reason": "tool_calls", "message": {"role": "assistant", "tool_calls": self.calls}}]},
                CompletionEvidence("provider-request-test", "response-test", model, model, effort,
                                  {"prompt_tokens": 1, "completion_tokens": 1}, {}))


class FakeMCP:
    def __init__(self, server):
        pass

    def initialize(self):
        return {"serverInfo": {"name": "novelty-v7-hardware-skill"}, "capabilities": {"tools": {}}}

    def list_tools(self):
        return [{"name": "query_hardware_skill", "inputSchema": {"type": "object", "required": ["query"]}}]

    def close(self):
        pass


def read_calls(paths):
    return [{"id": f"call-{i}", "type": "function", "function": {
        "name": "read_workspace_file", "arguments": json.dumps({"relative_path": path})}}
        for i, path in enumerate(paths)]


def run_case(treatment: str, paths: list[str]) -> tuple[dict, list[dict]]:
    with tempfile.TemporaryDirectory(prefix="v7_provider_attempt_") as temp:
        base = Path(temp)
        workspace = base / "workspace"
        workspace.mkdir()
        for name in ["task.md", "candidate.cu", "visible_observation.json", "visible_history.json", "auxiliary_context.txt", "tool_usage_guide.md"]:
            (workspace / name).write_text("fixture " + name, encoding="utf-8")
        tools = ("query_hardware_skill",) if treatment == "H_RETRIEVAL_V7" else ()
        adapter = ParateraAgentAdapter.__new__(ParateraAgentAdapter)
        adapter.model = "DeepSeek-V4-Pro"
        adapter.force_mcp_first = False
        adapter.client = FakeClient(read_calls(paths))
        old_mcp = module.MCPStdioClient
        module.MCPStdioClient = FakeMCP
        try:
            try:
                adapter(workspace, tools, "read required workspace files")
            except ProviderError as exc:
                assert str(exc) == "PARATERA_TRANSPORT_FAILURE"
            else:
                raise AssertionError("expected second provider call failure")
        finally:
            module.MCPStdioClient = old_mcp
        failure_path = next((base / "provider_trace").glob("failure_*.json"))
        failure = json.loads(failure_path.read_text(encoding="utf-8"))
        attempt_events = [json.loads(line) for line in (base / "provider_trace" / "provider_attempts.jsonl").read_text(encoding="utf-8").splitlines()]
        return failure, failure["requests"][0]["tools"], attempt_events


def test_treatment_scoped_file_schema():
    g_tools = ParateraAgentAdapter.__new__(ParateraAgentAdapter)._tools((), [])[0]["function"]["parameters"]["properties"]["relative_path"]["enum"]
    h_tools = ParateraAgentAdapter.__new__(ParateraAgentAdapter)._tools(("query_hardware_skill",), [])[0]["function"]["parameters"]["properties"]["relative_path"]["enum"]
    assert "tool_usage_guide.md" not in g_tools
    assert "tool_usage_guide.md" in h_tools


def test_failed_provider_call_is_durable_and_credentials_absent():
    previous = os.environ.get("jlapi")
    os.environ["jlapi"] = "TEST_ONLY_CREDENTIAL_SENTINEL"
    try:
        failure, tools, attempt_events = run_case("G_CONTROL", ["task.md", "candidate.cu", "visible_observation.json", "visible_history.json", "auxiliary_context.txt"])
        serialized = json.dumps(failure, ensure_ascii=False)
        assert failure["error"] == "PARATERA_TRANSPORT_FAILURE"
        assert len(failure["requests"]) == 2
        assert failure["requests"][0]["status"] == "RESPONSE_RECEIVED"
        assert failure["requests"][1]["status"] == "NO_RESPONSE"
        assert failure["requests"][1]["outcome_unknown_if_transport_fails"] is True
        provider_events = [event["event"] for event in attempt_events if event["event"] in {"REQUEST_START", "RESPONSE_RECEIVED", "REQUEST_NO_RESPONSE"}]
        assert provider_events == ["REQUEST_START", "RESPONSE_RECEIVED", "REQUEST_START", "REQUEST_NO_RESPONSE"]
        assert any(event.get("event") == "TOOL_ACTION_COMPLETED" for event in attempt_events)
        assert "tool_usage_guide.md" not in tools[0]["function"]["parameters"]["properties"]["relative_path"]["enum"]
        assert "TEST_ONLY_CREDENTIAL_SENTINEL" not in serialized
    finally:
        if previous is None:
            os.environ.pop("jlapi", None)
        else:
            os.environ["jlapi"] = previous


def test_retrieval_guide_read_and_schema_are_scoped():
    failure, tools, _ = run_case("H_RETRIEVAL_V7", ["task.md", "candidate.cu", "visible_observation.json",
                                                      "visible_history.json", "auxiliary_context.txt",
                                                      "tool_usage_guide.md"])
    assert "tool_usage_guide.md" in tools[0]["function"]["parameters"]["properties"]["relative_path"]["enum"]
    guide_events = [event for event in failure["tool_events"]
                    if event.get("arguments", {}).get("relative_path") == "tool_usage_guide.md"]
    assert len(guide_events) == 1 and guide_events[0].get("status") == "completed"
    assert any(tool.get("function", {}).get("name") == "query_hardware_skill" for tool in tools)


def test_formal_runner_does_not_import_v5_seed_score():
    runner_source = (ROOT / "run_deepseek_formal_episode.py").read_text(encoding="utf-8")
    assert "canonical_seed_fixture.json" not in runner_source
    assert '"C0000_SEED_BASELINE"' in runner_source
    assert '"CANONICAL_SEED_BASELINE_NOT_VALID:"' in runner_source


def test_v7_profile_list_normalizes_before_model_visible_feedback():
    raw = {"verdict": "VALID_MEASUREMENT", "request_id": "REQ-CPU-TEST",
           "candidate_sha256": "a" * 64, "aggregate_speedup": 0.5,
           "measurement_repetition_count": 5, "score_aggregation": "K5_TEST",
           "target_gpu_uuid": "GPU-TEST", "shape_results": [],
           "profiling": [{"shape": [1, 128], "kernel": {"duration_us": 2.5,
                         "duration_ns": 2500}}, {"shape": [1, 256],
                         "kernel": {"duration_us": 3.5, "duration_ns": 3500}}]}
    normalized = V7EvaluatorAdapter(Path("." )).normalize(raw)
    assert isinstance(normalized["profile"], dict)
    assert len(normalized["profile"]["per_shape"]) == 2
    state = State("", Treatment("G_CONTROL"), "a" * 64, "a" * 64,
                  0.5, 0.5, incumbent_profile=normalized["profile"],
                  best_profile=normalized["profile"], seed_result=normalized)
    observation = build_observation(state)
    assert observation["best"]["aggregate_score"] == 0.5
    assert observation["best"]["profile"]["per_shape"][0]["kernel"]["duration_us"] == 2.5


def test_provider_timeout_allows_long_high_effort_tool_followup_without_network():
    seen = {}
    old_popen = chat_module.subprocess.Popen
    previous = os.environ.get("jlapi")
    os.environ["jlapi"] = "TEST_ONLY_CREDENTIAL_SENTINEL"
    class FakeProcess:
        pid = 12345
        returncode = 0
        class Input:
            def write(self, value): seen["body"] = json.loads(value)["body"]
            def flush(self): pass
            def close(self): pass
        stdin = Input()
        stdout = [json.dumps({"event":"TERMINAL","terminal_status":"RESPONSE_RECEIVED",
                              "payload":{"id":"cpu-test","model":"DeepSeek-V4-Pro",
                                         "choices":[{"message":{"role":"assistant","content":"ok"}}],
                                         "usage":{"completion_tokens":1}},
                              "response_headers":{"x-request-id":"request-test"},
                              "telemetry":{"http_status":200,"time_to_first_byte":0.2,
                                           "response_bytes_received":42,"response_id":"cpu-test"}})]
        def wait(self, timeout=None):
            seen["timeout"] = timeout
            return 0
    chat_module.subprocess.Popen = lambda *args, **kwargs: FakeProcess()
    try:
        client = ParateraChatCompletions(timeout_seconds=3600)
        payload, evidence = client.complete("DeepSeek-V4-Pro", "high", [{"role":"user","content":"cpu-only"}], max_tokens=32768)
        assert seen["timeout"] == 3600
        assert seen["body"]["max_tokens"] == 32768
        assert evidence.telemetry["request_sha256"] == client.request_sha256("DeepSeek-V4-Pro", "high", [{"role":"user","content":"cpu-only"}], max_tokens=32768)
        assert evidence.telemetry["http_status"] == 200
        assert payload["id"] == "cpu-test"
        assert module.PROVIDER_TIMEOUT_SECONDS == 3600
    finally:
        chat_module.subprocess.Popen = old_popen
        if previous is None: os.environ.pop("jlapi", None)
        else: os.environ["jlapi"] = previous


if __name__ == "__main__":
    test_treatment_scoped_file_schema()
    test_failed_provider_call_is_durable_and_credentials_absent()
    test_retrieval_guide_read_and_schema_are_scoped()
    test_formal_runner_does_not_import_v5_seed_score()
    test_v7_profile_list_normalizes_before_model_visible_feedback()
    test_provider_timeout_allows_long_high_effort_tool_followup_without_network()
    print("FORMAL_RECOVERY_CONTROLS_PASSED=6 FORMAL_RECOVERY_CONTROLS_FAILED=0")
