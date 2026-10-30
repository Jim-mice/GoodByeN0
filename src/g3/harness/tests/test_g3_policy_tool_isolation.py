import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FACTORIAL = ROOT / "factorial_harness"
sys.path.insert(0, str(FACTORIAL))

from experiment_policy import InformationRouter, Treatment, treatment_tools, validate_schema_factorial
from policy_tool_bridge import PolicyToolBridge


def base_tools():
    return json.loads((ROOT / "protocol" / "TOOL_SCHEMAS.json").read_text(encoding="utf-8"))["tools"]


def test_nine_policy_schemas_have_no_expert_or_standalone_profile_tool():
    schemas = validate_schema_factorial(base_tools())
    for tools in schemas.values():
        names = [tool["function"]["name"] for tool in tools]
        assert names[:3] == ["read_workspace_file", "edit_candidate", "query_candidate_history"]
        assert "query_expert_knowledge" not in names
        assert "query_candidate_profile" not in names
    g3 = treatment_tools(base_tools(), Treatment("1", "1"))
    assert [x["function"]["name"] for x in g3] == [
        "read_workspace_file", "edit_candidate", "query_candidate_history", "query_hardware_skill"]
    assert "profile" in g3[2]["function"]["parameters"]["properties"]["fields"]["items"]["enum"]
    p0 = treatment_tools(base_tools(), Treatment("1", "0"))
    assert "profile" not in p0[2]["function"]["parameters"]["properties"]["fields"]["items"]["enum"]


class FakeHistory:
    def call(self, tool_name, arguments, **kwargs):
        ids = arguments["candidate_ids"]
        fields = arguments["fields"]
        return {"status": "OK", "data": {"episode_id": kwargs["episode_id"],
            "candidate_ids": ids, "fields": fields,
            "records": [{"candidate_id": cid, "evaluation": {"verdict": "VALID_MEASUREMENT"},
                         "profile": {"untrusted": True}} for cid in ids]}}


def router(level, hardware_service=None):
    metrics = {"gpu__time_duration.sum": [{"unit": "ms", "value": 0.25}]}
    receipt = {"receipt_sha256": "a" * 64}
    query = (lambda text: hardware_service.query_hardware_skill({"query": text})) if hardware_service else (lambda _: {})
    validate_hardware = None
    if hardware_service:
        from experiment_policy import make_r5_hardware_validator
        validate_hardware = make_r5_hardware_validator(hardware_service.hardware_sha256)
    return InformationRouter(Treatment("1", level), hardware_query=query,
        profile_lookup=lambda cid: receipt if cid == "C0001" else None,
        profile_validator=lambda rec, cid: {"status": "PASS", "candidate_id": cid,
            "episode_id": "E1", "request_id": "R1", "candidate_sha256": "b" * 64,
            "oj_receipt_sha256": "c" * 64, "metrics": metrics},
        profile_status_lookup=lambda _: "COLLECTION_FAILED", hardware_validator=validate_hardware)


def call_history(level, fields):
    bridge = PolicyToolBridge(ordinary_tool_server=FakeHistory(), information_router=router(level))
    return bridge.call("query_candidate_history", {"candidate_ids": ["C0001"], "fields": fields},
        episode_id="E1", invocation_id="I1", turn_id="T2", request_id="P2", response_id="P2",
        tool_call_id="TC1", visible_content_sha256s=[], current_candidate_id="C0002")


def test_profile_only_flows_through_authorized_history_field():
    result = call_history("1", ["evaluation", "profile"])
    profile = result["data"]["records"][0]["profile"]
    assert profile["status"] == "OK"
    assert profile["metrics"]["gpu__time_duration.sum"][0]["value"] == 0.25
    assert profile.get("untrusted") is None
    from n0_autonomous.engine import InvocationTranscript, KnowledgeService, build_first_request_messages, build_provider_request_body, sha256_text
    from n0_autonomous.prompt_bundle import load_prompt_bundle
    bundle = load_prompt_bundle(ROOT)
    fixture = ROOT / "tests" / "fixtures" / "N0_AUTONOMOUS_INFORMATION_ACQUISITION_R4" / "template"
    source = (fixture / "candidate.cu").read_text(encoding="utf-8")
    footprint = json.dumps({"previously_retrieved": [], "last_history_query": None}, sort_keys=True, separators=(",", ":"))
    history = {"format": "EPISODE_STATE_V1", "episode_id": "E1", "task_id": "KB_L1_40_LAYERNORM",
        "current_candidate_id": "C0002", "workspace_sha256": "a" * 64, "best_candidate": "C0001",
        "settled_candidates": [], "previously_retrieved": [], "last_history_query": None,
        "retrieval_footprint_note": "past successful returns only", "working_note": None,
        "working_note_sha256": None}
    initial = build_first_request_messages(bundle.system_prompt, bundle.task_user_message,
        task_definition=(fixture / "task_definition.json").read_text(encoding="utf-8"),
        candidate_abi=(fixture / "candidate_abi.json").read_text(encoding="utf-8"),
        candidate_source=source, candidate_sha256=sha256_text(source), visible_observation=json.dumps({"score": 1.0}),
        history_brief=json.dumps(history), previously_retrieved=footprint, working_note=None, tools=bundle.tools)
    transcript = InvocationTranscript(initial)
    knowledge = KnowledgeService(ROOT / "knowledge" / "hardware" / "atoms_v2.json")
    bridge = PolicyToolBridge(ordinary_tool_server=FakeHistory(), information_router=router("1", knowledge))
    hw_args = {"query": "compute capability 12.0 maximum threads per block"}
    hw_result = bridge.call("query_hardware_skill", hw_args, episode_id="E1", invocation_id="I1", turn_id="T2",
        request_id="P2", response_id="P2", tool_call_id="TH1", current_candidate_id="C0002")
    hist_args = {"candidate_ids": ["C0001"], "fields": ["profile"]}
    transcript.append_assistant({"role": "assistant", "tool_calls": [{"id": "TH1", "type": "function",
        "function": {"name": "query_hardware_skill", "arguments": json.dumps(hw_args)}}, {"id": "TC1", "type": "function",
        "function": {"name": "query_candidate_history", "arguments": json.dumps({"candidate_ids": ["C0001"],
        "fields": ["profile"]})}}]})
    transcript.append_tool_result("TH1", json.dumps(hw_result))
    actual_history = bridge.call("query_candidate_history", hist_args, episode_id="E1", invocation_id="I1", turn_id="T2",
        request_id="P2", response_id="P2", tool_call_id="TC1", visible_content_sha256s=[], current_candidate_id="C0002")
    transcript.append_tool_result("TC1", json.dumps(actual_history))
    next_body = build_provider_request_body(transcript.messages, tools=bundle.tools)
    next_request = json.dumps(next_body, ensure_ascii=False)
    tool_messages = [m for m in next_body["messages"] if m.get("role") == "tool"]
    delivered = "\n".join(m["content"] for m in tool_messages)
    assert "gpu__time_duration.sum" in delivered
    assert '"value": 0.25' in delivered
    assert "cc12.max_threads_per_block" in delivered
    assert "query_candidate_profile" not in next_request


def test_profile_policy_no_information_and_no_profile_authorization():
    no_info = call_history("0.5", ["profile"])
    assert no_info["data"]["records"][0]["profile"]["status"] == "NO_RELEVANT_INFORMATION"
    try:
        call_history("0", ["profile"])
    except ValueError as exc:
        assert str(exc) == "PROFILE_FIELD_NOT_AUTHORIZED"
    else:
        raise AssertionError("P0 profile access was not rejected")


def test_common_workspace_and_observation_scrubber_removes_profile_sidechannels():
    from experiment_policy import scrub_common_information
    source = {"profile": {"metrics": {"gpu__time_duration.sum": 1}},
              "visible_observation": {"profile_status": "PASS", "ncu_metrics": {"secret": 1}},
              "score": 1.1}
    clean = scrub_common_information(source)
    assert clean == {"score": 1.1, "visible_observation": {}}
