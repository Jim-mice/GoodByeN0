from __future__ import annotations

import base64
import hashlib
import json
import os
import sys
import tempfile
import unittest
from http.client import IncompleteRead
from pathlib import Path
from urllib.error import URLError

ROOT = Path(__file__).resolve().parents[1]
PROVIDER = ROOT / "provider_staging"
sys.path.insert(0, str(PROVIDER))
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "canonical_harness"))

import paratera_http_worker as http_worker
from campaign_extension import allocate_next_replicate, allocate_recovery_id
from paratera_chat import CompletionEvidence
sys.path.insert(0,str(Path(r"C:\NOVELTY_V7_SHARED\staging\v7_oj_release") / "implementation"))
import oj_v7_core
from result_recovery import read_persisted_result
from recovery_api import inspect_recovery_checkpoint, reconcile_provider_attempt, resume_from_saved_response, resume_from_saved_request, record_episode_recovery_event
from paratera_agent import ParateraAgentAdapter
from canonical_harness.treatment import load_treatments


class FakeResponse:
    def __init__(self, chunks, headers=None, status=200):
        self.chunks = list(chunks); self.headers = headers or {}; self.status = status; self.closed = False
    def getcode(self): return self.status
    def read(self, size=-1):
        if not self.chunks: return b""
        item = self.chunks.pop(0)
        if isinstance(item, Exception): raise item
        return item
    def close(self): self.closed = True


class R11ProviderQueueRecoveryTests(unittest.TestCase):
    def setUp(self): os.environ["jlapi"] = "test-secret-never-log"
    def tearDown(self): os.environ.pop("jlapi", None)

    @staticmethod
    def fixed_clock(values):
        items=iter(values)
        return lambda: next(items)

    def test_600_second_upstream_disconnect_is_transport_diagnostic(self):
        def fail(request, timeout):
            self.assertEqual(timeout, 3600)
            raise URLError(ConnectionResetError(10054, "connection reset by peer"))
        result = http_worker.perform({"url":"https://example.invalid/chat/completions","body":{},"timeout_seconds":3600}, opener=fail,clock=self.fixed_clock([0.0,600.125]))
        self.assertEqual(result["terminal_status"], "TRANSPORT_FAILURE_OUTCOME_UNKNOWN")
        self.assertEqual(result["telemetry"]["exception_class"], "URLError")
        self.assertEqual(result["telemetry"]["exception_errno"], 10054)
        self.assertIsNone(result["telemetry"]["http_status"])
        self.assertEqual(result["telemetry"]["elapsed_seconds"],600.125)
        self.assertNotIn("test-secret-never-log", json.dumps(result))

    def test_http_200_partial_body_is_never_success(self):
        body = b'{"debug":"test-secret-never-log","choices":['
        response = FakeResponse([body, IncompleteRead(b"tail", 40)], {"Content-Length":"40"})
        emitted=[]
        result = http_worker.perform({"url":"https://example.invalid","body":{}}, opener=lambda *a, **k: response,clock=self.fixed_clock([0.0,1.0,2.0,2.05,2.1]),emit=emitted.append)
        self.assertEqual(result["terminal_status"], "HTTP_BODY_INCOMPLETE")
        self.assertEqual(result["telemetry"]["http_status"], 200)
        self.assertFalse(result["telemetry"]["body_complete"])
        self.assertEqual(result["telemetry"]["content_length_if_present"],"40")
        self.assertEqual(result["telemetry"]["body_bytes_received"],len(body)+4)
        self.assertIsNotNone(result["telemetry"]["response_headers_received_at"])
        self.assertIsNotNone(result["telemetry"]["first_body_byte_at"])
        self.assertEqual(base64.b64decode(result["partial_body_base64"]), body.replace(b"test-secret-never-log",b"[REDACTED]") + b"tail")
        self.assertNotIn("test-secret-never-log",json.dumps(emitted))

    def test_complete_buffered_tool_call_response(self):
        tool_args=json.dumps({"relative_path":"task.md"})
        payload = {"id":"resp-1","choices":[{"finish_reason":"tool_calls","message":{"tool_calls":[{"id":"call-1","type":"function","function":{"name":"read_workspace_file","arguments":tool_args}}]}}]}
        raw = json.dumps(payload).encode()
        response = FakeResponse([raw], {"Content-Length":str(len(raw))})
        result = http_worker.perform({"url":"https://example.invalid","body":{}}, opener=lambda *a, **k: response,clock=self.fixed_clock([0.0,601.0,601.1,601.2,601.3]))
        self.assertEqual(result["terminal_status"], "RESPONSE_RECEIVED")
        self.assertEqual(result["telemetry"]["json_parse_status"], "PASS")
        self.assertTrue(result["telemetry"]["body_complete"])
        self.assertEqual(result["payload"]["choices"][0]["finish_reason"], "tool_calls")
        self.assertEqual(json.loads(result["payload"]["choices"][0]["message"]["tool_calls"][0]["function"]["arguments"])["relative_path"],"task.md")
        self.assertGreater(result["telemetry"]["elapsed_seconds"],600)

    def test_http_timeout_ceiling_is_3600_and_higher_values_rejected(self):
        observed = {}
        def opener(request, timeout): observed["timeout"] = timeout; return FakeResponse([b"{}"], {"Content-Length":"2"})
        http_worker.perform({"url":"https://example.invalid","body":{},"timeout_seconds":3600}, opener=opener)
        self.assertEqual(observed["timeout"], 3600)
        self.assertEqual(http_worker.perform({"url":"x","body":{},"timeout_seconds":3601})["terminal_status"], "TIMEOUT_CONFIGURATION_INVALID")

    def test_all_four_treatments_share_the_same_r14_provider_configuration(self):
        config=json.loads((ROOT/"configs"/"model_request_config_v7.json").read_text(encoding="utf-8"))
        treatments=load_treatments(ROOT)
        self.assertEqual(set(treatments),{"G_CONTROL","H_STATIC","H_RETRIEVAL_V7","E_EXPERT"})
        self.assertEqual(config["models"],["DeepSeek-V4-Pro"])
        self.assertEqual(config["requested_reasoning_effort"],"high")
        self.assertEqual(config["effective_reasoning_effort"],"UNVERIFIED")
        self.assertEqual(config["provider_timeout_seconds"],1800)
        self.assertEqual(config["http_connect_read_timeout_seconds"],1800)
        self.assertEqual(config["max_attempts_per_request"],3)
        self.assertEqual(config["max_replays"],2)
        self.assertEqual(config["total_http_wait_budget_seconds"],5400)
        self.assertEqual(config["max_tokens"],32768)
        self.assertEqual(len(set((config["api_base"],config["provider_timeout_seconds"],config["max_tokens"],config["requested_reasoning_effort"]))),4)

    def test_existing_queue_result_reads_identity_and_never_admits(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td); rid = "V7-abc"; sha = "a" * 64
            result_dir = root / "results" / rid; result_dir.mkdir(parents=True)
            (result_dir / "result.json").write_text(json.dumps({"state":"COMPLETED","request_id":rid,"candidate_sha256":sha,"score":1.0}), encoding="utf-8")
            self.assertEqual(read_persisted_result(root,rid,sha)["score"],1.0)
            with self.assertRaisesRegex(RuntimeError,"CANDIDATE_SHA_MISMATCH"):
                read_persisted_result(root,rid,"b"*64,wait_seconds=0)
            self.assertFalse((root / "queue").exists())

    def test_terminal_request_with_torn_result_fails_without_retrying_submission(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td); rid="V7-torn"; sha="e"*64
            request_dir=root/"requests"/rid; request_dir.mkdir(parents=True)
            (request_dir/"request.json").write_text(json.dumps({"request_id":rid,"candidate_sha256":sha,"state":"COMPLETED"}),encoding="utf-8")
            result_dir=root/"results"/rid; result_dir.mkdir(parents=True); (result_dir/"result.json").write_text("",encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError,"TERMINAL_REQUEST_MISSING_OR_INVALID_RESULT"):
                read_persisted_result(root,rid,sha,wait_seconds=3.0)

    def test_historical_hs_valid_result_recovers_by_original_id_without_resubmission(self):
        queue=Path(r"C:\NOVELTY_V7_RUNS\deepseek_first20\shared_queue")
        rid="V7-cdcb18d408be47f49c1abfa0d3485ab6"
        request=json.loads((queue/"requests"/rid/"request.json").read_text(encoding="utf-8"))
        candidate_sha=hashlib.sha256((queue/"requests"/rid/"candidate.cu").read_bytes()).hexdigest()
        self.assertEqual(candidate_sha,request["candidate_sha256"])
        result=read_persisted_result(queue,rid,candidate_sha,wait_seconds=0)
        self.assertEqual(result["verdict"],"VALID_MEASUREMENT")
        self.assertAlmostEqual(result["aggregate_speedup"],0.948589351140722)
        self.assertFalse((queue/"queue"/(rid+".json")).exists())

    def test_dispatcher_publishes_result_atomically_before_request_terminal_state(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td); rid="V7-publish"; sha="d"*64
            request_dir=root/"requests"/rid; request_dir.mkdir(parents=True)
            queue_dir=root/"queue"; queue_dir.mkdir()
            queue_file=queue_dir/(rid+".json"); queue_file.write_text(json.dumps({"request_id":rid}),encoding="utf-8")
            (request_dir/"request.json").write_text(json.dumps({"request_id":rid,"candidate_sha256":sha}),encoding="utf-8")
            cfg=oj_v7_core.DispatcherConfig(root=root,max_attempts=1,attempt_fn=lambda *a:{"verdict":"VALID_MEASUREMENT","request_id":rid,"candidate_sha256":sha},deadline=oj_v7_core.DeadlineConfig(max_environment_retries=1,per_attempt_soft_seconds=2,request_hard_deadline_seconds=3))
            outcome=oj_v7_core.dispatch_request(cfg,rid,{"request_id":rid,"candidate_sha256":sha},queue_file)
            result=read_persisted_result(root,rid,sha,wait_seconds=0)
            request=json.loads((request_dir/"request.json").read_text(encoding="utf-8"))
            self.assertEqual(outcome.state,"COMPLETED")
            self.assertEqual(result["state"],"COMPLETED")
            self.assertEqual(request["state"],"COMPLETED")
            self.assertEqual(list((root/"results"/rid).glob("*.tmp")),[])

    def test_saved_response_executes_through_agent_tool_executor_once(self):
        with tempfile.TemporaryDirectory() as td:
            episode = Path(td) / "DSV4_E_REP_001_REC07"; workspace = episode / "model_workspace"
            trace = episode / "provider_trace"; workspace.mkdir(parents=True); trace.mkdir()
            for name, text in (("task.md","task"),("candidate.cu","OLD"),("visible_observation.json","{}"),("visible_history.json","[]"),("auxiliary_context.txt","")):
                (workspace / name).write_text(text,encoding="utf-8")
            tool_calls = []
            for i, name in enumerate(("task.md","candidate.cu","visible_observation.json","visible_history.json","auxiliary_context.txt")):
                tool_calls.append({"id":f"read-{i}","type":"function","function":{"name":"read_workspace_file","arguments":json.dumps({"relative_path":name})}})
            tool_calls.append({"id":"edit-1","type":"function","function":{"name":"edit_candidate","arguments":json.dumps({"old_text":"OLD","new_text":"NEW"})}})
            record = {"attempt_id":"attempt-original","raw_response":{"choices":[{"message":{"role":"assistant","tool_calls":tool_calls}}]}}
            start = {"event":"REQUEST_START","attempt_id":"attempt-original","request_sha256":"f"*64,"messages":[{"role":"user","content":"resume"}]}
            response = {"event":"RESPONSE_RECEIVED",**record,"response_id":"response-1"}
            (trace / "provider_attempts.jsonl").write_text(json.dumps(start)+"\n"+json.dumps(response)+"\n",encoding="utf-8")
            agent = ParateraAgentAdapter("DeepSeek-V4-Pro")
            reconciled = reconcile_provider_attempt(episode,"attempt-original")
            self.assertEqual(reconciled["status"],"SAVED_RESPONSE_AVAILABLE")
            resumed = resume_from_saved_response(episode,workspace,"attempt-original",agent)
            self.assertEqual((workspace / "candidate.cu").read_text(encoding="utf-8"),"NEW")
            self.assertEqual(len(resumed["messages_for_next_model_turn"]),8)
            self.assertEqual(resumed["tool_events"][-1]["tool_name"],"edit_candidate")
            self.assertEqual(hashlib.sha256(Path(resumed["continuation_context_path"]).read_bytes()).hexdigest(),resumed["continuation_context_sha256"])
            def fake_complete(model,effort,messages,**kwargs):
                payload={"id":"next-response","model":model,"choices":[{"finish_reason":"stop","message":{"role":"assistant","content":"CONTINUE"}}],"usage":{"completion_tokens":1}}
                evidence=CompletionEvidence(None,"next-response",model,model,effort,payload["usage"],payload,{"http_status":200,"body_complete":True})
                return payload,evidence
            agent.client.complete=fake_complete
            continued=agent.continue_from_messages(workspace,(),resumed["messages_for_next_model_turn"],prior_tool_events=resumed["tool_events"])
            self.assertEqual(continued["final_text"],"CONTINUE")
            new_start=[json.loads(x) for x in (trace/"provider_attempts.jsonl").read_text(encoding="utf-8").splitlines() if json.loads(x).get("event")=="REQUEST_START"][-1]
            self.assertNotEqual(new_start["attempt_id"],"attempt-original")
            resumed_again = resume_from_saved_response(episode,workspace,"attempt-original",agent)
            self.assertEqual((workspace / "candidate.cu").read_text(encoding="utf-8"),"NEW")
            self.assertEqual(resumed_again["tool_messages"][-1]["tool_call_id"],"edit-1")
            self.assertEqual(resume_from_saved_request(episode,"attempt-original")["status"],"SAVED_RESPONSE_AVAILABLE")
            self.assertEqual(inspect_recovery_checkpoint(episode)["episode_id"],episode.name)
            event = record_episode_recovery_event(episode,{"event":"RECOVERY_CHECKPOINT","recovery_event_id":"fixed-event","state":"RESUMED_CONTINUOUS_TRACE"})
            episode_events = [json.loads(x) for x in (episode / "recovery_events.jsonl").read_text(encoding="utf-8").splitlines()]
            campaign_events = [json.loads(x) for x in (episode.parent / "recovery_history.jsonl").read_text(encoding="utf-8").splitlines()]
            self.assertIn(event,episode_events); self.assertIn(event,campaign_events)

    def test_unknown_provider_attempt_is_never_replayed(self):
        with tempfile.TemporaryDirectory() as td:
            episode = Path(td); trace = episode / "provider_trace"; trace.mkdir()
            (trace / "provider_attempts.jsonl").write_text(json.dumps({"event":"REQUEST_START","attempt_id":"a","request_sha256":"c"*64})+"\n"+json.dumps({"event":"REQUEST_NO_RESPONSE","attempt_id":"a","terminal_status":"OUTCOME_UNKNOWN"})+"\n",encoding="utf-8")
            result = resume_from_saved_request(episode,"a")
            self.assertEqual(result["status"],"OUTCOME_UNKNOWN_NO_AUTORETRY")
            self.assertEqual(result["resume_action"],"MANAGER_DECISION_REQUIRED_NEW_ATTEMPT_ID_ONLY")

    def test_extension_allocation_is_prospective_and_unique(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td)
            (root / "campaign_ledger.json").write_text(json.dumps({"episodes":{"DSV4_G_REP_001":{},"DSV4_G_REP_001_REC06":{}}}),encoding="utf-8")
            amendment=root / "amendment.json"
            amendment.write_text(json.dumps({"amendment_id":"DEEPSEEK_V7_REPLICATE_EXTENSION","scope":"prospective_only","planned_valid_replicates_per_treatment":10,"recovery_consumes_valid_replicate_slot":False,"allocation_ledger":"allocations.json"}),encoding="utf-8")
            first=allocate_next_replicate(root,"G_CONTROL",amendment)
            second=allocate_next_replicate(root,"G_CONTROL",amendment)
            self.assertEqual(first["episode_id"],"DSV4_G_REP_002")
            self.assertEqual(second["episode_id"],"DSV4_G_REP_003")
            self.assertEqual(first["state"],"ALLOCATED_NOT_STARTED")
            recovery=allocate_recovery_id(root,"G_CONTROL","DSV4_G_REP_001",amendment)
            self.assertEqual(recovery["episode_id"],"DSV4_G_REP_001_REC07")
            self.assertFalse(recovery["consumes_valid_replicate_slot"])



if __name__ == "__main__": unittest.main()
