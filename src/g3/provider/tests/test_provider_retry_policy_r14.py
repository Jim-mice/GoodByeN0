import hashlib
import json
import multiprocessing as mp
import io
import os
import tempfile
import time
import urllib.error
from email.message import Message
import unittest
from pathlib import Path

from canonical_harness.provider_retry import ProviderRequestGate, complete_with_retries, await_candidate_boundary_pause
from canonical_harness.recovery_api import prepare_authorized_exact_request_recovery
from provider_staging.paratera_chat import ProviderError
from provider_staging import paratera_http_worker


class FakeGate:
    def __init__(self): self.entries = []
    class Permit:
        def __init__(self, outer, attempt): self.outer, self.attempt = outer, attempt
        def __enter__(self): self.outer.entries.append(("enter", self.attempt))
        def __exit__(self, *args): self.outer.entries.append(("exit", self.attempt))
    def permit(self, attempt_id, episode_id): return self.Permit(self, attempt_id)


class FakeClient:
    request_sha256 = staticmethod(lambda model, effort, messages, tools, choice, tokens:
        hashlib.sha256(json.dumps([model, effort, messages, tools, choice, tokens], sort_keys=True).encode()).hexdigest())
    def __init__(self, outcomes): self.outcomes = list(outcomes); self.calls = []
    def complete(self, *args, **kwargs):
        self.calls.append({"args": args, "kwargs": kwargs})
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception): raise outcome
        return outcome


def transport_error(status="TRANSPORT_FAILURE_OUTCOME_UNKNOWN", http_status=None, retry_after=None):
    telemetry = {"terminal_status": status, "http_status": http_status}
    if retry_after is not None: telemetry["retry_after_seconds"] = retry_after
    return ProviderError("simulated", telemetry)


def gate_child(root, index, active, maximum):
    gate=ProviderRequestGate(root,.01)
    with gate.permit("a"+str(index),"e"+str(index)):
        with active.get_lock():
            active.value += 1
            with maximum.get_lock(): maximum.value=max(maximum.value,active.value)
        time.sleep(.1)
        with active.get_lock(): active.value -= 1


class ProviderRetryPolicyTests(unittest.TestCase):
    def invoke(self, outcomes, sleep=lambda _: None):
        messages = [{"role": "user", "content": "same"}]
        tools = [{"type": "function", "function": {"name": "read"}}]
        sha = FakeClient.request_sha256("DeepSeek-V4-Pro", "high", messages, tools, "auto", 32768)
        client = FakeClient(outcomes)
        events = []
        with tempfile.TemporaryDirectory() as td:
            result = complete_with_retries(client, model="DeepSeek-V4-Pro", effort="high", messages=messages,
                tools=tools, tool_choice="auto", max_tokens=32768, initial_attempt_id="first",
                initial_artifact_path=Path(td) / "first.response.json", episode_id="test", request_sha256=sha,
                on_event=events.append, gate=FakeGate(), sleep=sleep)
        return client, events, result

    def test_first_success_no_replay(self):
        c, _, r = self.invoke([({"choices": []}, "evidence")])
        self.assertEqual(len(c.calls), 1); self.assertEqual(r[2]["attempt_ids"], ["first"])

    def test_first_timeout_second_success(self):
        c, _, r = self.invoke([transport_error(), ({"choices": []}, "evidence")])
        self.assertEqual(len(c.calls), 2); self.assertEqual(len(set(r[2]["attempt_ids"])), 2)

    def test_two_timeouts_third_success(self):
        c, _, r = self.invoke([transport_error(), transport_error(), ({"choices": []}, "evidence")])
        self.assertEqual(len(c.calls), 3); self.assertEqual(len(set(r[2]["attempt_ids"])), 3)

    def test_three_failures_exhaust_without_fourth(self):
        messages = [{"role": "user", "content": "same"}]; tools = []
        sha = FakeClient.request_sha256("DeepSeek-V4-Pro", "high", messages, tools, "auto", 32768)
        client = FakeClient([transport_error(), transport_error(), transport_error()])
        with tempfile.TemporaryDirectory() as td, self.assertRaises(ProviderError) as cm:
            complete_with_retries(client, model="DeepSeek-V4-Pro", effort="high", messages=messages, tools=tools,
                tool_choice="auto", max_tokens=32768, initial_attempt_id="first",
                initial_artifact_path=Path(td) / "first.response.json", episode_id="test", request_sha256=sha,
                gate=FakeGate())
        self.assertTrue(cm.exception.retry_exhausted); self.assertEqual(len(client.calls), 3)

    def test_429_obeys_retry_after_and_reenters_gate(self):
        delays = []; gate = FakeGate(); messages = []; tools = []
        sha = FakeClient.request_sha256("DeepSeek-V4-Pro", "high", messages, tools, "auto", 10)
        client = FakeClient([transport_error("HTTP_ERROR", 429, 4), ({"choices": []}, "ev")])
        with tempfile.TemporaryDirectory() as td:
            complete_with_retries(client, model="DeepSeek-V4-Pro", effort="high", messages=messages, tools=tools,
                tool_choice="auto", max_tokens=10, initial_attempt_id="a", initial_artifact_path=Path(td)/"a.json",
                episode_id="e", request_sha256=sha, gate=gate, sleep=delays.append)
        self.assertEqual(delays, [4]); self.assertEqual([x[0] for x in gate.entries], ["enter", "exit", "enter", "exit"])

    def test_http_worker_parses_retry_after_without_logging_credentials(self):
        old=os.environ.get("jlapi"); os.environ["jlapi"]="dummy-not-a-real-key"
        headers=Message(); headers["Retry-After"]="7"; headers["Content-Length"]="0"
        error=urllib.error.HTTPError("https://example.invalid",429,"rate limited",headers,io.BytesIO(b"{}"))
        try:
            result=paratera_http_worker.perform({"url":"https://example.invalid","body":{},"timeout_seconds":3},opener=lambda *a,**k: (_ for _ in ()).throw(error))
        finally:
            if old is None: os.environ.pop("jlapi",None)
            else: os.environ["jlapi"]=old
        self.assertEqual(result["telemetry"]["http_status"],429)
        self.assertEqual(result["telemetry"]["retry_after_seconds"],7.0)
        self.assertNotIn("dummy-not-a-real-key",json.dumps(result))

    def test_auth_failure_is_not_replayed(self):
        c = FakeClient([transport_error("HTTP_ERROR", 401), ({}, "never")])
        msgs=[]; tools=[]; sha=FakeClient.request_sha256("DeepSeek-V4-Pro","high",msgs,tools,"auto",1)
        with tempfile.TemporaryDirectory() as td, self.assertRaises(ProviderError):
            complete_with_retries(c, model="DeepSeek-V4-Pro", effort="high", messages=msgs, tools=tools,
                tool_choice="auto", max_tokens=1, initial_attempt_id="x", initial_artifact_path=Path(td)/"x.json",
                episode_id="e", request_sha256=sha, gate=FakeGate())
        self.assertEqual(len(c.calls), 1)

    def test_complete_response_stops_attempt_sequence(self):
        client, _, result = self.invoke([({"choices": [{"message": {"content": "ok"}}]}, "verified-sidecar")])
        self.assertEqual(len(client.calls),1); self.assertEqual(result[2]["attempt_ids"], ["first"])

    def test_replay_does_not_create_another_logical_candidate(self):
        logical_candidate_count=7
        client, _, _ = self.invoke([transport_error(), ({"choices": []}, "evidence")])
        self.assertEqual(len(client.calls),2); self.assertEqual(logical_candidate_count,7)

    def test_candidate_boundary_pause_resume_records_both_states(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td); (root/"PAUSE").write_text("pause",encoding="utf-8"); (root/"RESUME").write_text("resume",encoding="utf-8")
            self.assertTrue(await_candidate_boundary_pause(root,poll_seconds=0))
            events=[json.loads(x) for x in (root/"pause_events.jsonl").read_text(encoding="utf-8").splitlines()]
            self.assertEqual([x["event"] for x in events],["PAUSED_AT_CANDIDATE_BOUNDARY","RESUMED_FROM_SAVED_IN_MEMORY_CHECKPOINT"])

    def test_hs_recovery_material_is_exact_and_does_not_send(self):
        from provider_staging.paratera_chat import ParateraChatCompletions
        with tempfile.TemporaryDirectory() as td:
            root=Path(td); (root/"provider_trace").mkdir()
            messages=[{"role":"user","content":"preserve"}]; tools=[]
            digest=ParateraChatCompletions.request_sha256("DeepSeek-V4-Pro","high",messages,tools,"auto",32768)
            event={"event":"REQUEST_START","attempt_id":"sealed-old-attempt","request_sha256":digest,
                   "model":"DeepSeek-V4-Pro","requested_effort":"high","messages":messages,"tools":tools,
                   "tool_choice":"auto","max_tokens":32768}
            (root/"provider_trace"/"provider_attempts.jsonl").write_text(json.dumps(event)+"\n",encoding="utf-8")
            result=prepare_authorized_exact_request_recovery(root,"sealed-old-attempt",digest,2)
            self.assertEqual(result["new_attempt_limit"],2); self.assertFalse(result["provider_request_started"])
            self.assertEqual(result["request_material"]["messages"],messages)

    def test_formal_recovery_entry_uses_new_ids_and_preserves_sealed_history(self):
        from canonical_harness.paratera_agent import ParateraAgentAdapter
        from provider_staging.paratera_chat import ParateraChatCompletions, CompletionEvidence
        with tempfile.TemporaryDirectory() as td:
            episode=Path(td)/"DSV4_HS_REP_007_RECOVERY"; workspace=episode/"model_workspace"
            trace=episode/"provider_trace"; workspace.mkdir(parents=True); trace.mkdir()
            messages=[{"role":"user","content":"unchanged context"}]; tools=[]
            digest=ParateraChatCompletions.request_sha256("DeepSeek-V4-Pro","high",messages,tools,"auto",32768)
            old={"event":"REQUEST_START","attempt_id":"sealed-attempt","request_sha256":digest,
                 "model":"DeepSeek-V4-Pro","requested_effort":"high","messages":messages,
                 "tools":tools,"tool_choice":"auto","max_tokens":32768}
            no_response={"event":"REQUEST_NO_RESPONSE","attempt_id":"sealed-attempt","terminal_status":"TIMEOUT_OUTCOME_UNKNOWN"}
            trace_file=trace/"provider_attempts.jsonl"; trace_file.write_text(json.dumps(old)+"\n"+json.dumps(no_response)+"\n",encoding="utf-8")
            old_bytes=trace_file.read_bytes(); agent=ParateraAgentAdapter("DeepSeek-V4-Pro",treatment_id="H_STATIC")
            def fake_complete(model,effort,msgs,**kwargs):
                payload={"id":"fresh-response","model":model,"choices":[{"finish_reason":"stop","message":{"role":"assistant","content":"CONTINUE"}}]}
                evidence=CompletionEvidence(None,"fresh-response",model,model,effort,None,payload,{"http_status":200,"body_complete":True})
                return payload,evidence
            import os
            old_queue=os.environ.get("NOVELTY_V7_PROVIDER_QUEUE_ROOT")
            os.environ["NOVELTY_V7_PROVIDER_QUEUE_ROOT"]=str(episode/"provider_queue")
            queue_result={"status":"COMPLETED","request_id":"queue-request","request_body_sha256":digest,
                "attempt_lineage_id":"sealed-attempt","episode_id":episode.name,"treatment":"H_STATIC",
                "attempt_ids":["new-attempt"],"attempts_used":2,"assigned_window":"A","worker_key_alias":"jlapi",
                "payload":{"id":"fresh-response","model":"DeepSeek-V4-Pro","choices":[{"finish_reason":"stop","message":{"role":"assistant","content":"CONTINUE"}}]},
                "response_id":"fresh-response"}
            from unittest.mock import patch
            try:
                with patch("canonical_harness.paratera_agent._r16_submit_and_wait",return_value=queue_result) as queue_call:
                    result=agent.resume_authorized_provider_request(workspace,(),"sealed-attempt",digest,max_new_attempts=2)
                self.assertEqual(queue_call.call_args.kwargs["attempts_already_used"],1)
                self.assertEqual(queue_call.call_args.kwargs["max_attempts"],2)
            finally:
                if old_queue is None: os.environ.pop("NOVELTY_V7_PROVIDER_QUEUE_ROOT",None)
                else: os.environ["NOVELTY_V7_PROVIDER_QUEUE_ROOT"]=old_queue
            self.assertEqual(result["final_text"],"CONTINUE")
            self.assertNotIn("sealed-attempt",result["attempt_ids"])
            self.assertEqual(trace_file.read_bytes()[:len(old_bytes)],old_bytes)
            events=[json.loads(x) for x in trace_file.read_text(encoding="utf-8").splitlines()]
            self.assertTrue(any(x.get("recovery_of")=="sealed-attempt" and x.get("event")=="RESPONSE_RECEIVED" for x in events))

    def test_exact_payload_sha_and_body_objects_across_replays(self):
        c, _, _ = self.invoke([transport_error(), ({"choices": []}, "evidence")])
        self.assertEqual(c.calls[0]["args"], c.calls[1]["args"])
        self.assertEqual(c.calls[0]["kwargs"]["request_sha256"] if "request_sha256" in c.calls[0]["kwargs"] else
                         FakeClient.request_sha256("DeepSeek-V4-Pro","high",c.calls[0]["args"][2],c.calls[0]["kwargs"]["tools"],c.calls[0]["kwargs"]["tool_choice"],c.calls[0]["kwargs"]["max_tokens"]),
                         FakeClient.request_sha256("DeepSeek-V4-Pro","high",c.calls[1]["args"][2],c.calls[1]["kwargs"]["tools"],c.calls[1]["kwargs"]["tool_choice"],c.calls[1]["kwargs"]["max_tokens"]))

    def test_payload_hash_mismatch_fails_before_provider_call(self):
        client=FakeClient([({},"must-not-run")]); msgs=[{"role":"user","content":"mutated"}]
        with tempfile.TemporaryDirectory() as td, self.assertRaisesRegex(RuntimeError,"PROVIDER_RETRY_PAYLOAD_SHA_MISMATCH"):
            complete_with_retries(client,model="DeepSeek-V4-Pro",effort="high",messages=msgs,tools=[],
                tool_choice="auto",max_tokens=1,initial_attempt_id="x",initial_artifact_path=Path(td)/"x.json",
                episode_id="e",request_sha256="0"*64,gate=FakeGate())
        self.assertEqual(client.calls,[])

    def test_each_replay_attempt_persists_complete_exact_request_material(self):
        from canonical_harness.paratera_agent import _record_retry_attempt_with_payload
        with tempfile.TemporaryDirectory() as td:
            trace=Path(td)/"provider_attempts.jsonl"; checkpoint=Path(td)/"checkpoint.json"
            messages=[{"role":"user","content":"immutable"}]; tools=[]; state={"provider_attempt_ids":[]}
            _record_retry_attempt_with_payload(trace,{"event":"PROVIDER_ATTEMPT_START","attempt_id":"replay-2",
                "request_sha256":"a"*64,"request_started_utc":"now"},state,checkpoint,"first","DeepSeek-V4-Pro",
                messages,tools,"auto",32768,"high","episode")
            events=[json.loads(x) for x in trace.read_text(encoding="utf-8").splitlines()]
            start=next(x for x in events if x["event"]=="REQUEST_START")
            self.assertEqual(start["attempt_id"],"replay-2"); self.assertEqual(start["messages"],messages)
            self.assertEqual(start["tools"],tools); self.assertEqual(start["replay_of"],"first")

    def test_four_processes_share_one_provider_permit(self):
        with tempfile.TemporaryDirectory() as td:
            ctx = mp.get_context("spawn")
            active = ctx.Value("i", 0)
            max_active = ctx.Value("i", 0)
            ps=[ctx.Process(target=gate_child,args=(td,i,active,max_active)) for i in range(4)]
            for p in ps: p.start()
            for p in ps: p.join(2)
            alive=[p for p in ps if p.is_alive()]
            if alive:
                for p in alive: p.terminate()
                for p in alive: p.join(1)
            self.assertTrue(all(p.exitcode == 0 for p in ps), msg=f"exitcodes={[p.exitcode for p in ps]}; active={Path(td,'active.json').read_text() if Path(td,'active.json').exists() else None}; tickets={[x.name for x in Path(td,'tickets').glob('*.json')]}")
            self.assertEqual(max_active.value,1)


if __name__ == "__main__": unittest.main()
