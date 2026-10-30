import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from n0_autonomous.agent_loop import N0AgentHarness, ProviderContextWindowExceeded, is_explicit_context_window_error
from n0_autonomous.engine import AuditJournal, HistoryService, KnowledgeService, build_first_request_messages, sha256_text
from n0_autonomous.parallel_runtime import N0PersistentQueue
from n0_autonomous.provider_adapter import N0R5ProviderQueueAdapter
from n0_autonomous.parallel_runtime import N0_COMPILER_CONTRACT_SHA256


def load_current_template_files():
    template=ROOT/"tests"/"fixtures"/"N0_AUTONOMOUS_INFORMATION_ACQUISITION_R4"/"template"
    files={name:(template/name).read_bytes() for name in ("task_definition.json","candidate_abi.json","candidate.cu","visible_observation.json")}
    abi=json.loads(files["candidate_abi.json"].decode("utf-8"))
    runtime=ROOT/"tests"/"fixtures"/"N0_AUTONOMOUS_INFORMATION_ACQUISITION_R4"/"runtime"
    oj=ROOT.parent/"runtime"/"oj"/"releases"/"N0_KERNELBENCH_L1_40_LAYERNORM_R6_6_KERNEL_NAME_NCU_FILTER"/"RELEASE_MANIFEST.json"
    abi["compiler_contract_sha256"]=N0_COMPILER_CONTRACT_SHA256
    abi["oj_release_id"]="N0_KERNELBENCH_L1_40_LAYERNORM_R6_6_KERNEL_NAME_NCU_FILTER"
    abi["oj_manifest_sha256"]=__import__("hashlib").sha256(oj.read_bytes()).hexdigest().upper()
    files["candidate_abi.json"]=(json.dumps(abi,ensure_ascii=False,indent=2)+"\n").encode("utf-8")
    return files


class R5ContextPolicyTests(unittest.TestCase):
    def _queue_and_adapter(self,temp,enqueue):
        files=load_current_template_files()
        store=N0PersistentQueue(Path(temp)/"runtime")
        store.register_episode(episode_id="SYNTH-R5",replicate_slot="SYNTH-R5-SLOT",treatment_id="N0_AUTONOMOUS",
            approved_allocation={"authorized":True,"episode_id":"SYNTH-R5","replicate_slot":"SYNTH-R5-SLOT"},
            max_candidates=30,workspace_files=files)
        adapter=N0R5ProviderQueueAdapter(store=store,queue_capability={"status":"PASS","treatment_id":"N0_AUTONOMOUS",
            "provider_release_id":"SYNTH-R5","manifest_sha256":"c"*64,"allowed_treatments":["N0_AUTONOMOUS"]},
            enqueue=enqueue,provider_release_id="SYNTH-R5",provider_manifest_sha256="c"*64)
        return store,adapter

    def test_over_legacy_context_gate_payload_is_forwarded_byte_for_byte(self):
        seen=[]
        response=json.dumps({"id":"SYNTH-RESP","choices":[{"message":{"content":"ok"}}]}).encode()
        def enqueue(**kwargs):
            seen.append(kwargs["payload"])
            return {"request_id":kwargs["request_id"],"attempt_id":kwargs["attempt_id"],
                    "response_id":"SYNTH-RESP","response_body":response}
        with tempfile.TemporaryDirectory() as temp:
            store,adapter=self._queue_and_adapter(temp,enqueue)
            payload=json.dumps({"model":"synthetic","messages":[{"role":"user","content":"X"*200000}],"tools":[]},separators=(",",":" )).encode()
            result=adapter.dispatch(episode_id="SYNTH-R5",invocation_id="INV-LARGE",request_index=1,payload=payload)
            self.assertEqual(seen,[payload])
            self.assertEqual(result["response"]["id"],"SYNTH-RESP")
            self.assertEqual(store.snapshot()["provider_requests"][0]["state"],"CONSUMED")

    def test_large_complete_mandatory_context_has_no_local_length_gate_and_tool_directory_is_plain_text(self):
        source="CUDA source fixture"
        state={"format":"EPISODE_STATE_V1","episode_id":"SYNTH","task_id":"KB_L1_40_LAYERNORM",
            "current_candidate_id":"C0001",
            "workspace_sha256":"a"*64,"best_candidate":"C0000","settled_candidates":[],
            "previously_retrieved":[],"last_history_query":None,"retrieval_footprint_note":"Past successful returns only.",
            "working_note":None,"working_note_sha256":None}
        schema=json.loads((ROOT/"protocol"/"TOOL_SCHEMAS.json").read_text(encoding="utf-8"))
        footprint=json.dumps({"previously_retrieved":[],"last_history_query":None},sort_keys=True,separators=(",",":"))
        messages=build_first_request_messages("SYSTEM","TASK",task_definition="T"*70000,candidate_abi="ABI",
            candidate_source=source,candidate_sha256=sha256_text(source),visible_observation=json.dumps({"message":"O"*1000,
                "profile_feedback":{"metrics":{"gpu__time_duration.sum":123}},"score":1.2}),
            history_brief=json.dumps(state),previously_retrieved=footprint,
            working_note=None,tools=schema["tools"])
        self.assertGreater(len(json.dumps(messages,ensure_ascii=False).encode()),65536)
        user=messages[1]["content"]
        self.assertIn("AVAILABLE_TOOLS_V1",user)
        directory=user.split("AVAILABLE_TOOLS_V1\n",1)[1].split("\n\nN0_MANDATORY_INVOCATION_CONTEXT_JSON",1)[0]
        parsed=json.loads(directory)
        self.assertEqual([x["name"] for x in parsed],
            ["read_workspace_file","edit_candidate","query_candidate_history","query_hardware_skill"])
        history_description=parsed[2]["description"]
        for field in ("source","evaluation","timing","profile","diagnostics","lineage"):
            self.assertIn(field,history_description)
        self.assertIn("Repeated queries",history_description)
        self.assertIn("Optional",parsed[3]["description"])
        self.assertNotIn("budget", user.lower())
        self.assertEqual(len(parsed), 4)
        dynamic=json.loads(user.split("N0_MANDATORY_INVOCATION_CONTEXT_JSON\n",1)[1])
        self.assertNotIn("gpu__time_duration.sum", json.dumps(dynamic["latest_complete_evaluation_observation"]))

    def test_tool_results_are_not_capped_by_default(self):
        history=HistoryService("SYNTH",[])
        large={"content":"H"*50000}
        result=history._paginate(large)
        self.assertEqual(result["status"],"OK")
        self.assertEqual(result["data"],large)
        knowledge=KnowledgeService._paginate_static(large,max_bytes=None,page_token=None,scope="hardware",snapshot="a"*64)
        self.assertEqual(knowledge["status"],"OK")
        self.assertEqual(knowledge["data"],large)

    def test_c0030_cutoff_does_not_dispatch_provider_or_request_summary(self):
        from n0_autonomous.agent_loop import N0AgentHarness
        class Audit:
            def __init__(self): self.events=[]
            def append(self, event): self.events.append(event)
        harness=object.__new__(N0AgentHarness)
        harness.audit=Audit()
        episode={"episode_id":"SYNTH-C0030","candidates":[{"candidate_id":f"C{i:04d}"} for i in range(31)]}
        result=harness.run_outer_invocation(episode=episode,candidate_budget=30,visible_observation="{}",
            invocation_id="INV-END",turn_id="T-END",audit_events=[])
        self.assertEqual(result["status"],"EPISODE_EXTERNAL_CUTOFF")
        self.assertFalse(result["provider_dispatched"])
        self.assertEqual(harness.audit.events[0]["event_type"],"EXTERNAL_CANDIDATE_CUTOFF")

    def test_explicit_remote_context_error_is_persisted_and_batch_abort_is_idempotent(self):
        error=json.dumps({"error":{"type":"ContextWindowExceededError","message":"Input length 1100083 exceeds the maximum length 1048566"}}).encode()
        self.assertTrue(is_explicit_context_window_error(400,error))
        self.assertFalse(is_explicit_context_window_error(400,b'{"error":{"message":"bad request"}}'))
        self.assertFalse(is_explicit_context_window_error(413,error))
        calls=[]
        def enqueue(**kwargs):
            calls.append(kwargs)
            return {"request_id":kwargs["request_id"],"attempt_id":kwargs["attempt_id"],"status_code":400,"error_body":error}
        with tempfile.TemporaryDirectory() as temp:
            store,adapter=self._queue_and_adapter(temp,enqueue)
            payload=b'{"model":"synthetic","messages":[{"role":"user","content":"oversize"}],"tools":[]}'
            result=adapter.dispatch(episode_id="SYNTH-R5",invocation_id="INV-ERROR",request_index=1,payload=payload)
            self.assertTrue(result["provider_error"])
            request=store.snapshot()["provider_requests"][0]
            self.assertEqual(request["state"],"PROVIDER_HTTP_ERROR")
            self.assertEqual((store.root/request["response_path"]).read_bytes(),error)
            self.assertEqual(request["response_sha256"],result["error_body_sha256"])
            event={"event_type":"PROVIDER_CONTEXT_WINDOW_EXCEEDED","episode_id":"SYNTH-R5","request_id":result["request_id"],
                   "attempt_id":result["attempt_id"],"request_body_sha256":"a"*64,"error_body_sha256":result["error_body_sha256"]}
            first=store.abort("PROVIDER_CONTEXT_WINDOW_EXCEEDED",event)
            second=store.abort("PROVIDER_CONTEXT_WINDOW_EXCEEDED",event)
            self.assertEqual(first["status"],"INFRA_ABORT_CONTEXT_EXCEEDED")
            self.assertEqual(first,second)
            self.assertEqual(store.is_aborted(),first)
            self.assertEqual(len(calls),1)
            with self.assertRaises(Exception):
                adapter.dispatch(episode_id="SYNTH-R5",invocation_id="INV-ERROR",request_index=1,payload=payload)
            self.assertEqual(len(calls),1)

    def test_agent_loop_terminates_on_remote_context_error_without_stop_or_candidate(self):
        error=json.dumps({"error":{"type":"ContextWindowExceededError","message":"Input length exceeds the maximum length"}}).encode()
        with tempfile.TemporaryDirectory() as temp:
            store,_=self._queue_and_adapter(temp,lambda **kwargs:None)
            audit=AuditJournal(Path(temp)/"audit")
            def dispatch(payload):
                return {"provider_error":True,"status_code":400,"request_id":"SYNTH-REQ-CTX",
                        "attempt_id":"SYNTH-ATT-CTX","error_body":error}
            harness=N0AgentHarness(system_prompt="S",task_user_message="U",tools=[],model="synthetic",
                reasoning_effort="high",tool_server=None,audit=audit,dispatch=dispatch,
                max_api_requests_per_invocation=8,working_note_store=object(),
                control_finalizer=lambda **kwargs:{"status":"SHOULD_NOT_RUN"},batch_control=store)
            harness._build_initial_messages=lambda **kwargs:[{"role":"system","content":"S"},{"role":"user","content":"large"}]
            episode={"episode_id":"SYNTH-R5","candidates":[]}
            with self.assertRaises(ProviderContextWindowExceeded):
                harness.run_outer_invocation(episode=episode,candidate_budget=30,visible_observation="{}",
                    invocation_id="INV-CTX",turn_id="TURN-1",audit_events=[])
            self.assertEqual(store.is_aborted()["status"],"INFRA_ABORT_CONTEXT_EXCEEDED")
            self.assertFalse(any(x.get("event_type")=="CONTROL_DECISION" for x in audit.events()))
            self.assertEqual(episode["candidates"],[])

    def test_batch_abort_stops_new_work_for_all_four_workers_and_preserves_queued_oj(self):
        files=load_current_template_files()
        with tempfile.TemporaryDirectory() as temp:
            queue=N0PersistentQueue(Path(temp)/"runtime")
            episode_ids=[f"SYNTH-BATCH-{i}" for i in range(4)]
            for i,eid in enumerate(episode_ids):
                slot=f"SYNTH-BATCH-SLOT-{i}"
                queue.register_episode(episode_id=eid,replicate_slot=slot,treatment_id="N0_AUTONOMOUS",
                    approved_allocation={"authorized":True,"episode_id":eid,"replicate_slot":slot},
                    max_candidates=30,workspace_files=files)
                source=files["candidate.cu"].decode()+f"\n// worker {i}\n"
                queue.submit_continue(episode_id=eid,invocation_id=f"INV-{i}",current_source=source,
                    frozen_identity_rule=lambda new,old:new!=old)
            before_used=[x["candidates_used"] for x in queue.snapshot()["episodes"]]
            queue.abort("PROVIDER_CONTEXT_WINDOW_EXCEEDED",{"event_type":"PROVIDER_CONTEXT_WINDOW_EXCEEDED",
                "episode_id":episode_ids[0],"request_id":"SYNTH-CTX-REQUEST"})
            new_provider_calls=[]
            adapter=N0R5ProviderQueueAdapter(store=queue,queue_capability={"status":"PASS","treatment_id":"N0_AUTONOMOUS",
                "provider_release_id":"SYNTH-R5","manifest_sha256":"c"*64,"allowed_treatments":["N0_AUTONOMOUS"]},
                enqueue=lambda **kwargs:new_provider_calls.append(kwargs),provider_release_id="SYNTH-R5",provider_manifest_sha256="c"*64)
            with self.assertRaisesRegex(Exception,"SCIENCE_BATCH_ABORTED_NO_NEW_PROVIDER_REQUEST"):
                adapter.dispatch(episode_id=episode_ids[1],invocation_id="AFTER-ABORT",request_index=1,
                    payload=b'{"model":"synthetic","messages":[],"tools":[]}')
            self.assertEqual(new_provider_calls,[])
            self.assertEqual(queue.claim_next_oj("SYNTH-OJ",worker_pid=111,worker_start_utc="synthetic"),None)
            self.assertEqual(queue.snapshot()["queue_depth"],4)
            for i,eid in enumerate(episode_ids):
                source=files["candidate.cu"].decode()+f"\n// after abort {i}\n"
                with self.assertRaisesRegex(Exception,"SCIENCE_BATCH_ABORTED_NO_NEW_CANDIDATE"):
                    queue.submit_continue(episode_id=eid,invocation_id=f"POST-{i}",current_source=source,
                        frozen_identity_rule=lambda new,old:new!=old)
            self.assertEqual([x["candidates_used"] for x in queue.snapshot()["episodes"]],before_used)


if __name__=="__main__":
    unittest.main()
