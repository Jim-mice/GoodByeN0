import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from n0_autonomous.engine import (WorkspaceTools, build_history_brief, canonical_bytes,
    sha256_bytes)
from n0_autonomous.parallel_runtime import InvocationFinalizer, N0PersistentQueue, make_worker_contexts
from n0_autonomous.parallel_runtime import N0_COMPILER_CONTRACT_SHA256


class R5RuntimeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.project = ROOT
        cls.template = ROOT / "tests" / "fixtures" / "N0_AUTONOMOUS_INFORMATION_ACQUISITION_R4" / "template"
        cls.files = {name:(cls.template/name).read_bytes() for name in ("task_definition.json","candidate_abi.json","candidate.cu","visible_observation.json")}
        abi=json.loads(cls.files["candidate_abi.json"].decode("utf-8"))
        oj=ROOT.parent/"runtime"/"oj"/"releases"/"N0_KERNELBENCH_L1_40_LAYERNORM_R6_6_KERNEL_NAME_NCU_FILTER"/"RELEASE_MANIFEST.json"
        abi["compiler_contract_sha256"]=N0_COMPILER_CONTRACT_SHA256
        abi["oj_release_id"]="N0_KERNELBENCH_L1_40_LAYERNORM_R6_6_KERNEL_NAME_NCU_FILTER"
        abi["oj_manifest_sha256"]=__import__("hashlib").sha256(oj.read_bytes()).hexdigest().upper()
        cls.files["candidate_abi.json"]=(json.dumps(abi,ensure_ascii=False,indent=2)+"\n").encode("utf-8")
        cls.seed_sha = sha256_bytes(cls.files["candidate.cu"])

    def test_fresh_invocation_injects_seed_and_complete_contract(self):
        from n0_autonomous.engine import build_first_request_messages, sha256_text
        schema = json.loads((ROOT / "protocol" / "TOOL_SCHEMAS.json").read_text(encoding="utf-8"))
        state = {"format":"EPISODE_STATE_V1","episode_id":"N0-SYNTH-1","task_id":"KB_L1_40_LAYERNORM",
                 "current_candidate_id":"C0001","workspace_sha256":"a"*64,
                 "best_candidate":"C0000","settled_candidates":[],"previously_retrieved":[],
                 "last_history_query":None,"retrieval_footprint_note":"records successful past returns only",
                 "working_note":None,"working_note_sha256":None}
        source=self.files["candidate.cu"].decode("utf-8")
        footprint=json.dumps({"previously_retrieved":[],"last_history_query":None},sort_keys=True,separators=(",",":"))
        messages=build_first_request_messages("SYSTEM","TASK",task_definition=self.files["task_definition.json"].decode(),
            candidate_abi=self.files["candidate_abi.json"].decode(),candidate_source=source,candidate_sha256=sha256_text(source),
            visible_observation=self.files["visible_observation.json"].decode(),
            history_brief=json.dumps(state),previously_retrieved=footprint,working_note=None,tools=schema["tools"])
        dynamic=json.loads(messages[1]["content"].split("N0_MANDATORY_INVOCATION_CONTEXT_JSON\n",1)[1])
        self.assertEqual(dynamic["current_candidate"]["source"],source)
        self.assertEqual(dynamic["current_candidate"]["sha256"],sha256_text(source))
        self.assertIn("AVAILABLE_TOOLS_V1",messages[1]["content"])
        self.assertEqual([m["role"] for m in messages],["system","user"])

    def test_thirty_agent_candidates_count_excludes_seed_and_keeps_failures_visible(self):
        candidates=[]
        for i in range(31):
            cid=f"C{i:04d}"; source=self.files["candidate.cu"].decode() if i==0 else f"synthetic source {i}"; source_sha=sha256_bytes(source.encode())
            verdict="VALID_MEASUREMENT"; score=1.0+i/100; promotion="NOT_PROMOTED"
            if i==3: verdict="CANDIDATE_COMPILE_FAILURE"; score=None
            if i==4: verdict="CANDIDATE_CORRECTNESS_FAILURE"; score=None
            if i==0: promotion="SEED_BASELINE"
            artifacts={"timing":{"sample":i},"profile":{"status":"SYNTHETIC"},
                       "diagnostics":{},"lineage":{"parent_sha256":"0"*64}}
            receipt={"terminal":True,"candidate_sha256":source_sha,"task_sha256":"8D975D7CEE6946513DCFB6BA84C971E53A890F9D9C6F0A4FB828904941F25975",
                     "gpu_identity":"SYNTH-GPU","release_id":"SYNTH-OJ","manifest_sha256":"a"*64,
                     "measurement_protocol":"SYNTHETIC","request_id":f"REQ{i}","verdict":verdict,"score":score,
                     **artifacts}
            receipt["receipt_sha256"]=sha256_bytes(canonical_bytes(receipt))
            artifact_sha={k:sha256_bytes(canonical_bytes(v)) for k,v in artifacts.items()}
            ledger={"candidate_id":cid,"candidate_sha256":source_sha,"request_id":f"REQ{i}","verdict":verdict,
                    "receipt_sha256":receipt["receipt_sha256"],"artifact_sha256":artifact_sha,
                    "score":score,"promotion":promotion}
            ledger["ledger_entry_sha256"]=sha256_bytes(canonical_bytes(ledger))
            candidates.append({"candidate_id":cid,"source":source,"source_sha256":source_sha,"oj_receipt":receipt,
                               **artifacts,"ledger_entry":ledger})
        brief=json.loads(build_history_brief({"episode_id":"SYNTH","task_id":"KB_L1_40_LAYERNORM",
            "current_candidate_id":"C0031","workspace_files":{"candidate.cu":self.files["candidate.cu"].decode()},
            "candidates":candidates},candidate_budget=30))
        self.assertNotIn("budget",brief)
        self.assertEqual(len(brief["settled_candidates"]),31)
        self.assertEqual([c["candidate_id"] for c in brief["settled_candidates"]][0],"C0000")
        self.assertEqual(brief["settled_candidates"][3]["verdict"],"CANDIDATE_COMPILE_FAILURE")
        self.assertEqual(brief["settled_candidates"][4]["verdict"],"CANDIDATE_CORRECTNESS_FAILURE")
        self.assertIsNone(brief["settled_candidates"][3]["score"])
        self.assertEqual(len(brief["settled_candidates"][0]["source_sha256"]),64)
        self.assertEqual(brief["settled_candidates"][3]["evaluation_status"],"CANDIDATE_COMPILE_FAILURE")
        self.assertFalse(brief["settled_candidates"][3]["promoted"])
        self.assertNotIn("timing",brief["settled_candidates"][0])
        self.assertNotIn("profile",brief["settled_candidates"][0])

    def test_v11_seed_reference_hold_is_unscored_but_history_is_valid(self):
        source=self.files["candidate.cu"].decode("utf-8")
        source_sha=sha256_bytes(source.encode("utf-8"))
        artifacts={"timing":None,"profile":{"status":"UNAVAILABLE"},"diagnostics":None,"lineage":None}
        receipt={"terminal":True,"candidate_sha256":source_sha,
            "task_sha256":"8D975D7CEE6946513DCFB6BA84C971E53A890F9D9C6F0A4FB828904941F25975",
            "gpu_identity":"RTX5060-SYNTH","release_id":"N0_KERNELBENCH_L1_40_LAYERNORM_R5_REFERENCE_TRUST",
            "manifest_sha256":"a"*64,"measurement_protocol":"KERNELBENCH_40_V1",
            "request_id":"SYNTH-REP04-C0000","episode_id":"SYNTH-REP04","candidate_id":"C0000",
            "verdict":"OJ_INFRASTRUCTURE_HOLD_REFERENCE_CONTROL_MISMATCH","score":None,**artifacts}
        receipt["receipt_sha256"]=sha256_bytes(canonical_bytes({k:v for k,v in receipt.items() if k!="receipt_sha256"}))
        artifact_sha={k:sha256_bytes(canonical_bytes(v)) for k,v in artifacts.items()}
        ledger={"candidate_id":"C0000","candidate_sha256":source_sha,"request_id":receipt["request_id"],
            "verdict":receipt["verdict"],"receipt_sha256":receipt["receipt_sha256"],
            "artifact_sha256":artifact_sha,"score":None,"promotion":"BASELINE"}
        ledger["ledger_entry_sha256"]=sha256_bytes(canonical_bytes(ledger))
        candidate={"candidate_id":"C0000","source":source,"source_sha256":source_sha,"oj_receipt":receipt,
            **artifacts,"ledger_entry":ledger}
        brief=json.loads(build_history_brief({"episode_id":"SYNTH-REP04","task_id":"KB_L1_40_LAYERNORM",
            "current_candidate_id":"C0001","workspace_files":{"candidate.cu":source},"candidates":[candidate]},candidate_budget=30))
        self.assertIsNone(brief["settled_candidates"][0]["score"])
        self.assertFalse(brief["settled_candidates"][0]["promoted"])
        self.assertEqual(brief["settled_candidates"][0]["verdict"],"C0000_BASELINE_SCORE_UNAVAILABLE_INFRA_HOLD")
        self.assertEqual(brief["settled_candidates"][0]["evaluation_status"],"C0000_BASELINE_SCORE_UNAVAILABLE_INFRA_HOLD")

    def test_four_allocations_share_identical_seed_but_isolate_writable_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            queue=N0PersistentQueue(Path(tmp)/"runtime")
            ids=[f"N0SYNTH{i}" for i in range(4)]
            paths=[]
            for i,eid in enumerate(ids):
                allocation={"authorized":True,"episode_id":eid,"replicate_slot":f"SYNTH-{i}"}
                path=queue.register_episode(episode_id=eid,replicate_slot=f"SYNTH-{i}",treatment_id="N0_AUTONOMOUS",
                    approved_allocation=allocation,max_candidates=30,workspace_files=self.files)
                paths.append(path)
                self.assertEqual(sha256_bytes((path.workspace/"candidate.cu").read_bytes()),self.seed_sha)
            contexts=make_worker_contexts(queue,ids)
            self.assertEqual(len(contexts),4)
            self.assertEqual(len({str(x.workspace) for x in contexts}),4)
            self.assertEqual(len({str(x.checkpoint) for x in contexts}),4)
            for i,ctx in enumerate(contexts):
                ws=WorkspaceTools(ctx.workspace)
                ws.begin_invocation(f"INV-{i}")
                before=sha256_bytes((ctx.workspace/"candidate.cu").read_bytes())
                first=ws.edit_candidate(expected_sha256=before,source=(ctx.workspace/"candidate.cu").read_text()+f"\n// edit {i} a\n")
                second=ws.edit_candidate(expected_sha256=first["after_sha256"],source=(ctx.workspace/"candidate.cu").read_text()+f"// edit {i} b\n")
                self.assertEqual(second["path"],"candidate.cu")
                self.assertNotEqual(second["after_sha256"],self.seed_sha)
                result=queue.submit_continue(episode_id=ctx.episode_id,invocation_id=f"INV-{i}",
                    current_source=(ctx.workspace/"candidate.cu").read_text(),frozen_identity_rule=lambda a,b:a!=b)
                self.assertEqual(result["candidate_id"],"C0001")
            claimed=queue.claim_next_oj("SYNTH-OJ",worker_pid=12345,worker_start_utc="synthetic")
            self.assertIsNotNone(claimed)
            self.assertIsNone(queue.claim_next_oj("SYNTH-OJ-2",worker_pid=12346,worker_start_utc="synthetic"))

    def test_stop_after_workspace_edit_records_edit_without_oj_submission(self):
        with tempfile.TemporaryDirectory() as tmp:
            queue=N0PersistentQueue(Path(tmp)/"runtime")
            episode_id="N0SYNTH-STOP"
            allocation={"authorized":True,"episode_id":episode_id,"replicate_slot":"SYNTH-STOP"}
            paths=queue.register_episode(episode_id=episode_id,replicate_slot="SYNTH-STOP",
                treatment_id="N0_AUTONOMOUS",approved_allocation=allocation,max_candidates=30,
                workspace_files=self.files)
            workspace=WorkspaceTools(paths.workspace)
            workspace.begin_invocation("INV-STOP")
            original=(paths.workspace/"candidate.cu").read_text(encoding="utf-8")
            first_sha=sha256_bytes(original.encode("utf-8"))
            edit=workspace.edit_candidate(expected_sha256=first_sha,source=original+"\n// unsubmitted stop edit\n")
            result=InvocationFinalizer(queue,workspace,frozen_identity_rule=lambda source_sha,last_sha:source_sha!=last_sha)(
                episode_id=episode_id,invocation_id="INV-STOP",turn_id="TURN-1",decision="STOP",reason="synthetic stop")
            state=queue.snapshot()
            self.assertEqual(edit["status"],"EDITED")
            self.assertEqual(result["status"],"STOP_RECORDED")
            self.assertFalse(result["oj_submitted"])
            self.assertEqual(state["oj_requests"],[])
            self.assertEqual((paths.workspace/"candidate.cu").read_text(encoding="utf-8"),original+"\n// unsubmitted stop edit\n")


if __name__ == "__main__":
    unittest.main()
