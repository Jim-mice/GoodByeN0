from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import MethodType

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from canonical_harness.paratera_agent import ParateraAgentAdapter
from canonical_harness.provider_retry import ProviderRequestPendingReplay,retryable_provider_error
from canonical_harness.recovery_api import prepare_authorized_exact_request_recovery
from canonical_harness.runner import CanonicalRunner
from canonical_harness.treatment import load_treatments


class PendingReplayR14Tests(unittest.TestCase):
    def test_authorized_cancel_is_replayable_unknown_outcome(self):
        class Cancelled(Exception):
            telemetry={"terminal_status":"CANCELLED_OUTCOME_UNKNOWN"}
        allowed,delay=retryable_provider_error(Cancelled())
        self.assertTrue(allowed)
        self.assertEqual(delay,0)

    def test_provider_failure_becomes_pending_not_scientific_failure(self):
        treatments=load_treatments(ROOT)
        seed=(ROOT/"canonical_harness"/"canonical_seed.cu").read_bytes()
        with tempfile.TemporaryDirectory() as td:
            base=Path(td); workspace=base/"episode"/"model_workspace"; private=base/"episode"/"controller_private"
            class PendingModel:
                model="DeepSeek-V4-Pro"
                def __call__(self,*args,**kwargs):
                    raise ProviderRequestPendingReplay("attempt-1","a"*64,"three attempts timed out")
            runner=CanonicalRunner(ROOT,treatments["G_CONTROL"],seed,model_fn=PendingModel(),
                                   replicate_id="DSV4_G_REP_900",run_id="pending-test",allow_model=True)
            runner.init_workspace(workspace,private)
            before=(workspace/"candidate.cu").read_bytes()
            result=runner.turn(evaluate=False)
            checkpoint=json.loads((private/"checkpoints"/"checkpoint_latest.json").read_text(encoding="utf-8"))
            self.assertEqual(result["termination"],"REQUEST_PENDING_REPLAY")
            self.assertEqual(runner.state.logical_candidates,0)
            self.assertEqual((workspace/"candidate.cu").read_bytes(),before)
            self.assertEqual(checkpoint["phase"],"REQUEST_PENDING_REPLAY")
            self.assertEqual(checkpoint["state"]["turn"],0)
            self.assertEqual(checkpoint["pending_turn"],1)

    def test_successful_replay_is_applied_to_same_runner_turn(self):
        treatments=load_treatments(ROOT)
        seed=(ROOT/"canonical_harness"/"canonical_seed.cu").read_bytes()
        with tempfile.TemporaryDirectory() as td:
            base=Path(td); episode=base/"episode"; workspace=episode/"model_workspace"; private=episode/"controller_private"
            runner=CanonicalRunner(ROOT,treatments["G_CONTROL"],seed,replicate_id="DSV4_G_REP_901",
                                   run_id="resume-test",allow_model=True)
            runner.init_workspace(workspace,private)
            runner._save_checkpoint("REQUEST_PENDING_REPLAY",pending_turn=1,state_turn=0,
                                    pre_turn_candidate=(workspace/"candidate.cu").read_bytes())
            adapter=ParateraAgentAdapter.__new__(ParateraAgentAdapter)
            adapter.model="DeepSeek-V4-Pro"
            adapter.resume_authorized_provider_request=MethodType(
                lambda self,*args,**kwargs:{"recovery_id":"recovery-1","attempt_ids":["replay-1"],
                    "final_text":json.dumps({"decision":"STOP","reason":"recovered model response"})},adapter)
            out=adapter.resume_pending_turn_into_runner(runner,workspace,(),"original-1","b"*64)
            self.assertEqual(out["runner_turn_index"],1)
            self.assertEqual(out["runner_turn"]["termination"],"SEMANTIC_AGENT_STOP")
            self.assertEqual(runner.state.logical_candidates,0)
            events=(episode/"provider_trace"/"provider_attempts.jsonl").read_text(encoding="utf-8")
            self.assertIn("PENDING_PROVIDER_TURN_APPLIED_TO_RUNNER",events)

    def test_recovered_workspace_edit_is_counted_against_saved_pre_turn_source(self):
        treatments=load_treatments(ROOT)
        seed=(ROOT/"canonical_harness"/"canonical_seed.cu").read_bytes()
        import hashlib
        with tempfile.TemporaryDirectory() as td:
            base=Path(td); workspace=base/"episode"/"model_workspace"; private=base/"episode"/"controller_private"
            runner=CanonicalRunner(ROOT,treatments["G_CONTROL"],seed,replicate_id="DSV4_G_REP_902",
                                   run_id="recovered-edit-test",allow_model=True)
            runner.init_workspace(workspace,private)
            edited=seed+b"\n// edit from recovered real model tool call\n"
            (workspace/"candidate.cu").write_bytes(edited)
            out=runner.turn(raw_response=json.dumps({"decision":"CONTINUE","reason":"continue"}),
                            evaluate=False,candidate_before_sha=hashlib.sha256(seed).hexdigest(),
                            candidate_before_bytes=seed)
            self.assertTrue(out["changed"])
            self.assertEqual(runner.state.logical_candidates,1)
            self.assertEqual(out["candidate_sha"],hashlib.sha256(edited).hexdigest())

    def test_recovery_attempt_budget_does_not_reset_across_release_records(self):
        model="DeepSeek-V4-Pro"; effort="high"; messages=[{"role":"user","content":"x"}]
        tools=[]; choice="auto"; tokens=32768
        import sys
        sys.path.insert(0,str(ROOT/"provider_staging"))
        from paratera_chat import ParateraChatCompletions
        digest=ParateraChatCompletions.request_sha256(model,effort,messages,tools,choice,tokens)
        with tempfile.TemporaryDirectory() as td:
            episode=Path(td)/"ep"; trace=episode/"provider_trace"/"provider_attempts.jsonl"; trace.parent.mkdir(parents=True)
            rows=[{"event":"REQUEST_START","attempt_id":"a0","episode_id":"ep","request_sha256":digest,
                   "model":model,"requested_effort":effort,"messages":messages,"tools":tools,
                   "tool_choice":choice,"max_tokens":tokens}]
            for aid in ("a1","a2"):
                rows.append({"event":"REQUEST_START","attempt_id":aid,"episode_id":"ep","request_sha256":digest,
                             "recovery_of":"a0"})
            trace.write_text("".join(json.dumps(x)+"\n" for x in rows),encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError,"ATTEMPT_LIMIT_EXHAUSTED"):
                prepare_authorized_exact_request_recovery(episode,"a0",digest,2)

    def test_one_prior_replay_leaves_only_one_recovery_attempt(self):
        model="DeepSeek-V4-Pro"; effort="high"; messages=[{"role":"user","content":"x"}]
        tools=[]; choice="auto"; tokens=32768
        sys.path.insert(0,str(ROOT/"provider_staging"))
        from paratera_chat import ParateraChatCompletions
        digest=ParateraChatCompletions.request_sha256(model,effort,messages,tools,choice,tokens)
        with tempfile.TemporaryDirectory() as td:
            episode=Path(td)/"ep"; trace=episode/"provider_trace"/"provider_attempts.jsonl"; trace.parent.mkdir(parents=True)
            rows=[{"event":"REQUEST_START","attempt_id":"a0","episode_id":"ep","request_sha256":digest,
                   "model":model,"requested_effort":effort,"messages":messages,"tools":tools,
                   "tool_choice":choice,"max_tokens":tokens},
                  {"event":"REQUEST_START","attempt_id":"a1","episode_id":"ep","request_sha256":digest,"recovery_of":"a0"}]
            trace.write_text("".join(json.dumps(x)+"\n" for x in rows),encoding="utf-8")
            prepared=prepare_authorized_exact_request_recovery(episode,"a0",digest,2)
            self.assertEqual(prepared["attempts_already_used"],2)
            self.assertEqual(prepared["new_attempt_limit"],1)


if __name__=="__main__": unittest.main(verbosity=2)
