import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from n0_autonomous.engine import N0Error, canonical_bytes
from n0_autonomous.parallel_runtime import N0PersistentQueue
from n0_autonomous.provider_adapter import N0R5ProviderQueueAdapter
from n0_autonomous.parallel_runtime import N0_COMPILER_CONTRACT_SHA256


class R5ProviderAdapterTests(unittest.TestCase):
    def test_old_queue_without_n0_allowlist_is_rejected_before_submit(self):
        calls=[]
        adapter=N0R5ProviderQueueAdapter(store=None,queue_capability={"status":"PASS","treatment_id":"N0_AUTONOMOUS",
            "provider_release_id":"R19","manifest_sha256":"a"*64,"allowed_treatments":["G_CONTROL"]},
            enqueue=lambda **kwargs:calls.append(kwargs),provider_release_id="R19",provider_manifest_sha256="a"*64)
        with self.assertRaisesRegex(N0Error,"N0_TREATMENT_NOT_ALLOWLISTED"):
            adapter.preflight()
        self.assertEqual(calls,[])

    def test_synthetic_authorized_queue_persists_consumes_once_and_never_replays(self):
        template=ROOT/"tests"/"fixtures"/"N0_AUTONOMOUS_INFORMATION_ACQUISITION_R4"/"template"
        files={name:(template/name).read_bytes() for name in ("task_definition.json","candidate_abi.json","candidate.cu","visible_observation.json")}
        abi=json.loads(files["candidate_abi.json"].decode("utf-8"))
        oj=ROOT.parents[1]/"runtime"/"oj"/"releases"/"N0_KERNELBENCH_L1_40_LAYERNORM_R6_6_KERNEL_NAME_NCU_FILTER"/"RELEASE_MANIFEST.json"
        abi["compiler_contract_sha256"]=N0_COMPILER_CONTRACT_SHA256
        abi["oj_release_id"]="N0_KERNELBENCH_L1_40_LAYERNORM_R6_6_KERNEL_NAME_NCU_FILTER"
        abi["oj_manifest_sha256"]=__import__("hashlib").sha256(oj.read_bytes()).hexdigest().upper()
        files["candidate_abi.json"]=(json.dumps(abi,ensure_ascii=False,indent=2)+"\n").encode("utf-8")
        with tempfile.TemporaryDirectory() as temp:
            store=N0PersistentQueue(Path(temp)/"runtime")
            store.register_episode(episode_id="SYNTH-ADAPTER",replicate_slot="SYNTH-ADAPTER-SLOT",treatment_id="N0_AUTONOMOUS",
                approved_allocation={"authorized":True,"episode_id":"SYNTH-ADAPTER","replicate_slot":"SYNTH-ADAPTER-SLOT"},
                max_candidates=30,workspace_files=files)
            response=json.dumps({"id":"SYNTH-RESP-1","choices":[{"message":{"content":"synthetic"}}]}).encode()
            calls=[]
            def enqueue(**kwargs):
                calls.append(kwargs)
                kwargs["mark_dispatched"]()
                return {"request_id":kwargs["request_id"],"attempt_id":kwargs["attempt_id"],
                        "response_id":"SYNTH-RESP-1","response_body":response,
                        "provider_attempt_ids":["SYNTH-PROVIDER-ATTEMPT"],
                        "provider_request_id":"SYNTH-PROVIDER-REQUEST"}
            adapter=N0R5ProviderQueueAdapter(store=store,queue_capability={"status":"PASS","treatment_id":"N0_AUTONOMOUS",
                "provider_release_id":"SYNTH-R4","manifest_sha256":"b"*64,"allowed_treatments":["N0_AUTONOMOUS"]},
                enqueue=enqueue,provider_release_id="SYNTH-R4",provider_manifest_sha256="b"*64)
            payload=b'{"model":"SYNTH","messages":[{"role":"user","content":"synthetic"}],"tools":[],"tool_choice":"auto"}'
            first=adapter.dispatch(episode_id="SYNTH-ADAPTER",invocation_id="INV-1",request_index=1,payload=payload)
            self.assertEqual(first["response"]["id"],"SYNTH-RESP-1")
            self.assertEqual(first["provider_attempt_ids"],["SYNTH-PROVIDER-ATTEMPT"])
            self.assertEqual(first["provider_request_id"],"SYNTH-PROVIDER-REQUEST")
            self.assertEqual(store.snapshot()["provider_requests"][0]["state"],"CONSUMED")
            with self.assertRaisesRegex(N0Error,"ALREADY_DISPATCHED_OR_UNKNOWN"):
                adapter.dispatch(episode_id="SYNTH-ADAPTER",invocation_id="INV-1",request_index=1,payload=payload)
            self.assertEqual(len(calls),1)


if __name__=="__main__":
    unittest.main()
