import sys
import unittest
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from n0_autonomous.engine import N0Error
from n0_autonomous.provider_adapter import N0R5ProviderQueueAdapter

class G3ProviderRouteTests(unittest.TestCase):
    def test_g3_requires_exact_policy_capability(self):
        cap={"status":"PASS","treatment_id":"G3","group":"G3","task_id":"KB_L1_40_LAYERNORM",
             "hardware_information":"FULL","profile_information":"FULL","expert_knowledge":"ABSENT",
             "provider_release_id":"G3-R20","manifest_sha256":"a"*64,"allowed_treatments":["G3"]}
        adapter=N0R5ProviderQueueAdapter(store=None,queue_capability=cap,enqueue=lambda **kw:{},
            provider_release_id="G3-R20",provider_manifest_sha256="a"*64,treatment_id="G3")
        self.assertEqual(adapter.preflight()["treatment_id"],"G3")

    def test_g3_cannot_be_relabelled_or_misconfigured(self):
        cap={"status":"PASS","treatment_id":"N0_AUTONOMOUS","group":"G3","task_id":"KB_L1_40_LAYERNORM",
             "hardware_information":"FULL","profile_information":"FULL","expert_knowledge":"ABSENT",
             "provider_release_id":"G3-R20","manifest_sha256":"a"*64,"allowed_treatments":["N0_AUTONOMOUS","G3"]}
        adapter=N0R5ProviderQueueAdapter(store=None,queue_capability=cap,enqueue=lambda **kw:{},
            provider_release_id="G3-R20",provider_manifest_sha256="a"*64,treatment_id="G3")
        with self.assertRaisesRegex(N0Error,"TREATMENT_NOT_AUTHORIZED"):
            adapter.preflight()

if __name__=="__main__": unittest.main()
