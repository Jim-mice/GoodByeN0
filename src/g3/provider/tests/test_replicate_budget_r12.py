import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "canonical_harness"))
from campaign_extension import allocate_next_replicate, allocate_recovery_id
sys.path.insert(0, str(ROOT))
import run_deepseek_formal_episode as formal_runner

TREATMENTS = {"G_CONTROL": "G", "H_STATIC": "HS", "H_RETRIEVAL_V7": "HR", "E_EXPERT": "E"}


class ReplicateBudgetR12Tests(unittest.TestCase):
    def setup_case(self, root, target=50):
        amendment = root / "amendment.json"
        amendment.write_text(json.dumps({
            "amendment_id": "DEEPSEEK_V7_REPLICATE_EXTENSION_50",
            "scope": "prospective_only",
            "target_valid_replicates_per_treatment": target,
            "total_target_valid_episodes": target * 4,
            "recovery_consumes_additional_valid_slot": False,
            "allocation_ledger": "allocations.json",
            "historical_allocation_ledgers": ["r11_allocations.json"],
        }), encoding="utf-8")
        return amendment

    def write_campaign(self, root, episodes):
        (root / "campaign_ledger.json").write_text(json.dumps({"episodes": episodes, "active": []}), encoding="utf-8")

    def eid(self, treatment, number, recovery=None):
        value = f"DSV4_{TREATMENTS[treatment]}_REP_{number:03d}"
        return value + (f"_REC{recovery:02d}" if recovery else "")

    def test_allocation_continues_after_ten_valid(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td); amendment = self.setup_case(root)
            episodes = {self.eid("G_CONTROL", i): {"treatment":"G_CONTROL", "scientific_valid":True, "status":"VALID"} for i in range(1, 11)}
            self.write_campaign(root, episodes)
            result = allocate_next_replicate(root, "G_CONTROL", amendment)
            self.assertEqual(result["episode_id"], "DSV4_G_REP_011")
            self.assertEqual(result["valid_replicate_slot_index"], 11)

    def test_serial_number_may_exceed_fifty(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td); amendment = self.setup_case(root)
            self.write_campaign(root, {self.eid("H_STATIC", i): {"treatment":"H_STATIC", "scientific_valid":False, "status":"INFRASTRUCTURE_INCIDENT"} for i in range(1, 51)})
            result = allocate_next_replicate(root, "H_STATIC", amendment)
            self.assertEqual(result["episode_id"], "DSV4_HS_REP_051")
            self.assertEqual(result["valid_replicate_slot_index"], 1)

    def test_exactly_fifty_valid_stops_allocation(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td); amendment = self.setup_case(root)
            self.write_campaign(root, {self.eid("E_EXPERT", i): {"treatment":"E_EXPERT", "scientific_valid":True, "status":"VALID"} for i in range(1, 51)})
            with self.assertRaisesRegex(RuntimeError, "VALID_REPLICATE_BUDGET_REACHED"):
                allocate_next_replicate(root, "E_EXPERT", amendment)

    def test_invalid_episode_does_not_consume_valid_quota(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td); amendment = self.setup_case(root)
            self.write_campaign(root, {self.eid("G_CONTROL", 1): {"treatment":"G_CONTROL", "scientific_valid":False, "status":"INFRASTRUCTURE_INCIDENT"}})
            result = allocate_next_replicate(root, "G_CONTROL", amendment)
            self.assertEqual(result["episode_id"], "DSV4_G_REP_002")
            self.assertEqual(result["valid_replicate_slot_index"], 1)

    def test_recovery_is_same_sample_and_does_not_consume_slot(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td); amendment = self.setup_case(root)
            parent = self.eid("H_RETRIEVAL_V7", 1)
            self.write_campaign(root, {parent: {"treatment":"H_RETRIEVAL_V7", "scientific_valid":True, "status":"VALID"}, parent+"_REC01": {"treatment":"H_RETRIEVAL_V7", "scientific_valid":True, "status":"VALID", "recovery_of":parent}})
            (root / "allocations.json").write_text(json.dumps({"allocations":[{"episode_id":parent,"treatment":"H_RETRIEVAL_V7","amendment_id":"DEEPSEEK_V7_REPLICATE_EXTENSION_50","valid_replicate_slot_index":1,"state":"VALID","scientific_valid":True}],"recovery_allocations":[]}),encoding="utf-8")
            recovery = allocate_recovery_id(root, "H_RETRIEVAL_V7", parent, amendment)
            self.assertFalse(recovery["consumes_valid_replicate_slot"])
            self.assertEqual(recovery["valid_replicate_slot_index"], 1)
            next_item = allocate_next_replicate(root, "H_RETRIEVAL_V7", amendment)
            self.assertEqual(next_item["valid_replicate_slot_index"], 2)

    def test_four_treatments_have_isolated_quota_and_ids(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td); amendment=self.setup_case(root); self.write_campaign(root,{})
            results=[allocate_next_replicate(root,treatment,amendment) for treatment in TREATMENTS]
            self.assertEqual({x["treatment"] for x in results},set(TREATMENTS))
            self.assertEqual(len({x["episode_id"] for x in results}),4)
            self.assertEqual({x["valid_replicate_slot_index"] for x in results},{1})

    def test_active_claim_reserves_slot_and_cannot_be_duplicated(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td); amendment=self.setup_case(root); self.write_campaign(root,{})
            first=allocate_next_replicate(root,"G_CONTROL",amendment)
            second=allocate_next_replicate(root,"G_CONTROL",amendment)
            self.assertEqual(first["valid_replicate_slot_index"],1)
            self.assertEqual(second["valid_replicate_slot_index"],2)
            self.assertNotEqual(first["episode_id"],second["episode_id"])

    def test_fifty_unstarted_claims_block_a_fifty_first_claim(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td); amendment=self.setup_case(root); self.write_campaign(root,{})
            for _ in range(50):
                allocate_next_replicate(root,"G_CONTROL",amendment)
            with self.assertRaisesRegex(RuntimeError,"NO_UNCLAIMED_VALID_REPLICATE_SLOTS"):
                allocate_next_replicate(root,"G_CONTROL",amendment)

    def test_cross_process_lock_allocates_distinct_ids_and_slots(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td); amendment=self.setup_case(root); self.write_campaign(root,{})
            module_path=str(ROOT/"canonical_harness")
            code=("import sys,json;sys.path.insert(0,"+repr(module_path)+");"
                  "from pathlib import Path;from campaign_extension import allocate_next_replicate;"
                  "print(json.dumps(allocate_next_replicate(Path("+repr(str(root))+"),'H_STATIC',Path("+repr(str(amendment))+"))))")
            children=[subprocess.Popen([sys.executable,"-c",code],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True) for _ in range(4)]
            outputs=[child.communicate(timeout=20) for child in children]
            self.assertTrue(all(child.returncode==0 for child in children),outputs)
            items=[json.loads(out[0]) for out in outputs]
            self.assertEqual(len({x["episode_id"] for x in items}),4)
            self.assertEqual(len({x["valid_replicate_slot_index"] for x in items}),4)

    def test_runner_accepts_serial_over_fifty_by_allocated_slot(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td); amendment=self.setup_case(root)
            amendment.write_text(amendment.read_text(encoding="utf-8").replace('"allocation_ledger": "allocations.json"','"allocation_ledger": "allocations.json"'),encoding="utf-8")
            ledger={"allocations":[{"episode_id":"DSV4_G_REP_051","treatment":"G_CONTROL","amendment_id":"DEEPSEEK_V7_REPLICATE_EXTENSION_50","valid_replicate_slot_index":7,"state":"ALLOCATED_NOT_STARTED"}]}
            (root/"allocations.json").write_text(json.dumps(ledger),encoding="utf-8")
            original_amendment,original_ledger=formal_runner.EXTENSION_AMENDMENT,formal_runner.EXTENSION_LEDGER
            try:
                formal_runner.EXTENSION_AMENDMENT=amendment
                formal_runner.EXTENSION_LEDGER=root/"allocations.json"
                formal_runner.validate_extension_allocation("DSV4_G_REP_051","G_CONTROL",7)
                with self.assertRaisesRegex(RuntimeError,"EXTENSION_EPISODE_ID_NOT_ALLOCATED_FOR_TREATMENT"):
                    formal_runner.validate_extension_allocation("DSV4_G_REP_051","G_CONTROL",8)
            finally:
                formal_runner.EXTENSION_AMENDMENT=original_amendment
                formal_runner.EXTENSION_LEDGER=original_ledger


if __name__ == "__main__":
    unittest.main()
