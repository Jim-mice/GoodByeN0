import os, sys, tempfile, unittest
from pathlib import Path

PROVIDER=Path(os.environ.get("N0_G3_PROVIDER_RELEASE_ROOT", Path(__file__).resolve().parents[2]/"N0_PARATERA_PROVIDER_R20_G3_ROUTE_R2_HTTP_IDENTITY")).resolve()
if not PROVIDER.is_dir(): raise RuntimeError("G3_PROVIDER_RELEASE_ROOT_MISSING")
sys.path.insert(0,str(PROVIDER))
from canonical_harness.provider_queue import SharedProviderQueue

class G3ProviderQueueAdmissionTests(unittest.TestCase):
    def test_g3_is_persisted_as_g3_without_http_dispatch(self):
        root=Path(r"D:\projects\kernelagent\runtime\tmp")
        root.mkdir(parents=True,exist_ok=True)
        with tempfile.TemporaryDirectory(dir=root) as temp:
            q=SharedProviderQueue(Path(temp))
            row=q.submit(episode_id="G3-DIAG-E01",treatment="G3",model="DeepSeek-V4-Pro-0813",
                request_body={"model":"DeepSeek-V4-Pro-0813","messages":[{"role":"user","content":"x"}]},
                attempt_lineage_id="G3-DIAG-INV1",max_attempts=1)
            self.assertEqual(row["treatment"],"G3")
            self.assertEqual(row["status"],"QUEUED")

    def test_unapproved_treatment_still_rejected(self):
        root=Path(r"D:\projects\kernelagent\runtime\tmp")
        with tempfile.TemporaryDirectory(dir=root) as temp:
            q=SharedProviderQueue(Path(temp))
            with self.assertRaisesRegex(ValueError,"TREATMENT_NOT_ALLOWLISTED"):
                q.submit(episode_id="X",treatment="H_RETRIEVAL_V7",model="DeepSeek-V4-Pro-0813",
                    request_body={"model":"DeepSeek-V4-Pro-0813","messages":[]},attempt_lineage_id="lineage",max_attempts=1)

if __name__=="__main__": unittest.main()
