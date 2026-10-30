import datetime
import importlib.util
import unittest
from pathlib import Path

RELEASE = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("pool20_r8_1_controller", RELEASE / "pool20_r8_controller.py")
CONTROLLER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CONTROLLER)

def qualified_receipt():
    expires = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=1)).isoformat()
    return {
        "status": "PASS",
        "checks": {"gpu": True, "baseline": True, "toolchain": True},
        "device_snapshot": {"gpu_uuid": CONTROLLER.GPU_UUID},
        "r6_manifest_sha256": CONTROLLER.OJ_SHA,
        "frozen_baseline_sha256": CONTROLLER.BASELINE_SHA,
        "frozen_baseline_qualification_validation": {
            "status": "PASS", "failures": [], "valid_until_utc": expires,
            "baseline_id": "331F8AB3579E47E3926D715D45365376DCB81B0365F19B6BC4B95A89B7F63701",
            "scope": {"gpu_identity": {"gpu_uuid": CONTROLLER.GPU_UUID}},
        },
    }

class QualificationContractTests(unittest.TestCase):
    def test_observed_r3_pass_receipt_is_accepted(self):
        self.assertTrue(CONTROLLER.qualification_valid(qualified_receipt()))

    def test_missing_check_fails_closed(self):
        receipt = qualified_receipt()
        receipt["checks"]["ncu"] = False
        self.assertFalse(CONTROLLER.qualification_valid(receipt))

    def test_gpu_identity_mismatch_fails_closed(self):
        receipt = qualified_receipt()
        receipt["device_snapshot"]["gpu_uuid"] = "GPU-OTHER"
        self.assertFalse(CONTROLLER.qualification_valid(receipt))

    def test_baseline_failure_fails_closed(self):
        receipt = qualified_receipt()
        receipt["frozen_baseline_qualification_validation"]["status"] = "FAIL"
        self.assertFalse(CONTROLLER.qualification_valid(receipt))

    def test_frozen_baseline_identity_mismatch_fails_closed(self):
        receipt = qualified_receipt()
        receipt["frozen_baseline_sha256"] = "0" * 64
        self.assertFalse(CONTROLLER.qualification_valid(receipt))

    def test_expired_qualification_fails_closed(self):
        receipt = qualified_receipt()
        receipt["frozen_baseline_qualification_validation"]["valid_until_utc"] = "2000-01-01T00:00:00+00:00"
        self.assertFalse(CONTROLLER.qualification_valid(receipt))

if __name__ == "__main__":
    unittest.main()
