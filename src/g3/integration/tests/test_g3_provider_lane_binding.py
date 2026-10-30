import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from n0_integration.provider_r20_bridge import _required_provider_window


class G3ProviderLaneBindingTests(unittest.TestCase):
    def test_g3_uses_previously_successful_jlapi2_lane(self):
        self.assertEqual(_required_provider_window("G3"), "B")

    def test_non_g3_lane_selection_is_unchanged(self):
        self.assertIsNone(_required_provider_window("N0_AUTONOMOUS"))


if __name__ == "__main__":
    unittest.main()
