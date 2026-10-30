from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from n0_integration.online_best import classify_online_best


class OnlineBestTests(unittest.TestCase):
    def setUp(self):
        self.incumbent = {"baseline_id":"base", "score_version":"N0_FROZEN_REFERENCE_SCORE_ONLINE_V2",
                          "candidate_runtime_ms":2.0, "candidate_sha256":"A"}

    def measured(self, runtime, source="B", baseline="base"):
        return {"baseline_id":baseline, "score_version":"N0_FROZEN_REFERENCE_SCORE_ONLINE_V2",
                "candidate_runtime_ms":runtime, "candidate_sha256":source}

    def test_clearer_candidate_becomes_explicitly_provisional(self):
        self.assertEqual(classify_online_best(self.measured(1.96), self.incumbent),
                         ("ONLINE_PROVISIONAL_BEST", "CANDIDATE_RUNTIME_IMPROVED_BEYOND_FROZEN_MARGIN"))

    def test_regression_does_not_promote(self):
        self.assertEqual(classify_online_best(self.measured(2.04), self.incumbent)[0], "CONFIRMED_REGRESSION")

    def test_small_difference_is_uncertain(self):
        self.assertEqual(classify_online_best(self.measured(1.99), self.incumbent)[0], "UNCERTAIN")

    def test_same_source_is_not_promoted_again(self):
        self.assertEqual(classify_online_best(self.measured(1.0, source="A"), self.incumbent)[0], "UNCERTAIN")

    def test_reference_or_score_version_mismatch_is_uncertain(self):
        self.assertEqual(classify_online_best(self.measured(1.0, baseline="old"), self.incumbent)[0], "UNCERTAIN")
        changed = dict(self.incumbent, score_version="old")
        self.assertEqual(classify_online_best(self.measured(1.0), changed)[0], "UNCERTAIN")


if __name__ == "__main__":
    unittest.main()
