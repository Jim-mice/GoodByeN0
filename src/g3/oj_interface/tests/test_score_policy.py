import unittest

from n0_kernelbench_oj.score_policy import settle_measurement, assess_candidate_sample_stability
from n0_kernelbench_oj.measurement_trust import (assess_against_frozen_baseline,
    assess_reference_path_agreement, TIMER_PROTOCOL)


class ScorePolicyTests(unittest.TestCase):
    def best(self, samples, baseline="BASELINE_A"):
        return settle_measurement(baseline_id=baseline, frozen_reference_ms=7.35,
                                  candidate_samples_ms=samples, prior_best=None)

    def test_same_source_and_baseline_have_same_score(self):
        a = self.best([1.48] * 10)
        b = self.best([1.48] * 10)
        self.assertEqual(a["score"], b["score"])
        self.assertEqual(a["baseline_id"], b["baseline_id"])

    def test_slower_candidate_cannot_promote_from_reference_drift(self):
        best = self.best([2.47] * 10)
        slower = settle_measurement(baseline_id="BASELINE_A", frozen_reference_ms=7.35,
                                    candidate_samples_ms=[2.60] * 10, prior_best=best)
        self.assertFalse(slower["promotion"])
        self.assertLess(slower["score"], best["score"])

    def test_clear_improvement_promotes(self):
        best = self.best([2.60] * 10)
        improved = settle_measurement(baseline_id="BASELINE_A", frozen_reference_ms=7.35,
                                      candidate_samples_ms=[2.00] * 10, prior_best=best)
        self.assertTrue(improved["promotion"])

    def test_uncertain_overlap_does_not_promote(self):
        best = self.best([2.50, 2.52, 2.48, 2.51, 2.49, 2.50, 2.51, 2.49, 2.50, 2.50])
        nearby = settle_measurement(baseline_id="BASELINE_A", frozen_reference_ms=7.35,
                                    candidate_samples_ms=[2.49, 2.51, 2.50, 2.52, 2.48, 2.50, 2.49, 2.51, 2.50, 2.50],
                                    prior_best=best)
        self.assertFalse(nearby["promotion"])
        self.assertEqual(nearby["promotion_status"], "UNCERTAIN")

    def test_cross_baseline_and_score_version_are_not_comparable(self):
        best = self.best([2.50] * 10)
        mismatch = settle_measurement(baseline_id="BASELINE_B", frozen_reference_ms=7.35,
                                      candidate_samples_ms=[2.0] * 10, prior_best=best)
        self.assertIsNone(mismatch["score"])
        self.assertEqual(mismatch["status"], "OJ_INFRASTRUCTURE_HOLD_BASELINE_MISMATCH")

    def test_invalid_measurement_has_no_score_or_promotion(self):
        invalid = settle_measurement(baseline_id="BASELINE_A", frozen_reference_ms=7.35,
                                     candidate_samples_ms=[float("nan")] * 10, prior_best=None)
        self.assertIsNone(invalid["score"])
        self.assertFalse(invalid["promotion"])

    def test_equal_runtime_does_not_promote(self):
        best = self.best([2.50] * 10)
        repeated = settle_measurement(baseline_id="BASELINE_A", frozen_reference_ms=7.35,
                                      candidate_samples_ms=[2.50] * 10, prior_best=best)
        self.assertFalse(repeated["promotion"])

    def test_frozen_margin_classifies_clear_improvement_and_regression(self):
        best = self.best([2.50] * 10)
        improved = settle_measurement(baseline_id="BASELINE_A", frozen_reference_ms=7.35,
                                      candidate_samples_ms=[2.45] * 10, prior_best=best)
        regression = settle_measurement(baseline_id="BASELINE_A", frozen_reference_ms=7.35,
                                       candidate_samples_ms=[2.55] * 10, prior_best=best)
        uncertain = settle_measurement(baseline_id="BASELINE_A", frozen_reference_ms=7.35,
                                      candidate_samples_ms=[2.49] * 10, prior_best=best)
        self.assertTrue(improved["promotion"])
        self.assertEqual(improved["promotion_status"], "ONLINE_PROVISIONAL_BEST")
        self.assertEqual(regression["promotion_status"], "CONFIRMED_REGRESSION")
        self.assertEqual(uncertain["promotion_status"], "UNCERTAIN")
        self.assertEqual(improved["score_version"], "N0_FROZEN_REFERENCE_SCORE_ONLINE_V2")

    def test_unstable_candidate_event_batch_is_held_without_score(self):
        raw = [2.478, 2.472, 2.474, 2.471, 2.474, 3.166, 2.474, 3.158, 2.478, 3.024]
        stability = assess_candidate_sample_stability(raw)
        self.assertEqual(stability["status"], "HOLD_CANDIDATE_TIMING_UNSTABLE")
        held = settle_measurement(baseline_id="BASELINE_A", frozen_reference_ms=7.35,
                                  candidate_samples_ms=raw, prior_best=None)
        self.assertEqual(held["status"], "OJ_INFRASTRUCTURE_HOLD_CANDIDATE_TIMING_UNSTABLE")
        self.assertIsNone(held["score"])
        self.assertFalse(held["promotion"])

    def test_live_reference_drift_outside_empirical_band_holds(self):
        frozen = {"baseline_id": "BASELINE_A", "runtime_stats_ms": {"mean": 7.35},
                  "monitoring": {"relative_deviation_p99": 0.025}}
        evidence = {"raw_samples_ms": [8.29] * 10}
        assessment = assess_against_frozen_baseline(evidence, frozen)
        self.assertFalse(assessment["pass"])
        self.assertGreater(assessment["relative_deviation"], assessment["empirical_p99_limit"])

    def test_control_measurement_within_band_passes(self):
        frozen = {"baseline_id": "BASELINE_A", "runtime_stats_ms": {"mean": 7.35},
                  "monitoring": {"relative_deviation_p99": 0.025}}
        evidence = {"raw_samples_ms": [7.36] * 10}
        self.assertTrue(assess_against_frozen_baseline(evidence, frozen)["pass"])

    def test_score_version_mismatch_is_not_comparable(self):
        best = self.best([2.50] * 10)
        best["score_version"] = "OLD_SCORE_VERSION"
        outcome = settle_measurement(baseline_id="BASELINE_A", frozen_reference_ms=7.35,
                                     candidate_samples_ms=[2.0] * 10, prior_best=best)
        self.assertIsNone(outcome["score"])
        self.assertEqual(outcome["status"], "OJ_INFRASTRUCTURE_HOLD_SCORE_VERSION_MISMATCH")

    def test_duplicate_capture_receipt_is_rejected(self):
        evidence = {"capture_id": "same", "protocol": dict(TIMER_PROTOCOL),
                    "timing_source_sha256": "A" * 64, "raw_samples_ms": [7.3] * 10}
        outcome = assess_reference_path_agreement(evidence, dict(evidence))
        self.assertFalse(outcome["pass"])
        self.assertEqual(outcome["failure"], "DUPLICATE_REFERENCE_CAPTURE_ID")

    def test_malformed_receipt_samples_are_rejected(self):
        control = {"capture_id": "control", "protocol": dict(TIMER_PROTOCOL),
                   "timing_source_sha256": "A" * 64, "raw_samples_ms": [7.3] * 10}
        evaluator = {"capture_id": "evaluator", "protocol": dict(TIMER_PROTOCOL),
                     "timing_source_sha256": "A" * 64, "raw_samples_ms": [7.3] * 9}
        self.assertFalse(assess_reference_path_agreement(control, evaluator)["pass"])


if __name__ == "__main__":
    unittest.main()
