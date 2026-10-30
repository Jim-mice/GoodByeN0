from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from canonical_harness.recovery_api import prepare_model_route_correction, record_model_route_correction
from provider_staging.paratera_chat import ALLOWED_MODELS, ParateraChatCompletions


class ModelRouteCorrectionTests(unittest.TestCase):
    def test_runner_adapter_and_worker_are_pinned_to_corrected_release(self):
        root = Path(__file__).resolve().parents[1]
        config = json.loads((root / "configs" / "model_request_config_v7.json").read_text(encoding="utf-8"))
        matrix = json.loads((root / "configs" / "treatment_matrix_v7.json").read_text(encoding="utf-8"))
        runner = (root / "run_deepseek_formal_episode.py").read_text(encoding="utf-8")
        worker = (root / "tools" / "provider_queue_worker.py").read_text(encoding="utf-8")
        queue = (root / "canonical_harness" / "provider_queue.py").read_text(encoding="utf-8")
        self.assertEqual(config["models"], ["DeepSeek-V4-Pro-0813"])
        self.assertIn("DeepSeek-V4-Pro-0813", ALLOWED_MODELS)
        self.assertEqual(matrix["models"], ["DeepSeek-V4-Pro-0813"])
        self.assertIn('ParateraAgentAdapter("DeepSeek-V4-Pro-0813"', runner)
        self.assertIn("HARNESS_RELEASE_MANIFEST_R16_ROUTE_0813.json", worker)
        self.assertIn('model != "DeepSeek-V4-Pro-0813"', queue)

    def make_episode(self, replay_count: int = 0):
        tmp = tempfile.TemporaryDirectory()
        root = Path(tmp.name) / "DSV4_G_REP_006"
        trace = root / "provider_trace" / "provider_attempts.jsonl"
        trace.parent.mkdir(parents=True)
        payload = ParateraChatCompletions.build_body(
            "DeepSeek-V4-Pro", "high", [{"role":"user","content":"continue"}],
            [{"type":"function","function":{"name":"edit_candidate","parameters":{"type":"object"}}}],
            "auto", 32768)
        raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        digest = hashlib.sha256(raw).hexdigest()
        events = [{"event":"REQUEST_START","attempt_id":"old-attempt-1","request_id":"old-request",
                   "attempt_lineage_id":"lineage-1","request_sha256":digest,"model":payload["model"],
                   "requested_effort":payload["reasoning_effort"],"messages":payload["messages"],
                   "tools":payload["tools"],"tool_choice":payload["tool_choice"],"max_tokens":payload["max_tokens"]}]
        events.append({"event":"REQUEST_NO_RESPONSE","attempt_id":"old-attempt-1","terminal_status":"TIMEOUT_OUTCOME_UNKNOWN"})
        for n in range(replay_count):
            events.append({"event":"REQUEST_START","attempt_id":f"old-attempt-{n+2}",
                           "request_sha256":digest,"model":payload["model"],"replay_of":events[-1]["attempt_id"]})
        trace.write_text("".join(json.dumps(item)+"\n" for item in events), encoding="utf-8")
        candidate = root / "candidate.cu"
        candidate.write_text("seed", encoding="utf-8")
        return tmp, root, digest, candidate

    def test_model_field_only_changes_and_prior_attempt_counts(self):
        tmp, root, original_sha, candidate = self.make_episode(replay_count=1)
        try:
            before = candidate.read_bytes()
            correction = prepare_model_route_correction(root, "old-attempt-1", original_sha)
            body = correction["request_body"]
            self.assertEqual(body["model"], "DeepSeek-V4-Pro-0813")
            old_body = dict(body, model="DeepSeek-V4-Pro")
            self.assertEqual({k:v for k,v in old_body.items() if k != "model"},
                             {k:v for k,v in body.items() if k != "model"})
            self.assertEqual(correction["attempts_already_used"], 2)
            self.assertEqual(correction["max_attempts_remaining"], 1)
            self.assertTrue(correction["attempt_lineage_id"].startswith("lineage-1:MODEL_ROUTE_CORRECTION:"))
            self.assertEqual(correction["previous_attempt_lineage_id"], "lineage-1")
            self.assertNotEqual(correction["original_request_sha256"], correction["new_request_sha256"])
            record = record_model_route_correction(root, correction, "new-request")
            self.assertEqual(record["original_request_id"], "old-request")
            self.assertEqual(record["new_request_id"], "new-request")
            self.assertEqual(record["original_request_sha256"], original_sha)
            self.assertEqual(record["new_request_sha256"], correction["new_request_sha256"])
            self.assertEqual(record["previous_attempt_lineage_id"], "lineage-1")
            self.assertEqual(candidate.read_bytes(), before)
        finally:
            tmp.cleanup()

    def test_exhausted_attempt_lineage_is_not_reset_by_route_change(self):
        tmp, root, digest, _ = self.make_episode(replay_count=2)
        try:
            with self.assertRaisesRegex(RuntimeError, "PROVIDER_REQUEST_ATTEMPT_LIMIT_EXHAUSTED"):
                prepare_model_route_correction(root, "old-attempt-1", digest)
        finally:
            tmp.cleanup()

    def test_completed_response_cannot_be_route_replayed(self):
        tmp, root, digest, _ = self.make_episode()
        try:
            trace = root / "provider_trace" / "provider_attempts.jsonl"
            with trace.open("a", encoding="utf-8") as stream:
                stream.write(json.dumps({"event":"RESPONSE_RECEIVED","attempt_id":"old-attempt-1"})+"\n")
            with self.assertRaisesRegex(RuntimeError, "COMPLETE_RESPONSE_EXISTS"):
                prepare_model_route_correction(root, "old-attempt-1", digest)
        finally:
            tmp.cleanup()


if __name__ == "__main__":
    unittest.main()
