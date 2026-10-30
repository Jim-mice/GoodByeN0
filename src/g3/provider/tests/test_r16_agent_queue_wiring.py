import os
import unittest
from unittest.mock import patch

from canonical_harness.paratera_agent import _complete_provider_turn
from provider_staging.paratera_chat import ParateraChatCompletions


class R16AgentQueueWiringTests(unittest.TestCase):
    def setUp(self):
        self.old_root = os.environ.get("NOVELTY_V7_PROVIDER_QUEUE_ROOT")
        os.environ["NOVELTY_V7_PROVIDER_QUEUE_ROOT"] = r"C:\r16-test-queue"
        self.body = ParateraChatCompletions.build_body(
            "DeepSeek-V4-Pro-0813", "high", [{"role": "user", "content": "x"}], [], "auto", 32)

    def tearDown(self):
        if self.old_root is None:
            os.environ.pop("NOVELTY_V7_PROVIDER_QUEUE_ROOT", None)
        else:
            os.environ["NOVELTY_V7_PROVIDER_QUEUE_ROOT"] = self.old_root

    def test_agent_turn_submits_directly_to_r16_and_returns_real_result_shape(self):
        result = {"status": "COMPLETED", "request_id": "queue-r", "request_body_sha256": None,
                  "attempt_lineage_id": "lineage", "episode_id": "ep", "treatment": "G_CONTROL",
                  "attempt_ids": ["a1"], "assigned_window": "A", "worker_key_alias": "jlapi",
                  "attempts_used": 1, "payload": {"model": "DeepSeek-V4-Pro-0813", "id": "response-1",
                  "choices": [{"message": {"content": "ok"}}], "usage": {"completion_tokens": 1}},
                  "usage": {"completion_tokens": 1}, "response_id": "response-1"}
        import hashlib, json
        result["request_body_sha256"] = hashlib.sha256(json.dumps(self.body, ensure_ascii=False,
            separators=(",", ":")).encode("utf-8")).hexdigest()
        with patch("canonical_harness.paratera_agent._r16_submit_and_wait", return_value=result) as submit:
            payload, evidence, meta = _complete_provider_turn(
                ParateraChatCompletions, model="DeepSeek-V4-Pro-0813", effort="high",
                messages=self.body["messages"], tools=[], tool_choice="auto", max_tokens=32,
                initial_attempt_id="initial", initial_artifact_path=__import__("pathlib").Path("unused"),
                episode_id="ep", request_sha256=result["request_body_sha256"], treatment="G_CONTROL",
                on_event=None, attempt_lineage_id="lineage")
        self.assertEqual(payload["id"], "response-1")
        self.assertEqual(evidence.returned_model, "DeepSeek-V4-Pro-0813")
        self.assertTrue(meta["queue_owned_retries"])
        self.assertEqual(submit.call_args.kwargs["attempt_lineage_id"], "lineage")
        self.assertEqual(submit.call_args.kwargs["attempts_already_used"], 0)
        self.assertEqual(submit.call_args.kwargs["max_attempts"], 3)

    def test_queue_result_identity_mismatch_fails_closed(self):
        import hashlib, json
        valid_sha = hashlib.sha256(json.dumps(self.body, ensure_ascii=False,
            separators=(",", ":")).encode("utf-8")).hexdigest()
        result = {"status": "COMPLETED", "request_body_sha256": valid_sha, "attempt_lineage_id": "other-lineage",
                  "episode_id": "ep", "treatment": "G_CONTROL"}
        with patch("canonical_harness.paratera_agent._r16_submit_and_wait", return_value=result):
            with self.assertRaisesRegex(RuntimeError, "R16_QUEUE_RESULT_PROVENANCE_MISMATCH"):
                _complete_provider_turn(ParateraChatCompletions, model="DeepSeek-V4-Pro-0813", effort="high",
                    messages=self.body["messages"], tools=[], tool_choice="auto", max_tokens=32,
                    initial_attempt_id="initial", initial_artifact_path=__import__("pathlib").Path("unused"),
                    episode_id="ep", request_sha256=valid_sha, treatment="G_CONTROL", on_event=None,
                    attempt_lineage_id="lineage")


if __name__ == "__main__":
    unittest.main()
