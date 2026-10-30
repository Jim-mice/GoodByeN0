from __future__ import annotations

import threading
import time
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

from canonical_harness.provider_queue import (
    MAX_ATTEMPTS,
    TIMEOUT_SECONDS,
    WINDOWS,
    ProviderQueueService,
    SharedProviderQueue,
)


class FakeClient:
    def __init__(self, counts: dict, lock: threading.Lock):
        self.counts = counts
        self.lock = lock

    def complete(self, model, effort, messages, tools, tool_choice, max_tokens, **kwargs):
        with self.lock:
            self.counts["active"] += 1
            self.counts["peak"] = max(self.counts["peak"], self.counts["active"])
        time.sleep(0.15)
        with self.lock:
            self.counts["active"] -= 1
        payload = {"id": "resp-" + str(messages[0]["content"]), "model": model,
                   "choices": [{"message": {"content": "{}"}, "finish_reason": "stop"}]}
        evidence = SimpleNamespace(request_id="http-" + str(messages[0]["content"]),
                                   response_id=payload["id"], returned_model=model,
                                   usage={"total_tokens": 4}, telemetry={})
        return payload, evidence


class N0ProviderR20Tests(unittest.TestCase):
    def test_n0_only_scope_four_lanes_and_retry_contract(self):
        self.assertEqual(set(WINDOWS), {"A", "B", "C", "D"})
        self.assertEqual(WINDOWS, {"A": "jlapi", "B": "jlapi2", "C": "jlapi", "D": "jlapi2"})
        self.assertEqual(MAX_ATTEMPTS, 3)
        self.assertEqual(TIMEOUT_SECONDS, 1800)
        with TemporaryDirectory() as tmp:
            q = SharedProviderQueue(tmp)
            with self.assertRaisesRegex(ValueError, "TREATMENT_NOT_ALLOWLISTED"):
                q.submit(episode_id="old", treatment="G_CONTROL", model="DeepSeek-V4-Pro-0813",
                         request_body={"model": "DeepSeek-V4-Pro-0813", "messages": []},
                         attempt_lineage_id="old-lineage")

    def test_four_independent_lanes_complete_concurrently(self):
        with TemporaryDirectory() as tmp:
            q = SharedProviderQueue(tmp)
            for i, window in enumerate(WINDOWS):
                body = {"model": "DeepSeek-V4-Pro-0813", "reasoning_effort": "high",
                        "messages": [{"role": "user", "content": str(i) + ("x" * 40000)}], "tools": [], "tool_choice": "auto"}
                q.submit(episode_id=f"SMOKE-{i}", treatment="N0_AUTONOMOUS", model=body["model"],
                         request_body=body, attempt_lineage_id=f"lineage-{i}",
                         request_id=f"req-{i}", required_window=window)
            counts = {"active": 0, "peak": 0}
            lock = threading.Lock()
            service = ProviderQueueService(tmp, client_factory=lambda _alias: FakeClient(counts, lock), poll_seconds=0.01)
            threads = [threading.Thread(target=service.run_window, args=(window,), daemon=True) for window in WINDOWS]
            for thread in threads:
                thread.start()
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                if all((q.results / f"req-{i}.json").is_file() for i in range(4)):
                    break
                time.sleep(0.02)
            service.stop_event.set()
            for thread in threads:
                thread.join(timeout=2)
            self.assertTrue(all(not thread.is_alive() for thread in threads))
            self.assertEqual(counts["peak"], 4)
            for i in range(4):
                result = q.wait(f"req-{i}")
                self.assertEqual(result["status"], "COMPLETED")
                self.assertEqual(result["payload"]["choices"][0]["message"]["content"], "{}")
            followup = {"model": "DeepSeek-V4-Pro-0813", "reasoning_effort": "high",
                        "messages": [{"role": "user", "content": "z" * 40000}], "tools": [], "tool_choice": "auto"}
            q.submit(episode_id="SMOKE-FOLLOWUP", treatment="N0_AUTONOMOUS", model=followup["model"],
                     request_body=followup, attempt_lineage_id="lineage-followup",
                     request_id="req-followup", required_window="A")


if __name__ == "__main__":
    unittest.main()
