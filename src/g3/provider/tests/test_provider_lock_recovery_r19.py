from __future__ import annotations

import errno
import json
import os
import tempfile
import threading
import time
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

import canonical_harness.provider_queue as queue_module
from canonical_harness.provider_queue import LockAcquisitionTimeout, ProviderQueueService, SharedProviderQueue


class FakeClient:
    def complete(self, model, effort, messages, tools, tool_choice, max_tokens, attempt_id=None,
                 response_artifact_path=None):
        payload = {"id":"response-" + str(attempt_id),"model":model,
                   "choices":[{"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}
        return payload, type("Evidence", (), {"request_id":"provider-request","response_id":payload["id"],
            "returned_model":model,"usage":{},"telemetry":{"body_complete":True}})()


class ProviderLockRecoveryR19Tests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name) / "queue" / "provider"
        os.environ["jlapi"] = "offline-test-a"
        os.environ["jlapi2"] = "offline-test-b"

    def tearDown(self):
        os.environ.pop("jlapi", None)
        os.environ.pop("jlapi2", None)
        self.temp.cleanup()

    def submit(self, q, *, content="payload", lineage="lineage", request_id=None):
        return q.submit(episode_id="EP-R19", treatment="G_CONTROL", model="DeepSeek-V4-Pro-0813",
            request_body={"model":"DeepSeek-V4-Pro-0813","messages":[{"role":"user","content":content}],
                          "reasoning_effort":"high"},attempt_lineage_id=lineage,request_id=request_id)

    def test_large_payload_is_serialized_before_admission_lock(self):
        q = SharedProviderQueue(self.root)
        original = queue_module._exclusive_file_lock
        waits = []
        @contextmanager
        def observed(path, **kwargs):
            started = time.monotonic()
            if kwargs.get("operation") == "submit":
                temps = list(q.queue.glob(".*.tmp"))
                self.assertTrue(temps)
                self.assertGreater(temps[0].stat().st_size, 1_000_000)
            with original(path, **kwargs):
                yield
            if kwargs.get("operation") == "submit": waits.append(time.monotonic() - started)
        with patch.object(queue_module, "_exclusive_file_lock", observed):
            record = self.submit(q, content="x" * 2_000_000)
        self.assertEqual(record["status"], "QUEUED")
        self.assertEqual(len(waits), 1)
        self.assertLess(waits[0], 1.0)

    def test_worker_survives_transient_claim_lock_timeout_without_duplicate_provider_call(self):
        q = SharedProviderQueue(self.root)
        record = self.submit(q, content="one-call", lineage="one-call")
        service = ProviderQueueService(self.root, client_factory=lambda alias: FakeClient(), poll_seconds=0.01)
        original = queue_module._exclusive_file_lock
        state = {"raised":False}
        @contextmanager
        def one_timeout(path, **kwargs):
            if kwargs.get("operation") == "claim" and not state["raised"]:
                state["raised"] = True
                raise LockAcquisitionTimeout(path, "claim", 30.0)
            with original(path, **kwargs): yield
        thread = threading.Thread(target=service.run_window, args=("A",), daemon=True)
        with patch.object(queue_module, "_exclusive_file_lock", one_timeout):
            thread.start()
            result = q.wait(record["request_id"], poll_seconds=0.02)
            service.stop_event.set()
            thread.join(timeout=3)
        self.assertFalse(thread.is_alive())
        self.assertTrue(state["raised"])
        self.assertEqual(result["status"], "COMPLETED")
        self.assertEqual(result["attempts_used"], 1)
        self.assertEqual(len(list(q.attempts.glob("*.json"))), 1)

    def test_real_lock_operation_error_fails_closed_without_retry(self):
        lock_path = self.root / "runtime" / "permission-test.lock"
        calls = []
        def denied(*args):
            calls.append(1)
            raise OSError(errno.EIO, "invalid lock handle")
        with patch.object(queue_module.msvcrt, "locking", denied):
            with self.assertRaises(queue_module.QueueLockFatal):
                with queue_module._exclusive_file_lock(lock_path, operation="claim", wait_seconds=0.1):
                    pass
        self.assertEqual(len(calls), 1)

    def test_consumed_saved_response_is_reconciled_without_rewriting_receipts(self):
        q = SharedProviderQueue(self.root)
        episode_id = "EP-CONSUMED"
        attempt_id = "attempt-consumed"
        request_id = "request-consumed"
        rec = {"request_id":request_id,"episode_id":episode_id,"treatment":"H_RETRIEVAL_V7",
               "model":"DeepSeek-V4-Pro-0813","request_body_sha256":"payload-sha",
               "attempt_lineage_id":"lineage-consumed","attempt_ids":[attempt_id],
               "current_attempt_id":attempt_id,"assigned_window":"A","status":"RUNNING"}
        processing = q.processing / (request_id + ".json")
        queue_module._atomic_json(processing, rec)
        result_path = q.results / (request_id + ".json")
        queue_module._atomic_json(result_path, {**rec,"status":"REQUEST_PENDING_REPLAY",
            "terminal_status":"NON_RETRYABLE_PROVIDER_FAILURE"})
        artifact_path = q.attempts / (attempt_id + ".response.json")
        artifact_path.write_text("{}", encoding="utf-8")
        artifact = {"body_complete":True,"payload":{"id":"response-consumed"},
                    "artifact_sha256":"artifact-sha","payload_sha256":"payload-sha"}
        recovery_events = self.root.parent.parent / "hr-run" / "episodes" / episode_id / "recovery_events.jsonl"
        recovery_events.parent.mkdir(parents=True, exist_ok=True)
        recovery_events.write_text("\n".join([
            json.dumps({"event":"SAVED_PROVIDER_RESPONSE_CONTINUATION_RECOVERY_STARTED",
                "request_id":request_id,"queue_attempt_id":attempt_id,"response_id":"response-consumed",
                "artifact_sha256":"artifact-sha","recovery_event_id":"recovery-link","recovery_id":"recovery-1"}),
            json.dumps({"event":"SAVED_RESPONSE_RESUMED","attempt_id":attempt_id,"recovery_event_id":"recovery-link"})
        ]) + "\n", encoding="utf-8")
        before_processing = queue_module._sha(processing.read_bytes())
        before_result = queue_module._sha(result_path.read_bytes())
        service = ProviderQueueService(self.root)
        with patch("provider_staging.paratera_chat.ParateraChatCompletions._load_response_artifact", return_value=artifact):
            service._recover_processing_for_window("A")
        self.assertEqual(queue_module._sha(processing.read_bytes()), before_processing)
        self.assertEqual(queue_module._sha(result_path.read_bytes()), before_result)
        events = [json.loads(line) for line in (q.runtime / "provider_reconciliations.jsonl").read_text().splitlines()]
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["event"], "SAVED_RESPONSE_CONSUMED_BY_RECOVERY_BRANCH")
        self.assertTrue(events[0]["queue_result_unchanged"])

    def test_dispatcher_records_worker_exit_and_restarts_within_bound(self):
        service = ProviderQueueService(self.root)
        calls = {"A":0,"B":0}
        class FakeProcess:
            def __init__(self, window): self.window=window; self.pid=10000+len(calls)+calls[window]
            def poll(self):
                if self.window == "A" and calls["A"] == 1:
                    queue_module._atomic_json(service.q.runtime / "window_A_status.json", {
                        "state":"FAILED","error_class":"RuntimeError","last_request_id":None})
                    return 1
                return None
            def terminate(self): return None
        def fake_popen(args, **kwargs):
            window=args[-1]; calls[window]+=1
            return FakeProcess(window)
        def stop_after_restart(seconds): service.stop_event.set()
        with patch.object(queue_module.subprocess, "Popen", fake_popen), patch.object(queue_module.time, "sleep", stop_after_restart):
            service.run()
        self.assertEqual(calls["A"], 2)
        self.assertEqual(calls["B"], 1)
        events = [json.loads(line) for line in (self.root / "runtime" / "provider_queue_diagnostics.jsonl").read_text().splitlines()]
        exits = [event for event in events if event.get("event") == "WORKER_EXITED"]
        self.assertEqual(len(exits), 1)
        self.assertEqual(exits[0]["restart_decision"], "RESTART_AFTER_BOUNDED_BACKOFF")


if __name__ == "__main__":
    unittest.main()
