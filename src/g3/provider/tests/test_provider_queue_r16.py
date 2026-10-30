from __future__ import annotations
import json, os, tempfile, threading, time, unittest
from pathlib import Path
from types import SimpleNamespace

from canonical_harness import provider_queue as provider_queue_module
from canonical_harness.provider_queue import ProviderQueueService, SharedProviderQueue
from canonical_harness.provider_retry import complete_with_retries
from provider_staging.paratera_chat import ParateraChatCompletions


class FakeClient:
    def __init__(self, alias, counts, active, peak, lock):
        self.alias, self.counts, self.active, self.peak, self.lock = alias, counts, active, peak, lock
    def complete(self, model, effort, messages, tools, tool_choice, max_tokens, attempt_id=None, response_artifact_path=None):
        with self.lock:
            self.active[self.alias] = self.active.get(self.alias, 0) + 1
            self.peak[self.alias] = max(self.peak.get(self.alias, 0), self.active[self.alias])
            n = self.counts.get(messages[0]["content"], 0) + 1; self.counts[messages[0]["content"]] = n
        time.sleep(.04)
        with self.lock: self.active[self.alias] -= 1
        if messages[0]["content"].startswith("unknown"):
            e = RuntimeError("uncertain timeout")
            e.telemetry = {"terminal_status":"TIMEOUT_OUTCOME_UNKNOWN"}
            raise e
        if messages[0]["content"].startswith("fail2") and n < 3:
            e = RuntimeError("server error")
            e.telemetry = {"terminal_status":"HTTP_ERROR", "http_status":503}
            raise e
        if messages[0]["content"].startswith("retry") and n < 2:
            e = RuntimeError("server error")
            e.telemetry = {"terminal_status":"HTTP_ERROR", "http_status":503}
            raise e
        if messages[0]["content"].startswith("alwaysfail"):
            e = RuntimeError("server error")
            e.telemetry = {"terminal_status":"HTTP_ERROR", "http_status":503}
            raise e
        payload = {"id":"resp-" + attempt_id, "model":model, "choices":[{"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}], "usage":{"completion_tokens":1}}
        return payload, SimpleNamespace(request_id="req", response_id=payload["id"], returned_model=model,
                                        usage=payload["usage"], telemetry={"body_complete":True})


class ProviderQueueR16Tests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.root = Path(self.tmp.name)
        self.old_backoff = (provider_queue_module.RETRY_BACKOFF_BASE_SECONDS,
                            provider_queue_module.RETRY_BACKOFF_MAX_SECONDS)
        provider_queue_module.RETRY_BACKOFF_BASE_SECONDS = .01
        provider_queue_module.RETRY_BACKOFF_MAX_SECONDS = .02
        os.environ.pop("NOVELTY_V7_PROVIDER_QUEUE_ROOT",None)
        os.environ["jlapi"] = "test-secret-a"; os.environ["jlapi2"] = "test-secret-b"
        self.counts = {}; self.active = {}; self.peak = {}; self.lock = threading.Lock()
        self.service = ProviderQueueService(self.root, client_factory=lambda alias: FakeClient(alias,self.counts,self.active,self.peak,self.lock), poll_seconds=.01)
        self.threads = [threading.Thread(target=self.service.run_window,args=(w,),daemon=True) for w in ("A","B")]
        for t in self.threads: t.start()
        self.q = SharedProviderQueue(self.root)

    def tearDown(self):
        self.service.stop_event.set()
        for t in self.threads: t.join(timeout=2)
        (provider_queue_module.RETRY_BACKOFF_BASE_SECONDS,
         provider_queue_module.RETRY_BACKOFF_MAX_SECONDS) = self.old_backoff
        os.environ.pop("NOVELTY_V7_PROVIDER_QUEUE_ROOT",None)
        self.tmp.cleanup()

    def submit(self, text, treatment="G_CONTROL", used=0, max_attempts=3, lineage=None):
        return self.q.submit(episode_id="EP-1", treatment=treatment, model="DeepSeek-V4-Pro-0813",
            request_body={"model":"DeepSeek-V4-Pro-0813","messages":[{"role":"user","content":text}],"reasoning_effort":"high","max_tokens":16},
            attempt_lineage_id=lineage or text, attempts_already_used=used, max_attempts=max_attempts)

    def test_two_windows_run_concurrently_and_queue_is_shared(self):
        orders = [self.submit("normal-"+str(i), t) for i,t in enumerate(("G_CONTROL","H_STATIC","H_RETRIEVAL_V7","E_EXPERT"))]
        results = [self.q.wait(x["request_id"], poll_seconds=.01) for x in orders]
        self.assertTrue(all(x["status"] == "COMPLETED" for x in results), json.dumps(results,default=str))
        self.assertEqual(max(self.peak.values()), 1)
        self.assertLessEqual(sum(self.peak.values()), 2)
        self.assertEqual({x["treatment"] for x in results},{"G_CONTROL","H_STATIC","H_RETRIEVAL_V7","E_EXPERT"})

    def test_replay_success_with_attempt_lineage_and_same_payload_sha(self):
        order = self.submit("retry-success")
        result = self.q.wait(order["request_id"], poll_seconds=.01)
        self.assertEqual(result["status"],"COMPLETED",json.dumps(result,default=str)); self.assertEqual(result["attempts_used"],2)
        events = [json.loads(p.read_text()) for p in (self.root/"attempts").glob("*.json")]
        self.assertEqual(len(events),2)
        self.assertEqual(len({x["request_body_sha256"] for x in events}),1)
        failed_attempt = next(x for x in events if x["attempt_number"] == 1)
        self.assertEqual(failed_attempt["status"], "HTTP_ERROR_RECEIVED")
        self.assertGreater(failed_attempt["retry_delay_seconds"], 0)

    def test_third_attempt_success_and_exhaustion_cap(self):
        ok = self.submit("fail2-success"); result = self.q.wait(ok["request_id"],poll_seconds=.01)
        self.assertEqual(result["attempts_used"],3,json.dumps({"result":result,"attempts":[json.loads(p.read_text()) for p in (self.root/"attempts").glob("*.json")]},default=str))
        fail = self.submit("alwaysfail-exhaust",lineage="lineage-exhaust")
        result2 = self.q.wait(fail["request_id"],poll_seconds=.01)
        self.assertEqual(result2["status"],"RETRY_EXHAUSTED"); self.assertEqual(result2["attempts_used"],3)

    def test_ambiguous_timeout_is_not_replayed(self):
        order = self.submit("unknown-timeout", lineage="unknown-timeout-lineage")
        result = self.q.wait(order["request_id"], poll_seconds=.01)
        self.assertEqual(result["status"], "RETRY_EXHAUSTED")
        self.assertEqual(result["terminal_status"], "OUTCOME_UNKNOWN_REQUIRES_RECONCILIATION")
        self.assertTrue(result["automatic_retries_suppressed"])
        self.assertEqual(result["attempts_used"], 1)
        attempts = [json.loads(p.read_text()) for p in (self.root/"attempts").glob("*.json")]
        self.assertEqual(len(attempts), 1)
        self.assertEqual(attempts[0]["status"], "OUTCOME_UNKNOWN")

    def test_retry_policy_uses_backoff_only_for_known_retryable_http_status(self):
        delay = provider_queue_module._retry_delay_seconds
        self.assertAlmostEqual(delay({"http_status":503}, 1), .01)
        self.assertAlmostEqual(delay({"http_status":503}, 2), .02)
        self.assertAlmostEqual(delay({"http_status":503}, 8), .02)
        self.assertAlmostEqual(delay({"http_status":429, "retry_after_seconds":.015}, 1), .015)
        self.assertAlmostEqual(delay({"http_status":429}, 1), .01)
        self.assertIsNone(delay({"terminal_status":"TIMEOUT_OUTCOME_UNKNOWN"}, 1))
        self.assertIsNone(delay({"http_status":401}, 1))

    def test_saved_result_survives_service_restart_and_same_lineage_attach(self):
        first = self.submit("durable",lineage="durable-lineage")
        result = self.q.wait(first["request_id"],poll_seconds=.01)
        second = self.submit("durable",lineage="durable-lineage")
        self.assertEqual(second["request_id"],first["request_id"])
        self.assertEqual(self.q.wait(second["request_id"])["payload"],result["payload"])

    def test_duplicate_lineage_payload_conflict_rejected(self):
        self.submit("one",lineage="same-lineage")
        with self.assertRaisesRegex(ValueError,"PAYLOAD_CONFLICT"):
            self.submit("two",lineage="same-lineage")

    def test_concurrent_submitters_attach_to_one_cross_window_lineage(self):
        barrier = threading.Barrier(3)
        submitted = []
        errors = []
        def submit_same():
            try:
                barrier.wait()
                submitted.append(self.q.submit(episode_id="EP-1",treatment="G_CONTROL",model="DeepSeek-V4-Pro-0813",
                    request_body={"model":"DeepSeek-V4-Pro-0813","messages":[{"role":"user","content":"one-lineage"}],
                                  "reasoning_effort":"high","max_tokens":16},attempt_lineage_id="cross-window-single-flight"))
            except Exception as exc:
                errors.append(type(exc).__name__ + ":" + str(exc))
        threads=[threading.Thread(target=submit_same) for _ in range(2)]
        for thread in threads: thread.start()
        barrier.wait()
        for thread in threads: thread.join(timeout=2)
        self.assertEqual(errors,[])
        self.assertEqual(len(submitted),2)
        self.assertEqual(len({record["request_id"] for record in submitted}),1)
        result=self.q.wait(submitted[0]["request_id"],poll_seconds=.01)
        self.assertEqual(result["status"],"COMPLETED")
        self.assertEqual(len(result["attempt_ids"]),1)

    def test_queue_records_do_not_include_credentials(self):
        self.submit("secret-check",lineage="secret-check")
        text = "".join(p.read_text(encoding="utf-8") for folder in ("queue","processing","results","attempts") for p in (self.root/folder).glob("*.json"))
        self.assertNotIn("test-secret-a",text); self.assertNotIn("test-secret-b",text)
        self.assertNotIn("Authorization",text)

    def test_queued_wait_has_no_http_attempt_or_timeout_clock(self):
        q = SharedProviderQueue(self.root / "no-worker")
        body = {"model":"DeepSeek-V4-Pro-0813","messages":[{"role":"user","content":"waiting"}],"reasoning_effort":"high","max_tokens":16}
        order = q.submit(episode_id="EP",treatment="G_CONTROL",model="DeepSeek-V4-Pro-0813",request_body=body,attempt_lineage_id="wait-lineage")
        self.assertEqual(order["status"],"QUEUED")
        self.assertFalse((q.processing / (order["request_id"]+".json")).exists())
        self.assertEqual(list(q.attempts.glob("*.json")),[])

    def test_old_model_route_is_rejected_before_queue_admission(self):
        with self.assertRaisesRegex(ValueError,"MODEL_NOT_ALLOWLISTED"):
            self.q.submit(episode_id="EP-OLD",treatment="G_CONTROL",model="DeepSeek-V4-Pro",
                request_body={"model":"DeepSeek-V4-Pro","messages":[{"role":"user","content":"x"}],
                              "reasoning_effort":"high","max_tokens":8},attempt_lineage_id="old-route")
        self.assertEqual(list(self.q.queue.glob("*.json")),[])

    def test_r15_adapter_entry_routes_through_queue_and_returns_first_complete_result(self):
        os.environ["NOVELTY_V7_PROVIDER_QUEUE_ROOT"] = str(self.root)
        body = ParateraChatCompletions.build_body("DeepSeek-V4-Pro-0813","high",[{"role":"user","content":"retry-route"}],[],"auto",16)
        sha = ParateraChatCompletions.request_sha256("DeepSeek-V4-Pro-0813","high",[{"role":"user","content":"retry-route"}],[],"auto",16)
        client = object.__new__(ParateraChatCompletions)
        payload,evidence,retry = complete_with_retries(client,model="DeepSeek-V4-Pro-0813",effort="high",messages=body["messages"],
            tools=body["tools"],tool_choice=body["tool_choice"],max_tokens=16,initial_attempt_id="route-lineage",
            initial_artifact_path=self.root/"unused.json",episode_id="EP-ROUTE",request_sha256=sha,treatment="H_STATIC")
        self.assertEqual(payload["model"],"DeepSeek-V4-Pro-0813")
        self.assertTrue(retry["queue_owned_retries"])
        self.assertEqual(self.counts["retry-route"],2)

    def test_attempt_budget_includes_prior_r15_attempts(self):
        item = self.submit("retry-success-prior",used=1,max_attempts=2,lineage="prior-attempt-lineage")
        result = self.q.wait(item["request_id"],poll_seconds=.01)
        self.assertEqual(result["attempts_used"],3)


if __name__ == "__main__": unittest.main()
