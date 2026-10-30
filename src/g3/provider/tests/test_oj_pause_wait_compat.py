from __future__ import annotations
import json, tempfile, threading, time, unittest
from pathlib import Path
from canonical_harness.v7_evaluator_adapter import V7EvaluatorAdapter, wait_for_external_result, _sha

class RunnerPauseWaitTests(unittest.TestCase):
    def test_pause_time_does_not_consume_queue_wait_budget(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp); runtime = root / "runtime"; runtime.mkdir()
            result = root / "results" / "REQ-1" / "result.json"
            result.parent.mkdir(parents=True)
            (runtime / "oj_control_state.json").write_text(json.dumps({"state":"PAUSED"}), encoding="utf-8")
            received = {}
            def publish():
                time.sleep(0.38)
                result.write_text('{"state":"COMPLETED"}', encoding="utf-8")
            thread = threading.Thread(target=publish); thread.start()
            value = wait_for_external_result(result, root, 0.12, "REQ-1", lambda rid: json.loads(result.read_text(encoding="utf-8")))
            thread.join(2)
            self.assertEqual(value["state"], "COMPLETED")

    def test_unreadable_gate_fails_closed_as_controlled_wait(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp); runtime = root / "runtime"; runtime.mkdir()
            (runtime / "oj_control_state.json").write_text('{broken', encoding="utf-8")
            result = root / "results" / "REQ-2" / "result.json"
            result.parent.mkdir(parents=True)
            def publish():
                time.sleep(0.28)
                result.write_text('{"state":"COMPLETED"}', encoding="utf-8")
            thread = threading.Thread(target=publish); thread.start()
            value = wait_for_external_result(result, root, 0.05, "REQ-2", lambda rid: json.loads(result.read_text(encoding="utf-8")))
            thread.join(2)
            self.assertEqual(value["state"], "COMPLETED")

    def test_restart_reuses_existing_request_id_and_does_not_enqueue(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp); candidate = root / "candidate.cu"; candidate.write_text("kernel", encoding="utf-8")
            digest = _sha(candidate); request_id = "V7-EXISTING-001"
            req_dir = root / "requests" / request_id; req_dir.mkdir(parents=True)
            (req_dir / "candidate.cu").write_bytes(candidate.read_bytes())
            record = {"request_id":request_id,"run_id":"RUN-1","candidate_id":"C0001",
                "candidate_sha256":digest,"treatment_id":"G_CONTROL","state":"QUEUED"}
            (req_dir / "request.json").write_text(json.dumps(record), encoding="utf-8")
            queue = root / "queue"; queue.mkdir()
            (queue / (request_id + ".json")).write_text(json.dumps({"request_id":request_id}), encoding="utf-8")
            (root / "runtime").mkdir()
            (root / "runtime" / "oj_control_state.json").write_text(json.dumps({"state":"PAUSED"}), encoding="utf-8")
            result = root / "results" / request_id / "result.json"; result.parent.mkdir(parents=True)
            def publish():
                time.sleep(0.28)
                result.write_text('{"state":"COMPLETED"}', encoding="utf-8")
            thread = threading.Thread(target=publish); thread.start()
            adapter = V7EvaluatorAdapter(root, dispatcher_timeout=0.08, external_worker=True)
            adapter.read_existing_result = lambda rid, sha, wait_seconds=3.0: json.loads(result.read_text(encoding="utf-8"))
            admitted = []
            output = adapter.evaluate_with_retry(candidate, "RUN-1", "C0001", "G_CONTROL", on_admitted=admitted.append)
            thread.join(2)
            self.assertEqual(output["state"], "COMPLETED")
            self.assertEqual(admitted[0]["request_id"], request_id)
            self.assertTrue(admitted[0]["resumed_existing"])
            self.assertEqual(len(list(queue.glob("*.json"))), 1)

if __name__ == "__main__": unittest.main(verbosity=2)
