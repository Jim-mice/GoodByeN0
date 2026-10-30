from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path

import msvcrt

from n0_integration.supervised_evaluator import _execution_provenance


class LockedLeaseOwnerReadTests(unittest.TestCase):
    def test_owner_record_is_read_without_touching_locked_byte_zero(self) -> None:
        request = {
            "request_id": "N0-OJ-SYNTHETIC-LOCKED-READ",
            "episode_id": "N0_MANUAL_SYNTHETIC",
            "candidate_sha256": "a" * 64,
            "lease_id": "logical-lease-synthetic",
        }
        owner_record = {
            "lease": {
                "pid": os.getpid(),
                "request_id": request["request_id"],
                "episode_id": request["episode_id"],
                "candidate_sha256": request["candidate_sha256"],
                "lease_id": "physical-lease-synthetic",
            },
            "acquired_at_unix": 1.0,
            "health_evidence": {"status": "PASS"},
        }
        with tempfile.TemporaryDirectory(prefix="n0-locked-lease-read-") as temporary:
            path = Path(temporary) / "gpu.lock"
            path.write_bytes(bytes([0]) + json.dumps(owner_record).encode("utf-8"))
            with path.open("r+b", buffering=0) as owner_file:
                owner_file.seek(0)
                msvcrt.locking(owner_file.fileno(), msvcrt.LK_NBLCK, 1)
                try:
                    evidence = _execution_provenance(request, path)
                finally:
                    owner_file.seek(0)
                    msvcrt.locking(owner_file.fileno(), msvcrt.LK_UNLCK, 1)
        self.assertEqual(evidence["gpu_lease_evidence"]["owner"]["lease_id"],
                         "physical-lease-synthetic")
        self.assertTrue(evidence["gpu_lease_evidence"]["captured_while_held"])
        self.assertEqual(evidence["worker_identity"]["request_id"], request["request_id"])


if __name__ == "__main__":
    unittest.main()
