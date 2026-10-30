"""Cross-process Windows GPU exclusive lease with external-workload fail-closed gate."""
from __future__ import annotations

import json
import os
import time
import uuid
from pathlib import Path
from typing import Any, Callable

from .engine import N0Error


class GpuExclusiveLease:
    def __init__(self, lock_path: Path, *, health_check: Callable[[], dict[str, Any]],
                 wait_seconds: float = 60.0, owner: dict[str, Any] | None = None):
        self.path=Path(lock_path).resolve()
        self.health_check=health_check
        self.wait_seconds=wait_seconds
        self.owner={**(owner or {}),"pid":os.getpid(),"lease_id":uuid.uuid4().hex}
        self._file=None
        self._acquired=False

    def acquire(self) -> dict[str, Any]:
        if os.name != "nt": raise N0Error("GPU_LEASE_REQUIRES_WINDOWS_BYTE_RANGE_LOCK")
        if not callable(self.health_check) or self.wait_seconds < 0:
            raise N0Error("GPU_HEALTH_CHECK_OR_WAIT_INVALID")
        import msvcrt
        self.path.parent.mkdir(parents=True,exist_ok=True)
        try: self._file=self.path.open("r+b",buffering=0)
        except FileNotFoundError:
            try: self._file=self.path.open("x+b",buffering=0)
            except FileExistsError: self._file=self.path.open("r+b",buffering=0)
        self._file.seek(0,os.SEEK_END)
        if self._file.tell()==0:
            self._file.write(b"\0")
            self._file.flush(); os.fsync(self._file.fileno())
        deadline=time.monotonic()+self.wait_seconds
        delay=0.025
        while True:
            try:
                self._file.seek(0)
                msvcrt.locking(self._file.fileno(),msvcrt.LK_NBLCK,1)
                break
            except OSError:
                if time.monotonic()>=deadline:
                    self._file.close(); self._file=None
                    raise N0Error("GPU_EXCLUSIVE_LEASE_TIMEOUT")
                time.sleep(delay)
                delay=min(delay*1.5,0.5)
        self._acquired=True
        try:
            owner_record={"lease":self.owner,"acquired_at_unix":time.time(),"health_evidence":"PENDING"}
            self._write_owner(owner_record)
            health=self.health_check()
            if (not isinstance(health,dict) or health.get("status")!="PASS"
                    or health.get("uncoordinated_gpu_workloads") != []
                    or not isinstance(health.get("gpu_identity"),str) or not health["gpu_identity"]):
                raise N0Error("GPU_EXTERNAL_WORKLOAD_OR_HEALTH_NOT_PROVEN")
            payload={"lease":self.owner,"acquired_at_unix":owner_record["acquired_at_unix"],"health_evidence":health}
            self._write_owner(payload)
            return payload
        except BaseException:
            self.release()
            raise

    def _write_owner(self, payload: dict[str, Any]) -> None:
        encoded=json.dumps(payload,sort_keys=True,separators=(",",":"),ensure_ascii=False).encode("utf-8")
        self._file.seek(1); self._file.truncate(1); self._file.write(encoded); self._file.flush(); os.fsync(self._file.fileno())

    def release(self) -> None:
        if not self._acquired or self._file is None: return
        import msvcrt
        try:
            self._file.seek(1)
            existing=json.loads(self._file.read().decode("utf-8"))
            if existing.get("lease",{}).get("lease_id")!=self.owner["lease_id"] or existing.get("lease",{}).get("pid")!=os.getpid():
                raise N0Error("GPU_LEASE_OWNER_MISMATCH")
            self._file.seek(1); self._file.truncate(1); self._file.flush(); os.fsync(self._file.fileno())
            self._file.seek(0); msvcrt.locking(self._file.fileno(),msvcrt.LK_UNLCK,1)
        finally:
            self._acquired=False
            self._file.close(); self._file=None

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.release()


def run_one_oj_request(store, *, worker_id: str, lease_path: Path,
                       worker_pid: int, worker_start_utc: str,
                       health_check: Callable[[], dict[str, Any]],
                       evaluator: Callable[[dict[str, Any]], dict[str, Any]],
                       receipt_validator: Callable[[dict[str, Any], dict[str, Any]], bool]) -> dict[str, Any] | None:
    """Claim one persisted request, execute under one device lease, or hold ambiguity."""
    request=store.claim_next_oj(worker_id,worker_pid=worker_pid,worker_start_utc=worker_start_utc)
    if request is None: return None
    try:
        lease=GpuExclusiveLease(lease_path,health_check=health_check,owner={"worker_id":worker_id,
            "episode_id":request["episode_id"],"request_id":request["request_id"],"candidate_sha256":request["candidate_sha256"]})
        with lease:
            receipt=evaluator(request)
            if not isinstance(receipt,dict) or not receipt_validator(request,receipt):
                state=store.finish_oj(request_id=request["request_id"],worker_id=worker_id,lease_id=request["lease_id"],
                                      receipt=None,infrastructure_error="OJ_RECEIPT_IDENTITY_OR_COMPLETENESS_FAILED")
            else:
                state=store.finish_oj(request_id=request["request_id"],worker_id=worker_id,lease_id=request["lease_id"],receipt=receipt)
        return {"request_id":request["request_id"],"state":state}
    except Exception as exc:
        state=store.finish_oj(request_id=request["request_id"],worker_id=worker_id,lease_id=request["lease_id"],
                              receipt=None,infrastructure_error=type(exc).__name__+":"+str(exc))
        return {"request_id":request["request_id"],"state":state,"error":type(exc).__name__+":"+str(exc)}
