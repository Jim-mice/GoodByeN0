"""One-request CUDA evaluator child. The supervising Worker owns the GPU Lease."""
from __future__ import annotations
import json, os, sys
from pathlib import Path

class _NoopMonitor:
    """GPU telemetry is collected by the Lease-owning Supervisor, outside the Job Object."""
    def start(self): return None
    def stop(self): return []
    def evidence(self):
        return {"schema":"N0_CHILD_GPU_MONITOR_DELEGATED_V1","observations":[],"events":[],
                "delegated_to_lease_owner_supervisor":True}


def main(config_path: str, result_path: str, lease_arg: str | None = None) -> int:
    config = json.loads(Path(config_path).read_text(encoding="utf-8"))
    integration = Path(config["integration_root"]).resolve()
    oj = Path(config["oj_root"]).resolve()
    harness = Path(config["harness_root"]).resolve()
    kernelbench = Path(config["repository_root"]).resolve()
    sys.path[:0] = [str(integration), str(oj), str(harness), str(kernelbench / "src")]
    os.environ["N0_GPU_LEASE_PATH"] = str(Path(config["lease_path"]).resolve())
    if lease_arg is not None and Path(lease_arg).resolve() != Path(config["lease_path"]).resolve():
        raise RuntimeError("CANDIDATE_CHILD_LEASE_IDENTITY_MISMATCH")
    os.environ["N0_CANDIDATE_EVAL_CHILD"] = "1"
    os.environ["N0_SUPERVISOR_GPU_SNAPSHOT_JSON"] = json.dumps(config["gpu_snapshot"], separators=(",", ":"))
    from n0_integration.oj_r5_worker import make_r5_queue_evaluator
    evaluator = make_r5_queue_evaluator(queue_root=Path(config["queue_root"]),
        repository_root=kernelbench, release_root=oj,
        qualification=config["qualification"], hardware_identity=config["hardware_identity"],
        profile_runner=object(), monitor_factory=_NoopMonitor)
    try:
        result = evaluator(config["request"])
        body = {"status": "CHILD_COMPLETED", "result": result}
    except BaseException as exc:
        body = {"status": "CHILD_EXCEPTION", "exception_type": type(exc).__name__,
                "exception_message": str(exc)[:65536],
                "exception_stdout": str(getattr(exc, "stdout", "") or "")[:32768],
                "exception_stderr": str(getattr(exc, "stderr", "") or "")[:32768]}
    temporary = Path(result_path).with_suffix(".tmp")
    temporary.write_text(json.dumps(body, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    os.replace(temporary, result_path)
    return 0


if __name__ == "__main__":
    lease_path = sys.argv[4] if len(sys.argv) > 4 and sys.argv[3] == "--gpu-lease-path" else None
    raise SystemExit(main(sys.argv[1], sys.argv[2], lease_path))
