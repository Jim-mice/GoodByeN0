import json
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from n0_autonomous.g3_start_gate import validate_g3_start


def isolated_release(tmp_path):
    source = json.loads((ROOT / "RELEASE_MANIFEST.json").read_text(encoding="utf-8"))
    target = tmp_path / "release"
    target.mkdir()
    for entry in source["source_files"]:
        src = ROOT / entry["path"]
        dst = target / entry["path"]
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
    shutil.copy2(ROOT / "RELEASE_MANIFEST.json", target / "RELEASE_MANIFEST.json")
    shutil.copy2(ROOT / "RELEASE_MANIFEST.json.sha256", target / "RELEASE_MANIFEST.json.sha256")
    return target


def qualification(root):
    manifest = json.loads((root / "RELEASE_MANIFEST.json").read_text(encoding="utf-8"))
    integration = manifest["runtime_bindings"]["integration"]
    return {"status": "PASS", "max_deviation_percent": 2.0, "terminated_tree": True,
            "integration_release_id": integration["release_id"],
            "integration_manifest_sha256": integration["manifest_sha256"],
            "controls": [{"status": "PASS"}, {"status": "PASS"}, {"status": "PASS"}]}


def inputs(root, **overrides):
    values = {"release_root": root, "qualification": qualification(root),
        "worker_ids": [f"G3-W{i:02d}" for i in range(1, 11)], "gpu_lease_available": True,
        "provider_configured": True, "unresolved_requests": 0, "stop_requested": False}
    values.update(overrides)
    return validate_g3_start(**values)


def test_preflight_recognizes_manifest_bound_g3_worker_runtime(tmp_path):
    root = isolated_release(tmp_path)
    result = inputs(root)
    assert "G3_PRODUCTION_WORKER_RUNTIME_NOT_BOUND" not in result["blockers"]
    assert "G3_PRODUCTION_WORKER_RUNTIME_NOT_IN_RELEASE_MANIFEST" not in result["blockers"]
    assert result["formal_start"] is False


def test_reference_hold_and_unterminated_tree_fail_closed(tmp_path):
    root = isolated_release(tmp_path)
    hold = inputs(root, qualification={"status": "HOLD"})
    assert "REFERENCE_QUALIFICATION_NOT_PASS" in hold["blockers"]
    bad_tree = qualification(root)
    bad_tree["terminated_tree"] = False
    result = inputs(root, qualification=bad_tree)
    assert "QUALIFICATION_PROCESS_TREE_NOT_TERMINATED" in result["blockers"]
    bad_identity = qualification(root)
    bad_identity["integration_manifest_sha256"] = "0" * 64
    result = inputs(root, qualification=bad_identity)
    assert "QUALIFICATION_INTEGRATION_IDENTITY_MISMATCH" in result["blockers"]


def test_worker_lease_provider_queue_and_stop_checks_fail_closed(tmp_path):
    root = isolated_release(tmp_path)
    result = inputs(root, worker_ids=["DUP"] * 10, gpu_lease_available=False,
                    provider_configured=False, unresolved_requests=1, stop_requested=True)
    for blocker in ("WORKER_REGISTRY_NOT_TEN_UNIQUE_IDENTITIES", "GPU_LEASE_UNAVAILABLE",
                    "PROVIDER_CONFIGURATION_UNAVAILABLE", "UNRESOLVED_OJ_REQUESTS_PRESENT", "STOP_REQUESTED"):
        assert blocker in result["blockers"]
