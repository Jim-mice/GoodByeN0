from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(r"D:\projects\kernelagent")
RID = "N0_KERNEL_AGENT_POOL20_R8_18_9_R8_03_WORKER_RELEASE_BINDING_FIX_20261027"
RELEASE = ROOT / "runtime/deployment/releases" / RID
PARENT_ID = "N0_KERNEL_AGENT_POOL20_R8_18_8_R8_03_VERIFIED_SUPERVISOR_CHAIN_RESUME_20261027"
PARENT_SHA = "86277CA282A4A0E67FEA3BA27C9A7C0BF4B9274F9C3AF0076A72CDDF8AB202AE"

def digest(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest().upper()

def main() -> None:
    parent = ROOT / "runtime/deployment/releases" / PARENT_ID / "RELEASE_MANIFEST.json"
    if digest(parent.read_bytes()) != PARENT_SHA:
        raise RuntimeError("R817_PARENT_MANIFEST_MISMATCH")
    binding = json.loads((RELEASE / "PRODUCTION_BINDING.json").read_text(encoding="utf-8"))
    components = []
    for key, path in (("oj", "oj/releases"), ("integration", "integrations"), ("harness", "harness/releases"), ("knowledge", "harness/releases")):
        item = binding[key]
        mid = item["release_id"]
        mpath = ROOT / "runtime" / path / mid / "RELEASE_MANIFEST.json"
        if digest(mpath.read_bytes()) != item["manifest_sha256"]:
            raise RuntimeError("COMPONENT_MANIFEST_MISMATCH:" + mid)
        components.append({"release_id": mid, "path": mpath.relative_to(ROOT / "runtime").as_posix(), "sha256": item["manifest_sha256"]})
    files = []
    for path in sorted(RELEASE.rglob("*")):
        rel = path.relative_to(RELEASE)
        if (not path.is_file() or rel.as_posix() in {"RELEASE_MANIFEST.json", "RELEASE_MANIFEST.json.sha256"}
                or any(part in {"__pycache__", ".pytest_cache"} for part in rel.parts)
                or path.suffix.lower() in {".pyc", ".pyo"}):
            continue
        raw = path.read_bytes()
        files.append({"path": rel.as_posix(), "bytes": len(raw), "sha256": digest(raw)})
    binding["direct_parent_deployment_release"] = PARENT_ID
    if binding.get("deployment_release_id") != RID:
        raise RuntimeError("R818_BINDING_IDENTITY_MISMATCH")
    config = (RELEASE / "POOL20_RUN_CONFIG.json").read_bytes()
    if binding.get("pool20_run_config_sha256") != digest(config):
        raise RuntimeError("POOL20_RUN_CONFIG_BINDING_MISMATCH")
    manifest = {
        "schema": "N0_POOL20_DEPLOYMENT_RELEASE_V2",
        "release_id": RID,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "parent_release": {"release_id": PARENT_ID, "manifest_sha256": PARENT_SHA},
        "components": components,
        "changes": [
            "Bind Candidate Worker startup to this immutable Pool release and retain R8.18.5 as parent.",
            "Validate live queue snapshots instead of historical request-count and request-ID constants.",
            "Accept only matching active Run/Pool/Integration manifests and archive the operator STOP markers in a snapshot-bound recovery transaction.",
            "Verify and orderly hand off the existing R8.18.5 monitor using its real PID, command line, launch nonce and identity receipt.",
            "Idempotently route eligible completed receipts; keep exhausted infrastructure retry requests isolated without score or budget changes.",
            "Resolve R9.6 serving identity through verified Windows launcher/child process argv and ancestor chain; support continuation after a fail-closed partial handoff and reuse a verified running OJ rather than launching a duplicate.",
            "No OJ, frozen Reference, timing threshold, Score, Episode, Candidate budget, Receipt or Provider behavior changed.",
        ],
        "qualification": {"status": "EXISTING_RUN_RECOVERY_RELEASE", "run_id": binding["run_id"],
                          "formal_episode_started": False, "provider_called": False, "gpu_test_run": False},
        "production_binding_sha256": digest((RELEASE / "PRODUCTION_BINDING.json").read_bytes()),
        "source_files": files,
    }
    raw = (json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode("utf-8")
    (RELEASE / "RELEASE_MANIFEST.json").write_bytes(raw)
    (RELEASE / "RELEASE_MANIFEST.json.sha256").write_text(digest(raw) + "  RELEASE_MANIFEST.json\n", encoding="ascii")
    print(json.dumps({"release_id": RID, "manifest_sha256": digest(raw), "members": len(files), "status": manifest["qualification"]["status"]}, indent=2))

if __name__ == "__main__":
    main()
