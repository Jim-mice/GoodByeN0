"""Load the immutable G3 R1 model-visible prompt and four-tool contract."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


RELEASE_ID = "N0_G3_RUNTIME_INTEGRATION_R4_5_PROVIDER_MANIFEST_CASEFIX"
EXPECTED_TOOLS = ["read_workspace_file", "edit_candidate", "query_candidate_history", "query_hardware_skill"]


@dataclass(frozen=True)
class PromptBundle:
    system_prompt: str
    task_user_message: str
    tools: list[dict[str, Any]]
    tool_choice: str
    final_control_schema: dict[str, Any]
    file_sha256: dict[str, str]


def _read_utf8(path: Path) -> tuple[str, str]:
    raw = path.read_bytes()
    return raw.decode("utf-8"), hashlib.sha256(raw).hexdigest().upper()


def load_prompt_bundle(release_root: Path) -> PromptBundle:
    root = Path(release_root)
    manifest_path = root / "RELEASE_MANIFEST.json"
    manifest_raw = manifest_path.read_bytes()
    sidecar = manifest_path.with_name("RELEASE_MANIFEST.json.sha256").read_text(encoding="ascii").split()[0].upper()
    manifest_sha = hashlib.sha256(manifest_raw).hexdigest().upper()
    if manifest_sha != sidecar:
        raise ValueError("N0_RELEASE_MANIFEST_SHA_MISMATCH")
    manifest = json.loads(manifest_raw.decode("utf-8"))
    if manifest.get("release_id") != RELEASE_ID or manifest.get("state") != "STAGED_FOR_G3_READINESS":
        raise ValueError("N0_G3_RELEASE_IDENTITY_OR_STATE_INVALID")
    declared = {x["path"]: x["sha256"] for x in manifest.get("source_files", [])}
    protocol = root / "protocol"
    system, system_sha = _read_utf8(protocol / "BASE_PROMPT.md")
    task, task_sha = _read_utf8(protocol / "TASK_USER_MESSAGE.md")
    tool_raw = (protocol / "TOOL_SCHEMAS.json").read_bytes()
    final_raw = (protocol / "FINAL_CONTROL_SCHEMA.json").read_bytes()
    tool_doc = json.loads(tool_raw.decode("utf-8"))
    final_schema = json.loads(final_raw.decode("utf-8"))
    observed = {
        "protocol/BASE_PROMPT.md": system_sha,
        "protocol/TASK_USER_MESSAGE.md": task_sha,
        "protocol/TOOL_SCHEMAS.json": hashlib.sha256(tool_raw).hexdigest().upper(),
        "protocol/FINAL_CONTROL_SCHEMA.json": hashlib.sha256(final_raw).hexdigest().upper(),
    }
    for relative, digest in observed.items():
        if declared.get(relative) != digest:
            raise ValueError("N0_G3_PROMPT_BUNDLE_NOT_MANIFESTED:" + relative)
    if tool_doc.get("tool_choice") != "auto" or not isinstance(tool_doc.get("tools"), list):
        raise ValueError("N0_G3_TOOL_CONTRACT_INVALID")
    tool_names = [x.get("function", {}).get("name") for x in tool_doc["tools"]]
    if tool_names != EXPECTED_TOOLS:
        raise ValueError("N0_G3_TOOL_SET_OR_ORDER_INVALID")
    for tool in tool_doc["tools"]:
        if "auxiliary_context.txt" in json.dumps(tool, ensure_ascii=False):
            raise ValueError("N0_G3_AUXILIARY_CONTEXT_MUST_NOT_BE_MODEL_ACCESSIBLE")
    return PromptBundle(system, task, tool_doc["tools"], "auto", final_schema, observed)
