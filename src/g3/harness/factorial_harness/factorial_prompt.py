"""Load a task-neutral prompt and frozen tool schemas for an engineering build."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from experiment_policy import ProtocolError, Treatment, treatment_tools, validate_schema_factorial


@dataclass(frozen=True)
class FactorialPrompt:
    system_prompt: str
    task_user_message: str
    tools: list[dict[str, Any]]
    prompt_sha256: str
    schema_sha256: str


def load_factorial_prompt(prompt_root: Path, *, base_tools: list[dict[str, Any]],
                          treatment: Treatment) -> FactorialPrompt:
    root = Path(prompt_root)
    system_raw = (root / "BASE_PROMPT.md").read_bytes()
    task_raw = (root / "TASK_TEMPLATE.md").read_bytes()
    try:
        system = system_raw.decode("utf-8")
        task = task_raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ProtocolError("PROMPT_NOT_UTF8") from exc
    tools = treatment_tools(base_tools, treatment)
    validate_schema_factorial(base_tools)
    body = (system_raw + b"\x00" + task_raw)
    prompt_sha = hashlib.sha256(body).hexdigest().upper()
    schema_sha = hashlib.sha256(json.dumps(tools, ensure_ascii=False, sort_keys=True,
        separators=(",", ":")).encode("utf-8")).hexdigest().upper()
    return FactorialPrompt(system, task, tools, prompt_sha, schema_sha)
