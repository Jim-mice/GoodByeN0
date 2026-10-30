from __future__ import annotations

import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def process_identity(pid: int) -> dict[str, Any]:
    if not isinstance(pid, int) or pid <= 0:
        raise ValueError("PROCESS_AUDIT_PID_INVALID")
    command = (f"$p=Get-CimInstance Win32_Process -Filter 'ProcessId={pid}'; "
               "if ($null -eq $p) { '{}' } else { $p | Select-Object ProcessId,ParentProcessId,CreationDate,ExecutablePath,CommandLine | ConvertTo-Json -Compress }")
    result = subprocess.run(["powershell.exe", "-NoProfile", "-Command", command], capture_output=True,
                            text=True, timeout=12)
    if result.returncode:
        raise RuntimeError("PROCESS_AUDIT_CIM_FAILED:" + result.stderr[-500:])
    data = json.loads(result.stdout.strip() or "{}")
    if not data:
        raise RuntimeError("PROCESS_AUDIT_PROCESS_NOT_FOUND")
    return data


def append_process_audit(path: Path, *, pid: int, role: str, task_identity: dict[str, Any]) -> dict[str, Any]:
    proc = process_identity(pid)
    record = {"schema": "N0_PROCESS_IDENTITY_EVENT_V1", "recorded_at_utc": datetime.now(timezone.utc).isoformat(),
              "role": role, "task_identity": task_identity, "process": proc}
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("ab") as stream:
        stream.write((json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n").encode("utf-8"))
        stream.flush()
        os.fsync(stream.fileno())
    return record
