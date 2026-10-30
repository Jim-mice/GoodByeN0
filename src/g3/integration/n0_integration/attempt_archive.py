"""Immutable, attempt-scoped artifact locations for diagnostic Provider calls."""
from __future__ import annotations

import hashlib
import os
import re
from pathlib import Path


def response_artifact_path(root: Path, attempt_id: str) -> Path:
    if not isinstance(attempt_id, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", attempt_id):
        raise ValueError("PROVIDER_ATTEMPT_ID_INVALID")
    return Path(root).resolve() / attempt_id / (attempt_id + ".response.json")


def persist_immutable(path: Path, *, attempt_id: str, request_sha256: str, response_bytes: bytes) -> dict[str, str]:
    if not re.fullmatch(r"[0-9a-fA-F]{64}", request_sha256) or not response_bytes:
        raise ValueError("PROVIDER_ARTIFACT_IDENTITY_INVALID")
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("xb") as stream:
            stream.write(response_bytes)
            stream.flush()
            os.fsync(stream.fileno())
        status = "CREATED_IMMUTABLY"
    except FileExistsError:
        if path.read_bytes() != response_bytes:
            raise ValueError("PROVIDER_ATTEMPT_ARTIFACT_COLLISION")
        status = "ALREADY_PRESENT_IDENTICAL"
    return {"attempt_id": attempt_id, "request_sha256": request_sha256.upper(),
            "response_sha256": hashlib.sha256(response_bytes).hexdigest().upper(),
            "response_artifact_path": str(path.resolve()), "status": status}
