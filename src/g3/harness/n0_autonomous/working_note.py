"""Append-only, verbatim model working-note storage."""
from __future__ import annotations

import hashlib
import json
import os
import time
from pathlib import Path

from .engine import canonical_bytes, sha256_text, N0Error


class WorkingNoteStore:
    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def history(self) -> list[dict]:
        if not self.path.exists():
            return []
        return [json.loads(x) for x in self.path.read_text(encoding="utf-8").splitlines() if x.strip()]

    def latest(self) -> str | None:
        rows = self.history()
        return rows[-1]["text"] if rows else None

    def record(self, text: str | None, *, episode_id: str, turn_id: str) -> dict:
        if text is not None and (not isinstance(text, str) or len(text) > 1000):
            raise N0Error("WORKING_NOTE_INVALID_OR_TOO_LONG")
        rows = self.history()
        prior = [row for row in rows if row.get("episode_id") == episode_id and row.get("turn_id") == turn_id]
        if prior:
            if prior[-1].get("text") != text:
                raise N0Error("WORKING_NOTE_TURN_IDEMPOTENCY_CONFLICT")
            return prior[-1]
        item = {"episode_id":episode_id,"turn_id":turn_id,"update_number":len(rows)+1,
                "text":text,"sha256":sha256_text(text) if text is not None else None,
                "timestamp_utc":time.strftime("%Y-%m-%dT%H:%M:%SZ",time.gmtime())}
        with self.path.open("ab") as f:
            f.write(canonical_bytes(item)+b"\n");f.flush();os.fsync(f.fileno())
        return item
