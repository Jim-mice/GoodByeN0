from __future__ import annotations

import hashlib
import io
import json
import sys
import urllib.error
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "provider_staging"))
import paratera_http_worker as http_worker


def _http_error(body: bytes, status: int = 400):
    return urllib.error.HTTPError("https://provider.invalid", status, "bad request",
                                  {"Content-Length": str(len(body))}, io.BytesIO(body))


def test_http_400_body_and_identity_are_persisted(tmp_path, monkeypatch):
    monkeypatch.setenv("jlapi", "do-not-leak")
    body = b'{"error":{"code":"bad_tool_schema","message":"schema mismatch do-not-leak"}}'
    artifact = tmp_path / "attempt-01.response.json"
    request_body = {"model": "m"}
    request_sha = hashlib.sha256(json.dumps(request_body, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()
    payload = {"url": "https://provider.invalid", "body": {"model": "m"},
               "attempt_id": "attempt-01", "request_sha256": request_sha,
               "response_artifact_path": str(artifact)}
    result = http_worker.perform(payload, opener=lambda *a, **k: (_ for _ in ()).throw(_http_error(body)))
    assert result["terminal_status"] == "HTTP_ERROR"
    telemetry = result["telemetry"]
    assert telemetry["http_status"] == 400
    assert telemetry["request_sha256"] == request_sha
    assert telemetry["attempt_id"] == "attempt-01"
    assert "bad_tool_schema" in telemetry["http_error_body_redacted"]
    record = json.loads(artifact.with_name("attempt-01.response.http-error.json").read_text(encoding="utf-8"))
    assert record["attempt_id"] == "attempt-01"
    assert record["request_sha256"] == request_sha
    assert record["http_status"] == 400
    assert record["body_complete"] is True
    assert record["redacted_body_sha256"] == hashlib.sha256(telemetry["http_error_body_redacted"].encode()).hexdigest()
    assert "do-not-leak" not in json.dumps(record)


def test_http_error_truncation_is_not_reported_complete(tmp_path, monkeypatch):
    monkeypatch.setenv("jlapi", "secret")
    body = b"x" * 9000
    artifact = tmp_path / "attempt-02.response.json"
    request_body = {"model": "m"}
    request_sha = hashlib.sha256(json.dumps(request_body, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()
    payload = {"url": "https://provider.invalid", "body": {"model": "m"},
               "attempt_id": "attempt-02", "request_sha256": request_sha,
               "response_artifact_path": str(artifact)}
    result = http_worker.perform(payload, opener=lambda *a, **k: (_ for _ in ()).throw(_http_error(body)))
    telemetry = result["telemetry"]
    assert telemetry["http_error_body_truncated"] is True
    assert telemetry["body_complete"] is False
    record = json.loads(artifact.with_name("attempt-02.response.http-error.json").read_text(encoding="utf-8"))
    assert record["body_truncated"] is True
    assert record["body_bytes_received"] == 8193
    assert len(record["redacted_body_utf8"].encode()) <= 8192
