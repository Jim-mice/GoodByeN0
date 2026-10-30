"""Minimal, fail-closed Paratera OpenAI-compatible Chat Completions client.

This module is deliberately independent of the V6 Codex Model Adapter. It does
not load credentials from any fallback variable and never logs request headers.
"""
from __future__ import annotations

import json
import hashlib
import base64
import os
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from dataclasses import dataclass
from pathlib import Path
from typing import Any


BASE_URL = "https://llmapi.paratera.com/v1"
API_KEY_ENV = "jlapi"
ALLOWED_MODELS = frozenset({"DeepSeek-V4-Pro", "DeepSeek-V4-Pro-0813", "GLM-5.3", "Qwen3.8-Max"})
ALLOWED_EFFORTS = frozenset({"low", "high"})


class ProviderError(RuntimeError):
    """Sanitized provider error; never contains headers or the API key."""

    def __init__(self, message: str, telemetry: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.telemetry = telemetry or {}


@dataclass(frozen=True)
class CompletionEvidence:
    request_id: str | None
    response_id: str | None
    requested_model: str
    returned_model: str | None
    requested_effort: str
    usage: dict[str, Any] | None
    raw_response: dict[str, Any]
    telemetry: dict[str, Any] | None = None


class ParateraChatCompletions:
    def __init__(self, timeout_seconds: int = 3600, api_key_env: str = API_KEY_ENV) -> None:
        if not isinstance(timeout_seconds, int) or timeout_seconds <= 0 or timeout_seconds > 3600:
            raise ProviderError("PARATERA_TIMEOUT_CONFIGURATION_INVALID")
        if api_key_env not in {"jlapi", "jlapi2"}:
            raise ProviderError("PARATERA_CREDENTIAL_ALIAS_INVALID")
        token = os.environ.get(api_key_env)
        if not token or not token.strip():
            raise ProviderError("PARATERA_CREDENTIAL_MISSING")
        self._token = token.strip()
        self._api_key_env = api_key_env
        self._timeout = timeout_seconds
        self.last_telemetry: dict[str, Any] = {}

    @staticmethod
    def build_body(
        model: str, effort: str, messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None = None,
        tool_choice: str | dict[str, Any] | None = None,
        max_tokens: int | None = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {
            "model": model, "messages": messages,
            "reasoning_effort": effort,
        }
        if max_tokens is not None:
            body["max_tokens"] = int(max_tokens)
        if tools is not None:
            body["tools"] = tools
        if tool_choice is not None:
            body["tool_choice"] = tool_choice
        return body

    @classmethod
    def request_sha256(
        cls, model: str, effort: str, messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None = None,
        tool_choice: str | dict[str, Any] | None = None,
        max_tokens: int | None = None,
    ) -> str:
        body = cls.build_body(model, effort, messages, tools, tool_choice, max_tokens)
        data = json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        return hashlib.sha256(data).hexdigest()

    def complete(
        self,
        model: str,
        effort: str,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None = None,
        tool_choice: str | dict[str, Any] | None = None,
        max_tokens: int | None = None,
        attempt_id: str | None = None,
        request_id: str | None = None,
        response_artifact_path: Path | str | None = None,
    ) -> tuple[dict[str, Any], CompletionEvidence]:
        if model not in ALLOWED_MODELS:
            raise ProviderError("MODEL_NOT_ALLOWLISTED")
        if effort not in ALLOWED_EFFORTS:
            raise ProviderError("EFFORT_NOT_ALLOWLISTED")
        body = self.build_body(model, effort, messages, tools, tool_choice, max_tokens)
        data = json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        request_sha = hashlib.sha256(data).hexdigest()
        attempt_id = attempt_id or uuid.uuid4().hex
        artifact_path = Path(response_artifact_path).resolve() if response_artifact_path else None
        started = time.monotonic()
        helper = Path(__file__).with_name("paratera_http_worker.py")
        telemetry = {
            "request_sha256": request_sha, "http_status": None,
            "request_id": request_id, "key_alias": self._api_key_env,
            "credential_sha256_prefix": hashlib.sha256(self._token.encode("utf-8")).hexdigest()[:12],
            "provider_base_url": BASE_URL, "model_id": model,
            "provider_release_id": os.environ.get("N0_PROVIDER_RELEASE_ID"),
            "provider_manifest_sha256": os.environ.get("N0_PROVIDER_MANIFEST_SHA256"),
            "request_started_utc": datetime.now(timezone.utc).isoformat(),
            "time_to_first_body_byte": None,
            "response_headers_received_at": None, "first_body_byte_at": None,
            "body_bytes_received": 0, "response_bytes_received": 0,
            "content_length_if_present": None, "transfer_encoding_if_present": None,
            "body_complete": False, "json_parse_status": "NOT_ATTEMPTED",
            "exception_class": None, "exception_errno": None, "exception_reason": None,
            "response_id": None, "elapsed_seconds": 0.0,
            "terminal_status": "OUTCOME_UNKNOWN", "helper_pid": None,
            "attempt_id": attempt_id, "request_sha256": request_sha,
            "response_artifact_path": str(artifact_path) if artifact_path else None,
            "helper_returncode": None, "helper_terminal_event_received": False,
            "helper_pipe_reader_error": None,
        }
        if not helper.is_file():
            raise ProviderError("PARATERA_HTTP_WORKER_MISSING", telemetry)
        self.last_telemetry = dict(telemetry)
        try:
            child_env = dict(os.environ)
            child_env.pop("jlapi", None); child_env.pop("jlapi2", None)
            # The HTTP helper has one credential variable by design. Only the
            # selected window's token is placed in that child process.
            child_env[API_KEY_ENV] = self._token
            proc = subprocess.Popen(
                [sys.executable, "-u", str(helper)], stdin=subprocess.PIPE,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                encoding="utf-8", errors="replace", bufsize=1, env=child_env,
            )
            telemetry["helper_pid"] = proc.pid
            remaining = self._timeout - (time.monotonic() - started)
            if remaining <= 0:
                raise subprocess.TimeoutExpired(proc.args, self._timeout)
            state: dict[str, Any] = {"terminal": None, "writer_error": None,
                                     "reader_error": None, "stderr": ""}
            state_lock = threading.Lock()

            def read_worker_events() -> None:
                assert proc.stdout is not None
                try:
                    for line in proc.stdout:
                        try:
                            event = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        with state_lock:
                            if isinstance(event.get("telemetry"), dict):
                                telemetry.update(event["telemetry"])
                            if isinstance(event.get("raw_body_prefix_hex"), str):
                                telemetry["raw_body_prefix_hex"] = event["raw_body_prefix_hex"][:8192]
                                self.last_telemetry = dict(telemetry)
                            if event.get("event") == "TERMINAL":
                                state["terminal"] = event
                except Exception as exc:
                    with state_lock:
                        state["reader_error"] = type(exc).__name__

            def read_worker_stderr() -> None:
                assert proc.stderr is not None
                try:
                    data = proc.stderr.read(8192)
                    state["stderr"] = data
                except Exception as exc:
                    state["stderr"] = "stderr_reader_error:" + type(exc).__name__

            def write_request() -> None:
                try:
                    assert proc.stdin is not None
                    proc.stdin.write(json.dumps({"url": BASE_URL + "/chat/completions", "body": body,
                                                 "timeout_seconds": self._timeout,
                                                 "attempt_id": attempt_id,
                                                 "request_id": request_id,
                                                 "key_alias": self._api_key_env,
                                                 "request_sha256": request_sha,
                                                 "response_artifact_path": str(artifact_path) if artifact_path else None},
                                                ensure_ascii=False, separators=(",", ":")))
                    proc.stdin.flush()
                    proc.stdin.close()
                except Exception as exc:
                    with state_lock:
                        state["writer_error"] = type(exc).__name__

            reader = threading.Thread(target=read_worker_events, name="paratera-http-output", daemon=True)
            stderr_reader = threading.Thread(target=read_worker_stderr, name="paratera-http-stderr", daemon=True)
            writer = threading.Thread(target=write_request, name="paratera-http-input", daemon=True)
            reader.start()
            stderr_reader.start()
            writer.start()
            timed_out = False
            try:
                proc.wait(timeout=remaining)
            except subprocess.TimeoutExpired:
                timed_out = True
                try:
                    proc.kill()
                    telemetry["helper_termination_requested"] = True
                except Exception as exc:
                    telemetry["helper_termination_error"] = type(exc).__name__
                try:
                    proc.wait(timeout=10)
                    telemetry["helper_cleanup_status"] = "TERMINATED_AND_REAPED"
                except Exception as exc:
                    telemetry["helper_cleanup_status"] = "CLEANUP_FAILED"
                    telemetry["helper_cleanup_error"] = type(exc).__name__
            writer.join(timeout=3)
            reader.join(timeout=10)
            stderr_reader.join(timeout=3)
            telemetry["elapsed_seconds"] = round(time.monotonic() - started, 3)
            telemetry["helper_returncode"] = proc.returncode
            self.last_telemetry = dict(telemetry)
            if timed_out:
                telemetry["terminal_status"] = "TIMEOUT_OUTCOME_UNKNOWN"
                raise ProviderError("PARATERA_TOTAL_REQUEST_TIMEOUT_OUTCOME_UNKNOWN", telemetry)
            if writer.is_alive() or reader.is_alive() or stderr_reader.is_alive():
                telemetry["terminal_status"] = "HTTP_WORKER_PIPE_CLEANUP_FAILED"
                raise ProviderError("PARATERA_HTTP_WORKER_PIPE_CLEANUP_FAILED", telemetry)
            with state_lock:
                transport = state["terminal"]
                writer_error = state["writer_error"]
                telemetry["helper_pipe_reader_error"] = state["reader_error"]
                telemetry["helper_terminal_event_received"] = transport is not None
                stderr_text = state["stderr"]
            token = self._token
            if stderr_text:
                safe_stderr = stderr_text.replace(token, "[REDACTED]")
                safe_stderr = __import__("re").sub(r"(?i)(bearer\s+)[^\s,;]+", r"\1[REDACTED]", safe_stderr)
                telemetry["helper_stderr"] = safe_stderr[:4096]

            # The atomic sidecar is durable before the HTTP worker writes TERMINAL.
            # If pipe delivery fails after a complete response is saved, recover it
            # without issuing another provider request.
            artifact = None
            if artifact_path is not None and artifact_path.is_file():
                try:
                    artifact = self._load_response_artifact(artifact_path, attempt_id, request_sha)
                except Exception as exc:
                    telemetry["response_artifact_validation_error"] = type(exc).__name__ + ":" + str(exc)
            if artifact is not None and artifact.get("body_complete") is True and isinstance(artifact.get("payload"), dict):
                payload = artifact["payload"]
                transport_telemetry = dict(artifact.get("telemetry") or {})
                transport_telemetry.update({"helper_pid": proc.pid, "helper_returncode": proc.returncode,
                                            "helper_terminal_event_received": transport is not None,
                                            "response_artifact_path": str(artifact_path),
                                            "response_artifact_sha256": artifact.get("artifact_sha256"),
                                            "response_handoff_recovered": (proc.returncode != 0 or transport is None or state["reader_error"] is not None)})
                telemetry.update(transport_telemetry)
                transport = {"terminal_status": "RESPONSE_RECEIVED", "telemetry": transport_telemetry,
                             "payload": payload,
                             "response_headers": {"x-request-id": artifact.get("provider_request_id")}}
            elif proc.returncode != 0:
                if transport is not None and isinstance(transport.get("telemetry"), dict):
                    telemetry.update(transport["telemetry"])
                telemetry["helper_returncode"] = proc.returncode
                telemetry["terminal_status"] = (transport or {}).get("terminal_status", "HTTP_WORKER_EXIT_NONZERO")
                raise ProviderError("PARATERA_HTTP_WORKER_EXIT_NONZERO_OUTCOME_UNKNOWN", telemetry)
            if transport is None:
                telemetry["terminal_status"] = "HTTP_WORKER_TERMINAL_EVENT_MISSING"
                raise ProviderError("PARATERA_HTTP_WORKER_TERMINAL_EVENT_MISSING", telemetry)
            telemetry.update(transport.get("telemetry", {}))
            if isinstance(transport.get("partial_body_base64"), str):
                telemetry["partial_body_base64"] = transport["partial_body_base64"]
            telemetry["elapsed_seconds"] = round(time.monotonic() - started, 3)
            if writer_error:
                telemetry["writer_error"] = writer_error
            status = transport.get("terminal_status")
            if status == "HTTP_ERROR":
                telemetry["terminal_status"] = status
                code = telemetry.get("http_status")
                raise ProviderError(f"PARATERA_HTTP_ERROR_STATUS_{code}", telemetry)
            if status != "RESPONSE_RECEIVED":
                telemetry["terminal_status"] = status or "TRANSPORT_FAILURE_OUTCOME_UNKNOWN"
                raise ProviderError("PARATERA_TRANSPORT_FAILURE_OUTCOME_UNKNOWN", telemetry)
            payload = transport.get("payload")
            headers = transport.get("response_headers", {})
        except ProviderError:
            raise
        except (OSError, subprocess.SubprocessError):
            telemetry.update({"elapsed_seconds": round(time.monotonic() - started, 3),
                              "terminal_status": "TRANSPORT_FAILURE_OUTCOME_UNKNOWN"})
            raise ProviderError("PARATERA_TRANSPORT_FAILURE_OUTCOME_UNKNOWN", telemetry) from None
        if not isinstance(payload, dict) or not isinstance(payload.get("choices"), list):
            telemetry["terminal_status"] = "PROTOCOL_FAILURE"
            raise ProviderError("PARATERA_PROTOCOL_FAILURE", telemetry)
        response_id = payload.get("id")
        telemetry["response_id"] = response_id if isinstance(response_id, str) else None
        telemetry["terminal_status"] = "RESPONSE_RECEIVED"
        request_id = headers.get("x-request-id") or headers.get("request-id")
        evidence = CompletionEvidence(
            request_id=request_id,
            response_id=response_id if isinstance(response_id, str) else None,
            requested_model=model,
            returned_model=payload.get("model") if isinstance(payload.get("model"), str) else None,
            requested_effort=effort,
            usage=payload.get("usage") if isinstance(payload.get("usage"), dict) else None,
            raw_response=payload,
            telemetry=telemetry,
        )
        return payload, evidence

    @staticmethod
    def _load_response_artifact(path: Path, attempt_id: str, request_sha: str) -> dict[str, Any]:
        raw_file = path.read_bytes()
        record = json.loads(raw_file.decode("utf-8", errors="strict"))
        if record.get("schema") != "NOVELTY_V7_PROVIDER_RESPONSE_ARTIFACT_V1":
            raise ValueError("RESPONSE_ARTIFACT_SCHEMA_MISMATCH")
        if record.get("attempt_id") != attempt_id or record.get("request_sha256") != request_sha:
            raise ValueError("RESPONSE_ARTIFACT_REQUEST_IDENTITY_MISMATCH")
        body = base64.b64decode(record.get("body_base64", ""), validate=True)
        if hashlib.sha256(body).hexdigest() != record.get("body_sha256"):
            raise ValueError("RESPONSE_ARTIFACT_BODY_SHA_MISMATCH")
        if len(body) != record.get("body_bytes_received"):
            raise ValueError("RESPONSE_ARTIFACT_BODY_LENGTH_MISMATCH")
        if record.get("body_complete") is not True:
            raise ValueError("RESPONSE_ARTIFACT_BODY_INCOMPLETE")
        content_length = record.get("content_length_if_present")
        if content_length is not None and len(body) != int(content_length):
            raise ValueError("RESPONSE_ARTIFACT_CONTENT_LENGTH_MISMATCH")
        if record.get("telemetry", {}).get("response_mode") == "SSE":
            payload = record.get("normalized_payload")
            if not isinstance(payload, dict):
                raise ValueError("SSE_RESPONSE_ARTIFACT_NORMALIZED_PAYLOAD_MISSING")
            record["raw_sse_body_base64"] = record.pop("body_base64")
        else:
            payload = json.loads(body.decode("utf-8", errors="strict"))
        if not isinstance(payload, dict) or not isinstance(payload.get("choices"), list):
            raise ValueError("RESPONSE_ARTIFACT_CHAT_COMPLETIONS_SCHEMA_INVALID")
        record["payload"] = payload
        record["artifact_sha256"] = hashlib.sha256(raw_file).hexdigest()
        return record
