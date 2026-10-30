"""Isolated Paratera HTTP worker with stage-specific, credential-safe diagnostics."""
from __future__ import annotations

import json
import base64
import hashlib
import http.client
import os
import re
import sys
import uuid
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from http.client import IncompleteRead
from typing import Any, Callable
from pathlib import Path


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_reason(value: Any, token: str) -> str | None:
    if value is None:
        return None
    value = str(value)
    if token:
        value = value.replace(token, "[REDACTED]")
    value = re.sub(r"(?i)(bearer\s+)[^\s,;]+", r"\1[REDACTED]", value)
    value = re.sub(r"(?i)(api[_-]?key\s*[=:]\s*)[^\s,;]+", r"\1[REDACTED]", value)
    return value[:500]


def _error_fields(raw: bytes, token: str) -> str:
    try:
        data = json.loads(raw.decode("utf-8", errors="strict"))
        error = data.get("error", {}) if isinstance(data, dict) else {}
        if isinstance(error, dict):
            parts = [error.get(k) for k in ("type", "code", "param", "message")]
            return _safe_reason(" ".join(str(x) for x in parts if isinstance(x, (str, int, float))), token) or ""
    except Exception:
        pass
    return ""


def _base_telemetry() -> dict[str, Any]:
    return {
        "request_started_utc": _utc_now(),
        "http_method": "POST",
        "key_alias": None,
        "credential_sha256_prefix": None,
        "provider_release_id": os.environ.get("N0_PROVIDER_RELEASE_ID"),
        "provider_manifest_sha256": os.environ.get("N0_PROVIDER_MANIFEST_SHA256"),
        "request_id": None,
        "http_status": None,
        "response_headers_received_at": None,
        "first_body_byte_at": None,
        "body_bytes_received": 0,
        "response_bytes_received": 0,
        "content_length_if_present": None,
        "transfer_encoding_if_present": None,
        "body_complete": False,
        "elapsed_seconds": 0.0,
        "json_parse_status": "NOT_ATTEMPTED",
        "exception_class": None,
        "exception_errno": None,
        "exception_reason": None,
        "response_id": None,
    }


def _diagnostic_bytes(raw: bytes, token: str) -> bytes:
    safe = raw
    if token:
        secret = token.encode("utf-8", errors="ignore")
        if secret: safe = safe.replace(secret,b"[REDACTED]")
    return re.sub(rb"(?i)(bearer\s+)[^\s,;\"']+",rb"\1[REDACTED]",safe)


def _persist_response_artifact(payload: dict[str, Any], raw: bytes, telemetry: dict[str, Any], token: str,
                               normalized_payload: dict[str, Any] | None = None) -> dict[str, Any] | None:
    """Persist transport bytes before handing a response back over stdout."""
    target = payload.get("response_artifact_path")
    if not isinstance(target, str) or not target:
        return None
    if token and token.encode("utf-8", errors="ignore") in raw:
        telemetry["response_artifact_status"] = "REFUSED_SECRET_ECHO"
        return None
    path = Path(target)
    body_sha = hashlib.sha256(raw).hexdigest()
    record = {
        "schema": "NOVELTY_V7_PROVIDER_RESPONSE_ARTIFACT_V1",
        "attempt_id": payload.get("attempt_id"),
        "request_sha256": payload.get("request_sha256"),
        "http_status": telemetry.get("http_status"),
        "provider_request_id": telemetry.get("provider_request_id"),
        "content_length_if_present": telemetry.get("content_length_if_present"),
        "transfer_encoding_if_present": telemetry.get("transfer_encoding_if_present"),
        "body_bytes_received": len(raw),
        "body_sha256": body_sha,
        "body_complete": bool(telemetry.get("body_complete")),
        "json_parse_status": telemetry.get("json_parse_status"),
        "terminal_status": telemetry.get("terminal_status"),
        "telemetry": dict(telemetry),
        "body_base64": base64.b64encode(raw).decode("ascii"),
    }
    if normalized_payload is not None:
        record["normalized_payload"] = normalized_payload
    encoded = (json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        try:
            old = json.loads(path.read_text(encoding="utf-8"))
            if old.get("attempt_id") == record["attempt_id"] and old.get("body_sha256") == body_sha:
                telemetry["response_artifact_status"] = "ALREADY_PRESENT_IDENTICAL"
                telemetry["response_artifact_path"] = str(path)
                telemetry["response_artifact_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
                return {"path": str(path), "sha256": telemetry["response_artifact_sha256"], "body_sha256": body_sha}
        except Exception:
            pass
        telemetry["response_artifact_status"] = "IMMUTABLE_PATH_COLLISION"
        return None
    temp = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    try:
        with temp.open("xb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp, path)
    finally:
        try:
            temp.unlink(missing_ok=True)
        except OSError:
            pass
    telemetry["response_artifact_status"] = "PERSISTED_ATOMICALLY"
    telemetry["response_artifact_path"] = str(path)
    telemetry["response_artifact_sha256"] = hashlib.sha256(encoded).hexdigest()
    return {"path": str(path), "sha256": telemetry["response_artifact_sha256"], "body_sha256": body_sha}


def _persist_http_error_artifact(payload: dict[str, Any], raw: bytes, telemetry: dict[str, Any], token: str) -> dict[str, Any] | None:
    """Persist a bounded, credential-redacted HTTP error body immutably."""
    response_path = payload.get("response_artifact_path")
    if not isinstance(response_path, str) or not response_path:
        return None
    safe = _diagnostic_bytes(raw[:8192], token)
    response = Path(response_path)
    path = response.with_name(response.stem + ".http-error.json")
    record = {
        "schema": "N0_PROVIDER_HTTP_ERROR_ARTIFACT_V1",
        "attempt_id": payload.get("attempt_id"),
        "request_sha256": payload.get("request_sha256"),
        "http_status": telemetry.get("http_status"),
        "body_bytes_received": int(telemetry.get("body_bytes_received", len(raw))),
        "body_truncated": bool(telemetry.get("http_error_body_truncated", len(raw) > 8192)),
        "body_complete": bool(telemetry.get("body_complete")),
        "redacted_body_sha256": hashlib.sha256(safe).hexdigest(),
        "redacted_body_utf8": safe.decode("utf-8", errors="replace"),
        "telemetry": dict(telemetry),
    }
    encoded = (json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        old = path.read_bytes()
        if old != encoded:
            telemetry["http_error_artifact_status"] = "IMMUTABLE_PATH_COLLISION"
            return None
        telemetry["http_error_artifact_status"] = "ALREADY_PRESENT_IDENTICAL"
    else:
        temp = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
        try:
            with temp.open("xb") as stream:
                stream.write(encoded)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temp, path)
            telemetry["http_error_artifact_status"] = "PERSISTED_ATOMICALLY"
        finally:
            try:
                temp.unlink(missing_ok=True)
            except OSError:
                pass
    telemetry["http_error_artifact_path"] = str(path)
    telemetry["http_error_artifact_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
    telemetry["http_error_body_sha256"] = hashlib.sha256(safe).hexdigest()
    return {"path": str(path), "sha256": telemetry["http_error_artifact_sha256"],
            "body_sha256": telemetry["http_error_body_sha256"]}


def _aggregate_sse(raw: bytes, telemetry: dict[str, Any]) -> dict[str, Any]:
    """Parse a complete Chat Completions SSE stream without inventing fields."""
    text = raw.decode("utf-8-sig", errors="strict")
    events: list[dict[str, Any]] = []
    data_lines: list[str] = []
    done = False

    def consume() -> None:
        nonlocal done, data_lines
        if not data_lines:
            return
        data = "\n".join(data_lines)
        data_lines = []
        if data.strip() == "[DONE]":
            done = True
            return
        try:
            item = json.loads(data)
        except json.JSONDecodeError as exc:
            raise ValueError("SSE_EVENT_JSON_INVALID") from exc
        if not isinstance(item, dict):
            raise ValueError("SSE_EVENT_NOT_OBJECT")
        events.append(item)

    for line in text.splitlines():
        if line == "":
            consume()
        elif line.startswith(":"):
            continue
        elif line.startswith("data:"):
            value = line[5:]
            data_lines.append(value[1:] if value.startswith(" ") else value)
        elif line.startswith("event:") or line.startswith("id:") or line.startswith("retry:"):
            continue
        else:
            raise ValueError("SSE_LINE_FORMAT_INVALID")
    consume()
    if not done:
        raise ValueError("SSE_DONE_MARKER_MISSING")

    choices: dict[int, dict[str, Any]] = {}
    first: dict[str, Any] = {}
    usage: dict[str, Any] | None = None
    for event in events:
        for key in ("id", "created", "model", "system_fingerprint", "service_tier"):
            if key in event and key not in first:
                first[key] = event[key]
        if isinstance(event.get("usage"), dict):
            usage = event["usage"]
        for choice in event.get("choices", []) if isinstance(event.get("choices"), list) else []:
            if not isinstance(choice, dict):
                continue
            try:
                index = int(choice.get("index", 0))
            except (TypeError, ValueError):
                raise ValueError("SSE_CHOICE_INDEX_INVALID")
            target = choices.setdefault(index, {"message": {"role": None, "content": ""}, "tool_calls": {}})
            delta = choice.get("delta")
            if isinstance(delta, dict):
                message = target["message"]
                if isinstance(delta.get("role"), str):
                    message["role"] = delta["role"]
                for key in ("content", "reasoning_content", "refusal"):
                    if isinstance(delta.get(key), str):
                        if key == "content":
                            message["content"] += delta[key]
                        else:
                            message[key] = message.get(key, "") + delta[key]
                for call in delta.get("tool_calls", []) if isinstance(delta.get("tool_calls"), list) else []:
                    if not isinstance(call, dict):
                        continue
                    try:
                        call_index = int(call.get("index", 0))
                    except (TypeError, ValueError):
                        raise ValueError("SSE_TOOL_CALL_INDEX_INVALID")
                    tool = target["tool_calls"].setdefault(call_index, {"id": None, "type": "function", "function": {"name": "", "arguments": ""}})
                    if isinstance(call.get("id"), str):
                        tool["id"] = call["id"]
                    if isinstance(call.get("type"), str):
                        tool["type"] = call["type"]
                    function = call.get("function")
                    if isinstance(function, dict):
                        if isinstance(function.get("name"), str):
                            tool["function"]["name"] += function["name"]
                        if isinstance(function.get("arguments"), str):
                            tool["function"]["arguments"] += function["arguments"]
                if isinstance(delta.get("function_call"), dict):
                    function = target["message"].setdefault("function_call", {"name": "", "arguments": ""})
                    for key in ("name", "arguments"):
                        if isinstance(delta["function_call"].get(key), str):
                            function[key] += delta["function_call"][key]
            if "finish_reason" in choice:
                target["finish_reason"] = choice["finish_reason"]
            if "logprobs" in choice:
                target["logprobs"] = choice["logprobs"]
    if not choices:
        raise ValueError("SSE_NO_CHOICES")
    normalized_choices = []
    for index in sorted(choices):
        item = choices[index]
        calls = item.pop("tool_calls")
        if calls:
            item["message"]["tool_calls"] = [calls[key] for key in sorted(calls)]
        if not item["message"].get("role"):
            raise ValueError("SSE_ASSISTANT_ROLE_MISSING")
        normalized_choices.append({"index": index, "message": item["message"],
                                   "finish_reason": item.get("finish_reason"),
                                   "logprobs": item.get("logprobs")})
    if any(choice.get("finish_reason") is None for choice in normalized_choices):
        raise ValueError("SSE_FINISH_REASON_MISSING")
    result: dict[str, Any] = {"object": "chat.completion", "choices": normalized_choices}
    result.update(first)
    if usage is not None:
        result["usage"] = usage
    telemetry.update({"response_mode": "SSE", "sse_event_count": len(events), "sse_done_received": True,
                      "sse_finish_reasons": [choice.get("finish_reason") for choice in normalized_choices],
                      "usage_present_in_stream": usage is not None})
    return result


def perform(payload: dict[str, Any], timeout_seconds: float | None = None,
            emit: Callable[[dict[str, Any]], None] | None = None,
            opener: Callable[..., Any] | None = None,
            clock: Callable[[], float] | None = None) -> dict[str, Any]:
    token = os.environ.get("jlapi", "").strip()
    telemetry = _base_telemetry()
    telemetry["key_alias"] = payload.get("key_alias") if isinstance(payload.get("key_alias"), str) else None
    request_url = payload.get("url")
    telemetry["provider_request_url"] = request_url if isinstance(request_url, str) else None
    if not token:
        return {"terminal_status": "CREDENTIAL_MISSING", "telemetry": telemetry}
    url, body = payload.get("url"), payload.get("body")
    if not isinstance(url, str) or not isinstance(body, dict):
        return {"terminal_status": "WORKER_INPUT_INVALID", "telemetry": telemetry}
    timeout = float(timeout_seconds if timeout_seconds is not None else payload.get("timeout_seconds", 3600))
    if timeout <= 0 or timeout > 3600:
        return {"terminal_status": "TIMEOUT_CONFIGURATION_INVALID", "telemetry": telemetry}
    monotonic = clock or time.monotonic
    started = monotonic()
    telemetry["attempt_id"] = payload.get("attempt_id")
    telemetry["request_id"] = payload.get("request_id")
    telemetry["request_sha256"] = payload.get("request_sha256")
    logical_data = json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    # Streaming is transport-only. Keep the queued/logical body hash intact and
    # record the distinct bytes actually sent on the wire.
    wire_body = dict(body)
    wire_body["stream"] = True
    data = json.dumps(wire_body, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    actual_request_sha = hashlib.sha256(logical_data).hexdigest()
    telemetry.update({
        "model_id": body.get("model"),
        "request_sha256_actual": actual_request_sha,
        "wire_request_sha256": hashlib.sha256(data).hexdigest(),
        "response_mode_requested": "SSE",
        "credential_sha256_prefix": hashlib.sha256(token.encode("utf-8")).hexdigest()[:12],
        "request_message_count": len(body.get("messages", [])) if isinstance(body.get("messages"), list) else None,
        "request_message_roles": [m.get("role") for m in body.get("messages", []) if isinstance(m, dict)] if isinstance(body.get("messages"), list) else [],
        "request_tool_names": [t.get("function", {}).get("name") for t in body.get("tools", []) if isinstance(t, dict) and isinstance(t.get("function"), dict)] if isinstance(body.get("tools"), list) else [],
        "request_tool_choice": body.get("tool_choice"),
        "request_reasoning_effort": body.get("reasoning_effort"),
        "request_max_tokens": body.get("max_tokens"),
    })
    if payload.get("request_sha256") != actual_request_sha:
        telemetry["terminal_status"] = "REQUEST_HASH_MISMATCH"
        telemetry["elapsed_seconds"] = round(monotonic() - started, 3)
        return {"terminal_status": "REQUEST_HASH_MISMATCH", "telemetry": telemetry}
    request = urllib.request.Request(url, data=data,
        headers={"Authorization": "Bearer " + token, "Content-Type": "application/json",
                 "Accept": "text/event-stream"}, method="POST")
    open_request = opener or urllib.request.urlopen
    response = None
    raw = bytearray()

    def publish(event: str) -> None:
        if emit:
            emit({"event": event, "telemetry": dict(telemetry), "raw_body_prefix_hex": _diagnostic_bytes(bytes(raw[:4096]),token).hex()})

    try:
        response = open_request(request, timeout=timeout)
        telemetry["http_status"] = getattr(response, "status", None) or response.getcode()
        telemetry["response_headers_received_at"] = _utc_now()
        telemetry["response_header_elapsed_seconds"] = round(monotonic() - started, 3)
        headers = response.headers
        telemetry["response_content_type"] = headers.get("Content-Type")
        telemetry["connected_at"] = telemetry["response_headers_received_at"]
        telemetry["connection_elapsed_seconds"] = telemetry["response_header_elapsed_seconds"]
        telemetry["content_length_if_present"] = headers.get("Content-Length")
        telemetry["transfer_encoding_if_present"] = headers.get("Transfer-Encoding")
        telemetry["provider_request_id"] = headers.get("x-request-id") or headers.get("request-id")
        publish("HTTP_HEADERS_RECEIVED")
        last_data_at = None
        max_data_gap = 0.0
        while True:
            try:
                # HTTPResponse.readline returns each flushed SSE line as it
                # arrives. Fake/non-stream responses retain the read fallback.
                chunk = response.readline() if callable(getattr(response, "readline", None)) else response.read(64 * 1024)
            except IncompleteRead as exc:
                partial = getattr(exc, "partial", b"") or b""
                if partial and telemetry["first_body_byte_at"] is None:
                    telemetry["first_body_byte_at"] = _utc_now()
                raw.extend(partial)
                telemetry["body_bytes_received"] = len(raw)
                telemetry["response_bytes_received"] = len(raw)
                telemetry["exception_class"] = type(exc).__name__
                telemetry["exception_errno"] = getattr(exc, "errno", None)
                telemetry["exception_reason"] = _safe_reason(exc, token)
                expected = telemetry.get("content_length_if_present")
                expected_matches = False
                if expected is not None:
                    try:
                        expected_matches = len(raw) == int(expected)
                    except (TypeError, ValueError):
                        expected_matches = False
                telemetry["body_complete"] = expected_matches
                telemetry["elapsed_seconds"] = round(monotonic() - started, 3)
                telemetry["read_exception_after_declared_length"] = expected_matches
                if not expected_matches:
                    telemetry["json_parse_status"] = "NOT_ATTEMPTED_INCOMPLETE_BODY"
                    telemetry["terminal_status"] = "HTTP_BODY_INCOMPLETE"
                    _persist_response_artifact(payload, bytes(raw), telemetry, token)
                    publish("BODY_READ_INTERRUPTED")
                    return {"terminal_status": "HTTP_BODY_INCOMPLETE", "telemetry": telemetry,
                            "partial_body_base64": base64.b64encode(_diagnostic_bytes(bytes(raw),token)).decode("ascii")}
                telemetry["exception_class"] = "IncompleteReadAfterDeclaredContentLength"
                telemetry["exception_reason"] = "HTTP library raised after all declared Content-Length bytes were received"
                break
            if not chunk:
                break
            if telemetry["first_body_byte_at"] is None:
                telemetry["first_body_byte_at"] = _utc_now()
                telemetry["time_to_first_body_byte"] = round(monotonic() - started, 3)
            raw.extend(chunk)
            telemetry["body_bytes_received"] = len(raw)
            telemetry["response_bytes_received"] = len(raw)
            now = monotonic()
            if chunk.lstrip().startswith(b"data:"):
                if telemetry.get("first_sse_data_at") is None:
                    telemetry["first_sse_data_at"] = _utc_now()
                    telemetry["time_to_first_sse_data_seconds"] = round(now - started, 3)
                if last_data_at is not None:
                    max_data_gap = max(max_data_gap, now - last_data_at)
                last_data_at = now
            telemetry["last_body_byte_at"] = _utc_now()
            telemetry["last_body_byte_elapsed_seconds"] = round(now - started, 3)
            telemetry["max_sse_data_gap_seconds"] = round(max_data_gap, 3)
            publish("BODY_PROGRESS")
            if now - started >= timeout:
                telemetry["exception_class"] = "TotalDeadlineExceeded"
                telemetry["exception_reason"] = "HTTP worker total request deadline reached while reading response body"
                telemetry["elapsed_seconds"] = round(monotonic() - started, 3)
                return {"terminal_status": "TOTAL_TIMEOUT_OUTCOME_UNKNOWN", "telemetry": telemetry,
                        "partial_body_base64": __import__("base64").b64encode(_diagnostic_bytes(bytes(raw),token)).decode("ascii")}
        expected = telemetry["content_length_if_present"]
        if expected is not None:
            try:
                telemetry["body_complete"] = len(raw) == int(expected)
            except (TypeError, ValueError):
                telemetry["body_complete"] = False
                telemetry["content_length_parse_error"] = True
        else:
            # urllib/http.client returns EOF only after a valid chunk terminator for chunked bodies.
            telemetry["body_complete"] = True
        if not telemetry["body_complete"]:
            telemetry["elapsed_seconds"] = round(monotonic() - started, 3)
            telemetry["json_parse_status"] = "NOT_ATTEMPTED_INCOMPLETE_BODY"
            telemetry["terminal_status"] = "HTTP_BODY_INCOMPLETE"
            _persist_response_artifact(payload, bytes(raw), telemetry, token)
            return {"terminal_status": "HTTP_BODY_INCOMPLETE", "telemetry": telemetry,
                    "partial_body_base64": base64.b64encode(_diagnostic_bytes(bytes(raw),token)).decode("ascii")}
        content_type = str(telemetry.get("response_content_type") or "").lower()
        prefix = bytes(raw).lstrip()
        looks_sse = "text/event-stream" in content_type or prefix.startswith((b"data:", b"event:", b":"))
        normalized_payload: dict[str, Any] | None = None
        if looks_sse:
            try:
                normalized_payload = _aggregate_sse(bytes(raw), telemetry)
                telemetry["json_parse_status"] = "PASS_SSE_AGGREGATED"
                telemetry["stream_completed_at"] = _utc_now()
                telemetry["stream_completion_elapsed_seconds"] = round(monotonic() - started, 3)
                telemetry["raw_response_sha256"] = hashlib.sha256(bytes(raw)).hexdigest()
                telemetry["sse_done_received"] = True
                telemetry["finish_reason"] = normalized_payload["choices"][0].get("finish_reason")
            except (UnicodeDecodeError, ValueError, json.JSONDecodeError) as exc:
                telemetry["json_parse_status"] = "FAIL_SSE"
                telemetry["terminal_status"] = "SSE_STREAM_INCOMPLETE_OUTCOME_UNKNOWN" if str(exc) == "SSE_DONE_MARKER_MISSING" else "SSE_PROTOCOL_FAILURE_OUTCOME_UNKNOWN"
                telemetry["exception_class"] = type(exc).__name__
                telemetry["exception_reason"] = _safe_reason(exc, token)
                telemetry["elapsed_seconds"] = round(monotonic() - started, 3)
                telemetry["raw_response_sha256"] = hashlib.sha256(bytes(raw)).hexdigest()
                _persist_response_artifact(payload, bytes(raw), telemetry, token)
                publish("SSE_STREAM_NOT_USABLE")
                return {"terminal_status": telemetry["terminal_status"], "telemetry": telemetry,
                        "partial_body_base64": base64.b64encode(_diagnostic_bytes(bytes(raw), token)).decode("ascii")}
            parsed = normalized_payload
        else:
            try:
                parsed = json.loads(raw.decode("utf-8", errors="strict"))
                if not isinstance(parsed, dict) or not isinstance(parsed.get("choices"), list):
                    raise ValueError("CHAT_COMPLETION_SCHEMA_INVALID")
                telemetry["response_mode"] = "JSON"
                telemetry["json_parse_status"] = "PASS"
            except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
                telemetry["json_parse_status"] = "FAIL"
                telemetry["terminal_status"] = "PROTOCOL_FAILURE_OUTCOME_UNKNOWN"
                telemetry["exception_class"] = type(exc).__name__
                telemetry["exception_reason"] = _safe_reason(exc, token)
                telemetry["elapsed_seconds"] = round(monotonic() - started, 3)
                telemetry["raw_response_sha256"] = hashlib.sha256(bytes(raw)).hexdigest()
                _persist_response_artifact(payload, bytes(raw), telemetry, token)
                publish("JSON_PARSE_FAILED")
                return {"terminal_status": "PROTOCOL_FAILURE_OUTCOME_UNKNOWN", "telemetry": telemetry,
                        "partial_body_base64": base64.b64encode(_diagnostic_bytes(bytes(raw), token)).decode("ascii")}
        telemetry["raw_response_sha256"] = hashlib.sha256(bytes(raw)).hexdigest()
        telemetry["terminal_status"] = "RESPONSE_RECEIVED"
        artifact = _persist_response_artifact(payload, bytes(raw), telemetry, token, normalized_payload=parsed if looks_sse else None)
        if not isinstance(parsed, dict):
            telemetry["json_parse_status"] = "FAIL"
            telemetry["terminal_status"] = "PROTOCOL_FAILURE_OUTCOME_UNKNOWN"
            telemetry["elapsed_seconds"] = round(monotonic() - started, 3)
            return {"terminal_status": "PROTOCOL_FAILURE_OUTCOME_UNKNOWN", "telemetry": telemetry}
        telemetry["response_id"] = parsed.get("id")
        telemetry["elapsed_seconds"] = round(monotonic() - started, 3)
        telemetry["terminal_status"] = "RESPONSE_RECEIVED"
        return {"terminal_status": "RESPONSE_RECEIVED", "payload": parsed,
                "response_headers": {"x-request-id": telemetry.get("provider_request_id")},
                "telemetry": telemetry, "response_artifact": artifact}
    except urllib.error.HTTPError as exc:
        telemetry["http_status"] = exc.code
        telemetry["elapsed_seconds"] = round(monotonic() - started, 3)
        telemetry["response_headers_received_at"] = _utc_now()
        telemetry["content_length_if_present"] = exc.headers.get("Content-Length") if exc.headers else None
        telemetry["transfer_encoding_if_present"] = exc.headers.get("Transfer-Encoding") if exc.headers else None
        retry_after = exc.headers.get("Retry-After") if exc.headers else None
        if retry_after is not None:
            try:
                telemetry["retry_after_seconds"] = max(0.0, float(retry_after.strip()))
            except (TypeError, ValueError):
                try:
                    from email.utils import parsedate_to_datetime
                    from datetime import datetime, timezone
                    retry_time = parsedate_to_datetime(retry_after)
                    telemetry["retry_after_seconds"] = max(0.0, (retry_time - datetime.now(timezone.utc)).total_seconds())
                except Exception:
                    telemetry["retry_after_parse_status"] = "INVALID"
        try:
            # Read one byte beyond the persisted bound so truncation is observable.
            error_body = exc.read(8193)
        except Exception as read_exc:
            error_body = b""
            telemetry["exception_class"] = type(read_exc).__name__
            telemetry["exception_errno"] = getattr(read_exc, "errno", None)
            telemetry["exception_reason"] = _safe_reason(getattr(read_exc, "reason", read_exc), token)
        captured = error_body[:8192]
        telemetry["body_bytes_received"] = len(error_body)
        telemetry["response_bytes_received"] = len(error_body)
        declared_length = telemetry.get("content_length_if_present")
        if len(error_body) > 8192:
            telemetry["body_complete"] = False
            telemetry["http_error_body_truncated"] = True
        elif declared_length is not None:
            try:
                telemetry["body_complete"] = len(error_body) == int(declared_length)
            except (TypeError, ValueError):
                telemetry["body_complete"] = False
                telemetry["content_length_parse_error"] = True
        else:
            # HTTPError.read() returned EOF; no declared length was available.
            telemetry["body_complete"] = True
        safe_error_body = _diagnostic_bytes(captured, token)
        telemetry["http_error_detail"] = _error_fields(captured, token)
        telemetry["http_error_body_redacted"] = safe_error_body.decode("utf-8", errors="replace")
        telemetry["http_error_body_sha256"] = hashlib.sha256(safe_error_body).hexdigest()
        telemetry["http_error_body_truncated"] = len(error_body) > 8192
        _persist_http_error_artifact(payload, captured, telemetry, token)
        publish("HTTP_ERROR_BODY_PERSISTED")
        return {"terminal_status": "HTTP_ERROR",
                "telemetry": telemetry}
    except (urllib.error.URLError, TimeoutError, OSError, http.client.HTTPException) as exc:
        reason = getattr(exc, "reason", None)
        telemetry["exception_class"] = type(exc).__name__
        telemetry["exception_errno"] = getattr(exc, "errno", None)
        if telemetry["exception_errno"] is None:
            telemetry["exception_errno"] = getattr(reason, "errno", None)
        telemetry["exception_reason"] = _safe_reason(reason if reason is not None else exc, token)
        telemetry["elapsed_seconds"] = round(monotonic() - started, 3)
        telemetry["json_parse_status"] = "NOT_ATTEMPTED"
        telemetry["terminal_status"] = "TRANSPORT_FAILURE_OUTCOME_UNKNOWN"
        _persist_response_artifact(payload, bytes(raw), telemetry, token)
        publish("TRANSPORT_FAILURE")
        return {"terminal_status": "TRANSPORT_FAILURE_OUTCOME_UNKNOWN", "telemetry": telemetry,
                "partial_body_base64": base64.b64encode(_diagnostic_bytes(bytes(raw),token)).decode("ascii")}
    finally:
        if response is not None:
            try:
                response.close()
            except Exception:
                pass


def main() -> int:
    try:
        payload = json.loads(sys.stdin.buffer.read().decode("utf-8", errors="strict"))
        def emit(event: dict[str, Any]) -> None:
            sys.stdout.write(json.dumps(event, ensure_ascii=False, separators=(",", ":")) + "\n")
            sys.stdout.flush()
        result = perform(payload, emit=emit)
    except Exception as exc:
        result = {"terminal_status": "WORKER_PROTOCOL_FAILURE", "telemetry": {
            "exception_class": type(exc).__name__, "exception_reason": _safe_reason(exc, os.environ.get("jlapi", ""))}}
    if result.get("terminal_status") == "RESPONSE_RECEIVED" and result.get("response_artifact"):
        # The durable sidecar is authoritative; avoid a large JSON response on a fragile pipe.
        result.pop("payload", None)
        result.pop("response_headers", None)
        result["payload_in_response_artifact"] = True
    sys.stdout.write(json.dumps({"event": "TERMINAL", **result}, ensure_ascii=False, separators=(",", ":")) + "\n")
    sys.stdout.flush()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
