import hashlib

import pytest

from n0_integration.attempt_archive import persist_immutable, response_artifact_path


def test_each_attempt_has_its_own_immutable_path_and_identity(tmp_path):
    first = response_artifact_path(tmp_path, "attempt-001")
    second = response_artifact_path(tmp_path, "attempt-002")
    assert first != second
    request_sha = "a" * 64
    one = persist_immutable(first, attempt_id="attempt-001", request_sha256=request_sha,
                            response_bytes=b'{"id":"response-1"}')
    two = persist_immutable(second, attempt_id="attempt-002", request_sha256=request_sha,
                            response_bytes=b'{"id":"response-2"}')
    assert one["attempt_id"] != two["attempt_id"]
    assert one["request_sha256"] == two["request_sha256"] == request_sha.upper()
    assert one["response_sha256"] != two["response_sha256"]
    assert first.read_bytes() == b'{"id":"response-1"}'
    assert second.read_bytes() == b'{"id":"response-2"}'


def test_retry_same_attempt_is_idempotent_but_conflicting_bytes_are_rejected(tmp_path):
    path = response_artifact_path(tmp_path, "attempt-003")
    args = {"attempt_id": "attempt-003", "request_sha256": "b" * 64}
    first = persist_immutable(path, response_bytes=b'{"id":"response-3"}', **args)
    repeated = persist_immutable(path, response_bytes=b'{"id":"response-3"}', **args)
    assert first["response_sha256"] == repeated["response_sha256"]
    assert repeated["status"] == "ALREADY_PRESENT_IDENTICAL"
    with pytest.raises(ValueError, match="PROVIDER_ATTEMPT_ARTIFACT_COLLISION"):
        persist_immutable(path, response_bytes=b'{"id":"different"}', **args)


def test_attempt_id_path_traversal_rejected(tmp_path):
    with pytest.raises(ValueError, match="PROVIDER_ATTEMPT_ID_INVALID"):
        response_artifact_path(tmp_path, "../overwrite")
