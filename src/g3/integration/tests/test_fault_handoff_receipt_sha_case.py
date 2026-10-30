from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from n0_integration.service_supervisor import same_sha256

def test_same_verified_sha_accepts_hex_case_difference():
    value = "b5719fca261b63d37b723e49ddd77191367c6ffdcfb8334757048036bba2ca0b"
    assert same_sha256(value, value.upper())

def test_receipt_hash_mismatch_is_rejected():
    assert not same_sha256("b" * 64, "c" * 64)

def test_malformed_receipt_hash_is_rejected():
    assert not same_sha256("not-a-hash", "NOT-A-HASH")
