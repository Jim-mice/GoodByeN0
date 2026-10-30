import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from n0_autonomous.engine import KnowledgeService


SERVICE = KnowledgeService(ROOT / "knowledge/hardware/atoms_v2.json")


def hardware(query):
    return SERVICE.query_hardware_skill({"query": query})["data"]["results"]


def ids(rows):
    return [row["knowledge_id"] for row in rows]


def test_cc120_max_threads_per_block():
    rows = hardware("compute capability 12.0 maximum threads per block")
    assert ids(rows) == ["cc12.max_threads_per_block"]
    assert "1024" in rows[0]["fact"]["model_visible_text"]


def test_optin_shared_memory_api_returns_meaning_and_cc_limit():
    rows = hardware("cudaDevAttrMaxSharedMemoryPerBlockOptin")
    texts = [row["fact"]["model_visible_text"] for row in rows]
    assert "cuda.runtime.max_shared_memory_per_block_optin" in ids(rows)
    assert any("99 KB" in x and "explicit opt-in" in x for x in texts)
    assert any("may vary by chip" in x and "bytes" in x for x in texts)


def test_exact_sku_sm_count_returns_unknown_and_runtime_path():
    rows = hardware("RTX 5060 Laptop GPU exact SM count")
    assert ids(rows) == ["rtx5060l.sku.sm_count"]
    text = rows[0]["fact"].get("model_visible_text", rows[0]["fact"].get("claim", ""))
    assert "does not specify" in text and "multiProcessorCount" in text


def test_register_risk_does_not_return_cache_or_source_code():
    rows = hardware("register limit per thread and spill risk")
    got = ids(rows)
    assert "cc12.register_file" in got
    assert "cuda_v2.local_spill" in got
    assert not any("l2" in x.lower() or x.startswith("EXP-") for x in got)


def test_runtime_max_threads_per_sm_alias():
    rows = hardware("cudaDevAttrMaxThreadsPerMultiProcessor")
    assert ids(rows) == ["cc12.max_threads_per_sm"]
    assert "1536" in rows[0]["fact"]["model_visible_text"]


def test_partial_warp_shuffle_answer_has_mask_constraints():
    rows = hardware("partial warp __shfl_sync mask active lanes")
    assert ids(rows) == ["cuda_v2.shuffle"]
    text = rows[0]["fact"]["model_visible_text"]
    assert "own bit" in text and "same mask" in text and "undefined" in text
    assert "partial final warp" in text


def test_bandwidth_duplicate_removed():
    rows = hardware("RTX 5060 Laptop published memory bandwidth")
    got = ids(rows)
    assert got.count("rtx5060l.sku.memory_bandwidth") == 1
    assert "rtx5060l.sku.published_bandwidth" not in got


def test_service_initializes_without_expert_assets():
    assert not hasattr(SERVICE, "expert_path")
    assert not hasattr(SERVICE, "query_expert_knowledge")
