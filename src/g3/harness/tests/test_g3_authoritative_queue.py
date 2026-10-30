import json
import sys
from pathlib import Path
import asyncio

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from n0_autonomous.engine import N0Error, canonical_bytes, sha256_bytes
from n0_autonomous.parallel_runtime import (N0PersistentQueue, N0_COMPILER_CONTRACT_SHA256,
    FourEpisodeWorkerPool, make_worker_contexts, N0_SEED_SHA256)

ARCHIVE = ROOT / "tests" / "fixtures" / "N0_AUTONOMOUS_INFORMATION_ACQUISITION_R4"
TEMPLATE = ARCHIVE / "template"
OJ_RELEASE = ROOT.parents[1] / "runtime" / "oj" / "releases" / "N0_KERNELBENCH_L1_40_LAYERNORM_R6_6_KERNEL_NAME_NCU_FILTER"


def episode_files():
    files = {name: (TEMPLATE / name).read_bytes() for name in (
        "task_definition.json", "candidate_abi.json", "candidate.cu", "visible_observation.json")}
    abi = json.loads(files["candidate_abi.json"].decode("utf-8"))
    abi["compiler_contract_sha256"] = N0_COMPILER_CONTRACT_SHA256
    abi["oj_release_id"] = "N0_KERNELBENCH_L1_40_LAYERNORM_R6_6_KERNEL_NAME_NCU_FILTER"
    abi["oj_manifest_sha256"] = sha256_bytes((OJ_RELEASE / "RELEASE_MANIFEST.json").read_bytes()).upper()
    files["candidate_abi.json"] = (json.dumps(abi, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
    return files


def make_receipt(request, verdict="VALID_MEASUREMENT"):
    return {
        "request_id": request["request_id"], "episode_id": request["episode_id"],
        "candidate_id": request["candidate_id"], "candidate_sha256": request["candidate_sha256"],
        "verdict": verdict, "score": 1.0 if verdict == "VALID_MEASUREMENT" else None,
        "measurement": {"source": "SIMULATED_OJ_ONLY"},
    }


def submit(queue, episode_id, candidate_index, seed):
    source = seed + f"\n// unique simulated candidate {candidate_index}\n"
    return queue.submit_continue(episode_id=episode_id, invocation_id=f"INV-{candidate_index:03d}",
                                 current_source=source, frozen_identity_rule=lambda src, prior: src != prior)


def settle(queue, episode_id, request, sequence):
    claimed = queue.claim_next_oj(f"OJ-SIM-{sequence}", worker_pid=10000 + sequence,
                                  worker_start_utc="2026-10-29T00:00:00Z")
    assert claimed and claimed["request_id"] == request["request_id"]
    receipt = make_receipt(request)
    queue.finish_oj(request_id=request["request_id"], worker_id=claimed["worker_id"],
                    lease_id=claimed["lease_id"], receipt=receipt)
    digest = sha256_bytes(canonical_bytes(receipt))
    consumed = queue.consume_oj_result(request["request_id"], digest)
    assert consumed == receipt
    return digest


def test_ten_workers_thirty_candidates_full_authoritative_sqlite_lifecycle(tmp_path):
    queue = N0PersistentQueue(tmp_path / "runtime", max_active_episodes=10,
                              worker_count=10, oj_gpu_active_jobs_max=1)
    files = episode_files()
    seed = files["candidate.cu"].decode("utf-8")
    episode_ids = [f"G3-SIM-E{i:02d}" for i in range(10)]
    for i, episode_id in enumerate(episode_ids):
        allocation = {"authorized": True, "group": "G3", "episode_id": episode_id,
                      "replicate_slot": f"G3-SIM-SLOT-{i:02d}"}
        queue.register_episode(episode_id=episode_id, replicate_slot=allocation["replicate_slot"],
                               treatment_id="G3", approved_allocation=allocation,
                               max_candidates=30, workspace_files=files)

    receipt_hashes = set()
    sequence = 0
    for candidate_number in range(1, 31):
        for episode_id in episode_ids:
            sequence += 1
            request = submit(queue, episode_id, candidate_number, seed)
            assert request["candidate_id"] == f"C{candidate_number:04d}"
            digest = settle(queue, episode_id, request, sequence)
            assert digest not in receipt_hashes
            receipt_hashes.add(digest)
            same = queue.submit_continue(episode_id=episode_id, invocation_id=f"INV-{candidate_number:03d}",
                                        current_source=seed + f"\n// unique simulated candidate {candidate_number}\n",
                                        frozen_identity_rule=lambda src, prior: src != prior)
            assert same["request_id"] == request["request_id"]
            snap = queue.snapshot()
            episode = next(e for e in snap["episodes"] if e["episode_id"] == episode_id)
            assert episode["candidates_used"] == candidate_number

    for episode_id in episode_ids:
        try:
            submit(queue, episode_id, 31, seed)
        except N0Error as exc:
            assert str(exc) == "CANDIDATE_BUDGET_EXHAUSTED"
        else:
            raise AssertionError("31st Candidate was accepted")
    snap = queue.snapshot()
    assert len(snap["oj_requests"]) == 300
    assert len(snap["candidate_slots"]) == 300
    assert all(e["candidates_used"] == 30 for e in snap["episodes"])
    assert all(r["state"] == "CONSUMED" for r in snap["oj_requests"])
    assert snap["queue_depth"] == 0 and snap["gpu_active_jobs"] == 0
    assert len(receipt_hashes) == 300


def test_infrastructure_hold_and_unknown_do_not_settle_budget_or_allow_new_candidate(tmp_path):
    queue = N0PersistentQueue(tmp_path / "runtime")
    files = episode_files()
    seed = files["candidate.cu"].decode("utf-8")
    episode_id = "G3-SIM-INFRA"
    allocation = {"authorized": True, "group": "G3", "episode_id": episode_id,
                  "replicate_slot": "G3-SIM-INFRA-SLOT"}
    queue.register_episode(episode_id=episode_id, replicate_slot=allocation["replicate_slot"],
                           treatment_id="G3", approved_allocation=allocation,
                           max_candidates=30, workspace_files=files)
    request = submit(queue, episode_id, 1, seed)
    claimed = queue.claim_next_oj("OJ-INFRA", worker_pid=24001, worker_start_utc="synthetic")
    infra_receipt = make_receipt(request, "OJ_INFRASTRUCTURE_HOLD_REFERENCE_CONTROL_MISMATCH")
    queue.finish_oj(request_id=request["request_id"], worker_id=claimed["worker_id"],
                    lease_id=claimed["lease_id"], receipt=infra_receipt)
    digest = sha256_bytes(canonical_bytes(infra_receipt))
    result = queue.consume_oj_result(request["request_id"], digest)
    assert result["status"] == "INFRASTRUCTURE_HOLD" and result["budget_settled"] is False
    episode = next(e for e in queue.snapshot()["episodes"] if e["episode_id"] == episode_id)
    assert episode["candidates_used"] == 0
    try:
        submit(queue, episode_id, 2, seed)
    except N0Error as exc:
        assert str(exc) == "PRIOR_CANDIDATE_RESULT_MUST_BE_RESOLVED_BEFORE_NEW_SUBMISSION"
    else:
        raise AssertionError("submission proceeded past infrastructure hold")


def test_unknown_execution_stays_unsettled_and_is_never_replayed(tmp_path):
    queue = N0PersistentQueue(tmp_path / "runtime")
    files = episode_files()
    seed = files["candidate.cu"].decode("utf-8")
    episode_id = "G3-SIM-UNKNOWN"
    allocation = {"authorized": True, "group": "G3", "episode_id": episode_id,
                  "replicate_slot": "G3-SIM-UNKNOWN-SLOT"}
    queue.register_episode(episode_id=episode_id, replicate_slot=allocation["replicate_slot"],
                           treatment_id="G3", approved_allocation=allocation,
                           max_candidates=30, workspace_files=files)
    request = submit(queue, episode_id, 1, seed)
    claimed = queue.claim_next_oj("OJ-UNKNOWN", worker_pid=24002, worker_start_utc="synthetic")
    queue.finish_oj(request_id=request["request_id"], worker_id=claimed["worker_id"],
                    lease_id=claimed["lease_id"], receipt=None)
    try:
        queue.consume_oj_result(request["request_id"], "0" * 64)
    except N0Error as exc:
        assert str(exc) == "OJ_RESULT_NOT_COMPLETED_OR_MISSING"
    else:
        raise AssertionError("unknown result was consumed")
    episode = next(e for e in queue.snapshot()["episodes"] if e["episode_id"] == episode_id)
    assert episode["candidates_used"] == 0
    assert queue.snapshot()["oj_requests"][0]["state"] == "HOLD_UNKNOWN"
    try:
        submit(queue, episode_id, 2, seed)
    except N0Error as exc:
        assert str(exc) == "PRIOR_CANDIDATE_RESULT_MUST_BE_RESOLVED_BEFORE_NEW_SUBMISSION"
    else:
        raise AssertionError("new candidate was submitted after unknown execution")


def test_free_c0000_seed_can_be_claimed_without_creating_or_charging_candidate_slot(tmp_path):
    queue = N0PersistentQueue(tmp_path / "runtime")
    files = episode_files()
    episode_id = "G3-SEED-DISPATCH"
    allocation = {"authorized": True, "group": "G3", "episode_id": episode_id,
                  "replicate_slot": "G3-SEED-DISPATCH-SLOT"}
    queue.register_episode(episode_id=episode_id, replicate_slot=allocation["replicate_slot"],
                           treatment_id="G3", approved_allocation=allocation,
                           max_candidates=30, workspace_files=files)
    request = {"request_id":"N0-OJ-G3-SEED-TEST", "episode_id":episode_id,
               "invocation_id":"FORMAL_INIT_C0000", "candidate_id":"C0000",
               "candidate_sha256":N0_SEED_SHA256.lower(), "state":"QUEUED",
               "queue_sequence":1, "candidate_source_path":"episodes/"+episode_id+"/workspace/candidate.cu"}
    db = queue._connect()
    try:
        db.execute("BEGIN IMMEDIATE")
        db.execute("UPDATE runtime_meta SET value='1' WHERE key='queue_sequence'")
        db.execute("INSERT INTO oj_requests(request_id,episode_id,invocation_id,candidate_id,candidate_sha256,state,queue_sequence,candidate_source_path,created_at,updated_at) VALUES(?,?,?,?,?,'QUEUED',?,?,?,?)",
                   (request["request_id"],episode_id,request["invocation_id"],"C0000",request["candidate_sha256"],1,
                    request["candidate_source_path"],1.0,1.0))
        db.execute("COMMIT")
    finally:
        db.close()
    claimed = queue.claim_next_oj("OJ-SEED-TEST", worker_pid=34001, worker_start_utc="synthetic")
    assert claimed and claimed["request_id"] == request["request_id"]
    receipt = make_receipt(request)
    queue.finish_oj(request_id=request["request_id"], worker_id=claimed["worker_id"],
                    lease_id=claimed["lease_id"], receipt=receipt)
    snapshot = queue.snapshot()
    assert snapshot["oj_requests"][0]["state"] == "COMPLETED"
    assert snapshot["candidate_slots"] == []
    assert snapshot["episodes"][0]["candidates_used"] == 0


def test_ten_distinct_worker_contexts_run_and_report_correct_pool_size(tmp_path):
    queue = N0PersistentQueue(tmp_path / "runtime", max_active_episodes=10,
                              worker_count=10, oj_gpu_active_jobs_max=1)
    files = episode_files()
    episode_ids = [f"G3-CTX-{i:02d}" for i in range(10)]
    for i, episode_id in enumerate(episode_ids):
        allocation = {"authorized": True, "group": "G3", "episode_id": episode_id,
                      "replicate_slot": f"G3-CTX-SLOT-{i:02d}"}
        queue.register_episode(episode_id=episode_id, replicate_slot=allocation["replicate_slot"],
                               treatment_id="G3", approved_allocation=allocation,
                               max_candidates=30, workspace_files=files)
    contexts = make_worker_contexts(queue, episode_ids)
    assert len(contexts) == 10
    assert len({c.worker_id for c in contexts}) == 10
    assert len({c.episode_id for c in contexts}) == 10
    pool = FourEpisodeWorkerPool(contexts, store=queue)

    async def one(_context):
        await asyncio.sleep(0)
        return "DIAGNOSTIC_COMPLETE"

    result = asyncio.run(pool.run_round(one))
    assert result["worker_count"] == 10
    assert result["completed_workers"] == 10
    assert result["max_simultaneous_agents"] == 10
