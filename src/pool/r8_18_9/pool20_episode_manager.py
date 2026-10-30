# -*- coding: utf-8 -*-
"""Manage independent N0 manual Episodes. Windows / PowerShell + pinned runtime.

Commands:
  python manual_episode_manager.py status N0_MANUAL_01
  python manual_episode_manager.py resume N0_MANUAL_01 --until C0030
  python manual_episode_manager.py new N0_MANUAL_02
  python manual_episode_manager.py resume N0_MANUAL_02 --until C0030

A new Episode has an isolated workspace, history, SQLite, results and logs.
Its own C0000 seed is measured before the first DeepSeek request.  The
registered current Episode is NEVER cloned as a running-state snapshot.
No formal queue, worker, or DB is modified.
"""
from __future__ import annotations

import argparse
import ast
import hashlib
import importlib.util
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(r"D:\projects\kernelagent")
R = ROOT / "runtime"
H = Path(r"D:\projects\kernelagent\runtime\harness\releases\N0_AUTONOMOUS_INFORMATION_ACQUISITION_R7_3_POOL20_R5_FILELIST_FIX")
RELEASE_DIR = Path(__file__).resolve().parent
PINNED_PYTHON = R / ".venv_n0_r6/Scripts/python.exe"
EXPECTED_TASK_SHA256 = "8D975D7CEE6946513DCFB6BA84C971E53A890F9D9C6F0A4FB828904941F25975"
EXPECTED_TASK_DEFINITION_SHA256 = "3F0EB778C8F6673F1B6589060742CAF56FD21AB57A9DA48C9DECFED79E198709"
TASK_DEFINITION_SOURCE = H / "tests/fixtures/N0_AUTONOMOUS_INFORMATION_ACQUISITION_R4/template/task_definition.json"
SEED_SHA = "cbd7e93b47ad3957e4e133bde45c0af1e933dd9bec3525c27637049337fb2d91"
C0002_TEMPLATE_SHA = "62041245d5948b54def5020a2774b24a4367e3c48bb171799a0aa569e304894b"
ID_PATTERN = re.compile(r"^N0_MANUAL_([0-9]{2,10})$")
ORIGINAL_EPISODE = "N0_MANUAL_200001"
ORIGINAL_DIR = ROOT / "manual_episode_200001"
MANAGER_SCHEMA = "N0_POOL20_SHARED_OJ_EPISODE_V1"


def fail_if(value, why):
    if value:
        raise RuntimeError(why)


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def digest(path: Path) -> str:
    return sha(path.read_bytes())


def jd(value) -> str:
    return json.dumps(value, indent=2, ensure_ascii=False, sort_keys=True) + "\n"


def load(path: Path):
    return json.loads(path.read_text(encoding="utf-8-sig"))


def write_once(path: Path, value) -> None:
    data = value if isinstance(value, bytes) else value.encode("utf-8")
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        fail_if(path.read_bytes() != data, "IMMUTABLE_FILE_CONFLICT:" + str(path))
        return
    with path.open("xb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())


def eid_dir(eid: str) -> Path:
    m = ID_PATTERN.fullmatch(eid)
    fail_if(m is None, "BAD_EPISODE_ID: expected N0_MANUAL_02 or N0_MANUAL_003")
    number = int(m.group(1))
    # Do not let alternate textual episode IDs resolve to the same folder.
    fail_if(eid != f"N0_MANUAL_{number:0{len(m.group(1))}d}", "NONCANONICAL_ID")
    root = Path(os.environ.get("N0_POOL20_EPISODE_ROOT", str(ROOT))).resolve()
    return root / ("manual_episode_" + m.group(1))


def verify_task_definition(path: Path) -> dict:
    raw = path.read_bytes()
    obj = json.loads(raw.decode("utf-8-sig"))
    raw_sha = sha(raw).upper()
    task_sha = str(obj.get("task_sha256", "")).upper()
    fail_if(task_sha != EXPECTED_TASK_SHA256, "TASK_DEFINITION_INTERNAL_SHA_MISMATCH:" + str(path))
    fail_if(raw_sha != EXPECTED_TASK_DEFINITION_SHA256, "TASK_DEFINITION_FILE_SHA_MISMATCH:" + str(path))
    return {"path": str(path), "task_sha256": task_sha, "task_definition_sha256": raw_sha}


def utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def replace_function(source: str, name: str, replacement: str) -> str:
    module = ast.parse(source)
    matches = [node for node in module.body
               if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name]
    fail_if(len(matches) != 1, "FUNCTION_NOT_UNIQUE:" + name)
    node = matches[0]
    lines = source.splitlines(keepends=True)
    prefix = "".join(lines[:node.lineno - 1])
    suffix = "".join(lines[node.end_lineno:])
    return prefix + replacement.rstrip() + "\n\n" + suffix


GENERALIZED_HISTORY = '''def baseline_and_history(ids):
    # Strict Episode-scoped local records.  Never import another Episode's history.
    candidates = []
    for cid in ids:
        path = RESULTS / f"{cid.lower()}_manual_history_record.json"
        need(path.is_file(), "HISTORY_MISSING:" + cid)
        rec = load(path)
        need(rec.get("candidate_id") == cid, "HISTORY_CANDIDATE_MISMATCH")
        need(sha(rec["source"].encode("utf-8")) == rec["source_sha256"],
             "HISTORY_SOURCE_HASH_MISMATCH:" + cid)
        candidates.append(rec)
    return candidates
'''

GENERALIZED_DB = '''def init_db():
    need(DB.is_file(), "EPISODE_SQLITE_NOT_BOOTSTRAPPED")
    con = sqlite3.connect(str(DB), timeout=30, isolation_level=None)
    con.execute("PRAGMA busy_timeout=30000")
    con.execute("""CREATE TABLE IF NOT EXISTS candidate_outcomes (
        episode_id TEXT NOT NULL, candidate_id TEXT NOT NULL,
        source_sha256 TEXT NOT NULL, verdict TEXT NOT NULL, score REAL,
        promotion TEXT NOT NULL, result_file TEXT NOT NULL,
        result_sha256 TEXT NOT NULL, history_file TEXT NOT NULL,
        history_sha256 TEXT NOT NULL, settled_at_utc TEXT NOT NULL,
        PRIMARY KEY (episode_id, candidate_id))""")
    con.execute("""CREATE TABLE IF NOT EXISTS candidate_settlements (
        episode_id TEXT NOT NULL, candidate_id TEXT NOT NULL,
        source_sha256 TEXT NOT NULL, status TEXT NOT NULL, score REAL NOT NULL,
        promotion TEXT NOT NULL, result_path TEXT NOT NULL, result_sha256 TEXT NOT NULL,
        candidate_ms REAL NOT NULL, reference_ms REAL NOT NULL, history_json TEXT NOT NULL,
        PRIMARY KEY (episode_id, candidate_id))""")
    con.execute("""CREATE TABLE IF NOT EXISTS evaluation_attempts (
        episode_id TEXT NOT NULL, candidate_id TEXT NOT NULL, attempt_index INTEGER NOT NULL,
        status TEXT NOT NULL, result_path TEXT NOT NULL, sha256 TEXT NOT NULL,
        PRIMARY KEY(episode_id,candidate_id,attempt_index))""")
    ids = settled_ids(con)
    need(ids and ids[0] == "C0000", "BASELINE_NOT_SETTLED")
    for cid in ids:
        row = con.execute("SELECT source_sha256,result_file,result_sha256,history_file,history_sha256 "
                          "FROM candidate_outcomes WHERE episode_id=? AND candidate_id=?",
                          (EPISODE,cid)).fetchone()
        source_sha, result, result_sha, history, history_sha = row
        need(Path(result).is_file() and file_sha(result)==result_sha, "RESULT_HASH_MISMATCH:"+cid)
        need(Path(history).is_file() and file_sha(history)==history_sha, "HISTORY_HASH_MISMATCH:"+cid)
        need(load(history)["source_sha256"]==source_sha, "LEDGER_SOURCE_MISMATCH:"+cid)
    return con
'''

GENERALIZED_PREFLIGHT = '''def preflight():
    need(M.is_dir() and H.is_dir() and P.is_file(), "MISSING_RUNTIME_OR_VENV")
    need(DB.is_file(), "BASELINE_DB_MISSING: resume with episode manager")
    need((RESULTS / "c0000_result.json").is_file(), "BASELINE_RESULT_MISSING")
    need((RESULTS / "c0000_manual_history_record.json").is_file(), "BASELINE_HISTORY_MISSING")
    need((LOGS / "c0000_bootstrap.json").is_file(), "BASELINE_BOOTSTRAP_MISSING")
    need((M / "manual_agent_until_submit.py").is_file(), "AGENT_TEMPLATE_MISSING")
    need(file_sha(M / "manual_agent_until_submit.py") == RUNNER_TEMPLATE_SHA,
         "AGENT_TEMPLATE_CHANGED")
    need(BASE_OJ.is_file(), "EVALUATOR_TEMPLATE_MISSING")
    evaluator = BASE_OJ.read_text(encoding="utf-8-sig")
    for marker in ("C0002_pending.cu", "c0002_result.json", "c0002_reference.json", C0002_SHA.upper()):
        need(marker in evaluator, "EVALUATOR_TEMPLATE_INCOMPATIBLE:"+marker)
    need((M / "oj_local/n0_kernelbench_oj/adapter.py").is_file(), "MANUAL_OJ_MISSING")
    need((M / "tools/bin/ninja.exe").is_file(), "NINJA_MISSING")
    need((R / "state/n0_gpu_device.lock").parent.is_dir(), "SHARED_GPU_LOCK_PARENT_MISSING")
    with sqlite3.connect(str(DB)) as con:
        ids = settled_ids(con)
        leader = best_record(con)
    print(f"=== {EPISODE} PREFLIGHT PASS: settled={len(ids)-1}/30 best={leader} ===", flush=True)
    print("Formal queue, database and Worker are not used by this Episode.", flush=True)
'''

POOL20_AGENT_HISTORY = '''def manual_history():
    paths = sorted(RESULTS.glob("*_manual_history_record.json"),
                   key=lambda p: int(p.name.split("_", 1)[0][1:]))
    if not paths:
        raise RuntimeError("POOL20_HARNESS_HISTORY_EMPTY")
    records = []
    for path in paths:
        record = read_json(path)
        if not isinstance(record.get("oj_receipt"), dict):
            raise RuntimeError("POOL20_HISTORY_RECORD_MISSING_OFFICIAL_RECEIPT:" + path.name)
        receipt = record["oj_receipt"]
        if receipt.get("episode_id") != EPISODE or receipt.get("candidate_sha256") != record.get("source_sha256"):
            raise RuntimeError("POOL20_HISTORY_RECEIPT_IDENTITY_MISMATCH:" + path.name)
        records.append(record)
    whitelist = read_json(R / "integrations/N0_PRODUCTION_INTEGRATION_R12_10_4_R9_6_R8_03_NATURAL_EXIT_HANDOFF_20261027/N0_PROFILE_WHITELIST_NONSCIENTIFIC_V1.json")
    return HistoryService(EPISODE, records, profile_whitelist=whitelist)
'''


def rewrite_paths(source: str, new_path: Path) -> str:
    # Script literals are Windows raw strings; preserve backslashes for Python.
    old = str(ORIGINAL_DIR)
    target = str(new_path)
    fail_if(old not in source, "SCRIPT_MISSING_OLD_PATH")
    return source.replace(old, target)


def pool_gpu_queue_runner(source: str) -> str:
    """Validate the shared OJ client path; the OJ service owns the GPU Lease."""
    if "from pool20_shared_oj import evaluate_candidate as shared_evaluate" not in source:
        raise RuntimeError("RUNNER_SHARED_OJ_CLIENT_MISSING")
    if "N0_KERNEL_AGENT_POOL20_R3" in source or "N0_KERNEL_AGENT_POOL20_R6_" in source:
        raise RuntimeError("RUNNER_UNAUTHORIZED_LEGACY_RELEASE_PATH")
    tree = ast.parse(source)
    raw = next((x for x in tree.body if isinstance(x, ast.FunctionDef) and x.name == "run_subprocess"), None)
    if raw is None or any(isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and
                          n.func.id == "run_subprocess" for n in ast.walk(raw)):
        raise RuntimeError("RUNNER_SUBPROCESS_RECURSION_OR_MISSING")
    return source


def stage_new(eid: str) -> Path:
    dst = eid_dir(eid)
    fail_if(dst == ORIGINAL_DIR, "CANNOT_RECREATE_EXISTING_EPISODE_01")
    fail_if(dst.exists(), "EPISODE_ALREADY_EXISTS: use resume " + eid)
    src = ORIGINAL_DIR
    required = ["manual_episode_auto.py", "manual_agent_until_submit.py",
                "manual_seed_eval.py", "manual_c0002_eval.py", "manual_advance_c0002.py",
                "oj_local/n0_kernelbench_oj/adapter.py", "tools/bin/ninja.exe",
                "workspace/candidate_abi.json"]
    for relative in required:
        fail_if(not (src / relative).is_file(), "SOURCE_TEMPLATE_MISSING:" + relative)
    fail_if(not H.is_dir() or not PINNED_PYTHON.is_file(), "RUNTIME_NOT_INSTALLED")
    fail_if(not (H / "protocol/SEED_CANDIDATE.cu").is_file(), "SEED_NOT_FOUND")
    task_identity = verify_task_definition(TASK_DEFINITION_SOURCE)
    seed = (H / "protocol/SEED_CANDIDATE.cu").read_bytes()
    fail_if(sha(seed) != SEED_SHA, "FROZEN_SEED_SHA256_MISMATCH")
    fail_if(sha((src / "manual_agent_until_submit.py").read_bytes()) !=
            "5163e56d8867a9156a638edd5aa9e6174e8dde2b46964916d2cf68e9b929f526",
            "AGENT_TEMPLATE_IDENTITY_CHANGED")
    # Work in a staging directory; all script paths refer to the eventual final destination.
    tmp = dst.with_name("." + dst.name + ".stage_" + os.urandom(5).hex())
    tmp.mkdir(parents=False, exist_ok=False)
    try:
        for folder in ["logs", "results", "state", "workspace", "tools/bin"]:
            (tmp / folder).mkdir(parents=True, exist_ok=True)
        shutil.copy2(src / "tools/bin/ninja.exe", tmp / "tools/bin/ninja.exe")
        shutil.copytree(src / "oj_local", tmp / "oj_local",
                        ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "*.pyd", "*.obj", "*.o"))
        # Absolute path in manually patched adapter must point into this new Episode.
        for pyfile in (tmp / "oj_local").rglob("*.py"):
            raw = pyfile.read_text(encoding="utf-8-sig")
            if str(ORIGINAL_DIR) in raw:
                pyfile.write_text(raw.replace(str(ORIGINAL_DIR), str(dst)), encoding="utf-8")
        files = ["manual_agent_until_submit.py", "manual_advance_c0002.py",
                 "manual_seed_eval.py", "manual_c0002_eval.py"]
        for filename in files:
            source = (src / filename).read_text(encoding="utf-8-sig")
            output = rewrite_paths(source, dst)
            if filename == "manual_agent_until_submit.py":
                output = output.replace("N0_AUTONOMOUS_INFORMATION_ACQUISITION_R6_1_FORMAL_V1", "N0_AUTONOMOUS_INFORMATION_ACQUISITION_R7_3_POOL20_R5_FILELIST_FIX")
                output = replace_function(output, "manual_history", POOL20_AGENT_HISTORY)
            if filename != "manual_seed_eval.py" and filename != "manual_c0002_eval.py":
                output = output.replace(ORIGINAL_EPISODE, eid)
            compile(output, filename, "exec")
            (tmp / filename).write_text(output, encoding="utf-8")
        # Clone the CURRENTLY deployed manual loop logic, including current fault fixes.
        # Generalize only baseline bootstrap and Episode identity, not tool behavior.
        template_sha = digest(tmp / "manual_agent_until_submit.py")
        runner = rewrite_paths((RELEASE_DIR / "pool20_runner_template.py").read_text(encoding="utf-8-sig"), dst)
        runner = runner.replace(ORIGINAL_EPISODE, eid)
        runner = re.sub(r'(?m)^RUNNER_TEMPLATE_SHA = "[0-9a-f]{64}"$',
                        f'RUNNER_TEMPLATE_SHA = "{template_sha}"', runner, count=1)
        runner = replace_function(runner, "init_db", GENERALIZED_DB)
        runner = replace_function(runner, "preflight", GENERALIZED_PREFLIGHT)
        runner = replace_function(runner, "baseline_and_history", GENERALIZED_HISTORY)
        runner = pool_gpu_queue_runner(runner)
        compile(runner, "manual_episode_auto.py", "exec")
        (tmp / "manual_episode_auto.py").write_text(runner, encoding="utf-8")
        # Do not clone the old Episode's state, prompt, candidate edits or SQLite.
        shutil.copy2(TASK_DEFINITION_SOURCE, tmp / "workspace/task_definition.json")
        shutil.copy2(src / "workspace/candidate_abi.json", tmp / "workspace/candidate_abi.json")
        verify_task_definition(tmp / "workspace/task_definition.json")
        write_once(tmp / "workspace/candidate.cu", seed)
        report = {
            "schema": MANAGER_SCHEMA,
            "episode_id": eid,
            "episode_dir": str(dst),
            "created_utc": utc(),
            "status": "NEEDS_INDEPENDENT_C0000_BASELINE",
            "seed_sha256": SEED_SHA,
            "parent_template": str(ORIGINAL_DIR),
            "parent_template_runner_sha256": digest(src / "manual_episode_auto.py"),
            "runtime_runner_sha256": digest(tmp / "manual_episode_auto.py"),
            "agent_loop_sha256": template_sha,
            "oj_release_id": "N0_KERNELBENCH_L1_40_LAYERNORM_R6_5_FAULT_CONTAINED",
            "integration_release_id": "N0_PRODUCTION_INTEGRATION_R12_10_4_R9_6_R8_03_NATURAL_EXIT_HANDOFF_20261027",
            "knowledge_release_id": "N0_KB_RETRIEVAL_V2_TARGETED_R5_SEMANTIC_FIX_20261024",
            "frozen_harness_release": H.name,
            "task_sha256": task_identity["task_sha256"],
            "task_definition_sha256": task_identity["task_definition_sha256"],
    "episode_manager_release": "N0_KERNEL_AGENT_POOL20_R8_18_R8_03_RESUME_REMAINING_PATH_FIX_20261027",
            "oj_manifest_sha256": "8894FF6B14F7D76D7A4FC7B2033E76DE55894EA0EA7DDBBC2A3B0F7FB010A22C",
            "integration_manifest_sha256": "AE3F53C1A59A431B8C340AA7F755A525C3D13EE1865C4658AD83F9E96AB88BF0",
            "harness_manifest_sha256": "0E7D76E6625557EA02D4A79B09EB6F7BEDCC0DB5B536D470DC148433E57055DA",
            "knowledge_manifest_sha256": "3326A78555518874FF22F3E8637E20AD2600547B1A6FD8D766D3DAD96F14FAA3",
            "treatment_id": "N0_AUTONOMOUS",
            "episode_limit": 5,
            "no_previous_episode_history_imported": True,
            "local_oj_scoring": False,
            "shared_oj_queue": "run-scoped/n0_oj_queue",
            "run_id": os.environ.get("N0_POOL20_RUN_ID"),
        }
        write_once(tmp / "episode_manager_manifest.json", jd(report))
        fail_if(dst.exists(), "EPISODE_PATH_ALREADY_EXISTS")
        tmp.rename(dst)
    except Exception:
        shutil.rmtree(tmp, ignore_errors=True)
        raise
    print("=== NEW EPISODE STAGED ===")
    print("Episode:", eid)
    print("Root:", dst)
    print("C0000 common Seed SHA256:", SEED_SHA)
    print("History: EMPTY (other Episodes not imported)")
    print("Shared OJ queue baseline bootstrap is recorded for", eid)
    print("Local Episode evaluator is not used for scoring.")
    return dst


def spawn(cmd, cwd: Path, env: dict | None = None, *, log: Path | None = None) -> int:
    final_env = os.environ.copy()
    if env:
        final_env.update(env)
    if log is None:
        return subprocess.call([str(x) for x in cmd], cwd=str(cwd), env=final_env)
    with log.open("a", encoding="utf-8") as f:
        f.write("\n=== " + utc() + " START " + " ".join(map(str, cmd)) + " ===\n")
        f.flush()
        with subprocess.Popen([str(x) for x in cmd], cwd=str(cwd), env=final_env,
                              stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                              text=True, encoding="utf-8", errors="replace", bufsize=1) as process:
            assert process.stdout is not None
            for line in process.stdout:
                print(line, end="", flush=True)
                f.write(line)
                f.flush()
            return process.wait()


def seed_baseline(dst: Path, max_attempts: int) -> Path:
    import sys
    sys.path.insert(0, str(RELEASE_DIR))
    from pool20_shared_oj import evaluate_seed
    return evaluate_seed(dst.name.replace("manual_episode_", "N0_MANUAL_"), dst / "workspace", dst / "results")



def bootstrap_new(dst: Path, eid: str, attempts: int) -> None:
    results = dst / "results"
    marker = dst / "episode_manager_manifest.json"
    fail_if(not marker.is_file(), "EPISODE_MANAGER_MANIFEST_MISSING")
    meta = load(marker)
    fail_if(meta.get("episode_id") != eid or meta.get("schema") != MANAGER_SCHEMA,
            "EPISODE_IDENTITY_CONFLICT")
    verify_task_definition(dst / "workspace/task_definition.json")
    # Subsequent resumption must NOT rewrite the visible observation or reset
    # candidate.cu: the Agent may already have edited both after C0000.
    bootstrap_marker = dst / "logs/c0000_bootstrap.json"
    baseline_db = dst / "state/manual_episode.sqlite3"
    if bootstrap_marker.is_file() and baseline_db.is_file():
        with sqlite3.connect(baseline_db.resolve().as_uri() + "?mode=ro", uri=True) as check_db:
            row = check_db.execute(
                "SELECT source_sha256,result_file,result_sha256,history_file,history_sha256 "
                "FROM candidate_outcomes WHERE episode_id=? AND candidate_id='C0000'",
                (eid,)).fetchone()
        if row is not None:
            source_sha, result_file, result_sha, history_file, history_sha = row
            fail_if(source_sha != SEED_SHA or not Path(result_file).is_file() or
                    digest(Path(result_file)) != result_sha or
                    not Path(history_file).is_file() or digest(Path(history_file)) != history_sha,
                    "ESTABLISHED_BASELINE_INTEGRITY_FAILURE")
            print(f"=== {eid} BASELINE ALREADY INITIALIZED: preserving workspace and history ===",flush=True)
            return
    result_path = seed_baseline(dst, attempts)
    result = load(result_path)
    score = float(result["timing"]["kernelbench_speedup"])
    fail_if(not result["official_kernelbench_result"].get("compiled") or
            not result["official_kernelbench_result"].get("correctness"),
            "C0000_INVALID_TRUSTED_RESULT")
    sys.path.insert(0, str(dst))
    try:
        # The shared helper produces validated local records, not formal signed receipts.
        module_name = "manual_advance_c0002_" + eid
        spec = importlib.util.spec_from_file_location(module_name, dst / "manual_advance_c0002.py")
        assert spec and spec.loader
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        from n0_autonomous.engine import HistoryService
        seed_bytes = (dst / "workspace/candidate.cu").read_bytes()
        envelope_path = results / "c0000_shared_oj_envelope.json"
        if envelope_path.is_file():
            seed_record = load(envelope_path)["candidate_record"]
        else:
            raise RuntimeError("POOL20_C0000_SHARED_OJ_RECEIPT_MISSING")
        # A legacy local request-id pattern is used by the helper, but isolation
        # is maintained by Episode-scoped database keys and directories.
        HistoryService(eid, [seed_record], profile_whitelist=module.load(
            R / "integrations/N0_PRODUCTION_INTEGRATION_R12_10_4_R9_6_R8_03_NATURAL_EXIT_HANDOFF_20261027/N0_PROFILE_WHITELIST_NONSCIENTIFIC_V1.json"
        )).snapshot_sha256()
    finally:
        sys.path.remove(str(dst))
    results = dst / "results"
    write_once(results / "c0000_manual_history_record.json", jd(seed_record))
    bootstrap = {"episode_id":eid, "candidate_id":"C0000", "source_sha256":SEED_SHA,
                 "verdict":"VALID_MEASUREMENT", "score":score, "promotion":"SEED_BASELINE",
                 "result_file":str(result_path), "result_sha256":digest(result_path),
                 "baseline_created_utc":utc(), "model_attempts":0,
                 "note":"CONTROLLER_BOOTSTRAP_NOT_AN_AGENT_SUBMISSION"}
    # Bootstrap evidence must be stable on resume. Timestamp is fixed after first creation.
    marker_file = dst / "logs/c0000_bootstrap.json"
    if marker_file.exists():
        previous = load(marker_file)
        fail_if(any(previous.get(k) != bootstrap.get(k) for k in
                    ("episode_id","candidate_id","source_sha256","score","result_sha256")),
                "BOOTSTRAP_CONFLICT")
    else:
        write_once(marker_file, jd(bootstrap))
    decision = {"episode_id":eid,"candidate_id":"C0000", "decision":"CONTINUE",
                "reason":"Controller baseline bootstrap, not a DeepSeek decision",
                "working_note":None,"candidate_source_sha256":SEED_SHA,
                "origin":"CONTROLLER_SEED_BOOTSTRAP"}
    write_once(results / "c0000_manual_decision.json", jd(decision))
    # The Episode's initial observation represents the measured C0000 seed.
    envelope_path = results / "c0000_shared_oj_envelope.json"
    if not envelope_path.is_file(): raise RuntimeError("POOL20_C0000_SHARED_OJ_ENVELOPE_MISSING")
    observation = load(envelope_path)["agent_observation"]
    visible_path = dst / "workspace/visible_observation.json"
    if visible_path.is_file():
        fail_if(load(visible_path) != observation, "POOL20_C0000_VISIBLE_OBSERVATION_CONFLICT")
    else:
        write_once(visible_path, jd(observation))
    db = dst / "state/manual_episode.sqlite3"
    con = sqlite3.connect(str(db), timeout=30)
    try:
        con.execute("PRAGMA busy_timeout=30000")
        con.executescript('''CREATE TABLE IF NOT EXISTS candidate_outcomes (
            episode_id TEXT NOT NULL, candidate_id TEXT NOT NULL,
            source_sha256 TEXT NOT NULL, verdict TEXT NOT NULL, score REAL,
            promotion TEXT NOT NULL, result_file TEXT NOT NULL,
            result_sha256 TEXT NOT NULL, history_file TEXT NOT NULL,
            history_sha256 TEXT NOT NULL, settled_at_utc TEXT NOT NULL,
            PRIMARY KEY (episode_id,candidate_id));
          CREATE TABLE IF NOT EXISTS candidate_settlements (
            episode_id TEXT NOT NULL, candidate_id TEXT NOT NULL,
            source_sha256 TEXT NOT NULL,status TEXT NOT NULL,score REAL NOT NULL,
            promotion TEXT NOT NULL,result_path TEXT NOT NULL,result_sha256 TEXT NOT NULL,
            candidate_ms REAL NOT NULL,reference_ms REAL NOT NULL,history_json TEXT NOT NULL,
            PRIMARY KEY (episode_id,candidate_id));
          CREATE TABLE IF NOT EXISTS evaluation_attempts (
            episode_id TEXT NOT NULL,candidate_id TEXT NOT NULL,attempt_index INTEGER NOT NULL,
            status TEXT NOT NULL,result_path TEXT NOT NULL,sha256 TEXT NOT NULL,
            PRIMARY KEY (episode_id,candidate_id,attempt_index));''')
        history = results / "c0000_manual_history_record.json"
        old = con.execute("SELECT source_sha256,score,result_sha256,history_sha256 FROM candidate_outcomes "
                          "WHERE episode_id=? AND candidate_id='C0000'", (eid,)).fetchone()
        values = (SEED_SHA,score,digest(result_path),digest(history))
        if old:
            fail_if(tuple(old) != values, "BASELINE_DB_CONFLICT")
        else:
            con.execute("INSERT INTO candidate_outcomes VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                        (eid,"C0000",SEED_SHA,"VALID_MEASUREMENT",score,"SEED_BASELINE",
                         str(result_path),digest(result_path),str(history),digest(history),
                         load(marker_file)["baseline_created_utc"]))
        old_score = con.execute("SELECT score,result_sha256 FROM candidate_settlements "
                                "WHERE episode_id=? AND candidate_id='C0000'", (eid,)).fetchone()
        if old_score:
            fail_if(tuple(old_score) != (score,digest(result_path)), "BASELINE_SCORE_DB_CONFLICT")
        else:
            con.execute("INSERT INTO candidate_settlements VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                        (eid,"C0000",SEED_SHA,"VALID_MEASUREMENT",score,"SEED_BASELINE",
                         str(result_path),digest(result_path),
                         float(result["timing"]["candidate_runtime_ms"]),
                         float(result["timing"]["kernelbench_reference_runtime_ms"]),jd(seed_record)))
        con.commit()
    finally:
        con.close()
    print(f"=== {eid} INITIALIZED: independent C0000 speedup={score:.6f}x, budget=0/30 ===",flush=True)


def status_episode(eid: str):
    dst = eid_dir(eid)
    fail_if(not dst.is_dir(), "EPISODE_NOT_FOUND:" + str(dst))
    print("=== EPISODE STATUS ===")
    print("Episode:", eid)
    print("Directory:", dst)
    if (dst / "episode_manager_manifest.json").exists():
        print("Manager manifest:", digest(dst / "episode_manager_manifest.json"))
    db = dst / "state/manual_episode.sqlite3"
    last_settled = 0
    if db.is_file():
        with sqlite3.connect(db.resolve().as_uri() + "?mode=ro",uri=True) as con:
            tables = {x[0] for x in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if "candidate_outcomes" in tables:
                rows=con.execute("SELECT candidate_id,verdict,score FROM candidate_outcomes WHERE episode_id=? "
                                 "ORDER BY candidate_id",(eid,)).fetchall()
            else:
                rows=con.execute("SELECT candidate_id,status,score FROM candidate_settlements "
                                 "WHERE episode_id=? ORDER BY candidate_id",(eid,)).fetchall()
            if rows:
                last_settled=int(rows[-1][0][1:]); print("Settled:",last_settled,"/30")
                best = max((r for r in rows if r[2] is not None), key=lambda r:float(r[2]), default=None)
                print("Best:", best)
                print("Latest:", rows[-1])
            else:
                print("Settled: none")
    else:
        print("Database: absent; independent C0000 not yet qualified")
    next_cid=f"C{last_settled+1:04d}"
    decision = dst / "results" / f"{next_cid.lower()}_manual_decision.json"
    state = dst / "logs" / f"manual_agent_loop_{next_cid.lower()}_state.json"
    print("Next candidate:",next_cid)
    if state.exists():
        s=load(state)
        print("Invocation:", s.get("phase"), "turn",s.get("turn"), "request",s.get("request_file"))
        if s.get("phase") == "DISPATCHING":
            print("Last HTTP request may have completed remotely; recovery checks saved response first.")
    if decision.exists():
        print("Model decision already on disk:", load(decision).get("decision"))
    if (dst / "state/manual_full_episode.lock").exists():
        print("WARNING: Episode lock file exists; inspect process before resuming")
    if (dst / "state/manual_episode_agent_stop.json").exists():
        print("STOP marker present; resume will not start another Candidate")


def prepare_resume_runner(dst: Path, run_id: str) -> Path:
    """Create a release-pinned execution overlay while preserving Episode bytes."""
    source_path = dst / "manual_episode_auto.py"
    meta_path = dst / "episode_manager_manifest.json"
    fail_if(not source_path.is_file() or not meta_path.is_file(), "RESUME_RUNNER_EVIDENCE_MISSING")
    original = source_path.read_bytes()
    meta = load(meta_path)
    original_sha = sha(original)
    fail_if(meta.get("runtime_runner_sha256") != original_sha, "RESUME_RUNNER_SOURCE_IDENTITY_MISMATCH")
    source = original.decode("utf-8-sig")
    tree = ast.parse(source)
    raw_defs = [x for x in tree.body if isinstance(x, ast.FunctionDef) and x.name == "_pool20_raw_run_subprocess"]
    normalize_legacy_wrapper = False
    if len(raw_defs) == 2:
        recursive = [x for x in raw_defs if any(
            isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id == "_pool20_raw_run_subprocess"
            for n in ast.walk(x))]
        fail_if(len(recursive) != 1, "RESUME_RUNNER_RECURSION_SHAPE_UNRECOGNIZED")
        bad = recursive[0]
        lines = source.splitlines(keepends=True)
        del lines[bad.lineno - 1:bad.end_lineno]
        source = "".join(lines)
        normalize_legacy_wrapper = True
    elif len(raw_defs) == 1:
        raw = raw_defs[0]
        fail_if(any(isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and
                    n.func.id == "_pool20_raw_run_subprocess" for n in ast.walk(raw)),
                "RESUME_RUNNER_SINGLE_RAW_FUNCTION_RECURSIVE")
        normalize_legacy_wrapper = True
    elif len(raw_defs) != 0:
        raise RuntimeError("RESUME_RUNNER_RAW_FUNCTION_COUNT_UNSUPPORTED:" + str(len(raw_defs)))
    tree = ast.parse(source)
    run_defs = [x for x in tree.body if isinstance(x, ast.FunctionDef) and x.name == "run_subprocess"]
    fail_if(len(run_defs) != 1, "RESUME_RUNNER_DISPATCH_FUNCTION_COUNT_UNSUPPORTED")
    if normalize_legacy_wrapper:
        dispatch = run_defs[0]
        lines = source.splitlines(keepends=True)
        direct_dispatch = [
            "def run_subprocess(cmd, *, log=None, env=None):\n",
            "    return _pool20_raw_run_subprocess(cmd, log=log, env=env)\n",
        ]
        lines[dispatch.lineno - 1:dispatch.end_lineno] = direct_dispatch
        source = "".join(lines)
    else:
        dispatch = run_defs[0]
        self_calls = [n for n in ast.walk(dispatch) if isinstance(n, ast.Call) and
                      isinstance(n.func, ast.Name) and n.func.id == "run_subprocess"]
        fail_if(self_calls or not any(isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and
                    isinstance(n.func.value, ast.Name) and n.func.value.id == "subprocess" and
                    n.func.attr in {"Popen", "call"} for n in ast.walk(dispatch)),
                "RESUME_RUNNER_CLEAN_SUBPROCESS_FUNCTION_INVALID")
    old_shared = r"D:\projects\kernelagent\runtime\deployment\releases\N0_KERNEL_AGENT_POOL20_R3"
    source = source.replace(old_shared, str(RELEASE_DIR))
    source = source.replace("N0_PRODUCTION_INTEGRATION_R5", "N0_PRODUCTION_INTEGRATION_R12_10_4_R9_6_R8_03_NATURAL_EXIT_HANDOFF_20261027")
    source = source.replace("N0_PRODUCTION_INTEGRATION_R12_9_POOL20_R5_R7_3_HARNESS",
                            "N0_PRODUCTION_INTEGRATION_R12_10_4_R9_6_R8_03_NATURAL_EXIT_HANDOFF_20261027")
    source = replace_function(source, "evaluate_candidate", f'''def evaluate_candidate(cid,args):
    import sys
    sys.path.insert(0, r"{str(RELEASE_DIR)}")
    from pool20_shared_oj import evaluate_candidate as shared_evaluate
    source_path=RESULTS/f"{{cid}}_pending.cu"
    result_path,result=shared_evaluate(EPISODE,cid,source_path,WORK,RESULTS)
    return result_path,[result_path]
''')
    episode_root = dst.resolve()
    source, count = re.subn(r"(?m)^M\s*=\s*Path\(r?['\"][^'\"]*manual_episode_[0-9]+['\"]\)\s*$",
                            lambda _: "M = Path(" + json.dumps(str(episode_root)) + ")", source, count=1)
    fail_if(count != 1, "RESUME_RUNNER_EPISODE_ROOT_BINDING_MISSING")
    tree = ast.parse(source)
    raw = next((x for x in tree.body if isinstance(x, ast.FunctionDef) and x.name == "_pool20_raw_run_subprocess"), None)
    recursive_left = raw and any(isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and
        n.func.id == "_pool20_raw_run_subprocess" for n in ast.walk(raw))
    if raw is not None:
        fail_if(recursive_left, "RESUME_RUNNER_RECURSION_REMAINS")
        dispatch = next((x for x in tree.body if isinstance(x, ast.FunctionDef) and x.name == "run_subprocess"), None)
        fail_if(dispatch is None or any(isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and
                    n.func.id == "run_subprocess" for n in ast.walk(dispatch)),
                "RESUME_RUNNER_DISPATCH_RECURSION_REMAINS")
    else:
        dispatch = next((x for x in tree.body if isinstance(x, ast.FunctionDef) and x.name == "run_subprocess"), None)
        fail_if(dispatch is None or any(isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and
                    n.func.id == "run_subprocess" for n in ast.walk(dispatch)),
                "RESUME_RUNNER_CLEAN_PATH_VALIDATION_FAILED")
    fail_if("N0_KERNEL_AGENT_POOL20_R3" in source or "N0_KERNEL_AGENT_POOL20_R6_" in source or "gpu_gate" in source,
            "RESUME_RUNNER_UNAUTHORIZED_LEGACY_RELEASE_PATH")
    compile(source, str(source_path), "exec")
    run_root = Path(os.environ.get("N0_POOL20_RUN_ROOT", str(R / "state/n0_pool20_runs_r10" / run_id))).resolve()
    overlay_root = run_root / "runner_overlays" / dst.name
    overlay_root.mkdir(parents=True, exist_ok=True)
    overlay_bytes = source.encode("utf-8")
    overlay_sha = sha(overlay_bytes)
    overlay_path = overlay_root / ("manual_episode_auto_" + overlay_sha[:16] + ".py")
    write_once(overlay_path, overlay_bytes)
    receipt = {"schema":"N0_POOL20_RUNNER_OVERLAY_V1","run_id":run_id,
        "episode_id":dst.name.replace("manual_episode_","N0_MANUAL_"),
        "source_path":str(source_path),"source_sha256":original_sha,"overlay_path":str(overlay_path),
        "overlay_sha256":overlay_sha,"pool_release_id":RELEASE_DIR.name,
        "pool_release_root":str(RELEASE_DIR),"seed_receipt_reused":True,"episode_files_modified":False}
    write_once(overlay_root / ("runner_overlay_receipt_" + overlay_sha[:16] + ".json"), jd(receipt))
    return overlay_path


def resume_episode(eid: str, args):
    dst = eid_dir(eid)
    fail_if(not dst.is_dir(), "EPISODE_NOT_FOUND:" + str(dst))
    if (dst / "episode_manager_manifest.json").exists():
        bootstrap_new(dst, eid, args.seed_attempts)
    runner = prepare_resume_runner(dst, os.environ.get("N0_POOL20_RUN_ID", "UNSCOPED"))
    cmd = [PINNED_PYTHON,"-u",runner,"--run","--until",args.until,
           "--timeout",str(args.timeout),"--max-provider-errors",str(args.max_provider_errors),
           "--max-turns",str(args.max_turns),"--oj-attempts",str(args.oj_attempts)]
    print("=== RESUME EPISODE ===",eid,flush=True)
    print("Episode runner source preserved; release overlay:",runner,flush=True)
    print("Resume uses saved per-turn state and does not regenerate existing requests.",flush=True)
    code = spawn(cmd,dst)
    print("=== EPISODE EXIT CODE ===",code,flush=True)
    return code


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest="command", required=True)
    for command in ("new","resume","status"):
        sp = sub.add_parser(command)
        sp.add_argument("episode",help="N0_MANUAL_01, N0_MANUAL_02, ...")
        if command == "resume":
            sp.add_argument("--until",default="C0030")
            sp.add_argument("--timeout",type=int,default=900)
            sp.add_argument("--max-provider-errors",type=int,default=3)
            sp.add_argument("--max-turns",type=int,default=100)
            sp.add_argument("--oj-attempts",type=int,default=3,choices=(1,2,3))
            sp.add_argument("--seed-attempts",type=int,default=3)
    args=p.parse_args()
    eid_dir(args.episode)
    if args.command == "status":
        status_episode(args.episode)
        return 0
    if args.command == "new":
        stage_new(args.episode)
        bootstrap_new(eid_dir(args.episode), args.episode, 1)
        return 0
    fail_if(args.seed_attempts < 1 or args.seed_attempts > 10, "SEED_ATTEMPTS_MUST_BE_1_TO_10")
    return resume_episode(args.episode,args)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("MANAGER INTERRUPTED: saved Episode data retained",flush=True)
        sys.exit(130)
    except Exception as exc:
        print("MANAGER PAUSED:",type(exc).__name__,str(exc),flush=True)
        raise
