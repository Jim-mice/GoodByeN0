# -*- coding: utf-8 -*-
"""N0 manual Episode automation (starts AFTER settled C0002).

Run in the pinned Windows venv.  Reuses the existing frozen Harness tool loop,
manual OJ evaluator, and local Episode records.  NEVER touches formal OJ queue
or formal SQLite and NEVER uses the formal Worker.

Usage:
  python manual_episode_auto.py --check
  python manual_episode_auto.py --run --until C0030
  python manual_episode_auto.py --run --until C0003

One process/one Episode. The existing shared GPU lease is enforced by the
manually qualified OJ evaluator script. Every attempt and tool request is
persisted, so a restart can resume from settled state or a pending decision.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

M = Path(r"D:\projects\kernelagent\manual_episode_200001")
R = Path(r"D:\projects\kernelagent\runtime")
H = Path(r"D:\projects\kernelagent\runtime\harness\releases\N0_AUTONOMOUS_INFORMATION_ACQUISITION_R7_3_POOL20_R5_FILELIST_FIX")
P = R / ".venv_n0_r6/Scripts/python.exe"
LOGS = M / "logs"
RESULTS = M / "results"
STATE = M / "state"
WORK = M / "workspace"
EPISODE = "N0_MANUAL_200001"
TOTAL_BUDGET = 30
RUNNER_TEMPLATE_SHA = "5163e56d8867a9156a638edd5aa9e6174e8dde2b46964916d2cf68e9b929f526"
C0002_SHA = "62041245d5948b54def5020a2774b24a4367e3c48bb171799a0aa569e304894b"
DB = STATE / "manual_episode.sqlite3"
LOCK = STATE / "manual_full_episode.lock"
BASE_OJ = M / "manual_c0002_eval.py"


def need(condition, label):
    if not condition:
        raise RuntimeError(label)


def load(path):
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def jd(v):
    return json.dumps(v, indent=2, ensure_ascii=False, sort_keys=True) + "\n"


def sha(data):
    return hashlib.sha256(data).hexdigest()


def file_sha(path):
    return sha(Path(path).read_bytes())


def immutable(path, content):
    path = Path(path)
    data = content if isinstance(content, bytes) else content.encode("utf-8")
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        need(path.read_bytes() == data, "FILE_CONTENT_CONFLICT:" + str(path))
        return
    with path.open("xb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())


def replace_exact(text, before, after):
    count = text.count(before)
    need(count == 1, f"TEMPLATE_MARKER_NOT_UNIQUE ({count}): {before[:100]}")
    return text.replace(before, after)


def ensure_modules():
    sys.path[:0] = [str(H), str(M)]
    from n0_autonomous.engine import (AuditJournal, HistoryService, build_history_brief,
                                       build_first_request_messages, build_provider_request_body,
                                       canonical_bytes, sha256_bytes, _workspace_sha)
    from n0_autonomous.prompt_bundle import load_prompt_bundle
    return (AuditJournal, HistoryService, build_history_brief, build_first_request_messages,
            build_provider_request_body, canonical_bytes, sha256_bytes, _workspace_sha,
            load_prompt_bundle)


def settled_ids(con):
    rows = con.execute("SELECT candidate_id FROM candidate_outcomes WHERE episode_id=? ORDER BY candidate_id", (EPISODE,)).fetchall()
    ids = [str(x[0]) for x in rows]
    need(ids == [f"C{i:04d}" for i in range(len(ids))], "SETTLEMENT_ID_SEQUENCE_GAP")
    return ids


def init_db():
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



def baseline_and_history(ids):
    # Strict Episode-scoped local records.  Never import another Episode's history.
    candidates = []
    for cid in ids:
        path = RESULTS / f"{cid.lower()}_manual_history_record.json"
        need(path.is_file(), "HISTORY_MISSING:" + cid)
        rec = load(path)
        need(rec.get("candidate_id") == cid, "HISTORY_CANDIDATE_MISMATCH")
        need(sha(rec["source"].encode("utf-8")) == rec["source_sha256"],
             "HISTORY_SOURCE_HASH_MISMATCH:" + cid)
        receipt = rec.get("oj_receipt") or {}
        if receipt.get("verdict") == "CANDIDATE_CUDA_RUNTIME_FAILURE" and receipt.get("score") is None:
            # Preserve this terminal failure in its Episode audit, but do not
            # feed an incomplete, non-scoring legacy error receipt into the
            # scoring HistoryService contract.
            need(receipt.get("terminal") is True and receipt.get("candidate_sha256") == rec["source_sha256"],
                 "NONSCORING_HISTORY_RECEIPT_IDENTITY_MISMATCH:" + cid)
            need(receipt.get("request_id") and rec.get("ledger_entry", {}).get("request_id") == receipt.get("request_id"),
                 "NONSCORING_HISTORY_REQUEST_ID_MISMATCH:" + cid)
            body = {k: v for k, v in receipt.items() if k != "receipt_sha256"}
            _, _, _, _, _, canonical, sha256_bytes, _, _ = ensure_modules()
            need(str(receipt.get("receipt_sha256", "")).lower() == sha256_bytes(canonical(body)).lower(),
                 "NONSCORING_HISTORY_RECEIPT_SHA_MISMATCH:" + cid)
            continue
        candidates.append(rec)
    return candidates



def historical_audit_events(AuditJournal):
    events = AuditJournal(LOGS / 'manual_tool_audit').events()
    # C0001 started with two pre-loop knowledge queries, stored separately.
    import manual_advance_c0002 as advance
    return events + advance.early_retrieval_events()


def best_record(con):
    return con.execute("SELECT candidate_id,score FROM candidate_outcomes WHERE episode_id=? AND score IS NOT NULL ORDER BY score DESC,candidate_id ASC LIMIT 1", (EPISODE,)).fetchone()


def prepare_candidate(cid, con):
    (AuditJournal, HistoryService, build_history_brief, build_first_request_messages,
     build_provider_request_body, _, _, _, load_prompt_bundle) = ensure_modules()
    index = int(cid[1:])
    prior = f'C{index-1:04d}'
    ids = settled_ids(con)
    need(ids == [f'C{i:04d}' for i in range(index)], 'PRIOR_CANDIDATES_NOT_SETTLED')
    prior_dec = load(RESULTS / f'{prior.lower()}_manual_decision.json')
    need(prior_dec['decision'] == 'CONTINUE', 'PRIOR_AGENT_STOPPED')
    current_source = (WORK / 'candidate.cu').read_bytes()
    need(sha(current_source) == prior_dec['candidate_source_sha256'].lower(), 'WORKSPACE_NOT_AT_PREVIOUS_SUBMISSION')
    parent_record = load(RESULTS / f'{prior.lower()}_manual_history_record.json')
    need(parent_record['source_sha256'] == sha(current_source), 'PARENT_HISTORY_SOURCE_CONFLICT')

    settled = con.execute("SELECT verdict,score,promotion,result_file FROM candidate_outcomes WHERE episode_id=? AND candidate_id=?", (EPISODE, prior)).fetchone()
    need(settled is not None, 'PARENT_OUTCOME_MISSING')
    verdict, score, promotion, result_file = settled
    measured = load(result_file)
    official = measured.get('official_kernelbench_result') or {}
    timing = measured.get('timing') or {}
    observation = {
        'candidate_id': prior,
        'candidate_sha256': sha(current_source),
        'verdict': verdict,
        'score': score,
        'promotion': promotion,
        'compiled': official.get('compiled', measured.get('compiled')),
        'correctness': official.get('correctness', measured.get('correctness')),
        'candidate_runtime_ms': timing.get('candidate_runtime_ms', official.get('candidate_runtime_ms')),
        'kernelbench_reference_runtime_ms': timing.get('kernelbench_reference_runtime_ms', official.get('reference_runtime_ms')),
        'profile_status': 'NOT_COLLECTED_IN_MANUAL_TEST',
    }
    obs_text = jd(observation)
    obs_path = WORK / 'visible_observation.json'
    old = obs_path.read_bytes()
    previous_snapshot = LOGS / f'{prior.lower()}_visible_observation_before_{cid.lower()}.json'
    if previous_snapshot.exists():
        # An interrupted request setup may have updated the workspace already.
        # Preserve the original snapshot; never overwrite it with the new observation.
        need(old == previous_snapshot.read_bytes() or old == obs_text.encode('utf-8'),
             'OBSERVATION_UNEXPECTED_ON_RESUME')
    else:
        immutable(previous_snapshot, old)
    if obs_path.read_text(encoding='utf-8-sig') != obs_text:
        # No OJ or Provider call has started for this new candidate at this point.
        tmp = obs_path.with_suffix('.json.next')
        with tmp.open('wb') as f:
            f.write(obs_text.encode('utf-8')); f.flush(); os.fsync(f.fileno())
        os.replace(tmp, obs_path)

    files = {name:(WORK/name).read_text(encoding='utf-8-sig')
             for name in ('task_definition.json','candidate_abi.json','candidate.cu','visible_observation.json')}
    candidates = baseline_and_history(ids)
    history = HistoryService(EPISODE, candidates,
        profile_whitelist=load(R/'integrations/N0_PRODUCTION_INTEGRATION_R12_10_4_R9_6_R8_03_NATURAL_EXIT_HANDOFF_20261027/N0_PROFILE_WHITELIST_NONSCIENTIFIC_V1.json'))
    history.snapshot_sha256()
    episode = {'episode_id':EPISODE, 'task_id':'KB_L1_40_LAYERNORM',
               'current_candidate_id':cid, 'candidates':candidates,
               'workspace_files':files, 'unsettled_requests':[], 'receipt_conflicts':[]}
    note = prior_dec.get('working_note')
    brief = build_history_brief(episode, candidate_budget=TOTAL_BUDGET,
        audit_events=historical_audit_events(AuditJournal), working_note=note)
    brief_obj = json.loads(brief)
    need(brief_obj['budget'] == {'used':index-1,'max':TOTAL_BUDGET,'remaining':TOTAL_BUDGET-index+1},'BUDGET_BAD')
    history_fp = json.dumps({'previously_retrieved':brief_obj['previously_retrieved'],
                             'last_history_query':brief_obj['last_history_query']},
                            ensure_ascii=False,sort_keys=True,separators=(',',':'))
    bundle = load_prompt_bundle(H)
    messages = build_first_request_messages(
        bundle.system_prompt,bundle.task_user_message,
        task_definition=files['task_definition.json'], candidate_abi=files['candidate_abi.json'],
        candidate_source=files['candidate.cu'],candidate_sha256=sha(current_source),
        visible_observation=files['visible_observation.json'],
        candidate_budget=brief_obj['budget'],history_brief=brief,
        previously_retrieved=history_fp,working_note=note,tools=bundle.tools)
    body=build_provider_request_body(messages,model='DeepSeek-V4-Pro-0813',reasoning_effort='high',tools=bundle.tools)
    req = LOGS / f'{cid.lower()}_first_request.json'
    immutable(LOGS / f'{cid.lower()}_episode_state_v1.json', brief)
    immutable(req, jd(body))
    print(f'Prepared {cid}: used={index-1}/{TOTAL_BUDGET} best={best_record(con)} request={req}',flush=True)
    return req


def produce_candidate_runner(cid, req):
    source= M/'manual_agent_until_submit.py'
    raw=source.read_bytes()
    need(sha(raw)==RUNNER_TEMPLATE_SHA, 'AGENT_TEMPLATE_CHANGED')
    text=raw.decode('utf-8-sig').replace('\r\n', '\n').replace('\r', '\n')
    original_start=text.index('def manual_history():')
    original_end=text.index('\n\ndef build_tools():',original_start)
    replacement='''def manual_history():
    # Local validated records; no signed formal receipts are fabricated.
    from manual_episode_auto import baseline_and_history
    import sqlite3
    db = sqlite3.connect(str(M / "state/manual_episode.sqlite3"))
    try:
        ids = [x[0] for x in db.execute("SELECT candidate_id FROM candidate_outcomes WHERE episode_id=? ORDER BY candidate_id", (EPISODE,)).fetchall()]
    finally:
        db.close()
    current = int(INVOCATION.rsplit("C", 1)[-1])
    ids = [x for x in ids if int(x[1:]) < current]
    whitelist = read_json(R / "integrations/N0_PRODUCTION_INTEGRATION_R12_10_4_R9_6_R8_03_NATURAL_EXIT_HANDOFF_20261027/N0_PROFILE_WHITELIST_NONSCIENTIFIC_V1.json")
    return HistoryService(EPISODE, baseline_and_history(ids), profile_whitelist=whitelist)
'''
    text=text[:original_start]+replacement+text[original_end:]
    lower=cid.lower()
    index=int(cid[1:])
    substitutions={
      'STATE_PATH = LOGS / "manual_agent_loop_state.json"':f'STATE_PATH = LOGS / "manual_agent_loop_{lower}_state.json"',
      'LOCK_PATH = LOGS / "manual_agent_loop.lock"':f'LOCK_PATH = LOGS / "manual_agent_loop_{lower}.lock"',
      'INVOCATION = "N0_MANUAL_200001_C0001"':f'INVOCATION = "N0_MANUAL_200001_{cid}"',
      'FIRST_CONTINUATION = LOGS / "c0001_second_request.json"':f'FIRST_CONTINUATION = LOGS / "{lower}_first_request.json"',
      'FIRST_SHA = "fbf7fc3f537d94a1ae9f67f26a4da605b57c8064f7af171067d55b7da7bfc9db"':f'FIRST_SHA = "{file_sha(req)}"',
      '"candidate_id": "C0001",':f'"candidate_id": "{cid}",',
      'C0001_pending.cu':f'{cid}_pending.cu',
      'c0001_manual_decision.json':f'{lower}_manual_decision.json',
      'manual_turn_':f'{lower}_turn_',
      'manual_agent_events.jsonl':f'{lower}_agent_events.jsonl',
      '"turn": 2,': '"turn": 1,',
      'MISSING_C0001_SECOND_REQUEST':f'MISSING_{cid}_FIRST_REQUEST',
      'C0001_SECOND_REQUEST_SHA_MISMATCH':f'{cid}_FIRST_REQUEST_SHA_MISMATCH',
      '"candidate_budget_used": 0':f'"candidate_budget_used": {index-1}',
      'Budget still 0/30.':f'Budget at final decision: {index-1}/30; awaiting OJ.',
    }
    for before,after in substitutions.items():
        text=replace_exact(text,before,after) if before in (
            '"candidate_id": "C0001",','"turn": 2,') else text.replace(before,after)
    # Original C0001 code uses one matching instance of each required identity.
    need(f'FIRST_SHA = "{file_sha(req)}"' in text, 'RUNNER_REQUEST_SHA_NOT_EMBEDDED')
    need(f'INVOCATION = "N0_MANUAL_200001_{cid}"' in text, 'RUNNER_INVOCATION_NOT_EMBEDDED')
    compile(text, f'manual_agent_{lower}.py', 'exec')
    runner=M/f'manual_agent_{lower}.py'
    immutable(runner,text)
    return runner


def run_subprocess(cmd, *, log=None, env=None):
    kwargs={'cwd':str(M),'env':env,'check':False,'text':True,'errors':'replace'}
    if log:
        log.parent.mkdir(parents=True,exist_ok=True)
        # tee child output, keeping a text transcript independently of console.
        with log.open('a',encoding='utf-8') as f:
            f.write('\n=== RUN '+repr(cmd)+' at '+datetime.now(timezone.utc).isoformat()+' ===\n')
            f.flush()
            p=subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,bufsize=1,**{k:v for k,v in kwargs.items() if k!='check'})
            assert p.stdout is not None
            for line in p.stdout:
                print(line,end='',flush=True)
                f.write(line)
                f.flush()
            return p.wait()
    return subprocess.call(cmd,cwd=str(M),env=env)


def run_model(cid, req, args):
    runner=produce_candidate_runner(cid,req)
    decision_path=RESULTS/f'{cid.lower()}_manual_decision.json'
    if decision_path.exists():
        print(f'{cid} model decision already exists; skipping Provider',flush=True)
        return load(decision_path)
    cmd=[str(P),'-u',str(runner),'--timeout',str(args.timeout),
         '--max-provider-errors',str(args.max_provider_errors),'--max-turns',str(args.max_turns)]
    code=run_subprocess(cmd,log=LOGS/f'{cid.lower()}_agent_console.log')
    if not decision_path.exists():
        raise RuntimeError(f'{cid}_PROVIDER_LOOP_NOT_COMPLETE: exit={code}; resume same --run command later')
    need(code==0, f'{cid}_PROVIDER_EXIT_NONZERO:{code}')
    dec=load(decision_path)
    need(dec.get('candidate_id')==cid and dec.get('decision') in ('CONTINUE','STOP'), 'AGENT_DECISION_INVALID')
    return dec


def make_evaluator(cid, attempt):
    need(BASE_OJ.is_file(), 'KNOWN_WORKING_C0002_EVALUATOR_MISSING')
    txt=BASE_OJ.read_text(encoding='utf-8-sig')
    lower=cid.lower()
    stem=lower if attempt==0 else f'{lower}_retry{attempt:02d}'
    candidate=RESULTS/f'{cid}_pending.cu'
    expected=file_sha(candidate)
    orig=[('C0002_pending.cu',f'{cid}_pending.cu'),
          ('c0002_result.json',f'{stem}_result.json'),
          ('c0002_reference.json',f'{stem}_reference.json'),
          (C0002_SHA.upper(),expected.upper())]
    for before, after in orig:
        need(before in txt,'EVALUATOR_MARKER_MISSING:'+before)
        txt=txt.replace(before,after)
    # These are cosmetic labels; exact output files and source sha are checked above.
    txt=txt.replace('C0002',cid).replace('c0002',lower)
    path=M/(f'manual_{stem}_submission_eval.py' if stem == 'c0002' else f'manual_{stem}_eval.py')
    compile(txt,str(path),'exec')
    immutable(path,txt)
    return path,RESULTS/f'{stem}_result.json'


def record_cccl_c1189_failure(cid, pending, log_path, result_path, exit_code):
    # Only a precisely identified CUDA 13.4 CCCL fatal C1189 error is
    # normalized. Other build failures still pause for attribution.
    need(exit_code != 0 and not result_path.exists(), 'COMPILER_FAILURE_RECORD_PRECONDITION')
    need(log_path.is_file(), 'MISSING_COMPILATION_TRANSCRIPT:'+str(log_path))
    log=log_path.read_text(encoding='utf-8', errors='replace')
    markers=(
        'fatal error C1189',
        'MSVC/cl.exe with traditional preprocessor',
        'CCCL_IGNORE_MSVC_TRADITIONAL_PREPROCESSOR_WARNING',
        'Error building extension',
    )
    if not all(x in log for x in markers):
        return False
    messages=[line.strip() for line in log.splitlines()
              if ('fatal error C1189' in line or
                  'MSVC/cl.exe with traditional preprocessor' in line or
                  'CCCL_IGNORE_MSVC_TRADITIONAL_PREPROCESSOR_WARNING' in line)]
    obj={
        'status':'OJ_INFRASTRUCTURE_HOLD_COMPILATION_ATTRIBUTION_UNKNOWN',
        'result_origin':'MANUAL_RUNNER_NORMALIZATION_NOT_OJ_RECEIPT',
        'manual_failure_code':'CCCL_MSVC_TRADITIONAL_PREPROCESSOR_C1189',
        'candidate_id':cid,
        'candidate_source_sha256':file_sha(pending),
        'compiled':False,
        'correctness':None,
        'timing':None,
        'official_kernelbench_result':None,
        'failure':'CUDA 13.4 CCCL requires an MSVC standard-conforming preprocessor; '
                  'frozen compiler contract was not changed. Attribution remains infrastructure-unknown.',
        'build_exit_code':exit_code,
        'raw_compiler_log':str(log_path),
        'raw_compiler_log_sha256':file_sha(log_path),
        'diagnostic_lines':messages,
        'compiler_flags_changed':False,
        'score':None,
    }
    immutable(result_path,jd(obj))
    print(f'{cid} compiler exception normalized as infrastructure-unknown; '
          f'raw log retained at {log_path}',flush=True)
    return True


def record_ptxas_build_failure(cid, pending, log_path, result_path, exit_code):
    """Persist source-bound, manual-only evidence of a ptxas compilation failure."""
    if result_path.exists() or exit_code == 0 or not log_path.is_file():
        return False
    raw_log = log_path.read_text(encoding='utf-8', errors='replace')
    low = raw_log.lower()
    if not ('ptxas fatal' in low and 'ninja: build stopped' in low):
        return False
    decision = load(RESULTS/f'{cid.lower()}_manual_decision.json')
    source_sha = file_sha(pending)
    need(decision.get('candidate_id') == cid and
         decision.get('decision') == 'CONTINUE' and
         str(decision.get('candidate_source_sha256','')).lower() == source_sha,
         'COMPILER_FAILURE_SOURCE_NOT_BOUND_TO_DECISION')
    lines = raw_log.splitlines()
    spots = [i for i, line in enumerate(lines) if
             ('ptxas fatal' in line.lower() or 'ptxas error' in line.lower())]
    indices = sorted({j for i in spots for j in range(max(0,i-3), min(len(lines),i+2))})
    diagnostics = [f'L{i+1}: {lines[i].strip()}' for i in indices][:80]
    failure_code = 'PTXAS_ASSEMBLY_ABORT_ATTRIBUTION_UNDETERMINED'
    obj = {
        'status': 'MANUAL_COMPILE_FAILED_PTXAS_UNCLASSIFIED',
        'result_origin': 'MANUAL_RUNNER_NORMALIZATION_NOT_OJ_RECEIPT',
        'manual_failure_code': failure_code,
        'candidate_id': cid,
        'candidate_source_sha256': source_sha,
        'compiled': False, 'correctness': None,
        'timing': None, 'official_kernelbench_result': None,
        'score': None,
        'failure': 'NVCC ptxas assembly failed before any correctness or timing; '
                   'candidate-vs-toolchain attribution remains undetermined. '
                   'See unchanged raw compiler log.',
        'build_exit_code': exit_code,
        'raw_compiler_log': str(log_path),
        'raw_compiler_log_sha256': file_sha(log_path),
        'diagnostic_lines': diagnostics,
        'compiler_flags_changed': False,
    }
    immutable(result_path, jd(obj))
    print(f'{cid}: ptxas failure recorded with NULL score; '
          'full compiler log preserved:', log_path, flush=True)
    for line in diagnostics[:14]:
        print('  '+line,flush=True)
    return True

def evaluate_candidate(cid,args):
    import importlib
    release=Path(r"D:\projects\kernelagent\runtime\deployment\releases\N0_KERNEL_AGENT_POOL20_R8_18_R8_03_RESUME_REMAINING_PATH_FIX_20261027").resolve()
    sys.path.insert(0, str(release))
    module=importlib.import_module("pool20_shared_oj")
    actual=Path(module.__file__).resolve()
    expected=(release/"pool20_shared_oj.py").resolve()
    need(actual==expected,"POOL20_SHARED_OJ_MODULE_ORIGIN_MISMATCH:"+str(actual))
    manifest=release/"RELEASE_MANIFEST.json"
    binding={"schema":"N0_POOL20_RUNTIME_MODULE_BINDING_V1","run_id":RUN_ID,
        "episode_id":EPISODE,"candidate_id":cid,"python_executable":str(Path(sys.executable).resolve()),
        "python_prefix":str(Path(sys.prefix).resolve()),"working_directory":str(Path.cwd().resolve()),
        "runner_overlay":str(Path(__file__).resolve()),"runner_overlay_sha256":file_sha(__file__),
        "pool_release_id":release.name,"pool_manifest_sha256":file_sha(manifest).upper(),
        "module_file":str(actual),"module_sha256":file_sha(actual).upper(),
        "sys_path":[str(Path(x).resolve()) for x in sys.path if x]}
    need(binding["python_executable"].casefold()==str(P.resolve()).casefold() and
         binding["python_prefix"].casefold()==str((R/".venv_n0_r6").resolve()).casefold(),
         "POOL20_RUNTIME_PYTHON_IDENTITY_MISMATCH")
    immutable(LOGS/f"{cid.lower()}_runtime_binding.json",jd(binding))
    shared_evaluate=module.evaluate_candidate
    source_path=RESULTS/f"{cid}_pending.cu"
    existing=RESULTS/f"{cid.lower()}_result.json"
    if existing.is_file():
        result=load(existing)
        prior=validate_existing_result_against_queue(cid,source_path,result)
        if not prior.get("infrastructure_hold"):
            return existing,[existing]
        # A consumed infrastructure HOLD is immutable evidence, not a Candidate
        # terminal result. The shared client resumes only its single linked retry.
        result_path,_=shared_evaluate(EPISODE,cid,source_path,WORK,RESULTS)
        return result_path,[existing,result_path]
    result_path,result=shared_evaluate(EPISODE,cid,source_path,WORK,RESULTS)
    return result_path,[result_path]


def validate_existing_result_against_queue(cid,source_path,result):
    root=Path(os.environ.get("N0_OJ_QUEUE_ROOT", "")).resolve()
    db=root/"n0_runtime.sqlite3"
    need(root.is_dir() and db.is_file(),"EXISTING_RESULT_QUEUE_ROOT_UNAVAILABLE")
    source_sha=sha(Path(source_path).read_bytes())
    need(result.get("candidate_id")==cid and str(result.get("candidate_source_sha256","")).lower()==source_sha.lower(),"EXISTING_RESULT_CANDIDATE_IDENTITY_MISMATCH")
    request_id=result.get("request_id");need(request_id,"EXISTING_RESULT_REQUEST_ID_MISSING")
    con=sqlite3.connect(db.resolve().as_uri()+"?mode=ro",uri=True,timeout=10);con.row_factory=sqlite3.Row
    try:
        row=con.execute("SELECT * FROM oj_requests WHERE request_id=?",(request_id,)).fetchone()
    finally:con.close()
    need(row is not None,"EXISTING_RESULT_REQUEST_NOT_FOUND")
    receipt=json.loads(row["receipt_json"] or "{}")
    need(row["state"]=="CONSUMED" and row["episode_id"]==EPISODE and row["candidate_id"]==cid and
         str(row["candidate_sha256"]).lower()==source_sha.lower() and
         str(row["receipt_sha256"]).lower()==str(result.get("oj_receipt_sha256","")).lower(),
         "EXISTING_RESULT_QUEUE_IDENTITY_MISMATCH")
    queue_sha=sha(json.dumps(receipt,sort_keys=True,separators=(",",":"),ensure_ascii=False).encode("utf-8"))
    need(queue_sha.lower()==str(row["receipt_sha256"]).lower(),"EXISTING_RESULT_AUTHORITY_RECEIPT_SHA_MISMATCH")
    need(receipt.get("request_id")==request_id and receipt.get("candidate_id")==cid and
         receipt.get("episode_id")==EPISODE and str(receipt.get("candidate_sha256","")).lower()==source_sha.lower() and
         result.get("status")==receipt.get("verdict") and result.get("score")==receipt.get("score"),
         "EXISTING_RESULT_RECEIPT_CONTENT_MISMATCH")
    if result.get("status")!="VALID_MEASUREMENT":
        if str(result.get("status","")).startswith("OJ_INFRASTRUCTURE_HOLD_"):
            need(result.get("score") is None and receipt.get("score") is None and
                 receipt.get("verdict")==result.get("status"),
                 "EXISTING_RESULT_INFRASTRUCTURE_HOLD_MISMATCH")
            return {"request_id":request_id,"receipt_sha256":row["receipt_sha256"],"queue_sequence":row["queue_sequence"],"infrastructure_hold":True}
        need(result.get("score") is None and receipt.get("score") is None and
             receipt.get("verdict")=="CANDIDATE_CUDA_RUNTIME_FAILURE" and
             (receipt.get("candidate_failure_diagnostics") or {}).get("attribution")=="CANDIDATE",
             "EXISTING_RESULT_NONSCORING_FAILURE_NOT_ADOPTABLE")
    return {"request_id":request_id,"receipt_sha256":row["receipt_sha256"],"queue_sequence":row["queue_sequence"],"infrastructure_hold":False}



def failed_record(cid, raw, result, parent_sha, promotion, attempts):
    _,_,_,_,_,canonical,sha256_bytes,_,_=ensure_modules()
    source_sha=sha(raw)
    verdict=str(result['status'])
    need(verdict!='VALID_MEASUREMENT','FAILED_RECORD_RECEIVED_VALID')
    task_sha='8D975D7CEE6946513DCFB6BA84C971E53A890F9D9C6F0A4FB828904941F25975'
    manifest=(R/'oj/releases/N0_KERNELBENCH_L1_40_LAYERNORM_R5_REFERENCE_TRUST/RELEASE_MANIFEST.json')
    # Failed C0001 may have no C0002 retry artifact.
    # Use this Episode's independently qualified C0000 baseline.
    identity=load(RESULTS/'c0000_result.json')
    need(identity.get('status') == 'VALID_MEASUREMENT',
         'FAILURE_RECEIPT_BASELINE_NOT_VALID')
    need(isinstance(identity.get('gpu_identity'), dict) and identity['gpu_identity'],
         'FAILURE_RECEIPT_BASELINE_GPU_IDENTITY_MISSING')
    if result.get('gpu_identity') is not None:
        need(result['gpu_identity'] == identity['gpu_identity'],
             'FAILURE_RECEIPT_GPU_IDENTITY_CONFLICT')
    gpu=identity['gpu_identity']
    timing={'status':'UNAVAILABLE','trusted_score':None}
    profile={'status':'NOT_COLLECTED_IN_MANUAL_TEST','metrics':{}}
    diagnostics={'status':verdict, 'failure':result.get('failure') or result.get('reason'), 'official':result.get('official_kernelbench_result'),
                 'original_evaluation_attempts': attempts,
                 'manual_failure_code':result.get('manual_failure_code'),
                 'raw_compiler_log':result.get('raw_compiler_log'),
                 'raw_compiler_log_sha256':result.get('raw_compiler_log_sha256'),
                 'diagnostic_lines':result.get('diagnostic_lines')}
    lineage={'parent_sha256':parent_sha,'version':cid+'_MANUAL_SUBMISSION'}
    artifacts={'timing':timing,'profile':profile,'diagnostics':diagnostics,'lineage':lineage}
    receipt={'terminal':True,'candidate_sha256':source_sha,'task_sha256':task_sha,
             'gpu_identity':gpu, 'release_id':'N0_KERNELBENCH_L1_40_LAYERNORM_R5_REFERENCE_TRUST',
             'manifest_sha256':sha(manifest.read_bytes()).upper(),
             'measurement_protocol':'MANUAL_UNTRUSTED_RESULT_NO_FORMAL_RECEIPT',
             'request_id':'N0-MANUAL-'+cid, 'verdict':verdict,
             'evaluation_status':verdict,'score':None,'speedup':None,**artifacts}
    receipt['receipt_sha256']=sha256_bytes(canonical(receipt))
    ledger={'candidate_id':cid,'candidate_sha256':source_sha,'request_id':receipt['request_id'],
            'verdict':verdict,'score':None,'promotion':promotion,
            'receipt_sha256':receipt['receipt_sha256'],
            'artifact_sha256':{k:sha256_bytes(canonical(v)) for k,v in artifacts.items()}}
    ledger['ledger_entry_sha256']=sha256_bytes(canonical(ledger))
    return {'candidate_id':cid,'source':raw.decode('utf-8'), 'source_sha256':source_sha,
            'oj_receipt':receipt,'ledger_entry':ledger,**artifacts}


def settle_candidate(cid, result_path, attempts, con):
    current=RESULTS/f'{cid}_pending.cu'
    raw=current.read_bytes()
    source_sha=sha(raw)
    decision=load(RESULTS/f'{cid.lower()}_manual_decision.json')
    need(decision.get('candidate_source_sha256')==source_sha,'DECISION_SOURCE_CONFLICT')
    outcome=load(result_path)
    returned_sha=outcome.get('candidate_source_sha256')
    if returned_sha is None:
        # Original R5 omits the digest on an early static syntax rejection.
        # This local provenance digest is derived from the immutable source,
        # not attributed to an OJ field that was never returned.
        need(outcome.get('status')=='INVALID_SUBMISSION' and
             (outcome.get('syntax') or {}).get('status')=='INVALID_SUBMISSION',
             'EVAL_SOURCE_IDENTITY_MISSING')
    else:
        need(str(returned_sha).lower()==source_sha,'EVAL_SOURCE_CONFLICT')
    prior=f'C{int(cid[1:])-1:04d}'
    parent=load(RESULTS/f'{prior.lower()}_manual_history_record.json')
    parent_sha=parent['source_sha256']
    status=outcome['status']
    score=None
    promo='NOT_PROMOTED'
    notes=[]
    for p in attempts:
        r=load(p)
        notes.append({'file':str(p),'file_sha256':file_sha(p),'status':r['status'],'failure':r.get('failure')})
    official_record=outcome.get('candidate_record')
    if isinstance(official_record,dict):
        record=official_record
        score=record.get('ledger_entry',{}).get('score')
        promo=record.get('ledger_entry',{}).get('promotion','NOT_PROMOTED')
    elif status=='VALID_MEASUREMENT':
        raise RuntimeError('POOL20_OFFICIAL_HARNESS_RECORD_MISSING')
    else:
        raise RuntimeError('POOL20_OFFICIAL_FAILURE_RECORD_MISSING')
    _,HistoryService,*_=ensure_modules()
    if score is not None:
        history_candidates=baseline_and_history(settled_ids(con))+[record]
        hs=HistoryService(EPISODE,history_candidates,
                          profile_whitelist=load(R/'integrations/N0_PRODUCTION_INTEGRATION_R12_10_4_R9_6_R8_03_NATURAL_EXIT_HANDOFF_20261027/N0_PROFILE_WHITELIST_NONSCIENTIFIC_V1.json'))
        hs.snapshot_sha256()
    else:
        need(status=="CANDIDATE_CUDA_RUNTIME_FAILURE" and record.get("source_sha256")==source_sha,
             "NONSCORING_FAILURE_SOURCE_OR_STATUS_MISMATCH")
        validate_existing_result_against_queue(cid,current,outcome)
        receipt=record.get("oj_receipt") or {}
        ledger=record.get("ledger_entry") or {}
        need(receipt.get("terminal") is True and receipt.get("score") is None and
             receipt.get("candidate_sha256")==source_sha and receipt.get("request_id")==outcome.get("request_id") and
             receipt.get("verdict")==status and ledger.get("candidate_sha256")==source_sha and
             ledger.get("score") is None and ledger.get("promotion")=="NOT_PROMOTED",
             "NONSCORING_FAILURE_RECORD_IDENTITY_MISMATCH")
    history_file=RESULTS/f'{cid.lower()}_manual_history_record.json'
    immutable(history_file,jd(record))
    settlement=RESULTS/f'{cid.lower()}_manual_settlement.json'
    existing_settlement = RESULTS/f'{cid.lower()}_manual_settlement.json'
    settlement_time = (load(existing_settlement)['settled_at_utc'] if existing_settlement.exists()
                       else datetime.now(timezone.utc).isoformat())
    summary={'episode_id':EPISODE,'candidate_id':cid,'source_sha256':source_sha,
             'status':status,'score':score,'promotion':promo,'result_file':str(result_path),
             'result_sha256':file_sha(result_path),'history_file':str(history_file),
             'history_sha256':file_sha(history_file),'attempts':notes,
             'budget_used':int(cid[1:]),'legacy_episode_db_written':True,
             'shared_oj_queue_written':True,
             'official_oj_request_id':outcome.get('request_id'),
             'official_oj_receipt_sha256':outcome.get('oj_receipt_sha256'),
             'settled_at_utc':settlement_time}
    immutable(settlement,jd(summary))

    # Update both outcome ledger and existing score-only ledger in one local txn.
    key=(EPISODE,cid)
    tup=(EPISODE,cid,source_sha,status,score,promo,str(result_path),file_sha(result_path),
         str(history_file),file_sha(history_file),summary['settled_at_utc'])
    cur=con.execute('SELECT * FROM candidate_outcomes WHERE episode_id=? AND candidate_id=?',key).fetchone()
    if cur is not None:
        need(tuple(cur)==tup,'ALREADY_SETTLED_CONFLICT:'+cid)
        return
    con.execute('BEGIN IMMEDIATE')
    try:
        # Keep evaluation_attempts immutable; if a retry occurred it uses next index.
        for i,p in enumerate(attempts):
            r=load(p)
            con.execute('INSERT OR IGNORE INTO evaluation_attempts VALUES (?,?,?,?,?,?)',
                        (EPISODE,cid,i,r['status'],str(p),file_sha(p)))
            check=con.execute('SELECT status,result_path,sha256 FROM evaluation_attempts WHERE episode_id=? AND candidate_id=? AND attempt_index=?',(EPISODE,cid,i)).fetchone()
            need(check==(r['status'],str(p),file_sha(p)), 'ATTEMPT_DB_CONFLICT')
        if score is not None:
            timing=outcome['timing']
            record_json=jd(record)
            con.execute('INSERT INTO candidate_settlements VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                 (EPISODE,cid,source_sha,status,score,promo,str(result_path),file_sha(result_path),
                  float(timing['candidate_runtime_ms']),float(timing['kernelbench_reference_runtime_ms']),record_json))
        con.execute('INSERT INTO candidate_outcomes VALUES (?,?,?,?,?,?,?,?,?,?,?)',tup)
        con.commit()
    except Exception:
        con.rollback()
        raise
    print(f'=== {cid} SETTLED: status={status}, score={score}, promotion={promo} '
          f'budget={int(cid[1:])}/30 ===',flush=True)


def drive(args):
    STATE.mkdir(parents=True,exist_ok=True)
    LOGS.mkdir(parents=True,exist_ok=True)
    RESULTS.mkdir(parents=True,exist_ok=True)
    if args.check:
        preflight()
        return
    need(args.run, 'SPECIFY_--run_OR_--check')
    need(args.until>=3 and args.until<=30,'UNTIL_OUTSIDE_C0003_C0030')
    with LOCK.open('x',encoding='utf-8') as f:
        f.write(jd({'pid':os.getpid(),'started_at_utc':datetime.now(timezone.utc).isoformat()}))
        f.flush();os.fsync(f.fileno())
    try:
        preflight()
        con=init_db()
        try:
            while True:
                stop_file=os.environ.get('N0_POOL20_STOP_FILE')
                if stop_file and Path(stop_file).is_file():
                    print('=== POOL20 SAFE STOP: between Candidate invocations; current Episode preserved ===',flush=True)
                    return
                ids=settled_ids(con)
                next_n=len(ids)
                if next_n>args.until:
                    print(f'=== EPISODE PAUSED AT {ids[-1]} (requested --until C{args.until:04d}) ===',flush=True)
                    return
                if next_n>TOTAL_BUDGET:
                    print('=== EPISODE BUDGET EXHAUSTED: 30/30 ===',flush=True)
                    return
                cid=f'C{next_n:04d}'
                # Pending STOP from an earlier launch prevents accidental continuation.
                stop_path=STATE/'manual_episode_agent_stop.json'
                if stop_path.exists():
                    print('=== EPISODE ALREADY STOPPED BY AGENT ===',load(stop_path),flush=True)
                    return
                dec_path=RESULTS/f'{cid.lower()}_manual_decision.json'
                if not dec_path.exists():
                    req_path=LOGS/f'{cid.lower()}_first_request.json'
                    if req_path.exists():
                        # The request was already persisted, possibly with edits in progress.
                        # Never reconstruct it from a modified workspace.
                        print(f'=== RESUME {cid} EXISTING REQUEST {req_path} ===',flush=True)
                        req=req_path
                    else:
                        req=prepare_candidate(cid,con)
                    dec=run_model(cid,req,args)
                else:
                    dec=load(dec_path)
                    print(f'=== RESUME {cid}: cached model decision {dec.get("decision")} ===',flush=True)
                need(dec.get('candidate_id')==cid,'CACHED_MODEL_DECISION_CANDIDATE_MISMATCH')
                if dec.get('decision')=='STOP':
                    immutable(stop_path,jd({'episode_id':EPISODE,'candidate_id':cid,
                                             'model_decision_file':str(dec_path),'reason':dec.get('reason'),
                                             'stopped_at_utc':datetime.now(timezone.utc).isoformat()}))
                    print(f'=== AGENT STOP BEFORE {cid} OJ; budget remains {next_n-1}/30 ===',flush=True)
                    return
                need(dec.get('decision')=='CONTINUE','UNKNOWN_AGENT_DECISION')
                result,attempts=evaluate_candidate(cid,args)
                settle_candidate(cid,result,attempts,con)
        finally:
            con.close()
    finally:
        LOCK.unlink(missing_ok=True)


def preflight():
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
    print("Shared R12.7 OJ queue is used; local Episode OJ is not the scoring source.", flush=True)



def parse_args():
    p=argparse.ArgumentParser(description=__doc__)
    e=p.add_mutually_exclusive_group(required=True)
    e.add_argument('--check',action='store_true')
    e.add_argument('--run',action='store_true')
    p.add_argument('--until',type=lambda x:int(x[1:]) if x.startswith('C') else int(x),default=30)
    p.add_argument('--timeout',type=int,default=900)
    p.add_argument('--max-provider-errors',type=int,default=3)
    p.add_argument('--max-turns',type=int,default=100)
    p.add_argument('--oj-attempts',type=int,default=3,choices=(1,2,3))
    return p.parse_args()


if __name__=='__main__':
    try:
        drive(parse_args())
    except KeyboardInterrupt:
        print('Manual Episode interrupted: all completed files remain for resumption.',flush=True)
        raise SystemExit(130)
    except Exception as ex:
        print('MANUAL_EPISODE_PAUSED:',type(ex).__name__,str(ex),flush=True)
        raise
