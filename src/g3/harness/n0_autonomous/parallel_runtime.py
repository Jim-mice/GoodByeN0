"""Durable episode worker metadata and one-at-a-time OJ request queue.

No Provider or GPU implementation is included. Integrations inject those
adapters; ambiguous in-flight work is held and never automatically replayed.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import sqlite3
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable

from .engine import N0Error, canonical_bytes, sha256_bytes


ACTIVE_STATES = {"ALLOCATED", "RUNNING", "WAITING_PROVIDER", "WAITING_OJ"}
N0_TASK_SHA256 = "8D975D7CEE6946513DCFB6BA84C971E53A890F9D9C6F0A4FB828904941F25975"
N0_TASK_DEFINITION_SHA256 = "3F0EB778C8F6673F1B6589060742CAF56FD21AB57A9DA48C9DECFED79E198709"
N0_COMPILER_CONTRACT_SHA256 = "303AF7065B56C8A29ED370391681F06C1F31EF5066632D82B7141BF0AEB9C1FA"
N0_SEED_SHA256 = "CBD7E93B47AD3957E4E133BDE45C0AF1E933DD9BEC3525C27637049337FB2D91"


@dataclass(frozen=True)
class EpisodePaths:
    episode_id: str
    root: Path
    workspace: Path
    archive: Path
    checkpoint: Path
    ledger: Path
    summary: Path
    logs: Path
    working_note: Path


class N0PersistentQueue:
    """SQLite WAL state for authorized Episode slots and shared OJ scheduling."""

    def __init__(self, root: Path, *, max_active_episodes: int = 4, worker_count: int = 4,
                 oj_gpu_active_jobs_max: int = 1):
        if (max_active_episodes, worker_count, oj_gpu_active_jobs_max) not in {(4, 4, 1), (10, 10, 1), (20, 20, 1)}:
            raise N0Error("N0_QUEUE_LIMITS_MUST_BE_4_4_1_10_10_1_OR_20_20_1")
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.db_path = self.root / "n0_runtime.sqlite3"
        self.max_active_episodes = max_active_episodes
        self.worker_count = worker_count
        self.oj_gpu_active_jobs_max = oj_gpu_active_jobs_max
        self._init_db()
        self.reconcile_all_episode_exports()

    def _connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.db_path, timeout=30, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("PRAGMA busy_timeout=30000")
        db.execute("PRAGMA synchronous=FULL")
        return db

    def is_aborted(self) -> dict[str, Any] | None:
        db=self._connect()
        try:
            row=db.execute("SELECT value FROM runtime_meta WHERE key='science_batch_abort'").fetchone()
            return json.loads(row[0]) if row else None
        finally: db.close()

    def abort(self, reason: str, event: dict[str, Any]) -> dict[str, Any]:
        """Persist the first batch-wide infrastructure abort; never cancels in-flight work."""
        if reason!="PROVIDER_CONTEXT_WINDOW_EXCEEDED" or event.get("event_type")!=reason:
            raise N0Error("SCIENCE_BATCH_ABORT_REASON_INVALID")
        body={"status":"INFRA_ABORT_CONTEXT_EXCEEDED","reason":reason,"event":event,"created_at":time.time()}
        encoded=canonical_bytes(body).decode("utf-8")
        db=self._connect()
        try:
            db.execute("BEGIN IMMEDIATE")
            row=db.execute("SELECT value FROM runtime_meta WHERE key='science_batch_abort'").fetchone()
            if row:
                prior=json.loads(row[0]); db.execute("COMMIT"); return prior
            db.execute("INSERT INTO runtime_meta(key,value) VALUES('science_batch_abort',?)",(encoded,))
            episode_id=event.get("episode_id")
            if episode_id:
                exists=db.execute("SELECT 1 FROM episodes WHERE episode_id=?",(episode_id,)).fetchone()
                if exists:
                    event_id="batch-abort:"+str(event.get("request_id"))
                    db.execute("INSERT OR IGNORE INTO episode_events(event_id,episode_id,event_json,created_at) VALUES(?,?,?,?)",
                               (event_id,episode_id,canonical_bytes(event).decode("utf-8"),time.time()))
            db.execute("COMMIT")
            return body
        except BaseException:
            if db.in_transaction: db.execute("ROLLBACK")
            raise
        finally: db.close()

    def _init_db(self) -> None:
        db = self._connect()
        try:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript("""
              CREATE TABLE IF NOT EXISTS episodes(
                episode_id TEXT PRIMARY KEY, replicate_slot TEXT NOT NULL UNIQUE,
                treatment_id TEXT NOT NULL, state TEXT NOT NULL, approved_allocation_sha256 TEXT NOT NULL,
                max_candidates INTEGER NOT NULL, candidates_used INTEGER NOT NULL DEFAULT 0,
                last_candidate_sha256 TEXT, created_at REAL NOT NULL, updated_at REAL NOT NULL);
              CREATE TABLE IF NOT EXISTS provider_requests(
                request_id TEXT PRIMARY KEY, episode_id TEXT NOT NULL REFERENCES episodes(episode_id),
                invocation_id TEXT NOT NULL, request_index INTEGER NOT NULL, payload_sha256 TEXT NOT NULL,
                state TEXT NOT NULL, attempt_id TEXT, response_id TEXT, response_sha256 TEXT,
                payload_path TEXT, response_path TEXT, response_consumed_at REAL,
                created_at REAL NOT NULL, updated_at REAL NOT NULL,
                UNIQUE(episode_id,invocation_id,request_index));
              CREATE TABLE IF NOT EXISTS oj_requests(
                request_id TEXT PRIMARY KEY, episode_id TEXT NOT NULL REFERENCES episodes(episode_id),
                invocation_id TEXT NOT NULL, candidate_id TEXT NOT NULL, candidate_sha256 TEXT NOT NULL,
                state TEXT NOT NULL, queue_sequence INTEGER NOT NULL UNIQUE, worker_id TEXT,
                lease_id TEXT, worker_pid INTEGER, worker_start_utc TEXT,
                receipt_sha256 TEXT, receipt_json TEXT, error TEXT,
                candidate_source_path TEXT,
                created_at REAL NOT NULL, claimed_at REAL, completed_at REAL, receipt_consumed_at REAL, updated_at REAL NOT NULL,
                UNIQUE(episode_id,invocation_id));
              CREATE TABLE IF NOT EXISTS candidate_slots(
                episode_id TEXT NOT NULL REFERENCES episodes(episode_id),
                candidate_id TEXT NOT NULL, request_id TEXT NOT NULL UNIQUE REFERENCES oj_requests(request_id),
                candidate_sha256 TEXT NOT NULL, status TEXT NOT NULL,
                verdict TEXT, receipt_sha256 TEXT, created_at REAL NOT NULL, updated_at REAL NOT NULL,
                PRIMARY KEY(episode_id,candidate_id), UNIQUE(episode_id,candidate_sha256));
              CREATE INDEX IF NOT EXISTS candidate_slots_by_status
                ON candidate_slots(episode_id,status,candidate_id);
              CREATE INDEX IF NOT EXISTS oj_pending_by_episode
                ON oj_requests(state,episode_id,queue_sequence);
              CREATE TABLE IF NOT EXISTS runtime_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
              CREATE TABLE IF NOT EXISTS episode_events(
                event_id TEXT PRIMARY KEY, episode_id TEXT NOT NULL REFERENCES episodes(episode_id),
                event_json TEXT NOT NULL, created_at REAL NOT NULL);
              INSERT OR IGNORE INTO runtime_meta(key,value) VALUES('round_robin_cursor','');
              INSERT OR IGNORE INTO runtime_meta(key,value) VALUES('queue_sequence','0');
            """)
            columns={row[1] for row in db.execute("PRAGMA table_info(oj_requests)")}
            if "claimed_at" not in columns: db.execute("ALTER TABLE oj_requests ADD COLUMN claimed_at REAL")
            if "completed_at" not in columns: db.execute("ALTER TABLE oj_requests ADD COLUMN completed_at REAL")
            if "receipt_json" not in columns: db.execute("ALTER TABLE oj_requests ADD COLUMN receipt_json TEXT")
            if "error" not in columns: db.execute("ALTER TABLE oj_requests ADD COLUMN error TEXT")
            if "candidate_source_path" not in columns: db.execute("ALTER TABLE oj_requests ADD COLUMN candidate_source_path TEXT")
            if "worker_pid" not in columns: db.execute("ALTER TABLE oj_requests ADD COLUMN worker_pid INTEGER")
            if "worker_start_utc" not in columns: db.execute("ALTER TABLE oj_requests ADD COLUMN worker_start_utc TEXT")
            if "receipt_consumed_at" not in columns: db.execute("ALTER TABLE oj_requests ADD COLUMN receipt_consumed_at REAL")
            provider_cols={row[1] for row in db.execute("PRAGMA table_info(provider_requests)")}
            if "payload_path" not in provider_cols: db.execute("ALTER TABLE provider_requests ADD COLUMN payload_path TEXT")
            if "response_path" not in provider_cols: db.execute("ALTER TABLE provider_requests ADD COLUMN response_path TEXT")
            if "response_consumed_at" not in provider_cols: db.execute("ALTER TABLE provider_requests ADD COLUMN response_consumed_at REAL")
            db.execute("BEGIN IMMEDIATE")
            ledger_version=db.execute("SELECT value FROM runtime_meta WHERE key='candidate_ledger_schema_version'").fetchone()
            if not ledger_version:
                prior_episodes=db.execute("SELECT COUNT(*) FROM episodes").fetchone()[0]
                prior_requests=db.execute("SELECT COUNT(*) FROM oj_requests").fetchone()[0]
                prior_slots=db.execute("SELECT COUNT(*) FROM candidate_slots").fetchone()[0]
                if prior_episodes or prior_requests or prior_slots:
                    raise N0Error("G3_AUTHORITY_LEDGER_REQUIRES_FRESH_ISOLATED_DATABASE")
                db.execute("INSERT INTO runtime_meta(key,value) VALUES('candidate_ledger_schema_version','G3_ATOMIC_QUEUE_LEDGER_V1')")
            elif ledger_version[0]!="G3_ATOMIC_QUEUE_LEDGER_V1":
                raise N0Error("CANDIDATE_LEDGER_SCHEMA_VERSION_MISMATCH")
            db.execute("COMMIT")
        finally:
            db.close()

    def register_episode(self, *, episode_id: str, replicate_slot: str, treatment_id: str,
                         approved_allocation: dict[str, Any], max_candidates: int,
                         workspace_files: dict[str, str | bytes]) -> EpisodePaths:
        """Register only a separately authorized slot; the pool never invents replicates."""
        if (not episode_id or not replicate_slot or treatment_id not in {"G3", "N0_AUTONOMOUS"}
                or approved_allocation.get("authorized") is not True
                or approved_allocation.get("episode_id") != episode_id
                or approved_allocation.get("replicate_slot") != replicate_slot
                or not isinstance(max_candidates, int) or isinstance(max_candidates, bool) or max_candidates != 30):
            raise N0Error("FORMAL_EPISODE_ALLOCATION_NOT_AUTHORIZED_OR_INVALID")
        if treatment_id == "G3" and approved_allocation.get("group") != "G3":
            raise N0Error("G3_EPISODE_ALLOCATION_GROUP_INVALID")
        required_names={"task_definition.json","candidate_abi.json","candidate.cu","visible_observation.json"}
        if not isinstance(workspace_files,dict) or not required_names <= set(workspace_files) or not set(workspace_files) <= {
            "task_definition.json","candidate_abi.json","candidate.cu","visible_observation.json"}:
            raise N0Error("EPISODE_INITIAL_WORKSPACE_INCOMPLETE_OR_UNALLOWLISTED")
        normalized={k:(v.encode("utf-8") if isinstance(v,str) else v) for k,v in workspace_files.items()}
        if any(not isinstance(v,bytes) for v in normalized.values()): raise N0Error("EPISODE_WORKSPACE_BYTES_INVALID")
        if (json.loads(normalized["task_definition.json"].decode("utf-8")).get("task_sha256", "").upper()!=N0_TASK_SHA256
                or sha256_bytes(normalized["task_definition.json"]).upper()!=N0_TASK_DEFINITION_SHA256):
            raise N0Error("TASK_DEFINITION_SHA_MISMATCH")
        try:
            abi=json.loads(normalized["candidate_abi.json"])
            json.loads(normalized["visible_observation.json"])
            if (abi.get("abi_id")!="N0_LAYERNORM_EXTENSION_ABI_V1"
                    or abi.get("compiler_contract_sha256")!=N0_COMPILER_CONTRACT_SHA256):
                raise N0Error("CANDIDATE_ABI_OR_COMPILER_CONTRACT_IDENTITY_MISMATCH")
            from .cuda_contract import validate_candidate_source
            validate_candidate_source(normalized["candidate.cu"].decode("utf-8"))
        except Exception as exc: raise N0Error("EPISODE_INITIAL_WORKSPACE_CONTENT_INVALID") from exc
        if sha256_bytes(normalized["candidate.cu"]).upper() != N0_SEED_SHA256:
            raise N0Error("COMMON_C0000_SEED_SHA_MISMATCH")
        allocation_bytes = canonical_bytes(approved_allocation)
        allocation_sha = sha256_bytes(allocation_bytes)
        db = self._connect()
        now = time.time()
        try:
            db.execute("BEGIN IMMEDIATE")
            if db.execute("SELECT 1 FROM runtime_meta WHERE key='science_batch_abort'").fetchone():
                raise N0Error("SCIENCE_BATCH_ABORTED_NO_NEW_EPISODE")
            existing = db.execute("SELECT * FROM episodes WHERE episode_id=? OR replicate_slot=?",
                                  (episode_id,replicate_slot)).fetchone()
            if existing:
                if (existing["episode_id"] != episode_id or existing["replicate_slot"] != replicate_slot
                        or existing["approved_allocation_sha256"] != allocation_sha):
                    raise N0Error("EPISODE_IDENTITY_CONFLICT")
            else:
                count = db.execute("SELECT COUNT(*) FROM episodes WHERE state IN ('ALLOCATED','RUNNING','WAITING_PROVIDER','WAITING_OJ')").fetchone()[0]
                if count >= self.max_active_episodes:
                    raise N0Error("MAX_ACTIVE_EPISODES_REACHED")
                db.execute("INSERT INTO episodes(episode_id,replicate_slot,treatment_id,state,approved_allocation_sha256,max_candidates,last_candidate_sha256,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)",
                    (episode_id,replicate_slot,treatment_id,"ALLOCATED",allocation_sha,max_candidates,N0_SEED_SHA256,now,now))
            paths=self.episode_paths(episode_id)
            for p in (paths.workspace,paths.archive,paths.checkpoint.parent,paths.ledger.parent,paths.summary.parent,paths.logs,paths.working_note.parent):
                p.mkdir(parents=True, exist_ok=True)
            for name,raw in normalized.items():
                target=paths.workspace/name
                if target.exists():
                    if target.read_bytes()!=raw: raise N0Error("EPISODE_WORKSPACE_ALREADY_EXISTS_WITH_DIFFERENT_BYTES")
                else:
                    with target.open("xb") as f:
                        f.write(raw); f.flush(); os.fsync(f.fileno())
            db.execute("COMMIT")
        except BaseException:
            if db.in_transaction: db.execute("ROLLBACK")
            raise
        finally:
            db.close()
        self.reconcile_episode_exports(episode_id)
        return paths

    def append_episode_event(self, episode_id: str, event: dict[str, Any], *, event_id: str | None = None) -> str:
        event_id=event_id or uuid.uuid4().hex
        record={**event,"event_id":event_id,"episode_id":episode_id,"created_at":time.time()}
        raw=canonical_bytes(record); db=self._connect()
        try:
            db.execute("INSERT OR IGNORE INTO episode_events(event_id,episode_id,event_json,created_at) VALUES(?,?,?,?)",
                       (event_id,episode_id,raw.decode("utf-8"),record["created_at"]))
            old=db.execute("SELECT event_json FROM episode_events WHERE event_id=?",(event_id,)).fetchone()
            if old[0] != raw.decode("utf-8"):
                raise N0Error("EPISODE_EVENT_IDEMPOTENCY_CONFLICT")
        finally: db.close()
        self.reconcile_episode_exports(episode_id)
        return event_id

    @staticmethod
    def _insert_event_tx(db: sqlite3.Connection, episode_id: str, event: dict[str, Any], event_id: str) -> None:
        record={**event,"event_id":event_id,"episode_id":episode_id,"created_at":time.time()}
        raw=canonical_bytes(record).decode("utf-8")
        db.execute("INSERT OR IGNORE INTO episode_events(event_id,episode_id,event_json,created_at) VALUES(?,?,?,?)",
                   (event_id,episode_id,raw,record["created_at"]))

    def reconcile_episode_exports(self, episode_id: str) -> int:
        """Idempotently materialize each episode's append-only ledger from durable events."""
        paths=self.episode_paths(episode_id); ledger=paths.ledger
        ledger.parent.mkdir(parents=True,exist_ok=True)
        existing=set()
        if ledger.exists():
            for line in ledger.read_text(encoding="utf-8").splitlines():
                if line.strip():
                    try: existing.add(json.loads(line)["event_id"])
                    except Exception as exc: raise N0Error("EPISODE_LEDGER_CORRUPT") from exc
        db=self._connect(); wrote=0
        try: rows=db.execute("SELECT event_id,event_json FROM episode_events WHERE episode_id=? ORDER BY created_at,event_id",(episode_id,)).fetchall()
        finally: db.close()
        for row in rows:
            if row["event_id"] in existing: continue
            with ledger.open("ab") as f:
                f.write(row["event_json"].encode("utf-8")+b"\n"); f.flush(); os.fsync(f.fileno())
            existing.add(row["event_id"]); wrote+=1
        return wrote

    def reconcile_all_episode_exports(self) -> int:
        db=self._connect()
        try: ids=[row[0] for row in db.execute("SELECT episode_id FROM episodes ORDER BY episode_id")]
        finally: db.close()
        return sum(self.reconcile_episode_exports(x) for x in ids)

    def episode_paths(self, episode_id: str) -> EpisodePaths:
        if not episode_id or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-" for c in episode_id):
            raise N0Error("EPISODE_ID_PATH_INVALID")
        base = (self.root / "episodes" / episode_id).resolve()
        if base.parent != (self.root / "episodes").resolve():
            raise N0Error("EPISODE_PATH_ESCAPE")
        return EpisodePaths(episode_id,base,base/"workspace",base/"archive",base/"checkpoint"/"state.json",
                           base/"ledger"/"ledger.jsonl",base/"summary"/"summary.json",base/"logs",
                           base/"working_note"/"working_notes.jsonl")

    def mark_running(self, episode_id: str, worker_id: str) -> None:
        db=self._connect()
        try:
            cur=db.execute("UPDATE episodes SET state='RUNNING',updated_at=? WHERE episode_id=? AND state IN ('ALLOCATED','RUNNING')",
                           (time.time(),episode_id))
            if cur.rowcount != 1: raise N0Error("EPISODE_NOT_ALLOCATED_OR_NOT_RUNNABLE")
        finally: db.close()

    def begin_provider_request(self, *, request_id: str, episode_id: str, invocation_id: str,
                               request_index: int, payload: bytes) -> dict[str, Any]:
        if request_index < 1: raise N0Error("PROVIDER_REQUEST_INDEX_INVALID")
        if not request_id or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-" for c in request_id):
            raise N0Error("PROVIDER_REQUEST_ID_INVALID")
        digest=sha256_bytes(payload); now=time.time(); db=self._connect()
        try:
            db.execute("BEGIN IMMEDIATE")
            if db.execute("SELECT 1 FROM runtime_meta WHERE key='science_batch_abort'").fetchone():
                raise N0Error("SCIENCE_BATCH_ABORTED_NO_NEW_PROVIDER_REQUEST")
            old=db.execute("SELECT * FROM provider_requests WHERE episode_id=? AND invocation_id=? AND request_index=?",
                           (episode_id,invocation_id,request_index)).fetchone()
            if old:
                if old["payload_sha256"] != digest or old["request_id"] != request_id:
                    raise N0Error("PROVIDER_REQUEST_IDEMPOTENCY_CONFLICT")
                if old["state"] != "QUEUED": raise N0Error("PROVIDER_REQUEST_ALREADY_DISPATCHED_OR_UNKNOWN")
                db.execute("COMMIT"); return dict(old)
            payload_path=self.episode_paths(episode_id).archive/"provider"/"requests"/(request_id+".body")
            payload_path.parent.mkdir(parents=True,exist_ok=True)
            if payload_path.exists():
                if payload_path.read_bytes()!=payload: raise N0Error("PROVIDER_PAYLOAD_IDENTITY_CONFLICT")
            else:
                with payload_path.open("xb") as f: f.write(payload); f.flush(); os.fsync(f.fileno())
            db.execute("INSERT INTO provider_requests(request_id,episode_id,invocation_id,request_index,payload_sha256,state,payload_path,created_at,updated_at) VALUES(?,?,?,?,?,'QUEUED',?,?,?)",
                       (request_id,episode_id,invocation_id,request_index,digest,payload_path.relative_to(self.root).as_posix(),now,now))
            db.execute("UPDATE episodes SET state='WAITING_PROVIDER',updated_at=? WHERE episode_id=?",(now,episode_id))
            self._insert_event_tx(db,episode_id,{"event_type":"PROVIDER_REQUEST_QUEUED","request_id":request_id,
                "invocation_id":invocation_id,"request_index":request_index,"payload_sha256":digest,
                "payload_path":payload_path.relative_to(self.root).as_posix()},"provider-queued:"+request_id)
            db.execute("COMMIT")
            self.reconcile_episode_exports(episode_id)
            return {"request_id":request_id,"episode_id":episode_id,"payload_sha256":digest,
                    "payload_path":payload_path.relative_to(self.root).as_posix(),"state":"QUEUED"}
        except BaseException:
            if db.in_transaction: db.execute("ROLLBACK")
            raise
        finally: db.close()

    def mark_provider_dispatched(self, request_id: str, attempt_id: str) -> None:
        db=self._connect()
        try:
            db.execute("BEGIN IMMEDIATE")
            if db.execute("SELECT 1 FROM runtime_meta WHERE key='science_batch_abort'").fetchone():
                raise N0Error("SCIENCE_BATCH_ABORTED_PROVIDER_NOT_DISPATCHED")
            row=db.execute("SELECT episode_id,payload_sha256,attempt_id FROM provider_requests WHERE request_id=? AND state='QUEUED'",(request_id,)).fetchone()
            if not row: raise N0Error("PROVIDER_REQUEST_NOT_QUEUED_OR_ALREADY_DISPATCHED")
            cur=db.execute("UPDATE provider_requests SET state='OUTCOME_UNKNOWN',attempt_id=?,updated_at=? WHERE request_id=? AND state='QUEUED'",
                           (attempt_id,time.time(),request_id))
            if cur.rowcount != 1: raise N0Error("PROVIDER_REQUEST_NOT_QUEUED_OR_ALREADY_DISPATCHED")
            self._insert_event_tx(db,row["episode_id"],{"event_type":"PROVIDER_ATTEMPT_DISPATCHED_OUTCOME_UNKNOWN",
                "request_id":request_id,"attempt_id":attempt_id,"payload_sha256":row["payload_sha256"]},"provider-attempt:"+request_id+":"+attempt_id)
            db.execute("COMMIT")
            self.reconcile_episode_exports(row["episode_id"])
        except BaseException:
            if db.in_transaction: db.execute("ROLLBACK")
            raise
        finally: db.close()

    def record_provider_response(self, request_id: str, attempt_id: str, response_id: str, body: bytes) -> str:
        digest=sha256_bytes(body); db=self._connect()
        try:
            db.execute("BEGIN IMMEDIATE")
            row=db.execute("SELECT episode_id FROM provider_requests WHERE request_id=? AND attempt_id=? AND state='OUTCOME_UNKNOWN'",
                           (request_id,attempt_id)).fetchone()
            if not row: raise N0Error("PROVIDER_RESPONSE_IDENTITY_OR_STATE_MISMATCH")
            try:
                doc=json.loads(body.decode("utf-8"))
            except Exception as exc: raise N0Error("PROVIDER_RESPONSE_BODY_INVALID") from exc
            if doc.get("id")!=response_id: raise N0Error("PROVIDER_RESPONSE_ID_MISMATCH")
            response_path=self.episode_paths(row["episode_id"]).archive/"provider"/"responses"/(request_id+"-"+response_id+".body")
            response_path.parent.mkdir(parents=True,exist_ok=True)
            if response_path.exists():
                if response_path.read_bytes()!=body: raise N0Error("PROVIDER_RESPONSE_IDENTITY_CONFLICT")
            else:
                with response_path.open("xb") as f: f.write(body); f.flush(); os.fsync(f.fileno())
            cur=db.execute("UPDATE provider_requests SET state='RESPONSE_RECEIVED',response_id=?,response_sha256=?,response_path=?,updated_at=? WHERE request_id=? AND attempt_id=? AND state='OUTCOME_UNKNOWN'",
                           (response_id,digest,response_path.relative_to(self.root).as_posix(),time.time(),request_id,attempt_id))
            if cur.rowcount != 1: raise N0Error("PROVIDER_RESPONSE_IDENTITY_OR_STATE_MISMATCH")
            self._insert_event_tx(db,row["episode_id"],{"event_type":"PROVIDER_RESPONSE_PERSISTED","request_id":request_id,
                "attempt_id":attempt_id,"response_id":response_id,"response_sha256":digest,
                "response_path":response_path.relative_to(self.root).as_posix()},"provider-response:"+request_id+":"+response_id)
            db.execute("COMMIT")
            self.reconcile_episode_exports(row["episode_id"])
        except BaseException:
            if db.in_transaction: db.execute("ROLLBACK")
            raise
        finally: db.close()
        return digest

    def consume_provider_response(self, request_id: str, response_id: str, response_sha256: str) -> bytes:
        db=self._connect()
        try:
            db.execute("BEGIN IMMEDIATE")
            row=db.execute("SELECT * FROM provider_requests WHERE request_id=?",(request_id,)).fetchone()
            if (row and row["state"]=="CONSUMED" and row["response_id"]==response_id
                    and row["response_sha256"]==response_sha256 and row["response_consumed_at"] is not None):
                raise N0Error("PROVIDER_RESPONSE_ALREADY_CONSUMED")
            if not row or row["state"]!="RESPONSE_RECEIVED" or row["response_id"]!=response_id or row["response_sha256"]!=response_sha256:
                raise N0Error("SAVED_PROVIDER_RESPONSE_IDENTITY_MISMATCH")
            if row["response_consumed_at"] is not None: raise N0Error("PROVIDER_RESPONSE_ALREADY_CONSUMED")
            path=self.root/Path(row["response_path"])
            body=path.read_bytes()
            if sha256_bytes(body)!=response_sha256: raise N0Error("SAVED_PROVIDER_RESPONSE_HASH_MISMATCH")
            db.execute("UPDATE provider_requests SET state='CONSUMED',response_consumed_at=?,updated_at=? WHERE request_id=? AND state='RESPONSE_RECEIVED'",
                       (time.time(),time.time(),request_id))
            self._insert_event_tx(db,row["episode_id"],{"event_type":"PROVIDER_RESPONSE_CONSUMED","request_id":request_id,
                "response_id":response_id,"response_sha256":response_sha256},"provider-consumed:"+request_id+":"+response_id)
            db.execute("UPDATE episodes SET state='RUNNING',updated_at=? WHERE episode_id=?",(time.time(),row["episode_id"]))
            db.execute("COMMIT")
            self.reconcile_episode_exports(row["episode_id"])
            return body
        except BaseException:
            if db.in_transaction: db.execute("ROLLBACK")
            raise
        finally: db.close()

    def record_provider_error_response(self, request_id: str, attempt_id: str, status_code: int, body: bytes) -> str:
        """Persist an HTTP error body against a dispatched attempt, never as a model response."""
        if not isinstance(status_code,int) or isinstance(status_code,bool) or not isinstance(body,bytes) or not body:
            raise N0Error("PROVIDER_ERROR_RESPONSE_INVALID")
        digest=sha256_bytes(body); db=self._connect()
        try:
            db.execute("BEGIN IMMEDIATE")
            row=db.execute("SELECT episode_id,state,response_sha256 FROM provider_requests WHERE request_id=? AND attempt_id=?",
                           (request_id,attempt_id)).fetchone()
            if not row: raise N0Error("PROVIDER_ERROR_REQUEST_ATTEMPT_MISMATCH")
            if row["state"]=="PROVIDER_HTTP_ERROR" and row["response_sha256"]==digest:
                db.execute("COMMIT"); return digest
            if row["state"]!="OUTCOME_UNKNOWN": raise N0Error("PROVIDER_ERROR_REQUEST_NOT_IN_FLIGHT")
            path=self.episode_paths(row["episode_id"]).archive/"provider"/"errors"/(request_id+"-"+attempt_id+".http.body")
            path.parent.mkdir(parents=True,exist_ok=True)
            if path.exists():
                if path.read_bytes()!=body: raise N0Error("PROVIDER_ERROR_BODY_IDENTITY_CONFLICT")
            else:
                with path.open("xb") as f: f.write(body); f.flush(); os.fsync(f.fileno())
            cur=db.execute("UPDATE provider_requests SET state='PROVIDER_HTTP_ERROR',response_sha256=?,response_path=?,updated_at=? WHERE request_id=? AND attempt_id=? AND state='OUTCOME_UNKNOWN'",
                           (digest,path.relative_to(self.root).as_posix(),time.time(),request_id,attempt_id))
            if cur.rowcount!=1: raise N0Error("PROVIDER_ERROR_REQUEST_STATE_RACE")
            self._insert_event_tx(db,row["episode_id"],{"event_type":"PROVIDER_HTTP_ERROR_PERSISTED","request_id":request_id,
                "attempt_id":attempt_id,"status_code":status_code,"error_body_sha256":digest,
                "error_body_path":path.relative_to(self.root).as_posix()},"provider-http-error:"+request_id+":"+attempt_id)
            db.execute("COMMIT"); self.reconcile_episode_exports(row["episode_id"]); return digest
        except BaseException:
            if db.in_transaction: db.execute("ROLLBACK")
            raise
        finally: db.close()

    def submit_continue(self, *, episode_id: str, invocation_id: str, current_source: str,
                        frozen_identity_rule: Callable[[str,str|None],bool] | None) -> dict[str, Any]:
        """At most one queued Candidate per CONTINUE invocation; no rule means fail closed."""
        if frozen_identity_rule is None: raise N0Error("CANDIDATE_IDENTITY_RULE_NOT_FROZEN")
        from .cuda_contract import validate_candidate_source
        validate_candidate_source(current_source)
        source_bytes=current_source.encode("utf-8"); source_sha=sha256_bytes(source_bytes); now=time.time(); db=self._connect()
        try:
            db.execute("BEGIN IMMEDIATE")
            if db.execute("SELECT 1 FROM runtime_meta WHERE key='science_batch_abort'").fetchone():
                raise N0Error("SCIENCE_BATCH_ABORTED_NO_NEW_CANDIDATE")
            existing=db.execute("SELECT * FROM oj_requests WHERE episode_id=? AND invocation_id=?",(episode_id,invocation_id)).fetchone()
            if existing:
                if existing["candidate_sha256"] != source_sha: raise N0Error("CONTINUE_RETRY_SOURCE_CONFLICT")
                db.execute("COMMIT")
                self.reconcile_episode_exports(episode_id)
                return dict(existing)
            ep=db.execute("SELECT * FROM episodes WHERE episode_id=?",(episode_id,)).fetchone()
            if not ep:
                raise N0Error("EPISODE_NOT_ELIGIBLE_FOR_CANDIDATE_COMMIT")
            unresolved=db.execute("SELECT 1 FROM candidate_slots WHERE episode_id=? AND status IN ('RESERVED','DISPATCHED','AWAITING_CONSUMPTION','OUTCOME_UNKNOWN','INFRASTRUCTURE_HOLD') LIMIT 1",(episode_id,)).fetchone()
            if unresolved:
                raise N0Error("PRIOR_CANDIDATE_RESULT_MUST_BE_RESOLVED_BEFORE_NEW_SUBMISSION")
            prior=db.execute("SELECT candidate_id,request_id,state FROM oj_requests WHERE episode_id=? AND candidate_sha256=? ORDER BY created_at LIMIT 1",
                             (episode_id,source_sha)).fetchone()
            if prior:
                db.execute("COMMIT")
                self.reconcile_episode_exports(episode_id)
                return {"status":"NO_NEW_LOGICAL_CANDIDATE","candidate_id":prior["candidate_id"],
                        "prior_request_id":prior["request_id"],"prior_state":prior["state"],"candidate_sha256":source_sha}
            if ep["candidates_used"] >= ep["max_candidates"]:
                raise N0Error("CANDIDATE_BUDGET_EXHAUSTED")
            if ep["state"] not in {"ALLOCATED","RUNNING","WAITING_PROVIDER"}:
                raise N0Error("EPISODE_NOT_ELIGIBLE_FOR_CANDIDATE_COMMIT")
            slot_rows=db.execute("SELECT candidate_id,status FROM candidate_slots WHERE episode_id=? ORDER BY candidate_id",(episode_id,)).fetchall()
            occupied=sum(1 for r in slot_rows if r["status"] in {"RESERVED","DISPATCHED","AWAITING_CONSUMPTION","OUTCOME_UNKNOWN","INFRASTRUCTURE_HOLD","SETTLED"})
            if occupied >= ep["max_candidates"]: raise N0Error("CANDIDATE_BUDGET_EXHAUSTED")
            if not frozen_identity_rule(source_sha,ep["last_candidate_sha256"]):
                db.execute("COMMIT"); return {"status":"NO_NEW_LOGICAL_CANDIDATE","candidate_sha256":source_sha}
            used_ids={r["candidate_id"] for r in slot_rows}
            next_n=1
            while f"C{next_n:04d}" in used_ids: next_n+=1
            cid=f"C{next_n:04d}"; rid="N0-OJ-"+uuid.uuid4().hex
            candidate_dir=self.episode_paths(episode_id).archive/"candidates"
            candidate_dir.mkdir(parents=True,exist_ok=True)
            candidate_path=candidate_dir/(cid+"-"+source_sha+".cu")
            if candidate_path.exists():
                if candidate_path.read_bytes()!=source_bytes: raise N0Error("IMMUTABLE_CANDIDATE_SOURCE_CONFLICT")
            else:
                with candidate_path.open("xb") as f:
                    f.write(source_bytes); f.flush(); os.fsync(f.fileno())
            seq=int(db.execute("SELECT value FROM runtime_meta WHERE key='queue_sequence'").fetchone()[0])+1
            db.execute("UPDATE runtime_meta SET value=? WHERE key='queue_sequence'",(str(seq),))
            db.execute("INSERT INTO oj_requests(request_id,episode_id,invocation_id,candidate_id,candidate_sha256,state,queue_sequence,candidate_source_path,created_at,updated_at) VALUES(?,?,?,?,?,'QUEUED',?,?,?,?)",
                       (rid,episode_id,invocation_id,cid,source_sha,seq,candidate_path.relative_to(self.root).as_posix(),now,now))
            db.execute("INSERT INTO candidate_slots(episode_id,candidate_id,request_id,candidate_sha256,status,created_at,updated_at) VALUES(?,?,?,?, 'RESERVED',?,?)",
                       (episode_id,cid,rid,source_sha,now,now))
            db.execute("UPDATE episodes SET last_candidate_sha256=?,state='WAITING_OJ',updated_at=? WHERE episode_id=?",
                       (source_sha,now,episode_id))
            result={"request_id":rid,"episode_id":episode_id,"invocation_id":invocation_id,"candidate_id":cid,
                    "candidate_sha256":source_sha,"candidate_source_path":candidate_path.relative_to(self.root).as_posix(),
                    "state":"QUEUED","queue_sequence":seq}
            self._insert_event_tx(db,episode_id,{"event_type":"CANDIDATE_SUBMITTED_TO_OJ_QUEUE",**result},
                                  "continue:"+episode_id+":"+invocation_id)
            db.execute("COMMIT")
            self.reconcile_episode_exports(episode_id)
            return result
        except BaseException:
            if db.in_transaction: db.execute("ROLLBACK")
            raise
        finally: db.close()

    def record_stop(self, *, episode_id: str, invocation_id: str, source_sha256: str, reason: str,
                    turn_id: str | None = None) -> None:
        db=self._connect(); now=time.time()
        try:
            db.execute("BEGIN IMMEDIATE")
            key=f"stop:{episode_id}:{invocation_id}"
            old=db.execute("SELECT value FROM runtime_meta WHERE key=?",(key,)).fetchone()
            value=json.dumps({"source_sha256":source_sha256,"reason":reason},sort_keys=True)
            if old and old[0] != value: raise N0Error("STOP_DECISION_IDEMPOTENCY_CONFLICT")
            db.execute("INSERT OR IGNORE INTO runtime_meta(key,value) VALUES(?,?)",(key,value))
            self._insert_event_tx(db,episode_id,{"event_type":"MODEL_STOP","invocation_id":invocation_id,
                "turn_id":turn_id,"source_sha256":source_sha256,"reason":reason},key)
            cur=db.execute("UPDATE episodes SET state='SCIENTIFICALLY_COMPLETED',updated_at=? WHERE episode_id=? AND state NOT IN ('SCIENTIFICALLY_COMPLETED','INFRASTRUCTURE_HOLD')",
                           (now,episode_id))
            if cur.rowcount!=1 and not old: raise N0Error("EPISODE_STOP_STATE_TRANSITION_INVALID")
            db.execute("COMMIT")
            self.reconcile_episode_exports(episode_id)
        except BaseException:
            if db.in_transaction: db.execute("ROLLBACK")
            raise
        finally: db.close()

    def request_stop_after_current_invocation(self, episode_id: str, *, reason: str) -> None:
        key="stop-request:"+episode_id
        db=self._connect()
        try:
            row=db.execute("SELECT value FROM runtime_meta WHERE key=?",(key,)).fetchone()
            if row:
                if json.loads(row[0]).get("reason")!=reason: raise N0Error("STOP_REQUEST_ALREADY_EXISTS")
                return
            value=json.dumps({"reason":reason,"requested_at":time.time()},sort_keys=True)
            db.execute("INSERT OR IGNORE INTO runtime_meta(key,value) VALUES(?,?)",(key,value))
        finally: db.close()

    def stop_requested(self, episode_id: str) -> bool:
        db=self._connect()
        try: return db.execute("SELECT 1 FROM runtime_meta WHERE key=?",("stop-request:"+episode_id,)).fetchone() is not None
        finally: db.close()

    def claim_next_oj(self, worker_id: str, *, worker_pid: int, worker_start_utc: str) -> dict[str, Any] | None:
        if not worker_id or not isinstance(worker_pid,int) or worker_pid<1 or not worker_start_utc:
            raise N0Error("OJ_WORKER_PROCESS_IDENTITY_REQUIRED")
        db=self._connect()
        try:
            db.execute("BEGIN IMMEDIATE")
            if db.execute("SELECT 1 FROM runtime_meta WHERE key='science_batch_abort'").fetchone():
                db.execute("COMMIT"); return None
            if db.execute("SELECT 1 FROM oj_requests WHERE state='RUNNING' LIMIT 1").fetchone():
                db.execute("COMMIT"); return None
            queued=db.execute("SELECT DISTINCT episode_id FROM oj_requests WHERE state='QUEUED' ORDER BY episode_id").fetchall()
            if not queued:
                db.execute("COMMIT"); return None
            episodes=[r[0] for r in queued]
            cursor=db.execute("SELECT value FROM runtime_meta WHERE key='round_robin_cursor'").fetchone()[0]
            next_ep=next((x for x in episodes if x>cursor),episodes[0])
            row=db.execute("SELECT * FROM oj_requests WHERE episode_id=? AND state='QUEUED' ORDER BY queue_sequence LIMIT 1",(next_ep,)).fetchone()
            lease=uuid.uuid4().hex; now=time.time()
            db.execute("UPDATE oj_requests SET state='RUNNING',worker_id=?,worker_pid=?,worker_start_utc=?,lease_id=?,claimed_at=?,updated_at=? WHERE request_id=? AND state='QUEUED'",
                       (worker_id,worker_pid,worker_start_utc,lease,now,now,row["request_id"]))
            cur=db.execute("UPDATE candidate_slots SET status='DISPATCHED',updated_at=? WHERE request_id=? AND status='RESERVED'",
                            (now,row["request_id"]))
            # C0000 is the shared, free seed. It is deliberately outside the
            # logical Candidate-slot ledger; allow only the frozen seed SHA
            # with no slot row. All billable Candidates still require the
            # RESERVED -> DISPATCHED transition in this transaction.
            seed_without_slot = (row["candidate_id"] == "C0000"
                                 and str(row["candidate_sha256"]).upper() == N0_SEED_SHA256
                                 and cur.rowcount == 0
                                 and db.execute("SELECT 1 FROM candidate_slots WHERE request_id=?",
                                                (row["request_id"],)).fetchone() is None)
            if cur.rowcount != 1 and not seed_without_slot:
                raise N0Error("CANDIDATE_SLOT_DISPATCH_STATE_MISMATCH")
            db.execute("UPDATE runtime_meta SET value=? WHERE key='round_robin_cursor'",(next_ep,))
            self._insert_event_tx(db,next_ep,{"event_type":"OJ_REQUEST_CLAIMED","request_id":row["request_id"],
                "worker_id":worker_id,"lease_id":lease,"candidate_id":row["candidate_id"],
                "candidate_sha256":row["candidate_sha256"],"queue_sequence":row["queue_sequence"]},
                "oj-claim:"+row["request_id"]+":"+lease)
            db.execute("COMMIT")
            claimed=dict(row); claimed.update(state="RUNNING",worker_id=worker_id,worker_pid=worker_pid,
                                               worker_start_utc=worker_start_utc,lease_id=lease)
            self.reconcile_episode_exports(next_ep)
            return claimed
        except BaseException:
            if db.in_transaction: db.execute("ROLLBACK")
            raise
        finally: db.close()

    def finish_oj(self, *, request_id: str, worker_id: str, lease_id: str,
                  receipt: dict[str, Any] | None, infrastructure_error: str | None = None) -> str:
        if receipt is None:
            state="HOLD_UNKNOWN"
            receipt_sha=None
            receipt_json=None
        else:
            state="COMPLETED"
            receipt_sha=sha256_bytes(canonical_bytes(receipt))
            receipt_json=canonical_bytes(receipt).decode("utf-8")
        db=self._connect()
        try:
            db.execute("BEGIN IMMEDIATE")
            row=db.execute("SELECT * FROM oj_requests WHERE request_id=? AND worker_id=? AND lease_id=? AND state='RUNNING'",
                           (request_id,worker_id,lease_id)).fetchone()
            if not row: raise N0Error("OJ_LEASE_OR_REQUEST_OWNERSHIP_MISMATCH")
            if receipt is not None and any(receipt.get(k)!=row[db_key] for k,db_key in (
                ("request_id","request_id"),("episode_id","episode_id"),
                ("candidate_id","candidate_id"),("candidate_sha256","candidate_sha256"))):
                state="HOLD_UNKNOWN"
                infrastructure_error="OJ_RECEIPT_IDENTITY_MISMATCH"
            cur=db.execute("UPDATE oj_requests SET state=?,receipt_sha256=?,receipt_json=?,error=?,completed_at=?,updated_at=? WHERE request_id=? AND worker_id=? AND lease_id=? AND state='RUNNING'",
                           (state,receipt_sha,receipt_json,infrastructure_error,time.time(),time.time(),request_id,worker_id,lease_id))
            if cur.rowcount != 1: raise N0Error("OJ_LEASE_OR_REQUEST_OWNERSHIP_MISMATCH")
            slot_state=("OUTCOME_UNKNOWN" if state=="HOLD_UNKNOWN" else
                        ("INFRASTRUCTURE_HOLD" if infrastructure_error else "AWAITING_CONSUMPTION"))
            cur=db.execute("UPDATE candidate_slots SET status=?,verdict=?,receipt_sha256=?,updated_at=? WHERE request_id=? AND status='DISPATCHED'",
                           (slot_state, (receipt or {}).get("verdict") or (receipt or {}).get("status"), receipt_sha, time.time(), request_id))
            seed_without_slot = (row["candidate_id"] == "C0000"
                                 and str(row["candidate_sha256"]).upper() == N0_SEED_SHA256
                                 and cur.rowcount == 0
                                 and db.execute("SELECT 1 FROM candidate_slots WHERE request_id=?",
                                                (request_id,)).fetchone() is None)
            if cur.rowcount != 1 and not seed_without_slot:
                raise N0Error("CANDIDATE_SLOT_FINISH_STATE_MISMATCH")
            db.execute("UPDATE episodes SET state='RUNNING',updated_at=? WHERE episode_id=?",(time.time(),row["episode_id"]))
            if infrastructure_error:
                db.execute("INSERT OR REPLACE INTO runtime_meta(key,value) VALUES(?,?)",
                           ("oj-error:"+request_id,infrastructure_error))
            self._insert_event_tx(db,row["episode_id"],{"event_type":"OJ_REQUEST_SETTLED","request_id":request_id,
                "worker_id":worker_id,"lease_id":lease_id,"state":state,"receipt_sha256":receipt_sha,
                "trusted_receipt":state=="COMPLETED","error":infrastructure_error},"oj-settle:"+request_id)
            db.execute("COMMIT")
        except BaseException:
            if db.in_transaction: db.execute("ROLLBACK")
            raise
        finally: db.close()
        self.reconcile_episode_exports(row["episode_id"])
        return state

    def recover_unknown_inflight(self, owner_state: Callable[[dict[str, Any]], str] | None = None) -> int:
        """Only verified-dead owners move RUNNING to HOLD; never requeue ambiguous GPU work."""
        if not callable(owner_state): raise N0Error("OJ_OWNER_LIVENESS_CHECK_REQUIRED")
        db=self._connect(); changed=[]
        try:
            db.execute("BEGIN IMMEDIATE")
            rows=[dict(x) for x in db.execute("SELECT * FROM oj_requests WHERE state='RUNNING'")]
            for row in rows:
                status=owner_state(row)
                if status not in {"ALIVE","DEAD","UNKNOWN"}: raise N0Error("OJ_OWNER_LIVENESS_RESULT_INVALID")
                if status!="DEAD": continue
                db.execute("UPDATE oj_requests SET state='HOLD_UNKNOWN',error='OWNER_VERIFIED_DEAD_RESULT_UNKNOWN',updated_at=? WHERE request_id=? AND state='RUNNING'",
                           (time.time(),row["request_id"]))
                db.execute("UPDATE candidate_slots SET status='OUTCOME_UNKNOWN',updated_at=? WHERE request_id=? AND status='DISPATCHED'",
                           (time.time(),row["request_id"]))
                self._insert_event_tx(db,row["episode_id"],{"event_type":"OJ_CRASH_RECOVERY_HOLD",
                    "request_id":row["request_id"],"worker_id":row["worker_id"],"lease_id":row["lease_id"],
                    "candidate_sha256":row["candidate_sha256"],"prior_state":"RUNNING",
                    "recovery":"HOLD_UNKNOWN_NO_REPLAY"},"oj-crash-hold:"+row["request_id"])
                changed.append(row["episode_id"])
            db.execute("COMMIT")
        except BaseException:
            if db.in_transaction: db.execute("ROLLBACK")
            raise
        finally: db.close()
        for ep in set(changed): self.reconcile_episode_exports(ep)
        return len(changed)

    def consume_oj_result(self, request_id: str, expected_receipt_sha256: str) -> dict[str, Any]:
        """Return one identity-verified OJ receipt and atomically mark it consumed."""
        db=self._connect()
        try:
            db.execute("BEGIN IMMEDIATE")
            row=db.execute("SELECT * FROM oj_requests WHERE request_id=?",(request_id,)).fetchone()
            if not row or row["state"]!="COMPLETED" or not row["receipt_json"]:
                raise N0Error("OJ_RESULT_NOT_COMPLETED_OR_MISSING")
            if row["receipt_consumed_at"] is not None:
                raise N0Error("OJ_RESULT_ALREADY_CONSUMED")
            if row["receipt_sha256"] != expected_receipt_sha256:
                raise N0Error("OJ_RESULT_RECEIPT_SHA_MISMATCH")
            receipt=json.loads(row["receipt_json"])
            if (receipt.get("request_id")!=row["request_id"] or receipt.get("episode_id")!=row["episode_id"]
                    or receipt.get("candidate_id")!=row["candidate_id"]
                    or receipt.get("candidate_sha256")!=row["candidate_sha256"]
                    or sha256_bytes(canonical_bytes(receipt))!=expected_receipt_sha256):
                raise N0Error("OJ_RESULT_IDENTITY_OR_CONTENT_MISMATCH")
            verdict=str(receipt.get("verdict") or receipt.get("status") or "")
            slot=db.execute("SELECT * FROM candidate_slots WHERE request_id=?",(request_id,)).fetchone()
            if not slot or slot["candidate_sha256"]!=row["candidate_sha256"] or slot["candidate_id"]!=row["candidate_id"]:
                raise N0Error("CANDIDATE_SLOT_RECEIPT_IDENTITY_MISMATCH")
            if row["error"] or verdict.startswith("OJ_INFRASTRUCTURE_HOLD"):
                db.execute("UPDATE candidate_slots SET status='INFRASTRUCTURE_HOLD',verdict=?,receipt_sha256=?,updated_at=? WHERE request_id=?",
                           (verdict,row["receipt_sha256"],time.time(),request_id))
                db.execute("COMMIT")
                return {"status":"INFRASTRUCTURE_HOLD","request_id":request_id,
                        "candidate_id":row["candidate_id"],"candidate_sha256":row["candidate_sha256"],
                        "receipt_sha256":row["receipt_sha256"],"budget_settled":False}
            if not (verdict=="VALID_MEASUREMENT" or verdict.startswith("CANDIDATE_") or verdict in {"CUDA_RUNTIME_ERROR","CUDA_LAUNCH_ERROR","CUDA_EXECUTION_ERROR"}):
                db.execute("UPDATE candidate_slots SET status='OUTCOME_UNKNOWN',verdict=?,updated_at=? WHERE request_id=?",
                           (verdict,time.time(),request_id))
                db.execute("COMMIT")
                return {"status":"OUTCOME_UNKNOWN","request_id":request_id,
                        "candidate_id":row["candidate_id"],"candidate_sha256":row["candidate_sha256"],
                        "receipt_sha256":row["receipt_sha256"],"budget_settled":False}
            now=time.time()
            cur=db.execute("UPDATE oj_requests SET state='CONSUMED',receipt_consumed_at=?,updated_at=? WHERE request_id=? AND state='COMPLETED' AND receipt_consumed_at IS NULL",
                (now,now,request_id))
            if cur.rowcount!=1: raise N0Error("OJ_RESULT_CONSUMPTION_RACE")
            cur=db.execute("UPDATE candidate_slots SET status='SETTLED',verdict=?,receipt_sha256=?,updated_at=? WHERE request_id=? AND status='AWAITING_CONSUMPTION'",
                           (verdict,expected_receipt_sha256,now,request_id))
            if cur.rowcount!=1: raise N0Error("CANDIDATE_SLOT_SETTLEMENT_RACE")
            self._insert_event_tx(db,row["episode_id"],{"event_type":"OJ_RESULT_CONSUMED",
                "request_id":request_id,"candidate_id":row["candidate_id"],"candidate_sha256":row["candidate_sha256"],
                "receipt_sha256":expected_receipt_sha256,"receipt":receipt},"oj-result-consumed:"+request_id)
            db.execute("UPDATE episodes SET state='RUNNING',updated_at=? WHERE episode_id=?",(now,row["episode_id"]))
            cur=db.execute("UPDATE episodes SET candidates_used=candidates_used+1 WHERE episode_id=? AND candidates_used < max_candidates",
                           (row["episode_id"],))
            if cur.rowcount!=1: raise N0Error("CANDIDATE_BUDGET_SETTLEMENT_LIMIT")
            updated=db.execute("SELECT candidates_used,max_candidates FROM episodes WHERE episode_id=?",
                               (row["episode_id"],)).fetchone()
            if updated["candidates_used"] == updated["max_candidates"]:
                cur=db.execute("UPDATE episodes SET state='SCIENTIFICALLY_COMPLETED',updated_at=? WHERE episode_id=? AND state='RUNNING'",
                               (now,row["episode_id"]))
                if cur.rowcount!=1: raise N0Error("EPISODE_EXTERNAL_CUTOFF_STATE_TRANSITION_INVALID")
                self._insert_event_tx(db,row["episode_id"],{"event_type":"EXTERNAL_CANDIDATE_CUTOFF",
                    "request_id":request_id,"candidate_id":row["candidate_id"],
                    "candidates_used":updated["candidates_used"],"max_candidates":updated["max_candidates"],
                    "provider_summary_requested":False},"external-cutoff:"+row["episode_id"])
            db.execute("COMMIT")
            self.reconcile_episode_exports(row["episode_id"])
            return receipt
        except BaseException:
            if db.in_transaction: db.execute("ROLLBACK")
            raise
        finally: db.close()

    def snapshot(self) -> dict[str, Any]:
        db=self._connect()
        try:
            return {"episodes":[dict(x) for x in db.execute("SELECT * FROM episodes ORDER BY episode_id")],
                    "oj_requests":[dict(x) for x in db.execute("SELECT * FROM oj_requests ORDER BY queue_sequence")],
                    "candidate_slots":[dict(x) for x in db.execute("SELECT * FROM candidate_slots ORDER BY episode_id,candidate_id")],
                    "provider_requests":[dict(x) for x in db.execute("SELECT * FROM provider_requests ORDER BY created_at")],
                    "queue_depth":db.execute("SELECT COUNT(*) FROM oj_requests WHERE state='QUEUED'").fetchone()[0],
                    "gpu_active_jobs":db.execute("SELECT COUNT(*) FROM oj_requests WHERE state='RUNNING'").fetchone()[0],
                    "oj_queue_wait_seconds":{"mean":(lambda xs:sum(xs)/len(xs) if xs else 0.0)([max(0.0,(x["claimed_at"] or x["updated_at"])-x["created_at"]) for x in db.execute("SELECT created_at,claimed_at,updated_at FROM oj_requests WHERE claimed_at IS NOT NULL")]),
                                               "max":max([max(0.0,(x["claimed_at"] or x["updated_at"])-x["created_at"]) for x in db.execute("SELECT created_at,claimed_at,updated_at FROM oj_requests WHERE claimed_at IS NOT NULL")],default=0.0)}}
        finally: db.close()


@dataclass
class EpisodeWorkerContext:
    episode_id: str
    worker_id: str
    workspace: Path
    archive: Path
    checkpoint: Path
    ledger: Path
    summary: Path
    working_note: Path
    provider_messages: list[dict[str, Any]]
    tool_call_ids: dict[str, Any]
    candidate_budget: dict[str, int]
    metadata: dict[str, Any]


class FourEpisodeWorkerPool:
    """Independent async worker slots; allocation is supplied externally."""
    def __init__(self, contexts: list[EpisodeWorkerContext], *, store: N0PersistentQueue | None = None):
        if len(contexts) not in {4, 10, 20} or len({x.episode_id for x in contexts}) != len(contexts):
            raise N0Error("POOL_REQUIRES_4_10_OR_20_DISTINCT_AUTHORIZED_EPISODE_CONTEXTS")
        owned_paths=[]
        for x in contexts:
            owned_paths.extend(str(p.resolve()) for p in (x.workspace,x.archive,x.checkpoint,x.ledger,x.summary,x.working_note))
        if len(set(owned_paths)) != len(owned_paths): raise N0Error("EPISODE_STATE_PATHS_MUST_BE_DISTINCT")
        if (len({id(x.provider_messages) for x in contexts})!=len(contexts) or len({id(x.metadata) for x in contexts})!=len(contexts)
                or len({id(x.tool_call_ids) for x in contexts})!=len(contexts) or len({id(x.candidate_budget) for x in contexts})!=len(contexts)):
            raise N0Error("EPISODE_IN_MEMORY_STATE_MUST_BE_DISTINCT")
        self.contexts=contexts
        self.store=store
        self.active=0; self.max_active=0; self.completed=0
        self.provider_active=0; self.max_provider_active=0; self.provider_latencies=[]
        self.oj_execution_latencies=[]; self.gpu_busy_seconds=0.0
        self._sem=asyncio.Semaphore(len(contexts))

    def provider_enter(self) -> float:
        self.provider_active+=1; self.max_provider_active=max(self.max_provider_active,self.provider_active)
        return time.perf_counter()

    def provider_exit(self, started_at: float) -> None:
        self.provider_active=max(0,self.provider_active-1)
        self.provider_latencies.append(max(0.0,time.perf_counter()-started_at))

    def record_oj_execution(self, seconds: float, *, gpu_busy_seconds: float | None = None) -> None:
        self.oj_execution_latencies.append(max(0.0,seconds))
        if gpu_busy_seconds is not None: self.gpu_busy_seconds+=max(0.0,gpu_busy_seconds)

    def metrics(self) -> dict[str, Any]:
        def summary(xs):
            ordered=sorted(xs)
            if not ordered:return {"count":0,"mean":0.0,"max":0.0}
            return {"count":len(ordered),"mean":sum(ordered)/len(ordered),"max":ordered[-1]}
        return {"active_agents":self.active,"max_simultaneous_agents":self.max_active,
                "active_provider_requests":self.provider_active,"max_simultaneous_provider_requests":self.max_provider_active,
                "provider_elapsed_seconds":summary(self.provider_latencies),
                "oj_execution_seconds":summary(self.oj_execution_latencies),
                "gpu_busy_seconds_observed":self.gpu_busy_seconds}

    async def run_round(self, worker: Callable[[EpisodeWorkerContext], Awaitable[Any]]) -> dict[str, Any]:
        start=time.perf_counter()
        async def one(ctx: EpisodeWorkerContext):
            async with self._sem:
                if self.store is not None and self.store.stop_requested(ctx.episode_id):
                    return {"episode_id":ctx.episode_id,"status":"STOP_REQUESTED_AFTER_PRIOR_BOUNDARY"}
                self.active+=1; self.max_active=max(self.max_active,self.active)
                try:
                    result=await worker(ctx)
                    self.completed+=1
                    return {"episode_id":ctx.episode_id,"result":result}
                finally:
                    self.active-=1
        results=await asyncio.gather(*(one(ctx) for ctx in self.contexts),return_exceptions=True)
        return {"results":results,"worker_count":len(self.contexts),"max_simultaneous_agents":self.max_active,
                "wall_seconds":time.perf_counter()-start,"completed_workers":self.completed,
                "metrics":self.metrics()}


def make_worker_contexts(store: N0PersistentQueue, episode_ids: list[str]) -> list[EpisodeWorkerContext]:
    if len(episode_ids) not in {4, 10, 20} or len(set(episode_ids))!=len(episode_ids):
        raise N0Error("POOL_REQUIRES_4_10_OR_20_DISTINCT_REGISTERED_EPISODES")
    contexts=[]
    registered={x["episode_id"]:x for x in store.snapshot()["episodes"]}
    for i,episode_id in enumerate(episode_ids):
        if episode_id not in registered: raise N0Error("WORKER_EPISODE_NOT_REGISTERED")
        p=store.episode_paths(episode_id)
        ep=registered[episode_id]
        contexts.append(EpisodeWorkerContext(episode_id,f"N0-WORKER-{i+1}",p.workspace,p.archive,p.checkpoint,
            p.ledger,p.summary,p.working_note,[],{},
            {"used":ep["candidates_used"],"max":ep["max_candidates"],
             "remaining":ep["max_candidates"]-ep["candidates_used"]},{}))
    return contexts


class InvocationFinalizer:
    """Records STOP without OJ, or commits only the last current source once on CONTINUE."""
    def __init__(self, store: N0PersistentQueue, workspace, *, frozen_identity_rule=None):
        self.store=store; self.workspace=workspace; self.identity_rule=frozen_identity_rule

    def __call__(self, *, episode_id: str, invocation_id: str, turn_id: str,
                 decision: str, reason: str) -> dict[str, Any]:
        source=self.workspace.required_context()["candidate.cu"]
        source_sha=sha256_bytes(source.encode("utf-8"))
        if decision=="STOP":
            self.store.record_stop(episode_id=episode_id,invocation_id=invocation_id,source_sha256=source_sha,reason=reason,turn_id=turn_id)
            return {"status":"STOP_RECORDED","source_sha256":source_sha,"oj_submitted":False}
        if decision!="CONTINUE": raise N0Error("FINAL_DECISION_INVALID")
        return self.store.submit_continue(episode_id=episode_id,invocation_id=invocation_id,
                                          current_source=source,frozen_identity_rule=self.identity_rule)
