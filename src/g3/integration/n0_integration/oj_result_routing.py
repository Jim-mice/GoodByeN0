from __future__ import annotations

import json, time

INFRA_PREFIXES = ("OJ_INFRASTRUCTURE_HOLD", "INFRASTRUCTURE_HOLD", "HOLD_UNKNOWN")
AGENT_VISIBLE_VERDICTS = {"VALID_MEASUREMENT", "CANDIDATE_COMPILE_FAILURE", "CANDIDATE_CORRECTNESS_FAILURE", "INVALID_SUBMISSION"}


def is_infrastructure_hold(receipt: dict) -> bool:
    if not isinstance(receipt, dict):
        return True
    verdict = str(receipt.get("verdict") or receipt.get("status") or "")
    return verdict not in AGENT_VISIBLE_VERDICTS or verdict.startswith(INFRA_PREFIXES) or receipt.get("trusted_measurement") is False


def route_oj_receipt(store, request_id: str) -> dict:
    """Keep evaluator Holds out of Agent feedback and append an Episode-level HOLD event."""
    db = store._connect()
    try:
        row = db.execute("SELECT * FROM oj_requests WHERE request_id=?", (request_id,)).fetchone()
        if not row or row["state"] not in {"COMPLETED", "CONSUMED"} or not row["receipt_json"] or not row["receipt_sha256"]:
            raise ValueError("OJ_RECEIPT_NOT_DURABLY_SETTLED")
        receipt = json.loads(row["receipt_json"])
        if is_infrastructure_hold(receipt):
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM oj_requests WHERE request_id=?", (request_id,)).fetchone()
            if not row or row["state"] not in {"COMPLETED", "CONSUMED"} or row["receipt_sha256"] is None:
                raise ValueError("OJ_HOLD_RECEIPT_IDENTITY_CHANGED")
            receipt = json.loads(row["receipt_json"])
            episode = db.execute("SELECT state FROM episodes WHERE episode_id=?", (row["episode_id"],)).fetchone()
            if not episode:
                raise ValueError("OJ_HOLD_EPISODE_MISSING")
            if episode["state"] not in {"INFRASTRUCTURE_HOLD", "SCIENTIFICALLY_COMPLETED"}:
                db.execute("UPDATE episodes SET state='INFRASTRUCTURE_HOLD',updated_at=? WHERE episode_id=?", (time.time(), row["episode_id"]))
            elif episode["state"] == "SCIENTIFICALLY_COMPLETED":
                raise ValueError("OJ_HOLD_AFTER_SCIENTIFIC_COMPLETION")
            event = {"event_type": "OJ_INFRASTRUCTURE_HOLD_PROPAGATED", "episode_id": row["episode_id"], "request_id": request_id,
                     "candidate_id": row["candidate_id"], "candidate_sha256": row["candidate_sha256"],
                     "receipt_sha256": row["receipt_sha256"], "verdict": receipt.get("verdict"),
                     "agent_feedback_allowed": False, "receipt_preserved": True}
            store._insert_event_tx(db, row["episode_id"], event, "oj-infra-hold:" + request_id)
            db.execute("COMMIT")
            store.reconcile_episode_exports(row["episode_id"])
            return {"state": "INFRASTRUCTURE_HOLD", "request_id": request_id, "receipt_sha256": row["receipt_sha256"],
                    "agent_feedback_allowed": False, "receipt": receipt}
        db.close()
        db = None
        if row["state"] == "COMPLETED":
            receipt = store.consume_oj_result(request_id, row["receipt_sha256"])
        else:
            receipt = json.loads(row["receipt_json"])
        return {"state": "CONSUMED", "request_id": request_id, "receipt_sha256": row["receipt_sha256"],
                "agent_feedback_allowed": True, "receipt": receipt}
    except BaseException:
        if db is not None and db.in_transaction:
            db.execute("ROLLBACK")
        raise
    finally:
        if db is not None:
            db.close()
