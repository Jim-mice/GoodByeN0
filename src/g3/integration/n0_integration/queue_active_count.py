from __future__ import annotations
import sqlite3, sys
from pathlib import Path

def main()->None:
    path=Path(sys.argv[1]).resolve()
    if not path.is_file(): raise SystemExit("QUEUE_DATABASE_MISSING")
    db=sqlite3.connect(f"file:{path}?mode=ro",uri=True)
    try:
        row=db.execute("SELECT COUNT(*) FROM oj_requests WHERE state IN (?,?)",("QUEUED","RUNNING")).fetchone()
        if row is None: raise SystemExit("QUEUE_COUNT_QUERY_FAILED")
        print(int(row[0]))
    finally: db.close()

if __name__=="__main__": main()
