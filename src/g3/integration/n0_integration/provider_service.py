from __future__ import annotations
import hashlib, json, subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _cim_process(pid:int)->dict[str,Any]|None:
    command=(f"$p=Get-CimInstance Win32_Process -Filter 'ProcessId={int(pid)}'; "
             "if ($null -eq $p) { '{}' } else { $p | Select-Object ProcessId,ParentProcessId,CreationDate,ExecutablePath,CommandLine | ConvertTo-Json -Compress }")
    p=subprocess.run(["powershell.exe","-NoProfile","-Command",command],capture_output=True,text=True,timeout=12)
    if p.returncode: raise RuntimeError("CIM_QUERY_FAILED:"+p.stderr[-500:])
    value=json.loads(p.stdout.strip() or "{}")
    return value or None


def snapshot_r20_service(*, queue_root:Path, release_root:Path, manifest_sha256:str,
                         output_path:Path, heartbeat_max_age_seconds:float=15.0)->dict[str,Any]:
    root=Path(queue_root).resolve(); release=Path(release_root).resolve()
    status_path=root/"runtime"/"dispatcher_status.json"
    result={"schema":"N0_R20_SERVICE_SNAPSHOT_V1","captured_at_utc":datetime.now(timezone.utc).isoformat(),
            "queue_root":str(root),"release_id":None,"manifest_sha256":manifest_sha256.lower(),
            "status":"BLOCKED","processes":[]}
    try:
        if str(root.drive).upper()!="D:": raise ValueError("QUEUE_ROOT_NOT_ON_D")
        raw=(release/"R20_PROVIDER_RELEASE_MANIFEST.json").read_bytes()
        if hashlib.sha256(raw).hexdigest()!=manifest_sha256.lower(): raise ValueError("R20_MANIFEST_SHA_MISMATCH")
        manifest=json.loads(raw.decode("utf-8"))
        result["release_id"] = manifest.get("release_id")
        for rel,want in manifest.get("source_sha256",{}).items():
            f=(release/rel).resolve(); f.relative_to(release)
            if not f.is_file() or hashlib.sha256(f.read_bytes()).hexdigest()!=str(want).lower():
                raise ValueError("R20_SOURCE_SHA_MISMATCH:"+rel)
        d=json.loads(status_path.read_text(encoding="utf-8"))
        result["dispatcher_status"]={"state":d.get("state"),"heartbeat_utc":d.get("heartbeat_utc")}
        if d.get("release_manifest_sha256")!=manifest_sha256.lower() or Path(d.get("release_source_root","")).resolve()!=release:
            raise ValueError("DISPATCHER_RELEASE_IDENTITY_MISMATCH")
        if d.get("state")!="RUNNING": raise ValueError("DISPATCHER_NOT_RUNNING")
        age=(datetime.now(timezone.utc)-datetime.fromisoformat(d["heartbeat_utc"].replace("Z","+00:00"))).total_seconds()
        result["heartbeat_age_seconds"]=age
        if age<0 or age>heartbeat_max_age_seconds: raise ValueError("DISPATCHER_HEARTBEAT_STALE")
        expected_pids={"dispatcher":d.get("pid"),**{"worker_"+k:v for k,v in (d.get("worker_pids") or {}).items()}}
        for name,pid in expected_pids.items():
            if not isinstance(pid,int) or pid<=0: raise ValueError("SERVICE_PID_MISSING:"+name)
            proc=_cim_process(pid)
            if not proc: raise ValueError("SERVICE_PID_NOT_LIVE:"+name)
            cmd=str(proc.get("CommandLine") or "")
            if ("provider_queue_worker.py" not in cmd or str(root).casefold() not in cmd.casefold()
                    or str(release).casefold() not in cmd.casefold()):
                raise ValueError("SERVICE_PROCESS_COMMAND_MISMATCH:"+name)
            if name=="dispatcher" and "--window" in cmd: raise ValueError("DISPATCHER_IS_LANE_WORKER")
            if name.startswith("worker_"):
                lane=name[-1]
                if "--window "+lane not in cmd: raise ValueError("LANE_PROCESS_NOT_WORKER")
                if int(proc.get("ParentProcessId",0))!=int(d.get("pid",-1)): raise ValueError("LANE_PARENT_MISMATCH")
                lane_state=(d.get("windows") or {}).get(lane) or {}
                try: lane_age=(datetime.now(timezone.utc)-datetime.fromisoformat(lane_state["heartbeat_utc"].replace("Z","+00:00"))).total_seconds()
                except Exception: raise ValueError("LANE_HEARTBEAT_INVALID")
                if lane_age<0 or lane_age>heartbeat_max_age_seconds: raise ValueError("LANE_HEARTBEAT_STALE")
            result["processes"].append({"role":name,"pid":pid,"parent_pid":proc.get("ParentProcessId"),
                                        "creation_date":proc.get("CreationDate"),"command_line":cmd})
        if len(result["processes"])!=5: raise ValueError("SERVICE_PROCESS_COUNT_MISMATCH")
        result["launch_generation"]=str(d.get("pid"))+":"+str(result["processes"][0].get("creation_date"))
        result["pid"]=d.get("pid"); result["process_creation_date"]=result["processes"][0].get("creation_date")
        result["heartbeat_utc"]=d.get("heartbeat_utc")
        result["status"]="PASS"
    except Exception as exc:
        result["status"]="BLOCKED"; result["reason"]=type(exc).__name__+":"+str(exc)
    output_path=Path(output_path); output_path.parent.mkdir(parents=True,exist_ok=True)
    tmp=output_path.with_name(output_path.name+".tmp")
    tmp.write_text(json.dumps(result,ensure_ascii=False,sort_keys=True,indent=2)+"\n",encoding="utf-8")
    tmp.replace(output_path)
    return result
