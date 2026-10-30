from __future__ import annotations
import argparse,ctypes,hashlib,json,os,subprocess,sys,time,uuid
import sqlite3
from datetime import datetime,timezone
from pathlib import Path
ROOT=Path(r"D:\projects\kernelagent")
R=ROOT/"runtime"
RELEASE=R/"deployment/releases/N0_KERNEL_AGENT_POOL20_R8_18_9_R8_03_WORKER_RELEASE_BINDING_FIX_20261027"
RELEASE_ID="N0_KERNEL_AGENT_POOL20_R8_18_9_R8_03_WORKER_RELEASE_BINDING_FIX_20261027"
MANAGER=RELEASE/"pool20_episode_manager.py"
PY=R/".venv_n0_r6/Scripts/python.exe"
RUNS=R/"state/n0_pool20_runs_r10"
LEASE_PATH=R/"state/n0_gpu_device.lock"
def utc():return datetime.now(timezone.utc).isoformat()
def process_command_line():
    if os.name=="nt":
        kernel=ctypes.WinDLL("kernel32",use_last_error=True)
        kernel.GetCommandLineW.restype=ctypes.c_wchar_p
        return kernel.GetCommandLineW()
    return " ".join([sys.executable,*sys.argv])
def write(path,obj,expected_launch_id=None):
    if expected_launch_id and path.name.startswith("W") and path.suffix==".json" and path.parent.name=="workers":
        if path.is_file():
            current=json.loads(path.read_text(encoding="utf-8"))
            if current.get("launch_id")!=expected_launch_id:
                raise RuntimeError("WORKER_REGISTRY_LAUNCH_SUPERSEDED")
    path.parent.mkdir(parents=True,exist_ok=True);tmp=path.with_name(path.name+"."+uuid.uuid4().hex+".tmp")
    tmp.write_text(json.dumps(obj,indent=2,ensure_ascii=False)+"\n",encoding="utf-8")
    for attempt in range(5):
        try:
            os.replace(tmp,path);return
        except PermissionError as exc:
            winerror=getattr(exc,"winerror",None)
            if winerror not in {5,32,33} or attempt==4:raise
            time.sleep(.025*(attempt+1))
def pid_is_live(pid:int)->bool:
    if pid<=0:return False
    if os.name=="nt":
        from ctypes import wintypes
        kernel=ctypes.WinDLL("kernel32",use_last_error=True)
        kernel.OpenProcess.argtypes=[wintypes.DWORD,wintypes.BOOL,wintypes.DWORD]
        kernel.OpenProcess.restype=wintypes.HANDLE
        kernel.GetExitCodeProcess.argtypes=[wintypes.HANDLE,ctypes.POINTER(wintypes.DWORD)]
        kernel.GetExitCodeProcess.restype=wintypes.BOOL
        kernel.CloseHandle.argtypes=[wintypes.HANDLE]
        kernel.CloseHandle.restype=wintypes.BOOL
        handle=kernel.OpenProcess(0x00100000|0x1000,False,pid)
        if not handle:
            err=ctypes.get_last_error()
            if err in (87,1168):return False
            raise RuntimeError("WORKER_LIVENESS_UNVERIFIABLE:WIN32_ERROR_"+str(err))
        try:
            code=wintypes.DWORD()
            if not kernel.GetExitCodeProcess(handle,ctypes.byref(code)):
                raise RuntimeError("WORKER_LIVENESS_UNVERIFIABLE:WIN32_ERROR_"+str(ctypes.get_last_error()))
            return code.value==259
        finally:kernel.CloseHandle(handle)
    try:os.kill(pid,0);return True
    except ProcessLookupError:return False
    except PermissionError:return True
def reject_if_prior_worker_alive(old:dict)->None:
    if old.get("pid") is not None and old.get("phase") in {"STARTING","RUNNING","SEED_PENDING"}:
        if pid_is_live(int(old["pid"])):raise RuntimeError("WORKER_ALREADY_ACTIVE")
def await_launch_handshake(state_path:Path,launch_record_path:Path,launch_id:str,timeout_s:float=15.0)->tuple[dict,dict]:
    deadline=time.monotonic()+timeout_s
    while time.monotonic()<deadline:
        if launch_record_path.is_file() and state_path.is_file():
            try:doc=json.loads(state_path.read_text(encoding="utf-8"))
            except (OSError,json.JSONDecodeError):doc={}
            launch=json.loads(launch_record_path.read_text(encoding="utf-8"))
            if launch.get("launch_id")!=launch_id or launch.get("worker_id")!=state_path.stem or launch.get("run_id")!=os.environ.get("N0_POOL20_RUN_ID"):
                raise RuntimeError("WORKER_LAUNCH_RECORD_IDENTITY_MISMATCH")
            if doc.get("launch_id")==launch_id:
                actual=os.getpid();parent=os.getppid();cmd=process_command_line().lower()
                if str(Path(__file__).resolve()).lower() not in cmd or launch_id.lower() not in cmd:
                    raise RuntimeError("WORKER_LAUNCH_COMMAND_IDENTITY_MISMATCH")
                return launch,launch.get("previous_state") or {}
            if doc and doc.get("launch_id") not in {None,launch_id}:
                raise RuntimeError("WORKER_LAUNCH_NONCE_MISMATCH")
        time.sleep(0.02)
    raise RuntimeError("WORKER_LAUNCH_HANDSHAKE_TIMEOUT")
def terminal(episode:Path,episode_id:str):
    stop=episode/"state/manual_episode_agent_stop.json"
    if stop.is_file():
        obj=json.loads(stop.read_text(encoding="utf-8-sig"));
        if obj.get("episode_id")!=episode_id:raise RuntimeError("EPISODE_STOP_IDENTITY_MISMATCH")
        return "AGENT_STOP"
    final=episode/"results/c0030_manual_settlement.json"
    if final.is_file():
        obj=json.loads(final.read_text(encoding="utf-8-sig"))
        if obj.get("episode_id")!=episode_id or obj.get("candidate_id")!="C0030":raise RuntimeError("EPISODE_FINAL_IDENTITY_MISMATCH")
        return "EPISODE_BUDGET_COMPLETE"
    return None
def main():
    p=argparse.ArgumentParser();p.add_argument("--run-id",required=True);p.add_argument("--worker-id",required=True);p.add_argument("--launch-id",required=True);p.add_argument("--resume-only",action="store_true");p.add_argument("--handshake-only",action="store_true");a=p.parse_args()
    root=Path(os.environ.get("N0_POOL20_RUN_ROOT",str(RUNS/a.run_id))).resolve()
    if root.name!=a.run_id:raise RuntimeError("WORKER_RUN_ROOT_IDENTITY_MISMATCH")
    state_path=root/"workers"/(a.worker_id+".json");stop=Path(os.environ.get("N0_POOL20_STOP_FILE",str(root/"STOP_REQUESTED")))
    manifest=json.loads((root/"run_manifest.json").read_text(encoding="utf-8"))
    item=next((x for x in manifest["workers"] if x["worker_id"]==a.worker_id),None)
    if not item or len(item.get("episode_ids",[]))!=5 or len(set(item["episode_ids"]))!=5 or item["treatment_id"]!="N0_AUTONOMOUS":
        raise RuntimeError("WORKER_RUN_ASSIGNMENT_IDENTITY_MISMATCH")
    launch_record_path=root/"launches"/(a.worker_id+"."+a.launch_id+".json")
    launch,old=await_launch_handshake(state_path,launch_record_path,a.launch_id)
    completed=list(dict.fromkeys(old.get("completed_episode_ids",[])))
    if old.get("phase")=="COMPLETED" and set(completed)==set(item["episode_ids"]):
        write(state_path,{**old,"event":"DUPLICATE_START_REJECTED","launch_id":a.launch_id,"pid":os.getpid(),"updated_at_utc":utc()},expected_launch_id=a.launch_id);return 0
    reject_if_prior_worker_alive(old)
    manifest_path=RELEASE/"RELEASE_MANIFEST.json"
    manifest_sha=hashlib.sha256(manifest_path.read_bytes()).hexdigest().upper()
    worker_identity={"pid":os.getpid(),"launcher_pid":os.getppid(),"launch_id":a.launch_id,
        "release_id":RELEASE_ID,"manifest_sha256":manifest_sha,"run_id":a.run_id,
        "python_executable":str(Path(sys.executable).resolve()),"python_prefix":str(Path(sys.prefix).resolve()),
        "worker_source_path":str(Path(__file__).resolve()),"worker_source_sha256":hashlib.sha256(Path(__file__).read_bytes()).hexdigest().upper(),
        "command_line":process_command_line(),
        "restart_count":int(launch.get("restart_count",0)),
        "historical_restart_count":int(launch.get("historical_restart_count",0)),
        "restart_generation":launch.get("restart_generation"),
        "generation_restart_count":int(launch.get("generation_restart_count",0)),
        "started_at_utc":old.get("started_at_utc",utc()),"resume_only":a.resume_only}
    env=dict(os.environ);env["N0_POOL20_RUN_ID"]=a.run_id;env["N0_POOL20_RUN_ROOT"]=str(root);env["N0_POOL20_STOP_FILE"]=str(stop)
    env["N0_POOL20_EPISODE_ROOT"]=str(Path(os.environ["N0_POOL20_EPISODE_ROOT"]).resolve())
    env["N0_OJ_QUEUE_ROOT"]=str(Path(os.environ["N0_OJ_QUEUE_ROOT"]).resolve());env["N0_GPU_LEASE_PATH"]=str(LEASE_PATH)
    env["N0_PROFILE_STATE_ROOT"]=str(Path(os.environ["N0_PROFILE_STATE_ROOT"]).resolve())
    if a.handshake_only:
        test_root=Path(os.environ.get("N0_POOL20_HANDSHAKE_TEST_ROOT","")).resolve()
        if os.environ.get("N0_POOL20_HANDSHAKE_TEST")!="1" or not test_root or root.resolve()!=test_root or root.resolve()==(RUNS/a.run_id).resolve():
            raise RuntimeError("HANDSHAKE_ONLY_REQUIRES_ISOLATED_TEST_ROOT")
        time.sleep(.5)
        write(state_path,{**item,**worker_identity,"completed_episode_ids":list(item["episode_ids"]),"phase":"COMPLETED","handshake_test_only":True,"updated_at_utc":utc()},expected_launch_id=a.launch_id)
        return 0
    env["N0_PROVIDER_R20_SOURCE_ROOT"]=str(R/"provider/releases/N0_PARATERA_PROVIDER_R20")
    alias=item["key_alias"];key_value=env.get(alias) or env.get(alias.upper())
    if not key_value or not key_value.strip():raise RuntimeError("ASSIGNED_PROVIDER_KEY_ALIAS_MISSING:"+alias)
    if alias=="jlapi2":env["JLAPI"]=key_value
    write(state_path,{**item,**worker_identity,"completed_episode_ids":completed,"phase":"RUNNING","updated_at_utc":utc()},expected_launch_id=a.launch_id)
    for eid in item["episode_ids"]:
        if eid in completed:continue
        if stop.exists():write(state_path,{**item,**worker_identity,"completed_episode_ids":completed,"phase":"PAUSED_STOP","current_episode_id":eid,"updated_at_utc":utc()},expected_launch_id=a.launch_id);return 0
        ep_root=Path(env["N0_POOL20_EPISODE_ROOT"]);episode=ep_root/("manual_episode_"+eid.rsplit("_",1)[1])
        env["N0_POOL20_CURRENT_EPISODE_ID"]=eid
        if not episode.exists():
            if a.resume_only:
                db=Path(env["N0_OJ_QUEUE_ROOT"])/"n0_runtime.sqlite3"
                if db.is_file():
                    con=sqlite3.connect(db.resolve().as_uri()+"?mode=ro",uri=True)
                    try:
                        tables={x[0] for x in con.execute("select name from sqlite_master where type='table'")}
                        for table in ("episodes","oj_requests","provider_requests"):
                            if table in tables and "episode_id" in {x[1] for x in con.execute("pragma table_info("+table+")")}:
                                if con.execute("select 1 from "+table+" where episode_id=? limit 1",(eid,)).fetchone():raise RuntimeError("RESUME_REFUSES_EPISODE_MISSING_WITH_QUEUE_EVIDENCE:"+eid)
                    finally:con.close()
            write(state_path,{**item,**worker_identity,"completed_episode_ids":completed,"current_episode_id":eid,"phase":"SEED_PENDING","updated_at_utc":utc()},expected_launch_id=a.launch_id)
            result=subprocess.run([str(PY),"-u",str(MANAGER),"new",eid],cwd=str(ROOT),env=env,capture_output=True,text=True,encoding="utf-8",errors="replace",timeout=3600)
            (root/"workers"/(a.worker_id+"_"+eid+"_new.stdout.txt")).write_text(result.stdout or "",encoding="utf-8")
            (root/"workers"/(a.worker_id+"_"+eid+"_new.stderr.txt")).write_text(result.stderr or "",encoding="utf-8")
            if result.returncode:raise RuntimeError("EPISODE_NEW_OR_SEED_BOOTSTRAP_FAILED:"+str(result.returncode)+":"+(result.stderr or "")[-1000:])
        meta_path=episode/"episode_manager_manifest.json"
        if not meta_path.is_file():raise RuntimeError("EPISODE_MANAGER_MANIFEST_MISSING")
        meta=json.loads(meta_path.read_text(encoding="utf-8"))
        if meta.get("run_id")!=a.run_id or meta.get("episode_id")!=eid:raise RuntimeError("EPISODE_RUN_MANIFEST_MISMATCH")
        done=terminal(episode,eid)
        if not done and env.get("N0_POOL20_DIAGNOSTIC_STOP_AFTER_SEED")=="1":
            write(state_path,{**item,**worker_identity,"completed_episode_ids":completed,"current_episode_id":eid,"phase":"PAUSED_STOP","reason":"DIAGNOSTIC_STOP_AFTER_SEED","updated_at_utc":utc()},expected_launch_id=a.launch_id)
            return 0
        if not done:
            write(state_path,{**item,**worker_identity,"completed_episode_ids":completed,"current_episode_id":eid,"phase":"RUNNING","updated_at_utc":utc()},expected_launch_id=a.launch_id)
            result=subprocess.run([str(PY),"-u",str(MANAGER),"resume",eid,"--until","C0030","--timeout","900","--max-provider-errors","3","--max-turns","100","--oj-attempts","3","--seed-attempts","3"],cwd=str(ROOT),env=env,capture_output=True,text=True,encoding="utf-8",errors="replace",timeout=86400)
            (root/"workers"/(a.worker_id+"_"+eid+"_resume.stdout.txt")).write_text(result.stdout or "",encoding="utf-8")
            (root/"workers"/(a.worker_id+"_"+eid+"_resume.stderr.txt")).write_text(result.stderr or "",encoding="utf-8")
            done=terminal(episode,eid)
            if not done or result.returncode:
                phase="PAUSED_STOP" if stop.exists() else "PAUSED"
                write(state_path,{**item,**worker_identity,"completed_episode_ids":completed,"current_episode_id":eid,"phase":phase,"exit_code":result.returncode,"episode_terminal":done,"updated_at_utc":utc()},expected_launch_id=a.launch_id)
                return 0 if stop.exists() else 78
        completed.append(eid)
        write(state_path,{**item,**worker_identity,"completed_episode_ids":completed,"current_episode_id":None,"phase":"EPISODE_COMPLETE","updated_at_utc":utc()},expected_launch_id=a.launch_id)
    write(state_path,{**item,**worker_identity,"completed_episode_ids":completed,"phase":"COMPLETED","updated_at_utc":utc()},expected_launch_id=a.launch_id)
    return 0
if __name__=="__main__":
    try:sys.exit(main())
    except Exception as e:
        try:
            root=RUNS/str(sys.argv[sys.argv.index("--run-id")+1]);worker=sys.argv[sys.argv.index("--worker-id")+1]
            path=root/"workers"/(worker+".json");old=json.loads(path.read_text(encoding="utf-8")) if path.is_file() else {}
            launch_id=sys.argv[sys.argv.index("--launch-id")+1]
            write(path,{**old,"phase":"PAUSED","error":type(e).__name__+":"+str(e),"updated_at_utc":utc()},expected_launch_id=launch_id)
        except Exception:pass
        print(type(e).__name__+":"+str(e),file=sys.stderr,flush=True);sys.exit(78)
