"""N0 R8_03 online OJ handoff: R8.11/R9.3 -> R8.14/R9.4.

All inspection paths are read-only. authorize writes a snapshot-bound intent file;
apply validates the exact same state under a single transaction lock and hands
only the *verified* old monitor to the new generation.

Never modifies Gate, OJ Receipt, budget, or individual Candidate results.
"""
from __future__ import annotations
import datetime, hashlib, json, os, subprocess, time, uuid
from pathlib import Path

OLD_POOL="N0_KERNEL_AGENT_POOL20_R8_14_R8_03_MONITORLESS_R9_4_20261027"
OLD_POOL_SHA="1545D2579275DF8E08AD152309A24A992726BD88DDDFCD443DC728D487A0810C"
OLD_INTEGRATION="N0_PRODUCTION_INTEGRATION_R12_10_4_R9_3_R8_03_REFERENCE_CONTROL_HANDOFF_FIX_20261027"
OLD_INTEGRATION_SHA="DA39ACC5D22F58F25FB48244D96E8D1F38C3BE69F19CA27A329ADDFBD804E14E"
FAULT_GATE_SCHEMA="N0_GPU_FAULT_RECOVERY_GATE_V1"
MAX_CONTROL_DEVIATION=0.02


def sha(raw): return hashlib.sha256(raw).hexdigest().upper()
def same_digest(a,b):
    x,y=str(a or ""),str(b or "")
    return len(x)==len(y)==64 and all(c in "0123456789abcdefABCDEF" for c in x+y) and x.upper()==y.upper()
def read_json(path):return json.loads(Path(path).read_text(encoding="utf-8"))
def canonical(v):return json.dumps(v,ensure_ascii=False,sort_keys=True,separators=(",",":")).encode('utf-8')


def validate_recovery_evidence(gate:dict,receipt:dict,raw:bytes,authoritative_row:dict,baseline_id:str,gpu_uuid:str)->dict:
    """Validate durable fault recovery without depending on transient in-memory Supervisor status."""
    def check(cond, reason):
        if not cond:raise RuntimeError(reason)
    check(gate.get('schema')==FAULT_GATE_SCHEMA and gate.get('status')=='READY','FAULT_READY_GATE_INVALID')
    fault_id=gate.get('fault_request_id')
    check(bool(fault_id) and authoritative_row.get('request_id')==fault_id and authoritative_row.get('state')=='CONSUMED', 'FAULT_READY_PARENT_REQUEST_NOT_CONSUMED')
    check(receipt.get('schema')=='N0_CANDIDATE_FAULT_RECOVERY_RECEIPT_V1' and receipt.get('status')=='PASS','FAULT_READY_RECOVERY_RECEIPT_NOT_PASS')
    check(receipt.get('recovery_id')==gate.get('recovery_receipt_id'), 'FAULT_READY_RECOVERY_ID_MISMATCH')
    check(receipt.get('fault_request_id')==fault_id,'FAULT_READY_FAULT_REQUEST_MISMATCH')
    check(same_digest(receipt.get('fault_candidate_sha256'),authoritative_row.get('candidate_sha256')),'FAULT_READY_CANDIDATE_SHA_MISMATCH')
    check(same_digest(receipt.get('oj_receipt_sha256'),authoritative_row.get('receipt_sha256')),'FAULT_READY_OJ_RECEIPT_SHA_MISMATCH')
    check(same_digest(receipt.get('fault_receipt_sha256'),gate.get('fault_receipt_sha256')),'FAULT_READY_FAULT_RECEIPT_SHA_MISMATCH')
    check(same_digest(receipt.get('recovery_receipt_sha256'),gate.get('recovery_receipt_sha256')),'FAULT_READY_RECOVERY_CONTENT_SHA_MISMATCH')
    body={k:v for k,v in receipt.items() if k not in {'recovery_receipt_sha256','recovery_receipt_file_sha256'}}
    check(same_digest(receipt.get('recovery_receipt_sha256'),sha(canonical(body))),'FAULT_READY_RECOVERY_CONTENT_INVALID')
    check(receipt.get('required_consecutive_controls')==3,'FAULT_READY_CONTROL_POLICY_MISMATCH')
    controls=receipt.get('controls') or []
    check(3<=len(controls)<=10,'FAULT_READY_CONTROL_COUNT_INVALID')
    streak=0
    for ctl in controls:
        trust=ctl.get('trust') or {};health=ctl.get('health') or {}
        deviation=trust.get('relative_deviation')
        healthy=(trust.get('pass') is True and type(deviation) in (int,float)
                 and abs(float(deviation))<=MAX_CONTROL_DEVIATION and health.get('status')=='PASS'
                 and trust.get('baseline_id')==baseline_id)
        streak=streak+1 if healthy else 0
        check(ctl.get('consecutive_passes')==streak,'FAULT_READY_CONTROL_SEQUENCE_INVALID')
    check(streak>=3,'FAULT_READY_REFERENCE_NOT_THREE_CONSECUTIVE_PASS')
    lease=receipt.get('recovery_lease') or {}
    check(lease.get('released') is True and lease.get('lease_id') and int(lease.get('pid') or 0)>0,'FAULT_READY_LEASE_RELEASE_EVIDENCE_INVALID')
    check(receipt.get('gpu_identity',{}).get('gpu_uuid')==gpu_uuid if 'gpu_identity' in receipt else True,'FAULT_READY_GPU_IDENTITY_MISMATCH')
    return {'status':'PASS','fault_request_id':fault_id,'recovery_id':receipt['recovery_id'],
            'recovery_content_sha256':sha(canonical(body)),'recovery_file_sha256':sha(raw),'consecutive_controls':streak}


def monitor_process_group(processes, identity, launcher, *, release_id, manifest_sha, controller_path, runtime_root):
    """Resolve a genuine Windows Python launcher+child into ONE logical monitor.

    Reject any other monitor process, any PID/parent spoof, or mismatched source.
    Both the launcher and Python worker must be stopped during handoff.
    """
    def check(ok, msg):
        if not ok:raise RuntimeError(msg)
    check(identity.get('release_id')==release_id and same_digest(identity.get('manifest_sha256'),manifest_sha),
          'ONLINE_OLD_MONITOR_RELEASE_MISMATCH')
    check(launcher.get('release_id')==release_id and same_digest(launcher.get('manifest_sha256'),manifest_sha),
          'ONLINE_OLD_MONITOR_LAUNCH_RELEASE_MISMATCH')
    try:
        ipid=int(identity.get('pid') or 0)
        lpid=int(launcher.get('pid') or 0)
    except (ValueError, TypeError):
        raise RuntimeError('ONLINE_MONITOR_PID_INVALID')
    check(ipid>0 and lpid>0,'ONLINE_MONITOR_PID_INVALID')
    target=str(controller_path).lower().replace('/',chr(92))
    runtime=str(runtime_root).lower().replace('/',chr(92))
    all_monitors=[]
    for p in processes:
        cmd=str(p.get('CommandLine') or '').lower().replace('/',chr(92))
        if 'pool20_r8_controller.py' in cmd and 'monitor' in cmd and runtime in cmd:
            all_monitors.append(p)
    # R8.14: the operator may have explicitly stopped BOTH original Python
    # monitor processes after verifying their exact PID/CLI/parent relationship.
    # Their immutable identity + launcher records remain, and no live monitor
    # whatsoever may be present. Absence alone is NOT a sign of completed handoff:
    # we still validate the previous full OJ HOLD and the READY fault Gate evidence.
    if len(all_monitors)==0:
        recorded_cmd=str(identity.get('command_line') or '').lower().replace('/',chr(92))
        check(ipid!=lpid and int(identity.get('parent_pid') or 0)==lpid,
              'ONLINE_STOPPED_MONITOR_PARENT_IDENTITY_INVALID')
        check(target in recorded_cmd and ' monitor' in recorded_cmd,
              'ONLINE_STOPPED_MONITOR_RECORDED_SOURCE_INVALID')
        if identity.get('executable_path'):
            check(str(identity['executable_path']).lower().endswith('python.exe'),
                  'ONLINE_STOPPED_MONITOR_EXECUTABLE_INVALID')
        return {'identity_pid':ipid,'launcher_pid':lpid,'mode':'ALREADY_STOPPED',
                'members':[], 'no_live_monitors_verified':True}
    check(len(all_monitors) in (1,2),'ONLINE_MONITOR_CONFLICT_OR_MISSING')
    by_pid={int(p.get('ProcessId') or 0):p for p in all_monitors}
    check(len(by_pid)==len(all_monitors) and ipid in by_pid and lpid in by_pid,
          'ONLINE_MONITOR_CONFLICT_OR_MISSING')
    if ipid==lpid:
        check(len(all_monitors)==1,'ONLINE_MONITOR_CONFLICT_OR_MISSING')
    else:
        check(len(all_monitors)==2,'ONLINE_MONITOR_CONFLICT_OR_MISSING')
        check(int(by_pid[ipid].get('ParentProcessId') or 0)==lpid,
              'ONLINE_MONITOR_PARENT_CHILD_MISMATCH')
        if identity.get('parent_pid') is not None:
            check(int(identity['parent_pid'])==lpid,'ONLINE_MONITOR_RECORD_PARENT_MISMATCH')
    for pid in sorted(by_pid):
        row=by_pid[pid]
        cmd=str(row.get('CommandLine') or '').lower().replace('/',chr(92))
        check(target in cmd and ' monitor' in cmd,'ONLINE_MONITOR_COMMAND_MISMATCH')
        check(str(row.get('Name') or '').lower() in ('python.exe','pythonw.exe'),
              'ONLINE_MONITOR_EXECUTABLE_INVALID')
    return {'identity_pid':ipid,'launcher_pid':lpid,'members':[
       {'pid':pid,'parent_pid':int(by_pid[pid].get('ParentProcessId') or 0),
        'command_line':str(by_pid[pid].get('CommandLine') or ''),
        'creation_date':str(by_pid[pid].get('CreationDate') or '')}
       for pid in sorted(by_pid)]}


def monitor_row_matches(saved, row):
    return bool(row and int(row.get('ProcessId') or 0)==saved['pid']
          and int(row.get('ParentProcessId') or 0)==saved['parent_pid']
          and str(row.get('CommandLine') or '').lower()==saved['command_line'].lower()
          and (not saved['creation_date'] or str(row.get('CreationDate') or '')==saved['creation_date']))


def verified_new_monitor(processes, identity, *, launcher_pid, release_id, manifest_sha, controller_path):
    """Windows venv python.exe may own a child conda python.exe that writes the identity."""
    def check(ok,msg):
        if not ok:raise RuntimeError(msg)
    check(identity.get('release_id')==release_id and same_digest(identity.get('manifest_sha256'),manifest_sha),
          'ONLINE_NEW_MONITOR_RELEASE_MISMATCH')
    ipid=int(identity.get('pid') or 0)
    check(ipid>0,'ONLINE_NEW_MONITOR_IDENTITY_PID_INVALID')
    target=str(controller_path).lower().replace('/',chr(92))
    by_pid={int(p.get('ProcessId') or 0):p for p in processes}
    child=by_pid.get(ipid); parent=by_pid.get(launcher_pid)
    check(child is not None and parent is not None,'ONLINE_NEW_MONITOR_PROCESS_MISSING')
    check(ipid==launcher_pid or int(child.get('ParentProcessId') or 0)==launcher_pid,
          'ONLINE_NEW_MONITOR_PARENT_CHILD_MISMATCH')
    if identity.get('parent_pid') is not None and ipid!=launcher_pid:
        check(int(identity['parent_pid'])==launcher_pid,'ONLINE_NEW_MONITOR_RECORD_PARENT_MISMATCH')
    for row in [child,parent]:
        cmd=str(row.get('CommandLine') or '').lower().replace('/',chr(92))
        check(target in cmd and ' monitor' in cmd,'ONLINE_NEW_MONITOR_COMMAND_MISMATCH')
    return {'monitor_pid':ipid,'launcher_pid':launcher_pid}


class OnlineHandoff:
    def __init__(self,c):
        self.c=c
        self.root=c.PROFILE/'online_r814_handoff'

    def load_context(self, *, need_old_monitor=True, require_old_service=True):
        c=self.c;chk=c.require
        release=c.release_check()
        c.check_manifest(c.INTEGRATION,c.INTEGRATION.name,c.INTEGRATION_SHA)
        c.check_manifest(c.OJ,c.OJ.name,c.OJ_SHA)
        c.check_manifest(c.HARNESS,c.HARNESS.name,c.HARNESS_SHA)
        c.check_manifest(c.KB,c.KB.name,c.KB_SHA)
        monitor_identity=c.RUN_ROOT/'monitor_identity.json'
        monitor_launcher=c.RUN_ROOT/'monitor_launcher_r9_2.json'
        if not monitor_identity.is_file():
            live=c.process_rows();live_pids={int(x.get('ProcessId') or 0) for x in live}
            identities=[]
            idir=c.RUN_ROOT/'monitor_identities_r817'
            for candidate in sorted(idir.glob('*.json')) if idir.is_dir() else []:
                try:doc=read_json(candidate)
                except Exception:continue
                if int(doc.get('pid') or 0) in live_pids and doc.get('launch_id')==candidate.stem:identities.append(candidate)
            chk(len(identities)==1,'ONLINE_R817_MONITOR_IDENTITY_NOT_UNIQUE')
            monitor_identity=identities[0]
            launch_id=monitor_identity.stem
            launchers=[]
            for candidate in sorted(c.RUN_ROOT.glob('monitor_launcher*.json')):
                try:doc=read_json(candidate)
                except Exception:continue
                if doc.get('launch_id')==launch_id:launchers.append(candidate)
            chk(len(launchers)==1,'ONLINE_R817_MONITOR_LAUNCH_RECEIPT_NOT_UNIQUE')
            monitor_launcher=launchers[0]
        paths={
            'active':c.ACTIVE,'run':c.RUN_ROOT/'run_manifest.json',
            'active_run':c.RUNS/'ACTIVE_RUN.json',
            'service':c.QUEUE/'service_status.json',
            'gate':c.PROFILE/'timing_gate.json',
            'monitor_identity':monitor_identity,
            'monitor_launcher':monitor_launcher,
        }
        for label,p in paths.items():chk(p.is_file(),'ONLINE_REQUIRED_FILE_MISSING:'+label)
        values={k:read_json(v) for k,v in paths.items()}
        active,run,svc,gate=values['active'],values['run'],values['service'],values['gate']
        chk(active.get('run_id')==c.RUN_ID and active.get('deployment_release_id')==OLD_POOL and same_digest(active.get('manifest_sha256'),OLD_POOL_SHA),'ONLINE_OLD_ACTIVE_BINDING_MISMATCH')
        chk(run.get('run_id')==c.RUN_ID and run.get('pool_release_id')==OLD_POOL and same_digest(run.get('pool_manifest_sha256'),OLD_POOL_SHA) and run.get('state')=='RUNNING','ONLINE_OLD_RUN_BINDING_MISMATCH')
        chk(run.get('integration_release_id')==OLD_INTEGRATION and same_digest(run.get('integration_manifest_sha256'),OLD_INTEGRATION_SHA),'ONLINE_OLD_RUN_INTEGRATION_MISMATCH')
        chk(active.get('integration_release_id')==OLD_INTEGRATION and same_digest(active.get('integration_manifest_sha256'),OLD_INTEGRATION_SHA),'ONLINE_OLD_ACTIVE_INTEGRATION_MISMATCH')
        chk(values['active_run'].get('run_id')==c.RUN_ID and values['active_run'].get('release_id')==OLD_POOL,'ONLINE_ACTIVE_RUN_RESERVATION_MISMATCH')
        chk(not (c.RUN_ROOT/'STOP_REQUESTED').exists() and not (c.QUEUE/'STOP_REQUESTED').exists(),'ONLINE_STOP_MARKER_PRESENT')
        chk(svc.get('state')=='HOLD' and svc.get('release_id')==OLD_INTEGRATION and same_digest(svc.get('manifest_sha256'),OLD_INTEGRATION_SHA),'ONLINE_OJ_NOT_EXPECTED_HOLD')
        detail=svc.get('detail') or {}
        chk(detail.get('shutdown')=='FRESH_PROCESS_FAULT_RECOVERY_NOT_READY','ONLINE_OJ_HOLD_UNEXPECTED_REASON')
        chk(detail.get('recovery_child_identity')=='PASS' and not detail.get('recovery_validation_error'),'ONLINE_PREVIOUS_RECOVERY_CHILD_NOT_VERIFIED')
        prev_gate=detail.get('timing_gate') or {}
        chk(prev_gate.get('status')=='READY' and prev_gate.get('fault_request_id')==gate.get('fault_request_id') and same_digest(prev_gate.get('recovery_receipt_sha256'),gate.get('recovery_receipt_sha256')),'ONLINE_GATE_MISMATCH_WITH_OJ_HOLD')
        boot=c.bootstrap_module();snapshot=boot.queue_snapshot(c.QUEUE)
        counts=snapshot['payload']['request_state_counts']
        chk(snapshot['episode_count']==20 and snapshot['running_count']==0 and snapshot['hold_unknown_count']==0 and not counts.get('COMPLETED') and snapshot['queued_count']>0,'ONLINE_QUEUE_NOT_QUIESCENT')
        chk(snapshot['request_count']==sum(int(n) for n in counts.values()),'ONLINE_QUEUE_COUNT_INCONSISTENT')
        chk(snapshot['candidate_budget_used']>=0,'ONLINE_BUDGET_INVALID')
        matches=[x for x in snapshot['payload']['requests'] if x.get('request_id')==gate.get('fault_request_id')]
        chk(len(matches)==1,'ONLINE_GATE_FAULT_REQUEST_NOT_IN_QUEUE')
        recfile=Path(str(gate.get('recovery_receipt_path') or ''))
        chk(recfile.is_file() and recfile.parent.resolve()==(c.PROFILE/'fault_recovery_receipts').resolve(),'ONLINE_RECOVERY_RECEIPT_PATH_INVALID')
        chk(recfile.name==str(gate.get('recovery_receipt_id'))+'.json','ONLINE_RECOVERY_RECEIPT_FILENAME_INVALID')
        raw=recfile.read_bytes();receipt=json.loads(raw)
        expected_file_sha=(detail.get('recovery_receipt') or {}).get('recovery_receipt_file_sha256')
        chk(same_digest(expected_file_sha,sha(raw)),'ONLINE_RECOVERY_FILE_SHA_VS_SERVICE_INVALID')
        evidence=validate_recovery_evidence(gate,receipt,raw,matches[0],c.BASELINE_ID,c.GPU_UUID)
        lease=receipt.get('recovery_lease') or {}
        chk(Path(str(lease.get('path') or '')).resolve()==c.LEASE.resolve(),'ONLINE_RECOVERY_LEASE_PATH_MISMATCH')
        workers={};worker_paths={}
        for item in c.expected_workers():
            wpath=c.RUN_ROOT/'workers'/(item['worker_id']+'.json');chk(wpath.is_file(),'ONLINE_WORKER_RECORD_MISSING:'+item['worker_id'])
            w=read_json(wpath);chk(w.get('worker_id')==item['worker_id'] and w.get('phase') in {'PAUSED','PAUSED_STOP','COMPLETED'},'ONLINE_WORKER_NOT_PAUSED:'+item['worker_id'])
            workers[item['worker_id']]=w;worker_paths[item['worker_id']]=wpath
        processes=c.process_rows();live=[]
        for p in processes:
            cmd=str(p.get('CommandLine') or '').lower()
            if any(k in cmd for k in ('pool20_worker.py','service_supervisor.py','run_r5_oj_worker.py','candidate_eval_child.py')) and c.RUN_ID.lower() in cmd:
                live.append(p)
        chk(not live,'ONLINE_LIVE_WORKER_OR_OJ_PRESENT')
        old_monitor=values['monitor_identity'];launcher=values['monitor_launcher']
        group=monitor_process_group(processes,old_monitor,launcher,
            release_id=OLD_POOL,manifest_sha=OLD_POOL_SHA,
            controller_path=c.R/'deployment/releases'/OLD_POOL/'pool20_r8_controller.py',
            runtime_root=c.ROOT)
        gpu=c.gpu_state()
        chk(not gpu['compute_processes'],'ONLINE_GPU_BUSY')
        with c.LEASE.open('r+b') as f:
            import msvcrt
            try:f.seek(0);msvcrt.locking(f.fileno(),msvcrt.LK_NBLCK,1)
            except OSError as exc:raise RuntimeError('ONLINE_GPU_LEASE_BUSY') from exc
            f.seek(0);msvcrt.locking(f.fileno(),msvcrt.LK_UNLCK,1)
        hashes={k:sha(p.read_bytes()) for k,p in paths.items()}
        hashes['workers']=sha(canonical({w:sha(p.read_bytes()) for w,p in worker_paths.items()}))
        hashes['recovery_receipt_file']=sha(raw)
        return {'status':'PASS','mode':'ONLINE_OLD_OJ_HOLD_FAULT_GATE_READY','run_id':c.RUN_ID,
            'release_id':c.RELEASE_ID,'pool_manifest_sha256':release['manifest_sha256'],
            'integration_manifest_sha256':c.INTEGRATION_SHA,
            'queue_snapshot_sha256':snapshot['sha256'],'candidate_budget_used':snapshot['candidate_budget_used'],
            'queue_counts':counts,'fault_request_id':gate['fault_request_id'],'recovery_evidence':evidence,
            'monitor_pid':group['identity_pid'],'monitor_process_group':group,'state_file_sha256':hashes,'gpu_uuid':gpu['uuid']}

    def inspect(self):return self.load_context()

    def authorize(self):
        # Only the new receipt is written. The Run, Gate, budget, worker registry,
        # ACTIVE binding, OJ Receipt and existing queue are untouched.
        snapshot=self.load_context();ident=uuid.uuid4().hex
        a={'schema':'N0_POOL20_R814_ONLINE_HANDOFF_AUTH_V1','authorization_id':ident,
           'authorized_by':'Operator requested to continue live R8_03 experiments',
           'issued_at_utc':self.c.utc(),'snapshot':snapshot}
        target=self.root/(ident+'.authorization.json');self.c.write_once(target,a)
        return {'status':'PASS','authorization_id':ident,'authorization_path':str(target),
                'authorization_sha256':sha(target.read_bytes()),'queue_snapshot_sha256':snapshot['queue_snapshot_sha256'],
                'candidate_budget_used':snapshot['candidate_budget_used']}

    def validate_fault_ready_gate(self,snapshot=None):
        c=self.c;gate_path=c.PROFILE/'timing_gate.json';gate_raw=gate_path.read_bytes();gate=json.loads(gate_raw)
        if snapshot is None:snapshot=c.bootstrap_module().queue_snapshot(c.QUEUE)
        rows=[x for x in snapshot['payload']['requests'] if x.get('request_id')==gate.get('fault_request_id')]
        c.require(len(rows)==1,'REMAINING_FAULT_GATE_PARENT_REQUEST_NOT_FOUND')
        path=Path(str(gate.get('recovery_receipt_path') or ''))
        c.require(path.is_file() and path.parent.resolve()==(c.PROFILE/'fault_recovery_receipts').resolve(),'REMAINING_FAULT_RECOVERY_PATH_INVALID')
        raw=path.read_bytes();receipt=json.loads(raw)
        proof=validate_recovery_evidence(gate,receipt,raw,rows[0],c.BASELINE_ID,c.GPU_UUID)
        c.require(Path(str((receipt.get('recovery_lease') or {}).get('path') or '')).resolve()==c.LEASE.resolve(),'REMAINING_LEASE_PATH_MISMATCH')
        return {'gate':gate,'gate_sha256':sha(gate_raw),'recovery_receipt_file_sha256':sha(raw),
                'recovery_receipt_sha256':proof['recovery_content_sha256'],
                'recovery_controls_passed':proof['consecutive_controls'],
                'queue_snapshot_sha256':snapshot['sha256']}

    def verify_own_monitor_and_service(self,service):
        c=self.c;rows=c.process_rows();by_pid={int(p.get('ProcessId') or 0):p for p in rows}
        pid=int(service.get('pid') or 0)
        proc=by_pid.get(pid)
        c.require(proc and str((c.INTEGRATION/'run_r5_oj_worker.py').resolve()).lower() in str(proc.get('CommandLine') or '').lower(),'ONLINE_SERVING_PROCESS_NOT_VERIFIED')
        identities=[]
        identity_dir=c.RUN_ROOT/'monitor_identities_r817'
        for path in sorted(identity_dir.glob('*.json')) if identity_dir.is_dir() else []:
            try:identity=read_json(path)
            except Exception:continue
            monitor=by_pid.get(int(identity.get('pid') or 0))
            command=str((monitor or {}).get('CommandLine') or '').lower()
            launch_id=str(identity.get('launch_id') or '').lower()
            if (launch_id and launch_id in command and identity.get('launch_id')==path.stem and identity.get('release_id') in {c.RELEASE_ID,'N0_KERNEL_AGENT_POOL20_R8_18_5_R8_03_R96_PROFILE_WORKER_BINDING_TEST_IDENTITY_FIX_20261027'} and same_digest(identity.get('manifest_sha256'),(c.release_check()['manifest_sha256'] if identity.get('release_id')==c.RELEASE_ID else '7F50683F7DDCBC64480105078A98BFD3DDE0B8BB13BD03E425CF066B846062E5'))
                    and monitor and str((c.R/'deployment/releases'/identity['release_id']/'pool20_r8_controller.py').resolve()).lower() in command
                    and ' monitor' in command and int(identity.get('parent_pid') or 0)==int(monitor.get('ParentProcessId') or 0)):
                identities.append(identity)
        c.require(len(identities)==1,'ONLINE_MONITOR_NOT_VERIFIED')
        return {'service_pid':pid,'monitor_pid':identities[0]['pid'],'monitor_identity_path':identities[0].get('identity_path')}

    def apply(self,authorization_id):
        c=self.c;auth_path=self.root/(authorization_id+'.authorization.json')
        c.require(auth_path.is_file(),'ONLINE_AUTHORIZATION_NOT_FOUND')
        auth=read_json(auth_path)
        c.require(auth.get('schema')=='N0_POOL20_R814_ONLINE_HANDOFF_AUTH_V1' and auth.get('authorization_id')==authorization_id,'ONLINE_AUTHORIZATION_INVALID')
        # Same exact immutable snapshot; any change requires new inspect + authorization.
        issued=str(auth.get('issued_at_utc') or '')
        try:
            issued_at=datetime.datetime.fromisoformat(issued.replace('Z','+00:00'))
            age=(datetime.datetime.now(datetime.timezone.utc)-issued_at).total_seconds()
        except (ValueError,TypeError):age=999999
        c.require(0<=age<=300,'ONLINE_AUTHORIZATION_EXPIRED')
        current=self.load_context()
        c.require(auth.get('snapshot')==current,'ONLINE_AUTHORIZATION_SNAPSHOT_CHANGED')
        import msvcrt
        lock_path=c.RUN_ROOT/'.online_r814_transition.lock'
        with lock_path.open('a+b') as lock:
            lock.seek(0)
            try:msvcrt.locking(lock.fileno(),msvcrt.LK_NBLCK,1)
            except OSError as exc:raise RuntimeError('ONLINE_TRANSITION_ALREADY_ACTIVE') from exc
            try:
                # Revalidate under transaction lock before any mutation.
                c.require(auth['snapshot']==self.load_context(),'ONLINE_AUTHORIZATION_SNAPSHOT_STALE_UNDER_LOCK')
                return self._apply_locked(authorization_id,auth_path,current)
            finally:
                lock.seek(0);msvcrt.locking(lock.fileno(),msvcrt.LK_UNLCK,1)

    def _apply_locked(self,authorization_id,auth_path,snapshot):
        c=self.c
        transition=c.RUN_ROOT/'recovery_transitions'/('r814_online_'+authorization_id)
        transition.mkdir(parents=True,exist_ok=False)
        c.write_once(transition/'authorization.json',read_json(auth_path))
        c.write_once(transition/'preflight_snapshot.json',snapshot)
        for label,p in [('run',c.RUN_ROOT/'run_manifest.json'),('active',c.ACTIVE),('active_run',c.RUNS/'ACTIVE_RUN.json'),
                        ('service',c.QUEUE/'service_status.json'),('gate',c.PROFILE/'timing_gate.json'),
                        ('monitor_identity',c.RUN_ROOT/'monitor_identity.json')]:
            c.write_bytes_once(transition/(label+'.before.json'),p.read_bytes())
        # Stop ONLY the two identity-verified members of the *same* old Monitor
        # process chain (Python venv launcher + actual conda Python child).
        # Never use taskkill /T: unrelated descendants must remain untouched.
        group=snapshot['monitor_process_group'];recorded=group['members']
        if group.get('mode')=='ALREADY_STOPPED':
            c.require(not recorded and group.get('no_live_monitors_verified') is True,
                      'ONLINE_STOPPED_MONITOR_SNAPSHOT_INVALID')
        current=c.process_rows()
        lookup={int(row.get('ProcessId') or 0):row for row in current}
        for item in recorded:
            c.require(monitor_row_matches(item,lookup.get(item['pid'])),
                      'ONLINE_OLD_MONITOR_IDENTITY_CHANGED')
        stop_results=[]
        # Child before launcher, since launcher waits for child to exit.
        order=sorted(recorded,key=lambda x:x['pid']==group['launcher_pid'])
        for item in order:
            pid=item['pid']
            rows={int(row.get('ProcessId') or 0):row for row in c.process_rows()}
            current=rows.get(pid)
            if current is None:
                stop_results.append({'pid':pid,'already_exited':True});continue
            c.require(monitor_row_matches(item,current),'ONLINE_OLD_MONITOR_IDENTITY_CHANGED_BEFORE_STOP')
            result=subprocess.run(['taskkill.exe','/PID',str(pid),'/F'],capture_output=True,text=True,timeout=20)
            stop_results.append({'pid':pid,'exit_code':result.returncode,
                                 'stdout':result.stdout,'stderr':result.stderr})
            for _ in range(40):
                if not any(int(x.get('ProcessId') or 0)==pid for x in c.process_rows()):break
                time.sleep(.25)
            c.require(not any(int(x.get('ProcessId') or 0)==pid for x in c.process_rows()),
                      'ONLINE_OLD_MONITOR_STILL_LIVE')
        c.write_once(transition/'monitor_stop.json',
                     {'monitor_identity_pid':group['identity_pid'],
                      'launcher_pid':group['launcher_pid'],
                      'stop_mode':group.get('mode','LIVE_IDENTITY_VERIFIED_STOP'),
                      'no_live_monitors_verified':len(recorded)==0,
                      'results':stop_results,'at_utc':c.utc()})
        c.require(not any('pool20_r8_controller.py' in str(x.get('CommandLine') or '').lower()
                          and ' monitor' in str(x.get('CommandLine') or '').lower()
                          and str(c.ROOT).lower() in str(x.get('CommandLine') or '').lower()
                          for x in c.process_rows()),'ONLINE_OLD_MONITOR_PROCESS_CHAIN_REMAINS')
        old_ident=c.RUN_ROOT/'monitor_identity.json'
        os.replace(old_ident,transition/'monitor_identity.previous.json')
        c.write_once(transition/'monitor_handoff_complete.json',{'old_monitor_pid':group['identity_pid'],'old_launcher_pid':group['launcher_pid'],'status':'PASS','time_utc':c.utc()})
        # No queue, receipt, Gate or budget mutations. Active/Run records update
        # through existing atomic JSON functions and are audit-preserved above.
        old_active=read_json(c.ACTIVE)
        new_active={**old_active,'deployment_release_id':c.RELEASE_ID,
          'manifest_sha256':c.release_check()['manifest_sha256'],'integration_release_id':c.INTEGRATION.name,
          'integration_manifest_sha256':c.INTEGRATION_SHA,'authorization_id':authorization_id,
          'activation_scope':'R8_03_ONLINE_R9_4_HANDOFF','activated_at_utc':c.utc(),
          'previous_binding':{'sha256':sha(canonical(old_active))}}
        c.atomic_doc(c.ACTIVE,new_active)
        ar_path=c.RUNS/'ACTIVE_RUN.json';ar=read_json(ar_path)
        ar.update({'release_id':c.RELEASE_ID,'manifest_sha256':c.release_check()['manifest_sha256'],
                   'integration_release_id':c.INTEGRATION.name,'integration_manifest_sha256':c.INTEGRATION_SHA,
                   'resume_authorization_id':authorization_id,'updated_at_utc':c.utc()})
        c.atomic_doc(ar_path,ar)
        rpath=c.RUN_ROOT/'run_manifest.json';run=read_json(rpath)
        run.update({'state':'RESUMING','pool_release_id':c.RELEASE_ID,'pool_manifest_sha256':c.release_check()['manifest_sha256'],
                    'integration_release_id':c.INTEGRATION.name,'integration_manifest_sha256':c.INTEGRATION_SHA,
                    'resume_authorization_id':authorization_id,'updated_at_utc':c.utc()})
        c.atomic_doc(rpath,run)
        env=dict(os.environ)
        env.update({'N0_OJ_QUEUE_ROOT':str(c.QUEUE),'N0_PROFILE_STATE_ROOT':str(c.PROFILE),
                    'N0_GPU_LEASE_PATH':str(c.LEASE),'N0_GPU_TIMING_GATE_PATH':str(c.PROFILE/'timing_gate.json')})
        env['PATH']=str(c.PY.parent)+os.pathsep+env.get('PATH','')
        cmd=[str(c.PY),'-u',str(c.INTEGRATION/'n0_integration/service_supervisor.py'),
            '--queue-root',str(c.QUEUE),'--qualification-out',str(transition/'oj_qualification.json'),
            '--profile-whitelist',str(c.INTEGRATION/'N0_PROFILE_WHITELIST_NONSCIENTIFIC_V1.json'),
            '--gpu-lease-path',str(c.LEASE),'--pool20-run-id',c.RUN_ID,'--idle-seconds','0.5']
        with (transition/'oj_supervisor.stdout.log').open('ab') as log:
            p=subprocess.Popen(cmd,cwd=str(c.INTEGRATION),env=env,stdout=log,stderr=subprocess.STDOUT,
                               creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0),close_fds=True)
        c.write_once(transition/'supervisor_launch.json',{'pid':p.pid,'command':cmd,'integration_release_id':c.INTEGRATION.name,
           'integration_manifest_sha256':c.INTEGRATION_SHA,'authorization_id':authorization_id,'time_utc':c.utc()})
        service=None
        for _ in range(240):
            if p.poll() is not None:break
            try:service=read_json(c.QUEUE/'service_status.json')
            except Exception:service=None
            if service and service.get('release_id')==c.INTEGRATION.name and service.get('state') in {'RUNNING','HOLD','DEGRADED'}:break
            time.sleep(.5)
        c.require(service and service.get('state')=='RUNNING' and p.poll() is None,'ONLINE_NEW_OJ_SERVICE_NOT_RUNNING')
        self.validate_fault_ready_gate()
        run=read_json(rpath);run.update({'state':'RUNNING','updated_at_utc':c.utc()});c.atomic_doc(rpath,run)
        snap=c.bootstrap_module().queue_snapshot(c.QUEUE)
        queued=sorted((x for x in snap['payload']['requests'] if x.get('state')=='QUEUED'),key=lambda x:x['queue_sequence'])
        c.require(queued,'ONLINE_NO_QUEUED_REQUEST_AFTER_OJ_START')
        first=queued[0]
        owner=next((w for w in c.expected_workers() if first['episode_id'] in w['episode_ids']),None)
        c.require(owner,'ONLINE_QUEUED_REQUEST_OWNER_UNKNOWN')
        old=read_json(c.RUN_ROOT/'workers'/(owner['worker_id']+'.json'))
        c.require(old.get('phase') in {'PAUSED','PAUSED_STOP'},'ONLINE_FIRST_WORKER_NOT_PAUSED')
        started=c.spawn_worker(owner,c.RUN_ROOT,resume_only=True,restart_count=0,
             restart_generation=authorization_id,generation_restart_count=0)
        c.write_once(transition/'first_worker_started.json',{'worker_id':owner['worker_id'],'pid':started['pid'],
                          'launch_id':started['launch_id'],'request_id':first['request_id'],'time_utc':c.utc()})
        monitor=c.launch_monitor(c.RUN_ROOT,uuid.uuid4().hex)
        c.write_once(transition/'monitor_launch.json',{'monitor':monitor,'authorization_id':authorization_id,'time_utc':c.utc()})
        receipt={'schema':'N0_POOL20_R814_ONLINE_HANDOFF_RECEIPT_V1','status':'STARTED_ONE_WORKER',
                'authorization_id':authorization_id,'pool_release_id':c.RELEASE_ID,'integration_release_id':c.INTEGRATION.name,
                'run_id':c.RUN_ID,'supervisor_pid':p.pid,'serving_pid':service.get('pid'),
                'monitor_pid':monitor['pid'],'monitor_launcher_pid':monitor['parent_pid'],
                'first_worker_id':owner['worker_id'],'first_worker_pid':started['pid'],
                'first_queued_request_id':first['request_id'],'time_utc':c.utc()}
        c.write_once(transition/'online_start_receipt.json',receipt)
        return receipt
