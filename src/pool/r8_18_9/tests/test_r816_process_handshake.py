from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import tempfile
import time
import unittest
import uuid
from pathlib import Path

RELEASE=Path(__file__).resolve().parents[1]
CONTROLLER_PATH=RELEASE/"pool20_r8_controller.py"
spec=importlib.util.spec_from_file_location("pool20_r815_controller_test",CONTROLLER_PATH)
controller=importlib.util.module_from_spec(spec)
spec.loader.exec_module(controller)


class R815WorkerLaunchHandshakeTest(unittest.TestCase):
    def test_real_windows_child_uses_immutable_launch_record_and_no_provider(self):
        self.assertEqual(os.name,"nt")
        run_id=controller.RUN_ID
        item=next(x for x in controller.expected_workers() if x["worker_id"]=="W15")
        launch_id=uuid.uuid4().hex
        with tempfile.TemporaryDirectory(prefix="n0_r815_handshake_") as temp:
            root=Path(temp)/run_id
            (root/"workers").mkdir(parents=True)
            (root/"launches").mkdir()
            queue=Path(temp)/"queue";queue.mkdir()
            profile=Path(temp)/"profile";profile.mkdir()
            episode_root=Path(temp)/"episodes";episode_root.mkdir()
            manifest={"run_id":run_id,"workers":[item]}
            (root/"run_manifest.json").write_text(json.dumps(manifest),encoding="utf-8")
            previous={"worker_id":"W15","phase":"PAUSED","completed_episode_ids":[],"restart_count":0}
            record={**item,"run_id":run_id,"pid":None,"phase":"STARTING","launch_id":launch_id,
                    "release_id":controller.RELEASE_ID,"manifest_sha256":controller.release_check()["manifest_sha256"],
                    "previous_state":previous,"launcher_pid":os.getpid(),"controller_pid":os.getpid(),
                    "launch_started_at_utc":controller.utc()}
            (root/"workers"/"W15.json").write_text(json.dumps(record),encoding="utf-8")
            (root/"launches"/("W15."+launch_id+".json")).write_text(json.dumps(record),encoding="utf-8")
            env=dict(os.environ)
            env.update({"N0_POOL20_RUN_ID":run_id,"N0_POOL20_RUN_ROOT":str(root),
                "N0_POOL20_EPISODE_ROOT":str(episode_root),"N0_POOL20_STOP_FILE":str(root/"STOP_REQUESTED"),
                "N0_OJ_QUEUE_ROOT":str(queue),"N0_PROFILE_STATE_ROOT":str(profile),
                "N0_GPU_LEASE_PATH":str(controller.LEASE),"N0_POOL20_HANDSHAKE_TEST":"1",
                "N0_POOL20_HANDSHAKE_TEST_ROOT":str(root)})
            cmd=[str(controller.PY),"-u",str(RELEASE/"pool20_worker.py"),"--run-id",run_id,
                 "--worker-id","W15","--launch-id",launch_id,"--resume-only","--handshake-only"]
            log=(root/"worker.log").open("ab")
            child=subprocess.Popen(cmd,cwd=str(controller.ROOT),env=env,stdout=log,stderr=subprocess.STDOUT,
                creationflags=getattr(subprocess,"CREATE_NEW_PROCESS_GROUP",0)|getattr(subprocess,"CREATE_NO_WINDOW",0),close_fds=True)
            log.close()
            handshake=controller.wait_worker_handshake(item,root,launch_id,child.pid,os.getpid(),child,timeout=15)
            self.assertEqual(handshake["launch_id"],launch_id)
            self.assertEqual(handshake["release_id"],controller.RELEASE_ID)
            self.assertEqual(handshake["manifest_sha256"],controller.release_check()["manifest_sha256"])
            self.assertEqual(child.wait(timeout=10),0)
            final=json.loads((root/"workers"/"W15.json").read_text(encoding="utf-8"))
            self.assertEqual(final["phase"],"COMPLETED")
            self.assertTrue(final["handshake_test_only"])
            self.assertEqual(set(final["completed_episode_ids"]),set(item["episode_ids"]))

    def test_monitor_launch_nonce_matches_actual_popen_pid_in_isolated_root(self):
        self.assertEqual(os.name,"nt")
        with tempfile.TemporaryDirectory(prefix="n0_r816_monitor_handshake_") as temp:
            result=controller.launch_monitor(Path(temp),test_only=True)
            self.assertEqual(result["release_id"],controller.RELEASE_ID)
            self.assertEqual(result["manifest_sha256"],controller.release_check()["manifest_sha256"])
            self.assertTrue(result["launch_id"])
            identity=json.loads(Path(result["identity_path"]).read_text(encoding="utf-8"))
            self.assertEqual(identity["pid"],result["pid"])
            self.assertEqual(identity["parent_pid"],result["parent_pid"])
            self.assertEqual(identity["launch_id"],result["launch_id"])
            self.assertIn(result["launch_id"],identity["command_line"])
            self.assertTrue(Path(identity["command_line"].split()[2].strip('"')).name=="pool20_r8_controller.py")


if __name__=="__main__":
    unittest.main()
