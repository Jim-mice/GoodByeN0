import importlib
import json
import os
import sys
import tempfile
from pathlib import Path

ROOT=Path(r"D:\projects\kernelagent")
R=ROOT/"runtime"
POOL=R/"deployment/releases/N0_KERNEL_AGENT_POOL20_R8_18_9_R8_03_WORKER_RELEASE_BINDING_FIX_20261027"
HARNESS=R/"harness/releases/N0_AUTONOMOUS_INFORMATION_ACQUISITION_R7_3_POOL20_R5_FILELIST_FIX"
INTEGRATION=R/"integrations/N0_PRODUCTION_INTEGRATION_R12_10_4_R9_6_R8_03_NATURAL_EXIT_HANDOFF_20261027"
OJ=R/"oj/releases/N0_KERNELBENCH_L1_40_LAYERNORM_R6_5_FAULT_CONTAINED"

def main():
    sys.path[:0]=[str(POOL),str(HARNESS),str(INTEGRATION),str(OJ),str(HARNESS/"third_party/KernelBench_repo/src")]
    with tempfile.TemporaryDirectory(prefix="n0-r8-import-probe-") as temp:
        os.environ["N0_OJ_QUEUE_ROOT"]=str(Path(temp)/"diagnostic-queue")
        modules={name:importlib.import_module(name) for name in ("pool20_shared_oj","pool20_episode_manager","pool20_r8_controller","n0_autonomous","n0_integration.service_supervisor","n0_kernelbench_oj.adapter")}
    origins={name:str(Path(module.__file__).resolve()) for name,module in modules.items()}
    expected={"pool20_shared_oj":POOL/"pool20_shared_oj.py","pool20_episode_manager":POOL/"pool20_episode_manager.py","pool20_r8_controller":POOL/"pool20_r8_controller.py","n0_autonomous":HARNESS/"n0_autonomous/__init__.py","n0_integration.service_supervisor":INTEGRATION/"n0_integration/service_supervisor.py","n0_kernelbench_oj.adapter":OJ/"n0_kernelbench_oj/adapter.py"}
    for name,path in expected.items():
        if Path(origins[name]).resolve()!=path.resolve():raise RuntimeError("MODULE_ORIGIN_MISMATCH:"+name+":"+origins[name])
    for path in (POOL/"pool20_worker.py",POOL/"pool20_episode_manager.py",POOL/"pool20_runner_template.py",POOL/"pool20_shared_oj.py",POOL/"Start-N0Pool20.ps1",POOL/"Start-N0Pool20OJService.ps1"):
        text=path.read_text(encoding="utf-8-sig")
        if "R12_10_2_PROFILE_IDENTITY_SCOPE_FIX" in text or "535A5A2C03453CBB6E01E529454D94E3BF02EAAD7C16D9DA7062B5F8124EBD52" in text:
            raise RuntimeError("UNAUTHORIZED_INTEGRATION_REFERENCE:"+str(path))
        if "N0_KERNEL_AGENT_POOL20_R7_3_1_CONFIG_BINDING_REPAIR" in text:
            raise RuntimeError("UNAUTHORIZED_POOL_RELEASE_REFERENCE:"+str(path))
    print(json.dumps({"status":"PASS","python":sys.executable,"origins":origins,"pool_release_id":POOL.name,
                      "integration_release_id":INTEGRATION.name,"integration_manifest_sha256":"AE3F53C1A59A431B8C340AA7F755A525C3D13EE1865C4658AD83F9E96AB88BF0"},indent=2))

if __name__=="__main__":main()
