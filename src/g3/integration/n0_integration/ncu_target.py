from __future__ import annotations
import argparse
import importlib.util
import os
from pathlib import Path
import torch

def main() -> None:
    p=argparse.ArgumentParser()
    p.add_argument('--extension',required=True)
    p.add_argument('--pid-file',required=True)
    a=p.parse_args()
    Path(a.pid_file).write_text(str(os.getpid()),encoding='ascii')
    spec=importlib.util.spec_from_file_location(Path(a.extension).stem,a.extension)
    if spec is None or spec.loader is None: raise RuntimeError('PROFILE_EXTENSION_LOAD_FAILED')
    module=importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    torch.cuda.set_device(0)
    shape=(16,64,256,256); norm=(64,256,256)
    x=torch.randn(shape,device='cuda',dtype=torch.float32,generator=torch.Generator(device='cuda').manual_seed(20261020))
    w=torch.ones(norm,device='cuda',dtype=torch.float32); b=torch.zeros(norm,device='cuda',dtype=torch.float32)
    y=module.forward(x,w,b,1e-5)
    torch.cuda.synchronize()
    ref=torch.nn.functional.layer_norm(x,norm,w,b,1e-5)
    torch.cuda.synchronize()
    if not bool(torch.isfinite(y).all().item()) or not bool(torch.allclose(y,ref,atol=1e-4,rtol=1e-4)):
        raise RuntimeError('PROFILE_CANDIDATE_CORRECTNESS_CHANGED')
    print('N0_PROFILE_TARGET_OK')

if __name__=='__main__': main()
