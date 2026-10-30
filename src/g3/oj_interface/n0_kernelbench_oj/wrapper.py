"""Controller-owned KernelBench ModelNew wrapper; candidate arithmetic stays in CUDA."""
import importlib
import os
import torch
from torch import nn


class ModelNew(nn.Module):
    def __init__(self, normalized_shape=(64, 256, 256), eps=1e-5):
        super().__init__()
        if tuple(normalized_shape) != (64, 256, 256):
            raise ValueError("N0_FIXED_NORMALIZED_SHAPE_MISMATCH")
        self.eps = float(eps)
        self.register_buffer("weight", torch.ones((64, 256, 256), dtype=torch.float32))
        self.register_buffer("bias", torch.zeros((64, 256, 256), dtype=torch.float32))
        self._n0_extension = None

    def bind_extension(self, extension):
        self._n0_extension = extension

    def forward(self, x):
        if self._n0_extension is None:
            module_name = os.environ.get("N0_EXTENSION_MODULE")
            if not module_name:
                raise RuntimeError("N0_FIXED_EXTENSION_NOT_BOUND")
            self._n0_extension = importlib.import_module(module_name)
        if tuple(x.shape) != (16, 64, 256, 256) or x.dtype != torch.float32:
            raise ValueError("N0_FIXED_TASK_INPUT_MISMATCH")
        return self._n0_extension.forward(x, self.weight, self.bias, self.eps)
