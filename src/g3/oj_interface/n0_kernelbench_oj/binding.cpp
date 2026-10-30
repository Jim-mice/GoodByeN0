#include <torch/extension.h>
#include <ATen/cuda/CUDAContext.h>
#include <cuda_runtime_api.h>
#include <cstdint>

extern "C" size_t n0_workspace_bytes(int64_t, int64_t);
extern "C" void n0_layernorm_forward(const float*, const float*, const float*, float*, void*, size_t, cudaStream_t, int64_t, int64_t, float);

torch::Tensor n0_forward(torch::Tensor x, torch::Tensor weight, torch::Tensor bias, double eps) {
    TORCH_CHECK(x.is_cuda() && weight.is_cuda() && bias.is_cuda(), "all tensors must be CUDA");
    TORCH_CHECK(x.scalar_type() == at::kFloat && weight.scalar_type() == at::kFloat && bias.scalar_type() == at::kFloat, "FP32 required");
    TORCH_CHECK(x.is_contiguous() && weight.is_contiguous() && bias.is_contiguous(), "contiguous tensors required");
    TORCH_CHECK(x.dim() == 4 && x.size(0) == 16 && x.size(1) == 64 && x.size(2) == 256 && x.size(3) == 256, "fixed task shape required");
    TORCH_CHECK(weight.numel() == 64 * 256 * 256 && bias.numel() == weight.numel(), "affine shape mismatch");
    auto y = torch::empty_like(x);
    const size_t workspace_bytes = n0_workspace_bytes(x.size(0), weight.numel());
    TORCH_CHECK(workspace_bytes <= (static_cast<size_t>(1) << 30), "candidate workspace exceeds fixed 1 GiB cap");
    auto workspace = torch::empty({static_cast<int64_t>(workspace_bytes)}, x.options().dtype(at::kByte));
    auto stream = at::cuda::getCurrentCUDAStream(x.get_device()).stream();
    n0_layernorm_forward(x.data_ptr<float>(), weight.data_ptr<float>(), bias.data_ptr<float>(),
                         y.data_ptr<float>(), workspace.data_ptr(), workspace_bytes, stream,
                         x.size(0), weight.numel(), static_cast<float>(eps));
    return y;
}

PYBIND11_MODULE(TORCH_EXTENSION_NAME, m) { m.def("forward", &n0_forward, "N0 CUDA-only LayerNorm forward"); }
