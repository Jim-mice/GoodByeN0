#include <cuda_runtime.h>
#include <cstdint>
#include <cstddef>

extern "C" size_t n0_workspace_bytes(int64_t batch, int64_t) {
    return static_cast<size_t>(batch) * 2 * sizeof(float);
}

__global__ void n0_mean_kernel(const float* x, float* stats, int64_t n) {
    const int sample = blockIdx.x;
    double sum = 0.0;
    for (int64_t i = threadIdx.x; i < n; i += blockDim.x) {
        sum += static_cast<double>(x[static_cast<int64_t>(sample) * n + i]);
    }
    extern __shared__ double partial[];
    partial[threadIdx.x] = sum;
    __syncthreads();
    for (unsigned int stride = blockDim.x / 2; stride > 0; stride >>= 1) {
        if (threadIdx.x < stride) partial[threadIdx.x] += partial[threadIdx.x + stride];
        __syncthreads();
    }
    if (threadIdx.x == 0) stats[sample * 2] = static_cast<float>(partial[0] / static_cast<double>(n));
}

__global__ void n0_variance_kernel(const float* x, float* stats, int64_t n) {
    const int sample = blockIdx.x;
    const double mean = static_cast<double>(stats[sample * 2]);
    double sum = 0.0;
    for (int64_t i = threadIdx.x; i < n; i += blockDim.x) {
        const double d = static_cast<double>(x[static_cast<int64_t>(sample) * n + i]) - mean;
        sum += d * d;
    }
    extern __shared__ double partial[];
    partial[threadIdx.x] = sum;
    __syncthreads();
    for (unsigned int stride = blockDim.x / 2; stride > 0; stride >>= 1) {
        if (threadIdx.x < stride) partial[threadIdx.x] += partial[threadIdx.x + stride];
        __syncthreads();
    }
    if (threadIdx.x == 0) stats[sample * 2 + 1] = static_cast<float>(partial[0] / static_cast<double>(n));
}

__global__ void n0_output_kernel(const float* x, const float* weight, const float* bias,
                                 const float* stats, float* y, int64_t n, int64_t total, float eps) {
    const int64_t i = static_cast<int64_t>(blockIdx.x) * blockDim.x + threadIdx.x;
    if (i >= total) return;
    const int sample = static_cast<int>(i / n);
    const float mean = stats[sample * 2];
    const float variance = stats[sample * 2 + 1];
    y[i] = (x[i] - mean) / sqrtf(variance + eps) * weight[i % n] + bias[i % n];
}

extern "C" void n0_layernorm_forward(const float* x, const float* weight, const float* bias,
                                      float* y, void* workspace, size_t workspace_bytes, cudaStream_t stream,
                                      int64_t batch, int64_t features, float eps) {
    if (workspace == nullptr || workspace_bytes < static_cast<size_t>(batch) * 2 * sizeof(float)) return;
    float* stats = static_cast<float*>(workspace);
    const int threads = 256;
    const size_t shared = static_cast<size_t>(threads) * sizeof(double);
    n0_mean_kernel<<<static_cast<unsigned int>(batch), threads, shared, stream>>>(x, stats, features);
    n0_variance_kernel<<<static_cast<unsigned int>(batch), threads, shared, stream>>>(x, stats, features);
    const int64_t total = batch * features;
    const int blocks = static_cast<int>((total + threads - 1) / threads);
    n0_output_kernel<<<blocks, threads, 0, stream>>>(x, weight, bias, stats, y, features, total, eps);
}
