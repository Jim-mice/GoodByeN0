# R5 targeted source record

Release-specific source record for the facts and retrieval repairs added in R5. All links below point to versioned NVIDIA documentation.

| Knowledge item | Source and version | Section / stable locator | Scope and limitations |
|---|---|---|---|
| `cc12.max_threads_per_block` | CUDA C++ Programming Guide 13.2.0 | Section 5.1, Compute Capabilities, Table 30 | CC 12.x maximum block size is 1024 threads. This is a launch limit, not an optimal block size; the selected device Runtime API can be queried for actual dimensions. |
| `cc12.max_threads_per_sm` | CUDA C++ Programming Guide 13.2.0 | Section 5.1, Compute Capabilities, Table 30 | CC 12.x maximum resident threads per SM is 1536. Actual kernel residency can be lower due to block, warp, register, and shared-memory limits. |
| `cuda.runtime.max_shared_memory_per_block_optin` | CUDA Runtime API 13.2.0 | `cudaDevAttrMaxSharedMemoryPerBlockOptin` in `group__CUDART__TYPES` | Reports opt-in per-block shared-memory capacity in bytes; the value may vary by chip and must be queried on the selected device. |
| `cuda_v2.shuffle` (R5 wording) | CUDA C++ Programming Guide 13.2.0 | Section 5.4, C/C++ Language Extensions, Warp Shuffle Functions | Each caller's lane bit must be present; all non-exited named lanes must execute the same intrinsic with the same mask. Reading from a non-participating lane is undefined. Applies to the documented `_sync` warp shuffle intrinsics. |
| Existing `cc12.shared_memory` alias | CUDA C++ Programming Guide 13.2.0 | Section 20.10.3, Shared Memory; Appendix Table 31, CC 12.x | 99 KiB maximum per block and 128 KiB per SM shared-memory capacity. Values are CC 12.x limits, not measurements for one laptop implementation. |
| Existing `rtx5060l.sku.sm_count` unknown answer | CUDA Runtime API 12.8.1 | `cudaDeviceProp::multiProcessorCount` | Runtime query path only. No SKU-specific SM count is asserted by this item. |
| `EXP_GUIDE_002` LayerNorm numerical guidance | CUDA C++ Programming Guide 13.2.0; Welford (1962), *Technometrics* 4(3), pp. 419-420; Chan, Golub, and LeVeque (1983), *The American Statistician* 37(3), pp. 242-247; NVIDIA Transformer Engine 2.8.0 | CUDA Guide section 5.5.1.8; Welford DOI `10.1080/00401706.1962.10490022`; Chan et al. DOI `10.2307/2683386`; Transformer Engine Normalization / LayerNorm | Sources support floating-point stability cautions and algorithm alternatives. They do not establish a universally best LayerNorm implementation or performance. Preserve task dtype/epsilon semantics and validate. |

## Source URLs

- CUDA Programming Guide 13.2.0, Compute Capabilities: https://docs.nvidia.com/cuda/archive/13.2.0/cuda-programming-guide/05-appendices/compute-capabilities.html
- CUDA Programming Guide 13.2.0, C/C++ Language Extensions: https://docs.nvidia.com/cuda/archive/13.2.0/cuda-programming-guide/05-appendices/cpp-language-extensions.html
- CUDA Runtime API 13.2.0, Runtime attributes: https://docs.nvidia.com/cuda/archive/13.2.0/cuda-runtime-api/group__CUDART__TYPES.html
- CUDA Runtime API 12.8.1, `cudaDeviceProp`: https://docs.nvidia.com/cuda/archive/12.8.1/cuda-runtime-api/structcudaDeviceProp.html
- CUDA Programming Guide 13.2.0: https://docs.nvidia.com/cuda/archive/13.2.0/cuda-programming-guide/index.html
- NVIDIA Blackwell Tuning Guide 13.2.0: https://docs.nvidia.com/cuda/archive/13.2.0/blackwell-tuning-guide/index.html
- Welford (1962): https://doi.org/10.1080/00401706.1962.10490022
- Chan, Golub, and LeVeque (1983): https://doi.org/10.2307/2683386
- NVIDIA Transformer Engine 2.8.0 LayerNorm reference: https://docs.nvidia.com/deeplearning/transformer-engine-releases/release-2.8/user-guide/api/c/normalization.html
