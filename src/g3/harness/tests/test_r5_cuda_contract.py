import hashlib
import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from n0_autonomous.cuda_contract import CUDAContractError, PERFORMANCE_FLAGS, compile_command, validate_candidate_source, validate_flags


class R5CudaContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.seed = (ROOT / "protocol" / "SEED_CANDIDATE.cu").read_text(encoding="utf-8")

    def test_seed_is_real_multi_kernel_cuda_and_hash_matches_episode_template(self):
        result = validate_candidate_source(self.seed)
        self.assertGreaterEqual(result["kernel_count"], 3)
        template = ROOT / "tests" / "fixtures" / "N0_AUTONOMOUS_INFORMATION_ACQUISITION_R4" / "template" / "candidate.cu"
        self.assertEqual(hashlib.sha256((ROOT / "protocol" / "SEED_CANDIDATE.cu").read_bytes()).hexdigest().upper(),
                         hashlib.sha256(template.read_bytes()).hexdigest().upper())
        contract = json.loads((ROOT / "protocol" / "G3_EXPERIMENT_CONFIGURATION_R1.json").read_text(encoding="utf-8"))
        self.assertEqual(contract["seed_candidate_id"], "C0000")
        from n0_autonomous.parallel_runtime import N0_SEED_SHA256
        self.assertEqual(result["source_sha256"], N0_SEED_SHA256)

    def test_multiple_kernels_are_allowed(self):
        altered = self.seed + "\n__global__ void another_kernel(float* p) { if (threadIdx.x == 0) p[0] = 1.0f; }\n"
        self.assertGreaterEqual(validate_candidate_source(altered)["kernel_count"], 4)

    def test_python_dsl_and_binding_escape_are_rejected(self):
        for token in ("torch::nn", "triton", "PYBIND11_MODULE"):
            with self.subTest(token=token), self.assertRaises(CUDAContractError):
                validate_candidate_source(self.seed + "\n" + token)

    def test_normal_cuda_source_flexibility_is_not_rejected(self):
        source=self.seed+"\n#include <algorithm>\n__global__ void extra(float* p) { extern __shared__ float sm[]; asm volatile(\"mov.u32 %0, %tid.x;\" : \"=r\"(sm[0])); }\n"
        self.assertGreaterEqual(validate_candidate_source(source)["kernel_count"],4)

    def test_flags_are_exact(self):
        validate_flags(list(PERFORMANCE_FLAGS))
        with self.assertRaises(CUDAContractError):
            validate_flags([*PERFORMANCE_FLAGS, "--use_fast_math"])
        command = compile_command(Path("nvcc.exe"), Path("candidate.cu"), Path("candidate.obj"), list(PERFORMANCE_FLAGS))
        self.assertIn("-gencode=arch=compute_120,code=sm_120", command)

    def test_tool_set_and_prompt_keep_queries_optional(self):
        schema = json.loads((ROOT / "protocol" / "TOOL_SCHEMAS.json").read_text(encoding="utf-8"))
        names = [x["function"]["name"] for x in schema["tools"]]
        self.assertEqual(names, ["read_workspace_file", "edit_candidate", "query_candidate_history", "query_hardware_skill"])
        prompt = (ROOT / "protocol" / "BASE_PROMPT.md").read_text(encoding="utf-8")
        self.assertIn("No query is required before editing, continuing, or stopping", prompt)


if __name__ == "__main__":
    unittest.main()
