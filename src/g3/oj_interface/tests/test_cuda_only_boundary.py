import unittest
from pathlib import Path

from n0_kernelbench_oj.cuda_only import CUDAOnlyError, PERFORMANCE_FLAGS, build_plan, validate_cuda_candidate
from n0_kernelbench_oj.adapter import KERNELBENCH_FP32_TOLERANCE, validate_candidate_syntax, verify_upstream_source

ROOT = Path(__file__).resolve().parents[1]


class CUDAOnlyBoundaryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        harness = Path(r"D:\projects\kernelagent\runtime\harness\releases\N0_AUTONOMOUS_INFORMATION_ACQUISITION_R4")
        cls.seed = (harness / "protocol" / "SEED_CANDIDATE.cu").read_text(encoding="utf-8")

    def test_seed_static_candidate_gate_passes(self):
        self.assertEqual(validate_cuda_candidate(self.seed)["status"], "STATIC_PASS")
        self.assertEqual(validate_candidate_syntax(self.seed)["status"], "CPU_SYNTAX_PASS")

    def test_python_and_alternate_backend_are_rejected(self):
        self.assertEqual(validate_candidate_syntax("class ModelNew: pass")["status"], "INVALID_SUBMISSION")
        with self.assertRaises(CUDAOnlyError):
            validate_cuda_candidate(self.seed + "\ntriton")

    def test_build_plan_is_frozen_and_no_compilation_occurs(self):
        plan = build_plan(nvcc=Path("nvcc.exe"), source=Path("candidate.cu"), binding=ROOT / "n0_kernelbench_oj" / "binding.cpp", output=Path("candidate.obj"))
        self.assertEqual(plan["status"], "PLAN_ONLY_NO_COMPILATION")
        with self.assertRaises(CUDAOnlyError):
            build_plan(nvcc=Path("nvcc.exe"), source=Path("candidate.cu"), binding=ROOT / "n0_kernelbench_oj" / "binding.cpp", output=Path("candidate.obj"), flags=[*PERFORMANCE_FLAGS, "--use_fast_math"])

    def test_pinned_upstream_checkout_identity(self):
        repo=Path(r"D:\projects\kernelagent\runtime\harness\releases\N0_AUTONOMOUS_INFORMATION_ACQUISITION_R4\third_party\KernelBench_repo")
        result=verify_upstream_source(repo)
        self.assertEqual(result["status"],"PASS")
        self.assertEqual(result["hashes"]["task"],"8D975D7CEE6946513DCFB6BA84C971E53A890F9D9C6F0A4FB828904941F25975")

    def test_fp32_tolerance_matches_pinned_evaluator(self):
        self.assertEqual(KERNELBENCH_FP32_TOLERANCE, 1e-4)
        source = (Path(r"D:\projects\kernelagent\runtime\harness\releases\N0_AUTONOMOUS_INFORMATION_ACQUISITION_R4\third_party\KernelBench_repo\src\kernelbench\eval.py")).read_text(encoding="utf-8")
        self.assertIn("torch.float32: 1e-4", source)


if __name__ == "__main__":
    unittest.main()
