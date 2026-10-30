from __future__ import annotations
import os, sys, tempfile, unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from n0_integration.cuda_faults import (WindowsJobProcess, classify_fault, fault_recovery_decision,
                                        classify_candidate_runtime_message)


class FaultClassifierTests(unittest.TestCase):
    def test_domains_are_not_interchangeable(self):
        runtime = classify_fault("CUDA_RUNTIME", 700, "KERNEL_SYNC", "illegal address")
        with patch("n0_integration.cuda_faults._cuda_api",
                   return_value=("CUDA_ERROR_ILLEGAL_ADDRESS", "an illegal address was encountered")):
            driver = classify_fault("CUDA_DRIVER", 700, "DRIVER_CALL", "raw driver value")
        self.assertEqual(runtime["domain"], "CUDA_RUNTIME")
        self.assertEqual(runtime["classification"], "ILLEGAL_ADDRESS")
        self.assertEqual(runtime["attribution"], "UNRESOLVED")
        self.assertEqual(driver["domain"], "CUDA_DRIVER")
        self.assertEqual(driver["classification"], "DRIVER_CUDA_ERROR_ILLEGAL_ADDRESS")
        self.assertFalse(driver["score_allowed"])
        self.assertNotEqual(driver["classification"], runtime["classification"])
        self.assertFalse(runtime["score_allowed"])

    def test_cuda_runtime_values_match_frozen_toolkit_enum(self):
        expected = {1:"INVALID_ARGUMENT", 2:"RESOURCE_OUT_OF_MEMORY",
            3:"CUDA_INITIALIZATION_FAILURE", 9:"INVALID_CONFIGURATION",
            98:"INVALID_DEVICE_FUNCTION", 700:"ILLEGAL_ADDRESS",
            701:"LAUNCH_OUT_OF_RESOURCES", 702:"LAUNCH_TIMEOUT",
            710:"DEVICE_SIDE_ASSERT", 714:"HARDWARE_STACK_ERROR",
            715:"ILLEGAL_INSTRUCTION", 716:"MISALIGNED_ADDRESS",
            717:"INVALID_ADDRESS_SPACE", 719:"LAUNCH_FAILURE",
            801:"UNSUPPORTED_OPERATION"}
        for code, classification in expected.items():
            with self.subTest(code=code):
                self.assertEqual(classify_fault("CUDA_RUNTIME", code, "ENUM_TEST")["classification"],
                                 classification)

    def test_unknown_values_preserve_domain_and_raw_value(self):
        for domain in ("CUDA_RUNTIME", "CUDA_DRIVER", "NVCC_EXIT", "WINDOWS_PROCESS_EXIT"):
            fault = classify_fault(domain, 987654, "TEST_STAGE", "unrecognized raw diagnostic")
            self.assertEqual(fault["domain"], domain)
            self.assertEqual(fault["numeric_value"], 987654)
            self.assertEqual(fault["classification"], "UNKNOWN_FAIL_CLOSED")
            self.assertEqual(fault["attribution"], "UNRESOLVED")

    def test_gpu_timeout_poison_and_driver_loss_hold_until_three_controls(self):
        self.assertEqual(fault_recovery_decision(active_processes_after=1,
            health_checks=["PASS"] * 5, reference_checks=[True] * 5)["status"], "HOLD")
        self.assertEqual(fault_recovery_decision(active_processes_after=None,
            health_checks=["PASS"] * 5, reference_checks=[True] * 5)["status"], "HOLD")
        self.assertEqual(fault_recovery_decision(active_processes_after=0,
            health_checks=["BLOCKED"] * 3, reference_checks=[False] * 3)["status"], "HOLD")
        self.assertEqual(fault_recovery_decision(active_processes_after=0,
            health_checks=["PASS"] * 3, reference_checks=[True] * 3)["status"], "READY")

    def test_known_candidate_cuda_failures_and_unknowns(self):
        known = classify_candidate_runtime_message(
            "RuntimeError: CUDA error: invalid configuration argument (cudaErrorInvalidConfiguration)",
            "EVALUATOR_KERNEL_EXECUTION")
        self.assertEqual(known["classification"], "CANDIDATE_LAUNCH_INVALID_CONFIGURATION")
        self.assertEqual(known["attribution"], "CANDIDATE")
        self.assertIsNone(classify_candidate_runtime_message(
            "RuntimeError: CUDA error: unknown future device status 9132", "EVALUATOR_KERNEL_EXECUTION"))

    def test_child_driver_illegal_address_is_candidate_error_with_domain_and_code(self):
        message = ("CUDA error: an illegal memory access was encountered\n"
            "The CUDA driver logged these messages: Returning 700 (CUDA_ERROR_ILLEGAL_ADDRESS) "
            "from cuCtxSynchronize_v2")
        fault = classify_candidate_runtime_message(message, "EVALUATION_CHILD", require_cuda_marker=True)
        self.assertEqual(fault["domain"], "CUDA_DRIVER")
        self.assertEqual(fault["raw_value"], 700)
        self.assertEqual(fault["official_name"], "CUDA_ERROR_ILLEGAL_ADDRESS")
        self.assertEqual(fault["classification"], "CANDIDATE_EXECUTION_ILLEGAL_ADDRESS")
        self.assertEqual(fault["attribution"], "CANDIDATE")
        self.assertFalse(fault["score_allowed"])

    def test_strict_child_mapping_does_not_relabel_plain_python_errors(self):
        self.assertIsNone(classify_candidate_runtime_message(
            "ValueError: invalid argument from user code", "EVALUATION_CHILD", require_cuda_marker=True))


@unittest.skipUnless(os.name == "nt", "Windows Job Objects are required")
class WindowsJobTests(unittest.TestCase):
    def test_cpu_infinite_wait_is_terminated_with_descendants(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            code = ("import subprocess,sys,time; subprocess.Popen([sys.executable,'-c',"
                    "'import time;time.sleep(120)']); time.sleep(120)")
            result = WindowsJobProcess(timeout_seconds=0.5, output_limit_bytes=65536,
                process_limit=8, memory_limit_bytes=1024**3).run(
                [sys.executable, "-I", "-c", code], cwd=directory, env=dict(os.environ),
                stdout_path=str(root / "out.log"), stderr_path=str(root / "err.log"))
            self.assertTrue(result.timed_out)
            self.assertTrue(result.terminated_tree)
            self.assertEqual(result.active_processes_after, 0)


if __name__ == "__main__":
    unittest.main()
