# Pinned KernelBench semantics and N0 safeguards

## Pinned source

This release uses the KernelBench repository copy pinned by Harness R6 at commit `423217d9fda91e0c2d67e4a43bf62f96f6d104f1`. The corresponding timing implementation is `third_party/KernelBench_repo/src/kernelbench/timing.py`; evaluation and correctness are in `src/kernelbench/eval.py`. No upstream source is patched by this OJ release.

## Official behavior compared with N0

| Area | Pinned KernelBench behavior | N0 behavior | Disposition |
|---|---|---|---|
| Correctness | CUDA backend, FP32, one correctness trial for this task; `torch.allclose` uses the pinned FP32 tolerance `1e-4` for both absolute and relative tolerance. | Same evaluator and arguments; Candidate extension is precompiled before live timing controls. | **C:** one trial matches the pinned evaluator contract. Multi-input CUDA correctness qualification is useful additional coverage, not a timing-score semantic mismatch. |
| Warmup and samples | The pinned CUDA Event timing function defaults to 3 warmups and 10 measured trials; `discard_first=1` is accepted but unused by that function. | Evaluator follows those defaults. Independent Reference Control calls `measure_ref_program_time` with warmup 3, trials 10, and discards the first trial. | **B:** N0 preserves the upstream evaluator and separately records a 10-sample Control. The independent Control and evaluator Reference use the same timer source; stage ordering remains checked by the existing 2% guard. |
| CUDA Event timing | Events are recorded around each forward call; CUDA synchronization occurs after the trial loop before elapsed times are read. | The official evaluator remains the Candidate and evaluator Reference timer. Raw Event samples are captured and checked against the reported means. | **B:** raw-sample integrity checks and frozen environment identity. |
| Cache handling | The CUDA Event timing path obtains Triton's benchmark cache buffer and clears L2 before each measured invocation. | Same pinned timer path. The independent reference helper also invokes KernelBench's `measure_ref_program_time`, which clears L2 through that timing function. | **C:** no N0 cache-policy substitution. |
| Reference/Candidate order | The evaluator measures the correct Candidate first and its reference comparison afterward, in separate calls. | R6 precompiles first, then records Reference Control, runs correctness and official timing, and records post-evaluator control. | **B:** compile-before-control and pre/post control gates detect phase drift. NCU must run only after the terminal OJ receipt and under a new Lease. |
| Speedup | KernelBench reports evaluator reference runtime divided by Candidate runtime. | Score is qualified frozen Reference runtime divided by Candidate runtime, within a Baseline ID and Score Version. The live evaluator Reference and Controls are trust checks only. | **B:** fixed Baseline makes cross-Episode Score comparable; drift invalidates the measurement instead of changing the numerator. |

## Required fixes and limits

No discrepancy was found in the pinned evaluator's CUDA Event, correctness, or cache semantics that requires patching upstream. The prior compile-order issue is repaired in R6.2 and retained. The 2% Reference path guard is an N0 engineering gate; it is not represented as a KernelBench rule. The frozen-baseline identity and expiry checks, raw-timer verification, Lease, immutable receipts, and independent Best recheck are additional N0 safeguards.

Candidate correctness remains one official deterministic trial in the scored path. The separate multi-input Seed qualification is not equivalent to multi-input qualification of every Candidate; this is a known coverage limit and must remain visible in future protocol reviews.

## Test runner

The project-frozen dependency file pins `pytest==9.1.1`. Run each release suite from its own root so imports resolve against that release copy:

```powershell
D:\projects\kernelagent\.venv\Scripts\python.exe -m pytest tests -q
```
