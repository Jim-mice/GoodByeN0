import importlib.util
from pathlib import Path

import pytest

RELEASE = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("pool20_r818_controller", RELEASE / "pool20_r8_controller.py")
CONTROLLER = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(CONTROLLER)


def test_live_worker_is_skipped_only_with_release_run_and_worker_identity():
    script = RELEASE / "pool20_worker.py"
    command = f'"{script.resolve()}" --run-id N0_RUN --worker-id W04'
    assert CONTROLLER.worker_command_identity_matches(command, script, "N0_RUN", "W04")
    assert CONTROLLER.classify_resume_worker("RUNNING", True, True, False) == "LIVE"
    assert not CONTROLLER.worker_command_identity_matches(command, RELEASE.parent / "old" / "pool20_worker.py", "N0_RUN", "W04")
    assert not CONTROLLER.worker_command_identity_matches(command, script, "OTHER_RUN", "W04")
    assert not CONTROLLER.worker_command_identity_matches(command, script, "N0_RUN", "W11")


def test_all_twenty_paused_workers_are_start_eligible_without_episode_id_collisions():
    workers = CONTROLLER.expected_workers()
    episode_ids = [episode for worker in workers for episode in worker["episode_ids"]]
    assert len(workers) == 20
    assert len(episode_ids) == len(set(episode_ids)) == 100
    assert all(CONTROLLER.classify_resume_worker("PAUSED_STOP", False, False, False) == "START" for _ in workers)


@pytest.mark.parametrize("phase", ["PAUSED", "PAUSED_STOP", "CRASHED"])
def test_exhausted_w02_w08_retry_workers_remain_isolated(phase):
    for worker_id in ("W02", "W08"):
        assert CONTROLLER.classify_resume_worker(phase, False, False, True) == "ISOLATED"


@pytest.mark.parametrize("gate,run_stop,queue_stop,reason", [
    ("HOLD", False, False, "RESUME_REMAINING_GATE_NOT_READY"),
    ("READY", True, False, "RESUME_REMAINING_STOP_MARKER_PRESENT"),
    ("READY", False, True, "RESUME_REMAINING_STOP_MARKER_PRESENT"),
])
def test_hold_and_stop_markers_fail_before_worker_dispatch(gate, run_stop, queue_stop, reason):
    with pytest.raises(RuntimeError, match=reason):
        CONTROLLER.validate_resume_gate_preflight(gate, run_stop, queue_stop)


def test_completed_worker_is_never_started_twice():
    assert CONTROLLER.classify_resume_worker("COMPLETED", False, False, False) == "COMPLETED"


def test_active_worker_with_dead_or_mismatched_pid_fails_closed():
    with pytest.raises(RuntimeError, match="PID_NOT_LIVE"):
        CONTROLLER.classify_resume_worker("RUNNING", False, False, False)
    with pytest.raises(RuntimeError, match="IDENTITY_INVALID"):
        CONTROLLER.classify_resume_worker("RUNNING", True, False, False)


def test_unexpected_worker_state_is_rejected():
    with pytest.raises(RuntimeError, match="UNEXPECTED_WORKER_STATE"):
        CONTROLLER.classify_resume_worker("STARTING", False, False, False)
