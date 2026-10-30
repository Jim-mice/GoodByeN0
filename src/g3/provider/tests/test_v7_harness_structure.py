"""Focused V7 treatment visibility, history and isolation checks; no API/GPU calls."""
import json, hashlib, tempfile
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from canonical_harness.treatment import load_treatments
from canonical_harness.history_builder import build_history
from canonical_harness.runner import CanonicalRunner

ROOT = Path(__file__).resolve().parents[1]

def test_four_treatment_isolation_and_guide():
    ts = load_treatments(ROOT)
    assert set(ts) == {"G_CONTROL","H_STATIC","H_RETRIEVAL_V7","E_EXPERT"}
    assert ts["G_CONTROL"].context_path is None and ts["G_CONTROL"].extra_tools == ()
    assert ts["H_STATIC"].context_path and ts["H_STATIC"].extra_tools == ()
    assert ts["H_RETRIEVAL_V7"].context_path is None and ts["H_RETRIEVAL_V7"].extra_tools == ("query_hardware_skill",)
    assert ts["E_EXPERT"].context_path and ts["E_EXPERT"].extra_tools == ()
    guide = (ROOT/"treatments"/"H_RETRIEVAL_V7"/"tool_usage_guide.md").read_text(encoding="utf-8")
    assert "query_hardware_skill" in guide and "Optional" in guide
    assert "tool_usage_guide.md" not in (ROOT/"treatments"/"G_CONTROL"/"manifest.json").read_text(encoding="utf-8")

def test_each_episode_bootstraps_complete_seed_history_in_isolated_workspace():
    ts=load_treatments(ROOT); seed=(ROOT/"canonical_harness"/"canonical_seed.cu").read_bytes()
    seed_sha=hashlib.sha256(seed).hexdigest(); tmp=Path(tempfile.mkdtemp(prefix="v7_harness_visibility_"))
    for treatment_id in ("G_CONTROL","H_STATIC","H_RETRIEVAL_V7","E_EXPERT"):
        episode=tmp/treatment_id; runner=CanonicalRunner(ROOT,ts[treatment_id],seed,replicate_id=treatment_id,run_id="V7_STRUCTURE_"+treatment_id)
        runner.init_workspace(episode/"workspace",episode/"controller_private")
        ws=episode/"workspace"; history=json.loads((ws/"visible_history.json").read_text(encoding="utf-8"))
        assert history["history_complete"] is True
        assert len(history["candidates"])==1 and history["candidates"][0]["candidate_sha256"]==seed_sha
        assert history["candidates"][0]["full_source_text"]==seed.decode("utf-8")
        assert (ws/"candidate.cu").read_bytes()==seed
        assert (ws/"task.md").read_bytes()==(ROOT/"canonical_harness"/"canonical_task.md").read_bytes()
        assert (ws/"tool_usage_guide.md").is_file() == (treatment_id=="H_RETRIEVAL_V7")
    assert len({p.name for p in tmp.iterdir()})==4

if __name__=="__main__":
    test_four_treatment_isolation_and_guide()
    test_each_episode_bootstraps_complete_seed_history_in_isolated_workspace()
    print("V7_HARNESS_STRUCTURE_PASSED=2 V7_HARNESS_STRUCTURE_FAILED=0")
