import json
import re
from pathlib import Path

from n0_autonomous.prompt_bundle import EXPECTED_TOOLS, RELEASE_ID, load_prompt_bundle


def test_g3_r4_prompt_is_blinded_and_four_tool_contract(tmp_path):
    root = Path(__file__).resolve().parents[1]
    bundle = load_prompt_bundle(root)
    visible = bundle.system_prompt + "\n" + bundle.task_user_message + "\n" + json.dumps(bundle.tools)
    assert RELEASE_ID == "N0_G3_RUNTIME_INTEGRATION_R4_5_PROVIDER_MANIFEST_CASEFIX"
    assert [x["function"]["name"] for x in bundle.tools] == EXPECTED_TOOLS
    assert not re.search(r"(?i)N0_AUTONOMOUS|query_expert|expert knowledge|candidate budget|remaining budget|30 logical submissions", visible)
    assert "G3" not in visible
