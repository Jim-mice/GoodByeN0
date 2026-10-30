from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from paratera_chat import ALLOWED_EFFORTS, ALLOWED_MODELS, ParateraChatCompletions, ProviderError


OUT = Path(r"C:\NOVELTY_V7_SHARED\audit\paratera_identity_effort_probe.json")
PROMPT = "For this non-scientific connectivity check, reply with the single word READY. Do not use tools or provide code."


def main() -> int:
    client = ParateraChatCompletions()
    records = []
    for model in sorted(ALLOWED_MODELS):
        for effort in sorted(ALLOWED_EFFORTS):
            record = {
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "provider": "PARATERA_API",
                "requested_model": model,
                "requested_effort": effort,
                "api_protocol": "OPENAI_CHAT_COMPLETIONS",
                "scientific_episode": False,
                "oj_submitted": False,
            }
            try:
                payload, evidence = client.complete(
                    model,
                    effort,
                    [{"role": "user", "content": PROMPT}],
                    max_tokens=32,
                )
                record.update(
                    status="RESPONSE_RECEIVED",
                    request_id=evidence.request_id or "UNAVAILABLE",
                    response_id=evidence.response_id or "UNAVAILABLE",
                    returned_model=evidence.returned_model or "UNAVAILABLE",
                    usage=evidence.usage or "UNAVAILABLE",
                    raw_response=payload,
                )
            except ProviderError as exc:
                record.update(status="FAILED", safe_error=str(exc))
            records.append(record)
            print(json.dumps({k: record.get(k) for k in ("requested_model", "requested_effort", "status", "request_id", "response_id", "returned_model", "usage", "safe_error") if k in record}, ensure_ascii=False))
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps({"schema": "NOVELTY_V7_PROVIDER_PROBE_01", "records": records}, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    successes = sum(record.get("status") == "RESPONSE_RECEIVED" for record in records)
    print(f"RESULTS={successes}/{len(records)}")
    print(f"EVIDENCE={OUT}")
    return 0 if successes == len(records) else 1


if __name__ == "__main__":
    raise SystemExit(main())
