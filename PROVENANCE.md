# Source lineage and status

This document records upstream Release identities. The repository contains source subsets and is not itself any of these Releases; all runtime state, measurements, receipts, database files, and original Release manifests were excluded.

| Component | Source Release | Source Manifest SHA256 | Status in N0 |
|---|---|---|---|
| Pool | N0_KERNEL_AGENT_POOL20_R8_18_9_R8_03_WORKER_RELEASE_BINDING_FIX_20261027 | 8265028941970E9104DB464251AD278F0B84C7D5945BD10640A7D05311344422 | deployed to archived R8.03 |
| Integration | N0_PRODUCTION_INTEGRATION_R12_10_4_R9_6_R8_03_NATURAL_EXIT_HANDOFF_20261027 | AE3F53C1A59A431B8C340AA7F755A525C3D13EE1865C4658AD83F9E96AB88BF0 | deployed to archived R8.03 |
| OJ | N0_KERNELBENCH_L1_40_LAYERNORM_R6_5_FAULT_CONTAINED | 8894FF6B14F7D76D7A4FC7B2033E76DE55894EA0EA7DDBBC2A3B0F7FB010A22C | deployed to archived R8.03 |
| Harness | N0_AUTONOMOUS_INFORMATION_ACQUISITION_R7_3_POOL20_R5_FILELIST_FIX | 0E7D76E6625557EA02D4A79B09EB6F7BEDCC0DB5B536D470DC148433E57055DA | deployed to archived R8.03 |
| Knowledge | N0_KB_RETRIEVAL_V2_TARGETED_R5_SEMANTIC_FIX_20261024 | 3326A78555518874FF22F3E8637E20AD2600547B1A6FD8D766D3DAD96F14FAA3 | R5 facts retained; expert assets omitted |
| G3 Harness | N0_G3_RUNTIME_INTEGRATION_R4_5_PROVIDER_MANIFEST_CASEFIX | D434B077CB79CCF4C776FAF6BA557D351DDAA81FAC124FAA785CB9C8C4A9A812 | staged diagnostic code |
| G3 Integration | N0_G3_INTEGRATION_R4_6_SSE_RESPONSE_BRIDGE | 770BF0DAFA42304C4E400E8E3E11843AE78AC1C810900403F575C439BBE0C242 | staged; real SSE partial diagnostic only |
| Provider | N0_PARATERA_PROVIDER_R20_G3_TREATMENT_ROUTE_R5_SSE_CASEFIX | 97F44B4846CFC39959E8ABDA6C41ECF8EDB0F978F13ECB3431EE257F87973651 | G3 route; not formal-run qualified |
| G3 OJ source subset | N0_KERNELBENCH_L1_40_LAYERNORM_R6_6_KERNEL_NAME_NCU_FILTER | BA5D6E36BC79CF4735F7DCF60E3C99F92AE96C50E38C97314281E0513E662A75 | staged source subset |
| Composition | N0 G3 R4.6 composition | C10070751945375BEE110C091655FF44CF27A1AE5EC7E23DEAB0EA8623A8FE4D | diagnostic composition only |

The G3 diagnostic completed C0000 OJ and NCU Profile and a 3-control Reference recovery sequence. The first Provider request completed; two Hardware tool results reached the next request. That next request remained OUTCOME_UNKNOWN, and the Agent did not query Profile. Formal G3 was never started.

The project root was not a Git repository. No old Git history was copied. No remote is configured.

Archive preparation applied whitespace-only normalization to files listed in `ARCHIVE_SOURCE_MANIFEST.json` with `archive_transform`. Their pre-normalization SHA256 is retained as `source_sha256`; no code tokens or logic were changed.
