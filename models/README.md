# Saved models

| Version | Persistence lag | Status |
|---|---|---|
| `v20261010` | 60 days | **Current. Use this.** |
| `v20261008` | 30 days | Superseded: with the observed UCDP release delay (40 days on 2026-10-10) it could not be scored. Kept for provenance of the lag-30 evaluation. |

Each version contains `prod_clf`, `prod_reg`, `shadow_clf` (Spark `PipelineModel`s) and `contract.json`.
`shadow_clf` (GDELT features) is **shadow only and must never raise an alert.**
Checksum (`.crc`) and `_SUCCESS` files are intentionally not committed. See `docs/person_b_handoff.md`.
