# Person B → Person C Handoff

Model version: **v20261010** (persistence lag 60 days) · Written 2026-10-10 · Owner: Person B
`v20261008` (lag 30) is **superseded**: with the UCDP release lag we observed, a 30-day lag could not be scored. Use v20261010.

## 0. Read this first

1. **The production models use no GDELT.** They use two UCDP persistence features. The GDELT
   model (`shadow_clf`) failed our pre-registered improvement rules and is **shadow only: it must never
   raise an alert.** See `docs/person_b_results.md`.
2. **Probabilities are uncalibrated risk scores for ranking.** Isotonic calibration was tested and
   rejected. Do not label them "probability of escalation = 72%" on the dashboard.
3. **Live GDELT features are NOT yet reproducible** by the current `live_landing.py` (section 5).
   This blocks the shadow model, not the production model.
4. **UCDP freshness limits scoring.** The newest release we have (Aug 2026) covers through 2026-08-31. With the model's 60-day lag, days up to 2026-10-30 can be scored using it; later days return `stale_outcome_data` until newer Candidate files are downloaded (section 4).

## 1. What you receive

| Item | Path | Committed? |
|---|---|---|
| Trained models + contract | `models/v20261010/` (prod_clf, prod_reg, shadow_clf, contract.json) | Yes, after the `.gitignore` fix |
| Feature/label code (canonical) | `processing/person_b/features_lib.py` | Yes |
| Scoring entry point | `processing/person_b/score_day.py` | Yes |
| Parity/leakage checks | `processing/person_b/verify_refactor.py` | Yes |
| Training script | `processing/person_b/train_final_models.py` | Yes |
| Feature-set definitions | `results/person_b/feature_sets.json` | Yes |
| Evaluation outputs | `results/person_b/final_results_mllib*.csv`, `isotonic_calibration.csv` | Yes |
| Label spec + amendments | `docs/label_spec.md`, `docs/label_spec_amendments.md` | Yes |
| Data (country-day table, labels, model table) | `data/processed/person_b/*.csv` | No (git-ignored, regenerable) |

## 2. The contract

**Supported countries:** NI (Nigeria), UP (Ukraine). IN, SF and US return `status = unsupported`
with no probability. Country codes are GDELT/FIPS, not ISO.

**Models**

| Artifact | Algorithm | Features | Role |
|---|---|---|---|
| `prod_clf` | Random Forest classifier (300 trees, depth 3, min leaf 5, seed 0) | `p_lag_ratio`, `p_lag_level` (lag 60) | Escalation risk score |
| `prod_reg` | Random Forest regressor (same settings) | same 2 | Severity (log ratio) |
| `shadow_clf` | GBT classifier (50 iter, depth 2, step 0.05, subsample 0.8, seed 0) | 2 persistence + 12 GDELT | Shadow only |

Feature-set names such as `persist_lag30` / `lag30_plus_gdelt` are historical labels for the column groups; the lag actually used is `persistence_lag_days` in the contract.

Each artifact is a Spark `PipelineModel` containing its own `VectorAssembler`. Input must contain the
exact column names listed in `contract.json`, as `double`. No scaling is needed for tree models.

**Persistence features (UCDP, lag 60 days; `persistence_lag_days` in the contract)**
- `p_lag_level` = log1p(sum of UCDP `best` deaths over t−66…t−60)
- `p_lag_ratio` = `p_lag_level` − log1p(baseline_7d at t−60), where baseline_7d = 7 × mean daily deaths over the 28 days ending that day
- Needs UCDP daily history for t−87…t−60 and a release that covers at least t−60. `score_day.py` reads the lag from the contract.

**GDELT features (12), all of the form `g_<X>_d7`**
`g_<X>_d7` = mean of X over t−6…t **minus** mean of X over t−20…t−7 (baseline requires ≥7 days), per country.
X is log1p-transformed first for: `event_count`, `doc_count`, `unique_mention_sources`, `spread_p50_min`, `media_per_event` (= doc_count / event_count).
X is used raw for: `violent_event_share`, `conflict_quad_share`, `gold_mean`, `neg_event_ratio`, `ev_tone_mean`, `source_hhi`, `multi_source_event_share`.
First valid date needs 14 days of GDELT history. The subtraction removes the strong weekday volume cycle.
Base columns come from `country_day_features_spark` (event-level stats from one row per event; document-level stats from distinct URLs).

**Label (for retraining and later evaluation)**
Escalation(c, t) = 1 if deaths over t+1…t+7 ≥ max(1.5 × baseline_7d(t), 5). Source: UCDP Candidate. Frozen.

**Elevated flag** = p ≥ training prevalence (0.1667). Diagnostic only, not a calibrated decision rule.

## 3. Serving rules

- Time zone is UTC. Score day t **only after the 23:45 UTC batch of day t has landed** (about 00:15–00:30 UTC next day). Never score a partial day.
- Spark session must set `spark.sql.session.timeZone=UTC`.
- Persist each day's `country_day_features_spark` output to a history table; `score()` needs the last ≥14 days.
- `score()` returns `status` per country: `ok`, `unsupported`, `stale_outcome_data`, `insufficient_history`, `no_gdelt_data`. **Show the status; never fill a missing prediction with a number.**
- UCDP: pass `ucdp_covered_through` = the last date the newest release **covers**. Not the maximum event date.
- Dashboard wording: "uncalibrated risk score (ranking only)". Do not show accuracy claims.

```python
import sys; sys.path.append("processing/person_b")
from pathlib import Path
from features_lib import load_ucdp
from score_day import score

events = load_ucdp(sorted(Path("data/external/ucdp_candidate").glob("*.csv")))
res = score(spark, "models/v20261010", gdelt_daily_pdf, events,
            ucdp_covered_through="2026-07-31", as_of="2026-07-31")
```

## 4. UCDP data freshness

UCDP Candidate is released monthly. On 2026-10-10 the newest release (August, covers through 2026-08-31) was
**40 days** old, and the gap can approach two months just before a release. The lag was therefore set to **60 days**
after a pre-declared sensitivity run (`docs/label_spec_amendments.md`, Amendment 3); the original 30-day model could not
have been scored on that date. Someone must download each new release into `data/external/ucdp_candidate/` on a schedule
and pass `ucdp_covered_through` = the last day the newest release covers. Lag 60 is still an assumption: a longer real
lag would make `score()` return `stale_outcome_data`, which is the intended safe behaviour.

## 5. Blockers Person C must resolve with Person A (found by reading `stage2_live/*.py`)

| # | Issue | Effect | Owner |
|---|---|---|---|
| 1 | `live_landing.py` joins Events↔Mentions **within one 15-minute batch**. Training joined a day's events to **all 96 mention batches**. | 6 of 12 GDELT features (`doc_count`, `unique_mention_sources`, `spread_p50_min`, `media_per_event`, `source_hhi`, `multi_source_event_share`) would be systematically different live. | A + C |
| 2 | `live_downloader.py` fetches only the latest batch each cycle and never backfills; zips are deleted after landing, so the newest batch is re-downloaded and overwritten every ~60 s. | Missed batches are never ingested; daily features need all 96. | A |
| 3 | Historical Events files have 58 columns; `live_landing.py` hard-codes 61. | Confirm both use the same `GlobalEventID` space. | A |
| 4 | GKG is joined within the batch only. | Lower match rate. **No model feature uses GKG,** so no model impact. | A (info) |
| 5 | `data/temp` must exist (`mkdtemp(dir=...)`). | Crash on a clean clone. | A |

Proposed fix for #1: land compact per-batch Events/Mentions Parquet (as in the historical layout), then at the
end of each UTC day run the Stage-5 join on that day's 96 batches and pass the result to
`country_day_features_spark`. This reproduces training exactly, including the known midnight truncation.

## 6. Retraining

- **Cadence: monthly,** after each UCDP release. Labels for day t exist only after a release covers t+7 (5–8 weeks later in practice). Weekly retraining adds no new labels.
- Recipe: rebuild `labels.csv` (`build_labels.py` / `features_lib`), rebuild `model_table.csv` with the extended window (`build_model_table.py`: edit `M_END`; the fold dictionary is for evaluation only), then `python train_final_models.py <new_version>`. The script asserts the NI + UP row count, so update that assertion when the window grows.
- **Do not change feature definitions during retraining.** A definition change needs a new contract and re-evaluation.
- Re-run the pre-registered comparison (lag30+GDELT vs lag30) on the new data before ever promoting the GDELT model to production.

## 7. Reproduce from scratch (order)

0. (once) pin UCDP files: `data/external/ucdp_candidate/` holds exactly the two original releases used for training (Jan–Jun, Jul 2026).
1. `build_country_day.py` → `country_day_features.csv`
2. `build_labels.py` → `labels.csv` (needs the two UCDP files in `data/external/ucdp_candidate/`)
3. `build_model_table.py` → `model_table.csv`, `feature_sets.json`
4. `run_models_mllib.py` and `run_models_mllib.py --primary` → results CSVs
5. `calibrate_isotonic.py` → `isotonic_calibration.csv`
6. `build_lag_variant.py 60` → `model_table_lag60.csv`; `run_models_mllib.py --primary --table=model_table_lag60.csv --tag=_lag60` (sensitivity)
7. `train_final_models.py v20261010 --table=model_table_lag60.csv --lag=60` → `models/v20261010/`; then `patch_contract.py v20261010`
8. `verify_refactor.py --serve` → serving parity (reads the lag from the contract)

## 8. Open items

| Item | Owner | Status |
|---|---|---|
| Live same-day join (section 5, #1–#3) | A + C | Open |
| GCAM features: the master plan lists them; our model has none | A | Open: add or document the omission |
| Newer UCDP releases (September onward) | B / C | Download monthly; August is the newest as of 2026-10-10 |
| Team confirmation of production framing (persistence prod + GDELT shadow) | All | Needs explicit confirmation |

## 9. Known limitations

About 11 escalation episodes in total and 2 test episodes (both Ukraine); propagation features are truncated at
midnight; UCDP Candidate data may be revised; no GCAM features; IN is marginal and was not trained; SF and US have too few
UCDP events to model; deaths are attributed to `date_start` for multi-day events.