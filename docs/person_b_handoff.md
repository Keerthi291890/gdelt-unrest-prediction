# Person B → Person C Handoff

Model version: **v20261010** (persistence lag 60 days) · Updated 2026-10-10 · Owner: Person B
`v20261008` (lag 30) is **superseded**: with the UCDP release delay we observed, a 30-day lag could not be scored. Use v20261010.

## 0. Read this first

1. **The production models use no GDELT.** They use two UCDP persistence features. The GDELT model (`shadow_clf`) did not meet our pre-declared improvement rules (stated before the run, committed after) and is **shadow only: it must never raise an alert.** See `docs/person_b_results.md`.
2. **Probabilities are uncalibrated risk scores for ranking.** Isotonic calibration was tested and rejected. Do not label them "probability of escalation = 72%" on the dashboard.
3. **Live GDELT features are NOT yet reproducible** by the current `live_landing.py` (section 6). This blocks the shadow model, not the production model.
4. **UCDP freshness limits scoring.** The newest release we have (Aug 2026) covers through 2026-08-31. With the 60-day lag, days up to 2026-10-30 can be scored; later days return `stale_outcome_data` until newer Candidate files are downloaded (section 5).

## 1. What you receive

| Item | Path | Committed? |
|---|---|---|
| Trained models + contract | `models/v20261010/` (prod_clf, prod_reg, shadow_clf, contract.json) | Yes |
| Feature/label code (canonical) | `processing/person_b/features_lib.py` | Yes |
| Scoring entry point | `processing/person_b/score_day.py` | Yes |
| Runnable scoring example | `processing/person_b/demo_score.py` | Yes |
| Parity/leakage checks | `processing/person_b/verify_refactor.py` | Yes |
| Training script | `processing/person_b/train_final_models.py` | Yes |
| Feature-set definitions | `results/person_b/feature_sets.json` | Yes |
| Evaluation outputs | `results/person_b/final_results_mllib*.csv`, `isotonic_calibration.csv`, `lag60_evaluation.txt` | Yes |
| Label spec + amendments | `docs/label_spec.md`, `docs/label_spec_amendments.md` | Yes |
| Intermediate tables | `data/processed/person_b/` (`country_day_features.csv` 455 rows, `labels.csv`, `model_table.csv` 390 rows, `model_table_lag60.csv`) | No (git-ignored, regenerable) |

## 2. Environment

Spark 3.5.9 (local mode), Python with pandas, numpy, scipy, pyspark. Spark sessions must set `spark.sql.session.timeZone=UTC`. `metrics_np.py` replaces scikit-learn metrics (sklearn's compiled files were blocked by a Windows policy on one machine); results are identical to sklearn's.

## 3. The contract (`models/v20261010/contract.json`)

**Supported countries:** NI (Nigeria), UP (Ukraine). IN, SF and US return `unsupported` with no probability. Country codes are GDELT/FIPS, not ISO.

**Models**

| Artifact | Algorithm | Features | Role |
|---|---|---|---|
| `prod_clf` | Random Forest classifier (300 trees, depth 3, min leaf 5, seed 0) | `p_lag_ratio`, `p_lag_level` (lag 60) | Escalation risk score |
| `prod_reg` | Random Forest regressor (same settings) | same 2 | Severity (log ratio) |
| `shadow_clf` | GBT classifier (50 iter, depth 2, step 0.05, subsample 0.8, seed 0) | 2 persistence + 12 GDELT | Shadow only |

Feature-set names such as `persist_lag30` / `lag30_plus_gdelt` are historical labels for column groups; the lag actually used is `persistence_lag_days` in the contract. Each artifact is a Spark `PipelineModel` containing its own `VectorAssembler`; input must have the exact column names listed in the contract, as `double`. No scaling is needed for tree models.

**Persistence features (UCDP, lag L = 60 days)**
- `p_lag_level` = log1p(sum of UCDP `best` deaths over t−L−6 … t−L)
- `p_lag_ratio` = `p_lag_level` − log1p(baseline_7d at t−L), where baseline_7d = 7 × mean daily deaths over the 28 days ending that day
- Needs UCDP daily history for t−(L+27) … t−L and a release that covers at least t−L. `score_day.py` reads L from the contract.

**GDELT features (12), all of the form `g_<X>_d7`**
`g_<X>_d7` = mean of X over t−6…t **minus** mean of X over t−20…t−7 (baseline needs ≥7 days), per country. The subtraction removes the strong weekday volume cycle.
- log1p first: `event_count`, `doc_count`, `unique_mention_sources`, `spread_p50_min`, `media_per_event` (= doc_count / event_count)
- raw: `violent_event_share`, `conflict_quad_share`, `gold_mean`, `neg_event_ratio`, `ev_tone_mean`, `source_hhi`, `multi_source_event_share`
- First valid date needs 14 days of GDELT history.
- Base columns come from `features_lib.country_day_features_spark` (event-level statistics from one row per event; document-level statistics from distinct URLs).

**Label (for retraining and later evaluation):** Escalation(c, t) = 1 if UCDP deaths over t+1…t+7 ≥ max(1.5 × baseline_7d(t), 5). Frozen (`docs/label_spec.md`).

**Elevated flag** = p ≥ training prevalence (0.1667). Diagnostic only, not a calibrated decision rule.

## 4. Serving rules

- Time zone is UTC. Score day t **only after the 23:45 UTC batch of day t has landed** (about 00:15–00:30 UTC next day). Never score a partial day.
- Persist each day's `country_day_features_spark` output to a history table (columns as in `country_day_features.csv`, including `part_date` and `has_data`); `score()` needs at least the last 14 days.
- `score()` returns one row per country with: `prod_status`, `shadow_status`, `p_escalation_uncalibrated`, `severity_logratio`, `elevated_flag_diagnostic`, `shadow_p_gdelt_SHADOW_ONLY`, `model_version`.
- **Statuses:** `ok`, `unsupported`, `stale_outcome_data`, `insufficient_history`, and (shadow only) `no_gdelt_data`. Production and shadow are gated **independently**: missing GDELT data never blocks the production prediction. **Show the status; never fill a missing prediction with a number.**
- UCDP: pass `ucdp_covered_through` = the last date the newest release **covers**, not the maximum event date.
- Dashboard wording: "uncalibrated risk score (ranking only)". Do not show accuracy claims.

```python
import sys; sys.path.append("processing/person_b")
from pathlib import Path
from features_lib import load_ucdp
from score_day import score

events = load_ucdp(sorted(Path("data/external/ucdp_candidate").glob("*.csv")))
res = score(spark, "models/v20261010", gdelt_daily_pdf, events,
            ucdp_covered_through="2026-07-31", as_of="2026-06-30")
```
`processing/person_b/demo_score.py` runs this end to end, including the stale-data refusal.

## 5. UCDP data freshness

UCDP Candidate is released monthly. On 2026-10-10 the newest release (August, covers through 2026-08-31) was **40 days** old, and the gap can approach two months just before a release. The lag was therefore set to **60 days** after a pre-declared sensitivity run (`docs/label_spec_amendments.md`, Amendment 3). Someone must download each new release into `data/external/ucdp_candidate/` and pass the correct `ucdp_covered_through`. Lag 60 is still an assumption: a longer real lag makes `score()` return `stale_outcome_data`, which is the intended safe behaviour.

## 6. Blockers for the live pipeline (found by reading `stage2_live/*.py`; owners A + C)

| # | Issue | Effect | Owner |
|---|---|---|---|
| 1 | `live_landing.py` joins Events↔Mentions **within one 15-minute batch**. Training joined a day's events to **all 96 mention batches**. | 6 of 12 GDELT features (`doc_count`, `unique_mention_sources`, `spread_p50_min`, `media_per_event`, `source_hhi`, `multi_source_event_share`) would differ systematically live. | A + C |
| 2 | `live_downloader.py` fetches only the latest batch each cycle and never backfills; zips are deleted after landing, so the newest batch is re-downloaded and overwritten every ~60 s. | Missed batches are never ingested; daily features need all 96. | A |
| 3 | Historical Events files have 58 columns; `live_landing.py` hard-codes 61. | Confirm both use the same `GlobalEventID` space. | A |
| 4 | GKG is joined within the batch only. | Lower match rate. **No model feature uses GKG.** | A (info) |
| 5 | `data/temp` must exist (`mkdtemp(dir=...)`). | Crash on a clean clone. | A |

**Proposed fix for #1:** land compact per-batch Events/Mentions Parquet (as in the historical layout); at the end of each UTC day run the Stage-5 join on that day's 96 batches and pass the result to `country_day_features_spark`. This reproduces training, including the known midnight truncation.

**Acceptance test for the live path:** replay one historical day (for example 2026-06-30) through the live path and compare its 12 base columns with the row in `country_day_features.csv`; they must match within floating-point tolerance. GKG-derived columns are excluded.

## 7. Person C task list

1. Fix the live join and backfill (section 6) with Person A; pass the acceptance test.
2. Daily scoring job: after 23:45 UTC, compute country-day features, append to the history table, call `score()`, store results with the model version.
3. UCDP downloader: fetch each monthly Candidate release and record the covered-through date.
4. Dashboard: show `prod_status`; use the wording in section 4; display shadow output only in a clearly separate "experimental" view.
5. Live evaluation (section 8) and monthly retraining (section 9).

## 8. Judging the shadow model on live data (proposal; needs team agreement)

Store every day's predictions with the model version. When a UCDP release covers t+7, compute the label with `features_lib.label_frame` and re-apply Rules C and R from `docs/label_spec_amendments.md`, comparing the GDELT shadow model with the production model. Consider promoting the GDELT model only after at least **5 new independent escalation episodes** (positive runs separated by 14 days or more) across NI and UP, and only if it meets both rules. Until then it stays shadow.

## 9. Retraining

- **Cadence: monthly,** after each UCDP release. Labels for day t exist only after a release covers t+7 (5–8 weeks later in practice). Weekly retraining adds no new labels.
- Recipe: rebuild `labels.csv` (`build_labels.py`), rebuild `model_table.csv` with the extended window (`build_model_table.py`), build the lag variant (`build_lag_variant.py 60`), then `python train_final_models.py <new_version> --table=model_table_lag60.csv --lag=60` and `patch_contract.py <new_version>`.
- **These scripts are deliberately strict about the original experiment and are not yet generalized for retraining:** `build_labels.py` asserts 11,879 rows / 3,028 events and a grid ending 2026-07-31; `build_lag_variant.py` asserts exactly two UCDP files; `build_model_table.py` hard-codes `M_START`, `M_END` and the folds; `train_final_models.py` asserts 156 rows; `verify_refactor.py` asserts 3,028 events. Retraining also needs a country-day feature table that covers the new days (the historical table covers Apr 1–Jun 30 only). Generalizing these is a joint B/C task.
- **Do not change feature definitions during retraining.** A definition change needs a new contract and re-evaluation.

## 10. Reproduce from scratch (order)

0. Pin UCDP: `data/external/ucdp_candidate/` holds exactly the two releases used for training (Jan–Jun and Jul 2026). Put newer releases in a separate folder.
1. `build_country_day.py` → `country_day_features.csv`
2. `build_labels.py` → `labels.csv`
3. `build_model_table.py` → `model_table.csv`, `feature_sets.json`
4. `run_models_mllib.py` and `run_models_mllib.py --primary` → results CSVs
5. `calibrate_isotonic.py` → `isotonic_calibration.csv`
6. `build_lag_variant.py 60` → `model_table_lag60.csv`; `run_models_mllib.py --primary --table=model_table_lag60.csv --tag=_lag60`
7. `train_final_models.py v20261010 --table=model_table_lag60.csv --lag=60`, then `patch_contract.py v20261010`
8. `verify_refactor.py`, `verify_refactor.py --serve`, and optionally `--spark`

## 11. Data validation done by Person B on Person A's handoff

| Check | Result |
|---|---|
| Daily partitions | 91 (April 5 was missing at first and was added: 40,618 rows) |
| Joined rows | 9,631,794 (five target countries only) |
| Distinct events | 3,824,602; no event ID appears in two partitions |
| Mention date vs partition date | 0 mismatches; key nulls = 0 |
| Duplicate event–mention pairs | 163,189 (about 1.7% of rows), all exactly 2 copies (566 without a GKG match, 162,623 with one GKG variant) |
| Rule applied | one row per `(part_date, GlobalEventID, MentionIdentifier)`, taking `min(MentionTimeDate)`; about 9.47 million pairs remain |
| Country-day table | 455 rows (91 × 5), 0 days without data; event counts sum to 3,824,602; GKG match about 99% |

Person A's handoff quotes the earlier 90-day figures (3,803,889 events, about 9.62 million pairs); Person A should update them.

## 12. Open items

| Item | Owner | Status |
|---|---|---|
| Live same-day join and backfill (section 6, #1–#3) | A + C | Open |
| GCAM features: the master plan lists them; our model has none | A | Open: add or document the omission |
| Newer UCDP releases (September onward) | B / C | Download monthly; August is the newest as of 2026-10-10 |
| Generalize scripts for retraining (section 9) | B + C | Open |
| Team confirmation of production framing (persistence in production, GDELT in shadow) | All | Needs explicit confirmation |

## 13. Known limitations

About 11 escalation episodes in total and 2 test episodes (both Ukraine); the lag-60 fold-3 ranking is one episode and not evidence of skill; propagation features are truncated at midnight; UCDP Candidate data may be revised; persistence lag 60 is an assumption about release delay; no GCAM features; IN is marginal and not trained; SF and US have too few UCDP events to model; deaths are attributed to `date_start` for multi-day events.