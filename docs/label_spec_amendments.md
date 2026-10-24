# Label Specification: Amendments

These amendments record decisions that `docs/label_spec.md` did not contain. The decision rules were stated in the project design discussion **before** the model run; this file was first committed (commit `fb3e304`) **after** the run, so git history does not by itself prove prior registration. "Pre-declared" below means stated before the run, not formally registered.

## Amendment 1: modelling window and evaluation

- Model-table window: 2026-04-14 to 2026-06-30 (GDELT features need 14 days of history), 78 days per country.
- Folds: expanding training window; test blocks May 20–Jun 2, Jun 3–16, Jun 17–30. Training rows satisfy t ≤ testStart − 8 (7-day gap equal to the label horizon).
- Held-out positives exist only for UP (folds 1 and 3, one episode each). NI and IN are regression-only for evaluation.
- Rule C: `lag30_plus_gdelt` must beat `persist_lag30` on PR-AUC in UP folds 1 **and** 3.
- Rule R: `lag30_plus_gdelt` must beat `persist_lag30` on MAE **and** Spearman in at least 2 of 3 countries (NI, UP, IN).
- GDELT feature rule: 7-day mean minus the mean over t−20…t−7; log1p applied to count-like features first.
- Persistence: lag 30 (UCDP data ≤ t−30) was the baseline for the main evaluation (the deployed lag is changed by Amendment 3); `oracle` (data ≤ t) is reference only.
- Fallback: a country with fewer than 2 positive episodes in the modelling window is evaluated by regression only.

## Amendment 2: production set (made after seeing results)

- Production models train on NI + UP only (156 rows, 26 positives). IN is neither trained nor scored.
- The NI + UP run is a sensitivity analysis; Rule R then requires both NI and UP.
- Outcome: Rules C and R were **not met** in the 3-country run or the NI + UP run.
- Isotonic calibration (fit on folds 1–2, tested on fold 3) worsened Brier for all four models tested. No calibration is applied; outputs are labelled uncalibrated risk scores.
- Production = random forest on persistence features (a convention fixed in advance, not an empirical win; the best family differed between runs; the lag is set by Amendment 3). GDELT model = shadow only.

## Amendment 3: persistence lag 60 (made after seeing results; decision rule declared before the sensitivity run)

- Observation: on 2026-10-10 the newest UCDP release (August 2026, max `date_start` 2026-08-31) was 40 days old, so the 30-day lag model could not be scored.
- Pre-declared rule: persistence skill is "retained" at lag L if, for RF and GBT, Ukraine PR-AUC ≥ 0.60 in folds 1 and 3 and Ukraine MAE improvement over baseline ≥ 0.10.
- Result at lag 60: retained (PR-AUC fold 1: RF 0.705, GBT 0.691; fold 3: 1.000 for both, one episode; MAE improvement: RF 0.166, GBT 0.206). GDELT Rules C and R again not met.
- Production model v20261010 uses lag 60 (`persistence_lag_days: 60`). v20261008 (lag 30) is superseded and kept only for provenance.
- The lag change is justified by the observed release delay, not by lag-60 scores.