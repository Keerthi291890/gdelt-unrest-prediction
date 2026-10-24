# Person B: Features, Labels, Modelling and Evaluation

## 1. Method

**Outcome (independent of GDELT).** UCDP Candidate Events (Jan–Jul 2026; two releases deduplicated on event id, latest release kept). Daily deaths per country by `date_start`. A country-day is an *escalation* if deaths over the next 7 days ≥ max(1.5 × 7 × trailing 28-day daily mean, 5). The ratio 1.5 was chosen on Jan 28–Mar 24 2026 (no GDELT data) using a rule fixed in advance: 10–40% positives in NI and UP.

**Features.**
- *GDELT (12):* country-day aggregates built from Person A's event–mention–GKG join. Event-level statistics come from one row per event; mention/document statistics from distinct event–mention pairs and distinct URLs. Each is the 7-day mean minus the earlier 14-day mean (t−20…t−7), which removes the weekday volume cycle.
- *Persistence (2):* UCDP deaths over the 7 days ending L days earlier (level, and ratio to its baseline). L stands in for UCDP's release delay. The main evaluation uses L = 30; the production model uses L = 60 (section 3b). An *oracle* variant (data up to t) is kept only as a reference.

**Evaluation.** Time-ordered expanding windows. Test blocks: May 20–Jun 2, Jun 3–16, Jun 17–30 (28 test rows each: 14 NI + 14 UP). Training rows end 8 days before each block, so the 7-day label window never overlaps training (training sets: 58, 86 and 114 rows with 12, 14 and 19 positives). Models: logistic regression, random forest, gradient-boosted trees (Spark MLlib, fixed hyperparameters, no tuning). Baselines: training-prevalence constant and always-negative. PR-AUC and ROC-AUC are computed with numpy equivalents (`metrics_np.py`) because sklearn's compiled files were blocked by a Windows policy; a re-run reproduced the earlier sklearn numbers exactly.

**Pre-declared decision rules** (stated before the model run; committed to the repository afterwards, so git history does not prove prior registration).
- *Rule C:* adding GDELT to lagged persistence must raise PR-AUC in both Ukrainian test folds.
- *Rule R:* it must lower MAE and raise Spearman in at least 2 of 3 countries (3-country run) or in both NI and UP (primary-only run).

## 2. What the data can support

| Country | Apr 14–Jun 30 status |
|---|---|
| UP | Primary. Only country with held-out positives (folds 1 and 3, 7 each; one episode per block). |
| NI | Primary, trained. Regression-only evaluation (no held-out positives). |
| IN | Marginal, not trained (about 7 deaths per week; no model beat the trivial baseline). |
| SF | Excluded: baseline below the 5-death floor. |
| US | Excluded: UCDP records no events after Mar 30. |

Roughly 11 escalation episodes exist in total and only 2 are test episodes. **No significance claims are possible.**

## 3. Results (primary-only run, NI + UP; Spark MLlib; `results/person_b/final_results_mllib_primary.csv`)

**Ukraine classification, PR-AUC (test prevalence 0.5 in both folds)**

| Model | Fold 1: lag30 | Fold 1: lag30 + GDELT | Fold 3: lag30 | Fold 3: lag30 + GDELT |
|---|---|---|---|---|
| Logistic | 0.929 | 0.647 | 0.721 | 0.498 |
| Random forest | 0.864 | 0.763 | 0.900 | 0.876 |
| GBT | 0.893 | 0.738 | 0.774 | 0.852 |

**Rule C: not met** (no model improves in both folds; GBT improves only in fold 3).

**Regression, MAE improvement over the training-mean baseline (mean of 3 folds)**

| | NI lag30 | NI +GDELT | UP lag30 | UP +GDELT | UP GDELT-only |
|---|---|---|---|---|---|
| Linear | +0.47 | −0.20 | +0.36 | −0.11 | −0.41 |
| Random forest | +0.08 | +0.25 | +0.30 | −0.22 | −0.28 |
| GBT | +0.08 | −0.16 | +0.25 | −0.35 | −0.25 |

**Rule R: not met.** The only favourable case is NI random forest (MAE 0.554 → 0.478), where Spearman is unchanged (0.256 vs 0.250); it is not reproduced by the other two model families.

**Other observations**
- Lagged persistence ranks Ukrainian days well (regression Spearman up to 0.55), but its probabilities are barely better than the training prevalence (Brier skill between −0.12 and +0.09).
- GDELT-only classification is unstable: above chance in fold 3 (PR-AUC about 0.85), near or below chance in fold 1.
- The best persistence model family differs between runs (random forest in the 3-country run, linear in the NI + UP run); the choice of random forest for production is a convention fixed in advance, not an empirical win.
- The 3-country run (including IN) reached the same verdict, so the conclusion does not depend on IN.
- Alert counts on the zero-positive blocks showed no consistent difference between tree models with and without GDELT, so they are not used as evidence.
- Run-to-run variation between sklearn and MLlib random forests reached about 0.07 PR-AUC, comparable to many of the differences discussed above.

**Calibration (algorithm 9).** Isotonic regression fitted on folds 1–2 and tested on fold 3 (56 fit rows, 28 test rows, 7 positives) worsened the Brier score for all four models (for example GBT with GDELT 0.112 → 0.213; RF on persistence 0.180 → 0.194), so every model lost skill relative to its raw output. The calibration data contain only 7 positives, all from one Ukrainian fold, which is too little for isotonic regression. No calibration is applied.

## 3b. Persistence-lag sensitivity (rule declared before the run)

On 2026-10-10 the newest UCDP release was 40 days old, so the 30-day lag was optimistic. Rule: persistence skill is "retained" at a longer lag if, for both RF and GBT, Ukraine PR-AUC ≥ 0.60 in folds 1 and 3 **and** Ukraine MAE improvement over baseline ≥ 0.10. Lag 60 (same folds, labels and GDELT features; only the two persistence columns recomputed; `final_results_mllib_primary_lag60.csv`, where the feature set still named `persist_lag30` holds the lag-60 values):

| Ukraine | RF | GBT |
|---|---|---|
| PR-AUC fold 1 (lag 30 → 60) | 0.864 → 0.705 | 0.893 → 0.691 |
| PR-AUC fold 3 (lag 30 → 60) | 0.900 → 1.000 | 0.774 → 1.000 |
| MAE improvement vs baseline (lag 30 → 60) | 0.296 → 0.166 | 0.250 → 0.206 |

The rule is met, so persistence skill is **retained** at lag 60 and the production lag was set to 60. Cautions: the perfect fold-3 ranking (PR-AUC 1.000) has Brier skill near zero (0.004–0.047) and comes from one episode of 7 consecutive days, which a slowly varying feature can separate by chance; it is not used as evidence. Nigeria regression Spearman falls to about zero. The GDELT comparison is unchanged at lag 60: Rules C and R are again not met (Ukraine MAE 2.04–2.25 with GDELT vs 1.44–1.54 persistence only). The choice of lag 60 rests on the observed release delay, not on these scores.

## 4. Conclusion

In this April–June 2026 evaluation, lagged UCDP persistence provided useful ranking signal for Ukraine. Adding GDELT features did not meet our pre-declared improvement criteria for classification or regression, at lag 30 or lag 60. GDELT helped in isolated cells (Ukraine fold 3 for GBT; Nigeria random-forest MAE) that were not replicated across folds or model families. With about 11 episodes and two test episodes, the experiment can neither confirm nor rule out a GDELT contribution. We therefore deploy a persistence-based production model (v20261010, lag 60) and run the GDELT model in shadow mode so that live data can address the question later.

## 5. Algorithms implemented by Person B (course assignment)

| Algorithm | Approach | Where | Outcome |
|---|---|---|---|
| Logistic / Ridge regression | Linear | `run_models_mllib.py` | Baseline; unstable with GDELT |
| Gradient-Boosted Trees | Boosting | `run_models_mllib.py`, `train_final_models.py` (shadow model) | Helps in one fold only |
| Isotonic Regression | Monotone calibration | `calibrate_isotonic.py` | Worsened Brier for all four models tested |

Random Forest (used for the production model) is Person C's assigned algorithm; the shared training code is `run_models_mllib.py`.

## 6. Threats to validity

Very few independent episodes (overlapping 7-day labels); UCDP counts fatal organized violence only (not protests or unrest); the persistence lag (60 days in production) is an assumption about UCDP release delay; UCDP Candidate data can be revised; GDELT propagation features are truncated at midnight; deaths for multi-day events are attributed to the start date; no GCAM features; the thresholds (1.5 ratio, 5-death floor) were chosen once and not tuned on the modelling window; the decision rules were committed to the repository after the run.

## 7. Reproducibility

Spark 3.5.9 local mode; fixed seeds (0) for all tree models; scripts and order in `docs/person_b_handoff.md` §10; `verify_refactor.py` checks that the shared library reproduces labels, features and serving behaviour.