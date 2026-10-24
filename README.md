# Real-Time, Uncertainty-Aware Global Event Escalation Early Warning Using GDELT and Apache Spark

A Big Data Analytics project using GDELT and Apache Spark to build a country-level early-warning pipeline for escalation of lethal organized violence.

## Project Goal

The pipeline treats GDELT as a noisy, high-frequency observation layer and uses an independent conflict dataset (UCDP) as the outcome layer.

```text
GDELT information at time t  →  country-day features  →  future independent outcome (UCDP)  →  escalation prediction
```

## Historical Experiment

- Period: April 1, 2026 – June 30, 2026 (91 daily partitions; model table starts April 14 because features need 14 days of history)
- Countries (GDELT/FIPS codes): United States `US`, India `IN`, Nigeria `NI`, Ukraine `UP`, South Africa `SF`
- Supervised modelling: `NI` and `UP` only (see `docs/person_b_results.md` for why)
- Processing framework: Apache Spark 3.5 (PySpark, Spark MLlib)

## Headline finding (honest summary)

In the April–June 2026 evaluation (about 11 escalation episodes, 2 held-out test episodes, both in Ukraine), lagged UCDP persistence ranked Ukrainian escalation well, and adding GDELT features **did not** meet our pre-declared improvement rules. We therefore ship a persistence-based production model and run the GDELT model in shadow mode. The sample is too small for significance claims. Details: `docs/person_b_results.md`.

## Team Responsibilities and Status

| Person | Scope | Status |
|---|---|---|
| A: Data pipeline | Acquisition, Events/Mentions/compact GKG processing, joins, validation, historical handoff | Historical data complete (`docs/person_a_handoff.md`). Live ingestion has open issues (`docs/person_b_handoff.md` §6). |
| B: Features and modelling | Outcome dataset, country-day features, labels, time-aware evaluation, models, calibration test | **Complete.** Model `v20261010`; handoff in `docs/person_b_handoff.md`. |
| C: Live inference | Live ingestion, feature reuse, scoring, retraining, dashboard | Starts from `docs/person_b_handoff.md`. |

## Course assignment: 9 algorithms (3 per member)

| Member | Algorithms |
|---|---|
| A | K-Means, FP-Growth, MinHash LSH |
| B | Logistic Regression, Gradient-Boosted Trees, Isotonic Regression (all Spark MLlib) |
| C | Random Forest, PCA, CUSUM / Gaussian Mixture |

## Repository Structure

```text
gdelt-unrest-prediction/
├── README.md
├── LICENSE
├── .gitignore / .gitattributes
├── docs/
│   ├── person_a_handoff.md
│   ├── person_b_handoff.md        ← start here for Person C
│   ├── person_b_results.md        ← methods, results, limitations
│   ├── label_spec.md              ← original frozen label spec
│   └── label_spec_amendments.md   ← later decisions (Amendments 1–3)
├── models/
│   ├── README.md
│   ├── v20261010/                 ← CURRENT (persistence lag 60 days)
│   └── v20261008/                 ← superseded (lag 30), kept for provenance
├── results/person_b/              ← evaluation CSVs, feature_sets.json
├── src/person_a/
└── processing/
    ├── person_a/ (incl. stage2_live/)
    └── person_b/                  ← features_lib.py, score_day.py, verify_refactor.py, ...
```

Data (not tracked): `data/PersonA_handoff/joined_daily/` (about 20 GB, shared privately by Person A), `data/processed/`, `data/external/ucdp_candidate/`.

## Quick start (Person B code)

```text
python processing/person_b/verify_refactor.py            # features/labels reproduce the artifacts
python processing/person_b/verify_refactor.py --serve    # serving == training features, safety checks
python processing/person_b/demo_score.py                 # end-to-end scoring example
```

## Data sources and citations

- GDELT 2.0 Events, Mentions and GKG (Leetaru and Schrodt, 2013, "GDELT: Global Data on Events, Location and Tone, 1979–2012").
- UCDP Candidate Events Dataset, CC BY 4.0 (Hegre, Croicu, Eck and Högbladh, 2020, "Introducing the UCDP Candidate Events Dataset", *Research & Politics*); UCDP GED background: Sundberg and Melander, 2013, *Journal of Peace Research*.