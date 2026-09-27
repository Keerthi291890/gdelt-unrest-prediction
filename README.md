# Real-Time, Uncertainty-Aware Global Event Escalation Early Warning Using GDELT and Apache Spark

A Big Data Analytics project using GDELT and Apache Spark to build a country-level early-warning pipeline for event escalation.

## Project Goal

The pipeline treats GDELT as a noisy, high-frequency observation layer and uses an independent conflict/event dataset as the future outcome layer.

Conceptually:

GDELT information at time t
        ↓
country-level features
        ↓
future independent outcome
        ↓
escalation prediction

## Historical Experiment

- Period: April 1, 2026 – June 30, 2026
- Duration: 91 days
- Countries:
  - United States (`US`)
  - India (`IN`)
  - Nigeria (`NI`)
  - Ukraine (`UP`)
  - South Africa (`SF`)
- Processing framework: Apache Spark

## Team Responsibilities

### Person A — Data Pipeline

- GDELT data acquisition
- Events processing
- Mentions processing
- Compact GKG processing
- Country filtering
- Events–Mentions–GKG joining
- Validation and data-quality checks
- Temporal auditing
- Historical handoff to Person B

### Person B — Features and Modeling

- Independent outcome dataset
- Country-day aggregation
- Feature engineering
- Future escalation label
- Time-aware evaluation
- Baseline and ML models
- Calibration and evaluation

### Person C — Live Inference

- Live/near-real-time ingestion
- Reuse of Person B feature definitions
- Model inference
- Periodic retraining
- Uncertainty-aware outputs
- Monitoring/dashboard

## Repository Structure

```text
gdelt-unrest-prediction/
├── README.md
├── LICENSE
├── .gitignore
├── docs/
│   └── person_a_handoff.md
├── src/
│   └── person_a/
└── processing/
    └── person_a/
        └── stage2_live/