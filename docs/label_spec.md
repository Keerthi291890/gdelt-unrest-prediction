# Label Specification

> Original frozen specification. Later decisions (modelling window, production set, persistence lag) are recorded in `docs/label_spec_amendments.md` (Amendments 1–3) and take precedence where they differ.

## Target

Predict whether a country experiences an escalation in lethal organized violence during the next 7 days.

## External outcome source

UCDP Candidate Events Dataset (CC BY 4.0). Files used for training and evaluation:

- `GEDEvent_v26_01_26_06.csv`
- `GEDEvent_v26_0_7.csv`

Monthly-release duplicates are removed using UCDP event `id`, keeping the latest release (releases are ordered by the maximum `date_start` in each file). Later releases (for example `GEDEvent_v26_0_8.csv`, covering through 2026-08-31) are not part of the training data and are used only for live scoring freshness.

## Country mapping

- US = United States of America
- IN = India
- NI = Nigeria
- UP = Ukraine
- SF = South Africa

(GDELT/FIPS codes, not ISO.)

## Daily outcome

For each country and date t: daily deaths = sum of UCDP `best` fatalities for events whose `date_start` equals t. For multi-day events, `best` fatalities are attributed to `date_start` (multi-day events are about 5–15% of events per country).

## Future outcome

future_7d(t) = total UCDP `best` deaths from t+1 through t+7.

## Baseline

baseline_7d(t) = 7 × mean daily UCDP deaths over the trailing 28 days, including day t. Undefined for the first 27 days of the series.

## Escalation label

label = 1 if future_7d(t) >= max(1.5 × baseline_7d(t), 5); otherwise label = 0. The label is undefined when baseline_7d(t) or the full 7-day future window is unavailable.

## Threshold selection

Candidate ratios 1.25 and 1.50 were evaluated on the Jan 28–Mar 24, 2026 period (no GDELT data). The pre-stated selection rule was to choose a ratio giving roughly 10–40% positives in the primary usable countries NI and UP. Both ratios satisfied the criterion; 1.50 was selected and frozen before examining Apr–Jun modelling outcomes.

## Prediction period

GDELT features exist for April 1 – June 30, 2026. Because July UCDP data is available, June 30 has a complete 7-day future outcome through July 7. The model table starts on April 14 because GDELT window features need 14 days of history (Amendment 1).

## Countries

NI and UP are the primary supervised-learning countries. IN is retained for analysis but considered marginal because of sparse UCDP events. SF is retained in the feature table but is not a primary supervised-learning country because its baseline is below the 5-death floor. US is excluded from supervised learning because UCDP contains no events after March 30, 2026.

## Secondary target

log1p(future_7d_deaths) − log1p(baseline_7d), retained to avoid dependence on binary class balance.

## Important limitations

- UCDP Candidate is an external outcome source and is not constructed from GDELT predictors.
- Multi-day events are attributed to `date_start`.
- The 7-day labels overlap in time, so consecutive positive rows do not represent independent escalation episodes.
- UCDP Candidate data may subsequently be revised.