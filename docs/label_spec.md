# Label Specification

## Target

Predict whether a country experiences an escalation in lethal organized violence during the next 7 days.

## External outcome source

UCDP Candidate Events Dataset.

Files used:

- GEDEvent_v26_01_26_06.csv
- GEDEvent_v26_0_7.csv

Monthly-release duplicates are removed using UCDP event `id`, keeping the latest release.

## Country mapping

- US = United States of America
- IN = India
- NI = Nigeria
- UP = Ukraine
- SF = South Africa

## Daily outcome

For each country and date t:

daily deaths = sum of UCDP `best` fatalities for events whose `date_start` equals t.

For multi-day events, `best` fatalities are attributed to `date_start`.

This convention is applied consistently during label construction.

## Future outcome

future_7d(t) = total UCDP `best` deaths from t+1 through t+7.

## Baseline

baseline_7d(t) =

7 × mean daily UCDP deaths over the trailing 28 days, including day t.

## Escalation label

label = 1 if:

future_7d(t) >= max(1.5 × baseline_7d(t), 5)

Otherwise:

label = 0.

## Threshold selection

Candidate ratios 1.25 and 1.50 were evaluated using the Jan 28–Mar 24, 2026 period.

The pre-stated selection rule was to select a ratio producing approximately 10–40% positives in the primary usable countries NI and UP.

Both ratios satisfied the criterion.

Ratio 1.50 was selected and is frozen before examining Apr–Jun modeling outcomes.

## Prediction period

GDELT feature period:

April 1, 2026 – June 30, 2026.

Because July UCDP data is available, June 30 can have a complete 7-day future outcome through July 7.

## Countries

NI and UP are the primary supervised-learning countries.

IN is retained for analysis but considered marginal because of sparse UCDP events.

SF is retained in the feature table but is not a primary supervised-learning country because of sparse outcomes.

US is excluded from supervised learning because UCDP contains no events after March 30, 2026.

## Secondary target

A continuous outcome will also be retained:

log1p(future_7d_deaths) - log1p(baseline_7d)

This avoids dependence on binary class balance.

## Important limitations

- UCDP Candidate is an external outcome source and is not constructed from GDELT predictors.
- Multi-day events are attributed to `date_start`.
- The 7-day labels overlap in time, so consecutive positive rows do not represent independent escalation episodes.
- UCDP Candidate data may subsequently be revised.