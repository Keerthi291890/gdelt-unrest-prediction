"""
score_day.py  (Person B -> Person C)

score(...) turns one COMPLETE UTC day of country-day GDELT features plus UCDP history into
per-country predictions, following models/<version>/contract.json.

Production and shadow are gated INDEPENDENTLY:
  prod_status    : needs only UCDP persistence data (production models use no GDELT)
  shadow_status  : needs persistence AND GDELT features

Status values: ok | unsupported | stale_outcome_data | insufficient_history | no_gdelt_data
  * countries outside contract["supported_countries"] never get a prediction
  * UCDP release older than the model's persistence lag (contract) -> stale_outcome_data (no silent zeros)
  * missing GDELT data never blocks the production prediction

Probabilities are UNCALIBRATED risk scores (isotonic calibration was tested and rejected).
The shadow (GDELT) output must never be used to alert.
"""
import json
from pathlib import Path

import numpy as np
import pandas as pd

from features_lib import (COUNTRIES, PERSISTENCE_LAG_DAYS, daily_deaths,
                          make_gdelt_features, persistence_for_lag)


def _predict(spark, model_dir, contract, X):
    """X: DataFrame with 'rid' + feature columns. Rows with NaN in a model's features are not scored
    by that model. Returns DataFrame[rid, p_prod, sev_prod, p_shadow]."""
    from pyspark.ml import PipelineModel
    from pyspark.ml.functions import vector_to_array
    from pyspark.sql import functions as F

    model_dir = Path(model_dir)
    m = contract["models"]
    out = X[["rid"]].copy()
    for col, name, kind in [("p_prod", "prod_clf", "prob"), ("sev_prod", "prod_reg", "reg"),
                            ("p_shadow", "shadow_clf", "prob")]:
        cols = m[name]["features"]
        ok = X[cols].notna().all(axis=1)
        if not ok.any():
            out[col] = np.nan
            continue
        sub = X.loc[ok, ["rid"] + cols].astype({c: float for c in cols})
        res = PipelineModel.load((model_dir / m[name]["path"]).as_posix()).transform(
            spark.createDataFrame(sub))
        if kind == "prob":
            r = res.select("rid", vector_to_array("probability")[1].alias(col)).toPandas()
        else:
            r = res.select("rid", F.col("prediction").alias(col)).toPandas()
        out = out.merge(r, on="rid", how="left")          # merged on key, never by position
    return out


def score(spark, model_dir, gdelt_daily, ucdp_events, ucdp_covered_through, as_of,
          countries=COUNTRIES, return_features=False):
    """
    gdelt_daily : country-day table (output of country_day_features_spark), history up to as_of
    ucdp_events : output of features_lib.load_ucdp (all releases deduped)
    ucdp_covered_through : last date the newest UCDP release COVERS (not the max event date)
    as_of       : the complete UTC day to score
    """
    model_dir = Path(model_dir)
    contract = json.load(open(model_dir / "contract.json"))
    as_of = pd.Timestamp(as_of).normalize()
    covered = pd.Timestamp(ucdp_covered_through).normalize()
    supported = set(contract["supported_countries"])
    m = contract["models"]
    pers = m["prod_clf"]["features"]
    gd = [f for f in m["shadow_clf"]["features"] if f.startswith("g_")]

    g = gdelt_daily.rename(columns={"part_date": "date"}).copy()
    g["date"] = pd.to_datetime(g["date"])
    g = g[g["date"] <= as_of]                                 # never look past as_of

    lag = int(contract.get("persistence_lag_days", PERSISTENCE_LAG_DAYS))   # the lag the model was TRAINED with
    stale = covered < as_of - pd.Timedelta(days=lag)
    short_hist = ucdp_events["date_start"].min() > as_of - pd.Timedelta(days=lag + 27)
    bad_ucdp = bool(stale or short_hist)

    # persistence side (UCDP); values after the release coverage are UNKNOWN, not zero
    daily = daily_deaths(ucdp_events, as_of - pd.Timedelta(days=lag + 60), as_of)
    daily.loc[daily.index > covered] = np.nan
    P = persistence_for_lag(daily, lag)

    # GDELT side (may be empty or incomplete without affecting production)
    if len(g):
        G = make_gdelt_features(g)
    else:
        G = pd.DataFrame(columns=["country", "date"] + gd)

    X = (pd.DataFrame({"country": list(countries), "date": as_of})
         .merge(P, on=["country", "date"], how="left")
         .merge(G, on=["country", "date"], how="left"))
    for c in gd:
        if c not in X:
            X[c] = np.nan
    X["rid"] = np.arange(len(X))

    today = g[g["date"] == as_of]
    has_series = (today.set_index("country")["has_data"]
                  if len(today) and "has_data" in today else pd.Series(dtype=float))
    has = X["country"].map(lambda c: has_series.get(c, 0) == 1).to_numpy(bool)
    sup = X["country"].isin(supported).to_numpy(bool)

    if bad_ucdp:
        X[pers] = np.nan
    X.loc[~sup, pers + gd] = np.nan
    X.loc[~has, gd] = np.nan

    prod_ok = sup & X[pers].notna().all(axis=1).to_numpy(bool)
    shadow_ok = prod_ok & X[gd].notna().all(axis=1).to_numpy(bool)
    n = len(X)
    prod_status = np.select([~sup, np.full(n, bad_ucdp), ~prod_ok],
                            ["unsupported", "stale_outcome_data", "insufficient_history"], "ok")
    shadow_status = np.select([~sup, np.full(n, bad_ucdp), ~has, ~shadow_ok],
                              ["unsupported", "stale_outcome_data", "no_gdelt_data",
                               "insufficient_history"], "ok")

    pred = _predict(spark, model_dir, contract, X[sup])
    X = X.merge(pred, on="rid", how="left")

    thr = contract["alert_threshold"]["value"]
    res = pd.DataFrame({
        "country": X["country"], "date": X["date"],
        "prod_status": prod_status, "shadow_status": shadow_status,
        "p_escalation_uncalibrated": X["p_prod"],
        "severity_logratio": X["sev_prod"],
        "elevated_flag_diagnostic": np.where(prod_status == "ok", X["p_prod"] >= thr, np.nan),
        "shadow_p_gdelt_SHADOW_ONLY": X["p_shadow"],
        "model_version": contract["version"],
    })
    res.loc[prod_status != "ok", ["p_escalation_uncalibrated", "severity_logratio"]] = np.nan
    res.loc[shadow_status != "ok", "shadow_p_gdelt_SHADOW_ONLY"] = np.nan
    return (res, X) if return_features else res