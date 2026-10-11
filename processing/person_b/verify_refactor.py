"""
verify_refactor.py  (Person B)

Proves that features_lib.py / score_day.py reproduce the artifacts the original scripts made.

  python verify_refactor.py            # checks 1-2 (pandas only, seconds)
  python verify_refactor.py --serve    # + check 3: serving parity with simulated UCDP lag (needs Spark)
  python verify_refactor.py --spark    # + check 4: recompute country-day features from joined_daily (slow)

Every check prints PASS or raises. Paste the output back if anything fails.
"""
import json
import sys
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from features_lib import (COUNTRIES, daily_deaths, label_frame, load_ucdp,
                          make_gdelt_features, persistence_for_lag)

ROOT = Path(__file__).resolve().parents[2]
D = ROOT / "data" / "processed" / "person_b"
UCDP = sorted((ROOT / "data" / "external" / "ucdp_candidate").glob("*.csv"))
FS = json.load(open(D / "feature_sets.json"))
FEATS = sorted({f for k in ("persist_lag30", "lag30_plus_gdelt") for f in FS[k]})


def same(a, b, what, rtol=1e-6):
    a, b = np.asarray(a, float), np.asarray(b, float)
    assert a.shape == b.shape, f"{what}: shape {a.shape} vs {b.shape}"
    assert np.allclose(a, b, rtol=rtol, atol=1e-9, equal_nan=True), f"{what}: values differ"


# ---------------- 0. contract <-> feature_sets <-> leakage-name guard ----------------
ctr = json.load(open(sorted((ROOT / "models").glob("v*"))[-1] / "contract.json"))
LAG = int(ctr.get("persistence_lag_days", 30))
MT_FILE = "model_table.csv" if LAG == 30 else f"model_table_lag{LAG}.csv"
print(f"contract persistence lag = {LAG} days -> comparing against {MT_FILE}")
for name, spec in ctr["models"].items():
    assert spec["features"] == FS[spec["feature_set"]], f"{name}: contract features != feature_sets.json"
FORBIDDEN = ("label", "future", "target", "baseline_7d", "threshold", "deaths")
for k, cols in FS.items():
    for c in cols:
        assert c.startswith(("p_", "g_")) and not any(w in c for w in FORBIDDEN), f"suspicious feature {k}.{c}"
_mt_cols = set(pd.read_csv(D / MT_FILE, nrows=0).columns)
assert all(c in _mt_cols for cols in FS.values() for c in cols), "feature missing from model_table.csv"
print("PASS 0: contract == feature_sets.json (names AND order); no outcome fields among predictors")

# ---------------- 1. labels ----------------
events = load_ucdp(UCDP)
assert len(events) == 3028, f"expected 3,028 deduped target events, got {len(events)}"
daily = daily_deaths(events, "2026-01-01", "2026-07-31")
lab = label_frame(daily)
old = pd.read_csv(D / "labels.csv", parse_dates=["date"]).sort_values(["country", "date"]).reset_index(drop=True)
new = lab.sort_values(["country", "date"]).reset_index(drop=True)
for c in ["deaths", "baseline_7d", "future_7d", "persist_oracle_7d", "persist_lag30_7d",
          "threshold", "label", "target_logratio"]:
    same(old[c], new[c], f"labels.{c}")
print("PASS 1: labels reproduced exactly")

# ---------------- 2. model table features ----------------
feat = pd.read_csv(D / "country_day_features.csv", parse_dates=["part_date"]).rename(columns={"part_date": "date"})
G, P = make_gdelt_features(feat), persistence_for_lag(daily, LAG)
mt = pd.read_csv(D / MT_FILE, parse_dates=["date"])
cmp = mt[["country", "date"] + FEATS].merge(
    P.merge(G, on=["country", "date"]), on=["country", "date"], suffixes=("_old", ""))
assert len(cmp) == len(mt), "row loss in merge"
for f in FEATS:
    same(cmp[f + "_old"], cmp[f], f"model_table.{f}")
print(f"PASS 2: {len(FEATS)} features x {len(mt)} rows identical to {MT_FILE}")

# ---------------- 3. serving parity with simulated UCDP lag ----------------
if "--serve" in sys.argv:
    from pyspark.sql import SparkSession
    from score_day import score
    spark = (SparkSession.builder.appName("verify_serve").master("local[2]")
             .config("spark.ui.enabled", "false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    model_dir = sorted((ROOT / "models").glob("v*"))[-1]
    for as_of in ["2026-05-10", "2026-06-01", "2026-06-20"]:
        t = pd.Timestamp(as_of)
        covered = t - pd.Timedelta(days=LAG)                      # simulate: newest UCDP covers exactly t-LAG
        ev_cut = events[events["date_start"] <= covered]
        hist = feat[feat["date"] <= t].rename(columns={"date": "part_date"})
        res, X = score(spark, model_dir, hist, ev_cut, covered, t, return_features=True)
        row = mt[mt["date"] == t].set_index("country")
        for c in ("NI", "UP"):
            for f in FEATS:
                same([X.set_index("country").loc[c, f]], [row.loc[c, f]], f"serve {as_of} {c} {f}")
        r = res.set_index("country")
        assert (r.loc[["IN", "SF", "US"], ["prod_status", "shadow_status"]] == "unsupported").all().all()
        assert (r.loc[["NI", "UP"], ["prod_status", "shadow_status"]] == "ok").all().all(), res
        assert r.loc[["IN", "SF", "US"], "p_escalation_uncalibrated"].isna().all()
        print(f"PASS 3: {as_of} serving features == training features; unsupported countries blocked")
    stale = score(spark, model_dir, hist, ev_cut, t - pd.Timedelta(days=LAG + 15), t).set_index("country")
    assert (stale.loc[["NI", "UP"], ["prod_status", "shadow_status"]] == "stale_outcome_data").all().all()
    assert stale.loc[["NI", "UP"], "p_escalation_uncalibrated"].isna().all()
    print("PASS 3b: stale UCDP data refused (no silent zeros)")
    # production must still score when the day's GDELT data is missing
    nog = score(spark, model_dir, hist[hist["part_date"] < t], ev_cut, covered, t).set_index("country")
    assert (nog.loc[["NI", "UP"], "prod_status"] == "ok").all()
    assert nog.loc[["NI", "UP"], "p_escalation_uncalibrated"].notna().all()
    assert (nog.loc[["NI", "UP"], "shadow_status"] == "no_gdelt_data").all()
    assert nog.loc[["NI", "UP"], "shadow_p_gdelt_SHADOW_ONLY"].isna().all()
    print("PASS 3c: production scores without GDELT; shadow reports no_gdelt_data")
    spark.stop()

# ---------------- 4. recompute country-day features from joined_daily ----------------
if "--spark" in sys.argv:
    from pyspark.sql import SparkSession
    from features_lib import country_day_features_spark
    spark = (SparkSession.builder.appName("verify_spark").master("local[2]")
             .config("spark.driver.memory", "6g").config("spark.sql.shuffle.partitions", "16")
             .config("spark.sql.session.timeZone", "UTC").config("spark.ui.enabled", "false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    joined = spark.read.parquet((ROOT / "data" / "PersonA_handoff" / "joined_daily").as_posix() + "/*")
    days = [date(2026, 4, 1) + timedelta(n) for n in range(91)]
    pdf = country_day_features_spark(joined, COUNTRIES, days).sort_values(["country", "part_date"]).reset_index(drop=True)
    old = pd.read_csv(D / "country_day_features.csv", parse_dates=["part_date"]).sort_values(["country", "part_date"]).reset_index(drop=True)
    for c in ["event_count", "doc_count", "unique_mention_sources", "spread_p50_min", "violent_event_share",
              "conflict_quad_share", "gold_mean", "neg_event_ratio", "ev_tone_mean", "source_hhi",
              "multi_source_event_share"]:
        same(old[c], pdf[c], f"country_day.{c}")
    print("PASS 4: Spark country-day features reproduced from joined_daily")
    spark.stop()
print("\nALL REQUESTED CHECKS PASSED")