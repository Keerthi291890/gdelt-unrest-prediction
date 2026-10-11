"""
features_lib.py  (Person B)

Single source of truth for feature and label logic.
Used by the batch scripts AND by live scoring (score_day.py).

Changing ANYTHING here changes the feature contract (docs/person_b_handoff.md)
and requires a new model version. verify_refactor.py proves parity with the
artifacts produced by the original scripts.

pandas/numpy only at import time; pyspark is imported lazily inside the Spark function.
"""
from pathlib import Path

import numpy as np
import pandas as pd

# --------------------------------------------------------------------------------------
# Constants (frozen, see docs/label_spec.md)
# --------------------------------------------------------------------------------------
COUNTRIES = ["US", "IN", "NI", "UP", "SF"]            # GDELT/FIPS codes
UCDP_NAME_TO_FIPS = {"United States of America": "US", "India": "IN", "Nigeria": "NI",
                     "Ukraine": "UP", "South Africa": "SF"}
LABEL = dict(ratio=1.5, floor=5.0, horizon=7, baseline_days=28)
PERSISTENCE_LAG_DAYS = 30

LOG_COLS = ["event_count", "doc_count", "unique_mention_sources",
            "spread_p50_min", "media_per_event"]
RAW_COLS = ["violent_event_share", "conflict_quad_share", "gold_mean", "neg_event_ratio",
            "ev_tone_mean", "source_hhi", "multi_source_event_share"]


# --------------------------------------------------------------------------------------
# GDELT window features (pandas). Every value at date t uses only rows with date <= t.
# --------------------------------------------------------------------------------------
def make_gdelt_features(g):
    """g: country-day table with columns country, date + the base columns."""
    g = g.sort_values(["country", "date"]).copy()
    g["media_per_event"] = g["doc_count"] / g["event_count"]
    out = g[["country", "date"]].copy()
    for c in LOG_COLS + RAW_COLS:
        x = np.log1p(g[c]) if c in LOG_COLS else g[c]
        x = x.groupby(g["country"])
        recent = x.transform(lambda s: s.rolling(7, min_periods=7).mean())               # t-6..t
        base = x.transform(lambda s: s.shift(7).rolling(14, min_periods=7).mean())       # t-20..t-7
        out[f"g_{c}_d7"] = recent - base
    return out


# --------------------------------------------------------------------------------------
# UCDP outcome side
# --------------------------------------------------------------------------------------
def load_ucdp(files):
    """Read UCDP Candidate CSVs; dedupe on event id keeping the LATEST release.

    Release order = by each file's max date_start (then name), so it does not depend on
    file-name sorting (v26_0_10 would sort before v26_0_7 alphabetically).
    Returns DataFrame[id, country, date_start(datetime), best, cc]  (target countries only).
    """
    loaded = []
    for f in files:
        x = pd.read_csv(f, usecols=["id", "country", "date_start", "best"], low_memory=False)
        x["date_start"] = pd.to_datetime(x["date_start"])
        loaded.append((x["date_start"].max(), Path(f).name, x))
    loaded.sort(key=lambda t: (t[0], t[1]))                  # oldest release first
    df = pd.concat([x.assign(release=i) for i, (_, _, x) in enumerate(loaded)], ignore_index=True)
    df = df.sort_values("release", kind="stable").drop_duplicates("id", keep="last")
    df["cc"] = df["country"].map(UCDP_NAME_TO_FIPS)
    return df.dropna(subset=["cc"]).drop(columns="release")


def daily_deaths(events, start, end):
    """Daily UCDP `best` deaths per country by date_start. Missing days -> 0.
    NOTE: zeros are only valid up to the release coverage date (see score_day)."""
    idx = pd.date_range(start, end)
    d = (events.assign(date=events["date_start"].dt.normalize())
         .groupby(["date", "cc"])["best"].sum().unstack("cc")
         .reindex(index=idx, columns=COUNTRIES).fillna(0.0))
    return d


def label_frame(daily, ratio=None, floor=None, horizon=None, baseline_days=None):
    """Label + persistence inputs from the daily deaths grid (index = dates)."""
    ratio = LABEL["ratio"] if ratio is None else ratio
    floor = LABEL["floor"] if floor is None else floor
    H = LABEL["horizon"] if horizon is None else horizon
    B = LABEL["baseline_days"] if baseline_days is None else baseline_days
    idx = daily.index
    rows = []
    for cc in daily.columns:
        s = daily[cc]
        w7 = s.rolling(H).sum()
        out = pd.DataFrame({
            "country": cc, "date": idx, "deaths": s.values,
            "baseline_7d": (7.0 * s.rolling(B, min_periods=B).mean()).values,
            "future_7d": w7.shift(-H).values,                      # sum over t+1..t+7
            "persist_oracle_7d": w7.values,                        # data up to t (NOT deployable)
            "persist_lag30_7d": w7.shift(PERSISTENCE_LAG_DAYS).values,
        })
        out["threshold"] = np.maximum(ratio * out["baseline_7d"], floor)
        ok = out["baseline_7d"].notna() & out["future_7d"].notna()
        out["label"] = np.where(ok, (out["future_7d"] >= out["threshold"]).astype(float), np.nan)
        out["target_logratio"] = np.log1p(out["future_7d"]) - np.log1p(out["baseline_7d"])
        rows.append(out)
    return pd.concat(rows, ignore_index=True)


def make_persistence(lab):
    lab = lab.sort_values(["country", "date"]).copy()
    base_lag = lab.groupby("country")["baseline_7d"].shift(PERSISTENCE_LAG_DAYS)
    out = lab[["country", "date"]].copy()
    out["p_or_ratio"] = np.log1p(lab["persist_oracle_7d"]) - np.log1p(lab["baseline_7d"])
    out["p_or_level"] = np.log1p(lab["persist_oracle_7d"])
    out["p_lag_ratio"] = np.log1p(lab["persist_lag30_7d"]) - np.log1p(base_lag)
    out["p_lag_level"] = np.log1p(lab["persist_lag30_7d"])
    return out


def persistence_for_lag(daily, lag):
    """UCDP persistence features computed with an arbitrary lag (days).
    p_lag_level = log1p(7-day deaths ending t-lag); p_lag_ratio = p_lag_level - log1p(baseline_7d at t-lag).
    lag=30 reproduces make_persistence()'s p_lag_* exactly."""
    w7 = daily.rolling(7).sum()
    base = 7.0 * daily.rolling(LABEL["baseline_days"], min_periods=LABEL["baseline_days"]).mean()
    parts = []
    for cc in daily.columns:
        lvl = np.log1p(w7[cc].shift(lag))
        ratio = lvl - np.log1p(base[cc].shift(lag))
        parts.append(pd.DataFrame({"country": cc, "date": daily.index,
                                   "p_lag_level": lvl.values, "p_lag_ratio": ratio.values}))
    return pd.concat(parts, ignore_index=True)


# --------------------------------------------------------------------------------------
# Spark: event-mention-GKG joined rows -> country-day base features
# --------------------------------------------------------------------------------------
def country_day_features_spark(joined, countries, days, pairs_path=None):
    """joined: Spark DF with the Stage-5 / live joined schema (string columns).
    days: list of datetime.date for the grid. Returns a pandas DF, one row per country-day.
    Session must use spark.sql.session.timeZone=UTC.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    spark = joined.sparkSession
    raw = joined.select(
        "GlobalEventID", "Day", "EventRootCode", "QuadClass", "GoldsteinScale",
        "NumMentions", "NumSources", "NumArticles", "AvgTone", "ActionGeo_CountryCode",
        "MentionIdentifier", "MentionTimeDate", "MentionSourceName", "Confidence",
        "MentionDocLen", "MentionDocTone", "document_identifier", "v1_5_tone")

    base = (raw.withColumnRenamed("ActionGeo_CountryCode", "country")
            .filter(F.col("country").isin(list(countries)))
            .withColumn("part_date", F.to_date(F.substring("MentionTimeDate", 1, 8), "yyyyMMdd")))

    fi = lambda c: F.first(c, ignorenulls=True)
    pairs = (base.groupBy("country", "part_date", "GlobalEventID", "MentionIdentifier")
             .agg(F.min("MentionTimeDate").alias("first_mt"),
                  F.max(F.col("Confidence").cast("double")).alias("conf"),
                  fi("MentionSourceName").alias("src"),
                  fi(F.col("MentionDocTone").cast("double")).alias("doc_tone"),
                  fi(F.col("MentionDocLen").cast("double")).alias("doc_len"),
                  fi("document_identifier").alias("gkg_doc"),
                  fi(F.split("v1_5_tone", ",").getItem(0).cast("double")).alias("gkg_tone"),
                  fi("Day").alias("ev_day"),
                  fi("EventRootCode").alias("root"),
                  fi(F.col("QuadClass").cast("int")).alias("quad"),
                  fi(F.col("GoldsteinScale").cast("double")).alias("gold"),
                  fi(F.col("NumMentions").cast("double")).alias("n_mentions"),
                  fi(F.col("NumSources").cast("double")).alias("n_sources"),
                  fi(F.col("NumArticles").cast("double")).alias("n_articles"),
                  fi(F.col("AvgTone").cast("double")).alias("ev_tone"))
             .withColumn("mt", F.to_timestamp("first_mt", "yyyyMMddHHmmss")))

    if pairs_path:
        pairs.write.mode("overwrite").parquet(pairs_path)
        pairs = spark.read.parquet(pairs_path)
    else:
        pairs = pairs.cache()

    # event-level (one row per event)
    ev = pairs.dropDuplicates(["part_date", "GlobalEventID"])
    ev_day = (ev.groupBy("country", "part_date").agg(
        F.count("*").alias("event_count"),
        F.mean("gold").alias("gold_mean"), F.stddev("gold").alias("gold_std"),
        F.min("gold").alias("gold_min"),
        F.mean((F.col("gold") < 0).cast("double")).alias("neg_event_ratio"),
        F.mean(F.col("quad").isin(3, 4).cast("double")).alias("conflict_quad_share"),
        F.sum(F.col("root").isin("18", "19", "20").cast("int")).alias("violent_event_count"),
        F.mean(F.col("root").isin("18", "19", "20").cast("double")).alias("violent_event_share"),
        F.mean("ev_tone").alias("ev_tone_mean"), F.stddev("ev_tone").alias("ev_tone_std"),
        F.sum("n_mentions").alias("sum_num_mentions"), F.mean("n_mentions").alias("avg_num_mentions"),
        F.mean("n_sources").alias("avg_num_sources"), F.mean("n_articles").alias("avg_num_articles"),
        F.mean((F.col("ev_day") < F.date_format("part_date", "yyyyMMdd")).cast("double"))
         .alias("old_event_share")))

    # mention / propagation
    ev_prop = (pairs.groupBy("country", "part_date", "GlobalEventID").agg(
        F.count("*").alias("n_m"), F.countDistinct("src").alias("n_s"),
        F.min("mt").alias("t0"), F.max("mt").alias("t1"))
        .withColumn("spread_min", (F.unix_timestamp("t1") - F.unix_timestamp("t0")) / 60.0))
    prop_day = ev_prop.groupBy("country", "part_date").agg(
        F.mean("n_m").alias("obs_mentions_per_event"),
        F.mean("n_s").alias("obs_sources_per_event"),
        F.mean("spread_min").alias("spread_mean_min"),
        F.expr("percentile_approx(spread_min, 0.5)").alias("spread_p50_min"),
        F.mean((F.col("n_s") >= 2).cast("double")).alias("multi_source_event_share"))

    w = Window.partitionBy("country", "part_date")
    src_day = (pairs.filter(F.col("src").isNotNull())
               .groupBy("country", "part_date", "src").agg(F.count("*").alias("n"))
               .withColumn("p", F.col("n") / F.sum("n").over(w))
               .groupBy("country", "part_date").agg(
                   F.sum("n").alias("pair_count"),
                   F.count("*").alias("unique_mention_sources"),
                   F.sum(F.col("p") * F.col("p")).alias("source_hhi"),
                   F.max("p").alias("top_source_share"),
                   (-F.sum(F.col("p") * F.log("p"))).alias("source_entropy")))

    # document-level (distinct URL) + GKG
    docs = pairs.groupBy("country", "part_date", "MentionIdentifier").agg(
        fi("doc_tone").alias("doc_tone"), fi("doc_len").alias("doc_len"),
        F.max(F.col("gkg_doc").isNotNull().cast("int")).alias("matched"),
        fi("gkg_tone").alias("gkg_tone"), F.max("conf").alias("conf"))
    doc_day = docs.groupBy("country", "part_date").agg(
        F.count("*").alias("doc_count"),
        F.mean("doc_tone").alias("doc_tone_mean"), F.stddev("doc_tone").alias("doc_tone_std"),
        F.mean("doc_len").alias("doc_len_mean"), F.mean("conf").alias("mention_conf_mean"),
        F.mean("matched").alias("gkg_match_rate"),
        F.mean("gkg_tone").alias("gkg_tone_mean"), F.stddev("gkg_tone").alias("gkg_tone_std"))

    grid = spark.createDataFrame([(c, d) for c in countries for d in days],
                                 ["country", "part_date"])
    final = grid
    for t in (ev_day, prop_day, src_day, doc_day):
        final = final.join(t, ["country", "part_date"], "left")
    final = final.withColumn("has_data", F.col("event_count").isNotNull().cast("int"))
    pdf = final.orderBy("country", "part_date").toPandas()
    pdf["part_date"] = pd.to_datetime(pdf["part_date"])
    return pdf