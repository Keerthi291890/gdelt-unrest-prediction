# processing/person_b/build_model_table.py
import json
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
D = ROOT / "data" / "processed" / "person_b"

M_START, M_END = "2026-04-14", "2026-06-30"
TIER = {"NI": "primary", "UP": "primary", "IN": "marginal", "SF": "excluded", "US": "excluded"}
GAP = 7                                              # label horizon
FOLDS = {1: ("2026-05-20", "2026-06-02"),            # 14-day test blocks, expanding train
         2: ("2026-06-03", "2026-06-16"),
         3: ("2026-06-17", "2026-06-30")}

LOG_COLS = ["event_count", "doc_count", "unique_mention_sources", "spread_p50_min", "media_per_event"]
RAW_COLS = ["violent_event_share", "conflict_quad_share", "gold_mean", "neg_event_ratio",
            "ev_tone_mean", "source_hhi", "multi_source_event_share"]


def make_gdelt_features(g):
    """Every feature at date t uses only rows with date <= t."""
    g = g.sort_values(["country", "date"]).copy()
    g["media_per_event"] = g["doc_count"] / g["event_count"]
    out = g[["country", "date"]].copy()
    for c in LOG_COLS + RAW_COLS:
        x = np.log1p(g[c]) if c in LOG_COLS else g[c]
        x = x.groupby(g["country"])
        recent = x.transform(lambda s: s.rolling(7, min_periods=7).mean())      # t-6..t
        base = x.transform(lambda s: s.shift(7).rolling(14, min_periods=7).mean())  # t-20..t-7
        out[f"g_{c}_d7"] = recent - base
    return out


def make_persistence(lab):
    lab = lab.sort_values(["country", "date"]).copy()
    base_lag = lab.groupby("country")["baseline_7d"].shift(30)
    out = lab[["country", "date"]].copy()
    # oracle: assumes UCDP data up to day t is already available (it is not, in real time)
    out["p_or_ratio"] = np.log1p(lab["persist_oracle_7d"]) - np.log1p(lab["baseline_7d"])
    out["p_or_level"] = np.log1p(lab["persist_oracle_7d"])
    # realistic: UCDP data available only up to t-30
    out["p_lag_ratio"] = np.log1p(lab["persist_lag30_7d"]) - np.log1p(base_lag)
    out["p_lag_level"] = np.log1p(lab["persist_lag30_7d"])
    return out


feat = pd.read_csv(D / "country_day_features.csv", parse_dates=["part_date"]).rename(
    columns={"part_date": "date"})
lab = pd.read_csv(D / "labels.csv", parse_dates=["date"])

G = make_gdelt_features(feat)
P = make_persistence(lab)
G_COLS = [c for c in G.columns if c.startswith("g_")]
P_OR = ["p_or_ratio", "p_or_level"]
P_LAG = ["p_lag_ratio", "p_lag_level"]

# ---- leakage test: features at t must be unchanged if all later data is deleted ----
for t in ["2026-05-10", "2026-06-01", "2026-06-20"]:
    t = pd.Timestamp(t)
    a = make_gdelt_features(feat[feat.date <= t]).query("date == @t").set_index("country")[G_COLS]
    b = G.query("date == @t").set_index("country")[G_COLS]
    assert np.allclose(a.sort_index().values, b.sort_index().values, equal_nan=True), f"LEAK at {t}"
    a = make_persistence(lab[lab.date <= t]).query("date == @t").set_index("country")[P_OR + P_LAG]
    b = P.query("date == @t").set_index("country")[P_OR + P_LAG]
    assert np.allclose(a.sort_index().values, b.sort_index().values, equal_nan=True), f"LEAK(P) at {t}"
print("leakage test passed")

# ---- assemble ----
T = (lab[["country", "date", "label", "target_logratio", "future_7d", "baseline_7d"]]
     .merge(P, on=["country", "date"]).merge(G, on=["country", "date"]))
T = T[(T.date >= M_START) & (T.date <= M_END)].copy()
T["tier"] = T["country"].map(TIER)

# ---- expanding-window splits with a gap equal to the label horizon ----
for k, (s, e) in FOLDS.items():
    s, e = pd.Timestamp(s), pd.Timestamp(e)
    train_end = s - pd.Timedelta(days=GAP + 1)       # label(t) covers t+1..t+7, so t <= s-8
    T[f"fold{k}"] = np.select(
        [T.date.between(s, e), T.date <= train_end, T.date < s],
        ["test", "train", "gap"], default="unused")

feature_sets = {
    "persist_oracle": P_OR,
    "persist_lag30": P_LAG,
    "gdelt_only": G_COLS,
    "lag30_plus_gdelt": P_LAG + G_COLS,        # the realistic headline comparison
    "oracle_plus_gdelt": P_OR + G_COLS,
}
assert not any(k in c for cols in feature_sets.values() for c in cols
               for k in ("future", "label", "target"))

T.to_csv(D / "model_table.csv", index=False)
json.dump(feature_sets, open(D / "feature_sets.json", "w"), indent=2)

# ---- report ----
print("\nshape:", T.shape)
print("NaNs in features:\n", T[sum(feature_sets.values(), [])].isna().sum()[lambda s: s > 0])
for k in FOLDS:
    r = (T[T.tier != "excluded"].groupby(["country", f"fold{k}"])["label"]
         .agg(["size", "sum"]).unstack(fill_value=0))
    print(f"\nFOLD {k}  (size / positives)\n", r)