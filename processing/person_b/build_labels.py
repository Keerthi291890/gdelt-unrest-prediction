# processing/person_b/build_labels.py
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "data" / "external" / "ucdp_candidate"
OUT = ROOT / "data" / "processed" / "person_b"
OUT.mkdir(parents=True, exist_ok=True)

RATIO, FLOOR, H, BASE_DAYS = 1.5, 5.0, 7, 28          # FROZEN (docs/label_spec.md)
MAP = {"United States of America": "US", "India": "IN", "Nigeria": "NI",
       "Ukraine": "UP", "South Africa": "SF"}
CODES = list(MAP.values())

RELEASE_ORDER = {
    "GEDEvent_v26_01_26_06.csv": 1,  # older release: Jan-Jun
    "GEDEvent_v26_0_7.csv": 2,       # later release: July
}

frames = []

for f in sorted(SRC.glob("*.csv")):
    assert f.name in RELEASE_ORDER, f"Unexpected UCDP file: {f.name}"

    x = pd.read_csv(
        f,
        usecols=["id", "country", "date_start", "best"],
        low_memory=False
    )

    x["release"] = RELEASE_ORDER[f.name]
    frames.append(x)
df = pd.concat(frames, ignore_index=True)
assert len(df) == 11879, len(df)

# overlapping monthly releases: keep the latest release of each event id
df = df.sort_values("release").drop_duplicates("id", keep="last")
df["cc"] = df["country"].map(MAP)
df = df.dropna(subset=["cc"])
assert len(df) == 3028, len(df)

df["date"] = pd.to_datetime(df["date_start"])          # deaths attributed to date_start
idx = pd.date_range("2026-01-01", "2026-07-31")
daily = (df.groupby(["date", "cc"])["best"].sum().unstack("cc")
           .reindex(index=idx, columns=CODES).fillna(0.0))

rows = []
for cc in CODES:
    s = daily[cc]
    w7 = s.rolling(H).sum()
    out = pd.DataFrame({
        "country": cc, "date": idx, "deaths": s.values,
        "baseline_7d": (7.0 * s.rolling(BASE_DAYS, min_periods=BASE_DAYS).mean()).values,
        "future_7d": w7.shift(-H).values,                 # sum over t+1..t+7
        "persist_oracle_7d": w7.values,                   # uses data up to t
        "persist_lag30_7d": w7.shift(30).values,          # realistic UCDP release delay
    })
    out["threshold"] = np.maximum(RATIO * out["baseline_7d"], FLOOR)
    ok = out["baseline_7d"].notna() & out["future_7d"].notna()
    out["label"] = np.where(ok, (out["future_7d"] >= out["threshold"]).astype(float), np.nan)
    out["target_logratio"] = np.log1p(out["future_7d"]) - np.log1p(out["baseline_7d"])
    rows.append(out)

lab = pd.concat(rows, ignore_index=True)
lab.to_csv(OUT / "labels.csv", index=False)

def episodes(x):                                          # number of 0->1 runs
    x = x.fillna(0).astype(int).values
    return int(((x[1:] == 1) & (x[:-1] == 0)).sum() + (x[0] == 1))

for name, a, b in [("SELECTION WINDOW (Jan28-Mar24)", "2026-01-28", "2026-03-24"),
                   ("MODELLING WINDOW (Apr1-Jun30)", "2026-04-01", "2026-06-30")]:
    w = lab[(lab.date >= a) & (lab.date <= b) & lab.label.notna()]
    print(f"\n{name}")
    print(w.groupby("country").agg(days=("label", "size"), positive=("label", "sum"),
          pct=("label", lambda v: round(100 * v.mean(), 1)),
          episodes=("label", episodes)).reindex(CODES))