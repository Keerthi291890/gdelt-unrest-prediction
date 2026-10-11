"""
build_lag_variant.py  (Person B)  -- persistence-lag SENSITIVITY table

  python processing/person_b/build_lag_variant.py 60
  -> data/processed/person_b/model_table_lag60.csv

Identical to model_table.csv except p_lag_level / p_lag_ratio are recomputed with a different lag.
Labels, folds, GDELT features and column order are untouched. Writes a NEW file, overwrites nothing.
Uses only the two ORIGINAL UCDP files in data/external/ucdp_candidate/ (not the August file).
Sanity: with lag 30 it must reproduce the original columns exactly.
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from features_lib import COUNTRIES, daily_deaths, load_ucdp, persistence_for_lag

ROOT = Path(__file__).resolve().parents[2]
D = ROOT / "data" / "processed" / "person_b"


if __name__ == "__main__":
    L = int(sys.argv[1])
    ucdp = sorted((ROOT / "data" / "external" / "ucdp_candidate").glob("*.csv"))
    assert len(ucdp) == 2, f"expected the 2 original UCDP files, found {[f.name for f in ucdp]}"
    daily = daily_deaths(load_ucdp(ucdp), "2026-01-01", "2026-07-31")
    P = persistence_for_lag(daily, L)

    mt = pd.read_csv(D / "model_table.csv", parse_dates=["date"])
    new = mt.drop(columns=["p_lag_level", "p_lag_ratio"]).merge(P, on=["country", "date"], how="left")
    new = new[list(mt.columns)]
    assert new[["p_lag_level", "p_lag_ratio"]].notna().all().all(), "lag too long for available history"
    if L == 30:
        for c in ("p_lag_level", "p_lag_ratio"):
            assert np.allclose(new[c], mt[c], rtol=1e-9, atol=1e-12), f"lag-30 mismatch in {c}"
        print("lag 30 reproduces the original columns exactly")
    out = D / f"model_table_lag{L}.csv"
    new.to_csv(out, index=False)
    print("wrote", out, new.shape)