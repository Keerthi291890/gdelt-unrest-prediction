# processing/person_b/calibrate_isotonic.py
from pathlib import Path
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import IsotonicRegression

ROOT = Path(__file__).resolve().parents[2]
D = ROOT / "data" / "processed" / "person_b"
spark = (SparkSession.builder.appName("m2_isotonic").master("local[2]")
         .config("spark.ui.enabled", "false").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

O = pd.read_csv(D / "oof_predictions_mllib_primary.csv")
O["fold"] = O["fold"].astype(str)
O = O[O.fold.isin(["1", "2", "3"]) & O.country.isin(["NI", "UP"])]

rows = []
for fset, model in [("persist_lag30", "rf"), ("persist_lag30", "linear"),
                    ("lag30_plus_gdelt", "gbt"), ("lag30_plus_gdelt", "rf")]:
    g = O[(O.fset == fset) & (O.model == model)]
    fit_pd, te_pd = g[g.fold.isin(["1", "2"])], g[g.fold == "3"].copy()
    assert len(fit_pd) == 56 and len(te_pd) == 28, (len(fit_pd), len(te_pd))
    prev = fit_pd["y"].mean()

    
    asm = VectorAssembler(inputCols=["p"], outputCol="features")

    # Fit isotonic regression using folds 1 and 2 only.
    fit_pd = fit_pd.reset_index(drop=True).copy()
    fit_sdf = asm.transform(
       spark.createDataFrame(fit_pd[["p", "y"]].astype(float))
    )

    iso = IsotonicRegression(
     featuresCol="features",
     labelCol="y",
     isotonic=True
    ).fit(fit_sdf)

    # Give every test row a unique ID so predictions match the correct rows.
    te_pd = te_pd.reset_index(drop=True).copy()
    te_pd["rid"] = np.arange(len(te_pd), dtype=np.int64)

    te_input = te_pd[["rid", "p", "y"]].copy()
    te_input["p"] = te_input["p"].astype(float)
    te_input["y"] = te_input["y"].astype(float)

    te_sdf = asm.transform(spark.createDataFrame(te_input))

    pred_pd = (
       iso.transform(te_sdf)
       .select("rid", "prediction")
       .toPandas()
       .rename(columns={"prediction": "p_cal"})
    )

    # Join predictions using the explicit row ID.
    te_pd = te_pd.merge(
     pred_pd,
     on="rid",
     how="left",
     validate="one_to_one"
    )

    assert len(te_pd) == 28, f"Expected 28 test rows, got {len(te_pd)}"
    assert te_pd["p_cal"].notna().all(), "Missing isotonic predictions"

    # Isotonic calibration must assign the same prediction to identical scores.
    assert (
       te_pd.groupby("p")["p_cal"].nunique() == 1
    ).all(), "Identical raw scores have different calibrated predictions"

    # Calibrated predictions must be non-decreasing as raw scores increase.
    chk = te_pd.sort_values(["p", "rid"])
    assert (
       chk["p_cal"].diff().dropna() >= -1e-12
    ).all(), "Isotonic predictions are not monotone"
    y = te_pd["y"].values
    b_raw = np.mean((te_pd["p"] - y) ** 2)
    b_cal = np.mean((te_pd["p_cal"] - y) ** 2)
    b_const = np.mean((prev - y) ** 2)
    rows.append(dict(fset=fset, model=model, fit_n=len(fit_pd), test_n=len(te_pd),
                     test_pos=int(y.sum()), brier_raw=b_raw, brier_isotonic=b_cal,
                     brier_const=b_const, skill_raw=1 - b_raw / b_const,
                     skill_isotonic=1 - b_cal / b_const,
                     iso_boundaries=len(iso.boundaries)))

R = pd.DataFrame(rows)
R.to_csv(D / "isotonic_calibration.csv", index=False)
pd.set_option("display.width", 200, "display.max_columns", 20)
print(R.round(3).to_string(index=False))
spark.stop()