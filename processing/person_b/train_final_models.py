# processing/person_b/train_final_models.py
import json, sys
from datetime import date
from pathlib import Path
import numpy as np
from pyspark.sql import SparkSession, functions as F
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.ml.classification import RandomForestClassifier, GBTClassifier
from pyspark.ml.regression import RandomForestRegressor

ROOT = Path(__file__).resolve().parents[2]
D = ROOT / "data" / "processed" / "person_b"
args = [a for a in sys.argv[1:] if not a.startswith("--")]
VERSION = args[0] if args else "v" + date.today().strftime("%Y%m%d")
TABLE = next((a.split("=", 1)[1] for a in sys.argv if a.startswith("--table=")), "model_table.csv")
LAG = int(next((a.split("=", 1)[1] for a in sys.argv if a.startswith("--lag=")), "30"))
OUT = ROOT / "models" / VERSION
OUT.mkdir(parents=True, exist_ok=True)

FS = json.load(open(D / "feature_sets.json"))
spark = (SparkSession.builder.appName("m2_final_models").master("local[2]")
         .config("spark.ui.enabled", "false").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

df = (spark.read.csv((D / TABLE).as_posix(), header=True, inferSchema=True)
      .filter(F.col("tier") == "primary")
      .filter(F.col("label").isNotNull() & F.col("target_logratio").isNotNull())
      .withColumn("y", F.col("label").cast("double"))
      .cache())
n = df.count()
by_c = {r["country"]: r["count"] for r in df.groupBy("country").count().collect()}

assert set(by_c) == {"NI", "UP"} and n == 156, (by_c, n)

print("rows per country:", by_c)
stats = df.agg(F.sum("y").alias("pos"), F.avg("y").alias("prev"),
               F.min("date").alias("d0"), F.max("date").alias("d1")).first()
print(f"version {VERSION} | train rows {n} | positives {int(stats['pos'])} | prev {stats['prev']:.3f}")

SPECS = {
    "prod_clf": ("persist_lag30", RandomForestClassifier(
        featuresCol="features", labelCol="y", numTrees=300, maxDepth=3,
        minInstancesPerNode=5, seed=0)),
    "prod_reg": ("persist_lag30", RandomForestRegressor(
        featuresCol="features", labelCol="target_logratio", numTrees=300, maxDepth=3,
        minInstancesPerNode=5, seed=0)),
    "shadow_clf": ("lag30_plus_gdelt", GBTClassifier(
        featuresCol="features", labelCol="y", maxIter=50, maxDepth=2, stepSize=0.05,
        minInstancesPerNode=5, subsamplingRate=0.8, seed=0)),
}


def prob(x):
    return vector_to_array("probability")[1]


for name, (fset, est) in SPECS.items():
    pipe = Pipeline(stages=[VectorAssembler(inputCols=FS[fset], outputCol="features"), est])
    model = pipe.fit(df)
    path = (OUT / name).as_posix()
    model.write().overwrite().save(path)

    # round-trip test: reloaded model must reproduce in-memory predictions
    a = model.transform(df)
    b = PipelineModel.load(path).transform(df)
    col = "prediction"
    if name.endswith("clf"):
        da = np.array(a.select(prob(a).alias("p")).toPandas()["p"])
        db = np.array(b.select(prob(b).alias("p")).toPandas()["p"])
    else:
        da = np.array(a.select(col).toPandas()[col]); db = np.array(b.select(col).toPandas()[col])
    assert np.max(np.abs(da - db)) < 1e-9, f"round-trip mismatch for {name}"
    print(f"saved {name:10s} ({fset}, {len(FS[fset])} features) round-trip OK")

contract = {
    "version": VERSION,
    "trained_on": {
    "rows": n,
    "positives": int(stats["pos"]),
    "prevalence": float(stats["prev"]),
    "date_min": str(stats["d0"]),
    "date_max": str(stats["d1"]),
    "countries": ["NI", "UP"],
    "marginal_not_trained": ["IN"],
    "excluded": ["SF", "US"]
    },
    "supported_countries": ["NI", "UP"],
    "unsupported_behaviour": "for IN, SF, US return status='unsupported', no probability",
    "models": {k: {"feature_set": v[0], "features": FS[v[0]], "path": k,
                   "role": {"prod_clf": "production escalation probability",
                            "prod_reg": "production severity (log ratio)",
                            "shadow_clf": "shadow only, never alerts"}[k]}
               for k, v in SPECS.items()},
    "label": {"horizon_days": 7, "baseline_days": 28, "ratio": 1.5, "floor_deaths": 5,
              "source": "UCDP Candidate GEDEvent_v26_01_26_06.csv + GEDEvent_v26_0_7.csv"},
    "persistence_lag_days": LAG,
    "alert_threshold": {"rule": "p >= training prevalence", "value": float(stats["prev"]),
                        "status": "diagnostic only, not a calibrated decision rule"},
    "hyperparameters": {"rf": "300 trees, depth 3, min leaf 5, seed 0",
                        "gbt": "50 iter, depth 2, step 0.05, subsample 0.8, seed 0"},
    "country_code_system": "GDELT/FIPS: US IN NI UP SF",
    "evaluation_status": "pre-registered rules for GDELT improvement NOT met (see final_results_mllib.csv)",
    "known_limits": ["~11 escalation episodes, 2 test episodes (both UP)",
                     "propagation features truncated at midnight",
                     "UCDP Candidate data may be revised",
                     "no GCAM features", "IN is marginal and was NOT used for training; the production model must not score IN"],
}
json.dump(contract, open(OUT / "contract.json", "w"), indent=2)
print("contract written:", OUT / "contract.json")
spark.stop()