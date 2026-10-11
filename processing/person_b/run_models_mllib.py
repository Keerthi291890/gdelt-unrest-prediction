# processing/person_b/run_models_mllib.py
import json, warnings
from pathlib import Path
import numpy as np
import sys
import pandas as pd
from scipy.stats import spearmanr
from metrics_np import average_precision_score, roc_auc_score
from pyspark.sql import SparkSession, functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier, GBTClassifier
from pyspark.ml.regression import LinearRegression, RandomForestRegressor, GBTRegressor

warnings.filterwarnings("ignore")
ROOT = Path(__file__).resolve().parents[2]
D = ROOT / "data" / "processed" / "person_b"
FS = json.load(open(D / "feature_sets.json"))

spark = (SparkSession.builder.appName("m2_mllib").master("local[2]")
         .config("spark.sql.shuffle.partitions", "4").config("spark.ui.enabled", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

PRIMARY = "--primary" in sys.argv
TABLE = next((a.split("=", 1)[1] for a in sys.argv if a.startswith("--table=")), "model_table.csv")
TAG = next((a.split("=", 1)[1] for a in sys.argv if a.startswith("--tag=")), "")
SUF = ("_primary" if PRIMARY else "") + TAG
# ...and read the table from:  (D / TABLE).as_posix()

sdf = (spark.read.csv((D / TABLE).as_posix(), header=True, inferSchema=True)
       .filter((F.col("tier") == "primary") if PRIMARY else (F.col("tier") != "excluded"))
       .withColumn("rid", F.monotonically_increasing_id())
       .cache())
print("spark", spark.version, "| rows", sdf.count())

HEADLINE = ["persist_lag30", "gdelt_only", "lag30_plus_gdelt"]
REFERENCE = ["persist_oracle", "oracle_plus_gdelt"]        # not deployable: reference only
FAMS = ["linear", "rf", "gbt"]
TIER = {"NI": "primary", "UP": "primary", "IN": "marginal"}   # SF, US excluded earlier


def make_clf(f):
    if f == "linear":
        return LogisticRegression(featuresCol="features", labelCol="y",
                                  regParam=0.1, elasticNetParam=0.0, maxIter=200)
    if f == "rf":
        return RandomForestClassifier(featuresCol="features", labelCol="y", numTrees=300,
                                      maxDepth=3, minInstancesPerNode=5, seed=0)
    return GBTClassifier(featuresCol="features", labelCol="y", maxIter=50, maxDepth=2,
                         stepSize=0.05, minInstancesPerNode=5, subsamplingRate=0.8, seed=0)


def make_reg(f):
    kw = dict(featuresCol="features", labelCol="target_logratio")
    if f == "linear":
        return LinearRegression(regParam=0.1, elasticNetParam=0.0, maxIter=200, **kw)
    if f == "rf":
        return RandomForestRegressor(numTrees=300, maxDepth=3, minInstancesPerNode=5, seed=0, **kw)
    return GBTRegressor(maxIter=50, maxDepth=2, stepSize=0.05, minInstancesPerNode=5,
                        subsamplingRate=0.8, seed=0, **kw)


def prob(df):
    if "probability" in df.columns:
        return vector_to_array("probability")[1]
    return 1.0 / (1.0 + F.exp(-2.0 * vector_to_array("rawPrediction")[1]))   # older GBT


parts = []
for k in (1, 2, 3):
    tr0 = sdf.filter(F.col(f"fold{k}") == "train").withColumn("y", F.col("label").cast("double"))
    te0 = sdf.filter(F.col(f"fold{k}") == "test").withColumn("y", F.col("label").cast("double"))
    prev = tr0.agg(F.avg("y")).first()[0]
    tmean = tr0.agg(F.avg("target_logratio")).first()[0]
    meta = (te0.select("rid", "country", "date", "y", "target_logratio").toPandas()
            .rename(columns={"target_logratio": "ry"}).assign(fold=str(k), pc=prev, tm=tmean))
    print(f"fold {k}: train n={tr0.count()} prev={prev:.3f} | test n={len(meta)}")
    for fs in HEADLINE + REFERENCE:
        asm = VectorAssembler(inputCols=FS[fs], outputCol="features")
        trv, tev = asm.transform(tr0), asm.transform(te0)
        for fam in FAMS:
            co = make_clf(fam).fit(trv).transform(tev)
            p = co.select("rid", prob(co).alias("p")).toPandas()
            ro = (make_reg(fam).fit(trv).transform(tev)
                  .select("rid", F.col("prediction").alias("rp")).toPandas())
            parts.append(meta.merge(p, on="rid").merge(ro, on="rid").assign(fset=fs, model=fam))
    for name, pv in (("always_neg", 0.0), ("train_prev", prev)):
        parts.append(meta.assign(p=pv, rp=tmean, fset="-", model=name))

O = pd.concat(parts, ignore_index=True)
O = pd.concat([O, O.assign(fold="pooled")], ignore_index=True)    # stacked out-of-fold
O.to_csv(D / f"oof_predictions_mllib{SUF}.csv", index=False)


def summarize(g):
    y, p, pc = g.y.values, g.p.values, g.pc.values
    n, pos = len(y), int(y.sum())
    ok = 0 < pos < n
    b, bc = np.mean((p - y) ** 2), np.mean((pc - y) ** 2)
    mae, mb = np.mean(np.abs(g.rp - g.ry)), np.mean(np.abs(g.tm - g.ry))
    sp = np.nan
    if n >= 5 and g.ry.std() > 0 and g.rp.std() > 0:
        sp = spearmanr(g.rp, g.ry).correlation
    return dict(N=n, Positives=pos,
                PR_AUC=average_precision_score(y, p) if ok else np.nan,
                ROC_AUC=roc_auc_score(y, p) if ok else np.nan,
                Brier=b, Brier_skill=(1 - b / bc) if pos > 0 else np.nan,
                MAE=mae, MAE_impr_vs_base=(mb - mae) / mb, Spearman=sp,
                mean_p=p.mean(), alerts_diag=int((p >= pc).sum()))   # diagnostics only


rows = []
for (fold, c, fs, m), g in O.groupby(["fold", "country", "fset", "model"]):
    rows.append(dict(Country=c, Tier=TIER[c], Fold=fold, Feature_set=fs, Model=m, **summarize(g)))
G = pd.DataFrame(rows)
G.to_csv(D / f"final_results_mllib{SUF}.csv", index=False)

pd.set_option("display.width", 230, "display.max_columns", 30, "display.max_rows", 500)
R3 = lambda d: d.round(3).to_string(index=False)

print("\n=== 1. Test positives per fold/country ===")
print(G[(G.Model == "always_neg") & (G.Fold != "pooled")]
      .pivot_table(index="Fold", columns="Country", values="Positives").astype(int))

print("\n=== 2. UKRAINE classification (primary) ===")
u = G[(G.Country == "UP") & G.Model.isin(FAMS + ["train_prev"])].sort_values(["Fold", "Feature_set", "Model"])
print(R3(u[["Fold", "Feature_set", "Model", "N", "Positives", "PR_AUC", "ROC_AUC", "Brier", "Brier_skill"]]))

print("\n=== 3. REGRESSION (mean over folds 1-3) ===")
rg = (G[(G.Fold != "pooled") & G.Model.isin(FAMS)]
      .groupby(["Country", "Tier", "Feature_set", "Model"])[["MAE", "MAE_impr_vs_base", "Spearman"]].mean())
print(rg.round(3))

print("\n=== 4. Zero-positive blocks IN/NI (pooled): diagnostics only ===")
z = G[(G.Fold == "pooled") & G.Country.isin(["IN", "NI"]) & G.Model.isin(FAMS)]
print(R3(z[["Country", "Feature_set", "Model", "N", "mean_p", "Brier", "alerts_diag"]]))

print("\n=== 5. PRE-REGISTERED RULES (lag30_plus_gdelt vs persist_lag30) ===")
H = G[G.Feature_set.isin(["persist_lag30", "lag30_plus_gdelt"]) & G.Model.isin(FAMS)]
c = H[(H.Country == "UP") & H.Fold.isin(["1", "3"])].pivot_table(
    index=["Model", "Fold"], columns="Feature_set", values="PR_AUC")
c["gdelt_wins"] = c["lag30_plus_gdelt"] > c["persist_lag30"]
print(c.round(3))
print("RULE C passes for:", [m for m, v in c.groupby("Model")["gdelt_wins"].all().items() if v] or "none")
r = (H[H.Fold.isin(["1", "2", "3"])].groupby(["Model", "Country", "Feature_set"])[["MAE", "Spearman"]]
     .mean().unstack("Feature_set"))
r["better_both"] = ((r[("MAE", "lag30_plus_gdelt")] < r[("MAE", "persist_lag30")]) &
                    (r[("Spearman", "lag30_plus_gdelt")] > r[("Spearman", "persist_lag30")]))
print(r.round(3))
print("RULE R passes for:", [m for m, v in r.groupby("Model")["better_both"].sum().items() if v >= 2] or "none")
spark.stop()