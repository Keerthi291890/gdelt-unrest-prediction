"""
demo_score.py  (Person B -> Person C): runnable end-to-end scoring example on the data we have.

  python processing/person_b/demo_score.py [as_of] [ucdp_covered_through]
  default: as_of=2026-06-30 (last GDELT day), ucdp_covered_through=2026-07-31

Shows: a normal scored day, then the SAME day with UCDP data that is too old (must refuse).
Reads the newest models/v* directory and its contract (persistence lag included).
"""
import sys
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession

from features_lib import load_ucdp
from score_day import score

ROOT = Path(__file__).resolve().parents[2]
D = ROOT / "data" / "processed" / "person_b"
as_of = pd.Timestamp(sys.argv[1] if len(sys.argv) > 1 else "2026-06-30")
covered = pd.Timestamp(sys.argv[2] if len(sys.argv) > 2 else "2026-07-31")
model_dir = sorted((ROOT / "models").glob("v*"))[-1]

feat = pd.read_csv(D / "country_day_features.csv", parse_dates=["part_date"])
events = load_ucdp(sorted((ROOT / "data" / "external" / "ucdp_candidate").glob("*.csv")))

spark = (SparkSession.builder.appName("demo_score").master("local[2]")
         .config("spark.ui.enabled", "false").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
pd.set_option("display.width", 220, "display.max_columns", 20)

print(f"\nmodel {model_dir.name} | as_of {as_of.date()} | UCDP covered through {covered.date()}")
print(score(spark, model_dir, feat, events, covered, as_of).round(3).to_string(index=False))

old = as_of - pd.Timedelta(days=200)
print(f"\nSame day, but newest UCDP release only covers through {old.date()} (too old) -> must refuse:")
print(score(spark, model_dir, feat, events, old, as_of).round(3).to_string(index=False))
print("\nNote: probabilities are UNCALIBRATED risk scores; shadow_p_gdelt_SHADOW_ONLY must never alert.")
spark.stop()