from pathlib import Path
from pyspark.sql import SparkSession, functions as F

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data" / "PersonA_handoff" / "joined_daily"

spark = (
    SparkSession.builder
    .appName("Check_Exact_Duplicates")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet(str(DATA / "*"))

# Create a signature using every column
signature = F.sha2(
    F.to_json(F.struct(*[F.col(c) for c in df.columns])),
    256
)

df = df.withColumn("_signature", signature)

# Examine only event-mention pairs that occur more than once
duplicate_pairs = (
    df.groupBy("GlobalEventID", "MentionIdentifier")
      .count()
      .filter(F.col("count") > 1)
      .select("GlobalEventID", "MentionIdentifier")
)

repeated = df.join(
    duplicate_pairs,
    ["GlobalEventID", "MentionIdentifier"],
    "inner"
)

# Count distinct complete rows within each pair
pair_summary = (
    repeated.groupBy("GlobalEventID", "MentionIdentifier")
    .agg(
        F.count("*").alias("rows"),
        F.countDistinct("_signature").alias("distinct_full_rows")
    )
)

print("DUPLICATE PAIR BREAKDOWN:")
pair_summary.groupBy("rows", "distinct_full_rows").count().show()

print("EXACT DUPLICATE ROW GROUPS:")
exact = (
    df.groupBy("_signature")
      .count()
      .filter(F.col("count") > 1)
)
print(exact.count())

spark.stop()