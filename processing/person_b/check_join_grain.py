
from pathlib import Path
from pyspark.sql import SparkSession, functions as F

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data" / "PersonA_handoff" / "joined_daily"

spark = (
    SparkSession.builder
    .appName("Check_Join_Grain")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet(str(DATA / "*"))

print("TOTAL ROWS:", df.count())
print("DISTINCT EVENTS:", df.select("GlobalEventID").distinct().count())
print("DISTINCT MENTIONS:", df.select("MentionIdentifier").distinct().count())

print("\nDUPLICATE ROWS AT EVENT-MENTION LEVEL:")
pairs = df.groupBy("GlobalEventID", "MentionIdentifier").count()

pairs.select(
    F.sum(F.when(F.col("count") > 1, 1).otherwise(0))
    .alias("duplicate_event_mention_pairs"),
    F.max("count").alias("max_rows_per_pair")
).show()

print("\nDATE CONSISTENCY:")
df.select(
    F.sum(
        F.when(
            F.substring("date", 1, 8) !=
            F.substring("MentionTimeDate", 1, 8),
            1
        ).otherwise(0)
    ).alias("gkg_mention_date_mismatches")
).show()

print("\nMOST REPEATED EVENT-MENTION PAIRS:")
pairs.filter(F.col("count") > 1).orderBy(
    F.desc("count")
).show(10, truncate=False)

spark.stop()