from pathlib import Path
from pyspark.sql import SparkSession, functions as F

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data" / "PersonA_handoff" / "joined_daily"

spark = (
    SparkSession.builder
    .appName("Inspect_Different_Duplicate_Rows")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "16")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# Read only the columns we actually need
cols = [
    "GlobalEventID",
    "MentionIdentifier",
    "MentionTimeDate",
    "ActionGeo_CountryCode",
    "date",
    "source_common_name",
    "v1_5_tone",
    "gkg_country_codes"
]

df = spark.read.parquet(str(DATA / "*")).select(*cols)

# ---------------------------------------------------------
# Find duplicated Event + Mention pairs
# ---------------------------------------------------------

duplicate_pairs = (
    df.groupBy("GlobalEventID", "MentionIdentifier")
      .count()
      .filter(F.col("count") > 1)
      .select("GlobalEventID", "MentionIdentifier")
)

# ---------------------------------------------------------
# For each duplicate pair, check whether selected columns
# have more than one distinct value.
# ---------------------------------------------------------

different_pairs = (
    df.join(
        F.broadcast(duplicate_pairs),
        ["GlobalEventID", "MentionIdentifier"],
        "inner"
    )
    .groupBy("GlobalEventID", "MentionIdentifier")
    .agg(
        *[
            F.countDistinct(F.col(c)).alias(f"{c}_variants")
            for c in cols[2:]
        ]
    )
    .filter(
        (F.col("MentionTimeDate_variants") > 1)
        | (F.col("ActionGeo_CountryCode_variants") > 1)
        | (F.col("date_variants") > 1)
        | (F.col("source_common_name_variants") > 1)
        | (F.col("v1_5_tone_variants") > 1)
        | (F.col("gkg_country_codes_variants") > 1)
    )
    .select("GlobalEventID", "MentionIdentifier")
)

print(
    "NUMBER OF PAIRS WITH DIFFERENCES:",
    different_pairs.count()
)

# ---------------------------------------------------------
# Show examples
# ---------------------------------------------------------

print("\nSAMPLE OF DIFFERENT ROWS:")

sample = (
    df.join(
        F.broadcast(different_pairs.limit(20)),
        ["GlobalEventID", "MentionIdentifier"],
        "inner"
    )
    .orderBy("GlobalEventID", "MentionIdentifier")
)

sample.show(40, truncate=70)

spark.stop()