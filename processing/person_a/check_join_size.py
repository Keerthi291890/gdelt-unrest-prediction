from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

ROOT = str(Path(__file__).resolve().parents[2] / "data\processed\person_a"

spark = (
    SparkSession.builder
    .appName("CheckJoinSize")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


def parquet_dirs(root, condition):
    return [
        os.path.join(root, x)
        for x in os.listdir(root)
        if condition(x)
    ]


# ---------------------------------------------------------
# Read Events: only YYYYMMDD folders
# ---------------------------------------------------------

events_paths = parquet_dirs(
    ROOT + r"\events",
    lambda x: x.isdigit() and len(x) == 8
)

events = spark.read.parquet(*events_paths)


# ---------------------------------------------------------
# Read Mentions: only timestamp folders
# ---------------------------------------------------------

mentions_paths = parquet_dirs(
    ROOT + r"\mentions",
    lambda x: x.endswith(".mentions.CSV")
)

mentions = spark.read.parquet(*mentions_paths)


# ---------------------------------------------------------
# Read compact GKG: only 14-digit timestamp folders
# ---------------------------------------------------------

gkg_paths = parquet_dirs(
    ROOT + r"\gkg_countries",
    lambda x: x.isdigit() and len(x) == 14
)

gkg = spark.read.parquet(*gkg_paths)


# ---------------------------------------------------------
# 1. Target-country Events
# ---------------------------------------------------------

countries = ["IN", "NI", "SF", "UP", "US"]

target_events = events.filter(
    col("ActionGeo_CountryCode").isin(countries)
)

target_event_count = target_events.count()

print("\nTARGET EVENTS:", target_event_count)


# ---------------------------------------------------------
# 2. Events -> Mentions
# ---------------------------------------------------------

event_mentions = target_events.join(
    mentions,
    on="GlobalEventID",
    how="inner"
)

event_mentions_count = event_mentions.count()

print("EVENT-MENTION ROWS:", event_mentions_count)


# ---------------------------------------------------------
# 3. Mentions -> GKG
# ---------------------------------------------------------

joined = event_mentions.join(
    gkg,
    event_mentions.MentionIdentifier == gkg.document_identifier,
    how="left"
)

final_count = joined.count()

print("FINAL LEFT-JOIN ROWS:", final_count)


# ---------------------------------------------------------
# 4. GKG match statistics
# ---------------------------------------------------------

matched = joined.filter(
    col("document_identifier").isNotNull()
).count()

unmatched = joined.filter(
    col("document_identifier").isNull()
).count()

print("GKG MATCHED ROWS:", matched)
print("GKG UNMATCHED ROWS:", unmatched)


# ---------------------------------------------------------
# 5. Check duplicate GKG URLs
# ---------------------------------------------------------

duplicate_urls = (
    gkg.groupBy("document_identifier")
       .count()
       .filter(col("count") > 1)
       .count()
)

print("GKG DUPLICATE DOCUMENT URLs:", duplicate_urls)

print("\nJOIN SIZE CHECK COMPLETE")

spark.stop()
