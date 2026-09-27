from pyspark.sql import SparkSession
from pyspark.sql.functions import col

import os
import re


PROJECT_ROOT = str(Path(__file__).resolve().parents[2])

EVENTS_ROOT = PROJECT_ROOT + r"\data\processed\person_a\events"
MENTIONS_ROOT = PROJECT_ROOT + r"\data\processed\person_a\mentions"

TARGET_COUNTRIES = ["IN", "NI", "SF", "UP", "US"]


spark = (
    SparkSession.builder
    .appName("DiagnoseEventsMentionsCoverage")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("=" * 70)
print("EVENTS -> MENTIONS COVERAGE DIAGNOSTIC")
print("=" * 70)


# ------------------------------------------------------------
# 1. Find Events date folders
# ------------------------------------------------------------

event_folders = []

for name in os.listdir(EVENTS_ROOT):
    full_path = os.path.join(EVENTS_ROOT, name)

    if os.path.isdir(full_path) and re.fullmatch(r"\d{8}", name):
        event_folders.append(full_path)

event_folders.sort()

print(f"Events date folders : {len(event_folders)}")


# ------------------------------------------------------------
# 2. Read Events
# ------------------------------------------------------------

events = spark.read.parquet(*event_folders)

target_events = (
    events
    .filter(col("ActionGeo_CountryCode").isin(TARGET_COUNTRIES))
    .select("GlobalEventID", "ActionGeo_CountryCode")
    .dropna(subset=["GlobalEventID"])
    .dropDuplicates(["GlobalEventID"])
)

target_event_count = target_events.count()

print(f"Distinct target-country Event IDs : {target_event_count:,}")


# ------------------------------------------------------------
# 3. Find Mentions timestamp folders
# ------------------------------------------------------------

mention_folders = []

for name in os.listdir(MENTIONS_ROOT):
    full_path = os.path.join(MENTIONS_ROOT, name)

    if os.path.isdir(full_path) and re.fullmatch(
        r"\d{14}\.mentions\.CSV", name
    ):
        mention_folders.append(full_path)

mention_folders.sort()

print(f"Mentions timestamp folders : {len(mention_folders)}")


# ------------------------------------------------------------
# 4. Read Mentions
# ------------------------------------------------------------

mentions = (
    spark.read
    .parquet(*mention_folders)
    .select("GlobalEventID")
    .dropna(subset=["GlobalEventID"])
    .dropDuplicates(["GlobalEventID"])
)

mention_event_count = mentions.count()

print(f"Distinct Event IDs in Mentions : {mention_event_count:,}")


# ------------------------------------------------------------
# 5. Find target Events that HAVE Mentions
# ------------------------------------------------------------

matched = (
    target_events
    .join(
        mentions,
        on="GlobalEventID",
        how="inner"
    )
)

matched_count = matched.count()


# ------------------------------------------------------------
# 6. Find target Events that DO NOT have Mentions
# ------------------------------------------------------------

unmatched = (
    target_events
    .join(
        mentions,
        on="GlobalEventID",
        how="left_anti"
    )
)

unmatched_count = unmatched.count()


# ------------------------------------------------------------
# 7. Results
# ------------------------------------------------------------

print("\n" + "-" * 70)
print("COVERAGE RESULTS")
print("-" * 70)

print(f"Target-country Event IDs       : {target_event_count:,}")
print(f"Event IDs with Mentions        : {matched_count:,}")
print(f"Event IDs without Mentions     : {unmatched_count:,}")

if target_event_count > 0:
    coverage = matched_count / target_event_count * 100
    print(f"Events with Mentions           : {coverage:.2f}%")

    no_coverage = unmatched_count / target_event_count * 100
    print(f"Events without Mentions        : {no_coverage:.2f}%")


# ------------------------------------------------------------
# 8. Coverage by country
# ------------------------------------------------------------

print("\n" + "-" * 70)
print("COVERAGE BY TARGET COUNTRY")
print("-" * 70)

country_total = (
    target_events
    .groupBy("ActionGeo_CountryCode")
    .count()
    .withColumnRenamed("count", "total_events")
)

country_matched = (
    matched
    .groupBy("ActionGeo_CountryCode")
    .count()
    .withColumnRenamed("count", "events_with_mentions")
)

(
    country_total
    .join(
        country_matched,
        on="ActionGeo_CountryCode",
        how="left"
    )
    .fillna(0)
    .orderBy("ActionGeo_CountryCode")
    .show(truncate=False)
)


print("\n" + "=" * 70)
print("DIAGNOSTIC COMPLETE")
print("=" * 70)

spark.stop()
