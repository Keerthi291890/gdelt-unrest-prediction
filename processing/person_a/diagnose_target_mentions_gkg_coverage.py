from pyspark.sql import SparkSession
from pyspark.sql.functions import col

import os
import re


PROJECT_ROOT = str(Path(__file__).resolve().parents[2])

EVENTS_ROOT = PROJECT_ROOT + r"\data\processed\person_a\events"
MENTIONS_ROOT = PROJECT_ROOT + r"\data\processed\person_a\mentions"
GKG_ROOT = PROJECT_ROOT + r"\data\processed\person_a\gkg_countries"

TARGET_COUNTRIES = ["IN", "NI", "SF", "UP", "US"]


spark = (
    SparkSession.builder
    .appName("DiagnoseTargetMentionsGKG")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("=" * 70)
print("TARGET-COUNTRY MENTIONS -> GKG COVERAGE DIAGNOSTIC")
print("=" * 70)


# ============================================================
# 1. EVENTS
# ============================================================

event_folders = []

for name in os.listdir(EVENTS_ROOT):
    full_path = os.path.join(EVENTS_ROOT, name)

    if os.path.isdir(full_path) and re.fullmatch(r"\d{8}", name):
        event_folders.append(full_path)

event_folders.sort()

print(f"Events date folders : {len(event_folders)}")

events = spark.read.parquet(*event_folders)


# Keep target-country Event IDs.
#
# We retain the country because this is the country anchor
# for the later integration.

target_events = (
    events
    .filter(col("ActionGeo_CountryCode").isin(TARGET_COUNTRIES))
    .select(
        "GlobalEventID",
        "ActionGeo_CountryCode"
    )
    .dropna(subset=["GlobalEventID"])
    .dropDuplicates(["GlobalEventID"])
)

target_event_count = target_events.count()

print(f"Target-country Event IDs : {target_event_count:,}")


# ============================================================
# 2. MENTIONS
# ============================================================

mention_folders = []

for name in os.listdir(MENTIONS_ROOT):
    full_path = os.path.join(MENTIONS_ROOT, name)

    if os.path.isdir(full_path) and re.fullmatch(
        r"\d{14}\.mentions\.CSV", name
    ):
        mention_folders.append(full_path)

mention_folders.sort()

print(f"Mentions timestamp folders : {len(mention_folders)}")

mentions = (
    spark.read
    .parquet(*mention_folders)
    .select(
        "GlobalEventID",
        "MentionIdentifier"
    )
    .dropna(subset=["GlobalEventID"])
)


# ============================================================
# 3. SELECT MENTIONS BELONGING TO TARGET EVENTS
# ============================================================

target_mentions = (
    mentions
    .join(
        target_events,
        on="GlobalEventID",
        how="inner"
    )
)

target_mention_rows = target_mentions.count()

print(f"Target-country Mention rows : {target_mention_rows:,}")


# ============================================================
# 4. UNIQUE TARGET MENTION URLs
# ============================================================

target_mention_urls = (
    target_mentions
    .filter(
        col("MentionIdentifier").isNotNull()
        & (col("MentionIdentifier") != "")
    )
    .select("MentionIdentifier")
    .dropDuplicates()
)

target_unique_urls = target_mention_urls.count()

print(
    f"Unique target Mention URLs : {target_unique_urls:,}"
)


# ============================================================
# 5. GKG
# ============================================================

gkg_folders = []

for name in os.listdir(GKG_ROOT):
    full_path = os.path.join(GKG_ROOT, name)

    if os.path.isdir(full_path) and re.fullmatch(r"\d{14}", name):
        gkg_folders.append(full_path)

gkg_folders.sort()

print(f"GKG timestamp folders : {len(gkg_folders)}")


gkg_urls = (
    spark.read
    .parquet(*gkg_folders)
    .select("document_identifier")
    .filter(
        col("document_identifier").isNotNull()
        & (col("document_identifier") != "")
    )
    .dropDuplicates()
)

gkg_unique_urls = gkg_urls.count()

print(
    f"Unique GKG document URLs : {gkg_unique_urls:,}"
)


# ============================================================
# 6. MATCH TARGET MENTION URLs TO GKG
# ============================================================

matched_urls = (
    target_mention_urls
    .join(
        gkg_urls,
        target_mention_urls.MentionIdentifier
        == gkg_urls.document_identifier,
        how="inner"
    )
)

matched_count = matched_urls.count()


# ============================================================
# 7. UNMATCHED TARGET MENTION URLs
# ============================================================

unmatched_urls = (
    target_mention_urls
    .join(
        gkg_urls,
        target_mention_urls.MentionIdentifier
        == gkg_urls.document_identifier,
        how="left_anti"
    )
)

unmatched_count = unmatched_urls.count()


# ============================================================
# 8. RESULTS
# ============================================================

print("\n" + "-" * 70)
print("TARGET MENTION -> GKG RESULTS")
print("-" * 70)

print(
    f"Target-country Mention rows : "
    f"{target_mention_rows:,}"
)

print(
    f"Unique target Mention URLs   : "
    f"{target_unique_urls:,}"
)

print(
    f"Unique GKG document URLs    : "
    f"{gkg_unique_urls:,}"
)

print(
    f"Matching target URLs        : "
    f"{matched_count:,}"
)

print(
    f"Unmatched target URLs       : "
    f"{unmatched_count:,}"
)


if target_unique_urls > 0:

    match_percentage = (
        matched_count / target_unique_urls * 100
    )

    unmatched_percentage = (
        unmatched_count / target_unique_urls * 100
    )

    print(
        f"Target URL match percentage : "
        f"{match_percentage:.2f}%"
    )

    print(
        f"Target URL unmatched        : "
        f"{unmatched_percentage:.2f}%"
    )


# ============================================================
# 9. SAMPLE MATCHED URLS
# ============================================================

print("\n" + "-" * 70)
print("SAMPLE MATCHED TARGET URLs")
print("-" * 70)

matched_urls.show(10, truncate=120)


# ============================================================
# 10. SAMPLE UNMATCHED URLS
# ============================================================

print("\n" + "-" * 70)
print("SAMPLE UNMATCHED TARGET URLs")
print("-" * 70)

unmatched_urls.show(10, truncate=120)


print("\n" + "=" * 70)
print("DIAGNOSTIC COMPLETE")
print("=" * 70)


spark.stop()
