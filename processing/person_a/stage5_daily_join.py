import os
from datetime import date, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# ============================================================
# CONFIGURATION
# ============================================================

BASE = str(Path(__file__).resolve().parents[2])

EVENTS = os.path.join(
    BASE, "data", "processed", "person_a", "events"
)

MENTIONS = os.path.join(
    BASE, "data", "processed", "person_a", "mentions"
)

GKG = os.path.join(
    BASE, "data", "processed", "person_a", "gkg_countries"
)

OUTPUT = os.path.join(
    BASE, "data", "processed", "person_a", "joined_daily"
)


# ============================================================
# SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("PersonA_Stage5_DailyJoin")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "16")
    .config("spark.default.parallelism", "16")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.files.maxRecordsPerFile", "100000")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# START
# ============================================================

print("=" * 70)
print("PERSON A - STAGE 5 DAILY JOIN")
print("=" * 70)

os.makedirs(OUTPUT, exist_ok=True)


# ============================================================
# DATE RANGE
# ============================================================

start = date(2026, 4, 1)
end = date(2026, 6, 30)

current = start


# ============================================================
# PROCESS ONE DAY AT A TIME
# ============================================================

while current <= end:

    day = current.strftime("%Y%m%d")
    out_day = current.strftime("%Y-%m-%d")

    print()
    print("=" * 70)
    print("PROCESSING:", out_day)
    print("=" * 70)

    # ========================================================
    # 1. EVENTS
    # ========================================================

    event_path = os.path.join(EVENTS, day)

    if not os.path.isdir(event_path):
        print("WARNING: Events folder missing:", event_path)
        current += timedelta(days=1)
        continue

    event_files = [
        os.path.join(event_path, f)
        for f in os.listdir(event_path)
        if f.endswith(".parquet")
    ]

    event_files.sort()

    print("Event parquet files:", len(event_files))

    if len(event_files) == 0:
        print("WARNING: No Event parquet files.")
        current += timedelta(days=1)
        continue

    events = (
        spark.read.parquet(*event_files)
        .filter(
            col("ActionGeo_CountryCode").isin(
                "US",
                "IN",
                "NI",
                "UP",
                "SF"
            )
        )
        .select(
            "GlobalEventID",
            "Day",
            "EventCode",
            "EventRootCode",
            "QuadClass",
            "GoldsteinScale",
            "NumMentions",
            "NumSources",
            "NumArticles",
            "AvgTone",
            "ActionGeo_CountryCode",
            "ActionGeo_FullName",
            "SOURCEURL"
        )
    )

    # ========================================================
    # 2. MENTIONS - ALL 96 FILES FOR THE DAY
    # ========================================================

    mention_folders = [
        os.path.join(MENTIONS, name)
        for name in os.listdir(MENTIONS)
        if name.startswith(day)
        and name.endswith(".mentions.CSV")
    ]

    mention_folders.sort()

    print("Mentions folders:", len(mention_folders))

    if len(mention_folders) == 0:
        print("WARNING: No Mentions folders.")
        current += timedelta(days=1)
        continue

    mentions = (
        spark.read.parquet(*mention_folders)
        .select(
            "GlobalEventID",
            "MentionIdentifier",
            "MentionTimeDate",
            "MentionSourceName",
            "Confidence",
            "MentionDocLen",
            "MentionDocTone"
        )
    )

    # ========================================================
    # 3. GKG - ALL TIMESTAMP FOLDERS FOR THE DAY
    # ========================================================

    gkg_folders = [
        os.path.join(GKG, name)
        for name in os.listdir(GKG)
        if name.startswith(day)
        and os.path.isdir(os.path.join(GKG, name))
    ]

    gkg_folders.sort()

    print("GKG folders:", len(gkg_folders))

    if len(gkg_folders) == 0:
        print("WARNING: No GKG folders.")
        current += timedelta(days=1)
        continue

    gkg = (
        spark.read.parquet(*gkg_folders)
        .select(
            "document_identifier",
            "date",
            "source_common_name",
            "v1_5_tone",
            "v2_gcam",
            "gkg_country_codes",
            "gkg_country_names"
        )
    )

    # ========================================================
    # 4. EVENTS -> MENTIONS
    # ========================================================

    print("Joining Events -> Mentions...")

    event_mentions = events.join(
        mentions,
        on="GlobalEventID",
        how="inner"
    )

    # ========================================================
    # 5. MENTIONS -> GKG
    # ========================================================

    print("Joining Mentions -> GKG...")

    joined = event_mentions.join(
        gkg,
        event_mentions.MentionIdentifier
        == gkg.document_identifier,
        how="left"
    )

    # ========================================================
    # 6. WRITE DAILY OUTPUT
    # ========================================================

    output_path = os.path.join(
        OUTPUT,
        out_day
    )

    print("Writing:", output_path)

    (
        joined
        .write
        .mode("overwrite")
        .parquet(output_path)
    )

    # ========================================================
    # 7. VALIDATE WRITTEN OUTPUT
    # ========================================================

    result = spark.read.parquet(output_path)

    result_count = result.count()

    print("Rows written:", result_count)
    print("Output validated:", output_path)

    # ========================================================
    # NEXT DAY
    # ========================================================

    current += timedelta(days=1)


# ============================================================
# FINISH
# ============================================================

print()
print("=" * 70)
print("STAGE 5 COMPLETE")
print("=" * 70)

spark.stop()
