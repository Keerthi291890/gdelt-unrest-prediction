from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when

PROJECT_ROOT = str(Path(__file__).resolve().parents[2])

EVENTS_ROOT = PROJECT_ROOT + r"\data\processed\person_a\events"

TARGET_COUNTRIES = ["IN", "NI", "SF", "UP", "US"]


spark = (
    SparkSession.builder
    .appName("DiagnoseEventsTargetCountries")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("=" * 70)
print("EVENTS TARGET-COUNTRY DIAGNOSTIC")
print("=" * 70)


# ------------------------------------------------------------
# 1. Find only real historical date folders
# ------------------------------------------------------------

import os
import re

date_folders = []

for name in os.listdir(EVENTS_ROOT):
    full_path = os.path.join(EVENTS_ROOT, name)

    if os.path.isdir(full_path) and re.fullmatch(r"\d{8}", name):
        date_folders.append(full_path)

date_folders.sort()

print(f"Historical date folders : {len(date_folders)}")

if not date_folders:
    raise RuntimeError("No Events date folders found.")


# ------------------------------------------------------------
# 2. Read Events Parquet
# ------------------------------------------------------------

df = spark.read.parquet(*date_folders)

print(f"Total Events rows      : {df.count():,}")


# ------------------------------------------------------------
# 3. Check required column
# ------------------------------------------------------------

if "ActionGeo_CountryCode" not in df.columns:
    raise RuntimeError(
        "ActionGeo_CountryCode not found in Events dataset."
    )

print("ActionGeo_CountryCode  : FOUND")


# ------------------------------------------------------------
# 4. Overall country-code diagnostics
# ------------------------------------------------------------

print("\n" + "-" * 70)
print("TARGET COUNTRY COUNTS")
print("-" * 70)

target_df = (
    df
    .filter(col("ActionGeo_CountryCode").isin(TARGET_COUNTRIES))
    .groupBy("ActionGeo_CountryCode")
    .count()
    .orderBy("ActionGeo_CountryCode")
)

target_df.show(truncate=False)


# ------------------------------------------------------------
# 5. Total target-country Events
# ------------------------------------------------------------

target_count = (
    df
    .filter(col("ActionGeo_CountryCode").isin(TARGET_COUNTRIES))
    .count()
)

total_count = df.count()

print(f"Total Events             : {total_count:,}")
print(f"Target-country Events    : {target_count:,}")

if total_count > 0:
    percentage = target_count / total_count * 100
    print(f"Target-country percentage : {percentage:.2f}%")


# ------------------------------------------------------------
# 6. Null / missing ActionGeo country
# ------------------------------------------------------------

null_count = (
    df
    .filter(
        col("ActionGeo_CountryCode").isNull()
        | (col("ActionGeo_CountryCode") == "")
    )
    .count()
)

print(f"Null/empty country code  : {null_count:,}")


# ------------------------------------------------------------
# 7. Top country codes
# ------------------------------------------------------------

print("\n" + "-" * 70)
print("TOP 20 ACTION GEO COUNTRY CODES")
print("-" * 70)

(
    df
    .groupBy("ActionGeo_CountryCode")
    .count()
    .orderBy(col("count").desc())
    .show(20, truncate=False)
)


print("\n" + "=" * 70)
print("DIAGNOSTIC COMPLETE")
print("=" * 70)

spark.stop()
