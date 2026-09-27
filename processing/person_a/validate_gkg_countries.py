from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    explode,
    min,
    max,
    count,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]

GKG_COUNTRIES_OUTPUT = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "person_a"
    / "gkg_countries"
)


EXPECTED_FOLDER_COUNT = 8735
EXPECTED_ROW_COUNT = 4767103

EXPECTED_COUNTRY_COUNTS = {
    "IN": 705198,
    "NI": 138072,
    "SF": 119965,
    "UP": 169087,
    "US": 3942908,
}

EXPECTED_COLUMNS = [
    "gkg_record_id",
    "date",
    "source_common_name",
    "document_identifier",
    "v1_5_tone",
    "v2_gcam",
    "v2_enhanced_themes",
    "v2_enhanced_locations",
    "v2_1_enhanced_dates",
    "gkg_country_codes",
    "gkg_country_names",
]


spark = (
    SparkSession.builder
    .appName("ValidateGKGCountries")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("=" * 75)
print("COMPACT GKG COUNTRY VALIDATION")
print("=" * 75)


# ---------------------------------------------------------------------
# 1. CHECK FOLDERS
# ---------------------------------------------------------------------

folders = sorted(
    p for p in GKG_COUNTRIES_OUTPUT.iterdir()
    if p.is_dir() and p.name.isdigit()
)

print()
print(f"Timestamp folders found : {len(folders)}")
print(f"Expected                : {EXPECTED_FOLDER_COUNT}")

if len(folders) == EXPECTED_FOLDER_COUNT:
    print("Folder count : PASS")
else:
    print("Folder count : FAIL")


# ---------------------------------------------------------------------
# 2. CHECK COMPLETION MARKERS
# ---------------------------------------------------------------------

complete_markers = sum(
    (folder / "_FILTER_COMPLETE").exists()
    for folder in folders
)

print()
print(f"Completion markers found : {complete_markers}")
print(f"Expected                 : {EXPECTED_FOLDER_COUNT}")

if complete_markers == EXPECTED_FOLDER_COUNT:
    print("Completion markers : PASS")
else:
    print("Completion markers : FAIL")


# ---------------------------------------------------------------------
# 3. READ ALL COMPACT GKG
# ---------------------------------------------------------------------

print()
print("Reading compact GKG Parquet...")

df = spark.read.parquet(
    *[str(folder) for folder in folders]
)

print("Read complete.")


# ---------------------------------------------------------------------
# 4. COLUMN CHECK
# ---------------------------------------------------------------------

print()
print("Column check...")

print(f"Actual columns : {len(df.columns)}")
print(f"Expected       : {len(EXPECTED_COLUMNS)}")

if df.columns == EXPECTED_COLUMNS:
    print("Column check : PASS")
else:
    print("Column check : FAIL")

    print()
    print("Expected:")
    for i, name in enumerate(EXPECTED_COLUMNS):
        print(f"{i:2d} -> {name}")

    print()
    print("Actual:")
    for i, name in enumerate(df.columns):
        print(f"{i:2d} -> {name}")


# ---------------------------------------------------------------------
# 5. TOTAL ROW COUNT
# ---------------------------------------------------------------------

print()
print("Counting rows...")

total_rows = df.count()

print(f"Actual rows   : {total_rows}")
print(f"Expected rows : {EXPECTED_ROW_COUNT}")

if total_rows == EXPECTED_ROW_COUNT:
    print("Row count : PASS")
else:
    print("Row count : FAIL")


# ---------------------------------------------------------------------
# 6. DATE RANGE
# ---------------------------------------------------------------------

print()
print("Date range:")

df.select(
    min(col("date")).alias("min_date"),
    max(col("date")).alias("max_date"),
).show(truncate=False)


# ---------------------------------------------------------------------
# 7. NULL CHECKS
# ---------------------------------------------------------------------

print()
print("Critical null checks:")

for column_name in [
    "gkg_record_id",
    "date",
    "document_identifier",
    "gkg_country_codes",
]:
    null_count = df.filter(
        col(column_name).isNull()
    ).count()

    print(f"{column_name}: {null_count} null rows")


# ---------------------------------------------------------------------
# 8. COUNTRY COUNTS
# ---------------------------------------------------------------------

print()
print("Country counts:")

country_counts_df = (
    df
    .select(explode(col("gkg_country_codes")).alias("country_code"))
    .groupBy("country_code")
    .count()
    .orderBy("country_code")
)

country_counts = {
    row["country_code"]: row["count"]
    for row in country_counts_df.collect()
}

for country_code, expected_count in EXPECTED_COUNTRY_COUNTS.items():

    actual_count = country_counts.get(country_code, 0)

    print(
        f"{country_code}: "
        f"actual={actual_count}, "
        f"expected={expected_count}"
    )

    if actual_count == expected_count:
        print(f"  {country_code} : PASS")
    else:
        print(f"  {country_code} : FAIL")


# ---------------------------------------------------------------------
# 9. SAMPLE RECORDS
# ---------------------------------------------------------------------

print()
print("Sample compact records:")

df.select(
    "gkg_record_id",
    "date",
    "source_common_name",
    "document_identifier",
    "gkg_country_codes",
    "gkg_country_names",
    "v1_5_tone",
    "v2_gcam",
).show(
    5,
    truncate=120
)


# ---------------------------------------------------------------------
# 10. FINAL SUMMARY
# ---------------------------------------------------------------------

folder_pass = len(folders) == EXPECTED_FOLDER_COUNT
marker_pass = complete_markers == EXPECTED_FOLDER_COUNT
column_pass = df.columns == EXPECTED_COLUMNS
row_pass = total_rows == EXPECTED_ROW_COUNT

country_pass = all(
    country_counts.get(country, 0) == expected
    for country, expected in EXPECTED_COUNTRY_COUNTS.items()
)


print()
print("=" * 75)
print("VALIDATION SUMMARY")
print("=" * 75)

print(f"Folders            : {'PASS' if folder_pass else 'FAIL'}")
print(f"Completion markers : {'PASS' if marker_pass else 'FAIL'}")
print(f"Columns            : {'PASS' if column_pass else 'FAIL'}")
print(f"Rows               : {'PASS' if row_pass else 'FAIL'}")
print(f"Country counts     : {'PASS' if country_pass else 'FAIL'}")

all_pass = (
    folder_pass
    and marker_pass
    and column_pass
    and row_pass
    and country_pass
)

print()

if all_pass:
    print("OVERALL VALIDATION : PASS")
    print()
    print("Compact GKG country dataset is ready.")
    print("The full GKG has NOT been deleted.")
else:
    print("OVERALL VALIDATION : FAIL")
    print()
    print("DO NOT DELETE THE FULL GKG.")


print("=" * 75)

spark.stop()