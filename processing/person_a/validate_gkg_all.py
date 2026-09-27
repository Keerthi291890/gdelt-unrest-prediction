from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, max


PROJECT_ROOT = Path(__file__).resolve().parents[2]

GKG_OUTPUT = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "person_a"
    / "gkg"
)


EXPECTED_COLUMNS = [
    "gkg_record_id",
    "date",
    "source_collection_identifier",
    "source_common_name",
    "document_identifier",
    "v1_counts",
    "v2_1_counts",
    "v1_themes",
    "v2_enhanced_themes",
    "v1_locations",
    "v2_enhanced_locations",
    "v1_persons",
    "v2_enhanced_persons",
    "v1_organizations",
    "v2_enhanced_organizations",
    "v1_5_tone",
    "v2_1_enhanced_dates",
    "v2_gcam",
    "v2_1_sharing_image",
    "v2_1_related_images",
    "v2_1_social_image_embeds",
    "v2_1_social_video_embeds",
    "v2_1_quotations",
    "v2_1_all_names",
    "v2_1_amounts",
    "v2_1_translation_info",
    "v2_1_extras",
]


spark = (
    SparkSession.builder
    .appName("ValidateAllGKG")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("=" * 70)
print("FULL GKG PARQUET VALIDATION")
print("=" * 70)

folders = sorted(
    p for p in GKG_OUTPUT.iterdir()
    if p.is_dir() and p.name.isdigit()
)

print(f"Output folders found : {len(folders)}")

if not folders:
    raise RuntimeError("No GKG output folders found.")

# Read all processed GKG Parquet files
df = spark.read.parquet(
    *[str(folder) for folder in folders]
)

print()
print(f"Total columns : {len(df.columns)}")
print(f"Total rows    : {df.count()}")

print()
print("Checking columns...")

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

print()
print("Date range:")

df.select(
    min(col("date")).alias("min_date"),
    max(col("date")).alias("max_date")
).show(truncate=False)

print()
print("Null checks:")

for column_name in [
    "gkg_record_id",
    "date",
    "document_identifier",
]:
    null_count = df.filter(
        col(column_name).isNull()
    ).count()

    print(f"{column_name}: {null_count} null rows")

print()
print("Sample important fields:")

df.select(
    "gkg_record_id",
    "date",
    "source_common_name",
    "document_identifier",
    "v2_enhanced_locations",
    "v2_gcam",
).show(5, truncate=120)

print()
print("=" * 70)
print("FULL GKG VALIDATION COMPLETE")
print("=" * 70)

spark.stop()