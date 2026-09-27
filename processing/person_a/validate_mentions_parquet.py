from pathlib import Path

from pyspark.sql import SparkSession


PROJECT_ROOT = Path(str(Path(__file__).resolve().parents[2]))

INPUT_DIR = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "person_a"
    / "mentions"
)


spark = (
    SparkSession.builder
    .appName("Validate_GDELT_Mentions_Parquet")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("=" * 70)
print("VALIDATING GDELT MENTIONS PARQUET")
print("=" * 70)

print("Input:", INPUT_DIR)


# ------------------------------------------------------------
# Find processed outputs
# ------------------------------------------------------------

outputs = sorted(
    p for p in INPUT_DIR.iterdir()
    if p.is_dir()
)

print()
print("Parquet output folders found:", len(outputs))


if len(outputs) != 8735:
    print()
    print("WARNING:")
    print("Expected 8735 available outputs.")
    print("Found:", len(outputs))


# ------------------------------------------------------------
# Read all Parquet outputs
# ------------------------------------------------------------

df = spark.read.parquet(str(INPUT_DIR / "*"))


# ------------------------------------------------------------
# Schema check
# ------------------------------------------------------------

print()
print("COLUMN COUNT")
print("=" * 70)

print("Columns:", len(df.columns))

print()
print("COLUMN NAMES")
print("=" * 70)

for i, column in enumerate(df.columns):
    print(f"{i:2d} -> {column}")


if len(df.columns) != 16:
    raise RuntimeError(
        f"Expected 16 columns, found {len(df.columns)}"
    )


# ------------------------------------------------------------
# Row count
# ------------------------------------------------------------

print()
print("TOTAL ROW COUNT")
print("=" * 70)

row_count = df.count()

print(f"Rows: {row_count:,}")


# ------------------------------------------------------------
# Check GlobalEventID nulls
# ------------------------------------------------------------

print()
print("GLOBAL EVENT ID CHECK")
print("=" * 70)

null_ids = df.filter(
    df.GlobalEventID.isNull()
).count()

print("Null GlobalEventID rows:", null_ids)


# ------------------------------------------------------------
# Date range
# ------------------------------------------------------------

print()
print("MENTION TIME RANGE")
print("=" * 70)

df.select(
    "EventTimeDate",
    "MentionTimeDate"
).describe().show()


# ------------------------------------------------------------
# Sample
# ------------------------------------------------------------

print()
print("SAMPLE ROWS")
print("=" * 70)

df.show(5, truncate=False)


# ------------------------------------------------------------
# Finish
# ------------------------------------------------------------

spark.stop()

print()
print("=" * 70)
print("MENTIONS PARQUET VALIDATION COMPLETE")
print("=" * 70)
