
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data" / "PersonA_handoff" / "joined_daily"

spark = (
    SparkSession.builder
    .appName("PersonA_Handoff_Verification")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

expected_columns = {
    "GlobalEventID",
    "MentionIdentifier",
    "MentionTimeDate",
    "ActionGeo_CountryCode",
    "document_identifier",
    "date",
    "EventCode",
    "GoldsteinScale",
    "AvgTone"
}

# Build the expected 91 calendar dates.
from datetime import date, timedelta

expected_dates = set()
d = date(2026, 4, 1)
while d <= date(2026, 6, 30):
    expected_dates.add(d.isoformat())
    d += timedelta(days=1)

# Build the expected 91 calendar dates.
from datetime import date, timedelta

expected_dates = set()
d = date(2026, 4, 1)
while d <= date(2026, 6, 30):
    expected_dates.add(d.isoformat())
    d += timedelta(days=1)

actual_dates = {
    p.name for p in DATA.iterdir()
    if p.is_dir() and p.name[:4].isdigit()
}

print("\nEXPECTED PARTITIONS:", len(expected_dates))
print("ACTUAL PARTITIONS:", len(actual_dates))
print("MISSING:", sorted(expected_dates - actual_dates))
print("UNEXPECTED:", sorted(actual_dates - expected_dates))

# Read every available daily partition.
frames = []
reference_schema = None
schema_issues = []

for day in sorted(actual_dates):
    path = DATA / day

    try:
        df = spark.read.parquet(str(path))

        if reference_schema is None:
            reference_schema = df.schema
        elif df.schema != reference_schema:
            schema_issues.append(day)

        missing_columns = expected_columns - set(df.columns)

        if missing_columns:
            print(day, "MISSING COLUMNS:", sorted(missing_columns))

        count = df.count()
        print(day, "ROWS:", f"{count:,}")

        frames.append(df)

    except Exception as e:
        print(day, "READ ERROR:", str(e)[:300])

print("\nSCHEMA MISMATCH DATES:", schema_issues)

if not frames:
    raise RuntimeError("No readable partitions found.")

# Combine the available partitions.
all_data = frames[0]
for frame in frames[1:]:
    all_data = all_data.unionByName(frame)

print("\nTOTAL ROWS:", f"{all_data.count():,}")

# Null checks on essential columns.
required = [
    "GlobalEventID",
    "MentionIdentifier",
    "MentionTimeDate",
    "ActionGeo_CountryCode"
]

print("\nNULL COUNTS:")
all_data.select([
    F.count(F.when(F.col(c).isNull(), 1)).alias(c)
    for c in required
]).show()

# Country distribution.
print("\nCOUNTRY DISTRIBUTION:")
all_data.groupBy("ActionGeo_CountryCode").count().orderBy(
    F.desc("count")
).show()

# Duplicate checks.
print("\nDUPLICATE CHECKS:")

for key in ["GlobalEventID", "MentionIdentifier"]:
    duplicates = (
        all_data.groupBy(key)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    print(key, "duplicate groups:", duplicates)

# Check partition date against GKG date.
print("\nSAMPLE DATE VALUES:")
all_data.select("date", "MentionTimeDate").show(10, truncate=True)

print("\nVERIFICATION SCRIPT FINISHED")

spark.stop()