from pathlib import Path
from pyspark.sql import SparkSession

PROJECT_ROOT = Path(str(Path(__file__).resolve().parents[2]))
EVENTS_DIR = PROJECT_ROOT / "data" / "processed" / "person_a" / "events"

spark = (
    SparkSession.builder
    .appName("Validate_GDELT_Events_Parquet")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

parquet_dirs = sorted(EVENTS_DIR.iterdir())

print("=" * 70)
print("GDELT EVENTS PARQUET VALIDATION")
print("=" * 70)
print(f"Parquet directories found: {len(parquet_dirs)}")

# Read all 91 Parquet outputs together
df = spark.read.parquet(str(EVENTS_DIR / "*"))

print(f"Columns: {len(df.columns)}")
print(f"Rows   : {df.count():,}")

print()
print("Schema check:")

if len(df.columns) == 58:
    print("SUCCESS: 58 columns found.")
else:
    print(f"ERROR: Expected 58 columns, found {len(df.columns)}")

print()
print("First 5 GlobalEventID values:")
df.select("GlobalEventID").show(5, truncate=False)

print()
print("Date output directories:")
print(parquet_dirs[0].name, "to", parquet_dirs[-1].name)

print("=" * 70)

spark.stop()
