
from pathlib import Path
from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parents[2]

DATA = ROOT / "data" / "PersonA_handoff" / "joined_daily"

spark = (
    SparkSession.builder
    .appName("Check_Adjacent_Dates")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

for day in ["2026-04-04", "2026-04-05", "2026-04-06"]:

    path = DATA / day

    print(f"\nChecking {day}")

    if not path.exists():
        print("PARTITION MISSING")
        continue

    df = spark.read.parquet(str(path))

    print("Rows:", f"{df.count():,}")

    df.groupBy("ActionGeo_CountryCode").count().show()

spark.stop()