from pathlib import Path
from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data" / "PersonA_handoff" / "joined_daily"

spark = (
    SparkSession.builder
    .appName("Inspect_PersonA_Schema")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

day = DATA / "2026-04-01"

df = spark.read.parquet(str(day))

print("\n===== PERSON A HANDOFF SCHEMA =====\n")
df.printSchema()

print("\n===== COLUMNS =====\n")
print(df.columns)

print("\n===== SAMPLE =====\n")
df.show(5, truncate=60)

spark.stop()