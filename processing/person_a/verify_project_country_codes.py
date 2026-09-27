from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, trim


PROJECT_ROOT = Path(__file__).resolve().parents[2]

GKG_OUTPUT = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "person_a"
    / "gkg"
)

spark = (
    SparkSession.builder
    .appName("VerifyProjectCountryCodes")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Only the real 15-minute production folders.
folders = sorted(
    p for p in GKG_OUTPUT.iterdir()
    if p.is_dir() and len(p.name) == 14 and p.name.isdigit()
)

df = spark.read.parquet(
    *[str(folder) for folder in folders]
)

# Remove malformed rows.
df = df.filter(
    col("gkg_record_id").isNotNull() &
    col("date").isNotNull()
)

# Split multiple V1 location entries.
locations = (
    df
    .filter(
        col("v1_locations").isNotNull() &
        (col("v1_locations") != "")
    )
    .select(
        explode(split(col("v1_locations"), ";")).alias("location")
    )
)

# The third # separated field is the country code.
location_parts = locations.select(
    col("location"),
    split(col("location"), "#").alias("parts")
)

verified = location_parts.select(
    "location",
    trim(col("parts")[1]).alias("location_name"),
    trim(col("parts")[2]).alias("country_code")
)

countries = [
    "United States",
    "India",
    "Nigeria",
    "Ukraine",
    "South Africa",
]

print("=" * 80)
print("PROJECT COUNTRY CODE VERIFICATION")
print("=" * 80)

for country in countries:

    print()
    print("-" * 80)
    print(f"COUNTRY: {country}")
    print("-" * 80)

    result = (
        verified
        .filter(
            col("location_name").contains(country)
        )
        .select(
            "location_name",
            "country_code"
        )
        .distinct()
        .orderBy("country_code")
    )

    result.show(20, truncate=False)

print("=" * 80)

spark.stop()