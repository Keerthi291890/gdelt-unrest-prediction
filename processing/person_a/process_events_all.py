import os
import zipfile
import shutil
from pathlib import Path

from pyspark.sql import SparkSession


# ============================================================
# PATHS
# ============================================================

PROJECT_ROOT = Path(str(Path(__file__).resolve().parents[2]))

RAW_DIR = PROJECT_ROOT / "data" / "raw" / "events_full"
TEMP_DIR = PROJECT_ROOT / "data" / "temp" / "events_batch"
OUTPUT_DIR = PROJECT_ROOT / "data" / "processed" / "person_a" / "events"

TEMP_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# GDELT EVENTS - 58 COLUMNS
# ============================================================

EVENT_COLUMNS = [
    "GlobalEventID",
    "Day",
    "MonthYear",
    "Year",
    "FractionDate",
    "Actor1Code",
    "Actor1Name",
    "Actor1CountryCode",
    "Actor1KnownGroupCode",
    "Actor1EthnicCode",
    "Actor1Religion1Code",
    "Actor1Religion2Code",
    "Actor1Type1Code",
    "Actor1Type2Code",
    "Actor1Type3Code",
    "Actor2Code",
    "Actor2Name",
    "Actor2CountryCode",
    "Actor2KnownGroupCode",
    "Actor2EthnicCode",
    "Actor2Religion1Code",
    "Actor2Religion2Code",
    "Actor2Type1Code",
    "Actor2Type2Code",
    "Actor2Type3Code",
    "IsRootEvent",
    "EventCode",
    "EventBaseCode",
    "EventRootCode",
    "QuadClass",
    "GoldsteinScale",
    "NumMentions",
    "NumSources",
    "NumArticles",
    "AvgTone",
    "Actor1Geo_Type",
    "Actor1Geo_FullName",
    "Actor1Geo_CountryCode",
    "Actor1Geo_ADM1Code",
    "Actor1Geo_Lat",
    "Actor1Geo_Long",
    "Actor1Geo_FeatureID",
    "Actor2Geo_Type",
    "Actor2Geo_FullName",
    "Actor2Geo_CountryCode",
    "Actor2Geo_ADM1Code",
    "Actor2Geo_Lat",
    "Actor2Geo_Long",
    "Actor2Geo_FeatureID",
    "ActionGeo_Type",
    "ActionGeo_FullName",
    "ActionGeo_CountryCode",
    "ActionGeo_ADM1Code",
    "ActionGeo_Lat",
    "ActionGeo_Long",
    "ActionGeo_FeatureID",
    "DATEADDED",
    "SOURCEURL",
]


# ============================================================
# SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("GDELT_Events_Batch_Processing")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# FIND RAW FILES
# ============================================================

zip_files = sorted(RAW_DIR.glob("*.zip"))

print("=" * 70)
print("GDELT EVENTS - BATCH PROCESSING")
print("=" * 70)
print(f"Raw directory   : {RAW_DIR}")
print(f"Output directory: {OUTPUT_DIR}")
print(f"ZIP files found : {len(zip_files)}")
print("=" * 70)


# ============================================================
# PROCESS EACH ZIP
# ============================================================

processed = 0
skipped = 0
failed = 0

for index, zip_path in enumerate(zip_files, start=1):

    date_name = zip_path.name.replace(".export.CSV.zip", "")

    output_path = OUTPUT_DIR / date_name

    print()
    print("-" * 70)
    print(f"[{index}/{len(zip_files)}] {zip_path.name}")
    print("-" * 70)

    # --------------------------------------------------------
    # RESTART SAFETY
    # --------------------------------------------------------

    if output_path.exists():
        print(f"SKIP: Parquet already exists -> {output_path}")
        skipped += 1
        continue

    # --------------------------------------------------------
    # CLEAN TEMP DIRECTORY
    # --------------------------------------------------------

    if TEMP_DIR.exists():
        shutil.rmtree(TEMP_DIR)

    TEMP_DIR.mkdir(parents=True, exist_ok=True)

    try:

        # ----------------------------------------------------
        # EXTRACT ONE CSV
        # ----------------------------------------------------

        print("Extracting temporary CSV...")

        with zipfile.ZipFile(zip_path, "r") as z:
            members = z.namelist()

            if len(members) != 1:
                raise RuntimeError(
                    f"Expected exactly 1 file inside ZIP, found {len(members)}"
                )

            csv_name = members[0]
            z.extract(csv_name, TEMP_DIR)

        csv_path = TEMP_DIR / csv_name

        print(f"Temporary CSV: {csv_path}")

        # ----------------------------------------------------
        # READ WITH SPARK
        # ----------------------------------------------------

        print("Reading with Spark...")

        df = (
            spark.read
            .option("sep", "\t")
            .option("header", "false")
            .option("inferSchema", "false")
            .csv(str(csv_path))
        )

        actual_columns = len(df.columns)

        print(f"Columns read: {actual_columns}")

        if actual_columns != 58:
            raise RuntimeError(
                f"Expected 58 columns but Spark read {actual_columns}"
            )

        # ----------------------------------------------------
        # APPLY VERIFIED 58-COLUMN NAMES
        # ----------------------------------------------------

        df = df.toDF(*EVENT_COLUMNS)

        # ----------------------------------------------------
        # COUNT
        # ----------------------------------------------------

        row_count = df.count()

        print(f"Rows read: {row_count:,}")

        # ----------------------------------------------------
        # WRITE PARQUET
        # ----------------------------------------------------

        print("Writing Parquet...")

        (
            df.write
            .mode("overwrite")
            .parquet(str(output_path))
        )

        print(f"SUCCESS: {date_name}")
        print(f"Output: {output_path}")

        processed += 1

    except Exception as e:

        print()
        print(f"FAILED: {date_name}")
        print(f"ERROR: {type(e).__name__}: {e}")

        failed += 1

    finally:

        # ----------------------------------------------------
        # DELETE TEMPORARY EXTRACTED CSV
        # ----------------------------------------------------

        if TEMP_DIR.exists():
            shutil.rmtree(TEMP_DIR)

        print("Temporary files cleaned.")


# ============================================================
# FINAL SUMMARY
# ============================================================

print()
print("=" * 70)
print("BATCH PROCESSING SUMMARY")
print("=" * 70)
print(f"ZIP files found : {len(zip_files)}")
print(f"Processed       : {processed}")
print(f"Skipped         : {skipped}")
print(f"Failed          : {failed}")
print("=" * 70)

spark.stop()
