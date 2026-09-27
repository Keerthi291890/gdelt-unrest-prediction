import zipfile
import shutil
from pathlib import Path

from pyspark.sql import SparkSession


# ============================================================
# PROJECT PATHS
# ============================================================

PROJECT_ROOT = Path(str(Path(__file__).resolve().parents[2]))

RAW_DIR = PROJECT_ROOT / "data" / "raw" / "mentions"

TEMP_DIR = PROJECT_ROOT / "data" / "temp" / "mentions_batch"

OUTPUT_DIR = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "person_a"
    / "mentions"
)


# ============================================================
# TEST CONTROL
# ============================================================

# FIRST RUN:
# Process only 3 files.
#
# After we verify the result, change this to:
# MAX_FILES = None
#
MAX_FILES = None


# ============================================================
# MENTIONS SCHEMA
# ============================================================

MENTION_COLUMNS = [
    "GlobalEventID",
    "EventTimeDate",
    "MentionTimeDate",
    "MentionType",
    "MentionSourceName",
    "MentionIdentifier",
    "SentenceID",
    "Actor1CharOffset",
    "Actor2CharOffset",
    "ActionCharOffset",
    "InRawText",
    "Confidence",
    "MentionDocLen",
    "MentionDocTone",
    "MentionDocTranslationInfo",
    "Extras",
]


# ============================================================
# PREPARE DIRECTORIES
# ============================================================

TEMP_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# START SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("GDELT_Mentions_Batch")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("=" * 70)
print("GDELT MENTIONS - BATCH PROCESSING")
print("=" * 70)

print("Raw directory   :", RAW_DIR)
print("Output directory:", OUTPUT_DIR)
print("MAX_FILES       :", MAX_FILES)


# ============================================================
# FIND ZIP FILES
# ============================================================

zip_files = sorted(RAW_DIR.glob("*.zip"))

print()
print("ZIP files found:", len(zip_files))

if len(zip_files) == 0:
    raise RuntimeError("No Mentions ZIP files found.")


# Apply test limit
files_to_process = zip_files

if MAX_FILES is not None:
    files_to_process = zip_files[:MAX_FILES]

print("Files selected :", len(files_to_process))


# ============================================================
# COUNTERS
# ============================================================

processed = 0
skipped = 0
failed = 0


# ============================================================
# PROCESS EACH ZIP
# ============================================================

for index, zip_path in enumerate(files_to_process, start=1):

    print()
    print("=" * 70)
    print(f"FILE {index} / {len(files_to_process)}")
    print("=" * 70)

    print("ZIP:", zip_path.name)

    # --------------------------------------------------------
    # Output folder name
    # --------------------------------------------------------

    output_name = zip_path.stem

    file_output_dir = OUTPUT_DIR / output_name

    # --------------------------------------------------------
    # Restart safety
    # --------------------------------------------------------

    if file_output_dir.exists():
        print("SKIPPED: Output already exists.")
        skipped += 1
        continue

    # --------------------------------------------------------
    # Clean temporary directory
    # --------------------------------------------------------

    shutil.rmtree(TEMP_DIR, ignore_errors=True)
    TEMP_DIR.mkdir(parents=True, exist_ok=True)

    try:

        # ----------------------------------------------------
        # Extract ZIP
        # ----------------------------------------------------

        with zipfile.ZipFile(zip_path, "r") as z:

            members = z.namelist()

            if len(members) != 1:
                raise RuntimeError(
                    f"Expected 1 file inside ZIP, found {len(members)}"
                )

            csv_name = members[0]

            z.extract(csv_name, TEMP_DIR)

        csv_path = TEMP_DIR / csv_name

        print("Extracted:", csv_name)

        # ----------------------------------------------------
        # Read with Spark
        # ----------------------------------------------------

        df = (
            spark.read
            .option("sep", "\t")
            .option("header", "false")
            .option("inferSchema", "false")
            .csv(str(csv_path))
        )

        actual_columns = len(df.columns)

        print("Columns:", actual_columns)

        if actual_columns != 16:
            raise RuntimeError(
                f"Expected 16 columns, got {actual_columns}"
            )

        # ----------------------------------------------------
        # Apply schema names
        # ----------------------------------------------------

        df = df.toDF(*MENTION_COLUMNS)

        # ----------------------------------------------------
        # Count rows
        # ----------------------------------------------------

        row_count = df.count()

        print(f"Rows: {row_count:,}")

        # ----------------------------------------------------
        # Write Parquet
        # ----------------------------------------------------

        df.write.mode("overwrite").parquet(str(file_output_dir))

        print("SUCCESS: Parquet written.")

        processed += 1

    except Exception as e:

        print("FAILED:", repr(e))

        failed += 1

    finally:

        # ----------------------------------------------------
        # Always clean temporary extraction
        # ----------------------------------------------------

        shutil.rmtree(TEMP_DIR, ignore_errors=True)


# ============================================================
# FINAL SUMMARY
# ============================================================

spark.stop()


print()
print("=" * 70)
print("MENTIONS BATCH PROCESSING SUMMARY")
print("=" * 70)

print("ZIP files found :", len(zip_files))
print("Files selected  :", len(files_to_process))
print("Processed       :", processed)
print("Skipped         :", skipped)
print("Failed          :", failed)

print("=" * 70)

if failed == 0:
    print("BATCH TEST COMPLETED SUCCESSFULLY.")
else:
    print("BATCH COMPLETED WITH FAILURES.")
