from pathlib import Path
import zipfile
import shutil

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim


# ============================================================
# PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

INPUT_DIR = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "gkg"
)

OUTPUT_DIR = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "person_a"
    / "gkg"
)

TEMP_DIR = (
    PROJECT_ROOT
    / "data"
    / "temp"
    / "gkg_processing"
)


# ============================================================
# CONTROL
# ============================================================
# None = process all available files
#
# For the FIRST test, keep this as 3.
# After successful validation, change to None.

MAX_FILES = None


# ============================================================
# GKG COLUMN NAMES
# ============================================================

COLUMN_NAMES = [
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


# ============================================================
# START
# ============================================================

print("=" * 70)
print("GDELT GKG FULL PROCESSING")
print("=" * 70)

print("Input directory :", INPUT_DIR)
print("Output directory:", OUTPUT_DIR)
print("MAX_FILES       :", MAX_FILES)


# ============================================================
# PREPARE DIRECTORIES
# ============================================================

INPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
TEMP_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# FIND INPUT ZIP FILES
# ============================================================

zip_files = sorted(
    INPUT_DIR.glob("*.gkg.csv.zip")
)

print("\nZIP files found:", len(zip_files))

if not zip_files:
    raise FileNotFoundError(
        f"No GKG ZIP files found in {INPUT_DIR}"
    )


# ============================================================
# LIMIT FOR TESTING
# ============================================================

if MAX_FILES is None:
    selected_files = zip_files
else:
    selected_files = zip_files[:MAX_FILES]

print("Files selected :", len(selected_files))


# ============================================================
# START SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("PersonA_GKG_Full_Processing")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# COUNTERS
# ============================================================

processed = 0
skipped = 0
failed = 0


# ============================================================
# PROCESS EACH ZIP
# ============================================================

for index, zip_path in enumerate(selected_files, start=1):

    print("\n" + "=" * 70)
    print(f"[{index}/{len(selected_files)}]")
    print("Processing:", zip_path.name)
    print("=" * 70)

    # --------------------------------------------------------
    # Determine output path
    # --------------------------------------------------------
    slot_name = zip_path.name.replace(".gkg.csv.zip", "")
    output_path = OUTPUT_DIR / slot_name

    # --------------------------------------------------------
    # Restart safety
    # --------------------------------------------------------

    if output_path.exists():

        print("SKIPPED: output already exists")

        skipped += 1

        continue

    # --------------------------------------------------------
    # Temporary directory for this ZIP
    # --------------------------------------------------------

    file_temp_dir = (
        TEMP_DIR
        / zip_path.stem
    )

    if file_temp_dir.exists():
        shutil.rmtree(file_temp_dir)

    file_temp_dir.mkdir(
        parents=True,
        exist_ok=True
    )

    try:

        # ====================================================
        # EXTRACT
        # ====================================================

        with zipfile.ZipFile(zip_path, "r") as z:

            csv_files = [
                name
                for name in z.namelist()
                if name.endswith(".gkg.csv")
            ]

            if not csv_files:
                raise FileNotFoundError(
                    "No .gkg.csv found inside ZIP"
                )

            csv_name = csv_files[0]

            extracted_file = (
                file_temp_dir
                / Path(csv_name).name
            )

            with z.open(csv_name) as source, open(
                extracted_file,
                "wb"
            ) as target:

                shutil.copyfileobj(
                    source,
                    target
                )

        print("Extracted:", extracted_file.name)

        # ====================================================
        # READ WITH SPARK
        # ====================================================

        df = (
            spark.read
            .option("sep", "\t")
            .option("header", "false")
            .option("inferSchema", "false")
            .csv(str(extracted_file))
        )

        # ====================================================
        # SCHEMA CHECK
        # ====================================================

        column_count = len(df.columns)

        if column_count != 27:

            raise ValueError(
                f"Expected 27 columns, "
                f"found {column_count}"
            )

        # ====================================================
        # RENAME COLUMNS
        # ====================================================

        df = df.toDF(*COLUMN_NAMES)

        # ====================================================
        # BASIC CLEANING
        # ====================================================

        df = (
            df
            .withColumn(
                "gkg_record_id",
                trim(col("gkg_record_id"))
            )
            .withColumn(
                "document_identifier",
                trim(col("document_identifier"))
            )
            .withColumn(
                "source_common_name",
                trim(col("source_common_name"))
            )
        )

        # ====================================================
        # ROW COUNT
        # ====================================================

        row_count = df.count()

        print("Rows:", row_count)

        # ====================================================
        # WRITE PARQUET
        # ====================================================

        (
            df.write
            .mode("overwrite")
            .parquet(str(output_path))
        )

        print(
            "Parquet written:",
            output_path
        )

        processed += 1

    except Exception as e:

        failed += 1

        print(
            "FAILED:",
            zip_path.name
        )

        print(
            "ERROR:",
            repr(e)
        )

    finally:

        # ====================================================
        # DELETE TEMPORARY EXTRACTED CSV
        # ====================================================

        if file_temp_dir.exists():

            shutil.rmtree(
                file_temp_dir,
                ignore_errors=True
            )

        print("Temporary files cleaned.")


# ============================================================
# STOP SPARK
# ============================================================

spark.stop()


# ============================================================
# FINAL SUMMARY
# ============================================================

print("\n" + "=" * 70)
print("GKG PROCESSING SUMMARY")
print("=" * 70)

print("ZIP files found :", len(zip_files))
print("Files selected  :", len(selected_files))
print("Processed       :", processed)
print("Skipped         :", skipped)
print("Failed          :", failed)

print("=" * 70)