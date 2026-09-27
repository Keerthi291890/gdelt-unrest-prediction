from pathlib import Path
import shutil

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    explode,
    split,
    trim,
    collect_set,
    sort_array,
    array,
    array_contains,
    lit,
    when,
    expr,
)


# ============================================================
# 1. PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

GKG_INPUT = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "person_a"
    / "gkg"
)

GKG_OUTPUT = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "person_a"
    / "gkg_countries"
)


# ============================================================
# 2. TARGET COUNTRIES
# ============================================================
# These country codes were verified from the actual GDELT data.

COUNTRY_CODES = {
    "IN": "India",
    "NI": "Nigeria",
    "SF": "South Africa",
    "UP": "Ukraine",
    "US": "United States",
}


# ============================================================
# 3. SAFETY SETTING
# ============================================================
# Stop before disk becomes dangerously full.

MIN_FREE_GB = 10.0


# ============================================================
# 4. COMPACT OUTPUT COLUMNS
# ============================================================

OUTPUT_COLUMNS = [
    "gkg_record_id",
    "date",
    "source_common_name",
    "document_identifier",
    "v1_5_tone",
    "v2_gcam",
    "v2_enhanced_themes",
    "v2_enhanced_locations",
    "v2_1_enhanced_dates",
    "gkg_country_codes",
    "gkg_country_names",
]


# ============================================================
# 5. START SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("GDELT_GKG_Country_Filter")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# 6. CREATE OUTPUT DIRECTORY
# ============================================================

GKG_OUTPUT.mkdir(
    parents=True,
    exist_ok=True
)


# ============================================================
# 7. FIND GKG TIMESTAMP FOLDERS
# ============================================================

input_folders = sorted(
    [
        folder
        for folder in GKG_INPUT.iterdir()
        if (
            folder.is_dir()
            and folder.name.isdigit()
            and len(folder.name) == 14
        )
    ]
)


# ============================================================
# 8. START MESSAGE
# ============================================================

print()
print("=" * 75)
print("GDELT GKG COUNTRY FILTER + COMPACT WRITER")
print("=" * 75)

print(f"Input folders : {len(input_folders)}")
print(f"Input         : {GKG_INPUT}")
print(f"Output        : {GKG_OUTPUT}")
print()

print("Target countries:")
for code, name in COUNTRY_CODES.items():
    print(f"  {code} -> {name}")

print()

print("Compact fields:")
for column in OUTPUT_COLUMNS:
    print(f"  - {column}")

print("=" * 75)
print()


# ============================================================
# 9. COUNTERS
# ============================================================

processed_this_run = 0
skipped_complete = 0
failed = []

stopped_for_low_disk = False


# ============================================================
# 10. PROCESS TIMESTAMP FOLDERS
# ============================================================

try:

    for index, input_folder in enumerate(
        input_folders,
        start=1
    ):

        slot = input_folder.name

        # Final output for this timestamp
        final_output = GKG_OUTPUT / slot

        # Temporary output while Spark is writing
        temp_output = GKG_OUTPUT / f".{slot}.tmp"

        # Marker means this timestamp was completely written
        completion_marker = (
            final_output / "_FILTER_COMPLETE"
        )


        # ====================================================
        # SKIP ALREADY COMPLETED TIMESTAMP
        # ====================================================

        if completion_marker.exists():

            skipped_complete += 1

            continue


        # ====================================================
        # REMOVE INCOMPLETE OLD FINAL OUTPUT
        # ====================================================

        if final_output.exists():

            print(
                f"[{index}/{len(input_folders)}] "
                f"{slot}: incomplete output found -> removing"
            )

            shutil.rmtree(
                final_output,
                ignore_errors=True
            )


        # ====================================================
        # REMOVE STALE TEMPORARY OUTPUT
        # ====================================================

        if temp_output.exists():

            shutil.rmtree(
                temp_output,
                ignore_errors=True
            )


        # ====================================================
        # CHECK FREE DISK SPACE
        # ====================================================

        free_gb = (
            shutil.disk_usage(PROJECT_ROOT).free
            / (1024 ** 3)
        )

        if free_gb < MIN_FREE_GB:

            print()
            print("=" * 75)
            print("STOPPING: LOW DISK SPACE")
            print("=" * 75)

            print(
                f"Free space      : {free_gb:.2f} GB"
            )

            print(
                f"Minimum allowed : {MIN_FREE_GB:.2f} GB"
            )

            print()
            print(
                "All completed timestamp folders are preserved."
            )

            print("=" * 75)

            stopped_for_low_disk = True

            break


        # ====================================================
        # PROGRESS MESSAGE
        # ====================================================

        print(
            f"[{index}/{len(input_folders)}] "
            f"Processing {slot} "
            f"| Free disk: {free_gb:.2f} GB"
        )


        try:

            # =================================================
            # STEP A
            # READ ONE GKG TIMESTAMP
            # =================================================

            df = spark.read.parquet(
                str(input_folder)
            )


            # =================================================
            # STEP B
            # REMOVE MALFORMED RECORDS
            # =================================================

            df = df.filter(
                col("gkg_record_id").isNotNull()
                &
                col("date").isNotNull()
                &
                col("document_identifier").isNotNull()
            )


            # =================================================
            # STEP C
            # EXTRACT COUNTRY CODES
            #
            # GDELT V1 location format:
            #
            # feature_id#type#country#ADM1#ADM2...
            #
            # Therefore country = field index 2.
            # =================================================

            location_rows = (
                df
                .select(
                    "gkg_record_id",

                    explode(
                        split(
                            col("v1_locations"),
                            ";"
                        )
                    ).alias("location")
                )
            )


            location_codes = (
                location_rows
                .select(
                    "gkg_record_id",

                    trim(
                        split(
                            col("location"),
                            "#"
                        ).getItem(2)
                    ).alias("country_code")
                )
                .filter(
                    col("country_code").isin(
                        list(COUNTRY_CODES.keys())
                    )
                )
            )


            # =================================================
            # STEP D
            # COLLECT ALL TARGET COUNTRIES PER RECORD
            # =================================================

            country_per_record = (
                location_codes
                .groupBy("gkg_record_id")
                .agg(
                    sort_array(
                        collect_set(
                            "country_code"
                        )
                    ).alias(
                        "gkg_country_codes"
                    )
                )
            )


            # =================================================
            # STEP E
            # KEEP ONLY RECORDS ABOUT TARGET COUNTRIES
            # =================================================

            filtered = (
                df
                .join(
                    country_per_record,
                    on="gkg_record_id",
                    how="inner"
                )
            )


            # =================================================
            # STEP F
            # CREATE COUNTRY NAME ARRAY
            # =================================================

            country_name_array = array(
                *[
                    when(
                        array_contains(
                            col("gkg_country_codes"),
                            lit(code)
                        ),
                        lit(name)
                    )
                    for code, name in COUNTRY_CODES.items()
                ]
            )


            # =================================================
            # STEP G
            # CONVERT NULL COUNTRY NAMES TO A CLEAN ARRAY
            # =================================================

            filtered = (
                filtered
                .withColumn(
                    "country_name_array",
                    country_name_array
                )
                .withColumn(
                    "gkg_country_names",
                    expr(
                        "filter("
                        "country_name_array, "
                        "x -> x is not null"
                        ")"
                    )
                )
                .drop(
                    "country_name_array"
                )
            )


            # =================================================
            # STEP H
            # SELECT COMPACT PIPELINE FIELDS
            # =================================================

            filtered = filtered.select(
                *OUTPUT_COLUMNS
            )


            # =================================================
            # STEP I
            # WRITE TO TEMPORARY DIRECTORY
            # =================================================

            filtered.write.mode(
                "overwrite"
            ).parquet(
                str(temp_output)
            )


            # =================================================
            # STEP J
            # MARK THIS BATCH AS COMPLETE
            # =================================================

            completion_file = (
                temp_output
                / "_FILTER_COMPLETE"
            )

            completion_file.write_text(
                "GKG country filtering completed successfully.\n",
                encoding="utf-8"
            )


            # =================================================
            # STEP K
            # MOVE TEMP OUTPUT TO FINAL LOCATION
            # =================================================

            temp_output.rename(
                final_output
            )


            processed_this_run += 1

            print(
                f"    DONE: {slot}"
            )


        except KeyboardInterrupt:

            print()
            print("=" * 75)
            print("STOPPED BY USER")
            print("=" * 75)

            print(
                f"Completed this run: "
                f"{processed_this_run}"
            )

            print(
                "Completed timestamp folders are preserved."
            )

            print(
                "The interrupted timestamp will be reprocessed "
                "on the next run."
            )

            print("=" * 75)

            raise


        except Exception as error:

            failed.append(
                (
                    slot,
                    str(error)
                )
            )

            print()
            print(
                f"ERROR: {slot}"
            )

            print(
                str(error)
            )

            print()

            # Remove incomplete temporary data
            if temp_output.exists():

                shutil.rmtree(
                    temp_output,
                    ignore_errors=True
                )

            # Continue with the next timestamp
            continue


finally:

    # ========================================================
    # ALWAYS STOP SPARK
    # ========================================================

    spark.stop()


# ============================================================
# 11. SAVE FAILED TIMESTAMPS
# ============================================================

failed_file = (
    GKG_OUTPUT / "failed_folders.txt"
)


if failed:

    with open(
        failed_file,
        "w",
        encoding="utf-8"
    ) as file:

        for slot, error in failed:

            file.write(
                f"{slot}\t{error}\n"
            )

else:

    if failed_file.exists():

        failed_file.unlink()


# ============================================================
# 12. FINAL SUMMARY
# ============================================================

print()
print()
print("=" * 75)
print("FINAL SUMMARY")
print("=" * 75)

print(
    f"Production folders : {len(input_folders)}"
)

print(
    f"Already complete   : {skipped_complete}"
)

print(
    f"Processed this run : {processed_this_run}"
)

print(
    f"Failed             : {len(failed)}"
)

if stopped_for_low_disk:

    print(
        "Status             : STOPPED - LOW DISK SPACE"
    )

else:

    print(
        "Status             : FINISHED"
    )

print()

print(
    f"Output             : {GKG_OUTPUT}"
)

if failed:

    print()
    print(
        f"Failed list        : {failed_file}"
    )

print("=" * 75)
