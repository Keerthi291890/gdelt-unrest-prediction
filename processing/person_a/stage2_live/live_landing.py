from pathlib import Path
import shutil
import zipfile
import tempfile
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract

BASE_DIR = Path(__file__).resolve().parents[3]

VERIFIED_DIR = BASE_DIR / "data" / "raw" / "incoming" / "verified"
OUTPUT_DIR = BASE_DIR / "data" / "processed" / "person_a" / "live_joined"

TARGET_COUNTRIES = ["US", "IN", "NI", "UP", "SF"]


def free_space_gb(path):
    return shutil.disk_usage(path).free / (1024 ** 3)


def extract_zip(zip_path, temp_dir):
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(temp_dir)


def find_file(temp_dir, suffix):
    files = list(Path(temp_dir).glob(f"*{suffix}"))
    if not files:
        raise FileNotFoundError(f"No file found with suffix: {suffix}")
    return str(files[0])


print("=" * 70)
print("STAGE 2 LIVE -> COMPACT PARQUET LANDING")
print("=" * 70)

free_gb = free_space_gb(BASE_DIR)
print(f"Free disk space: {free_gb:.2f} GB")

if free_gb < 30:
    raise RuntimeError(
        f"STOP: only {free_gb:.2f} GB free. "
        "Need at least 30 GB before processing."
    )

zip_files = list(VERIFIED_DIR.glob("*.zip"))

if len(zip_files) != 3:
    raise RuntimeError(
        f"Expected exactly 3 verified ZIP files, found {len(zip_files)}"
    )

events_zip = next(x for x in zip_files if ".export.CSV.zip" in x.name)
mentions_zip = next(x for x in zip_files if ".mentions.CSV.zip" in x.name)
gkg_zip = next(x for x in zip_files if ".gkg.csv.zip" in x.name)

batch_timestamp = events_zip.name[:14]

print(f"Batch: {batch_timestamp}")
print(f"Events:   {events_zip.name}")
print(f"Mentions: {mentions_zip.name}")
print(f"GKG:      {gkg_zip.name}")


spark = (
    SparkSession.builder
    .appName("GDELT_Stage2_Live_Landing")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

temp_root = Path(tempfile.mkdtemp(prefix="gdelt_live_", dir=str(BASE_DIR / "data" / "temp")))

try:
    events_dir = temp_root / "events"
    mentions_dir = temp_root / "mentions"
    gkg_dir = temp_root / "gkg"

    events_dir.mkdir()
    mentions_dir.mkdir()
    gkg_dir.mkdir()

    extract_zip(events_zip, events_dir)
    extract_zip(mentions_zip, mentions_dir)
    extract_zip(gkg_zip, gkg_dir)

    # ------------------------------------------------------------
    # EVENTS
    # ------------------------------------------------------------

    events_file = find_file(events_dir, ".export.CSV")

    events_raw = (
        spark.read
        .option("sep", "\t")
        .option("header", "false")
        .option("quote", "")
        .option("escape", "")
        .csv(events_file)
    )

    event_columns = [
        "GlobalEventID", "Day", "MonthYear", "Year", "FractionDate",
        "Actor1Code", "Actor1Name", "Actor1CountryCode",
        "Actor1KnownGroupCode", "Actor1EthnicCode",
        "Actor1Religion1Code", "Actor1Religion2Code",
        "Actor1Type1Code", "Actor1Type2Code", "Actor1Type3Code",
        "Actor2Code", "Actor2Name", "Actor2CountryCode",
        "Actor2KnownGroupCode", "Actor2EthnicCode",
        "Actor2Religion1Code", "Actor2Religion2Code",
        "Actor2Type1Code", "Actor2Type2Code", "Actor2Type3Code",
        "IsRootEvent", "EventCode", "EventBaseCode", "EventRootCode",
        "QuadClass", "GoldsteinScale", "NumMentions", "NumSources",
        "NumArticles", "AvgTone",
        "Actor1Geo_Type", "Actor1Geo_FullName",
        "Actor1Geo_CountryCode", "Actor1Geo_ADM1Code",
        "Actor1Geo_ADM2Code", "Actor1Geo_Lat", "Actor1Geo_Long",
        "Actor1Geo_FeatureID",
        "Actor2Geo_Type", "Actor2Geo_FullName",
        "Actor2Geo_CountryCode", "Actor2Geo_ADM1Code",
        "Actor2Geo_ADM2Code", "Actor2Geo_Lat", "Actor2Geo_Long",
        "Actor2Geo_FeatureID",
        "ActionGeo_Type", "ActionGeo_FullName",
        "ActionGeo_CountryCode", "ActionGeo_ADM1Code",
        "ActionGeo_ADM2Code", "ActionGeo_Lat", "ActionGeo_Long",
        "ActionGeo_FeatureID",
        "DATEADDED", "SOURCEURL"
    ]

    events = events_raw.toDF(*event_columns)

    events = (
        events
        .filter(col("ActionGeo_CountryCode").isin(TARGET_COUNTRIES))
        .select(
            "GlobalEventID",
            "Day",
            "EventCode",
            "EventRootCode",
            "QuadClass",
            "GoldsteinScale",
            "NumMentions",
            "NumSources",
            "NumArticles",
            "AvgTone",
            "ActionGeo_CountryCode",
            "ActionGeo_FullName",
            "SOURCEURL"
        )
    )

    print(f"Target-country Events: {events.count()}")

    # ------------------------------------------------------------
    # MENTIONS
    # ------------------------------------------------------------

    mentions_file = find_file(mentions_dir, ".mentions.CSV")

    mentions_raw = (
        spark.read
        .option("sep", "\t")
        .option("header", "false")
        .option("quote", "")
        .option("escape", "")
        .csv(mentions_file)
    )

    mention_columns = [
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
        "Extras"
    ]

    mentions = mentions_raw.toDF(*mention_columns)

    mentions = mentions.select(
        "GlobalEventID",
        "MentionTimeDate",
        "MentionType",
        "MentionSourceName",
        "MentionIdentifier",
        "Confidence",
        "MentionDocLen",
        "MentionDocTone"
    )

    # ------------------------------------------------------------
    # GKG
    # ------------------------------------------------------------

    gkg_file = find_file(gkg_dir, ".gkg.csv")

    gkg_raw = (
        spark.read
        .option("sep", "\t")
        .option("header", "false")
        .option("quote", "")
        .option("escape", "")
        .csv(gkg_file)
    )

    gkg_columns = [
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
        "v2_1_extras"
    ]

    gkg = gkg_raw.toDF(*gkg_columns)

    gkg = gkg.select(
        "gkg_record_id",
        "date",
        "source_common_name",
        "document_identifier",
        "v1_5_tone",
        "v2_gcam",
        "v2_enhanced_themes",
        "v2_enhanced_locations",
        "v2_1_enhanced_dates"
    )

    # ------------------------------------------------------------
    # EVENTS -> MENTIONS
    # ------------------------------------------------------------

    event_mentions = events.join(
        mentions,
        on="GlobalEventID",
        how="inner"
    )

    # ------------------------------------------------------------
    # MENTIONS -> GKG
    # ------------------------------------------------------------

    joined = event_mentions.join(
        gkg,
        event_mentions.MentionIdentifier == gkg.document_identifier,
        how="left"
    )

    # GCAM word count
    joined = joined.withColumn(
        "gcam_word_count",
        regexp_extract(
            col("v2_gcam"),
            r"wc:([0-9]+)",
            1
        ).cast("long")
    )

    final_df = joined.select(
        "GlobalEventID",
        "Day",
        "ActionGeo_CountryCode",
        "ActionGeo_FullName",
        "EventCode",
        "EventRootCode",
        "QuadClass",
        "GoldsteinScale",
        "NumMentions",
        "NumSources",
        "NumArticles",
        "AvgTone",
        "MentionTimeDate",
        "MentionType",
        "MentionSourceName",
        "MentionIdentifier",
        "Confidence",
        "MentionDocLen",
        "MentionDocTone",
        "gkg_record_id",
        "date",
        "source_common_name",
        "document_identifier",
        "v1_5_tone",
        "v2_gcam",
        "gcam_word_count",
        "v2_enhanced_themes",
        "v2_enhanced_locations",
        "v2_1_enhanced_dates",
        "SOURCEURL"
    )

    row_count = final_df.count()

    print(f"Final live joined rows: {row_count}")

    # ------------------------------------------------------------
    # WRITE COMPACT PARQUET
    # ------------------------------------------------------------

    output_date = batch_timestamp[:8]

    year = output_date[:4]
    month = output_date[4:6]
    day = output_date[6:8]

    output_path = (
        OUTPUT_DIR
        / f"{year}-{month}-{day}"
        / f"batch={batch_timestamp}"
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)

    (
        final_df
        .coalesce(2)
        .write
        .mode("overwrite")
        .parquet(str(output_path))
    )

    print(f"Parquet written to:")
    print(output_path)

    # ------------------------------------------------------------
    # VALIDATE
    # ------------------------------------------------------------

    check = spark.read.parquet(str(output_path))
    check_count = check.count()

    if check_count != row_count:
        raise RuntimeError(
            f"Validation failed: wrote {check_count}, expected {row_count}"
        )

    print(f"Output validation: PASS ({check_count} rows)")

finally:
    spark.stop()

    # Remove extracted temporary files
    shutil.rmtree(temp_root, ignore_errors=True)

    # Remove raw verified ZIPs ONLY after successful Parquet creation
    for f in [events_zip, mentions_zip, gkg_zip]:
        if f.exists():
            f.unlink()

    print("Temporary extracted files removed.")
    print("Processed raw batch removed.")
    print("=" * 70)
    print("LIVE LANDING COMPLETE")
    print("=" * 70)
