from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

spark = (
    SparkSession.builder
    .appName("CheckMentionsGKGURLMatch")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

mentions_path = str(Path(__file__).resolve().parents[2] / "data\processed\person_a\mentions"

gkg_path = str(Path(__file__).resolve().parents[2] / "data\processed\person_a\gkg_countries"

print("=" * 70)
print("CHECKING MENTIONS â†” GKG DOCUMENT URL MATCH")
print("=" * 70)

# --------------------------------------------------
# Find Mentions timestamp folders
# --------------------------------------------------
mentions_folders = []

for name in os.listdir(mentions_path):
    full_path = os.path.join(mentions_path, name)

    if os.path.isdir(full_path) and name.endswith(".mentions.CSV"):
        mentions_folders.append(full_path)

mentions_folders.sort()

print(f"\nMentions timestamp folders: {len(mentions_folders)}")

# --------------------------------------------------
# Read Mentions from timestamp folders
# --------------------------------------------------
mentions = spark.read.parquet(*mentions_folders)

mentions_urls = (
    mentions
    .select("MentionIdentifier")
    .filter(col("MentionIdentifier").isNotNull())
    .dropDuplicates()
)

mention_url_count = mentions_urls.count()

print(f"Unique non-null MentionIdentifier URLs: {mention_url_count}")

# --------------------------------------------------
# Find GKG timestamp folders
# --------------------------------------------------
gkg_folders = []

for name in os.listdir(gkg_path):
    full_path = os.path.join(gkg_path, name)

    if os.path.isdir(full_path) and name.isdigit() and len(name) == 14:
        gkg_folders.append(full_path)

gkg_folders.sort()

print(f"GKG timestamp folders: {len(gkg_folders)}")

# --------------------------------------------------
# Read compact GKG
# --------------------------------------------------
gkg = spark.read.parquet(*gkg_folders)

gkg_urls = (
    gkg
    .select("document_identifier")
    .filter(col("document_identifier").isNotNull())
    .dropDuplicates()
)

gkg_url_count = gkg_urls.count()

print(f"Unique non-null GKG document_identifier URLs: {gkg_url_count}")

# --------------------------------------------------
# Exact URL intersection
# --------------------------------------------------
matched = mentions_urls.join(
    gkg_urls,
    mentions_urls.MentionIdentifier == gkg_urls.document_identifier,
    "inner"
)

matched_count = matched.count()

print(f"\nExact URL matches: {matched_count}")

if mention_url_count > 0:
    percentage = (matched_count / mention_url_count) * 100
    print(f"Match percentage: {percentage:.2f}%")

print("\nSample matched URLs:")

matched.select(
    mentions_urls.MentionIdentifier
).show(10, truncate=120)

print("=" * 70)
print("URL MATCH CHECK COMPLETE")
print("=" * 70)

spark.stop()
