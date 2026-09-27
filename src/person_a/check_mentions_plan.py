from pathlib import Path
from datetime import datetime
import re

START_DATE = datetime(2025, 10, 1)
END_DATE = datetime(2026, 6, 30, 23, 59, 59)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
MASTER_FILE = PROJECT_ROOT / "data" / "raw" / "masterfilelist.txt"

print("=" * 70)
print("GDELT HISTORICAL MENTIONS DRY RUN")
print("=" * 70)

print("\nHistorical window:")
print(f"START: {START_DATE}")
print(f"END  : {END_DATE}")

print("\nMaster file:")
print(MASTER_FILE)

if not MASTER_FILE.exists():
    print("\nERROR: Master file does not exist.")
    raise SystemExit(1)

# Actual GDELT format:
# http://data.gdeltproject.org/gdeltv2/20260101000000.mentions.CSV.zip
pattern = re.compile(
    r"https?://[^\s]+/(\d{14})\.mentions\.CSV\.zip",
    re.IGNORECASE
)

mentions = []

with MASTER_FILE.open("r", encoding="utf-8", errors="ignore") as f:
    for line in f:
        match = pattern.search(line)

        if not match:
            continue

        timestamp = match.group(1)

        try:
            dt = datetime.strptime(timestamp, "%Y%m%d%H%M%S")
        except ValueError:
            continue

        if START_DATE <= dt <= END_DATE:
            url = match.group(0)
            mentions.append((dt, url))

print("\n" + "=" * 70)
print("MENTIONS FILE COUNT")
print("=" * 70)

print(f"Files found: {len(mentions)}")

if not mentions:
    print("\nERROR: No Mentions files found.")
    raise SystemExit(1)

mentions.sort()

print("\nFirst 5 files:")
for dt, url in mentions[:5]:
    print(dt, "->", url)

print("\nLast 5 files:")
for dt, url in mentions[-5:]:
    print(dt, "->", url)

print("\n" + "=" * 70)
print("DATE COVERAGE")
print("=" * 70)

print("First timestamp:", mentions[0][0])
print("Last timestamp :", mentions[-1][0])

expected_slots = int(
    (END_DATE - START_DATE).total_seconds() // (15 * 60)
) + 1

print("\nExpected 15-minute slots:", expected_slots)
print("Mentions files found     :", len(mentions))
print("Missing slots            :", expected_slots - len(mentions))

print("\n" + "=" * 70)
print("DRY RUN COMPLETE")
print("=" * 70)
print("NO FILES WERE DOWNLOADED.")