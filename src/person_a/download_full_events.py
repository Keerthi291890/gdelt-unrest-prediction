import os
import requests
from datetime import date, timedelta

# ============================================================
# CONFIGURATION
# ============================================================

START_DATE = date(2025, 10, 1)
END_DATE = date(2026, 6, 30)

BASE_URL = "https://data.gdeltproject.org/events/"

OUTPUT_DIR = str(Path(__file__).resolve().parents[2] / "data" / "raw" / "events_full")

os.makedirs(OUTPUT_DIR, exist_ok=True)


# ============================================================
# DOWNLOAD
# ============================================================

current_date = START_DATE

downloaded = []
missing = []
failed = []

total_days = (END_DATE - START_DATE).days + 1

print("=" * 70)
print("GDELT FULL 9-MONTH EVENT DATA COLLECTION")
print("=" * 70)
print(f"Start date : {START_DATE}")
print(f"End date   : {END_DATE}")
print(f"Total days : {total_days}")
print(f"Output     : {OUTPUT_DIR}")
print("=" * 70)


while current_date <= END_DATE:

    date_string = current_date.strftime("%Y%m%d")

    filename = f"{date_string}.export.CSV.zip"

    url = BASE_URL + filename

    output_path = os.path.join(OUTPUT_DIR, filename)

    print(f"\n[{date_string}]")

    # Already downloaded
    if os.path.exists(output_path):
        print("  Already exists -> SKIP")
        downloaded.append(filename)
        current_date += timedelta(days=1)
        continue

    try:

        response = requests.get(
            url,
            timeout=60,
            stream=True
        )

        if response.status_code == 200:

            with open(output_path, "wb") as f:

                for chunk in response.iter_content(
                    chunk_size=1024 * 1024
                ):
                    if chunk:
                        f.write(chunk)

            size_mb = os.path.getsize(output_path) / (1024 * 1024)

            print(f"  Downloaded -> {size_mb:.2f} MB")

            downloaded.append(filename)

        elif response.status_code == 404:

            print("  File not available -> MISSING")

            missing.append(filename)

        else:

            print(
                f"  HTTP error {response.status_code} -> FAILED"
            )

            failed.append(filename)

    except requests.RequestException as e:

        print(f"  Request error -> {e}")

        failed.append(filename)

    current_date += timedelta(days=1)


# ============================================================
# COLLECTION REPORT
# ============================================================

print("\n")
print("=" * 70)
print("COLLECTION SUMMARY")
print("=" * 70)

print(f"Expected days : {total_days}")
print(f"Downloaded    : {len(downloaded)}")
print(f"Missing       : {len(missing)}")
print(f"Failed        : {len(failed)}")


# ============================================================
# SAVE REPORT
# ============================================================

report_path = os.path.join(
    OUTPUT_DIR,
    "collection_report.txt"
)

with open(report_path, "w", encoding="utf-8") as f:

    f.write("GDELT 9-MONTH COLLECTION REPORT\n")
    f.write("=" * 60 + "\n")

    f.write(f"Start date: {START_DATE}\n")
    f.write(f"End date: {END_DATE}\n")
    f.write(f"Expected days: {total_days}\n\n")

    f.write(f"Downloaded: {len(downloaded)}\n")
    f.write(f"Missing: {len(missing)}\n")
    f.write(f"Failed: {len(failed)}\n\n")

    f.write("MISSING FILES\n")
    f.write("-" * 60 + "\n")

    for filename in missing:
        f.write(filename + "\n")

    f.write("\nFAILED FILES\n")
    f.write("-" * 60 + "\n")

    for filename in failed:
        f.write(filename + "\n")

print(f"\nReport saved to:")
print(report_path)
