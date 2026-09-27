from pathlib import Path
import zipfile
import csv
from collections import Counter


# ============================================================
# PROJECT PATH
# ============================================================

project_root = Path(__file__).resolve().parents[2]

zip_path = (
    project_root
    / "data"
    / "raw"
    / "events"
    / "20260101.export.CSV.zip"
)


# ============================================================
# COUNTERS
# ============================================================

event_dates = Counter()
date_added = Counter()


# ============================================================
# READ FILE
# ============================================================

with zipfile.ZipFile(zip_path, "r") as z:

    filename = z.namelist()[0]

    with z.open(filename) as f:

        reader = csv.reader(
            (
                line.decode(
                    "utf-8",
                    errors="replace"
                )
                for line in f
            ),
            delimiter="\t"
        )

        for row in reader:

            if len(row) != 58:
                continue

            # Event date
            event_dates[row[1]] += 1

            # GDELT DATEADDED
            date_added[row[56]] += 1


# ============================================================
# EVENT DATE
# ============================================================

print("\n" + "=" * 60)
print("EVENT DATE (Column 1)")
print("=" * 60)

print(
    "Number of distinct event dates:",
    len(event_dates)
)

for date, count in event_dates.most_common(20):

    print(
        f"{date} -> {count}"
    )


# ============================================================
# DATE ADDED
# ============================================================

print("\n" + "=" * 60)
print("DATE ADDED (Column 56)")
print("=" * 60)

print(
    "Number of distinct DATEADDED values:",
    len(date_added)
)

for date, count in date_added.most_common(20):

    print(
        f"{date} -> {count}"
    )