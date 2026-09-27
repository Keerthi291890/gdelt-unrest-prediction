import zipfile
import csv
import os
from datetime import datetime

DATA_DIR = str(Path(__file__).resolve().parents[2] / "data" / "raw" / "sample_events")

FILES = [
    "20251015.export.CSV.zip",
    "20251115.export.CSV.zip",
    "20251215.export.CSV.zip",
    "20260115.export.CSV.zip",
    "20260215.export.CSV.zip",
    "20260315.export.CSV.zip",
    "20260415.export.CSV.zip",
    "20260515.export.CSV.zip",
    "20260615.export.CSV.zip",
]

for filename in FILES:

    path = os.path.join(DATA_DIR, filename)

    print("=" * 70)
    print(filename)

    if not os.path.exists(path):
        print("FILE NOT FOUND")
        continue

    min_date = None
    max_date = None
    valid_rows = 0

    with zipfile.ZipFile(path, "r") as z:

        csv_file = z.namelist()[0]

        with z.open(csv_file) as f:

            reader = csv.reader(
                (line.decode("utf-8", errors="replace") for line in f),
                delimiter="\t"
            )

            for row in reader:

                if len(row) != 58:
                    continue

                day = row[1].strip()

                if len(day) != 8 or not day.isdigit():
                    continue

                try:
                    date = datetime.strptime(day, "%Y%m%d").date()
                except ValueError:
                    continue

                valid_rows += 1

                if min_date is None or date < min_date:
                    min_date = date

                if max_date is None or date > max_date:
                    max_date = date

    print("Valid rows :", valid_rows)
    print("Minimum Day:", min_date)
    print("Maximum Day:", max_date)
