from pathlib import Path
import zipfile
import csv

# Find project root
project_root = Path(__file__).resolve().parents[2]

zip_path = (
    project_root
    / "data"
    / "raw"
    / "events"
    / "20260101.export.CSV.zip"
)

print("ZIP:", zip_path)
print("Exists:", zip_path.exists())

with zipfile.ZipFile(zip_path, "r") as z:

    filename = z.namelist()[0]

    print("Inside ZIP:", filename)

    with z.open(filename) as f:

        reader = csv.reader(
            (line.decode("utf-8", errors="replace") for line in f),
            delimiter="\t"
        )

        for i, row in enumerate(reader):

            print(f"\nRow {i + 1}")
            print("Number of columns:", len(row))
            print(row[:10])

            if i == 4:
                break