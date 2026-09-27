from pathlib import Path
import zipfile
import csv

project_root = Path(__file__).resolve().parents[2]

zip_path = (
    project_root
    / "data"
    / "raw"
    / "events"
    / "20260101.export.CSV.zip"
)

with zipfile.ZipFile(zip_path, "r") as z:

    filename = z.namelist()[0]

    with z.open(filename) as f:

        reader = csv.reader(
            (
                line.decode("utf-8", errors="replace")
                for line in f
            ),
            delimiter="\t"
        )

        for row in reader:

            if len(row) == 58:

                print("\nTotal columns:", len(row))

                print("\nColumns 35-57:")

                for i in range(35, 58):
                    print(f"{i:02d} -> {repr(row[i])}")

                break