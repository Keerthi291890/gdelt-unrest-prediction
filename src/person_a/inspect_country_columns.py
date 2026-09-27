from pathlib import Path
import zipfile
import csv
from collections import Counter

project_root = Path(__file__).resolve().parents[2]

zip_path = (
    project_root
    / "data"
    / "raw"
    / "events"
    / "20260101.export.CSV.zip"
)

columns_to_check = {
    7: "Actor1CountryCode",
    17: "Actor2CountryCode",
    44: "GeoCountryCode_1",
    51: "GeoCountryCode_2",
}

counters = {
    column: Counter()
    for column in columns_to_check
}

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

            if len(row) != 58:
                continue

            for column in columns_to_check:
                counters[column][row[column]] += 1


for column, name in columns_to_check.items():

    print("\n" + "=" * 50)
    print(f"Column {column} -> {name}")
    print("=" * 50)

    for value, count in counters[column].most_common(20):
        print(repr(value), "->", count)