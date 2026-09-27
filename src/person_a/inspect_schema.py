import zipfile
import csv
import os

zip_path = str(Path(__file__).resolve().parents[2] / "data" / "raw" / "sample_events" / "20251015.export.CSV.zip")

print("=" * 80)
print("ACTUAL 58-COLUMN GDELT ROW")
print("=" * 80)

print(f"ZIP: {zip_path}")
print(f"Exists: {os.path.exists(zip_path)}")

with zipfile.ZipFile(zip_path, "r") as z:

    files = z.namelist()
    print(f"Inside ZIP: {files[0]}")

    with z.open(files[0]) as f:

        reader = csv.reader(
            (line.decode("utf-8", errors="replace") for line in f),
            delimiter="\t"
        )

        for row_number, row in enumerate(reader, start=1):

            if len(row) != 58:
                continue

            print("\n" + "=" * 80)
            print(f"ROW {row_number}")
            print("=" * 80)

            print(f"Number of columns: {len(row)}")

            for i, value in enumerate(row):
                print(f"{i:02d} -> {repr(value)}")

            break
