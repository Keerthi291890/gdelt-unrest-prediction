import zipfile
from pathlib import Path
from collections import Counter

GKG_DIR = Path(r"..\..\data\raw\gkg")

# Check a few files spread across the 3-month period
FILES_TO_CHECK = [
    "20260401000000.gkg.csv.zip",
    "20260415120000.gkg.csv.zip",
    "20260501000000.gkg.csv.zip",
    "20260515120000.gkg.csv.zip",
    "20260601000000.gkg.csv.zip",
    "20260615120000.gkg.csv.zip",
    "20260630120000.gkg.csv.zip",
]

IMPORTANT_FIELDS = {
    1: "Timestamp",
    3: "Source / Domain",
    4: "Document URL",
    7: "Themes",
    11: "Persons",
    13: "Organizations",
    15: "Tone",
    17: "GCAM / Emotion Data",
    26: "Page Links",
}

print("=" * 70)
print("GKG FIELD VALIDATION")
print("=" * 70)

field_counts = Counter()
sample_values = {}

files_checked = 0
rows_checked = 0

for filename in FILES_TO_CHECK:

    path = GKG_DIR / filename

    if not path.exists():
        print(f"\n[SKIP] {filename} not found")
        continue

    files_checked += 1

    with zipfile.ZipFile(path) as z:
        inner_file = z.namelist()[0]

        with z.open(inner_file) as f:

            # Inspect first 5 records only
            for row_number in range(5):

                line = f.readline()

                if not line:
                    break

                row = line.decode(
                    "utf-8",
                    errors="replace"
                ).rstrip("\r\n").split("\t")

                rows_checked += 1
                field_counts[len(row)] += 1

                # Save first non-empty example for each important field
                for index, name in IMPORTANT_FIELDS.items():

                    if index < len(row):

                        value = row[index].strip()

                        if value and index not in sample_values:
                            # Keep output short
                            sample_values[index] = value[:150]


print("\n" + "=" * 70)
print("FILES / ROWS CHECKED")
print("=" * 70)

print(f"Files checked : {files_checked}")
print(f"Rows checked  : {rows_checked}")

print("\n" + "=" * 70)
print("FIELD COUNT CHECK")
print("=" * 70)

for count, frequency in sorted(field_counts.items()):
    print(f"{count} fields -> {frequency} rows")

print("\n" + "=" * 70)
print("IMPORTANT FIELD VALIDATION")
print("=" * 70)

for index, name in IMPORTANT_FIELDS.items():

    if index in sample_values:
        print(f"\n[{index}] {name}")
        print(f"    SAMPLE: {sample_values[index]}")
    else:
        print(f"\n[{index}] {name}")
        print("    SAMPLE: EMPTY in checked rows")

print("\n" + "=" * 70)
print("VALIDATION COMPLETE")
print("=" * 70)