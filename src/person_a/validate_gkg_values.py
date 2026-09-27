import zipfile
from pathlib import Path

GKG_DIR = Path(r"..\..\data\raw\gkg")

FILES_TO_CHECK = [
    "20260401000000.gkg.csv.zip",
    "20260415120000.gkg.csv.zip",
    "20260501000000.gkg.csv.zip",
    "20260515120000.gkg.csv.zip",
    "20260601000000.gkg.csv.zip",
    "20260615120000.gkg.csv.zip",
]

print("=" * 70)
print("GKG VALUE VALIDATION")
print("=" * 70)

for filename in FILES_TO_CHECK:

    path = GKG_DIR / filename

    if not path.exists():
        print(f"[SKIP] {filename}")
        continue

    with zipfile.ZipFile(path) as z:

        with z.open(z.namelist()[0]) as f:

            line = f.readline()

            row = line.decode(
                "utf-8",
                errors="replace"
            ).rstrip("\r\n").split("\t")

            timestamp = row[1]
            source = row[3]
            url = row[4]

            tone_values = row[15].split(",") if row[15] else []
            gcam = row[17]

            # Word count is the last value in the tone field
            word_count = tone_values[-1] if tone_values else ""

            print(f"\nFILE: {filename}")
            print(f"  Timestamp : {timestamp}")
            print(f"  Source    : {source}")
            print(f"  URL       : {'YES' if url else 'EMPTY'}")
            print(f"  Tone vals : {len(tone_values)}")
            print(f"  Word count: {word_count}")
            print(f"  GCAM      : {'YES' if gcam.startswith('wc:') else 'NO'}")

print("\n" + "=" * 70)
print("VALIDATION COMPLETE")
print("=" * 70)