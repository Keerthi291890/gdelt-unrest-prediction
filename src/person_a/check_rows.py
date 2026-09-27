from pathlib import Path
import zipfile

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

        for i in range(10):
            line = f.readline()

            if not line:
                break

            row = line.decode(
                "utf-8",
                errors="replace"
            ).rstrip("\r\n").split("\t")

            print(f"Row {i + 1}: {len(row)} columns")