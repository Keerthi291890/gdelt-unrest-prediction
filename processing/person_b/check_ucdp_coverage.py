
from pathlib import Path
from datetime import date
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
path = ROOT / "data" / "external" / "August_new" / "GEDEvent_v26_0_8.csv"

if not path.exists():
    raise FileNotFoundError(f"Download the UCDP file first: {path}")

max_date = None
rows = 0

for chunk in pd.read_csv(
    path,
    usecols=["date_start"],
    dtype={"date_start": "string"},
    chunksize=100_000,
):
    dates = pd.to_datetime(chunk["date_start"], errors="coerce")
    chunk_max = dates.max()

    if pd.notna(chunk_max):
        max_date = chunk_max if max_date is None else max(max_date, chunk_max)

    rows += len(chunk)

print("File:", path.name)
print("Checked on:", date.today().isoformat())
print("Rows scanned:", rows)
print("Maximum date_start:", max_date.date() if max_date is not None else "NONE")
