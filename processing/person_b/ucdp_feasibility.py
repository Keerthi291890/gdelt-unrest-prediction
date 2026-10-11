from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "data" / "external" / "ucdp_candidate"

files = sorted(SRC.glob("*.csv"))

if not files:
    raise FileNotFoundError(
        f"No UCDP CSV files found in {SRC}"
    )

print("FILES:")
for f in files:
    print(" -", f.name)

# Only read columns needed for feasibility
wanted = {
    "id",
    "country",
    "date_start",
    "date_end",
    "best",
    "type_of_violence",
}

frames = []

for f in files:
    print(f"\nReading: {f.name}")

    df_part = pd.read_csv(
        f,
        usecols=lambda c: c in wanted,
        low_memory=False
    )

    print("Rows:", len(df_part))
    print("Columns:", df_part.columns.tolist())

    frames.append(df_part)

df = pd.concat(frames, ignore_index=True)

print("\nTOTAL RAW ROWS:", len(df))

# ---------------------------------------------------------
# Basic schema check
# ---------------------------------------------------------

required = {
    "id",
    "country",
    "date_start",
    "date_end",
    "best",
    "type_of_violence",
}

missing = required - set(df.columns)

if missing:
    raise ValueError(f"Missing required columns: {missing}")

# ---------------------------------------------------------
# Dates / numeric fields
# ---------------------------------------------------------

df["date_start"] = pd.to_datetime(df["date_start"], errors="coerce")
df["date_end"] = pd.to_datetime(df["date_end"], errors="coerce")
df["best"] = pd.to_numeric(df["best"], errors="coerce")

print("\nINVALID date_start:", df["date_start"].isna().sum())
print("INVALID date_end:", df["date_end"].isna().sum())
print("INVALID best:", df["best"].isna().sum())

# ---------------------------------------------------------
# Target countries
# ---------------------------------------------------------

TARGETS = {
    "United States of America": "US",
    "India": "IN",
    "Nigeria": "NI",
    "Ukraine": "UP",
    "South Africa": "SF",
}

d = df[df["country"].isin(TARGETS)].copy()

d["country_code"] = d["country"].map(TARGETS)

print("\nTARGET COUNTRY COVERAGE:")
print(
    d.groupby("country_code")
     .agg(
         events=("id", "nunique"),
         rows=("id", "size"),
         first_date=("date_start", "min"),
         last_date=("date_start", "max"),
     )
     .reset_index()
     .to_string(index=False)
)

# ---------------------------------------------------------
# 2026 coverage
# ---------------------------------------------------------

d2026 = d[
    (d["date_start"] >= "2026-01-01") &
    (d["date_start"] <= "2026-07-31")
].copy()

print("\n2026 TARGET COVERAGE:")
print(
    d2026.groupby("country_code")
         .agg(
             events=("id", "nunique"),
             deaths_best=("best", "sum"),
             first_date=("date_start", "min"),
             last_date=("date_start", "max"),
         )
         .reset_index()
         .to_string(index=False)
)

# ---------------------------------------------------------
# Monthly coverage
# ---------------------------------------------------------

d2026["month"] = d2026["date_start"].dt.to_period("M")

print("\nEVENTS / BEST DEATHS BY COUNTRY AND MONTH:")

monthly = (
    d2026.groupby(["country_code", "month"])
         .agg(
             events=("id", "nunique"),
             deaths_best=("best", "sum"),
         )
         .reset_index()
)

print(monthly.to_string(index=False))

# ---------------------------------------------------------
# Violence types
# ---------------------------------------------------------

print("\nVIOLENCE TYPE DISTRIBUTION:")

violence = (
    d2026.groupby(["country_code", "type_of_violence"])
         .agg(
             events=("id", "nunique"),
             deaths_best=("best", "sum"),
         )
         .reset_index()
)

print(violence.to_string(index=False))

# ---------------------------------------------------------
# Multi-day events
# ---------------------------------------------------------

d2026["multi_day"] = d2026["date_end"] > d2026["date_start"]

print("\nMULTI-DAY EVENT SHARE:")

multi = (
    d2026.groupby("country_code")["multi_day"]
         .mean()
         .round(3)
)

print(multi)

print("\n===== FEASIBILITY COMPLETE =====")