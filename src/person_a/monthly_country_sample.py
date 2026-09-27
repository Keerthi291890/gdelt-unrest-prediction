from pathlib import Path
import zipfile
import csv
from collections import Counter


# ============================================================
# PATHS
# ============================================================

project_root = Path(__file__).resolve().parents[2]

sample_dir = (
    project_root
    / "data"
    / "raw"
    / "sample_events"
)


# ============================================================
# COUNTRIES
# ============================================================

countries = {
    "IND": {"name": "India", "geo": "IN"},
    "NGA": {"name": "Nigeria", "geo": "NI"},
    "BRA": {"name": "Brazil", "geo": "BR"},
    "UKR": {"name": "Ukraine", "geo": "UP"},
    "USA": {"name": "United States", "geo": "US"},
    "MEX": {"name": "Mexico", "geo": "MX"},
    "IDN": {"name": "Indonesia", "geo": "ID"},
    "ZAF": {"name": "South Africa", "geo": "SF"},
}


# ============================================================
# FILES
# ============================================================

files = sorted(sample_dir.glob("*.export.CSV.zip"))

print("=" * 80)
print("MONTHLY COUNTRY FEASIBILITY ANALYSIS")
print("=" * 80)

print("\nFiles found:", len(files))

for file in files:
    print(" -", file.name)


# ============================================================
# RESULTS
# ============================================================

results = {}


# ============================================================
# PROCESS EACH FILE
# ============================================================

for zip_path in files:

    date = zip_path.name[:8]

    print("\nProcessing:", zip_path.name)

    actor1 = Counter()
    actor2 = Counter()
    action_geo = Counter()

    valid_rows = 0

    with zipfile.ZipFile(zip_path, "r") as z:

        filename = z.namelist()[0]

        with z.open(filename) as f:

            reader = csv.reader(
                (
                    line.decode(
                        "utf-8",
                        errors="replace"
                    )
                    for line in f
                ),
                delimiter="\t"
            )

            for row in reader:

                if len(row) != 58:
                    continue

                valid_rows += 1

                # Actor 1
                if row[7] in countries:
                    actor1[row[7]] += 1

                # Actor 2
                if row[17] in countries:
                    actor2[row[17]] += 1

                # Action geography
                for code, info in countries.items():

                    if row[51] == info["geo"]:
                        action_geo[code] += 1

    results[date] = {
        "valid_rows": valid_rows,
        "actor1": actor1,
        "actor2": actor2,
        "action_geo": action_geo,
    }


# ============================================================
# PRINT RESULTS
# ============================================================

for date in sorted(results):

    print("\n" + "=" * 80)
    print("DATE:", date)
    print("Valid rows:", results[date]["valid_rows"])
    print("=" * 80)

    print(
        f"{'Country':15}"
        f"{'Actor1':>10}"
        f"{'Actor2':>10}"
        f"{'ActionGeo':>12}"
    )

    print("-" * 50)

    for code, info in countries.items():

        print(
            f"{info['name']:15}"
            f"{results[date]['actor1'][code]:10}"
            f"{results[date]['actor2'][code]:10}"
            f"{results[date]['action_geo'][code]:12}"
        )


# ============================================================
# SUMMARY: ACTION GEOGRAPHY
# ============================================================

print("\n" + "=" * 80)
print("ACTION-GEOGRAPHY SUMMARY")
print("=" * 80)

print(
    f"{'Country':15}"
    f"{'Mean':>12}"
    f"{'Minimum':>12}"
    f"{'Maximum':>12}"
    f"{'Months > 0':>12}"
)

print("-" * 65)

for code, info in countries.items():

    values = [
        results[date]["action_geo"][code]
        for date in sorted(results)
    ]

    mean_value = sum(values) / len(values)

    print(
        f"{info['name']:15}"
        f"{mean_value:12.1f}"
        f"{min(values):12}"
        f"{max(values):12}"
        f"{sum(v > 0 for v in values):12}"
    )