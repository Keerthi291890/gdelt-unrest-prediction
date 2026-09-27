from pathlib import Path
import zipfile
import csv
from collections import Counter


# ============================================================
# 1. PROJECT PATH
# ============================================================

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


# ============================================================
# 2. CANDIDATE COUNTRIES
# ============================================================

countries = {
    "IND": {
        "name": "India",
        "geo": "IN"
    },

    "NGA": {
        "name": "Nigeria",
        "geo": "NI"
    },

    "BRA": {
        "name": "Brazil",
        "geo": "BR"
    },

    "UKR": {
        "name": "Ukraine",
        "geo": "UP"
    },

    "USA": {
        "name": "United States",
        "geo": "US"
    },

    "MEX": {
        "name": "Mexico",
        "geo": "MX"
    },

    "IDN": {
        "name": "Indonesia",
        "geo": "ID"
    },

    "ZAF": {
        "name": "South Africa",
        "geo": "SF"
    }
}


# ============================================================
# 3. COUNTERS
# ============================================================

actor1_counts = Counter()
actor2_counts = Counter()
action_geo_counts = Counter()

total_rows = 0
invalid_rows = 0


# ============================================================
# 4. READ GDELT
# ============================================================

with zipfile.ZipFile(zip_path, "r") as z:

    filename = z.namelist()[0]

    print("Inside ZIP:", filename)

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
                invalid_rows += 1
                continue

            total_rows += 1

            # ------------------------------------------------
            # Actor country fields
            # ------------------------------------------------

            actor1_country = row[7]
            actor2_country = row[17]

            # ------------------------------------------------
            # Action geography country
            # ------------------------------------------------

            action_geo_country = row[51]

            # ------------------------------------------------
            # Actor 1
            # ------------------------------------------------

            if actor1_country in countries:
                actor1_counts[actor1_country] += 1

            # ------------------------------------------------
            # Actor 2
            # ------------------------------------------------

            if actor2_country in countries:
                actor2_counts[actor2_country] += 1

            # ------------------------------------------------
            # Action Geography
            # ------------------------------------------------

            for code, info in countries.items():

                if action_geo_country == info["geo"]:
                    action_geo_counts[code] += 1


# ============================================================
# 5. FILE STATISTICS
# ============================================================

print("\n" + "=" * 60)
print("FILE STATISTICS")
print("=" * 60)

print("Total valid rows:", total_rows)
print("Invalid rows:", invalid_rows)


# ============================================================
# 6. ACTOR 1
# ============================================================

print("\n" + "=" * 60)
print("ACTOR 1 COUNTS")
print("=" * 60)

for code, info in countries.items():

    print(
        f"{info['name']:15} "
        f"({code}) : "
        f"{actor1_counts[code]}"
    )


# ============================================================
# 7. ACTOR 2
# ============================================================

print("\n" + "=" * 60)
print("ACTOR 2 COUNTS")
print("=" * 60)

for code, info in countries.items():

    print(
        f"{info['name']:15} "
        f"({code}) : "
        f"{actor2_counts[code]}"
    )


# ============================================================
# 8. ACTION GEOGRAPHY
# ============================================================

print("\n" + "=" * 60)
print("ACTION GEOGRAPHY COUNTS")
print("=" * 60)

for code, info in countries.items():

    print(
        f"{info['name']:15} "
        f"({code}) : "
        f"{action_geo_counts[code]}"
    )