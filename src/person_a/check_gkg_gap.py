from datetime import datetime, timedelta
import download_full_gkg as g


print("=" * 70)
print("CHECKING GKG 15-MINUTE COVERAGE")
print("=" * 70)

# Get GKG URLs from the master list
urls = g.find_gkg_files()

# Extract timestamps
found = set()

for url in urls:
    filename = url.split("/")[-1]
    timestamp = filename[:14]

    try:
        dt = datetime.strptime(timestamp, "%Y%m%d%H%M%S")
        found.add(dt)
    except ValueError:
        pass


# Expected timestamps
start = datetime(2025, 10, 1, 0, 0)
end = datetime(2026, 6, 30, 23, 45)

expected = 0
missing = []

current = start

while current <= end:

    expected += 1

    if current not in found:
        missing.append(current)

    current += timedelta(minutes=15)


print()
print("=" * 70)
print("RESULT")
print("=" * 70)

print("Expected files :", expected)
print("Found files    :", len(found))
print("Missing files  :", len(missing))

if missing:
    print()
    print("Missing timestamp(s):")

    for timestamp in missing:
        print(" ", timestamp)

else:
    print()
    print("NO MISSING FILES")


print()
print("=" * 70)
print("CHECK COMPLETE")
print("=" * 70)