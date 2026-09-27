import requests
from datetime import datetime

URL = "https://data.gdeltproject.org/gdeltv2/lastupdate.txt"

response = requests.get(URL, timeout=30)
response.raise_for_status()

print("GDELT source is accessible.")
print()

for line in response.text.splitlines():

    parts = line.split()

    if len(parts) < 4:
        continue

    url = parts[3]

    if ".mentions.CSV.zip" in url:
        print("Mentions file:")
        print(url)