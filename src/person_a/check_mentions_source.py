import requests

URL = "https://data.gdeltproject.org/gdeltv2/lastupdate.txt"

response = requests.get(URL, timeout=30)

print("Status:", response.status_code)

if response.status_code == 200:
    print("\nLATEST GDELT FILES")
    print("=" * 70)

    lines = response.text.strip().splitlines()

    for line in lines:
        print(line)

else:
    print("Could not access GDELT update file.")