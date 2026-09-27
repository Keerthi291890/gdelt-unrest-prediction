from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError


dates = [
    "20251015",
    "20251115",
    "20251215",
    "20260115",
    "20260215",
    "20260315",
    "20260415",
    "20260515",
    "20260615",
]


BASE_URL = "https://data.gdeltproject.org/events/"


print("=" * 70)
print("CHECKING GDELT MONTHLY SAMPLE FILES")
print("=" * 70)


for date in dates:

    filename = f"{date}.export.CSV.zip"
    url = BASE_URL + filename

    try:

        request = Request(
            url,
            method="HEAD",
            headers={
                "User-Agent": "Mozilla/5.0"
            }
        )

        with urlopen(request, timeout=20) as response:

            print(
                f"{date}  ->  AVAILABLE  "
                f"({response.status})"
            )

    except HTTPError as e:

        print(
            f"{date}  ->  NOT AVAILABLE  "
            f"(HTTP {e.code})"
        )

    except URLError as e:

        print(
            f"{date}  ->  CONNECTION ERROR"
        )

    except Exception as e:

        print(
            f"{date}  ->  ERROR: {e}"
        )