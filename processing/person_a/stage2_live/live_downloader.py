import shutil
import requests
from pathlib import Path
from datetime import datetime, timedelta

from config import LAST_UPDATE_URL, MIN_FREE_GB

BASE_DIR = Path(__file__).resolve().parents[3]

INCOMING_DIR = BASE_DIR / "data" / "raw" / "incoming"
VERIFIED_DIR = INCOMING_DIR / "verified"


def get_free_space_gb():
    _, _, free = shutil.disk_usage(BASE_DIR)
    return free / (1024 ** 3)


def get_last_update():
    response = requests.get(LAST_UPDATE_URL, timeout=30)
    response.raise_for_status()
    return response.text


def check_file_available(url):
    try:
        response = requests.head(
            url,
            timeout=30,
            allow_redirects=True
        )
        return response.status_code == 200
    except requests.RequestException:
        return False


def build_urls(timestamp):
    base = "http://data.gdeltproject.org/gdeltv2"

    return [
        f"{base}/{timestamp}.export.CSV.zip",
        f"{base}/{timestamp}.mentions.CSV.zip",
        f"{base}/{timestamp}.gkg.csv.zip",
    ]


def download_file(url, output_path):
    print(f"Downloading: {url}")

    response = requests.get(
        url,
        stream=True,
        timeout=120
    )
    response.raise_for_status()

    with open(output_path, "wb") as f:
        for chunk in response.iter_content(
            chunk_size=1024 * 1024
        ):
            if chunk:
                f.write(chunk)

    size_mb = output_path.stat().st_size / (1024 ** 2)

    print(
        f"Saved: {output_path.name} "
        f"({size_mb:.2f} MB)"
    )


def main():

    VERIFIED_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    print("=" * 70)
    print("GDELT STAGE 2 - COMPLETE LIVE BATCH")
    print("=" * 70)

    free_gb = get_free_space_gb()

    print(f"Free space: {free_gb:.2f} GB")

    if free_gb < MIN_FREE_GB:
        print("SAFETY STOP: insufficient free space.")
        return

    text = get_last_update()

    first_line = text.strip().splitlines()[0]
    parts = first_line.split()

    if len(parts) != 3:
        print("Could not read GDELT timestamp.")
        return

    latest_timestamp = (
        parts[2]
        .split("/")[-1]
        .split(".")[0]
    )

    latest_time = datetime.strptime(
        latest_timestamp,
        "%Y%m%d%H%M%S"
    )

    print(
        f"Latest advertised timestamp: "
        f"{latest_timestamp}"
    )

    # Look back through recent 15-minute batches.
    for i in range(8):

        current_time = (
            latest_time -
            timedelta(minutes=15 * i)
        )

        timestamp = current_time.strftime(
            "%Y%m%d%H%M%S"
        )

        print()
        print("-" * 70)
        print(f"Checking batch: {timestamp}")

        urls = build_urls(timestamp)

        all_available = True

        for url in urls:

            filename = Path(url).name

            if check_file_available(url):
                print(
                    f"AVAILABLE : {filename}"
                )
            else:
                print(
                    f"NOT READY : {filename}"
                )
                all_available = False

        if not all_available:
            continue

        print()
        print(
            f"COMPLETE BATCH FOUND: "
            f"{timestamp}"
        )

        downloaded = []

        try:

            for url in urls:

                filename = Path(url).name

                output_path = (
                    VERIFIED_DIR / filename
                )

                # Avoid downloading a batch
                # that is already present.
                if output_path.exists():
                    print(
                        f"Already present: "
                        f"{filename}"
                    )
                    downloaded.append(
                        output_path
                    )
                    continue

                download_file(
                    url,
                    output_path
                )

                downloaded.append(
                    output_path
                )

            print()
            print("=" * 70)
            print(
                "COMPLETE BATCH STORED IN VERIFIED"
            )
            print("=" * 70)

            print(
                f"Location: {VERIFIED_DIR}"
            )

            return

        except Exception as e:

            print()
            print(
                "DOWNLOAD FAILED:",
                e
            )

            print(
                "Cleaning incomplete batch..."
            )

            for path in downloaded:

                if path.exists():

                    path.unlink()

                    print(
                        f"Deleted: "
                        f"{path.name}"
                    )

            print(
                "Verified folder cleaned."
            )

            return

    print()
    print("=" * 70)
    print("NO COMPLETE BATCH FOUND")
    print("=" * 70)


if __name__ == "__main__":
    main()
