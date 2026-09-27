from datetime import datetime

from urllib.request import urlopen
from urllib.error import URLError, HTTPError

import re
import time
import shutil
from pathlib import Path


# ============================================================
# CONFIGURATION
# ============================================================

START_DATE = datetime(2026, 4, 1)

END_DATE = datetime(2026, 6, 30, 23, 59, 59)

MASTER_URL = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"

PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_MENTIONS_DIR = PROJECT_ROOT / "data" / "raw" / "mentions"

MASTER_FILE = PROJECT_ROOT / "data" / "raw" / "masterfilelist.txt"

FAILED_FILE = RAW_MENTIONS_DIR / "failed_downloads.txt"

# Never allow C: to fall below this free space.
MIN_FREE_GB = 30

MAX_RETRIES = 3

RETRY_WAIT_SECONDS = 5

DRY_RUN = False

# Known source-level missing Mentions file.
KNOWN_MISSING_FILE = "20260509233000.mentions.CSV.zip"


# ============================================================
# DOWNLOAD MASTER FILE LIST
# ============================================================

def download_master_list():

    MASTER_FILE.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    if MASTER_FILE.exists():

        print("Master file list already exists.")
        print(f"Path: {MASTER_FILE}")

        return

    print("Downloading GDELT master file list...")
    print(MASTER_URL)

    with urlopen(
        MASTER_URL,
        timeout=60
    ) as response:

        data = response.read()

    MASTER_FILE.write_bytes(data)

    print("Master file list downloaded.")
    print(f"Path: {MASTER_FILE}")


# ============================================================
# FIND MENTIONS FILES
# ============================================================

def find_mentions_files():

    print()
    print("=" * 70)
    print("SCANNING MASTER FILE LIST")
    print("=" * 70)

    urls = []

    pattern = re.compile(
        r"(https?://[^\s]+/(\d{14})\.mentions\.CSV\.zip)"
    )

    with MASTER_FILE.open(
        "r",
        encoding="utf-8",
        errors="replace"
    ) as f:

        for line in f:

            match = pattern.search(line)

            if not match:
                continue

            url = match.group(1)

            timestamp = match.group(2)

            try:

                file_time = datetime.strptime(
                    timestamp,
                    "%Y%m%d%H%M%S"
                )

            except ValueError:

                continue

            if START_DATE <= file_time <= END_DATE:

                urls.append(url)

    return urls


# ============================================================
# CHECK FREE DISK SPACE
# ============================================================

def check_disk_space():

    total, used, free = shutil.disk_usage(PROJECT_ROOT)

    free_gb = free / (1024 ** 3)

    return free_gb


# ============================================================
# DOWNLOAD ONE FILE
# ============================================================

def download_one(url, destination):

    temp_destination = destination.with_suffix(
        destination.suffix + ".part"
    )

    for attempt in range(
        1,
        MAX_RETRIES + 1
    ):

        try:

            print(
                f"    Download attempt "
                f"{attempt}/{MAX_RETRIES}"
            )

            with urlopen(
                url,
                timeout=120
            ) as response:

                with temp_destination.open(
                    "wb"
                ) as output:

                    while True:

                        chunk = response.read(
                            1024 * 1024
                        )

                        if not chunk:
                            break

                        output.write(chunk)

            # Only rename after successful completion.
            temp_destination.replace(
                destination
            )

            return True

        except (
            URLError,
            HTTPError,
            TimeoutError,
            OSError
        ) as e:

            print(
                f"    Download failed: {e}"
            )

            if temp_destination.exists():

                temp_destination.unlink()

            if attempt < MAX_RETRIES:

                print(
                    f"    Waiting "
                    f"{RETRY_WAIT_SECONDS} seconds..."
                )

                time.sleep(
                    RETRY_WAIT_SECONDS
                )

    return False


# ============================================================
# MAIN
# ============================================================

def main():

    print("=" * 70)
    print("GDELT HISTORICAL MENTIONS DOWNLOAD")
    print("=" * 70)

    print()

    print("Historical period:")
    print("START:", START_DATE)
    print("END  :", END_DATE)

    print()

    print("Known source-level missing file:")
    print(KNOWN_MISSING_FILE)

    # --------------------------------------------------------
    # Prepare directories
    # --------------------------------------------------------

    RAW_MENTIONS_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    # --------------------------------------------------------
    # Master list
    # --------------------------------------------------------

    download_master_list()

    # --------------------------------------------------------
    # Find files
    # --------------------------------------------------------

    urls = find_mentions_files()

    print()

    print("=" * 70)
    print("DOWNLOAD PLAN")
    print("=" * 70)

    print(
        "Mentions files available:",
        len(urls)
    )

    print(
        "Expected theoretical files:",
        8736
    )

    print(
        "Known missing file:",
        KNOWN_MISSING_FILE
    )

    print("Output directory:")
    print(RAW_MENTIONS_DIR)

    # --------------------------------------------------------
    # Initial disk check
    # --------------------------------------------------------

    free_gb = check_disk_space()

    print()

    print(
        f"Free disk space: "
        f"{free_gb:.2f} GB"
    )

    print(
        f"Minimum allowed: "
        f"{MIN_FREE_GB} GB"
    )

    if free_gb < MIN_FREE_GB:

        print()
        print("STOPPING.")
        print(
            "Not enough free disk space."
        )

        return

    if DRY_RUN:
    
            print()
            print("=" * 70)
            print("DRY RUN ONLY")
            print("=" * 70)
    
            print("No files will be downloaded.")
            print()
    
            print(
                "Mentions files available:",
                len(urls)
            )
    
            print(
                "Expected theoretical files:",
                8736
            )
    
            print(
                "Known missing:",
                KNOWN_MISSING_FILE
            )
    
            print(
                f"Free disk space: "
                f"{free_gb:.2f} GB"
            )
    
            print()
            print("Dry run successful.")
            print("Ready for actual download.")
    
            return

    # --------------------------------------------------------
    # Download statistics
    # --------------------------------------------------------

    downloaded = 0

    skipped = 0

    failed = 0

    failed_urls = []

    # --------------------------------------------------------
    # Download sequentially
    # --------------------------------------------------------

    print()

    print("=" * 70)
    print("STARTING MENTIONS DOWNLOAD")
    print("=" * 70)

    for index, url in enumerate(
        urls,
        start=1
    ):

        filename = url.split("/")[-1]

        # Explicitly skip the known unavailable file.
        if filename == KNOWN_MISSING_FILE:

            print()

            print(
                f"[{index}/{len(urls)}] "
                f"{filename}"
            )

            print(
                "    Known source-level missing "
                "file -> SKIPPING"
            )

            skipped += 1

            continue

        destination = (
            RAW_MENTIONS_DIR / filename
        )

        print()

        print(
            f"[{index}/{len(urls)}] "
            f"{filename}"
        )

        # ----------------------------------------------------
        # Resume support
        # ----------------------------------------------------

        if destination.exists():

            skipped += 1

            print(
                "    Already exists -> SKIPPING"
            )

            continue

        # ----------------------------------------------------
        # Disk safety check
        # ----------------------------------------------------

        free_gb = check_disk_space()

        if free_gb < MIN_FREE_GB:

            print()

            print("=" * 70)
            print(
                "DOWNLOAD STOPPED — "
                "LOW DISK SPACE"
            )
            print("=" * 70)

            print(
                f"Free space: "
                f"{free_gb:.2f} GB"
            )

            print(
                f"Required minimum: "
                f"{MIN_FREE_GB} GB"
            )

            break

        # ----------------------------------------------------
        # Download
        # ----------------------------------------------------

        success = download_one(
            url,
            destination
        )

        if success:

            downloaded += 1

            print(
                "    SUCCESS"
            )

        else:

            failed += 1

            failed_urls.append(url)

            print(
                "    FAILED"
            )

    # --------------------------------------------------------
    # Save failed URLs
    # --------------------------------------------------------

    if failed_urls:

        with FAILED_FILE.open(
            "w",
            encoding="utf-8"
        ) as f:

            for url in failed_urls:

                f.write(
                    url + "\n"
                )

    # --------------------------------------------------------
    # Final report
    # --------------------------------------------------------

    free_gb = check_disk_space()

    print()

    print("=" * 70)
    print("MENTIONS DOWNLOAD SUMMARY")
    print("=" * 70)

    print(
        "Files in download plan :",
        len(urls)
    )

    print(
        "Downloaded this run    :",
        downloaded
    )

    print(
        "Already existed        :",
        skipped
    )

    print(
        "Failed this run        :",
        failed
    )

    print(
        f"Free disk space        : "
        f"{free_gb:.2f} GB"
    )

    if failed_urls:

        print()

        print(
            "Failed URLs saved to:"
        )

        print(
            FAILED_FILE
        )

    print()

    print("=" * 70)
    print("DOWNLOAD RUN COMPLETE")
    print("=" * 70)


if __name__ == "__main__":

    main()