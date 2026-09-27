from pathlib import Path
import sys
import time
import shutil
import requests
import subprocess

BASE_DIR = Path(__file__).resolve().parents[3]

PYTHON = sys.executable

DOWNLOADER = (
    BASE_DIR
    / "processing"
    / "person_a"
    / "stage2_live"
    / "live_downloader.py"
)

LANDER = (
    BASE_DIR
    / "processing"
    / "person_a"
    / "stage2_live"
    / "live_landing.py"
)

VERIFIED_DIR = (
    BASE_DIR
    / "data"
    / "raw"
    / "incoming"
    / "verified"
)

STATE_FILE = (
    BASE_DIR
    / "data"
    / "raw"
    / "incoming"
    / "last_processed_batch.txt"
)

MIN_FREE_GB = 60
POLL_SECONDS = 60


def free_space_gb():
    return shutil.disk_usage(BASE_DIR).free / (1024 ** 3)


def get_verified_batch():
    files = list(VERIFIED_DIR.glob("*.zip"))

    if len(files) != 3:
        return None

    events = [f for f in files if ".export.CSV.zip" in f.name]
    mentions = [f for f in files if ".mentions.CSV.zip" in f.name]
    gkg = [f for f in files if ".gkg.csv.zip" in f.name]

    if len(events) != 1 or len(mentions) != 1 or len(gkg) != 1:
        return None

    timestamp = events[0].name[:14]

    if not (
        mentions[0].name.startswith(timestamp)
        and gkg[0].name.startswith(timestamp)
    ):
        return None

    return timestamp


def get_last_processed():
    if not STATE_FILE.exists():
        return None

    return STATE_FILE.read_text().strip()


def save_last_processed(timestamp):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(timestamp)


def run_once():
    print("\n" + "=" * 70)
    print("LIVE POLLER CHECK")
    print("=" * 70)

    free_gb = free_space_gb()

    print(f"Free disk space: {free_gb:.2f} GB")

    if free_gb < MIN_FREE_GB:
        print("STOP: disk safety limit reached.")
        return

    existing_batch = get_verified_batch()

    if existing_batch:
        print(f"Verified batch found: {existing_batch}")

        last_processed = get_last_processed()

        if existing_batch == last_processed:
            print("Batch already processed. Nothing to do.")
            return

        print("Starting live landing...")

        result = subprocess.run(
            [PYTHON, str(LANDER)],
            cwd=str(BASE_DIR)
        )

        if result.returncode == 0:
            save_last_processed(existing_batch)
            print(f"Batch {existing_batch} marked as processed.")
        else:
            print("Landing failed. Batch is NOT marked as processed.")

        return

    print("No verified batch currently available.")
    print("Running safe downloader...")

    result = subprocess.run(
        [PYTHON, str(DOWNLOADER)],
        cwd=str(BASE_DIR)
    )

    if result.returncode != 0:
        print("Downloader failed or found no complete batch.")
        return

    new_batch = get_verified_batch()

    if not new_batch:
        print("Downloader finished but no complete verified batch was found.")
        return

    print(f"New verified batch: {new_batch}")
    print("Starting live landing...")

    result = subprocess.run(
        [PYTHON, str(LANDER)],
        cwd=str(BASE_DIR)
    )

    if result.returncode == 0:
        save_last_processed(new_batch)
        print(f"Batch {new_batch} marked as processed.")
    else:
        print("Landing failed. Batch is NOT marked as processed.")


# ------------------------------------------------------------
# FIRST RUN: ONE CYCLE ONLY
# ------------------------------------------------------------
if __name__ == "__main__":

    print("=" * 70)
    print("GDELT SAFE LIVE POLLER")
    print("=" * 70)
    print("Mode: CONTINUOUS")
    print(f"Polling interval: {POLL_SECONDS} seconds")
    print(f"Minimum free space: {MIN_FREE_GB} GB")
    print("Press Ctrl+C to stop safely.")
    print("=" * 70)

    try:
        while True:
            run_once()

            print(f"\nWaiting {POLL_SECONDS} seconds...")
            time.sleep(POLL_SECONDS)

    except KeyboardInterrupt:
        print("\n")
        print("=" * 70)
        print("LIVE POLLER STOPPED SAFELY")
        print("=" * 70)
