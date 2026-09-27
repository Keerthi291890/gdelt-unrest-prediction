# Stage 2 Live Acquisition Configuration

from pathlib import Path

# Project root
BASE_DIR = Path(__file__).resolve().parents[3]

# Temporary live GDELT download location
LIVE_INCOMING_DIR = BASE_DIR / "data" / "raw" / "incoming"

# GDELT live update list
LAST_UPDATE_URL = "https://data.gdeltproject.org/gdeltv2/lastupdate.txt"

# Safety limit:
# Stop downloading if free C: drive space falls below this value.
MIN_FREE_GB = 30

# Polling interval
POLL_INTERVAL_SECONDS = 60
