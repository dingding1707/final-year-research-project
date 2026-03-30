from pathlib import Path

OUTPUT_DIR = Path("out")
OUTPUT_DIR.mkdir(exist_ok=True)

# Study parameters
US_LOCATIONS = [
    "New York, NY",        # Northeast – finance + tech
    "San Francisco, CA",   # West Coast – tech hub
    "Seattle, WA",         # West Coast – cloud / big tech
    "Austin, TX",          # South – growing tech ecosystem
    "Atlanta, GA",         # Southeast – enterprise + fintech
    "Chicago, IL"          # Midwest – enterprise + analytics
]

RESULTS_PER_TITLE = 12             # cap per title
HEADLESS = True                    # set False while debugging

# Polite throttling (seconds)
MIN_SLEEP = 6.0
MAX_SLEEP = 12.0
