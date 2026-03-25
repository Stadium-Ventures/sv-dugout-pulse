"""
SV Dugout Pulse — Configuration
"""

import logging
import os

# ---------------------------------------------------------------------------
# Google Sheet "Publish to Web" CSV URL
# Set via environment variable or replace the default with your URL.
# In Google Sheets: File -> Share -> Publish to Web -> CSV format -> copy link
# ---------------------------------------------------------------------------
ROSTER_URL = os.environ.get(
    "ROSTER_URL",
    "https://docs.google.com/spreadsheets/d/e/2PACX-1vROl4fVdpx2LKElwbZ9kqtXsMY2CmiHn5Jsjn7R5NAyJ5rqpt-EG2JiRj5YExQ1Asi47PO8vEXbum-N/pub?output=csv",
)

# Recruits/Following sheet — players we're tracking but not yet clients
RECRUITS_URL = os.environ.get(
    "RECRUITS_URL",
    "https://docs.google.com/spreadsheets/d/e/2PACX-1vRz1OQbckLK52-dxY9YqRNL8U-BRS6p6ipVPCVnhe7W-Bpo2v2imyqaV3hoz0HKRjx6dz207LShEg6j/pub?output=csv",
)

# ---------------------------------------------------------------------------
# Column name mapping (Sheet header -> internal key)
# If a column is renamed in the Sheet, update ONLY here.
# ---------------------------------------------------------------------------
COLUMN_MAP = {
    "Player Name": "player_name",
    "MLB_ID": "mlb_id",
    "Org": "team",
    "Level": "level",
    "Position": "position",
    "Draft Class": "draft_class",
    "X Handle": "x_handle",
    "Tier": "roster_priority",  # 1-4 internal priority — NOT performance grade
    "State (High School)": "state",
    "State": "state",  # Recruits sheet uses "State" instead of "State (High School)"
    "IG Handle": "ig_handle",
    "DOB": "dob",
    "Age": "age",
}

# Levels to include (everything else is excluded)
INCLUDED_LEVELS = {"Pro", "NCAA", "HS"}

# ---------------------------------------------------------------------------
# Performance grade thresholds
# ---------------------------------------------------------------------------
HITTER_STANDOUT_HITS = 3
HITTER_GOOD_HITS = 2
PITCHER_STANDOUT_KS = 5
PITCHER_QS_IP = 6.0
PITCHER_QS_MAX_ER = 3
SLUMP_HITLESS_AB = 12  # 0-for-last-N triggers Soft Flag

# ---------------------------------------------------------------------------
# Window stats grade thresholds (7D/30D/Season aggregate views)
# ---------------------------------------------------------------------------
# Hitter grades based on OPS
WINDOW_HITTER_HOT_OPS = 1.000
WINDOW_HITTER_SOLID_OPS = 0.750
WINDOW_HITTER_QUIET_OPS = 0.550

# Pitcher grades based on ERA
WINDOW_PITCHER_HOT_ERA = 2.00
WINDOW_PITCHER_SOLID_ERA = 3.50
WINDOW_PITCHER_QUIET_ERA = 5.00

# Minimum sample sizes (show "--" if below threshold)
WINDOW_MIN_PA = {"7d": 1, "season": 1}
WINDOW_MIN_IP = {"7d": 0.1, "season": 0.1}

# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "current_pulse.json")

# Window stats output paths (7D = Pro only, Season = everyone)
WINDOW_7D_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "window_7d.json")
WINDOW_SEASON_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "window_season.json")
YESTERDAY_PULSE_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "yesterday_pulse.json")
NCAA_GAME_LOG_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "ncaa_game_log.json")
SCHOOL_LOOKUP_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "school_lookup.json")
SIDEARM_FOLDER_CACHE_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "sidearm_folder_cache.json")
SENT_ALERTS_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "sent_alerts.json")
ROSTER_CACHE_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "roster_cache.json")
HS_GAME_LOG_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "hs_game_log.json")

# ---------------------------------------------------------------------------
# High School Stats (Google Sheet export)
# ---------------------------------------------------------------------------
HS_STATS_XLSX_URL = (
    os.environ.get("HS_STATS_URL", "")
    or "https://docs.google.com/spreadsheets/d/1oKuxG0JxCoBJprCup1PGaAJFRluo4-qc10jOni2aG1s/export?format=xlsx"
)

# Name aliases for normalizing misspellings/variants in the HS sheet
HS_NAME_ALIASES = {
    "David Vargas (DH)": "David Vargas",
    "Trevor Condon (DH)": "Trevor Condon",
    "Grifin Loy": "Griffin Loy",
    "Bradedon Mackay": "Braedon Mackay",
    "Braedon Makay": "Braedon Mackay",
    "Hunter Wiecksowki": "Hunter Wieckowski",
}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
