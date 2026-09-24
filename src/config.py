"""
SV Dugout Pulse — Configuration
"""

import logging
import os

# ---------------------------------------------------------------------------
# Google Sheet "Publish to Web" CSV URLs — ONLY from the environment
# (GitHub Actions secrets ROSTER_URL / RECRUITS_URL). There is deliberately no
# default: this repo is public, and a published-CSV link in source is a public
# door to the sheet behind it.
# ---------------------------------------------------------------------------
ROSTER_URL = os.environ.get("ROSTER_URL", "")

# Recruits/Following sheet — players we're tracking but not yet clients.
# Recruits are NOT clients and are not in the registry projection: this list
# always comes from its own sheet, whatever ROSTER_SOURCE says.
RECRUITS_URL = os.environ.get("RECRUITS_URL", "")

# ---------------------------------------------------------------------------
# Client roster source switch (read at call time so tests / rollbacks work):
#   ROSTER_SOURCE=sheet     (default) master sheet published CSV (ROSTER_URL)
#   ROSTER_SOURCE=registry  sv-registry authenticated roster projection,
#                           GET $SV_REGISTRY_URL/api/roster-projection with the
#                           svt_ token in $SV_REGISTRY_ROSTER_TOKEN as Bearer.
# Rollback = set ROSTER_SOURCE back to "sheet". There is no automatic
# registry→sheet fallback.
# ---------------------------------------------------------------------------
DEFAULT_REGISTRY_URL = "https://sv-registry.vercel.app"
REGISTRY_PROJECTION_PATH = "/api/roster-projection"
REGISTRY_TOKEN_ENV = "SV_REGISTRY_ROSTER_TOKEN"
REGISTRY_MIN_ROWS = 40  # fewer rows than this = truncated/broken response → fail closed

# Canon position labels (sv-registry identity.position) → this app's
# Pitcher / Hitter / Two-Way routing (stats_engine._is_pitcher_pos etc.).
PITCHER_POSITION_LABELS = {
    "p", "pitcher", "rhp", "lhp", "sp", "rp", "cl", "rhsp", "lhsp", "rhrp", "lhrp",
}
TWO_WAY_POSITION_LABELS = {"two-way", "two way", "twp", "2-way"}

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
    "Affiliate": "affiliate",
    "Tier": "roster_priority",  # 1-4 internal priority — NOT performance grade
    # Sheet-side suppression switch: Status (or Org) = "Retired" removes a
    # player from the dashboard with no code change — see filter_roster().
    "Status": "status",
    "State (High School)": "state",
    "State": "state",  # Recruits sheet uses "State" instead of "State (High School)"
    "IG Handle": "ig_handle",
    # DOB / Age are deliberately NOT mapped: nothing here uses them, and the
    # roster cache is committed to this PUBLIC repo and served by Pages.
    # Scout the Statline peak projections — written to the roster sheet daily
    # by sv-scouting-data's roster-sync; Pro players with pro stats only.
    "Peak WAR": "peak_war",
    "Peak wRC+": "peak_wrc_plus",
    "Peak ERA (20 TBF)": "peak_era_20tbf",
}

# Levels to include (everything else is excluded)
INCLUDED_LEVELS = {"Pro", "NCAA", "HS"}

# The 30 MLB clubs, lowercased, plus naming variants. A drafted-but-unsigned
# client keeps Level=NCAA/HS on the master sheet but gets the drafting club in
# the Org column (sheet convention since the 2026 draft; school moves to
# "Amateur Org"). Any NCAA/HS row whose Org matches one of these is treated as
# Pro internally — see normalize_player().
MLB_CLUB_NAMES = {
    "arizona diamondbacks", "atlanta braves", "baltimore orioles",
    "boston red sox", "chicago cubs", "chicago white sox",
    "cincinnati reds", "cleveland guardians", "colorado rockies",
    "detroit tigers", "houston astros", "kansas city royals",
    "los angeles angels", "los angeles dodgers", "miami marlins",
    "milwaukee brewers", "minnesota twins", "new york mets",
    "new york yankees", "philadelphia phillies", "pittsburgh pirates",
    "san diego padres", "san francisco giants", "seattle mariners",
    "st. louis cardinals", "st louis cardinals", "tampa bay rays",
    "texas rangers", "toronto blue jays", "washington nationals",
    "athletics", "oakland athletics", "las vegas athletics",
}

# MLB IDs to always exclude (retired clients still on the master sheet, etc.)
# Why: Forrest Wall is retired but appeared with Iowa Cubs; keep him off the dashboard.
EXCLUDED_MLB_IDS = {
    657088,  # Forrest Wall — retired
}

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
WINDOW_MIN_PA = {"7d": 1, "month": 1, "season": 1}
WINDOW_MIN_IP = {"7d": 0.1, "month": 0.1, "season": 0.1}

# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "current_pulse.json")

# Window stats output paths. Rolling windows (7/14/30D) are most meaningful for
# Pro (MLB Stats API supports arbitrary date ranges); NCAA fills from its game
# log where available, Season from Baseball Reference / D1Baseball.
WINDOW_7D_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "window_7d.json")
WINDOW_14D_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "window_14d.json")
WINDOW_30D_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "window_30d.json")
WINDOW_SEASON_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "window_season.json")
YESTERDAY_PULSE_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "yesterday_pulse.json")
NCAA_GAME_LOG_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "ncaa_game_log.json")
SCHOOL_LOOKUP_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "school_lookup.json")
SIDEARM_FOLDER_CACHE_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "sidearm_folder_cache.json")
SENT_ALERTS_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "sent_alerts.json")
ROSTER_CACHE_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "roster_cache.json")

# The ONLY player fields persisted to data/roster_cache.json. That file is
# committed to this PUBLIC repo and served by GitHub Pages, so it is an
# allowlist, not a denylist: a new sheet column (or registry field) can never
# leak into it by default. Every field here is either already shown on the
# public dashboard or is an internal flag. Never add contact or personal data
# (DOB, age, phone, email, address, parents, social handles, home state).
ROSTER_CACHE_FIELDS = (
    "slug",
    "player_name",
    "mlb_id",
    "team",
    "level",
    "position",
    "draft_class",
    "affiliate",
    "roster_priority",
    "status",
    "is_client",
    "peak_war",
    "peak_wrc_plus",
    "peak_era_20tbf",
    "_api_team_applied",
)
HS_GAME_LOG_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "hs_game_log.json")
PLAYER_HEALTH_HISTORY_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "player_health_history.json")

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
    "Ax Westmoreland": "Axton Westmoreland",
    "Ty Howard": "Tyler Howard",
    "Lucas Lawerence": "Lucas Lawrence",
    "Kyle Rogossienski": "Kyle Rogosienski",
    "Alex Smith": "Alexander Smith",
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
