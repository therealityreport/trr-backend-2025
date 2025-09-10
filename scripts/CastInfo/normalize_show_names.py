#!/usr/bin/env python3
"""
Normalize Show Names in CastInfo and UpdateInfo sheets

Replaces ShowName values with the official ShowName from ShowInfo based on Show IMDbID.
Logs warnings for missing Show IMDbIDs in ShowInfo.

"""

import os
from collections import defaultdict
from typing import Dict, List

import gspread
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Google Sheets bootstrap
# ---------------------------------------------------------------------------
load_dotenv()

SERVICE_KEY_PATH = \
    "/Users/thomashulihan/Projects/TRR-Backend/keys/trr-backend-df2c438612e1.json"
SPREADSHEET_TITLE = "Realitease2025Data"


def open_spreadsheet():
    gc = gspread.service_account(filename=SERVICE_KEY_PATH)
    try:
        ss = gc.open(SPREADSHEET_TITLE)
    except Exception:
        ssid = os.getenv("SPREADSHEET_ID", "").strip()
        if not ssid:
            raise RuntimeError(
                "Could not open spreadsheet by title and SPREADSHEET_ID is not set.")
        ss = gc.open_by_key(ssid)
    return gc, ss


gc, spreadsheet = open_spreadsheet()

# Worksheets
showinfo_ws = spreadsheet.worksheet("ShowInfo")
castinfo_ws = spreadsheet.worksheet("CastInfo")
update_ws = spreadsheet.worksheet("UpdateInfo")

# ---------------------------------------------------------------------------
# Helpers to read whole sheets safely
# ---------------------------------------------------------------------------

def read_values(ws) -> List[List[str]]:
    try:
        vals = ws.get_all_values()
        return vals if vals else []
    except Exception:
        return []

def header_index(header: List[str], name: str, fallback_idx: int) -> int:
    try:
        return header.index(name)
    except ValueError:
        lname = name.lower().replace(" ", "")
        for i, h in enumerate(header):
            if h.lower().replace(" ", "") == lname:
                return i
        return fallback_idx

# ---------------------------------------------------------------------------
# Build ShowInfo mapping: IMDbSeriesID (col F) -> ShowName (col B)
# ---------------------------------------------------------------------------
show_vals = read_values(showinfo_ws)
show_title_map: Dict[str, str] = {}
if show_vals:
    sh = [h.strip() for h in show_vals[0]]
    imdb_series_i = header_index(sh, "IMDbSeriesID", 5)
    showname_i = header_index(sh, "ShowName", 1)
    for row in show_vals[1:]:
        if len(row) <= max(imdb_series_i, showname_i):
            continue
        imdb_series_id = row[imdb_series_i].strip()
        show_name = row[showname_i].strip()
        if imdb_series_id and show_name:
            show_title_map[imdb_series_id] = show_name

print(f"Loaded {len(show_title_map)} ShowInfo IMDbSeriesID → ShowName mappings.")

# ---------------------------------------------------------------------------
# Normalize CastInfo ShowName (col F) using ShowInfo mapping keyed by Show IMDbID (col A)
# ---------------------------------------------------------------------------
cast_vals = read_values(castinfo_ws)
if not cast_vals:
    print("CastInfo sheet is empty or could not be read.")
    cast_vals = []

if cast_vals:
    ch = [h.strip() for h in cast_vals[0]]
    cast_show_imdb_i = header_index(ch, "Show IMDbID", 0)
    cast_showname_i = header_index(ch, "ShowName", 5)

    rows_to_update = []
    for r_idx, row in enumerate(cast_vals[1:], start=2):
        if len(row) <= max(cast_show_imdb_i, cast_showname_i):
            # Pad row if too short
            while len(row) <= max(cast_show_imdb_i, cast_showname_i):
                row.append("")
        show_imdb = row[cast_show_imdb_i].strip()
        current_showname = row[cast_showname_i].strip()
        official_showname = show_title_map.get(show_imdb, "")
        if not official_showname:
            print(f"⚠️ CastInfo row {r_idx}: Show IMDbID '{show_imdb}' not found in ShowInfo.")
            continue
        if current_showname != official_showname:
            row[cast_showname_i] = official_showname
            rows_to_update.append((r_idx, row))

    # Batch update CastInfo in chunks of 500 rows
    batch_size = 500
    for i in range(0, len(rows_to_update), batch_size):
        chunk = rows_to_update[i:i+batch_size]
        # Prepare data and range
        start_row = chunk[0][0]
        end_row = chunk[-1][0]
        update_range = f"A{start_row}:Z{end_row}"  # Z is arbitrary large enough column
        # Extract rows only
        data = [r[1] for r in chunk]
        try:
            castinfo_ws.update(update_range, data)
        except Exception as e:
            print(f"Error updating CastInfo rows {start_row}-{end_row}: {e}")

    print(f"Updated {len(rows_to_update)} rows in CastInfo with official ShowName.")

# ---------------------------------------------------------------------------
# Normalize UpdateInfo ShowName (col B) using ShowInfo mapping keyed by Show IMDbID (col F)
# ---------------------------------------------------------------------------
update_vals = read_values(update_ws)
if not update_vals:
    print("UpdateInfo sheet is empty or could not be read.")
    update_vals = []

if update_vals:
    uh = [h.strip() for h in update_vals[0]]
    update_showname_i = header_index(uh, "ShowName", 1)
    update_showimdb_i = header_index(uh, "ShowIMDbID", 5)

    rows_to_update = []
    for r_idx, row in enumerate(update_vals[1:], start=2):
        if len(row) <= max(update_showname_i, update_showimdb_i):
            # Pad row if too short
            while len(row) <= max(update_showname_i, update_showimdb_i):
                row.append("")
        show_imdb = row[update_showimdb_i].strip()
        current_showname = row[update_showname_i].strip()
        official_showname = show_title_map.get(show_imdb, "")
        if not official_showname:
            print(f"⚠️ UpdateInfo row {r_idx}: Show IMDbID '{show_imdb}' not found in ShowInfo.")
            continue
        if current_showname != official_showname:
            row[update_showname_i] = official_showname
            rows_to_update.append((r_idx, row))

    # Batch update UpdateInfo in chunks of 500 rows
    batch_size = 500
    for i in range(0, len(rows_to_update), batch_size):
        chunk = rows_to_update[i:i+batch_size]
        start_row = chunk[0][0]
        end_row = chunk[-1][0]
        update_range = f"A{start_row}:Z{end_row}"
        data = [r[1] for r in chunk]
        try:
            update_ws.update(update_range, data)
        except Exception as e:
            print(f"Error updating UpdateInfo rows {start_row}-{end_row}: {e}")

    print(f"Updated {len(rows_to_update)} rows in UpdateInfo with official ShowName.")