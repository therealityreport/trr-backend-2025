#!/usr/bin/env python3
"""
Append-only builder for ViableCast.

- Reads CastInfo and aggregates per person to decide eligibility.
- Appends NEW rows (A–F only) to ViableCast; never reorders or touches G–H.
- If an existing ViableCast row has a blank/mismatched Cast IMDbID (col D),
  fixes just that cell using UpdateInfo as ground truth.
- Uses the official IMDb show title from ShowInfo for ShowName.
"""

import os
import time
from collections import defaultdict

import gspread
from dotenv import load_dotenv

load_dotenv()

SPREADSHEET_NAME = os.getenv("SPREADSHEET_NAME", "Realitease2025Data")
SA_KEYFILE = os.getenv(
    "GOOGLE_APPLICATION_CREDENTIALS",
    "keys/trr-backend-df2c438612e1.json"
)

# ---------- Helpers ----------

def gc_open():
    gc = gspread.service_account(filename=SA_KEYFILE)
    sh = gc.open(SPREADSHEET_NAME)
    return gc, sh

def get_sheet(sh, title):
    return sh.worksheet(title)

def to_int(x, default=0):
    try:
        if x is None or str(x).strip() == "":
            return default
        return int(str(x).strip())
    except Exception:
        return default

def csv_split(s):
    if not s:
        return []
    return [p.strip() for p in str(s).split(",") if p and str(p).strip()]

# ---------- Load lookups ----------

def load_show_titles(show_ws):
    """
    Build Show IMDbID -> Official IMDb Title (prefer ShowInfo's title).
    Columns can vary; try common names.
    """
    rows = show_ws.get_all_records()
    title_map = {}
    for r in rows:
        imdb = str(r.get("Show IMDbID", "")).strip()
        # Try multiple likely header names for title
        title = (
            str(r.get("ShowName", "")).strip()
            or str(r.get("Show Title", "")).strip()
            or str(r.get("Title", "")).strip()
        )
        if imdb:
            if title:  # take first non-empty
                title_map.setdefault(imdb, title)
    return title_map

def load_updateinfo_id_map(update_ws):
    """
    Build TMDb Person ID -> IMDb Person ID from UpdateInfo.
    UpdateInfo.PersonTMDbID may have multiple TMDb IDs comma-separated.
    """
    rows = update_ws.get_all_records()
    tmdb_to_imdb = {}
    for r in rows:
        imdb_id = str(r.get("PersonIMDbID", "")).strip()
        tmdb_csv = str(r.get("PersonTMDbID", "")).strip()
        for tid in csv_split(tmdb_csv):
            if tid and imdb_id:
                tmdb_to_imdb.setdefault(tid, imdb_id)
    return tmdb_to_imdb

def load_viable_pairs_and_rows(viable_ws):
    """
    Return:
      - existing_pairs: set of (cast_imdb_id, show_imdb_id)
      - row_index_for_pair: dict -> row number (1-based)
    Also capture rows missing D to patch.
    """
    values = viable_ws.get_all_values()
    existing_pairs = set()
    row_index_for_pair = {}
    # Header indices (A..)
    # A Show IMDbID, B CastID, C CastName, D Cast IMDbID, E ShowID, F ShowName
    for i, row in enumerate(values[1:], start=2):
        show_imdb = row[0].strip() if len(row) > 0 else ""
        cast_imdb = row[3].strip() if len(row) > 3 else ""
        if show_imdb:
            key = (cast_imdb, show_imdb)
            row_index_for_pair[key] = i
            if cast_imdb:
                existing_pairs.add((cast_imdb, show_imdb))
    return existing_pairs, row_index_for_pair, len(values) + 1  # next empty row if appending whole rows

def aggregate_person_counts(cast_rows):
    """
    For each person (by Cast IMDbID), compute:
      - TotalShows (distinct Show IMDbIDs)
      - TotalEpisodes (sum of TotalEpisodes from CastInfo, treat blank as 0)
    Also keep per-row detail so we can decide per-row append.
    """
    per_person = defaultdict(lambda: {"shows": set(), "episodes": 0})
    for r in cast_rows:
        cast_imdb = str(r.get("Cast IMDbID", "")).strip()
        show_imdb = str(r.get("Show IMDbID", "")).strip()
        eps = to_int(r.get("TotalEpisodes", 0), 0)
        if cast_imdb:
            if show_imdb:
                per_person[cast_imdb]["shows"].add(show_imdb)
            per_person[cast_imdb]["episodes"] += eps
    return per_person

def eligible_to_add(total_shows, total_eps):
    """
    Implement the include/skip rules.
    """
    if total_eps == 0:
        return True
    if total_eps > 7:
        return True
    if total_shows == 1 and 1 <= total_eps <= 7:
        return False
    if total_shows == 2 and 2 <= total_eps <= 7:
        return False
    # Everything else falls back to the permissive rule set (add)
    return True

# ---------- Main work ----------

def main():
    print("🚀 Append-only ViableCast updater")
    gc, sh = gc_open()

    cast_ws = get_sheet(sh, "CastInfo")
    update_ws = get_sheet(sh, "UpdateInfo")
    show_ws = get_sheet(sh, "ShowInfo")
    viable_ws = get_sheet(sh, "ViableCast")

    print("🔄 Loading lookups…")
    tmdb_to_imdb = load_updateinfo_id_map(update_ws)
    show_title_map = load_show_titles(show_ws)
    print(f"  • UpdateInfo TMDb→IMDb mappings: {len(tmdb_to_imdb)}")
    print(f"  • ShowInfo titles loaded: {len(show_title_map)}")

    print("📥 Reading ViableCast to build existing pair set…")
    existing_pairs, pair_row_map, _ = load_viable_pairs_and_rows(viable_ws)
    print(f"  • Existing (CastIMDbID, ShowIMDbID) pairs: {len(existing_pairs)}")

    print("📥 Reading CastInfo source rows…")
    cast_rows = cast_ws.get_all_records()
    print(f"  • CastInfo rows: {len(cast_rows)}")

    print("📊 Aggregating per-person totals…")
    per_person = aggregate_person_counts(cast_rows)
    print(f"  • Unique people in CastInfo: {len(per_person)}")

    # First, patch existing ViableCast column D if blank or mismatched
    print("🧩 Checking existing ViableCast rows for missing/mismatched Cast IMDbID…")
    patches = []
    values = viable_ws.get_all_values()
    headers = values[0] if values else []
    # Build a quick helper for current sheet values
    for i, row in enumerate(values[1:], start=2):
        show_imdb = row[0].strip() if len(row) > 0 else ""
        cast_tmdb = row[1].strip() if len(row) > 1 else ""
        cast_name = row[2].strip() if len(row) > 2 else ""
        cast_imdb_current = row[3].strip() if len(row) > 3 else ""

        # If we can infer the correct IMDb from UpdateInfo by TMDb
        imdb_from_updateinfo = tmdb_to_imdb.get(cast_tmdb, "")

        needs_patch = False
        new_value = cast_imdb_current
        if imdb_from_updateinfo:
            if not cast_imdb_current:
                needs_patch = True
                new_value = imdb_from_updateinfo
            elif cast_imdb_current != imdb_from_updateinfo:
                needs_patch = True
                new_value = imdb_from_updateinfo

        if needs_patch:
            patches.append((i, 4, new_value))  # row i, column D (4)
    if patches:
        print(f"  • Patching {len(patches)} existing Cast IMDbIDs in ViableCast (col D)…")
        # Batch update with a single range write per Google Sheets best practice
        for (row_i, col_j, val) in patches:
            viable_ws.update_cell(row_i, col_j, val)
            time.sleep(0.1)  # gentle pacing
    else:
        print("  • No patches needed.")

    # Build rows to append (A–F only)
    print("🧮 Selecting new rows to append (A–F only)…")
    to_append = []
    for r in cast_rows:
        show_imdb = str(r.get("Show IMDbID", "")).strip()
        cast_tmdb = str(r.get("TMDb CastID", "")).strip()  # Fixed: was "CastID"
        cast_name = str(r.get("CastName", "")).strip()
        cast_imdb = str(r.get("Cast IMDbID", "")).strip()
        show_tmdb = str(r.get("TMDb ShowID", "")).strip()  # Fixed: was "ShowID"

        if not (show_imdb and cast_name):
            continue

        # Determine totals for this person
        totals = per_person.get(cast_imdb or "", {"shows": set(), "episodes": 0})
        total_shows = len(totals["shows"])
        total_eps = totals["episodes"]

        # Decide eligibility
        if not eligible_to_add(total_shows, total_eps):
            continue

        # If Cast IMDbID is missing in the source row, try to fill from UpdateInfo (via TMDb)
        if not cast_imdb and cast_tmdb in tmdb_to_imdb:
            cast_imdb = tmdb_to_imdb[cast_tmdb]

        # Skip if we still can’t identify the person
        if not cast_imdb:
            continue

        # De-dupe against existing ViableCast
        pair_key = (cast_imdb, show_imdb)
        if pair_key in existing_pairs:
            # Already present — we do nothing (we already patched D above if needed)
            continue

        # ShowName: use IMDb official title if available
        show_name = show_title_map.get(show_imdb) or str(r.get("ShowName", "")).strip()

        # A–F only
        to_append.append([
            show_imdb,      # A Show IMDbID
            cast_tmdb,      # B CastID (TMDb person)
            cast_name,      # C CastName
            cast_imdb,      # D Cast IMDbID
            show_tmdb,      # E ShowID (TMDb show)
            show_name       # F ShowName (official if available)
        ])

    if not to_append:
        print("✅ No new rows to append. ViableCast is up to date per current rules.")
        return

    # Append at bottom without touching existing order or columns G–H
    first_empty_row = len(values) + 1 if values else 2
    start_row = first_empty_row
    end_row = start_row + len(to_append) - 1
    rng = f"A{start_row}:F{end_row}"
    print(f"📝 Appending {len(to_append)} new rows to ViableCast at {rng} …")
    viable_ws.update(rng, to_append)
    print("✅ Append complete (A–F only). Columns G–H left untouched.")

if __name__ == "__main__":
    main()