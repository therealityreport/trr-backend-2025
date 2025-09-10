#!/usr/bin/env python3
"""
Append to ViableCast with revised filtering rules.

Revised Rules:
1. If TotalShows = 1 or 2 and TotalEpisodes = 0-7, don't add as viable
2. If TotalShows = 1 or more and TotalEpisodes > 7, add as viable  
3. If TotalEpisodes = 0 but TotalShows > 1, add (likely data retrieval issue)
4. If TotalShows = 1 and TotalEpisodes = 0, don't include
5. Reality shows only viable if appearing in 2+ shows with total episodes > 7
"""

import os
from typing import Dict, List, Set, Tuple, Optional
from collections import defaultdict
import gspread
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
load_dotenv()
SERVICE_KEY_PATH = "/Users/thomashulihan/Projects/TRR-Backend/keys/trr-backend-df2c438612e1.json"
SPREADSHEET_TITLE = "Realitease2025Data"

# Reality competition shows that need special handling
REALITY_SHOWS = {
    'big brother', 
    'the amazing race', 
    'building the band',
    'american idol', 
    'top chef', 
    'project runway all stars',
    'project runway allstars'  # Handle variant spelling
}

# ---------------------------------------------------------------------------
# Google Sheets bootstrap
# ---------------------------------------------------------------------------
def open_spreadsheet():
    gc = gspread.service_account(filename=SERVICE_KEY_PATH)
    try:
        ss = gc.open(SPREADSHEET_TITLE)
    except Exception:
        ssid = os.getenv("SPREADSHEET_ID", "").strip()
        if not ssid:
            raise RuntimeError("Could not open spreadsheet by title and SPREADSHEET_ID is not set.")
        ss = gc.open_by_key(ssid)
    return gc, ss

gc, spreadsheet = open_spreadsheet()
castinfo_ws = spreadsheet.worksheet("CastInfo")
viable_ws = spreadsheet.worksheet("ViableCast")
update_ws = spreadsheet.worksheet("UpdateInfo")
showinfo_ws = spreadsheet.worksheet("ShowInfo")

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------
def find_col(header, keys):
    """Find column index by matching header names."""
    norm = [h.strip().lower().replace(" ", "") for h in header]
    for key in keys:
        lk = key.lower().replace(" ", "")
        for i, h in enumerate(norm):
            if h == lk:
                return i
    return None

def is_reality_show_only(show_names):
    """Check if all shows for a person are reality competition shows."""
    if not show_names:
        return False
    
    for show in show_names:
        show_lower = show.lower().strip()
        # If any show is NOT a reality show, return False
        if not any(reality in show_lower for reality in REALITY_SHOWS):
            return False
    return True

def aggregate_person_data(rows):
    """
    For each person (by Cast IMDbID), aggregate:
    - Set of distinct Show IMDbIDs
    - Set of distinct ShowNames  
    - Total episodes across all shows
    - Per-show episode counts
    """
    per_person = defaultdict(lambda: {
        "show_ids": set(),
        "show_names": set(), 
        "total_episodes": 0,
        "show_episodes": {},  # show_id -> episode count
        "rows": []
    })
    
    for r in rows:
        cast_imdb = str(r.get("Cast IMDbID", "")).strip()
        show_imdb = str(r.get("Show IMDbID", r.get("ShowIMDbID", ""))).strip()
        show_name = str(r.get("ShowName", "")).strip()
        
        # Parse episodes - handle various formats
        eps_str = str(r.get("TotalEpisodes", r.get("EpisodeCount", "0"))).strip()
        try:
            eps = int(eps_str) if eps_str and eps_str != "" else 0
        except (ValueError, TypeError):
            eps = 0
        
        if cast_imdb:
            if show_imdb:
                per_person[cast_imdb]["show_ids"].add(show_imdb)
                per_person[cast_imdb]["show_episodes"][show_imdb] = eps
            if show_name:
                per_person[cast_imdb]["show_names"].add(show_name)
            per_person[cast_imdb]["total_episodes"] += eps
            per_person[cast_imdb]["rows"].append(r)
    
    return per_person

def is_eligible(cast_imdb, person_data):
    """
    Determine if a person is eligible based on revised rules.
    
    Returns: (is_eligible: bool, reason: str)
    """
    total_shows = len(person_data["show_ids"])
    total_eps = person_data["total_episodes"]
    show_names = list(person_data["show_names"])
    
    # Rule: If only 1 show and 0 episodes, not eligible
    if total_shows == 1 and total_eps == 0:
        return False, f"1 show with 0 episodes"
    
    # Rule: If 1-2 shows with 1-7 episodes, not eligible
    if total_shows in [1, 2] and 1 <= total_eps <= 7:
        return False, f"{total_shows} show(s) with {total_eps} episodes (1-7 range)"
    
    # Rule: Reality show participants need 2+ shows
    if is_reality_show_only(show_names):
        if total_shows < 2:
            return False, f"Reality show participant with only {total_shows} show"
        if total_shows >= 2 and total_eps <= 7:
            return False, f"Reality show participant with {total_shows} shows but only {total_eps} episodes"
    
    # Rule: If episodes > 7, eligible
    if total_eps > 7:
        return True, f"Has {total_eps} episodes (>7)"
    
    # Rule: If episodes = 0 but multiple shows, likely data issue - include
    if total_eps == 0 and total_shows > 1:
        return True, f"Multiple shows ({total_shows}) with 0 episodes - likely data retrieval issue"
    
    # Default case
    return True, f"Default eligible: {total_shows} shows, {total_eps} episodes"

def process_sheet_data(worksheet, sheet_name):
    """Process data from a worksheet into dictionaries."""
    vals = worksheet.get_all_values()
    if not vals:
        print(f"❌ No {sheet_name} data found.")
        return []
    
    headers = [h.strip() for h in vals[0]]
    rows = []
    for row in vals[1:]:
        row_dict = {}
        for i, header in enumerate(headers):
            if i < len(row):
                row_dict[header] = row[i]
            else:
                row_dict[header] = ""
        rows.append(row_dict)
    
    print(f"Loaded {len(rows)} rows from {sheet_name}")
    return rows

def append_new_viablecast_rows():
    """Main function to append new viable cast rows from both CastInfo and UpdateInfo."""
    
    print("=" * 60)
    print("Starting ViableCast append with revised filtering rules")
    print("=" * 60)
    
    # Get existing (Cast IMDbID, Show IMDbID) pairs to avoid duplicates
    existing_pairs = set()
    viable_vals = viable_ws.get_all_values()
    
    if viable_vals:
        vh = [h.strip() for h in viable_vals[0]]
        v_show_imdb_i = find_col(vh, ["Show IMDbID"]) or 0
        v_cast_imdb_i = find_col(vh, ["Cast IMDbID"]) or 3
        
        for row in viable_vals[1:]:
            if len(row) > max(v_show_imdb_i, v_cast_imdb_i):
                show_id = row[v_show_imdb_i].strip()
                cast_imdb = row[v_cast_imdb_i].strip()
                if show_id and cast_imdb:
                    existing_pairs.add((cast_imdb, show_id))
    
    print(f"Found {len(existing_pairs)} existing pairs in ViableCast")
    
    # Process both CastInfo and UpdateInfo sheets
    all_rows = []
    
    # Read CastInfo data
    cast_rows = process_sheet_data(castinfo_ws, "CastInfo")
    all_rows.extend(cast_rows)
    
    # Read UpdateInfo data
    update_rows = process_sheet_data(update_ws, "UpdateInfo")
    
    # UpdateInfo might have different column names, so we need to map them
    for row in update_rows:
        mapped_row = {}
        # Map UpdateInfo columns to standard names - handle both possible column name formats
        mapped_row["Cast IMDbID"] = row.get("Cast IMDbID", "")
        mapped_row["Show IMDbID"] = row.get("ShowIMDbID", row.get("Show IMDbID", ""))
        mapped_row["TMDb CastID"] = row.get("TMDb CastID", row.get("CastID", ""))
        mapped_row["CastName"] = row.get("CastName", "")
        mapped_row["TMDb ShowID"] = row.get("TMDb ShowID", row.get("ShowID", ""))
        mapped_row["ShowName"] = row.get("ShowName", "")
        mapped_row["TotalEpisodes"] = row.get("TotalEpisodes", row.get("EpisodeCount", "0"))
        all_rows.append(mapped_row)
    
    print(f"Total rows to process: {len(all_rows)} (CastInfo + UpdateInfo)")
    
    # Aggregate data per person from all sources
    per_person = aggregate_person_data(all_rows)
    print(f"Found {len(per_person)} unique cast members across all sources")
    
    # Statistics
    stats = {
        "eligible": 0,
        "ineligible": 0,
        "already_exists": 0,
        "missing_data": 0,
        "reality_filtered": 0,
        "low_episodes": 0
    }
    
    # Prepare rows to append
    to_append = []
    ineligible_reasons = []
    
    for cast_imdb, person_data in per_person.items():
        if not cast_imdb:
            stats["missing_data"] += 1
            continue
        
        # Check eligibility
        eligible, reason = is_eligible(cast_imdb, person_data)
        
        if not eligible:
            stats["ineligible"] += 1
            if "Reality show" in reason:
                stats["reality_filtered"] += 1
            elif "episodes" in reason and "1-7" in reason:
                stats["low_episodes"] += 1
            ineligible_reasons.append((cast_imdb, reason))
            continue
        
        stats["eligible"] += 1
        
        # Process each show for this eligible person
        for r in person_data["rows"]:
            show_imdb = str(r.get("Show IMDbID", r.get("ShowIMDbID", ""))).strip()
            cast_tmdb = str(r.get("TMDb CastID", r.get("CastID", ""))).strip()
            cast_name = str(r.get("CastName", "")).strip()
            cast_imdb_check = str(r.get("Cast IMDbID", "")).strip()
            show_tmdb = str(r.get("TMDb ShowID", r.get("ShowID", ""))).strip()
            show_name = str(r.get("ShowName", "")).strip()
            
            if not show_imdb or not cast_imdb_check:
                continue
            
            # Check for duplicates
            pair_key = (cast_imdb, show_imdb)
            if pair_key in existing_pairs:
                stats["already_exists"] += 1
                continue
            
            # Add to append list (columns A-F only)
            to_append.append([
                show_imdb,      # A: Show IMDbID
                cast_tmdb,      # B: TMDb CastID
                cast_name,      # C: CastName
                cast_imdb,      # D: Cast IMDbID
                show_tmdb,      # E: TMDb ShowID
                show_name       # F: ShowName
            ])
            
            # Mark as added to prevent future duplicates
            existing_pairs.add(pair_key)
    
    # Print statistics
    print("\n" + "=" * 60)
    print("FILTERING STATISTICS:")
    print("=" * 60)
    print(f"✅ Eligible cast members: {stats['eligible']}")
    print(f"❌ Ineligible cast members: {stats['ineligible']}")
    print(f"   - Reality show filtered: {stats['reality_filtered']}")
    print(f"   - Low episode count (1-7): {stats['low_episodes']}")
    print(f"📋 Already in ViableCast: {stats['already_exists']}")
    print(f"⚠️  Missing required data: {stats['missing_data']}")
    
    # Show sample of filtered out entries
    if ineligible_reasons:
        print("\n" + "=" * 60)
        print("SAMPLE OF FILTERED OUT ENTRIES (first 10):")
        print("=" * 60)
        for cast_id, reason in ineligible_reasons[:10]:
            print(f"  {cast_id}: {reason}")
    
    # Append new rows if any
    if not to_append:
        print("\n✅ No new rows to append. ViableCast is up to date.")
        return
    
    print(f"\n📝 Appending {len(to_append)} new rows to ViableCast...")
    
    # Batch append
    batch_size = 1000
    total_appended = 0
    
    for i in range(0, len(to_append), batch_size):
        chunk = to_append[i:i + batch_size]
        try:
            viable_ws.append_rows(values=chunk, value_input_option="RAW", table_range="A1:F1")
            total_appended += len(chunk)
            print(f"   Batch {i//batch_size + 1}: Added {len(chunk)} rows")
        except Exception as e:
            print(f"❌ Error appending batch {i//batch_size + 1}: {e}")
    
    print(f"\n✅ Successfully appended {total_appended} rows to ViableCast")

def backfill_viablecast_data(castinfo_ws, viable_ws, update_ws, showinfo_ws):
    """Backfill missing TMDb ShowID and ShowName in ViableCast from CastInfo."""
    
    print("\n" + "=" * 60)
    print("Starting ViableCast backfill process")
    print("=" * 60)
    
    def find_col(header, keys):
        norm = [h.strip().lower().replace(" ", "") for h in header]
        for key in keys:
            lk = key.lower().replace(" ", "")
            for i, h in enumerate(norm):
                if h == lk:
                    return i
        return None
    
    # Build mapping from CastInfo: Show IMDbID -> (TMDb ShowID, ShowName)
    castinfo_map = {}
    cast_vals = castinfo_ws.get_all_values()
    if cast_vals:
        ch = [h.strip() for h in cast_vals[0]]
        ci_show_imdb_i = find_col(ch, ["Show IMDbID"]) or 0
        ci_tmdb_showid_i = find_col(ch, ["TMDb ShowID"]) or 3
        ci_showname_i = find_col(ch, ["ShowName"]) or 5
        for row in cast_vals[1:]:
            if len(row) <= max(ci_show_imdb_i, ci_tmdb_showid_i, ci_showname_i):
                row = row + [""] * (max(ci_show_imdb_i, ci_tmdb_showid_i, ci_showname_i) + 1 - len(row))
            show_imdb = row[ci_show_imdb_i].strip()
            tmdb_showid = row[ci_tmdb_showid_i].strip() if ci_tmdb_showid_i < len(row) else ""
            showname = row[ci_showname_i].strip() if ci_showname_i < len(row) else ""
            if show_imdb:
                castinfo_map[show_imdb] = (tmdb_showid, showname)
    
    # Build UpdateInfo mapping for CastID -> Cast IMDbID
    update_castid_to_imdb = {}
    update_vals = update_ws.get_all_values()
    if update_vals:
        uh = [h.strip() for h in update_vals[0]]
        u_cast_imdb_i = find_col(uh, ["Cast IMDbID"])
        u_castid_i = find_col(uh, ["TMDb CastID", "CastID"])
        if u_cast_imdb_i is not None and u_castid_i is not None:
            for row in update_vals[1:]:
                if len(row) <= max(u_cast_imdb_i, u_castid_i):
                    row = row + [""] * (max(u_cast_imdb_i, u_castid_i) + 1 - len(row))
                cast_imdb_val = row[u_cast_imdb_i].strip()
                castid_val = row[u_castid_i].strip()
                if castid_val and cast_imdb_val:
                    update_castid_to_imdb[castid_val] = cast_imdb_val
    
    # Process ViableCast for backfilling
    viable_vals = viable_ws.get_all_values()
    backfilled_cast_imdb = 0
    backfilled_tmdb_showid = 0
    backfilled_showname = 0
    
    if viable_vals:
        vh = [h.strip() for h in viable_vals[0]]
        v_show_imdb_i = find_col(vh, ["Show IMDbID"]) or 0   # A
        v_castid_i = find_col(vh, ["TMDb CastID"]) or 1      # B
        v_cast_imdb_i = find_col(vh, ["Cast IMDbID"]) or 3   # D
        v_tmdb_showid_i = find_col(vh, ["TMDb ShowID"]) or 4 # E
        v_showname_i = find_col(vh, ["ShowName"]) or 5       # F
        
        rows_to_update = []
        for r_idx, row in enumerate(viable_vals[1:], start=2):
            max_i = max(v_show_imdb_i, v_castid_i, v_cast_imdb_i, v_tmdb_showid_i, v_showname_i)
            if len(row) <= max_i:
                row = row + [""] * (max_i + 1 - len(row))
            
            show_imdb_val = row[v_show_imdb_i].strip()
            changed = False
            
            # Backfill Cast IMDbID using UpdateInfo mapping via CastID
            if not row[v_cast_imdb_i].strip():
                castid_val = row[v_castid_i].strip()
                if castid_val and castid_val in update_castid_to_imdb:
                    row[v_cast_imdb_i] = update_castid_to_imdb[castid_val]
                    backfilled_cast_imdb += 1
                    changed = True
            
            # Backfill TMDb ShowID from CastInfo if missing
            if not row[v_tmdb_showid_i].strip():
                if show_imdb_val and show_imdb_val in castinfo_map:
                    tmdb_showid, _ = castinfo_map[show_imdb_val]
                    if tmdb_showid:
                        row[v_tmdb_showid_i] = tmdb_showid
                        backfilled_tmdb_showid += 1
                        changed = True
            
            # Backfill ShowName from CastInfo if missing
            if not row[v_showname_i].strip():
                if show_imdb_val and show_imdb_val in castinfo_map:
                    _, showname = castinfo_map[show_imdb_val]
                    if showname:
                        row[v_showname_i] = showname
                        backfilled_showname += 1
                        changed = True
            
            if changed:
                rows_to_update.append((r_idx, row))
        
        # Batch update to ViableCast
        if rows_to_update:
            batch_size = 500
            for i in range(0, len(rows_to_update), batch_size):
                chunk = rows_to_update[i:i + batch_size]
                start_row = chunk[0][0]
                end_row = chunk[-1][0]
                update_range = f"A{start_row}:F{end_row}"
                data = [r[1][:6] for r in chunk]  # Only update columns A-F
                try:
                    viable_ws.update(range_name=update_range, values=data)
                except Exception as e:
                    print(f"Error backfilling ViableCast rows {start_row}-{end_row}: {e}")
    
    print(f"🔧 Backfilled Cast IMDbID in {backfilled_cast_imdb} ViableCast rows.")
    print(f"🔧 Backfilled TMDb ShowID in {backfilled_tmdb_showid} ViableCast rows.")
    print(f"🔧 Backfilled ShowName in {backfilled_showname} ViableCast rows.")

if __name__ == "__main__":
    # Run the main append process
    append_new_viablecast_rows()
    
    # Run backfill to fill in missing data
    backfill_viablecast_data(castinfo_ws, viable_ws, update_ws, showinfo_ws)