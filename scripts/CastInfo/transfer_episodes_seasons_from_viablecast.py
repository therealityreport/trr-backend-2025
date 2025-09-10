#!/usr/bin/env python3
"""
Transfer Episodes and Seasons from ViableCast to CastInfo
=========================================================

This script copies EpisodeCount and Seasons data from ViableCast sheet to CastInfo sheet
by matching on Cast IMDbID and Show IMDbID.

ViableCast Headers: Show IMDbID, TMDb CastID, CastName, Cast IMDbID, TMDb ShowID, ShowName, EpisodeCount, Seasons
CastInfo Headers:   CastName, TMDb CastID, Cast IMDbID, ShowName, Show IMDbID, TMDb ShowID, TotalEpisodes, Seasons

Matching Logic: Cast IMDbID (col D in ViableCast, col C in CastInfo) + Show IMDbID (col A in ViableCast, col E in CastInfo)
Transfer Data:  EpisodeCount (col G in ViableCast) -> TotalEpisodes (col G in CastInfo)
               Seasons (col H in ViableCast) -> Seasons (col H in CastInfo)
"""

import gspread
import time

# Google Sheets setup
SERVICE_ACCOUNT_FILE = '/Users/thomashulihan/Projects/TRR-Backend/keys/trr-backend-df2c438612e1.json'

def setup_gsheets():
    """Initialize Google Sheets client and open workbook"""
    gc = gspread.service_account(filename=SERVICE_ACCOUNT_FILE)
    workbook = gc.open("Realitease2025Data")
    return workbook

def load_viablecast_data(workbook):
    """Load ViableCast data and create lookup dictionary"""
    print("📊 Loading ViableCast data...")
    
    viablecast_sheet = workbook.worksheet("ViableCast")
    all_values = viablecast_sheet.get_all_values()
    
    if not all_values:
        raise ValueError("ViableCast sheet is empty")
    
    headers = all_values[0]
    print(f"ViableCast headers: {headers}")
    
    # Expected: Show IMDbID, TMDb CastID, CastName, Cast IMDbID, TMDb ShowID, ShowName, EpisodeCount, Seasons
    show_imdb_col = 0    # A: Show IMDbID
    cast_imdb_col = 3    # D: Cast IMDbID  
    episode_count_col = 6 # G: EpisodeCount
    seasons_col = 7      # H: Seasons
    
    # Create lookup dictionary: (cast_imdb_id, show_imdb_id) -> (episodes, seasons)
    lookup = {}
    
    for i, row in enumerate(all_values[1:], start=2):  # Skip header, start numbering from 2
        if len(row) <= max(show_imdb_col, cast_imdb_col, episode_count_col, seasons_col):
            continue
            
        show_imdb_id = row[show_imdb_col].strip()
        cast_imdb_id = row[cast_imdb_col].strip()
        episodes = row[episode_count_col].strip()
        seasons = row[seasons_col].strip()
        
        # Skip if missing key data
        if not show_imdb_id or not cast_imdb_id:
            continue
            
        # Create lookup key
        key = (cast_imdb_id, show_imdb_id)
        
        # Store the data (episodes, seasons)
        lookup[key] = (episodes, seasons)
    
    print(f"✅ Loaded {len(lookup)} records from ViableCast")
    return lookup

def load_castinfo_data(workbook):
    """Load CastInfo data for processing"""
    print("📊 Loading CastInfo data...")
    
    castinfo_sheet = workbook.worksheet("CastInfo")
    all_values = castinfo_sheet.get_all_values()
    
    if not all_values:
        raise ValueError("CastInfo sheet is empty")
    
    headers = all_values[0]
    print(f"CastInfo headers: {headers}")
    
    # Expected: CastName, TMDb CastID, Cast IMDbID, ShowName, Show IMDbID, TMDb ShowID, TotalEpisodes, Seasons
    cast_imdb_col = 2      # C: Cast IMDbID
    show_imdb_col = 4      # E: Show IMDbID
    episodes_col = 6       # G: TotalEpisodes
    seasons_col = 7        # H: Seasons
    
    print(f"✅ Loaded {len(all_values)-1} rows from CastInfo")
    return castinfo_sheet, all_values, cast_imdb_col, show_imdb_col, episodes_col, seasons_col

def load_viablecast_full_data(workbook):
    """Load complete ViableCast data for adding missing rows"""
    print("📊 Loading complete ViableCast data...")
    
    viablecast_sheet = workbook.worksheet("ViableCast")
    all_values = viablecast_sheet.get_all_values()
    
    if not all_values:
        raise ValueError("ViableCast sheet is empty")
    
    headers = all_values[0]
    print(f"ViableCast headers: {headers}")
    
    # Expected: Show IMDbID, TMDb CastID, CastName, Cast IMDbID, TMDb ShowID, ShowName, EpisodeCount, Seasons
    viable_cast_data = []
    
    for i, row in enumerate(all_values[1:], start=2):  # Skip header
        if len(row) < 8:
            continue
            
        show_imdb_id = row[0].strip()    # A: Show IMDbID
        tmdb_cast_id = row[1].strip()    # B: TMDb CastID
        cast_name = row[2].strip()       # C: CastName
        cast_imdb_id = row[3].strip()    # D: Cast IMDbID
        tmdb_show_id = row[4].strip()    # E: TMDb ShowID
        show_name = row[5].strip()       # F: ShowName
        episode_count = row[6].strip()   # G: EpisodeCount
        seasons = row[7].strip()         # H: Seasons
        
        # Skip if missing essential data
        if not cast_imdb_id or not show_imdb_id or not cast_name:
            continue
            
        viable_cast_data.append({
            'show_imdb_id': show_imdb_id,
            'tmdb_cast_id': tmdb_cast_id,
            'cast_name': cast_name,
            'cast_imdb_id': cast_imdb_id,
            'tmdb_show_id': tmdb_show_id,
            'show_name': show_name,
            'episode_count': episode_count,
            'seasons': seasons
        })
    
    print(f"✅ Loaded {len(viable_cast_data)} complete records from ViableCast")
    return viable_cast_data

def get_existing_castinfo_keys(workbook):
    """Get existing (cast_imdb_id, show_imdb_id) pairs from CastInfo"""
    print("📊 Loading existing CastInfo keys...")
    
    castinfo_sheet = workbook.worksheet("CastInfo")
    all_values = castinfo_sheet.get_all_values()
    
    if not all_values:
        return set()
    
    existing_keys = set()
    
    for row in all_values[1:]:  # Skip header
        if len(row) < 5:
            continue
            
        cast_imdb_id = row[2].strip() if len(row) > 2 else ""  # C: Cast IMDbID
        show_imdb_id = row[4].strip() if len(row) > 4 else ""  # E: Show IMDbID
        
        if cast_imdb_id and show_imdb_id:
            existing_keys.add((cast_imdb_id, show_imdb_id))
    
    print(f"✅ Found {len(existing_keys)} existing (cast, show) pairs in CastInfo")
    return existing_keys

def add_missing_rows(workbook):
    """Add missing rows from ViableCast to CastInfo"""
    
    # Load ViableCast data
    viable_cast_data = load_viablecast_full_data(workbook)
    
    # Get existing CastInfo keys
    existing_keys = get_existing_castinfo_keys(workbook)
    
    # Find missing rows
    missing_rows = []
    
    for data in viable_cast_data:
        key = (data['cast_imdb_id'], data['show_imdb_id'])
        
        if key not in existing_keys:
            # Build CastInfo row: CastName, TMDb CastID, Cast IMDbID, ShowName, Show IMDbID, TMDb ShowID, TotalEpisodes, Seasons
            new_row = [
                data['cast_name'],        # A: CastName
                data['tmdb_cast_id'],     # B: TMDb CastID  
                data['cast_imdb_id'],     # C: Cast IMDbID
                data['show_name'],        # D: ShowName
                data['show_imdb_id'],     # E: Show IMDbID
                data['tmdb_show_id'],     # F: TMDb ShowID
                data['episode_count'],    # G: TotalEpisodes
                data['seasons']           # H: Seasons
            ]
            missing_rows.append(new_row)
    
    print(f"\n📋 Found {len(missing_rows)} missing rows to add")
    
    if missing_rows:
        # Add to CastInfo sheet
        castinfo_sheet = workbook.worksheet("CastInfo")
        
        print(f"📤 Adding {len(missing_rows)} new rows to CastInfo...")
        
        # Add in batches
        BATCH_SIZE = 100
        for i in range(0, len(missing_rows), BATCH_SIZE):
            batch = missing_rows[i:i+BATCH_SIZE]
            castinfo_sheet.append_rows(batch)
            print(f"  ✅ Added batch {i//BATCH_SIZE + 1}: {len(batch)} rows")
            time.sleep(1)  # Rate limiting
        
        print(f"🎉 Successfully added {len(missing_rows)} new cast members to CastInfo!")
        
        # Show some examples
        print(f"\n� Sample additions:")
        for i, row in enumerate(missing_rows[:5]):
            print(f"  {i+1}. {row[0]} | {row[3]} | {row[6]} episodes")
        
        if len(missing_rows) > 5:
            print(f"  ... and {len(missing_rows) - 5} more")
    
    else:
        print("ℹ️  No missing rows found - all ViableCast entries already exist in CastInfo")

def transfer_data(workbook):
    """Add missing rows from ViableCast to CastInfo (no updates to existing rows)"""
    add_missing_rows(workbook)

def main():
    """Main function"""
    print("🚀 Adding missing ViableCast entries to CastInfo...")
    print("ℹ️  Note: This only adds missing rows, does not update existing ones")
    
    try:
        workbook = setup_gsheets()
        transfer_data(workbook)
        
    except Exception as e:
        print(f"❌ Error: {e}")
        raise

if __name__ == "__main__":
    main()
