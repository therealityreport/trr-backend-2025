#!/usr/bin/env python3
"""
Build RealiteaseInfo sheet from ViableCast data.

This script aggregates unique cast members from ViableCast and creates/updates
the RealiteaseInfo sheet with comprehensive cast member information.

ViableCast headers: Show IMDbID, TMDb CastID, CastName, Cast IMDbID, TMDb ShowID, ShowName, EpisodeCount, Seasons
RealiteaseInfo headers: CastName, CastIMDbID, CastTMDbID, ShowNames, ShowIMDbIDs, ShowTMDbIDs, ShowCount, Gender, Birthday, Zodiac
"""

import os
import gspread
from dotenv import load_dotenv
from collections import defaultdict
import time

# Load environment variables
load_dotenv()

# Configuration
SPREADSHEET_NAME = os.getenv("SPREADSHEET_NAME", "Realitease2025Data")
SA_KEYFILE = os.getenv(
    "GOOGLE_APPLICATION_CREDENTIALS", 
    "../../keys/trr-backend-e16bfa49d861.json"
)

def connect_to_spreadsheet():
    """Connect to Google Sheets and return the spreadsheet object."""
    try:
        gc = gspread.service_account(filename=SA_KEYFILE)
        spreadsheet = gc.open(SPREADSHEET_NAME)
        return spreadsheet
    except Exception as e:
        print(f"❌ Error connecting to Google Sheets: {e}")
        return None

def load_viablecast_data(spreadsheet):
    """Load and process data from ViableCast sheet."""
    print("📊 Loading data from ViableCast sheet...")
    
    try:
        viable_cast_sheet = spreadsheet.worksheet("ViableCast")
        all_values = viable_cast_sheet.get_all_values()
        
        if not all_values:
            print("❌ ViableCast sheet is empty")
            return {}
        
        # ViableCast headers: Show IMDbID, TMDb CastID, CastName, Cast IMDbID, TMDb ShowID, ShowName, EpisodeCount, Seasons
        headers = all_values[0]
        print(f"📋 ViableCast headers: {headers}")
        
        # Expected column indices
        expected_headers = {
            'show_imdb_id': 0,    # Show IMDbID
            'tmdb_cast_id': 1,    # TMDb CastID  
            'cast_name': 2,       # CastName
            'cast_imdb_id': 3,    # Cast IMDbID
            'tmdb_show_id': 4,    # TMDb ShowID
            'show_name': 5,       # ShowName
            'episode_count': 6,   # EpisodeCount
            'seasons': 7          # Seasons
        }
        
        cast_data = defaultdict(lambda: {
            'cast_name': '',
            'cast_imdb_id': '',
            'cast_tmdb_id': '',
            'shows': [],
            'show_imdb_ids': [],
            'show_tmdb_ids': []
        })
        
        # Process each row (skip header)
        for row_idx, row in enumerate(all_values[1:], start=2):
            if len(row) < 8:  # Ensure we have all required columns
                continue
                
            # Extract data using expected positions
            cast_name = row[expected_headers['cast_name']].strip()
            cast_imdb_id = row[expected_headers['cast_imdb_id']].strip()
            cast_tmdb_id = row[expected_headers['tmdb_cast_id']].strip()
            show_name = row[expected_headers['show_name']].strip()
            show_imdb_id = row[expected_headers['show_imdb_id']].strip()
            show_tmdb_id = row[expected_headers['tmdb_show_id']].strip()
            
            if not cast_name:  # Skip rows without cast name
                continue
            
            # Use cast name as key for aggregation
            key = cast_name
            
            # Update cast info (use first non-empty values)
            if not cast_data[key]['cast_name']:
                cast_data[key]['cast_name'] = cast_name
            if not cast_data[key]['cast_imdb_id'] and cast_imdb_id:
                cast_data[key]['cast_imdb_id'] = cast_imdb_id
            if not cast_data[key]['cast_tmdb_id'] and cast_tmdb_id:
                cast_data[key]['cast_tmdb_id'] = cast_tmdb_id
            
            # Add show info if not already present
            if show_name and show_name not in cast_data[key]['shows']:
                cast_data[key]['shows'].append(show_name)
            if show_imdb_id and show_imdb_id not in cast_data[key]['show_imdb_ids']:
                cast_data[key]['show_imdb_ids'].append(show_imdb_id)
            if show_tmdb_id and show_tmdb_id not in cast_data[key]['show_tmdb_ids']:
                cast_data[key]['show_tmdb_ids'].append(show_tmdb_id)
        
        print(f"✅ Processed {len(cast_data)} unique cast members from ViableCast")
        return dict(cast_data)
        
    except Exception as e:
        print(f"❌ Error loading ViableCast data: {e}")
        return {}

def create_or_update_realitease_info(spreadsheet, cast_data):
    """Create or update the RealiteaseInfo sheet."""
    print("🔄 Creating/updating RealiteaseInfo sheet...")
    
    try:
        # Try to get existing sheet or create new one
        try:
            realitease_sheet = spreadsheet.worksheet("RealiteaseInfo")
            print("📋 Found existing RealiteaseInfo sheet - will overwrite")
            realitease_sheet.clear()
        except gspread.WorksheetNotFound:
            print("📋 Creating new RealiteaseInfo sheet")
            realitease_sheet = spreadsheet.add_worksheet(title="RealiteaseInfo", rows=len(cast_data) + 100, cols=10)
        
        # RealiteaseInfo headers
        headers = [
            "CastName",
            "CastIMDbID", 
            "CastTMDbID",
            "ShowNames",
            "ShowIMDbIDs",
            "ShowTMDbIDs", 
            "ShowCount",
            "Gender",
            "Birthday",
            "Zodiac"
        ]
        
        # Prepare data rows
        rows_to_insert = [headers]
        
        for cast_name, data in cast_data.items():
            if not data['cast_name']:  # Skip empty entries
                continue
                
            row = [
                data['cast_name'],
                data['cast_imdb_id'],
                data['cast_tmdb_id'],
                ', '.join(data['shows']),  # ShowNames as comma-separated
                ', '.join(data['show_imdb_ids']),  # ShowIMDbIDs as comma-separated
                ', '.join(data['show_tmdb_ids']),  # ShowTMDbIDs as comma-separated
                len(data['shows']),  # ShowCount
                '',  # Gender (empty for now)
                '',  # Birthday (empty for now)
                ''   # Zodiac (empty for now)
            ]
            rows_to_insert.append(row)
        
        # Insert all data at once
        print(f"📝 Writing {len(rows_to_insert)-1} cast members to RealiteaseInfo sheet...")
        realitease_sheet.update(f'A1:J{len(rows_to_insert)}', rows_to_insert)
        
        print(f"✅ Successfully created RealiteaseInfo sheet with {len(rows_to_insert)-1} cast members")
        
        # Show some stats
        show_counts = [len(data['shows']) for data in cast_data.values() if data['shows']]
        if show_counts:
            print(f"📊 Statistics:")
            print(f"   - Average shows per cast member: {sum(show_counts)/len(show_counts):.1f}")
            print(f"   - Max shows for one cast member: {max(show_counts)}")
            print(f"   - Cast members with IMDb IDs: {sum(1 for d in cast_data.values() if d['cast_imdb_id'])}")
            print(f"   - Cast members with TMDb IDs: {sum(1 for d in cast_data.values() if d['cast_tmdb_id'])}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error creating RealiteaseInfo sheet: {e}")
        return False

def show_sample_data(cast_data, num_samples=5):
    """Show sample data for verification."""
    print(f"\n🔍 Sample data (first {num_samples} cast members):")
    
    count = 0
    for cast_name, data in cast_data.items():
        if count >= num_samples:
            break
            
        print(f"\n   {count+1}. {data['cast_name']}")
        print(f"      IMDb ID: {data['cast_imdb_id'] or 'None'}")
        print(f"      TMDb ID: {data['cast_tmdb_id'] or 'None'}")
        print(f"      Shows ({len(data['shows'])}): {', '.join(data['shows'][:3])}{'...' if len(data['shows']) > 3 else ''}")
        
        count += 1

def main():
    """Main execution function."""
    print("🎯 RealiteaseInfo Builder")
    print("=" * 50)
    print("This script builds the RealiteaseInfo sheet from ViableCast data")
    print()
    
    # Connect to spreadsheet
    spreadsheet = connect_to_spreadsheet()
    if not spreadsheet:
        return
    
    print(f"✅ Connected to '{SPREADSHEET_NAME}' spreadsheet")
    
    # Load ViableCast data
    cast_data = load_viablecast_data(spreadsheet)
    if not cast_data:
        print("❌ No data loaded from ViableCast - aborting")
        return
    
    # Show sample data
    show_sample_data(cast_data)
    
    # Ask for confirmation
    print(f"\n❓ Do you want to create/update RealiteaseInfo sheet with {len(cast_data)} cast members? (y/N): ", end="")
    confirmation = input().strip().lower()
    
    if confirmation != 'y':
        print("❌ Operation cancelled.")
        return
    
    # Create/update RealiteaseInfo sheet
    success = create_or_update_realitease_info(spreadsheet, cast_data)
    
    if success:
        print("\n🎉 RealiteaseInfo sheet successfully created/updated!")
    else:
        print("\n❌ Failed to create/update RealiteaseInfo sheet")
    
    print("\n🏁 Script completed!")

if __name__ == "__main__":
    main()
