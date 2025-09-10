#!/usr/bin/env python3
"""
Quick debug script to understand the duplicate detection issue
"""

import gspread
import os
from typing import Set, Tuple, List, Dict

# Load Google Sheets credentials
gc = gspread.service_account(filename='/Users/thomashulihan/Projects/TRR-Backend/keys/trr-backend-e16bfa49d861.json')
sh = gc.open('The Reality TV Database')

def load_existing_castinfo() -> Set[Tuple[str, str]]:
    """Load existing CastInfo entries to see what's in there."""
    try:
        cast_ws = sh.worksheet('CastInfo')
        existing_cast_data = cast_ws.get_all_values()
        
        if not existing_cast_data or len(existing_cast_data) <= 1:
            return set()
        
        existing_entries = set()
        header = existing_cast_data[0]
        
        # Find indices for CastID and ShowID
        cast_id_idx = -1
        show_id_idx = -1
        
        for i, col in enumerate(header):
            if col.lower() == 'castid':
                cast_id_idx = i
            elif col.lower() == 'showid':
                show_id_idx = i
        
        print(f"📋 Header: {header}")
        print(f"📋 CastID index: {cast_id_idx}, ShowID index: {show_id_idx}")
        
        if cast_id_idx >= 0 and show_id_idx >= 0:
            for row in existing_cast_data[1:]:
                if len(row) > max(cast_id_idx, show_id_idx):
                    cast_id = row[cast_id_idx].strip()
                    show_id = row[show_id_idx].strip()
                    if cast_id and show_id:
                        existing_entries.add((cast_id, show_id))
        
        print(f"📋 Found {len(existing_entries)} existing CastInfo entries")
        
        # Show first 20 entries to see pattern
        print(f"\n📋 Sample existing entries:")
        for i, entry in enumerate(sorted(existing_entries)):
            if i >= 20:
                break
            print(f"  {i+1:2d}. CastID: {entry[0]}, ShowID: {entry[1]}")
        
        return existing_entries
        
    except Exception as e:
        print(f"⚠️ Error loading existing CastInfo: {e}")
        return set()

def check_recent_castids():
    """Check what CastIDs in the 5632xxx range exist."""
    existing_entries = load_existing_castinfo()
    
    # Look for recent CastIDs (5632xxx range)
    recent_entries = [entry for entry in existing_entries if entry[0].startswith('5632')]
    
    print(f"\n🆔 Found {len(recent_entries)} entries with CastIDs in 5632xxx range:")
    for entry in sorted(recent_entries):
        print(f"  CastID: {entry[0]}, ShowID: {entry[1]}")

if __name__ == '__main__':
    check_recent_castids()
