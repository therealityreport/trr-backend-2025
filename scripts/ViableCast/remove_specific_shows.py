#!/usr/bin/env python3
"""
Remove rows for specific shows from ViableCast sheet.

This script removes all rows where Column F (Show name) matches any of these shows:
- 1000-lb Sisters
- Are You Smarter Than a Celebrity  
- Harry Loves Lisa
- Pictionary

The script will:
1. Connect to the ViableCast sheet
2. Find all rows matching the specified show names
3. Delete those rows from the sheet
4. Provide a summary of removed rows
"""

import os
import gspread
from dotenv import load_dotenv
import time

# Load environment variables
load_dotenv()

# Configuration
SPREADSHEET_NAME = os.getenv("SPREADSHEET_NAME", "Realitease2025Data")
SA_KEYFILE = os.getenv(
    "GOOGLE_APPLICATION_CREDENTIALS", 
    "../../keys/trr-backend-e16bfa49d861.json"
)

# Shows to remove (exact matches in Column F)
SHOWS_TO_REMOVE = [
    "1000-lb Sisters",
    "Are You Smarter Than a Celebrity",
    "Harry Loves Lisa", 
    "Pictionary"
]

def connect_to_sheet():
    """Connect to Google Sheets and return the ViableCast worksheet."""
    try:
        gc = gspread.service_account(filename=SA_KEYFILE)
        spreadsheet = gc.open(SPREADSHEET_NAME)
        viable_cast_sheet = spreadsheet.worksheet("ViableCast")
        return viable_cast_sheet
    except Exception as e:
        print(f"❌ Error connecting to Google Sheets: {e}")
        return None

def find_rows_to_remove(sheet):
    """Find all rows that match the shows to remove."""
    print("🔍 Scanning ViableCast sheet for rows to remove...")
    
    try:
        # Get all values from the sheet
        all_values = sheet.get_all_values()
        
        rows_to_remove = []
        
        # Skip header row (index 0), start from row 2 (index 1)
        for i, row in enumerate(all_values[1:], start=2):  # start=2 because sheet rows are 1-indexed
            if len(row) > 5:  # Make sure we have at least column F
                show_name = row[5].strip()  # Column F (index 5)
                
                if show_name in SHOWS_TO_REMOVE:
                    cast_name = row[2] if len(row) > 2 else "Unknown"  # Column C
                    rows_to_remove.append({
                        'row_number': i,
                        'cast_name': cast_name,
                        'show_name': show_name,
                        'full_row': row
                    })
        
        return rows_to_remove
    
    except Exception as e:
        print(f"❌ Error scanning sheet: {e}")
        return []

def remove_rows(sheet, rows_to_remove):
    """Remove the identified rows from the sheet."""
    if not rows_to_remove:
        print("✅ No rows found to remove.")
        return
    
    print(f"🗑️ Found {len(rows_to_remove)} rows to remove:")
    
    # Show what will be removed
    for row_info in rows_to_remove:
        print(f"   Row {row_info['row_number']}: {row_info['cast_name']} from {row_info['show_name']}")
    
    # Ask for confirmation
    print("\n❓ Do you want to proceed with removing these rows? (y/N): ", end="")
    confirmation = input().strip().lower()
    
    if confirmation != 'y':
        print("❌ Operation cancelled.")
        return
    
    try:
        # Sort rows in descending order so we can delete from bottom to top
        # This prevents row number shifts affecting subsequent deletions
        rows_to_remove.sort(key=lambda x: x['row_number'], reverse=True)
        
        print(f"\n🗑️ Removing {len(rows_to_remove)} rows...")
        
        removed_count = 0
        for row_info in rows_to_remove:
            try:
                sheet.delete_rows(row_info['row_number'])
                print(f"✅ Removed row {row_info['row_number']}: {row_info['cast_name']} from {row_info['show_name']}")
                removed_count += 1
                
                # Add a small delay to avoid rate limiting
                time.sleep(0.5)
                
            except Exception as e:
                print(f"❌ Error removing row {row_info['row_number']}: {e}")
        
        print(f"\n🎉 Successfully removed {removed_count} out of {len(rows_to_remove)} rows.")
        
    except Exception as e:
        print(f"❌ Error during row removal: {e}")

def show_summary(rows_to_remove):
    """Show a summary of what will be removed, grouped by show."""
    if not rows_to_remove:
        return
    
    print("\n📊 Summary by show:")
    
    show_counts = {}
    for row_info in rows_to_remove:
        show_name = row_info['show_name']
        if show_name not in show_counts:
            show_counts[show_name] = []
        show_counts[show_name].append(row_info['cast_name'])
    
    for show_name, cast_members in show_counts.items():
        print(f"   {show_name}: {len(cast_members)} cast members")
        for cast_name in cast_members:
            print(f"     - {cast_name}")

def main():
    """Main execution function."""
    print("🧹 ViableCast Show Removal Script")
    print("=" * 50)
    print(f"Target shows to remove: {', '.join(SHOWS_TO_REMOVE)}")
    print()
    
    # Connect to the sheet
    sheet = connect_to_sheet()
    if not sheet:
        return
    
    print(f"✅ Connected to '{SPREADSHEET_NAME}' - ViableCast sheet")
    
    # Find rows to remove
    rows_to_remove = find_rows_to_remove(sheet)
    
    if not rows_to_remove:
        print("✅ No rows found matching the specified shows.")
        return
    
    # Show summary
    show_summary(rows_to_remove)
    
    # Remove the rows
    remove_rows(sheet, rows_to_remove)
    
    print("\n🏁 Script completed!")

if __name__ == "__main__":
    main()
