#!/usr/bin/env python3
"""
Remove specific cast members from ViableCast sheet.

This script removes all rows for:
- Alexisamor Ramirez
- Nilda Alicea-Felix

These are the cast members who couldn't get their IMDb IDs resolved.
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

# Cast members to remove (exact matches in Column C - CastName)
CAST_MEMBERS_TO_REMOVE = [
    "Alexisamor Ramirez",
    "Nilda Alicea-Felix"
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
    """Find all rows that match the cast members to remove."""
    print("🔍 Scanning ViableCast sheet for specific cast members to remove...")
    
    try:
        # Get all values from the sheet
        all_values = sheet.get_all_values()
        
        rows_to_remove = []
        
        # Skip header row (index 0), start from row 2 (index 1)
        for i, row in enumerate(all_values[1:], start=2):  # start=2 because sheet rows are 1-indexed
            if len(row) > 2:  # Make sure we have at least column C
                cast_name = row[2].strip()  # Column C (index 2): CastName
                show_name = row[5].strip() if len(row) > 5 else "Unknown"  # Column F: ShowName
                
                if cast_name in CAST_MEMBERS_TO_REMOVE:
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
    """Show a summary of what will be removed, grouped by cast member."""
    if not rows_to_remove:
        return
    
    print("\n📊 Summary by cast member:")
    
    cast_counts = {}
    for row_info in rows_to_remove:
        cast_name = row_info['cast_name']
        if cast_name not in cast_counts:
            cast_counts[cast_name] = []
        cast_counts[cast_name].append(row_info['show_name'])
    
    for cast_name, shows in cast_counts.items():
        print(f"   {cast_name}: {len(shows)} appearances")
        for show_name in shows:
            print(f"     - {show_name}")

def main():
    """Main execution function."""
    print("🧹 ViableCast Cast Member Removal Script")
    print("=" * 50)
    print(f"Target cast members to remove: {', '.join(CAST_MEMBERS_TO_REMOVE)}")
    print("(These are the cast members who couldn't get their IMDb IDs resolved)")
    print()
    
    # Connect to the sheet
    sheet = connect_to_sheet()
    if not sheet:
        return
    
    print(f"✅ Connected to '{SPREADSHEET_NAME}' - ViableCast sheet")
    
    # Find rows to remove
    rows_to_remove = find_rows_to_remove(sheet)
    
    if not rows_to_remove:
        print("✅ No rows found matching the specified cast members.")
        return
    
    # Show summary
    show_summary(rows_to_remove)
    
    # Remove the rows
    remove_rows(sheet, rows_to_remove)
    
    print("\n🏁 Script completed!")

if __name__ == "__main__":
    main()
