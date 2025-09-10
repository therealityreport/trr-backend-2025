#!/usr/bin/env python3
"""
Remove Cast Members Who Only Appear on One Show

This script removes cast members from the Google Sheets database who only appear
on a single show. This helps optimize processing time for season extraction by
focusing on cast members who appear across multiple shows.

If a cast member ends up being on another show later, the find_missing_cast script
will re-add them, and they can be processed again.
"""

import gspread
from collections import defaultdict
import time

def setup_google_sheets():
    """Initialize Google Sheets connection"""
    print("🔄 Connecting to Google Sheets...")
    gc = gspread.service_account(filename='/Users/thomashulihan/Projects/TRR-Backend/keys/trr-backend-df2c438612e1.json')
    sheet = gc.open('Realitease2025Data').worksheet('CastInfo')
    print("✅ Connected to Google Sheets")
    return sheet

def analyze_cast_distribution(sheet):
    """Analyze how many shows each cast member appears on based on NAME"""
    print("🔄 Loading all cast data...")
    
    # Get all data
    all_data = sheet.get_all_records()
    print(f"📊 Loaded {len(all_data)} cast member records")
    
    # Count shows per cast member NAME (not ID)
    cast_name_show_count = defaultdict(set)
    cast_name_records = defaultdict(list)
    
    for i, record in enumerate(all_data, start=2):  # Start at row 2 (row 1 is headers)
        show_id = record.get('Show IMDbID', '').strip()  # Updated column name
        cast_name = record.get('CastName', '').strip()   # Updated column name
        
        # Use cast name as the key, skip empty names or show IDs
        if cast_name and show_id:
            cast_name_show_count[cast_name].add(show_id)
            cast_name_records[cast_name].append({
                'row': i,
                'name': cast_name,
                'show_id': show_id,
                'record': record
            })
    
    # Analyze distribution
    single_show_cast = []
    multi_show_cast = []
    
    for cast_name, shows in cast_name_show_count.items():
        if len(shows) == 1:
            single_show_cast.extend(cast_name_records[cast_name])
        else:
            multi_show_cast.extend(cast_name_records[cast_name])
    
    print(f"\n📊 Cast Distribution Analysis (by NAME):")
    print(f"  👤 Unique cast member names: {len(cast_name_show_count)}")
    print(f"  🎬 Single-show cast members: {len([c for c in cast_name_show_count.values() if len(c) == 1])}")
    print(f"  🌟 Multi-show cast members: {len([c for c in cast_name_show_count.values() if len(c) > 1])}")
    print(f"  📝 Total single-show records: {len(single_show_cast)}")
    print(f"  📝 Total multi-show records: {len(multi_show_cast)}")
    
    return single_show_cast, multi_show_cast, cast_name_show_count

def show_removal_preview(single_show_cast, cast_name_show_count):
    """Show preview of what will be removed"""
    print(f"\n🗑️ Cast Members to Remove (Single Show Only by NAME):")
    
    # Group by show for better overview
    by_show = defaultdict(list)
    for record in single_show_cast:
        by_show[record['show_id']].append(record)
    
    total_to_remove = 0
    for show_id, records in sorted(by_show.items()):
        print(f"\n  📺 Show {show_id}: {len(records)} cast members")
        total_to_remove += len(records)
        
        # Show first few examples
        for i, record in enumerate(records[:5]):
            print(f"    - {record['name']} (Row {record['row']})")
        
        if len(records) > 5:
            print(f"    ... and {len(records) - 5} more")
    
    print(f"\n📊 Summary:")
    print(f"  🗑️ Total records to remove: {total_to_remove}")
    print(f"  💾 Records to keep: {sum(len(shows) for shows in cast_name_show_count.values() if len(shows) > 1)}")
    
    return total_to_remove

def remove_single_show_cast(sheet, single_show_cast, dry_run=True):
    """Remove single-show cast members from the sheet"""
    
    if dry_run:
        print(f"\n🧪 DRY RUN MODE - No actual deletions will be performed")
        return
    
    print(f"\n🗑️ Starting removal of {len(single_show_cast)} single-show cast records...")
    
    # Sort by row number in descending order (delete from bottom up to avoid row shifting)
    single_show_cast.sort(key=lambda x: x['row'], reverse=True)
    
    removed_count = 0
    batch_size = 10
    
    for i in range(0, len(single_show_cast), batch_size):
        batch = single_show_cast[i:i + batch_size]
        
        print(f"🔄 Processing batch {i//batch_size + 1}/{(len(single_show_cast) + batch_size - 1)//batch_size}")
        
        for record in batch:
            try:
                # Delete the row
                sheet.delete_rows(record['row'])
                removed_count += 1
                print(f"  ✅ Removed {record['name']} (Row {record['row']})")
                
                # Small delay to avoid rate limiting
                time.sleep(0.1)
                
            except Exception as e:
                print(f"  ❌ Error removing {record['name']} (Row {record['row']}): {e}")
        
        # Longer delay between batches
        if i + batch_size < len(single_show_cast):
            print(f"  ⏸️ Waiting 2 seconds before next batch...")
            time.sleep(2)
    
    print(f"\n✅ Removal complete!")
    print(f"  🗑️ Successfully removed: {removed_count} records")
    print(f"  ❌ Failed to remove: {len(single_show_cast) - removed_count} records")

def main():
    """Main execution function"""
    print("🚀 Starting Single-Show Cast Removal Script")
    print("=" * 60)
    
    try:
        # Setup
        sheet = setup_google_sheets()
        
        # Analyze cast distribution
        single_show_cast, multi_show_cast, cast_name_show_count = analyze_cast_distribution(sheet)
        
        # Show preview
        total_to_remove = show_removal_preview(single_show_cast, cast_name_show_count)
        
        # Confirm with user
        print(f"\n❓ This will remove {total_to_remove} cast member records whose NAMES only appear on one show.")
        print("   Cast members whose names appear on multiple shows will be preserved for cross-show analysis.")
        print("   If cast members later appear on other shows, find_missing_cast will re-add them.")
        
        confirm = input("\n🤔 Proceed with removal? (yes/no): ").strip().lower()
        
        if confirm in ['yes', 'y']:
            # Ask about dry run
            dry_run_confirm = input("🧪 Run in dry-run mode first? (yes/no): ").strip().lower()
            dry_run = dry_run_confirm in ['yes', 'y']
            
            # Perform removal
            remove_single_show_cast(sheet, single_show_cast, dry_run=dry_run)
            
            if dry_run:
                print("\n🧪 Dry run completed. Run again with dry_run=False to perform actual removal.")
            else:
                print("\n✅ Single-show cast removal completed!")
                print("💡 You can now run the season extraction script on the optimized dataset.")
        else:
            print("❌ Removal cancelled by user.")
    
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
