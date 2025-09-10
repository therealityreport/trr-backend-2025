#!/usr/bin/env python3
"""
Build UpdateInfo Script
======================

Aggregates cast data from CastInfo sheet to create one row per person.
Reads the 6-column CastInfo structure and creates person-level aggregations.

CastInfo Input Structure (6 columns):
- Column A: Show IMDbID
- Column B: CastID (TMDb Person ID)  
- Column C: CastName
- Column D: Cast IMDbID
- Column E: ShowID (TMDb Show ID)
- Column F: ShowName

UpdateInfo Output Structure:
- PersonName: Cast member's name
- PersonIMDbID: Their IMDb ID (from CastInfo Column D)
- PersonTMDbID: Their TMDb ID (from CastInfo Column B)
- TotalShows: Number of different shows they appeared in
- TotalEpisodes: Sum of episodes across all shows (placeholder for now)
- ShowIMDbID: Comma-separated list of show IMDb IDs (from CastInfo Column A)
- ShowTMDbID: Comma-separated list of show TMDb IDs (from CastInfo Column E)

Usage: python build_update_info.py [--dry-run]
"""

import gspread
import os
import argparse
from collections import defaultdict
from dotenv import load_dotenv

# Load environment
load_dotenv()

class UpdateInfoBuilder:
    def __init__(self):
        """Initialize the UpdateInfo builder with Google Sheets connection"""
        print("🔄 Connecting to Google Sheets...")
        
        # Connect to Google Sheets
        gc = gspread.service_account(filename='keys/trr-backend-df2c438612e1.json')
        self.sh = gc.open_by_key(os.getenv('SPREADSHEET_ID'))
        
        print("✅ Connected to Google Sheets")
    
    def load_castinfo_data(self):
        """Load data from CastInfo sheet"""
        print("📋 Loading CastInfo data...")
        
        try:
            cast_ws = self.sh.worksheet('CastInfo')
        except gspread.WorksheetNotFound:
            print("❌ CastInfo sheet not found!")
            return []
        
        # Get all data from CastInfo
        all_data = cast_ws.get_all_records()
        print(f"📊 Loaded {len(all_data)} records from CastInfo")
        
        return all_data
    
    def aggregate_cast_data(self, castinfo_data):
        """Aggregate cast data by person to create one row per person"""
        print("🔄 Aggregating cast data by person...")
        
        # Dictionary to store aggregated data by PersonTMDbID (CastID)
        person_aggregation = defaultdict(lambda: {
            'person_name': '',
            'person_imdb_id': '',
            'person_tmdb_id': '',
            'shows': set(),  # Use set to avoid duplicates
            'show_imdb_ids': set(),
            'show_tmdb_ids': set(),
            'total_episodes': 0  # Placeholder for now
        })
        
        processed_count = 0
        skipped_count = 0
        
        for record in castinfo_data:
            # Extract data from CastInfo columns
            show_imdb_id = str(record.get('Show IMDbID', '')).strip()  # Column A
            cast_id = str(record.get('CastID', '')).strip()            # Column B (TMDb Person ID)
            cast_name = str(record.get('CastName', '')).strip()        # Column C
            cast_imdb_id = str(record.get('Cast IMDbID', '')).strip()  # Column D
            show_id = str(record.get('ShowID', '')).strip()            # Column E (TMDb Show ID)
            show_name = str(record.get('ShowName', '')).strip()        # Column F
            
            # Skip records missing essential data
            if not cast_id or not cast_name:
                skipped_count += 1
                continue
            
            # Use CastID (TMDb Person ID) as the unique key
            person_key = cast_id
            
            # Set person info (should be consistent across records for same person)
            if not person_aggregation[person_key]['person_name']:
                person_aggregation[person_key]['person_name'] = cast_name
                person_aggregation[person_key]['person_tmdb_id'] = cast_id
                person_aggregation[person_key]['person_imdb_id'] = cast_imdb_id
            
            # Add show information (using sets to avoid duplicates)
            if show_name:
                person_aggregation[person_key]['shows'].add(show_name)
            if show_imdb_id:
                person_aggregation[person_key]['show_imdb_ids'].add(show_imdb_id)
            if show_id:
                person_aggregation[person_key]['show_tmdb_ids'].add(show_id)
            
            processed_count += 1
        
        print(f"  ✅ Processed {processed_count} records")
        print(f"  ⚠️  Skipped {skipped_count} records (missing CastID or CastName)")
        print(f"  👥 Aggregated into {len(person_aggregation)} unique persons")
        
        return person_aggregation
    
    def prepare_updateinfo_data(self, person_aggregation):
        """Convert aggregated data into rows for UpdateInfo sheet"""
        print("📝 Preparing UpdateInfo sheet data...")
        
        # Headers for UpdateInfo sheet
        headers = [
            "PersonName",
            "PersonIMDbID", 
            "PersonTMDbID",
            "TotalShows",
            "TotalEpisodes",
            "ShowIMDbID",
            "ShowTMDbID"
        ]
        
        # Build data rows
        data_rows = []
        
        for person_key, data in person_aggregation.items():
            # Convert sets to comma-separated strings, sorted for consistency
            show_imdb_ids = ', '.join(sorted(data['show_imdb_ids'])) if data['show_imdb_ids'] else ''
            show_tmdb_ids = ', '.join(sorted(data['show_tmdb_ids'])) if data['show_tmdb_ids'] else ''
            
            row = [
                data['person_name'],           # PersonName
                data['person_imdb_id'],        # PersonIMDbID
                data['person_tmdb_id'],        # PersonTMDbID
                len(data['shows']),            # TotalShows
                data['total_episodes'],        # TotalEpisodes (placeholder)
                show_imdb_ids,                 # ShowIMDbID (comma-separated)
                show_tmdb_ids                  # ShowTMDbID (comma-separated)
            ]
            
            data_rows.append(row)
        
        # Sort by PersonName for consistency
        data_rows.sort(key=lambda x: x[0].lower())
        
        print(f"  📊 Prepared {len(data_rows)} person records")
        
        return headers, data_rows
    
    def create_updateinfo_sheet(self, headers, data_rows, dry_run=False):
        """Create or update the UpdateInfo sheet"""
        print("📋 Creating UpdateInfo sheet...")
        
        if dry_run:
            print("🔍 DRY RUN - Would create UpdateInfo sheet with:")
            print(f"  📊 Headers: {headers}")
            print(f"  📊 Data rows: {len(data_rows)}")
            print(f"  📄 Sample data (first 5 rows):")
            for i, row in enumerate(data_rows[:5]):
                print(f"    {i+1}: {row}")
            return
        
        try:
            # Try to get existing UpdateInfo sheet
            update_ws = self.sh.worksheet('UpdateInfo')
            print("  📋 Found existing UpdateInfo sheet - will clear and rebuild")
            update_ws.clear()
        except gspread.WorksheetNotFound:
            # Create new UpdateInfo sheet
            print("  📋 Creating new UpdateInfo sheet")
            update_ws = self.sh.add_worksheet(title='UpdateInfo', rows=len(data_rows) + 100, cols=len(headers) + 10)
        
        # Write headers
        print("  📝 Writing headers...")
        update_ws.update('A1', [headers])
        
        # Write data in batches
        if data_rows:
            print(f"  📝 Writing {len(data_rows)} data rows...")
            
            # Write data starting from row 2
            range_name = f'A2:G{len(data_rows) + 1}'
            update_ws.update(range_name, data_rows)
        
        print(f"✅ UpdateInfo sheet created successfully!")
        print(f"  📊 {len(data_rows)} person records written")
        print(f"  📋 Columns: {', '.join(headers)}")
        
        # Add documentation
        print("  📝 Adding documentation...")
        doc_range = f'I1:I{len(headers) + 5}'
        documentation = [
            ["UpdateInfo Sheet Documentation"],
            [""],
            ["Purpose: Aggregated cast data - one row per person"],
            ["Source: CastInfo sheet"],
            [""],
            ["Column Descriptions:"],
            ["- PersonName: Cast member's name"],
            ["- PersonIMDbID: Their IMDb ID"],
            ["- PersonTMDbID: Their TMDb ID (CastID)"],
            ["- TotalShows: Number of shows they appeared in"],
            ["- TotalEpisodes: Total episodes (placeholder)"],
            ["- ShowIMDbID: Comma-separated IMDb show IDs"],
            ["- ShowTMDbID: Comma-separated TMDb show IDs"]
        ]
        
        try:
            update_ws.update(doc_range, documentation)
        except Exception as e:
            print(f"  ⚠️  Could not add documentation: {e}")
    
    def build_updateinfo_sheet(self, dry_run=False):
        """Main method to build UpdateInfo sheet from CastInfo data"""
        print("🚀 Starting UpdateInfo sheet build process")
        print("=" * 60)
        
        try:
            # Step 1: Load CastInfo data
            castinfo_data = self.load_castinfo_data()
            if not castinfo_data:
                print("❌ No CastInfo data found - cannot build UpdateInfo")
                return
            
            # Step 2: Aggregate by person
            person_aggregation = self.aggregate_cast_data(castinfo_data)
            if not person_aggregation:
                print("❌ No person data aggregated - cannot build UpdateInfo")
                return
            
            # Step 3: Prepare sheet data
            headers, data_rows = self.prepare_updateinfo_data(person_aggregation)
            
            # Step 4: Create UpdateInfo sheet
            self.create_updateinfo_sheet(headers, data_rows, dry_run)
            
            print("\n✅ UpdateInfo sheet build complete!")
            print(f"📊 Summary: {len(person_aggregation)} unique persons aggregated from CastInfo")
            
        except Exception as e:
            print(f"❌ Error building UpdateInfo sheet: {e}")
            raise

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Build UpdateInfo sheet from CastInfo data')
    parser.add_argument('--dry-run', action='store_true', help='Preview changes without writing to sheet')
    
    args = parser.parse_args()
    
    builder = UpdateInfoBuilder()
    builder.build_updateinfo_sheet(dry_run=args.dry_run)

if __name__ == "__main__":
    main()
