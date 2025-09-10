#!/usr/bin/env python3
"""
Find and fill missing Cast IMDbIDs in ViableCast sheet.

This script will:
1. Identify rows with missing Cast IMDbIDs (Column D)
2. First try to find the IMDb ID from other rows with the same cast member name
3. If not found internally, scrape IMDb to find the person's ID
4. Update the missing IMDb IDs in the sheet

Strategies used:
- Internal lookup: Check if the same cast member has an IMDb ID in another row
- IMDb search: Search IMDb by name to find the person's profile page
- Validation: Verify the found IMDb ID matches the person's name
"""

import os
import gspread
from dotenv import load_dotenv
import time
import requests
from bs4 import BeautifulSoup
import re
from urllib.parse import quote
import random
from collections import defaultdict

# Load environment variables
load_dotenv()

# Configuration
SPREADSHEET_NAME = os.getenv("SPREADSHEET_NAME", "Realitease2025Data")
SA_KEYFILE = os.getenv(
    "GOOGLE_APPLICATION_CREDENTIALS", 
    "../../keys/trr-backend-e16bfa49d861.json"
)

# Headers for web scraping
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
}

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

def analyze_missing_imdb_ids(sheet):
    """Analyze the sheet to find rows with missing Cast IMDb IDs."""
    print("🔍 Analyzing ViableCast sheet for missing Cast IMDb IDs...")
    
    try:
        all_values = sheet.get_all_values()
        
        missing_imdb_rows = []
        cast_name_to_imdb = {}  # Cache of cast names to their IMDb IDs
        
        # First pass: build cache of existing Cast Name -> IMDb ID mappings
        for i, row in enumerate(all_values[1:], start=2):  # Skip header
            if len(row) > 3:
                cast_name = row[2].strip() if row[2] else ""  # Column C: CastName
                imdb_id = row[3].strip() if row[3] else ""    # Column D: Cast IMDbID
                
                if cast_name and imdb_id:
                    # Clean up IMDb ID (remove any prefixes)
                    clean_imdb_id = imdb_id.replace('nm', '').replace('tt', '')
                    if clean_imdb_id.isdigit():
                        cast_name_to_imdb[cast_name] = f"nm{clean_imdb_id}"
        
        # Second pass: find missing IMDb IDs
        for i, row in enumerate(all_values[1:], start=2):
            if len(row) > 3:
                cast_name = row[2].strip() if row[2] else ""  # Column C: CastName
                imdb_id = row[3].strip() if row[3] else ""    # Column D: Cast IMDbID
                show_name = row[5].strip() if len(row) > 5 else ""  # Column F: ShowName
                
                if cast_name and not imdb_id:  # Missing IMDb ID
                    # Check if we have this cast member's IMDb ID from another row
                    existing_imdb_id = cast_name_to_imdb.get(cast_name)
                    
                    missing_imdb_rows.append({
                        'row_number': i,
                        'cast_name': cast_name,
                        'show_name': show_name,
                        'existing_imdb_id': existing_imdb_id,
                        'full_row': row
                    })
        
        return missing_imdb_rows, cast_name_to_imdb
    
    except Exception as e:
        print(f"❌ Error analyzing sheet: {e}")
        return [], {}

def search_imdb_for_person(name):
    """Search IMDb for a person and return their IMDb ID."""
    try:
        # Clean up the name for search
        search_name = name.strip()
        search_url = f"https://www.imdb.com/find?q={quote(search_name)}&s=nm"
        
        print(f"   🌐 Searching IMDb for: {search_name}")
        
        response = requests.get(search_url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Look for person results
        results = soup.find_all('li', class_='find-name-result') or soup.find_all('li', class_='ipc-metadata-list-summary-item')
        
        if not results:
            # Try alternative search result structure
            results = soup.find_all('a', href=re.compile(r'/name/nm\d+/'))
        
        for result in results[:3]:  # Check first 3 results
            # Extract IMDb ID from href
            link = result.find('a', href=re.compile(r'/name/nm\d+/'))
            if not link:
                link = result if result.name == 'a' else None
            
            if link:
                href = link.get('href', '')
                imdb_match = re.search(r'/name/(nm\d+)/', href)
                if imdb_match:
                    imdb_id = imdb_match.group(1)
                    
                    # Get the name from the result to verify it's a reasonable match
                    name_element = link.get_text(strip=True) if link else ""
                    
                    # Basic similarity check
                    if name_element and (
                        search_name.lower() in name_element.lower() or 
                        name_element.lower() in search_name.lower() or
                        len(set(search_name.lower().split()) & set(name_element.lower().split())) >= 1
                    ):
                        print(f"   ✅ Found match: {name_element} -> {imdb_id}")
                        return imdb_id
        
        print(f"   ❌ No suitable match found for {search_name}")
        return None
        
    except Exception as e:
        print(f"   ❌ Error searching IMDb for {name}: {e}")
        return None

def update_missing_imdb_ids(sheet, missing_rows):
    """Update the sheet with found IMDb IDs."""
    if not missing_rows:
        print("✅ No missing IMDb IDs to update.")
        return
    
    print(f"\n🔄 Processing {len(missing_rows)} rows with missing IMDb IDs...")
    
    updates_made = 0
    internal_fixes = 0
    scraped_fixes = 0
    
    for row_info in missing_rows:
        cast_name = row_info['cast_name']
        row_number = row_info['row_number']
        existing_imdb_id = row_info['existing_imdb_id']
        
        imdb_id_to_use = None
        
        # Strategy 1: Use existing IMDb ID from another row
        if existing_imdb_id:
            print(f"🔍 Row {row_number}: {cast_name} - Found existing IMDb ID: {existing_imdb_id}")
            imdb_id_to_use = existing_imdb_id
            internal_fixes += 1
        
        # Strategy 2: Scrape IMDb
        else:
            print(f"🌐 Row {row_number}: {cast_name} - Searching IMDb...")
            imdb_id_to_use = search_imdb_for_person(cast_name)
            if imdb_id_to_use:
                scraped_fixes += 1
            
            # Add delay to be respectful to IMDb
            time.sleep(random.uniform(1, 3))
        
        # Update the sheet if we found an IMDb ID
        if imdb_id_to_use:
            try:
                # Update Column D (Cast IMDbID)
                sheet.update_cell(row_number, 4, imdb_id_to_use)
                print(f"✅ Updated row {row_number}: {cast_name} -> {imdb_id_to_use}")
                updates_made += 1
                
                # Small delay between updates
                time.sleep(0.5)
                
            except Exception as e:
                print(f"❌ Error updating row {row_number}: {e}")
        else:
            print(f"❌ Could not find IMDb ID for {cast_name}")
    
    print(f"\n📊 Summary:")
    print(f"   Total rows processed: {len(missing_rows)}")
    print(f"   Successfully updated: {updates_made}")
    print(f"   Internal fixes (found in other rows): {internal_fixes}")
    print(f"   Scraped from IMDb: {scraped_fixes}")
    print(f"   Could not resolve: {len(missing_rows) - updates_made}")

def show_missing_summary(missing_rows):
    """Show a summary of missing IMDb IDs."""
    if not missing_rows:
        print("✅ No missing Cast IMDb IDs found!")
        return
    
    print(f"\n📊 Found {len(missing_rows)} rows with missing Cast IMDb IDs:")
    
    # Group by whether we have existing data
    with_existing = []
    without_existing = []
    
    for row_info in missing_rows:
        if row_info['existing_imdb_id']:
            with_existing.append(row_info)
        else:
            without_existing.append(row_info)
    
    if with_existing:
        print(f"\n🔍 {len(with_existing)} can be fixed from existing data:")
        for row_info in with_existing[:10]:  # Show first 10
            print(f"   Row {row_info['row_number']}: {row_info['cast_name']} (found: {row_info['existing_imdb_id']})")
        if len(with_existing) > 10:
            print(f"   ... and {len(with_existing) - 10} more")
    
    if without_existing:
        print(f"\n🌐 {len(without_existing)} need to be scraped from IMDb:")
        for row_info in without_existing[:10]:  # Show first 10
            print(f"   Row {row_info['row_number']}: {row_info['cast_name']} from {row_info['show_name']}")
        if len(without_existing) > 10:
            print(f"   ... and {len(without_existing) - 10} more")

def main():
    """Main execution function."""
    print("🔍 ViableCast Cast IMDb ID Finder")
    print("=" * 50)
    
    # Connect to the sheet
    sheet = connect_to_sheet()
    if not sheet:
        return
    
    print(f"✅ Connected to '{SPREADSHEET_NAME}' - ViableCast sheet")
    
    # Analyze missing IMDb IDs
    missing_rows, existing_cache = analyze_missing_imdb_ids(sheet)
    
    if not missing_rows:
        print("✅ All rows already have Cast IMDb IDs!")
        return
    
    print(f"📊 Found {len(existing_cache)} unique cast members with existing IMDb IDs")
    
    # Show summary
    show_missing_summary(missing_rows)
    
    # Ask for confirmation
    print(f"\n❓ Do you want to proceed with finding and updating {len(missing_rows)} missing IMDb IDs? (y/N): ", end="")
    confirmation = input().strip().lower()
    
    if confirmation != 'y':
        print("❌ Operation cancelled.")
        return
    
    # Update missing IMDb IDs
    update_missing_imdb_ids(sheet, missing_rows)
    
    print("\n🏁 Script completed!")

if __name__ == "__main__":
    main()
