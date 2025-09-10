#!/usr/bin/env python3
"""
RealiteaseInfo Sheet Builder
============================

Creates a Google Sheet called 'RealiteaseInfo' with unique cast members and their show information.
Enhanced with Gender,     def load_viablecast_data(self):
        """Load all data from ViableCast sheet"""
        print("🔄 Loading ViableCast data...")
        
        try:
            viable_cast_sheet = self.spreadsheet.worksheet("ViableCast")
            
            # Define expected headers to handle any blank columns
            expected_headers = [
                "Index",           # Column A
                "CastID",          # Column B  
                "CastName",        # Column C
                "Cast IMDbID",     # Column D
                "ShowID",          # Column E
                "ShowName",        # Column F
                "Episodes",        # Column G
                "Seasons"          # Column H
            ]
            
            # Get all data with expected headers to avoid duplicate header issues
            all_data = viable_cast_sheet.get_all_records(expected_headers=expected_headers)
            print(f"📊 Loaded {len(all_data)} records from ViableCast")
            
            return all_data
            
        except gspread.WorksheetNotFound:
            print("❌ ViableCast sheet not found!")
            return []
        except Exception as e:
            print(f"❌ Error loading ViableCast data: {e}")
            print("🔄 Trying alternative approach...")
            
            # Alternative approach: get all values and manually create records
            try:
                all_values = viable_cast_sheet.get_all_values()
                if not all_values:
                    return []
                
                # Use first row as headers, but clean them up
                headers = all_values[0]
                expected_headers = [
                    "Index",           # Column A
                    "CastID",          # Column B  
                    "CastName",        # Column C
                    "Cast IMDbID",     # Column D
                    "ShowID",          # Column E
                    "ShowName",        # Column F
                    "Episodes",        # Column G
                    "Seasons"          # Column H
                ]
                
                # Create records manually
                records = []
                for row in all_values[1:]:  # Skip header row
                    if len(row) >= 8:  # Ensure we have enough columns
                        record = {}
                        for i, header in enumerate(expected_headers):
                            record[header] = row[i] if i < len(row) else ""
                        records.append(record)
                
                print(f"📊 Loaded {len(records)} records from ViableCast using alternative method")
                return records
                
            except Exception as e2:
                print(f"❌ Alternative approach also failed: {e2}")
                return []diac data extraction from TMDb API.

Sheet Structure:
- CastName: Cast member's name
- CastIMDbID: Their IMDb ID
- CastTMDbID: Their TMDb ID
- ShowNames: Comma-separated list of all shows they appeared in
- ShowIMDbIDs: Comma-separated list of all show IMDb IDs
- ShowTMDbIDs: Comma-separated list of all show TMDb IDs
- ShowCount: Number of different shows they appeared in
- Gender: M/F (extracted from TMDb)
- Birthday: YYYY-MM-DD format (extracted from TMDb)
- Zodiac: Astrological sign based on birthday

Data Source: ViableCast sheet (existing optimized dataset)
"""

import gspread
import time
from collections import defaultdict
import os
from datetime import datetime
import requests
import json
import re
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def calculate_zodiac(birthday):
    """Calculate zodiac sign from birthday"""
    if not birthday:
        return ""
    
    try:
        # Parse birthday (expecting YYYY-MM-DD format)
        month, day = birthday.split('-')[1:3]
        month, day = int(month), int(day)
        
        if (month == 3 and day >= 21) or (month == 4 and day <= 19):
            return "Aries"
        elif (month == 4 and day >= 20) or (month == 5 and day <= 20):
            return "Taurus"
        elif (month == 5 and day >= 21) or (month == 6 and day <= 20):
            return "Gemini"
        elif (month == 6 and day >= 21) or (month == 7 and day <= 22):
            return "Cancer"
        elif (month == 7 and day >= 23) or (month == 8 and day <= 22):
            return "Leo"
        elif (month == 8 and day >= 23) or (month == 9 and day <= 22):
            return "Virgo"
        elif (month == 9 and day >= 23) or (month == 10 and day <= 22):
            return "Libra"
        elif (month == 10 and day >= 23) or (month == 11 and day <= 21):
            return "Scorpio"
        elif (month == 11 and day >= 22) or (month == 12 and day <= 21):
            return "Sagittarius"
        elif (month == 12 and day >= 22) or (month == 1 and day <= 19):
            return "Capricorn"
        elif (month == 1 and day >= 20) or (month == 2 and day <= 18):
            return "Aquarius"
        elif (month == 2 and day >= 19) or (month == 3 and day <= 20):
            return "Pisces"
        else:
            return ""
    except (ValueError, IndexError):
        return ""

class RealiteaseInfoBuilder:
    def __init__(self):
        """Initialize the RealiteaseInfo builder"""
        print("🎬 Starting RealiteaseInfo Sheet Builder with Bio Data Extraction")
        print("=" * 60)
        
        # Initialize Google Sheets connection
        self.gc = gspread.service_account(filename='/Users/thomashulihan/Projects/TRR-Backend/keys/trr-backend-df2c438612e1.json')
        self.spreadsheet = self.gc.open("Realitease2025Data")
        
        # Initialize TMDb API session
        self.setup_tmdb()
        
        # Get or create RealiteaseInfo sheet
        self.realitease_sheet = self.get_or_create_realitease_sheet()
        
        # Load ViableCast data
        self.viable_cast_data = self.load_viable_cast_data()
        
    def setup_tmdb(self):
        """Setup TMDb API session"""
        print("🔄 Setting up TMDb API...")
        self.session = requests.Session()
        self.session.headers.update({"accept": "application/json"})
        
        # Try to get TMDb API key from environment or use a default placeholder
        # Note: In production, you'd want to properly configure this
        self.tmdb_api_key = os.getenv('TMDB_API_KEY', '')
        if self.tmdb_api_key:
            print("✅ TMDb API key found")
        else:
            print("⚠️ No TMDb API key found - bio data extraction will be limited")
    
    def tmdb_person(self, person_id):
        """Fetch TMDb person details"""
        if not self.tmdb_api_key or not person_id:
            return {}
            
        try:
            url = f"https://api.themoviedb.org/3/person/{person_id}"
            params = {"api_key": self.tmdb_api_key}
            
            response = self.session.get(url, params=params, timeout=10)
            if response.status_code == 200:
                time.sleep(0.25)  # Rate limiting
                return response.json()
            else:
                print(f"  ⚠️ TMDb API error for {person_id}: {response.status_code}")
                return {}
        except Exception as e:
            print(f"  ⚠️ TMDb error for {person_id}: {e}")
            return {}
    
    def map_gender(self, code):
        """Map TMDb gender code to readable format"""
        try:
            code = int(code) if code else 0
        except:
            return ""
        
        if code == 1:
            return "F"  # Female
        elif code == 2:
            return "M"  # Male
        else:
            return ""  # Unknown or other
        
    def get_or_create_realitease_sheet(self):
        """Get existing RealiteaseInfo sheet or create a new one"""
        try:
            sheet = self.spreadsheet.worksheet("RealiteaseInfo")
            print("✅ Found existing RealiteaseInfo sheet")
            
            # Check if headers need updating
            current_headers = sheet.row_values(1)
            expected_headers = [
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
            
            if current_headers != expected_headers:
                print("🔄 Updating headers to new format...")
                sheet.update(values=[expected_headers], range_name='A1:J1')
                
                # Format headers
                sheet.format('A1:J1', {
                    'textFormat': {'bold': True},
                    'backgroundColor': {'red': 0.2, 'green': 0.6, 'blue': 0.9}
                })
                print("✅ Headers updated successfully")
            else:
                print("✅ Headers are already in correct format")
                
            return sheet
        except gspread.WorksheetNotFound:
            print("🔄 Creating new RealiteaseInfo sheet...")
            
            # Create new sheet
            sheet = self.spreadsheet.add_worksheet(title="RealiteaseInfo", rows=10000, cols=10)
            
            # Set up headers
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
            
            sheet.update(values=[headers], range_name='A1:J1')
            
            # Format headers
            sheet.format('A1:J1', {
                'textFormat': {'bold': True},
                'backgroundColor': {'red': 0.2, 'green': 0.6, 'blue': 0.9}
            })
            
            print("✅ Created RealiteaseInfo sheet with headers")
            return sheet
            
    def load_viable_cast_data(self):
        """Load all data from ViableCast sheet"""
        print("🔄 Loading ViableCast data...")
        
        try:
            viable_cast_sheet = self.spreadsheet.worksheet("ViableCast")
            
            # Get all data
            all_data = viable_cast_sheet.get_all_records()
            print(f"📊 Loaded {len(all_data)} records from ViableCast")
            
            return all_data
            
        except gspread.WorksheetNotFound:
            print("❌ ViableCast sheet not found!")
            return []
    
    def extract_bio_data(self, cast_name, cast_imdb_id, cast_tmdb_id):
        """Extract gender and birthday using tiered approach: IMDb -> Wikidata -> TMDb"""
        print(f"🔍 Extracting bio data for: {cast_name}")
        
        bio_data = {
            'gender': '',
            'birthday': '',
            'zodiac': ''
        }
        
        # Try IMDb first using simple web scraping
        if cast_imdb_id:
            bio_data = self.extract_from_imdb(cast_imdb_id, bio_data)
            if bio_data['gender'] and bio_data['birthday']:
                bio_data['zodiac'] = calculate_zodiac(bio_data['birthday'])
                print(f"  ✅ Complete data from IMDb")
                return bio_data
        
        # Try Wikidata if IMDb incomplete
        if cast_name and (not bio_data['gender'] or not bio_data['birthday']):
            bio_data = self.extract_from_wikidata(cast_name, bio_data)
            if bio_data['gender'] and bio_data['birthday']:
                bio_data['zodiac'] = calculate_zodiac(bio_data['birthday'])
                print(f"  ✅ Complete data from Wikidata")
                return bio_data
        
        # Try TMDb if still incomplete
        if cast_tmdb_id and (not bio_data['gender'] or not bio_data['birthday']):
            bio_data = self.extract_from_tmdb(cast_tmdb_id, bio_data)
        
        # Calculate zodiac if we have birthday
        if bio_data['birthday']:
            bio_data['zodiac'] = calculate_zodiac(bio_data['birthday'])
        
        source = "TMDb" if cast_tmdb_id else "Wikidata" if cast_name else "IMDb" if cast_imdb_id else "none"
        print(f"  📊 Final result from {source}: Gender={bio_data['gender']}, Birthday={bio_data['birthday']}")
        return bio_data
    
    def extract_from_imdb(self, imdb_id, current_data):
        """Extract data from IMDb using requests"""
        try:
            url = f"https://www.imdb.com/name/{imdb_id}/"
            headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'}
            
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code != 200:
                return current_data
                
            content = response.text
            
            # Extract birthday using regex
            if not current_data['birthday']:
                # Look for birth date pattern
                birth_patterns = [
                    r'Born[^>]*>([^<]*\d{4})',
                    r'birth_date[^>]*>([^<]*\d{4})',
                    r'(\w+\s+\d{1,2},\s+\d{4})'
                ]
                
                for pattern in birth_patterns:
                    match = re.search(pattern, content, re.IGNORECASE)
                    if match:
                        date_str = match.group(1).strip()
                        try:
                            # Try to parse common date formats
                            for fmt in ['%B %d, %Y', '%b %d, %Y', '%Y-%m-%d']:
                                try:
                                    parsed_date = datetime.strptime(date_str, fmt)
                                    current_data['birthday'] = parsed_date.strftime('%Y-%m-%d')
                                    break
                                except:
                                    continue
                            if current_data['birthday']:
                                break
                        except:
                            pass
            
            # Extract gender using pronouns in bio
            if not current_data['gender']:
                bio_text = content.lower()
                he_count = bio_text.count(' he ') + bio_text.count(' his ') + bio_text.count(' him ')
                she_count = bio_text.count(' she ') + bio_text.count(' her ') + bio_text.count(' hers ')
                
                if he_count > she_count and he_count > 2:
                    current_data['gender'] = 'M'
                elif she_count > he_count and she_count > 2:
                    current_data['gender'] = 'F'
            
            time.sleep(0.5)  # Rate limiting
            return current_data
            
        except Exception as e:
            print(f"  ⚠️ IMDb extraction failed: {e}")
            return current_data
    
    def extract_from_wikidata(self, cast_name, current_data):
        """Extract data from Wikidata API"""
        try:
            # Search for the person on Wikidata
            search_url = "https://www.wikidata.org/w/api.php"
            search_params = {
                'action': 'wbsearchentities',
                'search': cast_name,
                'language': 'en',
                'type': 'item',
                'format': 'json',
                'limit': 5
            }
            
            response = requests.get(search_url, params=search_params, timeout=10)
            if response.status_code != 200:
                return current_data
                
            search_data = response.json()
            
            if not search_data.get('search'):
                return current_data
            
            # Try the first few results to find a person
            for result in search_data['search'][:3]:
                entity_id = result['id']
                
                # Get entity data
                entity_params = {
                    'action': 'wbgetentities',
                    'ids': entity_id,
                    'format': 'json'
                }
                
                entity_response = requests.get(search_url, params=entity_params, timeout=10)
                if entity_response.status_code != 200:
                    continue
                    
                entity_data = entity_response.json()
                
                if entity_id not in entity_data.get('entities', {}):
                    continue
                    
                claims = entity_data['entities'][entity_id].get('claims', {})
                
                # Check if this is actually a person (P31 = instance of, Q5 = human)
                if 'P31' in claims:
                    instance_claims = claims['P31']
                    is_human = any(
                        claim.get('mainsnak', {}).get('datavalue', {}).get('value', {}).get('id') == 'Q5'
                        for claim in instance_claims
                    )
                    if not is_human:
                        continue
                
                # Extract gender (P21)
                if not current_data['gender'] and 'P21' in claims:
                    try:
                        gender_claim = claims['P21'][0]['mainsnak']['datavalue']['value']['id']
                        if gender_claim == 'Q6581097':  # male
                            current_data['gender'] = 'M'
                        elif gender_claim == 'Q6581072':  # female
                            current_data['gender'] = 'F'
                    except:
                        pass
                
                # Extract birthday (P569)
                if not current_data['birthday'] and 'P569' in claims:
                    try:
                        birthday_claim = claims['P569'][0]['mainsnak']['datavalue']['value']['time']
                        # Extract date from +1990-01-01T00:00:00Z format
                        date_match = re.search(r'\+(\d{4})-(\d{2})-(\d{2})', birthday_claim)
                        if date_match:
                            current_data['birthday'] = f"{date_match.group(1)}-{date_match.group(2)}-{date_match.group(3)}"
                    except:
                        pass
                
                # If we found data, break out of the loop
                if current_data['gender'] or current_data['birthday']:
                    break
            
            time.sleep(0.3)  # Rate limiting
            return current_data
            
        except Exception as e:
            print(f"  ⚠️ Wikidata extraction failed: {e}")
            return current_data
    
    def extract_from_tmdb(self, tmdb_id, current_data):
        """Extract data from TMDb API"""
        try:
            if not self.tmdb_api_key or not tmdb_id:
                return current_data
                
            person_data = self.tmdb_person(tmdb_id)
            if person_data:
                if not current_data['gender']:
                    current_data['gender'] = self.map_gender(person_data.get('gender'))
                if not current_data['birthday']:
                    current_data['birthday'] = person_data.get('birthday', '').strip()
            
            return current_data
            
        except Exception as e:
            print(f"  ⚠️ TMDb extraction failed: {e}")
            return current_data
    
    def build_cast_aggregation(self):
        """Aggregate all shows for each unique cast member"""
        print("🔄 Building cast member aggregation...")
        
        cast_aggregation = defaultdict(lambda: {
            'cast_name': '',
            'cast_imdb_id': '',
            'cast_tmdb_id': '',  # Will be populated from ViableCast column B
            'shows': [],
            'show_imdb_ids': [],
            'show_tmdb_ids': [],  # Will be populated from ViableCast column E
            'gender': '',
            'birthday': '',
            'zodiac': '',
            'bio_extracted': False
        })
        
        # PHASE 1: Aggregate all show data first (no bio extraction)
        print("  📊 Phase 1: Aggregating show data...")
        for record in self.viable_cast_data:
            cast_imdb_id = record.get('Cast IMDbID', '').strip()
            cast_name = record.get('CastName', '').strip()
            cast_tvdb_id = str(record.get('CastID', '')).strip()  # Column B - CastID is the TVDb ID
            show_name = record.get('ShowName', '').strip()
            show_imdb_id = record.get('Show IMDbID', '').strip()
            show_tvdb_id = str(record.get('ShowID', '')).strip()  # Column E - ShowID is the TVDb ID
            
            # Skip if missing essential data
            if not cast_imdb_id or not cast_name or not show_name:
                continue
                
            # Use Cast IMDb ID as the unique key
            cast_key = cast_imdb_id
            
            # Set cast info (should be consistent across records)
            if not cast_aggregation[cast_key]['cast_name']:
                cast_aggregation[cast_key]['cast_name'] = cast_name
                cast_aggregation[cast_key]['cast_imdb_id'] = cast_imdb_id
                cast_aggregation[cast_key]['cast_tmdb_id'] = cast_tvdb_id  # Use actual TVDb ID from CastID
            
            # Add show info if not already present
            if show_name not in cast_aggregation[cast_key]['shows']:
                cast_aggregation[cast_key]['shows'].append(show_name)
                
            if show_imdb_id not in cast_aggregation[cast_key]['show_imdb_ids']:
                cast_aggregation[cast_key]['show_imdb_ids'].append(show_imdb_id)
                cast_aggregation[cast_key]['show_tmdb_ids'].append(show_tvdb_id)  # Use actual TVDb ID from ShowID
        
        print(f"  ✅ Phase 1 complete: {len(cast_aggregation)} unique cast members aggregated")
        
        print(f"📊 Aggregated {len(cast_aggregation)} unique cast members")
        return cast_aggregation
    

    
    def write_to_sheet(self, cast_aggregation):
        """Write aggregated data to RealiteaseInfo sheet WITHOUT clearing ANY data"""
        print("🔄 Writing data to RealiteaseInfo sheet (preserving ALL existing data)...")
        
        # Get current data to preserve existing bio data
        print("  🔍 Reading existing data to preserve bio data...")
        current_data = self.realitease_sheet.get_all_records()
        existing_cast_map = {}
        
        for i, record in enumerate(current_data):
            cast_imdb_id = record.get('CastIMDbID', '')
            if cast_imdb_id:
                existing_cast_map[cast_imdb_id] = {
                    'row_index': i + 2,  # +2 for header and 1-indexing
                    'gender': record.get('Gender', ''),
                    'birthday': record.get('Birthday', ''),
                    'zodiac': record.get('Zodiac', ''),
                    'cast_name': record.get('CastName', ''),
                    'show_names': record.get('ShowNames', ''),
                    'show_count': record.get('ShowCount', 0)
                }
        
        # Find next available row for new cast members
        next_row = len(current_data) + 2  # +2 for header and 1-indexing
        
        # Process each cast member
        updates_made = 0
        new_members_added = 0
        
        for cast_key, data in cast_aggregation.items():
            cast_imdb_id = data['cast_imdb_id']
            
            # Join lists with commas
            show_names = ', '.join(data['shows'])
            show_imdb_ids = ', '.join(data['show_imdb_ids'])
            show_tmdb_ids = ', '.join(data['show_tmdb_ids'])
            show_count = len(data['shows'])
            
            if cast_imdb_id in existing_cast_map:
                # Cast member exists - update their show data and preserve bio data
                existing = existing_cast_map[cast_imdb_id]
                row_num = existing['row_index']
                
                # Only update if show data has changed
                if (existing['show_names'] != show_names or 
                    existing['show_count'] != show_count):
                    
                    row_data = [
                        data['cast_name'],           # A
                        data['cast_imdb_id'],        # B  
                        data['cast_tmdb_id'],        # C
                        show_names,                  # D
                        show_imdb_ids,               # E
                        show_tmdb_ids,               # F
                        show_count,                  # G
                        existing['gender'],          # H - preserve existing
                        existing['birthday'],        # I - preserve existing  
                        existing['zodiac']           # J - preserve existing
                    ]
                    
                    # Update this specific row
                    range_name = f'A{row_num}:J{row_num}'
                    self.realitease_sheet.update(values=[row_data], range_name=range_name)
                    updates_made += 1
                    print(f"  ✅ Updated existing cast member: {data['cast_name']} (row {row_num})")
                    time.sleep(0.8)  # Rate limiting
            else:
                # New cast member - add them
                row_data = [
                    data['cast_name'],           # A
                    data['cast_imdb_id'],        # B  
                    data['cast_tmdb_id'],        # C
                    show_names,                  # D
                    show_imdb_ids,               # E
                    show_tmdb_ids,               # F
                    show_count,                  # G
                    '',                          # H - empty gender for new member
                    '',                          # I - empty birthday for new member  
                    ''                           # J - empty zodiac for new member
                ]
                
                # Add at next available row
                range_name = f'A{next_row}:J{next_row}'
                self.realitease_sheet.update(values=[row_data], range_name=range_name)
                new_members_added += 1
                next_row += 1
                print(f"  ✅ Added new cast member: {data['cast_name']} (row {next_row-1})")
                time.sleep(0.8)  # Rate limiting
        
        total_cast_count = len(cast_aggregation)
        print(f"✅ Processing complete! Updated: {updates_made}, New members: {new_members_added}, Total cast: {total_cast_count}")
        return total_cast_count
    
    def add_summary_stats(self, total_cast_count):
        """Add summary statistics to the sheet"""
        print("🔄 Adding summary statistics...")
        
        # Find an empty area for stats (after the data)
        stats_start_row = total_cast_count + 5  # Leave some space
        
        stats_data = [
            ["📊 REALITEASE INFO SUMMARY", ""],
            ["=" * 30, ""],
            ["Total Unique Cast Members:", total_cast_count],
            ["Data Source:", "ViableCast Sheet"],
            ["Last Updated:", datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
            ["", ""],
            ["📝 Notes:", ""],
            ["- CastTMDbID: Populated from ViableCast Column B (CastID)", ""],
            ["- ShowTMDbIDs: Populated from ViableCast Column E (ShowID)", ""],
            ["- Gender: M/F extracted from TMDb API", ""],
            ["- Birthday: YYYY-MM-DD format from TMDb API", ""],
            ["- Zodiac: Calculated from birthday", ""],
            ["- ShowCount: Number of different shows each cast member appeared in", ""]
        ]
        
        # Write stats
        for i, row in enumerate(stats_data):
            range_name = f'A{stats_start_row + i}:B{stats_start_row + i}'
            self.realitease_sheet.update(values=[row], range_name=range_name)
            time.sleep(0.5)
        
        print("✅ Added summary statistics")
    
    def cleanup(self):
        """Clean up resources"""
        print("✅ Cleanup complete")
    
    def run(self):
        """Main execution method"""
        print(f"⏰ Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        if not self.viable_cast_data:
            print("❌ No ViableCast data found. Exiting.")
            return
        
        # Build basic aggregation (no bio data yet)
        cast_aggregation = self.build_cast_aggregation()
        
        if not cast_aggregation:
            print("❌ No cast data to process. Exiting.")
            return
        
        # Write basic data to sheet IMMEDIATELY
        print("🔄 Writing basic cast and show data to sheet...")
        total_count = self.write_to_sheet(cast_aggregation)
        print(f"✅ Successfully wrote {total_count} cast members with show data")
        
        # THEN extract bio data and update (optional/can be interrupted)
        print("🔄 Now extracting bio data (this may take a while, but basic data is already saved)...")
        try:
            self.extract_and_update_bio_data(cast_aggregation)
        except KeyboardInterrupt:
            print("\n⚠️ Bio data extraction interrupted, but basic data is saved")
        
        # Add summary stats
        self.add_summary_stats(total_count)
        
        # Cleanup
        self.cleanup()
        
        print("\n🎉 RealiteaseInfo Sheet Build Complete!")
        print(f"📊 Final Stats:")
        print(f"   👤 Unique Cast Members: {total_count}")
        print(f"   📺 Source Records: {len(self.viable_cast_data)}")
        print(f"   ⏰ Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    def extract_and_update_bio_data(self, cast_aggregation):
        """Extract bio data for cast members that don't already have it"""
        print("🔍 Checking existing bio data and extracting only missing data...")
        
        # Get current sheet data to see who already has bio data
        current_data = self.realitease_sheet.get_all_records()
        existing_bio_data = {}
        
        for record in current_data:
            cast_imdb_id = record.get('CastIMDbID', '')
            gender = record.get('Gender', '').strip()
            birthday = record.get('Birthday', '').strip()
            zodiac = record.get('Zodiac', '').strip()
            
            existing_bio_data[cast_imdb_id] = {
                'has_gender': bool(gender),
                'has_birthday': bool(birthday),
                'has_zodiac': bool(zodiac),
                'gender': gender,
                'birthday': birthday,
                'zodiac': zodiac
            }
        
        # Filter to only cast members that need bio data
        cast_needing_bio = []
        for cast_key, data in cast_aggregation.items():
            cast_imdb_id = data['cast_imdb_id']
            existing = existing_bio_data.get(cast_imdb_id, {})
            
            # Check if this cast member needs any bio data
            needs_gender = not existing.get('has_gender', False)
            needs_birthday = not existing.get('has_birthday', False)
            needs_zodiac = not existing.get('has_zodiac', False)
            
            if needs_gender or needs_birthday or needs_zodiac:
                cast_needing_bio.append({
                    'cast_key': cast_key,
                    'data': data,
                    'needs_gender': needs_gender,
                    'needs_birthday': needs_birthday,
                    'needs_zodiac': needs_zodiac,
                    'existing': existing
                })
        
        total_cast = len(cast_aggregation)
        need_bio = len(cast_needing_bio)
        have_bio = total_cast - need_bio
        
        print(f"📊 Bio data status: {have_bio}/{total_cast} already have complete bio data")
        print(f"🔍 Need to extract bio data for {need_bio} cast members")
        
        if need_bio == 0:
            print("✅ All cast members already have complete bio data!")
            return
        
        batch = []
        count = 0
        
        for item in cast_needing_bio:
            count += 1
            data = item['data']
            existing = item['existing']
            
            print(f"🔍 Extracting bio data for: {data['cast_name']} ({count}/{need_bio})")
            
            bio_data = self.extract_bio_data(data['cast_name'], data['cast_imdb_id'], data['cast_tmdb_id'])
            
            # Only update fields that are missing
            final_bio = {
                'gender': bio_data['gender'] if item['needs_gender'] else existing.get('gender', ''),
                'birthday': bio_data['birthday'] if item['needs_birthday'] else existing.get('birthday', ''),
                'zodiac': bio_data['zodiac'] if item['needs_zodiac'] else existing.get('zodiac', '')
            }
            
            # Add to batch if we got any new bio data
            if (item['needs_gender'] and bio_data['gender']) or \
               (item['needs_birthday'] and bio_data['birthday']) or \
               (item['needs_zodiac'] and bio_data['zodiac']):
                batch.append({
                    'cast_imdb_id': data['cast_imdb_id'],
                    'cast_name': data['cast_name'],
                    'bio_data': final_bio
                })
            
            # Update every 5 cast members
            if len(batch) >= 5:
                self.batch_update_bio_data(batch)
                batch = []
                time.sleep(0.8)  # 0.8 seconds between batches
        
        # Update any remaining cast members in the final batch
        if batch:
            self.batch_update_bio_data(batch)
        
        print(f"✅ Completed bio data extraction for {need_bio} cast members (preserved existing data)")
    
    def batch_update_bio_data(self, batch):
        """Update bio data for a batch of cast members in a single API call"""
        try:
            # Get current sheet data to find row numbers
            all_data = self.realitease_sheet.get_all_records()
            cast_row_map = {record.get('CastIMDbID'): i + 2 for i, record in enumerate(all_data)}
            
            # Prepare batch update
            updates = []
            for item in batch:
                cast_imdb_id = item['cast_imdb_id']
                bio_data = item['bio_data']
                
                if cast_imdb_id in cast_row_map:
                    row_num = cast_row_map[cast_imdb_id]
                    updates.append({
                        'range': f'H{row_num}:J{row_num}',
                        'values': [[bio_data['gender'], bio_data['birthday'], bio_data['zodiac']]]
                    })
            
            # Send batch update if we have any updates
            if updates:
                body = {
                    'valueInputOption': 'RAW',
                    'data': updates
                }
                self.realitease_sheet.spreadsheet.values_batch_update(body=body)
                
                cast_names = [item['cast_name'] for item in batch]
                print(f"  ✅ Batch updated {len(updates)} cast members: {', '.join(cast_names[:3])}{'...' if len(cast_names) > 3 else ''}")
                
        except Exception as e:
            print(f"  ⚠️ Batch update failed: {e}")
            # Fallback to individual updates with longer delay
            for item in batch:
                try:
                    self.update_bio_data_in_sheet(item['cast_imdb_id'], item['bio_data'])
                    time.sleep(1)  # Longer delay for individual fallback
                except Exception as e2:
                    print(f"  ⚠️ Failed to update {item['cast_name']}: {e2}")
                    time.sleep(5)  # Even longer wait for rate limit errors
    
    def update_bio_data_in_sheet(self, cast_imdb_id, bio_data):
        """Update bio data for a specific cast member in the sheet"""
        try:
            # Find the row for this cast member
            all_data = self.realitease_sheet.get_all_records()
            for i, record in enumerate(all_data):
                if record.get('CastIMDbID') == cast_imdb_id:
                    row_num = i + 2  # +2 for header and 1-indexing
                    
                    # Update all bio columns (H, I, J) in a single API call
                    bio_row = [bio_data['gender'], bio_data['birthday'], bio_data['zodiac']]
                    self.realitease_sheet.update(values=[bio_row], range_name=f'H{row_num}:J{row_num}')
                    
                    print(f"  ✅ Updated bio data for {record.get('CastName', 'unknown')}")
                    break
        except Exception as e:
            print(f"  ⚠️ Failed to update bio data for {cast_imdb_id}: {e}")
            time.sleep(5)  # Wait longer if we hit rate limits


def main():
    """Main entry point"""
    builder = None
    try:
        builder = RealiteaseInfoBuilder()
        builder.run()
    except KeyboardInterrupt:
        print("\n⚠️ Process interrupted by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")
    finally:
        if builder:
            builder.cleanup()


if __name__ == "__main__":
    main()
