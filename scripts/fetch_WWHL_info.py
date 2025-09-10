#!/usr/bin/env python3

import os
import sys
import time
import re
import requests
from dotenv import load_dotenv
import gspread
from google.oauth2.service_account import Credentials

# Load environment variables
load_env_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(load_env_path)

class WWHLExtractor:
    def __init__(self):
        self.tmdb_api_key = os.getenv('TMDB_API_KEY')
        self.tmdb_bearer = os.getenv('TMDB_BEARER')
        
        if not self.tmdb_bearer and not self.tmdb_api_key:
            raise ValueError("Neither TMDB_BEARER nor TMDB_API_KEY found in environment variables")
        
        self.base_url = "https://api.themoviedb.org/3"
        self.session = requests.Session()
        
        # Use bearer token if available, otherwise use API key
        if self.tmdb_bearer:
            self.session.headers.update({
                'Authorization': f'Bearer {self.tmdb_bearer}',
                'Content-Type': 'application/json'
            })
            print("🔑 Using TMDB Bearer token for authentication")
        else:
            self.session.headers.update({
                'Authorization': f'Bearer {self.tmdb_api_key}',
                'Content-Type': 'application/json'
            })
            print("🔑 Using TMDB API key for authentication")
        
        # Test API key first
        test_response = self.session.get(f"{self.base_url}/configuration")
        if test_response.status_code != 200:
            print(f"❌ TMDB API Key test failed: {test_response.status_code}")
            raise ValueError("Invalid TMDB API key or API access issue")
        else:
            print("✅ TMDB API Key validated successfully")
        
        # Google Sheets setup
        self.gc = None
        self.source_worksheet = None
        self.wwhl_worksheet = None
        
        # WWHL TMDb Show ID (constant)
        self.WWHL_SHOW_ID = "22980"
        
        # Processing counters
        self.processed_count = 0
        self.found_count = 0
        self.skipped_count = 0
        self.failed_count = 0
        
    def setup_google_sheets(self):
        """Setup Google Sheets connection"""
        try:
            print("🔄 WWHL: Setting up Google Sheets connection...")
            
            scope = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"
            ]
            
            key_file_path = os.path.join(os.path.dirname(__file__), '..', 'keys', 'trr-backend-df2c438612e1.json')
            credentials = Credentials.from_service_account_file(key_file_path, scopes=scope)
            
            self.gc = gspread.authorize(credentials)
            workbook = self.gc.open("Realitease2025Data")
            
            # Get source sheet (ViableCast)
            self.source_worksheet = workbook.worksheet("ViableCast")
            
                # Create or get WWHL sheet
            try:
                self.wwhl_worksheet = workbook.worksheet("WWHLinfo")
                print("✅ Found existing WWHLinfo sheet")
                
                # Check if headers exist, if not add them
                try:
                    first_row = self.wwhl_worksheet.row_values(1)
                    if not first_row or first_row[0] != "CastName":
                        # Add headers with all columns in correct order
                        headers = [
                            "CastName", 
                            "Cast IMDbID", 
                            "Cast TMDbID",
                            "Episode Marker", 
                            "OtherGuestNames",
                            "IMDb CastIDs of Other Guests", 
                            "TMDb CastIDs of Other Guests"
                        ]
                        self.wwhl_worksheet.update('A1:G1', [headers])
                        print("✅ Added headers to existing WWHLinfo sheet")
                except Exception as e:
                    print(f"⚠️ Could not check/update headers: {e}")
                
            except gspread.exceptions.WorksheetNotFound:
                print("🔄 Creating new WWHLinfo sheet...")
                self.wwhl_worksheet = workbook.add_worksheet(title="WWHLinfo", rows=1000, cols=10)
                # Add headers with all columns in correct order
                headers = [
                    "CastName", 
                    "Cast IMDbID", 
                    "Cast TMDbID",
                    "Episode Marker", 
                    "OtherGuestNames",
                    "IMDb CastIDs of Other Guests", 
                    "TMDb CastIDs of Other Guests"
                ]
                self.wwhl_worksheet.update('A1:G1', [headers])
                print("✅ Created WWHLinfo sheet with headers")
            
            print("✅ WWHL: Google Sheets connection successful")
            return True
            
        except Exception as e:
            print(f"❌ WWHL: Google Sheets setup failed: {str(e)}")
            return False

    def get_person_tmdb_credits(self, person_tmdb_id, cast_name):
        """Get TV credits for a person from TMDb (following tmdb_final_extractor pattern)"""
        try:
            print(f"🔍 WWHL: Getting TV credits for {cast_name} (TMDb ID: {person_tmdb_id})")
            
            # Validate that we have a TMDB Person ID
            if not person_tmdb_id or not person_tmdb_id.strip():
                print(f"❌ No TMDB Person ID provided for {cast_name}")
                return None
            
            credits_url = f"{self.base_url}/person/{person_tmdb_id}/tv_credits"
            response = self.session.get(credits_url)
            
            if response.status_code != 200:
                print(f"❌ WWHL: Failed to get TV credits for {cast_name}: {response.status_code}")
                return None
            
            credits_data = response.json()
            
            # Look for WWHL credits in both cast and crew (following tmdb_final_extractor pattern)
            cast_credits = []
            crew_credits = []
            
            # Check cast credits
            for credit in credits_data.get('cast', []):
                if str(credit.get('id')) == self.WWHL_SHOW_ID:
                    character = credit.get('character', '').lower()
                    # Look for "self" or guest appearances
                    if 'self' in character or 'guest' in character:
                        cast_credits.append(credit)
                        print(f"✅ WWHL: Found cast credit for {cast_name} - Character: {credit.get('character')}")
            
            # Check crew credits
            for credit in credits_data.get('crew', []):
                if str(credit.get('id')) == self.WWHL_SHOW_ID:
                    job = credit.get('job', '').lower()
                    # Look for guest appearances in crew roles
                    if 'guest' in job or 'self' in job:
                        crew_credits.append(credit)
                        print(f"✅ WWHL: Found crew credit for {cast_name} - Job: {credit.get('job')}")
            
            all_credits = cast_credits + crew_credits
            
            if not all_credits:
                print(f"⚠️ WWHL: No WWHL credits found for {cast_name}")
                return None
            
            return all_credits
            
        except Exception as e:
            print(f"❌ WWHL: Error getting TV credits for {cast_name}: {e}")
            return None

    def get_credit_details_improved(self, credit_id, cast_name):
        """Get detailed episode information from credit details (following tmdb_final_extractor pattern)"""
        try:
            print(f"🔍 WWHL: Getting credit details for {cast_name} (Credit ID: {credit_id})")
            credit_url = f"{self.base_url}/credit/{credit_id}"
            response = self.session.get(credit_url)
            
            if response.status_code != 200:
                print(f"❌ WWHL: Failed to get credit details for {cast_name}: {response.status_code}")
                return None
            
            credit_data = response.json()
            
            episodes = []
            
            # PRIORITY 1: Extract from episodes array (most reliable - following tmdb_final_extractor)
            episodes_array = credit_data.get('media', {}).get('episodes', [])
            if episodes_array:
                print(f"🎯 WWHL: Found {len(episodes_array)} episodes in episodes array")
                
                for episode in episodes_array:
                    episode_info = {
                        'season_number': episode.get('season_number'),
                        'episode_number': episode.get('episode_number'),
                        'air_date': episode.get('air_date'),
                        'name': episode.get('name', ''),
                        'episode_id': episode.get('id')
                    }
                    
                    # Only include episodes with valid season/episode numbers
                    if episode_info['season_number'] and episode_info['episode_number']:
                        episodes.append(episode_info)
                        print(f"📺 WWHL: Found episode S{episode_info['season_number']}E{episode_info['episode_number']} - {episode_info['name']}")
            
            # PRIORITY 2: Try seasons array if episodes array was empty
            if not episodes_array:
                seasons_array = credit_data.get('media', {}).get('seasons', [])
                if seasons_array:
                    print(f"🎯 WWHL: Found {len(seasons_array)} seasons in seasons array (no episode details)")
                    # Note: Seasons array doesn't give us individual episode details for WWHL
                    # This is mainly for completeness following tmdb_final_extractor pattern
            
            return episodes if episodes else None
            
        except Exception as e:
            print(f"❌ WWHL: Error getting credit details for {cast_name}: {e}")
            return None

    def get_episode_cast(self, season_number, episode_number):
        """Get cast information for a specific WWHL episode"""
        try:
            print(f"🎭 WWHL: Getting cast for S{season_number}E{episode_number}")
            episode_url = f"{self.base_url}/tv/{self.WWHL_SHOW_ID}/season/{season_number}/episode/{episode_number}/credits"
            response = self.session.get(episode_url)
            
            if response.status_code != 200:
                print(f"❌ WWHL: Failed to get episode cast for S{season_number}E{episode_number}: {response.status_code}")
                return None, None
            
            cast_data = response.json()
            
            # Andy Cohen's TMDb ID (host - should be filtered out)
            ANDY_COHEN_TMDB_ID = 54772
            
            # Extract guest cast from guest_stars array (this is where WWHL guests are listed!)
            guest_cast = []
            
            # Check guest_stars array (this is the key!)
            guest_stars = cast_data.get('guest_stars', [])
            print(f"🎯 WWHL: Found {len(guest_stars)} guest_stars in S{season_number}E{episode_number}")
            
            for guest in guest_stars:
                character = guest.get('character', '').lower()
                tmdb_id = guest.get('id')
                name = guest.get('name', '')
                
                print(f"🔍 WWHL: Checking guest_star: {name} (TMDb: {tmdb_id}) - Character: '{character}'")
                
                # Include guests with "Self" roles
                if 'self' in character or 'guest' in character:
                    guest_info = {
                        'name': guest.get('name'),
                        'tmdb_id': guest.get('id'),
                        'character': guest.get('character'),
                        'credit_id': guest.get('credit_id')
                    }
                    guest_cast.append(guest_info)
                    print(f"✅ WWHL: Added guest_star: {name} (TMDb: {tmdb_id}) - {guest.get('character')}")
            
            # Also check regular cast array (excluding Andy Cohen) for completeness
            all_cast = cast_data.get('cast', [])
            print(f"🔍 WWHL: Also checking {len(all_cast)} cast members for any additional guests")
            
            for cast_member in all_cast:
                character = cast_member.get('character', '').lower()
                tmdb_id = cast_member.get('id')
                name = cast_member.get('name', '')
                
                # Skip Andy Cohen (the host) and include only guests with "Self" roles
                if ('self' in character or 'guest' in character) and tmdb_id != ANDY_COHEN_TMDB_ID:
                    # Check if we already have this person from guest_stars
                    if not any(g.get('tmdb_id') == tmdb_id for g in guest_cast):
                        guest_info = {
                            'name': cast_member.get('name'),
                            'tmdb_id': cast_member.get('id'),
                            'character': cast_member.get('character'),
                            'credit_id': cast_member.get('credit_id')
                        }
                        guest_cast.append(guest_info)
                        print(f"✅ WWHL: Added cast guest: {name} (TMDb: {tmdb_id}) - {cast_member.get('character')}")
            
            # Also check crew for guests (excluding Andy Cohen)
            crew_members = cast_data.get('crew', [])
            for crew_member in crew_members:
                job = crew_member.get('job', '').lower()
                tmdb_id = crew_member.get('id')
                name = crew_member.get('name', '')
                
                # Skip Andy Cohen and include only guests
                if ('guest' in job or 'self' in job) and tmdb_id != ANDY_COHEN_TMDB_ID:
                    # Check if we already have this person
                    if not any(g.get('tmdb_id') == tmdb_id for g in guest_cast):
                        guest_info = {
                            'name': crew_member.get('name'),
                            'tmdb_id': crew_member.get('id'),
                            'job': crew_member.get('job'),
                            'credit_id': crew_member.get('credit_id')
                        }
                        guest_cast.append(guest_info)
                        print(f"✅ WWHL: Added crew guest: {name} (TMDb: {tmdb_id}) - {crew_member.get('job')}")
            
            print(f"👥 WWHL: Found {len(guest_cast)} total guests in S{season_number}E{episode_number}")
            return guest_cast, cast_data
            
        except Exception as e:
            print(f"❌ WWHL: Error getting episode cast for S{season_number}E{episode_number}: {e}")
            return None, None

    def get_imdb_ids_for_tmdb_ids(self, tmdb_ids):
        """Get IMDb IDs for a list of TMDb person IDs"""
        imdb_ids = []
        
        for tmdb_id in tmdb_ids:
            try:
                print(f"🔍 WWHL: Getting IMDb ID for TMDb person {tmdb_id}")
                person_url = f"{self.base_url}/person/{tmdb_id}/external_ids"
                response = self.session.get(person_url)
                
                if response.status_code == 200:
                    external_data = response.json()
                    imdb_id = external_data.get('imdb_id')
                    if imdb_id:
                        imdb_ids.append(imdb_id)
                        print(f"✅ WWHL: Found IMDb ID {imdb_id} for TMDb {tmdb_id}")
                    else:
                        print(f"⚠️ WWHL: No IMDb ID found for TMDb {tmdb_id}")
                        imdb_ids.append("")  # Keep position but empty
                else:
                    print(f"❌ WWHL: Failed to get external IDs for TMDb {tmdb_id}")
                    imdb_ids.append("")
                
                time.sleep(0.5)  # Rate limiting
                
            except Exception as e:
                print(f"❌ WWHL: Error getting IMDb ID for TMDb {tmdb_id}: {e}")
                imdb_ids.append("")
        
        return imdb_ids

    def process_cast_member(self, cast_name, cast_imdb_id, person_tmdb_id):
        """Process a single cast member to find their WWHL appearances (TMDb first approach)"""
        try:
            print(f"\n🎭 WWHL: Processing {cast_name} (IMDb: {cast_imdb_id}, TMDb: {person_tmdb_id})")
            
            # Get WWHL credits for this person using TMDb (primary method)
            wwhl_credits = self.get_person_tmdb_credits(person_tmdb_id, cast_name)
            
            if not wwhl_credits:
                print(f"⚠️ WWHL: No WWHL credits found for {cast_name}")
                return []
            
            appearances = []
            
            # Process each credit to get detailed episode information (following tmdb_final_extractor pattern)
            for credit in wwhl_credits:
                credit_id = credit.get('credit_id')
                episode_count = credit.get('episode_count', 0)
                
                print(f"🔍 WWHL: Processing credit - ID: {credit_id}, Episode count: {episode_count}")
                
                if credit_id:
                    # Get detailed episode information using improved logic (like tmdb_final_extractor)
                    print(f"🔍 WWHL: Getting detailed episodes for credit_id: {credit_id}")
                    episodes = self.get_credit_details_improved(credit_id, cast_name)
                    
                    if episodes:
                        # Process each episode appearance
                        for episode in episodes:
                            season_num = episode['season_number']
                            episode_num = episode['episode_number']
                            
                            if not season_num or not episode_num:
                                continue
                            
                            # Create episode marker
                            episode_marker = f"S{season_num}E{episode_num}"
                            
                            # Get other guests in this episode
                            episode_cast, _ = self.get_episode_cast(season_num, episode_num)
                            
                            # Filter out the current cast member and Andy Cohen from other guests
                            other_guests = []
                            if episode_cast:
                                for guest in episode_cast:
                                    guest_tmdb_id = guest.get('tmdb_id')
                                    # Exclude current cast member and Andy Cohen (54772)
                                    if (str(guest_tmdb_id) != str(person_tmdb_id) and 
                                        guest_tmdb_id != 54772):
                                        other_guests.append(guest)
                            
                            # Extract TMDb IDs, IMDb IDs, and Names
                            other_tmdb_ids = [str(guest.get('tmdb_id', '')) for guest in other_guests if guest.get('tmdb_id')]
                            other_guest_names = [guest.get('name', '') for guest in other_guests if guest.get('name')]
                            other_imdb_ids = self.get_imdb_ids_for_tmdb_ids(other_tmdb_ids) if other_tmdb_ids else []
                            
                            # Format the lists
                            guest_names_str = ", ".join(filter(None, other_guest_names))
                            tmdb_ids_str = ", ".join(filter(None, other_tmdb_ids))
                            imdb_ids_str = ", ".join(filter(None, other_imdb_ids))
                            
                            appearance = {
                                'cast_name': cast_name,
                                'cast_imdb_id': cast_imdb_id,
                                'cast_tmdb_id': person_tmdb_id,  # Add Cast TMDb ID
                                'episode_marker': episode_marker,
                                'other_guest_names': guest_names_str,  # Add guest names
                                'other_imdb_ids': imdb_ids_str,
                                'other_tmdb_ids': tmdb_ids_str
                            }
                            
                            appearances.append(appearance)
                            print(f"✅ WWHL: Found appearance - {episode_marker} with {len(other_guests)} other guests")
                    else:
                        # Fallback: if no detailed episodes but we have episode count, create generic entries
                        if episode_count > 0:
                            print(f"⚠️ WWHL: No detailed episodes found, but credit shows {episode_count} episodes")
                            # Note: Without detailed episode info, we can't determine specific episode markers
                            # This would require additional API calls or IMDb fallback
                else:
                    # No credit_id available - limited information
                    if episode_count > 0:
                        print(f"⚠️ WWHL: No credit_id but episode count: {episode_count}")
                
                time.sleep(1.0)  # Rate limiting between credits
            
            return appearances
            
        except Exception as e:
            print(f"❌ WWHL: Error processing {cast_name}: {e}")
            return []

    def write_appearances_to_sheet(self, appearances):
        """Write WWHL appearances to the WWHLinfo sheet"""
        if not appearances:
            return
        
        try:
            # Get current data to find next empty row
            existing_data = self.wwhl_worksheet.get_all_values()
            next_row = len(existing_data) + 1
            
            # Prepare data for batch update
            rows_to_add = []
            for appearance in appearances:
                row = [
                    appearance['cast_name'],
                    appearance['cast_imdb_id'],
                    appearance['cast_tmdb_id'],  # Cast TMDb ID
                    appearance['episode_marker'],
                    appearance['other_guest_names'],  # Guest names
                    appearance['other_imdb_ids'],
                    appearance['other_tmdb_ids']
                ]
                rows_to_add.append(row)
            
            if rows_to_add:
                # Batch update (now A1:G1 for 7 columns)
                range_name = f"A{next_row}:G{next_row + len(rows_to_add) - 1}"
                self.wwhl_worksheet.update(range_name, rows_to_add)
                print(f"✅ WWHL: Added {len(rows_to_add)} appearances to sheet")
            
        except Exception as e:
            print(f"❌ WWHL: Error writing to sheet: {e}")

    def process_all_cast(self):
        """Process all cast members from ViableCast sheet to find WWHL appearances"""
        if not self.setup_google_sheets():
            return False
        
        try:
            # Get all data from ViableCast sheet
            all_data = self.source_worksheet.get_all_values()
            
            if len(all_data) < 2:
                print(f"❌ ViableCast sheet only has {len(all_data)} rows, need at least 2 (header + data)")
                return False
            
            # Dynamic header mapping (like v2UniversalSeasonExtractorMiddleDownAllFormats)
            headers = [h.strip().lower() for h in all_data[0]]
            header_map = {h: i for i, h in enumerate(headers)}
            
            def col_of(fragment, default=None):
                for k, idx in header_map.items():
                    if fragment in k:
                        return idx
                return default
            
            # Find column indexes dynamically
            idx_cast_name = col_of("castname")
            idx_cast_imdb = col_of("cast imdbid") 
            idx_cast_tmdb = col_of("castid")  # TMDb Person ID is in CastID column
            
            if idx_cast_name is None or idx_cast_tmdb is None:
                print("❌ Header lookup failed. Ensure headers 'CastName' and 'CastID' exist in ViableCast sheet.")
                return False
            
            print(f"📊 WWHL: Processing cast members from ViableCast sheet")
            print(f"📊 WWHL: Found columns - CastName: {idx_cast_name}, Cast IMDbID: {idx_cast_imdb}, CastID (TMDb): {idx_cast_tmdb}")
            print(f"📊 WWHL: Total rows to check: {len(all_data) - 1}")
            
            batch_appearances = []
            
            # Process each row
            for row_num in range(1, len(all_data)):  # Skip header row
                row = all_data[row_num]
                
                self.processed_count += 1
                
                # Parse row data using dynamic column indexes
                def safe_get(i):
                    return row[i].strip() if i is not None and i < len(row) else ""
                
                cast_name = safe_get(idx_cast_name)
                cast_imdb_id = safe_get(idx_cast_imdb) 
                person_tmdb_id = safe_get(idx_cast_tmdb)
                
                # Validate required data
                if not all([cast_name, person_tmdb_id]):
                    print(f"⚠️ WWHL: Row {row_num + 1} - missing required data (CastName: '{cast_name}', TMDb ID: '{person_tmdb_id}')")
                    self.skipped_count += 1
                    continue
                
                # Process this cast member for WWHL appearances
                appearances = self.process_cast_member(cast_name, cast_imdb_id, person_tmdb_id)
                
                if appearances:
                    batch_appearances.extend(appearances)
                    self.found_count += len(appearances)
                    print(f"✅ WWHL: Found {len(appearances)} WWHL appearances for {cast_name}")
                else:
                    self.skipped_count += 1
                
                # Write batch every 10 appearances to see data sooner
                if len(batch_appearances) >= 10:
                    self.write_appearances_to_sheet(batch_appearances)
                    batch_appearances = []
                
                # Add delay between cast members to avoid rate limiting (like tmdb_final_extractor)
                time.sleep(2.0)  # Increased delay for API stability
                
                # Progress update every 10 cast members
                if self.processed_count % 10 == 0:
                    print(f"📈 WWHL Progress: {self.processed_count} processed, {self.found_count} appearances found, {self.skipped_count} skipped")
            
            # Write any remaining appearances
            if batch_appearances:
                self.write_appearances_to_sheet(batch_appearances)
            
            # Final summary
            print(f"\n🎉 WWHL: Processing complete!")
            print(f"📊 Total cast members processed: {self.processed_count}")
            print(f"✅ WWHL appearances found: {self.found_count}")
            print(f"⏭️ Cast members skipped (no WWHL appearances): {self.skipped_count}")
            
            return True
            
        except Exception as e:
            print(f"❌ Error processing cast members: {e}")
            return False

def main():
    """Main function"""
    extractor = WWHLExtractor()
    
    print("🚀 Starting WWHL Extractor...")
    print(f"🎯 Looking for WWHL appearances (Show ID: {extractor.WWHL_SHOW_ID})")
    print(f"🔍 Searching for 'Self' and guest roles")
    print(f"📝 Results will be written to WWHLinfo sheet")
    
    success = extractor.process_all_cast()
    
    if success:
        print("✅ WWHL extraction completed successfully!")
    else:
        print("❌ WWHL extraction failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
