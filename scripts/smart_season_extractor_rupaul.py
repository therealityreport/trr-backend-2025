#!/usr/bin/env python3
"""
Smart Season Extractor - RuPaul's Drag Race Section
Efficiently searches for specific IMDb IDs on the page instead of loading all cast.
Accurately extracts seasons by clicking episode buttons and parsing season details.
"""

import os
import sys
import time
import random
import re
from collections import defaultdict
import gspread
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from google.auth.exceptions import RefreshError

class SmartSeasonExtractorRuPaul:
    def __init__(self):
        """Initialize the smart RuPaul extractor with targeted IMDb ID search"""
        self.driver = None
        self.sheet = None
        self.viable_cast_data = []
        self.processed_count = 0
        self.error_count = 0
        self.service_account_file = '/Users/thomashulihan/Projects/TRR-Backend/keys/trr-backend-df2c438612e1.json'
        
        # Anti-timeout configurations
        self.max_retries = 3
        self.base_delay = 2
        self.max_delay = 8
        self.request_timeout = 15
        self.page_load_timeout = 20
        
        print("🎭 Smart RuPaul Extractor: Initializing with targeted IMDb ID search...")
        
    def setup_google_sheets(self):
        """Setup Google Sheets connection with error handling"""
        print("🔄 Smart RuPaul Extractor: Setting up Google Sheets connection...")
        
        try:
            if not os.path.exists(self.service_account_file):
                raise FileNotFoundError(f"Service account file not found: {self.service_account_file}")
            
            gc = gspread.service_account(filename=self.service_account_file)
            workbook = gc.open("Realitease2025Data")
            self.sheet = workbook.worksheet("ViableCast")
            
            print("✅ Smart RuPaul Extractor: Google Sheets connection successful")
            return True
            
        except Exception as e:
            print(f"❌ Smart RuPaul Extractor: Google Sheets setup failed: {str(e)}")
            return False
    
    def setup_webdriver(self):
        """Setup WebDriver with enhanced anti-detection"""
        print("🔄 Smart RuPaul Extractor: Setting up WebDriver with enhanced anti-detection...")
        
        try:
            chrome_options = Options()
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            chrome_options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            self.driver.implicitly_wait(10)
            self.driver.set_page_load_timeout(self.page_load_timeout)
            
            print("✅ Smart RuPaul Extractor: WebDriver setup successful")
            return True
            
        except Exception as e:
            print(f"❌ Smart RuPaul Extractor: WebDriver setup failed: {str(e)}")
            return False
    
    def smart_delay(self, base=None):
        """Implement smart delays to avoid rate limiting"""
        if base is None:
            base = self.base_delay
        delay = base + random.uniform(0.5, 2.0)
        time.sleep(delay)
    
    def load_viable_cast_data_from_row(self, start_row):
        """Load ViableCast data starting from specified row with analysis"""
        print(f"🔄 Smart RuPaul Extractor: Loading ViableCast data starting from row {start_row}...")
        
        try:
            # Get all data using raw values to handle duplicate headers
            all_data = self.sheet.get_all_values()
            
            print(f"📋 Smart RuPaul Extractor: Processing {len(all_data)} total rows...")
            
            # Get headers and find column positions
            headers = all_data[0] if all_data else []
            header_mapping = {}
            
            for i, header in enumerate(headers):
                normalized_header = str(header).strip().lower()
                print(f"  Column {i}: '{header}' -> '{normalized_header}'")
                header_mapping[normalized_header] = i
            
            # Find essential columns
            show_imdbid_col = None
            castname_col = None
            cast_imdbid_col = None
            episode_count_col = None
            seasons_col = None
            
            for norm_header, col_idx in header_mapping.items():
                if 'show imdbid' in norm_header:
                    show_imdbid_col = col_idx
                elif 'castname' in norm_header:
                    castname_col = col_idx
                elif 'cast imdbid' in norm_header:
                    cast_imdbid_col = col_idx
                elif 'episodecount' in norm_header:
                    episode_count_col = col_idx
                elif 'seasons' in norm_header:
                    seasons_col = col_idx
            
            if show_imdbid_col is None:
                print("❌ Smart RuPaul Extractor: Could not find Show IMDbID column")
                return None
            
            print(f"✅ Found Show IMDbID at column {show_imdbid_col}")
            print(f"📋 Smart RuPaul Extractor: Column mapping - Show IMDbID: {show_imdbid_col}, CastName: {castname_col}, Cast IMDbID: {cast_imdbid_col}, EpisodeCount: {episode_count_col}, Seasons: {seasons_col}")
            
            # Filter data from start_row onward
            filtered_data = []
            for i in range(start_row - 1, len(all_data)):  # -1 because row numbers are 1-indexed
                row = all_data[i]
                if len(row) > show_imdbid_col:
                    filtered_data.append({
                        'row_number': i + 1,
                        'show_imdbid': row[show_imdbid_col] if len(row) > show_imdbid_col else '',
                        'cast_name': row[castname_col] if castname_col is not None and len(row) > castname_col else '',
                        'cast_imdbid': row[cast_imdbid_col] if cast_imdbid_col is not None and len(row) > cast_imdbid_col else '',
                        'episode_count': row[episode_count_col] if episode_count_col is not None and len(row) > episode_count_col else '',
                        'seasons': row[seasons_col] if seasons_col is not None and len(row) > seasons_col else ''
                    })
            
            print(f"📊 Smart RuPaul Extractor: Filtered to {len(filtered_data)} records from row {start_row} onward")
            
            # Analyze completion status
            total_records = len(filtered_data)
            already_complete = 0
            need_processing = 0
            unique_shows = set()
            
            for record in filtered_data:
                unique_shows.add(record['show_imdbid'])
                if record['episode_count'] and record['seasons']:
                    already_complete += 1
                else:
                    need_processing += 1
            
            print(f"📊 Smart RuPaul Extractor: Analysis complete:")
            print(f"  📝 Total records processed: {total_records}")
            print(f"  ✅ Already complete: {already_complete}")
            print(f"  ❌ Need processing: {need_processing}")
            print(f"  📺 Shows needing processing: {len(unique_shows)}")
            
            return {
                'data': filtered_data,
                'stats': {
                    'total': total_records,
                    'complete': already_complete,
                    'need_processing': need_processing,
                    'unique_shows': len(unique_shows)
                },
                'columns': {
                    'show_imdbid': show_imdbid_col,
                    'cast_name': castname_col,
                    'cast_imdbid': cast_imdbid_col,
                    'episode_count': episode_count_col,
                    'seasons': seasons_col
                }
            }
            
        except Exception as e:
            print(f"❌ Smart RuPaul Extractor: Error loading data: {str(e)}")
            return None

    def search_for_cast_member_by_imdb_id(self, show_imdb_id, cast_imdb_id, cast_name):
        """
        Search for a specific cast member by their IMDb ID on the show's full credits page.
        This is much more efficient than loading all cast members.
        """
        try:
            print(f"🔍 Smart RuPaul Extractor: Searching for {cast_name} ({cast_imdb_id}) in {show_imdb_id}")
            
            # Load the full credits page
            url = f"https://www.imdb.com/title/{show_imdb_id}/fullcredits"
            print(f"🔄 Smart RuPaul Extractor: Loading page: {url}")
            
            for attempt in range(self.max_retries):
                try:
                    self.driver.get(url)
                    self.smart_delay(3)
                    print(f"✅ Smart RuPaul Extractor: Page loaded successfully")
                    break
                except Exception as e:
                    if attempt < self.max_retries - 1:
                        print(f"⚠️ Smart RuPaul Extractor: Load attempt {attempt + 1} failed, retrying...")
                        self.smart_delay(5)
                    else:
                        print(f"❌ Smart RuPaul Extractor: Failed to load page after {self.max_retries} attempts")
                        return None
            
            # Method 1: Search by IMDb ID in href attributes
            if cast_imdb_id:
                print(f"🎯 Smart RuPaul Extractor: Searching for IMDb ID: {cast_imdb_id}")
                try:
                    # Look for links containing the cast member's IMDb ID
                    cast_link = self.driver.find_element(By.XPATH, f"//a[contains(@href, '/name/{cast_imdb_id}/')]")
                    print(f"✅ Smart RuPaul Extractor: Found cast member by IMDb ID!")
                    return self.extract_episode_data_from_cast_element(cast_link, cast_name)
                except NoSuchElementException:
                    print(f"⚠️ Smart RuPaul Extractor: Cast member not found by IMDb ID")
            
            # Method 2: Search by name in the cast list
            if cast_name:
                print(f"🎯 Smart RuPaul Extractor: Searching for name: {cast_name}")
                try:
                    # Look for the cast member's name
                    name_variations = [
                        cast_name,
                        cast_name.replace("'", "'"),  # Different apostrophe
                        cast_name.replace("'", ""),   # No apostrophe
                    ]
                    
                    for name_variant in name_variations:
                        try:
                            cast_link = self.driver.find_element(By.XPATH, f"//a[contains(text(), '{name_variant}')]")
                            print(f"✅ Smart RuPaul Extractor: Found cast member by name: {name_variant}")
                            return self.extract_episode_data_from_cast_element(cast_link, cast_name)
                        except NoSuchElementException:
                            continue
                    
                    print(f"⚠️ Smart RuPaul Extractor: Cast member not found by name")
                except Exception as e:
                    print(f"⚠️ Smart RuPaul Extractor: Name search error: {str(e)}")
            
            print(f"❌ Smart RuPaul Extractor: Could not find cast member {cast_name}")
            return None
            
        except Exception as e:
            print(f"❌ Smart RuPaul Extractor: Error searching for cast member: {str(e)}")
            return None

    def extract_episode_data_from_cast_element(self, cast_link_element, cast_name):
        """
        Extract episode count and seasons from the cast member's element.
        Click the episode button to get accurate season information.
        """
        try:
            print(f"🎭 Smart RuPaul Extractor: Extracting data for {cast_name}")
            
            # Find the parent container that holds the episode information
            try:
                # Look for the episode button in the same container
                parent_container = cast_link_element.find_element(By.XPATH, "./ancestor::li[contains(@class, 'full-credits-page-list-item')]")
                
                # Look for episode button within this container
                episode_buttons = parent_container.find_elements(By.XPATH, ".//button[contains(text(), 'episode')]")
                
                if episode_buttons:
                    episode_button = episode_buttons[0]
                    button_text = episode_button.text.strip()
                    print(f"🔍 Smart RuPaul Extractor: Found episode button: '{button_text}'")
                    
                    # Extract episode count from button text
                    episode_match = re.search(r'(\d+)\s+episodes?', button_text)
                    if episode_match:
                        episode_count = int(episode_match.group(1))
                        print(f"✅ Smart RuPaul Extractor: Episode count: {episode_count}")
                        
                        # Click the button to get detailed season information
                        seasons = self.click_episode_button_and_extract_seasons(episode_button, cast_name)
                        
                        return {
                            'episode_count': episode_count,
                            'seasons': seasons,
                            'found': True
                        }
                    else:
                        print(f"⚠️ Smart RuPaul Extractor: Could not parse episode count from: '{button_text}'")
                else:
                    print(f"⚠️ Smart RuPaul Extractor: No episode button found for {cast_name}")
                    
                    # Fallback: look for episode information in text
                    container_text = parent_container.text
                    episode_match = re.search(r'(\d+)\s+episodes?', container_text)
                    if episode_match:
                        episode_count = int(episode_match.group(1))
                        print(f"✅ Smart RuPaul Extractor: Found episode count in text: {episode_count}")
                        return {
                            'episode_count': episode_count,
                            'seasons': None,
                            'found': True
                        }
                
            except NoSuchElementException as e:
                print(f"⚠️ Smart RuPaul Extractor: Could not find parent container: {str(e)}")
            
            print(f"❌ Smart RuPaul Extractor: Could not extract episode data for {cast_name}")
            return None
            
        except Exception as e:
            print(f"❌ Smart RuPaul Extractor: Error extracting episode data: {str(e)}")
            return None

    def click_episode_button_and_extract_seasons(self, episode_button, cast_name):
        """
        Click the episode button and extract accurate season information from the modal.
        Parse season details like 'S4.E1' to determine actual seasons.
        """
        try:
            print(f"🖱️ Smart RuPaul Extractor: Clicking episode button for {cast_name}")
            
            # Scroll to button and click
            self.driver.execute_script("arguments[0].scrollIntoView(true);", episode_button)
            self.smart_delay(1)
            
            try:
                episode_button.click()
                self.smart_delay(2)
                print(f"✅ Smart RuPaul Extractor: Episode button clicked")
                
                # Look for the modal or expanded content with season details
                seasons_found = set()
                
                # Method 1: Look for season episode patterns like "S4.E1"
                try:
                    season_elements = self.driver.find_elements(By.XPATH, "//li[contains(text(), 'S') and contains(text(), '.E')]")
                    for season_element in season_elements:
                        season_text = season_element.text
                        season_matches = re.findall(r'S(\d+)\.E\d+', season_text)
                        for season_num in season_matches:
                            seasons_found.add(int(season_num))
                            print(f"🔍 Smart RuPaul Extractor: Found season {season_num} from: {season_text}")
                    
                    if seasons_found:
                        min_season = min(seasons_found)
                        max_season = max(seasons_found)
                        if min_season == max_season:
                            season_result = str(min_season)
                        else:
                            season_result = f"{min_season}-{max_season}"
                        print(f"✅ Smart RuPaul Extractor: Extracted seasons: {season_result}")
                        return season_result
                        
                except Exception as e:
                    print(f"⚠️ Smart RuPaul Extractor: Season parsing method 1 failed: {str(e)}")
                
                # Method 2: Look for year ranges and estimate seasons
                try:
                    year_elements = self.driver.find_elements(By.XPATH, "//*[contains(text(), '20') and contains(text(), '–')]")
                    for year_element in year_elements:
                        year_text = year_element.text
                        year_match = re.search(r'(\d{4})–(\d{4})', year_text)
                        if year_match:
                            start_year = int(year_match.group(1))
                            end_year = int(year_match.group(2))
                            estimated_seasons = end_year - start_year + 1
                            if estimated_seasons > 1:
                                season_result = f"1-{estimated_seasons}"
                            else:
                                season_result = "1"
                            print(f"✅ Smart RuPaul Extractor: Estimated seasons from years {start_year}-{end_year}: {season_result}")
                            return season_result
                except Exception as e:
                    print(f"⚠️ Smart RuPaul Extractor: Season estimation failed: {str(e)}")
                
                # Close any modal that might have opened
                try:
                    close_buttons = self.driver.find_elements(By.XPATH, "//button[contains(@aria-label, 'Close') or contains(@class, 'close')]")
                    if close_buttons:
                        close_buttons[0].click()
                        self.smart_delay(1)
                except:
                    pass
                
            except Exception as e:
                print(f"⚠️ Smart RuPaul Extractor: Button click failed: {str(e)}")
            
            print(f"⚠️ Smart RuPaul Extractor: Could not extract season information for {cast_name}")
            return None
            
        except Exception as e:
            print(f"❌ Smart RuPaul Extractor: Error clicking episode button: {str(e)}")
            return None

    def update_cast_member_data(self, row_number, episode_count, seasons):
        """Update cast member data in the spreadsheet with proper API formatting"""
        try:
            # Update Episode Count (Column G)
            if episode_count is not None:
                range_name = f'ViableCast!G{row_number}:G{row_number}'
                values = [[str(episode_count)]]
                self.sheet.update(range_name, values, value_input_option='RAW')
                print(f"📝 Smart RuPaul Extractor: Updated Episode Count for row {row_number}: {episode_count}")
            
            # Update Seasons (Column H)
            if seasons:
                range_name = f'ViableCast!H{row_number}:H{row_number}'
                values = [[str(seasons)]]
                self.sheet.update(range_name, values, value_input_option='RAW')
                print(f"📝 Smart RuPaul Extractor: Updated Seasons for row {row_number}: {seasons}")
            
            # Small delay to respect API limits
            self.smart_delay(1)
            
            return True
            
        except Exception as e:
            print(f"❌ Smart RuPaul Extractor: Error updating row {row_number}: {str(e)}")
            return False

    def run_extraction(self, start_row=2598):
        """Main extraction process with smart IMDb ID search"""
        print(f"🚀 Smart RuPaul Extractor: Starting smart extraction from row {start_row}")
        print(f"🎯 Smart RuPaul Extractor: Target show - RuPaul's Drag Race (tt1353056)")
        
        try:
            # Setup connections
            if not self.setup_google_sheets():
                return False
            
            if not self.setup_webdriver():
                return False
            
            # Load data
            shows_analysis = self.load_viable_cast_data_from_row(start_row)
            if not shows_analysis:
                return False
            
            cast_members = shows_analysis['data']
            
            # Filter to only members needing processing
            members_to_process = [
                member for member in cast_members 
                if not member['episode_count'] or not member['seasons']
            ]
            
            print(f"📊 Smart RuPaul Extractor: Found {len(members_to_process)} cast members to process")
            
            if not members_to_process:
                print("✅ Smart RuPaul Extractor: All cast members already have complete data!")
                return True
            
            # Process in smaller batches
            batch_size = 5
            total_processed = 0
            
            for i in range(0, len(members_to_process), batch_size):
                batch = members_to_process[i:i+batch_size]
                batch_num = (i // batch_size) + 1
                
                print(f"\n🔄 Smart RuPaul Extractor: Processing batch {batch_num} ({len(batch)} members)")
                
                for member in batch:
                    try:
                        cast_name = member['cast_name']
                        cast_imdb_id = member['cast_imdbid']
                        show_imdb_id = member['show_imdbid']
                        row_number = member['row_number']
                        
                        print(f"\n🎭 Smart RuPaul Extractor: Processing {cast_name} from {show_imdb_id} (Row {row_number})")
                        
                        # Smart search for this specific cast member
                        result = self.search_for_cast_member_by_imdb_id(show_imdb_id, cast_imdb_id, cast_name)
                        
                        if result and result.get('found'):
                            episode_count = result.get('episode_count')
                            seasons = result.get('seasons')
                            
                            print(f"✅ Smart RuPaul Extractor: Found {cast_name} - Episodes: {episode_count}, Seasons: {seasons}")
                            
                            # Update spreadsheet
                            if self.update_cast_member_data(row_number, episode_count, seasons):
                                self.processed_count += 1
                            else:
                                self.error_count += 1
                        else:
                            print(f"⚠️ Smart RuPaul Extractor: Could not find episode data for {cast_name}")
                            self.error_count += 1
                        
                        total_processed += 1
                        
                        # Progress update
                        if total_processed % 5 == 0:
                            print(f"📈 Smart RuPaul Extractor: Progress - Processed: {self.processed_count}, Errors: {self.error_count}, Total: {total_processed}")
                        
                        # Short delay between members
                        self.smart_delay(2)
                        
                    except KeyboardInterrupt:
                        print("\n⏹️ Smart RuPaul Extractor: Interrupted by user")
                        return False
                    except Exception as e:
                        print(f"❌ Smart RuPaul Extractor: Error processing {member.get('cast_name', 'Unknown')}: {str(e)}")
                        self.error_count += 1
                        continue
                
                # Longer delay between batches
                if i + batch_size < len(members_to_process):
                    print(f"⏸️ Smart RuPaul Extractor: Batch complete, pausing before next batch...")
                    self.smart_delay(5)
            
            # Final summary
            print(f"\n🎉 Smart RuPaul Extractor: Extraction complete!")
            print(f"✅ Successfully processed: {self.processed_count}")
            print(f"❌ Errors encountered: {self.error_count}")
            print(f"📊 Total attempted: {total_processed}")
            
            return True
            
        except KeyboardInterrupt:
            print("\n⏹️ Smart RuPaul Extractor: Extraction interrupted by user")
            return False
        except Exception as e:
            print(f"❌ Smart RuPaul Extractor: Fatal error: {str(e)}")
            return False
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Clean up resources"""
        print("🧹 Smart RuPaul Extractor: Cleaning up resources...")
        if self.driver:
            try:
                self.driver.quit()
                print("✅ Smart RuPaul Extractor: WebDriver closed")
            except:
                pass

def main():
    """Main function"""
    extractor = SmartSeasonExtractorRuPaul()
    
    try:
        success = extractor.run_extraction(start_row=2598)
        if success:
            print("🎉 Smart RuPaul Extractor: Process completed successfully!")
        else:
            print("❌ Smart RuPaul Extractor: Process failed")
            sys.exit(1)
    except KeyboardInterrupt:
        print("\n⏹️ Smart RuPaul Extractor: Process interrupted")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Smart RuPaul Extractor: Unexpected error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
