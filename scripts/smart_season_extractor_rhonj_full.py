#!/usr/bin/env python3
"""
Smart Season Extractor - RHONJ Full Version
Starts from the beginning of the spreadsheet and processes all shows.
Handles both traditional table format and new React-based IMDb layouts.
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
from selenium.webdriver.common.keys import Keys

class SmartSeasonExtractorRHONJFull:
    def __init__(self):
        """Initialize the smart RHONJ extractor with dual format support"""
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
        
        print("🎭 Smart RHONJ Full Extractor: Initializing with dual format support...")
        
    def setup_google_sheets(self):
        """Setup Google Sheets connection with error handling"""
        print("🔄 Smart RHONJ Full Extractor: Setting up Google Sheets connection...")
        
        try:
            if not os.path.exists(self.service_account_file):
                raise FileNotFoundError(f"Service account file not found: {self.service_account_file}")
            
            gc = gspread.service_account(filename=self.service_account_file)
            workbook = gc.open("Realitease2025Data")
            self.sheet = workbook.worksheet("ViableCast")
            
            print("✅ Smart RHONJ Full Extractor: Google Sheets connection successful")
            return True
            
        except Exception as e:
            print(f"❌ Smart RHONJ Full Extractor: Google Sheets setup failed: {str(e)}")
            return False
    
    def setup_webdriver(self):
        """Setup WebDriver with enhanced anti-detection"""
        print("🔄 Smart RHONJ Full Extractor: Setting up WebDriver with enhanced anti-detection...")
        
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
            
            print("✅ Smart RHONJ Full Extractor: WebDriver setup successful")
            return True
            
        except Exception as e:
            print(f"❌ Smart RHONJ Full Extractor: WebDriver setup failed: {str(e)}")
            return False
    
    def smart_delay(self, base=None):
        """Implement smart delays to avoid rate limiting"""
        if base is None:
            base = self.base_delay
        delay = base + random.uniform(0.5, 2.0)
        time.sleep(delay)
    
    def load_viable_cast_data_from_beginning(self):
        """Load ViableCast data starting from the beginning of the spreadsheet"""
        print(f"🔄 Smart RHONJ Full Extractor: Loading ViableCast data from the beginning...")
        
        try:
            # Get all data using raw values to handle duplicate headers
            all_data = self.sheet.get_all_values()
            
            print(f"📋 Smart RHONJ Full Extractor: Processing {len(all_data)} total rows...")
            
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
                print("❌ Smart RHONJ Full Extractor: Could not find Show IMDbID column")
                return None
            
            print(f"✅ Found Show IMDbID at column {show_imdbid_col}")
            print(f"📋 Smart RHONJ Full Extractor: Column mapping - Show IMDbID: {show_imdbid_col}, CastName: {castname_col}, Cast IMDbID: {cast_imdbid_col}, EpisodeCount: {episode_count_col}, Seasons: {seasons_col}")
            
            # Process all data from beginning (skip header row)
            filtered_data = []
            for i in range(1, len(all_data)):  # Start from row 1 (skip header)
                row = all_data[i]
                if len(row) > show_imdbid_col:
                    filtered_data.append({
                        'row_number': i + 1,  # 1-indexed row number
                        'show_imdbid': row[show_imdbid_col] if len(row) > show_imdbid_col else '',
                        'cast_name': row[castname_col] if castname_col is not None and len(row) > castname_col else '',
                        'cast_imdbid': row[cast_imdbid_col] if cast_imdbid_col is not None and len(row) > cast_imdbid_col else '',
                        'episode_count': row[episode_count_col] if episode_count_col is not None and len(row) > episode_count_col else '',
                        'seasons': row[seasons_col] if seasons_col is not None and len(row) > seasons_col else ''
                    })
            
            print(f"📊 Smart RHONJ Full Extractor: Loaded {len(filtered_data)} records from the beginning")
            
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
            
            print(f"📊 Smart RHONJ Full Extractor: Analysis complete:")
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
            print(f"❌ Smart RHONJ Full Extractor: Error loading data: {str(e)}")
            return None

    def search_for_cast_member_dual_format(self, show_imdb_id, cast_imdb_id, cast_name):
        """
        Search for a specific cast member using both traditional and new React formats.
        Handles both IMDb layout types efficiently.
        """
        try:
            print(f"🔍 Smart RHONJ Full Extractor: Searching for {cast_name} ({cast_imdb_id}) in {show_imdb_id}")
            
            # Load the full credits page
            url = f"https://www.imdb.com/title/{show_imdb_id}/fullcredits"
            print(f"🔄 Smart RHONJ Full Extractor: Loading page: {url}")
            
            for attempt in range(self.max_retries):
                try:
                    self.driver.get(url)
                    self.smart_delay(3)
                    print(f"✅ Smart RHONJ Full Extractor: Page loaded successfully")
                    break
                except Exception as e:
                    if attempt < self.max_retries - 1:
                        print(f"⚠️ Smart RHONJ Full Extractor: Load attempt {attempt + 1} failed, retrying...")
                        self.smart_delay(5)
                    else:
                        print(f"❌ Smart RHONJ Full Extractor: Failed to load page after {self.max_retries} attempts")
                        return None
            
            # Method 1: Search by IMDb ID in href attributes (works for both formats)
            if cast_imdb_id:
                print(f"🎯 Smart RHONJ Full Extractor: Searching for IMDb ID: {cast_imdb_id}")
                try:
                    # Look for links containing the cast member's IMDb ID
                    cast_link = self.driver.find_element(By.XPATH, f"//a[contains(@href, '/name/{cast_imdb_id}/')]")
                    print(f"✅ Smart RHONJ Full Extractor: Found cast member by IMDb ID!")
                    return self.extract_episode_data_dual_format(cast_link, cast_name)
                except NoSuchElementException:
                    print(f"⚠️ Smart RHONJ Full Extractor: Cast member not found by IMDb ID")
            
            # Method 2: Search by name in the cast list (works for both formats)
            if cast_name:
                print(f"🎯 Smart RHONJ Full Extractor: Searching for name: {cast_name}")
                try:
                    # Look for the cast member's name with variations
                    name_variations = [
                        cast_name,
                        cast_name.replace("'", "'"),  # Different apostrophe
                        cast_name.replace("'", ""),   # No apostrophe
                    ]
                    
                    for name_variant in name_variations:
                        try:
                            cast_link = self.driver.find_element(By.XPATH, f"//a[contains(text(), '{name_variant}')]")
                            print(f"✅ Smart RHONJ Full Extractor: Found cast member by name: {name_variant}")
                            return self.extract_episode_data_dual_format(cast_link, cast_name)
                        except NoSuchElementException:
                            continue
                    
                    print(f"⚠️ Smart RHONJ Full Extractor: Cast member not found by name")
                except Exception as e:
                    print(f"⚠️ Smart RHONJ Full Extractor: Name search error: {str(e)}")
            
            print(f"❌ Smart RHONJ Full Extractor: Could not find cast member {cast_name}")
            return None
            
        except Exception as e:
            print(f"❌ Smart RHONJ Full Extractor: Error searching for cast member: {str(e)}")
            return None

    def extract_episode_data_dual_format(self, cast_link_element, cast_name):
        """
        Extract episode count and seasons from cast member element.
        Handles both traditional table format and new React-based format.
        """
        try:
            print(f"🎭 Smart RHONJ Full Extractor: Extracting data for {cast_name}")
            
            # Try New React Format First (most common now)
            print(f"🔄 Smart RHONJ Full Extractor: Trying new React format...")
            result = self.extract_from_react_format(cast_link_element, cast_name)
            if result and result.get('found'):
                return result
            
            # Try Traditional Table Format as fallback
            print(f"🔄 Smart RHONJ Full Extractor: Trying traditional table format...")
            result = self.extract_from_traditional_format(cast_link_element, cast_name)
            if result and result.get('found'):
                return result
            
            print(f"❌ Smart RHONJ Full Extractor: Could not extract episode data for {cast_name}")
            return None
            
        except Exception as e:
            print(f"❌ Smart RHONJ Full Extractor: Error extracting episode data: {str(e)}")
            return None

    def extract_from_react_format(self, cast_link_element, cast_name):
        """Extract data from new React-based IMDb format"""
        try:
            # Find the parent container for React format
            parent_container = cast_link_element.find_element(By.XPATH, "./ancestor::li[contains(@class, 'full-credits-page-list-item')]")
            
            # Look for episode button within this container
            episode_buttons = parent_container.find_elements(By.XPATH, ".//button[contains(text(), 'episode')]")
            
            if episode_buttons:
                episode_button = episode_buttons[0]
                button_text = episode_button.text.strip()
                print(f"🔍 Smart RHONJ Full Extractor: Found episode button: '{button_text}' (React format)")
                
                # Extract episode count from button text
                episode_match = re.search(r'(\d+)\s+episodes?', button_text)
                if episode_match:
                    episode_count = int(episode_match.group(1))
                    print(f"✅ Smart RHONJ Full Extractor: Episode count: {episode_count}")
                    
                    # Click the button to get detailed season information
                    seasons = self.click_episode_button_and_extract_seasons_dual(episode_button, cast_name)
                    
                    return {
                        'episode_count': episode_count,
                        'seasons': seasons,
                        'found': True,
                        'format': 'react'
                    }
                else:
                    print(f"⚠️ Smart RHONJ Full Extractor: Could not parse episode count from: '{button_text}'")
            else:
                print(f"⚠️ Smart RHONJ Full Extractor: No episode button found (React format)")
            
            return None
            
        except NoSuchElementException:
            print(f"⚠️ Smart RHONJ Full Extractor: React format elements not found")
            return None
        except Exception as e:
            print(f"⚠️ Smart RHONJ Full Extractor: React format extraction error: {str(e)}")
            return None

    def extract_from_traditional_format(self, cast_link_element, cast_name):
        """Extract data from traditional table-based IMDb format"""
        try:
            # Find the parent row for traditional format
            parent_row = cast_link_element.find_element(By.XPATH, "./ancestor::tr")
            
            # Look for episode information in the row
            episode_cells = parent_row.find_elements(By.TAG_NAME, "td")
            
            for cell in episode_cells:
                cell_text = cell.text.strip()
                episode_match = re.search(r'(\d+)\s+episodes?', cell_text)
                if episode_match:
                    episode_count = int(episode_match.group(1))
                    print(f"✅ Smart RHONJ Full Extractor: Episode count: {episode_count} (Traditional format)")
                    
                    # Look for season information in the same cell or adjacent cells
                    seasons = self.extract_seasons_from_traditional_format(parent_row, cast_name)
                    
                    return {
                        'episode_count': episode_count,
                        'seasons': seasons,
                        'found': True,
                        'format': 'traditional'
                    }
            
            print(f"⚠️ Smart RHONJ Full Extractor: No episode data found (Traditional format)")
            return None
            
        except NoSuchElementException:
            print(f"⚠️ Smart RHONJ Full Extractor: Traditional format elements not found")
            return None
        except Exception as e:
            print(f"⚠️ Smart RHONJ Full Extractor: Traditional format extraction error: {str(e)}")
            return None

    def click_episode_button_and_extract_seasons_dual(self, episode_button, cast_name):
        """
        Click the episode button and extract season information with enhanced parsing.
        Works with both modal types and episode list formats.
        """
        try:
            print(f"🖱️ Smart RHONJ Full Extractor: Clicking episode button for {cast_name}")
            
            # Scroll to button and click
            self.driver.execute_script("arguments[0].scrollIntoView(true);", episode_button)
            self.smart_delay(1)
            
            try:
                episode_button.click()
                self.smart_delay(2)
                print(f"✅ Smart RHONJ Full Extractor: Episode button clicked")
                
                # Method 1: Look for season episode patterns like "S4.E1"
                seasons_found = self.extract_seasons_from_episode_markers()
                if seasons_found:
                    return seasons_found
                
                # Method 2: Look for season links
                seasons_found = self.extract_seasons_from_links()
                if seasons_found:
                    return seasons_found
                
                # Method 3: Look for year ranges and estimate seasons
                seasons_found = self.extract_seasons_from_year_ranges()
                if seasons_found:
                    return seasons_found
                
                # Close any modal that might have opened
                self.close_modal_dual_format()
                
            except Exception as e:
                print(f"⚠️ Smart RHONJ Full Extractor: Button click failed: {str(e)}")
            
            print(f"⚠️ Smart RHONJ Full Extractor: Could not extract season information for {cast_name}")
            return None
            
        except Exception as e:
            print(f"❌ Smart RHONJ Full Extractor: Error clicking episode button: {str(e)}")
            return None

    def extract_seasons_from_episode_markers(self):
        """Extract seasons from episode markers like 'S4.E1'"""
        try:
            seasons_found = set()
            
            # Look for episode markers with season patterns
            selectors_to_try = [
                'li[role="presentation"] .ipc-inline-list__item',  # Inline list items
                'ul.ipc-inline-list li.ipc-inline-list__item',     # Specific inline list
                '.episodic-credits-bottomsheet__menu-item li',     # Modal menu items
                'a[role="menuitem"] li',                           # Menu item links
                'li.ipc-inline-list__item'                         # Direct class match
            ]
            
            for selector in selectors_to_try:
                episode_markers = self.driver.find_elements(By.CSS_SELECTOR, selector)
                print(f"🔍 Smart RHONJ Full Extractor: Trying selector '{selector}': found {len(episode_markers)} elements")
                
                for marker in episode_markers[:20]:  # Check first 20 markers
                    marker_text = marker.text.strip()
                    
                    # Look for pattern like "S12.E1" or "S1.E1"
                    season_match = re.search(r'S(\d+)\.E\d+', marker_text)
                    if season_match:
                        season_num = int(season_match.group(1))
                        seasons_found.add(season_num)
                        print(f"✅ Smart RHONJ Full Extractor: Found season {season_num} from episode marker: {marker_text}")
                
                if seasons_found:
                    break
            
            if seasons_found:
                seasons = sorted(list(seasons_found))
                if len(seasons) == 1:
                    return str(seasons[0])
                else:
                    return f"{min(seasons)}-{max(seasons)}"
            
            return None
            
        except Exception as e:
            print(f"⚠️ Smart RHONJ Full Extractor: Episode marker extraction error: {str(e)}")
            return None

    def extract_seasons_from_links(self):
        """Extract seasons from season links"""
        try:
            seasons_found = set()
            
            # Look for season links
            season_elements = self.driver.find_elements(By.CSS_SELECTOR, 'a[href*="/episodes?season="]')
            for elem in season_elements:
                href = elem.get_attribute('href')
                if 'season=' in href:
                    season_num = href.split('season=')[1].split('&')[0]
                    try:
                        seasons_found.add(int(season_num))
                        print(f"✅ Smart RHONJ Full Extractor: Found season {season_num} from link")
                    except:
                        pass
            
            if seasons_found:
                seasons = sorted(list(seasons_found))
                if len(seasons) == 1:
                    return str(seasons[0])
                else:
                    return f"{min(seasons)}-{max(seasons)}"
            
            return None
            
        except Exception as e:
            print(f"⚠️ Smart RHONJ Full Extractor: Season link extraction error: {str(e)}")
            return None

    def extract_seasons_from_year_ranges(self):
        """Extract seasons from year ranges as fallback"""
        try:
            # Look for year ranges and estimate seasons
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
                    print(f"✅ Smart RHONJ Full Extractor: Estimated seasons from years {start_year}-{end_year}: {season_result}")
                    return season_result
            
            return None
            
        except Exception as e:
            print(f"⚠️ Smart RHONJ Full Extractor: Year range extraction error: {str(e)}")
            return None

    def extract_seasons_from_traditional_format(self, parent_row, cast_name):
        """Extract season information from traditional table format"""
        try:
            # Look for season information in table cells
            cells = parent_row.find_elements(By.TAG_NAME, "td")
            
            for cell in cells:
                cell_text = cell.text.strip()
                
                # Look for season patterns
                season_match = re.search(r'Season\s+(\d+)', cell_text, re.IGNORECASE)
                if season_match:
                    season_num = season_match.group(1)
                    print(f"✅ Smart RHONJ Full Extractor: Found season {season_num} (Traditional format)")
                    return season_num
                
                # Look for year ranges
                year_match = re.search(r'(\d{4})–(\d{4})', cell_text)
                if year_match:
                    start_year = int(year_match.group(1))
                    end_year = int(year_match.group(2))
                    estimated_seasons = end_year - start_year + 1
                    if estimated_seasons > 1:
                        season_result = f"1-{estimated_seasons}"
                    else:
                        season_result = "1"
                    print(f"✅ Smart RHONJ Full Extractor: Estimated seasons from years (Traditional): {season_result}")
                    return season_result
            
            return None
            
        except Exception as e:
            print(f"⚠️ Smart RHONJ Full Extractor: Traditional season extraction error: {str(e)}")
            return None

    def close_modal_dual_format(self):
        """Close any open modal with multiple strategies"""
        try:
            # Strategy 1: Look for X button with data-testid
            close_button = self.driver.find_element(By.CSS_SELECTOR, '[data-testid="promptable__x"] button')
            self.driver.execute_script("arguments[0].click();", close_button)
            print(f"🔄 Smart RHONJ Full Extractor: Closed modal with X button")
            return True
        except:
            try:
                # Strategy 2: Look for close button by class
                close_button = self.driver.find_element(By.CSS_SELECTOR, '.ipc-promptable-base__close button')
                self.driver.execute_script("arguments[0].click();", close_button)
                print(f"🔄 Smart RHONJ Full Extractor: Closed modal with close button")
                return True
            except:
                try:
                    # Strategy 3: Click outside modal (body)
                    self.driver.execute_script("document.body.click();")
                    print(f"🔄 Smart RHONJ Full Extractor: Closed modal by clicking body")
                    return True
                except:
                    try:
                        # Strategy 4: Press Escape key
                        self.driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.ESCAPE)
                        print(f"🔄 Smart RHONJ Full Extractor: Closed modal with Escape key")
                        return True
                    except:
                        print(f"⚠️ Smart RHONJ Full Extractor: Could not close modal")
                        return False

    def update_cast_member_data(self, row_number, episode_count, seasons):
        """Update cast member data in the spreadsheet with proper API formatting"""
        try:
            # Update Episode Count (Column G)
            if episode_count is not None:
                range_name = f'ViableCast!G{row_number}:G{row_number}'
                values = [[str(episode_count)]]
                self.sheet.update(range_name, values, value_input_option='RAW')
                print(f"📝 Smart RHONJ Full Extractor: Updated Episode Count for row {row_number}: {episode_count}")
            
            # Update Seasons (Column H)
            if seasons:
                range_name = f'ViableCast!H{row_number}:H{row_number}'
                values = [[str(seasons)]]
                self.sheet.update(range_name, values, value_input_option='RAW')
                print(f"📝 Smart RHONJ Full Extractor: Updated Seasons for row {row_number}: {seasons}")
            
            # Small delay to respect API limits
            self.smart_delay(1)
            
            return True
            
        except Exception as e:
            print(f"❌ Smart RHONJ Full Extractor: Error updating row {row_number}: {str(e)}")
            return False

    def run_extraction(self, start_row=1):
        """Main extraction process starting from the beginning"""
        print(f"🚀 Smart RHONJ Full Extractor: Starting full extraction from row {start_row}")
        print(f"🎯 Smart RHONJ Full Extractor: Processing all shows from the beginning")
        
        try:
            # Setup connections
            if not self.setup_google_sheets():
                return False
            
            if not self.setup_webdriver():
                return False
            
            # Load data from beginning
            shows_analysis = self.load_viable_cast_data_from_beginning()
            if not shows_analysis:
                return False
            
            cast_members = shows_analysis['data']
            
            # Filter to only members needing processing
            members_to_process = [
                member for member in cast_members 
                if not member['episode_count'] or not member['seasons']
            ]
            
            print(f"📊 Smart RHONJ Full Extractor: Found {len(members_to_process)} cast members to process")
            
            if not members_to_process:
                print("✅ Smart RHONJ Full Extractor: All cast members already have complete data!")
                return True
            
            # Process in smaller batches
            batch_size = 3
            total_processed = 0
            
            for i in range(0, len(members_to_process), batch_size):
                batch = members_to_process[i:i+batch_size]
                batch_num = (i // batch_size) + 1
                
                print(f"\n🔄 Smart RHONJ Full Extractor: Processing batch {batch_num} ({len(batch)} members)")
                
                for member in batch:
                    try:
                        cast_name = member['cast_name']
                        cast_imdb_id = member['cast_imdbid']
                        show_imdb_id = member['show_imdbid']
                        row_number = member['row_number']
                        
                        print(f"\n🎭 Smart RHONJ Full Extractor: Processing {cast_name} from {show_imdb_id} (Row {row_number})")
                        
                        # Smart search for this specific cast member with dual format support
                        result = self.search_for_cast_member_dual_format(show_imdb_id, cast_imdb_id, cast_name)
                        
                        if result and result.get('found'):
                            episode_count = result.get('episode_count')
                            seasons = result.get('seasons')
                            format_used = result.get('format', 'unknown')
                            
                            print(f"✅ Smart RHONJ Full Extractor: Found {cast_name} ({format_used} format) - Episodes: {episode_count}, Seasons: {seasons}")
                            
                            # Update spreadsheet
                            if self.update_cast_member_data(row_number, episode_count, seasons):
                                self.processed_count += 1
                            else:
                                self.error_count += 1
                        else:
                            print(f"⚠️ Smart RHONJ Full Extractor: Could not find episode data for {cast_name}")
                            self.error_count += 1
                        
                        total_processed += 1
                        
                        # Progress update
                        if total_processed % 5 == 0:
                            print(f"📈 Smart RHONJ Full Extractor: Progress - Processed: {self.processed_count}, Errors: {self.error_count}, Total: {total_processed}")
                        
                        # Short delay between members
                        self.smart_delay(3)
                        
                    except KeyboardInterrupt:
                        print("\n⏹️ Smart RHONJ Full Extractor: Interrupted by user")
                        return False
                    except Exception as e:
                        print(f"❌ Smart RHONJ Full Extractor: Error processing {member.get('cast_name', 'Unknown')}: {str(e)}")
                        self.error_count += 1
                        continue
                
                # Longer delay between batches
                if i + batch_size < len(members_to_process):
                    print(f"⏸️ Smart RHONJ Full Extractor: Batch complete, pausing before next batch...")
                    self.smart_delay(8)
            
            # Final summary
            print(f"\n🎉 Smart RHONJ Full Extractor: Extraction complete!")
            print(f"✅ Successfully processed: {self.processed_count}")
            print(f"❌ Errors encountered: {self.error_count}")
            print(f"📊 Total attempted: {total_processed}")
            
            return True
            
        except KeyboardInterrupt:
            print("\n⏹️ Smart RHONJ Full Extractor: Extraction interrupted by user")
            return False
        except Exception as e:
            print(f"❌ Smart RHONJ Full Extractor: Fatal error: {str(e)}")
            return False
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Clean up resources"""
        print("🧹 Smart RHONJ Full Extractor: Cleaning up resources...")
        if self.driver:
            try:
                self.driver.quit()
                print("✅ Smart RHONJ Full Extractor: WebDriver closed")
            except:
                pass

def main():
    """Main function"""
    extractor = SmartSeasonExtractorRHONJFull()
    
    try:
        success = extractor.run_extraction(start_row=1)  # Start from the beginning
        if success:
            print("🎉 Smart RHONJ Full Extractor: Process completed successfully!")
        else:
            print("❌ Smart RHONJ Full Extractor: Process failed")
            sys.exit(1)
    except KeyboardInterrupt:
        print("\n⏹️ Smart RHONJ Full Extractor: Process interrupted")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Smart RHONJ Full Extractor: Unexpected error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
