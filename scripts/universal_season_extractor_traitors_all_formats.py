#!/usr/bin/env python3
"""
Universal Season Extractor - The Traitors All Formats Version
Handles ALL possible IMDb layout versions for The Traitors section.
Starts from The Traitors (tt10541088) at row 3213 and processes downward.
Includes real-time validation to skip rows that have been manually filled.
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
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains

class UniversalSeasonExtractorTraitorsAllFormats:
    def __init__(self):
        """Initialize the universal Traitors extractor with support for ALL IMDb formats"""
        self.driver = None
        self.sheet = None
        self.processed_count = 0
        self.error_count = 0
        self.skipped_count = 0
        self.service_account_file = '/Users/thomashulihan/Projects/TRR-Backend/keys/trr-backend-df2c438612e1.json'
        
        # Enhanced configurations for all formats
        self.max_retries = 3
        self.base_delay = 2
        self.max_delay = 10
        self.request_timeout = 20
        self.page_load_timeout = 30
        
        print("🕵️ Universal Traitors All-Formats Extractor: Initializing...")
        
    def setup_google_sheets(self):
        """Setup Google Sheets connection"""
        print("🔄 Traitors All-Formats: Setting up Google Sheets connection...")
        
        try:
            if not os.path.exists(self.service_account_file):
                raise FileNotFoundError(f"Service account file not found: {self.service_account_file}")
            
            gc = gspread.service_account(filename=self.service_account_file)
            workbook = gc.open("Realitease2025Data")
            self.sheet = workbook.worksheet("ViableCast")
            print("✅ Traitors All-Formats: Google Sheets connection successful")
            return True
            
        except Exception as e:
            print(f"❌ Traitors All-Formats: Google Sheets setup failed: {str(e)}")
            return False

    def setup_webdriver(self):
        """Setup WebDriver with comprehensive compatibility"""
        print("🔄 Traitors All-Formats: Setting up WebDriver with maximum compatibility...")
        
        try:
            chrome_options = Options()
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--remote-debugging-port=9222')
            chrome_options.add_argument('--disable-features=VizDisplayCompositor')
            chrome_options.add_argument('--disable-extensions')
            chrome_options.add_argument('--disable-plugins')
            chrome_options.add_argument('--disable-images')
            chrome_options.add_argument('--disable-javascript')
            chrome_options.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
            
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.set_page_load_timeout(self.page_load_timeout)
            self.driver.implicitly_wait(5)
            
            print("✅ Traitors All-Formats: WebDriver setup successful")
            return True
            
        except Exception as e:
            print(f"❌ Traitors All-Formats: WebDriver setup failed: {str(e)}")
            return False

    def load_traitors_data_from_row(self, start_row=3213):
        """Load cast data starting from The Traitors row with comprehensive filtering"""
        print(f"🔄 Traitors All-Formats: Loading ViableCast data starting from row {start_row}...")
        
        try:
            all_data = self.sheet.get_all_values()
            print(f"📋 Traitors All-Formats: Processing {len(all_data)} total rows...")
            
            if len(all_data) < 2:
                print("❌ Traitors All-Formats: Insufficient data in sheet")
                return []
            
            headers = all_data[0]
            print("✅ Found Show IMDbID at column 0")
            
            # Find column indices
            show_imdbid_col = 0  # Column A
            castname_col = None
            cast_imdbid_col = None
            episode_count_col = None
            seasons_col = None
            
            for i, header in enumerate(headers):
                if 'cast' in header.lower() and 'name' in header.lower():
                    castname_col = i
                elif 'cast' in header.lower() and 'imdb' in header.lower():
                    cast_imdbid_col = i
                elif 'episode' in header.lower() and 'count' in header.lower():
                    episode_count_col = i
                elif 'season' in header.lower() and i > 5:  # Avoid confusion with other season columns
                    seasons_col = i
            
            print(f"📋 Traitors All-Formats: Column mapping - Show IMDbID: {show_imdbid_col}, CastName: {castname_col}, Cast IMDbID: {cast_imdbid_col}, EpisodeCount: {episode_count_col}, Seasons: {seasons_col}")
            
            # Filter data from start_row onward
            filtered_data = []
            for i in range(start_row - 1, len(all_data)):
                row = all_data[i]
                if len(row) > show_imdbid_col and row[show_imdbid_col]:
                    filtered_data.append({
                        'row_number': i + 1,
                        'show_imdbid': row[show_imdbid_col],
                        'cast_name': row[castname_col] if castname_col is not None and len(row) > castname_col else '',
                        'cast_imdbid': row[cast_imdbid_col] if cast_imdbid_col is not None and len(row) > cast_imdbid_col else '',
                        'episode_count': row[episode_count_col] if episode_count_col is not None and len(row) > episode_count_col else '',
                        'seasons': row[seasons_col] if seasons_col is not None and len(row) > seasons_col else ''
                    })
            
            print(f"📊 Traitors All-Formats: Filtered to {len(filtered_data)} records from row {start_row} onward")
            
            # Analyze data
            total_records = len(filtered_data)
            complete_records = sum(1 for record in filtered_data if record['episode_count'] and record['seasons'])
            incomplete_records = total_records - complete_records
            
            # Get unique shows
            shows = set(record['show_imdbid'] for record in filtered_data)
            
            print(f"📊 Traitors All-Formats: Analysis complete:")
            print(f"  📝 Total records processed: {total_records}")
            print(f"  ✅ Already complete: {complete_records}")
            print(f"  ❌ Need processing: {incomplete_records}")
            print(f"  📺 Shows needing processing: {len(shows)}")
            
            # Filter to only incomplete records
            members_to_process = [
                record for record in filtered_data 
                if not record['episode_count'] or not record['seasons']
            ]
            
            print(f"📊 Traitors All-Formats: Found {len(members_to_process)} cast members needing processing")
            
            return members_to_process
            
        except Exception as e:
            print(f"❌ Traitors All-Formats: Error loading data: {str(e)}")
            return []

    def check_row_current_status(self, row_number):
        """Check if a specific row has been filled since we started processing"""
        try:
            # Get current row data (1-indexed)
            current_row = self.sheet.row_values(row_number)
            
            # Check if episode_count (column G, index 6) and seasons (column H, index 7) are filled
            episode_count = current_row[6] if len(current_row) > 6 else ''
            seasons = current_row[7] if len(current_row) > 7 else ''
            
            is_complete = bool(episode_count and seasons)
            
            if is_complete:
                print(f"🔄 Traitors All-Formats: Row {row_number} has been filled manually - Episodes: {episode_count}, Seasons: {seasons}")
            
            return is_complete
            
        except Exception as e:
            print(f"⚠️ Traitors All-Formats: Error checking row {row_number}: {str(e)}")
            return False

    def find_cast_member_all_formats(self, show_imdb_id, cast_imdb_id, cast_name):
        """
        Find cast member using ALL possible IMDb layout formats and selectors with timeout protection.
        This method tries every known selector and format combination.
        """
        try:
            print(f"🔍 Traitors All-Formats: Searching for {cast_name} ({cast_imdb_id}) in {show_imdb_id}")
            
            import time
            search_start_time = time.time()
            max_search_time = 30  # 30 second timeout for entire cast member search
            
            # Load the show's full credits page
            url = f"https://www.imdb.com/title/{show_imdb_id}/fullcredits"
            print(f"🔄 Traitors All-Formats: Loading page: {url}")
            
            self.driver.get(url)
            time.sleep(3)
            print("✅ Traitors All-Formats: Page loaded successfully")
            
            # Strategy 1: Direct IMDb ID search (most reliable)
            print(f"🎯 Traitors All-Formats: STRATEGY 1 - Searching for IMDb ID: {cast_imdb_id}")
            if self.find_by_imdb_id_all_formats(cast_imdb_id, cast_name, search_start_time, max_search_time):
                return self.extract_episode_data_all_formats(cast_name)
            
            # Strategy 2: Name-based search with comprehensive selectors
            print(f"🎯 Traitors All-Formats: STRATEGY 2 - Searching by name: {cast_name}")
            if self.find_by_name_all_formats(cast_name, search_start_time, max_search_time):
                return self.extract_episode_data_all_formats(cast_name)
            
            # Strategy 3: Partial name matching
            print(f"🎯 Traitors All-Formats: STRATEGY 3 - Partial name matching")
            if self.find_by_partial_name_all_formats(cast_name, search_start_time, max_search_time):
                return self.extract_episode_data_all_formats(cast_name)
            
            total_time = time.time() - search_start_time
            print(f"❌ Traitors All-Formats: All search strategies failed after {total_time:.1f}s for {cast_name}")
            return {'found': False}
            
        except Exception as e:
            print(f"❌ Traitors All-Formats: Error searching for {cast_name}: {str(e)}")
            return {'found': False}

    def find_by_imdb_id_all_formats(self, cast_imdb_id, cast_name, start_time, max_time):
        """Search by IMDb ID using all possible selectors"""
        try:
            # Comprehensive IMDb ID selectors for all formats
            id_selectors = [
                f"//a[contains(@href, '/name/{cast_imdb_id}/')]",
                f"a[href*='/name/{cast_imdb_id}/']",
                f"//a[@href='/name/{cast_imdb_id}/']",
                f"a[href='/name/{cast_imdb_id}/']",
                f"//a[contains(@href, '{cast_imdb_id}')]",
                f"a[href*='{cast_imdb_id}']",
                f"//*[@data-const='{cast_imdb_id}']",
                f"[data-const='{cast_imdb_id}']",
                f"//td[contains(@class, 'name')]//a[contains(@href, '{cast_imdb_id}')]",
                f"td.name a[href*='{cast_imdb_id}']",
                f"//div[contains(@class, 'name')]//a[contains(@href, '{cast_imdb_id}')]",
                f"div.name a[href*='{cast_imdb_id}']"
            ]
            
            for selector in id_selectors:
                if time.time() - start_time > max_time:
                    break
                    
                try:
                    if selector.startswith('//'):
                        elements = self.driver.find_elements(By.XPATH, selector)
                    else:
                        elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    
                    if elements:
                        print(f"✅ Traitors All-Formats: Found cast member by IMDb ID using: {selector}")
                        # Scroll to element and highlight it
                        element = elements[0]
                        self.driver.execute_script("arguments[0].scrollIntoView(true);", element)
                        time.sleep(1)
                        return True
                        
                except Exception:
                    continue
            
            return False
            
        except Exception as e:
            print(f"⚠️ Traitors All-Formats: Error in IMDb ID search: {str(e)}")
            return False

    def find_by_name_all_formats(self, cast_name, start_time, max_time):
        """Search by name using all possible selectors and formats"""
        try:
            # Name variations
            name_variations = [
                cast_name,
                cast_name.strip(),
                cast_name.replace("'", "'"),
                cast_name.replace("'", "'"),
                cast_name.replace(""", '"').replace(""", '"'),
                cast_name.split()[0] if " " in cast_name else cast_name,  # First name only
                cast_name.split()[-1] if " " in cast_name else cast_name,  # Last name only
            ]
            
            # Comprehensive name selectors for all IMDb formats
            for name_var in name_variations:
                if time.time() - start_time > max_time:
                    break
                    
                name_selectors = [
                    f"//a[contains(text(), '{name_var}')]",
                    f"a:contains('{name_var}')",
                    f"//td[contains(text(), '{name_var}')]//following-sibling::td//a",
                    f"//td[contains(text(), '{name_var}')]//preceding-sibling::td//a",
                    f"//tr[contains(., '{name_var}')]//a[contains(@href, '/name/')]",
                    f"//div[contains(text(), '{name_var}')]//a",
                    f"//span[contains(text(), '{name_var}')]//ancestor::tr//a[contains(@href, '/name/')]",
                    f"//li[contains(text(), '{name_var}')]//a",
                    f"//*[contains(@class, 'cast') or contains(@class, 'name') or contains(@class, 'person')]//*[contains(text(), '{name_var}')]",
                    f"//table//tr[contains(., '{name_var}')]//a[contains(@href, '/name/')]"
                ]
                
                for selector in name_selectors:
                    if time.time() - start_time > max_time:
                        break
                        
                    try:
                        if selector.startswith('//'):
                            elements = self.driver.find_elements(By.XPATH, selector)
                        else:
                            elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                        
                        if elements:
                            print(f"✅ Traitors All-Formats: Found cast member by name using: {selector}")
                            element = elements[0]
                            self.driver.execute_script("arguments[0].scrollIntoView(true);", element)
                            time.sleep(1)
                            return True
                            
                    except Exception:
                        continue
            
            return False
            
        except Exception as e:
            print(f"⚠️ Traitors All-Formats: Error in name search: {str(e)}")
            return False

    def find_by_partial_name_all_formats(self, cast_name, start_time, max_time):
        """Search using partial name matching for difficult cases"""
        try:
            if " " not in cast_name:
                return False
                
            name_parts = cast_name.split()
            
            for part in name_parts:
                if time.time() - start_time > max_time or len(part) < 3:
                    continue
                    
                partial_selectors = [
                    f"//a[contains(text(), '{part}')]",
                    f"//td[contains(text(), '{part}')]//following-sibling::td//a",
                    f"//tr[contains(., '{part}')]//a[contains(@href, '/name/')]",
                    f"//*[contains(text(), '{part}')]//ancestor::tr//a[contains(@href, '/name/')]"
                ]
                
                for selector in partial_selectors:
                    try:
                        elements = self.driver.find_elements(By.XPATH, selector)
                        if elements:
                            print(f"✅ Traitors All-Formats: Found cast member by partial name '{part}' using: {selector}")
                            element = elements[0]
                            self.driver.execute_script("arguments[0].scrollIntoView(true);", element)
                            time.sleep(1)
                            return True
                    except Exception:
                        continue
            
            return False
            
        except Exception as e:
            print(f"⚠️ Traitors All-Formats: Error in partial name search: {str(e)}")
            return False

    def extract_episode_data_all_formats(self, cast_name):
        """Extract episode data using all possible formats and methods"""
        print(f"🎭 Traitors All-Formats: Extracting data for {cast_name}")
        
        try:
            # Try React format first
            print("🔄 Traitors All-Formats: Trying React format...")
            result = self.extract_react_format_data(cast_name)
            if result['found']:
                return result
            
            # Try traditional table format
            print("🔄 Traitors All-Formats: Trying traditional table format...")
            result = self.extract_table_format_data(cast_name)
            if result['found']:
                return result
            
            # Try hybrid format
            print("🔄 Traitors All-Formats: Trying hybrid format...")
            result = self.extract_hybrid_format_data(cast_name)
            if result['found']:
                return result
            
            # Try enhanced episode marker detection
            print("🔄 Traitors All-Formats: Trying enhanced episode markers...")
            result = self.extract_seasons_from_episode_markers_enhanced(cast_name)
            if result['found']:
                return result
            
            print(f"❌ Traitors All-Formats: Could not extract data using any format for {cast_name}")
            return {'found': False}
            
        except Exception as e:
            print(f"❌ Traitors All-Formats: Error extracting episode data: {str(e)}")
            return {'found': False}

    def extract_react_format_data(self, cast_name):
        """Extract data from React-based IMDb layouts - now targets individual cast member episode data"""
        try:
            print(f"🔍 Traitors All-Formats: Looking for individual episode data for {cast_name}")
            
            # First, try to find a cast member link and click it to get their detailed view
            cast_links = self.driver.find_elements(By.XPATH, f"//a[contains(text(), '{cast_name}')]")
            
            if cast_links:
                print(f"🎯 Traitors All-Formats: Found cast member link for {cast_name}, clicking to get details...")
                try:
                    cast_links[0].click()
                    time.sleep(3)  # Wait for the detail view to load
                    
                    # Now look for episode-specific information in the detail view
                    episode_data = self.extract_from_cast_member_detail_view(cast_name)
                    if episode_data['found']:
                        return episode_data
                        
                    # Go back to the main page
                    self.driver.back()
                    time.sleep(2)
                    
                except Exception as e:
                    print(f"⚠️ Traitors All-Formats: Error clicking cast member link: {str(e)}")
                    try:
                        self.driver.back()
                        time.sleep(1)
                    except:
                        pass
            
            # Alternative approach: Look for episode containers near the cast member name
            containers = self.driver.find_elements(By.CSS_SELECTOR, "div, section, li, tr")
            
            for container in containers:
                try:
                    text_content = container.text
                    if cast_name.lower() in text_content.lower():
                        # Look for season-specific episode patterns (like S3.E1, S3.E2, etc.)
                        season_episode_patterns = [
                            r'S(\d+)\.E\d+',  # Matches S3.E1, S3.E2, etc.
                            r'Season\s*(\d+).*?(\d+)\s*episodes?',
                            r'(\d+)\s*episodes?.*?Season\s*(\d+)'
                        ]
                        
                        seasons_found = set()
                        episode_count = 0
                        
                        # Count individual episode references
                        season_episode_matches = re.findall(r'S(\d+)\.E(\d+)', text_content)
                        if season_episode_matches:
                            for season, episode in season_episode_matches:
                                seasons_found.add(season)
                                episode_count += 1
                            
                            seasons_str = ", ".join(sorted(seasons_found, key=int))
                            
                            return {
                                'found': True,
                                'episode_count': str(episode_count),
                                'seasons': seasons_str,
                                'format': 'React Individual Episodes'
                            }
                        
                        # Fallback: Look for other episode patterns but be more specific
                        for pattern in season_episode_patterns:
                            match = re.search(pattern, text_content, re.IGNORECASE)
                            if match:
                                if 'Season' in pattern:
                                    season = match.group(1)
                                    episode_count = match.group(2) if len(match.groups()) > 1 else "1"
                                else:
                                    season = match.group(1)
                                    episode_count = "1"  # Default if we can't determine
                                
                                return {
                                    'found': True,
                                    'episode_count': episode_count,
                                    'seasons': season,
                                    'format': 'React Pattern Match'
                                }
                except Exception:
                    continue
            
            return {'found': False}
            
        except Exception as e:
            print(f"⚠️ Traitors All-Formats: Error in React format extraction: {str(e)}")
            return {'found': False}

    def extract_from_cast_member_detail_view(self, cast_name):
        """Extract episode data from the cast member's detailed view"""
        try:
            print(f"🔍 Traitors All-Formats: Analyzing detailed view for {cast_name}")
            
            # Wait for content to load
            time.sleep(2)
            
            # Look for episode listings in the detail view
            page_text = self.driver.page_source
            
            # Count individual episode references (S3.E1, S3.E2, etc.)
            season_episode_matches = re.findall(r'S(\d+)\.E(\d+)', page_text)
            
            if season_episode_matches:
                seasons_found = set()
                episode_count = len(season_episode_matches)
                
                for season, episode in season_episode_matches:
                    seasons_found.add(season)
                
                seasons_str = ", ".join(sorted(seasons_found, key=int))
                
                print(f"✅ Traitors All-Formats: Found {episode_count} episodes in seasons {seasons_str} for {cast_name}")
                
                return {
                    'found': True,
                    'episode_count': str(episode_count),
                    'seasons': seasons_str,
                    'format': 'Cast Member Detail View'
                }
            
            # Alternative: Look for other episode indicators
            episode_indicators = [
                r'(\d+)\s*episodes?.*?Season\s*(\d+)',
                r'Season\s*(\d+).*?(\d+)\s*episodes?',
                r'Episodes?\s*(\d+).*?Season\s*(\d+)'
            ]
            
            for pattern in episode_indicators:
                matches = re.findall(pattern, page_text, re.IGNORECASE)
                if matches:
                    for match in matches:
                        if len(match) == 2:
                            episode_count = match[0] if match[0].isdigit() else match[1]
                            season = match[1] if match[0].isdigit() else match[0]
                            
                            return {
                                'found': True,
                                'episode_count': episode_count,
                                'seasons': season,
                                'format': 'Detail View Pattern'
                            }
            
            return {'found': False}
            
        except Exception as e:
            print(f"⚠️ Traitors All-Formats: Error in detail view extraction: {str(e)}")
            return {'found': False}

    def extract_table_format_data(self, cast_name):
        """Extract data from traditional table-based layouts"""
        try:
            # Find all table rows
            rows = self.driver.find_elements(By.CSS_SELECTOR, "tr, .cast_list tr, table tr")
            
            for row in rows:
                try:
                    row_text = row.text
                    if cast_name.lower() in row_text.lower():
                        # Look for episode data in the row
                        cells = row.find_elements(By.CSS_SELECTOR, "td, th")
                        
                        episode_count = None
                        seasons = None
                        
                        for cell in cells:
                            cell_text = cell.text.strip()
                            
                            # Check for episode count
                            episode_match = re.search(r'(\d+)\s*episodes?', cell_text, re.IGNORECASE)
                            if episode_match:
                                episode_count = episode_match.group(1)
                            
                            # Check for season info
                            season_match = re.search(r'seasons?\s*(\d+(?:,\s*\d+)*)', cell_text, re.IGNORECASE)
                            if season_match:
                                seasons = season_match.group(1)
                        
                        if episode_count:
                            if not seasons:
                                seasons = self.extract_seasons_from_context_table(row)
                            
                            return {
                                'found': True,
                                'episode_count': episode_count,
                                'seasons': seasons or "1",
                                'format': 'Table'
                            }
                            
                except Exception:
                    continue
            
            return {'found': False}
            
        except Exception as e:
            print(f"⚠️ Traitors All-Formats: Error in table format extraction: {str(e)}")
            return {'found': False}

    def extract_hybrid_format_data(self, cast_name):
        """Extract data from hybrid layouts"""
        try:
            # Look for various container types
            containers = self.driver.find_elements(By.CSS_SELECTOR, "div, section, article, li")
            
            for container in containers:
                try:
                    container_text = container.text
                    if cast_name.lower() in container_text.lower():
                        # Try to find episode buttons or links
                        episode_buttons = container.find_elements(By.CSS_SELECTOR, "button, a, span")
                        
                        for button in episode_buttons:
                            button_text = button.text.strip()
                            if 'episode' in button_text.lower():
                                try:
                                    button.click()
                                    time.sleep(2)
                                    
                                    # Look for episode data after clicking
                                    episode_data = self.extract_from_modal_or_expanded_view()
                                    if episode_data['found']:
                                        return episode_data
                                        
                                except Exception:
                                    continue
                except Exception:
                    continue
            
            return {'found': False}
            
        except Exception as e:
            print(f"⚠️ Traitors All-Formats: Error in hybrid format extraction: {str(e)}")
            return {'found': False}

    def extract_seasons_from_episode_markers_enhanced(self, cast_name):
        """Enhanced season extraction focusing on individual cast member episode data"""
        try:
            print(f"🔍 Traitors All-Formats: Looking for individual episode markers for {cast_name}")
            
            # Get the current page source to analyze
            page_source = self.driver.page_source
            
            # Look for season-episode patterns specific to this cast member
            season_episode_patterns = [
                r'S(\d+)\.E\d+',  # S3.E1, S3.E2, etc.
                r'Season\s*(\d+).*?Episode\s*\d+',
                r'(\d+)x\d+',  # 3x01, 3x02 format
            ]
            
            seasons_found = set()
            total_episodes = 0
            
            # Count episodes per pattern
            for pattern in season_episode_patterns:
                matches = re.findall(pattern, page_source, re.IGNORECASE)
                if matches:
                    for match in matches:
                        season_num = match if isinstance(match, str) else match
                        if season_num.isdigit() and 1 <= int(season_num) <= 50:
                            seasons_found.add(season_num)
                    
                    # Count total episodes for this pattern
                    episode_count = len(matches)
                    if episode_count > total_episodes:
                        total_episodes = episode_count
            
            # Also look for cast member specific containers
            cast_containers = self.driver.find_elements(By.XPATH, f"//*[contains(text(), '{cast_name}')]")
            
            for container in cast_containers:
                try:
                    # Get parent container that might have episode data
                    parent = container.find_element(By.XPATH, "./..")
                    container_text = parent.text
                    
                    # Look for episode references in this specific container
                    episode_refs = re.findall(r'S(\d+)\.E\d+', container_text)
                    if episode_refs:
                        for season in episode_refs:
                            seasons_found.add(season)
                        
                        container_episode_count = len(episode_refs)
                        if container_episode_count > total_episodes:
                            total_episodes = container_episode_count
                        
                except Exception:
                    continue
            
            if seasons_found and total_episodes > 0:
                sorted_seasons = sorted(seasons_found, key=int)
                seasons_str = ", ".join(sorted_seasons)
                
                print(f"✅ Traitors All-Formats: Found {total_episodes} episodes across seasons {seasons_str} for {cast_name}")
                
                return {
                    'found': True,
                    'episode_count': str(total_episodes),
                    'seasons': seasons_str,
                    'format': 'Enhanced Individual Episode Detection'
                }
            
            return {'found': False}
            
        except Exception as e:
            print(f"⚠️ Traitors All-Formats: Error in enhanced season extraction: {str(e)}")
            return {'found': False}

    def extract_seasons_from_context_react(self, container):
        """Extract season information from React context"""
        try:
            season_patterns = [
                r'Season\s*(\d+(?:,\s*\d+)*)',
                r'Seasons?\s*(\d+(?:-\d+)?)',
                r'S(\d+)',
                r'(\d+)(?:st|nd|rd|th)?\s*season'
            ]
            
            container_text = container.text
            
            for pattern in season_patterns:
                match = re.search(pattern, container_text, re.IGNORECASE)
                if match:
                    return match.group(1)
            
            return "1"  # Default
            
        except Exception:
            return "1"

    def extract_seasons_from_context_table(self, row):
        """Extract season information from table context"""
        try:
            # Look in adjacent cells or parent elements
            parent = row.find_element(By.XPATH, "..")
            parent_text = parent.text
            
            season_patterns = [
                r'Season\s*(\d+(?:,\s*\d+)*)',
                r'Seasons?\s*(\d+(?:-\d+)?)',
                r'S(\d+)',
                r'(\d+)(?:st|nd|rd|th)?\s*season'
            ]
            
            for pattern in season_patterns:
                match = re.search(pattern, parent_text, re.IGNORECASE)
                if match:
                    return match.group(1)
            
            return "1"  # Default
            
        except Exception:
            return "1"

    def extract_from_modal_or_expanded_view(self):
        """Extract data from modal dialogs or expanded views"""
        try:
            # Wait for modal content to load
            time.sleep(2)
            
            # Look for modal or expanded content
            modal_selectors = [
                "[role='dialog']",
                ".modal",
                ".popup",
                ".expanded-view",
                "[data-testid*='modal']"
            ]
            
            for selector in modal_selectors:
                try:
                    modal = self.driver.find_element(By.CSS_SELECTOR, selector)
                    modal_text = modal.text
                    
                    # Extract episode and season data from modal
                    episode_match = re.search(r'(\d+)\s*episodes?', modal_text, re.IGNORECASE)
                    season_match = re.search(r'seasons?\s*(\d+(?:,\s*\d+)*)', modal_text, re.IGNORECASE)
                    
                    if episode_match:
                        return {
                            'found': True,
                            'episode_count': episode_match.group(1),
                            'seasons': season_match.group(1) if season_match else "1",
                            'format': 'Modal'
                        }
                        
                except Exception:
                    continue
            
            return {'found': False}
            
        except Exception:
            return {'found': False}

    def find_episode_count_for_seasons(self, cast_name, seasons):
        """Try to find episode count for identified seasons"""
        try:
            # This is a simplified approach - in a real implementation,
            # you might click through season tabs to count episodes
            return str(len(seasons))  # Basic estimation
            
        except Exception:
            return "1"

    def update_cast_member_data(self, row_number, episode_count, seasons):
        """Update the Google Sheet with extracted data"""
        try:
            print(f"📝 Traitors All-Formats: Updating row {row_number} - Episodes: {episode_count}, Seasons: {seasons}")
            
            # Update columns G (episode count) and H (seasons)
            self.sheet.update(values=[[episode_count]], range_name=f'G{row_number}')
            time.sleep(0.5)  # Rate limiting
            self.sheet.update(values=[[seasons]], range_name=f'H{row_number}')
            time.sleep(0.5)  # Rate limiting
            
            print(f"✅ Traitors All-Formats: Successfully updated row {row_number}")
            return True
            
        except Exception as e:
            print(f"❌ Traitors All-Formats: Error updating row {row_number}: {str(e)}")
            return False

    def run_extraction(self, start_row=3213):
        """Main extraction process starting from The Traitors"""
        print("🚀 Traitors All-Formats: Starting comprehensive extraction from The Traitors (row 3213)")
        print("🎯 Traitors All-Formats: Supporting ALL possible IMDb layout formats")
        print("🔄 Traitors All-Formats: Setting up Google Sheets connection...")
        
        if not self.setup_google_sheets():
            return False
        
        print("🔄 Traitors All-Formats: Setting up WebDriver with maximum compatibility...")
        if not self.setup_webdriver():
            return False
        
        try:
            # Load Traitors data
            members_to_process = self.load_traitors_data_from_row(start_row)
            if not members_to_process:
                print("✅ Traitors All-Formats: No members need processing!")
                return True
            
            # Process in small batches
            batch_size = 2
            total_processed = 0
            
            for i in range(0, len(members_to_process), batch_size):
                batch = members_to_process[i:i+batch_size]
                batch_num = (i // batch_size) + 1
                
                print(f"\n🔄 Traitors All-Formats: Processing batch {batch_num} ({len(batch)} members)")
                
                for member in batch:
                    try:
                        cast_name = member['cast_name']
                        cast_imdb_id = member['cast_imdbid']
                        show_imdb_id = member['show_imdbid']
                        row_number = member['row_number']
                        
                        print(f"\n🎭 Traitors All-Formats: Processing {cast_name} from {show_imdb_id} (Row {row_number})")
                        
                        # REAL-TIME CHECK: Skip if row has been filled manually
                        if self.check_row_current_status(row_number):
                            print(f"⏭️ Traitors All-Formats: SKIPPING {cast_name} - Row {row_number} already filled manually")
                            self.skipped_count += 1
                            total_processed += 1
                            continue
                        
                        # Universal search with all format support
                        result = self.find_cast_member_all_formats(show_imdb_id, cast_imdb_id, cast_name)
                        
                        if result and result.get('found'):
                            episode_count = result.get('episode_count')
                            seasons = result.get('seasons')
                            format_used = result.get('format', 'unknown')
                            
                            print(f"✅ Traitors All-Formats: SUCCESS - {cast_name} ({format_used} format) - Episodes: {episode_count}, Seasons: {seasons}")
                            
                            if self.update_cast_member_data(row_number, episode_count, seasons):
                                self.processed_count += 1
                            else:
                                self.error_count += 1
                        else:
                            print(f"❌ Traitors All-Formats: FAILED - Could not extract data for {cast_name}")
                            self.error_count += 1
                        
                        total_processed += 1
                        
                        # Progress update
                        if total_processed % 5 == 0:
                            success_rate = (self.processed_count / total_processed * 100) if total_processed > 0 else 0
                            print(f"📈 Traitors All-Formats: Progress - Processed: {self.processed_count}, Errors: {self.error_count}, Skipped: {self.skipped_count}, Success Rate: {success_rate:.1f}%")
                        
                        # Delay between members
                        time.sleep(random.uniform(2, 4))
                        
                    except Exception as e:
                        print(f"❌ Traitors All-Formats: Error processing {member.get('cast_name', 'unknown')}: {str(e)}")
                        self.error_count += 1
                        total_processed += 1
                
                # Pause between batches
                if i + batch_size < len(members_to_process):
                    pause_time = random.uniform(5, 10)
                    print(f"⏸️ Traitors All-Formats: Batch complete, pausing {pause_time:.1f}s before next batch...")
                    time.sleep(pause_time)
            
            # Final summary
            final_success_rate = (self.processed_count / total_processed * 100) if total_processed > 0 else 0
            print(f"\n🎉 Traitors All-Formats: Extraction complete!")
            print(f"📊 Final Results:")
            print(f"  ✅ Successfully processed: {self.processed_count}")
            print(f"  ❌ Errors encountered: {self.error_count}")
            print(f"  ⏭️ Skipped (already filled): {self.skipped_count}")
            print(f"  📈 Final success rate: {final_success_rate:.1f}%")
            
            return True
            
        except Exception as e:
            print(f"❌ Traitors All-Formats: Critical error in extraction: {str(e)}")
            return False
            
        finally:
            if self.driver:
                print("🔄 Traitors All-Formats: Closing WebDriver...")
                self.driver.quit()

if __name__ == "__main__":
    extractor = UniversalSeasonExtractorTraitorsAllFormats()
    success = extractor.run_extraction()
    
    if success:
        print("✅ Traitors All-Formats: Extraction completed successfully")
    else:
        print("❌ Traitors All-Formats: Extraction failed")
        sys.exit(1)
