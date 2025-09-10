#!/usr/bin/env python3
"""
The Challenge: All Stars IMDb Extractor - Specialized Version
Uses the exact same pattern as the successful universal extractor.
Starts from row 3017 and only processes The Challenge: All Stars records.
Skips rows that already have both Episode Count (G) and Seasons (H) filled out.
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

class ChallengeAllStarsIMDbExtractor:
    def __init__(self):
        """Initialize The Challenge: All Stars specific extractor"""
        self.driver = None
        self.sheet = None
        self.processed_count = 0
        self.error_count = 0
        self.skipped_count = 0
        self.service_account_file = '/Users/thomashulihan/Projects/TRR-Backend/keys/trr-backend-df2c438612e1.json'
        
        # Target show information
        self.target_show_id = "tt14257126"  # The Challenge: All Stars IMDb ID (with tt prefix)
        self.start_row = 3017
        
        # Enhanced configurations for all formats
        self.max_retries = 3
        self.base_delay = 2
        self.max_delay = 10
        self.request_timeout = 20
        self.page_load_timeout = 30
        
        print("🔄 Challenge All Stars Extractor: Initializing...")
        
    def setup_google_sheets(self):
        """Setup Google Sheets connection"""
        print("🔄 Challenge All Stars: Setting up Google Sheets connection...")
        
        try:
            if not os.path.exists(self.service_account_file):
                raise FileNotFoundError(f"Service account file not found: {self.service_account_file}")
            
            gc = gspread.service_account(filename=self.service_account_file)
            workbook = gc.open("Realitease2025Data")
            self.sheet = workbook.worksheet("ViableCast")
            
            print("✅ Challenge All Stars: Google Sheets connection successful")
            return True
            
        except Exception as e:
            print(f"❌ Challenge All Stars: Google Sheets setup failed: {str(e)}")
            return False
    
    def setup_webdriver(self):
        """Setup WebDriver with maximum compatibility"""
        print("🔄 Challenge All Stars: Setting up WebDriver with maximum compatibility...")
        
        try:
            chrome_options = Options()
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            chrome_options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            chrome_options.add_argument("--disable-web-security")
            chrome_options.add_argument("--disable-features=VizDisplayCompositor")
            
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            self.driver.implicitly_wait(15)
            self.driver.set_page_load_timeout(self.page_load_timeout)
            
            print("✅ Challenge All Stars: WebDriver setup successful")
            return True
            
        except Exception as e:
            print(f"❌ Challenge All Stars: WebDriver setup failed: {str(e)}")
            return False
    
    def smart_delay(self, base=None):
        """Implement smart delays"""
        if base is None:
            base = self.base_delay
        delay = base + random.uniform(0.5, 2.5)
        time.sleep(delay)
    
    def load_challenge_all_stars_data(self):
        """Load Challenge All Stars data starting from row 3017"""
        print(f"🔄 Challenge All Stars: Loading data starting from row {self.start_row}...")
        
        try:
            all_data = self.sheet.get_all_values()
            
            print(f"📋 Challenge All Stars: Processing {len(all_data)} total rows...")
            
            headers = all_data[0] if all_data else []
            header_mapping = {}
            
            for i, header in enumerate(headers):
                normalized_header = str(header).strip().lower()
                header_mapping[normalized_header] = i
            
            # Find column positions
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
            
            print(f"📋 Challenge All Stars: Column mapping - Show IMDbID: {show_imdbid_col}, CastName: {castname_col}, Cast IMDbID: {cast_imdbid_col}, EpisodeCount: {episode_count_col}, Seasons: {seasons_col}")
            
            # Process data starting from specified row, filtering for Challenge All Stars
            filtered_data = []
            challenge_all_stars_count = 0
            already_complete_count = 0
            
            # Debug: Check first 20 rows to see what IMDb IDs are present
            print(f"🔍 Challenge All Stars: Checking Show IMDb IDs around row {self.start_row}...")
            for i in range(self.start_row - 1, min(self.start_row + 19, len(all_data))):
                row = all_data[i]
                if len(row) > show_imdbid_col:
                    show_imdb_id = row[show_imdbid_col] if show_imdbid_col is not None else ''
                    cast_name = row[castname_col] if castname_col is not None and len(row) > castname_col else ''
                    print(f"🔍 Row {i + 1}: Show IMDb ID = '{show_imdb_id}', Cast = '{cast_name}'")
            
            for i in range(self.start_row - 1, len(all_data)):  # Convert to 0-indexed
                row = all_data[i]
                if len(row) > max(show_imdbid_col or 0, castname_col or 0):
                    show_imdb_id = row[show_imdbid_col] if show_imdbid_col is not None and len(row) > show_imdbid_col else ''
                    
                    # Only process Challenge All Stars records
                    if show_imdb_id == self.target_show_id:
                        challenge_all_stars_count += 1
                        episode_count = row[episode_count_col] if episode_count_col is not None and len(row) > episode_count_col else ''
                        seasons = row[seasons_col] if seasons_col is not None and len(row) > seasons_col else ''
                        cast_name = row[castname_col] if castname_col is not None and len(row) > castname_col else ''
                        
                        # Skip rows that already have both Episode Count and Seasons filled
                        if episode_count and episode_count.strip() and seasons and seasons.strip():
                            already_complete_count += 1
                            print(f"⏭️ Challenge All Stars: Row {i + 1} ({cast_name}) - already complete: Episodes({episode_count}) Seasons({seasons})")
                            continue
                        
                        print(f"✅ Challenge All Stars: Row {i + 1} ({cast_name}) - needs processing: Episodes({episode_count}) Seasons({seasons})")
                        filtered_data.append({
                            'row_number': i + 1,  # 1-indexed row number
                            'show_imdbid': show_imdb_id,
                            'cast_name': cast_name,
                            'cast_imdbid': row[cast_imdbid_col] if cast_imdbid_col is not None and len(row) > cast_imdbid_col else '',
                            'episode_count': episode_count,
                            'seasons': seasons
                        })
            
            print(f"📊 Challenge All Stars: Found {challenge_all_stars_count} total Challenge All Stars records")
            print(f"📊 Challenge All Stars: {already_complete_count} already complete, {len(filtered_data)} need processing")
            
            print(f"📊 Challenge All Stars: Found {len(filtered_data)} Challenge All Stars cast members needing processing")
            print(f"📊 Challenge All Stars: Processing from row {self.start_row} onward")
            
            return filtered_data
            
        except Exception as e:
            print(f"❌ Challenge All Stars: Error loading data: {str(e)}")
            return []

    def find_cast_member_all_formats(self, show_imdb_id, cast_imdb_id, cast_name):
        """
        Find cast member using ALL possible IMDb layout formats and selectors with timeout protection.
        This method tries every known selector and format combination.
        """
        try:
            print(f"🔍 Challenge All Stars: Searching for {cast_name} ({cast_imdb_id}) in {show_imdb_id}")
            
            import time
            search_start_time = time.time()
            max_search_time = 30  # 30 second timeout for entire cast member search
            
            # Load the page with retries
            url = f"https://www.imdb.com/title/{show_imdb_id}/fullcredits"
            for attempt in range(self.max_retries):
                try:
                    self.driver.get(url)
                    self.smart_delay(4)
                    
                    # Wait for page to be ready
                    WebDriverWait(self.driver, 15).until(
                        lambda driver: driver.execute_script("return document.readyState") == "complete"
                    )
                    
                    print(f"✅ Challenge All Stars: Page loaded successfully")
                    break
                except Exception as e:
                    if attempt < self.max_retries - 1:
                        print(f"⚠️ Challenge All Stars: Load attempt {attempt + 1} failed, retrying...")
                        self.smart_delay(6)
                    else:
                        print(f"❌ Challenge All Stars: Failed to load page after {self.max_retries} attempts")
                        return None
            
            # Strategy 1: Search by IMDb ID (works across all formats)
            if cast_imdb_id and (time.time() - search_start_time < max_search_time):
                result = self.search_by_imdb_id_all_formats(cast_imdb_id, cast_name)
                if result:
                    return result
            
            # Strategy 2: Search by name with all possible selectors
            if cast_name and (time.time() - search_start_time < max_search_time):
                result = self.search_by_name_all_formats(cast_name)
                if result:
                    return result
            
            # Check if we timed out
            if time.time() - search_start_time >= max_search_time:
                print(f"⏰ Challenge All Stars: Search timeout ({max_search_time}s) for {cast_name}")
            else:
                print(f"❌ Challenge All Stars: Could not find {cast_name} with any method")
            
            return None
            
        except Exception as e:
            print(f"❌ Challenge All Stars: Error searching for cast member: {str(e)}")
            return None

    def search_by_imdb_id_all_formats(self, cast_imdb_id, cast_name):
        """Search by IMDb ID using all possible formats"""
        try:
            print(f"🎯 Challenge All Stars: Searching by IMDb ID: {cast_imdb_id}")
            
            # All possible IMDb ID selectors across different formats
            imdb_id_selectors = [
                f"//a[contains(@href, '/name/{cast_imdb_id}/')]",
                f"//a[contains(@href, 'name/{cast_imdb_id}')]",
                f"//a[@href*='{cast_imdb_id}']",
                f"//link[contains(@href, '{cast_imdb_id}')]",
                f"//*[contains(@data-const, '{cast_imdb_id}')]",
                f"//*[contains(@data-nm, '{cast_imdb_id}')]"
            ]
            
            for selector in imdb_id_selectors:
                try:
                    cast_element = self.driver.find_element(By.XPATH, selector)
                    print(f"✅ Challenge All Stars: Found cast member by IMDb ID using selector: {selector}")
                    return self.extract_data_from_element_all_formats(cast_element, cast_name)
                except NoSuchElementException:
                    continue
            
            print(f"⚠️ Challenge All Stars: IMDb ID {cast_imdb_id} not found with any selector")
            return None
            
        except Exception as e:
            print(f"⚠️ Challenge All Stars: IMDb ID search error: {str(e)}")
            return None

    def search_by_name_all_formats(self, cast_name):
        """Search by name using all possible formats and selectors with timeout protection"""
        try:
            print(f"🎯 Challenge All Stars: Searching by name: {cast_name}")
            
            import time
            search_start_time = time.time()
            search_timeout = 15  # 15 second timeout for name search
            
            # Create name variations
            name_variations = [
                cast_name,
                cast_name.replace("'", "'"),  # Different apostrophe
                cast_name.replace("'", ""),   # No apostrophe
                cast_name.replace('"', ''),   # No quotes
                cast_name.strip(),
                cast_name.title(),
                cast_name.upper(),
                cast_name.lower()
            ]
            
            # All possible name selectors across different formats
            name_selectors = [
                # Modern React format selectors
                "//a[contains(@class, 'name-credits--title-text')]",
                "//a[contains(@class, 'ipc-link') and contains(@class, 'name-credits')]",
                "//a[@class='ipc-link ipc-link--base name-credits--title-text name-credits--title-text-big']",
                "//a[@class='ipc-link ipc-link--base name-credits--title-text name-credits--title-text-small']",
                
                # Traditional format selectors
                "//td[@class='name']//a",
                "//td[contains(@class, 'name')]//a",
                "//table//td//a[contains(@href, '/name/')]",
                
                # General selectors
                "//a[contains(@href, '/name/')]",
                "//a[contains(text(), '{}')]",
                "//*[contains(text(), '{}')]//ancestor::*//a[contains(@href, '/name/')]",
                
                # List item selectors
                "//li[contains(@class, 'cast')]//a",
                "//li[contains(@class, 'credit')]//a",
                "//li//a[contains(@href, '/name/')]",
                
                # Div-based selectors
                "//div[contains(@class, 'cast')]//a",
                "//div[contains(@class, 'credit')]//a",
                "//div//a[contains(@href, '/name/')]"
            ]
            
            for name_variant in name_variations:
                # Check timeout before each name variation
                if time.time() - search_start_time > search_timeout:
                    print(f"⏰ Challenge All Stars: Name search timeout ({search_timeout}s) for {cast_name}")
                    return None
                
                for selector_template in name_selectors:
                    try:
                        # Check timeout before each selector
                        if time.time() - search_start_time > search_timeout:
                            print(f"⏰ Challenge All Stars: Name search timeout during selector search")
                            return None
                        
                        # Some selectors need the name inserted
                        if '{}' in selector_template:
                            selector = selector_template.format(name_variant)
                        else:
                            selector = selector_template
                        
                        # Use WebDriverWait with short timeout for each element search
                        try:
                            elements = WebDriverWait(self.driver, 2).until(
                                EC.presence_of_all_elements_located((By.XPATH, selector))
                            )
                        except TimeoutException:
                            elements = []
                        
                        for element in elements[:5]:  # Limit to first 5 elements to avoid infinite loops
                            try:
                                element_text = element.text.strip()
                                if self.names_match_flexible(element_text, name_variant):
                                    print(f"✅ Challenge All Stars: Found cast member '{element_text}' matches '{name_variant}'")
                                    return self.extract_data_from_element_all_formats(element, cast_name)
                            except Exception:
                                continue
                    
                    except Exception:
                        continue
            
            print(f"⚠️ Challenge All Stars: Name {cast_name} not found with any selector")
            return None
            
        except Exception as e:
            print(f"⚠️ Challenge All Stars: Name search error: {str(e)}")
            return None

    def names_match_flexible(self, imdb_name, target_name):
        """Flexible name matching with multiple strategies"""
        if not imdb_name or not target_name:
            return False
        
        # Normalize both names
        imdb_clean = re.sub(r'[^\w\s]', '', imdb_name.lower().strip())
        target_clean = re.sub(r'[^\w\s]', '', target_name.lower().strip())
        
        # Exact match
        if imdb_clean == target_clean:
            return True
        
        # Contains match
        if imdb_clean in target_clean or target_clean in imdb_clean:
            return True
        
        # Word-based matching
        imdb_words = set(imdb_clean.split())
        target_words = set(target_clean.split())
        
        # If they share most words, consider it a match
        if len(imdb_words.intersection(target_words)) >= min(2, min(len(imdb_words), len(target_words))):
            return True
        
        return False

    def extract_data_from_element_all_formats(self, cast_element, cast_name):
        """
        Extract episode and season data from cast element using ALL possible formats.
        Tries every known method across all IMDb layout versions.
        """
        try:
            print(f"🎭 Challenge All Stars: Extracting data for {cast_name}")
            
            # Strategy 1: New React format with episode buttons
            result = self.extract_from_react_format_comprehensive(cast_element, cast_name)
            if result and result.get('found'):
                return result
            
            # Strategy 2: Traditional table format
            result = self.extract_from_table_format_comprehensive(cast_element, cast_name)
            if result and result.get('found'):
                return result
            
            # Strategy 3: Hybrid format (mix of old and new)
            result = self.extract_from_hybrid_format(cast_element, cast_name)
            if result and result.get('found'):
                return result
            
            # Strategy 4: Text-based extraction as last resort
            result = self.extract_from_text_analysis(cast_element, cast_name)
            if result and result.get('found'):
                return result
            
            print(f"❌ Challenge All Stars: No data extraction method worked for {cast_name}")
            return None
            
        except Exception as e:
            print(f"❌ Challenge All Stars: Data extraction error: {str(e)}")
            return None

    def extract_from_react_format_comprehensive(self, cast_element, cast_name):
        """Extract from React format with comprehensive container search and debugging"""
        try:
            print(f"🔄 Challenge All Stars: Trying React format extraction...")
            print(f"🔍 Challenge All Stars: Cast element tag: {cast_element.tag_name}, text: '{cast_element.text[:50]}...'")
            
            # All possible parent container selectors for React format
            container_selectors = [
                "./ancestor::li[contains(@class, 'full-credits-page-list-item')]",
                "./ancestor::li[contains(@class, 'ipc-metadata-list-summary-item')]",
                "./ancestor::li[contains(@class, 'cast')]",
                "./ancestor::div[contains(@class, 'sc-2840b417-3')]",
                "./ancestor::div[contains(@class, 'cast')]",
                "./ancestor::div[contains(@class, 'credit')]",
                "./ancestor::*[contains(@class, 'cast')]",
                "./ancestor::*[contains(@class, 'credit')]",
                "./parent::*",
                "./ancestor::li",
                "./ancestor::div"
            ]
            
            container_found = False
            for i, container_selector in enumerate(container_selectors, 1):
                try:
                    parent_container = cast_element.find_element(By.XPATH, container_selector)
                    container_found = True
                    
                    print(f"✅ Challenge All Stars: Found container {i} using: {container_selector}")
                    print(f"🔍 Challenge All Stars: Container class: {parent_container.get_attribute('class')}")
                    print(f"🔍 Challenge All Stars: Container text preview: '{parent_container.text[:100]}'")
                    
                    # Look for episode buttons with all possible selectors
                    episode_button_selectors = [
                        ".//button[contains(text(), 'episode')]",
                        ".//button[contains(@class, 'ipc-link')]",
                        ".//a[contains(text(), 'episode')]",
                        ".//span[contains(text(), 'episode')]",
                        ".//*[contains(text(), 'episode')]"
                    ]
                    
                    button_found = False
                    for j, button_selector in enumerate(episode_button_selectors, 1):
                        try:
                            episode_buttons = parent_container.find_elements(By.XPATH, button_selector)
                            print(f"🔍 Challenge All Stars: Button selector {j} '{button_selector}' found {len(episode_buttons)} elements")
                            
                            for k, episode_button in enumerate(episode_buttons, 1):
                                button_text = episode_button.text.strip()
                                button_class = episode_button.get_attribute('class')
                                print(f"🔍 Challenge All Stars: Button {k} text: '{button_text}', class: '{button_class}'")
                                
                                if 'episode' in button_text.lower():
                                    button_found = True
                                    print(f"🔍 Challenge All Stars: Found episode button: '{button_text}' (React format)")
                                    
                                    # Extract episode count
                                    episode_match = re.search(r'(\d+)\s+episodes?', button_text, re.IGNORECASE)
                                    if episode_match:
                                        episode_count = int(episode_match.group(1))
                                        print(f"✅ Challenge All Stars: Extracted episode count: {episode_count}")
                                        
                                        # Try to click for season data
                                        seasons = self.click_and_extract_seasons_comprehensive(episode_button, cast_name)
                                        
                                        return {
                                            'episode_count': episode_count,
                                            'seasons': seasons,
                                            'found': True,
                                            'format': 'react'
                                        }
                        
                        except NoSuchElementException:
                            continue
                    
                    if button_found:
                        break
                
                except NoSuchElementException:
                    continue
            
            if not container_found:
                print(f"⚠️ Challenge All Stars: No React format containers found for {cast_name}")
            else:
                print(f"⚠️ Challenge All Stars: React format containers found but no episode buttons for {cast_name}")
            
            return None
            
        except Exception as e:
            print(f"⚠️ Challenge All Stars: React format error: {str(e)}")
            return None

    def extract_from_table_format_comprehensive(self, cast_element, cast_name):
        """Extract from traditional table format with comprehensive search"""
        try:
            print(f"🔄 Challenge All Stars: Trying table format extraction...")
            
            # All possible table row selectors
            row_selectors = [
                "./ancestor::tr",
                "./parent::td/parent::tr",
                "./ancestor::table//tr",
                "./ancestor::*[name()='tr']"
            ]
            
            for row_selector in row_selectors:
                try:
                    parent_row = cast_element.find_element(By.XPATH, row_selector)
                    
                    # Get all cells in the row
                    cells = parent_row.find_elements(By.TAG_NAME, "td")
                    
                    for cell in cells:
                        cell_text = cell.text.strip()
                        
                        # Look for episode information
                        episode_match = re.search(r'(\d+)\s+episodes?', cell_text, re.IGNORECASE)
                        if episode_match:
                            episode_count = int(episode_match.group(1))
                            print(f"✅ Challenge All Stars: Found episodes in table: {episode_count}")
                            
                            # Look for season information in same or adjacent cells
                            seasons = self.extract_seasons_from_table_cells(cells, cast_name)
                            
                            return {
                                'episode_count': episode_count,
                                'seasons': seasons,
                                'found': True,
                                'format': 'table'
                            }
                
                except NoSuchElementException:
                    continue
            
            print(f"⚠️ Challenge All Stars: Table format extraction failed for {cast_name}")
            return None
            
        except Exception as e:
            print(f"⚠️ Challenge All Stars: Table format error: {str(e)}")
            return None

    def extract_from_hybrid_format(self, cast_element, cast_name):
        """Extract from hybrid format (mix of old and new elements)"""
        try:
            print(f"🔄 Challenge All Stars: Trying hybrid format extraction...")
            
            # Look in the general area around the cast element
            parent_area = cast_element.find_element(By.XPATH, "./ancestor::*[position()<=3]")
            
            # Search for any text containing episode information
            area_text = parent_area.text
            
            episode_match = re.search(r'(\d+)\s+episodes?', area_text, re.IGNORECASE)
            if episode_match:
                episode_count = int(episode_match.group(1))
                print(f"✅ Challenge All Stars: Found episodes in hybrid format: {episode_count}")
                
                # Look for season information in the same area
                season_matches = re.findall(r'S(\d+)\.E\d+', area_text)
                if season_matches:
                    seasons = sorted(list(set(int(s) for s in season_matches)))
                    seasons_str = ", ".join(str(s) for s in seasons)
                else:
                    seasons_str = None
                
                return {
                    'episode_count': episode_count,
                    'seasons': seasons_str,
                    'found': True,
                    'format': 'hybrid'
                }
            
            print(f"⚠️ Challenge All Stars: Hybrid format extraction failed for {cast_name}")
            return None
            
        except Exception as e:
            print(f"⚠️ Challenge All Stars: Hybrid format error: {str(e)}")
            return None

    def extract_from_text_analysis(self, cast_element, cast_name):
        """Last resort: analyze all text around the element"""
        try:
            print(f"🔄 Challenge All Stars: Trying text analysis extraction...")
            
            # Get the page source and analyze around this element
            page_source = self.driver.page_source
            
            # Find the cast name in the source and look around it
            if cast_name in page_source:
                # Find all episode mentions near the cast name
                lines = page_source.split('\n')
                cast_lines = [line for line in lines if cast_name in line]
                
                for i, line in enumerate(lines):
                    if cast_name in line:
                        # Look at surrounding lines
                        context_lines = lines[max(0, i-3):i+4]
                        context_text = ' '.join(context_lines)
                        
                        episode_match = re.search(r'(\d+)\s+episodes?', context_text, re.IGNORECASE)
                        if episode_match:
                            episode_count = int(episode_match.group(1))
                            print(f"✅ Challenge All Stars: Found episodes via text analysis: {episode_count}")
                            
                            # Look for season markers
                            season_matches = re.findall(r'S(\d+)\.E\d+', context_text)
                            if season_matches:
                                seasons = sorted(list(set(int(s) for s in season_matches)))
                                seasons_str = ", ".join(str(s) for s in seasons)
                            else:
                                seasons_str = None
                            
                            return {
                                'episode_count': episode_count,
                                'seasons': seasons_str,
                                'found': True,
                                'format': 'text_analysis'
                            }
            
            print(f"⚠️ Challenge All Stars: Text analysis extraction failed for {cast_name}")
            return None
            
        except Exception as e:
            print(f"⚠️ Challenge All Stars: Text analysis error: {str(e)}")
            return None

    def click_and_extract_seasons_comprehensive(self, episode_button, cast_name):
        """Click episode button and extract seasons using all possible methods"""
        try:
            print(f"🖱️ Challenge All Stars: Clicking episode button for {cast_name}")
            
            # Scroll to button and click with multiple strategies
            self.driver.execute_script("arguments[0].scrollIntoView(true);", episode_button)
            self.smart_delay(1)
            
            # Try different click methods
            click_methods = [
                lambda: episode_button.click(),
                lambda: self.driver.execute_script("arguments[0].click();", episode_button),
                lambda: ActionChains(self.driver).click(episode_button).perform(),
                lambda: ActionChains(self.driver).move_to_element(episode_button).click().perform()
            ]
            
            clicked = False
            for click_method in click_methods:
                try:
                    click_method()
                    clicked = True
                    break
                except:
                    continue
            
            if not clicked:
                print(f"⚠️ Challenge All Stars: Could not click episode button")
                return None
            
            self.smart_delay(3)
            
            # Try all season extraction methods
            season_methods = [
                self.extract_seasons_from_episode_markers_comprehensive,
                self.extract_seasons_from_modal_comprehensive,
                self.extract_seasons_from_links_comprehensive,
                self.extract_seasons_from_year_ranges_comprehensive
            ]
            
            for method in season_methods:
                try:
                    seasons = method()
                    if seasons:
                        print(f"✅ Challenge All Stars: Extracted seasons: {seasons}")
                        self.close_any_modals()
                        return seasons
                except:
                    continue
            
            self.close_any_modals()
            print(f"⚠️ Challenge All Stars: Could not extract seasons for {cast_name}")
            return None
            
        except Exception as e:
            print(f"❌ Challenge All Stars: Season extraction error: {str(e)}")
            self.close_any_modals()
            return None

    def extract_seasons_from_episode_markers_comprehensive(self):
        """Extract seasons from episode markers targeting season tabs"""
        seasons_found = set()
        
        print(f"🎯 Challenge All Stars: Looking for season tabs after episode button click...")
        
        # Wait for modal to load
        time.sleep(3)
        
        # PRIORITY 1: Look for season tabs (most accurate)
        season_tab_selectors = [
            'li[data-testid*="season-tab-"]',  # Primary: data-testid="season-tab-15"
            'li.ipc-tab[data-testid*="season-tab-"]',
            'li[role="tab"][data-testid*="season-tab-"]',
            'ul[role="tablist"] li[data-testid*="season-tab-"]',
            '.ipc-tab[data-testid*="season-tab-"]'
        ]
        
        for selector in season_tab_selectors:
            try:
                season_tabs = self.driver.find_elements(By.CSS_SELECTOR, selector)
                print(f"🔍 Challenge All Stars: Found {len(season_tabs)} season tabs with: {selector}")
                
                for tab in season_tabs:
                    try:
                        # Extract from data-testid attribute
                        data_testid = tab.get_attribute('data-testid') or ''
                        if 'season-tab-' in data_testid:
                            season_num = data_testid.replace('season-tab-', '')
                            try:
                                season_number = int(season_num)
                                seasons_found.add(season_number)
                                print(f"✅ Challenge All Stars: Found season {season_number} from data-testid")
                            except ValueError:
                                pass
                        
                        # Extract from span text as backup
                        try:
                            span_element = tab.find_element(By.TAG_NAME, 'span')
                            span_text = span_element.text.strip()
                            if span_text.isdigit():
                                season_number = int(span_text)
                                seasons_found.add(season_number)
                                print(f"✅ Challenge All Stars: Found season {season_number} from span")
                        except:
                            pass
                    except:
                        continue
                
                if seasons_found:
                    break
                    
            except:
                continue
        
        # PRIORITY 2: Episode markers fallback
        if not seasons_found:
            print(f"🔍 Challenge All Stars: No season tabs, trying episode markers...")
            marker_selectors = [
                'li[role="presentation"] .ipc-inline-list__item',
                'ul.ipc-inline-list li.ipc-inline-list__item',
                '.episodic-credits-bottomsheet__menu-item li',
                'a[role="menuitem"] li',
                'li.ipc-inline-list__item',
                '.ipc-inline-list__item'
            ]
            
            for selector in marker_selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for element in elements:
                        text = element.text.strip()
                        season_matches = re.findall(r'S(\d+)\.E\d+', text)
                        for season_num in season_matches:
                            season_number = int(season_num)
                            if 1 <= season_number <= 25:
                                seasons_found.add(season_number)
                                print(f"✅ Challenge All Stars: Found season {season_number} from: {text}")
                except:
                    continue
        
        if seasons_found:
            seasons = sorted(list(seasons_found))
            print(f"🎉 Challenge All Stars: Final seasons: {seasons}")
            return ", ".join(str(s) for s in seasons)
        
        return None

    def extract_seasons_from_modal_comprehensive(self):
        """Extract seasons from modal windows with comprehensive search"""
        try:
            # Look for modal elements
            modal_selectors = [
                '.ipc-promptable-base',
                '.modal',
                '.popup',
                '[role="dialog"]',
                '[aria-modal="true"]',
                '.overlay',
                '.episodic-credits-bottomsheet'
            ]
            
            for modal_selector in modal_selectors:
                try:
                    modal = self.driver.find_element(By.CSS_SELECTOR, modal_selector)
                    modal_text = modal.text
                    
                    # Look for season information
                    season_matches = re.findall(r'S(\d+)\.E\d+', modal_text)
                    if season_matches:
                        seasons = sorted(list(set(int(s) for s in season_matches)))
                        return ", ".join(str(s) for s in seasons)
                except:
                    continue
            
            return None
            
        except:
            return None

    def extract_seasons_from_links_comprehensive(self):
        """Extract seasons from season links with comprehensive search"""
        try:
            # All possible season link patterns
            link_patterns = [
                'a[href*="/episodes?season="]',
                'a[href*="season="]',
                'a[href*="/season/"]',
                '*[href*="season"]',
                '*[data-season]'
            ]
            
            for pattern in link_patterns:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, pattern)
                    seasons_found = set()
                    
                    for element in elements:
                        href = element.get_attribute('href') or ''
                        data_season = element.get_attribute('data-season') or ''
                        
                        for text in [href, data_season]:
                            if 'season=' in text:
                                season_num = text.split('season=')[1].split('&')[0]
                                try:
                                    seasons_found.add(int(season_num))
                                except:
                                    pass
                    
                    if seasons_found:
                        seasons = sorted(list(seasons_found))
                        return ", ".join(str(s) for s in seasons)
                except:
                    continue
            
            return None
            
        except:
            return None

    def extract_seasons_from_year_ranges_comprehensive(self):
        """Very conservative year range extraction - much more restrictive"""
        try:
            # Look for year ranges but be much more conservative
            page_text = self.driver.find_element(By.TAG_NAME, 'body').text
            
            year_matches = re.findall(r'(\d{4})–(\d{4})', page_text)
            for start_year, end_year in year_matches:
                try:
                    start_year = int(start_year)
                    end_year = int(end_year)
                    
                    # Much more conservative - only recent shows with short ranges
                    if 2020 <= start_year <= 2025 and 2020 <= end_year <= 2025:
                        estimated_seasons = end_year - start_year + 1
                        # Only allow very small ranges - likely wrong for established shows
                        if 1 <= estimated_seasons <= 3:
                            if estimated_seasons == 1:
                                return "1"
                            else:
                                # Don't assume 1-N for established shows
                                return None
                except:
                    continue
            
            return None
            
        except:
            return None

    def extract_seasons_from_table_cells(self, cells, cast_name):
        """Extract season information from table cells"""
        try:
            for cell in cells:
                cell_text = cell.text.strip()
                
                # Look for season patterns
                season_match = re.search(r'Season\s+(\d+)', cell_text, re.IGNORECASE)
                if season_match:
                    return season_match.group(1)
                
                # Look for episode markers
                season_matches = re.findall(r'S(\d+)\.E\d+', cell_text)
                if season_matches:
                    seasons = sorted(list(set(int(s) for s in season_matches)))
                    return ", ".join(str(s) for s in seasons)
                
                # Look for year ranges
                year_match = re.search(r'(\d{4})–(\d{4})', cell_text)
                if year_match:
                    start_year = int(year_match.group(1))
                    end_year = int(year_match.group(2))
                    estimated_seasons = end_year - start_year + 1
                    if 1 <= estimated_seasons <= 20:
                        if estimated_seasons == 1:
                            return "1"
                        else:
                            return ", ".join(str(i) for i in range(1, estimated_seasons + 1))
            
            return None
            
        except Exception as e:
            print(f"⚠️ Challenge All Stars: Table cell season extraction error: {str(e)}")
            return None

    def close_any_modals(self):
        """Close any open modals with comprehensive strategies"""
        close_strategies = [
            # X button strategies
            lambda: self.driver.find_element(By.CSS_SELECTOR, '[data-testid="promptable__x"] button').click(),
            lambda: self.driver.find_element(By.CSS_SELECTOR, '.ipc-promptable-base__close button').click(),
            lambda: self.driver.find_element(By.CSS_SELECTOR, '.close').click(),
            lambda: self.driver.find_element(By.CSS_SELECTOR, '[aria-label="Close"]').click(),
            
            # Escape key strategies
            lambda: self.driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.ESCAPE),
            lambda: ActionChains(self.driver).send_keys(Keys.ESCAPE).perform(),
            
            # Click outside strategies
            lambda: self.driver.execute_script("document.body.click();"),
            lambda: ActionChains(self.driver).move_by_offset(0, 0).click().perform()
        ]
        
        for strategy in close_strategies:
            try:
                strategy()
                self.smart_delay(1)
                return True
            except:
                continue
        
        return False

    def update_cast_member_data(self, row_number, episode_count, seasons):
        """Update cast member data in spreadsheet"""
        try:
            # Update Episode Count (Column G)
            if episode_count is not None:
                range_name = f'G{row_number}:G{row_number}'
                values = [[str(episode_count)]]
                self.sheet.update(values, range_name, value_input_option='RAW')
                print(f"📝 Challenge All Stars: Updated Episode Count for row {row_number}: {episode_count}")
            
            # Update Seasons (Column H)
            if seasons:
                range_name = f'H{row_number}:H{row_number}'
                values = [[str(seasons)]]
                self.sheet.update(values, range_name, value_input_option='RAW')
                print(f"📝 Challenge All Stars: Updated Seasons for row {row_number}: {seasons}")
            
            self.smart_delay(1)
            return True
            
        except Exception as e:
            print(f"❌ Challenge All Stars: Error updating row {row_number}: {str(e)}")
            return False

    def run_extraction(self):
        """Main extraction process for Challenge All Stars"""
        print(f"🚀 Challenge All Stars: Starting extraction from row {self.start_row}")
        print(f"🎯 Challenge All Stars: Target Show ID: {self.target_show_id}")
        
        try:
            # Setup
            if not self.setup_google_sheets():
                return False
            
            if not self.setup_webdriver():
                return False
            
            # Load Challenge All Stars data
            members_to_process = self.load_challenge_all_stars_data()
            if not members_to_process:
                print("✅ Challenge All Stars: No members need processing!")
                return True
            
            # Process in small batches
            batch_size = 2
            total_processed = 0
            
            for i in range(0, len(members_to_process), batch_size):
                batch = members_to_process[i:i+batch_size]
                batch_num = (i // batch_size) + 1
                
                print(f"\n🔄 Challenge All Stars: Processing batch {batch_num} ({len(batch)} members)")
                
                for member in batch:
                    try:
                        cast_name = member['cast_name']
                        cast_imdb_id = member['cast_imdbid']
                        show_imdb_id = member['show_imdbid']
                        row_number = member['row_number']
                        
                        print(f"\n🎭 Challenge All Stars: Processing {cast_name} from {show_imdb_id} (Row {row_number})")
                        
                        # Universal search with all format support
                        result = self.find_cast_member_all_formats(show_imdb_id, cast_imdb_id, cast_name)
                        
                        if result and result.get('found'):
                            episode_count = result.get('episode_count')
                            seasons = result.get('seasons')
                            format_used = result.get('format', 'unknown')
                            
                            print(f"✅ Challenge All Stars: SUCCESS - {cast_name} ({format_used} format) - Episodes: {episode_count}, Seasons: {seasons}")
                            
                            if self.update_cast_member_data(row_number, episode_count, seasons):
                                self.processed_count += 1
                            else:
                                self.error_count += 1
                        else:
                            print(f"❌ Challenge All Stars: FAILED - Could not extract data for {cast_name}")
                            self.error_count += 1
                        
                        total_processed += 1
                        
                        # Progress update
                        if total_processed % 5 == 0:
                            success_rate = (self.processed_count / total_processed * 100) if total_processed > 0 else 0
                            print(f"📈 Challenge All Stars: Progress - Processed: {self.processed_count}, Errors: {self.error_count}, Success Rate: {success_rate:.1f}%")
                        
                        # Delay between members
                        self.smart_delay(4)
                        
                    except KeyboardInterrupt:
                        print("\n⏹️ Challenge All Stars: Interrupted by user")
                        return False
                    except Exception as e:
                        print(f"❌ Challenge All Stars: Error processing {member.get('cast_name', 'Unknown')}: {str(e)}")
                        self.error_count += 1
                        continue
                
                # Longer delay between batches
                if i + batch_size < len(members_to_process):
                    print(f"⏸️ Challenge All Stars: Batch complete, pausing before next batch...")
                    self.smart_delay(10)
            
            # Final summary
            success_rate = (self.processed_count / total_processed * 100) if total_processed > 0 else 0
            print(f"\n🎉 Challenge All Stars: Extraction Complete!")
            print(f"✅ Successfully processed: {self.processed_count}")
            print(f"❌ Errors encountered: {self.error_count}")
            print(f"📊 Total attempted: {total_processed}")
            print(f"📈 Success rate: {success_rate:.1f}%")
            
            return True
            
        except KeyboardInterrupt:
            print("\n⏹️ Challenge All Stars: Extraction interrupted by user")
            return False
        except Exception as e:
            print(f"❌ Challenge All Stars: Fatal error: {str(e)}")
            return False
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Clean up resources"""
        print("🧹 Challenge All Stars: Cleaning up...")
        if self.driver:
            try:
                self.driver.quit()
                print("✅ Challenge All Stars: WebDriver closed")
            except:
                pass

def main():
    """Main function"""
    extractor = ChallengeAllStarsIMDbExtractor()
    
    try:
        success = extractor.run_extraction()
        if success:
            print("🎉 Challenge All Stars: Process completed successfully!")
        else:
            print("❌ Challenge All Stars: Process failed")
            sys.exit(1)
    except KeyboardInterrupt:
        print("\n⏹️ Challenge All Stars: Process interrupted")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Challenge All Stars: Unexpected error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
