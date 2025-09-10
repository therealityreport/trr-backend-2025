#!/usr/bin/env python3
"""
Universal Season Extractor - Middle-Down All Formats Version
Starts from ROW 4459 (Vanderpump Rules, tt2343157) and processes DOWNWARD.
Handles ALL possible IMDb layout versions with comprehensive timeout protection.
Moves seamlessly between different shows with universal format detection.
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

class UniversalSeasonExtractorMiddleDownAllFormats:
    def __init__(self):
        """Initialize the middle-down universal extractor with support for ALL IMDb formats"""
        self.driver = None
        self.sheet = None
        self.processed_count = 0
        self.error_count = 0
        self.service_account_file = '/Users/thomashulihan/Projects/TRR-Backend/keys/trr-backend-df2c438612e1.json'
        
        # Enhanced configurations for all formats
        self.max_retries = 3
        self.base_delay = 2
        self.max_delay = 10
        self.request_timeout = 20
        self.page_load_timeout = 30
        
        # Starting parameters
        self.start_row = 4459
        self.target_show_id = "tt2343157"  # Vanderpump Rules
        
        print("🔄 Universal Middle-Down All-Formats Extractor: Initializing...")
        print(f"📍 Starting at Row {self.start_row} - Vanderpump Rules ({self.target_show_id})")
        
    def setup_google_sheets(self):
        """Setup Google Sheets connection"""
        print("🔄 Middle-Down All-Formats: Setting up Google Sheets connection...")
        
        try:
            if not os.path.exists(self.service_account_file):
                raise FileNotFoundError(f"Service account file not found: {self.service_account_file}")
            
            gc = gspread.service_account(filename=self.service_account_file)
            workbook = gc.open("Realitease2025Data")
            self.sheet = workbook.worksheet("ViableCast")
            
            print("✅ Middle-Down All-Formats: Google Sheets connection successful")
            return True
            
        except Exception as e:
            print(f"❌ Middle-Down All-Formats: Google Sheets setup failed: {str(e)}")
            return False
    
    def setup_webdriver(self):
        """Setup WebDriver with maximum compatibility"""
        print("🔄 Middle-Down All-Formats: Setting up WebDriver with maximum compatibility...")
        
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
            
            print("✅ Middle-Down All-Formats: WebDriver setup successful")
            return True
            
        except Exception as e:
            print(f"❌ Middle-Down All-Formats: WebDriver setup failed: {str(e)}")
            return False
    
    def smart_delay(self, base=None):
        """Implement smart delays"""
        if base is None:
            base = self.base_delay
        delay = base + random.uniform(0.5, 2.5)
        time.sleep(delay)
    
    def load_middle_down_data(self):
        """Load ViableCast data starting from ROW 4459 and processing DOWNWARD"""
        print(f"🔄 Middle-Down All-Formats: Loading data from ROW {self.start_row} DOWNWARD...")
        print(f"🎯 Middle-Down All-Formats: Starting with Vanderpump Rules ({self.target_show_id})")
        
        try:
            all_data = self.sheet.get_all_values()
            
            print(f"📋 Middle-Down All-Formats: Processing {len(all_data)} total rows...")
            
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
            
            print(f"✅ Found Show IMDbID at column {show_imdbid_col}")
            print(f"📋 Middle-Down All-Formats: Column mapping - Show IMDbID: {show_imdbid_col}, CastName: {castname_col}, Cast IMDbID: {cast_imdbid_col}, EpisodeCount: {episode_count_col}, Seasons: {seasons_col}")
            
            # Process data from START_ROW DOWNWARD (4459 → end)
            filtered_data = []
            for i in range(self.start_row - 1, len(all_data)):  # -1 for 0-indexed, start_row is 1-indexed
                row = all_data[i]
                if len(row) > max(show_imdbid_col or 0, castname_col or 0):
                    filtered_data.append({
                        'row_number': i + 1,  # 1-indexed row number
                        'show_imdbid': row[show_imdbid_col] if show_imdbid_col is not None and len(row) > show_imdbid_col else '',
                        'cast_name': row[castname_col] if castname_col is not None and len(row) > castname_col else '',
                        'cast_imdbid': row[cast_imdbid_col] if cast_imdbid_col is not None and len(row) > cast_imdbid_col else '',
                        'episode_count': row[episode_count_col] if episode_count_col is not None and len(row) > episode_count_col else '',
                        'seasons': row[seasons_col] if seasons_col is not None and len(row) > seasons_col else ''
                    })
            
            # Filter to only incomplete records
            members_to_process = [
                record for record in filtered_data 
                if not record['episode_count'] or not record['seasons']
            ]
            
            print(f"📊 Middle-Down All-Formats: Filtered to {len(filtered_data)} records (row {self.start_row} → end)")
            print(f"📊 Middle-Down All-Formats: Found {len(members_to_process)} cast members needing processing")
            print(f"📊 Middle-Down All-Formats: Processing order: ROW {self.start_row} → DOWNWARD")
            
            # Show breakdown by shows
            show_counts = defaultdict(int)
            for record in members_to_process:
                show_counts[record['show_imdbid']] += 1
            
            print(f"📺 Middle-Down All-Formats: Shows to process:")
            for show_id, count in list(show_counts.items())[:10]:  # Show first 10
                print(f"  📺 {show_id}: {count} cast members")
            if len(show_counts) > 10:
                print(f"  📺 ... and {len(show_counts) - 10} more shows")
            
            return members_to_process
            
        except Exception as e:
            print(f"❌ Middle-Down All-Formats: Error loading data: {str(e)}")
            return []

    def find_cast_member_all_formats(self, show_imdb_id, cast_imdb_id, cast_name):
        """
        Find cast member using ALL possible IMDb layout formats and selectors with timeout protection.
        This method tries every known selector and format combination.
        """
        try:
            print(f"🔍 Middle-Down All-Formats: Searching for {cast_name} ({cast_imdb_id}) in {show_imdb_id}")
            
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
                    
                    print(f"✅ Middle-Down All-Formats: Page loaded successfully")
                    break
                except Exception as e:
                    if attempt < self.max_retries - 1:
                        print(f"⚠️ Middle-Down All-Formats: Load attempt {attempt + 1} failed, retrying...")
                        self.smart_delay(6)
                    else:
                        print(f"❌ Middle-Down All-Formats: Failed to load page after {self.max_retries} attempts")
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
                print(f"⏰ Middle-Down All-Formats: Search timeout ({max_search_time}s) for {cast_name}")
            else:
                print(f"❌ Middle-Down All-Formats: Could not find {cast_name} with any method")
            
            return None
            
        except Exception as e:
            print(f"❌ Middle-Down All-Formats: Error searching for cast member: {str(e)}")
            return None

    def search_by_imdb_id_all_formats(self, cast_imdb_id, cast_name):
        """Search by Cast IMDb ID (Column D) using all possible formats - PRIMARY METHOD"""
        try:
            print(f"🎯 Middle-Down All-Formats: PRIMARY SEARCH by Cast IMDb ID: {cast_imdb_id}")
            
            # Enhanced IMDb ID selectors - prioritizing exact Cast IMDb ID matches
            imdb_id_selectors = [
                # Direct href matches (most reliable)
                f"//a[contains(@href, '/name/{cast_imdb_id}/')]",
                f"//a[contains(@href, '/name/{cast_imdb_id}?')]", 
                f"//a[@href='/name/{cast_imdb_id}/']",
                f"//a[contains(@href, 'name/{cast_imdb_id}')]",
                
                # Any element with the IMDb ID
                f"//*[contains(@href, '{cast_imdb_id}')]",
                f"//*[contains(@data-const, '{cast_imdb_id}')]",
                f"//*[contains(@data-nm, '{cast_imdb_id}')]",
                f"//*[contains(@id, '{cast_imdb_id}')]",
                f"//*[contains(@class, '{cast_imdb_id}')]",
                
                # Look for the ID in any attribute
                f"//*[@*[contains(., '{cast_imdb_id}')]]",
                
                # Search in link elements specifically
                f"//link[contains(@href, '{cast_imdb_id}')]"
            ]
            
            for i, selector in enumerate(imdb_id_selectors, 1):
                try:
                    print(f"🔍 Middle-Down All-Formats: Trying IMDb ID selector {i}: {selector}")
                    
                    cast_elements = self.driver.find_elements(By.XPATH, selector)
                    print(f"🔍 Middle-Down All-Formats: Found {len(cast_elements)} elements with Cast IMDb ID {cast_imdb_id}")
                    
                    for j, cast_element in enumerate(cast_elements, 1):
                        try:
                            element_href = cast_element.get_attribute('href') or ''
                            element_text = cast_element.text.strip()
                            element_class = cast_element.get_attribute('class') or ''
                            
                            print(f"🔍 Middle-Down All-Formats: Element {j} - Text: '{element_text}', Class: '{element_class}', Href: '{element_href}'")
                            
                            # Verify this is the right cast member
                            if cast_imdb_id in element_href or self.names_match_flexible(element_text, cast_name):
                                print(f"✅ Middle-Down All-Formats: FOUND cast member by IMDb ID {cast_imdb_id} using selector {i}")
                                print(f"🎯 Middle-Down All-Formats: Element details - Text: '{element_text}', Href: '{element_href}'")
                                
                                result = self.extract_data_from_element_all_formats(cast_element, cast_name)
                                if result:
                                    return result
                        except Exception as e:
                            print(f"⚠️ Middle-Down All-Formats: Error processing element {j}: {str(e)}")
                            continue
                            
                except NoSuchElementException:
                    print(f"⚠️ Middle-Down All-Formats: No elements found with selector {i}")
                    continue
                except Exception as e:
                    print(f"⚠️ Middle-Down All-Formats: Error with selector {i}: {str(e)}")
                    continue
            
            print(f"⚠️ Middle-Down All-Formats: Cast IMDb ID {cast_imdb_id} not found with any selector")
            return None
            
        except Exception as e:
            print(f"⚠️ Middle-Down All-Formats: Cast IMDb ID search error: {str(e)}")
            return None

    def search_by_name_all_formats(self, cast_name):
        """Search by name using all possible formats and selectors with timeout protection"""
        try:
            print(f"🎯 Middle-Down All-Formats: Searching by name: {cast_name}")
            
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
                    print(f"⏰ Middle-Down All-Formats: Name search timeout ({search_timeout}s) for {cast_name}")
                    return None
                
                for selector_template in name_selectors:
                    try:
                        # Check timeout before each selector
                        if time.time() - search_start_time > search_timeout:
                            print(f"⏰ Middle-Down All-Formats: Name search timeout during selector search")
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
                                    print(f"✅ Middle-Down All-Formats: Found cast member '{element_text}' matches '{name_variant}'")
                                    return self.extract_data_from_element_all_formats(element, cast_name)
                            except Exception:
                                continue
                    
                    except Exception:
                        continue
            
            print(f"⚠️ Middle-Down All-Formats: Name {cast_name} not found with any selector")
            return None
            
        except Exception as e:
            print(f"⚠️ Middle-Down All-Formats: Name search error: {str(e)}")
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
            print(f"🎭 Middle-Down All-Formats: Extracting data for {cast_name}")
            
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
            
            print(f"❌ Middle-Down All-Formats: No data extraction method worked for {cast_name}")
            return None
            
        except Exception as e:
            print(f"❌ Middle-Down All-Formats: Data extraction error: {str(e)}")
            return None

    def extract_from_react_format_comprehensive(self, cast_element, cast_name):
        """Extract from React format targeting the exact elements from the user's example"""
        try:
            print(f"🔄 Middle-Down All-Formats: Trying React format extraction for {cast_name}...")
            print(f"🔍 Middle-Down All-Formats: Cast element tag: {cast_element.tag_name}, text: '{cast_element.text[:50]}...'")
            
            # TARGET 1: Look for the exact container structure from user's example
            # The cast member name is in: div.sc-2840b417-3.gfatgE
            container_selectors = [
                "./ancestor::div[contains(@class, 'sc-2840b417-3')]",  # Exact from user example
                "./ancestor::div[contains(@class, 'gfatgE')]",         # Secondary class from example
                "./ancestor::li[contains(@class, 'full-credits-page-list-item')]",
                "./ancestor::div[contains(@class, 'cast')]",
                "./parent::div/parent::div",  # Go up two levels
                "./ancestor::div[position()<=3]"  # Look in nearest 3 parent divs
            ]
            
            container_found = False
            for i, container_selector in enumerate(container_selectors, 1):
                try:
                    parent_container = cast_element.find_element(By.XPATH, container_selector)
                    container_found = True
                    
                    print(f"✅ Middle-Down All-Formats: Found container {i} using: {container_selector}")
                    print(f"🔍 Middle-Down All-Formats: Container class: {parent_container.get_attribute('class')}")
                    
                    # TARGET 2: Look for the exact episode button structure from user's example
                    # Button text: "46 episodes" in <button class="ipc-link ipc-link--base" tabindex="0" aria-disabled="false">46 episodes</button>
                    episode_button_selectors = [
                        ".//button[contains(@class, 'ipc-link ipc-link--base') and contains(text(), 'episode')]",  # Exact match
                        ".//button[contains(@class, 'ipc-link') and contains(text(), 'episode')]",
                        ".//button[contains(text(), 'episode')]",
                        ".//div[contains(@class, 'fOtfDU')]//button[contains(text(), 'episode')]",  # Specific div class
                        ".//*[contains(text(), 'episode') and contains(text(), '•')]//button"  # Button with "• year" format
                    ]
                    
                    button_found = False
                    for j, button_selector in enumerate(episode_button_selectors, 1):
                        try:
                            episode_buttons = parent_container.find_elements(By.XPATH, button_selector)
                            print(f"🔍 Middle-Down All-Formats: Button selector {j} found {len(episode_buttons)} elements")
                            
                            for k, episode_button in enumerate(episode_buttons, 1):
                                button_text = episode_button.text.strip()
                                button_class = episode_button.get_attribute('class')
                                print(f"🔍 Middle-Down All-Formats: Button {k} text: '{button_text}', class: '{button_class}'")
                                
                                # Extract episode count from button text (e.g., "46 episodes")
                                episode_match = re.search(r'(\d+)\s+episodes?', button_text, re.IGNORECASE)
                                if episode_match:
                                    episode_count = int(episode_match.group(1))
                                    button_found = True
                                    print(f"✅ Middle-Down All-Formats: Found episode button with count: {episode_count}")
                                    
                                    # Debug the button details before clicking
                                    print(f"🔍 Middle-Down All-Formats: name row is {parent_container.get_attribute('outerHTML')[:200]}...")
                                    print(f"🔍 Middle-Down All-Formats: episodes button is {episode_button.get_attribute('outerHTML')}")
                                    
                                    # Try to click for season data
                                    seasons = self.click_and_extract_seasons_targeted(episode_button, cast_name)
                                    
                                    return {
                                        'episode_count': episode_count,
                                        'seasons': seasons,
                                        'found': True,
                                        'format': 'react_targeted'
                                    }
                        
                        except NoSuchElementException:
                            continue
                    
                    if button_found:
                        break
                
                except NoSuchElementException:
                    continue
            
            if not container_found:
                print(f"⚠️ Middle-Down All-Formats: No React format containers found for {cast_name}")
            else:
                print(f"⚠️ Middle-Down All-Formats: React format containers found but no episode buttons for {cast_name}")
            
            return None
            
        except Exception as e:
            print(f"⚠️ Middle-Down All-Formats: React format error: {str(e)}")
            return None

    def extract_from_table_format_comprehensive(self, cast_element, cast_name):
        """Extract from traditional table format with comprehensive search"""
        try:
            print(f"🔄 Middle-Down All-Formats: Trying table format extraction...")
            
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
                            print(f"✅ Middle-Down All-Formats: Found episodes in table: {episode_count}")
                            
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
            
            print(f"⚠️ Middle-Down All-Formats: Table format extraction failed for {cast_name}")
            return None
            
        except Exception as e:
            print(f"⚠️ Middle-Down All-Formats: Table format error: {str(e)}")
            return None

    def extract_from_hybrid_format(self, cast_element, cast_name):
        """Extract from hybrid format (mix of old and new elements)"""
        try:
            print(f"🔄 Middle-Down All-Formats: Trying hybrid format extraction...")
            
            # Look in the general area around the cast element
            parent_area = cast_element.find_element(By.XPATH, "./ancestor::*[position()<=3]")
            
            # Search for any text containing episode information
            area_text = parent_area.text
            
            episode_match = re.search(r'(\d+)\s+episodes?', area_text, re.IGNORECASE)
            if episode_match:
                episode_count = int(episode_match.group(1))
                print(f"✅ Middle-Down All-Formats: Found episodes in hybrid format: {episode_count}")
                
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
            
            print(f"⚠️ Middle-Down All-Formats: Hybrid format extraction failed for {cast_name}")
            return None
            
        except Exception as e:
            print(f"⚠️ Middle-Down All-Formats: Hybrid format error: {str(e)}")
            return None

    def extract_from_text_analysis(self, cast_element, cast_name):
        """Last resort: analyze all text around the element"""
        try:
            print(f"🔄 Middle-Down All-Formats: Trying text analysis extraction...")
            
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
                            print(f"✅ Middle-Down All-Formats: Found episodes via text analysis: {episode_count}")
                            
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
            
            print(f"⚠️ Middle-Down All-Formats: Text analysis extraction failed for {cast_name}")
            return None
            
        except Exception as e:
            print(f"⚠️ Middle-Down All-Formats: Text analysis error: {str(e)}")
            return None

    def click_and_extract_seasons_targeted(self, episode_button, cast_name):
        """Click episode button and extract seasons using ALL season structure patterns"""
        try:
            print(f"🖱️ Middle-Down All-Formats: Clicking episode button for {cast_name}")
            
            # Store original button info for debugging
            button_text = episode_button.text.strip()
            button_class = episode_button.get_attribute('class')
            print(f"🔍 Middle-Down All-Formats: Button details - Text: '{button_text}', Class: '{button_class}'")
            
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
            for i, click_method in enumerate(click_methods, 1):
                try:
                    print(f"🖱️ Middle-Down All-Formats: Trying click method {i}")
                    click_method()
                    clicked = True
                    print(f"✅ Middle-Down All-Formats: Successfully clicked with method {i}")
                    break
                except Exception as e:
                    print(f"⚠️ Middle-Down All-Formats: Click method {i} failed: {str(e)}")
                    continue
            
            if not clicked:
                print(f"❌ Middle-Down All-Formats: Could not click episode button")
                return None
            
            # Wait for content to load
            self.smart_delay(4)
            
            # Try ALL season extraction methods in priority order
            season_extraction_methods = [
                ("Season Tabs (Primary)", self.extract_all_season_structures_comprehensive),
                ("Episode Markers", self.extract_seasons_from_episode_markers_comprehensive),
                ("Modal Content", self.extract_seasons_from_modal_comprehensive),
                ("Season Links", self.extract_seasons_from_links_comprehensive),
                ("Text Analysis", self.extract_seasons_from_text_patterns),
                ("Year Ranges (Conservative)", self.extract_seasons_from_year_ranges_comprehensive)
            ]
            
            for method_name, method_func in season_extraction_methods:
                try:
                    print(f"🔍 Middle-Down All-Formats: Trying {method_name} extraction...")
                    seasons = method_func()
                    if seasons:
                        print(f"✅ Middle-Down All-Formats: {method_name} found seasons: {seasons}")
                        self.close_any_modals()
                        return seasons
                    else:
                        print(f"⚠️ Middle-Down All-Formats: {method_name} found no seasons")
                except Exception as e:
                    print(f"⚠️ Middle-Down All-Formats: {method_name} error: {str(e)}")
                    continue
            
            # Final attempt: page-wide season search
            print(f"🔍 Middle-Down All-Formats: Trying page-wide season search as final attempt...")
            seasons = self.extract_seasons_page_wide_search()
            if seasons:
                print(f"✅ Middle-Down All-Formats: Page-wide search found seasons: {seasons}")
                self.close_any_modals()
                return seasons
            
            self.close_any_modals()
            print(f"❌ Middle-Down All-Formats: Could not extract seasons for {cast_name} with any method")
            return None
            
        except Exception as e:
            print(f"❌ Middle-Down All-Formats: Season extraction error: {str(e)}")
            self.close_any_modals()
            return None

    def extract_all_season_structures_comprehensive(self):
        """Extract from ALL possible season structure patterns - COMPREHENSIVE METHOD"""
        seasons_found = set()
        
        print(f"🎯 Middle-Down All-Formats: Comprehensive season structure detection...")
        
        # Wait for any dynamic content to load
        time.sleep(4)
        
        # PRIORITY 1: Look for Kandi-style episode markers in modal (most accurate for reality shows)
        kandi_style_selectors = [
            # Target the exact structure from user's example
            'li[role="presentation"].ipc-inline-list__item',  # <li role="presentation" class="ipc-inline-list__item">S1.E9</li>
            'ul.ipc-inline-list li.ipc-inline-list__item',    # Episodes within inline lists
            '.ipc-inline-list__item',                         # Any inline list item
            'a[role="menuitem"] ul.ipc-inline-list li',       # Menu items with episode info
            '.episodic-credits-bottomsheet__menu-item ul.ipc-inline-list li',  # Specific to episode sheets
            'a.episodic-credits-bottomsheet__menu-item li',   # Episode menu items
        ]
        
        print(f"🔍 Middle-Down All-Formats: PRIORITY 1 - Checking Kandi-style episode markers...")
        for i, selector in enumerate(kandi_style_selectors, 1):
            try:
                episode_elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                print(f"🔍 Middle-Down All-Formats: Kandi-style selector {i} ({selector}) found {len(episode_elements)} elements")
                
                for j, element in enumerate(episode_elements, 1):
                    try:
                        element_text = element.text.strip()
                        print(f"🔍 Middle-Down All-Formats: Episode element {j} text: '{element_text}'")
                        
                        # Look for episode markers like "S1.E9", "S2.E3", etc.
                        season_episode_matches = re.findall(r'S(\d+)\.E\d+', element_text)
                        for season_num in season_episode_matches:
                            season_number = int(season_num)
                            if 1 <= season_number <= 50:  # Reasonable range
                                seasons_found.add(season_number)
                                print(f"✅ Found Season {season_number} from episode marker: {element_text}")
                        
                        # Also look for just "S1", "S2" patterns
                        season_only_matches = re.findall(r'S(\d+)', element_text)
                        for season_num in season_only_matches:
                            season_number = int(season_num)
                            if 1 <= season_number <= 50:
                                seasons_found.add(season_number)
                                print(f"✅ Found Season {season_number} from season marker: {element_text}")
                                
                    except Exception as e:
                        print(f"⚠️ Middle-Down All-Formats: Error processing episode element {j}: {str(e)}")
                        continue
                
                # If we found seasons with episode markers, prioritize this method
                if seasons_found:
                    print(f"🎉 Middle-Down All-Formats: Found {len(seasons_found)} seasons from episode markers!")
                    break
                    
            except Exception as e:
                print(f"⚠️ Middle-Down All-Formats: Error with Kandi-style selector {i}: {str(e)}")
                continue
        
        # PRIORITY 2: Look for episode count in modal header (like "1 episode" for Kandi)
        if not seasons_found:
            print(f"🔍 Middle-Down All-Formats: PRIORITY 2 - Checking modal header for episode info...")
            header_selectors = [
                '.ipc-prompt-header__subtitle ul.ipc-inline-list li.ipc-inline-list__item',  # Kandi's structure
                '.ipc-prompt-header__text ul.ipc-inline-list li',
                '.ipc-prompt-header li.ipc-inline-list__item',
                'ul.ipc-inline-list.baseAlt li[role="presentation"]',  # baseAlt variant
                '.ipc-prompt-header__subtitle li',
            ]
            
            for i, selector in enumerate(header_selectors, 1):
                try:
                    header_elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    print(f"🔍 Middle-Down All-Formats: Header selector {i} found {len(header_elements)} elements")
                    
                    for j, element in enumerate(header_elements, 1):
                        try:
                            element_text = element.text.strip()
                            print(f"🔍 Middle-Down All-Formats: Header element {j} text: '{element_text}'")
                            
                            # If this is "1 episode" or similar, we need to look for the first episode to get the season
                            episode_count_match = re.search(r'(\d+)\s+episodes?', element_text, re.IGNORECASE)
                            if episode_count_match:
                                episode_count = int(episode_count_match.group(1))
                                print(f"✅ Found episode count in header: {episode_count}")
                                
                                # If only 1 episode, look for the first episode marker to get season
                                if episode_count == 1:
                                    first_episode_season = self.find_first_episode_season()
                                    if first_episode_season:
                                        seasons_found.add(first_episode_season)
                                        print(f"✅ Found season {first_episode_season} from first episode")
                                        break
                                
                        except Exception as e:
                            continue
                    
                    if seasons_found:
                        break
                        
                except Exception as e:
                    print(f"⚠️ Middle-Down All-Formats: Error with header selector {i}: {str(e)}")
                    continue
        
        # PRIORITY 3: Traditional season tab structures (from earlier examples)
        if not seasons_found:
            print(f"🔍 Middle-Down All-Formats: PRIORITY 3 - Checking traditional season tabs...")
            season_tab_selectors = [
                'li[data-testid*="season-tab-"]',  # data-testid="season-tab-1", "season-tab-2", etc.
                'li.ipc-tab[data-testid*="season-tab-"]',
                'li[role="tab"][data-testid*="season-tab-"]',
                'ul[role="tablist"] li[data-testid*="season-tab-"]',
                '.ipc-tab[data-testid*="season-tab-"]',
                'li.ipc-tab[role="tab"]',
                'ul.ipc-tabs li.ipc-tab',
            ]
            
            for i, selector in enumerate(season_tab_selectors, 1):
                try:
                    season_elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    print(f"🔍 Middle-Down All-Formats: Season tab selector {i} found {len(season_elements)} elements")
                    
                    for j, element in enumerate(season_elements, 1):
                        try:
                            # Extract from data-testid attribute
                            data_testid = element.get_attribute('data-testid') or ''
                            if 'season-tab-' in data_testid:
                                season_num = data_testid.replace('season-tab-', '')
                                try:
                                    season_number = int(season_num)
                                    if 1 <= season_number <= 50:
                                        seasons_found.add(season_number)
                                        print(f"✅ Found Season {season_number} from data-testid: {data_testid}")
                                except ValueError:
                                    pass
                            
                            # Extract from span text content
                            try:
                                span_elements = element.find_elements(By.TAG_NAME, 'span')
                                for span in span_elements:
                                    span_text = span.text.strip()
                                    if span_text.isdigit():
                                        season_number = int(span_text)
                                        if 1 <= season_number <= 50:
                                            seasons_found.add(season_number)
                                            print(f"✅ Found Season {season_number} from span text: {span_text}")
                            except:
                                pass
                                
                        except Exception as e:
                            continue
                    
                    if seasons_found:
                        break
                        
                except Exception as e:
                    print(f"⚠️ Middle-Down All-Formats: Error with season tab selector {i}: {str(e)}")
                    continue
        
        # PRIORITY 4: Fallback to text-based search in entire modal
        if not seasons_found:
            print(f"🔍 Middle-Down All-Formats: PRIORITY 4 - Fallback text-based search...")
            text_seasons = self.extract_seasons_from_text_patterns()
            if text_seasons:
                try:
                    season_numbers = [int(s.strip()) for s in text_seasons.split(',')]
                    seasons_found.update(season_numbers)
                    print(f"✅ Found seasons from text patterns: {text_seasons}")
                except:
                    pass
        
        if seasons_found:
            seasons = sorted(list(seasons_found))
            seasons_str = ", ".join(str(s) for s in seasons)
            print(f"🎉 Middle-Down All-Formats: FINAL comprehensive result - Seasons: {seasons_str}")
            return seasons_str
        
        print(f"❌ Middle-Down All-Formats: No seasons found with comprehensive detection")
        return None

    def find_first_episode_season(self):
        """Find the season from the first episode marker (for single-episode appearances)"""
        try:
            print(f"🔍 Middle-Down All-Formats: Looking for first episode season marker...")
            
            # Look for episode links that contain season/episode info
            episode_link_selectors = [
                'a[role="menuitem"].episodic-credits-bottomsheet__menu-item',  # From user's example
                'a.episodic-credits-bottomsheet__menu-item',
                'a[href*="/title/"]',  # Episode links
                'a[role="menuitem"]',   # Menu item links
            ]
            
            for i, selector in enumerate(episode_link_selectors, 1):
                try:
                    episode_links = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    print(f"🔍 Middle-Down All-Formats: Episode link selector {i} found {len(episode_links)} elements")
                    
                    for j, link in enumerate(episode_links[:3], 1):  # Check first 3 episodes
                        try:
                            link_text = link.text.strip()
                            print(f"🔍 Middle-Down All-Formats: Episode link {j} text: '{link_text[:100]}...'")
                            
                            # Look for season/episode pattern in the link text
                            season_episode_match = re.search(r'S(\d+)\.E\d+', link_text)
                            if season_episode_match:
                                season_number = int(season_episode_match.group(1))
                                print(f"✅ Found season {season_number} from first episode link")
                                return season_number
                                
                        except Exception as e:
                            continue
                    
                except Exception as e:
                    print(f"⚠️ Middle-Down All-Formats: Error with episode link selector {i}: {str(e)}")
                    continue
            
            print(f"❌ Middle-Down All-Formats: Could not find first episode season")
            return None
            
        except Exception as e:
            print(f"❌ Middle-Down All-Formats: Error finding first episode season: {str(e)}")
            return None

    def extract_seasons_from_episode_markers_fallback(self):
        """Extract seasons from episode markers as fallback"""
        seasons_found = set()
        
        try:
            # Look for episode markers like "S1.E1", "S2.E5", etc.
            marker_selectors = [
                'li[role="presentation"] .ipc-inline-list__item',
                'ul.ipc-inline-list li.ipc-inline-list__item',
                '.episodic-credits-bottomsheet__menu-item li',
                'a[role="menuitem"] li',
                'li.ipc-inline-list__item',
                '.ipc-inline-list__item',
                '*[class*="episode"]',
                '*[class*="episodic"]'
            ]
            
            for selector in marker_selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for element in elements:
                        text = element.text.strip()
                        # Look for season patterns in episode markers
                        season_matches = re.findall(r'S(\d+)\.E\d+', text)
                        for season_num in season_matches:
                            season_number = int(season_num)
                            if 1 <= season_number <= 50:
                                seasons_found.add(season_number)
                                print(f"✅ Found Season {season_number} from episode marker: {text}")
                except:
                    continue
            
            return seasons_found
            
        except:
            return set()

    def extract_seasons_from_text_patterns(self):
        """Extract seasons from text patterns across the page"""
        try:
            # Get all visible text on the page
            body_text = self.driver.find_element(By.TAG_NAME, 'body').text
            
            # Look for various season patterns
            season_patterns = [
                r'Season\s+(\d+)',
                r'S(\d+)\.E\d+',
                r'season\s*=\s*(\d+)',
                r'Season:\s*(\d+)',
                r'S(\d+)\s',
                r'(\d+)\s+episodes'  # Sometimes seasons are implied by episode counts
            ]
            
            seasons_found = set()
            for pattern in season_patterns:
                matches = re.findall(pattern, body_text, re.IGNORECASE)
                for match in matches:
                    try:
                        season_number = int(match)
                        if 1 <= season_number <= 50:
                            seasons_found.add(season_number)
                    except:
                        continue
            
            if seasons_found:
                seasons = sorted(list(seasons_found))
                return ", ".join(str(s) for s in seasons)
            
            return None
            
        except:
            return None

    def extract_seasons_page_wide_search(self):
        """Final attempt: page-wide season search"""
        try:
            print(f"🔍 Middle-Down All-Formats: Performing page-wide season search...")
            
            # Look for ANY element containing season information
            all_elements = self.driver.find_elements(By.XPATH, "//*[contains(text(), 'Season') or contains(text(), 'season') or contains(@data-testid, 'season') or contains(@class, 'season')]")
            
            seasons_found = set()
            for element in all_elements[:20]:  # Limit to first 20 to avoid performance issues
                try:
                    element_text = element.text.strip()
                    element_class = element.get_attribute('class') or ''
                    element_data = element.get_attribute('data-testid') or ''
                    
                    print(f"🔍 Page-wide: Checking element - Text: '{element_text[:50]}', Class: '{element_class}', Data: '{element_data}'")
                    
                    # Extract seasons from any available data
                    for text in [element_text, element_class, element_data]:
                        season_matches = re.findall(r'(\d+)', text)
                        for match in season_matches:
                            try:
                                season_number = int(match)
                                if 1 <= season_number <= 25:  # Conservative range
                                    seasons_found.add(season_number)
                                    print(f"✅ Page-wide: Found Season {season_number}")
                            except:
                                continue
                                
                except:
                    continue
            
            if seasons_found:
                seasons = sorted(list(seasons_found))
                return ", ".join(str(s) for s in seasons)
            
            return None
            
        except:
            return None

    def extract_seasons_from_episode_markers_comprehensive(self):
        """Extract seasons from episode markers targeting season tabs"""
        seasons_found = set()
        
        print(f"🎯 Middle-Down All-Formats: Looking for season tabs after episode button click...")
        
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
                print(f"🔍 Middle-Down All-Formats: Found {len(season_tabs)} season tabs with: {selector}")
                
                for tab in season_tabs:
                    try:
                        # Extract from data-testid attribute
                        data_testid = tab.get_attribute('data-testid') or ''
                        if 'season-tab-' in data_testid:
                            season_num = data_testid.replace('season-tab-', '')
                            try:
                                season_number = int(season_num)
                                seasons_found.add(season_number)
                                print(f"✅ Middle-Down All-Formats: Found season {season_number} from data-testid")
                            except ValueError:
                                pass
                        
                        # Extract from span text as backup
                        try:
                            span_element = tab.find_element(By.TAG_NAME, 'span')
                            span_text = span_element.text.strip()
                            if span_text.isdigit():
                                season_number = int(span_text)
                                seasons_found.add(season_number)
                                print(f"✅ Middle-Down All-Formats: Found season {season_number} from span")
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
            print(f"🔍 Middle-Down All-Formats: No season tabs, trying episode markers...")
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
                                print(f"✅ Middle-Down All-Formats: Found season {season_number} from: {text}")
                except:
                    continue
        
        if seasons_found:
            seasons = sorted(list(seasons_found))
            print(f"🎉 Middle-Down All-Formats: Final seasons: {seasons}")
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
        """Extract seasons from year ranges as conservative fallback"""
        try:
            # Look for year ranges but be much more conservative
            page_text = self.driver.find_element(By.TAG_NAME, 'body').text
            
            year_matches = re.findall(r'(\d{4})–(\d{4})', page_text)
            for start_year, end_year in year_matches:
                try:
                    start_year = int(start_year)
                    end_year = int(end_year)
                    
                    # Only if it's a reasonable range and recent years
                    if 2010 <= start_year <= 2025 and 2010 <= end_year <= 2025:
                        estimated_seasons = end_year - start_year + 1
                        # Only use if it's a small range (1-5 seasons max)
                        if 1 <= estimated_seasons <= 5:
                            return ", ".join(str(i) for i in range(1, estimated_seasons + 1))
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
                
                # Look for year ranges (conservative)
                year_match = re.search(r'(\d{4})–(\d{4})', cell_text)
                if year_match:
                    start_year = int(year_match.group(1))
                    end_year = int(year_match.group(2))
                    estimated_seasons = end_year - start_year + 1
                    if 1 <= estimated_seasons <= 5:  # Very conservative
                        return ", ".join(str(i) for i in range(1, estimated_seasons + 1))
            
            return None
            
        except Exception as e:
            print(f"⚠️ Middle-Down All-Formats: Table cell season extraction error: {str(e)}")
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
                print(f"📝 Middle-Down All-Formats: Updated Episode Count for row {row_number}: {episode_count}")
            
            # Update Seasons (Column H)
            if seasons:
                range_name = f'H{row_number}:H{row_number}'
                values = [[str(seasons)]]
                self.sheet.update(values, range_name, value_input_option='RAW')
                print(f"📝 Middle-Down All-Formats: Updated Seasons for row {row_number}: {seasons}")
            
            self.smart_delay(1)
            return True
            
        except Exception as e:
            print(f"❌ Middle-Down All-Formats: Error updating row {row_number}: {str(e)}")
            return False

    def run_extraction(self):
        """Main extraction process starting from ROW 4459 and working DOWN"""
        print(f"🚀 Middle-Down All-Formats: Starting ROW {self.start_row} DOWNWARD extraction")
        print(f"🎯 Middle-Down All-Formats: Starting with Vanderpump Rules → Multiple Shows")
        
        try:
            # Setup
            if not self.setup_google_sheets():
                return False
            
            if not self.setup_webdriver():
                return False
            
            # Load middle-down data
            members_to_process = self.load_middle_down_data()
            if not members_to_process:
                print("✅ Middle-Down All-Formats: No members need processing!")
                return True
            
            # Process in small batches
            batch_size = 2
            total_processed = 0
            
            for i in range(0, len(members_to_process), batch_size):
                batch = members_to_process[i:i+batch_size]
                batch_num = (i // batch_size) + 1
                
                print(f"\n🔄 Middle-Down All-Formats: Processing batch {batch_num} ({len(batch)} members)")
                
                for member in batch:
                    try:
                        cast_name = member['cast_name']
                        cast_imdb_id = member['cast_imdbid']
                        show_imdb_id = member['show_imdbid']
                        row_number = member['row_number']
                        
                        print(f"\n🎭 Middle-Down All-Formats: Processing {cast_name} from {show_imdb_id} (Row {row_number})")
                        
                        # Universal search with all format support
                        result = self.find_cast_member_all_formats(show_imdb_id, cast_imdb_id, cast_name)
                        
                        if result and result.get('found'):
                            episode_count = result.get('episode_count')
                            seasons = result.get('seasons')
                            format_used = result.get('format', 'unknown')
                            
                            print(f"✅ Middle-Down All-Formats: SUCCESS - {cast_name} ({format_used} format) - Episodes: {episode_count}, Seasons: {seasons}")
                            
                            if self.update_cast_member_data(row_number, episode_count, seasons):
                                self.processed_count += 1
                            else:
                                self.error_count += 1
                        else:
                            print(f"❌ Middle-Down All-Formats: FAILED - Could not extract data for {cast_name}")
                            self.error_count += 1
                        
                        total_processed += 1
                        
                        # Progress update
                        if total_processed % 5 == 0:
                            success_rate = (self.processed_count / total_processed * 100) if total_processed > 0 else 0
                            print(f"📈 Middle-Down All-Formats: Progress - Processed: {self.processed_count}, Errors: {self.error_count}, Success Rate: {success_rate:.1f}%")
                        
                        # Delay between members
                        self.smart_delay(4)
                        
                    except KeyboardInterrupt:
                        print("\n⏹️ Middle-Down All-Formats: Interrupted by user")
                        return False
                    except Exception as e:
                        print(f"❌ Middle-Down All-Formats: Error processing {member.get('cast_name', 'Unknown')}: {str(e)}")
                        self.error_count += 1
                        continue
                
                # Longer delay between batches
                if i + batch_size < len(members_to_process):
                    print(f"⏸️ Middle-Down All-Formats: Batch complete, pausing before next batch...")
                    self.smart_delay(10)
            
            # Final summary
            success_rate = (self.processed_count / total_processed * 100) if total_processed > 0 else 0
            print(f"\n🎉 Middle-Down All-Formats: Extraction Complete!")
            print(f"✅ Successfully processed: {self.processed_count}")
            print(f"❌ Errors encountered: {self.error_count}")
            print(f"📊 Total attempted: {total_processed}")
            print(f"📈 Success rate: {success_rate:.1f}%")
            
            return True
            
        except KeyboardInterrupt:
            print("\n⏹️ Middle-Down All-Formats: Extraction interrupted by user")
            return False
        except Exception as e:
            print(f"❌ Middle-Down All-Formats: Fatal error: {str(e)}")
            return False
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Clean up resources"""
        print("🧹 Middle-Down All-Formats: Cleaning up...")
        if self.driver:
            try:
                self.driver.quit()
                print("✅ Middle-Down All-Formats: WebDriver closed")
            except:
                pass

def main():
    """Main function"""
    extractor = UniversalSeasonExtractorMiddleDownAllFormats()
    
    try:
        success = extractor.run_extraction()
        if success:
            print("🎉 Middle-Down All-Formats: Process completed successfully!")
        else:
            print("❌ Middle-Down All-Formats: Process failed")
            sys.exit(1)
    except KeyboardInterrupt:
        print("\n⏹️ Middle-Down All-Formats: Process interrupted")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Middle-Down All-Formats: Unexpected error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
