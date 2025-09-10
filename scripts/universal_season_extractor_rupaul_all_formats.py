#!/usr/bin/env python3
"""
Universal Season Extractor - RuPaul All Formats Version
Handles ALL possible IMDb layout versions for RuPaul's Drag Race section.
Starts from RuPaul's Drag Race (tt1353056) at row 2598 and processes downward.
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

class UniversalSeasonExtractorRuPaulAllFormats:
    def __init__(self):
        """Initialize the universal RuPaul extractor with support for ALL IMDb formats"""
        self.driver = None
        self.sheet = None
        self.processed_count = 0
        self.error_count = 0
        self.service_account_file = '/Users/thomashulihan/Projects/TRR-Backend/keys/trr-backend-df2c438612e1.json'
        
        # Enhanced configurations for all formats - OPTIMIZED
        self.max_retries = 3
        self.base_delay = 1.5
        self.max_delay = 8
        self.request_timeout = 15
        self.page_load_timeout = 25
        self.cast_search_timeout = 25
        
        print("🎭 Universal RuPaul All-Formats Extractor: Initializing...")
        
    def setup_google_sheets(self):
        """Setup Google Sheets connection"""
        print("🔄 RuPaul All-Formats: Setting up Google Sheets connection...")
        
        try:
            if not os.path.exists(self.service_account_file):
                raise FileNotFoundError(f"Service account file not found: {self.service_account_file}")
            
            gc = gspread.service_account(filename=self.service_account_file)
            workbook = gc.open("Realitease2025Data")
            self.sheet = workbook.worksheet("ViableCast")
            
            print("✅ RuPaul All-Formats: Google Sheets connection successful")
            return True
            
        except Exception as e:
            print(f"❌ RuPaul All-Formats: Google Sheets setup failed: {str(e)}")
            return False
    
    def setup_webdriver(self):
        """Setup WebDriver with maximum compatibility"""
        print("🔄 RuPaul All-Formats: Setting up WebDriver...")
        
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
            
            print("✅ RuPaul All-Formats: WebDriver setup successful")
            return True
            
        except Exception as e:
            print(f"❌ RuPaul All-Formats: WebDriver setup failed: {str(e)}")
            return False
    
    def smart_delay(self, base=None):
        """Implement smart delays"""
        if base is None:
            base = self.base_delay
        delay = base + random.uniform(0.5, 2.5)
        time.sleep(delay)
    
    def load_rupaul_data_from_row(self, start_row=2598):
        """Load RuPaul ViableCast data starting from specified row"""
        print(f"🔄 RuPaul All-Formats: Loading data from row {start_row}...")
        
        try:
            all_data = self.sheet.get_all_values()
            
            print(f"📋 RuPaul All-Formats: Processing {len(all_data)} total rows...")
            
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
            
            print(f"📋 RuPaul All-Formats: Column mapping - Show IMDbID: {show_imdbid_col}, CastName: {castname_col}, Cast IMDbID: {cast_imdbid_col}")
            
            # Filter data from start_row onward
            filtered_data = []
            for i in range(start_row - 1, len(all_data)):
                row = all_data[i]
                if len(row) > max(show_imdbid_col or 0, castname_col or 0):
                    filtered_data.append({
                        'row_number': i + 1,
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
            
            print(f"📊 RuPaul All-Formats: Found {len(members_to_process)} cast members needing processing")
            
            return members_to_process
            
        except Exception as e:
            print(f"❌ RuPaul All-Formats: Error loading data: {str(e)}")
            return []

    def find_cast_member_all_formats(self, show_imdb_id, cast_imdb_id, cast_name):
        """
        Find cast member using optimized IMDb search with timeout management.
        """
        start_time = time.time()
        try:
            print(f"🔍 RuPaul All-Formats: Searching for {cast_name} ({cast_imdb_id}) in {show_imdb_id}")
            
            # Load the page with retries
            url = f"https://www.imdb.com/title/{show_imdb_id}/fullcredits"
            for attempt in range(self.max_retries):
                try:
                    self.driver.get(url)
                    self.smart_delay(2)
                    
                    # Wait for page to be ready
                    WebDriverWait(self.driver, 10).until(
                        lambda driver: driver.execute_script("return document.readyState") == "complete"
                    )
                    
                    print(f"✅ RuPaul All-Formats: Page loaded successfully")
                    break
                except Exception as e:
                    if attempt < self.max_retries - 1:
                        print(f"⚠️ RuPaul All-Formats: Load attempt {attempt + 1} failed, retrying...")
                        self.smart_delay(3)
                    else:
                        print(f"❌ RuPaul All-Formats: Failed to load page after {self.max_retries} attempts")
                        return None
            
            # Strategy 1: Search by IMDb ID (most reliable)
            if cast_imdb_id:
                print(f"🎯 RuPaul All-Formats: STRATEGY 1 - Searching for IMDb ID: {cast_imdb_id}")
                result = self.search_by_imdb_id_optimized(cast_imdb_id, cast_name)
                if result:
                    return result
                
                # Check timeout
                if time.time() - start_time > self.cast_search_timeout:
                    print(f"⏰ RuPaul All-Formats: Search timeout after {time.time() - start_time:.1f}s")
                    return None
            
            # Strategy 2: Search by name with optimized selectors
            if cast_name:
                print(f"🎯 RuPaul All-Formats: STRATEGY 2 - Searching for name: {cast_name}")
                result = self.search_by_name_optimized(cast_name)
                if result:
                    return result
            
            elapsed_time = time.time() - start_time
            print(f"❌ RuPaul All-Formats: All search strategies failed after {elapsed_time:.1f}s for {cast_name}")
            return None
            
        except Exception as e:
            elapsed_time = time.time() - start_time
            print(f"❌ RuPaul All-Formats: Error searching for cast member after {elapsed_time:.1f}s: {str(e)}")
            return None

    def search_by_imdb_id_optimized(self, cast_imdb_id, cast_name):
        """Search by IMDb ID using optimized selectors"""
        try:
            # Primary IMDb ID selector (most common in React format)
            imdb_selector = f"//a[contains(@href, '/name/{cast_imdb_id}/')]"
            
            try:
                cast_element = self.driver.find_element(By.XPATH, imdb_selector)
                print(f"✅ RuPaul All-Formats: Found cast member by IMDb ID using selector: {imdb_selector}")
                return self.extract_data_from_element_all_formats(cast_element, cast_name)
            except NoSuchElementException:
                print(f"⚠️ RuPaul All-Formats: IMDb ID {cast_imdb_id} not found")
                return None
            
        except Exception as e:
            print(f"⚠️ RuPaul All-Formats: IMDb ID search error: {str(e)}")
            return None

    def search_by_name_optimized(self, cast_name):
        """Search by name using optimized selectors"""
        try:
            # Create name variations
            name_variations = [
                cast_name,
                cast_name.replace("'", "'"),  # Different apostrophe
                cast_name.replace("'", ""),   # No apostrophe
                cast_name.strip()
            ]
            
            # Optimized name selectors (most effective first)
            name_selectors = [
                "//a[contains(@href, '/name/')]",
                "//li[contains(@class, 'cast')]//a",
                "//div[contains(@class, 'cast')]//a"
            ]
            
            for name_variant in name_variations:
                for selector in name_selectors:
                    try:
                        elements = self.driver.find_elements(By.XPATH, selector)
                        
                        for element in elements[:50]:  # Limit for performance
                            element_text = element.text.strip()
                            if self.names_match_flexible(element_text, name_variant):
                                print(f"✅ RuPaul All-Formats: Found cast member '{element_text}' matches '{name_variant}'")
                                return self.extract_data_from_element_all_formats(element, cast_name)
                    
                    except Exception:
                        continue
            
            print(f"⚠️ RuPaul All-Formats: Name {cast_name} not found with any selector")
            return None
            
        except Exception as e:
            print(f"⚠️ RuPaul All-Formats: Name search error: {str(e)}")
            return None

    def search_by_imdb_id_all_formats(self, cast_imdb_id, cast_name):
        """Search by IMDb ID using all possible formats"""
        try:
            print(f"🎯 RuPaul All-Formats: Searching by IMDb ID: {cast_imdb_id}")
            
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
                    print(f"✅ RuPaul All-Formats: Found cast member by IMDb ID using selector: {selector}")
                    return self.extract_data_from_element_all_formats(cast_element, cast_name)
                except NoSuchElementException:
                    continue
            
            print(f"⚠️ RuPaul All-Formats: IMDb ID {cast_imdb_id} not found with any selector")
            return None
            
        except Exception as e:
            print(f"⚠️ RuPaul All-Formats: IMDb ID search error: {str(e)}")
            return None

    def search_by_name_all_formats(self, cast_name):
        """Search by name using all possible formats and selectors"""
        try:
            print(f"🎯 RuPaul All-Formats: Searching by name: {cast_name}")
            
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
                for selector_template in name_selectors:
                    try:
                        # Some selectors need the name inserted
                        if '{}' in selector_template:
                            selector = selector_template.format(name_variant)
                        else:
                            selector = selector_template
                        
                        elements = self.driver.find_elements(By.XPATH, selector)
                        
                        for element in elements:
                            element_text = element.text.strip()
                            if self.names_match_flexible(element_text, name_variant):
                                print(f"✅ RuPaul All-Formats: Found cast member '{element_text}' matches '{name_variant}'")
                                return self.extract_data_from_element_all_formats(element, cast_name)
                    
                    except Exception:
                        continue
            
            print(f"⚠️ RuPaul All-Formats: Name {cast_name} not found with any selector")
            return None
            
        except Exception as e:
            print(f"⚠️ RuPaul All-Formats: Name search error: {str(e)}")
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
        Extract episode and season data from cast element using optimized methods.
        """
        try:
            print(f"🎭 RuPaul All-Formats: Extracting data for {cast_name}")
            
            # Strategy 1: New React format with episode buttons - PRIORITY
            result = self.extract_from_react_format_comprehensive(cast_element, cast_name)
            if result and result.get('found'):
                return result
            
            # Strategy 2: Traditional format DISABLED - gets total show data instead of individual
            print(f"⚠️ RuPaul All-Formats: Traditional format DISABLED - gets total show data")
            
            # Strategy 3: Hybrid format DISABLED - unreliable without episode buttons
            print(f"⚠️ RuPaul All-Formats: Hybrid format DISABLED - unreliable without episode buttons")
            
            # Strategy 4: Text-based extraction DISABLED - gets total show data
            print(f"⚠️ RuPaul All-Formats: Text-based extraction DISABLED - gets total show data")
            
            print(f"❌ RuPaul All-Formats: Could not extract episode data for {cast_name} with any format")
            return None
            
        except Exception as e:
            print(f"❌ RuPaul All-Formats: Data extraction error: {str(e)}")
            return None

    def extract_from_react_format_comprehensive(self, cast_element, cast_name):
        """Extract from React format with comprehensive container search - OPTIMIZED"""
        try:
            print(f"🔄 RuPaul All-Formats: Trying React format extraction...")
            print(f"🔍 RuPaul All-Formats: Cast element tag: {cast_element.tag_name}, text: '{cast_element.text[:50]}...'")
            
            # All possible parent container selectors for React format
            container_selectors = [
                "./ancestor::li[contains(@class, 'full-credits-page-list-item')]",
                "./ancestor::li[contains(@class, 'ipc-metadata-list-summary-item')]",
                "./ancestor::div[contains(@class, 'sc-2840b417-3')]",
                "./ancestor::div[contains(@class, 'cast-list__item')]",
                "./ancestor::li[contains(@data-testid, 'name-credits')]"
            ]
            
            for i, container_selector in enumerate(container_selectors, 1):
                try:
                    parent_container = cast_element.find_element(By.XPATH, container_selector)
                    print(f"✅ RuPaul All-Formats: Found container {i} using: {container_selector}")
                    print(f"🔍 RuPaul All-Formats: Container class: {parent_container.get_attribute('class')}")
                    print(f"🔍 RuPaul All-Formats: Container text preview: '{parent_container.text[:100]}...'")
                    
                    # Look for episode buttons with all possible selectors
                    episode_button_selectors = [
                        ".//button[contains(text(), 'episode')]",
                        ".//button[contains(@class, 'ipc-link') and contains(text(), 'episode')]",
                        ".//a[contains(text(), 'episode')]",
                        ".//span[contains(text(), 'episode')]"
                    ]
                    
                    for j, button_selector in enumerate(episode_button_selectors, 1):
                        try:
                            episode_buttons = parent_container.find_elements(By.XPATH, button_selector)
                            print(f"🔍 RuPaul All-Formats: Button selector {j} '{button_selector}' found {len(episode_buttons)} elements")
                            
                            for k, episode_button in enumerate(episode_buttons, 1):
                                button_text = episode_button.text.strip()
                                button_class = episode_button.get_attribute('class')
                                print(f"🔍 RuPaul All-Formats: Button {k} text: '{button_text}', class: '{button_class}'")
                                
                                if 'episode' in button_text.lower():
                                    print(f"🔍 RuPaul All-Formats: Found episode button: '{button_text}' (React format)")
                                    
                                    # Extract episode count from button text
                                    episode_match = re.search(r'(\d+)\s+episodes?', button_text, re.IGNORECASE)
                                    if episode_match:
                                        episode_count = int(episode_match.group(1))
                                        print(f"✅ RuPaul All-Formats: Extracted episode count from button text '{button_text}': {episode_count}")
                                        
                                        # Try to click for season data
                                        seasons = self.click_and_extract_seasons_optimized(episode_button, cast_name)
                                        
                                        return {
                                            'episode_count': episode_count,
                                            'seasons': seasons,
                                            'found': True,
                                            'format': 'react'
                                        }
                        
                        except NoSuchElementException:
                            continue
                    
                    print(f"❌ RuPaul All-Formats: No valid React format episode data found")
                
                except NoSuchElementException:
                    print(f"⚠️ RuPaul All-Formats: Container {i} error: {container_selector}")
                    continue
            
            print(f"⚠️ RuPaul All-Formats: React format extraction failed for {cast_name}")
            return None
            
        except Exception as e:
            print(f"⚠️ RuPaul All-Formats: React format error: {str(e)}")
            return None

    def extract_from_table_format_comprehensive(self, cast_element, cast_name):
        """Extract from traditional table format with comprehensive search"""
        try:
            print(f"🔄 RuPaul All-Formats: Trying table format extraction...")
            
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
                            print(f"✅ RuPaul All-Formats: Found episodes in table: {episode_count}")
                            
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
            
            print(f"⚠️ RuPaul All-Formats: Table format extraction failed for {cast_name}")
            return None
            
        except Exception as e:
            print(f"⚠️ RuPaul All-Formats: Table format error: {str(e)}")
            return None

    def extract_from_hybrid_format(self, cast_element, cast_name):
        """Extract from hybrid format (mix of old and new elements)"""
        try:
            print(f"🔄 RuPaul All-Formats: Trying hybrid format extraction...")
            
            # Look in the general area around the cast element
            parent_area = cast_element.find_element(By.XPATH, "./ancestor::*[position()<=3]")
            
            # Search for any text containing episode information
            area_text = parent_area.text
            
            episode_match = re.search(r'(\d+)\s+episodes?', area_text, re.IGNORECASE)
            if episode_match:
                episode_count = int(episode_match.group(1))
                print(f"✅ RuPaul All-Formats: Found episodes in hybrid format: {episode_count}")
                
                # Look for season information in the same area
                season_matches = re.findall(r'S(\d+)\.E\d+', area_text)
                if season_matches:
                    seasons = sorted(list(set(int(s) for s in season_matches)))
                    if len(seasons) == 1:
                        seasons_str = str(seasons[0])
                    else:
                        seasons_str = f"{min(seasons)}-{max(seasons)}"
                else:
                    seasons_str = None
                
                return {
                    'episode_count': episode_count,
                    'seasons': seasons_str,
                    'found': True,
                    'format': 'hybrid'
                }
            
            print(f"⚠️ RuPaul All-Formats: Hybrid format extraction failed for {cast_name}")
            return None
            
        except Exception as e:
            print(f"⚠️ RuPaul All-Formats: Hybrid format error: {str(e)}")
            return None

    def extract_from_text_analysis(self, cast_element, cast_name):
        """Last resort: analyze all text around the element"""
        try:
            print(f"🔄 RuPaul All-Formats: Trying text analysis extraction...")
            
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
                            print(f"✅ RuPaul All-Formats: Found episodes via text analysis: {episode_count}")
                            
                            # Look for season markers
                            season_matches = re.findall(r'S(\d+)\.E\d+', context_text)
                            if season_matches:
                                seasons = sorted(list(set(int(s) for s in season_matches)))
                                if len(seasons) == 1:
                                    seasons_str = str(seasons[0])
                                else:
                                    seasons_str = f"{min(seasons)}-{max(seasons)}"
                            else:
                                seasons_str = None
                            
                            return {
                                'episode_count': episode_count,
                                'seasons': seasons_str,
                                'found': True,
                                'format': 'text_analysis'
                            }
            
            print(f"⚠️ RuPaul All-Formats: Text analysis extraction failed for {cast_name}")
            return None
            
        except Exception as e:
            print(f"⚠️ RuPaul All-Formats: Text analysis error: {str(e)}")
            return None

    def click_and_extract_seasons_optimized(self, episode_button, cast_name):
        """Click episode button and extract seasons using optimized methods"""
        try:
            print(f"🖱️ RuPaul All-Formats: Clicking episode button for {cast_name}")
            
            # Scroll to button and click
            self.driver.execute_script("arguments[0].scrollIntoView(true);", episode_button)
            self.smart_delay(1)
            
            # Get header text before clicking
            header_text = None
            try:
                # Look for header information that might contain episode count
                header_elements = self.driver.find_elements(By.CSS_SELECTOR, 'h3, h4, .ipc-title, .header, [role="heading"]')
                for header in header_elements[:3]:  # Check first few headers
                    if 'episode' in header.text.lower():
                        header_text = header.text.strip()
                        print(f"🔍 RuPaul All-Formats: Header item text: '{header_text}'")
                        
                        episode_match = re.search(r'(\d+)\s+episodes?', header_text, re.IGNORECASE)
                        if episode_match:
                            episode_count = int(episode_match.group(1))
                            print(f"✅ RuPaul All-Formats: Found episode count in modal header: {episode_count}")
                            break
            except:
                pass
            
            # Click the episode button
            try:
                episode_button.click()
                print(f"✅ RuPaul All-Formats: Episode element clicked successfully")
            except:
                try:
                    self.driver.execute_script("arguments[0].click();", episode_button)
                    print(f"✅ RuPaul All-Formats: Episode element clicked via JavaScript")
                except:
                    print(f"⚠️ RuPaul All-Formats: Could not click episode button")
                    return None
            
            self.smart_delay(2)
            
            # Extract seasons using optimized selectors
            print(f"🎯 RuPaul All-Formats: Looking for season tabs after episode button click...")
            
            # Season tab selectors in order of preference
            season_selectors = [
                'li[data-testid*="season-tab-"]',
                'li.ipc-tab[data-testid*="season-tab-"]', 
                'li[role="tab"][data-testid*="season-tab-"]',
                'ul[role="tablist"] li[data-testid*="season-tab-"]',
                '.ipc-tab[data-testid*="season-tab-"]'
            ]
            
            seasons_found = []
            
            for selector in season_selectors:
                try:
                    season_tabs = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    print(f"🔍 RuPaul All-Formats: Found {len(season_tabs)} season tabs with selector: {selector}")
                    
                    if season_tabs:
                        for tab in season_tabs:
                            # Extract season number from data-testid
                            testid = tab.get_attribute('data-testid') or ''
                            if 'season-tab-' in testid:
                                season_match = re.search(r'season-tab-(\d+)', testid)
                                if season_match:
                                    season_num = int(season_match.group(1))
                                    seasons_found.append(season_num)
                                    print(f"✅ RuPaul All-Formats: Found season {season_num} from data-testid")
                            
                            # Also try to extract from span text
                            span_text = tab.text.strip()
                            if span_text.isdigit():
                                season_num = int(span_text)
                                if season_num not in seasons_found:
                                    seasons_found.append(season_num)
                                    print(f"✅ RuPaul All-Formats: Found season {season_num} from span text")
                        
                        if seasons_found:
                            print(f"✅ RuPaul All-Formats: Season tabs found, breaking from selector loop")
                            break
                except Exception as e:
                    continue
            
            # Fallback: Try episode markers if no season tabs found
            if not seasons_found:
                print(f"🔍 RuPaul All-Formats: No season tabs found, trying episode markers as fallback...")
                seasons_found = self.extract_seasons_from_episode_markers_optimized()
            
            # Close modal
            self.close_modal_optimized()
            
            # Format seasons result
            if seasons_found:
                seasons_found = sorted(list(set(seasons_found)))
                if len(seasons_found) == 1:
                    seasons_str = str(seasons_found[0])
                else:
                    seasons_str = ', '.join(map(str, seasons_found))
                
                print(f"🎉 RuPaul All-Formats: Final seasons extracted: {seasons_found}")
                return seasons_str
            
            print(f"⚠️ RuPaul All-Formats: Could not extract seasons for {cast_name}")
            return None
            
        except Exception as e:
            print(f"❌ RuPaul All-Formats: Season extraction error: {str(e)}")
            self.close_modal_optimized()
            return None

    def extract_seasons_from_episode_markers_optimized(self):
        """Extract seasons from episode markers with optimized search"""
        seasons_found = []
        
        try:
            # Look for episode markers in the page
            episode_elements = self.driver.find_elements(By.CSS_SELECTOR, '*')
            
            for element in episode_elements[:100]:  # Limit search for performance
                try:
                    text = element.text.strip()
                    if 'S' in text and 'E' in text:
                        season_matches = re.findall(r'S(\d+)\.E\d+', text)
                        for season_num in season_matches:
                            season_int = int(season_num)
                            if season_int not in seasons_found:
                                seasons_found.append(season_int)
                                print(f"✅ RuPaul All-Formats: Found season {season_int} from episode marker: {text[:20]}...")
                except:
                    continue
            
            return sorted(list(set(seasons_found))) if seasons_found else []
            
        except:
            return []

    def close_modal_optimized(self):
        """Close modal with optimized strategies"""
        try:
            # Strategy 1: X button
            close_selectors = [
                '[data-testid="promptable__x"] button',
                '.ipc-promptable-base__close button',
                '.close',
                '[aria-label="Close"]'
            ]
            
            for selector in close_selectors:
                try:
                    close_btn = self.driver.find_element(By.CSS_SELECTOR, selector)
                    close_btn.click()
                    print(f"🔄 RuPaul All-Formats: Closed modal successfully")
                    self.smart_delay(1)
                    return True
                except:
                    continue
            
            # Strategy 2: Escape key
            try:
                self.driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.ESCAPE)
                print(f"✅ RuPaul All-Formats: Modal closed with Escape key")
                self.smart_delay(1)
                return True
            except:
                pass
            
            return False
            
        except:
            return False

    def extract_seasons_from_episode_markers_comprehensive(self):
        """Extract seasons from episode markers with comprehensive selectors"""
        seasons_found = set()
        
        # All possible episode marker selectors
        marker_selectors = [
            'li[role="presentation"] .ipc-inline-list__item',
            'ul.ipc-inline-list li.ipc-inline-list__item',
            '.episodic-credits-bottomsheet__menu-item li',
            'a[role="menuitem"] li',
            'li.ipc-inline-list__item',
            '.ipc-inline-list__item',
            'li[role="presentation"]',
            'li:contains("S")',
            'div:contains("S")',
            '*[class*="episode"]',
            '*[class*="season"]'
        ]
        
        for selector in marker_selectors:
            try:
                elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                for element in elements:
                    text = element.text.strip()
                    season_matches = re.findall(r'S(\d+)\.E\d+', text)
                    for season_num in season_matches:
                        seasons_found.add(int(season_num))
            except:
                continue
        
        if seasons_found:
            seasons = sorted(list(seasons_found))
            if len(seasons) == 1:
                return str(seasons[0])
            else:
                return f"{min(seasons)}-{max(seasons)}"
        
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
                        if len(seasons) == 1:
                            return str(seasons[0])
                        else:
                            return f"{min(seasons)}-{max(seasons)}"
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
                        if len(seasons) == 1:
                            return str(seasons[0])
                        else:
                            return f"{min(seasons)}-{max(seasons)}"
                except:
                    continue
            
            return None
            
        except:
            return None

    def extract_seasons_from_year_ranges_comprehensive(self):
        """Extract seasons from year ranges as fallback"""
        try:
            # Look for year ranges
            page_text = self.driver.find_element(By.TAG_NAME, 'body').text
            
            year_matches = re.findall(r'(\d{4})–(\d{4})', page_text)
            for start_year, end_year in year_matches:
                try:
                    start_year = int(start_year)
                    end_year = int(end_year)
                    
                    if 2000 <= start_year <= 2025 and 2000 <= end_year <= 2025:
                        estimated_seasons = end_year - start_year + 1
                        if 1 <= estimated_seasons <= 20:  # Reasonable range
                            if estimated_seasons == 1:
                                return "1"
                            else:
                                return f"1-{estimated_seasons}"
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
                    if len(seasons) == 1:
                        return str(seasons[0])
                    else:
                        return f"{min(seasons)}-{max(seasons)}"
                
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
                            return f"1-{estimated_seasons}"
            
            return None
            
        except Exception as e:
            print(f"⚠️ RuPaul All-Formats: Table cell season extraction error: {str(e)}")
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
        """Update cast member data in spreadsheet with optimized error handling"""
        try:
            # Update Episode Count (Column G)
            if episode_count is not None:
                episode_range = f'G{row_number}'
                episode_values = [[str(episode_count)]]
                self.sheet.update(episode_values, episode_range, value_input_option='RAW')
                print(f"📝 RuPaul All-Formats: Updated Episode Count for row {row_number}: {episode_count}")
            
            # Update Seasons (Column H)
            if seasons:
                seasons_range = f'H{row_number}'
                seasons_values = [[str(seasons)]]
                self.sheet.update(seasons_values, seasons_range, value_input_option='RAW')
                print(f"📝 RuPaul All-Formats: Updated Seasons for row {row_number}: {seasons}")
            
            self.smart_delay(0.5)
            return True
            
        except Exception as e:
            print(f"❌ RuPaul All-Formats: Error updating row {row_number}: {str(e)}")
            return False

    def run_extraction(self, start_row=2598):
        """Main extraction process for RuPaul section"""
        print(f"🚀 RuPaul All-Formats: Starting comprehensive extraction from row {start_row}")
        print(f"🎯 RuPaul All-Formats: Target - RuPaul's Drag Race section with ALL format support")
        
        try:
            # Setup
            if not self.setup_google_sheets():
                return False
            
            if not self.setup_webdriver():
                return False
            
            # Load RuPaul data
            members_to_process = self.load_rupaul_data_from_row(start_row)
            if not members_to_process:
                print("✅ RuPaul All-Formats: No members need processing!")
                return True
            
            # Process in small batches
            batch_size = 2
            total_processed = 0
            
            for i in range(0, len(members_to_process), batch_size):
                batch = members_to_process[i:i+batch_size]
                batch_num = (i // batch_size) + 1
                
                print(f"\n🔄 RuPaul All-Formats: Processing batch {batch_num} ({len(batch)} members)")
                
                for member in batch:
                    try:
                        cast_name = member['cast_name']
                        cast_imdb_id = member['cast_imdbid']
                        show_imdb_id = member['show_imdbid']
                        row_number = member['row_number']
                        
                        print(f"\n🎭 RuPaul All-Formats: Processing {cast_name} from {show_imdb_id} (Row {row_number})")
                        
                        # Universal search with all format support
                        result = self.find_cast_member_all_formats(show_imdb_id, cast_imdb_id, cast_name)
                        
                        if result and result.get('found'):
                            episode_count = result.get('episode_count')
                            seasons = result.get('seasons')
                            format_used = result.get('format', 'unknown')
                            
                            print(f"✅ RuPaul All-Formats: SUCCESS - {cast_name} ({format_used} format) - Episodes: {episode_count}, Seasons: {seasons}")
                            
                            if self.update_cast_member_data(row_number, episode_count, seasons):
                                self.processed_count += 1
                            else:
                                self.error_count += 1
                        else:
                            print(f"❌ RuPaul All-Formats: FAILED - Could not extract data for {cast_name}")
                            self.error_count += 1
                        
                        total_processed += 1
                        
                        # Progress update
                        if total_processed % 5 == 0:
                            success_rate = (self.processed_count / total_processed * 100) if total_processed > 0 else 0
                            print(f"📈 RuPaul All-Formats: Progress - Processed: {self.processed_count}, Errors: {self.error_count}, Success Rate: {success_rate:.1f}%")
                        
                        # Delay between members
                        self.smart_delay(2)
                        
                    except KeyboardInterrupt:
                        print("\n⏹️ RuPaul All-Formats: Interrupted by user")
                        return False
                    except Exception as e:
                        print(f"❌ RuPaul All-Formats: Error processing {member.get('cast_name', 'Unknown')}: {str(e)}")
                        self.error_count += 1
                        continue
                
                # Longer delay between batches
                if i + batch_size < len(members_to_process):
                    print(f"⏸️ RuPaul All-Formats: Batch complete, pausing before next batch...")
                    self.smart_delay(5)
            
            # Final summary
            success_rate = (self.processed_count / total_processed * 100) if total_processed > 0 else 0
            print(f"\n🎉 RuPaul All-Formats: Extraction Complete!")
            print(f"✅ Successfully processed: {self.processed_count}")
            print(f"❌ Errors encountered: {self.error_count}")
            print(f"📊 Total attempted: {total_processed}")
            print(f"📈 Success rate: {success_rate:.1f}%")
            
            return True
            
        except KeyboardInterrupt:
            print("\n⏹️ RuPaul All-Formats: Extraction interrupted by user")
            return False
        except Exception as e:
            print(f"❌ RuPaul All-Formats: Fatal error: {str(e)}")
            return False
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Clean up resources"""
        print("🧹 RuPaul All-Formats: Cleaning up...")
        if self.driver:
            try:
                self.driver.quit()
                print("✅ RuPaul All-Formats: WebDriver closed")
            except:
                pass

def main():
    """Main function"""
    extractor = UniversalSeasonExtractorRuPaulAllFormats()
    
    try:
        success = extractor.run_extraction(start_row=2598)
        if success:
            print("🎉 RuPaul All-Formats: Process completed successfully!")
        else:
            print("❌ RuPaul All-Formats: Process failed")
            sys.exit(1)
    except KeyboardInterrupt:
        print("\n⏹️ RuPaul All-Formats: Process interrupted")
        sys.exit(1)
    except Exception as e:
        print(f"❌ RuPaul All-Formats: Unexpected error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
