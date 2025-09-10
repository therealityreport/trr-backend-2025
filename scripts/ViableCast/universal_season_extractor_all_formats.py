#!/usr/bin/env python3
"""
Universal Season Extractor - ALL FORMATS SUPPORT
Handles ALL possible IMDb layout versions with comprehensive fallback strategies.
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

class UniversalSeasonExtractorAllFormats:
    def __init__(self):
        """Initialize the universal extractor with support for all IMDb formats"""
        self.driver = None
        self.sheet = None
        self.viable_cast_data = []
        self.processed_count = 0
        self.error_count = 0
        self.service_account_file = '/Users/thomashulihan/Projects/TRR-Backend/keys/trr-backend-df2c438612e1.json'
        
        # Enhanced configurations for maximum compatibility
        self.max_retries = 5
        self.base_delay = 2
        self.max_delay = 10
        self.request_timeout = 20
        self.page_load_timeout = 30
        
        print("🌟 Universal All-Formats Extractor: Initializing with comprehensive IMDb support...")
        
    def setup_google_sheets(self):
        """Setup Google Sheets connection with error handling"""
        print("🔄 Universal All-Formats: Setting up Google Sheets connection...")
        
        try:
            if not os.path.exists(self.service_account_file):
                raise FileNotFoundError(f"Service account file not found: {self.service_account_file}")
            
            gc = gspread.service_account(filename=self.service_account_file)
            workbook = gc.open("Realitease2025Data")
            self.sheet = workbook.worksheet("ViableCast")
            
            print("✅ Universal All-Formats: Google Sheets connection successful")
            return True
            
        except Exception as e:
            print(f"❌ Universal All-Formats: Google Sheets setup failed: {str(e)}")
            return False
    
    def setup_webdriver(self):
        """Setup WebDriver with maximum compatibility"""
        print("🔄 Universal All-Formats: Setting up WebDriver with maximum compatibility...")
        
        try:
            chrome_options = Options()
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            chrome_options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            chrome_options.add_argument("--disable-extensions")
            chrome_options.add_argument("--disable-plugins")
            chrome_options.add_argument("--disable-images")  # Faster loading
            
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            self.driver.implicitly_wait(15)
            self.driver.set_page_load_timeout(self.page_load_timeout)
            
            print("✅ Universal All-Formats: WebDriver setup successful")
            return True
            
        except Exception as e:
            print(f"❌ Universal All-Formats: WebDriver setup failed: {str(e)}")
            return False
    
    def smart_delay(self, base=None):
        """Implement smart delays to avoid rate limiting"""
        if base is None:
            base = self.base_delay
        delay = base + random.uniform(0.5, 2.0)
        time.sleep(delay)
    
    def load_viable_cast_data_from_row(self, start_row):
        """Load ViableCast data starting from specified row"""
        print(f"🔄 Universal All-Formats: Loading ViableCast data starting from row {start_row}...")
        
        try:
            # Get all data using raw values to handle duplicate headers
            all_data = self.sheet.get_all_values()
            
            print(f"📋 Universal All-Formats: Processing {len(all_data)} total rows...")
            
            # Get headers and find column positions
            headers = all_data[0] if all_data else []
            header_mapping = {}
            
            for i, header in enumerate(headers):
                normalized_header = str(header).strip().lower()
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
                print("❌ Universal All-Formats: Could not find Show IMDbID column")
                return None
            
            print(f"✅ Found Show IMDbID at column {show_imdbid_col}")
            print(f"📋 Universal All-Formats: Column mapping - Show IMDbID: {show_imdbid_col}, CastName: {castname_col}, Cast IMDbID: {cast_imdbid_col}, EpisodeCount: {episode_count_col}, Seasons: {seasons_col}")
            
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
            
            print(f"📊 Universal All-Formats: Filtered to {len(filtered_data)} records from row {start_row} onward")
            
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
            
            print(f"📊 Universal All-Formats: Analysis complete:")
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
            print(f"❌ Universal All-Formats: Error loading data: {str(e)}")
            return None

    def search_for_cast_member_all_formats(self, show_imdb_id, cast_imdb_id, cast_name):
        """
        Universal cast member search that handles ALL possible IMDb layouts.
        Tries multiple strategies in order of likelihood.
        """
        try:
            print(f"🔍 Universal All-Formats: Searching for {cast_name} ({cast_imdb_id}) in {show_imdb_id}")
            
            # Load the full credits page
            url = f"https://www.imdb.com/title/{show_imdb_id}/fullcredits"
            print(f"🔄 Universal All-Formats: Loading page: {url}")
            
            for attempt in range(self.max_retries):
                try:
                    self.driver.get(url)
                    # Wait for page content to load
                    WebDriverWait(self.driver, 15).until(
                        lambda driver: driver.execute_script("return document.readyState") == "complete"
                    )
                    self.smart_delay(4)
                    print(f"✅ Universal All-Formats: Page loaded successfully")
                    break
                except Exception as e:
                    if attempt < self.max_retries - 1:
                        print(f"⚠️ Universal All-Formats: Load attempt {attempt + 1} failed, retrying...")
                        self.smart_delay(7)
                    else:
                        print(f"❌ Universal All-Formats: Failed to load page after {self.max_retries} attempts")
                        return None
            
            # STRATEGY 1: Search by IMDb ID (most reliable)
            if cast_imdb_id:
                print(f"🎯 Universal All-Formats: STRATEGY 1 - Searching for IMDb ID: {cast_imdb_id}")
                result = self.search_by_imdb_id_all_formats(cast_imdb_id, cast_name)
                if result and result.get('found'):
                    return result
            
            # STRATEGY 2: Search by exact name match
            if cast_name:
                print(f"🎯 Universal All-Formats: STRATEGY 2 - Searching for exact name: {cast_name}")
                result = self.search_by_name_all_formats(cast_name)
                if result and result.get('found'):
                    return result
            
            # STRATEGY 3: Search by partial name match
            if cast_name:
                print(f"🎯 Universal All-Formats: STRATEGY 3 - Searching for partial name matches")
                result = self.search_by_partial_name_all_formats(cast_name)
                if result and result.get('found'):
                    return result
            
            print(f"❌ Universal All-Formats: Could not find cast member {cast_name} with any strategy")
            return None
            
        except Exception as e:
            print(f"❌ Universal All-Formats: Error searching for cast member: {str(e)}")
            return None

    def search_by_imdb_id_all_formats(self, cast_imdb_id, cast_name):
        """Search by IMDb ID using all possible selectors"""
        try:
            # All possible IMDb ID link selectors
            imdb_id_selectors = [
                f"//a[contains(@href, '/name/{cast_imdb_id}/')]",  # Standard XPath
                f"a[href*='/name/{cast_imdb_id}/']",               # CSS selector
                f"//a[contains(@href, '{cast_imdb_id}')]",         # Broader XPath
                f"a[href*='{cast_imdb_id}']"                       # Broader CSS
            ]
            
            for selector in imdb_id_selectors:
                try:
                    if selector.startswith("//"):
                        cast_element = self.driver.find_element(By.XPATH, selector)
                    else:
                        cast_element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    
                    print(f"✅ Universal All-Formats: Found cast member by IMDb ID using: {selector}")
                    return self.extract_episode_data_all_formats(cast_element, cast_name)
                    
                except NoSuchElementException:
                    continue
            
            return None
            
        except Exception as e:
            print(f"⚠️ Universal All-Formats: IMDb ID search error: {str(e)}")
            return None

    def search_by_name_all_formats(self, cast_name):
        """Search by exact name using all possible selectors"""
        try:
            # Name variations to try
            name_variations = [
                cast_name,
                cast_name.replace("'", "'"),
                cast_name.replace("'", ""),
                cast_name.replace("'", "'"),
                cast_name.strip(),
            ]
            
            # All possible name selectors
            for name in name_variations:
                name_selectors = [
                    f"//a[text()='{name}']",                        # Exact text match
                    f"//a[contains(text(), '{name}')]",             # Contains text
                    f"a:contains('{name}')",                        # CSS contains (if supported)
                    f"//*[text()='{name}']",                        # Any element with exact text
                    f"//*[contains(text(), '{name}')]",             # Any element containing text
                ]
                
                for selector in name_selectors:
                    try:
                        if selector.startswith("//") or selector.startswith("//*"):
                            cast_element = self.driver.find_element(By.XPATH, selector)
                        else:
                            continue  # Skip CSS :contains as it's not supported in modern browsers
                        
                        print(f"✅ Universal All-Formats: Found cast member by name '{name}' using: {selector}")
                        return self.extract_episode_data_all_formats(cast_element, cast_name)
                        
                    except NoSuchElementException:
                        continue
            
            return None
            
        except Exception as e:
            print(f"⚠️ Universal All-Formats: Name search error: {str(e)}")
            return None

    def search_by_partial_name_all_formats(self, cast_name):
        """Search by partial name matches for difficult cases"""
        try:
            # Break name into parts for partial matching
            name_parts = cast_name.split()
            if len(name_parts) < 2:
                return None
            
            first_name = name_parts[0]
            last_name = name_parts[-1]
            
            # Search for elements containing both first and last name
            partial_selectors = [
                f"//*[contains(text(), '{first_name}') and contains(text(), '{last_name}')]",
                f"//a[contains(text(), '{first_name}') and contains(text(), '{last_name}')]",
                f"//*[contains(text(), '{last_name}')]",  # Last name only
                f"//*[contains(text(), '{first_name}')]", # First name only (less reliable)
            ]
            
            for selector in partial_selectors:
                try:
                    elements = self.driver.find_elements(By.XPATH, selector)
                    
                    for element in elements:
                        element_text = element.text.strip()
                        # Check if this looks like our cast member
                        if (first_name.lower() in element_text.lower() and 
                            last_name.lower() in element_text.lower() and
                            len(element_text) < 100):  # Reasonable name length
                            
                            print(f"✅ Universal All-Formats: Found partial match: '{element_text}' for '{cast_name}'")
                            return self.extract_episode_data_all_formats(element, cast_name)
                    
                except NoSuchElementException:
                    continue
            
            return None
            
        except Exception as e:
            print(f"⚠️ Universal All-Formats: Partial name search error: {str(e)}")
            return None

    def extract_episode_data_all_formats(self, cast_element, cast_name):
        """
        Universal episode data extraction that tries ALL possible IMDb formats.
        Handles React, Traditional, and any hybrid formats.
        """
        try:
            print(f"🎭 Universal All-Formats: Extracting data for {cast_name}")
            
            # FORMAT 1: New React-based format (most common)
            print(f"🔄 Universal All-Formats: Trying React format...")
            result = self.extract_from_react_format_enhanced(cast_element, cast_name)
            if result and result.get('found'):
                return result
            
            # FORMAT 2: Traditional table format
            print(f"🔄 Universal All-Formats: Trying traditional table format...")
            result = self.extract_from_traditional_format_enhanced(cast_element, cast_name)
            if result and result.get('found'):
                return result
            
            # FORMAT 3: Hybrid format (mix of both)
            print(f"🔄 Universal All-Formats: Trying hybrid format...")
            result = self.extract_from_hybrid_format(cast_element, cast_name)
            if result and result.get('found'):
                return result
            
            # FORMAT 4: Text-based extraction (fallback)
            print(f"🔄 Universal All-Formats: Trying text-based extraction...")
            result = self.extract_from_text_content(cast_element, cast_name)
            if result and result.get('found'):
                return result
            
            print(f"❌ Universal All-Formats: Could not extract episode data for {cast_name} with any format")
            return None
            
        except Exception as e:
            print(f"❌ Universal All-Formats: Error extracting episode data: {str(e)}")
            return None

    def extract_from_react_format_enhanced(self, cast_element, cast_name):
        """Enhanced React format extraction with multiple container types"""
        try:
            # Try different React container patterns
            react_containers = [
                "./ancestor::li[contains(@class, 'full-credits-page-list-item')]",
                "./ancestor::li[contains(@class, 'ipc-metadata-list-summary-item')]",
                "./ancestor::div[contains(@class, 'sc-2840b417-3')]",
                "./ancestor::div[contains(@class, 'cast-list__item')]",
                "./ancestor::li[contains(@data-testid, 'name-credits')]",
                "./ancestor::*[contains(@class, 'cast')]",
            ]
            
            for container_xpath in react_containers:
                try:
                    parent_container = cast_element.find_element(By.XPATH, container_xpath)
                    
                    # Look for episode buttons with various patterns
                    episode_button_selectors = [
                        ".//button[contains(text(), 'episode')]",
                        ".//button[contains(@class, 'ipc-link') and contains(text(), 'episode')]",
                        ".//a[contains(text(), 'episode')]",
                        ".//span[contains(text(), 'episode')]",
                        ".//*[contains(text(), 'episode')]",
                    ]
                    
                    for button_selector in episode_button_selectors:
                        try:
                            episode_buttons = parent_container.find_elements(By.XPATH, button_selector)
                            
                            if episode_buttons:
                                episode_button = episode_buttons[0]
                                button_text = episode_button.text.strip()
                                print(f"🔍 Universal All-Formats: Found episode button: '{button_text}' (React format)")
                                
                                # Extract episode count
                                episode_match = re.search(r'(\d+)\s+episodes?', button_text)
                                if episode_match:
                                    episode_count = int(episode_match.group(1))
                                    print(f"✅ Universal All-Formats: Episode count: {episode_count}")
                                    
                                    # Try to click for season info
                                    seasons = self.click_episode_button_all_formats(episode_button, cast_name)
                                    
                                    return {
                                        'episode_count': episode_count,
                                        'seasons': seasons,
                                        'found': True,
                                        'format': 'react'
                                    }
                        except:
                            continue
                    
                except:
                    continue
            
            return None
            
        except Exception as e:
            print(f"⚠️ Universal All-Formats: React format extraction error: {str(e)}")
            return None

    def extract_from_traditional_format_enhanced(self, cast_element, cast_name):
        """Enhanced traditional format extraction with multiple table patterns"""
        try:
            # Try different traditional container patterns
            traditional_containers = [
                "./ancestor::tr",
                "./ancestor::table",
                "./ancestor::tbody",
                "./ancestor::*[contains(@class, 'cast_list')]",
                "./ancestor::*[contains(@class, 'credit')]",
            ]
            
            for container_xpath in traditional_containers:
                try:
                    parent_container = cast_element.find_element(By.XPATH, container_xpath)
                    
                    # Look for episode information in cells or text
                    cells = parent_container.find_elements(By.TAG_NAME, "td")
                    if not cells:
                        cells = parent_container.find_elements(By.TAG_NAME, "div")
                    
                    for cell in cells:
                        cell_text = cell.text.strip()
                        episode_match = re.search(r'(\d+)\s+episodes?', cell_text)
                        if episode_match:
                            episode_count = int(episode_match.group(1))
                            print(f"✅ Universal All-Formats: Episode count: {episode_count} (Traditional format)")
                            
                            # Look for season information
                            seasons = self.extract_seasons_from_text(cell_text)
                            
                            return {
                                'episode_count': episode_count,
                                'seasons': seasons,
                                'found': True,
                                'format': 'traditional'
                            }
                    
                except:
                    continue
            
            return None
            
        except Exception as e:
            print(f"⚠️ Universal All-Formats: Traditional format extraction error: {str(e)}")
            return None

    def extract_from_hybrid_format(self, cast_element, cast_name):
        """Extract from hybrid formats that mix React and traditional elements"""
        try:
            # Look in the general area around the cast element
            parent_area = cast_element.find_element(By.XPATH, "./ancestor::*[position()<=3]")
            
            # Search for any text containing episode information
            area_text = parent_area.text
            episode_match = re.search(r'(\d+)\s+episodes?', area_text)
            
            if episode_match:
                episode_count = int(episode_match.group(1))
                print(f"✅ Universal All-Formats: Episode count: {episode_count} (Hybrid format)")
                
                # Look for season information in the same area
                seasons = self.extract_seasons_from_text(area_text)
                
                return {
                    'episode_count': episode_count,
                    'seasons': seasons,
                    'found': True,
                    'format': 'hybrid'
                }
            
            return None
            
        except Exception as e:
            print(f"⚠️ Universal All-Formats: Hybrid format extraction error: {str(e)}")
            return None

    def extract_from_text_content(self, cast_element, cast_name):
        """Extract from raw text content as last resort"""
        try:
            # Get all text in the vicinity of the cast element
            nearby_elements = self.driver.find_elements(By.XPATH, f"//*[contains(text(), '{cast_name}')]/following-sibling::*[position()<=5]")
            nearby_elements.extend(self.driver.find_elements(By.XPATH, f"//*[contains(text(), '{cast_name}')]/parent::*//*"))
            
            for element in nearby_elements:
                element_text = element.text.strip()
                episode_match = re.search(r'(\d+)\s+episodes?', element_text)
                
                if episode_match:
                    episode_count = int(episode_match.group(1))
                    print(f"✅ Universal All-Formats: Episode count: {episode_count} (Text-based)")
                    
                    # Look for season information
                    seasons = self.extract_seasons_from_text(element_text)
                    
                    return {
                        'episode_count': episode_count,
                        'seasons': seasons,
                        'found': True,
                        'format': 'text-based'
                    }
            
            return None
            
        except Exception as e:
            print(f"⚠️ Universal All-Formats: Text-based extraction error: {str(e)}")
            return None

    def click_episode_button_all_formats(self, episode_button, cast_name):
        """Universal episode button clicking with comprehensive season extraction"""
        try:
            print(f"🖱️ Universal All-Formats: Clicking episode button for {cast_name}")
            
            # Scroll to button and click
            self.driver.execute_script("arguments[0].scrollIntoView(true);", episode_button)
            self.smart_delay(2)
            
            try:
                # Try different click methods
                click_methods = [
                    lambda: episode_button.click(),
                    lambda: self.driver.execute_script("arguments[0].click();", episode_button),
                    lambda: self.driver.execute_script("arguments[0].dispatchEvent(new Event('click'));", episode_button),
                ]
                
                for click_method in click_methods:
                    try:
                        click_method()
                        self.smart_delay(3)
                        print(f"✅ Universal All-Formats: Episode button clicked")
                        break
                    except:
                        continue
                
                # Try multiple season extraction methods
                seasons_methods = [
                    self.extract_seasons_from_episode_markers_enhanced,
                    self.extract_seasons_from_modal_links,
                    self.extract_seasons_from_year_ranges_enhanced,
                    self.extract_seasons_from_page_content,
                ]
                
                for method in seasons_methods:
                    try:
                        seasons = method()
                        if seasons:
                            print(f"✅ Universal All-Formats: Extracted seasons: {seasons}")
                            return seasons
                    except:
                        continue
                
                # Close any modal that might have opened
                self.close_modal_all_formats()
                
            except Exception as e:
                print(f"⚠️ Universal All-Formats: Button click failed: {str(e)}")
            
            return None
            
        except Exception as e:
            print(f"❌ Universal All-Formats: Error clicking episode button: {str(e)}")
            return None

    def extract_seasons_from_episode_markers_enhanced(self):
        """Enhanced season extraction from episode markers"""
        try:
            seasons_found = set()
            
            # Comprehensive list of selectors for episode markers
            episode_marker_selectors = [
                'li[role="presentation"] .ipc-inline-list__item',
                'ul.ipc-inline-list li.ipc-inline-list__item',
                '.episodic-credits-bottomsheet__menu-item li',
                'a[role="menuitem"] li',
                'li.ipc-inline-list__item',
                '.episode-list-item',
                '.episode-item',
                '*[class*="episode"]',
                '*[class*="season"]',
                'li:contains("S")',  # Will need XPath equivalent
            ]
            
            for selector in episode_marker_selectors:
                try:
                    if selector.endswith(')'):  # XPath selector
                        elements = self.driver.find_elements(By.XPATH, f"//*[contains(text(), 'S') and contains(text(), '.E')]")
                    else:
                        elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    
                    for element in elements[:30]:  # Check more elements
                        element_text = element.text.strip()
                        
                        # Look for various season patterns
                        season_patterns = [
                            r'S(\d+)\.E\d+',      # S4.E1
                            r'Season\s+(\d+)',     # Season 4
                            r'(\d+)x\d+',          # 4x01
                            r'S(\d+)',             # S4
                        ]
                        
                        for pattern in season_patterns:
                            matches = re.findall(pattern, element_text)
                            for match in matches:
                                try:
                                    season_num = int(match)
                                    seasons_found.add(season_num)
                                    print(f"✅ Universal All-Formats: Found season {season_num} from: {element_text}")
                                except:
                                    pass
                    
                    if seasons_found:
                        break
                        
                except:
                    continue
            
            if seasons_found:
                seasons = sorted(list(seasons_found))
                if len(seasons) == 1:
                    return str(seasons[0])
                else:
                    return f"{min(seasons)}-{max(seasons)}"
            
            return None
            
        except Exception as e:
            print(f"⚠️ Universal All-Formats: Episode marker extraction error: {str(e)}")
            return None

    def extract_seasons_from_modal_links(self):
        """Extract seasons from season links in modals"""
        try:
            # Look for season links with various patterns
            season_link_selectors = [
                'a[href*="/episodes?season="]',
                'a[href*="season="]',
                '*[href*="season"]',
                'a[href*="/season/"]',
            ]
            
            seasons_found = set()
            
            for selector in season_link_selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    
                    for element in elements:
                        href = element.get_attribute('href')
                        if href and 'season=' in href:
                            season_match = re.search(r'season=(\d+)', href)
                            if season_match:
                                season_num = int(season_match.group(1))
                                seasons_found.add(season_num)
                                print(f"✅ Universal All-Formats: Found season {season_num} from link")
                except:
                    continue
            
            if seasons_found:
                seasons = sorted(list(seasons_found))
                if len(seasons) == 1:
                    return str(seasons[0])
                else:
                    return f"{min(seasons)}-{max(seasons)}"
            
            return None
            
        except Exception as e:
            print(f"⚠️ Universal All-Formats: Modal link extraction error: {str(e)}")
            return None

    def extract_seasons_from_year_ranges_enhanced(self):
        """Enhanced year range extraction"""
        try:
            # Look for year patterns in text
            year_elements = self.driver.find_elements(By.XPATH, "//*[contains(text(), '20') and (contains(text(), '–') or contains(text(), '-'))]")
            
            for element in year_elements:
                text = element.text
                # Various year range patterns
                year_patterns = [
                    r'(\d{4})[–-](\d{4})',    # 2020–2023
                    r'(\d{4})[–-]',           # 2020–
                    r'(\d{4})\s*-\s*(\d{4})', # 2020 - 2023
                ]
                
                for pattern in year_patterns:
                    match = re.search(pattern, text)
                    if match:
                        start_year = int(match.group(1))
                        end_year = int(match.group(2)) if len(match.groups()) > 1 and match.group(2) else start_year
                        
                        if end_year - start_year >= 0:
                            estimated_seasons = max(1, end_year - start_year + 1)
                            if estimated_seasons > 1:
                                return f"1-{estimated_seasons}"
                            else:
                                return "1"
            
            return None
            
        except Exception as e:
            print(f"⚠️ Universal All-Formats: Year range extraction error: {str(e)}")
            return None

    def extract_seasons_from_page_content(self):
        """Extract seasons from any visible page content"""
        try:
            # Get all visible text on the page
            page_text = self.driver.find_element(By.TAG_NAME, "body").text
            
            # Look for season indicators
            season_patterns = [
                r'Seasons?\s+(\d+)[-–](\d+)',  # Seasons 1-3
                r'Season\s+(\d+)',             # Season 4
                r'(\d+)\s+seasons?',           # 4 seasons
            ]
            
            seasons_found = set()
            
            for pattern in season_patterns:
                matches = re.findall(pattern, page_text, re.IGNORECASE)
                for match in matches:
                    if isinstance(match, tuple):
                        for num in match:
                            try:
                                seasons_found.add(int(num))
                            except:
                                pass
                    else:
                        try:
                            seasons_found.add(int(match))
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
            print(f"⚠️ Universal All-Formats: Page content extraction error: {str(e)}")
            return None

    def extract_seasons_from_text(self, text):
        """Extract season information from any text"""
        try:
            # Look for season patterns in the text
            season_patterns = [
                r'Season\s+(\d+)',             # Season 4
                r'(\d+)\s+seasons?',           # 4 seasons
                r'S(\d+)',                     # S4
                r'(\d{4})[–-](\d{4})',        # Year range
            ]
            
            for pattern in season_patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    if len(match.groups()) == 2:  # Year range
                        start_year = int(match.group(1))
                        end_year = int(match.group(2))
                        estimated_seasons = max(1, end_year - start_year + 1)
                        if estimated_seasons > 1:
                            return f"1-{estimated_seasons}"
                        else:
                            return "1"
                    else:
                        return match.group(1)
            
            return None
            
        except Exception as e:
            print(f"⚠️ Universal All-Formats: Text season extraction error: {str(e)}")
            return None

    def close_modal_all_formats(self):
        """Universal modal closing with multiple strategies"""
        close_strategies = [
            # Strategy 1: X button with data-testid
            lambda: self.driver.find_element(By.CSS_SELECTOR, '[data-testid="promptable__x"] button').click(),
            # Strategy 2: Close button by class
            lambda: self.driver.find_element(By.CSS_SELECTOR, '.ipc-promptable-base__close button').click(),
            # Strategy 3: Any close button
            lambda: self.driver.find_element(By.XPATH, "//button[contains(@aria-label, 'Close') or contains(text(), 'Close')]").click(),
            # Strategy 4: Click outside modal
            lambda: self.driver.execute_script("document.body.click();"),
            # Strategy 5: Press Escape key
            lambda: self.driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.ESCAPE),
        ]
        
        for strategy in close_strategies:
            try:
                strategy()
                self.smart_delay(1)
                print(f"🔄 Universal All-Formats: Closed modal successfully")
                return True
            except:
                continue
        
        print(f"⚠️ Universal All-Formats: Could not close modal")
        return False

    def update_cast_member_data(self, row_number, episode_count, seasons):
        """Update cast member data in the spreadsheet with proper API formatting"""
        try:
            # Update Episode Count (Column G)
            if episode_count is not None:
                range_name = f'ViableCast!G{row_number}:G{row_number}'
                values = [[str(episode_count)]]
                self.sheet.update(range_name, values, value_input_option='RAW')
                print(f"📝 Universal All-Formats: Updated Episode Count for row {row_number}: {episode_count}")
            
            # Update Seasons (Column H)
            if seasons:
                range_name = f'ViableCast!H{row_number}:H{row_number}'
                values = [[str(seasons)]]
                self.sheet.update(range_name, values, value_input_option='RAW')
                print(f"📝 Universal All-Formats: Updated Seasons for row {row_number}: {seasons}")
            
            # Small delay to respect API limits
            self.smart_delay(1)
            
            return True
            
        except Exception as e:
            print(f"❌ Universal All-Formats: Error updating row {row_number}: {str(e)}")
            return False

    def run_extraction(self, start_row=1):
        """Main extraction process with universal format support"""
        print(f"🚀 Universal All-Formats: Starting comprehensive extraction from row {start_row}")
        print(f"🎯 Universal All-Formats: Supporting ALL possible IMDb layout formats")
        
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
            
            print(f"📊 Universal All-Formats: Found {len(members_to_process)} cast members to process")
            
            if not members_to_process:
                print("✅ Universal All-Formats: All cast members already have complete data!")
                return True
            
            # Process in smaller batches for better monitoring
            batch_size = 2
            total_processed = 0
            
            for i in range(0, len(members_to_process), batch_size):
                batch = members_to_process[i:i+batch_size]
                batch_num = (i // batch_size) + 1
                
                print(f"\n🔄 Universal All-Formats: Processing batch {batch_num} ({len(batch)} members)")
                
                for member in batch:
                    try:
                        cast_name = member['cast_name']
                        cast_imdb_id = member['cast_imdbid']
                        show_imdb_id = member['show_imdbid']
                        row_number = member['row_number']
                        
                        print(f"\n🎭 Universal All-Formats: Processing {cast_name} from {show_imdb_id} (Row {row_number})")
                        
                        # Universal search with all format support
                        result = self.search_for_cast_member_all_formats(show_imdb_id, cast_imdb_id, cast_name)
                        
                        if result and result.get('found'):
                            episode_count = result.get('episode_count')
                            seasons = result.get('seasons')
                            format_used = result.get('format', 'unknown')
                            
                            print(f"✅ Universal All-Formats: Found {cast_name} ({format_used} format) - Episodes: {episode_count}, Seasons: {seasons}")
                            
                            # Update spreadsheet
                            if self.update_cast_member_data(row_number, episode_count, seasons):
                                self.processed_count += 1
                            else:
                                self.error_count += 1
                        else:
                            print(f"⚠️ Universal All-Formats: Could not find episode data for {cast_name}")
                            self.error_count += 1
                        
                        total_processed += 1
                        
                        # Progress update
                        if total_processed % 10 == 0:
                            success_rate = (self.processed_count / total_processed * 100) if total_processed > 0 else 0
                            print(f"📈 Universal All-Formats: Progress - Processed: {self.processed_count}, Errors: {self.error_count}, Total: {total_processed}, Success Rate: {success_rate:.1f}%")
                        
                        # Short delay between members
                        self.smart_delay(4)
                        
                    except KeyboardInterrupt:
                        print("\n⏹️ Universal All-Formats: Interrupted by user")
                        return False
                    except Exception as e:
                        print(f"❌ Universal All-Formats: Error processing {member.get('cast_name', 'Unknown')}: {str(e)}")
                        self.error_count += 1
                        continue
                
                # Longer delay between batches
                if i + batch_size < len(members_to_process):
                    print(f"⏸️ Universal All-Formats: Batch complete, pausing before next batch...")
                    self.smart_delay(10)
            
            # Final summary
            success_rate = (self.processed_count / total_processed * 100) if total_processed > 0 else 0
            print(f"\n🎉 Universal All-Formats: Extraction complete!")
            print(f"✅ Successfully processed: {self.processed_count}")
            print(f"❌ Errors encountered: {self.error_count}")
            print(f"📊 Total attempted: {total_processed}")
            print(f"📈 Final success rate: {success_rate:.1f}%")
            
            return True
            
        except KeyboardInterrupt:
            print("\n⏹️ Universal All-Formats: Extraction interrupted by user")
            return False
        except Exception as e:
            print(f"❌ Universal All-Formats: Fatal error: {str(e)}")
            return False
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Clean up resources"""
        print("🧹 Universal All-Formats: Cleaning up resources...")
        if self.driver:
            try:
                self.driver.quit()
                print("✅ Universal All-Formats: WebDriver closed")
            except:
                pass

def main():
    """Main function"""
    extractor = UniversalSeasonExtractorAllFormats()
    
    try:
        success = extractor.run_extraction(start_row=1)  # Start from beginning or specify row
        if success:
            print("🎉 Universal All-Formats: Process completed successfully!")
        else:
            print("❌ Universal All-Formats: Process failed")
            sys.exit(1)
    except KeyboardInterrupt:
        print("\n⏹️ Universal All-Formats: Process interrupted")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Universal All-Formats: Unexpected error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
