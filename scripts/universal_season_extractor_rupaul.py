#!/usr/bin/env python3
"""
Universal Season Extractor - RuPaul's Drag Race Section
Starts from RuPaul's Drag Race (tt1353056) at row 2598 and processes downward.
Optimized for robust handling of any data presentation format with timeout prevention.
"""

import os
import sys
import time
import random
from collections import defaultdict
import gspread
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from google.auth.exceptions import RefreshError

class UniversalSeasonExtractorRuPaul:
    def __init__(self):
        """Initialize the RuPaul section extractor with robust error handling"""
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
        
        print("🎭 RuPaul Extractor: Initializing with enhanced anti-timeout measures...")
        
    def setup_google_sheets(self):
        """Setup Google Sheets connection with error handling"""
        print("🔄 RuPaul Extractor: Setting up Google Sheets connection...")
        
        try:
            if not os.path.exists(self.service_account_file):
                raise FileNotFoundError(f"Service account file not found: {self.service_account_file}")
            
            gc = gspread.service_account(filename=self.service_account_file)
            workbook = gc.open("Realitease2025Data")
            self.sheet = workbook.worksheet("ViableCast")
            
            print("✅ RuPaul Extractor: Google Sheets connection successful")
            return True
            
        except Exception as e:
            print(f"❌ RuPaul Extractor: Google Sheets setup failed: {str(e)}")
            return False
    
    def load_viable_cast_data_from_row(self, start_row):
        """Load ViableCast data starting from specific row with robust header handling"""
        print(f"🔄 RuPaul Extractor: Loading ViableCast data starting from row {start_row}...")
        
        try:
            # Get all values as raw data to handle duplicate headers
            all_data = self.sheet.get_all_values()
            
            if not all_data:
                print(f"❌ RuPaul Extractor: No data found in sheet")
                return {}
            
            # Get headers and find column indices
            headers = all_data[0]
            print(f"📋 RuPaul Extractor: Processing {len(all_data)} total rows...")
            
            # Find required columns with flexible matching
            show_imdb_col = None
            cast_name_col = None
            episode_count_col = None
            seasons_col = None
            
            for i, header in enumerate(headers):
                header_clean = str(header).strip().lower()
                print(f"  Column {i}: '{header}' -> '{header_clean}'")
                if header_clean == 'show imdbid':  # Exact match for the correct column
                    show_imdb_col = i
                    print(f"  ✅ Found Show IMDbID at column {i}")
                elif 'castname' in header_clean:
                    cast_name_col = i
                elif 'episodecount' in header_clean:
                    episode_count_col = i
                elif 'seasons' in header_clean:
                    seasons_col = i
            
            print(f"📋 RuPaul Extractor: Column mapping - Show IMDbID: {show_imdb_col}, CastName: {cast_name_col}, EpisodeCount: {episode_count_col}, Seasons: {seasons_col}")
            
            if None in [show_imdb_col, cast_name_col, episode_count_col, seasons_col]:
                print(f"❌ RuPaul Extractor: Could not find required columns")
                return {}
            
            # Process data rows starting from our target row
            filtered_records = []
            shows_analysis = defaultdict(lambda: {'total': 0, 'complete': 0, 'incomplete': 0, 'cast_members': []})
            
            for row_idx in range(start_row - 1, len(all_data)):  # start_row - 1 because list is 0-indexed
                row = all_data[row_idx]
                actual_row_num = row_idx + 1  # 1-indexed row number
                
                # Skip if row doesn't have enough columns
                if len(row) <= max(show_imdb_col, cast_name_col, episode_count_col, seasons_col):
                    continue
                
                show_id = str(row[show_imdb_col]).strip() if show_imdb_col < len(row) else ''
                cast_name = str(row[cast_name_col]).strip() if cast_name_col < len(row) else ''
                episode_count = str(row[episode_count_col]).strip() if episode_count_col < len(row) else ''
                seasons = str(row[seasons_col]).strip() if seasons_col < len(row) else ''
                
                # Skip if no show ID or cast name
                if not show_id or not cast_name:
                    continue
                
                # Determine if this entry is complete
                is_complete = bool(episode_count and seasons and episode_count != '' and seasons != '')
                
                # Create record for filtered_records list (for backward compatibility)
                record = {
                    'Show IMDbID': show_id,
                    'CastName': cast_name,
                    'EpisodeCount': episode_count,
                    'Seasons': seasons,
                    '_row_number': actual_row_num
                }
                filtered_records.append(record)
                
                # Add to shows analysis
                cast_entry = {
                    'name': cast_name,
                    'show_id': show_id,
                    'episode_count': episode_count,
                    'seasons': seasons,
                    'complete': is_complete,
                    'row': actual_row_num
                }
                
                shows_analysis[show_id]['cast_members'].append(cast_entry)
                shows_analysis[show_id]['total'] += 1
                if is_complete:
                    shows_analysis[show_id]['complete'] += 1
                else:
                    shows_analysis[show_id]['incomplete'] += 1
            
            print(f"📊 RuPaul Extractor: Filtered to {len(filtered_records)} records from row {start_row} onward")
            
            # Count shows that need processing
            shows_needing_processing = sum(1 for show_data in shows_analysis.values() if show_data['incomplete'] > 0)
            total_incomplete_cast = sum(show_data['incomplete'] for show_data in shows_analysis.values())
            
            print(f"📊 RuPaul Extractor: Analysis complete:")
            print(f"  📝 Total records processed: {len(filtered_records)}")
            print(f"  ✅ Already complete: {len(filtered_records) - total_incomplete_cast}")
            print(f"  ❌ Need processing: {total_incomplete_cast}")
            print(f"  📺 Shows needing processing: {shows_needing_processing}")
            
            self.viable_cast_data = filtered_records
            return shows_analysis
            
        except Exception as e:
            print(f"❌ RuPaul Extractor: Error loading data: {str(e)}")
            return {}
    
    def setup_webdriver(self):
        """Setup Chrome WebDriver with enhanced anti-detection and timeout prevention"""
        print("🔄 RuPaul Extractor: Setting up WebDriver with enhanced anti-detection...")
        
        try:
            options = Options()
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-blink-features=AutomationControlled')
            options.add_experimental_option("excludeSwitches", ["enable-automation"])
            options.add_experimental_option('useAutomationExtension', False)
            options.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
            options.add_argument('--disable-extensions')
            options.add_argument('--disable-plugins')
            options.add_argument('--disable-images')  # Speed up page loading
            options.add_argument('--aggressive-cache-discard')
            
            # Set timeouts
            options.add_argument(f'--timeout={self.page_load_timeout}')
            
            self.driver = webdriver.Chrome(options=options)
            self.driver.set_page_load_timeout(self.page_load_timeout)
            self.driver.implicitly_wait(10)
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            print("✅ RuPaul Extractor: WebDriver setup successful")
            return True
            
        except Exception as e:
            print(f"❌ RuPaul Extractor: WebDriver setup failed: {str(e)}")
            return False
    
    def get_show_label_with_fallback(self, show_id):
        """Get show label with multiple fallback strategies"""
        try:
            # Try main title first
            title_selectors = [
                'h1[data-testid="hero__pageTitle"] span.hero__primary-text',
                'h1[data-testid="hero__pageTitle"]',
                'h1.titleHeader',
                'h1.hero__primary-text',
                '.title_wrapper h1',
                'h1'
            ]
            
            for selector in title_selectors:
                try:
                    element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    if element and element.text.strip():
                        return element.text.strip()
                except:
                    continue
            
            # Fallback to show ID
            return f"Show {show_id}"
            
        except Exception as e:
            print(f"⚠️ RuPaul Extractor: Could not get show label: {str(e)}")
            return f"Show {show_id}"
    
    def smart_delay(self, base_delay=None):
        """Intelligent delay with randomization to prevent pattern detection"""
        if base_delay is None:
            base_delay = self.base_delay
        
        # Add randomization (±50%)
        delay = base_delay + random.uniform(-base_delay * 0.5, base_delay * 0.5)
        delay = max(1, min(delay, self.max_delay))  # Clamp between 1 and max_delay
        
        time.sleep(delay)
    
    def robust_page_load(self, url, retries=None):
        """Load page with robust error handling and retry logic"""
        if retries is None:
            retries = self.max_retries
        
        for attempt in range(retries):
            try:
                print(f"🔄 RuPaul Extractor: Loading page (attempt {attempt + 1}/{retries}): {url}")
                
                self.driver.get(url)
                
                # Wait for page to load
                WebDriverWait(self.driver, self.request_timeout).until(
                    lambda driver: driver.execute_script("return document.readyState") == "complete"
                )
                
                # Additional wait for dynamic content
                self.smart_delay(2)
                
                print("✅ RuPaul Extractor: Page loaded successfully")
                return True
                
            except TimeoutException:
                print(f"⚠️ RuPaul Extractor: Page load timeout (attempt {attempt + 1})")
                if attempt < retries - 1:
                    self.smart_delay(self.base_delay * (attempt + 1))
                    continue
                else:
                    print(f"❌ RuPaul Extractor: All page load attempts failed")
                    return False
                    
            except Exception as e:
                print(f"⚠️ RuPaul Extractor: Page load error (attempt {attempt + 1}): {str(e)}")
                if attempt < retries - 1:
                    self.smart_delay(self.base_delay * (attempt + 1))
                    continue
                else:
                    print(f"❌ RuPaul Extractor: All page load attempts failed")
                    return False
        
        return False
    
    def extract_cast_episodes_and_seasons(self, cast_link_element):
        """Extract episode count and seasons by clicking the episode button and analyzing the modal"""
        try:
            episode_count = None
            seasons = None
            
            print(f"🔍 RuPaul Extractor: Starting extraction for cast member...")
            
            # Step 1: Find the episode button in the same container as the cast member link
            try:
                # Find the parent container for this cast member
                parent_container = cast_link_element.find_element(By.XPATH, "./ancestor::div[contains(@class, 'sc-2840b417-3')]")
                
                # Look for the episode button within this container
                episode_buttons = parent_container.find_elements(By.XPATH, ".//button[contains(text(), 'episode')]")
                
                if episode_buttons:
                    episode_button = episode_buttons[0]
                    button_text = episode_button.text.strip()
                    print(f"🎯 RuPaul Extractor: Found episode button: '{button_text}'")
                    
                    # Extract episode count from button text before clicking
                    import re
                    episode_match = re.search(r'(\d+)\s+episodes?', button_text)
                    if episode_match:
                        episode_count = int(episode_match.group(1))
                        print(f"✅ RuPaul Extractor: Extracted episode count from button: {episode_count}")
                    
                    # Step 2: Click the episode button to open the modal
                    print(f"🖱️ RuPaul Extractor: Clicking episode button...")
                    self.driver.execute_script("arguments[0].scrollIntoView(true);", episode_button)
                    self.smart_delay(1)
                    
                    # Try clicking the button
                    try:
                        episode_button.click()
                        print(f"✅ RuPaul Extractor: Episode button clicked successfully")
                        self.smart_delay(2)  # Wait for modal to load
                        
                        # Step 3: Extract seasons from the modal
                        try:
                            # Look for season tabs in the modal
                            season_tabs = self.driver.find_elements(By.XPATH, "//li[@role='tab' and contains(@data-testid, 'season-tab-')]")
                            
                            if season_tabs:
                                season_numbers = []
                                for tab in season_tabs:
                                    try:
                                        testid = tab.get_attribute('data-testid')
                                        if testid and 'season-tab-' in testid:
                                            season_num = testid.replace('season-tab-', '')
                                            if season_num.isdigit():
                                                season_numbers.append(int(season_num))
                                    except:
                                        continue
                                
                                if season_numbers:
                                    season_numbers.sort()
                                    if len(season_numbers) == 1:
                                        seasons = str(season_numbers[0])
                                    else:
                                        seasons = f"1-{max(season_numbers)}"
                                    print(f"✅ RuPaul Extractor: Extracted seasons from tabs: {seasons} (found {len(season_numbers)} seasons)")
                            
                            # Step 4: Also try to get episode count from modal header if we didn't get it from button
                            if episode_count is None:
                                try:
                                    modal_episode_elements = self.driver.find_elements(By.XPATH, "//div[contains(@class, 'ipc-prompt-header')]//li[contains(text(), 'episode')]")
                                    if modal_episode_elements:
                                        modal_text = modal_episode_elements[0].text.strip()
                                        episode_match = re.search(r'(\d+)\s+episodes?', modal_text)
                                        if episode_match:
                                            episode_count = int(episode_match.group(1))
                                            print(f"✅ RuPaul Extractor: Extracted episode count from modal: {episode_count}")
                                except Exception as e:
                                    print(f"⚠️ RuPaul Extractor: Could not extract episodes from modal: {str(e)}")
                            
                        except Exception as e:
                            print(f"⚠️ RuPaul Extractor: Error extracting data from modal: {str(e)}")
                        
                        # Step 5: Close the modal
                        try:
                            # Look for close button or click outside modal
                            close_buttons = self.driver.find_elements(By.XPATH, "//button[contains(@aria-label, 'Close') or contains(@class, 'close') or contains(@data-testid, 'promptable-overlay-close')]")
                            if close_buttons:
                                close_buttons[0].click()
                                print(f"✅ RuPaul Extractor: Modal closed successfully")
                            else:
                                # Try pressing Escape key
                                from selenium.webdriver.common.keys import Keys
                                self.driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.ESCAPE)
                                print(f"✅ RuPaul Extractor: Modal closed with Escape key")
                            
                            self.smart_delay(1)  # Wait for modal to close
                            
                        except Exception as e:
                            print(f"⚠️ RuPaul Extractor: Could not close modal: {str(e)}")
                        
                    except Exception as e:
                        print(f"⚠️ RuPaul Extractor: Could not click episode button: {str(e)}")
                        
                else:
                    print(f"⚠️ RuPaul Extractor: No episode button found in container")
                    
            except Exception as e:
                print(f"⚠️ RuPaul Extractor: Could not find parent container: {str(e)}")
            
            # Fallback: If we still don't have episode count, try to extract from visible text
            if episode_count is None:
                try:
                    # Look for episode text in the cast member's area
                    parent = cast_link_element.find_element(By.XPATH, "./ancestor::div[2]")
                    all_text = parent.text
                    import re
                    episode_match = re.search(r'(\d+)\s+episodes?', all_text, re.IGNORECASE)
                    if episode_match:
                        episode_count = int(episode_match.group(1))
                        print(f"✅ RuPaul Extractor: Fallback episode extraction: {episode_count}")
                except Exception as e:
                    print(f"⚠️ RuPaul Extractor: Fallback episode extraction failed: {str(e)}")
            
            # Fallback: If we still don't have seasons, try to estimate from date range
            if seasons is None:
                try:
                    parent = cast_link_element.find_element(By.XPATH, "./ancestor::div[2]")
                    all_text = parent.text
                    import re
                    # Look for year ranges like "2012–2025"
                    year_match = re.search(r'(\d{4})[–-](\d{4})', all_text)
                    if year_match:
                        start_year = int(year_match.group(1))
                        end_year = int(year_match.group(2))
                        season_count = end_year - start_year + 1
                        seasons = f"1-{season_count}" if season_count > 1 else "1"
                        print(f"✅ RuPaul Extractor: Fallback season estimation from years: {seasons}")
                except Exception as e:
                    print(f"⚠️ RuPaul Extractor: Fallback season estimation failed: {str(e)}")
            
            return episode_count, seasons
            
        except Exception as e:
            print(f"❌ RuPaul Extractor: Error in extract_cast_episodes_and_seasons: {str(e)}")
            return None, None
    
    def process_cast_member_with_timeout_protection(self, cast_entry):
        """Process individual cast member with comprehensive timeout protection"""
        try:
            show_id = cast_entry['show_id']
            cast_name = cast_entry['name']
            row_num = cast_entry['row']
            
            print(f"\n🎭 RuPaul Extractor: Processing {cast_name} from {show_id} (Row {row_num})")
            
            # Construct IMDb URL
            base_url = f"https://www.imdb.com/title/{show_id}/fullcredits"
            
            # Load page with retry logic
            if not self.robust_page_load(base_url):
                print(f"❌ RuPaul Extractor: Failed to load page for {cast_name}")
                return False
            
            # Get show label
            show_label = self.get_show_label_with_fallback(show_id)
            print(f"📺 RuPaul Extractor: Processing {show_label}")
            
            # Smart delay before searching
            self.smart_delay()
            
            # Find cast member with multiple strategies for both old and new IMDb formats
            cast_found = False
            episode_count = None
            seasons = None
            
            # Strategy 1: New IMDb format - look for name in React components
            react_selectors = [
                f"//a[contains(@class, 'name-credits--title-text') and contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{cast_name.lower()}')]",
                f"//a[contains(@class, 'ipc-link') and contains(@href, '/name/') and contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{cast_name.lower()}')]"
            ]
            
            for selector in react_selectors:
                try:
                    cast_elements = self.driver.find_elements(By.XPATH, selector)
                    
                    for cast_element in cast_elements:
                        element_text = cast_element.text.strip()
                        if element_text and cast_name.lower() in element_text.lower():
                            print(f"🎭 RuPaul Extractor: Found {cast_name} in new format")
                            
                            # Extract episode and season info using updated method
                            ep_count, seasons_str = self.extract_cast_episodes_and_seasons(cast_element)
                            
                            if ep_count is not None:
                                episode_count = ep_count
                                seasons = seasons_str
                                cast_found = True
                                print(f"✅ RuPaul Extractor: Found {cast_name} - Episodes: {episode_count}, Seasons: {seasons}")
                                break
                
                except Exception as e:
                    print(f"⚠️ RuPaul Extractor: Error with new format selector: {str(e)}")
                    continue
                
                if cast_found:
                    break
            
            # Strategy 2: Traditional IMDb format - original selectors
            if not cast_found:
                traditional_selectors = [
                    f"//a[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{cast_name.lower()}')]",
                    f"//td[contains(@class, 'name')]//a[contains(text(), '{cast_name}')]",
                    f"//a[contains(text(), '{cast_name}')]"
                ]
                
                for selector in traditional_selectors:
                    try:
                        cast_elements = self.driver.find_elements(By.XPATH, selector)
                        
                        for cast_element in cast_elements:
                            # Check if this is in a cast/crew context
                            element_text = cast_element.text.strip()
                            if element_text and cast_name.lower() in element_text.lower():
                                
                                # Extract episode and season info
                                ep_count, seasons_str = self.extract_cast_episodes_and_seasons(cast_element)
                                
                                if ep_count is not None:
                                    episode_count = ep_count
                                    seasons = seasons_str
                                    cast_found = True
                                    print(f"✅ RuPaul Extractor: Found {cast_name} in traditional format - Episodes: {episode_count}, Seasons: {seasons}")
                                    break
                    
                    except Exception as e:
                        print(f"⚠️ RuPaul Extractor: Error with traditional selector {selector}: {str(e)}")
                        continue
                    
                    if cast_found:
                        break
            
            # If found, update the spreadsheet
            if cast_found and episode_count is not None:
                success = self.update_cast_member_data(row_num, episode_count, seasons)
                if success:
                    self.processed_count += 1
                    print(f"🎯 RuPaul Extractor: Successfully updated {cast_name} (Episodes: {episode_count}, Seasons: {seasons})")
                    return True
                else:
                    print(f"❌ RuPaul Extractor: Failed to update spreadsheet for {cast_name}")
            else:
                print(f"⚠️ RuPaul Extractor: Could not find episode data for {cast_name}")
            
            return False
            
        except Exception as e:
            print(f"❌ RuPaul Extractor: Error processing {cast_entry.get('name', 'unknown')}: {str(e)}")
            self.error_count += 1
            return False
    
    def update_cast_member_data(self, row_number, episode_count, seasons):
        """Update cast member data in the spreadsheet with error handling"""
        try:
            # Update Episode Count (Column G) - fix the range format
            if episode_count is not None:
                range_name = f'ViableCast!G{row_number}:G{row_number}'
                values = [[str(episode_count)]]
                body = {'values': values}
                self.sheet.update(range_name, values, value_input_option='RAW')
                print(f"📝 RuPaul Extractor: Updated Episode Count for row {row_number}: {episode_count}")
            
            # Update Seasons (Column H) - fix the range format
            if seasons:
                range_name = f'ViableCast!H{row_number}:H{row_number}'
                values = [[str(seasons)]]
                body = {'values': values}
                self.sheet.update(range_name, values, value_input_option='RAW')
                print(f"📝 RuPaul Extractor: Updated Seasons for row {row_number}: {seasons}")
            
            # Small delay to respect API limits
            self.smart_delay(1)
            
            return True
            
        except Exception as e:
            print(f"❌ RuPaul Extractor: Error updating row {row_number}: {str(e)}")
            return False
    
    def run_extraction(self, start_row=2598):
        """Main extraction process with comprehensive error handling and timeout prevention"""
        print(f"🚀 RuPaul Extractor: Starting extraction from row {start_row}")
        print(f"🎯 RuPaul Extractor: Target show - RuPaul's Drag Race (tt1353056)")
        
        try:
            # Setup connections
            if not self.setup_google_sheets():
                return False
            
            if not self.setup_webdriver():
                return False
            
            # Load data
            shows_analysis = self.load_viable_cast_data_from_row(start_row)
            if not shows_analysis:
                print("❌ RuPaul Extractor: No data to process")
                return False
            
            # Process shows in priority order (incomplete cast members first)
            total_processed = 0
            shows_to_process = []
            
            # Collect incomplete cast members
            for show_id, show_data in shows_analysis.items():
                if show_data['incomplete'] > 0:
                    incomplete_members = [member for member in show_data['cast_members'] if not member['complete']]
                    shows_to_process.extend(incomplete_members)
            
            print(f"📊 RuPaul Extractor: Found {len(shows_to_process)} cast members to process")
            
            # Process each cast member with timeout protection
            batch_size = 10  # Process in small batches to prevent timeouts
            for i in range(0, len(shows_to_process), batch_size):
                batch = shows_to_process[i:i + batch_size]
                print(f"\n🔄 RuPaul Extractor: Processing batch {i//batch_size + 1} ({len(batch)} members)")
                
                for cast_entry in batch:
                    try:
                        success = self.process_cast_member_with_timeout_protection(cast_entry)
                        total_processed += 1
                        
                        # Progress update
                        if total_processed % 5 == 0:
                            print(f"📈 RuPaul Extractor: Progress - Processed: {self.processed_count}, Errors: {self.error_count}, Total: {total_processed}")
                        
                        # Prevent rate limiting
                        self.smart_delay()
                        
                    except KeyboardInterrupt:
                        print("\n⏹️ RuPaul Extractor: Extraction interrupted by user")
                        break
                    except Exception as e:
                        print(f"❌ RuPaul Extractor: Unexpected error: {str(e)}")
                        self.error_count += 1
                        continue
                
                # Longer delay between batches
                if i + batch_size < len(shows_to_process):
                    print(f"⏸️ RuPaul Extractor: Batch complete, pausing before next batch...")
                    self.smart_delay(5)
            
            # Final summary
            print(f"\n🎉 RuPaul Extractor: Extraction complete!")
            print(f"✅ Successfully processed: {self.processed_count}")
            print(f"❌ Errors encountered: {self.error_count}")
            print(f"📊 Total attempted: {total_processed}")
            
            return True
            
        except KeyboardInterrupt:
            print("\n⏹️ RuPaul Extractor: Extraction interrupted by user")
            return False
        except Exception as e:
            print(f"❌ RuPaul Extractor: Fatal error: {str(e)}")
            return False
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Clean up resources"""
        print("🧹 RuPaul Extractor: Cleaning up resources...")
        if self.driver:
            try:
                self.driver.quit()
                print("✅ RuPaul Extractor: WebDriver closed")
            except:
                pass

def main():
    """Main function"""
    extractor = UniversalSeasonExtractorRuPaul()
    
    try:
        success = extractor.run_extraction(start_row=2598)
        if success:
            print("\n🎭 RuPaul Extractor: Mission accomplished! 🎉")
        else:
            print("\n❌ RuPaul Extractor: Mission failed")
            sys.exit(1)
    except KeyboardInterrupt:
        print("\n⏹️ RuPaul Extractor: Stopped by user")
    except Exception as e:
        print(f"\n💥 RuPaul Extractor: Unexpected error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
