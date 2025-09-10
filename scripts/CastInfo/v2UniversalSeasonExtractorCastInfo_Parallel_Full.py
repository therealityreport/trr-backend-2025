#!/usr/bin/env python3
# v2UniversalSeasonExtractorCastInfo_Parallel_Full.py
"""
v2 Universal Season Extractor - Full CastInfo Parallel Processing

Enhanced multi-threaded version that processes ALL shows with proper load balancing:
- Runs 5 browser instances concurrently 
- Each browser processes every 5th show (round-robin distribution)
- Thread-safe Google Sheets updates with optimized batching
- Comprehensive episode and season extraction logic
- Crew detection and removal
- Progress tracking and error handling

Key Features:
- Browser 1: rows 1, 6, 11, 16, 21...
- Browser 2: rows 2, 7, 12, 17, 22...
- Browser 3: rows 3, 8, 13, 18, 23...
- Browser 4: rows 4, 9, 14, 19, 24...
- Browser 5: rows 5, 10, 15, 20, 25...

Performance: ~5x faster with proper load distribution and no overlap.

CastInfo Sheet Structure:
A: CastName, B: TMDb CastID, C: Cast IMDbID, D: ShowName, E: Show IMDbID, F: TMDb ShowID
G: TotalEpisodes (TARGET), H: Seasons (TARGET)
"""

print("🚀 Starting Full Parallel CastInfo Season Extractor!")

import os
import sys
import time
import random
import re
import threading
import queue
import uuid
import shutil
import datetime
import traceback
import signal
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

print("🔍 Basic imports successful!")

import gspread
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, NoSuchElementException, WebDriverException
)
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains

print("🔍 All imports successful!")

# ---------- Configuration ----------
def setup_service_account():
    """Set up Google Service Account credentials for both local and cloud environments"""
    # Check if running in GitHub Codespaces
    if os.environ.get('CODESPACES'):
        print("🌩️ Running in GitHub Codespaces - setting up credentials from environment...")
        service_account_json = os.environ.get('GOOGLE_SERVICE_ACCOUNT_JSON')
        if not service_account_json:
            raise ValueError("GOOGLE_SERVICE_ACCOUNT_JSON environment variable not set in Codespaces")
        
        # Create credentials file from environment variable
        credentials_path = "/tmp/service_account.json"
        with open(credentials_path, 'w') as f:
            f.write(service_account_json)
        print(f"✅ Credentials file created at {credentials_path}")
        return credentials_path
    else:
        # Local development - use the key file
        local_path = "/Users/thomashulihan/Projects/TRR-Backend/keys/trr-backend-df2c438612e1.json"
        if os.path.exists(local_path):
            print(f"🏠 Using local credentials at {local_path}")
            return local_path
        else:
            raise FileNotFoundError(f"Service account file not found at {local_path}")

SERVICE_ACCOUNT_FILE = setup_service_account()
WORKBOOK_NAME = "Realitease2025Data"
SHEET_NAME = "CastInfo"

# Parallel processing configuration
NUM_BROWSERS = 8   # Optimized to 8 browsers for stability
BATCH_SIZE = 50    # Batch updates to Google Sheets (increased from 10)
BATCH_FLUSH_INTERVAL = 60  # Seconds between forced flushes (increased from 30)

# Fixed columns for CastInfo (G & H)
COL_G_EPISODES = 7  # TotalEpisodes
COL_H_SEASONS = 8   # Seasons

# Timeouts - Optimized for better reliability
PAGE_LOAD_TIMEOUT = 25  # Balanced timeout for initial page loads
MODAL_TIMEOUT = 8       # Reduced for faster recovery from stuck modals
REQ_TIMEOUT = 12        # Reduced overall request timeout
CAST_MEMBER_TIMEOUT = 8 # Individual cast member processing timeout

# ========== Thread-Safe Results Manager ==========
class ThreadSafeResultsManager:
    def __init__(self, sheet):
        self.sheet = sheet
        self.results_lock = threading.Lock()
        self.pending_updates = []  # (row_num, ep_count, seasons)
        self.pending_deletions = []  # row_nums to delete
        self.processed_count = 0
        self.deleted_crew = 0
        self.errors = 0
        self.last_flush = time.time()
        self.heartbeats = {}  # worker_id -> (timestamp, message)
        self.failed_cast_members = []  # Track failed cast members for investigation

    def add_result(self, row_num, ep_count, seasons):
        """Thread-safe method to add a successful result."""
        with self.results_lock:
            self.pending_updates.append((row_num, ep_count, seasons))
            self.processed_count += 1
            print(f"📝 Row {row_num}: {ep_count} episodes, seasons: {seasons}")
            print(f"📊 Pending updates: {len(self.pending_updates)}/{BATCH_SIZE}")
            
            # Auto-flush when batch size reached
            if len(self.pending_updates) >= BATCH_SIZE:
                print("🚀 Auto-flushing: Batch size reached!")
                self._flush_now()
    
    def add_update(self, update_data):
        """Thread-safe method to add update data (new format)."""
        try:
            with self.results_lock:
                for row_num, data in update_data.items():
                    ep_count = data['episodes']
                    seasons = data['seasons']
                    self.pending_updates.append((row_num, ep_count, seasons))
                    self.processed_count += 1
                    print(f"📝 Row {row_num}: {ep_count} episodes, seasons: {seasons}")
                
                print(f"📊 Pending updates: {len(self.pending_updates)}/{BATCH_SIZE}")
                
                # Auto-flush when batch size reached
                if len(self.pending_updates) >= BATCH_SIZE:
                    print("🚀 Auto-flushing: Batch size reached!")
                    self._flush_now()
        except Exception as e:
            print(f"❌ Error in add_update: {e}")
            self.errors += 1
            
    def add_crew_deletion(self, row_num):
        """Thread-safe method to record crew deletion."""
        with self.results_lock:
            self.pending_deletions.append(row_num)
            self.deleted_crew += 1
            print(f"🗑️ Marking row {row_num} for deletion (crew)")
            
    def add_error(self):
        """Thread-safe method to record an error."""
        with self.results_lock:
            self.errors += 1

    def add_failed_cast_member(self, cast_name, cast_imdb_id, show_name, reason):
        """Thread-safe method to record a failed cast member for investigation."""
        with self.results_lock:
            self.failed_cast_members.append({
                'cast_name': cast_name,
                'cast_imdb_id': cast_imdb_id,
                'show_name': show_name,
                'reason': reason,
                'timestamp': time.time()
            })

    def update_heartbeat(self, worker_id, message):
        """Record a short heartbeat message for a worker (diagnostic)."""
        try:
            with self.results_lock:
                self.heartbeats[worker_id] = (time.time(), str(message))
        except Exception:
            pass
            
    def should_flush(self):
        """Check if we should flush updates."""
        with self.results_lock:
            time_elapsed = time.time() - self.last_flush
            return (len(self.pending_updates) >= BATCH_SIZE or 
                   time_elapsed >= BATCH_FLUSH_INTERVAL or
                   len(self.pending_deletions) > 0)
    
    def _flush_now(self):
        """Internal method to flush immediately (already has lock)."""
        if not self.pending_updates and not self.pending_deletions:
            return
            
        # Process updates first
        if self.pending_updates:
            print(f"💾 Writing {len(self.pending_updates)} updates to Google Sheets...")
            
            # Batch update for efficiency
            update_data = []
            for row_num, ep_count, seasons in self.pending_updates:
                try:
                    range_name = f"G{row_num}:H{row_num}"
                    values = [[str(ep_count), str(seasons)]]
                    update_data.append({
                        'range': range_name,
                        'values': values
                    })
                except Exception as e:
                    print(f"❌ Error preparing update for row {row_num}: {e}")
                    self.errors += 1
                except Exception as e:
                    print(f"❌ Error preparing update for result {result}: {e}")
                    self.errors += 1
            
            # Execute batch update
            if update_data:
                try:
                    # Debug: Show exactly what we're writing
                    print(f"🔍 Debug - Writing to sheet: {SHEET_NAME}")
                    print(f"🔍 Debug - Spreadsheet ID: {self.sheet.spreadsheet.id}")
                    for i, item in enumerate(update_data[:3]):  # Show first 3 updates
                        print(f"   📝 Update {i+1}: {item['range']} = {item['values']}")
                    if len(update_data) > 3:
                        print(f"   📝 ... and {len(update_data) - 3} more updates")
                    
                    print(f"🔍 Debug - About to call batch_update with {len(update_data)} updates")
                    
                    # Use the correct gspread batch update method
                    result = self.sheet.batch_update(update_data)
                    
                    print(f"✅ Successfully batch updated {len(update_data)} rows")
                    print(f"🔍 Google Sheets response: {result.get('totalUpdatedCells', 'unknown')} cells updated")
                            
                except Exception as e:
                    print(f"❌ Batch update failed: {e}")
                    print(f"🔍 Update data sample: {update_data[:1] if update_data else 'No data'}")
                    # Fallback to individual updates using correct syntax
                    for item in update_data:
                        try:
                            range_name = item['range']
                            values = item['values']
                            self.sheet.update(values=values, range_name=range_name)
                            time.sleep(0.1)  # Rate limiting
                        except Exception as e2:
                            print(f"❌ Individual update failed for {range_name}: {e2}")
                            self.errors += 1
            
            self.pending_updates.clear()
        
        # Process deletions (handle separately to avoid row number issues)
        if self.pending_deletions:
            print(f"🗑️ Processing {len(self.pending_deletions)} crew deletions...")
            # Sort in reverse order to delete from bottom up (preserves row numbers)
            sorted_deletions = sorted(self.pending_deletions, reverse=True)
            
            for row_num in sorted_deletions:
                try:
                    self.sheet.delete_rows(row_num)
                    print(f"🗑️ Deleted row {row_num} (crew member)")
                    time.sleep(0.1)  # Rate limiting for deletions
                except Exception as e:
                    print(f"❌ Failed deleting row {row_num}: {e}")
                    self.errors += 1
            
            self.pending_deletions.clear()
            
        self.last_flush = time.time()
            
    def flush_updates(self, force=False):
        """Thread-safe method to write pending updates to Google Sheets."""
        with self.results_lock:
            if not force and not self.should_flush():
                return
                
            self._flush_now()
            
    def get_stats(self):
        """Get current processing statistics."""
        with self.results_lock:
            return {
                'processed': self.processed_count,
                'deleted_crew': self.deleted_crew, 
                'errors': self.errors,
                'pending_updates': len(self.pending_updates),
                'pending_deletions': len(self.pending_deletions),
                'failed_cast_members': len(self.failed_cast_members)
            }

    def get_heartbeats(self):
        """Return a copy of current heartbeats."""
        with self.results_lock:
            return dict(self.heartbeats)

    def save_failed_cast_members_log(self):
        """Save failed cast members to a log file for investigation."""
        with self.results_lock:
            if not self.failed_cast_members:
                return
                
            import json
            import datetime
            
            timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"failed_cast_members_{timestamp}.json"
            
            try:
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(self.failed_cast_members, f, indent=2, ensure_ascii=False)
                print(f"📝 Saved {len(self.failed_cast_members)} failed cast members to {filename}")
            except Exception as e:
                print(f"❌ Failed to save failed cast members log: {e}")

    def get_failed_cast_members(self):
        """Return a copy of failed cast members list."""
        with self.results_lock:
            return list(self.failed_cast_members)

# ========== Enhanced Worker Thread Class ==========
class CastInfoWorker:
    def __init__(self, worker_id, results_manager):
        self.worker_id = worker_id
        self.results_manager = results_manager
        self.driver = None
        
    def setup_webdriver(self):
        """Setup Chrome WebDriver with unique user data directory for this worker."""
        import os
        import time
        import uuid
        
        try:
            chrome_options = Options()
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            chrome_options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36")
            
            # Create unique user data directory with timestamp and UUID
            timestamp = int(time.time() * 1000)
            unique_id = str(uuid.uuid4())[:8]
            user_data_dir = f"/tmp/chrome_worker_{self.worker_id}_{timestamp}_{unique_id}"
            
            # Ensure the directory doesn't exist and create it
            if os.path.exists(user_data_dir):
                import shutil
                shutil.rmtree(user_data_dir)
            os.makedirs(user_data_dir, exist_ok=True)
            
            chrome_options.add_argument(f"--user-data-dir={user_data_dir}")
            
            # Additional isolation arguments
            chrome_options.add_argument(f"--remote-debugging-port={9222 + self.worker_id}")
            chrome_options.add_argument("--disable-web-security")
            chrome_options.add_argument("--disable-features=VizDisplayCompositor")
            
            # Performance optimizations
            chrome_options.add_argument("--disable-extensions")
            chrome_options.add_argument("--disable-plugins")
            chrome_options.add_argument("--disable-images")
            chrome_options.add_argument("--disable-background-timer-throttling")
            chrome_options.add_argument("--disable-backgrounding-occluded-windows")
            chrome_options.add_argument("--disable-renderer-backgrounding")
            
            # Enable headless mode for cloud deployment
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--disable-software-rasterizer")
            
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            # Store the data directory for cleanup later
            self.user_data_dir = user_data_dir
            
            print(f"✅ Worker {self.worker_id}: WebDriver ready with data dir: {user_data_dir}")
            return True
        except Exception as e:
            print(f"❌ Worker {self.worker_id}: WebDriver setup failed: {e}")
            return False

    def smart_delay(self, base=0.3, jitter=0.2):
        """Smart delay with randomization."""
        time.sleep(base + random.uniform(0, jitter))

    def is_browser_responsive(self):
        """Check if browser is still responsive."""
        try:
            # Simple test to see if browser responds
            self.driver.current_url
            self.driver.execute_script("return document.readyState")
            return True
        except Exception:
            return False
    
    def restart_browser(self):
        """Restart the browser when it crashes."""
        print(f"🔄 Worker {self.worker_id}: Restarting crashed browser...")
        try:
            if self.driver:
                self.driver.quit()
        except Exception:
            pass
        
        # Wait a moment before restart
        self.smart_delay(1.0, 0.5)
        
        # Setup new browser
        if self.setup_webdriver():
            print(f"✅ Worker {self.worker_id}: Browser restarted successfully")
            return True
        else:
            print(f"❌ Worker {self.worker_id}: Browser restart failed")
            return False

    def click(self, element):
        """Enhanced click method with stale element recovery."""
        methods = [
            lambda: element.click(),
            lambda: self.driver.execute_script("arguments[0].click();", element),
            lambda: ActionChains(self.driver).move_to_element(element).click().perform(),
            lambda: ActionChains(self.driver).click(element).perform()
        ]
        
        for i, method in enumerate(methods):
            try:
                method()
                return True
            except Exception as e:
                error_msg = str(e).lower()
                if "stale element" in error_msg:
                    print(f"  ⚠️ Stale element detected on attempt {i+1}, element needs to be re-found")
                    return "stale_element"  # Signal that element needs to be re-found
                elif i < len(methods) - 1:
                    self.smart_delay(0.2, 0.2)
                else:
                    print(f"  ⚠️ All click methods failed: {e}")
        return False

    def open_full_credits(self, show_imdbid):
        """Open IMDb fullcredits page with better error handling."""
        url = f"https://www.imdb.com/title/{show_imdbid}/fullcredits"
        for attempt in range(2):  # Reduced retries from 3 to 2
            try:
                print(f"  🌐 Worker {self.worker_id}: Loading {url}")
                self.driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
                self.driver.get(url)
                
                # Quick check if page loaded
                WebDriverWait(self.driver, 10).until(  # Increased from 6 - give pages more time
                    lambda d: d.execute_script("return document.readyState") == "complete"
                )
                self.smart_delay(0.2, 0.1)  # Quick delay
                return True
            except TimeoutException:
                print(f"  ⚠️ Worker {self.worker_id}: Timeout on attempt {attempt + 1}")
                if attempt < 1:  # Only retry once
                    try:
                        self.driver.execute_script("window.stop();")  # Stop loading
                        self.smart_delay(0.3, 0.2)
                    except Exception:
                        pass
            except Exception as e:
                print(f"  ⚠️ Worker {self.worker_id}: Load attempt {attempt + 1} failed: {e}")
                if attempt < 1:
                    self.smart_delay(0.5, 0.3)
        print(f"❌ Worker {self.worker_id}: Could not load {url}")
        return False

    def find_cast_anchor(self, cast_imdbid, cast_name):
        """Find cast member link by IMDb ID or name with enhanced search and retry logic."""
        max_retries = 2
        for attempt in range(max_retries):
            try:
                # First try by IMDb ID
                if cast_imdbid and cast_imdbid.startswith('nm'):
                    selectors = [
                        f"//a[contains(@href, '/name/{cast_imdbid}/')]",
                        f"//a[contains(@href, '{cast_imdbid}')]"
                    ]
                    for selector in selectors:
                        try:
                            element = self.driver.find_element(By.XPATH, selector)
                            print(f"  ✅ Found {cast_name} by IMDb ID: {cast_imdbid}")
                            return element
                        except NoSuchElementException:
                            continue

                # Then try by name with variations
                if cast_name:
                    name_variants = [
                        cast_name,
                        cast_name.replace("'", "'"),
                        cast_name.replace("'", "'"),
                        cast_name.replace('"', '"'),
                        cast_name.replace('"', '"')
                    ]
                    
                    for name in name_variants:
                        selectors = [
                            f"//a[contains(@href,'/name/') and normalize-space(text())='{name}']",
                            f"//a[contains(@href,'/name/') and contains(text(), '{name}')]",
                            f"//a[text()='{name}']"
                        ]
                        for selector in selectors:
                            try:
                                element = self.driver.find_element(By.XPATH, selector)
                                print(f"  ✅ Found {cast_name} by name match")
                                return element
                            except NoSuchElementException:
                                continue
                
                # If not found on first attempt, try refreshing elements
                if attempt == 0:
                    print(f"  🔄 Retrying search for {cast_name}...")
                    self.smart_delay(0.5, 0.2)
                    continue
                    
            except Exception as e:
                print(f"  ⚠️ Search attempt {attempt + 1} failed for {cast_name}: {e}")
                if attempt < max_retries - 1:
                    self.smart_delay(0.5, 0.2)
                    continue
        
        print(f"  ⚠️ Could not find {cast_name} (ID: {cast_imdbid})")
        # Log failed cast member for investigation
        self.results_manager.add_failed_cast_member(
            cast_name, cast_imdbid, "", f"Cast member not found on page after {max_retries} attempts"
        )
        return None

    def find_episodes_button_near(self, cast_anchor):
        """Find episodes button near cast member with enhanced search and stale element handling."""
        max_retries = 2
        for attempt in range(max_retries):
            try:
                # Try different container types
                container_xpaths = [
                    "./ancestor::li[1]",
                    "./ancestor::tr[1]", 
                    "./ancestor::div[contains(@class, 'cast_list')][1]",
                    "./parent::*[1]",
                    "./ancestor::*[contains(@class, 'cast')][1]"
                ]
                
                for xpath in container_xpaths:
                    try:
                        container = cast_anchor.find_element(By.XPATH, xpath)
                        
                        # Look for episodes button/link in container
                        episode_selectors = [
                            ".//*[self::button or self::a or self::span][contains(translate(text(), 'EPISODE', 'episode'),'episode')]",
                            ".//*[contains(@class, 'episode')]",
                            ".//*[contains(text(), 'episode')]",
                            ".//button[contains(text(), 'episodes')]",
                            ".//a[contains(text(), 'episodes')]"
                        ]
                        
                        for selector in episode_selectors:
                            try:
                                buttons = container.find_elements(By.XPATH, selector)
                                if buttons:
                                    print(f"  ✅ Found episodes button using xpath: {xpath}")
                                    return buttons[0]
                            except Exception:
                                continue
                                
                    except Exception:
                        continue
                
                # If not found on first attempt, wait and retry
                if attempt == 0:
                    print(f"  🔄 Retrying episodes button search...")
                    self.smart_delay(0.5, 0.2)
                    continue
                    
            except Exception as e:
                error_msg = str(e).lower()
                if "stale element" in error_msg and attempt == 0:
                    print(f"  ⚠️ Stale element in episodes button search, retrying...")
                    self.smart_delay(0.5, 0.2)
                    continue
                    
        print(f"  ⚠️ No episodes button found near cast member after {max_retries} attempts")
        return None

    def extract_episode_count_from_modal(self):
        """Extract episode count from modal header."""
        # Compact extraction to avoid iterating huge numbers of empty nodes
        MAX_INSPECT = 8
        print("  🔍 Extracting episode count from modal (limited inspect)...")

        # Wait briefly for modal header
        try:
            WebDriverWait(self.driver, 6).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".ipc-prompt-header"))
            )
            self.smart_delay(0.6, 0.3)
        except TimeoutException:
            print("  ⚠️ Timeout waiting for popup header")
            return None

        start = time.time()

        selectors = [
            "li.ipc-inline-list__item",
            ".ipc-prompt-header__subtitle li.ipc-inline-list__item",
            ".ipc-prompt-header__text li.ipc-inline-list__item",
            "ul.ipc-inline-list li.ipc-inline-list__item",
            ".ipc-prompt-header ul li",
            ".ipc-prompt-header li",
            ".ipc-prompt-header",
        ]

        for sel in selectors:
            # Protect against spending too long inside modal parsing
            if time.time() - start > MODAL_TIMEOUT:
                print("  ⚠️ Modal extraction timed out")
                return None
            try:
                items = self.driver.find_elements(By.CSS_SELECTOR, sel)
                if not items:
                    continue
                # Inspect only the first few items to avoid massive loops and logging
                for it in items[:MAX_INSPECT]:
                    try:
                        text = (it.text or "").strip()
                    except Exception:
                        text = ""

                    if not text:
                        # try innerText as fallback for empty text nodes
                        try:
                            text = (it.get_attribute('innerText') or '').strip()
                        except Exception:
                            text = ""

                    if not text:
                        continue

                    m = re.search(r"(\d+)\s+episodes?", text, re.I)
                    if m:
                        ep_count = int(m.group(1))
                        print(f"  📊 Found {ep_count} episodes in popup using selector '{sel}'")
                        return ep_count
                # small pause between selector attempts
                self.smart_delay(0.05, 0.05)
            except Exception:
                continue

        # Fallback: limited count of episode links
        try:
            episode_links = self.driver.find_elements(By.CSS_SELECTOR, 'a.episodic-credits-bottomsheet__menu-item')
            if episode_links:
                ep_count = len(episode_links)
                print(f"  📊 Counted {ep_count} episode links in modal")
                return ep_count
        except Exception:
            pass

        # Last resort: try header text blob
        try:
            header = self.driver.find_element(By.CSS_SELECTOR, '.ipc-prompt-header')
            blob = (header.text or "").strip()
            m = re.search(r"(\d+)\s+episodes?", blob, re.I)
            if m:
                ep_count = int(m.group(1))
                print(f"  📊 Found {ep_count} episodes in popup header text")
                return ep_count
        except Exception:
            pass

        return None

    def extract_seasons_from_modal(self):
        """Extract seasons from modal."""
        print("  🔍 Extracting seasons from modal...")
        
        # SEASONS from TABS (primary method)
        seasons = self.parse_seasons_from_tabs()

        # If no season tabs found, try year-based banner approach
        if not seasons:
            print("  🔄 No season tabs found, trying year banner...")
            seasons = self.parse_seasons_from_year_banner()

        # If still no seasons, try reading them from any episode marker in the modal
        if not seasons:
            print("  🔄 No year banner, trying episode markers...")
            s = self.parse_season_from_any_episode_marker()
            if s:
                seasons = s

        return seasons if seasons else "1"  # Default to season 1 if can't extract

    def parse_seasons_from_tabs(self):
        """Read seasons from the pop-up's season tabs."""
        selectors = [
            'li[data-testid^="season-tab-"]',
            'li.ipc-tab[data-testid^="season-tab-"]',
            'li[role="tab"][data-testid^="season-tab-"]',
            'ul[role="tablist"] li[data-testid^="season-tab-"]',
        ]
        seasons = set()
        for sel in selectors:
            try:
                tabs = self.driver.find_elements(By.CSS_SELECTOR, sel)
                for t in tabs:
                    dt = t.get_attribute("data-testid") or ""
                    m = re.search(r"season-tab-(\d+)", dt)
                    if m:
                        seasons.add(int(m.group(1)))
                    else:
                        # fallback to inner text
                        try:
                            txt = t.text.strip()
                            if txt.isdigit():
                                seasons.add(int(txt))
                        except Exception:
                            pass
            except Exception:
                continue
        if seasons:
            return ", ".join(str(s) for s in sorted(seasons))
        return None

    def parse_seasons_from_year_banner(self):
        """Handle year-based banners by clicking each year tab and extracting season from first episode only."""
        try:
            year_tabs = self.driver.find_elements(By.CSS_SELECTOR, 'li[role="tab"]')
            year_seasons = set()
            
            # Limit processing time for year banners to prevent stalls
            start_time = time.time()
            MAX_YEAR_PROCESSING = 8  # Reduced to 8 seconds max for all year tabs
            
            for tab in year_tabs:
                # Check timeout
                if time.time() - start_time > MAX_YEAR_PROCESSING:
                    print(f"  ⏰ Year banner timeout, using collected seasons so far")
                    break
                    
                try:
                    tab_text = tab.text.strip()
                    if re.match(r'^\d{4}$', tab_text) or re.match(r'^(19|20)\d{2}$', tab_text):
                        print(f"    🗓️ Checking year tab: {tab_text}")
                        self.click(tab)
                        self.smart_delay(0.5, 0.2)  # Reduced delay
                        
                        # Just look at the FIRST episode to determine the season for this year
                        episode_selectors = [
                            'a.episodic-credits-bottomsheet__menu-item',
                            'a[role="menuitem"]',
                            '[data-testid*="episode"]',
                            'li.ipc-inline-list__item',
                            '[class*="episode"]'
                        ]
                        
                        found_season_this_year = None
                        for selector in episode_selectors:
                            try:
                                episode_elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                                if episode_elements:
                                    # Only check the FIRST episode
                                    first_elem = episode_elements[0]
                                    elem_text = first_elem.text.strip()
                                    if not elem_text:
                                        elem_text = first_elem.get_attribute('innerText') or ''
                                    
                                    season_patterns = [
                                        r'S(\d+)\.E\d+',
                                        r'S(\d+)E\d+',
                                        r'S(\d+)\s*·\s*E\d+',
                                        r'Season\s+(\d+)',
                                        r'S(\d+)',
                                        r'(\d+)x\d+',
                                    ]
                                    
                                    for pattern in season_patterns:
                                        season_match = re.search(pattern, elem_text, re.I)
                                        if season_match:
                                            season_num = int(season_match.group(1))
                                            found_season_this_year = season_num
                                            print(f"      ✅ Year {tab_text} = Season {season_num} (from first episode: {elem_text[:40]}...)")
                                            break
                                    
                                    if found_season_this_year:
                                        break
                            except Exception:
                                continue
                            
                            if found_season_this_year:
                                break
                        
                        if found_season_this_year:
                            year_seasons.add(found_season_this_year)
                        
                except Exception as e:
                    print(f"    ⚠️ Error processing year tab: {e}")
                    continue
            
            if year_seasons:
                result = ", ".join(str(s) for s in sorted(year_seasons))
                print(f"  📅 All seasons from year banners: {result}")
                return result
        except Exception as e:
            print(f"  ⚠️ Error parsing year banner: {e}")
        return None

    def parse_season_from_any_episode_marker(self):
        """Look anywhere inside the open IMDb episodes modal for season markers."""
        roots = []
        try:
            roots.append(self.driver.find_element(By.CSS_SELECTOR, '[role="dialog"]'))
        except Exception:
            pass
        try:
            roots.append(self.driver.find_element(By.CSS_SELECTOR, '.ipc-promptable-base__content'))
        except Exception:
            pass
        try:
            roots.append(self.driver.find_element(By.CSS_SELECTOR, '.ipc-prompt'))
        except Exception:
            pass
        if not roots:
            roots = [self.driver]

        selectors = [
            "a.episodic-credits-bottomsheet__menu-item",
            "a[role='menuitem']",
            "[data-testid*='episode']",
            "li.ipc-inline-list__item",
            "ul.ipc-inline-list li",
            "[class*='episode']",
            "[class*='credit']",
            ".ipc-metadata-list-summary-item",
        ]
        
        patterns = [
            r"S\s*(\d+)\s*[\.|·\-E]\s*\d+",
            r"Season\s*(\d+)(?!\s*Tab)",
            r"(\d+)x\d+",
            r"S(\d+)",
            r"Ep\s*\d+\s*S(\d+)",
            r"(?:Episode\s*\d+.*?)?S(\d+)",
        ]

        # Limit the number of elements inspected to avoid long processing times
        MAX_PER_SELECTOR = 10
        for root in roots:
            for sel in selectors:
                try:
                    els = root.find_elements(By.CSS_SELECTOR, sel)
                    if not els:
                        continue
                    for el in els[:MAX_PER_SELECTOR]:
                        txt = (el.text or "").strip()
                        if not txt:
                            try:
                                txt = (el.get_attribute('innerText') or '').strip()
                            except Exception:
                                txt = ''
                        if not txt:
                            try:
                                raw = (el.get_attribute('innerHTML') or '').strip()
                                txt = re.sub(r'<[^>]+>', ' ', raw)
                            except Exception:
                                txt = ''

                        if not txt:
                            continue

                        for pat in patterns:
                            m = re.search(pat, txt, re.I)
                            if m:
                                try:
                                    season_num = int(m.group(1))
                                    print(f"  📌 Found season {season_num} from episode marker")
                                    return str(season_num)
                                except Exception:
                                    continue
                except Exception:
                    continue

        # Try examining the entire modal text as a last resort
        for root in roots:
            try:
                blob = (root.text or "").strip()
                if blob:
                    for pat in patterns:
                        m = re.search(pat, blob, re.I)
                        if m:
                            season_num = int(m.group(1))
                            print(f"  🎯 Season {season_num} found from modal text using pattern '{pat}'")
                            return str(season_num)
            except Exception:
                continue
        return None

    def close_modal(self):
        """Close any open modal."""
        try:
            # Try the specific close button first (most reliable)
            close_selectors = [
                'button[aria-label="Close Prompt"][title="Close Prompt"]',  # Your specific button
                'button.ipc-icon-button[aria-label="Close Prompt"]',
                '[data-testid="promptable__x"] button',
                '.ipc-promptable-base__close button',
                '[aria-label="Close"]',
                '.ipc-prompt button[aria-label="Close"]'
            ]
            
            for selector in close_selectors:
                try:
                    close_btn = WebDriverWait(self.driver, 2).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                    )
                    self.click(close_btn)
                    self.smart_delay(0.2, 0.1)
                    print(f"  ✅ Modal closed using selector: {selector}")
                    return True
                except Exception:
                    continue
            
            # Fallback: ESC key
            ActionChains(self.driver).send_keys(Keys.ESCAPE).perform()
            self.smart_delay(0.2, 0.1)
            print("  ✅ Modal closed using ESC key")
            return True
            
        except Exception as e:
            print(f"  ⚠️ Could not close modal: {e}")
            return False

    def detect_crew_member(self, cast_imdbid, cast_name):
        """Enhanced crew detection."""
        try:
            crew_sections = [
                'Directed by', 'Produced by', 'Writing Credits', 'Music by',
                'Cinematography by', 'Film Editing by', 'Production Design by',
                'Art Direction by', 'Set Decoration by', 'Costume Design by',
                'Makeup Department', 'Production Management', 'Sound Department',
                'Special Effects', 'Visual Effects', 'Stunts', 'Camera and Electrical',
                'Editorial Department', 'Location Management', 'Music Department',
                'Transportation Department', 'Additional Crew'
            ]
            
            # Check by IMDb ID first
            if cast_imdbid:
                for section in crew_sections:
                    xpath = f"//h4[contains(text(), '{section}')]/following-sibling::*//a[contains(@href, '{cast_imdbid}')]"
                    try:
                        crew_links = self.driver.find_elements(By.XPATH, xpath)
                        if crew_links:
                            print(f"  🎬 CREW detected by ID in {section}: {cast_name}")
                            return True
                    except Exception:
                        continue
            
            # Check by name
            if cast_name:
                safe_name = cast_name.replace("'", "\\'")
                for section in crew_sections:
                    xpath = f"//h4[contains(text(), '{section}')]/following-sibling::*//a[contains(text(), '{safe_name}')]"
                    try:
                        crew_links = self.driver.find_elements(By.XPATH, xpath)
                        if crew_links:
                            print(f"  🎬 CREW detected by name in {section}: {cast_name}")
                            return True
                    except Exception:
                        continue
            
            return False
            
        except Exception as e:
            print(f"  ⚠️ Error in crew detection: {e}")
            return False

    def process_cast_member_on_loaded_page(self, cast_data):
        """Process a single cast member on an already-loaded show page with improved error handling."""
        row_num = cast_data["row_num"]
        cast_name = cast_data["cast_name"]
        cast_imdb_id = cast_data["cast_imdb_id"]
        show_name = cast_data["show_name"]
        
        # Timeout using the new CAST_MEMBER_TIMEOUT configuration
        start_time = time.time()
        
        try:
            # Check if browser is responsive
            if not self.is_browser_responsive():
                print(f"💥 Worker {self.worker_id}: Browser unresponsive for {cast_name}")
                self.results_manager.add_failed_cast_member(
                    cast_name, cast_imdb_id, show_name, "Browser unresponsive"
                )
                self.results_manager.add_error()
                return False
                
            # Check if crew member (page already loaded)
            if self.detect_crew_member(cast_imdb_id, cast_name):
                self.results_manager.add_crew_deletion(row_num)
                return True
                
            # Find cast member on current page with retry logic
            anchor = self.find_cast_anchor(cast_imdb_id, cast_name)
            if not anchor:
                print(f"  ⚠️ Could not locate {cast_name}")
                self.results_manager.add_failed_cast_member(
                    cast_name, cast_imdb_id, show_name, "Cast member not found on page"
                )
                return False
                
            # Find episodes button with retry logic
            episodes_button = self.find_episodes_button_near(anchor)
            if not episodes_button:
                print(f"  ⚠️ No episodes button for {cast_name}")
                self.results_manager.add_failed_cast_member(
                    cast_name, cast_imdb_id, show_name, "Episodes button not found"
                )
                return False
                
            # Scroll and click episodes button with stale element handling
            try:
                self.driver.execute_script("arguments[0].scrollIntoView({block:'center'});", episodes_button)
                self.smart_delay(0.1, 0.1)
                
                click_result = self.click(episodes_button)
                if click_result == "stale_element":
                    # Re-find the cast member and episodes button
                    print(f"  🔄 Re-finding elements for {cast_name} due to stale element")
                    anchor = self.find_cast_anchor(cast_imdb_id, cast_name)
                    if anchor:
                        episodes_button = self.find_episodes_button_near(anchor)
                        if episodes_button:
                            click_result = self.click(episodes_button)
                
                if not click_result or click_result == "stale_element":
                    print(f"  ⚠️ Could not click episodes button for {cast_name}")
                    self.results_manager.add_failed_cast_member(
                        cast_name, cast_imdb_id, show_name, "Could not click episodes button"
                    )
                    return False
                
                self.smart_delay(0.3, 0.2)  # Wait for modal
                self.results_manager.update_heartbeat(self.worker_id, f"clicked_episodes:{cast_name}")
            except Exception as e:
                print(f"  ⚠️ Error clicking episodes button: {e}")
                self.results_manager.add_failed_cast_member(
                    cast_name, cast_imdb_id, show_name, f"Click error: {str(e)}"
                )
                return False
            
            # Extract episode count and seasons with timeout protection
            try:
                # Check timeout before modal extraction
                if time.time() - start_time > CAST_MEMBER_TIMEOUT:
                    print(f"  ⏰ TIMEOUT: Skipping {cast_name} after {CAST_MEMBER_TIMEOUT}s")
                    self.close_modal()
                    self.results_manager.add_failed_cast_member(
                        cast_name, cast_imdb_id, show_name, f"Processing timeout ({CAST_MEMBER_TIMEOUT}s)"
                    )
                    return False
                    
                self.results_manager.update_heartbeat(self.worker_id, f"extracting_data:{cast_name}")
                episode_count = self.extract_episode_count_from_modal()
                
                # Check timeout after episode extraction
                if time.time() - start_time > CAST_MEMBER_TIMEOUT:
                    print(f"  ⏰ TIMEOUT: Skipping {cast_name} during season extraction")
                    self.close_modal()
                    self.results_manager.add_failed_cast_member(
                        cast_name, cast_imdb_id, show_name, f"Season extraction timeout ({CAST_MEMBER_TIMEOUT}s)"
                    )
                    return False
                    
                self.results_manager.update_heartbeat(self.worker_id, f"got_episode_count:{cast_name}:{episode_count}")
                seasons = self.extract_seasons_from_modal()
                self.results_manager.update_heartbeat(self.worker_id, f"got_seasons:{cast_name}:{seasons}")
            except Exception as e:
                print(f"  ⚠️ Error extracting data from modal: {e}")
                self.results_manager.add_failed_cast_member(
                    cast_name, cast_imdb_id, show_name, f"Modal extraction error: {str(e)}"
                )
                self.close_modal()
                return False
            
            # Close modal
            print(f"  🔒 Closing modal for {cast_name}...")
            self.close_modal()
            self.smart_delay(0.1, 0.1)
            
            if episode_count and seasons:
                print(f"  📋 Queuing for batch: {cast_name} - {episode_count} episodes, {seasons} seasons")
                # Save to Google Sheets
                update_data = {
                    row_num: {
                        'episodes': str(episode_count),
                        'seasons': str(seasons)
                    }
                }
                self.results_manager.add_update(update_data)
                print(f"  ✅ {cast_name}: Successfully queued - {episode_count} episodes, seasons: {seasons}")
                return True
            else:
                print(f"  ⚠️ Could not extract episode data for {cast_name}")
                self.results_manager.add_failed_cast_member(
                    cast_name, cast_imdb_id, show_name, f"No episode/season data extracted (episodes: {episode_count}, seasons: {seasons})"
                )
                return False
                
        except Exception as e:
            print(f"  ⚠️ Unexpected error processing {cast_name}: {e}")
            self.results_manager.add_failed_cast_member(
                cast_name, cast_imdb_id, show_name, f"Unexpected error: {str(e)}"
            )
            return False

    def is_browser_responsive(self):
        """Quick check if browser is still responsive."""
        try:
            self.driver.current_url
            return True
        except Exception:
            return False

    def process_cast_member(self, cast_data):
        """Process a single cast member with comprehensive extraction and crash recovery (loads page each time)."""
        row_num = cast_data["row_num"]
        cast_name = cast_data["cast_name"]
        cast_imdb_id = cast_data["cast_imdb_id"]
        show_name = cast_data["show_name"]
        show_imdb_id = cast_data["show_imdb_id"]
        
        print(f"🎭 Worker {self.worker_id}: Row {row_num} | {cast_name} | {show_name}")
        
        # Check if browser is responsive, restart if needed
        if not self.is_browser_responsive():
            print(f"💥 Worker {self.worker_id}: Browser unresponsive, restarting...")
            if not self.restart_browser():
                print(f"❌ Worker {self.worker_id}: Could not restart browser, skipping {cast_name}")
                self.results_manager.add_error()
                return False
        
        try:
            # Load IMDb page with timeout protection
            if not self.open_full_credits(show_imdb_id):
                print(f"  ❌ Failed to load page for {cast_name}")
                return False
                
            # Use the optimized processing method
            return self.process_cast_member_on_loaded_page(cast_data)
                
        except WebDriverException as e:
            print(f"  💥 Browser crash detected for {cast_name}: {e}")
            self.results_manager.add_error()
            # Try to restart browser for next request
            if not self.restart_browser():
                print(f"❌ Worker {self.worker_id}: Browser restart failed, worker may be unusable")
            return False
        except Exception as e:
            print(f"  ❌ Error processing {cast_name}: {e}")
            self.results_manager.add_error()
            # Try to recover by refreshing the page
            try:
                if self.is_browser_responsive():
                    self.driver.refresh()
                    self.smart_delay(1.0, 0.5)
                else:
                    self.restart_browser()
            except Exception:
                pass
            return False
        finally:
            # Small delay between requests
            self.smart_delay(0.1, 0.1)

    def process_assigned_rows(self, all_cast_members):
        """Process rows assigned to this worker (every 5th row)."""
        if not self.setup_webdriver():
            return
            
        try:
            # Filter to only this worker's assigned rows
            assigned_rows = []
            for i, cast_data in enumerate(all_cast_members):
                # Worker 1 gets indices 0,5,10... (rows 1,6,11...)
                # Worker 2 gets indices 1,6,11... (rows 2,7,12...)
                # etc.
                if i % NUM_BROWSERS == (self.worker_id - 1):
                    assigned_rows.append(cast_data)
            
            print(f"🎯 Worker {self.worker_id}: Processing {len(assigned_rows)} rows (every {NUM_BROWSERS}th starting from row {self.worker_id})")
            
            processed = 0
            for cast_data in assigned_rows:
                if self.process_cast_member(cast_data):
                    processed += 1
                
                # Periodic flush check
                if processed % 10 == 0:
                    self.results_manager.flush_updates()
                    
                self.smart_delay(0.2, 0.2)  # Pacing between requests
            
            print(f"✅ Worker {self.worker_id}: Completed {processed}/{len(assigned_rows)} rows")
                
        finally:
            if self.driver:
                self.driver.quit()
                print(f"🔒 Worker {self.worker_id}: Browser closed")

    def process_assigned_shows(self, assigned_shows):
        """Process shows assigned to this worker - load each show page once and process all cast members."""
        if not self.setup_webdriver():
            return
            
        try:
            total_cast_members = sum(len(cast_list) for _, cast_list in assigned_shows)
            show_names = [show_name for show_name, _ in assigned_shows[:3]]  # Show first 3 shows
            more_text = f" + {len(assigned_shows) - 3} more" if len(assigned_shows) > 3 else ""
            
            print(f"🎯 Worker {self.worker_id}: Processing {len(assigned_shows)} shows, {total_cast_members} cast members")
            print(f"   📺 Shows: {', '.join(show_names)}{more_text}")
            
            processed = 0
            for show_name, cast_list in assigned_shows:
                print(f"🔄 Worker {self.worker_id}: Starting show '{show_name}' ({len(cast_list)} cast members)")
                
                # Load the show page ONCE for all cast members
                first_cast = cast_list[0]
                show_imdb_id = first_cast["show_imdb_id"]
                
                if not self.open_full_credits(show_imdb_id):
                    print(f"❌ Worker {self.worker_id}: Could not load show page for '{show_name}', skipping all cast")
                    for cast_data in cast_list:
                        self.results_manager.add_error()
                    continue
                
                print(f"✅ Worker {self.worker_id}: Show page loaded, processing {len(cast_list)} cast members...")
                
                # Process all cast members from this show without reloading the page
                try:
                    for i, cast_data in enumerate(cast_list):
                        cast_name = cast_data['cast_name']
                        print(f"  🎭 Processing {i+1}/{len(cast_list)}: {cast_name}")
                        
                        try:
                            start_time = time.time()
                            
                            # Use a threading-based timeout instead of signal
                            import threading
                            
                            # Result container
                            result_container = {'success': None, 'timed_out': False}
                            
                            def process_with_result():
                                try:
                                    result_container['success'] = self.process_cast_member_on_loaded_page(cast_data)
                                except Exception as e:
                                    result_container['error'] = e
                                    result_container['success'] = False
                            
                            # Create and start the processing thread
                            process_thread = threading.Thread(target=process_with_result)
                            process_thread.daemon = True
                            process_thread.start()
                            
                            # Wait for thread with timeout
                            process_thread.join(timeout=CAST_MEMBER_TIMEOUT)  # Use configurable timeout
                            
                            if process_thread.is_alive():
                                # Thread is still running - timeout occurred
                                result_container['timed_out'] = True
                                elapsed = time.time() - start_time
                                print(f"    ⏰ TIMEOUT: Skipping {cast_name} after {elapsed:.1f}s")
                                
                                # Log the timeout for investigation
                                self.results_manager.add_failed_cast_member(
                                    cast_name, cast_data.get('cast_imdb_id', ''), show_name, 
                                    f"Threading timeout after {elapsed:.1f}s"
                                )
                                
                                # Try to force cleanup
                                try:
                                    self.driver.execute_script("document.querySelectorAll('button[aria-label*=\"Close\"]').forEach(btn => btn.click());")
                                    ActionChains(self.driver).send_keys(Keys.ESCAPE).perform()
                                    self.driver.execute_script("window.stop();")
                                except Exception:
                                    pass
                                    
                                self.smart_delay(0.5, 0.2)
                                continue
                                
                            # Check if an error occurred
                            if 'error' in result_container:
                                raise result_container['error']
                                
                            # Process completed within timeout
                            success = result_container['success']
                            elapsed = time.time() - start_time
                            
                            if success:
                                processed += 1
                                print(f"    ✅ Completed in {elapsed:.1f}s")
                            else:
                                print(f"    ❌ Failed in {elapsed:.1f}s")
                                    
                        except Exception as e:
                            elapsed = time.time() - start_time
                            print(f"    ❌ CAST MEMBER ERROR: {cast_name}: {type(e).__name__}: {e} in {elapsed:.1f}s")
                            import traceback
                            print(f"    📝 CAST MEMBER TRACEBACK: {traceback.format_exc()}")
                            # Don't let errors kill the worker - continue to next cast member
                            try:
                                self.close_modal()
                                self.driver.execute_script("window.stop();")
                            except Exception:
                                pass
                            continue
                            
                            # Periodic flush check
                            if processed % 10 == 0:
                                self.results_manager.flush_updates()
                                
                            self.smart_delay(0.1, 0.1)  # Small delay between cast members on same page
                        
                        except Exception as loop_error:
                            print(f"    ❌ LOOP ERROR for {cast_name}: {type(loop_error).__name__}: {loop_error}")
                            import traceback
                            print(f"    📝 LOOP TRACEBACK: {traceback.format_exc()}")
                            continue
                
                except Exception as cast_loop_error:
                    print(f"❌ CAST PROCESSING LOOP ERROR for show {show_name}: {type(cast_loop_error).__name__}: {cast_loop_error}")
                    import traceback 
                    print(f"📝 CAST LOOP TRACEBACK: {traceback.format_exc()}")
                    # Don't exit worker - continue to next show
                
                print(f"✅ Worker {self.worker_id}: Completed show '{show_name}' - {len(cast_list)} cast members processed")
                self.smart_delay(0.3, 0.2)  # Slightly longer delay between shows
            
            print(f"✅ Worker {self.worker_id}: Completed {processed}/{total_cast_members} cast members across {len(assigned_shows)} shows")
                
        finally:
            self.cleanup()

    def cleanup(self):
        """Clean up browser and temporary files."""
        try:
            if self.driver:
                self.driver.quit()
                print(f"🔒 Worker {self.worker_id}: Browser closed")
            
            # Clean up user data directory
            if hasattr(self, 'user_data_dir') and self.user_data_dir:
                import shutil
                import os
                if os.path.exists(self.user_data_dir):
                    shutil.rmtree(self.user_data_dir, ignore_errors=True)
                    print(f"🗑️ Worker {self.worker_id}: Cleaned up data directory")
        except Exception as e:
            print(f"⚠️ Worker {self.worker_id}: Cleanup warning: {e}")

# ========== Main Parallel Extractor ==========
class v2UniversalSeasonExtractorCastInfoParallelFull:
    def __init__(self):
        self.sheet = None
        self.results_manager = None
        self.skipped_filled = 0

    def setup_google_sheets(self):
        try:
            gc = gspread.service_account(filename=SERVICE_ACCOUNT_FILE)
            wb = gc.open(WORKBOOK_NAME)
            self.sheet = wb.worksheet(SHEET_NAME)
            self.results_manager = ThreadSafeResultsManager(self.sheet)
            print("✅ Google Sheets connected to CastInfo.")
            print(f"🔍 Connected to workbook: '{WORKBOOK_NAME}', sheet: '{SHEET_NAME}'")
            print(f"🔍 Sheet URL: {wb.url}")
            print(f"🔍 Sheet ID: {self.sheet.id}")
            
            # Test write to verify connection
            try:
                test_range = "A1"
                test_value = self.sheet.acell(test_range).value
                print(f"🔍 Test read from {test_range}: '{test_value}'")
                print("✅ Read access confirmed")
                
                # Test write to a safe cell (that exists)
                import datetime
                test_write_value = [['Test_' + datetime.datetime.now().strftime('%H%M%S')]]
                try:
                    # Use proper gspread syntax with range_name parameter
                    self.sheet.update(values=test_write_value, range_name='A1')
                    
                    # Read it back to confirm
                    written_value = self.sheet.acell('A1').value
                    if 'Test_' in str(written_value):
                        print("✅ Write access confirmed")
                    else:
                        print(f"❌ Write test failed: wrote '{test_write_value}', read '{written_value}'")
                        return False
                except Exception as we:
                    print(f"❌ Write test failed: {we}")
                    return False
                    
            except Exception as e:
                print(f"❌ Read/Write test failed: {e}")
                return False
                
            return True
        except Exception as e:
            print(f"❌ Google Sheets setup failed: {e}")
            return False

    def load_all_castinfo_rows(self):
        """Load ALL CastInfo rows that need processing."""
        all_values = self.sheet.get_all_values()
        print(f"🔍 Total rows in CastInfo sheet: {len(all_values)}")
        
        if not all_values or len(all_values) < 2:
            return []

        rows = []
        
        # Process all rows starting from row 2
        for r in range(1, len(all_values)):  # Skip header row
            row = all_values[r]
            if len(row) < 8:
                continue
                
            cast_name = row[0].strip()
            cast_imdb_id = row[2].strip()
            show_name = row[3].strip()
            show_imdb_id = row[4].strip()
            total_episodes = row[6].strip()
            seasons = row[7].strip()

            # Skip if both columns G and H are already filled
            if total_episodes and seasons:
                self.skipped_filled += 1
                continue

            # Skip if missing essential data
            if not show_imdb_id or not (cast_imdb_id or cast_name):
                continue

            rows.append({
                "row_num": r + 1,  # Convert to 1-based row number
                "cast_name": cast_name,
                "cast_imdb_id": cast_imdb_id,
                "show_name": show_name,
                "show_imdb_id": show_imdb_id,
            })

        print(f"📋 Total rows to process: {len(rows)}")
        print(f"⏭️ Skipped (already filled): {self.skipped_filled}")
        return rows

    def group_rows_by_show(self, all_cast_members):
        """Group cast members by show and distribute shows across workers."""
        # Group by show name
        shows_dict = {}
        for cast_data in all_cast_members:
            show_name = cast_data['show_name']
            if show_name not in shows_dict:
                shows_dict[show_name] = []
            shows_dict[show_name].append(cast_data)
        
        # Convert to list of shows with their cast members
        shows_list = list(shows_dict.items())
        print(f"📺 Found {len(shows_list)} unique shows to process")
        
        # Display show distribution info
        print(f"📊 Show distribution across {NUM_BROWSERS} browsers:")
        for i in range(NUM_BROWSERS):
            assigned_shows = shows_list[i::NUM_BROWSERS]  # Every NUM_BROWSERS-th show starting from index i
            total_cast_members = sum(len(cast_list) for _, cast_list in assigned_shows)
            show_names = [show_name for show_name, _ in assigned_shows[:3]]  # Show first 3 shows
            more_text = f" + {len(assigned_shows) - 3} more" if len(assigned_shows) > 3 else ""
            print(f"  🔸 Browser {i+1}: {len(assigned_shows)} shows, {total_cast_members} cast members")
            print(f"     📺 Shows: {', '.join(show_names)}{more_text}")
        
        return shows_list

    def run(self):
        print(f"🚀 Starting Full Parallel CastInfo Processing with {NUM_BROWSERS} browsers")
        
        if not self.setup_google_sheets():
            return False

        try:
            # Load ALL cast members that need processing
            all_cast_members = self.load_all_castinfo_rows()
            if not all_cast_members:
                print("📭 No rows to process")
                return True

            # Group cast members by show and distribute shows across browsers
            shows_list = self.group_rows_by_show(all_cast_members)
            if not shows_list:
                print("📭 No shows to process")
                return True
            
            # Start parallel processing with all workers
            with ThreadPoolExecutor(max_workers=NUM_BROWSERS) as executor:
                # Submit all workers with their assigned shows
                futures = {}
                for worker_id in range(1, NUM_BROWSERS + 1):
                    # Assign shows to this worker (every NUM_BROWSERS-th show starting from worker_id-1)
                    assigned_shows = shows_list[(worker_id-1)::NUM_BROWSERS]
                    
                    worker = CastInfoWorker(worker_id=worker_id, results_manager=self.results_manager)
                    future = executor.submit(worker.process_assigned_shows, assigned_shows)
                    futures[future] = worker_id
                
                # Monitor progress
                completed = 0
                start_time = time.time()
                
                for future in as_completed(futures.keys()):
                    worker_id = futures[future]
                    completed += 1
                    elapsed = time.time() - start_time
                    stats = self.results_manager.get_stats()
                    
                    print(f"\n📊 Worker {worker_id} completed ({completed}/{NUM_BROWSERS})")
                    print(f"⏱️ Elapsed time: {elapsed:.1f}s")
                    print(f"✅ Processed: {stats['processed']}")
                    print(f"🗑️ Crew deletions: {stats['deleted_crew']}")
                    print(f"❌ Errors: {stats['errors']}")
                    print(f"📝 Pending updates: {stats['pending_updates']}")
                    
                    # Flush any pending updates
                    self.results_manager.flush_updates()

            # Final flush of all pending updates
            print("\n💾 Final flush of all pending updates...")
            self.results_manager.flush_updates(force=True)
            
            # Save failed cast members log for investigation
            print("\n📝 Saving failed cast members log...")
            self.results_manager.save_failed_cast_members_log()
            
            # Final statistics
            final_stats = self.results_manager.get_stats()
            elapsed_total = time.time() - start_time
            
            print("\n🎉 Full Parallel CastInfo Processing Complete!")
            print(f"⏱️ Total time: {elapsed_total:.1f}s")
            print(f"✅ Successfully updated: {final_stats['processed']} rows")
            print(f"🗑️ Crew members deleted: {final_stats['deleted_crew']} rows")
            print(f"⏭️ Skipped (already filled): {self.skipped_filled} rows")
            print(f"❌ Errors encountered: {final_stats['errors']}")
            print(f"🔍 Failed cast members logged: {final_stats['failed_cast_members']}")
            print(f"📈 Average rate: {final_stats['processed'] / (elapsed_total / 60):.1f} rows/minute")
            return True

        except Exception as e:
            print(f"❌ Fatal error in parallel processing: {e}")
            import traceback
            traceback.print_exc()
            return False

# ---------- Entry Point ----------
def main():
    try:
        print("🚀 Starting v2 Universal Season Extractor - Full Parallel Processing")
        print(f"🔧 Configuration: {NUM_BROWSERS} browsers, batch size {BATCH_SIZE}")
        
        extractor = v2UniversalSeasonExtractorCastInfoParallelFull()
        success = extractor.run()
        
        print("✅ Script completed successfully" if success else "❌ Script failed")
        
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
