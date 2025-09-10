#!/usr/bin/env python3
# v3UniversalSeasonExtractorBottomUpAllFormats.py
"""
v3 Universal Season Extractor - All Formats (Bottom → Top)

- Starts at the BOTTOM of the sheet and works UP to row 2.
- Skips rows where BOTH G and H are already filled (non-empty).
- IMDb extraction:
  * Clicks the per-cast "episodes" pop-up.
  * Gets EPISODE COUNT from the pop-up header (e.g., "3 episodes").
  * Gets SEASONS from tabs like data-testid="season-tab-<n>".
  * If only 1 episode is listed, infers season from the first episode marker (e.g., "S1.E1").
- Batches Google Sheets updates every 10 rows (two cells per row: G,H).
- NEVER writes placeholders; if it can't confidently extract, it leaves the row untouched.
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
from selenium.common.exceptions import (
    TimeoutException, NoSuchElementException, WebDriverException
)
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains

# ---------- Configuration ----------
SERVICE_ACCOUNT_FILE = os.environ.get(
    "GSPREAD_SERVICE_ACCOUNT",
    "/Users/thomashulihan/Projects/TRR-Backend/keys/trr-backend-df2c438612e1.json"
)
WORKBOOK_NAME = "Realitease2025Data"
SHEET_NAME = "ViableCast"

# Fixed columns (G & H)
COL_G = 7  # Episode Count
COL_H = 8  # Seasons

# Batch update threshold
FLUSH_EVERY = 10

# Selenium timeouts
PAGE_LOAD_TIMEOUT = 30
REQ_TIMEOUT = 20


# ========== v3 Extractor (Bottom-Up) ==========
class v3UniversalSeasonExtractorBottomUpAllFormats:
    def __init__(self):
        self.driver = None
        self.sheet = None
        self.header_map = {}
        self.updated_buffer = []  # list of (row, ep_count, seasons)
        self.processed_count = 0
        self.skipped_filled = 0
        self.deleted_crew = 0
        self.errors = 0

    # ----- Setup -----
    def setup_google_sheets(self):
        try:
            gc = gspread.service_account(filename=SERVICE_ACCOUNT_FILE)
            wb = gc.open(WORKBOOK_NAME)
            self.sheet = wb.worksheet(SHEET_NAME)
            print("✅ Google Sheets connected.")
            return True
        except Exception as e:
            print(f"❌ Google Sheets setup failed: {e}")
            return False

    def setup_webdriver(self):
        try:
            chrome_options = Options()
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            chrome_options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebDriver/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36")

            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
            print("✅ WebDriver ready.")
            return True
        except Exception as e:
            print(f"❌ WebDriver setup failed: {e}")
            return False

    def smart_delay(self, base=1.2, jitter=0.8):
        time.sleep(base + random.uniform(0, jitter))

    # ----- Utility: headers & rows (BOTTOM-UP) -----
    def load_all_rows_bottom_up(self):
        """Return list of row dicts from BOTTOM to row 2 (reverse order)."""
        all_values = self.sheet.get_all_values()
        if not all_values or len(all_values) < 2:
            return []

        headers = [h.strip().lower() for h in all_values[0]]
        self.header_map = {h: i for i, h in enumerate(headers)}

        def col_of(fragment, default=None):
            for k, idx in self.header_map.items():
                if fragment in k:
                    return idx
            return default

        idx_show = col_of("show imdbid")
        idx_cast_name = col_of("castname")
        idx_cast_id = col_of("cast imdbid")
        # Fallback if headers are funky
        if idx_show is None or idx_cast_name is None or idx_cast_id is None:
            print("⚠️ Header lookup failed. Ensure headers 'Show IMDbID', 'CastName', 'Cast IMDbID' exist.")
            return []

        rows = []
        # Start from the LAST row and work backwards to row 2
        for r in range(len(all_values) - 1, 0, -1):  # Reverse: from last row down to row index 1 (spreadsheet row 2)
            row = all_values[r]
            # Make sure row length covers indexes
            def safe_get(i):
                return row[i].strip() if i is not None and i < len(row) else ""

            show_id = safe_get(idx_show)
            cast_name = safe_get(idx_cast_name)
            cast_imdb = safe_get(idx_cast_id)
            g_val = row[COL_G - 1].strip() if len(row) >= COL_G else ""
            h_val = row[COL_H - 1].strip() if len(row) >= COL_H else ""

            # Skip if both G & H already have values
            if g_val and h_val:
                self.skipped_filled += 1
                continue

            if not show_id or not (cast_imdb or cast_name):
                # Nothing we can do for this row
                continue

            rows.append({
                "row_num": r + 1,  # 1-indexed
                "show_imdbid": show_id,
                "cast_name": cast_name,
                "cast_imdbid": cast_imdb
            })

        print(f"📋 Rows queued (BOTTOM-UP): {len(rows)} | Skipped (G&H filled): {self.skipped_filled}")
        if rows:
            print(f"🔄 Processing order: Row {rows[0]['row_num']} (bottom) → Row {rows[-1]['row_num']} (top)")
        return rows

    # ----- Batch write -----
    def flush_updates(self, force=False):
        if not self.updated_buffer:
            return
        if not force and len(self.updated_buffer) < FLUSH_EVERY:
            return

        # Write each row's G&H together; doing sequential updates after batching 10 findings.
        # Intentionally not using fancy spreadsheet-level batch to keep it robust.
        for row_num, ep_count, seasons in self.updated_buffer:
            try:
                self.sheet.update(f"G{row_num}:H{row_num}", [[str(ep_count), str(seasons)]], value_input_option="RAW")
                self.smart_delay(0.3, 0.2)
            except Exception as e:
                print(f"❌ Failed writing row {row_num}: {e}")
                self.errors += 1
        print(f"💾 Wrote {len(self.updated_buffer)} row(s) to sheet.")
        self.updated_buffer.clear()

    # ----- IMDb extraction helpers -----
    def open_full_credits(self, show_imdbid):
        url = f"https://www.imdb.com/title/{show_imdbid}/fullcredits"
        for attempt in range(3):
            try:
                self.driver.get(url)
                WebDriverWait(self.driver, 15).until(
                    lambda d: d.execute_script("return document.readyState") == "complete"
                )
                return True
            except Exception:
                self.smart_delay(1.2, 1.0)
        print(f"❌ Could not load {url}")
        return False

    def find_cast_anchor(self, cast_imdbid, cast_name):
        """Prefer finding by cast IMDb id; fallback to name match."""
        # Try by ID (XPath; most reliable)
        if cast_imdbid:
            xpaths = [
                f"//a[contains(@href, '/name/{cast_imdbid}/')]",
                f"//a[contains(@href,'{cast_imdbid}')]",
            ]
            for xp in xpaths:
                els = self.driver.find_elements(By.XPATH, xp)
                if els:
                    return els[0]

        # Fallback by name (few sane selectors)
        if cast_name:
            variants = [cast_name, cast_name.strip(), cast_name.replace("'", "'")]
            for name in variants:
                try:
                    els = self.driver.find_elements(By.XPATH, f"//a[contains(@href,'/name/') and normalize-space(text())='{name}']")
                    if els:
                        return els[0]
                except Exception:
                    pass
        return None

    def find_episodes_button_near(self, cast_anchor):
        """Search upward in the DOM for a container, then find a button/element containing 'episode'."""
        containers = []
        try:
            containers.append(cast_anchor.find_element(By.XPATH, "./ancestor::li[1]"))
        except Exception:
            pass
        try:
            containers.append(cast_anchor.find_element(By.XPATH, "./ancestor::tr[1]"))
        except Exception:
            pass
        try:
            containers.append(cast_anchor.find_element(By.XPATH, "./ancestor::div[1]"))
        except Exception:
            pass

        for cont in containers:
            try:
                # prioritize buttons/links mentioning 'episode'
                btns = cont.find_elements(By.XPATH, ".//*[self::button or self::a or self::span][contains(translate(., 'EPISODE', 'episode'),'episode')]")
                if btns:
                    return btns[0]
            except Exception:
                continue
        return None

    def click(self, el):
        methods = [
            lambda: el.click(),
            lambda: self.driver.execute_script("arguments[0].click();", el),
            lambda: ActionChains(self.driver).move_to_element(el).click().perform()
        ]
        for m in methods:
            try:
                m()
                return True
            except Exception:
                self.smart_delay(0.2, 0.2)
        return False

    def parse_episode_count_from_popup(self):
        """Read pop-up header list for 'X episodes'. Enhanced to look in more places."""
        try:
            # Wait for prompt container with longer timeout
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".ipc-prompt-header"))
            )
            # Extra wait for content to fully render
            self.smart_delay(1.0, 0.5)
        except TimeoutException:
            print("  ⚠️ Timeout waiting for popup header")
            return None

        # Enhanced selectors to look for episode count in more places
        selectors = [
            "li.ipc-inline-list__item",  # Most specific - matches HTML exactly
            ".ipc-prompt-header__subtitle li.ipc-inline-list__item",
            ".ipc-prompt-header__text li.ipc-inline-list__item", 
            "ul.ipc-inline-list li.ipc-inline-list__item",
            ".ipc-prompt-header ul li",  # Broader match
            ".ipc-prompt-header li",     # Even broader
        ]
        
        for sel in selectors:
            try:
                items = self.driver.find_elements(By.CSS_SELECTOR, sel)
                for it in items:
                    text = it.text.strip()
                    # Look for "X episode" or "X episodes"
                    m = re.search(r"(\d+)\s+episodes?", text, re.I)
                    if m:
                        ep_count = int(m.group(1))
                        print(f"  📊 Found {ep_count} episodes in popup header")
                        return ep_count
            except Exception:
                continue
                
        # If no episode count found in headers, try counting episode links
        try:
            episode_links = self.driver.find_elements(By.CSS_SELECTOR, 'a.episodic-credits-bottomsheet__menu-item')
            if episode_links:
                ep_count = len(episode_links)
                print(f"  📊 Counted {ep_count} episode links in modal")
                return ep_count
        except Exception:
            pass
            
        return None

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
        """Handle year-based banners by clicking each year tab and extracting season from episode markers."""
        try:
            # Look for year-based tabs (typically 4-digit years)
            year_tabs = self.driver.find_elements(By.CSS_SELECTOR, 'li[role="tab"]')
            year_seasons = set()
            
            for tab in year_tabs:
                try:
                    tab_text = tab.text.strip()
                    # Check if this looks like a year (4 digits) - enhanced detection
                    if re.match(r'^\d{4}$', tab_text) or re.match(r'^(19|20)\d{2}$', tab_text):
                        print(f"  🗓️ Found year tab: {tab_text} - extracting seasons from episode markers")
                        self.click(tab)
                        self.smart_delay(0.7, 0.4)  # Give more time for content to load
                        
                        # Enhanced episode marker selectors
                        episode_selectors = [
                            'a.episodic-credits-bottomsheet__menu-item',
                            'a[role="menuitem"]',
                            '[data-testid*="episode"]',
                            'li.ipc-inline-list__item',
                            '[class*="episode"]'
                        ]
                        
                        found_seasons_this_year = set()
                        for selector in episode_selectors:
                            try:
                                episode_elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                                for elem in episode_elements[:10]:  # Check more episodes
                                    try:
                                        elem_text = elem.text.strip()
                                        if not elem_text:
                                            elem_text = elem.get_attribute('innerText') or ''
                                        
                                        # Enhanced season extraction patterns
                                        season_patterns = [
                                            r'S(\d+)\.E\d+',           # S1.E1 format
                                            r'S(\d+)E\d+',             # S1E1 format  
                                            r'S(\d+)\s*·\s*E\d+',      # S1 · E1 format
                                            r'Season\s+(\d+)',         # Season 1 format
                                            r'S(\d+)',                 # Just S1 format
                                            r'(\d+)x\d+',              # 1x01 format
                                        ]
                                        
                                        for pattern in season_patterns:
                                            season_match = re.search(pattern, elem_text, re.I)
                                            if season_match:
                                                season_num = int(season_match.group(1))
                                                found_seasons_this_year.add(season_num)
                                                print(f"    ✅ Found season {season_num} from pattern '{pattern}' in year {tab_text}")
                                                break
                                    except Exception:
                                        continue
                            except Exception:
                                continue
                        
                        # Add all seasons found in this year
                        year_seasons.update(found_seasons_this_year)
                        
                        # If we found seasons in this year, no need to check more tabs
                        if found_seasons_this_year:
                            print(f"    🎯 Year {tab_text} contributed seasons: {sorted(found_seasons_this_year)}")
                        
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
        """Look anywhere inside the open IMDb episodes modal for season markers.
        Handles formats like 'S1.E3', 'S1E3', 'Season 2', 'S02 · E01', etc.
        Enhanced to handle year-based layouts better.
        Returns a string season number or None.
        """
        # Try to scope to the modal/prompt if present, else search whole page
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
            "[class*='credit']",  # Added for credits-based layouts
            ".ipc-metadata-list-summary-item",  # Added for metadata layouts
        ]
        
        # Enhanced patterns to handle more formats
        patterns = [
            r"S\s*(\d+)\s*[\.|·\-E]\s*\d+",      # S1.E1, S1·E1, S1-E1, S1E1
            r"Season\s*(\d+)(?!\s*Tab)",          # Season 1 (but not Season Tabs)
            r"(\d+)x\d+",                         # 1x01 format
            r"S(\d+)",                            # Just S1
            r"Ep\s*\d+\s*S(\d+)",                # Ep 1 S1 format
            r"(?:Episode\s*\d+.*?)?S(\d+)",       # Episode 1 ... S1
        ]

        for root in roots:
            for sel in selectors:
                try:
                    els = root.find_elements(By.CSS_SELECTOR, sel)
                    for el in els[:25]:  # Check more elements
                        txt = (el.text or "").strip()
                        if not txt:
                            try:
                                txt = (el.get_attribute('innerText') or '').strip()
                            except Exception:
                                pass
                        
                        if not txt:  # Try innerHTML as well
                            try:
                                txt = (el.get_attribute('innerHTML') or '').strip()
                                # Clean HTML tags for better matching
                                txt = re.sub(r'<[^>]+>', ' ', txt)
                            except Exception:
                                pass
                        
                        for pat in patterns:
                            m = re.search(pat, txt, re.I)
                            if m:
                                season_num = int(m.group(1))
                                print(f"  🎯 Season {season_num} found from element text using pattern '{pat}': '{txt[:50]}...'")
                                return str(season_num)
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

    def detect_crew_role(self, cast_imdbid, cast_name):
        """
        Check if this person is crew (director, producer, etc.) rather than cast.
        Returns True if they are crew, False if they are cast or unknown.
        """
        try:
            # Look for crew sections first
            crew_sections = [
                "//h4[contains(text(), 'Directed by') or contains(text(), 'Produced by') or contains(text(), 'Writing Credits') or contains(text(), 'Music by') or contains(text(), 'Cinematography by') or contains(text(), 'Art Direction by') or contains(text(), 'Production Management') or contains(text(), 'Second Unit Director') or contains(text(), 'Assistant Director') or contains(text(), 'Sound Department') or contains(text(), 'Special Effects') or contains(text(), 'Visual Effects') or contains(text(), 'Stunts') or contains(text(), 'Camera and Electrical') or contains(text(), 'Editorial Department') or contains(text(), 'Music Department') or contains(text(), 'Transportation Department') or contains(text(), 'Additional Crew')]"
            ]
            
            # Check if person appears in crew sections
            if cast_imdbid:
                # Look for their IMDb ID in crew sections
                crew_links = self.driver.find_elements(By.XPATH, f"//div[contains(@class, 'header') and (contains(text(), 'Directed by') or contains(text(), 'Produced by') or contains(text(), 'Writing') or contains(text(), 'Music') or contains(text(), 'Cinematography') or contains(text(), 'Art Direction') or contains(text(), 'Production') or contains(text(), 'Assistant Director') or contains(text(), 'Sound') or contains(text(), 'Effects') or contains(text(), 'Stunts') or contains(text(), 'Camera') or contains(text(), 'Editorial') or contains(text(), 'Transportation') or contains(text(), 'Additional Crew'))]/following-sibling::*//a[contains(@href, '{cast_imdbid}')]")
                if crew_links:
                    print(f"  🎬 CREW DETECTED: {cast_name} found in crew section")
                    return True
            
            # Fallback: check by name in crew sections
            if cast_name:
                crew_name_links = self.driver.find_elements(By.XPATH, f"//div[contains(@class, 'header') and (contains(text(), 'Directed by') or contains(text(), 'Produced by') or contains(text(), 'Writing') or contains(text(), 'Music') or contains(text(), 'Cinematography') or contains(text(), 'Art Direction') or contains(text(), 'Production') or contains(text(), 'Assistant Director') or contains(text(), 'Sound') or contains(text(), 'Effects') or contains(text(), 'Stunts') or contains(text(), 'Camera') or contains(text(), 'Editorial') or contains(text(), 'Transportation') or contains(text(), 'Additional Crew'))]/following-sibling::*//a[contains(text(), '{cast_name}')]")
                if crew_name_links:
                    print(f"  🎬 CREW DETECTED: {cast_name} found in crew section by name")
                    return True
            
            return False
        except Exception as e:
            print(f"  ⚠️ Error checking crew role: {e}")
            return False

    def delete_row(self, row_num):
        """Delete a row from the sheet."""
        try:
            self.sheet.delete_rows(row_num)
            print(f"  🗑️ DELETED row {row_num} (crew member)")
            return True
        except Exception as e:
            print(f"  ❌ Failed to delete row {row_num}: {e}")
            return False

    def close_any_modal(self):
        # Try various close strategies, keep silent if they fail
        candidates = [
            (By.CSS_SELECTOR, '[data-testid="promptable__x"] button'),
            (By.CSS_SELECTOR, ".ipc-promptable-base__close button"),
            (By.CSS_SELECTOR, '[aria-label="Close"]'),
            (By.CSS_SELECTOR, ".close"),
        ]
        for by, sel in candidates:
            try:
                self.driver.find_element(by, sel).click()
                self.smart_delay(0.2, 0.2)
                return True
            except Exception:
                pass
        try:
            ActionChains(self.driver).send_keys(Keys.ESCAPE).perform()
            return True
        except Exception:
            return False
        # Try various close strategies, keep silent if they fail
        candidates = [
            (By.CSS_SELECTOR, '[data-testid="promptable__x"] button'),
            (By.CSS_SELECTOR, ".ipc-promptable-base__close button"),
            (By.CSS_SELECTOR, '[aria-label="Close"]'),
            (By.CSS_SELECTOR, ".close"),
        ]
        for by, sel in candidates:
            try:
                self.driver.find_element(by, sel).click()
                self.smart_delay(0.2, 0.2)
                return True
            except Exception:
                pass
        try:
            ActionChains(self.driver).send_keys(Keys.ESCAPE).perform()
            return True
        except Exception:
            return False

    def extract_for_row(self, show_imdbid, cast_imdbid, cast_name):
        """Core: open full credits → check if crew → if cast, open episodes pop-up → parse header & seasons."""
        if not self.open_full_credits(show_imdbid):
            return None, None

        # First check if this person is crew rather than cast
        if self.detect_crew_role(cast_imdbid, cast_name):
            return "DELETE_CREW", None

        anchor = self.find_cast_anchor(cast_imdbid, cast_name)
        if not anchor:
            return None, None

        btn = self.find_episodes_button_near(anchor)
        if not btn:
            return None, None

        # Click episodes
        self.driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
        if not self.click(btn):
            return None, None
        self.smart_delay(0.8, 0.7)

        # EPISODE COUNT from POP-UP HEADER
        ep_count = self.parse_episode_count_from_popup()

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

        # If exactly one episode and still no season, try once more (in case of delayed render)
        if ep_count == 1 and not seasons:
            self.smart_delay(0.4, 0.3)
            s = self.parse_season_from_any_episode_marker()
            if s:
                seasons = s

        # All done — close the modal if present
        self.close_any_modal()

        return ep_count, seasons

    # ----- Main loop (BOTTOM-UP) -----
    def run(self):
        if not self.setup_google_sheets():
            return False
        if not self.setup_webdriver():
            return False

        try:
            targets = self.load_all_rows_bottom_up()
            if not targets:
                print("Nothing to do.")
                return True

            print(f"🔄 Starting BOTTOM-UP processing...")
            for i, t in enumerate(targets, 1):
                rn = t["row_num"]
                show_id = t["show_imdbid"]
                cast_id = t["cast_imdbid"]
                cast_name = t["cast_name"]

                print(f"\n🎭 ⬆️ [{i}/{len(targets)}] Row {rn} | {cast_name} | {show_id}")
                try:
                    ep_count, seasons = self.extract_for_row(show_id, cast_id, cast_name)
                    
                    # Handle crew deletion
                    if ep_count == "DELETE_CREW":
                        if self.delete_row(rn):
                            self.deleted_crew += 1
                            # After deletion, we need to adjust row numbers for subsequent rows
                            # since we're going bottom-up, rows above this one shift down by 1
                            for j in range(i, len(targets)):
                                if targets[j]["row_num"] < rn:
                                    targets[j]["row_num"] -= 1
                        continue
                    
                    # Normal processing for cast members
                    if ep_count is not None and seasons:
                        # Both episode count and seasons found
                        self.updated_buffer.append((rn, ep_count, seasons))
                        self.processed_count += 1
                        print(f"  ✅ Episodes: {ep_count} | Seasons: {seasons}")
                        self.flush_updates()  # flush when >= FLUSH_EVERY
                    elif seasons and ep_count is None:
                        # Found seasons but no episode count - use default of 1
                        default_ep_count = 1
                        self.updated_buffer.append((rn, default_ep_count, seasons))
                        self.processed_count += 1
                        print(f"  ✅ Episodes: {default_ep_count} (default) | Seasons: {seasons}")
                        self.flush_updates()  # flush when >= FLUSH_EVERY
                    else:
                        print("  ⚠️ Could not extract confidently — leaving row untouched.")
                except KeyboardInterrupt:
                    print("\n⏹️ Interrupted.")
                    break
                except Exception as e:
                    print(f"  ❌ Error on row {rn}: {e}")
                    self.errors += 1

                # Gentle pacing
                self.smart_delay(0.8, 0.8)

            # Final flush
            self.flush_updates(force=True)

            print("\n🎉 BOTTOM-UP processing complete!")
            print(f"  ✅ Updated rows: {self.processed_count}")
            print(f"  🗑️ Deleted crew rows: {self.deleted_crew}")
            print(f"  ⏭️ Skipped (G&H already filled): {self.skipped_filled}")
            print(f"  ❌ Errors: {self.errors}")
            return True

        finally:
            try:
                if self.driver:
                    self.driver.quit()
            except Exception:
                pass


# ---------- Entry ----------
def main():
    extractor = v3UniversalSeasonExtractorBottomUpAllFormats()
    ok = extractor.run()
    if not ok:
        sys.exit(1)

if __name__ == "__main__":
    main()
