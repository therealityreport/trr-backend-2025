#!/usr/bin/env python3
# v2UniversalSeasonExtractorCastInfo.py
"""
v2 Universal Season Extractor - CastInfo Sheet Version

- Updates TotalEpisodes (Column G) and Seasons (Column H) in CastInfo sheet
- Skips rows where BOTH TotalEpisodes and Seasons are already filled (non-empty)
- IMDb extraction:
  * Clicks the per-cast "episodes" pop-up
  * Gets EPISODE COUNT from the pop-up header (e.g., "3 episodes")
  * Gets SEASONS from tabs like data-testid="season-tab-<n>"
  * Enhanced year-based banner support for extracting seasons from episode markers
  * If only 1 episode is listed, infers season from the first episode marker (e.g., "S1.E1")
- Crew detection and deletion functionality
- Batches Google Sheets updates every 15 rows (two cells per row: G,H)
- NEVER writes placeholders; if it can't confidently extract, it leaves the row untouched

CastInfo Sheet Structure:
A: CastName
B: TMDb CastID  
C: Cast IMDbID
D: ShowName
E: Show IMDbID
F: TMDb ShowID
G: TotalEpisodes (TARGET)
H: Seasons (TARGET)
"""

print("🔍 DEBUG: CastInfo Script file is being executed!")

import os
import sys
import time
import random
import re
from collections import defaultdict

print("🔍 DEBUG: Basic imports successful!")

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

print("🔍 DEBUG: All imports successful!")

# ---------- Configuration ----------
SERVICE_ACCOUNT_FILE = os.environ.get(
    "GSPREAD_SERVICE_ACCOUNT",
    "/Users/thomashulihan/Projects/TRR-Backend/keys/trr-backend-df2c438612e1.json"
)
WORKBOOK_NAME = "Realitease2025Data"
SHEET_NAME = "CastInfo"

# Fixed columns for CastInfo (G & H)
COL_G_EPISODES = 7  # TotalEpisodes
COL_H_SEASONS = 8   # Seasons

# Batch update threshold
FLUSH_EVERY = 25

# Selenium timeouts
PAGE_LOAD_TIMEOUT = 30
REQ_TIMEOUT = 20


# ========== CastInfo Extractor ==========
class v2UniversalSeasonExtractorCastInfo:
    def __init__(self):
        self.driver = None
        self.sheet = None
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
            print("✅ Google Sheets connected to CastInfo.")
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
            chrome_options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36")

            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
            print("✅ WebDriver ready.")
            return True
        except Exception as e:
            print(f"❌ WebDriver setup failed: {e}")
            return False

    def smart_delay(self, base=1.2, jitter=0.8):
        time.sleep(base + random.uniform(0, jitter))

    # ----- Load CastInfo rows -----
    def load_castinfo_rows(self, start_row=2, limit=None):
        """Load CastInfo rows that need TotalEpisodes and/or Seasons filled."""
        all_values = self.sheet.get_all_values()
        print(f"🔍 DEBUG: Total rows in CastInfo sheet: {len(all_values)}")
        
        if not all_values or len(all_values) < start_row:
            print(f"❌ Sheet has {len(all_values) if all_values else 0} rows, but start_row is {start_row}")
            return []

        headers = all_values[0]
        print(f"📋 Headers: {headers}")
        
        # Expected structure: CastName, TMDb CastID, Cast IMDbID, ShowName, Show IMDbID, TMDb ShowID, TotalEpisodes, Seasons
        rows = []
        end_row = min(len(all_values), start_row + limit - 1) if limit else len(all_values)
        print(f"🔍 Processing rows {start_row} to {end_row} (limit: {limit if limit else 'none'})")
        
        for r in range(start_row - 1, end_row):  # start_row-1 because 0-indexed
            row = all_values[r]
            
            # Ensure row has enough columns
            if len(row) < 8:
                continue
                
            cast_name = row[0].strip() if len(row) > 0 else ""
            tmdb_cast_id = row[1].strip() if len(row) > 1 else ""
            cast_imdb_id = row[2].strip() if len(row) > 2 else ""
            show_name = row[3].strip() if len(row) > 3 else ""
            show_imdb_id = row[4].strip() if len(row) > 4 else ""
            tmdb_show_id = row[5].strip() if len(row) > 5 else ""
            total_episodes = row[6].strip() if len(row) > 6 else ""
            seasons = row[7].strip() if len(row) > 7 else ""

            # Skip if both TotalEpisodes and Seasons are already filled
            if total_episodes and seasons:
                self.skipped_filled += 1
                continue

            # Skip if we don't have essential data
            if not show_imdb_id or not (cast_imdb_id or cast_name):
                continue

            rows.append({
                "row_num": r + 1,  # 1-indexed
                "cast_name": cast_name,
                "tmdb_cast_id": tmdb_cast_id,
                "cast_imdb_id": cast_imdb_id,
                "show_name": show_name,
                "show_imdb_id": show_imdb_id,
                "tmdb_show_id": tmdb_show_id,
                "current_episodes": total_episodes,
                "current_seasons": seasons
            })

        print(f"📋 Rows queued for processing: {len(rows)} | Skipped (G&H filled): {self.skipped_filled}")
        return rows

    # ----- Batch write -----
    def flush_updates(self, force=False):
        if not self.updated_buffer:
            return
        if not force and len(self.updated_buffer) < FLUSH_EVERY:
            return

        # Write each row's G&H together
        for row_num, ep_count, seasons in self.updated_buffer:
            try:
                self.sheet.update(f"G{row_num}:H{row_num}", [[str(ep_count), str(seasons)]], value_input_option="RAW")
                self.smart_delay(0.3, 0.2)
            except Exception as e:
                print(f"❌ Failed writing row {row_num}: {e}")
                self.errors += 1
                print(f"💾 Wrote {len(self.updated_buffer)} row(s) to CastInfo sheet (batch every 25 rows).")
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

        # Fallback by name
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
        """Read pop-up header for 'X episodes'. Enhanced to look in more places."""
        try:
            # Wait for popup container
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".ipc-prompt-header"))
            )
            self.smart_delay(1.0, 0.5)
        except TimeoutException:
            print("  ⚠️ Timeout waiting for popup header")
            return None

        # Enhanced selectors
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
            try:
                items = self.driver.find_elements(By.CSS_SELECTOR, sel)
                print(f"    🔍 Selector '{sel}' found {len(items)} elements")
                
                for i, it in enumerate(items):
                    text = it.text.strip()
                    print(f"      Element {i+1}: '{text}'")
                    
                    # Look for "X episode" or "X episodes"
                    m = re.search(r"(\d+)\s+episodes?", text, re.I)
                    if m:
                        ep_count = int(m.group(1))
                        print(f"  📊 Found {ep_count} episodes in popup header using selector '{sel}'")
                        return ep_count
            except Exception as e:
                print(f"    ❌ Error with selector '{sel}': {e}")
                continue
                
        # Fallback: count episode links
        print("  🔍 No episode count in headers, trying to count episode links...")
        try:
            episode_links = self.driver.find_elements(By.CSS_SELECTOR, 'a.episodic-credits-bottomsheet__menu-item')
            if episode_links:
                ep_count = len(episode_links)
                print(f"  📊 Counted {ep_count} episode links in modal")
                return ep_count
            else:
                print("  ⚠️ No episode links found")
        except Exception as e:
            print(f"  ❌ Error counting episode links: {e}")
            
        print("  ❌ Could not find episode count anywhere")
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
            year_tabs = self.driver.find_elements(By.CSS_SELECTOR, 'li[role="tab"]')
            year_seasons = set()
            
            for tab in year_tabs:
                try:
                    tab_text = tab.text.strip()
                    if re.match(r'^\d{4}$', tab_text) or re.match(r'^(19|20)\d{2}$', tab_text):
                        print(f"  🗓️ Found year tab: {tab_text} - extracting seasons from episode markers")
                        self.click(tab)
                        self.smart_delay(0.7, 0.4)
                        
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
                                for elem in episode_elements[:10]:
                                    try:
                                        elem_text = elem.text.strip()
                                        if not elem_text:
                                            elem_text = elem.get_attribute('innerText') or ''
                                        
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
                                                found_seasons_this_year.add(season_num)
                                                print(f"    ✅ Found season {season_num} from pattern '{pattern}' in year {tab_text}")
                                                break
                                    except Exception:
                                        continue
                            except Exception:
                                continue
                        
                        year_seasons.update(found_seasons_this_year)
                        
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

        for root in roots:
            for sel in selectors:
                try:
                    els = root.find_elements(By.CSS_SELECTOR, sel)
                    for el in els[:25]:
                        txt = (el.text or "").strip()
                        if not txt:
                            try:
                                txt = (el.get_attribute('innerText') or '').strip()
                            except Exception:
                                pass
                        
                        if not txt:
                            try:
                                txt = (el.get_attribute('innerHTML') or '').strip()
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
        """Check if this person is crew rather than cast."""
        try:
            if cast_imdbid:
                crew_xpath = f"//h4[contains(text(), 'Directed by') or contains(text(), 'Produced by') or contains(text(), 'Writing') or contains(text(), 'Music') or contains(text(), 'Art Direction') or contains(text(), 'Production') or contains(text(), 'Assistant Director') or contains(text(), 'Sound') or contains(text(), 'Effects') or contains(text(), 'Stunts') or contains(text(), 'Camera') or contains(text(), 'Editorial') or contains(text(), 'Transportation') or contains(text(), 'Additional Crew')]/following-sibling::*//a[contains(@href, '{cast_imdbid}')]"
                crew_links = self.driver.find_elements(By.XPATH, crew_xpath)
                if crew_links:
                    print(f"  🎬 CREW DETECTED: {cast_name} found in crew section by ID")
                    return True
            
            if cast_name:
                safe_name = cast_name.replace("'", "\\'")
                crew_name_xpath = f"//h4[contains(text(), 'Directed by') or contains(text(), 'Produced by') or contains(text(), 'Writing') or contains(text(), 'Music') or contains(text(), 'Art Direction') or contains(text(), 'Production') or contains(text(), 'Assistant Director') or contains(text(), 'Sound') or contains(text(), 'Effects') or contains(text(), 'Stunts') or contains(text(), 'Camera') or contains(text(), 'Editorial') or contains(text(), 'Transportation') or contains(text(), 'Additional Crew')]/following-sibling::*//a[contains(text(), '{safe_name}')]"
                crew_name_links = self.driver.find_elements(By.XPATH, crew_name_xpath)
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

        # If exactly one episode and still no season, try once more
        if ep_count == 1 and not seasons:
            self.smart_delay(0.4, 0.3)
            s = self.parse_season_from_any_episode_marker()
            if s:
                seasons = s

        # Close the modal
        self.close_any_modal()

        return ep_count, seasons

    # ----- Main loop -----
    def run(self, start_row=2, limit=None):
        print("🚀 Starting v2 Universal Season Extractor for CastInfo")
        
        if not self.setup_google_sheets():
            print("❌ Failed to setup Google Sheets")
            return False
        
        if not self.setup_webdriver():
            print("❌ Failed to setup WebDriver")
            return False

        try:
            targets = self.load_castinfo_rows(start_row, limit)
            if not targets:
                print("📭 Nothing to do - no rows found to process.")
                return True

            print(f"🔄 Starting CastInfo processing from row {start_row}...")
            for i, t in enumerate(targets, 1):
                rn = t["row_num"]
                show_id = t["show_imdb_id"]
                cast_id = t["cast_imdb_id"]
                cast_name = t["cast_name"]
                show_name = t["show_name"]

                print(f"\n🎭 [{i}/{len(targets)}] Row {rn} | {cast_name} | {show_name} | {show_id}")
                try:
                    ep_count, seasons = self.extract_for_row(show_id, cast_id, cast_name)
                    
                    # Handle crew deletion
                    if ep_count == "DELETE_CREW":
                        if self.delete_row(rn):
                            self.deleted_crew += 1
                            # Adjust subsequent row numbers after deletion
                            for j in range(i, len(targets)):
                                if targets[j]["row_num"] > rn:
                                    targets[j]["row_num"] -= 1
                        continue
                    
                    # Normal processing for cast members
                    if ep_count is not None and seasons:
                        # Both episode count and seasons found
                        self.updated_buffer.append((rn, ep_count, seasons))
                        self.processed_count += 1
                        print(f"  ✅ Episodes: {ep_count} | Seasons: {seasons}")
                        self.flush_updates()
                    elif seasons and ep_count is None:
                        # Found seasons but no episode count - use default of 1
                        default_ep_count = 1
                        self.updated_buffer.append((rn, default_ep_count, seasons))
                        self.processed_count += 1
                        print(f"  ✅ Episodes: {default_ep_count} (default) | Seasons: {seasons}")
                        self.flush_updates()
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

            print("\n🎉 CastInfo processing complete!")
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
    try:
        print("🚀 CastInfo Season Extractor starting...")
        extractor = v2UniversalSeasonExtractorCastInfo()
        
        # Process first 100 rows with batching every 25 rows
        start_row = 2
        limit = 0
        print(f"🎯 Processing first {limit} rows starting from row {start_row}")
        print(f"📦 Batching updates every 25 rows")
        
        ok = extractor.run(start_row, limit)
        if not ok:
            print("❌ Script failed")
            sys.exit(1)
        print("✅ Script completed successfully")
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
