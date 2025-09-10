#!/usr/bin/env python3
# v2UniversalSeasonExtractorCastInfo_SpecificSelectors.py
"""
v2 Universal Season Extractor - CastInfo Sheet Version with User-Specified Selectors

Uses the exact selectors provided by the user for precise modal extraction:

1. Cast Table: #__next > main > div > section > div > section > div > div.sc-e1aae3e0-1.eEFIsG.ipc-page-grid__item.ipc-page-grid__item--span-2 > section:nth-child(9)

2. Episode Count (from modal): 
   body > div.ipc-promptable-base.ipc-promptable-dialog.sc-5dfe8819-0.bqEnKQ.enter-done > div.ipc-promptable-base__panel.ipc-promptable-base__panel--baseAlt.episodic-credits-bottomsheet__panel > div > div.ipc-promptable-base__auto-focus > div > div.ipc-promptable-base__header > div > div.ipc-prompt-header__text-block > div.ipc-prompt-header__text.ipc-prompt-header__subtitle > ul > li:nth-child(1)

3. Season Banner:
   body > div.ipc-promptable-base.ipc-promptable-dialog.sc-5dfe8819-0.bqEnKQ.enter-done > div.ipc-promptable-base__panel.ipc-promptable-base__panel--baseAlt.episodic-credits-bottomsheet__panel > div > div.ipc-promptable-base__auto-focus > div > div.ipc-promptable-base__content > div > ul > div.sc-52bce4b8-0.iQhUZy

4. Episode Marker (fallback for single season):
   body > div.ipc-promptable-base.ipc-promptable-dialog.sc-5dfe8819-0.bqEnKQ.enter-done > div.ipc-promptable-base__panel.ipc-promptable-base__panel--baseAlt.episodic-credits-bottomsheet__panel > div > div.ipc-promptable-base__auto-focus > div > div.ipc-promptable-base__content > div > ul > div:nth-child(4) > a > span

- Updates TotalEpisodes (Column G) and Seasons (Column H) in CastInfo sheet
- Skips rows where BOTH TotalEpisodes and Seasons are already filled
"""

print("🎯 Starting CastInfo extractor with user-specified selectors!")

import os
import sys
import time
import random
import re
from collections import defaultdict

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
SERVICE_ACCOUNT_FILE = os.environ.get(
    "GSPREAD_SERVICE_ACCOUNT",
    "/Users/thomashulihan/Projects/TRR-Backend/keys/trr-backend-df2c438612e1.json"
)
WORKBOOK_NAME = "Realitease2025Data"
SHEET_NAME = "CastInfo"

# User-specified selectors
CAST_TABLE_SELECTOR = "#__next > main > div > section > div > section > div > div.sc-e1aae3e0-1.eEFIsG.ipc-page-grid__item.ipc-page-grid__item--span-2 > section:nth-child(9)"

MODAL_EPISODE_COUNT_SELECTOR = "body > div.ipc-promptable-base.ipc-promptable-dialog.sc-5dfe8819-0.bqEnKQ.enter-done > div.ipc-promptable-base__panel.ipc-promptable-base__panel--baseAlt.episodic-credits-bottomsheet__panel > div > div.ipc-promptable-base__auto-focus > div > div.ipc-promptable-base__header > div > div.ipc-prompt-header__text-block > div.ipc-prompt-header__text.ipc-prompt-header__subtitle > ul > li:nth-child(1)"

MODAL_SEASON_BANNER_SELECTOR = "body > div.ipc-promptable-base.ipc-promptable-dialog.sc-5dfe8819-0.bqEnKQ.enter-done > div.ipc-promptable-base__panel.ipc-promptable-base__panel--baseAlt.episodic-credits-bottomsheet__panel > div > div.ipc-promptable-base__auto-focus > div > div.ipc-promptable-base__content > div > ul > div.sc-52bce4b8-0.iQhUZy"

MODAL_EPISODE_MARKER_SELECTOR = "body > div.ipc-promptable-base.ipc-promptable-dialog.sc-5dfe8819-0.bqEnKQ.enter-done > div.ipc-promptable-base__panel.ipc-promptable-base__panel--baseAlt.episodic-credits-bottomsheet__panel > div > div.ipc-promptable-base__auto-focus > div > div.ipc-promptable-base__content > div > ul > div:nth-child(4) > a > span"

# Batch update threshold
FLUSH_EVERY = 15
MODAL_TIMEOUT = 15

# ========== Specific Selector CastInfo Extractor ==========
class v2UniversalSeasonExtractorCastInfoSpecific:
    def __init__(self):
        self.driver = None
        self.sheet = None
        self.updated_buffer = []
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
            self.driver.set_page_load_timeout(30)
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
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
        print(f"🔍 Total rows in CastInfo sheet: {len(all_values)}")
        
        if not all_values or len(all_values) < start_row:
            return []

        rows = []
        end_row = min(len(all_values), start_row + limit - 1) if limit else len(all_values)
        
        for r in range(start_row - 1, end_row):
            row = all_values[r]
            if len(row) < 8:
                continue
                
            cast_name = row[0].strip()
            cast_imdb_id = row[2].strip()
            show_name = row[3].strip()
            show_imdb_id = row[4].strip()
            total_episodes = row[6].strip()
            seasons = row[7].strip()

            # Skip if both filled
            if total_episodes and seasons:
                self.skipped_filled += 1
                continue

            # Skip if missing essential data
            if not show_imdb_id or not (cast_imdb_id or cast_name):
                continue

            rows.append({
                "row_num": r + 1,
                "cast_name": cast_name,
                "cast_imdb_id": cast_imdb_id,
                "show_name": show_name,
                "show_imdb_id": show_imdb_id,
                "current_episodes": total_episodes,
                "current_seasons": seasons
            })

        print(f"📋 Rows queued: {len(rows)} | Skipped (filled): {self.skipped_filled}")
        return rows

    # ----- Core extraction methods -----
    def open_fullcredits_page(self, show_imdb_id):
        """Open IMDb fullcredits page and wait for cast table."""
        url = f"https://www.imdb.com/title/{show_imdb_id}/fullcredits"
        print(f"  🌐 Opening {url}")
        
        try:
            self.driver.get(url)
            # Wait for the specific cast table to load
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, CAST_TABLE_SELECTOR))
            )
            print(f"  ✅ Cast table loaded")
            return True
        except Exception as e:
            print(f"  ❌ Failed to load page: {e}")
            return False

    def find_cast_member_episodes_button(self, cast_name, cast_imdb_id=None):
        """Find the cast member's episodes button using the cast table."""
        print(f"  🔍 Searching for {cast_name} in cast table...")
        
        try:
            cast_table = self.driver.find_element(By.CSS_SELECTOR, CAST_TABLE_SELECTOR)
            
            # Strategy 1: Find by IMDb ID
            if cast_imdb_id:
                try:
                    cast_link = cast_table.find_element(By.XPATH, f".//a[contains(@href, '/name/{cast_imdb_id}/')]")
                    print(f"  ✅ Found {cast_name} by ID")
                    return self.find_episodes_button_near_element(cast_link)
                except NoSuchElementException:
                    pass
            
            # Strategy 2: Find by name
            name_variations = [cast_name, cast_name.replace("'", "'")]
            for name_var in name_variations:
                try:
                    cast_xpath = f".//a[contains(@href, '/name/') and normalize-space(text())='{name_var}']"
                    cast_link = cast_table.find_element(By.XPATH, cast_xpath)
                    print(f"  ✅ Found {cast_name} by name")
                    return self.find_episodes_button_near_element(cast_link)
                except NoSuchElementException:
                    continue
            
            print(f"  ❌ Could not find {cast_name} in cast table")
            return None
            
        except Exception as e:
            print(f"  ❌ Error searching cast table: {e}")
            return None

    def find_episodes_button_near_element(self, cast_element):
        """Find episodes button near the cast member element."""
        try:
            # Look for episodes button in the same row/container
            parent_row = cast_element.find_element(By.XPATH, "./ancestor::li[1]")
            episodes_button = parent_row.find_element(By.XPATH, ".//button[contains(text(), 'episode')]")
            
            button_text = episodes_button.text.strip()
            print(f"  📊 Found episodes button: '{button_text}'")
            return episodes_button
            
        except Exception as e:
            print(f"  ⚠️ Could not find episodes button: {e}")
            return None

    def click_episodes_button_and_extract(self, button):
        """Click episodes button and extract modal data."""
        print(f"  🖱️ Clicking episodes button...")
        
        try:
            # Scroll into view and click
            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", button)
            self.smart_delay(0.5, 0.3)
            
            # Try clicking
            try:
                button.click()
            except:
                self.driver.execute_script("arguments[0].click();", button)
            
            print(f"  ✅ Button clicked, waiting for modal...")
            
            # Wait for modal to appear and extract data
            return self.extract_modal_data()
            
        except Exception as e:
            print(f"  ❌ Error clicking button: {e}")
            return None, None

    def extract_modal_data(self):
        """Extract episode count and seasons using user-specified selectors."""
        try:
            # Wait for modal
            print(f"  ⏳ Waiting for modal to load...")
            WebDriverWait(self.driver, MODAL_TIMEOUT).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.ipc-promptable-base"))
            )
            self.smart_delay(2, 0.5)  # Extra time for content to load
            
            # Extract episode count
            episode_count = self.extract_episode_count()
            
            # Extract seasons
            seasons = self.extract_seasons()
            
            # Close modal
            self.close_modal()
            
            return episode_count, seasons
            
        except TimeoutException:
            print(f"  ❌ Modal timeout")
            return None, None
        except Exception as e:
            print(f"  ❌ Modal extraction error: {e}")
            return None, None

    def extract_episode_count(self):
        """Extract episode count using specific selector."""
        print(f"  📊 Extracting episode count...")
        
        try:
            episode_element = self.driver.find_element(By.CSS_SELECTOR, MODAL_EPISODE_COUNT_SELECTOR)
            text = episode_element.text.strip()
            print(f"  📄 Episode element text: '{text}'")
            
            # Extract number from "309 episodes"
            match = re.search(r'(\d+)\s+episodes?', text, re.IGNORECASE)
            if match:
                episode_count = int(match.group(1))
                print(f"  ✅ Episode count: {episode_count}")
                return episode_count
            else:
                print(f"  ⚠️ No episode count pattern found in: '{text}'")
                return None
                
        except Exception as e:
            print(f"  ❌ Error extracting episode count: {e}")
            return None

    def extract_seasons(self):
        """Extract seasons from banner or episode markers."""
        print(f"  📅 Extracting seasons...")
        
        # Strategy 1: Try season banner
        try:
            season_banner = self.driver.find_element(By.CSS_SELECTOR, MODAL_SEASON_BANNER_SELECTOR)
            # Look for season tabs in the banner
            season_tabs = season_banner.find_elements(By.CSS_SELECTOR, "li[data-testid^='season-tab-']")
            
            if season_tabs:
                seasons = set()
                for tab in season_tabs:
                    test_id = tab.get_attribute("data-testid")
                    match = re.search(r'season-tab-(\d+)', test_id)
                    if match:
                        seasons.add(int(match.group(1)))
                
                if seasons:
                    seasons_str = ", ".join(str(s) for s in sorted(seasons))
                    print(f"  ✅ Seasons from banner: {seasons_str}")
                    return seasons_str
        except Exception as e:
            print(f"  ⚠️ No season banner found: {e}")
        
        # Strategy 2: Try episode marker for single season
        try:
            episode_marker = self.driver.find_element(By.CSS_SELECTOR, MODAL_EPISODE_MARKER_SELECTOR)
            marker_text = episode_marker.text.strip()
            print(f"  📄 Episode marker text: '{marker_text}'")
            
            # Look for season in marker like "S1.E1" or "Season 1"
            season_patterns = [r'S(\d+)\.E\d+', r'Season\s+(\d+)', r'S(\d+)']
            for pattern in season_patterns:
                match = re.search(pattern, marker_text, re.IGNORECASE)
                if match:
                    season = match.group(1)
                    print(f"  ✅ Season from marker: {season}")
                    return season
                    
        except Exception as e:
            print(f"  ⚠️ No episode marker found: {e}")
        
        print(f"  ❌ Could not determine seasons")
        return None

    def close_modal(self):
        """Close the modal."""
        try:
            # Try escape key first
            ActionChains(self.driver).send_keys(Keys.ESCAPE).perform()
            self.smart_delay(0.5, 0.3)
            print(f"  🔒 Modal closed")
        except Exception:
            print(f"  ⚠️ Could not close modal")

    def detect_crew_member(self, cast_name, cast_imdb_id=None):
        """Check if person is in crew sections."""
        crew_sections = ['Directed by', 'Produced by', 'Writing Credits']
        
        for section in crew_sections:
            try:
                section_xpath = f"//h4[contains(text(), '{section}')]"
                section_headers = self.driver.find_elements(By.XPATH, section_xpath)
                
                for header in section_headers:
                    if cast_imdb_id:
                        crew_xpath = f"./following-sibling::*//a[contains(@href, '{cast_imdb_id}')]"
                        if header.find_elements(By.XPATH, crew_xpath):
                            return True
            except Exception:
                continue
        return False

    # ----- Batch operations -----
    def flush_updates(self, force=False):
        if not self.updated_buffer or (not force and len(self.updated_buffer) < FLUSH_EVERY):
            return

        for row_num, ep_count, seasons in self.updated_buffer:
            try:
                self.sheet.update(f"G{row_num}:H{row_num}", [[str(ep_count), str(seasons)]], value_input_option="RAW")
                self.smart_delay(0.3, 0.2)
            except Exception as e:
                print(f"❌ Failed writing row {row_num}: {e}")
                self.errors += 1
        
        print(f"💾 Wrote {len(self.updated_buffer)} row(s) to CastInfo sheet")
        self.updated_buffer.clear()

    def delete_row(self, row_num):
        try:
            self.sheet.delete_rows(row_num)
            print(f"  🗑️ DELETED row {row_num} (crew member)")
            return True
        except Exception as e:
            print(f"  ❌ Failed to delete row {row_num}: {e}")
            return False

    # ----- Main extraction -----
    def extract_for_row(self, show_imdb_id, cast_name, cast_imdb_id=None):
        """Main extraction logic."""
        try:
            if not self.open_fullcredits_page(show_imdb_id):
                return None, None
            
            if self.detect_crew_member(cast_name, cast_imdb_id):
                return "DELETE_CREW", None
            
            episodes_button = self.find_cast_member_episodes_button(cast_name, cast_imdb_id)
            if not episodes_button:
                return None, None
            
            return self.click_episodes_button_and_extract(episodes_button)
            
        except Exception as e:
            print(f"  ❌ Extraction error: {e}")
            return None, None

    # ----- Main run method -----
    def run(self, start_row=2, limit=None):
        print("🚀 Starting CastInfo extraction with user-specified selectors")
        
        if not self.setup_google_sheets() or not self.setup_webdriver():
            return False

        try:
            targets = self.load_castinfo_rows(start_row, limit)
            if not targets:
                print("📭 No rows to process")
                return True

            for i, target in enumerate(targets, 1):
                row_num = target["row_num"]
                cast_name = target["cast_name"]
                cast_imdb_id = target["cast_imdb_id"]
                show_name = target["show_name"]
                show_imdb_id = target["show_imdb_id"]

                print(f"\n🎭 [{i}/{len(targets)}] Row {row_num} | {cast_name} | {show_name}")
                
                try:
                    episode_count, seasons = self.extract_for_row(show_imdb_id, cast_name, cast_imdb_id)
                    
                    if episode_count == "DELETE_CREW":
                        if self.delete_row(row_num):
                            self.deleted_crew += 1
                        continue
                    
                    if episode_count is not None and seasons is not None:
                        self.updated_buffer.append((row_num, episode_count, seasons))
                        self.processed_count += 1
                        print(f"  ✅ Episodes: {episode_count} | Seasons: {seasons}")
                        self.flush_updates()
                    elif episode_count is not None:
                        seasons = "1"  # Default
                        self.updated_buffer.append((row_num, episode_count, seasons))
                        self.processed_count += 1
                        print(f"  ✅ Episodes: {episode_count} | Seasons: {seasons} (default)")
                        self.flush_updates()
                    else:
                        print("  ⚠️ Could not extract data")
                        
                except KeyboardInterrupt:
                    print("\n⏹️ Interrupted")
                    break
                except Exception as e:
                    print(f"  ❌ Error: {e}")
                    self.errors += 1

                self.smart_delay(1.5, 1.0)

            self.flush_updates(force=True)

            print("\n🎉 Extraction complete!")
            print(f"  ✅ Updated: {self.processed_count}")
            print(f"  🗑️ Deleted crew: {self.deleted_crew}")
            print(f"  ⏭️ Skipped: {self.skipped_filled}")
            print(f"  ❌ Errors: {self.errors}")
            return True

        finally:
            if self.driver:
                self.driver.quit()


# ---------- Entry Point ----------
def main():
    try:
        extractor = v2UniversalSeasonExtractorCastInfoSpecific()
        
        # Test with first 10 rows to find one that needs processing
        start_row = 2
        limit = 10
        print(f"🎯 Testing with rows {start_row} to {start_row + limit - 1}")
        
        success = extractor.run(start_row, limit)
        print("✅ Script completed" if success else "❌ Script failed")
        
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
