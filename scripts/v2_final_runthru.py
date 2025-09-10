#!/usr/bin/env python3
# File: v2_final_runthru.py
"""
Name That Tune IMDb Episode/Season Extractor (Focused)
- ONLY processes Name That Tune (tt13491734) records using IMDb methods
- Uses detailed IMDb parsing for specific episode counts
- SKIPS all other shows (no meaningless defaults)
- Optimized for Google Sheets A        print("🎵 Name That Tune IMDb Extractor (focused) starting...")I rate limits
- Includes retry logic and resumption capability
"""

import os
import sys
import re
import time
import random
import json
from typing import Dict, Tuple, Optional, List

import requests
import gspread
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials


class NameThatTuneImdbExtractor:
    def __init__(self):
        self.sheet = None
        self.processed_rows = 0
        self.updated_cells = 0
        self.skipped_rows = 0
        self.errors = 0
        self.checkpoint_file = "/tmp/imdb_extractor_checkpoint.json"

        # credentials (choose the first that exists)
        possible_creds = [
            "/Users/thomashulihan/Projects/TRR-Backend/keys/trr-backend-df2c438612e1.json",
            "/Users/thomashulihan/Projects/TRR-Backend/keys/trr-backend-e16bfa49d861.json",
        ]
        self.service_account_file = next((p for p in possible_creds if os.path.exists(p)), None)
        if not self.service_account_file:
            print(f"❌ No credentials file found. Checked: {possible_creds}")

        # Target show for Name That Tune parsing
        self.show_imdb_id = "tt13491734"  # Name That Tune
        self.default_seasons = "1, 2, 3, 4"

        # Rate limiting settings
        self.batch_size = 50  # Reduced batch size for better rate limiting
        self.retry_delay = 30  # Wait 30 seconds on rate limit
        self.max_retries = 3

        # HTTP session with retries + headers
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.8",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })
        adapter = requests.adapters.HTTPAdapter(max_retries=3)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    # ---------- Setup ----------

    def load_checkpoint(self) -> int:
        """Load last processed row from checkpoint file."""
        try:
            if os.path.exists(self.checkpoint_file):
                with open(self.checkpoint_file, 'r') as f:
                    data = json.load(f)
                    return data.get('last_row', 1)
        except Exception as e:
            print(f"⚠️ Could not load checkpoint: {e}")
        return 1

    def save_checkpoint(self, row: int):
        """Save current progress to checkpoint file."""
        try:
            with open(self.checkpoint_file, 'w') as f:
                json.dump({'last_row': row, 'timestamp': time.time()}, f)
        except Exception as e:
            print(f"⚠️ Could not save checkpoint: {e}")

    def setup_google_sheets(self) -> bool:
        try:
            if not self.service_account_file or not os.path.exists(self.service_account_file):
                raise FileNotFoundError(f"Service account file not found: {self.service_account_file}")

            credentials = Credentials.from_service_account_file(
                self.service_account_file,
                scopes=[
                    "https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive",
                ],
            )
            gc = gspread.authorize(credentials)
            workbook = gc.open("Realitease2025Data")
            self.sheet = workbook.worksheet("ViableCast")
            print("✅ Google Sheets connected")
            return True
        except Exception as e:
            print(f"❌ Google Sheets setup failed: {e}")
            return False

    # ---------- IMDb scraping (fast) ----------

    def fetch_fullcredits(self) -> Optional[str]:
        """Fetch the full credits HTML once."""
        url = f"https://www.imdb.com/title/{self.show_imdb_id}/fullcredits"
        try:
            r = self.session.get(url, timeout=20)
            if r.status_code != 200:
                print(f"❌ fullcredits HTTP {r.status_code}")
                return None
            return r.text
        except Exception as e:
            print(f"❌ fullcredits request error: {e}")
            return None

    def _norm(self, s: str) -> str:
        return re.sub(r"\s+", " ", (s or "").strip().lower())

    def parse_episodes_map(self, html: str) -> Tuple[Dict[str, int], Dict[str, str]]:
        """
        Build two maps from the fullcredits page:
          - imdb_id -> episode_count
          - normalized_name -> imdb_id (to allow fallback by name)
        """
        soup = BeautifulSoup(html, "lxml")

        # Cover both classic table & newer list-based variants
        cast_blocks: List = []
        cast_blocks += soup.select("table.cast_list tr")  # classic
        cast_blocks += soup.select("[data-testid='sub-section-cast'] li")  # new

        id_to_episodes: Dict[str, int] = {}
        name_to_id: Dict[str, str] = {}

        def extract_id_from_href(href: str) -> Optional[str]:
            if not href:
                return None
            m = re.search(r"/name/(nm\d+)/", href)
            return m.group(1) if m else None

        for node in cast_blocks:
            # prefer anchor to name page
            a = None
            a = a or node.select_one("td.primary_photo a")
            a = a or node.select_one("td a[href*='/name/nm']")
            a = a or node.select_one("a[href*='/name/nm']")
            if not a:
                continue

            imdb_id = extract_id_from_href(a.get("href"))
            if not imdb_id:
                continue

            name_text = a.get_text(strip=True) or ""
            if name_text:
                name_to_id[self._norm(name_text)] = imdb_id

            # look for "N episodes" near this row/item
            node_text = " ".join(t.strip() for t in node.stripped_strings if t.strip())
            match = re.search(r"\b(\d+)\s+episodes?\b", node_text, flags=re.IGNORECASE)
            if match:
                try:
                    id_to_episodes[imdb_id] = int(match.group(1))
                except ValueError:
                    pass

        print(f"🔎 Parsed {len(id_to_episodes)} episode counts from fullcredits")
        print(f"📋 Also mapped {len(name_to_id)} names to IMDb IDs")
        return id_to_episodes, name_to_id

    # ---------- Sheet IO ----------

    def load_rows(self):
        """Fetch all rows once and detect columns by header names."""
        all_data = self.sheet.get_all_values()
        if not all_data:
            return [], {}

        headers = [h.strip() for h in all_data[0]]
        header_map = {h.lower(): idx for idx, h in enumerate(headers)}

        def idx_of(*contains):
            for k, idx in header_map.items():
                if all(c in k for c in contains):
                    return idx
            return None

        # Robust header detection (handles spaces & variants)
        col_show_imdb = idx_of("show", "imdbid") or idx_of("show", "imdb", "id")
        col_cast_name = idx_of("castname") or idx_of("cast", "name")
        col_cast_imdb = idx_of("cast", "imdbid") or idx_of("imdbid",) or idx_of("imdb", "id")
        col_episodes  = idx_of("episodecount") or idx_of("episode", "count")
        col_seasons   = idx_of("seasons") or idx_of("season")

        if None in (col_show_imdb, col_cast_name, col_cast_imdb, col_episodes, col_seasons):
            print("❌ Could not find required columns. Check header names.")
            print(f"Found headers: {header_map}")
            return [], {}

        meta = dict(
            col_show_imdb=col_show_imdb,
            col_cast_name=col_cast_name,
            col_cast_imdb=col_cast_imdb,
            col_episodes=col_episodes,
            col_seasons=col_seasons,
            n_rows=len(all_data),
            rows=all_data,
        )
        return all_data, meta

    def should_update_cell(self, existing: str) -> bool:
        """Check if a cell needs updating - only if truly empty or explicitly marked as SKIP"""
        if not existing:
            return True
        existing = existing.strip().upper()
        return existing in ("SKIP", "")

    @staticmethod
    def _col_letter(idx0: int) -> str:
        """0-based column index to A1 column letters."""
        n = idx0 + 1
        s = ""
        while n:
            n, r = divmod(n - 1, 26)
            s = chr(65 + r) + s
        return s

    def _flush_updates(self, grouped_ranges: List[Tuple[str, List[List[str]]]]):
        """Perform a series of contiguous range updates with retry logic."""
        for rng, vals in grouped_ranges:
            retry_count = 0
            while retry_count < self.max_retries:
                try:
                    # Use the new parameter order for gspread
                    self.sheet.update(values=vals, range_name=rng, value_input_option="RAW")
                    # Longer delay to respect rate limits
                    time.sleep(1.0 + random.random() * 0.5)
                    break
                except Exception as e:
                    if "429" in str(e) or "quota" in str(e).lower():
                        retry_count += 1
                        wait_time = self.retry_delay * (2 ** retry_count)  # Exponential backoff
                        print(f"⏳ Rate limit hit. Waiting {wait_time}s (attempt {retry_count}/{self.max_retries})")
                        time.sleep(wait_time)
                    else:
                        print(f"❌ Update error: {e}")
                        self.errors += 1
                        break
            else:
                print(f"❌ Failed to update {rng} after {self.max_retries} retries")
                self.errors += 1

    def batch_update_sparse(self, updates: Dict[str, str]) -> bool:
        """
        updates: { "G23": "5", "H23": "1, 2, 3, 4", "G104": "1" }
        Performs compact contiguous updates per column with improved error handling.
        Returns True if successful, False if rate limited.
        """
        if not updates:
            return True

        # Group by column → {row -> value}
        by_col: Dict[str, Dict[int, str]] = {}
        for a1, val in updates.items():
            m = re.match(r"([A-Z]+)(\d+)$", a1)
            if not m:
                continue
            col, row = m.group(1), int(m.group(2))
            by_col.setdefault(col, {})[row] = val

        grouped_ranges: List[Tuple[str, List[List[str]]]] = []

        for col, rowvals in by_col.items():
            rows_sorted = sorted(rowvals.keys())
            block = []
            start = prev = None
            for r in rows_sorted:
                if start is None:
                    start = prev = r
                    block = [r]
                elif r == prev + 1:
                    prev = r
                    block.append(r)
                else:
                    # flush previous block
                    values = [[rowvals[rr]] for rr in block]
                    grouped_ranges.append((f"{col}{block[0]}:{col}{block[-1]}", values))
                    # start new block
                    start = prev = r
                    block = [r]
            if block:
                values = [[rowvals[rr]] for rr in block]
                grouped_ranges.append((f"{col}{block[0]}:{col}{block[-1]}", values))

        # Execute minimal set of updates
        try:
            self._flush_updates(grouped_ranges)
            self.updated_cells += len(updates)
            return True
        except Exception as e:
            if "429" in str(e) or "quota" in str(e).lower():
                print(f"⚠️ Rate limit encountered during batch update")
                return False
            else:
                print(f"❌ Batch update failed: {e}")
                self.errors += 1
                return False

    # ---------- Main processing ----------

    def run(self) -> bool:
        print("� Universal IMDb Extractor (optimized) starting...")

        if not self.setup_google_sheets():
            return False

        # Load checkpoint to resume from last position
        start_row = self.load_checkpoint()
        if start_row > 1:
            print(f"📍 Resuming from row {start_row + 1}")

        rows, meta = self.load_rows()
        if not rows:
            return False

        col_show = meta["col_show_imdb"]
        col_name = meta["col_cast_name"]
        col_castid = meta["col_cast_imdb"]
        col_epi = meta["col_episodes"]
        col_sea = meta["col_seasons"]
        n_rows = meta["n_rows"]

        # Precompute A1 column letters for Episodes/Seasons columns
        epi_col_letter = self._col_letter(col_epi)
        sea_col_letter = self._col_letter(col_sea)

        # Pull the fullcredits page once for Name That Tune
        html = self.fetch_fullcredits()
        if not html:
            print("❌ Could not fetch IMDb full credits page for Name That Tune.")
            print("⚠️ Will use defaults for all Name That Tune entries.")
            id_to_eps, name_to_id = {}, {}
        else:
            id_to_eps, name_to_id = self.parse_episodes_map(html)

        # Debug: Show what we found
        print(f"📊 Total rows in sheet: {n_rows}")
        print(f"🎯 Processing ALL shows using IMDb methods")
        print(f"🔧 Rate limiting: {self.batch_size} updates per batch, {self.retry_delay}s retry delay")

        updates: Dict[str, str] = {}
        attempted_rows = 0

        # Iterate from start_row to last row - PROCESS ALL SHOWS
        for r in range(start_row, n_rows):
            row = rows[r]

            show_id = row[col_show] if len(row) > col_show else ""
            cast_name = row[col_name] if len(row) > col_name else ""
            cast_id   = row[col_castid] if len(row) > col_castid else ""
            epi_val   = row[col_epi] if len(row) > col_epi else ""
            sea_val   = row[col_sea] if len(row) > col_sea else ""

            # Debug output for progress tracking
            if attempted_rows < 10 or attempted_rows % 100 == 0:
                print(f"🔍 Row {r+1}: {cast_name} (Show: {show_id}) - Episodes: '{epi_val}' Seasons: '{sea_val}'")

            # Skip if both cells already have non-empty, non-SKIP values
            epi_needs_update = self.should_update_cell(epi_val)
            sea_needs_update = self.should_update_cell(sea_val)
            
            if not epi_needs_update and not sea_needs_update:
                self.skipped_rows += 1
                continue

            attempted_rows += 1

            # Show we're processing this record (for first few only)
            if self.processed_rows < 5:
                print(f"✅ Processing {cast_name} (Show: {show_id}) - Episodes need update: {epi_needs_update}, Seasons need update: {sea_needs_update}")

            # Decide updates (only if blank or 'SKIP')
            a1_epi = f"{epi_col_letter}{r+1}"
            a1_sea = f"{sea_col_letter}{r+1}"

            # Determine episode count and seasons based on show
            if show_id == self.show_imdb_id:
                # Name That Tune - use our parsed data
                episodes = None
                # 1) prefer by imdb id (exact)
                if cast_id and re.match(r"^nm\d+$", cast_id.strip()):
                    episodes = id_to_eps.get(cast_id.strip())
                # 2) fallback by normalized name
                if episodes is None and cast_name:
                    nid = name_to_id.get(self._norm(cast_name))
                    if nid:
                        episodes = id_to_eps.get(nid)
                # 3) default if not shown on fullcredits
                if episodes is None:
                    episodes = 1
                seasons = self.default_seasons
                
                # Only update if we actually have meaningful data
                if self.should_update_cell(epi_val):
                    updates[a1_epi] = str(episodes)
                if self.should_update_cell(sea_val):
                    updates[a1_sea] = seasons
            else:
                # Other shows - SKIP! Don't put meaningless defaults
                print(f"⏭️ Skipping {cast_name} (Show: {show_id}) - not Name That Tune, no generic data to add")
                self.skipped_rows += 1
                continue

            self.processed_rows += 1

            # Flush periodically with smaller batches and better error handling
            if len(updates) >= self.batch_size:
                success = self.batch_update_sparse(updates)
                if success:
                    updates.clear()
                    self.save_checkpoint(r)  # Save progress
                    # Longer delay between batches to respect rate limits
                    time.sleep(2.0 + random.random() * 1.0)
                else:
                    # Rate limit hit - wait longer and retry
                    print(f"⏳ Rate limit hit at row {r+1}. Waiting {self.retry_delay}s...")
                    time.sleep(self.retry_delay)
                    # Try again with same batch
                    success = self.batch_update_sparse(updates)
                    if success:
                        updates.clear()
                        self.save_checkpoint(r)
                    else:
                        print("❌ Still rate limited after retry. Stopping to avoid further issues.")
                        break

        # Final flush
        if updates:
            success = self.batch_update_sparse(updates)
            if success:
                self.save_checkpoint(n_rows - 1)  # Mark as complete
            else:
                print(f"⚠️ Final batch update failed due to rate limits")

        print("\n🎉 Done!")
        print(f"Processed rows: {self.processed_rows}")
        print(f"Cells updated (attempted): {self.updated_cells}")
        print(f"Skipped rows (already had values): {self.skipped_rows}")
        print(f"Errors: {self.errors}")
        
        # Clean up checkpoint file if completed successfully
        if self.errors == 0:
            try:
                os.remove(self.checkpoint_file)
                print("✅ Checkpoint file cleaned up")
            except:
                pass
                
        return True


def main():
    extractor = NameThatTuneImdbExtractor()
    ok = extractor.run()
    if ok:
        print("✅ Name That Tune IMDb extraction completed!")
    else:
        print("❌ Name That Tune IMDb extraction failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
