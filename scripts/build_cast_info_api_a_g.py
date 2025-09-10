#!/usr/bin/env python3
"""
CastInfo Data Collection Script - API Enhanced Version for A-G Shows
Enhanced version of fetch_cast_info_simple.py that:
1. Processes only shows A through G
2. Uses IMDbAPI.dev for accurate episode counts
3. Maintains existing structure and logic
"""

import argparse
import os
import re
import time
from typing import Any, Dict, List, Optional, Set, Tuple

import gspread
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Load environment variables
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
TMDB_BEARER = os.getenv("TMDB_BEARER")
REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "0.4"))

def _sleep():
    """Small throttle for respectful API usage."""
    time.sleep(REQUEST_DELAY)

def normalize_person_name(s: str) -> str:
    """Loose normalization for name matching across sites."""
    s = s or ""
    s = s.replace("'", "'").replace("`", "'")
    s = re.sub(r"\(.*?\)", "", s)  # drop parentheses
    s = re.sub(r"[^a-z0-9]+", " ", s.lower()).strip()
    return " ".join(s.split())

def best_token_ratio(a: str, b: str) -> float:
    """Simple token similarity (0..1)."""
    ta = set(normalize_person_name(a).split())
    tb = set(normalize_person_name(b).split())
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / len(ta | tb)

class IMDbAPIClient:
    """Client for IMDbAPI.dev to get accurate episode counts."""
    
    def __init__(self):
        self.base_url = "https://api.imdbapi.dev"
        self.session = requests.Session()
        self._cache = {}  # Cache episode counts by title_id
        
    def get_episode_count_for_person(self, title_id: str, person_imdb_id: str) -> int:
        """Get episode count for a specific person on a show."""
        if not title_id or not person_imdb_id:
            return 0
            
        # Check cache first
        cache_key = f"{title_id}:{person_imdb_id}"
        if cache_key in self._cache:
            return self._cache[cache_key]
            
        try:
            # Get all cast credits for this title
            cast_data = self._get_all_cast_credits(title_id)
            
            # Find this person's episode count
            episode_count = cast_data.get(person_imdb_id, {}).get("episodes", 0)
            
            # Cache the result
            self._cache[cache_key] = episode_count
            
            return episode_count
            
        except Exception as e:
            print(f"    ⚠️  API error for {person_imdb_id}: {e}")
            return 0
    
    def _get_all_cast_credits(self, title_id: str) -> Dict[str, dict]:
        """Get all cast credits for a title (with caching)."""
        if title_id in self._cache:
            return self._cache[title_id]
            
        cast_data = {}
        next_token = None
        
        try:
            while True:
                # Build URL
                url = f"{self.base_url}/titles/{title_id}/credits"
                params = {"categories": "self"}  # Reality TV contestants
                if next_token:
                    params["pageToken"] = next_token
                    
                response = self.session.get(url, params=params)
                response.raise_for_status()
                data = response.json()
                
                # Process credits
                credits = data.get("credits", [])
                for credit in credits:
                    name_data = credit.get("name", {})
                    name_id = name_data.get("id", "")
                    
                    if name_id and name_id.startswith("nm"):
                        cast_data[name_id] = {
                            "name": name_data.get("displayName", ""),
                            "episodes": credit.get("episodeCount", 0),
                            "characters": credit.get("characters", []),
                        }
                
                # Check for next page
                next_token = data.get("nextPageToken")
                if not next_token:
                    break
                    
                # Rate limiting
                time.sleep(0.5)
                
        except Exception as e:
            print(f"    ⚠️  Failed to fetch cast for {title_id}: {e}")
        
        # Cache the complete result
        self._cache[title_id] = cast_data
        return cast_data

class TMDbAPI:
    """Simple TMDb API wrapper."""
    
    def __init__(self, bearer_token: str):
        self.bearer = bearer_token
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {bearer_token}"})

    def tv_aggregate(self, tmdb_id: str) -> Dict[str, Any]:
        """Get aggregate credits for a TV show."""
        resp = self.session.get(f"https://api.themoviedb.org/3/tv/{tmdb_id}/aggregate_credits")
        resp.raise_for_status()
        _sleep()
        return resp.json()

    def tv_details(self, tmdb_id: str) -> Dict[str, Any]:
        """Get basic details for a TV show."""
        resp = self.session.get(f"https://api.themoviedb.org/3/tv/{tmdb_id}")
        resp.raise_for_status()
        _sleep()
        return resp.json()

    def person_external_ids(self, person_id: str) -> Dict[str, Any]:
        """Get external IDs for a person."""
        resp = self.session.get(f"https://api.themoviedb.org/3/person/{person_id}/external_ids")
        resp.raise_for_status()
        _sleep()
        return resp.json()

class PersonMapper:
    """Maps TMDb person IDs to IMDb IDs using various sources."""
    
    def __init__(self, tmdb: TMDbAPI, imdb_api: IMDbAPIClient):
        self.tmdb = tmdb
        self.imdb_api = imdb_api
        self.session = requests.Session()

    def resolve_imdb_id(self, tmdb_person: str, tmdb_name: str, show_imdb_id: str = "") -> Tuple[str, str]:
        """Resolve TMDb person to IMDb ID and canonical name."""
        
        # Try TMDb external IDs first
        try:
            ext_ids = self.tmdb.person_external_ids(tmdb_person)
            imdb_id = ext_ids.get("imdb_id", "").strip()
            if imdb_id and imdb_id.startswith("nm"):
                print(f"    ✅ TMDb→IMDb: {tmdb_name} → {imdb_id}")
                return imdb_id, tmdb_name
        except Exception as e:
            print(f"    ⚠️  TMDb external IDs failed for {tmdb_name}: {e}")

        # If no IMDb ID found, return what we have
        print(f"    ❌ No IMDb ID found for: {tmdb_name}")
        return "", tmdb_name

class CastInfoBuilder:
    """Main builder class for CastInfo data."""
    
    def __init__(self):
        if not TMDB_BEARER:
            raise ValueError("TMDB_BEARER not set")
        if not SPREADSHEET_ID:
            raise ValueError("SPREADSHEET_ID not set")
        
        self.tmdb = TMDbAPI(TMDB_BEARER)
        self.imdb_api = IMDbAPIClient()
        self.person_mapper = PersonMapper(self.tmdb, self.imdb_api)
        
        # Connect to Google Sheets
        gc = gspread.service_account(filename="/Users/thomashulihan/Projects/TRR-Backend/keys/trr-backend-df2c438612e1.json")
        self.ss = gc.open_by_key(SPREADSHEET_ID)

    def load_show_info(self) -> Dict[str, Dict[str, str]]:
        """Load show information from ShowInfo sheet, filtering for A-G only."""
        try:
            ws = self.ss.worksheet("ShowInfo")
            rows = ws.get_all_values()
        except gspread.exceptions.WorksheetNotFound:
            print("❌ ShowInfo sheet not found")
            return {}

        if not rows:
            return {}

        headers = rows[0]
        shows = {}
        
        for row in rows[1:]:
            if len(row) < len(headers):
                continue
                
            data = dict(zip(headers, row))
            tmdb_id = data.get("TheMovieDB ID", "").strip()
            show_name = data.get("ShowName", "").strip()
            
            if not tmdb_id or not show_name:
                continue
                
            # Filter for shows A through G only
            first_letter = show_name[0].upper()
            if first_letter < 'A' or first_letter > 'G':
                continue
                
            shows[tmdb_id] = {
                "name": show_name,
                "imdb_id": data.get("IMDbSeriesID", "").strip()
            }
        
        print(f"📺 Loaded {len(shows)} shows A-G from ShowInfo")
        return shows

    def ensure_castinfo_headers(self) -> gspread.Worksheet:
        """Get or create CastInfo sheet with proper headers."""
        try:
            ws = self.ss.worksheet("CastInfo")
        except gspread.exceptions.WorksheetNotFound:
            print("📄 Creating CastInfo sheet...")
            ws = self.ss.add_worksheet("CastInfo", rows=1000, cols=8)

        # Ensure headers
        headers = ["CastName", "CastID", "Cast IMDbID", "ShowName", "Show IMDbID", "ShowID", "TotalEpisodes", "TotalSeasons"]
        try:
            current_headers = ws.row_values(1)
            if current_headers != headers:
                print("📝 Setting CastInfo headers...")
                ws.update("A1:H1", [headers])
        except Exception:
            print("📝 Setting CastInfo headers...")
            ws.update("A1:H1", [headers])

        return ws

    def existing_pairs_with_rows(self) -> Tuple[Set[Tuple[str, str]], Dict[Tuple[str, str], int]]:
        """Get existing (CastID, ShowID) pairs and track rows missing IMDb IDs."""
        ws = self.ensure_castinfo_headers()
        
        try:
            all_values = ws.get_all_values()
        except Exception as e:
            print(f"❌ Error reading CastInfo: {e}")
            return set(), {}

        filled_pairs = set()
        missing_imdb_rows = {}

        for row_idx, row in enumerate(all_values[1:], start=2):  # Start from row 2
            if len(row) < 6:
                continue
                
            cast_id = row[1].strip()     # Column B - CastID
            show_id = row[5].strip()     # Column F - ShowID  
            cast_imdb_id = row[2].strip() # Column C - Cast IMDbID
            
            if cast_id and show_id:
                pair = (cast_id, show_id)
                if cast_imdb_id:
                    filled_pairs.add(pair)
                else:
                    missing_imdb_rows[pair] = row_idx

        print(f"📊 Existing: {len(filled_pairs)} filled, {len(missing_imdb_rows)} missing IMDb IDs")
        return filled_pairs, missing_imdb_rows

    def build_rows_for_show(
        self,
        tmdb_id: str,
        show_imdb_id: str,
        canonical_showname: str,
        filled_pairs: Set[Tuple[str, str]],
        missing_pairs: Set[Tuple[str, str]],
    ) -> List[List[str]]:
        """Build cast rows for a single show with API episode counts."""
        show_title = canonical_showname or ""
        rows: List[List[str]] = []

        try:
            agg = self.tmdb.tv_aggregate(tmdb_id)
            details = self.tmdb.tv_details(tmdb_id)
            total_seasons = str(details.get("number_of_seasons", ""))
        except Exception as e:
            print(f"❌ TMDb data failed for {tmdb_id}: {e}")
            return rows

        cast_list = agg.get("cast", []) or []
        print(f"  👥 TMDb cast count: {len(cast_list)}")
        
        # Get episode counts from API if we have show IMDb ID
        episode_counts = {}
        if show_imdb_id:
            print(f"  🔍 Fetching episode counts from API...")
            try:
                api_cast_data = self.imdb_api._get_all_cast_credits(show_imdb_id)
                print(f"  📊 API returned {len(api_cast_data)} cast members with episode data")
            except Exception as e:
                print(f"  ⚠️  API fetch failed: {e}")
                api_cast_data = {}
        else:
            api_cast_data = {}

        for m in cast_list:
            tmdb_person = str(m.get("id") or "")
            tmdb_name = m.get("name") or ""
            
            if not tmdb_person or not tmdb_name:
                continue

            pair = (tmdb_person, tmdb_id)

            # Skip if this pair already has an IMDb ID
            if pair in filled_pairs:
                print(f"    ⏭️  Skip (already has IMDb ID): {tmdb_name}")
                continue

            # Resolve IMDb ID
            imdb_id, cast_name = self.person_mapper.resolve_imdb_id(tmdb_person, tmdb_name, show_imdb_id)

            # Get episode count from API if we have IMDb ID
            episode_count = ""
            if imdb_id and api_cast_data:
                episodes = api_cast_data.get(imdb_id, {}).get("episodes", 0)
                if episodes > 0:
                    episode_count = str(episodes)
                    print(f"    📺 {cast_name}: {episodes} episodes")

            # Build row
            row = [
                cast_name,              # A CastName
                tmdb_person,            # B CastID (TMDb person ID)
                imdb_id,                # C Cast IMDbID
                show_title,             # D ShowName
                show_imdb_id or "",     # E Show IMDbID
                tmdb_id,                # F ShowID (TMDb)
                episode_count,          # G TotalEpisodes (from API!)
                total_seasons,          # H TotalSeasons
            ]
            rows.append(row)

        print(f"  ➕ Built {len(rows)} rows for {show_title}")
        return rows

    def update_missing_imdb_ids(self, ws: gspread.Worksheet, updates: List[Tuple[int, str, str]]):
        """Update missing IMDb IDs in existing rows."""
        if not updates:
            return
            
        print(f"  🔄 Updating {len(updates)} IMDb IDs...")
        
        for row_num, imdb_id, cast_name in updates:
            try:
                ws.update_cell(row_num, 3, imdb_id)  # Column C - Cast IMDbID
                print(f"    ✅ Updated row {row_num}: {cast_name} → {imdb_id}")
                time.sleep(0.5)  # Rate limiting
            except Exception as e:
                print(f"    ❌ Failed to update row {row_num}: {e}")

    def append_rows(self, ws: gspread.Worksheet, rows: List[List[str]]):
        """Append new rows to the sheet."""
        if not rows:
            return
            
        try:
            ws.append_rows(rows)
            print(f"✅ Appended {len(rows)} new rows")
        except Exception as e:
            print(f"❌ Failed to append rows: {e}")

    def run_build(self, dry_run: bool):
        """Main build process for shows A-G."""
        ws = self.ensure_castinfo_headers()
        filled_pairs, missing_imdb_rows = self.existing_pairs_with_rows()
        shows = self.load_show_info()

        items: List[Tuple[str, Dict[str, str]]] = list(shows.items())
        print(f"🔍 Will process {len(items)} shows A-G")

        all_new: List[List[str]] = []
        
        for tmdb_id, info in items:
            show_imdb_id = info.get("imdb_id", "").strip()
            show_name = info.get("name", "")
            print(f"\n🎭 Processing: {show_name} (TMDb {tmdb_id}, IMDb {show_imdb_id or '—'})")
            
            rows = self.build_rows_for_show(
                tmdb_id,
                show_imdb_id,
                show_name,
                filled_pairs=filled_pairs,
                missing_pairs=set(missing_imdb_rows.keys())
            )
            
            imdb_updates: List[Tuple[int, str, str]] = []

            # Process each row
            for r in rows:
                cast_id = r[1]   # Column B - CastID
                show_id = r[5]   # Column F - ShowID
                cast_name = r[0] # Column A - CastName
                new_imdb_id = r[2] # Column C - Cast IMDbID
                pair = (cast_id, show_id)
                
                if cast_id and show_id:
                    if pair in filled_pairs:
                        print(f"      ⏭️  Has IMDb ID: {cast_name}")
                    elif pair in missing_imdb_rows:
                        if new_imdb_id:
                            row_num = missing_imdb_rows[pair]
                            imdb_updates.append((row_num, new_imdb_id, cast_name))
                            print(f"      🔄 Will update IMDb ID: {cast_name} → {new_imdb_id}")
                            filled_pairs.add(pair)
                            del missing_imdb_rows[pair]
                        else:
                            print(f"      ❌ Still no IMDb ID found: {cast_name}")
                    else:
                        all_new.append(r)
                        print(f"      ➕ New: {cast_name}")

            # Update IMDb IDs for this show
            if not dry_run and imdb_updates:
                self.update_missing_imdb_ids(ws, imdb_updates)

        # Add new rows
        if not dry_run and all_new:
            print(f"\n📝 Adding {len(all_new)} new rows...")
            self.append_rows(ws, all_new)
            print("✅ New rows added.")
        elif dry_run:
            print("🔍 DRY RUN — not writing.")
            if all_new:
                print(f"\n📊 Would add {len(all_new)} new rows")

        if not all_new:
            print("\nℹ️  No new entries to add.")

def main():
    parser = argparse.ArgumentParser(description="Build CastInfo for shows A-G with API episode counts.")
    parser.add_argument("--mode", choices=["build"], default="build", help="Operation mode")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without writing")
    args = parser.parse_args()

    builder = CastInfoBuilder()
    if args.mode == "build":
        builder.run_build(dry_run=args.dry_run)

if __name__ == "__main__":
    main()
