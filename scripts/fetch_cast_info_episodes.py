#!/usr/bin/env python3
"""
CastInfo Episode Count Updater
==============================

This script updates the TotalEpisodes column (Column G) in the CastInfo sheet
by fetching episode counts from multiple sources:

1. IMDbAPI.dev - Using filmography with "self" category to get episode counts
2. TMDb TV Credits API - Get episode counts from person's TV credits

The script processes existing CastInfo entries and fills in missing episode counts.

Usage: python fetch_cast_info_episodes.py [--dry-run] [--person-filter PERSON_NAME]
"""

import argparse
import os
import re
import time
from typing import Any, Dict, List, Optional, Tuple

import gspread
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
TMDB_BEARER = os.getenv("TMDB_BEARER")
REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "0.4"))

def _sleep():
    """Small throttle for respectful API usage."""
    time.sleep(REQUEST_DELAY)

class IMDbAPIClient:
    """Client for IMDbAPI.dev free service."""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Accept": "application/json",
        })
        self._cache: Dict[str, Any] = {}

    def get_person_filmography(self, imdb_id: str) -> Dict[str, Any]:
        """Get person filmography from IMDbAPI.dev using 'self' category."""
        if not imdb_id or not imdb_id.startswith("nm"):
            return {}
        
        cache_key = f"filmography_{imdb_id}"
        if cache_key in self._cache:
            return self._cache[cache_key]
        
        try:
            _sleep()
            # Use the correct API endpoint format from your testing
            url = f"https://api.imdbapi.dev/names/{imdb_id}/filmography"
            params = {
                "categories": "self",
                "pageSize": 50  # Get more results per page
            }
            response = self.session.get(url, params=params, timeout=15)
            
            if response.status_code == 200:
                data = response.json()
                self._cache[cache_key] = data
                print(f"      ✅ IMDb API returned {data.get('totalCount', 0)} total credits")
                return data
            else:
                print(f"      ⚠️ IMDbAPI filmography failed for {imdb_id}: HTTP {response.status_code}")
                if response.status_code == 404:
                    print(f"      📝 Response: {response.text[:200]}")
        except Exception as e:
            print(f"      ⚠️ IMDbAPI filmography failed for {imdb_id}: {e}")
        
        self._cache[cache_key] = {}
        return {}

    def get_all_shows_with_episodes(self, imdb_id: str) -> Dict[str, int]:
        """Get all TV shows and their episode counts for a person from IMDb API."""
        filmography = self.get_person_filmography(imdb_id)
        shows_episodes = {}
        
        if not filmography:
            return shows_episodes
        
        # The API response structure is: { "credits": [...], "totalCount": 95 }
        credits = filmography.get("credits", [])
        
        for credit in credits:
            # Extract show ID and episode count from the new structure
            show_id = None
            episode_count = 0
            
            # Get show ID from title.id
            if "title" in credit and isinstance(credit["title"], dict):
                show_id = credit["title"].get("id", "")
                # Only process TV series, not specials
                title_type = credit["title"].get("type", "")
                if title_type not in ["tvSeries"]:
                    continue
            
            # Extract episode count
            episode_count = credit.get("episodeCount", 0)
            
            if show_id and show_id.startswith("tt") and episode_count > 0:
                shows_episodes[show_id] = episode_count
                title = credit["title"].get("primaryTitle", show_id)
                print(f"      📺 IMDb: {title} ({show_id}) → {episode_count} episodes")
        
        return shows_episodes

    def extract_episode_count(self, filmography: Dict[str, Any], show_imdb_id: str) -> int:
        """Extract episode count for a specific show from filmography data."""
        if not filmography or not show_imdb_id:
            return 0
        
        # The API response structure is: { "credits": [...], "totalCount": 95 }
        credits = filmography.get("credits", [])
        
        for credit in credits:
            # Check if this credit matches our show
            credit_id = None
            
            # Get show ID from title.id
            if "title" in credit and isinstance(credit["title"], dict):
                credit_id = credit["title"].get("id", "")
            
            if credit_id == show_imdb_id:
                # Look for episode count
                episode_count = credit.get("episodeCount", 0)
                
                if episode_count > 0:
                    title = credit["title"].get("primaryTitle", show_imdb_id)
                    print(f"      📺 Found {episode_count} episodes from IMDbAPI for {title}")
                    return episode_count
        
        return 0

    def _parse_episode_count_from_credit(self, credit: Dict[str, Any]) -> int:
        """Parse episode count from a credit entry."""
        # Try different fields that might contain episode count
        for field in ["episodes", "episode_count", "episodeCount"]:
            if field in credit and isinstance(credit[field], (int, str)):
                try:
                    return int(credit[field])
                except (ValueError, TypeError):
                    continue
        
        # Look in description or notes for episode patterns
        for field in ["description", "notes", "character"]:
            if field in credit and isinstance(credit[field], str):
                text = credit[field]
                episode_count = self._extract_episode_from_text(text)
                if episode_count > 0:
                    return episode_count
        
        return 0

    def _extract_episode_from_text(self, text: str) -> int:
        """Extract episode count from text using regex patterns."""
        patterns = [
            r"(\d+)\s+episodes?",
            r"(\d+)\s+eps?",
            r"\((\d+)\s+episodes?\)",
            r"Episodes?\s*:\s*(\d+)",
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    return int(match.group(1))
                except (ValueError, IndexError):
                    continue
        
        return 0

class TMDbClient:
    """Enhanced TMDb client for TV credits."""
    
    def __init__(self, bearer: str):
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {bearer}",
            "accept": "application/json",
        })
        self._cache: Dict[str, Any] = {}

    def get_person_tv_credits(self, person_id: str) -> Dict[str, Any]:
        """Get person's TV credits from TMDb."""
        if not person_id:
            return {}
        
        cache_key = f"tv_credits_{person_id}"
        if cache_key in self._cache:
            return self._cache[cache_key]
        
        try:
            _sleep()
            url = f"https://api.themoviedb.org/3/person/{person_id}/tv_credits"
            response = self.session.get(url, timeout=20)
            response.raise_for_status()
            
            data = response.json()
            self._cache[cache_key] = data
            return data
        except Exception as e:
            print(f"      ⚠️ TMDb TV credits failed for person {person_id}: {e}")
        
        self._cache[cache_key] = {}
        return {}

    def get_all_shows_with_episodes(self, tv_credits: Dict[str, Any]) -> Dict[str, int]:
        """Get all TV shows and their episode counts from TV credits."""
        shows_episodes = {}
        
        if not tv_credits:
            return shows_episodes
        
        # Check both cast and crew
        for credit_type in ["cast", "crew"]:
            credits = tv_credits.get(credit_type, [])
            for credit in credits:
                show_id = str(credit.get("id", ""))
                episode_count = credit.get("episode_count", 0)
                if show_id and episode_count and episode_count > 0:
                    shows_episodes[show_id] = episode_count
        
        return shows_episodes

    def extract_episode_count_from_credits(self, tv_credits: Dict[str, Any], show_tmdb_id: str) -> int:
        """Extract episode count for a specific show from TV credits."""
        if not tv_credits or not show_tmdb_id:
            return 0
        
        # Check both cast and crew
        for credit_type in ["cast", "crew"]:
            credits = tv_credits.get(credit_type, [])
            for credit in credits:
                if str(credit.get("id", "")) == str(show_tmdb_id):
                    episode_count = credit.get("episode_count", 0)
                    if episode_count and episode_count > 0:
                        print(f"      📺 Found {episode_count} episodes from TMDb")
                        return episode_count
        
        return 0

def find_col_idx(header: List[str], patterns: List[str]) -> int:
    """Find column index by matching patterns."""
    for i, col in enumerate(header):
        low = (col or "").strip().lower()
        for p in patterns:
            if re.search(p, low):
                return i
    return -1

class EpisodeCountUpdater:
    def __init__(self):
        self.gc = gspread.service_account(
            filename=os.path.join(os.path.dirname(__file__), "..", "keys", "trr-backend-df2c438612e1.json")
        )
        self.sh = self.gc.open_by_key(SPREADSHEET_ID)
        self.tmdb = TMDbClient(TMDB_BEARER)
        self.imdb_api = IMDbAPIClient()

    def load_castinfo_entries(self, person_filter: Optional[str] = None) -> List[Tuple[int, Dict[str, str]]]:
        """Load CastInfo entries that need episode count updates."""
        try:
            ws = self.sh.worksheet("CastInfo")
            data = ws.get_all_values()
        except gspread.WorksheetNotFound:
            print("❌ CastInfo sheet not found!")
            return []
        
        if len(data) < 2:
            print("❌ CastInfo sheet has no data!")
            return []
        
        header = data[0]
        print(f"📊 CastInfo header: {header}")
        
        # Map column indices
        idx_cast_name = 0      # Column A - CastName
        idx_cast_id = 1        # Column B - CastID (TMDb person ID)
        idx_cast_imdb = 2      # Column C - Cast IMDbID
        idx_show_name = 3      # Column D - ShowName
        idx_show_imdb = 4      # Column E - Show IMDbID
        idx_show_id = 5        # Column F - ShowID (TMDb show ID)
        idx_episodes = 6       # Column G - TotalEpisodes
        
        entries_to_update = []
        
        for row_idx, row in enumerate(data[1:], start=2):  # Start at row 2 (1-indexed)
            if len(row) <= idx_episodes:
                continue
            
            cast_name = row[idx_cast_name] if idx_cast_name < len(row) else ""
            cast_id = row[idx_cast_id] if idx_cast_id < len(row) else ""
            cast_imdb = row[idx_cast_imdb] if idx_cast_imdb < len(row) else ""
            show_name = row[idx_show_name] if idx_show_name < len(row) else ""
            show_imdb = row[idx_show_imdb] if idx_show_imdb < len(row) else ""
            show_id = row[idx_show_id] if idx_show_id < len(row) else ""
            current_episodes = row[idx_episodes] if idx_episodes < len(row) else ""
            
            # Skip if already has episode count
            if current_episodes and current_episodes.strip() and current_episodes.strip() != "0":
                continue
            
            # Skip if missing essential data
            if not cast_name or not cast_id:
                continue
            
            # Apply person filter if specified
            if person_filter and person_filter.lower() not in cast_name.lower():
                continue
            
            entry = {
                "cast_name": cast_name,
                "cast_id": cast_id,
                "cast_imdb": cast_imdb,
                "show_name": show_name,
                "show_imdb": show_imdb,
                "show_id": show_id,
                "current_episodes": current_episodes
            }
            
            entries_to_update.append((row_idx, entry))
        
        print(f"📋 Found {len(entries_to_update)} entries needing episode count updates")
        return entries_to_update

    def get_all_episode_counts_for_person(self, cast_name: str, cast_id: str, cast_imdb: str) -> Dict[str, int]:
        """Get ALL episode counts for a person from both APIs."""
        print(f"  🔍 Getting ALL TV credits for {cast_name}")
        
        all_episodes = {}
        
        # Method 1: IMDbAPI.dev filmography - get ALL shows with episodes
        if cast_imdb:
            print(f"      📡 Checking IMDbAPI.dev for {cast_imdb}")
            imdb_shows = self.imdb_api.get_all_shows_with_episodes(cast_imdb)
            for show_imdb_id, episode_count in imdb_shows.items():
                all_episodes[show_imdb_id] = episode_count
                print(f"      📺 IMDb: {show_imdb_id} → {episode_count} episodes")
        
        # Method 2: TMDb TV credits - get ALL shows with episodes
        if cast_id:
            print(f"      📡 Checking TMDb for person {cast_id}")
            tmdb_credits = self.tmdb.get_person_tv_credits(cast_id)
            tmdb_shows = self.tmdb.get_all_shows_with_episodes(tmdb_credits)
            for show_tmdb_id, episode_count in tmdb_shows.items():
                # Convert TMDb ID to key format
                tmdb_key = f"tmdb_{show_tmdb_id}"
                if tmdb_key not in all_episodes:  # Don't override IMDb data
                    all_episodes[tmdb_key] = episode_count
                    print(f"      📺 TMDb: {show_tmdb_id} → {episode_count} episodes")
        
        return all_episodes

    def get_episode_count(self, entry: Dict[str, str]) -> int:
        """Get episode count using multiple sources."""
        cast_name = entry["cast_name"]
        cast_id = entry["cast_id"]
        cast_imdb = entry["cast_imdb"]
        show_name = entry["show_name"]
        show_imdb = entry["show_imdb"]
        show_id = entry["show_id"]
        
        print(f"  🔍 Getting episodes for {cast_name} in {show_name}")
        
        episode_count = 0
        
        # Method 1: IMDbAPI.dev filmography
        if cast_imdb and show_imdb:
            filmography = self.imdb_api.get_person_filmography(cast_imdb)
            episode_count = self.imdb_api.extract_episode_count(filmography, show_imdb)
        
        # Method 2: TMDb TV credits (if IMDb didn't work)
        if episode_count == 0 and cast_id and show_id:
            tv_credits = self.tmdb.get_person_tv_credits(cast_id)
            episode_count = self.tmdb.extract_episode_count_from_credits(tv_credits, show_id)
        
        if episode_count > 0:
            print(f"    ✅ Found {episode_count} episodes")
        else:
            print(f"    ❌ No episode count found")
        
        return episode_count

    def update_episode_counts(self, updates: List[Tuple[int, int, str]], dry_run: bool = False):
        """Update episode counts in the CastInfo sheet."""
        if not updates:
            return
        
        print(f"\n📝 Updating {len(updates)} episode counts...")
        
        if dry_run:
            print("🔍 DRY RUN - Would update:")
            for row_num, episode_count, cast_name in updates[:10]:
                print(f"  Row {row_num}: {cast_name} → {episode_count} episodes")
            return
        
        try:
            ws = self.sh.worksheet("CastInfo")
            
            # Batch update the TotalEpisodes column (column G)
            batch_data = []
            for row_num, episode_count, cast_name in updates:
                range_name = f"G{row_num}"
                batch_data.append({
                    "range": range_name,
                    "values": [[str(episode_count)]]
                })
                print(f"   🔄 Row {row_num}: {cast_name} → {episode_count} episodes")
            
            ws.batch_update(batch_data, value_input_option="RAW")
            print("✅ Episode count updates completed.")
            
        except Exception as e:
            print(f"❌ Batch update failed: {e}")
            # Fallback to individual updates
            for row_num, episode_count, cast_name in updates:
                try:
                    ws.update(values=[[str(episode_count)]], range_name=f"G{row_num}")
                    print(f"   ✅ Updated row {row_num}: {cast_name}")
                except Exception as e2:
                    print(f"   ❌ Failed to update row {row_num}: {e2}")

    def run_episode_update(self, person_filter: Optional[str] = None, dry_run: bool = False):
        """Main process to update episode counts."""
        print("🚀 Starting episode count update process")
        print("=" * 60)
        
        # Load entries that need updates
        entries = self.load_castinfo_entries(person_filter)
        if not entries:
            print("ℹ️  No entries need episode count updates.")
            return
        
        # Group entries by person to batch process
        person_entries = {}
        for row_num, entry in entries:
            person_key = f"{entry['cast_name']}_{entry['cast_id']}_{entry['cast_imdb']}"
            if person_key not in person_entries:
                person_entries[person_key] = {
                    'person_data': entry,
                    'entries': []
                }
            person_entries[person_key]['entries'].append((row_num, entry))
        
        updates: List[Tuple[int, int, str]] = []
        processed_people = 0
        total_people = len(person_entries)
        
        for person_key, person_info in person_entries.items():
            processed_people += 1
            person_data = person_info['person_data']
            person_entries_list = person_info['entries']
            
            cast_name = person_data['cast_name']
            cast_id = person_data['cast_id']
            cast_imdb = person_data['cast_imdb']
            
            print(f"\n� Processing person {processed_people}/{total_people}: {cast_name}")
            print(f"   📝 Found {len(person_entries_list)} shows for this person")
            
            # Get ALL episode counts for this person
            all_episodes = self.get_all_episode_counts_for_person(cast_name, cast_id, cast_imdb)
            
            # Match episodes to specific show entries
            person_updates = 0
            for row_num, entry in person_entries_list:
                show_name = entry['show_name']
                show_imdb = entry['show_imdb']
                show_id = entry['show_id']
                
                episode_count = 0
                
                # Try to match by IMDb ID first
                if show_imdb and show_imdb in all_episodes:
                    episode_count = all_episodes[show_imdb]
                    print(f"      ✅ {show_name}: {episode_count} episodes (IMDb match)")
                
                # Try to match by TMDb ID
                elif show_id and f"tmdb_{show_id}" in all_episodes:
                    episode_count = all_episodes[f"tmdb_{show_id}"]
                    print(f"      ✅ {show_name}: {episode_count} episodes (TMDb match)")
                
                # Fallback to individual lookup for this specific show
                elif not episode_count:
                    print(f"      🔍 No bulk match for {show_name}, trying individual lookup...")
                    episode_count = self.get_episode_count(entry)
                
                if episode_count > 0:
                    updates.append((row_num, episode_count, f"{cast_name} - {show_name}"))
                    person_updates += 1
                else:
                    print(f"      ❌ No episodes found for {show_name}")
            
            print(f"   📊 Updated {person_updates}/{len(person_entries_list)} shows for {cast_name}")
            
            # Update in batches of 50 to avoid overwhelming the sheet
            if len(updates) >= 50:
                self.update_episode_counts(updates, dry_run)
                updates = []
        
        # Update remaining entries
        if updates:
            self.update_episode_counts(updates, dry_run)
        
        print(f"\n✅ Episode count update process complete!")
        print(f"📊 Processed: {processed_people} people")
        print(f"📊 Updated: {len([u for u in updates if u[1] > 0])} episode counts")

def main():
    parser = argparse.ArgumentParser(description="Update episode counts in CastInfo sheet")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without writing")
    parser.add_argument("--person-filter", help="Filter to specific person name (partial match)")
    args = parser.parse_args()

    updater = EpisodeCountUpdater()
    updater.run_episode_update(
        person_filter=args.person_filter, 
        dry_run=args.dry_run
    )

if __name__ == "__main__":
    main()
