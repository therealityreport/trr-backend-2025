#!/usr/bin/env python3
"""
CastInfo Data Collection Script - Simplified Version (v2 runner: no filters, bottom-up updates)
Intended filename: v2_fetch_cast_info_simple.py

This script builds comprehensive cast information using multiple ID sources:
- TMDb API for cast data and external IDs
- IMDbAPI.dev for scraping IMDb IDs
- Wikidata for additional ID resolution
- IMDb direct scraping as fallback

Focus is on populating all cast members with proper IDs rather than episode counts.
This is the v2 runner: processes ALL shows (no --show-filter) and updates rows bottom-up.
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
    """Client for IMDbAPI.dev free service."""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Accept": "application/json",
        })
        self._cache: Dict[str, Any] = {}

    def search_person(self, name: str) -> List[Dict[str, Any]]:
        """Search for person using IMDbAPI.dev."""
        if not name:
            return []
        
        cache_key = f"search_{normalize_person_name(name)}"
        if cache_key in self._cache:
            return self._cache[cache_key]
        
        try:
            _sleep()
            url = f"https://imdbapi.dev/search/person"
            params = {"name": name}
            response = self.session.get(url, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                results = data.get("results", [])[:5]  # Limit to top 5
                self._cache[cache_key] = results
                return results
        except Exception as e:
            print(f"⚠️  IMDbAPI search failed for '{name}': {e}")
        
        self._cache[cache_key] = []
        return []

    def get_person_details(self, imdb_id: str) -> Dict[str, Any]:
        """Get person details from IMDbAPI.dev."""
        if not imdb_id or not imdb_id.startswith("nm"):
            return {}
        
        if imdb_id in self._cache:
            return self._cache[imdb_id]
        
        try:
            _sleep()
            url = f"https://imdbapi.dev/person/{imdb_id}"
            response = self.session.get(url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                self._cache[imdb_id] = data
                return data
        except Exception as e:
            print(f"⚠️  IMDbAPI details failed for '{imdb_id}': {e}")
        
        self._cache[imdb_id] = {}
        return {}

class WikidataClient:
    """Client for Wikidata SPARQL queries."""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "TRR-Backend-CastInfo/1.0",
            "Accept": "application/json",
        })
        self._cache: Dict[str, str] = {}

    def get_imdb_from_tmdb(self, tmdb_person_id: str) -> str:
        """Get IMDb ID from Wikidata using TMDb person ID."""
        if not tmdb_person_id:
            return ""
        
        cache_key = f"tmdb_{tmdb_person_id}"
        if cache_key in self._cache:
            return self._cache[cache_key]
        
        # SPARQL query to find IMDb ID using TMDb ID
        query = f"""
        SELECT ?imdbId WHERE {{
          ?person wdt:P4985 "{tmdb_person_id}" .
          ?person wdt:P345 ?imdbId .
        }}
        LIMIT 1
        """
        
        try:
            _sleep()
            url = "https://query.wikidata.org/sparql"
            params = {
                "query": query,
                "format": "json"
            }
            response = self.session.get(url, params=params, timeout=15)
            
            if response.status_code == 200:
                data = response.json()
                bindings = data.get("results", {}).get("bindings", [])
                if bindings:
                    imdb_id = bindings[0].get("imdbId", {}).get("value", "")
                    if imdb_id and not imdb_id.startswith("nm"):
                        imdb_id = f"nm{imdb_id}"
                    self._cache[cache_key] = imdb_id
                    return imdb_id
        except Exception as e:
            print(f"   ⚠️ Wikidata query failed for TMDb {tmdb_person_id}: {e}")
        
        self._cache[cache_key] = ""
        return ""

class TMDbClient:
    """Enhanced TMDb client with external IDs support."""
    
    def __init__(self, bearer: str):
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {bearer}",
            "accept": "application/json",
        })

    def tv_aggregate(self, tv_id: str) -> Dict[str, Any]:
        _sleep()
        r = self.session.get(f"https://api.themoviedb.org/3/tv/{tv_id}/aggregate_credits", timeout=20)
        r.raise_for_status()
        return r.json()

    def tv_details(self, tv_id: str) -> Dict[str, Any]:
        _sleep()
        r = self.session.get(f"https://api.themoviedb.org/3/tv/{tv_id}", timeout=20)
        r.raise_for_status()
        return r.json()

    def person_external_ids(self, person_id: str) -> Dict[str, Any]:
        """Get external IDs for a person from TMDb."""
        _sleep()
        r = self.session.get(f"https://api.themoviedb.org/3/person/{person_id}/external_ids", timeout=20)
        r.raise_for_status()
        return r.json()

    def person_details(self, person_id: str) -> Dict[str, Any]:
        """Get person details from TMDb."""
        _sleep()
        r = self.session.get(f"https://api.themoviedb.org/3/person/{person_id}", timeout=20)
        r.raise_for_status()
        return r.json()

class IMDbScraper:
    """Fallback IMDb scraper for direct access."""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
        })
        self._cache: Dict[str, str] = {}

    def search_person(self, name: str) -> List[str]:
        """Search for person on IMDb and return nm IDs."""
        if not name:
            return []
        
        cache_key = f"search_{normalize_person_name(name)}"
        if cache_key in self._cache:
            return self._cache[cache_key].split(",") if self._cache[cache_key] else []
        
        try:
            _sleep()
            url = f"https://www.imdb.com/find/?s=nm&q={requests.utils.quote(name)}"
            response = self.session.get(url, timeout=15)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, "html.parser")
                ids = []
                for a in soup.find_all("a", href=re.compile(r"/name/(nm\d+)/"))[:5]:
                    m = re.search(r"/name/(nm\d+)/", a.get("href", ""))
                    if m:
                        ids.append(m.group(1))
                
                # Dedupe while preserving order
                ids = list(dict.fromkeys(ids))
                self._cache[cache_key] = ",".join(ids)
                return ids
        except Exception as e:
            print(f"⚠️  IMDb search failed for '{name}': {e}")
        
        self._cache[cache_key] = ""
        return []

    def get_person_name(self, imdb_id: str) -> str:
        """Get official name from IMDb person page."""
        if not imdb_id or not imdb_id.startswith("nm"):
            return ""
        
        if imdb_id in self._cache:
            return self._cache[imdb_id]
        
        try:
            _sleep()
            url = f"https://www.imdb.com/name/{imdb_id}/"
            response = self.session.get(url, timeout=15)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, "html.parser")
                for sel in [
                    'h1[data-testid="hero__pageTitle"] span',
                    'h1 span[itemprop="name"]',
                    'h1 span',
                ]:
                    el = soup.select_one(sel)
                    if el:
                        name = el.get_text(strip=True)
                        self._cache[imdb_id] = name
                        return name
        except Exception as e:
            print(f"⚠️  IMDb name fetch failed for '{imdb_id}': {e}")
        
        self._cache[imdb_id] = ""
        return ""

def find_col_idx(header: List[str], patterns: List[str]) -> int:
    """Find column index by matching patterns."""
    for i, col in enumerate(header):
        low = (col or "").strip().lower()
        for p in patterns:
            if re.search(p, low):
                return i
    return -1

class CastInfoBuilder:
    def __init__(self):
        self.gc = gspread.service_account(
            filename=os.path.join(os.path.dirname(__file__), "..", "keys", "trr-backend-df2c438612e1.json")
        )
        self.sh = self.gc.open_by_key(SPREADSHEET_ID)
        self.tmdb = TMDbClient(TMDB_BEARER)
        self.imdb_api = IMDbAPIClient()
        self.wikidata = WikidataClient()
        self.imdb_scraper = IMDbScraper()

    def load_show_info(self) -> Dict[str, Dict[str, str]]:
        """Load show information from ShowInfo sheet."""
        ws = self.sh.worksheet("ShowInfo")
        rows = ws.get_all_values()
        if not rows or len(rows) < 2:
            return {}
        
        header = rows[0]
        print(f"📊 ShowInfo header: {header}")

        # Find the correct columns based on the actual structure
        idx_tmdb = find_col_idx(header, [r"\bthemoviedb\b", r"\btmdb\b"])
        idx_imdb = find_col_idx(header, [r"\bimdbseriesid\b", r"\bimdb.*series\b"])
        idx_name = find_col_idx(header, [r"^\s*showname\s*$", r"\btitle\b"])
        idx_nick = find_col_idx(header, [r"^\s*show\s*$"])

        print(f"📍 Column indices: TMDb={idx_tmdb}, IMDb={idx_imdb}, Name={idx_name}, Nick={idx_nick}")

        out: Dict[str, Dict[str, str]] = {}
        for r in rows[1:]:
            tmdb_id = (r[idx_tmdb] if idx_tmdb >= 0 and idx_tmdb < len(r) else "").strip()
            imdb_id = (r[idx_imdb] if idx_imdb >= 0 and idx_imdb < len(r) else "").strip()
            show_name = (r[idx_name] if idx_name >= 0 and idx_name < len(r) else "").strip()
            show_nick = (r[idx_nick] if idx_nick >= 0 and idx_nick < len(r) else "").strip()
            
            if tmdb_id and tmdb_id.isdigit():
                out[tmdb_id] = {
                    "name": show_name, 
                    "imdb_id": imdb_id, 
                    "nickname": show_nick
                }
        
        print(f"📺 Loaded {len(out)} shows from ShowInfo")
        return out

    def ensure_castinfo_headers(self) -> gspread.Worksheet:
        """Get or create CastInfo sheet with proper headers."""
        try:
            ws = self.sh.worksheet("CastInfo")
        except gspread.WorksheetNotFound:
            ws = self.sh.add_worksheet(title="CastInfo", rows=50000, cols=8)
        
        headers = [
            "CastName",      # A - Official name (prefer IMDb)
            "CastID",        # B - TMDb person ID
            "Cast IMDbID",   # C - IMDb person ID
            "ShowName",      # D - Official show name from ShowInfo
            "Show IMDbID",   # E - Show IMDb ID
            "ShowID",        # F - TMDb show ID
            "TotalEpisodes", # G - Episode count (empty for now)
            "TotalSeasons",  # H - Season count
        ]
        ws.update(values=[headers], range_name="A1:H1")
        print(f"✅ Set up CastInfo headers: {headers}")
        return ws

    def existing_pairs_with_rows(self) -> Tuple[Set[Tuple[str, str]], Dict[Tuple[str, str], int]]:
        """Load existing (CastID, ShowID) pairs and track their row numbers for updates (bottom-up)."""
        try:
            ws = self.sh.worksheet("CastInfo")
            data = ws.get_all_values()
        except gspread.WorksheetNotFound:
            return set(), {}
        
        if len(data) < 2:
            return set(), {}
        
        header = data[0]
        print(f"📊 CastInfo header: {header}")
        
        # Column positions
        idx_cast = 1    # Column B - CastID  
        idx_imdb = 2    # Column C - Cast IMDbID
        idx_show = 5    # Column F - ShowID
        
        filled_pairs: Set[Tuple[str, str]] = set()
        missing_imdb_rows: Dict[Tuple[str, str], int] = {}
        
        # iterate bottom-up: last data row to first data row
        for i in range(len(data) - 1, 0, -1):
            r = data[i]
            row_idx = i + 1  # sheet rows are 1-indexed
            cast_id = r[idx_cast] if idx_cast < len(r) else ""
            show_id = r[idx_show] if idx_show < len(r) else ""
            imdb_id = r[idx_imdb] if idx_imdb < len(r) else ""
            
            if cast_id and show_id:
                pair = (cast_id, show_id)
                if imdb_id and imdb_id.strip():
                    filled_pairs.add(pair)
                else:
                    # Track rows missing IMDb IDs (bottom rows will be preferred later)
                    if pair not in missing_imdb_rows:
                        missing_imdb_rows[pair] = row_idx
        
        print(f"📋 Found {len(filled_pairs)} entries with Cast IMDb IDs filled")
        print(f"📋 Found {len(missing_imdb_rows)} entries missing Cast IMDb IDs")
        return filled_pairs, missing_imdb_rows

    def append_rows(self, ws: gspread.Worksheet, rows: List[List[str]]) -> None:
        """Append rows to sheet."""
        if not rows:
            return
        try:
            ws.append_rows(rows, value_input_option="RAW")
        except Exception:
            # Fallback to batch update
            start = len(ws.get_all_values()) + 1
            body = []
            for i, r in enumerate(rows):
                rng = f"A{start+i}:H{start+i}"
                body.append({"range": rng, "values": [r]})
            ws.batch_update(body)

    def _match_show_filter(self, show_name: str, nickname: str, tmdb_id: str, imdb_id: str, pattern: str) -> bool:
        """Check if show matches the filter pattern."""
        if not pattern:
            return True
        
        s = pattern.strip().lower()
        if not s:
            return True
        
        name = (show_name or "").lower()
        nick = (nickname or "").lower()
        tid = (tmdb_id or "").lower()
        iid = (imdb_id or "").lower()
        
        # Exact ID match
        if s == tid or s == iid:
            return True
        
        # Substring match on name or nickname
        if s in name or s in nick:
            return True
        
        # All tokens present
        tokens = [t for t in s.split() if t]
        if tokens and (all(t in name for t in tokens) or all(t in nick for t in tokens)):
            return True
        
        return False

    def resolve_imdb_id(self, tmdb_person_id: str, tmdb_name: str, show_imdb_id: str) -> Tuple[str, str]:
        """
        Resolve IMDb ID and official name using multiple sources.
        Returns (imdb_id, official_name).
        """
        imdb_id = ""
        official_name = tmdb_name
        
        print(f"    🔍 Resolving IMDb ID for: {tmdb_name} (TMDb: {tmdb_person_id})")
        
        # Method 1: TMDb external IDs
        try:
            external_ids = self.tmdb.person_external_ids(tmdb_person_id)
            if external_ids.get("imdb_id"):
                imdb_id = external_ids["imdb_id"]
                print(f"      ✅ TMDb external_ids: {imdb_id}")
        except Exception as e:
            print(f"      ⚠️ TMDb external_ids failed: {e}")
        
        # Method 2: Wikidata lookup
        if not imdb_id:
            wikidata_id = self.wikidata.get_imdb_from_tmdb(tmdb_person_id)
            if wikidata_id:
                imdb_id = wikidata_id
                print(f"      ✅ Wikidata: {imdb_id}")
        
        # Method 3: IMDbAPI.dev search
        if not imdb_id:
            search_results = self.imdb_api.search_person(tmdb_name)
            for result in search_results:
                candidate_id = result.get("id", "")
                candidate_name = result.get("name", "")
                if candidate_id and best_token_ratio(tmdb_name, candidate_name) >= 0.7:
                    imdb_id = candidate_id
                    print(f"      ✅ IMDbAPI search: {imdb_id} ({candidate_name})")
                    break
        
        # Method 4: Direct IMDb scraping
        if not imdb_id:
            search_results = self.imdb_scraper.search_person(tmdb_name)
            if search_results:
                # Take the first result for now - could add verification later
                imdb_id = search_results[0]
                print(f"      ✅ IMDb scraping: {imdb_id}")
        
        # Get official name if we have IMDb ID
        if imdb_id:
            # Try IMDbAPI first
            person_details = self.imdb_api.get_person_details(imdb_id)
            if person_details.get("name"):
                official_name = person_details["name"]
                print(f"      📛 Official name (IMDbAPI): {official_name}")
            else:
                # Fallback to scraping
                scraped_name = self.imdb_scraper.get_person_name(imdb_id)
                if scraped_name:
                    official_name = scraped_name
                    print(f"      📛 Official name (scraping): {official_name}")
        
        if not imdb_id:
            print(f"      ❌ No IMDb ID found for {tmdb_name}")
        
        return imdb_id, official_name

    def build_rows_for_show(
        self,
        tmdb_id: str,
        show_imdb_id: str,
        canonical_showname: str,
        filled_pairs: Set[Tuple[str, str]],
        missing_pairs: Set[Tuple[str, str]],
    ) -> List[List[str]]:
        """Build cast rows for a single show."""
        show_title = canonical_showname or ""
        rows: List[List[str]] = []

        try:
            agg = self.tmdb.tv_aggregate(tmdb_id)
            # Get show details for season count
            details = self.tmdb.tv_details(tmdb_id)
            total_seasons = str(details.get("number_of_seasons", ""))
        except Exception as e:
            print(f"❌ TMDb data failed for {tmdb_id}: {e}")
            return rows

        cast_list = agg.get("cast", []) or []
        print(f"  👥 TMDb cast count: {len(cast_list)}")

        for m in cast_list:
            tmdb_person = str(m.get("id") or "")
            tmdb_name = m.get("name") or ""
            
            if not tmdb_person or not tmdb_name:
                continue

            pair = (tmdb_person, tmdb_id)

            # ✅ Hard skip if this (CastID, ShowID) already has an IMDb ID in column C.
            if pair in filled_pairs:
                print(f"    ⏭️  Skip (already has IMDb ID): {tmdb_name} — TMDb {tmdb_person}")
                continue

            # Only resolve IMDb ID for pairs that are NEW or MISSING (no IMDb yet)
            imdb_id, cast_name = self.resolve_imdb_id(tmdb_person, tmdb_name, show_imdb_id)

            # Build row (no episode count for now)
            row = [
                cast_name,              # A CastName (official name)
                tmdb_person,            # B CastID (TMDb person ID)
                imdb_id,                # C Cast IMDbID
                show_title,             # D ShowName (from ShowInfo)
                show_imdb_id or "",     # E Show IMDbID
                tmdb_id,                # F ShowID (TMDb)
                "",                     # G TotalEpisodes (empty for now)
                total_seasons,          # H TotalSeasons
            ]
            rows.append(row)
            
            status = "🎬" if imdb_id else "⚠️"
            print(f"    {status} {cast_name}: TMDb {tmdb_person}, IMDb {imdb_id or 'None'}")

        return rows

    def update_missing_imdb_ids(self, ws: gspread.Worksheet, updates: List[Tuple[int, str, str]]) -> None:
        """Update rows that are missing IMDb IDs (bottom-up)."""
        if not updates:
            return
        
        # ensure bottom-up updates
        updates = sorted(updates, key=lambda t: t[0], reverse=True)
        print(f"📝 Updating {len(updates)} rows with missing IMDb IDs...")
        
        # Batch update the Cast IMDbID column (column C)
        batch_data = []
        for row_num, imdb_id, cast_name in updates:
            range_name = f"C{row_num}"
            batch_data.append({
                "range": range_name,
                "values": [[imdb_id]]
            })
            print(f"   🔄 Row {row_num}: {cast_name} → {imdb_id}")
        
        try:
            ws.batch_update(batch_data, value_input_option="RAW")
            print("✅ IMDb ID updates completed.")
        except Exception as e:
            print(f"❌ Batch update failed: {e}")
            # Fallback to individual updates
            for row_num, imdb_id, cast_name in updates:
                try:
                    ws.update(values=[[imdb_id]], range_name=f"C{row_num}")
                    print(f"   ✅ Updated row {row_num}: {cast_name}")
                except Exception as e2:
                    print(f"   ❌ Failed to update row {row_num}: {e2}")

    def run_build(self, dry_run: bool):
        """Main build process."""
        ws = self.ensure_castinfo_headers()
        filled_pairs, missing_imdb_rows = self.existing_pairs_with_rows()
        shows = self.load_show_info()

        items: List[Tuple[str, Dict[str, str]]] = list(shows.items())
        print(f"🔍 Will process {len(items)} shows (no filter)")
        print("⬇️  Bottom-up mode: existing rows with missing IMDb IDs will be updated starting from the bottom.")

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
            imdb_updates: List[Tuple[int, str, str]] = []  # Updates for this show

            # Process each row
            for r in rows:
                cast_id = r[1]   # Column B - CastID (TMDb person ID)
                show_id = r[5]   # Column F - ShowID (TMDb show ID)
                cast_name = r[0] # Column A - CastName
                new_imdb_id = r[2] # Column C - Cast IMDbID (newly resolved)
                pair = (cast_id, show_id)
                
                if cast_id and show_id:
                    if pair in filled_pairs:
                        # Entry exists with Cast IMDb ID already filled - skip it
                        print(f"      ⏭️  Has IMDb ID: {cast_name}")
                    elif pair in missing_imdb_rows:
                        # Entry exists but Cast IMDb ID not filled - update it if we found one
                        if new_imdb_id:
                            row_num = missing_imdb_rows[pair]
                            imdb_updates.append((row_num, new_imdb_id, cast_name))
                            print(f"      🔄 Will update IMDb ID: {cast_name} → {new_imdb_id}")
                            # Mark as filled to avoid duplicate updates
                            filled_pairs.add(pair)
                            del missing_imdb_rows[pair]
                        else:
                            print(f"      ❌ Still no IMDb ID found: {cast_name}")
                    else:
                        # Brand new entry
                        all_new.append(r)
                        print(f"      ➕ New: {cast_name}")
                else:
                    print(f"      ❌ Invalid IDs: {cast_name}")

            # Update IMDb IDs for this show immediately
            if not dry_run and imdb_updates:
                self.update_missing_imdb_ids(ws, imdb_updates)

        # Handle new rows at the end
        if not dry_run and all_new:
            print(f"\n📝 Adding {len(all_new)} new rows...")
            self.append_rows(ws, all_new)
            print("✅ New rows added.")
        elif dry_run:
            print("🔍 DRY RUN — not writing.")
            if all_new:
                print(f"\n📊 Would add {len(all_new)} new rows:")
                for i, row in enumerate(all_new[:3]):
                    print(f"  {i+1}: {' | '.join(row)}")

        if not all_new:
            print("\nℹ️  No new entries to add.")

def main():
    parser = argparse.ArgumentParser(description="Build CastInfo with multi-source ID resolution.")
    parser.add_argument("--mode", choices=["build"], default="build", help="Operation mode")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without writing")
    args = parser.parse_args()

    builder = CastInfoBuilder()
    if args.mode == "build":
        builder.run_build(dry_run=args.dry_run)

if __name__ == "__main__":
    main()
