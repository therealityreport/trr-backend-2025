#!/usr/bin/env python3
"""
CastInfo Data Collection Script

This script combines TMDb API data with IMDb web scraping to build comprehensive cast information:
1. Extracts cast data from TMDb API using aggregate_credits endpoint
2. Scrapes additional cast members from IMDb pages that aren't in TMDb database
3. Uses TMDb person IDs as CastID to match TMDb-anchored schema
4. Builds CastInfo sheet with 7 columns: Show IMDbID, CastID, CastName, Cast IMDbID, ShowName, ShowID, TotalEpisodes

Key Features:
- Hybrid TMDb + IMDb data extraction for maximum cast coverage
- D    #!/usr/bin/env python3
"""
CastInfo Data Collection Script

This script combines TMDb API data with IMDb web scraping to build comprehensive cast information:
1. Extracts cast data from TMDb API using aggregate_credits endpoint
2. Scrapes additional cast members from IMDb pages that aren't in TMDb database
3. Uses TMDb person IDs as CastID to match TMDb-anchored schema
4. Builds CastInfo sheet with 7 columns: Show IMDbID, CastID, CastName, Cast IMDbID, ShowName, ShowID, TotalEpisodes

Key Features:
- Hybrid TMDb + IMDb data extraction for maximum cast coverage
- Duplicate detection via (CastID, ShowID) tuples
- Rate limiting and error handling for web scraping
- BeautifulSoup fallback parsing for different IMDb page structures
"""

    def append_new_cast_entries(self, new_cast_rows: List[List[str]]) -> None:plicate detection via (CastID, ShowID) tuples
- Rate limiting and error handling for web scraping
- BeautifulSoup fallback parsing for different IMDb page structures
"""

import argparse
import gspread
import os
import re
import requests
import time
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from typing import Dict, List, Optional, Set, Tuple, Any

# Load environment
load_dotenv()

# Configuration
SPREADSHEET_ID = os.getenv('SPREADSHEET_ID')
TMDB_BEARER = os.getenv('TMDB_BEARER')
REQUEST_DELAY = 0.5  # Delay between API requests


class TMDbCastExtractor:
    """Extract cast information from TMDb with enhanced episode/season data."""

    def __init__(self, bearer_token: str):
        self.bearer_token = bearer_token
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {bearer_token}',
            'accept': 'application/json'
        })

    def get_series_cast(self, tmdb_id: str) -> List[Dict[str, Any]]:
        """Get comprehensive cast list from TMDb with season/episode details."""
        try:
            print(f"🎬 Fetching cast from TMDb series: {tmdb_id}")

            # Get series details first
            series_url = f"https://api.themoviedb.org/3/tv/{tmdb_id}"
            series_response = self.session.get(series_url)
            series_response.raise_for_status()
            series_data = series_response.json()

            total_seasons = series_data.get('number_of_seasons', 0)
            print(f"📺 Series has {total_seasons} seasons")

            # Get aggregate credits for the series
            credits_url = f"https://api.themoviedb.org/3/tv/{tmdb_id}/aggregate_credits"
            time.sleep(REQUEST_DELAY)

            credits_response = self.session.get(credits_url)
            credits_response.raise_for_status()
            credits_data = credits_response.json()

            cast_members = []
            cast_list = credits_data.get('cast', [])

            print(f"👥 Found {len(cast_list)} cast members in aggregate credits")

            for member in cast_list:
                name = member.get('name', '')
                person_id = member.get('id', '')
                total_episodes = member.get('total_episode_count', 0)

                roles = member.get('roles', [])
                # roles[].episode_count exists per role; not always season-specific,
                # but we keep the list for reference / debugging.
                season_counts = [r.get('episode_count', 0) for r in roles if isinstance(r, dict)]
                unique_counts = sorted(set([c for c in season_counts if c]))

                cast_members.append({
                    'name': name,
                    'tmdb_person_id': person_id,
                    'total_episodes': total_episodes,
                    'seasons': unique_counts,
                    'character': member.get('character', ''),
                    'order': member.get('order', 999)
                })
                print(f"👤 {name}: TMDb total {total_episodes} (roles counts {unique_counts})")

            # Sort by order (main cast first)
            cast_members.sort(key=lambda x: x['order'])
            print(f"✅ Extracted {len(cast_members)} cast members")
            return cast_members

        except Exception as e:
            print(f"❌ Error fetching cast for TMDb {tmdb_id}: {e}")
            return []

    def get_tmdb_external_ids(self, tmdb_person_id: str) -> Dict[str, str]:
        """Get external IDs (including IMDb) for a TMDb person."""
        try:
            url = f"https://api.themoviedb.org/3/person/{tmdb_person_id}/external_ids"
            time.sleep(REQUEST_DELAY)

            response = self.session.get(url)
            response.raise_for_status()
            data = response.json()

            return {
                'imdb_id': data.get('imdb_id', ''),
                'instagram_id': data.get('instagram_id', ''),
                'twitter_id': data.get('twitter_id', '')
            }

        except Exception as e:
            print(f"⚠️  Could not get external IDs for TMDb person {tmdb_person_id}: {e}")
            return {}


class PersonMapper:
    """Return the TMDb person ID as CastID when available; blank otherwise."""

    def __init__(self, update_info_sheet):
        self.update_info = update_info_sheet
        self.cast_id_counter = 1  # preserved but no longer used to mint IDs
        self.name_to_cast_id = {}
        self.load_existing_mappings()

    def load_existing_mappings(self):
        """(Kept for backward compatibility; not used to mint CastIDs anymore)."""
        try:
            if not self.update_info:
                return
            rows = self.update_info.get_all_values()
            for row in rows[1:]:
                if len(row) >= 2:
                    try:
                        cast_id = int(row[0])
                        name = row[1].lower()
                        if cast_id >= self.cast_id_counter:
                            self.cast_id_counter = cast_id + 1
                        self.name_to_cast_id[name] = str(cast_id)
                    except (ValueError, IndexError):
                        continue
            print(f"  📋 Loaded {len(self.name_to_cast_id)} legacy person mappings.")
        except Exception as e:
            print(f"  ⚠️  Could not load existing person mappings: {e}")

    def get_or_create_cast_id(self, name: str, tmdb_id: str = '', imdb_id: str = '') -> str:
        """
        Return the TMDb person ID to use as CastID when available.
        If no TMDb person ID is available (e.g., IMDb-only cast), return an empty string.
        """
        if tmdb_id:
            return str(tmdb_id)
        return ''


class IMDbCastScraper:
    """Scrape additional cast members from IMDb that aren't in TMDb."""

    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        }

    def get_episode_count_from_imdb(self, person_imdb_id: str, show_imdb_id: str, show_name: str) -> int:
        """Get episode count for a person from IMDb using multiple strategies - PRIORITIZED."""
        if not person_imdb_id or not show_imdb_id:
            return 0

        try:
            print(f"    🔍 Trying IMDb episode count for {person_imdb_id} in {show_imdb_id}...")

            # Strategy 1: Person's filmography page
            filmography_url = f"https://www.imdb.com/name/{person_imdb_id}/"
            response = requests.get(filmography_url, headers=self.headers, timeout=10)

            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')

                # Find all links that reference the show and inspect their parent containers
                show_links = soup.find_all('a', href=re.compile(rf'/title/{show_imdb_id}/'))
                for link in show_links:
                    parent = link.find_parent(['div', 'li', 'tr'])
                    if parent:
                        text = parent.get_text(" ", strip=True)
                        episode_patterns = [
                            r'\((\d+)\s+episodes?\)',
                            r'(\d+)\s+episodes?\s*\(',
                            r'(\d+)\s+episodes?\s*[-–,]',
                            r'(\d+)\s+episodes?\s*$',
                            r'\((\d+)\s+episode\)',
                            r'(\d+)\s+eps?\b'
                        ]
                        for pattern in episode_patterns:
                            m = re.search(pattern, text, re.IGNORECASE)
                            if m:
                                count = int(m.group(1))
                                if 1 <= count <= 10000:
                                    print(f"    📺 IMDb filmography: {count} episodes")
                                    return count

            # Strategy 2: Show's cast page - look for the person row mentioning 'episodes'
            cast_url = f"https://www.imdb.com/title/{show_imdb_id}/fullcredits"
            response = requests.get(cast_url, headers=self.headers, timeout=10)
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                person_links = soup.find_all('a', href=re.compile(rf'/name/{person_imdb_id}/'))
                for link in person_links:
                    row = link.find_parent('tr')
                    if row:
                        row_text = row.get_text(" ", strip=True)
                        episode_patterns = [
                            r'\((\d+)\s+episodes?\)',
                            r'(\d+)\s+episodes?',
                            r'(\d+)\s+eps?\b',
                            r'\((\d+)\s+episode\)'
                        ]
                        for pattern in episode_patterns:
                            m = re.search(pattern, row_text, re.IGNORECASE)
                            if m:
                                count = int(m.group(1))
                                if 1 <= count <= 10000:
                                    print(f"    📺 IMDb cast page: {count} episodes")
                                    return count

        except Exception as e:
            print(f"    ⚠️  Error getting IMDb episode count for {person_imdb_id}: {e}")

        print(f"    📺 No IMDb episode count found for {person_imdb_id}")
        return 0

    def scrape_imdb_cast(self, imdb_series_id: str, show_name: str, existing_tmdb_names: Set[str]) -> List[Dict[str, str]]:
        """Scrape cast from IMDb that aren't already in TMDb results."""
        if not imdb_series_id or not imdb_series_id.startswith('tt'):
            print(f"  ⚠️  No valid IMDb ID for {show_name}")
            return []

        try:
            cast_url = f"https://www.imdb.com/title/{imdb_series_id}/fullcredits"
            print(f"  🌐 Scraping IMDb cast: {cast_url}")
            response = requests.get(cast_url, headers=self.headers, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')

            cast_section = soup.find('table', class_='cast_list')
            if not cast_section:
                cast_section = soup.find('div', {'data-testid': 'title-cast'})
            if not cast_section:
                for table in soup.find_all('table'):
                    if 'cast' in str(table).lower():
                        cast_section = table
                        break
            if not cast_section:
                print(f"  ⚠️  No cast section found for {show_name}.")
                return []

            additional_cast = []
            if cast_section.name == 'table':
                rows = cast_section.find_all('tr')
                for row in rows[1:]:
                    try:
                        name_cell = row.find('td', class_='primary_photo')
                        if name_cell:
                            name_cell = name_cell.find_next_sibling('td')
                        if not name_cell:
                            name_links = row.find_all('a', href=re.compile(r'/name/nm\d+/'))
                            if not name_links:
                                continue
                            name_link = name_links[0]
                        else:
                            name_link = name_cell.find('a')
                            if not name_link:
                                continue

                        cast_name = name_link.get_text(strip=True)
                        person_url = name_link.get('href', '')
                        person_imdb_id = ''
                        if person_url:
                            m = re.search(r'/name/(nm\d+)/', person_url)
                            if m:
                                person_imdb_id = m.group(1)

                        if cast_name.lower() in existing_tmdb_names:
                            continue
                        if len(cast_name) < 3:
                            continue

                        additional_cast.append({
                            'name': cast_name,
                            'imdb_id': person_imdb_id,
                            'source': 'imdb_scraping'
                        })
                        print(f"    ➕ Found additional cast: {cast_name} ({person_imdb_id})")
                    except Exception as e:
                        print(f"    ⚠️  Error parsing cast row: {e}")
                        continue

            print(f"  ✅ Found {len(additional_cast)} additional IMDb cast members for {show_name}")
            return additional_cast[:50]

        except Exception as e:
            print(f"  ❌ Error scraping IMDb cast for {show_name}: {e}")
            return []


class CastInfoBuilder:
    """Build comprehensive cast information using TMDb as foundation with IMDb enhancement."""

    def __init__(self):
        self.gc = gspread.service_account(filename='keys/trr-backend-df2c438612e1.json')
        self.sh = self.gc.open_by_key(SPREADSHEET_ID)
        self.tmdb_extractor = TMDbCastExtractor(TMDB_BEARER)

        # Load existing data
        self.show_info = self.load_show_info()
        self.person_imdb_map = self.load_person_imdb_map()
        self.additional_cast = self.load_additional_cast_members()

    def load_show_info(self) -> Dict[str, Dict[str, str]]:
        """Load show information from ShowInfo sheet."""
        try:
            show_ws = self.sh.worksheet('ShowInfo')
            rows = show_ws.get_all_values()
            if not rows:
                return {}

            header = rows[0]
            show_info = {}

            tmdb_idx = -1
            imdb_idx = -1
            name_idx = -1

            for i, col in enumerate(header):
                lc = col.lower()
                if 'showid' in lc or 'tmdb' in lc or 'moviedb' in lc:
                    tmdb_idx = i
                elif 'imdbseriesid' in lc or ('imdb' in lc and 'id' in lc):
                    imdb_idx = i
                elif 'showname' in lc or 'name' in lc:
                    name_idx = i

            for row in rows[1:]:
                if len(row) > max(tmdb_idx, imdb_idx, name_idx):
                    tmdb_id = row[tmdb_idx] if tmdb_idx >= 0 else ''
                    imdb_id = row[imdb_idx] if imdb_idx >= 0 else ''
                    show_name = row[name_idx] if name_idx >= 0 else ''
                    if tmdb_id and show_name:
                        show_info[tmdb_id] = {'name': show_name, 'imdb_id': imdb_id}

            print(f"📺 Loaded {len(show_info)} shows from ShowInfo")
            return show_info

        except Exception as e:
            print(f"❌ Error loading show info: {e}")
            return {}

    def load_person_imdb_map(self) -> Dict[str, str]:
        """Load person name to IMDb ID mapping from UpdateInfo (optional, not required)."""
        try:
            update_ws = self.sh.worksheet('UpdateInfo')
            rows = update_ws.get_all_values()
            if not rows:
                return {}

            header = rows[0]
            person_map = {}

            name_idx = -1
            imdb_idx = -1
            for i, col in enumerate(header):
                lc = col.lower()
                if lc in ['castname', 'name']:
                    name_idx = i
                elif lc in ['imdbid', 'imdb_id']:
                    imdb_idx = i

            for row in rows[1:]:
                if len(row) > max(name_idx, imdb_idx) and name_idx >= 0 and imdb_idx >= 0:
                    name = row[name_idx].strip()
                    imdb_id = row[imdb_idx].strip()
                    if name and imdb_id:
                        person_map[name.lower()] = imdb_id

            print(f"👥 Loaded {len(person_map)} person IMDb IDs from UpdateInfo")
            return person_map

        except Exception as e:
            print(f"❌ Error loading person IMDb map: {e}")
            return {}

    def load_additional_cast_members(self) -> Dict[str, List[Dict[str, str]]]:
        """Static additions (kept as-is; optional)."""
        additional_cast = {
            'The Real Housewives of Beverly Hills': [
                {'name': 'Mauricio Umansky', 'role': "Kyle Richards' husband", 'seasons': 'All'},
                {'name': 'Harry Hamlin', 'role': "Lisa Rinna' husband", 'seasons': 'Multiple'},
                {'name': 'PK Kemsley', 'role': "Dorit Kemsley' husband", 'seasons': 'Multiple'},
                {'name': 'Teddi Mellencamp', 'role': 'Former housewife', 'seasons': '8,9,10'}
            ],
            'The Real Housewives of Atlanta': [
                {'name': 'Todd Tucker', 'role': "Kandi Burruss' husband", 'seasons': 'Multiple'},
                {'name': 'Marc Daly', 'role': 'Kenya Moore ex-husband', 'seasons': 'Multiple'}
            ]
        }
        print(f"📋 Loaded additional cast data for {len(additional_cast)} shows")
        return additional_cast

    def get_or_create_castinfo_sheet(self) -> gspread.Worksheet:
        """Get or create the CastInfo sheet with new structure."""
        try:
            cast_ws = self.sh.worksheet('CastInfo')
            print("📋 Found existing CastInfo sheet - will overwrite with new structure")
            cast_ws.clear()
        except gspread.WorksheetNotFound:
            print("📋 Creating new CastInfo sheet")
            cast_ws = self.sh.add_worksheet(title='CastInfo', rows=1000, cols=7)

        headers = [
            'Show IMDbID',    # A
            'CastID',         # B (TMDb person ID)
            'CastName',       # C
            'Cast IMDbID',    # D
            'ShowName',       # E
            'ShowID',         # F (TMDb show ID)
            'TotalEpisodes'   # G (blank for now)
        ]
        cast_ws.update('A1:G1', [headers])
        print(f"✅ Set up CastInfo headers: {headers}")
        return cast_ws

    def build_cast_for_show(self, show_info: Dict[str, str], person_mapper: PersonMapper, imdb_scraper: IMDbCastScraper) -> List[List[Any]]:
        """Build cast data for a single show, including additional IMDb cast."""
        show_name = show_info['ShowName']
        tmdb_show_id = show_info['ShowID']
        show_imdb_id = show_info.get('Show IMDbID', '')

        print(f"\n🎭 Processing cast for: {show_name}")

        if not tmdb_show_id:
            print(f"  ⚠️  No TMDb Show ID for {show_name}")
            return []

        cast_rows: List[List[Any]] = []
        tmdb_cast_names: Set[str] = set()

        # TMDb cast first
        cast_data = self.tmdb_extractor.get_series_cast(tmdb_show_id)
        if cast_data:
            print(f"  📺 Processing {len(cast_data)} TMDb cast members...")
            for person in cast_data:
                try:
                    person_name = person['name']
                    tmdb_person_id = str(person['tmdb_person_id'])

                    # External IDs (IMDb)
                    external_ids = self.tmdb_extractor.get_tmdb_external_ids(tmdb_person_id)
                    person_imdb_id = external_ids.get('imdb_id', '')
                    
                    # Log IMDb ID status
                    if person_imdb_id:
                        print(f"    🎬 Found IMDb ID for {person_name}: {person_imdb_id}")
                    else:
                        print(f"    ⚠️  No IMDb ID found for {person_name}")

                    tmdb_cast_names.add(person_name.lower())

                    # CastID must be TMDb person ID
                    cast_id = tmdb_person_id

                    row = [
                        show_imdb_id,   # Show IMDbID
                        cast_id,        # CastID (TMDb person ID)
                        person_name,    # CastName
                        person_imdb_id, # Cast IMDbID
                        show_name,      # ShowName
                        tmdb_show_id,   # ShowID (TMDb)
                        ""              # TotalEpisodes (blank for now)
                    ]
                    cast_rows.append(row)
                    print(f"    ✅ TMDb: {person_name} (CastID: {cast_id}, IMDb: {person_imdb_id or 'None'})")

                except Exception as e:
                    print(f"    ⚠️  Error processing TMDb person {person.get('name', 'Unknown')}: {e}")
                    continue

        # Additional IMDb-only cast
        if show_imdb_id:
            print(f"  🌐 Scraping additional IMDb cast...")
            additional_cast = imdb_scraper.scrape_imdb_cast(show_imdb_id, show_name, tmdb_cast_names)
            for person in additional_cast:
                try:
                    # IMDb-only: no TMDb person ID → CastID blank
                    cast_id = ''

                    row = [
                        show_imdb_id,        # Show IMDbID
                        cast_id,             # CastID (blank for IMDb-only)
                        person['name'],      # CastName
                        person['imdb_id'],   # Cast IMDbID
                        show_name,           # ShowName
                        tmdb_show_id,        # ShowID (TMDb)
                        ""                   # TotalEpisodes (blank for now)
                    ]
                    cast_rows.append(row)
                    print(f"    ➕ IMDb: {person['name']}")

                except Exception as e:
                    print(f"    ⚠️  Error processing IMDb person {person.get('name', 'Unknown')}: {e}")
                    continue
        else:
            print(f"  ⚠️  No IMDb ID available for {show_name}, skipping IMDb scraping")

        print(f"  📊 Total cast members: {len(cast_rows)} (TMDb + IMDb)")
        return cast_rows

    def load_existing_castinfo(self) -> Set[Tuple[str, str]]:
        """Load existing CastInfo entries to avoid duplicates and store data for updates."""
        try:
            cast_ws = self.sh.worksheet('CastInfo')
            self.existing_cast_data = cast_ws.get_all_values()

            if not self.existing_cast_data or len(self.existing_cast_data) <= 1:
                self.existing_cast_data = []
                return set()

            existing_entries = set()
            header = self.existing_cast_data[0]

            cast_id_idx = -1
            show_id_idx = -1
            for i, col in enumerate(header):
                lc = col.lower()
                if lc == 'castid':
                    cast_id_idx = i
                elif lc == 'showid':
                    show_id_idx = i

            if cast_id_idx >= 0 and show_id_idx >= 0:
                for row in self.existing_cast_data[1:]:
                    if len(row) > max(cast_id_idx, show_id_idx):
                        cast_id = row[cast_id_idx].strip()
                        show_id = row[show_id_idx].strip()
                        if cast_id and show_id:
                            existing_entries.add((cast_id, show_id))

            print(f"📋 Found {len(existing_entries)} existing CastInfo entries")
            return existing_entries

        except gspread.WorksheetNotFound:
            print("📋 No existing CastInfo sheet found")
            self.existing_cast_data = []
            return set()
        except Exception as e:
            print(f"⚠️ Error loading existing CastInfo: {e}")
            self.existing_cast_data = []
            return set()

    def append_new_cast_entries(self, new_cast_rows: List[List[Any]]) -> None:
        """Append only new cast entries to the bottom of CastInfo sheet."""
        try:
            cast_ws = self.sh.worksheet('CastInfo')
            existing_data = cast_ws.get_all_values()
            next_row = len(existing_data) + 1

            if new_cast_rows:
                print(f"📝 Appending {len(new_cast_rows)} new entries starting at row {next_row}")
                for i, row in enumerate(new_cast_rows):
                    row_num = next_row + i
                    range_name = f"A{row_num}:G{row_num}"
                    cast_ws.update(range_name, [row])
                    cast_name = row[2] if len(row) > 2 else "Unknown"
                    show_name = row[4] if len(row) > 4 else "Unknown"  # ShowName is now column E (index 4)
                    print(f"  ➕ Added: {cast_name} in {show_name}")
                    time.sleep(0.2)
                print(f"✅ Successfully appended {len(new_cast_rows)} new cast entries")
            else:
                print("ℹ️  No new entries to append")

        except Exception as e:
            print(f"❌ Error appending cast entries: {e}")

    def build_complete_cast_info(self, show_filter: Optional[str] = None, dry_run: bool = False):
        """Build complete cast information for all shows."""
        print("🚀 Starting comprehensive cast information rebuild...")

        existing_entries = self.load_existing_castinfo()

        try:
            update_ws = self.sh.worksheet('UpdateInfo')
            person_mapper = PersonMapper(update_ws)
        except Exception as e:
            print(f"⚠️  Could not load UpdateInfo sheet, using basic person mapping: {e}")
            person_mapper = PersonMapper(None)

        imdb_scraper = IMDbCastScraper()

        show_info_dict = self.load_show_info()

        show_info_list = []
        for tmdb_id, info in show_info_dict.items():
            show_entry = {
                'ShowID': tmdb_id,
                'ShowName': info['name'],
                'Show IMDbID': info['imdb_id']
            }
            show_info_list.append(show_entry)

        if show_filter:
            show_info_list = [info for info in show_info_list if show_filter.lower() in info.get('ShowName', '').lower()]
            print(f"🔍 Filtering to {len(show_info_list)} shows matching '{show_filter}'")

        if not show_info_list:
            print("❌ No shows to process")
            return

        if not existing_entries and not dry_run:
            self.get_or_create_castinfo_sheet()

        total_new_entries = 0
        total_duplicate_count = 0

        for show_info in show_info_list:
            show_name = show_info.get('ShowName', 'Unknown')

            try:
                cast_rows = self.build_cast_for_show(show_info, person_mapper, imdb_scraper)

                show_new_cast_rows: List[List[Any]] = []
                show_duplicate_count = 0

                for cast_row in cast_rows:
                    cast_id = cast_row[1]  # Column B: CastID
                    show_id = cast_row[5]  # Column F: ShowID  (FIXED index)
                    cast_name = cast_row[2] if len(cast_row) > 2 else "Unknown"

                    # Only dedupe when we actually have a (CastID, ShowID) tuple
                    if cast_id and show_id:
                        if (cast_id, show_id) not in existing_entries:
                            show_new_cast_rows.append(cast_row)
                            existing_entries.add((cast_id, show_id))
                            print(f"  ➕ New: {cast_name} in {show_name}")
                        else:
                            show_duplicate_count += 1
                            print(f"  ⏭️  Skipping existing: {cast_name} in {show_name}")
                    else:
                        # IMDb-only rows (blank CastID) can't collide on (CastID, ShowID); just append
                        show_new_cast_rows.append(cast_row)
                        print(f"  ➕ New (IMDb-only): {cast_name} in {show_name}")

                if show_new_cast_rows:
                    if not dry_run:
                        print(f"  📝 Appending {len(show_new_cast_rows)} new entries for {show_name}...")
                        self.append_new_cast_entries(show_new_cast_rows)
                    print(f"  ✨ Found {len(show_new_cast_rows)} new cast members for {show_name}")
                    total_new_entries += len(show_new_cast_rows)
                else:
                    print(f"  ℹ️  No new cast members found for {show_name}")

                total_duplicate_count += show_duplicate_count
                time.sleep(REQUEST_DELAY)

            except Exception as e:
                print(f"❌ Error processing {show_name}: {e}")
                continue

        print(f"\n📊 Summary:")
        print(f"  Shows processed: {len(show_info_list)}")
        print(f"  New cast entries: {total_new_entries}")
        print(f"  Existing entries skipped: {total_duplicate_count}")

        if dry_run:
            print("🔍 DRY RUN - No data written to sheet")
            print("ℹ️  In live run, entries would be added immediately after each show")
        else:
            if total_new_entries > 0:
                print(f"✅ Successfully added {total_new_entries} new cast entries to CastInfo sheet")
            else:
                print("ℹ️  No new cast entries were added")


def main():
    parser = argparse.ArgumentParser(description='Rebuild CastInfo using TMDb with IMDb enhancement')
    parser.add_argument('--show-filter', help='Filter to shows containing this name')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be done without writing')

    args = parser.parse_args()

    try:
        builder = CastInfoBuilder()
        builder.build_complete_cast_info(
            show_filter=args.show_filter,
            dry_run=args.dry_run
        )

    except Exception as e:
        print(f"❌ Script failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()
    