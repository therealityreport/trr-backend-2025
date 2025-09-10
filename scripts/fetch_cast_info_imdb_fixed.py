#!/usr/bin/env python3
"""
CastInfo Data Collection Script - Enhanced Version

This script builds comprehensive cast information using TMDb API and IMDb scraping.
It properly handles official names, episode counts, and creates the correct column structure.
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
    """Small throttle for respectful scraping/API usage."""
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

class IMDB:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
        })
        self._fullcredits_cache: Dict[str, BeautifulSoup] = {}
        self._person_name_cache: Dict[str, str] = {}

    def _get_soup(self, url: str) -> Optional[BeautifulSoup]:
        try:
            _sleep()
            r = self.session.get(url, timeout=15)
            if r.status_code == 200:
                return BeautifulSoup(r.content, "html.parser")
        except Exception as e:
            print(f"⚠️  IMDb fetch failed {url}: {e}")
        return None

    def get_fullcredits_soup(self, tt: str) -> Optional[BeautifulSoup]:
        if not tt or not tt.startswith("tt"):
            return None
        if tt in self._fullcredits_cache:
            return self._fullcredits_cache[tt]
        soup = self._get_soup(f"https://www.imdb.com/title/{tt}/fullcredits")
        self._fullcredits_cache[tt] = soup
        return soup

    def fullcredits_people(self, tt: str) -> Dict[str, Dict[str, Any]]:
        """
        Returns dict keyed by IMDb person id -> {
            'name': display name,
            'episodes': int or None
        }
        """
        soup = self.get_fullcredits_soup(tt)
        out: Dict[str, Dict[str, Any]] = {}
        if not soup:
            return out

        # Method 1: Try new IMDb structure (2024+) - look for cast section specifically
        cast_section = None
        
        # Look for the section containing the "Cast" header with id="cast"
        cast_header = soup.find('span', id='cast')
        if cast_header:
            # Find the parent section
            cast_section = cast_header.find_parent('section', class_='ipc-page-section')
        
        # If no specific cast section found, fall back to searching the whole page
        search_area = cast_section if cast_section else soup
        
        # Find cast members in new structure - updated for 2024 IMDb layout
        # Look for name links with the correct modern classes
        name_links = search_area.find_all("a", class_=re.compile(r"name-credits--title-text"))
        
        if name_links:
            print(f"🔍 Found {len(name_links)} potential cast members with new structure")
            seen_nm_ids = set()  # Track seen IDs to avoid duplicates
            for name_link in name_links:
                if name_link and name_link.get("href"):
                    href = name_link.get("href")
                    match = re.search(r"/name/(nm\d+)/", href)
                    if match:
                        nm_id = match.group(1)
                        
                        # Skip if we've already seen this person ID (prevents duplicates)
                        if nm_id in seen_nm_ids:
                            continue
                        seen_nm_ids.add(nm_id)
                        
                        name = name_link.get_text().strip()
                        
                        # Find the parent container to check for role and episode info
                        container = name_link.find_parent("div")
                        if not container:
                            continue
                            
                        # Check if this is likely a cast member (not crew)
                        is_cast = False
                        episodes = None
                        seasons_text = None
                        
                        # Look for character/role information in nearby elements
                        container_text = container.get_text().lower()
                        
                        # Common cast indicators for reality TV and scripted shows
                        cast_indicators = [
                            'self', 'contestant', 'host', 'himself', 'herself', 'character',
                            'episodes', 'episode', 'as ', 'starring', 'regular'
                        ]
                        
                        if any(indicator in container_text for indicator in cast_indicators):
                            is_cast = True
                            
                        # Look for episode count in container text
                        episode_match = re.search(r"(\d+)\s+episodes?", container_text)
                        if episode_match:
                            episodes = int(episode_match.group(1))
                            is_cast = True  # Having episode count strongly suggests cast member
                            
                        # For reality shows, assume all name links in cast section are cast members
                        # if we haven't found clear indicators
                        if not is_cast and cast_section:
                            # Check if we're in the cast section and this looks like a person name
                            if len(name.split()) >= 2:  # Has first and last name
                                is_cast = True
                        
                        # Only include if we think this is a cast member
                        if is_cast:
                            # Try to extract seasons (skip for now as per user request)
                            # seasons_text = self._extract_seasons_from_cast_popup(nm_id, tt) if nm_id else None
                            
                            out[nm_id] = {
                                "name": name, 
                                "episodes": episodes,
                                "seasons": seasons_text
                            }
            
            if out:
                print(f"✅ Found {len(out)} cast members using new IMDb structure (2024+)")
                return out
        else:
            print("⚠️  No name links found with new structure classes")

        # Method 2: Fallback to old table structure
        table = soup.find("table", class_="cast_list")
        if not table:
            container = soup.find("div", attrs={"data-testid": "title-cast"})
            if container:
                table = container.find("table")
        if not table:
            print("⚠️  Could not find cast data in either new or old IMDb structure")
            return out

        print("📋 Using fallback old IMDb table structure")
        for row in table.find_all("tr"):
            link = row.find("a", href=re.compile(r"/name/(nm\d+)/"))
            if not link:
                continue
            href = link.get("href", "")
            m = re.search(r"/name/(nm\d+)/", href)
            if not m:
                continue
            nm = m.group(1)
            name = link.get_text(strip=True)

            # Look for episode count in the row text
            row_text = row.get_text(" ", strip=True)
            ep = None
            for pat in [
                r"\((\d+)\s+episodes?\)",
                r"\b(\d+)\s+episodes?\b",
                r"\b(\d+)\s+eps?\b",
                r"\((\d+)\s+episode\)",
            ]:
                mm = re.search(pat, row_text, re.IGNORECASE)
                if mm:
                    try:
                        ep = int(mm.group(1))
                        break
                    except:
                        pass

            out[nm] = {"name": name, "episodes": ep}
        return out

    def person_name(self, nm: str) -> str:
        """Get official person name from IMDb."""
        if not nm or not nm.startswith("nm"):
            return ""
        if nm in self._person_name_cache:
            return self._person_name_cache[nm]
        
        soup = self._get_soup(f"https://www.imdb.com/name/{nm}/")
        name = ""
        if soup:
            for sel in [
                'h1[data-testid="hero__pageTitle"] span',
                'h1 span[itemprop="name"]',
                'h1 span',
            ]:
                el = soup.select_one(sel)
                if el:
                    name = el.get_text(strip=True)
                    break
        self._person_name_cache[nm] = name
        return name

    def person_has_show(self, nm: str, tt: str) -> bool:
        """Verify if a person appeared in a specific show."""
        soup = self._get_soup(f"https://www.imdb.com/name/{nm}/")
        if not soup:
            return False
        return tt in str(soup)

    def _extract_seasons_from_text(self, text: str) -> Optional[str]:
        """
        Universal season extraction from container text.
        Uses the same approach as v2UniversalSeasonExtractor but for static HTML.
        """
        if not text:
            return None
            
        # Method 1: Look for explicit season mentions first (most reliable)
        season_patterns = [
            r'season[s]?\s*(\d+(?:\s*[,&]\s*\d+)*)',  # "seasons 2, 3" or "seasons 2 & 3"
            r's(\d+(?:\s*[,&]\s*\d+)*)',             # "S2, S3" or "S2 & S3"
            r'(\d+)(?:\s*[,&]\s*\d+)*\s*season',     # "2, 3 seasons"
        ]
        
        for pattern in season_patterns:
            season_match = re.search(pattern, text, re.I)
            if season_match:
                seasons_raw = season_match.group(1)
                seasons_list = [s.strip() for s in re.split(r'[,&\s]+', seasons_raw) if s.strip().isdigit()]
                if seasons_list:
                    unique_seasons = sorted(set(int(s) for s in seasons_list))
                    return ", ".join(map(str, unique_seasons))
        
        # Method 2: Look for episode markers like "S3.E1" (universal approach)
        episode_markers = re.findall(r'S(\d+)\.E\d+', text, re.I)
        if episode_markers:
            unique_seasons = sorted(set(int(s) for s in episode_markers))
            return ", ".join(map(str, unique_seasons))
            
        # Method 3: Look for season/episode format like "3x05" (season 3, episode 5)
        season_ep_markers = re.findall(r'(\d+)x\d+', text)
        if season_ep_markers:
            unique_seasons = sorted(set(int(s) for s in season_ep_markers))
            return ", ".join(map(str, unique_seasons))
                    
        return None

    def _extract_seasons_from_cast_popup(self, nm_id: str, tt: str) -> Optional[str]:
        """
        Extract season information using the universal approach from v2UniversalSeasonExtractor.
        Looks for season tabs and episode markers in the cast member's popup.
        """
        if not nm_id or not tt:
            return None
            
        try:
            # Access the cast member's episode popup 
            popup_url = f"https://www.imdb.com/title/{tt}/fullcredits?actor={nm_id}"
            
            soup = self._get_soup(popup_url)
            if not soup:
                return None
            
            # Method 1: Look for season tabs (most reliable - same as v2UniversalSeasonExtractor)
            # <li data-testid="season-tab-1"><span>1</span></li>
            season_tabs = soup.find_all("li", {"data-testid": re.compile(r"season-tab-\d+")})
            if season_tabs:
                seasons = set()
                for tab in season_tabs:
                    data_testid = tab.get("data-testid", "")
                    match = re.search(r"season-tab-(\d+)", data_testid)
                    if match:
                        seasons.add(int(match.group(1)))
                    else:
                        # Fallback to inner text
                        span = tab.find("span")
                        if span and span.get_text().strip().isdigit():
                            seasons.add(int(span.get_text().strip()))
                
                if seasons:
                    season_list = sorted(seasons)
                    return ", ".join(map(str, season_list))
            
            # Method 2: Look for episode markers in episode listings (universal approach)
            # <a...class="...episodic-credits-bottomsheet__menu-item"><span...><ul...><li...>S3.E1</li>...
            episode_items = soup.find_all("a", class_=re.compile(r"episodic-credits-bottomsheet__menu-item"))
            if episode_items:
                seasons_found = set()
                for item in episode_items:
                    text = item.get_text()
                    # Use the same patterns as v2UniversalSeasonExtractor
                    season_patterns = [
                        r'S(\d+)\.E\d+',       # S1.E1
                        r'S(\d+)E\d+',         # S1E1  
                        r'S(\d+)\s*·\s*E\d+',  # S1 · E1
                        r'Season\s+(\d+)',     # Season 1
                        r'S(\d+)',             # S1
                        r'(\d+)x\d+',          # 1x01
                    ]
                    
                    for pattern in season_patterns:
                        season_match = re.search(pattern, text, re.I)
                        if season_match:
                            seasons_found.add(int(season_match.group(1)))
                            break
                
                if seasons_found:
                    season_list = sorted(seasons_found)
                    return ", ".join(map(str, season_list))
            
            # Method 3: Look for year tabs with episode markers (handles year-based organization)
            year_tabs = soup.find_all("li", {"role": "tab"})
            if year_tabs:
                all_seasons = set()
                for tab in year_tabs:
                    tab_text = tab.get_text().strip()
                    # Check if this is a year tab
                    if re.match(r'^\d{4}$', tab_text):
                        # Look for episode markers in the content area after this tab
                        # This would require clicking the tab in Selenium, but for static parsing
                        # we can look for any episode markers in the overall content
                        pass
                        
            # Method 4: Fallback - extract from any episode-related content 
            all_text = soup.get_text()
            episode_markers = re.findall(r'S(\d+)\.E\d+', all_text, re.I)
            if episode_markers:
                unique_seasons = sorted(set(int(s) for s in episode_markers))
                return ", ".join(map(str, unique_seasons))
                        
        except Exception as e:
            print(f"   ⚠️ Season extraction failed for {nm_id}: {e}")
            
        return None

    def imdblike_search_person(self, q: str) -> List[str]:
        """Returns up to 5 nm ids from IMDb search results for a name string."""
        url = f"https://www.imdb.com/find/?s=nm&q={requests.utils.quote(q)}"
        soup = self._get_soup(url)
        ids: List[str] = []
        if not soup:
            return ids
        for a in soup.find_all("a", href=re.compile(r"/name/(nm\d+)/"))[:5]:
            m = re.search(r"/name/(nm\d+)/", a.get("href", ""))
            if m:
                ids.append(m.group(1))
        return list(dict.fromkeys(ids))  # dedupe, keep order

    def episode_count_via_filmography(self, nm: str, tt: str) -> int:
        """Get episode count from person's filmography page."""
        soup = self._get_soup(f"https://www.imdb.com/name/{nm}/")
        if not soup:
            return 0
        
        for a in soup.find_all("a", href=re.compile(rf"/title/{tt}/")):
            parent = a.find_parent(["div", "li", "tr"])
            txt = parent.get_text(" ", strip=True) if parent else ""
            for pat in [
                r"\((\d+)\s+episodes?\)",
                r"\b(\d+)\s+episodes?\b",
                r"\b(\d+)\s+eps?\b",
                r"\((\d+)\s+episode\)",
                r"\b(\d+)\s+Ep\b",
            ]:
                m = re.search(pat, txt, re.IGNORECASE)
                if m:
                    try:
                        return int(m.group(1))
                    except:
                        pass
        return 0

class TMDb:
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
        _sleep()
        r = self.session.get(f"https://api.themoviedb.org/3/person/{person_id}/external_ids", timeout=20)
        r.raise_for_status()
        return r.json()

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
        self.tmdb = TMDb(TMDB_BEARER)
        self.imdb = IMDB()

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
            ws = self.sh.add_worksheet(title="CastInfo", rows=20000, cols=8)
        
        headers = [
            "CastName",      # A
            "CastID",        # B (TMDb person)
            "Cast IMDbID",   # C
            "ShowName",      # D (Official name from ShowInfo)
            "Show IMDbID",   # E
            "ShowID",        # F (TMDb)
            "TotalEpisodes", # G
            "TotalSeasons",  # H
        ]
        ws.update(values=[headers], range_name="A1:H1")
        print(f"✅ Set up CastInfo headers: {headers}")
        return ws

    def existing_pairs(self) -> Set[Tuple[str, str]]:
        """Load existing (CastID, ShowID) pairs to avoid duplicates."""
        try:
            ws = self.sh.worksheet("CastInfo")
            data = ws.get_all_values()
        except gspread.WorksheetNotFound:
            return set()
        
        if len(data) < 2:
            return set()
        
        header = data[0]
        idx_cast = find_col_idx(header, [r"\bcastid\b"])
        idx_show = find_col_idx(header, [r"\bshowid\b"])
        
        pairs: Set[Tuple[str, str]] = set()
        for r in data[1:]:
            cast_id = r[idx_cast] if idx_cast >= 0 and idx_cast < len(r) else ""
            show_id = r[idx_show] if idx_show >= 0 and idx_show < len(r) else ""
            if cast_id and show_id:
                pairs.add((cast_id, show_id))
        
        print(f"📋 Found {len(pairs)} existing CastInfo entries")
        return pairs

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

    def _imdb_id_from_tmdb(self, tmdb_person_id: str) -> str:
        """Get IMDb ID from TMDb external IDs."""
        try:
            data = self.tmdb.person_external_ids(tmdb_person_id)
            nm = (data or {}).get("imdb_id") or ""
            if nm and nm.startswith("nm"):
                return nm
        except Exception as e:
            print(f"   ⚠️ TMDb external_ids failed for {tmdb_person_id}: {e}")
        return ""

    def _imdb_id_from_fullcredits(self, show_tt: str, candidate_name: str) -> str:
        """Find IMDb ID by matching name in fullcredits."""
        people = self.imdb.fullcredits_people(show_tt)
        cand = ""
        best = 0.0
        for nm, meta in people.items():
            score = best_token_ratio(candidate_name, meta["name"])
            if score > best:
                best, cand = score, nm
        if best >= 0.6:
            return cand
        return ""

    def _imdb_id_via_search_verify(self, person_name: str, show_tt: str) -> str:
        """Find IMDb ID via search and verification."""
        for nm in self.imdb.imdblike_search_person(person_name):
            if self.imdb.person_has_show(nm, show_tt):
                return nm
        return ""

    def build_rows_for_show(self, tmdb_id: str, show_tt: str, canonical_showname: str) -> List[List[str]]:
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

        # Load fullcredits for episode counts and name matching
        credits_map = self.imdb.fullcredits_people(show_tt) if show_tt else {}

        for m in cast_list:
            tmdb_person = str(m.get("id") or "")
            tmdb_name = m.get("name") or ""
            tmdb_episodes = m.get("total_episode_count", 0)
            
            if not tmdb_person or not tmdb_name:
                continue

            # Find IMDb person ID
            nm_id = self._imdb_id_from_tmdb(tmdb_person)
            if not nm_id and show_tt:
                nm_id = self._imdb_id_from_fullcredits(show_tt, tmdb_name)
            if not nm_id and show_tt:
                nm_id = self._imdb_id_via_search_verify(tmdb_name, show_tt)

            # Get official name and episode count
            individual_seasons = ""  # Initialize seasons for this cast member
            
            if nm_id:
                imdb_name = self.imdb.person_name(nm_id)
                cast_name = imdb_name or tmdb_name
                
                # Get episode count and seasons from IMDb if available
                if nm_id in credits_map:
                    credit_data = credits_map[nm_id]
                    imdb_episodes = credit_data.get("episodes")
                    individual_seasons = credit_data.get("seasons", "") or ""
                    
                    if imdb_episodes is not None:
                        episode_count = str(imdb_episodes)
                    else:
                        # Try filmography fallback
                        ep_count = self.imdb.episode_count_via_filmography(nm_id, show_tt)
                        episode_count = str(ep_count) if ep_count > 0 else str(tmdb_episodes)
                else:
                    episode_count = str(tmdb_episodes) if tmdb_episodes > 0 else ""
                    
                print(f"    🎬 {cast_name}: IMDb {nm_id}, Episodes: {episode_count}, Seasons: {individual_seasons}")
            else:
                cast_name = tmdb_name
                episode_count = str(tmdb_episodes) if tmdb_episodes > 0 else ""
                print(f"    ⚠️  {cast_name}: No IMDb ID, Episodes: {episode_count}")

            # Build row - use individual seasons instead of total show seasons
            row = [
                cast_name,         # A CastName (IMDb official if available)
                tmdb_person,       # B CastID (TMDb person ID)
                nm_id,             # C Cast IMDbID
                show_title,        # D ShowName (from ShowInfo)
                show_tt or "",     # E Show IMDbID
                tmdb_id,           # F ShowID (TMDb)
                episode_count,     # G TotalEpisodes
                individual_seasons,# H Seasons (specific seasons this person appeared in)
            ]
            rows.append(row)

        return rows

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

    def run_build(self, show_filter: Optional[str], dry_run: bool):
        """Main build process."""
        ws = self.ensure_castinfo_headers()
        existing = self.existing_pairs()
        shows = self.load_show_info()

        # Filter shows
        items: List[Tuple[str, Dict[str, str]]] = [
            (tid, info) for tid, info in shows.items()
            if self._match_show_filter(
                info.get("name", ""),
                info.get("nickname", ""),
                tid,
                info.get("imdb_id", ""),
                show_filter or ""
            )
        ]

        print(f"🔍 Will process {len(items)} shows (filter='{show_filter or ''}')")
        all_new: List[List[str]] = []
        
        for tmdb_id, info in items:
            show_tt = info.get("imdb_id", "").strip()
            show_name = info.get("name", "")
            print(f"\n🎭 Processing: {show_name} (TMDb {tmdb_id}, IMDb {show_tt or '—'})")
            
            rows = self.build_rows_for_show(tmdb_id, show_tt, show_name)

            # Dedupe by (CastID, ShowID)
            for r in rows:
                cast_id = r[1]
                show_id = r[5]
                if cast_id and show_id and (cast_id, show_id) not in existing:
                    all_new.append(r)
                    existing.add((cast_id, show_id))
                    print(f"      ➕ New: {r[0]}")
                else:
                    print(f"      ⏭️  Existing: {r[0]}")

        if not all_new:
            print("ℹ️  No new rows to append.")
            return

        print(f"📝 Prepared {len(all_new)} new rows.")
        if dry_run:
            print("🔍 DRY RUN — not writing.")
            # Show sample of what would be written
            print("\n📊 Sample rows that would be added:")
            for i, row in enumerate(all_new[:3]):
                print(f"  {i+1}: {' | '.join(row)}")
        else:
            self.append_rows(ws, all_new)
            print("✅ Rows appended.")

def main():
    parser = argparse.ArgumentParser(description="Build CastInfo with enhanced IMDb integration.")
    parser.add_argument("--mode", choices=["build"], default="build", help="Operation mode")
    parser.add_argument("--show-filter", help="Filter shows by name, nickname, or ID")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without writing")
    args = parser.parse_args()

    builder = CastInfoBuilder()
    if args.mode == "build":
        builder.run_build(show_filter=args.show_filter, dry_run=args.dry_run)

if __name__ == "__main__":
    main()
