# fetch_show_info.py
import os
import time
import re
import json
import requests
import gspread
from typing import Optional, Dict, List, Set
from pathlib import Path
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv, find_dotenv

# ===============================
# Environment / Config (robust .env find)
# ===============================
HERE = Path(__file__).resolve().parent

def _load_env():
    # Try common locations, then fallback
    candidates = [
        HERE / ".env",
        HERE.parent / ".env",
        Path(find_dotenv(filename=".env", raise_error_if_not_found=False or False)),
    ]
    for p in candidates:
        try:
            if p and str(p) != "" and Path(p).exists():
                load_dotenv(dotenv_path=str(p), override=True)
                break
        except Exception:
            pass
    # also allow default search
    load_dotenv(override=True)

_load_env()

GOOGLE_CREDS   = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
TMDB_API_KEY   = os.getenv("TMDB_API_KEY")
TMDB_BEARER    = os.getenv("TMDB_BEARER")
TMDB_LIST_ID   = os.getenv("TMDB_LIST_ID")  # e.g., "8301263"
THETVDB_API_KEY= os.getenv("THETVDB_API_KEY")

# IMDb list URL — set to your list
IMDB_LIST_URL  = os.getenv("IMDB_LIST_URL", "https://www.imdb.com/list/ls4106677119/")

# --- TheTVDB v4 token helper (login if needed) ---
_TVDB_JWT = None

def tvdb_get_token() -> Optional[str]:
    """
    Returns a valid TheTVDB v4 JWT. If THETVDB_API_KEY already looks like a JWT (has two dots),
    use it as-is. Otherwise, log in with the API key to obtain a JWT and cache it.
    """
    global _TVDB_JWT
    # Reuse cached JWT if present and looks valid
    if _TVDB_JWT and _TVDB_JWT.count(".") == 2:
        return _TVDB_JWT

    # Some users store a JWT directly in THETVDB_API_KEY; detect by two dots in the string
    if THETVDB_API_KEY and THETVDB_API_KEY.count(".") == 2:
        _TVDB_JWT = THETVDB_API_KEY
        return _TVDB_JWT

    if not THETVDB_API_KEY:
        return None

    try:
        r = requests.post(
            "https://api4.thetvdb.com/v4/login",
            json={"apikey": THETVDB_API_KEY},
            timeout=15
        )
        if r.status_code == 200:
            _TVDB_JWT = (r.json().get("data") or {}).get("token")
            return _TVDB_JWT
        else:
            print(f"TheTVDB login failed: {r.status_code} {r.text}")
            return None
    except Exception as e:
        print(f"TheTVDB login error: {e}")
        return None

def require(name: str, value: Optional[str]):
    if not value:
        raise ValueError(f"Missing {name} in .env (or environment).")
    return value

# Soft-fallback for service account path
if not GOOGLE_CREDS or not Path(GOOGLE_CREDS).exists():
    keys_dir = HERE.parent / "keys"
    if keys_dir.exists():
        for p in keys_dir.iterdir():
            if p.suffix == ".json":
                GOOGLE_CREDS = str(p)
                break

GOOGLE_CREDS   = require("GOOGLE_APPLICATION_CREDENTIALS", GOOGLE_CREDS)
SPREADSHEET_ID = require("SPREADSHEET_ID", SPREADSHEET_ID)
TMDB_API_KEY   = require("TMDB_API_KEY", TMDB_API_KEY)
TMDB_BEARER    = require("TMDB_BEARER", TMDB_BEARER)

# ===============================
# Target column layout (A..K)
# ===============================
NEW_HEADERS: List[str] = [
    "Show",                # A (TMDb ID - primary identifier)
    "ShowName",            # B
    "Network",             # C
    "ShowTotalSeasons",    # D
    "ShowTotalEpisodes",   # E
    "IMDbSeriesID",        # F
    "TheMovieDB ID",       # G
    "TVdbID",              # H
    "Most Recent Episode", # I
    "OVERRIDE",            # J
    "WikidataID",          # K
]

COLS = {h:i for i,h in enumerate(NEW_HEADERS)}

# ===============================
# Google Sheets helpers
# ===============================
def sheets_client():
    creds = Credentials.from_service_account_file(
        GOOGLE_CREDS,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    return gspread.authorize(creds)

def open_sheet(gc):
    sh = gc.open_by_key(SPREADSHEET_ID)
    try:
        ws = sh.worksheet("ShowInfo")
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title="ShowInfo", rows=1000, cols=len(NEW_HEADERS))
        ws.update(values=[NEW_HEADERS], range_name="A1")
    return ws

def get_all_values_safe(ws) -> List[List[str]]:
    try:
        return ws.get_all_values()
    except Exception:
        return []

def header_index_map(headers: List[str]) -> Dict[str, int]:
    return {h: i for i, h in enumerate(headers)}

def normalize_sheet_structure(ws) -> None:
    """Make the header row exactly NEW_HEADERS. Preserve data by remapping."""
    data = get_all_values_safe(ws)
    if not data:
        ws.update(values=[NEW_HEADERS], range_name="A1")
        return
    current_headers = data[0]
    if current_headers == NEW_HEADERS:
        return

    print("Normalizing header structure…")
    cur_map = header_index_map(current_headers)
    remapped: List[List[str]] = [NEW_HEADERS]
    for row in data[1:]:
        new_row = []
        for col_name in NEW_HEADERS:
            if col_name in cur_map and cur_map[col_name] < len(row):
                new_row.append(row[cur_map[col_name]])
            else:
                new_row.append("")
        remapped.append(new_row)

    ws.clear()
    try:
        ws.resize(rows=max(len(remapped), 1000), cols=len(NEW_HEADERS))
    except Exception:
        pass
    ws.update(values=remapped, range_name="A1", value_input_option="RAW")
    print("Header normalization complete.")

def read_existing_shows(ws) -> Dict[str, Dict]:
    """Read existing shows keyed by 'Show' (col A). Keep row_index for in-place updates."""
    existing: Dict[str, Dict] = {}
    data = get_all_values_safe(ws)
    if not data:
        return existing
    headers = data[0]
    cmap = header_index_map(headers)
    for i, row in enumerate(data[1:], start=2):
        if not row:
            continue
        show_id = row[cmap.get("Show", 0)] if len(row) > cmap.get("Show", 0) else ""
        if not show_id:
            continue
        existing[show_id] = {
            "row_index": i,
            "ShowName": row[cmap.get("ShowName", 1)] if len(row) > 1 else "",
            "Network": row[cmap.get("Network", 2)] if len(row) > 2 else "",
            "ShowTotalSeasons": row[cmap.get("ShowTotalSeasons", 3)] if len(row) > 3 else "",
            "ShowTotalEpisodes": row[cmap.get("ShowTotalEpisodes", 4)] if len(row) > 4 else "",
            "IMDbSeriesID": row[cmap.get("IMDbSeriesID", 5)] if len(row) > 5 else "",
            "TheMovieDB ID": row[cmap.get("TheMovieDB ID", 6)] if len(row) > 6 else "",
            "TVdbID": row[cmap.get("TVdbID", 7)] if len(row) > 7 else "",
            "Most Recent Episode": row[cmap.get("Most Recent Episode", 8)] if len(row) > 8 else "",
            "OVERRIDE": row[cmap.get("OVERRIDE", 9)] if len(row) > 9 else "",
            "WikidataID": row[cmap.get("WikidataID", 10)] if len(row) > 10 else "",
        }
    return existing

# ===============================
# IMDb Scraper
# ===============================
def fetch_imdb_list_shows() -> List[Dict]:
    """
    Scrape (name, imdb_id) from your IMDb list using structured data.
    
    Primary strategy: Extract from JSON-LD structured data (gets all items in one request).
    Fallback strategies: HTML parsing and pagination if needed.
    
    Returns:
        List of dicts with 'name' and 'imdb_id' keys.
    """
    base = (IMDB_LIST_URL or "").split("?")[0].rstrip("/") + "/"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    
    try:
        print(f"Fetching IMDb list: {base}")
        resp = requests.get(base, headers=headers, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, "html.parser")
        
        # Strategy 1: Extract from JSON-LD structured data (most reliable)
        json_scripts = soup.find_all("script", type="application/ld+json")
        shows = []
        
        for script in json_scripts:
            try:
                data = json.loads(script.string)
                
                # Check if this is the list data with itemListElement
                if isinstance(data, dict) and "itemListElement" in data:
                    items = data["itemListElement"]
                    print(f"Found structured data with {len(items)} items")
                    
                    for item in items:
                        try:
                            tv_series = item.get("item", {})
                            if tv_series.get("@type") == "TVSeries":
                                name = tv_series.get("name", "")
                                url = tv_series.get("url", "")
                                
                                # Extract IMDb ID from URL
                                m = re.search(r"/title/(tt\d+)/", url)
                                if m and name:
                                    imdb_id = m.group(1)
                                    shows.append({"name": name, "imdb_id": imdb_id})
                        except Exception as e:
                            print(f"  Error parsing structured item: {e}")
                    
                    if shows:
                        print(f"Successfully extracted {len(shows)} shows from structured data")
                        return shows
                        
            except Exception as e:
                print(f"  Error parsing JSON-LD: {e}")
        
        # Strategy 2: Fallback to HTML parsing if structured data fails
        print("Structured data extraction failed, falling back to HTML parsing")
        items = soup.find_all("li", class_="ipc-metadata-list-summary-item")
        seen_ids = set()
        
        for it in items:
            try:
                link = it.find("a", class_="ipc-title-link-wrapper")
                if not link:
                    continue
                href = link.get("href", "")
                title = link.find("h3")
                if not title:
                    continue
                m = re.search(r"/title/(tt\d+)/", href)
                if not m:
                    continue
                imdb_id = m.group(1)
                if imdb_id in seen_ids:
                    continue
                name = re.sub(r"^\d+\.\s*", "", title.get_text(strip=True))
                shows.append({"name": name, "imdb_id": imdb_id})
                seen_ids.add(imdb_id)
            except Exception as e:
                print(f"  HTML parse error: {e}")
        
        print(f"HTML parsing extracted {len(shows)} shows")
        
        # If we still don't have many shows, try pagination
        if len(shows) < 50:
            print("Attempting pagination fallback...")
            return fetch_imdb_with_pagination(base, headers, shows)
        
        return shows
        
    except Exception as e:
        print(f"IMDb list fetch error: {e}")
        return []

def fetch_imdb_with_pagination(base: str, headers: dict, initial_shows: List[Dict]) -> List[Dict]:
    """Fallback pagination logic for IMDb lists."""
    shows = initial_shows.copy()
    seen_ids = {show["imdb_id"] for show in shows}
    visited = set()
    page_url = base
    page_num = 1
    
    while page_url and page_url not in visited and page_num <= 10:  # Safety limit
        if page_num == 1 and initial_shows:
            # Skip first page since we already processed it
            page_num += 1
            # Try different pagination URL patterns
            for pattern in ["?page=2", "?start=25", "/?page=2"]:
                test_url = base.rstrip("/") + pattern
                try:
                    test_resp = requests.get(test_url, headers=headers, timeout=15)
                    if test_resp.status_code == 200:
                        test_soup = BeautifulSoup(test_resp.content, "html.parser")
                        test_items = test_soup.find_all("li", class_="ipc-metadata-list-summary-item")
                        if test_items:
                            page_url = test_url
                            break
                except Exception:
                    continue
            else:
                break
        
        visited.add(page_url)
        try:
            print(f"Fetching pagination page {page_num}: {page_url}")
            resp = requests.get(page_url, headers=headers, timeout=30)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.content, "html.parser")
            
            items = soup.find_all("li", class_="ipc-metadata-list-summary-item")
            page_count = 0
            
            for it in items:
                try:
                    link = it.find("a", class_="ipc-title-link-wrapper")
                    if not link:
                        continue
                    href = link.get("href", "")
                    title = link.find("h3")
                    if not title:
                        continue
                    m = re.search(r"/title/(tt\d+)/", href)
                    if not m:
                        continue
                    imdb_id = m.group(1)
                    if imdb_id in seen_ids:
                        continue
                    name = re.sub(r"^\d+\.\s*", "", title.get_text(strip=True))
                    shows.append({"name": name, "imdb_id": imdb_id})
                    seen_ids.add(imdb_id)
                    page_count += 1
                except Exception as e:
                    print(f"  Pagination parse error: {e}")
            
            print(f"  Page {page_num}: {page_count} new shows (total: {len(shows)})")
            
            if page_count == 0:
                break
                
            # Try to find next page (simplified)
            page_num += 1
            next_url = base.rstrip("/") + f"?page={page_num}"
            page_url = next_url
            time.sleep(0.5)
            
        except Exception as e:
            print(f"Pagination error on page {page_num}: {e}")
            break
    
    return shows

# ===============================
# TMDb helpers
# ===============================
V4_HEADERS = {"Authorization": f"Bearer {TMDB_BEARER}", "accept": "application/json"}
V3_BASE = "https://api.themoviedb.org/3"

def fetch_list_items(list_id: str) -> List[dict]:
    """Fetch all items from a TMDb v4 list (paged)."""
    url = f"https://api.themoviedb.org/4/list/{list_id}"
    page = 1
    out: List[dict] = []
    while True:
        resp = requests.get(url, headers=V4_HEADERS, params={"page": page}, timeout=30)
        if resp.status_code != 200:
            raise RuntimeError(f"TMDb v4 list fetch failed p{page}: {resp.status_code} {resp.text}")
        data = resp.json()
        out.extend([r for r in data.get("results", []) if r.get("media_type") in (None, "tv")])
        total_pages = data.get("total_pages", 1)
        if page >= total_pages:
            break
        page += 1
        time.sleep(0.25)
    return out

def fetch_tv_details(tv_id: int) -> dict:
    """Fetch full TV details (including external_ids)."""
    params = {"api_key": TMDB_API_KEY}
    base_url = f"{V3_BASE}/tv/{tv_id}"
    resp = requests.get(base_url, params=params, timeout=30)
    if resp.status_code != 200:
        return {"number_of_seasons": None, "number_of_episodes": None, "networks": [], "external_ids": {}}
    data = resp.json()
    try:
        ext = requests.get(f"{base_url}/external_ids", params=params, timeout=30)
        data["external_ids"] = ext.json() if ext.status_code == 200 else {}
    except Exception:
        data["external_ids"] = {}
    return data

def search_tmdb_by_imdb_id(imdb_id: str) -> Optional[Dict]:
    """Match on TMDb via IMDb ID."""
    if not imdb_id.startswith("tt"):
        imdb_id = "tt" + imdb_id
    url = f"{V3_BASE}/find/{imdb_id}"
    params = {"api_key": TMDB_API_KEY, "external_source": "imdb_id"}
    try:
        r = requests.get(url, params=params, timeout=30)
        if r.status_code == 200:
            tv = r.json().get("tv_results", [])
            if tv:
                return tv[0]
    except Exception as e:
        print("TMDb find-by-IMDb error:", e)
    return None

def search_tmdb_by_name(show_name: str) -> Optional[Dict]:
    """Fallback: TMDb search by name."""
    url = f"{V3_BASE}/search/tv"
    params = {"api_key": TMDB_API_KEY, "query": show_name}
    try:
        r = requests.get(url, params=params, timeout=30)
        if r.status_code == 200:
            res = r.json().get("results", [])
            if res:
                return res[0]
    except Exception as e:
        print("TMDb search-by-name error:", e)
    return None

def get_most_recent_episode_date(tv_id: int) -> str:
    """Return YYYY-MM-DD of the last aired episode (best effort)."""
    try:
        params = {"api_key": TMDB_API_KEY}
        r = requests.get(f"{V3_BASE}/tv/{tv_id}", params=params, timeout=30)
        if r.status_code != 200:
            return ""
        data = r.json()
        if data.get("last_episode_to_air") and data["last_episode_to_air"].get("air_date"):
            return data["last_episode_to_air"]["air_date"]
        seasons = data.get("seasons", [])
        if not seasons:
            return ""
        latest = max(seasons, key=lambda s: s.get("season_number", 0))
        sn = latest.get("season_number", 1)
        rs = requests.get(f"{V3_BASE}/tv/{tv_id}/season/{sn}", params=params, timeout=30)
        if rs.status_code != 200:
            return ""
        eps = rs.json().get("episodes", [])
        if not eps:
            return ""
        from datetime import datetime
        today = datetime.now().strftime("%Y-%m-%d")
        aired = [ep for ep in eps if ep.get("air_date") and ep["air_date"] <= today]
        if not aired:
            return ""
        return max(aired, key=lambda e: e.get("air_date", "")).get("air_date", "")
    except Exception as e:
        print("Recent-episode fetch error:", e)
        return ""

# ===============================
# External IDs
# ===============================
def get_wikidata_id(show_name: str, imdb_id: Optional[str] = None) -> Optional[str]:
    """Lightweight Wikidata search by name."""
    try:
        r = requests.get(
            "https://www.wikidata.org/w/api.php",
            params={"action":"wbsearchentities","format":"json","search":show_name,"language":"en","type":"item","limit":5},
            timeout=15
        )
        if r.status_code == 200:
            data = r.json()
            for item in data.get("search", []):
                desc = (item.get("description") or "").lower()
                if any(t in desc for t in ["television series","tv series","reality show","tv show"]):
                    return item.get("id")
        time.sleep(0.3)
    except Exception as e:
        print("Wikidata search error:", e)
    return None

def get_tvdb_id(show_name: str, imdb_id: Optional[str] = None) -> Optional[str]:
    """TheTVDB v4 search by name with proper JWT handling."""
    token = tvdb_get_token()
    if not token:
        return None
    try:
        headers = {"Authorization": f"Bearer {token}", "accept": "application/json"}
        r = requests.get(
            "https://api4.thetvdb.com/v4/search",
            headers=headers,
            params={"query": show_name, "type": "series"},
            timeout=15
        )
        if r.status_code == 200:
            data = r.json().get("data", [])
            if data:
                return str(data[0].get("tvdb_id") or data[0].get("id", ""))
        elif r.status_code in (401, 403):
            # Refresh token and retry once
            token = tvdb_get_token()
            if token:
                headers["Authorization"] = f"Bearer {token}"
                r2 = requests.get(
                    "https://api4.thetvdb.com/v4/search",
                    headers=headers,
                    params={"query": show_name, "type": "series"},
                    timeout=15
                )
                if r2.status_code == 200:
                    data = r2.json().get("data", [])
                    if data:
                        return str(data[0].get("tvdb_id") or data[0].get("id", ""))
        return None
    except Exception as e:
        print(f"TheTVDB search error '{show_name}': {e}")
        return None
    
# ===============================
# Utility
# ===============================
def col_letter(idx0: int) -> str:
    return chr(ord('A') + idx0)  # good up to K

# ===============================
# Main
# ===============================
def main():
    print("Starting Show Info Collection")
    gc = sheets_client()
    ws = open_sheet(gc)

    # 1) Ensure header layout (adds I/J/K if missing, preserves OVERRIDE by remap)
    normalize_sheet_structure(ws)

    # 2) Read current sheet
    existing = read_existing_shows(ws)
    print(f"Found {len(existing)} existing rows")

    # 3) Collect IDs from both lists
    collected: Dict[str, Dict] = {}

    # TMDb list
    if TMDB_LIST_ID:
        print(f"Fetching TMDb list {TMDB_LIST_ID}…")
        tmdb_items = fetch_list_items(TMDB_LIST_ID)
        tv_items = [it for it in tmdb_items if it.get("media_type") in (None, "tv")]
        print(f"  TMDb TV items: {len(tv_items)}")
        for it in tv_items:
            tv_id = str(it.get("id"))
            name  = it.get("name") or it.get("title") or "N/A"
            collected[tv_id] = {"name": name, "tmdb_id": tv_id, "imdb_id": None, "source": "tmdb_list"}

    # IMDb list
    print("Fetching IMDb list…")
    imdb_items = fetch_imdb_list_shows()
    print(f"  IMDb items: {len(imdb_items)}")

    for s in imdb_items:
        imdb_id = s["imdb_id"]
        name    = s["name"]

        tmdb_match = search_tmdb_by_imdb_id(imdb_id) or search_tmdb_by_name(name)
        if tmdb_match:
            tv_id = str(tmdb_match.get("id"))
            if tv_id in collected:
                collected[tv_id]["imdb_id"] = imdb_id
            else:
                collected[tv_id] = {"name": tmdb_match.get("name", name), "tmdb_id": tv_id, "imdb_id": imdb_id, "source":"imdb_list_matched"}
        else:
            imdb_key = f"imdb_{imdb_id}"
            if imdb_key not in collected:
                collected[imdb_key] = {"name": name, "tmdb_id": None, "imdb_id": imdb_id, "source":"imdb_only"}

    print(f"Total collected unique shows (TMDb + IMDb-only): {len(collected)}")

    # 4) Build rows & updates
    new_rows: List[List[str]] = []
    active_ids: Set[str] = set()
    updates: List[Dict] = []  # batch updates for existing rows (Most Recent Ep + backfills)

    for key, data in collected.items():
        name    = data["name"]
        tmdb_id = data.get("tmdb_id")
        imdb_id = data.get("imdb_id", "")

        # Use the collection key as the show_id - this is already correctly formatted
        # (TMDb ID for matched shows, "imdb_" format only for unmatched IMDb-only shows)
        show_id = key
        active_ids.add(show_id)

        # Fetch the existing row (for short-circuit decisions)
        existing_row = existing.get(show_id)

        # Pull details if TMDb is known
        network_name = ""
        season_count = ""
        episode_count= ""
        most_recent  = ""
        tvdb_id      = ""
        wikidata_id  = ""

        if tmdb_id:
            try:
                det = fetch_tv_details(int(tmdb_id))
                season_count = det.get("number_of_seasons") or ""
                episode_count= det.get("number_of_episodes") or ""
                nets = det.get("networks") or []
                network_name = (nets[0]["name"] if nets else "") or ""
                if not imdb_id:
                    imdb_id = (det.get("external_ids") or {}).get("imdb_id", "") or imdb_id
                most_recent = get_most_recent_episode_date(int(tmdb_id)) or ""
            except Exception as e:
                print(f"  TMDb details error for {name} ({tmdb_id}):", e)

        # External IDs (best effort) — skip expensive calls if we already have values on this row
        if existing_row and (existing_row.get("TVdbID") or "").strip():
            tvdb_id = existing_row.get("TVdbID", "")
        else:
            try:
                tvdb_id = get_tvdb_id(name, imdb_id) or ""
            except Exception:
                tvdb_id = ""

        if existing_row and (existing_row.get("WikidataID") or "").strip():
            wikidata_id = existing_row.get("WikidataID", "")
        else:
            try:
                wikidata_id = get_wikidata_id(name, imdb_id) or ""
            except Exception:
                wikidata_id = ""

        # Construct final row
        row = [
            str(show_id),       # A Show
            name,               # B ShowName
            network_name,       # C Network
            season_count,       # D ShowTotalSeasons
            episode_count,      # E ShowTotalEpisodes
            imdb_id,            # F IMDbSeriesID
            str(tmdb_id or ""), # G TheMovieDB ID
            tvdb_id,            # H TVdbID
            most_recent,        # I Most Recent Episode
            "",                 # J OVERRIDE (empty/new)
            wikidata_id,        # K WikidataID
        ]

        if show_id in existing:
            # Existing row: targeted updates only
            r = existing[show_id]["row_index"]

            # Always refresh I: Most Recent Episode if we found a value
            if row[COLS["Most Recent Episode"]]:
                updates.append({
                    "range": f"{col_letter(COLS['Most Recent Episode'])}{r}:{col_letter(COLS['Most Recent Episode'])}{r}",
                    "values": [[row[COLS["Most Recent Episode"]]]]
                })

            # Backfill blanks (never overwrite)
            for col_name in ["Network","ShowTotalSeasons","ShowTotalEpisodes","IMDbSeriesID","TheMovieDB ID","TVdbID","WikidataID"]:
                new_val = row[COLS[col_name]]
                if new_val and not (existing[show_id].get(col_name) or "").strip():
                    updates.append({
                        "range": f"{col_letter(COLS[col_name])}{r}:{col_letter(COLS[col_name])}{r}",
                        "values": [[new_val]]
                    })
        else:
            # New row → append at bottom with all first 8 columns filled as best-effort + I and K
            new_rows.append(row)

        # Progress heartbeat every 10 items
        if len(active_ids) % 10 == 0:
            print(f"  Processed {len(active_ids)} / {len(collected)} …")

        time.sleep(0.05)

    # 5) Append new shows at bottom
    if new_rows:
        print(f"Appending {len(new_rows)} new show(s) at the bottom…")
        ws.append_rows(new_rows, value_input_option="RAW")
        print("New rows appended.")
    else:
        print("No new shows to add.")

    # 6) Apply targeted updates (existing rows: I and backfills)
    if updates:
        print(f"Applying {len(updates)} cell updates…")
        ws.batch_update(updates, value_input_option="RAW")
        print("Cell updates applied.")

    # 7) Mark rows not present in either list as SKIP (OVERRIDE J)
    print("Marking rows not found in either list as SKIP…")
    # Reload after appends
    existing = read_existing_shows(ws)
    current_ids = set(existing.keys())
    to_skip = current_ids - active_ids - {""}
    if to_skip:
        j_col = NEW_HEADERS.index("OVERRIDE") + 1  # 1-based
        for sid in sorted(to_skip):
            r = existing[sid]["row_index"]
            cur = (existing[sid].get("OVERRIDE") or "").strip().upper()
            if cur != "SKIP":
                ws.update_cell(r, j_col, "SKIP")
                print(f"  Marked row {r} ({sid}) as SKIP")
            time.sleep(0.03)
        print("SKIP marking complete.")
    else:
        print("No rows require SKIP.")

    print("Done. A–H filled for new rows, I/J/K maintained, removed shows → SKIP, existing data preserved.")

if __name__ == "__main__":
    main()