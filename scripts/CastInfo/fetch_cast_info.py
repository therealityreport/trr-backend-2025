# scripts/fetch_cast_info.py
# Update-in-place builder for CastInfo:
#   • Phase 1: fill Seasons (col F) for existing (CastID, ShowID) rows only
#   • Phase 2: optionally append NEW rows that don't exist yet (can be disabled)
#   • One row per (Cast × Show). Uses TMDb API only (no scraping).

import os
import time
from typing import Any, Dict, List, Optional, Tuple
import argparse

import gspread
import requests
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError as GspreadAPIError
from pathlib import Path
import sys

# ---------------- Tunables ----------------

# --- New CLI/Env flags ---
APPEND_ONLY = os.getenv("APPEND_ONLY", "0") == "1"  # Only append missing rows, do not update existing
NO_UPDATES = os.getenv("NO_UPDATES", "0") == "1"    # Skip updating existing rows entirely
LANG = "en-US"
RATE_DELAY = float(os.getenv("RATE_DELAY", "1.0"))  # seconds between TMDb calls
BATCH_APPEND_DELAY = 0.6
TMDB_MAX_RETRIES = int(os.getenv("TMDB_MAX_RETRIES", "3"))
RETRY_BACKOFF_BASE = float(os.getenv("RETRY_BACKOFF_BASE", "1.5"))
ONLY_UPDATE_SEASONS = os.getenv("ONLY_UPDATE_SEASONS", "0") == "1"  # if true, skip appending
UPDATE_EXISTING = os.getenv("UPDATE_EXISTING", "1") == "1"  # if false, skip updating existing rows

# --- Resume index helper ---

# --- CLI flags helper ---
def parse_cli_flags():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--start-at", type=int, help="Start at Nth data row (row 2 == 1).")
    parser.add_argument("--start-at-row", type=int, help="Start at absolute sheet row (e.g., 70).")
    parser.add_argument("--append-only", action="store_true", help="Do not update existing rows, only append missing ones.")
    parser.add_argument("--no-updates", action="store_true", help="Skip updating existing rows.")
    parser.add_argument("--test-write", action="store_true", help="Perform a single test write to CastInfo!A2 and exit.")
    args, _ = parser.parse_known_args()
    return {
        "start_at": args.start_at,
        "start_at_row": args.start_at_row,
        "append_only": args.append_only,
        "no_updates": args.no_updates,
        "test_write": args.test_write,
    }

def parse_resume_index(total_shows: int) -> int:
    """
    Determine the starting data index (1-based) from CLI or env.
    Precedence: --start-at-row > --start-at > START_AT_ROW > START_AT > default(2)
    Note: Sheet row 2 corresponds to data index 1.
    """
    flags = parse_cli_flags()
    start_at: Optional[int] = None
    src = "default"

    if flags.get("start_at_row"):
        start_at = max(1, flags["start_at_row"] - 1)
        src = "cli --start-at-row"
    elif flags.get("start_at"):
        start_at = max(1, flags["start_at"])
        src = "cli --start-at"
    elif os.getenv("START_AT_ROW"):
        try:
            start_at = max(1, int(os.getenv("START_AT_ROW")) - 1)
            src = "env START_AT_ROW"
        except Exception:
            pass
    elif os.getenv("START_AT"):
        try:
            start_at = max(1, int(os.getenv("START_AT")))
            src = "env START_AT"
        except Exception:
            pass

    # Default to starting at sheet row 2 unless overridden
    DEFAULT_START_ROW = int(os.getenv("DEFAULT_START_ROW", "2"))
    if not start_at:
        start_at = max(1, DEFAULT_START_ROW - 1)
        src = f"default row {DEFAULT_START_ROW}"

    # Friendly log
    print(
        f"Resume: {src} -> start at data index {start_at} "
        f"(sheet row {start_at + 1}); will skip {max(0, start_at - 1)} of {total_shows}"
    )
    return start_at

# ---------------- Env ----------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = PROJECT_ROOT / ".env"

load_dotenv(dotenv_path=str(ENV_FILE), override=True)

# We'll read credentials after parsing CLI args so the script can print helpful errors
GOOGLE_CREDS: Optional[str] = None
SPREADSHEET_ID: Optional[str] = None
TMDB_BEARER: Optional[str] = None

# Spreadsheet client placeholder (initialized in main)
sh: Optional[gspread.Spreadsheet] = None
client: Optional[gspread.Client] = None
def need(name: str) -> str:
    # Prefer actual process env
    v = os.getenv(name)
    if v:
        return v

    # Fallback: try a simple .env parser if python-dotenv failed to load
    try:
        for line in SIMPLE_ENV.items():
            pass
    except NameError:
        # Build a tiny, robust parser: KEY=VALUE ignoring quotes and comments
        SIMPLE_ENV = {}
        try:
            with open(ENV_FILE, "r", encoding="utf-8") as fh:
                for raw in fh:
                    line = raw.strip()
                    if not line or line.startswith("#"):
                        continue
                    if "=" not in line:
                        continue
                    k, val = line.split("=", 1)
                    k = k.strip()
                    vraw = val.strip()
                    # strip surrounding single/double quotes
                    if (vraw.startswith('"') and vraw.endswith('"')) or (vraw.startswith("'") and vraw.endswith("'")):
                        vraw = vraw[1:-1]
                    SIMPLE_ENV[k] = vraw
        except Exception:
            SIMPLE_ENV = {}

    if name in SIMPLE_ENV:
        return SIMPLE_ENV[name]

    # Special-case: if GOOGLE_APPLICATION_CREDENTIALS missing, try to find a key file under ./keys/
    if name == "GOOGLE_APPLICATION_CREDENTIALS":
        keys_dir = PROJECT_ROOT / "keys"
        if keys_dir.exists() and keys_dir.is_dir():
            for p in keys_dir.iterdir():
                if p.suffix == ".json":
                    return str(p)

    raise ValueError(
        f"Missing {name} in environment. Looked in process env and {ENV_FILE}.\n"
        f"→ Ensure it exists in {ENV_FILE} as a simple line: {name}=<value> (no quotes).\n"
        f"→ Current CWD: {os.getcwd()}\n"
    )


def load_simple_env() -> Dict[str, str]:
    """Parse the project's .env file into a dict (robust, minimal parser)."""
    env_map: Dict[str, str] = {}
    try:
        with open(ENV_FILE, "r", encoding="utf-8") as fh:
            for raw in fh:
                line = raw.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" not in line:
                    continue
                k, val = line.split("=", 1)
                k = k.strip()
                vraw = val.strip()
                if (vraw.startswith('"') and vraw.endswith('"')) or (vraw.startswith("'") and vraw.endswith("'")):
                    vraw = vraw[1:-1]
                env_map[k] = vraw
    except Exception:
        pass
    return env_map

# ---------------- Google Sheets ----------------

def get_or_create_worksheet(title: str, headers: Optional[List[str]] = None) -> gspread.Worksheet:
    try:
        ws = sh.worksheet(title)
        print(f"DEBUG: opened worksheet {title}")
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=200, cols=20)
        print(f"DEBUG: created worksheet {title}")
    # Only seed headers if the sheet is empty
    if headers:
        current = ws.row_values(1)
        if not any(current):
            ws.update("A1", [headers])
            print(f"DEBUG: seeded headers for worksheet {title}: {headers}")
    return ws

# ---------------- TMDb ----------------
session = requests.Session()

def tmdb_get(url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    params = params or {}
    params.setdefault("language", LANG)
    last_err: Optional[Exception] = None
    for attempt in range(1, TMDB_MAX_RETRIES + 1):
        try:
            r = session.get(url, params=params, timeout=30)
            if r.status_code == 200:
                time.sleep(RATE_DELAY)
                return r.json()
            # Retry on common transient statuses
            if r.status_code in (429, 500, 502, 503, 504):
                wait = RETRY_BACKOFF_BASE ** attempt
                print(f"TMDb {r.status_code}; retrying in {wait:.1f}s… ({attempt}/{TMDB_MAX_RETRIES}) -> {url}")
                time.sleep(wait)
                continue
            # Non-retriable
            raise RuntimeError(f"TMDb GET {url} -> {r.status_code} {r.text}")
        except requests.exceptions.RequestException as e:
            last_err = e
            wait = RETRY_BACKOFF_BASE ** attempt
            print(f"TMDb request error: {e}; retrying in {wait:.1f}s… ({attempt}/{TMDB_MAX_RETRIES}) -> {url}")
            time.sleep(wait)
            continue
    # Exhausted retries
    raise RuntimeError(f"TMDb GET {url} failed after {TMDB_MAX_RETRIES} retries: {last_err}")

def tv_details(tv_id: str) -> Dict[str, Any]:
    return tmdb_get(f"https://api.themoviedb.org/3/tv/{tv_id}")

def series_aggregate_cast(tv_id: str) -> List[Dict[str, Any]]:
    data = tmdb_get(f"https://api.themoviedb.org/3/tv/{tv_id}/aggregate_credits")
    return data.get("cast") or []

def season_cast_any(tv_id: str, season_number: int) -> List[Dict[str, Any]]:
    """Return cast for a season using /aggregate_credits. If 404, treat as empty."""
    url_agg = f"https://api.themoviedb.org/3/tv/{tv_id}/season/{season_number}/aggregate_credits"
    try:
        data = tmdb_get(url_agg)
        return data.get("cast") or []
    except RuntimeError as e:
        msg = str(e)
        if "404" in msg or "could not be found" in msg:
            # Treat missing season credits as empty rather than erroring.
            return []
        raise

# ---------------- Build rows for one show ----------------

def rows_for_show(tv_id: str, show_name: str, min_eps: int, season_count_hint: Optional[int]) -> Tuple[List[List[Any]], Dict[int, str]]:
    """
    Returns:
      rows: [[CastID, CastName, ShowID, ShowName, EpisodeCount, Seasons, ""]]
      seasons_by_cast_text: {cast_id: "1, 3, 5"}
    """
    # Get season count
    season_count = int(season_count_hint or 0)
    if season_count <= 0:
        details = tv_details(tv_id)
        season_count = int(details.get("number_of_seasons") or 0)

    # Series-level cast for totals + names
    series_cast = series_aggregate_cast(tv_id)
    per_cast: Dict[int, Dict[str, Any]] = {}
    for c in series_cast:
        cid = c.get("id")
        if cid is None:
            continue
        total = int(c.get("total_episode_count") or 0)
        if total < min_eps:
            continue
        name = c.get("name") or c.get("original_name") or ""
        per_cast[int(cid)] = {"name": name, "total": total}

    # If series cast empty, derive totals later from seasons
    if not per_cast:
        seen: Dict[int, int] = {}
        for s in range(1, season_count + 1):
            for cm in season_cast_any(tv_id, s):
                cid = cm.get("id")
                if cid is None:
                    continue
                seen[int(cid)] = seen.get(int(cid), 0) + 1
        # Names may still be missing here if aggregate credits were empty
        for cid, cnt in seen.items():
            if cnt >= max(1, min_eps):
                per_cast[cid] = {"name": "", "total": cnt}

    # Build seasons list per cast
    seasons_by_cast: Dict[int, List[int]] = {cid: [] for cid in per_cast.keys()}

    # 1) OLD METHOD (preferred): seed from series-level aggregate_credits 'roles'
    for c in series_cast:
        cid = c.get("id")
        if cid is None:
            continue
        icid = int(cid)
        if icid not in seasons_by_cast:
            continue
        for role in c.get("roles", []):
            sn = role.get("season")
            if isinstance(sn, int):
                seasons_by_cast[icid].append(sn)

    # 2) Fallback: if some casts still have no seasons, probe each season's aggregate_credits
    if season_count > 0:
        for s in range(1, season_count + 1):
            season_cast = season_cast_any(tv_id, s)
            if not season_cast:
                continue
            present = {int(cm.get("id")) for cm in season_cast if cm.get("id") is not None}
            for cid in list(per_cast.keys()):
                if cid in present and s not in seasons_by_cast[cid]:
                    seasons_by_cast[cid].append(s)

    # Emit rows + seasons text map
    out: List[List[Any]] = []
    seasons_text: Dict[int, str] = {}
    for cid, info in per_cast.items():
        name = info["name"]
        total = int(info["total"])
        seasons_sorted = sorted(set(seasons_by_cast.get(cid, [])))
        seasons_txt = ", ".join(map(str, seasons_sorted))
        seasons_text[cid] = seasons_txt
        out.append([cid, name, tv_id, show_name, total, seasons_txt, ""])  # Seasons-Update empty
    return out, seasons_text

# ---------------- Helpers ----------------

def build_cast_index(ws: gspread.Worksheet) -> Tuple[Dict[Tuple[str, str], int], int, int, int]:
    """Return (index, idx_cast, idx_show, idx_seasons).
    index maps (CastID, ShowID) -> row_number (1-based).
    """
    values = ws.get_all_values()
    if not values:
        return {}, -1, -1, -1
    header = [h.strip() for h in values[0]]
    def idx(name: str) -> int:
        try:
            return header.index(name)
        except ValueError:
            return -1
    idx_cast = idx("CastID")
    idx_show = idx("ShowID")
    idx_seasons = idx("Seasons")
    index: Dict[Tuple[str, str], int] = {}
    for i, row in enumerate(values[1:], start=2):  # start at row 2
        if idx_cast == -1 or idx_show == -1:
            break
        k = (row[idx_cast].strip(), row[idx_show].strip())
        if k[0] and k[1]:
            index[k] = i
    print(f"Loaded CastInfo index with {len(index)} rows.")
    return index, idx_cast, idx_show, idx_seasons


def update_seasons_batch(ws: gspread.Worksheet, updates: Dict[int, str]):
    if not updates:
        return
    # Build batch updates for scattered single cells in the 'Seasons' column.
    # Find header column dynamically; fall back to F if not found.
    header = ws.row_values(1)
    col_idx = -1
    for i, h in enumerate(header):
        if (h or "").strip().lower() == "seasons":
            col_idx = i + 1
            break
    if col_idx == -1:
        # default to column F
        col_idx = 6

    def col_letter(n: int) -> str:
        s = ""
        while n > 0:
            n, rem = divmod(n - 1, 26)
            s = chr(ord("A") + rem) + s
        return s

    col = col_letter(col_idx)
    data = []
    for row, text in updates.items():
        # include sheet title to avoid ambiguous ranges and properly target the worksheet
        data.append({"range": f"'{ws.title}'!{col}{row}", "values": [[text]]})
    # Chunk to avoid payload too large
    CHUNK = 500
    for i in range(0, len(data), CHUNK):
        chunk = data[i:i+CHUNK]
        body = {"valueInputOption": "RAW", "data": chunk}
        # Ensure the sheet has enough rows for any target range in this chunk
        max_row = 0
        for item in chunk:
            # range looks like "'Sheet Title'!F1234" -> extract trailing digits
            try:
                rng = item.get("range", "")
                tail = rng.split("!")[-1]
                # strip column letters
                digits = ''.join(ch for ch in tail if ch.isdigit())
                if digits:
                    max_row = max(max_row, int(digits))
            except Exception:
                pass
        try:
            current_rows = int(ws.row_count)
        except Exception:
            current_rows = 0
        if max_row > current_rows:
            add_n = max_row - current_rows
            print(f"    DEBUG: expanding worksheet '{ws.title}' rows from {current_rows} to {max_row} (+{add_n})")
            try:
                ws.add_rows(add_n)
            except Exception as e:
                print(f"    ! failed to add rows: {e}")
        # Use the values:batchUpdate endpoint via gspread's values_batch_update
        try:
            resp = ws.spreadsheet.values_batch_update(body)
            try:
                print(f"    ↳ values_batch_update OK; sent {len(chunk)} ranges; resp_keys={list(resp.keys())}")
            except Exception:
                print(f"    ↳ values_batch_update OK; sent {len(chunk)} ranges")
        except Exception as e:
            print(f"    ! values_batch_update error: {e}")
            # Re-raise after logging to let caller handle/report
            raise
        time.sleep(0.4)


def append_in_batches(ws: gspread.Worksheet, rows: List[List[Any]], batch_size=100):
    for i in range(0, len(rows), batch_size):
        chunk = rows[i:i + batch_size]
        attempts = 0
        while True:
            try:
                ws.append_rows(chunk, value_input_option="RAW")
                try:
                    values = ws.get_all_values()
                    print(f"    ↳ appended {len(chunk)} rows to '{ws.title}' (sheet rows now={len(values)})")
                except Exception:
                    print(f"    ↳ appended {len(chunk)} rows to '{ws.title}' (append call returned)")
                break
            except GspreadAPIError as e:
                msg = str(e)
                if "Quota exceeded" in msg or "429" in msg:
                    attempts += 1
                    wait_s = min(60 * attempts, 180)
                    print(f"    ! Sheets 429 rate limit. Sleeping {wait_s}s…")
                    time.sleep(wait_s)
                    continue
                # Some GspreadAPIError messages wrap lower-level connection resets
                if "Connection reset by peer" in msg or "Read timed out" in msg:
                    attempts += 1
                    wait_s = min(30 * attempts, 120)
                    print(f"    ! Sheets transient error '{msg}'. Sleeping {wait_s}s…")
                    time.sleep(wait_s)
                    continue
                raise
            except requests.exceptions.RequestException as e:
                attempts += 1
                wait_s = min(30 * attempts, 120)
                print(f"    ! Sheets connection error '{e}'. Sleeping {wait_s}s…")
                time.sleep(wait_s)
                continue
            except Exception as e:
                # Last-chance retry for typical network flakiness
                if "Connection reset by peer" in str(e) or "Read timed out" in str(e):
                    attempts += 1
                    wait_s = min(30 * attempts, 120)
                    print(f"    ! Sheets transient error '{e}'. Sleeping {wait_s}s…")
                    time.sleep(wait_s)
                    continue
                raise
        time.sleep(BATCH_APPEND_DELAY)

# ---------------- Main ----------------

def main():
    # Sheets
    global GOOGLE_CREDS, SPREADSHEET_ID, TMDB_BEARER, sh, client
    # Read required envs/creds now and initialize clients so earlier missing-env errors are clearer
    simple_env = load_simple_env()
    GOOGLE_CREDS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS") or simple_env.get("GOOGLE_APPLICATION_CREDENTIALS")
    # fallback: pick a keys/*.json file if present
    if not GOOGLE_CREDS:
        keys_dir = PROJECT_ROOT / "keys"
        if keys_dir.exists() and keys_dir.is_dir():
            jsons = sorted([p for p in keys_dir.iterdir() if p.suffix == ".json"])
            if jsons:
                GOOGLE_CREDS = str(jsons[0])

    SPREADSHEET_ID = os.getenv("SPREADSHEET_ID") or simple_env.get("SPREADSHEET_ID")
    TMDB_BEARER = os.getenv("TMDB_BEARER") or simple_env.get("TMDB_BEARER")

    if not GOOGLE_CREDS:
        raise ValueError(f"Missing GOOGLE_APPLICATION_CREDENTIALS: set env or put a JSON key under {PROJECT_ROOT}/keys/")
    if not SPREADSHEET_ID:
        # Diagnostic output to help debug python-dotenv / parsing issues
        print("DEBUG: SPREADSHEET_ID not found. Inspecting .env and parser output:")
        print(f"  .env path: {ENV_FILE} exists={ENV_FILE.exists()}")
        try:
            st = ENV_FILE.stat()
            print(f"  .env size={st.st_size} bytes, mtime={st.st_mtime}")
        except Exception as e:
            print(f"  .env stat error: {e}")
        try:
            with open(ENV_FILE, 'rb') as f:
                raw = f.read()
            print(f"  .env raw first 200 bytes repr: {repr(raw[:200])}")
        except Exception as e:
            print(f"  .env read error: {e}")
        print(f"  simple_env parsed keys: {list(simple_env.keys())}")
        # also print enumerated lines for clarity
        try:
            with open(ENV_FILE, 'r', encoding='utf-8', errors='replace') as fh:
                for idx, ln in enumerate(fh, start=1):
                    line_repr = repr(ln.rstrip('\n'))
                    print("  L%03d: %s" % (idx, line_repr))
        except Exception as e:
            print(f"  .env line-read error: {e}")
        raise ValueError("Missing SPREADSHEET_ID in environment or .env")
    if not TMDB_BEARER:
        raise ValueError("Missing TMDB_BEARER in environment or .env")

    creds = Credentials.from_service_account_file(
        GOOGLE_CREDS,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    client = gspread.authorize(creds)
    sh = client.open_by_key(SPREADSHEET_ID)
    try:
        print(f"DEBUG: opened spreadsheet title={sh.title!r} id={SPREADSHEET_ID}")
    except Exception:
        print(f"DEBUG: opened spreadsheet id={SPREADSHEET_ID}")

    # set TMDb header now that bearer is available
    session.headers.update({"Authorization": f"Bearer {TMDB_BEARER}", "accept": "application/json"})

    show_ws = get_or_create_worksheet("ShowInfo")
    cast_ws = get_or_create_worksheet(
        "CastInfo",
        ["CastID", "CastName", "ShowID", "ShowName", "EpisodeCount", "Seasons", "Seasons-Update"],
    )

    # Build index of existing CastInfo BEFORE any appends so row numbers remain stable
    index, idx_cast, idx_show, idx_seasons = build_cast_index(cast_ws)

    # Read ShowInfo
    show_rows = show_ws.get_all_values()
    if not show_rows:
        print("ShowInfo is empty.")
        return

    header = [h.strip() for h in show_rows[0]]
    rows = show_rows[1:]
    total_shows = len(rows)
    # --- TEMPORARY OVERRIDE: force start at ShowInfo sheet ROW 70 ---
    # Data rows start at sheet row 2 -> data index 1. Therefore row 70 -> data index 69.
    # This override ignores CLI/env resume flags for this run to quickly backfill initial data.
    start_at = 69
    print(f"FORCE START: beginning at ShowInfo sheet row 70 (data index {start_at}).")

    # Parse CLI flags a second time for mode switches
    flags = parse_cli_flags()
    update_existing = UPDATE_EXISTING and not (APPEND_ONLY or flags.get('append_only')) and not (NO_UPDATES or flags.get('no_updates'))
    append_only = APPEND_ONLY or flags.get('append_only')
    no_updates = NO_UPDATES or flags.get('no_updates')

    # --- TEMPORARY MODE OVERRIDE: append NEW rows only, do not update existing rows ---
    update_existing = False      # disable updates to existing CastInfo rows
    append_only = True           # only append missing rows
    no_updates = True            # skip all updates
    # ensure seasons appends are allowed (do not block with ONLY_UPDATE_SEASONS)
    globals()['ONLY_UPDATE_SEASONS'] = False
    print("FORCE MODE: append_only=True, no_updates=True, ONLY_UPDATE_SEASONS=False (no changes to existing rows)")

    # If user requested a test write, attempt a single cell update and exit
    if flags.get("test_write"):
        print("Performing test write to CastInfo!A2...")
        try:
            cast_ws.update("A2", [["TEST_WRITE"]])
            print("Test write OK: wrote 'TEST_WRITE' to CastInfo!A2")
        except Exception as e:
            print(f"Test write FAILED: {e}")
        return

    def hidx(name: str) -> int:
        try:
            return header.index(name)
        except ValueError:
            return -1

    idx_show_id = hidx("ShowID")
    # Grab both columns so we can fall back per-row: if ShowNameEdit is empty, use ShowName
    idx_show_name_edit = hidx("ShowNameEdit")
    idx_show_name_fallback = hidx("ShowName")
    idx_season_ct = hidx("SeasonCount")
    # Allow either MinimumEpisodes or MinEpisode
    idx_min_eps = hidx("MinimumEpisodes") if hidx("MinimumEpisodes") != -1 else hidx("MinEpisode")

    print(
        f"ONLY_UPDATE_SEASONS={ONLY_UPDATE_SEASONS} | UPDATE_EXISTING={UPDATE_EXISTING} | "
        f"APPEND_ONLY={APPEND_ONLY} | NO_UPDATES={NO_UPDATES} (force overrides active)"
    )

    # Process shows one-by-one and commit updates/appends per show.
    for i, row in enumerate(rows, start=1):
        if i < start_at:
            continue

        tv_id = (row[idx_show_id] or "").strip() if idx_show_id != -1 else ""
        if not tv_id:
            print(f"[{i}/{len(rows)}] (skip) missing ShowID")
            continue

        show_name = ""
        if idx_show_name_edit != -1:
            show_name = (row[idx_show_name_edit] or "").strip()
        if (not show_name) and idx_show_name_fallback != -1:
            show_name = (row[idx_show_name_fallback] or "").strip()

        season_hint = None
        if idx_season_ct != -1:
            try:
                season_hint = int(row[idx_season_ct] or 0)
            except ValueError:
                season_hint = None

        min_eps = 0
        if idx_min_eps != -1:
            try:
                min_eps = int(row[idx_min_eps] or 0)
            except ValueError:
                min_eps = 0

        try:
            out_rows, seasons_text = rows_for_show(tv_id, show_name, min_eps, season_hint)
        except Exception as e:
            short = (show_name or tv_id)[:14]
            print(f"[{i}/{len(rows)}] {short} -> ERROR: {e}")
            continue

        # Partition into updates vs appends using the CURRENT index snapshot
        updates_for_show: Dict[int, str] = {}
        to_append_for_show: List[List[Any]] = []
        appended_season_targets: Dict[Tuple[str, str], str] = {}

        for r in out_rows:
            cast_id = str(r[0])
            seasons_txt = seasons_text.get(int(cast_id), "") or ""
            key = (cast_id, tv_id)
            rownum = index.get(key)
            if rownum:
                if idx_seasons != -1:
                    updates_for_show[rownum] = seasons_txt
            else:
                to_append_for_show.append(r)
                appended_season_targets[key] = seasons_txt

        # Debug sample for would-be appended rows
        if to_append_for_show:
            sample_row = to_append_for_show[0]
            sample_key = (str(sample_row[0]), tv_id)
            print(f"      · sample new key {sample_key} (CastID, ShowID)")

        short = (show_name or tv_id)[:24]
        existing_ct = len(updates_for_show)
        append_ct = len(to_append_for_show)
        total_candidates = len(out_rows)
        print(
            f"[{i}/{len(rows)}] {short} -> candidates={total_candidates}, will_update={existing_ct}, will_append={append_ct} "
            f"(ONLY_UPDATE_SEASONS={ONLY_UPDATE_SEASONS}, append_only={append_only}, no_updates={no_updates})"
        )
        if append_ct == 0 and total_candidates > 0 and not update_existing:
            print("      • Note: No appends because all (CastID, ShowID) pairs already exist in CastInfo. With UPDATE_EXISTING=0, nothing is changed for this show.")

        # Apply season updates for existing rows (column F) unless disabled
        if updates_for_show and not (append_only or no_updates):
            update_seasons_batch(cast_ws, updates_for_show)
            print(f"      ✓ wrote Seasons to {len(updates_for_show)} existing rows")
        elif updates_for_show:
            print(f"      ↷ skipping {len(updates_for_show)} existing-row updates (append_only={append_only} no_updates={no_updates})")

        # Append NEW rows for this show immediately (ALWAYS append under forced mode)
        if to_append_for_show:
            # ALWAYS append under forced mode
            print(f"      → Appending {len(to_append_for_show)} new rows to CastInfo…")
            append_in_batches(cast_ws, to_append_for_show, batch_size=100)
            print(f"      ✓ Appended {len(to_append_for_show)} rows.")
            # After appending, refresh index and set Seasons for those new rows
            index, _, _, _ = build_cast_index(cast_ws)
            updates_for_new: Dict[int, str] = {}
            for key, seasons_txt in appended_season_targets.items():
                rownum = index.get(key)
                if rownum:
                    updates_for_new[rownum] = seasons_txt
            if updates_for_new:
                update_seasons_batch(cast_ws, updates_for_new)
                print(f"      ✓ Set Seasons for {len(updates_for_new)} newly appended rows.")
        else:
            print("      (No new rows to append; all candidates already present)")

    print("Done.")


if __name__ == "__main__":
    main()