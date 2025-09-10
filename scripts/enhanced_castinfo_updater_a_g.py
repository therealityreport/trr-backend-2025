#!/usr/bin/env python3
"""
Enhanced CastInfo Updater for Shows A-G
========================================

Updates CastInfo with accurate episode counts AND adds missing cast members
using smart         pri    def get_shows_a_g(self) -> Tuple[List[Dict[str, str]], List[str]]:
        """Get shows with data in columns A-G that meet criteria AND shows to remove."""
        try:
            ws = self.ss.worksheet("ShowInfo")
            rows = ws.get_all_values()
        except Exception as e:
            print(f"❌ Error reading ShowInfo: {e}")
            return [], []
        
        # Get existing show IMDb IDs from CastInfo
        existing_show_ids = self.get_existing_show_ids()
        print(f"Found {len(existing_show_ids)} shows already in CastInfo")
        
        shows = []
        shows_to_remove = []
        missing_count = 0
        qualifying_count = 0
        
        print(f"
🔍 Analyzing ALL shows with data in columns A-G:")
        
        for row in rows[1:]:  # Skip header
            if len(row) < 10:
                continue
                
            show_name = row[1].strip()      # Column B
            imdb_id = row[5].strip()        # Column F  
            recent_episode = row[8].strip() # Column I: Most Recent Episode
            threshold_flag = row[9].strip() # Column J: Episode threshold or SKIP
            
            if not show_name or not imdb_id:
                continue
                
            # Check if show has data in columns A-G (we need at least show name and IMDb ID)
            # This processes ALL shows regardless of starting letter
            has_data_in_columns_a_g = bool(show_name and imdb_id)
            if not has_data_in_columns_a_g:
                continue
            
            # Handle SKIP shows - mark for removal
            if threshold_flag == "SKIP":
                if imdb_id in existing_show_ids:
                    shows_to_remove.append(imdb_id)
                    print(f"  🗑️  Will remove: {show_name} (marked SKIP)")
                continue
            
            # Check if show exists in CastInfo - we'll process both existing and new shows
            already_in_castinfo = imdb_id in existing_show_ids
            if already_in_castinfo:
                print(f"  🔄 Will update: {show_name} (already in CastInfo - will update episodes)")
            else:
                print(f"  ➕ Will add: {show_name} (missing from CastInfo)")
                missing_count += 1
            
            # Parse episode threshold from Column J
            min_episodes = 2  # default for blank entries
            if threshold_flag == "Y":
                min_episodes = 1
            elif threshold_flag and threshold_flag.isdigit():
                min_episodes = int(threshold_flag)
            elif threshold_flag and not threshold_flag == "SKIP":
                # Non-numeric, non-Y, non-SKIP value - use default
                min_episodes = 2
            
            # Special overrides for specific shows
            if "X Factor" in show_name:
                min_episodes = 6  # Override for X Factor shows
            
            shows.append({
                "name": show_name,
                "imdb_id": imdb_id,
                "min_episodes": min_episodes,
                "recent_episode": recent_episode
            })
            qualifying_count += 1
            
            print(f"  📺 {show_name} - Min episodes: {min_episodes} (from column J: '{threshold_flag}')")
        
        print(f"
📊 Show filtering results:")
        print(f"  🔍 Shows missing from CastInfo: {missing_count}")
        print(f"  🎯 Qualifying shows to process: {qualifying_count}")
        print(f"  🗑️  Shows to remove (SKIP): {len(shows_to_remove)}")
        
        return shows, shows_to_remove\n📊 Show filtering results:")
        print(f"  🔍 A-G shows missing from CastInfo: {missing_count}")
        print(f"  📅 Shows with recent episodes (past 10 days): {recent_count}")
        print(f"  🎯 Qualifying shows (ALL missing A-G shows): {qualifying_count}")
        print(f"  🗑️  Shows to remove (SKIP): {len(shows_to_remove)}")ring rules:

1. New people (not in CastInfo): Need 3+ episodes 
2. Existing people (already in CastInfo): Can add with 1+ episodes
3. Special shows (Celebrity Family Feud, RuPaul, Wife Swap): 1+ episode OK for new people
4. No episode data: Don't add
"""

import gspread
import requests
import time
from typing import Dict, List, Set, Tuple

class IMDbAPIClient:
    """Client for IMDbAPI.dev to get accurate episode counts."""
    
    def __init__(self):
        self.base_url = "https://api.imdbapi.dev"
        self.session = requests.Session()
        self.session.timeout = 30  # 30 second timeout
        self._cache = {}
        
    def get_all_cast_credits(self, title_id: str) -> Dict[str, dict]:
        """Get all cast credits for a title (with caching)."""
        if title_id in self._cache:
            return self._cache[title_id]
            
        cast_data = {}
        next_token = None
        max_pages = 10  # Prevent infinite loops
        page_count = 0
        
        try:
            while page_count < max_pages:
                url = f"{self.base_url}/titles/{title_id}/credits"
                params = {"categories": "self"}
                if next_token:
                    params["pageToken"] = next_token
                    
                print(f"    🌐 API request page {page_count + 1} for {title_id}...")
                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                credits = data.get("credits", [])
                print(f"    📄 Got {len(credits)} credits on page {page_count + 1}")
                
                for credit in credits:
                    name_data = credit.get("name", {})
                    name_id = name_data.get("id", "")
                    
                    if name_id and name_id.startswith("nm"):
                        cast_data[name_id] = {
                            "name": name_data.get("displayName", ""),
                            "episodes": credit.get("episodeCount", 0),
                            "characters": credit.get("characters", []),
                        }
                
                next_token = data.get("nextPageToken")
                page_count += 1
                
                if not next_token:
                    break
                    
                time.sleep(0.5)
                
        except requests.exceptions.Timeout:
            print(f"    ⏰ API request timed out for {title_id}")
        except requests.exceptions.RequestException as e:
            print(f"    ⚠️  API request failed for {title_id}: {e}")
        except Exception as e:
            print(f"    ⚠️  Failed to fetch cast for {title_id}: {e}")
        
        self._cache[title_id] = cast_data
        print(f"    ✅ Cached {len(cast_data)} cast members for {title_id}")
        return cast_data

class EnhancedCastInfoUpdater:
    """Enhanced updater that both updates existing entries and adds missing cast."""
    
    def __init__(self):
        # Connect to Google Sheets
        SERVICE_KEY_PATH = "/Users/thomashulihan/Projects/TRR-Backend/keys/trr-backend-df2c438612e1.json"
        gc = gspread.service_account(filename=SERVICE_KEY_PATH)
        self.ss = gc.open("Realitease2025Data")
        self.imdb_api = IMDbAPIClient()
    
    def get_shows_a_g(self) -> Tuple[List[Dict[str, str]], List[str]]:
        """Get shows with data in columns A-G that meet criteria AND shows to remove."""
        try:
            ws = self.ss.worksheet("ShowInfo")
            rows = ws.get_all_values()
        except Exception as e:
            print(f"❌ Error reading ShowInfo: {e}")
            return [], []
        
        # Get existing show IMDb IDs from CastInfo
        existing_show_ids = self.get_existing_show_ids()
        print(f"Found {len(existing_show_ids)} shows already in CastInfo")
        
        # Get current date for recent episode check
        from datetime import datetime, timedelta
        current_date = datetime.now()
        ten_days_ago = current_date - timedelta(days=10)
        
        shows = []
        shows_to_remove = []
        recent_count = 0
        missing_count = 0
        qualifying_count = 0
        
        print(f"\n🔍 Analyzing ALL shows with data in columns A-G:")
        
        for row in rows[1:]:  # Skip header
            if len(row) < 10:
                continue
                
            show_name = row[1].strip()      # Column B
            imdb_id = row[5].strip()        # Column F  
            recent_episode = row[8].strip() # Column I: Most Recent Episode
            threshold_flag = row[9].strip() # Column J: Episode threshold or SKIP
            
            if not show_name or not imdb_id:
                continue
                
            # Check if show has data in columns A-G (we need at least show name and IMDb ID)
            # This processes ALL shows regardless of starting letter
            has_data_in_columns_a_g = bool(show_name and imdb_id)
            if not has_data_in_columns_a_g:
                continue
            
            # Handle SKIP shows - mark for removal
            if threshold_flag == "SKIP":
                if imdb_id in existing_show_ids:
                    shows_to_remove.append(imdb_id)
                    print(f"  🗑️  Will remove: {show_name} (marked SKIP)")
                continue
            
            # Check if show exists in CastInfo - we'll process both existing and new shows
            already_in_castinfo = imdb_id in existing_show_ids
            if already_in_castinfo:
                print(f"  🔄 Will update: {show_name} (already in CastInfo - will update episodes)")
            else:
                print(f"  ➕ Will add: {show_name} (missing from CastInfo)")
                missing_count += 1
            
            # Check if recent episode is within 10 days
            is_recent = False
            episode_age_info = ""
            if recent_episode:
                try:
                    episode_date = None
                    for date_format in ['%Y-%m-%d', '%m/%d/%Y', '%m-%d-%Y', '%Y/%m/%d']:
                        try:
                            episode_date = datetime.strptime(recent_episode, date_format)
                            break
                        except ValueError:
                            continue
                    
                    if episode_date:
                        days_ago = (current_date - episode_date).days
                        episode_age_info = f"({days_ago} days ago)"
                        if days_ago <= 10:
                            is_recent = True
                            recent_count += 1
                except:
                    episode_age_info = "(invalid date)"
            else:
                episode_age_info = "(no recent episode date)"
            
            # Show analysis for missing shows - process ALL missing A-G shows
            # Parse episode threshold
            min_episodes = 3  # default
            if threshold_flag == "Y":
                min_episodes = 1
            else:
                try:
                    min_episodes = int(threshold_flag)
                except:
                    min_episodes = 3
            
            # Special overrides for specific shows
            if "X Factor" in show_name:
                min_episodes = 6  # Override for X Factor shows
            
            shows.append({
                "name": show_name,
                "imdb_id": imdb_id,
                "min_episodes": min_episodes,
                "recent_episode": recent_episode
            })
            qualifying_count += 1
            
            if is_recent:
                print(f"  ✅ {show_name} - Recent: {recent_episode} {episode_age_info}, Min episodes: {min_episodes}")
            else:
                print(f"  📺 {show_name} - Not recent: {recent_episode} {episode_age_info}, Min episodes: {min_episodes}")
        
        print(f"\n� Show filtering results:")
        print(f"  🔍 A-G shows missing from CastInfo: {missing_count}")
        print(f"  📅 Shows with recent episodes (past 10 days): {recent_count}")
        print(f"  🎯 Qualifying shows (missing AND recent): {qualifying_count}")
        print(f"  🗑️  Shows to remove (SKIP): {len(shows_to_remove)}")
        
        return shows, shows_to_remove
    
    def get_existing_show_ids(self) -> Set[str]:
        """Get set of show IMDb IDs that already exist in CastInfo"""
        try:
            ws = self.ss.worksheet("CastInfo")
            all_data = ws.get_all_values()
        except Exception as e:
            print(f"❌ Error reading CastInfo: {e}")
            return set()
        
        existing_show_ids = set()
        
        for row in all_data[1:]:  # Skip header
            if len(row) > 4:
                show_imdb_id = row[4].strip()  # E: Show IMDbID
                if show_imdb_id:
                    existing_show_ids.add(show_imdb_id)
        
        return existing_show_ids
    
    def remove_shows_from_castinfo(self, shows_to_remove: List[str], dry_run: bool) -> int:
        """Remove all rows for shows marked as SKIP."""
        if not shows_to_remove:
            return 0
            
        try:
            ws = self.ss.worksheet("CastInfo")
            all_data = ws.get_all_values()
        except Exception as e:
            print(f"❌ Error reading CastInfo: {e}")
            return 0
        
        rows_to_delete = []
        
        # Find rows to delete (work backwards to maintain row numbers)
        for i in range(len(all_data) - 1, 0, -1):  # Skip header, work backwards
            row = all_data[i]
            if len(row) > 4:
                show_imdb_id = row[4].strip()  # E: Show IMDbID
                if show_imdb_id in shows_to_remove:
                    rows_to_delete.append(i + 1)  # +1 for 1-based indexing
        
        print(f"\n🗑️  Found {len(rows_to_delete)} rows to remove for SKIP shows")
        
        if rows_to_delete and not dry_run:
            # Delete rows one by one (in reverse order to maintain indices)
            for row_idx in sorted(rows_to_delete, reverse=True):
                try:
                    ws.delete_rows(row_idx)
                    time.sleep(0.5)  # Rate limiting
                except Exception as e:
                    print(f"    ❌ Failed to delete row {row_idx}: {e}")
        
        if dry_run and rows_to_delete:
            print(f"  🔍 DRY RUN - would delete {len(rows_to_delete)} rows")
        
        return len(rows_to_delete)
    
    def get_all_existing_cast_imdb_ids(self) -> Set[str]:
        """Get all IMDb IDs that exist in CastInfo."""
        try:
            ws = self.ss.worksheet("CastInfo")
            all_values = ws.get_all_values()
        except Exception as e:
            print(f"❌ Error reading CastInfo: {e}")
            return set()
            
        imdb_ids = set()
        for row in all_values[1:]:  # Skip header
            if len(row) > 2:  # Column C - Cast IMDbID
                imdb_id = row[2].strip()
                if imdb_id and imdb_id.startswith("nm"):
                    imdb_ids.add(imdb_id)
        
        return imdb_ids
    
    def get_existing_cast_for_show(self, show_imdb_id: str) -> Dict[str, dict]:
        """Get existing cast entries for a specific show."""
        try:
            ws = self.ss.worksheet("CastInfo")
            all_values = ws.get_all_values()
        except Exception as e:
            print(f"❌ Error reading CastInfo: {e}")
            return {}
        
        existing_cast = {}
        for i, row in enumerate(all_values[1:], start=2):  # Skip header
            if len(row) < 8:
                continue
                
            cast_imdb_id = row[2].strip()  # Column C
            show_imdb_id_row = row[4].strip()  # Column E
            current_episodes = row[6].strip()  # Column G
            cast_name = row[0].strip()  # Column A
            
            if show_imdb_id_row == show_imdb_id and cast_imdb_id:
                existing_cast[cast_imdb_id] = {
                    "row_idx": i,
                    "cast_name": cast_name,
                    "current_episodes": current_episodes
                }
        
        return existing_cast
    
    def should_add_cast_member(self, name: str, imdb_id: str, episodes: int, show_min_episodes: int, existing_cast_ids: Set[str]) -> Tuple[bool, str]:
        """Determine if a cast member should be added based on show's minimum threshold."""
        
        if episodes == 0:
            return False, "No episode data"
        
        is_already_in_castinfo = imdb_id in existing_cast_ids
        
        if is_already_in_castinfo:
            # Person exists for other shows - can add with 1+ episodes
            if episodes >= 1:
                return True, f"Existing person, {episodes} episodes"
            else:
                return False, f"Existing person but only {episodes} episodes"
        else:
            # New person - use show's minimum threshold
            if episodes >= show_min_episodes:
                return True, f"New person, {episodes} episodes (threshold: {show_min_episodes}+)"
            else:
                return False, f"New person with only {episodes} episodes (need {show_min_episodes}+)"
    
    def update_existing_cast(self, ws: gspread.Worksheet, existing_cast: Dict[str, dict], api_cast_data: Dict[str, dict], dry_run: bool) -> int:
        """Update episode counts for existing cast members."""
        updates_made = 0
        
        for imdb_id, cast_info in existing_cast.items():
            cast_name = cast_info["cast_name"]
            row_idx = cast_info["row_idx"]
            current_episodes = cast_info["current_episodes"]
            
            # Get API episode count
            api_episodes = 0
            if imdb_id in api_cast_data:
                api_episodes = api_cast_data[imdb_id]["episodes"]
                
            if api_episodes > 0:
                if current_episodes != str(api_episodes):
                    print(f"    🔄 {cast_name}: {current_episodes} → {api_episodes} episodes")
                    
                    if not dry_run:
                        try:
                            ws.update_cell(row_idx, 7, str(api_episodes))  # Column G
                            updates_made += 1
                            time.sleep(0.5)
                        except Exception as e:
                            print(f"      ❌ Update failed: {e}")
                    else:
                        print(f"      🔍 DRY RUN - would update")
                else:
                    print(f"    ✅ {cast_name}: {api_episodes} episodes (already correct)")
            else:
                if current_episodes:
                    print(f"    ⚠️  {cast_name}: API shows 0 episodes (currently {current_episodes})")
                else:
                    print(f"    ❌ {cast_name}: No episode data in API")
        
        return updates_made
    
    def add_missing_cast_members(self, ws: gspread.Worksheet, show_name: str, show_imdb_id: str, api_cast_data: Dict[str, dict], existing_cast: Dict[str, dict], existing_cast_ids: Set[str], show_min_episodes: int, dry_run: bool) -> int:
        """Add missing cast members based on show's minimum episode threshold."""
        
        new_rows = []
        
        for imdb_id, cast_info in api_cast_data.items():
            # Skip if already exists for this show
            if imdb_id in existing_cast:
                continue
                
            name = cast_info.get('name', '')
            episodes = cast_info.get('episodes', 0)
            
            if not name or not episodes:
                continue
                
            should_add, reason = self.should_add_cast_member(name, imdb_id, episodes, show_min_episodes, existing_cast_ids)
            
            if should_add:
                # Build new row: CastName, CastID, Cast IMDbID, ShowName, Show IMDbID, ShowID, TotalEpisodes, TotalSeasons
                new_row = [
                    name,           # A: CastName
                    "",             # B: CastID (TMDb person ID - we don't have this from API)
                    imdb_id,        # C: Cast IMDbID
                    show_name,      # D: ShowName
                    show_imdb_id,   # E: Show IMDbID
                    "",             # F: ShowID (TMDb show ID - we don't have this)
                    str(episodes),  # G: TotalEpisodes
                    ""              # H: TotalSeasons
                ]
                new_rows.append(new_row)
                print(f"      ➕ NEW: {name} ({imdb_id}) - {episodes} episodes ({reason})")
                
                if dry_run:
                    print(f"        🔍 DRY RUN - would add")
        
        # Add ALL new rows for this show in a SINGLE batch operation to avoid API issues
        if new_rows and not dry_run:
            try:
                print(f"  📦 Adding {len(new_rows)} cast members in single batch operation...")
                ws.append_rows(new_rows)
                print(f"  ✅ Successfully batched {len(new_rows)} new cast members")
            except Exception as e:
                print(f"  ❌ Error in batch adding cast members: {e}")
                return 0
        elif new_rows and dry_run:
            print(f"  📦 Would batch add {len(new_rows)} cast members in single operation")
        
        return len(new_rows)
    
    def process_show(self, show_info: Dict[str, str], existing_cast_ids: Set[str], dry_run: bool) -> Tuple[int, int]:
        """Process a single show - update existing and add missing cast."""
        show_name = show_info["name"]
        show_imdb_id = show_info["imdb_id"]
        show_min_episodes = show_info["min_episodes"]
        
        print(f"\n🎭 Processing: {show_name} ({show_imdb_id}) - Min episodes: {show_min_episodes}")
        
        # Get existing cast for this show
        existing_cast = self.get_existing_cast_for_show(show_imdb_id)
        print(f"  👥 Found {len(existing_cast)} existing cast entries")
        
        # Get API cast data
        api_cast_data = self.imdb_api.get_all_cast_credits(show_imdb_id)
        if not api_cast_data:
            print(f"  ❌ No API data returned")
            return 0, 0
            
        print(f"  📊 API returned {len(api_cast_data)} cast members")
        
        # Update existing entries
        ws = self.ss.worksheet("CastInfo")
        updates_made = self.update_existing_cast(ws, existing_cast, api_cast_data, dry_run)
        
        # Add missing cast members with show-specific threshold
        additions_made = self.add_missing_cast_members(ws, show_name, show_imdb_id, api_cast_data, existing_cast, existing_cast_ids, show_min_episodes, dry_run)
        
        # Update global existing_cast_ids set with new additions
        if not dry_run and additions_made > 0:
            for imdb_id, cast_info in api_cast_data.items():
                if imdb_id not in existing_cast:  # New addition
                    existing_cast_ids.add(imdb_id)
        
        action_text = "would update" if dry_run else "updated"
        print(f"  🔍 {action_text.title()} {updates_made} episode counts, {'would add' if dry_run else 'added'} {additions_made} new cast")
        
        return updates_made, additions_made
    
    def run_update(self, dry_run: bool = False):
        """Main update process for shows A-G."""
        print("🚀 Starting enhanced CastInfo update for shows A-G...")
        print("   📝 Will update existing episode counts AND add missing cast members")
        print("   🎯 Processing ALL shows missing from CastInfo (A-G)")
        print("   🗑️  Will remove cast for shows marked SKIP")
        
        shows, shows_to_remove = self.get_shows_a_g()
        
        # First, remove shows marked as SKIP
        if shows_to_remove:
            print(f"\n🗑️  Removing cast for {len(shows_to_remove)} SKIP shows...")
            removed_rows = self.remove_shows_from_castinfo(shows_to_remove, dry_run)
            print(f"  {'Would remove' if dry_run else 'Removed'} {removed_rows} cast rows for SKIP shows")
        
        if not shows:
            print("❌ No qualifying shows found to process")
            return
        
        # Get all existing IMDb IDs for cross-referencing
        print("📊 Loading existing cast IMDb IDs...")
        existing_cast_ids = self.get_all_existing_cast_imdb_ids()
        print(f"  Found {len(existing_cast_ids)} unique cast members across all shows")
        
        total_updates = 0
        total_additions = 0
        
        for show_info in shows:
            updates, additions = self.process_show(
                show_info,
                existing_cast_ids,
                dry_run
            )
            total_updates += updates
            total_additions += additions
        
        print(f"\n🎉 Processing complete!")
        print(f"  📊 Total shows processed: {len(shows)}")
        print(f"  🔄 Total episode updates: {total_updates}")
        print(f"  ➕ Total new cast added: {total_additions}")
        if shows_to_remove:
            print(f"  🗑️  Shows removed/cleaned: {len(shows_to_remove)}")
        if dry_run:
            print("  🔍 This was a DRY RUN - no actual changes made")

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Enhanced CastInfo updater: update episodes AND add missing cast for shows A-G")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be updated without making changes")
    args = parser.parse_args()
    
    updater = EnhancedCastInfoUpdater()
    updater.run_update(dry_run=args.dry_run)

if __name__ == "__main__":
    main()
