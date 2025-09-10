#!/usr/bin/env python3
"""
Enhanced CastInfo Updater for Shows with Recent Episodes
========================================================

Updates CastInfo with accurate episode counts AND adds missing cast members
using smart filtering rules for ALL shows that have aired in the past 10 days:

1. New people (not in CastInfo): Need 3+ episodes 
2. Existing people (already in CastInfo): Can add with 1+ episodes
3. Special shows (Celebrity Family Feud, RuPaul, Wife Swap): 1+ episode OK for new people
4. No episode data: Don't add
"""

import gspread
import requests
import time
from typing import Dict, List, Set, Tuple

class ImdbAPI:
    """IMDb API client for fetching cast information."""
    
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
                time.sleep(0.5)  # Rate limit IMDb API requests
                response = self.session.get(url, params=params, timeout=30)
                
                if response.status_code != 200:
                    print(f"    ⚠️  API returned status {response.status_code}")
                    break
                    
                data = response.json()
                credits = data.get("credits", [])
                print(f"    📄 Got {len(credits)} credits on page {page_count + 1}")
                
                for credit in credits:
                    # Extract basic info
                    name_info = credit.get("name", {})
                    if not name_info or "id" not in name_info:
                        continue
                    
                    credit_id = name_info["id"]
                    name = name_info.get("displayName", "")
                    episodes = credit.get("episodeCount", 0)  # Direct field
                    character = credit.get("characters", [""])[0] if credit.get("characters") else ""
                    
                    cast_data[credit_id] = {
                        "name": name,
                        "episodes": episodes,
                        "character": character
                    }
                
                # Check for next page
                next_token = data.get("nextToken")
                page_count += 1
                
                if not next_token:
                    break
                    
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
        # Initialize Google Sheets client
        gc = gspread.service_account(filename="keys/trr-backend-df2c438612e1.json")
        self.ss = gc.open("Realitease2025Data")
        self.imdb_api = ImdbAPI()
        self.batch_buffer = []
        self.batch_size = 25  # Smaller batches for better rate limiting
        
        # Cache all CastInfo data once to avoid multiple reads
        print("📊 Loading existing cast data...")
        self.all_cast_data = self._load_all_cast_data()
        print(f"  Found {len([c for show_cast in self.all_cast_data.values() for c in show_cast])} total cast entries")
        
    def _load_all_cast_data(self) -> Dict[str, Dict[str, dict]]:
        """Load all CastInfo data once and organize by show."""
        try:
            ws = self.ss.worksheet("CastInfo")
            all_values = ws.get_all_values()
        except Exception as e:
            print(f"❌ Error reading CastInfo: {e}")
            return {}
        
        # Organize by show IMDb ID
        cast_by_show = {}
        for i, row in enumerate(all_values[1:], start=2):  # Skip header, start from row 2
            if len(row) >= 7:  # Need at least 7 columns
                cast_imdb_id = row[2].strip()  # Column C
                show_id = row[4].strip()       # Column E
                episodes = row[6].strip()      # Column G
                
                if show_id and cast_imdb_id:
                    if show_id not in cast_by_show:
                        cast_by_show[show_id] = {}
                    
                    cast_by_show[show_id][cast_imdb_id] = {
                        "row": i,
                        "episodes": int(episodes) if episodes.isdigit() else 0,
                        "name": row[0].strip() if len(row) > 0 else ""
                    }
        
        return cast_by_show
        self.total_batched = 0
    
    def flush_batch_buffer(self, ws: gspread.Worksheet, force: bool = False):
        """Flush the batch buffer to Google Sheets."""
        if not self.batch_buffer:
            return
            
        if not force and len(self.batch_buffer) < self.batch_size:
            return
            
        try:
            ws.append_rows(self.batch_buffer)
            print(f"    📦 Batch uploaded {len(self.batch_buffer)} rows")
            self.total_batched += len(self.batch_buffer)
            self.batch_buffer.clear()
            time.sleep(1)  # Rate limiting
        except Exception as e:
            print(f"    ❌ Batch upload failed: {e}")
            self.batch_buffer.clear()
    
    def get_shows_with_recent_episodes(self) -> Tuple[List[Dict[str, str]], List[str]]:
        """Get ALL shows with data in columns A-G that have aired in past 10 days."""
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
        total_shows_count = 0
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
                
            total_shows_count += 1
            
            # Handle SKIP shows - mark for removal
            if threshold_flag == "SKIP":
                if imdb_id in existing_show_ids:
                    shows_to_remove.append(imdb_id)
                continue
            
            # Check if recent episode is within 10 days
            is_recent = False
            episode_age_info = ""
            if recent_episode:
                try:
                    episode_date = datetime.strptime(recent_episode, "%Y-%m-%d")
                    days_ago = (current_date - episode_date).days
                    is_recent = days_ago <= 10
                    episode_age_info = f"({days_ago} days ago)"
                    if is_recent:
                        recent_count += 1
                except:
                    episode_age_info = "(invalid date format)"
            else:
                episode_age_info = "(no recent episode date)"
            
            # Only process shows with recent episodes (past 10 days)
            if not is_recent:
                print(f"  ⏭️  Skip: {show_name} - Not recent: {recent_episode} {episode_age_info}")
                continue
            
            # Parse episode threshold
            min_episodes = 3  # default (blank stays at 3)
            if threshold_flag == "Y":
                min_episodes = 2  # changed from 1 to 2
            else:
                try:
                    min_episodes = int(threshold_flag)
                except:
                    min_episodes = 3  # default (blank stays at 3)
            
            shows.append({
                "name": show_name,
                "imdb_id": imdb_id,
                "min_episodes": min_episodes,
                "recent_episode": recent_episode
            })
            qualifying_count += 1
            
            # Show what we're processing
            already_in_castinfo = imdb_id in existing_show_ids
            status = "UPDATE cast" if already_in_castinfo else "ADD missing cast"
            print(f"  ✅ {show_name} - Recent: {recent_episode} {episode_age_info}, Min episodes: {min_episodes} ({status})")
        
        print(f"\n📊 Show filtering results:")
        print(f"  🔍 Total shows with A-G data: {total_shows_count}")
        print(f"  📅 Shows with recent episodes (past 10 days): {recent_count}")
        print(f"  🎯 Qualifying shows for processing: {qualifying_count}")
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
            if len(row) >= 5:  # Show IMDb ID is in column E (index 4)
                show_imdb_id = row[4].strip()
                if show_imdb_id:
                    existing_show_ids.add(show_imdb_id)
        
        return existing_show_ids
    
    def remove_shows_from_castinfo(self, shows_to_remove: List[str], dry_run: bool) -> int:
        """Remove all rows for shows marked as SKIP using efficient batch operations."""
        if not shows_to_remove:
            return 0
            
        try:
            ws = self.ss.worksheet("CastInfo")
            all_data = ws.get_all_values()
        except Exception as e:
            print(f"❌ Error reading CastInfo for removal: {e}")
            return 0
        
        # Find rows to keep (more efficient than deleting)
        rows_to_keep = []
        rows_removed_count = 0
        
        # Keep header row
        if all_data:
            rows_to_keep.append(all_data[0])
        
        # Filter out SKIP show rows
        for i in range(1, len(all_data)):  # Skip header
            row = all_data[i]
            if len(row) >= 5:  # Show IMDb ID is in column E (index 4)
                show_imdb_id = row[4].strip()
                if show_imdb_id in shows_to_remove:
                    rows_removed_count += 1
                    continue  # Skip this row (don't add to rows_to_keep)
            rows_to_keep.append(row)
        
        print(f"\n🗑️  Found {rows_removed_count} rows to remove for SKIP shows")
        
        if rows_removed_count > 0 and not dry_run:
            try:
                # Clear the entire sheet and write back filtered data in one operation
                print(f"  📝 Clearing sheet and rewriting {len(rows_to_keep)} rows...")
                ws.clear()
                time.sleep(2)  # Give API time to process
                
                if rows_to_keep:
                    # Write data in batches to avoid hitting quota  
                    batch_size = 2000  # Larger batch for better efficiency
                    for i in range(0, len(rows_to_keep), batch_size):
                        batch = rows_to_keep[i:i+batch_size]
                        if i == 0:
                            # First batch - start from A1
                            ws.update(f"A1:H{len(batch)}", batch)
                        else:
                            # Subsequent batches - append
                            start_row = i + 1
                            end_row = start_row + len(batch) - 1
                            ws.update(f"A{start_row}:H{end_row}", batch)
                        time.sleep(3)  # More conservative rate limiting
                        print(f"    📦 Wrote batch {i//batch_size + 1}: rows {i+1}-{min(i+batch_size, len(rows_to_keep))}")
                
                print(f"  ✅ Successfully removed {rows_removed_count} SKIP show rows")
            except Exception as e:
                print(f"❌ Error during batch removal: {e}")
                return 0
        
        if dry_run and rows_removed_count > 0:
            print(f"  🔍 DRY RUN: Would remove {rows_removed_count} rows using efficient batch operation")
        
        return rows_removed_count
    
    def get_all_existing_cast_imdb_ids(self) -> Set[str]:
        """Get all IMDb IDs that exist in CastInfo from cached data."""
        imdb_ids = set()
        for show_cast in self.all_cast_data.values():
            imdb_ids.update(show_cast.keys())
        return imdb_ids
    
    def get_existing_cast_for_show(self, show_imdb_id: str) -> Dict[str, dict]:
        """Get existing cast entries for a specific show from cached data."""
        return self.all_cast_data.get(show_imdb_id, {})
    
    def should_add_cast_member(self, name: str, imdb_id: str, episodes: int, show_min_episodes: int, existing_cast_ids: Set[str]) -> Tuple[bool, str]:
        """Determine if a cast member should be added based on show's minimum threshold."""
        
        if episodes == 0:
            return False, f"Skipped: {name} - 0 episodes"
        
        is_already_in_castinfo = imdb_id in existing_cast_ids
        
        if is_already_in_castinfo:
            # Existing person: can add with 1+ episodes
            if episodes >= 1:
                return True, f"Existing person, {episodes} episodes"
            else:
                return False, f"Existing person, but only {episodes} episodes"
        else:
            # New person: needs show_min_episodes threshold
            if episodes >= show_min_episodes:
                return True, f"New person, {episodes} episodes (threshold: {show_min_episodes}+)"
            else:
                return False, f"New person, only {episodes} episodes (threshold: {show_min_episodes}+)"
    
    def update_existing_cast(self, ws: gspread.Worksheet, existing_cast: Dict[str, dict], api_cast_data: Dict[str, dict], dry_run: bool) -> int:
        """Update episode counts for existing cast members."""
        updates_made = 0
        
        for imdb_id, cast_info in existing_cast.items():
            if imdb_id in api_cast_data:
                api_episodes = api_cast_data[imdb_id]["episodes"]
                current_episodes = cast_info["episodes"]
                
                if api_episodes != current_episodes:
                    row_num = cast_info["row"]
                    if not dry_run:
                        try:
                            # Update single cell with proper format
                            ws.update(f"G{row_num}:G{row_num}", [[str(api_episodes)]])
                            time.sleep(0.3)  # Rate limiting
                        except Exception as e:
                            print(f"      ❌ Failed to update row {row_num}: {e}")
                            continue
                    
                    print(f"      🔄 UPDATED: {cast_info['name']} episodes: {current_episodes} → {api_episodes}")
                    updates_made += 1
        
        return updates_made
    
    def add_missing_cast_members(self, ws: gspread.Worksheet, show_name: str, show_imdb_id: str, api_cast_data: Dict[str, dict], existing_cast: Dict[str, dict], existing_cast_ids: Set[str], show_min_episodes: int, dry_run: bool) -> int:
        """Add missing cast members based on show's minimum episode threshold."""
        
        new_rows = []
        
        for imdb_id, cast_info in api_cast_data.items():
            # Skip if already exists for this show
            if imdb_id in existing_cast:
                continue
                
            name = cast_info["name"]
            episodes = cast_info["episodes"]
            
            should_add, reason = self.should_add_cast_member(name, imdb_id, episodes, show_min_episodes, existing_cast_ids)
            
            if should_add:
                new_row = [
                    name,           # A: CastName
                    "",            # B: TMDb CastID (empty)
                    imdb_id,       # C: Cast IMDbID
                    show_name,     # D: ShowName
                    show_imdb_id,  # E: Show IMDbID
                    "",            # F: TMDb ShowID (empty)
                    str(episodes), # G: TotalEpisodes
                    ""             # H: Seasons (empty)
                ]
                new_rows.append(new_row)
                print(f"      ➕ NEW: {name} ({imdb_id}) - {episodes} episodes ({reason})")
        
        # Add new rows using batch upload
        if new_rows and not dry_run:
            try:
                # Add to batch buffer
                self.batch_buffer.extend(new_rows)
                print(f"      📋 Added {len(new_rows)} rows to batch buffer (total: {len(self.batch_buffer)})")
                
                # Flush if buffer is full
                self.flush_batch_buffer(ws)
                
            except Exception as e:
                print(f"      ❌ Failed to queue {len(new_rows)} new cast members: {e}")
                return 0
        
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
            print(f"  ❌ No cast data from API")
            return 0, 0
            
        print(f"  📊 API returned {len(api_cast_data)} cast members")
        
        # Update existing entries
        ws = self.ss.worksheet("CastInfo")
        updates_made = self.update_existing_cast(ws, existing_cast, api_cast_data, dry_run)
        
        # Add missing cast members with show-specific threshold
        additions_made = self.add_missing_cast_members(ws, show_name, show_imdb_id, api_cast_data, existing_cast, existing_cast_ids, show_min_episodes, dry_run)
        
        # Update global existing_cast_ids set with new additions
        if not dry_run and additions_made > 0:
            for imdb_id in api_cast_data:
                if imdb_id not in existing_cast:
                    existing_cast_ids.add(imdb_id)
        
        action_text = "would update" if dry_run else "updated"
        print(f"  🔍 {action_text.title()} {updates_made} episode counts, {'would add' if dry_run else 'added'} {additions_made} new cast")
        
        return updates_made, additions_made
    
    def run_update(self, dry_run: bool = False):
        """Main update process for ALL shows with recent episodes."""
        print("🚀 Starting enhanced CastInfo update for ALL shows...")
        print("   📝 Will update existing episode counts AND add missing cast members")
        print("   🎯 Processing shows with data in columns A-G that aired in past 10 days")
        print("   🗑️  Will remove cast for shows marked SKIP")
        
        shows, shows_to_remove = self.get_shows_with_recent_episodes()
        
        # First, remove shows marked as SKIP
        if shows_to_remove:
            removed_count = self.remove_shows_from_castinfo(shows_to_remove, dry_run)
        
        if not shows:
            print("📭 No shows with recent episodes found for processing.")
            return
        
        # Get all existing IMDb IDs for cross-referencing
        print("📊 Loading existing cast IMDb IDs...")
        existing_cast_ids = self.get_all_existing_cast_imdb_ids()
        print(f"  Found {len(existing_cast_ids)} unique cast members across all shows")
        
        total_updates = 0
        total_additions = 0
        
        for show_info in shows:
            try:
                updates, additions = self.process_show(show_info, existing_cast_ids, dry_run)
                total_updates += updates
                total_additions += additions
            except Exception as e:
                print(f"❌ Error processing {show_info['name']}: {e}")
        
        print(f"\n🎉 Processing complete!")
        print(f"  📊 Total shows processed: {len(shows)}")
        print(f"  🔄 Total episode updates: {total_updates}")
        print(f"  ➕ Total new cast added: {total_additions}")
        if shows_to_remove:
            print(f"  🗑️  Shows removed/cleaned: {len(shows_to_remove)}")
        if dry_run:
            print("  🔍 This was a DRY RUN - no actual changes made")
        else:
            # Final flush of any remaining batch items
            if hasattr(self, 'batch_buffer') and self.batch_buffer:
                try:
                    castinfo_ws = self.ss.worksheet("CastInfo")
                    self.flush_batch_buffer(castinfo_ws, force=True)
                    print(f"  📦 Final batch flush: {self.total_batched} total rows uploaded")
                except Exception as e:
                    print(f"  ❌ Final batch flush failed: {e}")

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Enhanced CastInfo updater: update episodes AND add missing cast for shows with recent episodes")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be updated without making changes")
    args = parser.parse_args()
    
    updater = EnhancedCastInfoUpdater()
    updater.run_update(dry_run=args.dry_run)

if __name__ == "__main__":
    main()
