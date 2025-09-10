#!/usr/bin/env python3
"""
Update CastInfo columns A-G for shows A-G using IMDbAPI.dev
Uses ShowInfo Column B (ShowName) and Column F (IMDbSeriesID)
Skips only if Column J = "SKIP"
"""

import gspread
import requests
import time
from typing import Dict, List, Set

class IMDbAPIClient:
    """Client for IMDbAPI.dev to get accurate episode counts."""
    
    def __init__(self):
        self.base_url = "https://api.imdbapi.dev"
        self.session = requests.Session()
        self._cache = {}
        
    def get_all_cast_credits(self, title_id: str) -> Dict[str, dict]:
        """Get all cast credits for a title."""
        if title_id in self._cache:
            return self._cache[title_id]
            
        cast_data = {}
        next_token = None
        
        try:
            while True:
                url = f"{self.base_url}/titles/{title_id}/credits"
                params = {"categories": "self"}  # Reality TV contestants
                if next_token:
                    params["pageToken"] = next_token
                    
                response = self.session.get(url, params=params)
                response.raise_for_status()
                data = response.json()
                
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
                
                next_token = data.get("nextPageToken")
                if not next_token:
                    break
                    
                time.sleep(0.5)  # Rate limiting
                
        except Exception as e:
            print(f"    ⚠️  Failed to fetch cast for {title_id}: {e}")
        
        self._cache[title_id] = cast_data
        return cast_data

class CastInfoUpdater:
    """Updates CastInfo sheet with API episode data."""
    
    def __init__(self):
        # Connect to Google Sheets
        SERVICE_KEY_PATH = '/Users/thomashulihan/Projects/TRR-Backend/keys/trr-backend-df2c438612e1.json'
        gc = gspread.service_account(filename=SERVICE_KEY_PATH)
        self.ss = gc.open('Realitease2025Data')
        
        self.imdb_api = IMDbAPIClient()
        
    def get_shows_a_g(self) -> List[Dict[str, str]]:
        """Get shows A-G from ShowInfo with proper filtering."""
        try:
            ws = self.ss.worksheet("ShowInfo")
            rows = ws.get_all_values()
        except Exception as e:
            print(f"❌ Error reading ShowInfo: {e}")
            return []

        if not rows:
            return []

        shows = []
        headers = rows[0]
        print(f"📋 ShowInfo headers: {headers}")
        
        for row_idx, row in enumerate(rows[1:], start=2):
            if len(row) < 10:  # Need at least 10 columns
                continue
                
            show_name = row[1].strip()  # Column B - ShowName
            imdb_id = row[5].strip()    # Column F - IMDbSeriesID  
            skip_flag = row[9].strip()  # Column J - OVERRIDE
            
            # Skip if marked as SKIP
            if skip_flag == "SKIP":
                print(f"  ⏭️  Skipping {show_name} (marked SKIP)")
                continue
                
            # Filter for shows A-G
            if not show_name or not show_name[0].isalpha():
                continue
                
            first_letter = show_name[0].upper()
            if first_letter < 'A' or first_letter > 'G':
                continue
                
            # Need IMDb ID
            if not imdb_id or not imdb_id.startswith('tt'):
                print(f"  ❌ No IMDb ID for {show_name}")
                continue
                
            shows.append({
                "name": show_name,
                "imdb_id": imdb_id,
                "row": row_idx
            })
            print(f"  ✅ {show_name} ({imdb_id})")
        
        print(f"📺 Found {len(shows)} shows A-G to process")
        return shows
    
    def get_castinfo_for_show(self, show_imdb_id: str) -> List[Dict]:
        """Get existing CastInfo entries for a show."""
        try:
            ws = self.ss.worksheet("CastInfo")
            all_values = ws.get_all_values()
        except Exception as e:
            print(f"❌ Error reading CastInfo: {e}")
            return []
        
        cast_entries = []
        
        for row_idx, row in enumerate(all_values[1:], start=2):  # Skip header
            if len(row) < 5:
                continue
                
            show_imdb_col = row[4].strip()  # Column E - Show IMDbID
            cast_name = row[0].strip()      # Column A - CastName
            cast_imdb_id = row[2].strip()   # Column C - Cast IMDbID
            current_episodes = row[6].strip() if len(row) > 6 else ""  # Column G - TotalEpisodes
            
            if show_imdb_col == show_imdb_id:
                cast_entries.append({
                    "row_idx": row_idx,
                    "cast_name": cast_name,
                    "cast_imdb_id": cast_imdb_id,
                    "current_episodes": current_episodes
                })
        
        return cast_entries
    
    def update_cast_episodes(self, show_name: str, show_imdb_id: str, dry_run: bool = False):
        """Update episode counts for a show's cast."""
        print(f"\n🎭 Processing: {show_name} ({show_imdb_id})")
        
        # Get existing cast entries
        cast_entries = self.get_castinfo_for_show(show_imdb_id)
        if not cast_entries:
            print(f"  ❌ No existing cast entries found in CastInfo")
            return
            
        print(f"  👥 Found {len(cast_entries)} existing cast entries")
        
        # Get API episode data
        try:
            api_cast_data = self.imdb_api.get_all_cast_credits(show_imdb_id)
            print(f"  📊 API returned {len(api_cast_data)} cast members")
        except Exception as e:
            print(f"  ❌ API error: {e}")
            return
        
        # Update episodes
        ws = self.ss.worksheet("CastInfo")
        updates_made = 0
        
        for entry in cast_entries:
            cast_imdb_id = entry["cast_imdb_id"]
            cast_name = entry["cast_name"]
            row_idx = entry["row_idx"]
            current_episodes = entry["current_episodes"]
            
            if not cast_imdb_id:
                print(f"    ❌ No IMDb ID for {cast_name}")
                continue
                
            # Get episode count from API
            api_episodes = 0
            if cast_imdb_id in api_cast_data:
                api_episodes = api_cast_data[cast_imdb_id]["episodes"]
                
            if api_episodes > 0:
                if current_episodes != str(api_episodes):
                    print(f"    🔄 {cast_name}: {current_episodes} → {api_episodes} episodes")
                    
                    if not dry_run:
                        try:
                            ws.update_cell(row_idx, 7, str(api_episodes))  # Column G
                            updates_made += 1
                            time.sleep(0.5)  # Rate limiting
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
        
        if not dry_run:
            print(f"  ✅ Updated {updates_made} episode counts")
        else:
            print(f"  🔍 DRY RUN - would update {updates_made} episode counts")
    
    def run_update(self, dry_run: bool = False):
        """Main update process."""
        print("🚀 Starting CastInfo update for shows A-G...")
        
        shows = self.get_shows_a_g()
        if not shows:
            print("❌ No shows found to process")
            return
        
        for show in shows:
            self.update_cast_episodes(
                show["name"], 
                show["imdb_id"],
                dry_run=dry_run
            )
        
        print(f"\n✅ Completed processing {len(shows)} shows")

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Update CastInfo columns A-G for shows A-G")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be updated without making changes")
    args = parser.parse_args()
    
    updater = CastInfoUpdater()
    updater.run_update(dry_run=args.dry_run)

if __name__ == "__main__":
    main()
