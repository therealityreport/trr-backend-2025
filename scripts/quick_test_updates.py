#!/usr/bin/env python3
"""
Quick test script to show 5 updates and 5 new additions for shows A-G
"""

import gspread
import requests
import time

class QuickTester:
    def __init__(self):
        # Connect to Google Sheets
        gc = gspread.service_account(filename="/Users/thomashulihan/Projects/TRR-Backend/keys/trr-backend-df2c438612e1.json")
        self.ss = gc.open("Realitease2025Data")
        
    def get_api_cast_data(self, imdb_id: str):
        """Get cast data from API"""
        cast_data = {}
        try:
            url = f"https://api.imdbapi.dev/titles/{imdb_id}/credits"
            params = {"categories": "self"}
            
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            for credit in data.get("credits", [])[:20]:  # Limit to first 20
                name_data = credit.get("name", {})
                name_id = name_data.get("id", "")
                
                if name_id and name_id.startswith("nm"):
                    cast_data[name_id] = {
                        "name": name_data.get("displayName", ""),
                        "episodes": credit.get("episodeCount", 0),
                    }
        except Exception as e:
            print(f"    ⚠️  API error: {e}")
        
        return cast_data
    
    def test_one_show(self, show_name: str, imdb_id: str):
        """Test one show and show sample updates/additions"""
        print(f"\n🎭 Testing: {show_name} ({imdb_id})")
        
        # Get existing cast from CastInfo
        try:
            ws = self.ss.worksheet("CastInfo")
            all_data = ws.get_all_values()
            headers = all_data[0]
            
            existing_cast = []
            for row in all_data[1:]:
                if len(row) >= 7 and row[4] == imdb_id:  # Show IMDbID matches
                    existing_cast.append({
                        "CastName": row[0],
                        "Cast IMDbID": row[2], 
                        "TotalEpisodes": row[6]
                    })
        except Exception as e:
            print(f"  ❌ Error reading CastInfo: {e}")
            return
            
        print(f"  👥 Found {len(existing_cast)} existing cast entries")
        
        # Get API data
        api_cast = self.get_api_cast_data(imdb_id)
        print(f"  📊 API returned {len(api_cast)} cast members")
        
        if not api_cast:
            return
        
        # Sample updates
        updates_shown = 0
        new_shown = 0
        
        existing_imdb_ids = {cast["Cast IMDbID"] for cast in existing_cast}
        
        for api_imdb_id, api_info in api_cast.items():
            if updates_shown >= 5 and new_shown >= 5:
                break
                
            name = api_info["name"]
            episodes = api_info["episodes"]
            
            if api_imdb_id in existing_imdb_ids:
                # This is an update
                if updates_shown < 5:
                    # Find current episode count
                    current_episodes = ""
                    for cast in existing_cast:
                        if cast["Cast IMDbID"] == api_imdb_id:
                            current_episodes = cast["TotalEpisodes"]
                            break
                    
                    if str(episodes) != str(current_episodes):
                        print(f"    🔄 UPDATE: {name}: {current_episodes} → {episodes} episodes")
                        updates_shown += 1
                    elif updates_shown < 3:  # Show a few that are correct
                        print(f"    ✅ CORRECT: {name}: {episodes} episodes")
                        updates_shown += 1
            else:
                # This is potentially new
                if new_shown < 5 and episodes >= 3:  # Simple 3+ episode rule for demo
                    print(f"    ➕ NEW: {name} ({api_imdb_id}) - {episodes} episodes")
                    new_shown += 1
                elif new_shown < 5 and episodes < 3:
                    print(f"    ❌ SKIP: {name} - only {episodes} episodes (need 3+)")
                    new_shown += 1

def main():
    tester = QuickTester()
    
    # Test a few shows A-G
    test_shows = [
        ("American Idol", "tt0319931"),
        ("The Traitors US", "tt15557874"),  # This should show Parvati as NEW
        ("America's Got Talent", "tt0759364")
    ]
    
    for show_name, imdb_id in test_shows:
        tester.test_one_show(show_name, imdb_id)
        
if __name__ == "__main__":
    main()
