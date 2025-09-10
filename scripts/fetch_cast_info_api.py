"""
Fetch cast information using the imdbapi.dev API instead of web scraping.
This provides accurate episode counts and structured data.
"""

import requests
import time
from typing import Dict, List, Tuple

class IMDbAPIClient:
    def __init__(self):
        self.base_url = "https://api.imdbapi.dev"
        self.session = requests.Session()
        
    def get_cast_credits(self, title_id: str, category: str = "self") -> Dict[str, dict]:
        """
        Get cast credits for a title using the API.
        
        Args:
            title_id: IMDb title ID (e.g., 'tt15557874')
            category: Credit category ('self' for reality TV contestants)
            
        Returns:
            Dict mapping IMDb name IDs to cast info
        """
        cast_data = {}
        next_token = None
        page_count = 0
        
        while True:
            page_count += 1
            print(f"📄 Fetching page {page_count}...")
            
            # Build URL
            url = f"{self.base_url}/titles/{title_id}/credits"
            params = {"categories": category}
            if next_token:
                params["pageToken"] = next_token
                
            try:
                response = self.session.get(url, params=params)
                response.raise_for_status()
                data = response.json()
                
                # Process credits
                credits = data.get("credits", [])
                print(f"   Found {len(credits)} credits on this page")
                
                for credit in credits:
                    name_data = credit.get("name", {})
                    name_id = name_data.get("id", "")
                    
                    if name_id and name_id.startswith("nm"):
                        cast_data[name_id] = {
                            "name": name_data.get("displayName", ""),
                            "episodes": credit.get("episodeCount", 0),
                            "characters": credit.get("characters", []),
                            "category": credit.get("category", ""),
                            "alternative_names": name_data.get("alternativeNames", [])
                        }
                
                # Check for next page
                next_token = data.get("nextPageToken")
                if not next_token:
                    break
                    
                # Rate limiting
                time.sleep(0.5)
                
            except Exception as e:
                print(f"❌ Error fetching page {page_count}: {e}")
                break
        
        print(f"✅ Total cast members found: {len(cast_data)}")
        return cast_data
    
    def search_cast_member(self, cast_data: Dict[str, dict], search_name: str) -> List[Tuple[str, dict]]:
        """Search for a cast member by name."""
        results = []
        search_lower = search_name.lower()
        
        for name_id, info in cast_data.items():
            # Check display name
            if search_lower in info["name"].lower():
                results.append((name_id, info))
                continue
                
            # Check alternative names
            for alt_name in info.get("alternative_names", []):
                if search_lower in alt_name.lower():
                    results.append((name_id, info))
                    break
        
        return results

def test_api():
    """Test the API with The Traitors US."""
    client = IMDbAPIClient()
    
    print("🔍 Testing API with The Traitors US...")
    cast_data = client.get_cast_credits("tt15557874", "self")
    
    # Look for Parvati
    parvati_results = client.search_cast_member(cast_data, "parvati")
    if parvati_results:
        for name_id, info in parvati_results:
            print(f"🎯 FOUND PARVATI: {info['name']} ({name_id}) - {info['episodes']} episodes")
    else:
        print("❌ Parvati not found")
    
    # Show top cast members by episode count
    print(f"\n📺 Top 10 cast members by episode count:")
    sorted_cast = sorted(cast_data.items(), key=lambda x: x[1]["episodes"], reverse=True)
    for i, (name_id, info) in enumerate(sorted_cast[:10]):
        print(f"  {i+1:2d}. {info['name']} ({name_id}) - {info['episodes']} episodes")
    
    return cast_data

if __name__ == "__main__":
    test_api()
