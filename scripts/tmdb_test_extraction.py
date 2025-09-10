#!/usr/bin/env python3

import os
import sys
import requests
from dotenv import load_dotenv

# Load environment variables
load_env_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(load_env_path)

def test_tmdb_extraction():
    """Test TMDB extraction with known examples"""
    
    tmdb_bearer = os.getenv('TMDB_BEARER')
    if not tmdb_bearer:
        print("❌ No TMDB_BEARER found")
        return
    
    base_url = "https://api.themoviedb.org/3"
    session = requests.Session()
    session.headers.update({
        'Authorization': f'Bearer {tmdb_bearer}',
        'Content-Type': 'application/json'
    })
    
    # Test cases from your examples
    test_cases = [
        {
            'name': 'Kathy Hilton',
            'person_id': '525728e4760ee3776a2a253e',  # From the URL
            'show_id': '137678',  # Paris in Love
            'credit_id': '65faa9c604733f0164e60334'
        },
        {
            'name': 'Kim Kardashian (example)',
            'show_id': '154521',  # The Kardashians
            'credit_id': '6261446f6eecee13a4131657',
            'person_id': '56a7b62a9251413c2e00175d'
        }
    ]
    
    for test_case in test_cases:
        print(f"\n🧪 Testing: {test_case['name']}")
        print(f"🔍 Show ID: {test_case['show_id']}")
        print(f"🔍 Credit ID: {test_case['credit_id']}")
        
        # Test credit details endpoint
        credit_url = f"{base_url}/credit/{test_case['credit_id']}"
        print(f"🔍 Credit URL: {credit_url}")
        
        response = session.get(credit_url)
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Credit response keys: {list(data.keys())}")
            
            if 'media' in data:
                media = data['media']
                print(f"📺 Media keys: {list(media.keys())}")
                
                if 'seasons' in media:
                    seasons = media['seasons']
                    print(f"📅 Found {len(seasons)} seasons:")
                    for season in seasons:
                        print(f"   Season {season.get('season_number')}: {season.get('name', 'Unknown')}")
                
                if 'episodes' in media:
                    episodes = media['episodes']
                    print(f"📺 Found {len(episodes)} episodes in credit")
                    season_numbers = set()
                    for ep in episodes[:5]:  # Show first 5
                        season_num = ep.get('season_number')
                        episode_num = ep.get('episode_number')
                        name = ep.get('name', 'Unknown')
                        print(f"   S{season_num}E{episode_num}: {name}")
                        season_numbers.add(season_num)
                    print(f"   Unique seasons from episodes: {sorted(season_numbers)}")
            
        else:
            print(f"❌ Credit request failed: {response.status_code}")
        
        # Test person credits endpoint  
        if 'person_id' in test_case:
            person_url = f"{base_url}/person/{test_case['person_id']}/tv_credits"
            print(f"🔍 Person credits URL: {person_url}")
            
            response = session.get(person_url)
            if response.status_code == 200:
                data = response.json()
                
                # Look for the specific show
                for credit in data.get('cast', []) + data.get('crew', []):
                    if str(credit.get('id')) == test_case['show_id']:
                        print(f"✅ Found credit for show {test_case['show_id']}:")
                        print(f"   Episode count: {credit.get('episode_count', 'N/A')}")
                        print(f"   Credit ID: {credit.get('credit_id', 'N/A')}")
                        print(f"   Character: {credit.get('character', 'N/A')}")
                        break
            else:
                print(f"❌ Person credits failed: {response.status_code}")

if __name__ == "__main__":
    test_tmdb_extraction()
