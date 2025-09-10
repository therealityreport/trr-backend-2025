#!/usr/bin/env python3
"""
Test the enhanced IMDb pagination directly using the updated function
"""
import sys
import os
sys.path.append(os.path.dirname(__file__))

# Import the enhanced function from the main script
from fetch_show_info import fetch_imdb_list_shows

def test_enhanced_pagination():
    """Test the enhanced IMDb scraping function."""
    print("Testing enhanced IMDb pagination...")
    shows = fetch_imdb_list_shows()
    
    print(f"\n=== RESULTS ===")
    print(f"Total shows found: {len(shows)}")
    
    if shows:
        print(f"\nFirst 10 shows:")
        for i, show in enumerate(shows[:10]):
            print(f"  {i+1}. {show['name']} ({show['imdb_id']})")
        
        if len(shows) > 10:
            print(f"\nLast 5 shows:")
            for i, show in enumerate(shows[-5:], len(shows)-4):
                print(f"  {i}. {show['name']} ({show['imdb_id']})")
    
    # Check if we got close to the expected 164 titles
    if len(shows) >= 150:
        print(f"\n✅ SUCCESS: Found {len(shows)} shows (close to expected 164)")
    elif len(shows) >= 100:
        print(f"\n⚠️  PARTIAL: Found {len(shows)} shows (expected ~164)")
    else:
        print(f"\n❌ ISSUE: Only found {len(shows)} shows (expected ~164)")
        print("   The pagination detection may need further refinement.")

if __name__ == "__main__":
    test_enhanced_pagination()
