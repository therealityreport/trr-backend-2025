#!/usr/bin/env python3
"""
Debug the IMDb list structure to understand pagination
"""
import os
import requests
from bs4 import BeautifulSoup
from pathlib import Path
from dotenv import load_dotenv

HERE = Path(__file__).resolve().parent
load_dotenv(HERE.parent / ".env", override=True)

IMDB_LIST_URL = os.getenv("IMDB_LIST_URL", "https://www.imdb.com/list/ls4106677119/")

def debug_imdb_structure():
    """Debug the actual IMDb list structure."""
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    
    print(f"Debugging: {IMDB_LIST_URL}")
    
    # Try different URL variations
    urls_to_try = [
        IMDB_LIST_URL,
        IMDB_LIST_URL.rstrip("/") + "/?view=detail",
        IMDB_LIST_URL.rstrip("/") + "/?sort=list_order,asc",
        IMDB_LIST_URL.rstrip("/") + "/?sort=list_order,asc&view=detail",
        IMDB_LIST_URL.rstrip("/") + "/?mode=detail",
        IMDB_LIST_URL.rstrip("/") + "/?ref_=uspf_t_1",
    ]
    
    for i, url in enumerate(urls_to_try):
        print(f"\n=== TRYING URL {i+1}: {url} ===")
        try:
            resp = requests.get(url, headers=headers, timeout=30)
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.content, "html.parser")
                items = soup.find_all("li", class_="ipc-metadata-list-summary-item")
                print(f"Items found: {len(items)}")
                
                if len(items) > 25:
                    print(f"🎉 FOUND FULL LIST! {len(items)} items")
                    # Show first few titles to verify
                    for j, item in enumerate(items[:5]):
                        try:
                            link = item.find("a", class_="ipc-title-link-wrapper")
                            if link:
                                title_elem = link.find("h3")
                                if title_elem:
                                    title = title_elem.get_text(strip=True)
                                    print(f"  {j+1}. {title}")
                        except:
                            pass
                    print(f"  ... and {len(items)-5} more")
                    return url, len(items)
                    
            else:
                print(f"Status: {resp.status_code}")
        except Exception as e:
            print(f"Error: {e}")
    
    print("\n❌ None of the URL variations worked to get the full list")
    return None, 0

if __name__ == "__main__":
    debug_imdb_structure()
