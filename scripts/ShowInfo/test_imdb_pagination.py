#!/usr/bin/env python3
"""
Test script to verify IMDb pagination is working correctly.
This will show us the actual pagination structure and ensure we're getting all pages.
"""
import os
import time
import re
import requests
from bs4 import BeautifulSoup
from pathlib import Path
from dotenv import load_dotenv

# Load environment
HERE = Path(__file__).resolve().parent
load_dotenv(HERE.parent / ".env", override=True)

IMDB_LIST_URL = os.getenv("IMDB_LIST_URL", "https://www.imdb.com/list/ls4106677119/")

def test_imdb_pagination():
    """Test the IMDb pagination to ensure we capture all pages."""
    base = IMDB_LIST_URL.split("?")[0].rstrip("/") + "/"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    
    print(f"Testing IMDb list: {IMDB_LIST_URL}")
    print(f"Base URL: {base}")
    
    page_url = base
    page_num = 1
    visited = set()
    total_shows = 0
    
    while page_url and page_url not in visited and page_num <= 20:  # Safety limit
        visited.add(page_url)
        print(f"\n=== PAGE {page_num} ===")
        print(f"URL: {page_url}")
        
        try:
            resp = requests.get(page_url, headers=headers, timeout=30)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.content, "html.parser")
            
            # Count shows on this page
            items = soup.find_all("li", class_="ipc-metadata-list-summary-item")
            page_count = len(items)
            total_shows += page_count
            print(f"Shows on this page: {page_count}")
            print(f"Total shows so far: {total_shows}")
            
            # Show first few show titles for verification
            for i, item in enumerate(items[:3]):
                try:
                    link = item.find("a", class_="ipc-title-link-wrapper")
                    if link:
                        title_elem = link.find("h3")
                        if title_elem:
                            title = re.sub(r"^\d+\.\s*", "", title_elem.get_text(strip=True))
                            href = link.get("href", "")
                            imdb_match = re.search(r"/title/(tt\d+)/", href)
                            imdb_id = imdb_match.group(1) if imdb_match else "Unknown"
                            print(f"  {i+1}. {title} ({imdb_id})")
                except Exception as e:
                    print(f"  Error parsing item {i+1}: {e}")
            
            # Look for pagination info
            pagination_section = soup.find("nav", attrs={"aria-label": "Pagination"})
            if pagination_section:
                print(f"Found pagination section: {pagination_section.get('class', [])}")
                
                # Look for all pagination links
                page_links = pagination_section.find_all("a")
                print(f"Pagination links found: {len(page_links)}")
                for link in page_links:
                    link_text = link.get_text(strip=True)
                    link_href = link.get("href", "")
                    link_aria = link.get("aria-label", "")
                    print(f"  Link: '{link_text}' | aria-label: '{link_aria}' | href: {link_href[:50]}...")
            
            # Find next page using multiple selectors
            next_selectors = [
                'a[aria-label="Next Page"]',
                'a[aria-label="Next"]', 
                'a[class*="next"]',
                'a[class*="pagination"][class*="btn"]'
            ]
            
            next_link = None
            for selector in next_selectors:
                next_link = soup.select_one(selector)
                if next_link:
                    print(f"Found next link with selector: {selector}")
                    break
            
            # Also try the original fallback method
            if not next_link:
                next_link = soup.find("a", class_=lambda c: c and "ipc-pagination__btn" in c and "next" in (c or ""))
                if next_link:
                    print("Found next link with fallback method")
            
            if next_link:
                href = next_link.get("href", "")
                print(f"Next page href: {href}")
                
                if href:
                    if href.startswith("/"):
                        page_url = "https://www.imdb.com" + href
                    elif href.startswith("http"):
                        page_url = href
                    else:
                        page_url = base + href.lstrip("./")
                    
                    print(f"Next page URL: {page_url}")
                    page_num += 1
                    time.sleep(1)  # Be nice to IMDb
                else:
                    print("Next link has no href - stopping")
                    break
            else:
                print("No next page link found - end of pagination")
                break
                
        except Exception as e:
            print(f"Error fetching page {page_num}: {e}")
            break
    
    print(f"\n=== SUMMARY ===")
    print(f"Total pages processed: {page_num}")
    print(f"Total shows found: {total_shows}")
    print(f"URLs visited: {len(visited)}")

if __name__ == "__main__":
    test_imdb_pagination()
