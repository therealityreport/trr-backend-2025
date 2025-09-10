#!/usr/bin/env python3
"""
Test script with a large IMDb list that definitely has pagination
"""
import os
import time
import re
import requests
from bs4 import BeautifulSoup
from pathlib import Path

def test_large_imdb_list():
    """Test with a larger IMDb list that has pagination."""
    # Using a known large IMDb list for testing
    test_url = "https://www.imdb.com/list/ls000522954/"  # Top 250 movies - should have pagination
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    
    print(f"Testing large IMDb list: {test_url}")
    
    try:
        resp = requests.get(test_url, headers=headers, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, "html.parser")
        
        # Count items on first page
        items = soup.find_all("li", class_="ipc-metadata-list-summary-item")
        print(f"Items on first page: {len(items)}")
        
        # Look for pagination
        pagination_section = soup.find("nav", attrs={"aria-label": "Pagination"})
        if pagination_section:
            print("Found pagination section!")
            
            # Get all pagination elements
            page_elements = pagination_section.find_all(["a", "span"])
            print("Pagination elements:")
            for elem in page_elements:
                text = elem.get_text(strip=True)
                if elem.name == "a":
                    href = elem.get("href", "")
                    aria_label = elem.get("aria-label", "")
                    classes = elem.get("class", [])
                    print(f"  LINK: '{text}' | aria-label: '{aria_label}' | classes: {classes} | href: {href[:50]}...")
                else:
                    classes = elem.get("class", [])
                    print(f"  SPAN: '{text}' | classes: {classes}")
        else:
            print("No pagination section found")
        
        # Test various next page selectors
        next_selectors = [
            'a[aria-label="Next Page"]',
            'a[aria-label="Next"]',
            'a[aria-label="Go to next page"]',
            '.ipc-pagination .ipc-pagination__btn--next',
            '.ipc-pagination .ipc-pagination__btn[aria-label*="next" i]',
            'a.ipc-pagination__btn',
            'a[class*="pagination"][class*="next"]'
        ]
        
        for selector in next_selectors:
            next_link = soup.select_one(selector)
            if next_link:
                href = next_link.get("href", "")
                aria_label = next_link.get("aria-label", "")
                classes = next_link.get("class", [])
                text = next_link.get_text(strip=True)
                print(f"FOUND with '{selector}': text='{text}' aria-label='{aria_label}' classes={classes} href={href[:50]}...")
                break
        else:
            print("No next link found with any selector")
    
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test_large_imdb_list()
