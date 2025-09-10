#!/usr/bin/env python3
"""
Check pagination controls and try to follow them properly
"""
import os
import requests
from bs4 import BeautifulSoup
from pathlib import Path
from dotenv import load_dotenv
import re

HERE = Path(__file__).resolve().parent
load_dotenv(HERE.parent / ".env", override=True)

IMDB_LIST_URL = os.getenv("IMDB_LIST_URL", "https://www.imdb.com/list/ls4106677119/")

def check_pagination():
    """Check the actual pagination structure."""
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    
    url = IMDB_LIST_URL
    print(f"Checking pagination on: {url}")
    
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        soup = BeautifulSoup(resp.content, "html.parser")
        
        print(f"Items on page: {len(soup.find_all('li', class_='ipc-metadata-list-summary-item'))}")
        
        # Look for any pagination elements
        print("\n=== SEARCHING FOR PAGINATION ===")
        
        # Look for navigation with pagination
        nav_elements = soup.find_all("nav")
        for i, nav in enumerate(nav_elements):
            aria_label = nav.get("aria-label", "")
            if "pagination" in aria_label.lower():
                print(f"Found pagination nav {i}: {aria_label}")
                print(f"HTML: {nav}")
        
        # Look for any elements containing "next" or page numbers
        print("\n=== SEARCHING FOR 'NEXT' ELEMENTS ===")
        next_elements = soup.find_all(text=re.compile(r"next|>", re.I))
        for elem in next_elements[:5]:  # Limit output
            parent = elem.parent if elem.parent else None
            if parent:
                print(f"Text: '{elem.strip()}' | Parent: {parent.name} | Classes: {parent.get('class', [])}")
        
        # Look for numbered pagination
        print("\n=== SEARCHING FOR PAGE NUMBERS ===")
        page_links = soup.find_all("a", text=re.compile(r"^\d+$"))
        for link in page_links:
            href = link.get("href", "")
            text = link.get_text(strip=True)
            classes = link.get("class", [])
            print(f"Page link: '{text}' | href: {href} | classes: {classes}")
        
        # Look for links with "page" in href
        print("\n=== SEARCHING FOR PAGE HREFS ===")
        page_hrefs = soup.find_all("a", href=re.compile(r"page", re.I))
        for link in page_hrefs[:10]:  # Limit output
            href = link.get("href", "")
            text = link.get_text(strip=True)
            print(f"Page href: '{text}' | href: {href}")
        
        # Check if there's any indication of total items
        print("\n=== SEARCHING FOR TOTAL COUNT ===")
        # Look for text patterns like "1-25 of 164"
        count_pattern = re.compile(r"\d+\s*-\s*\d+\s+of\s+(\d+)", re.I)
        page_text = soup.get_text()
        count_match = count_pattern.search(page_text)
        if count_match:
            total = count_match.group(1)
            print(f"Found total count: {total}")
        else:
            print("No total count pattern found")
            
        # Look for any text mentioning the number of items
        item_counts = re.findall(r"(\d+)\s+(?:items?|titles?|shows?)", page_text, re.I)
        if item_counts:
            print(f"Found item counts: {item_counts}")
    
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_pagination()
