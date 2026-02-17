import requests
import json

BASE_URL = "https://api.marktguru.de/api/v1"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Origin": "https://www.marktguru.de",
    "Referer": "https://www.marktguru.de/"
}

import asyncio
from sentinel import Sentinel

def probe(headers, url, name):
    print(f"--- Probing {name} ---")
    try:
        response = requests.get(url, headers=headers, timeout=10)
        print(f"Status: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            if isinstance(data, dict):
                print(f"Keys: {list(data.keys())}")
                if "results" in data:
                     results = data["results"]
                     print(f"Results len: {len(results)}")
                     if results:
                         # Print first 3 results summary (advertiser name)
                         for i, res in enumerate(results[:3]):
                             adv = res.get("advertisers", [{}])[0].get("name", "Unknown")
                             prod = res.get("product", {}).get("name", "Unknown")
                             print(f"Result {i}: {adv} - {prod}")
            else:
                print(f"Data type: {type(data)}")
        else:
            print(f"Error: {response.text[:200]}")
    except Exception as e:
        print(f"Exception: {e}")
    print("\n")

from playwright.async_api import async_playwright
from playwright_stealth import Stealth
import re

async def main():
    print("--- Scraping HTML for Retailer ID ---")
    async with Stealth().use_async(async_playwright()) as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        url = "https://www.marktguru.de/angebote/kaufland"
        print(f"Navigating to {url}...")
        await page.goto(url, wait_until="networkidle")
        
        content = await page.content()
        await browser.close()
        
        # Search for "retailerId" patterns
        print("Searching for 'retailerId'...")
        matches = re.findall(r'"retailerId"\s*:\s*(\d+)', content)
        if matches:
            print(f"Found match: {matches}")
        else:
            print("No 'retailerId' found.")

        # Search for "Kaufland" context
        idx = content.find("Kaufland")
        if idx != -1:
             print(f"Found 'Kaufland' at index {idx}")
             start = max(0, idx - 200)
             end = min(len(content), idx + 200)
             print(f"Context: {content[start:end]}")
        
        # Save to file for manual inspection if needed
        with open("kaufland.html", "w") as f:
            f.write(content)
        print("Saved to kaufland.html")
