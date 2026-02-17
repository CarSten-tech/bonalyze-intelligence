print("--- SCRIPT START ---")
import sys
import asyncio
import re
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

print("Imports done")

async def main():
    print("--- Scraping HTML for Retailer ID ---")
    async with Stealth().use_async(async_playwright()) as p:
        print("Launching browser")
        browser = await p.chromium.launch(headless=True)
        print("Browser launched")
        page = await browser.new_page()
        
        url = "https://www.marktguru.de/"
        print(f"Navigating to {url}...")
        await page.goto(url, wait_until="networkidle")
        print("Navigation done")
        
        await page.screenshot(path="homepage.png")
        print("Screenshot saved")

        content = await page.content()
        await browser.close()
        
        # Search for "Kaufland" links
        print("Searching for 'Kaufland' links...")
        matches = re.findall(r'href="([^"]*kaufland[^"]*)"', content, re.IGNORECASE)
        if matches:
            print(f"Found links: {matches}")
        else:
            print("No 'Kaufland' links found.")
        
        # Save to file
        with open("homepage.html", "w") as f:
            f.write(content)
        print("Saved to homepage.html")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"CRASH: {e}")
