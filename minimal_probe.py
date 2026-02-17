import sys
print("Start probe")
try:
    import requests
    import json
    import asyncio
    from playwright.async_api import async_playwright
    from playwright_stealth import Stealth
    import re
    from sentinel import Sentinel # Test this only if others pass
    print("All imports successful")
    
    async def run_pw():
        print("Starting playwright")
        async with Stealth().use_async(async_playwright()) as p:
             print("Playwright launched")
             browser = await p.chromium.launch(headless=True)
             print("Browser launched")
             await browser.close()
             print("Browser closed")
    
    asyncio.run(run_pw())
except ImportError as e:
    print(f"ImportError: {e}")
except Exception as e:
    print(f"Error: {e}")
print("End probe")
