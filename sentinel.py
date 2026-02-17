import asyncio
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

class Sentinel:
    def __init__(self, headless=True):
        self.headless = headless
        self.headers = {}

    async def extract_headers(self, url="https://www.marktguru.de"):
        """
        Launches a stealth browser, navigates to the target URL,
        and intercepts request headers to extract dynamic/custom headers.
        """
        # Use Stealth context manager
        async with Stealth().use_async(async_playwright()) as p:
            # Launch chromium explicitly
            browser = await p.chromium.launch(headless=self.headless)
            context = await browser.new_context()
            
            # Apply stealth
            page = await context.new_page()
            # stealth is applied automatically by the context manager wrapper

            # Listener for requests to capture headers
            async def handle_request(route, request):
                if "marktguru" in request.url:
                    print(f"DEBUG: Intercepted URL: {request.url}")

                all_headers = request.headers
                # Filter for relevant headers (x- or non-standard)
                # This logic can be refined based on specific needs
                for key, value in all_headers.items():
                    if key.lower().startswith("x-") or key.lower() not in [
                        "host", "connection", "sec-ch-ua", "sec-ch-ua-mobile",
                        "user-agent", "sec-ch-ua-platform", "accept",
                        "sec-fetch-site", "sec-fetch-mode", "sec-fetch-user",
                        "sec-fetch-dest", "referer", "accept-encoding", "accept-language"
                    ]:
                         self.headers[key] = value
                
                await route.continue_()

            await page.route("**/*", handle_request)
            
            try:
                # Visit a specific retailer page to trigger relevant API calls
                target_url = url if url != "https://www.marktguru.de" else "https://www.marktguru.de/angebote/kaufland"
                print(f"Sentinel: Navigating to {target_url} to capture headers...")
                await page.goto(target_url, wait_until="networkidle")
                # Wait a bit to ensure dynamic requests fire
                await page.wait_for_timeout(5000) 
            except Exception as e:
                print(f"Sentinel Error during navigation: {e}")
            finally:
                await browser.close()
        
        return self.headers

if __name__ == "__main__":
    sentinel = Sentinel(headless=False) # Run headed to see what happens in debug
    headers = asyncio.run(sentinel.extract_headers())
    print("Captured Headers:", headers)
