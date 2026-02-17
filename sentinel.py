import asyncio
import logging
import re
from playwright.async_api import async_playwright
from playwright_stealth import Stealth
from config import settings

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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
            # Launch chromium with stability flags for CI/Linux environments
            browser = await p.chromium.launch(
                headless=self.headless,
                args=[
                    "--disable-gpu",
                    "--disable-dev-shm-usage",
                    "--no-sandbox"
                ]
            )
            context = await browser.new_context()
            
            # Apply stealth
            page = await context.new_page()
            # stealth is applied automatically by the context manager wrapper

            # Listener for requests to capture headers
            async def handle_request(route, request):
                if "marktguru" in request.url:
                    logger.debug(f"Intercepted URL: {request.url}")

                all_headers = request.headers
                # Filter for relevant headers (x- or non-standard)
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
                logger.info(f"Navigating to {target_url} to capture headers...")
                await page.goto(target_url, wait_until="networkidle")
                
                # Refined wait: longer timeout (30s) and specific request trigger
                try:
                    await page.wait_for_request(re.compile(r'.*/api/v1/offers.*'), timeout=settings.SENTINEL_TIMEOUT)
                    logger.info("Captured offers API request successfully.")
                except Exception:
                    logger.warning(f"Timeout waiting for offers API request ({settings.SENTINEL_TIMEOUT}ms), continuing with captured headers so far...")

            except Exception as e:
                logger.error(f"Error during navigation: {e}")
            finally:
                await browser.close()
        
        return self.headers

if __name__ == "__main__":
    sentinel = Sentinel(headless=False) # Run headed to see what happens in debug
    headers = asyncio.run(sentinel.extract_headers())
    logger.info(f"Captured Headers: {headers}")
