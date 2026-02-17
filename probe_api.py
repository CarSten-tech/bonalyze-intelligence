
import requests
import json
import re

def main():
    with open("homepage.html", "r") as f:
        content = f.read()

    # Extract Config
    config_match = re.search(r'<script type="application/json">(.*?)</script>', content)
    if not config_match:
        print("No config found")
        return

    data = json.loads(config_match.group(1))
    config = data.get('config', {})
    
    api_key = config.get('apiKey')
    client_key = config.get('clientKey')
    api_host = config.get('apiHostAddress')
    
    print(f"API Key: {api_key}")
    print(f"Client Key: {client_key}")
    print(f"Host: {api_host}")

    # Extract Retailer IDs
    # We look for "id":12345,"name":"Kaufland" structure in the big JSON
    # It's in data['data']['retailerOffers']...
    
    retailers = {}
    targets = ["Kaufland", "ALDI SÜD", "E center", "Edeka", "Aldi", "Lidl"]
    
    # Recursively search JSON for retailer names
    def search_json(obj):
        if isinstance(obj, dict):
            if "name" in obj and "id" in obj:
                name = obj["name"]
                rid = obj["id"]
                #Check if name matches any target
                for t in targets:
                    if t.lower() in str(name).lower() or str(name).lower() in t.lower():
                         if str(rid).startswith("retailers/"):
                             rid = rid.split("/")[-1]
                         if str(rid).isdigit():
                             retailers[name] = rid
            for k, v in obj.items():
                search_json(v)
        elif isinstance(obj, list):
            for item in obj:
                search_json(item)

    search_json(data)
    print("Found Retailers:", retailers)

    kaufland_id = retailers.get("Kaufland")
    if not kaufland_id:
        print("Kaufland ID not found, using fallback 126654")
        kaufland_id = "126654"

    # Try API Call
    url = f"https://{api_host}/api/v1/offers"
    params = {
        "retailerIds": kaufland_id,
        "zipCode": "41460",
        "limit": 2,
        "offset": 2
    }
    
    headers = {
        "x-apikey": api_key,
        "x-clientkey": client_key,
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
        "Origin": "https://www.marktguru.de",
        "Referer": "https://www.marktguru.de/"
    }

    print(f"Testing {url} with params {params}")
    try:
        resp = requests.get(url, params=params, headers=headers)
        print(f"Status: {resp.status_code}")
        print(f"Response: {resp.text[:500]}...")
        
        if resp.status_code == 200:
            offers = resp.json()
            print(f"Got {len(offers)} offers")
            with open("api_test_offers.json", "w") as f:
                json.dump(offers, f, indent=2)
                
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
