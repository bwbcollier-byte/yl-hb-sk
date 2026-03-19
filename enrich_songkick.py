import os
import requests
from bs4 import BeautifulSoup
import json
import time
import argparse
from datetime import date
import re

# --- Config ---
# Use environment variables for secrets when running in GitHub Actions or locally with a .env
AIRTABLE_API_KEY = os.environ.get("AIRTABLE_API_KEY")
BASE_ID = "appUEhyrtwZUV5sxO"
TABLE_ID = "tblF13Ue2J0VdaLb1"
VIEW_ID = "viwWo16zremTogroi" # "To Process" View

if not AIRTABLE_API_KEY:
    print("Error: AIRTABLE_API_KEY environment variable not set.")
    exit(1)

AIRTABLE_HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_API_KEY}",
    "Content-Type": "application/json"
}

def scrape_songkick_profile(url):
    """Scrape the Songkick profile page for artist metadata."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
    }
    
    try:
        r = requests.get(url, headers=headers, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, 'html.parser')
        
        data = {
            "name": "",
            "trackers": "",
            "bio": "",
            "touring": "FALSE",
            "concerts": [],
            "most_played": "",
            "popularity_ranking": "",
            "collab_names": "",
            "collab_urls": "",
            "distance": "",
            "rel_names": "",
            "rel_urls": ""
        }
        
        # Name
        h1 = soup.select_one("h1") or soup.select_one(".artist-header h1")
        if h1:
            data["name"] = h1.get_text(strip=True)
            
        # Trackers / Followers
        tracker_selectors = [
            ".artist-header__trackers",
            ".track-button .count",
            ".follower-count",
            ".artist-header__tracker-count",
            ".track-artist-section p"
        ]
        for sel in tracker_selectors:
            tag = soup.select_one(sel)
            if tag:
                text = tag.get_text(strip=True)
                digits = re.sub(r'[^\d]', '', text)
                if digits:
                    data["trackers"] = digits
                    break
        
        if not data["trackers"]:
            tracker_text = soup.find(string=re.compile(r'[\d,]+\s*(trackers|followers|fans|interested)', re.I))
            if tracker_text:
                data["trackers"] = re.sub(r'[^\d]', '', tracker_text)

        # --- Stats Section ---
        # 1. Most Played Cities
        most_played = []
        mp_card = soup.find("div", class_="artist-stats__card", string=re.compile(r"Most played", re.I))
        if not mp_card:
             mp_header = soup.find(class_="artist-stats__card-title", string=re.compile(r"Most played", re.I))
             if mp_header: mp_card = mp_header.find_parent("div", class_="artist-stats__card")
        
        if mp_card:
            rows = mp_card.select(".artist-stats__ranking-row")
            for row in rows:
                city = row.select_one(".artist-stats__city")
                count = row.select_one(".artist-stats__count-small")
                if city and count:
                    most_played.append(f"{city.get_text(strip=True)} ({count.get_text(strip=True)})")
        data["most_played"] = ", ".join(most_played)

        # 2. Popularity Ranking
        pop_card = soup.find("div", class_="artist-stats__card", string=re.compile(r"Popularity ranking", re.I))
        if not pop_card:
            pop_header = soup.find(class_="artist-stats__card-title", string=re.compile(r"Popularity ranking", re.I))
            if pop_header: pop_card = pop_header.find_parent("div", class_="artist-stats__card")
            
        if pop_card:
            rank_tag = pop_card.select_one(".artist-stats__rank--current")
            if rank_tag:
                 data["popularity_ranking"] = rank_tag.get_text(strip=True)

        # 3. Appears most with (Collaborators)
        collab_names = []
        collab_urls = []
        col_card = soup.find("div", class_="artist-stats__card", string=re.compile(r"Appears most with", re.I))
        if not col_card:
            col_header = soup.find(class_="artist-stats__card-title", string=re.compile(r"Appears most with", re.I))
            if col_header: col_card = col_header.find_parent("div", class_="artist-stats__card")

        if col_card:
            links = col_card.select(".artist-stats__name a")
            for a in links:
                name = a.get_text(strip=True)
                href = a.get("href", "")
                if name: collab_names.append(name)
                if href:
                    full_url = href if href.startswith("http") else f"https://www.songkick.com{href}"
                    collab_urls.append(full_url)
        data["collab_names"] = ", ".join(collab_names)
        data["collab_urls"] = ", ".join(collab_urls)

        # 4. Distance Traveled
        dist_val = soup.select_one(".artist-stats__metric-value")
        data["distance"] = dist_val.get_text(strip=True) if dist_val else ""

        # 5. Related Artists
        rel_names = []
        rel_urls = []
        rel_card = soup.select_one(".related-artists") or soup.select_one(".similar-artists") or soup.select_one(".related-artists-v2")
        if rel_card:
            links = rel_card.select("a.artist-name") or rel_card.select(".artist-stats__name a")
            for a in links:
                name = a.get_text(strip=True)
                href = a.get("href", "")
                if name: rel_names.append(name)
                if href:
                    full_url = href if href.startswith("http") else f"https://www.songkick.com{href}"
                    rel_urls.append(full_url)
        data["rel_names"] = ", ".join(rel_names)
        data["rel_urls"] = ", ".join(rel_urls)

        # Bio
        bio_tag = soup.select_one(".artist-biography") or soup.select_one(".biography-container")
        if not bio_tag:
             bio_header = soup.find(['h2', 'h3'], string=re.compile(r'Biography', re.I))
             if bio_header:
                 bio_tag = bio_header.find_next('div') or bio_header.find_next('p')
        
        if bio_tag:
            paragraphs = bio_tag.find_all('p')
            if paragraphs:
                bio_text = "\n\n".join([p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)])
            else:
                bio_text = bio_tag.get_text(strip=True)
            data["bio"] = str(bio_text).replace("Read more", "").strip()
            
        # Touring Status & Events
        for script in soup.find_all('script', type='application/ld+json'):
            try:
                ld = json.loads(script.string)
                items = ld if isinstance(ld, list) else [ld]
                for item in items:
                    if item.get('@type') == 'MusicEvent':
                        event = {
                            "name": item.get("name"),
                            "url": item.get("url"),
                            "startDate": item.get("startDate"),
                            "location": item.get("location", {}).get("name"),
                            "city": item.get("location", {}).get("address", {}).get("addressLocality"),
                            "country": item.get("location", {}).get("address", {}).get("addressCountry")
                        }
                        data["concerts"].append(event)
            except:
                continue
                
        if data["concerts"]:
            data["touring"] = "TRUE"
        else:
            on_tour_tag = soup.find(string=re.compile(r'On tour:', re.I))
            if on_tour_tag and "yes" in on_tour_tag.lower():
                data["touring"] = "TRUE"

        return data
        
    except Exception as e:
        print(f"  [Error] Scraping {url}: {e}")
        return None

def update_records_bulk(records_batch):
    """Batch update up to 10 records in Airtable."""
    if not records_batch:
        return True
    
    url = f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_ID}"
    r = requests.patch(url, headers=AIRTABLE_HEADERS, json={"records": records_batch}, timeout=15)
    if r.status_code != 200:
        print(f"  [Error] Airtable Update: {r.text}")
        return False
    return True

def main():
    parser = argparse.ArgumentParser(description="Enrich Songkick artist profiles in Airtable.")
    parser.add_argument("--limit", type=int, default=None, help="Stop after N records")
    parser.add_argument("--all", action="store_true", help="Process all records in view")
    args = parser.parse_args()
    
    if not args.all and args.limit is None:
        args.limit = 5
        
    print(f"Starting Songkick enrichment for view {VIEW_ID}...")
    
    processed_count = 0
    batch_queue = []
    today_str = date.today().isoformat()
    
    url = f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_ID}"
    params = {
        "view": VIEW_ID,
        "pageSize": 100
    }
    
    while True:
        r = requests.get(url, headers=AIRTABLE_HEADERS, params=params, timeout=15)
        data = r.json()
        
        if "error" in data:
            print(f"[Error] Fetching records: {data}")
            break
            
        records = data.get("records", [])
        if not records:
            break
            
        for record in records:
            if args.limit and processed_count >= args.limit:
                break
                
            rec_id = record["id"]
            fields = record.get("fields", {})
            sk_url = fields.get("Soc Songkick", "").strip()
            name = fields.get("Name", "Unknown")
            
            print(f"[{processed_count + 1}] Processing: {name}")
            
            if not sk_url:
                print("  Skipping: No Songkick URL")
                processed_count += 1
                continue
                
            sk_data = scrape_songkick_profile(sk_url)
            
            if sk_data:
                update_fields = {
                    "Soc SK name": sk_data["name"],
                    "Soc SK trackers": str(sk_data["trackers"]),
                    "Soc SK Bio": sk_data["bio"],
                    "Soc SK touring": sk_data["touring"],
                    "Soc SK Concerts JSON": json.dumps(sk_data["concerts"], ensure_ascii=False),
                    "SK Most played (Array)": sk_data["most_played"],
                    "SK Popularity ranking": sk_data["popularity_ranking"],
                    "SK Appears most with Names": sk_data["collab_names"],
                    "SK Appears most with Urls": sk_data["collab_urls"],
                    "SK Distance traveled": sk_data["distance"],
                    "SK Related artists Names (Array)": sk_data["rel_names"],
                    "SK Related artists Links (Array)": sk_data["rel_urls"],
                    "Last Check": today_str
                }
                
                # Cleanup: Only update if we have a value
                update_fields = {k: v for k, v in update_fields.items() if v and v != "[]" and v != "None"}
                
                batch_queue.append({"id": rec_id, "fields": update_fields})
                
                if len(batch_queue) >= 10:
                    print(f"  --> Flushing batch of {len(batch_queue)}...")
                    update_records_bulk(batch_queue)
                    batch_queue = []
                    time.sleep(0.5)
            
            processed_count += 1
            time.sleep(1.2) # Polite scraping
            
        if args.limit and processed_count >= args.limit:
            break
            
        offset = data.get("offset")
        if not offset:
            break
        params["offset"] = offset

    if batch_queue:
        print(f"  --> Final flush of {len(batch_queue)}...")
        update_records_bulk(batch_queue)

    print(f"\nDone! Processed {processed_count} records.")

if __name__ == "__main__":
    main()
