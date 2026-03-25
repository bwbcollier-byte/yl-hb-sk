import os
import requests
import re
import time
import argparse
import json
from datetime import date
from urllib.parse import urlparse, unquote_plus

# ── Config ─────────────────────────────────────────────────────────────────────
AIRTABLE_API_KEY = os.environ.get("AIRTABLE_API_KEY")
LASTFM_API_KEY   = os.environ.get("LASTFM_API_KEY", "227ce34b7a8ef247e69613c03101508f")
BASE_ID          = "appENJEi9WyVXFrNU"
TABLE_ID         = "tbl49Xnh3IIYjOi5q"
VIEW_ID          = "viw0ZWMs5Ky5rsf9d" # New view for Last.fm
LASTFM_BASE_URL  = "http://ws.audioscrobbler.com/2.0/"

if not AIRTABLE_API_KEY:
    print("Error: AIRTABLE_API_KEY environment variable not set.")
    exit(1)

AIRTABLE_HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_API_KEY}",
    "Content-Type": "application/json"
}

# ── Helpers ────────────────────────────────────────────────────────────────────

def extract_artist_slug(url: str):
    """Parse artist name from Last.fm URL."""
    if not url: return None, None
    parsed = urlparse(url)
    parts  = parsed.path.strip('/').split('/')
    if len(parts) >= 2 and parts[0] == 'music':
        slug = parts[1]
        name = unquote_plus(slug)
        return slug, name
    return None, None

def clean_bio(text: str) -> str:
    """Strip Last.fm attribution tags."""
    if not text: return ""
    text = re.sub(r'<a href=[^>]+>.*?</a>', '', text, flags=re.DOTALL)
    return text.strip()

def fetch_lastfm(method: str, artist: str, **extra):
    params = {
        "method":  method,
        "artist":  artist,
        "api_key": LASTFM_API_KEY,
        "format":  "json",
        **extra
    }
    try:
        r = requests.get(LASTFM_BASE_URL, params=params, timeout=12)
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        print(f"  [WARN] {method} failed for '{artist}': {e}")
    return None

def enrich_lastfm_artist(artist_name: str, lastfm_url: str) -> dict:
    """Fetch all requested data from Last.fm."""
    fields = {
        "lfm_url": lastfm_url,
        "lfm_id": "",
        "lfm_listeners": "0",
        "lfm_tags": "",
        "lfm_bio": "",
        "lfm_bio_full": "",
        "lfm_image": "",
        "lfm_on_tour": "No",
        "lfm_playcount": "0",
        "lfm_mbid": "",
        "lfm_bio_published_date": "",
        "lfm_wiki_url": "",
        "lfm_enriched": "No",
        "lfm_last_check": date.today().isoformat()
    }

    slug, _ = extract_artist_slug(lastfm_url)
    fields["lfm_id"] = slug if slug else ""

    # 1. artist.getinfo
    info = fetch_lastfm("artist.getinfo", artist_name)
    if info and "artist" in info:
        a = info["artist"]
        stats = a.get("stats", {})
        fields["lfm_listeners"] = stats.get("listeners", "0")
        fields["lfm_playcount"] = stats.get("playcount", "0")
        fields["lfm_mbid"] = a.get("mbid", "")
        fields["lfm_on_tour"] = "Yes" if str(a.get("ontour", "0")) == "1" else "No"
        
        tags = [t["name"] for t in a.get("tags", {}).get("tag", []) if isinstance(t, dict)]
        fields["lfm_tags"] = ", ".join(tags)
        
        fields["lfm_bio"] = clean_bio(a.get("bio", {}).get("summary", ""))
        fields["lfm_bio_full"] = clean_bio(a.get("bio", {}).get("content", ""))
        fields["lfm_bio_published_date"] = a.get("bio", {}).get("published", "")
        
        bio_link = a.get("bio", {}).get("links", {}).get("link", {})
        fields["lfm_wiki_url"] = bio_link.get("href", "") if isinstance(bio_link, dict) else ""
        
        images = {img["size"]: img["#text"] for img in a.get("image", []) if img.get("#text")}
        fields["lfm_image"] = images.get("extralarge") or images.get("large") or ""
        fields["lfm_enriched"] = "Yes"

    # 2. Similar Artists
    sim_data = fetch_lastfm("artist.getSimilar", artist_name, limit=10)
    if sim_data and "similarartists" in sim_data:
        sims = sim_data["similarartists"].get("artist", [])
        fields["lfm_similar_rtists"] = ", ".join(s["name"] for s in sims if isinstance(s, dict))
        fields["lfm_similar_artists_urls"] = ", ".join(s.get("url", "") for s in sims if isinstance(s, dict))

    # 3. Top Tracks
    tracks_data = fetch_lastfm("artist.getTopTracks", artist_name, limit=10)
    if tracks_data and "toptracks" in tracks_data:
        tracks = tracks_data["toptracks"].get("track", [])
        fields["lfm_top_tracks"] = ", ".join(t["name"] for t in tracks if isinstance(t, dict))
        fields["lfm_top_tracks_urls"] = ", ".join(t.get("url", "") for t in tracks if isinstance(t, dict))

    # 4. Top Albums
    albums_data = fetch_lastfm("artist.getTopAlbums", artist_name, limit=10)
    if albums_data and "topalbums" in albums_data:
        albums = albums_data["topalbums"].get("album", [])
        fields["lfm_top_albums"] = ", ".join(al["name"] for al in albums if isinstance(al, dict))
        fields["lfm_top_albums_urls"] = ", ".join(al.get("url", "") for al in albums if isinstance(al, dict))

    return fields

def update_records_bulk(records_batch):
    if not records_batch: return True
    url = f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_ID}"
    r = requests.patch(url, headers=AIRTABLE_HEADERS, json={"records": records_batch}, timeout=15)
    return r.status_code == 200

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--all", action="store_true")
    args = parser.parse_args()

    limit = args.limit if not args.all else None
    if not args.all and not args.limit: limit = 5

    print(f"🚀 Starting Last.fm Enrichment for view: {VIEW_ID}...", flush=True)
    
    url = f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_ID}"
    params = {"view": VIEW_ID, "pageSize": 100}
    processed_count = 0
    batch_queue = []

    while True:
        r = requests.get(url, headers=AIRTABLE_HEADERS, params=params)
        data = r.json()
        if "error" in data: break
        
        records = data.get("records", [])
        if not records: break
        
        for record in records:
            if limit and processed_count >= limit: break
            
            rec_id = record["id"]
            fields = record.get("fields", {})
            lfm_url = fields.get("soc_lastfm", "").strip()
            name = fields.get("Name", "Unknown Artist")

            print(f"🔍 [{processed_count+1}] Processing: {name}", end=" ", flush=True)
            
            if not lfm_url:
                print("⏭️ Skipped (No Last.fm URL)", flush=True)
                processed_count += 1
                continue
            
            _, artist_name = extract_artist_slug(lfm_url)
            if not artist_name:
                print("❌ Invalid URL", flush=True)
                processed_count += 1
                continue

            try:
                enriched_data = enrich_lastfm_artist(artist_name, lfm_url)
                # Cleanup empty fields
                update_fields = {k: v for k, v in enriched_data.items() if v}
                batch_queue.append({"id": rec_id, "fields": update_fields})
                print(f"✅ Success (Listeners: {enriched_data['lfm_listeners']})", flush=True)
            except Exception as e:
                print(f"❌ Failed: {e}", flush=True)

            if len(batch_queue) >= 10:
                update_records_bulk(batch_queue)
                batch_queue = []
                print("  📤 Batch updated.", flush=True)
                time.sleep(0.5)

            processed_count += 1
            time.sleep(0.5)

        if limit and processed_count >= limit: break
        offset = data.get("offset")
        if not offset: break
        params["offset"] = offset

    if batch_queue:
        update_records_bulk(batch_queue)
        print("  📤 Final batch updated.", flush=True)

    print(f"\n🎉 Done! Processed {processed_count} records.", flush=True)

if __name__ == "__main__":
    main()
