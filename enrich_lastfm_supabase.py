import os
import requests
from bs4 import BeautifulSoup
import re
import time
import argparse
from datetime import datetime
from urllib.parse import urlparse, unquote_plus
from supabase import create_client

# ── Config ─────────────────────────────────────────────────────────────────────
SUPABASE_URL     = os.environ['SUPABASE_URL']
SUPABASE_KEY     = os.environ['SUPABASE_SERVICE_KEY']
LASTFM_API_KEY   = os.environ.get("LASTFM_API_KEY", "227ce34b7a8ef247e69613c03101508f")
LASTFM_BASE_URL  = "https://ws.audioscrobbler.com/2.0/"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

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

def scrape_lastfm_events(artist_name):
    """Scrape upcoming events as Last.fm API no longer supports this."""
    events = []
    url = f"https://www.last.fm/music/{artist_name}/+events"
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
    try:
        r = requests.get(url, headers=headers, timeout=15)
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, 'html.parser')
            rows = soup.select(".events-list-item")
            for row in rows:
                event_name  = row.select_one(".events-list-item-name")
                event_date  = row.select_one(".events-list-item-date")
                event_venue = row.select_one(".events-list-item-venue")
                if event_name:
                    events.append({
                        "name":  event_name.get_text(strip=True),
                        "date":  event_date.get_text(strip=True) if event_date else "",
                        "venue": event_venue.get_text(strip=True) if event_venue else "",
                    })
    except Exception as e:
        print(f"  [WARN] Event scraping failed: {e}", flush=True)
    return events

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
    for attempt in range(2):
        try:
            r = requests.get(LASTFM_BASE_URL, params=params, timeout=25)
            if r.status_code == 200:
                return r.json()
            elif r.status_code == 404:
                return None
            else:
                print(f"  [WARN] {method} response: {r.status_code}", flush=True)
        except (requests.exceptions.Timeout, requests.exceptions.ReadTimeout):
            if attempt == 0:
                print(f"  Timeout on {method}, retrying...", flush=True)
                time.sleep(1)
            else:
                print(f"  [FAIL] {method} timed out after 2 attempts.", flush=True)
        except Exception as e:
            print(f"  [WARN] {method} error: {e}", flush=True)
    return None

def enrich_lastfm_artist(artist_name: str, lastfm_url: str) -> dict:
    """Fetch all requested data from Last.fm. Returns a flat result dict."""
    result = {
        "listener_count": None,
        "play_count":     None,
        "bio":            "",
        "tags":           [],
        "events":         [],
    }

    # 1. artist.getinfo
    info = fetch_lastfm("artist.getinfo", artist_name)
    if not info or "artist" not in info:
        print(f"  [SKIP] Main profile call failed — skipping secondary calls.", flush=True)
        return result

    a = info["artist"]
    stats = a.get("stats", {})
    result["listener_count"] = int(stats.get("listeners", 0) or 0)
    result["play_count"]     = int(stats.get("playcount",  0) or 0)

    tags = [t["name"] for t in a.get("tags", {}).get("tag", []) if isinstance(t, dict)]
    result["tags"] = tags

    bio_summary = clean_bio(a.get("bio", {}).get("summary", ""))
    bio_full    = clean_bio(a.get("bio", {}).get("content", ""))
    result["bio"] = bio_full if bio_full else bio_summary

    # 2. Scrape Events
    slug, _ = extract_artist_slug(lastfm_url)
    target  = slug if slug else artist_name
    result["events"] = scrape_lastfm_events(target)

    return result

def upsert_events(events: list, linked_talent: str):
    """Write scraped Last.fm events to hb_events (best-effort, skip on error)."""
    if not events or not linked_talent:
        return
    for ev in events:
        if not ev.get("name") or not ev.get("date"):
            continue
        try:
            supabase.table('hb_events').insert({
                "name":          ev["name"],
                "date_start":    ev["date"],
                "location_name": ev.get("venue", ""),
                "linked_talent": [linked_talent],
            }).execute()
        except Exception as e:
            print(f"  [WARN] hb_events insert failed for '{ev['name']}': {e}", flush=True)

def process_record(record: dict):
    """Enrich one hb_socials row and write results back to Supabase."""
    social_id      = record["id"]
    social_url     = (record.get("social_url") or "").strip()
    linked_talent  = record.get("linked_talent")  # UUID or None

    _, artist_name = extract_artist_slug(social_url)
    if not artist_name:
        print("  [SKIP] Cannot parse artist slug from URL.", flush=True)
        return False

    data = enrich_lastfm_artist(artist_name, social_url)
    now  = datetime.utcnow().isoformat()

    # 1. Update hb_socials
    social_update = {
        "last_analytics_check": now,
        "updated_at":           now,
    }
    if data["listener_count"] is not None:
        social_update["followers"]    = data["listener_count"]
        social_update["interactions"] = data["play_count"]
    if data["bio"]:
        social_update["description"] = data["bio"]

    supabase.table('hb_socials').update(social_update).eq('id', social_id).execute()

    # 2. Conditionally update hb_talent (only if linked and fields are empty)
    if linked_talent and (data["bio"] or data["tags"]):
        try:
            talent_row = supabase.table('hb_talent').select('biography, categories').eq('id', linked_talent).maybe_single().execute()
            if talent_row and talent_row.data:
                talent_update = {"updated_at": now}
                existing_bio  = (talent_row.data.get("biography") or "").strip()
                existing_cats = talent_row.data.get("categories") or []

                if not existing_bio and data["bio"]:
                    talent_update["biography"] = data["bio"]
                if not existing_cats and data["tags"]:
                    talent_update["categories"] = data["tags"][:8]

                if len(talent_update) > 1:  # more than just updated_at
                    supabase.table('hb_talent').update(talent_update).eq('id', linked_talent).execute()
        except Exception as e:
            print(f"  [WARN] hb_talent update failed: {e}", flush=True)

    # 3. Upsert events
    upsert_events(data["events"], linked_talent)

    listeners = data["listener_count"] if data["listener_count"] is not None else "n/a"
    print(f"  OK (listeners: {listeners}, events: {len(data['events'])})", flush=True)
    return True

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--all",   action="store_true")
    args = parser.parse_args()

    limit = args.limit if not args.all else None
    if not args.all and not args.limit:
        limit = 5

    LIMIT = limit or 500
    print(f"Starting Last.fm Supabase Enrichment (limit={LIMIT})...", flush=True)

    response = (
        supabase.table('hb_socials')
        .select('id, social_url, identifier, linked_talent')
        .eq('type', 'LastFM')
        .not_.is_('social_url', 'null')
        .order('last_analytics_check', desc=False, nullsfirst=True)
        .limit(LIMIT)
        .execute()
    )

    records = response.data or []
    print(f"Fetched {len(records)} records.", flush=True)

    processed = 0
    for i, record in enumerate(records):
        identifier = record.get("identifier") or record.get("social_url") or record.get("id")
        print(f"[{i+1}/{len(records)}] {identifier}", end=" ", flush=True)
        try:
            process_record(record)
        except Exception as e:
            print(f"  [FAIL] {e}", flush=True)
        processed += 1
        time.sleep(0.5)

    print(f"\nDone. Processed {processed} records.", flush=True)

if __name__ == "__main__":
    main()
