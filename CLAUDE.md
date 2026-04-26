# CLAUDE.md — `yl-hb-sk` (Songkick + Last.fm enrichment via Airtable)

Conventions shared across the `yl-hb-*` fleet live in
[`SCRAPER-CLAUDE-TEMPLATE.md`](../SCRAPER-CLAUDE-TEMPLATE.md). **This repo
diverges from the template** because it doesn't talk to Supabase at all.

## What this repo does

Two Python scrapers that hit **Songkick** and **Last.fm** profile pages,
extract artist data (followers/trackers, biography, upcoming gigs, related
artists, popularity rankings) and bulk-PATCH it back into Airtable.

## Stack

**Custom variant — Airtable-only.** Two standalone Python scripts using
`requests` + `beautifulsoup4`. Single GitHub Actions workflow currently
wired up for the Songkick scraper.

## Repo layout

```
enrich_songkick.py
enrich_lastfm.py
requirements.txt                      # requests, beautifulsoup4
README.md
.github/workflows/
  enrich_songkick.yml                 # only Songkick is currently scheduled
```

> The Last.fm scraper has no scheduled workflow. If you want it on cron,
> copy `enrich_songkick.yml` and swap the script name.

## Auth

> Convention divergence: no `SUPABASE_*` env vars.

```
AIRTABLE_API_KEY        # required
```

The Airtable base / table / view IDs are hardcoded in each script. No
external API key is needed — both Songkick and Last.fm are scraped via
unauthenticated public pages with `requests` + soup.

## Workflow lifecycle convention

> Convention divergence: no `log_workflow_run` — the dashboard won't see
> runs of this job. Add the service-role auth + standard blocks per
> template if observability is wanted later.

## Tables this repo touches

Airtable only — no Supabase tables.

| Operation | Notes |
|---|---|
| Airtable SELECT (paged 100/req) | Reads artist records from a specific view |
| Airtable PATCH (10/req) | Updates with: followers, biography, touring boolean, upcoming gigs JSON, popularity rank, related artists, distance, most-played cities |

## Running locally

```bash
pip install -r requirements.txt
export AIRTABLE_API_KEY=...
python enrich_songkick.py --all       # full sweep (or --limit N)
python enrich_lastfm.py --all
```

## Per-repo gotchas

- **Songkick and Last.fm are public-page scrapers** — no API key needed,
  no contractual rate limits, but they will block aggressive scraping.
  Keep concurrency low and add sleeps between requests.
- **No retry/backoff on 5xx or rate-limit responses** in either script.
  Add at the request layer if scraping volume goes up.
- **Bulk-PATCH 10 is the Airtable max per request.** Don't switch to
  single-record updates.
- **Last.fm scraper has no scheduled workflow.** Anyone running it now
  is doing so manually.

## Conventions Claude should follow when editing this repo

- **Don't add a Supabase client unless the data layer is migrating off
  Airtable.** This is intentionally Airtable-only.
- **When adding new fields, dedupe in Airtable before pushing through
  the bulk-PATCH** — Airtable doesn't dedupe for you.

## Related repos

- `yl-hb-ig`, `yl-hb-sc`, `yl-hb-tw` — sibling Airtable-only scrapers.
- The remaining `yl-hb-*` repos write to Supabase.
