# yl-hb-sk: Songkick Artist Enrichment

This repository contains an automated scraper to enrich Airtable records with artist data from Songkick.

## Data Extracted
- **Artist Metadata**: Name, Followers (Trackers), Biography.
- **Touring Status**: Boolean and a JSON array of upcoming concerts.
- **Performance Metrics**: Most played cities, Popularity ranking, Collaborators, Distance traveled, and Related artists.

## Setup
### Local Execution
1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Set your environment variable:
   ```bash
   export AIRTABLE_API_KEY="your_key_here"
   ```
3. Run the script:
   ```bash
   python enrich_songkick.py --all
   ```

### GitHub Actions
The script is set up to run daily at 3 AM UTC via GitHub Actions.
- Ensure you add `AIRTABLE_API_KEY` to your **Repository Secrets** (Settings > Secrets and variables > Actions).
- You can also trigger it manually from the "Actions" tab.

## View Requirements
The script processes records from the **"To Process"** view (`viwWo16zremTogroi`) in the Airtable table `tblF13Ue2J0VdaLb1`.
