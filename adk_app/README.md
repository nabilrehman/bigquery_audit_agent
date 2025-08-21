# ADK BigQuery Audit (Local App)

A minimal, sharable app that lets any user authenticate with their own Google account and analyze recent BigQuery query jobs to find the most expensive query and export job stats to CSV.

This uses Google Cloud Application Default Credentials (ADC) for per-user auth and the official BigQuery Python SDK. Share this repo/folder; each user signs in with their own Google account locally.

## Prerequisites
- Python 3.9+
- Google Cloud SDK (for ADC): https://cloud.google.com/sdk/docs/install
- Access to the target GCP project and BigQuery

## One-time setup
```bash
# From repo root
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Per-user OAuth sign-in (opens browser)
gcloud auth application-default login
```

## Run
```bash
# Analyze last 90 days in US and EU, write CSV
python adk_app/main.py \
  --project YOUR_PROJECT_ID \
  --days 90 \
  --locations US,EU \
  --limit 2000 \
  --outfile ./bq_job_stats.csv
```

Output:
- Prints the most expensive query (billed bytes) with details
- Writes `bq_job_stats.csv` with recent query jobs and stats

## Notes
- Each user authenticates with their own account via `gcloud auth application-default login`.
- If you need to distribute without requiring gcloud, adapt `tools/bq_audit.py` to use OAuth client credentials (`google-auth-oauthlib` InstalledAppFlow) and provide instructions for creating an OAuth client in the recipient’s GCP project.
