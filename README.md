# adk-bq-audit

Installable CLI that authenticates via Google ADC and audits recent BigQuery jobs to find the most expensive query and export a CSV.

## Install (local)
```bash
python3 -m venv .venv && source .venv/bin/activate
pip install --upgrade pip
pip install .
```

## Auth
```bash
gcloud auth application-default login
```

## Run
```bash
bq-adk-audit --project YOUR_PROJECT_ID --days 90 --locations US,EU --limit 2000 --outfile ./bq_job_stats.csv
```

## Uninstall
```bash
pip uninstall adk-bq-audit -y
```
