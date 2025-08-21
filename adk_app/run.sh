#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   ./run.sh --project YOUR_PROJECT_ID [--days 90] [--locations US,EU] [--limit 2000] [--outfile ./bq_job_stats.csv]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${REPO_ROOT}"

if [[ ! -d .venv ]]; then
  python3 -m venv .venv
fi

source .venv/bin/activate
pip install -q -r requirements.txt

# Ensure ADC exists; if not, prompt user to sign in
ADC_PATH="${HOME}/.config/gcloud/application_default_credentials.json"
if ! command -v gcloud >/dev/null 2>&1; then
  echo "gcloud CLI not found. Please install: https://cloud.google.com/sdk/docs/install" >&2
  exit 1
fi

if [[ ! -f "${ADC_PATH}" ]]; then
  echo "No Application Default Credentials found. Launching Google sign-in..."
  gcloud auth application-default login
fi

python adk_app/main.py "$@"


