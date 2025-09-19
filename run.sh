#!/usr/bin/env bash
set -euo pipefail

here() { cd "$(dirname "$0")"; pwd; }
ROOT="$(here)"

usage() {
  cat <<'USAGE'
Usage: ./run.sh <command> [args]

Commands:
  setup                          Create venv and install package (editable)
  audit [--project P] [--days D] Run audit CLI and write CSV (default 30 days)
  forensic-top1                  Generate forensic report for top 1 job
  forensic-top10                 Generate one concatenated report for top 10 local jobs
  inspector                      Run all-job inspector summary over last 30 days
  all                            Run audit, analysis PDF, schema+optimizer, inspector, and top10 bundle

Environment:
  GOOGLE_CLOUD_PROJECT           GCP project (defaults from `gcloud config get-value project`)
  GOOGLE_API_KEY                 Optional key for LLM fallback
USAGE
}

ensure_project() {
  if [[ -z "${GOOGLE_CLOUD_PROJECT:-}" ]]; then
    export GOOGLE_CLOUD_PROJECT="$(gcloud config get-value project 2>/dev/null || true)"
  fi
  if [[ -z "${GOOGLE_CLOUD_PROJECT:-}" ]]; then
    echo "ERROR: GOOGLE_CLOUD_PROJECT not set and gcloud has no project." >&2
    exit 2
  fi
}

ensure_location() {
  # Default Vertex AI location for LLM agents if not provided
  if [[ -z "${GOOGLE_CLOUD_LOCATION:-}" ]]; then
    export GOOGLE_CLOUD_LOCATION="us-central1"
  fi
}

py() { python - "$@"; }

cmd_setup() {
  cd "$ROOT"
  python3 -m venv .venv
  source .venv/bin/activate
  python -m pip install -U pip
  pip install -e .
  echo "Setup complete. Activate with: source .venv/bin/activate"
}

cmd_audit() {
  cd "$ROOT"; source .venv/bin/activate; ensure_project; ensure_location
  local PROJECT="${GOOGLE_CLOUD_PROJECT}"
  local DAYS=30
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --project) PROJECT="$2"; shift 2;;
      --days) DAYS="$2"; shift 2;;
      *) echo "Unknown arg: $1"; usage; exit 1;;
    esac
  done
  python -m adk_bq_audit.cli --project "$PROJECT" --days "$DAYS" --locations US --limit 5000 --topn 100 --outfile ./bq_job_stats_today.csv | cat
  echo "CSV -> $ROOT/bq_job_stats_today.csv"
}

cmd_forensic_top1() {
  cd "$ROOT"; source .venv/bin/activate; ensure_project; ensure_location
  py <<'PY'
import os, csv
from adk_app.tools.forensic_agent_tool import forensic_agent_tool
from adk_app.tools.query_analysis_tool import query_analysis_tool
from adk_app.schemas import ForensicInput, QueryAnalysisInput
proj=os.environ['GOOGLE_CLOUD_PROJECT']
csv_path='./bq_job_stats_today.csv'
if not os.path.exists(csv_path):
  os.system(f"python -m adk_bq_audit.cli --project '{proj}' --days 30 --locations US --limit 5000 --topn 100 --outfile {csv_path}")
with open(csv_path) as f:
  rows=list(csv.DictReader(f))
rows=[r for r in rows if r.get('query')]
rows.sort(key=lambda r:(int(r.get('total_bytes_billed') or 0), int(r.get('total_slot_ms') or 0)), reverse=True)
job=rows[0]; job_id=job['job_id']; sql=job['query']
qa=query_analysis_tool(QueryAnalysisInput(sql=sql, project=proj, job_id=job_id))
md=qa.metadata_file
out='./analysis_out/forensic_report.md'
res=forensic_agent_tool(ForensicInput(md_path=md, output_path=out))
print('Report ->', os.path.abspath(res.report_path))
print('\nPREVIEW:\n', res.text_preview)
PY
}

cmd_forensic_top10() {
  cd "$ROOT"; source .venv/bin/activate; ensure_project; ensure_location
  py <<'PY'
import os, csv, re, shutil
from google.cloud import bigquery
from adk_app.tools.query_analysis_tool import query_analysis_tool
from adk_app.schemas import QueryAnalysisInput
from adk_app.tools.forensic_agent_tool import forensic_agent_tool
from adk_app.schemas import ForensicInput
proj=os.environ['GOOGLE_CLOUD_PROJECT']
csv_path='./bq_job_stats_today.csv'
if not os.path.exists(csv_path):
  os.system(f"python -m adk_bq_audit.cli --project '{proj}' --days 30 --locations US --limit 5000 --topn 100 --outfile {csv_path}")
with open(csv_path) as f:
  rows=list(csv.DictReader(f))
rows=[r for r in rows if r.get('query')]
rows.sort(key=lambda r:(int(r.get('total_bytes_billed') or 0), int(r.get('total_slot_ms') or 0)), reverse=True)
rows=rows[:10]
os.makedirs('./analysis_out', exist_ok=True)
bundle='./analysis_out/forensic_report_top10.md'
paths=[]
for r in rows:
  job_id=r['job_id']; sql=r['query']
  qa=query_analysis_tool(QueryAnalysisInput(sql=sql, project=proj, job_id=job_id))
  fr=forensic_agent_tool(ForensicInput(md_path=qa.metadata_file, output_path=f'./analysis_out/forensic_report_{job_id}.md'))
  paths.append((job_id, fr.report_path))
with open(bundle,'w') as out:
  out.write('---\nformat: bq_forensic_bundle\nversion: 1\nproject: '+proj+'\n---\n\n')
  out.write('# Forensic Reports: Top 10 Queries\n')
  for i,(job_id,path) in enumerate(paths, start=1):
    out.write(f"\n\n## Job {i}: {job_id}\n\n")
    try:
      with open(path) as f: out.writelines(f.readlines())
    except Exception: out.write('_Report missing._\n')
print('Bundle ->', os.path.abspath(bundle))
PY
}

cmd_inspector() {
  cd "$ROOT"; source .venv/bin/activate; ensure_project; ensure_location
  py <<'PY'
import os
from adk_app.tools.all_job_inspector_tool import all_job_inspector_tool
from adk_app.schemas import AllJobsInspectorInput
proj=os.environ['GOOGLE_CLOUD_PROJECT']
res=all_job_inspector_tool(AllJobsInspectorInput(project=proj, region='US', days=30, limit=200, output_path='./analysis_out/all_job_inspector.md'))
print('Report ->', res.report_path)
print('\nPREVIEW:\n', res.text_preview)
PY
}

cmd_all() {
  cd "$ROOT"; source .venv/bin/activate; ensure_project; ensure_location
  PROJECT="${GOOGLE_CLOUD_PROJECT}"
  echo "[1/6] Audit jobs..."
  python -m adk_bq_audit.cli --project "$PROJECT" --days 30 --locations US --limit 5000 --topn 100 --outfile ./bq_job_stats_today.csv | cat
  echo "[2/6] Analysis PDF..."
  py <<'PY'
import os
from adk_app.tools.analyze_tool import analyze_tool
from adk_app.schemas import AnalyzeInput
res=analyze_tool(AnalyzeInput(csv_path='./bq_job_stats_today.csv', output_dir='./analysis_out'))
print('PDF(s):', res.plots)
PY
  echo "[3/6] Top1 schema report..."
  py <<'PY'
import os, csv
from adk_app.tools.query_analysis_tool import query_analysis_tool
from adk_app.schemas import QueryAnalysisInput
proj=os.environ['GOOGLE_CLOUD_PROJECT']
rows=list(csv.DictReader(open('./bq_job_stats_today.csv')))
rows=[r for r in rows if r.get('query')]
rows.sort(key=lambda r:(int(r.get('total_bytes_billed') or 0), int(r.get('total_slot_ms') or 0)), reverse=True)
job=rows[0]
sql=job['query']; job_id=job['job_id']
qa=query_analysis_tool(QueryAnalysisInput(sql=sql, project=proj, job_id=job_id))
print('Schema report ->', qa.metadata_file)
PY
  echo "[4/6] All-job inspector..."
  py <<'PY'
import os
from adk_app.tools.all_job_inspector_tool import all_job_inspector_tool
from adk_app.schemas import AllJobsInspectorInput
proj=os.environ['GOOGLE_CLOUD_PROJECT']
res=all_job_inspector_tool(AllJobsInspectorInput(project=proj, region='US', days=30, limit=200, output_path='./analysis_out/all_job_inspector.md'))
print('Inspector ->', res.report_path)
PY
  echo "[5/6] Top10 forensic bundle..."
  cmd_forensic_top10
  echo "[6/6] Done. Outputs in ./analysis_out"
}

case "${1:-}" in
  setup) shift; cmd_setup "$@" ;;
  audit) shift; cmd_audit "$@" ;;
  forensic-top1) shift; cmd_forensic_top1 "$@" ;;
  forensic-top10) shift; cmd_forensic_top10 "$@" ;;
  inspector) shift; cmd_inspector "$@" ;;
  all) shift; cmd_all "$@" ;;
  *) usage; exit 1 ;;
esac


