#!/usr/bin/env python3
import argparse
import csv
import os
import sys
from dataclasses import dataclass
from typing import List, Optional

import pytz  # noqa: F401  # kept for potential timezone handling
from google.cloud import bigquery
from .optimizer import query_optimizer_tool


US_REGIONAL_INFO_SCHEMA = "`region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT"
EU_REGIONAL_INFO_SCHEMA = "`region-eu`.INFORMATION_SCHEMA.JOBS_BY_PROJECT"


@dataclass
class JobStat:
    location: str
    job_id: str
    user_email: str
    creation_time: str
    end_time: str
    total_bytes_processed: int
    total_bytes_billed: int
    total_slot_ms: int
    statement_type: Optional[str]
    query: Optional[str]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Authenticate with Google and analyze recent BigQuery jobs, reporting the most expensive query and exporting stats to CSV.",
        add_help=True,
    )
    parser.add_argument("--project", required=True, help="GCP project ID")
    parser.add_argument("--days", type=int, default=90, help="Lookback window in days (default: 90)")
    parser.add_argument(
        "--outfile",
        default="bq_job_stats.csv",
        help="Path to write the CSV with job stats (default: bq_job_stats.csv)",
    )
    parser.add_argument(
        "--locations",
        default="US,EU",
        help="Comma-separated BigQuery multi-regions to scan (choices include US,EU)",
    )
    parser.add_argument("--limit", type=int, default=1000, help="Max jobs per location (default: 1000)")
    parser.add_argument(
        "--topn",
        type=int,
        default=5,
        help="Number of most expensive jobs to print (default: 5)",
    )
    return parser.parse_args()


def _jobs_query_sql(days: int, limit: int, schema: str) -> str:
    return (
        "SELECT job_id, user_email, creation_time, end_time, total_bytes_processed, "
        "total_bytes_billed, total_slot_ms, statement_type, query "
        f"FROM {schema} "
        "WHERE job_type = \"QUERY\" "
        f"AND creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY) "
        f"ORDER BY creation_time DESC LIMIT {limit}"
    )


def _pick_schema_for_location(location: str) -> str:
    loc = location.upper()
    if loc == "US":
        return US_REGIONAL_INFO_SCHEMA
    if loc == "EU":
        return EU_REGIONAL_INFO_SCHEMA
    raise ValueError(f"Unsupported location '{location}'. Use US or EU.")


def _fetch_jobs(client: bigquery.Client, location: str, days: int, limit: int) -> List[JobStat]:
    schema = _pick_schema_for_location(location)
    sql = _jobs_query_sql(days=days, limit=limit, schema=schema)
    job = client.query(sql, location=location)
    rows = list(job.result())
    stats: List[JobStat] = []
    for r in rows:
        stats.append(
            JobStat(
                location=location,
                job_id=str(r["job_id"]),
                user_email=str(r["user_email"]) if r["user_email"] is not None else "",
                creation_time=str(r["creation_time"]) if r["creation_time"] is not None else "",
                end_time=str(r["end_time"]) if r["end_time"] is not None else "",
                total_bytes_processed=int(r["total_bytes_processed"]) if r["total_bytes_processed"] is not None else 0,
                total_bytes_billed=int(r["total_bytes_billed"]) if r["total_bytes_billed"] is not None else 0,
                total_slot_ms=int(r["total_slot_ms"]) if r["total_slot_ms"] is not None else 0,
                statement_type=str(r["statement_type"]) if r["statement_type"] is not None else None,
                query=str(r["query"]) if r["query"] is not None else None,
            )
        )
    return stats


def _most_expensive(jobs: List[JobStat]) -> Optional[JobStat]:
    if not jobs:
        return None
    return max(jobs, key=lambda j: (j.total_bytes_billed, j.total_slot_ms))


def _top_n_most_expensive(jobs: List[JobStat], n: int) -> List[JobStat]:
    """Return the top N jobs by billed bytes (tie-breaker: total_slot_ms)."""
    if n <= 0:
        return []
    return sorted(
        jobs,
        key=lambda j: (j.total_bytes_billed, j.total_slot_ms),
        reverse=True,
    )[:n]




def _write_csv(path: str, jobs: List[JobStat]) -> None:
    fieldnames = [
        "location",
        "job_id",
        "user_email",
        "creation_time",
        "end_time",
        "total_bytes_processed",
        "total_bytes_billed",
        "total_slot_ms",
        "statement_type",
        "query",
    ]
    with open(path, "w", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames)
        writer.writeheader()
        for j in jobs:
            writer.writerow(
                {
                    "location": j.location,
                    "job_id": j.job_id,
                    "user_email": j.user_email,
                    "creation_time": j.creation_time,
                    "end_time": j.end_time,
                    "total_bytes_processed": j.total_bytes_processed,
                    "total_bytes_billed": j.total_bytes_billed,
                    "total_slot_ms": j.total_slot_ms,
                    "statement_type": j.statement_type or "",
                    "query": j.query or "",
                }
            )


def main() -> int:
    args = _parse_args()
    client = bigquery.Client(project=args.project)
    locations = [s.strip() for s in args.locations.split(",") if s.strip()]

    all_jobs: List[JobStat] = []
    for loc in locations:
        try:
            all_jobs.extend(_fetch_jobs(client, loc, args.days, args.limit))
        except Exception as exc:  # noqa: BLE001
            print(f"Warning: failed fetching jobs from {loc}: {exc}", file=sys.stderr)

    if not all_jobs:
        print("No jobs found in the specified window/locations.")
        return 0

    _write_csv(args.outfile, all_jobs)

    top = _most_expensive(all_jobs)
    if top is None:
        print("No query jobs found.")
        return 0

    print("Most expensive query in the window:")
    print(f"  Location: {top.location}")
    print(f"  Job ID:   {top.job_id}")
    print(f"  User:     {top.user_email}")
    print(f"  Created:  {top.creation_time}")
    print(f"  Billed:   {top.total_bytes_billed:,} bytes")
    print(f"  Processed:{top.total_bytes_processed:,} bytes")
    print(f"  Slot ms:  {top.total_slot_ms:,}")
    print(f"  Type:     {top.statement_type}")
    print("  Query:")
    print(top.query or "")

    # Print top-N most expensive queries as requested
    topn_jobs = _top_n_most_expensive(all_jobs, args.topn)
    if topn_jobs:
        print(f"\nTop {len(topn_jobs)} most expensive queries:")
        for i, j in enumerate(topn_jobs, start=1):
            print(f"[{i}] Location: {j.location}")
            print(f"    Job ID:   {j.job_id}")
            print(f"    User:     {j.user_email}")
            print(f"    Billed:   {j.total_bytes_billed:,} bytes")
            print(f"    Slot ms:  {j.total_slot_ms:,}")
            print(f"    Type:     {j.statement_type}")
            print("    Query:")
            print(j.query or "")

    print(f"\nWrote job CSV to: {os.path.abspath(args.outfile)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())


