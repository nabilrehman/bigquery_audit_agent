from typing import List
from google.cloud import bigquery
from ..schemas import AuditInput, AuditOutput, JobStat


US_REGIONAL_INFO_SCHEMA = "`region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT"
EU_REGIONAL_INFO_SCHEMA = "`region-eu`.INFORMATION_SCHEMA.JOBS_BY_PROJECT"


def _pick_schema_for_location(location: str) -> str:
    loc = location.upper()
    if loc == "US":
        return US_REGIONAL_INFO_SCHEMA
    if loc == "EU":
        return EU_REGIONAL_INFO_SCHEMA
    raise ValueError(f"Unsupported location '{location}'. Use US or EU.")


def _jobs_query_sql(days: int, limit: int, schema: str) -> str:
    return (
        "SELECT job_id, user_email, creation_time, end_time, total_bytes_processed, "
        "total_bytes_billed, total_slot_ms, statement_type, query "
        f"FROM {schema} "
        "WHERE job_type = \"QUERY\" "
        f"AND creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY) "
        f"ORDER BY creation_time DESC LIMIT {limit}"
    )


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


def _top_n_most_expensive(jobs: List[JobStat], n: int) -> List[JobStat]:
    return sorted(jobs, key=lambda j: (j.total_bytes_billed, j.total_slot_ms), reverse=True)[:n]


def bq_audit_tool(params: AuditInput) -> AuditOutput:
    client = bigquery.Client(project=params.project)
    all_jobs: List[JobStat] = []
    for loc in params.locations:
        try:
            all_jobs.extend(_fetch_jobs(client, loc, params.days, params.limit))
        except Exception as exc:
            # Swallow per-location failures but continue others
            print(f"Warning: failed fetching jobs from {loc}: {exc}")

    # Write CSV
    import csv, os
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
    with open(params.outfile, "w", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames)
        writer.writeheader()
        for j in all_jobs:
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

    top = _top_n_most_expensive(all_jobs, params.topn)
    return AuditOutput(csv_path=os.path.abspath(params.outfile), jobs=all_jobs, top=top)


