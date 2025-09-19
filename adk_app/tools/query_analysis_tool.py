import json
import re
import os
from typing import List, Optional
import datetime

from google.adk import Agent
from google.adk.runners import Runner
from google.adk.sessions.in_memory_session_service import InMemorySessionService
from google.genai.types import Content, Part
from google.cloud import bigquery

from ..schemas import QueryAnalysisInput, QueryAnalysisOutput, ExtractedTable
def _run_in_dataset(
    client: bigquery.Client,
    project: str,
    dataset: str,
    sql: str,
    params: Optional[List[bigquery.ScalarQueryParameter]] = None,
):
    # Resolve dataset location to avoid regional mismatches on INFORMATION_SCHEMA views
    try:
        ds = client.get_dataset(f"{project}.{dataset}")
        location = getattr(ds, "location", None)
    except Exception:
        location = None
    cfg = bigquery.QueryJobConfig(
        default_dataset=bigquery.DatasetReference(project, dataset),
        query_parameters=params or [],
    )
    return client.query(sql, job_config=cfg, location=location)



EXTRACT_SYSTEM_PROMPT = (
    "You are a SQL analysis assistant. Extract all fully qualified or partially qualified table references "
    "from the user's BigQuery SQL. Return a JSON array where each element has 'project', 'dataset', 'table'. "
    "If project is not specified, leave it empty. Do not include temporary CTE names."
)


def _llm_extract_tables(sql: str) -> List[ExtractedTable]:
    agent = Agent(
        name="table_extractor",
        model="gemini-2.5-flash-lite",
        instruction=EXTRACT_SYSTEM_PROMPT,
        tools=[],
    )
    session_service = InMemorySessionService()
    app_name = "query_analysis"
    user_id = "local_user"
    session_id = "extract_session"

    async def _run() -> str:
        await session_service.create_session(app_name=app_name, user_id=user_id, session_id=session_id)
        runner = Runner(app_name=app_name, agent=agent, session_service=session_service)
        msg = Content(role="user", parts=[Part.from_text(text=sql)])
        text_chunks: List[str] = []
        async for ev in runner.run_async(user_id=user_id, session_id=session_id, new_message=msg):
            txt = getattr(ev, "text", None)
            if txt:
                text_chunks.append(txt)
        return "\n".join(text_chunks).strip()

    import asyncio

    out = asyncio.run(_run())
    tables: List[ExtractedTable] = []
    try:
        data = json.loads(out)
        for item in data if isinstance(data, list) else []:
            proj = str(item.get("project", ""))
            dset = str(item.get("dataset", ""))
            tbl = str(item.get("table", ""))
            if dset and tbl:
                tables.append(ExtractedTable(project=proj, dataset=dset, table=tbl))
    except Exception:
        # Heuristic: none parsed
        pass
    return tables


def _resolve_project(maybe_project: str, default_project: str) -> str:
    return maybe_project or default_project


_BACKTICK_FQN = re.compile(r"`(?P<project>[\w\-]+)`\.`(?P<dataset>[\w\$]+)`\.`(?P<table>[\w\$]+)`|`(?P<project2>[\w\-]+)\.(?P<dataset2>[\w\$]+)\.(?P<table2>[\w\$]+)`")
_PLAIN_FQN = re.compile(r"(?P<project>[\w\-]+)\.(?P<dataset>[\w\$]+)\.(?P<table>[\w\$]+)")
_PLAIN_DSET = re.compile(r"(?P<dataset>[\w\$]+)\.(?P<table>[\w\$]+)")


def _regex_extract_tables(sql: str, default_project: str) -> List[ExtractedTable]:
    found: List[ExtractedTable] = []
    seen = set()
    for m in _BACKTICK_FQN.finditer(sql):
        proj = m.group("project") or m.group("project2")
        dset = m.group("dataset") or m.group("dataset2")
        tbl = m.group("table") or m.group("table2")
        key = (proj, dset, tbl)
        if key not in seen:
            seen.add(key)
            found.append(ExtractedTable(project=proj, dataset=dset, table=tbl))
    # Plain fully-qualified
    for m in _PLAIN_FQN.finditer(sql):
        proj, dset, tbl = m.group("project"), m.group("dataset"), m.group("table")
        key = (proj, dset, tbl)
        if key not in seen:
            seen.add(key)
            found.append(ExtractedTable(project=proj, dataset=dset, table=tbl))
    # dataset.table -> assume default project
    for m in _PLAIN_DSET.finditer(sql):
        dset, tbl = m.group("dataset"), m.group("table")
        key = (default_project, dset, tbl)
        if key not in seen:
            seen.add(key)
            found.append(ExtractedTable(project=default_project, dataset=dset, table=tbl))
    return found

def _info_schema_for_table(client: bigquery.Client, project: str, dataset: str, table: str) -> str:
    # Table metadata via INFORMATION_SCHEMA only (size_bytes, creation_time, table_type)
    # Use identifier form `{project}.{dataset}`.INFORMATION_SCHEMA.VIEW to avoid parser confusion.
    lines = [f"Table: {project}.{dataset}.{table}"]

    sql_basic = """
    SELECT table_type, creation_time
    FROM `INFORMATION_SCHEMA.TABLES`
    WHERE table_name = @table
    """
    job_b = _run_in_dataset(
        client,
        project,
        dataset,
        sql_basic,
        [bigquery.ScalarQueryParameter("table", "STRING", table)],
    )
    rows_b = list(job_b.result())
    if rows_b:
        b = rows_b[0]
        if b.get('table_type') is not None:
            lines.append(f"  table_type:     {b['table_type']}")
        lines.append(f"  created:        {b['creation_time']}")

    # Storage breakdown (logical/physical bytes)
    sqls = """
    SELECT total_physical_bytes, total_logical_bytes
    FROM `INFORMATION_SCHEMA.TABLE_STORAGE`
    WHERE table_name = @table
    """
    job_s = _run_in_dataset(
        client,
        project,
        dataset,
        sqls,
        [bigquery.ScalarQueryParameter("table", "STRING", table)],
    )
    stor = list(job_s.result())
    if stor:
        s = stor[0]
        lines.append(f"  physical_bytes: {int(s['total_physical_bytes']) if s['total_physical_bytes'] is not None else 0}")
        lines.append(f"  logical_bytes:  {int(s['total_logical_bytes']) if s['total_logical_bytes'] is not None else 0}")

    # Partitioning/clustering info from INFORMATION_SCHEMA
    sql2 = """
    SELECT partition_id, total_logical_bytes, last_modified_time
    FROM `INFORMATION_SCHEMA.PARTITIONS`
    WHERE table_name = @table
    """
    job2 = _run_in_dataset(
        client,
        project,
        dataset,
        sql2,
        [bigquery.ScalarQueryParameter("table", "STRING", table)],
    )
    parts = list(job2.result())
    if parts:
        lines.append(f"  partitions: {len(parts)} (sample of first 3):")
        for p in parts[:3]:
            lines.append(f"    partition_id={p.get('partition_id')}, total_logical_bytes={p.get('total_logical_bytes')}")
    else:
        lines.append("  partitions: none")

    # Columns info
    sql3 = """
    SELECT COUNT(1) AS col_count FROM `INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = @table
    """
    job3 = _run_in_dataset(
        client,
        project,
        dataset,
        sql3,
        [bigquery.ScalarQueryParameter("table", "STRING", table)],
    )
    cols = list(job3.result())
    if cols:
        lines.append(f"  columns: {int(cols[0]['col_count'])}")

    # Clustering info
    sql4 = """
    SELECT clustering_ordinal_position, column_name
    FROM `INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = @table AND clustering_ordinal_position IS NOT NULL
    ORDER BY clustering_ordinal_position
    """
    job4 = _run_in_dataset(
        client,
        project,
        dataset,
        sql4,
        [bigquery.ScalarQueryParameter("table", "STRING", table)],
    )
    clus = list(job4.result())
    if clus:
        names = ", ".join([str(c["column_name"]) for c in clus])
        lines.append(f"  clustering: {names}")
    else:
        lines.append("  clustering: none")

    return "\n".join(lines)


def _info_schema_for_dataset(client: bigquery.Client, project: str, dataset: str) -> str:
    # Dataset table count and sizes via INFORMATION_SCHEMA.TABLES (dataset-scoped)
    sql = """
    SELECT COUNT(*) AS table_count
    FROM `INFORMATION_SCHEMA.TABLES`
    """
    job = _run_in_dataset(client, project, dataset, sql)
    rows = list(job.result())
    lines = [f"Dataset: {project}.{dataset}"]
    if rows:
        r = rows[0]
        lines.append(f"  tables:        {int(r['table_count']) if r['table_count'] is not None else 0}")

    # Storage totals
    sqls = """
    SELECT SUM(total_logical_bytes) AS total_logical_bytes,
           SUM(total_physical_bytes) AS total_physical_bytes
    FROM `INFORMATION_SCHEMA.TABLE_STORAGE`
    """
    jobs = _run_in_dataset(client, project, dataset, sqls)
    rows2 = list(jobs.result())
    if rows2:
        r2 = rows2[0]
        lines.append(f"  logical_bytes:  {int(r2['total_logical_bytes']) if r2['total_logical_bytes'] is not None else 0}")
        lines.append(f"  physical_bytes: {int(r2['total_physical_bytes']) if r2['total_physical_bytes'] is not None else 0}")
    return "\n".join(lines)


def _dataset_api_totals(client: bigquery.Client, project: str, dataset: str) -> str:
    total_bytes = 0
    table_count = 0
    try:
        for tbl in client.list_tables(f"{project}.{dataset}"):
            try:
                t = client.get_table(tbl.reference)
                total_bytes += int(getattr(t, "num_bytes", 0) or 0)
                table_count += 1
            except Exception:
                continue
    except Exception as e:
        return f"Dataset API totals error: {e}"
    return f"Dataset API totals: tables={table_count}, sum_num_bytes={total_bytes}"


def _info_schema_table_options(client: bigquery.Client, project: str, dataset: str, table: str) -> List[str]:
    # Partitioning, clustering, require_partition_filter, expiration
    sql = """
    SELECT option_name, option_type, option_value
    FROM `INFORMATION_SCHEMA.TABLE_OPTIONS`
    WHERE table_name = @table
    ORDER BY option_name
    """
    job = _run_in_dataset(
        client,
        project,
        dataset,
        sql,
        [bigquery.ScalarQueryParameter("table", "STRING", table)],
    )
    opts = list(job.result())
    lines: List[str] = []
    if opts:
        lines.append("  options:")
        for o in opts:
            lines.append(f"    {o['option_name']}: {o['option_value']}")
    return lines


def _info_schema_columns_detailed(client: bigquery.Client, project: str, dataset: str, table: str) -> List[str]:
    sql = """
    SELECT ordinal_position, column_name, data_type, is_nullable, is_hidden, is_generated, is_system_defined
    FROM `INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = @table
    ORDER BY ordinal_position
    """
    job = _run_in_dataset(
        client,
        project,
        dataset,
        sql,
        [bigquery.ScalarQueryParameter("table", "STRING", table)],
    )
    rows = list(job.result())
    lines: List[str] = []
    if rows:
        lines.append("  columns_detailed:")
        for r in rows[:100]:  # cap to first 100 for readability
            lines.append(
                f"    {int(r['ordinal_position'])}. {r['column_name']}: {r['data_type']}, nullable={r['is_nullable']}, hidden={r['is_hidden']}, generated={r['is_generated']}, system={r['is_system_defined']}"
            )
        if len(rows) > 100:
            lines.append(f"    ... and {len(rows) - 100} more columns")
    return lines


def _info_schema_column_field_paths(client: bigquery.Client, project: str, dataset: str, table: str) -> List[str]:
    # Flattened path listing for nested RECORD schemas
    sql = """
    SELECT field_path, data_type
    FROM `INFORMATION_SCHEMA.COLUMN_FIELD_PATHS`
    WHERE table_name = @table
    ORDER BY field_path
    """
    job = _run_in_dataset(
        client,
        project,
        dataset,
        sql,
        [bigquery.ScalarQueryParameter("table", "STRING", table)],
    )
    rows = list(job.result())
    lines: List[str] = []
    if rows:
        lines.append("  column_field_paths:")
        for r in rows[:200]:
            lines.append(f"    {r['field_path']}: {r['data_type']}")
        if len(rows) > 200:
            lines.append(f"    ... and {len(rows) - 200} more field paths")
    return lines


def _info_schema_views_info(client: bigquery.Client, project: str, dataset: str, table: str) -> List[str]:
    # For views: definition (snippet)
    sql = """
    SELECT view_definition
    FROM `INFORMATION_SCHEMA.VIEWS`
    WHERE table_name = @table
    """
    job = _run_in_dataset(
        client,
        project,
        dataset,
        sql,
        [bigquery.ScalarQueryParameter("table", "STRING", table)],
    )
    rows = list(job.result())
    lines: List[str] = []
    if rows:
        v = rows[0]
        lines.append("  view:")
        # Truncate definition for readability
        defn = str(v.get('view_definition') or '')
        if defn:
            snippet = defn if len(defn) <= 1000 else defn[:1000] + " ... (truncated)"
            lines.append("    view_definition_snippet:")
            for ln in snippet.splitlines()[:20]:
                lines.append("      " + ln)
    return lines


def _info_schema_mviews_info(client: bigquery.Client, project: str, dataset: str, table: str) -> List[str]:
    # Materialized view metadata (if applicable)
    sql = """
    SELECT *
    FROM `INFORMATION_SCHEMA.MATERIALIZED_VIEWS`
    WHERE table_name = @table
    """
    job = _run_in_dataset(
        client,
        project,
        dataset,
        sql,
        [bigquery.ScalarQueryParameter("table", "STRING", table)],
    )
    rows = list(job.result())
    lines: List[str] = []
    if rows:
        mv = rows[0]
        lines.append("  materialized_view:")
        for k in mv.keys():
            val = mv.get(k)
            # Avoid dumping huge strings
            if isinstance(val, str) and len(val) > 500:
                val = val[:500] + " ... (truncated)"
            lines.append(f"    {k}: {val}")
    return lines


def _dataset_exists(client: bigquery.Client, project: str, dataset: str) -> bool:
    try:
        client.get_dataset(f"{project}.{dataset}")
        return True
    except Exception:
        return False


def _table_api_details(client: bigquery.Client, project: str, dataset: str, table: str) -> List[str]:
    lines: List[str] = []
    try:
        t = client.get_table(f"{project}.{dataset}.{table}")
        lines.append("  api_details:")
        lines.append(f"    num_rows: {t.num_rows}")
        lines.append(f"    num_bytes: {t.num_bytes}")
        if getattr(t, 'time_partitioning', None):
            tp = t.time_partitioning
            lines.append("    time_partitioning:")
            lines.append(f"      type: {getattr(tp, 'type_', None) or getattr(tp, 'type', None)}")
            lines.append(f"      field: {getattr(tp, 'field', None)}")
            lines.append(f"      expiration_ms: {getattr(tp, 'expiration_ms', None)}")
            lines.append(f"      require_partition_filter: {getattr(tp, 'require_partition_filter', None)}")
        if getattr(t, 'range_partitioning', None):
            rp = t.range_partitioning
            lines.append("    range_partitioning:")
            lines.append(f"      field: {getattr(rp, 'field', None)}")
            r = getattr(rp, 'range_', None) or getattr(rp, 'range', None)
            if r:
                lines.append(f"      start: {getattr(r, 'start', None)} end: {getattr(r, 'end', None)} interval: {getattr(r, 'interval', None)}")
        if getattr(t, 'clustering_fields', None):
            lines.append(f"    clustering_fields: {t.clustering_fields}")
        if getattr(t, 'labels', None):
            lines.append(f"    labels: {t.labels}")
        if getattr(t, 'encryption_configuration', None):
            kms = getattr(t.encryption_configuration, 'kms_key_name', None)
            lines.append(f"    encryption_kms_key: {kms}")
        if getattr(t, 'snapshot_definition', None):
            sd = t.snapshot_definition
            lines.append("    snapshot_definition:")
            lines.append(f"      base_table: {getattr(sd, 'base_table_reference', None)}")
            lines.append(f"      snapshot_time: {getattr(sd, 'snapshot_time', None)}")
    except Exception as e:
        lines.append(f"  api_details_error: {e}")
    return lines


def _job_diagnostics_compact(client: bigquery.Client, project: str, job_id: str) -> List[str]:
    # Compact diagnostic query to avoid large payloads and scans
    sql = f"""
    SELECT
      j.job_id,
      j.user_email,
      j.job_type,
      j.state,
      j.error_result,
      j.start_time,
      j.end_time,
      j.total_slot_ms,
      j.total_bytes_billed,
      j.total_bytes_processed,
      j.cache_hit,
      j.statement_type,
      SUBSTR(j.query, 1, 1000) AS query_snippet,
      -- limited arrays as structs
      ARRAY(SELECT AS STRUCT s.name, s.slot_ms, s.records_read, s.records_written FROM UNNEST(j.job_stages) AS s LIMIT 3) AS job_stages_limited,
      ARRAY(SELECT AS STRUCT t.elapsed_ms, t.total_slot_ms, t.active_units FROM UNNEST(j.timeline) AS t LIMIT 3) AS timeline_limited,
      ARRAY(SELECT AS STRUCT r.project_id, r.dataset_id, r.table_id FROM UNNEST(j.referenced_tables) AS r LIMIT 3) AS referenced_tables_limited
    FROM `region-us`.INFORMATION_SCHEMA.JOBS AS j
    WHERE j.job_id = @job_id
    LIMIT 1
    """
    job = client.query(sql, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("job_id", "STRING", job_id)]
    ))
    rows = list(job.result())
    lines: List[str] = []
    if rows:
        r = rows[0]
        lines.append("## Job Diagnostics (compact)")
        for k in [
            "job_id","user_email","job_type","state","start_time","end_time",
            "total_slot_ms","total_bytes_billed","total_bytes_processed","cache_hit","statement_type","query_snippet",
        ]:
            lines.append(f"{k}: {r.get(k)}")
        lines.append("job_stages_limited:")
        for s in (r.get("job_stages_limited") or [])[:3]:
            lines.append(f"  - name: {s.get('name')}, slot_ms: {s.get('slot_ms')}, rr: {s.get('records_read')}, rw: {s.get('records_written')}")
        lines.append("timeline_limited:")
        for t in (r.get("timeline_limited") or [])[:3]:
            lines.append(f"  - elapsed_ms: {t.get('elapsed_ms')}, total_slot_ms: {t.get('total_slot_ms')}, active_units: {t.get('active_units')}")
        lines.append("referenced_tables_limited:")
        for rt in (r.get("referenced_tables_limited") or [])[:3]:
            lines.append(f"  - {rt.get('project_id')}.{rt.get('dataset_id')}.{rt.get('table_id')}")
    return lines


def query_analysis_tool(params: QueryAnalysisInput) -> QueryAnalysisOutput:
    # Step 1: Table extraction via LLM
    extracted = _llm_extract_tables(params.sql)
    if not extracted:
        extracted = _regex_extract_tables(params.sql, params.project)

    # Step 2: INFORMATION_SCHEMA lookups
    client = bigquery.Client(project=params.project)
    lines: List[str] = []
    seen_datasets = set()
    resolved: List[ExtractedTable] = []
    notes: List[str] = []
    compact_mode = bool(os.environ.get("ADK_COMPACT_V2"))

    for t in extracted:
        proj = _resolve_project(t.project, params.project)
        # Local-only enforcement
        if proj != params.project:
            notes.append(f"Skipping external table: {proj}.{t.dataset}.{t.table}")
            continue
        if not _dataset_exists(client, proj, t.dataset):
            notes.append(f"Skipping table: Dataset {proj}.{t.dataset} not found in current project.")
            continue

        resolved.append(ExtractedTable(project=proj, dataset=t.dataset, table=t.table))

        # Table core
        try:
            lines.append(_info_schema_for_table(client, proj, t.dataset, t.table))
        except Exception as e:
            notes.append(f"Error TABLES/TABLE_STORAGE/PARTITIONS/COLUMNS for {proj}.{t.dataset}.{t.table}: {e}")

        # Options
        try:
            lines.extend(_info_schema_table_options(client, proj, t.dataset, t.table))
        except Exception as e:
            notes.append(f"Error TABLE_OPTIONS for {proj}.{t.dataset}.{t.table}: {e}")

        if not compact_mode:
            try:
                lines.extend(_info_schema_columns_detailed(client, proj, t.dataset, t.table))
            except Exception as e:
                notes.append(f"Error detailed COLUMNS for {proj}.{t.dataset}.{t.table}: {e}")
            try:
                lines.extend(_info_schema_column_field_paths(client, proj, t.dataset, t.table))
            except Exception as e:
                notes.append(f"Error COLUMN_FIELD_PATHS for {proj}.{t.dataset}.{t.table}: {e}")

        # Views / Materialized views
        try:
            lines.extend(_info_schema_views_info(client, proj, t.dataset, t.table))
        except Exception as e:
            notes.append(f"Error VIEWS for {proj}.{t.dataset}.{t.table}: {e}")
        try:
            lines.extend(_info_schema_mviews_info(client, proj, t.dataset, t.table))
        except Exception as e:
            notes.append(f"Error MATERIALIZED_VIEWS for {proj}.{t.dataset}.{t.table}: {e}")

        # API enrichments
        try:
            lines.extend(_table_api_details(client, proj, t.dataset, t.table))
        except Exception as e:
            notes.append(f"Error API details for {proj}.{t.dataset}.{t.table}: {e}")

        key = (proj, t.dataset)
        if key not in seen_datasets:
            seen_datasets.add(key)
            try:
                lines.append(_info_schema_for_dataset(client, proj, t.dataset))
            except Exception as e:
                notes.append(f"Error dataset totals for {proj}.{t.dataset}: {e}")
            # Add API totals fallback so report still has dataset size signal
            try:
                lines.append(_dataset_api_totals(client, proj, t.dataset))
            except Exception as e:
                notes.append(f"Error dataset API totals for {proj}.{t.dataset}: {e}")

    # Optional: job diagnostics for a specific job_id from regional INFORMATION_SCHEMA.JOBS
    if params.job_id:
        try:
            # Compact diagnostics only in v2 mode
            if compact_mode:
                lines.extend(_job_diagnostics_compact(client, params.project, params.job_id))
            else:
                notes.append("Job diagnostics skipped (non-compact mode disabled to reduce size).")
        except Exception as e:
            notes.append(f"Job diagnostics error: {e}")

    # Write Markdown report for downstream agent consumption
    out_dir = os.path.abspath("./analysis_out")
    os.makedirs(out_dir, exist_ok=True)
    meta_path = os.path.join(out_dir, "schema_report.md")
    header = [
        "---",
        "format: bq_schema_report",
        "version: 1",
        f"project: {params.project}",
        f"generated_utc: {datetime.datetime.utcnow().isoformat()}Z",
        "---",
        "",
        "# BigQuery Schema Metadata",
    ]
    body = ("\n\n".join(lines) if lines else "No metadata found.")
    notes_section = ("\n\n## Notes\n" + "\n".join(notes)) if notes else ""
    with open(meta_path, "w") as fp:
        fp.write("\n".join(header) + "\n\n" + body + notes_section)

    return QueryAnalysisOutput(tables=resolved, metadata_file=meta_path, notes=("\n".join(notes) if notes else ("" if extracted else "No tables extracted; check SQL.")))


