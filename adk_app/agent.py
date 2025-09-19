from google.adk import Agent
from google.adk.tools.function_tool import FunctionTool
from google.adk.runners import Runner
from google.adk.sessions.in_memory_session_service import InMemorySessionService
from google.genai.types import Content, Part
import os
import asyncio
from .schemas import AuditInput, OptimizeInput, AnalyzeInput, QueryAnalysisInput
from google.cloud import bigquery
from .tools.bq_audit_tool import bq_audit_tool
from .tools.query_optimizer_tool import query_optimizer_tool
from .tools.analyze_tool import analyze_tool
from .tools.query_analysis_tool import query_analysis_tool
from .tools.query_analysis_tool import _regex_extract_tables


def _bq_audit_tool_entry(
    project: str,
    days: int = 90,
    locations: list = None,
    limit: int = 1000,
    topn: int = 5,
    outfile: str = "bq_job_stats.csv",
):
    """bq_audit_tool: analyze recent BigQuery jobs and return top-N.

    Mirrors the CLI parameters for ease-of-use in ADK contexts.
    """
    params = AuditInput(
        project=project,
        days=days,
        locations=locations or ["US", "EU"],
        limit=limit,
        topn=topn,
        outfile=outfile,
    )
    return bq_audit_tool(params)


def _query_optimizer_tool_entry(sql: str):
    """query_optimizer_tool: suggest BigQuery SQL optimizations for the given query."""
    return query_optimizer_tool(OptimizeInput(sql=sql))


def _analyze_tool_entry(csv_path: str, output_dir: str = "./analysis_out", instructions: str = ""):
    """analyze_tool: run code-executing LLM to analyze CSV and save plots."""
    prompt = (
        instructions
        or "You are python data analyst using pandas and matplotlib. Your task is to write and execute code to perform analysis on the data provided and plot graphs. Your response must be graphs to visualize data."
    )
    return analyze_tool(AnalyzeInput(csv_path=csv_path, output_dir=output_dir, instructions=prompt))


def _query_analysis_tool_entry(sql: str, project: str):
    """query_analysis_tool: extract tables via LLM and enrich with INFORMATION_SCHEMA; writes temp.txt"""
    return query_analysis_tool(QueryAnalysisInput(sql=sql, project=project))


def _query_analysis_latest_local_entry(
    project: str,
    days: int = 1,
    locations: list = None,
    limit: int = 2000,
    topn: int = 10,
):
    """Pick the latest expensive query that references only local project tables, then analyze it.

    Skips queries whose referenced tables are in external projects (e.g., public datasets).
    """
    ai = AuditInput(
        project=project,
        days=days,
        locations=locations or ["US", "EU"],
        limit=limit,
        topn=topn,
        outfile="./bq_job_stats_today.csv",
    )
    res = bq_audit_tool(ai)
    # Sort by billed bytes then slot time desc
    candidates = sorted(res.jobs, key=lambda j: (j.total_bytes_billed, j.total_slot_ms), reverse=True)
    # Preload local datasets list
    bq_client = bigquery.Client(project=project)
    local_datasets = {d.dataset_id for d in bq_client.list_datasets(project=project)}
    for j in candidates:
        sql = j.query or ""
        if not sql.strip():
            continue
        tables = _regex_extract_tables(sql, project)
        if not tables:
            continue
        # accept only if all tables are in the local project AND datasets exist locally
        if all(((t.project or project) == project) for t in tables):
            if all((t.dataset in local_datasets) for t in tables):
                return query_analysis_tool(QueryAnalysisInput(sql=sql, project=project))
    # If none found, return empty output with note
    return {
        "tables": [],
        "metadata_file": "",
        "notes": "No local-project-only queries found in the window.",
    }


def load_agent() -> Agent:
        agent = Agent(
            name="bq_audit_agent",
            model="gemini-2.5-flash-lite",
        instruction=(
            "You are a BigQuery audit agent. Use tools to fetch job stats and summarize top-N costly queries."
        ),
        description="Agent that audits recent BigQuery jobs and suggests SQL optimizations.",
        tools=[
            FunctionTool(_bq_audit_tool_entry),
            FunctionTool(_query_optimizer_tool_entry),
            FunctionTool(_analyze_tool_entry),
            FunctionTool(_query_analysis_tool_entry),
            FunctionTool(_query_analysis_latest_local_entry),
        ],
    )
    return agent


def load_simple_optimizer_agent() -> Agent:
    """A lightweight agent that takes a SQL and asks Gemini for optimization tips."""
        agent = Agent(
            name="bq_sql_optimizer",
            model="gemini-2.5-flash-lite",
        instruction=(
            "You are a BigQuery SQL optimization assistant. Given a SQL query, provide concise, actionable"
            " recommendations to reduce cost and improve performance. Consider partitioning/clustering, pruning,"
            " materialization, avoiding SELECT *, approximate aggregations, limiting scanned columns, and using"
            " INFORMATION_SCHEMA where relevant. Return bullet points only."
        ),
        description="Suggests optimization recommendations for a given BigQuery SQL query.",
        tools=[],  # pure LLM
    )
    return agent


def optimize_sql_with_agent(sql: str) -> str:
    """Invoke the simple optimizer agent with the provided SQL and return the final text response."""
    async def _run() -> str:
        agent = load_simple_optimizer_agent()
        session_service = InMemorySessionService()
        app_name = "local_bq_optimizer"
        user_id = "local_user"
        session_id = "local_session"
        await session_service.create_session(app_name=app_name, user_id=user_id, session_id=session_id)
        runner = Runner(app_name=app_name, agent=agent, session_service=session_service)
        user_msg = Content(role="user", parts=[Part.from_text(text=f"Optimize this BigQuery SQL:\n```sql\n{sql}\n```")])
        chunks: list[str] = []
        async for ev in runner.run_async(user_id=user_id, session_id=session_id, new_message=user_msg):
            txt = getattr(ev, "text", None)
            if txt:
                chunks.append(txt)
        return "\n".join(chunks).strip()
    out = asyncio.run(_run())
    if out:
        return out
    # Fallback to direct Gemini API if ADK produced no text
    try:
        from google.genai import Client  # type: ignore
        api_key = os.environ.get("GOOGLE_API_KEY") or os.environ.get("GENAI_API_KEY")
        if api_key:
            client = Client(api_key=api_key)
            prompt = (
                "You are a BigQuery SQL optimization assistant. Provide concise bullet-point recommendations "
                "to reduce cost and improve performance. Consider partitioning, clustering, pruning, materialization, "
                "avoiding SELECT *, approximate aggregations, limiting scanned columns, and using INFORMATION_SCHEMA.\n\n"
                f"SQL to optimize:\n```sql\n{sql}\n```\n"
            )
            resp = client.models.generate_content(model="gemini-2.0-flash", contents=prompt)
            return (getattr(resp, "text", "") or "").strip()
    except Exception:
        pass
    return ""
