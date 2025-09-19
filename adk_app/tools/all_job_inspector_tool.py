import os
from typing import List

from google.cloud import bigquery
from google.adk import Agent
from google.adk.runners import Runner
from google.adk.sessions.in_memory_session_service import InMemorySessionService
from google.genai.types import Content, Part

from ..schemas import AllJobsInspectorInput, AllJobsInspectorOutput


PROMPT = (
    "Analyze the BigQuery jobs listed below and produce an optimization brief.\n\n"
    "Goals:\n"
    "- Identify the most frequently recurring or most expensive job patterns (by job_id, user, query signatures).\n"
    "- Explain which jobs run a lot and why (e.g., scheduled preview queries, repeated SELECT *).\n"
    "- Provide targeted, actionable optimization actions (e.g., add WHERE on partition, remove SELECT *, materialize summaries, schedule tuning).\n"
    "- Quantify expected savings qualitatively where possible.\n\n"
    "Input (Plaintext):\n\n{JOBS_TEXT}\n"
)


def _fetch_jobs(project: str, region: str, days: int, limit: int) -> List[bigquery.table.Row]:
    client = bigquery.Client(project=project)
    sql = f"""
    SELECT j.*, stage.*, timeline_entry.*, ref_table.*
    FROM `region-{region.lower()}`.INFORMATION_SCHEMA.JOBS AS j
    LEFT JOIN UNNEST(j.job_stages) AS stage
    LEFT JOIN UNNEST(j.timeline) AS timeline_entry
    LEFT JOIN UNNEST(j.referenced_tables) as ref_table
    WHERE j.creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
      AND j.job_type = 'QUERY'
    ORDER BY j.creation_time DESC
    LIMIT {limit}
    """
    return list(client.query(sql, location=region).result())


def _rows_to_text(rows: List[bigquery.table.Row]) -> str:
    lines: List[str] = []
    for r in rows[:300]:
        parts = []
        for k in r.keys():
            v = r.get(k)
            if isinstance(v, str) and len(v) > 500:
                v = v[:500] + " ... (truncated)"
            parts.append(f"{k}={v}")
        lines.append(" | ".join(parts))
    if len(rows) > 300:
        lines.append(f"... and {len(rows)-300} more rows")
    return "\n".join(lines)


def all_job_inspector_tool(params: AllJobsInspectorInput) -> AllJobsInspectorOutput:
    rows = _fetch_jobs(params.project, params.region, params.days, params.limit)
    jobs_text = _rows_to_text(rows)
    prompt = PROMPT.replace("{JOBS_TEXT}", jobs_text)

    agent = Agent(
        name="all_job_inspector",
        model="gemini-2.5-pro",
        instruction="Summarize and optimize job patterns across many jobs.",
        tools=[],
    )

    async def _run() -> str:
        session_service = InMemorySessionService()
        app_name = "all_job_inspector_app"
        user_id = "local_user"
        session_id = "aji_session"
        await session_service.create_session(app_name=app_name, user_id=user_id, session_id=session_id)
        runner = Runner(app_name=app_name, agent=agent, session_service=session_service)
        msg = Content(role="user", parts=[Part.from_text(text=prompt)])
        chunks: List[str] = []
        async for ev in runner.run_async(user_id=user_id, session_id=session_id, new_message=msg):
            txt = getattr(ev, "text", None)
            if txt:
                chunks.append(txt)
        return "\n".join(chunks).strip()

    import asyncio
    text = asyncio.run(_run())
    if not text:
        # Fallback to Google AI API
        try:
            from google.genai import Client  # type: ignore
            api_key = os.environ.get("GOOGLE_API_KEY") or os.environ.get("GENAI_API_KEY")
            if api_key:
                client = Client(api_key=api_key)
                resp = client.models.generate_content(model="gemini-2.0-flash", contents=prompt)
                text = (getattr(resp, "text", "") or "").strip()
        except Exception:
            text = text or ""

    os.makedirs(os.path.dirname(params.output_path), exist_ok=True)
    with open(params.output_path, "w") as f:
        f.write(text)
    preview = "\n".join(text.splitlines()[:40])
    return AllJobsInspectorOutput(report_path=os.path.abspath(params.output_path), text_preview=preview)


