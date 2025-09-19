import os
from typing import List

from google.adk import Agent
from google.adk.runners import Runner
from google.adk.sessions.in_memory_session_service import InMemorySessionService
from google.genai.types import Content, Part

from ..schemas import ForensicInput, ForensicOutput


PROMPT_TEMPLATE = (
    "You are a Principal Cloud Economist & Data Strategist. Your mission is to bridge the gap between deep technical analysis and high-level strategic and financial decision-making. You will analyze a single expensive query from a \"Top 100\" list, treating it as a representative symptom of a larger pattern, and build a comprehensive, data-driven investment proposal to address it.\n\n"
    "Context:\n"
    "I am analyzing one of our top 100 most expensive queries. I need a full strategic and tactical breakdown to determine the best course of action.\n\n"
    "Analyzed Query:\n\nSQL\n\n{SQL_BLOCK}\n\n"
    "Table Schema(s):\n\nPlaintext\n\n{SCHEMA_BLOCK}\n\n"
    "Systemic Context (Optional, but provide if known):\n\n"
    "This query represents a common pattern of: [Describe the pattern, e.g., 'full table scans for daily reporting', 'unfiltered joins between large fact/dimension tables', 'exploratory queries on raw data'].\n\n"
    "We estimate this pattern occurs approximately [Number] times per [Day/Week/Month].\n\n"
    "The engineering team's blended cost is [e.g., $150/hour].\n\n"
    "The company's data volume is projected to grow [e.g., 100% year-over-year].\n\n"
    "Input Data:\n\nPlaintext\n\n{FULL_MD}\n\n"
    "Your Task: Generate a Strategic Optimization & Investment Proposal\n\n"
    "1. Executive Briefing (Target: CEO, CDO)\n\n"
    "Problem Statement: Start by framing the issue at a strategic level. Based on the query pattern, describe how this type of workload is creating financial waste and accumulating technical debt that threatens future scalability.\n\n"
    "Financial Analysis (Annualized):\n\n"
    "Single Query Cost: Calculate the on-demand cost for the single job analyzed.\n\n"
    "Total Annual Waste (if context is provided): Extrapolate this single cost based on the frequency provided in the context to project the Total Annual Waste from this pattern.\n\n"
    "Future Financial Risk (if context is provided): Project the annual waste for next year, factoring in the data growth projection. If no context is given, state that this is the cost of a single run and the risk is magnified with data growth.\n\n"
    "Strategic Risk: Quantify the non-financial costs. Analyze the job's timeline and slot usage to assess the opportunity cost (wasted compute) and the risk of delayed business decisions due to inefficient performance at scale.\n\n"
    "Proposed Solution: In one sentence, summarize the proposed three-part solution: immediate remediation, architectural investment, and a cultural shift towards financial accountability.\n\n"
    "2. Business Case & ROI (Target: Head of Data)\n\n"
    "Proposed Investment (The \"Ask\"):\n\n"
    "Effort Estimate: Based on the proposed architectural changes in the next section, provide a reasonable estimate of the engineering hours required.\n\n"
    "Total Investment Cost: Calculate the one-time cost of this effort using the engineering rate provided in the context.\n\n"
    "Projected Financial Return:\n\n"
    "12-Month Savings: State the projected cost savings over the next 12 months.\n\n"
    "Return on Investment (ROI): Calculate the ROI percentage for this initiative.\n\n"
    "Payback Period: Calculate how quickly this one-time investment will pay for itself.\n\n"
    "3. Root Cause Analysis & Tactical Execution Plan (Target: BigQuery Lead)\n\n"
    "Root Cause Identification: Analyze the job metadata to pinpoint the primary performance bottleneck. Check for common anti-patterns such as:\n\n"
    "Inefficient Scans: Is the query using SELECT * unnecessarily? Does it lack a WHERE clause on a potentially partitionable/clusterable column?\n\n"
    "Expensive Joins: Is it joining large tables without pre-filtering? Is there evidence of a join explosion (records_written >> records_read in a join stage)?\n\n"
    "Data Skew / Hotspots: Is one stage consuming a disproportionate amount of slot_ms?\n\n"
    "Suboptimal Operations: Is the query using inefficient functions or an ORDER BY on a massive, unfiltered dataset?\n\n"
    "Tiered Recommendations: Based on your findings, provide a tiered list of solutions:\n\n"
    "Tier 1 (Immediate SQL Fix): Provide a direct SQL rewrite to address the primary bottleneck identified.\n\n"
    "Tier 2 (Architectural Change): If applicable, provide the DDL to re-create the table(s) with optimal partitioning and clustering, justifying the choice of keys based on the schema and query logic.\n\n"
    "4. Governance & Cultural Initiative (Target: CDO)\n\n"
    "Strategic Recommendation: Frame this incident as a critical opportunity to instill a culture of cost-awareness and financial accountability in our data practices.\n\n"
    "Proposed Program: \"Data FinOps\":\n\n"
    "Policy: Recommend the formal adoption of policies that govern efficient querying (e.g., mandatory WHERE clauses on partitioned tables).\n\n"
    "Enablement: Propose the creation of mandatory training modules for all data users on cost-aware querying.\n\n"
    "Enforcement & Automation: Recommend investing in automated governance tools and building monitoring dashboards on INFORMATION_SCHEMA to proactively detect and flag wasteful queries across the entire organization.\n"
)


def forensic_agent_tool(params: ForensicInput) -> ForensicOutput:
    md_path = os.path.abspath(params.md_path)
    if not os.path.exists(md_path):
        return ForensicOutput(report_path="", text_preview="schema_report.md not found")

    with open(md_path, "r") as f:
        md_text = f.read()

    # Attempt to fetch the top expensive SQL from CSV if present
    sql_block = ""
    try:
        import csv
        csv_path = os.path.abspath("./bq_job_stats_today.csv")
        if os.path.exists(csv_path):
            with open(csv_path) as f:
                rows = list(csv.DictReader(f))
            rows.sort(key=lambda r: (int(r.get('total_bytes_billed') or 0), int(r.get('total_slot_ms') or 0)), reverse=True)
            for r in rows:
                if r.get('query'):
                    sql_block = r['query']
                    break
    except Exception:
        pass

    # Extract a lightweight schema block from the markdown (columns_detailed section if available)
    schema_block = ""
    try:
        lines = md_text.splitlines()
        start = None
        for i, ln in enumerate(lines):
            if ln.strip().lower().startswith('columns_detailed'):
                start = i
                break
        if start is not None:
            schema_block = "\n".join(lines[start:start+50])
        else:
            schema_block = md_text[:800]
    except Exception:
        schema_block = md_text[:800]

    prompt = PROMPT_TEMPLATE.replace("{SQL_BLOCK}", sql_block or "[SQL not found]") \
                             .replace("{SCHEMA_BLOCK}", schema_block or "[Schema not found]") \
                             .replace("{FULL_MD}", md_text)

    # ADK-first: use an LLM agent, fallback to Google AI API only if no text
    agent = Agent(
        name="forensic_bq_finops",
        model="gemini-2.5-pro",
        instruction="Generate the requested forensic report strictly following the user's template.",
        tools=[],
    )

    async def _run() -> str:
        session_service = InMemorySessionService()
        app_name = "forensic_app"
        user_id = "local_user"
        session_id = "forensic_session"
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
        try:
            from google.genai import Client  # type: ignore
            import os as _os
            api_key = _os.environ.get("GOOGLE_API_KEY") or _os.environ.get("GENAI_API_KEY")
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
    return ForensicOutput(report_path=os.path.abspath(params.output_path), text_preview=preview)


