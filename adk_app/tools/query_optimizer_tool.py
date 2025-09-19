from typing import List
import os
from ..schemas import OptimizeInput, OptimizeOutput


SYSTEM_PROMPT = (
    "You are a BigQuery SQL optimization assistant. Provide concrete, actionable recommendations "
    "to reduce cost and improve performance. Consider partitioning, clustering, pruning, materialization, "
    "avoiding SELECT *, using approximate aggregations, limiting scanned columns, and using INFORMATION_SCHEMA."
)


def _local_rules(sql: str) -> List[str]:
    recs: List[str] = []
    s = sql.upper()
    if "SELECT *" in s:
        recs.append("Avoid SELECT *; project only required columns to reduce scanned bytes.")
    if "FROM `BIGQUERY-PUBLIC-DATA" in s or "FROM\n`BIGQUERY-PUBLIC-DATA" in s:
        recs.append("Consider creating a filtered/materialized table for frequently accessed subsets of public datasets.")
    if "WHERE YEAR(" in s or "EXTRACT(YEAR" in s:
        recs.append("If tables are partitioned by date/timestamp, filter on the partition column to prune partitions.")
    if "ORDER BY" in s and "LIMIT" in s:
        recs.append("Use approximate aggregations or pre-aggregated tables if ORDER BY LIMIT causes large scans.")
    return recs


def query_optimizer_tool(params: OptimizeInput) -> OptimizeOutput:
    sql = params.sql or ""

        # ADK/Vertex-first (as before); fall back to local rules
        recs: List[str] = []
        try:
            project = os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCLOUD_PROJECT")
            location = os.environ.get("GOOGLE_CLOUD_LOCATION", "us-central1")
            if project:
                from vertexai import init as vertexai_init  # type: ignore
                from vertexai.generative_models import GenerativeModel, GenerationConfig  # type: ignore

                vertexai_init(project=project, location=location)
                model = GenerativeModel("gemini-2.5-pro")
                prompt = (
                    f"{SYSTEM_PROMPT}\n\n"
                    "Return a concise list of bullet-point recommendations only.\n"
                    "If you suggest schema changes, explain the exact BigQuery features to use.\n\n"
                    "SQL to optimize:\n"
                    f"```sql\n{sql}\n```\n"
                )
                response = model.generate_content(
                    [prompt],
                    generation_config=GenerationConfig(temperature=0.2, max_output_tokens=1024),
                )
                text = getattr(response, "text", None) or ""
                lines = [ln.strip("- ") for ln in text.splitlines()]
                recs = [ln for ln in lines if ln]
        except Exception:
            recs = []

    if not recs:
        recs = _local_rules(sql)
        if not recs:
            recs = [
                "No obvious local optimizations detected. Consider partition/clustering and avoiding SELECT *.",
            ]
    return OptimizeOutput(recommendations=recs)


