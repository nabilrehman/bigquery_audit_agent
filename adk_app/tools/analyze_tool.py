import os
from typing import List
import datetime as dt

from google.adk import Agent
from google.adk.code_executors import BuiltInCodeExecutor
from google.adk.runners import Runner
from google.adk.sessions.in_memory_session_service import InMemorySessionService
from google.genai.types import Content, Part

from ..schemas import AnalyzeInput, AnalyzeOutput


ANALYZE_SYSTEM_PROMPT = (
    "You are a Python data analyst using pandas and matplotlib. "
    "Write and execute code to analyze the provided CSV and produce multiple charts. "
    "Save all charts into a single PDF using matplotlib.backends.backend_pdf.PdfPages (do not call plt.show()). "
    "Express all byte-based values in GiB on axes and labels (divide bytes by 1024**3). "
    "Also compute an estimated on-demand cost per job using: cost_usd = (total_bytes_billed / (1024**4)) * PRICE_PER_TiB_USD (default 5.0). "
    "Create at least 6 charts: (1) Top-N jobs by billed bytes (GiB), (2) Distribution (histogram) of billed bytes (GiB), "
    "(3) Daily trend of total billed bytes (GiB), (4) Breakdown by statement_type or user_email (GiB), "
    "(5) A slot time chart using total_slot_ms converted to seconds, (6) Top-N jobs by estimated cost (USD) and a daily total cost trend."
)


def _ensure_dir(path: str) -> str:
    os.makedirs(path, exist_ok=True)
    return os.path.abspath(path)


def analyze_tool(params: AnalyzeInput) -> AnalyzeOutput:
    csv_path = os.path.abspath(params.csv_path)
    out_dir = _ensure_dir(params.output_dir)
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    target_pdf = os.path.abspath(os.path.join(out_dir, f"analysis_report_{ts}.pdf"))

    # Build an LLM agent with a built-in code executor
        agent = Agent(
        name="csv_analysis_agent",
        model="gemini-2.0-flash",
        instruction=ANALYZE_SYSTEM_PROMPT,
        description="Analyzes CSV data with pandas and matplotlib and saves plots.",
        code_executor=BuiltInCodeExecutor(),
        tools=[],
    )

    # Create a session and run instructions asking to analyze the CSV and save a PDF report into out_dir
    session_service = InMemorySessionService()
    app_name = "csv_analysis_app"
    user_id = "local_user"
    session_id = "analysis_session"

    # Prepare a directive to ensure code saves figures into a timestamped PDF
    task = (
        f"Load the CSV at: {csv_path}. Use pandas for analysis and matplotlib for plotting.\n"
        f"Create multiple sleek, modern charts (consistent palette, light grid, annotated bars where applicable) and save them into a single combined PDF report at '{target_pdf}' using matplotlib.backends.backend_pdf.PdfPages.\n"
        f"Do not call plt.show(); ensure the PDF file is written to disk."
    )

    # Runner requires an existing session
    async def _run() -> List[str]:
        await session_service.create_session(app_name=app_name, user_id=user_id, session_id=session_id)
        runner = Runner(app_name=app_name, agent=agent, session_service=session_service)
        msg = Content(role="user", parts=[Part.from_text(text=params.instructions + "\n\n" + task)])
        # Stream events; we don't need the text, just let the agent execute code
        async for _ in runner.run_async(user_id=user_id, session_id=session_id, new_message=msg):
            pass
        # After run completes, prefer the intended timestamped PDF if present
        pdfs: List[str] = []
        if os.path.exists(target_pdf):
            pdfs = [target_pdf]
        else:
            pdfs = [os.path.join(out_dir, p) for p in os.listdir(out_dir) if p.lower().endswith(".pdf")]
        return pdfs

    import asyncio

        try:
            plots = asyncio.run(_run())
        except Exception:
            # Explicit fallback to Google AI API if ADK path fails due to missing Vertex setup
            plots = []

    # If the LLM did not write to the timestamped target, force-generate our sleek report there
    if not os.path.exists(target_pdf):
        plots = []
        try:
            import pandas as pd
            import matplotlib
            matplotlib.use("Agg")  # non-interactive backend
            import matplotlib.pyplot as plt
            from matplotlib.backends.backend_pdf import PdfPages

            df = pd.read_csv(csv_path)
            # Coerce numeric fields
            for col in ["total_bytes_billed", "total_bytes_processed", "total_slot_ms"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
            # Parse creation_time
            if "creation_time" in df.columns:
                with pd.option_context('mode.chained_assignment', None):
                    df["creation_time"] = pd.to_datetime(df["creation_time"], errors="coerce")
                    df["date"] = df["creation_time"].dt.date
            BYTES_PER_GIB = 1024 ** 3
            BYTES_PER_TIB = 1024 ** 4
            # Allow override of price per TiB via env; default $5/TiB for on-demand
            try:
                PRICE_PER_TIB_USD = float(os.environ.get("BQ_ONDEMAND_PRICE_PER_TIB_USD", "5.0"))
            except Exception:
                PRICE_PER_TIB_USD = 5.0
            # Apply sleek, modern styling
            import matplotlib as mpl
            mpl.rcParams.update({
                "axes.spines.top": False,
                "axes.spines.right": False,
                "axes.grid": True,
                "grid.alpha": 0.2,
                "axes.titlesize": 14,
                "axes.labelsize": 12,
                "xtick.labelsize": 10,
                "ytick.labelsize": 10,
            })
            import matplotlib.pyplot as plt
            plt.style.use("seaborn-v0_8")
            palette = ["#4C78A8", "#F58518", "#54A24B", "#EECA3B", "#B279A2", "#FF9DA6", "#9C755F", "#BAB0AC"]

            pdf_path = target_pdf
            with PdfPages(pdf_path) as pdf:
                # Plot 1: Top 10 jobs by billed bytes
                if {"job_id", "total_bytes_billed"}.issubset(df.columns):
                    top10 = (
                        df[["job_id", "total_bytes_billed"]]
                        .sort_values("total_bytes_billed", ascending=False)
                        .head(10)
                    )
                    top10_gib = top10.copy()
                    top10_gib["total_gib"] = top10_gib["total_bytes_billed"] / BYTES_PER_GIB
                    plt.figure(figsize=(10, 6))
                    bars = plt.barh(top10_gib["job_id"], top10_gib["total_gib"], color=palette[0]) 
                    plt.gca().invert_yaxis()
                    plt.title("Top 10 Jobs by Billed Bytes (GiB)")
                    plt.xlabel("Billed GiB")
                    # annotate values
                    mx = max(top10_gib["total_gib"]) if len(top10_gib) else 0
                    for b in bars:
                        w = b.get_width()
                        plt.text(w + mx * 0.01, b.get_y() + b.get_height()/2, f"{w:.2f}", va="center")
                    plt.tight_layout()
                    pdf.savefig()
                    plt.close()

                # Plot 2: Top 10 users by total billed bytes
                if {"user_email", "total_bytes_billed"}.issubset(df.columns):
                    by_user_bytes = df.groupby("user_email")["total_bytes_billed"].sum().sort_values(ascending=False).head(10)
                    by_user_gib = by_user_bytes / BYTES_PER_GIB
                    plt.figure(figsize=(10, 6))
                    bars = plt.barh(by_user_gib.index, by_user_gib.values, color=palette[1])
                    plt.gca().invert_yaxis()
                    plt.title("Top 10 Users by Total Billed (GiB)")
                    plt.xlabel("Billed GiB")
                    mx = max(by_user_gib.values) if len(by_user_gib.values) else 0
                    for b, v in zip(bars, by_user_gib.values):
                        plt.text(v + mx * 0.01, b.get_y() + b.get_height()/2, f"{v:.2f}", va="center")
                    plt.tight_layout()
                    pdf.savefig()
                    plt.close()

                # Plot 3: Distribution of billed bytes (log scale) for non-zero values
                if "total_bytes_billed" in df.columns:
                    billed_nonzero_gib = (df.loc[df["total_bytes_billed"] > 0, "total_bytes_billed"]) / BYTES_PER_GIB
                    if len(billed_nonzero_gib) > 0:
                        plt.figure(figsize=(10, 6))
                        plt.hist(billed_nonzero_gib, bins=30, color=palette[2], edgecolor="white")
                        plt.title("Distribution of Billed (GiB)")
                        plt.xlabel("Billed GiB")
                        plt.ylabel("Count")
                        plt.tight_layout()
                        pdf.savefig()
                        plt.close()

                # Plot 4: Daily total billed bytes trend
                if "date" in df.columns and "total_bytes_billed" in df.columns:
                    daily_gib = (df.groupby("date")["total_bytes_billed"].sum().sort_index()) / BYTES_PER_GIB
                    if len(daily_gib) > 0:
                        plt.figure(figsize=(11, 5))
                        plt.plot(daily_gib.index, daily_gib.values, marker="o", linewidth=2, color=palette[3])
                        plt.title("Daily Total Billed (GiB)")
                        plt.xlabel("Date")
                        plt.ylabel("Billed GiB")
                        plt.xticks(rotation=45, ha="right")
                        plt.tight_layout()
                        pdf.savefig()
                        plt.close()

                # Plot 5: Breakdown by statement_type (top categories)
                if {"statement_type", "total_bytes_billed"}.issubset(df.columns):
                    by_type_gib = (df.groupby("statement_type")["total_bytes_billed"].sum().sort_values(ascending=False).head(10)) / BYTES_PER_GIB
                    if len(by_type_gib) > 0:
                        plt.figure(figsize=(10, 6))
                        plt.barh(by_type_gib.index.astype(str), by_type_gib.values, color=palette[4])
                        plt.gca().invert_yaxis()
                        plt.title("Top Statement Types by Total Billed (GiB)")
                        plt.xlabel("Billed GiB")
                        plt.tight_layout()
                        pdf.savefig()
                        plt.close()

                # Plot 6: Top 10 jobs by total slot time (seconds)
                if {"job_id", "total_slot_ms"}.issubset(df.columns):
                    top_slot = (
                        df[["job_id", "total_slot_ms"]]
                        .sort_values("total_slot_ms", ascending=False)
                        .head(10)
                    )
                    top_slot_sec = top_slot.copy()
                    top_slot_sec["slot_seconds"] = top_slot_sec["total_slot_ms"] / 1000.0
                    plt.figure(figsize=(10, 6))
                    bars = plt.barh(top_slot_sec["job_id"], top_slot_sec["slot_seconds"], color=palette[5])
                    plt.gca().invert_yaxis()
                    plt.title("Top 10 Jobs by Slot Time")
                    plt.xlabel("Slot Time (seconds)")
                    mx = max(top_slot_sec["slot_seconds"]) if len(top_slot_sec) else 0
                    for b, v in zip(bars, top_slot_sec["slot_seconds"].values):
                        plt.text(v + mx * 0.01, b.get_y() + b.get_height()/2, f"{v:.1f}", va="center")
                    plt.tight_layout()
                    pdf.savefig()
                    plt.close()

                # Plot 7: Top 10 jobs by estimated on-demand cost (USD)
                if {"job_id", "total_bytes_billed"}.issubset(df.columns):
                    cost_df = df[["job_id", "total_bytes_billed"]].copy()
                    cost_df["cost_usd"] = (cost_df["total_bytes_billed"] / BYTES_PER_TIB) * PRICE_PER_TIB_USD
                    top_cost = cost_df.sort_values("cost_usd", ascending=False).head(10)
                    plt.figure(figsize=(10, 6))
                    bars = plt.barh(top_cost["job_id"], top_cost["cost_usd"], color=palette[6])
                    plt.gca().invert_yaxis()
                    plt.title(f"Top 10 Jobs by Estimated Cost (USD) @ ${PRICE_PER_TIB_USD}/TiB")
                    plt.xlabel("Estimated Cost (USD)")
                    mx = max(top_cost["cost_usd"]) if len(top_cost) else 0
                    for b, v in zip(bars, top_cost["cost_usd"].values):
                        plt.text(v + mx * 0.01, b.get_y() + b.get_height()/2, f"${v:.2f}", va="center")
                    plt.tight_layout()
                    pdf.savefig()
                    plt.close()

                # Plot 8: Daily total estimated cost (USD)
                if "date" in df.columns and "total_bytes_billed" in df.columns:
                    daily_cost = (df.groupby("date")["total_bytes_billed"].sum() / BYTES_PER_TIB) * PRICE_PER_TIB_USD
                    daily_cost = daily_cost.sort_index()
                    if len(daily_cost) > 0:
                        plt.figure(figsize=(11, 5))
                        plt.plot(daily_cost.index, daily_cost.values, marker="o", linewidth=2, color=palette[7])
                        plt.title(f"Daily Estimated Cost (USD) @ ${PRICE_PER_TIB_USD}/TiB")
                        plt.xlabel("Date")
                        plt.ylabel("Estimated Cost (USD)")
                        plt.xticks(rotation=45, ha="right")
                        plt.tight_layout()
                        pdf.savefig()
                        plt.close()

            plots.append(pdf_path)
        except Exception:
            # If fallback fails, return what we have (likely empty)
            pass

    return AnalyzeOutput(plots=plots)


