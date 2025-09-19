from typing import List, Optional
from pydantic import BaseModel, Field


class AuditInput(BaseModel):
    project: str = Field(..., description="GCP project ID")
    days: int = Field(90, ge=1, description="Lookback window in days")
    locations: List[str] = Field(default_factory=lambda: ["US", "EU"], description="BigQuery multi-regions")
    limit: int = Field(1000, ge=1, description="Max jobs per location")
    topn: int = Field(5, ge=1, description="Number of top jobs to return")
    outfile: str = Field("bq_job_stats.csv", description="CSV output path")


class JobStat(BaseModel):
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


class AuditOutput(BaseModel):
    csv_path: str
    jobs: List[JobStat]
    top: List[JobStat]


class OptimizeInput(BaseModel):
    sql: str


class OptimizeOutput(BaseModel):
    recommendations: List[str]


class AnalyzeInput(BaseModel):
    csv_path: str = Field(..., description="Path to CSV to analyze")
    output_dir: str = Field("./analysis_out", description="Directory to save generated plots")
    instructions: str = Field(
        "You are python data analyst using pandas and matplotlib. Your task is to write and execute code to perform analysis on the data provided and plot graphs. Your response must be graphs to visualize data.",
        description="High-level prompt for the analysis agent",
    )


class AnalyzeOutput(BaseModel):
    plots: List[str] = Field(default_factory=list, description="List of saved plot file paths")


class QueryAnalysisInput(BaseModel):
    sql: str = Field(..., description="SQL to analyze for referenced tables")
    project: str = Field(..., description="Default GCP project to assume when not fully-qualified")
    job_id: Optional[str] = Field(None, description="Optional BigQuery job_id for diagnostics from INFORMATION_SCHEMA.JOBS")


class ExtractedTable(BaseModel):
    project: str
    dataset: str
    table: str


class QueryAnalysisOutput(BaseModel):
    tables: List[ExtractedTable] = Field(default_factory=list)
    metadata_file: str = Field("", description="Path to written schema information text file")
    notes: str = Field("", description="Any notes or warnings")


class ForensicInput(BaseModel):
    md_path: str = Field(..., description="Path to schema_report.md")
    output_path: str = Field("./analysis_out/forensic_report.md", description="Path to save the forensic report")


class ForensicOutput(BaseModel):
    report_path: str
    text_preview: str = Field("", description="First few lines of the report for quick preview")


class AllJobsInspectorInput(BaseModel):
    project: str = Field(..., description="GCP project ID")
    region: str = Field("US", description="BigQuery multi-region (US/EU)")
    days: int = Field(3, ge=1, description="Lookback window in days")
    limit: int = Field(200, ge=1, description="Max rows to pull from INFORMATION_SCHEMA.JOBS")
    output_path: str = Field("./analysis_out/all_job_inspector.md", description="Report output path")


class AllJobsInspectorOutput(BaseModel):
    report_path: str
    text_preview: str = Field("", description="First lines preview")
