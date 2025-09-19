#!/usr/bin/env python3
"""
Safeguarded generator for large synthetic BigQuery tables.

IMPORTANT:
- This script is designed with hard safety rails. It will NOT create multi‑TB data
  unless you pass explicit confirmation flags.
- By default, it generates a small sample (1 GiB per table) so you can validate the
  end‑to‑end flow without risk.

Strategy (safe baseline):
- Creates (or ensures) a dataset.
- Creates two tables with a fixed schema.
- Uses CREATE TABLE AS SELECT with UNNEST(GENERATE_ARRAY(...)) to generate rows
  whose average size is controlled via a payload column.
- Supports chunked writes so you can scale up deliberately.

Costs/limits to keep in mind:
- Storage cost ~ $0.02/GB‑month (standard), so 5 TiB ≈ 5120 GB ≈ $102/month per table.
- Query processing cost with this approach is low (no scanned sources), but DML/DDL
  and storage costs still apply; always test with small sizes first.
"""

from __future__ import annotations

import argparse
import math
import os
from typing import Tuple

from google.cloud import bigquery


DEFAULT_SCHEMA = [
    bigquery.SchemaField("id", "INT64"),
    bigquery.SchemaField("user", "STRING"),
    bigquery.SchemaField("ts", "TIMESTAMP"),
    bigquery.SchemaField("payload", "STRING"),
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Safeguarded large data generator for BigQuery")
    p.add_argument("--project", required=True, help="GCP project ID")
    p.add_argument("--dataset", required=True, help="Target dataset ID (will be created if missing)")
    p.add_argument("--location", default="US", help="Dataset location (default: US)")
    p.add_argument("--table1", default="test_table_1")
    p.add_argument("--table2", default="test_table_2")
    p.add_argument(
        "--target_gib",
        type=float,
        default=1.0,
        help="Approximate GiB to generate PER table (default: 1 GiB).",
    )
    p.add_argument(
        "--confirm",
        action="store_true",
        help="Actually execute data generation (without this, script prints a plan only)",
    )
    p.add_argument(
        "--force_really_big",
        action="store_true",
        help="Additional confirmation required when target_gib >= 1024 (1 TiB) per table.",
    )
    p.add_argument(
        "--chunk_gib",
        type=float,
        default=1.0,
        help="Chunk size GiB per CTAS execution (default: 1 GiB).",
    )
    p.add_argument(
        "--payload_bytes",
        type=int,
        default=4096,
        help="Payload size per row (approximate row bytes; default: 4096)",
    )
    return p.parse_args()


def ensure_dataset(client: bigquery.Client, dataset_id: str, location: str) -> None:
    ds_ref = bigquery.Dataset(f"{client.project}.{dataset_id}")
    ds_ref.location = location
    try:
        client.get_dataset(ds_ref)
    except Exception:
        client.create_dataset(ds_ref, exists_ok=True)


def drop_table_if_exists(client: bigquery.Client, full_table_id: str) -> None:
    client.delete_table(full_table_id, not_found_ok=True)


def compute_rows_for_bytes(target_bytes: float, payload_bytes: int) -> int:
    # Very rough estimate: one row ≈ payload + small overhead (we add ~64 bytes)
    avg_row = max(1, payload_bytes + 64)
    return max(1, int(target_bytes // avg_row))


def ctas_chunk_sql(dataset: str, table: str, rows: int, payload_bytes: int, append: bool) -> str:
    # Generate synthetic data with controlled payload size; id cycles from 1..rows
    # Avoid BigQuery's GENERATE_ARRAY element caps by using a 2D grid when rows are large.
    write_clause = "CREATE TABLE" if not append else "INSERT INTO"
    target = f"`{dataset}.{table}`"
    max_single_array = 9_000_000  # stay under ~10M element limit per array

    if rows <= max_single_array:
        prefix = f"{write_clause} {target} AS" if not append else f"{write_clause} {target}"
        return f"""
        {prefix}
        WITH gen AS (
          SELECT id
          FROM UNNEST(GENERATE_ARRAY(1, {rows})) AS id
        )
        SELECT
          id,
          CONCAT('user_', CAST(MOD(id, 100000) AS STRING)) AS user,
          TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), SECOND) AS ts,
          REPEAT('x', {payload_bytes}) AS payload
        FROM gen
        """.strip()

    # Use 2D grid: rows_i * rows_j >= rows, each dimension kept small
    rows_i = min(10_000, int(math.ceil(math.sqrt(rows))))
    rows_j = int(math.ceil(rows / rows_i))
    if rows_j > 10_000:
        rows_j = 10_000
        rows_i = int(math.ceil(rows / rows_j))

    # Safety: final guard
    rows_i = max(1, rows_i)
    rows_j = max(1, rows_j)

    id_expr = f"(i - 1) * {rows_j} + j"
    prefix = f"{write_clause} {target} AS" if not append else f"{write_clause} {target}"
    return f"""
    {prefix}
    WITH gen_i AS (
      SELECT i FROM UNNEST(GENERATE_ARRAY(1, {rows_i})) AS i
    ),
    gen_j AS (
      SELECT j FROM UNNEST(GENERATE_ARRAY(1, {rows_j})) AS j
    ),
    gen AS (
      SELECT {id_expr} AS id
      FROM gen_i CROSS JOIN gen_j
      WHERE {id_expr} <= {rows}
    )
    SELECT
      id,
      CONCAT('user_', CAST(MOD(id, 100000) AS STRING)) AS user,
      TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), SECOND) AS ts,
      REPEAT('x', {payload_bytes}) AS payload
    FROM gen
    """.strip()


def generate_table(client: bigquery.Client, dataset: str, table: str, target_gib: float, chunk_gib: float, payload_bytes: int, do_run: bool) -> Tuple[int, float]:
    target_bytes = target_gib * (1024 ** 3)
    chunk_bytes = min(target_bytes, max(1, int(chunk_gib * (1024 ** 3))))
    chunks = int(math.ceil(target_bytes / chunk_bytes))
    full_table_id = f"{client.project}.{dataset}.{table}"
    # Ensure a clean start: drop existing so first chunk can CREATE TABLE
    drop_table_if_exists(client, full_table_id)

    total_rows = 0
    for i in range(chunks):
        rows = compute_rows_for_bytes(chunk_bytes, payload_bytes)
        append = i > 0
        sql = ctas_chunk_sql(dataset, table, rows, payload_bytes, append)
        if not do_run:
            print(f"[PLAN] {'APPEND' if append else 'CREATE'} {full_table_id} chunk {i+1}/{chunks}: ~{chunk_gib:.2f} GiB, rows≈{rows}")
        else:
            job = client.query(sql)
            job.result()
            print(f"[DONE]  {'APPEND' if append else 'CREATE'} {full_table_id} chunk {i+1}/{chunks}: rows≈{rows}")
        total_rows += rows
    return total_rows, target_gib


def main() -> int:
    args = parse_args()
    client = bigquery.Client(project=args.project)

    if args.target_gib >= 1024 and not args.force_really_big:
        print("Refusing to proceed: --target_gib >= 1024 requires --force_really_big.")
        print("Use smaller sizes first, or set --force_really_big if you really understand the costs.")
        return 2

    ensure_dataset(client, args.dataset, args.location)

    # Plan phase (always printed)
    print(f"Project:  {args.project}")
    print(f"Dataset:  {args.dataset} ({args.location})")
    print(f"Tables:   {args.table1}, {args.table2}")
    print(f"Target:   ~{args.target_gib:.2f} GiB per table in chunks of ~{args.chunk_gib:.2f} GiB (payload {args.payload_bytes} bytes)")
    if not args.confirm:
        print("\nSAFETY: --confirm not provided. Showing plan only. No data has been written.")
    else:
        print("\nCONFIRMED: proceeding to create data.")

    # Execute (or plan) table 1
    rows1, gib1 = generate_table(
        client,
        dataset=args.dataset,
        table=args.table1,
        target_gib=args.target_gib,
        chunk_gib=args.chunk_gib,
        payload_bytes=args.payload_bytes,
        do_run=args.confirm,
    )
    # Execute (or plan) table 2
    rows2, gib2 = generate_table(
        client,
        dataset=args.dataset,
        table=args.table2,
        target_gib=args.target_gib,
        chunk_gib=args.chunk_gib,
        payload_bytes=args.payload_bytes,
        do_run=args.confirm,
    )

    print("\nSummary:")
    print(f"  {args.table1}: rows≈{rows1}, size target≈{gib1:.2f} GiB")
    print(f"  {args.table2}: rows≈{rows2}, size target≈{gib2:.2f} GiB")
    if not args.confirm:
        print("\nTo actually run, re‑invoke with --confirm (and --force_really_big for >= 1 TiB).\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


