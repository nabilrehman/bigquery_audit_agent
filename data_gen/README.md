# data_gen

Safeguarded synthetic data generator for BigQuery.

This creates a dataset and two tables, and can grow them to large sizes using chunked CTAS.
By default, it only prints a plan (no writes). You must pass `--confirm` to execute, and
`--force_really_big` if you request >= 1 TiB per table.

## Usage

Plan only (no writes):

```bash
python data_gen/generate_large_dataset.py \
  --project bq-demos-469816 \
  --dataset stress_test \
  --location US \
  --table1 logs_1 --table2 logs_2 \
  --target_gib 5 \
  --chunk_gib 1 \
  --payload_bytes 4096
```

Actually write (small size first, recommended ≤ 5 GiB):

```bash
python data_gen/generate_large_dataset.py \
  --project bq-demos-469816 \
  --dataset stress_test \
  --location US \
  --table1 logs_1 --table2 logs_2 \
  --target_gib 5 \
  --chunk_gib 1 \
  --payload_bytes 4096 \
  --confirm
```

Requests >= 1 TiB per table require an extra confirmation flag:

```bash
python data_gen/generate_large_dataset.py \
  --project bq-demos-469816 \
  --dataset stress_test \
  --location US \
  --table1 logs_1 --table2 logs_2 \
  --target_gib 5120 \
  --chunk_gib 64 \
  --payload_bytes 4096 \
  --confirm --force_really_big
```

## Notes
- Storage and cost implications are significant for multi‑TB sizes. Start small and scale up deliberately.
- The generated rows have a controllable payload; you can adjust `--payload_bytes` to approximate row size.
- CTAS from UNNEST(GENERATE_ARRAY(...)) avoids scanning existing tables, minimizing query costs while writing.
