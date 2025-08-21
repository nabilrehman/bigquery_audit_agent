SELECT
  job_id,
  project_id,
  user_email,
  creation_time,
  start_time,
  end_time,
  job_type,
  state,
  statement_type,
  priority,
  cache_hit,
  reservation_id,
  total_bytes_processed,
  total_bytes_billed,
  total_slot_ms,
  TO_JSON_STRING(destination_table)   AS destination_table,
  TO_JSON_STRING(referenced_tables)   AS referenced_tables,
  TO_JSON_STRING(referenced_routines) AS referenced_routines,
  TO_JSON_STRING(error_result)        AS error_result,
  TO_JSON_STRING(labels)              AS labels,
  TO_JSON_STRING(job_stages)          AS job_stages
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_USER
WHERE user_email = SESSION_USER()
  AND job_type = "QUERY"
  AND creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)
ORDER BY creation_time DESC;
