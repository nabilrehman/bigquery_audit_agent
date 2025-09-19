---
format: bq_schema_report
version: 1
project: bq-demos-469816
generated_utc: 2025-09-19T01:31:31.565119Z
---

# BigQuery Schema Metadata

  api_details_error: 404 GET https://bigquery.googleapis.com/bigquery/v2/projects/bq-demos-469816/datasets/stress_test/tables/INFORMATION_SCHEMA?prettyPrint=false: Not found: Table bq-demos-469816:stress_test.INFORMATION_SCHEMA

Dataset API totals: tables=2, sum_num_bytes=138441823016

## Job Diagnostics
job_id: 3545af0b-0c61-47c6-8488-0c5a2328ca1e
region: US
- row:
    creation_time: 2025-09-18 22:57:32.065000+00:00
    project_id: bq-demos-469816
    project_number: 549403515075
    user_email: admin@nabilrehman.altostrat.com
    principal_subject: user:admin@nabilrehman.altostrat.com
    job_id: 3545af0b-0c61-47c6-8488-0c5a2328ca1e
    job_type: QUERY
    statement_type: SELECT
    priority: INTERACTIVE
    start_time: 2025-09-18 22:57:32.157000+00:00
    end_time: 2025-09-18 22:57:32.393000+00:00
    query: 
    SELECT view_definition
    FROM `stress_test.INFORMATION_SCHEMA.VIEWS`
    WHERE table_name = @table
    
    state: DONE
    reservation_id: None
    total_bytes_processed: 10485760
    total_slot_ms: 93
    error_result: None
    cache_hit: False
    destination_table: {'project_id': 'bq-demos-469816', 'dataset_id': '_36181d09bb4edb188f015b344717f5ac947b40c1', 'table_id': 'anond066b407_fdcb_4196_a67d_a5eb85f67b71'}
    referenced_tables: [{'project_id': 'bq-demos-469816', 'dataset_id': 'stress_test', 'table_id': 'INFORMATION_SCHEMA.VIEWS'}]
    labels: []
    timeline: [{'elapsed_ms': 160, 'total_slot_ms': 93, 'pending_units': 0, 'completed_units': 1, 'active_units': None, 'estimated_runnable_units': 0}]
    job_stages: [{'name': 'S00: Output', 'id': 0, 'start_ms': 1758236252249, 'end_ms': 1758236252298, 'input_stages': [], 'wait_ratio_avg': 0.06666666666666667, 'wait_ms_avg': 1, 'wait_ratio_max': 0.06666666666666667, 'wait_ms_max': 1, 'read_ratio_avg': 0.0, 'read_ms_avg': 0, 'read_ratio_max': 0.0, 'read_ms_max': 0, 'compute_ratio_avg': 1.0, 'compute_ms_avg': 15, 'compute_ratio_max': 1.0, 'compute_ms_max': 15, 'write_ratio_avg': 0.2, 'write_ms_avg': 3, 'write_ratio_max': 0.2, 'write_ms_max': 3, 'shuffle_output_bytes': 0, 'shuffle_output_bytes_spilled': 0, 'records_read': 0, 'records_written': 0, 'parallel_inputs': 1, 'completed_parallel_inputs': 1, 'status': 'COMPLETE', 'steps': [{'kind': 'READ', 'substeps': ['$2:view_definition, $1:table_name', 'FROM stress_test.INFORMATION_SCHEMA.VIEWS', "WHERE equal($1, 'logs_1')"]}, {'kind': 'WRITE', 'substeps': ['$2', 'TO __stage00_output']}], 'slot_ms': 93, 'compute_mode': 'BIGQUERY'}]
    total_bytes_billed: 10485760
    transaction_id: None
    parent_job_id: None
    session_info: None
    dml_statistics: None
    total_modified_partitions: 0
    bi_engine_statistics: None
    query_info: {'resource_warning': None, 'optimization_details': None, 'query_hashes': {'normalized_literals': 'c7b6e7ed50aa402e3107c7dc6712dab22b6da40ef6eb9db68d1adced6a88feb3'}, 'performance_insights': {'avg_previous_execution_ms': 122, 'stage_performance_standalone_insights': [], 'stage_performance_change_insights': []}}
    transferred_bytes: 0
    materialized_view_statistics: None
    edition: None
    job_creation_reason: {'code': 'REQUESTED'}
    continuous_query_info: None
    continuous: False
    query_dialect: GOOGLE_SQL
    metadata_cache_statistics: {'table_metadata_cache_usage': []}
    search_statistics: {'index_usage_mode': 'UNUSED', 'index_unused_reasons': [{'code': 'INDEX_CONFIG_NOT_AVAILABLE', 'message': 'There is no index configuration for the base table `bq-demos-469816:stress_test.INFORMATION_SCHEMA.VIEWS`.', 'base_table': {'project_id': 'bq-demos-469816', 'dataset_id': 'stress_test', 'table_id': 'INFORMATION_SCHEMA.VIEWS'}, 'index_name': None}], 'index_pruning_stats': []}
    vector_search_statistics: None
    name: S00: Output
    id: 0
    start_ms: 1758236252249
    end_ms: 1758236252298
    input_stages: []
    wait_ratio_avg: 0.06666666666666667
    wait_ms_avg: 1
    wait_ratio_max: 0.06666666666666667
    wait_ms_max: 1
    read_ratio_avg: 0.0
    read_ms_avg: 0
    read_ratio_max: 0.0
    read_ms_max: 0
    compute_ratio_avg: 1.0
    compute_ms_avg: 15
    compute_ratio_max: 1.0
    compute_ms_max: 15
    write_ratio_avg: 0.2
    write_ms_avg: 3
    write_ratio_max: 0.2
    write_ms_max: 3
    shuffle_output_bytes: 0
    shuffle_output_bytes_spilled: 0
    records_read: 0
    records_written: 0
    parallel_inputs: 1
    completed_parallel_inputs: 1
    status: COMPLETE
    steps: [{'kind': 'READ', 'substeps': ['$2:view_definition, $1:table_name', 'FROM stress_test.INFORMATION_SCHEMA.VIEWS', "WHERE equal($1, 'logs_1')"]}, {'kind': 'WRITE', 'substeps': ['$2', 'TO __stage00_output']}]
    slot_ms: 93
    compute_mode: BIGQUERY
    elapsed_ms: 160
    total_slot_ms_1: 93
    pending_units: 0
    completed_units: 1
    active_units: None
    estimated_runnable_units: 0
    project_id_1: bq-demos-469816
    dataset_id: stress_test
    table_id: INFORMATION_SCHEMA.VIEWS

## Notes
Skipping external table: stress_test.INFORMATION_SCHEMA.VIEWS
Error TABLES/TABLE_STORAGE/PARTITIONS/COLUMNS for bq-demos-469816.stress_test.INFORMATION_SCHEMA: 404 Not found: Table bq-demos-469816:INFORMATION_SCHEMA.TABLE_STORAGE was not found in location US; reason: notFound, message: Not found: Table bq-demos-469816:INFORMATION_SCHEMA.TABLE_STORAGE was not found in location US

Location: US
Job ID: f0dec9e8-9b3d-43ba-ae41-e9039500c607

Error dataset totals for bq-demos-469816.stress_test: 404 Not found: Table bq-demos-469816:INFORMATION_SCHEMA.TABLE_STORAGE was not found in location US; reason: notFound, message: Not found: Table bq-demos-469816:INFORMATION_SCHEMA.TABLE_STORAGE was not found in location US

Location: US
Job ID: d81024d6-8c1a-4591-97b4-2c56a977f7c2
