-- Example Data Quality Tests using shared macros
-- Place this file in dbt_common/tests and symlink/copy into dbt_postgres/tests and dbt_duckdb/tests as needed

-- Not Null Test Example
{{ test_not_null(ref('my_first_dbt_model'), 'id') }}

-- Unique Test Example
{{ test_unique(ref('my_first_dbt_model'), 'id') }}

-- Accepted Values Test Example
{{ test_accepted_values(ref('my_first_dbt_model'), 'status', ['active', 'inactive', 'pending']) }}

-- Cross-Table Consistency (Foreign Key) Test Example
{{ test_cross_table_consistency(ref('my_first_dbt_model'), 'customer_id', ref('dim_customers'), 'id') }}

-- These tests are compatible with both Postgres and DuckDB DBT projects.
