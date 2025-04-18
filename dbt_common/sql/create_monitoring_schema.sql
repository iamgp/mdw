-- Schema for monitoring DBT test results
-- Compatible with PostgreSQL and DuckDB

-- Create monitoring schema if it doesn't exist
-- Note: Adapt schema creation command based on the target database
-- For PostgreSQL: CREATE SCHEMA IF NOT EXISTS monitoring;
-- For DuckDB: ATTACH DATABASE ':memory:' AS monitoring; -- Or use a file

-- Table to store test results
CREATE TABLE IF NOT EXISTS monitoring.dbt_test_results (
    result_id VARCHAR PRIMARY KEY, -- Unique ID from run_results.json
    invocation_id VARCHAR,         -- DBT invocation ID
    test_unique_id VARCHAR,        -- Unique ID of the test node
    test_name VARCHAR,             -- Name of the test (e.g., unique, not_null)
    test_type VARCHAR,             -- Type of test (e.g., generic, singular)
    model_name VARCHAR,            -- Name of the model being tested
    column_name VARCHAR,           -- Name of the column being tested (if applicable)
    status VARCHAR NOT NULL,       -- 'pass', 'fail', 'error', 'skipped'
    execution_time REAL,           -- Time taken to execute the test in seconds
    failure_details TEXT,          -- Details if the test failed or errored
    rows_affected INTEGER,         -- Number of failing rows (if applicable)
    run_timestamp TIMESTAMP WITH TIME ZONE NOT NULL -- Timestamp of the DBT run completion
);

-- Optional: Indexes for faster querying
CREATE INDEX IF NOT EXISTS idx_dbt_test_results_status ON monitoring.dbt_test_results (status);
CREATE INDEX IF NOT EXISTS idx_dbt_test_results_model ON monitoring.dbt_test_results (model_name);
CREATE INDEX IF NOT EXISTS idx_dbt_test_results_timestamp ON monitoring.dbt_test_results (run_timestamp);
