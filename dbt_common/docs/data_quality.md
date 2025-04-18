# Data Quality Testing Framework

This document outlines the data quality testing framework implemented using DBT, shared across both the Postgres and DuckDB projects.

## Overview

The framework utilizes DBT's testing capabilities, enhanced with custom macros stored in `dbt_common/macros`. Test results are logged to a monitoring database for historical tracking and alerting.

## Shared Macros (`dbt_common/macros/data_quality_macros.sql`)

These macros provide reusable test logic:

- `test_not_null(model, column_name)`: Checks if a column contains NULL values.
- `test_unique(model, column_name)`: Checks if all values in a column are unique.
- `test_accepted_values(model, column_name, accepted_values)`: Checks if column values are within a specified list.
- `test_cross_table_consistency(model, column_name, ref_model, ref_column)`: Checks for referential integrity between two tables (similar to a foreign key check).

## Example Tests (`dbt_common/tests/data_quality_tests.sql`)

This file demonstrates how to use the shared macros. Copy or symlink this file into the `tests/` directory of your specific DBT project (`dbt_postgres/tests` or `dbt_duckdb/tests`).

```sql
-- Example: Test that the 'id' column in 'my_model' is not null
{{ test_not_null(ref('my_model'), 'id') }}

-- Example: Test that 'status' in 'my_model' is one of the accepted values
{{ test_accepted_values(ref('my_model'), 'status', ['active', 'inactive', 'pending']) }}
```

## Monitoring Test Results

1.  **Run Tests:** Execute `dbt test` within either the `dbt_postgres` or `dbt_duckdb` project directory.
2.  **Log Results:** After running tests, use the CLI command:

    ```bash
    # For Postgres project results logged to Postgres DB (defaults)
    python -m data_warehouse.cli.main transforms log-results

    # For DuckDB project results logged to DuckDB DB
    python -m data_warehouse.cli.main transforms log-results --dbt-project duckdb --db-target duckdb

    # Specify a custom results file path
    python -m data_warehouse.cli.main transforms log-results --results-path path/to/your/run_results.json
    ```

3.  **Database Table:** Test results are stored in the `monitoring.dbt_test_results` table in the target database (Postgres or DuckDB).
4.  **Dashboard (Placeholder):** A future dashboard will visualize these results. Access it (when implemented) via:
    ```bash
    python -m data_warehouse.cli.main monitor test-dashboard --db-target [postgres|duckdb]
    ```

## Alerting

The `log-results` command automatically checks for test failures (`fail` or `error` status). If failures are detected, a `CRITICAL` log message is generated:

```
CRITICAL: <N> DBT tests failed or errored. Check the 'monitoring.dbt_test_results' table or DBT artifacts for details.
```

Integrate this logging output with your preferred monitoring/alerting system (e.g., Datadog, Grafana Loki, PagerDuty) to trigger notifications on critical failures.

## Adding New Tests

1.  Create new test SQL files in `dbt_postgres/tests` or `dbt_duckdb/tests`.
2.  Use the shared macros from `dbt_common/macros` or DBT's built-in tests.
3.  For complex, reusable logic, add new macros to `dbt_common/macros` and copy/symlink them to the relevant projects.
