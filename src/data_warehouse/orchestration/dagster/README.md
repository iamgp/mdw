# Dagster Workflow Orchestration

This module implements workflow orchestration for the data warehouse using [Dagster](https://dagster.io/).

## Overview

Dagster is used to orchestrate the data pipelines in the data warehouse, handling:

- Asset-based data flows from ingestion to transformation to serving
- Scheduled pipeline execution
- Event-driven pipeline triggering
- Resource management (database connections, etc.)
- Data quality validation
- Pipeline monitoring and alerting

## Project Structure

```
src/data_warehouse/orchestration/dagster/
├── assets/               # Data assets definitions
│   ├── raw.py            # Raw data assets
│   ├── staging.py        # Staging data assets
│   └── warehouse.py      # Warehouse data assets
├── ops/                  # Reusable operations
│   ├── data_quality.py   # Data quality operations
│   ├── notifications.py  # Notification operations
│   └── utilities.py      # Utility operations
├── resources/            # Resource definitions
│   ├── database.py       # Database connection resources
│   └── io_managers.py    # I/O managers for data storage
├── schedules/            # Schedule definitions
└── sensors/              # Sensor definitions
```

## Asset Model

The asset model follows the data flow in the data warehouse:

1. **Raw Assets**: Data extracted from source systems
2. **Staging Assets**: Cleaned and transformed data
3. **Warehouse Assets**: Final data models for analytics

## Running Dagster

### Local Development

To run Dagster locally for development:

```bash
# Start the Dagster webserver
dagster dev -f src/data_warehouse/orchestration/dagster/__init__.py
```

This will start the Dagster UI at http://localhost:3000

### Production Deployment

For production, Dagster can be deployed using:

1. **Dagster Daemon**: For running schedules and sensors
2. **Dagster Webserver**: For the UI
3. **PostgreSQL Database**: For storing Dagster's state

Example deployment configuration:

```bash
# Start the Dagster daemon
dagster-daemon run

# Start the Dagster webserver
dagster-webserver -f src/data_warehouse/orchestration/dagster/__init__.py
```

## Configuration

Dagster is configured via:

1. **dagster.yaml**: Main configuration file
2. **workspace.yaml**: Workspace configuration

Environment-specific configuration is handled through environment variables.

## Adding New Components

### Adding a New Asset

1. Create a new asset function in one of the asset modules or create a new module
2. Decorate the function with `@asset`
3. Specify dependencies using the `deps` parameter
4. Update tests as needed

Example:

```python
@asset(
    group_name="staging",
    deps=["raw_data"],
    description="New staging data asset",
)
def new_staging_asset(context, raw_data):
    # Transform raw_data into new_staging_asset
    return transformed_data
```

### Adding a New Schedule

1. Define a job that materializes the assets you want to schedule
2. Create a schedule definition with the appropriate cron schedule
3. Add the schedule to the definitions object

Example:

```python
new_job = define_asset_job(name="new_job", selection=AssetSelection.keys("asset1", "asset2"))

new_schedule = ScheduleDefinition(
    name="new_schedule",
    cron_schedule="0 2 * * *",  # Run at 2:00 AM daily
    job=new_job,
)
```

### Adding a New Sensor

1. Define a job that materializes the assets to trigger when the sensor fires
2. Create a sensor function that returns a RunRequest when appropriate
3. Add the sensor to the definitions object

Example:

```python
@sensor(job=some_job)
def new_sensor(context):
    # Check for some condition
    if condition_met:
        return RunRequest(run_key=f"key_{time.time()}")
    return None
```
