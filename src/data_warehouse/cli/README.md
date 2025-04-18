# Data Warehouse CLI

This directory contains the command-line interface (CLI) for the data warehouse project. The CLI allows you to manage various aspects of the data warehouse, including:

- Storage configuration
- Data ingestion
- Data transformations
- Dagster workflow orchestration
- Monitoring and alerting
- Documentation

## CLI Structure

The CLI is built using Python's [Click](https://click.palletsprojects.com/) library. Commands are organized into logical groups:

```
data-warehouse
├── storage        # Storage management commands
├── ingestion      # Data ingestion commands
├── transforms     # Data transformation commands
├── dagster        # Dagster workflow orchestration commands
├── deploy         # Deployment commands
├── monitor        # Monitoring and alerting commands
├── docs           # Documentation commands
├── version        # Show version information
├── info           # Show configuration information
└── doctor         # Run diagnostic checks
```

## Dagster Commands

The Dagster commands allow you to interact with the Dagster workflow orchestration engine. These commands enable you to:

- Start the Dagster UI
- Start the Dagster daemon for schedules and sensors
- Materialize assets
- Run jobs
- List assets, jobs, and schedules
- Manage Dagster storage

### Available Dagster Commands

```
data-warehouse dagster
├── ui                    # Start the Dagster UI server
├── daemon                # Start the Dagster daemon process
├── materialize           # Materialize assets
├── run                   # Run a specific job
├── list-assets           # List all available assets
├── list-jobs             # List all available jobs
├── list-schedules        # List all defined schedules
└── wipe                  # Wipe all Dagster storage
```

### Examples

Start the Dagster UI for local development:

```bash
data-warehouse dagster ui
data-warehouse dagster ui --port 8000
```

Materialize specific assets:

```bash
data-warehouse dagster materialize raw_sales transformed_sales
```

Materialize all assets:

```bash
data-warehouse dagster materialize --all
```

Run a specific job:

```bash
data-warehouse dagster run daily_refresh
data-warehouse dagster run etl_job --config job_config.yaml
```

List all available assets:

```bash
data-warehouse dagster list-assets
```

Start the Dagster daemon for schedules and sensors:

```bash
data-warehouse dagster daemon
```

## Other Commands

For information on other command groups, run:

```bash
data-warehouse --help
```

For help with specific command groups, run:

```bash
data-warehouse <command-group> --help
```

For example:

```bash
data-warehouse dagster --help
data-warehouse transforms --help
```
