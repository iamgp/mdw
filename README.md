# Data Warehouse

A modern data warehouse implementation using Python, Dagster, PostgreSQL, and DuckDB.

## Overview

This project implements a modular, scalable data warehouse with:

- **Extraction** from multiple sources (APIs, files)
- **Loading** into PostgreSQL (raw layer)
- **Transformation** into standardized formats
- **Data marts** in DuckDB for analytics
- **Orchestration** using Dagster

The pipeline follows ELT (Extract, Load, Transform) principles, with a layered approach:

1. **Raw Layer** (PostgreSQL): Stores raw data from sources without modification
2. **Transformed Layer** (PostgreSQL): Cleaned, validated, and standardized data
3. **Mart Layer** (DuckDB): Analytics-ready aggregated data for specific domains

## Project Structure

```
src/data_warehouse/
├── api/                  # API for accessing data warehouse
├── cli/                  # Command-line interface
├── config/               # Configuration management
├── core/                 # Core domain logic
├── ingestion/            # Data ingestion utilities
├── orchestration/        # Pipeline orchestration
│   └── dagster/          # Dagster assets and resources
│       ├── assets/       # Data assets
│       │   ├── raw_data.py       # Raw data extraction
│       │   ├── transformed_data.py # Data transformation
│       │   └── marts.py         # Data mart creation
│       └── resources/    # Resource definitions
│           ├── database.py      # Database connections
│           └── io_managers.py   # I/O managers
├── services/             # Service interfaces
├── sources/              # Source data clients
│   ├── api_client.py     # API client for external data
│   └── file_client.py    # File client for CSV/Excel data
├── utils/                # Utility functions
│   ├── transformations.py # Data transformation utilities
│   └── error_handler.py  # Error handling utilities
└── main.py               # Application entry point
```

## Setup

### Prerequisites

- Python 3.10+
- PostgreSQL
- DuckDB

### Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/yourusername/data_warehouse.git
   cd data_warehouse
   ```

2. Create a virtual environment:

   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. Install dependencies:

   ```bash
   pip install -e .
   ```

4. Set up environment variables:
   ```bash
   export POSTGRES_CONNECTION_STRING="postgresql://user:password@localhost:5432/data_warehouse"
   export DUCKDB_DATABASE_PATH="/path/to/analytics.duckdb"
   export API_BASE_URL="https://api.example.com/v1"
   export DATA_FILES_DIRECTORY="/path/to/data/files"
   ```

## Running the Pipeline

### Using the CLI

The data warehouse comes with a comprehensive CLI that makes it easy to manage all aspects of the system:

```bash
# Show available commands
data-warehouse --help

# Get information about the current configuration
data-warehouse info

# Run storage commands
data-warehouse storage --help

# Run Dagster commands
data-warehouse dagster --help
```

#### Dagster CLI Commands

Interact with Dagster directly from the command line:

```bash
# Start the Dagster UI
data-warehouse dagster ui

# Start the Dagster daemon (for schedules and sensors)
data-warehouse dagster daemon

# List all available assets
data-warehouse dagster list-assets

# Materialize specific assets
data-warehouse dagster materialize raw_sales transformed_sales

# Materialize all assets
data-warehouse dagster materialize --all

# Run a specific job
data-warehouse dagster run daily_refresh
```

### Start Dagster UI Directly

You can also start Dagster directly:

```bash
dagster dev
```

Then open http://localhost:3000 in your browser.

### Run a Full Warehouse Update

From the Dagster UI, you can:

1. Materialize all assets
2. Run individual asset groups
3. Schedule regular updates

## Data Flow

1. **Extraction**:

   - Raw data extracted from APIs and files
   - Loaded into PostgreSQL without transformation

2. **Transformation**:

   - Data cleaned and standardized
   - Business rules applied
   - Quality checks performed

3. **Data Marts**:
   - Aggregated views created for analysis
   - Optimized for query performance
   - Segmented by business domain

## Development

### Adding a New Data Source

1. Create a client in `sources/` if needed
2. Add raw assets in `orchestration/dagster/assets/raw_data.py`
3. Add transformations in `utils/transformations.py`
4. Add transformed assets in `orchestration/dagster/assets/transformed_data.py`
5. Update data marts in `orchestration/dagster/assets/marts.py`

### Running Tests

```bash
pytest
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.
