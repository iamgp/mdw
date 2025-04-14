# Modern Data Warehouse

A modern data warehouse implementation using DuckDB, PostgreSQL, DBT, and Dagster.

## Features

- Hybrid storage approach using DuckDB and PostgreSQL
- ETL pipeline framework with modular components
- Data transformation using DBT
- Workflow orchestration with Dagster
- GraphQL API for data access
- CLI tools for data management
- Comprehensive logging and monitoring
- Type-safe implementation with Python 3.11+

## Project Structure

```
data_warehouse/
├── src/
│   └── data_warehouse/
│       ├── api/          # FastAPI/GraphQL endpoints
│       ├── cli/          # CLI commands
│       ├── config/       # Configuration management
│       ├── core/         # Core components and base classes
│       ├── extractors/   # Data extraction components
│       ├── loaders/      # Data loading components
│       ├── transformers/ # Data transformation components
│       └── utils/        # Utility functions
├── tests/
│   ├── unit/            # Unit tests
│   └── integration/     # Integration tests
├── docs/                # Documentation
├── dbt/                 # DBT project
└── dagster_home/        # Dagster configuration
```

## Requirements

- Python 3.11 or higher
- PostgreSQL 15 or higher
- Poetry for dependency management
- Docker (optional, for containerized deployment)

## Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/yourusername/data_warehouse.git
   cd data_warehouse
   ```

2. Create and activate a virtual environment:

   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. Install dependencies:

   ```bash
   pip install -e ".[dev]"
   ```

4. Install pre-commit hooks:

   ```bash
   pre-commit install
   ```

5. Copy the example environment file and update it:
   ```bash
   cp .env.example .env
   ```

## Environment Variables

The following environment variables can be configured:

- `POSTGRES_HOST`: PostgreSQL host (default: localhost)
- `POSTGRES_PORT`: PostgreSQL port (default: 5432)
- `POSTGRES_USER`: PostgreSQL username (default: postgres)
- `POSTGRES_PASSWORD`: PostgreSQL password
- `POSTGRES_DB`: PostgreSQL database name (default: data_warehouse)
- `MINIO_HOST`: MinIO host (default: localhost)
- `MINIO_PORT`: MinIO port (default: 9000)
- `MINIO_ACCESS_KEY`: MinIO access key
- `MINIO_SECRET_KEY`: MinIO secret key
- `DATA_WAREHOUSE_ROOT`: (Optional) Absolute path to the project root directory. If set, this overrides auto-detection.
  - Resolution order: (1) If DATA_WAREHOUSE_ROOT is set, it is used as the project root. (2) Otherwise, the root is auto-detected as three levels up from the config file location.
  - Recommended to set in containerized or production environments for robust path resolution.
- `LOG_LEVEL`: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)

## Development

1. Ensure PostgreSQL is running and accessible with the credentials in your `.env` file.

2. Run the tests:

   ```bash
   pytest
   ```

3. Start the API server:

   ```bash
   uvicorn data_warehouse.api.main:app --reload
   ```

4. Start the Dagster UI:
   ```bash
   dagster dev
   ```

## Usage

### CLI Commands

The package provides several CLI commands for data management:

```bash
# Initialize the data warehouse
dw init

# Run a data pipeline
dw pipeline run --name my_pipeline

# Query the warehouse
dw query "SELECT * FROM my_table"
```

### API Endpoints

The GraphQL API is available at `http://localhost:8000/graphql` and provides:

- Data querying
- Pipeline management
- Metadata access
- Schema information

### DBT Models

DBT models are organized in the `dbt/` directory:

- `models/staging/`: Initial data transformations
- `models/intermediate/`: Business logic layer
- `models/marts/`: Final presentation layer

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests and linting
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
