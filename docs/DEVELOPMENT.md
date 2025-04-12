# Development Guide

## Development Environment Setup

### Prerequisites

- Python 3.11+
- PostgreSQL 15+
- Docker and Docker Compose
- Make (optional, but recommended)

### Initial Setup

1. Clone the repository
2. Create a virtual environment:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # or `.venv\Scripts\activate` on Windows
   ```
3. Install dependencies:
   ```bash
   python -m pip install -U pip
   pip install -r requirements.txt
   ```
4. Install pre-commit hooks:
   ```bash
   pre-commit install
   ```

## Project Structure

```
data_warehouse/
├── src/               # Source code
│   ├── ingestion/    # Data ingestion components
│   ├── transformation/ # Data transformation logic
│   ├── storage/      # Storage layer configurations
│   ├── orchestration/ # Workflow orchestration
│   ├── semantic/     # Semantic layer definitions
│   ├── visualization/ # Dashboard and viz components
│   └── api/         # API implementations
├── tests/           # Test suite
│   ├── unit/       # Unit tests
│   ├── integration/ # Integration tests
│   └── fixtures/   # Test fixtures and data
├── docs/           # Documentation
│   ├── components/ # Component-specific docs
│   └── api/       # API documentation
└── scripts/        # Utility scripts
```

## Development Standards

### Code Style

- We use `ruff` for both linting and formatting
- Type hints are required for all function signatures
- Docstrings follow Google style
- Maximum line length is 88 characters
- Use snake_case for functions and variables
- Use PascalCase for classes

### Testing

- All code must have unit tests
- Integration tests for data pipelines
- Use pytest fixtures for test data
- Maintain minimum 80% code coverage
- Test both success and error cases

### Git Workflow

1. Create a feature branch from main:
   ```bash
   git checkout -b feature/your-feature-name
   ```
2. Make your changes
3. Run tests and linting:
   ```bash
   pytest
   ruff check .
   ruff format .
   ```
4. Commit using conventional commits:
   ```bash
   git commit -m "feat: add new feature"
   ```
5. Push and create a pull request

### Database Migrations

- Use Alembic for schema migrations
- Always include both up and down migrations
- Test migrations on a copy of production data
- Document breaking changes

### Data Pipeline Development

1. Start with source configuration in `src/ingestion`
2. Implement ingestion logic
3. Create dbt models in `src/transformation`
4. Define Dagster assets in `src/orchestration`
5. Add data quality tests
6. Document the pipeline

### API Development

- Follow REST/GraphQL best practices
- Document all endpoints with OpenAPI/GraphQL schema
- Include request/response examples
- Implement proper error handling
- Add rate limiting where appropriate

### Configuration Management

- Use environment variables for secrets
- Store configs in YAML/TOML files
- Separate dev/staging/prod configs
- Document all configuration options

### Logging and Monitoring

- Use structured logging
- Include correlation IDs
- Log appropriate detail level
- Add metrics for key operations

### Performance Optimization

- Profile code before optimizing
- Document performance requirements
- Benchmark critical paths
- Consider scalability implications

### Security Practices

- Never commit secrets
- Validate all inputs
- Use prepared statements
- Implement proper authentication
- Follow least privilege principle

## Common Development Tasks

### Adding a New Data Source

1. Create source connector in `src/ingestion`
2. Define source schema
3. Implement extraction logic
4. Add data quality checks
5. Create dbt models in `src/transformation`
6. Update documentation

### Creating a New API Endpoint

1. Define endpoint specification
2. Implement route handler in `src/api`
3. Add request validation
4. Write unit tests
5. Document the endpoint
6. Add usage examples

### Debugging Tips

- Use VS Code debugger
- Check Dagster UI for pipeline issues
- Review PostgreSQL query plans
- Monitor system resources
- Check application logs

## Troubleshooting

### Common Issues

- Database connection problems
- Pipeline failures
- API errors
- Performance bottlenecks

### Support Resources

- Internal documentation
- Team chat channels
- Issue tracker
- External documentation links
