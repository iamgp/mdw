# Contributing to Modern Data Warehouse

Thank you for your interest in contributing to the Modern Data Warehouse project! This document provides guidelines and instructions for contributing.

## Development Setup

1. Fork and clone the repository:

   ```bash
   git clone https://github.com/yourusername/data_warehouse.git
   cd data_warehouse
   ```

2. Set up your development environment:

   ```bash
   # Create and activate virtual environment
   uv venv
   source .venv/bin/activate  # On Unix/macOS

   # Install dependencies
   uv pip install -e ".[dev]"
   ```

3. Install pre-commit hooks:
   ```bash
   pre-commit install
   ```

## Development Workflow

1. Create a new branch for your feature:

   ```bash
   git checkout -b feature/your-feature-name
   ```

2. Make your changes following our coding standards:

   - Use type hints for all function signatures
   - Follow PEP 8 style guide (enforced by ruff)
   - Write tests for new functionality
   - Update documentation as needed

3. Run tests and checks:

   ```bash
   # Run tests
   pytest

   # Run linter
   ruff check .
   ruff format .

   # Run type checker
   basedpyright .
   ```

4. Commit your changes:

   ```bash
   git add .
   git commit -m "feat: description of your changes"
   ```

   Follow conventional commits format:

   - feat: New feature
   - fix: Bug fix
   - docs: Documentation changes
   - style: Code style changes
   - refactor: Code refactoring
   - test: Test updates
   - chore: Maintenance tasks

5. Push to your fork and create a pull request

## Pull Request Process

1. Ensure all tests pass and code quality checks succeed
2. Update documentation if needed
3. Add tests for new functionality
4. Update the README.md if needed
5. Link any related issues
6. Request review from maintainers

## Code Review Process

1. Maintainers will review your PR
2. Address any requested changes
3. Once approved, maintainers will merge your PR

## Testing Guidelines

- Write unit tests for all new functionality
- Maintain minimum 90% code coverage
- Include integration tests for workflows
- Add performance tests for critical paths
- Test edge cases and error conditions

## Documentation

- Update docstrings for new functions and classes
- Keep README.md current
- Add examples for new features
- Document configuration options

## Questions or Problems?

- Open an issue for bugs or feature requests
- Join our community discussions
- Contact maintainers for guidance

Thank you for contributing!
