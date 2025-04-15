# CI/CD Pipeline

This project uses GitHub Actions for continuous integration (CI) and continuous deployment (CD).

## Workflow Location

- `.github/workflows/ci.yml`

## Triggers

- On push to `main` or `master` branches
- On pull requests targeting `main` or `master`

## Steps

1. **Checkout code**
2. **Set up Python 3.11**
3. **Install dependencies** using Hatch
4. **Run Ruff** for linting
5. **Run Pyright** for type checking
6. **Run Pytest** for tests and coverage
7. **Upload coverage report** as an artifact
8. **Fail the build** if any step fails

## Troubleshooting

- **Linting errors:** Run `hatch run lint:ruff check .` locally and fix reported issues.
- **Type errors:** Run `hatch run lint:pyright` locally to see type issues.
- **Test failures:** Run `hatch run test:pytest` locally and review test output.
- **Missing coverage report:** Ensure tests generate `coverage.xml`.
- **Dependency issues:** Run `hatch env create` to recreate the environment.

For more details, see the workflow file or contact the maintainers.
