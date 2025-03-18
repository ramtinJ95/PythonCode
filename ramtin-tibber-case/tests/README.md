# Tests for Tibber Case

This directory contains tests for the Tibber Data Engineering Case project.

## Test Structure

- `test_database_manager.py`: Tests for the database manager functionality
- `test_main.py`: Tests for the main function and exception handling

## Running Tests

Run all tests with coverage:

```bash
poetry run pytest
```

Run specific test file:

```bash
poetry run pytest tests/test_database_manager.py
```

## Test Coverage

The tests focus on the most important aspects of the codebase:

1. Database connection handling
2. Query execution (both SELECT and non-SELECT queries)
3. Batch insertion functionality
4. Checkpoint management
5. Database setup process
6. Exception handling in the main function

The tests use mocking to avoid actual database connections while still verifying the correct behavior of the code. 