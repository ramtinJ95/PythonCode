# Tibber Data Engineering Task

This project implements a data pipeline for currency conversion and transaction data processing as specified in the Tibber case assignment.

## Requirements

- Python 3.11.9 or higher
- Poetry (1.x or 2.x)
- PostgreSQL database

## Project Structure

```
tibber-case/
├── data/                  # CSV transaction data files
│   ├── batch1.csv
│   ├── batch2.csv
│   └── batch3.csv
├── src/                   # Source code
│   └── tibber_case/       # Main package
│       ├── currency/      # Currency rate handling
│       ├── transaction/   # Transaction data processing
│       └── database/      # Database connection and models
├── pyproject.toml         # Poetry configuration
├── poetry.lock            # Dependency lock file
└── README.md              # This file
```

## Installation

### Using Poetry 2.x

```bash
# Clone the repository
git clone <repository-url>
cd tibber-case

# Install dependencies
poetry install

# Activate the virtual environment
poetry shell
```

### Using Poetry 1.x

```bash
# Clone the repository
git clone <repository-url>
cd tibber-case

# Install dependencies
poetry install

# Activate the virtual environment
poetry shell
```

## Configuration

Create a `.env` file in the project root with the following variables:

```
# Database configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=tibber
DB_USER=your_username
DB_PASSWORD=your_password

# API configuration (if needed)
VATCOMPLY_API_KEY=your_api_key
```

## Usage

### 1. Currency Rate Ingestion

Fetch and store currency conversion rates:

```bash
poetry run python -m src.tibber_case.currency.ingest
```

### 2. Transaction Data Ingestion

Process and store transaction data from CSV files:

```bash
poetry run python -m src.tibber_case.transaction.ingest
```

### 3. Create SQL View for Currency Conversion

Create the SQL view for currency conversion:

```bash
poetry run python -m src.tibber_case.database.create_view
```

## Development

### Running Tests

```bash
poetry run pytest
```

### Code Formatting

```bash
poetry run black src
```

## Troubleshooting

### Poetry Version Differences

- **Poetry 1.x**: Uses `poetry run` to execute commands in the virtual environment
- **Poetry 2.x**: Also uses `poetry run`, but has some different command-line options

If you encounter issues with Poetry 2.x, try:

```bash
# Check Poetry version
poetry --version

# For Poetry 2.x specific issues, try:
poetry config virtualenvs.in-project true
poetry env use python3.11
poetry install
```

### Database Connection Issues

If you encounter database connection issues:
1. Verify PostgreSQL is running
2. Check your `.env` file for correct credentials
3. Ensure the database exists: `createdb tibber`
