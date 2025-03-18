# Tibber Data Engineering Task

Data pipeline for currency conversion and transaction data processing.

## Requirements
- Python 3.11.9+
- Poetry: https://python-poetry.org/docs/#installing-with-pipx you can also
install poetry through normal pip there are some subtle differences and using
pip to install it is usually done in a CI environment.

If you dont want to install Poetry to run this project, I have exported a requirements.txt file
that can be used with the old school way of virtual env + pip install -r workflow. 

- PostgreSQL

## Installation
Before running make install run the following command:

```bash
poetry config virtualenvs.in-project true
```
and it should just use the poetry.lock that
already exists in this repo and create a `.venv` folder for you.

```bash
make install
```

## Configuration
Create a `.env` file:
```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=tibber
DB_USER=your_username
DB_PASSWORD=your_password
```
Most likley the project has the .env file already for this case.

## Usage
```bash
# Run the complete pipeline
make run_case
```
If you want to rerun the insert from csv's and getting data from the api then choose 'N'
when asked for input once you run make run_case. If you choose to reset the database it will drop all tables and views, recreate them and then insert data again into them. Can be useful for testing.

## Development
```bash
# Run tests
make test

# Format code
make format