import logging
import os
from typing import Any

import psycopg2
from dotenv import load_dotenv
from psycopg2.extensions import connection
from psycopg2.extras import execute_values

load_dotenv()

logger = logging.getLogger(__name__)


def get_db_connection() -> connection:
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )


def execute_query(
    query: str,
    params: tuple[Any, ...] | None = None,
    conn: connection | None = None,
) -> list[tuple] | None:
    should_close_conn = False
    if conn is None:
        conn = get_db_connection()
        should_close_conn = True

    try:
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            conn.commit()
            try:  # If the query is not a select, fetchall will raise an error
                return cursor.fetchall()
            except psycopg2.ProgrammingError:
                return None
    finally:
        if should_close_conn:
            conn.close()


def batch_insert(query: str, params_list: list[tuple[Any, ...]]) -> int:
    if not params_list:
        return 0

    conn = get_db_connection()
    rows_affected = 0

    try:
        with conn.cursor() as cursor:
            # For execute_values, we need to modify the query format
            # It expects "INSERT INTO table (cols) VALUES %s" where %s is a placeholder for all value sets
            table_and_columns = query.split("VALUES")[0].strip()

            # Create the new query format for execute_values
            new_query = f"{table_and_columns} VALUES %s"

            # If there's an ON CONFLICT clause, preserve it
            if "ON CONFLICT" in query:
                on_conflict_clause = query.split("ON CONFLICT")[1].strip()
                new_query = f"{new_query} ON CONFLICT {on_conflict_clause}"

            # Execute the batch insert
            execute_values(cursor, new_query, params_list, template=None, page_size=100)
            rows_affected = cursor.rowcount
            conn.commit()
        return rows_affected
    finally:
        conn.close()


def check_database_encoding() -> None:
    query = "SHOW server_encoding;"
    result = execute_query(query)
    encoding = result[0][0] if result else "Unknown"

    logger.info("Database server encoding: %s", encoding)


def create_currency_table() -> None:
    query = """
    CREATE TABLE IF NOT EXISTS currencies (
        currency_code VARCHAR(3) PRIMARY KEY,
        currency_name VARCHAR(50) NOT NULL,
        currency_symbol VARCHAR(10) NOT NULL,
        _inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    execute_query(query)
    logger.info("Currencies table created successfully.")


def create_currency_conversion_rates_table(base_currency: str) -> None:
    query = f"""
    CREATE TABLE IF NOT EXISTS currency_conversion_rates_base_{base_currency} (
        currency_code VARCHAR(3) PRIMARY KEY,
        rate DECIMAL(32, 16) NOT NULL,
        last_updated_at DATE NOT NULL,
        _inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT fk_currency_code
            FOREIGN KEY (currency_code)
            REFERENCES currencies (currency_code)
            ON DELETE CASCADE
    );
    """
    execute_query(query)
    logger.info("Currency conversion rates table created successfully.")


def create_item_prices_table() -> None:
    query = """
    CREATE TABLE IF NOT EXISTS item_prices (
        id UUID NOT NULL,
        item VARCHAR(100) NOT NULL,
        price DECIMAL(10, 2) NOT NULL,
        currency VARCHAR(3) NOT NULL,
        created_at TIMESTAMP WITH TIME ZONE NOT NULL,
        updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
        system_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
        CONSTRAINT unique_id_timestamp UNIQUE (id, system_timestamp)
    );
    """
    execute_query(query)
    logger.info("Item prices table created successfully.")


def create_checkpoint_table() -> None:
    query = """
    CREATE TABLE IF NOT EXISTS processing_checkpoints (
        checkpoint_name VARCHAR(50) PRIMARY KEY,
        last_processed_timestamp TIMESTAMP WITH TIME ZONE,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    );
    """
    execute_query(query)
    logger.info("Checkpoint table created successfully.")


def get_latest_checkpoint(checkpoint_name: str = "item_prices_ingestion") -> str | None:
    query = """
    SELECT last_processed_timestamp
    FROM processing_checkpoints
    WHERE checkpoint_name = %s;
    """
    result = execute_query(query, (checkpoint_name,))

    if result and result[0][0]:
        return result[0][0].isoformat()
    return None


def update_checkpoint(checkpoint_name: str, timestamp: str) -> None:
    query = """
    INSERT INTO processing_checkpoints (checkpoint_name, last_processed_timestamp, updated_at)
    VALUES (%s, %s, CURRENT_TIMESTAMP)
    ON CONFLICT (checkpoint_name) DO UPDATE SET
        last_processed_timestamp = EXCLUDED.last_processed_timestamp,
        updated_at = CURRENT_TIMESTAMP;
    """
    execute_query(query, (checkpoint_name, timestamp))
    logger.info("Checkpoint '%s' updated to %s", checkpoint_name, timestamp)


def database_setup() -> None:
    #check_database_encoding() # used this to make sure the currency symbols would look right in the db.
    create_currency_table()  # this table must be created first because of foreign key constraint
    create_currency_conversion_rates_table("NOK")
    create_item_prices_table()
    create_checkpoint_table()


def main() -> None:
    try:
        database_setup()
    except Exception:
        logger.exception("Database setup failed")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    main()
