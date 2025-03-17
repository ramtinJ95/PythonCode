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
            table_and_columns = query.split("VALUES")[0].strip()

            new_query = f"{table_and_columns} VALUES %s"

            if "ON CONFLICT" in query:
                on_conflict_clause = query.split("ON CONFLICT")[1].strip()
                new_query = f"{new_query} ON CONFLICT {on_conflict_clause}"

            try:
                execute_values(cursor, new_query, params_list, template=None, page_size=100)
                rows_affected = cursor.rowcount
                if rows_affected < len(params_list):
                    logger.info(
                        "Some rows were skipped due to conflicts. Attempted: %s, Inserted: %s",
                        len(params_list),
                        rows_affected,
                    )
                    # checks which rows were skipped
                    for params in params_list:
                        check_query = """
                        SELECT COUNT(*) FROM item_prices 
                        WHERE id = %s AND system_timestamp = %s
                        """
                        cursor.execute(check_query, (params[0], params[6]))  # id and system_timestamp
                        count = cursor.fetchone()[0]
                        if count > 0:
                            logger.info("Row with id=%s and system_timestamp=%s already exists", params[0], params[6])
            except Exception as e:
                logger.error("Error during batch insert: %s", str(e))
                raise
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
        last_processed_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    );
    """
    execute_query(query)
    logger.info("Checkpoint table created successfully.")


def update_checkpoint(checkpoint_name: str, timestamp: str) -> None:
    query = """
    INSERT INTO processing_checkpoints (checkpoint_name, last_processed_timestamp, updated_at)
    VALUES (%s, %s, CURRENT_TIMESTAMP)
    ON CONFLICT (checkpoint_name) DO UPDATE SET
        last_processed_timestamp = EXCLUDED.last_processed_timestamp,
        updated_at = CURRENT_TIMESTAMP;
    """
    # Log the exact timestamp being stored
    logger.info("Updating checkpoint '%s' with timestamp: %s", checkpoint_name, timestamp)
    execute_query(query, (checkpoint_name, timestamp))
    logger.info("Checkpoint '%s' updated successfully", checkpoint_name)


def get_latest_checkpoint(checkpoint_name: str) -> str | None:
    query = """
    SELECT last_processed_timestamp 
    FROM processing_checkpoints 
    WHERE checkpoint_name = %s;
    """
    result = execute_query(query, (checkpoint_name,))
    if not result:
        return None

    return result[0][0], result[0][0].isoformat()  # Return ISO formatted timestamp string that pandas can parse


def create_nok_prices_view() -> None:
    query = """
    CREATE OR REPLACE VIEW transaction_prices_in_nok AS
    WITH latest_rates AS (
        SELECT 
            ip.id, 
            ip.item, 
            ip.price as original_price, 
            ip.currency as original_currency, 
            rate as latest_rate, 
            last_updated_at as rate_valid_date, 
            ip.created_at, 
            ip.updated_at,
            ip.system_timestamp
        FROM item_prices as ip
        LEFT JOIN currency_conversion_rates_base_nok as cr 
            ON cr.currency_code = ip.currency
    ),
    calculate_converted_price AS (
        SELECT 
            id, 
            item, 
            original_price, 
            original_currency, 
            latest_rate, 
            rate_valid_date,
            ROUND(original_price / latest_rate, 2) as price_in_nok
        FROM latest_rates
        ORDER BY id, system_timestamp
    )
    SELECT * FROM calculate_converted_price;
    """
    execute_query(query)
    logger.info("NOK prices view created successfully.")


# If I was to prepare this data for forecasting, then a scd2 view/table like this would be my approach
# Then I would fetch all historical data for exchange rates and get proper historical data for the prices
# But that would be too much and a bit overkill for this case I think.
def create_item_prices_scd2_view() -> None:
    query = """
    CREATE OR REPLACE VIEW item_prices_scd2 AS
    WITH ranked_changes AS (
        SELECT 
            id,
            item,
            price,
            currency,
            created_at,
            updated_at,
            system_timestamp as valid_from,
            LEAD(system_timestamp) OVER (
                PARTITION BY id 
                ORDER BY system_timestamp
            ) as valid_to
        FROM item_prices
    )
    SELECT 
        id,
        item,
        price,
        currency,
        created_at,
        updated_at,
        valid_from,
        COALESCE(valid_to, '9999-12-31'::timestamp with time zone) as valid_to,
        CASE 
            WHEN valid_to IS NULL THEN true 
            ELSE false 
        END as is_current
    FROM ranked_changes
    ORDER BY id, valid_from;
    """
    execute_query(query)
    logger.info("Item prices SCD2 view created successfully.")


def reset_database() -> None:
    """Drop and recreate all tables."""
    queries = [
        "DROP VIEW IF EXISTS transaction_prices_in_nok CASCADE;",
        "DROP TABLE IF EXISTS processing_checkpoints CASCADE;",
        "DROP TABLE IF EXISTS item_prices CASCADE;",
        "DROP TABLE IF EXISTS currency_conversion_rates_base_NOK CASCADE;",
        "DROP TABLE IF EXISTS currencies CASCADE;",
    ]

    for query in queries:
        execute_query(query)


def database_setup() -> None:
    # check_database_encoding() # used this to make sure the currency symbols would look right in the db.
    create_currency_table()  # this table must be created first because of foreign key constraint
    create_currency_conversion_rates_table("NOK")
    create_item_prices_table()
    create_checkpoint_table()
    create_nok_prices_view()


def main() -> None:
    try:
        database_setup()
    except Exception:
        logger.exception("Database setup failed")


if __name__ == "__main__":
    main()
