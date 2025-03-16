import logging
from datetime import UTC, datetime
from typing import Any

import requests

from database_manager import batch_insert

logger = logging.getLogger(__name__)

# Constants
HTTP_OK = 200
REQUEST_TIMEOUT = 10  # seconds


def get_currencies() -> dict[str, dict[str, str]] | None:
    url = "https://api.vatcomply.com/currencies"
    try:
        response = requests.get(url, timeout=REQUEST_TIMEOUT)

        if response.status_code == HTTP_OK:
            return response.json()
        logger.error("Request failed with status code %s", response.status_code)
        return None
    except requests.RequestException:
        logger.exception("Error retrieving currencies")
        return None


def get_currency_conversion_rates(currency_code: str, date: str | None = None) -> dict[str, Any] | None:
    if date is None:
        date = datetime.now(tz=UTC).date().strftime("%Y-%m-%d")

    url = f"https://api.vatcomply.com/rates?base={currency_code}&date={date}"
    try:
        response = requests.get(url, timeout=REQUEST_TIMEOUT)

        if response.status_code == HTTP_OK:
            return response.json()
        logger.error("Request failed with status code %s", response.status_code)
        return None
    except requests.RequestException:
        logger.exception("Error retrieving conversion rates")
        return None


def insert_currencies() -> int:
    inserted_count = 0

    currencies_data = get_currencies()

    if currencies_data is None:
        logger.error("Failed to retrieve currency data")
        return inserted_count

    try:
        batch_size = 50
        currency_items = list(currencies_data.items())
        total_items = len(currency_items)

        logger.info("Processing %s currencies in batches of %s", total_items, batch_size)

        query_template = """
        INSERT INTO currencies (currency_code, currency_name, currency_symbol)
        VALUES (%s, %s, %s)
        ON CONFLICT (currency_code)
        DO UPDATE SET
            currency_name = EXCLUDED.currency_name,
            currency_symbol = EXCLUDED.currency_symbol
        RETURNING currency_code;
        """

        for i in range(0, total_items, batch_size):
            batch_end = min(i + batch_size, total_items)
            batch = currency_items[i:batch_end]

            logger.info(
                "Processing batch %s (%s to %s) of %s",
                i // batch_size + 1,
                i,
                batch_end - 1,
                (total_items - 1) // batch_size + 1,
            )

            # Create a list of parameter tuples for the batch
            params_list = []
            for currency_code, currency_info in batch:
                currency_name = currency_info.get("name", "")
                currency_symbol = currency_info.get("symbol", "")
                params_list.append((currency_code, currency_name, currency_symbol))

            # Execute all inserts in the batch with a single database operation
            try:
                batch_rows_inserted = batch_insert(query_template, params_list)
                inserted_count += batch_rows_inserted
                logger.info("Batch inserted %s rows", batch_rows_inserted)
            except Exception:
                logger.exception("Error inserting batch")

        logger.info("Successfully stored/updated %s currencies in the database.", inserted_count)
        return inserted_count
    except Exception:
        logger.exception("Error during currency insertion")
        return inserted_count


def insert_rates(rates_data: dict[str, Any] | None = None) -> int:
    inserted_count = 0

    if rates_data is None:
        logger.error("No rates data provided")
        return inserted_count

    date = rates_data.get("date")
    rates = rates_data.get("rates", {})

    try:
        # Process in batches for better performance
        batch_size = 50
        rate_items = list(rates.items())
        total_items = len(rate_items)

        # Prepare the query template once outside the loop - use parameterized table name
        query_template = """
        INSERT INTO currency_conversion_rates_base_NOK (currency_code, rate, last_updated_at)
        VALUES (%s, %s, %s)
        ON CONFLICT (currency_code)
        DO UPDATE SET
            rate = EXCLUDED.rate,
            last_updated_at = EXCLUDED.last_updated_at
        RETURNING currency_code;
        """

        for i in range(0, total_items, batch_size):
            batch_end = min(i + batch_size, total_items)
            batch = rate_items[i:batch_end]

            logger.info(
                "Processing batch %s (%s to %s) of %s",
                i // batch_size + 1,
                i,
                batch_end - 1,
                (total_items - 1) // batch_size + 1,
            )

            # Create a list of parameter tuples for the batch
            params_list = []
            for currency_code, rate in batch:
                params_list.append((currency_code, rate, date))

            # Execute all inserts in the batch with a single database operation
            try:
                batch_rows_inserted = batch_insert(query_template, params_list)
                inserted_count += batch_rows_inserted
                logger.info("Batch inserted %s rows", batch_rows_inserted)
            except Exception:
                logger.exception("Error inserting batch")

        logger.info("Successfully stored/updated %s rates in the database.", inserted_count)
        return inserted_count
    except Exception:
        logger.exception("Error during rates insertion")
        return inserted_count


def main() -> None:
    logger.info("Inserting currencies into the database...")
    currency_count = insert_currencies()

    logger.info("Inserting rates into the database...")
    rates = get_currency_conversion_rates("NOK")
    rates_count = insert_rates(rates)

    logger.info(
        "Summary: Inserted/updated %s currencies and %s conversion rates.",
        currency_count,
        rates_count,
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    main()
