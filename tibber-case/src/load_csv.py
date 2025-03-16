import logging
import re
from pathlib import Path

import pandas as pd

from database_manager import batch_insert, get_latest_checkpoint, update_checkpoint

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def natural_sort_key(s: str) -> list[str]:
    return [int(text) if text.isdigit() else text.lower() for text in re.split(r"(\d+)", s)]


def get_csv_files_in_order(directory: str = "data") -> list[str]:
    data_dir = Path(directory)

    if not data_dir.exists():
        logger.error("Directory %s does not exist", directory)
        return []

    csv_files = [f.name for f in data_dir.iterdir() if f.is_file() and f.suffix.lower() == ".csv"]
    csv_files.sort(key=natural_sort_key)

    return [str(data_dir / file) for file in csv_files]


def parse_csv_file(file_path: str, checkpoint_timestamp: str | None = None) -> pd.DataFrame:
    logger.info("Parsing file: %s", file_path)

    try:
        data_frame = pd.read_csv(file_path)

        # Filter rows based on checkpoint if provided
        if checkpoint_timestamp:
            logger.info("Filtering rows with system_timestamp > %s", checkpoint_timestamp)
            # Convert checkpoint_timestamp to datetime for comparison
            checkpoint_dt = pd.to_datetime(checkpoint_timestamp)
            # Convert system_timestamp column to datetime
            data_frame["system_timestamp"] = pd.to_datetime(data_frame["system_timestamp"])
            # Filter rows with system_timestamp greater than checkpoint
            original_count = len(data_frame)
            data_frame = data_frame[data_frame["system_timestamp"] > checkpoint_dt]
            filtered_count = len(data_frame)
            logger.info(
                "Filtered out %s rows, keeping %s rows",
                original_count - filtered_count,
                filtered_count,
            )

        logger.info("Parsed %s rows from %s", len(data_frame), file_path)
        return data_frame
    except Exception:
        logger.exception("Error reading file %s", file_path)
        return pd.DataFrame()


def insert_data_into_database(data_frame: pd.DataFrame) -> None:
    if data_frame.empty:
        logger.warning("No data to insert into database")
        return

    rows_inserted = 0
    latest_timestamp = None

    try:
        # For a truly incremental approach, we insert all records
        # The unique constraint on (id, system_timestamp) prevents exact duplicates
        query_template = """
        INSERT INTO item_prices
        (id, item, price, currency, created_at, updated_at, system_timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id, system_timestamp) DO NOTHING;
        """

        # Process in batches of 100 for better performance
        batch_size = 100
        total_rows = len(data_frame)

        for i in range(0, total_rows, batch_size):
            batch_end = min(i + batch_size, total_rows)
            batch = data_frame.iloc[i:batch_end]

            # Avoid line length issues by breaking up the log message
            batch_num = i // batch_size + 1
            total_batches = (total_rows - 1) // batch_size + 1
            logger.info(
                "Processing batch %s (%s to %s) of %s",
                batch_num,
                i,
                batch_end - 1,
                total_batches,
            )

            # Create a list of parameter tuples for the batch
            params_list = []
            current_batch_latest_timestamp = None

            for _, row in batch.iterrows():
                params = (
                    row["id"],
                    row["item"],
                    row["price"],
                    row["currency"],
                    row["created_at"],
                    row["updated_at"],
                    row["system_timestamp"],
                )

                params_list.append(params)

                # Keep track of the latest timestamp in this batch
                if current_batch_latest_timestamp is None or row["system_timestamp"] > current_batch_latest_timestamp:
                    current_batch_latest_timestamp = row["system_timestamp"]

            # Execute all inserts in the batch with a single database operation
            try:
                batch_rows_inserted = batch_insert(query_template, params_list)
                rows_inserted += batch_rows_inserted
                logger.info("Batch inserted %s rows", batch_rows_inserted)

                # Update the overall latest timestamp
                if latest_timestamp is None or current_batch_latest_timestamp > latest_timestamp:
                    latest_timestamp = current_batch_latest_timestamp

            except Exception:
                logger.exception("Error inserting batch")

        logger.info("Successfully inserted %s rows into the database", rows_inserted)

        # Update the checkpoint with the latest timestamp if we inserted any rows
        if latest_timestamp is not None:
            # Check if latest_timestamp is already a string
            if isinstance(latest_timestamp, str):
                update_checkpoint("item_prices_ingestion", latest_timestamp)
            else:
                update_checkpoint("item_prices_ingestion", latest_timestamp.isoformat())

    except Exception:
        logger.exception("Error inserting data into database")


def main() -> None:
    try:
        logger.info("Starting CSV processing")

        # Get the latest checkpoint
        checkpoint_timestamp = get_latest_checkpoint("item_prices_ingestion")
        if checkpoint_timestamp:
            logger.info("Found checkpoint: %s", checkpoint_timestamp)
        else:
            logger.info("No checkpoint found, will process all data")

        file_paths = get_csv_files_in_order()
        logger.info("Found %s CSV files to process", len(file_paths))

        for file_path in file_paths:
            data_frame = parse_csv_file(file_path, checkpoint_timestamp)
            if not data_frame.empty:
                insert_data_into_database(data_frame)

        logger.info("CSV processing completed successfully")
    except Exception:
        logger.exception("Error during CSV processing")


if __name__ == "__main__":
    main()
