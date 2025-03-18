import logging
import re
from pathlib import Path

import pandas as pd

from src.database_manager import batch_insert, get_latest_checkpoint, update_checkpoint

logger = logging.getLogger(__name__)


def natural_sort_key(s: str) -> list[str]:
    return [int(text) if text.isdigit() else text.lower() for text in re.split(r"(\d+)", s)]


def get_csv_files_in_order(directory: str = "data") -> list[str]:
    # project root directory (one level up from the src directory)
    project_root = Path(__file__).parent.parent
    data_dir = project_root / directory

    if not data_dir.exists():
        logger.error("Directory %s does not exist", data_dir)
        return []

    csv_files = [f.name for f in data_dir.iterdir() if f.is_file() and f.suffix.lower() == ".csv"]
    csv_files.sort(key=natural_sort_key)

    return [str(data_dir / file) for file in csv_files]


def parse_csv_file(
    file_path: str, checkpoint_timestamp: str | None = None, human_readable_dt: str | None = None
) -> pd.DataFrame:
    logger.info("Parsing file: %s", file_path)

    try:
        data_frame = pd.read_csv(file_path)

        if checkpoint_timestamp:
            logger.info("Filtering rows with system_timestamp > %s", human_readable_dt)
            checkpoint_dt = pd.to_datetime(checkpoint_timestamp)
            data_frame["system_timestamp"] = pd.to_datetime(data_frame["system_timestamp"])
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

    try:
        query_template = """
        INSERT INTO item_prices
        (id, item, price, currency, created_at, updated_at, system_timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id, system_timestamp) DO NOTHING;
        """

        batch_size = 100
        rows_inserted = 0
        latest_timestamp = None

        for batch_df in [data_frame[i : i + batch_size] for i in range(0, len(data_frame), batch_size)]:
            params_list = [tuple(row) for row in batch_df.values]

            try:
                batch_rows_inserted = batch_insert(query_template, params_list)
                rows_inserted += batch_rows_inserted
                logger.info("Batch inserted %s rows", batch_rows_inserted)

                batch_max_timestamp = batch_df["system_timestamp"].max()
                if latest_timestamp is None or batch_max_timestamp > latest_timestamp:
                    latest_timestamp = batch_max_timestamp

            except Exception:
                logger.exception("Error inserting batch")

        logger.info("Successfully inserted %s rows into the database", rows_inserted)

        if latest_timestamp is not None:
            update_checkpoint(
                "item_prices_ingestion",
                latest_timestamp if isinstance(latest_timestamp, str) else latest_timestamp.isoformat(),
            )

    except Exception:
        logger.exception("Error inserting data into database")


def load_csv_files() -> None:
    try:
        logger.info("Starting CSV processing")

        file_paths = get_csv_files_in_order()
        logger.info("Found %s CSV files to process", len(file_paths))

        for file_path in file_paths:
            # Get the latest checkpoint before processing each file
            checkpoint_result = get_latest_checkpoint("item_prices_ingestion")
            datetime_object = None
            checkpoint_timestamp = None
            human_readable = None

            if checkpoint_result:
                datetime_object, checkpoint_timestamp = checkpoint_result
                human_readable = datetime_object.astimezone().strftime("%Y-%m-%d %H:%M:%S")
                logger.info("Processing file %s with checkpoint: %s", file_path, human_readable)
            else:
                logger.info("Processing file %s with no checkpoint", file_path)

            data_frame = parse_csv_file(file_path, checkpoint_timestamp, human_readable)
            if not data_frame.empty:
                insert_data_into_database(data_frame)

        logger.info("CSV processing completed successfully")
    except Exception:
        logger.exception("Error during CSV processing")


def main() -> None:
    load_csv_files()


if __name__ == "__main__":
    main()
