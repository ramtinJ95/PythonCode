import os
import sys
import pandas as pd
import pytest
from unittest.mock import MagicMock, patch

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

sys.modules["database_manager"] = MagicMock()

from src.load_csv import (
    natural_sort_key,
    insert_data_into_database,
)


@pytest.fixture
def mock_dataframe_with_timestamps():
    return pd.DataFrame(
        {
            "id": [1, 2, 3],
            "item": ["item1", "item2", "item3"],
            "price": [10.0, 20.0, 30.0],
            "currency": ["USD", "USD", "USD"],
            "created_at": ["2023-01-01", "2023-01-02", "2023-01-03"],
            "updated_at": ["2023-01-01", "2023-01-02", "2023-01-03"],
            "system_timestamp": ["2023-01-01T00:00:00", "2023-01-02T00:00:00", "2023-01-03T00:00:00"],
        }
    )


def test_natural_sort_key_mixed_content():
    assert natural_sort_key("abc123def") == ["abc", 123, "def"]
    assert natural_sort_key("abc") == ["abc"]
    assert natural_sort_key("123") == ["", 123, ""]


def test_natural_sort_key_for_sorting():
    files = ["file10.csv", "file1.csv", "file2.csv", "file20.csv"]
    sorted_files = sorted(files, key=natural_sort_key)
    assert sorted_files == ["file1.csv", "file2.csv", "file10.csv", "file20.csv"]


def test_insert_empty_dataframe():
    with patch("src.load_csv.batch_insert") as mock_batch_insert:
        insert_data_into_database(pd.DataFrame())
        mock_batch_insert.assert_not_called()


@patch("src.load_csv.batch_insert")
@patch("src.load_csv.update_checkpoint")
def test_insert_data_into_database(mock_update_checkpoint, mock_batch_insert, mock_dataframe_with_timestamps):
    mock_batch_insert.return_value = 3  # 3 rows inserted

    insert_data_into_database(mock_dataframe_with_timestamps)

    # Check that batch_insert was called with the correct parameters
    assert mock_batch_insert.call_count == 1

    # Check that update_checkpoint was called with the latest timestamp
    mock_update_checkpoint.assert_called_once_with("item_prices_ingestion", "2023-01-03T00:00:00")
