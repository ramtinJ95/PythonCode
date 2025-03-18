import os
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
import psycopg2
from psycopg2.extensions import connection

import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.database_manager import (
    execute_query,
    batch_insert,
    update_checkpoint,
    get_latest_checkpoint,
)


@pytest.fixture
def mock_connection():
    mock_conn = MagicMock(spec=connection)
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    return mock_conn, mock_cursor


@pytest.fixture
def mock_env_vars(monkeypatch):
    monkeypatch.setenv("DB_HOST", "test_host")
    monkeypatch.setenv("DB_PORT", "5432")
    monkeypatch.setenv("DB_NAME", "test_db")
    monkeypatch.setenv("DB_USER", "test_user")
    monkeypatch.setenv("DB_PASSWORD", "test_password")


def test_execute_query_non_select(mock_connection):
    mock_conn, mock_cursor = mock_connection
    mock_cursor.fetchall.side_effect = psycopg2.ProgrammingError("no results to fetch")

    with patch("src.database_manager.get_db_connection", return_value=mock_conn):
        result = execute_query("INSERT INTO test_table VALUES ('test')")

        mock_cursor.execute.assert_called_once_with("INSERT INTO test_table VALUES ('test')", None)
        mock_conn.commit.assert_called_once()
        assert result is None
        mock_conn.close.assert_called_once()


def test_batch_insert(mock_connection):
    mock_conn, mock_cursor = mock_connection
    mock_cursor.rowcount = 3
    params_list = [
        ("item1", 10.5, "USD"),
        ("item2", 20.0, "EUR"),
        ("item3", 30.0, "NOK"),
    ]

    with patch("src.database_manager.get_db_connection", return_value=mock_conn):
        with patch("src.database_manager.execute_values") as mock_execute_values:
            rows = batch_insert("INSERT INTO items (name, price, currency) VALUES (%s, %s, %s)", params_list)

            assert rows == 3
            mock_execute_values.assert_called_once()
            mock_conn.commit.assert_called_once()
            mock_conn.close.assert_called_once()


def test_batch_insert_empty_list(mock_connection):
    mock_conn, _ = mock_connection

    with patch("src.database_manager.get_db_connection", return_value=mock_conn):
        rows = batch_insert("INSERT INTO test_table VALUES (%s)", [])

        assert rows == 0
        mock_conn.close.assert_not_called()


def test_update_and_get_checkpoint(mock_connection):
    mock_conn, mock_cursor = mock_connection
    test_timestamp = datetime.now(timezone.utc)
    test_timestamp_str = test_timestamp.isoformat()

    mock_cursor.fetchall.return_value = [(test_timestamp,)]

    with patch("src.database_manager.get_db_connection", return_value=mock_conn):
        update_checkpoint("test_checkpoint", test_timestamp_str)

        mock_cursor.execute.assert_called_once()
        call_args = mock_cursor.execute.call_args[0]
        assert "INSERT INTO processing_checkpoints" in call_args[0]
        assert "ON CONFLICT (checkpoint_name) DO UPDATE" in call_args[0]
        assert call_args[1][0] == "test_checkpoint"
        assert call_args[1][1] == test_timestamp_str

        mock_conn.commit.assert_called_once()

        mock_cursor.reset_mock()
        mock_conn.reset_mock()

        checkpoint = get_latest_checkpoint("test_checkpoint")

        mock_cursor.execute.assert_called_once()
        call_args = mock_cursor.execute.call_args[0]
        assert "SELECT last_processed_timestamp" in call_args[0]
        assert "FROM processing_checkpoints" in call_args[0]
        assert "WHERE checkpoint_name = %s" in call_args[0]
        assert call_args[1][0] == "test_checkpoint"

        assert checkpoint is not None
        assert test_timestamp in checkpoint
