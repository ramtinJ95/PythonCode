import os
from unittest.mock import patch

import pytest
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.get_currencies_and_rates import (
    insert_currencies,
    insert_rates,
    get_currencies_and_rates,
)


@pytest.fixture
def mock_currencies_data():
    return {
        "USD": {"name": "US Dollar", "symbol": "$"},
        "EUR": {"name": "Euro", "symbol": "€"},
        "NOK": {"name": "Norwegian Krone", "symbol": "kr"},
    }


@pytest.fixture
def mock_rates_data():
    return {
        "base": "NOK",
        "date": "2023-03-16",
        "rates": {
            "USD": 0.095,
            "EUR": 0.087,
            "GBP": 0.074,
        },
    }


def test_insert_currencies_success(mock_currencies_data):
    with patch("src.get_currencies_and_rates.get_currencies", return_value=mock_currencies_data):
        with patch("src.get_currencies_and_rates.batch_insert") as mock_batch_insert:
            mock_batch_insert.return_value = 3

            result = insert_currencies()

            assert result == 3
            mock_batch_insert.assert_called_once()

            query = mock_batch_insert.call_args[0][0]
            assert "ON CONFLICT (currency_code)" in query
            assert "DO UPDATE SET" in query

            params_list = mock_batch_insert.call_args[0][1]
            assert len(params_list) == 3
            assert ("USD", "US Dollar", "$") in params_list
            assert ("EUR", "Euro", "€") in params_list
            assert ("NOK", "Norwegian Krone", "kr") in params_list


def test_insert_currencies_batch_processing():
    mock_data = {}
    for i in range(1, 102):
        currency_code = f"CUR{i}"
        mock_data[currency_code] = {"name": f"Currency {i}", "symbol": f"${i}"}

    with patch("src.get_currencies_and_rates.get_currencies", return_value=mock_data):
        with patch("src.get_currencies_and_rates.batch_insert") as mock_batch_insert:
            mock_batch_insert.side_effect = [50, 50, 1]

            result = insert_currencies()

            assert result == 101

            assert mock_batch_insert.call_count == 3

            first_batch_params = mock_batch_insert.call_args_list[0][0][1]
            assert len(first_batch_params) == 50

            second_batch_params = mock_batch_insert.call_args_list[1][0][1]
            assert len(second_batch_params) == 50

            third_batch_params = mock_batch_insert.call_args_list[2][0][1]
            assert len(third_batch_params) == 1


def test_insert_rates_success(mock_rates_data):
    with patch("src.get_currencies_and_rates.batch_insert") as mock_batch_insert:
        mock_batch_insert.return_value = 3

        result = insert_rates(mock_rates_data)

        assert result == 3
        mock_batch_insert.assert_called_once()

        query = mock_batch_insert.call_args[0][0]
        assert "ON CONFLICT (currency_code)" in query
        assert "DO UPDATE SET" in query

        params_list = mock_batch_insert.call_args[0][1]
        assert len(params_list) == 3
        assert ("USD", 0.095, "2023-03-16") in params_list
        assert ("EUR", 0.087, "2023-03-16") in params_list
        assert ("GBP", 0.074, "2023-03-16") in params_list


def test_insert_rates_batch_processing():
    mock_rates = {}
    for i in range(1, 102):
        currency_code = f"CUR{i}"
        mock_rates[currency_code] = 0.01 * i

    mock_data = {"base": "NOK", "date": "2023-03-16", "rates": mock_rates}

    with patch("src.get_currencies_and_rates.batch_insert") as mock_batch_insert:
        mock_batch_insert.side_effect = [50, 50, 1]  # 3 batches: 50 + 50 + 1 = 101

        result = insert_rates(mock_data)

        assert result == 101

        assert mock_batch_insert.call_count == 3

        first_batch_params = mock_batch_insert.call_args_list[0][0][1]
        assert len(first_batch_params) == 50

        second_batch_params = mock_batch_insert.call_args_list[1][0][1]
        assert len(second_batch_params) == 50

        third_batch_params = mock_batch_insert.call_args_list[2][0][1]
        assert len(third_batch_params) == 1

        # Verify the parameters format (currency_code, rate, date)
        for param in first_batch_params:
            assert len(param) == 3
            assert isinstance(param[0], str)
            assert isinstance(param[1], float)
            assert param[2] == "2023-03-16"
