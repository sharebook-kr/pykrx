from unittest.mock import patch

import pytest

from pykrx import stock

DISPATCHERS = [
    ("get_market_ohlcv", "get_market_ohlcv_by_date", "get_market_ohlcv_by_ticker"),
    ("get_market_cap", "get_market_cap_by_date", "get_market_cap_by_ticker"),
    (
        "get_exhaustion_rates_of_foreign_investment",
        "get_exhaustion_rates_of_foreign_investment_by_date",
        "get_exhaustion_rates_of_foreign_investment_by_ticker",
    ),
    (
        "get_market_fundamental",
        "get_market_fundamental_by_date",
        "get_market_fundamental_by_ticker",
    ),
    ("get_index_ohlcv", "get_index_ohlcv_by_date", "get_index_ohlcv_by_ticker"),
    (
        "get_index_fundamental",
        "get_index_fundamental_by_date",
        "get_index_fundamental_by_ticker",
    ),
    (
        "get_shorting_balance",
        "get_shorting_balance_by_date",
        "get_shorting_balance_by_ticker",
    ),
]


@pytest.mark.parametrize("name, by_date, by_ticker", DISPATCHERS)
def test_date_range_dispatch(name, by_date, by_ticker):
    func = getattr(stock, name)
    with (
        patch.object(stock.stock_api, by_date) as date_mock,
        patch.object(stock.stock_api, by_ticker) as ticker_mock,
    ):
        func("20210118", "20210126", "005930")
        func("20210118", todate="20210126", ticker="005930")
        func(fromdate="20210118", todate="20210126", ticker="005930")
        assert date_mock.call_count == 3
        ticker_mock.assert_not_called()

        func("20210118")
        func(date="20210118")
        assert ticker_mock.call_count == 2
        assert date_mock.call_count == 3


def test_market_price_change_accepts_mixed_date_args():
    with patch.object(stock.stock_api, "get_market_price_change_by_ticker") as mock:
        stock.get_market_price_change("20210118", todate="20210126")
        mock.assert_called_once_with("20210118", todate="20210126")
