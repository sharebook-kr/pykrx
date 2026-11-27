import datetime
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


D1, D2 = datetime.date(2021, 1, 18), datetime.date(2021, 1, 26)


def test_index_portfolio_deposit_file_accepts_date():
    with patch.object(stock.stock_api.krx, "get_index_portfolio_deposit_file") as mock:
        stock.get_index_portfolio_deposit_file("1001", D1)
        mock.assert_called_once_with("20210118", "1001")


@pytest.mark.parametrize("freq", ["m", "y"])
def test_index_ohlcv_by_date_keeps_full_fromdate(freq):
    with patch.object(stock.stock_api.krx, "get_index_ohlcv_by_date") as mock:
        mock.side_effect = RuntimeError  # 호출 인자만 확인한다
        with pytest.raises(RuntimeError):
            stock.get_index_ohlcv_by_date(D1, D2, "1001", freq=freq)
        mock.assert_called_once_with("20210118", "20210126", "1001")


def test_index_fundamental_by_date_accepts_date():
    with patch.object(stock.stock_api.krx, "get_index_fundamental_by_date") as mock:
        stock.get_index_fundamental_by_date(D1, D2, "1001")
        mock.assert_called_once_with("20210118", "20210126", "1001")


@pytest.mark.parametrize(
    "extra", [(), ("580011",), ("580011", "금액"), ("580011", "금액", "순매수")]
)
def test_etf_trading_volume_and_value_accepts_date(extra):
    names = [
        "get_trading_volume_and_value_by_investor",
        "get_indivisual_trading_volume_and_value_by_investor",
        "get_trading_volume_and_value_by_date",
        "get_indivisual_trading_volume_and_value_by_date",
    ]
    mocks = [patch.object(stock.stock_api.krx, n) for n in names]
    for m in mocks:
        m.start()
    try:
        stock.get_etf_trading_volume_and_value(D1, "20210126", *extra)
        stock.get_etf_trading_volume_and_value("20210118", D2, *extra)
    finally:
        for m in mocks:
            m.stop()
