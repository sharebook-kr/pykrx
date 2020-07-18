import unittest
from pykrx import stock


class KrxIndexBasicTest(unittest.TestCase):
    def test_not_empty_result(self):
        df = stock.get_index_ohlcv_by_date("20190101", "20190228", "코스피 200")
        self.assertNotEqual(df.empty, True)

        df = stock.get_index_ohlcv_by_date("20190101", "20190228", "코스닥 150")
        self.assertNotEqual(df.empty, True)

        df = stock.get_index_status_by_group("20190228", "KOSPI")
        self.assertNotEqual(df.empty, True)

        df = stock.get_index_status_by_group("20190228", "KOSDAQ")
        self.assertNotEqual(df.empty, True)


class IndexTickerListTest(unittest.TestCase):
    def test_io_with_default_param(self):
        tickers = stock.get_index_ticker_list()
        self.assertIsInstance(tickers, list)
        self.assertNotEqual(len(tickers), 0)

    def test_io_with_business_date(self):
        tickers = stock.get_market_ticker_list("20170717")
        self.assertIsInstance(tickers, list)
        self.assertNotEqual(len(tickers), 0)


if __name__ == '__main__':
    unittest.main()
