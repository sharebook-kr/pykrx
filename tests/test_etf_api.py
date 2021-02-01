import unittest
from pykrx import stock
import pandas as pd
import numpy as np


class EtfTickerList(unittest.TestCase):
    def test_ticker_list(self):
        tickers = stock.get_etf_ticker_list()
        self.assertIsInstance(tickers, list)
        self.assertGreater(len(tickers), 0)

    def test_ticker_list_with_a_businessday(self):
        tickers = stock.get_etf_ticker_list("20210104")
        self.assertIsInstance(tickers, list)
        self.assertGreater(len(tickers), 0)

    def test_ticker_list_with_a_holiday(self):
        tickers = stock.get_etf_ticker_list("20210103")
        self.assertIsInstance(tickers, list)
        self.assertGreater(len(tickers), 0)


class EtnTickerList(unittest.TestCase):
    def test_ticker_list(self):
        tickers = stock.get_etn_ticker_list()
        self.assertIsInstance(tickers, list)
        self.assertGreater(len(tickers), 0)

    def test_ticker_list_with_a_businessday(self):
        tickers = stock.get_etn_ticker_list("20210104")
        self.assertIsInstance(tickers, list)
        self.assertGreater(len(tickers), 0)

    def test_ticker_list_with_a_holiday(self):
        tickers = stock.get_etn_ticker_list("20210103")
        self.assertIsInstance(tickers, list)
        self.assertGreater(len(tickers), 0)


class ElwTickerList(unittest.TestCase):
    def test_ticker_list(self):
        tickers = stock.get_elw_ticker_list()
        self.assertIsInstance(tickers, list)
        self.assertGreater(len(tickers), 0)

    def test_ticker_list_with_a_businessday(self):
        tickers = stock.get_elw_ticker_list("20210104")
        self.assertIsInstance(tickers, list)
        self.assertGreater(len(tickers), 0)

    def test_ticker_list_with_a_holiday(self):
        tickers = stock.get_elw_ticker_list("20210103")
        self.assertIsInstance(tickers, list)
        self.assertGreater(len(tickers), 0)

class EtfOhlcvByDate(unittest.TestCase):
    def test_with_business_day(self):
        df = stock.get_etf_ohlcv_by_date("20210104", "20210108", "292340")
        #                 NAV  시가  고가  저가  종가 거래량    거래대금     기초지수
        # 날짜
        # 2021-01-04  9737.23  9730  9730  9730  9730     81      788130  1303.290039
        # 2021-01-05  9756.27  9705  9990  9700  9770      6       58845  1306.589966
        # 2021-01-06  9796.98     0     0     0  9770      0           0  1306.760010
        # 2021-01-07  9723.65  9845  9855  9845  9855      2       19700  1301.650024
        # 2021-01-08  9771.73  9895  9900  9855  9885      6       59320  1306.729980
        temp = df.iloc[0:5, 0] == np.array([9737.23, 9756.27, 9796.98, 9723.65, 9771.73])
        self.assertEqual(temp.sum(), 5)
        self.assertIsInstance(df.index   , pd.core.indexes.datetimes.DatetimeIndex)
        self.assertIsInstance(df.index[0], pd._libs.tslibs.timestamps.Timestamp)
        self.assertTrue(df.index[0] < df.index[-1])

    def test_with_holiday_0(self):
        df = stock.get_etf_ohlcv_by_date("20210103", "20210108", "292340")
        temp = df.iloc[0:5, 0] == np.array([9737.23, 9756.27, 9796.98, 9723.65, 9771.73])
        self.assertEqual(temp.sum(), 5)
        self.assertIsInstance(df.index   , pd.core.indexes.datetimes.DatetimeIndex)
        self.assertIsInstance(df.index[0], pd._libs.tslibs.timestamps.Timestamp)
        self.assertTrue(df.index[0] < df.index[-1])

    def test_with_holiday_1(self):
        df = stock.get_etf_ohlcv_by_date("20210103", "20210109", "292340")
        temp = df.iloc[0:5, 0] == np.array([9737.23, 9756.27, 9796.98, 9723.65, 9771.73])
        self.assertEqual(temp.sum(), 5)
        self.assertIsInstance(df.index   , pd.core.indexes.datetimes.DatetimeIndex)
        self.assertIsInstance(df.index[0], pd._libs.tslibs.timestamps.Timestamp)
        self.assertTrue(df.index[0] < df.index[-1])

    def test_with_freq(self):
        df = stock.get_etf_ohlcv_by_date("20200101", "20200531", "292340", freq="m")
        #                 NAV  시가  고가  저가  종가 거래량   거래대금 기초지수
        # 날짜
        # 2020-01-31  8910.61  8900  9270   0  8795   36559   330991070  1231.00
        # 2020-02-29  8633.13     0  9395   0  7555      72      658080  1213.88
        # 2020-03-31  7720.09  7520  9965   0  6030  206070  1373727350  1149.86
        # 2020-04-30  5590.35  6055  6975   0  6975    8743    57352845   997.80
        # 2020-05-31  6845.59  6835  7450   0  7415    1788    13057270  1107.92
        temp = df.iloc[0:5, 0] == np.array([8910.61, 8633.13, 7720.09, 5590.35, 6845.59])
        self.assertEqual(temp.sum(), 5)
        self.assertIsInstance(df.index   , pd.core.indexes.datetimes.DatetimeIndex)
        self.assertIsInstance(df.index[0], pd._libs.tslibs.timestamps.Timestamp)
        self.assertTrue(df.index[0] < df.index[-1])

class EtfPdf(unittest.TestCase):
    def test_with_business_day(self)
        df = stock.get_etf_portfolio_deposit_file("152100")
        #          계약수       금액   비중
        # 티커
        # 005930   8140.0  667480000  31.77
        # 000660    968.0  118580000   5.69
        # 035420    218.0   74774000   3.57
        # 051910     79.0   72443000   3.53
        # 068270    184.0   59616000   3.21
        temp = df.iloc[0:5, 0] == np.array([8140.0, 968.0, 218.0, 79.0, 184.0])
        self.assertEqual(temp.sum(), 5)

if __name__ == '__main__':
    unittest.main()
