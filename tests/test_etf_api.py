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


class EtfOhlcvByTicker(unittest.TestCase):
    def test_with_business_day(self):
        df = stock.get_etf_ohlcv_by_ticker("20210325")
        #           NAV   시가   고가   저가    종가 거래량    거래대금  기초지수
        # 티커
        # 152100   41887.33  41705  42145  41585   41835  59317  2479398465    408.53
        # 295820   10969.41  10780  10945  10780   10915     69      750210   2364.03
        # 253150   46182.13  45640  46700  45540   46145   1561    71730335   2043.75
        # 253160    4344.07   4400   4400   4295    4340  58943   256679440   2043.75
        # 278420    9145.45   9055   9150   9055    9105   1164    10598375   1234.03
        temp = df.iloc[0:5, 0] == np.array([41887.33, 10969.41, 46182.13, 4344.07, 9145.45])
        self.assertEqual(temp.sum(), 5)

    def test_with_holiday(self):
        df = stock.get_etf_ohlcv_by_ticker("20210321")
        self.assertTrue(df.empty)


class EtfPriceChange(unittest.TestCase):
    def test_with_business_day(self):
        df = stock.get_etf_price_change_by_ticker("20210325", "20210402")
        #           시가    종가  변동폭  등락률   거래량     거래대금
        # 152100   41715   43405    1690    4.05  1002296  42802174550
        # 295820   10855   11185     330    3.04     1244     13820930
        # 253150   45770   49735    3965    8.66    13603    650641700
        # 253160    4380    4015    -365   -8.33   488304   2040509925
        # 278420    9095    9385     290    3.19     9114     84463155
        temp = df.iloc[0:5, 2] == np.array([1690, 330, 3965, -365, 290])
        self.assertEqual(temp.sum(), 5)

    def test_with_holiday_0(self):
        df = stock.get_etf_price_change_by_ticker("20210321", "20210325")
        #           시가    종가  변동폭  등락률   거래량     거래대금
        # 152100   42225   41835    -390   -0.92   577936  24345828935
        # 295820   10990   10915     -75   -0.68      239      2611835
        # 253150   47095   46145    -950   -2.02     9099    422031050
        # 253160    4270    4340      70    1.64   297894   1290824525
        # 278420    9190    9105     -85   -0.92     5312     48491950
        temp = df.iloc[0:5, 2] == np.array([-390, -75, -950, 70, -85])
        self.assertEqual(temp.sum(), 5)

    def test_with_holiday_1(self):
        df = stock.get_etf_price_change_by_ticker("20210321", "20210321")
        self.assertTrue(df.empty)


class EtfPdf(unittest.TestCase):
    def test_with_business_day(self):
        df = stock.get_etf_portfolio_deposit_file("152100", "20210402")
        #          계약수       금액   비중
        # 티커
        # 005930  8140.0  674806000  31.74
        # 000660   968.0  136004000   6.28
        # 035420   218.0   82513000   3.80
        # 051910    79.0   64701000   3.01
        # 006400    89.0   59363000   2.73
        temp = df.iloc[0:5, 0] == np.array([8140.0, 968.0, 218.0, 79.0, 89.0])
        self.assertEqual(temp.sum(), 5)
    
    def test_with_negative_value(self):
        # 음수 -3.28 확인
        df = stock.get_etf_portfolio_deposit_file("114800", "20210402")
        # 069500 -324.00                     0  0.0
        # 101R90   -3.58  18446744073334367616  0.0
        #           0.00                     0  0.0
        #           0.00                     0  0.0
        self.assertAlmostEqual(df.iloc[1, 0], -3.58)


if __name__ == '__main__':
    unittest.main()
