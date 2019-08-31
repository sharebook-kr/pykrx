import unittest
from pykrx import stock
import pandas
import numpy as np


class KrxMarketBasicTest(unittest.TestCase):
    def test_not_empty_result(self):
        df = stock.get_market_ohlcv_by_date("20190225", "20190228", "000660")
        self.assertNotEqual(df.empty, True)

        df = stock.get_market_price_change_by_ticker("20180301", "20180320")
        self.assertNotEqual(df.empty, True)

        df = stock.get_market_fundamental_by_ticker("20180305", "KOSPI")
        self.assertNotEqual(df.empty, True)

        df = stock.get_market_fundamental_by_date("20180301", "20180320",
                                                  '005930')
        self.assertNotEqual(df.empty, True)


class StockPriceChangeTest(unittest.TestCase):
    def test_price_query(self):
        # holiday - holiday
        df = stock.get_market_price_change_by_ticker("20040418", "20040418")
        self.assertEqual(df.empty, True)

        # holiday - weekday
        #  - 상장 폐지 종목 079660 (20190625)
        df = stock.get_market_price_change_by_ticker("20190624", "20190630")
        print(df.loc['079660'])
        self.assertEqual(df.loc['079660']['종가'], 0)
        self.assertEqual(df.loc['079660']['등락률'], -100)

        # weekday - weekday
        df = stock.get_market_price_change_by_ticker("20040420", "20040422")
        self.assertNotEqual(df.empty, True)


class StockOhlcvTest(unittest.TestCase):
    def test_ohlcv_query(self):
        # 1 business day
        df = stock.get_market_ohlcv_by_date("20180208", "20180208", "066570")
        self.assertEqual(len(df), 1)

        # one more business days
        df = stock.get_market_ohlcv_by_date("20180101", "20180208", "066570")
        self.assertIsNotNone(df)

        # None for holiday
        df = stock.get_market_ohlcv_by_date("20190209", "20190209", "066570")
        self.assertTrue(df.empty)

    def test_ohlcv_format(self):
        target = "20180208"
        df = stock.get_market_ohlcv_by_date(target, target, "066570")
        self.assertEqual(type(df.index[0]), pandas.Timestamp)
        self.assertEqual(type(df['시가'].iloc[0]), np.int32)
        self.assertEqual(type(df['고가'].iloc[0]), np.int32)
        self.assertEqual(type(df['저가'].iloc[0]), np.int32)
        self.assertEqual(type(df['종가'].iloc[0]), np.int32)
        self.assertEqual(type(df['거래량'].iloc[0]), np.int32)


class StockStatusTest(unittest.TestCase):
    def test_market_fundamental_1_format(self):
        df = stock.get_market_fundamental_by_ticker("20180212")
        self.assertEqual(type(df.index[0]), str)
        self.assertEqual(type(df['종목명'].iloc[0]), str)
        self.assertEqual(type(df['DIV'].iloc[0]), np.float64)
        self.assertEqual(type(df['BPS'].iloc[0]), np.int32)
        self.assertEqual(type(df['PER'].iloc[0]), np.float64)
        self.assertEqual(type(df['EPS'].iloc[0]), np.int32)
        self.assertEqual(type(df['PBR'].iloc[0]), np.float64)

    def test_market_fundamental_2_format(self):
        df = stock.get_market_fundamental_by_date("20180212", "20180216",
                                                  "006800")
        self.assertEqual(type(df.index[0]), str)
        self.assertEqual(type(df['DIV'].iloc[0]), np.float64)
        self.assertEqual(type(df['BPS'].iloc[0]), np.int32)
        self.assertEqual(type(df['PER'].iloc[0]), np.float64)
        self.assertEqual(type(df['EPS'].iloc[0]), np.int32)
        self.assertEqual(type(df['PBR'].iloc[0]), np.float64)


if __name__ == '__main__':
    unittest.main()
