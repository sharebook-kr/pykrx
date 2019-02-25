import unittest
from pykrx import Krx
import numpy as np


class KrxStockBasicTest(unittest.TestCase):
    def setUp(self):
        self.krx = Krx()

    def test_not_empty_result(self):
        df = self.krx.get_market_index("20190211")
        self.assertNotEqual(df.empty, True)

        df = self.krx.get_market_status_by_date("20190211")
        self.assertNotEqual(df.empty, True)

        df = self.krx.get_market_price_change("20190211", "20190215")
        self.assertNotEqual(df.empty, True)

        df = self.krx.get_market_ohlcv("20190211", "20190215", "000660")
        self.assertNotEqual(df.empty, True)


class StockPriceChangeTest(unittest.TestCase):
    def setUp(self):
        self.krx = Krx()

    def test_price_query(self):
        # holiday - holiday
        df = self.krx.get_market_price_change("20040418", "20040418")
        self.assertEqual(df.empty, True)

        # holiday - weekday
        #  - 상장 폐지 종목 037730 (20040422)
        df = self.krx.get_market_price_change("20040418", "20040430")
        self.assertEqual(df.loc['037730']['종료일종가'], 0)
        self.assertEqual(df.loc['037730']['등락률'    ], -100)

        # weekday - weekday
        df = self.krx.get_market_price_change("20040420", "20040422")
        self.assertNotEqual(df.empty, True)

class StockOhlcvTest(unittest.TestCase):
    def setUp(self):
        self.krx = Krx()

    def test_ohlcv_query(self):
        # 1 business day
        df = self.krx.get_market_ohlcv("20180208", "20180208", "066570")
        self.assertEqual(len(df), 1)

        # one more business days
        df = self.krx.get_market_ohlcv("20180101", "20180208", "066570")
        self.assertIsNotNone(df)

        # None for holiday
        df = self.krx.get_market_ohlcv("20190209", "20190209", "066570")
        self.assertTrue(df.empty)

    def test_ohlcv_format(self):
        target = "20180208"
        df = self.krx.get_market_ohlcv(target, target, "066570")
        self.assertEqual(type(df.index[0]), str)
        self.assertEqual(type(df['시가'].iloc[0]), np.int32)
        self.assertEqual(type(df['고가'].iloc[0]), np.int32)
        self.assertEqual(type(df['저가'].iloc[0]), np.int32)
        self.assertEqual(type(df['종가'].iloc[0]), np.int32)
        self.assertEqual(type(df['거래량'].iloc[0]), np.int32)


class StockStatusTest(unittest.TestCase):
    def setUp(self):
        self.krx = Krx()

    def test_market_status_format(self):
        df = self.krx.get_market_status_by_date("20180212")
        self.assertEqual(type(df.index[0]), str)
        self.assertEqual(type(df['종목명'].iloc[0]), str)
        self.assertEqual(type(df['DIV'].iloc[0]), np.float32)
        self.assertEqual(type(df['BPS'].iloc[0]), np.int32)
        self.assertEqual(type(df['PER'].iloc[0]), np.float32)
        self.assertEqual(type(df['EPS'].iloc[0]), np.int32)


if __name__ == '__main__':
    unittest.main()
