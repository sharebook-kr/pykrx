import unittest
from pykrx import Krx
import numpy as np


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
        self.assertEqual(df.index, target)
        self.assertEqual(type(df['시가'][0]), np.int32)
        self.assertEqual(type(df['고가'][0]), np.int32)
        self.assertEqual(type(df['저가'][0]), np.int32)
        self.assertEqual(type(df['종가'][0]), np.int32)
        self.assertEqual(type(df['거래량'][0]), np.int32)


class StockStatusTest(unittest.TestCase):
    def setUp(self):
        self.krx = Krx()

    def test_market_status_format(self):
        df = self.krx.get_market_status_by_date("20180212")
        self.assertEqual(type(df.index[0]), str)
        self.assertEqual(type(df['종목명'][0]), str)
        self.assertEqual(type(df['DIV'][0]), np.float32)
        self.assertEqual(type(df['BPS'][0]), np.int32)
        self.assertEqual(type(df['PER'][0]), np.float32)
        self.assertEqual(type(df['EPS'][0]), np.int32)



if __name__ == '__main__':
    unittest.main()
