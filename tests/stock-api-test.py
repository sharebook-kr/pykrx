import unittest
from pykrx.krx_stock import *


class StockApiTest(unittest.TestCase):
    def test_ohlcv_query(self):
        # 1 business day
        df = MKD30040().scraping("20180208", "20180208", "KR7066570003")
        self.assertEqual(len(df), 1)

        # one more business days
        df = MKD30040().scraping("20180101", "20180208", "KR7066570003")
        self.assertIsNotNone(df)

        # None for holiday
        df = MKD30040().scraping("20190209", "20190209", "KR7066570003")
        self.assertIsNone(df)

    def test_ohlcv_query(self):
        pass
if __name__ == '__main__':
    unittest.main()
