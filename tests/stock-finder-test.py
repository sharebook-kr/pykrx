import unittest
from pykrx.krx_stock import *


class StockFinderTest(unittest.TestCase):
    def test_fetch_all_market(self):
        df = StockFinder().scraping()
        self.assertIsNotNone(df)

    def test_fetch_kospi_market(self):
        df = StockFinder().scraping(market="코스피")
        self.assertIsNotNone(df)

    def test_fetch_kosdaq_market(self):
        df = StockFinder().scraping(market="코스닥")
        self.assertIsNotNone(df)

    def test_fetch_samsung(self):
        df = StockFinder().scraping(name="삼성")
        self.assertIsNotNone(df)


class DelistFinderTest(unittest.TestCase):
    def test_fetch_all_market(self):
        df = DelistingFinder().scraping()
        self.assertIsNotNone(df)

    def test_fetch_kospi_market(self):
        df = DelistingFinder().scraping(market="코스피")
        self.assertIsNotNone(df)

    def test_fetch_kosdaq_market(self):
        df = DelistingFinder().scraping(market="코스닥")
        self.assertIsNotNone(df)

    def test_fetch_nongshim(self):
        df = DelistingFinder().scraping(name="농심")
        self.assertIsNotNone(df)


if __name__ == '__main__':
    unittest.main()


