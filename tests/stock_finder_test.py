import unittest
from pykrx.comm.ticker import *


class StockFinderTest(unittest.TestCase):
    def test_fetch_all_market(self):
        df = StockFinder().read()
        self.assertIsNotNone(df)

    def test_fetch_kospi_market(self):
        df = StockFinder().read(market="코스피")
        self.assertIsNotNone(df)

    def test_fetch_kosdaq_market(self):
        df = StockFinder().read(market="코스닥")
        self.assertIsNotNone(df)

    def test_fetch_samsung(self):
        df = StockFinder().read(name="삼성")
        self.assertIsNotNone(df)


class DelistFinderTest(unittest.TestCase):
    def test_fetch_all_market(self):
        df = DelistingFinder().read()
        self.assertIsNotNone(df)

    def test_fetch_kospi_market(self):
        df = DelistingFinder().read(market="코스피")
        self.assertIsNotNone(df)

    def test_fetch_kosdaq_market(self):
        df = DelistingFinder().read(market="코스닥")
        self.assertIsNotNone(df)

    def test_fetch_nongshim(self):
        df = DelistingFinder().read(name="농심")
        self.assertIsNotNone(df)


if __name__ == '__main__':
    unittest.main()


