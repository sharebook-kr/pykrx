import unittest
from pykrx import e3


class KrxEtfBasicTest(unittest.TestCase):
    def test_not_empty_result(self):
        df = e3.get_etf_ohlcv_by_date("20190301", "20190405", "ARIRANG 200동일가중")
        self.assertNotEqual(df.empty, True)

        df = e3.get_etf_portfolio_deposit_file("20190405", "ARIRANG 200동일가중")
        self.assertNotEqual(df.empty, True)


if __name__ == '__main__':
    unittest.main()
