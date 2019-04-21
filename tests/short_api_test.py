import unittest
from pykrx import stock


class KrxShortBasicTest(unittest.TestCase):
    def test_not_empty_result(self):
        df = stock.get_shorting_investor_volume_by_date("20190401", "20190405",
                                                        "KOSPI")
        self.assertNotEqual(df.empty, True)

        df = stock.get_shorting_investor_price_by_date("20190401", "20190405",
                                                       "KOSPI")
        self.assertNotEqual(df.empty, True)

        df = stock.get_shorting_volume_top50("20190401", "KOSPI")
        self.assertNotEqual(df.empty, True)

        df = stock.get_shorting_balance_by_ticker("20190401", "20190405",
                                                  "005930")
        self.assertNotEqual(df.empty, True)

        df = stock.get_shorting_balance_top50("20190401", "KOSPI")
        self.assertNotEqual(df.empty, True)


if __name__ == '__main__':
    unittest.main()
