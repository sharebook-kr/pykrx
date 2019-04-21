import unittest
from pykrx import stock


class KrxIndexBasicTest(unittest.TestCase):
    def test_not_empty_result(self):
        df = stock.get_index_kospi_ohlcv_by_date("20190101", "20190228",
                                                 "코스피 200")
        self.assertNotEqual(df.empty, True)

        df = stock.get_index_kosdaq_ohlcv_by_date("20190101", "20190228",
                                                  "코스닥 150")
        self.assertNotEqual(df.empty, True)

        df = stock.get_index_status_by_group("20190228", "KOSPI")
        self.assertNotEqual(df.empty, True)

        df = stock.get_index_status_by_group("20190228", "KOSDAQ")
        self.assertNotEqual(df.empty, True)


if __name__ == '__main__':
    unittest.main()
