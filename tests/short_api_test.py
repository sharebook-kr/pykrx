import unittest
from pykrx import Krx


class KrxShortBasicTest(unittest.TestCase):
    def setUp(self):
        self.krx = Krx()

    def test_not_empty_result(self):
        df = self.krx.get_shorting_status_by_date("20190211", "20190215", "000660")
        self.assertNotEqual(df.empty, True)

        df = self.krx.get_shorting_volume_by_ticker("20190211", "20190215", "000660")
        self.assertNotEqual(df.empty, True)

        df = self.krx.get_shorting_volume_by_investor("20190211", "20190215")
        self.assertNotEqual(df.empty, True)

        df = self.krx.get_shorting_volume_top50("20190211")
        self.assertNotEqual(df.empty, True)

        df = self.krx.get_shorting_balance_by_ticker("20190211", "20190215", "000660")
        self.assertNotEqual(df.empty, True)

        df = self.krx.get_shorting_balance_top50("20190211")
        self.assertNotEqual(df.empty, True)


if __name__ == '__main__':
    unittest.main()
