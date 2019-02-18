import unittest
import time
from pykrx import Krx


class BusinessDaysResultTest(unittest.TestCase):
    def setUp(self):
        self.krx = Krx()

    def test_wrong_business_days(self):
        year = 2018
        month = 3
        business_days = self.krx.get_business_days(year, month)
        for day in business_days[::6]:
            df = self.krx.get_market_index(day)
            self.assertFalse(df.empty)
            time.sleep(0.3)

    def test_empty_results(self):
        for year in range(1998, 2000):
            for month in range(1, 13):
                business_days = self.krx.get_business_days(year, month)
                self.assertGreater(len(business_days), 0)
                time.sleep(0.3)

    def test_wrong_input(self):
        business_days = self.krx.get_business_days(1950, 1)
        self.assertEqual(type(business_days), list)
        self.assertEqual(len(business_days), 0)


if __name__ == '__main__':
    unittest.main()

