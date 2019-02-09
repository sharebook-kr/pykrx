import unittest
import time
from pykrx import Krx


class BusinessDaysResultTest(unittest.TestCase):
    def setUp(self):
        self.krx = Krx()

    def result_not_empty_test(self):
        for year in range(1998, 2019):
            for month in range(1, 13):
                business_days = self.krx.get_business_days(year, month)
                self.assertGreater(len(business_days), 0)
                time.sleep(1)

if __name__ == '__main__':
    unittest.main()

