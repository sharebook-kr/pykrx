import unittest
from pykrx import stock
import pandas as pd
import numpy as np
# pylint: disable-all
# flake8: noqa

class IndexTickerList(unittest.TestCase):
    def test_index_list_for_a_specific_day(self):
        tickers = stock.get_index_ticker_list('20210118')
        self.assertIsInstance(tickers, list)
        self.assertGreater(len(tickers), 0)
        self.assertEqual(tickers[0], '1001')

    def test_index_list_for_a_holiday(self):
        tickers = stock.get_index_ticker_list('20210130')
        self.assertIsInstance(tickers, list)
        self.assertGreater(len(tickers), 0)
        self.assertEqual(tickers[0], '1001')

    def test_index_list_in_kosdaq(self):
        tickers = stock.get_index_ticker_list('20210130', 'KOSDAQ')
        self.assertIsInstance(tickers, list)
        self.assertGreater(len(tickers), 0)
        self.assertEqual(tickers[0], '2001')

    def test_index_list_in_theme(self):
        tickers = stock.get_index_ticker_list('20210130', '테마')
        self.assertIsInstance(tickers, list)
        self.assertGreater(len(tickers), 0)
        self.assertEqual(tickers[0], '1163')

    def test_index_name(self):
        name = stock.get_index_ticker_name("1001")
        self.assertEqual(name, "코스피")
        name = stock.get_index_ticker_name("2001")
        self.assertEqual(name, "코스닥")
        name = stock.get_index_ticker_name("1163")
        self.assertEqual(name, "코스피 고배당 50")

class IndexPortfolioDepositFile(unittest.TestCase):
    def test_pdf_list_width_default_params(self):
        tickers = stock.get_index_portfolio_deposit_file('1001')
        self.assertIsInstance(tickers, list)
        self.assertTrue(len(tickers) > 0)

        tickers = stock.get_index_portfolio_deposit_file('2001')
        self.assertIsInstance(tickers, list)
        self.assertTrue(len(tickers) > 0)

        tickers = stock.get_index_portfolio_deposit_file('1163')
        self.assertIsInstance(tickers, list)
        self.assertTrue(len(tickers) > 0)

    def test_pdf_list_in_businessday(self):
        tickers = stock.get_index_portfolio_deposit_file('1001', '20210129')
        self.assertIsInstance(tickers, list)
        self.assertEqual(len(tickers), 796)

        tickers = stock.get_index_portfolio_deposit_file('1001', '20140502')
        self.assertIsInstance(tickers, list)
        self.assertEqual(len(tickers), 760)

    def test_pdf_list_prior_to_20140501(self):
        tickers = stock.get_index_portfolio_deposit_file('1001', '20140429')
        # KRX web server does NOT provide data prior to 2014/05/01.
        self.assertIsInstance(tickers, list)
        self.assertEqual(len(tickers), 0)


class IndexOhlcvByDate(unittest.TestCase):
    def test_ohlcv_simple(self):
        df = stock.get_index_ohlcv_by_date("20210101", "20210130", "1001")
        #                시가     고가     저가     종가      거래량         거래대금
        # 날짜
        # 2021-01-04  2874.50  2946.54  2869.11  2944.45  1026510465  25011393960858
        # 2021-01-05  2943.67  2990.57  2921.84  2990.57  1519911750  26548380179493
        # 2021-01-06  2993.34  3027.16  2961.37  2968.21  1793418534  29909396443430
        # 2021-01-07  2980.75  3055.28  2980.75  3031.68  1524654500  27182807334912
        # 2021-01-08  3040.11  3161.11  3040.11  3152.18  1297903388  40909490005818
        temp = df.iloc[0:5, 0] == np.array([2874.50, 2943.67, 2993.34, 2980.75, 3040.11])
        self.assertEqual(temp.sum(), 5)
        self.assertIsInstance(df.index   , pd.core.indexes.datetimes.DatetimeIndex)
        self.assertIsInstance(df.index[0], pd._libs.tslibs.timestamps.Timestamp)
        self.assertTrue(df.index[0] < df.index[-1])

    def test_ohlcv_in_holiday(self):
        df = stock.get_index_ohlcv_by_date("20210101", "20210101", "1001")
        self.assertIsInstance(df, pd.DataFrame)
        self.assertTrue(df.empty)

    def test_ohlcv_with_freq(self):
        df = stock.get_index_ohlcv_by_date("20200101", "20200630", "1001", freq="m")
        #                시가     고가     저가     종가       거래량
        # 날짜
        # 2020-01-31  2201.21  2277.23  2119.01  2119.01  13096066333
        # 2020-02-29  2086.61  2255.49  1980.82  1987.01  13959766513
        # 2020-03-31  1997.03  2089.08  1439.43  1754.64  17091025314
        # 2020-04-30  1737.28  1957.51  1664.13  1947.56  21045120912
        # 2020-05-31  1906.42  2054.52  1894.29  2029.60  16206496902
        temp = df.iloc[0:5, 0] == np.array([2201.21, 2086.61, 1997.03, 1737.28, 1906.42])
        self.assertEqual(temp.sum(), 5)
        self.assertIsInstance(df.index   , pd.core.indexes.datetimes.DatetimeIndex)
        self.assertIsInstance(df.index[0], pd._libs.tslibs.timestamps.Timestamp)
        self.assertTrue(df.index[0] < df.index[-1])

    def test_ohlcv_with_nan(self):
        df = stock.get_index_ohlcv_by_date("19800101", "20200831", "1001")
        #               시가      고가     저가     종가      거래량        거래대금
        # 날짜
        # 1980-01-04     0.00     0.00     0.00   100.00       95900       602800000
        # 1980-01-05     0.00     0.00     0.00   100.15      131100       776400000
        # 1980-01-07     0.00     0.00     0.00   102.53      358300      2029700000
        # 1980-01-08     0.00     0.00     0.00   105.28      795800      5567200000
        temp = df.iloc[0:5, 0] == np.array([0, 0, 0, 0, 0])
        self.assertEqual(temp.sum(), 5)


class IndexListingDate(unittest.TestCase):
    def test_listing_info(self):
        df = stock.get_index_listing_date()
        #                        기준시점    발표시점   기준지수  종목수
        # 지수명
        # 코스피               1980.01.04  1983.01.04      100.0     796
        # 코스피 200           1990.01.03  1994.06.15      100.0     201
        # 코스피 100           2000.01.04  2000.03.02     1000.0     100
        # 코스피 50            2000.01.04  2000.03.02     1000.0      50
        # 코스피 200 중소형주  2010.01.04  2015.07.13     1000.0     101
        self.assertIsInstance(df, pd.DataFrame)
        self.assertFalse(df.empty)
        self.assertEqual(df.loc['코스피', '기준시점'], "1980.01.04")
        self.assertEqual(df.loc['코스피', '발표시점'], "1983.01.04")
        self.assertEqual(df.loc['코스피', '기준지수'], 100.0)


class IndexPriceChangeByTicker(unittest.TestCase):
    def test_with_valid_business_days(self):
        df = stock.get_index_price_change_by_ticker("20210104", "20210108")
        #                           시가      종가     등락률      거래량         거래대금
        # 지수명
        # 코스피                 2873.47   3152.18   9.703125  7162398637  149561467924511
        # 코스피 200              389.29    430.22  10.507812  2221276866  119905899468167
        # 코스피 100             2974.06   3293.96  10.757812  1142234783   95023508273187
        # 코스피 50              2725.20   3031.59  11.242188   742099360   79663247553065
        # 코스피 200 중소형주    1151.78   1240.92   7.738281  1079042083   24882391194980
        temp = df.iloc[0:5, 0] == np.array([2873.47, 389.29, 2974.06, 2725.20, 1151.78])
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(temp.sum(), 5)
        self.assertFalse(df.empty)

    def test_with_a_holiday_0(self):
        # 20210103 sunday / 20210108 friday
        df = stock.get_index_price_change_by_ticker("20210103", "20210108")
        temp = df.iloc[0:5, 0] == np.array([2873.47, 389.29, 2974.06, 2725.20, 1151.78])
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(temp.sum(), 5)
        self.assertFalse(df.empty)

    def test_with_a_holiday_1(self):
        # 20210104 monday / 20210109 saturday
        df = stock.get_index_price_change_by_ticker("20210104", "20210109")
        temp = df.iloc[0:5, 0] == np.array([2873.47, 389.29, 2974.06, 2725.20, 1151.78])
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(temp.sum(), 5)
        self.assertFalse(df.empty)

    def test_with_holidays(self):
        # 20210103 sunday / 20210110 sunday
        df = stock.get_index_price_change_by_ticker("20210103", "20210110")
        temp = df.iloc[0:5, 0] == np.array([2873.47, 389.29, 2974.06, 2725.20, 1151.78])
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(temp.sum(), 5)
        self.assertFalse(df.empty)

if __name__ == '__main__':
    unittest.main()
