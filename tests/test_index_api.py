import unittest
from pykrx import stock
import pandas as pd
import numpy as np


class IndexTickerList(unittest.TestCase):
    def test_index_list_for_a_specific_day(self):
        tickers = stock.get_index_ticker_list('20210118')
        self.assertIsInstance(tickers, list)
        self.assertEqual(len(tickers), 46)
        self.assertEqual(tickers[0], '1001')

    def test_index_list_for_a_holiday(self):
        tickers = stock.get_index_ticker_list('20210130')
        self.assertIsInstance(tickers, list)
        self.assertEqual(len(tickers), 46)
        self.assertEqual(tickers[0], '1001')

    def test_index_list_in_kosdaq(self):
        tickers = stock.get_index_ticker_list('20210130', 'KOSDAQ')
        self.assertIsInstance(tickers, list)
        self.assertEqual(len(tickers), 50)
        self.assertEqual(tickers[0], '2001')

    def test_index_list_in_theme(self):
        tickers = stock.get_index_ticker_list('20210130', '테마')
        self.assertIsInstance(tickers, list)
        self.assertEqual(len(tickers), 21)
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
        #                    시가         고가         저가         종가      거래량        거래대금
        # 날짜
        # 2021-01-04  2874.500000  2946.540039  2869.110107  2944.449951  1026510465  25011393960858
        # 2021-01-05  2943.669922  2990.570068  2921.840088  2990.570068  1519911750  26548380179493
        # 2021-01-06  2993.340088  3027.159912  2961.370117  2968.209961  1793418534  29909396443430
        # 2021-01-07  2980.750000  3055.280029  2980.750000  3031.679932  1524654500  27182807334912
        # 2021-01-08  3040.110107  3161.110107  3040.110107  3152.179932  1297903388  40909490005818
        temp = df.iloc[0:5, 0] == np.array([2874.500000, 2943.669922, 2993.340088, 2980.750000, 3040.110107])
        self.assertEqual(temp.sum(), 5)
        self.assertIsInstance(df.index   , pd.core.indexes.datetimes.DatetimeIndex)
        self.assertIsInstance(df.index[0], pd._libs.tslibs.timestamps.Timestamp)
        self.assertTrue(df.index[0] < df.index[-1])

    def test_ohlcv_in_holiday(self):
        df = stock.get_index_ohlcv_by_date("20210101", "20210101", "1001")
        self.assertIsInstance(df, pd.DataFrame)
        self.assertTrue(df.empty)

    def test_ohlcv_with_freq(self):
        df = get_index_ohlcv_by_date("20200101", "20200630", "1001", freq="m")
        #                    시가         고가         저가         종가       거래량
        # 날짜
        # 2020-01-31  2201.209961  2277.229980  2119.010010  2119.010010  13096066333
        # 2020-02-29  2086.610107  2255.489990  1980.819946  1987.010010  13959766513
        # 2020-03-31  1997.030029  2089.080078  1439.430054  1754.640015  17091025314
        # 2020-04-30  1737.280029  1957.510010  1664.130005  1947.560059  21045120912
        # 2020-05-31  1906.420044  2054.520020  1894.290039  2029.599976  16206496902
        # 2020-06-30  2037.040039  2217.209961  2030.819946  2108.330078  19863704134
        temp = df.iloc[0:5, 0] == np.array([2201.209961, 2086.610107, 1997.030029, 1737.280029, 1906.420044])
        self.assertEqual(temp.sum(), 5)
        self.assertIsInstance(df.index   , pd.core.indexes.datetimes.DatetimeIndex)
        self.assertIsInstance(df.index[0], pd._libs.tslibs.timestamps.Timestamp)
        self.assertTrue(df.index[0] < df.index[-1])


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
