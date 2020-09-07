import unittest
from pykrx import stock
import pandas
import numpy as np


class KrxMarketBasicTest(unittest.TestCase):
    def test_not_empty_result(self):
        df = stock.get_market_ohlcv_by_date("20190225", "20190228", "000660")
        self.assertFalse(df.empty)

        df = stock.get_market_cap_by_date("20190101", "20190131", "005930")
        self.assertFalse(df.empty)

        df = stock.get_market_cap_by_ticker("20200625")
        self.assertFalse(df.empty)

        df = stock.get_market_price_change_by_ticker("20180301", "20180320")
        self.assertFalse(df.empty)

        df = stock.get_market_fundamental_by_ticker("20180305", "KOSPI")
        self.assertFalse(df.empty)

        df = stock.get_market_fundamental_by_date("20180301", "20180320", '005930')
        self.assertFalse(df.empty)


class StockTickerListTest(unittest.TestCase):
    def test_io_with_default_param(self):
        tickers = stock.get_market_ticker_list()
        self.assertIsInstance(tickers, list)
        self.assertNotEqual(len(tickers), 0)

    def test_io_with_specific_business_date(self):
        tickers = stock.get_market_ticker_list("20170717")
        self.assertIsInstance(tickers, list)
        self.assertNotEqual(len(tickers), 0)


class StockTickerNameTest(unittest.TestCase):
    def test_io(self):
        name = stock.get_market_ticker_name("000660")
        self.assertIsInstance(name, str)
        self.assertNotEqual(len(name), 0)


class StockOhlcvByDateTest(unittest.TestCase):
    def test_io_for_1_day(self):
        df = stock.get_market_ohlcv_by_date("20200717", "20200717", "005930")
        self.assertIsNotNone(df)
        self.assertIsInstance(df.index[0], pandas.Timestamp)
        self.assertIsInstance(df['시가'].iloc[0], np.int32)
        self.assertIsInstance(df['고가'].iloc[0], np.int32)
        self.assertIsInstance(df['저가'].iloc[0], np.int32)
        self.assertIsInstance(df['종가'].iloc[0], np.int32)
        self.assertIsInstance(df['거래량'].iloc[0], np.int32)

    def test_io_for_n_day(self):
        df = stock.get_market_ohlcv_by_date("20200701", "20200717", "005930")
        self.assertIsNotNone(df)
        self.assertIsInstance(df.index[0], pandas.Timestamp)
        self.assertIsInstance(df['시가'].iloc[0], np.int32)
        self.assertIsInstance(df['고가'].iloc[0], np.int32)
        self.assertIsInstance(df['저가'].iloc[0], np.int32)
        self.assertIsInstance(df['종가'].iloc[0], np.int32)
        self.assertIsInstance(df['거래량'].iloc[0], np.int32)

    def test_io_for_month(self):
        df = stock.get_market_ohlcv_by_date("20200101", "20200630", "005930", "m")
        self.assertIsNotNone(df)
        self.assertEqual(len(df), 6)


class StockOhlcvByTickerTest(unittest.TestCase):
    def test_io(self):
        df = stock.get_market_ohlcv_by_ticker("20180212")
        self.assertIsInstance(df.index[0], str)
        self.assertIsInstance(df['종목명'].iloc[0], str)
        self.assertIsInstance(df['시가'].iloc[0], np.int32)
        self.assertIsInstance(df['고가'].iloc[0], np.int32)
        self.assertIsInstance(df['저가'].iloc[0], np.int32)
        self.assertIsInstance(df['종가'].iloc[0], np.int32)
        self.assertIsInstance(df['거래량'].iloc[0], np.int64)
        self.assertIsInstance(df['시가총액'].iloc[0], np.int64)
        self.assertIsInstance(df['거래대금'].iloc[0], np.int64)
        self.assertIsInstance(df['시총비중'].iloc[0], np.float16)
        self.assertIsInstance(df['상장주식수'].iloc[0], np.int32)

class StockPriceChangeByTickerTest(unittest.TestCase):
    def test_io_for_holiday(self):
        df = stock.get_market_price_change_by_ticker("20040418", "20040418")
        self.assertEqual(df.empty, True)

    def test_io_for_weekday(self):
        df = stock.get_market_price_change_by_ticker("20040420", "20040422")
        self.assertIsNotNone(df)
        self.assertNotEqual(df.empty, True)

    def test_io_for_delisting(self):
        #  - 상장 폐지 종목 079660 (20190625)
        df = stock.get_market_price_change_by_ticker("20190624", "20190630")
        self.assertEqual(df.loc['079660']['종가'], 0)
        self.assertEqual(df.loc['079660']['등락률'], -100)


class StockFundamentalByTickerTest(unittest.TestCase):
    def test_io(self):
        df = stock.get_market_fundamental_by_ticker("20180212")
        self.assertIsInstance(df.index[0], str)
        self.assertIsInstance(df['종목명'].iloc[0], str)
        self.assertIsInstance(df['DIV'].iloc[0], np.float64)
        self.assertIsInstance(df['BPS'].iloc[0], np.int32)
        self.assertIsInstance(df['PER'].iloc[0], np.float64)
        self.assertIsInstance(df['EPS'].iloc[0], np.int32)
        self.assertIsInstance(df['PBR'].iloc[0], np.float64)


class StockFundamentalByDateTest(unittest.TestCase):
    def test_io(self):
        df = stock.get_market_fundamental_by_date("20180212", "20180216", "006800")
        self.assertIsInstance(df.index[0], pandas._libs.tslibs.timestamps.Timestamp)
        self.assertIsInstance(df['DIV'].iloc[0], np.float64)
        self.assertIsInstance(df['BPS'].iloc[0], np.int32)
        self.assertIsInstance(df['PER'].iloc[0], np.float64)
        self.assertIsInstance(df['EPS'].iloc[0], np.int32)
        self.assertIsInstance(df['PBR'].iloc[0], np.float64)

    def test_io_with_frequency(self):
        df = stock.get_market_fundamental_by_date("20180101", "20180331", "006800", "m")
        self.assertIsNotNone(df)
        self.assertEqual(len(df), 3)


class StockTradingVolumeTest(unittest.TestCase):
    def test_io_for_kospi(self):
        df = stock.get_market_trading_volume_by_date("20200519", "20200526", "KOSPI")
        self.assertIsNotNone(df)
        self.assertIsInstance(df['전체'].iloc[0], np.int64)
        self.assertIsInstance(df['정규매매'].iloc[0], np.int64)

    def test_io_for_kosdaq(self):
        df = stock.get_market_trading_volume_by_date("20200519", "20200526", "KOSDAQ")
        print(df)
        self.assertIsNotNone(df)
        self.assertIsInstance(df['전체'].iloc[0], np.int64)
        self.assertIsInstance(df['정규매매'].iloc[0], np.int64)

    def test_io_for_kosdaq(self):
        df = stock.get_market_trading_volume_by_date("20200519", "20200526", "KONEX")
        self.assertIsNotNone(df)
        self.assertIsInstance(df['전체'].iloc[0], np.int64)
        self.assertIsInstance(df['정규매매'].iloc[0], np.int64)

    def test_io_for_month(self):
        df = stock.get_market_trading_volume_by_date("20200101", "20200331", "KOSPI", freq="m")
        self.assertIsNotNone(df)
        self.assertEqual(len(df), 3)

    def test_io_for_diverse_market(self):
        kospi = stock.get_market_trading_volume_by_date("20200519", "20200520", "KOSPI")
        kosdaq = stock.get_market_trading_volume_by_date("20200519", "20200520", "KOSDAQ")
        konex = stock.get_market_trading_volume_by_date("20200519", "20200520", "KONEX")

        self.assertNotEqual(kospi.iloc[0][1], kosdaq.iloc[0][1])
        self.assertNotEqual(kospi.iloc[0][1], konex.iloc[0][1])


class StockCapByTickerTest(unittest.TestCase):
    def test_io(self):
        df = stock.get_market_cap_by_ticker("20200101")
        self.assertTrue(df.empty)

        df = stock.get_market_cap_by_ticker("20200625")
        self.assertFalse(df.empty)
        for col in df.columns:
            self.assertEqual(type(df[col].iloc[0]), np.int64)


class StockCapByDateTest(unittest.TestCase):
    def test_io(self):
        df = stock.get_market_cap_by_date("20200101", "20200102", "005930")
        self.assertTrue(len(df) == 1)

        for col in df.columns:
            self.assertEqual(type(df[col].iloc[0]), np.int64)


class StockMarketTradingValueAndVolumeByTicker(unittest.TestCase):
    def test_io(self):
        df = stock.get_market_trading_value_and_volume_by_ticker("20200907")
        self.assertFalse(df.empty)


class StockExhaustionRatesOfForeignInvestmentByTicker(unittest.TestCase):
    def test_io(self):
        df = stock.get_exhaustion_rates_of_foreign_investment_by_ticker('20200703')
        self.assertIsNotNone(df)

    def test_io_for_diverse_market(self):
        kospi = stock.get_exhaustion_rates_of_foreign_investment_by_ticker('20200703', "KOSPI")
        kosdaq = stock.get_exhaustion_rates_of_foreign_investment_by_ticker('20200703', "KOSDAQ")
        self.assertNotEqual(kospi.iloc[0][1], kosdaq.iloc[0][1])


if __name__ == '__main__':
    unittest.main()
