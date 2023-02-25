import unittest
from m_pykrx import stock
import pandas as pd
import numpy as np
# pylint: disable-all
# flake8: noqa

class ShortStatusByDate(unittest.TestCase):
    def test_with_a_business_day(self):
        df = stock.get_shorting_status_by_date("20210104", "20210108", "005930")
        #           거래량 잔고수량   거래대금      잔고금액
        # 날짜
        # 2021-01-04  9279  2722585  771889500  225974555000
        # 2021-01-05   169  2676924   14011100  224593923600
        # 2021-01-06   967  3002548   80855100  246809445600
        # 2021-01-07   763  2447030   63634800  202858787000
        # 2021-01-08     6  2319328     534000  205956326400
        self.assertIsInstance(df, pd.DataFrame)
        temp = df.iloc[0:5, 0] == np.array([9279, 169, 967, 763, 6])
        self.assertEqual(temp.sum(), 5)
        self.assertTrue(df.index[0] < df.index[-1])

    def test_with_a_holiday(self):
        df = stock.get_shorting_status_by_date("20210104", "20210109", "005930")
        self.assertIsInstance(df, pd.DataFrame)
        temp = df.iloc[0:5, 0] == np.array([9279, 169, 967, 763, 6])
        self.assertEqual(temp.sum(), 5)
        self.assertTrue(df.index[0] < df.index[-1])

    def test_with_holidays(self):
        df = stock.get_shorting_status_by_date("20210103", "20210109", "005930")
        self.assertIsInstance(df, pd.DataFrame)
        temp = df.iloc[0:5, 0] == np.array([9279, 169, 967, 763, 6])
        self.assertEqual(temp.sum(), 5)
        self.assertTrue(df.index[0] < df.index[-1])

    def test_in_kosdaq(self):
        df = stock.get_shorting_status_by_date("20210103", "20210109", "017550")
        #          거래량 잔고수량 거래대금   잔고금액
        # 날짜
        # 2021-01-04   36    95384   112500  298075000
        # 2021-01-05  623    95986  2006060  309074920
        # 2021-01-06  349    97046  1111955  310061970
        # 2021-01-07  224    95732   727025  310650340
        # 2021-01-08  206    92281   636100  285148290
        self.assertIsInstance(df, pd.DataFrame)
        temp = df.iloc[0:5, 0] == np.array([36, 623, 349, 224, 206])
        self.assertEqual(temp.sum(), 5)
        self.assertTrue(df.index[0] < df.index[-1])


class ShortVolumeByTicker(unittest.TestCase):
    def test_with_default_param(self):
        df = stock.get_shorting_volume_by_ticker("20210125")
        #        공매도     매수  비중
        # 티커
        # 095570     32   180458  0.02
        # 006840     79   386257  0.02
        # 027410  18502  8453962  0.22
        # 282330     96    82986  0.12
        # 138930   1889  1181748  0.16
        self.assertIsInstance(df, pd.DataFrame)
        temp = df.iloc[0:5, 0] == np.array([32, 79, 18502, 96, 1889])
        self.assertEqual(temp.sum(), 5)
        temp = df.index[0:5] == np.array(["095570", "006840", "027410", "282330", "138930"])
        self.assertEqual(temp.sum(), 5)

    def test_with_holiday(self):
        df_0 = stock.get_shorting_volume_by_ticker("20210103", alternative=True)
        df_1 = stock.get_shorting_volume_by_ticker("20201230")
        same = (df_0 == df_1).all(axis=None)
        self.assertTrue(same)

    def test_in_kosdaq_0(self):
        df = stock.get_shorting_volume_by_ticker("20210104", market="KOSDAQ")
        #       공매도    매수      비중
        # 티커
        # 060310     0  588133  0.000000
        # 054620     0  300316  0.000000
        # 265520     0  415285  0.000000
        # 211270     0  424730  0.000000
        # 035760  1042  156069  0.669922
        self.assertIsInstance(df, pd.DataFrame)
        temp = df.index[:5] == np.array(["060310", "054620", "265520", "211270", "035760"])
        self.assertEqual(temp.sum(), 5)

    def test_in_kosdaq_1(self):
        df = stock.get_shorting_volume_by_ticker("20210104", "KOSDAQ")
        #       공매도    매수      비중
        # 티커
        # 060310     0  588133  0.000000
        # 054620     0  300316  0.000000
        # 265520     0  415285  0.000000
        # 211270     0  424730  0.000000
        # 035760  1042  156069  0.669922
        self.assertIsInstance(df, pd.DataFrame)
        temp = df.index[:5] == np.array(["060310", "054620", "265520", "211270", "035760"])
        self.assertEqual(temp.sum(), 5)

    def test_in_konex(self):
        df = stock.get_shorting_volume_by_ticker("20210104", market="KONEX")
        #      공매도  매수 비중
        # 티커
        # 112190    0  1240  0.0
        # 224880    0     0  0.0
        # 183410    0     0  0.0
        # 076340    0  7933  0.0
        # 329050    0     0  0.0
        self.assertIsInstance(df, pd.DataFrame)
        temp = df.index[:5] == np.array(["112190", "224880", "183410", "076340", "329050"])
        self.assertEqual(temp.sum(), 5)


class ShortValueByTicker(unittest.TestCase):
    def test_with_default_param(self):
        df = stock.get_shorting_value_by_ticker("20210125")
        #            공매도         매수      비중
        # 티커
        # 095570     134240    757272515  0.020004
        # 006840    2377900  11554067000  0.020004
        # 027410  108713300  49276275460  0.219971
        # 282330   14928000  13018465500  0.109985
        # 138930   10635610   6658032800  0.160034
        self.assertIsInstance(df, pd.DataFrame)
        temp = df.iloc[0:5, 0] == np.array([134240, 2377900, 108713300, 14928000, 10635610])
        self.assertEqual(temp.sum(), 5)
        temp = df.index[0:5] == np.array(["095570", "006840", "027410", "282330", "138930"])
        self.assertEqual(temp.sum(), 5)

    def test_with_holiday(self):
        df_0 = stock.get_shorting_value_by_ticker("20210103", alternative=True)
        df_1 = stock.get_shorting_value_by_ticker("20201230")
        same = (df_0 == df_1).all(axis=None)
        self.assertTrue(same)


class ShortVolumeByDate(unittest.TestCase):
    def test_with_default_param(self):
        df = stock.get_shorting_volume_by_date("20210104", "20210108", "005930")
        #           공매도      매수      비중
        # 날짜
        # 2021-01-04  9279  38655276  0.020004
        # 2021-01-05   169  35335669  0.000000
        # 2021-01-06   967  42089013  0.000000
        # 2021-01-07   763  32644642  0.000000
        # 2021-01-08     6  59013307  0.000000
        self.assertIsInstance(df, pd.DataFrame)
        self.assertTrue(df.index[0] < df.index[-1])
        temp = df.iloc[0:5, 0] == np.array([9279, 169, 967, 763, 6])
        self.assertEqual(temp.sum(), 5)

    def test_with_holiday_0(self):
        # 20210103 sunday / 20210108 friday
        df = stock.get_shorting_volume_by_date("20210103", "20210108", "005930")
        self.assertIsInstance(df, pd.DataFrame)
        self.assertTrue(df.index[0] < df.index[-1])
        temp = df.iloc[0:5, 0] == np.array([9279, 169, 967, 763, 6])
        self.assertEqual(temp.sum(), 5)

    def test_with_holiday_1(self):
        # 20210103 sunday / 20210109 friday
        df = stock.get_shorting_volume_by_date("20210103", "20210109", "005930")
        self.assertIsInstance(df, pd.DataFrame)
        self.assertTrue(df.index[0] < df.index[-1])
        temp = df.iloc[0:5, 0] == np.array([9279, 169, 967, 763, 6])
        self.assertEqual(temp.sum(), 5)


class ShortValueByDate(unittest.TestCase):
    def test_with_default_param(self):
        df = stock.get_shorting_value_by_date("20210104", "20210108", "005930")
        #                공매도           매수  비중
        # 날짜
        # 2021-01-04  771889500  3185356823460  0.02
        # 2021-01-05   14011100  2915618322800  0.00
        # 2021-01-06   80855100  3506903681680  0.00
        # 2021-01-07   63634800  2726112459660  0.00
        # 2021-01-08     534000  5083939899952  0.00
        self.assertIsInstance(df, pd.DataFrame)
        self.assertTrue(df.index[0] < df.index[-1])
        temp = df.iloc[0:5, 0] == np.array([771889500, 14011100, 80855100, 63634800, 534000])
        self.assertEqual(temp.sum(), 5)

    def test_with_holiday_0(self):
        # 20210103 sunday / 20210108 friday
        df = stock.get_shorting_value_by_date("20210103", "20210108", "005930")
        self.assertIsInstance(df, pd.DataFrame)
        temp = df.iloc[0:5, 0] == np.array([771889500, 14011100, 80855100, 63634800, 534000])
        self.assertEqual(temp.sum(), 5)

    def test_with_holiday_1(self):
        # 20210103 sunday / 20210109 friday
        df = stock.get_shorting_value_by_date("20210103", "20210109", "005930")
        self.assertIsInstance(df, pd.DataFrame)
        self.assertTrue(df.index[0] < df.index[-1])
        temp = df.iloc[0:5, 0] == np.array([771889500, 14011100, 80855100, 63634800, 534000])
        self.assertEqual(temp.sum(), 5)


class ShortInvestorVolumeByDate(unittest.TestCase):
    def test_with_default_param(self):
        df = stock.get_shorting_investor_volume_by_date("20200106", "20200110")
        #                기관    개인    외국인  기타    합계
        # 날짜
        # 2020-01-06  3783324  215700   9213745   0  13212769
        # 2020-01-07  3627906  270121   7112215   0  11010242
        # 2020-01-08  5161993  284087  13164830   0  18610910
        # 2020-01-09  5265706  271622  11138406   0  16675734
        # 2020-01-10  5129724  141885   7849543   0  13121152
        self.assertIsInstance(df, pd.DataFrame)
        self.assertTrue(df.index[0] < df.index[-1])
        temp = df.iloc[0:5, 0] == np.array([3783324, 3627906, 5161993, 5265706, 5129724])
        self.assertEqual(temp.sum(), 5)

    def test_with_holiday_0(self):
        # 20210103 sunday / 20210108 friday
        df = stock.get_shorting_investor_volume_by_date("20200105", "20200110")
        self.assertIsInstance(df, pd.DataFrame)
        self.assertTrue(df.index[0] < df.index[-1])
        temp = df.iloc[0:5, 0] == np.array([3783324, 3627906, 5161993, 5265706, 5129724])
        self.assertEqual(temp.sum(), 5)

    def test_with_holiday_1(self):
        # 20210103 sunday / 20210109 friday
        df = stock.get_shorting_investor_volume_by_date("20200105", "20200111")
        self.assertIsInstance(df, pd.DataFrame)
        self.assertTrue(df.index[0] < df.index[-1])
        temp = df.iloc[0:5, 0] == np.array([3783324, 3627906, 5161993, 5265706, 5129724])
        self.assertEqual(temp.sum(), 5)


class ShortInvestorValueByDate(unittest.TestCase):
    def test_with_default_param(self):
        df = stock.get_shorting_investor_value_by_date("20200106", "20200110")
        #                     기관        개인        외국인  기타        합계
        # 날짜
        # 2020-01-06  135954452715  2502658310  119387130395   0  257844241420
        # 2020-01-07  140062017520  2924582225  129899020748   0  272885620493
        # 2020-01-08  175731372983  2579881000  266907627745   0  445218881728
        # 2020-01-09  189541838466  3021427705  241819376326   0  434382642497
        # 2020-01-10  185561759364  3182000295  165327866557   0  354071626216
        self.assertIsInstance(df, pd.DataFrame)
        self.assertTrue(df.index[0] < df.index[-1])
        temp = df.iloc[0:5, 0] == np.array([135954452715, 140062017520, 175731372983, 189541838466, 185561759364])
        self.assertEqual(temp.sum(), 5)

    def test_with_holiday_0(self):
        # 20210103 sunday / 20210108 friday
        df = stock.get_shorting_investor_value_by_date("20200105", "20200110")
        self.assertIsInstance(df, pd.DataFrame)
        self.assertTrue(df.index[0] < df.index[-1])
        temp = df.iloc[0:5, 0] == np.array([135954452715, 140062017520, 175731372983, 189541838466, 185561759364])
        self.assertEqual(temp.sum(), 5)

    def test_with_holiday_1(self):
        # 20210103 sunday / 20210109 friday
        df = stock.get_shorting_investor_value_by_date("20200105", "20200111")
        self.assertIsInstance(df, pd.DataFrame)
        self.assertTrue(df.index[0] < df.index[-1])
        temp = df.iloc[0:5, 0] == np.array([135954452715, 140062017520, 175731372983, 189541838466, 185561759364])
        self.assertEqual(temp.sum(), 5)


class ShortVolumeTop50(unittest.TestCase):
    def test_with_default_param(self):
        df = stock.get_shorting_volume_top50("20200106")
        self.assertIsInstance(df, pd.DataFrame)
        self.assertTrue(len(df), 50)

    def test_with_holiday(self):
        # 20200105 sunday
        df = stock.get_shorting_volume_top50("20200105")
        self.assertIsInstance(df, pd.DataFrame)
        self.assertTrue(df.empty)

    def test_in_kosdaq_0(self):
        df = stock.get_shorting_volume_top50("20200106", market="KOSDAQ")
        self.assertIsInstance(df, pd.DataFrame)
        self.assertTrue(len(df), 50)

    def test_in_kosdaq_1(self):
        df = stock.get_shorting_volume_top50("20200106", "KOSDAQ")
        self.assertIsInstance(df, pd.DataFrame)
        self.assertTrue(len(df), 50)


class ShortBalanceTop50(unittest.TestCase):
    def test_with_default_param(self):
        df = stock.get_shorting_balance_top50("20200106")
        self.assertIsInstance(df, pd.DataFrame)
        self.assertTrue(len(df), 50)

    def test_with_holiday(self):
        # 20200105 sunday
        df = stock.get_shorting_balance_top50("20200105")
        self.assertIsInstance(df, pd.DataFrame)
        self.assertTrue(df.empty)

    def test_in_kosdaq_0(self):
        df = stock.get_shorting_balance_top50("20200106", market="KOSDAQ")
        self.assertIsInstance(df, pd.DataFrame)
        self.assertTrue(len(df), 50)

    def test_in_kosdaq_1(self):
        df = stock.get_shorting_balance_top50("20200106", "KOSDAQ")
        self.assertIsInstance(df, pd.DataFrame)
        self.assertTrue(len(df), 50)


class ShortBalanceByTicker(unittest.TestCase):
    def test_with_default_param(self):
        df = stock.get_shorting_balance_by_ticker("20210127")
        #         공매도잔고   상장주식수  공매도금액      시가총액      비중
        # 티커
        # 095570       33055     46822295   134864400  1.910350e+11  0.070007
        # 006840        4575     13247561   131760000  3.815298e+11  0.029999
        # 027410       68060     95716791   449196000  6.317308e+11  0.070007
        # 282330        4794     17283906   757452000  2.730857e+12  0.029999
        # 138930      596477    325935246  3340271200  1.825237e+12  0.180054
        self.assertIsInstance(df, pd.DataFrame)
        temp = df.iloc[0:5, 0] == np.array([33055, 4575, 68060, 4794, 596477])
        self.assertEqual(temp.sum(), 5)
        temp = df.index[0:5] == np.array(["095570", "006840", "027410", "282330", "138930"])
        self.assertEqual(temp.sum(), 5)


class ShortBalanceByDate(unittest.TestCase):
    def test_with_default_param(self):
        df = stock.get_shorting_balance_by_date("20200106", "20200110", "005930")
        #          공매도잔고  상장주식수    공매도금액      시가총액      비중
        # 날짜
        # 2020-01-06  5630893  5969782550  312514561500  3.313229e+14  0.090027
        # 2020-01-07  5169745  5969782550  288471771000  3.331139e+14  0.090027
        # 2020-01-08  5224233  5969782550  296736434400  3.390836e+14  0.090027
        # 2020-01-09  5387073  5969782550  315682477800  3.498293e+14  0.090027
        # 2020-01-10  5489240  5969782550  326609780000  3.552021e+14  0.090027
        self.assertIsInstance(df, pd.DataFrame)
        self.assertTrue(df.index[0] < df.index[-1])
        temp = df.iloc[0:5, 0] == np.array([5630893, 5169745, 5224233, 5387073, 5489240])
        self.assertEqual(temp.sum(), 5)

    def test_with_holiday_0(self):
        df = stock.get_shorting_balance_by_date("20200105", "20200110", "005930")
        self.assertIsInstance(df, pd.DataFrame)
        self.assertTrue(df.index[0] < df.index[-1])
        temp = df.iloc[0:5, 0] == np.array([5630893, 5169745, 5224233, 5387073, 5489240])
        self.assertEqual(temp.sum(), 5)

    def test_with_holiday_1(self):
        df = stock.get_shorting_balance_by_date("20200105", "20200111", "005930")
        self.assertIsInstance(df, pd.DataFrame)
        self.assertTrue(df.index[0] < df.index[-1])
        temp = df.iloc[0:5, 0] == np.array([5630893, 5169745, 5224233, 5387073, 5489240])
        self.assertEqual(temp.sum(), 5)


if __name__ == '__main__':
    unittest.main()
