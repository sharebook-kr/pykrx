import unittest
from pykrx import stock
import pandas as pd
import numpy as np
# pylint: disable-all
# flake8: noqa

class StockBusinessDaysTest(unittest.TestCase):
    def test_every_month(self):
        year = 2020
        for month in range(1, 13):
            days = stock.get_previous_business_days(year=year, month=month)
            self.assertNotEqual(len(days), 0)
            self.assertIsInstance(days, list)
            self.assertIsInstance(days[0], pd._libs.tslibs.timestamps.Timestamp)

    def test_days_for_a_specified_period_of_time(self):
        days = stock.get_previous_business_days(fromdate="20200101", todate="20200115")
        self.assertNotEqual(len(days), 0)
        self.assertIsInstance(days, list)
        self.assertIsInstance(days[0], pd._libs.tslibs.timestamps.Timestamp)


class StockOhlcvByDateTest(unittest.TestCase):
    def test_ohlcv_simple_call(self):
        df = stock.get_market_ohlcv_by_date("20210118", "20210126", "005930")
        #               시가    고가    저가    종가      거래량         거래대금  등락률
        # 날짜
        # 2021-01-18  86600  87300  84100  85000  43227951  3715775992600 -3.41
        # 2021-01-19  84500  88000  83600  87000  39895044  3441342754500  2.35
        # 2021-01-20  89000  89000  86500  87200  25211127  2205739149700  0.23
        # 2021-01-21  87500  88600  86500  88100  25318011  2211209788500  1.03
        # 2021-01-22  89000  89700  86800  86800  30861661  2717635251520 -1.48
        temp = df.iloc[0:5, 0] == np.array([86600, 84500, 89000, 87500, 89000])
        self.assertEqual(temp.sum(), 5)
        self.assertIsInstance(df.index   , pd.core.indexes.datetimes.DatetimeIndex)
        self.assertIsInstance(df.index[0], pd._libs.tslibs.timestamps.Timestamp)
        self.assertTrue(df.index[0] < df.index[-1])

    def test_ohlcv_for_a_day(self):
        df = stock.get_market_ohlcv_by_date("20210118", "20210118", "005930")
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 1)

    def test_ohlcv_with_adjusted(self):
        df = stock.get_market_ohlcv_by_date("20180427", "20180504", "005930")
        #               시가     고가    저가    종가     거래량         거래대금   등락률
        # 날짜
        # 2018-04-27  53380  53640  52440  53000    606216  1611240055340  1.65
        # 2018-04-30      0      0      0  53000         0              0  0.00
        # 2018-05-02      0      0      0  53000         0              0  0.00
        # 2018-05-03      0      0      0  53000         0              0  0.00
        # 2018-05-04  53000  53900  51800  51900  39565391  2078017927600 -2.08
        self.assertEqual(df.loc['2018-04-27']['시가'], 53380)
        self.assertEqual(df.loc['2018-05-04']['시가'], 53000)

    def test_ohlcv_with_not_adjusted(self):
        df = stock.get_market_ohlcv_by_date("20180427", "20180504", "005930", adjusted=False)
        #                 시가       고가      저가      종가     거래량         거래대금   등락률
        # 날짜
        # 2018-04-27  2669000  2682000  2622000  2650000    606216  1611240055340  1.65
        # 2018-04-30        0        0        0  2650000         0              0  0.00
        # 2018-05-02        0        0        0  2650000         0              0  0.00
        # 2018-05-03        0        0        0  2650000         0              0  0.00
        # 2018-05-04    53000    53900    51800    51900  39565391  2078017927600 -2.08
        self.assertEqual(df.loc['2018-04-27']['시가'], 2669000)
        self.assertEqual(df.loc['2018-05-04']['시가'], 53000)


class StockOhlcvByTickerTest(unittest.TestCase):
    def test_ohlcv_for_a_day(self):
        df = stock.get_market_ohlcv_by_ticker("20210122")
        #           시가    고가    저가    종가   거래량     거래대금     등락률
        # 티커
        # 095570    4190    4245    4160    4210   216835    910274405   0.839844
        # 006840   25750   29550   25600   29100   727088  20462325950  12.570312
        # 027410    5020    5250    4955    5220  1547629   7990770515   4.191406
        # 282330  156500  156500  151500  152000    62510   9555364000  -2.560547
        temp = df.iloc[0:5, 0] == np.array([4190, 25750, 5020, 156500, 5720])
        self.assertEqual(temp.sum(), 5)
        temp = df.index[0:5] == np.array(["095570", "006840", "027410", "282330", "138930"])
        self.assertEqual(temp.sum(), 5)

    def test_ohlcv_for_a_day_in_kosdaq(self):
        df = stock.get_market_ohlcv_by_ticker("20210122", "KOSDAQ")
        #           시가    고가    저가    종가   거래량     거래대금    등락률
        # 티커
        # 060310    2265    2290    2225    2255   275425    619653305 -0.219971
        # 054620    7210    7250    7030    7120   124636    883893780 -1.110352
        # 265520   25850   25850   25200   25400   196384   4994644750 -0.779785
        # 211270   10250   10950   10050   10350  1664154  17351956900  1.469727
        # 035760  165200  166900  162600  163800   179018  29574003100  0.429932
        temp = df.iloc[0:5, 0] == np.array([2265, 7210, 25850, 10250, 165200])
        self.assertEqual(temp.sum(), 5)
        temp = df.index[0:5] == np.array(["060310", "054620", "265520", "211270", "035760"])
        self.assertEqual(temp.sum(), 5)

    def test_ohlcv_for_a_day_on_holiday1(self):
        df0 = stock.get_market_ohlcv_by_ticker("20210123", alternative=True)    # Saturday
        df1 = stock.get_market_ohlcv_by_ticker("20210122")                      # Friday
        self.assertTrue((df0 == df1).all(axis=None))

    def test_ohlcv_for_a_day_on_holiday2(self):
        df0 = stock.get_market_ohlcv_by_ticker("20210123", alternative=True)   # Saturday
        df1 = stock.get_market_ohlcv_by_ticker("20210122")                     # Friday
        self.assertTrue((df0 == df1).all(axis=None))


class StockPriceChangeByTicker(unittest.TestCase):
    def test_with_valid_business_days(self):
        df = stock.get_market_price_change_by_ticker(fromdate="20210104", todate="20210111")
        #            종목명      시가    종가  변동폭    등락률       거래량      거래대금
        # 티커
        # 095570   AJ네트웍스    4615    4360    -255    -553.0      3445854   16332546190
        # 006840     AK홀딩스   25150   24800    -350    -139.0       605351   15190840550
        # 027410          BGF    4895    4690    -205    -419.0      3296226   16198124270
        # 282330    BGF리테일  135500  135500       0       0.0       399330   54474898000
        # 138930  BNK금융지주    5680    5720      40      70.0      2211455  125465685660
        self.assertIsInstance(df, pd.DataFrame)
        temp = df.iloc[0:5, 1] == np.array([4615, 25150, 4895, 135500, 5680])
        self.assertEqual(temp.sum(), 5)

    def test_with_holidays(self):
        df = stock.get_market_price_change_by_ticker(fromdate="20210710", todate="20210711")
        self.assertIsInstance(df, pd.DataFrame)
        self.assertTrue(df.empty)


    def test_with_holiday(self):
        df = stock.get_market_price_change_by_ticker(fromdate="20210101", todate="20210111")
        #            종목명      시가    종가  변동폭    등락률       거래량      거래대금
        # 티커
        # 095570   AJ네트웍스    4615    4360    -255    -553.0      3445854   16332546190
        # 006840     AK홀딩스   25150   24800    -350    -139.0       605351   15190840550
        # 027410          BGF    4895    4690    -205    -419.0      3296226   16198124270
        # 282330    BGF리테일  135500  135500       0       0.0       399330   54474898000
        # 138930  BNK금융지주    5680    5720      40      70.0      2211455  125465685660
        self.assertIsInstance(df, pd.DataFrame)
        temp = df.iloc[0:5, 1] == np.array([4615, 25150, 4895, 135500, 5680])
        self.assertEqual(temp.sum(), 5)

    def test_with_dash_dates(self):
        df = stock.get_market_price_change_by_ticker(fromdate="2021-01-04", todate="20210111")
        temp = df.iloc[0:5, 1] == np.array([4615, 25150, 4895, 135500, 5680])
        self.assertEqual(temp.sum(), 5)

        df = stock.get_market_price_change_by_ticker(fromdate="2021-01-04", todate="2021-01-11")
        temp = df.iloc[0:5, 1] == np.array([4615, 25150, 4895, 135500, 5680])
        self.assertEqual(temp.sum(), 5)

    def test_with_adjusted(self):
        df_adjusted = stock.get_market_price_change_by_ticker(fromdate="20180427", todate="20180504")
        samsung_adjusted = df_adjusted.loc['005930']
        # 종목명    삼성전자
        # 시가      52140
        # 종가      51900
        # ...
        self.assertEqual(samsung_adjusted['시가'], 52140)
        self.assertEqual(samsung_adjusted['종가'], 51900)

    def test_with_not_adjusted(self):
        df_not_adjusted = stock.get_market_price_change_by_ticker(fromdate="20180427", todate="20180504", adjusted=False)
        # 종목명    삼성전자
        # 시가      2607000
        # 종가      51900
        # ...
        samsung_not_adjusted = df_not_adjusted.loc['005930']
        self.assertEqual(samsung_not_adjusted['시가'], 2607000)
        self.assertEqual(samsung_not_adjusted['종가'], 51900)


class StockFundamentalByDate(unittest.TestCase):
    def test_with_valid_business_days(self):
        df = stock.get_market_fundamental_by_date("20210104", "20210108", "005930")
        #               BPS    PER   PBR   EPS   DIV   DPS
        # 날짜
        # 2021-01-04  37528  26.22  2.21  3166  1.71  1416
        # 2021-01-05  37528  26.50  2.24  3166  1.69  1416
        # 2021-01-06  37528  25.96  2.19  3166  1.72  1416
        # 2021-01-07  37528  26.18  2.21  3166  1.71  1416
        # 2021-01-08  37528  28.05  2.37  3166  1.59  1416
        self.assertIsInstance(df, pd.DataFrame)
        temp = np.isclose(df.iloc[0], [37528, 26.22,  2.21, 3166, 1.71, 1416])
        self.assertEqual(temp.sum(), 6)
        self.assertEqual(len(df), 5)

    def test_with_a_holiday_1(self):
        # 20210104 monday / 20210109 saturday
        df = stock.get_market_fundamental_by_date("20210104", "20210109", "005930")
        self.assertIsInstance(df, pd.DataFrame)
        temp = np.isclose(df.iloc[0], [37528, 26.22,  2.21, 3166, 1.71, 1416])
        self.assertEqual(temp.sum(), 6)
        self.assertEqual(len(df), 5)

    def test_with_holidays(self):
        # 20210103 sunday / 20210110 sunday
        df = stock.get_market_fundamental_by_date("20210103", "20210110", "005930")
        self.assertIsInstance(df, pd.DataFrame)
        temp = np.isclose(df.iloc[0], [37528, 26.22,  2.21, 3166, 1.71, 1416])
        self.assertEqual(temp.sum(), 6)
        self.assertEqual(len(df), 5)

    def test_with_freq(self):
        df = stock.get_market_fundamental_by_date("20200101", "20200430", "005930", freq="m")
        self.assertIsInstance(df, pd.DataFrame)
        temp = np.isclose(df.iloc[0], [35342, 8.54,  1.56, 6461, 2.57, 1416])
        self.assertEqual(temp.sum(), 6)
        self.assertEqual(len(df), 4)

    def test_in_kosdaq(self):
        df = stock.get_market_fundamental_by_date("20200101", "20200430", "263720")
        #              BPS    PER   PBR  EPS  DIV  DPS
        # 날짜
        # 2020-01-02  3033  48.78  6.59  410  0.0    0
        # 2020-01-03  3033  45.24  6.12  410  0.0    0
        # 2020-01-06  3033  43.90  5.93  410  0.0    0
        # 2020-01-07  3033  45.73  6.18  410  0.0    0
        # 2020-01-08  3033  44.15  5.97  410  0.0    0
        self.assertIsInstance(df, pd.DataFrame)
        temp = np.isclose(df.iloc[0], [3033, 48.78,  6.59, 410, 0.0, 0])
        self.assertEqual(temp.sum(), 6)


class StockFundamentalByTicker(unittest.TestCase):
    def test_with_valid_a_business_day(self):
        # 20210108 friday
        df = stock.get_market_fundamental_by_ticker("20210108")
        #           BPS    PER   PBR   EPS   DIV   DPS
        # 티커
        # 095570   6802   4.62  0.67   982  6.61   300
        # 006840  62448  11.69  0.41  2168  2.96   750
        # 027410  15699  17.46  0.31   281  2.24   110
        # 282330  36022  16.09  3.91  8763  1.91  2700
        # 138930  25415   3.51  0.23  1647  6.23   360
        self.assertIsInstance(df, pd.DataFrame)
        temp = np.isclose(df.iloc[0], [6802, 4.62, 0.67, 982, 6.61, 300])
        self.assertEqual(temp.sum(), 6)
        self.assertEqual(len(df), 895)

    def test_with_valid_a_holiday(self):
        # 20210109 saturday
        df = stock.get_market_fundamental_by_ticker("20210104")
        self.assertIsInstance(df, pd.DataFrame)
        temp = np.isclose(df.iloc[0], [6802, 4.66, 0.67, 982, 6.55, 300])
        self.assertEqual(temp.sum(), 6)
        self.assertEqual(len(df), 895)


class StockMarketCapByTicker(unittest.TestCase):
    def test_with_a_businessday(self):
        df = stock.get_market_cap_by_ticker("20210104")
        #           종가         시가총액    거래량       거래대금  상장주식수
        # 티커
        # 005930   83000  495491951650000  38655276  3185356823460  5969782550
        # 000660  126000   91728297990000   7995016   994276505704   728002365
        # 051910  889000   62756592927000    858451   747929748128    70592343
        # 005935   74400   61222770480000   5455139   405685236800   822886700
        # 207940  829000   54850785000000    182864   149889473000    66165000
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 2531)
        temp = np.isclose(df.iloc[0:5, 0], [83000, 126000, 889000, 74400, 829000])
        self.assertEqual(temp.sum(), 5)

    def test_with_a_holiday(self):
        df_0 = stock.get_market_cap_by_ticker("20210103")
        df_1 = stock.get_market_cap_by_ticker("20201230")
        # 해당 연휴에 상폐된 종목이 없기 때문에 두 index를 비교해 봄
        result = set(df_0.index) - set(df_1.index)
        self.assertEqual(len(result), 0)


class StockNetPurchasesOfEquitiesByTickerTest(unittest.TestCase):
    def test_net_purchases_of_equities_is_same_0(self):
        df = stock.get_market_net_purchases_of_equities_by_ticker("20210115", "20210122")
        #               종목명  매도거래량  매수거래량   순매수거래량   매도거래대금   매수거래대금 순매수거래대금
        # 티커
        # 005930      삼성전자    79567418   102852747       23285329  6918846810800  8972911580500  2054064769700
        # 000270        기아차    44440252    49880626        5440374  3861283906400  4377698855000   516414948600
        # 005935    삼성전자우    15849762    20011325        4161563  1207133611400  1528694164400   321560553000
        # 051910        LG화학      709872      921975         212103   700823533000   908593419000   207769886000
        # 096770  SK이노베이션     4848359     5515777         667418  1298854139000  1478890602000   180036463000
        temp = df.iloc[0:5, -1] == np.array([2054064769700, 516414948600, 321560553000, 207769886000, 180036463000])
        self.assertEqual(temp.sum(), 5)
        temp = df.index[0:5] == np.array(["005930", "000270", "005935", "051910", "096770"])
        self.assertEqual(temp.sum(), 5)

    def test_net_purchases_of_equities_is_same_1(self):
        #               종목명  매도거래량  매수거래량 순매수거래량   매도거래대금   매수거래대금 순매수거래대금
        #   티커
        # 034730            SK      456805      585912       129107   146275554500   188418240500    42142686000
        # 035420         NAVER     1084406     1204178       119772   343524584000   383697248000    40172664000
        # 000660    SK하이닉스     3146942     3401946       255004   409523728500   443014951000    33491222500
        # 017670      SK텔레콤      433019      564491       131472   108248823000   141157900500    32909077500
        # 086280  현대글로비스      306280      447356       141076    61999464500    91084909000    29085444500
        df = stock.get_market_net_purchases_of_equities_by_ticker("20210115", "20210122", investor="금융투자")
        temp = df.iloc[0:5, -1] == np.array([42142686000, 40172664000, 33491222500, 32909077500, 29085444500])
        self.assertEqual(temp.sum(), 5)
        temp = df.index[0:5] == np.array(["034730", "035420", "000660", "017670", "086280"])
        self.assertEqual(temp.sum(), 5)

    def test_net_purchases_of_equities_is_same_2(self):
        #               종목명  매도거래량  매수거래량 순매수거래량   매도거래대금   매수거래대금 순매수거래대금
        #   티커
        # 095570    AJ네트웍스     2088907     2088907            0     8914353675     8914353675              0
        # 006840      AK홀딩스     1409142     1409142            0    38080803500    38080803500              0
        # 027410           BGF     4987904     4987904            0    25072969705    25072969705              0
        # 282330     BGF리테일      410697      410697            0    61421158500    61421158500              0
        # 138930   BNK금융지주    11505016    11505016            0    66265083520    66265083520              0
        df = stock.get_market_net_purchases_of_equities_by_ticker("20210115", "20210122", investor="전체")
        temp = df.iloc[0:5, -2] == np.array([8914353675, 38080803500, 25072969705, 61421158500, 66265083520])
        self.assertEqual(temp.sum(), 5)
        temp = df.index[0:5] == np.array(["095570", "006840", "027410", "282330", "138930"])
        self.assertEqual(temp.sum(), 5)

    def test_net_purchases_of_equities_is_same_in_kosdaq(self):
        #               종목명  매도거래량  매수거래량 순매수거래량   매도거래대금   매수거래대금 순매수거래대금
        #   티커
        # 236810        엔비티    10896787    12917977      2021190   441165915800   529418663600    88252747800
        # 347860        알체라    13313280    14126491       813211   498152570750   524842304250    26689733500
        # 235980      메드팩토     4013688     4234321       220633   351960930900   372072449300    20111518400
        # 039200      오스코텍     4720525     5104237       383712   214412650350   233303007450    18890357100
        # 000250    삼천당제약     2065806     2302470       236664   138698548300   154602581800    15904033500
        df = stock.get_market_net_purchases_of_equities_by_ticker("20210115", "20210122", market="KOSDAQ")
        temp = df.iloc[0:5, -1] == np.array([88252747800, 26689733500, 20111518400, 18890357100, 15904033500])
        self.assertEqual(temp.sum(), 5)
        temp = df.index[0:5] == np.array(["236810", "347860", "235980", "039200", "000250"])
        self.assertEqual(temp.sum(), 5)

    def test_net_purchases_of_equities_is_same_in_konex(self):
        #                      종목명  매도거래량  매수거래량 순매수거래량   매도거래대금   매수거래대금 순매수거래대금
        #   티커
        # 140610   엔솔바이오사이언스      181980      222487        40507     3120731650     3812653350      691921700
        # 044990   에이치엔에스하이텍       11827       47463        35636      108445380      429105250      320659870
        # 246250     에스엘에스바이오       66974       89265        22291      694143600      927707350      233563750
        # 058970                 엠로       41594       54831        13237      687397650      916045050      228647400
        # 067370             선바이오       80726       92160        11434     1512812100     1726903850      214091750
        df = stock.get_market_net_purchases_of_equities_by_ticker("20210115", "20210122", market="KONEX")
        temp = df.iloc[0:5, -1] == np.array([691921700, 320659870, 233563750, 228647400, 214091750])
        self.assertEqual(temp.sum(), 5)
        temp = df.index[0:5] == np.array(["140610", "044990", "246250", "058970", "067370"])
        self.assertEqual(temp.sum(), 5)


class StockTradingVolumeByInvestorTest(unittest.TestCase):
    def test_trading_volume_is_same_0(self):
        #                 매도       매수    순매수
        # 투자자구분
        # 금융투자    29455909   26450600  -3005309
        # 보험         1757287     509535  -1247752
        # 투신         2950680    1721970  -1228710
        # 사모          745727     696135    -49592
        # 은행           38675      46394      7719
        df = stock.get_market_trading_volume_by_investor("20210115", "20210122", "005930")
        temp = df.iloc[0:5, -1] == np.array([-3005309, -1247752, -1228710, -49592, 7719])
        self.assertEqual(temp.sum(), 5)
        temp = df.index[0:5] == np.array(["금융투자", "보험", "투신", "사모", "은행"])
        self.assertEqual(temp.sum(), 5)

    def test_trading_volume_is_same_1(self):
        #                  매도        매수    순매수
        # 투자자구분
        # 금융투자    137969209   127697577 -10271632
        # 보험         15737709     8577242  -7160467
        # 투신         46872846    34307243 -12565603
        # 사모         20780475    16342937  -4437538
        # 은행          2236667      632814  -1603853
        df = stock.get_market_trading_volume_by_investor("20210115", "20210122", "KOSPI")
        temp = df.iloc[0:5, -1] == np.array([-10271632, -7160467, -12565603, -4437538, -1603853])
        self.assertEqual(temp.sum(), 5)

    def test_trading_volume_is_same_2(self):
        #                   매도         매수     순매수
        # 투자자구분
        # 금융투자    1857447354   1660620713 -196826641
        # 보험          29594468     19872165   -9722303
        # 투신          69348190     60601427   -8746763
        # 사모          31673292     26585281   -5088011
        # 은행          44279242     51690814    7411572
        df = stock.get_market_trading_volume_by_investor("20210115", "20210122", "KOSPI", etf=True, etn=True, elw=True)
        temp = df.iloc[0:5, -1] == np.array([-196826641, -9722303, -8746763, -5088011, 7411572])
        self.assertEqual(temp.sum(), 5)


class StockTradingValueByInvestorTest(unittest.TestCase):
    def test_trading_value_is_same_0(self):
        #                      매도            매수         순매수
        # 투자자구분
        # 금융투자    2580964135000   2309054317700  -271909817300
        # 보험         153322228800     44505136200  -108817092600
        # 투신         258073006600    150715203700  -107357802900
        # 사모          65167773900     60862926800    -4304847100
        # 은행           3369626100      4004806100      635180000
        df = stock.get_market_trading_value_by_investor("20210115", "20210122", "005930")
        temp = df.iloc[0:5, -1] == np.array([-271909817300, -108817092600, -107357802900, -4304847100, 635180000])
        self.assertEqual(temp.sum(), 5)
        temp = df.index[0:5] == np.array(["금융투자", "보험", "투신", "사모", "은행"])
        self.assertEqual(temp.sum(), 5)

    def test_trading_value_is_same_1(self):
        #                       매도             매수         순매수
        # 투자자구분
        # 금융투자     9827334289654    9294592831462  -532741458192
        # 보험          912820396542     560818697065  -352001699477
        # 투신         1790231574897    1421181450288  -369050124609
        # 사모          830445404788     665802837480  -164642567308
        # 은행           58624439870      37109603010   -21514836860
        df = stock.get_market_trading_value_by_investor("20210115", "20210122", "KOSPI")
        temp = df.iloc[0:5, -1] == np.array([-532741458192, -352001699477, -369050124609, -164642567308, -21514836860])
        self.assertEqual(temp.sum(), 5)

    def test_trading_value_is_same_2(self):
        #                       매도             매수         순매수
        # 투자자구분
        # 금융투자    15985568261831   15006116511544  -979451750287
        # 보험         1219035502445     757575677208  -461459825237
        # 투신         2235561259511    1799363743367  -436197516144
        # 사모          999084910863     846067212945  -153017697918
        # 은행          886226324790     936210985810    49984661020
        df = stock.get_market_trading_value_by_investor("20210115", "20210122", "KOSPI", etf=True, etn=True, elw=True)
        temp = df.iloc[0:5, -1] == np.array([-979451750287, -461459825237, -436197516144, -153017697918, 49984661020])
        self.assertEqual(temp.sum(), 5)


class StockTradingValueByDateTest(unittest.TestCase):
    def test_trading_value_is_same_0(self):
        #                 기관합계     기타법인          개인    외국인합계  전체
        # 날짜
        # 2021-01-15 -440769209300  25442287800  661609085600 -246282164100     0
        # 2021-01-18   42323535000  22682344800   14829121700  -79835001500     0
        # 2021-01-19   95523053500  -3250422500 -173484213300   81211582300     0
        # 2021-01-20 -364476214000  22980632900  430115581000  -88619999900     0
        # 2021-01-21  -60637506300 -27880854000  250285510000 -161767149700     0
        df = stock.get_market_trading_value_by_date("20210115", "20210122", "005930")
        temp = df.iloc[0:5, 0] == np.array([-440769209300, 42323535000, 95523053500, -364476214000, -60637506300])
        self.assertEqual(temp.sum(), 5)
        self.assertIsInstance(df.index   , pd.core.indexes.datetimes.DatetimeIndex)
        self.assertIsInstance(df.index[0], pd._libs.tslibs.timestamps.Timestamp)
        self.assertTrue(df.index[0] < df.index[-1])

    def test_trading_value_is_same_1(self):
        #                  기관합계     기타법인           개인    외국인합계  전체
        # 날짜
        # 2021-01-15 -1414745885546  54444293672  2113924037705 -753622445831     0
        # 2021-01-18  -278880716957 -26004926379   514299140387 -209413497051     0
        # 2021-01-19   593956459208  21472281148 -1025418915468  409990175112     0
        # 2021-01-20 -1234485992694  34510184945  1436793223994 -236817416245     0
        # 2021-01-21  -292666343147 -13168420832   -62476631241  368311395220     0
        df = stock.get_market_trading_value_by_date("20210115", "20210122", "KOSPI")
        temp = df.iloc[0:5, 0] == np.array([-1414745885546, -278880716957, 593956459208, -1234485992694, -292666343147])
        self.assertEqual(temp.sum(), 5)
        self.assertIsInstance(df.index   , pd.core.indexes.datetimes.DatetimeIndex)
        self.assertIsInstance(df.index[0], pd._libs.tslibs.timestamps.Timestamp)

    def test_trading_value_is_same_2(self):
        #                  기관합계     기타법인           개인    외국인합계  전체
        # 날짜
        # 2021-01-15 -1536570309441  63110174617  2251672617980 -778212483156     0
        # 2021-01-18  -601428111357 -27000808439   494341183227  134087736569     0
        # 2021-01-19   544654406338  21787409868  -968965427363  402523611157     0
        # 2021-01-20 -1227642472619  32139813590  1444113501769 -248610842740     0
        # 2021-01-21  -284899892322 -19072459127   -61503500921  365475852370     0
        df = stock.get_market_trading_value_by_date("20210115", "20210122", "KOSPI", etf=True, etn=True, elw=True)
        temp = df.iloc[0:5, 0] == np.array([-1536570309441, -601428111357, 544654406338, -1227642472619, -284899892322])
        self.assertEqual(temp.sum(), 5)
        self.assertIsInstance(df.index   , pd.core.indexes.datetimes.DatetimeIndex)
        self.assertIsInstance(df.index[0], pd._libs.tslibs.timestamps.Timestamp)

    def test_trading_value_is_same_3(self):
        #                  기관합계     기타법인           개인    외국인합계  전체
        # 날짜
        # 2021-01-31 -4542136360183  98264910507  4883844366239 -439972916563     0
        df = stock.get_market_trading_value_by_date("20210115", "20210122", "KOSPI", etf=True, etn=True, elw=True, freq='m')
        temp = df.iloc[0] == np.array([-4542136360183, 98264910507, 4883844366239, -439972916563, 0])
        self.assertEqual(temp.sum(), 5)
        self.assertIsInstance(df.index   , pd.core.indexes.datetimes.DatetimeIndex)
        self.assertIsInstance(df.index[0], pd._libs.tslibs.timestamps.Timestamp)


class StockTradingVolumeByDateTest(unittest.TestCase):
    def test_trading_value_is_same_0(self):
        #            기관합계 기타법인    개인 외국인합계  전체
        # 날짜
        # 2021-01-15 -5006115  288832  7485785   -2768502     0
        # 2021-01-18   505669  262604   151228    -919501     0
        # 2021-01-19  1139258  -34023 -2044543     939308     0
        # 2021-01-20 -4157919  262408  4917655   -1022144     0
        # 2021-01-21  -712099 -321732  2890389   -1856558     0
        df = stock.get_market_trading_volume_by_date("20210115", "20210122", "005930")
        temp = df.iloc[0:5, 0] == np.array([-5006115, 505669, 1139258, -4157919, -712099])
        self.assertEqual(temp.sum(), 5)
        self.assertIsInstance(df.index   , pd.core.indexes.datetimes.DatetimeIndex)
        self.assertIsInstance(df.index[0], pd._libs.tslibs.timestamps.Timestamp)
        self.assertTrue(df.index[0] < df.index[-1])

    def test_trading_value_is_same_1(self):
        #             기관합계 기타법인      개인 외국인합계  전체
        # 날짜
        # 2021-01-15 -20393142  8435634  29119751  -17162243     0
        # 2021-01-18  -5700054 -1198498  15316328   -8417776     0
        # 2021-01-19   7216278  -246496 -24395243   17425461     0
        # 2021-01-20 -23038683  -793906  31606917   -7774328     0
        # 2021-01-21 -18443990 -7082091   8365421   17160660     0
        df = stock.get_market_trading_volume_by_date("20210115", "20210122", "KOSPI")
        temp = df.iloc[0:5, 0] == np.array([-20393142, -5700054, 7216278, -23038683, -18443990])
        self.assertEqual(temp.sum(), 5)
        self.assertIsInstance(df.index   , pd.core.indexes.datetimes.DatetimeIndex)
        self.assertIsInstance(df.index[0], pd._libs.tslibs.timestamps.Timestamp)

    def test_trading_value_is_same_2(self):
        #             기관합계 기타법인      개인 외국인합계  전체
        # 날짜
        # 2021-01-15 -26571037  8455599  37942108  -19826670     0
        # 2021-01-18 -65039501  -757841  39922005   25875337     0
        # 2021-01-19 -41855511  4320588  31709703    5825220     0
        # 2021-01-20 -23038880 -2562184  38031657  -12430593     0
        # 2021-01-21 -38539026 -8798430  38195538    9141918     0
        df = stock.get_market_trading_volume_by_date("20210115", "20210122", "KOSPI", etf=True, etn=True, elw=True)
        temp = df.iloc[0:5, 0] == np.array([-26571037, -65039501, -41855511, -23038880, -38539026])
        self.assertEqual(temp.sum(), 5)
        self.assertIsInstance(df.index   , pd.core.indexes.datetimes.DatetimeIndex)
        self.assertIsInstance(df.index[0], pd._libs.tslibs.timestamps.Timestamp)

    def test_trading_value_is_same_3(self):
        #              기관합계 기타법인       개인  외국인합계  전체
        # 날짜
        # 2021-01-31 -258249088  1167570  261341862    -4260344     0
        df = stock.get_market_trading_volume_by_date("20210115", "20210122", "KOSPI", etf=True, etn=True, elw=True, freq='m')
        temp = df.iloc[0] == np.array([-258249088, 1167570, 261341862, -4260344, 0])
        self.assertEqual(temp.sum(), 5)
        self.assertIsInstance(df.index   , pd.core.indexes.datetimes.DatetimeIndex)
        self.assertIsInstance(df.index[0], pd._libs.tslibs.timestamps.Timestamp)

class StockExhaustionRatesOfForeignInvestmentByTickerTest(unittest.TestCase):
    def test_kospi_for_specific_day(self):
        df = stock.get_exhaustion_rates_of_foreign_investment_by_ticker('20210118', "KOSPI")
        self.assertEqual(len(df), 917)
        df = stock.get_exhaustion_rates_of_foreign_investment_by_ticker('20210118', "KOSPI", True)
        self.assertEqual(len(df), 18)


class StockExhaustionRatesOfForeignInvestmentByDateTest(unittest.TestCase):
    def test_kospi_for_specific_day(self):
        df = stock.get_exhaustion_rates_of_foreign_investment_by_date("20200120", "20200120", "005930")
        self.assertEqual(len(df), 1)


if __name__ == '__main__':
    unittest.main()
