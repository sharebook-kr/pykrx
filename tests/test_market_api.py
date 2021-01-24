import unittest
from pykrx import stock
import pandas as pd
import numpy as np


class NetPurchasesOfEquitiesByTickerTest(unittest.TestCase):
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


class TradingVolumeByInvestorTest(unittest.TestCase):
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


class TradingValueByInvestorTest(unittest.TestCase):
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


class TradingValueByDateTest(unittest.TestCase):
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


class TradingVolumeByDateTest(unittest.TestCase):
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


# class MarketTickerName(unittest.TestCase):
#     def test_get_listed_stock(self):
#         name = stock.get_market_ticker_name("000660")
#         self.assertIsInstance(name, str)
#         self.assertIsNotNone(name)

#     def test_get_delisted_stock(self):
#         # 코리아오토글라스 / 코스피 / 해산사유발생 / 2015.12.29
#         name = stock.get_market_ticker_name("152330")
#         self.assertIsNotNone(name)

# class MarketTickerList(unittest.TestCase):
#     def test_with_default_param(self):
#         tickers = stock.get_market_ticker_list()
#         self.assertIsInstance(tickers, list)
#         self.assertNotEqual(len(tickers), 0)

#     def test_with_specific_business_date(self):
#         tickers = stock.get_market_ticker_list("20210115")
#         self.assertIsInstance(tickers, list)
#         self.assertNotEqual(len(tickers), 0)

#     def test_with_holiday(self):
#         tickers = stock.get_market_ticker_list("20170101")
#         self.assertIsInstance(tickers, list)
#         self.assertNotEqual(len(tickers), 0)

#     def test_io_with_specific_market(self):
#         tickers = stock.get_market_ticker_list("20190225", "KOSDAQ")
#         self.assertIsInstance(tickers, list)
#         self.assertNotEqual(len(tickers), 0)

#     def test_with_specific_business_date_and_market(self):
#         tickers = stock.get_market_ticker_list("20190225")
#         self.assertEqual(len(tickers), 901)
#         tickers = stock.get_market_ticker_list("20190225", "KOSDAQ")
#         self.assertEqual(len(tickers), 1328)
#         tickers = stock.get_market_ticker_list("20190225", "ALL")
#         self.assertEqual(len(tickers), 2382)


# class StockOhlcvByDateTest(unittest.TestCase):
#     def test_io_for_1_day(self):
#         df = stock.get_market_ohlcv_by_date("20200717", "20200717", "005930")
#         self.assertIsNotNone(df)
#         self.assertIsInstance(df.index[0], pandas.Timestamp)
#         self.assertIsInstance(df['시가'].iloc[0], np.int32)
#         self.assertIsInstance(df['고가'].iloc[0], np.int32)
#         self.assertIsInstance(df['저가'].iloc[0], np.int32)
#         self.assertIsInstance(df['종가'].iloc[0], np.int32)
#         self.assertIsInstance(df['거래량'].iloc[0], np.int32)

#     def test_io_for_n_day(self):
#         df = stock.get_market_ohlcv_by_date("20200701", "20200717", "005930")
#         self.assertIsNotNone(df)
#         self.assertIsInstance(df.index[0], pandas.Timestamp)
#         self.assertIsInstance(df['시가'].iloc[0], np.int32)
#         self.assertIsInstance(df['고가'].iloc[0], np.int32)
#         self.assertIsInstance(df['저가'].iloc[0], np.int32)
#         self.assertIsInstance(df['종가'].iloc[0], np.int32)
#         self.assertIsInstance(df['거래량'].iloc[0], np.int32)

#     def test_io_for_month(self):
#         df = stock.get_market_ohlcv_by_date("20200101", "20200630", "005930", "m")
#         self.assertIsNotNone(df)
#         self.assertEqual(len(df), 6)


# class StockOhlcvByTickerTest(unittest.TestCase):
#     def test_io(self):
#         df = stock.get_market_ohlcv_by_ticker("20180212")
#         self.assertIsInstance(df.index[0], str)
#         self.assertIsInstance(df['종목명'].iloc[0], str)
#         self.assertIsInstance(df['시가'].iloc[0], np.int32)
#         self.assertIsInstance(df['고가'].iloc[0], np.int32)
#         self.assertIsInstance(df['저가'].iloc[0], np.int32)
#         self.assertIsInstance(df['종가'].iloc[0], np.int32)
#         self.assertIsInstance(df['거래량'].iloc[0], np.int64)
#         self.assertIsInstance(df['시가총액'].iloc[0], np.int64)
#         self.assertIsInstance(df['거래대금'].iloc[0], np.int64)
#         self.assertIsInstance(df['시총비중'].iloc[0], np.float16)
#         self.assertIsInstance(df['상장주식수'].iloc[0], np.int32)





# class KrxMarketBasicTest(unittest.TestCase):
#     def test_not_empty_result(self):
#         df = stock.get_market_ohlcv_by_date("20190225", "20190228", "000660")
#         self.assertFalse(df.empty)

#         df = stock.get_market_cap_by_date("20190101", "20190131", "005930")
#         self.assertFalse(df.empty)

#         df = stock.get_market_cap_by_ticker("20200625")
#         self.assertFalse(df.empty)

#         df = stock.get_market_price_change_by_ticker("20180301", "20180320")
#         self.assertFalse(df.empty)

#         df = stock.get_market_fundamental_by_ticker("20180305", "KOSPI")
#         self.assertFalse(df.empty)

#         df = stock.get_market_fundamental_by_date("20180301", "20180320", '005930')
#         self.assertFalse(df.empty)


# class StockPriceChangeByTickerTest(unittest.TestCase):
#     def test_io_for_holiday(self):
#         df = stock.get_market_price_change_by_ticker("20040418", "20040418")
#         self.assertEqual(df.empty, True)

#     def test_io_for_weekday(self):
#         df = stock.get_market_price_change_by_ticker("20040420", "20040422")
#         self.assertIsNotNone(df)
#         self.assertNotEqual(df.empty, True)

#     def test_io_for_delisting(self):
#         #  - 상장 폐지 종목 079660 (20190625)
#         df = stock.get_market_price_change_by_ticker("20190624", "20190630")
#         self.assertEqual(df.loc['079660']['종가'], 0)
#         self.assertEqual(df.loc['079660']['등락률'], -100)


# class StockFundamentalByTickerTest(unittest.TestCase):
#     def test_io(self):
#         df = stock.get_market_fundamental_by_ticker("20180212")
#         self.assertIsInstance(df.index[0], str)
#         self.assertIsInstance(df['종목명'].iloc[0], str)
#         self.assertIsInstance(df['DIV'].iloc[0], np.float64)
#         self.assertIsInstance(df['BPS'].iloc[0], np.int32)
#         self.assertIsInstance(df['PER'].iloc[0], np.float64)
#         self.assertIsInstance(df['EPS'].iloc[0], np.int32)
#         self.assertIsInstance(df['PBR'].iloc[0], np.float64)


# class StockFundamentalByDateTest(unittest.TestCase):
#     def test_io(self):
#         df = stock.get_market_fundamental_by_date("20180212", "20180216", "006800")
#         self.assertIsInstance(df.index[0], pandas._libs.tslibs.timestamps.Timestamp)
#         self.assertIsInstance(df['DIV'].iloc[0], np.float64)
#         self.assertIsInstance(df['BPS'].iloc[0], np.int32)
#         self.assertIsInstance(df['PER'].iloc[0], np.float64)
#         self.assertIsInstance(df['EPS'].iloc[0], np.int32)
#         self.assertIsInstance(df['PBR'].iloc[0], np.float64)

#     def test_io_with_frequency(self):
#         df = stock.get_market_fundamental_by_date("20180101", "20180331", "006800", "m")
#         self.assertIsNotNone(df)
#         self.assertEqual(len(df), 3)


# class StockTradingVolumeTest(unittest.TestCase):
#     def test_io_for_kospi(self):
#         df = stock.get_market_trading_volume_by_date("20200519", "20200526", "KOSPI")
#         self.assertIsNotNone(df)
#         self.assertIsInstance(df['전체'].iloc[0], np.int64)
#         self.assertIsInstance(df['정규매매'].iloc[0], np.int64)

#     def test_io_for_kosdaq(self):
#         df = stock.get_market_trading_volume_by_date("20200519", "20200526", "KOSDAQ")
#         print(df)
#         self.assertIsNotNone(df)
#         self.assertIsInstance(df['전체'].iloc[0], np.int64)
#         self.assertIsInstance(df['정규매매'].iloc[0], np.int64)

#     def test_io_for_kosdaq(self):
#         df = stock.get_market_trading_volume_by_date("20200519", "20200526", "KONEX")
#         self.assertIsNotNone(df)
#         self.assertIsInstance(df['전체'].iloc[0], np.int64)
#         self.assertIsInstance(df['정규매매'].iloc[0], np.int64)

#     def test_io_for_month(self):
#         df = stock.get_market_trading_volume_by_date("20200101", "20200331", "KOSPI", freq="m")
#         self.assertIsNotNone(df)
#         self.assertEqual(len(df), 3)

#     def test_io_for_diverse_market(self):
#         kospi = stock.get_market_trading_volume_by_date("20200519", "20200520", "KOSPI")
#         kosdaq = stock.get_market_trading_volume_by_date("20200519", "20200520", "KOSDAQ")
#         konex = stock.get_market_trading_volume_by_date("20200519", "20200520", "KONEX")

#         self.assertNotEqual(kospi.iloc[0][1], kosdaq.iloc[0][1])
#         self.assertNotEqual(kospi.iloc[0][1], konex.iloc[0][1])


# class StockCapByTickerTest(unittest.TestCase):
#     def test_io(self):
#         df = stock.get_market_cap_by_ticker("20200101")
#         self.assertTrue(df.empty)

#         df = stock.get_market_cap_by_ticker("20200625")
#         self.assertFalse(df.empty)
#         for col in df.columns:
#             self.assertEqual(type(df[col].iloc[0]), np.int64)


# class StockCapByDateTest(unittest.TestCase):
#     def test_io(self):
#         df = stock.get_market_cap_by_date("20200101", "20200102", "005930")
#         self.assertTrue(len(df) == 1)

#         for col in df.columns:
#             self.assertEqual(type(df[col].iloc[0]), np.int64)


# class StockMarketTradingValueAndVolumeByTicker(unittest.TestCase):
#     def test_io(self):
#         df = stock.get_market_trading_value_and_volume_by_ticker("20200907")
#         self.assertFalse(df.empty)


# class StockExhaustionRatesOfForeignInvestmentByTicker(unittest.TestCase):
#     def test_io(self):
#         df = stock.get_exhaustion_rates_of_foreign_investment_by_ticker('20200703')
#         self.assertIsNotNone(df)

#     def test_io_for_diverse_market(self):
#         kospi = stock.get_exhaustion_rates_of_foreign_investment_by_ticker('20200703', "KOSPI")
#         kosdaq = stock.get_exhaustion_rates_of_foreign_investment_by_ticker('20200703', "KOSDAQ")
#         self.assertNotEqual(kospi.iloc[0][1], kosdaq.iloc[0][1])


if __name__ == '__main__':
    unittest.main()
