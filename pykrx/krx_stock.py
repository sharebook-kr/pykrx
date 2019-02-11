from pykrx.krx_http import MarketDataHttp
from pandas import DataFrame


class StockFinder(MarketDataHttp):
    @property
    def bld(self):
        return "COM/finder_stkisu"

    @staticmethod
    def scraping(market="전체", name=""):
        """30040 일자별 시세 스크래핑에서 종목 검색기
        http://marketdata.krx.co.kr/mdi#document=040204
        :param name  : 검색할 종목명 -  입력하지 않을 경우 전체
        :param market: 전체/코스피/코스닥 - 입력하지 않을 경우 전체
        :return      : 종목 검색 결과 DataFrame
                             ISIN      시장      티커
            3S           KR7060310000  KOSDAQ  060310
            AJ네트웍스   KR7095570008   KOSPI  095570
            AJ렌터카     KR7068400001   KOSPI  068400
            AK홀딩스     KR7006840003   KOSPI  006840
            APS홀딩스    KR7054620000  KOSDAQ  054620
            AP시스템     KR7265520007  KOSDAQ  265520
        """
        try:
            market_idx = {"코스피": "STK", "코스닥": "KSQ", "전체": "ALL"}.get(market, "ALL")
            result = StockFinder().post(mktsel=market_idx, searchText=name)
            df = DataFrame(result['block1'])
            if df.empty:
                return None
            df.columns = ['종목', 'ISIN', '시장', '티커']
            df.set_index('종목', inplace=True)
            # - 티커 축약 (A037440 -> 037440)
            df['티커'] = df['티커'].apply(lambda x: x[1:])
            return df
        except (TypeError, IndexError, KeyError) as e:
            print(e)
            return None


class DelistingFinder(MarketDataHttp):
    @property
    def bld(self):
        return "COM/finder_dellist_isu"

    @staticmethod
    def scraping(market="전체", name=""):
        """30031 상장 폐지 종목에서 종목 검색기
        http://marketdata.krx.co.kr/mdi#document=040603
        :param name  : 검색할 종목명 -  입력하지 않을 경우 전체
        :param market: 전체/코스피/코스닥 - 입력하지 않을 경우 전체
        :return      : 종목 검색 결과 DataFrame
        .                              ISIN     시장       티커    상폐일
            AK홀딩스8R           KRA006840144  KOSPI  J00684014  20140804
            AP우주통신           KR7015670003  KOSPI    A015670  20070912
            AP우주통신(1우B)     KR7015671001  KOSPI    A015675  20070912
            BHK보통주            KR7003990009  KOSPI    A003990  20090430
        """
        try:
            market_idx = {"코스피": "STK", "코스닥": "KSQ", "전체": "ALL"}.get(market, "ALL")
            result = DelistingFinder().post(mktsel=market_idx, searchText=name)                        
            df = DataFrame(result['result'])
            if df.empty:
                return None

            df = df[['isu_nm', 'isu_cd', 'market_name', 'shrt_isu_cd', 'delist_dd']]
            df.columns = ['종목', 'ISIN', '시장', '티커', '상폐일']
            df.set_index('종목', inplace=True)
            # - 티커 축약 (A037440 -> 037440)
            #df['티커'] = df['티커'].apply(lambda x: x[1:])
            return df
        except (TypeError, IndexError, KeyError) as e:
            print(e)
            return None


class MKD30040(MarketDataHttp):
    @property
    def bld(self):
        return "MKD/04/0402/04020100/mkd04020100t3_02"

    @staticmethod
    def scraping(fromdate, todate, isin):
        """30040 일자별 시세 조회
        :param fromdate: 조회 시작 일자
        :param todate: 조회 마지막 일자
        :param isin: 조회할 종목의 ISIN 번호
        :return: 일자별 시세 조회 결과 DataFrame
            acc_trdval     acc_trdvol  fluc_tp  list_shrs    mktcap     tdd_clsprc tdd_cmpr tdd_hgprc tdd_lwprc  tdd_opnprc  trd_dd
        0   80,437,317,800    813,467       1  163,647,814  16,250,228     99,300    2,800    99,700    97,100     97,200  2018/02/08
        1  106,022,586,600  1,082,264       1  163,647,814  15,792,014     96,500      400   100,500    96,000     98,000  2018/02/07
        2  104,081,455,600  1,094,871       2  163,647,814  15,726,555     96,100    1,600    96,700    93,400     94,900  2018/02/06
        3   73,279,645,300    745,562       2  163,647,814  15,988,391     97,700    3,300    99,600    97,200     99,400  2018/02/05
        4   98,290,649,100    975,164       2  163,647,814  16,528,429    101,000    2,500   103,500    99,900    103,000  2018/02/02
        """
        try:
            result = MKD30040().post(isu_cd=isin, fromdate=fromdate, todate=todate)
            return DataFrame(result['block1'])

        except (TypeError, IndexError, KeyError) as e:
            print(e)
            return None


class MKD30009(MarketDataHttp):
    @property
    def bld(self):
        return "MKD/13/1302/13020401/mkd13020401"

    def scraping(self, date, market):
        """
        :param date: 조회 일자 (YYMMDD)
        :param market: 조회 시장 (STK/KSQ/ALL)
        :return:
                          bps dvd_yld  end_pr iisu_code  isu_cd      pbr     per prv_eps rn stk_dvd totCnt     work_dt
            0   5,689    0.27  18,650      -  000250   삼천당제약   3.28   44.19     422  1      50   2157  2018/01/03
            1  37,029    2.82  28,350      -  000440  중앙에너비스  0.77   24.98   1,135  2     800         2018/01/03
            2     563       0   2,720      -  001000    신라섬유    4.83  247.27      11  3       0         2018/01/03
            3  10,036    1.75  12,600      -  001540    안국약품    1.26      84     150  4     220         2018/01/03
            4   8,266    1.07   2,815      -  001810    무림SP      0.34   24.06     117  5      30         2018/01/03
        """
        result = self.post(market_gubun=market, gubun=1, schdate=date)
        return DataFrame(result['result'])


class MKD01023(MarketDataHttp):
    @property
    def bld(self):
        return "MKD/01/0110/01100305/mkd01100305_01"

    def scraping(self, date):
        try:
            result = self.post(search_bas_yy=date)
            if len(result['block1']) == 0:
                return None
            df = DataFrame(result['block1'])
            df = df[['calnd_dd', 'kr_dy_tp', 'holdy_nm']]
            df = df.replace("-", "", regex=True)
            df.columns = ['일자', '요일', '비고']
            df.set_index('일자', inplace=True)
            df.index.name = "휴장일"
            return df
        except (TypeError, IndexError, KeyError) as e:
            print(e)
            return None

class MKD20011(MarketDataHttp):
    @property
    def bld(self):
        return "/MKD/03/0304/03040100/mkd03040100"

    def scraping(self, date):
        try:
            result = self.post(idx_upclss_cd='01', idx_midclss_cd='02', lang='ko', bz_dd=date)
            df = DataFrame(result['output'])

            df = df[['idx_nm', 'annc_tm', 'bas_tm', 'bas_idx', 'prsnt_prc', 'idx_mktcap']]
            df.columns = ['지수명', '기준시점', '발표시점', '기준지수', '현재지수', '시가총액']
            df.set_index('지수명', inplace=True)

            df = df.replace(',', '', regex=True)
            df = df.replace('', 0)
            df = df.astype({"기준지수": float, "현재지수": float, "시가총액": int}, )
            df.index.name = "KOSPI시리즈"
            return df

        except (TypeError, IndexError, KeyError) as e:
            print(e)
            return None


if __name__ == "__main__":
    print(MKD30009().scraping("19990221"))
