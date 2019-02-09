from pykrx.krx_http import MarketDataHttp
from pandas import DataFrame
import numpy as np


class StockFinder(MarketDataHttp):
    @property
    def bld(self):
        return "COM/finder_stkisu"

    @staticmethod
    def scraping(market="전체", name=""):
        '''30040 일자별 시세 스크래핑에서 종목 검색기
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
        '''
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
        '''30031 상장 폐지 종목에서 종목 검색기
        http://marketdata.krx.co.kr/mdi#document=040603
        :param name  : 검색할 종목명 -  입력하지 않을 경우 전체
        :param market: 전체/코스피/코스닥 - 입력하지 않을 경우 전체
        :return      : 종목 검색 결과 DataFrame
        .                              ISIN     시장       티커    상폐일
            AK홀딩스8R           KRA006840144  KOSPI  J00684014  20140804
            AP우주통신           KR7015670003  KOSPI    A015670  20070912
            AP우주통신(1우B)     KR7015671001  KOSPI    A015675  20070912
            BHK보통주            KR7003990009  KOSPI    A003990  20090430
        '''
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
        '''30040 일자별 시세 조회
        :param fromdate: 조회 시작 일자
        :param todate: 조회 마지막 일자
        :param isin: 조회할 종목의 ISIN 번호
        :return: 일자별 시세 조회 결과 DataFrame
        .                  시가     고가    저가    종가   거래량
            2018/02/08     97200   99700   97100   99300   813467
            2018/02/07     98000  100500   96000   96500  1082264
            2018/02/06     94900   96700   93400   96100  1094871
            2018/02/05     99400   99600   97200   97700   745562
        '''
        try:
            result = MKD30040().post(isu_cd=isin, fromdate=fromdate, todate=todate)
            df = DataFrame(result['block1'])
            if df.empty:
                return None

            df = df[['trd_dd', 'tdd_opnprc', 'tdd_hgprc', 'tdd_lwprc', 'tdd_clsprc', 'acc_trdvol']]
            df.columns = ['날짜', '시가', '고가', '저가', '종가', '거래량']
            df.set_index('날짜', inplace=True)
            df = df.replace({',': ''}, regex=True).astype(np.int64)
            df.index.name = isin
            return df
        except (TypeError, IndexError, KeyError) as e:

            print(e)
            return None

class MKD30009(MarketDataHttp):
    @property
    def bld(self):
        return "MKD/13/1302/13020401/mkd13020401"

    def scraping(self, date, market="전체"):
        try:
            market_idx = {"코스피": "STK", "코스닥": "KSQ", "전체": "ALL"}.get(market, "ALL")
            result = self.post(market_gubun=market_idx, gubun=1, schdate=date)
            if len(result['result']) == 0:
                return None
            df = DataFrame(result['result'])
            df = df[['isu_nm', 'isu_cd', 'dvd_yld', 'bps', 'per', 'prv_eps']]
            df.columns = ['종목명', '티커', 'DIV', 'BPS', 'PER', 'EPS']
            df.set_index('티커', inplace=True)

            df = df.replace({',':'', '-':'0'}, regex=True)
            df = df.astype({"종목명": str, "DIV": str, "BPS": int, "PER": float, "EPS": int}, )
            df.index.name = date
            return df
        except (TypeError, IndexError, KeyError) as e:
            print(e)
            return None

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
    import pandas as pd
    import matplotlib.pyplot as plt
    import matplotlib.pyplot as plt
    from matplotlib import font_manager

    font_fname = 'C:/Windows/Fonts/malgunsl.ttf'
    font_family = font_manager.FontProperties(fname=font_fname).get_name()
    plt.rcParams["font.family"] = font_family
    pd.set_option('display.expand_frame_repr', False)
    print(MKD30009().scraping("19990221"))
