from pykrx.core import *
from pandas import DataFrame
import numpy as np


class MarketDataHttp(KrxHttp):
    @property
    def otp_url(self):
        return "http://marketdata.krx.co.kr/contents/COM/GenerateOTP.jspx"

    @property
    def contents_url(self):
        return "http://marketdata.krx.co.kr/contents"

    @property
    def uri(self):
        return "/MKD/99/MKD99000001.jspx"


class StockFinder(MarketDataHttp, Singleton):
    @property
    def bld(self):

        return "COM/finder_stkisu"

    @staticmethod
    def scraping(name="", market="전체"):
        '''30040 일자별 시세 스크래핑에서 종목 검색기
        http://marketdata.krx.co.kr/mdi#document=040204
        :param name  : 검색할 종목명 -  입력하지 않을 경우 전체
        :param market: 전체/코스피/코스닥 - 입력하지 않을 경우 전체
        :return      : 종목 검색 결과 DataFrame
        '''
        try:
            market_idx = {"코스피": "STK", "코스닥": "KSQ", "전체": "ALL"}.get(market, "ALL")
            result = StockFinder().post(mktsel=market_idx, searchText=name)

            df = DataFrame(result['block1'])
            df.columns = ['종목', 'ISIN', '시장', '티커']
            df.set_index('종목', inplace=True)
            # - 티커 축약 (A037440 -> 037440)
            df['티커'] = df['티커'].apply(lambda x: x[1:])
            return df
        except (TypeError, IndexError, KeyError) as e:
            print(e)
            return None


class MKD30040(MarketDataHttp, Singleton):
    @property
    def bld(self):
        return "MKD/04/0402/04020100/mkd04020100t3_02"

    @staticmethod
    def scraping(fromdate, todate, isin):
        try:
            result = MKD30040().post(isu_cd=isin, fromdate=fromdate, todate=todate)

            df = DataFrame(result['block1'])
            df = df[['trd_dd', 'tdd_opnprc', 'tdd_hgprc', 'tdd_lwprc', 'tdd_clsprc', 'acc_trdvol']]
            df.columns = ['날짜', '시가', '고가', '저가', '종가', '거래량']
            df.set_index('날짜', inplace=True)
            df = df.replace({',': ''}, regex=True).astype(np.int64)
            df.index.name = isin
            return df
        except (TypeError, IndexError, KeyError) as e:
            print(e)
            return None



if __name__ == "__main__":
    # print(StockFinder.scraping())
    # print(StockFinder().scraping(market="코스닥"))
    # print(StockFinder().scraping(name="삼성전자"))

    print(MKD30040().scraping("20181205", "20181212", "KR7005930003"))
    pass