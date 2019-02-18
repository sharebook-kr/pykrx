from pykrx.comm.http import MarketDataHttp
from pykrx.comm.util import dataframe_empty_handler, singleton
import pandas as pd
from pandas import DataFrame


class StockFinder(MarketDataHttp):
    @property
    def bld(self):
        return "COM/finder_stkisu"

    @staticmethod
    def read(market="ALL", name=""):
        """30040 일자별 시세 스크래핑에서 종목 검색기
        http://marketdata.krx.co.kr/mdi#document=040204
        :param market: 조회 시장 (STK/KSQ/ALL)
        :param name  : 검색할 종목명 -  입력하지 않을 경우 전체
        :return      : 종목 검색 결과 DataFrame

        """
        result = StockFinder().post(mktsel=market, searchText=name)
        return DataFrame(result['block1'])


class DelistingFinder(MarketDataHttp):
    @property
    def bld(self):
        return "COM/finder_dellist_isu"

    @staticmethod
    def read(market="ALL", name=""):
        """30031 상장 폐지 종목에서 종목 검색기
        http://marketdata.krx.co.kr/mdi#document=040603
        :param market: 조회 시장 (STK/KSQ/ALL)
        :param name  : 검색할 종목명 -  입력하지 않을 경우 전체
        :return      : 종목 검색 결과 DataFrame
        """
        result = DelistingFinder().post(mktsel=market, searchText=name)
        return DataFrame(result['result'])


@singleton
class KrxTicker:
    def __init__(self):
        # 조회일 기준의 상장/상폐 종목 리스트
        df_listed = self._get_stock_info_listed()
        df_delisted = self._get_stock_info_delisted()
        # Merge two DataFrame
        self.df = pd.merge(df_listed, df_delisted, how='outer')
        self.df = self.df.set_index('티커')

    @dataframe_empty_handler
    def _get_stock_info_listed(self, market="전체"):
        """조회 시점 기준의 상장된 종목 정보를 가져온다
        :param market: 전체/코스피/코스닥/코넥스 - 입력하지 않을 경우 전체
        :return : 종목 검색 결과 DataFrame
                            종목          ISIN    시장
            060310            3S  KR7060310000  KOSDAQ
            095570    AJ네트웍스  KR7095570008   KOSPI
            068400      AJ렌터카  KR7068400001   KOSPI
            006840      AK홀딩스  KR7006840003   KOSPI
        """
        market = {"코스피": "STK", "코스닥": "KSQ", "코넥스": "KNX", "전체": "ALL"}.get(market, "ALL")
        df = StockFinder().read(market)
        df.columns = ['종목', 'ISIN', '시장', '티커']
        # - 티커 축약 (A037440 -> 037440)
        df['티커'] = df['티커'].apply(lambda x: x[1:])
        return df

    @dataframe_empty_handler
    def _get_stock_info_delisted(self, market="전체"):
        """조회 시점 기준의 상폐 종목 정보를 가져온다
        :param market: 전체/코스피/코스닥/코넥스 - 입력하지 않을 경우 전체
        :return      : 종목 검색 결과 DataFrame
        .                              ISIN     시장       티커    상폐일
            AK홀딩스8R           KRA006840144  KOSPI  J00684014  20140804
            AP우주통신           KR7015670003  KOSPI    A015670  20070912
            AP우주통신(1우B)     KR7015671001  KOSPI    A015675  20070912
            BHK보통주            KR7003990009  KOSPI    A003990  20090430
        """
        market = {"코스피": "STK", "코스닥": "KSQ", "코넥스": "KNX", "전체": "ALL"}.get(market, "ALL")
        df = DelistingFinder().read(market)

        df = df[['shrt_isu_cd', 'isu_nm', 'isu_cd', 'market_name', 'delist_dd']]
        df.columns = ['티커', '종목', 'ISIN', '시장', '상폐일']
        # - 티커 축약 (A037440 -> 037440)
        df['티커'] = df['티커'].apply(lambda x: x[1:])
        return df

    @dataframe_empty_handler
    def get(self, date=None):
        # 조회 시점에 상장된 종목을 반환
        cond = self.df['상폐일'].isnull()
        if date is not None:
            # 조회 일자가 지정됐다면, 조회 일자보다 앞선 상폐일을 갖은 데이터 추가
            cond |= self.df['상폐일'] < date
        return list(self.df[cond].index)

    @dataframe_empty_handler
    def get_isin(self, ticker):
        return self.df['ISIN'][ticker]


if __name__ == "__main__":
    pd.set_option('display.width', None)
    ticker = KrxTicker()
    print(ticker.get())
