from pykrx.comm.util import dataframe_empty_handler, singleton
from pykrx.comm.http import KrxHttp
from pandas import DataFrame
import pandas as pd


class _StockFinder(KrxHttp):
    @property
    def bld(self):
        return "COM/finder_stkisu"

    def read(self, market="ALL", name=""):
        """30040 일자별 시세 스크래핑에서 종목 검색기
        http://marketdata.krx.co.kr/mdi#document=040204
        :param market: 조회 시장 (STK/KSQ/ALL)
        :param name  : 검색할 종목명 -  입력하지 않을 경우 전체
        :return      : 종목 검색 결과 DataFrame
        """
        result = self.post(mktsel=market, searchText=name)
        return DataFrame(result['block1'])


class _DelistingFinder(KrxHttp):
    @property
    def bld(self):
        return "COM/finder_dellist_isu"

    def read(self, market="ALL", name=""):
        """30031 상장 폐지 종목에서 종목 검색기
        http://marketdata.krx.co.kr/mdi#document=040603
        :param market: 조회 시장 (STK/KSQ/ALL)
        :param name  : 검색할 종목명 -  입력하지 않을 경우 전체
        :return      : 종목 검색 결과 DataFrame
        """
        result = self.post(mktsel=market, searchText=name)
        return DataFrame(result['result'])


@singleton
class _StockTicker:
    def __init__(self):
        # 조회일 기준의 상장/상폐 종목 리스트
        df_listed = self._get_stock_info_listed()
        df_delisted = self._get_stock_info_delisted()
        self.df_delisted = df_delisted                ## DEBUG !!
        # Merge two DataFrame
        self.df = pd.merge(df_listed, df_delisted, how='outer')
        self.df = self.df.set_index('티커')
        self.df = self.df.drop_duplicates(['ISIN'])

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
        df = _StockFinder().read(market)
        df.columns = ['종목', 'ISIN', '시장', '티커']
        # - 증권(7)과 사용자 영역 선택
        df = df[(df.ISIN.str[2] >= '7')]
        # - 티커 축약 (A037440 -> 037440)
        df['티커'] = df['티커'].apply(lambda x: x[1:7])
        df = df.drop_duplicates(['ISIN'])
        return df

    @dataframe_empty_handler
    def _get_stock_info_delisted(self, market="전체"):
        """조회 시점 기준의 상폐 종목 정보를 가져온다
        :param market: 전체/코스피/코스닥/코넥스 - 입력하지 않을 경우 전체
        :return      : 종목 검색 결과 DataFrame
        .                              ISIN     시장       티커    상폐일
            AK홀딩스8R           KRA006840144  KOSPI    J006840  20140804
            AP우주통신           KR7015670003  KOSPI    A015670  20070912
            AP우주통신(1우B)     KR7015671001  KOSPI    A015675  20070912
            BHK보통주            KR7003990009  KOSPI    A003990  20090430
        """
        market = {"코스피": "STK", "코스닥": "KSQ", "코넥스": "KNX", "전체": "ALL"}.get(market, "ALL")
        df = _DelistingFinder().read(market)

        df = df[['shrt_isu_cd', 'isu_nm', 'isu_cd', 'market_name', 'delist_dd']]
        df.columns = ['티커', '종목', 'ISIN', '시장', '상폐일']
        # - 증권(7)과 사용자 영역 선택
        df = df[(df.ISIN.str[2] == '7') | (df.ISIN.str[2] == '9')]
        # - 티커 축약 (A037440 -> 037440)
        df['티커'] = df['티커'].apply(lambda x: x[1:7])
        df = df.drop_duplicates(['ISIN'])
        return df


@dataframe_empty_handler
def get_stock_ticker_isin(ticker):
    stock = _StockTicker()
    return stock.df['ISIN'][ticker]


@dataframe_empty_handler
def get_stock_market_from(ticker):
    stock = _StockTicker()
    return stock.df['시장'][ticker]


@dataframe_empty_handler
def get_stock_ticker_list(date=None):
    stock = _StockTicker()
    # 조회 시점에 상장된 종목을 반환
    cond = stock.df['상폐일'].isnull()
    if date is not None:
        # 조회 일자가 지정됐다면, 조회 일자보다 앞선 상폐일을 갖은 데이터 추가
        cond |= stock.df['상폐일'] > date
    return list(stock.df[cond].index)


@dataframe_empty_handler
def get_stock_ticker_delist(todate, fromdate=None):
    stock = _StockTicker()
    cond = stock.df['상폐일'].notnull()
    if fromdate is not None:
        cond &= stock.df['상폐일'] >= fromdate
    cond &= stock.df['상폐일'] <= todate
    return list(stock.df[cond].index)


if __name__ == "__main__":
    pd.set_option('display.width', None)
    # print(get_stock_ticker_delist(fromdate="20040422", todate="20040423"))
    # print(get_stock_ticker_list())
    # print(get_stock_ticker_isin("000660"))
    market = get_stock_market_from("000660")
    print(market)
#    tickers = get_stock_ticker_list("20150720")
#    print(len(tickers))
    # print(get_stock_ticker_isin("035420"))
