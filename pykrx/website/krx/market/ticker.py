from pykrx.website.comm import dataframe_empty_handler, singleton
from pykrx.website.krx.krxio import KrxWebIo
from pykrx.website.krx.market.core import MKD20011
from pandas import DataFrame
from datetime import datetime
import numpy as np
import pandas as pd


class _StockFinder(KrxWebIo):
    @property
    def bld(self):
        return "COM/finder_stkisu"

    def fetch(self, market="ALL", name=""):
        """30040 일자별 시세 스크래핑에서 종목 검색기
        http://marketdata.krx.co.kr/mdi#document=040204
        :param market: 조회 시장 (STK/KSQ/ALL)
        :param name  : 검색할 종목명 -  입력하지 않을 경우 전체
        :return      : 종목 검색 결과 DataFrame
        """
        result = self.post(mktsel=market, searchText=name)
        return DataFrame(result['block1'])


class _DelistingFinder(KrxWebIo):
    @property
    def bld(self):
        return "COM/finder_dellist_isu"

    def fetch(self, market="ALL", name=""):
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
        self.listed = self._get_stock_info_listed()
        self.delisted = self._get_stock_info_delisted()


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
        df = _StockFinder().fetch(market)
        df.rename(columns = {'full_code': 'ISIN', 'short_code': '티커', 'codeName': '종목', 'marketName': '시장'}, inplace=True)
        # - 티커 축약 (A037440 -> 037440)
        df['티커'] = df['티커'].apply(lambda x: x[1:7])
        df = df.drop_duplicates(['ISIN'])
        df = df.set_index('티커')
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
        df = _DelistingFinder().fetch(market)

        df = df[['shrt_isu_cd', 'isu_nm', 'isu_cd', 'market_name', 'delist_dd']]
        df.columns = ['티커', '종목', 'ISIN', '시장', '상폐일']
        # - 티커 축약 (A037440 -> 037440)
        df['티커'] = df['티커'].apply(lambda x: x[1:7])
        df = df.drop_duplicates(['ISIN'])
        df = df.set_index('티커')
        df = df.drop_duplicates(['ISIN'])
        return df

    def get_series(self, ticker):
        """입력된 종목(ticker)의 정보를 Series로 반환
        :param ticker: 6자리 종목 구분 정보
        :return      : 종목정보가 저장된 Series
            종목         삼성전자
            ISIN    KR7005930003
            시장           KOSPI
            상폐일           NaN
        """
        df = self.listed
        if ticker not in df.index:
            df = self.delisted
            if ticker not in df.index:
                return None
            # 030270 에스마크	KR7030270003
            # 030270 가희 11R	KRA030270151
            elif isinstance(df.loc[ticker], DataFrame):
                # ISIN을 기준으로 sorting 후 첫 번째 데이터를 선택
                df = df.loc[ticker].sort_values('ISIN').iloc[:1]
        return df.loc[ticker]


@dataframe_empty_handler
def get_stock_name(ticker):
    s = _StockTicker().get_series(ticker)
    return s['종목']


@dataframe_empty_handler
def get_stock_ticker_isin(ticker):
    s = _StockTicker().get_series(ticker)
    return s['ISIN']


@dataframe_empty_handler
def get_stock_market_from(ticker):
    s = _StockTicker().get_series(ticker)
    return s['시장']


################################################################################
# Index

def convert_date_string(method):
    def func_wrapper(self, dedicated, date=None):
        if date is None:
            date = datetime.now()
        if not isinstance(date, datetime):
            date = datetime.strptime(date, "%Y%m%d")
        return method(self, dedicated, date)
    return func_wrapper


def fetch_index_df(method):
    def func_wrapper(self, dedicated, date=None):
        if self.df.empty or np.count_nonzero(self.df.index.levels[0].day == date.day) == 0:
            # 02 : KOSPI / 03 : KOSDAQ
            for index in ["02", "03"]:
                df = MKD20011().fetch(date, index)
                if len(df) == 0:
                    continue

                # data formatting
                # - 3 level index : (날짜, 시장, 지수명)
                df['date'] = date
                df['ind_tp_cd'] = df['ind_tp_cd'].apply(
                    lambda x: "KOSPI" if x == "1" else "KOSDAQ")
                df = df.set_index(['date', 'ind_tp_cd', 'idx_nm'])
                self.df = self.df.append(df)

            self.df = self.df.sort_index()
        return method(self, dedicated, date)
    return func_wrapper


@singleton
class IndexTicker:
    def __init__(self):
        self.df = DataFrame()

    @convert_date_string
    @fetch_index_df
    def get_ticker(self, market, date=None):
        return self.df.loc[(date, market)].index.tolist()

    @convert_date_string
    @fetch_index_df
    def get_id(self, ticker, date=None):
        result = self.df.loc[(date, slice(None), ticker)]
        if len(result) == 0:
            print("NOT FOUND")
            return None
        return result['idx_ind_cd'].iloc[0]

    @convert_date_string
    @fetch_index_df
    def get_market(self, ticker, date=None):
        result = self.df.loc[(date, slice(None), ticker)]
        return result.index[0][1]


if __name__ == "__main__":
    pd.set_option('display.width', None)
    # print(get_stock_name("060310"))
    # print(get_stock_ticker_isin("000660"))
    # print(get_stock_market_from("000660"))
    # print(get_stock_ticker_isin("035420"))

    # Index Ticker
    #    tickers = IndexTicker().get_ticker("20190412", "KOSPI")
    #    print(tickers)
    # index_id = IndexTicker().get_id("코스피")
    # print(index_id)
    # print(IndexTicker().get_id("코스피 200", "20000201"))
