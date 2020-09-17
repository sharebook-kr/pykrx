from pykrx.website.comm import dataframe_empty_handler, singleton
from pykrx.website.krx.krxio import KrxWebIo
from pykrx.website.krx.market.core import MKD20011
from pandas import DataFrame
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

def fetch_index_df(method):
    def func_wrapper(self, *args, **kwargs):
        if self.df.empty:
            # 02 : KOSPI / 03 : KOSDAQ
            for index in ["02", "03"]:
                # date is not supported
                df = MKD20011().fetch("", index)
                if len(df) == 0:
                    continue

                df = df[['idx_nm', 'bas_idx', 'ind_tp_cd', 'bas_tm', 'idx_clss', 'idx_ind_cd']]
                df.columns = ['지수명', '기준지수', '시장', '기준시점', '구분', '티커']
                # 다른 지수에 같은 티커가 존재함. 중복 문제를 피하기 위해 코스피 1xxx 코스닥 2xxx로
                # 내부에서 사용함
                df['티커'] = df['시장'] + df['티커']
                df['시장'] = df['시장'].apply(lambda x: "KOSPI" if x == "1" else "KOSDAQ")
                df['기준시점'] = pd.to_datetime(df['기준시점'])
                df = df.set_index('티커')
                self.df = self.df.append(df)

        return method(self, *args, **kwargs)
    return func_wrapper


@singleton
class IndexTicker:
    def __init__(self):
        self.df = DataFrame()

    @fetch_index_df
    def get_ticker(self, market, date):
        cond = (self.df['시장'] == market) & (self.df['기준시점'] <= date)
        return self.df[cond].index.tolist()

    @fetch_index_df
    def get_name(self, ticker):
        return self.df.loc[ticker, '지수명']

    @fetch_index_df
    def get_market(self, ticker):
        return self.df.loc[ticker].index['시장']


if __name__ == "__main__":
    pd.set_option('display.width', None)
    print(IndexTicker().get_ticker("KOSPI", "19800104"))
    tickers = IndexTicker().get_ticker("KOSPI", "20200917")
    for ticker in tickers:
        print(ticker, IndexTicker().get_name(ticker))

