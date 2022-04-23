from pykrx.website.comm import dataframe_empty_handler, singleton
from pykrx.website.krx.market.core import (
    상장종목검색, 상폐종목검색, 전체지수기본정보
)
from pandas import DataFrame
import pandas as pd


@singleton
class StockTicker:
    def __init__(self):
        self.listed = self.__fetch(상장종목검색)
        self.delisted = self.__fetch(상폐종목검색)

    @dataframe_empty_handler
    def __fetch(self, what, market="전체"):
        market_dict = {"코스피": "STK", "코스닥": "KSQ", "코넥스": "KNX", "전체": "ALL"}
        market = market_dict.get(market, "ALL")
        df = what().fetch(market)
        df = df[['short_code', 'codeName', 'full_code', 'marketName']]
        df = df.replace("유가증권", "코스피")
        df.columns = ['티커', '종목', 'ISIN', '시장']
        df['시장'] = df['시장'].apply(lambda x: market_dict[x])
        df = df.set_index('티커')
        return df

    def get(self, ticker):
        """입력된 종목(ticker)의 정보를 Series로 반환

        Args:
            ticker (str): 6자리 종목 구분 정보

        Returns:
            Series : 종목정보가 저장된 시리즈

                종목         삼성전자
                ISIN    KR7005930003
                시장          코스피
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
    s = StockTicker().get(ticker)
    return s['종목']


@dataframe_empty_handler
def get_stock_ticker_isin(ticker):
    s = StockTicker().get(ticker)
    return s['ISIN']


@dataframe_empty_handler
def get_stock_ticekr_market(ticker):
    s = StockTicker().get(ticker)
    return s['시장']


# ----------------------------------------------------------------------------------------------------
# Index
# ----------------------------------------------------------------------------------------------------

@singleton
class IndexTicker:
    def __init__(self):
        self.df = self.__fetch()

    @dataframe_empty_handler
    def __fetch(self):
        # - 01 : KRX
        # - 02 : KOSPI
        # - 03 : KOSDAQ
        # - 04 : 테마
        data = []
        for market in ["01", "02", "03", "04"]:
            df = 전체지수기본정보().fetch(market)
            df = df[['IDX_IND_CD', 'IDX_NM', 'BAS_TM_CONTN', 'IND_TP_CD']]
            df.columns = ['티커', '지수명', '기준일', '그룹']

            code2market = {
                "01": "KRX",
                "02": "KOSPI",
                "03": "KOSDAQ",
                "04": "테마"
            }
            df['시장'] = code2market[market]
            # 다른 지수에 같은 티커가 존재함. 중복 문제를 피하기 위해 코스피
            # 1xxx 코스닥 2xxx로 내부에서 사용함
            #    full_code short_code    codeName marketCode marketName
            # 29         1        001      코스피        STK      KOSPI
            # 75         2        001      코스닥        KSQ     KOSDAQ
            df['티커'] = df['그룹'] + df['티커']
            data.append(df.set_index('티커'))

        return pd.concat(data).sort_index(ascending=True)

    def get_ticker(self, market, date):
        cond = (self.df['시장'] == market) & (self.df['기준일'] <= date)
        return self.df[cond].index.tolist()

    def get_name(self, ticker):
        return self.df.loc[ticker, '지수명']

    def get_market(self, ticker):
        return self.df.loc[ticker].index['시장']


if __name__ == "__main__":
    pd.set_option('display.width', None)
    # print(StockTicker().get("000660"))
    # print(IndexTicker().df.head())
    # print(IndexTicker().get_ticker("KOSPI", "19800104"))
    tickers = IndexTicker().get_ticker("KRX", "20200917")
    for ticker in tickers:
        print(ticker, IndexTicker().get_name(ticker))
