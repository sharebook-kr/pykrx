from pykrx.website.comm import dataframe_empty_handler, singleton
from pykrx.website.krx.etx.core import (
    ETF_전종목기본종목, ETN_전종목기본종목, ELW_전종목기본종목
)
import pandas as pd


@singleton
class EtxTicker:
    def __init__(self):
        self.df = self._get_tickers()

    @dataframe_empty_handler
    def _get_tickers(self):
        df_etf = ETF_전종목기본종목().fetch()
        df_etf = df_etf[["ISU_CD", "ISU_SRT_CD", "ISU_ABBRV", "LIST_DD"]].copy()
        df_etf['CATEGORY'] = "ETF"

        df_etn = ETN_전종목기본종목().fetch()
        df_etn = df_etn[["ISU_CD", "ISU_SRT_CD", "ISU_ABBRV", "LIST_DD"]].copy()
        df_etn['CATEGORY'] = "ETN"

        df_elw = ELW_전종목기본종목().fetch()
        df_elw = df_elw[["ISU_CD", "ISU_SRT_CD", "ISU_ABBRV", "LIST_DD"]].copy()
        df_elw['CATEGORY'] = "ELW"

        df = pd.concat([df_etf, df_etn, df_elw])
        df.columns = ["isin", "ticker", "종목명", "상장일", "시장"]
        df = df.replace('/', '', regex=True)
        return df.set_index('ticker')

    def get_ticker(self, market, date) -> list:
        if market == "ALL":
            return self.df.index.to_list()
        cond1 = self.df['시장'] == market
        cond2 = self.df['상장일'] <= date
        return self.df[cond1 & cond2].index.to_list()

    def get_name(self, ticker) -> str:
        return self.df.loc[ticker, '종목명']

    def get_market(self, ticker) -> str:
        return self.df.loc[ticker].index['시장']


def get_etx_name(ticker):
    return EtxTicker().get_name(ticker)


def get_etx_ticker_list(date: str, market: str) -> list:
    """ETF/ETN/ELW에서 사용되는 티커 목록 조회

    Args:
        date   (str): 조회 일자 (YYMMDD)
        market (str): 조회 시장 (ETF/ETN/ELW/ALL)

    Returns:
        list:  ['069500', '069660', ....]
    """
    return EtxTicker().get_ticker(market.upper(), date)


def is_etf(ticker):
    return EtxTicker().df.loc[ticker, '시장'] == 'ETF'


def is_etn(ticker):
    return EtxTicker().df.loc[ticker, '시장'] == 'ETN'


def is_elw(ticker):
    return EtxTicker().df.loc[ticker, '시장'] == 'ELW'


def get_etx_isin(ticker):
    return EtxTicker().df.loc[ticker, 'isin']


if __name__ == "__main__":
    pd.set_option('display.width', None)
    # df = get_etx_ticker_list("ETF", "20021014")
    # print(df)
    # print(len(df))
    print(is_etn("580011"))
    # print(get_etf_ticker_list("20200717"))
    # print(get_etf_isin("346000"))
    # print(get_etf_name("346000"))
