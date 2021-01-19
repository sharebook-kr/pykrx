from pykrx.website.comm import dataframe_empty_handler, singleton
from pykrx.website.krx.e3.core import 상장종목검색
import datetime


@singleton
class EtfTicker:
    def __init__(self):
        self.df = self._get_tickers()

    @dataframe_empty_handler
    def _get_tickers(self):
        df = 상장종목검색().fetch()
        df.columns = ['isin', 'ticker', 'name']
        return df.set_index('ticker')

    @dataframe_empty_handler
    def get_ticker(self, date):
        return self.df.index.to_list()

    def get_name(self, ticker):
        return self.df.loc[ticker, 'name']

    def get_isin(self, ticker):
        return self.df.loc[ticker, 'isin']

@dataframe_empty_handler
def get_etf_name(ticker):
    return EtfTicker().get_name(ticker)


def get_etf_isin(ticker):
    return EtfTicker().get_isin(ticker)


def get_etf_ticker_list(date):
    return EtfTicker().get_ticker(date)


if __name__ == "__main__":
    import pandas as pd
    pd.set_option('display.width', None)
    print(get_etf_ticker_list("20200717"))
    print(get_etf_isin("346000"))
    print(get_etf_name("346000"))
