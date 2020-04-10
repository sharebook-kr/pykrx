from pykrx.website.comm import dataframe_empty_handler, singleton
from pykrx.website.krx.e3.etf.core import (MKD60003, MKD60007)
import datetime


@singleton
class EtfTicker:
    def __init__(self):
        self.df = None        
        self.prev_date = None

    @dataframe_empty_handler
    def _get_tickers(self, date):
        df = MKD60003().read(date)
        df['ticker'] = df['isu_cd'].str[3:9]
        df = df.set_index('ticker')
        df = df[['isu_cd', 'isu_abbrv']]
        df.columns = ['isin', 'name']
        return df

    @dataframe_empty_handler
    def get_ticker(self, date):
        """
        ETF의 티커를 조회
        :arg date 문자열 형태의 날짜정보
        :return ticker의 리스트를 반환
        """
        if self.df is None or self.prev_date != date:
            self.__update_db(date)
        return self.df.index.to_list()

    @dataframe_empty_handler
    def __update_db(self, date=None):
        if date is None:
            date = EtfTicker._get_closest_business_day()
        self.df = self._get_tickers(date)

    def get_name(self, ticker):
        if self.df is None:
            self.__update_db()
        return self.df.loc[ticker, 'name']

    def get_isin(self, ticker):
        if self.df is None:
            self.__update_db()
        return self.df.loc[ticker, 'isin']

    @staticmethod
    def _get_closest_business_day():
        now = datetime.datetime.now()
        past = now - datetime.timedelta(days=14)
        df = MKD60007().read(past.strftime("%Y%m%d"), now.strftime("%Y%m%d"), "KR7069500007")
        return df['work_dt'].iloc[0].replace("/", "")


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
    print(get_etf_ticker_list())
    print(get_etf_isin("346000"))
    print(get_etf_name("346000"))
