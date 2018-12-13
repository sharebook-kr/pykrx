from pykrx.market import *
from pykrx.shorting import *
from pandas import DataFrame
import re

class Krx :
    def __init__(self):
        self.stock_codes = get_stock_codes()
        self.re_ticker = re.compile("^[0-9]{6}$")
        self.re_isin = re.compile("^KR[0-9]{10}$")

    def get_tickers(self):
        return list(self.stock_codes['티커'].values)

    def get_isins(self):
        return list(self.stock_codes['ISIN'].values)

    def get_isin_by_ticker(self, ticker):
        try:
            cond = self.stock_codes['티커'] == ticker
            return self.stock_codes['ISIN'][cond].values[0]
        except IndexError:
            return None

    def get_isin_by_stock_name(self, stock_name):
        try:
            cond = self.stock_codes.index == stock_name
            return self.stock_codes['ISIN'][cond].values[0]
        except IndexError:
            return None

    def get_stock_name_by_ticker(self, ticker):
        try:
            cond = self.stock_codes['티커'] == ticker
            return self.stock_codes[cond].index[0]
        except IndexError:
            return None

    def get_stock_name_by_isin(self, isin):
        try:
            cond = self.stock_codes['ISIN'] == isin
            return self.stock_codes[cond].index[0]
        except IndexError:
            return None

    def _get_isin_and_name(self, ticker):
        if self.re_ticker.match(ticker):
            isin = self.get_isin_by_ticker(ticker)
            name = self.get_stock_name_by_isin(isin)
        elif self.re_isin.match(ticker):
            isin = ticker
            name = self.get_stock_name_by_ticker(isin)
        else:
            print("잘못된 Ticker를 입력했습니다. ")
            print(" - 올바른 예: 000660")
            raise RuntimeError
        return isin, name

    def _get_market_ohlcv(self, ticker, fromdate, todate):
        isin, name = self._get_isin_and_name(ticker)
        return get_market_ohlcv(isin, name, fromdate, todate)

    def get_shorting_status(self, ticker, fromdate, todate):
        isin, name = self._get_isin_and_name(ticker)
        return get_shorting_comprehensive_status(isin, name, fromdate, todate)

    def get_shorting_investor(self, ticker, fromdate, todate):
        isin, name = self._get_isin_and_name(ticker)
        return get_shorting_investor_status(isin, name, fromdate, todate)


if __name__ == "__main__":
    import pandas as pd
    pd.set_option('display.width', None)

    k = Krx()
    # print(k.get_tickers())
    # print(k.get_isins())
    # print(k.get_stock_name_by_ticker("060310"))
    # print(k.get_stock_name_by_ticker("0603100"))
    # print(k.get_stock_name_by_isin("KR7054620000"))
    # print(k.get_stock_name_by_isin("##054620000"))

    # print(k._get_market_ohlcv("005930", "20181201", "20181213"))

    #print(k.get_shorting_status("005930", "20181101", "20181213"))
    print(k.get_shorting_investor("005930", "20181101", "20181213"))