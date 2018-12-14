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
        elif self.re_isin.match(ticker):
            isin = ticker
        else:
            print("잘못된 Ticker를 입력했습니다. ")
            print(" - 올바른 예: 000660")
            raise RuntimeError
        return isin

    def _get_market_ohlcv(self, ticker, fromdate, todate):
        isin  = self._get_isin_and_name(ticker)
        return get_market_ohlcv(isin, "RM", fromdate, todate)

    def get_shorting_status_by_date(self, fromdate, todate, ticker):
        # fromdate : 조회 시작 일자   (YYYYMMDD)
        # todate   : 조회 마지막 일자 (YYYYMMDD)
        isin = self._get_isin_and_name(ticker)
        return SRT02010100.scraping(isin, fromdate, todate)

    def get_shorting_volume_by_ticker(self, fromdate, todate, ticker=None, market="코스피"):
        # fromdate : 조회 시작 일자   (YYYYMMDD)
        # todate   : 조회 마지막 일자 (YYYYMMDD)
        # ticker   : 조회 종목 티커/None 일 경우 전체 조회
        # market   : 코스피/코스닥

        if ticker is None:
            isin = None
        else:
            isin = self._get_isin_and_name(ticker)

        return SRT02020100.scraping(fromdate, todate, isin, market)

    def get_shorting_investor_by_date(self, fromdate, todate, inquery="거래량", market="코스피"):
        # fromdate : 조회 시작 일자   (YYYYMMDD)
        # todate   : 조회 마지막 일자 (YYYYMMDD)
        # market   : 코스피 / 코스닥
        # inquery  : 거래량조회 / 거래대금조회
        return SRT02020300.scraping(fromdate, todate, inquery, market)

    def get_shorting_trade_top50(self, date, market="코스피"):
        # date   : 조회 일자 (YYYYMMDD)
        # market : 코스피 / 코스닥
        return SRT02020400.scraping(date, market)


if __name__ == "__main__":
    import pandas as pd
    pd.set_option('display.width', None)

    krx = Krx()
    # df = krx.get_shorting_status_by_date("20181210", "20181212", "005930")

    # df = krx.get_shorting_volume_by_ticker("20181212", "20181212")
    # df = krx.get_shorting_volume_by_ticker("20181201", "20181212", "005930")

    # df = krx.get_shorting_investor_by_date("20181201", "20181212")
    # df = krx.get_shorting_investor_by_date("20181201", "20181212", inquery="거래량")
    # df = krx.get_shorting_investor_by_date("20181201", "20181212", inquery="거래대금")

    df = krx.get_shorting_trade_top50("20181212")
    # df = krx.get_shorting_trade_top50("20181212", market="코스닥")
    print(df)