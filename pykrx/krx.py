from pykrx.market import *
from pykrx.shorting import *
from pandas import DataFrame
import re

class Krx :
    def __init__(self):
        self.stock_finder = StockFinder.scraping()
        self.re_ticker = re.compile("^[0-9]{6}$")
        self.re_isin = re.compile("^KR[0-9]{10}$")

    def get_tickers(self):
        return list(self.stock_finder['티커'].values)

    def get_isin_by_ticker(self, ticker):
        try:
            cond = self.stock_finder['티커'] == ticker
            return self.stock_finder['ISIN'][cond].values[0]
        except IndexError:
            return None

    def _get_isin(self, ticker):
        if ticker is None:
            isin = ""
        elif self.re_ticker.match(ticker):
            isin = self.get_isin_by_ticker(ticker)
        elif self.re_isin.match(ticker):
            isin = ticker
        else:
            print("잘못된 Ticker를 입력했습니다. ")
            print(" - 올바른 예: 000660")
            raise RuntimeError
        return isin

    def get_market_ohlcv(self, fromdate, todate, ticker):
        '''일자별 OHLCV
        :param fromdate: 조회 시작 일자   (YYYYMMDD)
        :param todate  : 조회 마지막 일자 (YYYYMMDD)
        :param ticker  : 종목 번호
        :return        : OHLCV DataFrame
        '''
        isin = self._get_isin(ticker)
        return MKD30040.scraping(fromdate, todate, isin)

    def get_shorting_status_by_date(self, fromdate, todate, ticker):
        '''일자별 공매도 종합 현황
        :param fromdate: 조회 시작 일자   (YYYYMMDD)
        :param todate  : 조회 마지막 일자 (YYYYMMDD)
        :param ticker  : 종목 번호
        :return        : 종합 현황 DataFrame
        '''
        isin = self._get_isin(ticker)
        return SRT02010100.scraping(isin, fromdate, todate)

    def get_shorting_volume_by_ticker(self, fromdate, todate, ticker=None, market="코스피"):
        '''종목별 공매도 거래 현황 조회
        :param fromdate: 조회 시작 일자   (YYYYMMDD)
        :param todate  : 조회 마지막 일자 (YYYYMMDD)
        :param ticker  : 종목 번호 - None일 경우 전체 조회
        :param market  : 코스피/코스닥
        :return        : 거래 현황 DataFrame
        '''
        isin = self._get_isin(ticker)
        return SRT02020100.scraping(fromdate, todate, market, isin)

    def get_shorting_volume_by_investor(self, fromdate, todate, market="코스피", inquery="거래량"):
        '''투자자별 공매도 거래 현황
        :param fromdate: 조회 시작 일자   (YYYYMMDD)
        :param todate  : 조회 마지막 일자 (YYYYMMDD)
        :param market  : 코스피/코스닥
        :param inquery : 거래량조회 / 거래대금조회
        :return        : 거래 현황 DataFrame
        '''
        return SRT02020300.scraping(fromdate, todate, inquery, market)

    def get_shorting_volume_top50(self, date, market="코스피"):
        '''공매도 거래 비중 TOP 50
        :param date    : 조회 일자   (YYYYMMDD)
        :param market  : 코스피/코스닥
        :return        : 거래 비중 DataFrame
        '''
        # date   : 조회 일자 (YYYYMMDD)
        # market : 코스피 / 코스닥
        return SRT02020400.scraping(date, market)

    def get_shorting_balance_by_ticker(self, fromdate, todate, ticker=None, market="코스피"):
        '''종목별 공매도 잔고 현황
        :param fromdate: 조회 시작 일자   (YYYYMMDD)
        :param todate  : 조회 마지막 일자 (YYYYMMDD)
        :param ticker  : 종목 번호 - None일 경우 전체 조회
        :param market  : 코스피/코스닥
        :return        : 잔고 현황 DataFrame
        '''
        isin = self._get_isin(ticker)
        return SRT02030100.scraping(fromdate, todate, market, isin)

    def get_shorting_balance_top50(self, date, market="코스피"):
        '''종목별 공매도 잔고 TOP 50
        :param date    : 조회 일자   (YYYYMMDD)
        :param market  : 코스피/코스닥
        :return        : 잔고 현황 DataFrame
        '''
        return SRT02030400.scraping(date, market)


if __name__ == "__main__":
    import pandas as pd
    pd.set_option('display.width', None)

    krx = Krx()
    # print(krx.get_tickers())

    for ticker in krx.get_tickers():
        df = krx.get_market_ohlcv("20181210", "20181212", ticker)
        print(df)
        time.sleep(1)

    # 시장 데이터
    # print(krx.get_market_ohlcv("20181210", "20181212", "005930"))

    # 공매도
    # print(krx.get_shorting_status_by_date("20181210", "20181212", "005930"))

    # print(krx.get_shorting_volume_by_ticker("20181212", "20181212"))
    # print(krx.get_shorting_volume_by_ticker("20181201", "20181212", "005930"))

    # print(krx.get_shorting_volume_by_investor("20181201", "20181212"))
    # print(krx.get_shorting_volume_by_investor("20181201", "20181212", inquery="거래량"))
    # print(krx.get_shorting_volume_by_investor("20181201", "20181212", inquery="거래대금"))

    # print(krx.get_shorting_volume_top50("20181212"))
    # print(krx.get_shorting_volume_top50("20181212", market="코스닥"))

    # print(krx.get_shorting_balance_by_ticker("20181212", "20181212"))
    # print(krx.get_shorting_balance_by_ticker("20181112", "20181212", "005930"))
    # print(krx.get_shorting_balance_top50("20181212"))
    # print(krx.get_shorting_balance_top50("20181212", market="코스닥"))

    pass