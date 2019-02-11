from pykrx.krx_stock import MKD01023, MKD20011
from pykrx.krx_stock import MKD30040, MKD30009, StockFinder
from pykrx.krx_bond import MKD40038, MKD40013
from pykrx.krx_short import SRT02010100, SRT02020100
from pykrx.krx_short import SRT02020300, SRT02020400
from pykrx.krx_short import SRT02030100, SRT02030400
from pandas import DataFrame
import numpy as np
import re
import calendar


def dataframe_empty_handler(func):
    def applicator(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (AttributeError, KeyError):
            return DataFrame()
    return applicator


class Krx:
    def __init__(self):
        self.stock_finder = StockFinder.scraping()
        self.re_ticker = re.compile("^[0-9]{6}$")
        self.re_isin = re.compile("^KR[0-9]{10}$")

    def get_tickers(self, date=None):
        if date is None:
            return list(self.stock_finder['티커'].values)
        else:
            # HACK :
            df = MKD30009().scraping(date)
            return list(df.index)

    def get_isin_by_ticker(self, ticker):
        try:
            cond = self.stock_finder['티커'] == ticker
            return self.stock_finder['ISIN'][cond].values[0]
        except IndexError:
            return None

    def _get_isin(self, ticker):
        if ticker is None:
            return None
        elif self.re_ticker.match(ticker):
            isin = self.get_isin_by_ticker(ticker)
        elif self.re_isin.match(ticker):
            isin = ticker
        else:
            print("잘못된 Ticker를 입력했습니다. ")
            print(" - 올바른 예: 000660")
            raise RuntimeError
        return isin

    @dataframe_empty_handler
    def get_business_days(self, year, month):
        """영업일의 리스트를 구하는 함수
        :param year: 조회년도
        :param month: 조회월
        :return: business day list
          >> krx.get_business_days(1998, 1)
            ['19980103', '19980105', '19980106', .....]
        """

        first_day, last_day = calendar.monthrange(year, month)
        first_day_in_string = "{}{:02d}{:02d}".format(year, month, first_day)
        last_day_in_string = "{}{:02d}{:02d}".format(year, month, last_day)

        # HACK: 동화약품 (000020)은 가장 오래된 상장 기업
        df = self.get_market_ohlcv(first_day_in_string, last_day_in_string, "000020")
        return df.index.tolist()

    @staticmethod
    def get_treasury_yields_in_kerb_market(date):
        df = MKD40013().scraping(date)
        if df is None:
            return None
        return df

    @staticmethod
    def get_treasury_yields_in_bond_index(fromdate, todate):
        df = MKD40038().scraping(fromdate, todate)#.reset_index(drop=True)
        if df is None:
            return None

        if fromdate not in df.index.tolist():
            print("WARN: fromdate seems to be a holiday")
            print("- {} is used instead of {}".format(df.index[-1], fromdate))
        if todate not in df.index.tolist():
            print("WARN: todate seems to be a holiday")
            print("- {} is used instead of {}".format(df.index[0], todate))

        # 구현!
        return df

    @staticmethod
    def get_market_index(date):
        df = MKD20011().scraping(date)
        if df is None:
            return None
        return df

    @dataframe_empty_handler
    def get_market_status_by_date(self, date):
        """일자별 BPS/PER/PBR/배당수익률
        :param date : 조회 일자   (YYYYMMDD)
                       20000101 이후의 데이터 제공
        :return     : DataFrame
                           종목명   DIV    BPS      PER   EPS
            000250     삼천당제약  0.27   5689    44.19   422
            000440   중앙에너비스  2.82  37029    24.98  1135
            001000       신라섬유     0    563   247.27    11
            001540       안국약품  1.75  10036    84.00   150
            001810         무림SP  1.07   8266    24.06   117
        """
        market = {"코스피": "STK", "코스닥": "KSQ", "전체": "ALL"}.get("전체", "ALL")
        df = MKD30009().scraping(date, market)

        df = df[['isu_nm', 'isu_cd', 'dvd_yld', 'bps', 'per', 'prv_eps']]
        df.columns = ['종목명', '티커', 'DIV', 'BPS', 'PER', 'EPS']
        df.set_index('티커', inplace=True)

        df = df.replace('-', '0', regex=True)
        df = df.replace('', '0', regex=True)
        df = df.replace(',', '', regex=True)
        df = df.astype({"종목명": str, "DIV": np.float32, "BPS": np.int32, "PER": np.float32, "EPS": np.int32}, )
        return df

    @dataframe_empty_handler
    def get_market_ohlcv(self, fromdate, todate, ticker):
        """일자별 OHLCV
        :param fromdate: 조회 시작 일자   (YYYYMMDD)
        :param todate  : 조회 마지막 일자 (YYYYMMDD)
        :param ticker  : 종목 번호
        :return        : OHLCV DataFrame
                         시가     고가    저가    종가   거래량
            20180208     97200   99700   97100   99300   813467
            20180207     98000  100500   96000   96500  1082264
            20180206     94900   96700   93400   96100  1094871
            20180205     99400   99600   97200   97700   745562
        """
        isin = self._get_isin(ticker)
        df = MKD30040().scraping(fromdate, todate, isin)

        df = df[['trd_dd', 'tdd_opnprc', 'tdd_hgprc', 'tdd_lwprc', 'tdd_clsprc', 'acc_trdvol']]
        df.columns = ['날짜', '시가', '고가', '저가', '종가', '거래량']
        df = df.replace('/', '', regex=True)
        df = df.replace(',', '', regex=True).astype(np.int32)
        df = df.set_index('날짜')
        return df.sort_index()


    def get_shorting_status_by_date(self, fromdate, todate, ticker):
        """일자별 공매도 종합 현황
        :param fromdate: 조회 시작 일자   (YYYYMMDD)
        :param todate  : 조회 마지막 일자 (YYYYMMDD)
        :param ticker  : 종목 번호
        :return        : 종합 현황 DataFrame
        """
        isin = self._get_isin(ticker)
        return SRT02010100().scraping(isin, fromdate, todate)

    def get_shorting_volume_by_ticker(self, fromdate, todate, ticker=None, market="코스피"):
        """종목별 공매도 거래 현황 조회
        :param fromdate: 조회 시작 일자   (YYYYMMDD)
        :param todate  : 조회 마지막 일자 (YYYYMMDD)
        :param ticker  : 종목 번호 - None일 경우 전체 조회
        :param market  : 코스피/코스닥
        :return        : 거래 현황 DataFrame
        """
        isin = self._get_isin(ticker)
        return SRT02020100().scraping(fromdate, todate, market, isin)

    def get_shorting_volume_by_investor(self, fromdate, todate, market="코스피", inquery="거래량"):
        """투자자별 공매도 거래 현황
        :param fromdate: 조회 시작 일자   (YYYYMMDD)
        :param todate  : 조회 마지막 일자 (YYYYMMDD)
        :param market  : 코스피/코스닥
        :param inquery : 거래량조회 / 거래대금조회
        :return        : 거래 현황 DataFrame
        """
        return SRT02020300().scraping(fromdate, todate, inquery, market)

    def get_shorting_volume_top50(self, date, market="코스피"):
        """공매도 거래 비중 TOP 50
        :param date    : 조회 일자   (YYYYMMDD)
        :param market  : 코스피/코스닥
        :return        : 거래 비중 DataFrame
        """
        # date   : 조회 일자 (YYYYMMDD)
        # market : 코스피 / 코스닥
        return SRT02020400().scraping(date, market)

    def get_shorting_balance_by_ticker(self, fromdate, todate, ticker=None, market="코스피"):
        """종목별 공매도 잔고 현황
        :param fromdate: 조회 시작 일자   (YYYYMMDD)
        :param todate  : 조회 마지막 일자 (YYYYMMDD)
        :param ticker  : 종목 번호 - None일 경우 전체 조회
        :param market  : 코스피/코스닥
        :return        : 잔고 현황 DataFrame
        """
        isin = self._get_isin(ticker)
        return SRT02030100().scraping(fromdate, todate, market, isin)

    def get_shorting_balance_top50(self, date, market="코스피"):
        """종목별 공매도 잔고 TOP 50
        :param date    : 조회 일자   (YYYYMMDD)
        :param market  : 코스피/코스닥
        :return        : 잔고 현황 DataFrame
        """
        return SRT02030400().scraping(date, market)


if __name__ == "__main__":
    import pandas as pd
    pd.set_option('display.width', None)
    df = Krx().get_market_status_by_date("20180103")
    print(df.head())