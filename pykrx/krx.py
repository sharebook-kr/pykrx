from pykrx.stock import *
from pykrx.bond import *
from pykrx.short import *
from pykrx.comm import *
import calendar


class Krx:
    def __init__(self):
        self.stock = KrxMarket()
        self.bond = KrxBond()
        self.short = KrxShort()

    def get_tickers(self, date=None):
        """입력된 일자의 티커를 반환한다.
        :param date: 조회할 일자 (YYMMDD) - 입력하지 않을 경우 API 호출한 날로 조회
        :return: 티커 리스트
        """
        return self.stock.ticker.get(date)

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
        df = self.stock.get_market_ohlcv(first_day_in_string, last_day_in_string, "000020")
        return df.index.tolist()

    # STOCK
    def get_market_index(self, date):
        return self.stock.get_market_index(date)

    def get_market_status_by_date(self, date):
        return self.stock.get_market_status_by_date(date)

    def get_market_price_change(self, fromdate, todate):
        return self.stock.get_market_price_change(fromdate, todate)

    def get_market_ohlcv(self, fromdate, todate, ticker):
        return self.stock.get_market_ohlcv(fromdate, todate, ticker)

    # BOND
    def get_treasury_yields_in_kerb_market(self, date):
        return self.bond.get_treasury_yields_in_kerb_market(date)

    # SHORT
    def get_shorting_status_by_date(self, fromdate, todate, ticker):
        return self.short.get_shorting_status_by_date(fromdate, todate, ticker)

    def get_shorting_volume_by_ticker(self, fromdate, todate, ticker, market="코스피"):
        return self.short.get_shorting_volume_by_ticker(fromdate, todate, ticker, market)

    def get_shorting_volume_by_investor(self, fromdate, todate, market="코스피", inquery="거래량"):
        return self.short.get_shorting_volume_by_investor(fromdate, todate, market, inquery)

    def get_shorting_volume_top50(self, date, market="코스피"):
        return self.short.get_shorting_volume_top50(date, market)

    def get_shorting_balance_by_ticker(self, fromdate, todate, ticker, market="코스피"):
        return self.short.get_shorting_balance_by_ticker(fromdate, todate, ticker, market)

    def get_shorting_balance_top50(self, date, market="코스피"):
        return self.short.get_shorting_balance_top50(date, market)


if __name__ == "__main__":
    import pandas as pd
    pd.set_option('display.width', None)
    df = Krx().get_market_ohlcv("20180103", "20180103", "066570")
