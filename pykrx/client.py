from pykrx.core import *
from pandas import DataFrame

class Krx :
    def __init__(self):
        self.stock_codes = self._stock_codes()

    def _stock_codes(self):
        result = KrxStockFinder().post(mktsel="ALL")
        # DataFrame으로 저장
        df = DataFrame(result['block1'])
        # - ISIN, 시장, 티커 column으로 구성
        df.columns = ['종목', 'ISIN', '시장', '티커']
        # - 종목이름을 index로 설정
        df.set_index('종목', inplace=True)
        # - 티커 축약 (A037440 -> 037440)
        df['티커'] = df['티커'].apply(lambda x: x[1:])
        return df

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

    def get_ohlcv(self, ticker, fromdate, todate):
        if len(ticker) == 6:
            isin = self.get_isin_by_ticker(ticker)
        else:
            isin = ticker
        result = KrxDailiyPrice().post(isu_cd=isin, fromdate=fromdate, todate=todate)

        # DataFrame으로 저장
        df = DataFrame(result['block1'])
        df = df[['trd_dd', 'tdd_opnprc', 'tdd_hgprc', 'tdd_lwprc', 'tdd_clsprc', 'acc_trdvol']]
        df.columns = ['날짜', '시가', '고가', '저가', '종가', '거래량']
        df.set_index('날짜', inplace=True)

        # 숫자를 문자로 변경하자.
        return df


if __name__ == "__main__":
    import pandas as pd
    pd.set_option('max_colwidth', 800)

    k = Krx()
    print(k.get_tickers())
    print(k.get_isins())
    print(k.get_stock_name_by_ticker("060310"))
    print(k.get_stock_name_by_ticker("0603100"))
    print(k.get_stock_name_by_isin("KR7054620000"))
    print(k.get_stock_name_by_isin("##054620000"))

    print(k.get_ohlcv("000660", "20181201", "20181212"))