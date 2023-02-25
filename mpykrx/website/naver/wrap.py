from mpykrx.website.naver.core import Sise
import xml.etree.ElementTree as et
from pandas import DataFrame
import pandas as pd
import numpy as np
from datetime import datetime

# fromdate, todate, isin
def get_market_ohlcv_by_date(fromdate, todate, ticker):
    strtd = datetime.strptime(fromdate, '%Y%m%d')
    lastd = datetime.strptime(todate, '%Y%m%d')    
    today = datetime.now()
    elapsed = today - strtd + pd.Timedelta(days=1)

    xml = Sise().fetch(ticker, elapsed.days)

    result = []
    try:
        for node in et.fromstring(xml).iter(tag='item'):
            row = node.get('data')
            result.append(row.split("|"))

        cols = ['날짜', '시가', '고가', '저가', '종가', '거래량']
        df = DataFrame(result, columns=cols)
        df = df.set_index('날짜')
        df.index = pd.to_datetime(df.index, format='%Y%m%d')
        df = df.astype(np.int32)
        return df.loc[(strtd <= df.index) & (df.index <= lastd)]
    except et.ParseError:
        return DataFrame()


if __name__ == "__main__":
    # df = get_market_ohlcv_by_date("20010101", "20190820", "005930")
    # df = get_market_ohlcv_by_date("20191220", "20200227", "000020")
    # df = get_market_ohlcv_by_date("19991220", "20191231", "008480")
    df = get_market_ohlcv_by_date("20211213", "20230222", "005930")
    # df = get_market_ohlcv_by_date("20211221", "20211222", "005930")
    # df = get_market_ohlcv_by_date("20200121", "20200222", "005930")
    print(df)