from pykrx.website.naver.core import Sise
import xml.etree.ElementTree as et
from pandas import DataFrame
import pandas as pd
import numpy as np
from datetime import datetime

# fromdate, todate, isin
def get_market_ohlcv_by_date(fromdate, todate, ticker):
    strtd = pd.to_datetime(fromdate)
    lastd = pd.to_datetime(todate)
    today = datetime.now()
    # additional requests for fluctuation rate
    elapsed = today - strtd + pd.Timedelta(days=2)

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
        df = df.astype(np.int64)
        close_1d = df['종가'].shift(1)
        df['등락률'] = (df['종가'] - close_1d) / close_1d * 100
        return df.loc[(strtd <= df.index) & (df.index <= lastd)]
    except et.ParseError:
        return DataFrame()


if __name__ == "__main__":
    # df = get_market_ohlcv_by_date("20010101", "20190820", "005930")
    # df = get_market_ohlcv_by_date("20191220", "20200227", "000020")
    # df = get_market_ohlcv_by_date("19991220", "20191231", "008480")
    df = get_market_ohlcv_by_date("20100104", "20230222", "005930")
    # df = get_market_ohlcv_by_date("20211221", "20211222", "005930")
    # df = get_market_ohlcv_by_date("20200121", "20200222", "005930")
    print(df)