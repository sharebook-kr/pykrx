from pykrx.e3 import etf
from pykrx.e3 import etn
from pykrx.e3 import elw
from pykrx.comm.util import resample_ohlcv


# -----------------------------------------------------------------------------
# ETF API
# -----------------------------------------------------------------------------

def get_etf_ticker_list(date=None):
    return etf.get_etf_ticker_list(date)

def get_etf_ohlcv_by_date(fromdate, todate, ticker, freq='d'):
    df = etf.get_etf_ohlcv_by_date(fromdate, todate, ticker)
    how = {'시가': 'first', '고가': 'max', '저가': 'min', '종가': 'last',
           '거래량': 'sum'}
    return resample_ohlcv(df, freq, how)

def get_etf_portfolio_deposit_file(date, ticker):
    return etf.get_etf_portfolio_deposit_file(date, ticker)
    

if __name__ == "__main__":    
    import pandas as pd
    pd.set_option('display.expand_frame_repr', False)
    tickers = get_etf_ticker_list()
    print(tickers)
#    df = get_etf_ohlcv_by_date("20020601", "20190405", "ARIRANG 200동일가중", 'm')   
#    df = get_etf_portfolio_deposit_file("20190405", "ARIRANG 200동일가중")   
#    print(df.head())
    