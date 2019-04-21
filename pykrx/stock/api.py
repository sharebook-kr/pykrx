from pykrx.stock import market as mrkt
from pykrx.stock import index as indx
from pykrx.stock import short as shrt
from pykrx.comm.util import resample_ohlcv
import pandas as pd


# -----------------------------------------------------------------------------
# 주식 API
# -----------------------------------------------------------------------------

def get_market_ticker_list(date=None):
    return mrkt.get_stock_ticker_list(date)


def get_business_days(year, mon):
    strt = "{}{:02d}01".format(year, mon)
    last = "{}{:02d}31".format(year, mon)
    df = get_market_ohlcv_by_date(strt, last, "000020")
    return df.index.tolist()


def get_market_ohlcv_by_date(fromdate, todate, ticker, freq='d'):
    """
    :param fromdate: 조회 시작 일자 (YYYYMMDD)
    :param todate  : 조회 종료 일자 (YYYYMMDD)
    :param ticker  : 조회할 종목의 티커
    :param freq    : d - 일 / m - 월 / y - 년
    :return:
    """
    isin = mrkt.get_stock_ticker_isin(ticker)
    df = mrkt.get_market_ohlcv_by_date(fromdate, todate, isin)    
    how = {'시가': 'first', '고가': 'max', '저가': 'min', '종가': 'last',
           '거래량': 'sum'}
    return resample_ohlcv(df, freq, how)


def get_market_price_change_by_ticker(fromdate, todate):
    df_a = mrkt.get_market_price_change_by_ticker(fromdate, todate)    
    # MKD80037는 상장 폐지 종목은 제외한 정보를 전달하기 때문에, 시작일의 가격
    # 정보 중에서 시가를 가져온다.
    # - 시작일이 주말일 경우를 고려해서 가까운 미래의 평일의 날짜를 얻어온다.
    # - 동화약품(000020)은 가장 오래된 상장 회사
    hack = get_market_ohlcv_by_date(fromdate, todate, "000020")
    fromdate = hack.index[0]    
    # - 시작일 하루간의 가격 정보를 얻어온다.
    df_1 = mrkt.get_market_price_change_by_ticker(fromdate, fromdate)
    # - 시작일에는 존재하지만 기간 동안 없는(상폐) 종목을 찾아낸다.
    # - 종가/대비/등락률/거래량/거래대금을 0으로 업데이트한다.    
    cond = ~df_1.index.isin(df_a.index)    
    if len(df_1[cond]) > 1:
        df_1.loc[cond, '종가'    ] = 0
        df_1.loc[cond, '변동폭'  ] = -df_1.loc[cond, '시가']    
        df_1.loc[cond, '등락률'  ] = -100.0
        df_1.loc[cond, '거래량'  ] = 0    
        df_1.loc[cond, '거래대금'] = 0
        # 조회 정보에 상장 폐지 정보를 추가한다.    
        df_a = df_a.append(df_1[cond])            
    return df_a


def get_market_fundamental_by_date(fromdate, todate, ticker, freq='d'):
    isin = mrkt.get_stock_ticker_isin(ticker)
    df = mrkt.get_market_fundamental_by_date(fromdate, todate, isin)
    how = {'DIV': 'first', 'BPS': 'first', 'PER': 'first', 'EPS': 'first'}
    return resample_ohlcv(df, freq, how)


def get_market_fundamental_by_ticker(date, market="ALL"):
    df = mrkt.get_market_fundamental_by_ticker(date, market)
    # 추정 PBR
    df['PBR'] = df['PER'] * df['EPS'] / df['BPS']
    df.loc[df['BPS'] == 0, 'PBR'] = 0
    return df

# -----------------------------------------------------------------------------
# 지수(INDEX) API
# -----------------------------------------------------------------------------

def get_index_ticker_list(date, market="KOSPI"):
    return indx.IndexTicker().get_ticker(date, market)

    
def get_index_portfolio_deposit_file(date, ticker, market="KOSPI"):
    id = indx.IndexTicker().get_id(date, market, ticker)
    return indx.get_index_portfolio_deposit_file(date, id, market)


def _get_index_ohlcv_by_date(fromdate, todate, ticker, market, freq):
    """
        :param fromdate: 조회 시작 일자 (YYYYMMDD)
        :param todate  : 조회 종료 일자 (YYYYMMDD)
        :param ticker  : 조회할 지표의 티커
        :param market  : KOSPI / KOSDAQ
        :param freq    : d - 일 / m - 월 / y - 년
        :return:
        """
    id = indx.IndexTicker().get_id(fromdate, market, ticker)
    df = indx.get_index_ohlcv_by_date(fromdate, todate, id, market)
    how = {'시가': 'first', '고가': 'max', '저가': 'min', '종가': 'last',
           '거래량': 'sum'}
    return resample_ohlcv(df, freq, how)


def get_index_kospi_ohlcv_by_date(fromdate, todate, ticker, freq='d'):
    return _get_index_ohlcv_by_date(fromdate, todate, ticker, "KOSPI", freq)


def get_index_kosdaq_ohlcv_by_date(fromdate, todate, ticker, freq='d'):
    return _get_index_ohlcv_by_date(fromdate, todate, ticker, "KOSDAQ", freq)


def get_index_status_by_group(date, market):
    return indx.get_index_status_by_group(date, market)


# -----------------------------------------------------------------------------
# 공매도(SHORTING) API
# -----------------------------------------------------------------------------

def get_shorting_status_by_date(fromdate, todate, ticker):
    isin = mrkt.get_stock_ticker_isin(ticker)
    return shrt.get_shorting_status_by_date(fromdate, todate, isin)


def get_shorting_volume_by_ticker(date, market):    
    return shrt.get_shorting_volume_by_ticker(date, market)


def get_shorting_investor_volume_by_date(fromdate, todate, market):
    return shrt.get_shorting_investor_by_date(fromdate, todate, market, 
                                               "거래량")


def get_shorting_investor_price_by_date(fromdate, todate, market):
    return shrt.get_shorting_investor_by_date(fromdate, todate, market,
                                              "거래대금")


def get_shorting_volume_top50(date, market):
    return shrt.get_shorting_volume_top50(date, market)


def get_shorting_balance_by_ticker(fromdate, todate, ticker):
    isin = mrkt.get_stock_ticker_isin(ticker)
    mark = mrkt.get_stock_market_from(ticker)
    return shrt.get_shorting_balance_by_ticker(fromdate, todate, isin, mark)


def get_shorting_balance_top50(date, market):
    return shrt.get_shorting_balance_top50(date, market)


if __name__ == "__main__":    
    pd.set_option('display.expand_frame_repr', False)
    # df = get_market_ohlcv_by_date("20190225", "20190228", "000660")
    # df = get_market_ohlcv_by_date("20150720", "20150810", "000020", "m")
    # df = get_market_price_change_by_ticker("20180301", "20180320")
    df = get_market_fundamental_by_ticker("20180305")
    # df = get_market_fundamental_by_date("20180301", "20180320", '005930')
    # df = get_market_fundamental_by_date("20180301", "20180320", "005930", "m")
    # tickers = get_index_ticker_list("20190225", "KOSDAQ")
    # print(tickers)
    # df = get_index_kosdaq_ohlcv_by_date("20190101", "20190228", "코스닥 150")
    # df = get_index_kospi_ohlcv_by_date("20190101", "20190228", "코스피")
    # df = get_index_kosdaq_ohlcv_by_date("20190101", "20190228", "코스닥")
    # df = get_index_kospi_ohlcv_by_date("20000101", "20180630", "코스피 200", "m")
    # df = get_index_kospi_by_group("20190228")
    # df = get_index_portfolio_deposit_file("20000104", "코스피 소형주")
    # df = indx.IndexTicker().get_id("20000201", "KOSPI", "004")
    # df = get_index_portfolio_deposit_file("20000201", "코스피 소형주")
    # df = get_shorting_investor_volume_by_date("20190401", "20190405", "KOSPI")
    # df = get_shorting_investor_price_by_date("20190401", "20190405", "KOSPI")
    # df = get_shorting_volume_top50("20190401", "KOSPI")
    # df = get_shorting_balance_by_ticker("20190401", "20190405", "005930")
    # df = get_shorting_balance_top50("20190401", "KOSDAQ")
    print(df)
