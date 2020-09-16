from pykrx.website import krx
from pykrx.website import naver
import datetime
import pandas as pd


def _datetime2string(dt, freq='d'):
    if freq.upper() == 'Y':
        return dt.strftime("%Y")
    elif freq.upper() == 'M':
        return dt.strftime("%Y%m")
    else:
        return dt.strftime("%Y%m%d")


def resample_ohlcv(df, freq, how):
    """
    :param df   : KRX OLCV format의 DataFrame
    :param freq : d - 일 / m - 월 / y - 년
    :return:    : resampling된 DataFrame
    """
    if freq != 'd' and len(df) > 0:
        if freq == 'm':
            df = df.resample('M').apply(how)
        elif freq == 'y':
            df = df.resample('Y').apply(how)
        else:
            print("choose a freq parameter in ('m', 'y', 'd')")
            raise RuntimeError
    return df


def get_recent_business_day():
    curr = datetime.datetime.now()
    prev = curr - datetime.timedelta(days=7)

    curr = _datetime2string(curr)
    prev = _datetime2string(prev)

    df = krx.get_index_ohlcv_by_date(prev, curr, "001", "KOSPI")
    return df.index[-1].strftime("%Y%m%d")


# -----------------------------------------------------------------------------
# 주식 API
# -----------------------------------------------------------------------------
def get_market_ticker_list(date=None, market="KOSPI"):
    """티커 목록 조회
    :param date: 조회 일자 (YYYYMMDD)
    :param market: 조회 시장 (KOSPI/KOSDAQ/KONEX/ALL)
    :return: ticker 리스트
    """
    if date is None:
        # 조회 시작일 / 조회 종료일
        todate = datetime.datetime.now()
        fromdate = todate - datetime.timedelta(days=7)
        todate = _datetime2string(todate)
        fromdate = _datetime2string(fromdate)

        # 삼성전자 최근 가격 정보 조회
        df = get_market_ohlcv_by_date(fromdate, todate, "005930")
        date = _datetime2string(df.index[-1])

    s = krx.get_market_ticker_and_name(date, market)
    return s.index.to_list()


def get_market_ticker_name(ticker):
    return krx.get_stock_name(ticker)


def get_business_days(year, mon):
    strt = "{}{:02d}01".format(year, mon)
    last = "{}{:02d}01".format(year, mon+1)
    df = get_market_ohlcv_by_date(strt, last, "000020")
    if df.index[-1].month != int(mon):
        df = df.iloc[:-1]
    return df.index.tolist()


def get_market_ohlcv_by_date(fromdate, todate, ticker, freq='d', adjusted=True):
    """지정된 일자의 OHLCV 조회
    :param fromdate: 조회 시작 일자 (YYYYMMDD)
    :param todate  : 조회 종료 일자 (YYYYMMDD)
    :param ticker  : 조회할 종목의 티커
    :param freq    : d - 일 / m - 월 / y - 년
    :param adjusted: 수정 종가 여부 (True/False)
    :return:
    """
    if isinstance(fromdate, datetime.datetime):
        fromdate = _datetime2string(fromdate)

    if isinstance(todate, datetime.datetime):
        todate = _datetime2string(todate)

    if adjusted:
        df = naver.get_market_ohlcv_by_date(fromdate, todate, ticker)
    else:
        df = krx.get_market_ohlcv_by_date(fromdate, todate, ticker)

    how = {'시가': 'first', '고가': 'max', '저가': 'min', '종가': 'last',
           '거래량': 'sum'}
    return resample_ohlcv(df, freq, how)


def get_market_ohlcv_by_ticker(date, market="ALL"):
    """"
    :param date    : 조회  일자 (YYYYMMDD)
    :param market  : KOSPI / KOSDAQ / KONEX
    :return        : OHLCV DataFrame
    """
    if isinstance(date, datetime.datetime):
        date = _datetime2string(date)

    return krx.get_market_ohlcv_by_ticker(date, market)


def get_market_cap_by_date(fromdate, todate, ticker, freq='d'):
    if isinstance(fromdate, datetime.datetime):
        fromdate = _datetime2string(fromdate)

    if isinstance(todate, datetime.datetime):
        todate = _datetime2string(todate)

    df = krx.get_market_cap_by_date(fromdate, todate, ticker)

    how = {'시가총액': 'last', '거래량': 'sum', '거래대금': 'sum', '상장주식수': 'last'}
    return resample_ohlcv(df, freq, how)


def get_market_cap_by_ticker(date, market="ALL"):
    if isinstance(date, datetime.datetime):
        date = _datetime2string(date)

    return krx.get_market_cap_by_ticker(date, market)


def get_exhaustion_rates_of_foreign_investment_by_ticker(date, market="ALL", balance_limit=False):
    if isinstance(date, datetime.datetime):
        date = _datetime2string(date)

    return krx.get_exhaustion_rates_of_foreign_investment_by_ticker(date, market, balance_limit)


def get_market_price_change_by_ticker(fromdate, todate):
    if isinstance(fromdate, datetime.datetime):
        fromdate = _datetime2string(fromdate)

    if isinstance(todate, datetime.datetime):
        todate = _datetime2string(todate)

    df_a = krx.get_market_price_change_by_ticker(fromdate, todate)
    if df_a.empty:
        return df_a

    # MKD80037는 상장 폐지 종목은 제외한 정보를 전달하기 때문에, 시작일의 가격
    # 정보 중에서 시가를 가져온다.
    # - 시작일이 주말일 경우를 고려해서 가까운 미래의 평일의 날짜를 얻어온다.
    # - 동화약품(000020)은 가장 오래된 상장 회사
    dt = datetime.date(int(fromdate[:4]), int(fromdate[4:6]), int(fromdate[6:]))
    dt += datetime.timedelta(days=7)
    hack = get_market_ohlcv_by_date(fromdate, dt.strftime("%Y%m%d"), "000020")
    fromdate = hack.index[0].strftime("%Y%m%d")

    # - 시작일 하루간의 가격 정보를 얻어온다.
    df_1 = krx.get_market_price_change_by_ticker(fromdate, fromdate)
    # - 시작일에는 존재하지만 기간 동안 없는(상폐) 종목을 찾아낸다.
    # - 종가/대비/등락률/거래량/거래대금을 0으로 업데이트한다.    
    cond = ~df_1.index.isin(df_a.index)
    if len(df_1[cond]) >= 1:
        df_1.loc[cond, '종가'    ] = 0
        df_1.loc[cond, '변동폭'  ] = -df_1.loc[cond, '시가']    
        df_1.loc[cond, '등락률'  ] = -100.0
        df_1.loc[cond, '거래량'  ] = 0    
        df_1.loc[cond, '거래대금'] = 0
        # 조회 정보에 상장 폐지 정보를 추가한다.    
        df_a = df_a.append(df_1[cond])
    return df_a


def get_market_fundamental_by_date(fromdate, todate, ticker, freq='d'):
    if isinstance(fromdate, datetime.datetime):
        fromdate = _datetime2string(fromdate)

    if isinstance(todate, datetime.datetime):
        todate = _datetime2string(todate)

    isin = krx.get_stock_ticker_isin(ticker)
    df = krx.get_market_fundamental_by_date(fromdate, todate, isin)
    if df.empty:
        return df

    df['PBR'] = df['PER'] * df['EPS'] / df['BPS']
    df.loc[df['BPS'] == 0, 'PBR'] = 0
    how = {'DIV': 'first', 'BPS': 'first', 'PER': 'first', 'EPS': 'first',
           'PBR': 'first'}
    return resample_ohlcv(df, freq, how)


def get_market_fundamental_by_ticker(date, market="ALL"):
    if isinstance(date, datetime.datetime):
        date = _datetime2string(date)

    df = krx.get_market_fundamental_by_ticker(date, market)
    if not df.empty:
        df['PBR'] = df['PER'] * df['EPS'] / df['BPS']
        df.loc[df['BPS'] == 0, 'PBR'] = 0
    return df


def get_market_trading_volume_by_date(fromdate, todate, market="KOSPI", on="세션", freq='d'):
    """
    :param fromdate: 조회 시작 일자 (YYYYMMDD)
    :param todate  : 조회 종료 일자 (YYYYMMDD)
    :param market  : KOSPI / KOSDAQ / KONEX
    :param on      : 세션/종류/매수/매도/전체
    :param freq    : d - 일 / m - 월 / y - 년
    :return        : 거래실적(거래량) 추이 DataFrame
    """
    if isinstance(fromdate, datetime.datetime):
        fromdate = _datetime2string(fromdate)
    if isinstance(todate, datetime.datetime):
        todate = _datetime2string(todate)

    df = krx.get_market_trading_volume_by_date(fromdate, todate, market)

    if on == "전체":
        return resample_ohlcv(df, freq, sum)
    else:
        if on not in df.columns.get_level_values(0):
            return None
        df = pd.concat([df['전체'], df[on]], axis=1)
        return resample_ohlcv(df, freq, sum)


def get_market_trading_value_by_date(fromdate, todate, market="KOSPI", on="세션", freq='d'):
    """
    :param fromdate: 조회 시작 일자 (YYYYMMDD)
    :param todate  : 조회 종료 일자 (YYYYMMDD)
    :param market  : KOSPI / KOSDAQ / KONEX
    :param freq    : d - 일 / m - 월 / y - 년
    :return        : 거래실적(거래대금) 추이 DataFrame
    """
    if isinstance(fromdate, datetime.datetime):
        fromdate = _datetime2string(fromdate)
    if isinstance(todate, datetime.datetime):
        todate = _datetime2string(todate)

    df = krx.get_market_trading_value_by_date(fromdate, todate, market)

    if on == "전체":
        return resample_ohlcv(df, freq, sum)
    else:
        df = pd.concat([df['전체'], df[on]], axis=1)
        return resample_ohlcv(df, freq, sum)


def get_market_trading_value_and_volume_by_ticker(date, market="KOSPI", investor="전체", market_detail="ST"):
    """거래실적 추이 (거래대금)
    :param date           : 조회 일자 (YYMMDD)
    :param market         : 조회 시장 (KOSPI/KOSDAQ/KONEX/ALL)
    :param investor       : 투자주체
        1000 - 금융투자
        2000 - 보험
        3000 - 투신
        3100 - 사모
        4000 - 은행
        5000 - 기타금융
        6000 - 연기금
        7050 - 기관
        7100 - 기타법인
        8000 - 개인
        9000 - 외국인
        9001 - 기타외국인
        9999 - 전체
    :param market_detail   : 세부검색항목
        복수 선택 가능 : ["주식", "ETF", "ELW", "ETN"]
        ST - STC
        EF - ETF
        EW - ELW
        EN - ETN
    :return              :
                                  종목명  매수거래량  매도거래량   순매수거래량   매수거래대금    매도거래대금  순매수거래대금
        034020                두산중공업    3540069     610138      2929931     55633172300     9686899000    45946273300
        069500                KODEX 200    5169740    4230962       938778     161877705700   132616689635    29261016065
        233740  KODEX 코스닥150 레버리지    1934459    106592       1827867      26822115070    1474326130     25347788940
        122630           KODEX 레버리지    3778502    2157651       1620851     56537672200    32152356945    24385315255
        102110               TIGER 200     574050     166359        407691      17971019205    5200620380     12770398825
    """
    if isinstance(date, datetime.datetime):
        date = _datetime2string(date)

    df = krx.get_market_trading_value_and_volume_by_ticker(date, market, investor, market_detail)
    return df


# -----------------------------------------------------------------------------
# 지수(INDEX) API
# -----------------------------------------------------------------------------
def get_index_ticker_list(date=None, market="KOSPI"):
    if date is None:
        date = get_recent_business_day()
    return krx.IndexTicker().get_ticker(market, date)

    
def get_index_portfolio_deposit_file(date, ticker):
    if isinstance(date, datetime.datetime):
        date = _datetime2string(date)

    id = krx.IndexTicker().get_id(ticker, date)
    market = krx.IndexTicker().get_market(ticker, date)
    return krx.get_index_portfolio_deposit_file(date, id, market)


def _get_index_ohlcv_by_date(fromdate, todate, ticker, freq):
    """
        :param fromdate: 조회 시작 일자 (YYYYMMDD)
        :param todate  : 조회 종료 일자 (YYYYMMDD)
        :param ticker  : 조회할 지표의 티커
        :param market  : KOSPI / KOSDAQ
        :param freq    : d - 일 / m - 월 / y - 년
        :return:
    """
    id = krx.IndexTicker().get_id(ticker, fromdate)
    market = krx.IndexTicker().get_market(ticker, fromdate)
    df = krx.get_index_ohlcv_by_date(fromdate, todate, id, market)
    how = {'시가': 'first', '고가': 'max', '저가': 'min', '종가': 'last', '거래량': 'sum'}
    return resample_ohlcv(df, freq, how)


def get_index_ohlcv_by_date(fromdate, todate, ticker, freq='d'):
    if isinstance(fromdate, datetime.datetime):
        fromdate = _datetime2string(fromdate, freq)

    if isinstance(todate, datetime.datetime):
        todate = _datetime2string(todate)
    return _get_index_ohlcv_by_date(fromdate, todate, ticker, freq)


def get_index_status_by_group(date, market="KOSPI"):
    if isinstance(date, datetime.datetime):
        date = _datetime2string(date)
    return krx.get_index_status_by_group(date, market)


def get_index_price_change_by_name(fromdate, todate, market="KOSPI"):
    if isinstance(fromdate, datetime.datetime):
        fromdate = _datetime2string(fromdate)
    if isinstance(todate, datetime.datetime):
        todate = _datetime2string(todate)
    return krx.get_index_price_change_by_name(fromdate, todate, market)


# -----------------------------------------------------------------------------
# 공매도(SHORTING) API
# -----------------------------------------------------------------------------

def get_shorting_status_by_date(fromdate, todate, ticker):
    isin = krx.get_stock_ticker_isin(ticker)
    return krx.get_shorting_status_by_date(fromdate, todate, isin)


def get_shorting_volume_by_ticker(date, market="KOSPI"):
    if isinstance(date, datetime.datetime):
        date = _datetime2string(date)

    return krx.get_shorting_volume_by_ticker(date, market)


def get_shorting_volume_by_date(fromdate, todate, ticker, market="KOSPI"):
    if isinstance(fromdate, datetime.datetime):
        fromdate = _datetime2string(fromdate)
    if isinstance(todate, datetime.datetime):
        todate = _datetime2string(todate)

    isin = krx.get_stock_ticker_isin(ticker)
    return krx.get_shorting_volume_by_date(fromdate, todate, isin, market)


def get_shorting_investor_volume_by_date(fromdate, todate, market):
    return krx.get_shorting_investor_by_date(fromdate, todate, market, "거래량")


def get_shorting_investor_price_by_date(fromdate, todate, market):
    return krx.get_shorting_investor_by_date(fromdate, todate, market, "거래대금")


def get_shorting_volume_top50(date, market):
    return krx.get_shorting_volume_top50(date, market)


def get_shorting_balance_by_date(fromdate, todate, ticker):
    isin = krx.get_stock_ticker_isin(ticker)
    mark = krx.get_stock_market_from(ticker)
    return krx.get_shorting_balance_by_date(fromdate, todate, isin, mark)


def get_shorting_balance_top50(date, market):
    return krx.get_shorting_balance_top50(date, market)


# -----------------------------------------------------------------------------
# ETF API
# -----------------------------------------------------------------------------
def get_etf_ticker_list(date=None):
    if date is None:
        date = get_recent_business_day()
    return krx.get_etf_ticker_list(date)


def get_etf_isin(ticker):
    return krx.get_etf_isin(ticker)


def get_etf_ohlcv_by_date(fromdate, todate, ticker):
    return krx.get_etf_ohlcv_by_date(fromdate, todate, ticker)


def get_etf_portfolio_deposit_file(ticker, date=None):
    if date is None:
        date = get_recent_business_day()
    return krx.get_etf_portfolio_deposit_file(ticker, date)


def get_etf_price_deviation(fromdate, todate, ticker):
    return krx.get_etf_price_deviation(fromdate, todate, ticker)


def get_etf_tracking_error(fromdate, todate, ticker):
    return krx.get_etf_tracking_error(fromdate, todate, ticker)


if __name__ == "__main__":
    pd.set_option('display.expand_frame_repr', False)
    tickers = get_market_ticker_list()
    for ticker in tickers:
        name = get_market_ticker_name(ticker)
        print(ticker, name)
    # tickers = get_market_ticker_list("20190225")
    # tickers = get_market_ticker_list()
    # tickers = get_market_ticker_list("20190225", "KOSDAQ")
    # tickers = get_market_ticker_list("20190225", "ALL")
    # print(tickers)
    # df = get_market_ticker_name("000660")
    # df = get_market_ohlcv_by_date("20190225", "20190228", "000660")
    # df = get_market_ohlcv_by_date("20190225", "20190228", "000660", adjusted=False)
    # df = get_market_ohlcv_by_date("20040418", "20140418", "000020")
    # df = get_market_ohlcv_by_ticker("20200831", "KOSPI")
    # df = get_market_ohlcv_by_ticker("20200831", "KOSDAQ")
    # df = get_market_price_change_by_ticker("20190624", "20190630")
    # df = get_market_ohlcv_by_date("20180101", "20181231", "000660", "y")
    # df = get_market_fundamental_by_ticker("20180305")
    # df = get_market_fundamental_by_date("20000101", "20181231", "092970", "m")
    # df = get_market_fundamental_by_date("20180301", "20180320", '005930')
    # df = get_market_fundamental_by_date("20180301", "20180320", '005930')
    # df = get_market_trading_volume_by_date("20200322", "20200430", 'KOSPI', '세션', 'm')
    # df = get_market_trading_value_by_date("20190101", "20200430", 'KOSPI')
    # df = get_market_trading_value_and_volume_by_ticker("20200907", "KOSPI", "전체")
    # df = get_market_trading_value_and_volume_by_ticker("20200907", market="KOSPI", investor="전체",
    #                                                    market_detail=['STC', 'ELW'])
    # df = get_market_cap_by_date("20190101", "20190131", "005930")
    # df = get_market_cap_by_date("20200101", "20200430", "005930", "m")
    # df = get_market_cap_by_ticker("20200625")
    # df = get_exhaustion_rates_of_foreign_investment_by_ticker("20200703")

    # tickers = get_index_ticker_list("20190225", "KOSDAQ")
    # print(tickers)
    # df = get_index_ohlcv_by_date("20190101", "20190228", "코스닥 150")
    # df = get_index_ohlcv_by_date("20190101", "20190228", "코스피")
    # df = get_index_ohlcv_by_date("20190101", "20190228", "코스닥")
    # df = get_index_ohlcv_by_date("20000101", "20180630", "코스피 200", "m")
    # df = get_index_price_change_by_name("20200520", "20200527", "KOSDAQ")
    # df = get_index_portfolio_deposit_file("20190412", "코스피 소형주")
    # df = krx.IndexTicker().get_id("코스피 200", "20000201")
    # df = get_index_portfolio_deposit_file("20000201", "코스피 소형주")

    # df = get_shorting_status_by_date("20181210", "20181212", "005930")
    # df = get_shorting_investor_volume_by_date("20190401", "20190405", "KOSPI")
    # df = get_shorting_investor_price_by_date("20190401", "20190405", "KOSPI")
    # df = get_shorting_volume_by_ticker("20190211", "KOSPI")
    # df = get_shorting_volume_by_date("20200101", "20200115", "005930")

    # df = get_shorting_volume_top50("20190401", "KOSPI")
    # df = get_shorting_balance_by_date("20190401", "20190405", "005930")
    # df = get_shorting_balance_top50("20190401", "KOSDAQ")

    # df = get_etf_ticker_list()
    # df = get_etf_isin("346000")
    # df = get_etf_ohlcv_by_date("20200101", "20200401", "295820")
    # df = get_etf_portfolio_deposit_file("252650", "20190329")
    # df = get_etf_price_deviation("20200101", "20200401", "295820")
    # df = get_etf_tracking_error("20200101", "20200401", "295820")
    # print(df)
    pass


