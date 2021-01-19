from pykrx.website import krx
from pykrx.website import naver
import datetime
import inspect
import functools
import pandas as pd
from deprecated import deprecated
from pandas import DataFrame


def market_valid_check(func):
    sig = inspect.signature(func)
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if 'market' in sig.bind_partial(*args, **kwargs).arguments:
            valid_market_list = ["ALL", "KOSPI", "KOSDAQ", "KONEX"]
            for v in args:
                if v in  valid_market_list:
                    return func(*args, **kwargs)
            print(f"market 옵션이 올바르지 않습니다." )
            return None
        return func(*args, **kwargs)
    return wrapper

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


def get_nearest_business_day_in_a_week():
    curr = datetime.datetime.now()
    prev = curr - datetime.timedelta(days=7)
    curr = _datetime2string(curr)
    prev = _datetime2string(prev)
    df = krx.get_index_ohlcv_by_date(prev, curr, "1001")
    return df.index[-1].strftime("%Y%m%d")


# -----------------------------------------------------------------------------
# 주식 API
# -----------------------------------------------------------------------------
def get_market_ticker_list(date: str=None, market: str="KOSPI") -> list:
    """티커 목록 조회

    Args:
        date   (str, optional):조회 일자 (YYYYMMDD)
        market (str, optional): 조회 시장 (KOSPI/KOSDAQ/KONEX/ALL)

    Returns:
        list: 티커가 담긴 리스트
    """
    if date is None:
        date = get_nearest_business_day_in_a_week()

    s = krx.get_market_ticker_and_name(date, market)
    return s.index.to_list()


def get_market_ticker_name(ticker: str) -> str:
    """티커에 대응되는 종목 이름 반환

    Args:
        ticker (str): 티커

    Returns:
        str: 종목명
    """
    return krx.get_stock_name(ticker)


def get_business_days(year: int, mon: int) -> list:
    """영업일을 조회해서 리스트로 반환

    Args:
        year (int): 4자리 년도
        mon  (int): 월

    Returns:
        list: 지정된 월의 영업일을 조회

    Note: 과거의 영업일만을 반환
    """
    strt = "{}{:02d}01".format(year, mon)
    last = "{}{:02d}01".format(year, mon+1)
    df = get_market_ohlcv_by_date(strt, last, "000020")
    if df.index[-1].month != int(mon):
        df = df.iloc[:-1]
    return df.index.tolist()


def get_market_ohlcv_by_date(fromdate: str, todate: str, ticker: str, freq: str='d', adjusted: bool=True,
                             name_display: bool=False) -> DataFrame:
    """특정 종목의 일자별로 정렬된 OHLCV

    Args:
        fromdate     (str           ): 조회 시작 일자 (YYYYMMDD)
        todate       (str           ): 조회 종료 일자 (YYYYMMDD)
        ticker       (str           ): 조회할 종목의 티커
        freq         (str,  optional):  d - 일 / m - 월 / y - 년
        adjusted     (bool, optional): 수정 종가 여부 (True/False)
        name_display (bool, optional): columns의 이름 출력 여부 (True/False)

    Returns:
        DataFrame:
                         시가   고가   저가   종가   거래량
            날짜
            2019-02-25  77300  77600  74800  75400  2865712
            2019-02-26  75000  75900  74100  75200  2483512
            2019-02-27  75300  75700  73700  73700  2864044
            2019-02-28  72500  72600  69900  70000  7869451
    """
    if isinstance(fromdate, datetime.datetime):
        fromdate = _datetime2string(fromdate)

    if isinstance(todate, datetime.datetime):
        todate = _datetime2string(todate)

    if adjusted:
        df = naver.get_market_ohlcv_by_date(fromdate, todate, ticker)
    else:
        df = krx.get_market_ohlcv_by_date(fromdate, todate, ticker)

    if name_display:
        df.columns.name = get_market_ticker_name(ticker)

    how = {'시가': 'first', '고가': 'max', '저가': 'min', '종가': 'last',
           '거래량': 'sum'}
    return resample_ohlcv(df, freq, how)

@market_valid_check
def get_market_ohlcv_by_ticker(date, market="KOSPI"):
    """티커별로 정리된 전종목 OHLCV

    Args:
        date   (str): 조회 일자 (YYYYMMDD)
        market (str): 조회 시장 (KOSPI/KOSDAQ/KONEX/ALL)

    Returns:
        DataFrame:
                        종목명   시가   고가   저가   종가  거래량    거래대금
            티커
            060310          3S   2150   2390   2150   2190  981348  2209370985
            095570  AJ네트웍스   3135   3200   3100   3130   89871   282007385
            006840    AK홀딩스  17050  17200  16500  16500   30567   512403000
            054620   APS홀딩스   8550   8740   8400   8650  647596  5525789290
            265520    AP시스템  22150  23100  22050  22400  255846  5798313650
    """
    if isinstance(date, datetime.datetime):
        date = _datetime2string(date)

    return krx.get_market_ohlcv_by_ticker(date, market)


def get_market_cap_by_date(fromdate: str, todate: str, ticker: str, freq: str='d'):
    """일자별로 정렬된 시가총액

    Args:
        fromdate (str           ): 조회 시작 일자 (YYYYMMDD)
        todate   (str           ): 조회 종료 일자 (YYYYMMDD)
        ticker   (str           ): 티커
        freq     (str,  optional):  d - 일 / m - 월 / y - 년

    Returns:
        DataFrame:
                               시가총액  거래량      거래대금 상장주식수
            날짜
            2015-07-20  187806654675000  128928  165366199000  147299337
            2015-07-21  186039062631000  194055  244129106000  147299337
            2015-07-22  184566069261000  268323  333813094000  147299337
            2015-07-23  181767381858000  208965  259446564000  147299337
            2015-07-24  181030885173000  196584  241383636000  147299337
    """
    if isinstance(fromdate, datetime.datetime):
        fromdate = _datetime2string(fromdate)

    if isinstance(todate, datetime.datetime):
        todate = _datetime2string(todate)

    df = krx.get_market_cap_by_date(fromdate, todate, ticker)

    how = {'시가총액': 'last', '거래량': 'sum', '거래대금': 'sum', '상장주식수': 'last'}
    return resample_ohlcv(df, freq, how)

@market_valid_check
def get_market_cap_by_ticker(date, market="ALL", acending=False):
    """티커별로 정렬된 시가총액

    Args:
        date                 (str): 조회 일자 (YYYYMMDD)
        market     (str, optional): 조회 시장 (KOSPI/KOSDAQ/KONEX/ALL)
        ascending (bool, optional): 정렬 기준.

    Returns:
        DataFrame :
                      종가         시가총액    거래량         거래대금   상장주식수
            티커
            005930   51900  309831714345000  18541624  309831714345000   5969782550
            000660   84300   61370599369500   3397112   61370599369500    728002365
            207940  815000   53924475000000    163339   53924475000000     66165000
            035420  269500   44268984952500   1196267   44268984952500    164263395
            068270  316000   42640845660000    918369   42640845660000    134939385
    """
    if isinstance(date, datetime.datetime):
        date = _datetime2string(date)

    return krx.get_market_cap_by_ticker(date, market, acending)


def get_exhaustion_rates_of_foreign_investment_by_date(fromdate: str, todate: str, ticker: str) -> DataFrame:
    """지정된 종목의 일자별로 정렬된 외국인 보유 수량 및 한도 수량

    Args:
        fromdate (str): 조회 시작 일자 (YYYYMMDD)
        todate   (str): 조회 종료 일자 (YYYYMMDD)
        ticker   (str): 종목의 티커

    Returns:
        DataFrame:
                        상장주식수    보유수량    지분율    한도수량 한도소진율
            날짜
            2021-01-08  5969782550  3314966371  55.53125  5969782550   55.53125
            2021-01-11  5969782550  3324115988  55.68750  5969782550   55.68750
            2021-01-12  5969782550  3318676206  55.59375  5969782550   55.59375
            2021-01-13  5969782550  3316551070  55.56250  5969782550   55.56250
            2021-01-14  5969782550  3314652740  55.53125  5969782550   55.53125
    """
    if isinstance(fromdate, datetime.datetime):
        fromdate = _datetime2string(fromdate)

    if isinstance(todate, datetime.datetime):
        todate = _datetime2string(todate)

    return krx.get_exhaustion_rates_of_foreign_investment_by_ticker(fromdate, todate, ticker)


@market_valid_check
def get_exhaustion_rates_of_foreign_investment_by_ticker(date: str, market: str="KOSPI", balance_limit: bool=False) -> DataFrame:
    """특정 시장에서 티커로 정렬된 외국인 보유량 조회

    Args:
        date          (str ): 조회 시작 일자 (YYYYMMDD)
        market        (str ): 조회 시장 (KOSPI/KOSDAQ/KONEX/ALL)
        balance_limit (bool): 외국인보유제한종목
            - 0 : check X
            - 1 : check O

    Returns:
        DataFrame:
                   상장주식수   보유수량     지분율   한도수량 한도소진율
            티커
            003490   94844634   12350096  13.023438   47412833  26.046875
            003495    1110794      29061   2.619141     555286   5.230469
            015760  641964077  127919592  19.937500  256785631  49.812500
            017670   80745711   28962369  35.875000   39565398  73.187500
            020560  223235294   13871465   6.210938  111595323  12.429688
    """
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


def get_market_fundamental_by_date(fromdate, todate, ticker, freq='d', name_display=False):
    if isinstance(fromdate, datetime.datetime):
        fromdate = _datetime2string(fromdate)

    if isinstance(todate, datetime.datetime):
        todate = _datetime2string(todate)

    df = krx.get_market_fundamental_by_date(fromdate, todate, ticker)
    if df.empty:
        return df

    if name_display:
        df.columns.name = get_market_ticker_name(ticker)

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


def get_market_trading_value_and_volume_by_ticker(date, market="KOSPI", investor="전체", market_detail="STC"):
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
        복수 선택 가능 : ["STC", "ETF", "ELW", "ETN"]
        - STC : 일반 주식
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
def get_index_ticker_list(date: str=None, market: str="KOSPI") -> list:
    """모든 지수 (index)의 티커 조회

    Args:
        date   (str, optional): 조회 일자 (YYMMDD)
        market (str, optional): 조회 시장 (KOSPI/KOSDAQ/KRX/테마)

    Returns:
        list:
            ['1001', '1002', '1003', '1004', '1005', '1006', '1007', '1008', '1009', '1010', '1011', '1012', '1013', '1014', '1015', '1016', '1017', '1018', '1019', '1020', '1021', '1022', '1024', '1025', '1026', '1027', '1028', '1034', '1035', '1150', '1151', '1152', '1153', '1154', '1155', '1156', '1157', '1158', '1159', '1160', '1167', '1182', '1224', '1227', '1232', '1244']

        for ticker in stock.get_index_ticker_list():
            print(ticker, stock.get_index_ticker_name(ticker))
    """
    if date is None:
        date = datetime.datetime.now()
    if isinstance(date, datetime.datetime):
        date = _datetime2string(date)

    return krx.IndexTicker().get_ticker(market, date)


def get_index_ticker_name(ticker):
    return krx.IndexTicker().get_name(ticker)


def get_index_portfolio_deposit_file(ticker, date=None):
    """지수 구성 종목 조회
        :param ticker  : 조회할 지표의 티커
        :param date           : 조회 일자 (YYMMDD)
        :return 구성 종목의 티커를 리스트로 반환
    """
    if date is None:
        date = get_nearest_business_day_in_a_week()
    if isinstance(date, datetime.datetime):
        date = _datetime2string(date)

    return krx.get_index_portfolio_deposit_file(date, ticker)


def get_index_ohlcv_by_date(fromdate, todate, ticker, freq='d', name_display=False):
    """인덱스 OHLCV 조회
        :param fromdate: 조회 시작 일자 (YYYYMMDD)
        :param todate  : 조회 종료 일자 (YYYYMMDD)
        :param ticker  : 조회할 지표의 티커
        :param freq    : d - 일 / m - 월 / y - 년
        :param name_display : columns의 이름 출력 여부 (True/False)
        :return:
    """
    if isinstance(fromdate, datetime.datetime):
        fromdate = _datetime2string(fromdate, freq)

    if isinstance(todate, datetime.datetime):
        todate = _datetime2string(todate)

    df = krx.get_index_ohlcv_by_date(fromdate, todate, ticker)

    if name_display:
        df.columns.name = get_index_ticker_name(ticker)

    how = {'시가': 'first', '고가': 'max', '저가': 'min', '종가': 'last', '거래량': 'sum'}
    return resample_ohlcv(df, freq, how)


def get_index_listing_date(계열구분: str="KOSPI") -> DataFrame:
    """[11004] 전체지수 기본정보

    Args:
        계열구분 (str, optional): KRX/KOSPI/KOSDAQ/테마

    Returns:
        DataFrame:
                                   기준시점    발표시점   기준지수  종목수
            지수명
            코스피               1980.01.04  1983.01.04      100.0       1
            코스피 200           1990.01.03  1994.06.15      100.0      28
            코스피 100           2000.01.04  2000.03.02     1000.0      34
            코스피 50            2000.01.04  2000.03.02     1000.0      35
            코스피 200 중소형주  2010.01.04  2015.07.13     1000.0     167
    """
    defined_list = ["KRX", "KOSPI", "KOSDAQ", "테마"]
    if 계열구분 not in defined_list:
        print(f"{계열구분}이 올바르지 않습니다." )
        print(f" - 허용된 값: {' '.join(defined_list)}" )
        print(f"KOSPI로 변경 조회합니다." )
        계열구분 = "KOSPI"

    return krx.get_index_listing_date(계열구분)

@deprecated(version='1.0', reason="You should use get_index_price_change_by_ticker() instead")
def get_index_price_change_by_name(fromdate, todate, market="KOSPI"):
    return get_index_price_change_by_ticker(fromdate, todate, market)

def get_index_price_change_by_ticker(fromdate, todate, market="KOSPI"):
    if isinstance(fromdate, datetime.datetime):
        fromdate = _datetime2string(fromdate)
    if isinstance(todate, datetime.datetime):
        todate = _datetime2string(todate)
    return krx.get_index_price_change_by_ticker(fromdate, todate, market)


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
        date = get_nearest_business_day_in_a_week()
    return krx.get_etf_ticker_list(date)


def get_etf_isin(ticker):
    return krx.get_etf_isin(ticker)


def get_etf_ohlcv_by_date(fromdate, todate, ticker):
    return krx.get_etf_ohlcv_by_date(fromdate, todate, ticker)


def get_etf_portfolio_deposit_file(ticker, date=None):
    if date is None:
        date = get_nearest_business_day_in_a_week()
    return krx.get_etf_portfolio_deposit_file(date, ticker)


def get_etf_price_deviation(fromdate, todate, ticker):
    return krx.get_etf_price_deviation(fromdate, todate, ticker)


def get_etf_tracking_error(fromdate, todate, ticker):
    return krx.get_etf_tracking_error(fromdate, todate, ticker)


if __name__ == "__main__":
    pd.set_option('display.expand_frame_repr', False)
    # tickers = get_market_ticker_list()
    # print(tickers)
    # for ticker in tickers:
    #     name = get_market_ticker_name(ticker)
    #     print(ticker, name)
    # tickers = get_market_ticker_list("20190225")
    # print(tickers)
    # tickers = get_market_ticker_list()
    # tickers = get_market_ticker_list("20190225", "KOSDAQ")
    # tickers = get_market_ticker_list("20190225", "ALL")
    # print(tickers)
    # df = get_market_ticker_name("000660")
    # print(get_market_ohlcv_by_date("20190225", "20190228", "000660"))
    # df = get_market_ohlcv_by_date("20190225", "20190228", "000660", adjusted=False)
    # df = get_market_ohlcv_by_date("20040418", "20140418", "000020")
    print(get_market_ohlcv_by_ticker("20200831"))
    # print(get_market_ohlcv_by_ticker("20200831", "KOSPI"))

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
    # ticker_list = get_index_ticker_list()
    # for ticker in ticker_list:
    #     print(ticker, get_index_ticker_name(ticker))
    # print (ticker_list)
    # df = get_index_listing_date("KOSPI")
    # tickers = get_index_ticker_list()
    # tickers = get_index_ticker_list("20190225", "KOSDAQ")
    # print(tickers)
    # for ticker in get_index_ticker_list():
    #     print(ticker, get_index_name(ticker))
    # df = get_index_ohlcv_by_date("20190101", "20190228", "1009")
    # pdf = get_index_portfolio_deposit_file("1005")
    # print(len(pdf), pdf)
    # df = get_index_ohlcv_by_date("20190101", "20190228", "1001", "m")
    # df = get_index_price_change_by_name("20200520", "20200527", "KOSDAQ")
    # print(get_index_portfolio_deposit_file("20190412", "2001"))
    # df = krx.IndexTicker().get_id("코스피 200", "20000201")
    # df = get_index_portfolio_deposit_file("20200916", "1001")

    # df = get_shorting_status_by_date("20181210", "20181212", "005930")
    # df = get_shorting_investor_volume_by_date("20190401", "20190405", "KOSPI")
    # df = get_shorting_investor_price_by_date("20190401", "20190405", "KOSPI")
    # df = get_shorting_volume_by_ticker("20190211", "KOSPI")
    # df = get_shorting_volume_by_date("20200101", "20200115", "005930")

    # df = get_shorting_volume_top50("20190401", "KOSPI")
    # df = get_shorting_balance_by_date("20190401", "20190405", "005930")
    # df = get_shorting_balance_top50("20190401", "KOSDAQ")

    # print(get_etf_ticker_list())
    # print(get_etf_isin("346000"))
    # print(get_etf_ohlcv_by_date("20200101", "20200401", "295820"))
    # print(get_etf_portfolio_deposit_file("252650", "20190329"))
    # print(get_etf_price_deviation("20200101", "20200401", "295820"))
    # print(get_etf_tracking_error("20200101", "20200401", "295820"))
    # print(df)


