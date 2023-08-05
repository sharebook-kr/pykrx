from multipledispatch import dispatch
from typing import overload
from pykrx.website import krx, naver
import datetime
import inspect
import functools
import pandas as pd
from deprecated import deprecated
from pandas import DataFrame
import re

regex_yymmdd = re.compile(r"\d{4}[-/]?\d{2}[-/]?\d{2}")


def market_valid_check(param=None):
    def _market_valid_check(func):
        sig = inspect.signature(func)

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # default parameter
            if 'market' in sig.bind_partial(*args, **kwargs).arguments:
                if param is None:
                    valid_market_list = ["ALL", "KOSPI", "KOSDAQ", "KONEX"]
                else:
                    valid_market_list = param

                for v in [x for x in kwargs.values()] + list(args):
                    if v in valid_market_list:
                        return func(*args, **kwargs)
                print("market 옵션이 올바르지 않습니다.")
                return DataFrame()
            return func(*args, **kwargs)
        return wrapper
    return _market_valid_check


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


def get_nearest_business_day_in_a_week(date: str = None, prev: bool = True) \
        -> str:
    """인접한 영업일을 조회한다.

    Args:
        date (str , optional): 조회할 날짜, 입력하지 않으면 현재 시간으로 대체
        prev (bool, optional): 휴일일 경우 이전/이후 영업일 선택

    Returns:
        str: 날짜 (YYMMDD)
    """
    return krx.get_nearest_business_day_in_a_week(date, prev)


def get_market_ticker_list(date: str = None, market: str = "KOSPI") -> list:
    """티커 목록 조회

    Args:
        date   (str, optional): 조회 일자 (YYYYMMDD)
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


def __get_business_days_0(year: int, month: int):
    strt = f"{year}{month:02}01"
    if month == 12:
        last = f"{year+1}0101"
    else:
        last = f"{year}{month+1:02}01"
    df = krx.get_market_ohlcv_by_date(strt, last, "000020")
    cond = df.index.month[0] == df.index.month
    return df.index[cond].to_list()


def __get_business_days_1(strt: str, last: str):
    df = krx.get_market_ohlcv_by_date(strt, last, "000020")
    return df.index.to_list()


def get_previous_business_days(**kwargs) -> list:
    """과거의 영업일 조회

    Returns:
        list: 영업일을 pandas의 Timestamp로 저장해서 리스트로 반환

        >> get_previous_business_days(year=2020, month=10)
         -> 10월의 영업일을 조회

        >> get_previous_business_days(fromdate="20200101", todate="20200115")
         -> 주어진 기간 동안의 영업일을 조회

    """
    if "year" in kwargs and "month" in kwargs:
        return __get_business_days_0(kwargs['year'], kwargs['month'])

    elif 'fromdate' in kwargs and "todate" in kwargs:
        return __get_business_days_1(kwargs['fromdate'], kwargs['todate'])
    else:
        print("This option is not supported.")
        return []


@deprecated(version='1.1',
            reason="You should use get_previous_business_days() instead")
def get_business_days(year, month) -> list:
    return get_previous_business_days(year=year, month=month)


def get_market_ohlcv(*args, **kwargs):
    """OHLCV 조회
    Args:

        특정 종목의 지정된 기간 OHLCV 조회

        fromdate     (str           ): 조회 시작 일자 (YYYYMMDD)
        todate       (str           ): 조회 종료 일자 (YYYYMMDD)
        ticker       (str,  optional): 조회할 종목의 티커
        freq         (str,  optional): d - 일 / m - 월 / y - 년
        adjusted     (bool, optional): 수정 종가 여부 (True/False)

        특정 일자의 전종목 OHLCV 조회

        date   (str): 조회 일자 (YYYYMMDD)
        market (str): 조회 시장 (KOSPI/KOSDAQ/KONEX/ALL)

    Returns:
        DataFrame:

            특정 종목의 지정된 기간 OHLCV 조회
            >> get_market_ohlcv("20210118", "20210126", "005930")

                         시가   고가   저가   종가    거래량
            날짜
            2021-01-18  86600  87300  84100  85000  43227951
            2021-01-19  84500  88000  83600  87000  39895044
            2021-01-20  89000  89000  86500  87200  25211127
            2021-01-21  87500  88600  86500  88100  25318011
            2021-01-22  89000  89700  86800  86800  30861661

            특정 일자의 전종목 OHLCV 조회
            >> get_market_ohlcv("20210122")

                      시가    고가    저가    종가   거래량     거래대금     등락률
            티커
            095570    4190    4245    4160    4210   216835    910274405   0.839844
            006840   25750   29550   25600   29100   727088  20462325950  12.570312
            027410    5020    5250    4955    5220  1547629   7990770515   4.191406
            282330  156500  156500  151500  152000    62510   9555364000  -2.560547

    """  # pylint: disable=line-too-long # noqa: E501

    dates = list(filter(regex_yymmdd.match, [str(x) for x in args]))
    if len(dates) == 2 or ('fromdate' in kwargs and
                           'todate' in kwargs):
        return get_market_ohlcv_by_date(*args, **kwargs)
    else:
        return get_market_ohlcv_by_ticker(*args, **kwargs)


def get_market_ohlcv_by_date(
    fromdate: str, todate: str, ticker: str, freq: str = 'd',
        adjusted: bool = True, name_display: bool = False) -> DataFrame:
    """특정 종목의 일자별로 정렬된 OHLCV

    Args:
        fromdate     (str           ): 조회 시작 일자 (YYYYMMDD)
        todate       (str           ): 조회 종료 일자 (YYYYMMDD)
        ticker       (str           ): 조회할 종목의 티커
        freq         (str,  optional): d - 일 / m - 월 / y - 년
        adjusted     (bool, optional): 수정 종가 여부 (True/False)
        name_display (bool, optional): columns의 이름 출력 여부 (True/False)

    Returns:
        DataFrame:

            >> get_market_ohlcv_by_date("20210118", "20210126", "005930")

                         시가   고가   저가   종가    거래량
            날짜
            2021-01-18  86600  87300  84100  85000  43227951
            2021-01-19  84500  88000  83600  87000  39895044
            2021-01-20  89000  89000  86500  87200  25211127
            2021-01-21  87500  88600  86500  88100  25318011
            2021-01-22  89000  89700  86800  86800  30861661
    """

    if isinstance(fromdate, datetime.datetime):
        fromdate = krx.datetime2string(fromdate)

    if isinstance(todate, datetime.datetime):
        todate = krx.datetime2string(todate)

    fromdate = fromdate.replace("-", "")
    todate = todate.replace("-", "")

    if adjusted:
        df = naver.get_market_ohlcv_by_date(fromdate , todate, ticker)
    else:
        df = krx.get_market_ohlcv_by_date(fromdate, todate, ticker, False)

    if name_display:
        df.columns.name = get_market_ticker_name(ticker)

    how = {
        '시가': 'first',
        '고가': 'max',
        '저가': 'min',
        '종가': 'last',
        '거래량': 'sum'
    }

    return resample_ohlcv(df, freq, how)


@market_valid_check()
def get_market_ohlcv_by_ticker(
        date, market: str = "KOSPI", alternative: bool = False) -> DataFrame:
    """티커별로 정리된 전종목 OHLCV

    Args:
        date        (str           ): 조회 일자 (YYYYMMDD)
        market      (str           ): 조회 시장 (KOSPI/KOSDAQ/KONEX/ALL)
        alternative (bool, optional): 휴일일 경우 이전 영업일 선택 여부

    Returns:
        DataFrame:

            >> get_market_ohlcv_by_ticker("20210122")

                      시가    고가    저가    종가   거래량     거래대금     등락률
            티커
            095570    4190    4245    4160    4210   216835    910274405   0.839844
            006840   25750   29550   25600   29100   727088  20462325950  12.570312
            027410    5020    5250    4955    5220  1547629   7990770515   4.191406
            282330  156500  156500  151500  152000    62510   9555364000  -2.560547

            >> get_market_ohlcv_by_ticker("20210122", "KOSDAQ")

                      시가    고가    저가    종가   거래량     거래대금    등락률
            티커
            060310    2265    2290    2225    2255   275425    619653305 -0.219971
            054620    7210    7250    7030    7120   124636    883893780 -1.110352
            265520   25850   25850   25200   25400   196384   4994644750 -0.779785
            211270   10250   10950   10050   10350  1664154  17351956900  1.469727
            035760  165200  166900  162600  163800   179018  29574003100  0.429932

        NOTE: 거래정지 종목은 종가만 존재하며 나머지는 0으로 채워진다.
    """  # pylint: disable=line-too-long # noqa: E501

    if isinstance(date, datetime.datetime):
        date = krx.datetime2string(date)

    date = date.replace("-", "")

    df = krx.get_market_ohlcv_by_ticker(date, market)
    holiday = (df[['시가', '고가', '저가', '종가']] == 0).all(axis=None)
    if holiday and alternative:
        target_date = get_nearest_business_day_in_a_week(date=date, prev=True)
        df = krx.get_market_ohlcv_by_ticker(target_date, market)
    return df


def get_market_cap(*args, **kwargs):
    """시가총액 조회

    Args:
        특정 종목의 지정된 기간 시가총액 조회

        fromdate (str           ): 조회 시작 일자 (YYYYMMDD)
        todate   (str           ): 조회 종료 일자 (YYYYMMDD)
        ticker   (str           ): 티커
        freq     (str,  optional):  d - 일 / m - 월 / y - 년

        특정 일자의 전종목 시가총액 조회

        date      (str           ): 조회 일자 (YYYYMMDD)
        market    (str , optional): 조회 시장 (KOSPI/KOSDAQ/KONEX/ALL)
        ascending (bool, optional): 정렬 기준.
        prev      (bool, optional): 휴일일 경우 이전/이후 영업일 선택

    Returns:
        DataFrame:

            특정 종목의 지정된 기간 시가총액 조회
            >> get_market_cap("20150720", "20150724", "005930")

                               시가총액  거래량      거래대금 상장주식수
            날짜
            2015-07-20  187806654675000  128928  165366199000  147299337
            2015-07-21  186039062631000  194055  244129106000  147299337
            2015-07-22  184566069261000  268323  333813094000  147299337
            2015-07-23  181767381858000  208965  259446564000  147299337
            2015-07-24  181030885173000  196584  241383636000  147299337

            특정 일자의 전종목 시가총액 조회
            >> get_market_cap("20210104")

                      종가         시가총액    거래량       거래대금   상장주식수
            티커
            005930   83000  495491951650000  38655276  3185356823460  5969782550
            000660  126000   91728297990000   7995016   994276505704   728002365
            051910  889000   62756592927000    858451   747929748128    70592343
            005935   74400   61222770480000   5455139   405685236800   822886700
            207940  829000   54850785000000    182864   149889473000    66165000

    """  # pylint: disable=line-too-long # noqa: E501

    dates = list(filter(regex_yymmdd.match, [str(x) for x in args]))
    if len(dates) == 2 or ('fromdate' in kwargs and
                           'todate' in kwargs):
        return get_market_cap_by_date(*args, **kwargs)
    else:
        return get_market_cap_by_ticker(*args, **kwargs)


def get_market_cap_by_date(
        fromdate: str, todate: str, ticker: str, freq: str = 'd') -> DataFrame:
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
    """  # pylint: disable=line-too-long # noqa: E501

    if isinstance(fromdate, datetime.datetime):
        fromdate = krx.datetime2string(fromdate)

    if isinstance(todate, datetime.datetime):
        todate = krx.datetime2string(todate)

    fromdate = fromdate.replace("-", "")
    todate = todate.replace("-", "")

    df = krx.get_market_cap_by_date(fromdate, todate, ticker)

    how = {
        '시가총액': 'last',
        '거래량': 'sum',
        '거래대금': 'sum',
        '상장주식수': 'last'
    }
    return resample_ohlcv(df, freq, how)


@market_valid_check()
def get_market_cap_by_ticker(
        date, market: str = "ALL", acending: bool = False,
        alternative: bool = False) -> DataFrame:
    """티커별로 정렬된 시가총액

    Args:
        date        (str           ): 조회 일자 (YYYYMMDD)
        market      (str , optional): 조회 시장 (KOSPI/KOSDAQ/KONEX/ALL)
        ascending   (bool, optional): 정렬 기준.
        alternative (bool, optional): 휴일일 경우 이전 영업일 선택 여부

    Returns:
        DataFrame :

            >> get_market_cap_by_ticker("20210104")

                      종가         시가총액    거래량       거래대금   상장주식수
            티커
            005930   83000  495491951650000  38655276  3185356823460  5969782550
            000660  126000   91728297990000   7995016   994276505704   728002365
            051910  889000   62756592927000    858451   747929748128    70592343
            005935   74400   61222770480000   5455139   405685236800   822886700
            207940  829000   54850785000000    182864   149889473000    66165000
    """  # pylint: disable=line-too-long # noqa: E501

    if isinstance(date, datetime.datetime):
        date = krx.datetime2string(date)

    date = date.replace("-", "")

    df = krx.get_market_cap_by_ticker(date, market, acending)
    holiday = (df[['종가', '시가총액', '거래량', '거래대금']] == 0) \
        .all(axis=None)
    if holiday and alternative:
        target_date = get_nearest_business_day_in_a_week(date=date, prev=True)
        df = krx.get_market_cap_by_ticker(target_date, market, acending)
    return df


def get_exhaustion_rates_of_foreign_investment(*args, **kwargs):
    """외국인 한도소진률 조회

    Args:
        특정 종목의 지정된 기간 한도소진률 조회

        fromdate (str           ): 조회 시작 일자 (YYYYMMDD)
        todate   (str           ): 조회 종료 일자 (YYYYMMDD)
        ticker   (str           ): 티커
        freq     (str,  optional):  d - 일 / m - 월 / y - 년

        특정 일자의 전종목 시가총액 조회

        date          (str ): 조회 시작 일자 (YYYYMMDD)
        market        (str ): 조회 시장 (KOSPI/KOSDAQ/KONEX/ALL)
        balance_limit (bool): 외국인보유제한종목
                                - False : check X
                                - True  : check O

    Returns:
        DataFrame:

            특정 종목의 지정된 기간 한도소진률 조회
            >> get_exhaustion_rates_of_foreign_investment(
                "20210108", "20210114", "005930")

                        상장주식수    보유수량    지분율    한도수량 한도소진율
            날짜
            2021-01-08  5969782550  3314966371  55.53125  5969782550   55.53125
            2021-01-11  5969782550  3324115988  55.68750  5969782550   55.68750
            2021-01-12  5969782550  3318676206  55.59375  5969782550   55.59375
            2021-01-13  5969782550  3316551070  55.56250  5969782550   55.56250
            2021-01-14  5969782550  3314652740  55.53125  5969782550   55.53125

            특정 일자의 전종목 시가총액 조회
            >> get_exhaustion_rates_of_foreign_investment("20210108")

                    상장주식수   보유수량      지분율   한도수량   한도소진률
            티커
            003490   94844634   12350096  13.023438   47412833  26.046875
            003495    1110794      29061   2.619141     555286   5.230469
            015760  641964077  127919592  19.937500  256785631  49.812500
            017670   80745711   28962369  35.875000   39565398  73.187500
            020560  223235294   13871465   6.210938  111595323  12.429688
    """

    dates = list(filter(regex_yymmdd.match, [str(x) for x in args]))
    if len(dates) == 2 or ('fromdate' in kwargs and
                           'todate' in kwargs):
        return get_exhaustion_rates_of_foreign_investment_by_date(
            *args, **kwargs)
    else:
        return get_exhaustion_rates_of_foreign_investment_by_ticker(
            *args, **kwargs)


def get_exhaustion_rates_of_foreign_investment_by_date(
        fromdate: str, todate: str, ticker: str) -> DataFrame:
    """지정된 종목의 일자별로 정렬된 외국인 보유 수량 및 한도 수량

    Args:
        fromdate (str): 조회 시작 일자 (YYYYMMDD)
        todate   (str): 조회 종료 일자 (YYYYMMDD)
        ticker   (str): 종목의 티커

    Returns:
        DataFrame:
                        상장주식수    보유수량    지분율    한도수량 한도소진률
            날짜
            2021-01-08  5969782550  3314966371  55.53125  5969782550   55.53125
            2021-01-11  5969782550  3324115988  55.68750  5969782550   55.68750
            2021-01-12  5969782550  3318676206  55.59375  5969782550   55.59375
            2021-01-13  5969782550  3316551070  55.56250  5969782550   55.56250
            2021-01-14  5969782550  3314652740  55.53125  5969782550   55.53125
    """
    if isinstance(fromdate, datetime.datetime):
        fromdate = krx.datetime2string(fromdate)

    if isinstance(todate, datetime.datetime):
        todate = krx.datetime2string(todate)

    fromdate = fromdate.replace("-", "")
    todate = todate.replace("-", "")

    return krx.get_exhaustion_rates_of_foreign_investment_by_date(
        fromdate, todate, ticker)


@market_valid_check()
def get_exhaustion_rates_of_foreign_investment_by_ticker(
    date: str, market: str = "KOSPI", balance_limit: bool = False) \
        -> DataFrame:
    """특정 시장에서 티커로 정렬된 외국인 보유량 조회

    Args:
        date          (str ): 조회 시작 일자 (YYYYMMDD)
        market        (str ): 조회 시장 (KOSPI/KOSDAQ/KONEX/ALL)
        balance_limit (bool): 외국인보유제한종목
            - False : check X
            - True  : check O

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
        date = krx.datetime2string(date)

    date = date.replace("-", "")

    return krx.get_exhaustion_rates_of_foreign_investment_by_ticker(
        date, market, balance_limit)


def get_market_price_change(*args, **kwargs):
    """외국인 한도소진률 조회

    Args:
        특정 종목의 지정된 기간 한도소진률 조회

        fromdate (str           ): 조회 시작 일자 (YYYYMMDD)
        todate   (str           ): 조회 종료 일자 (YYYYMMDD)
        market   (str,  optional): 조회 시장 (KOSPI/KOSDAQ/KONEX/ALL)
        adjusted (bool, optional): 수정 종가 여부 (True/False)

    Returns:
        DataFrame:

            특정 기간동안의 전종목 등락률 조회
            >> get_market_price_change("20210101", "20210108")

                        종목명     시가    종가 변동폭  등락률    거래량       거래대금
            티커
            095570   AJ네트웍스    4615    4540   -75  -1.63   3004291   14398725745
            006840     AK홀딩스   25150   25350   200   0.80    474849   11957437450
            027410         BGF    4895    4905    10    0.20   2664868   13195079855
            282330    BGF리테일  135500  141000  5500   4.06    329888   44965345000
            138930  BNK금융지주    5680    5780   100   1.76  18363844  103453756550
    """  # pylint: disable=line-too-long # noqa: E501

    dates = list(filter(regex_yymmdd.match, [str(x) for x in args]))
    if len(dates) == 2 or ('fromdate' in kwargs and
                           'todate' in kwargs):
        return get_market_price_change_by_ticker(*args, **kwargs)
    else:
        raise NotImplementedError


def get_market_price_change_by_ticker(
        fromdate: str, todate: str, market: str = "KOSPI",
        adjusted: bool = True, delist: bool = False):
    if isinstance(fromdate, datetime.datetime):
        fromdate = krx.datetime2string(fromdate)

    if isinstance(todate, datetime.datetime):
        todate = krx.datetime2string(todate)

    fromdate = fromdate.replace("-", "")
    todate = todate.replace("-", "")

    # businessday check
    fromdate = get_nearest_business_day_in_a_week(fromdate, prev=False)
    todate = get_nearest_business_day_in_a_week(todate)

    df_0 = krx.get_market_price_change_by_ticker(fromdate, todate, market,
                                                 adjusted)
    if df_0.empty:
        return df_0

    # - 시작일에는 존재하지만 기간 동안 없는(상폐) 종목을 찾아낸다.
    # - 시작일 하루간의 가격 정보를 얻어온다.
    df_1 = krx.get_market_price_change_by_ticker(fromdate, fromdate, market,
                                                 adjusted)
    # - 종가/대비/등락률/거래량/거래대금을 0으로 업데이트한다.
    cond = ~df_1.index.isin(df_0.index)
    if len(df_1[cond]) >= 1:
        df_1.loc[cond, '종가'] = 0
        df_1.loc[cond, '변동폭'] = -df_1.loc[cond, '시가']
        df_1.loc[cond, '등락률'] = -100.0
        df_1.loc[cond, '거래량'] = 0
        df_1.loc[cond, '거래대금'] = 0
        # 조회 정보에 상장 폐지 정보를 추가한다.
        df_0 = pd.concat([df_0, df_1[cond]])


    # - 상장폐지 옵션을 부여하면 상장폐지된 종목들만 출력
    if delist:
        df_0 = df_0[(df_0['종가'] == 0) & (df_0['등락률'] == -100)]
        return df_0

    return df_0


def get_market_fundamental(*args, **kwargs):
    """종목의 PER/PBR/배당수익률 조회

    Args:
        특정 종목의 지정된 기간 PER/PBR/배당수익률 조회

        fromdate     (str           ): 조회 시작 일자 (YYYYMMDD)
        todate       (str           ): 조회 종료 일자 (YYYYMMDD)
        ticker       (str           ): 조회 종목 티커
        freq         (str , optional): d - 일 / m - 월 / y - 년
        name_display (bool, optional): 종목 이름 출력 여부 (True/False)

        특정 일자의 전종목 PER/PBR/배당수익률 조회

        date   (str           ): 조회 일자 (YYMMDD)
        market (str,  optional): 조회 시장 (KOSPI/KOSDAQ/KONEX/ALL)
        prev   (bool, optional): 휴일일 경우 이전/이후 영업일 선택

    Returns:
        DataFrame:

            특정 종목의 지정된 기간 PER/PBR/배당수익률 조회
            >> get_market_fundamental("20210104", "20210108", "005930")

                              BPS        PER       PBR   EPS       DIV   DPS
                날짜
                2021-01-04  37528  26.218750  2.210938  3166  1.709961  1416
                2021-01-05  37528  26.500000  2.240234  3166  1.690430  1416
                2021-01-06  37528  25.953125  2.189453  3166  1.719727  1416
                2021-01-07  37528  26.187500  2.210938  3166  1.709961  1416
                2021-01-08  37528  28.046875  2.369141  3166  1.589844  1416


            특정 일자의 전종목 PER/PBR/배당수익률 조회
            >> get_market_fundamental("20210104")

                           BPS        PER       PBR   EPS       DIV   DPS
                티커
                095570    6802   4.660156  0.669922   982  6.550781   300
                006840   62448  11.648438  0.399902  2168  2.970703   750
                027410   15699  17.765625  0.320068   281  2.199219   110
                282330   36022  15.062500  3.660156  8763  2.050781  2700
                138930   25415   3.380859  0.219971  1647  6.468750   360
    """
    dates = list(filter(regex_yymmdd.match, [str(x) for x in args]))
    if len(dates) == 2 or ('fromdate' in kwargs and
                           'todate' in kwargs):
        return get_market_fundamental_by_date(*args, **kwargs)
    else:
        return get_market_fundamental_by_ticker(*args, **kwargs)


def get_market_fundamental_by_date(
    fromdate: str, todate: str, ticker: str, freq: str = 'd',
        name_display: bool = False) -> DataFrame:
    """기간별 특정 종목의 PER/PBR/배당수익률 조회

    Args:
        fromdate     (str           ): 조회 시작 일자 (YYYYMMDD)
        todate       (str           ): 조회 종료 일자 (YYYYMMDD)
        ticker       (str           ): 조회 종목 티커
        freq         (str , optional): d - 일 / m - 월 / y - 년
        name_display (bool, optional): 종목 이름 출력 여부 (True/False)

    Returns:
        DataFrame:

            >> get_market_fundamental_by_date("20210104", "20210108", "005930")

                              BPS        PER       PBR   EPS       DIV   DPS
                날짜
                2021-01-04  37528  26.218750  2.210938  3166  1.709961  1416
                2021-01-05  37528  26.500000  2.240234  3166  1.690430  1416
                2021-01-06  37528  25.953125  2.189453  3166  1.719727  1416
                2021-01-07  37528  26.187500  2.210938  3166  1.709961  1416
                2021-01-08  37528  28.046875  2.369141  3166  1.589844  1416

            >> get_market_fundamental_by_date(
                "20200101", "20200430", "005930", freq="m")

                              BPS       PER       PBR   EPS       DIV   DPS
                날짜
                2020-01-31  35342  8.539062  1.559570  6461  2.570312  1416
                2020-02-29  35342  8.851562  1.620117  6461  2.480469  1416
                2020-03-31  35342  8.507812  1.559570  6461  2.570312  1416
                2020-04-30  35342  7.089844  1.299805  6461  3.089844  1416
    """

    if isinstance(fromdate, datetime.datetime):
        fromdate = krx.datetime2string(fromdate)

    if isinstance(todate, datetime.datetime):
        todate = krx.datetime2string(todate)

    fromdate = fromdate.replace("-", "")
    todate = todate.replace("-", "")

    df = krx.get_market_fundamental_by_date(fromdate, todate, ticker)

    if df.empty:
        return df

    if name_display:
        df.columns.name = get_market_ticker_name(ticker)

    how = {
        'BPS': 'first',
        'PER': 'first',
        'PBR': 'first',
        'EPS': 'first',
        'DIV': 'first',
        'DPS': 'first'
    }
    return resample_ohlcv(df, freq, how)


def get_market_fundamental_by_ticker(
        date: str, market: str = "KOSPI", alternative: bool = False) \
            -> DataFrame:
    """특정 일자의 전종목 PER/PBR/배당수익률 조회

    Args:
        date        (str           ): 조회 일자 (YYMMDD)
        market      (str,  optional): 조회 시장 (KOSPI/KOSDAQ/KONEX/ALL)
        alternative (bool, optional): 휴일일 경우 이전 영업일 선택 여부

    Returns:
        DataFrame:

            >> get_market_fundamental_by_ticker("20210104")

                           BPS        PER       PBR   EPS       DIV   DPS
                티커
                095570    6802   4.660156  0.669922   982  6.550781   300
                006840   62448  11.648438  0.399902  2168  2.970703   750
                027410   15699  17.765625  0.320068   281  2.199219   110
                282330   36022  15.062500  3.660156  8763  2.050781  2700
                138930   25415   3.380859  0.219971  1647  6.468750   360
    """
    if isinstance(date, datetime.datetime):
        date = krx.datetime2string(date)

    date = date.replace("-", "")

    df = krx.get_market_fundamental_by_ticker(date, market)
    holiday = (df[['BPS', 'PER', 'PBR', 'EPS', 'DIV', 'DPS']] == 0).all(
        axis=None)
    if holiday and alternative:
        target_date = get_nearest_business_day_in_a_week(date=date, prev=True)
        df = krx.get_market_fundamental_by_ticker(target_date, market)
    return df


def __get_market_trading_value_and_volume_by_investor(
    fromdate: str, todate: str, ticker: str, etf: bool, etn: bool,
        elw: bool, key: str) -> DataFrame:
    """투자자별 거래실적 기간합계

    Args:
        fromdate (str ): 조회 시작 일자 (YYMMDD)
        todate   (str ): 조회 종료 일자 (YYMMDD)
        ticker   (str ): 조회 종목 티커
          - KOSPI/KOSDAQ/KONEX/ALL을 입력할 경우 전체 시장을 조회
        etf      (bool): 시장 포함 여부 - KOSPI/KOSDAQ/KONEX/ALL 시장일
                                          경우에만 유효
        etn      (bool): 시장 포함 여부 - KOSPI/KOSDAQ/KONEX/ALL 시장일
                                          경우에만 유효
        elw      (bool): 시장 포함 여부 - KOSPI/KOSDAQ/KONEX/ALL 시장일
                                          경우에만 유효
        key      (str ): column 인덱스 : 거래량 / 거래대금

    Returns:
        DataFrame:

            >> __get_market_trading_value_and_volume_by_investor("20210115", "20210122", "005930")

                         거래량                             거래대금
                           매도       매수    순매수            매도            매수         순매수
            투자자구분
            금융투자    29455909   26450600  -3005309   2580964135000   2309054317700  -271909817300
            보험         1757287     509535  -1247752    153322228800     44505136200  -108817092600
            투신         2950680    1721970  -1228710    258073006600    150715203700  -107357802900
            사모          745727     696135    -49592     65167773900     60862926800    -4304847100
            은행           38675      46394      7719      3369626100      4004806100      635180000

            >> __get_market_trading_value_and_volume_by_investor("20210115", "20210122", "KOSPI")

                            거래량                                 거래대금
                              매도         매수     순매수             매도             매수         순매수
            투자자구분
            금융투자    1857447354   1660620713 -196826641   15985568261831   15006116511544  -979451750287
            보험          29594468     19872165   -9722303    1219035502445     757575677208  -461459825237
            투신          69348190     60601427   -8746763    2235561259511    1799363743367  -436197516144
            사모          31673292     26585281   -5088011     999084910863     846067212945  -153017697918
            은행          44279242     51690814    7411572     886226324790     936210985810    49984661020

            >> __get_market_trading_value_and_volume_by_investor("20210115", "20210122", "KOSPI", False, False)

                            거래량                                 거래대금
                              매도         매수     순매수             매도             매수         순매수
            투자자구분
            금융투자    1241225479   1156823717  -84401762    9897795863804    9355167899112  -542627964692
            보험          15737709      8577242   -7160467     912820396542     560818697065  -352001699477
            투신          46872846     34307243  -12565603    1790231574897    1421181450288  -369050124609
            사모          20780475     16342937   -4437538     830445404788     665802837480  -164642567308
            은행           2236667       632814   -1603853      58624439870      37109603010   -21514836860
    """  # pylint: disable=line-too-long # noqa: E501

    if isinstance(fromdate, datetime.datetime):
        fromdate = krx.datetime2string(fromdate)
    if isinstance(todate, datetime.datetime):
        todate = krx.datetime2string(todate)

    fromdate = fromdate.replace("-", "")
    todate = todate.replace("-", "")

    if ticker in ["KOSPI", "KOSDAQ", "KONEX", "ALL"]:
        df = krx.get_market_trading_value_and_volume_on_market_by_investor(
            fromdate, todate, ticker, etf, etn, elw)
    else:
        df = krx.get_market_trading_value_and_volume_on_ticker_by_investor(
            fromdate, todate, ticker)
    return df[key]


def get_market_trading_value_by_investor(
    fromdate: str, todate: str, ticker: str, etf: bool = False,
        etn: bool = False, elw: bool = False) -> DataFrame:
    """투자자별 거래실적 기간합계

    Args:
        fromdate (str           ): 조회 시작 일자 (YYMMDD)
        todate   (str           ): 조회 종료 일자 (YYMMDD)
        ticker   (str           ): 조회 종목 티커
          - KOSPI/KOSDAQ/KONEX/ALL을 입력할 경우 전체 시장을 조회
        etf      (bool          ): 시장 포함 여부 - KOSPI/KOSDAQ/KONEX/ALL
                                   시장일 경우에만 유효
        etn      (bool          ): 시장 포함 여부 - KOSPI/KOSDAQ/KONEX/ALL
                                   시장일 경우에만 유효
        elw      (bool          ): 시장 포함 여부 - KOSPI/KOSDAQ/KONEX/ALL
                                   시장일 경우에만 유효
        detail   (bool, optional): 상세조회 여부

    Returns:
        DataFrame:

            >> get_market_trading_value_by_investor(
                "20210115", "20210122", "005930")

                                 매도            매수         순매수
            투자자구분
            금융투자    2580964135000   2309054317700  -271909817300
            보험         153322228800     44505136200  -108817092600
            투신         258073006600    150715203700  -107357802900
            사모          65167773900     60862926800    -4304847100
            은행           3369626100      4004806100      635180000

            >> get_market_trading_value_by_investor(
                "20210115", "20210122", "KOSPI")

                                  매도             매수         순매수
            투자자구분
            금융투자     9827334289654    9294592831462  -532741458192
            보험          912820396542     560818697065  -352001699477
            투신         1790231574897    1421181450288  -369050124609
            사모          830445404788     665802837480  -164642567308
            은행           58624439870      37109603010   -21514836860

            >> get_market_trading_value_by_investor(
                "20210115", "20210122", "KOSPI", etf=True, etn=True, elw=True)

                                  매도             매수         순매수
            투자자구분
            금융투자    15985568261831   15006116511544  -979451750287
            보험         1219035502445     757575677208  -461459825237
            투신         2235561259511    1799363743367  -436197516144
            사모          999084910863     846067212945  -153017697918
            은행          886226324790     936210985810    49984661020
    """

    if isinstance(fromdate, datetime.datetime):
        fromdate = krx.datetime2string(fromdate)
    if isinstance(todate, datetime.datetime):
        todate = krx.datetime2string(todate)

    fromdate = fromdate.replace("-", "")
    todate = todate.replace("-", "")

    return __get_market_trading_value_and_volume_by_investor(
        fromdate, todate, ticker, etf, etn, elw, '거래대금')


def get_market_trading_volume_by_investor(
    fromdate: str, todate: str, ticker: str, etf: bool = False,
        etn: bool = False, elw: bool = False) -> DataFrame:
    """투자자별 거래실적 기간합계

    Args:
        fromdate (str           ): 조회 시작 일자 (YYMMDD)
        todate   (str           ): 조회 종료 일자 (YYMMDD)
        ticker   (str           ): 조회 종목 티커
          - KOSPI/KOSDAQ/KONEX/ALL을 입력할 경우 전체 시장을 조회
        etf      (bool          ): 시장 포함 여부 - KOSPI/KOSDAQ/KONEX/ALL
                                   시장일 경우에만 유효
        etn      (bool          ): 시장 포함 여부 - KOSPI/KOSDAQ/KONEX/ALL
                                   시장일 경우에만 유효
        elw      (bool          ): 시장 포함 여부 - KOSPI/KOSDAQ/KONEX/ALL
                                   시장일 경우에만 유효

    Returns:
        DataFrame:

            >> get_market_trading_volume_by_investor(
                "20210115", "20210122", "005930")

                            매도       매수    순매수
            투자자구분
            금융투자    29455909   26450600  -3005309
            보험         1757287     509535  -1247752
            투신         2950680    1721970  -1228710
            사모          745727     696135    -49592
            은행           38675      46394      7719

            >> get_market_trading_volume_by_investor(
                "20210115", "20210122", "KOSPI")

                             매도        매수    순매수
            투자자구분
            금융투자    137969209   127697577 -10271632
            보험         15737709     8577242  -7160467
            투신         46872846    34307243 -12565603
            사모         20780475    16342937  -4437538
            은행          2236667      632814  -1603853

            >> get_market_trading_volume_by_investor(
                "20210115", "20210122", "KOSPI", etf=True, etn=True, elw=True)

                              매도         매수     순매수
            투자자구분
            금융투자    1857447354   1660620713 -196826641
            보험          29594468     19872165   -9722303
            투신          69348190     60601427   -8746763
            사모          31673292     26585281   -5088011
            은행          44279242     51690814    7411572
    """

    if isinstance(fromdate, datetime.datetime):
        fromdate = krx.datetime2string(fromdate)
    if isinstance(todate, datetime.datetime):
        todate = krx.datetime2string(todate)

    fromdate = fromdate.replace("-", "")
    todate = todate.replace("-", "")

    return __get_market_trading_value_and_volume_by_investor(
        fromdate, todate, ticker, etf, etn, elw, '거래량')


def get_market_trading_value_by_date(
    fromdate: str, todate: str, ticker: str, etf: bool = False,
    etn: bool = False, elw: bool = False, on: str = "순매수",
        detail: bool = False, freq: str = 'd') -> DataFrame:
    """투자자별 거래실적 일별추이

    Args:
        fromdate (str           ): 조회 시작 일자 (YYMMDD)
        todate   (str           ): 조회 종료 일자 (YYMMDD)
        ticker   (str           ): 조회 종목 티커
          - KOSPI/KOSDAQ/KONEX/ALL을 입력할 경우 전체 시장을 조회
        on       (str           ): 일별 추이 옵션 (매수/매도/순매수)
        etf      (bool          ): 시장 포함 여부 - KOSPI/KOSDAQ/KONEX/ALL
                                   시장일 경우에만 유효
        etn      (bool          ): 시장 포함 여부 - KOSPI/KOSDAQ/KONEX/ALL
                                   시장일 경우에만 유효
        elw      (bool          ): 시장 포함 여부 - KOSPI/KOSDAQ/KONEX/ALL
                                   시장일 경우에만 유효
        detail   (bool, optional): 상세조회 여부
        freq     (str,  optional):  d - 일 / m - 월 / y - 년

    Returns:
           DataFrame:

            >> get_market_trading_value_by_date("20210115", "20210122", "005930")

                            기관합계     기타법인          개인    외국인합계  전체
            날짜
            2021-01-15 -440769209300  25442287800  661609085600 -246282164100     0
            2021-01-18   42323535000  22682344800   14829121700  -79835001500     0
            2021-01-19   95523053500  -3250422500 -173484213300   81211582300     0
            2021-01-20 -364476214000  22980632900  430115581000  -88619999900     0
            2021-01-21  -60637506300 -27880854000  250285510000 -161767149700     0

           >> get_market_trading_value_by_date("20210115", "20210122", "KOSPI")

                             기관합계     기타법인           개인    외국인합계  전체
            날짜
            2021-01-15 -1414745885546  54444293672  2113924037705 -753622445831     0
            2021-01-18  -278880716957 -26004926379   514299140387 -209413497051     0
            2021-01-19   593956459208  21472281148 -1025418915468  409990175112     0
            2021-01-20 -1234485992694  34510184945  1436793223994 -236817416245     0
            2021-01-21  -292666343147 -13168420832   -62476631241  368311395220     0
            2021-01-22 -1364772011847  24513231108  1608263875827 -268005095088     0

        >> get_market_trading_value_by_date("20210115", "20210122", "KOSPI", etf=True, etn=True, elw=True)

                             기관합계     기타법인           개인    외국인합계  전체
            날짜
            2021-01-15 -1536570309441  63110174617  2251672617980 -778212483156     0
            2021-01-18  -601428111357 -27000808439   494341183227  134087736569     0
            2021-01-19   544654406338  21787409868  -968965427363  402523611157     0
            2021-01-20 -1227642472619  32139813590  1444113501769 -248610842740     0
            2021-01-21  -284899892322 -19072459127   -61503500921  365475852370     0
    """  # pylint: disable=line-too-long # noqa: E501

    if isinstance(fromdate, datetime.datetime):
        fromdate = krx.datetime2string(fromdate)
    if isinstance(todate, datetime.datetime):
        todate = krx.datetime2string(todate)

    fromdate = fromdate.replace("-", "")
    todate = todate.replace("-", "")

    if ticker in ["KOSPI", "KOSDAQ", "KONEX", "ALL"]:
        df = krx.get_market_trading_value_and_volume_on_market_by_date(
            fromdate, todate, ticker, etf, etn, elw, "거래대금", on, detail)
    else:
        df = krx.get_market_trading_value_and_volume_on_ticker_by_date(
            fromdate, todate, ticker, "거래대금", on, detail)
    return resample_ohlcv(df, freq, sum)


def get_market_trading_volume_by_date(
    fromdate: str, todate: str, ticker: str, etf: bool = False,
    etn: bool = False, elw: bool = False, on: str = "순매수",
        detail: bool = False, freq: str = 'd') -> DataFrame:
    """투자자별 거래실적 일별추이

    Args:
        fromdate (str           ): 조회 시작 일자 (YYMMDD)
        todate   (str           ): 조회 종료 일자 (YYMMDD)
        ticker   (str           ): 조회 종목 티커
          - KOSPI/KOSDAQ/KONEX/ALL을 입력할 경우 전체 시장을 조회
        on       (str           ): 일별 추이 옵션 2 (매수/매도/순매수)
        etf      (bool          ): 시장 포함 여부 - KOSPI/KOSDAQ/KONEX/ALL
                                   시장일 경우에만 유효
        etn      (bool          ): 시장 포함 여부 - KOSPI/KOSDAQ/KONEX/ALL
                                   시장일 경우에만 유효
        elw      (bool          ): 시장 포함 여부 - KOSPI/KOSDAQ/KONEX/ALL
                                   시장일 경우에만 유효
        detail   (bool, optional): 상세조회 여부
        freq     (str,  optional):  d - 일 / m - 월 / y - 년

    Returns:
           DataFrame:

            >> get_market_trading_volume_by_date(
                "20210115", "20210122", "005930")

                       기관합계  기타법인     개인   외국인합계  전체
            날짜
            2021-01-15 -5006115    288832  7485785     -2768502     0
            2021-01-18   505669    262604   151228      -919501     0
            2021-01-19  1139258    -34023 -2044543       939308     0
            2021-01-20 -4157919    262408  4917655     -1022144     0
            2021-01-21  -712099   -321732  2890389     -1856558     0
            2021-01-22 -6384793     56478  9884815     -3556500     0

           >> get_market_trading_volume_by_date(
               "20210115", "20210122", "KOSPI")

                        기관합계   기타법인      개인  외국인합계  전체
            날짜
            2021-01-15 -20393142    8435634  29119751   -17162243     0
            2021-01-18  -5700054   -1198498  15316328    -8417776     0
            2021-01-19   7216278    -246496 -24395243    17425461     0
            2021-01-20 -23038683    -793906  31606917    -7774328     0
            2021-01-21 -18443990   -7082091   8365421    17160660     0
            2021-01-22 -20475792    -775181  34931834   -13680861     0
    """
    if isinstance(fromdate, datetime.datetime):
        fromdate = krx.datetime2string(fromdate)
    if isinstance(todate, datetime.datetime):
        todate = krx.datetime2string(todate)

    fromdate = fromdate.replace("-", "")
    todate = todate.replace("-", "")

    if ticker in ["KOSPI", "KOSDAQ", "KONEX", "ALL"]:
        df = krx.get_market_trading_value_and_volume_on_market_by_date(
            fromdate, todate, ticker, etf, etn, elw, "거래량", on, detail)
    else:
        df = krx.get_market_trading_value_and_volume_on_ticker_by_date(
            fromdate, todate, ticker, "거래량", on, detail)
    return resample_ohlcv(df, freq, sum)


def get_market_net_purchases_of_equities(
    fromdate: str, todate: str, market: str = "KOSPI",
        investor: str = "개인") -> DataFrame:
    """입력된 투자자에 대한 티커별로 나열된 순매수 상위종목
    Args:
            fromdate (str): 조회 시작 일자 (YYMMDD)
            todate   (str): 조회 종료 일자 (YYMMDD)
            market   (str): 조회 시장 (KOSPI/KOSDAQ/KONEX/ALL)
            investor (str): 투자자
             - 금융투자 / 보험 / 투신 / 사모 / 은행 / 기타금융 / 연기금 / 기관합계 / 기타법인 / 개인 / 외국인 / 기타외국인 / 전체

            Note : inverstor를 전체로 설정하면 순매수 금액이 0으로 나옵니다.

    Returns:
           DataFrame:

           >> get_market_net_purchases_of_equities_by_ticker("20210115", "20210122", "KOSPI", "개인")

                         종목명   매도거래량 매수거래량  순매수거래량   매도거래대금   매수거래대금 순매수거래대금
            티커
            005930     삼성전자     79567418  102852747      23285329  6918846810800  8972911580500  2054064769700
            000270       기아차     44440252   49880626       5440374  3861283906400  4377698855000   516414948600
            005935   삼성전자우     15849762   20011325       4161563  1207133611400  1528694164400   321560553000
            051910       LG화학       709872     921975        212103   700823533000   908593419000   207769886000
            096770 SK이노베이션      4848359    5515777        667418  1298854139000  1478890602000   180036463000
    """  # pylint: disable=line-too-long # noqa: E501

    return get_market_net_purchases_of_equities_by_ticker(
        fromdate, todate, market, investor)


def get_market_net_purchases_of_equities_by_ticker(
    fromdate: str, todate: str, market: str = "KOSPI",
        investor: str = "개인") -> DataFrame:
    """입력된 투자자에 대한 티커별로 나열된 순매수 상위종목

    Args:
            fromdate (str): 조회 시작 일자 (YYMMDD)
            todate   (str): 조회 종료 일자 (YYMMDD)
            market   (str): 조회 시장 (KOSPI/KOSDAQ/KONEX/ALL)
            investor (str): 투자자
             - 금융투자 / 보험 / 투신 / 사모 / 은행 / 기타금융 / 연기금 / 기관합계 / 기타법인 / 개인 / 외국인 / 기타외국인 / 전체

            Note : inverstor를 전체로 설정하면 순매수 금액이 0으로 나옵니다.

    Returns:
           DataFrame:

           >> get_market_net_purchases_of_equities_by_ticker("20210115", "20210122", "KOSPI", "개인")

                         종목명   매도거래량 매수거래량  순매수거래량   매도거래대금   매수거래대금 순매수거래대금
            티커
            005930     삼성전자     79567418  102852747      23285329  6918846810800  8972911580500  2054064769700
            000270       기아차     44440252   49880626       5440374  3861283906400  4377698855000   516414948600
            005935   삼성전자우     15849762   20011325       4161563  1207133611400  1528694164400   321560553000
            051910       LG화학       709872     921975        212103   700823533000   908593419000   207769886000
            096770 SK이노베이션      4848359    5515777        667418  1298854139000  1478890602000   180036463000
    """  # pylint: disable=line-too-long # noqa: E501

    if isinstance(fromdate, datetime.datetime):
        fromdate = krx.datetime2string(fromdate)

    if isinstance(todate, datetime.datetime):
        todate = krx.datetime2string(todate)

    fromdate = fromdate.replace("-", "")
    todate = todate.replace("-", "")

    return krx.get_market_net_purchases_of_equities_by_ticker(
        fromdate, todate, market, investor)


@deprecated(version='1.1',
            reason="You should use get_market_net_purchases_of_equities"
                    "_by_ticker() instead")
def get_market_trading_value_and_volume_by_ticker(
    fromdate: str, todate: str, market: str = "KOSPI",
        investor: str = "개인") -> DataFrame:
    return get_market_net_purchases_of_equities_by_ticker(
        fromdate, todate, market, investor)


@market_valid_check(["KOSPI", "KOSDAQ", "KRX", "테마"])
def get_index_ticker_list(date: str = None, market: str = "KOSPI") -> list:
    """모든 지수 (index)의 티커 조회

    Args:
        date   (str, optional): 조회 일자 (YYMMDD)
        market (str, optional): 조회 시장 (KOSPI/KOSDAQ/KRX/테마)

    Returns:
        list:
            ['1001', '1002', '1003', ..., '1227', '1232', '1244']

        for ticker in stock.get_index_ticker_list():
            print(ticker, stock.get_index_ticker_name(ticker))
    """
    if date is None:
        date = datetime.datetime.now()
    if isinstance(date, datetime.datetime):
        date = krx.datetime2string(date)

    date = date.replace("-", "")

    return krx.IndexTicker().get_ticker(market, date)


def get_index_ticker_name(ticker: str) -> str:
    """티커의 이름 조회

    Args:
        ticker (str): 조회 인덱스 티커

    Returns:
        str: 종목이름
    """
    return krx.IndexTicker().get_name(ticker)


def get_index_portfolio_deposit_file(
        ticker: str, date: str = None, alternative: bool = False) -> list:
    """지수 구성 종목 조회

    Args:
        ticker      (str          ): 조회 인덱스
        date        (str, optional): 조회 일자 (YYMMDD)
        alternative (bool, optional): 휴일일 경우 이전 영업일 선택 여부

    NOTE: 2014년 5월 2일 까지만 조회 가능

    Returns:
        list: 구성 종목의 티커 리스트
    """

    if date is None:
        target_date = get_nearest_business_day_in_a_week()
    else:
        target_date = date

    if target_date <= "20140501":
        print("KRX web server does NOT provide data prior to 2014/05/01.")
        return []

    if isinstance(target_date, datetime.datetime):
        target_date = krx.datetime2string(target_date)

    target_date = target_date.replace("-", "")

    pdf = krx.get_index_portfolio_deposit_file(target_date, ticker)
    # 주말 or 비영업일 여부를 확인하는 것 자체가 상대적으로 오랜 시간 걸려 lazy 판단
    # - 결과가 없으면 과거의 가장 가까운 영업일로 조회
    if len(pdf) == 0 and alternative:
        target_date = get_nearest_business_day_in_a_week(target_date)
        pdf = krx.get_index_portfolio_deposit_file(target_date, ticker)
        if len(pdf) != 0 and date is not None:
            print(f"The date you entered {date} seems to be a holiday. PYKRX "
                  f"changes the date parameter to {target_date}.")
    return pdf


def get_index_ohlcv(*args, **kwargs):
    """특정 종목의 지정된 기간 OHLCV 조회
    Args:
        fromdate     (str           ): 조회 시작 일자 (YYMMDD)
        todate       (str           ): 조회 종료 일자 (YYMMDD)
        ticker       (str           ): 조회 인덱스 티커
        freq         (str, optional ): d - 일 / m - 월 / y - 년
        name_display (bool, optional): 인덱스 이름 출력 유무

        특정 일자의 전종목 OHLCV 조회

        date   (str): 조회 일자 (YYYYMMDD)
        market (str): 조회 시장 (KOSPI/KOSDAQ/KRX/테마/ALL)

    Returns:
        DataFrame:

            특정 종목의 지정된 기간 OHLCV 조회

            >> get_index_ohlcv("20210101", "20210130", "1001")

                           시가     고가     저가     종가      거래량        거래대금
            날짜
            2021-01-04  2874.50  2946.54  2869.11  2944.45  1026510465  25011393960858
            2021-01-05  2943.67  2990.57  2921.84  2990.57  1519911750  26548380179493
            2021-01-06  2993.34  3027.16  2961.37  2968.21  1793418534  29909396443430
            2021-01-07  2980.75  3055.28  2980.75  3031.68  1524654500  27182807334912
            2021-01-08  3040.11  3161.11  3040.11  3152.18  1297903388  40909490005818

            특정 일자의 전종목 OHLCV 조회

            >> get_index_ohlcv("20210122")

                                    시가      고가      저가      종가      거래량        거래대금
            지수명
            코스피외국주포함        0.00      0.00      0.00      0.00  1111222984  24305355507985
            코스피               3163.83   3185.26   3140.60   3140.63  1110004515  24300291238350
            코스피200             430.73    433.84    427.13    427.13   269257473  19087641760593
            코스피100            3295.34   3318.12   3266.10   3266.10   164218193  15401724965613
            코스피50             3034.99   3055.71   3008.02   3008.02   110775949  13083864634083
    """  # pylint: disable=line-too-long # noqa: E501

    dates = list(filter(regex_yymmdd.match, [str(x) for x in args]))
    if len(dates) == 2 or ('fromdate' in kwargs and
                           'todate' in kwargs):
        return get_index_ohlcv_by_date(*args, **kwargs)
    else:
        return get_index_ohlcv_by_ticker(*args, **kwargs)


def get_index_ohlcv_by_ticker(
        date, market: str = "KOSPI", alternative: bool = False):
    """티커별로 정리된 전종목 OHLCV

    Args:
        date   (str): 조회 일자 (YYYYMMDD)
        market (str): 조회 시장 (KOSPI/KOSDAQ/KRX/테마/ALL)
        alternative (bool, optional): 휴일일 경우 이전 영업일 선택 여부

    Returns:
        DataFrame:

            >> get_index_ohlcv_by_ticker("20210122")

                                    시가      고가       저가     종가      거래량        거래대금
            지수명
            코스피외국주포함        0.00      0.00      0.00      0.00  1111222984  24305355507985
            코스피               3163.83   3185.26   3140.60   3140.63  1110004515  24300291238350
            코스피200             430.73    433.84    427.13    427.13   269257473  19087641760593
            코스피100            3295.34   3318.12   3266.10   3266.10   164218193  15401724965613
            코스피50             3034.99   3055.71   3008.02   3008.02   110775949  13083864634083

            >> get_index_ohlcv_by_ticker("20210122", "KOSDAQ")

                                    시가      고가      저가       종가      거래량        거래대금
            지수명
            코스닥외국주포함         0.00      0.00      0.00      0.00  2288183346  15030875451228
            코스닥지수             982.20    984.67    975.05    979.98  2228382472  14929462086998
            코스닥150             1495.47   1501.68   1482.26   1492.01   104675565   3789026943821
            코스닥150정보기술      868.99    873.55    852.24    852.71    37578587    998283997365
            코스닥150헬스케어     4928.03   4974.17   4855.87   4949.63    21481119   1364482054586

        NOTE: 거래정지 종목은 종가만 존재하며 나머지는 0으로 채워진다.
    """  # pylint: disable=line-too-long # noqa: E501

    if isinstance(date, datetime.datetime):
        date = krx.datetime2string(date)

    date = date.replace("-", "")

    df = krx.get_index_ohlcv_by_ticker(date, market)
    holiday = (df[['시가', '고가', '저가', '종가']] == 0).all(axis=None)
    if holiday and alternative:
        target_date = get_nearest_business_day_in_a_week(date=date, prev=True)
        df = krx.get_index_ohlcv_by_ticker(target_date, market)
    return df


def get_index_ohlcv_by_date(
    fromdate: str, todate: str, ticker: str,
        freq: str = 'd', name_display: bool = True) -> DataFrame:
    """일자별로 정렬된 인덱스 OHLCV 조회

    Args:
        fromdate     (str           ): 조회 시작 일자 (YYMMDD)
        todate       (str           ): 조회 종료 일자 (YYMMDD)
        ticker       (str           ): 조회 인덱스 티커
        freq         (str, optional ): d - 일 / m - 월 / y - 년
        name_display (bool, optional): 인덱스 이름 출력 유무

    Returns:
        DataFrame:

        >> df = get_index_ohlcv_by_date("20210101", "20210130", "1001")

                       시가     고가     저가     종가      거래량         거래대금
        날짜
        2021-01-04  2874.50  2946.54  2869.11  2944.45  1026510465  25011393960858
        2021-01-05  2943.67  2990.57  2921.84  2990.57  1519911750  26548380179493
        2021-01-06  2993.34  3027.16  2961.37  2968.21  1793418534  29909396443430
        2021-01-07  2980.75  3055.28  2980.75  3031.68  1524654500  27182807334912
        2021-01-08  3040.11  3161.11  3040.11  3152.18  1297903388  40909490005818

        >> get_index_ohlcv_by_date("20200101", "20200531", "1001", freq="m")

                       시가     고가     저가     종가       거래량
        날짜
        2020-01-31  2201.21  2277.23  2119.01  2119.01  13096066333
        2020-02-29  2086.61  2255.49  1980.82  1987.01  13959766513
        2020-03-31  1997.03  2089.08  1439.43  1754.64  17091025314
        2020-04-30  1737.28  1957.51  1664.13  1947.56  21045120912
        2020-05-31  1906.42  2054.52  1894.29  2029.60  16206496902
    """  # pylint: disable=line-too-long # noqa: E501

    if isinstance(fromdate, datetime.datetime):
        fromdate = krx.datetime2string(fromdate, freq)

    if isinstance(todate, datetime.datetime):
        todate = krx.datetime2string(todate)

    fromdate = fromdate.replace("-", "")
    todate = todate.replace("-", "")

    df = krx.get_index_ohlcv_by_date(fromdate, todate, ticker)

    if name_display:
        df.columns.name = get_index_ticker_name(ticker)

    how = {
        '시가': 'first',
        '고가': 'max',
        '저가': 'min',
        '종가': 'last',
        '거래량': 'sum',
        '상장시가총액': 'last'
    }
    return resample_ohlcv(df, freq, how)


def get_index_fundamental(*args, **kwargs):
    """종목의 PER/PBR/배당수익률 조회

    Args:
        특정 종목의 지정된 기간 PER/PBR/배당수익률 조회

        fromdate     (str           ): 조회 시작 일자 (YYYYMMDD)
        todate       (str           ): 조회 종료 일자 (YYYYMMDD)
        ticker       (str           ): 조회 종목 티커
        freq         (str , optional): d - 일 / m - 월 / y - 년
        name_display (bool, optional): 종목 이름 출력 여부 (True/False)

        특정 일자의 전종목 PER/PBR/배당수익률 조회

        date   (str           ): 조회 일자 (YYMMDD)
        market (str,  optional): 조회 시장 (KOSPI/KOSDAQ/KONEX/ALL)
        prev   (bool, optional): 휴일일 경우 이전/이후 영업일 선택

    Returns:
        DataFrame:

            특정 종목의 지정된 기간 PER/PBR/배당수익률 조회

            >> get_index_fundamental("20210101", "20210130", "1001")

                           종가 등락률       PER 선행PER   PBR  배당수익률
            날짜
            2021-01-04  2944.45  2.47  30.200001    0.0  1.19      1.44
            2021-01-05  2990.57  1.57  30.680000    0.0  1.20      1.42
            2021-01-06  2968.21 -0.75  30.450001    0.0  1.20      1.43
            2021-01-07  3031.68  2.14  31.110001    0.0  1.22      1.40
            2021-01-08  3152.18  3.97  32.349998    0.0  1.27      1.35


            특정 일자의 전종목 PER/PBR/배당수익률 조회

            > get_index_fundamental("20210122")

                                     종가   등락률         PER  선행PER   PBR  배당수익률
            지수명
            코스피                 3140.63  -0.64    32.259998    0.0   1.27       1.35
            코스피 200              427.13  -0.73    29.760000    0.0   1.36       1.39
            코스피 100             3266.10  -0.78    28.910000    0.0   1.36       1.41
            코스피 50              3008.02  -0.79    28.530001    0.0   1.40       1.38
            코스피 200 중소형주    1264.5    0.40    62.820000    0.0   1.19       0.98
    """  # pylint: disable=line-too-long # noqa: E501

    dates = list(filter(regex_yymmdd.match, [str(x) for x in args]))
    if len(dates) == 2 or ('fromdate' in kwargs and
                           'todate' in kwargs):
        return get_index_fundamental_by_date(*args, **kwargs)
    else:
        return get_index_fundamental_by_ticker(*args, **kwargs)


def get_index_fundamental_by_ticker(
        date: str, market: str = "KOSPI", alternative: bool = False) \
        -> DataFrame:
    """특정 일자의 전종목 PER/PBR/배당수익률 조회

    Args:
        date        (str           ): 조회 일자 (YYYYMMDD)
        market      (str           ): 조회 시장 (KOSPI/KOSDAQ/KRX/테마)
        alternative (bool, optional): 휴일일 경우 이전 영업일 선택 여부

    Returns:
        DataFrame:

            >> get_index_fundamental_by_ticker("20210122")

                                     종가   등락률         PER  선행PER   PBR  배당수익률
            지수명
            코스피                 3140.63  -0.64    32.259998    0.0   1.27       1.35
            코스피 200              427.13  -0.73    29.760000    0.0   1.36       1.39
            코스피 100             3266.10  -0.78    28.910000    0.0   1.36       1.41
            코스피 50              3008.02  -0.79    28.530001    0.0   1.40       1.38
            코스피 200 중소형주     1264.5   0.40    62.820000    0.0   1.19       0.98
    """  # pylint: disable=line-too-long # noqa: E501

    if isinstance(date, datetime.datetime):
        date = krx.datetime2string(date)

    date = date.replace("-", "")

    df = krx.get_index_fundamental_by_ticker(date, market)
    holiday = (df[['종가', '등락률', 'PER', '선행PER', 'PBR', '배당수익률']]
               == 0).all(axis=None)
    if holiday and alternative:
        target_date = get_nearest_business_day_in_a_week(date=date, prev=True)
        df = krx.get_index_fundamental_by_ticker(target_date, market)
    return df


def get_index_fundamental_by_date(
    fromdate: str, todate: str, ticker: str, prev: bool = True) \
        -> DataFrame:
    """특정 종목의 지정된 기간 PER/PBR/배당수익률 조회

    Args:
        fromdate     (str           ): 조회 시작 일자 (YYMMDD)
        todate       (str           ): 조회 종료 일자 (YYMMDD)
        ticker       (str           ): 조회 인덱스 티커
        prev         (bool, optional): 휴일일 경우 이전/이후 영업일 선택

    Returns:
        DataFrame:

        >> get_index_fundamental_by_date("20210101", "20210130", "1001")

                       종가 등락률       PER 선행PER   PBR  배당수익률
        날짜
        2021-01-04  2944.45  2.47  30.200001    0.0  1.19      1.44
        2021-01-05  2990.57  1.57  30.680000    0.0  1.20      1.42
        2021-01-06  2968.21 -0.75  30.450001    0.0  1.20      1.43
        2021-01-07  3031.68  2.14  31.110001    0.0  1.22      1.40
        2021-01-08  3152.18  3.97  32.349998    0.0  1.27      1.35
    """

    if isinstance(fromdate, datetime.datetime):
        fromdate = krx.datetime2string(fromdate, prev)

    if isinstance(todate, datetime.datetime):
        todate = krx.datetime2string(todate)

    fromdate = fromdate.replace("-", "")
    todate = todate.replace("-", "")

    df = krx.get_index_fundamental_by_date(fromdate, todate, ticker)
    return df


def get_index_listing_date(계열구분: str = "KOSPI") -> DataFrame:
    """[11004] 전체지수 기본정보

    Args:
        계열구분 (str, optional): KRX/KOSPI/KOSDAQ/테마

    Returns:

        DataFrame:

            >> get_index_listing_date()

                                   기준시점    발표시점   기준지수  종목수
            지수명
            코스피               1980.01.04  1983.01.04      100.0     796
            코스피 200           1990.01.03  1994.06.15      100.0     201
            코스피 100           2000.01.04  2000.03.02     1000.0     100
            코스피 50            2000.01.04  2000.03.02     1000.0      50
            코스피 200 중소형주  2010.01.04  2015.07.13     1000.0     101
    """

    defined_list = ["KRX", "KOSPI", "KOSDAQ", "테마"]
    if 계열구분 not in defined_list:
        print(f"{계열구분}이 올바르지 않습니다.")
        print(f" - 허용된 값: {' '.join(defined_list)}")
        print("KOSPI로 변경 조회합니다.")
        계열구분 = "KOSPI"

    return krx.get_index_listing_date(계열구분)


@deprecated(version='1.0',
            reason="You should use get_index_price_change_by_ticker() instead")
def get_index_price_change_by_name(fromdate, todate, market="KOSPI"):
    return get_index_price_change_by_ticker(fromdate, todate, market)


def get_index_price_change(
        fromdate: str, todate: str, market: str = "KOSPI") -> DataFrame:
    """입력된 기간동안의 전체 지수 등락률

    Args:
        fromdate (str          ): 조회 시작 일자 (YYMMDD)
        todate   (str          ): 조회 종료 일자 (YYMMDD)
        market   (str, optional): 조회 시장 (KOSPI/KOSDAQ/RKX/테마)

    Returns:
        DataFrame:

            >> get_index_price_change_by_ticker("20210101", "20210130")

                                      시가      종가     등락률      거래량         거래대금
            지수명
            코스피                 2873.47   3152.18   9.703125  7162398637  149561467924511
            코스피 200              389.29    430.22  10.507812  2221276866  119905899468167
            코스피 100             2974.06   3293.96  10.757812  1142234783   95023508273187
            코스피 50              2725.20   3031.59  11.242188   742099360   79663247553065
            코스피 200 중소형주    1151.78   1240.92   7.738281  1079042083   24882391194980
    """  # pylint: disable=line-too-long # noqa: E501

    return get_index_price_change_by_ticker(fromdate, todate, market)


def get_index_price_change_by_ticker(
        fromdate: str, todate: str, market: str = "KOSPI") -> DataFrame:
    """입력된 기간동안의 전체 지수 등락률

    Args:
        fromdate (str          ): 조회 시작 일자 (YYMMDD)
        todate   (str          ): 조회 종료 일자 (YYMMDD)
        market   (str, optional): 조회 시장 (KOSPI/KOSDAQ/RKX/테마)

    Returns:
        DataFrame:

            >> get_index_price_change_by_ticker("20210101", "20210130")

                                      시가      종가     등락률      거래량         거래대금
            지수명
            코스피                 2873.47   3152.18   9.703125  7162398637  149561467924511
            코스피 200              389.29    430.22  10.507812  2221276866  119905899468167
            코스피 100             2974.06   3293.96  10.757812  1142234783   95023508273187
            코스피 50              2725.20   3031.59  11.242188   742099360   79663247553065
            코스피 200 중소형주    1151.78   1240.92   7.738281  1079042083   24882391194980
    """  # pylint: disable=line-too-long # noqa: E501

    if isinstance(fromdate, datetime.datetime):
        fromdate = krx.datetime2string(fromdate)
    if isinstance(todate, datetime.datetime):
        todate = krx.datetime2string(todate)

    fromdate = fromdate.replace("-", "")
    todate = todate.replace("-", "")

    # KRX 웹 서버의 제약으로 인한 영업일 검사
    fromdate = get_nearest_business_day_in_a_week(fromdate, prev=False)
    todate = get_nearest_business_day_in_a_week(todate)

    return krx.get_index_price_change_by_ticker(fromdate, todate, market)


def get_market_sector_classifications(date: str, market: str) -> DataFrame:
    if isinstance(date, datetime.datetime):
        date = krx.datetime2string(date)
    date = date.replace("-", "")

    df = krx.get_market_sector_classifications(date, market)
    if (df["종가"] == 0).all(axis=None):
        return DataFrame()
    return df


# -----------------------------------------------------------------------------
# 공매도(SHORTING) API
# -----------------------------------------------------------------------------

# def get_shorting_status(*args, **kwargs):
#     """
#     Args:
#         특정 종목의 지정된 기간 OHLCV 조회

#         fromdate     (str           ): 조회 시작 일자 (YYMMDD)
#         todate       (str           ): 조회 종료 일자 (YYMMDD)
#         ticker       (str           ): 조회 인덱스 티커
#         freq         (str, optional ): d - 일 / m - 월 / y - 년
#         name_display (bool, optional): 인덱스 이름 출력 유무

#         특정 일자의 전종목 OHLCV 조회

#         date   (str): 조회 일자 (YYYYMMDD)
#         market (str): 조회 시장 (KOSPI/KOSDAQ/KRX/테마/ALL)

#     Returns:
#         DataFrame:

#             특정 종목의 지정된 기간 OHLCV 조회

#             >> get_shorting_status("20210101", "20210130", "1001")

#                             시가    고가     저가     종가      거래량        거래대금
#             날짜
#             2021-01-04  2874.50  2946.54  2869.11  2944.45  1026510465  25011393960858
#             2021-01-05  2943.67  2990.57  2921.84  2990.57  1519911750  26548380179493
#             2021-01-06  2993.34  3027.16  2961.37  2968.21  1793418534  29909396443430
#             2021-01-07  2980.75  3055.28  2980.75  3031.68  1524654500  27182807334912
#             2021-01-08  3040.11  3161.11  3040.11  3152.18  1297903388  40909490005818

#             특정 일자의 전종목 OHLCV 조회

#             >> get_shorting_status("20210122")

#                                     시가      고가      저가      종가      거래량        거래대금
#             지수명
#             코스피외국주포함        0.00      0.00      0.00      0.00  1111222984  24305355507985
#             코스피               3163.83   3185.26   3140.60   3140.63  1110004515  24300291238350
#             코스피200             430.73    433.84    427.13    427.13   269257473  19087641760593
#             코스피100            3295.34   3318.12   3266.10   3266.10   164218193  15401724965613
#             코스피50             3034.99   3055.71   3008.02   3008.02   110775949  13083864634083
#     """  # pylint: disable=line-too-long # noqa: E501
#
#     dates = list(filter(yymmdd.match, [str(x) for x in args]))
#     if len(dates) == 2:
#         return get_shorting_status_by_date(*args, **kwargs)
#     else:
#         return get_shorting_status_by_ticker(*args, **kwargs)


def get_shorting_status_by_date(fromdate: str, todate: str, ticker: str) \
        -> DataFrame:
    """공매도 거래량/누적수량/거래대금/누적잔고

    Args:
        fromdate (str): 조회 시작 일자 (YYMMDD)
        todate   (str): 조회 종료 일자 (YYMMDD)
        market   (str): 조회 종목 티커
    Returns:

        DataFrame:

            >> get_shorting_status_by_date("20210104", "20210108", "005930")

                      거래량 잔고수량   거래대금      잔고금액
            날짜
            2021-01-04  9279  2722585  771889500  225974555000
            2021-01-05   169  2676924   14011100  224593923600
            2021-01-06   967  3002548   80855100  246809445600
            2021-01-07   763  2447030   63634800  202858787000
            2021-01-08     6  2319328     534000  205956326400
    """
    return krx.get_shorting_status_by_date(fromdate, todate, ticker)


@market_valid_check(["KOSPI", "KOSDAQ", "KONEX"])
def get_shorting_value_by_ticker(
    date: str, market: str = "KOSPI", include: list = None,
        alternative: bool = False) -> DataFrame:
    """티커별로 정리된 전종목 공매도 거래 대금

    Args:
        date        (str, optional): 조회 일자 (YYYYMMDD)
        market      (str, optional): 조회 시장 (KOSPI/KOSDAQ/KONEX)
        include     (str, optional): 증권 구분 (주식/ETF/ETN/ELW/신주인수권및증권/수익증권)
        alternative (bool, optional): 휴일일 경우 이전 영업일 선택 여부

    Returns:
        DataFrame:

            >> get_shorting_value_by_ticker("20210125")

                       공매도         매수      비중
            티커
            095570     134240    757272515  0.020004
            006840    2377900  11554067000  0.020004
            027410  108713300  49276275460  0.219971
            282330   14928000  13018465500  0.109985
            138930   10635610   6658032800  0.160034
    """  # pylint: disable=line-too-long # noqa: E501

    if isinstance(date, datetime.datetime):
        date = krx.datetime2string(date)
    date = date.replace("-", "")

    if include is None:
        include = ["주식"]

    df = krx.get_shorting_trading_value_and_volume_by_ticker(
        date, market, include)
    if df.empty:
        target_date = get_nearest_business_day_in_a_week(date=date, prev=True)
        df = krx.get_shorting_trading_value_and_volume_by_ticker(
            target_date, market, include)
        if not alternative:
            df.loc[:] = 0
        return df['거래대금']

    return df['거래대금']


@market_valid_check(["KOSPI", "KOSDAQ", "KONEX"])
def get_shorting_volume_by_ticker(
    date: str, market: str = "KOSPI", include: list = None,
        alternative: bool = False) -> DataFrame:
    """티커별로 정리된 전종목 공매도 거래량

    Args:
        date        (str, optional): 조회 일자 (YYYYMMDD)
        market      (str, optional): 조회 시장 (KOSPI/KOSDAQ/KONEX)
        include     (str, optional): 증권 구분 (주식/ETF/ETN/ELW/신주인수권및증권/수익증권)
        alternative (bool, optional): 휴일일 경우 이전 영업일 선택 여부

    NOTE: include 항목을 입력하지 않으면 "주식"만 조회

    Returns:
        DataFrame:

            >> get_shorting_volume_by_ticker("20210125")

                   공매도     매수      비중
            티커
            095570     32   180458  0.020004
            006840     79   386257  0.020004
            027410  18502  8453962  0.219971
            282330     96    82986  0.119995
            138930   1889  1181748  0.160034
    """  # pylint: disable=line-too-long # noqa: E501

    if isinstance(date, datetime.datetime):
        date = krx.datetime2string(date)

    date = date.replace("-", "")

    if include is None:
        include = ["주식"]

    df = krx.get_shorting_trading_value_and_volume_by_ticker(
        date, market, include)
    if df.empty:
        target_date = get_nearest_business_day_in_a_week(
            date=date, prev=True)
        df = krx.get_shorting_trading_value_and_volume_by_ticker(
            target_date, market, include)
        if not alternative:
            df.loc[:] = 0
        return df['거래량']

    return df['거래량']


def get_shorting_volume_by_date(
        fromdate: str, todate: str, ticker: str) -> DataFrame:
    """일자별로 정렬된 공매도 거래량

    Args:
        fromdate (str): 조회 시작 일자 (YYMMDD)
        todate   (str): 조회 종료 일자 (YYMMDD)
        ticker   (str): 조회 종목 티커

    Returns:
        DataFrame:

            >> get_shorting_volume_by_date("20210104", "20210108", "005930")

                      공매도      매수      비중
            날짜
            2021-01-04  9279  38655276  0.020004
            2021-01-05   169  35335669  0.000000
            2021-01-06   967  42089013  0.000000
            2021-01-07   763  32644642  0.000000
            2021-01-08     6  59013307  0.000000
    """

    if isinstance(fromdate, datetime.datetime):
        fromdate = krx.datetime2string(fromdate)
    if isinstance(todate, datetime.datetime):
        todate = krx.datetime2string(todate)

    fromdate = fromdate.replace("-", "")
    todate = todate.replace("-", "")

    df = krx.get_shorting_trading_value_and_volume_by_date(
        fromdate, todate, ticker)
    return df['거래량']


def get_shorting_value_by_date(fromdate: str, todate: str, ticker: str)  \
        -> DataFrame:
    """일자별로 정렬된 공매도 거래량

    Args:
        fromdate (str): 조회 시작 일자 (YYMMDD)
        todate   (str): 조회 종료 일자 (YYMMDD)
        ticker   (str): 조회 종목 티커

    Returns:
        DataFrame:

            >> get_shorting_value_by_date("20210104", "20210108", "005930")

                           공매도           매수      비중
            날짜
            2021-01-04  771889500  3185356823460  0.020004
            2021-01-05   14011100  2915618322800  0.000000
            2021-01-06   80855100  3506903681680  0.000000
            2021-01-07   63634800  2726112459660  0.000000
            2021-01-08     534000  5083939899952  0.000000
    """

    if isinstance(fromdate, datetime.datetime):
        fromdate = krx.datetime2string(fromdate)
    if isinstance(todate, datetime.datetime):
        todate = krx.datetime2string(todate)

    fromdate = fromdate.replace("-", "")
    todate = todate.replace("-", "")

    df = krx.get_shorting_trading_value_and_volume_by_date(
        fromdate, todate, ticker)
    return df['거래대금']


@market_valid_check(["KOSPI", "KOSDAQ", "KONEX"])
def get_shorting_investor_volume_by_date(
        fromdate: str, todate: str, market: str = "KOSPI") -> DataFrame:
    """투자자별 공매도 잔고 수량

    Args:
        fromdate (str          ): 조회 시작 일자 (YYMMDD)
        todate   (str          ): 조회 종료 일자 (YYMMDD)
        market   (str, optional): 조회 시장 (KOSPI/KOSDAQ/KONEX)

    Returns:
        DataFrame:

            >> get_shorting_investor_volume_by_date("20200106", "20200110")

                            기관    개인    외국인 기타      합계
            날짜
            2020-01-06  3783324  215700   9213745    0  13212769
            2020-01-07  3627906  270121   7112215    0  11010242
            2020-01-08  5161993  284087  13164830    0  18610910
            2020-01-09  5265706  271622  11138406    0  16675734
            2020-01-10  5129724  141885   7849543    0  13121152
    """
    if isinstance(fromdate, datetime.datetime):
        fromdate = krx.datetime2string(fromdate)
    if isinstance(todate, datetime.datetime):
        todate = krx.datetime2string(todate)

    fromdate = fromdate.replace("-", "")
    todate = todate.replace("-", "")

    return krx.get_shorting_investor_by_date(
        fromdate, todate, market, "거래량")


@market_valid_check(["KOSPI", "KOSDAQ", "KONEX"])
def get_shorting_investor_value_by_date(
        fromdate: str, todate: str, market: str = "KOSPI") -> DataFrame:
    """투자자별 공매도 잔고 수량

    Args:
        fromdate (str          ): 조회 시작 일자 (YYMMDD)
        todate   (str          ): 조회 종료 일자 (YYMMDD)
        market   (str, optional): 조회 시장 (KOSPI/KOSDAQ/KONEX)

    Returns:
        DataFrame:

            >> get_shorting_investor_value_by_date("20200106", "20200110")

                                기관        개인        외국인  기타        합계
            날짜
            2020-01-06  135954452715  2502658310  119387130395   0  257844241420
            2020-01-07  140062017520  2924582225  129899020748   0  272885620493
            2020-01-08  175731372983  2579881000  266907627745   0  445218881728
            2020-01-09  189541838466  3021427705  241819376326   0  434382642497
            2020-01-10  185561759364  3182000295  165327866557   0  354071626216
    """  # pylint: disable=line-too-long # noqa: E501

    if isinstance(fromdate, datetime.datetime):
        fromdate = krx.datetime2string(fromdate)
    if isinstance(todate, datetime.datetime):
        todate = krx.datetime2string(todate)

    fromdate = fromdate.replace("-", "")
    todate = todate.replace("-", "")

    return krx.get_shorting_investor_by_date(
        fromdate, todate, market, "거래대금")


@market_valid_check(["KOSPI", "KOSDAQ", "KONEX"])
def get_shorting_volume_top50(date: str, market: str = "KOSPI") -> DataFrame:
    """공매도 비중 상위 50개 종목 정보
        - 비중 = 거래대금/거래대금

    Args:
        date   (str): 조회 일자 (YYMMDD)
        market (str): 조회 시장 (KOSPI/KOSDAQ/KONEX)

    Returns:
        DataFrame:

            >> get_shorting_volume_top50("20210127")

                   순위  공매도거래대금   총거래대금  공매도비중  직전40일거래대금평균  공매도거래대금증가율  직전40일공매도평균비중  공매도비중증가율  주가수익률
            티커
            003545   1         38510030    915824030        4.21               5814411                  6.62                    0.51              8.33       -1.25
            267290   2         13265200    329805000        4.02               2755259                  4.82                    0.66              6.14       -2.46
            015890   3         15865860    428852660        3.70               8316412                  1.91                    1.30              2.85       -4.46
            005945   4         25401240    908915950        2.79               4610634                  5.51                    0.44              6.40       -0.35
            227840   5         13784400    546597900        2.52               3084294                  4.47                    0.51              4.91       -2.37

            >> get_shorting_volume_top50("20210127", market="KOSDAQ")

                  순위    공매도거래대금   총거래대금  공매도비중  직전40일거래대금평균  공매도거래대금증가율  직전40일공매도평균비중  공매도비중증가율  주가수익률
            티커
            003800   1           2460150    140242350        1.75               1881099                  1.31                    2.33              0.75        1.17
            018120   2           3141500    230696500        1.36               2671128                  1.18                    1.99              0.68        0.00
            092130   3           2217850    190657900        1.16               3134340                  0.71                    0.76              1.52       -1.10
            260930   4           1197350    126576300        0.95                641443                  1.87                    0.27              3.56        0.97
    """  # pylint: disable=line-too-long # noqa: E501

    if isinstance(date, datetime.datetime):
        date = krx.datetime2string(date)

    date = date.replace("-", "")

    return krx.get_shorting_volume_top50(date, market)


@market_valid_check(["KOSPI", "KOSDAQ", "KONEX"])
def get_shorting_balance_top50(date: str, market: str = "KOSPI") -> DataFrame:
    """공매도 잔고 상위 50개 종목 정보
       - 공매비중 = 공매도 잔고/상장주식수

    Args:
        date   (str): 조회 일자 (YYMMDD)
        market (str): 조회 시장 (KOSPI/KOSDAQ/KONEX)

    Returns:
        DataFrame:

            >> get_shorting_balance_top50("20210127")

                  순위  공매도잔고  상장주식수     공매도금액      시가총액      비중
            티커
            032350   1     4693027    69275662    74853780650  1.104947e+12  6.769531
            042670   2    10846251   215931625    92843908560  1.848375e+12  5.019531
            068270   3     6523965   134997805  2146384485000  4.441428e+13  4.828125
            008770   4     1269261    39248121   106237145700  3.285068e+12  3.230469
            011690   5     1604890    58494201     1957965800  7.136293e+10  2.740234

            >> get_shorting_balance_top50("20210129", market="KOSDAQ")

                  순위  공매도잔고  상장주식수    공매도금액      시가총액      비중
            티커
            215600   1     6497785    71617125   78623198500  8.665672e+11  9.070312
            032500   2     2846196    39820883  217733994000  3.046298e+12  7.148438
            028300   3     3483185    53013031  313834968500  4.776474e+12  6.570312
            263750   4      443142    13189850  139944243600  4.165355e+12  3.359375
            078130   5     4034831   126348384   18297958585  5.729899e+11  3.189453
    """  # pylint: disable=line-too-long # noqa: E501

    if isinstance(date, datetime.datetime):
        date = krx.datetime2string(date)

    date = date.replace("-", "")

    return krx.get_shorting_balance_top50(date, market)


def get_shorting_balance(*args, **kwargs) -> DataFrame:
    """공매도 잔고 현황

    1) 일자별로 정렬된 공매도 잔고 현황

    Args:
        fromdate (str): 조회 시작 일자 (YYMMDD)
        todate   (str): 조회 종료 일자 (YYMMDD)
        ticker   (str): 조회 종목 티커

    Returns:
        DataFrame:

            >> get_shorting_balance("20200106", "20200110", "005930")

                     공매도잔고  상장주식수    공매도금액      시가총액      비중
            날짜
            2020-01-06  5630893  5969782550  312514561500  3.313229e+14  0.090027
            2020-01-07  5169745  5969782550  288471771000  3.331139e+14  0.090027
            2020-01-08  5224233  5969782550  296736434400  3.390836e+14  0.090027
            2020-01-09  5387073  5969782550  315682477800  3.498293e+14  0.090027
            2020-01-10  5489240  5969782550  326609780000  3.552021e+14  0.090027

    2) 티커로 정렬된 공매도 잔고 현황

    Args:
        date   (str): 조회 일자 (YYMMDD)
        market (str): 조회 시장 (KOSPI/KOSDAQ/KONEX)

    Returns:
        DataFrame:

            >> get_shorting_balance("20210127")

                    공매도잔고   상장주식수  공매도금액      시가총액      비중
            티커
            095570       33055     46822295   134864400  1.910350e+11  0.070007
            006840        4575     13247561   131760000  3.815298e+11  0.029999
            027410       68060     95716791   449196000  6.317308e+11  0.070007
            282330        4794     17283906   757452000  2.730857e+12  0.029999
            138930      596477    325935246  3340271200  1.825237e+12  0.180054
    """  # pylint: disable=line-too-long # noqa: E501

    dates = list(filter(regex_yymmdd.match, [str(x) for x in args]))
    if len(dates) == 2 or ('fromdate' in kwargs and
                           'todate' in kwargs):
        return get_shorting_balance_by_date(*args, **kwargs)
    else:
        return get_shorting_balance_by_ticker(*args, **kwargs)


@market_valid_check(["KOSPI", "KOSDAQ", "KONEX"])
def get_shorting_balance_by_ticker(date: str, market: str = "KOSPI") \
        -> DataFrame:
    """티커로 정렬된 공매도 잔고 현황

    Args:
        date   (str): 조회 일자 (YYMMDD)
        market (str): 조회 시장 (KOSPI/KOSDAQ/KONEX)

    Returns:
        DataFrame:

            >> get_shorting_balance_by_ticker("20210127")

                    공매도잔고   상장주식수  공매도금액      시가총액      비중
            티커
            095570       33055     46822295   134864400  1.910350e+11  0.070007
            006840        4575     13247561   131760000  3.815298e+11  0.029999
            027410       68060     95716791   449196000  6.317308e+11  0.070007
            282330        4794     17283906   757452000  2.730857e+12  0.029999
            138930      596477    325935246  3340271200  1.825237e+12  0.180054
    """

    if isinstance(date, datetime.datetime):
        date = krx.datetime2string(date)

    date = date.replace("-", "")

    return krx.get_shorting_balance_by_ticker(date, market)


def get_shorting_balance_by_date(fromdate: str, todate: str, ticker: str) \
        -> DataFrame:
    """일자별로 정렬된 공매도 잔고 현황

    Args:
        fromdate (str): 조회 시작 일자 (YYMMDD)
        todate   (str): 조회 종료 일자 (YYMMDD)
        ticker   (str): 조회 종목 티커

    Returns:
        DataFrame:

            >> get_shorting_balance_by_date("20200106", "20200110", "005930")

                     공매도잔고  상장주식수    공매도금액      시가총액      비중
            날짜
            2020-01-06  5630893  5969782550  312514561500  3.313229e+14  0.090027
            2020-01-07  5169745  5969782550  288471771000  3.331139e+14  0.090027
            2020-01-08  5224233  5969782550  296736434400  3.390836e+14  0.090027
            2020-01-09  5387073  5969782550  315682477800  3.498293e+14  0.090027
            2020-01-10  5489240  5969782550  326609780000  3.552021e+14  0.090027
    """  # pylint: disable=line-too-long # noqa: E501

    if isinstance(fromdate, datetime.datetime):
        fromdate = krx.datetime2string(fromdate)
    if isinstance(todate, datetime.datetime):
        todate = krx.datetime2string(todate)

    fromdate = fromdate.replace("-", "")
    todate = todate.replace("-", "")

    return krx.get_shorting_balance_by_date(fromdate, todate, ticker)


# -----------------------------------------------------------------------------
# ETX API
# -----------------------------------------------------------------------------
@market_valid_check(["ETF", "ETN", "ELW", "ALL"])
def get_etx_ticker_list(market: str, date: str = None) -> list:
    """ETX 티커 목록 조회

    Args:
        market (str          ): 조회 시장 (ETF/ETN/ELW/ALL)
        date   (str, optional): 조회 일자 (YYMMDD)
         - 입력하지 않을 경우 당일 기준 티커 조회

    Returns:
        list:

            >> get_etx_ticker_list("ETF")

            ['292340', '159800', '361580', '285000', '287300', '287310', ....]
    """

    if date is None:
        date = get_nearest_business_day_in_a_week()
    if isinstance(date, datetime.datetime):
        date = krx.datetime2string(date)

    date = date.replace("-", "")

    return krx.get_etx_ticker_list(date, market)


def get_etf_ticker_list(date: str = None) -> list:
    """ETF 티커 목록 조회

    Args:
        date (str, optional): 조회 일자 (YYMMDD)
         - 입력하지 않을 경우 당일 기준 티커 조회

    Returns:
        list:

            >> get_etf_ticker_list("20021014")

            ['069500', '069660']
    """

    if date is None:
        date = get_nearest_business_day_in_a_week()
    if isinstance(date, datetime.datetime):
        date = krx.datetime2string(date)

    date = date.replace("-", "")

    return krx.get_etx_ticker_list(date, "ETF")


def get_etn_ticker_list(date: str = None) -> list:
    """ETN 티커 목록 조회

    Args:
        date (str, optional): 조회 일자 (YYMMDD)
         - 입력하지 않을 경우 당일 기준 티커 조회

    Returns:
        list:

            >> get_etn_ticker_list("20141215")

            ['550001', '550002', '500001', '500002']
    """

    if date is None:
        date = get_nearest_business_day_in_a_week()
    if isinstance(date, datetime.datetime):
        date = krx.datetime2string(date)

    date = date.replace("-", "")

    return krx.get_etx_ticker_list(date, "ETN")


def get_elw_ticker_list(date: str = None) -> list:
    """ETW 티커 목록 조회

    Args:
        date (str, optional): 조회 일자 (YYMMDD)
         - 입력하지 않을 경우 당일 기준 티커 조회

    Returns:
        list:

            >> get_elw_ticker_list("20200306")

            ['58F194', '58F195']
    """

    if date is None:
        date = get_nearest_business_day_in_a_week()
    if isinstance(date, datetime.datetime):
        date = krx.datetime2string(date)

    date = date.replace("-", "")

    return krx.get_etx_ticker_list(date, "ELW")


def get_etf_ticker_name(ticker: str) -> str:
    """종목 이름 조회

    Args:
        ticker (str): 티커

    Returns:
        str: 종목명

            >> get_etf_ticker_name("069500")
            KODEX 200
    """

    return krx.get_etx_name(ticker)


def get_etn_ticker_name(ticker: str) -> str:
    """종목 이름 조회

    Args:
        ticker (str): 티커

    Returns:
        str: 종목명

            >> get_etn_ticker_name("550001")
            QV Big Vol ETN
    """

    return krx.get_etx_name(ticker)


def get_elw_ticker_name(ticker: str) -> str:
    """종목 이름 조회

    Args:
        ticker (str): 티커

    Returns:
        str: 종목명

            >> get_elw_ticker_name("58F194")
            KODEX 200
    """

    return krx.get_etx_name(ticker)


def get_etf_isin(ticker: str) -> str:
    """ISIN 조회

    Args:
        ticker (str): [description]

    Returns:
        str: ISIN

            >> get_etf_isin("069500")
            KR7069500007
    """

    return krx.get_etx_isin(ticker)


def get_etf_ohlcv_by_date(
        fromdate: str, todate: str, ticker: str, freq: str = "d") -> DataFrame:
    """일자별로 정렬된 특정 종목의 OHLCV 조회

    Args:
        fromdate     (str           ): 조회 시작 일자 (YYYYMMDD)
        todate       (str           ): 조회 종료 일자 (YYYYMMDD)
        ticker       (str           ): 조회할 종목의 티커
        freq         (str,  optional): d - 일 / m - 월 / y - 년

    Returns:
        DataFrame:

            >> get_etf_ohlcv_by_date("20210104", "20210108", "292340")

                            NAV  시가  고가  저가  종가 거래량  거래대금  기초지수
            날짜
            2021-01-04  9737.23  9730  9730  9730  9730     81    788130   1303.29
            2021-01-05  9756.27  9705  9990  9700  9770      6     58845   1306.59
            2021-01-06  9796.98     0     0     0  9770      0         0   1306.76
            2021-01-07  9723.65  9845  9855  9845  9855      2     19700   1301.65
            2021-01-08  9771.73  9895  9900  9855  9885      6     59320   1306.73

            >> get_etf_ohlcv_by_date("20200101", "20200630", "292340", freq="m")

                            NAV  시가  고가  저가  종가 거래량   거래대금 기초지수
            날짜
            2020-01-31  8910.61  8900  9270   0  8795   36559   330991070  1231.00
            2020-02-29  8633.13     0  9395   0  7555      72      658080  1213.88
            2020-03-31  7720.09  7520  9965   0  6030  206070  1373727350  1149.86
            2020-04-30  5590.35  6055  6975   0  6975    8743    57352845   997.80
            2020-05-31  6845.59  6835  7450   0  7415    1788    13057270  1107.92
            2020-06-30  7469.16  7425  7570   0  7305    1265     9391255  1155.68
    """  # pylint: disable=line-too-long # noqa: E501

    if isinstance(fromdate, datetime.datetime):
        fromdate = krx.datetime2string(fromdate)

    if isinstance(todate, datetime.datetime):
        todate = krx.datetime2string(todate)

    fromdate = fromdate.replace("-", "")
    todate = todate.replace("-", "")

    df = krx.get_etf_ohlcv_by_date(fromdate, todate, ticker)

    how = {
        'NAV': 'first',
        '시가': 'first',
        '고가': 'max',
        '저가': 'min',
        '종가': 'last',
        '거래량': 'sum',
        '거래대금': 'sum',
        '기초지수': 'first'
    }

    return resample_ohlcv(df, freq, how)


def get_etf_ohlcv_by_ticker(date: str) -> DataFrame:
    """특정 일자의 전종목의 OHLCV 조회
        - 휴일일 경우 비어있는 DataFrame을 반환
    Args:
        date(str): 조회 일자 (YYYYMMDD)

    Returns:
        DataFrame:
            >> get_etf_ohlcv_by_ticker("20210325")

                          NAV   시가   고가   저가    종가 거래량    거래대금  기초지수
            티커
            152100   41887.33  41705  42145  41585   41835  59317  2479398465    408.53
            295820   10969.41  10780  10945  10780   10915     69      750210   2364.03
            253150   46182.13  45640  46700  45540   46145   1561    71730335   2043.75
            253160    4344.07   4400   4400   4295    4340  58943   256679440   2043.75
            278420    9145.45   9055   9150   9055    9105   1164    10598375   1234.03

            >> get_etf_ohlcv_by_ticker("20210321")

            Empty DataFrame
            Columns: []
            Index: []
    """  # pylint: disable=line-too-long # noqa: E501

    if isinstance(date, datetime.datetime):
        date = krx.datetime2string(date)

    date = date.replace("-", "")

    df = krx.get_etf_ohlcv_by_ticker(date)
    if (df == 0).all(axis=None):
        return DataFrame()
    return df


def get_etf_price_change_by_ticker(fromdate: str, todate: str) -> DataFrame:
    """특정 기간동안 전종의 등락률 조회

    Args:
        fromdate(str): 조회 시작 일자 (YYYYMMDD)
        todate  (str): 조회 종료 일자 (YYYYMMDD)

    Returns:
        DataFrame:

            >> get_etf_price_change_by_ticker("20210325", "20210402")

                      시가    종가  변동폭  등락률   거래량     거래대금
              티커
            152100   41715   43405    1690    4.05  1002296  42802174550
            295820   10855   11185     330    3.04     1244     13820930
            253150   45770   49735    3965    8.66    13603    650641700
            253160    4380    4015    -365   -8.33   488304   2040509925
            278420    9095    9385     290    3.19     9114     84463155

    """

    if isinstance(fromdate, datetime.datetime):
        fromdate = krx.datetime2string(fromdate)

    if isinstance(todate, datetime.datetime):
        todate = krx.datetime2string(todate)

    fromdate = fromdate.replace("-", "")
    todate = todate.replace("-", "")

    fromdate = get_nearest_business_day_in_a_week(fromdate, prev=False)
    todate = get_nearest_business_day_in_a_week(todate)

    return krx.get_etf_price_change_by_ticker(fromdate, todate)


def get_etf_portfolio_deposit_file(ticker: str, date: str = None) -> DataFrame:
    """PDF 상세 내역 조회

    Args:
        ticker (str          ): 조회 종목 티커
        date   (str, optional): 조회 일자 (YYMMDD)

    Returns:
        DataFrame:

            >> get_etf_portfolio_deposit_file("152100")

                     계약수       금액    비중
            티커
            005930   8140.0  667480000  31.77
            000660    968.0  118580000   5.69
            035420    218.0   74774000   3.57
            051910     79.0   72443000   3.53
            068270    184.0   59616000   3.21
    """

    if date is None:
        date = get_nearest_business_day_in_a_week()
    return krx.get_etf_portfolio_deposit_file(date, ticker)


def get_etf_price_deviation(fromdate: str, todate: str, ticker: str) \
        -> DataFrame:
    """괴리율 조회

    Args:
        fromdate (str): 조회 시작 일자 (YYYYMMDD)
        todate   (str): 조회 종료 일자 (YYYYMMDD)
        ticker   (str): 조회할 종목의 티커

    Returns:
        DataFrame:

            >> get_etf_price_deviation("20210104", "20210108", "152100")

                         종가       NAV   괴리율
            날짜
            2021-01-04  40815  40885.24    -0.17
            2021-01-05  41450  41510.71    -0.15
            2021-01-06  40985  41112.12    -0.31
            2021-01-07  41935  42002.85    -0.16
            2021-01-08  43845  43983.05    -0.31
    """

    if isinstance(fromdate, datetime.datetime):
        fromdate = krx.datetime2string(fromdate)
    if isinstance(todate, datetime.datetime):
        todate = krx.datetime2string(todate)

    fromdate = fromdate.replace("-", "")
    todate = todate.replace("-", "")

    return krx.get_etf_price_deviation(fromdate, todate, ticker)


def get_etf_tracking_error(fromdate, todate, ticker) -> DataFrame:
    """추적 오차율 조회

    Args:
        fromdate (str): 조회 시작 일자 (YYYYMMDD)
        todate   (str): 조회 종료 일자 (YYYYMMDD)
        ticker   (str): 조회할 종목의 티커

    Returns:
        DataFrame:

            >> get_etf_tracking_error("20210104", "20210108", "152100")

                             NAV    지수  추적오차율
            날짜
            2021-01-04  40885.24  399.88        0.44
            2021-01-05  41510.71  406.03        0.44
            2021-01-06  41112.12  402.08        0.44
            2021-01-07  42002.85  410.81        0.44
            2021-01-08  43983.05  430.22        0.44
    """

    if isinstance(fromdate, datetime.datetime):
        fromdate = krx.datetime2string(fromdate)
    if isinstance(todate, datetime.datetime):
        todate = krx.datetime2string(todate)

    fromdate = fromdate.replace("-", "")
    todate = todate.replace("-", "")

    return krx.get_etf_tracking_error(fromdate, todate, ticker)


@overload
def get_etf_trading_volume_and_value(
    fromdate: str, todate: str) -> DataFrame: ...


@overload
def get_etf_trading_volume_and_value(fromdate: str, todate: str, # pylint: disable=function-redefined  # noqa
                                     ticker: str) -> DataFrame: ...


@overload
def get_etf_trading_volume_and_value(fromdate: str, todate: str, # pylint: disable=function-redefined  # noqa
                                     query_type1: str,
                                     query_type2: str) -> DataFrame: ...


@overload
def get_etf_trading_volume_and_value(fromdate: str, todate: str, ticker: str, # pylint: disable=function-redefined  # noqa
                                     query_type1: str,
                                     query_type2: str) -> DataFrame: ...


@dispatch(str, str)
def get_etf_trading_volume_and_value(fromdate: str, todate: str) -> DataFrame: # pylint: disable=function-redefined  # noqa
    """주어진 기간의 투자자별 거래실적 합계



    Args:
        fromdate    (str): 조회 시작 일자 (YYMMDD)
        todate      (str): 조회 종료 일자 (YYMMDD)

    Returns:
        DataFrame:
            > get_etf_trading_volume_and_value("20220908", "20220916")

                           거래량                              거래대금
                             매도        매수    순매수            매도            매수         순매수
            금융투자    345997085   324299496 -21697589   2837651098275   2547396695135 -290254403140
            보험          3274320     3016380   -257940     47863677445     67629772625   19766095180
            투신         35903044    34340340  -1562704    384044484795    442895636810   58851152015
            사모          9280868     9421625    140757    109053819595    176660869270   67607049675
            은행          9351480     6880644  -2470836    124954335800    120708432910   -4245902890
            기타금융       634616     1250658    616042      9073593605     17197744015    8124150410
            연기금 등      340847      497155    156308      5195478420      8181474870    2985996450
            기관합계    404782260   379706298 -25075962   3517836487935   3380670625635 -137165862300
            기타법인     16684374    16223283   -461091     78915300735     80730150250    1814849515
            개인        697741088   719636083  21894995   5592416203015   5681592506945   89176303930
            외국인      575071221   578396025   3324804   3210507902815   3255764637940   45256735125
            기타외국인    1808918     2126172    317254      9761352810     10679326540     917973730
            전체       1696087861  1696087861         0  12409437247310  12409437247310             0
    """  # pylint: disable=line-too-long # noqa: E501
    if isinstance(fromdate, datetime.datetime):
        fromdate = krx.datetime2string(fromdate)

    if isinstance(todate, datetime.datetime):
        todate = krx.datetime2string(todate)

    return krx.get_trading_volume_and_value_by_investor(fromdate, todate)


@dispatch(str, str, str)
def get_etf_trading_volume_and_value(fromdate: str, todate: str, # pylint: disable=function-redefined  # noqa
                                     ticker: str) -> DataFrame:
    """주어진 기간의 투자자별(개별종목) 거래실적 합계
    Args:
        fromdate    (str): 조회 시작 일자 (YYMMDD)
        todate      (str): 조회 종료 일자 (YYMMDD)
        ticker      (str): 조회할 종목의 티커

    Returns:
        DataFrame:
            > get_etf_trading_volume_and_value("20220908", "20220916", "069500")

                            거래량             거래대금
                            매도  매수 순매수      매도    매수 순매수
                INVST_NM
                금융투자      27    25     -2    266785  243320 -23465
                보험           0     0      0         0       0      0
                투신           0     0      0         0       0      0
                사모           0     0      0         0       0      0
                은행           0     0      0         0       0      0
                기타금융       0     0      0         0       0      0
                연기금 등      0     0      0         0       0      0
                기관합계      27    25     -2    266785  243320 -23465
                기타법인       0     0      0         0       0      0
                개인          25    27      2    243320  266785  23465
                외국인         0     0      0         0       0      0
                기타외국인     0     0      0         0       0      0
                전체          52    52      0    510105  510105      0
    """  # pylint: disable=line-too-long # noqa: E501
    if isinstance(fromdate, datetime.datetime):
        fromdate = krx.datetime2string(fromdate)

    if isinstance(todate, datetime.datetime):
        todate = krx.datetime2string(todate)

    return krx.get_indivisual_trading_volume_and_value_by_investor(
                fromdate, todate, ticker)


@dispatch(str, str, str, str)
def get_etf_trading_volume_and_value(fromdate: str, todate: str, # pylint: disable=function-redefined  # noqa
                                     query_type1: str,
                                     query_type2: str) -> DataFrame:
    """주어진 기간의 일자별 거래 실적 조회

    Args:
        fromdate        (str): 조회 시작 일자 (YYMMDD)
        todate          (str): 조회 종료 일자 (YYMMDD)
        query_type1     (str): 거래대금 / 거래량
        query_type2     (str): 순매수 / 매수 / 매도

    Returns:
        DataFrame:

            > get_etf_trading_volume_and_value("20220415", "20220422", "거래대금", "순매수")

                                기관    기타법인         개인        외국인 전체
            날짜
            2022-04-15   25346770535  -138921500  17104310255  -42312159290    0
            2022-04-18 -168362290065  -871791310  88115812520   81118268855    0
            2022-04-19  -36298873785  7555666910  -1968998025   30712204900    0
            2022-04-20 -235935697655  8965445880  19247888605  207722363170    0
            2022-04-21  -33385835805  2835764290  35920390975   -5370319460    0
            2022-04-22  -10628831870  2032673735  39477777530  -30881619395    0
    """  # pylint: disable=line-too-long # noqa: E501
    if isinstance(fromdate, datetime.datetime):
        fromdate = krx.datetime2string(fromdate)

    if isinstance(todate, datetime.datetime):
        todate = krx.datetime2string(todate)

    return krx.get_trading_volume_and_value_by_date(
        fromdate, todate, query_type1, query_type2)


@dispatch(str, str, str, str, str)
def get_etf_trading_volume_and_value(fromdate: str, todate: str, ticker: str, # pylint: disable=function-redefined  # noqa
                                     query_type1: str,
                                     query_type2: str) -> DataFrame:
    """주어진 기간 동안 개별 종목의 일자별 거래 실적 조회

    Args:
        fromdate        (str): 조회 시작 일자 (YYMMDD)
        todate          (str): 조회 종료 일자 (YYMMDD)
        query_type1     (str): 거래대금 / 거래량
        query_type2     (str): 순매수 / 매수 / 매도
        ticker          (str): 조회 종목의 티커

    Returns:
        DataFrame:

            > get_etf_trading_volume_and_value("20220908", "20220916", "거래대금", "순매수", "580011")

                             기관  기타법인     개인  외국인  전체
                날짜
                2022-09-08  -3570         0     3570       0     0
                2022-09-13 -10205         0    10205       0     0
                2022-09-14    -65         0       65       0     0
                2022-09-15    -65         0       65       0     0
                2022-09-16  -9560         0     9560       0     0
    """  # pylint: disable=line-too-long # noqa: E501
    if isinstance(fromdate, datetime.datetime):
        fromdate = krx.datetime2string(fromdate)

    if isinstance(todate, datetime.datetime):
        todate = krx.datetime2string(todate)

    return krx.get_indivisual_trading_volume_and_value_by_date(
        fromdate, todate, ticker, query_type1, query_type2)


# def get_etn_trading_volume_and_value(*args, **kwargs):
#     """ETN 거래량과 거래대금 조회
#     """
#     return _get_trading_volume_and_value(args, kwargs)

# def get_elw_trading_volume_and_value(*args, **kwargs):
#     """ELW 거래량과 거래대금 조회
#     """
#     return _get_trading_volume_and_value(args, kwargs)


def get_stock_major_changes(ticker: str) -> DataFrame:
    """기업 주요 변동사항

    Args:
        ticker   (str): 조회 종목 티커

    Returns:

        DataFrame:

            >> get_stock_major_changes("005930")

                            상호변경전     상호변경후    업종변경전   업종변경후  액면변경전 액면변경후                             대표이사변경전                                            대표이사변경후
            날짜
            1975-06-11        -        삼성전자공업주         -           -           0          0                                     -                                                          -
            1979-03-13        -               -               -           -        1000        500                                     -                                                          -
            1984-03-23  삼성전자공업주     삼성전자주         -           -           0          0                                     -                                                          -
            1987-01-05        -               -               -           -         500       5000                                     -                                                          -
            2000-01-20        -               -               -           -           0          0                    이건희윤종용이윤우이학수진대제문병대                        이건희윤종용이윤우이학수진대제최도석이상현임형규
            2001-03-21        -               -               -           -           0          0    이건희윤종용이윤우이학수진대제최도석이상현임형규이기태이상완황창규한용외           이건희윤종용이학수이윤우진
    """  # pylint: disable=line-too-long # noqa: E501

    return krx.get_stock_major_changes(ticker)


if __name__ == "__main__":
    pd.set_option('display.expand_frame_repr', False)
    # print(get_market_price_change_by_ticker(
    #       fromdate="20210101", todate="20210111"))
    # print(get_etf_ohlcv_by_ticker("20210321"))
    # print(get_market_ohlcv_by_date("19991220", "20191231", "008480"))
    # print(get_market_cap_by_ticker("20210101"))
    # print(get_market_ohlcv("20150720", "20150810", "005930", adjusted=False))
    # print(get_market_ohlcv("20210122"))
    # print(get_market_price_change("20210101", "20210108"))
    # df = get_stock_major_changes("005930")
    # st_date = "20220331"
    # stock_code = "017810"
    # df = get_market_trading_volume_by_investor(st_date, st_date, stock_code)
    # df = get_shorting_volume_by_ticker("20220605")

    # df = get_market_sector_classifications("20220902", "KOSPI")
    #
    # df = get_market_cap("20210104", "20210108", "005930")
    # print(df)
    # df = get_etf_trading_volume_and_value("20220908", "20220916")
    # df = get_etf_trading_volume_and_value("20220908", "20220916", "069500")
    # df = get_etf_trading_volume_and_value("20220908", "20220916", "069500", "거래대금", "순매수")
    # df = get_etf_trading_volume_and_value("20220908", "20220916", "거래대금", "순매수")
    # df = get_etf_trading_volume_and_value("20220908", "20220916", "069500")
    # print(df.head())

    start = "20000101"
    end = "20230512"
    ticker = "005930"

    # df = get_market_ohlcv(start, end, ticker, adjusted=True)
    # df = get_market_ohlcv_by_date("20210118", "20210118", "005930")
    # df = get_index_ohlcv("20180101", "20210130", "1001")
    df = get_market_ohlcv('20100101', '20230731', '005930', adjusted=True)

    print(df)
