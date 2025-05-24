from pykrx.website.comm import dataframe_empty_handler
from pykrx.website.krx.market.ticker import get_stock_ticker_isin
from pykrx.website.krx.market.core import (
    개별종목시세, 전종목등락률, PER_PBR_배당수익률_전종목,
    PER_PBR_배당수익률_개별, 전종목시세, 외국인보유량_개별추이,
    외국인보유량_전종목, 투자자별_순매수상위종목,
    투자자별_거래실적_개별종목_기간합계,
    투자자별_거래실적_개별종목_일별추이_일반,
    투자자별_거래실적_개별종목_일별추이_상세,
    투자자별_거래실적_전체시장_기간합계, 업종분류현황,
    투자자별_거래실적_전체시장_일별추이_일반, 개별종목_공매도_거래_전종목,
    투자자별_거래실적_전체시장_일별추이_상세, 개별종목_공매도_종합정보,
    개별종목_공매도_거래_개별추이, 투자자별_공매도_거래, 전종목_공매도_잔고,
    개별종목_공매도_잔고, 공매도_거래상위_50종목, 공매도_잔고상위_50종목,
    전체지수기본정보, 개별지수시세, 전체지수등락률, 전체지수시세, 지수구성종목,
    PER_PBR_배당수익률_전지수, PER_PBR_배당수익률_개별지수, 기업주요변동사항
)

import numpy as np
import pandas as pd
from pandas import Series, DataFrame


# -----------------------------------------------------------------------------
# stock
@dataframe_empty_handler
def get_market_ohlcv_by_date(fromdate: str, todate: str, ticker: str,
                             adjusted: bool = True) -> DataFrame:
    """일자별로 정렬된 특정 종목의 OHLCV

    Args:
        fromdate    (str): 조회 시작 일자 (YYYYMMDD)
        todate      (str): 조회 종료 일자 (YYYYMMDD)
        ticker      (str): 조회 종목의 ticker
        adjusted    (bool, optional): 수정 종가 여부 (True/False)

    Returns:
        DataFrame:

        >> get_market_ohlcv_by_date("20150720", "20150810", "005930")

                           시가     고가     저가     종가  거래량      거래대금    등락률
            날짜
            2015-07-20  1291000  1304000  1273000  1275000  128928  165366199000 -2.300781
            2015-07-21  1275000  1277000  1247000  1263000  194055  244129106000 -0.939941
            2015-07-22  1244000  1260000  1235000  1253000  268323  333813094000 -0.790039
            2015-07-23  1244000  1253000  1234000  1234000  208965  259446564000 -1.519531
    """  # pylint: disable=line-too-long # noqa: E501

    isin = get_stock_ticker_isin(ticker)
    adjusted = 2 if adjusted else 1
    df = 개별종목시세().fetch(fromdate, todate, isin, adjusted)

    df = df[['TRD_DD', 'TDD_OPNPRC', 'TDD_HGPRC', 'TDD_LWPRC', 'TDD_CLSPRC',
             'ACC_TRDVOL', 'ACC_TRDVAL', 'FLUC_RT']]
    df.columns = ['날짜', '시가', '고가', '저가', '종가', '거래량', '거래대금',
                  '등락률']
    df = df.set_index('날짜')
    df.index = pd.to_datetime(df.index, format='%Y/%m/%d')
    df = df.replace(r'[^-\w\.]', '', regex=True)
    df = df.replace(r'\-$', '0', regex=True)
    df = df.replace('', '0')
    df = df.astype({"시가": np.int32, "고가": np.int32, "저가": np.int32,
                    "종가": np.int32, "거래량": np.int32, "거래대금": np.int64,
                    "등락률": np.float32})
    return df.sort_index()


@dataframe_empty_handler
def get_market_ohlcv_by_ticker(date: str, market: str = "KOSPI") -> DataFrame:
    """티커별로 정리된 전종목 OHLCV

    Args:
        date   (str): 조회 일자 (YYYYMMDD)
        market (str): 조회 시장 (KOSPI/KOSDAQ/KONEX/ALL)

    Returns:
        DataFrame:
                     시가   고가   저가   종가  거래량    거래대금
            티커
            060310   2150   2390   2150   2190  981348  2209370985
            095570   3135   3200   3100   3130   89871   282007385
            006840  17050  17200  16500  16500   30567   512403000
            054620   8550   8740   8400   8650  647596  5525789290
            265520  22150  23100  22050  22400  255846  5798313650
    """

    market2mktid = {
        "ALL": "ALL",
        "KOSPI": "STK",
        "KOSDAQ": "KSQ",
        "KONEX": "KNX"
    }

    df = 전종목시세().fetch(date, market2mktid[market])
    df = df[['ISU_SRT_CD', 'TDD_OPNPRC', 'TDD_HGPRC', 'TDD_LWPRC',
             'TDD_CLSPRC', 'ACC_TRDVOL', 'ACC_TRDVAL', 'FLUC_RT', 'MKTCAP']]
    df.columns = ['티커', '시가', '고가', '저가', '종가', '거래량', '거래대금',
                  '등락률', '시가총액']
    df = df.replace(r'[^-\w\.]', '', regex=True)
    df = df.replace(r'\-$', '0', regex=True)
    df = df.replace('', '0')
    df = df.set_index('티커')
    df = df.astype({
        "시가": np.int32,
        "고가": np.int32,
        "저가": np.int32,
        "종가": np.int32,
        "거래량": np.int32,
        "거래대금": np.int64,
        "등락률": np.float32,
        "시가총액": np.int64
    })
    return df


@dataframe_empty_handler
def get_market_cap_by_date(fromdate: str, todate: str, ticker: str,
                           adjusted: bool = True) -> DataFrame:
    """일자별로 정렬된 시가총액

    Args:
        fromdate (str): 조회 시작 일자 (YYYYMMDD)
        todate   (str): 조회 종료 일자 (YYYYMMDD)
        ticker   (str): 티커

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

    isin = get_stock_ticker_isin(ticker)
    adjusted = 2 if adjusted else 1
    df = 개별종목시세().fetch(fromdate, todate, isin, adjusted)
    df = df[['TRD_DD', 'MKTCAP', 'ACC_TRDVOL', 'ACC_TRDVAL', 'LIST_SHRS']]
    df.columns = ['날짜', '시가총액', '거래량', '거래대금', '상장주식수']

    df = df.replace('/', '', regex=True)
    df = df.replace(',', '', regex=True)
    df = df.set_index('날짜')
    df = df.astype(np.int64)
    df.index = pd.to_datetime(df.index, format='%Y%m%d')
    return df.sort_index()


@dataframe_empty_handler
def get_market_cap_by_ticker(date: str, market: str = "KOSPI",
                             ascending: bool = False) -> DataFrame:
    """티커별로 정렬된 시가총액

    Args:
        date                 (str): 조회 일자 (YYYYMMDD)
        market     (str, optional): 조회 시장 (KOSPI/KOSDAQ/ALL).
                                    Defaults to KOSPI.
        ascending (bool, optional): 정렬 기준

    Returns:
        DataFrame :
                      종가         시가총액    거래량         거래대금  상장주식수
            티커
            005930   51900  309831714345000  18541624  309831714345000  5969782550
            000660   84300   61370599369500   3397112   61370599369500   728002365
            207940  815000   53924475000000    163339   53924475000000    66165000
            035420  269500   44268984952500   1196267   44268984952500   164263395
            068270  316000   42640845660000    918369   42640845660000   134939385
    """  # pylint: disable=line-too-long # noqa: E501

    market2mktid = {
        "ALL": "ALL",
        "KOSPI": "STK",
        "KOSDAQ": "KSQ",
        "KONEX": "KNX"
    }

    df = 전종목시세().fetch(date, market2mktid[market])
    df = df[['ISU_SRT_CD', 'TDD_CLSPRC', 'MKTCAP', 'ACC_TRDVOL', 'ACC_TRDVAL',
             'LIST_SHRS']]
    df.columns = ['티커', '종가', '시가총액', '거래량', '거래대금',
                  '상장주식수']

    df = df.set_index('티커')
    df = df.replace(r'\W', '', regex=True)
    df = df.replace('', 0)
    df = df.astype(np.int64)
    return df.sort_values('시가총액', ascending=ascending)


@dataframe_empty_handler
def get_market_fundamental_by_ticker(date: str, market: str = "KOSPI") \
        -> DataFrame:
    """티커별로 정리된 특정 일자의 BPS/PER/PBR/배당수익률

    Args:
        date (str): 조회 일자 (YYYYMMDD)
        market (str, optional): 조회 시장 (KOSPI/KOSDAQ/ALL)

    Returns:
        DataFrame:
                         종목명    BPS    PER   PBR    EPS   DIV  DPS
            티커
            060310          3S     704   0.00  3.38      0  0.00    0
            095570  AJ네트웍스    6168  15.03  0.78    320  1.79   86
            068400    AJ렌터카   11091  20.45  1.05    572  0.00    0
            006840    AK홀딩스   52954   6.78  0.99   7727  1.24  650
            054620   APS홀딩스   13639   0.10  0.32  46508  0.00    0
    """

    market2mktid = {
        "ALL": "ALL",
        "KOSPI": "STK",
        "KOSDAQ": "KSQ",
        "KONEX": "KNX"
    }
    df = PER_PBR_배당수익률_전종목().fetch(date, market2mktid[market])

    df = df[['ISU_SRT_CD', 'BPS', 'PER', 'PBR', 'EPS', 'DVD_YLD', 'DPS']]
    df.columns = ['티커', 'BPS', 'PER', 'PBR', 'EPS', 'DIV', 'DPS']
    df.set_index('티커', inplace=True)

    df = df.replace(r'\-$', '0', regex=True)
    df = df.replace('', '0', regex=True)
    df = df.replace(',', '', regex=True)
    df = df.astype({
        "BPS": np.int32,
        "PER": np.float64,
        "PBR": np.float64,
        "EPS": np.int32,
        "DIV": np.float64,
        "DPS": np.int32
    })
    return df


@dataframe_empty_handler
def get_market_fundamental_by_date(fromdate: str, todate: str, ticker: str) \
        -> DataFrame:
    """날짜로 정렬된 종목별 BPS/PER/PBR/배당수익률

    Args:
        fromdate (str          ): 조회 시작 일자 (YYYYMMDD)
        todate   (str          ): 조회 종료 일자 (YYYYMMDD)
        ticker   (str          ): 종목의 티커

    Returns:
        DataFrame:
                           BPS       PER       PBR     EPS       DIV    DPS
            날짜
            2015-07-20  953266  8.328125  1.339844  153105  1.570312  20000
            2015-07-21  953266  8.250000  1.320312  153105  1.580078  20000
            2015-07-22  953266  8.179688  1.309570  153105  1.599609  20000
            2015-07-23  953266  8.062500  1.290039  153105  1.620117  20000
            2015-07-24  953266  8.031250  1.290039  153105  1.629883  20000
    """

    isin = get_stock_ticker_isin(ticker)
    # market = get_stock_ticekr_market(ticker)

    df = PER_PBR_배당수익률_개별().fetch(fromdate, todate, "ALL", isin)

    df = df[['TRD_DD', 'BPS', 'PER', 'PBR', 'EPS', 'DVD_YLD', 'DPS']]
    df.columns = ['날짜', 'BPS', 'PER', 'PBR', 'EPS', 'DIV', 'DPS']

    df = df.replace(r'\-$', '0', regex=True)
    df = df.replace('/', '', regex=True)
    df = df.replace('', '0', regex=True)
    df = df.replace(',', '', regex=True)
    df = df.astype({"BPS": np.int32, "PER": np.float64,
                    "PBR": np.float32, "EPS": np.int32, "DIV": np.float32,
                    "DPS": np.int32}, )
    df = df.set_index('날짜')
    df.index = pd.to_datetime(df.index, format='%Y%m%d')
    return df.sort_index()


@dataframe_empty_handler
def get_market_ticker_and_name(date: str, market: str = "KOSPI") -> Series:
    """티커이름 (index), 종목명 (value)으로 구성된 시리즈 반환

    Note:
        KRX가 20000101 이후의 데이터만을 제공

    Args:
        date   (str): 조회 일자 (YYYYMMDD)
        market (str): 조회 시장 (KOSPI/KOSDAQ/ALL)

    Returns:
        Series:
            095570    AJ네트웍스
            068400     AJ렌터카
            006840     AK홀딩스
            027410       BGF
            282330    BGF리테일
    """

    market2code = {
        "ALL": "ALL",
        "KOSPI": "STK",
        "KOSDAQ": "KSQ",
        "KONEX": "KNX"
    }

    df = 전종목시세().fetch(date, market2code[market])
    df = df[['ISU_SRT_CD', 'ISU_ABBRV']]
    df.columns = ['티커', '종목명']
    df = df.set_index('티커')
    return df['종목명']


@dataframe_empty_handler
def get_market_price_change_by_ticker(fromdate: str, todate: str,
                                      market: str = "KOSPI",
                                      adjusted: bool = True) -> DataFrame:
    """입력된 기간동안의 전 종목 수익률 반환

    Args:
        fromdate (str           ): 조회 시작 일자 (YYYYMMDD)
        todate   (str           ): 조회 종료 일자 (YYYYMMDD)
        market   (str , optional): 조회 시장 (KOSPI/KOSDAQ/ALL)
        adjusted (book, optional): 수정 종가 여부 (True/False)

    Returns:
        DataFrame:
    """
    market2mktid = {
        "ALL": "ALL",
        "KOSPI": "STK",
        "KOSDAQ": "KSQ",
        "KONEX": "KNX"
    }

    adjusted = 2 if adjusted else 1

    df = 전종목등락률().fetch(fromdate, todate, market2mktid[market], adjusted)
    df = df[['ISU_ABBRV', 'ISU_SRT_CD', 'BAS_PRC', 'TDD_CLSPRC',
             'CMPPREVDD_PRC', 'FLUC_RT', 'ACC_TRDVOL', 'ACC_TRDVAL']]
    df.columns = ['종목명', '티커', '시가', '종가', '변동폭',
                  '등락률', '거래량', '거래대금']
    df = df.set_index('티커')

    df = df.replace(r'[^-.\w]', '', regex=True)
    df = df.replace(r'\-$', '', regex=True)
    df = df.replace('', '0')
    df = df.astype({
        "시가": np.int32,
        "종가": np.int32,
        "변동폭": np.int32,
        "등락률": np.float64,
        "거래량": np.int64,
        "거래대금": np.int64
    })
    return df


def get_exhaustion_rates_of_foreign_investment_by_date(
        fromdate: str, todate: str, ticker: str) -> DataFrame:
    """[12023] 외국인보유량(개별종목) - 개별추이

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

    isin = get_stock_ticker_isin(ticker)

    df = 외국인보유량_개별추이().fetch(fromdate, todate, isin)

    df = df[['TRD_DD', 'LIST_SHRS', 'FORN_HD_QTY', 'FORN_SHR_RT',
             'FORN_ORD_LMT_QTY', 'FORN_LMT_EXHST_RT']]
    df.columns = ['날짜', '상장주식수', '보유수량', '지분율', '한도수량',
                  '한도소진률']

    df = df.replace('/', '', regex=True)
    df = df.replace('', '0', regex=True)
    df = df.replace(',', '', regex=True)
    df = df.astype({
        "상장주식수": np.int64,
        "보유수량": np.int64,
        "지분율": np.float16,
        "한도수량": np.int64,
        "한도소진률": np.float16
    })
    df = df.set_index('날짜')
    df.index = pd.to_datetime(df.index, format='%Y%m%d')
    return df.sort_index()


def get_exhaustion_rates_of_foreign_investment_by_ticker(
        date: str, market: str, balance_limit: bool) -> DataFrame:
    """[12023] 외국인보유량(개별종목) - 전종목

    Args:
        date          (str ): 조회 일자 (YYYYMMDD)
        market        (str ): 조회 시장 (KOSPI/KOSDAQ/ALL)
        balance_limit (bool): 외국인보유제한종목
            - 0 : check X
            - 1 : check O

    Returns:
        DataFrame:
                    상장주식수    보유수량     지분율    한도수량  한도소진률
            티커
            003490   94844634   12350096  13.023438   47412833  26.046875
            003495    1110794      29061   2.619141     555286   5.230469
            015760  641964077  127919592  19.937500  256785631  49.812500
            017670   80745711   28962369  35.875000   39565398  73.187500
            020560  223235294   13871465   6.210938  111595323  12.429688
    """

    market2mktid = {
        "ALL": "ALL",
        "KOSPI": "STK",
        "KOSDAQ": "KSQ",
        "KONEX": "KNX"
    }

    balance_limit = 1 if balance_limit else 0
    df = 외국인보유량_전종목().fetch(date, market2mktid[market], balance_limit)

    df = df[['ISU_SRT_CD', 'LIST_SHRS', 'FORN_HD_QTY', 'FORN_SHR_RT',
             'FORN_ORD_LMT_QTY', 'FORN_LMT_EXHST_RT']]
    df.columns = ['티커', '상장주식수', '보유수량', '지분율', '한도수량',
                  '한도소진률']
    df = df.replace('', '0', regex=True)
    df = df.replace(',', '', regex=True)
    df = df.astype({
        "상장주식수": np.int64,
        "보유수량": np.int64,
        "지분율": np.float16,
        "한도수량": np.int64,
        "한도소진률": np.float16
    })
    df = df.set_index('티커')
    return df.sort_index()


@dataframe_empty_handler
def get_market_trading_value_and_volume_on_ticker_by_investor(
        fromdate: str, todate: str, ticker: str) -> DataFrame:
    """[12009] 투자자별 거래실적 기간합계(개별 종목)

    다음 메뉴의 내용을 스크래핑 함

    거래실적
         ㄴ 투자자별 거래실적(개별종목)
            http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=
            MDC0201020302

    Args:
        fromdate (str ): 조회 시작 일자 (YYMMDD)
        todate   (str ): 조회 종료 일자 (YYMMDD)
        ticker   (str ): 조회 종목 티커

    Returns:
        DataFrame:

            >> get_market_trading_value_and_volume_on_ticker_by_investor(
                "20210113", "20210120", "005930")

                         거래량                             거래대금
                           매도       매수    순매수            매도            매수         순매수
            투자자구분
            금융투자   31324444   28513421   2811023   2765702311200   2510494630400   255207680800
            보험        1790469     561307   1229162    158120209600     49570523900   108549685700
            투신        3966211    1486178   2480033    351753222200    130513380300   221239841900
            사모         756726     541912    214814     67202238800     47475872700    19726366100
            은행         105323      70598     34725      9360874400      6170507400     3190367000
    """  # pylint: disable=line-too-long # noqa: E501

    isin = get_stock_ticker_isin(ticker)
    df = 투자자별_거래실적_개별종목_기간합계().fetch(fromdate, todate, isin)

    df = df.set_index('INVST_TP_NM')
    df.index.name = '투자자구분'
    df.columns = pd.MultiIndex.from_product([['거래량', '거래대금'],
                                             ['매도', '매수', '순매수']])
    df = df.replace(r'[^-\w]', '', regex=True)
    df = df.replace('', '0')
    return df.astype(np.int64)


@dataframe_empty_handler
def get_market_trading_value_and_volume_on_market_by_investor(
        fromdate: str, todate: str, market: str, etf: bool = True,
        etn: bool = True, elw: bool = True) -> DataFrame:
    """[12008] 투자자별 거래실적 기간합계

    다음 메뉴의 내용을 스크래핑 함

    거래실적
         ㄴ 투자자별 거래실적
            http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=
            MDC0201020301

    Args:
        fromdate    (str ): 조회 시작 일자 (YYMMDD)
        todate      (str ): 조회 종료 일자 (YYMMDD)
        market      (str ): 조회 시장 (KOSPI/KOSDAQ/KONEX/ALL)
        etf         (bool): 시장 포함 여부
        etn         (bool): 시장 포함 여부
        elw         (bool): 시장 포함 여부

    Returns:
        DataFrame:

            >> get_market_trading_value_and_volume_on_market_by_investor(
                "20210115", "20210122", "KOSPI", True, True)

                            거래량                                거래대금
                              매도         매수     순매수            매도             매수         순매수
            투자자구분
            금융투자    1857447354   1660620713 -196826641  15985568261831   15006116511544  -979451750287
            보험          29594468     19872165   -9722303   1219035502445     757575677208  -461459825237
            투신          69348190     60601427   -8746763   2235561259511    1799363743367  -436197516144
            사모          31673292     26585281   -5088011    999084910863     846067212945  -153017697918
            은행          44279242     51690814    7411572    886226324790     936210985810    49984661020


            >> get_market_trading_value_and_volume_on_market_by_investor(
                "20210115", "20210122", "KOSPI", True, True, True)

                            거래량                                거래대금
                              매도         매수     순매수            매도             매수         순매수
            투자자구분
            금융투자    1857447354   1660620713 -196826641  15985568261831   15006116511544  -979451750287
            보험          29594468     19872165   -9722303   1219035502445     757575677208  -461459825237
            투신          69348190     60601427   -8746763   2235561259511    1799363743367  -436197516144
            사모          31673292     26585281   -5088011    999084910863     846067212945  -153017697918
            은행          44279242     51690814    7411572    886226324790     936210985810    49984661020
    """  # pylint: disable=line-too-long # noqa: E501

    etf = "EF" if etf else ""
    etn = "EN" if etn else ""
    elw = "EW" if elw else ""

    market2mktid = {
        "ALL": "ALL",
        "KOSPI": "STK",
        "KOSDAQ": "KSQ",
        "KONEX": "KNX"
    }

    df = 투자자별_거래실적_전체시장_기간합계().fetch(
        fromdate, todate, market2mktid[market], etf, etn, elw)

    df = df.set_index('INVST_TP_NM')
    df.index.name = '투자자구분'
    df.columns = pd.MultiIndex.from_product([['거래량', '거래대금'],
                                             ['매도', '매수', '순매수']])
    df = df.replace(r'[^-\w]', '', regex=True)
    df = df.replace('', '0')
    return df.astype(np.int64)


@dataframe_empty_handler
def get_market_trading_value_and_volume_on_market_by_date(
        fromdate: str, todate: str, market: str, etf: bool, etn: bool,
        elw: bool, option_a: str, option_b: str, detail_view: bool) \
        -> DataFrame:
    """[12008] 투자자별 거래실적

    Args:
        fromdate    (str ): 조회 시작 일자 (YYMMDD)
        todate      (str ): 조회 종료 일자 (YYMMDD)
        market      (str ): 조회 시장 (KOSPI/KOSDAQ/KONEX/ALL)
        etf         (bool): 시장 포함 여부
        etn         (bool): 시장 포함 여부
        elw         (bool): 시장 포함 여부
        option_a    (str ): 일별 추이 옵션 1 (거래량/거래대금)
        option_b    (str ): 일별 추이 옵션 2 (매수/매도/순매수)
        detail_view (bool): 상세조회 여부

    Returns:
        DataFrame:

                       TRD_DD     TRDVAL1     TRDVAL2        TRDVAL3      TRDVAL4     TRDVAL_TOT
                0  2021/01/22  67,656,491   6,020,990    927,119,399  110,426,104  1,111,222,984
                1  2021/01/21  69,180,642  13,051,423  1,168,810,381  109,023,034  1,360,065,480
    """  # pylint: disable=line-too-long # noqa: E501

    etf = "EF" if etf else ""
    etn = "EN" if etn else ""
    elw = "EW" if elw else ""
    option_a = {"거래량": 1, "거래대금": 2}.get(option_a, 1)
    option_b = {"매도": 1, "매수": 2, "순매수": 3}.get(option_b, 3)

    market2mktid = {
        "ALL": "ALL",
        "KOSPI": "STK",
        "KOSDAQ": "KSQ",
        "KONEX": "KNX"
    }

    if detail_view:
        df = 투자자별_거래실적_전체시장_일별추이_상세().fetch(
            fromdate, todate, market2mktid[market], etf, etn, elw, option_a,
            option_b)
        df.columns = ['날짜', '금융투자', '보험', '투신', '사모', '은행',
                      '기타금융', '연기금', '기타법인', '개인', '외국인',
                      '기타외국인', '전체']
    else:
        df = 투자자별_거래실적_전체시장_일별추이_일반().fetch(
            fromdate, todate, market2mktid[market], etf, etn, elw, option_a,
            option_b)
        df.columns = ['날짜', '기관합계', '기타법인', '개인', '외국인합계',
                      '전체']

    df = df.set_index('날짜')
    df.index = pd.to_datetime(df.index, format='%Y/%m/%d')
    df = df.replace(r'[^-\w]', '', regex=True)
    df = df.replace('', '0')
    df = df.astype(np.int64)
    return df.sort_index()


@dataframe_empty_handler
def get_market_trading_value_and_volume_on_ticker_by_date(
        fromdate: str, todate: str, ticker: str, option_a: str, option_b: str,
        detail_view: bool) -> DataFrame:
    """[12008] 투자자별 거래실적

    Args:
        fromdate    (str ): 조회 시작 일자 (YYMMDD)
        todate      (str ): 조회 종료 일자 (YYMMDD)
        ticker      (str ): 조회 종목 티커
        option_a    (str ): 일별 추이 옵션 1 (거래량/거래대금)
        option_b    (str ): 일별 추이 옵션 2 (매수/매도/순매수)
        detail_view (bool): 상세조회 여부

    Returns:
        DataFrame:

                       TRD_DD     TRDVAL1     TRDVAL2        TRDVAL3      TRDVAL4     TRDVAL_TOT
                0  2021/01/22  67,656,491   6,020,990    927,119,399  110,426,104  1,111,222,984
                1  2021/01/21  69,180,642  13,051,423  1,168,810,381  109,023,034  1,360,065,480
    """  # pylint: disable=line-too-long # noqa: E501

    isin = get_stock_ticker_isin(ticker)

    option_a = {"거래량": 1, "거래대금": 2}.get(option_a, 1)
    option_b = {"매도": 1, "매수": 2, "순매수": 3}.get(option_b, 3)

    if detail_view:
        df = 투자자별_거래실적_개별종목_일별추이_상세().fetch(
            fromdate, todate, isin, option_a, option_b)
        df.columns = ['날짜', '금융투자', '보험', '투신', '사모', '은행',
                      '기타금융', '연기금', '기타법인', '개인', '외국인',
                      '기타외국인', '전체']
    else:
        df = 투자자별_거래실적_개별종목_일별추이_일반().fetch(
            fromdate, todate, isin, option_a, option_b)
        df.columns = ['날짜', '기관합계', '기타법인', '개인', '외국인합계',
                      '전체']

    df = df.set_index('날짜')
    df.index = pd.to_datetime(df.index, format='%Y/%m/%d')
    df = df.replace(r'[^-\w]', '', regex=True)
    df = df.replace('', '0')
    df = df.astype(np.int64)
    return df.sort_index()


@dataframe_empty_handler
def get_market_net_purchases_of_equities_by_ticker(
        fromdate: str, todate: str, market: str, investor: str) -> DataFrame:
    """[12010] 투자자별 순매수상위종목

    Args:
        fromdate (str): 조회 시작 일자 (YYYYMMDD)
        todate   (str): 조회 종료 일자 (YYYYMMDD)
        market   (str): 조회 시장 (KOSPI/KOSDAQ/KONEX/ALL)
        investor (str): 투자자
             - 1000 - 금융투자
             - 2000 - 보험
             - 3000 - 투신
             - 3100 - 사모
             - 4000 - 은행
             - 5000 - 기타금융
             - 6000 - 연기금
             - 7050 - 기관합계
             - 7100 - 기타법인
             - 8000 - 개인
             - 9000 - 외국인
             - 9001 - 기타외국인
             - 9999 - 전체

    Returns:
        DataFrame:
                       종목명  매도거래량  매수거래량  순매수거래량  매도거래대금  매수거래대금  순매수거래대금
            티커
            034730         SK     1581633     1767494        185861  448072973000  511094137000     63021164000
            010130   고려아연      188718      296707        107989   79480106000  126281029000     46800923000
            039490   키움증권      374940      715079        340139   53685623500   99770954000     46085330500
            011070   LG이노텍      743878      929876        185998  137990915000  173082664000     35091749000
            352820     빅히트      247298      442325        195027   39722470000   73131351500     33408881500
    """  # pylint: disable=line-too-long # noqa: E501

    market2mktid = {
        "ALL": "ALL",
        "KOSPI": "STK",
        "KOSDAQ": "KSQ",
        "KONEX": "KNX"
    }

    investor2invstTpCd = {
        "금융투자": 1000,
        "보험": 2000,
        "투신": 3000,
        "사모": 3100,
        "은행": 4000,
        "기타금융": 5000,
        "연기금": 6000,
        "기관합계": 7050,
        "기타법인": 7100,
        "개인": 8000,
        "외국인": 9000,
        "기타외국인": 9001,
        "전체": 9999
    }

    df = 투자자별_순매수상위종목().fetch(
        fromdate, todate, market2mktid[market], investor2invstTpCd[investor])

    df.columns = ['티커', '종목명', '매도거래량', '매수거래량', '순매수거래량',
                  '매도거래대금', '매수거래대금', '순매수거래대금']
    df = df.replace('/', '', regex=True)
    df = df.replace(',', '', regex=True)
    df = df.astype({
        '티커': str, '종목명': str, '매수거래량': np.int64,
        '매도거래량': np.int64, '순매수거래량': np.int64,
        '매수거래대금': np.int64, '매도거래대금': np.int64,
        '순매수거래대금': np.int64
    })
    df['티커'] = df['티커'].apply(lambda x: x.zfill(6))
    return df.set_index('티커')


@dataframe_empty_handler
def get_market_sector_classifications(date: str, market: str) -> DataFrame:
    """[12025] 업종별 분류 현황

    Args:
        date    (str ): 조회 일자 (YYYYMMDD)
        market  (str ): 조회 시장 (KOSPI/KOSDAQ)

    Returns:
        DataFrame:

            > get_market_sector_classifications("20220902", "KOSPI")

                         종목명     업종명    종가    대비  등락률       시가총액
            종목코드
            095570   AJ네트웍스   서비스업    7280    80.0    1.11   340866307600
            006840     AK홀딩스   기타금융   15900   150.0    0.95   210636219900
            027410          BGF   기타금융    3990     0.0    0.00   381909996090
            282330    BGF리테일     유통업  156000 -1500.0   -0.95  2696289336000
            138930  BNK금융지주   기타금융    6560   -40.0   -0.61  2138135213760
    """  # pylint: disable=line-too-long # noqa: E501
    market2mktid = {
        "KOSPI": "STK",
        "KOSDAQ": "KSQ",
    }
    df = 업종분류현황().fetch(date, market2mktid[market])

    df = df[["ISU_SRT_CD", "ISU_ABBRV", "IDX_IND_NM", "TDD_CLSPRC",
             "CMPPREVDD_PRC", "FLUC_RT", "MKTCAP"]]
    df.columns = ["종목코드", "종목명", "업종명", "종가", "대비", "등락률",
                  "시가총액"]
    df = df.replace(r'\-$', '0', regex=True)
    df = df.replace('', '0', regex=True)
    df = df.replace(',', '', regex=True)
    df = df.astype({
        "종가": np.int32,
        "대비": np.float64,
        "등락률": np.float64,
        "시가총액": np.int64
    })
    return df.set_index("종목코드")


# -----------------------------------------------------------------------------
# index
@dataframe_empty_handler
def get_index_ohlcv_by_date(fromdate: str, todate: str, ticker: str) \
        -> DataFrame:
    """일자별 특정 지수의 OHLCV

    Args:
        fromdate (str): 조회 시작 일자 (YYYYMMDD)
        todate   (str): 조회 종료 일자 (YYYYMMDD)
        ticker   (str): 인덱스 티커

    Returns:
        DataFrame:
                              시가        고가        저가        종가     거래량       거래대금
            날짜
            2019-04-12  765.599976  769.090027  762.989990  767.849976  926529303  4342635071383
            2019-04-11  762.150024  766.559998  762.130005  766.489990  760873641  4329711935026
            2019-04-10  755.729980  760.150024  754.049988  760.150024  744528283  4692968086240
            2019-04-09  753.719971  756.919983  749.869995  756.809998  836573253  4701168278265
            2019-04-08  755.320007  756.159973  750.020020  751.919983  762374091  4321665707119
    """  # pylint: disable=line-too-long # noqa: E501

    df = 개별지수시세().fetch(ticker[1:], ticker[0], fromdate, todate)
    df = df[['TRD_DD', 'OPNPRC_IDX', 'HGPRC_IDX', 'LWPRC_IDX',
             'CLSPRC_IDX', 'ACC_TRDVOL', 'ACC_TRDVAL', 'MKTCAP']]
    df.columns = [
        '날짜', '시가', '고가', '저가', '종가', '거래량', '거래대금',
        '상장시가총액'
    ]

    df = df.replace(r'[^-\w\.]', '', regex=True)
    df = df.replace(r'\-$', '0', regex=True)
    df = df.replace('', '0')
    df = df.set_index('날짜')
    df = df.astype({
        '시가': np.float64,
        '고가': np.float64,
        '저가': np.float64,
        '종가': np.float64,
        '거래량': np.int64,
        '거래대금': np.int64,
        '상장시가총액': np.int64
    })
    df.index = pd.to_datetime(df.index, format='%Y%m%d')
    return df.sort_index()


@dataframe_empty_handler
def get_index_ohlcv_by_ticker(date: str, market: str = "KOSPI") -> DataFrame:
    """전종목 지수 OHLCV

    Args:
        fromdate (str         ): 조회 일자 (YYYYMMDD)
        계열구분 (str, optional): KRX/KOSPI/KOSDAQ/테마

    Returns:
        DataFrame:

            > get_index_ohlcv_by_date("20211126", "KOSPI")

                                    시가      고가      저가       종가     거래량         거래대금
            지수명
            코스피외국주포함         0.00      0.00      0.00      0.00  595597647   11901297731572
            코스피                2973.04   2985.77   2930.31   2936.44  594707257   11894910355357
            코스피200              390.61    392.81    384.19    385.07  145771166    8625603922656
            코스피100             2947.18   2963.27   2900.41   2906.68  100357121    7370285846691
            코스피50              2736.77   2752.70   2693.90   2700.81   52627040    5768837287881
    """  # pylint: disable=line-too-long # noqa: E501

    market2idx = {
        "KRX": "01",
        "KOSPI": "02",
        "KOSDAQ": "03",
        "테마": "04"
    }
    df = 전체지수시세().fetch(date, market2idx[market])
    df = df[['IDX_NM', 'OPNPRC_IDX', 'HGPRC_IDX', 'LWPRC_IDX',
             'CLSPRC_IDX', 'ACC_TRDVOL', 'ACC_TRDVAL', 'MKTCAP']]
    df.columns = ['지수명', '시가', '고가', '저가', '종가', '거래량',
                  '거래대금', '상장시가총액']
    df = df.replace(r'[^-\w\.]', '', regex=True)
    df = df.replace(r'\-$', '0', regex=True)
    df = df.replace('', '0')
    df = df.set_index('지수명')
    df = df.astype({
        '시가': np.float64,
        '고가': np.float64,
        '저가': np.float64,
        '종가': np.float64,
        '거래량': np.int64,
        '거래대금': np.int64,
        '상장시가총액': np.int64
    })
    return df


@dataframe_empty_handler
def get_index_listing_date(market: str = "KOSPI") -> DataFrame:
    """[11004] 전체지수 기본정보

    Args:
        계열구분 (str, optional): KRX/KOSPI/KOSDAQ/테마

    Returns:
        DataFrame:
                                   기준시점    발표시점   기준지수  종목수
            지수명
            코스피               1980.01.04  1983.01.04      100.0     796
            코스피 200           1990.01.03  1994.06.15      100.0     201
            코스피 100           2000.01.04  2000.03.02     1000.0     100
            코스피 50            2000.01.04  2000.03.02     1000.0      50
            코스피 200 중소형주  2010.01.04  2015.07.13     1000.0     101
    """

    market2idx = {
        "KRX": "01",
        "KOSPI": "02",
        "KOSDAQ": "03",
        "테마": "04"
    }
    df = 전체지수기본정보().fetch(market2idx[market])
    df = df[['IDX_NM', 'BAS_TM_CONTN', 'ANNC_TM_CONTN', 'BAS_IDX_CONTN',
             'COMPST_ISU_CNT']]
    df.columns = ['지수명', '기준시점', '발표시점', '기준지수', '종목수']
    df = df.set_index('지수명')
    df = df.replace(',', '', regex=True)
    df = df.replace('', 0)
    df = df.astype({
        "기준지수": np.float64,
        "종목수": np.int16
    })
    return df


@dataframe_empty_handler
def get_index_price_change_by_ticker(fromdate: str, todate: str, market: str) \
        -> DataFrame:
    """지정된 기간 동안의 전종목 OHLCV

    Args:
        fromdate (str): 조회 시작 일자 (YYYYMMDD)
        todate   (str): 조회 종료 일자 (YYYYMMDD)
        market   (str): 검색 시장 (KRX/KOSPI/KOSDAQ/테마)

    NOTE: KRX 웹 서버에 의한 제약사항으로 반드시 시작일과 종료일은 영업일이어야
          한다.

    Returns:
        DataFrame:
                                                   시가          종가    등락률       거래량        거래대금
            지수명
            코스닥                           696.500000    724.500000  4.050781  10488319776  62986196230829
            코스닥 150                      1065.000000   1103.000000  3.490234    729479528  18619100922088
            코스닥 150 정보기술              603.500000    631.500000  4.648438    268338653   5203201290465
            코스닥 150 헬스케어             3450.000000   3532.000000  2.369141    135927364   7874689575610
            코스닥 150 커뮤니케이션서비스   2037.000000   2090.000000  2.599609     25001250    816778277690
    """  # pylint: disable=line-too-long # noqa: E501

    market2idx = {
        "KRX": "01",
        "KOSPI": "02",
        "KOSDAQ": "03",
        "테마": "04"
    }
    df = 전체지수등락률().fetch(fromdate, todate, market2idx[market])
    df = df[['IDX_IND_NM', 'OPN_DD_INDX', 'END_DD_INDX', 'FLUC_RT',
             'ACC_TRDVOL', 'ACC_TRDVAL']]
    df.columns = ['지수명', '시가', '종가', '등락률', '거래량', '거래대금']
    df = df.set_index('지수명')
    df = df.replace(r'[^\w\.-]', '', regex=True)
    df = df.replace('', 0)
    df = df.replace('-', 0)
    df = df.astype({
        "시가": np.float64,
        "종가": np.float64,
        "등락률": np.float16,
        "거래량": np.int64,
        "거래대금": np.int64
    })
    return df


@dataframe_empty_handler
def get_index_fundamental_by_ticker(date: str, market: str = "KOSPI") \
        -> DataFrame:
    """[11004] 전체지수 기본정보

    Args
        date    (str          ): 조회 일자 (YYMMDD)
        계열구분 (str, optional): KRX/KOSPI/KOSDAQ/테마

    Returns:
        DataFrame:

            > get_index_fundamental_by_ticker("20211129", "KRX")

                          종가   등락률       PER 선행PER      PBR   배당수익률
            지수명
            KRX 300     1753.96  -0.92  13.609375     0.0  1.240234    2.009766
            KTOP 30    10348.84  -1.20  12.671875     0.0  1.219727    2.330078
            KRX 100     6045.16  -0.89  13.421875     0.0  1.219727    1.969727
            KRX 자동차  2030.72  -2.00  11.937500     0.0  0.790039    1.419922
            KRX 반도체  3649.78  -1.07  21.484375     0.0  2.589844    0.609863

    """

    market2idx = {
        "KRX": "01",
        "KOSPI": "02",
        "KOSDAQ": "03",
        "테마": "04"
    }
    df = PER_PBR_배당수익률_전지수().fetch(date, market2idx[market])
    df = df[['IDX_NM', 'CLSPRC_IDX', 'FLUC_RT', 'WT_PER', 'FWD_PER',
             'WT_STKPRC_NETASST_RTO', 'DIV_YD']]
    df.columns = ['지수명', '종가', '등락률', 'PER', '선행PER', 'PBR',
                  '배당수익률']
    df = df.set_index('지수명')
    df = df.replace('^-$', 0, regex=True)
    df = df.replace(',', '', regex=True)
    df = df.replace('', 0)
    df = df.astype({
        "종가": np.float64,
        "등락률": np.float64,
        "PER": np.float32,
        "선행PER": np.float32,
        "PBR": np.float32,
        "배당수익률": np.float32
    })
    return df


@dataframe_empty_handler
def get_index_fundamental_by_date(fromdate: str, todate: str, ticker: str) \
        -> DataFrame:
    """일자별 특정 지수의 OHLCV

    Args:
        fromdate (str): 조회 시작 일자 (YYYYMMDD)
        todate   (str): 조회 종료 일자 (YYYYMMDD)
        ticker   (str): 인덱스 티커

    Returns:
        DataFrame:

            > get_index_fundamental_by_date("20211122", "20211129", "5300")

                           종가   등락률     PER  선행PER   PBR  배당수익률
            날짜
            2021-11-22  1832.66    1.92    14.22      0.0  1.30        1.92
            2021-11-23  1817.75   -0.81    14.10      0.0  1.29        1.94
            2021-11-24  1815.36   -0.13    14.08      0.0  1.29        1.94
            2021-11-25  1799.26   -0.89    13.96      0.0  1.28        1.96
            2021-11-26  1770.31   -1.61    13.73      0.0  1.26        1.99
    """

    df = PER_PBR_배당수익률_개별지수().fetch(
        fromdate, todate, ticker[0], ticker[1:])
    df = df[['TRD_DD', 'CLSPRC_IDX', 'FLUC_RT', 'WT_PER',
             'WT_STKPRC_NETASST_RTO', 'DIV_YD']]
    df.columns = ['날짜', '종가', '등락률', 'PER', 'PBR',
                  '배당수익률']
    df = df.set_index('날짜')
    df.index = pd.to_datetime(df.index)
    df = df.replace('^-$', 0, regex=True)
    df = df.replace(',', '', regex=True)
    df = df.replace('', 0)
    df = df.astype({
        "종가": np.float64,
        "등락률": np.float64,
        "PER": np.float32,
        "PBR": np.float32,
        "배당수익률": np.float32
    })
    return df.sort_index()


@dataframe_empty_handler
def get_index_portfolio_deposit_file(date: str, ticker: str) -> list:
    """지수구성종목을 리스트로 반환

    Args:
        date   (str): 조회 일자 (YYMMDD)
        ticker (str): 인덱스 ticker

    Returns:
        list: ['005930', '000660', '051910', ...]

    """
    df = 지수구성종목().fetch(date, ticker[1:], ticker[0])
    if df.empty:
        return []
    return df['ISU_SRT_CD'].tolist()


# -----------------------------------------------------------------------------
# shorting
@dataframe_empty_handler
def get_shorting_status_by_date(fromdate, todate, ticker):
    """일자별 공매도 종합 현황
    :param fromdate: 조회 시작 일자   (YYYYMMDD)
    :param todate  : 조회 종료 일자 (YYYYMMDD)
    :param ticker  : 종목 번호
    :return        : 종합 현황 DataFrame
                  공매도    잔고   공매도금액     잔고금액
        날짜
        20180105   41726  177954   3303209900  14111752200
        20180108   32411  167754   2528196100  13118362800
        20180109   50486  175261   3885385100  13477570900
    """

    isin = get_stock_ticker_isin(ticker)
    df = 개별종목_공매도_종합정보().fetch(fromdate, todate, isin)
    df = df[['TRD_DD', 'CVSRTSELL_TRDVOL', 'STR_CONST_VAL1',
             'CVSRTSELL_TRDVAL', 'STR_CONST_VAL2']]
    df.columns = ['날짜', '거래량', '잔고수량', '거래대금', '잔고금액']
    df = df.set_index('날짜')
    df.index = pd.to_datetime(df.index, format='%Y/%m/%d')

    # '-'는 데이터가 집계되지 않은 것을 의미한다.
    # 최근 2일 간의 데이터 ([:2])에서 '-'가 하나는 행의 갯수를 계산함
    idx = (df.iloc[:2] == '-').any(axis=1).sum()
    df = df.iloc[idx:]

    df = df.replace(r'\D', '', regex=True)
    df = df.replace('', 0)
    df = df.astype({
        "거래량": np.int32,
        "잔고수량": np.int32,
        "거래대금": np.int64,
        "잔고금액": np.int64
    })
    return df.sort_index()


@dataframe_empty_handler
def get_shorting_trading_value_and_volume_by_date(
        fromdate: str, todate: str, ticker: str) -> DataFrame:
    """[32001] 개별종목 공매도 거래

    Args:
        fromdate (str): 조회 시작 일자 (YYYYMMDD)
        todate   (str): 조회 종료 일자 (YYYYMMDD)
        ticker   (str): 인덱스 티커

    Returns:
        DataFrame:

            >> get_shorting_trading_value_and_volume(
                "20201226", "20210126", "005930")

                       거래량                    거래대금
                       공매도      매수  비중      공매도           매수  비중
            날짜
            2020-12-28   6924  40085044  0.02   544918800  3172810866091  0.02
            2020-12-29  15834  30339449  0.05  1236917300  2368814098000  0.05
            2020-12-30   2978  29417421  0.01   239159500  2344317462700  0.01
            2021-01-04   9279  38655276  0.02   771889500  3185356823460  0.02
            2021-01-05    169  35335669  0.00    14011100  2915618322800  0.00
    """
    isin = get_stock_ticker_isin(ticker)
    df = 개별종목_공매도_거래_개별추이().fetch(fromdate, todate, isin)

    df = df.set_index('TRD_DD')
    df.index.name = "날짜"
    df = df[['CVSRTSELL_TRDVOL', 'ACC_TRDVOL', 'TRDVOL_WT', 'CVSRTSELL_TRDVAL',
             'ACC_TRDVAL', 'TRDVAL_WT']]
    df.columns = pd.MultiIndex.from_product([['거래량', '거래대금'],
                                             ['공매도', '매수', '비중']])
    df = df.replace(r'[^-\w\.]', '', regex=True).replace('', '0')
    df = df.astype({
        ("거래량", "공매도"): np.int64,
        ("거래량", "매수"): np.int64,
        ("거래량", "비중"): np.float32,
        ("거래대금", "공매도"): np.int64,
        ("거래대금", "매수"): np.int64,
        ("거래대금", "비중"): np.float32
    })
    df.index = pd.to_datetime(df.index, format='%Y/%m/%d')
    return df.sort_index()


@dataframe_empty_handler
def get_shorting_trading_value_and_volume_by_ticker(
        date: str, market: str, include: list) -> DataFrame:
    """[32001] 개별종목 공매도 거래

    Args:
        date    (str): 조회 시작 일자 (YYYYMMDD)
        market  (str): 검색 시장 (KRX/KOSPI/KOSDAQ)
        include (str): 증권 구분 (주식/ETF/ETN/ELW/신주인수권및증권/수익증권)

    Returns:
        DataFrame:

            >> get_shorting_trading_value_and_volume_by_ticker(
                "20210125", "KOSPI", ["주식"])

                    거래량                     거래대금
                    공매도    매수      비중     공매도         매수      비중
            티커
            095570     32   180458  0.020004     134240    757272515  0.020004
            006840     79   386257  0.020004    2377900  11554067000  0.020004
            027410  18502  8453962  0.219971  108713300  49276275460  0.219971
            282330     96    82986  0.119995   14928000  13018465500  0.109985

    """
    inc2code = {
        "주식": "STMFRTSCIFDRFS",
        "ETF": "EF",
        "ETN": "EN",
        "ELW": "EW",
        "신주인수권증서및증권": "SRSW",
        "수익증권": "BC",
    }
    include = [inc2code[x] for x in include]
    market = {"KOSPI": "STK", "KOSDAQ": "KSQ", "KONEX": "KNX"}[market]

    df = 개별종목_공매도_거래_전종목().fetch(date, market, include)

    df = df.set_index('ISU_CD')
    df.index.name = "티커"
    df = df[['CVSRTSELL_TRDVOL', 'ACC_TRDVOL', 'TRDVOL_WT', 'CVSRTSELL_TRDVAL',
             'ACC_TRDVAL', 'TRDVAL_WT']]
    df.columns = pd.MultiIndex.from_product([['거래량', '거래대금'],
                                             ['공매도', '매수', '비중']])
    df = df.replace(r'[^-\w\.]', '', regex=True).replace('', '0')
    df = df.astype({
        ("거래량", "공매도"): np.int64,
        ("거래량", "매수"): np.int64,
        ("거래량", "비중"): np.float32,
        ("거래대금", "공매도"): np.int64,
        ("거래대금", "매수"): np.int64,
        ("거래대금", "비중"): np.float32
    })
    return df


@dataframe_empty_handler
def get_shorting_investor_by_date(fromdate: str, todate: str,
                                  market: str = "KOSPI",
                                  inquery: str = "거래량") -> DataFrame:
    """일자별로 정렬된 투자자별 공매도 잔고 현황

    Args:
        fromdate (str): 조회 시작 일자 (YYYYMMDD)
        todate   (str): 조회 종료 일자 (YYYYMMDD)
        market   (str): 조회 시장 (KOSPI/KOSDAQ/KONEX)
        inquery  (str): 조회 구분 (거래량/거래대금)

    Returns:
        DataFrame:
            >> get_shorting_investor_by_date(
                "20210104", "20210108", "KOSPI", "거래대금")

                                   기관  개인  외국인  기타         합계
                날짜
                2021-01-04  31128651988     0       0     0  31128651988
                2021-01-05  13862504742     0       0     0  13862504742
                2021-01-06  12510404426     0       0     0  12510404426
                2021-01-07   8899480971     0       0     0   8899480971
                2021-01-08  15758050170     0       0     0  15758050170

            >> get_shorting_investor_by_date(
                "20210104", "20210108", "KOSPI", "거래량")

                              기관  개인  외국인  기타    합계
                날짜
                2021-01-04  522089     0       0     0  522089
                2021-01-05  612363     0       0     0  612363
                2021-01-06  437326     0       0     0  437326
                2021-01-07  510984     0       0     0  510984
                2021-01-08  525053     0       0     0  525053

    """
    inquery2idx = {
        "거래량": 1,
        "거래대금": 2
    }
    market2idx = {
        "KOSPI": 1,
        "KOSDAQ": 2,
        "KONEX": 6
    }

    df = 투자자별_공매도_거래().fetch(
        fromdate, todate, inquery2idx[inquery], market2idx[market])

    df.columns = ['날짜', '기관', '개인', '외국인', '기타', '합계']
    df = df.replace(r'[^\w\.]', '', regex=True)
    df = df.replace('', 0)
    df = df.set_index('날짜')
    df.index = pd.to_datetime(df.index, format='%Y%m%d')
    return df.astype(np.int64).sort_index()


@dataframe_empty_handler
def get_shorting_volume_top50(date: str, market: str) -> DataFrame:
    """공매도 비중 상위 50개 종목 정보
        - 비중 = 거래대금/거래대금
    Args:
        date   (str): 조회 일자 (YYMMDD)
        market (str): 조회 시장 (KOSPI/KOSDAQ/KONEX)

    Returns:

        DataFrame:

            >> get_shorting_volume_top50("20210129", "KOSPI")

                   순위  공매도거래대금   총거래대금  공매도비중   직전40일거래대금평균  공매도거래대금증가율   직전40일공매도평균비중  공매도비중증가율  주가수익률
            티커
            003545   1         38510030    915824030        4.21               5814411                  6.62                     0.51              8.33       -1.25
            267290   2         13265200    329805000        4.02               2755259                  4.82                     0.66              6.14       -2.46
            015890   3         15865860    428852660        3.70               8316412                  1.91                     1.30              2.85       -4.46
            005945   4         25401240    908915950        2.79               4610634                  5.51                     0.44              6.40       -0.35
            227840   5         13784400    546597900        2.52               3084294                  4.47                     0.51              4.91       -2.37
    """  # pylint: disable=line-too-long # noqa: E501
    market2idx = {
        "KOSPI": 1,
        "KOSDAQ": 2,
        "KONEX": 3
    }
    df = 공매도_거래상위_50종목().fetch(date, market2idx[market])

    df = df[["RANK", "ISU_CD", "CVSRTSELL_TRDVAL", "ACC_TRDVAL",
             "TDD_SRTSELL_WT", "STR_CONST_VAL1", "STR_CONST_VAL2",
             "VALU_PD_AVG_SRTSELL_WT", "VALU_PD_CMP_TDD_SRTSELL_RTO",
             "PRC_YD"]]
    df.columns = ['순위', '티커', '공매도거래대금', '총거래대금', '공매도비중',
                  '직전40일거래대금평균', '공매도거래대금증가율',
                  '직전40일공매도평균비중', '공매도비중증가율', '주가수익률']
    df = df.set_index('티커')
    df = df.replace(r'[^-\w\.]', '', regex=True)
    df = df.replace('', 0)
    df = df.astype({
        "순위": np.int32,
        "공매도거래대금": np.int64,
        "총거래대금": np.int64,
        "직전40일거래대금평균": np.int64,
        "공매도비중": np.float64,
        "공매도거래대금증가율": np.float64,
        "직전40일공매도평균비중": np.float64,
        "공매도비중증가율": np.float64,
        "주가수익률": np.float64
    })
    return df


@dataframe_empty_handler
def get_shorting_balance_top50(date: str, market: str) -> DataFrame:
    """공매도 잔고 상위 50개 종목 정보
       - 공매비중 = 공매도 잔고/상장주식수
    Args:
        date   (str): 조회 일자 (YYMMDD)
        market (str): 조회 시장 (KOSPI/KOSDAQ/KONEX)

    Returns:

        DataFrame:

            >> get_shorting_balance_top50("20210129", "KOSPI")

                    순위  공매도잔고   상장주식수     공매도금액      시가총액       비중
            티커
            032350    1      4693027     69275662    74853780650  1.104947e+12   6.769531
            042670    2     10846251    215931625    92843908560  1.848375e+12   5.019531
            068270    3      6523965    134997805  2146384485000  4.441428e+13   4.828125
            008770    4      1269261     39248121   106237145700  3.285068e+12   3.230469
            011690    5      1604890     58494201     1957965800  7.136293e+10   2.740234
    """  # pylint: disable=line-too-long # noqa: E501

    market2idx = {
        "KOSPI": 1,
        "KOSDAQ": 2,
        "KONEX": 3
    }
    df = 공매도_잔고상위_50종목().fetch(date, market2idx[market])

    df = df[["RANK", "ISU_CD", "BAL_QTY", "LIST_SHRS", "BAL_AMT", "MKTCAP",
             "BAL_RTO"]]
    df.columns = ['순위', '티커', '공매도잔고', '상장주식수', '공매도금액',
                  '시가총액', '비중']
    df = df.set_index('티커')
    df = df.replace(r'[^-\w\.]', '', regex=True)
    df = df.replace('', 0)
    df = df.astype({
        "순위": np.int32,
        "공매도잔고": np.int64,
        "상장주식수": np.int64,
        "공매도금액": np.int64,
        "시가총액": np.float64,
        "비중": np.float16
    })
    return df


@dataframe_empty_handler
def get_shorting_balance_by_ticker(date: str, market: str) -> DataFrame:
    """티커로 정렬된 공매도 잔고 현황

    Args:
        date   (str): 조회 일자 (YYMMDD)
        market (str): 조회 시장 (KOSPI/KOSDAQ/KONEX)

    Returns:

        DataFrame:

            >> get_shorting_balance_by_ticker("20210127", "KOSPI")

                    공매도잔고   상장주식수  공매도금액      시가총액      비중
            티커
            095570       33055     46822295   134864400  1.910350e+11  0.070007
            006840        4575     13247561   131760000  3.815298e+11  0.029999
            027410       68060     95716791   449196000  6.317308e+11  0.070007
            282330        4794     17283906   757452000  2.730857e+12  0.029999
            138930      596477    325935246  3340271200  1.825237e+12  0.180054
    """

    market2idx = {
        "KOSPI": 1,
        "KOSDAQ": 2,
        "KONEX": 3
    }
    df = 전종목_공매도_잔고().fetch(date, market2idx[market])

    df = df[["ISU_CD", "BAL_QTY", "LIST_SHRS", "BAL_AMT", "MKTCAP", "BAL_RTO"]]
    df.columns = ['티커', '공매도잔고', '상장주식수', '공매도금액',
                  '시가총액', '비중']
    df = df.set_index('티커')
    df = df.replace(r'[^-\w\.]', '', regex=True)
    df = df.replace('', 0)
    df = df.astype({
        "공매도잔고": np.int64,
        "상장주식수": np.int64,
        "공매도금액": np.int64,
        "시가총액": np.float64,
        "비중": np.float16
    })
    return df


@dataframe_empty_handler
def get_shorting_balance_by_date(fromdate: str, todate: str, ticker: str) \
        -> DataFrame:
    """일자별로 정렬된 투자자별 공매도 잔고 현황

    Args:
        fromdate (str): 조회 시작 일자 (YYYYMMDD)
        todate   (str): 조회 종료 일자 (YYYYMMDD)
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

    isin = get_stock_ticker_isin(ticker)
    df = 개별종목_공매도_잔고().fetch(fromdate, todate, isin)

    df = df[["RPT_DUTY_OCCR_DD", "BAL_QTY", "LIST_SHRS", "BAL_AMT", "MKTCAP",
             "BAL_RTO"]]
    df.columns = ['날짜', '공매도잔고', '상장주식수', '공매도금액', '시가총액',
                  '비중']
    df = df.set_index('날짜')
    df = df.replace(r'[^-\w\.]', '', regex=True)
    df = df.replace('', 0)
    df.index = pd.to_datetime(df.index, format='%Y/%m/%d')
    df = df.astype({
        "공매도잔고": np.int64,
        "상장주식수": np.int64,
        "공매도금액": np.int64,
        "시가총액": np.float64,
        "비중": np.float32
    })
    return df.sort_index()


@dataframe_empty_handler
def get_stock_major_changes(ticker: str) -> DataFrame:
    """기업 주요 변동사항

    Args:
        ticker   (str): 조회 종목 티커

    Returns:

        DataFrame:

            >> get_stock_major_changes("005930")
    """

    isin = get_stock_ticker_isin(ticker)
    df = 기업주요변동사항().fetch(isin)

    df.columns = ['날짜', '상호변경전', '상호변경후', '업종변경전',
                  '업종변경후', '액면변경전', '액면변경후', '대표이사변경전',
                  '대표이사변경후']
    df = df.set_index('날짜')
    df = df.replace(r'[^-\w\.]', '', regex=True)
    df[['액면변경전', '액면변경후']] = df[['액면변경전', '액면변경후']] \
        .replace('', 0)
    df = df.replace('', '-')
    df.index = pd.to_datetime(df.index, format='%Y/%m/%d')
    df = df.astype({"액면변경전": np.int16})
    return df.sort_index()


if __name__ == "__main__":
    pd.set_option('display.expand_frame_repr', False)
    # df = get_market_price_change_by_ticker(
    #   fromdate="20210101", todate="20210111")
    # df = get_shorting_trading_value_and_volume_by_date(
    #   "20201226", "20210126", "005930")
    # df = get_index_ohlcv_by_date("20211126", "KOSPI")
    # df = get_stock_major_changes("005930")
    # df = get_market_ohlcv_by_date(
    #     "20201226", "20210126", "005930")
    # df = get_index_ohlcv_by_ticker("20220602")
    df = get_market_sector_classifications("20220902", "KOSPI")
    print(df)
