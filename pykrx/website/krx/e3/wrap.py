from pykrx.website.comm import dataframe_empty_handler
from pykrx.website.krx.e3.core import (개별종목시세_ETF, 전종목시세_ETF,
                                       PDF, 추적오차율추이, 괴리율추이)
from pykrx.website.krx.e3.ticker import EtfTicker
import numpy as np
import decimal
import pandas as pd
from pandas import DataFrame


@dataframe_empty_handler
def get_etf_ohlcv_by_date(fromdate: str, todate: str, ticker: str) -> DataFrame:
    """주어진 기간동안 특정 ETF의 OHLCV

    Args:
        fromdate (str): 조회 시작 일자 (YYYYMMDD)
        todate   (str): 조회 종료 일자 (YYYYMMDD)
        ticker   (str): 조회 종목의 티커

    Returns:
        DataFrame:
                             NAV  시가  고가  저가  종가  거래량    거래대금  기초지수
            날짜
            2020-01-02  830258.0  8290  8315  8270  8285     162     1342740  181911.0
            2020-01-03  829789.0  8340  8365  8275  8290      29      241025  181813.0
            2020-01-06  814595.0  8230  8230  8140  8150      32      261570  178472.0
            2020-01-07  822605.0  8225  8225  8200  8220  238722  1960114040  180234.0
    """
    isin = EtfTicker().get_isin(ticker)
    df = 개별종목시세_ETF().fetch(fromdate, todate, isin)

    df = df[['TRD_DD', 'LST_NAV', 'TDD_OPNPRC', 'TDD_HGPRC', 'TDD_LWPRC',
             'TDD_CLSPRC', 'ACC_TRDVOL', 'ACC_TRDVAL', 'OBJ_STKPRC_IDX']]
    df.columns = ['날짜', 'NAV', '시가', '고가', '저가', '종가', '거래량',
                  '거래대금', '기초지수']
    df = df.replace('[^-\w\.]', '', regex=True)
    df = df.set_index('날짜')
    df = df.astype({"NAV": np.float32, "시가": np.uint32, "고가": np.uint32,
                    "저가": np.uint32, "종가": np.uint32, "거래량": np.uint64,
                    "거래대금": np.uint64, "기초지수": np.float32})
    df.index = pd.to_datetime(df.index, format='%Y%m%d')
    return df.sort_index()


@dataframe_empty_handler
def get_etf_portfolio_deposit_file(date: str, ticker: str) -> DataFrame:
    """Portfolio Deposit File 조회

    Args:
        date   (str): 조회 일자 (YYMMDD)
        ticker (str): 조회 종목 티커

    Returns:
        DataFrame:
                        종목명  계약수       금액       비중
            티커
            005930    삼성전자  8175.0  694875000  16.531250
            000660  SK하이닉스   972.0  126360000   2.949219
            051910      LG화학    80.0   77120000   1.849609
            035420       NAVER   219.0   65809500   1.570312
    """
    isin = EtfTicker().get_isin(ticker)
    df = PDF().fetch(date, isin)
    df = df[['COMPST_ISU_CD', 'COMPST_ISU_NM', 'COMPST_ISU_CU1_SHRS', 'VALU_AMT', 'COMPST_RTO']]
    df.columns = ['티커', '종목명', '계약수', '금액', '비중']

    # NOTE: 웹 서버가 COMPST_ISU_CD에 ISIN과 축향형을 혼합해서 반환한다. Why?
    df['티커'] = df['티커'].apply(lambda x : x[3:9] if len(x) > 6 else x)
    df = df.set_index('티커')

    df = df.replace(',', '', regex=True)
    # - empty string은 int, float로 형변환 불가
    #  -> 이 문제를 해결하기 위해 '-' 문자는 0으로 치환
    df = df.replace('-', '0', regex=True)
    df = df.astype({"계약수": np.float32, "금액": np.uint64, "비중": np.float16 })
    return df


@dataframe_empty_handler
def get_etf_price_deviation(fromdate: str, todate: str, ticker: str) -> DataFrame:
    """주어진 기간동안 특정 종목의 괴리율 추이를 반환

    Args:
        fromdate (str): 조회 시작 일자 (YYYYMMDD)
        todate   (str): 조회 종료 일자 (YYYYMMDD)
        ticker   (str): 조회 종목의 티커

    Returns:
        DataFrame:
                        종가          NAV    괴리율
            날짜
            2020-01-02  8285  8302.580078 -0.209961
            2020-01-03  8290  8297.889648 -0.099976
            2020-01-06  8150  8145.950195  0.049988
            2020-01-07  8220  8226.049805 -0.070007
            2020-01-08  7980  7998.839844 -0.239990

    """
    isin = EtfTicker().get_isin(ticker)
    df = 괴리율추이().fetch(fromdate, todate, isin)
    df = df[['TRD_DD', 'CLSPRC', 'LST_NAV', 'DIVRG_RT']]
    df.columns = ['날짜', '종가', 'NAV', '괴리율']
    df = df.set_index('날짜')
    df = df.replace(',', '', regex=True)

    df = df.astype({"종가": np.uint32, "NAV": np.float32, "괴리율": np.float16})
    df.index = pd.to_datetime(df.index, format='%Y/%m/%d')
    return df.sort_index()


@dataframe_empty_handler
def get_etf_tracking_error(fromdate: str, todate: str, ticker: str) -> DataFrame:
    """주어진 기간동안 특정 종목의 추적 오차율을 반환

    Args:
        fromdate (str): 조회 시작 일자 (YYYYMMDD)
        todate   (str): 조회 종료 일자 (YYYYMMDD)
        ticker   (str): 조회 종목의 티커

    Returns:
        DataFrame:
                                NAV        지수 추적오차율
            날짜
            2020-01-02  8302.580078  1819.109985  0.320068
            2020-01-03  8297.889648  1818.130005  0.320068
            2020-01-06  8145.950195  1784.719971  0.320068
            2020-01-07  8226.049805  1802.339966  0.320068
            2020-01-08  7998.839844  1752.359985  0.320068
    """
    isin = EtfTicker().get_isin(ticker)
    df = 추적오차율추이().fetch(fromdate, todate, isin)
    df = df[['TRD_DD', 'LST_NAV', 'OBJ_STKPRC_IDX', 'TRACE_ERR_RT']]
    df.columns = ['날짜', 'NAV', '지수', '추적오차율']
    df = df.set_index('날짜')
    df = df.replace(',', '', regex=True)
    df = df.astype({"NAV": np.float32, "지수": np.float32, "추적오차율": np.float16})
    df.index = pd.to_datetime(df.index, format='%Y/%m/%d')
    return df.sort_index()


if __name__ == "__main__":
    import pandas as pd
    pd.set_option('display.width', None)
    # print(get_etf_ohlcv_by_date("20200101", "20200401", "295820"))
    # print(get_etf_portfolio_deposit_file("20210119", "152100"))
    # print( get_etf_price_deviation("20200101", "20200401", "295820"))
    print(get_etf_tracking_error("20200101", "20200401", "295820"))



