from pykrx.website.comm import dataframe_empty_handler
from pykrx.website.krx.future.core import (
    파생상품검색, 전종목시세
)
import numpy as np
from pandas import DataFrame


def get_future_ticker_and_name() -> DataFrame:
    return 파생상품검색().fetch()


def get_future_ticker_list() -> DataFrame:
    return 파생상품검색().fetch().index.to_list()


@dataframe_empty_handler
def get_future_ohlcv_by_ticker(date: str, prod: str) -> DataFrame:
    """티커별로 정리된 특정 일자의 OHLCV

    Args:
        date (str): 조회 일자 (YYYYMMDD)
        prod (str): 조회 상품

    Returns:
    >> get_future_ohlcv_by_ticker("20220902", "KRDRVFUK2I")

                                        종목명     종가  대비    시가    고가    저가  현물가  거래량        거래대금
        종목코드
        101S9000      코스피200 F 202209 (주간)  313.85 -0.65  315.70  315.85  311.15  313.85  307202  24105144800000
        101SC000      코스피200 F 202212 (주간)  314.75 -0.75  316.60  316.75  312.15  314.75   14474   1139311725000
        101T3000      코스피200 F 202303 (주간)  311.25 -1.45  312.70  313.50  310.75  311.25     274     21450150000
        101T6000      코스피200 F 202306 (주간)  311.40 -3.55  313.60  313.60  311.40  311.40       4       313050000
        101TC000      코스피200 F 202312 (주간)  316.65 -2.35  316.65  316.65  316.65  316.65       1        79162500
        101V6000      코스피200 F 202406 (주간)  320.55 -1.45  320.55  320.55  320.55  320.55       3       240412500
        101VC000      코스피200 F 202412 (주간)    0.00  0.00    0.00    0.00    0.00  318.45       0               0
        401S9SCS  코스피200 SP 2209-2212 (주간)    0.90  0.90    1.00    1.00    0.90    0.00   17618   2767660050000
        401S9T3S  코스피200 SP 2209-2303 (주간)    0.00  0.00    0.00    0.00    0.00    0.00       0               0
        401S9T6S  코스피200 SP 2209-2306 (주간)    0.00  0.00    0.00    0.00    0.00    0.00       0               0
        401S9TCS  코스피200 SP 2209-2312 (주간)    0.00  0.00    0.00    0.00    0.00    0.00       0               0
        401S9V6S  코스피200 SP 2209-2406 (주간)    0.00  0.00    0.00    0.00    0.00    0.00       0               0
        401S9VCS  코스피200 SP 2209-2412 (주간)    0.00  0.00    0.00    0.00    0.00    0.00       0               0
        """  # pylint: disable=line-too-long # noqa: E501
    df = 전종목시세().fetch(date, prod)
    df = df[['ISU_SRT_CD', 'ISU_NM', 'TDD_CLSPRC', 'CMPPREVDD_PRC',
             'TDD_OPNPRC', 'TDD_HGPRC', 'TDD_LWPRC', 'SETL_PRC', 'ACC_TRDVOL',
             'ACC_TRDVAL']]
    df.columns = ['종목코드', '종목명', '종가', '대비', '시가', '고가', '저가',
                  '현물가', '거래량', '거래대금']
    df = df.set_index('종목코드')

    df = df.replace(r'\-$', '0', regex=True)
    df = df.replace('', '0', regex=True)
    df = df.replace(',', '', regex=True)
    df = df.astype({
        "종가": np.float64,
        "대비": np.float64,
        "시가": np.float64,
        "고가": np.float64,
        "저가": np.float64,
        "현물가": np.float64,
        "거래량": np.int32,
        "거래대금": np.int64
    })
    return df


if __name__ == "__main__":
    tickers = get_future_ticker_list()
    for t in tickers[:1]:
        df = get_future_ohlcv_by_ticker("20220902", t)
        print(df)
