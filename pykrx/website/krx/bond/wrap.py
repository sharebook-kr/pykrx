from pykrx.website.comm import dataframe_empty_handler
from pykrx.website.krx.bond.core import (
    전종목_장외채권수익률, 개별추이_장외채권수익률
)
from pandas import DataFrame
import pandas as pd
import numpy as np


@dataframe_empty_handler
def get_otc_treasury_yields_by_ticker(date: str) -> DataFrame:
    """[14017] 장외 채권수익률 - 전종목

    Args:
        date (str): 조회 일자 (YYYYMMDD)

    Returns:
        DataFrame:

            > get_otc_treasury_yields_by_ticker("20220204")

                                   수익률     대비
            채권종류
            국고채 1년               1.467  0.015
            국고채 2년               1.995  0.026
            국고채 3년               2.194  0.036
            국고채 5년               2.418  0.045
            국고채 10년              2.619  0.053
            국고채 20년              2.639  0.055
            국고채 30년              2.559  0.057
            국민주택 1종 5년          2.570  0.048
            회사채 AA-(무보증 3년)    2.771  0.038
            회사채 BBB- (무보증 3년)  8.637  0.036
            CD(91일)                1.500  0.000
    """
    df = 전종목_장외채권수익률().fetch(date)
    if not df.empty:
        df.columns = ['채권종류', '수익률', '대비']
        df = df.astype({"수익률": np.float32, "대비": np.float32})
        df = df.set_index('채권종류')
    return df


@dataframe_empty_handler
def get_otc_treasury_yields_by_date(fromdate: str, todate: str, ticker: str) \
        -> DataFrame:
    """[14017] 장외 채권수익률 - 개별추이

        Args:
            startDd     (str): 시작 일자 (YYMMDD)
            endDd       (str): 종료 일자 (YYMMDD)
            bndKindTpCd (str): 장외 채권 티커
                - 국고채1년
                - 국고채2년
                - 국고채3년
                - 국고채5년
                - 국고채10년
                - 국고채20년
                - 국고채30년
                - 국민주택1종5년
                - 회사채AA
                - 회사채BBB
                - CD

        Returns:

            > get_otc_treasury_yields_by_date(
                "20220104", "20220204", "국고채2년")

                        수익률    대비
            일자
            2022-01-04  1.717  0.007
            2022-01-05  1.791  0.074
            2022-01-06  1.878  0.087
            2022-01-07  1.895  0.017
            2022-01-10  1.902  0.007
        """
    ticker2code = {
         "국고채1년": "3006",
         "국고채2년": "3019",
         "국고채3년": "3000",
         "국고채5년": "3007",
         "국고채10년": "3013",
         "국고채20년": "3014",
         "국고채30년": "3017",
         "국민주택1종5년": "3008",
         "회사채AA": "3009",
         "회사채BBB": "3010",
         "CD": "4000"
    }

    df = 개별추이_장외채권수익률().fetch(fromdate, todate, ticker2code[ticker])
    df.columns = ['일자', '수익률', '대비']
    df = df.astype({"수익률": np.float32, "대비": np.float32})
    df = df.set_index('일자')
    df.index = pd.to_datetime(df.index, format='%Y/%m/%d')
    return df.sort_index()


if __name__ == "__main__":
    pd.set_option('display.width', None)
    # df = get_otc_treasury_yields_by_ticker("20220204")
    df = get_otc_treasury_yields_by_date("20220104", "20220204", "국고채2년")
    print(df)
