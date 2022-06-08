from sqlite3 import NotSupportedError
from pykrx.website import krx
from pandas import DataFrame


def get_otc_treasury_yields(*args) -> DataFrame:
    """장외 일자별 채권수익률

    Args:
        date   (str, optional): 조회 일자 (YYYYMMDD)

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
        DataFrame:

        > get_otc_treasury_yields_by_date("20220104", "20220204", "국고채2년")

                        수익률    대비
            일자
            2022-01-04  1.717  0.007
            2022-01-05  1.791  0.074
            2022-01-06  1.878  0.087
            2022-01-07  1.895  0.017
            2022-01-10  1.902  0.007
    """

    if len(args) == 1:
        df = krx.bond.get_otc_treasury_yields_by_ticker(args[0])
        if df.empty:
            target_date = krx.get_nearest_business_day_in_a_week(
                date=args[0], prev=True)
            df = krx.get_otc_treasury_yields_by_ticker(target_date)
    elif len(args) == 3:
        df = krx.bond.get_otc_treasury_yields_by_date(*args)
    else:
        raise NotSupportedError

    return df


if __name__ == "__main__":
    # df = get_otc_treasury_yields("20220204")
    # print(df)

    # df = get_otc_treasury_yields("20220104", "20220203", "국고채1년")
    # print(df)
    print(get_otc_treasury_yields("20220204"))
