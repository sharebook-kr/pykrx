from pykrx.website.comm import dataframe_empty_handler
from pykrx.website.krx.etx.core import (
    개별종목시세_ETF, 전종목시세_ETF, 전종목등락률_ETF, PDF, 추적오차율추이,
    괴리율추이, ETF_투자자별거래실적_기간합계, ETF_투자자별거래실적_일별추이,
    ETN_투자자별거래실적_개별종목_기간합계,
    ETN_투자자별거래실적_개별종목_일별추이,
    ETF_투자자별거래실적_개별종목_기간합계,
    ETF_투자자별거래실적_개별종목_일별추이,

)
from pykrx.website.krx.etx.ticker import get_etx_isin, is_etf
import numpy as np
import pandas as pd
from pandas import DataFrame


@dataframe_empty_handler
def get_etf_ohlcv_by_date(fromdate: str, todate: str, ticker: str) \
        -> DataFrame:
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
    """  # pylint: disable=line-too-long # noqa: E501

    isin = get_etx_isin(ticker)
    df = 개별종목시세_ETF().fetch(fromdate, todate, isin)

    df = df[['TRD_DD', 'LST_NAV', 'TDD_OPNPRC', 'TDD_HGPRC', 'TDD_LWPRC',
             'TDD_CLSPRC', 'ACC_TRDVOL', 'ACC_TRDVAL', 'OBJ_STKPRC_IDX']]
    df.columns = ['날짜', 'NAV', '시가', '고가', '저가', '종가', '거래량',
                  '거래대금', '기초지수']
    df = df.replace(r'^-$', '0', regex=True)
    df = df.replace(r',', '', regex=True)
    df = df.set_index('날짜')
    df = df.astype({
        "NAV": np.float64,
        "시가": np.uint32,
        "고가": np.uint32,
        "저가": np.uint32,
        "종가": np.uint32,
        "거래량": np.uint64,
        "거래대금": np.uint64,
        "기초지수": np.float64
    })
    df.index = pd.to_datetime(df.index, format='%Y/%m/%d')
    return df.sort_index()


@dataframe_empty_handler
def get_etf_ohlcv_by_ticker(date: str) -> DataFrame:
    """특정 일자의 전종목의 OHLCV 조회

    Args:
        date (str): 조회 시작 일자 (YYYYMMDD)

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
    """  # pylint: disable=line-too-long # noqa: E501

    df = 전종목시세_ETF().fetch(date)
    df = df[['ISU_SRT_CD', 'NAV', 'TDD_OPNPRC', 'TDD_HGPRC', 'TDD_LWPRC',
             'TDD_CLSPRC', 'ACC_TRDVOL',  'ACC_TRDVAL', 'OBJ_STKPRC_IDX']]
    df.columns = ['티커', 'NAV', '시가', '고가', '저가', '종가', '거래량',
                  '거래대금', '기초지수']
    df = df.replace(r'[^-\w\.]', '', regex=True)
    df = df.replace(r'\-$', '0', regex=True)
    df = df.set_index('티커')
    df = df.astype({
        "NAV": np.float64,
        "시가": np.uint32,
        "고가": np.uint32,
        "저가": np.uint32,
        "종가": np.uint32,
        "거래량": np.uint64,
        "거래대금": np.uint64,
        "기초지수": np.float64
    })
    return df


@dataframe_empty_handler
def get_etf_price_change_by_ticker(fromdate: str, todate: str) -> DataFrame:
    """특정 기간동안 전종의 등락률 조회

    Args:
        fromdate (str): 조회 시작 일자 (YYYYMMDD)
        todate   (str): 조회 종료 일자 (YYYYMMDD)

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

    df = 전종목등락률_ETF().fetch(fromdate, todate)
    df = df[['ISU_SRT_CD', 'BAS_PRC', 'CLSPRC', 'CMP_PRC', 'FLUC_RT',
             'ACC_TRDVOL', 'ACC_TRDVAL']]
    df.columns = ['티커', '시가', '종가', '변동폭', '등락률', '거래량',
                  '거래대금']
    df = df.replace(r'[^-\w\.]', '', regex=True)
    df = df.replace(r'\-$', '0', regex=True)
    df = df.set_index('티커')
    df = df.astype({
        "시가": np.uint32,
        "종가": np.uint32,
        "변동폭": np.int32,
        "등락률": np.float32,
        "거래량": np.uint64,
        "거래대금": np.uint64
    })
    return df


@dataframe_empty_handler
def get_etf_portfolio_deposit_file(date: str, ticker: str) -> DataFrame:
    """Portfolio Deposit File 조회

    Args:
        date   (str): 조회 일자 (YYMMDD)
        ticker (str): 조회 종목 티커

    Returns:
        DataFrame:
                     계약수       금액       비중
            티커
            005930   8175.0  694875000  16.531250
            000660    972.0  126360000   2.949219
            051910     80.0   77120000   1.849609
            035420    219.0   65809500   1.570312
    """

    isin = get_etx_isin(ticker)
    df = PDF().fetch(date, isin)
    df = df[['COMPST_ISU_CD', 'COMPST_ISU_CU1_SHRS', 'VALU_AMT', 'COMPST_RTO']]
    df.columns = ['티커', '계약수', '금액', '비중']

    # NOTE: 웹 서버가 COMPST_ISU_CD에 ISIN과 축향형을 혼합해서 반환한다. Why?
    df['티커'] = df['티커'].apply(lambda x: x[3:9] if len(x) > 6 else x)
    df = df.set_index('티커')

    df = df.replace(',', '', regex=True)
    # - empty string은 int, float로 형변환 불가
    #  -> 이 문제를 해결하기 위해 '-' 문자는 0으로 치환
    df = df.replace(r'\-$', '0', regex=True)
    df = df.astype({
        "계약수": np.float64,
        "금액": np.uint64,
        "비중": np.float32
    })
    df = df[(df.T != 0).any()]
    return df


@dataframe_empty_handler
def get_etf_price_deviation(fromdate: str, todate: str, ticker: str) \
        -> DataFrame:
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

    isin = get_etx_isin(ticker)
    df = 괴리율추이().fetch(fromdate, todate, isin)
    df = df[['TRD_DD', 'CLSPRC', 'LST_NAV', 'DIVRG_RT']]
    df.columns = ['날짜', '종가', 'NAV', '괴리율']
    df = df.set_index('날짜')
    df = df.replace(',', '', regex=True)

    df = df.astype({
        "종가": np.uint32,
        "NAV": np.float64,
        "괴리율": np.float32
    })
    df.index = pd.to_datetime(df.index, format='%Y/%m/%d')
    return df.sort_index()


@dataframe_empty_handler
def get_etf_tracking_error(fromdate: str, todate: str, ticker: str) \
        -> DataFrame:
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

    isin = get_etx_isin(ticker)
    df = 추적오차율추이().fetch(fromdate, todate, isin)
    df = df[['TRD_DD', 'LST_NAV', 'OBJ_STKPRC_IDX', 'TRACE_ERR_RT']]
    df.columns = ['날짜', 'NAV', '지수', '추적오차율']
    df = df.set_index('날짜')
    df = df.replace(',', '', regex=True)
    df = df.astype({
        "NAV": np.float64,
        "지수": np.float64,
        "추적오차율": np.float32
    })
    df.index = pd.to_datetime(df.index, format='%Y/%m/%d')
    return df.sort_index()


@dataframe_empty_handler
def get_trading_volume_and_value_by_investor(fromdate: str, todate: str) \
        -> DataFrame:
    """주어진 기간의 투자자별 거래실적 합계

    Args:
        fromdate    (str): 조회 시작 일자 (YYMMDD)
        todate      (str): 조회 종료 일자 (YYMMDD)

    Returns:
        DataFrame:

            > get_etf_trading_volumne_and_value_by_investor("20220415", "20220422")

                           거래량                              거래대금
                             매도        매수    순매수            매도            매수            순
            매수
            금융투자    375220036   328066683 -47153353   3559580094684   3040951626908 -518628467776
            보험         15784738    15490448   -294290    309980189819    293227931019  -16752258800
            투신         14415013    15265023    850010    287167721259    253185404050  -33982317209
            사모          6795002     7546735    751733     58320840040    120956023820   62635183780
    """  # pylint: disable=line-too-long # noqa: E501

    df = ETF_투자자별거래실적_기간합계().fetch(fromdate, todate)
    df = df[df.columns[1:]]
    # df.columns = ["투자자", "거래량-매도"]

    df = df.set_index('INVST_NM')
    df.index.name = None
    df.columns = pd.MultiIndex.from_product(
        [["거래량", "거래대금"], ["매도", "매수", "순매수"]])

    df = df.replace(',', '', regex=True)
    df = df.astype({
        ("거래량", "매도"): np.uint64,
        ("거래량", "매수"): np.uint64,
        ("거래량", "순매수"): np.int64,
        ("거래대금", "매도"): np.uint64,
        ("거래대금", "매수"): np.uint64,
        ("거래대금", "순매수"): np.int64,
    })
    return df


@dataframe_empty_handler
def get_trading_volume_and_value_by_date(
    fromdate: str, todate: str, query_type1: str, query_type2: str) \
         -> DataFrame:
    """주어진 기간의 일자별 거래 실적 조회

    Args:
        fromdate        (str): 조회 시작 일자 (YYMMDD)
        todate          (str): 조회 종료 일자 (YYMMDD)
        query_type1     (str): 거래대금 / 거래량
        query_type2     (str): 순매수 / 매수 / 매도

    Returns:
        DataFrame:

            > get_etf_trading_volumne_and_value_by_date("20220415", "20220422", "거래대금", "순매수")

                                기관    기타법인         개인        외국인 전체
            날짜
            2022-04-15   25346770535  -138921500  17104310255  -42312159290    0
            2022-04-18 -168362290065  -871791310  88115812520   81118268855    0
            2022-04-19  -36298873785  7555666910  -1968998025   30712204900    0
            2022-04-20 -235935697655  8965445880  19247888605  207722363170    0
            2022-04-21  -33385835805  2835764290  35920390975   -5370319460    0
            2022-04-22  -10628831870  2032673735  39477777530  -30881619395    0
    """  # pylint: disable=line-too-long # noqa: E501
    query_type1 = {"거래대금": 1, "거래량": 2}.get(query_type1, 1)
    query_type2 = {"순매수": 1, "매수": 2, "매도": 3}.get(query_type2, 3)

    df = ETF_투자자별거래실적_일별추이().fetch(
        fromdate, todate, query_type1, query_type2)
    df.columns = ['날짜', '기관', '기타법인', '개인', '외국인', "전체"]

    df = df.set_index('날짜')
    df.index = pd.to_datetime(df.index, format='%Y/%m/%d')

    df = df.replace(',', '', regex=True)
    df = df.astype({
        "기관": np.int64,
        "기타법인": np.int64,
        "개인": np.int64,
        "외국인": np.int64,
        "전체": np.uint64,
    })
    return df.sort_index()


def get_indivisual_trading_volume_and_value_by_investor(
        fromdate: str, todate: str, ticker: str) -> DataFrame:
    """주어진 기간 동안 ETF 개별종목의 거래실적 합계

    Args:
        fromdate    (str): 조회 시작 일자 (YYMMDD)
        todate      (str): 조회 종료 일자 (YYMMDD)
        ticker      (str): 조회 종목의 티커

    Returns:
        DataFrame:

            > get_indivisual_trading_volume_and_value_by_investor("20220415", "20220422", "295820")

                            거래량                            거래대금
                            매도      매수   순매수           매도           매수        순매수
            INVST_NM
            금융투자    13453041  11123937 -2329104   444469514445   365724346362  -78745168083
            보험         1260497    345498  -914999    41472479252    11310518250  -30161961002
            투신         3342071   7782039  4439968   108944563190   258175090550  149230527360
            사모           45740    770704   724964     1504460550    25202532050   23698071500
            은행           68385    298436   230051     2262720980     9795886410    7533165430
            기타금융      187880    154684   -33196     6139349970     5092411005   -1046938965
            연기금 등     697475    616932   -80543    22792327650    20070873780   -2721453870
            기관합계    19055089  21092230  2037141   627585416037   695371658407   67786242370
            기타법인      120080      6390  -113690     3898307805      209841125   -3688466680
            개인         4791437   4806978    15541   158063728290   158360713095     296984805
            외국인       7637514   5699037 -1938477   252228793405   187851734035  -64377059370
            기타외국인      3213      2698     -515      106815950       89114825     -17701125
            전체        31607333  31607333        0  1041883061487  1041883061487             0
    """  # pylint: disable=line-too-long # noqa: E501

    isin = get_etx_isin(ticker)
    if is_etf(ticker):
        df = ETF_투자자별거래실적_개별종목_기간합계().fetch(
            fromdate, todate, isin)
    else:
        df = ETN_투자자별거래실적_개별종목_기간합계().fetch(
            fromdate, todate, isin)

    df = df[df.columns[1:]]
    df = df.set_index("INVST_NM")
    df.columns = pd.MultiIndex.from_product(
        [["거래량", "거래대금"], ["매도", "매수", "순매수"]])

    df = df.replace(',', '', regex=True)
    df = df.astype({
        ("거래량", "매도"): np.uint64,
        ("거래량", "매수"): np.uint64,
        ("거래량", "순매수"): np.int64,
        ("거래대금", "매도"): np.uint64,
        ("거래대금", "매수"): np.uint64,
        ("거래대금", "순매수"): np.int64,
    })
    return df


@dataframe_empty_handler
def get_indivisual_trading_volume_and_value_by_date(
    fromdate: str, todate: str, ticker: str, query_type1: str,
        query_type2: str) -> DataFrame:
    """주어진 기간동안 ETF 개별 종목 일자별 거래 실적 조회

    Args:
        fromdate        (str): 조회 시작 일자 (YYMMDD)
        todate          (str): 조회 종료 일자 (YYMMDD)
        ticker          (str): 조회 종목의 티커
        query_type1     (str): 거래대금 / 거래량
        query_type2     (str): 순매수 / 매수 / 매도

    Returns:
        DataFrame:

            > get_etf_indivisual_trading_volume_and_value_by_date("20230421", "20230428", "069500", "거래대금", "순매수")

                               기관    기타법인        개인       외국인  전체
            날짜
            2023-04-21  10044395710  -111645190 -5941906355  -3990844165     0
            2023-04-24  31985801385  -139497015 -2374485250 -29471819120     0
            2023-04-25   5704378590   -19479320  3507386490  -9192285760     0
            2023-04-26   4380096075  -346222120 -1034965685  -2998908270     0
            2023-04-27   5193885065 -3021189345  2319333145  -4492028865     0
            2023-04-28  10477685545   -50433690  3821622460 -14248874315     0
    """  # pylint: disable=line-too-long # noqa: E501

    isin = get_etx_isin(ticker)
    query_type1 = {"거래대금": 1, "거래량": 2}.get(query_type1, 1)
    query_type2 = {"순매수": 1, "매수": 2, "매도": 3}.get(query_type2, 3)

    if is_etf(ticker):
        df = ETF_투자자별거래실적_개별종목_일별추이().fetch(
            fromdate, todate, isin, inqCondTpCd1=query_type1,
            inqCondTpCd2=query_type2)
    else:
        df = ETN_투자자별거래실적_개별종목_일별추이().fetch(
            fromdate, todate, isin, inqCondTpCd1=query_type1,
            inqCondTpCd2=query_type2)
    df.columns = ['날짜', '기관', '기타법인', '개인', '외국인', "전체"]

    df = df.set_index('날짜')
    df.index = pd.to_datetime(df.index, format='%Y/%m/%d')

    df = df.replace(',', '', regex=True)
    df = df.astype({
        "기관": np.int64,
        "기타법인": np.int64,
        "개인": np.int64,
        "외국인": np.int64,
        "전체": np.uint64,
    })
    return df.sort_index()


if __name__ == "__main__":
    pd.set_option('display.width', None)
    # print(get_etf_ohlcv_by_date("20200101", "20200401", "295820"))
    # print(get_etf_portfolio_deposit_file("20210119", "152100"))
    # print( get_etf_price_deviation("20200101", "20200401", "295820"))
    # print(get_etf_tracking_error("20200101", "20200401", "295820"))
    # print(get_etf_portfolio_deposit_file("20210705", "114800"))
    # df = get_etf_trading_volumne_and_value_by_investor("20220415", "20220422")
    # df = get_etf_trading_volumne_and_value_by_date(
    #     "20220415", "20220422", 1, 1)
    # df = get_etf_unbound_trading_volumne_and_value_by_investor("20220908", "20220916", "580011")
    # df = get_indivisual_trading_volume_and_value_by_date("20230421", "20230428", "069500", "거래대금", "순매수")
    # df = get_indivisual_trading_volume_and_value_by_investor("20230421", "20230428", "069500")
    # df = get_indivisual_trading_volume_and_value_by_date("20230421", "20230428", "580011", "거래대금", "순매수")
    df = get_indivisual_trading_volume_and_value_by_investor("20230421", "20230428", "580011")
    print(df)
