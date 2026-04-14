from pandas import DataFrame

from pykrx.website.comm import dataframe_empty_handler
from pykrx.website.krx.items.core import (
    개별종목_시세_추이,
    전종목_시세_검색,
)

# -----------------------------------------------------------------------------
# physical gold price


@dataframe_empty_handler
def get_item_gold_price(isuCd: str, strtDd: str, endDd: str) -> DataFrame:
    """Get physical gold price data from KRX.

    Args:
        start (str): Start date in 'YYYYMMDD' format.
        end (str): End date in 'YYYYMMDD' format.

    Returns:
        DataFrame: DataFrame containing gold price data.
    """
    gold = 개별종목_시세_추이()

    return gold.fetch(
        isuCd=isuCd,
        strtDd=strtDd,
        endDd=endDd,
    )


@dataframe_empty_handler
def get_item_gold_ticker(date: str) -> str:
    """Get the ticker code for physical gold.

    Returns:
        str: Ticker code for physical gold.
    """
    return 전종목_시세_검색().fetch(trdDd=date)  # 금현물 ISIN 코드


if __name__ == "__main__":
    # 종목 코드 확인 (ISU_CD)
    item_gold_ticker = get_item_gold_ticker("20251125")
    # ISU_CD 금 99.99_1Kg(KRD040200002), 미니금 99.99_100g(KRD040201000)

    # isuCd: KRD040200002 금현물
    df_gold_price = get_item_gold_price(
        isuCd="KRD040200002",
        strtDd="20251107",
        endDd="20251125",
    )
    print(df_gold_price)
    #     get_item_gold_price("KRD040200002", "20251107", "20251125")
    #     TRD_DD	TDD_CLSPRC	FLUC_TP_CD	CMPPREVDD_PRC	FLUC_RT	TDD_OPNPRC	TDD_HGPRC	TDD_LWPRC	ACC_TRDVOL	ACC_TRDVAL
    # 0	2025/11/25	197,300	1	3,850	1.99	196,960	197,500	196,290	574,628	112,714,602,040
    # 1	2025/11/24	193,450	1	950	0.49	193,990	194,370	192,640	450,377	86,957,824,440
    # 2	2025/11/21	192,500	2	-1,300	-0.67	194,300	194,800	192,290	762,186	147,389,964,110
    # 3	2025/11/20	193,800	2	-550	-0.28	196,010	196,010	192,380	714,488	138,800,566,800
    # 4	2025/11/19	194,350	1	3,550	1.86	193,500	194,480	192,670	659,017	127,413,851,260
    # 5	2025/11/18	190,800	2	-2,670	-1.38	191,450	192,990	190,700	683,411	131,314,838,820
    # 6	2025/11/17	193,470	2	-6,510	-3.26	195,700	195,800	192,540	1,123,533	218,978,678,800
    # 7	2025/11/14	199,980	2	-4,850	-2.37	203,000	203,770	199,240	1,072,925	217,020,603,320
    # 8	2025/11/13	204,830	1	4,460	2.23	206,290	206,300	203,350	884,953	180,140,812,950
    # 9	2025/11/12	200,370	1	1,140	0.57	200,000	202,220	199,450	913,622	183,275,103,110
    # 10	2025/11/11	199,230	1	3,550	1.81	199,500	200,140	197,430	1,084,552	215,164,666,400
    # 11	2025/11/10	195,680	1	1,000	0.51	196,750	197,400	193,720	1,071,682	209,544,414,510
    # 12	2025/11/07	194,680	1	2,690	1.40	192,110	197,600	192,110	1,027,700	200,027,514,400
