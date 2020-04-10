from pykrx.website.comm import dataframe_empty_handler
from pykrx.website.krx.e3.etf.core import (MKD60007, MKD60015, MKD80118, MKD80117)
from pykrx.website.krx.e3.etf.ticker import EtfTicker
import numpy as np
import decimal
import pandas as pd


@dataframe_empty_handler
def get_etf_ohlcv_by_date(fromdate, todate, ticker):
    """일자별 OHLCV
    :param fromdate: 조회 시작 일자 (YYYYMMDD)
    :param todate  : 조회 종료 일자 (YYYYMMDD)
    :param isin    : 조회 종목의 티커
    :return        : OHLCV DataFrame
                     시가     고가    저가    종가   거래량
        20180208     97200   99700   97100   99300   813467
        20180207     98000  100500   96000   96500  1082264
        20180206     94900   96700   93400   96100  1094871
        20180205     99400   99600   97200   97700   745562
    """
    isin = EtfTicker().get_isin(ticker)
    df = MKD60007().read(fromdate, todate, isin)
    df = df[['work_dt', 'last_nav', 'isu_opn_pr', 'isu_hg_pr', 'isu_lw_pr',
             'isu_end_pr', 'tot_tr_vl', 'tot_tr_amt', 'last_indx']]
    df.columns = ['날짜', 'NAV', '시가', '고가', '저가', '종가', '거래량',
                  '거래대금', '기초지수']
    df = df.replace('/', '', regex=True)
    df = df.replace(',', '', regex=True)
    df = df.set_index('날짜')
    df = df.astype({"NAV": np.float, "시가": np.int32, "고가": np.int32,
                    "저가": np.int32, "종가": np.int32, "거래량": np.int64,
                    "거래대금": np.int64, "기초지수": np.float})
    df['거래대금'] = df['거래대금'] * 1000000
    df.index = pd.to_datetime(df.index, format='%Y%m%d')
    return df.sort_index()


@dataframe_empty_handler
def get_etf_portfolio_deposit_file(ticker, date):
    """종목의 PDF 조회
    :param date: 조회 일자 (YYMMDD)
    :param isin: 조회 종목의 ISIN
    :return: PDF DataFrame

                   계약수       금액   비중
        삼성전자     8446  377113900  26.54
        SK하이닉스   1004   74496800   5.24
        셀트리온      176   31856000   2.24
        POSCO         123   31119000   2.19
        신한지주      731   30702000   2.16
        현대차        252   30114000   2.12

    """
    isin = EtfTicker().get_isin(ticker)
    df = MKD60015().read(date, isin)
    df = df[['isu_kor_nm', 'cu1_shrs', 'compst_amt', 'compst_amt_rt']]
    df.columns = ['종목', '계약수', '금액', '비중']
    df = df.set_index('종목')
    df = df.replace(',', '', regex=True)
    df = df.replace('-', '0', regex=True)
    # - empty string은 int, float로 형변환 불가
    #   -> 이 문제를 해결하기 위해 공백 문자는 0으로 치환
    # - 7.00 과 같은 문자열은 int, float로 형변환 불가
    #   -> 이 문제를 해결하기 위해 Decimal 클래스를 활용
    df = df.applymap(lambda x: 0 if x == "" else decimal.Decimal(x))
    df = df.astype({"계약수": np.int32, "금액": np.int64, "비중": np.float32 })
    return df


@dataframe_empty_handler
def get_etf_price_deviation(fromdate, todate, ticker):
    """
    괴리율 추이
    :param fromdate: 조회 시작 일자 (YYYYMMDD)
    :param todate  : 조회 종료 일자 (YYYYMMDD)
    :param ticker: ETF의 티커
    :return 괴리율이 포함된 DataFrame

                    종가       NAV    괴리율
        날짜
        2020-01-02  8285  8302.580078 -0.21
        2020-01-03  8290  8297.889648 -0.10
        2020-01-06  8150  8145.950195  0.05
        2020-01-07  8220  8226.049805 -0.07
        2020-01-08  7980  7998.839844 -0.24

    """
    isin = EtfTicker().get_isin(ticker)
    df = MKD80118().read(fromdate, todate, isin)
    df = df[['work_dt', 'isu_end_pr', 'last_nav', 'diff_rt_9']]
    df.columns = ['날짜', '종가', 'NAV', '괴리율']
    df = df.set_index('날짜')
    df = df.replace(',', '', regex=True)

    df = df.astype({"종가": np.int32, "NAV": np.float32, "괴리율": np.float32})
    df.index = pd.to_datetime(df.index, format='%Y/%m/%d')
    return df.sort_index()


@dataframe_empty_handler
def get_etf_tracking_error(fromdate, todate, ticker):
    """
    추적 오차율 추이
    :param fromdate: 조회 시작 일자 (YYYYMMDD)
    :param todate  : 조회 종료 일자 (YYYYMMDD)
    :param ticker: ETF의 티커
    :return 추적오차가 포함된 DataFrame

                       NAV           지수   추적오차
        날짜
        2020-01-02  8302.580078  1819.109985  0.32
        2020-01-03  8297.889648  1818.130005  0.32
        2020-01-06  8145.950195  1784.719971  0.32
        2020-01-07  8226.049805  1802.339966  0.32
        2020-01-08  7998.839844  1752.359985  0.32

    """
    isin = EtfTicker().get_isin(ticker)
    df = MKD80117().read(fromdate, todate, isin)
    df = df[['work_dt', 'mktd_nav', 'trc_tgt_indx', 'trc_err_rt']]
    df.columns = ['날짜', 'NAV', '지수', '추적오차']
    df = df.set_index('날짜')
    df = df.replace(',', '', regex=True)
    df = df.astype({"NAV": np.float32, "지수": np.float32, "추적오차": np.float32})
    df.index = pd.to_datetime(df.index, format='%Y/%m/%d')
    return df.sort_index()


if __name__ == "__main__":
    import pandas as pd
    pd.set_option('display.width', None)
    # df = get_etf_ohlcv_by_date("20200101", "20200401", "295820")
    # df = get_etf_portfolio_deposit_file("252650", "20190329")
    # df = get_etf_price_deviation("20200101", "20200401", "295820")
    # df = get_etf_tracking_error("20200101", "20200401", "295820")
    # print(df)


