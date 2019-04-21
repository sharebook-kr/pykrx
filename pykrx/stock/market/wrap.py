from pykrx.comm import dataframe_empty_handler
from pykrx.stock.market.core import (MKD30040, MKD80037, MKD30009_0, 
                                     MKD30009_1)
import numpy as np


@dataframe_empty_handler
def get_market_ohlcv_by_date(fromdate, todate, isin):
    """일자별 OHLCV
    :param fromdate: 조회 시작 일자   (YYYYMMDD)
    :param todate  : 조회 마지막 일자 (YYYYMMDD)
    :param isin    : 조회 종목의 ISIN
    :return        : OHLCV DataFrame
                     시가     고가    저가    종가   거래량
        20180208     97200   99700   97100   99300   813467
        20180207     98000  100500   96000   96500  1082264
        20180206     94900   96700   93400   96100  1094871
        20180205     99400   99600   97200   97700   745562
    """
    df = MKD30040().read(fromdate, todate, isin)

    df = df[['trd_dd', 'tdd_opnprc', 'tdd_hgprc', 'tdd_lwprc',
             'tdd_clsprc', 'acc_trdvol']]
    df.columns = ['날짜', '시가', '고가', '저가', '종가', '거래량']
    df = df.replace('/', '', regex=True)
    df = df.replace(',', '', regex=True)    
    df = df.set_index('날짜')
    df = df.astype(np.int32)
    return df.sort_index()


@dataframe_empty_handler
def get_market_price_change_by_ticker(fromdate, todate, market="ALL"):
    """
    :param fromdate: 조회 시작 일자 (YYYYMMDD)
    :param todate  : 조회 종료 일자 (YYYYMMDD)
    :param market  : 조회 시장      (KOSPI/KOSDAQ/ALL)
    :return        : DataFrame
              종목명     시가    종가   대비   등락률   거래량      거래대금
    티커
    000020   동화약품   11550   11250   -300    -2.60   1510666   16851737550
    000030   우리은행   16050   15400   -650    -4.05  11623346  181243425100
    000040   KR모터스     667     717     50     7.50   9521456    6506765090
    000050       경방   14900   14200   -700    -4.70    526376    7608850250
    000060  메리츠화재  20950   20350   -600    -2.86   1094745   22498470632
    """
    market = {"ALL": "ALL", "KOSPI": "STK", "KOSDAQ": "KSQ", "KONEX": "KNX"}.\
        get(market, "ALL")    
    df = MKD80037().read(market, fromdate, todate)    
    

    df = df[['kor_shrt_isu_nm', 'isu_cd', 'opn_dd_end_pr', 'end_dd_end_pr',
             'prv_dd_cmpr', 'updn_rate', 'isu_tr_vl', 'isu_tr_amt']]
    df.columns = ['종목명', '티커', '시가', '종가', '변동폭',
                  '등락률', '거래량', '거래대금']
    df = df.set_index('티커')

    df = df.replace(',', '', regex=True)
    df = df.astype({"시가": np.int32, "종가": np.int32,
                    "변동폭": np.int32, "등락률": np.float32,
                    "거래량": np.int64, "거래대금": np.int64})
    return df


@dataframe_empty_handler
def get_market_fundamental_by_ticker(date, market="ALL"):
    """일자별 BPS/PER/PBR/배당수익률
    :param date    : 조회 일자 (YYYYMMDD)
                      20000101 이후의 데이터 제공
    :param market  : 조회 시장 (KOSPI/KOSDAQ/ALL)
    :return        : DataFrame
                       종목명   DIV    BPS      PER   EPS
        000250     삼천당제약  0.27   5689    44.19   422
        000440   중앙에너비스  2.82  37029    24.98  1135
        001000       신라섬유     0    563   247.27    11
        001540       안국약품  1.75  10036    84.00   150
        001810         무림SP  1.07   8266    24.06   117
    """
    market = {"ALL": "ALL", "KOSPI": "STK", "KOSDAQ": "KSQ", "KONEX": "KNX"}.\
        get(market, "ALL")    
    df = MKD30009_0().read(date, market)

    df = df[['isu_nm', 'isu_cd', 'dvd_yld', 'bps', 'per', 'prv_eps']]
    df.columns = ['종목명', '티커', 'DIV', 'BPS', 'PER', 'EPS']
    df.set_index('티커', inplace=True)

    df = df.replace('-', '0', regex=True)
    df = df.replace('', '0', regex=True)
    df = df.replace(',', '', regex=True)
    df = df.astype({"종목명": str, "DIV": np.float32, "BPS": np.int32,
                    "PER": np.float32, "EPS": np.int32}, )
    return df


@dataframe_empty_handler
def get_market_fundamental_by_date(fromdate, todate, isin, market="ALL"):
    """일자별 BPS/PER/PBR/배당수익률
    :param date    : 조회 일자 (YYYYMMDD)
                      20000101 이후의 데이터 제공
    :param market  : 조회 시장 (KOSPI/KOSDAQ/ALL)
    :return        : DataFrame
                       종목명   DIV    BPS      PER   EPS
        000250     삼천당제약  0.27   5689    44.19   422
        000440   중앙에너비스  2.82  37029    24.98  1135
        001000       신라섬유     0    563   247.27    11
        001540       안국약품  1.75  10036    84.00   150
        001810         무림SP  1.07   8266    24.06   117
    """
    market = {"ALL": "ALL", "KOSPI": "STK", "KOSDAQ": "KSQ", "KONEX": "KNX"}.\
        get(market, "ALL")    
    df = MKD30009_1().read(fromdate, todate, market, isin)
    df = df[['work_dt', 'dvd_yld', 'bps', 'per', 'prv_eps']]
    df.columns = ['날짜', 'DIV', 'BPS', 'PER', 'EPS']
    
    df = df.replace('-', '0', regex=True)
    df = df.replace('/', '', regex=True)
    df = df.replace('', '0', regex=True)
    df = df.replace(',', '', regex=True)
    df = df.astype({"DIV": np.float32, "BPS": np.int32,
                    "PER": np.float32, "EPS": np.int32}, )
    df.set_index('날짜', inplace=True)
    return df.sort_index()


if __name__ == "__main__":
    import pandas as pd
    pd.set_option('display.expand_frame_repr', False)
    df = get_market_fundamental_by_ticker("20190401", "ALL")
    # df = get_market_ohlcv_by_date("20150720", "20150810", "KR7005930003")
    # df = get_market_fundamental_by_date("20150720", "20150810", "KR7005930003")
    print(df)
    
