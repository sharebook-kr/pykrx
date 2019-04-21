from pykrx.comm.util import dataframe_empty_handler, singleton
from pykrx.e3.etf.core import (MKD60003, MKD60007, MKD60015)
import numpy as np
import datetime


@singleton
class _EtfTicker:
    def __init__(self):
        closest_business_day = _get_closest_business_day()
        self.df = self._get(closest_business_day)
        self.dd = {}       
        self.dd[closest_business_day] = self.df
        
    @dataframe_empty_handler    
    def _get(self, date):
        df = MKD60003().read(date)
        df = df.set_index('isu_cd')['isu_abbrv']
        return df
        
    @dataframe_empty_handler
    def get_ticker(self, date):
        df = self.dd.get(date, None)
        if df is None:            
            df = self._get(date)
            self.dd[date] = df            
            self.df.update(df)            
            self.df.drop_duplicates()
        return df.to_list()
    
    def get_isin(self, ticker):        
        df = self.df.index[self.df == ticker]
        if len(df) != 0:
            return df[0]
        return None


def _get_etf_ticker_to_isin(ticker):    
    return _EtfTicker().get_isin(ticker)


def get_etf_ticker_list(date=None):
    if date is None:
        now = datetime.datetime.now()
        if now.hour < 16:
            now = now - datetime.timedelta(days=1)
        date = now.strftime("%Y%m%d")
    return _EtfTicker().get_ticker(date)


def _get_closest_business_day():
    now  = datetime.datetime.now()
    past = now - datetime.timedelta(days=14)
    df = MKD60007().read(past.strftime("%Y%m%d"), now.strftime("%Y%m%d"), 
                  "KR7069500007")      
    return df['work_dt'].iloc[0].replace("/", "")


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
    isin = _get_etf_ticker_to_isin(ticker)    
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
    return df.sort_index()


@dataframe_empty_handler
def get_etf_portfolio_deposit_file(date, ticker):
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
    isin = _get_etf_ticker_to_isin(ticker)  
    df = MKD60015().read(date, isin)
    df = df[['isu_kor_nm', 'cu1_shrs', 'compst_amt', 'compst_amt_rt']]
    df.columns = ['종목', '계약수', '금액', '비중']
    df = df.replace(',', '', regex=True)
    df = df.replace('-', '0', regex=True)
    df = df.set_index('종목')
    df["계약수"] = df["계약수"].astype(np.float).astype(np.int32)
    df = df.astype({"금액": np.int64, "비중": np.float})
    return df


if __name__ == "__main__":
    import pandas as pd
    pd.set_option('display.width', None)
    df = get_etf_ticker_list()        
    # df = get_etf_ohlcv_by_date("20120401", "20190329", "KOSEF 저PBR가중")
    # df = get_etf_portfolio_deposit_file("20190329", "ARIRANG 200동일가중")
    print(df)
