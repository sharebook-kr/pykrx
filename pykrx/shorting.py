from pykrx.core import *
from pandas import DataFrame
import numpy as np


class ShortHttp(KrxHttp):
    @property
    def otp_url(self):
        return "http://short.krx.co.kr/contents/COM/GenerateOTP.jspx"

    @property
    def contents_url(self):
        return "http://short.krx.co.kr/contents"



class KrxShortComprehensiveStatus(ShortHttp, Singleton):
    '''
    @Brief : 공매도 종합 포털 - 종목별 공매도 종합 현황
     - http://short.krx.co.kr/contents/SRT/02/02010100/SRT02010100.jsp
    @Param :
     - isu_cd : 조회할 종목의 ISIN 번호
     - strd_dd : 조회 시작 일자 (YYYYMMDD)
     - end_dd : 조회 마지막 일자 (YYYYMMDD)
    '''

    @property
    def bld(self):
        return "SRT/02/02010100/srt02010100"

    @property
    def uri(self):
        return "/SRT/99/SRT99000001.jspx"


class KrxShortInvestorStatus(ShortHttp, Singleton):
    '''
    @Brief : 공매도 종합 포털 - 투자자별 공매도 종합 현황
     - http://short.krx.co.kr/contents/SRT/02/02010100/SRT02010100.jsp
    @Param :
     - mkt_tp_cd : 코스피 (1) / 코스닥 (2)
     - inqCondTpCd : 거래량조회 (1) / 거래대금조회(2)
     - strd_dd : 조회 시작 일자 (YYYYMMDD)
     - end_dd : 조회 마지막 일자 (YYYYMMDD)
    '''

    @property
    def bld(self):
        return "SRT/02/02020300/srt02020300"

    @property
    def uri(self):
        return "/SRT/99/SRT99000001.jspx"


class KrxShortVolume30(ShortHttp, Singleton):
    '''
    @Brief : 공매도 종합 포털 - 공매도 상위 30
     - http://short.krx.co.kr/contents/SRT/02/02010100/SRT02010100.jsp
    @Param :
     - mkt_tp_cd : 코스피 (1) / 코스닥 (2)
     - schdate : 조회 일자 (YYYYMMDD)
    '''

    @property
    def bld(self):
        return "SRT/02/02020400/srt02020400"

    @property
    def uri(self):
        return "/SRT/99/SRT99000001.jspx"


def get_shorting_comprehensive_status(isin, name, fromdate, todate):
    try :
        result = KrxShortComprehensiveStatus().post(isu_cd=isin, strt_dd=fromdate, end_dd=todate)
        # DataFrame으로 저장
        df = DataFrame(result['block1'][2:])
        df = df[['trd_dd', 'cvsrtsell_trdvol', 'str_const_val1', 'cvsrtsell_trdval', 'str_const_val2']]
        df.columns = ['날짜', '거래량', '잔고', '공매금액', '잔고금액']

        df.set_index('날짜', inplace=True)
        df = df.replace({',': ''}, regex=True).astype(np.int64)
        df.index.name = name

        repay = df['잔고'].shift(-1) + df['거래량'] - df['잔고']
        df['상환'] = repay
        return df
    except (IndexError, KeyError):
        return None


def get_shorting_investor_status(fromdate, todate, market="코스피", inquery="거래량"):
    try :
        kor2mkt = {"코스피": "1", "코스닥": "2"}
        kor2inq = {"거래량": "1", "거래대금": "2"}
        result = KrxShortInvestorStatus().post(mkt_tp_cd=kor2mkt.get(market, "1"),
                                               inqCondTpCd=kor2inq.get(inquery, "1"),
                                               strt_dd=fromdate, end_dd=todate)
        # DataFrame으로 저장
        df = DataFrame(result['block1'])
        df = df[['str_const_val1', 'str_const_val2', 'str_const_val3', 'str_const_val4', 'str_const_val5', 'trd_dd']]
        df.columns = ['기관', '개인', '외국인', '기타', '합계', '날짜']

        df.set_index('날짜', inplace=True)
        df = df.replace({',': ''}, regex=True).astype(np.int64)
        df.index.name = market + inquery
        return df
    except (IndexError, KeyError):
        return None


def get_shorting_volume30(date, market="코스피"):
    try :
        kor2mkt = {"코스피": "1", "코스닥": "2"}
        result = KrxShortVolume30().post(mkt_tp_cd=kor2mkt.get(market, "1"), schdate=date)
        # DataFrame으로 저장
        df = DataFrame(result['block1'])
        df = df[['rank', 'isu_abbrv', 'cvsrtsell_trdval', 'acc_trdval', 'tdd_srtsell_wt',
                 'srtsell_trdval_avg','tdd_srtsell_trdval_incdec_rt', 'valu_pd_avg_srtsell_wt', 'srtsell_rto', 'prc_yd']]
        df.columns = ['순위', '종목명', '공매도거래대금', '총거래대금', '공매도비중', '직전40일거래대금평균',
                      '공매도거래대금증가율', '직전40일공매도평균비중', '공매도비중증가율', '주가수익률']
        df.set_index('종목명', inplace=True)
        df[["순위", "공매도거래대금", "총거래대금", "직전40일거래대금평균"]] = \
            df[["순위", "공매도거래대금", "총거래대금", "직전40일거래대금평균"]].replace({',': ''}, regex=True).astype(np.int64)
        df[["공매도비중", "공매도거래대금증가율", "직전40일공매도평균비중", "공매도비중증가율", "주가수익률"]] = \
            df[["공매도비중", "공매도거래대금증가율", "직전40일공매도평균비중", "공매도비중증가율", "주가수익률"]].\
                astype(float)
        df.index.name = market + date
        return df
    except (IndexError, KeyError):
        return None


if __name__ == "__main__":
    import pandas as pd
    pd.set_option('display.width', None)

    # print(get_shorting_investor_status("20181113", "20181213"))
    # print(get_shorting_investor_status("20181113", "20181213", market="코스닥"))
    # print(get_shorting_investor_status("20181113", "20181213", inquery="거래대금"))

    print(get_shorting_volume30("20181213"))