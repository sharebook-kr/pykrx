from pykrx.comm.http import KrxHttp
from pandas import DataFrame
import time


class ShortHttp(KrxHttp):
    @property
    def otp_url(self):
        return "http://short.krx.co.kr/contents/COM/GenerateOTP.jspx"

    @property
    def contents_url(self):
        return "http://short.krx.co.kr/contents"

    @property
    def uri(self):
        return "/SRT/99/SRT99000001.jspx"


class SRT02010100(ShortHttp):
    @property
    def bld(self):
        return "SRT/02/02010100/srt02010100"

    @staticmethod
    def read(fromdate, todate, isin):
        """02010100 공매도 종합 현황
           http://short.krx.co.kr/contents/SRT/02/02010100/SRT02010100.jsp
        :param fromdate: 조회 시작 일자 (YYMMDD)
        :param todate: 조회 마지막 일자 (YYMMDD)
        :param isin:
        :return: 공매도 종합 현황 DataFrame
        """
        result = SRT02010100().post(isu_cd=isin, strt_dd=fromdate, end_dd=todate)
        return DataFrame(result['block1'])


class SRT02020100(ShortHttp):
    @property
    def bld(self):
        return "SRT/02/02020100/srt02020100"

    @staticmethod
    def read(fromdate, todate, market=1, isin=""):
        """02020100 공매도 거래 현황
           http://short.krx.co.kr/contents/SRT/02/02020100/SRT02020100.jsp
        :param fromdate: 조회 시작 일자 (YYMMDD)
        :param todate: 조회 마지막 일자 (YYMMDD)
        :param market: 1 (코스피) / 3 (코스닥) / 4 (코넥스)
        :param isin: 종목의 ISIN 값 - 입력하지 않을 경우 전체 종목 검색
        :return:  종목별 공매도 거래 현황 DataFrame
        """
        df = DataFrame()
        page = 1
        while True:
            result = SRT02020100().post(mkt_tp_cd=market, isu_cd=isin, strt_dd=fromdate, end_dd=todate, curPage=page)
            if len(result['block1']) == 0 :
                return df
            df = df.append(DataFrame(result['block1']))            
            # exit condition            
            load_data_idx = int(result['block1'][-1]['rn'])
            total_data_cnt = int(result['block1'][0]['totCnt'])
            if load_data_idx == total_data_cnt:
                break

            page += 1
            time.sleep(0.2)
        return df


class SRT02020300(ShortHttp):
    @property
    def bld(self):
        return "SRT/02/02020300/srt02020300"

    @staticmethod
    def read(fromdate, todate, market=1, inquery=1):
        """02020300 공매도 거래 현황
           http://short.krx.co.kr/contents/SRT/02/02020300/SRT02020300.jsp
        :param fromdate: 조회 시작 일자 (YYMMDD)
        :param todate  : 조회 마지막 일자 (YYMMDD)
        :param market  : 1 (코스피) / 3 (코스닥) / 6 (코넥스)
        :param inquery : 1 (거래대금) / 2 (거래량)
        :return: 투자자별 공매도 거래 현황 DataFrame
           str_const_val1 str_const_val2 str_const_val3 str_const_val4 str_const_val5      trd_dd
        0       1,161,522         37,396      6,821,963              0      8,020,881  2018/01/19
        1         970,406         41,242      8,018,997         13,141      9,043,786  2018/01/18
        2       1,190,006         28,327      8,274,090          6,465      9,498,888  2018/01/17
        """        
        result = SRT02020300().post(mkt_tp_cd=market, inqCondTpCd=inquery, strt_dd=fromdate, end_dd=todate)
        return DataFrame(result['block1'])


class SRT02020400(ShortHttp):
    @property
    def bld(self):
        return "SRT/02/02020400/srt02020400"

    @staticmethod
    def read(date, market=1):
        """02020400 공매도 거래 현황
           http://short.krx.co.kr/contents/SRT/02/02010100/SRT02010100.jsp
        :param date  : 조회 일자 (YYMMDD)
        :param market: 1 (코스피) / 2 (코스닥) / 6 (코넥스)
        :return: 공매도 거래비중 상위 50 종목 DataFrame
               acc_trdval      bas_dd cvsrtsell_trdval isu_abbrv        isu_cd  prc_yd rank srtsell_rto srtsell_trdval_avg tdd_srtsell_trdval_incdec_rt tdd_srtsell_wt valu_pd_avg_srtsell_wt
            0  35,660,149,500  2018/01/05   15,217,530,000     아모레퍼시픽  KR7090430000   0.334    1       2.877      7,945,445,875        1.915         42.674         14.834
            1     176,886,900  2018/01/05       69,700,600   영원무역홀딩스  KR7009970005   2.698    2       4.259         20,449,658        3.408         39.404          9.251
            2  27,690,715,500  2018/01/05    9,034,795,500             한샘  KR7009240003  -5.233    3       1.543      2,131,924,250        4.238         32.628         21.142
            3   2,444,863,350  2018/01/05      701,247,550             동서  KR7026960005  -0.530    4       2.820        255,763,771        2.742         28.682         10.172
        """
        result = SRT02020400().post(mkt_tp_cd=market, schdate=date)
        return DataFrame(result['block1'])


class SRT02030100(ShortHttp):
    @property
    def bld(self):
        return "SRT/02/02030100/srt02030100"

    @staticmethod
    def read(fromdate, todate, market=1, isin=""):
        """02030100 공매도 잔고 현황
           http://short.krx.co.kr/contents/SRT/02/02010100/SRT02010100.jsp
        :param fromdate: 조회 시작 일자 (YYMMDD)
        :param todate  : 조회 마지막 일자 (YYMMDD)
        :param market  : 1 (코스피) / 2 (코스닥) / 6 (코넥스)
        :param isin    : 조회 종목의 ISIN
        :return        : 종목별 공매도 잔고 현황 DataFrame
                  bal_amt  bal_qty bal_rto isu_abbrv        isu_cd    list_shrs              mktcap rn totCnt      trd_dd
            0  11,982,777,500  164,825    0.02    SK하이닉스  KR7000660001  728,002,365  52,925,771,935,500  1      7  2018/01/15
            1  12,427,999,200  167,043    0.02    SK하이닉스  KR7000660001  728,002,365  54,163,375,956,000  2      7  2018/01/12
            2  13,297,270,800  183,158    0.02    SK하이닉스  KR7000660001  728,002,365  52,852,971,699,000  3      7  2018/01/11
            3  14,594,580,000  200,200    0.03    SK하이닉스  KR7000660001  728,002,365  53,071,372,408,500  4      7  2018/01/10
        """
        df = DataFrame()
        page = 1

        while True:
            result = SRT02030100().post(mkt_tp_cd=market, strt_dd=fromdate, end_dd=todate,
                                        isu_cd=isin, curPage=page)
            df = df.append(DataFrame(result['block1']))

            # exit condition
            load_data_idx = int(result['block1'][-1]['rn'])
            total_data_cnt = int(result['block1'][0]['totCnt'])
            if load_data_idx == total_data_cnt:
                break

            page += 1
            time.sleep(0.2)
        return df


class SRT02030400(ShortHttp):
    @property
    def bld(self):
        return "SRT/02/02030400/srt02030400"

    @staticmethod
    def read(date, market=1):
        """02030400 공매도 잔고 현황
           http://short.krx.co.kr/contents/SRT/02/02020300/SRT02020300.jsp
        :param date  : 조회 일자 (YYMMDD)
        :param market: 1 (코스피) / 3 (코스닥) / 6 (코넥스)
        :return: 잔고 비중 상위 50 DataFrame
                       bal_amt    bal_qty bal_rto      isu_abbrv        isu_cd    list_shrs             mktcap rank rpt_duty_occr_dd    trd_dd
            0  190,835,680,350  5,323,171   10.12     한화테크윈  KR7012450003   52,600,000  1,885,710,000,000    1       2018/01/05  20180105
            1  161,456,413,600  2,570,962    9.45       현대위아  KR7011210002   27,195,083  1,707,851,212,400    2       2018/01/05  20180105
            2  147,469,396,800  9,131,232    8.58     두산중공업  KR7034020008  106,463,061  1,719,378,435,150    3       2018/01/05  20180105
            3  179,776,282,650  6,002,547    8.38         GS건설  KR7006360002   71,675,237  2,146,673,348,150    4       2018/01/05  20180105
        """
        result = SRT02030400().post(mkt_tp_cd=market, schdate=date)
        return DataFrame(result['block1'])


if __name__ == "__main__":
    import pandas as pd
    pd.set_option('display.width', None)

    # print(SRT02010100.read("KR7005930003", "20181205", "20181207"))
    print(SRT02020100.read("20190402", "20190402", market=1))
    # print(SRT02020100.read("20181207", "20181212", "코스피", "KR7005930003"))
    # print(SRT02020300.read("20181207", "20181212", "코스피", "거래대금"))
    # print(SRT02020400.read("20181212", "코스피"))

    # print(SRT02030100.read("20181212", "20181212", 1, "KR7210980009"))
    # print(SRT02030100.read("20181207", "20181212", "코스피", "KR7210980009"))

    # print(SRT02030400.read("20181214", 1))