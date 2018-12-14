from pykrx.core import *
from pandas import DataFrame
from datetime import datetime
import numpy as np
import time


class ShortHttp(KrxHttp):
    @property
    def otp_url(self):
        return "http://short.krx.co.kr/contents/COM/GenerateOTP.jspx"

    @property
    def contents_url(self):
        return "http://short.krx.co.kr/contents"


class SRT02010100(ShortHttp, Singleton):
    # @Brief : 공매도 종합 현황
    # - http://short.krx.co.kr/contents/SRT/02/02010100/SRT02010100.jsp

    @property
    def bld(self):
        return "SRT/02/02010100/srt02010100"

    @property
    def uri(self):
        return "/SRT/99/SRT99000001.jspx"

    @staticmethod
    def scraping(isin, fromdate, todate):
        try:
            result = SRT02010100().post(isu_cd=isin, strt_dd=fromdate, end_dd=todate)

            # (T+2)일 이전의 제공하기 때문에 (T), (T+1)의 비어있는 데이터를 제거
            elapsedTime = datetime.now() - datetime.strptime(todate+"0900", '%Y%m%d%H%M')
            day_offset = 2 - min(int(elapsedTime.total_seconds()/3600/24), 2)

            # DataFrame 생성
            df = DataFrame(result['block1'][day_offset:])
            df = df[['trd_dd', 'cvsrtsell_trdvol', 'str_const_val1', 'cvsrtsell_trdval', 'str_const_val2']]
            df.columns = ['날짜', '공매도', '잔고', '공매도금액', '잔고금액']

            # Index 설정
            df.set_index('날짜', inplace=True)
            df.index.name = result['block1'][0]['isu_abbrv']
            df = df.replace({',': ''}, regex=True).astype(np.int64)

            # 상환량 추가
            repay = df['잔고'].shift(-1) + df['공매도'] - df['잔고']
            repay.iloc[-1] = 0
            df['상환'] = repay.astype(np.int64)
            df = df[['공매도', '상환', '잔고', '공매도금액', '잔고금액']]

            return df
        except (IndexError, KeyError):
            return None


class SRT02020100(ShortHttp, Singleton):
    # @Brief : 종목별 공매도 거래 현황
    # - http://short.krx.co.kr/contents/SRT/02/02020100/SRT02020100.jsp

    @property
    def bld(self):
        return "SRT/02/02020100/srt02020100"

    @property
    def uri(self):
        return "/SRT/99/SRT99000001.jspx"

    @staticmethod
    def scraping(fromdate, todate, isin, market):
        try:
            market_idx = {"코스피": 1, "코스닥": 3, "코넥스": 4}.get(market, 1)
            req_idx = 1
            df = None

            while True:
                if isin == None:
                    result = SRT02020100().post(mkt_tp_cd=market_idx, strt_dd=fromdate, end_dd=todate, curPage=req_idx)
                else:
                    result = SRT02020100().post(mkt_tp_cd=market_idx, isu_cd=isin, strt_dd=fromdate, end_dd=todate,
                                                curPage=req_idx)
                if df is None:
                    df = DataFrame(result['block1'])
                else:
                    df = df.append(DataFrame(result['block1']))

                # 한 번에 500개 씩 데이터를 조회
                if req_idx == int((int(result['block1'][0]["totCnt"]) + 499) / 500):
                    break
                req_idx += 1
                time.sleep(0.3)

            df = df[['trd_dd', 'isu_abbrv', 'cvsrtsell_trdvol', 'acc_trdvol', 'trdvol_wt']]
            df.columns = ['날짜', '종목명', '공매도수량', '총거래량', '거래량비중']

            df.set_index('날짜', inplace=True)
            index_name = "전체" if isin is None else isin[3:9]
            df.index.name = "{}({}-{})".format(index_name, fromdate, todate)

            for key in df.columns[1:3]:
                df[key] = df[key].replace({',': ''}, regex=True).astype(np.int64)
            df['거래량비중'] = df['거래량비중'].astype(np.float)

            return df
        except (IndexError, KeyError):
            return None


class SRT02020300(ShortHttp, Singleton):
    # @Brief : 투자자별 공매도 종합 현황
    # - http://short.krx.co.kr/contents/SRT/02/02020300/SRT02020300.jsp

    @property
    def bld(self):
        return "SRT/02/02020300/srt02020300"

    @property
    def uri(self):
        return "/SRT/99/SRT99000001.jspx"

    @staticmethod
    def scraping(fromdate, todate, market, inquery):
        try:
            index_name = "{}-{}".format(market, inquery)
            market = {"코스피": 1, "코스닥": 2, "코넥스": 6}.get(market, 1)
            inquery = {"거래대금": 1, "거래량": 2}.get(inquery, 1)

            result = SRT02020300().post(mkt_tp_cd=market, inqCondTpCd=inquery, strt_dd=fromdate, end_dd=todate)

            df = DataFrame(result['block1'])
            df = df[['str_const_val1', 'str_const_val2', 'str_const_val3', 'str_const_val4', 'str_const_val5', 'trd_dd']]
            df.columns = ['기관', '개인', '외국인', '기타', '합계', '날짜']

            df.set_index('날짜', inplace=True)
            df = df.replace({',': ''}, regex=True).astype(np.int64)
            df.index.name = index_name
            return df
        except (IndexError, KeyError):
            return None


class SRT02020400(ShortHttp, Singleton):
    # @Brief : 공매도 거래비중 상위 50 종목
    #  - http://short.krx.co.kr/contents/SRT/02/02010100/SRT02010100.jsp

    @property
    def bld(self):
        return "SRT/02/02020400/srt02020400"

    @property
    def uri(self):
        return "/SRT/99/SRT99000001.jspx"

    @staticmethod
    def scraping(date, market):
        try:
            market_idx = {"코스피": 1, "코스닥": 2, "코넥스": 6}.get(market, 1)
            result = SRT02020400().post(mkt_tp_cd=market_idx, schdate=date)

            df = DataFrame(result['block1'])
            df = df[['isu_abbrv', 'rank', 'cvsrtsell_trdval', 'acc_trdval', 'tdd_srtsell_wt',
                     'srtsell_trdval_avg','tdd_srtsell_trdval_incdec_rt', 'valu_pd_avg_srtsell_wt', 'srtsell_rto', 'prc_yd']]
            df.columns = ['종목명', '순위', '공매도거래대금', '총거래대금', '공매도비중', '직전40일거래대금평균',
                          '공매도거래대금증가율', '직전40일공매도평균비중', '공매도비중증가율', '주가수익률']
            
            df.set_index('종목명', inplace=True)
            df.index.name = "{}({})".format(market, date)

            for key in ["순위", "공매도거래대금", "총거래대금", "직전40일거래대금평균"]:
                df[key] = df[key].replace({',': ''}, regex=True).astype(np.int64)
            for key in ["공매도비중", "공매도거래대금증가율", "직전40일공매도평균비중", "공매도비중증가율", "주가수익률"]:
                df[key] = df[key].replace({',': ''}, regex=True).astype(np.float)

            return df
        except (IndexError, KeyError):
            return None


if __name__ == "__main__":
    import pandas as pd
    pd.set_option('display.width', None)

    # print(SRT02010100.scraping("KR7005930003", "20181205", "20181207"))
    # print(SRT02020100.scraping("20181207", "20181212", "KR7005930003", 1))
    # print(SRT02020300.scraping("20181207", "20181212", "코스피", "거래대금"))
    print(SRT02020400.scraping("20181212", "코스피"))

    # print(get_shorting_investor_status("20181113", "20181213"))
    # print(get_shorting_investor_status("20181113", "20181213", market="코스닥"))
    # print(get_shorting_investor_status("20181113", "20181213", inquery="거래대금"))
    # print(get_shorting_volume30("20181213"))
    # print(get_shorting_indivisual_status("20181213", "20181213", "코스피"))