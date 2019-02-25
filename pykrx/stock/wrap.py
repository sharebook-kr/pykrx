from pykrx.stock.core import MKD20011, MKD20011_KOSPI
from pykrx.stock.core import MKD30040, MKD30009
from pykrx.stock.core import MKD80037
from pykrx.comm.ticker import *
import pandas as pd
import numpy as np


class KrxMarket:
    def __init__(self):
        self.ticker = KrxTicker()

    @dataframe_empty_handler
    def get_market_ohlcv(self, fromdate, todate, ticker):
        """일자별 OHLCV
        :param fromdate: 조회 시작 일자   (YYYYMMDD)
        :param todate  : 조회 마지막 일자 (YYYYMMDD)
        :param ticker  : 종목 번호
        :return        : OHLCV DataFrame
                         시가     고가    저가    종가   거래량
            20180208     97200   99700   97100   99300   813467
            20180207     98000  100500   96000   96500  1082264
            20180206     94900   96700   93400   96100  1094871
            20180205     99400   99600   97200   97700   745562
        """
        isin = self.ticker.get_isin(ticker)
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
    def get_market_price_change(self, fromdate, todate):
        """
        :param fromdate: 조회 시작 일자 (YYYYMMDD)
        :param todate  : 조회 종료 일자 (YYYYMMDD)
        :return        : DataFrame
                  종목명  시작일기준가  종료일종가   대비   등락률   거래량      거래대금
        티커
        000020   동화약품       11550       11250   -300    -2.60   1510666   16851737550
        000030   우리은행       16050       15400   -650    -4.05  11623346  181243425100
        000040   KR모터스         667         717     50     7.50   9521456    6506765090
        000050       경방       14900       14200   -700    -4.70    526376    7608850250
        000060  메리츠화재      20950       20350   -600    -2.86   1094745   22498470632
        """
        market = {"코스피": "STK", "코스닥": "KSQ",
                  "코넥스": "KNX", "전체": "ALL"}.get("전체", "ALL")
        df = MKD80037().read(market, fromdate, todate)

        # MKD80037는 상장 폐지 종목은 제외한 정보를 전달하기 때문에,
        # 조회 일자에 상장 폐지 종목이 포함돼 있다면, 강제로 추가한다
        delist = self.ticker.get_delist(fromdate=fromdate, todate=todate)
        if len(delist) > 0:
            # 입력된 조회 일자가 주말일 경우, 가까운 평일로 변경
            df_for_business_day = self.get_market_ohlcv(fromdate,
                todate, "000020")
            fromdate = df_for_business_day.index[0]
            # 평일 하루의 OHLCV를 얻어와 시가를 가져온다
            tmp = MKD80037().read(market, fromdate, fromdate)
            delist = [x for x in delist if x in tmp['isu_cd'].values]
            for idx in delist:
                # 상폐 종목의 종가를 0으로 저장한다
                cond = tmp['isu_cd']==idx
                tmp.loc[cond, 'end_dd_end_pr'] = 0
                tmp.loc[cond, 'prv_dd_cmpr'  ] = '-' + tmp.loc[cond, 'opn_dd_end_pr']
                tmp.loc[cond, 'updn_rate'    ] = -100
                df = df.append(tmp[cond])

        df = df[['kor_shrt_isu_nm', 'isu_cd', 'opn_dd_end_pr', 'end_dd_end_pr',
                 'prv_dd_cmpr', 'updn_rate', 'isu_tr_vl', 'isu_tr_amt']]
        df.columns = ['종목명', '티커', '시가', '종가', '변동폭',
                      '등락률', '거래량', '거래대금']
        df = df.set_index('티커')

        df = df.replace(',', '', regex=True)
        df = df.astype({"시작일기준가": np.int32, "종료일종가": np.int32,
                        "대비": np.int32, "등락률": np.float32,
                        "거래량": np.int64, "거래대금": np.int64})
        return df

    @dataframe_empty_handler
    def get_market_index(self, date):
        """시장지수
        :param date: 조회 일자 (YYYYMMDD)
        :return    : 시장 지수 DataFrame
                                       기준시점    발표시점 기준지수 현재지수    시가총액
            코스피                   1983.01.04  1980.01.04   100.0   2486.35  1617634318
            코스피 벤치마크          2015.09.14  2010.01.04  1696.0   2506.92  1554948117
            코스피 비중제한 8% 지수  2017.12.18  2015.01.02  1000.0   1272.93  1559869409
            코스피 200               1994.06.15  1990.01.03   100.0    327.13  1407647304
            코스피 100               2000.03.02  2000.01.04  1000.0   2489.34  1277592989
            코스피 50                2000.03.02  2000.01.04  1000.0   2205.53  1102490712
        """
        df = MKD20011().read(date)

        df = df[['idx_nm', 'annc_tm', 'bas_tm', 'bas_idx', 'prsnt_prc',
                 'idx_mktcap']]
        df.columns = ['지수명', '기준시점', '발표시점', '기준지수', '현재지수',
                      '시가총액']
        df = df.set_index('지수명')

        df = df.replace(',', '', regex=True)
        df = df.replace('', 0)
        df = df.astype({"기준지수": float, "현재지수": float, "시가총액": int}, )
        return df

    @dataframe_empty_handler
    def get_market_index_change(self, fromdate, todate, market="코스피"):
        """
        :param fromdate: 조회 시작 일자 (YYYYMMDD)
        :param todate  : 조회 종료 일자 (YYYYMMDD)
        :param index   : 코스피/코스피 벤치마크/코스피 200/코스피 100/
                          코스피 50/코스피 대형주/코스피 중형주/코스피 소형주
        :return        : Kospi Index의 OHLCV DataFrame
                         시가지수     고가지수     저가지수     종가지수     거래량
            일자
            20190125  2147.919922  2178.010010  2146.639893  2177.729980  410002000
            20190128  2184.409912  2188.149902  2169.169922  2177.300049  371619000
            20190129  2172.830078  2183.360107  2162.530029  2183.360107  552587000
            20190130  2183.489990  2206.199951  2177.879883  2206.199951  480390000
            20190131  2222.879883  2222.879883  2201.219971  2204.850098  545248000

            종합지수 - 코스피          (001)
                           종합지수 - 코스피 벤치마크 (100)
                           대표지수 - 코스피 200      (028)
                           대표지수 - 코스피 100      (034)
                           대표지수 - 코스피 50       (035)
                           규모별   - 코스피 대형주   (002)
                           규모별   - 코스피 중형주   (003)
                           규모별   - 코스피 소형주   (004)
        """

        index = {"코스피"       : "001", "코스피 벤치마크": "100",
                  "코스피 200"   : "028", "코스피 100"     : "034",
                  "코스피 50"    : "035", "코스피 대형주"  : "002",
                  "코스피 중형주": "003", "코스피 소형주"  : "004"}.get(market)
        df = MKD20011_KOSPI().read(fromdate, todate, index)
        df = df[['trd_dd', 'opnprc_idx', 'hgprc_idx', 'lwprc_idx',
                 'clsprc_idx', 'acc_trdvol']]
        df.columns = ['일자', '시가지수', '고가지수', '저가지수',
                      '종가지수', '거래량']
        df = df.replace(',', '', regex=True)
        df = df.replace('/', '', regex=True)
        df = df.set_index('일자')
        df = df.astype({'시가지수': np.float32, '고가지수': np.float32,
                        '저가지수': np.float32, '종가지수': np.float32,
                        '거래량': np.int64})
        df['거래량'] = df['거래량'] * 1000
        return df

    @dataframe_empty_handler
    def get_market_status_by_date(self, date):
        """일자별 BPS/PER/PBR/배당수익률
        :param date : 조회 일자 (YYYYMMDD)
                       20000101 이후의 데이터 제공
        :return     : DataFrame
                           종목명   DIV    BPS      PER   EPS
            000250     삼천당제약  0.27   5689    44.19   422
            000440   중앙에너비스  2.82  37029    24.98  1135
            001000       신라섬유     0    563   247.27    11
            001540       안국약품  1.75  10036    84.00   150
            001810         무림SP  1.07   8266    24.06   117
        """
        market = {"코스피": "STK", "코스닥": "KSQ",
                  "코넥스": "KNX", "전체": "ALL"}.get("전체", "ALL")
        df = MKD30009().read(date, market)

        df = df[['isu_nm', 'isu_cd', 'dvd_yld', 'bps', 'per', 'prv_eps']]
        df.columns = ['종목명', '티커', 'DIV', 'BPS', 'PER', 'EPS']
        df.set_index('티커', inplace=True)

        df = df.replace('-', '0', regex=True)
        df = df.replace('', '0', regex=True)
        df = df.replace(',', '', regex=True)
        df = df.astype({"종목명": str, "DIV": np.float32, "BPS": np.int32,
                        "PER": np.float32, "EPS": np.int32}, )
        return df


if __name__ == "__main__":
    pd.set_option('display.width', None)
    km = KrxMarket()
    # df = km.get_market_ohlcv("20190201", "20190207", "066570")
    # df = km.get_market_price_change("20020605", "20020630")
    df = km.get_market_index_change("20190125", "20190225", "코스피 중형주")
    print(df)



