from pykrx.website.krx.krxio import KrxWebIo, KrxFileIo, SrtWebIo
import pandas as pd
from pandas import DataFrame
import time


################################################################################
# Market
class MKD30030(KrxWebIo):
    @property
    def bld(self):
        return "MKD/04/0406/04060200/mkd04060200"

    def fetch(self, date):
        """30030 상장 종목 검색
        :param date: 조회 일자 (YYMMDD)
        :return: 일자별 시세 조회 결과 DataFrame
                  bid_fst_qot_pr curr_iso fluc_tp_cd  isu_cd isu_cur_pr isu_hg_pr isu_lw_pr isu_opn_pr      isu_tr_amt isu_tr_vl kor_shrt_isu_nm    lst_stk_amt   lst_stk_vl ofr_fst_qot_pr par_pr prv_dd_cmpr totCnt updn_rate
        0          2,325   원(KRW)          2  060310      2,340     2,360     2,300      2,360     197,239,685    85,093                 3S    103,886,354,520   44,395,878          2,340    500          20   2382      0.85
        1          5,010   원(KRW)          1  095570      5,010     5,080     4,900      5,080     225,355,175    45,256          AJ네트웍스   234,579,697,950   46,822,295          5,020  1,000          30             0.60
        2         11,900   원(KRW)          1  068400     11,950    12,000    11,750     11,850   1,085,727,100    91,324           AJ렌터카    264,648,285,000   22,146,300         11,950    500          50             0.42
        3         54,100   원(KRW)          2  006840     54,200    55,300    53,400     55,300     669,714,100    12,367           AK홀딩스    718,017,806,200   13,247,561         54,200  5,000       1,100             1.99
        4          4,755   원(KRW)          2  054620      4,755     4,865     4,700      4,800     324,678,375    68,110          APS홀딩스     96,974,520,855   20,394,221          4,785    500         110             2.26
        """
        result = self.post(schdate=date, stock_gubun="on", secugrp="ST",
                           sect_tp_cd="ALL", marget_gubun="ALL")
        return DataFrame(result['상장종목검색'])


class MKD30040(KrxWebIo):
    @property
    def bld(self):
        return "MKD/04/0402/04020100/mkd04020100t3_02"

    def fetch(self, fromdate, todate, isin):
        """30040 일자별 시세 조회 (수정종가 아님)
        :param fromdate: 조회 시작 일자
        :param todate: 조회 마지막 일자
        :param isin: 조회할 종목의 ISIN 번호
        :return: 일자별 시세 조회 결과 DataFrame

            acc_trdval     acc_trdvol  fluc_tp  list_shrs    mktcap     tdd_clsprc tdd_cmpr tdd_hgprc tdd_lwprc  tdd_opnprc  trd_dd
        0   80,437,317,800    813,467       1  163,647,814  16,250,228     99,300    2,800    99,700    97,100     97,200  2018/02/08
        1  106,022,586,600  1,082,264       1  163,647,814  15,792,014     96,500      400   100,500    96,000     98,000  2018/02/07
        2  104,081,455,600  1,094,871       2  163,647,814  15,726,555     96,100    1,600    96,700    93,400     94,900  2018/02/06
        3   73,279,645,300    745,562       2  163,647,814  15,988,391     97,700    3,300    99,600    97,200     99,400  2018/02/05
        4   98,290,649,100    975,164       2  163,647,814  16,528,429    101,000    2,500   103,500    99,900    103,000  2018/02/02
        """
        result = self.post(isu_cd=isin, fromdate=fromdate, todate=todate)
        return DataFrame(result['block1'])


class MKD30015(KrxFileIo):
    @property
    def bld(self):
        return "MKD/04/0404/04040200/mkd04040200_01"

    def fetch(self, date, market):
        """30015 시가총액 상/하위
        :param date: 조회 시작 일자
        :param market: 조회 시장 (STK/KSQ/KNX/ALL)
        :return: 시가총액 DataFrame
            종목코드   종목명       현재가      대비   등락률        거래량             거래대금       시가       고가       저가                 시가총액  시가총액비중(%)        상장주식수      외국인 보유주식수  외국인 지분율(%)
        0   005930    삼성전자     45,050     400   0.9   7,362,129    332,300,460,800    45,200    45,450    44,850    268,938,703,877,500      15.88     5,969,782,550     3,392,279,209      56.82
        1   000660  SK하이닉스     76,600   2,400   3.2   3,579,070    273,797,653,100    75,600    77,100    75,500     55,764,981,159,000       3.29       728,002,365       369,317,688      50.73
        2   005935  삼성전자우     36,400    150     0.4   1,329,707     48,399,005,925    36,450    36,600    36,250     29,953,075,880,000       1.77       822,886,700       761,104,261      92.49
        3   051910    LG화학     370,500   4,500   1.2     170,894     63,586,807,000   367,000   376,000   366,500     26,154,463,081,500       1.54        70,592,343        27,377,609      38.78
        4   005380     현대차  	120,500   1,000   0.8     363,959     43,797,875,448   120,000   121,000   119,000     25,747,016,533,500       1.52       213,668,187        95,237,158      44.57
        """
        result = self.post(market_gubun=market, schdate=date)
        return pd.read_excel(result)


class MKD30009_0(KrxWebIo):
    @property
    def bld(self):
        return "MKD/13/1302/13020401/mkd13020401"

    def fetch(self, date, market):
        """30009 PER/PBR/배당수익률 (개별종목)
        :param date: 조회 일자 (YYMMDD)
        :param market: 조회 시장 (STK/KSQ/ALL)
        :return:
                          bps dvd_yld  end_pr iisu_code  isu_cd      pbr     per prv_eps rn stk_dvd totCnt     work_dt
            0   5,689    0.27  18,650      -  000250   삼천당제약   3.28   44.19     422  1      50   2157  2018/01/03
            1  37,029    2.82  28,350      -  000440  중앙에너비스  0.77   24.98   1,135  2     800         2018/01/03
            2     563       0   2,720      -  001000    신라섬유    4.83  247.27      11  3       0         2018/01/03
            3  10,036    1.75  12,600      -  001540    안국약품    1.26      84     150  4     220         2018/01/03
            4   8,266    1.07   2,815      -  001810    무림SP      0.34   24.06     117  5      30         2018/01/03
        """
        result = self.post(market_gubun=market, gubun=1, schdate=date)
        return DataFrame(result['result'])


class MKD30009_1(KrxWebIo):
    @property
    def bld(self):
        return "MKD/13/1302/13020401/mkd13020401"

    def fetch(self, fromdate, todate, market, isin):
        """30009 PER/PBR/배당수익률 (개별종목)
        :param market: 조회 시장 (STK/KSQ/ALL)
        :param fromdate: 조회 시작 일자 (YYMMDD)
        :param todate: 조회 종료 일자 (YYMMDD)
        :param isin: 조회할 종목의 ISIN 번호
        :return:
                  bps dvd_yld  end_pr iisu_code isu_cd     isu_nm                 isu_nm2   pbr   per prv_eps rn stk_dvd totCnt     work_dt
            0  28,126     1.9  44,650         -  005930   삼성전자   <em class ="up"></em>  1.59  7.45   5,997  1     850      6  2019/03/29
            1  28,126     1.9  44,850         -  005930   삼성전자   <em class ="up"></em>  1.59  7.48   5,997  2     850         2019/03/28
            2  28,126    1.87  45,350         -  005930   삼성전자   <em class ="up"></em>  1.61  7.56   5,997  3     850         2019/03/27
            3  28,126    1.88  45,250         -  005930   삼성전자   <em class ="up"></em>  1.61  7.55   5,997  4     850         2019/03/26
            4  28,126    1.87  45,500         -  005930   삼성전자   <em class ="up"></em>  1.62  7.59   5,997  5     850         2019/03/25
            5  28,126    1.83  46,550         -  005930   삼성전자   <em class ="up"></em>  1.66  7.76   5,997  6     850         2019/03/22
        """
        result = self.post(market_gubun=market, fromdate=fromdate,
                           todate=todate, gubun=2, isu_cd=isin,
                           isu_srt_cd="A" + isin[3:9])
        return DataFrame(result['result'])


class MKD01023(KrxWebIo):
    @property
    def bld(self):
        return "MKD/01/0110/01100305/mkd01100305_01"

    def fetch(self, date):
        result = self.post(search_bas_yy=date)
        return DataFrame(result['block1'])


class MKD80037(KrxWebIo):
    @property
    def bld(self):
        return "MKD/13/1302/13020102/mkd13020102"

    def fetch(self, market, fromdate, todate):
        """80037 전체종목 등락률 (수정종가로 비교)
        :param market  : 조회 시장 (STK/KSQ/ALL)
        :param fromdate: 조회 시작 일자 (YYMMDD)
        :param todate  : 조회 마지막 일자 (YYMMDD)
        :return        : 등락률 DataFrame
                     end_dd_end_pr fluc_tp_cd  isu_cd       isu_tr_amt    isu_tr_vl kor_shrt_isu_nm opn_dd_end_pr prv_dd_cmpr updn_rate
        0           11,250          2  000020   16,851,737,550    1,510,666            동화약품        11,550        -300      -2.6
        1           15,400          2  000030  181,243,425,100   11,623,346            우리은행        16,050        -650     -4.05
        2              717          1  000040    6,506,765,090    9,521,456           KR모터스           667          50       7.5
        3           14,200          2  000050    7,608,850,250      526,376              경방        14,900        -700      -4.7
        4           20,350          2  000060   22,498,470,632    1,094,745           메리츠화재        20,950        -600     -2.86
        5          116,500          1  000070   30,956,512,000      270,219           삼양홀딩스       111,000       5,500      4.95
        6           56,300          1  000075      502,884,700        9,140          삼양홀딩스우        53,300       3,000      5.63
        """
        result = self.post(ind_tp=market, adj_stkprc="Y", period_strt_dd=fromdate,
                           period_end_dd=todate)
        return DataFrame(result['block1'])


################################################################################
# index
class MKD20011(KrxWebIo):
    @property
    def bld(self):
        return "/MKD/03/0304/03040100/mkd03040100"

    def fetch(self, date, index='02'):
        """주가 지수 (http://marketdata.krx.co.kr/mdi#document=030403)
        :param date : 조회 일자
        :param index: 코스피 지수 (02)
                      코스닥 지수 (03)
        :return: 주가지수 DataFrame
        """
        result = self.post(idx_upclss_cd='01', idx_midclss_cd=index, lang='ko', bz_dd=date)
        return DataFrame(result['output'])


class MKD20011_SUB(KrxWebIo):
    @property
    def bld(self):
        return "MKD/03/0304/03040101/mkd03040101T2_02"

    def fetch(self, fromdate, todate, index, market):
        """코스피 주가 지수
        :param index    : 종합지수 - 코스피          (001)
                          종합지수 - 코스피 벤치마크 (100)
                          대표지수 - 코스피 200      (028)
                          대표지수 - 코스피 100      (034)
                          대표지수 - 코스피 50       (035)
                          규모별   - 코스피 대형주   (002)
                          규모별   - 코스피 중형주   (003)
                          규모별   - 코스피 소형주   (004)
        :param market   : 코스피 (1) / 코스닥 (2)
        :param fromdate : 조회 시작 일자 (YYMMDD)
        :param todate   : 조회 마지막 일자 (YYMMDD)
        :return         : 코스피 주가지수 DataFrame
               acc_trdval acc_trdvol clsprc_idx cmpprevdd_idx div_yd fluc_rt fluc_tp_cd hgprc_idx lwprc_idx         mktcap opnprc_idx      trd_dd wt_per wt_stkprc_netasst_rto
            0   4,897,406    419,441   2,117.77          6.84   1.86   -0.32          2  2,129.37  2,108.91  1,397,318,462   2,126.03  2019/01/22   9.95                  0.90
            1   5,170,562    408,600   2,127.78         10.01   1.85    0.47          1  2,131.05  2,106.74  1,403,936,954   2,108.72  2019/01/23  10.00                  0.90
            2   6,035,836    413,652   2,145.03         17.25   1.83    0.81          1  2,145.08  2,125.48  1,415,738,358   2,127.88  2019/01/24  10.08                  0.91
            3   7,065,652    410,002   2,177.73         32.70   1.81    1.52          1  2,178.01  2,146.64  1,437,842,917   2,147.92  2019/01/25  10.23                  0.93
        """
        idx_cd = "1{}".format(index)
        result = self.post(idx_cd=idx_cd, ind_tp_cd=market, idx_ind_cd=index, bz_dd=todate,
                           chartType="line", chartStandard="srate", fromdate=fromdate, todate=todate)
        return DataFrame(result['output'])


class MKD20011_PDF(KrxWebIo):
    @property
    def bld(self):
        return "MKD/03/0304/03040101/mkd03040101T3_01"

    def fetch(self, date, index, market):
        """주가지수 구성 항목
        :param date  : 조회 일자 (YYMMDD)
        :param index : KRX 웹에서 정의하는 index 번호
        :param market: 코스피 (1) / 코스닥 (2)
        :return      : PDF DataFrame
                  acc_trdval cmpprevdd_prc fluc_tp_cd  isu_cd      isu_nm           mktcap tdd_clsprc updn_rate
            0  1,623,651,330           140          1  095570  AJ네트웍스  294,044,012,600      6,280      2.28
            1  1,197,049,750           100          2  068400    AJ렌터카  260,219,025,000     11,750      0.84
            2    239,250,000         1,500          1  001460         BYC  158,339,902,500    253,500      0.60
        """
        result = self.post(ind_tp_cd=market, idx_ind_cd=index, lang="ko", schdate=date)
        return DataFrame(result['output'])


class MKD80002(KrxWebIo):
    @property
    def bld(self):
        return "MKD/13/1301/13010101/mkd13010101"

    def fetch(self, fromdate, todate, market):
        """전체지수 등락률
        :param fromdate : 조회 시작 일자 (YYMMDD)
        :param todate   : 조회 마지막 일자 (YYMMDD)
        :param market: 2(KRX) / 3(KOSPI) / 4 (KOSDAQ)
        :return      : 지수 등락률 DataFrame
                group_code group_name kor_indx_ind_nm     indx     prv_dd_indx  prv_dd_cmpr  fluc_tp  prv_dd_cmpr_chart updn_rate  updn_flag       tr_vl            tr_amt
            0          3        KRX         KRX 300     1,207.80    1,236.27       28.47       1               28.47      2.36         3      1,439,933,029  55,545,303,395,341
            1          3        KRX         KRX 100     4,234.80    4,335.24      100.44       1              100.44      2.37         3        500,776,865  32,360,209,330,383
            2          3        KRX         KTOP 30     6,740.79    6,965.40      224.61       1              224.61      3.33         3        267,094,630  22,349,375,479,336
            3          3        KRX         KRX 자동차  1,128.89    1,156.61       27.72       1               27.72      2.46         3         68,136,810   2,085,570,910,980
            4          3        KRX         KRX 반도체  2,510.86    2,602.63       91.77       1               91.77      3.65         3        211,516,546   4,892,632,198,230
        """

        result = self.post(marketTp=market, period_strt_dd=fromdate, period_end_dd=todate)
        return DataFrame(result['block1'])


class MDK80033_0(KrxWebIo):
    @property
    def bld(self):
        return "MKD/13/1302/13020301/mkd13020301_01"

    def fetch(self, fromdate, todate, market):
        """거래실적 추이 (거래량)
        :param fromdate : 조회 시작 일자 (YYMMDD)
        :param todate   : 조회 마지막 일자 (YYMMDD)
        :param market   : kospi / kosdaq / konex
        :return         : 거래실적 추이 DataFrame
                       dt            tot            stk        sect       reit             fm rpt_mass mktd_mass mktd_bsk mktd_dkpl tme_end_pr   tme_mass  tme_bsk    tme_unit tme_dkpl bz_termnl_ask cable_termnl_ask wrls_termnl_ask      hts_ask     etc_ask bz_termnl_bid cable_termnl_bid wrls_termnl_bid      hts_bid     etc_bid
                0  2020/05/27  1,178,847,686  1,156,800,749  19,567,184  1,181,347  1,159,323,745        0   421,116        0         0  3,271,098    855,312        0  14,976,415        0    71,353,441        2,112,812     567,158,607  453,973,325  84,249,501    72,550,845        1,870,205     563,795,722  451,021,048  89,609,866
                1  2020/05/26  1,017,804,023  1,008,972,681   6,436,255  1,438,271    999,126,798        0    15,406  249,637         0  2,885,337  1,164,620        0  14,362,225        0    64,446,407        1,521,342     481,103,100  408,886,368  61,846,806    64,630,912        1,302,029     472,185,427  406,662,217  73,023,438
                2  2020/05/25    641,458,990    612,379,841  27,522,107  1,021,316    629,592,119        0     9,539        0         0  2,069,182    393,961        0   9,394,189        0    46,899,361        1,141,784     306,793,497  235,845,426  50,778,922    44,817,143        1,167,310     306,395,061  234,893,243  54,186,233
                3  2020/05/22    847,467,288    830,997,456  13,472,156  2,173,679    832,046,355        0   245,749        0         0  3,101,995  3,094,402  382,092   8,596,695        0    63,527,104        1,580,861     382,694,417  312,464,634  87,200,272    54,102,924        1,552,136     413,978,629  315,154,794  62,678,805
                4  2020/05/21    601,199,346    596,327,758   2,325,716  1,780,149    588,696,773        0   426,864        0         0  2,359,680    368,345        0   9,347,684        0    60,969,199        1,050,550     283,828,511  192,808,025  62,543,061    52,553,279        1,177,388     293,822,455  194,186,529  59,459,695

        """
        result = self.post(ind_tp=market, fr_work_dt=fromdate, to_work_dt=todate)
        return DataFrame(result['block1'])

class MDK80033_1(KrxWebIo):
    @property
    def bld(self):
        return "MKD/13/1302/13020301/mkd13020301_02"

    def fetch(self, fromdate, todate, market):
        """거래실적 추이 (거래대금)
        :param fromdate : 조회 시작 일자 (YYMMDD)
        :param todate   : 조회 마지막 일자 (YYMMDD)
        :param market   : kospi / kosdaq / konex
        :return         : 거래실적 추이 DataFrame
                       dt            tot            stk        sect       reit             fm rpt_mass mktd_mass mktd_bsk mktd_dkpl tme_end_pr   tme_mass  tme_bsk    tme_unit tme_dkpl bz_termnl_ask cable_termnl_ask wrls_termnl_ask      hts_ask     etc_ask bz_termnl_bid cable_termnl_bid wrls_termnl_bid      hts_bid     etc_bid
                0  2020/05/27  1,178,847,686  1,156,800,749  19,567,184  1,181,347  1,159,323,745        0   421,116        0         0  3,271,098    855,312        0  14,976,415        0    71,353,441        2,112,812     567,158,607  453,973,325  84,249,501    72,550,845        1,870,205     563,795,722  451,021,048  89,609,866
                1  2020/05/26  1,017,804,023  1,008,972,681   6,436,255  1,438,271    999,126,798        0    15,406  249,637         0  2,885,337  1,164,620        0  14,362,225        0    64,446,407        1,521,342     481,103,100  408,886,368  61,846,806    64,630,912        1,302,029     472,185,427  406,662,217  73,023,438
                2  2020/05/25    641,458,990    612,379,841  27,522,107  1,021,316    629,592,119        0     9,539        0         0  2,069,182    393,961        0   9,394,189        0    46,899,361        1,141,784     306,793,497  235,845,426  50,778,922    44,817,143        1,167,310     306,395,061  234,893,243  54,186,233
                3  2020/05/22    847,467,288    830,997,456  13,472,156  2,173,679    832,046,355        0   245,749        0         0  3,101,995  3,094,402  382,092   8,596,695        0    63,527,104        1,580,861     382,694,417  312,464,634  87,200,272    54,102,924        1,552,136     413,978,629  315,154,794  62,678,805
                4  2020/05/21    601,199,346    596,327,758   2,325,716  1,780,149    588,696,773        0   426,864        0         0  2,359,680    368,345        0   9,347,684        0    60,969,199        1,050,550     283,828,511  192,808,025  62,543,061    52,553,279        1,177,388     293,822,455  194,186,529  59,459,695

        """
        result = self.post(ind_tp=market, fr_work_dt=fromdate, to_work_dt=todate)
        return DataFrame(result['block1'])


################################################################################
# Shorting
class SRT02010100(KrxWebIo):
    @property
    def bld(self):
        return "SRT/02/02010100/srt02010100"

    @staticmethod
    def fetch(fromdate, todate, isin):
        """02010100 공매도 종합 현황
           http://short.krx.co.kr/contents/SRT/02/02010100/SRT02010100.jsp
        :param fromdate: 조회 시작 일자 (YYMMDD)
        :param todate: 조회 마지막 일자 (YYMMDD)
        :param isin:
        :return: 공매도 종합 현황 DataFrame
        """
        result = SRT02010100().post(isu_cd=isin, strt_dd=fromdate, end_dd=todate)
        return DataFrame(result['block1'])


class SRT02020100(KrxFileIo):
    @property
    def bld(self):
        return "SRT/02/02020100/srt02020100"

    def fetch(self, fromedata, todate, market, isin):
        """02020100 공매도 거래 현황
           http://short.krx.co.kr/contents/SRT/02/02020100/SRT02020100.jsp
        :param fromdate: 조회 시작 일자 (YYMMDD)
        :param todate: 조회 마지막 일자 (YYMMDD)
        :param market: 1 (코스피) / 3 (코스닥) / 4 (코넥스)
        :param isin: 종목의 ISIN
        :return:  종목별 공매도 거래 현황 DataFrame
        """

        result = self.post(mkt_tp_cd=market, isu_cd=isin, strt_dd=fromedata, end_dd=todate)
        return pd.read_excel(result)


class SRT02020300(KrxWebIo):
    @property
    def bld(self):
        return "SRT/02/02020300/srt02020300"

    @staticmethod
    def fetch(fromdate, todate, market=1, inquery=1):
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


class SRT02020400(KrxWebIo):
    @property
    def bld(self):
        return "SRT/02/02020400/srt02020400"

    @staticmethod
    def fetch(date, market=1):
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


class SRT02030100(KrxFileIo):
    @property
    def bld(self):
        return "SRT/02/02030100/srt02030100"

    def fetch(self, fromdate, todate, market, isin):
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

        result = self.post(mkt_tp_cd=market, strt_dd=fromdate, end_dd=todate, isu_cd=isin)
        return pd.read_excel(result)


class SRT02030400(KrxWebIo):
    @property
    def bld(self):
        return "SRT/02/02030400/srt02030400"

    @staticmethod
    def fetch(date, market=1):
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
    pd.set_option('display.width', None)

    # stock
    # df = MKD80037().fetch("ALL", "20180501", "20180801")
    # df = MKD80037().fetch("ALL", "20180501", "20180515")
    # df = MKD30015().fetch("20190401", "ALL")

    # index
    # df = MKD20011_PDF().fetch("20190412", "001", 2)
    # df = MKD20011().fetch("20190413", "03")
    # df = MKD20011_SUB().fetch("20190408", "20190412", "001", 1)
    # df = MKD20011_SUB().fetch("20190408", "20190412", "001", 2)
    # print(MKD30009_1().fetch('20190322', '20190329', 'ALL', 'KR7005930003'))
    # df = MKD80002().fetch("20200520", "20200527", 2)
    # df = MDK80033_0().fetch("20200519", "20200526", 'kospi')
    # df = MDK80033_1().fetch("20200519", "20200526", 'kospi')
    # shorting
    # print(SRT02010100().fetch("KR7005930003", "20181205", "20181207"))
    # print(SRT02020100().fetch("20200525", "20200531", 1, "KR7210980009"))
    # print(SRT02020100().fetch("20181207", 1, "KR7005930003"))
    # print(SRT02020300().fetch("20181207", "20181212", "kospi", "거래대금"))
    # print(SRT02020400().fetch("20181212", "코스피"))
    # print(SRT02030100().fetch("20200101", "20200531", 1, "KR7210980009"))
    # print(SRT02030100().fetch("20181207", "20181212", "kospi", "KR7210980009"))
    # print(SRT02030400().fetch("20181214", 1))
    # print(df)