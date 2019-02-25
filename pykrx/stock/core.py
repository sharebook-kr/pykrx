from pykrx.comm.http import MarketDataHttp
from pandas import DataFrame


class MKD30030(MarketDataHttp):
    @property
    def bld(self):
        return "MKD/04/0406/04060200/mkd04060200"

    def read(self, date):
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


class MKD30040(MarketDataHttp):
    @property
    def bld(self):
        return "MKD/04/0402/04020100/mkd04020100t3_02"

    def read(self, fromdate, todate, isin):
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


class MKD30009(MarketDataHttp):
    @property
    def bld(self):
        return "MKD/13/1302/13020401/mkd13020401"

    def read(self, date, market):
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


class MKD01023(MarketDataHttp):
    @property
    def bld(self):
        return "MKD/01/0110/01100305/mkd01100305_01"

    def read(self, date):
        result = self.post(search_bas_yy=date)
        return DataFrame(result['block1'])


class MKD20011(MarketDataHttp):
    @property
    def bld(self):
        return "/MKD/03/0304/03040100/mkd03040100"

    def read(self, date):
        result = self.post(idx_upclss_cd='01', idx_midclss_cd='02', lang='ko',
                           bz_dd=date)
        return DataFrame(result['output'])


class MKD20011_KOSPI(MarketDataHttp):
    @property
    def bld(self):
        return "MKD/03/0304/03040101/mkd03040101T2_02"

    def read(self, fromdate, todate, index):
        """코스피 주가 지수
        :param index    : 종합지수 - 코스피          (001)
                           종합지수 - 코스피 벤치마크 (100)
                           대표지수 - 코스피 200      (028)
                           대표지수 - 코스피 100      (034)
                           대표지수 - 코스피 50       (035)
                           규모별   - 코스피 대형주   (002)
                           규모별   - 코스피 중형주   (003)
                           규모별   - 코스피 소형주   (004)
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
        result = self.post(idx_cd=idx_cd, ind_tp_cd='1', idx_ind_cd=index,
                           bz_dd=todate, chartType="line", chartStandard="srate",
                           fromdate=fromdate, todate=todate)
        return DataFrame(result['output'])


class MKD80037(MarketDataHttp):
    @property
    def bld(self):
        return "MKD/13/1302/13020102/mkd13020102"

    def read(self, market, fromdate, todate):
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
        result = self.post(ind_tp=market, adj_stkprc="Y",
                           period_strt_dd=fromdate, period_end_dd=todate)
        return DataFrame(result['block1'])


if __name__ == "__main__":
    import pandas as pd
    pd.set_option('display.width', None)
    # print(MKD20011().read("20180501"))
    print(MKD20011_KOSPI().read("20190122", "20190222", "028"))
    # print(MKD80037().read("ALL", "20180501", "20180515"))
