from pykrx.comm.http import KrxHttp
from pandas import DataFrame


class MKD20011(KrxHttp):
    @property
    def bld(self):
        return "/MKD/03/0304/03040100/mkd03040100"

    def read(self, date, index='02'):
        """주가 지수 (http://marketdata.krx.co.kr/mdi#document=030403)
        :param date : 조회 일자
        :param index: 코스피 지수 (02)
                      코스닥 지수 (03)
        :return: 주가지수 DataFrame
        """
        result = self.post(idx_upclss_cd='01', idx_midclss_cd=index, lang='ko',
                           bz_dd=date)
        return DataFrame(result['output'])


class MKD20011_SUB(KrxHttp):
    @property
    def bld(self):
        return "MKD/03/0304/03040101/mkd03040101T2_02"

    def read(self, fromdate, todate, index, market):
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
        result = self.post(idx_cd=idx_cd, ind_tp_cd=market, idx_ind_cd=index,
                           bz_dd=todate, chartType="line", chartStandard="srate",
                           fromdate=fromdate, todate=todate)
        return DataFrame(result['output'])


class MKD20011_PDF(KrxHttp):
    @property
    def bld(self):
        return "MKD/03/0304/03040101/mkd03040101T3_07"

    def read(self, date, index, market):
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
    
    
if __name__ == "__main__":
    import pandas as pd
    pd.set_option('display.expand_frame_repr', False)
    # df = MKD20011_PDF().read("20190412", "001", 2)
    # df = MKD20011().read("20190413", "03")
    df = MKD20011_SUB().read("20190408", "20190412", "001", 1)
    print(df.head())
    df = MKD20011_SUB().read("20190408", "20190412", "001", 2)
    print(df.head())