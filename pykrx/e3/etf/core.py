from pykrx.comm.http import KrxHttp
from pandas import DataFrame


class MKD60003(KrxHttp):
    @property
    def bld(self):
        return "MKD/08/0801/08012001/mkd08012001_01"

    def read(self, date):
        '''600003 상품 비교 검색
        '''
        result = self.post(type_gubun="00", isur_cd="00000", trd_dd=date)
        return DataFrame(result['block1'])


class MKD60004(KrxHttp):
    @property
    def bld(self):
        return "COM/etf_combo"

    def read(self):
        '''ETF 티커 조회
        :return: 티커와 ISIN 값 반환

                        label         value
                     ARIRANG 200  KR7152100004
             ARIRANG 200동일가중  KR7295820005
               ARIRANG 200로우볼  KR7295890008
               ARIRANG 200모멘텀  KR7295860001
                 ARIRANG 200밸류  KR7295840003
        '''
        result = self.post(gubun=1)
        return DataFrame(result['block1'])


class MKD60005(KrxHttp):
    #
    @property
    def bld(self):
        return "MKD/08/0801/08010500/mkd08010500_02"

    def read(self, fromdate, todate):
        result = self.post(trd_dd=todate, fromdate=fromdate, todate=todate, date=todate, gubun2=2,
                           acsString=0, domforn="01", uly_gubun="02", gubun="00", isu_cd="KR7114820004")
        return DataFrame(result['block1'])


class MKD60007(KrxHttp):
    @property
    def bld(self):
        return "MKD/08/0801/08010700/mkd08010700_04"

    def read(self, fromdate, todate, isin):
        ''' 종목의 NAV와 OHLCV
        :param fromdate: 조회 시작 일자 (YYYYMMDD)
        :param todate: 조회 종료 일자 (YYYYMMDD)
        :param isin: 조회할 종목의 ISIN 번호
        :return:

           fluc_tp_cd isu_end_pr isu_hg_pr isu_lw_pr isu_opn_pr last_indx last_nav   prv_dd_cmpr tot_tr_amt tot_tr_vl  work_dt
               1        28,440     28,440    28,195     28,405    276.48  28,419.20     195        2,949     104,277  2019/03/29
               2        28,245     28,345    28,110     28,110    275.08  28,276.06     155        3,222     114,079  2019/03/28
               2        28,400     28,490    28,170     28,190    277.28  28,434.08       5        7,196     253,742  2019/03/27
               1        28,405     28,495    28,380     28,480    277.60  28,465.95       5        4,118     144,815  2019/03/26
        '''
        result = self.post(fromdate=fromdate, todate=todate, isu_cd=isin)
        return DataFrame(result['block1'])


class MKD60015(KrxHttp):
    @property
    def bld(self):
        return "MKD/08/0801/08011402/mkd08011402_02"

    def read(self, date, isin):
        ''' PDF (Portfolio Deposit File)
        :param date: 조회 일자 (YYYYMMDD)
        :param isin: 조회할 종목의 ISIN 번호
        :return:

              compst_amt compst_amt_rt  cu1_shrs  isu_kor_nm   par_amt
             377,113,900         26.54  8,446.00    삼성전자      -
              74,496,800          5.24  1,004.00  SK하이닉스      -
              31,856,000          2.24    176.00    셀트리온      -
              31,119,000          2.19    123.00       POSCO      -
        '''
        result = self.post(schdate=date, isu_cd=isin)
        return DataFrame(result['ETF 종합시세'])


class MKD81010(KrxHttp):
    @property
    def bld(self):
        return "MKD/13/1304/13040102/mkd13040102_01"

    def read(self, fromdate, todate, isin):
        """개별 종목 시세 추이
           fluc_tp_cd isu_end_pr isu_hg_pr isu_lw_pr isu_opn_pr  last_indx    last_nav prv_dd_cmpr tot_tr_amt tot_tr_vl     work_dt
            2    110,040   110,070   109,980    110,055  10,647.21  110,018.70          15      3,410    30,987  2019/04/05
            2    110,055   110,070   110,010    110,070  10,650.88  110,044.23          10      3,559    32,344  2019/04/04
            2    110,065   110,125   110,025    110,125  10,648.04  110,034.25          40      2,507    22,779  2019/04/03
            1    110,105   110,105   109,960    110,020  10,654.24  110,095.63         105      3,439    31,273  2019/04/02
        """
        result = self.post(trd_dd=todate, fromdate=fromdate, todate=todate, 
                           date=todate, gubun2=2, domforn="00", accString="0",
                           uly_gubun="00", gubun="00", isu_cd=isin)        
        return DataFrame(result['block1'])
    


if __name__ == "__main__":
    import pandas as pd
    pd.set_option('display.width', None)
    df = MKD60003().read("20190409")
    # df = MKD60005().read("20190211", "20190311")
    # df = MKD60004().read()
    # df = MKD60007().read("20190228", "20190329", "KR7152100004")
    # df = MKD60015().read("20190329", "KR7152100004")
    # df = MKD81010().read("20180301", "20190329", "KR7114820004")
    print(df)