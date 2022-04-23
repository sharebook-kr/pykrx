from pykrx.website.krx.krxio import KrxWebIo
from pandas import DataFrame


class MKD40038(KrxWebIo):
    # 지표 수익률 (20140303 ~ 현재)
    # - http://marketdata.krx.co.kr/mdi#document=05030403
    @property
    def bld(self):
        return "MKD/05/0503/05030403/mkd05030403"

    def fetch(self, fromdate, todate):
        try:
            result = self.post(fr_work_dt=fromdate, to_work_dt=todate)
            if len(result['block1']) == 0:
                return None

            df = DataFrame(result['block1'])
            df = df[['trd_dd', 'prc_yd1', 'prc_yd2', 'prc_yd3', 'prc_yd4',
                     'prc_yd5']]
            df.columns = ['일자', '3년물', '5년물', '10년물', '20년물',
                          '30년물']
            df.set_index('일자', inplace=True)

            df.index = [x.replace('/', '-') for x in df.index]
            df = df.astype(float)
            df.index.name = "지표수익률"
            return df
        except (TypeError, IndexError, KeyError) as e:
            print(e)
            return None


class 전종목_장외채권수익률(KrxWebIo):
    @property
    def bld(self):
        return "dbms/MDC/STAT/standard/MDCSTAT11401"

    def fetch(self, trdDd):
        """
        [14017] 장외 채권수익률 - 전종목
         - http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=
           MDC0201040101

        Args:
            trdDd       (str): 조회 일자 (YYMMDD)

        Returns:

            > 전종목_장외채권수익률().fetch("20220203")

                           ITM_TP_NM LST_ORD_BAS_YD  CMP_YD
            0              국고채 1년          1.452  -0.011
            1              국고채 2년          1.969  -0.037
            2              국고채 3년          2.158  -0.031
            3              국고채 5년          2.373  -0.023
            4             국고채 10년          2.566  -0.020

        """
        result = self.read(inqTpCd="T", trdDd=trdDd)
        return DataFrame(result['output'])


class 개별추이_장외채권수익률(KrxWebIo):
    @property
    def bld(self):
        return "dbms/MDC/STAT/standard/MDCSTAT11402"

    def fetch(self, strtDd, endDd, bndKindTpCd):
        """
        [14017] 장외 채권수익률 - 개별추이
         - http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=
           MDC0201040101

        Args:
            startDd     (str): 시작 일자 (YYMMDD)
            endDd       (str): 종료 일자 (YYMMDD)
            bndKindTpCd (str):
                - 3006 : 국고채1년
                - 3019 : 국고채2년
                - 3000 : 국고채3년
                - 3007 : 국고채5년
                - 3013 : 국고채10년
                - 3014 : 국고채20년
                - 3017 : 국고채30년
                - 3008 : 국민주택1종5년
                - 3009 : 회사채AA-
                - 3010 : 회사채BBB-
                - 4000 : CD

        Returns:

            > 개별추이_장외채권수익률().fetch("20220103", "20220203", "3006")

                 DISCLS_DD LST_ORD_BAS_YD  CMP_YD
            0   2022/02/03          1.452  -0.011
            1   2022/01/28          1.463  -0.010
            2   2022/01/27          1.473   0.018
            3   2022/01/26          1.455   0.002
            4   2022/01/25          1.453   0.015
        """
        result = self.read(inqTpCd="E", strtDd=strtDd, endDd=endDd,
                           bndKindTpCd=bndKindTpCd)
        return DataFrame(result['output'])


if __name__ == "__main__":
    import pandas as pd
    pd.set_option('display.width', None)

    # df = 전종목_장외채권수익률().fetch("20220203")
    df = 개별추이_장외채권수익률().fetch("20220103", "20220203", "3006")
    print(df)
