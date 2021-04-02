from pykrx.website.krx.krxio import KrxWebIo
from pandas import DataFrame


# ------------------------------------------------------------------------------------------
# Ticker

class 상장종목검색(KrxWebIo):
    @property
    def bld(self):
        return "dbms/comm/finder/finder_secuprodisu"

    def fetch(self, market: str="ALL", name: str = "") -> DataFrame:
        """[13103] 개별종목 시세 추이에서 검색 버튼 눌러 활성화 되는 종목 검색창 스크래핑

        Args:
            market (str, optional): 조회 시장 (ETN/ETF/ELW/ALL)
            name   (str, optional): 검색할 종목명 -  입력하지 않을 경우 전체

        Returns:
            DataFrame : 상장 종목 정보를 반환

                     full_code short_code                  codeName
                0  KR7152100004     152100              ARIRANG 200
                1  KR7295820005     295820      ARIRANG 200동일가중
                2  KR7253150007     253150  ARIRANG 200선물레버리지
                3  KR7253160006     253160  ARIRANG 200선물인버스2X
                4  KR7278420005     278420      ARIRANG ESG우수기업
        """
        result = self.read(mktsel=market, searchText=name)
        return DataFrame(result['block1'])


class ETF_전종목기본종목(KrxWebIo):
    @property
    def bld(self):
        return "dbms/MDC/STAT/standard/MDCSTAT04601"

    def fetch(self) -> DataFrame:
        """[13104] 전종목 기본정보

        Returns:
            DataFrame : 상장 종목 정보를 반환

                           ISU_CD ISU_SRT_CD                                                              ISU_NM                        ISU_ABBRV                                         ISU_ENG_NM     LIST_DD                ETF_OBJ_IDX_NM IDX_CALC_INST_NM1     IDX_CALC_INST_NM2 ETF_REPLICA_METHD_TP_CD IDX_MKT_CLSS_NM IDX_ASST_CLSS_NM  LIST_SHRS        COM_ABBRV   CU_QTY ETF_TOT_FEE                 TAX_TP_CD
                0    KR7292340007     292340   DB 마이티 200커버드콜ATM레버리지증권상장지수투자신탁[주식-파생형]    마이티 200커버드콜ATM레버리지   DB Mighty KOSPI200 Covered Call ATM Leverage ETF  2018/03/20  코스피 200 커버드콜 ATM 지수               KRX       2X 레버리지 (2)                    실물            국내             주식    500,000     디비자산운용  100,000       0.510  배당소득세(보유기간과세)
                1    KR7159800002     159800                              DB마이티K100증권상장지수투자신탁(주식)                 마이티 코스피100                                 DB Mighty K100 ETF  2012/07/05                    코스피 100               KRX              일반 (1)                    실물            국내             주식    400,000     디비자산운용   40,000       0.390                    비과세
                2    KR7361580004     361580                KB KBSTAR 200 Total Return증권상장지수투자신탁(주식)                     KBSTAR 200TR                     KB KBSTAR 200 Total Return ETF  2020/08/21                 코스피 200 TR               KRX              일반 (1)                    실물            국내             주식  1,300,000   케이비자산운용   50,000       0.045  배당소득세(보유기간과세)
                3    KR7285000006     285000                           KB KBSTAR 200IT증권상장지수투자신탁(주식)                     KBSTAR 200IT           KB KBSTAR 200 Information Technology ETF  2017/12/08           코스피 200 정보기술               KRX              일반 (1)                    실물            국내             주식    700,000   케이비자산운용   20,000       0.190                    비과세
                4    KR7287300008     287300                         KB KBSTAR 200건설증권상장지수투자신탁(주식)                   KBSTAR 200건설                    KB KBSTAR 200 Constructions ETF  2017/12/22               코스피 200 건설               KRX              일반 (1)                    실물            국내             주식    560,000   케이비자산운용   20,000       0.190                    비과세
        """
        result = self.read()
        return DataFrame(result['output'])


class ETN_전종목기본종목(KrxWebIo):
    @property
    def bld(self):
        return "dbms/MDC/STAT/standard/MDCSTAT06701"

    def fetch(self) -> DataFrame:
        """[13202] 전종목 등락률

        Returns:
            DataFrame : 상장 종목 정보를 반환

                           ISU_CD ISU_SRT_CD                                                          ISU_NM               ISU_ABBRV                                     ISU_ENG_NM     LIST_DD   LSTTRD_DD            TRACE_IDX_NM IDX_CALC_INST_NM IDX_LVRG_INVRS_TP_CD ETP_PROD_TP_CD IDX_MKT_CLSS_NM IDX_ASST_CLSS_NM   LIST_SHRS   ISUR_NM   EXPS_RTO   TAX_TP_CD
                0    KRG580000112     580011                    KB증권 KB FnGuide 언택트 상장지수증권 제11호   KB FnGuide 언택트 ETN            KB Securities KB FnGuide Untact ETN  2020/09/01  2030/08/29     FnGuide 언택트 지수          FnGuide                 일반            ETN            국내             주식   1,000,000    KB증권       0.80      비과세
                1    KRG581100077     580007      KB증권 KB KQ 우량주30 ETN 파생결합증권(상장지수증권) 제7호      KB KQ 우량주30 ETN                      KB KB KQ Bluechip30 ETN 7  2016/12/28  2026/12/23   WISE KQ 우량주30 지수           WiseFn                 일반            ETN            국내             주식   5,000,000    KB증권       0.65      비과세
                2    KRG580000138     580013           KB증권 KB KRX ESG Eco Leaders 100 상장지수증권 제13호      KB KRX ESG Eco ETN   KB Securities KB KRX ESG Eco Leaders 100 ETN  2020/11/12  2030/11/08     KRX Eco Leaders 100              KRX                 일반            ETN            국내             주식   1,000,000    KB증권       0.80      비과세
                3    KRG580000120     580012                            KB증권 KB KRX300 상장지수증권 제12호           KB KRX300 ETN                    KB Securities KB KRX300 ETN  2020/09/10  2030/09/06                 KRX 300              KRX                 일반            ETN            국내             주식   1,000,000    KB증권       0.50      비과세
                4    KRG581100069     580006               KB증권 KB KTOP30 파생결합증권(상장지수증권) 제6호           KB KTOP30 ETN                             KB KB KTOP30 ETN 6  2016/10/27  2026/10/23                 KTOP 30              KRX                 일반            ETN            국내             주식   5,000,000    KB증권       0.39      비과세
        """
        result = self.read()
        return DataFrame(result['output'])


class ELW_전종목기본종목(KrxWebIo):
    @property
    def bld(self):
        return "dbms/MDC/STAT/standard/MDCSTAT08501"

    def fetch(self) -> DataFrame:
        """[13303] 전종목 기본정보

        Returns:
            DataFrame : 상장 종목 정보를 반환

                            ISU_CD ISU_SRT_CD                                ISU_NM           ISU_ABBRV                ISU_ENG_NM     LIST_DD   LSTTRD_DD      EXP_DD ELW_ULY_TP_NM       ULY_NM   LIST_SHRS   ISUR_NM ELW_CONV_RTO RGHT_TP_NM ELW_EXER_TP EXER_PRC   LP_NM ORD_SPD_RTO ELW_LST_SETL_METHD
                0     KRA5811AJA22     58F194    KB증권(주) 주식워런트증권 제F194호    KBF194SK하이닉콜    KB SECURITIES ELW F194  2020/02/26  2021/02/10  2021/02/16          주식   SK하이닉스  10,400,000    KB증권         0.01         콜      유럽형  110,000  KB증권          15           현금결제
                1     KRA5811AKA29     58F195    KB증권(주) 주식워런트증권 제F195호    KBF195삼성전자콜    KB SECURITIES ELW F195  2020/02/26  2021/02/10  2021/02/16          주식     삼성전자  15,000,000    KB증권         0.01         콜      유럽형   65,600  KB증권          15           현금결제
                2     KRA581194A30     58F298    KB증권(주) 주식워런트증권 제F298호    KBF298삼성전자콜    KB SECURITIES ELW F298  2020/03/31  2021/02/10  2021/02/16          주식     삼성전자  10,300,000    KB증권         0.01         콜      유럽형   52,300  KB증권          15           현금결제
                3     KRA581295A38     58F299    KB증권(주) 주식워런트증권 제F299호    KBF299삼성전자풋    KB SECURITIES ELW F299  2020/03/31  2021/02/10  2021/02/16          주식     삼성전자  23,900,000    KB증권         0.01         풋      유럽형   50,900  KB증권          15           현금결제
                4     KRA5811A0A44     58F407    KB증권(주) 주식워런트증권 제F407호      KBF407LG화학콜    KB SECURITIES ELW F407  2020/04/28  2021/02/10  2021/02/16          주식       LG화학  16,000,000    KB증권        0.002         콜      유럽형  322,500  KB증권          15           현금결제
        """
        result = self.read()
        return DataFrame(result['output'])

# ------------------------------------------------------------------------------------------
# ETF

class 개별종목시세_ETF(KrxWebIo):
    @property
    def bld(self):
        return "dbms/MDC/STAT/standard/MDCSTAT04501"

    def fetch(self, fromdate: str, todate: str, isin: str) -> DataFrame:
        """[13103] 개별종목 시세 추이

        Args:
            fromdate (str): 조회 시작 일자 (YYMMDD)
            todate   (str): 조회 종료 일자 (YYMMDD)
            isin     (str): 조회할 종목의 ISIN 번호

        Returns:
            DataFrame:
                       TRD_DD TDD_CLSPRC FLUC_TP_CD CMPPREVDD_PRC FLUC_RT    LST_NAV TDD_OPNPRC TDD_HGPRC TDD_LWPRC ACC_TRDVOL      ACC_TRDVAL           MKTCAP INVSTASST_NETASST_TOTAMT   LIST_SHRS    IDX_IND_NM OBJ_STKPRC_IDX FLUC_TP_CD1 CMPPREVDD_IDX IDX_FLUC_RT
                0  2021/01/19     42,965          1         1,080    2.58  43,079.14     42,075    43,250    41,900    192,061   8,222,510,755  850,707,000,000                        0  19,800,000    코스피 200         421.35           1         10.85        2.64
                1  2021/01/18     41,885          2           975   -2.27  41,970.16     42,505    42,795    41,755    443,478  18,658,324,445  831,417,250,000          831,009,080,773  19,850,000    코스피 200         410.50           2          9.93       -2.36
                2  2021/01/15     42,860          2           975   -2.22  42,987.78     44,095    44,550    42,840    418,224  18,200,519,245  825,055,000,000          853,307,406,018  19,250,000    코스피 200         420.43           2          9.42       -2.19
                3  2021/01/14     43,835          2            30   -0.07  43,942.97     43,725    43,995    43,585    196,552   8,602,863,505  861,357,750,000          845,902,227,451  19,650,000    코스피 200         429.85           2          0.53       -0.12
        """
        result = self.read(isuCd=isin, strtDd=fromdate, endDd=todate)
        return DataFrame(result['output'])


class 전종목시세_ETF(KrxWebIo):
    @property
    def bld(self):
        return "dbms/MDC/STAT/standard/MDCSTAT04301"

    def fetch(self, date: str) -> DataFrame:
        """[13101] 전종목 시세

        Args:
            date (str): 조회 일자 (YYMMDD)

        Returns:
            DataFrame: 전종목의 가격 정보

                    ISU_SRT_CD                ISU_ABBRV TDD_CLSPRC CMPPREVDD_PRC FLUC_TP_CD FLUC_RT         NAV TDD_OPNPRC TDD_HGPRC TDD_LWPRC ACC_TRDVOL     ACC_TRDVAL           MKTCAP INVSTASST_NETASST_TOTAMT   LIST_SHRS               IDX_IND_NM OBJ_STKPRC_IDX CMPPREVDD_IDX FLUC_TP_CD1 FLUC_RT1
                0       152100              ARIRANG 200     42,965         1,080          1    2.58   43,079.14     42,075    43,250    41,900    192,061  8,222,510,755  850,707,000,000                        0  19,800,000               코스피 200         421.35         10.85           1     2.64
                1       295820      ARIRANG 200동일가중     10,710           260          1    2.49   10,701.72     10,590    10,710    10,590         31        331,690    6,426,000,000                        0     600,000  코스피 200 동일가중지수       2,311.52         57.41           1     2.55
                2       253150  ARIRANG 200선물레버리지     49,560         2,605          1    5.55   49,687.23     47,205    50,030    47,000      7,237    353,452,240   14,868,000,000                        0     300,000      코스피 200 선물지수       2,112.71         67.25           1     3.29
                3       253160  ARIRANG 200선물인버스2X      4,220          -240          2   -5.38    4,193.34      4,430     4,455     4,175    485,817  2,095,171,435   16,036,000,000                        0   3,800,000      코스피 200 선물지수       2,112.71         67.25           1     3.29
                4       278420      ARIRANG ESG우수기업      8,950           145          1    1.65    8,952.56      8,815     8,975     8,810     39,513    352,947,304    4,027,500,000                        0     450,000    WISE ESG우수기업 지수       1,210.07         19.83           1     1.67
        """
        result = self.read(trdDd=date)
        return DataFrame(result['output'])


class 전종목등락률_ETF(KrxWebIo):
    @property
    def bld(self):
        return "dbms/MDC/STAT/standard/MDCSTAT04401"

    def fetch(self, fromdate: str, todate: str) -> DataFrame:
        """[13102] 전종목 등락률

        Args:
            fromdate (str): 조회 시작 일자 (YYMMDD)
            todate   (str): 조회 종료 일자 (YYMMDD)

        Returns:
            DataFrame: 전종목의 기간별 가격 정보

                >> 전종목등락률_ETF().fetch("20210325", "20210402")

                        ISU_SRT_CD                 ISU_ABBRV  BAS_PRC   CLSPRC FLUC_TP_CD CMP_PRC FLUC_RT ACC_TRDVOL      ACC_TRDVAL
                    0       152100               ARIRANG 200   41,715   43,405          1   1,690    4.05  1,002,296  42,802,174,550
                    1       295820       ARIRANG 200동일가중   10,855   11,185          1     330    3.04      1,244      13,820,930
                    2       253150   ARIRANG 200선물레버리지   45,770   49,735          1   3,965    8.66     13,603     650,641,700
                    3       253160   ARIRANG 200선물인버스2X    4,380    4,015          2    -365   -8.33    488,304   2,040,509,925
                    4       278420       ARIRANG ESG우수기업    9,095    9,385          1     290    3.19      9,114      84,463,155
        """
        result = self.read(strtDd=fromdate, endDd=todate)
        return DataFrame(result['output'])


class PDF(KrxWebIo):
    @property
    def bld(self):
        return "dbms/MDC/STAT/standard/MDCSTAT05001"

    def fetch(self, date: str, isin: str) -> DataFrame:
        """[13108] PDF(Portfolio Deposit File)

        Args:
            date (str): 조회 일자 (YYMMDD)
            isin (str): 조회할 종목의 ISIN 번호

        Returns:
            DataFrame:
                    COMPST_ISU_CD COMPST_ISU_NM COMPST_ISU_CU1_SHRS     VALU_AMT   COMPST_AMT COMPST_RTO
                0          005930      삼성전자            8,175.00  694,875,000  711,225,000      16.53
                1          000660    SK하이닉스               972.0  126,360,000  126,846,000       2.95
                2          051910        LG화학                80.0   77,120,000   79,760,000       1.85
                3          035420         NAVER               219.0   65,809,500   67,452,000       1.57
                4          006400       삼성SDI                89.0   62,834,000   65,148,000       1.51

                NOTE: 웹 서버가 COMPST_ISU_CD에 ISIN과 축향형을 혼합해서 반환한다. Why?>
        """
        result = self.read(trdDd=date, isuCd=isin)
        return DataFrame(result['output'])

class 추적오차율추이(KrxWebIo):
    @property
    def bld(self):
        return "dbms/MDC/STAT/standard/MDCSTAT05901"

    def fetch(self, fromdate: str, todate: str, isin: str) -> DataFrame:
        """[13112] 추적오차율 추이

        Args:
            fromdate (str): 조회 시작 일자 (YYMMDD)
            todate   (str): 조회 종료 일자 (YYMMDD)
            isin     (str): 조회할 종목의 ISIN 번호

        Returns:
            DataFrame:
                        TRD_DD    LST_NAV NAV_CHG_RT OBJ_STKPRC_IDX IDX_CHG_RTO TRACE_YD_MULT TRACE_ERR_RT
                0   2021/01/18  41,970.16      -2.40         410.50       -2.39           1.0         0.44
                1   2021/01/15  42,987.78      -2.20         420.43       -2.22           1.0         0.44
                2   2021/01/14  43,942.97      -0.12         429.85       -0.12           1.0         0.44
                3   2021/01/13  43,994.16       0.58         430.38        0.58           1.0         0.44
                4   2021/01/12  43,740.55      -0.77         427.87       -0.76           1.0         0.44
        """
        result = self.read(strtDd=fromdate, endDd=todate, isuCd=isin)
        return DataFrame(result['output'])


class 괴리율추이(KrxWebIo):
    @property
    def bld(self):
        return "dbms/MDC/STAT/standard/MDCSTAT06001"

    def fetch(self, fromdate: str, todate: str, isin: str) -> DataFrame:
        """[13113] 괴리율 추이

        Args:
            fromdate (str): 조회 시작 일자 (YYMMDD)
            todate   (str): 조회 종료 일자 (YYMMDD)
            isin     (str): 조회할 종목의 ISIN 번호

        Returns:
            DataFrame:
                        TRD_DD FLUC_TP_CD  CLSPRC    LST_NAV DIVRG_RT
                0   2020/12/21          1  37,410  37,477.27    -0.18
                1   2020/12/22          2  36,825  36,899.05    -0.20
                2   2020/12/23          1  37,325  37,391.69    -0.18
                3   2020/12/24          1  38,060  38,182.05    -0.32
                4   2020/12/28          1  38,240  38,259.11    -0.05
        """
        result = self.read(strtDd=fromdate, endDd=todate, isuCd=isin)
        return DataFrame(result['output'])


if __name__ == "__main__":
    import pandas as pd
    pd.set_option('display.width', None)
    # print(상장종목검색().fetch("ETF"))
    print(전종목등락률_ETF().fetch("20210325", "20210402"))
    # print(개별종목시세_ETF().fetch("20210111", "20210119", "KR7152100004"))
    # print(전종목시세_ETF().fetch("20210119"))
    # print(PDF().fetch("20210119", "KR7152100004"))
    # print(추적오차율추이().fetch("20201219", "20210119", "KR7152100004"))
    # print(괴리율추이().fetch("20201219", "20210119", "KR7152100004"))
