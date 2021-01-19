from pykrx.website.krx.krxio import KrxWebIo, KrxFileIo, SrtWebIo
import pandas as pd
from pandas import DataFrame

# ------------------------------------------------------------------------------------------
# Ticker

class 상장종목검색(KrxWebIo):
    @property
    def bld(self):
        return "dbms/comm/finder/finder_stkisu"

    def fetch(self, market: str="ALL", name: str = "") -> DataFrame:
        """[12003] 개별종목 시세 추이에서 검색 버튼 눌러 활성화 되는 종목 검색창 스크래핑

        Args:
            market (str, optional): 조회 시장 (STK/KSQ/ALL)
            name   (str, optional): 검색할 종목명 -  입력하지 않을 경우 전체

        Returns:
            DataFrame : 상장 종목 정보를 반환

                  full_code short_code    codeName marketCode marketName marketEngName ord1 ord2
            0  KR7060310000     060310          3S        KSQ     코스닥        KOSDAQ        16
            1  KR7095570008     095570  AJ네트웍스        STK   유가증권         KOSPI        16
            2  KR7006840003     006840    AK홀딩스        STK   유가증권         KOSPI        16
            3  KR7054620000     054620   APS홀딩스        KSQ     코스닥        KOSDAQ        16
            4  KR7265520007     265520    AP시스템        KSQ     코스닥        KOSDAQ        16
        """
        result = self.read(mktsel=market, searchText=name, typeNo=0)
        return DataFrame(result['block1'])


class 상폐종목검색(KrxWebIo):
    @property
    def bld(self):

        return "dbms/comm/finder/finder_listdelisu"

    def fetch(self, market:str = "ALL", name: str = "") -> DataFrame:
        """[20037] 상장폐지종목 현황
         - http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC02021301

        Args:
            market (str, optional): 조회 시장 (STK/KSQ/ALL) . Defaults to "ALL".
            name   (str, optional): 검색할 종목명으로 입력하지 않을 경우 전체 조회함

        Returns:
            DataFrame: 상장폐지 종목 정보를 반환

                         full_code short_code    codeName marketCode   marketName ord1 ord2
                0     KR7037730009     037730         3R        KSQ        코스닥        16
                1     KR7036360006     036360      3SOFT        KSQ        코스닥        16
                2     KYG887121070     900010 3노드디지탈       KSQ        코스닥        16
                3     KR7038120002     038120    AD모터스       KSQ        코스닥        16
        """
        result = self.read(mktsel=market, searchText=name, typeNo=0)
        return DataFrame(result['block1'])

# ------------------------------------------------------------------------------------------
# Market

class 개별종목시세(KrxWebIo):
    @property
    def bld(self):
        return "dbms/MDC/STAT/standard/MDCSTAT01701"

    def fetch(self, fromdate: str, todate: str, isin: str) -> DataFrame:
        """[12003] 개별종목 시세 추이 (수정종가 아님)

        Args:
            fromdate (str): 조회 시작 일자 (YYMMDD)
            todate   (str): 조회 종료 일자 (YYMMDD)
            isin     (str): 조회할 종목의 ISIN 번호

        Returns:
            DataFrame: 일자별 시세 조회 결과
                   TRD_DD TDD_CLSPRC FLUC_TP_CD CMPPREVDD_PRC FLUC_RT TDD_OPNPRC TDD_HGPRC TDD_LWPRC  ACC_TRDVOL         ACC_TRDVAL               MKTCAP      LIST_SHRS
            0  2021/01/15     88,000          2        -1,700   -1.90     89,800    91,800    88,000  33,431,809  2,975,231,937,664  525,340,864,400,000  5,969,782,550
            1  2021/01/14     89,700          3             0    0.00     88,700    90,000    88,700  26,393,970  2,356,661,622,700  535,489,494,735,000  5,969,782,550
            2  2021/01/13     89,700          2          -900   -0.99     89,800    91,200    89,100  36,068,848  3,244,066,562,850  535,489,494,735,000  5,969,782,550
            3  2021/01/12     90,600          2          -400   -0.44     90,300    91,400    87,800  48,682,416  4,362,546,108,950  540,862,299,030,000  5,969,782,550
            4  2021/01/11     91,000          1         2,200    2.48     90,000    96,800    89,500  90,306,177  8,379,237,727,064  543,250,212,050,000  5,969,782,550
        """
        result = self.read(isuCd=isin, strtDd=fromdate, endDd=todate)
        return DataFrame(result['output'])


class 전종목시세(KrxWebIo):
    @property
    def bld(self):
        return "dbms/MDC/STAT/standard/MDCSTAT01501"

    def fetch(self, date: str, market: str) -> DataFrame:
        """[12001] 전종목 시세

        Args:
            date (str): 조회 일자 (YYMMDD)
            market (str): 조회 시장 (STK/KSQ/KNX/ALL)

        Returns:
            DataFrame: 전종목의 가격 정보

                 ISU_SRT_CD    ISU_ABBRV  MKT_NM     SECT_TP_NM TDD_CLSPRC FLUC_TP_CD CMPPREVDD_PRC FLUC_RT TDD_OPNPRC TDD_HGPRC TDD_LWPRC ACC_TRDVOL     ACC_TRDVAL           MKTCAP    LIST_SHRS MKT_ID
            0        060310           3S  KOSDAQ     중견기업부      2,365          2            -5   -0.21      2,370     2,395     2,355    152,157    361,210,535  105,886,118,195   44,772,143    KSQ
            1        095570   AJ네트웍스   KOSPI                     5,400          1            70    1.31      5,330     5,470     5,260     90,129    485,098,680  252,840,393,000   46,822,295    STK
            2        068400     AJ렌터카   KOSPI                    12,000          1           400    3.45     11,600    12,000    11,550    219,282  2,611,434,750  265,755,600,000   22,146,300    STK
            3        006840     AK홀딩스   KOSPI                    55,000          1           800    1.48     54,700    55,300    53,600     16,541    901,619,600  728,615,855,000   13,247,561    STK
            4        054620    APS홀딩스  KOSDAQ     우량기업부      4,475          1            10    0.22      4,440     4,520     4,440     31,950    142,780,675   91,264,138,975   20,394,221    KSQ
        """
        result = self.read(mktId=market, trdDd=date)
        return DataFrame(result['OutBlock_1'])


class PER_PBR_배당수익률_전종목(KrxWebIo):
    @property
    def bld(self):
        return "dbms/MDC/STAT/standard/MDCSTAT03501"

    def fetch(self, date: str, market: str) -> DataFrame:
        """[12021] PER/PBR/배당수익률

        Args:
            date (str): 조회 일자 (YYMMDD)
            market (str): 조회 시장 (STK/KSQ/KNX/ALL)

        Returns:
            DataFrame:
                     ISU_SRT_CD   ISU_ABBRV                      ISU_ABBRV_STR TDD_CLSPRC FLUC_TP_CD CMPPREVDD_PRC FLUC_RT    EPS    PER     BPS   PBR  DPS DVD_YLD
                0        060310         3S            3S <em class ="up"></em>      2,195          1            20    0.92      -      -     745  2.95    0    0.00
                1        095570   AJ네트웍스  AJ네트웍스 <em class ="up"></em>      4,560          1            20    0.44    982   4.64   6,802  0.67  300    6.58
                2        006840    AK홀딩스     AK홀딩스 <em class ="up"></em>     27,550          1         2,150    8.46  2,168  12.71  62,448  0.44  750    2.72
                3        054620   APS홀딩스    APS홀딩스 <em class ="up"></em>      6,920          2          -250   -3.49      -      -  10,530  0.66    0    0.00
                4        265520    AP시스템     AP시스템 <em class ="up"></em>     25,600          1           600    2.40    671  38.15   7,468  3.43   50    0.20
        """
        result = self.read(mktId=market, trdDd=date)
        return DataFrame(result['output'])


class PER_PBR_배당수익률_개별(KrxWebIo):
    @property
    def bld(self):
        return "dbms/MDC/STAT/standard/MDCSTAT03502"

    def fetch(self, fromdate: str, todate: str, market: str, isin: str) -> DataFrame:
        """[12021] PER/PBR/배당수익률

        Args:
            fromdate (str): [description]
            todate (str): [description]
            market (str): [description]
            isin (str): [description]

        Returns:
            DataFrame:
                       TRD_DD TDD_CLSPRC FLUC_TP_CD CMPPREVDD_PRC FLUC_RT    EPS   PER     BPS   PBR  DPS DVD_YLD
                0  2019/03/29     44,650          2          -200   -0.45  5,997  7.45  28,126  1.59  850    1.90
                1  2019/03/28     44,850          2          -500   -1.10  5,997  7.48  28,126  1.59  850    1.90
                2  2019/03/27     45,350          1           100    0.22  5,997  7.56  28,126  1.61  850    1.87
                3  2019/03/26     45,250          2          -250   -0.55  5,997  7.55  28,126  1.61  850    1.88
                4  2019/03/25     45,500          2        -1,050   -2.26  5,997  7.59  28,126  1.62  850    1.87

        """
        result = self.read(mktId=market, strtDd=fromdate, endDd=todate, isuCd=isin)
        return DataFrame(result['output'])


class 전종목등락률(KrxWebIo):
    @property
    def bld(self):
        return "dbms/MDC/STAT/standard/MDCSTAT01602"

    def fetch(self, fromdate: str, todate: str, market: str, adjusted: int) -> DataFrame:
        """[12002] 전종목 등락률

        Args:
            fromdate (str): 조회 시작 일자 (YYMMDD)
            todate   (str): 조회 종료 일자 (YYMMDD)
            market   (str): 조회 시장 (STK/KSQ/ALL)
            adjusted (int): 수정 종가 여부 (2:수정종가/1:단순종가)

        Returns:
            DataFrame:
                  ISU_SRT_CD    ISU_ABBRV BAS_PRC TDD_CLSPRC CMPPREVDD_PRC FLUC_RT  ACC_TRDVOL       ACC_TRDVAL FLUC_TP
                0     060310           3S   2,420      3,290           870   35.95  40,746,975  132,272,050,410       1
                1     095570   AJ네트웍스   6,360      5,430          -930  -14.62   3,972,269   23,943,953,170       2
                2     068400     AJ렌터카  13,550     11,500        -2,050  -15.13  14,046,987  166,188,922,890       2
                3     006840     AK홀딩스  73,000     77,100         4,100    5.62   1,707,900  132,455,779,600       1
                4     054620    APS홀딩스   6,550      5,560          -990  -15.11   7,459,926   41,447,809,620       2
        """
        result = self.read(mktId=market, adj_stkprc=adjusted, strtDd=fromdate,
                           endDd=todate)
        return DataFrame(result['OutBlock_1'])


class 외국인보유량_전종목(KrxWebIo):
    @property
    def bld(self):
        return "dbms/MDC/STAT/standard/MDCSTAT03701"

    def fetch(self, date: str, market: str, islimit: int) -> DataFrame:
        """[12023] 외국인보유량(개별종목) - 전종목

        Args:
            market  (str): 조회 시장 (STK/KSQ/KNX/ALL)
            date    (str): 조회 일자 (YYMMDD)
            islimit (int): 외국인 보유제한 종목
            - 0 : check X
            - 1 : check O

        Returns:
            DataFrame:
                  ISU_SRT_CD   ISU_ABBRV TDD_CLSPRC FLUC_TP_CD CMPPREVDD_PRC FLUC_RT   LIST_SHRS FORN_HD_QTY FORN_SHR_RT FORN_ORD_LMT_QTY FORN_LMT_EXHST_RT
                0     060310          3S      2,185          2           -10   -0.46  44,802,511     739,059        1.65       44,802,511              1.65
                1     095570  AJ네트웍스      4,510          2           -50   -1.10  46,822,295   4,983,122       10.64       46,822,295             10.64
                2     006840    AK홀딩스     26,300          2        -1,250   -4.54  13,247,561   1,107,305        8.36       13,247,561              8.36
                3     054620   APS홀딩스      7,010          1            90    1.30  20,394,221     461,683        2.26       20,394,221              2.26
                4     265520    AP시스템     25,150          2          -450   -1.76  14,480,227   1,564,312       10.80       14,480,227             10.80
        """
        result = self.read(searchType=1, mktId=market, trdDd=date, isuLmtRto=islimit)
        return DataFrame(result['output'])


class 외국인보유량_개별추이(KrxWebIo):
    @property
    def bld(self):
        return "dbms/MDC/STAT/standard/MDCSTAT03702"

    def fetch(self, fromdate: str, todate: str, ticker: str) -> DataFrame:
        """[12023] 외국인보유량(개별종목) - 개별추이

        Args:
            date    (str): 조회 시작 일자 (YYMMDD)
            date    (str): 조회 종료 일자 (YYMMDD)
            ticker  (str): 종목 티커

        Returns:
            DataFrame:
                       TRD_DD TDD_CLSPRC FLUC_TP_CD CMPPREVDD_PRC FLUC_RT      LIST_SHRS    FORN_HD_QTY FORN_SHR_RT FORN_ORD_LMT_QTY FORN_LMT_EXHST_RT
                0  2021/01/15     88,000          2        -1,700   -1.90  5,969,782,550  3,317,574,926       55.57    5,969,782,550             55.57
                1  2021/01/14     89,700          3             0    0.00  5,969,782,550  3,314,652,740       55.52    5,969,782,550             55.52
                2  2021/01/13     89,700          2          -900   -0.99  5,969,782,550  3,316,551,070       55.56    5,969,782,550             55.56
                3  2021/01/12     90,600          2          -400   -0.44  5,969,782,550  3,318,676,206       55.59    5,969,782,550             55.59
                4  2021/01/11     91,000          1         2,200    2.48  5,969,782,550  3,324,115,988       55.68    5,969,782,550             55.68
        """
        result = self.read(searchType=2, strtDd=fromdate, endDd=todate, isuCd=ticker)
        return DataFrame(result['output'])


# ------------------------------------------------------------------------------------------
# index

class 전체지수기본정보(KrxWebIo):
    @property
    def bld(self):
        return "dbms/MDC/STAT/standard/MDCSTAT00401"

    def fetch(self, market: str) -> DataFrame:
        """[11004] 전체지수 기본정보

        Args:
            market (str): 검색할 시장
             - 01 : KRX
             - 02 : KOSPI
             - 03 : KOSDAQ
             - 04 : 테마

        Returns:
            DataFrame: [description]
                    IDX_NM   IDX_ENG_NM BAS_TM_CONTN ANNC_TM_CONTN BAS_IDX_CONTN CALC_CYCLE_CONTN        CALC_TM_CONTN COMPST_ISU_CNT IND_TP_CD IDX_IND_CD
            0      KRX 300      KRX 300   2010.01.04    2018.02.05      1,000.00              1초  09:00:10 ~ 15:30:00            300         5        300
            1      KTOP 30      KTOP 30   1996.01.03    2015.07.13        888.85              2초  09:00:10 ~ 15:30:00             30         5        600
            2      KRX 100      KRX 100   2001.01.02    2005.06.01      1,000.00              1초  09:00:10 ~ 15:30:00            100         5        042
        """
        result = self.read(idxIndMidclssCd=market)
        return DataFrame(result['output'])

class 주가지수검색(KrxWebIo):
    @property
    def bld(self):
        return "dbms/comm/finder/finder_equidx"

    def fetch(self, market: str) -> DataFrame:
        """[11004] 전체지수 기본정보

        Args:
            market (str): 검색 시장
             - 1 : 전체
             - 2 : KRX
             - 3 : KOSPI
             - 4 : KOSDAQ
             - 5 : 테마

        Returns:
            DataFrame:
                  full_code short_code     codeName marketCode marketName
                0         5        300      KRX 300        KRX        KRX
                1         5        600      KTOP 30        KRX        KRX
                2         5        042      KRX 100        KRX        KRX
                3         5        301  KRX Mid 200        KRX        KRX
                4         5        043   KRX 자동차        KRX        KRX

                marketCode : ['KRX' 'STK' 'KSQ' 'GBL']
                marketName : ['KRX' 'KOSPI' 'KOSDAQ' '테마']
        """
        result = self.read(mktsel=market)
        return DataFrame(result['block1'])


class 개별지수시세(KrxWebIo):
    @property
    def bld(self):
        return "dbms/MDC/STAT/standard/MDCSTAT00301"

    def fetch(self, ticker: str, group_id: str, fromdate: str, todate: str) -> DataFrame:
        """[11003] 개별지수 시세 추이

        Args:
            ticker   (str): index ticker
            group_id (str): index group id
            fromdate (str): 조회 시작 일자 (YYMMDD)
            todate   (str): 조회 종료 일자 (YYMMDD)

        Returns:
            DataFrame:
                       TRD_DD CLSPRC_IDX FLUC_TP_CD PRV_DD_CMPR UPDN_RATE OPNPRC_IDX HGPRC_IDX LWPRC_IDX  ACC_TRDVOL         ACC_TRDVAL               MKTCAP
                0  2021/01/15   2,298.05          2      -68.84     -2.91   2,369.94  2,400.69  2,292.92  22,540,416  1,967,907,809,615  137,712,088,395,380
                1  2021/01/14   2,366.89          2      -23.88     -1.00   2,390.59  2,393.24  2,330.76  23,685,783  2,058,155,913,335  142,206,993,223,695
                2  2021/01/13   2,390.77          1       25.68      1.09   2,367.94  2,455.05  2,300.10  33,690,790  3,177,416,322,985  144,549,058,033,310
                3  2021/01/12   2,365.09          2      -48.63     -2.01   2,403.51  2,428.76  2,295.91  41,777,076  3,933,263,957,150  143,250,319,286,660
                4  2021/01/11   2,413.72          1       33.32      1.40   2,403.37  2,613.83  2,352.21  50,975,686  6,602,833,901,895  146,811,113,380,140
        """
        result = self.read(indIdx2=ticker, indIdx=group_id, strtDd=fromdate, endDd=todate)
        return DataFrame(result['output'])

class 전체지수등락률(KrxWebIo):
    @property
    def bld(self):
        return "dbms/MDC/STAT/standard/MDCSTAT00201"

    def fetch(self, fromdate: str, todate: str, market: str) -> DataFrame:
        """[11002] 전체지수 등락률

        Args:
            fromdate (str): 조회 시작 일자 (YYMMDD)
            todate   (str): 조회 종료 일자 (YYMMDD)
            market   (str): 검색 시장
             - 01: KRX
             - 02: KOSPI
             - 03: KOSDAQ
             - 04: 테마

        Returns:
            DataFrame:
                        IDX_IND_NM OPN_DD_INDX END_DD_INDX FLUC_TP PRV_DD_CMPR FLUC_RT     ACC_TRDVOL           ACC_TRDVAL
                    0      KRX 300    1,845.82    1,920.52       1       74.70    4.05  3,293,520,227  201,056,395,899,602
                    1      KTOP 30   10,934.77   11,589.88       1      655.11    5.99    820,597,395  109,126,566,806,196
                    2      KRX 100    6,418.50    6,695.11       1      276.61    4.31  1,563,383,456  154,154,503,633,541
                    3  KRX Mid 200    1,751.19    1,722.32       2      -28.87   -1.65  2,807,696,801   27,059,313,040,039
                    4   KRX 자동차    2,046.67    2,298.05       1      251.38   12.28    288,959,592   29,886,192,965,797
        """
        result = self.read(idxIndMidclssCd=market, strtDd=fromdate, endDd=todate)
        return DataFrame(result['output'])


class 지수구성종목(KrxWebIo):
    @property
    def bld(self):
        return "dbms/MDC/STAT/standard/MDCSTAT00601"

    def fetch(self, date: str, ticker: str, group_id: str) -> DataFrame:
        """[11006] 지수구성종목

        Args:
            ticker   (str): index ticker
            group_id (str): index group id
            date     (str): 조회 일자 (YYMMDD)

        Returns:
            DataFrame:
                  ISU_SRT_CD     ISU_ABBRV TDD_CLSPRC FLUC_TP_CD STR_CMP_PRC FLUC_RT               MKTCAP MKTCAP_WT
                0     005930      삼성전자     46,850          1         600    1.30  279,684,312,467,500     19.54
                1     000660    SK하이닉스     78,100          2        -300   -0.38   56,856,984,706,500      3.97
                2     005380        현대차    126,500          2      -1,500   -1.17   27,029,025,655,500      1.89
                3     051910        LG화학    380,000          2      -8,500   -2.19   26,825,090,340,000      1.87
                4     068270      셀트리온    209,000          2      -1,500   -0.71   26,221,440,542,000      1.83
        """
        result = self.read(indIdx2=ticker, indIdx=group_id, trdDd=date)
        return DataFrame(result['output'])


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
        :param todate: 조회 종료 일자 (YYMMDD)
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
        :param todate: 조회 종료 일자 (YYMMDD)
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
        :param todate  : 조회 종료 일자 (YYMMDD)
        :param market  : 1 (코스피) / 2 (코스닥) / 6 (코넥스)
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
        :param todate  : 조회 종료 일자 (YYMMDD)
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
    # df = 개별종목시세().fetch("20210110", "20210115", "KR7005930003")
    # df = PER_PBR_배당수익률_전종목().fetch("20210115", "ALL")
    # df = PER_PBR_배당수익률_개별().fetch('20190322', '20190329', 'ALL', 'KR7005930003')
    # df = 전종목등락률().fetch("20180501", "20180801", "ALL", 2)
    # df = 외국인보유량_전종목().fetch("ALL", "20210115", 0)
    df = 외국인보유량_개별추이().fetch("20210108", "20210115", "KR7005930003")
    # index
    # df = 전체지수기본정보().fetch("04")
    # df = 전체지수등락률().fetch("20210107", "20210115", "01")
    # df = 주가지수검색().fetch("1")
    # df = 개별지수시세().fetch("043", "5", "20210107", "20210115")
    # df = 지수구성종목().fetch("20190412", "001", "1")

    print(df.head())
