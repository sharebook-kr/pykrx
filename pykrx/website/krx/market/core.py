from pykrx.website.krx.krxio import KrxWebIo
import pandas as pd
from pandas import DataFrame

# ------------------------------------------------------------------------------------------
# Ticker

class 상장종목검색(KrxWebIo):
    @property
    def bld(self):
        return "dbms/comm/finder/finder_stkisu"

    def fetch(self, mktsel: str="ALL", searchText: str = "") -> DataFrame:
        """[12003] 개별종목 시세 추이에서 검색 버튼 눌러 활성화 되는 종목 검색창 스크래핑

        Args:
            mktsel     (str, optional): 조회 시장 (STK/KSQ/ALL)
            searchText (str, optional): 검색할 종목명 -  입력하지 않을 경우 전체

        Returns:
            DataFrame : 상장 종목 정보를 반환

                  full_code short_code    codeName marketCode marketName marketEngName ord1 ord2
            0  KR7060310000     060310          3S        KSQ     코스닥        KOSDAQ        16
            1  KR7095570008     095570  AJ네트웍스        STK   유가증권         KOSPI        16
            2  KR7006840003     006840    AK홀딩스        STK   유가증권         KOSPI        16
            3  KR7054620000     054620   APS홀딩스        KSQ     코스닥        KOSDAQ        16
            4  KR7265520007     265520    AP시스템        KSQ     코스닥        KOSDAQ        16
        """
        result = self.read(mktsel=mktsel, searchText=searchText, typeNo=0)
        return DataFrame(result['block1'])


class 상폐종목검색(KrxWebIo):
    @property
    def bld(self):

        return "dbms/comm/finder/finder_listdelisu"

    def fetch(self, mktsel:str = "ALL", searchText: str = "") -> DataFrame:
        """[20037] 상장폐지종목 현황
         - http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC02021301

        Args:
            mktsel     (str, optional): 조회 시장 (STK/KSQ/ALL) . Defaults to "ALL".
            searchText (str, optional): 검색할 종목명으로 입력하지 않을 경우 전체 조회

        Returns:
            DataFrame: 상장폐지 종목 정보를 반환

                         full_code short_code    codeName marketCode   marketName ord1 ord2
                0     KR7037730009     037730         3R        KSQ        코스닥        16
                1     KR7036360006     036360      3SOFT        KSQ        코스닥        16
                2     KYG887121070     900010 3노드디지탈       KSQ        코스닥        16
                3     KR7038120002     038120    AD모터스       KSQ        코스닥        16
        """
        result = self.read(mktsel=mktsel, searchText=searchText, typeNo=0)
        return DataFrame(result['block1'])

# ------------------------------------------------------------------------------------------
# Market

class 개별종목시세(KrxWebIo):
    @property
    def bld(self):
        return "dbms/MDC/STAT/standard/MDCSTAT01701"

    def fetch(self, strtDd: str, endDd: str, isuCd: str) -> DataFrame:
        """[12003] 개별종목 시세 추이 (수정종가 아님)

        Args:
            strtDd (str): 조회 시작 일자 (YYMMDD)
            endDd  (str): 조회 종료 일자 (YYMMDD)
            isuCd  (str): 조회 종목 ISIN

        Returns:
            DataFrame: 일자별 시세 조회 결과
                   TRD_DD TDD_CLSPRC FLUC_TP_CD CMPPREVDD_PRC FLUC_RT TDD_OPNPRC TDD_HGPRC TDD_LWPRC  ACC_TRDVOL         ACC_TRDVAL               MKTCAP      LIST_SHRS
            0  2021/01/15     88,000          2        -1,700   -1.90     89,800    91,800    88,000  33,431,809  2,975,231,937,664  525,340,864,400,000  5,969,782,550
            1  2021/01/14     89,700          3             0    0.00     88,700    90,000    88,700  26,393,970  2,356,661,622,700  535,489,494,735,000  5,969,782,550
            2  2021/01/13     89,700          2          -900   -0.99     89,800    91,200    89,100  36,068,848  3,244,066,562,850  535,489,494,735,000  5,969,782,550
            3  2021/01/12     90,600          2          -400   -0.44     90,300    91,400    87,800  48,682,416  4,362,546,108,950  540,862,299,030,000  5,969,782,550
            4  2021/01/11     91,000          1         2,200    2.48     90,000    96,800    89,500  90,306,177  8,379,237,727,064  543,250,212,050,000  5,969,782,550
        """
        result = self.read(isuCd=isuCd, strtDd=strtDd, endDd=endDd)
        return DataFrame(result['output'])


class 전종목시세(KrxWebIo):
    @property
    def bld(self):
        return "dbms/MDC/STAT/standard/MDCSTAT01501"

    def fetch(self, trdDd: str, mktId: str) -> DataFrame:
        """[12001] 전종목 시세

        Args:
            trdDd (str): 조회 일자 (YYMMDD)
            mktId (str): 조회 시장 (STK/KSQ/KNX/ALL)

        Returns:
            DataFrame: 전종목의 가격 정보

                 ISU_SRT_CD    ISU_ABBRV  MKT_NM     SECT_TP_NM TDD_CLSPRC FLUC_TP_CD CMPPREVDD_PRC FLUC_RT TDD_OPNPRC TDD_HGPRC TDD_LWPRC ACC_TRDVOL     ACC_TRDVAL           MKTCAP    LIST_SHRS MKT_ID
            0        060310           3S  KOSDAQ     중견기업부      2,365          2            -5   -0.21      2,370     2,395     2,355    152,157    361,210,535  105,886,118,195   44,772,143    KSQ
            1        095570   AJ네트웍스   KOSPI                     5,400          1            70    1.31      5,330     5,470     5,260     90,129    485,098,680  252,840,393,000   46,822,295    STK
            2        068400     AJ렌터카   KOSPI                    12,000          1           400    3.45     11,600    12,000    11,550    219,282  2,611,434,750  265,755,600,000   22,146,300    STK
            3        006840     AK홀딩스   KOSPI                    55,000          1           800    1.48     54,700    55,300    53,600     16,541    901,619,600  728,615,855,000   13,247,561    STK
            4        054620    APS홀딩스  KOSDAQ     우량기업부      4,475          1            10    0.22      4,440     4,520     4,440     31,950    142,780,675   91,264,138,975   20,394,221    KSQ
        """
        result = self.read(mktId=mktId, trdDd=trdDd)
        return DataFrame(result['OutBlock_1'])


class PER_PBR_배당수익률_전종목(KrxWebIo):
    @property
    def bld(self):
        return "dbms/MDC/STAT/standard/MDCSTAT03501"

    def fetch(self, trdDd: str, mktId: str) -> DataFrame:
        """[12021] PER/PBR/배당수익률

        Args:
            trdDd (str): 조회 일자 (YYMMDD)
            mktId (str): 조회 시장 (STK/KSQ/KNX/ALL)

        Returns:
            DataFrame:
                     ISU_SRT_CD   ISU_ABBRV                      ISU_ABBRV_STR TDD_CLSPRC FLUC_TP_CD CMPPREVDD_PRC FLUC_RT    EPS    PER     BPS   PBR  DPS DVD_YLD
                0        060310         3S            3S <em class ="up"></em>      2,195          1            20    0.92      -      -     745  2.95    0    0.00
                1        095570   AJ네트웍스  AJ네트웍스 <em class ="up"></em>      4,560          1            20    0.44    982   4.64   6,802  0.67  300    6.58
                2        006840    AK홀딩스     AK홀딩스 <em class ="up"></em>     27,550          1         2,150    8.46  2,168  12.71  62,448  0.44  750    2.72
                3        054620   APS홀딩스    APS홀딩스 <em class ="up"></em>      6,920          2          -250   -3.49      -      -  10,530  0.66    0    0.00
                4        265520    AP시스템     AP시스템 <em class ="up"></em>     25,600          1           600    2.40    671  38.15   7,468  3.43   50    0.20
        """
        result = self.read(mktId=mktId, trdDd=trdDd)
        return DataFrame(result['output'])


class PER_PBR_배당수익률_개별(KrxWebIo):
    @property
    def bld(self):
        return "dbms/MDC/STAT/standard/MDCSTAT03502"

    def fetch(self, strtDd: str, endDd: str, mktId: str, isuCd: str) -> DataFrame:
        """[12021] PER/PBR/배당수익률

        Args:
            strtDd (str): 조회 시작 일자 (YYMMDD)
            endDd  (str): 조회 종료 일자 (YYMMDD)
            mktId  (str): 조회 시장 (STK/KSQ/KNX/ALL)
            isuCd  (str): 조회 종목 ISIN

        Returns:
            DataFrame:
                       TRD_DD TDD_CLSPRC FLUC_TP_CD CMPPREVDD_PRC FLUC_RT    EPS   PER     BPS   PBR  DPS DVD_YLD
                0  2019/03/29     44,650          2          -200   -0.45  5,997  7.45  28,126  1.59  850    1.90
                1  2019/03/28     44,850          2          -500   -1.10  5,997  7.48  28,126  1.59  850    1.90
                2  2019/03/27     45,350          1           100    0.22  5,997  7.56  28,126  1.61  850    1.87
                3  2019/03/26     45,250          2          -250   -0.55  5,997  7.55  28,126  1.61  850    1.88
                4  2019/03/25     45,500          2        -1,050   -2.26  5,997  7.59  28,126  1.62  850    1.87

        """
        result = self.read(mktId=mktId, strtDd=strtDd, endDd=endDd, isuCd=isuCd)
        return DataFrame(result['output'])


class 전종목등락률(KrxWebIo):
    @property
    def bld(self):
        return "dbms/MDC/STAT/standard/MDCSTAT01602"

    def fetch(self, strtDd: str, endDd: str, mktId: str, adj_stkprc: int) -> DataFrame:
        """[12002] 전종목 등락률

        Args:
            strtDd     (str): 조회 시작 일자 (YYMMDD)
            endDd      (str): 조회 종료 일자 (YYMMDD)
            mktId      (str): 조회 시장 (STK/KSQ/ALL)
            adj_stkprc (int): 수정 종가 여부 (2:수정종가/1:단순종가)

        Returns:
            DataFrame:
                  ISU_SRT_CD    ISU_ABBRV BAS_PRC TDD_CLSPRC CMPPREVDD_PRC FLUC_RT  ACC_TRDVOL       ACC_TRDVAL FLUC_TP
                0     060310           3S   2,420      3,290           870   35.95  40,746,975  132,272,050,410       1
                1     095570   AJ네트웍스   6,360      5,430          -930  -14.62   3,972,269   23,943,953,170       2
                2     068400     AJ렌터카  13,550     11,500        -2,050  -15.13  14,046,987  166,188,922,890       2
                3     006840     AK홀딩스  73,000     77,100         4,100    5.62   1,707,900  132,455,779,600       1
                4     054620    APS홀딩스   6,550      5,560          -990  -15.11   7,459,926   41,447,809,620       2
        """
        result = self.read(mktId=mktId, adj_stkprc=adj_stkprc, strtDd=strtDd,
                           endDd=endDd)
        return DataFrame(result['OutBlock_1'])


class 외국인보유량_전종목(KrxWebIo):
    @property
    def bld(self):
        return "dbms/MDC/STAT/standard/MDCSTAT03701"

    def fetch(self, trdDd: str, mktId: str, isuLmtRto: int) -> DataFrame:
        """[12023] 외국인보유량(개별종목) - 전종목

        Args:
            trdDd     (str): 조회 일자 (YYMMDD)
            mktId     (str): 조회 시장 (STK/KSQ/KNX/ALL)
            isuLmtRto (int): 외국인 보유제한 종목
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
        result = self.read(searchType=1, mktId=mktId, trdDd=trdDd, isuLmtRto=isuLmtRto)
        return DataFrame(result['output'])


class 외국인보유량_개별추이(KrxWebIo):
    @property
    def bld(self):
        return "dbms/MDC/STAT/standard/MDCSTAT03702"

    def fetch(self, strtDd: str, endDd: str, isuCd: str) -> DataFrame:
        """[12023] 외국인보유량(개별종목) - 개별추이

        Args:
            strtDd (str): 조회 시작 일자 (YYMMDD)
            endDd  (str): 조회 종료 일자 (YYMMDD)
            isuCd  (str): 조회 종목 ISIN

        Returns:
            DataFrame:
                       TRD_DD TDD_CLSPRC FLUC_TP_CD CMPPREVDD_PRC FLUC_RT      LIST_SHRS    FORN_HD_QTY FORN_SHR_RT FORN_ORD_LMT_QTY FORN_LMT_EXHST_RT
                0  2021/01/15     88,000          2        -1,700   -1.90  5,969,782,550  3,317,574,926       55.57    5,969,782,550             55.57
                1  2021/01/14     89,700          3             0    0.00  5,969,782,550  3,314,652,740       55.52    5,969,782,550             55.52
                2  2021/01/13     89,700          2          -900   -0.99  5,969,782,550  3,316,551,070       55.56    5,969,782,550             55.56
                3  2021/01/12     90,600          2          -400   -0.44  5,969,782,550  3,318,676,206       55.59    5,969,782,550             55.59
                4  2021/01/11     91,000          1         2,200    2.48  5,969,782,550  3,324,115,988       55.68    5,969,782,550             55.68
        """
        result = self.read(searchType=2, strtDd=strtDd, endDd=endDd, isuCd=isuCd)
        return DataFrame(result['output'])


class 투자자별_거래실적_전체시장_기간합계(KrxWebIo):
    @property
    def bld(self):
        return "dbms/MDC/STAT/standard/MDCSTAT02201"

    def fetch(self, strtDd: str, endDd: str, mktId: str, etf: str, etn: str, els: str) -> DataFrame:
        """[12009] 투자자별 거래실적

        Args:
            strtDd (str): 조회 시작 일자 (YYMMDD)
            endDd  (str): 조회 종료 일자 (YYMMDD)
            mktId  (str): 조회 시장 (STK/KSQ/ALL)
            etf    (str): ETF 포함 여부 (""/EF)
            etn    (str): ETN 포함 여부 (""/EN)
            els    (str): ELS 포함 여부 (""/ES)

        Returns:
            DataFrame:
                     INVST_TP_NM      ASK_TRDVOL      BID_TRDVOL NETBID_TRDVOL           ASK_TRDVAL           BID_TRDVAL       NETBID_TRDVAL
                0       금융투자     183,910,512     173,135,582   -10,774,930   11,088,878,744,833   10,518,908,333,291    -569,970,411,542
                1           보험      18,998,546      11,995,538    -7,003,008    1,011,736,647,106      661,574,577,285    -350,162,069,821
                2           투신      78,173,801      64,724,900   -13,448,901    2,313,376,665,370    1,943,337,885,168    -370,038,780,202
                3           사모      37,867,724      33,001,267    -4,866,457    1,142,499,274,494    1,000,228,858,448    -142,270,416,046
                4           은행       3,252,303         901,910    -2,350,393       69,744,809,430       43,689,969,205     -26,054,840,225
        """
        result = self.read(strtDd=strtDd, endDd=endDd, mktId=mktId, etf=etf, etn=etn, elw=els)
        return DataFrame(result['output']).drop('CONV_OBJ_TP_CD', axis=1)


class 투자자별_거래실적_전체시장_일별추이_일반(KrxWebIo):
    @property
    def bld(self):
        return "dbms/MDC/STAT/standard/MDCSTAT02202"

    def fetch(self, strtDd: str, endDd: str, mktId: str, etf: str, etn: str, els: str, trdVolVal: int, askBid: int) -> DataFrame:
        """[12009] 투자자별 거래실적 일별추이

        Args:
            strtDd     (str): 조회 시작 일자 (YYMMDD)
            endDd      (str): 조회 종료 일자 (YYMMDD)
            mktId      (str): 조회 시장 (STK/KSQ/ALL)
            etf        (str): ETF 포함 여부 (""/EF)
            etn        (str): ETN 포함 여부 (""/EN)
            els        (str): ELS 포함 여부 (""/ES)
            trdVolVal  (int): 1: 거래량 / 2: 거래대금
            askBid     (int): 1: 매도 / 2: 매수 / 3: 순매수

        Returns:
            DataFrame:

                >> 투자자별_거래실적_전체시장_일별추이_일반().fetch("20210115", "20210122", "STK", "", "", "", 1, 1)

                       TRD_DD     TRDVAL1     TRDVAL2        TRDVAL3      TRDVAL4     TRDVAL_TOT
                0  2021/01/22  67,656,491   6,020,990    927,119,399  110,426,104  1,111,222,984
                1  2021/01/21  69,180,642  13,051,423  1,168,810,381  109,023,034  1,360,065,480
                2  2021/01/20  70,184,991   5,947,195  1,010,578,768  105,984,335  1,192,695,289
                3  2021/01/19  56,242,065   6,902,124  1,183,520,475  106,647,770  1,353,312,434
                4  2021/01/18  70,527,745   7,512,434  1,270,483,687  123,524,707  1,472,048,573
        """
        result = self.read(strtDd=strtDd, endDd=endDd, mktId=mktId, etf=etf, etn=etn, elw=els, inqTpCd=2, trdVolVal=trdVolVal,
                           askBid=askBid)
        return DataFrame(result['output'])


class 투자자별_거래실적_전체시장_일별추이_상세(KrxWebIo):
    @property
    def bld(self):
        return "dbms/MDC/STAT/standard/MDCSTAT02203"

    def fetch(self, strtDd: str, endDd: str, mktId: str, etf: str, etn: str, els: str, trdVolVal: int, askBid: int) -> DataFrame:
        """[12009] 투자자별 거래실적 일별추이 (상세)

        Args:
            strtDd     (str): 조회 시작 일자 (YYMMDD)
            endDd      (str): 조회 종료 일자 (YYMMDD)
            mktId      (str): 조회 시장 (STK/KSQ/ALL)
            etf        (str): ETF 포함 여부 (""/EF)
            etn        (str): ETN 포함 여부 (""/EN)
            els        (str): ELS 포함 여부 (""/ES)
            trdVolVal  (int): 1: 거래량 / 2: 거래대금
            askBid     (int): 1: 매도 / 2: 매수 / 3: 순매수

        Returns:
            DataFrame:

                >> 투자자별_거래실적_전체시장_일별추이_상세().fetch("20210115", "20210122", "STK", "", "", "", 1, 1)

                       TRD_DD     TRDVAL1    TRDVAL2    TRDVAL3    TRDVAL4  TRDVAL5    TRDVAL6     TRDVAL7     TRDVAL8        TRDVAL9     TRDVAL10   TRDVAL11     TRDVAL_TOT
                0  2021/01/22  27,190,933  2,735,154  8,774,207  3,338,979  454,546    170,392  24,992,280   6,020,990    927,119,399  108,740,962  1,685,142  1,111,222,984
                1  2021/01/21  18,482,914  3,032,118  6,625,819  3,543,737  635,314  8,696,961  28,163,779  13,051,423  1,168,810,381  106,653,326  2,369,708  1,360,065,480
                2  2021/01/20  25,584,466  2,530,140  8,106,713  4,204,627  182,144    137,315  29,439,586   5,947,195  1,010,578,768  103,998,394  1,985,941  1,192,695,289
                3  2021/01/19  13,992,565  2,122,324  7,740,948  2,736,919  391,860    419,021  28,838,428   6,902,124  1,183,520,475  103,967,576  2,680,194  1,353,312,434
                4  2021/01/18  22,645,478  2,471,112  6,761,600  2,867,429  263,984    196,148  35,321,994   7,512,434  1,270,483,687  120,350,740  3,173,967  1,472,048,573
        """
        result = self.read(strtDd=strtDd, endDd=endDd, mktId=mktId, etf=etf, etn=etn, elw=els, trdVolVal=trdVolVal,
                           askBid=askBid, detailView=1)
        return DataFrame(result['output'])


class 투자자별_거래실적_개별종목_기간합계(KrxWebIo):
    @property
    def bld(self):
        return "dbms/MDC/STAT/standard/MDCSTAT02301"

    def fetch(self, strtDd: str, endDd: str, isuCd: str) -> DataFrame:
        """[12009] 투자자별 거래실적(개별종목)

        Args:
            strtDd    (str): 조회 시작 일자 (YYMMDD)
            endDd     (str): 조회 종료 일자 (YYMMDD)
            isuCd     (str): 조회 종목 ISIN

        Returns:
            DataFrame:
                     INVST_TP_NM   ASK_TRDVOL   BID_TRDVOL NETBID_TRDVOL          ASK_TRDVAL          BID_TRDVAL       NETBID_TRDVAL
                0       금융투자   31,324,444   28,513,421    -2,811,023   2,765,702,311,200   2,510,494,630,400    -255,207,680,800
                1           보험    1,790,469      561,307    -1,229,162     158,120,209,600      49,570,523,900    -108,549,685,700
                2           투신    3,966,211    1,486,178    -2,480,033     351,753,222,200     130,513,380,300    -221,239,841,900
                3           사모      756,726      541,912      -214,814      67,202,238,800      47,475,872,700     -19,726,366,100
                4           은행      105,323       70,598       -34,725       9,360,874,400       6,170,507,400      -3,190,367,000
        """
        result = self.read(strtDd=strtDd, endDd=endDd, isuCd=isuCd, inqTpCd=1, trdVolVal=1, askBid=1)
        return DataFrame(result['output']).drop('CONV_OBJ_TP_CD', axis=1)


class 투자자별_거래실적_개별종목_일별추이_일반(KrxWebIo):
    @property
    def bld(self):
        return "dbms/MDC/STAT/standard/MDCSTAT02302"

    def fetch(self, strtDd: str, endDd: str, isuCd: str, trdVolVal: int, askBid: int) -> DataFrame:
        """[12009] 투자자별 거래실적(개별종목)

        Args:
            strtDd     (str): 조회 시작 일자 (YYMMDD)
            endDd      (str): 조회 종료 일자 (YYMMDD)
            isuCd      (str): 조회 종목 ISIN
            trdVolVal  (int): 1: 거래량 / 2: 거래대금
            askBid     (int): 1: 매도 / 2: 매수 / 3: 순매수

        Returns:
            DataFrame:
                       TRD_DD     TRDVAL1  TRDVAL2     TRDVAL3    TRDVAL4  TRDVAL_TOT
                0  2021/01/20  13,121,791  114,341   7,346,474  4,628,521  25,211,127
                1  2021/01/19  13,912,581  323,382  20,956,376  4,702,705  39,895,044
                2  2021/01/18  15,709,256  258,096  21,942,253  5,318,346  43,227,951
                3  2021/01/15  16,944,750  216,653  10,371,182  5,899,224  33,431,809
                4  2021/01/14  15,722,824  232,674   6,483,589  3,954,883  26,393,970
        """
        result = self.read(strtDd=strtDd, endDd=endDd, isuCd=isuCd, inqTpCd=2, trdVolVal=trdVolVal, askBid=askBid)
        return DataFrame(result['output'])


class 투자자별_거래실적_개별종목_일별추이_상세(KrxWebIo):
    @property
    def bld(self):
        return "dbms/MDC/STAT/standard/MDCSTAT02303"

    def fetch(self, strtDd: str, endDd: str, isuCd: str, trdVolVal: int, askBid: int) -> DataFrame:
        """[12009] 투자자별 거래실적(개별종목)

        Args:
            strtDd     (str): 조회 시작 일자 (YYMMDD)
            endDd      (str): 조회 종료 일자 (YYMMDD)
            isuCd      (str): 조회 종목 ISIN
            trdVolVal  (int): 1: 거래량 / 2: 거래대금
            askBid     (int): 1: 매도 / 2: 매수 / 3: 순매수

        Returns:
            DataFrame:
                       TRD_DD    TRDVAL1  TRDVAL2    TRDVAL3  TRDVAL4 TRDVAL5 TRDVAL6     TRDVAL7  TRDVAL8     TRDVAL9   TRDVAL10 TRDVAL11  TRDVAL_TOT
                0  2021/01/20  5,328,172  259,546    313,812   58,992   3,449     256   7,157,564  114,341   7,346,474  4,615,231   13,290  25,211,127
                1  2021/01/19  2,835,217  119,057    312,695   42,163  10,100     180  10,593,169  323,382  20,956,376  4,644,854   57,851  39,895,044
                2  2021/01/18  4,175,051  286,158    349,739   98,050  11,261   4,486  10,784,511  258,096  21,942,253  5,262,225   56,121  43,227,951
                3  2021/01/15  7,080,570  272,542    838,871  112,920   1,691  21,958   8,616,198  216,653  10,371,182  5,878,858   20,366  33,431,809
                4  2021/01/14  6,926,895  366,023    707,874   67,391  25,022  10,072   7,619,547  232,674   6,483,589  3,937,223   17,660  26,393,970
                5  2021/01/13  4,978,539  487,143  1,443,220  377,210  53,800  74,669  10,728,979  122,212   9,029,353  8,746,689   27,034  36,068,848
        """
        result = self.read(strtDd=strtDd, endDd=endDd, isuCd=isuCd, inqTpCd=2, trdVolVal=trdVolVal, askBid=askBid, detailView=1)
        return DataFrame(result['output'])


class 투자자별_순매수상위종목(KrxWebIo):
    @property
    def bld(self):
        return "dbms/MDC/STAT/standard/MDCSTAT02401"

    def fetch(self, strtDd: str, endDd: str, mktId: str, invstTpCd: str) -> DataFrame:
        """[12010] 투자자별 순매수상위종목

        Args:
            strtDd    (str): 조회 시작 일자 (YYMMDD)
            endDd     (str): 조회 종료 일자 (YYMMDD)
            mktId     (str): 조회 시장 (STK/KSQ/KNX/ALL)
            invstTpCd (str): 투자자
             - 1000 - 금융투자
             - 2000 - 보험
             - 3000 - 투신
             - 3100 - 사모
             - 4000 - 은행
             - 5000 - 기타금융
             - 6000 - 연기금
             - 7050 - 기관합계
             - 7100 - 기타법인
             - 8000 - 개인
             - 9000 - 외국인
             - 9001 - 기타외국인
             - 9999 - 전체

        Returns:
            DataFrame:
                     ISU_SRT_CD        ISU_NM   ASK_TRDVOL   BID_TRDVOL NETBID_TRDVOL         ASK_TRDVAL         BID_TRDVAL     NETBID_TRDVAL
                0        006400       삼성SDI    1,298,644    1,636,929       338,285    899,322,500,000  1,125,880,139,000   226,557,639,000
                1        051910        LG화학    1,253,147    1,492,717       239,570  1,166,498,517,000  1,371,440,693,000   204,942,176,000
                2        096770  SK이노베이션    4,159,038    4,823,863       664,825  1,050,577,437,000  1,208,243,272,500   157,665,835,500
                3        003670  포스코케미칼    1,093,803    1,973,179       879,376    129,914,349,500    240,577,561,000   110,663,211,500
        """
        result = self.read(strtDd=strtDd, endDd=endDd, mktId=mktId, invstTpCd=invstTpCd)
        return DataFrame(result['output'])


# ------------------------------------------------------------------------------------------
# index

class 전체지수기본정보(KrxWebIo):
    @property
    def bld(self):
        return "dbms/MDC/STAT/standard/MDCSTAT00401"

    def fetch(self, idxIndMidclssCd: str) -> DataFrame:
        """[11004] 전체지수 기본정보

        Args:
            idxIndMidclssCd (str): 검색할 시장
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
        result = self.read(idxIndMidclssCd=idxIndMidclssCd)
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


class 전체지수시세(KrxWebIo):
    @property
    def bld(self):
        return "dbms/MDC/STAT/standard/MDCSTAT00101"

    def fetch(self, trdDd: str, idxIndMidclssCd: str) -> DataFrame:
        """[11001] 전체지수 시세

        Args:
            trdDd           (str): 조회 시작 일자 (YYMMDD)            
            idxIndMidclssCd (str): 검색 시장
             - 01: KRX
             - 02: KOSPI
             - 03: KOSDAQ
             - 04: 테마

        Returns:
            DataFrame

            >> 전체지수시세().fetch("20211126", "01")

                        IDX_NM CLSPRC_IDX FLUC_TP_CD CMPPREVDD_IDX FLUC_RT OPNPRC_IDX  HGPRC_IDX  LWPRC_IDX   ACC_TRDVOL          ACC_TRDVAL                 MKTCAP
                0      KRX 300   1,770.31          2        -28.95   -1.61   1,794.31   1,804.44   1,765.64  195,701,215  12,073,173,651,861  2,021,911,406,190,965
                1      KTOP 30  10,474.88          2       -194.69   -1.82  10,621.85  10,677.23  10,448.27   35,084,853   3,590,583,333,556  1,110,875,284,982,930
                2      KRX 100   6,099.23          2        -98.21   -1.58   6,177.12   6,213.58   6,083.65  118,353,920   8,948,592,533,231  1,705,817,023,537,590
                3    KRX 자동차   2,072.14          2        -55.88   -2.63   2,122.03   2,122.81   2,067.39    8,993,829     490,044,360,120    125,029,489,225,115
                4    KRX 반도체   3,689.43          2        -55.31   -1.48   3,736.03   3,776.01   3,664.35   24,833,578   1,050,786,237,050    124,684,859,892,510

            >> 전체지수시세().fetch("20211126", "02")

                               IDX_NM CLSPRC_IDX FLUC_TP_CD CMPPREVDD_IDX FLUC_RT OPNPRC_IDX  HGPRC_IDX  LWPRC_IDX   ACC_TRDVOL          ACC_TRDVAL                 MKTCAP
                0  코스피 (외국주포함)          -                        -       -          -          -          -  595,597,647  11,901,297,731,572  2,167,444,597,231,403       
                1              코스피   2,936.44          2        -43.83   -1.47   2,973.04   2,985.77   2,930.31  594,707,257  11,894,910,355,357  2,165,631,236,658,233
                2          코스피 200     385.07          2         -6.86   -1.75     390.61     392.81     384.19  145,771,166   8,625,603,922,656  1,831,345,766,736,180
                3          코스피 100   2,906.68          2        -50.79   -1.72   2,947.18   2,963.27   2,900.41  100,357,121   7,370,285,846,691  1,661,265,294,441,780
                4           코스피 50   2,700.81          2        -45.96   -1.67   2,736.77   2,752.70   2,693.90   52,627,040   5,768,837,287,881  1,453,136,066,992,400
        """
        result = self.read(idxIndMidclssCd=idxIndMidclssCd, trdDd=trdDd)
        return DataFrame(result['output'])


class 전체지수등락률(KrxWebIo):
    @property
    def bld(self):
        return "dbms/MDC/STAT/standard/MDCSTAT00201"

    def fetch(self, strtDd: str, endDd: str, idxIndMidclssCd: str) -> DataFrame:
        """[11002] 전체지수 등락률

        Args:
            strtDd          (str): 조회 시작 일자 (YYMMDD)
            endDd           (str): 조회 종료 일자 (YYMMDD)
            idxIndMidclssCd (str): 검색 시장
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
        result = self.read(idxIndMidclssCd=idxIndMidclssCd, strtDd=strtDd, endDd=endDd)
        return DataFrame(result['output'])


class PER_PBR_배당수익률_전지수(KrxWebIo):
    @property
    def bld(self):
        return "dbms/MDC/STAT/standard/MDCSTAT00701"

    def fetch(self, trdDd: str, idxIndMidclssCd: str) -> DataFrame:
        """[11007] PER/PBR/배당수익률

        Args:
            trdDd           (str): 조회 일자 (YYMMDD)
            idxIndMidclssCd (str): 검색할 시장
             - 01 : KRX
             - 02 : KOSPI
             - 03 : KOSDAQ
             - 04 : 테마

        Returns:
            DataFrame:
                     IDX_NM   CLSPRC_IDX FLUC_TP_CD PRV_DD_CMPR FLUC_RT WT_PER FWD_PER WT_STKPRC_NETASST_RTO DIV_YD
            0       KRX 300     1,753.96          2      -16.35   -0.92  13.61       -                  1.24   2.01
            1       KTOP 30    10,348.84          2     -126.04   -1.20  12.67       -                  1.22   2.33
            2       KRX 100     6,045.16          2      -54.07   -0.89  13.42       -                  1.22   1.97
            3     KRX 자동차     2,030.72         2      -41.42   -2.00  11.94       -                  0.79   1.42
            4     KRX 반도체     3,649.78         2      -39.65   -1.07  21.48       -                  2.59   0.61        
        """
        result = self.read(idxIndMidclssCd=idxIndMidclssCd, trdDd=trdDd)
        return DataFrame(result['output'])


class PER_PBR_배당수익률_개별지수(KrxWebIo):
    @property
    def bld(self):
        return "dbms/MDC/STAT/standard/MDCSTAT00702"

    def fetch(self, strtDd: str, endDd: str, indTpCd: str, indTpCd2: str) -> DataFrame:
        """[11007] PER/PBR/배당수익률

        Args:
            strtDd   (str): 조회 시작 일자 (YYMMDD)
            endDd    (str): 조회 종료 일자 (YYMMDD)
            indTpCd  (str): index group id
            indTpCd2 (str): index ticker
     
        Returns:
            DataFrame:

                > PER_PBR_배당수익률_개별지수().fetch("20211122", "20211129", 5, 300)

                       TRD_DD CLSPRC_IDX FLUC_TP_CD PRV_DD_CMPR FLUC_RT WT_PER FWD_PER WT_STKPRC_NETASST_RTO DIV_YD
                0  2021/11/29   1,753.96          2      -16.35   -0.92  13.61       -                  1.24   2.01
                1  2021/11/26   1,770.31          2      -28.95   -1.61  13.73       -                  1.26   1.99
                2  2021/11/25   1,799.26          2      -16.10   -0.89  13.96       -                  1.28   1.96
                3  2021/11/24   1,815.36          2       -2.39   -0.13  14.08       -                  1.29   1.94
                4  2021/11/23   1,817.75          2      -14.91   -0.81  14.10       -                  1.29   1.94  
        """
        result = self.read(indTpCd=indTpCd, indTpCd2=indTpCd2, strtDd=strtDd, endDd=endDd)
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

            >> 지수구성종목().fetch("20210125", "001", "1"))

            DataFrame:

                    ISU_SRT_CD     ISU_ABBRV TDD_CLSPRC FLUC_TP_CD STR_CMP_PRC FLUC_RT               MKTCAP
                0       005930      삼성전자     89,400          1       2,600    3.00  533,698,559,970,000
                1       000660    SK하이닉스    135,000          1       6,500    5.06   98,280,319,275,000
                2       051910        LG화학    990,000          1      15,000    1.54   69,886,419,570,000
                3       035420         NAVER    349,000          1       5,500    1.60   57,327,924,855,000
        """
        result = self.read(indIdx2=ticker, indIdx=group_id, trdDd=date)
        return DataFrame(result['output'])


# ------------------------------------------------------------------------------------------
# shorting

class 개별종목_공매도_종합정보(KrxWebIo):
    @property
    def bld(self):
        return "dbms/MDC/STAT/srt/MDCSTAT30001"

    def fetch(self, strtDd: str, endDd: str, isuCd: str) -> DataFrame:
        """[31001] 개별종목 공매도 종합정보

        Args:
            strtDd (str): 조회 시작 일자 (YYMMDD)
            endDd  (str): 조회 종료 일자 (YYMMDD)
            isuCd  (str): 조회 종목 ISIN

        Returns:
            DataFrame:

                >> 개별종목_공매도_종합정보().fetch("20210101", "20210115", "KR7005930003")

                       TRD_DD CVSRTSELL_TRDVOL STR_CONST_VAL1 CVSRTSELL_TRDVAL   STR_CONST_VAL2
                0  2021/01/15                0      3,365,984                0  296,206,592,000
                1  2021/01/14            1,432      3,374,585      127,498,700  302,700,274,500
                2  2021/01/13              228      3,268,098       20,571,200  293,148,390,600
                3  2021/01/12            5,144      3,659,530      466,020,200  331,553,418,000
                4  2021/01/11              204      3,152,160       18,686,400  286,846,560,000
        """
        result = self.read(isuCd=isuCd, strtDd=strtDd, endDd=endDd)
        return DataFrame(result['OutBlock_1'])


class 개별종목_공매도_거래_전종목(KrxWebIo):
    @property
    def bld(self):
        return "dbms/MDC/STAT/srt/MDCSTAT30101"

    def fetch(self, trdDd: str, mktId: str, secugrpId: list) -> DataFrame:
        """[32001] 개별종목 시세 추이

        Args:
            trdDd (str): 조회 일자 (YYMMDD)
            mktId (str): 조회 시장 (STK/KSQ/KNX)
            secugrpId (str): 증권구분 옵션을 리스트로 지정
              - STMFRTSCIFDRFS : 주식
              - EF             : ETF
              - EN             : ETN
              - EW             : ELW
              - SRSW           : 신주인수권증서및증권
              - BC             : 수익증권

        Returns:
            DataFrame:

                >> 개별종목_공매도_거래_전종목().fetch("20210125", "STK", ["STMFRTSCIFDRFS"])

                     ISU_CD    ISU_ABBRV SECUGRP_NM CVSRTSELL_TRDVOL ACC_TRDVOL TRDVOL_WT CVSRTSELL_TRDVAL      ACC_TRDVAL TRDVAL_WT
                0    095570   AJ네트웍스       주권               32    180,458      0.02          134,240     757,272,515      0.02
                1    006840     AK홀딩스       주권               79    386,257      0.02        2,377,900  11,554,067,000      0.02
                2    027410          BGF       주권           18,502  8,453,962      0.22      108,713,300  49,276,275,460      0.22
                3    282330    BGF리테일       주권               96     82,986      0.12       14,928,000  13,018,465,500      0.11
                4    138930  BNK금융지주       주권            1,889  1,181,748      0.16       10,635,610   6,658,032,800      0.16
        """
        result = self.read(trdDd=trdDd, mktId=mktId, inqCond="".join(secugrpId))
        return DataFrame(result['OutBlock_1'])


class 개별종목_공매도_거래_개별추이(KrxWebIo):
    @property
    def bld(self):
        return "dbms/MDC/STAT/srt/MDCSTAT30102"

    def fetch(self, strtDd: str, endDd: str, isuCd: str) -> DataFrame:
        """[32001] 개별종목 시세 추이

        Args:
            strtDd (str): 조회 시작 일자 (YYMMDD)
            endDd  (str): 조회 종료 일자 (YYMMDD)
            isuCd  (str): 조회 종목 ISIN

        Returns:
            DataFrame:

                >> 개별종목_공매도_거래_개별추이().fetch("20201226", "20210126", "KR7060310000"))

                        TRD_DD CVSRTSELL_TRDVOL ACC_TRDVOL TRDVOL_WT CVSRTSELL_TRDVAL     ACC_TRDVAL TRDVAL_WT
                0   2021/01/26                0  1,665,933       0.0                0  3,927,562,180       0.0
                1   2021/01/25                0    328,833       0.0                0    748,091,530       0.0
                2   2021/01/22                0    275,425       0.0                0    619,653,305       0.0
                3   2021/01/21                0    283,660       0.0                0    638,360,995       0.0
                4   2021/01/20                0    210,629       0.0                0    467,705,100       0.0
        """

        result = self.read(strtDd=strtDd, endDd=endDd, isuCd=isuCd)
        return DataFrame(result['OutBlock_1'])


class 투자자별_공매도_거래(KrxWebIo):
    @property
    def bld(self):
        return "dbms/MDC/STAT/srt/MDCSTAT30301"

    def fetch(self, strtDd: str, endDd: str, inqCondTpCd: int, mktTpCd: int) -> DataFrame:
        """[32003] 투자자별 공매도 거래

        Args:
            strtDd      (str): 조회 시작 일자 (YYMMDD)
            endDd       (str): 조회 종료 일자 (YYMMDD)
            inqCondTpCd (int): 조회 구분 (1:거래량/2:거래대금)
            mktTpCd     (int): 조회 시장 (1:KOSPI/2:KOSDAQ/3:KONEX)

        Returns:
            DataFrame:

                >> 투자자별_공매도_거래().fetch("20201226", "20210126", 1, 1))

                        TRD_DD STR_CONST_VAL1 STR_CONST_VAL2 STR_CONST_VAL3 STR_CONST_VAL4 STR_CONST_VAL5
                0   2021/01/26        478,268              0              0              0        478,268
                1   2021/01/25        303,055              0              0              0        303,055
                2   2021/01/22        356,044              0              0              0        356,044
                3   2021/01/21        422,529              0              0              0        422,529
                4   2021/01/20        206,970              0              0              0        206,970

                >> 투자자별_공매도_거래().fetch("20201226", "20210126", 2, 1))

                        TRD_DD  STR_CONST_VAL1 STR_CONST_VAL2 STR_CONST_VAL3 STR_CONST_VAL4  STR_CONST_VAL5
                0   2021/01/26  13,770,392,399              0              0              0  13,770,392,399
                1   2021/01/25  12,165,731,063              0              0              0  12,165,731,063
                2   2021/01/22  13,621,266,642              0              0              0  13,621,266,642
                3   2021/01/21  13,072,231,798              0              0              0  13,072,231,798
                4   2021/01/20   7,703,123,621              0              0              0   7,703,123,621
        """

        result = self.read(strtDd=strtDd, endDd=endDd, inqCondTpCd=inqCondTpCd, mktTpCd=mktTpCd)
        return DataFrame(result['OutBlock_1'])


class 공매도_거래상위_50종목(KrxWebIo):
    @property
    def bld(self):
        return "dbms/MDC/STAT/srt/MDCSTAT30401"

    def fetch(self, trdDd: str, mktTpCd: int) -> DataFrame:
        """[32004] 공매도 거래 상위 50종목

        Args:
            trdDd   (str): 조회 일자 (YYMMDD)
            mktTpCd (int): 시장구분 (1:KOSPI/2:KOSDAQ/3:KONEX)

        Returns:

            >> 공매도_거래상위_50종목().fetch("20210129", 1))

            DataFrame:

                   RANK  ISU_CD            ISU_ABBRV CVSRTSELL_TRDVAL      ACC_TRDVAL TDD_SRTSELL_WT STR_CONST_VAL1 STR_CONST_VAL2 VALU_PD_AVG_SRTSELL_WT VALU_PD_CMP_TDD_SRTSELL_RTO PRC_YD
                0     1  003545           대신증권우       38,510,030     915,824,030           4.21      5,814,411           6.62                   0.51                        8.33  -1.25
                1     2  267290         경동도시가스       13,265,200     329,805,000           4.02      2,755,259           4.82                   0.66                        6.14  -2.46
                2     3  015890             태경산업       15,865,860     428,852,660           3.70      8,316,412           1.91                   1.30                        2.85  -4.46
                3     4  005945         NH투자증권우       25,401,240     908,915,950           2.79      4,610,634           5.51                   0.44                        6.40  -0.35
                4     5  227840 현대코퍼레이션홀딩스       13,784,400     546,597,900           2.52      3,084,294           4.47                   0.51                        4.91  -2.37
        """
        result = self.read(trdDd=trdDd, mktTpCd=mktTpCd)
        return DataFrame(result['OutBlock_1'])


class 공매도_잔고상위_50종목(KrxWebIo):
    @property
    def bld(self):
        return "dbms/MDC/STAT/srt/MDCSTAT30801"

    def fetch(self, trdDd: str, mktTpCd: int) -> DataFrame:
        """[33004] 공매도 잔고 상위 50종목

        Args:
            trdDd   (str): 조회 일자 (YYMMDD)
            mktTpCd (int): 시장구분 (1:KOSPI/2:KOSDAQ/3:KONEX)

        Returns:

            DataFrame:

                >> 공매도_잔고상위_50종목().fetch("20210129", 1))

                       RANK  ISU_CD       ISU_CD2       ISU_ABBRV     BAL_QTY    LIST_SHRS            BAL_AMT              MKTCAP BAL_RTO
                    0     1  032350  KR7032350001    롯데관광개발   4,693,027   69,275,662     74,853,780,650   1,104,946,808,900    6.77
                    1     2  042670  KR7042670000  두산인프라코어  10,846,251  215,931,625     92,843,908,560   1,848,374,710,000    5.02
                    2     3  068270  KR7068270008        셀트리온   6,523,965  134,997,805  2,146,384,485,000  44,414,277,845,000    4.83
                    3     4  008770  KR7008770000        호텔신라   1,269,261   39,248,121    106,237,145,700   3,285,067,727,700    3.23
                    4     5  011690  KR7011690005      유양디앤유   1,604,890   58,494,201      1,957,965,800      71,362,925,220    2.74
        """
        result = self.read(trdDd=trdDd, mktTpCd=mktTpCd)
        return DataFrame(result['OutBlock_1'])


class 전종목_공매도_잔고(KrxWebIo):
    @property
    def bld(self):
        return "dbms/MDC/STAT/srt/MDCSTAT30501"

    def fetch(self, trdDd: str, mktTpCd: int) -> DataFrame:
        """[33001] 개별종목 공매도 잔고 (전종목)

        Args:
            trdDd   (str): 조회 일자 (YYMMDD)
            mktTpCd (int): 시장구분 (1:KOSPI/2:KOSDAQ/3:KONEX)

        Returns:

            DataFrame:

                >> 전종목_공매도_잔고().fetch("20210127", 1)

                     ISU_CD      ISU_ABBRV  BAL_QTY    LIST_SHRS        BAL_AMT             MKTCAP BAL_RTO
                0    095570     AJ네트웍스   33,055   46,822,295    134,864,400    191,034,963,600    0.07
                1    006840       AK홀딩스    4,575   13,247,561    131,760,000    381,529,756,800    0.03
                2    027410            BGF   68,060   95,716,791    449,196,000    631,730,820,600    0.07
                3    282330      BGF리테일    4,794   17,283,906    757,452,000  2,730,857,148,000    0.03
                4    138930    BNK금융지주  596,477  325,935,246  3,340,271,200  1,825,237,377,600    0.18
        """
        result = self.read(trdDd=trdDd, mktTpCd=mktTpCd)
        return DataFrame(result['OutBlock_1'])

class 개별종목_공매도_잔고(KrxWebIo):
    @property
    def bld(self):
        return "dbms/MDC/STAT/srt/MDCSTAT30502"

    def fetch(self, strtDd: str, endDd: str, isuCd: str) -> DataFrame:
        """[33001] 개별종목 공매도 잔고 (개별종목)

        Args:
            strtDd (str): 조회 시작 일자 (YYMMDD)
            endDd  (str): 조회 종료 일자 (YYMMDD)
            isuCd  (str): 조회 종목 ISIN

        Returns:

            DataFrame:

                >> 개별종목_공매도_잔고().fetch("20200106", "20200110", "KR7005930003")

                  RPT_DUTY_OCCR_DD    BAL_QTY      LIST_SHRS          BAL_AMT               MKTCAP BAL_RTO
                0       2020/01/10  5,489,240  5,969,782,550  326,609,780,000  355,202,061,725,000    0.09
                1       2020/01/09  5,387,073  5,969,782,550  315,682,477,800  349,829,257,430,000    0.09
                2       2020/01/08  5,224,233  5,969,782,550  296,736,434,400  339,083,648,840,000    0.09
                3       2020/01/07  5,169,745  5,969,782,550  288,471,771,000  333,113,866,290,000    0.09
                4       2020/01/06  5,630,893  5,969,782,550  312,514,561,500  331,322,931,525,000    0.09
        """
        result = self.read(strtDd=strtDd, endDd=endDd, isuCd=isuCd)
        return DataFrame(result['OutBlock_1'])


if __name__ == "__main__":
    pd.set_option('display.width', None)
    # print(개별종목_공매도_잔고().fetch("20200106", "20200110", "KR7005930003"))
    print(PER_PBR_배당수익률_개별지수().fetch("20211122", "20211129", 5, 300))

