from pykrx.website.comm import dataframe_empty_handler
from pykrx.website.krx.market.ticker import get_stock_ticker_isin
from pykrx.website.krx.market.core import (MKD30040, MKD80037, MKD30009_0,
                                           MKD30009_1, MKD20011, MKD20011_SUB,
                                           MKD20011_PDF, SRT02010100, MKD80002,
                                           SRT02020100, SRT02020300, MDK80033_0, MDK80033_1,
                                           SRT02020400, SRT02030100, SRT02030400
                                           )
import numpy as np
import pandas as pd
import datetime


################################################################################
# Market
@dataframe_empty_handler
def get_market_ohlcv_by_date(fromdate, todate, ticker):
    """일자별 OHLCV
    :param fromdate: 조회 시작 일자   (YYYYMMDD)
    :param todate  : 조회 마지막 일자 (YYYYMMDD)
    :param isin    : 조회 종목의 ticker
    :return        : OHLCV DataFrame
                     시가     고가    저가    종가   거래량
        20180208     97200   99700   97100   99300   813467
        20180207     98000  100500   96000   96500  1082264
        20180206     94900   96700   93400   96100  1094871
        20180205     99400   99600   97200   97700   745562
    """
    isin = get_stock_ticker_isin(ticker)
    df = MKD30040().read(fromdate, todate, isin)

    df = df[['trd_dd', 'tdd_opnprc', 'tdd_hgprc', 'tdd_lwprc',
             'tdd_clsprc', 'acc_trdvol']]
    df.columns = ['날짜', '시가', '고가', '저가', '종가', '거래량']
    df = df.replace('/', '', regex=True)
    df = df.replace(',', '', regex=True)
    df = df.set_index('날짜')
    df = df.astype(np.int32)
    df.index = pd.to_datetime(df.index, format='%Y%m%d')
    return df.sort_index()


@dataframe_empty_handler
def get_market_price_change_by_ticker(fromdate, todate, market="ALL"):
    """
    :param fromdate: 조회 시작 일자 (YYYYMMDD)
    :param todate  : 조회 종료 일자 (YYYYMMDD)
    :param market  : 조회 시장      (KOSPI/KOSDAQ/ALL)
    :return        : DataFrame
              종목명     시가    종가   대비   등락률   거래량      거래대금
    티커
    000020   동화약품   11550   11250   -300    -2.60   1510666   16851737550
    000030   우리은행   16050   15400   -650    -4.05  11623346  181243425100
    000040   KR모터스     667     717     50     7.50   9521456    6506765090
    000050       경방   14900   14200   -700    -4.70    526376    7608850250
    000060  메리츠화재  20950   20350   -600    -2.86   1094745   22498470632
    """
    market = {"ALL": "ALL", "KOSPI": "STK", "KOSDAQ": "KSQ", "KONEX": "KNX"}.\
        get(market, "ALL")    
    df = MKD80037().read(market, fromdate, todate)    
    

    df = df[['kor_shrt_isu_nm', 'isu_cd', 'opn_dd_end_pr', 'end_dd_end_pr',
             'prv_dd_cmpr', 'updn_rate', 'isu_tr_vl', 'isu_tr_amt']]
    df.columns = ['종목명', '티커', '시가', '종가', '변동폭',
                  '등락률', '거래량', '거래대금']
    df = df.set_index('티커')

    df = df.replace(',', '', regex=True)
    df = df.astype({"시가": np.int32, "종가": np.int32,
                    "변동폭": np.int32, "등락률": np.float64,
                    "거래량": np.int64, "거래대금": np.int64})
    return df


@dataframe_empty_handler
def get_market_fundamental_by_ticker(date, market="ALL"):
    """일자별 BPS/PER/PBR/배당수익률
    :param date    : 조회 일자 (YYYYMMDD)
                      20000101 이후의 데이터 제공
    :param market  : 조회 시장 (KOSPI/KOSDAQ/ALL)
    :return        : DataFrame
                       종목명   DIV    BPS      PER   EPS
        000250     삼천당제약  0.27   5689    44.19   422
        000440   중앙에너비스  2.82  37029    24.98  1135
        001000       신라섬유     0    563   247.27    11
        001540       안국약품  1.75  10036    84.00   150
        001810         무림SP  1.07   8266    24.06   117
    """
    market = {"ALL": "ALL", "KOSPI": "STK", "KOSDAQ": "KSQ", "KONEX": "KNX"}.\
        get(market, "ALL")    
    df = MKD30009_0().read(date, market)

    df = df[['isu_nm', 'isu_cd', 'dvd_yld', 'bps', 'per', 'prv_eps']]
    df.columns = ['종목명', '티커', 'DIV', 'BPS', 'PER', 'EPS']
    df.set_index('티커', inplace=True)

    df = df.replace('-', '0', regex=True)
    df = df.replace('', '0', regex=True)
    df = df.replace(',', '', regex=True)
    df = df.astype({"종목명": str, "DIV": np.float64, "BPS": np.int32,
                    "PER": np.float64, "EPS": np.int32}, )
    return df


@dataframe_empty_handler
def get_market_fundamental_by_date(fromdate, todate, isin, market="ALL"):
    """일자별 BPS/PER/PBR/배당수익률
    :param date    : 조회 일자 (YYYYMMDD)
                      20000101 이후의 데이터 제공
    :param market  : 조회 시장 (KOSPI/KOSDAQ/ALL)
    :return        : DataFrame
                       종목명   DIV    BPS      PER   EPS
        000250     삼천당제약  0.27   5689    44.19   422
        000440   중앙에너비스  2.82  37029    24.98  1135
        001000       신라섬유     0    563   247.27    11
        001540       안국약품  1.75  10036    84.00   150
        001810         무림SP  1.07   8266    24.06   117
    """
    market = {"ALL": "ALL", "KOSPI": "STK", "KOSDAQ": "KSQ", "KONEX": "KNX"}.\
        get(market, "ALL")    
    df = MKD30009_1().read(fromdate, todate, market, isin)
    df = df[['work_dt', 'dvd_yld', 'bps', 'per', 'prv_eps']]
    df.columns = ['날짜', 'DIV', 'BPS', 'PER', 'EPS']
    
    df = df.replace('-', '0', regex=True)
    df = df.replace('/', '', regex=True)
    df = df.replace('', '0', regex=True)
    df = df.replace(',', '', regex=True)
    df = df.astype({"DIV": np.float64, "BPS": np.int32,
                    "PER": np.float64, "EPS": np.int32}, )
    df = df.set_index('날짜')
    df.index = pd.to_datetime(df.index, format='%Y%m%d')
    return df.sort_index()


@dataframe_empty_handler
def get_market_trading_volume_by_date(fromdate, todate, market):
    """거래실적 추이(거래량)
    :param fromdate: 조회 시작 일자 (YYYYMMDD)
    :param todate  : 조회 종료 일자 (YYYYMMDD)
    :param market  :  KOSPI / KOSDQA / KONEX
    :return        : 거래실적 추이 DataFrame
                          전체        주권   투자회사  부동산투자회사
        2020-05-26  1017804023  1008972681   6436255  1438271
        2020-05-25   641458990   612379841  27522107  1021316
        2020-05-22   847467288   830997456  13472156  2173679
        2020-05-21   601199346   596327758   2325716  1780149
        2020-05-20   699446970   694915934   2694839   750944
        2020-05-19   773414630   768223852   2757578  1388145
    """
    market = {"KOSPI": 'kospi', "KOSDAQ": 'kosdaq', 'KONEX': 'konex'}.get(market, 'kospi')
    df = MDK80033_0().read(fromdate, todate, market)
    return _get_index_volume_by_date(df)


@dataframe_empty_handler
def get_market_trading_value_by_date(fromdate, todate, market):
    """거래실적 추이 (거래대금)
    :param fromdate: 조회 시작 일자 (YYYYMMDD)
    :param todate  : 조회 종료 일자 (YYYYMMDD)
    :param market  :  KOSPI / KOSDQA / KONEX
    :return        : 거래실적 추이 DataFrame (단위:원)
                          전체        주권   투자회사  부동산투자회사
        2020-05-26  12545313579  12524370281   3260524   7982490
        2020-05-25   7960324756   7936195529  14000492   5674769
        2020-05-22   9509771760   9484894049   6685432  12007486
        2020-05-21   8636997699   8619674049   1535003   9285644
        2020-05-20   8248632036   8233849994   1333854   3746196
        2020-05-19  11918300290  11900636354   1651565   6939447
    """
    market = {"KOSPI": 'kospi', "KOSDAQ": 'kosdaq', 'KONEX': 'konex'}.get(market, 'kospi')
    df = MDK80033_1().read(fromdate, todate, market)
    df = _get_index_volume_by_date(df)
    return df * 1000


################################################################################
# index
@dataframe_empty_handler
def get_index_ohlcv_by_date(fromdate, todate, id, market):
    """
    :param fromdate: 조회 시작 일자 (YYYYMMDD)
    :param todate  : 조회 종료 일자 (YYYYMMDD)
    :param id      : 코스피/코스피 벤치마크/코스피 200/코스피 100/
                     코스피 50/코스피 대형주/코스피 중형주/코스피 소형주
                      - 종합지수 - 코스피          (001)
                      - 종합지수 - 코스피 벤치마크 (100)
                      - 대표지수 - 코스피 200      (028)
                      - 대표지수 - 코스피 100      (034)
                      - 대표지수 - 코스피 50       (035)
                      - 규모별   - 코스피 대형주   (002)
                      - 규모별   - 코스피 중형주   (003)
                      - 규모별   - 코스피 소형주   (004)
                      - 생 략
    :param market  : KOSPI / KOSDAQ
    :return        : Kospi Index의 OHLCV DataFrame
                         시가         고가         저가         종가     거래량
        날짜
        20190125  2147.919922  2178.010010  2146.639893  2177.729980  410002000
        20190128  2184.409912  2188.149902  2169.169922  2177.300049  371619000
        20190129  2172.830078  2183.360107  2162.530029  2183.360107  552587000
        20190130  2183.489990  2206.199951  2177.879883  2206.199951  480390000
        20190131  2222.879883  2222.879883  2201.219971  2204.850098  545248000
    """
    market = {"KOSPI": 1, "KOSDAQ": 2}.get(market, 1)
    df = MKD20011_SUB().read(fromdate, todate, id, market)
    df = df[['trd_dd', 'opnprc_idx', 'hgprc_idx', 'lwprc_idx',
             'clsprc_idx', 'acc_trdvol']]
    df.columns = ['날짜', '시가', '고가', '저가', '종가', '거래량']
    df = df.replace(',', '', regex=True)
    df = df.replace('', '0', regex=True)
    df = df.replace('/', '', regex=True)
    df = df.set_index('날짜')
    df = df.astype({'시가': np.float64, '고가': np.float64,
                    '저가': np.float64, '종가': np.float64,
                    '거래량': np.int64})
    df.index = pd.to_datetime(df.index, format='%Y%m%d')
    df['거래량'] = df['거래량'] * 1000
    return df


@dataframe_empty_handler
def get_index_status_by_group(date, market):
    """시장지수
    :param date: 조회 일자 (YYYYMMDD)
    :return    : 시장 지수 DataFrame
                                   기준시점    발표시점 기준지수 현재지수    시가총액
        코스피                   1983.01.04  1980.01.04   100.0   2486.35  1617634318
        코스피 벤치마크           2015.09.14  2010.01.04  1696.0   2506.92  1554948117
        코스피 비중제한 8% 지수   2017.12.18  2015.01.02  1000.0   1272.93  1559869409
        코스피 200               1994.06.15  1990.01.03   100.0    327.13  1407647304
        코스피 100               2000.03.02  2000.01.04  1000.0   2489.34  1277592989
        코스피 50                2000.03.02  2000.01.04  1000.0   2205.53  1102490712
    """
    market = {"KOSPI": "02", "KOSDAQ": "03"}.get(market, "02")
    df = MKD20011().read(date, market)
    df = df[['idx_nm', 'annc_tm', 'bas_tm', 'bas_idx', 'prsnt_prc', 'idx_mktcap']]
    df.columns = ['지수명', '기준시점', '발표시점', '기준지수', '현재지수', '시가총액']
    df = df.set_index('지수명')
    df = df.replace(',', '', regex=True)
    df = df.replace('', 0)
    df = df.astype({"기준지수": float, "현재지수": float, "시가총액": int}, )
    return df


@dataframe_empty_handler
def get_index_price_change_by_name(fromdate, todate, market):
    """전체지수 등락률
    :param fromdate: 조회 시작 일자 (YYYYMMDD)
    :param todate  : 조회 종료 일자 (YYYYMMDD)
    :param market  :  KRX / KOSPI / KOSDQA
    :return        : 전체지수 등락률 DataFrame
                              시가        종가   등락률     거래량         거래대금
        코스닥지수            696.36    724.59   4.05  10488319776  62986196230829
        코스닥 150           1065.42   1102.57   3.49    729479528  18619100922088
        제조                 2266.76   2371.51   4.62   4855249693  27936884984652
        음식료·담배          9253.67   9477.11   2.41    156868081   1171238893745
        섬유·의류             141.03    147.24   4.40    107124162    449845448978
    """
    market = {"KRX": 2, "KOSPI": 3, "KOSDAQ": 4}.get(market, 3)
    df = MKD80002().read(fromdate, todate, market)
    df = df[['kor_indx_ind_nm', 'indx', 'prv_dd_indx', 'updn_rate', 'tr_vl', 'tr_amt']]
    df.columns = ['지수명', '시가', '종가', '등락률', '거래량', '거래대금']
    df = df.set_index('지수명')
    df = df.replace(',', '', regex=True)
    df = df.replace('', 0)
    df = df.astype({"시가": float, "종가": float, "등락률": float, "거래량": np.int64, "거래대금": np.int64})
    return df


def _get_index_volume_by_date(df):
    df = df[['dt', 'tot', 'stk', 'sect', 'reit']]
    df.columns = ['날짜', '전체', '주권', '투자회사', '부동산투자회사']
    df = df.set_index('날짜')
    # df.columns = pd.MultiIndex.from_arrays([['전체', '종류별', '종류별', '종류별'], ['', '주권', '투자회사',
    # '부동산투자회사']])
    df = df.replace(',', '', regex=True)
    df = df.replace('', 0)
    df = df.astype({"전체": np.int64, "주권": np.int64, "투자회사": np.int64, "부동산투자회사": np.int64})
    df.index = pd.to_datetime(df.index, format='%Y/%m/%d')
    return df


@dataframe_empty_handler
def get_index_portfolio_deposit_file(date, id, market):
    market = {"KOSPI": 1, "KOSDAQ": 2}.get(market, 1)
    df = MKD20011_PDF().read(date, id, market)
    return df['isu_cd'].tolist()


################################################################################
# Shorting
@dataframe_empty_handler
def get_shorting_status_by_date(fromdate, todate, isin):
    """일자별 공매도 종합 현황
    :param fromdate: 조회 시작 일자   (YYYYMMDD)
    :param todate  : 조회 마지막 일자 (YYYYMMDD)
    :param ticker  : 종목 번호
    :return        : 종합 현황 DataFrame
                  공매도    잔고   공매도금액     잔고금액
        날짜
        20180105   41726  177954   3303209900  14111752200
        20180108   32411  167754   2528196100  13118362800
        20180109   50486  175261   3885385100  13477570900
    """
    df = SRT02010100().read(fromdate, todate, isin)

    # (T+2)일 이전의 제공하기 때문에 (T), (T+1)의 비어있는 데이터를 제거
    today = datetime.datetime.now()
    # - today.isocalendar()[2] : 월(1)/화(2)/수(3)/목(4)/금(5)/토(6)/일(7) 반환
    # - base에는 최근 영업일이 저장
    base = today - datetime.timedelta(max(today.isocalendar()[2] - 5, 0))
    elapsedTime = base - datetime.datetime.strptime(todate + "0900",
                                                    '%Y%m%d%H%M')
    day_offset = 2 - min(int(elapsedTime.total_seconds() / 3600 / 24), 2)
    df = df.iloc[day_offset:]

    df = df[['trd_dd', 'cvsrtsell_trdvol', 'str_const_val1',
             'cvsrtsell_trdval', 'str_const_val2']]
    df.columns = ['날짜', '공매도', '잔고', '공매도금액', '잔고금액']
    df = df.replace('/', '', regex=True)
    df = df.set_index('날짜')
    df = df.replace('-', '0', regex=True)
    df = df.replace(',', '', regex=True)
    df = df.astype({"공매도": np.int32, "잔고": np.int32,
                    "공매도금액": np.int64, "잔고금액": np.int64})
    df.index = pd.to_datetime(df.index, format='%Y%m%d')
    return df.sort_index()


@dataframe_empty_handler
def get_shorting_volume_by_ticker(date, market="코스피"):
    """종목별 공매도 거래 현황 조회
    :param date: 조회 일자 (YYYYMMDD)
    :param market  : 코스피/코스닥
    :return        : 거래 현황 DataFrame
                       종목명   수량  거래량   비중
        000020       동화약품    454  196429   0.23
        000030       우리은행      0       0   0.00
        000040       KR모터스     69  175740   0.04
        000042   KR모터스 1WR      0    2795   0.00
        000050           경방    264   39956   0.66
    """
    market = {"코스피": 1, "코스닥": 3, "코넥스": 4}.get(market, 1)
    df = SRT02020100().read(date, date, market)
    df = df[
        ['isu_cd', 'isu_abbrv', 'cvsrtsell_trdvol', 'acc_trdvol', 'trdvol_wt']]
    df.columns = ['티커', '종목명', '수량', '거래량', '비중']

    df = df.replace('/', '', regex=True)
    df = df.replace(',', '', regex=True)
    df = df.set_index('티커')
    df.index = df.index.str[3:9]
    df = df.astype({"수량": np.int32, "거래량": np.int32, "비중": np.float64})
    return df.sort_index()


@dataframe_empty_handler
def get_shorting_investor_by_date(fromdate, todate, market, inquery="거래량"):
    """투자자별 공매도 거래 현황
    :param fromdate: 조회 시작 일자   (YYYYMMDD)
    :param todate  : 조회 마지막 일자 (YYYYMMDD)
    :param market  : 코스피/코스닥
    :param inquery : 거래량 / 거래대금
    :return        : 거래 현황 DataFrame
                     기관   개인   외국인   기타      합계
        날짜
        20180119  1161522  37396  6821963      0   8020881
        20180118   970406  41242  8018997  13141   9043786
        20180117  1190006  28327  8274090   6465   9498888
    """
    market = {"코스피": 1, "코스닥": 2, "코넥스": 6}.get(market, 1)
    inquery = {"거래량": 1, "거래대금": 2}.get(inquery, 1)
    df = SRT02020300().read(fromdate, todate, market, inquery)

    df = df[
        ['str_const_val1', 'str_const_val2', 'str_const_val3', 'str_const_val4',
         'str_const_val5', 'trd_dd']]
    df.columns = ['기관', '개인', '외국인', '기타', '합계', '날짜']

    df = df.replace('/', '', regex=True)
    df = df.set_index('날짜')
    df = df.replace(',', '', regex=True).astype(np.int64)
    df.index = pd.to_datetime(df.index, format='%Y%m%d')
    return df.sort_index()


@dataframe_empty_handler
def get_shorting_volume_top50(date, market="코스피"):
    """공매도 거래 비중 TOP 50
    :param date    : 조회 일자   (YYYYMMDD)
    :param market  : 코스피/코스닥/코넥스
    :return        : 거래 비중 DataFrame
                        순위 공매도거래대금 총거래대금 공매도비중 직전40일거래대금평균 공매도거래대금증가율 직전40일공매도평균비중 공매도비중증가율  주가수익률
        아모레퍼시픽      1  15217530000  35660149500  42.674   7945445875       1.915        14.834     2.877  0.334
        영원무역홀딩스    2     69700600    176886900  39.404     20449658       3.408         9.251     4.259  2.698
        한샘              3   9034795500  27690715500  32.628   2131924250       4.238        21.142     1.543 -5.233
        동서              4    701247550   2444863350  28.682    255763771       2.742        10.172     2.820 -0.530
    """
    market = {"코스피": 1, "코스닥": 2, "코넥스": 6}.get(market, 1)
    df = SRT02020400().read(date, market)

    df = df[['isu_abbrv', 'rank', 'cvsrtsell_trdval', 'acc_trdval',
             'tdd_srtsell_wt',
             'srtsell_trdval_avg', 'tdd_srtsell_trdval_incdec_rt',
             'valu_pd_avg_srtsell_wt', 'srtsell_rto',
             'prc_yd']]
    df.columns = ['종목명', '순위', '공매도거래대금', '총거래대금', '공매도비중', '직전40일거래대금평균',
                  '공매도거래대금증가율', '직전40일공매도평균비중', '공매도비중증가율', '주가수익률']
    df = df.set_index('종목명')

    df = df.replace(',', '', regex=True)
    df = df.astype({"순위": np.int32, "공매도거래대금": np.int64, "총거래대금": np.int64,
                    "직전40일거래대금평균": np.int64, "공매도비중": np.float64,
                    "공매도거래대금증가율": np.float64,
                    "직전40일공매도평균비중": np.float64, "공매도비중증가율": np.float64,
                    "주가수익률": np.float64})
    return df


@dataframe_empty_handler
def get_shorting_balance_by_ticker(fromdate, todate, isin, market="KOSPI"):
    """종목별 공매도 잔고 현황
    :param fromdate: 조회 시작 일자   (YYYYMMDD)
    :param todate  : 조회 마지막 일자 (YYYYMMDD)
    :param ticker  : 종목 번호
    :param market  : KOSPI/KOSDAQ
    :return        : 잔고 현황 DataFrame
                      공매도잔고  상장주식수   공매도금액        시가총액  비중
        2018/01/15        164825   728002365  11982777500  52925771935500  0.02
        2018/01/12        167043   728002365  12427999200  54163375956000  0.02
        2018/01/11        183158   728002365  13297270800  52852971699000  0.02
        2018/01/10        200200   728002365  14594580000  53071372408500  0.03
    """
    market = {"KOSPI": 1, "KOSDAQ": 2, "KONEX": 6}.get(market, 1)
    df = SRT02030100().read(fromdate, todate, market, isin)

    df = df[['trd_dd', 'bal_qty', 'list_shrs', 'bal_amt', 'mktcap', 'bal_rto']]
    df.columns = ['날짜', '공매도잔고', '상장주식수', '공매도금액', '시가총액', '비중']

    df = df.replace('/', '', regex=True)
    df = df.replace(',', '', regex=True)
    df = df.set_index('날짜')
    df = df.astype({"공매도잔고": np.int32, "상장주식수": np.int64, "공매도금액": np.int64,
                    "시가총액": np.int64,
                    "비중": np.float64})
    df.index = pd.to_datetime(df.index, format='%Y/%m/%d')
    return df.sort_index()


@dataframe_empty_handler
def get_shorting_balance_top50(date, market="KOSPI"):
    """종목별 공매도 잔고 TOP 50
    :param date    : 조회 일자   (YYYYMMDD)
    :param market  : KOSPI/KOSDAQ
    :return        : 잔고 현황 DataFrame
                       종목명    잔고수량  상장주식수      잔고금액        시가총액   비중
        009150        삼성전기   10074742   74693696  1077997394000   7992225472000  13.49
        042670   두산인프라코어  21415517  208158077   182674360010   1775588396810  10.29
        068270        셀트리온   11826917  125456133  2548700613500  27035796661500   9.43
        008770        호텔신라    3085595   39248121   223397078000   2841563960400   7.86
        001820       삼화콘덴서    617652   10395000    39220902000    660082500000   5.94
    """
    market = {"KOSPI": 1, "KOSDAQ": 2, "KONEX": 6}.get(market, 1)
    df = SRT02030400().read(date, market)

    df = df[["isu_cd", 'isu_abbrv', 'rank', 'bal_qty', 'list_shrs', 'bal_amt',
             'mktcap', 'bal_rto']]
    df.columns = ['티커', '종목명', '순위', '잔고수량', '주식수', '잔고금액', '시가총액', '비중']
    df['티커'] = df.티커.str[3:9]
    df = df.set_index('티커')

    df = df.replace(',', '', regex=True)
    df = df.astype(
        {"잔고수량": np.int32, "주식수": np.int64, "잔고금액": np.int64, "시가총액": np.int64,
         "비중": np.float64})
    return df


if __name__ == "__main__":
    pd.set_option('display.expand_frame_repr', False)
    # df = get_market_fundamental_by_ticker("20190401", "ALL")
    # df = get_market_ohlcv_by_date("20150720", "20150810", "005930")
    # df = get_market_price_change_by_ticker("20040418", "20040418")
    # df = get_market_price_change_by_ticker("20040418", "20040430")
    # df = get_market_fundamental_by_date("20150720", "20150810", "KR7005930003")

    # index
    # df = get_index_ohlcv_by_date("20190408", "20190412", "001", "KOSDAQ")
    # df = get_index_portfolio_deposit_file("20000104", "004", "KOSPI")
    # df = get_index_portfolio_deposit_file("20190410", "001", "KOSDAQ")
    # df = get_index_portfolio_deposit_file("20190410", "028", "KOSPI")
    # df = get_index_status_by_group("20190410", "KOSPI")
    # df = get_index_price_change_by_name("20200520", "20200527", "KOSDAQ")
    # df = get_market_trading_volume_by_date("20200519", "20200526", 'kospi')
    df = get_market_trading_value_by_date("20200519", "20200526", 'kospi')

    # shoring
    # df = get_shorting_status_by_date("20190401", "20190405", "KR7005930003")
    # df = get_shorting_volume_by_ticker("20190211", "코스닥")
    # df = get_shorting_investor_by_date("20190401", "20190405", "KR7005930003")
    # df = get_shorting_investor_by_date("20190401", "20190405", "KR7005930003", "거래대금")
    # df = get_shorting_volume_top50("20190211")
    # df = get_shorting_balance_by_ticker("20190211", "20190215", "KR7005930003")
    # df = get_shorting_balance_top50("20190401")
    print(df)
