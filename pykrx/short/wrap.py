from pykrx.short.core import *
from pykrx.comm.util import *
from pykrx.comm.ticker import KrxTicker
from datetime import datetime


class KrxShort:
    def __init__(self):
        self.ticker = KrxTicker()

    @dataframe_empty_handler
    def get_shorting_status_by_date(self, fromdate, todate, ticker):
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
        isin = self.ticker.get_isin(ticker)
        df = SRT02010100().read(fromdate, todate, isin)

        # (T+2)일 이전의 제공하기 때문에 (T), (T+1)의 비어있는 데이터를 제거
        elapsedTime = datetime.now() - datetime.strptime(todate + "0900", '%Y%m%d%H%M')
        day_offset = 2 - min(int(elapsedTime.total_seconds() / 3600 / 24), 2)
        df = df.iloc[day_offset:]

        df = df[['trd_dd', 'cvsrtsell_trdvol', 'str_const_val1', 'cvsrtsell_trdval', 'str_const_val2']]
        df.columns = ['날짜', '공매도', '잔고', '공매도금액', '잔고금액']

        # Index 설정
        df = df.replace('/', '', regex=True)
        df = df.set_index('날짜')
        df = df.replace('-', '0', regex=True)
        df = df.replace(',', '', regex=True)
        df = df.astype({"공매도": np.int32, "잔고": np.int32, "공매도금액": np.int64, "잔고금액": np.int64})
        # 상환량 추가
        # repay = df['잔고'].shift(-1) + df['공매도'] - df['잔고']
        # repay.iloc[-1] = 0
        # df['상환'] = repay.astype(np.int64)
        # df = df[['공매도', '상환', '잔고', '공매도금액', '잔고금액']]
        return df.sort_index()

    @dataframe_empty_handler
    def get_shorting_volume_by_ticker(self, fromdate, todate, ticker, market):
        """종목별 공매도 거래 현황 조회
        :param fromdate: 조회 시작 일자   (YYYYMMDD)
        :param todate  : 조회 마지막 일자 (YYYYMMDD)
        :param ticker  : 종목 번호 - None일 경우 전체 조회
        :param market  : 코스피/코스닥
        :return        : 거래 현황 DataFrame
                              종목명   공매도수량  총거래량  거래량비중
            2018/01/19    AJ네트웍스        6        40091         0.02
            2018/01/19      AJ렌터카    22164       519538         4.27
            2018/01/19      AK홀딩스     2413        41115         5.87
            2018/01/19           BGF     4193       299516         1.40
            2018/01/19     BGF리테일     1594        10446        15.26
        """
        isin = self.ticker.get_isin(ticker)
        market = {"코스피": 1, "코스닥": 3, "코넥스": 4}.get(market, 1)
        df = SRT02020100().read(fromdate, todate, market, isin)

        df = df[['trd_dd', 'isu_abbrv', 'cvsrtsell_trdvol', 'acc_trdvol', 'trdvol_wt']]
        df.columns = ['날짜', '종목명', '공매도수량', '총거래량', '거래량비중']

        df = df.replace('/', '', regex=True)
        df = df.replace(',', '', regex=True)
        df = df.set_index('날짜')
        df = df.astype({"공매도수량": np.int32, "총거래량": np.int32, "거래량비중": np.float32})
        return df.sort_index()

    @dataframe_empty_handler
    def get_shorting_volume_by_investor(self, fromdate, todate, market, inquery):
        """투자자별 공매도 거래 현황
        :param fromdate: 조회 시작 일자   (YYYYMMDD)
        :param todate  : 조회 마지막 일자 (YYYYMMDD)
        :param market  : 코스피/코스닥
        :param inquery : 거래량조회 / 거래대금조회
        :return        : 거래 현황 DataFrame
                         기관   개인   외국인   기타      합계
            날짜
            20180119  1161522  37396  6821963      0   8020881
            20180118   970406  41242  8018997  13141   9043786
            20180117  1190006  28327  8274090   6465   9498888
        """
        market = {"코스피": 1, "코스닥": 2, "코넥스": 6}.get(market, 1)
        inquery = {"거래대금": 1, "거래량": 2}.get(inquery, 1)
        df = SRT02020300().read(fromdate, todate, inquery, market)

        df = df[['str_const_val1', 'str_const_val2', 'str_const_val3', 'str_const_val4', 'str_const_val5', 'trd_dd']]
        df.columns = ['기관', '개인', '외국인', '기타', '합계', '날짜']

        df = df.replace('/', '', regex=True)
        df = df.set_index('날짜')
        df = df.replace(',', '', regex=True).astype(np.int32)
        return df.sort_index()

    @dataframe_empty_handler
    def get_shorting_volume_top50(self, date, market):
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

        df = df[['isu_abbrv', 'rank', 'cvsrtsell_trdval', 'acc_trdval', 'tdd_srtsell_wt',
                 'srtsell_trdval_avg', 'tdd_srtsell_trdval_incdec_rt', 'valu_pd_avg_srtsell_wt', 'srtsell_rto',
                 'prc_yd']]
        df.columns = ['종목명', '순위', '공매도거래대금', '총거래대금', '공매도비중', '직전40일거래대금평균',
                      '공매도거래대금증가율', '직전40일공매도평균비중', '공매도비중증가율', '주가수익률']
        df = df.set_index('종목명')

        df = df.replace(',', '', regex=True)
        df = df.astype({"순위": np.int32, "공매도거래대금": np.int64, "총거래대금": np.int64,
                        "직전40일거래대금평균": np.int64, "공매도비중": np.float32, "공매도거래대금증가율": np.float32,
                        "직전40일공매도평균비중": np.float32, "공매도비중증가율": np.float32, "주가수익률": np.float32})
        return df

    @dataframe_empty_handler
    def get_shorting_balance_by_ticker(self, fromdate, todate, ticker, market):
        """종목별 공매도 잔고 현황
        :param fromdate: 조회 시작 일자   (YYYYMMDD)
        :param todate  : 조회 마지막 일자 (YYYYMMDD)
        :param ticker  : 종목 번호
        :param market  : 코스피/코스닥
        :return        : 잔고 현황 DataFrame
                          공매도잔고  상장주식수   공매도금액        시가총액  비중
            2018/01/15        164825   728002365  11982777500  52925771935500  0.02
            2018/01/12        167043   728002365  12427999200  54163375956000  0.02
            2018/01/11        183158   728002365  13297270800  52852971699000  0.02
            2018/01/10        200200   728002365  14594580000  53071372408500  0.03
        """
        isin = self.ticker.get_isin(ticker)
        market = {"코스피": 1, "코스닥": 2, "코넥스": 6}.get(market, 1)
        df = SRT02030100().read(fromdate, todate, market, isin)

        df = df[['trd_dd', 'bal_qty', 'list_shrs', 'bal_amt', 'mktcap', 'bal_rto']]
        df.columns = ['날짜', '공매도잔고', '상장주식수', '공매도금액', '시가총액', '비중']

        df = df.replace('/', '', regex=True)
        df = df.replace(',', '', regex=True)
        df = df.set_index('날짜')
        df.index.name = ticker
        df = df.astype({"공매도잔고": np.int32, "상장주식수": np.int64, "공매도금액": np.int64, "시가총액": np.float32,
                        "비중": np.float32})
        return df.sort_index()

    @dataframe_empty_handler
    def get_shorting_balance_top50(self, date, market):
        """종목별 공매도 잔고 TOP 50
        :param date    : 조회 일자   (YYYYMMDD)
        :param market  : 코스피/코스닥
        :return        : 잔고 현황 DataFrame
                           종목명  순위  잔고수량 상장주식수      잔고금액        시가총액   비중
            20180105   한화테크윈     1   5323171   52600000  190835680350   1885710000000  10.12
            20180105     현대위아     2   2570962   27195083  161456413600   1707851212400   9.45
            20180105   두산중공업     3   9131232  106463061  147469396800   1719378435150   8.58
            20180105       GS건설     4   6002547   71675237  179776282650   2146673348150   8.38
        """
        market = {"코스피": 1, "코스닥": 2, "코넥스": 6}.get(market, 1)
        df = SRT02030400().read(date, market)

        df = df[['trd_dd', 'isu_abbrv', 'rank', 'bal_qty', 'list_shrs', 'bal_amt', 'mktcap', 'bal_rto']]
        df.columns = ['날짜', '종목명', '순위', '잔고수량', '상장주식수', '잔고금액', '시가총액', '비중']

        df = df.set_index('날짜')
        df = df.replace(',', '', regex=True)
        df = df.astype({"잔고수량": np.int32, "상장주식수": np.int64, "잔고금액": np.int64, "시가총액": np.int64,
                        "비중": np.float32})
        return df


if __name__ == "__main__":
    import pandas as pd
    pd.set_option('display.width', None)
    ks = KrxShort()
    df = ks.get_shorting_status_by_date("20190211", "20190215", "000660")
    print(df)
