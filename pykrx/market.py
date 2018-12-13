from pykrx.core import *
from pandas import DataFrame
import numpy as np


class MarketDataHttp(KrxHttp):
    @property
    def otp_url(self):
        return "http://marketdata.krx.co.kr/contents/COM/GenerateOTP.jspx"

    @property
    def contents_url(self):
        return "http://marketdata.krx.co.kr/contents"


class KrxMarketStockFinder(MarketDataHttp, Singleton):
    '''
    @Brief : 30040 일자별 시세 스크래핑에서 종목 검색기
     - http://marketdata.krx.co.kr/mdi#document=040204
    @Param :
     - searchText : 검색할 종목명, 입력하지 않을 경우 전체
     - mktsel : ALL (전체) / STK (코스피) / KSQ (코스닥)
    '''
    @property
    def bld(self):
        return "COM/finder_stkisu"

    @property
    def uri(self):
        return "/MKD/99/MKD99000001.jspx"


class KrxMarketDailiyPrice(MarketDataHttp, Singleton):
    '''
    @Brief : 30040 일자별 시세
     - http://marketdata.krx.co.kr/mdi#document=040204
    @Param :
     - isu_cd : 조회할 종목의 ISIN 번호
     - fromdate : 조회 시작 일자 (YYYYMMDD)
     - todate : 조회 마지막 일자 (YYYYMMDD)
    '''
    @property
    def bld(self):
        return "MKD/04/0402/04020100/mkd04020100t3_02"

    @property
    def uri(self):
        return "/MKD/99/MKD99000001.jspx"


def get_stock_codes(market="전체", stock_name=""):
    market2mktsel = {"코스피": "STK", "코스닥": "KSQ", "전체": "ALL"}
    result = KrxMarketStockFinder().post(mktsel=market2mktsel.get(market, "ALL"),
                                         searchText=stock_name)
    # DataFrame으로 저장
    df = DataFrame(result['block1'])
    df.columns = ['종목', 'ISIN', '시장', '티커']
    df.set_index('종목', inplace=True)
    # - 티커 축약 (A037440 -> 037440)
    df['티커'] = df['티커'].apply(lambda x: x[1:])
    return df


def get_market_ohlcv(isin, name, fromdate, todate):
    try :
        result = KrxMarketDailiyPrice().post(isu_cd=isin, fromdate=fromdate, todate=todate)
        # DataFrame으로 저장
        df = DataFrame(result['block1'])
        df = df[['trd_dd', 'tdd_opnprc', 'tdd_hgprc', 'tdd_lwprc', 'tdd_clsprc', 'acc_trdvol']]
        df.columns = ['날짜', '시가', '고가', '저가', '종가', '거래량']
        df.set_index('날짜', inplace=True)
        df = df.replace({',': ''}, regex=True).astype(np.int64)
        df.index.name = name
        return df
    except (IndexError, KeyError):
        return None


if __name__ == "__main__":
    # k = get_stock_codes()
    # k = get_stock_codes(market="코스닥")
    # k = get_stock_codes(stock_name="삼성전자")
    # print(k)

    # k = KrxMarketStockFinder()
    # print(k.post(mktsel="ALL"))
    #
    # k = KrxMarketDailiyPrice()
    # print(k.post(isu_cd="KR7009150004", fromdate="20181201", todate="20181212"))

    pass