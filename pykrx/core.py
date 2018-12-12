import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from abc import ABC, abstractmethod


class KrxHttp(ABC):
    def __init__(self):
        self.session = self._requests_retry_session()
        self._otp_url = "http://marketdata.krx.co.kr/contents/COM/GenerateOTP.jspx"
        self._data_base_url = "http://marketdata.krx.co.kr/contents"
        self.otp = self._get_otp_from_krx()

    def _get_otp_from_krx(self):
        url = "{}?bld={}&name={}".format(self._otp_url, self.bld, self.name)
        return self.session.get(url=url).text

    @staticmethod
    def _requests_retry_session(retries=5, backoff_factor=0.3, status_forcelist=(500, 502, 504), session=None):
        s = session or requests.Session()
        retry = Retry(total=retries, read=retries, connect=retries, backoff_factor=backoff_factor,
                      status_forcelist=status_forcelist)
        adapter = HTTPAdapter(max_retries=retry)
        s.mount('http://', adapter)
        s.mount('https://', adapter)
        return s

    def post(self, **kwargs):
        try:
            if self.header is not None :
                self.session.headers.update(self.header)
            uri = "{}{}".format(self._data_base_url, self.uri)
            kwargs.update({"code":self.otp})
            return self.session.post(url=uri, data=kwargs, timeout=3).json()
        except Exception as x:
            print("It failed", x.__class__.__name__)
            return None

    def get(self, path, timeout=3, **kwargs):
        try:
            if self.header is not None :
                self.session.headers.update(self.header)
            raise NotImplementedError
            #return self.session.get(url=uri, params=kwargs, timeout=timeout).json()
        except Exception as x:
            print("It failed", x.__class__.__name__)
            return None

    @property
    @abstractmethod
    def bld(self):
        pass

    @property
    def name(self):
        return "form"

    @property
    @abstractmethod
    def uri(self):
        pass

    @property
    def header(self):
        return None


class Singleton(object):
    _instance = None
    def __new__(class_, *args, **kwargs):
        if not isinstance(class_._instance, class_):
            class_._instance = object.__new__(class_, *args, **kwargs)
        return class_._instance


class KrxStockFinder(KrxHttp, Singleton):
    '''
    @Brief : 30040 일자별 시세 스크래핑에서 종목 검색기 스크래핑
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


class KrxDailiyPrice(KrxHttp, Singleton):
    '''
    @Brief : 30040 일자별 시세 스크래핑
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


if __name__ == "__main__":
    k = KrxStockFinder()
    print(k.post(mktsel="ALL"))

    k = KrxDailiyPrice()
    print(k.post(isu_cd="KR7009150004", fromdate="20181201", todate="20181212"))
