import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from abc import ABC, abstractmethod


class KrxHttp(ABC):
    def __init__(self):
        self.session = self._requests_retry_session()
        self.otp = self._get_otp_from_krx()

    def _get_otp_from_krx(self):
        url = "{}?bld={}&name={}".format(self.otp_url, self.bld, self.name)
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
            uri = "{}{}".format(self.contents_url, self.uri)
            kwargs.update({"code":self.otp})
            return self.session.post(url=uri, data=kwargs).json()
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
    def otp_url(self):
        return "http://marketdata.krx.co.kr/contents/COM/GenerateOTP.jspx"

    @property
    def contents_url(self):
        return "http://marketdata.krx.co.kr/contents"

    @property
    @abstractmethod
    def bld(self):
        raise NotImplementedError

    @property
    def uri(self):
        return "/MKD/99/MKD99000001.jspx"

    @property
    def name(self):
        return "form"

    @property
    def header(self):
        return None

