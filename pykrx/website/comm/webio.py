import requests
from abc import abstractmethod
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from abc import ABC


class Webio(ABC):
    def __init__(self):
        self.session = self._requests_retry_session()

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
            uri = "{}{}".format(self.base_url, self.uri)
            return self.session.post(url=uri, data=kwargs)
        except Exception as x:
            print("It failed", x.__class__.__name__)
            return None

    def get(self, **kwargs):
        try:
            # if self.header is not None :
            #     self.session.headers.update(self.header)
            uri = "{}{}".format(self.base_url, self.uri)
            return self.session.get(url=uri, params=kwargs)
        except Exception as x:
            print("It failed", x.__class__.__name__)
            return None

    @property
    @abstractmethod
    def base_url(self):
        return "http://marketdata.krx.co.kr/contents"

    @property
    def uri(self):
        return "/"

    @property
    def header(self):
        return {"User-Agent": "Mozilla/5.0"}

