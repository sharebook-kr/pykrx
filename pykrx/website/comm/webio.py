import requests
from abc import abstractmethod

class Get:
    def __init__(self):
        self.headers = {"User-Agent": "Mozilla/5.0", "Referer":"http://openapi.krx.co.kr/contents/OPP/USES/service/OPPUSES001_S2.cmd?BO_ID=SsgXTEspyJESKvyXZtCU"}

    def read(self, **params):
        resp = requests.get(self.url, headers=self.headers, params=params)
        return resp

    @property
    @abstractmethod
    def url(self):
        return NotImplementedError


class Post:
    def __init__(self, headers=None):
        self.headers = {"User-Agent": "Mozilla/5.0", "Referer":"http://openapi.krx.co.kr/contents/OPP/USES/service/OPPUSES001_S2.cmd?BO_ID=SsgXTEspyJESKvyXZtCU"}
        if headers is not None:
            self.headers.update(headers)

    def read(self, **params):
        resp = requests.post(self.url, headers=self.headers, data=params)
        return resp

    @property
    @abstractmethod
    def url(self):
        return NotImplementedError
