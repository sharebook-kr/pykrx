import requests
from abc import abstractmethod


class Get:
    def read(self, **params):
        resp = requests.get(self.url, headers=self.headers, params=params)
        return resp

    @property
    def headers(self):
        return {"User-Agent": "Mozilla/5.0"}

    @property
    @abstractmethod
    def url(self):
        return NotImplementedError


class Post:
    def read(self, **params):
        resp = requests.post(self.url, headers=self.headers, data=params)
        return resp

    @property
    @abstractmethod
    def headers(self):
        return {"User-Agent": "Mozilla/5.0"}

    @property
    @abstractmethod
    def url(self):
        return NotImplementedError


