import requests
from abc import abstractmethod


class Get:
    def __init__(self):
        self.headers = {"User-Agent": "Mozilla/5.0"}

    def read(self, **params):
        resp = requests.get(self.url, headers=self.headers, params=params)
        return resp

    @property
    @abstractmethod
    def url(self):
        return NotImplementedError


class Post:
    def __init__(self):
        self.headers = {"User-Agent": "Mozilla/5.0"}

    def read(self, **params):
        resp = requests.post(self.url, headers=self.headers, data=params)
        return resp

    @property
    @abstractmethod
    def url(self):
        return NotImplementedError


