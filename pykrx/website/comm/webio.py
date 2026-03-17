from abc import abstractmethod

import requests

_session = None


def set_session(session):
    global _session
    _session = session


def get_session():
    return _session


class Get:
    def __init__(self):
        self.headers = {
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://data.krx.co.kr/contents/MDC/MDI/outerLoader/index.cmd",
        }

    def read(self, **params):
        session = get_session()
        if session is None:
            resp = requests.get(self.url, headers=self.headers, params=params)
        else:
            resp = session.get(self.url, headers=self.headers, params=params)
        return resp

    @property
    @abstractmethod
    def url(self):
        return NotImplementedError


class Post:
    def __init__(self, headers=None):
        self.headers = {
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://data.krx.co.kr/contents/MDC/MDI/outerLoader/index.cmd",
        }
        if headers is not None:
            self.headers.update(headers)

    def read(self, **params):
        session = get_session()
        if session is None:
            resp = requests.post(self.url, headers=self.headers, data=params)
        else:
            resp = session.post(self.url, headers=self.headers, data=params)
        return resp

    @property
    @abstractmethod
    def url(self):
        return NotImplementedError
