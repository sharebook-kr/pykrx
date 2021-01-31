import io
from abc import abstractmethod
from pykrx.website.comm.webio import Get, Post
import logging

class KrxWebIo(Post):
    def read(self, **params):
        params.update(bld=self.bld)
        resp = super().read(**params)
        return resp.json()

    @property
    def url(self):
        return "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"

    @property
    @abstractmethod
    def bld(self):
        return NotImplementedError

    @bld.setter
    def bld(self, val):
        pass

    @property
    @abstractmethod
    def fetch(self, **params):
        return NotImplementedError

