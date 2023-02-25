from abc import abstractmethod
from m_pykrx.website.comm.webio import Get, Post


class KrxFutureIo(Get):
    @property
    def url(self):
        return "http://data.krx.co.kr/comm/bldAttendant/executeForResourceBundle.cmd"

    def read(self, **params):
        resp = super().read(**params)
        return resp.json()

    @property
    @abstractmethod
    def fetch(self, **params):
        return NotImplementedError


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
