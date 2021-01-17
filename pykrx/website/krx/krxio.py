import io
from abc import abstractmethod
from pykrx.website.comm.webio import Get, Post
import logging

class MarketOtp(Get):
    @property
    def url(self):
        return "http://marketdata.krx.co.kr/contents/COM/GenerateOTP.jspx"


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


class KrxFileIo(Post):

    def __init__(self):
        super().__init__()
        self.headers.update(
            Referer="http://marketdata.krx.co.kr/mdi"
        )

    def post(self, **params):
        otp = MarketOtp().read(name="fileDown", filetype="xls", url=self.bld, **params)
        resp = super().read(code=otp.text)
        return io.BytesIO(resp.content)

    @property
    def url(self):
        return "http://file.krx.co.kr/download.jspx"

    @property
    @abstractmethod
    def bld(self):
        return NotImplementedError

    @property
    @abstractmethod
    def fetch(self, **params):
        return NotImplementedError

class ShortOtp(Get):
    @property
    def url(self):
        return "http://short.krx.co.kr/contents/COM/GenerateOTP.jspx"


class SrtWebIo(Post):
    def post(self, **params):
        otp = MarketOtp().read(name="form", bld=self.bld)
        params.update({"code": otp.text})
        resp = super().read(**params)
        return resp.json()

    @property
    def url(self):
        return "http://short.krx.co.kr/contents/SRT/99/SRT99000001.jspx"

    @property
    @abstractmethod
    def bld(self):
        return NotImplementedError

    @property
    @abstractmethod
    def read(self, **params):
        return NotImplementedError
