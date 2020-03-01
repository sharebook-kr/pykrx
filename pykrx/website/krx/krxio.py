from abc import abstractmethod
from pykrx.website.comm.webio import Get, Post


class KrxOtp(Get):
    def get(self, bld):
        resp = super().read(name="form", bld=bld)
        return resp.text

    @property
    def url(self):
        return "http://marketdata.krx.co.kr/contents/COM/GenerateOTP.jspx"


class KrxWebIo(Post):
    def post(self, **params):
        otp = KrxOtp().get(self.bld)
        params.update({"code": otp})
        resp = super().read(**params)
        return resp.json()

    @property
    def url(self):
        return "http://marketdata.krx.co.kr/contents/MKD/99/MKD99000001.jspx"

    @property
    @abstractmethod
    def bld(self):
        return NotImplementedError

    @property
    def read(self, **params):
        return NotImplementedError
