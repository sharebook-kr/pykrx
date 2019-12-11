from abc import abstractmethod
from pykrx.website.comm.webio import Webio


class KrxWebIo(Webio):
    def __init__(self):
        super().__init__()
        self.otp = self._get_otp_from_krx()

    def _get_otp_from_krx(self):
        url = "{}?bld={}&name={}".format(self.otp_url, self.bld, self.name)
        headers = {"User-Agent": "Mozilla/5.0"}
        return self.session.get(url=url, headers=headers).text

    def post(self, **kwargs):
        kwargs.update({"code": self.otp})
        return super().post(**kwargs).json()

    @property
    def otp_url(self):
        return "http://marketdata.krx.co.kr/contents/COM/GenerateOTP.jspx"

    @property
    def base_url(self):
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
