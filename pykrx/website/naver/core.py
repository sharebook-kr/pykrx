from pykrx.website.comm.webio import Get


class NaverWebIo(Get):
    def get(self, **params):
        resp = super().read(**params)
        return resp

    @property
    def url(self):
        return "http://fchart.stock.naver.com/sise.nhn"


class Sise(NaverWebIo):
    @property
    def uri(self):
        return "/sise.nhn"

    def read(self, ticker, count, timeframe='day'):
        """
        :param ticker:
        :param count:
        :param timeframe: day/week/month
        :return:
        """
        result = self.get(symbol=ticker, timeframe=timeframe, count=count,
                          requestType="0")
        return result.text


if __name__ == "__main__":
    r = Sise().read("006800", 10, "week")
    print(r)
