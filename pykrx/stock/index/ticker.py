from pykrx.stock.index.core import MKD20011
from pykrx.comm.util import singleton
from pandas import DataFrame


@singleton
class IndexTicker:
    def __init__(self):
        self.df = DataFrame()

    def get_ticker(self, date, market):
        self._get(date, market)
        cond = (self.df['date'] == date) & (self.df['ind_tp_cd'] == market)
        return self.df[cond].index.tolist()

    def get_id(self, date, market, ticker):
        self._get(date, market)
        cond = (self.df.index == ticker) & (self.df['date'] == date)
        if len(self.df[cond]) == 0:
            print("NOT FOUND")
            return None
        return self.df.loc[cond, 'idx_ind_cd'][0]

    def get_market(self, date, market, ticker):
        self._get(date, market)
        cond = self.df.index == ticker
        return self.df.loc[cond, 'ind_tp_cd'][0]
        
    def _get(self, date, market):
        try:
            cond = (self.df['date'] == date) & (self.df['ind_tp_cd'] == market)
            if len(self.df[cond]) == 0:
                raise KeyError
        except KeyError:
            index = {"KOSPI": "02", "KOSDAQ": "03"}.get(market, "KOSPI")
            df = MKD20011().read(date, index)
            if len(df) == 0:
                return df

            df = df.set_index('idx_nm')
            df['date'] = date
            df['ind_tp_cd'] = df['ind_tp_cd'].apply(
                lambda x: "KOSPI" if x == "1" else "KOSDAQ")
            self.df = self.df.append(df)


if __name__ == "__main__":
    import pandas as pd
    pd.set_option('display.expand_frame_repr', False)

#    tickers = IndexTicker().get_ticker("20190412", "KOSPI")
#    print(tickers)
    index_id = IndexTicker().get_id("20190412", "KOSPI", "코스피")
    print(index_id)
    print(IndexTicker().get_market("20190412", "KOSPI", "코스피"))