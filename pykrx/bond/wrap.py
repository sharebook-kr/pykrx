from pykrx.bond.core import MKD40013, MKD40038
from pykrx.comm.util import dataframe_empty_handler


class KrxBond:
    @dataframe_empty_handler
    def get_treasury_yields_in_kerb_market(self, date):
        df = MKD40013().read(date)
        df = df[['str_const_val', 'lst_ord_bas_yd', 'fluc_chgrng']]
        df.columns = ['채권종류', '수익률', '등락폭']
        df = df.astype({"수익률": float, "등락폭": float})
        df.set_index('채권종류', inplace=True)
        return df

    @staticmethod
    def get_treasury_yields_in_bond_index(fromdate, todate):
        df = MKD40038().scraping(fromdate, todate)  # .reset_index(drop=True)
        if df is None:
            return None

        if fromdate not in df.index.tolist():
            print("WARN: fromdate seems to be a holiday")
            print("- {} is used instead of {}".format(df.index[-1], fromdate))
        if todate not in df.index.tolist():
            print("WARN: todate seems to be a holiday")
            print("- {} is used instead of {}".format(df.index[0], todate))

        # 구현!
        return df


if __name__ == "__main__":
    import pandas as pd
    pd.set_option('display.width', None)
    kb = KrxBond()
    df = kb.get_treasury_yields_in_kerb_market("20180105")
    print(df)
