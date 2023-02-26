from .stock_api import *
from .future_api import *

def test():
    print('testing Stock Module')
def is_holiday_df(df:DataFrame):
    result = (df[['시가', '고가', '저가', '종가']] == 0).all(axis=None)
    return result