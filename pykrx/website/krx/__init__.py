from .market import *
from .etx import *
from .bond import *
from .future import *
import datetime

def datetime2string(dt, freq='d'):
    if freq.upper() == 'Y':
        return dt.strftime("%Y")
    elif freq.upper() == 'M':
        return dt.strftime("%Y%m")
    else:
        return dt.strftime("%Y%m%d")

def get_nearest_business_day_in_a_week(date: str = None, prev: bool = True) \
        -> str:
    """인접한 영업일을 조회한다.

    Args:
        date (str , optional): 조회할 날짜로 입력하지 않으면 현재 시간으로 대체
        prev (bool, optional): 이전 영업일을 조회할지 이후 영업일을 조회할지
                               조정하는 flag

    Returns:
        str: 날짜 (YYMMDD)
    """
    if date is None:
        curr = datetime.datetime.now()
    else:
        curr = datetime.datetime.strptime(date, "%Y%m%d")

    if prev:
        prev = curr - datetime.timedelta(days=7)
        curr = curr.strftime("%Y%m%d")
        prev = prev.strftime("%Y%m%d")
        df = get_index_ohlcv_by_date(prev, curr, "1001")
        return df.index[-1].strftime("%Y%m%d")
    else:
        next = curr + datetime.timedelta(days=7)
        next = next.strftime("%Y%m%d")
        curr = curr.strftime("%Y%m%d")
        df = get_index_ohlcv_by_date(curr, next, "1001")
        return df.index[0].strftime("%Y%m%d")
