from pandas import DataFrame
import pandas as pd


def dataframe_empty_handler(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (AttributeError, KeyError, TypeError):
            return DataFrame()
    return wrapper


def singleton(class_):
    class class_w(class_):
        _instance = None
        def __new__(class_, *args, **kwargs):
            if class_w._instance is None:
                    class_w._instance = super(class_w, class_).__new__(class_, *args, **kwargs)
                    class_w._instance._sealed = False
            return class_w._instance
        def __init__(self, *args, **kwargs):
            if self._sealed:
                return
            super(class_w, self).__init__(*args, **kwargs)
            self._sealed = True
    class_w.__name__ = class_.__name__
    return class_w


def resample_ohlcv(df, freq, how):
    """
    :param df   : KRX OLCV format의 DataFrame
    :param freq : d - 일 / m - 월 / y - 년
    :return:    : resampling된 DataFrame
    """    
    if freq != 'd':        
        df.index = pd.to_datetime(df.index, format='%Y%m%d')
        if freq == 'm':
            df = df.resample('M').apply(how)
            df.index = df.index.strftime('%Y%m')            
        elif freq == 'y':
            df = df.resample('Y').apply(how)
            df.index = df.index.strftime('%Y')            
        else:
            print("choose a freq parameter in ('m', 'y', 'd')")
            raise RuntimeError
    return df
