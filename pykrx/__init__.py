import platform
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
from . import bond
from . import stock

os = platform.system()

if os == "Darwin":
    plt.rc('font', family="AppleGothic")

else:
    fe = fm.FontEntry(
        fname="pykrx/NanumBarunGothic.ttf",
        name='NanumBarunGothic'
    )
    fm.fontManager.ttflist.insert(0, fe)
    plt.rc('font', family=fe.name)

plt.rcParams['axes.unicode_minus'] = False

__all__ = [
    'bond',
    'stock'
]

__version__ = '1.0.44'
