from pykrx.version import __version__

try:
    import platform
    import matplotlib.pyplot as plt
    import matplotlib.font_manager as fm

    os = platform.system()
    if os == "Darwin":
        # fonts = [x.name for x in fm.fontManager.ttflist if 'AppleGothic' in x.name]
        plt.rc('font', family="AppleGothic")
    else:
        # fonts = [x.name for x in fm.fontManager.ttflist if 'Malgun Gothic' in x.name]
        plt.rc('font', family="Malgun Gothic")
except:
    pass