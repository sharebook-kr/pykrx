import platform
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm

os = platform.system()

if os == "Darwin":
    family = "AppleGothic"
    install_url = "https://www.download-free-fonts.com/details/89379/" \
                  "applegothic-regular"

elif os == "Windows":
    family = "Malgun Gothic"
    install_url = "https://docs.microsoft.com/ko-kr/typography/" \
                  "font-list/malgun-gothic"

    plt.rc('font', family="AppleGothic")

else:
    family = "NanumBarunGothic"
    install_url = """NanumBarunGothic font is required.

    1) font install

    !sudo apt-get install -y fonts-nanum
    !sudo fc-cache -fv
    !rm ~/.cache/matplotlib -rf

    2) runtime restart if colab
    """

fonts = [x.name for x in fm.fontManager.ttflist if family in x.name]
if len(fonts) == 0:
    print("No Korean fonts found. Please install the font shown below"
          "because Hangul can be broken on the chart.")
    print(f" - {install_url}")
    print("PYKRX works fine regardless of font installation.")
    family = "Malgun Gothic"

plt.rc('font', family=family)
plt.rcParams['axes.unicode_minus'] = False
