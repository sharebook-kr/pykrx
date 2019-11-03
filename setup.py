from setuptools import setup, find_packages

setup(
    name            = 'pykrx',
    version         = '0.1.8',
    description     = 'KRX scraping',
    url             = 'https://github.com/sharebook-kr/pykrx',
    author          = 'Brayden Jo, Lukas Yoo',
    author_email    = 'brayden.jo@outlook.com, jonghun.yoo@outlook.com, pystock@outlook.com',
    install_requires= ['requests', 'pandas', 'datetime', 'numpy'],
    license         = 'MIT',
    packages        = find_packages(include=['pykrx', 'pykrx.*', 'pykrx.stock.*']),
    python_requires = '>=3',
    zip_safe        = False
)