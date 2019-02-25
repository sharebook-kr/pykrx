from setuptools import setup

setup(
    name            = 'pykrx',
    version         = '0.0.6',
    description     = 'KRX scraping',
    url             = 'https://github.com/sharebook-kr/pykrx',
    author          = 'Lukas Yoo',
    author_email    = 'jonghun.yoo@outlook.com',
    install_requires=['requests', 'pandas'],
    license         = 'MIT',
    packages        = ['pykrx', 'pykrx.comm', 'pykrx.stock', 'pykrx.short', 'pykrx.bond'],
    python_requires  = '>=3',
    zip_safe        = False
)