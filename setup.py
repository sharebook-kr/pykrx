from setuptools import setup

setup(
    name            = 'pykrx',
    version         = '0.0.1',
    description     = 'KRX scraping',
    url             = 'https://github.com/sharebook-kr/pykrx',
    author          = 'Lukas Yoo',
    author_email    = 'jonghun.yoo@outlook.com',
    install_requires=['requests', 'pandas'],
    license         = 'MIT',
    packages        = ['pykrx'],
    zip_safe        = False
)